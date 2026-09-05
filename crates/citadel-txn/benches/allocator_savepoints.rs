//! Bounded in-memory transaction benchmark for reclaimed-page allocation.
//! Run with `cargo bench -p citadeldb-txn --bench allocator_savepoints`.

use std::hint::black_box;
use std::time::{Duration, Instant};

use citadel_core::types::SyncMode;
use citadel_core::{Error, Result, PAGE_SIZE};
use citadel_io::memory_io::MemoryPageIO;
use citadel_io::traits::PageIO;
use citadel_page::overflow::OVERFLOW_DATA_CAPACITY;
use citadel_txn::manager::TxnManager;
use citadel_txn::pending_free;
use rustc_hash::FxHashMap;

const MAX_BYTES: u64 = 256 * 1024 * 1024;
const MAX_DURATION: Duration = Duration::from_secs(90);
const POOL_SIZES: [usize; 4] = [0, 128, 2048, 16384];
const WARMUP: usize = 100;

struct BoundedIo(MemoryPageIO);

impl BoundedIo {
    fn check(&self, offset: u64, len: usize) -> Result<()> {
        if offset
            .checked_add(len as u64)
            .is_none_or(|end| end > MAX_BYTES)
        {
            return Err(Error::Io(std::io::Error::other("benchmark byte limit")));
        }
        Ok(())
    }
}

impl PageIO for BoundedIo {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.0.read_page(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.check(offset, buf.len())?;
        self.0.write_page(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.0.read_at(offset, buf)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        self.check(offset, buf.len())?;
        self.0.write_at(offset, buf)
    }

    fn fsync(&self) -> Result<()> {
        self.0.fsync()
    }

    fn file_size(&self) -> Result<u64> {
        self.0.file_size()
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.check(size, 0)?;
        self.0.truncate(size)
    }
}

fn check_deadline(started: Instant) {
    assert!(started.elapsed() < MAX_DURATION, "benchmark time limit");
}

fn available_entries(manager: &TxnManager) -> usize {
    let slot = manager.current_slot();
    let mut pages = FxHashMap::default();
    let mut next = slot.pending_free_root;
    while next.is_valid() {
        assert!(pages.len() < 128, "benchmark chain limit");
        let page = manager.read_page_from_disk(next).unwrap();
        let following = page.right_child();
        assert!(pages.insert(next, page).is_none(), "pending-free cycle");
        next = following;
    }
    pending_free::read_chain(&pages, slot.pending_free_root)
        .unwrap()
        .iter()
        .filter(|entry| entry.freed_at_txn < slot.txn_id)
        .count()
}

fn fixture(pool_pages: usize, started: Instant) -> TxnManager {
    // Fixed fixture keys are used only with the in-memory backend.
    let manager = TxnManager::create_with_sync(
        Box::new(BoundedIo(MemoryPageIO::new())),
        [1; 32],
        [2; 32],
        1,
        0x1234,
        [3; 32],
        256,
        SyncMode::Off,
    )
    .unwrap();
    let mut writer = manager.begin_write().unwrap();
    writer.insert(b"anchor", b"original").unwrap();
    if pool_pages != 0 {
        let value = vec![0x5a; pool_pages * OVERFLOW_DATA_CAPACITY];
        writer.insert(b"temporary", &value).unwrap();
    }
    writer.commit().unwrap();
    if pool_pages != 0 {
        let mut writer = manager.begin_write().unwrap();
        assert!(writer.delete(b"temporary").unwrap());
        writer.commit().unwrap();
        let mut writer = manager.begin_write().unwrap();
        writer.insert(b"anchor", b"original").unwrap();
        writer.commit().unwrap();
    }
    check_deadline(started);
    let available = available_entries(&manager);
    assert!(available >= pool_pages);
    assert!(available <= pool_pages + 16);
    assert_eq!(manager.reader_count(), 0);
    manager
}

fn measure(
    name: &str,
    pool_pages: usize,
    available: usize,
    iterations: usize,
    started: Instant,
    mut operation: impl FnMut(usize),
) {
    for index in 0..WARMUP {
        operation(index);
    }
    check_deadline(started);
    let timer = Instant::now();
    for index in 0..iterations {
        operation(index);
        if index % 1024 == 0 {
            check_deadline(started);
        }
    }
    let nanos = timer.elapsed().as_nanos();
    println!(
        "{{\"case\":\"{name}\",\"requested_pool\":{pool_pages},\"available_before\":{available},\"iterations\":{iterations},\"elapsed_ns\":{nanos}}}"
    );
}

fn smoke_test() {
    let started = Instant::now();
    for pool_pages in [0, 128] {
        let manager = fixture(pool_pages, started);
        let available = available_entries(&manager);
        let original_hwm = manager.current_slot().high_water_mark;
        manager.begin_write().unwrap().abort();
        manager.begin_write().unwrap().commit().unwrap();
        let mut writer = manager.begin_write().unwrap();
        for index in 0usize..8 {
            let snapshot = writer.begin_savepoint();
            writer.insert(b"anchor", &index.to_le_bytes()).unwrap();
            writer.restore_snapshot(snapshot);
            assert_eq!(
                writer.get(b"anchor").unwrap().as_deref(),
                Some(b"original".as_slice())
            );
        }
        writer.abort();
        assert_eq!(manager.current_slot().high_water_mark, original_hwm);
        assert_eq!(available_entries(&manager), available);
        let mut writer = manager.begin_write().unwrap();
        writer.insert(b"anchor", b"committed").unwrap();
        writer.commit().unwrap();
        assert_eq!(
            manager.begin_read().get(b"anchor").unwrap().as_deref(),
            Some(b"committed".as_slice())
        );
        assert_eq!(manager.reader_count(), 0);
        let integrity = manager.integrity_check().unwrap();
        assert!(integrity.is_ok(), "{integrity:?}");
        check_deadline(started);
    }
    println!("allocator savepoint smoke test passed");
}

fn main() {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    let benchmark = match arguments.as_slice() {
        [] => false,
        [argument] if argument == "--bench" => true,
        _ => panic!("expected no arguments or --bench"),
    };
    if !benchmark {
        smoke_test();
        return;
    }
    let started = Instant::now();
    println!("{{\"schema\":\"allocator-savepoints-v1\",\"backend\":\"memory\",\"sync\":\"off\",\"max_bytes\":{MAX_BYTES},\"max_seconds\":90,\"warmup\":{WARMUP}}}");
    for pool_pages in POOL_SIZES {
        let manager = fixture(pool_pages, started);
        let available = available_entries(&manager);
        let original_hwm = manager.current_slot().high_water_mark;
        measure(
            "begin_abort",
            pool_pages,
            available,
            50_000,
            started,
            |_| {
                black_box(manager.begin_write().unwrap()).abort();
            },
        );
        measure("begin_noop", pool_pages, available, 50_000, started, |_| {
            manager.begin_write().unwrap().commit().unwrap();
        });
        let mut writer = manager.begin_write().unwrap();
        measure(
            "savepoint_update_rollback",
            pool_pages,
            available,
            50_000,
            started,
            |index| {
                let snapshot = writer.begin_savepoint();
                writer.insert(b"anchor", &index.to_le_bytes()).unwrap();
                writer.restore_snapshot(snapshot);
            },
        );
        assert_eq!(
            writer.get(b"anchor").unwrap().as_deref(),
            Some(b"original".as_slice())
        );
        writer.abort();
        assert_eq!(manager.current_slot().high_water_mark, original_hwm);
        assert_eq!(available_entries(&manager), available);
        measure(
            "update_commit",
            pool_pages,
            available,
            200,
            started,
            |index| {
                let mut writer = manager.begin_write().unwrap();
                writer.insert(b"anchor", &index.to_le_bytes()).unwrap();
                writer.commit().unwrap();
            },
        );
        assert_eq!(
            manager.begin_read().get(b"anchor").unwrap(),
            Some(199usize.to_le_bytes().to_vec())
        );
        assert_eq!(manager.reader_count(), 0);
        let integrity = manager.integrity_check().unwrap();
        assert!(integrity.is_ok(), "{integrity:?}");
        println!(
            "{{\"fixture_complete\":{pool_pages},\"available_after\":{},\"hwm_before\":{original_hwm},\"hwm_after\":{}}}",
            available_entries(&manager),
            manager.current_slot().high_water_mark
        );
        check_deadline(started);
    }
}
