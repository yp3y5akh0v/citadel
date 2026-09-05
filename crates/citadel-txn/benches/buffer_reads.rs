//! Bounded encrypted read scans and empty buffer-pool construction.
//! Run with `cargo bench -p citadeldb-txn --bench buffer_reads`.
//! Isolate scans with `-- --case cache-hit` or `-- --case cache-miss`.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use citadel_buffer::pool::BufferPool;
use citadel_core::types::SyncMode;
use citadel_core::{Error, Result, PAGE_SIZE};
use citadel_io::memory_io::MemoryPageIO;
use citadel_io::traits::PageIO;
use citadel_txn::manager::TxnManager;

const MAX_BYTES: u64 = 64 * 1024 * 1024;
const MAX_DURATION: Duration = Duration::from_secs(90);
const VALUE: [u8; 320] = [0x5a; 320];

struct CountedIo {
    inner: MemoryPageIO,
    reads: Arc<AtomicU64>,
}

impl CountedIo {
    fn check(offset: u64, len: usize) -> Result<()> {
        if offset
            .checked_add(len as u64)
            .is_none_or(|end| end > MAX_BYTES)
        {
            return Err(Error::Io(std::io::Error::other("benchmark byte limit")));
        }
        Ok(())
    }
}

impl PageIO for CountedIo {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_page(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        Self::check(offset, buf.len())?;
        self.inner.write_page(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.inner.read_at(offset, buf)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        Self::check(offset, buf.len())?;
        self.inner.write_at(offset, buf)
    }

    fn fsync(&self) -> Result<()> {
        self.inner.fsync()
    }

    fn file_size(&self) -> Result<u64> {
        self.inner.file_size()
    }

    fn truncate(&self, size: u64) -> Result<()> {
        Self::check(size, 0)?;
        self.inner.truncate(size)
    }
}

fn check_deadline(started: Instant) {
    assert!(started.elapsed() < MAX_DURATION, "benchmark time limit");
}

fn fixture(rows: u64, capacity: usize, started: Instant) -> (TxnManager, Arc<AtomicU64>) {
    let reads = Arc::new(AtomicU64::new(0));
    // Fixed keys are used only by this in-memory fixture.
    let manager = TxnManager::create_with_sync(
        Box::new(CountedIo {
            inner: MemoryPageIO::new(),
            reads: Arc::clone(&reads),
        }),
        [1; 32],
        [2; 32],
        1,
        0x1234,
        [3; 32],
        capacity,
        SyncMode::Off,
    )
    .unwrap();
    let mut writer = manager.begin_write().unwrap();
    for key in 0..rows {
        writer.insert(&key.to_be_bytes(), &VALUE).unwrap();
        if key % 1024 == 0 {
            check_deadline(started);
        }
    }
    writer.commit().unwrap();
    check_deadline(started);
    (manager, reads)
}

fn scan(manager: &TxnManager, rows: u64) {
    let mut count = 0;
    let mut sum = 0u64;
    manager
        .begin_read()
        .for_each(|key, value| {
            count += 1;
            sum += u64::from_be_bytes(key.try_into().unwrap());
            assert_eq!(value, VALUE);
            Ok(())
        })
        .unwrap();
    assert_eq!(count, rows);
    assert_eq!(sum, rows * (rows - 1) / 2);
    assert_eq!(manager.reader_count(), 0);
    black_box(sum);
}

fn read_case(
    name: &str,
    rows: u64,
    capacity: usize,
    warmup: usize,
    iterations: usize,
    started: Instant,
) {
    let (manager, reads) = fixture(rows, capacity, started);
    let before = reads.load(Ordering::Relaxed);
    for _ in 0..warmup {
        scan(&manager, rows);
        check_deadline(started);
    }
    let warmup_reads = reads.load(Ordering::Relaxed) - before;
    let before = reads.load(Ordering::Relaxed);
    let timer = Instant::now();
    for _ in 0..iterations {
        scan(&manager, rows);
        check_deadline(started);
    }
    let nanos = timer.elapsed().as_nanos();
    let page_reads = reads.load(Ordering::Relaxed) - before;
    if name == "cache_hit_scan" {
        assert_eq!(page_reads, 0);
    } else {
        assert!(warmup_reads > capacity as u64);
        assert!(page_reads > iterations as u64 * capacity as u64);
    }
    let integrity = manager.integrity_check().unwrap();
    assert!(integrity.is_ok(), "{integrity:?}");
    println!(
        "{{\"case\":\"{name}\",\"rows\":{rows},\"capacity\":{capacity},\"warmup\":{warmup},\"warmup_page_reads\":{warmup_reads},\"iterations\":{iterations},\"page_reads\":{page_reads},\"elapsed_ns\":{nanos}}}"
    );
}

fn main() {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    let (benchmark, selected) = match arguments.as_slice() {
        [] => (false, "all"),
        [argument] if argument == "--bench" => (true, "all"),
        [first, second, third] => {
            let name = if first == "--bench" && second == "--case" {
                third
            } else if first == "--case" && third == "--bench" {
                second
            } else {
                panic!("expected --bench and --case cache-hit|cache-miss");
            };
            let name = name.to_str().unwrap();
            assert!(matches!(name, "cache-hit" | "cache-miss"), "unknown case");
            (true, name)
        }
        _ => panic!("expected no arguments or --bench [--case cache-hit|cache-miss]"),
    };
    let started = Instant::now();
    println!(
        "{{\"schema\":\"buffer-reads-v2\",\"backend\":\"memory\",\"sync\":\"off\",\"benchmark\":{benchmark},\"selected\":\"{selected}\",\"max_bytes\":{MAX_BYTES},\"max_seconds\":90}}"
    );
    if selected == "all" {
        for capacity in [256, 4096] {
            let iterations = if benchmark { 128 } else { 1 };
            drop(black_box(BufferPool::new(capacity)));
            let timer = Instant::now();
            for _ in 0..iterations {
                let pool = BufferPool::new(capacity);
                assert_eq!(pool.capacity(), capacity);
                assert!(pool.is_empty());
                drop(black_box(pool));
                check_deadline(started);
            }
            let nanos = timer.elapsed().as_nanos();
            println!(
                "{{\"case\":\"empty_pool\",\"capacity\":{capacity},\"iterations\":{iterations},\"elapsed_ns\":{nanos}}}"
            );
        }
    }
    if matches!(selected, "all" | "cache-hit") {
        read_case(
            "cache_hit_scan",
            64,
            64,
            if benchmark { 32_768 } else { 1 },
            if benchmark { 131_072 } else { 1 },
            started,
        );
    }
    if matches!(selected, "all" | "cache-miss") {
        read_case(
            "cache_miss_scan",
            if benchmark { 8192 } else { 512 },
            16,
            if benchmark { 32 } else { 1 },
            if benchmark { 64 } else { 1 },
            started,
        );
    }
    check_deadline(started);
}
