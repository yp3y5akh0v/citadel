use super::*;
use citadel_core::types::{PageType, SyncMode};
use citadel_crypto::hkdf_utils::derive_keys_from_rek;
use citadel_crypto::page_cipher::compute_dek_id;
use std::sync::Mutex as StdMutex;

#[cfg(feature = "parallel")]
#[path = "manager_parallel_tests.rs"]
mod parallel_commit_batches;

pub struct MemIO {
    data: Arc<StdMutex<Vec<u8>>>,
}

impl MemIO {
    pub fn new(size: usize) -> Self {
        Self {
            data: Arc::new(StdMutex::new(vec![0u8; size])),
        }
    }

    /// Second handle over the same backing buffer (close/reopen tests).
    pub fn share(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
        }
    }

    /// Independent copy of the current bytes (used to fork one durable state).
    pub fn deep_clone(&self) -> Self {
        Self {
            data: Arc::new(StdMutex::new(self.data.lock().unwrap().clone())),
        }
    }
}

impl PageIO for MemIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let data = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > data.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end",
            )));
        }
        buf.copy_from_slice(&data[start..end]);
        Ok(())
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > data.len() {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(buf);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let data = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + buf.len();
        if end > data.len() {
            let available = data.len().saturating_sub(start);
            if available > 0 {
                buf[..available].copy_from_slice(&data[start..start + available]);
            }
            buf[available..].fill(0);
            return Ok(());
        }
        buf.copy_from_slice(&data[start..end]);
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + buf.len();
        if end > data.len() {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(buf);
        Ok(())
    }

    fn fsync(&self) -> Result<()> {
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        Ok(self.data.lock().unwrap().len() as u64)
    }

    fn truncate(&self, size: u64) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data.resize(size as usize, 0);
        Ok(())
    }
}

/// Growth regressions must fail before their in-memory backing can grow without
/// bound. The guard also covers preallocation and the default batched writes.
struct CappedCommitIO<T> {
    inner: T,
    max_bytes: u64,
}

impl<T: PageIO> CappedCommitIO<T> {
    fn new(inner: T, max_bytes: u64) -> Self {
        assert!(inner.file_size().unwrap() <= max_bytes);
        Self { inner, max_bytes }
    }

    fn check_end(&self, end: Option<u64>) -> Result<()> {
        if end.is_some_and(|end| end <= self.max_bytes) {
            Ok(())
        } else {
            Err(Error::Io(std::io::Error::other(
                "bounded commit test exceeded its I/O limit",
            )))
        }
    }
}

impl<T: PageIO> PageIO for CappedCommitIO<T> {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.inner.read_page(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.check_end(offset.checked_add(PAGE_SIZE as u64))?;
        self.inner.write_page(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.inner.read_at(offset, buf)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        self.check_end(offset.checked_add(buf.len() as u64))?;
        self.inner.write_at(offset, buf)
    }

    fn fsync(&self) -> Result<()> {
        self.inner.fsync()
    }

    fn file_size(&self) -> Result<u64> {
        self.inner.file_size()
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.check_end(Some(size))?;
        self.inner.truncate(size)
    }
}

#[test]
fn capped_commit_io_refuses_growth_before_modifying_backing() {
    let backing = MemIO::new(PAGE_SIZE);
    let capped = CappedCommitIO::new(backing.share(), PAGE_SIZE as u64);
    assert!(capped.write_page(1, &[1; PAGE_SIZE]).is_err());
    assert!(capped.write_at(PAGE_SIZE as u64, &[1]).is_err());
    assert!(capped.write_at(u64::MAX, &[1]).is_err());
    assert!(capped.truncate(PAGE_SIZE as u64 + 1).is_err());
    assert_eq!(backing.file_size().unwrap(), PAGE_SIZE as u64);
    assert!(backing.data.lock().unwrap().iter().all(|&byte| byte == 0));
    capped.write_page(0, &[1; PAGE_SIZE]).unwrap();
    assert!(backing.data.lock().unwrap().iter().all(|&byte| byte == 1));
}

struct CacheReadCountingIO {
    inner: MemIO,
    page_reads: Arc<AtomicU64>,
}

impl PageIO for CacheReadCountingIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.page_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_page(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.inner.write_page(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.inner.read_at(offset, buf)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        self.inner.write_at(offset, buf)
    }

    fn fsync(&self) -> Result<()> {
        self.inner.fsync()
    }

    fn file_size(&self) -> Result<u64> {
        self.inner.file_size()
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size)
    }
}

#[test]
fn retired_cache_eviction_retains_registered_readers_and_resumes_after_release() {
    const MAX_BYTES: u64 = 2 * 1024 * 1024;
    let (dek, mac_key, dek_id) = test_keys();
    for sync_mode in [SyncMode::Off, SyncMode::Full] {
        for secure_delete in [false, true] {
            let io = MemIO::new(1024 * 1024);
            let page_reads = Arc::new(AtomicU64::new(0));
            let manager = TxnManager::create_with_sync(
                Box::new(CappedCommitIO::new(
                    CacheReadCountingIO {
                        inner: io.share(),
                        page_reads: Arc::clone(&page_reads),
                    },
                    MAX_BYTES,
                )),
                dek,
                mac_key,
                1,
                0x1234,
                dek_id,
                32,
                sync_mode,
            )
            .unwrap();
            manager.set_secure_delete(secure_delete);
            commit_insert(&manager, b"key", b"original");
            let old_root = manager.current_slot().tree_root;
            let mut pinned = manager.begin_read();
            assert_eq!(
                pinned.get(b"key").unwrap().as_deref(),
                Some(b"original".as_slice())
            );
            let mut cold = manager.begin_read();
            let mut reload = manager.begin_read();
            assert!(manager.pool.lock().is_cached(old_root));
            let mut old_ciphertext = [0u8; PAGE_SIZE];
            io.read_page(page_offset(old_root), &mut old_ciphertext)
                .unwrap();

            commit_insert(&manager, b"key", b"committed");
            let new_root = manager.current_slot().tree_root;
            assert_ne!(new_root, old_root);
            {
                let pool = manager.pool.lock();
                assert!(
                    pool.is_cached(old_root),
                    "registered readers conservatively retain retired shared pages"
                );
                assert!(
                    pool.is_cached(new_root),
                    "the committed replacement must be cached"
                );
            }
            let mut after = [0u8; PAGE_SIZE];
            io.read_page(page_offset(old_root), &mut after).unwrap();
            assert_eq!(
                after, old_ciphertext,
                "cache eviction must not erase retired storage"
            );

            page_reads.store(0, Ordering::Relaxed);
            assert_eq!(
                pinned.get(b"key").unwrap().as_deref(),
                Some(b"original".as_slice())
            );
            assert_eq!(
                page_reads.load(Ordering::Relaxed),
                0,
                "a pinned reader retains its Arc"
            );
            assert_eq!(
                manager.begin_read().get(b"key").unwrap().as_deref(),
                Some(b"committed".as_slice())
            );
            assert_eq!(
                page_reads.load(Ordering::Relaxed),
                0,
                "new readers use the committed cache page"
            );
            assert_eq!(
                cold.get(b"key").unwrap().as_deref(),
                Some(b"original".as_slice())
            );
            assert_eq!(
                page_reads.load(Ordering::Relaxed),
                0,
                "an unread old snapshot still benefits from the retained shared page"
            );
            assert!(
                manager.pool.lock().is_cached(old_root),
                "the old snapshot remains shared-cache resident"
            );
            assert_eq!(
                pinned.get(b"key").unwrap().as_deref(),
                Some(b"original".as_slice())
            );
            assert_eq!(page_reads.load(Ordering::Relaxed), 0);

            // Normal cache pressure can still evict the page. A registered
            // reader must reload its protected disk snapshot correctly.
            manager.pool.lock().invalidate(old_root);
            assert_eq!(
                reload.get(b"key").unwrap().as_deref(),
                Some(b"original".as_slice())
            );
            assert_eq!(page_reads.load(Ordering::Relaxed), 1);
            drop(reload);
            drop(pinned);
            drop(cold);

            // The gate is per commit; after readers leave, newly retired
            // current pages should stop occupying shared cache slots.
            commit_insert(&manager, b"key", b"after readers");
            let latest_root = manager.current_slot().tree_root;
            assert_ne!(latest_root, new_root);
            assert!(!manager.pool.lock().is_cached(new_root));
            assert!(manager.pool.lock().is_cached(latest_root));
            assert_eq!(
                manager.begin_read().get(b"key").unwrap().as_deref(),
                Some(b"after readers".as_slice())
            );
            assert!(manager.integrity_check().unwrap().is_ok());
        }
    }
}

#[test]
fn aborted_or_failed_commits_do_not_evict_committed_pages() {
    let (dek, mac_key, dek_id) = test_keys();
    for sync_mode in [SyncMode::Off, SyncMode::Full] {
        let io = MemIO::new(1024 * 1024);
        let faulty = FaultingIO::new(io, i64::MAX);
        let writes_left = Arc::clone(&faulty.writes_left);
        let manager = TxnManager::create_with_sync(
            Box::new(CappedCommitIO::new(faulty, 2 * 1024 * 1024)),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            32,
            sync_mode,
        )
        .unwrap();
        commit_insert(&manager, b"key", b"original");
        let before = manager.current_slot();
        assert!(manager.pool.lock().is_cached(before.tree_root));

        let mut aborted = manager.begin_write().unwrap();
        aborted.insert(b"key", b"aborted").unwrap();
        aborted.abort();
        assert!(manager.pool.lock().is_cached(before.tree_root));

        let mut failed = manager.begin_write().unwrap();
        failed.insert(b"key", b"failed").unwrap();
        writes_left.store(0, Ordering::SeqCst);
        assert!(matches!(failed.commit(), Err(Error::Io(_))));
        assert_eq!(manager.current_slot(), before);
        assert!(manager.pool.lock().is_cached(before.tree_root));
        assert_eq!(
            manager.begin_read().get(b"key").unwrap().as_deref(),
            Some(b"original".as_slice())
        );
    }
}

pub fn test_keys() -> ([u8; DEK_SIZE], [u8; MAC_KEY_SIZE], [u8; 32]) {
    let rek = [0x42u8; 32];
    let keys = derive_keys_from_rek(&rek);
    let dek_id = compute_dek_id(&keys.mac_key, &keys.dek);
    (keys.dek, keys.mac_key, dek_id)
}

pub fn create_test_manager() -> TxnManager {
    let (dek, mac_key, dek_id) = test_keys();
    let io = Box::new(MemIO::new(1024 * 1024));
    TxnManager::create(io, dek, mac_key, 1, 0x1234, dek_id, 256).unwrap()
}

pub fn create_test_manager_with_sync(sync_mode: citadel_core::types::SyncMode) -> TxnManager {
    let (dek, mac_key, dek_id) = test_keys();
    let io = Box::new(MemIO::new(1024 * 1024));
    TxnManager::create_with_sync(io, dek, mac_key, 1, 0x1234, dek_id, 256, sync_mode).unwrap()
}

fn commit_insert(mgr: &TxnManager, key: &[u8], val: &[u8]) {
    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(key, val).unwrap();
    wtx.commit().unwrap();
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CommitIoEffects {
    page_writes: usize,
    metadata_writes: usize,
    truncates: usize,
}

#[derive(Clone, Default)]
struct CommitIoRecorder(Arc<StdMutex<Option<CommitIoEffects>>>);

impl CommitIoRecorder {
    fn arm(&self) {
        *self.0.lock().unwrap() = Some(CommitIoEffects::default());
    }

    fn record(&self, update: impl FnOnce(&mut CommitIoEffects)) {
        if let Some(effects) = self.0.lock().unwrap().as_mut() {
            update(effects);
        }
    }

    fn effects(&self) -> CommitIoEffects {
        self.0.lock().unwrap().expect("commit recorder is armed")
    }
}

struct RecordingCommitIO {
    inner: MemIO,
    recorder: CommitIoRecorder,
}

impl PageIO for RecordingCommitIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.inner.read_page(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.recorder.record(|effects| effects.page_writes += 1);
        self.inner.write_page(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.inner.read_at(offset, buf)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        self.recorder.record(|effects| effects.metadata_writes += 1);
        self.inner.write_at(offset, buf)
    }

    fn fsync(&self) -> Result<()> {
        self.inner.fsync()
    }

    fn file_size(&self) -> Result<u64> {
        self.inner.file_size()
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.recorder.record(|effects| effects.truncates += 1);
        self.inner.truncate(size)
    }
}

// Keep physical and embedded IDs separate so a checksummed, authenticated
// page with the wrong embedded ID reaches the commit-time validation.
type PendingFixturePages = Vec<(PageId, Page)>;

fn reopen_with_pending_fixture<F>(build: F) -> (TxnManager, MemIO, CommitIoRecorder)
where
    F: FnOnce(PageId, TxnId) -> (PendingFixturePages, u32),
{
    use citadel_io::file_manager::{read_commit_slot, read_god_byte, write_commit_slot};

    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(4 * 1024 * 1024);
    let mgr =
        TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
    commit_insert(&mgr, b"seed", b"value");

    let active = (read_god_byte(&io).unwrap() & citadel_core::GOD_BIT_ACTIVE_SLOT) as usize;
    let mut slot = read_commit_slot(&io, active).unwrap();
    let root = PageId(slot.high_water_mark);
    let (pages, high_water_mark) = build(root, slot.txn_id);
    assert!(!pages.is_empty());
    assert!(high_water_mark > root.as_u32());
    for (physical, page) in &pages {
        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(
            &dek,
            &mac_key,
            *physical,
            slot.encryption_epoch,
            page.as_bytes(),
            &mut encrypted,
        );
        io.write_page(page_offset(*physical), &encrypted).unwrap();
    }
    slot.pending_free_root = root;
    slot.total_pages = high_water_mark;
    slot.high_water_mark = high_water_mark;
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();
    drop(mgr);

    let recorder = CommitIoRecorder::default();
    let reopened = TxnManager::open(
        Box::new(RecordingCommitIO {
            inner: io.share(),
            recorder: recorder.clone(),
        }),
        dek,
        mac_key,
        1,
        256,
    )
    .unwrap();
    (reopened, io, recorder)
}

fn assert_pending_fixture_refuses_commit<F>(case: &str, build: F) -> Error
where
    F: FnOnce(PageId, TxnId) -> (PendingFixturePages, u32),
{
    use citadel_io::file_manager::read_god_byte;

    let (mgr, io, recorder) = reopen_with_pending_fixture(build);
    let god_before = read_god_byte(&io).unwrap();
    let slot_before = mgr.current_slot();
    let generation_before = mgr.commit_generation();
    let mut txn = mgr.begin_write().unwrap();
    txn.insert(b"after", b"value").unwrap();
    recorder.arm();
    let error = txn.commit().expect_err(case);
    assert!(
        matches!(
            error,
            Error::DatabaseCorrupted | Error::PageOutOfBounds(_) | Error::InvalidPageType(_, _)
        ),
        "{case}: expected a metadata validation error, got {error:?}"
    );
    assert_eq!(
        recorder.effects(),
        CommitIoEffects::default(),
        "{case}: validation must precede all commit writes and truncation"
    );
    assert_eq!(mgr.current_slot(), slot_before, "{case}: slot changed");
    assert_eq!(
        mgr.commit_generation(),
        generation_before,
        "{case}: generation changed"
    );
    assert_eq!(
        read_god_byte(&io).unwrap(),
        god_before,
        "{case}: validation must fail before publishing recovery metadata"
    );
    mgr.begin_write().unwrap().abort();
    drop(mgr);

    let (dek, mac_key, _) = test_keys();
    let reopened = TxnManager::open(Box::new(io), dek, mac_key, 1, 256).unwrap();
    let mut reader = reopened.begin_read();
    assert_eq!(
        reader.get(b"seed").unwrap().as_deref(),
        Some(b"value".as_slice())
    );
    assert_eq!(reader.get(b"after").unwrap(), None, "{case}");
    error
}

const OVERFLOW_TABLE: &[u8] = b"overflow_table";
const OVERFLOW_DEFAULT_KEY: &[u8] = b"default-large";
const OVERFLOW_NAMED_KEY: &[u8] = b"named-large";

fn insert_overflow_fixture(mgr: &TxnManager) -> (Vec<u8>, Vec<u8>) {
    let default_value: Vec<u8> = (0..20_000).map(|index| (index % 251) as u8).collect();
    let named_value: Vec<u8> = (0..25_000).map(|index| 255 - (index % 239) as u8).collect();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(OVERFLOW_TABLE).unwrap();
    writer.insert(OVERFLOW_DEFAULT_KEY, &default_value).unwrap();
    writer
        .table_insert(OVERFLOW_TABLE, OVERFLOW_NAMED_KEY, &named_value)
        .unwrap();
    writer.commit().unwrap();
    (default_value, named_value)
}

fn assert_overflow_fixture(mgr: &TxnManager, default_value: &[u8], named_value: &[u8]) {
    let mut reader = mgr.begin_read();
    assert_eq!(
        reader.get(OVERFLOW_DEFAULT_KEY).unwrap().as_deref(),
        Some(default_value)
    );
    assert_eq!(
        reader
            .table_get(OVERFLOW_TABLE, OVERFLOW_NAMED_KEY)
            .unwrap()
            .as_deref(),
        Some(named_value)
    );
}

fn overflow_first_page(mgr: &TxnManager, root: PageId, key: &[u8]) -> PageId {
    let page = mgr.read_page_from_disk(root).unwrap();
    let cell = checked_leaf_cell_locations(&page)
        .unwrap()
        .into_iter()
        .find(|cell| cell.key(&page) == key)
        .expect("fixture key must be in its single-leaf tree");
    assert_eq!(cell.value_type, citadel_core::types::ValueType::Overflow);
    checked_overflow_reference(&page, cell).unwrap().0
}

/// Fault injection: allows `budget` successful writes, then every write
/// fails (sticky), simulating a process crash mid-commit. Reads pass
/// through, and the shared MemIO keeps whatever landed before the crash.
struct FaultingIO {
    inner: MemIO,
    writes_left: Arc<std::sync::atomic::AtomicI64>,
}

impl FaultingIO {
    fn new(inner: MemIO, budget: i64) -> Self {
        Self {
            inner,
            writes_left: Arc::new(std::sync::atomic::AtomicI64::new(budget)),
        }
    }

    fn charge(&self) -> Result<()> {
        if self.writes_left.fetch_sub(1, Ordering::SeqCst) <= 0 {
            return Err(Error::Io(std::io::Error::other("injected crash")));
        }
        Ok(())
    }
}

impl PageIO for FaultingIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.inner.read_page(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.charge()?;
        self.inner.write_page(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.inner.read_at(offset, buf)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        self.charge()?;
        self.inner.write_at(offset, buf)
    }

    fn fsync(&self) -> Result<()> {
        self.inner.fsync()
    }

    fn file_size(&self) -> Result<u64> {
        self.inner.file_size()
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size)
    }
}

/// A process crash at any write in the second commit must leave the first
/// commit's generation fully readable after reopen.
#[test]
fn off_mode_crash_mid_commit_preserves_previous_generation() {
    let (dek, mac_key, dek_id) = test_keys();
    let value_a = vec![b'a'; 200];
    let value_b = vec![b'b'; 300];

    for budget in 0..24i64 {
        let base = MemIO::new(1024 * 1024);
        {
            let mgr = TxnManager::create_with_sync(
                Box::new(base.share()),
                dek,
                mac_key,
                1,
                0x1234,
                dek_id,
                256,
                SyncMode::Off,
            )
            .unwrap();
            let mut wtx = mgr.begin_write().unwrap();
            for i in 0..40u32 {
                wtx.insert(format!("k{i:02}").as_bytes(), &value_a).unwrap();
            }
            wtx.commit().unwrap();
        }

        // Second commit through the crash-injected handle.
        let faulty = FaultingIO::new(base.share(), budget);
        let mgr = TxnManager::open_with_sync(Box::new(faulty), dek, mac_key, 1, 256, SyncMode::Off)
            .unwrap();
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..40u32 {
            wtx.insert(format!("k{i:02}").as_bytes(), &value_b).unwrap();
        }
        let commit_result = wtx.commit();
        drop(mgr); // the process dies; the shared buffer is the disk

        let mgr =
            TxnManager::open_with_sync(Box::new(base.share()), dek, mac_key, 1, 256, SyncMode::Off)
                .unwrap();
        let mut rtx = mgr.begin_read();
        let expected = if commit_result.is_ok() {
            &value_b
        } else {
            &value_a
        };
        assert_eq!(rtx.entry_count(), 40, "crash at write budget {budget}");
        for i in 0..40u32 {
            assert_eq!(
                rtx.get(format!("k{i:02}").as_bytes()).unwrap(),
                Some(expected.clone()),
                "row k{i:02} wrong after crash at write budget {budget}"
            );
        }
    }
}

#[test]
fn crash_with_reclaimed_chain_pages_preserves_reader_and_durable_snapshot() {
    let (dek, mac_key, dek_id) = test_keys();
    let keys: Vec<Vec<u8>> = (0..64)
        .map(|i| format!("loan-{i:02}").into_bytes())
        .collect();
    let oldest_value = vec![b'c'; 512];
    let durable_value = vec![b'd'; 512];
    let new_value = vec![b'e'; 512];

    let run = |sync_mode, fail_after: Option<i64>| {
        let io = MemIO::new(1024 * 1024);
        let faulty = FaultingIO::new(io.share(), i64::MAX);
        let writes_left = Arc::clone(&faulty.writes_left);
        let mgr = TxnManager::create_with_sync(
            Box::new(faulty),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            256,
            sync_mode,
        )
        .unwrap();
        for byte in *b"abc" {
            let mut txn = mgr.begin_write().unwrap();
            for key in &keys {
                txn.insert(key, &[byte; 512]).unwrap();
            }
            txn.commit().unwrap();
        }
        let oldest_slot = mgr.current_slot();
        let mut oldest_reader = mgr.begin_read();
        let mut txn = mgr.begin_write().unwrap();
        for key in &keys {
            txn.insert(key, &durable_value).unwrap();
        }
        txn.commit().unwrap();

        let before = mgr.current_slot();
        assert!(before.txn_id > oldest_slot.txn_id);
        assert_eq!(mgr.reclaim_horizon(), oldest_slot.txn_id);
        let before_tags = mgr.state.lock().retired_chain_pages.clone();
        assert!(
            !before_tags.is_empty(),
            "fixture must carry published metadata provenance"
        );
        let loan = {
            let mut state = mgr.state.lock();
            let ids: FxHashSet<_> = state.reclaimed_pages.iter().copied().collect();
            let tagged = state
                .reclaimed_pages
                .iter()
                .position(|id| before_tags.contains_key(id))
                .expect("fixture must offer a tagged metadata loan");
            let last = state.reclaimed_pages.len() - 1;
            // Change only allocation order: put an already-eligible metadata
            // page first in the allocator's LIFO body allocation path.
            Arc::make_mut(&mut state.reclaimed_pages).swap(tagged, last);
            assert_eq!(
                state
                    .reclaimed_pages
                    .iter()
                    .copied()
                    .collect::<FxHashSet<_>>(),
                ids
            );
            state.reclaimed_pages.clone()
        };
        let tagged_body = *loan.last().unwrap();
        assert!(
            loan.len() > 2,
            "fixture must offer data and structure loans"
        );
        let mut txn = mgr.begin_write().unwrap();
        txn.insert(&keys[0], &new_value).unwrap();
        let target_txn = txn.txn_id();
        assert_eq!(
            mgr.state.lock().retired_chain_pages,
            before_tags,
            "uncommitted body allocation must not publish tag removal"
        );
        let budget = fail_after.unwrap_or(i64::MAX);
        writes_left.store(budget, Ordering::SeqCst);
        let result = txn.commit();
        let write_attempts = budget - writes_left.load(Ordering::SeqCst);
        assert_eq!(
            result.is_ok(),
            fail_after.is_none(),
            "{sync_mode:?}, budget {fail_after:?}: {result:?}"
        );
        if let Some(limit) = fail_after {
            assert!(matches!(result, Err(Error::Io(_))));
            assert!(write_attempts > limit, "the injected write fault must fire");
            assert_eq!(
                mgr.state.lock().retired_chain_pages,
                before_tags,
                "failed loan consumption must not alter published metadata provenance"
            );
        } else {
            assert!(
                loan.contains(&mgr.current_slot().pending_free_root),
                "successful control must use a remaining loan as chain structure"
            );
            assert!(!mgr
                .state
                .lock()
                .retired_chain_pages
                .contains_key(&tagged_body));
            let reused = mgr.read_page_from_disk(tagged_body).unwrap();
            assert_eq!(reused.txn_id(), target_txn);
            assert!(
                matches!(reused.page_type(), Some(PageType::Leaf | PageType::Branch)),
                "successful control must consume the tagged loan as data, not structure"
            );
        }

        // No earlier reads populated this reader's local cache. Evict the
        // shared cache too, so a partial overwrite cannot hide behind old pages.
        mgr.pool.lock().clear();
        assert_eq!(oldest_reader.entry_count(), keys.len() as u64);
        for key in &keys {
            assert_eq!(
                oldest_reader.get(key).unwrap().as_deref(),
                Some(oldest_value.as_slice()),
                "old reader: {sync_mode:?}, budget {fail_after:?}"
            );
        }
        let expected_slot = if fail_after.is_some() {
            before
        } else {
            mgr.current_slot()
        };
        drop(oldest_reader);
        drop(mgr);

        let reopened =
            TxnManager::open_with_sync(Box::new(io), dek, mac_key, 1, 256, sync_mode).unwrap();
        // seal() computes the MAC, but serialize() materializes the checksum
        // without updating the live slot. Compare every decoded wire field.
        assert_eq!(
            reopened.current_slot(),
            CommitSlot::deserialize(&expected_slot.serialize())
        );
        let mut reader = reopened.begin_read();
        assert_eq!(reader.entry_count(), keys.len() as u64);
        for (index, key) in keys.iter().enumerate() {
            let expected = if index == 0 && fail_after.is_none() {
                &new_value
            } else {
                &durable_value
            };
            assert_eq!(
                reader.get(key).unwrap().as_deref(),
                Some(expected.as_slice()),
                "reopened: {sync_mode:?}, budget {fail_after:?}"
            );
        }
        drop(reader);
        let report = reopened.integrity_check().unwrap();
        assert!(
            report.is_ok(),
            "{sync_mode:?}, budget {fail_after:?}: {report:?}"
        );
        write_attempts
    };

    for sync_mode in [SyncMode::Off, SyncMode::Full] {
        let writes = run(sync_mode, None);
        assert!((1..=16).contains(&writes), "keep the fault matrix bounded");
        for budget in 0..writes {
            run(sync_mode, Some(budget));
        }
    }
}

fn pending_chain_pages(mgr: &TxnManager, root: PageId) -> Vec<Page> {
    let mut pages = Vec::new();
    let mut seen = FxHashSet::default();
    let mut next = root;
    while next.is_valid() {
        assert!(seen.insert(next), "pending-free chain must not cycle");
        let page = mgr.read_page_from_disk(next).unwrap();
        assert_eq!(page.page_id(), next);
        let _ = pending_free::read_page_entries(&page).unwrap();
        next = page.right_child();
        pages.push(page);
    }
    pages
}

#[test]
fn held_reader_pending_free_growth_is_linear_and_reuses_after_release() {
    const COMMITS: usize = 3000;
    const MAX_BYTES: u64 = 64 * 1024 * 1024;
    let (dek, mac_key, dek_id) = test_keys();
    for sync_mode in [SyncMode::Off, SyncMode::Full] {
        let io = MemIO::new(1024 * 1024);
        let mgr = TxnManager::create_with_sync(
            Box::new(CappedCommitIO::new(io.share(), MAX_BYTES)),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            32,
            sync_mode,
        )
        .unwrap();
        mgr.set_secure_delete(true);
        commit_insert(&mgr, b"key", b"original");
        let original_slot = mgr.current_slot();
        let initial_entries: usize = pending_chain_pages(&mgr, original_slot.pending_free_root)
            .iter()
            .map(|page| pending_free::read_page_entries(page).unwrap().len())
            .sum();
        let mut oldest = mgr.begin_read();
        let mut data_retirements = 0;
        for sequence in 1..=COMMITS {
            let before = mgr.current_slot().high_water_mark;
            let mut txn = mgr.begin_write().unwrap();
            txn.insert(b"key", &(sequence as u64).to_le_bytes())
                .unwrap();
            let retired = txn.pending_free_count();
            assert_eq!(retired, 1, "fixture must replace exactly one data page");
            data_retirements += retired;
            txn.commit().unwrap_or_else(|error| {
                panic!("{sync_mode:?}, held-reader commit {sequence}: {error}")
            });
            // One data replacement, plus a head and at most one packing spill.
            assert!(
                mgr.current_slot().high_water_mark <= before + retired as u32 + 2,
                "{sync_mode:?}, commit {sequence}: allocated beyond the per-update page budget"
            );
        }
        assert_eq!(mgr.reclaim_horizon(), original_slot.txn_id);
        // Data stays pinned, but already-durable retired chain structure has no
        // reader lifetime. Every early loan must have exact metadata provenance.
        {
            let state = mgr.state.lock();
            let chain = pending_chain_pages(&mgr, state.current_slot.pending_free_root);
            let entries: FxHashMap<_, _> = chain
                .iter()
                .flat_map(|page| pending_free::read_page_entries(page).unwrap())
                .map(|entry| (entry.page_id, entry.freed_at_txn))
                .collect();
            for page in state.reclaimed_pages.iter() {
                assert_eq!(state.retired_chain_pages.get(page), entries.get(page));
                assert!(state.retired_chain_pages.contains_key(page));
            }
        }
        let held_slot = mgr.current_slot();
        let chain = pending_chain_pages(&mgr, held_slot.pending_free_root);
        let entries: usize = chain
            .iter()
            .map(|page| pending_free::read_page_entries(page).unwrap().len())
            .sum();
        let entry_budget = initial_entries + data_retirements + COMMITS;
        assert!(chain.len() >= 3, "exercise shared multi-page tails");
        assert!(
            entries <= entry_budget,
            "{sync_mode:?}: {entries} entries exceed {entry_budget}; retired metadata fed back"
        );
        // A full rewrite can leave one legacy partial tail. New prefixes must
        // still be packed, rather than adding a one-entry page on every commit.
        assert!(chain.len() <= entries.div_ceil(pending_free::MAX_ENTRIES_PER_PAGE) + 1);
        let page_budget = original_slot.high_water_mark as usize
            + data_retirements
            + COMMITS
            + entry_budget.div_ceil(pending_free::MAX_ENTRIES_PER_PAGE);
        assert!(
            held_slot.high_water_mark as usize <= page_budget,
            "{sync_mode:?}: {} allocated pages exceed linear budget {page_budget}",
            held_slot.high_water_mark
        );
        assert!(io.file_size().unwrap() <= MAX_BYTES);

        // This reader has never loaded the row; clear the shared cache as well.
        mgr.pool.lock().clear();
        assert_eq!(oldest.entry_count(), 1);
        assert_eq!(
            oldest.get(b"key").unwrap().as_deref(),
            Some(b"original".as_slice())
        );
        drop(oldest);

        commit_insert(&mgr, b"key", &((COMMITS + 1) as u64).to_le_bytes());
        let available = mgr.state.lock().reclaimed_pages.clone();
        assert!(available.len() >= data_retirements);
        assert!(available.contains(&original_slot.tree_root));
        let mut erased = [0xff; PAGE_SIZE];
        io.read_page(page_offset(original_slot.tree_root), &mut erased)
            .unwrap();
        assert!(
            erased.iter().all(|&byte| byte == 0),
            "released original page must be securely erased"
        );
        let reuse_high_water = mgr.current_slot().high_water_mark;
        for sequence in COMMITS + 2..=COMMITS + 65 {
            commit_insert(&mgr, b"key", &(sequence as u64).to_le_bytes());
            assert_eq!(
                mgr.current_slot().high_water_mark,
                reuse_high_water,
                "{sync_mode:?}: eligible entries must fund later data and chain pages"
            );
        }
        let expected_slot = mgr.current_slot();
        drop(mgr);
        let reopened = TxnManager::open_with_sync(
            Box::new(CappedCommitIO::new(io, MAX_BYTES)),
            dek,
            mac_key,
            1,
            32,
            sync_mode,
        )
        .unwrap();
        assert_eq!(
            reopened.current_slot(),
            CommitSlot::deserialize(&expected_slot.serialize())
        );
        assert_eq!(
            reopened.begin_read().get(b"key").unwrap(),
            Some(((COMMITS + 65) as u64).to_le_bytes().to_vec())
        );
        let report = reopened.integrity_check().unwrap();
        assert!(report.is_ok(), "{sync_mode:?}: {report:?}");
    }
}

#[test]
fn shared_pending_free_tail_survives_each_commit_write_failure() {
    use citadel_io::file_manager::{read_commit_slot, write_god_byte};

    const MAX_BYTES: u64 = 32 * 1024 * 1024;
    let (dek, mac_key, dek_id) = test_keys();
    // One bounded overflow value produces several pages of retirement entries
    // without replaying thousands of setup commits for every injected fault.
    let original =
        vec![b'o'; (2 * pending_free::MAX_ENTRIES_PER_PAGE + 32) * citadel_core::USABLE_SIZE];
    let prepare = |sync_mode| {
        let io = MemIO::new(1024 * 1024);
        let mgr = TxnManager::create_with_sync(
            Box::new(CappedCommitIO::new(io.share(), MAX_BYTES)),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            32,
            sync_mode,
        )
        .unwrap();
        mgr.set_secure_delete(true);
        commit_insert(&mgr, b"key", &original);
        let mut original_reader = mgr.begin_read();
        commit_insert(&mgr, b"key", b"durable0");
        commit_insert(&mgr, b"key", b"durable1");
        mgr.pool.lock().clear();
        assert_eq!(
            original_reader.get(b"key").unwrap().as_deref(),
            Some(original.as_slice())
        );
        drop(original_reader);
        drop(mgr);
        // The loan-backed commit left a full head. Reopen naturally clears the
        // loan map, so the next small commit prepends a partial head. Both
        // physical slots now contain small rows instead of the bulk setup value.
        let mgr = TxnManager::open_with_sync(
            Box::new(CappedCommitIO::new(io.share(), MAX_BYTES)),
            dek,
            mac_key,
            1,
            32,
            sync_mode,
        )
        .unwrap();
        let chain = pending_chain_pages(&mgr, mgr.current_slot().pending_free_root);
        assert_eq!(
            pending_free::read_page_entries(&chain[0]).unwrap().len(),
            pending_free::MAX_ENTRIES_PER_PAGE
        );
        commit_insert(&mgr, b"key", b"durable2");
        drop(mgr);
        io
    };
    let run = |base: &MemIO, sync_mode, fail_after: Option<i64>| {
        let io = base.deep_clone();
        // Each independent reopen starts with no loans/provenance. Pin the new
        // snapshot before any write so the target exercises the no-removal lane.
        // Do not enable secure delete here: probing the older physical slot
        // after an interrupted commit requires retaining its old data too.
        let faulty = FaultingIO::new(io.share(), i64::MAX);
        let writes_left = Arc::clone(&faulty.writes_left);
        let mgr = TxnManager::open_with_sync(
            Box::new(CappedCommitIO::new(faulty, MAX_BYTES)),
            dek,
            mac_key,
            1,
            32,
            sync_mode,
        )
        .unwrap();
        let mut oldest = mgr.begin_read();
        assert!(mgr.state.lock().reclaimed_pages.is_empty());
        let before = mgr.current_slot();
        let chain = pending_chain_pages(&mgr, before.pending_free_root);
        assert!(
            chain.len() >= 3,
            "fault target needs a multi-page shared tail"
        );
        let head_entries = pending_free::read_page_entries(&chain[0]).unwrap().len();
        assert!((1..pending_free::MAX_ENTRIES_PER_PAGE - 2).contains(&head_entries));
        let old_bytes: Vec<_> = chain
            .iter()
            .map(|page| {
                let mut bytes = [0u8; PAGE_SIZE];
                io.read_page(page_offset(page.page_id()), &mut bytes)
                    .unwrap();
                (page.page_id(), bytes)
            })
            .collect();
        let before_slots = [
            read_commit_slot(&io, 0).unwrap(),
            read_commit_slot(&io, 1).unwrap(),
        ];
        assert_ne!(before_slots[0].txn_id, before_slots[1].txn_id);
        assert_ne!(before_slots[0].tree_root, before_slots[1].tree_root);
        let before_values = before_slots.each_ref().map(|slot| {
            if slot.tree_root == before.tree_root {
                b"durable2".as_slice()
            } else {
                b"durable1".as_slice()
            }
        });
        let before_tags = mgr.state.lock().retired_chain_pages.clone();
        assert!(before_tags.is_empty());

        let mut txn = mgr.begin_write().unwrap();
        txn.insert(b"key", b"newvalue").unwrap();
        let target_txn = txn.txn_id();
        assert_eq!(txn.pending_free_count(), 1);
        let budget = fail_after.unwrap_or(i64::MAX);
        writes_left.store(budget, Ordering::SeqCst);
        let result = txn.commit();
        let attempts = budget - writes_left.load(Ordering::SeqCst);
        assert_eq!(
            result.is_ok(),
            fail_after.is_none(),
            "{sync_mode:?}, {fail_after:?}: {result:?}"
        );
        if let Some(limit) = fail_after {
            assert!(matches!(result, Err(Error::Io(_))));
            assert!(attempts > limit, "the write fault must actually fire");
            assert_eq!(
                mgr.state.lock().retired_chain_pages,
                before_tags,
                "failed commit must not publish candidate metadata provenance"
            );
        } else {
            let after_chain = pending_chain_pages(&mgr, mgr.current_slot().pending_free_root);
            assert_ne!(after_chain[0].page_id(), chain[0].page_id());
            assert_eq!(after_chain[0].right_child(), chain[0].right_child());
            assert_eq!(after_chain.len(), chain.len());
            let retirement = pending_free::read_page_entries(&after_chain[0])
                .unwrap()
                .find(|entry| entry.page_id == chain[0].page_id())
                .expect("replaced head is retired");
            assert_eq!(retirement.freed_at_txn, target_txn);
            let state = mgr.state.lock();
            assert!(
                !state.reclaimed_pages.contains(&chain[0].page_id()),
                "newly retired head must not become a loan"
            );
            assert_eq!(
                state.retired_chain_pages.get(&chain[0].page_id()),
                Some(&target_txn)
            );
        }
        for (id, expected) in &old_bytes {
            let mut actual = [0u8; PAGE_SIZE];
            io.read_page(page_offset(*id), &mut actual).unwrap();
            assert_eq!(
                &actual, expected,
                "{sync_mode:?}, {fail_after:?}: shared or retired page {id} overwritten"
            );
        }
        mgr.pool.lock().clear();
        assert_eq!(
            oldest.get(b"key").unwrap().as_deref(),
            Some(b"durable2".as_slice()),
            "{sync_mode:?}, {fail_after:?}: cold oldest snapshot changed"
        );
        let expected_slot = if fail_after.is_none() {
            mgr.current_slot()
        } else {
            before
        };
        drop(oldest);
        drop(mgr);

        let reopened = TxnManager::open_with_sync(
            Box::new(CappedCommitIO::new(io.share(), MAX_BYTES)),
            dek,
            mac_key,
            1,
            32,
            sync_mode,
        )
        .unwrap();
        assert_eq!(
            reopened.current_slot(),
            CommitSlot::deserialize(&expected_slot.serialize())
        );
        let expected = if fail_after.is_none() {
            b"newvalue"
        } else {
            b"durable2"
        };
        assert_eq!(
            reopened.begin_read().get(b"key").unwrap().as_deref(),
            Some(expected.as_slice())
        );
        let report = reopened.integrity_check().unwrap();
        assert!(report.is_ok(), "{sync_mode:?}, {fail_after:?}: {report:?}");
        drop(reopened);

        // Independently select each complete, authenticated physical slot in a
        // fork. Normal recovery still follows its god byte above; these probes
        // additionally prove neither recovery generation lost its shared tail.
        for slot_index in 0..2 {
            let slot = read_commit_slot(&io, slot_index).unwrap();
            let expected = if slot.txn_id == target_txn {
                b"newvalue".as_slice()
            } else {
                assert_eq!(slot, before_slots[slot_index]);
                before_values[slot_index]
            };
            let fork = io.deep_clone();
            write_god_byte(&fork, slot_index as u8).unwrap();
            let recovered = TxnManager::open_with_sync(
                Box::new(CappedCommitIO::new(fork, MAX_BYTES)),
                dek,
                mac_key,
                1,
                32,
                sync_mode,
            )
            .unwrap();
            assert_eq!(recovered.current_slot(), slot);
            assert_eq!(
                recovered.begin_read().get(b"key").unwrap().as_deref(),
                Some(expected),
                "{sync_mode:?}, {fail_after:?}, physical slot {slot_index}"
            );
        }
        attempts
    };
    for sync_mode in [SyncMode::Off, SyncMode::Full] {
        let base = prepare(sync_mode);
        let writes = run(&base, sync_mode, None);
        assert!((1..=8).contains(&writes), "keep the fault matrix bounded");
        for budget in 0..writes {
            run(&base, sync_mode, Some(budget));
        }
    }
}

fn consuming_head_base(sync_mode: SyncMode) -> MemIO {
    const MAX_BYTES: u64 = 32 * 1024 * 1024;
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let manager = TxnManager::create_with_sync(
        Box::new(CappedCommitIO::new(io.share(), MAX_BYTES)),
        dek,
        mac_key,
        1,
        0x1234,
        dek_id,
        32,
        sync_mode,
    )
    .unwrap();
    let original = vec![
        b'o';
        (2 * pending_free::MAX_ENTRIES_PER_PAGE + 32)
            * citadel_page::overflow::OVERFLOW_DATA_CAPACITY
    ];
    commit_insert(&manager, b"key", &original);
    commit_insert(&manager, b"key", b"durable0");
    commit_insert(&manager, b"key", b"durable1");
    assert_eq!(
        pending_chain_pages(&manager, manager.current_slot().pending_free_root).len(),
        3,
        "the bounded fixture must produce three retirement pages"
    );
    io
}

#[test]
fn consuming_pending_head_survives_each_commit_write_failure() {
    use citadel_io::file_manager::{read_commit_slot, write_god_byte};

    const MAX_BYTES: u64 = 32 * 1024 * 1024;
    let (dek, mac_key, _) = test_keys();
    let run = |base: &MemIO,
               sync_mode,
               secure_delete,
               pin_readers: bool,
               fail_after: Option<i64>| {
        let io = base.deep_clone();
        let faulty = FaultingIO::new(io.share(), i64::MAX);
        let writes_left = Arc::clone(&faulty.writes_left);
        let manager = TxnManager::open_with_sync(
            Box::new(CappedCommitIO::new(faulty, MAX_BYTES)),
            dek,
            mac_key,
            1,
            32,
            sync_mode,
        )
        .unwrap();
        manager.set_secure_delete(secure_delete);
        // Reopen deliberately starts without loans. The first update makes
        // old entries available; the next packs the chain using those loans.
        commit_insert(&manager, b"key", b"durable2");
        let older_slot = manager.current_slot();
        let mut older_reader = pin_readers.then(|| manager.begin_read());
        commit_insert(&manager, b"key", b"durable3");
        let before = manager.current_slot();
        let mut current_reader = pin_readers.then(|| manager.begin_read());
        let before_generation = manager.commit_generation();
        let chain = pending_chain_pages(&manager, before.pending_free_root);
        assert_eq!(chain.len(), 3);
        let old_ids: Vec<_> = chain.iter().map(Page::page_id).collect();
        let head_ids: FxHashSet<_> = pending_free::read_page_entries(&chain[0])
            .unwrap()
            .map(|entry| entry.page_id)
            .collect();
        let old_entries: FxHashMap<_, _> = chain
            .iter()
            .flat_map(|page| pending_free::read_page_entries(page).unwrap())
            .map(|entry| (entry.page_id, entry.freed_at_txn))
            .collect();
        let (loan, before_tags) = {
            let state = manager.state.lock();
            (
                state.reclaimed_pages.clone(),
                state.retired_chain_pages.clone(),
            )
        };
        assert!(loan.len() >= 2);
        assert!(
            loan.iter().rev().take(2).all(|id| head_ids.contains(id)),
            "the next body and structure loans must both be in the old head"
        );
        let body_loan = loan[loan.len() - 1];
        let structure_loan = loan[loan.len() - 2];
        let old_bytes: Vec<_> = old_ids
            .iter()
            .map(|&id| {
                let mut bytes = [0u8; PAGE_SIZE];
                io.read_page(page_offset(id), &mut bytes).unwrap();
                (id, bytes)
            })
            .collect();
        let before_slots = [
            read_commit_slot(&io, 0).unwrap(),
            read_commit_slot(&io, 1).unwrap(),
        ];
        assert_ne!(before_slots[0].tree_root, before_slots[1].tree_root);
        assert!(before_slots
            .iter()
            .any(|slot| slot.txn_id == older_slot.txn_id));

        let mut writer = manager.begin_write().unwrap();
        writer.insert(b"key", b"newvalue").unwrap();
        assert_eq!(writer.pending_free_count(), 1);
        let target_txn = writer.txn_id();
        let budget = fail_after.unwrap_or(i64::MAX);
        writes_left.store(budget, Ordering::SeqCst);
        let result = writer.commit();
        let attempts = budget - writes_left.load(Ordering::SeqCst);
        assert_eq!(
            result.is_ok(),
            fail_after.is_none(),
            "{sync_mode:?}, secure={secure_delete}, budget={fail_after:?}: {result:?}"
        );
        if let Some(limit) = fail_after {
            assert!(matches!(result, Err(Error::Io(_))));
            assert!(attempts > limit);
            assert_eq!(manager.current_slot(), before);
            assert_eq!(manager.commit_generation(), before_generation);
            let state = manager.state.lock();
            assert_eq!(state.reclaimed_pages.as_ref(), loan.as_ref());
            assert_eq!(state.retired_chain_pages, before_tags);
        } else {
            let after = manager.current_slot();
            let next_chain = pending_chain_pages(&manager, after.pending_free_root);
            let next_ids: Vec<_> = next_chain.iter().map(Page::page_id).collect();
            assert_eq!(after.tree_root, body_loan);
            assert_eq!(after.pending_free_root, structure_loan);
            assert_eq!(next_ids.len(), old_ids.len());
            assert_eq!(next_ids[1..], old_ids[1..], "must share the actual tail");
            assert_eq!(after.high_water_mark, before.high_water_mark);
            let mut expected_entries = old_entries.clone();
            assert!(expected_entries.remove(&body_loan).is_some());
            assert!(expected_entries.remove(&structure_loan).is_some());
            assert!(expected_entries.insert(old_ids[0], target_txn).is_none());
            assert!(expected_entries
                .insert(before.tree_root, target_txn)
                .is_none());
            let actual_entries: Vec<_> = next_chain
                .iter()
                .flat_map(|page| pending_free::read_page_entries(page).unwrap())
                .map(|entry| (entry.page_id, entry.freed_at_txn))
                .collect();
            assert_eq!(actual_entries.len(), expected_entries.len());
            assert_eq!(
                actual_entries.into_iter().collect::<FxHashMap<_, _>>(),
                expected_entries
            );
            let mut expected_tags = before_tags.clone();
            expected_tags.remove(&body_loan);
            expected_tags.remove(&structure_loan);
            expected_tags.insert(old_ids[0], target_txn);
            let state = manager.state.lock();
            assert_eq!(state.retired_chain_pages, expected_tags);
            for id in [body_loan, structure_loan, old_ids[0], before.tree_root] {
                assert!(!state.reclaimed_pages.contains(&id));
            }
        }
        for (id, expected) in &old_bytes {
            let mut actual = [0u8; PAGE_SIZE];
            io.read_page(page_offset(*id), &mut actual).unwrap();
            assert_eq!(
                &actual, expected,
                "{sync_mode:?}, secure={secure_delete}, budget={fail_after:?}: old chain {id} changed"
            );
        }
        manager.pool.lock().clear();
        if let Some(reader) = &mut older_reader {
            assert_eq!(
                reader.get(b"key").unwrap().as_deref(),
                Some(b"durable2".as_slice())
            );
        }
        if let Some(reader) = &mut current_reader {
            assert_eq!(
                reader.get(b"key").unwrap().as_deref(),
                Some(b"durable3".as_slice())
            );
        }
        let expected_slot = manager.current_slot();
        drop(older_reader);
        drop(current_reader);
        drop(manager);

        let reopened = TxnManager::open_with_sync(
            Box::new(CappedCommitIO::new(io.share(), MAX_BYTES)),
            dek,
            mac_key,
            1,
            32,
            sync_mode,
        )
        .unwrap();
        assert_eq!(
            reopened.current_slot(),
            CommitSlot::deserialize(&expected_slot.serialize())
        );
        let expected = if fail_after.is_none() {
            b"newvalue"
        } else {
            b"durable3"
        };
        assert_eq!(
            reopened.begin_read().get(b"key").unwrap().as_deref(),
            Some(expected.as_slice())
        );
        let report = reopened.integrity_check().unwrap();
        assert!(
            report.is_ok(),
            "{sync_mode:?}, secure={secure_delete}, readers={pin_readers}, budget={fail_after:?}: {report:?}"
        );
        drop(reopened);

        // Both physical slots remain readable even without registered readers.
        for (slot_index, before_slot) in before_slots.iter().enumerate() {
            let slot = read_commit_slot(&io, slot_index).unwrap();
            let expected = if slot.txn_id == target_txn {
                b"newvalue".as_slice()
            } else {
                assert_eq!(&slot, before_slot);
                if slot.txn_id == before.txn_id {
                    b"durable3".as_slice()
                } else {
                    assert_eq!(slot.txn_id, older_slot.txn_id);
                    b"durable2".as_slice()
                }
            };
            let fork = io.deep_clone();
            write_god_byte(&fork, slot_index as u8).unwrap();
            let recovered = TxnManager::open_with_sync(
                Box::new(CappedCommitIO::new(fork, MAX_BYTES)),
                dek,
                mac_key,
                1,
                32,
                sync_mode,
            )
            .unwrap();
            assert_eq!(recovered.current_slot(), slot);
            assert_eq!(
                recovered.begin_read().get(b"key").unwrap().as_deref(),
                Some(expected)
            );
        }
        attempts
    };
    for sync_mode in [SyncMode::Off, SyncMode::Normal, SyncMode::Full] {
        let base = consuming_head_base(sync_mode);
        for secure_delete in [false, true] {
            for pin_readers in [false, true] {
                let writes = run(&base, sync_mode, secure_delete, pin_readers, None);
                assert!((1..=16).contains(&writes), "keep the fault matrix bounded");
                for budget in 0..writes {
                    run(&base, sync_mode, secure_delete, pin_readers, Some(budget));
                }
            }
        }
    }
}

#[test]
fn secure_delete_defers_newest_retirements_until_both_slots_release_them() {
    let (dek, mac_key, dek_id) = test_keys();
    for sync_mode in [SyncMode::Off, SyncMode::Normal, SyncMode::Full] {
        let io = MemIO::new(1024 * 1024);
        let manager = TxnManager::create_with_sync(
            Box::new(io.share()),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            32,
            sync_mode,
        )
        .unwrap();
        manager.set_secure_delete(true);
        commit_insert(&manager, b"key", b"original");
        let original = manager.current_slot();
        let mut original_bytes = [0u8; PAGE_SIZE];
        io.read_page(page_offset(original.tree_root), &mut original_bytes)
            .unwrap();

        // Readers and aborted/no-op writers consume transaction IDs without
        // publishing slots. Retirement age counts commits, not consecutive IDs.
        drop(manager.begin_read());
        drop(manager.begin_write().unwrap());
        manager.begin_write().unwrap().commit().unwrap();
        let mut writer = manager.begin_write().unwrap();
        writer.insert(b"key", b"replacement").unwrap();
        let checkpoint = writer.begin_savepoint();
        writer.insert(b"key", b"speculative").unwrap();
        writer.restore_snapshot(checkpoint);
        writer.commit().unwrap();
        let retired_at = manager.current_slot().txn_id;
        assert!(retired_at.as_u64() > original.txn_id.as_u64() + 1);

        drop(manager.begin_read());
        drop(manager.begin_write().unwrap());
        let mut writer = manager.begin_write().unwrap();
        writer.refresh_all_catalog_descriptors(&[]).unwrap();
        writer.commit().unwrap();
        assert!(manager
            .state
            .lock()
            .reclaimed_pages
            .contains(&original.tree_root));
        let mut bytes = [0u8; PAGE_SIZE];
        io.read_page(page_offset(original.tree_root), &mut bytes)
            .unwrap();
        assert!(
            bytes == original_bytes,
            "{sync_mode:?}: newest retirement was erased early"
        );
        assert!(manager.state.lock().zeroed_up_to < retired_at);
        let report = manager.integrity_check().unwrap();
        assert!(report.is_ok(), "{sync_mode:?}: {report:?}");
        drop(manager);

        // Reopen naturally clears loans, so the next commit zeroes the retained
        // data instead of overwriting it as a borrowed structure page.
        let reopened =
            TxnManager::open_with_sync(Box::new(io.share()), dek, mac_key, 1, 32, sync_mode)
                .unwrap();
        reopened.set_secure_delete(true);
        let mut writer = reopened.begin_write().unwrap();
        writer.refresh_all_catalog_descriptors(&[]).unwrap();
        writer.commit().unwrap();
        io.read_page(page_offset(original.tree_root), &mut bytes)
            .unwrap();
        assert!(
            bytes.iter().all(|&byte| byte == 0),
            "{sync_mode:?}: deferred data was not erased"
        );
        assert!(reopened.state.lock().zeroed_up_to >= retired_at);
        assert_eq!(
            reopened.begin_read().get(b"key").unwrap().as_deref(),
            Some(b"replacement".as_slice())
        );
        let report = reopened.integrity_check().unwrap();
        assert!(report.is_ok(), "{sync_mode:?}: {report:?}");
    }
}

#[test]
fn consuming_pending_head_preserves_aborted_and_restored_loans() {
    const MAX_BYTES: u64 = 32 * 1024 * 1024;
    let (dek, mac_key, _) = test_keys();
    let io = consuming_head_base(SyncMode::Off);
    let manager = TxnManager::open_with_sync(
        Box::new(CappedCommitIO::new(io, MAX_BYTES)),
        dek,
        mac_key,
        1,
        32,
        SyncMode::Off,
    )
    .unwrap();
    commit_insert(&manager, b"key", b"durable2");
    let mut reader = manager.begin_read();
    commit_insert(&manager, b"key", b"durable3");
    let before = manager.current_slot();
    let loan = manager.state.lock().reclaimed_pages.clone();
    let old_chain = pending_chain_pages(&manager, before.pending_free_root);
    let old_ids: Vec<_> = old_chain.iter().map(Page::page_id).collect();
    let old_entries: FxHashMap<_, _> = old_chain
        .iter()
        .flat_map(|page| pending_free::read_page_entries(page).unwrap())
        .map(|entry| (entry.page_id, entry.freed_at_txn))
        .collect();
    // Cross the head boundary: recording these rolled-back allocations as
    // consumed would remove still-free tail entries or force a full rewrite.
    let discarded = vec![
        b'x';
        (pending_free::MAX_ENTRIES_PER_PAGE + 8)
            * citadel_page::overflow::OVERFLOW_DATA_CAPACITY
    ];
    let mut aborted = manager.begin_write().unwrap();
    aborted.insert(b"key", &discarded).unwrap();
    aborted.abort();
    manager.begin_write().unwrap().commit().unwrap();
    assert_eq!(manager.current_slot(), before);
    assert_eq!(manager.state.lock().reclaimed_pages.as_ref(), loan.as_ref());

    let mut writer = manager.begin_write().unwrap();
    let snapshot = writer.begin_savepoint();
    for _ in 0..2 {
        writer.insert(b"key", &discarded).unwrap();
        writer.restore_snapshot(snapshot.clone());
        assert_eq!(writer.pending_free_count(), 0);
        assert_eq!(
            writer.get(b"key").unwrap().as_deref(),
            Some(b"durable3".as_slice())
        );
    }
    writer.insert(b"key", b"committed").unwrap();
    let target_txn = writer.txn_id();
    writer.commit().unwrap();
    let after = manager.current_slot();
    let new_chain = pending_chain_pages(&manager, after.pending_free_root);
    let new_ids: Vec<_> = new_chain.iter().map(Page::page_id).collect();
    assert_eq!(new_ids.len(), old_ids.len());
    assert_eq!(new_ids[1..], old_ids[1..]);
    assert_eq!(after.high_water_mark, before.high_water_mark);
    assert_eq!(after.tree_root, loan[loan.len() - 1]);
    assert_eq!(after.pending_free_root, loan[loan.len() - 2]);
    let mut expected = old_entries;
    assert!(expected.remove(&after.tree_root).is_some());
    assert!(expected.remove(&after.pending_free_root).is_some());
    assert!(expected.insert(old_ids[0], target_txn).is_none());
    assert!(expected.insert(before.tree_root, target_txn).is_none());
    let actual: Vec<_> = new_chain
        .iter()
        .flat_map(|page| pending_free::read_page_entries(page).unwrap())
        .map(|entry| (entry.page_id, entry.freed_at_txn))
        .collect();
    assert_eq!(actual.len(), expected.len());
    assert_eq!(actual.into_iter().collect::<FxHashMap<_, _>>(), expected);
    manager.pool.lock().clear();
    assert_eq!(
        reader.get(b"key").unwrap().as_deref(),
        Some(b"durable2".as_slice())
    );
    assert_eq!(
        manager.begin_read().get(b"key").unwrap().as_deref(),
        Some(b"committed".as_slice())
    );
    drop(reader);
    assert!(manager.integrity_check().unwrap().is_ok());
}

#[test]
fn rolling_readers_bound_retired_metadata_and_survive_reopen() {
    const LAG: usize = 1000;
    const COMMITS: usize = 4500;
    const MAX_BYTES: u64 = 64 * 1024 * 1024;
    let (dek, mac_key, dek_id) = test_keys();
    for sync_mode in [SyncMode::Off, SyncMode::Full] {
        let io = MemIO::new(1024 * 1024);
        let mgr = TxnManager::create_with_sync(
            Box::new(CappedCommitIO::new(io.share(), MAX_BYTES)),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            32,
            sync_mode,
        )
        .unwrap();
        mgr.set_secure_delete(true);
        commit_insert(&mgr, b"key", &0u64.to_le_bytes());
        let mut readers = std::collections::VecDeque::new();
        readers.push_back((0usize, mgr.begin_read()));
        let mut data_retirements = 0usize;
        for sequence in 1..=COMMITS {
            let mut txn = mgr.begin_write().unwrap();
            txn.insert(b"key", &(sequence as u64).to_le_bytes())
                .unwrap();
            assert_eq!(txn.pending_free_count(), 1);
            data_retirements += txn.pending_free_count();
            txn.commit().unwrap_or_else(|error| {
                panic!("{sync_mode:?}, rolling commit {sequence}: {error}")
            });
            readers.push_back((sequence, mgr.begin_read()));
            if readers.len() > LAG {
                readers.pop_front();
            }
            if sequence % 250 == 0 {
                let slot = mgr.current_slot();
                let chain = pending_chain_pages(&mgr, slot.pending_free_root);
                let entries: usize = chain
                    .iter()
                    .map(|page| pending_free::read_page_entries(page).unwrap().len())
                    .sum();
                // The retained data window is 1000 single-page retirements.
                // Generous slack covers two-slot staging and transient packing,
                // not a budget proportional to the number of elapsed commits.
                assert!(
                    entries <= 2 * LAG + 64,
                    "{sync_mode:?}, commit {sequence}: {entries} entries exceed the reader window"
                );
                assert!(
                    slot.high_water_mark as usize <= 4 * LAG + 128,
                    "{sync_mode:?}, commit {sequence}: {} pages exceed the reader window",
                    slot.high_water_mark
                );
                assert!(mgr.state.lock().retired_chain_pages.len() <= entries);
            }
        }
        assert_eq!(data_retirements, COMMITS);
        assert_eq!(readers.len(), LAG);
        assert_eq!(readers.front().unwrap().0, COMMITS - LAG + 1);
        mgr.pool.lock().clear();
        for (sequence, reader) in &mut readers {
            assert_eq!(
                reader.get(b"key").unwrap(),
                Some((*sequence as u64).to_le_bytes().to_vec()),
                "{sync_mode:?}: rolling snapshot {sequence}"
            );
        }
        drop(readers);
        drop(mgr);

        let reopened = TxnManager::open_with_sync(
            Box::new(CappedCommitIO::new(io, MAX_BYTES)),
            dek,
            mac_key,
            1,
            32,
            sync_mode,
        )
        .unwrap();
        assert!(reopened.state.lock().retired_chain_pages.is_empty());
        assert!(reopened.state.lock().reclaimed_pages.is_empty());
        // No write or maintenance pass is allowed before this first new reader.
        let mut pinned = reopened.begin_read();
        reopened.set_secure_delete(true);
        let before = reopened.current_slot().high_water_mark;
        for sequence in COMMITS + 1..=COMMITS + 256 {
            commit_insert(&reopened, b"key", &(sequence as u64).to_le_bytes());
        }
        assert!(reopened.current_slot().high_water_mark <= before + 512);
        reopened.pool.lock().clear();
        assert_eq!(
            pinned.get(b"key").unwrap(),
            Some((COMMITS as u64).to_le_bytes().to_vec())
        );
        drop(pinned);
        let report = reopened.integrity_check().unwrap();
        assert!(report.is_ok(), "{sync_mode:?}: {report:?}");
    }
}

#[test]
fn metadata_reused_as_data_loses_early_reclamation_provenance() {
    const MAX_BYTES: u64 = 8 * 1024 * 1024;
    let (dek, mac_key, dek_id) = test_keys();
    for sync_mode in [SyncMode::Off, SyncMode::Full] {
        let mgr = TxnManager::create_with_sync(
            Box::new(CappedCommitIO::new(MemIO::new(1024 * 1024), MAX_BYTES)),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            32,
            sync_mode,
        )
        .unwrap();
        mgr.set_secure_delete(true);
        commit_insert(&mgr, b"key", &0u64.to_le_bytes());
        let oldest = mgr.begin_read();
        let mut reused = None;
        for sequence in 1..=32u64 {
            let tags = mgr.state.lock().retired_chain_pages.clone();
            commit_insert(&mgr, b"key", &sequence.to_le_bytes());
            let root = mgr.current_slot().tree_root;
            if tags.contains_key(&root) {
                assert!(
                    !mgr.state.lock().retired_chain_pages.contains_key(&root),
                    "a body allocation must invalidate its previous metadata lifetime"
                );
                reused = Some((root, sequence, mgr.begin_read()));
                break;
            }
        }
        let (reused_page, expected, mut reader) =
            reused.expect("fixture must reuse a tagged metadata page as live data");
        drop(oldest);
        for sequence in 100..132u64 {
            commit_insert(&mgr, b"key", &sequence.to_le_bytes());
            let state = mgr.state.lock();
            assert!(!state.retired_chain_pages.contains_key(&reused_page));
            assert!(
                !state.reclaimed_pages.contains(&reused_page),
                "its new data lifetime must honor the reader horizon"
            );
        }
        mgr.pool.lock().clear();
        assert_eq!(
            reader.get(b"key").unwrap(),
            Some(expected.to_le_bytes().to_vec())
        );
        drop(reader);
        let report = mgr.integrity_check().unwrap();
        assert!(report.is_ok(), "{sync_mode:?}: {report:?}");
    }
}

#[test]
fn early_metadata_zeroing_does_not_skip_later_reader_pinned_data() {
    const MAX_BYTES: u64 = 8 * 1024 * 1024;
    let (dek, mac_key, dek_id) = test_keys();
    for sync_mode in [SyncMode::Off, SyncMode::Full] {
        let io = MemIO::new(1024 * 1024);
        let mgr = TxnManager::create_with_sync(
            Box::new(CappedCommitIO::new(io.share(), MAX_BYTES)),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            32,
            sync_mode,
        )
        .unwrap();
        mgr.set_secure_delete(true);
        commit_insert(&mgr, b"key", b"original");
        let original_page = mgr.current_slot().tree_root;
        let mut reader = mgr.begin_read();
        commit_insert(&mgr, b"key", &1u64.to_le_bytes());
        let retired_at = mgr.current_slot().txn_id;
        for sequence in 2..=16u64 {
            commit_insert(&mgr, b"key", &sequence.to_le_bytes());
        }
        // An empty RAM loan cache is valid after open. Clear only that cache so
        // maintenance retains metadata long enough to erase it; preserve its
        // durable entries, provenance, and the original data reader throughout.
        for _ in 0..2 {
            mgr.state.lock().reclaimed_pages = Arc::new(Vec::new());
            let mut writer = mgr.begin_write().unwrap();
            writer.refresh_all_catalog_descriptors(&[]).unwrap();
            writer.commit().unwrap();
        }
        {
            let state = mgr.state.lock();
            assert!(
                state.zeroed_chain_up_to > retired_at,
                "fixture must zero newer metadata while older data remains pinned"
            );
            assert!(
                state.zeroed_up_to < retired_at,
                "metadata erasure must not advance the data watermark"
            );
            assert!(!state.retired_chain_pages.contains_key(&original_page));
        }
        mgr.pool.lock().clear();
        assert_eq!(
            reader.get(b"key").unwrap().as_deref(),
            Some(b"original".as_slice())
        );
        drop(reader);
        commit_insert(&mgr, b"key", b"released");
        assert!(mgr.state.lock().reclaimed_pages.contains(&original_page));
        let mut bytes = [0xff; PAGE_SIZE];
        io.read_page(page_offset(original_page), &mut bytes)
            .unwrap();
        assert!(
            bytes.iter().all(|&byte| byte == 0),
            "older data must still be zeroed after newer metadata advanced its separate watermark"
        );
        assert!(mgr.state.lock().zeroed_up_to >= retired_at);
        let report = mgr.integrity_check().unwrap();
        assert!(report.is_ok(), "{sync_mode:?}: {report:?}");
    }
}

#[test]
fn create_and_open() {
    let (dek, mac_key, dek_id) = test_keys();
    let io = Box::new(MemIO::new(1024 * 1024));

    let mgr = TxnManager::create(io, dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
    let slot = mgr.current_slot();
    assert_eq!(slot.txn_id, TxnId(1));
    assert_eq!(slot.tree_root, PageId(0));
    assert_eq!(slot.tree_depth, 1);
    assert_eq!(slot.tree_entries, 0);
    assert_eq!(slot.high_water_mark, 1);
}

#[test]
fn open_validates_the_header_before_repairing_it() {
    use citadel_io::file_manager::{read_file_header, read_god_byte, write_file_header};

    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let manager =
        TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
    drop(manager);

    let mut header = read_file_header(&io).unwrap();
    header.page_size = 0;
    header.god_byte = citadel_core::GOD_BIT_RECOVERY;
    write_file_header(&io, &header).unwrap();

    assert!(matches!(
        TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256),
        Err(Error::DatabaseCorrupted)
    ));
    assert_eq!(
        read_god_byte(&io).unwrap(),
        citadel_core::GOD_BIT_RECOVERY,
        "open changed the selector before validating the fixed header"
    );
}

#[test]
fn begin_read_registers_reader() {
    let mgr = create_test_manager();
    assert_eq!(mgr.reader_count(), 0);

    let _rtx = mgr.begin_read();
    assert_eq!(mgr.reader_count(), 1);
}

#[test]
fn drop_read_unregisters_reader() {
    let mgr = create_test_manager();
    {
        let _rtx = mgr.begin_read();
        assert_eq!(mgr.reader_count(), 1);
    }
    assert_eq!(mgr.reader_count(), 0);
}

#[test]
fn multiple_concurrent_readers() {
    let mgr = create_test_manager();
    let _r1 = mgr.begin_read();
    let _r2 = mgr.begin_read();
    let _r3 = mgr.begin_read();
    assert_eq!(mgr.reader_count(), 3);
}

#[test]
fn single_writer_enforcement() {
    let mgr = create_test_manager();
    let _wtx = mgr.begin_write().unwrap();
    let result = mgr.begin_write();
    assert!(matches!(result, Err(Error::WriteTransactionActive)));
}

#[test]
fn writer_released_after_drop() {
    let mgr = create_test_manager();
    {
        let _wtx = mgr.begin_write().unwrap();
    }
    let _wtx2 = mgr.begin_write().unwrap();
}

#[test]
fn reclaim_horizon_unbounded_with_no_readers() {
    let mgr = create_test_manager();
    assert_eq!(mgr.reclaim_horizon(), TxnId(u64::MAX));
}

#[test]
fn reclaim_horizon_is_min_reader_snapshot() {
    let mgr = create_test_manager();
    let snapshot_txn = mgr.current_slot().txn_id;
    let _r1 = mgr.begin_read();
    let _r2 = mgr.begin_read();
    assert_eq!(mgr.reclaim_horizon(), snapshot_txn);
}

#[test]
fn read_txn_does_not_advance_commit_generation() {
    let mgr = create_test_manager();
    let gen0 = mgr.commit_generation();
    let _r1 = mgr.begin_read();
    let _r2 = mgr.begin_read();
    let _r3 = mgr.begin_read();
    assert_eq!(mgr.commit_generation(), gen0);
}

#[test]
fn single_commit_bumps_commit_generation() {
    let mgr = create_test_manager();
    let gen0 = mgr.commit_generation();

    let mut wtx = mgr.begin_write().unwrap();
    assert!(wtx.insert(b"key", b"val").unwrap());
    wtx.commit().unwrap();

    assert_eq!(mgr.commit_generation(), gen0 + 1);
}

#[test]
fn noop_commit_does_not_bump_commit_generation() {
    let mgr = create_test_manager();
    let gen0 = mgr.commit_generation();

    let wtx = mgr.begin_write().unwrap();
    wtx.commit().unwrap();

    assert_eq!(mgr.commit_generation(), gen0);
}

#[test]
fn concurrent_commit_does_not_corrupt_existing_reader_snapshot() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        assert!(wtx.insert(b"key", b"v1").unwrap());
        wtx.commit().unwrap();
    }
    let baseline = mgr.commit_generation();

    let mut reader = mgr.begin_read();
    let reader_gen = reader.commit_generation();
    assert_eq!(reader_gen, baseline);

    {
        let mut wtx = mgr.begin_write().unwrap();
        assert!(!wtx.insert(b"key", b"v2").unwrap());
        wtx.commit().unwrap();
    }
    assert_eq!(mgr.commit_generation(), baseline + 1);

    assert_eq!(reader.commit_generation(), reader_gen);
    assert_eq!(reader.get(b"key").unwrap(), Some(b"v1".to_vec()));
}

/// Regression (freed-page reclamation vs late readers): a reader that begins
/// while a write txn is in flight must keep its pre-write snapshot even after
/// later commits reclaim and reuse the pages it references.
#[test]
fn reader_begun_during_write_txn_survives_page_reclaim() {
    let mgr = create_test_manager();

    let mut w1 = mgr.begin_write().unwrap();
    w1.insert(b"k1", b"A").unwrap();
    let mut reader = mgr.begin_read(); // snapshot: empty tree, before w1
    w1.commit().unwrap();

    // Age the freed root page toward reclamation and keep committing.
    commit_insert(&mgr, b"k2", b"B");
    commit_insert(&mgr, b"k3", b"C");

    assert_eq!(reader.entry_count(), 0);
    assert_eq!(reader.get(b"k1").unwrap(), None);
    assert_eq!(reader.get(b"k2").unwrap(), None);
    assert_eq!(reader.get(b"k3").unwrap(), None);
}

/// Regression (SyncMode::Off reclaimed freed pages with no reader check): a
/// reader that begins between commits must keep its snapshot across later
/// Off-mode commits that would otherwise reuse its pages.
#[test]
fn off_mode_reader_between_commits_survives_page_reclaim() {
    let mgr = create_test_manager_with_sync(SyncMode::Off);
    commit_insert(&mgr, b"k1", b"A");

    let mut reader = mgr.begin_read();
    commit_insert(&mgr, b"k2", b"B");
    commit_insert(&mgr, b"k3", b"C");
    commit_insert(&mgr, b"k4", b"D");

    assert_eq!(reader.get(b"k1").unwrap(), Some(b"A".to_vec()));
    assert_eq!(reader.get(b"k2").unwrap(), None);
    assert_eq!(reader.get(b"k4").unwrap(), None);
}

/// Regression (in-place commit destroyed a late reader's snapshot pages):
/// commits always CoW now, so a reader registered mid-write keeps its view.
#[test]
fn off_mode_reader_begun_during_write_keeps_snapshot() {
    let mgr = create_test_manager_with_sync(SyncMode::Off);

    let mut wtx = mgr.begin_write().unwrap();
    let mut reader = mgr.begin_read(); // registered after begin_write
    wtx.insert(b"k1", b"A").unwrap();
    wtx.commit().unwrap();

    assert_eq!(reader.entry_count(), 0);
    assert_eq!(reader.get(b"k1").unwrap(), None);
}

/// Regression (Off-mode catalog skip + silent slot truncation lost committed
/// rows): with more tables than the commit slot can carry, moved roots must
/// reach the catalog so every table serves its rows after reopen.
#[test]
fn off_mode_more_tables_than_slot_capacity_survive_reopen() {
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let tables: Vec<Vec<u8>> = (0..citadel_core::SLOT_NAMED_MAX_ENTRIES + 1)
        .map(|i| format!("table{i}").into_bytes())
        .collect();

    {
        let mgr = TxnManager::create_with_sync(
            Box::new(io.share()),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            256,
            SyncMode::Off,
        )
        .unwrap();

        let mut wtx = mgr.begin_write().unwrap();
        for t in &tables {
            wtx.create_table(t).unwrap();
            wtx.table_insert(t, b"row1", b"v1").unwrap();
        }
        wtx.commit().unwrap();

        // An open reader forces CoW so every touched root moves.
        let reader = mgr.begin_read();
        let mut wtx = mgr.begin_write().unwrap();
        for t in &tables {
            wtx.table_insert(t, b"row2", b"v2").unwrap();
        }
        wtx.commit().unwrap();
        drop(reader);
    }

    let mgr = TxnManager::open_with_sync(Box::new(io.share()), dek, mac_key, 1, 256, SyncMode::Off)
        .unwrap();
    let mut rtx = mgr.begin_read();
    for t in &tables {
        assert_eq!(
            rtx.table_get(t, b"row2").unwrap(),
            Some(b"v2".to_vec()),
            "table {} lost its committed row after reopen",
            String::from_utf8_lossy(t)
        );
    }
}

/// Regression (multi-commit staging): a skip commit leaves the slot as the
/// sole durable record of the touched roots; a later DDL commit writes the
/// catalog for its new tables only, so the carried stale entries must never
/// be dropped from the serialized slot in favor of fresh cache entries.
#[test]
fn off_mode_stale_slot_entries_survive_later_ddl_commit() {
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let staged: Vec<Vec<u8>> = (0..citadel_core::SLOT_NAMED_MAX_ENTRIES_V1)
        .map(|i| format!("staged{i}").into_bytes())
        .collect();

    {
        let mgr = TxnManager::create_with_sync(
            Box::new(io.share()),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            256,
            SyncMode::Off,
        )
        .unwrap();

        let mut wtx = mgr.begin_write().unwrap();
        for t in &staged {
            wtx.create_table(t).unwrap();
            wtx.table_insert(t, b"row1", b"v1").unwrap();
        }
        wtx.commit().unwrap();

        // Data-only commit: the catalog skip is taken, so every moved root
        // rides the slot stale-flagged while its catalog descriptor lags.
        let mut wtx = mgr.begin_write().unwrap();
        for t in &staged {
            wtx.table_insert(t, b"row2", b"v2").unwrap();
        }
        wtx.commit().unwrap();

        // DDL commit: catalog descriptors are written for the new tables
        // only; their fresh entries must be trimmed before any stale one.
        let mut wtx = mgr.begin_write().unwrap();
        for t in [b"extra0".as_slice(), b"extra1".as_slice()] {
            wtx.create_table(t).unwrap();
            wtx.table_insert(t, b"row1", b"v1").unwrap();
        }
        wtx.commit().unwrap();
    }

    let mgr = TxnManager::open_with_sync(Box::new(io.share()), dek, mac_key, 1, 256, SyncMode::Off)
        .unwrap();
    let mut rtx = mgr.begin_read();
    for t in &staged {
        assert_eq!(
            rtx.table_get(t, b"row2").unwrap(),
            Some(b"v2".to_vec()),
            "staged table {} lost its committed row after reopen",
            String::from_utf8_lossy(t)
        );
    }
    for t in [b"extra0".as_slice(), b"extra1".as_slice()] {
        assert_eq!(rtx.table_get(t, b"row1").unwrap(), Some(b"v1".to_vec()));
    }
}

/// Session keys are wiped on close; Drop delegates to wipe_keys.
#[test]
fn wipe_keys_zeroizes_dek_and_mac_key() {
    let mut mgr = create_test_manager();
    commit_insert(&mgr, b"k", b"v");
    assert_ne!(mgr.dek, [0u8; citadel_core::DEK_SIZE]);
    assert_ne!(mgr.mac_key, [0u8; citadel_core::MAC_KEY_SIZE]);

    mgr.wipe_keys();
    assert_eq!(mgr.dek, [0u8; citadel_core::DEK_SIZE]);
    assert_eq!(mgr.mac_key, [0u8; citadel_core::MAC_KEY_SIZE]);
}

/// Regression (reclaimed pages leaked on abort/no-op commit): an aborted or
/// empty write txn hands its loaned reclaimed batch back, so file growth
/// matches a run without the abort.
#[test]
fn abort_and_noop_commit_do_not_leak_reclaimed_pages() {
    let with_abort = create_test_manager();
    let control = create_test_manager();

    let churn = |mgr: &TxnManager| {
        for _ in 0..3 {
            commit_insert(mgr, b"key", b"value");
        }
    };

    churn(&with_abort);
    churn(&control);

    let mut wtx = with_abort.begin_write().unwrap();
    wtx.insert(b"key", b"tossed").unwrap();
    wtx.abort();
    with_abort.begin_write().unwrap().commit().unwrap(); // no-op commit

    churn(&with_abort);
    churn(&control);

    assert_eq!(
        with_abort.current_slot().high_water_mark,
        control.current_slot().high_water_mark,
        "aborted/no-op txns must not strand reclaimed pages"
    );
}

#[test]
fn repeated_savepoint_rollback_preserves_the_reclaimed_loan_and_reader() {
    let manager = create_test_manager();
    let control = create_test_manager();
    for manager in [&manager, &control] {
        for _ in 0..4 {
            commit_insert(manager, b"key", b"original");
        }
    }
    let loan = manager.state.lock().reclaimed_pages.clone();
    assert!(!loan.is_empty());
    let before = manager.current_slot();
    let mut reader = manager.begin_read();
    let mut writer = manager.begin_write().unwrap();
    let snapshot = writer.begin_savepoint();
    for byte in 0..8 {
        writer.insert(b"key", &vec![byte; 128 * 1024]).unwrap();
        writer.restore_snapshot(snapshot.clone());
        assert_eq!(
            writer.get(b"key").unwrap().as_deref(),
            Some(b"original".as_slice())
        );
        assert_eq!(writer.pending_free_count(), 0);
    }
    writer.commit().unwrap();
    assert_eq!(manager.current_slot(), before);
    assert_eq!(manager.state.lock().reclaimed_pages.as_ref(), loan.as_ref());
    commit_insert(&manager, b"key", b"committed");
    commit_insert(&control, b"key", b"committed");
    assert_eq!(
        manager.current_slot().high_water_mark,
        control.current_slot().high_water_mark
    );
    manager.pool.lock().clear();
    assert_eq!(
        reader.get(b"key").unwrap().as_deref(),
        Some(b"original".as_slice())
    );
    assert_eq!(
        manager.begin_read().get(b"key").unwrap().as_deref(),
        Some(b"committed".as_slice())
    );
    drop(reader);
    assert!(manager.integrity_check().unwrap().is_ok());
}

/// Regression (Off commits persisted dirty pages with their pre-edit Merkle
/// hash, letting sync's merkle_diff prune changed subtrees): Off zeroes the
/// hash instead, which always forces diff traversal.
#[test]
fn off_mode_commit_clears_stale_merkle_hashes() {
    let mgr = create_test_manager_with_sync(SyncMode::Off);
    let old_root = mgr.current_slot().tree_root;
    let old_hash = mgr.read_page_from_disk(old_root).unwrap().merkle_hash();
    assert_ne!(old_hash, [0u8; citadel_core::MERKLE_HASH_SIZE]);

    commit_insert(&mgr, b"x", b"1");

    let new_root = mgr.current_slot().tree_root;
    let new_hash = mgr.read_page_from_disk(new_root).unwrap().merkle_hash();
    assert_eq!(
        new_hash,
        [0u8; citadel_core::MERKLE_HASH_SIZE],
        "Off-mode dirty page must not keep its pre-edit merkle hash"
    );
}

#[test]
fn off_mode_preserves_the_overflow_payload_digest() {
    let mgr = create_test_manager_with_sync(SyncMode::Off);
    let value = vec![b'v'; 20_000];
    commit_insert(&mgr, b"large", &value);

    let slot = mgr.current_slot();
    assert_eq!(slot.merkle_root, [0u8; citadel_core::MERKLE_HASH_SIZE]);
    assert_eq!(
        mgr.read_page_from_disk(slot.tree_root)
            .unwrap()
            .merkle_hash(),
        [0u8; citadel_core::MERKLE_HASH_SIZE],
        "Off mode still marks dirty tree pages UNKNOWN"
    );

    let leaf = mgr.read_page_from_disk(slot.tree_root).unwrap();
    let cell = checked_leaf_cell_locations(&leaf).unwrap()[0];
    let (first_page, _) = checked_overflow_reference(&leaf, cell).unwrap();
    assert_eq!(
        mgr.read_page_from_disk(first_page).unwrap().merkle_hash(),
        crate::merkle::overflow_payload_hash(&value),
        "the head digest is payload metadata, not a tree-page cache"
    );
    let mut read = mgr.begin_read();
    assert_eq!(read.get(b"large").unwrap(), Some(value));
}

#[test]
fn cancel_aware_overflow_walk_stops_between_pages() {
    let mgr = create_test_manager();
    let value = vec![b'v'; 20_000];
    commit_insert(&mgr, b"large", &value);

    let slot = mgr.current_slot();
    let leaf = mgr.read_page_from_disk(slot.tree_root).unwrap();
    let cell = checked_leaf_cell_locations(&leaf).unwrap()[0];
    let (first_page, total_len) = checked_overflow_reference(&leaf, cell).unwrap();
    let token = CancelToken::new();
    let mut checks = 0usize;
    let mut pages_visited = 0usize;

    let error = mgr
        .walk_overflow_chain_checked(
            first_page,
            total_len,
            slot.high_water_mark,
            Some(slot.txn_id),
            true,
            || {
                checks += 1;
                if checks == 3 {
                    token.cancel();
                }
                token.check()
            },
            |_, _| {
                pages_visited += 1;
                Ok(())
            },
        )
        .unwrap_err();

    assert!(matches!(error, Error::Interrupted));
    assert_eq!(pages_visited, 1);
}

#[test]
fn logical_overflow_read_rejects_a_missing_head_digest() {
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(4 * 1024 * 1024);
    let mgr =
        TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
    let value = vec![b'v'; 20_000];
    commit_insert(&mgr, b"large", &value);

    let slot = mgr.current_slot();
    let first_page = overflow_first_page(&mgr, slot.tree_root, b"large");
    let mut head = mgr.read_page_from_disk(first_page).unwrap();
    head.set_merkle_hash(&[0u8; citadel_core::MERKLE_HASH_SIZE]);
    head.update_checksum();
    let mut encrypted = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        &dek,
        &mac_key,
        first_page,
        slot.encryption_epoch,
        head.as_bytes(),
        &mut encrypted,
    );
    io.write_page(page_offset(first_page), &encrypted).unwrap();

    let mut read = mgr.begin_read();
    assert!(matches!(
        read.get(b"large"),
        Err(Error::CorruptOverflowChain(message))
            if message.contains("missing its logical payload digest")
    ));
}

#[test]
fn legacy_overflow_head_without_a_digest_does_not_block_a_commit() {
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(4 * 1024 * 1024);
    let value = vec![b'v'; 20_000];
    {
        let mgr =
            TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
        commit_insert(&mgr, b"large", &value);

        let slot = mgr.current_slot();
        let first_page = overflow_first_page(&mgr, slot.tree_root, b"large");
        let mut legacy_head = mgr.read_page_from_disk(first_page).unwrap();
        legacy_head.set_merkle_hash(&[0u8; citadel_core::MERKLE_HASH_SIZE]);
        legacy_head.update_checksum();
        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(
            &dek,
            &mac_key,
            first_page,
            slot.encryption_epoch,
            legacy_head.as_bytes(),
            &mut encrypted,
        );
        io.write_page(page_offset(first_page), &encrypted).unwrap();

        // Released 2.0 slots left both the scheme marker and overflow-head
        // digest zero. Re-encode the current generation in that exact shape.
        let mut header = citadel_io::file_manager::read_file_header(&io).unwrap();
        let mut legacy_slot = slot;
        legacy_slot.merkle_scheme = citadel_io::file_manager::MerkleScheme::Legacy;
        legacy_slot.slot_format = citadel_io::file_manager::SlotFormat::Legacy;
        legacy_slot.slot_mac = [0u8; citadel_core::SLOT_MAC_SIZE];
        header.flags &= !citadel_core::HEADER_FLAG_SLOTS_V1;
        header.god_byte = 0;
        header.slots = [legacy_slot.clone(), legacy_slot];
        io.write_at(0, &header.serialize()).unwrap();
    }

    let mgr = TxnManager::open(Box::new(io), dek, mac_key, 1, 256).unwrap();
    assert_eq!(
        mgr.current_slot().merkle_scheme,
        citadel_io::file_manager::MerkleScheme::Legacy
    );
    commit_insert(&mgr, b"later", b"value");
    assert_eq!(
        mgr.current_slot().merkle_root,
        [0u8; citadel_core::MERKLE_HASH_SIZE]
    );
    let mut read = mgr.begin_read();
    assert_eq!(read.get(b"large").unwrap(), Some(value.clone()));
    assert_eq!(read.get(b"later").unwrap(), Some(b"value".to_vec()));
    drop(read);

    let compacted = MemIO::new(4 * 1024 * 1024);
    mgr.compact_to(&compacted).unwrap();
    let compacted_mgr = TxnManager::open(Box::new(compacted), dek, mac_key, 1, 256).unwrap();
    assert_eq!(
        compacted_mgr.current_slot().merkle_scheme,
        citadel_io::file_manager::MerkleScheme::LogicalOverflowV1
    );
    let compacted_slot = compacted_mgr.current_slot();
    let compacted_head = overflow_first_page(&compacted_mgr, compacted_slot.tree_root, b"large");
    assert_eq!(
        compacted_mgr
            .read_page_from_disk(compacted_head)
            .unwrap()
            .merkle_hash(),
        crate::merkle::overflow_payload_hash(&value)
    );
    assert!(compacted_mgr.integrity_check().unwrap().is_ok());
}

#[test]
fn off_to_full_commit_keeps_unknown_when_an_untouched_child_hash_is_unknown() {
    use citadel_io::file_manager::read_commit_slot;

    let (dek, mac_key, dek_id) = test_keys();
    let base = MemIO::new(4 * 1024 * 1024);
    {
        let mgr = TxnManager::create_with_sync(
            Box::new(base.share()),
            dek,
            mac_key,
            1,
            0x1234,
            dek_id,
            256,
            SyncMode::Off,
        )
        .unwrap();
        let mut writer = mgr.begin_write().unwrap();
        for row in 0..200u32 {
            writer
                .insert(format!("k{row:04}").as_bytes(), &[b'x'; 200])
                .unwrap();
        }
        writer.commit().unwrap();
        assert!(mgr.current_slot().tree_depth > 1);
    }

    let fork_a = base.deep_clone();
    let fork_b = base.deep_clone();
    for (fork, divergent) in [(&fork_a, b'A'), (&fork_b, b'B')] {
        let mgr =
            TxnManager::open_with_sync(Box::new(fork.share()), dek, mac_key, 1, 256, SyncMode::Off)
                .unwrap();
        let mut writer = mgr.begin_write().unwrap();
        writer.insert(b"k0000", &[divergent; 200]).unwrap();
        writer.commit().unwrap();
    }

    for fork in [&fork_a, &fork_b] {
        let mgr = TxnManager::open_with_sync(
            Box::new(fork.share()),
            dek,
            mac_key,
            1,
            256,
            SyncMode::Full,
        )
        .unwrap();
        let mut writer = mgr.begin_write().unwrap();
        writer.insert(b"k0199", &[b'z'; 200]).unwrap();
        writer.commit().unwrap();
        assert_eq!(
            mgr.current_slot().merkle_root,
            [0u8; citadel_core::MERKLE_HASH_SIZE],
            "an untouched UNKNOWN child must keep the parent UNKNOWN"
        );
    }

    let mgr_a = TxnManager::open_with_sync(
        Box::new(fork_a.share()),
        dek,
        mac_key,
        1,
        256,
        SyncMode::Full,
    )
    .unwrap();
    let mgr_b = TxnManager::open_with_sync(
        Box::new(fork_b.share()),
        dek,
        mac_key,
        1,
        256,
        SyncMode::Full,
    )
    .unwrap();
    let mut reader_a = mgr_a.begin_read();
    let mut reader_b = mgr_b.begin_read();
    assert_eq!(reader_a.get(b"k0000").unwrap(), Some(vec![b'A'; 200]));
    assert_eq!(reader_b.get(b"k0000").unwrap(), Some(vec![b'B'; 200]));
    assert_eq!(reader_a.get(b"k0199").unwrap(), Some(vec![b'z'; 200]));
    assert_eq!(reader_b.get(b"k0199").unwrap(), Some(vec![b'z'; 200]));
    drop((reader_a, reader_b));

    for fork in [&fork_a, &fork_b] {
        for slot in 0..2 {
            assert_eq!(
                read_commit_slot(fork, slot).unwrap().merkle_root,
                [0u8; citadel_core::MERKLE_HASH_SIZE]
            );
        }
    }
}

/// Regression (commit slot unauthenticated, checksum stopped at byte 76): a
/// pre-v1 file whose slots carry no MAC tail must still open, and the first
/// commit must upgrade the active slot to the authenticated v1 format.
#[test]
fn legacy_slot_file_opens_and_upgrades_on_commit() {
    use citadel_io::file_manager::{
        read_commit_slot, read_god_byte, write_commit_slot, SlotFormat,
    };

    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);

    {
        let mgr =
            TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
        commit_insert(&mgr, b"k1", b"v1");
    }

    // Rewrite both slots in the legacy wire format, as a pre-v1 binary would
    // have produced them (no marker, no MAC; identical [0..76] checksum), and
    // clear the header flag byte, which pre-v1 files carry as zero padding.
    for idx in 0..2 {
        let mut slot = read_commit_slot(&io, idx).unwrap();
        slot.slot_format = SlotFormat::Legacy;
        slot.slot_mac = [0u8; citadel_core::SLOT_MAC_SIZE];
        write_commit_slot(&io, idx, &slot).unwrap();
    }
    io.write_at(citadel_core::HEADER_FLAGS_OFFSET as u64, &[0])
        .unwrap();
    assert_eq!(
        read_commit_slot(&io, 0).unwrap().slot_format,
        SlotFormat::Legacy
    );

    let mgr = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    {
        let mut reader = mgr.begin_read();
        assert_eq!(reader.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    }
    commit_insert(&mgr, b"k2", b"v2");

    let active = (read_god_byte(&io).unwrap() & citadel_core::GOD_BIT_ACTIVE_SLOT) as usize;
    let slot = read_commit_slot(&io, active).unwrap();
    assert_eq!(slot.slot_format, SlotFormat::V1, "commit must reseal as v1");
    assert!(slot.verify_mac(&mac_key));
    drop(mgr);

    // One physical slot is still legacy, so the one-way flag must wait.
    let mgr = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    {
        let mut reader = mgr.begin_read();
        assert_eq!(reader.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    }
    assert_eq!(
        citadel_io::file_manager::read_header_flags(&io).unwrap()
            & citadel_core::HEADER_FLAG_SLOTS_V1,
        0,
        "flag must not be set while a legacy slot remains"
    );
    commit_insert(&mgr, b"k3", b"v3");
    drop(mgr);

    // Both slots are V1 now; reopening stamps the flag one-way.
    let mgr = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    assert_ne!(
        citadel_io::file_manager::read_header_flags(&io).unwrap()
            & citadel_core::HEADER_FLAG_SLOTS_V1,
        0,
        "flag must be set once both slots reseal as v1"
    );
    let mut reader = mgr.begin_read();
    assert_eq!(reader.get(b"k3").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn authenticated_v1_requirement_survives_a_cleared_data_header_flag() {
    use citadel_io::file_manager::{read_commit_slot, write_commit_slot, SlotFormat};

    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    {
        let mgr =
            TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
        commit_insert(&mgr, b"k", b"v");
    }

    // Reproduce the whole downgrade: clear the mutable data-header bit and
    // re-encode both slots with fresh, valid keyless checksums.
    io.write_at(citadel_core::HEADER_FLAGS_OFFSET as u64, &[0])
        .unwrap();
    for idx in 0..2 {
        let mut slot = read_commit_slot(&io, idx).unwrap();
        slot.slot_format = SlotFormat::Legacy;
        slot.slot_mac = [0u8; citadel_core::SLOT_MAC_SIZE];
        write_commit_slot(&io, idx, &slot).unwrap();
    }

    // Lower-level callers with no authenticated sidecar retain legacy
    // compatibility. The facade path must fail and must not retry this path.
    let direct = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    drop(direct);
    assert!(matches!(
        TxnManager::open_with_v1_requirement(Box::new(io.share()), dek, mac_key, 1, 256, true,),
        Err(citadel_core::Error::SlotDowngradeDetected)
    ));
}

#[test]
fn authenticated_requirement_refuses_a_v1_looking_corrupt_slot() {
    use citadel_io::file_manager::{read_commit_slot, write_commit_slot, SlotFormat};

    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let mgr =
        TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();

    let mut slot = read_commit_slot(&io, 1).unwrap();
    assert_eq!(slot.slot_format, SlotFormat::V1);
    slot.slot_mac[0] ^= 1;
    write_commit_slot(&io, 1, &slot).unwrap();

    let exclusion = mgr.exclude_writers().unwrap();
    assert!(!exclusion.both_slots_v1().unwrap());
    assert!(matches!(
        exclusion.require_authenticated_v1(),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));
}

#[test]
fn slot_metadata_guard_excludes_writes_until_drop() {
    let mgr = create_test_manager();

    let exclusion = mgr.exclude_writers().unwrap();
    assert!(matches!(
        mgr.begin_write(),
        Err(citadel_core::Error::WriteTransactionActive)
    ));
    assert!(exclusion.both_slots_v1().unwrap());

    drop(exclusion);
    let txn = mgr.begin_write().unwrap();
    txn.abort();
}

/// The upgrade primitive reseals a heavy legacy file (more named tables than
/// the slot capacity, every carried entry blanket-stale) as V1 in exactly
/// two commits: refresh_all_catalog_descriptors clears staleness for every
/// table, and mark_slots_v1 stamps the flag in-process.
#[test]
fn upgrade_reseals_heavy_legacy_file_in_two_commits() {
    use citadel_io::file_manager::{
        read_commit_slot, read_header_flags, table_name_hash, write_commit_slot, SlotFormat,
    };

    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(2 * 1024 * 1024);

    {
        let mgr =
            TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
        let mut txn = mgr.begin_write().unwrap();
        for i in 1..=8u8 {
            txn.create_table(format!("t{i}").as_bytes()).unwrap();
        }
        txn.insert(b"k1", b"v1").unwrap();
        txn.commit().unwrap();
    }

    // Rewrite both slots as a released binary would have: legacy layout, no
    // MAC, and a full 7-entry table (real roots from the catalog), then
    // clear the flag byte.
    let mgr = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    let tables = mgr.list_tables().unwrap();
    assert_eq!(tables.len(), 8);
    let entries: Vec<(u32, u64, u32, u16)> = tables
        .iter()
        .take(citadel_core::SLOT_NAMED_MAX_ENTRIES)
        .map(|(name, desc)| {
            (
                table_name_hash(name),
                desc.entry_count,
                desc.root_page.as_u32(),
                desc.depth,
            )
        })
        .collect();
    drop(mgr);
    for idx in 0..2 {
        let mut slot = read_commit_slot(&io, idx).unwrap();
        slot.named_table_entries = entries.clone();
        slot.slot_format = SlotFormat::Legacy;
        slot.slot_mac = [0u8; citadel_core::SLOT_MAC_SIZE];
        write_commit_slot(&io, idx, &slot).unwrap();
    }
    io.write_at(citadel_core::HEADER_FLAGS_OFFSET as u64, &[0])
        .unwrap();

    // Ordinary commits cannot reseal this file (7 blanket-stale carried
    // entries exceed the V1 capacity); the upgrade primitive must.
    let mgr = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    let names: Vec<Vec<u8>> = mgr
        .list_tables()
        .unwrap()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    for _ in 0..2 {
        let mut txn = mgr.begin_write().unwrap();
        txn.refresh_all_catalog_descriptors(&names).unwrap();
        txn.commit().unwrap();
    }
    let exclusion = mgr.exclude_writers().unwrap();
    assert!(
        exclusion.mark_slots_v1().unwrap(),
        "flag must stamp in-process"
    );
    drop(exclusion);
    drop(mgr);

    for idx in 0..2 {
        let slot = read_commit_slot(&io, idx).unwrap();
        assert_eq!(slot.slot_format, SlotFormat::V1);
        assert!(slot.verify_mac(&mac_key));
    }
    assert_ne!(
        read_header_flags(&io).unwrap() & citadel_core::HEADER_FLAG_SLOTS_V1,
        0
    );

    // Everything survives on the now-flagged file.
    let mgr = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    assert_eq!(mgr.list_tables().unwrap().len(), 8);
    let mut reader = mgr.begin_read();
    assert_eq!(reader.get(b"k1").unwrap(), Some(b"v1".to_vec()));
}

/// A renamed-away table's slot entry must be dropped on the rename commit:
/// carrying it forever lets ensure_table's slot fast path resurrect the old
/// name as an alias of the renamed tree.
#[test]
fn renamed_table_old_name_does_not_resurrect() {
    use citadel_io::file_manager::{read_commit_slot, read_god_byte, table_name_hash};

    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let mgr =
        TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();

    let mut txn = mgr.begin_write().unwrap();
    txn.create_table(b"t").unwrap();
    txn.table_insert(b"t", b"k", b"v").unwrap();
    txn.commit().unwrap();

    let mut txn = mgr.begin_write().unwrap();
    txn.rename_table(b"t", b"u").unwrap();
    txn.commit().unwrap();

    let active = (read_god_byte(&io).unwrap() & citadel_core::GOD_BIT_ACTIVE_SLOT) as usize;
    let slot = read_commit_slot(&io, active).unwrap();
    assert!(
        !slot
            .named_table_entries
            .iter()
            .any(|&(h, ..)| h == table_name_hash(b"t")),
        "renamed-away name must not ride the slot"
    );

    let mut txn = mgr.begin_write().unwrap();
    assert!(matches!(
        txn.table_get(b"t", b"k"),
        Err(Error::TableNotFound(_))
    ));
    assert_eq!(txn.table_get(b"u", b"k").unwrap(), Some(b"v".to_vec()));
    txn.abort();
}

/// Off-mode catalog-skip commits make the slot entry the sole record of a
/// table's current root; backup and compact must both carry those rows
/// instead of trusting the stale catalog descriptor.
#[test]
fn off_mode_backup_and_compact_carry_slot_entry_roots() {
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(4 * 1024 * 1024);
    let mgr = TxnManager::create_with_sync(
        Box::new(io.share()),
        dek,
        mac_key,
        1,
        0x1234,
        dek_id,
        256,
        SyncMode::Off,
    )
    .unwrap();

    let mut txn = mgr.begin_write().unwrap();
    txn.create_table(b"t").unwrap();
    txn.table_insert(b"t", b"a", b"row-a").unwrap();
    txn.commit().unwrap();

    // Data-only commit: Off mode skips the catalog, so the new root lives
    // only in the (stale-flagged) slot entry.
    let mut txn = mgr.begin_write().unwrap();
    txn.table_insert(b"t", b"b", b"row-b").unwrap();
    txn.commit().unwrap();

    let backup = MemIO::new(4 * 1024 * 1024);
    mgr.backup_to(&backup).unwrap();
    let bmgr = TxnManager::open(Box::new(backup), dek, mac_key, 1, 256).unwrap();
    let mut txn = bmgr.begin_write().unwrap();
    assert_eq!(txn.table_get(b"t", b"a").unwrap(), Some(b"row-a".to_vec()));
    assert_eq!(
        txn.table_get(b"t", b"b").unwrap(),
        Some(b"row-b".to_vec()),
        "backup must carry the slot-entry root's rows"
    );
    txn.abort();
    drop(bmgr);

    let compacted = MemIO::new(4 * 1024 * 1024);
    mgr.compact_to(&compacted).unwrap();
    let cmgr = TxnManager::open(Box::new(compacted), dek, mac_key, 1, 256).unwrap();
    let mut txn = cmgr.begin_write().unwrap();
    assert_eq!(txn.table_get(b"t", b"a").unwrap(), Some(b"row-a".to_vec()));
    assert_eq!(
        txn.table_get(b"t", b"b").unwrap(),
        Some(b"row-b".to_vec()),
        "compacted catalog must be rewritten from the slot entry"
    );
    txn.abort();
}

#[test]
fn compaction_preserves_a_slot_only_named_root() {
    use citadel_io::file_manager::{
        read_commit_slot, read_god_byte, table_name_hash, write_commit_slot,
    };

    let (dek, mac_key, dek_id) = test_keys();
    let source = MemIO::new(4 * 1024 * 1024);
    let mgr = TxnManager::create(
        Box::new(source.share()),
        dek,
        mac_key,
        1,
        0x1234,
        dek_id,
        256,
    )
    .unwrap();

    let mut txn = mgr.begin_write().unwrap();
    txn.create_table(b"slot-only").unwrap();
    txn.table_insert(b"slot-only", b"row", b"value").unwrap();
    txn.commit().unwrap();

    // Model a valid sole-record slot entry: the catalog has no descriptor,
    // but the authenticated slot still carries the current root and count.
    let active = (read_god_byte(&source).unwrap() & citadel_core::GOD_BIT_ACTIVE_SLOT) as usize;
    let mut slot = read_commit_slot(&source, active).unwrap();
    let table_hash = table_name_hash(b"slot-only");
    let entry = slot
        .named_table_entries
        .iter_mut()
        .find(|entry| entry.0 == table_hash)
        .unwrap();
    entry.1 |= SLOT_ENTRY_STALE;
    let expected_count = entry.1 & !SLOT_ENTRY_STALE;
    let expected_depth = entry.3;
    slot.catalog_root = PageId::INVALID;
    slot.seal(&mac_key);
    write_commit_slot(&source, active, &slot).unwrap();
    drop(mgr);

    let mgr = TxnManager::open(Box::new(source.share()), dek, mac_key, 1, 256).unwrap();
    let source_report = mgr.integrity_check().unwrap();
    assert!(
        source_report.is_ok(),
        "slot-only source must be a walkable durable root: {:?}",
        source_report.errors
    );

    let compacted = MemIO::new(4 * 1024 * 1024);
    mgr.compact_to(&compacted).unwrap();
    let compacted_mgr = TxnManager::open(Box::new(compacted), dek, mac_key, 1, 256).unwrap();
    let compacted_slot = compacted_mgr.current_slot();
    let (root, depth) = compacted_slot
        .named_entry_root(b"slot-only")
        .expect("compaction must retain the slot-only root locator");
    assert_eq!(depth, expected_depth);
    assert_eq!(
        compacted_slot.named_entry_count(b"slot-only"),
        Some(expected_count)
    );
    let compacted_entry = compacted_slot
        .named_table_entries
        .iter()
        .find(|entry| entry.0 == table_hash)
        .unwrap();
    assert_ne!(compacted_entry.1 & SLOT_ENTRY_STALE, 0);

    let page = compacted_mgr.read_page_from_disk(root).unwrap();
    let index = citadel_page::leaf_node::search(&page, b"row").unwrap();
    assert_eq!(
        citadel_page::leaf_node::read_cell(&page, index).value,
        b"value"
    );
    let report = compacted_mgr.integrity_check().unwrap();
    assert!(report.is_ok(), "compacted errors: {:?}", report.errors);
}

#[test]
fn a_pending_free_cycle_refuses_the_next_commit() {
    assert_pending_fixture_refuses_commit("cycle", |root, txn_id| {
        let second = PageId(root.as_u32() + 1);
        let mut first_page = Page::new(root, PageType::PendingFree, txn_id);
        first_page.set_right_child(second);
        first_page.update_checksum();
        let mut second_page = Page::new(second, PageType::PendingFree, txn_id);
        second_page.set_right_child(root);
        second_page.update_checksum();
        (
            vec![(root, first_page), (second, second_page)],
            second.as_u32() + 1,
        )
    });
}

#[test]
fn an_oversized_pending_free_entry_count_refuses_the_next_commit() {
    assert_pending_fixture_refuses_commit("oversized count", |root, txn_id| {
        let mut page = Page::new(root, PageType::PendingFree, txn_id);
        let count = (pending_free::MAX_ENTRIES_PER_PAGE as u32) + 1;
        page.data[citadel_core::PAGE_HEADER_SIZE..citadel_core::PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&count.to_le_bytes());
        page.update_checksum();
        (vec![(root, page)], root.as_u32() + 1)
    });
}

#[test]
fn a_wrong_type_or_duplicate_pending_free_entry_refuses_the_next_commit() {
    assert_pending_fixture_refuses_commit("wrong page type", |root, txn_id| {
        let mut page = Page::new(root, PageType::Leaf, txn_id);
        page.update_checksum();
        (vec![(root, page)], root.as_u32() + 1)
    });

    assert_pending_fixture_refuses_commit("duplicate on one page", |root, txn_id| {
        let free_page = PageId(root.as_u32() + 1);
        let mut page = Page::new(root, PageType::PendingFree, txn_id);
        page.data[citadel_core::PAGE_HEADER_SIZE..citadel_core::PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&2u32.to_le_bytes());
        for index in 0..2 {
            let offset =
                citadel_core::PAGE_HEADER_SIZE + 4 + index * citadel_core::PENDING_FREE_ENTRY_SIZE;
            page.data[offset..offset + 4].copy_from_slice(&free_page.as_u32().to_le_bytes());
            page.data[offset + 4..offset + 12].copy_from_slice(&txn_id.as_u64().to_le_bytes());
        }
        page.update_checksum();
        (vec![(root, page)], free_page.as_u32() + 1)
    });
}

fn pending_fixture_page(
    page_id: PageId,
    txn_id: TxnId,
    entries: &[(PageId, TxnId)],
    next: PageId,
) -> Page {
    assert!(entries.len() <= pending_free::MAX_ENTRIES_PER_PAGE);
    let mut page = Page::new(page_id, PageType::PendingFree, txn_id);
    page.data[citadel_core::PAGE_HEADER_SIZE..citadel_core::PAGE_HEADER_SIZE + 4]
        .copy_from_slice(&(entries.len() as u32).to_le_bytes());
    for (index, (entry, freed_at)) in entries.iter().enumerate() {
        let offset =
            citadel_core::PAGE_HEADER_SIZE + 4 + index * citadel_core::PENDING_FREE_ENTRY_SIZE;
        page.data[offset..offset + 4].copy_from_slice(&entry.as_u32().to_le_bytes());
        page.data[offset + 4..offset + 12].copy_from_slice(&freed_at.as_u64().to_le_bytes());
    }
    page.set_right_child(next);
    page.update_checksum();
    page
}

#[test]
fn pending_free_metadata_validation_precedes_commit_io() {
    #[derive(Clone, Copy, Debug)]
    enum Damage {
        CrossPageDuplicate,
        EntryNamesLaterChainPage,
        InvalidEntry,
        EntryAtHighWater,
        NextAtHighWater,
        ZeroFreedAt,
        FutureFreedAt,
        FuturePageTxn,
        WrongEmbeddedPageId,
    }

    for damage in [
        Damage::CrossPageDuplicate,
        Damage::EntryNamesLaterChainPage,
        Damage::InvalidEntry,
        Damage::EntryAtHighWater,
        Damage::NextAtHighWater,
        Damage::ZeroFreedAt,
        Damage::FutureFreedAt,
        Damage::FuturePageTxn,
        Damage::WrongEmbeddedPageId,
    ] {
        let mut expected_bounds = None;
        let error =
            assert_pending_fixture_refuses_commit(&format!("{damage:?}"), |root, txn_id| {
                let second = PageId(root.as_u32() + 1);
                let free = PageId(root.as_u32() + 2);
                let mut high_water_mark = free.as_u32() + 1;
                let mut entries = vec![(free, txn_id)];
                let mut next = PageId::INVALID;
                let mut embedded_id = root;
                let mut page_txn = txn_id;
                let mut tail_entries = Vec::new();
                match damage {
                    Damage::CrossPageDuplicate => {
                        next = second;
                        tail_entries.push((free, txn_id));
                    }
                    Damage::EntryNamesLaterChainPage => {
                        next = second;
                        entries[0].0 = second;
                    }
                    Damage::InvalidEntry => entries[0].0 = PageId::INVALID,
                    Damage::EntryAtHighWater => entries[0].0 = PageId(high_water_mark),
                    Damage::NextAtHighWater => {
                        entries.clear();
                        next = second;
                        high_water_mark = second.as_u32();
                        expected_bounds = Some(second);
                    }
                    Damage::ZeroFreedAt => entries[0].1 = TxnId::ZERO,
                    Damage::FutureFreedAt => entries[0].1 = TxnId(txn_id.as_u64() + 1),
                    Damage::FuturePageTxn => page_txn = TxnId(txn_id.as_u64() + 1),
                    Damage::WrongEmbeddedPageId => embedded_id = second,
                }
                let first = pending_fixture_page(embedded_id, page_txn, &entries, next);
                let mut pages = vec![(root, first)];
                if next.is_valid() {
                    // The out-of-bounds case is physically present and authenticated;
                    // only the slot's high-water check may reject it.
                    let tail = pending_fixture_page(second, txn_id, &tail_entries, PageId::INVALID);
                    pages.push((second, tail));
                }
                (pages, high_water_mark)
            });
        if let Some(expected) = expected_bounds {
            assert!(
                matches!(error, Error::PageOutOfBounds(actual) if actual == expected),
                "{damage:?}: expected bounds rejection at {expected}, got {error:?}"
            );
        }
    }
}

#[test]
fn healthy_pending_free_fixture_records_commit_writes() {
    let (mgr, io, recorder) = reopen_with_pending_fixture(|root, txn_id| {
        let free = PageId(root.as_u32() + 1);
        let page = pending_fixture_page(root, txn_id, &[(free, txn_id)], PageId::INVALID);
        (vec![(root, page)], free.as_u32() + 1)
    });
    let generation_before = mgr.commit_generation();
    let mut txn = mgr.begin_write().unwrap();
    txn.insert(b"after", b"value").unwrap();
    recorder.arm();
    txn.commit().unwrap();
    let effects = recorder.effects();
    assert!(
        effects.page_writes > 0,
        "data/chain writes were not observed"
    );
    assert!(
        effects.metadata_writes > 0,
        "metadata writes were not observed"
    );
    assert_eq!(mgr.commit_generation(), generation_before + 1);
    drop(mgr);

    let (dek, mac_key, _) = test_keys();
    let reopened = TxnManager::open(Box::new(io), dek, mac_key, 1, 256).unwrap();
    let mut reader = reopened.begin_read();
    for key in [b"seed".as_slice(), b"after".as_slice()] {
        assert_eq!(
            reader.get(key).unwrap().as_deref(),
            Some(b"value".as_slice())
        );
    }
}

#[test]
fn off_mode_named_root_at_page_zero_survives_reopen_backup_and_compaction() {
    let (dek, mac_key, dek_id) = test_keys();
    let source = MemIO::new(4 * 1024 * 1024);
    let mgr = TxnManager::create_with_sync(
        Box::new(source.share()),
        dek,
        mac_key,
        1,
        0x1234,
        dek_id,
        256,
        SyncMode::Off,
    )
    .unwrap();

    // The first default-tree CoW retires the initial page-zero root. The
    // second commit advances the other recovery slot, making page zero the
    // only reclaimed page available to the next transaction.
    let mut txn = mgr.begin_write().unwrap();
    txn.create_table(b"root_zero").unwrap();
    txn.table_insert(b"root_zero", b"k", b"v1").unwrap();
    txn.insert(b"seed", b"one").unwrap();
    txn.commit().unwrap();

    let mut txn = mgr.begin_write().unwrap();
    txn.insert(b"seed", b"two").unwrap();
    txn.commit().unwrap();

    // Only the named leaf moves. In Off mode its same-depth catalog update is
    // skipped, so the slot cache is the sole durable record of root page zero.
    let mut txn = mgr.begin_write().unwrap();
    txn.table_insert(b"root_zero", b"k", b"v2").unwrap();
    txn.commit().unwrap();
    let (cached_root, cached_depth) = mgr
        .current_slot()
        .named_entry_root(b"root_zero")
        .expect("page zero with nonzero depth is a populated cache entry");
    assert_eq!(cached_root, PageId(0));
    assert!(cached_depth >= 1);

    let backup = MemIO::new(4 * 1024 * 1024);
    mgr.backup_to(&backup).unwrap();
    let compacted = MemIO::new(4 * 1024 * 1024);
    mgr.compact_to(&compacted).unwrap();
    drop(mgr);

    for copy in [source, backup, compacted] {
        let reopened =
            TxnManager::open_with_sync(Box::new(copy), dek, mac_key, 1, 256, SyncMode::Off)
                .unwrap();
        let mut reader = reopened.begin_read();
        assert_eq!(
            reader.table_get(b"root_zero", b"k").unwrap(),
            Some(b"v2".to_vec())
        );
    }
}

#[test]
fn backup_copies_default_and_named_overflow_chains() {
    let (dek, mac_key, dek_id) = test_keys();
    let source = MemIO::new(4 * 1024 * 1024);
    let mgr = TxnManager::create(
        Box::new(source.share()),
        dek,
        mac_key,
        1,
        0x1234,
        dek_id,
        256,
    )
    .unwrap();
    let (default_value, named_value) = insert_overflow_fixture(&mgr);

    let backup = MemIO::new(4 * 1024 * 1024);
    mgr.backup_to(&backup).unwrap();
    let backup_mgr = TxnManager::open(Box::new(backup), dek, mac_key, 1, 256).unwrap();

    assert_overflow_fixture(&backup_mgr, &default_value, &named_value);
    let report = backup_mgr.integrity_check().unwrap();
    assert!(
        report.is_ok(),
        "backup integrity errors: {:?}",
        report.errors
    );
}

#[test]
fn backup_and_compaction_refuse_an_overlapping_writer() {
    let mgr = create_test_manager();
    let _writer = mgr.begin_write().unwrap();
    let backup = MemIO::new(1024 * 1024);
    let compacted = MemIO::new(1024 * 1024);

    assert!(matches!(
        mgr.backup_to(&backup),
        Err(Error::WriteTransactionActive)
    ));
    assert!(matches!(
        mgr.compact_to(&compacted),
        Err(Error::WriteTransactionActive)
    ));
}

#[test]
fn same_overflow_page_ids_with_different_payloads_have_different_merkle_roots() {
    let (dek, mac_key, dek_id) = test_keys();
    let base = MemIO::new(4 * 1024 * 1024);
    {
        let mgr = TxnManager::create(Box::new(base.share()), dek, mac_key, 1, 0x1234, dek_id, 256)
            .unwrap();
        commit_insert(&mgr, b"large", &[b'x'; 20_000]);
    }

    let fork_a = base.deep_clone();
    let fork_b = base.deep_clone();
    for (fork, byte) in [(&fork_a, b'A'), (&fork_b, b'B')] {
        let mgr = TxnManager::open(Box::new(fork.share()), dek, mac_key, 1, 256).unwrap();
        commit_insert(&mgr, b"large", &[byte; 20_000]);
    }

    let mgr_a = TxnManager::open(Box::new(fork_a.share()), dek, mac_key, 1, 256).unwrap();
    let mgr_b = TxnManager::open(Box::new(fork_b.share()), dek, mac_key, 1, 256).unwrap();
    let slot_a = mgr_a.current_slot();
    let slot_b = mgr_b.current_slot();
    let head_a = overflow_first_page(&mgr_a, slot_a.tree_root, b"large");
    let head_b = overflow_first_page(&mgr_b, slot_b.tree_root, b"large");
    assert_eq!(
        head_a, head_b,
        "cloned allocators must exercise identical physical references"
    );
    assert_ne!(
        mgr_a.read_page_from_disk(head_a).unwrap().merkle_hash(),
        mgr_b.read_page_from_disk(head_b).unwrap().merkle_hash(),
        "the overflow head authenticates the logical payload"
    );
    assert_ne!(
        slot_a.merkle_root, slot_b.merkle_root,
        "same-length divergent payloads must never be sync-pruned"
    );
}

#[test]
fn compact_rewrites_overflow_links_and_preserves_logical_tree_hashes() {
    let (dek, mac_key, dek_id) = test_keys();
    let source = MemIO::new(4 * 1024 * 1024);
    let mgr = TxnManager::create(
        Box::new(source.share()),
        dek,
        mac_key,
        1,
        0x1234,
        dek_id,
        256,
    )
    .unwrap();
    let (default_value, named_value) = insert_overflow_fixture(&mgr);
    let source_slot = mgr.current_slot();
    let source_named_root = mgr.table_root(OVERFLOW_TABLE).unwrap().unwrap();
    let source_named_hash = mgr
        .read_page_from_disk(source_named_root)
        .unwrap()
        .merkle_hash();
    let source_default_overflow =
        overflow_first_page(&mgr, source_slot.tree_root, OVERFLOW_DEFAULT_KEY);
    let source_named_overflow = overflow_first_page(&mgr, source_named_root, OVERFLOW_NAMED_KEY);

    let compacted = MemIO::new(4 * 1024 * 1024);
    mgr.compact_to(&compacted).unwrap();
    let compacted_mgr = TxnManager::open(Box::new(compacted), dek, mac_key, 1, 256).unwrap();
    assert_overflow_fixture(&compacted_mgr, &default_value, &named_value);

    let compacted_slot = compacted_mgr.current_slot();
    let compacted_named_root = compacted_mgr.table_root(OVERFLOW_TABLE).unwrap().unwrap();
    let compacted_default_overflow = overflow_first_page(
        &compacted_mgr,
        compacted_slot.tree_root,
        OVERFLOW_DEFAULT_KEY,
    );
    let compacted_named_overflow =
        overflow_first_page(&compacted_mgr, compacted_named_root, OVERFLOW_NAMED_KEY);
    assert_ne!(source_default_overflow, compacted_default_overflow);
    assert_ne!(source_named_overflow, compacted_named_overflow);

    let root_page = compacted_mgr
        .read_page_from_disk(compacted_slot.tree_root)
        .unwrap();
    assert_ne!(
        compacted_slot.merkle_root,
        [0u8; citadel_core::MERKLE_HASH_SIZE]
    );
    assert_eq!(
        compacted_slot.merkle_root, source_slot.merkle_root,
        "physical overflow remapping must not change the logical tree digest"
    );
    assert_eq!(compacted_slot.merkle_root, root_page.merkle_hash());
    assert_eq!(
        compacted_mgr
            .read_page_from_disk(compacted_named_root)
            .unwrap()
            .merkle_hash(),
        source_named_hash,
        "named-tree hashes must use the same layout-independent overflow digest"
    );
    let report = compacted_mgr.integrity_check().unwrap();
    assert!(
        report.is_ok(),
        "compacted integrity errors: {:?}",
        report.errors
    );
}

/// The forced-commit path reseals a legacy file with NO named tables: a
/// refresh over an empty name list still rewrites both slots.
#[test]
fn upgrade_reseals_legacy_file_without_tables() {
    use citadel_io::file_manager::{
        read_commit_slot, read_header_flags, write_commit_slot, SlotFormat,
    };

    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);

    {
        let mgr =
            TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
        commit_insert(&mgr, b"k1", b"v1");
    }
    for idx in 0..2 {
        let mut slot = read_commit_slot(&io, idx).unwrap();
        slot.slot_format = SlotFormat::Legacy;
        slot.slot_mac = [0u8; citadel_core::SLOT_MAC_SIZE];
        write_commit_slot(&io, idx, &slot).unwrap();
    }
    io.write_at(citadel_core::HEADER_FLAGS_OFFSET as u64, &[0])
        .unwrap();

    let mgr = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    for _ in 0..2 {
        let mut txn = mgr.begin_write().unwrap();
        txn.refresh_all_catalog_descriptors(&[]).unwrap();
        txn.commit().unwrap();
    }
    let exclusion = mgr.exclude_writers().unwrap();
    assert!(exclusion.mark_slots_v1().unwrap());
    drop(exclusion);
    drop(mgr);

    for idx in 0..2 {
        let slot = read_commit_slot(&io, idx).unwrap();
        assert_eq!(slot.slot_format, SlotFormat::V1);
        assert!(slot.verify_mac(&mac_key));
    }
    assert_ne!(
        read_header_flags(&io).unwrap() & citadel_core::HEADER_FLAG_SLOTS_V1,
        0
    );

    let mgr = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    let mut reader = mgr.begin_read();
    assert_eq!(reader.get(b"k1").unwrap(), Some(b"v1".to_vec()));
}

const MEASURE_TABLE: &[u8] = b"scan_measurement_rows";

fn manager_with_scan_rows(rows: u32) -> TxnManager {
    let mgr = create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(MEASURE_TABLE).unwrap();
    for row in 0..rows {
        writer
            .table_insert(MEASURE_TABLE, &row.to_be_bytes(), b"value")
            .unwrap();
    }
    writer.commit().unwrap();
    mgr
}

fn scan_one_row(mgr: &TxnManager) {
    let mut reader = mgr.begin_read();
    reader
        .table_scan_from_fast(MEASURE_TABLE, b"", |_, _| Ok::<bool, Error>(false))
        .unwrap();
}

#[test]
fn scan_measurements_nest_and_remain_manager_local() {
    let mgr = manager_with_scan_rows(8);
    let other = manager_with_scan_rows(8);
    let telemetry_before = mgr.rows_scanned();

    let outer = mgr.measure_scans();
    scan_one_row(&other);
    assert_eq!(
        outer.rows_scanned(),
        0,
        "another manager leaked into the span"
    );

    scan_one_row(&mgr);
    let inner = mgr.measure_scans();
    scan_one_row(&mgr);
    assert_eq!(inner.rows_scanned(), 1);
    drop(inner);
    scan_one_row(&mgr);

    assert_eq!(outer.rows_scanned(), 3);
    assert_eq!(mgr.rows_scanned() - telemetry_before, 3);
}

#[test]
fn scan_measurements_do_not_include_another_threads_work() {
    let mgr = Arc::new(manager_with_scan_rows(8));
    let measurement = mgr.measure_scans();

    std::thread::scope(|scope| {
        scope
            .spawn(|| scan_one_row(&mgr))
            .join()
            .expect("worker scan panicked");
    });
    assert_eq!(
        measurement.rows_scanned(),
        0,
        "another thread's scan leaked into this operation"
    );

    scan_one_row(&mgr);
    assert_eq!(measurement.rows_scanned(), 1);
}

#[test]
fn scan_measurements_follow_leaf_shards_to_worker_threads() {
    const ROWS: u32 = 4_000;
    let mgr = manager_with_scan_rows(ROWS);
    let telemetry_before = mgr.rows_scanned();
    let mut reader = mgr.begin_read();
    let measurement = reader.measure_scans();
    let leaves = reader.collect_table_leaves(MEASURE_TABLE).unwrap();
    assert!(leaves.len() > 1, "test did not create multiple leaf shards");

    let reader = &reader;
    let leaves_per_shard = leaves.len().div_ceil(4);
    std::thread::scope(|scope| {
        let workers: Vec<_> = leaves
            .chunks(leaves_per_shard)
            .map(|shard| {
                scope.spawn(move || {
                    // Constructed on the worker, matching the SQL parallel
                    // aggregate path rather than inheriting worker TLS.
                    let mut scanner = reader.shard_scanner();
                    scanner.scan_leaves(shard, |_, _| true).unwrap();
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("leaf shard panicked");
        }
    });

    assert_eq!(measurement.rows_scanned(), u64::from(ROWS));
    assert_eq!(mgr.rows_scanned() - telemetry_before, u64::from(ROWS));
}

#[test]
fn scan_measurements_follow_direct_read_scans_to_worker_threads() {
    let mgr = manager_with_scan_rows(8);
    let mut reader = mgr.begin_read();
    let measurement = reader.measure_scans();

    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                reader
                    .table_scan_from_fast(MEASURE_TABLE, b"", |_, _| Ok::<bool, Error>(false))
                    .unwrap();
            })
            .join()
            .expect("worker scan panicked");
    });

    assert_eq!(
        measurement.rows_scanned(),
        1,
        "the transaction's inherited measurement was lost on worker handoff"
    );
}

#[test]
fn scan_measurements_flush_iterator_early_error_and_cancel_exits() {
    let mgr = manager_with_scan_rows(8);

    let early = mgr.measure_scans();
    {
        let mut reader = mgr.begin_read();
        let mut iter = reader.table_scan_iter(MEASURE_TABLE, b"").unwrap();
        assert!(iter.next().unwrap().is_some());
        // Dropping before exhaustion is the iterator's ordinary early exit.
    }
    assert_eq!(early.rows_scanned(), 1);
    drop(early);

    let callback_error = mgr.measure_scans();
    let mut reader = mgr.begin_read();
    let error = reader
        .table_scan_from_fast(MEASURE_TABLE, b"", |_, _| {
            Err::<bool, Error>(Error::Sync("injected callback failure".into()))
        })
        .unwrap_err();
    assert!(matches!(error, Error::Sync(_)));
    assert_eq!(callback_error.rows_scanned(), 1);
    drop(callback_error);

    let cancelled = mgr.measure_scans();
    let token = citadel_core::CancelToken::new();
    let mut reader = mgr.begin_read();
    reader.set_cancel(Some(token.clone()));
    {
        let mut iter = reader.table_scan_iter(MEASURE_TABLE, b"").unwrap();
        assert!(iter.next().unwrap().is_some());
        token.cancel();
        assert!(matches!(iter.next(), Err(Error::Interrupted)));
    }
    assert_eq!(cancelled.rows_scanned(), 1);
}

#[test]
fn iterator_drop_does_not_charge_a_measurement_opened_after_the_scan() {
    let mgr = manager_with_scan_rows(8);
    let mut reader = mgr.begin_read();
    let mut iter = reader.table_scan_iter(MEASURE_TABLE, b"").unwrap();
    assert!(iter.next().unwrap().is_some());

    let later = mgr.measure_scans();
    drop(iter);
    assert_eq!(
        later.rows_scanned(),
        0,
        "iterator drop charged a span that did not exist when the scan began"
    );
}

#[test]
fn iterator_drop_flushes_to_the_measurement_captured_at_construction() {
    use std::sync::atomic::Ordering;

    let mgr = manager_with_scan_rows(8);
    let measurement = mgr.measure_scans();
    let captured = measurement.weak_counter();
    let mut reader = mgr.begin_read();
    let mut iter = reader.table_scan_iter(MEASURE_TABLE, b"").unwrap();
    assert!(iter.next().unwrap().is_some());

    drop(measurement);
    let counter = captured
        .upgrade()
        .expect("the iterator must retain its captured measurement counter");
    drop(iter);
    assert_eq!(
        counter.load(Ordering::Relaxed),
        1,
        "iterator drop lost the span that was active at construction"
    );
}

#[test]
fn catalog_readers_reject_an_authenticated_malformed_page_without_panicking() {
    let mgr = create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"catalog-entry").unwrap();
    writer.commit().unwrap();

    let catalog_root = mgr.current_slot().catalog_root;
    let mut malformed = mgr.fetch_page_owned(catalog_root).unwrap();
    malformed.set_num_cells(u16::MAX);
    malformed.update_checksum();

    let mut encrypted = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        &mgr.dek,
        &mgr.mac_key,
        catalog_root,
        mgr.epoch,
        malformed.as_bytes(),
        &mut encrypted,
    );
    mgr.io
        .write_page(page_offset(catalog_root), &encrypted)
        .unwrap();
    mgr.pool.lock().invalidate(catalog_root);

    let root = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        mgr.begin_read().table_root_page(b"catalog-entry")
    }));
    assert!(root.is_ok(), "catalog lookup must not unwind");
    assert!(matches!(root.unwrap(), Err(Error::DatabaseCorrupted)));

    let listed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        mgr.begin_read().list_tables()
    }));
    assert!(listed.is_ok(), "catalog decoding must not unwind");
    assert!(matches!(listed.unwrap(), Err(Error::DatabaseCorrupted)));
}

#[test]
fn catalog_lookup_rejects_an_authenticated_cross_page_cycle() {
    let mgr = create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"catalog-entry").unwrap();
    writer.commit().unwrap();

    let slot = mgr.current_slot();
    let catalog_root = slot.catalog_root;
    let second_page = slot.tree_root;
    assert_ne!(catalog_root, second_page);

    let rewrite_as_branch = |page_id, right_child| {
        let mut page = mgr.fetch_page_owned(page_id).unwrap();
        page.set_page_type(PageType::Branch);
        page.rebuild_cells(&[]);
        page.set_right_child(right_child);
        page.update_checksum();

        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(
            &mgr.dek,
            &mgr.mac_key,
            page_id,
            mgr.epoch,
            page.as_bytes(),
            &mut encrypted,
        );
        mgr.io.write_page(page_offset(page_id), &encrypted).unwrap();
        mgr.pool.lock().invalidate(page_id);
    };
    rewrite_as_branch(catalog_root, second_page);
    rewrite_as_branch(second_page, catalog_root);

    assert!(matches!(
        mgr.begin_read().table_root_page(b"catalog-entry"),
        Err(Error::DatabaseCorrupted)
    ));
}

#[path = "manager_retry_tests.rs"]
mod retry_tests;

struct PausedCatalogIO {
    inner: MemIO,
    pause_at: Arc<AtomicU64>,
    entered: std::sync::mpsc::Sender<()>,
    resume: StdMutex<std::sync::mpsc::Receiver<()>>,
}

impl PageIO for PausedCatalogIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        if self
            .pause_at
            .compare_exchange(offset, u64::MAX, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(10))
                .map_err(|error| {
                    Error::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, error))
                })?;
        }
        self.inner.read_page(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.inner.write_page(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.inner.read_at(offset, buf)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        self.inner.write_at(offset, buf)
    }

    fn fsync(&self) -> Result<()> {
        self.inner.fsync()
    }

    fn file_size(&self) -> Result<u64> {
        self.inner.file_size()
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size)
    }
}

fn assert_manager_catalog_walk_pins_snapshot(operation: &str) {
    let pause_at = Arc::new(AtomicU64::new(u64::MAX));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (dek, mac, dek_id) = test_keys();
    let manager = TxnManager::create(
        Box::new(PausedCatalogIO {
            inner: MemIO::new(1024 * 1024),
            pause_at: Arc::clone(&pause_at),
            entered: entered_tx,
            resume: StdMutex::new(resume_rx),
        }),
        dek,
        mac,
        1,
        0x1234,
        dek_id,
        16,
    )
    .unwrap();
    let original = b"generation_0".to_vec();
    let mut writer = manager.begin_write().unwrap();
    writer.create_table(&original).unwrap();
    writer.commit().unwrap();
    // Complete collision-index initialization before pausing a read, so
    // writers never wait on the same initialization lock as the reader.
    manager.list_tables().unwrap();
    let original_root = manager.table_root(&original).unwrap().unwrap();
    let slot = manager.current_slot();
    manager.pool.lock().clear();
    pause_at.store(page_offset(slot.catalog_root), Ordering::SeqCst);

    std::thread::scope(|scope| {
        let lookup = scope.spawn(|| -> Result<Vec<Vec<u8>>> {
            match operation {
                "list_tables" => manager
                    .list_tables()
                    .map(|tables| tables.into_iter().map(|(name, _)| name).collect()),
                "table_root" => manager.table_root(&original).map(|root| {
                    assert_eq!(root, Some(original_root));
                    vec![original.clone()]
                }),
                _ => unreachable!(),
            }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        let mut name = original.clone();
        let mut root_reused = false;
        for generation in 1..=128 {
            let next = format!("generation_{generation}").into_bytes();
            let mut writer = manager.begin_write().unwrap();
            writer.rename_table(&name, &next).unwrap();
            writer.commit().unwrap();
            name = next;
            let page = manager.read_page_from_disk(slot.catalog_root).unwrap();
            root_reused |= page.txn_id() > slot.txn_id;
        }
        resume_tx.send(()).unwrap();
        let result = lookup
            .join()
            .unwrap()
            .unwrap_or_else(|error| panic!("{operation}, reused={root_reused}: {error:?}"));
        assert_eq!(
            result,
            vec![original.clone()],
            "{operation}, reused={root_reused}"
        );
    });
}

#[test]
fn manager_list_tables_pins_its_snapshot_while_commits_recycle_pages() {
    assert_manager_catalog_walk_pins_snapshot("list_tables");
}

#[test]
fn manager_table_root_pins_its_snapshot_while_commits_recycle_pages() {
    assert_manager_catalog_walk_pins_snapshot("table_root");
}
