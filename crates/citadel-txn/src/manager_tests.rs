use super::*;
use citadel_core::types::{PageType, SyncMode};
use citadel_crypto::hkdf_utils::derive_keys_from_rek;
use citadel_crypto::page_cipher::compute_dek_id;
use std::sync::Mutex as StdMutex;

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

fn reopen_with_pending_fixture<F>(build: F) -> (TxnManager, MemIO)
where
    F: FnOnce(PageId, TxnId) -> (Vec<Page>, u32),
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
    for page in &pages {
        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(
            &dek,
            &mac_key,
            page.page_id(),
            slot.encryption_epoch,
            page.as_bytes(),
            &mut encrypted,
        );
        io.write_page(page_offset(page.page_id()), &encrypted)
            .unwrap();
    }
    slot.pending_free_root = root;
    slot.total_pages = high_water_mark;
    slot.high_water_mark = high_water_mark;
    slot.seal(&mac_key);
    write_commit_slot(&io, active, &slot).unwrap();
    drop(mgr);

    let reopened = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 256).unwrap();
    (reopened, io)
}

fn assert_pending_fixture_refuses_commit<F>(build: F)
where
    F: FnOnce(PageId, TxnId) -> (Vec<Page>, u32),
{
    use citadel_io::file_manager::read_god_byte;

    let (mgr, io) = reopen_with_pending_fixture(build);
    let god_before = read_god_byte(&io).unwrap();
    let mut txn = mgr.begin_write().unwrap();
    txn.insert(b"after", b"value").unwrap();
    assert!(txn.commit().is_err());
    assert_eq!(
        read_god_byte(&io).unwrap(),
        god_before,
        "validation must fail before publishing recovery metadata"
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
    writes_left: std::sync::atomic::AtomicI64,
}

impl FaultingIO {
    fn new(inner: MemIO, budget: i64) -> Self {
        Self {
            inner,
            writes_left: std::sync::atomic::AtomicI64::new(budget),
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

/// Finding 11 (Off-mode process-crash guarantee): a crash at any write of the
/// second commit - data pages, chain pages, slot, or god byte - must leave
/// the first commit's generation fully readable after reopen.
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
    assert_pending_fixture_refuses_commit(|root, txn_id| {
        let second = PageId(root.as_u32() + 1);
        let mut first_page = Page::new(root, PageType::PendingFree, txn_id);
        first_page.set_right_child(second);
        first_page.update_checksum();
        let mut second_page = Page::new(second, PageType::PendingFree, txn_id);
        second_page.set_right_child(root);
        second_page.update_checksum();
        (vec![first_page, second_page], second.as_u32() + 1)
    });
}

#[test]
fn an_oversized_pending_free_entry_count_refuses_the_next_commit() {
    assert_pending_fixture_refuses_commit(|root, txn_id| {
        let mut page = Page::new(root, PageType::PendingFree, txn_id);
        let count = (pending_free::MAX_ENTRIES_PER_PAGE as u32) + 1;
        page.data[citadel_core::PAGE_HEADER_SIZE..citadel_core::PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&count.to_le_bytes());
        page.update_checksum();
        (vec![page], root.as_u32() + 1)
    });
}

#[test]
fn a_wrong_type_or_duplicate_pending_free_entry_refuses_the_next_commit() {
    assert_pending_fixture_refuses_commit(|root, txn_id| {
        let mut page = Page::new(root, PageType::Leaf, txn_id);
        page.update_checksum();
        (vec![page], root.as_u32() + 1)
    });

    assert_pending_fixture_refuses_commit(|root, txn_id| {
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
        (vec![page], free_page.as_u32() + 1)
    });
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
