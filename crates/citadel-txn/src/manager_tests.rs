use super::*;
use citadel_core::types::SyncMode;
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
    assert!(mgr.mark_slots_v1().unwrap(), "flag must stamp in-process");
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
    assert!(mgr.mark_slots_v1().unwrap());
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
