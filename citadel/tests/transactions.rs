//! Integration tests for transactions and commit protocol.
//!
//! Tests the full transaction lifecycle including:
//! - CRUD through transactions
//! - Snapshot isolation (MVCC)
//! - Commit protocol correctness
//! - Abort semantics
//! - Reader registration and oldest_active_reader tracking
//! - Pending-free chain lifecycle
//! - Crash recovery simulation (god byte states)
//! - Reopen-and-verify persistence
//! - Multiple sequential transactions

use std::sync::Mutex;
use citadel_core::{
    Error, Result, PAGE_SIZE, DEK_SIZE, MAC_KEY_SIZE,
    GOD_BIT_ACTIVE_SLOT, GOD_BIT_RECOVERY,
};
use citadel_crypto::hkdf_utils::derive_keys_from_rek;
use citadel_crypto::page_cipher::compute_dek_id;
use citadel_io::traits::PageIO;
use citadel_io::file_manager;
use citadel_txn::manager::TxnManager;

/// Shared in-memory storage that persists across TxnManager instances (simulates a file).
struct SharedStorage {
    data: Mutex<Vec<u8>>,
}

impl SharedStorage {
    fn new(size: usize) -> Self {
        Self { data: Mutex::new(vec![0u8; size]) }
    }

    #[allow(dead_code)]
    fn snapshot(&self) -> Vec<u8> {
        self.data.lock().unwrap().clone()
    }
}

/// PageIO backed by a shared Vec (for reopen simulation).
struct SharedIO {
    storage: std::sync::Arc<SharedStorage>,
}

impl SharedIO {
    fn new(storage: std::sync::Arc<SharedStorage>) -> Self {
        Self { storage }
    }
}

impl PageIO for SharedIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let data = self.storage.data.lock().unwrap();
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > data.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof, "read past end",
            )));
        }
        buf.copy_from_slice(&data[start..end]);
        Ok(())
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        let mut data = self.storage.data.lock().unwrap();
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > data.len() { data.resize(end, 0); }
        data[start..end].copy_from_slice(buf);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let data = self.storage.data.lock().unwrap();
        let start = offset as usize;
        let end = start + buf.len();
        if end > data.len() {
            let available = data.len().saturating_sub(start);
            if available > 0 { buf[..available].copy_from_slice(&data[start..start + available]); }
            buf[available..].fill(0);
            return Ok(());
        }
        buf.copy_from_slice(&data[start..end]);
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        let mut data = self.storage.data.lock().unwrap();
        let start = offset as usize;
        let end = start + buf.len();
        if end > data.len() { data.resize(end, 0); }
        data[start..end].copy_from_slice(buf);
        Ok(())
    }

    fn fsync(&self) -> Result<()> { Ok(()) }

    fn file_size(&self) -> Result<u64> {
        Ok(self.storage.data.lock().unwrap().len() as u64)
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.storage.data.lock().unwrap().resize(size as usize, 0);
        Ok(())
    }
}

fn test_keys() -> ([u8; DEK_SIZE], [u8; MAC_KEY_SIZE], [u8; 32]) {
    let rek = [0x42u8; 32];
    let keys = derive_keys_from_rek(&rek);
    let dek_id = compute_dek_id(&keys.mac_key, &keys.dek);
    (keys.dek, keys.mac_key, dek_id)
}

fn create_shared_manager(storage: &std::sync::Arc<SharedStorage>) -> TxnManager {
    let (dek, mac_key, dek_id) = test_keys();
    let io = Box::new(SharedIO::new(storage.clone()));
    TxnManager::create(io, dek, mac_key, 1, 0x1234, dek_id, 256).unwrap()
}

fn open_shared_manager(storage: &std::sync::Arc<SharedStorage>) -> TxnManager {
    let (dek, mac_key, _) = test_keys();
    let io = Box::new(SharedIO::new(storage.clone()));
    TxnManager::open(io, dek, mac_key, 1, 256).unwrap()
}

// === Basic CRUD ===

#[test]
fn insert_read_delete_cycle() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Insert
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"name", b"Alice").unwrap();
        wtx.insert(b"age", b"30").unwrap();
        wtx.commit().unwrap();
    }

    // Read
    {
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"name").unwrap(), Some(b"Alice".to_vec()));
        assert_eq!(rtx.get(b"age").unwrap(), Some(b"30".to_vec()));
    }

    // Delete
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.delete(b"age").unwrap();
        wtx.commit().unwrap();
    }

    // Verify
    {
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"name").unwrap(), Some(b"Alice".to_vec()));
        assert_eq!(rtx.get(b"age").unwrap(), None);
    }
}

// === Persistence (reopen-and-verify) ===

#[test]
fn persist_across_reopen() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));

    // Session 1: create and insert
    {
        let mgr = create_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("key-{i:04}");
            let val = format!("val-{i:04}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Session 2: reopen and verify all data
    {
        let mgr = open_shared_manager(&storage);
        let slot = mgr.current_slot();
        assert_eq!(slot.tree_entries, 100);

        let mut rtx = mgr.begin_read();
        for i in 0..100u32 {
            let key = format!("key-{i:04}");
            let val = format!("val-{i:04}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(val.into_bytes()),
                "key {key} should be present after reopen");
        }
    }
}

#[test]
fn multiple_sessions_accumulate() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));

    // Session 1: insert keys 0-49
    {
        let mgr = create_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..50u32 {
            let key = format!("k{i:04}");
            wtx.insert(key.as_bytes(), b"v1").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Session 2: insert keys 50-99
    {
        let mgr = open_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        for i in 50..100u32 {
            let key = format!("k{i:04}");
            wtx.insert(key.as_bytes(), b"v2").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Session 3: verify all 100
    {
        let mgr = open_shared_manager(&storage);
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.entry_count(), 100);
        for i in 0..100u32 {
            let key = format!("k{i:04}");
            assert!(rtx.get(key.as_bytes()).unwrap().is_some());
        }
    }
}

// === Snapshot Isolation ===

#[test]
fn snapshot_isolation_read_during_write() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Insert initial data
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"stable", b"yes").unwrap();
        wtx.commit().unwrap();
    }

    // Start a read (snapshot before write)
    let mut rtx_before = mgr.begin_read();
    assert_eq!(rtx_before.get(b"stable").unwrap(), Some(b"yes".to_vec()));

    // Write new data
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"new_key", b"new_val").unwrap();
        wtx.insert(b"stable", b"modified").unwrap();
        wtx.commit().unwrap();
    }

    // Old read should NOT see the new data (snapshot isolation via CoW)
    assert_eq!(rtx_before.get(b"stable").unwrap(), Some(b"yes".to_vec()));
    assert_eq!(rtx_before.get(b"new_key").unwrap(), None);

    // New read should see everything
    let mut rtx_after = mgr.begin_read();
    assert_eq!(rtx_after.get(b"stable").unwrap(), Some(b"modified".to_vec()));
    assert_eq!(rtx_after.get(b"new_key").unwrap(), Some(b"new_val".to_vec()));
}

// === Abort Semantics ===

#[test]
fn abort_leaves_database_unchanged() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Initial state
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"original", b"data").unwrap();
        wtx.commit().unwrap();
    }

    // Aborted write
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"aborted_key", b"should_not_exist").unwrap();
        wtx.delete(b"original").unwrap();
        wtx.abort();
    }

    // Verify no changes from the abort
    {
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"original").unwrap(), Some(b"data".to_vec()));
        assert_eq!(rtx.get(b"aborted_key").unwrap(), None);
    }
}

#[test]
fn drop_without_commit_is_abort() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"key", b"val").unwrap();
        // Dropped without commit
    }

    // Writer should be released for a new write
    let _wtx2 = mgr.begin_write().unwrap();
}

// === Commit Slot Alternation ===

#[test]
fn commit_slot_alternates() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Read initial god byte
    let io = SharedIO::new(storage.clone());
    let initial_god = file_manager::read_god_byte(&io).unwrap();
    let initial_slot = (initial_god & GOD_BIT_ACTIVE_SLOT) as usize;

    // Commit 1: should flip to opposite slot
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"k1", b"v1").unwrap();
        wtx.commit().unwrap();
    }

    let after_1 = file_manager::read_god_byte(&io).unwrap();
    let slot_1 = (after_1 & GOD_BIT_ACTIVE_SLOT) as usize;
    assert_ne!(slot_1, initial_slot, "commit should flip active slot");

    // Commit 2: should flip back
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"k2", b"v2").unwrap();
        wtx.commit().unwrap();
    }

    let after_2 = file_manager::read_god_byte(&io).unwrap();
    let slot_2 = (after_2 & GOD_BIT_ACTIVE_SLOT) as usize;
    assert_ne!(slot_2, slot_1, "second commit should flip again");
    assert_eq!(slot_2, initial_slot, "should be back to original slot");
}

#[test]
fn recovery_flag_cleared_after_commit() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"key", b"val").unwrap();
        wtx.commit().unwrap();
    }

    // After commit, recovery flag (bit 1) should be 0
    let io = SharedIO::new(storage.clone());
    let god = file_manager::read_god_byte(&io).unwrap();
    assert_eq!(god & GOD_BIT_RECOVERY, 0, "recovery flag should be cleared after commit");
}

// === Recovery Simulation ===

#[test]
fn recovery_with_recovery_flag_set() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));

    // Create and commit some data
    {
        let mgr = create_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"key", b"value").unwrap();
        wtx.commit().unwrap();
    }

    // Simulate crash during a commit: set recovery flag manually
    {
        let io = SharedIO::new(storage.clone());
        let god = file_manager::read_god_byte(&io).unwrap();
        let crashed_god = god | GOD_BIT_RECOVERY;
        file_manager::write_god_byte(&io, crashed_god).unwrap();
    }

    // Reopen — recovery should succeed, data should be intact
    {
        let mgr = open_shared_manager(&storage);
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    // Recovery flag should be cleared
    {
        let io = SharedIO::new(storage.clone());
        let god = file_manager::read_god_byte(&io).unwrap();
        assert_eq!(god & GOD_BIT_RECOVERY, 0, "recovery should clear the flag");
    }
}

// === Reader Registration ===

#[test]
fn reader_count_lifecycle() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    assert_eq!(mgr.reader_count(), 0);

    let r1 = mgr.begin_read();
    assert_eq!(mgr.reader_count(), 1);

    let r2 = mgr.begin_read();
    assert_eq!(mgr.reader_count(), 2);

    drop(r1);
    assert_eq!(mgr.reader_count(), 1);

    drop(r2);
    assert_eq!(mgr.reader_count(), 0);
}

// === Large Data ===

#[test]
fn thousand_keys_persist() {
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));

    // Write 1000 keys
    {
        let mgr = create_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..1000u32 {
            let key = format!("key-{i:06}");
            let val = format!("value-{i:06}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Reopen and verify
    {
        let mgr = open_shared_manager(&storage);
        let slot = mgr.current_slot();
        assert_eq!(slot.tree_entries, 1000);

        let mut rtx = mgr.begin_read();
        for i in 0..1000u32 {
            let key = format!("key-{i:06}");
            let val = format!("value-{i:06}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }
    }
}

// === Multiple Transactions ===

#[test]
fn ten_sequential_transactions() {
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    for batch in 0..10u32 {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..50u32 {
            let key = format!("b{batch}-k{i}");
            let val = format!("v{batch}-{i}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.entry_count(), 500);
    for batch in 0..10u32 {
        for i in 0..50u32 {
            let key = format!("b{batch}-k{i}");
            assert!(rtx.get(key.as_bytes()).unwrap().is_some());
        }
    }
}

// === Pending-Free Chain Lifecycle ===

#[test]
fn pending_free_pages_accumulate() {
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Insert then delete — should free pages
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("del-{i:04}");
            wtx.insert(key.as_bytes(), b"x").unwrap();
        }
        wtx.commit().unwrap();
    }

    let hwm_after_insert = mgr.current_slot().high_water_mark;

    // Delete all — CoW creates new pages, frees old
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("del-{i:04}");
            wtx.delete(key.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // HWM should have grown (CoW allocated new pages for deletion path)
    let hwm_after_delete = mgr.current_slot().high_water_mark;
    assert!(hwm_after_delete >= hwm_after_insert,
        "HWM should not shrink: before={hwm_after_insert} after={hwm_after_delete}");

    // Pending-free chain should exist
    let pf_root = mgr.current_slot().pending_free_root;
    assert!(pf_root.is_valid(), "pending-free chain should have entries");
}

// === Entry Count Correctness ===

#[test]
fn entry_count_tracks_across_transactions() {
    let storage = std::sync::Arc::new(SharedStorage::new(2 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Txn 1: +5
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..5 {
            wtx.insert(format!("k{i}").as_bytes(), b"v").unwrap();
        }
        wtx.commit().unwrap();
    }
    assert_eq!(mgr.current_slot().tree_entries, 5);

    // Txn 2: +3 new, update 1 existing
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"k0", b"updated").unwrap(); // update
        wtx.insert(b"k5", b"v").unwrap(); // new
        wtx.insert(b"k6", b"v").unwrap(); // new
        wtx.insert(b"k7", b"v").unwrap(); // new
        wtx.commit().unwrap();
    }
    assert_eq!(mgr.current_slot().tree_entries, 8);

    // Txn 3: -2
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.delete(b"k1").unwrap();
        wtx.delete(b"k2").unwrap();
        wtx.commit().unwrap();
    }
    assert_eq!(mgr.current_slot().tree_entries, 6);
}

// === Concurrent Read and Write ===

#[test]
fn reader_coexists_with_writer() {
    let storage = std::sync::Arc::new(SharedStorage::new(2 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Insert initial data
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"pre", b"existing").unwrap();
        wtx.commit().unwrap();
    }

    // Start a reader
    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"pre").unwrap(), Some(b"existing".to_vec()));

    // Start a writer while reader is active
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"during_write", b"new").unwrap();
        wtx.commit().unwrap();
    }

    // Reader still sees old snapshot
    assert_eq!(rtx.get(b"during_write").unwrap(), None);
    assert_eq!(rtx.get(b"pre").unwrap(), Some(b"existing".to_vec()));
}

// === Write-Read-Write Interleaving ===

#[test]
fn write_read_write_interleave() {
    let storage = std::sync::Arc::new(SharedStorage::new(2 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"a", b"1").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx1 = mgr.begin_read();
    assert_eq!(rtx1.get(b"a").unwrap(), Some(b"1".to_vec()));

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"a", b"2").unwrap();
        wtx.insert(b"b", b"3").unwrap();
        wtx.commit().unwrap();
    }

    // rtx1 still sees old state
    assert_eq!(rtx1.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(rtx1.get(b"b").unwrap(), None);

    // New reader sees new state
    let mut rtx2 = mgr.begin_read();
    assert_eq!(rtx2.get(b"a").unwrap(), Some(b"2".to_vec()));
    assert_eq!(rtx2.get(b"b").unwrap(), Some(b"3".to_vec()));
}

// ============================================================
// Edge Case & Regression Tests
// ============================================================

// --- BUG #1 Regression: Failed commit must release writer lock ---

/// FailingIO wraps SharedIO and fails on the Nth fsync call.
/// This lets us simulate a commit failure mid-protocol.
struct FailingIO {
    storage: std::sync::Arc<SharedStorage>,
    fsync_count: std::sync::atomic::AtomicU32,
    fail_on_fsync: u32,
}

impl FailingIO {
    fn new(storage: std::sync::Arc<SharedStorage>, fail_on_fsync: u32) -> Self {
        Self {
            storage,
            fsync_count: std::sync::atomic::AtomicU32::new(0),
            fail_on_fsync,
        }
    }
}

impl PageIO for FailingIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        SharedIO::new(self.storage.clone()).read_page(offset, buf)
    }
    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        SharedIO::new(self.storage.clone()).write_page(offset, buf)
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        SharedIO::new(self.storage.clone()).read_at(offset, buf)
    }
    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        SharedIO::new(self.storage.clone()).write_at(offset, buf)
    }
    fn fsync(&self) -> Result<()> {
        let n = self.fsync_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if n == self.fail_on_fsync {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other, "simulated fsync failure",
            )));
        }
        Ok(())
    }
    fn file_size(&self) -> Result<u64> {
        SharedIO::new(self.storage.clone()).file_size()
    }
    fn truncate(&self, size: u64) -> Result<()> {
        SharedIO::new(self.storage.clone()).truncate(size)
    }
}

#[test]
fn failed_commit_releases_writer_lock() {
    // BUG #1 regression: if commit_write fails, the writer must be released.
    // We use FailingIO that fails on the 2nd fsync (the first fsync is for
    // the recovery flag, the second is for data+slot durability).
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));

    // First, create a valid database with normal I/O
    {
        let mgr = create_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"initial", b"data").unwrap();
        wtx.commit().unwrap();
    }

    // Now open with FailingIO that fails on 2nd fsync (during commit protocol)
    let (dek, mac_key, _) = test_keys();
    let failing_io = Box::new(FailingIO::new(storage.clone(), 2));
    let mgr = TxnManager::open(failing_io, dek, mac_key, 1, 256).unwrap();

    // Attempt a write — commit should fail
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"new_key", b"new_val").unwrap();
        let result = wtx.commit();
        assert!(result.is_err(), "commit should fail due to simulated fsync failure");
        // WriteTxn is dropped here — Drop should call abort_write() because committed=false
    }

    // The critical test: can we begin a new write transaction?
    // Before the fix, this would fail with WriteTransactionActive.
    let result = mgr.begin_write();
    assert!(result.is_ok(), "writer lock should be released after failed commit");
}

// --- BUG #2 Regression: for_each must work on multi-leaf trees ---

#[test]
fn for_each_multi_leaf_tree() {
    // BUG #2 regression: for_each used to only preload the leftmost path,
    // causing failures when cursor.next() tried to access unloaded pages.
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    let count = 500u32;

    // Insert enough entries to span many leaves
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..count {
            let key = format!("key-{i:06}");
            let val = format!("val-{i:06}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Now use for_each in a NEW write transaction (reads from disk)
    {
        let mut wtx = mgr.begin_write().unwrap();
        let mut collected = Vec::new();
        wtx.for_each(|k, v| {
            collected.push((k.to_vec(), v.to_vec()));
            Ok(())
        }).unwrap();

        assert_eq!(collected.len(), count as usize,
            "for_each should visit all {count} entries");

        // Verify sorted order
        for i in 1..collected.len() {
            assert!(collected[i].0 > collected[i-1].0,
                "for_each must return entries in sorted order");
        }

        // Verify first and last
        assert_eq!(collected[0].0, b"key-000000");
        assert_eq!(collected[0].1, b"val-000000");
        assert_eq!(collected[count as usize - 1].0, format!("key-{:06}", count - 1).as_bytes());

        wtx.abort();
    }
}

#[test]
fn reclaimed_pages_reused() {
    let storage = std::sync::Arc::new(SharedStorage::new(8 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Insert 200 keys
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..200u32 {
            let key = format!("rkey-{i:04}");
            wtx.insert(key.as_bytes(), b"value").unwrap();
        }
        wtx.commit().unwrap();
    }
    let hwm_after_insert = mgr.current_slot().high_water_mark;
    let initial_hwm = mgr.current_slot().high_water_mark;
    assert!(initial_hwm >= 1, "initial HWM should include at least the root page");

    // The insert of 200 small keys should have grown HWM beyond the initial root
    assert!(hwm_after_insert > 1,
        "inserting 200 keys should allocate multiple pages, HWM={hwm_after_insert}");

    // Delete all 200 keys (CoW frees pages)
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..200u32 {
            let key = format!("rkey-{i:04}");
            wtx.delete(key.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let hwm_after_delete = mgr.current_slot().high_water_mark;
    // CoW deletes create new pages for the modified path, so HWM should grow
    assert!(hwm_after_delete >= hwm_after_insert,
        "CoW deletes should not shrink HWM: after_insert={hwm_after_insert} after_delete={hwm_after_delete}");

    // Trigger reclaim with no active readers
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"trigger-reclaim", b"x").unwrap();
        wtx.commit().unwrap();
    }

    // Insert new data — should reuse reclaimed pages, not grow HWM much
    let hwm_before_reuse = mgr.current_slot().high_water_mark;
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("new-{i:04}");
            wtx.insert(key.as_bytes(), b"val").unwrap();
        }
        wtx.commit().unwrap();
    }
    let hwm_after_reuse = mgr.current_slot().high_water_mark;

    // The HWM growth should be much less than 100 pages because reclaimed pages
    // should be reused. Without the fix, HWM would grow by ~100+ pages.
    let growth = hwm_after_reuse - hwm_before_reuse;
    assert!(growth < 50,
        "reclaimed pages should be reused, but HWM grew by {growth} \
         (before={hwm_before_reuse}, after={hwm_after_reuse})");
}

// --- for_each edge cases ---

#[test]
fn for_each_empty_tree() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    let mut wtx = mgr.begin_write().unwrap();
    let mut count = 0u32;
    wtx.for_each(|_, _| {
        count += 1;
        Ok(())
    }).unwrap();
    assert_eq!(count, 0);
    wtx.abort();
}

#[test]
fn for_each_single_entry() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"only-key", b"only-val").unwrap();
        wtx.commit().unwrap();
    }

    {
        let mut wtx = mgr.begin_write().unwrap();
        let mut entries = Vec::new();
        wtx.for_each(|k, v| {
            entries.push((k.to_vec(), v.to_vec()));
            Ok(())
        }).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, b"only-key");
        assert_eq!(entries[0].1, b"only-val");
        wtx.abort();
    }
}

#[test]
fn for_each_filters_tombstones() {
    let storage = std::sync::Arc::new(SharedStorage::new(2 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Insert 10 keys
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..10u32 {
            let key = format!("fkey-{i:02}");
            wtx.insert(key.as_bytes(), b"v").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Delete 5 keys — creates tombstones in the B+ tree
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..5u32 {
            let key = format!("fkey-{i:02}");
            wtx.delete(key.as_bytes()).unwrap();
        }

        // for_each within the same txn (sees uncommitted state)
        let mut entries = Vec::new();
        wtx.for_each(|k, v| {
            entries.push((k.to_vec(), v.to_vec()));
            Ok(())
        }).unwrap();

        // Should only see the 5 remaining keys (tombstones filtered)
        assert_eq!(entries.len(), 5, "for_each should filter tombstones");
        for (i, (k, _)) in entries.iter().enumerate() {
            let expected = format!("fkey-{:02}", i + 5);
            assert_eq!(*k, expected.as_bytes());
        }

        wtx.commit().unwrap();
    }
}

#[test]
fn for_each_after_mixed_operations() {
    let storage = std::sync::Arc::new(SharedStorage::new(2 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    {
        let mut wtx = mgr.begin_write().unwrap();
        // Insert 20 keys
        for i in 0..20u32 {
            let key = format!("m{i:03}");
            wtx.insert(key.as_bytes(), b"original").unwrap();
        }
        // Delete even-numbered keys
        for i in (0..20u32).step_by(2) {
            let key = format!("m{i:03}");
            wtx.delete(key.as_bytes()).unwrap();
        }
        // Update odd-numbered keys
        for i in (1..20u32).step_by(2) {
            let key = format!("m{i:03}");
            wtx.insert(key.as_bytes(), b"updated").unwrap();
        }

        let mut entries = Vec::new();
        wtx.for_each(|k, v| {
            entries.push((k.to_vec(), v.to_vec()));
            Ok(())
        }).unwrap();

        assert_eq!(entries.len(), 10, "should have 10 entries after deleting evens");
        for (_, v) in &entries {
            assert_eq!(v.as_slice(), b"updated");
        }
        wtx.commit().unwrap();
    }
}

// --- WriteTxn read-your-own-writes ---

#[test]
fn write_txn_sees_own_inserts() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    let mut wtx = mgr.begin_write().unwrap();

    // Insert and immediately read back (before commit)
    wtx.insert(b"key1", b"val1").unwrap();
    assert_eq!(wtx.get(b"key1").unwrap(), Some(b"val1".to_vec()));

    wtx.insert(b"key2", b"val2").unwrap();
    assert_eq!(wtx.get(b"key2").unwrap(), Some(b"val2".to_vec()));

    // Both still visible
    assert_eq!(wtx.get(b"key1").unwrap(), Some(b"val1".to_vec()));

    wtx.commit().unwrap();
}

#[test]
fn write_txn_sees_own_deletes() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"a", b"1").unwrap();
        wtx.insert(b"b", b"2").unwrap();
        wtx.insert(b"c", b"3").unwrap();
        wtx.commit().unwrap();
    }

    {
        let mut wtx = mgr.begin_write().unwrap();
        // Delete and verify immediately
        wtx.delete(b"b").unwrap();
        assert_eq!(wtx.get(b"b").unwrap(), None, "deleted key should not be visible");
        assert_eq!(wtx.get(b"a").unwrap(), Some(b"1".to_vec()), "non-deleted should be visible");
        assert_eq!(wtx.get(b"c").unwrap(), Some(b"3".to_vec()));
        wtx.commit().unwrap();
    }
}

// --- Empty commit ---

#[test]
fn empty_commit() {
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Insert data first
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"key", b"val").unwrap();
        wtx.commit().unwrap();
    }

    let slot_before = mgr.current_slot();

    // Empty commit (no operations)
    {
        let wtx = mgr.begin_write().unwrap();
        wtx.commit().unwrap();
    }

    let slot_after = mgr.current_slot();
    // Data should still be there
    assert_eq!(slot_after.tree_entries, slot_before.tree_entries);

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"val".to_vec()));
}

// --- Update same key many times ---

#[test]
fn update_same_key_many_times() {
    let storage = std::sync::Arc::new(SharedStorage::new(2 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..100u32 {
            let val = format!("version-{i:03}");
            wtx.insert(b"counter", val.as_bytes()).unwrap();
        }
        // Entry count should still be 1 (updates, not new inserts)
        assert_eq!(wtx.entry_count(), 1);
        assert_eq!(wtx.get(b"counter").unwrap(), Some(b"version-099".to_vec()));
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.entry_count(), 1);
    assert_eq!(rtx.get(b"counter").unwrap(), Some(b"version-099".to_vec()));
}

// --- Three-session persistence ---

#[test]
fn three_session_persistence() {
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));

    // Session 1: create DB, insert keys
    {
        let mgr = create_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..50u32 {
            let key = format!("s1-{i:03}");
            wtx.insert(key.as_bytes(), b"session1").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Session 2: reopen, verify, add more
    {
        let mgr = open_shared_manager(&storage);
        // Verify session 1 data
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.entry_count(), 50);
        assert_eq!(rtx.get(b"s1-000").unwrap(), Some(b"session1".to_vec()));
        drop(rtx);

        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..30u32 {
            let key = format!("s2-{i:03}");
            wtx.insert(key.as_bytes(), b"session2").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Session 3: reopen, verify all
    {
        let mgr = open_shared_manager(&storage);
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.entry_count(), 80);

        // Session 1 data intact
        for i in 0..50u32 {
            let key = format!("s1-{i:03}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(b"session1".to_vec()),
                "session 1 key {key} should persist through 3 sessions");
        }
        // Session 2 data intact
        for i in 0..30u32 {
            let key = format!("s2-{i:03}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(b"session2".to_vec()));
        }
    }
}

// --- Concurrent readers on different snapshots ---

#[test]
fn concurrent_readers_different_snapshots() {
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Version 1: 5 keys
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..5u32 {
            let key = format!("snap-{i}");
            wtx.insert(key.as_bytes(), b"v1").unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut reader_v1 = mgr.begin_read();

    // Version 2: add 5 more keys, update existing
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 5..10u32 {
            let key = format!("snap-{i}");
            wtx.insert(key.as_bytes(), b"v2").unwrap();
        }
        wtx.insert(b"snap-0", b"updated").unwrap();
        wtx.commit().unwrap();
    }

    let mut reader_v2 = mgr.begin_read();

    // Version 3: delete some keys
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.delete(b"snap-1").unwrap();
        wtx.delete(b"snap-2").unwrap();
        wtx.commit().unwrap();
    }

    let mut reader_v3 = mgr.begin_read();

    // Verify each reader sees its own snapshot
    assert_eq!(reader_v1.entry_count(), 5);
    assert_eq!(reader_v1.get(b"snap-0").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(reader_v1.get(b"snap-5").unwrap(), None); // Not yet in v1

    assert_eq!(reader_v2.entry_count(), 10);
    assert_eq!(reader_v2.get(b"snap-0").unwrap(), Some(b"updated".to_vec()));
    assert_eq!(reader_v2.get(b"snap-5").unwrap(), Some(b"v2".to_vec()));

    assert_eq!(reader_v3.entry_count(), 8);
    assert_eq!(reader_v3.get(b"snap-1").unwrap(), None); // Deleted in v3
    assert_eq!(reader_v3.get(b"snap-3").unwrap(), Some(b"v1".to_vec()));

    // 3 readers active simultaneously
    assert_eq!(mgr.reader_count(), 3);
}

// --- Large batch delete ---

#[test]
fn large_batch_delete_and_verify() {
    let storage = std::sync::Arc::new(SharedStorage::new(8 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Insert 1000 keys
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..1000u32 {
            let key = format!("d{i:06}");
            let val = format!("v{i:06}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Delete first 500
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..500u32 {
            let key = format!("d{i:06}");
            let existed = wtx.delete(key.as_bytes()).unwrap();
            assert!(existed, "key d{i:06} should exist");
        }
        wtx.commit().unwrap();
    }

    // Verify: first 500 gone, last 500 present
    {
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.entry_count(), 500);

        for i in 0..500u32 {
            let key = format!("d{i:06}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), None,
                "deleted key d{i:06} should not be present");
        }
        for i in 500..1000u32 {
            let key = format!("d{i:06}");
            let val = format!("v{i:06}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(val.into_bytes()),
                "surviving key d{i:06} should be present");
        }
    }
}

// --- HWM tracking ---

#[test]
fn hwm_tracking_across_transactions() {
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    let initial_hwm = mgr.current_slot().high_water_mark;
    assert!(initial_hwm >= 1, "initial HWM should include root page");

    // Insert many keys — HWM should grow
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..200u32 {
            let key = format!("h{i:04}");
            wtx.insert(key.as_bytes(), b"v").unwrap();
        }
        wtx.commit().unwrap();
    }

    let hwm_after_insert = mgr.current_slot().high_water_mark;
    assert!(hwm_after_insert > initial_hwm,
        "HWM should grow after inserts: initial={initial_hwm} after={hwm_after_insert}");

    // HWM never decreases (even after deletes)
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("h{i:04}");
            wtx.delete(key.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let hwm_after_delete = mgr.current_slot().high_water_mark;
    assert!(hwm_after_delete >= hwm_after_insert,
        "HWM should never decrease: after_insert={hwm_after_insert} after_delete={hwm_after_delete}");
}

// --- Oldest reader blocks page reclamation ---

#[test]
fn oldest_reader_blocks_reclamation() {
    // Uses large values (~1800 bytes each) to force multi-page trees.
    // This creates enough pages to make reclamation effects measurable:
    // Each leaf holds ~4 entries with 1800B values, so 80 entries ≈ 20+ leaves + branches.
    let storage = std::sync::Arc::new(SharedStorage::new(16 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    let big_val = vec![0xABu8; 1800]; // Forces ~4 entries per leaf

    // Insert 80 large-value keys → creates ~20+ leaf pages + branches
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..80u32 {
            let key = format!("o{i:04}");
            wtx.insert(key.as_bytes(), &big_val).unwrap();
        }
        wtx.commit().unwrap();
    }

    let hwm_after_insert = mgr.current_slot().high_water_mark;
    let initial_hwm = 1u32; // DB starts with root page at HWM=1
    assert!(hwm_after_insert > initial_hwm,
        "inserting 80 large-value entries should grow HWM significantly: \
         initial={initial_hwm}, after_insert={hwm_after_insert}");
    // With ~1800B values, each leaf holds ~4 entries, so 80 entries ≈ 20 leaves + branches ≈ 25+ pages
    assert!(hwm_after_insert >= 20,
        "80 entries with 1800B values should need at least 20 pages, got HWM={hwm_after_insert}");

    // Start a reader AFTER insert — this pins the current snapshot.
    // Pages freed in future txns (freed_at_txn >= reader's txn) can't be reclaimed.
    let mut old_reader = mgr.begin_read();
    assert_eq!(old_reader.entry_count(), 80);

    // Delete all 80 keys. CoW frees the old tree pages (many pages).
    // Since oldest_active_reader is our reader, these freed pages stay in pending-free.
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..80u32 {
            let key = format!("o{i:04}");
            wtx.delete(key.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Trigger reclaim attempt — reader blocks it
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"trigger", b"x").unwrap();
        wtx.commit().unwrap();
    }

    let hwm_with_reader = mgr.current_slot().high_water_mark;

    // Insert 80 new large-value keys. Because the old freed pages can't be
    // reclaimed (reader blocks), these MUST come from HWM (new disk space).
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..80u32 {
            let key = format!("n{i:04}");
            wtx.insert(key.as_bytes(), &big_val).unwrap();
        }
        wtx.commit().unwrap();
    }

    let hwm_after_blocked = mgr.current_slot().high_water_mark;
    let growth_with_reader = hwm_after_blocked - hwm_with_reader;
    assert!(growth_with_reader > 10,
        "with reader blocking reclamation, HWM must grow significantly \
         (grew by {growth_with_reader}, expected >10). \
         hwm_with_reader={hwm_with_reader}, hwm_after_blocked={hwm_after_blocked}");

    // Old reader still sees its original data (MVCC correctness)
    for i in 0..80u32 {
        let key = format!("o{i:04}");
        assert_eq!(old_reader.get(key.as_bytes()).unwrap(), Some(big_val.clone()),
            "old reader must see original data for key o{i:04}");
    }

    // Drop the old reader — now pages become reclaimable
    drop(old_reader);

    // Trigger reclaim: commit with no active readers
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"reclaim-trigger", b"y").unwrap();
        wtx.commit().unwrap();
    }

    let hwm_after_reclaim_trigger = mgr.current_slot().high_water_mark;

    // Insert 80 more large keys. This time, reclaimed pages should be reused,
    // so HWM growth should be much less than the first time.
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..80u32 {
            let key = format!("r{i:04}");
            wtx.insert(key.as_bytes(), &big_val).unwrap();
        }
        wtx.commit().unwrap();
    }

    let hwm_after_reuse = mgr.current_slot().high_water_mark;
    let growth_without_reader = hwm_after_reuse - hwm_after_reclaim_trigger;

    // With reclaimed pages available, growth should be significantly less
    assert!(growth_without_reader < growth_with_reader,
        "after reader dropped, reclaimed pages should be reused. \
         growth_with_reader={growth_with_reader}, growth_without_reader={growth_without_reader}");
}

// --- Write after abort ---

#[test]
fn write_after_abort_succeeds() {
    let storage = std::sync::Arc::new(SharedStorage::new(2 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Abort a write
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"aborted", b"data").unwrap();
        wtx.abort();
    }

    // New write should succeed
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"real", b"data").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"aborted").unwrap(), None);
    assert_eq!(rtx.get(b"real").unwrap(), Some(b"data".to_vec()));
}

// --- Mixed insert/update/delete entry count ---

#[test]
fn entry_count_mixed_operations_single_txn() {
    let storage = std::sync::Arc::new(SharedStorage::new(2 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    {
        let mut wtx = mgr.begin_write().unwrap();

        // Insert 10 keys
        for i in 0..10u32 {
            let key = format!("e{i}");
            wtx.insert(key.as_bytes(), b"v1").unwrap();
        }
        assert_eq!(wtx.entry_count(), 10);

        // Update 3 keys (should not change count)
        wtx.insert(b"e0", b"updated").unwrap();
        wtx.insert(b"e1", b"updated").unwrap();
        wtx.insert(b"e2", b"updated").unwrap();
        assert_eq!(wtx.entry_count(), 10, "updates should not change entry count");

        // Delete 2 keys
        wtx.delete(b"e8").unwrap();
        wtx.delete(b"e9").unwrap();
        assert_eq!(wtx.entry_count(), 8, "deletes should reduce entry count");

        // Insert 1 new key
        wtx.insert(b"e_new", b"fresh").unwrap();
        assert_eq!(wtx.entry_count(), 9);

        wtx.commit().unwrap();
    }

    assert_eq!(mgr.current_slot().tree_entries, 9);
}

// --- Persistence after delete ---

#[test]
fn deleted_keys_stay_deleted_after_reopen() {
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));

    // Create and insert
    {
        let mgr = create_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..20u32 {
            let key = format!("dk-{i:03}");
            wtx.insert(key.as_bytes(), b"v").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Reopen, delete half
    {
        let mgr = open_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..10u32 {
            let key = format!("dk-{i:03}");
            wtx.delete(key.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Reopen again, verify deletes persisted
    {
        let mgr = open_shared_manager(&storage);
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.entry_count(), 10);
        for i in 0..10u32 {
            let key = format!("dk-{i:03}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), None,
                "deleted key {key} should stay deleted after reopen");
        }
        for i in 10..20u32 {
            let key = format!("dk-{i:03}");
            assert!(rtx.get(key.as_bytes()).unwrap().is_some(),
                "surviving key {key} should be present after reopen");
        }
    }
}

// --- Stress: many small transactions ---

#[test]
fn fifty_sequential_transactions() {
    let storage = std::sync::Arc::new(SharedStorage::new(8 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    for batch in 0..50u32 {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..10u32 {
            let key = format!("t{batch:03}-{i:02}");
            wtx.insert(key.as_bytes(), b"v").unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.entry_count(), 500);

    // Spot-check some keys
    assert!(rtx.get(b"t000-00").unwrap().is_some());
    assert!(rtx.get(b"t025-05").unwrap().is_some());
    assert!(rtx.get(b"t049-09").unwrap().is_some());
}

// ============================================================
// Additional Edge Case Tests
// ============================================================

// --- Torn commit slot recovery ---

#[test]
fn torn_commit_slot_falls_back_to_active() {
    // Simulates a crash during step 2 of the commit protocol:
    // The inactive commit slot is only partially written (torn write).
    // Recovery should detect the invalid checksum on the inactive slot
    // and fall back to the active slot (old commit).
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));

    // Create DB with some data
    {
        let mgr = create_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..50u32 {
            let key = format!("torn-{i:03}");
            wtx.insert(key.as_bytes(), b"committed").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Determine which slot is inactive (we'll corrupt it)
    let io = SharedIO::new(storage.clone());
    let god = file_manager::read_god_byte(&io).unwrap();
    let active_slot_idx = (god & GOD_BIT_ACTIVE_SLOT) as usize;
    let inactive_slot_idx = 1 - active_slot_idx;

    // Corrupt the inactive slot: overwrite its bytes with garbage
    // This simulates a torn write where only part of the slot was written
    let inactive_offset = citadel_core::COMMIT_SLOT_OFFSET
        + inactive_slot_idx * citadel_core::COMMIT_SLOT_SIZE;
    let garbage = vec![0xDE; citadel_core::COMMIT_SLOT_SIZE];
    io.write_at(inactive_offset as u64, &garbage).unwrap();

    // Also set recovery_required flag to simulate mid-commit crash
    let crashed_god = god | GOD_BIT_RECOVERY;
    file_manager::write_god_byte(&io, crashed_god).unwrap();

    // Reopen — recovery should use the active slot (old commit)
    let mgr = open_shared_manager(&storage);
    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.entry_count(), 50, "recovery should use active slot with 50 entries");
    for i in 0..50u32 {
        let key = format!("torn-{i:03}");
        assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(b"committed".to_vec()),
            "key {key} should survive torn commit slot recovery");
    }
}

// --- Both commit slots corrupted = DatabaseCorrupted (double fault) ---

#[test]
fn both_slots_corrupted_returns_error() {
    // Extremely rare scenario: both commit slots have invalid checksums.
    // The only correct response is Error::DatabaseCorrupted.
    let storage = std::sync::Arc::new(SharedStorage::new(1024 * 1024));

    // Create a valid DB first
    {
        let mgr = create_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"key", b"val").unwrap();
        wtx.commit().unwrap();
    }

    // Corrupt BOTH commit slots
    let io = SharedIO::new(storage.clone());
    for slot_idx in 0..2 {
        let offset = citadel_core::COMMIT_SLOT_OFFSET
            + slot_idx * citadel_core::COMMIT_SLOT_SIZE;
        let garbage = vec![0xFF; citadel_core::COMMIT_SLOT_SIZE];
        io.write_at(offset as u64, &garbage).unwrap();
    }

    // Attempt to open — should get DatabaseCorrupted
    let (dek, mac_key, _) = test_keys();
    let io = Box::new(SharedIO::new(storage.clone()));
    let result = TxnManager::open(io, dek, mac_key, 1, 256);
    assert!(result.is_err(), "opening with both corrupted slots should fail");
}

// --- Rapid key overwrite: file size stabilizes ---

#[test]
fn rapid_key_overwrite_file_stabilizes() {
    // Rapidly overwriting the same key should not cause unbounded file growth.
    // With proper reclamation, freed pages are reused when no reader holds
    // old snapshots.
    let storage = std::sync::Arc::new(SharedStorage::new(8 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Warmup — 50 transactions overwriting the same key
    for i in 0..50u32 {
        let mut wtx = mgr.begin_write().unwrap();
        let val = format!("version-{i:05}");
        wtx.insert(b"hot-key", val.as_bytes()).unwrap();
        wtx.commit().unwrap();
    }

    let hwm_after_warmup = mgr.current_slot().high_water_mark;

    // 200 more transactions overwriting the same key
    // With no active readers, freed pages should be reclaimed and reused.
    // HWM growth should be bounded (not linear with transaction count).
    for i in 50..250u32 {
        let mut wtx = mgr.begin_write().unwrap();
        let val = format!("version-{i:05}");
        wtx.insert(b"hot-key", val.as_bytes()).unwrap();
        wtx.commit().unwrap();
    }

    let hwm_after_stress = mgr.current_slot().high_water_mark;
    let growth = hwm_after_stress - hwm_after_warmup;

    // With proper page reclamation, growth should be minimal because:
    // - Each overwrite only CoW's ~2-3 pages (leaf + ancestors)
    // - Freed pages are reclaimed by subsequent transactions
    // - 200 txns should NOT allocate 200+ new pages
    assert!(growth < 50,
        "HWM should stabilize with page reclamation: warmup_hwm={hwm_after_warmup} \
         stress_hwm={hwm_after_stress} growth={growth}");

    // Final value should be correct
    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"hot-key").unwrap(), Some(b"version-00249".to_vec()));
    assert_eq!(rtx.entry_count(), 1);
}

// --- Transient I/O error during commit + reopen consistency ---

#[test]
fn transient_io_error_does_not_corrupt_database() {
    // A transient I/O error during commit followed by clean close must not
    // corrupt the database. After a failed commit, the DB must remain at
    // the previous consistent state.
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));

    // Create initial state with data
    {
        let mgr = create_shared_manager(&storage);
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("safe-{i:04}");
            wtx.insert(key.as_bytes(), b"committed").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Open with FailingIO — commit will fail
    let (dek, mac_key, _) = test_keys();
    let failing_io = Box::new(FailingIO::new(storage.clone(), 2));
    let mgr = TxnManager::open(failing_io, dek, mac_key, 1, 256).unwrap();

    // Attempt a write that modifies existing data — commit fails
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("safe-{i:04}");
            wtx.insert(key.as_bytes(), b"CORRUPTED").unwrap();
        }
        let result = wtx.commit();
        assert!(result.is_err(), "commit should fail due to I/O error");
    }

    // Drop the manager (clean close)
    drop(mgr);

    // Reopen with normal I/O — database should be at the OLD consistent state
    let mgr = open_shared_manager(&storage);
    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.entry_count(), 100);
    for i in 0..100u32 {
        let key = format!("safe-{i:04}");
        assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(b"committed".to_vec()),
            "key {key} should have original value after failed commit + reopen");
    }
}

// --- CoW ancestor chain completeness ---

#[test]
fn cow_produces_new_root_each_commit() {
    // Verifies that CoW correctly creates a new root on each commit.
    // If the root were modified in-place, readers on old snapshots would
    // see corrupted data (the old root points to new children).
    let storage = std::sync::Arc::new(SharedStorage::new(4 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    // Commit 1: create initial tree
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("cow-{i:04}");
            wtx.insert(key.as_bytes(), b"v1").unwrap();
        }
        wtx.commit().unwrap();
    }

    let root_v1 = mgr.current_slot().tree_root;
    let mut reader_v1 = mgr.begin_read();

    // Commit 2: modify the tree — CoW should create a NEW root
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"cow-0000", b"modified").unwrap();
        wtx.commit().unwrap();
    }

    let root_v2 = mgr.current_slot().tree_root;
    assert_ne!(root_v1, root_v2,
        "CoW must produce a new root page: v1={root_v1:?} v2={root_v2:?}");

    // Reader v1 should still see the old data through the old root
    assert_eq!(reader_v1.get(b"cow-0000").unwrap(), Some(b"v1".to_vec()),
        "old reader must see old value through old root");

    // New reader should see modified data through new root
    let mut reader_v2 = mgr.begin_read();
    assert_eq!(reader_v2.get(b"cow-0000").unwrap(), Some(b"modified".to_vec()));

    // Commit 3: another modification — another new root
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.delete(b"cow-0050").unwrap();
        wtx.commit().unwrap();
    }

    let root_v3 = mgr.current_slot().tree_root;
    assert_ne!(root_v2, root_v3,
        "each commit must produce a new root: v2={root_v2:?} v3={root_v3:?}");
    assert_ne!(root_v1, root_v3,
        "root v3 must differ from v1: v1={root_v1:?} v3={root_v3:?}");

    // All readers still see their correct snapshots
    assert_eq!(reader_v1.get(b"cow-0050").unwrap(), Some(b"v1".to_vec()),
        "v1 reader must still see deleted key");
    assert_eq!(reader_v2.get(b"cow-0050").unwrap(), Some(b"v1".to_vec()),
        "v2 reader must still see deleted key");
    let mut reader_v3 = mgr.begin_read();
    assert_eq!(reader_v3.get(b"cow-0050").unwrap(), None,
        "v3 reader should not see deleted key");
}

// --- Pages freed at reader's txn_id are NOT reclaimable (off-by-one guard) ---

#[test]
fn pages_freed_at_reader_txn_not_reclaimable() {
    // Guards against an off-by-one in oldest_active_reader comparison:
    // If pages freed at txn_id=X are reclaimed while a reader at txn_id=X
    // is still active, the reader could access freed/reused pages.
    let storage = std::sync::Arc::new(SharedStorage::new(8 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    let big_val = vec![0xCCu8; 1800];

    // Insert large entries to create a multi-page tree
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..60u32 {
            let key = format!("ofb-{i:04}");
            wtx.insert(key.as_bytes(), &big_val).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Start a reader RIGHT NOW — this reader's txn_id is the current committed txn_id
    let mut reader = mgr.begin_read();
    assert_eq!(reader.entry_count(), 60);

    // Delete all entries (pages freed at a txn_id >= reader's snapshot)
    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..60u32 {
            let key = format!("ofb-{i:04}");
            wtx.delete(key.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Trigger reclaim attempt — reader should block reclamation
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"trigger", b"x").unwrap();
        wtx.commit().unwrap();
    }

    // Reader MUST still see all 60 entries (pages must not have been reclaimed/reused)
    for i in 0..60u32 {
        let key = format!("ofb-{i:04}");
        assert_eq!(reader.get(key.as_bytes()).unwrap(), Some(big_val.clone()),
            "reader must still see key {key} — freed pages must not be reclaimed \
             while reader at that txn_id is active");
    }

    drop(reader);
}

// --- Interleaved insert-delete stress across many transactions ---

#[test]
fn interleaved_insert_delete_stress() {
    // Stochastic insert/delete across many transactions exercises the full
    // commit/reload cycle under stress. Uses unique keys per insert to avoid
    // cross-transaction tombstone interactions.
    let storage = std::sync::Arc::new(SharedStorage::new(16 * 1024 * 1024));
    let mgr = create_shared_manager(&storage);

    let mut reference: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    let mut rng = SimpleRng(42);
    let mut next_unique = 0u32; // Ensures insert keys are unique across all txns

    for txn_num in 0..200u32 {
        let mut wtx = mgr.begin_write().unwrap();

        // Each transaction does 5-15 operations
        let num_ops = 5 + rng.next() % 11;
        for _ in 0..num_ops {
            let op = rng.next() % 10;

            if op < 6 {
                // Insert: always use a fresh unique key (no tombstone interactions)
                let key = format!("s{next_unique:08}");
                next_unique += 1;
                let val = format!("t{txn_num}-v{}", rng.next() % 1000);
                let is_new = wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
                assert!(is_new, "unique key should always be new: {key}");
                reference.insert(key, val);
            } else if !reference.is_empty() {
                // Delete: pick an existing key from the reference
                let idx = rng.next() as usize % reference.len();
                let key = reference.keys().nth(idx).unwrap().clone();
                let existed = wtx.delete(key.as_bytes()).unwrap();
                assert!(existed, "key {key} should exist in tree for deletion");
                reference.remove(&key);
            }
        }

        assert_eq!(wtx.entry_count(), reference.len() as u64,
            "entry count mismatch at txn {txn_num}: tree={} oracle={}",
            wtx.entry_count(), reference.len());
        wtx.commit().unwrap();

        // Every 50 txns, verify a read transaction sees the correct state
        if txn_num % 50 == 49 {
            let mut rtx = mgr.begin_read();
            assert_eq!(rtx.entry_count(), reference.len() as u64,
                "read txn entry count mismatch at txn {txn_num}");
            // Spot-check 20 keys from the reference
            let keys: Vec<_> = reference.keys().cloned().collect();
            for i in 0..std::cmp::min(20, keys.len()) {
                let key = &keys[i];
                let expected = reference.get(key).unwrap();
                assert_eq!(
                    rtx.get(key.as_bytes()).unwrap(),
                    Some(expected.as_bytes().to_vec()),
                    "value mismatch for key {key} at txn {txn_num}"
                );
            }
        }
    }

    // Final full verification via for_each
    {
        let mut wtx = mgr.begin_write().unwrap();
        let mut collected = Vec::new();
        wtx.for_each(|k, v| {
            collected.push((
                String::from_utf8(k.to_vec()).unwrap(),
                String::from_utf8(v.to_vec()).unwrap(),
            ));
            Ok(())
        }).unwrap();

        let oracle_entries: Vec<_> = reference.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        assert_eq!(collected.len(), oracle_entries.len(),
            "final for_each count mismatch: tree={} oracle={}",
            collected.len(), oracle_entries.len());
        for ((tk, tv), (ok, ov)) in collected.iter().zip(oracle_entries.iter()) {
            assert_eq!(tk, ok, "key mismatch in final scan");
            assert_eq!(tv, ov, "value mismatch in final scan");
        }

        wtx.abort();
    }
}

/// Simple deterministic PRNG for reproducible tests (xorshift32).
struct SimpleRng(u32);
impl SimpleRng {
    fn next(&mut self) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 17;
        self.0 ^= self.0 << 5;
        self.0
    }
}
