//! Recovery must preserve the selected commit across an abandoned candidate
//! followed by a failing secure-delete retry. Integrity still audits both
//! physical slots, including the abandoned candidate's retired pages.
use super::*;
use citadel_io::file_manager::{read_commit_slot, write_commit_slot};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};

struct RetryProbeIO {
    inner: FaultingIO,
    fail_selector: Arc<AtomicBool>,
    stop_after_zero: Arc<AtomicU64>,
}

impl PageIO for RetryProbeIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.inner.read_page(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.inner.write_page(offset, buf)?;
        if offset == self.stop_after_zero.load(Ordering::SeqCst)
            && buf.iter().all(|&byte| byte == 0)
        {
            // Preserve the accepted erase, then fail every later write before
            // the inactive slot can be replaced by this retry.
            self.inner.writes_left.store(0, Ordering::SeqCst);
        }
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.inner.read_at(offset, buf)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        if offset == citadel_core::GOD_BYTE_OFFSET as u64
            && self.fail_selector.load(Ordering::SeqCst)
        {
            self.inner.writes_left.store(0, Ordering::SeqCst);
        }
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

fn open_probe(
    io: &MemIO,
    fail_selector: &Arc<AtomicBool>,
    stop_after_zero: &Arc<AtomicU64>,
) -> (TxnManager, Arc<AtomicI64>) {
    let (dek, mac_key, _) = test_keys();
    let faulty = FaultingIO::new(io.share(), i64::MAX);
    let writes_left = Arc::clone(&faulty.writes_left);
    let manager = TxnManager::open_with_sync(
        Box::new(RetryProbeIO {
            inner: faulty,
            fail_selector: Arc::clone(fail_selector),
            stop_after_zero: Arc::clone(stop_after_zero),
        }),
        dek,
        mac_key,
        1,
        32,
        SyncMode::Off,
    )
    .unwrap();
    (manager, writes_left)
}

#[test]
fn secure_delete_retry_preserves_selected_commit_and_reports_abandoned_candidate() {
    let (dek, mac_key, _) = test_keys();
    let io = consuming_head_base(SyncMode::Off);
    let fail_selector = Arc::new(AtomicBool::new(false));
    let stop_after_zero = Arc::new(AtomicU64::new(u64::MAX));
    let (manager, _) = open_probe(&io, &fail_selector, &stop_after_zero);

    // Make old retirement entries available as loans. Secure delete stays off
    // for fixture preparation and the unpublished candidate.
    commit_insert(&manager, b"key", b"committed");
    let committed = manager.current_slot();
    let active = manager.state.lock().active_slot;
    let retirement_ages: FxHashMap<_, _> =
        pending_chain_pages(&manager, committed.pending_free_root)
            .iter()
            .flat_map(|page| pending_free::read_page_entries(page).unwrap())
            .map(|entry| (entry.page_id, entry.freed_at_txn))
            .collect();
    assert!(manager.state.lock().reclaimed_pages.len() > 100);

    // Build a multi-leaf candidate from reclaimed pages. Fail only its final
    // selector write: all data and the authenticated inactive slot land.
    let mut writer = manager.begin_write().unwrap();
    for id in 0..96u32 {
        writer.insert(&id.to_be_bytes(), &[b'n'; 1_024]).unwrap();
    }
    let unpublished_txn = writer.txn_id();
    fail_selector.store(true, Ordering::SeqCst);
    assert!(matches!(writer.commit(), Err(Error::Io(_))));
    assert_eq!(manager.current_slot().serialize(), committed.serialize());
    let candidate = read_commit_slot(&io, 1 - active).unwrap();
    assert_eq!(candidate.txn_id, unpublished_txn);
    assert_eq!(candidate.high_water_mark, committed.high_water_mark);
    assert!(candidate.verify_checksum() && candidate.verify_mac(&mac_key));
    let target = candidate.tree_root;
    assert_ne!(target, committed.tree_root);
    let retired_at = retirement_ages[&target];
    assert!(retired_at < committed.txn_id);
    let before_retry_report = manager.integrity_check().unwrap();
    assert!(before_retry_report.is_ok(), "{before_retry_report:?}");
    drop(manager);

    // A process restart selects the committed slot and naturally clears loans.
    // Thus the following forced no-data commit does not overwrite candidate
    // pages through allocator reuse: its page writes are retirement erasures.
    fail_selector.store(false, Ordering::SeqCst);
    stop_after_zero.store(page_offset(target), Ordering::SeqCst);
    let (retry, _) = open_probe(&io, &fail_selector, &stop_after_zero);
    assert_eq!(retry.current_slot().serialize(), committed.serialize());
    assert!(retry.state.lock().reclaimed_pages.is_empty());
    retry.set_secure_delete(true);
    let mut writer = retry.begin_write().unwrap();
    writer.refresh_all_catalog_descriptors(&[]).unwrap();
    assert!(matches!(writer.commit(), Err(Error::Io(_))));
    assert_eq!(read_commit_slot(&io, 1 - active).unwrap(), candidate);
    let mut target_bytes = [0xff; PAGE_SIZE];
    io.read_page(page_offset(target), &mut target_bytes)
        .unwrap();
    assert!(target_bytes.iter().all(|&byte| byte == 0));
    drop(retry);

    let recovered =
        TxnManager::open_with_sync(Box::new(io.share()), dek, mac_key, 1, 32, SyncMode::Off)
            .unwrap();
    assert_eq!(recovered.current_slot().serialize(), committed.serialize());
    assert_eq!(
        recovered.begin_read().get(b"key").unwrap().as_deref(),
        Some(b"committed".as_slice())
    );
    assert_eq!(
        recovered.begin_read().get(&0u32.to_be_bytes()).unwrap(),
        None
    );
    let report = recovered.integrity_check().unwrap();
    assert!(report.errors.iter().any(
        |error| matches!(error, integrity::IntegrityError::PageTampered(id) if *id == target)
    ));

    // Explicitly audit the selected graph in a separate copy, preserving the
    // original physical-slot failure report above instead of hiding it.
    let selected_image = io.deep_clone();
    for index in 0..2 {
        write_commit_slot(&selected_image, index, &committed).unwrap();
    }
    let selected =
        TxnManager::open_with_sync(Box::new(selected_image), dek, mac_key, 1, 32, SyncMode::Off)
            .unwrap();
    let selected_report = selected.integrity_check().unwrap();
    assert!(selected_report.is_ok(), "{selected_report:?}");
}
