use super::*;

const ROWS: u32 = 2_200;
const PARALLEL_BATCH: usize = 256;
const MAX_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Default)]
struct BatchLog {
    calls: Vec<usize>,
    successful_offsets: Vec<u64>,
}

struct BatchRecordingIO {
    inner: CappedCommitIO<FaultingIO>,
    log: Arc<StdMutex<BatchLog>>,
}

impl BatchRecordingIO {
    fn write_one(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.inner.write_page(offset, buf)?;
        self.log.lock().unwrap().successful_offsets.push(offset);
        Ok(())
    }
}

impl PageIO for BatchRecordingIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.inner.read_page(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.log.lock().unwrap().calls.push(1);
        self.write_one(offset, buf)
    }

    fn write_pages_ref(&self, pages: &[(u64, &[u8; PAGE_SIZE])]) -> Result<()> {
        self.log.lock().unwrap().calls.push(pages.len());
        // Preserve partial-write failure semantics. Logging the batch call
        // separately proves the injected error occurs in its second batch.
        for &(offset, buf) in pages {
            self.write_one(offset, buf)?;
        }
        Ok(())
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

fn value(row: u32, generation: u8) -> [u8; 1_024] {
    let mut bytes = [generation.wrapping_mul(71).wrapping_add(row as u8); 1_024];
    bytes[..4].copy_from_slice(&row.to_le_bytes());
    bytes[1_020..].copy_from_slice(&u32::from(generation).to_le_bytes());
    bytes
}

fn write_rows(manager: &TxnManager, generation: u8) -> Result<()> {
    let mut writer = manager.begin_write()?;
    for row in 0..ROWS {
        assert_eq!(
            writer.insert(&row.to_be_bytes(), &value(row, generation))?,
            generation == 1,
            "replacement must preserve the original keys"
        );
    }
    assert_eq!(writer.entry_count(), u64::from(ROWS));
    writer.commit()
}

fn assert_rows(reader: &mut ReadTxn<'_>, generation: u8) {
    assert_eq!(reader.entry_count(), u64::from(ROWS));
    for row in 0..ROWS {
        assert_eq!(
            reader.get(&row.to_be_bytes()).unwrap().as_deref(),
            Some(value(row, generation).as_slice()),
            "row {row}, generation {generation}"
        );
    }
    assert_eq!(reader.get(&ROWS.to_be_bytes()).unwrap(), None);
}

fn take_log(log: &Arc<StdMutex<BatchLog>>) -> BatchLog {
    std::mem::take(&mut *log.lock().unwrap())
}

fn assert_complete_batches(log: &BatchLog, workers: usize) {
    let total = log.successful_offsets.len();
    assert!(
        total > PARALLEL_BATCH,
        "fixture did not cross the bulk threshold"
    );
    assert_ne!(
        total % PARALLEL_BATCH,
        0,
        "fixture must have a partial tail"
    );
    assert_eq!(log.calls.iter().sum::<usize>(), total);
    assert!(log
        .calls
        .iter()
        .all(|&len| len > 0 && len <= PARALLEL_BATCH));
    if workers == 4 {
        assert_eq!(
            log.calls[0], PARALLEL_BATCH,
            "parallel batch was not exercised"
        );
        assert_eq!(log.calls.len(), total.div_ceil(PARALLEL_BATCH));
        assert!(log.calls[..log.calls.len() - 1]
            .iter()
            .all(|&len| len == PARALLEL_BATCH));
        assert_eq!(*log.calls.last().unwrap(), total % PARALLEL_BATCH);
    } else {
        assert!(log.calls.iter().all(|&len| len <= COMMIT_ARENA_PAGES));
        assert_eq!(log.calls.len(), total.div_ceil(COMMIT_ARENA_PAGES));
    }
}

fn page_id_at(offset: u64) -> PageId {
    let delta = offset
        .checked_sub(citadel_core::FILE_HEADER_SIZE as u64)
        .unwrap();
    assert_eq!(delta % PAGE_SIZE as u64, 0);
    PageId(u32::try_from(delta / PAGE_SIZE as u64).unwrap())
}

fn audit_stored_pages(io: &MemIO, log: &BatchLog) {
    let (dek, mac_key, _) = test_keys();
    let mut seen = FxHashSet::default();
    for &offset in &log.successful_offsets {
        let id = page_id_at(offset);
        assert!(
            seen.insert(id),
            "page {id:?} was written twice in one commit"
        );
        let mut stored = [0u8; PAGE_SIZE];
        io.read_page(offset, &mut stored).unwrap();
        let mut body = [0u8; BODY_SIZE];
        page_cipher::decrypt_page(&dek, &mac_key, id, 1, &stored, &mut body).unwrap();
        let page = Page::from_bytes(body);
        assert_eq!(page.page_id(), id, "physical/embedded page IDs disagree");
        assert!(
            page.verify_checksum(),
            "invalid persisted checksum for {id:?}"
        );

        // Compare the actual b2b batch output with the established scalar
        // cipher using the stored random IV, including the complete MAC.
        let iv: &[u8; citadel_core::IV_SIZE] = stored[..citadel_core::IV_SIZE].try_into().unwrap();
        let mut reference = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page_with_iv(
            &dek,
            &mac_key,
            id,
            1,
            page.as_bytes(),
            iv,
            &mut reference,
        );
        assert_eq!(stored, reference, "ciphertext/MAC mismatch for {id:?}");
    }
}

fn assert_integrity(manager: &TxnManager) {
    let report = manager.integrity_check().unwrap();
    assert!(report.is_ok(), "{report:?}");
}

// Four bounded success configurations: two real pool sizes and two commit
// protocols. Each performs an initial bulk commit and one bulk replacement.
#[test]
fn bulk_batches_authenticate_reopen_and_preserve_old_readers() {
    let (dek, mac_key, dek_id) = test_keys();
    for workers in [1, 4] {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap();
        for sync_mode in [SyncMode::Off, SyncMode::Full] {
            pool.install(|| {
                assert_eq!(rayon::current_num_threads(), workers);
                let io = MemIO::new(1024 * 1024);
                let log = Arc::new(StdMutex::new(BatchLog::default()));
                let manager = TxnManager::create_with_sync(
                    Box::new(BatchRecordingIO {
                        inner: CappedCommitIO::new(
                            FaultingIO::new(io.share(), i64::MAX),
                            MAX_BYTES,
                        ),
                        log: Arc::clone(&log),
                    }),
                    dek,
                    mac_key,
                    1,
                    0x1234,
                    dek_id,
                    1_024,
                    sync_mode,
                )
                .unwrap();
                take_log(&log);
                write_rows(&manager, 1).unwrap();
                let first = take_log(&log);
                assert_complete_batches(&first, workers);
                audit_stored_pages(&io, &first);

                let mut pinned = manager.begin_read();
                assert_eq!(
                    pinned.get(&0u32.to_be_bytes()).unwrap(),
                    Some(value(0, 1).to_vec())
                );
                let mut cold_snapshot = manager.begin_read();
                let original_root = manager.current_slot().tree_root;

                write_rows(&manager, 2).unwrap();
                let second = take_log(&log);
                assert_complete_batches(&second, workers);
                audit_stored_pages(&io, &second);
                assert_ne!(manager.current_slot().tree_root, original_root);
                assert_rows(&mut manager.begin_read(), 2);
                assert_rows(&mut pinned, 1);

                // Force the untouched reader to reload its old committed
                // graph from disk, independently of both caches above.
                {
                    let mut shared = manager.pool.lock();
                    for &offset in &first.successful_offsets {
                        shared.invalidate(page_id_at(offset));
                    }
                }
                assert_rows(&mut cold_snapshot, 1);
                assert_integrity(&manager);
                let committed = manager.current_slot();
                drop(cold_snapshot);
                drop(pinned);
                drop(manager);

                let reopened = TxnManager::open_with_sync(
                    Box::new(CappedCommitIO::new(io.share(), MAX_BYTES)),
                    dek,
                    mac_key,
                    1,
                    8,
                    sync_mode,
                )
                .unwrap();
                // Serialization fills the legacy checksum without mutating
                // the in-memory slot; compare the complete persisted format.
                assert_eq!(reopened.current_slot().serialize(), committed.serialize());
                assert_rows(&mut reopened.begin_read(), 2);
                assert_integrity(&reopened);
            });
        }
    }
}

// Two failure configurations, both under a four-worker pool. Fail after a
// complete 256-page batch and three pages of its partial final batch.
#[test]
fn partial_bulk_batch_write_keeps_old_slot_cache_and_disk_snapshot() {
    let (dek, mac_key, dek_id) = test_keys();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    for sync_mode in [SyncMode::Off, SyncMode::Full] {
        pool.install(|| {
            assert_eq!(rayon::current_num_threads(), 4);
            let io = MemIO::new(1024 * 1024);
            let faulty = FaultingIO::new(io.share(), i64::MAX);
            let writes_left = Arc::clone(&faulty.writes_left);
            let log = Arc::new(StdMutex::new(BatchLog::default()));
            let manager = TxnManager::create_with_sync(
                Box::new(BatchRecordingIO {
                    inner: CappedCommitIO::new(faulty, MAX_BYTES),
                    log: Arc::clone(&log),
                }),
                dek,
                mac_key,
                1,
                0x1234,
                dek_id,
                1_024,
                sync_mode,
            )
            .unwrap();
            take_log(&log);
            write_rows(&manager, 1).unwrap();
            let first = take_log(&log);
            assert_complete_batches(&first, 4);
            let before = manager.current_slot();
            let generation = manager.commit_generation();
            let mut pinned = manager.begin_read();
            assert_eq!(
                pinned.get(&0u32.to_be_bytes()).unwrap(),
                Some(value(0, 1).to_vec())
            );
            assert!(manager.pool.lock().is_cached(before.tree_root));
            let mut old_root_ciphertext = [0u8; PAGE_SIZE];
            io.read_page(page_offset(before.tree_root), &mut old_root_ciphertext)
                .unwrap();

            let recovery_marker_writes = if sync_mode == SyncMode::Off { 0 } else { 1 };
            writes_left.store(
                PARALLEL_BATCH as i64 + 3 + recovery_marker_writes,
                Ordering::SeqCst,
            );
            assert!(matches!(write_rows(&manager, 2), Err(Error::Io(_))));
            let failed = take_log(&log);
            assert_eq!(
                failed.calls.len(),
                2,
                "error did not occur in the second batch"
            );
            assert_eq!(failed.calls[0], PARALLEL_BATCH);
            assert!(failed.calls[1] > 3 && failed.calls[1] < PARALLEL_BATCH);
            assert_eq!(failed.successful_offsets.len(), PARALLEL_BATCH + 3);
            audit_stored_pages(&io, &failed);
            assert_eq!(manager.current_slot(), before);
            assert_eq!(manager.commit_generation(), generation);
            assert!(manager.pool.lock().is_cached(before.tree_root));
            assert_rows(&mut pinned, 1);
            assert_rows(&mut manager.begin_read(), 1);
            let mut after_root_ciphertext = [0u8; PAGE_SIZE];
            io.read_page(page_offset(before.tree_root), &mut after_root_ciphertext)
                .unwrap();
            assert_eq!(after_root_ciphertext, old_root_ciphertext);
            manager.begin_write().unwrap().abort();
            drop(pinned);
            drop(manager);

            let reopened = TxnManager::open_with_sync(
                Box::new(CappedCommitIO::new(io.share(), MAX_BYTES)),
                dek,
                mac_key,
                1,
                8,
                sync_mode,
            )
            .unwrap();
            assert_eq!(reopened.current_slot().serialize(), before.serialize());
            assert_rows(&mut reopened.begin_read(), 1);
            assert_integrity(&reopened);
        });
    }
}
