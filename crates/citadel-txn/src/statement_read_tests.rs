use citadel_core::{CancelToken, Error, SyncMode, MAX_INLINE_VALUE_SIZE};

use super::{ReadView, StatementReadTxn};
use crate::manager::tests::{create_test_manager, create_test_manager_with_sync, test_keys, MemIO};
use crate::manager::TxnManager;
use crate::ReadBudget;

fn value(view: &mut ReadView<'_, '_>, table: &[u8], key: &[u8]) -> Option<Vec<u8>> {
    view.table_get(table, key).unwrap()
}

fn missing(view: &mut ReadView<'_, '_>, table: &[u8]) {
    assert!(matches!(
        view.table_get(table, b"key"),
        Err(Error::TableNotFound(_))
    ));
}

#[test]
fn statement_snapshot_preserves_same_id_pending_pages_and_private_cache_identity() {
    let manager = create_test_manager();
    let mut seed = manager.begin_write().unwrap();
    seed.create_table(b"t").unwrap();
    seed.table_insert(b"t", b"key", b"committed").unwrap();
    seed.insert(b"main", b"committed").unwrap();
    seed.commit().unwrap();

    let mut committed = manager.begin_read();
    // Populate the shared descriptor cache before the pending view is made.
    assert_eq!(committed.table_entry_count(b"t").unwrap(), 1);
    let committed_generation = committed.commit_generation();
    let mut writer = manager.begin_write().unwrap();
    writer.table_insert(b"t", b"key", b"pending").unwrap();
    writer.table_insert(b"t", b"second", b"pending").unwrap();
    writer.insert(b"main", b"pending").unwrap();
    let stamp = writer.table_root_stamp(b"t").unwrap();
    let txn_id = writer.txn_id();
    let marker = writer.mutation_marker();
    let mut snapshot = writer.read_snapshot().unwrap();
    assert_eq!(writer.txn_id(), txn_id);
    assert!(!writer.mutated_since(marker));
    assert_eq!(manager.reader_count(), 2);

    writer.table_insert(b"t", b"key", b"later").unwrap();
    writer.table_delete(b"t", b"second").unwrap();
    writer.insert(b"main", b"later").unwrap();
    // Exercise Arc isolation, not just allocation of a fresh CoW page ID.
    assert_eq!(writer.table_root_stamp(b"t").unwrap(), stamp);
    let mut view = snapshot.view();
    assert_eq!(view.cache_generation(), None);
    assert_eq!(view.table_root_stamp(b"t").unwrap(), stamp);
    assert_eq!(view.table_entry_count(b"t").unwrap(), 2);
    assert_eq!(value(&mut view, b"t", b"key"), Some(b"pending".to_vec()));
    assert_eq!(value(&mut view, b"t", b"second"), Some(b"pending".to_vec()));
    assert_eq!(view.get(b"main").unwrap(), Some(b"pending".to_vec()));
    assert_eq!(view.entry_count(), 1);
    assert_eq!(
        committed.view().cache_generation(),
        Some(committed_generation)
    );
    assert_eq!(
        committed.table_get(b"t", b"key").unwrap(),
        Some(b"committed".to_vec())
    );
    assert_eq!(committed.table_entry_count(b"t").unwrap(), 1);

    writer.commit().unwrap();
    let mut current = manager.begin_read();
    assert_eq!(
        current.table_get(b"t", b"key").unwrap(),
        Some(b"later".to_vec())
    );
    assert_eq!(current.table_entry_count(b"t").unwrap(), 1);
    assert_eq!(
        value(&mut snapshot.view(), b"t", b"key"),
        Some(b"pending".to_vec())
    );
    drop(snapshot);
    assert_eq!(manager.reader_count(), 2);
    drop(current);
    drop(committed);
    assert_eq!(manager.reader_count(), 0);
}

#[test]
fn statement_snapshot_freezes_pending_catalog_create_rename_drop_and_truncate() {
    for sync in [SyncMode::Full, SyncMode::Off] {
        let manager = create_test_manager_with_sync(sync);
        let mut seed = manager.begin_write().unwrap();
        for table in [b"keep".as_slice(), b"rename", b"drop", b"truncate"] {
            seed.create_table(table).unwrap();
            seed.table_insert(table, b"key", table).unwrap();
        }
        seed.commit().unwrap();
        let mut writer = manager.begin_write().unwrap();
        writer.create_table(b"new").unwrap();
        writer.table_insert(b"new", b"key", b"created").unwrap();
        writer.rename_table(b"rename", b"renamed").unwrap();
        writer.drop_table(b"drop").unwrap();
        writer.table_truncate(b"truncate").unwrap();
        let mut snapshot = writer.read_snapshot().unwrap();

        writer.drop_table(b"new").unwrap();
        writer.rename_table(b"renamed", b"later_name").unwrap();
        writer.create_table(b"drop").unwrap();
        writer
            .table_insert(b"drop", b"key", b"replacement")
            .unwrap();
        writer.table_insert(b"truncate", b"key", b"later").unwrap();
        writer.commit().unwrap();

        let mut view = snapshot.view();
        assert_eq!(value(&mut view, b"keep", b"key"), Some(b"keep".to_vec()));
        assert_eq!(value(&mut view, b"new", b"key"), Some(b"created".to_vec()));
        assert_eq!(
            value(&mut view, b"renamed", b"key"),
            Some(b"rename".to_vec())
        );
        assert_eq!(view.table_entry_count(b"truncate").unwrap(), 0);
        missing(&mut view, b"rename");
        missing(&mut view, b"later_name");
        missing(&mut view, b"drop");
        // A fresh committed lookup must not reuse any pending descriptor.
        let mut current = manager.begin_read();
        assert!(matches!(
            current.table_get(b"new", b"key"),
            Err(Error::TableNotFound(_))
        ));
        assert_eq!(
            current.table_get(b"drop", b"key").unwrap(),
            Some(b"replacement".to_vec())
        );
    }
}

#[test]
fn statement_snapshot_preserves_private_pages_after_savepoint_id_reuse_and_abort() {
    let manager = create_test_manager();
    let mut writer = manager.begin_write().unwrap();
    let saved = writer.begin_savepoint().unwrap();
    writer.create_table(b"old").unwrap();
    writer.table_insert(b"old", b"key", b"captured").unwrap();
    let old_root = writer.table_root_stamp(b"old").unwrap().unwrap().0;
    let mut snapshot = writer.read_snapshot().unwrap();
    writer.restore_snapshot(saved).unwrap();
    writer.create_table(b"replacement").unwrap();
    writer
        .table_insert(b"replacement", b"key", b"reused")
        .unwrap();
    let replacement_root = writer.table_root_stamp(b"replacement").unwrap().unwrap().0;
    assert_eq!(
        replacement_root, old_root,
        "fixture must exercise actual page-ID reuse"
    );
    assert_eq!(
        value(&mut snapshot.view(), b"old", b"key"),
        Some(b"captured".to_vec())
    );
    writer.abort();
    let mut later = manager.begin_write().unwrap();
    later.create_table(b"new_commit").unwrap();
    later
        .table_insert(b"new_commit", b"key", b"committed")
        .unwrap();
    later.commit().unwrap();
    assert_eq!(
        value(&mut snapshot.view(), b"old", b"key"),
        Some(b"captured".to_vec())
    );
    missing(&mut snapshot.view(), b"replacement");
    missing(&mut snapshot.view(), b"new_commit");
}

#[test]
fn statement_snapshot_pins_unloaded_committed_pages_across_later_commits() {
    let (dek, mac, dek_id) = test_keys();
    let manager = TxnManager::create(
        Box::new(MemIO::new(1024 * 1024)),
        dek,
        mac,
        1,
        0x1234,
        dek_id,
        2,
    )
    .unwrap();
    let mut seed = manager.begin_write().unwrap();
    seed.create_table(b"disk").unwrap();
    let old_value = vec![0x5a; 900];
    for id in 0..120u32 {
        seed.table_insert(b"disk", &id.to_be_bytes(), &old_value)
            .unwrap();
    }
    seed.commit().unwrap();
    // This writer has not read any of the table's pages or descriptors.
    let writer = manager.begin_write().unwrap();
    let mut snapshot = writer.read_snapshot().unwrap();
    writer.abort();
    assert_eq!(manager.reader_count(), 1);
    for round in 0..3 {
        let mut later = manager.begin_write().unwrap();
        later.table_truncate(b"disk").unwrap();
        for id in 0..120u32 {
            later
                .table_insert(b"disk", &id.to_be_bytes(), &[round; 900])
                .unwrap();
        }
        later.commit().unwrap();
    }
    let mut view = snapshot.view();
    assert_eq!(view.table_entry_count(b"disk").unwrap(), 120);
    let mut count = 0;
    view.table_scan_raw(b"disk", |_, bytes| {
        assert_eq!(bytes, old_value);
        count += 1;
        true
    })
    .unwrap();
    assert_eq!(count, 120);
    drop(snapshot);
    assert_eq!(manager.reader_count(), 0);
}

#[test]
fn statement_snapshot_uses_sync_off_slot_roots_for_untouched_tables() {
    let manager = create_test_manager_with_sync(SyncMode::Off);
    let mut seed = manager.begin_write().unwrap();
    seed.create_table(b"t").unwrap();
    seed.table_insert(b"t", b"key", b"old").unwrap();
    seed.commit().unwrap();
    let mut change = manager.begin_write().unwrap();
    change.table_insert(b"t", b"key", b"current").unwrap();
    change.table_insert(b"t", b"second", b"current").unwrap();
    change.commit().unwrap();
    let writer = manager.begin_write().unwrap();
    let mut snapshot = writer.read_snapshot().unwrap();
    writer.abort();
    assert_eq!(snapshot.view().table_entry_count(b"t").unwrap(), 2);
    assert_eq!(
        value(&mut snapshot.view(), b"t", b"key"),
        Some(b"current".to_vec())
    );
}

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn assert_all_overflow_read_paths(snapshot: &mut StatementReadTxn<'_>, expected: &Rows) {
    let mut view = snapshot.view();
    for (key, expected_value) in expected {
        assert_eq!(value(&mut view, b"t", key).as_ref(), Some(expected_value));
    }
    for path in 0..7 {
        let mut rows = Vec::new();
        let mut emit = |key: &[u8], value: &[u8]| {
            rows.push((key.to_vec(), value.to_vec()));
            true
        };
        match path {
            0 => view
                .table_scan_from(b"t", b"", |key, val| Ok(emit(key, val)))
                .unwrap(),
            1 => view
                .table_scan_from_fast(b"t", b"", |key, val| Ok(emit(key, val)))
                .unwrap(),
            2 => view
                .table_scan_prefix(b"t", b"k", |key, val| Ok(emit(key, val)))
                .unwrap(),
            3 => view.table_scan_raw(b"t", &mut emit).unwrap(),
            4 => view
                .table_for_each(b"t", |key, val| {
                    emit(key, val);
                    Ok(())
                })
                .unwrap(),
            5 => {
                let mut iter = view.table_scan_iter(b"t", b"").unwrap();
                while let Some((key, val)) = iter.next().unwrap() {
                    emit(key, val);
                }
            }
            6 => {
                let leaves = view.collect_table_leaves(b"t").unwrap();
                view.scan_leaves(&leaves, &mut emit).unwrap();
            }
            _ => unreachable!(),
        }
        assert_eq!(&rows, expected, "read path {path}");
    }
    let leaves = view.collect_table_leaves(b"t").unwrap();
    let rows = std::thread::scope(|scope| {
        let handles: Vec<_> = leaves
            .chunks(1)
            .map(|chunk| {
                let mut scanner = view.shard_scanner();
                scope.spawn(move || {
                    let mut rows = Vec::new();
                    scanner
                        .scan_leaves(chunk, |key, val| {
                            rows.push((key.to_vec(), val.to_vec()));
                            true
                        })
                        .unwrap();
                    rows
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Rows>()
    });
    assert_eq!(&rows, expected);
}

#[test]
fn statement_snapshot_overflow_uses_captured_pages_for_point_stream_and_parallel_reads() {
    let manager = create_test_manager();
    let mut writer = manager.begin_write().unwrap();
    writer.create_table(b"t").unwrap();
    let expected = vec![
        (b"key1".to_vec(), vec![0x41; MAX_INLINE_VALUE_SIZE + 33]),
        (b"key2".to_vec(), vec![0x42; MAX_INLINE_VALUE_SIZE * 3]),
    ];
    for (key, val) in &expected {
        writer.table_insert(b"t", key, val).unwrap();
    }
    writer.insert(b"main", &expected[1].1).unwrap();
    let mut snapshot = writer.read_snapshot().unwrap();
    writer.table_truncate(b"t").unwrap();
    writer.insert(b"main", b"later").unwrap();
    writer.commit().unwrap();
    assert_eq!(
        snapshot.view().get(b"main").unwrap(),
        Some(expected[1].1.clone())
    );
    assert_all_overflow_read_paths(&mut snapshot, &expected);
}

#[test]
fn statement_snapshot_inherits_shared_budget_and_cancellation_and_rejects_poison() {
    let manager = create_test_manager();
    let token = CancelToken::new();
    let mut writer = manager.begin_write().unwrap();
    writer.create_table(b"t").unwrap();
    writer.table_insert(b"t", b"key", b"123").unwrap();
    let large = vec![0x42; MAX_INLINE_VALUE_SIZE + 33];
    writer.table_insert(b"t", b"large", &large).unwrap();
    let budget = ReadBudget::new(3, 5);
    writer.set_read_budget(Some(budget.clone()));
    writer.set_cancel(Some(token.clone()));
    let mut snapshot = writer.read_snapshot().unwrap();
    assert_eq!(
        value(&mut snapshot.view(), b"t", b"key"),
        Some(b"123".to_vec())
    );
    assert_eq!(budget.remaining(), 2);
    // Both the originating writer and snapshot spend the same allowance.
    assert!(matches!(
        writer.table_get(b"t", b"key"),
        Err(Error::ReadBudgetExceeded { .. })
    ));
    assert!(
        matches!(snapshot.view().table_get(b"t", b"large"), Err(Error::ReadBudgetExceeded { size, .. }) if size == large.len())
    );
    assert_eq!(budget.remaining(), 2);
    assert!(matches!(
        snapshot
            .view()
            .table_scan_raw(b"t", |_, _| panic!("over-budget callback")),
        Err(Error::ReadBudgetExceeded { .. })
    ));
    token.cancel();
    assert!(matches!(
        snapshot.view().table_entry_count(b"t"),
        Err(Error::Interrupted)
    ));
    assert!(matches!(writer.read_snapshot(), Err(Error::Interrupted)));
    assert_eq!(manager.reader_count(), 1);
    writer.set_cancel(None);
    writer.mark_failed();
    assert!(matches!(
        writer.read_snapshot(),
        Err(Error::TransactionFailed)
    ));
    assert_eq!(manager.reader_count(), 1);
}

#[test]
fn statement_snapshot_shards_share_owner_budget_and_cancellation() {
    let manager = create_test_manager();
    let token = CancelToken::new();
    let budget = ReadBudget::new(3, 5);
    let mut writer = manager.begin_write().unwrap();
    writer.create_table(b"t").unwrap();
    writer.table_insert(b"t", b"key", b"123").unwrap();
    writer.set_cancel(Some(token.clone()));
    writer.set_read_budget(Some(budget.clone()));
    let mut snapshot = writer.read_snapshot().unwrap();
    let mut view = snapshot.view();
    let leaves = view.collect_table_leaves(b"t").unwrap();
    let mut first = view.shard_scanner();
    let mut second = view.shard_scanner();
    let mut count = 0;
    first
        .scan_leaves(&leaves, |_, _| {
            count += 1;
            true
        })
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(budget.remaining(), 2);
    assert!(matches!(
        second.scan_leaves(&leaves, |_, _| panic!(
            "second shard bypassed shared budget"
        )),
        Err(Error::ReadBudgetExceeded { .. })
    ));
    token.cancel();
    assert!(matches!(
        first.scan_leaves(&leaves, |_, _| panic!("cancelled shard emitted")),
        Err(Error::Interrupted)
    ));
}
