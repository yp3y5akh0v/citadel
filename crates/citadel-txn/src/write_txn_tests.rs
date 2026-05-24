use crate::manager::tests::create_test_manager;
use citadel_core::types::PageId;

#[test]
fn insert_and_get() {
    let mgr = create_test_manager();

    let mut wtx = mgr.begin_write().unwrap();
    assert!(wtx.insert(b"key1", b"val1").unwrap());
    assert_eq!(wtx.get(b"key1").unwrap(), Some(b"val1".to_vec()));
    assert_eq!(wtx.get(b"missing").unwrap(), None);
    wtx.commit().unwrap();
}

#[test]
fn insert_update() {
    let mgr = create_test_manager();

    let mut wtx = mgr.begin_write().unwrap();
    assert!(wtx.insert(b"key", b"v1").unwrap()); // new
    assert!(!wtx.insert(b"key", b"v2").unwrap()); // update
    assert_eq!(wtx.get(b"key").unwrap(), Some(b"v2".to_vec()));
    wtx.commit().unwrap();

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn delete_key() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"a", b"1").unwrap();
        wtx.insert(b"b", b"2").unwrap();
        wtx.commit().unwrap();
    }

    {
        let mut wtx = mgr.begin_write().unwrap();
        assert!(wtx.delete(b"a").unwrap());
        assert!(!wtx.delete(b"nonexistent").unwrap());
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"a").unwrap(), None);
    assert_eq!(rtx.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn abort_discards_changes() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"key", b"value").unwrap();
        wtx.abort();
    }

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), None);
}

#[test]
fn snapshot_and_restore_main_tree() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.set_in_place(false);

    wtx.insert(b"a", b"1").unwrap();
    wtx.insert(b"b", b"2").unwrap();
    let snap = wtx.begin_savepoint();

    wtx.insert(b"c", b"3").unwrap();
    wtx.delete(b"a").unwrap();
    assert_eq!(wtx.get(b"c").unwrap(), Some(b"3".to_vec()));
    assert_eq!(wtx.get(b"a").unwrap(), None);

    wtx.restore_snapshot(snap);

    assert_eq!(wtx.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(wtx.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(wtx.get(b"c").unwrap(), None);

    wtx.commit().unwrap();
    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(rtx.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(rtx.get(b"c").unwrap(), None);
}

#[test]
fn snapshot_reusable_across_multiple_restores() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.set_in_place(false);

    wtx.insert(b"base", b"v").unwrap();
    let snap = wtx.begin_savepoint();

    for i in 0..5 {
        let k = format!("k{i}");
        wtx.insert(k.as_bytes(), b"x").unwrap();
        wtx.restore_snapshot(snap.clone());
        assert_eq!(wtx.get(k.as_bytes()).unwrap(), None);
    }
    assert_eq!(wtx.get(b"base").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn snapshot_restores_named_tables() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.set_in_place(false);

    wtx.create_table(b"t1").unwrap();
    wtx.table_insert(b"t1", b"k1", b"v1").unwrap();
    let snap = wtx.begin_savepoint();

    wtx.create_table(b"t2").unwrap();
    wtx.table_insert(b"t1", b"k2", b"v2").unwrap();
    wtx.table_insert(b"t2", b"k", b"v").unwrap();

    wtx.restore_snapshot(snap);

    assert_eq!(wtx.table_get(b"t1", b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(wtx.table_get(b"t1", b"k2").unwrap(), None);
    let err = wtx.table_get(b"t2", b"k").unwrap_err();
    assert!(matches!(err, citadel_core::Error::TableNotFound(_)));
}

#[test]
fn snapshot_drops_post_snapshot_pages() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.set_in_place(false);

    for i in 0..20u32 {
        let k = format!("k{i:03}");
        wtx.insert(k.as_bytes(), b"x").unwrap();
    }
    let pre_pages: std::collections::HashSet<PageId> = wtx.pages.keys().copied().collect();
    let snap = wtx.begin_savepoint();

    for i in 20..200u32 {
        let k = format!("k{i:03}");
        wtx.insert(k.as_bytes(), b"x").unwrap();
    }

    wtx.restore_snapshot(snap);
    for &page_id in wtx.pages.keys() {
        assert!(
            pre_pages.contains(&page_id),
            "post-savepoint page {page_id:?} leaked"
        );
    }
}

#[test]
fn nested_savepoints_rollback_inner() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.set_in_place(false);

    wtx.insert(b"a", b"1").unwrap();
    let outer = wtx.begin_savepoint();
    wtx.insert(b"b", b"2").unwrap();
    let inner = wtx.begin_savepoint();
    wtx.insert(b"c", b"3").unwrap();

    wtx.restore_snapshot(inner);
    assert_eq!(wtx.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(wtx.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(wtx.get(b"c").unwrap(), None);

    wtx.restore_snapshot(outer);
    assert_eq!(wtx.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(wtx.get(b"b").unwrap(), None);
}

#[test]
fn in_place_toggle_helpers() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    let original = wtx.in_place();
    wtx.set_in_place(!original);
    assert_eq!(wtx.in_place(), !original);
    wtx.set_in_place(original);
    assert_eq!(wtx.in_place(), original);
}

#[test]
fn base_txn_id_stays_fixed_across_savepoints() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    let base = wtx.base_txn_id();
    assert_eq!(wtx.txn_id, base);
    let _snap = wtx.begin_savepoint();
    assert!(wtx.txn_id.as_u64() > base.as_u64());
    assert_eq!(wtx.base_txn_id(), base);
}

#[test]
fn drop_without_commit_aborts() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"key", b"value").unwrap();
    }

    let _wtx2 = mgr.begin_write().unwrap();

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), None);
}

#[test]
fn many_inserts_commit() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..500u32 {
            let key = format!("key-{i:05}");
            let val = format!("val-{i:05}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        assert_eq!(wtx.entry_count(), 500);
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.entry_count(), 500);
    for i in 0..500u32 {
        let key = format!("key-{i:05}");
        let val = format!("val-{i:05}");
        assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
    }
}

#[test]
fn multiple_transactions() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        for i in 0..10u32 {
            let key = format!("k{i}");
            wtx.insert(key.as_bytes(), b"v1").unwrap();
        }
        wtx.commit().unwrap();
    }

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"k0", b"updated").unwrap();
        wtx.delete(b"k5").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"k0").unwrap(), Some(b"updated".to_vec()));
    assert_eq!(rtx.get(b"k5").unwrap(), None);
    assert_eq!(rtx.get(b"k1").unwrap(), Some(b"v1".to_vec()));
}

#[test]
fn key_too_large() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    let big_key = vec![0u8; MAX_KEY_SIZE + 1];
    assert!(matches!(
        wtx.insert(&big_key, b"val"),
        Err(citadel_core::Error::KeyTooLarge { .. })
    ));
}

#[test]
fn value_above_inline_round_trips_via_overflow() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    let big_val = vec![0xAB; MAX_INLINE_VALUE_SIZE + 1];
    assert!(wtx.insert(b"key", &big_val).unwrap());
    assert_eq!(wtx.get(b"key").unwrap(), Some(big_val));
}

#[test]
fn value_above_absolute_cap_is_rejected() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    let too_big = vec![0u8; citadel_core::MAX_VALUE_SIZE + 1];
    assert!(matches!(
        wtx.insert(b"key", &too_big),
        Err(citadel_core::Error::ValueTooLarge { .. })
    ));
}

#[test]
fn commit_updates_slot() {
    let mgr = create_test_manager();

    let slot_before = mgr.current_slot();
    assert_eq!(slot_before.tree_entries, 0);

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"key", b"val").unwrap();
        wtx.commit().unwrap();
    }

    let slot_after = mgr.current_slot();
    assert_eq!(slot_after.tree_entries, 1);
    assert!(slot_after.txn_id.as_u64() > slot_before.txn_id.as_u64());
    assert_ne!(slot_after.tree_root, slot_before.tree_root);
}

#[test]
fn create_table_and_insert() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"users").unwrap();
        wtx.table_insert(b"users", b"alice", b"admin").unwrap();
        wtx.table_insert(b"users", b"bob", b"user").unwrap();
        wtx.commit().unwrap();
    }

    let rtx = mgr.begin_read();
    assert_eq!(rtx.entry_count(), 0);
}

#[test]
fn table_not_found() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    assert!(matches!(
        wtx.table_insert(b"nonexistent", b"k", b"v"),
        Err(citadel_core::Error::TableNotFound(_))
    ));
}

#[test]
fn table_already_exists() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"test").unwrap();
    assert!(matches!(
        wtx.create_table(b"test"),
        Err(citadel_core::Error::TableAlreadyExists(_))
    ));
}

#[test]
fn table_for_each_named() {
    let mgr = create_test_manager();

    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"data").unwrap();
    wtx.table_insert(b"data", b"b", b"2").unwrap();
    wtx.table_insert(b"data", b"a", b"1").unwrap();
    wtx.table_insert(b"data", b"c", b"3").unwrap();

    let mut pairs = Vec::new();
    wtx.table_for_each(b"data", |k, v| {
        pairs.push((k.to_vec(), v.to_vec()));
        Ok(())
    })
    .unwrap();

    assert_eq!(pairs.len(), 3);
    assert_eq!(pairs[0], (b"a".to_vec(), b"1".to_vec()));
    assert_eq!(pairs[1], (b"b".to_vec(), b"2".to_vec()));
    assert_eq!(pairs[2], (b"c".to_vec(), b"3".to_vec()));
    wtx.commit().unwrap();
}

use citadel_core::MAX_INLINE_VALUE_SIZE;
use citadel_core::MAX_KEY_SIZE;

use super::InsertOutcome;

#[test]
fn insert_or_fetch_new_key_returns_inserted() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    let out = wtx.table_insert_or_fetch(b"t", b"k", b"v").unwrap();
    assert!(matches!(out, InsertOutcome::Inserted));
    assert_eq!(wtx.table_get(b"t", b"k").unwrap(), Some(b"v".to_vec()));
    wtx.commit().unwrap();
}

#[test]
fn insert_or_fetch_existing_key_returns_existed_with_value() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.table_insert(b"t", b"k", b"old").unwrap();

    let out = wtx.table_insert_or_fetch(b"t", b"k", b"new").unwrap();
    match out {
        InsertOutcome::Existed(bytes) => assert_eq!(bytes, b"old"),
        _ => panic!("expected Existed"),
    }
    assert_eq!(wtx.table_get(b"t", b"k").unwrap(), Some(b"old".to_vec()));
    wtx.commit().unwrap();
}

#[test]
fn insert_or_fetch_does_not_overwrite_on_conflict() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.table_insert(b"t", b"k", b"first").unwrap();
    let _ = wtx.table_insert_or_fetch(b"t", b"k", b"second").unwrap();
    let _ = wtx.table_insert_or_fetch(b"t", b"k", b"third").unwrap();
    assert_eq!(wtx.table_get(b"t", b"k").unwrap(), Some(b"first".to_vec()));
    wtx.commit().unwrap();
}

#[test]
fn insert_or_fetch_large_value_boundary() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    let big = vec![b'x'; MAX_INLINE_VALUE_SIZE - 16];
    assert!(matches!(
        wtx.table_insert_or_fetch(b"t", b"k", &big).unwrap(),
        InsertOutcome::Inserted
    ));
    match wtx.table_insert_or_fetch(b"t", b"k", &big).unwrap() {
        InsertOutcome::Existed(bytes) => assert_eq!(bytes.len(), big.len()),
        _ => panic!("expected Existed"),
    }
    wtx.commit().unwrap();
}

#[test]
fn insert_or_fetch_empty_value() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    assert!(matches!(
        wtx.table_insert_or_fetch(b"t", b"k", b"").unwrap(),
        InsertOutcome::Inserted
    ));
    match wtx.table_insert_or_fetch(b"t", b"k", b"x").unwrap() {
        InsertOutcome::Existed(bytes) => assert!(bytes.is_empty()),
        _ => panic!("expected Existed"),
    }
    wtx.commit().unwrap();
}

#[test]
fn insert_or_fetch_multi_row_sequential() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    for i in 0u8..32 {
        let out = wtx.table_insert_or_fetch(b"t", &[i], b"initial").unwrap();
        assert!(matches!(out, InsertOutcome::Inserted));
    }
    for i in 0u8..32 {
        match wtx.table_insert_or_fetch(b"t", &[i], b"other").unwrap() {
            InsertOutcome::Existed(bytes) => assert_eq!(bytes, b"initial"),
            _ => panic!("expected Existed for key {i}"),
        }
    }
    wtx.commit().unwrap();
}

#[test]
fn insert_or_fetch_persists_across_read_txn() {
    let mgr = create_test_manager();
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"t").unwrap();
        wtx.table_insert_or_fetch(b"t", b"k", b"v").unwrap();
        wtx.commit().unwrap();
    }
    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.table_get(b"t", b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn insert_or_fetch_abort_rolls_back_insert() {
    let mgr = create_test_manager();
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"t").unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = mgr.begin_write().unwrap();
        let _ = wtx.table_insert_or_fetch(b"t", b"k", b"v").unwrap();
        wtx.abort();
    }
    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.table_get(b"t", b"k").unwrap(), None);
}

use super::{UpsertAction, UpsertOutcome};

#[test]
fn upsert_with_new_key_inserts_default() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    let out = wtx
        .table_upsert_with::<_, citadel_core::Error>(b"t", b"k", b"default", |_| {
            Ok(UpsertAction::Replace(b"unused".to_vec()))
        })
        .unwrap();
    assert!(matches!(out, UpsertOutcome::Inserted));
    assert_eq!(
        wtx.table_get(b"t", b"k").unwrap(),
        Some(b"default".to_vec())
    );
    wtx.commit().unwrap();
}

#[test]
fn upsert_with_existing_key_replace() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.table_insert(b"t", b"k", b"old").unwrap();
    let out = wtx
        .table_upsert_with::<_, citadel_core::Error>(b"t", b"k", b"default", |old| {
            assert_eq!(old, b"old");
            Ok(UpsertAction::Replace(b"new".to_vec()))
        })
        .unwrap();
    assert!(matches!(out, UpsertOutcome::Updated));
    assert_eq!(wtx.table_get(b"t", b"k").unwrap(), Some(b"new".to_vec()));
    wtx.commit().unwrap();
}

#[test]
fn upsert_with_skip_leaves_cell_unchanged() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.table_insert(b"t", b"k", b"keep").unwrap();
    let out = wtx
        .table_upsert_with::<_, citadel_core::Error>(b"t", b"k", b"default", |_| {
            Ok(UpsertAction::Skip)
        })
        .unwrap();
    assert!(matches!(out, UpsertOutcome::Skipped));
    assert_eq!(wtx.table_get(b"t", b"k").unwrap(), Some(b"keep".to_vec()));
    wtx.commit().unwrap();
}

#[test]
fn upsert_with_closure_error_propagates() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.table_insert(b"t", b"k", b"v").unwrap();
    let err = wtx
        .table_upsert_with::<_, citadel_core::Error>(b"t", b"k", b"default", |_| {
            Err(citadel_core::Error::ValueTooLarge { size: 1, max: 1 })
        })
        .unwrap_err();
    assert!(matches!(err, citadel_core::Error::ValueTooLarge { .. }));
}

#[test]
fn upsert_with_sequential_inserts_via_lil() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    for i in 0u8..50 {
        let out = wtx
            .table_upsert_with::<_, citadel_core::Error>(b"t", &[i], b"v", |_| {
                Ok(UpsertAction::Replace(b"unused".to_vec()))
            })
            .unwrap();
        assert!(matches!(out, UpsertOutcome::Inserted));
    }
    for i in 0u8..50 {
        assert_eq!(wtx.table_get(b"t", &[i]).unwrap(), Some(b"v".to_vec()));
    }
    wtx.commit().unwrap();
}

#[test]
fn upsert_with_update_then_persist() {
    let mgr = create_test_manager();
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"ct").unwrap();
        wtx.table_insert(b"ct", b"hot", b"\x00\x00\x00\x00\x00\x00\x00\x00")
            .unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = mgr.begin_write().unwrap();
        for _ in 0..5 {
            wtx.table_upsert_with::<_, citadel_core::Error>(b"ct", b"hot", b"ignored", |old| {
                let cur = i64::from_le_bytes(old.try_into().unwrap());
                let next = cur + 1;
                Ok(UpsertAction::Replace(next.to_le_bytes().to_vec()))
            })
            .unwrap();
        }
        wtx.commit().unwrap();
    }
    let mut rtx = mgr.begin_read();
    let got = rtx.table_get(b"ct", b"hot").unwrap().unwrap();
    assert_eq!(i64::from_le_bytes(got.try_into().unwrap()), 5);
}

#[test]
fn shrink_overwrite_frees_overflow_chain() {
    let mgr = create_test_manager();
    let big = vec![0xAB; MAX_INLINE_VALUE_SIZE * 4 + 17];

    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.table_insert(b"t", b"k", &big).unwrap();
    let before = wtx.pending_free_count();
    wtx.table_insert(b"t", b"k", b"small").unwrap();
    let after = wtx.pending_free_count();

    assert!(
        after > before,
        "expected overflow chain pages to be freed (before={before}, after={after})"
    );
    assert_eq!(wtx.table_get(b"t", b"k").unwrap(), Some(b"small".to_vec()));
}

#[test]
fn shrink_overwrite_default_tree_frees_overflow_chain() {
    let mgr = create_test_manager();
    let big = vec![0xCD; MAX_INLINE_VALUE_SIZE * 4 + 17];

    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"k", &big).unwrap();
    let before = wtx.pending_free_count();
    wtx.insert(b"k", b"small").unwrap();
    let after = wtx.pending_free_count();

    assert!(
        after > before,
        "expected overflow chain pages to be freed (before={before}, after={after})"
    );
    assert_eq!(wtx.get(b"k").unwrap(), Some(b"small".to_vec()));
}
