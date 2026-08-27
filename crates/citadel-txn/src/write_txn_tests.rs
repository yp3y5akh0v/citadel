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
fn reclaimed_page_zero_is_not_used_as_an_overflow_chain_page() {
    let mgr = create_test_manager();

    // The first CoW frees the initial root at page zero. A second commit moves
    // the other physical slot forward, making page zero reclaimable.
    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"seed", b"one").unwrap();
    wtx.commit().unwrap();

    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"seed", b"two").unwrap();
    wtx.commit().unwrap();

    // Overflow chains encode zero as their terminator. The overflow allocator
    // must leave reclaimed page zero for the tree CoW instead of using it as
    // the first chain page.
    let value = vec![0x5a; citadel_core::MAX_INLINE_VALUE_SIZE + 1];
    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"overflow", &value).unwrap();
    wtx.commit().unwrap();

    let mut rtx = mgr.begin_read();
    assert_eq!(
        rtx.get(b"overflow").unwrap().as_deref(),
        Some(value.as_slice())
    );
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
fn create_table_rejects_uncommitted_and_committed_hash_collisions() {
    const FIRST: &[u8] = b"collision_table_51661";
    const SECOND: &[u8] = b"collision_table_134778";
    const HASH: u32 = 0xab88_afb6;

    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(FIRST).unwrap();
    assert!(matches!(
        wtx.create_table(SECOND),
        Err(citadel_core::Error::NamedTableHashCollision {
            requested,
            existing,
            hash: HASH,
        }) if requested == "collision_table_134778" && existing == "collision_table_51661"
    ));
    wtx.commit().unwrap();

    let mut wtx = mgr.begin_write().unwrap();
    assert!(matches!(
        wtx.create_table(SECOND),
        Err(citadel_core::Error::NamedTableHashCollision {
            requested,
            existing,
            hash: HASH,
        }) if requested == "collision_table_134778" && existing == "collision_table_51661"
    ));
}

#[test]
fn rename_rejects_other_collisions_but_excludes_the_old_name() {
    const FIRST: &[u8] = b"collision_table_51661";
    const SECOND: &[u8] = b"collision_table_134778";

    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(FIRST).unwrap();
    wtx.create_table(b"rename_source").unwrap();
    wtx.commit().unwrap();

    let mut wtx = mgr.begin_write().unwrap();
    assert!(matches!(
        wtx.rename_table(b"rename_source", SECOND),
        Err(citadel_core::Error::NamedTableHashCollision {
            requested,
            existing,
            hash: 0xab88_afb6,
        }) if requested == "collision_table_134778" && existing == "collision_table_51661"
    ));
    wtx.abort();

    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(FIRST).unwrap();
    wtx.commit().unwrap();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.rename_table(FIRST, SECOND).unwrap();
    wtx.commit().unwrap();
    assert_eq!(mgr.list_tables().unwrap()[0].0, SECOND);
}

#[test]
fn nonexistent_colliding_name_never_aliases_a_slot_entry() {
    const FIRST: &[u8] = b"collision_table_51661";
    const MISSING: &[u8] = b"collision_table_134778";

    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(FIRST).unwrap();
    wtx.table_insert(FIRST, b"kept", b"original").unwrap();
    wtx.commit().unwrap();

    // The slot lookup itself aliases these names by construction. Transaction
    // APIs must prove the exact catalog name before they trust this result.
    assert!(mgr.current_slot().named_entry_root(MISSING).is_some());

    let mut rtx = mgr.begin_read();
    assert!(matches!(
        rtx.table_get(MISSING, b"kept"),
        Err(citadel_core::Error::TableNotFound(_))
    ));
    drop(rtx);

    let mut wtx = mgr.begin_write().unwrap();
    assert!(matches!(
        wtx.table_get(MISSING, b"kept"),
        Err(citadel_core::Error::TableNotFound(_))
    ));
    wtx.abort();

    let mut wtx = mgr.begin_write().unwrap();
    assert!(matches!(
        wtx.table_insert(MISSING, b"injected", b"bad"),
        Err(citadel_core::Error::TableNotFound(_))
    ));
    wtx.abort();

    let mut wtx = mgr.begin_write().unwrap();
    assert!(matches!(
        wtx.drop_table(MISSING),
        Err(citadel_core::Error::TableNotFound(_))
    ));
    wtx.abort();

    let mut wtx = mgr.begin_write().unwrap();
    assert!(matches!(
        wtx.rename_table(MISSING, b"renamed_missing"),
        Err(citadel_core::Error::TableNotFound(_))
    ));
    wtx.abort();

    let mut rtx = mgr.begin_read();
    assert_eq!(
        rtx.table_get(FIRST, b"kept").unwrap(),
        Some(b"original".to_vec())
    );
    assert_eq!(rtx.table_get(FIRST, b"injected").unwrap(), None);
    let tables = mgr.list_tables().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].0, FIRST);
    assert_eq!(tables[0].1.entry_count, 1);
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

use citadel_core::{CancelToken, Error, MAX_INLINE_VALUE_SIZE, MAX_KEY_SIZE};

use super::{cancel_on_nth_tree_free, InsertOutcome};

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

#[test]
fn update_sorted_grows_rows_into_overflow_without_loss() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    for i in 0..60u32 {
        let key = format!("k{i:02}");
        wtx.table_insert(b"t", key.as_bytes(), &[b'a'; 120])
            .unwrap();
    }
    let big3k = vec![b'x'; 3000];
    let big10k = vec![b'y'; 10_000];
    let pairs: Vec<(&[u8], &[u8])> = vec![
        (b"k05".as_slice(), big3k.as_slice()),
        (b"k07".as_slice(), big10k.as_slice()),
    ];
    assert_eq!(wtx.table_update_sorted(b"t", &pairs).unwrap(), 2);
    assert_eq!(wtx.table_get(b"t", b"k05").unwrap(), Some(big3k.clone()));
    assert_eq!(wtx.table_get(b"t", b"k07").unwrap(), Some(big10k.clone()));
    assert_eq!(wtx.table_entry_count(b"t").unwrap(), 60);
    wtx.commit().unwrap();

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.table_get(b"t", b"k05").unwrap(), Some(big3k));
    assert_eq!(rtx.table_get(b"t", b"k07").unwrap(), Some(big10k));
}

#[test]
fn update_sorted_preloads_every_cold_leaf_path() {
    let mgr = create_test_manager();
    {
        let mut seed = mgr.begin_write().unwrap();
        seed.create_table(b"t").unwrap();
        for i in 0..400u32 {
            let key = format!("k{i:04}");
            seed.table_insert(b"t", key.as_bytes(), &[b'a'; 120])
                .unwrap();
        }
        seed.commit().unwrap();
    }

    // A fresh write transaction has no table leaf paths resident. The first
    // and last keys deliberately select different leaves.
    let mut wtx = mgr.begin_write().unwrap();
    wtx.ensure_table(b"t").unwrap();
    assert!(wtx.named_trees[b"t".as_slice()].depth > 1);
    let pairs: Vec<(&[u8], &[u8])> = vec![
        (b"k0000".as_slice(), b"first".as_slice()),
        (b"k0399".as_slice(), b"last".as_slice()),
    ];
    assert_eq!(wtx.table_update_sorted(b"t", &pairs).unwrap(), 2);
    wtx.commit().unwrap();

    let mut rtx = mgr.begin_read();
    assert_eq!(
        rtx.table_get(b"t", b"k0000").unwrap(),
        Some(b"first".to_vec())
    );
    assert_eq!(
        rtx.table_get(b"t", b"k0399").unwrap(),
        Some(b"last".to_vec())
    );
}

#[test]
fn update_sorted_shrink_frees_replaced_overflow_chain() {
    let mgr = create_test_manager();
    let big = vec![0xAB; MAX_INLINE_VALUE_SIZE * 4 + 17];
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.table_insert(b"t", b"k", &big).unwrap();

    let before = wtx.pending_free_count();
    let pairs: Vec<(&[u8], &[u8])> = vec![(b"k".as_slice(), b"small".as_slice())];
    assert_eq!(wtx.table_update_sorted(b"t", &pairs).unwrap(), 1);
    let after = wtx.pending_free_count();

    assert!(
        after > before,
        "replaced overflow chain must be freed (before={before}, after={after})"
    );
    assert_eq!(wtx.table_get(b"t", b"k").unwrap(), Some(b"small".to_vec()));
}

#[test]
fn update_sorted_absent_key_frees_staged_chain() {
    let mgr = create_test_manager();
    let big = vec![0xCD; MAX_INLINE_VALUE_SIZE * 3 + 9];
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.table_insert(b"t", b"k1", b"v1").unwrap();

    // The absent key's value is staged to an overflow chain before the tree
    // walk; the skipped pair must release it instead of orphaning the pages.
    let before = wtx.pending_free_count();
    let pairs: Vec<(&[u8], &[u8])> = vec![
        (b"absent".as_slice(), big.as_slice()),
        (b"k1".as_slice(), b"v2".as_slice()),
    ];
    assert_eq!(wtx.table_update_sorted(b"t", &pairs).unwrap(), 1);
    let after = wtx.pending_free_count();

    assert!(
        after > before,
        "staged chain of the skipped pair must be freed (before={before}, after={after})"
    );
    assert_eq!(wtx.table_get(b"t", b"k1").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(wtx.table_get(b"t", b"absent").unwrap(), None);
}

#[test]
fn insert_or_fetch_existing_overflow_value_is_materialized() {
    let mgr = create_test_manager();
    let big = vec![b'z'; MAX_INLINE_VALUE_SIZE * 2 + 5];
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.table_insert(b"t", b"k", &big).unwrap();

    // Inline incoming value: the full existing row must come back, not the ref.
    match wtx.table_insert_or_fetch(b"t", b"k", b"new").unwrap() {
        InsertOutcome::Existed(bytes) => assert_eq!(bytes, big),
        _ => panic!("expected Existed"),
    }

    // Overflow incoming value: its staged chain is dropped, existing returned.
    let incoming = vec![b'w'; MAX_INLINE_VALUE_SIZE + 100];
    let before = wtx.pending_free_count();
    match wtx.table_insert_or_fetch(b"t", b"k", &incoming).unwrap() {
        InsertOutcome::Existed(bytes) => assert_eq!(bytes, big),
        _ => panic!("expected Existed"),
    }
    assert!(wtx.pending_free_count() > before);
    assert_eq!(wtx.table_get(b"t", b"k").unwrap(), Some(big));
}

#[test]
fn upsert_with_materializes_overflow_and_stages_replacement() {
    let mgr = create_test_manager();
    let big = vec![b'o'; 5000];
    let replacement = vec![b'r'; 9000];
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.table_insert(b"t", b"k", &big).unwrap();

    let before = wtx.pending_free_count();
    let out = wtx
        .table_upsert_with::<_, citadel_core::Error>(b"t", b"k", b"default", |old| {
            assert_eq!(
                old,
                big.as_slice(),
                "callback must see the materialized row"
            );
            Ok(UpsertAction::Replace(replacement.clone()))
        })
        .unwrap();
    assert!(matches!(out, UpsertOutcome::Updated));
    assert!(
        wtx.pending_free_count() > before,
        "old overflow chain must be freed"
    );
    assert_eq!(wtx.table_get(b"t", b"k").unwrap(), Some(replacement));
}

#[test]
fn upsert_with_oversized_default_value_is_staged() {
    let mgr = create_test_manager();
    let big_default = vec![b'd'; 9000];
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    // Previously panicked in split_leaf_with_insert: the unstaged 9000-byte
    // cell can never fit a page.
    let out = wtx
        .table_upsert_with::<_, citadel_core::Error>(b"t", b"k", &big_default, |_| {
            Ok(UpsertAction::Skip)
        })
        .unwrap();
    assert!(matches!(out, UpsertOutcome::Inserted));
    assert_eq!(wtx.table_get(b"t", b"k").unwrap(), Some(big_default));
}

#[test]
fn update_range_materializes_overflow_rows_for_callback() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    // Alternate inline and overflow rows under the same range.
    let big = vec![b'q'; 3000];
    let small = vec![b's'; 100];
    for i in 0..6u8 {
        let v = if i % 2 == 0 { &big } else { &small };
        wtx.table_insert(b"t", &[i], v).unwrap();
    }

    let before = wtx.pending_free_count();
    let count = wtx
        .table_update_range::<_, citadel_core::Error>(b"t", &[0u8], |key, value| {
            let expected = if key[0] % 2 == 0 { 3000 } else { 100 };
            assert_eq!(value.len(), expected, "callback must see the full row");
            value[0] = b'P';
            Ok(Some(true))
        })
        .unwrap();
    assert_eq!(count, 6);
    assert!(
        wtx.pending_free_count() > before,
        "rewritten overflow chains must free the old ones"
    );
    for i in 0..6u8 {
        let mut expected = if i % 2 == 0 {
            big.clone()
        } else {
            small.clone()
        };
        expected[0] = b'P';
        assert_eq!(wtx.table_get(b"t", &[i]).unwrap(), Some(expected));
    }
}

#[test]
fn callback_failure_after_a_write_makes_commit_refuse_the_prefix() {
    let mgr = create_test_manager();
    {
        let mut seed = mgr.begin_write().unwrap();
        seed.create_table(b"t").unwrap();
        seed.table_insert(b"t", b"a", b"old-a").unwrap();
        seed.table_insert(b"t", b"b", b"old-b").unwrap();
        seed.commit().unwrap();
    }

    let mut wtx = mgr.begin_write().unwrap();
    let mut visited = 0;
    let error = wtx
        .table_update_range::<_, Error>(b"t", b"a", |_, value| {
            visited += 1;
            if visited == 2 {
                return Err(Error::Sync("injected callback failure".into()));
            }
            value[0] = b'X';
            Ok(Some(true))
        })
        .unwrap_err();
    assert!(matches!(error, Error::Sync(_)));
    assert!(matches!(wtx.check_usable(), Err(Error::TransactionFailed)));
    assert!(matches!(wtx.commit(), Err(Error::TransactionFailed)));

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.table_get(b"t", b"a").unwrap(), Some(b"old-a".to_vec()));
    assert_eq!(rtx.table_get(b"t", b"b").unwrap(), Some(b"old-b".to_vec()));
}

#[test]
fn callback_panic_after_a_write_makes_commit_refuse_the_prefix() {
    let mgr = create_test_manager();
    {
        let mut seed = mgr.begin_write().unwrap();
        seed.create_table(b"t").unwrap();
        seed.table_insert(b"t", b"a", b"old-a").unwrap();
        seed.table_insert(b"t", b"b", b"old-b").unwrap();
        seed.commit().unwrap();
    }

    let mut wtx = mgr.begin_write().unwrap();
    let mut visited = 0;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = wtx.table_update_range::<_, Error>(b"t", b"a", |_, value| {
            visited += 1;
            if visited == 2 {
                panic!("injected callback panic");
            }
            value[0] = b'X';
            Ok(Some(true))
        });
    }));
    assert!(panic.is_err());
    assert!(matches!(wtx.check_usable(), Err(Error::TransactionFailed)));
    assert!(matches!(wtx.commit(), Err(Error::TransactionFailed)));

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.table_get(b"t", b"a").unwrap(), Some(b"old-a".to_vec()));
    assert_eq!(rtx.table_get(b"t", b"b").unwrap(), Some(b"old-b".to_vec()));
}

#[test]
fn cancellation_during_tree_free_makes_truncate_uncommittable() {
    let mgr = create_test_manager();
    {
        let mut seed = mgr.begin_write().unwrap();
        seed.create_table(b"t").unwrap();
        for i in 0..200u32 {
            seed.table_insert(b"t", format!("k{i:04}").as_bytes(), &[b'v'; 120])
                .unwrap();
        }
        seed.commit().unwrap();
    }

    let mut wtx = mgr.begin_write().unwrap();
    let token = CancelToken::new();
    wtx.set_cancel(Some(token));
    let error = {
        let _cancel = cancel_on_nth_tree_free(2);
        wtx.table_truncate(b"t").unwrap_err()
    };
    assert!(matches!(error, Error::Interrupted));
    assert!(wtx.is_poisoned());
    assert!(matches!(wtx.commit(), Err(Error::Interrupted)));

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.table_entry_count(b"t").unwrap(), 200);
}

#[test]
fn drop_table_invalidates_only_its_fk_cache_entry() {
    let mgr = create_test_manager();

    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"parent_a").unwrap();
    wtx.create_table(b"parent_b").unwrap();
    wtx.mark_fk_verified(b"parent_a", b"k1");
    wtx.mark_fk_verified(b"parent_b", b"k2");

    wtx.drop_table(b"parent_b").unwrap();
    assert!(wtx.fk_check_cached(b"parent_a", b"k1"));
    assert!(!wtx.fk_check_cached(b"parent_b", b"k2"));
    wtx.commit().unwrap();
}
