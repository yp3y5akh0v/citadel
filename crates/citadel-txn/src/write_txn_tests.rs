use crate::manager::tests::create_test_manager;
use citadel_core::types::PageId;

#[test]
fn table_prefix_scan_tracks_uncommitted_rows_and_does_not_load_the_next_prefix() {
    let mgr = create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"prefix").unwrap();
    writer.table_insert(b"prefix", b"aa", b"ok").unwrap();
    writer
        .table_insert(b"prefix", b"ba", &vec![0x5a; 32_768])
        .unwrap();
    writer.set_read_budget(Some(crate::ReadBudget::new(8, 16)));
    let mut keys = Vec::new();
    writer
        .table_scan_prefix(b"prefix", b"a", |key, value| {
            keys.push(key.to_vec());
            assert_eq!(value, b"ok");
            Ok(true)
        })
        .unwrap();
    assert_eq!(keys, vec![b"aa".to_vec()]);
    writer.set_read_budget(None);
    let mut all = 0;
    writer
        .table_scan_prefix(b"prefix", b"", |_, _| {
            all += 1;
            Ok(true)
        })
        .unwrap();
    assert_eq!(all, 2);
    let mut missing = 0;
    writer
        .table_scan_prefix(b"prefix", b"az", |_, _| {
            missing += 1;
            Ok(true)
        })
        .unwrap();
    assert_eq!(missing, 0);
    let token = citadel_core::CancelToken::new();
    token.cancel();
    writer.set_cancel(Some(token));
    assert!(matches!(
        writer.table_scan_prefix(b"prefix", b"a", |_, _| panic!("cancelled scan emitted")),
        Err(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn table_root_stamp_tracks_the_write_view_and_root_page_txn() {
    let mgr = create_test_manager();
    let mut create = mgr.begin_write().unwrap();
    create.create_table(b"stamped").unwrap();
    create.table_insert(b"stamped", b"key", b"old").unwrap();
    create.commit().unwrap();

    let committed_stamp = {
        let mut rtx = mgr.begin_read();
        rtx.table_root_stamp(b"stamped").unwrap().unwrap()
    };
    let mut wtx = mgr.begin_write().unwrap();
    assert_eq!(
        wtx.table_root_stamp(b"stamped").unwrap(),
        Some(committed_stamp)
    );
    wtx.table_insert(b"stamped", b"key", b"new").unwrap();
    let write_stamp = wtx.table_root_stamp(b"stamped").unwrap().unwrap();
    assert_ne!(write_stamp, committed_stamp);
    assert_eq!(write_stamp.1, wtx.txn_id());
    wtx.commit().unwrap();

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.table_root_stamp(b"stamped").unwrap(), Some(write_stamp));
    assert_eq!(rtx.table_root_stamp(b"missing").unwrap(), None);
}

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
fn root_delete_reclaims_overflow_across_cold_paths_and_savepoints() {
    exercise_deep_delete(false);
}

#[test]
fn table_delete_reclaims_overflow_across_cold_paths_and_savepoints() {
    exercise_deep_delete(true);
}

fn exercise_deep_delete(named: bool) {
    use crate::manager::tests::{test_keys, MemIO};
    use crate::manager::TxnManager;

    const ROWS: u32 = 384;
    let key = |index: u32| {
        let mut key = vec![b'k'; 512];
        key[..4].copy_from_slice(&index.to_be_bytes());
        key
    };
    let value = |index: u32| {
        vec![
            index as u8;
            if index.is_multiple_of(31) {
                24_000
            } else {
                256
            }
        ]
    };
    let get = |writer: &mut super::WriteTxn<'_>, key: &[u8]| {
        if named {
            writer.table_get(b"deep", key)
        } else {
            writer.get(key)
        }
    };
    let delete = |writer: &mut super::WriteTxn<'_>, key: &[u8]| {
        if named {
            writer.table_delete(b"deep", key)
        } else {
            writer.delete(key)
        }
    };
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let manager =
        TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 32).unwrap();
    let mut seed = manager.begin_write().unwrap();
    if named {
        seed.create_table(b"deep").unwrap();
    }
    for index in 0..ROWS {
        if named {
            seed.table_insert(b"deep", &key(index), &value(index))
                .unwrap();
        } else {
            seed.insert(&key(index), &value(index)).unwrap();
        }
    }
    let tree = if named {
        &seed.named_trees[b"deep".as_slice()]
    } else {
        &seed.tree
    };
    assert!(tree.depth >= 3);
    seed.commit().unwrap();
    drop(manager);

    let manager = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 32).unwrap();
    let mut old_reader = manager.begin_read();
    let mut writer = manager.begin_write().unwrap();
    let checkpoint = writer.begin_savepoint();
    assert!(delete(&mut writer, &key(0)).unwrap());
    assert!(writer.pending_free_count() > 0);
    assert_eq!(get(&mut writer, &key(0)).unwrap(), None);
    writer.restore_snapshot(checkpoint);
    assert_eq!(get(&mut writer, &key(0)).unwrap(), Some(value(0)));
    // Alternate distant leaves, then drain every leaf and collapse the tree.
    for index in (0..ROWS / 2).flat_map(|i| [i, ROWS - 1 - i]) {
        assert!(delete(&mut writer, &key(index)).unwrap());
        assert!(!delete(&mut writer, &key(index)).unwrap());
        assert_eq!(get(&mut writer, &key(index)).unwrap(), None);
    }
    writer.commit().unwrap();
    for index in 0..ROWS {
        let actual = if named {
            old_reader.table_get(b"deep", &key(index)).unwrap()
        } else {
            old_reader.get(&key(index)).unwrap()
        };
        assert_eq!(actual, Some(value(index)));
    }
    drop(old_reader);
    let mut reader = manager.begin_read();
    if named {
        reader
            .table_for_each(b"deep", |_, _| panic!("deleted row survived"))
            .unwrap();
    } else {
        reader
            .for_each(|_, _| panic!("deleted row survived"))
            .unwrap();
    }
    drop(reader);
    assert!(manager.integrity_check().unwrap().is_ok());
    drop(manager);
    let reopened = TxnManager::open(Box::new(io), dek, mac_key, 1, 32).unwrap();
    assert!(reopened.integrity_check().unwrap().is_ok());
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

#[test]
fn insert_or_fetch_budget_failure_after_overflow_staging_poison_transaction() {
    use citadel_core::Error;

    for (max_value, total) in [(4, 64), (64, 4)] {
        let manager = create_test_manager();
        let mut setup = manager.begin_write().unwrap();
        setup.create_table(b"budget").unwrap();
        setup.table_insert(b"budget", b"key", b"original").unwrap();
        setup.commit().unwrap();
        let before = manager.current_slot();
        let generation = manager.commit_generation();

        let mut writer = manager.begin_write().unwrap();
        writer.set_read_budget(Some(crate::ReadBudget::new(max_value, total)));
        let incoming = vec![0x5a; citadel_core::MAX_INLINE_VALUE_SIZE * 2 + 5];
        assert!(matches!(
            writer.table_insert_or_fetch(b"budget", b"key", &incoming),
            Err(Error::ReadBudgetExceeded { size: 8, .. })
        ));
        assert!(
            writer.pending_free_count() > 0,
            "the rejected incoming overflow chain must have been staged and freed"
        );
        assert!(
            writer.is_poisoned(),
            "inline result admission failed after allocator mutation"
        );
        writer.set_read_budget(None);
        assert!(matches!(writer.commit(), Err(Error::TransactionFailed)));
        assert_eq!(manager.current_slot(), before);
        assert_eq!(manager.commit_generation(), generation);
        assert_eq!(
            manager.begin_read().table_get(b"budget", b"key").unwrap(),
            Some(b"original".to_vec())
        );
        let report = manager.integrity_check().unwrap();
        assert!(report.is_ok(), "{report:?}");
    }
}

#[derive(Clone, Copy)]
enum CallbackWriteRoute {
    Upsert,
    Update,
    UpdateWithBuffer,
}

#[test]
fn upsert_split_overflow_and_savepoint_preserve_deep_tree_snapshots() {
    exercise_callback_split_overflow_and_savepoint(CallbackWriteRoute::Upsert);
}

#[test]
fn update_with_split_overflow_and_savepoint_preserve_deep_tree_snapshots() {
    exercise_callback_split_overflow_and_savepoint(CallbackWriteRoute::Update);
}

#[test]
fn update_with_buffer_split_overflow_and_savepoint_preserve_deep_tree_snapshots() {
    exercise_callback_split_overflow_and_savepoint(CallbackWriteRoute::UpdateWithBuffer);
}

fn exercise_callback_split_overflow_and_savepoint(route: CallbackWriteRoute) {
    fn replace_existing(
        writer: &mut super::WriteTxn<'_>,
        key: &[u8],
        old_value: &[u8],
        new_value: &[u8],
        route: CallbackWriteRoute,
    ) {
        if matches!(route, CallbackWriteRoute::Upsert) {
            assert!(matches!(
                writer
                    .table_upsert_with::<_, Error>(b"deep", key, b"unused", |old| {
                        assert_eq!(old, old_value);
                        Ok(UpsertAction::Replace(new_value.to_vec()))
                    })
                    .unwrap(),
                UpsertOutcome::Updated
            ));
        } else {
            let update = |value: &mut Vec<u8>| -> Result<usize, Error> {
                assert_eq!(value, old_value);
                let previous_len = value.len();
                value.clear();
                value.extend_from_slice(new_value);
                Ok(previous_len)
            };
            let previous_len = match route {
                CallbackWriteRoute::Update => writer.table_update_with(b"deep", key, update),
                CallbackWriteRoute::UpdateWithBuffer => {
                    writer.table_update_with_buffer(b"deep", key, &mut Vec::new(), update)
                }
                CallbackWriteRoute::Upsert => unreachable!(),
            }
            .unwrap();
            assert_eq!(previous_len, Some(old_value.len()));
        }
    }
    use crate::manager::tests::{test_keys, MemIO};
    use crate::manager::TxnManager;

    const ROWS: u32 = 384;
    let key = |index: u32| {
        let mut key = vec![b'k'; 512];
        key[..4].copy_from_slice(&index.to_be_bytes());
        key
    };
    let original = vec![b'o'; 256];
    let grown = vec![b'g'; MAX_INLINE_VALUE_SIZE];
    let overflow = vec![b'v'; MAX_INLINE_VALUE_SIZE * 3 + 17];
    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let manager =
        TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 32).unwrap();
    let mut seed = manager.begin_write().unwrap();
    seed.create_table(b"deep").unwrap();
    for index in 0..ROWS {
        seed.table_insert(b"deep", &key(index), &original).unwrap();
    }
    let root = seed.named_trees[b"deep".as_slice()].root;
    assert!(seed.named_trees[b"deep".as_slice()].depth >= 3);
    let first = super::WriteTxn::descend_to_leaf(&mut seed.pages, &manager, root, &key(0)).unwrap();
    let tenth = super::WriteTxn::descend_to_leaf(&mut seed.pages, &manager, root, &key(9)).unwrap();
    assert_eq!(
        first, tenth,
        "fixture must start with a densely packed leaf"
    );
    seed.commit().unwrap();
    drop(manager);

    // Reopen makes the first mutation descend a genuinely cold multi-level tree.
    let manager = TxnManager::open(Box::new(io.share()), dek, mac_key, 1, 32).unwrap();
    let mut old_reader = manager.begin_read();
    let mut writer = manager.begin_write().unwrap();
    writer.ensure_table(b"deep").unwrap();
    let root = writer.named_trees[b"deep".as_slice()].root;
    assert!(writer.named_trees[b"deep".as_slice()].depth >= 3);
    assert!(!writer.pages.contains_key(&root));
    replace_existing(&mut writer, &key(0), &original, &grown, route);
    let root = writer.named_trees[b"deep".as_slice()].root;
    let first =
        super::WriteTxn::descend_to_leaf(&mut writer.pages, &manager, root, &key(0)).unwrap();
    let tenth =
        super::WriteTxn::descend_to_leaf(&mut writer.pages, &manager, root, &key(9)).unwrap();
    assert_ne!(first, tenth, "callback growth must actually split the leaf");

    let checkpoint = writer.begin_savepoint();
    replace_existing(&mut writer, &key(0), &grown, &overflow, route);
    assert!(writer
        .table_insert_if_absent(b"deep", &key(ROWS), b"speculative")
        .unwrap());
    writer.restore_snapshot(checkpoint);
    assert_eq!(
        writer.table_get(b"deep", &key(0)).unwrap(),
        Some(grown.clone())
    );
    assert_eq!(writer.table_get(b"deep", &key(ROWS)).unwrap(), None);

    assert!(matches!(
        writer
            .table_insert_or_fetch(b"deep", &key(ROWS), &overflow)
            .unwrap(),
        InsertOutcome::Inserted
    ));
    assert!(matches!(
        writer
            .table_upsert_with::<_, Error>(b"deep", &key(ROWS + 1), b"appended", |_| {
                panic!("new rightmost key must not invoke the callback")
            })
            .unwrap(),
        UpsertOutcome::Inserted
    ));
    replace_existing(&mut writer, &key(0), &grown, &overflow, route);
    let freed_before_shrink = writer.pending_free_count();
    replace_existing(&mut writer, &key(0), &overflow, b"kept", route);
    assert!(writer.pending_free_count() > freed_before_shrink);
    writer.commit().unwrap();

    // This reader has not read any data before the mutations and rollback.
    let mut index = 0;
    old_reader
        .table_for_each(b"deep", |actual_key, value| {
            assert_eq!(actual_key, key(index));
            assert_eq!(value, original);
            index += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(index, ROWS);
    drop(old_reader);
    let report = manager.integrity_check().unwrap();
    assert!(report.is_ok(), "{report:?}");
    drop(manager);

    let reopened = TxnManager::open(Box::new(io), dek, mac_key, 1, 32).unwrap();
    let mut reader = reopened.begin_read();
    let mut index = 0;
    reader
        .table_for_each(b"deep", |actual_key, value| {
            assert_eq!(actual_key, key(index));
            let expected: &[u8] = match index {
                0 => b"kept",
                ROWS => &overflow,
                i if i == ROWS + 1 => b"appended",
                _ => &original,
            };
            assert_eq!(value, expected);
            index += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(index, ROWS + 2);
    let report = reopened.integrity_check().unwrap();
    assert!(report.is_ok(), "{report:?}");
}

#[test]
fn point_operations_leave_no_staged_pages_after_a_cold_path_read_failure() {
    use crate::manager::tests::{test_keys, MemIO};
    use crate::manager::TxnManager;
    use citadel_core::{Result, PAGE_SIZE};
    use citadel_io::traits::PageIO;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    struct FaultingReadIO {
        inner: MemIO,
        reads_left: Arc<AtomicI64>,
    }

    impl PageIO for FaultingReadIO {
        fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
            if self.reads_left.fetch_sub(1, Ordering::SeqCst) <= 0 {
                return Err(Error::Io(std::io::Error::other(
                    "injected cold-path failure",
                )));
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

    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let manager =
        TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 32).unwrap();
    let mut seed = manager.begin_write().unwrap();
    seed.create_table(b"cold").unwrap();
    for index in 0..128u32 {
        seed.insert(&index.to_be_bytes(), &[b'o'; 256]).unwrap();
        seed.table_insert(b"cold", &index.to_be_bytes(), &[b'o'; 256])
            .unwrap();
    }
    seed.commit().unwrap();
    drop(manager);
    let incoming = vec![b'n'; MAX_INLINE_VALUE_SIZE * 3 + 17];
    for operation in 0..8 {
        let reads_left = Arc::new(AtomicI64::new(i64::MAX));
        let manager = TxnManager::open(
            Box::new(FaultingReadIO {
                inner: io.deep_clone(),
                reads_left: Arc::clone(&reads_left),
            }),
            dek,
            mac_key,
            1,
            32,
        )
        .unwrap();
        let before = manager.current_slot();
        let generation = manager.commit_generation();
        let mut writer = manager.begin_write().unwrap();
        writer.ensure_table(b"cold").unwrap();
        assert!(writer.tree.depth > 1);
        assert!(writer.named_trees[b"cold".as_slice()].depth > 1);
        // Permit the root read, fail the next page before any overflow staging.
        reads_left.store(1, Ordering::SeqCst);
        let key = 64u32.to_be_bytes();
        let result = match operation {
            0 => writer.insert(&key, &incoming).map(|_| ()),
            1 => writer.table_insert(b"cold", &key, &incoming).map(|_| ()),
            2 => writer
                .table_insert_if_absent(b"cold", &key, &incoming)
                .map(|_| ()),
            3 => writer
                .table_insert_or_fetch(b"cold", &key, &incoming)
                .map(|_| ()),
            4 => writer
                .table_upsert_with::<_, Error>(b"cold", &key, &incoming, |_| {
                    panic!("failed tree walk must not invoke the callback")
                })
                .map(|_| ()),
            5 => writer
                .table_update_with::<_, (), Error>(b"cold", &key, |_| {
                    panic!("failed tree walk must not invoke the callback")
                })
                .map(|_| ()),
            6 => writer.get(&key).map(|_| ()),
            7 => writer.delete(&key).map(|_| ()),
            _ => unreachable!(),
        };
        assert!(
            matches!(result, Err(Error::Io(_))),
            "operation {operation}: {result:?}"
        );
        assert!(
            !writer.is_poisoned(),
            "read-only failure poisoned operation {operation}"
        );
        assert!(writer.alloc.allocated_this_txn().is_empty());
        assert_eq!(writer.pending_free_count(), 0);
        reads_left.store(i64::MAX, Ordering::SeqCst);
        writer.commit().unwrap();
        assert_eq!(manager.current_slot(), before);
        assert_eq!(manager.commit_generation(), generation);
        let mut reader = manager.begin_read();
        assert_eq!(reader.get(&key).unwrap(), Some(vec![b'o'; 256]));
        assert_eq!(
            reader.table_get(b"cold", &key).unwrap(),
            Some(vec![b'o'; 256])
        );
        let report = manager.integrity_check().unwrap();
        assert!(report.is_ok(), "{report:?}");
    }
}

#[test]
fn owned_upsert_callback_edits_are_detached_until_replacement() {
    for size in [8, MAX_INLINE_VALUE_SIZE * 3 + 11] {
        let original = vec![0x5a; size];
        let manager = create_test_manager();
        let mut seed = manager.begin_write().unwrap();
        seed.create_table(b"owned").unwrap();
        seed.table_insert(b"owned", b"key", &original).unwrap();
        seed.commit().unwrap();
        let before = manager.current_slot();
        for outcome in 0..3 {
            let mut writer = manager.begin_write().unwrap();
            let marker = writer.mutation_marker();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                writer.table_upsert_with_owned::<_, Error>(
                    b"owned",
                    b"key",
                    b"unused",
                    |mut old| {
                        assert_eq!(old, original);
                        old.clear();
                        old.extend_from_slice(b"speculative");
                        match outcome {
                            0 => Err(Error::Sync(
                                "callback failed after editing owned bytes".into(),
                            )),
                            1 => panic!("callback panicked after editing owned bytes"),
                            2 => Ok(UpsertAction::Skip),
                            _ => unreachable!(),
                        }
                    },
                )
            }));
            match outcome {
                0 => assert!(matches!(result, Ok(Err(Error::Sync(_))))),
                1 => assert!(result.is_err()),
                2 => assert!(matches!(result, Ok(Ok(UpsertOutcome::Skipped)))),
                _ => unreachable!(),
            }
            assert!(!writer.is_poisoned());
            assert!(!writer.mutated_since(marker));
            assert!(writer.alloc.allocated_this_txn().is_empty());
            assert_eq!(writer.pending_free_count(), 0);
            assert_eq!(
                writer.table_get(b"owned", b"key").unwrap(),
                Some(original.clone())
            );
            writer.commit().unwrap();
            assert_eq!(manager.current_slot(), before);
        }
        let mut old_reader = manager.begin_read();
        let mut writer = manager.begin_write().unwrap();
        writer.set_read_budget(Some(crate::ReadBudget::new(size - 1, size)));
        assert!(matches!(
            writer.table_upsert_with_owned::<_, Error>(b"owned", b"key", b"unused", |_| {
                panic!("a denied value must not reach the callback")
            }),
            Err(Error::ReadBudgetExceeded { .. })
        ));
        writer.set_read_budget(Some(crate::ReadBudget::new(size, size)));
        assert!(matches!(
            writer
                .table_upsert_with_owned::<_, Error>(b"owned", b"key", b"unused", |mut old| {
                    assert_eq!(old, original);
                    old.clear();
                    old.extend_from_slice(b"replacement");
                    Ok(UpsertAction::Replace(old))
                })
                .unwrap(),
            UpsertOutcome::Updated
        ));
        writer.set_read_budget(None);
        assert!(matches!(
            writer
                .table_upsert_with_owned::<_, Error>(b"owned", b"new", &original, |_| {
                    panic!("a missing key must not reach the callback")
                })
                .unwrap(),
            UpsertOutcome::Inserted
        ));
        writer.commit().unwrap();
        assert_eq!(
            old_reader.table_get(b"owned", b"key").unwrap(),
            Some(original.clone())
        );
        assert_eq!(old_reader.table_get(b"owned", b"new").unwrap(), None);
        let mut reader = manager.begin_read();
        assert_eq!(
            reader.table_get(b"owned", b"key").unwrap(),
            Some(b"replacement".to_vec())
        );
        assert_eq!(reader.table_get(b"owned", b"new").unwrap(), Some(original));
        assert!(manager.integrity_check().unwrap().is_ok());
    }
}

#[test]
fn update_with_missing_and_tombstoned_keys_never_call_or_insert() {
    use citadel_core::types::ValueType;

    let manager = create_test_manager();
    let mut writer = manager.begin_write().unwrap();
    writer.create_table(b"update").unwrap();
    writer.table_insert(b"update", b"middle", b"kept").unwrap();
    writer
        .named_trees
        .get_mut(b"update".as_slice())
        .unwrap()
        .insert(
            &mut writer.pages,
            &mut writer.alloc,
            writer.txn_id,
            b"tombstone",
            ValueType::Tombstone,
            b"",
        )
        .unwrap();
    let marker = writer.mutation_marker();
    let allocated = writer.alloc.allocated_this_txn().len();
    let freed = writer.pending_free_count();
    for key in [b"before".as_slice(), b"tombstone", b"zz-after"] {
        assert_eq!(
            writer
                .table_update_with::<_, (), Error>(b"update", key, |_| {
                    panic!("a missing or tombstoned key must not invoke the callback")
                })
                .unwrap(),
            None
        );
        assert_eq!(writer.table_get(b"update", key).unwrap(), None);
    }
    assert!(!writer.mutated_since(marker));
    assert_eq!(writer.alloc.allocated_this_txn().len(), allocated);
    assert_eq!(writer.pending_free_count(), freed);
    assert_eq!(
        writer.table_get(b"update", b"middle").unwrap(),
        Some(b"kept".to_vec())
    );
}

#[test]
fn update_with_callback_failure_panic_and_cancellation_leave_owned_changes_unstored() {
    let manager = create_test_manager();
    let mut seed = manager.begin_write().unwrap();
    seed.create_table(b"update").unwrap();
    seed.table_insert(b"update", b"key", b"original").unwrap();
    seed.commit().unwrap();
    let before = manager.current_slot();
    let generation = manager.commit_generation();
    for outcome in 0..3 {
        let mut writer = manager.begin_write().unwrap();
        let token = CancelToken::new();
        writer.set_cancel(Some(token.clone()));
        let marker = writer.mutation_marker();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.table_update_with::<_, (), Error>(b"update", b"key", |value| {
                value.resize(MAX_INLINE_VALUE_SIZE * 3 + 7, 0x5a);
                match outcome {
                    0 => Err(Error::Sync(
                        "callback failed after editing owned bytes".into(),
                    )),
                    1 => panic!("callback panicked after editing owned bytes"),
                    2 => {
                        token.cancel();
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            })
        }));
        match outcome {
            0 => assert!(matches!(result, Ok(Err(Error::Sync(_))))),
            1 => assert!(result.is_err()),
            2 => assert!(matches!(result, Ok(Err(Error::Interrupted)))),
            _ => unreachable!(),
        }
        assert!(!writer.is_poisoned());
        assert!(!writer.mutated_since(marker));
        assert!(writer.alloc.allocated_this_txn().is_empty());
        assert_eq!(writer.pending_free_count(), 0);
        writer.set_cancel(None);
        assert_eq!(
            writer.table_get(b"update", b"key").unwrap(),
            Some(b"original".to_vec())
        );
        writer.commit().unwrap();
        assert_eq!(manager.current_slot(), before);
        assert_eq!(manager.commit_generation(), generation);
    }
}

#[test]
fn update_with_read_budget_admits_once_before_the_callback_and_any_mutation() {
    for size in [8, MAX_INLINE_VALUE_SIZE * 3 + 11] {
        let original = vec![0x5a; size];
        let manager = create_test_manager();
        let mut seed = manager.begin_write().unwrap();
        seed.create_table(b"update").unwrap();
        seed.table_insert(b"update", b"key", &original).unwrap();
        seed.commit().unwrap();
        for (max_value, total) in [(size - 1, size), (size, size - 1)] {
            let mut writer = manager.begin_write().unwrap();
            writer.set_read_budget(Some(crate::ReadBudget::new(max_value, total)));
            let marker = writer.mutation_marker();
            assert!(matches!(
                writer.table_update_with::<_, (), Error>(b"update", b"key", |_| {
                    panic!("a denied value must not reach the callback")
                }),
                Err(Error::ReadBudgetExceeded { .. })
            ));
            assert!(!writer.is_poisoned());
            assert!(!writer.mutated_since(marker));
            assert!(writer.alloc.allocated_this_txn().is_empty());
            assert_eq!(writer.pending_free_count(), 0);
            writer.set_read_budget(None);
            assert_eq!(
                writer.table_get(b"update", b"key").unwrap(),
                Some(original.clone())
            );
            writer.commit().unwrap();
        }
        let mut writer = manager.begin_write().unwrap();
        writer.set_read_budget(Some(crate::ReadBudget::new(size, size)));
        let returned = String::from("consumed once");
        assert_eq!(
            writer
                .table_update_with::<_, _, Error>(b"update", b"key", |value| {
                    assert_eq!(value, &original);
                    value.clear();
                    value.extend_from_slice(b"updated");
                    Ok(returned)
                })
                .unwrap(),
            Some(String::from("consumed once"))
        );
        writer.set_read_budget(None);
        writer.commit().unwrap();
        assert_eq!(
            manager.begin_read().table_get(b"update", b"key").unwrap(),
            Some(b"updated".to_vec())
        );
        let report = manager.integrity_check().unwrap();
        assert!(report.is_ok(), "{report:?}");
    }
}

#[test]
fn update_with_buffer_reuses_bytes_after_failure_missing_key_and_savepoint_restore() {
    let manager = create_test_manager();
    let mut seed = manager.begin_write().unwrap();
    seed.create_table(b"buffered").unwrap();
    seed.table_insert(b"buffered", b"a", b"alpha").unwrap();
    seed.table_insert(b"buffered", b"b", b"bravo").unwrap();
    seed.commit().unwrap();
    let mut old_reader = manager.begin_read();
    let mut writer = manager.begin_write().unwrap();
    let mut buffer = Vec::with_capacity(64);
    buffer.extend_from_slice(b"stale bytes longer than either row");
    let allocation = buffer.as_ptr();

    writer.set_read_budget(Some(crate::ReadBudget::new(5, 5)));
    assert_eq!(
        writer
            .table_update_with_buffer::<_, (), Error>(b"buffered", b"a", &mut buffer, |value| {
                assert_eq!(value.as_ptr(), allocation);
                assert_eq!(value, b"alpha");
                value.copy_from_slice(b"gamma");
                Ok(())
            })
            .unwrap(),
        Some(())
    );
    writer.set_read_budget(None);
    let checkpoint = writer.begin_savepoint();

    for panic in [false, true] {
        let marker = writer.mutation_marker();
        let allocated = writer.alloc.allocated_this_txn().len();
        let freed = writer.pending_free_count();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.table_update_with_buffer::<_, (), Error>(
                b"buffered",
                b"b",
                &mut buffer,
                |value| {
                    assert_eq!(value.as_ptr(), allocation);
                    assert_eq!(value, b"bravo");
                    value.resize(31, 0x5a);
                    if panic {
                        panic!("callback panicked after changing retained scratch");
                    }
                    Err(Error::Sync("callback rejected retained scratch".into()))
                },
            )
        }));
        if panic {
            assert!(result.is_err());
        } else {
            assert!(matches!(result, Ok(Err(Error::Sync(_)))));
        }
        assert!(!writer.is_poisoned());
        assert!(!writer.mutated_since(marker));
        assert_eq!(writer.alloc.allocated_this_txn().len(), allocated);
        assert_eq!(writer.pending_free_count(), freed);
        assert_eq!(
            writer.table_get(b"buffered", b"b").unwrap(),
            Some(b"bravo".to_vec())
        );
    }

    let marker = writer.mutation_marker();
    writer.set_read_budget(Some(crate::ReadBudget::new(4, 5)));
    assert!(matches!(
        writer.table_update_with_buffer::<_, (), Error>(
            b"buffered",
            b"b",
            &mut buffer,
            |_| panic!("a denied value must not reach the callback"),
        ),
        Err(Error::ReadBudgetExceeded { .. })
    ));
    assert_eq!(
        writer
            .table_update_with_buffer::<_, (), Error>(
                b"buffered",
                b"missing",
                &mut buffer,
                |_| panic!("a missing value must not reach the callback"),
            )
            .unwrap(),
        None
    );
    assert!(!writer.mutated_since(marker));
    writer.set_read_budget(Some(crate::ReadBudget::new(5, 5)));
    writer
        .table_update_with_buffer::<_, (), Error>(b"buffered", b"b", &mut buffer, |value| {
            assert_eq!(value.as_ptr(), allocation);
            assert_eq!(value, b"bravo");
            value.clear();
            value.push(b'x');
            Ok(())
        })
        .unwrap();
    writer.set_read_budget(None);
    writer.restore_snapshot(checkpoint);
    writer
        .table_update_with_buffer::<_, (), Error>(b"buffered", b"b", &mut buffer, |value| {
            assert_eq!(value.as_ptr(), allocation);
            assert_eq!(value, b"bravo", "rollback must supersede scratch contents");
            value.copy_from_slice(b"final");
            Ok(())
        })
        .unwrap();
    writer.commit().unwrap();
    assert_eq!(
        old_reader.table_get(b"buffered", b"a").unwrap(),
        Some(b"alpha".to_vec())
    );
    assert_eq!(
        old_reader.table_get(b"buffered", b"b").unwrap(),
        Some(b"bravo".to_vec())
    );
    let mut reader = manager.begin_read();
    assert_eq!(
        reader.table_get(b"buffered", b"a").unwrap(),
        Some(b"gamma".to_vec())
    );
    assert_eq!(
        reader.table_get(b"buffered", b"b").unwrap(),
        Some(b"final".to_vec())
    );
    assert!(manager.integrity_check().unwrap().is_ok());
}

#[test]
fn update_with_buffer_keeps_overflow_materialization_out_of_retained_scratch() {
    let manager = create_test_manager();
    let original = vec![0x5a; MAX_INLINE_VALUE_SIZE * 3 + 17];
    let mut seed = manager.begin_write().unwrap();
    seed.create_table(b"buffered").unwrap();
    seed.table_insert(b"buffered", b"large", &original).unwrap();
    seed.commit().unwrap();
    let mut writer = manager.begin_write().unwrap();
    let mut buffer = Vec::with_capacity(64);
    buffer.extend_from_slice(b"retained");
    let allocation = buffer.as_ptr();
    let capacity = buffer.capacity();
    writer.set_read_budget(Some(crate::ReadBudget::new(original.len(), original.len())));
    assert_eq!(
        writer
            .table_update_with_buffer::<_, (), Error>(b"buffered", b"large", &mut buffer, |value| {
                assert_eq!(value, &original);
                value.clear();
                value.extend_from_slice(b"small");
                Ok(())
            },)
            .unwrap(),
        Some(())
    );
    assert_eq!(buffer, b"retained");
    assert_eq!(buffer.as_ptr(), allocation);
    assert_eq!(buffer.capacity(), capacity);
    writer.set_read_budget(None);
    writer.commit().unwrap();
    assert_eq!(
        manager
            .begin_read()
            .table_get(b"buffered", b"large")
            .unwrap(),
        Some(b"small".to_vec())
    );
    assert!(manager.integrity_check().unwrap().is_ok());
}

#[test]
fn table_contains_key_uses_the_live_write_view_without_materializing_values() {
    use citadel_core::types::{PageType, ValueType};

    let manager = create_test_manager();
    let mut seed = manager.begin_write().unwrap();
    seed.create_table(b"contains").unwrap();
    seed.table_insert(b"contains", b"inline", b"value").unwrap();
    seed.table_insert(b"contains", b"overflow", &vec![0x5a; 32_768])
        .unwrap();
    seed.named_trees
        .get_mut(b"contains".as_slice())
        .unwrap()
        .insert(
            &mut seed.pages,
            &mut seed.alloc,
            seed.txn_id,
            b"tombstone",
            ValueType::Tombstone,
            b"",
        )
        .unwrap();
    seed.commit().unwrap();

    let mut writer = manager.begin_write().unwrap();
    let budget = crate::ReadBudget::new(0, 0);
    writer.set_read_budget(Some(budget.clone()));
    let marker = writer.mutation_marker();
    for (key, expected) in [
        (b"before".as_slice(), false),
        (b"inline", true),
        (b"overflow", true),
        (b"tombstone", false),
        (b"zz-after", false),
    ] {
        assert_eq!(
            writer.table_contains_key(b"contains", key).unwrap(),
            expected
        );
    }
    assert_eq!(budget.remaining(), 0);
    assert!(!writer.mutated_since(marker));
    assert!(writer
        .pages
        .values()
        .all(|page| page.page_type() != Some(PageType::Overflow)));

    writer.table_delete(b"contains", b"inline").unwrap();
    writer
        .table_insert(b"contains", b"new", b"new value")
        .unwrap();
    assert!(!writer.table_contains_key(b"contains", b"inline").unwrap());
    assert!(writer.table_contains_key(b"contains", b"new").unwrap());
    let token = citadel_core::CancelToken::new();
    token.cancel();
    writer.set_cancel(Some(token));
    assert!(matches!(
        writer.table_contains_key(b"contains", b"new"),
        Err(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn update_with_hint_survives_overflow_staging_map_growth_and_frees_each_chain_once() {
    use crate::manager::tests::{test_keys, MemIO};
    use crate::manager::TxnManager;
    use citadel_buffer::btree::BTree;
    use citadel_core::types::ValueType;
    use citadel_page::{leaf_node, overflow};

    let (dek, mac_key, dek_id) = test_keys();
    let io = MemIO::new(1024 * 1024);
    let manager =
        TxnManager::create(Box::new(io.share()), dek, mac_key, 1, 0x1234, dek_id, 32).unwrap();
    let mut seed = manager.begin_write().unwrap();
    seed.create_table(b"hints").unwrap();
    for key in 0..100u8 {
        seed.table_insert(b"hints", &[key], &[key; 8]).unwrap();
    }
    assert_eq!(seed.named_trees[b"hints".as_slice()].depth, 1);
    seed.commit().unwrap();
    let mut old_reader = manager.begin_read();
    let mut writer = manager.begin_write().unwrap();
    writer.ensure_table(b"hints").unwrap();
    let root = writer.named_trees[b"hints".as_slice()].root;
    super::WriteTxn::descend_to_leaf(&mut writer.pages, &manager, root, &[50]).unwrap();
    let capacity = writer.pages.capacity();
    let large = vec![0xa5; (capacity + 2) * overflow::OVERFLOW_DATA_CAPACITY];
    assert!(large.len() <= citadel_core::MAX_VALUE_SIZE);
    let mut buffer = Vec::with_capacity(64);
    writer.set_read_budget(Some(crate::ReadBudget::new(8, 8)));
    assert_eq!(
        writer
            .table_update_with_buffer::<_, _, Error>(b"hints", &[50], &mut buffer, |value| {
                assert_eq!(value, &[50; 8]);
                value.clear();
                value.extend_from_slice(&large);
                Ok(50)
            })
            .unwrap(),
        Some(50)
    );
    assert!(
        writer.pages.capacity() > capacity,
        "staging must actually grow the page map"
    );
    writer.set_read_budget(None);
    let root = writer.named_trees[b"hints".as_slice()].root;
    let leaf = super::WriteTxn::descend_to_leaf(&mut writer.pages, &manager, root, &[50]).unwrap();
    let (kind, payload) = BTree::search_at_leaf_ref(&writer.pages, leaf, &[50])
        .unwrap()
        .unwrap();
    assert_eq!(kind, ValueType::Overflow);
    let mut next = leaf_node::OverflowRef::from_bytes(payload).first_page;
    let mut old_chain = Vec::new();
    while next != PageId(0) {
        old_chain.push(next);
        next = overflow::next_page(&writer.pages[&next]);
    }
    assert!(old_chain.len() > capacity);
    let second = vec![0x5a; large.len() + 17];
    writer
        .table_update_with::<_, (), Error>(b"hints", &[50], |value| {
            assert_eq!(value, &large);
            value.clear();
            value.extend_from_slice(&second);
            Ok(())
        })
        .unwrap();
    for page in &old_chain {
        assert_eq!(
            writer
                .alloc
                .freed_this_txn()
                .iter()
                .filter(|p| *p == page)
                .count(),
            1
        );
    }
    writer
        .table_update_with_buffer::<_, (), Error>(b"hints", &[50], &mut buffer, |value| {
            assert_eq!(value, &second);
            value.clear();
            value.extend_from_slice(b"small");
            Ok(())
        })
        .unwrap();
    for page in &old_chain {
        assert_eq!(
            writer
                .alloc
                .freed_this_txn()
                .iter()
                .filter(|p| *p == page)
                .count(),
            1
        );
    }
    assert_eq!(writer.table_entry_count(b"hints").unwrap(), 100);
    writer.commit().unwrap();
    for key in 0..100u8 {
        assert_eq!(
            old_reader.table_get(b"hints", &[key]).unwrap(),
            Some(vec![key; 8])
        );
    }
    drop(old_reader);
    assert!(manager.integrity_check().unwrap().is_ok());
    drop(manager);
    let reopened = TxnManager::open(Box::new(io), dek, mac_key, 1, 32).unwrap();
    let mut reader = reopened.begin_read();
    for key in 0..100u8 {
        let expected = if key == 50 {
            b"small".to_vec()
        } else {
            vec![key; 8]
        };
        assert_eq!(reader.table_get(b"hints", &[key]).unwrap(), Some(expected));
    }
    assert!(reopened.integrity_check().unwrap().is_ok());
}
