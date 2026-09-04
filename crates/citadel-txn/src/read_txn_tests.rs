use crate::manager::tests::create_test_manager;

#[test]
fn table_prefix_scan_respects_empty_missing_and_early_stop() {
    let mgr = create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"prefix").unwrap();
    for key in [b"aa", b"ab", b"ba"] {
        writer.table_insert(b"prefix", key, key).unwrap();
    }
    writer.commit().unwrap();
    let mut reader = mgr.begin_read();
    for (prefix, expected) in [
        (
            b"".as_slice(),
            vec![b"aa".to_vec(), b"ab".to_vec(), b"ba".to_vec()],
        ),
        (b"a".as_slice(), vec![b"aa".to_vec(), b"ab".to_vec()]),
        (b"az".as_slice(), vec![]),
    ] {
        let mut keys = Vec::new();
        reader
            .table_scan_prefix(b"prefix", prefix, |key, _| {
                keys.push(key.to_vec());
                Ok(true)
            })
            .unwrap();
        assert_eq!(keys, expected);
    }
    let mut emitted = 0;
    reader
        .table_scan_prefix(b"prefix", b"", |_, _| {
            emitted += 1;
            Ok(false)
        })
        .unwrap();
    assert_eq!(emitted, 1);
    let token = citadel_core::CancelToken::new();
    token.cancel();
    reader.set_cancel(Some(token));
    assert!(matches!(
        reader.table_scan_prefix(b"prefix", b"a", |_, _| panic!("cancelled scan emitted")),
        Err(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn read_empty_tree() {
    let mgr = create_test_manager();
    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.entry_count(), 0);
    assert_eq!(rtx.get(b"anything").unwrap(), None);
}

#[test]
fn read_after_write_commit() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"hello", b"world").unwrap();
        wtx.commit().unwrap();
    }

    {
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(rtx.get(b"missing").unwrap(), None);
        assert_eq!(rtx.entry_count(), 1);
    }
}

#[test]
fn point_get_rejects_an_overflow_value_at_the_read_budget_boundary() {
    let mgr = create_test_manager();
    let value = vec![0x5A; citadel_core::MAX_INLINE_VALUE_SIZE + 1];
    let mut writer = mgr.begin_write().unwrap();
    writer.insert(b"large", &value).unwrap();
    writer.commit().unwrap();

    let mut reader = mgr.begin_read();
    reader.set_read_budget(Some(crate::ReadBudget::new(
        value.len() - 1,
        value.len() * 2,
    )));
    let err = reader.get(b"large").unwrap_err();

    assert!(matches!(
        err,
        citadel_core::Error::ReadBudgetExceeded { size, .. } if size == value.len()
    ));
}

#[test]
fn scan_charges_one_shared_total_before_each_inline_row() {
    let mgr = create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"budgeted").unwrap();
    writer.table_insert(b"budgeted", b"a", b"123").unwrap();
    writer.table_insert(b"budgeted", b"b", b"456").unwrap();
    writer.commit().unwrap();

    let budget = crate::ReadBudget::new(3, 5);
    let mut reader = mgr.begin_read();
    reader.set_read_budget(Some(budget.clone()));
    let mut emitted = 0;
    let err = reader
        .table_scan_from(b"budgeted", b"", |_, _| {
            emitted += 1;
            Ok(true)
        })
        .unwrap_err();

    assert!(matches!(
        err,
        citadel_core::Error::ReadBudgetExceeded {
            size: 3,
            remaining: 2,
            ..
        }
    ));
    assert_eq!(emitted, 1, "the rejected row reached the callback");
    assert_eq!(budget.remaining(), 2);
}

#[test]
fn snapshot_isolation() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"key1", b"v1").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"key1").unwrap(), Some(b"v1".to_vec()));

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"key2", b"v2").unwrap();
        wtx.commit().unwrap();
    }

    assert_eq!(rtx.get(b"key2").unwrap(), None);

    let mut rtx2 = mgr.begin_read();
    assert_eq!(rtx2.get(b"key1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(rtx2.get(b"key2").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn contains_key() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"exists", b"yes").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    assert!(rtx.contains_key(b"exists").unwrap());
    assert!(!rtx.contains_key(b"nope").unwrap());
}

#[test]
fn read_named_table() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"mydata").unwrap();
        wtx.table_insert(b"mydata", b"hello", b"world").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    assert_eq!(
        rtx.table_get(b"mydata", b"hello").unwrap(),
        Some(b"world".to_vec())
    );
    assert_eq!(rtx.table_get(b"mydata", b"missing").unwrap(), None);
}

#[test]
fn read_nonexistent_table() {
    let mgr = create_test_manager();
    let mut rtx = mgr.begin_read();
    assert!(matches!(
        rtx.table_get(b"nope", b"key"),
        Err(citadel_core::Error::TableNotFound(_))
    ));
}

#[test]
fn shared_catalog_keeps_off_mode_slot_overrides_snapshot_local() {
    use crate::manager::tests::create_test_manager_with_sync;
    use citadel_core::types::SyncMode;
    use std::sync::Arc;

    let mgr = create_test_manager_with_sync(SyncMode::Off);
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"items").unwrap();
    writer.table_insert(b"items", b"key", b"old").unwrap();
    writer.commit().unwrap();
    let mut old = mgr.begin_read();

    let mut writer = mgr.begin_write().unwrap();
    writer.table_insert(b"items", b"key", b"new").unwrap();
    writer.table_insert(b"items", b"extra", b"row").unwrap();
    writer.commit().unwrap();
    let mut new = mgr.begin_read();

    assert_eq!(old.snapshot.catalog_root, new.snapshot.catalog_root);
    assert!(Arc::ptr_eq(&old.resolved_catalog, &new.resolved_catalog));
    assert_ne!(
        old.snapshot.named_entry_root(b"items"),
        new.snapshot.named_entry_root(b"items")
    );
    assert_eq!(
        new.table_get(b"items", b"key").unwrap(),
        Some(b"new".to_vec())
    );
    assert_eq!(new.table_entry_count(b"items").unwrap(), 2);
    assert_eq!(
        old.table_get(b"items", b"key").unwrap(),
        Some(b"old".to_vec())
    );
    assert_eq!(old.table_entry_count(b"items").unwrap(), 1);
    assert_eq!(old.table_get(b"items", b"extra").unwrap(), None);
}

#[test]
fn catalog_cache_generations_isolate_rename_drop_and_recreate() {
    use citadel_core::Error;
    use std::sync::Arc;

    let mgr = create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"alpha").unwrap();
    writer.table_insert(b"alpha", b"key", b"old").unwrap();
    writer.commit().unwrap();
    let mut original = mgr.begin_read();
    assert_eq!(original.table_entry_count(b"alpha").unwrap(), 1);

    let mut writer = mgr.begin_write().unwrap();
    writer.rename_table(b"alpha", b"beta").unwrap();
    writer.commit().unwrap();
    let mut renamed = mgr.begin_read();
    assert!(!Arc::ptr_eq(
        &original.resolved_catalog,
        &renamed.resolved_catalog
    ));
    assert_eq!(renamed.table_entry_count(b"beta").unwrap(), 1);
    assert!(matches!(renamed.table_root_page(b"alpha"), Ok(None)));

    let mut writer = mgr.begin_write().unwrap();
    writer.drop_table(b"beta").unwrap();
    writer.create_table(b"alpha").unwrap();
    writer.table_insert(b"alpha", b"key", b"new").unwrap();
    writer.commit().unwrap();
    let mut recreated = mgr.begin_read();
    assert!(!Arc::ptr_eq(
        &renamed.resolved_catalog,
        &recreated.resolved_catalog
    ));
    assert_eq!(
        recreated.table_get(b"alpha", b"key").unwrap(),
        Some(b"new".to_vec())
    );
    assert!(matches!(
        recreated.table_get(b"beta", b"key"),
        Err(Error::TableNotFound(_))
    ));
    assert_eq!(
        original.table_get(b"alpha", b"key").unwrap(),
        Some(b"old".to_vec())
    );
    assert_eq!(
        renamed.table_get(b"beta", b"key").unwrap(),
        Some(b"old".to_vec())
    );
}

#[test]
fn recycled_catalog_root_gets_a_new_resolution_cache() {
    use citadel_core::Error;
    use rustc_hash::FxHashMap;
    use std::sync::Arc;

    let mgr = create_test_manager();
    let mut name = b"generation_0".to_vec();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(&name).unwrap();
    writer.commit().unwrap();

    let mut seen = FxHashMap::<_, (Vec<u8>, _)>::default();
    for generation in 1..=128 {
        let mut reader = mgr.begin_read();
        assert_eq!(reader.table_entry_count(&name).unwrap(), 0);
        let root = reader.snapshot.catalog_root;
        if let Some((previous_name, previous_cache)) = seen.get(&root) {
            assert!(!Arc::ptr_eq(previous_cache, &reader.resolved_catalog));
            assert!(matches!(
                reader.table_get(previous_name, b"key"),
                Err(Error::TableNotFound(_))
            ));
            return;
        }
        seen.insert(root, (name.clone(), Arc::clone(&reader.resolved_catalog)));
        drop(reader);

        let next_name = format!("generation_{generation}").into_bytes();
        let mut writer = mgr.begin_write().unwrap();
        writer.rename_table(&name, &next_name).unwrap();
        writer.commit().unwrap();
        name = next_name;
    }
    panic!("fixture did not recycle a catalog root");
}

#[test]
fn shared_catalog_cache_hits_remain_cancellable() {
    use citadel_core::{CancelToken, Error};

    let mgr = create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"items").unwrap();
    writer.commit().unwrap();
    assert_eq!(mgr.begin_read().table_entry_count(b"items").unwrap(), 0);

    let token = CancelToken::new();
    token.cancel();
    let mut cancelled = mgr.begin_read();
    cancelled.set_cancel(Some(token));
    assert!(matches!(
        cancelled.table_root_page(b"items"),
        Err(Error::Interrupted)
    ));
    assert!(matches!(
        cancelled.table_entry_count(b"items"),
        Err(Error::Interrupted)
    ));
    assert_eq!(mgr.begin_read().table_entry_count(b"items").unwrap(), 0);
}

#[test]
fn list_tables_uses_one_catalog_snapshot() {
    let mgr = create_test_manager();

    let mut wtx = mgr.begin_write().unwrap();
    for name in [b"alpha".as_slice(), b"beta".as_slice()] {
        wtx.create_table(name).unwrap();
        wtx.table_insert(name, b"generation", b"old").unwrap();
    }
    wtx.commit().unwrap();

    let mut snapshot = mgr.begin_read();

    let mut wtx = mgr.begin_write().unwrap();
    wtx.drop_table(b"alpha").unwrap();
    wtx.drop_table(b"beta").unwrap();
    wtx.commit().unwrap();
    let mut wtx = mgr.begin_write().unwrap();
    for name in [b"alpha".as_slice(), b"beta".as_slice()] {
        wtx.create_table(name).unwrap();
        wtx.table_insert(name, b"generation", b"new").unwrap();
    }
    wtx.commit().unwrap();

    let mut tables = snapshot.list_tables().unwrap();
    tables.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        tables
            .iter()
            .map(|(name, _)| name.as_slice())
            .collect::<Vec<_>>(),
        vec![b"alpha".as_slice(), b"beta".as_slice()]
    );
    for (name, descriptor) in tables {
        assert_eq!(
            snapshot.table_root_page(&name).unwrap(),
            Some(descriptor.root_page)
        );
        assert_eq!(
            snapshot.table_get(&name, b"generation").unwrap(),
            Some(b"old".to_vec())
        );
    }
}

#[test]
fn reachable_page_read_uses_the_transaction_high_water_mark() {
    let mgr = create_test_manager();
    let snapshot = mgr.begin_read();
    let snapshot_high_water_mark = snapshot.snapshot.high_water_mark;

    let mut writer = mgr.begin_write().unwrap();
    writer.insert(b"future", b"value").unwrap();
    writer.commit().unwrap();
    let future_root = mgr.current_slot().tree_root;
    assert!(future_root.as_u32() >= snapshot_high_water_mark);

    assert!(matches!(
        snapshot.read_reachable_page(future_root),
        Err(citadel_core::Error::PageOutOfBounds(page_id)) if page_id == future_root
    ));
}

#[test]
fn for_each_default_table() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.insert(b"c", b"3").unwrap();
        wtx.insert(b"a", b"1").unwrap();
        wtx.insert(b"b", b"2").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    let mut pairs = Vec::new();
    rtx.for_each(|k, v| {
        pairs.push((k.to_vec(), v.to_vec()));
        Ok(())
    })
    .unwrap();

    assert_eq!(pairs.len(), 3);
    assert_eq!(pairs[0], (b"a".to_vec(), b"1".to_vec()));
    assert_eq!(pairs[1], (b"b".to_vec(), b"2".to_vec()));
    assert_eq!(pairs[2], (b"c".to_vec(), b"3".to_vec()));
}

#[test]
fn for_each_empty_table() {
    let mgr = create_test_manager();
    let mut rtx = mgr.begin_read();
    let mut count = 0;
    rtx.for_each(|_, _| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn table_for_each_named_table() {
    let mgr = create_test_manager();

    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"items").unwrap();
        wtx.table_insert(b"items", b"x", b"10").unwrap();
        wtx.table_insert(b"items", b"y", b"20").unwrap();
        wtx.table_insert(b"items", b"z", b"30").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = mgr.begin_read();
    let mut pairs = Vec::new();
    rtx.table_for_each(b"items", |k, v| {
        pairs.push((k.to_vec(), v.to_vec()));
        Ok(())
    })
    .unwrap();

    assert_eq!(pairs.len(), 3);
    assert_eq!(pairs[0], (b"x".to_vec(), b"10".to_vec()));
    assert_eq!(pairs[1], (b"y".to_vec(), b"20".to_vec()));
    assert_eq!(pairs[2], (b"z".to_vec(), b"30".to_vec()));
}
