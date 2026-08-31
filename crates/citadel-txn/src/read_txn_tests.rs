use crate::manager::tests::create_test_manager;

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
