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
