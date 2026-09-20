use crate::manager::tests::create_test_manager;

#[test]
fn table_scan_iter_walks_all_entries() {
    let mgr = create_test_manager();
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"t").unwrap();
        for i in 0..10u32 {
            wtx.table_insert(b"t", &i.to_be_bytes(), &[i as u8])
                .unwrap();
        }
        wtx.commit().unwrap();
    }
    let mut rtx = mgr.begin_read();
    let mut iter = rtx.table_scan_iter(b"t", b"").unwrap();
    let mut collected: Vec<(u32, u8)> = Vec::new();
    while let Some((k, v)) = iter.next().unwrap() {
        let mut kbuf = [0u8; 4];
        kbuf.copy_from_slice(k);
        collected.push((u32::from_be_bytes(kbuf), v[0]));
    }
    assert_eq!(collected.len(), 10);
    for (i, (k, v)) in collected.iter().enumerate() {
        assert_eq!(*k as usize, i);
        assert_eq!(*v as usize, i);
    }
}

#[test]
fn table_scan_iter_start_key() {
    let mgr = create_test_manager();
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"t").unwrap();
        for i in 0..10u32 {
            wtx.table_insert(b"t", &i.to_be_bytes(), &[i as u8])
                .unwrap();
        }
        wtx.commit().unwrap();
    }
    let mut rtx = mgr.begin_read();
    let mut iter = rtx.table_scan_iter(b"t", &5u32.to_be_bytes()).unwrap();
    let mut count = 0;
    let mut first_key: Option<u32> = None;
    while let Some((k, _)) = iter.next().unwrap() {
        if first_key.is_none() {
            let mut kbuf = [0u8; 4];
            kbuf.copy_from_slice(k);
            first_key = Some(u32::from_be_bytes(kbuf));
        }
        count += 1;
    }
    assert_eq!(count, 5);
    assert_eq!(first_key, Some(5));
}

#[test]
fn table_scan_iter_enforces_the_same_cumulative_budget() {
    let mgr = create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"t").unwrap();
    writer.table_insert(b"t", b"a", b"123").unwrap();
    writer.table_insert(b"t", b"b", b"456").unwrap();
    writer.commit().unwrap();

    let budget = crate::ReadBudget::new(3, 5);
    let mut reader = mgr.begin_read();
    reader.set_read_budget(Some(budget.clone()));
    let mut iter = reader.table_scan_iter(b"t", b"").unwrap();
    assert!(iter.next().unwrap().is_some());
    let err = iter.next().unwrap_err();

    assert!(matches!(
        err,
        citadel_core::Error::ReadBudgetExceeded {
            size: 3,
            remaining: 2,
            ..
        }
    ));
    assert_eq!(budget.remaining(), 2);
}

#[test]
fn table_scan_iter_empty() {
    let mgr = create_test_manager();
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"t").unwrap();
        wtx.commit().unwrap();
    }
    let mut rtx = mgr.begin_read();
    let mut iter = rtx.table_scan_iter(b"t", b"").unwrap();
    assert!(iter.next().unwrap().is_none());
}

#[test]
fn table_scan_iter_skips_tombstones() {
    let mgr = create_test_manager();
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"t").unwrap();
        for i in 0..5u32 {
            wtx.table_insert(b"t", &i.to_be_bytes(), &[i as u8])
                .unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = mgr.begin_write().unwrap();
        wtx.table_delete(b"t", &2u32.to_be_bytes()).unwrap();
        wtx.commit().unwrap();
    }
    let mut rtx = mgr.begin_read();
    let mut iter = rtx.table_scan_iter(b"t", b"").unwrap();
    let mut keys: Vec<u32> = Vec::new();
    while let Some((k, _)) = iter.next().unwrap() {
        let mut kbuf = [0u8; 4];
        kbuf.copy_from_slice(k);
        keys.push(u32::from_be_bytes(kbuf));
    }
    assert_eq!(keys, vec![0, 1, 3, 4]);
}

#[test]
fn table_scan_iter_write_txn() {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    for i in 0..4u32 {
        wtx.table_insert(b"t", &i.to_be_bytes(), b"v").unwrap();
    }
    let mut iter = wtx.table_scan_iter(b"t", b"").unwrap();
    let mut count = 0;
    while iter.next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 4);
}

#[test]
fn table_scan_iter_returns_the_first_load_error_without_skipping_the_row() {
    use citadel_buffer::cursor::{Cursor, PageLoader, PageMap};
    use citadel_core::types::{PageId, PageType, TxnId, ValueType};
    use citadel_core::{Error, Result};
    use citadel_page::{leaf_node, page::Page};

    struct FallibleAdapter {
        page: Page,
        fail_next: bool,
        loads: usize,
    }
    impl PageMap for FallibleAdapter {
        fn get_page(&self, id: &PageId) -> Option<&Page> {
            (*id == self.page.page_id()).then_some(&self.page)
        }
    }
    impl PageLoader for FallibleAdapter {
        fn ensure_loaded(&mut self, id: PageId) -> Result<()> {
            self.loads += 1;
            if std::mem::take(&mut self.fail_next) {
                return Err(Error::ChecksumMismatch(id));
            }
            Ok(())
        }
    }
    impl super::TxnScanAdapter for FallibleAdapter {
        fn with_loader<R>(
            &mut self,
            f: &mut dyn FnMut(&mut dyn PageLoader) -> Result<R>,
        ) -> Result<R> {
            f(self)
        }
    }

    let id = PageId(7);
    let mut page = Page::new(id, PageType::Leaf, TxnId(1));
    page.rebuild_cells(&[
        &leaf_node::build_cell(b"a", ValueType::Inline, b"first"),
        &leaf_node::build_cell(b"b", ValueType::Inline, b"second"),
    ]);
    let mut adapter = FallibleAdapter {
        page,
        fail_next: false,
        loads: 0,
    };
    let cursor = Cursor::seek_lazy(&mut adapter, id, b"").unwrap();
    adapter.fail_next = true;
    let loads = adapter.loads;
    let mut iter = super::TableIter::new(adapter, cursor);
    assert!(matches!(iter.next(), Err(Error::ChecksumMismatch(page)) if page == id));
    assert_eq!(iter.inner.loads, loads + 1);
    assert_eq!(iter.next().unwrap(), Some((&b"a"[..], &b"first"[..])));
    assert_eq!(iter.next().unwrap(), Some((&b"b"[..], &b"second"[..])));
    assert!(iter.next().unwrap().is_none());
}

#[test]
fn table_scan_iter_preserves_mixed_inline_and_overflow_values_across_adapters() {
    fn assert_entries<T: super::TxnScanAdapter>(
        mut iter: super::TableIter<T>,
        expected: &[(Vec<u8>, Vec<u8>)],
    ) {
        for (key, value) in expected {
            assert_eq!(
                iter.next().unwrap(),
                Some((key.as_slice(), value.as_slice()))
            );
        }
        assert!(iter.next().unwrap().is_none());
        assert!(iter.next().unwrap().is_none());
    }

    let capacity = citadel_page::overflow::OVERFLOW_DATA_CAPACITY;
    let expected = vec![
        (b"a".to_vec(), Vec::new()),
        (b"b".to_vec(), vec![0xB1; capacity * 2 + 17]),
        (b"c".to_vec(), Vec::new()),
        (b"d".to_vec(), b"small".to_vec()),
        (b"e".to_vec(), vec![0xE2; capacity * 4 + 31]),
        (b"f".to_vec(), b"after overflow".to_vec()),
        (b"g".to_vec(), vec![0x73; capacity + 7]),
        (b"h".to_vec(), Vec::new()),
    ];
    let mgr = create_test_manager();
    let mut writer = mgr.begin_write().unwrap();
    writer.create_table(b"t").unwrap();
    for (key, value) in &expected {
        writer.table_insert(b"t", key, value).unwrap();
    }
    writer.commit().unwrap();

    let mut reader = mgr.begin_read();
    assert_entries(reader.table_scan_iter(b"t", b"").unwrap(), &expected);
    assert_entries(
        mgr.begin_read().into_table_scan_iter(b"t", b"").unwrap(),
        &expected,
    );

    let mut writer = mgr.begin_write().unwrap();
    let mut uncommitted = expected.clone();
    uncommitted[4].1 = vec![0xE4; capacity * 3 + 11];
    writer
        .table_insert(b"t", &uncommitted[4].0, &uncommitted[4].1)
        .unwrap();
    uncommitted.push((b"i".to_vec(), b"uncommitted tail".to_vec()));
    writer
        .table_insert(b"t", &uncommitted[8].0, &uncommitted[8].1)
        .unwrap();
    assert_entries(writer.table_scan_iter(b"t", b"").unwrap(), &uncommitted);
}
