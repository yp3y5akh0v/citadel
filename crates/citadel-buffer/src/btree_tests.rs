use super::*;

fn new_tree() -> (FxHashMap<PageId, Page>, PageAllocator, BTree) {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(0);
    let tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    (pages, alloc, tree)
}

#[test]
fn empty_tree_search() {
    let (pages, _, tree) = new_tree();
    assert_eq!(tree.search(&pages, b"anything").unwrap(), None);
}

#[test]
fn insert_and_search_single() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    let is_new = tree
        .insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            b"hello",
            ValueType::Inline,
            b"world",
        )
        .unwrap();
    assert!(is_new);
    assert_eq!(tree.entry_count, 1);

    let result = tree.search(&pages, b"hello").unwrap();
    assert_eq!(result, Some((ValueType::Inline, b"world".to_vec())));
}

#[test]
fn insert_update_existing() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"key",
        ValueType::Inline,
        b"v1",
    )
    .unwrap();
    let is_new = tree
        .insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            b"key",
            ValueType::Inline,
            b"v2",
        )
        .unwrap();
    assert!(!is_new);
    assert_eq!(tree.entry_count, 1);

    let result = tree.search(&pages, b"key").unwrap();
    assert_eq!(result, Some((ValueType::Inline, b"v2".to_vec())));
}

#[test]
fn insert_multiple_sorted() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    let keys = [b"dog", b"ant", b"cat", b"fox", b"bat", b"eel"];
    for k in &keys {
        tree.insert(&mut pages, &mut alloc, TxnId(1), *k, ValueType::Inline, *k)
            .unwrap();
    }
    assert_eq!(tree.entry_count, 6);

    for k in &keys {
        let result = tree.search(&pages, *k).unwrap();
        assert_eq!(result, Some((ValueType::Inline, k.to_vec())));
    }

    assert_eq!(tree.search(&pages, b"zebra").unwrap(), None);
}

#[test]
fn insert_triggers_leaf_split() {
    let (mut pages, mut alloc, mut tree) = new_tree();

    let count = 500;
    for i in 0..count {
        let key = format!("key-{i:05}");
        let val = format!("val-{i:05}");
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            key.as_bytes(),
            ValueType::Inline,
            val.as_bytes(),
        )
        .unwrap();
    }

    assert_eq!(tree.entry_count, count);
    assert!(
        tree.depth >= 2,
        "tree should have split (depth={})",
        tree.depth
    );

    for i in 0..count {
        let key = format!("key-{i:05}");
        let val = format!("val-{i:05}");
        let result = tree.search(&pages, key.as_bytes()).unwrap();
        assert_eq!(result, Some((ValueType::Inline, val.into_bytes())));
    }
}

#[test]
fn delete_existing_key() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"a",
        ValueType::Inline,
        b"1",
    )
    .unwrap();
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"b",
        ValueType::Inline,
        b"2",
    )
    .unwrap();
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"c",
        ValueType::Inline,
        b"3",
    )
    .unwrap();

    let found = tree.delete(&mut pages, &mut alloc, TxnId(1), b"b").unwrap();
    assert!(found);
    assert_eq!(tree.entry_count, 2);
    assert_eq!(tree.search(&pages, b"b").unwrap(), None);
    assert_eq!(
        tree.search(&pages, b"a").unwrap(),
        Some((ValueType::Inline, b"1".to_vec()))
    );
    assert_eq!(
        tree.search(&pages, b"c").unwrap(),
        Some((ValueType::Inline, b"3".to_vec()))
    );
}

#[test]
fn delete_nonexistent_key() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"a",
        ValueType::Inline,
        b"1",
    )
    .unwrap();
    let found = tree.delete(&mut pages, &mut alloc, TxnId(1), b"z").unwrap();
    assert!(!found);
    assert_eq!(tree.entry_count, 1);
}

#[test]
fn delete_all_from_root_leaf() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"x",
        ValueType::Inline,
        b"1",
    )
    .unwrap();
    tree.delete(&mut pages, &mut alloc, TxnId(1), b"x").unwrap();
    assert_eq!(tree.entry_count, 0);

    let root = pages.get(&tree.root).unwrap();
    assert_eq!(root.page_type(), Some(PageType::Leaf));
    assert_eq!(root.num_cells(), 0);
}

#[test]
fn cow_produces_new_page_ids() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    let root_before = tree.root;

    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(2),
        b"key",
        ValueType::Inline,
        b"val",
    )
    .unwrap();
    let root_after = tree.root;

    assert_ne!(root_before, root_after);
    assert!(alloc.freed_this_txn().contains(&root_before));
}

#[test]
fn insert_and_delete_many() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    let count = 1000u64;

    for i in 0..count {
        let key = format!("k{i:06}");
        let val = format!("v{i:06}");
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            key.as_bytes(),
            ValueType::Inline,
            val.as_bytes(),
        )
        .unwrap();
    }
    assert_eq!(tree.entry_count, count);

    for i in (0..count).step_by(2) {
        let key = format!("k{i:06}");
        let found = tree
            .delete(&mut pages, &mut alloc, TxnId(1), key.as_bytes())
            .unwrap();
        assert!(found);
    }
    assert_eq!(tree.entry_count, count / 2);

    for i in 0..count {
        let key = format!("k{i:06}");
        let result = tree.search(&pages, key.as_bytes()).unwrap();
        if i % 2 == 0 {
            assert_eq!(result, None, "deleted key {key} should not be found");
        } else {
            let val = format!("v{i:06}");
            assert_eq!(result, Some((ValueType::Inline, val.into_bytes())));
        }
    }
}

#[test]
fn deep_tree_insert_delete() {
    let (mut pages, mut alloc, mut tree) = new_tree();

    let count = 2000u64;
    for i in 0..count {
        let key = format!("{i:08}");
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            key.as_bytes(),
            ValueType::Inline,
            b"v",
        )
        .unwrap();
    }
    assert!(tree.depth >= 2, "depth={} expected >= 2", tree.depth);
    assert_eq!(tree.entry_count, count);

    for i in 0..count {
        let key = format!("{i:08}");
        let found = tree
            .delete(&mut pages, &mut alloc, TxnId(1), key.as_bytes())
            .unwrap();
        assert!(found, "key {key} should be deletable");
    }
    assert_eq!(tree.entry_count, 0);
}
