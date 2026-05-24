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

fn insert_keys(
    tree: &mut BTree,
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    keys: &[&[u8]],
) {
    for k in keys {
        tree.insert(pages, alloc, TxnId(1), k, ValueType::Inline, b"v")
            .unwrap();
    }
}

#[test]
fn lil_delete_sequential_keys_hits_cache() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    let keys: Vec<Vec<u8>> = (0..20).map(|i| format!("k{i:02}").into_bytes()).collect();
    let refs: Vec<&[u8]> = keys.iter().map(|v| v.as_slice()).collect();
    insert_keys(&mut tree, &mut pages, &mut alloc, &refs);

    // First delete walks; subsequent deletes should hit the cache.
    let first = tree
        .delete(&mut pages, &mut alloc, TxnId(2), &keys[0])
        .unwrap();
    assert!(first);
    assert!(
        tree.last_delete.is_some(),
        "first slow-path delete primes the cache"
    );

    for k in &keys[1..keys.len() - 1] {
        let lil = tree
            .try_lil_delete(&mut pages, &mut alloc, TxnId(2), k)
            .unwrap();
        let (deleted, head) = lil.expect("sequential same-leaf delete must hit LIL");
        assert!(deleted);
        assert!(head.is_none());
        tree.debug_assert_lil_disjoint();
    }
    assert_eq!(tree.entry_count, 1);
}

#[test]
fn lil_delete_key_outside_cached_range_falls_through() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    // Enough keys to create a multi-leaf tree.
    let keys: Vec<Vec<u8>> = (0..1000).map(|i| format!("k{i:05}").into_bytes()).collect();
    let refs: Vec<&[u8]> = keys.iter().map(|v| v.as_slice()).collect();
    insert_keys(&mut tree, &mut pages, &mut alloc, &refs);
    assert!(tree.depth >= 2);

    // Prime LIL by deleting from one leaf via the slow path.
    tree.delete(&mut pages, &mut alloc, TxnId(2), &keys[0])
        .unwrap();
    let cached_leaf = tree.last_delete.as_ref().unwrap().1;

    // A key from the far end is in a different leaf — try_lil_delete must miss.
    let far_key = &keys[keys.len() - 1];
    let res = tree
        .try_lil_delete(&mut pages, &mut alloc, TxnId(2), far_key)
        .unwrap();
    assert!(
        res.is_none(),
        "out-of-range key must return None (cache miss)"
    );
    // Cache should still point at the original leaf (not cleared by a clean miss).
    assert_eq!(tree.last_delete.as_ref().unwrap().1, cached_leaf);
}

#[test]
fn lil_delete_after_insert_clears_cache() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    let keys: Vec<Vec<u8>> = (0..20).map(|i| format!("k{i:02}").into_bytes()).collect();
    let refs: Vec<&[u8]> = keys.iter().map(|v| v.as_slice()).collect();
    insert_keys(&mut tree, &mut pages, &mut alloc, &refs);

    // Prime LIL with a delete.
    tree.delete(&mut pages, &mut alloc, TxnId(2), &keys[0])
        .unwrap();
    assert!(tree.last_delete.is_some());

    // An insert (rightmost append) clears last_delete.
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(2),
        b"z_after",
        ValueType::Inline,
        b"v",
    )
    .unwrap();
    assert!(
        tree.last_delete.is_none(),
        "rightmost insert must clear last_delete"
    );

    // Next delete still works via slow path.
    let res = tree
        .delete(&mut pages, &mut alloc, TxnId(2), &keys[1])
        .unwrap();
    assert!(res);
}

#[test]
fn lil_delete_to_empty_leaf_clears_cache() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    // Insert just 2 keys — single leaf, becomes empty quickly.
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"a",
        ValueType::Inline,
        b"v",
    )
    .unwrap();
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"b",
        ValueType::Inline,
        b"v",
    )
    .unwrap();

    tree.delete(&mut pages, &mut alloc, TxnId(2), b"a").unwrap();
    let cached = tree.last_delete.as_ref().map(|t| t.1);
    assert!(cached.is_some());

    // Delete the last key in the leaf — leaf becomes empty.
    let lil = tree
        .try_lil_delete(&mut pages, &mut alloc, TxnId(2), b"b")
        .unwrap();
    assert!(matches!(lil, Some((true, None))));
    assert_eq!(tree.entry_count, 0);
    // For a root-level leaf going empty, the leaf stays (path is empty) — cache may
    // still point at the (now-empty) leaf. The validity check at next try_lil_delete
    // (`num_cells == 0`) will catch it.
    if let Some((_, lid)) = tree.last_delete.as_ref() {
        let n = pages.get(lid).unwrap().num_cells();
        assert_eq!(n, 0);
        let miss = tree
            .try_lil_delete(&mut pages, &mut alloc, TxnId(2), b"c")
            .unwrap();
        assert!(miss.is_none(), "n==0 cached leaf must self-invalidate");
        assert!(tree.last_delete.is_none());
    }
}

#[test]
fn lil_delete_overflow_chain_returns_head() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    // Pre-insert two small inline cells, then OVERFLOW-typed cell.
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"k1",
        ValueType::Inline,
        b"v1",
    )
    .unwrap();
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"k2",
        ValueType::Inline,
        b"v2",
    )
    .unwrap();
    // Synthesize an Overflow cell. Its 16-byte value encodes an OverflowRef pointing
    // at a fake first_page; LIL delete must surface that head.
    let mut overflow_value = vec![0u8; 16];
    overflow_value[0..4].copy_from_slice(&7777u32.to_le_bytes());
    overflow_value[4..12].copy_from_slice(&64u64.to_le_bytes());
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"k3",
        ValueType::Overflow,
        &overflow_value,
    )
    .unwrap();

    tree.delete(&mut pages, &mut alloc, TxnId(2), b"k1")
        .unwrap();
    // Now LIL-delete the overflow cell.
    let res = tree
        .try_lil_delete(&mut pages, &mut alloc, TxnId(2), b"k3")
        .unwrap();
    let (deleted, head) = res.expect("LIL must hit (same leaf)");
    assert!(deleted);
    assert_eq!(head, Some(PageId(7777)));
}

#[test]
fn lil_delete_falls_back_on_missing_cached_leaf() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"a",
        ValueType::Inline,
        b"v",
    )
    .unwrap();
    tree.delete(&mut pages, &mut alloc, TxnId(2), b"a").unwrap();
    // Synthetic invalidation: drop the cached leaf from pages.
    let cached = tree.last_delete.as_ref().unwrap().1;
    pages.remove(&cached);
    // Restore tree.root to a valid leaf so search-path tests can still query.
    // For this test we just verify LIL returns None and clears the cache.
    let res = tree
        .try_lil_delete(&mut pages, &mut alloc, TxnId(2), b"a")
        .unwrap();
    assert!(res.is_none());
    assert!(tree.last_delete.is_none());
}
