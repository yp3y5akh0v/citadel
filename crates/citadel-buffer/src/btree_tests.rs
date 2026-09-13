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
fn insert_same_width_replacement_preserves_leaf_layout() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    for i in 0..16u8 {
        let key = format!("k{i:02}");
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            key.as_bytes(),
            ValueType::Inline,
            &[i; 32],
        )
        .unwrap();
    }
    assert_eq!(tree.depth, 1);
    let root = tree.root;
    let page = &pages[&root];
    let cell_area_start = page.cell_area_start();
    let free_space = page.free_space();
    let offsets: Vec<u16> = (0..page.num_cells()).map(|i| page.cell_offset(i)).collect();

    for byte in 0..64u8 {
        assert!(!tree
            .insert(
                &mut pages,
                &mut alloc,
                TxnId(1),
                b"k07",
                ValueType::Inline,
                &[byte; 32],
            )
            .unwrap());
        let page = &pages[&root];
        assert_eq!(tree.root, root);
        assert_eq!(tree.entry_count, 16);
        assert_eq!(page.num_cells(), 16);
        // Delete/reinsert preserves total free space but moves the cell and
        // consumes contiguous space. Check both to detect fragmentation.
        assert_eq!(page.cell_area_start(), cell_area_start);
        assert_eq!(page.free_space(), free_space);
        for (i, &offset) in offsets.iter().enumerate() {
            assert_eq!(page.cell_offset(i as u16), offset);
        }
    }
    for i in 0..16u8 {
        let key = format!("k{i:02}");
        let byte = if i == 7 { 63 } else { i };
        assert_eq!(
            tree.search(&pages, key.as_bytes()).unwrap(),
            Some((ValueType::Inline, vec![byte; 32]))
        );
    }
}

#[test]
fn insert_existing_growth_splits_full_leaf_and_shrink_preserves_keys() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    // 60 cells of 130 bytes plus their pointers nearly fill one leaf.
    for i in 0..60u32 {
        let key = format!("k{i:02}");
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            key.as_bytes(),
            ValueType::Inline,
            &[b'a'; 120],
        )
        .unwrap();
    }
    assert_eq!(tree.depth, 1);
    assert!(!tree
        .insert(
            &mut pages,
            &mut alloc,
            TxnId(2),
            b"k05",
            ValueType::Inline,
            &[b'B'; 700],
        )
        .unwrap());
    assert!(tree.depth > 1);
    assert_eq!(tree.entry_count, 60);

    for replacement in [vec![b'B'; 700], Vec::new(), vec![b'C'; 17]] {
        assert!(!tree
            .insert(
                &mut pages,
                &mut alloc,
                TxnId(2),
                b"k05",
                ValueType::Inline,
                &replacement,
            )
            .unwrap());
        assert_eq!(tree.entry_count, 60);
        for i in 0..60u32 {
            let key = format!("k{i:02}");
            let expected = if i == 5 {
                replacement.clone()
            } else {
                vec![b'a'; 120]
            };
            assert_eq!(
                tree.search(&pages, key.as_bytes()).unwrap(),
                Some((ValueType::Inline, expected))
            );
        }
    }
    assert!(tree
        .insert(
            &mut pages,
            &mut alloc,
            TxnId(2),
            b"k60",
            ValueType::Inline,
            b"new",
        )
        .unwrap());
    assert_eq!(tree.entry_count, 61);
}

#[test]
fn insert_existing_tombstone_preserves_physical_count_and_updates_tag() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    assert!(tree
        .insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            b"key",
            ValueType::Tombstone,
            b"",
        )
        .unwrap());
    for (val_type, value) in [
        (ValueType::Inline, b"".as_slice()),
        (ValueType::Tombstone, b"".as_slice()),
        (ValueType::Inline, b"restored".as_slice()),
        (ValueType::Tombstone, b"".as_slice()),
    ] {
        assert!(!tree
            .insert(&mut pages, &mut alloc, TxnId(1), b"key", val_type, value)
            .unwrap());
        assert_eq!(tree.entry_count, 1);
        assert_eq!(pages[&tree.root].num_cells(), 1);
        assert_eq!(
            tree.search(&pages, b"key").unwrap(),
            Some((val_type, value.to_vec()))
        );
    }
}

#[test]
fn insert_existing_equal_width_type_changes_report_old_overflow_head() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"key",
        ValueType::Inline,
        b"12345678",
    )
    .unwrap();
    let first_ref = leaf_node::OverflowRef {
        first_page: PageId(4242),
        total_len: 5000,
    }
    .to_bytes();
    let second_ref = leaf_node::OverflowRef {
        first_page: PageId(4343),
        total_len: 6000,
    }
    .to_bytes();
    let page = &pages[&tree.root];
    let cell_offset = page.cell_offset(0);
    let cell_area_start = page.cell_area_start();
    for (val_type, value, old_head) in [
        (ValueType::Overflow, first_ref.as_slice(), None),
        (
            ValueType::Overflow,
            second_ref.as_slice(),
            Some(PageId(4242)),
        ),
        (
            ValueType::Inline,
            b"87654321".as_slice(),
            Some(PageId(4343)),
        ),
    ] {
        let (path, leaf_id) = tree.walk_to_leaf(&pages, b"key").unwrap();
        let result = tree
            .insert_at_leaf(
                &mut pages,
                &mut alloc,
                TxnId(1),
                b"key",
                val_type,
                value,
                path,
                leaf_id,
            )
            .unwrap();
        assert_eq!(result, (false, old_head));
        assert_eq!(tree.entry_count, 1);
        assert_eq!(pages[&tree.root].cell_offset(0), cell_offset);
        assert_eq!(pages[&tree.root].cell_area_start(), cell_area_start);
        assert_eq!(
            tree.search(&pages, b"key").unwrap(),
            Some((val_type, value.to_vec()))
        );
    }
}

#[test]
fn insert_existing_cow_preserves_snapshot_and_remaps_append_cache() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    for i in 0..100u32 {
        let key = format!("k{i:03}");
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            key.as_bytes(),
            ValueType::Inline,
            &[b'a'; 120],
        )
        .unwrap();
    }
    assert!(tree.depth > 1);
    assert!(tree.last_insert.is_some());
    let snapshot = tree.clone();
    let (path, leaf_id) = tree.walk_to_leaf(&pages, b"k000").unwrap();
    assert_eq!(
        tree.insert_at_leaf(
            &mut pages,
            &mut alloc,
            TxnId(2),
            b"k000",
            ValueType::Inline,
            &[b'B'; 120],
            path,
            leaf_id,
        )
        .unwrap(),
        (false, None)
    );
    assert_ne!(tree.root, snapshot.root);
    assert!(tree.lil_would_hit(&pages, b"k100"));
    assert_eq!(
        tree.try_lil_insert(
            &mut pages,
            &mut alloc,
            TxnId(2),
            b"k100",
            ValueType::Inline,
            &[b'c'; 120],
        )
        .unwrap(),
        Some(true)
    );
    tree.debug_assert_lil_disjoint();
    assert_eq!(tree.entry_count, 101);
    assert_eq!(snapshot.entry_count, 100);
    for i in 0..100u32 {
        let key = format!("k{i:03}");
        assert_eq!(
            snapshot.search(&pages, key.as_bytes()).unwrap(),
            Some((ValueType::Inline, vec![b'a'; 120]))
        );
        assert_eq!(
            tree.search(&pages, key.as_bytes()).unwrap(),
            Some((
                ValueType::Inline,
                vec![if i == 0 { b'B' } else { b'a' }; 120]
            ))
        );
    }
    assert_eq!(snapshot.search(&pages, b"k100").unwrap(), None);
    assert_eq!(
        tree.search(&pages, b"k100").unwrap(),
        Some((ValueType::Inline, vec![b'c'; 120]))
    );
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

    // A key from the far end is in a different leaf - try_lil_delete must miss.
    let far_key = &keys[keys.len() - 1];
    let res = tree
        .try_lil_delete(&mut pages, &mut alloc, TxnId(2), far_key)
        .unwrap();
    assert!(
        res.is_none(),
        "out-of-range key must return None (cache miss)"
    );
    // Cache should still point at the original leaf (not cleared by a clean
    // miss).
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
    // Insert just 2 keys - single leaf, becomes empty quickly.
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

    // Delete the last key in the leaf - leaf becomes empty.
    let lil = tree
        .try_lil_delete(&mut pages, &mut alloc, TxnId(2), b"b")
        .unwrap();
    assert!(matches!(lil, Some((true, None))));
    assert_eq!(tree.entry_count, 0);
    // For a root-level leaf going empty, the leaf stays (path is empty) - cache
    // may still point at the (now-empty) leaf. The validity check at next
    // try_lil_delete (`num_cells == 0`) will catch it.
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
    // Pre-insert two small inline cells, then an overflow-typed cell.
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
    // Synthesize an Overflow cell. Its 16-byte value encodes an OverflowRef
    // pointing at a fake first_page; LIL delete must surface that head.
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
fn update_sorted_split_fallback_preserves_grown_row() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    // Pack a single leaf nearly full: 60 cells of 130 bytes (+2B pointers).
    for i in 0..60u32 {
        let key = format!("k{i:02}");
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            key.as_bytes(),
            ValueType::Inline,
            &[b'a'; 120],
        )
        .unwrap();
    }
    assert_eq!(tree.depth, 1, "test expects a single packed leaf");

    // Growing k05 to 700 bytes cannot fit even after compaction, forcing the
    // split fallback; k50 in the same batch must still be found afterwards.
    let grown = vec![b'B'; 700];
    let small = vec![b'c'; 120];
    let mut replaced = Vec::new();
    let mut skipped = Vec::new();
    let count = tree
        .update_sorted(
            &mut pages,
            &mut alloc,
            TxnId(1),
            &[
                (b"k05".as_slice(), ValueType::Inline, grown.as_slice()),
                (b"k50".as_slice(), ValueType::Inline, small.as_slice()),
            ],
            &mut replaced,
            &mut skipped,
        )
        .unwrap();
    assert_eq!(count, 2);
    assert!(replaced.is_empty());
    assert!(skipped.is_empty());
    assert_eq!(tree.entry_count, 60);
    assert_eq!(
        tree.search(&pages, b"k05").unwrap(),
        Some((ValueType::Inline, grown))
    );
    assert_eq!(
        tree.search(&pages, b"k50").unwrap(),
        Some((ValueType::Inline, small))
    );
    for i in 0..60u32 {
        let key = format!("k{i:02}");
        assert!(
            tree.search(&pages, key.as_bytes()).unwrap().is_some(),
            "key {key} lost"
        );
    }
}

#[test]
fn update_sorted_reports_replaced_overflow_head() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    // Synthesize an overflow cell: 8-byte ref pointing at page 4242.
    let mut oref = [0u8; 8];
    oref[0..4].copy_from_slice(&4242u32.to_le_bytes());
    oref[4..8].copy_from_slice(&5000u32.to_le_bytes());
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"k",
        ValueType::Overflow,
        &oref,
    )
    .unwrap();

    let mut replaced = Vec::new();
    let mut skipped = Vec::new();
    let count = tree
        .update_sorted(
            &mut pages,
            &mut alloc,
            TxnId(1),
            &[(b"k".as_slice(), ValueType::Inline, b"small".as_slice())],
            &mut replaced,
            &mut skipped,
        )
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(replaced, vec![PageId(4242)]);
    assert!(skipped.is_empty());
    assert_eq!(
        tree.search(&pages, b"k").unwrap(),
        Some((ValueType::Inline, b"small".to_vec()))
    );
}

#[test]
fn update_sorted_check_stops_before_the_next_pair() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    for key in [b"k1", b"k2", b"k3"] {
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            key,
            ValueType::Inline,
            b"old",
        )
        .unwrap();
    }

    let mut checks = 0;
    let mut replaced = Vec::new();
    let mut skipped = Vec::new();
    let err = tree
        .update_sorted_with(
            &mut pages,
            &mut alloc,
            TxnId(2),
            &[
                (b"k1".as_slice(), ValueType::Inline, b"new".as_slice()),
                (b"k2".as_slice(), ValueType::Inline, b"new".as_slice()),
                (b"k3".as_slice(), ValueType::Inline, b"new".as_slice()),
            ],
            &mut replaced,
            &mut skipped,
            || {
                checks += 1;
                if checks == 3 {
                    Err(Error::Interrupted)
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();

    assert!(matches!(err, Error::Interrupted));
    assert_eq!(checks, 3);
    assert_eq!(
        tree.search(&pages, b"k1").unwrap(),
        Some((ValueType::Inline, b"new".to_vec()))
    );
    assert_eq!(
        tree.search(&pages, b"k2").unwrap(),
        Some((ValueType::Inline, b"new".to_vec()))
    );
    assert_eq!(
        tree.search(&pages, b"k3").unwrap(),
        Some((ValueType::Inline, b"old".to_vec()))
    );
}

#[test]
fn deep_tree_ascending_delete_with_wide_keys() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    // 2000-byte keys: leaf fanout ~4, branch fanout ~5, so 40 keys give a
    // depth-3 tree. Ascending deletion collapses non-root branches, which
    // previously drove the global depth below the true height (u16 underflow
    // on the final root collapse).
    let count = 40u32;
    let make_key = |i: u32| {
        let mut k = format!("{i:04}").into_bytes();
        k.resize(2000, b'k');
        k
    };
    for i in 0..count {
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            &make_key(i),
            ValueType::Inline,
            b"v",
        )
        .unwrap();
    }
    assert!(tree.depth >= 3, "test needs depth >= 3, got {}", tree.depth);

    for i in 0..count {
        let found = tree
            .delete(&mut pages, &mut alloc, TxnId(1), &make_key(i))
            .unwrap();
        assert!(found, "key {i} should be deletable");
    }
    assert_eq!(tree.entry_count, 0);
    assert!(tree.depth >= 1);

    // Tree remains usable after full drain.
    tree.insert(
        &mut pages,
        &mut alloc,
        TxnId(1),
        b"after",
        ValueType::Inline,
        b"v",
    )
    .unwrap();
    assert_eq!(
        tree.search(&pages, b"after").unwrap(),
        Some((ValueType::Inline, b"v".to_vec()))
    );
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

mod rightmost_append_split {
    use super::*;
    use crate::cursor::Cursor;
    use std::collections::BTreeMap;

    type Expected = BTreeMap<Vec<u8>, (ValueType, Vec<u8>)>;

    #[derive(Clone, Copy, Debug)]
    enum Route {
        Insert,
        IfAbsent,
        OrFetch,
        Lil,
        AtLeaf,
        IfAbsentAtLeaf,
    }

    fn key(id: u32) -> Vec<u8> {
        id.to_be_bytes().to_vec()
    }

    fn wide_key(id: u32) -> Vec<u8> {
        let mut bytes = key(id);
        bytes.resize(2_000, b'k');
        bytes
    }

    fn payload(id: u32, size: usize) -> Vec<u8> {
        let mut bytes = vec![id as u8; size];
        if size >= 4 {
            bytes[..4].copy_from_slice(&id.to_le_bytes());
        }
        bytes
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_new(
        tree: &mut BTree,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn: TxnId,
        key: &[u8],
        value: &[u8],
        route: Route,
        cold: bool,
    ) -> bool {
        if cold {
            tree.clear_lil_caches();
        }
        match route {
            Route::Insert => assert!(tree
                .insert(pages, alloc, txn, key, ValueType::Inline, value)
                .unwrap()),
            Route::IfAbsent => assert!(tree
                .insert_if_absent(pages, alloc, txn, key, ValueType::Inline, value)
                .unwrap()),
            Route::OrFetch => assert!(tree
                .insert_or_fetch(pages, alloc, txn, key, ValueType::Inline, value)
                .unwrap()
                .is_none()),
            Route::Lil => {
                if let Some(inserted) = tree
                    .try_lil_insert(pages, alloc, txn, key, ValueType::Inline, value)
                    .unwrap()
                {
                    assert!(inserted);
                    tree.debug_assert_lil_disjoint();
                    return true;
                }
                assert!(tree
                    .insert(pages, alloc, txn, key, ValueType::Inline, value)
                    .unwrap());
            }
            Route::AtLeaf => {
                let (path, leaf) = tree.walk_to_leaf(pages, key).unwrap();
                assert_eq!(
                    tree.insert_at_leaf(
                        pages,
                        alloc,
                        txn,
                        key,
                        ValueType::Inline,
                        value,
                        path,
                        leaf,
                    )
                    .unwrap(),
                    (true, None)
                );
            }
            Route::IfAbsentAtLeaf => {
                let (path, leaf) = tree.walk_to_leaf(pages, key).unwrap();
                assert!(tree
                    .insert_if_absent_at_leaf(
                        pages,
                        alloc,
                        txn,
                        key,
                        ValueType::Inline,
                        value,
                        path,
                        leaf,
                    )
                    .unwrap());
            }
        }
        tree.debug_assert_lil_disjoint();
        false
    }

    fn assert_contents(tree: &BTree, pages: &FxHashMap<PageId, Page>, expected: &Expected) {
        assert_eq!(tree.entry_count, expected.len() as u64);
        let mut cursor = Cursor::first(pages, tree.root).unwrap();
        let mut actual = Vec::new();
        while let Some(entry) = cursor.current(pages) {
            actual.push((entry.key, (entry.val_type, entry.value)));
            cursor.next(pages).unwrap();
        }
        assert_eq!(
            actual,
            expected
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>()
        );
        for (key, value) in expected {
            assert_eq!(tree.search(pages, key).unwrap(), Some(value.clone()));
        }
    }

    fn leaf_ids(tree: &BTree, pages: &FxHashMap<PageId, Page>) -> Vec<PageId> {
        let mut cursor = Cursor::first(pages, tree.root).unwrap();
        let mut ids = Vec::new();
        while cursor.is_valid() {
            let id = cursor.leaf_page_id();
            if ids.last() != Some(&id) {
                ids.push(id);
            }
            cursor.next(pages).unwrap();
        }
        ids
    }

    fn dense_sequential(route: Route, cold: bool) {
        let (mut pages, mut alloc, mut tree) = new_tree();
        let mut expected = Expected::new();
        let mut lil_hits = 0;
        for id in 0..84 {
            let key = key(id);
            let value = payload(id, 1_024);
            lil_hits += usize::from(insert_new(
                &mut tree,
                &mut pages,
                &mut alloc,
                TxnId(1),
                &key,
                &value,
                route,
                cold,
            ));
            expected.insert(key, (ValueType::Inline, value));
        }
        assert_contents(&tree, &pages, &expected);
        if matches!(route, Route::Lil) {
            assert!(lil_hits > 0, "explicit LIL route was never exercised");
        }
        let counts: Vec<_> = leaf_ids(&tree, &pages)
            .into_iter()
            .map(|id| pages[&id].num_cells() as usize)
            .collect();
        // Seven 1 KiB cells fit, eight do not. Completed append leaves should
        // retain that capacity instead of being left half full after a split.
        let capacity = citadel_core::USABLE_SIZE / (leaf_node::cell_size(4, 1_024) + 2);
        assert_eq!(capacity, 7);
        assert_eq!(
            counts,
            vec![capacity; 84 / capacity],
            "{route:?}, cold={cold}"
        );
    }

    #[test]
    fn dense_insert_cached() {
        dense_sequential(Route::Insert, false);
    }

    #[test]
    fn dense_insert_uncached() {
        dense_sequential(Route::Insert, true);
    }

    #[test]
    fn dense_insert_if_absent_cached() {
        dense_sequential(Route::IfAbsent, false);
    }

    #[test]
    fn dense_insert_if_absent_uncached() {
        dense_sequential(Route::IfAbsent, true);
    }

    #[test]
    fn dense_insert_or_fetch_cached() {
        dense_sequential(Route::OrFetch, false);
    }

    #[test]
    fn dense_insert_or_fetch_uncached() {
        dense_sequential(Route::OrFetch, true);
    }

    #[test]
    fn dense_explicit_lil_insert() {
        dense_sequential(Route::Lil, false);
    }

    #[test]
    fn dense_insert_at_leaf() {
        dense_sequential(Route::AtLeaf, true);
    }

    #[test]
    fn dense_insert_if_absent_at_leaf() {
        dense_sequential(Route::IfAbsentAtLeaf, true);
    }

    fn assert_append_cache(tree: &BTree, pages: &FxHashMap<PageId, Page>, next: &[u8]) {
        let expected = tree.walk_to_leaf(pages, next).unwrap();
        assert_eq!(tree.last_insert.as_ref(), Some(&expected));
        assert!(tree.lil_would_hit(pages, next));
        assert!(tree.last_delete.is_none());
    }

    #[test]
    fn splits_retain_exact_append_path_across_routes_and_cow() {
        for route in [
            Route::Insert,
            Route::IfAbsent,
            Route::OrFetch,
            Route::Lil,
            Route::AtLeaf,
            Route::IfAbsentAtLeaf,
        ] {
            for cold in [false, true] {
                let (mut pages, mut alloc, mut tree) = new_tree();
                let mut expected = Expected::new();
                for id in 0..32 {
                    let key = wide_key(id);
                    let value = payload(id, 1_024);
                    insert_new(
                        &mut tree,
                        &mut pages,
                        &mut alloc,
                        TxnId(1),
                        &key,
                        &value,
                        route,
                        cold,
                    );
                    expected.insert(key, (ValueType::Inline, value));
                    assert_append_cache(&tree, &pages, &wide_key(id + 1));
                }
                let snapshot = tree.clone();
                let original = expected.clone();
                for id in 32..96 {
                    let key = wide_key(id);
                    let value = payload(id, 1_024);
                    insert_new(
                        &mut tree,
                        &mut pages,
                        &mut alloc,
                        TxnId(2),
                        &key,
                        &value,
                        route,
                        cold,
                    );
                    expected.insert(key, (ValueType::Inline, value));
                    assert_append_cache(&tree, &pages, &wide_key(id + 1));
                }
                assert!(tree.depth >= 4, "fixture must split branches and roots");
                assert_contents(&snapshot, &pages, &original);
                assert_contents(&tree, &pages, &expected);

                // The retained path must also survive a new CoW epoch when it
                // is consumed directly by the public fast route.
                let next = wide_key(96);
                assert_eq!(
                    tree.try_lil_insert(
                        &mut pages,
                        &mut alloc,
                        TxnId(3),
                        &next,
                        ValueType::Inline,
                        b"after split",
                    )
                    .unwrap(),
                    Some(true)
                );
                expected.insert(next, (ValueType::Inline, b"after split".to_vec()));
                assert_append_cache(&tree, &pages, &wide_key(97));
                assert_contents(&tree, &pages, &expected);
                assert_contents(&snapshot, &pages, &original);

                // Deletes and external CoW rerooting still invalidate the
                // append cache, including paths retained from a split.
                assert!(tree
                    .delete(&mut pages, &mut alloc, TxnId(3), &wide_key(96))
                    .unwrap());
                assert!(tree.last_insert.is_none());
                expected.remove(&wide_key(96));
                insert_new(
                    &mut tree,
                    &mut pages,
                    &mut alloc,
                    TxnId(3),
                    &wide_key(97),
                    b"reprimed",
                    route,
                    false,
                );
                expected.insert(wide_key(97), (ValueType::Inline, b"reprimed".to_vec()));
                assert_append_cache(&tree, &pages, &wide_key(98));
                tree.reroot_after_external_cow(tree.root);
                assert!(tree.last_insert.is_none());
                assert!(tree.last_delete.is_none());
                assert_contents(&tree, &pages, &expected);
            }
        }
    }

    #[test]
    fn deep_append_and_deletion_preserve_cow_snapshot() {
        for route in [Route::Insert, Route::IfAbsent, Route::OrFetch] {
            let (mut pages, mut alloc, mut tree) = new_tree();
            let mut expected = Expected::new();
            for id in 0..24 {
                let key = wide_key(id);
                let value = payload(id, 1_024);
                insert_new(
                    &mut tree,
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &key,
                    &value,
                    route,
                    false,
                );
                expected.insert(key, (ValueType::Inline, value));
            }
            let snapshot = tree.clone();
            let original = expected.clone();
            for id in 24..96 {
                let key = wide_key(id);
                let value = payload(id, 1_024);
                insert_new(
                    &mut tree,
                    &mut pages,
                    &mut alloc,
                    TxnId(2),
                    &key,
                    &value,
                    route,
                    false,
                );
                expected.insert(key, (ValueType::Inline, value));
            }
            assert!(
                tree.depth >= 3,
                "wide keys must exercise branch split propagation"
            );
            assert_ne!(tree.root, snapshot.root);
            assert_contents(&tree, &pages, &expected);
            assert_contents(&snapshot, &pages, &original);
            assert_eq!(snapshot.search(&pages, &wide_key(24)).unwrap(), None);

            // Drain an interleaved order, including the new sparse right edge.
            for step in 0..96 {
                let id = (step * 37) % 96;
                let key = wide_key(id);
                assert!(tree.delete(&mut pages, &mut alloc, TxnId(3), &key).unwrap());
                expected.remove(&key);
                if step % 24 == 23 {
                    assert_contents(&tree, &pages, &expected);
                }
            }
            assert_eq!(pages[&tree.root].page_type(), Some(PageType::Leaf));
            // Interior branch splicing can leave depth as a conservative
            // walk-capacity bound; the observable root must still be a leaf.
            assert!(tree.depth >= 1);
            assert_contents(&snapshot, &pages, &original);
            insert_new(
                &mut tree,
                &mut pages,
                &mut alloc,
                TxnId(3),
                &wide_key(100),
                b"reused",
                route,
                false,
            );
            expected.insert(wide_key(100), (ValueType::Inline, b"reused".to_vec()));
            assert_contents(&tree, &pages, &expected);
        }
    }

    #[test]
    fn growing_last_key_is_replacement_after_failed_leaf_write() {
        for route in 0..3 {
            let (mut pages, mut alloc, mut tree) = new_tree();
            let mut expected = Expected::new();
            for id in 0..7 {
                let key = key(id);
                let value = payload(id, 1_024);
                insert_new(
                    &mut tree,
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &key,
                    &value,
                    Route::Insert,
                    false,
                );
                expected.insert(key, (ValueType::Inline, value));
            }
            assert_eq!(tree.depth, 1);
            let snapshot = tree.clone();
            let original = expected.clone();
            let last_key = key(6);
            let grown = payload(106, citadel_core::MAX_INLINE_VALUE_SIZE);
            if route == 1 {
                let mut replaced = Vec::new();
                let mut skipped = Vec::new();
                assert_eq!(
                    tree.update_sorted(
                        &mut pages,
                        &mut alloc,
                        TxnId(2),
                        &[(&last_key, ValueType::Inline, &grown)],
                        &mut replaced,
                        &mut skipped,
                    )
                    .unwrap(),
                    1
                );
                assert!(replaced.is_empty());
                assert!(skipped.is_empty());
            } else if route == 2 {
                let (path, leaf) = tree.walk_to_leaf(&pages, &last_key).unwrap();
                let hint = BTree::search_at_leaf_ref_with_hint(&pages, leaf, &last_key)
                    .unwrap()
                    .unwrap()
                    .0;
                assert_eq!(
                    tree.insert_at_leaf_with_hint(
                        &mut pages,
                        &mut alloc,
                        TxnId(2),
                        &last_key,
                        ValueType::Inline,
                        &grown,
                        path,
                        leaf,
                        hint,
                    )
                    .unwrap(),
                    (false, None),
                );
            } else {
                assert!(!tree
                    .insert(
                        &mut pages,
                        &mut alloc,
                        TxnId(2),
                        &last_key,
                        ValueType::Inline,
                        &grown
                    )
                    .unwrap());
            }
            assert!(tree.last_insert.is_none());
            expected.insert(last_key.clone(), (ValueType::Inline, grown));
            assert_contents(&tree, &pages, &expected);
            assert_contents(&snapshot, &pages, &original);
            let leaves = leaf_ids(&tree, &pages);
            assert_eq!(
                leaves.len(),
                2,
                "growing the maximum must split this full leaf"
            );
            assert!(
                leaves.iter().all(|id| pages[id].num_cells() >= 2),
                "replacement must retain the balanced fallback, not a new singleton append leaf"
            );
            assert!(!tree
                .insert(
                    &mut pages,
                    &mut alloc,
                    TxnId(2),
                    &last_key,
                    ValueType::Inline,
                    b""
                )
                .unwrap());
            expected.insert(last_key, (ValueType::Inline, Vec::new()));
            insert_new(
                &mut tree,
                &mut pages,
                &mut alloc,
                TxnId(2),
                &key(7),
                b"next",
                Route::Insert,
                false,
            );
            expected.insert(key(7), (ValueType::Inline, b"next".to_vec()));
            assert_contents(&tree, &pages, &expected);
        }
    }

    #[test]
    fn maximum_tombstone_revival_keeps_one_key_and_balanced_split() {
        for route in [Route::IfAbsent, Route::OrFetch] {
            let (mut pages, mut alloc, mut tree) = new_tree();
            let mut expected = Expected::new();
            for id in 0..6 {
                let key = key(id);
                let value = payload(id, 1_024);
                insert_new(
                    &mut tree,
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &key,
                    &value,
                    Route::Insert,
                    false,
                );
                expected.insert(key, (ValueType::Inline, value));
            }
            let last_key = key(6);
            assert!(tree
                .insert(
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &last_key,
                    ValueType::Tombstone,
                    b"",
                )
                .unwrap());
            assert_eq!(tree.depth, 1);
            let grown = payload(106, citadel_core::MAX_INLINE_VALUE_SIZE);
            // Six existing 1 KiB cells plus this replacement no longer fit.
            // The failed leaf insertion removes the tombstone first, so a
            // comparison against the remaining max would misclassify it.
            match route {
                Route::IfAbsent => assert!(tree
                    .insert_if_absent(
                        &mut pages,
                        &mut alloc,
                        TxnId(2),
                        &last_key,
                        ValueType::Inline,
                        &grown,
                    )
                    .unwrap()),
                Route::OrFetch => assert!(tree
                    .insert_or_fetch(
                        &mut pages,
                        &mut alloc,
                        TxnId(2),
                        &last_key,
                        ValueType::Inline,
                        &grown,
                    )
                    .unwrap()
                    .is_none()),
                _ => unreachable!(),
            }
            assert!(tree.last_insert.is_none());
            expected.insert(last_key, (ValueType::Inline, grown));
            // The existing APIs count tombstone revival as an insertion;
            // verify physical rows here to isolate split classification.
            let mut cursor = Cursor::first(&pages, tree.root).unwrap();
            let mut actual = Vec::new();
            while let Some(entry) = cursor.current(&pages) {
                actual.push((entry.key, (entry.val_type, entry.value)));
                cursor.next(&pages).unwrap();
            }
            assert_eq!(
                actual,
                expected
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<Vec<_>>()
            );
            for (key, value) in &expected {
                assert_eq!(tree.search(&pages, key).unwrap(), Some(value.clone()));
            }
            let leaves = leaf_ids(&tree, &pages);
            assert_eq!(leaves.len(), 2);
            assert!(
                leaves.iter().all(|id| pages[id].num_cells() >= 2),
                "reviving the physical maximum is not a new rightmost append"
            );
        }
    }

    #[test]
    fn non_rightmost_leaf_maximum_gap_uses_balanced_fallback() {
        for route in [Route::Insert, Route::IfAbsent, Route::OrFetch] {
            let (mut pages, mut alloc, mut tree) = new_tree();
            let mut expected = Expected::new();
            for id in (0..7).map(|id| id * 100).chain([1_000]) {
                let key = key(id);
                let value = payload(id, 1_024);
                insert_new(
                    &mut tree,
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &key,
                    &value,
                    route,
                    false,
                );
                expected.insert(key, (ValueType::Inline, value));
            }
            let leaves = leaf_ids(&tree, &pages);
            assert_eq!(leaves.len(), 2);
            let left = leaves[0];
            let upper = u32::from_be_bytes(
                leaf_node::read_cell(&pages[&leaves[1]], 0)
                    .key
                    .try_into()
                    .unwrap(),
            );
            // Adapt to either the old half split or dense append layout, then
            // fill the first leaf using keys below its neighbor's lower bound.
            while pages[&left].num_cells() < 7 {
                let page = &pages[&left];
                let id = u32::from_be_bytes(
                    leaf_node::read_cell(page, page.num_cells() - 1)
                        .key
                        .try_into()
                        .unwrap(),
                ) + 1;
                assert!(id < upper);
                let value = payload(id, 1_024);
                insert_new(
                    &mut tree,
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &key(id),
                    &value,
                    route,
                    true,
                );
                expected.insert(key(id), (ValueType::Inline, value));
            }
            let page = &pages[&left];
            let id = u32::from_be_bytes(
                leaf_node::read_cell(page, page.num_cells() - 1)
                    .key
                    .try_into()
                    .unwrap(),
            ) + 1;
            assert!(id < upper);
            let (path, _) = tree.walk_to_leaf(&pages, &key(id)).unwrap();
            assert!(path
                .iter()
                .any(|(id, child)| *child < pages[id].num_cells() as usize));
            let value = payload(id, 1_024);
            insert_new(
                &mut tree,
                &mut pages,
                &mut alloc,
                TxnId(1),
                &key(id),
                &value,
                route,
                true,
            );
            expected.insert(key(id), (ValueType::Inline, value));
            assert_contents(&tree, &pages, &expected);
            assert!(tree.last_insert.is_none());
            let leaves = leaf_ids(&tree, &pages);
            assert_eq!(leaves.len(), 3);
            assert!(
                leaves[..2].iter().all(|id| pages[id].num_cells() >= 2),
                "an interior gap must not create an append-only singleton leaf"
            );
        }
    }

    #[test]
    fn random_backfill_duplicates_and_delete_collapse_preserve_rows() {
        let (mut pages, mut alloc, mut tree) = new_tree();
        let mut expected = Expected::new();
        for id in (0..128)
            .step_by(2)
            .chain((0..64).map(|i| ((i * 37) % 64) * 2 + 1))
        {
            let key = key(id);
            let value = payload(id, 1_024);
            insert_new(
                &mut tree,
                &mut pages,
                &mut alloc,
                TxnId(1),
                &key,
                &value,
                Route::IfAbsent,
                false,
            );
            expected.insert(key, (ValueType::Inline, value));
        }
        assert_contents(&tree, &pages, &expected);
        for id in [0, 1, 63, 126, 127] {
            assert!(!tree
                .insert_if_absent(
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &key(id),
                    ValueType::Inline,
                    b"wrong"
                )
                .unwrap());
            assert_eq!(
                tree.insert_or_fetch(
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &key(id),
                    ValueType::Inline,
                    b"wrong"
                )
                .unwrap(),
                expected.get(&key(id)).cloned()
            );
        }
        assert_contents(&tree, &pages, &expected);
        for step in 0..128 {
            let key = key((step * 53) % 128);
            assert!(tree.delete(&mut pages, &mut alloc, TxnId(2), &key).unwrap());
            expected.remove(&key);
            if step % 16 == 15 {
                assert_contents(&tree, &pages, &expected);
            }
        }
        assert_eq!(tree.depth, 1);
        assert_eq!(pages[&tree.root].page_type(), Some(PageType::Leaf));
    }

    #[test]
    fn large_keys_and_staged_overflow_append_preserve_tags_and_snapshot() {
        let (mut pages, mut alloc, mut tree) = new_tree();
        let mut expected = Expected::new();
        for id in 0..2 {
            let key = wide_key(id);
            let value = payload(id, citadel_core::MAX_INLINE_VALUE_SIZE);
            insert_new(
                &mut tree,
                &mut pages,
                &mut alloc,
                TxnId(1),
                &key,
                &value,
                Route::Insert,
                false,
            );
            expected.insert(key, (ValueType::Inline, value));
        }
        assert_eq!(tree.depth, 1);
        let snapshot = tree.clone();
        let original = expected.clone();
        let overflow = leaf_node::OverflowRef {
            first_page: PageId(4_242),
            total_len: 8_000,
        }
        .to_bytes();
        assert!(tree
            .insert_if_absent(
                &mut pages,
                &mut alloc,
                TxnId(2),
                &wide_key(2),
                ValueType::Overflow,
                &overflow
            )
            .unwrap());
        expected.insert(wide_key(2), (ValueType::Overflow, overflow.to_vec()));
        assert!(tree.depth >= 2);
        assert_contents(&tree, &pages, &expected);
        assert_contents(&snapshot, &pages, &original);
        assert_eq!(
            tree.insert_or_fetch(
                &mut pages,
                &mut alloc,
                TxnId(2),
                &wide_key(2),
                ValueType::Inline,
                b"wrong"
            )
            .unwrap(),
            Some((ValueType::Overflow, overflow.to_vec()))
        );
        assert_eq!(tree.entry_count, 3);
    }
}

#[test]
fn indexed_delete_preserves_cow_snapshots_and_sibling_paths() {
    let (mut pages, mut alloc, mut tree) = new_tree();
    let keys: Vec<_> = (0..128u32).map(u32::to_be_bytes).collect();
    for (i, key) in keys.iter().enumerate() {
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            key,
            ValueType::Inline,
            &[i as u8; 128],
        )
        .unwrap();
    }
    assert!(tree.depth > 1);
    let original = tree.clone();
    let mut removed = vec![keys[24]];
    assert!(tree
        .delete(&mut pages, &mut alloc, TxnId(2), &removed[0])
        .unwrap());
    let checkpoint = tree.clone();
    let checkpoint_leaf = tree.last_delete.as_ref().unwrap().1;

    // Resolve middle/front/end positions after each prior deletion. The first
    // cached deletion also clones the leaf, retaining its resolved cell index.
    for position in 0..3 {
        let leaf_id = tree.last_delete.as_ref().unwrap().1;
        let page = &pages[&leaf_id];
        let n = page.num_cells();
        assert!(n > 3);
        let index = match position {
            0 => n / 2,
            1 => 0,
            _ => n - 1,
        };
        let key: [u8; 4] = leaf_node::read_cell(page, index).key.try_into().unwrap();
        assert_eq!(
            tree.try_lil_delete(&mut pages, &mut alloc, TxnId(3), &key)
                .unwrap(),
            Some((true, None))
        );
        assert_ne!(tree.last_delete.as_ref().unwrap().1, checkpoint_leaf);
        removed.push(key);
    }

    let cached_leaf = tree.last_delete.as_ref().unwrap().1;
    let sibling_key = *keys
        .iter()
        .find(|key| {
            !removed.contains(key)
                && tree.walk_to_leaf(&pages, key.as_slice()).unwrap().1 != cached_leaf
        })
        .unwrap();
    assert_eq!(
        tree.try_lil_delete(&mut pages, &mut alloc, TxnId(3), &sibling_key)
            .unwrap(),
        None
    );
    assert!(tree
        .delete(&mut pages, &mut alloc, TxnId(3), &sibling_key)
        .unwrap());
    removed.push(sibling_key);

    assert_eq!(original.entry_count, keys.len() as u64);
    assert_eq!(checkpoint.entry_count, keys.len() as u64 - 1);
    assert_eq!(tree.entry_count, (keys.len() - removed.len()) as u64);
    for (i, key) in keys.iter().enumerate() {
        let value = Some((ValueType::Inline, vec![i as u8; 128]));
        assert_eq!(original.search(&pages, key).unwrap(), value);
        assert_eq!(
            checkpoint.search(&pages, key).unwrap(),
            if key == &removed[0] {
                None
            } else {
                value.clone()
            }
        );
        assert_eq!(
            tree.search(&pages, key).unwrap(),
            if removed.contains(key) { None } else { value }
        );
    }
}

mod checked_leaf_hint {
    use super::*;

    fn hint_for(tree: &BTree, pages: &FxHashMap<PageId, Page>, key: &[u8]) -> LeafEntryHint {
        let (_, leaf) = tree.walk_to_leaf(pages, key).unwrap();
        BTree::search_at_leaf_ref_with_hint(pages, leaf, key)
            .unwrap()
            .unwrap()
            .0
    }

    #[test]
    fn same_width_matches_preserve_layout_and_lookup_adapters() {
        let (mut pages, mut alloc, mut tree) = new_tree();
        for key in 0..32u8 {
            tree.insert(
                &mut pages,
                &mut alloc,
                TxnId(1),
                &[key],
                ValueType::Inline,
                &[key; 8],
            )
            .unwrap();
        }
        let root = tree.root;
        let start = pages[&root].cell_area_start();
        let free = pages[&root].free_space();
        let offsets: Vec<_> = (0..32).map(|i| pages[&root].cell_offset(i)).collect();
        for key in [0u8, 16, 31] {
            let old = BTree::search_at_leaf(&pages, root, &[key]).unwrap();
            let (hint, kind, borrowed) = BTree::search_at_leaf_ref_with_hint(&pages, root, &[key])
                .unwrap()
                .unwrap();
            assert_eq!(old, Some((kind, borrowed.to_vec())));
            assert_eq!(
                BTree::search_at_leaf_ref(&pages, root, &[key]).unwrap(),
                Some((kind, borrowed))
            );
            assert_eq!(
                tree.insert_at_leaf_with_hint(
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &[key],
                    ValueType::Inline,
                    &[key + 1; 8],
                    Vec::new(),
                    root,
                    hint,
                )
                .unwrap(),
                (false, None)
            );
        }
        assert_eq!(tree.root, root);
        assert_eq!(tree.entry_count, 32);
        assert_eq!(pages[&root].cell_area_start(), start);
        assert_eq!(pages[&root].free_space(), free);
        assert_eq!(
            (0..32)
                .map(|i| pages[&root].cell_offset(i))
                .collect::<Vec<_>>(),
            offsets
        );
        for key in 0..32u8 {
            let expected = if [0, 16, 31].contains(&key) {
                key + 1
            } else {
                key
            };
            assert_eq!(
                tree.search(&pages, &[key]).unwrap(),
                Some((ValueType::Inline, vec![expected; 8]))
            );
        }
        assert!(BTree::search_at_leaf_ref_with_hint(&pages, root, &[32])
            .unwrap()
            .is_none());
        assert!(matches!(
            BTree::search_at_leaf_ref_with_hint(&pages, PageId(999), b"x"),
            Err(Error::PageOutOfBounds(PageId(999)))
        ));
    }

    #[test]
    fn shifted_out_of_range_foreign_and_wrong_key_hints_fall_back() {
        for case in 0..5 {
            let (mut pages, mut alloc, mut tree) = new_tree();
            for key in *b"bdf" {
                tree.insert(
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &[key],
                    ValueType::Inline,
                    &[key],
                )
                .unwrap();
            }
            let key = if case == 2 || case == 4 { b'f' } else { b'd' };
            let hint = match case {
                2 => hint_for(&tree, &pages, b"f"),
                3 => {
                    let mut foreign = BTree::new(&mut pages, &mut alloc, TxnId(1));
                    foreign
                        .insert(
                            &mut pages,
                            &mut alloc,
                            TxnId(1),
                            b"d",
                            ValueType::Inline,
                            b"foreign",
                        )
                        .unwrap();
                    let hint = hint_for(&foreign, &pages, b"d");
                    assert_ne!(foreign.root, tree.root);
                    hint
                }
                _ => hint_for(&tree, &pages, b"d"),
            };
            match case {
                0 => {
                    assert!(tree
                        .insert(
                            &mut pages,
                            &mut alloc,
                            TxnId(1),
                            b"a",
                            ValueType::Inline,
                            b"a"
                        )
                        .unwrap());
                }
                1 => {
                    assert!(tree.delete(&mut pages, &mut alloc, TxnId(1), b"b").unwrap());
                }
                2 => {
                    assert!(tree.delete(&mut pages, &mut alloc, TxnId(1), b"f").unwrap());
                }
                _ => {}
            }
            let before = tree.entry_count;
            let (path, leaf) = tree.walk_to_leaf(&pages, &[key]).unwrap();
            assert_eq!(
                tree.insert_at_leaf_with_hint(
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &[key],
                    ValueType::Inline,
                    b"updated",
                    path,
                    leaf,
                    hint,
                )
                .unwrap(),
                (case == 2, None)
            );
            assert_eq!(tree.entry_count, before + u64::from(case == 2));
            for original in *b"abdf" {
                let expected = if original == key {
                    Some(b"updated".to_vec())
                } else if (original == b'a' && case != 0) || (original == b'b' && case == 1) {
                    None
                } else {
                    Some(vec![original])
                };
                assert_eq!(
                    tree.search(&pages, &[original]).unwrap().map(|(_, v)| v),
                    expected,
                    "case {case}, key {original}"
                );
            }
        }
    }

    #[test]
    fn accepted_hint_reads_current_overflow_metadata_before_cow() {
        let (mut pages, mut alloc, mut tree) = new_tree();
        let reference = |head| {
            leaf_node::OverflowRef {
                first_page: PageId(head),
                total_len: 12_000,
            }
            .to_bytes()
        };
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            b"key",
            ValueType::Overflow,
            &reference(101),
        )
        .unwrap();
        let hint = hint_for(&tree, &pages, b"key");
        tree.insert(
            &mut pages,
            &mut alloc,
            TxnId(1),
            b"key",
            ValueType::Overflow,
            &reference(202),
        )
        .unwrap();
        let old_tree = tree.clone();
        let (path, leaf) = tree.walk_to_leaf(&pages, b"key").unwrap();
        assert_eq!(
            tree.insert_at_leaf_with_hint(
                &mut pages,
                &mut alloc,
                TxnId(2),
                b"key",
                ValueType::Inline,
                b"small",
                path,
                leaf,
                hint,
            )
            .unwrap(),
            (false, Some(PageId(202)))
        );
        assert_ne!(old_tree.root, tree.root);
        assert_eq!(tree.entry_count, 1);
        assert_eq!(
            tree.search(&pages, b"key").unwrap(),
            Some((ValueType::Inline, b"small".to_vec()))
        );
        assert_eq!(
            old_tree.search(&pages, b"key").unwrap(),
            Some((ValueType::Overflow, reference(202).to_vec()))
        );
    }
}

#[test]
fn unavailable_or_nonleaf_append_hint_falls_back_without_changing_the_tree() {
    for nonleaf in [false, true] {
        let (mut pages, mut alloc, mut tree) = new_tree();
        assert!(tree
            .insert(
                &mut pages,
                &mut alloc,
                TxnId(1),
                b"a",
                ValueType::Inline,
                b"old"
            )
            .unwrap());
        let leaf = tree.last_insert.as_ref().unwrap().1;
        let held = pages.remove(&leaf).unwrap();
        if nonleaf {
            let mut branch = Page::new(leaf, PageType::Branch, TxnId(1));
            branch.set_right_child(PageId(101));
            branch.rebuild_cells(&[&branch_node::build_cell(PageId(100), b"a")]);
            pages.insert(leaf, branch);
        }
        let old_root = tree.root;
        let old_count = tree.entry_count;
        assert_eq!(
            tree.try_lil_insert(
                &mut pages,
                &mut alloc,
                TxnId(1),
                b"b",
                ValueType::Inline,
                b"new"
            )
            .unwrap(),
            None
        );
        assert_eq!(tree.root, old_root);
        assert_eq!(tree.entry_count, old_count);
        assert!(tree.last_insert.is_none());
        if nonleaf {
            assert_eq!(pages[&leaf].page_type(), Some(PageType::Branch));
            assert_eq!(pages[&leaf].num_cells(), 1);
            assert_eq!(pages[&leaf].right_child(), PageId(101));
        }
        pages.insert(leaf, held);
        assert!(tree
            .insert_if_absent(
                &mut pages,
                &mut alloc,
                TxnId(1),
                b"b",
                ValueType::Inline,
                b"new"
            )
            .unwrap());
        assert_eq!(
            tree.search(&pages, b"a").unwrap(),
            Some((ValueType::Inline, b"old".to_vec()))
        );
        assert_eq!(
            tree.search(&pages, b"b").unwrap(),
            Some((ValueType::Inline, b"new".to_vec()))
        );
    }
}

#[test]
fn default_hasher_page_map_preserves_split_cow_and_removal() {
    let mut pages = std::collections::HashMap::<PageId, Page>::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    let value = [0x52; 512];
    for key in 0..64u32 {
        assert!(tree
            .insert(
                &mut pages,
                &mut alloc,
                TxnId(1),
                &key.to_be_bytes(),
                ValueType::Inline,
                &value
            )
            .unwrap());
    }
    assert!(tree.depth > 1);
    let original = tree.clone();
    for key in 0..64u32 {
        assert!(tree
            .delete(&mut pages, &mut alloc, TxnId(2), &key.to_be_bytes())
            .unwrap());
        assert!(tree.search(&pages, &key.to_be_bytes()).unwrap().is_none());
    }
    assert_eq!(tree.entry_count, 0);
    assert_eq!(tree.depth, 1);
    assert_ne!(tree.root, original.root);
    for key in 0..64u32 {
        assert_eq!(
            original.search(&pages, &key.to_be_bytes()).unwrap(),
            Some((ValueType::Inline, value.to_vec()))
        );
    }
}

#[test]
fn lil_admission_checks_once_and_misses_do_not_enter_page_mutation() {
    use std::cell::Cell;

    struct ObservedPages {
        pages: FxHashMap<PageId, Page>,
        reads: Cell<usize>,
        writes: usize,
    }
    impl PageMap for ObservedPages {
        fn get_page(&self, id: &PageId) -> Option<&Page> {
            self.reads.set(self.reads.get() + 1);
            self.pages.get(id)
        }
    }
    impl MutablePageMap for ObservedPages {
        fn get_page_mut(&mut self, id: &PageId) -> Option<&mut Page> {
            self.writes += 1;
            self.pages.get_mut(id)
        }
        fn insert_page(&mut self, id: PageId, page: Page) {
            self.writes += 1;
            self.pages.insert(id, page);
        }
        fn remove_page(&mut self, id: &PageId) {
            self.writes += 1;
            self.pages.remove(id);
        }
    }

    for value_type in [ValueType::Inline, ValueType::Tombstone] {
        let (mut raw_pages, mut alloc, mut tree) = new_tree();
        tree.insert(&mut raw_pages, &mut alloc, TxnId(1), b"m", value_type, b"")
            .unwrap();
        let root = tree.root;
        let before = raw_pages[&root].as_bytes().to_vec();
        let cached = tree.last_insert.clone();
        let mut pages = ObservedPages {
            pages: raw_pages,
            reads: Cell::new(0),
            writes: 0,
        };

        tree.clear_lil_caches();
        assert_eq!(
            tree.try_lil_insert(
                &mut pages,
                &mut alloc,
                TxnId(1),
                b"z",
                ValueType::Inline,
                b"new"
            )
            .unwrap(),
            None
        );
        assert_eq!(pages.reads.get(), 0);
        tree.last_insert = cached;
        for key in [b"a", b"m"] {
            pages.reads.set(0);
            assert_eq!(
                tree.try_lil_insert(
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    key,
                    ValueType::Inline,
                    b"new"
                )
                .unwrap(),
                None
            );
            assert_eq!(pages.reads.get(), 1);
            assert_eq!(pages.writes, 0);
            assert_eq!(pages.pages[&root].as_bytes().as_slice(), before.as_slice());
            assert!(tree.last_insert.is_some());
        }
        pages.reads.set(0);
        assert_eq!(
            tree.try_lil_insert(
                &mut pages,
                &mut alloc,
                TxnId(1),
                b"z",
                ValueType::Inline,
                b"new"
            )
            .unwrap(),
            Some(true)
        );
        assert_eq!(
            pages.reads.get(),
            1,
            "successful admission must not repeat its lookup"
        );
        assert_eq!(pages.writes, 1);
        assert_eq!(tree.entry_count, 2);

        // The same cached physical ID may disappear or now name a non-leaf.
        // Admission must clear that stale cache before any cell mutation.
        for replacement in [None, Some(Page::new(root, PageType::Branch, TxnId(2)))] {
            match replacement {
                Some(page) => {
                    pages.pages.insert(root, page);
                }
                None => {
                    pages.pages.remove(&root);
                }
            }
            tree.last_insert = Some((Vec::new(), root));
            pages.reads.set(0);
            pages.writes = 0;
            assert_eq!(
                tree.try_lil_insert(
                    &mut pages,
                    &mut alloc,
                    TxnId(2),
                    b"zz",
                    ValueType::Inline,
                    b"new"
                )
                .unwrap(),
                None
            );
            assert!(tree.last_insert.is_none());
            assert_eq!(pages.reads.get(), 1);
            assert_eq!(pages.writes, 0);
            assert_eq!(tree.entry_count, 2);
        }
    }
}

mod unchanged_ancestor_tests {
    use super::*;

    struct ObservedPages {
        pages: FxHashMap<PageId, Page>,
        mutable_branches: Vec<PageId>,
    }

    impl PageMap for ObservedPages {
        fn get_page(&self, id: &PageId) -> Option<&Page> {
            self.pages.get(id)
        }
    }

    impl MutablePageMap for ObservedPages {
        fn get_page_mut(&mut self, id: &PageId) -> Option<&mut Page> {
            let page = self.pages.get_mut(id)?;
            if page.page_type() == Some(PageType::Branch) {
                self.mutable_branches.push(*id);
            }
            Some(page)
        }

        fn insert_page(&mut self, id: PageId, page: Page) {
            self.pages.insert(id, page);
        }

        fn remove_page(&mut self, id: &PageId) {
            self.pages.remove(id);
        }
    }

    fn deep_tree() -> (ObservedPages, PageAllocator, BTree) {
        let mut pages = FxHashMap::default();
        let mut alloc = PageAllocator::new(0);
        let mut leaves = Vec::new();
        for keys in [*b"ac", *b"hj", *b"np", *b"uw"] {
            let id = alloc.allocate();
            let mut page = Page::new(id, PageType::Leaf, TxnId(1));
            for key in keys {
                assert!(leaf_node::insert_append_direct(
                    &mut page,
                    &[key],
                    ValueType::Inline,
                    b"old"
                ));
            }
            pages.insert(id, page);
            leaves.push(id);
        }
        let mut branches = Vec::new();
        for (pair, separator) in [(0, b'g'), (2, b't')] {
            let id = alloc.allocate();
            let mut page = Page::new(id, PageType::Branch, TxnId(1));
            page.rebuild_cells(&[&branch_node::build_cell(leaves[pair], &[separator])]);
            page.set_right_child(leaves[pair + 1]);
            pages.insert(id, page);
            branches.push(id);
        }
        let root = alloc.allocate();
        let mut page = Page::new(root, PageType::Branch, TxnId(1));
        page.rebuild_cells(&[&branch_node::build_cell(branches[0], b"m")]);
        page.set_right_child(branches[1]);
        pages.insert(root, page);
        (
            ObservedPages {
                pages,
                mutable_branches: Vec::new(),
            },
            alloc,
            BTree::from_existing(root, 3, 8),
        )
    }

    #[test]
    fn same_generation_insert_routes_leave_unchanged_deep_branches_immutable() {
        for route in 0..3 {
            let (mut pages, mut alloc, mut tree) = deep_tree();
            let (path, leaf) = tree.walk_to_leaf(&pages, b"b").unwrap();
            assert_eq!(path.len(), 2);
            let original_root = tree.root;
            let branch_bytes: Vec<_> = path
                .iter()
                .map(|(id, _)| (*id, pages.pages[id].as_bytes().to_vec()))
                .collect();
            match route {
                0 => assert!(
                    tree.insert_at_leaf(
                        &mut pages,
                        &mut alloc,
                        TxnId(1),
                        b"b",
                        ValueType::Inline,
                        b"new",
                        path,
                        leaf
                    )
                    .unwrap()
                    .0
                ),
                1 => assert!(tree
                    .insert_or_fetch_at_leaf(
                        &mut pages,
                        &mut alloc,
                        TxnId(1),
                        b"b",
                        ValueType::Inline,
                        b"new",
                        path,
                        leaf
                    )
                    .unwrap()
                    .is_none()),
                2 => assert!(tree
                    .insert_if_absent_at_leaf(
                        &mut pages,
                        &mut alloc,
                        TxnId(1),
                        b"b",
                        ValueType::Inline,
                        b"new",
                        path,
                        leaf
                    )
                    .unwrap()),
                _ => unreachable!(),
            }
            assert!(
                pages.mutable_branches.is_empty(),
                "route {route} acquired unchanged branches mutably"
            );
            assert_eq!(tree.root, original_root);
            assert_eq!(tree.entry_count, 9);
            for (id, bytes) in branch_bytes {
                assert_eq!(pages.pages[&id].as_bytes().as_slice(), bytes);
            }
            for key in *b"achjnpuw" {
                assert_eq!(
                    tree.search(&pages, &[key]).unwrap(),
                    Some((ValueType::Inline, b"old".to_vec()))
                );
            }
            assert_eq!(
                tree.search(&pages, b"b").unwrap(),
                Some((ValueType::Inline, b"new".to_vec()))
            );
        }
    }

    #[test]
    fn propagation_keeps_old_epoch_cow_and_updates_changed_current_child() {
        for (key, replacement_keys) in [(b"a".as_slice(), *b"ac"), (b"h".as_slice(), *b"hj")] {
            for epoch in [TxnId(1), TxnId(2)] {
                let (mut pages, mut alloc, mut tree) = deep_tree();
                let original = tree.clone();
                let (mut path, leaf) = tree.walk_to_leaf(&pages, key).unwrap();
                let old_path = path.clone();
                let old_bytes: Vec<_> = path
                    .iter()
                    .map(|(id, _)| (*id, pages.pages[id].as_bytes().to_vec()))
                    .collect();
                // A public caller can request propagation with an unchanged child.
                // Old epochs still require physical CoW; current ones do not.
                tree.root = propagate_cow_up(&mut pages, &mut alloc, epoch, &mut path, leaf);
                if epoch == TxnId(1) {
                    assert_eq!(tree.root, original.root);
                    assert_eq!(path, old_path);
                    assert!(pages.mutable_branches.is_empty());
                } else {
                    assert_ne!(tree.root, original.root);
                    assert_eq!(pages.mutable_branches.len(), 2);
                    for ((old, _), (new, _)) in old_path.iter().zip(&path) {
                        assert_ne!(old, new);
                        assert_eq!(pages.pages[new].txn_id(), epoch);
                    }
                }
                for (id, bytes) in old_bytes {
                    assert_eq!(pages.pages[&id].as_bytes().as_slice(), bytes);
                }
                assert_eq!(
                    original.search(&pages, key).unwrap(),
                    Some((ValueType::Inline, b"old".to_vec()))
                );

                let replacement = alloc.allocate();
                let mut page = Page::new(replacement, PageType::Leaf, epoch);
                for replacement_key in replacement_keys {
                    assert!(leaf_node::insert_append_direct(
                        &mut page,
                        &[replacement_key],
                        ValueType::Inline,
                        b"replacement"
                    ));
                }
                pages.insert_page(replacement, page);
                pages.mutable_branches.clear();
                let root_before = tree.root;
                tree.root = propagate_cow_up(&mut pages, &mut alloc, epoch, &mut path, replacement);
                assert_eq!(tree.root, root_before);
                assert_eq!(
                    pages.mutable_branches,
                    [path[1].0],
                    "only the changed pointer's current parent needs mutable access"
                );
                assert_eq!(
                    tree.search(&pages, key).unwrap(),
                    Some((ValueType::Inline, b"replacement".to_vec()))
                );
            }
        }
    }

    #[test]
    fn full_leaf_split_still_propagates_and_preserves_old_deep_root() {
        let (mut pages, mut alloc, mut tree) = deep_tree();
        let (path, leaf) = tree.walk_to_leaf(&pages, b"b00000").unwrap();
        let mut packed = Page::new(leaf, PageType::Leaf, TxnId(1));
        let payload = vec![0x52; citadel_core::MAX_INLINE_VALUE_SIZE];
        let mut original_keys = Vec::new();
        for index in 0..1000 {
            let key = format!("a{index:05}").into_bytes();
            if !leaf_node::insert_append_direct(&mut packed, &key, ValueType::Inline, &payload) {
                break;
            }
            original_keys.push(key);
        }
        assert!(original_keys.len() >= 2 && original_keys.len() < 1000);
        pages.pages.insert(leaf, packed);
        tree.entry_count = 6 + original_keys.len() as u64;
        let original = tree.clone();
        let old_root_bytes = pages.pages[&original.root].as_bytes().to_vec();
        let before_pages = pages.pages.len();
        assert!(
            tree.insert_at_leaf(
                &mut pages,
                &mut alloc,
                TxnId(2),
                b"b00000",
                ValueType::Inline,
                &payload,
                path,
                leaf
            )
            .unwrap()
            .0
        );
        assert_ne!(tree.root, original.root);
        assert!(
            pages.pages.len() >= before_pages + 4,
            "leaf split plus two old ancestors must allocate physical pages"
        );
        assert!(!pages.mutable_branches.is_empty());
        assert_eq!(
            pages.pages[&original.root].as_bytes().as_slice(),
            old_root_bytes
        );
        assert_eq!(original.search(&pages, b"b00000").unwrap(), None);
        assert_eq!(
            tree.search(&pages, b"b00000").unwrap(),
            Some((ValueType::Inline, payload.clone()))
        );
        for key in original_keys {
            assert_eq!(
                original.search(&pages, &key).unwrap(),
                Some((ValueType::Inline, payload.clone()))
            );
            assert_eq!(
                tree.search(&pages, &key).unwrap(),
                Some((ValueType::Inline, payload.clone()))
            );
        }
        for key in *b"hjnpuw" {
            assert_eq!(
                tree.search(&pages, &[key]).unwrap(),
                Some((ValueType::Inline, b"old".to_vec()))
            );
        }
        assert_eq!(tree.entry_count, original.entry_count + 1);
    }
}

#[test]
fn missing_at_leaf_routes_preserve_permuted_keys_across_cow_and_splits() {
    const ROWS: u32 = 96;
    let key = |id: u32| {
        let mut bytes = vec![b'k'; 128];
        bytes[..4].copy_from_slice(&id.to_be_bytes());
        bytes
    };
    let original_value = vec![0x31; 256];
    let inserted_value = vec![0x52; 768];
    for route in 0..3 {
        let (mut pages, mut alloc, mut tree) = new_tree();
        for id in 0..ROWS {
            assert!(tree
                .insert(
                    &mut pages,
                    &mut alloc,
                    TxnId(1),
                    &key(id * 2),
                    ValueType::Inline,
                    &original_value
                )
                .unwrap());
        }
        assert!(tree.depth >= 2);
        let old = tree.clone();
        let old_root = pages[&old.root].as_bytes().to_vec();
        let old_pages = pages.len();
        for step in 0..ROWS {
            // Odd keys fill vacancies among old even keys in permuted order.
            let id = ((step * 37) % ROWS) * 2 + 1;
            let k = key(id);
            let (path, leaf) = tree.walk_to_leaf(&pages, &k).unwrap();
            assert!(leaf_node::search(&pages[&leaf], &k).is_err());
            match route {
                0 => assert_eq!(
                    tree.insert_at_leaf(
                        &mut pages,
                        &mut alloc,
                        TxnId(2),
                        &k,
                        ValueType::Inline,
                        &inserted_value,
                        path,
                        leaf
                    )
                    .unwrap(),
                    (true, None)
                ),
                1 => assert!(tree
                    .insert_if_absent_at_leaf(
                        &mut pages,
                        &mut alloc,
                        TxnId(2),
                        &k,
                        ValueType::Inline,
                        &inserted_value,
                        path,
                        leaf
                    )
                    .unwrap()),
                2 => assert!(tree
                    .insert_or_fetch_at_leaf(
                        &mut pages,
                        &mut alloc,
                        TxnId(2),
                        &k,
                        ValueType::Inline,
                        &inserted_value,
                        path,
                        leaf
                    )
                    .unwrap()
                    .is_none()),
                _ => unreachable!(),
            }
        }
        assert_ne!(tree.root, old.root);
        assert!(
            pages.len() > old_pages + old.depth as usize,
            "insertion must split beyond its first CoW path"
        );
        assert_eq!(tree.entry_count, (ROWS * 2) as u64);
        assert_eq!(pages[&old.root].as_bytes().as_slice(), old_root);
        for id in 0..ROWS * 2 {
            let expected = if id % 2 == 0 {
                &original_value
            } else {
                &inserted_value
            };
            assert_eq!(
                tree.search(&pages, &key(id)).unwrap(),
                Some((ValueType::Inline, expected.clone()))
            );
            let old_value = (id % 2 == 0).then(|| (ValueType::Inline, original_value.clone()));
            assert_eq!(old.search(&pages, &key(id)).unwrap(), old_value);
        }
        let mut cursor = crate::cursor::Cursor::first(&pages, tree.root).unwrap();
        for id in 0..ROWS * 2 {
            assert_eq!(cursor.current(&pages).unwrap().key, key(id));
            cursor.next(&pages).unwrap();
        }
        assert!(!cursor.is_valid());
    }
}
