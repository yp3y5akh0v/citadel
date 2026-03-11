//! Integration test: B+ tree correctness verified against BTreeMap reference.
//!
//! Runs deterministic pseudo-random operations (insert, delete, search)
//! against both the Citadel B+ tree and a standard BTreeMap, verifying
//! that results match at every step.

use std::collections::{BTreeMap, HashMap};
use citadel_core::types::*;
use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::btree::BTree;
use citadel_buffer::cursor::Cursor;
use citadel_page::page::Page;
use citadel_page::branch_node;

/// Simple deterministic PRNG (xorshift32) for reproducible tests.
struct Rng(u32);
impl Rng {
    fn new(seed: u32) -> Self { Self(seed) }
    fn next(&mut self) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 17;
        self.0 ^= self.0 << 5;
        self.0
    }
    fn next_range(&mut self, max: u32) -> u32 {
        self.next() % max
    }
}

#[test]
fn btree_vs_btreemap_oracle() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let mut rng = Rng::new(42);
    let ops = 5000;

    for i in 0..ops {
        let op = rng.next_range(10);
        let key = format!("key-{:04}", rng.next_range(500));
        let key_bytes = key.as_bytes();

        if op < 6 {
            // INSERT (60% of operations)
            let value = format!("val-{i}");
            let val_bytes = value.as_bytes();

            let tree_is_new = tree.insert(
                &mut pages, &mut alloc, TxnId(1),
                key_bytes, ValueType::Inline, val_bytes,
            ).unwrap();

            let oracle_existed = oracle.insert(key.clone().into_bytes(), value.into_bytes()).is_some();
            assert_eq!(tree_is_new, !oracle_existed,
                "insert mismatch for key={key} at op {i}");
        } else if op < 8 {
            // DELETE (20% of operations)
            let tree_found = tree.delete(
                &mut pages, &mut alloc, TxnId(1), key_bytes,
            ).unwrap();

            let oracle_found = oracle.remove(key.as_bytes()).is_some();
            assert_eq!(tree_found, oracle_found,
                "delete mismatch for key={key} at op {i}");
        } else {
            // SEARCH (20% of operations)
            let tree_result = tree.search(&pages, key_bytes).unwrap();
            let oracle_result = oracle.get(key.as_bytes());

            match (&tree_result, oracle_result) {
                (Some((_, tv)), Some(ov)) => {
                    assert_eq!(tv, ov, "value mismatch for key={key} at op {i}");
                }
                (None, None) => {}
                _ => panic!(
                    "search mismatch for key={key} at op {i}: tree={:?}, oracle={:?}",
                    tree_result.is_some(), oracle_result.is_some()
                ),
            }
        }

        // Verify entry count matches
        assert_eq!(tree.entry_count, oracle.len() as u64,
            "entry count mismatch at op {i}");
    }

    // Final verification: iterate all entries and compare
    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    let mut tree_entries = Vec::new();
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        tree_entries.push((entry.key.clone(), entry.value.clone()));
        cursor.next(&pages).unwrap();
    }

    let oracle_entries: Vec<(Vec<u8>, Vec<u8>)> = oracle.into_iter().collect();
    assert_eq!(tree_entries.len(), oracle_entries.len(),
        "final entry count mismatch: tree={}, oracle={}", tree_entries.len(), oracle_entries.len());

    for (i, ((tk, tv), (ok, ov))) in tree_entries.iter().zip(oracle_entries.iter()).enumerate() {
        assert_eq!(tk, ok, "key mismatch at position {i}");
        assert_eq!(tv, ov, "value mismatch at position {i}");
    }
}

#[test]
fn btree_cursor_range_scan() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    // Insert 1000 sequential keys
    for i in 0..1000u32 {
        let key = format!("{i:06}");
        let val = format!("v{i}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, val.as_bytes()).unwrap();
    }

    // Range scan [000200, 000300)
    let mut cursor = Cursor::seek(&pages, tree.root, b"000200").unwrap();
    let mut count = 0;
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        if entry.key >= b"000300".to_vec() {
            break;
        }
        let expected_key = format!("{:06}", 200 + count);
        assert_eq!(entry.key, expected_key.as_bytes());
        count += 1;
        cursor.next(&pages).unwrap();
    }
    assert_eq!(count, 100);
}

#[test]
fn btree_cursor_reverse_range() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..100u32 {
        let key = format!("{i:04}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"x").unwrap();
    }

    // Iterate backward from end
    let mut cursor = Cursor::last(&pages, tree.root).unwrap();
    let mut prev_key: Option<Vec<u8>> = None;
    let mut count = 0;
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        if let Some(ref pk) = prev_key {
            assert!(entry.key < *pk, "reverse order violated");
        }
        prev_key = Some(entry.key);
        count += 1;
        cursor.prev(&pages).unwrap();
    }
    assert_eq!(count, 100);
}

#[test]
fn btree_cow_isolation() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    // Insert some keys
    tree.insert(&mut pages, &mut alloc, TxnId(1), b"a", ValueType::Inline, b"1").unwrap();
    tree.insert(&mut pages, &mut alloc, TxnId(1), b"b", ValueType::Inline, b"2").unwrap();

    let root_v1 = tree.root;

    // Modify — should create new pages via CoW
    tree.insert(&mut pages, &mut alloc, TxnId(2), b"c", ValueType::Inline, b"3").unwrap();
    let root_v2 = tree.root;

    assert_ne!(root_v1, root_v2, "CoW should produce new root");

    // The new root should have all 3 keys
    let result = tree.search(&pages, b"c").unwrap();
    assert_eq!(result, Some((ValueType::Inline, b"3".to_vec())));
}

#[test]
fn btree_large_values() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    // Insert keys with large inline values (near the 1920-byte inline limit)
    for i in 0..20u32 {
        let key = format!("bigkey-{i:04}");
        let val = vec![0xAB; 1800]; // 1800 bytes per value
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, &val).unwrap();
    }

    assert_eq!(tree.entry_count, 20);
    assert!(tree.depth >= 2, "large values should cause more splits");

    // Verify all keys present
    for i in 0..20u32 {
        let key = format!("bigkey-{i:04}");
        let result = tree.search(&pages, key.as_bytes()).unwrap();
        assert!(result.is_some(), "key {key} should be present");
        let (_, val) = result.unwrap();
        assert_eq!(val.len(), 1800);
    }
}

#[test]
fn btree_tombstone_values() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    // Insert a tombstone
    tree.insert(&mut pages, &mut alloc, TxnId(1), b"deleted", ValueType::Tombstone, b"").unwrap();
    let result = tree.search(&pages, b"deleted").unwrap();
    assert_eq!(result, Some((ValueType::Tombstone, vec![])));
}

#[test]
fn btree_max_key_length() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    // 2048-byte key (max per plan)
    let big_key = vec![0x42; 2048];
    tree.insert(&mut pages, &mut alloc, TxnId(1), &big_key, ValueType::Inline, b"big").unwrap();

    let result = tree.search(&pages, &big_key).unwrap();
    assert_eq!(result, Some((ValueType::Inline, b"big".to_vec())));

    // Small key should also work alongside
    tree.insert(&mut pages, &mut alloc, TxnId(1), b"small", ValueType::Inline, b"tiny").unwrap();
    assert_eq!(tree.entry_count, 2);
}

#[test]
fn btree_insert_delete_reinsert() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    // Insert, delete, re-insert the same key
    tree.insert(&mut pages, &mut alloc, TxnId(1), b"key", ValueType::Inline, b"v1").unwrap();
    tree.delete(&mut pages, &mut alloc, TxnId(1), b"key").unwrap();
    assert_eq!(tree.search(&pages, b"key").unwrap(), None);

    tree.insert(&mut pages, &mut alloc, TxnId(1), b"key", ValueType::Inline, b"v2").unwrap();
    assert_eq!(
        tree.search(&pages, b"key").unwrap(),
        Some((ValueType::Inline, b"v2".to_vec()))
    );
}

#[test]
fn btree_allocator_frees_pages_on_cow() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..10u32 {
        let key = format!("k{i}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    let freed_before = alloc.freed_count();
    // Each insert after the first CoWs the root, so many pages should be freed
    assert!(freed_before > 0, "CoW should free old pages");

    // After commit, freed pages should be cleared
    let freed = alloc.commit();
    assert!(!freed.is_empty());
    assert_eq!(alloc.freed_count(), 0);
}

// === Edge Case Tests ===

#[test]
fn btree_single_entry() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    tree.insert(&mut pages, &mut alloc, TxnId(1), b"only", ValueType::Inline, b"one").unwrap();
    assert_eq!(tree.entry_count, 1);
    assert_eq!(tree.depth, 1);

    let result = tree.search(&pages, b"only").unwrap();
    assert_eq!(result, Some((ValueType::Inline, b"one".to_vec())));

    // Cursor on single-entry tree
    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    assert!(cursor.is_valid());
    let entry = cursor.current(&pages).unwrap();
    assert_eq!(entry.key, b"only");
    cursor.next(&pages).unwrap();
    assert!(!cursor.is_valid());

    // Delete the single entry
    tree.delete(&mut pages, &mut alloc, TxnId(1), b"only").unwrap();
    assert_eq!(tree.entry_count, 0);
    assert_eq!(tree.search(&pages, b"only").unwrap(), None);
}

#[test]
fn btree_delete_all_and_reinsert() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let count = 200;
    let keys: Vec<String> = (0..count).map(|i| format!("key-{i:04}")).collect();

    // Insert all
    for k in &keys {
        tree.insert(&mut pages, &mut alloc, TxnId(1), k.as_bytes(), ValueType::Inline, b"v").unwrap();
    }
    assert_eq!(tree.entry_count, count as u64);

    // Delete all
    for k in &keys {
        tree.delete(&mut pages, &mut alloc, TxnId(1), k.as_bytes()).unwrap();
    }
    assert_eq!(tree.entry_count, 0);

    // Verify empty
    for k in &keys {
        assert_eq!(tree.search(&pages, k.as_bytes()).unwrap(), None);
    }

    // Cursor on empty tree
    let cursor = Cursor::first(&pages, tree.root).unwrap();
    assert!(!cursor.is_valid());

    // Re-insert all with new values
    for k in &keys {
        tree.insert(&mut pages, &mut alloc, TxnId(2), k.as_bytes(), ValueType::Inline, b"v2").unwrap();
    }
    assert_eq!(tree.entry_count, count as u64);

    // Verify all present with new values
    for k in &keys {
        let result = tree.search(&pages, k.as_bytes()).unwrap();
        assert_eq!(result, Some((ValueType::Inline, b"v2".to_vec())));
    }
}

#[test]
fn btree_depth_grows_with_entries() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    assert_eq!(tree.depth, 1, "empty tree should have depth 1");

    // Insert entries and track depth
    let mut prev_depth = tree.depth;
    for i in 0..3000u32 {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
        if tree.depth > prev_depth {
            prev_depth = tree.depth;
        }
    }

    // With 3000 entries, tree must be at least depth 2
    assert!(tree.depth >= 2, "3000 entries should produce depth >= 2, got {}", tree.depth);
    assert_eq!(tree.entry_count, 3000);
}

#[test]
fn btree_sequential_insert_stress() {
    // Sequential keys are a worst-case pattern (all inserts at the right edge)
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let count = 5000u32;
    for i in 0..count {
        let key = format!("{i:08}");
        let val = format!("v{i}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, val.as_bytes()).unwrap();
    }
    assert_eq!(tree.entry_count, count as u64);

    // Verify all present
    for i in 0..count {
        let key = format!("{i:08}");
        let val = format!("v{i}");
        assert_eq!(
            tree.search(&pages, key.as_bytes()).unwrap(),
            Some((ValueType::Inline, val.into_bytes())),
            "sequential key {key} should be present"
        );
    }

    // Full forward cursor scan
    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    let mut scanned = 0u32;
    let mut prev_key: Option<Vec<u8>> = None;
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        if let Some(ref pk) = prev_key {
            assert!(entry.key > *pk, "cursor must return keys in sorted order");
        }
        prev_key = Some(entry.key);
        scanned += 1;
        cursor.next(&pages).unwrap();
    }
    assert_eq!(scanned, count);
}

#[test]
fn btree_cursor_bidirectional() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..100u32 {
        let key = format!("{i:04}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    // Seek to middle
    let mut cursor = Cursor::seek(&pages, tree.root, b"0050").unwrap();
    assert!(cursor.is_valid());
    let entry = cursor.current(&pages).unwrap();
    assert_eq!(entry.key, b"0050");

    // Go forward 5 entries
    for expected in 51..56u32 {
        cursor.next(&pages).unwrap();
        let entry = cursor.current(&pages).unwrap();
        assert_eq!(entry.key, format!("{expected:04}").as_bytes());
    }

    // Now go backward 10 entries (back past starting point)
    for expected in (45..55u32).rev() {
        cursor.prev(&pages).unwrap();
        let entry = cursor.current(&pages).unwrap();
        assert_eq!(entry.key, format!("{expected:04}").as_bytes());
    }
}

#[test]
fn btree_delete_until_empty_one_by_one() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let count = 100u32;
    let keys: Vec<String> = (0..count).map(|i| format!("{i:04}")).collect();

    for k in &keys {
        tree.insert(&mut pages, &mut alloc, TxnId(1), k.as_bytes(), ValueType::Inline, b"v").unwrap();
    }
    assert_eq!(tree.entry_count, count as u64);

    // Delete one by one in random-ish order (reverse)
    for k in keys.iter().rev() {
        let existed = tree.delete(&mut pages, &mut alloc, TxnId(1), k.as_bytes()).unwrap();
        assert!(existed, "key {k} should exist before deletion");
    }
    assert_eq!(tree.entry_count, 0);

    // Verify empty
    let cursor = Cursor::first(&pages, tree.root).unwrap();
    assert!(!cursor.is_valid());
}

#[test]
fn btree_heavy_random_oracle() {
    // Heavier variant: 10K operations
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let mut rng = Rng::new(123456);
    let ops = 10000;

    for i in 0..ops {
        let op = rng.next_range(10);
        let key = format!("k{:05}", rng.next_range(1000));
        let key_bytes = key.as_bytes();

        if op < 5 {
            let value = format!("v{i}");
            let tree_new = tree.insert(&mut pages, &mut alloc, TxnId(1), key_bytes, ValueType::Inline, value.as_bytes()).unwrap();
            let oracle_existed = oracle.insert(key.into_bytes(), value.into_bytes()).is_some();
            assert_eq!(tree_new, !oracle_existed, "insert mismatch at op {i}");
        } else if op < 8 {
            let tree_found = tree.delete(&mut pages, &mut alloc, TxnId(1), key_bytes).unwrap();
            let oracle_found = oracle.remove(key.as_bytes()).is_some();
            assert_eq!(tree_found, oracle_found, "delete mismatch at op {i}");
        } else {
            let tree_result = tree.search(&pages, key_bytes).unwrap();
            let oracle_result = oracle.get(key.as_bytes());
            match (&tree_result, oracle_result) {
                (Some((_, tv)), Some(ov)) => assert_eq!(tv, ov, "value mismatch at op {i}"),
                (None, None) => {}
                _ => panic!("search mismatch at op {i}"),
            }
        }
        assert_eq!(tree.entry_count, oracle.len() as u64, "count mismatch at op {i}");
    }

    // Final full iteration comparison
    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    let mut tree_entries = Vec::new();
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        tree_entries.push((entry.key.clone(), entry.value.clone()));
        cursor.next(&pages).unwrap();
    }
    let oracle_entries: Vec<_> = oracle.into_iter().collect();
    assert_eq!(tree_entries.len(), oracle_entries.len());
    for ((tk, tv), (ok, ov)) in tree_entries.iter().zip(oracle_entries.iter()) {
        assert_eq!(tk, ok);
        assert_eq!(tv, ov);
    }
}

#[test]
fn btree_duplicate_key_updates_value() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    // Insert same key 10 times with different values
    for i in 0..10u32 {
        let val = format!("version-{i}");
        let is_new = tree.insert(&mut pages, &mut alloc, TxnId(1), b"key", ValueType::Inline, val.as_bytes()).unwrap();
        if i == 0 {
            assert!(is_new);
        } else {
            assert!(!is_new, "subsequent inserts should be updates");
        }
    }

    // Should only have 1 entry
    assert_eq!(tree.entry_count, 1);

    // Should have the last value
    let result = tree.search(&pages, b"key").unwrap();
    assert_eq!(result, Some((ValueType::Inline, b"version-9".to_vec())));
}

// === Additional B+ Tree Edge Cases ===

#[test]
fn btree_variable_key_length_stress() {
    // Variable-length keys (1 to 512 bytes) with random insert/delete stress
    // split/merge paths and guard against page type confusion during rebalance.
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let mut rng = Rng::new(0xDEAD);
    let ops = 5000;

    for i in 0..ops {
        let op = rng.next_range(10);
        // Key length varies from 1 to 512 bytes
        let key_len = 1 + (rng.next_range(512)) as usize;
        let key: Vec<u8> = (0..key_len).map(|j| {
            ((rng.next_range(256)) as u8).wrapping_add(j as u8)
        }).collect();

        if op < 6 {
            // Insert with small value
            let val = format!("v{i}");
            let tree_new = tree.insert(
                &mut pages, &mut alloc, TxnId(1),
                &key, ValueType::Inline, val.as_bytes(),
            ).unwrap();
            let oracle_existed = oracle.insert(key, val.into_bytes()).is_some();
            assert_eq!(tree_new, !oracle_existed,
                "insert mismatch at op {i} (key_len={key_len})");
        } else {
            // Delete
            let tree_found = tree.delete(
                &mut pages, &mut alloc, TxnId(1), &key,
            ).unwrap();
            let oracle_found = oracle.remove(&key).is_some();
            assert_eq!(tree_found, oracle_found,
                "delete mismatch at op {i} (key_len={key_len})");
        }
        assert_eq!(tree.entry_count, oracle.len() as u64,
            "count mismatch at op {i}");
    }

    // Verify all branch pages contain only branch cells and all leaf pages
    // contain only leaf cells (guards against cross-merge corruption)
    let mut stack = vec![tree.root];
    let mut branch_count = 0u32;
    let mut leaf_count = 0u32;
    while let Some(page_id) = stack.pop() {
        let page = pages.get(&page_id).unwrap();
        match page.page_type() {
            Some(PageType::Branch) => {
                branch_count += 1;
                let num_cells = page.num_cells() as usize;
                for i in 0..num_cells {
                    let child = branch_node::get_child(page, i);
                    stack.push(child);
                }
                let right = page.right_child();
                if right.is_valid() {
                    stack.push(right);
                }
            }
            Some(PageType::Leaf) => {
                leaf_count += 1;
            }
            other => panic!("unexpected page type {:?} for page {:?}", other, page_id),
        }
    }
    assert!(leaf_count >= 1, "should have at least one leaf page");
    // If tree has depth > 1, should have branches
    if tree.depth > 1 {
        assert!(branch_count >= 1, "multi-level tree should have branch pages");
    }

    // Full cursor scan must match oracle
    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    let mut tree_entries = Vec::new();
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        if entry.val_type != ValueType::Tombstone {
            tree_entries.push((entry.key.clone(), entry.value.clone()));
        }
        cursor.next(&pages).unwrap();
    }
    let oracle_entries: Vec<_> = oracle.into_iter().collect();
    assert_eq!(tree_entries.len(), oracle_entries.len(),
        "final scan count mismatch");
    for ((tk, tv), (ok, ov)) in tree_entries.iter().zip(oracle_entries.iter()) {
        assert_eq!(tk, ok, "key mismatch in final scan");
        assert_eq!(tv, ov, "value mismatch in final scan");
    }
}

#[test]
fn btree_insert_delete_same_key_repeatedly() {
    // Guards against double-free: inserting and deleting the same key
    // repeatedly should CoW the same leaf multiple times without corruption.
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    // First populate the tree so the target key is in a multi-page tree
    for i in 0..200u32 {
        let key = format!("{i:04}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    // Now repeatedly insert and delete the same key
    let target = b"0100";
    for round in 0..50u32 {
        // Delete it
        let existed = tree.delete(&mut pages, &mut alloc, TxnId(1), target).unwrap();
        assert!(existed, "key should exist before delete in round {round}");
        assert_eq!(tree.entry_count, 199);

        // Re-insert it
        let is_new = tree.insert(
            &mut pages, &mut alloc, TxnId(1),
            target, ValueType::Inline, format!("round-{round}").as_bytes(),
        ).unwrap();
        assert!(is_new, "key should be new after delete in round {round}");
        assert_eq!(tree.entry_count, 200);
    }

    // Verify the final value
    let result = tree.search(&pages, target).unwrap();
    assert_eq!(result, Some((ValueType::Inline, b"round-49".to_vec())));

    // Verify the tree is still navigable
    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    let mut count = 0u32;
    while cursor.is_valid() {
        count += 1;
        cursor.next(&pages).unwrap();
    }
    assert_eq!(count, 200, "full tree scan should find all 200 entries");
}
