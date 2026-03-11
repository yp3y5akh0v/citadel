//! Integration test: B+ tree correctness verified against BTreeMap oracle.
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
