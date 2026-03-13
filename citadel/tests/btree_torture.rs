//! B+ tree torture tests: edge cases, stress, and correctness verification
//! at the in-memory BTree/Cursor/Allocator level.

use std::collections::{BTreeMap, HashMap, HashSet};
use citadel_core::types::*;
use citadel_core::constants::{MAX_KEY_SIZE, MAX_INLINE_VALUE_SIZE};
use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::btree::BTree;
use citadel_buffer::cursor::Cursor;
use citadel_page::page::Page;
use citadel_page::branch_node;
use citadel_page::leaf_node;

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

// =========================================================================
// Empty key
// =========================================================================

#[test]
fn empty_key() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let is_new = tree.insert(&mut pages, &mut alloc, TxnId(1), b"", ValueType::Inline, b"empty-key-val").unwrap();
    assert!(is_new);
    assert_eq!(tree.entry_count, 1);

    let result = tree.search(&pages, b"").unwrap();
    assert_eq!(result, Some((ValueType::Inline, b"empty-key-val".to_vec())));

    tree.insert(&mut pages, &mut alloc, TxnId(1), b"a", ValueType::Inline, b"a-val").unwrap();
    tree.insert(&mut pages, &mut alloc, TxnId(1), b"\x00", ValueType::Inline, b"null-val").unwrap();

    let cursor = Cursor::first(&pages, tree.root).unwrap();
    let entry = cursor.current(&pages).unwrap();
    assert_eq!(entry.key, b"", "empty key should be first");

    let existed = tree.delete(&mut pages, &mut alloc, TxnId(1), b"").unwrap();
    assert!(existed);
    assert_eq!(tree.search(&pages, b"").unwrap(), None);
    assert_eq!(tree.entry_count, 2);
}

// =========================================================================
// Max key + max value simultaneously
// =========================================================================

#[test]
fn max_key_max_value_together() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let big_key = vec![0xAA; MAX_KEY_SIZE];
    let big_val = vec![0xBB; MAX_INLINE_VALUE_SIZE];

    let is_new = tree.insert(&mut pages, &mut alloc, TxnId(1), &big_key, ValueType::Inline, &big_val).unwrap();
    assert!(is_new);

    let result = tree.search(&pages, &big_key).unwrap().unwrap();
    assert_eq!(result.0, ValueType::Inline);
    assert_eq!(result.1.len(), MAX_INLINE_VALUE_SIZE);
    assert!(result.1.iter().all(|b| *b == 0xBB));

    let big_val2 = vec![0xCC; MAX_INLINE_VALUE_SIZE];
    let is_new = tree.insert(&mut pages, &mut alloc, TxnId(1), &big_key, ValueType::Inline, &big_val2).unwrap();
    assert!(!is_new);

    let result = tree.search(&pages, &big_key).unwrap().unwrap();
    assert!(result.1.iter().all(|b| *b == 0xCC));
}

// =========================================================================
// Multiple max-size entries force splits
// =========================================================================

#[test]
fn many_max_size_entries() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let count = 30;
    for i in 0..count {
        let mut key = vec![0u8; MAX_KEY_SIZE];
        key[0] = (i / 256) as u8;
        key[1] = (i % 256) as u8;
        let val = vec![(i & 0xFF) as u8; MAX_INLINE_VALUE_SIZE];
        tree.insert(&mut pages, &mut alloc, TxnId(1), &key, ValueType::Inline, &val).unwrap();
    }

    assert_eq!(tree.entry_count, count as u64);
    assert!(tree.depth >= 3, "30 max-size entries should produce depth >= 3, got {}", tree.depth);

    for i in 0..count {
        let mut key = vec![0u8; MAX_KEY_SIZE];
        key[0] = (i / 256) as u8;
        key[1] = (i % 256) as u8;
        let result = tree.search(&pages, &key).unwrap();
        assert!(result.is_some(), "entry {i} should exist");
        let (_, val) = result.unwrap();
        assert_eq!(val.len(), MAX_INLINE_VALUE_SIZE);
        assert!(val.iter().all(|b| *b == (i & 0xFF) as u8));
    }

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    let mut count_scanned = 0u32;
    let mut prev_key: Option<Vec<u8>> = None;
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        if let Some(ref pk) = prev_key {
            assert!(entry.key > *pk, "cursor must be sorted");
        }
        prev_key = Some(entry.key);
        count_scanned += 1;
        cursor.next(&pages).unwrap();
    }
    assert_eq!(count_scanned, count);
}

// =========================================================================
// Delete from left edge repeatedly (stresses merge on left side)
// =========================================================================

#[test]
fn delete_from_left_edge() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let count = 500u32;
    for i in 0..count {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    for i in 0..count {
        let key = format!("{i:06}");
        let existed = tree.delete(&mut pages, &mut alloc, TxnId(1), key.as_bytes()).unwrap();
        assert!(existed, "key {key} should exist");
        assert_eq!(tree.entry_count, (count - 1 - i) as u64);

        if tree.entry_count > 0 {
            let cursor = Cursor::first(&pages, tree.root).unwrap();
            assert!(cursor.is_valid());
            let entry = cursor.current(&pages).unwrap();
            let expected = format!("{:06}", i + 1);
            assert_eq!(entry.key, expected.as_bytes(), "first key after deleting {i}");
        }
    }
    assert_eq!(tree.entry_count, 0);
}

// =========================================================================
// Delete from right edge repeatedly (stresses merge on right side)
// =========================================================================

#[test]
fn delete_from_right_edge() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let count = 500u32;
    for i in 0..count {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    for i in (0..count).rev() {
        let key = format!("{i:06}");
        let existed = tree.delete(&mut pages, &mut alloc, TxnId(1), key.as_bytes()).unwrap();
        assert!(existed, "key {key} should exist");

        if tree.entry_count > 0 {
            let cursor = Cursor::last(&pages, tree.root).unwrap();
            assert!(cursor.is_valid());
            let entry = cursor.current(&pages).unwrap();
            let expected = format!("{:06}", i - 1);
            assert_eq!(entry.key, expected.as_bytes(), "last key after deleting {i}");
        }
    }
    assert_eq!(tree.entry_count, 0);
}

// =========================================================================
// Alternating insert/delete pattern (delete every other)
// =========================================================================

#[test]
fn delete_every_other_key() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let count = 1000u32;
    for i in 0..count {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
        expected.insert(key.into_bytes(), b"v".to_vec());
    }

    for i in (0..count).step_by(2) {
        let key = format!("{i:06}");
        tree.delete(&mut pages, &mut alloc, TxnId(1), key.as_bytes()).unwrap();
        expected.remove(key.as_bytes());
    }

    assert_eq!(tree.entry_count, expected.len() as u64);

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    let expected_entries: Vec<_> = expected.into_iter().collect();
    for (ok, _) in &expected_entries {
        assert!(cursor.is_valid());
        let entry = cursor.current(&pages).unwrap();
        assert_eq!(&entry.key, ok);
        cursor.next(&pages).unwrap();
    }
    assert!(!cursor.is_valid());
}

// =========================================================================
// Cursor: prev from first position, next from last position
// =========================================================================

#[test]
fn cursor_boundary_movement() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..50u32 {
        let key = format!("{i:04}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    assert!(cursor.is_valid());
    let moved = cursor.prev(&pages).unwrap();
    assert!(!moved);
    assert!(!cursor.is_valid());

    let mut cursor = Cursor::last(&pages, tree.root).unwrap();
    assert!(cursor.is_valid());
    let entry = cursor.current(&pages).unwrap();
    assert_eq!(entry.key, b"0049");
    let moved = cursor.next(&pages).unwrap();
    assert!(!moved);
    assert!(!cursor.is_valid());
}

// =========================================================================
// Cursor: seek past all keys
// =========================================================================

#[test]
fn cursor_seek_past_all() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..100u32 {
        let key = format!("{i:04}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    let cursor = Cursor::seek(&pages, tree.root, b"zzzz").unwrap();
    assert!(!cursor.is_valid(), "seek past all keys should be invalid");

    let cursor = Cursor::seek(&pages, tree.root, b"").unwrap();
    assert!(cursor.is_valid(), "seek with empty key should find first entry");
    let entry = cursor.current(&pages).unwrap();
    assert_eq!(entry.key, b"0000");
}

// =========================================================================
// Cursor: full reverse iteration
// =========================================================================

#[test]
fn cursor_full_reverse_matches_forward() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..300u32 {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    let mut forward_keys = Vec::new();
    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    while cursor.is_valid() {
        forward_keys.push(cursor.current(&pages).unwrap().key);
        cursor.next(&pages).unwrap();
    }

    let mut reverse_keys = Vec::new();
    let mut cursor = Cursor::last(&pages, tree.root).unwrap();
    while cursor.is_valid() {
        reverse_keys.push(cursor.current(&pages).unwrap().key);
        cursor.prev(&pages).unwrap();
    }

    reverse_keys.reverse();
    assert_eq!(forward_keys, reverse_keys);
}

// =========================================================================
// Cursor: seek then reverse
// =========================================================================

#[test]
fn cursor_seek_then_reverse() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..200u32 {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    let mut cursor = Cursor::seek(&pages, tree.root, b"000100").unwrap();
    assert!(cursor.is_valid());
    assert_eq!(cursor.current(&pages).unwrap().key, b"000100");

    let mut count = 0;
    loop {
        let moved = cursor.prev(&pages).unwrap();
        if !moved { break; }
        count += 1;
    }
    assert_eq!(count, 100);
}

// =========================================================================
// Binary keys with all byte values
// =========================================================================

#[test]
fn binary_keys_all_byte_values() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for b in 0..=255u8 {
        let key = [b];
        tree.insert(&mut pages, &mut alloc, TxnId(1), &key, ValueType::Inline, &[b]).unwrap();
    }
    assert_eq!(tree.entry_count, 256);

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    for expected in 0..=255u8 {
        assert!(cursor.is_valid());
        let entry = cursor.current(&pages).unwrap();
        assert_eq!(entry.key, vec![expected], "byte order mismatch at {expected}");
        cursor.next(&pages).unwrap();
    }
    assert!(!cursor.is_valid());

    for b in 0..=255u8 {
        let key = [b];
        let existed = tree.delete(&mut pages, &mut alloc, TxnId(1), &key).unwrap();
        assert!(existed);
    }
    assert_eq!(tree.entry_count, 0);
}

// =========================================================================
// Keys that differ only in the last byte
// =========================================================================

#[test]
fn keys_differ_only_in_last_byte() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let prefix = vec![0xDE; 100];
    for b in 0..=255u8 {
        let mut key = prefix.clone();
        key.push(b);
        tree.insert(&mut pages, &mut alloc, TxnId(1), &key, ValueType::Inline, &[b]).unwrap();
    }
    assert_eq!(tree.entry_count, 256);

    for b in 0..=255u8 {
        let mut key = prefix.clone();
        key.push(b);
        let result = tree.search(&pages, &key).unwrap();
        assert_eq!(result, Some((ValueType::Inline, vec![b])));
    }
}

// =========================================================================
// Keys that are prefixes of each other
// =========================================================================

#[test]
fn prefix_key_chains() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for len in 1..=200 {
        let key = vec![b'A'; len];
        tree.insert(&mut pages, &mut alloc, TxnId(1), &key, ValueType::Inline, &[len as u8]).unwrap();
    }
    assert_eq!(tree.entry_count, 200);

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    let mut prev_len = 0;
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        assert!(entry.key.len() > prev_len, "prefix keys must sort shorter before longer");
        prev_len = entry.key.len();
        cursor.next(&pages).unwrap();
    }

    for len in (1..=200).step_by(2) {
        let key = vec![b'A'; len];
        tree.delete(&mut pages, &mut alloc, TxnId(1), &key).unwrap();
    }
    assert_eq!(tree.entry_count, 100);

    for len in (2..=200).step_by(2) {
        let key = vec![b'A'; len];
        let result = tree.search(&pages, &key).unwrap();
        assert!(result.is_some(), "even-length key {len} should exist");
    }
}

// =========================================================================
// Allocator: heavy page churn with CoW across many transactions
// =========================================================================

#[test]
fn allocator_heavy_page_churn() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..200u32 {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v1").unwrap();
    }
    let freed1 = alloc.commit();
    alloc.add_ready_to_use(freed1);

    for txn in 2..=11u64 {
        for i in 0..200u32 {
            let key = format!("{i:06}");
            let val = format!("v{txn}");
            tree.insert(&mut pages, &mut alloc, TxnId(txn), key.as_bytes(), ValueType::Inline, val.as_bytes()).unwrap();
        }
        let freed = alloc.commit();
        alloc.add_ready_to_use(freed);
    }

    for i in 0..200u32 {
        let key = format!("{i:06}");
        let result = tree.search(&pages, key.as_bytes()).unwrap();
        assert_eq!(result, Some((ValueType::Inline, b"v11".to_vec())));
    }

    let hwm = alloc.high_water_mark();
    assert!(hwm < 1000, "HWM should be bounded with page reuse, got {hwm}");
}

// =========================================================================
// Allocator: rollback discards freed pages
// =========================================================================

#[test]
fn allocator_rollback_discards_freed() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..50u32 {
        let key = format!("{i:04}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }
    let freed1 = alloc.commit();
    alloc.add_ready_to_use(freed1);
    let hwm_after_commit = alloc.high_water_mark();

    for i in 0..50u32 {
        let key = format!("{i:04}");
        tree.insert(&mut pages, &mut alloc, TxnId(2), key.as_bytes(), ValueType::Inline, b"v2").unwrap();
    }
    assert!(alloc.freed_count() > 0, "CoW should free pages");

    alloc.rollback();
    assert_eq!(alloc.freed_count(), 0, "rollback should clear freed pages");

    assert!(alloc.high_water_mark() >= hwm_after_commit);
}

// =========================================================================
// Tree structure verification: all leaves at same depth
// =========================================================================

#[test]
fn all_leaves_at_same_depth() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let mut rng = Rng::new(999);
    for i in 0..2000u32 {
        let key = format!("k{:06}", rng.next_range(5000));
        let val = format!("v{i}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, val.as_bytes()).unwrap();
    }
    for _ in 0..500 {
        let key = format!("k{:06}", rng.next_range(5000));
        tree.delete(&mut pages, &mut alloc, TxnId(1), key.as_bytes()).unwrap();
    }

    let mut queue: Vec<(PageId, u32)> = vec![(tree.root, 0)];
    let mut leaf_depths = HashSet::new();

    while let Some((page_id, depth)) = queue.pop() {
        let page = pages.get(&page_id).unwrap();
        match page.page_type() {
            Some(PageType::Leaf) => {
                leaf_depths.insert(depth);
            }
            Some(PageType::Branch) => {
                for i in 0..page.num_cells() {
                    let cell = branch_node::read_cell(page, i);
                    queue.push((cell.child, depth + 1));
                }
                if page.right_child().is_valid() {
                    queue.push((page.right_child(), depth + 1));
                }
            }
            _ => panic!("unexpected page type"),
        }
    }

    assert_eq!(leaf_depths.len(), 1,
        "all leaves must be at the same depth, got depths: {:?}", leaf_depths);
    assert_eq!(*leaf_depths.iter().next().unwrap(), (tree.depth - 1) as u32);
}

// =========================================================================
// Tree structure: keys in branch pages are valid separators
// =========================================================================

#[test]
fn branch_separators_valid() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..1000u32 {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    let mut stack = vec![tree.root];
    while let Some(page_id) = stack.pop() {
        let page = pages.get(&page_id).unwrap();
        if page.page_type() != Some(PageType::Branch) {
            continue;
        }

        let num_cells = page.num_cells();
        for i in 1..num_cells {
            let prev = branch_node::read_cell(page, i - 1);
            let curr = branch_node::read_cell(page, i);
            assert!(prev.key < curr.key,
                "branch separator order violated at page {:?}, cells {}-{}", page_id, i-1, i);
        }

        for i in 0..num_cells {
            stack.push(branch_node::read_cell(page, i).child);
        }
        if page.right_child().is_valid() {
            stack.push(page.right_child());
        }
    }
}

// =========================================================================
// Leaf pages: keys in sorted order within each leaf
// =========================================================================

#[test]
fn leaf_keys_sorted_within_page() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let mut rng = Rng::new(7777);
    for _ in 0..1500 {
        let key_len = 1 + rng.next_range(200) as usize;
        let key: Vec<u8> = (0..key_len).map(|_| rng.next_range(256) as u8).collect();
        tree.insert(&mut pages, &mut alloc, TxnId(1), &key, ValueType::Inline, b"v").unwrap();
    }

    let mut stack = vec![tree.root];
    while let Some(page_id) = stack.pop() {
        let page = pages.get(&page_id).unwrap();
        match page.page_type() {
            Some(PageType::Leaf) => {
                let num_cells = page.num_cells();
                for i in 1..num_cells {
                    let prev = leaf_node::read_cell(page, i - 1);
                    let curr = leaf_node::read_cell(page, i);
                    assert!(prev.key < curr.key,
                        "leaf key order violated at page {:?}, cells {}-{}", page_id, i-1, i);
                }
            }
            Some(PageType::Branch) => {
                for i in 0..page.num_cells() {
                    stack.push(branch_node::read_cell(page, i).child);
                }
                if page.right_child().is_valid() {
                    stack.push(page.right_child());
                }
            }
            _ => {}
        }
    }
}

// =========================================================================
// No duplicate page references in live tree
// =========================================================================

#[test]
fn no_duplicate_page_references() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let mut rng = Rng::new(54321);
    for i in 0..1000u32 {
        let key = format!("k{:05}", rng.next_range(800));
        let val = format!("v{i}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, val.as_bytes()).unwrap();
    }
    for _ in 0..300 {
        let key = format!("k{:05}", rng.next_range(800));
        tree.delete(&mut pages, &mut alloc, TxnId(1), key.as_bytes()).unwrap();
    }

    let mut seen = HashSet::new();
    let mut stack = vec![tree.root];
    while let Some(page_id) = stack.pop() {
        assert!(seen.insert(page_id), "duplicate page reference: {:?}", page_id);
        let page = pages.get(&page_id).unwrap();
        if page.page_type() == Some(PageType::Branch) {
            for i in 0..page.num_cells() {
                stack.push(branch_node::read_cell(page, i).child);
            }
            if page.right_child().is_valid() {
                stack.push(page.right_child());
            }
        }
    }
}

// =========================================================================
// Reverse sequential insert (left-edge stress)
// =========================================================================

#[test]
fn reverse_sequential_insert() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let count = 3000u32;
    for i in (0..count).rev() {
        let key = format!("{i:08}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }
    assert_eq!(tree.entry_count, count as u64);

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    for i in 0..count {
        assert!(cursor.is_valid());
        let entry = cursor.current(&pages).unwrap();
        assert_eq!(entry.key, format!("{i:08}").as_bytes());
        cursor.next(&pages).unwrap();
    }
    assert!(!cursor.is_valid());
}

// =========================================================================
// Interleaved insert pattern (even then odd)
// =========================================================================

#[test]
fn interleaved_insert_pattern() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let count = 2000u32;
    for i in (0..count).step_by(2) {
        let key = format!("{i:08}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"even").unwrap();
        expected.insert(key.into_bytes(), b"even".to_vec());
    }
    for i in (1..count).step_by(2) {
        let key = format!("{i:08}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"odd").unwrap();
        expected.insert(key.into_bytes(), b"odd".to_vec());
    }

    assert_eq!(tree.entry_count, expected.len() as u64);

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    for (ok, ov) in &expected {
        assert!(cursor.is_valid());
        let entry = cursor.current(&pages).unwrap();
        assert_eq!(&entry.key, ok);
        assert_eq!(&entry.value, ov);
        cursor.next(&pages).unwrap();
    }
    assert!(!cursor.is_valid());
}

// =========================================================================
// Empty value stress
// =========================================================================

#[test]
fn empty_value_stress() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..500u32 {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"").unwrap();
    }
    assert_eq!(tree.entry_count, 500);

    for i in 0..500u32 {
        let key = format!("{i:06}");
        let result = tree.search(&pages, key.as_bytes()).unwrap();
        assert_eq!(result, Some((ValueType::Inline, vec![])));
    }

    for i in (0..500u32).step_by(2) {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"updated").unwrap();
    }

    for i in 0..500u32 {
        let key = format!("{i:06}");
        let result = tree.search(&pages, key.as_bytes()).unwrap();
        if i % 2 == 0 {
            assert_eq!(result, Some((ValueType::Inline, b"updated".to_vec())));
        } else {
            assert_eq!(result, Some((ValueType::Inline, vec![])));
        }
    }
}

// =========================================================================
// Heavy randomized: 50K operations
// =========================================================================

#[test]
fn heavy_expected_50k_ops() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let mut rng = Rng::new(0xCAFE);
    let ops = 50_000;

    for i in 0..ops {
        let op = rng.next_range(10);
        let key = format!("k{:06}", rng.next_range(5000));
        let key_bytes = key.as_bytes().to_vec();

        if op < 5 {
            let val = format!("v{i}");
            let tree_new = tree.insert(
                &mut pages, &mut alloc, TxnId(1),
                &key_bytes, ValueType::Inline, val.as_bytes(),
            ).unwrap();
            let expected_existed = expected.insert(key_bytes, val.into_bytes()).is_some();
            assert_eq!(tree_new, !expected_existed, "insert mismatch at op {i}");
        } else if op < 8 {
            let tree_found = tree.delete(&mut pages, &mut alloc, TxnId(1), &key_bytes).unwrap();
            let expected_found = expected.remove(&key_bytes).is_some();
            assert_eq!(tree_found, expected_found, "delete mismatch at op {i}");
        } else {
            let tree_result = tree.search(&pages, &key_bytes).unwrap();
            let expected_result = expected.get(&key_bytes);
            match (&tree_result, expected_result) {
                (Some((_, tv)), Some(ov)) => assert_eq!(tv, ov, "value mismatch at op {i}"),
                (None, None) => {}
                _ => panic!("search mismatch at op {i}"),
            }
        }
        assert_eq!(tree.entry_count, expected.len() as u64, "count mismatch at op {i}");
    }

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    let mut count = 0u64;
    let mut prev_key: Option<Vec<u8>> = None;
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        if let Some(ref pk) = prev_key {
            assert!(entry.key > *pk, "cursor order violated");
        }
        let expected_val = expected.get(&entry.key);
        assert!(expected_val.is_some(), "cursor yielded key not in expected");
        assert_eq!(&entry.value, expected_val.unwrap());
        prev_key = Some(entry.key);
        count += 1;
        cursor.next(&pages).unwrap();
    }
    assert_eq!(count, expected.len() as u64);
}

// =========================================================================
// CoW across multiple transaction IDs
// =========================================================================

#[test]
fn cow_across_many_txn_ids() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in 0..100u32 {
        let key = format!("{i:04}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v1").unwrap();
    }
    let freed1 = alloc.commit();
    alloc.add_ready_to_use(freed1);

    let mut prev_root = tree.root;
    for txn in 2..=20u64 {
        for i in 0..100u32 {
            let key = format!("{i:04}");
            let val = format!("v{txn}");
            tree.insert(&mut pages, &mut alloc, TxnId(txn), key.as_bytes(), ValueType::Inline, val.as_bytes()).unwrap();
        }
        assert_ne!(tree.root, prev_root,
            "CoW must produce new root in txn {txn} (old={:?}, new={:?})", prev_root, tree.root);
        prev_root = tree.root;
        let freed = alloc.commit();
        alloc.add_ready_to_use(freed);
    }

    for i in 0..100u32 {
        let key = format!("{i:04}");
        let result = tree.search(&pages, key.as_bytes()).unwrap();
        assert_eq!(result, Some((ValueType::Inline, b"v20".to_vec())));
    }
}

// =========================================================================
// Delete all in random order, verify tree invariants at each step
// =========================================================================

#[test]
fn delete_all_random_order_verify_invariants() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let count = 300u32;
    let mut keys: Vec<String> = (0..count).map(|i| format!("{i:06}")).collect();
    for k in &keys {
        tree.insert(&mut pages, &mut alloc, TxnId(1), k.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    let mut rng = Rng::new(12345);
    for i in (1..keys.len()).rev() {
        let j = rng.next_range((i + 1) as u32) as usize;
        keys.swap(i, j);
    }

    let mut remaining: BTreeMap<Vec<u8>, ()> = (0..count)
        .map(|i| (format!("{i:06}").into_bytes(), ()))
        .collect();

    for (idx, k) in keys.iter().enumerate() {
        tree.delete(&mut pages, &mut alloc, TxnId(1), k.as_bytes()).unwrap();
        remaining.remove(k.as_bytes());
        assert_eq!(tree.entry_count, remaining.len() as u64,
            "entry count mismatch after deleting {idx}th key");

        if idx % 30 == 0 && !remaining.is_empty() {
            let mut cursor = Cursor::first(&pages, tree.root).unwrap();
            let expected: Vec<_> = remaining.keys().collect();
            for ek in &expected {
                assert!(cursor.is_valid(), "cursor ended early at step {idx}");
                let entry = cursor.current(&pages).unwrap();
                assert_eq!(&entry.key, *ek);
                cursor.next(&pages).unwrap();
            }
            assert!(!cursor.is_valid());
        }
    }
    assert_eq!(tree.entry_count, 0);
}

// =========================================================================
// Insert-heavy then delete-all then reinsert (exercises merge to empty)
// =========================================================================

#[test]
fn insert_delete_all_reinsert_3_cycles() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for cycle in 0..3u32 {
        let count = 500;
        for i in 0..count {
            let key = format!("c{cycle}k{i:06}");
            tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
        }
        assert_eq!(tree.entry_count, count);

        for i in 0..count {
            let key = format!("c{cycle}k{i:06}");
            tree.delete(&mut pages, &mut alloc, TxnId(1), key.as_bytes()).unwrap();
        }
        assert_eq!(tree.entry_count, 0);

        let cursor = Cursor::first(&pages, tree.root).unwrap();
        assert!(!cursor.is_valid());
    }
}

// =========================================================================
// Verify entry_count is accurate through all operations
// =========================================================================

#[test]
fn entry_count_always_accurate() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    let mut expected: BTreeMap<Vec<u8>, ()> = BTreeMap::new();

    let mut rng = Rng::new(77777);
    for i in 0..5000u32 {
        let key = format!("k{:04}", rng.next_range(1000));
        let key_bytes = key.as_bytes().to_vec();
        if rng.next_range(3) < 2 {
            tree.insert(&mut pages, &mut alloc, TxnId(1), &key_bytes, ValueType::Inline, b"v").unwrap();
            expected.insert(key_bytes, ());
        } else {
            tree.delete(&mut pages, &mut alloc, TxnId(1), &key_bytes).unwrap();
            expected.remove(&key_bytes);
        }
        assert_eq!(tree.entry_count, expected.len() as u64,
            "entry_count mismatch at op {i}, tree={}, expected={}", tree.entry_count, expected.len());
    }
}

// =========================================================================
// Cursor on tree with exactly 2 entries
// =========================================================================

#[test]
fn cursor_two_entries() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    tree.insert(&mut pages, &mut alloc, TxnId(1), b"a", ValueType::Inline, b"1").unwrap();
    tree.insert(&mut pages, &mut alloc, TxnId(1), b"z", ValueType::Inline, b"2").unwrap();

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    assert_eq!(cursor.current(&pages).unwrap().key, b"a");
    cursor.next(&pages).unwrap();
    assert_eq!(cursor.current(&pages).unwrap().key, b"z");
    cursor.next(&pages).unwrap();
    assert!(!cursor.is_valid());

    let mut cursor = Cursor::last(&pages, tree.root).unwrap();
    assert_eq!(cursor.current(&pages).unwrap().key, b"z");
    cursor.prev(&pages).unwrap();
    assert_eq!(cursor.current(&pages).unwrap().key, b"a");
    cursor.prev(&pages).unwrap();
    assert!(!cursor.is_valid());

    let cursor = Cursor::seek(&pages, tree.root, b"m").unwrap();
    assert!(cursor.is_valid());
    assert_eq!(cursor.current(&pages).unwrap().key, b"z");
}

// =========================================================================
// Update value to different size (shrink/grow)
// =========================================================================

#[test]
fn update_value_size_changes() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    tree.insert(&mut pages, &mut alloc, TxnId(1), b"key", ValueType::Inline, b"tiny").unwrap();

    let big = vec![0xAA; 1800];
    tree.insert(&mut pages, &mut alloc, TxnId(1), b"key", ValueType::Inline, &big).unwrap();
    let result = tree.search(&pages, b"key").unwrap().unwrap();
    assert_eq!(result.1.len(), 1800);

    tree.insert(&mut pages, &mut alloc, TxnId(1), b"key", ValueType::Inline, b"small-again").unwrap();
    let result = tree.search(&pages, b"key").unwrap().unwrap();
    assert_eq!(result.1, b"small-again");

    tree.insert(&mut pages, &mut alloc, TxnId(1), b"key", ValueType::Inline, b"").unwrap();
    let result = tree.search(&pages, b"key").unwrap().unwrap();
    assert_eq!(result.1, b"");

    let max_val = vec![0xFF; MAX_INLINE_VALUE_SIZE];
    tree.insert(&mut pages, &mut alloc, TxnId(1), b"key", ValueType::Inline, &max_val).unwrap();
    let result = tree.search(&pages, b"key").unwrap().unwrap();
    assert_eq!(result.1.len(), MAX_INLINE_VALUE_SIZE);
}

// =========================================================================
// Monotonically increasing keys with periodic bulk deletes
// =========================================================================

#[test]
fn monotonic_insert_with_periodic_bulk_delete() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let mut counter = 0u64;
    for _round in 0..10 {
        for _ in 0..200 {
            let key = format!("{counter:010}");
            tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
            expected.insert(key.into_bytes(), b"v".to_vec());
            counter += 1;
        }

        let to_delete: Vec<Vec<u8>> = expected.keys().take(100).cloned().collect();
        for k in &to_delete {
            tree.delete(&mut pages, &mut alloc, TxnId(1), k).unwrap();
            expected.remove(k);
        }

        assert_eq!(tree.entry_count, expected.len() as u64);
    }

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    for (ok, _) in &expected {
        assert!(cursor.is_valid());
        let entry = cursor.current(&pages).unwrap();
        assert_eq!(&entry.key, ok);
        cursor.next(&pages).unwrap();
    }
    assert!(!cursor.is_valid());
}

// =========================================================================
// Stress: many keys with identical prefixes (exercises binary search)
// =========================================================================

#[test]
fn identical_prefix_keys() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let prefix = vec![0x42; 500];
    for i in 0..300u32 {
        let mut key = prefix.clone();
        key.extend_from_slice(&i.to_be_bytes());
        tree.insert(&mut pages, &mut alloc, TxnId(1), &key, ValueType::Inline, b"v").unwrap();
    }
    assert_eq!(tree.entry_count, 300);

    for i in 0..300u32 {
        let mut key = prefix.clone();
        key.extend_from_slice(&i.to_be_bytes());
        assert!(tree.search(&pages, &key).unwrap().is_some(), "key with suffix {i} should exist");
    }

    for i in (0..300u32).step_by(3) {
        let mut key = prefix.clone();
        key.extend_from_slice(&i.to_be_bytes());
        tree.delete(&mut pages, &mut alloc, TxnId(1), &key).unwrap();
    }
    assert_eq!(tree.entry_count, 200);
}

// =========================================================================
// Verify search returns None for keys between existing keys
// =========================================================================

#[test]
fn search_nonexistent_keys_between_existing() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    for i in (0..1000).step_by(10) {
        let key = format!("{i:06}");
        tree.insert(&mut pages, &mut alloc, TxnId(1), key.as_bytes(), ValueType::Inline, b"v").unwrap();
    }

    for i in 0..1000u32 {
        if i % 10 != 0 {
            let key = format!("{i:06}");
            let result = tree.search(&pages, key.as_bytes()).unwrap();
            assert!(result.is_none(), "key {key} should not exist");
        }
    }
}

// =========================================================================
// Split then merge stress (grow tree deep, then shrink it back)
// =========================================================================

#[test]
fn grow_deep_then_shrink() {
    let mut pages: HashMap<PageId, Page> = HashMap::new();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let count = 5000u32;
    let keys: Vec<String> = (0..count).map(|i| format!("{i:08}")).collect();

    for k in &keys {
        tree.insert(&mut pages, &mut alloc, TxnId(1), k.as_bytes(), ValueType::Inline, b"v").unwrap();
    }
    let peak_depth = tree.depth;
    assert!(peak_depth >= 2, "5000 entries should produce depth >= 2");

    for k in &keys {
        tree.delete(&mut pages, &mut alloc, TxnId(1), k.as_bytes()).unwrap();
    }
    assert_eq!(tree.entry_count, 0);
    assert!(tree.depth <= peak_depth, "depth should not grow after deletions");

    for k in &keys {
        tree.insert(&mut pages, &mut alloc, TxnId(1), k.as_bytes(), ValueType::Inline, b"v2").unwrap();
    }
    assert_eq!(tree.entry_count, count as u64);
}
