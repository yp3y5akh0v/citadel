use super::*;
use crate::allocator::PageAllocator;
use crate::btree::BTree;
use citadel_core::types::TxnId;

fn build_tree(keys: &[&[u8]]) -> (rustc_hash::FxHashMap<PageId, Page>, BTree) {
    let mut pages = rustc_hash::FxHashMap::default();
    let mut alloc = PageAllocator::new(0);
    let mut tree = BTree::new(&mut pages, &mut alloc, TxnId(1));
    for k in keys {
        tree.insert(&mut pages, &mut alloc, TxnId(1), k, ValueType::Inline, k)
            .unwrap();
    }
    (pages, tree)
}

#[test]
fn cursor_forward_iteration() {
    let (pages, tree) = build_tree(&[b"c", b"a", b"e", b"b", b"d"]);
    let mut cursor = Cursor::first(&pages, tree.root).unwrap();

    let mut collected = Vec::new();
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        collected.push(entry.key.clone());
        cursor.next(&pages).unwrap();
    }

    assert_eq!(collected, vec![b"a", b"b", b"c", b"d", b"e"]);
}

#[test]
fn cursor_backward_iteration() {
    let (pages, tree) = build_tree(&[b"c", b"a", b"e", b"b", b"d"]);
    let mut cursor = Cursor::last(&pages, tree.root).unwrap();

    let mut collected = Vec::new();
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        collected.push(entry.key.clone());
        cursor.prev(&pages).unwrap();
    }

    assert_eq!(collected, vec![b"e", b"d", b"c", b"b", b"a"]);
}

#[test]
fn cursor_seek() {
    let (pages, tree) = build_tree(&[b"b", b"d", b"f", b"h"]);
    let cursor = Cursor::seek(&pages, tree.root, b"c").unwrap();
    assert!(cursor.is_valid());
    let entry = cursor.current(&pages).unwrap();
    assert_eq!(entry.key, b"d");
}

#[test]
fn cursor_seek_exact() {
    let (pages, tree) = build_tree(&[b"b", b"d", b"f"]);
    let cursor = Cursor::seek(&pages, tree.root, b"d").unwrap();
    assert!(cursor.is_valid());
    let entry = cursor.current(&pages).unwrap();
    assert_eq!(entry.key, b"d");
}

#[test]
fn cursor_seek_past_end() {
    let (pages, tree) = build_tree(&[b"a", b"b", b"c"]);
    let cursor = Cursor::seek(&pages, tree.root, b"z").unwrap();
    assert!(!cursor.is_valid());
}

#[test]
fn cursor_empty_tree() {
    let mut pages = rustc_hash::FxHashMap::default();
    let mut alloc = PageAllocator::new(0);
    let tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

    let cursor = Cursor::first(&pages, tree.root).unwrap();
    assert!(!cursor.is_valid());
}

/// PageLoader backed by a pre-built FxHashMap — tracks unique pages touched.
struct TrackingLoader {
    pages: rustc_hash::FxHashMap<PageId, Page>,
    touched: std::collections::HashSet<PageId>,
}

impl TrackingLoader {
    fn new(pages: rustc_hash::FxHashMap<PageId, Page>) -> Self {
        Self {
            pages,
            touched: std::collections::HashSet::new(),
        }
    }
    fn unique_pages_touched(&self) -> usize {
        self.touched.len()
    }
}

impl PageMap for TrackingLoader {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.pages.get(id)
    }
}

impl PageLoader for TrackingLoader {
    fn ensure_loaded(&mut self, id: PageId) -> citadel_core::Result<()> {
        if self.pages.contains_key(&id) {
            self.touched.insert(id);
            Ok(())
        } else {
            Err(citadel_core::Error::PageOutOfBounds(id))
        }
    }
}

#[test]
fn lazy_cursor_forward() {
    let keys: Vec<Vec<u8>> = (0..2000u32)
        .map(|i| format!("{i:06}").into_bytes())
        .collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let (pages, tree) = build_tree(&key_refs);

    let mut loader = TrackingLoader::new(pages);
    let mut cursor = Cursor::seek_lazy(&mut loader, tree.root, b"").unwrap();
    let mut count = 0u32;
    while cursor.is_valid() {
        let entry = cursor.current_ref_lazy(&mut loader);
        assert!(entry.is_some());
        count += 1;
        cursor.next_lazy(&mut loader).unwrap();
    }
    assert_eq!(count, 2000);
}

#[test]
fn lazy_cursor_range_loads_fewer_pages() {
    let keys: Vec<Vec<u8>> = (0..2000u32)
        .map(|i| format!("{i:06}").into_bytes())
        .collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let (pages, tree) = build_tree(&key_refs);
    let total_pages = pages.len();

    let mut loader = TrackingLoader::new(pages);
    let mut cursor = Cursor::seek_lazy(&mut loader, tree.root, b"001000").unwrap();
    let mut count = 0u32;
    while cursor.is_valid() {
        if let Some(entry) = cursor.current_ref_lazy(&mut loader) {
            if entry.key > b"001009".as_slice() {
                break;
            }
            count += 1;
        }
        cursor.next_lazy(&mut loader).unwrap();
    }
    assert_eq!(count, 10);
    let touched = loader.unique_pages_touched();
    assert!(
        touched < total_pages,
        "lazy touched {} unique pages but tree has {} total",
        touched,
        total_pages,
    );
}

#[test]
fn cursor_large_tree_forward() {
    let keys: Vec<Vec<u8>> = (0..2000u32)
        .map(|i| format!("{i:06}").into_bytes())
        .collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let (pages, tree) = build_tree(&key_refs);

    let mut cursor = Cursor::first(&pages, tree.root).unwrap();
    let mut count = 0u32;
    let mut prev_key: Option<Vec<u8>> = None;
    while cursor.is_valid() {
        let entry = cursor.current(&pages).unwrap();
        if let Some(ref pk) = prev_key {
            assert!(entry.key > *pk, "keys should be in sorted order");
        }
        prev_key = Some(entry.key);
        count += 1;
        cursor.next(&pages).unwrap();
    }
    assert_eq!(count, 2000);
}
