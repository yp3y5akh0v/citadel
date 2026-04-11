//! Cursor for B+ tree range iteration.
//!
//! Uses a saved root-to-leaf path stack (no sibling pointers needed).
//! Supports forward and backward iteration across leaf boundaries.

use std::collections::HashMap;
use std::sync::Arc;

use citadel_core::types::{PageId, PageType, ValueType};
use citadel_core::{Error, Result};
use citadel_page::leaf_node::LeafCell;
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};

pub trait PageMap {
    fn get_page(&self, id: &PageId) -> Option<&Page>;
}

impl PageMap for HashMap<PageId, Page> {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.get(id)
    }
}

impl PageMap for HashMap<PageId, Arc<Page>> {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.get(id).map(|a| a.as_ref())
    }
}

pub struct CursorEntry {
    pub key: Vec<u8>,
    pub val_type: ValueType,
    pub value: Vec<u8>,
}

/// Cursor position within the B+ tree.
/// Stores the path from root to the current leaf.
pub struct Cursor {
    /// Stack of (page_id, child_index) from root to current leaf's parent.
    path: Vec<(PageId, usize)>,
    /// Current leaf page ID.
    leaf: PageId,
    /// Current cell index within the leaf.
    cell_idx: u16,
    /// Whether the cursor is positioned at a valid entry.
    valid: bool,
}

impl Cursor {
    /// Position the cursor at the first key >= `key` (seek).
    /// If `key` is empty, positions at the first entry in the tree.
    pub fn seek(pages: &impl PageMap, root: PageId, key: &[u8]) -> Result<Self> {
        let mut path = Vec::new();
        let mut current = root;

        // Walk to the leaf
        loop {
            let page = pages
                .get_page(&current)
                .ok_or(Error::PageOutOfBounds(current))?;
            match page.page_type() {
                Some(PageType::Leaf) => break,
                Some(PageType::Branch) => {
                    let child_idx = branch_node::search_child_index(page, key);
                    let child = branch_node::get_child(page, child_idx);
                    path.push((current, child_idx));
                    current = child;
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }

        // Find the cell index in the leaf
        let page = pages.get_page(&current).unwrap();
        let cell_idx = match leaf_node::search(page, key) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        let valid = cell_idx < page.num_cells();

        let mut cursor = Self {
            path,
            leaf: current,
            cell_idx,
            valid,
        };

        // If we landed past the end of this leaf, advance to next leaf
        if !valid && page.num_cells() > 0 {
            cursor.advance_leaf(pages)?;
        } else if page.num_cells() == 0 {
            cursor.valid = false;
        }

        Ok(cursor)
    }

    /// Position the cursor at the first entry in the tree.
    pub fn first(pages: &impl PageMap, root: PageId) -> Result<Self> {
        let mut path = Vec::new();
        let mut current = root;

        // Walk to the leftmost leaf
        loop {
            let page = pages
                .get_page(&current)
                .ok_or(Error::PageOutOfBounds(current))?;
            match page.page_type() {
                Some(PageType::Leaf) => break,
                Some(PageType::Branch) => {
                    let child = branch_node::get_child(page, 0);
                    path.push((current, 0));
                    current = child;
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }

        let page = pages.get_page(&current).unwrap();
        let valid = page.num_cells() > 0;

        Ok(Self {
            path,
            leaf: current,
            cell_idx: 0,
            valid,
        })
    }

    /// Position the cursor at the last entry in the tree.
    pub fn last(pages: &impl PageMap, root: PageId) -> Result<Self> {
        let mut path = Vec::new();
        let mut current = root;

        // Walk to the rightmost leaf
        loop {
            let page = pages
                .get_page(&current)
                .ok_or(Error::PageOutOfBounds(current))?;
            match page.page_type() {
                Some(PageType::Leaf) => break,
                Some(PageType::Branch) => {
                    let n = page.num_cells() as usize;
                    let child = page.right_child();
                    path.push((current, n));
                    current = child;
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }

        let page = pages.get_page(&current).unwrap();
        let n = page.num_cells();
        let valid = n > 0;
        let cell_idx = if valid { n - 1 } else { 0 };

        Ok(Self {
            path,
            leaf: current,
            cell_idx,
            valid,
        })
    }

    /// Whether the cursor is at a valid position.
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn current(&self, pages: &impl PageMap) -> Option<CursorEntry> {
        if !self.valid {
            return None;
        }
        let page = pages.get_page(&self.leaf)?;
        let cell = leaf_node::read_cell(page, self.cell_idx);
        Some(CursorEntry {
            key: cell.key.to_vec(),
            val_type: cell.val_type,
            value: cell.value.to_vec(),
        })
    }

    pub fn current_ref<'a, P: PageMap>(&self, pages: &'a P) -> Option<LeafCell<'a>> {
        if !self.valid {
            return None;
        }
        let page = pages.get_page(&self.leaf)?;
        Some(leaf_node::read_cell(page, self.cell_idx))
    }

    /// Move the cursor to the next entry (forward).
    pub fn next(&mut self, pages: &impl PageMap) -> Result<bool> {
        if !self.valid {
            return Ok(false);
        }

        let page = pages
            .get_page(&self.leaf)
            .ok_or(Error::PageOutOfBounds(self.leaf))?;

        if self.cell_idx + 1 < page.num_cells() {
            self.cell_idx += 1;
            return Ok(true);
        }

        // Need to move to the next leaf
        self.advance_leaf(pages)
    }

    /// Move the cursor to the previous entry (backward).
    pub fn prev(&mut self, pages: &impl PageMap) -> Result<bool> {
        if !self.valid {
            return Ok(false);
        }

        if self.cell_idx > 0 {
            self.cell_idx -= 1;
            return Ok(true);
        }

        // Need to move to the previous leaf
        self.retreat_leaf(pages)
    }

    /// Advance to the first cell of the next leaf.
    fn advance_leaf(&mut self, pages: &impl PageMap) -> Result<bool> {
        // Walk up the path to find a parent where we can go right
        while let Some((parent_id, child_idx)) = self.path.pop() {
            let parent = pages
                .get_page(&parent_id)
                .ok_or(Error::PageOutOfBounds(parent_id))?;
            let n = parent.num_cells() as usize;

            if child_idx < n {
                // There's a sibling to the right: child_idx + 1
                let next_child_idx = child_idx + 1;
                let next_child = branch_node::get_child(parent, next_child_idx);
                self.path.push((parent_id, next_child_idx));

                // Walk down to the leftmost leaf of this subtree
                let mut current = next_child;
                loop {
                    let page = pages
                        .get_page(&current)
                        .ok_or(Error::PageOutOfBounds(current))?;
                    match page.page_type() {
                        Some(PageType::Leaf) => {
                            self.leaf = current;
                            self.cell_idx = 0;
                            self.valid = page.num_cells() > 0;
                            return Ok(self.valid);
                        }
                        Some(PageType::Branch) => {
                            let child = branch_node::get_child(page, 0);
                            self.path.push((current, 0));
                            current = child;
                        }
                        _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
                    }
                }
            }
            // child_idx == num_cells (rightmost child) - keep going up
        }

        // No more siblings - we've exhausted the tree
        self.valid = false;
        Ok(false)
    }

    /// Retreat to the last cell of the previous leaf.
    fn retreat_leaf(&mut self, pages: &impl PageMap) -> Result<bool> {
        // Walk up the path to find a parent where we can go left
        while let Some((parent_id, child_idx)) = self.path.pop() {
            if child_idx > 0 {
                // There's a sibling to the left: child_idx - 1
                let prev_child_idx = child_idx - 1;
                let parent = pages
                    .get_page(&parent_id)
                    .ok_or(Error::PageOutOfBounds(parent_id))?;
                let prev_child = branch_node::get_child(parent, prev_child_idx);
                self.path.push((parent_id, prev_child_idx));

                // Walk down to the rightmost leaf of this subtree
                let mut current = prev_child;
                loop {
                    let page = pages
                        .get_page(&current)
                        .ok_or(Error::PageOutOfBounds(current))?;
                    match page.page_type() {
                        Some(PageType::Leaf) => {
                            self.leaf = current;
                            let n = page.num_cells();
                            if n > 0 {
                                self.cell_idx = n - 1;
                                self.valid = true;
                            } else {
                                self.valid = false;
                            }
                            return Ok(self.valid);
                        }
                        Some(PageType::Branch) => {
                            let n = page.num_cells() as usize;
                            let child = page.right_child();
                            self.path.push((current, n));
                            current = child;
                        }
                        _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
                    }
                }
            }
            // child_idx == 0 (leftmost child) - keep going up
        }

        // No more siblings - we've exhausted the tree
        self.valid = false;
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::PageAllocator;
    use crate::btree::BTree;
    use citadel_core::types::TxnId;

    fn build_tree(keys: &[&[u8]]) -> (HashMap<PageId, Page>, BTree) {
        let mut pages = HashMap::new();
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
        // Seek to "c" - should land on "d" (first key >= "c")
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
        let mut pages = HashMap::new();
        let mut alloc = PageAllocator::new(0);
        let tree = BTree::new(&mut pages, &mut alloc, TxnId(1));

        let cursor = Cursor::first(&pages, tree.root).unwrap();
        assert!(!cursor.is_valid());
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
}
