//! CoW B+ tree engine.
//!
//! All mutations use Copy-on-Write: the old page is cloned to a new page ID,
//! modified, and ancestors are updated to point to the new page. Old pages
//! are freed via the allocator's pending-free list.
//!
//! The tree uses `HashMap<PageId, Page>` as the in-memory page store.

use crate::allocator::PageAllocator;
use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result};
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};
use std::collections::HashMap;

/// B+ tree metadata. Lightweight struct — pages are stored externally.
#[derive(Clone)]
pub struct BTree {
    pub root: PageId,
    pub depth: u16,
    pub entry_count: u64,
    last_insert: Option<(Vec<(PageId, usize)>, PageId)>,
}

impl BTree {
    /// Create a new empty B+ tree with a single leaf root.
    pub fn new(
        pages: &mut HashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
    ) -> Self {
        let root_id = alloc.allocate();
        let root = Page::new(root_id, PageType::Leaf, txn_id);
        pages.insert(root_id, root);
        Self {
            root: root_id,
            depth: 1,
            entry_count: 0,
            last_insert: None,
        }
    }

    /// Create a BTree from existing metadata (e.g., loaded from commit slot).
    pub fn from_existing(root: PageId, depth: u16, entry_count: u64) -> Self {
        Self {
            root,
            depth,
            entry_count,
            last_insert: None,
        }
    }

    /// Search for a key. Returns `Some((val_type, value))` if found, `None` otherwise.
    pub fn search(
        &self,
        pages: &HashMap<PageId, Page>,
        key: &[u8],
    ) -> Result<Option<(ValueType, Vec<u8>)>> {
        let mut current = self.root;
        loop {
            let page = pages.get(&current).ok_or(Error::PageOutOfBounds(current))?;
            match page.page_type() {
                Some(PageType::Leaf) => {
                    return match leaf_node::search(page, key) {
                        Ok(idx) => {
                            let cell = leaf_node::read_cell(page, idx);
                            Ok(Some((cell.val_type, cell.value.to_vec())))
                        }
                        Err(_) => Ok(None),
                    };
                }
                Some(PageType::Branch) => {
                    let idx = branch_node::search_child_index(page, key);
                    current = branch_node::get_child(page, idx);
                }
                _ => {
                    return Err(Error::InvalidPageType(page.page_type_raw(), current));
                }
            }
        }
    }

    pub fn lil_would_hit(&self, pages: &HashMap<PageId, Page>, key: &[u8]) -> bool {
        if let Some((_, cached_leaf)) = &self.last_insert {
            if let Some(page) = pages.get(cached_leaf) {
                let n = page.num_cells();
                return n > 0 && key > leaf_node::read_cell(page, n - 1).key;
            }
        }
        false
    }

    /// Insert a key-value pair. Returns `true` if a new entry was added,
    /// `false` if an existing key was updated.
    pub fn insert(
        &mut self,
        pages: &mut HashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
        val_type: ValueType,
        value: &[u8],
    ) -> Result<bool> {
        // LIL cache: skip walk_to_leaf for sequential appends to the rightmost leaf.
        if let Some((cached_path, cached_leaf)) = self.last_insert.take() {
            let hit = {
                let page = pages.get(&cached_leaf).ok_or(Error::PageOutOfBounds(cached_leaf))?;
                let n = page.num_cells();
                n > 0 && key > leaf_node::read_cell(page, n - 1).key
            };
            if hit {
                let cow_id = cow_page(pages, alloc, cached_leaf, txn_id);
                let ok = {
                    let page = pages.get_mut(&cow_id).unwrap();
                    leaf_node::insert_direct(page, key, val_type, value)
                };
                if ok {
                    if cow_id != cached_leaf {
                        self.root = propagate_cow_up(pages, alloc, txn_id, &cached_path, cow_id);
                    }
                    self.entry_count += 1;
                    self.last_insert = Some((cached_path, cow_id));
                    return Ok(true);
                }
                let (sep_key, right_id) =
                    split_leaf_with_insert(pages, alloc, txn_id, cow_id, key, val_type, value);
                self.root = propagate_split_up(
                    pages, alloc, txn_id, &cached_path, cow_id, &sep_key, right_id, &mut self.depth,
                );
                self.last_insert = None;
                self.entry_count += 1;
                return Ok(true);
            }
        }

        let (path, leaf_id) = self.walk_to_leaf(pages, key)?;

        let key_exists = {
            let page = pages.get(&leaf_id).unwrap();
            leaf_node::search(page, key).is_ok()
        };

        let new_leaf_id = cow_page(pages, alloc, leaf_id, txn_id);

        let leaf_ok = {
            let page = pages.get_mut(&new_leaf_id).unwrap();
            leaf_node::insert_direct(page, key, val_type, value)
        };

        if leaf_ok {
            let mut child = new_leaf_id;
            let mut is_rightmost = true;
            let mut new_path = path;
            for i in (0..new_path.len()).rev() {
                let (ancestor_id, child_idx) = new_path[i];
                let new_ancestor = cow_page(pages, alloc, ancestor_id, txn_id);
                let page = pages.get_mut(&new_ancestor).unwrap();
                update_branch_child(page, child_idx, child);
                if child_idx != page.num_cells() as usize {
                    is_rightmost = false;
                }
                new_path[i] = (new_ancestor, child_idx);
                child = new_ancestor;
            }
            self.root = child;

            if is_rightmost {
                self.last_insert = Some((new_path, new_leaf_id));
            }

            if !key_exists {
                self.entry_count += 1;
            }
            return Ok(!key_exists);
        }

        self.last_insert = None;
        let (sep_key, right_id) =
            split_leaf_with_insert(pages, alloc, txn_id, new_leaf_id, key, val_type, value);
        self.root = propagate_split_up(
            pages, alloc, txn_id, &path, new_leaf_id, &sep_key, right_id, &mut self.depth,
        );

        if !key_exists {
            self.entry_count += 1;
        }
        Ok(!key_exists)
    }

    /// Delete a key. Returns `true` if the key was found and deleted.
    pub fn delete(
        &mut self,
        pages: &mut HashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
    ) -> Result<bool> {
        self.last_insert = None;
        let (path, leaf_id) = self.walk_to_leaf(pages, key)?;

        // Check if key exists
        let found = {
            let page = pages.get(&leaf_id).unwrap();
            leaf_node::search(page, key).is_ok()
        };
        if !found {
            return Ok(false);
        }

        // CoW the leaf and delete
        let new_leaf_id = cow_page(pages, alloc, leaf_id, txn_id);
        {
            let page = pages.get_mut(&new_leaf_id).unwrap();
            leaf_node::delete(page, key);
        }

        // Check if leaf became empty
        let leaf_empty = pages.get(&new_leaf_id).unwrap().num_cells() == 0;

        if !leaf_empty || path.is_empty() {
            // Leaf is non-empty, or it's the root (root can be empty)
            self.root = propagate_cow_up(pages, alloc, txn_id, &path, new_leaf_id);
            self.entry_count -= 1;
            return Ok(true);
        }

        // Empty leaf — remove from tree
        alloc.free(new_leaf_id);
        pages.remove(&new_leaf_id);

        // Walk up, handling the removal
        self.root = propagate_remove_up(pages, alloc, txn_id, &path, &mut self.depth);
        self.entry_count -= 1;
        Ok(true)
    }

    /// Walk from root to the leaf that should contain `key`.
    /// Returns (path, leaf_page_id) where path is Vec<(ancestor_id, child_idx)>.
    fn walk_to_leaf(
        &self,
        pages: &HashMap<PageId, Page>,
        key: &[u8],
    ) -> Result<(Vec<(PageId, usize)>, PageId)> {
        let mut path = Vec::with_capacity(self.depth as usize);
        let mut current = self.root;
        loop {
            let page = pages.get(&current).ok_or(Error::PageOutOfBounds(current))?;
            match page.page_type() {
                Some(PageType::Leaf) => return Ok((path, current)),
                Some(PageType::Branch) => {
                    let child_idx = branch_node::search_child_index(page, key);
                    let child = branch_node::get_child(page, child_idx);
                    path.push((current, child_idx));
                    current = child;
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }
    }
}

/// Copy-on-Write: clone a page to a new page ID, free the old one.
/// If the page already belongs to this transaction, return it as-is.
fn cow_page(
    pages: &mut HashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    old_id: PageId,
    txn_id: TxnId,
) -> PageId {
    if pages.get(&old_id).unwrap().txn_id() == txn_id {
        return old_id;
    }
    let new_id = alloc.allocate();
    let mut new_page = pages.get(&old_id).unwrap().clone();
    new_page.set_page_id(new_id);
    new_page.set_txn_id(txn_id);
    pages.insert(new_id, new_page);
    alloc.free(old_id);
    new_id
}

/// Update a branch's child pointer at `child_idx` to point to `new_child`.
fn update_branch_child(page: &mut Page, child_idx: usize, new_child: PageId) {
    let n = page.num_cells() as usize;
    if child_idx < n {
        let offset = page.cell_offset(child_idx as u16) as usize;
        page.data[offset..offset + 4].copy_from_slice(&new_child.as_u32().to_le_bytes());
    } else {
        page.set_right_child(new_child);
    }
}

/// Propagate CoW up through ancestors (no split, just update child pointers).
fn propagate_cow_up(
    pages: &mut HashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    path: &[(PageId, usize)],
    mut new_child: PageId,
) -> PageId {
    for &(ancestor_id, child_idx) in path.iter().rev() {
        let new_ancestor = cow_page(pages, alloc, ancestor_id, txn_id);
        let page = pages.get_mut(&new_ancestor).unwrap();
        update_branch_child(page, child_idx, new_child);
        new_child = new_ancestor;
    }
    new_child
}

/// Split a full leaf and insert the new key-value pair.
/// Returns (separator_key, right_page_id). The left page is `leaf_id` (rebuilt in place).
fn split_leaf_with_insert(
    pages: &mut HashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    leaf_id: PageId,
    key: &[u8],
    val_type: ValueType,
    value: &[u8],
) -> (Vec<u8>, PageId) {
    // Collect all existing cells + the new cell, sorted
    let mut cells: Vec<(Vec<u8>, Vec<u8>)> = {
        let page = pages.get(&leaf_id).unwrap();
        let n = page.num_cells() as usize;
        (0..n)
            .map(|i| {
                let cell = leaf_node::read_cell(page, i as u16);
                let raw = leaf_node::read_cell_bytes(page, i as u16);
                (cell.key.to_vec(), raw)
            })
            .collect()
    };

    // Insert or update the new cell in sorted position
    let new_raw = leaf_node::build_cell(key, val_type, value);
    match cells.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
        Ok(idx) => cells[idx] = (key.to_vec(), new_raw),
        Err(idx) => cells.insert(idx, (key.to_vec(), new_raw)),
    }

    let total = cells.len();

    // Size-aware split: ensure both halves fit within USABLE_SIZE.
    // Simple midpoint-by-count fails when cell sizes vary significantly.
    let usable = citadel_core::constants::USABLE_SIZE;
    let mut cum: Vec<usize> = Vec::with_capacity(total + 1);
    cum.push(0);
    for (_, raw) in &cells {
        cum.push(cum.last().unwrap() + raw.len());
    }
    let left_fits = |sp: usize| cum[sp] + sp * 2 <= usable;
    let right_fits = |sp: usize| (cum[total] - cum[sp]) + (total - sp) * 2 <= usable;

    let mut split_point = total / 2;
    if !left_fits(split_point) || !right_fits(split_point) {
        split_point = 1;
        for sp in 1..total {
            if left_fits(sp) && right_fits(sp) {
                split_point = sp;
                if sp >= total / 2 {
                    break;
                }
            }
        }
    }

    let sep_key = cells[split_point].0.clone();

    // Rebuild left page with cells [0..split_point]
    {
        let left_refs: Vec<&[u8]> = cells[..split_point]
            .iter()
            .map(|(_, raw)| raw.as_slice())
            .collect();
        let page = pages.get_mut(&leaf_id).unwrap();
        page.rebuild_cells(&left_refs);
    }

    // Create right page with cells [split_point..total]
    let right_id = alloc.allocate();
    {
        let mut right_page = Page::new(right_id, PageType::Leaf, txn_id);
        let right_refs: Vec<&[u8]> = cells[split_point..]
            .iter()
            .map(|(_, raw)| raw.as_slice())
            .collect();
        right_page.rebuild_cells(&right_refs);
        pages.insert(right_id, right_page);
    }

    (sep_key, right_id)
}

/// Propagate a split upward through the ancestor chain.
/// Returns the new root page ID.
#[allow(clippy::too_many_arguments)]
fn propagate_split_up(
    pages: &mut HashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    path: &[(PageId, usize)],
    mut left_child: PageId,
    initial_sep: &[u8],
    mut right_child: PageId,
    depth: &mut u16,
) -> PageId {
    let mut sep_key = initial_sep.to_vec();
    let mut pending_split = true;

    for &(ancestor_id, child_idx) in path.iter().rev() {
        let new_ancestor = cow_page(pages, alloc, ancestor_id, txn_id);

        if pending_split {
            let ok = {
                let page = pages.get_mut(&new_ancestor).unwrap();
                branch_node::insert_separator(page, child_idx, left_child, &sep_key, right_child)
            };

            if ok {
                pending_split = false;
                left_child = new_ancestor;
            } else {
                // Branch also full — split it
                let (new_sep, new_right) = split_branch_with_insert(
                    pages,
                    alloc,
                    txn_id,
                    new_ancestor,
                    child_idx,
                    left_child,
                    &sep_key,
                    right_child,
                );
                left_child = new_ancestor;
                sep_key = new_sep;
                right_child = new_right;
            }
        } else {
            let page = pages.get_mut(&new_ancestor).unwrap();
            update_branch_child(page, child_idx, left_child);
            left_child = new_ancestor;
        }
    }

    if pending_split {
        // Create a new root
        let new_root_id = alloc.allocate();
        let mut new_root = Page::new(new_root_id, PageType::Branch, txn_id);
        let cell = branch_node::build_cell(left_child, &sep_key);
        new_root.write_cell(&cell).unwrap();
        new_root.set_right_child(right_child);
        pages.insert(new_root_id, new_root);
        *depth += 1;
        new_root_id
    } else {
        left_child
    }
}

/// Split a full branch and insert a separator.
/// Returns (promoted_separator_key, right_branch_page_id).
/// The left branch is `branch_id` (rebuilt in place).
#[allow(clippy::too_many_arguments)]
fn split_branch_with_insert(
    pages: &mut HashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    branch_id: PageId,
    child_idx: usize,
    new_left: PageId,
    sep_key: &[u8],
    new_right: PageId,
) -> (Vec<u8>, PageId) {
    // Collect all cells and apply the separator insertion logically
    let (new_cells, final_right_child) = {
        let page = pages.get(&branch_id).unwrap();
        let n = page.num_cells() as usize;
        let cells: Vec<(PageId, Vec<u8>)> = (0..n)
            .map(|i| {
                let cell = branch_node::read_cell(page, i as u16);
                (cell.child, cell.key.to_vec())
            })
            .collect();
        let old_rc = page.right_child();

        let mut result = Vec::with_capacity(n + 1);
        let final_rc;

        if child_idx < n {
            let old_key = cells[child_idx].1.clone();
            for (i, (child, key)) in cells.into_iter().enumerate() {
                if i == child_idx {
                    result.push((new_left, sep_key.to_vec()));
                    result.push((new_right, old_key.clone()));
                } else {
                    result.push((child, key));
                }
            }
            final_rc = old_rc;
        } else {
            result = cells;
            result.push((new_left, sep_key.to_vec()));
            final_rc = new_right;
        }

        (result, final_rc)
    };

    // Size-aware split — the middle key is promoted.
    // Left = [0..split_point], promoted = [split_point], right = [split_point+1..total].
    let total = new_cells.len();
    let usable = citadel_core::constants::USABLE_SIZE;
    let raw_sizes: Vec<usize> = new_cells.iter().map(|(_, key)| 6 + key.len()).collect();
    let mut cum: Vec<usize> = Vec::with_capacity(total + 1);
    cum.push(0);
    for &sz in &raw_sizes {
        cum.push(cum.last().unwrap() + sz);
    }
    let left_fits = |sp: usize| cum[sp] + sp * 2 <= usable;
    let right_fits = |sp: usize| {
        let right_count = total - sp - 1;
        (cum[total] - cum[sp + 1]) + right_count * 2 <= usable
    };

    let mut split_point = total / 2;
    if !left_fits(split_point) || !right_fits(split_point) {
        split_point = 1;
        for sp in 1..total.saturating_sub(1) {
            if left_fits(sp) && right_fits(sp) {
                split_point = sp;
                if sp >= total / 2 {
                    break;
                }
            }
        }
    }

    let promoted_sep = new_cells[split_point].1.clone();
    let promoted_child = new_cells[split_point].0;

    // Rebuild left branch with cells [0..split_point], right_child = promoted_child
    {
        let left_raw: Vec<Vec<u8>> = new_cells[..split_point]
            .iter()
            .map(|(child, key)| branch_node::build_cell(*child, key))
            .collect();
        let left_refs: Vec<&[u8]> = left_raw.iter().map(|c| c.as_slice()).collect();
        let page = pages.get_mut(&branch_id).unwrap();
        page.rebuild_cells(&left_refs);
        page.set_right_child(promoted_child);
    }

    // Create right branch with cells [split_point+1..total], right_child = final_right_child
    let right_branch_id = alloc.allocate();
    {
        let mut right_page = Page::new(right_branch_id, PageType::Branch, txn_id);
        let right_raw: Vec<Vec<u8>> = new_cells[split_point + 1..]
            .iter()
            .map(|(child, key)| branch_node::build_cell(*child, key))
            .collect();
        let right_refs: Vec<&[u8]> = right_raw.iter().map(|c| c.as_slice()).collect();
        right_page.rebuild_cells(&right_refs);
        right_page.set_right_child(final_right_child);
        pages.insert(right_branch_id, right_page);
    }

    (promoted_sep, right_branch_id)
}

/// Remove a child from a branch page at the given child index.
fn remove_child_from_branch(page: &mut Page, child_idx: usize) {
    let n = page.num_cells() as usize;
    if child_idx < n {
        let cell_sz = branch_node::get_cell_size(page, child_idx as u16);
        page.delete_cell_at(child_idx as u16, cell_sz);
    } else {
        // Removing right_child: promote last cell's child
        assert!(n > 0, "cannot remove right_child from branch with 0 cells");
        let last_child = branch_node::read_cell(page, (n - 1) as u16).child;
        let cell_sz = branch_node::get_cell_size(page, (n - 1) as u16);
        page.delete_cell_at((n - 1) as u16, cell_sz);
        page.set_right_child(last_child);
    }
}

/// Propagate child removal upward through the ancestor chain.
/// Handles cascading collapses when branches become empty.
fn propagate_remove_up(
    pages: &mut HashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    path: &[(PageId, usize)],
    depth: &mut u16,
) -> PageId {
    // Process the bottom-most ancestor first (parent of the deleted leaf)
    let mut level = path.len();

    // Track what needs to replace the removed child's slot in the parent above
    // Initially: the child was removed entirely, so we need to remove it from parent
    let mut need_remove_at_level = true;

    // Result: the page ID that should be propagated upward
    let mut new_child = PageId(0); // placeholder, set below

    while level > 0 && need_remove_at_level {
        level -= 1;
        let (ancestor_id, child_idx) = path[level];
        let new_ancestor = cow_page(pages, alloc, ancestor_id, txn_id);

        {
            let page = pages.get_mut(&new_ancestor).unwrap();
            remove_child_from_branch(page, child_idx);
        }

        let num_cells = pages.get(&new_ancestor).unwrap().num_cells();

        if num_cells > 0 || level == 0 {
            if num_cells == 0 && level == 0 {
                // Root collapsed — replace with its only child
                let only_child = pages.get(&new_ancestor).unwrap().right_child();
                alloc.free(new_ancestor);
                pages.remove(&new_ancestor);
                *depth -= 1;
                return only_child;
            }
            // Branch is non-empty, or it's the root with cells
            new_child = new_ancestor;
            need_remove_at_level = false;
        } else {
            // Branch became empty (0 cells) — collapse to its right_child
            let only_child = pages.get(&new_ancestor).unwrap().right_child();
            alloc.free(new_ancestor);
            pages.remove(&new_ancestor);
            *depth -= 1;

            // The only_child replaces this branch in the grandparent
            // This is a pointer update (not a removal), so we stop cascading
            new_child = only_child;
            need_remove_at_level = false;
        }
    }

    // Propagate CoW for remaining path levels above
    if level > 0 {
        let remaining_path = &path[..level];
        new_child = propagate_cow_up(pages, alloc, txn_id, remaining_path, new_child);
    }

    new_child
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_tree() -> (HashMap<PageId, Page>, PageAllocator, BTree) {
        let mut pages = HashMap::new();
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

        // Verify all keys searchable
        for k in &keys {
            let result = tree.search(&pages, *k).unwrap();
            assert_eq!(result, Some((ValueType::Inline, k.to_vec())));
        }

        // Verify non-existent key
        assert_eq!(tree.search(&pages, b"zebra").unwrap(), None);
    }

    #[test]
    fn insert_triggers_leaf_split() {
        let (mut pages, mut alloc, mut tree) = new_tree();

        // Insert enough keys to trigger at least one leaf split.
        // Each leaf cell: 7 + key_len + value_len bytes + 2 bytes pointer.
        // With 4-byte keys and 8-byte values: 7 + 4 + 8 = 19 bytes + 2 = 21 bytes per entry.
        // Page usable space: 8096 bytes. Fits ~385 entries per leaf.
        // We need > 385 entries to trigger a split.
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

        // Verify all keys present
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

        // Root is still a valid (empty) leaf
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

        // Root should have changed (CoW)
        assert_ne!(root_before, root_after);
        // Old root should have been freed via allocator
        assert!(alloc.freed_this_txn().contains(&root_before));
    }

    #[test]
    fn insert_and_delete_many() {
        let (mut pages, mut alloc, mut tree) = new_tree();
        let count = 1000u64;

        // Insert
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

        // Delete every other key
        for i in (0..count).step_by(2) {
            let key = format!("k{i:06}");
            let found = tree
                .delete(&mut pages, &mut alloc, TxnId(1), key.as_bytes())
                .unwrap();
            assert!(found);
        }
        assert_eq!(tree.entry_count, count / 2);

        // Verify remaining keys
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

        // Insert enough to create depth >= 2
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

        // Delete all
        for i in 0..count {
            let key = format!("{i:08}");
            let found = tree
                .delete(&mut pages, &mut alloc, TxnId(1), key.as_bytes())
                .unwrap();
            assert!(found, "key {key} should be deletable");
        }
        assert_eq!(tree.entry_count, 0);
    }
}
