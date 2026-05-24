//! B+ tree cursor for range iteration using a root-to-leaf path stack.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

use citadel_core::types::{PageId, PageType, ValueType};
use citadel_core::{Error, Result};
use citadel_page::leaf_node::LeafCell;
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};

pub trait PageMap {
    fn get_page(&self, id: &PageId) -> Option<&Page>;
}

/// Extends `PageMap` with on-demand page loading for lazy cursor traversal.
pub trait PageLoader: PageMap {
    fn ensure_loaded(&mut self, id: PageId) -> Result<()>;
}

impl<S: BuildHasher> PageMap for HashMap<PageId, Page, S> {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.get(id)
    }
}

impl<S: BuildHasher> PageMap for HashMap<PageId, Arc<Page>, S> {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.get(id).map(|a| a.as_ref())
    }
}

pub struct CursorEntry {
    pub key: Vec<u8>,
    pub val_type: ValueType,
    pub value: Vec<u8>,
}

/// B+ tree cursor position with root-to-leaf path.
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
    /// Seek to first key >= `key`. Empty key = first entry.
    pub fn seek(pages: &impl PageMap, root: PageId, key: &[u8]) -> Result<Self> {
        let mut path = Vec::new();
        let mut current = root;

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

        // Landed past the end of this leaf: advance to next.
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

    /// Current leaf page ID.
    pub fn leaf_page_id(&self) -> PageId {
        self.leaf
    }

    /// Current cell index within the leaf.
    pub fn cell_index(&self) -> u16 {
        self.cell_idx
    }

    /// Update the current leaf page ID after a CoW operation.
    pub fn set_leaf_page_id(&mut self, id: PageId) {
        self.leaf = id;
    }

    pub fn set_cell_index(&mut self, idx: u16) {
        self.cell_idx = idx;
    }

    pub fn advance_to_next_leaf<P: PageLoader + ?Sized>(&mut self, pages: &mut P) -> Result<bool> {
        self.advance_leaf_lazy(pages)
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

        self.retreat_leaf(pages)
    }

    /// Advance to the first cell of the next leaf.
    fn advance_leaf(&mut self, pages: &impl PageMap) -> Result<bool> {
        while let Some((parent_id, child_idx)) = self.path.pop() {
            let parent = pages
                .get_page(&parent_id)
                .ok_or(Error::PageOutOfBounds(parent_id))?;
            let n = parent.num_cells() as usize;

            if child_idx < n {
                let next_child_idx = child_idx + 1;
                let next_child = branch_node::get_child(parent, next_child_idx);
                self.path.push((parent_id, next_child_idx));

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

    /// Seek with lazy page loading — only loads the root-to-leaf path.
    pub fn seek_lazy(pages: &mut impl PageLoader, root: PageId, key: &[u8]) -> Result<Self> {
        let mut path = Vec::new();
        let mut current = root;

        loop {
            pages.ensure_loaded(current)?;
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

        if !valid && page.num_cells() > 0 {
            cursor.advance_leaf_lazy(pages)?;
        } else if page.num_cells() == 0 {
            cursor.valid = false;
        }

        Ok(cursor)
    }

    /// Read the current entry, loading the leaf page if needed.
    pub fn current_ref_lazy<'a, P: PageLoader + ?Sized>(
        &self,
        pages: &'a mut P,
    ) -> Option<LeafCell<'a>> {
        if !self.valid {
            return None;
        }
        pages.ensure_loaded(self.leaf).ok()?;
        let page = pages.get_page(&self.leaf)?;
        Some(leaf_node::read_cell(page, self.cell_idx))
    }

    /// Advance to the next entry, loading pages on demand.
    pub fn next_lazy<P: PageLoader + ?Sized>(&mut self, pages: &mut P) -> Result<bool> {
        if !self.valid {
            return Ok(false);
        }

        pages.ensure_loaded(self.leaf)?;
        let page = pages
            .get_page(&self.leaf)
            .ok_or(Error::PageOutOfBounds(self.leaf))?;

        if self.cell_idx + 1 < page.num_cells() {
            self.cell_idx += 1;
            return Ok(true);
        }

        self.advance_leaf_lazy(pages)
    }

    /// Advance to the next leaf, loading child pages on demand.
    fn advance_leaf_lazy<P: PageLoader + ?Sized>(&mut self, pages: &mut P) -> Result<bool> {
        while let Some((parent_id, child_idx)) = self.path.pop() {
            let parent = pages
                .get_page(&parent_id)
                .ok_or(Error::PageOutOfBounds(parent_id))?;
            let n = parent.num_cells() as usize;

            if child_idx < n {
                let next_child_idx = child_idx + 1;
                let next_child = branch_node::get_child(parent, next_child_idx);
                self.path.push((parent_id, next_child_idx));

                let mut current = next_child;
                loop {
                    pages.ensure_loaded(current)?;
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
        }

        self.valid = false;
        Ok(false)
    }

    /// Retreat to the last cell of the previous leaf.
    fn retreat_leaf(&mut self, pages: &impl PageMap) -> Result<bool> {
        while let Some((parent_id, child_idx)) = self.path.pop() {
            if child_idx > 0 {
                let prev_child_idx = child_idx - 1;
                let parent = pages
                    .get_page(&parent_id)
                    .ok_or(Error::PageOutOfBounds(parent_id))?;
                let prev_child = branch_node::get_child(parent, prev_child_idx);
                self.path.push((parent_id, prev_child_idx));

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
#[path = "cursor_tests.rs"]
mod tests;
