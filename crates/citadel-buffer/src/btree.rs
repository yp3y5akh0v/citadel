//! CoW B+ tree engine. Mutations clone pages; old pages go to pending-free list.

use crate::allocator::PageAllocator;
use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result};
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};
use rustc_hash::FxHashMap;

/// B+ tree metadata. Pages stored externally.
#[derive(Clone)]
pub struct BTree {
    pub root: PageId,
    pub depth: u16,
    pub entry_count: u64,
    last_insert: Option<(Vec<(PageId, usize)>, PageId)>,
}

#[derive(Debug, Clone)]
pub enum UpsertOutcome {
    Inserted,
    Updated,
    Skipped,
}

#[derive(Debug, Clone)]
pub enum UpsertAction {
    Replace(Vec<u8>),
    Skip,
}

impl BTree {
    /// Create a new empty B+ tree with a single leaf root.
    pub fn new(
        pages: &mut FxHashMap<PageId, Page>,
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
        pages: &FxHashMap<PageId, Page>,
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

    pub fn lil_would_hit(&self, pages: &FxHashMap<PageId, Page>, key: &[u8]) -> bool {
        if let Some((_, cached_leaf)) = &self.last_insert {
            if let Some(page) = pages.get(cached_leaf) {
                let n = page.num_cells();
                return n > 0 && key > leaf_node::read_cell(page, n - 1).key;
            }
        }
        false
    }

    /// Combined LIL check + insert. Returns `Some(was_new)` on hit, `None` on miss.
    pub fn try_lil_insert(
        &mut self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
        val_type: ValueType,
        value: &[u8],
    ) -> Result<Option<bool>> {
        let cached_leaf = match self.last_insert.as_ref() {
            Some((_, leaf)) => *leaf,
            None => return Ok(None),
        };
        let (hit, needs_cow) = {
            let page = pages
                .get(&cached_leaf)
                .ok_or(Error::PageOutOfBounds(cached_leaf))?;
            let n = page.num_cells();
            let h = n > 0 && key > leaf_node::read_cell(page, n - 1).key;
            let nc = page.txn_id() != txn_id;
            (h, nc)
        };
        if !hit {
            return Ok(None);
        }
        let mut cached_path = self.last_insert.take().unwrap().0;
        let cow_id = if needs_cow {
            cow_page(pages, alloc, cached_leaf, txn_id)
        } else {
            cached_leaf
        };
        let ok = {
            let page = pages.get_mut(&cow_id).unwrap();
            leaf_node::insert_append_direct(page, key, val_type, value)
        };
        if ok {
            if cow_id != cached_leaf {
                self.root = propagate_cow_up(pages, alloc, txn_id, &mut cached_path, cow_id);
            }
            self.entry_count += 1;
            self.last_insert = Some((cached_path, cow_id));
            return Ok(Some(true));
        }
        let (sep_key, right_id) =
            split_leaf_with_insert(pages, alloc, txn_id, cow_id, key, val_type, value);
        self.root = propagate_split_up(
            pages,
            alloc,
            txn_id,
            &cached_path,
            cow_id,
            &sep_key,
            right_id,
            &mut self.depth,
        );
        self.last_insert = None;
        self.entry_count += 1;
        Ok(Some(true))
    }

    /// Insert key-value. Returns `true` if new, `false` if updated existing.
    pub fn insert(
        &mut self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
        val_type: ValueType,
        value: &[u8],
    ) -> Result<bool> {
        // LIL cache: skip walk_to_leaf for sequential appends to the rightmost leaf.
        if let Some((mut cached_path, cached_leaf)) = self.last_insert.take() {
            let (hit, needs_cow) = {
                let page = pages
                    .get(&cached_leaf)
                    .ok_or(Error::PageOutOfBounds(cached_leaf))?;
                let n = page.num_cells();
                let h = n > 0 && key > leaf_node::read_cell(page, n - 1).key;
                let nc = page.txn_id() != txn_id;
                (h, nc)
            };
            if hit {
                let cow_id = if needs_cow {
                    cow_page(pages, alloc, cached_leaf, txn_id)
                } else {
                    cached_leaf
                };
                let ok = {
                    let page = pages.get_mut(&cow_id).unwrap();
                    leaf_node::insert_direct(page, key, val_type, value)
                };
                if ok {
                    if cow_id != cached_leaf {
                        self.root =
                            propagate_cow_up(pages, alloc, txn_id, &mut cached_path, cow_id);
                    }
                    self.entry_count += 1;
                    self.last_insert = Some((cached_path, cow_id));
                    return Ok(true);
                }
                let (sep_key, right_id) =
                    split_leaf_with_insert(pages, alloc, txn_id, cow_id, key, val_type, value);
                self.root = propagate_split_up(
                    pages,
                    alloc,
                    txn_id,
                    &cached_path,
                    cow_id,
                    &sep_key,
                    right_id,
                    &mut self.depth,
                );
                self.last_insert = None;
                self.entry_count += 1;
                return Ok(true);
            }
        }

        let (path, leaf_id) = self.walk_to_leaf(pages, key)?;
        self.insert_at_leaf(pages, alloc, txn_id, key, val_type, value, path, leaf_id)
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn insert_at_leaf(
        &mut self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
        val_type: ValueType,
        value: &[u8],
        path: Vec<(PageId, usize)>,
        leaf_id: PageId,
    ) -> Result<bool> {
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
            if alloc.in_place() && new_leaf_id == leaf_id {
                let mut is_rightmost = true;
                for &(ancestor_id, child_idx) in path.iter().rev() {
                    let page = pages.get(&ancestor_id).unwrap();
                    if child_idx != page.num_cells() as usize {
                        is_rightmost = false;
                        break;
                    }
                }
                if is_rightmost {
                    self.last_insert = Some((path, new_leaf_id));
                }
                if !key_exists {
                    self.entry_count += 1;
                }
                return Ok(!key_exists);
            }
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
            pages,
            alloc,
            txn_id,
            &path,
            new_leaf_id,
            &sep_key,
            right_id,
            &mut self.depth,
        );

        if !key_exists {
            self.entry_count += 1;
        }
        Ok(!key_exists)
    }

    pub fn insert_or_fetch(
        &mut self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
        val_type: ValueType,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        if let Some((mut cached_path, cached_leaf)) = self.last_insert.take() {
            let (hit, needs_cow) = {
                let page = pages
                    .get(&cached_leaf)
                    .ok_or(Error::PageOutOfBounds(cached_leaf))?;
                let n = page.num_cells();
                let h = n > 0 && key > leaf_node::read_cell(page, n - 1).key;
                let nc = page.txn_id() != txn_id;
                (h, nc)
            };
            if hit {
                let cow_id = if needs_cow {
                    cow_page(pages, alloc, cached_leaf, txn_id)
                } else {
                    cached_leaf
                };
                let ok = {
                    let page = pages.get_mut(&cow_id).unwrap();
                    leaf_node::insert_direct(page, key, val_type, value)
                };
                if ok {
                    if cow_id != cached_leaf {
                        self.root =
                            propagate_cow_up(pages, alloc, txn_id, &mut cached_path, cow_id);
                    }
                    self.entry_count += 1;
                    self.last_insert = Some((cached_path, cow_id));
                    return Ok(None);
                }
                let (sep_key, right_id) =
                    split_leaf_with_insert(pages, alloc, txn_id, cow_id, key, val_type, value);
                self.root = propagate_split_up(
                    pages,
                    alloc,
                    txn_id,
                    &cached_path,
                    cow_id,
                    &sep_key,
                    right_id,
                    &mut self.depth,
                );
                self.last_insert = None;
                self.entry_count += 1;
                return Ok(None);
            }
            self.last_insert = Some((cached_path, cached_leaf));
        }

        let (path, leaf_id) = self.walk_to_leaf(pages, key)?;

        let existing_value = {
            let page = pages.get(&leaf_id).unwrap();
            match leaf_node::search(page, key) {
                Ok(idx) => {
                    let cell = leaf_node::read_cell(page, idx);
                    if matches!(cell.val_type, ValueType::Tombstone) {
                        None
                    } else {
                        Some(cell.value.to_vec())
                    }
                }
                Err(_) => None,
            }
        };
        if let Some(v) = existing_value {
            return Ok(Some(v));
        }

        let new_leaf_id = cow_page(pages, alloc, leaf_id, txn_id);
        let leaf_ok = {
            let page = pages.get_mut(&new_leaf_id).unwrap();
            leaf_node::insert_direct(page, key, val_type, value)
        };

        if leaf_ok {
            if alloc.in_place() && new_leaf_id == leaf_id {
                let mut is_rightmost = true;
                for &(ancestor_id, child_idx) in path.iter().rev() {
                    let page = pages.get(&ancestor_id).unwrap();
                    if child_idx != page.num_cells() as usize {
                        is_rightmost = false;
                        break;
                    }
                }
                if is_rightmost {
                    self.last_insert = Some((path, new_leaf_id));
                }
                self.entry_count += 1;
                return Ok(None);
            }
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
            self.entry_count += 1;
            return Ok(None);
        }

        self.last_insert = None;
        let (sep_key, right_id) =
            split_leaf_with_insert(pages, alloc, txn_id, new_leaf_id, key, val_type, value);
        self.root = propagate_split_up(
            pages,
            alloc,
            txn_id,
            &path,
            new_leaf_id,
            &sep_key,
            right_id,
            &mut self.depth,
        );
        self.entry_count += 1;
        Ok(None)
    }

    #[inline]
    pub fn insert_if_absent(
        &mut self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
        val_type: ValueType,
        value: &[u8],
    ) -> Result<bool> {
        if let Some((mut cached_path, cached_leaf)) = self.last_insert.take() {
            let (hit, needs_cow) = {
                let page = pages
                    .get(&cached_leaf)
                    .ok_or(Error::PageOutOfBounds(cached_leaf))?;
                let n = page.num_cells();
                let h = n > 0 && key > leaf_node::read_cell(page, n - 1).key;
                let nc = page.txn_id() != txn_id;
                (h, nc)
            };
            if hit {
                let cow_id = if needs_cow {
                    cow_page(pages, alloc, cached_leaf, txn_id)
                } else {
                    cached_leaf
                };
                let ok = {
                    let page = pages.get_mut(&cow_id).unwrap();
                    leaf_node::insert_direct(page, key, val_type, value)
                };
                if ok {
                    if cow_id != cached_leaf {
                        self.root =
                            propagate_cow_up(pages, alloc, txn_id, &mut cached_path, cow_id);
                    }
                    self.entry_count += 1;
                    self.last_insert = Some((cached_path, cow_id));
                    return Ok(true);
                }
                let (sep_key, right_id) =
                    split_leaf_with_insert(pages, alloc, txn_id, cow_id, key, val_type, value);
                self.root = propagate_split_up(
                    pages,
                    alloc,
                    txn_id,
                    &cached_path,
                    cow_id,
                    &sep_key,
                    right_id,
                    &mut self.depth,
                );
                self.last_insert = None;
                self.entry_count += 1;
                return Ok(true);
            }
            self.last_insert = Some((cached_path, cached_leaf));
        }

        let (path, leaf_id) = self.walk_to_leaf(pages, key)?;
        self.insert_if_absent_at_leaf(pages, alloc, txn_id, key, val_type, value, path, leaf_id)
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn insert_if_absent_at_leaf(
        &mut self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
        val_type: ValueType,
        value: &[u8],
        path: Vec<(PageId, usize)>,
        leaf_id: PageId,
    ) -> Result<bool> {
        let exists = {
            let page = pages.get(&leaf_id).unwrap();
            match leaf_node::search(page, key) {
                Ok(idx) => {
                    let cell = leaf_node::read_cell(page, idx);
                    !matches!(cell.val_type, ValueType::Tombstone)
                }
                Err(_) => false,
            }
        };
        if exists {
            return Ok(false);
        }

        let new_leaf_id = cow_page(pages, alloc, leaf_id, txn_id);
        let leaf_ok = {
            let page = pages.get_mut(&new_leaf_id).unwrap();
            leaf_node::insert_direct(page, key, val_type, value)
        };

        if leaf_ok {
            if alloc.in_place() && new_leaf_id == leaf_id {
                let mut is_rightmost = true;
                for &(ancestor_id, child_idx) in path.iter().rev() {
                    let page = pages.get(&ancestor_id).unwrap();
                    if child_idx != page.num_cells() as usize {
                        is_rightmost = false;
                        break;
                    }
                }
                if is_rightmost {
                    self.last_insert = Some((path, new_leaf_id));
                }
                self.entry_count += 1;
                return Ok(true);
            }
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
            self.entry_count += 1;
            return Ok(true);
        }

        self.last_insert = None;
        let (sep_key, right_id) =
            split_leaf_with_insert(pages, alloc, txn_id, new_leaf_id, key, val_type, value);
        self.root = propagate_split_up(
            pages,
            alloc,
            txn_id,
            &path,
            new_leaf_id,
            &sep_key,
            right_id,
            &mut self.depth,
        );
        self.entry_count += 1;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn upsert_with<F, E>(
        &mut self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
        val_type: ValueType,
        default_value: &[u8],
        f: F,
    ) -> std::result::Result<UpsertOutcome, E>
    where
        F: FnMut(&[u8]) -> std::result::Result<UpsertAction, E>,
        E: From<Error>,
    {
        if let Some((mut cached_path, cached_leaf)) = self.last_insert.take() {
            let (hit, needs_cow) = {
                let page = pages
                    .get(&cached_leaf)
                    .ok_or(Error::PageOutOfBounds(cached_leaf))?;
                let n = page.num_cells();
                let h = n > 0 && key > leaf_node::read_cell(page, n - 1).key;
                let nc = page.txn_id() != txn_id;
                (h, nc)
            };
            if hit {
                let cow_id = if needs_cow {
                    cow_page(pages, alloc, cached_leaf, txn_id)
                } else {
                    cached_leaf
                };
                let ok = {
                    let page = pages.get_mut(&cow_id).unwrap();
                    leaf_node::insert_direct(page, key, val_type, default_value)
                };
                if ok {
                    if cow_id != cached_leaf {
                        self.root =
                            propagate_cow_up(pages, alloc, txn_id, &mut cached_path, cow_id);
                    }
                    self.entry_count += 1;
                    self.last_insert = Some((cached_path, cow_id));
                    return Ok(UpsertOutcome::Inserted);
                }
                let (sep_key, right_id) = split_leaf_with_insert(
                    pages,
                    alloc,
                    txn_id,
                    cow_id,
                    key,
                    val_type,
                    default_value,
                );
                self.root = propagate_split_up(
                    pages,
                    alloc,
                    txn_id,
                    &cached_path,
                    cow_id,
                    &sep_key,
                    right_id,
                    &mut self.depth,
                );
                self.last_insert = None;
                self.entry_count += 1;
                return Ok(UpsertOutcome::Inserted);
            }
            self.last_insert = Some((cached_path, cached_leaf));
        }

        let (path, leaf_id) = self.walk_to_leaf(pages, key)?;
        self.upsert_with_at_leaf(
            pages,
            alloc,
            txn_id,
            key,
            val_type,
            default_value,
            path,
            leaf_id,
            f,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn upsert_with_at_leaf<F, E>(
        &mut self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
        val_type: ValueType,
        default_value: &[u8],
        path: Vec<(PageId, usize)>,
        leaf_id: PageId,
        mut f: F,
    ) -> std::result::Result<UpsertOutcome, E>
    where
        F: FnMut(&[u8]) -> std::result::Result<UpsertAction, E>,
        E: From<Error>,
    {
        let action = {
            let page = pages.get(&leaf_id).unwrap();
            match leaf_node::search(page, key) {
                Ok(idx) => {
                    let cell = leaf_node::read_cell(page, idx);
                    if matches!(cell.val_type, ValueType::Tombstone) {
                        None
                    } else {
                        Some(f(cell.value)?)
                    }
                }
                Err(_) => None,
            }
        };

        if let Some(act) = action {
            match act {
                UpsertAction::Skip => return Ok(UpsertOutcome::Skipped),
                UpsertAction::Replace(new_bytes) => {
                    let new_leaf_id = cow_page(pages, alloc, leaf_id, txn_id);
                    let leaf_ok = {
                        let page = pages.get_mut(&new_leaf_id).unwrap();
                        leaf_node::insert_direct(page, key, val_type, &new_bytes)
                    };
                    if leaf_ok {
                        if new_leaf_id != leaf_id {
                            let mut new_path = path;
                            self.root =
                                propagate_cow_up(pages, alloc, txn_id, &mut new_path, new_leaf_id);
                        }
                        return Ok(UpsertOutcome::Updated);
                    }
                    self.last_insert = None;
                    let (sep_key, right_id) = split_leaf_with_insert(
                        pages,
                        alloc,
                        txn_id,
                        new_leaf_id,
                        key,
                        val_type,
                        &new_bytes,
                    );
                    self.root = propagate_split_up(
                        pages,
                        alloc,
                        txn_id,
                        &path,
                        new_leaf_id,
                        &sep_key,
                        right_id,
                        &mut self.depth,
                    );
                    return Ok(UpsertOutcome::Updated);
                }
            }
        }

        let new_leaf_id = cow_page(pages, alloc, leaf_id, txn_id);
        let leaf_ok = {
            let page = pages.get_mut(&new_leaf_id).unwrap();
            leaf_node::insert_direct(page, key, val_type, default_value)
        };

        if leaf_ok {
            if alloc.in_place() && new_leaf_id == leaf_id {
                let mut is_rightmost = true;
                for &(ancestor_id, child_idx) in path.iter().rev() {
                    let page = pages.get(&ancestor_id).unwrap();
                    if child_idx != page.num_cells() as usize {
                        is_rightmost = false;
                        break;
                    }
                }
                if is_rightmost {
                    self.last_insert = Some((path, new_leaf_id));
                }
                self.entry_count += 1;
                return Ok(UpsertOutcome::Inserted);
            }
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
            self.entry_count += 1;
            return Ok(UpsertOutcome::Inserted);
        }

        self.last_insert = None;
        let (sep_key, right_id) = split_leaf_with_insert(
            pages,
            alloc,
            txn_id,
            new_leaf_id,
            key,
            val_type,
            default_value,
        );
        self.root = propagate_split_up(
            pages,
            alloc,
            txn_id,
            &path,
            new_leaf_id,
            &sep_key,
            right_id,
            &mut self.depth,
        );
        self.entry_count += 1;
        Ok(UpsertOutcome::Inserted)
    }

    /// Bulk-update existing keys. Keys must be sorted.
    pub fn update_sorted(
        &mut self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        pairs: &[(&[u8], &[u8])],
    ) -> Result<u64> {
        if pairs.is_empty() {
            return Ok(0);
        }
        self.last_insert = None;

        let (mut path, mut leaf_id) = self.walk_to_leaf(pages, pairs[0].0)?;
        let mut cow_leaf = cow_page(pages, alloc, leaf_id, txn_id);
        if cow_leaf != leaf_id {
            self.root = propagate_cow_up(pages, alloc, txn_id, &mut path, cow_leaf);
        }

        let mut count: u64 = 0;
        let mut hint: u16 = 0;

        for &(key, value) in pairs {
            let past_leaf = {
                let page = pages.get(&cow_leaf).unwrap();
                let n = page.num_cells();
                n == 0 || key > leaf_node::read_cell(page, n - 1).key
            };

            if past_leaf {
                let (new_path, new_leaf) = self.walk_to_leaf(pages, key)?;
                path = new_path;
                leaf_id = new_leaf;
                cow_leaf = cow_page(pages, alloc, leaf_id, txn_id);
                if cow_leaf != leaf_id {
                    self.root = propagate_cow_up(pages, alloc, txn_id, &mut path, cow_leaf);
                }
                hint = 0;
            }

            let page = pages.get(&cow_leaf).unwrap();
            let n = page.num_cells();
            let idx = {
                let mut i = hint;
                loop {
                    if i >= n {
                        break None;
                    }
                    let cell = leaf_node::read_cell(page, i);
                    match key.cmp(cell.key) {
                        std::cmp::Ordering::Equal => break Some(i),
                        std::cmp::Ordering::Less => break None,
                        std::cmp::Ordering::Greater => i += 1,
                    }
                }
            };

            if let Some(idx) = idx {
                hint = idx + 1;
                let page = pages.get_mut(&cow_leaf).unwrap();
                if !leaf_node::update_value_in_place(page, idx, ValueType::Inline, value) {
                    leaf_node::insert_direct(page, key, ValueType::Inline, value);
                }
                count += 1;
            }
        }

        Ok(count)
    }

    /// Delete a key. Returns `true` if the key was found and deleted.
    pub fn delete(
        &mut self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
    ) -> Result<bool> {
        self.last_insert = None;
        let (mut path, leaf_id) = self.walk_to_leaf(pages, key)?;

        let found = {
            let page = pages.get(&leaf_id).unwrap();
            leaf_node::search(page, key).is_ok()
        };
        if !found {
            return Ok(false);
        }

        let new_leaf_id = cow_page(pages, alloc, leaf_id, txn_id);
        {
            let page = pages.get_mut(&new_leaf_id).unwrap();
            leaf_node::delete(page, key);
        }

        let leaf_empty = pages.get(&new_leaf_id).unwrap().num_cells() == 0;

        if !leaf_empty || path.is_empty() {
            if alloc.in_place() && new_leaf_id == leaf_id {
                self.entry_count -= 1;
                return Ok(true);
            }
            self.root = propagate_cow_up(pages, alloc, txn_id, &mut path, new_leaf_id);
            self.entry_count -= 1;
            return Ok(true);
        }

        alloc.free(new_leaf_id);
        pages.remove(&new_leaf_id);

        self.root = propagate_remove_up(pages, alloc, txn_id, &mut path, &mut self.depth);
        self.entry_count -= 1;
        Ok(true)
    }

    /// Walk root to leaf for `key`. Returns (path, leaf_page_id).
    pub fn walk_to_leaf(
        &self,
        pages: &FxHashMap<PageId, Page>,
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

/// CoW a page. No-op if already owned by this txn. In-place mode reuses page ID.
pub fn cow_page(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    old_id: PageId,
    txn_id: TxnId,
) -> PageId {
    if alloc.in_place() {
        let page = pages.get_mut(&old_id).unwrap();
        if page.txn_id() != txn_id {
            page.set_txn_id(txn_id);
        }
        return old_id;
    }
    let mut new_page = {
        let page = pages.get(&old_id).unwrap();
        if page.txn_id() == txn_id {
            return old_id;
        }
        page.clone()
    };
    let new_id = alloc.allocate();
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

/// Propagate CoW up through ancestors. Updates `path` in place so callers
/// caching the path (e.g. LIL) reuse current PageIds after CoW — critical
/// across SAVEPOINT boundaries where txn_id changes invalidate the cache.
pub fn propagate_cow_up(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    path: &mut [(PageId, usize)],
    mut new_child: PageId,
) -> PageId {
    for i in (0..path.len()).rev() {
        let (ancestor_id, child_idx) = path[i];
        let new_ancestor = cow_page(pages, alloc, ancestor_id, txn_id);
        let page = pages.get_mut(&new_ancestor).unwrap();
        update_branch_child(page, child_idx, new_child);
        path[i] = (new_ancestor, child_idx);
        new_child = new_ancestor;
    }
    new_child
}

/// Split full leaf and insert. Returns (separator_key, right_page_id).
fn split_leaf_with_insert(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    leaf_id: PageId,
    key: &[u8],
    val_type: ValueType,
    value: &[u8],
) -> (Vec<u8>, PageId) {
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

    let new_raw = leaf_node::build_cell(key, val_type, value);
    match cells.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
        Ok(idx) => cells[idx] = (key.to_vec(), new_raw),
        Err(idx) => cells.insert(idx, (key.to_vec(), new_raw)),
    }

    let total = cells.len();

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

    {
        let left_refs: Vec<&[u8]> = cells[..split_point]
            .iter()
            .map(|(_, raw)| raw.as_slice())
            .collect();
        let page = pages.get_mut(&leaf_id).unwrap();
        page.rebuild_cells(&left_refs);
    }

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

#[allow(clippy::too_many_arguments)]
fn propagate_split_up(
    pages: &mut FxHashMap<PageId, Page>,
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

#[allow(clippy::too_many_arguments)]
fn split_branch_with_insert(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    branch_id: PageId,
    child_idx: usize,
    new_left: PageId,
    sep_key: &[u8],
    new_right: PageId,
) -> (Vec<u8>, PageId) {
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

fn remove_child_from_branch(page: &mut Page, child_idx: usize) {
    let n = page.num_cells() as usize;
    if child_idx < n {
        let cell_sz = branch_node::get_cell_size(page, child_idx as u16);
        page.delete_cell_at(child_idx as u16, cell_sz);
    } else {
        assert!(n > 0, "cannot remove right_child from branch with 0 cells");
        let last_child = branch_node::read_cell(page, (n - 1) as u16).child;
        let cell_sz = branch_node::get_cell_size(page, (n - 1) as u16);
        page.delete_cell_at((n - 1) as u16, cell_sz);
        page.set_right_child(last_child);
    }
}

fn propagate_remove_up(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    path: &mut [(PageId, usize)],
    depth: &mut u16,
) -> PageId {
    let mut level = path.len();
    let mut need_remove_at_level = true;
    let mut new_child = PageId(0);

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
                // Root collapsed - replace with its only child
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
            // Branch became empty (0 cells) - collapse to its right_child
            let only_child = pages.get(&new_ancestor).unwrap().right_child();
            alloc.free(new_ancestor);
            pages.remove(&new_ancestor);
            *depth -= 1;

            new_child = only_child;
            need_remove_at_level = false;
        }
    }

    if level > 0 {
        let remaining_path = &mut path[..level];
        new_child = propagate_cow_up(pages, alloc, txn_id, remaining_path, new_child);
    }

    new_child
}

#[cfg(test)]
#[path = "btree_tests.rs"]
mod tests;
