//! Write transaction: CoW mutations with shadow-paging commit.

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result, MAX_INLINE_VALUE_SIZE, MAX_KEY_SIZE};
use citadel_io::file_manager::CommitSlot;
use citadel_page::branch_node;
use citadel_page::page::Page;
use std::collections::HashMap;

use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::btree::{self, BTree};
use citadel_buffer::cursor::{Cursor, PageLoader, PageMap};

use crate::catalog::TableDescriptor;
use crate::manager::TxnManager;

struct WritePages<'a> {
    pages: &'a mut HashMap<PageId, Page>,
    manager: &'a TxnManager,
}

impl PageMap for WritePages<'_> {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.pages.get(id)
    }
}

impl PageLoader for WritePages<'_> {
    fn ensure_loaded(&mut self, id: PageId) -> Result<()> {
        if !self.pages.contains_key(&id) {
            let page = self.manager.fetch_page_owned(id)?;
            self.pages.insert(id, page);
        }
        Ok(())
    }
}

pub struct WriteTxn<'a> {
    manager: &'a TxnManager,
    base_txn_id: TxnId,
    txn_id: TxnId,
    old_slot: CommitSlot,
    pages: HashMap<PageId, Page>,
    tree: BTree,
    alloc: PageAllocator,
    committed: bool,
    deferred_free: Vec<PageId>,
    named_trees: HashMap<Vec<u8>, BTree>,
    catalog: Option<BTree>,
    catalog_dirty: bool,
    loaded_tree_meta: HashMap<Vec<u8>, (PageId, u16)>,
}

#[derive(Clone)]
pub struct WriteTxnSnapshot {
    txn_id: TxnId,
    tree: BTree,
    alloc: PageAllocator,
    named_trees: HashMap<Vec<u8>, BTree>,
    catalog: Option<BTree>,
    catalog_dirty: bool,
    loaded_tree_meta: HashMap<Vec<u8>, (PageId, u16)>,
    deferred_free: Vec<PageId>,
}

impl<'a> WriteTxn<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        manager: &'a TxnManager,
        txn_id: TxnId,
        snapshot: CommitSlot,
        tree: BTree,
        alloc: PageAllocator,
        deferred_free: Vec<PageId>,
        recycled_pages: Option<HashMap<PageId, Page>>,
        recycle_safe: bool,
    ) -> Self {
        let pages = match recycled_pages {
            Some(mut m) => {
                if !(alloc.in_place() && recycle_safe) {
                    m.clear();
                }
                m
            }
            None => HashMap::with_capacity(16),
        };
        Self {
            manager,
            base_txn_id: txn_id,
            txn_id,
            old_slot: snapshot,
            pages,
            tree,
            alloc,
            committed: false,
            deferred_free,
            named_trees: HashMap::new(),
            catalog: None,
            catalog_dirty: false,
            loaded_tree_meta: HashMap::new(),
        }
    }

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    pub fn entry_count(&self) -> u64 {
        self.tree.entry_count
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.preload_path(self.tree.root, key)?;
        self.search_in_tree(&self.tree.clone(), key)
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        Self::validate_key_value(key, value)?;
        self.preload_path(self.tree.root, key)?;
        self.tree.insert(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            key,
            ValueType::Inline,
            value,
        )
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.preload_path(self.tree.root, key)?;
        self.tree
            .delete(&mut self.pages, &mut self.alloc, self.txn_id, key)
    }

    pub fn for_each<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        self.preload_all_pages(self.tree.root)?;
        let mut cursor = Cursor::first(&self.pages, self.tree.root)?;
        while cursor.is_valid() {
            if let Some(entry) = cursor.current_ref(&self.pages) {
                if entry.val_type != ValueType::Tombstone {
                    f(entry.key, entry.value)?;
                }
            }
            cursor.next(&self.pages)?;
        }
        Ok(())
    }

    pub fn table_entry_count(&mut self, table: &[u8]) -> Result<u64> {
        self.ensure_table(table)?;
        Ok(self.named_trees[table].entry_count)
    }

    pub fn table_for_each<F>(&mut self, table: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        self.ensure_table(table)?;
        let root = self.named_trees[table].root;
        self.preload_all_pages(root)?;
        let mut cursor = Cursor::first(&self.pages, root)?;
        while cursor.is_valid() {
            if let Some(entry) = cursor.current_ref(&self.pages) {
                if entry.val_type != ValueType::Tombstone {
                    f(entry.key, entry.value)?;
                }
            }
            cursor.next(&self.pages)?;
        }
        Ok(())
    }

    pub fn table_scan_from<F>(&mut self, table: &[u8], start_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        self.ensure_table(table)?;
        let root = self.named_trees[table].root;
        let mut view = WritePages {
            pages: &mut self.pages,
            manager: self.manager,
        };
        let mut cursor = Cursor::seek_lazy(&mut view, root, start_key)?;
        while cursor.is_valid() {
            if let Some(entry) = cursor.current_ref_lazy(&mut view) {
                if entry.val_type != ValueType::Tombstone && !f(entry.key, entry.value)? {
                    break;
                }
            }
            cursor.next_lazy(&mut view)?;
        }
        Ok(())
    }

    pub fn create_table(&mut self, name: &[u8]) -> Result<()> {
        self.ensure_catalog()?;

        if self.named_trees.contains_key(name) {
            return Err(Error::TableAlreadyExists(
                String::from_utf8_lossy(name).into_owned(),
            ));
        }

        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, name)?;
        if let Some((vt, _)) = self.catalog.as_ref().unwrap().search(&self.pages, name)? {
            if vt != ValueType::Tombstone {
                return Err(Error::TableAlreadyExists(
                    String::from_utf8_lossy(name).into_owned(),
                ));
            }
        }

        let page_id = self.alloc.allocate();
        let mut leaf = Page::new(page_id, PageType::Leaf, self.txn_id);
        leaf.update_checksum();
        self.pages.insert(page_id, leaf);

        let new_tree = BTree::from_existing(page_id, 1, 0);
        self.named_trees.insert(name.to_vec(), new_tree);
        self.catalog_dirty = true;
        Ok(())
    }

    pub fn drop_table(&mut self, name: &[u8]) -> Result<()> {
        self.ensure_table(name)?;
        self.ensure_catalog()?;

        let tree = self.named_trees.remove(name).unwrap();
        self.free_tree_pages(tree.root)?;

        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, name)?;
        self.catalog.as_mut().unwrap().delete(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            name,
        )?;
        self.catalog_dirty = true;
        Ok(())
    }

    /// Rename a table in the catalog. O(1) - no data copy.
    pub fn rename_table(&mut self, old_name: &[u8], new_name: &[u8]) -> Result<()> {
        self.ensure_table(old_name)?;

        if self.named_trees.contains_key(new_name) {
            return Err(Error::TableAlreadyExists(
                String::from_utf8_lossy(new_name).into_owned(),
            ));
        }

        self.ensure_catalog()?;
        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, new_name)?;
        if let Some((vt, _)) = self
            .catalog
            .as_ref()
            .unwrap()
            .search(&self.pages, new_name)?
        {
            if vt != ValueType::Tombstone {
                return Err(Error::TableAlreadyExists(
                    String::from_utf8_lossy(new_name).into_owned(),
                ));
            }
        }

        let tree = self.named_trees.remove(old_name).unwrap();
        self.named_trees.insert(new_name.to_vec(), tree);

        // Remove old meta so finalize_catalog writes the new name
        self.loaded_tree_meta.remove(old_name);

        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, old_name)?;
        self.catalog.as_mut().unwrap().delete(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            old_name,
        )?;
        self.catalog_dirty = true;
        Ok(())
    }

    pub fn table_insert(&mut self, table: &[u8], key: &[u8], value: &[u8]) -> Result<bool> {
        Self::validate_key_value(key, value)?;

        let (root, lil_hit) = match self.named_trees.get(table) {
            Some(tree) => (tree.root, tree.lil_would_hit(&self.pages, key)),
            None => {
                self.ensure_table(table)?;
                let tree = &self.named_trees[table];
                (tree.root, tree.lil_would_hit(&self.pages, key))
            }
        };
        if !lil_hit {
            self.preload_path(root, key)?;
        }

        let tree = self.named_trees.get_mut(table).unwrap();
        tree.insert(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            key,
            ValueType::Inline,
            value,
        )
    }

    /// Batch-update existing keys (sorted). Single traversal: O(depth + N).
    pub fn table_update_sorted(&mut self, table: &[u8], pairs: &[(&[u8], &[u8])]) -> Result<u64> {
        if pairs.is_empty() {
            return Ok(0);
        }
        self.ensure_table(table)?;
        // Preload path to first key only
        let root = self.named_trees[table].root;
        self.preload_path(root, pairs[0].0)?;

        let tree = self.named_trees.get_mut(table).unwrap();
        tree.update_sorted(&mut self.pages, &mut self.alloc, self.txn_id, pairs)
    }

    /// Fused scan + in-place patch from `start_key`. Callback: `Some(true)`=modified, `None`=stop.
    pub fn table_update_range<F, E>(
        &mut self,
        table: &[u8],
        start_key: &[u8],
        mut f: F,
    ) -> std::result::Result<u64, E>
    where
        F: FnMut(&[u8], &mut [u8]) -> std::result::Result<Option<bool>, E>,
        E: From<Error>,
    {
        self.ensure_table(table)?;
        let root = self.named_trees[table].root;

        // Lazy-seek to start_key
        let mut view = WritePages {
            pages: &mut self.pages,
            manager: self.manager,
        };
        let mut cursor = Cursor::seek_lazy(&mut view, root, start_key)?;

        let mut count: u64 = 0;
        let mut cow_leaf = PageId::INVALID;

        while cursor.is_valid() {
            // Ensure current leaf page is loaded
            let leaf_id = cursor.leaf_page_id();
            view.ensure_loaded(leaf_id)?;

            {
                let page = view.pages.get(&leaf_id).unwrap();
                let cell = citadel_page::leaf_node::read_cell(page, cursor.cell_index());
                if cell.val_type == ValueType::Tombstone {
                    cursor.next_lazy(&mut view)?;
                    continue;
                }
            }

            // CoW leaf if not yet owned by this txn
            if cow_leaf != leaf_id {
                let new_id = btree::cow_page(view.pages, &mut self.alloc, leaf_id, self.txn_id);
                if new_id != leaf_id {
                    let tree = self.named_trees.get_mut(table).unwrap();
                    let cell = citadel_page::leaf_node::read_cell(
                        view.pages.get(&new_id).unwrap(),
                        cursor.cell_index(),
                    );
                    let key_for_walk = cell.key.to_vec();
                    let (mut path, _) = tree.walk_to_leaf(view.pages, &key_for_walk)?;
                    tree.root = btree::propagate_cow_up(
                        view.pages,
                        &mut self.alloc,
                        self.txn_id,
                        &mut path,
                        new_id,
                    );
                    // Update cursor to track the new page ID
                    cursor.set_leaf_page_id(new_id);
                }
                cow_leaf = new_id;
            }

            // Direct mutable access to cell bytes in the CoW'd page
            let page = view.pages.get_mut(&cow_leaf).unwrap();
            let ci = cursor.cell_index();
            let cell_off = page.cell_offset(ci) as usize;
            let key_len =
                u16::from_le_bytes(page.data[cell_off..cell_off + 2].try_into().unwrap()) as usize;
            let val_len =
                u32::from_le_bytes(page.data[cell_off + 2..cell_off + 6].try_into().unwrap())
                    as usize;
            let key_start = cell_off + 6;
            let val_start = cell_off + 7 + key_len;

            // Split borrow: key (immutable) and value (mutable) from non-overlapping regions
            let (before_val, from_val) = page.data.split_at_mut(val_start);
            let key = &before_val[key_start..key_start + key_len];
            let value = &mut from_val[..val_len];

            match f(key, value)? {
                Some(true) => count += 1,
                Some(false) => {}
                None => break,
            }

            cursor.next_lazy(&mut view)?;
        }

        Ok(count)
    }

    pub fn table_delete(&mut self, table: &[u8], key: &[u8]) -> Result<bool> {
        self.ensure_table(table)?;

        let root = self.named_trees[table].root;
        self.preload_path(root, key)?;

        let tree = self.named_trees.get_mut(table).unwrap();
        tree.delete(&mut self.pages, &mut self.alloc, self.txn_id, key)
    }

    pub fn table_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.ensure_table(table)?;

        let tree = self.named_trees[table].clone();
        self.preload_path(tree.root, key)?;
        self.search_in_tree(&tree, key)
    }

    pub fn commit(mut self) -> Result<()> {
        let catalog_root = self.finalize_catalog()?;
        self.manager.commit_write(
            self.base_txn_id,
            self.txn_id,
            &mut self.pages,
            &mut self.alloc,
            &self.tree,
            &self.old_slot,
            &self.deferred_free,
            catalog_root,
            &self.named_trees,
            &self.loaded_tree_meta,
        )?;
        self.committed = true;
        Ok(())
    }

    pub fn abort(mut self) {
        self.committed = true;
        self.manager.abort_write();
    }

    // ── Savepoint support ──────────────────────────────────────────

    /// SAVEPOINT: snapshot state and advance txn_id so post-savepoint
    /// mutations CoW into fresh PageIds.
    pub fn begin_savepoint(&mut self) -> WriteTxnSnapshot {
        let snap = self.capture_snapshot();
        self.txn_id = self.manager.next_write_txn_id();
        snap
    }

    /// ROLLBACK TO SAVEPOINT: restore state and drop post-savepoint pages.
    /// Advances txn_id again so repeated rollback-to works.
    pub fn restore_snapshot(&mut self, snap: WriteTxnSnapshot) {
        let cutoff = snap.txn_id;
        self.tree = snap.tree;
        self.alloc = snap.alloc;
        self.named_trees = snap.named_trees;
        self.catalog = snap.catalog;
        self.catalog_dirty = snap.catalog_dirty;
        self.loaded_tree_meta = snap.loaded_tree_meta;
        self.deferred_free = snap.deferred_free;
        self.pages.retain(|_, page| page.txn_id() <= cutoff);
        self.txn_id = self.manager.next_write_txn_id();
    }

    fn capture_snapshot(&self) -> WriteTxnSnapshot {
        WriteTxnSnapshot {
            txn_id: self.txn_id,
            tree: self.tree.clone(),
            alloc: self.alloc.clone(),
            named_trees: self.named_trees.clone(),
            catalog: self.catalog.clone(),
            catalog_dirty: self.catalog_dirty,
            loaded_tree_meta: self.loaded_tree_meta.clone(),
            deferred_free: self.deferred_free.clone(),
        }
    }

    /// Must be disabled while savepoints are live — in-place CoW defeats rollback.
    pub fn set_in_place(&mut self, enabled: bool) {
        self.alloc.set_in_place(enabled);
    }

    pub fn in_place(&self) -> bool {
        self.alloc.in_place()
    }

    pub fn base_txn_id(&self) -> TxnId {
        self.base_txn_id
    }

    fn validate_key_value(key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() > MAX_KEY_SIZE {
            return Err(Error::KeyTooLarge {
                size: key.len(),
                max: MAX_KEY_SIZE,
            });
        }
        if value.len() > MAX_INLINE_VALUE_SIZE {
            return Err(Error::ValueTooLarge {
                size: value.len(),
                max: MAX_INLINE_VALUE_SIZE,
            });
        }
        Ok(())
    }

    fn search_in_tree(&self, tree: &BTree, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match tree.search(&self.pages, key)? {
            Some((ValueType::Tombstone, _)) => Ok(None),
            Some((_, value)) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    fn ensure_catalog(&mut self) -> Result<()> {
        if self.catalog.is_some() {
            return Ok(());
        }

        if self.old_slot.catalog_root.is_valid() {
            self.preload_path(self.old_slot.catalog_root, &[])?;
            let slot = self.catalog_slot_from_disk()?;
            self.catalog = Some(BTree::from_existing(
                slot.root_page,
                slot.depth,
                slot.entry_count,
            ));
        } else {
            let page_id = self.alloc.allocate();
            let mut leaf = Page::new(page_id, PageType::Leaf, self.txn_id);
            leaf.update_checksum();
            self.pages.insert(page_id, leaf);
            self.catalog = Some(BTree::from_existing(page_id, 1, 0));
            self.catalog_dirty = true;
        }
        Ok(())
    }

    fn catalog_slot_from_disk(&mut self) -> Result<TableDescriptor> {
        let root = self.old_slot.catalog_root;
        let mut depth: u16 = 1;
        let mut current = root;
        loop {
            if !self.pages.contains_key(&current) {
                let page = self.manager.fetch_page_owned(current)?;
                self.pages.insert(current, page);
            }
            let page = self.pages.get(&current).unwrap();
            match page.page_type() {
                Some(PageType::Leaf) => break,
                Some(PageType::Branch) => {
                    depth += 1;
                    current = branch_node::get_child(page, 0);
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }

        let entry_count = self.count_leaf_entries(root)?;
        Ok(TableDescriptor {
            root_page: root,
            entry_count,
            depth,
            flags: 0,
        })
    }

    fn ensure_table(&mut self, name: &[u8]) -> Result<()> {
        if self.named_trees.contains_key(name) {
            return Ok(());
        }

        // Fast path: use cached root from commit slot (avoids catalog B+ tree lookup)
        if let Some((root, depth)) = self.old_slot.named_entry_root(name) {
            let entry_count = self.old_slot.named_entry_count(name).unwrap_or(0);
            let tree = BTree::from_existing(root, depth, entry_count);
            self.loaded_tree_meta.insert(name.to_vec(), (root, depth));
            self.named_trees.insert(name.to_vec(), tree);
            return Ok(());
        }

        // Slow path: fall back to catalog B+ tree
        self.ensure_catalog()?;

        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, name)?;

        match self.catalog.as_ref().unwrap().search(&self.pages, name)? {
            Some((ValueType::Tombstone, _)) | None => {
                return Err(Error::TableNotFound(
                    String::from_utf8_lossy(name).into_owned(),
                ));
            }
            Some((_, desc_bytes)) => {
                let desc = TableDescriptor::deserialize(&desc_bytes);
                let entry_count = self
                    .old_slot
                    .named_entry_count(name)
                    .unwrap_or(desc.entry_count);
                let tree = BTree::from_existing(desc.root_page, desc.depth, entry_count);
                self.loaded_tree_meta
                    .insert(name.to_vec(), (desc.root_page, desc.depth));
                self.named_trees.insert(name.to_vec(), tree);
            }
        }
        Ok(())
    }

    fn finalize_catalog(&mut self) -> Result<PageId> {
        if !self.catalog_dirty && self.named_trees.is_empty() {
            return Ok(self.old_slot.catalog_root);
        }

        // SyncMode::Off: skip catalog update if only roots changed (cached in slot)
        if !self.catalog_dirty && self.manager.sync_mode() == citadel_core::types::SyncMode::Off {
            let needs_catalog = self.named_trees.iter().any(|(name, tree)| {
                match self.loaded_tree_meta.get(name.as_slice()) {
                    Some(&(_, old_depth)) => tree.depth != old_depth,
                    None => true, // new table
                }
            });
            if !needs_catalog {
                return Ok(self.old_slot.catalog_root);
            }
        }

        if self.catalog.is_none() {
            self.ensure_catalog()?;
        }

        let structural_entries: Vec<(Vec<u8>, [u8; 20])> = self
            .named_trees
            .iter()
            .filter(
                |(name, tree)| match self.loaded_tree_meta.get(name.as_slice()) {
                    Some(&(old_root, old_depth)) => {
                        tree.root != old_root || tree.depth != old_depth
                    }
                    None => true,
                },
            )
            .map(|(name, tree)| {
                let desc = TableDescriptor::from_tree(tree);
                (name.clone(), desc.serialize())
            })
            .collect();

        if structural_entries.is_empty() {
            return Ok(self.catalog.as_ref().unwrap().root);
        }

        for (name, value) in &structural_entries {
            let catalog = self.catalog.as_ref().unwrap();
            let catalog_root = catalog.root;
            self.preload_path(catalog_root, name)?;

            self.catalog.as_mut().unwrap().insert(
                &mut self.pages,
                &mut self.alloc,
                self.txn_id,
                name,
                ValueType::Inline,
                value,
            )?;
        }

        Ok(self.catalog.as_ref().unwrap().root)
    }

    fn free_tree_pages(&mut self, root: PageId) -> Result<()> {
        let mut stack = vec![root];
        while let Some(current) = stack.pop() {
            if !self.pages.contains_key(&current) {
                let page = self.manager.fetch_page_owned(current)?;
                self.pages.insert(current, page);
            }
            let page = self.pages.get(&current).unwrap();
            match page.page_type() {
                Some(PageType::Branch) => {
                    for i in 0..page.num_cells() as usize {
                        stack.push(branch_node::get_child(page, i));
                    }
                    let right = page.right_child();
                    if right.is_valid() {
                        stack.push(right);
                    }
                }
                Some(PageType::Leaf) => {}
                _ => {}
            }
            self.alloc.free(current);
        }
        Ok(())
    }

    fn count_leaf_entries(&mut self, root: PageId) -> Result<u64> {
        let mut count: u64 = 0;
        let mut stack = vec![root];
        while let Some(current) = stack.pop() {
            if !self.pages.contains_key(&current) {
                let page = self.manager.fetch_page_owned(current)?;
                self.pages.insert(current, page);
            }
            let page = self.pages.get(&current).unwrap();
            match page.page_type() {
                Some(PageType::Branch) => {
                    for i in 0..page.num_cells() as usize {
                        stack.push(branch_node::get_child(page, i));
                    }
                    let right = page.right_child();
                    if right.is_valid() {
                        stack.push(right);
                    }
                }
                Some(PageType::Leaf) => {
                    count += page.num_cells() as u64;
                }
                _ => {}
            }
        }
        Ok(count)
    }

    fn preload_path(&mut self, root: PageId, key: &[u8]) -> Result<()> {
        let mut current = root;
        loop {
            if !self.pages.contains_key(&current) {
                let page = self.manager.fetch_page_owned(current)?;
                self.pages.insert(current, page);
            }
            let page = self.pages.get(&current).unwrap();
            match page.page_type() {
                Some(PageType::Leaf) => return Ok(()),
                Some(PageType::Branch) => {
                    let idx = branch_node::search_child_index(page, key);
                    current = branch_node::get_child(page, idx);
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }
    }

    fn preload_all_pages(&mut self, root: PageId) -> Result<()> {
        let mut stack = vec![root];
        while let Some(current) = stack.pop() {
            if !self.pages.contains_key(&current) {
                let page = self.manager.fetch_page_owned(current)?;
                self.pages.insert(current, page);
            }
            let page = self.pages.get(&current).unwrap();
            match page.page_type() {
                Some(PageType::Branch) => {
                    let num_cells = page.num_cells() as usize;
                    for i in 0..num_cells {
                        stack.push(branch_node::get_child(page, i));
                    }
                    let right = page.right_child();
                    if right.is_valid() {
                        stack.push(right);
                    }
                }
                Some(PageType::Leaf) => {}
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }
        Ok(())
    }
}

impl<'a> Drop for WriteTxn<'a> {
    fn drop(&mut self) {
        if !self.committed {
            self.manager.abort_write();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::manager::tests::create_test_manager;

    #[test]
    fn insert_and_get() {
        let mgr = create_test_manager();

        let mut wtx = mgr.begin_write().unwrap();
        assert!(wtx.insert(b"key1", b"val1").unwrap());
        assert_eq!(wtx.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(wtx.get(b"missing").unwrap(), None);
        wtx.commit().unwrap();
    }

    #[test]
    fn insert_update() {
        let mgr = create_test_manager();

        let mut wtx = mgr.begin_write().unwrap();
        assert!(wtx.insert(b"key", b"v1").unwrap()); // new
        assert!(!wtx.insert(b"key", b"v2").unwrap()); // update
        assert_eq!(wtx.get(b"key").unwrap(), Some(b"v2".to_vec()));
        wtx.commit().unwrap();

        // Read back
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"key").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_key() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"a", b"1").unwrap();
            wtx.insert(b"b", b"2").unwrap();
            wtx.commit().unwrap();
        }

        {
            let mut wtx = mgr.begin_write().unwrap();
            assert!(wtx.delete(b"a").unwrap());
            assert!(!wtx.delete(b"nonexistent").unwrap());
            wtx.commit().unwrap();
        }

        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"a").unwrap(), None);
        assert_eq!(rtx.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn abort_discards_changes() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"key", b"value").unwrap();
            wtx.abort();
        }

        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"key").unwrap(), None);
    }

    #[test]
    fn snapshot_and_restore_main_tree() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        wtx.set_in_place(false);

        wtx.insert(b"a", b"1").unwrap();
        wtx.insert(b"b", b"2").unwrap();
        let snap = wtx.begin_savepoint();

        wtx.insert(b"c", b"3").unwrap();
        wtx.delete(b"a").unwrap();
        assert_eq!(wtx.get(b"c").unwrap(), Some(b"3".to_vec()));
        assert_eq!(wtx.get(b"a").unwrap(), None);

        wtx.restore_snapshot(snap);

        assert_eq!(wtx.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(wtx.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(wtx.get(b"c").unwrap(), None);

        wtx.commit().unwrap();
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(rtx.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(rtx.get(b"c").unwrap(), None);
    }

    #[test]
    fn snapshot_reusable_across_multiple_restores() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        wtx.set_in_place(false);

        wtx.insert(b"base", b"v").unwrap();
        let snap = wtx.begin_savepoint();

        for i in 0..5 {
            let k = format!("k{i}");
            wtx.insert(k.as_bytes(), b"x").unwrap();
            wtx.restore_snapshot(snap.clone());
            assert_eq!(wtx.get(k.as_bytes()).unwrap(), None);
        }
        assert_eq!(wtx.get(b"base").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn snapshot_restores_named_tables() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        wtx.set_in_place(false);

        wtx.create_table(b"t1").unwrap();
        wtx.table_insert(b"t1", b"k1", b"v1").unwrap();
        let snap = wtx.begin_savepoint();

        wtx.create_table(b"t2").unwrap();
        wtx.table_insert(b"t1", b"k2", b"v2").unwrap();
        wtx.table_insert(b"t2", b"k", b"v").unwrap();

        wtx.restore_snapshot(snap);

        assert_eq!(wtx.table_get(b"t1", b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(wtx.table_get(b"t1", b"k2").unwrap(), None);
        let err = wtx.table_get(b"t2", b"k").unwrap_err();
        assert!(matches!(err, citadel_core::Error::TableNotFound(_)));
    }

    #[test]
    fn snapshot_drops_post_snapshot_pages() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        wtx.set_in_place(false);

        for i in 0..20u32 {
            let k = format!("k{i:03}");
            wtx.insert(k.as_bytes(), b"x").unwrap();
        }
        let snap = wtx.begin_savepoint();
        let cutoff = snap.txn_id;

        for i in 20..200u32 {
            let k = format!("k{i:03}");
            wtx.insert(k.as_bytes(), b"x").unwrap();
        }

        wtx.restore_snapshot(snap);
        for page in wtx.pages.values() {
            assert!(page.txn_id() <= cutoff);
        }
    }

    #[test]
    fn nested_savepoints_rollback_inner() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        wtx.set_in_place(false);

        wtx.insert(b"a", b"1").unwrap();
        let outer = wtx.begin_savepoint();
        wtx.insert(b"b", b"2").unwrap();
        let inner = wtx.begin_savepoint();
        wtx.insert(b"c", b"3").unwrap();

        wtx.restore_snapshot(inner);
        assert_eq!(wtx.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(wtx.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(wtx.get(b"c").unwrap(), None);

        wtx.restore_snapshot(outer);
        assert_eq!(wtx.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(wtx.get(b"b").unwrap(), None);
    }

    #[test]
    fn in_place_toggle_helpers() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        let original = wtx.in_place();
        wtx.set_in_place(!original);
        assert_eq!(wtx.in_place(), !original);
        wtx.set_in_place(original);
        assert_eq!(wtx.in_place(), original);
    }

    #[test]
    fn base_txn_id_stays_fixed_across_savepoints() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        let base = wtx.base_txn_id();
        assert_eq!(wtx.txn_id, base);
        let _snap = wtx.begin_savepoint();
        assert!(wtx.txn_id.as_u64() > base.as_u64());
        assert_eq!(wtx.base_txn_id(), base);
    }

    #[test]
    fn drop_without_commit_aborts() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"key", b"value").unwrap();
            // Dropped without commit
        }

        // Writer should be released
        let _wtx2 = mgr.begin_write().unwrap();

        // Data should not be visible
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"key").unwrap(), None);
    }

    #[test]
    fn many_inserts_commit() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            for i in 0..500u32 {
                let key = format!("key-{i:05}");
                let val = format!("val-{i:05}");
                wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
            }
            assert_eq!(wtx.entry_count(), 500);
            wtx.commit().unwrap();
        }

        // Read all back
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.entry_count(), 500);
        for i in 0..500u32 {
            let key = format!("key-{i:05}");
            let val = format!("val-{i:05}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }
    }

    #[test]
    fn multiple_transactions() {
        let mgr = create_test_manager();

        // Txn 1: insert keys
        {
            let mut wtx = mgr.begin_write().unwrap();
            for i in 0..10u32 {
                let key = format!("k{i}");
                wtx.insert(key.as_bytes(), b"v1").unwrap();
            }
            wtx.commit().unwrap();
        }

        // Txn 2: update some, delete some
        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"k0", b"updated").unwrap();
            wtx.delete(b"k5").unwrap();
            wtx.commit().unwrap();
        }

        // Verify
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"k0").unwrap(), Some(b"updated".to_vec()));
        assert_eq!(rtx.get(b"k5").unwrap(), None);
        assert_eq!(rtx.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn key_too_large() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        let big_key = vec![0u8; MAX_KEY_SIZE + 1];
        assert!(matches!(
            wtx.insert(&big_key, b"val"),
            Err(citadel_core::Error::KeyTooLarge { .. })
        ));
    }

    #[test]
    fn value_too_large() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        let big_val = vec![0u8; MAX_INLINE_VALUE_SIZE + 1];
        assert!(matches!(
            wtx.insert(b"key", &big_val),
            Err(citadel_core::Error::ValueTooLarge { .. })
        ));
    }

    #[test]
    fn commit_updates_slot() {
        let mgr = create_test_manager();

        let slot_before = mgr.current_slot();
        assert_eq!(slot_before.tree_entries, 0);

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"key", b"val").unwrap();
            wtx.commit().unwrap();
        }

        let slot_after = mgr.current_slot();
        assert_eq!(slot_after.tree_entries, 1);
        assert!(slot_after.txn_id.as_u64() > slot_before.txn_id.as_u64());
        assert_ne!(slot_after.tree_root, slot_before.tree_root);
    }

    #[test]
    fn create_table_and_insert() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.create_table(b"users").unwrap();
            wtx.table_insert(b"users", b"alice", b"admin").unwrap();
            wtx.table_insert(b"users", b"bob", b"user").unwrap();
            wtx.commit().unwrap();
        }

        // Default table should be unaffected
        let rtx = mgr.begin_read();
        assert_eq!(rtx.entry_count(), 0);
    }

    #[test]
    fn table_not_found() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        assert!(matches!(
            wtx.table_insert(b"nonexistent", b"k", b"v"),
            Err(citadel_core::Error::TableNotFound(_))
        ));
    }

    #[test]
    fn table_already_exists() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"test").unwrap();
        assert!(matches!(
            wtx.create_table(b"test"),
            Err(citadel_core::Error::TableAlreadyExists(_))
        ));
    }

    #[test]
    fn table_for_each_named() {
        let mgr = create_test_manager();

        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"data").unwrap();
        wtx.table_insert(b"data", b"b", b"2").unwrap();
        wtx.table_insert(b"data", b"a", b"1").unwrap();
        wtx.table_insert(b"data", b"c", b"3").unwrap();

        let mut pairs = Vec::new();
        wtx.table_for_each(b"data", |k, v| {
            pairs.push((k.to_vec(), v.to_vec()));
            Ok(())
        })
        .unwrap();

        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(pairs[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(pairs[2], (b"c".to_vec(), b"3".to_vec()));
        wtx.commit().unwrap();
    }

    use citadel_core::MAX_INLINE_VALUE_SIZE;
    use citadel_core::MAX_KEY_SIZE;
}
