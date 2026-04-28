//! Write transaction: CoW mutations with shadow-paging commit.

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result, MAX_INLINE_VALUE_SIZE, MAX_KEY_SIZE};
use citadel_io::file_manager::CommitSlot;
use citadel_page::branch_node;
use citadel_page::page::Page;
use rustc_hash::FxHashMap;

use citadel_buffer::allocator::{AllocCheckpoint, PageAllocator};
use citadel_buffer::btree::{self, BTree, UpsertAction, UpsertOutcome};
use citadel_buffer::cursor::{Cursor, PageLoader, PageMap};

use crate::catalog::TableDescriptor;
use crate::manager::TxnManager;

#[derive(Debug, Clone)]
pub enum InsertOutcome {
    Inserted,
    Existed(Vec<u8>),
}

impl From<Option<Vec<u8>>> for InsertOutcome {
    fn from(v: Option<Vec<u8>>) -> Self {
        match v {
            None => InsertOutcome::Inserted,
            Some(existing) => InsertOutcome::Existed(existing),
        }
    }
}

struct WritePages<'a> {
    pages: &'a mut FxHashMap<PageId, Page>,
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
    pages: FxHashMap<PageId, Page>,
    tree: BTree,
    alloc: PageAllocator,
    committed: bool,
    deferred_free: Vec<PageId>,
    named_trees: FxHashMap<Vec<u8>, BTree>,
    catalog: Option<BTree>,
    catalog_dirty: bool,
    loaded_tree_meta: FxHashMap<Vec<u8>, (PageId, u16)>,
}

#[derive(Clone)]
pub struct WriteTxnSnapshot {
    tree: BTree,
    alloc_checkpoint: AllocCheckpoint,
    named_trees: FxHashMap<Vec<u8>, BTree>,
    catalog: Option<BTree>,
    catalog_dirty: bool,
    loaded_tree_meta: FxHashMap<Vec<u8>, (PageId, u16)>,
    deferred_free: Vec<PageId>,
}

impl<'db> WriteTxn<'db> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        manager: &'db TxnManager,
        txn_id: TxnId,
        snapshot: CommitSlot,
        tree: BTree,
        alloc: PageAllocator,
        deferred_free: Vec<PageId>,
        recycled_pages: Option<FxHashMap<PageId, Page>>,
        recycle_safe: bool,
    ) -> Self {
        let pages = match recycled_pages {
            Some(mut m) => {
                if !(alloc.in_place() && recycle_safe) {
                    m.clear();
                }
                m
            }
            None => FxHashMap::with_capacity_and_hasher(16, Default::default()),
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
            named_trees: FxHashMap::default(),
            catalog: None,
            catalog_dirty: false,
            loaded_tree_meta: FxHashMap::default(),
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

    /// Pull-based scan from `start_key`. Returns a lending iterator.
    pub fn table_scan_iter<'a>(
        &'a mut self,
        table: &[u8],
        start_key: &[u8],
    ) -> Result<crate::scan_iter::TableIter<WriteTxnScanAdapter<'a, 'db>>> {
        self.ensure_table(table)?;
        let root = self.named_trees[table].root;
        let cursor = {
            let mut view = WritePages {
                pages: &mut self.pages,
                manager: self.manager,
            };
            Cursor::seek_lazy(&mut view, root, start_key)?
        };
        let adapter = WriteTxnScanAdapter { txn: self };
        Ok(crate::scan_iter::TableIter::new(adapter, cursor))
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

    /// Rename a table in the catalog.
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

        if let Some(tree) = self.named_trees.get_mut(table) {
            if let Some(was_new) = tree.try_lil_insert(
                &mut self.pages,
                &mut self.alloc,
                self.txn_id,
                key,
                ValueType::Inline,
                value,
            )? {
                return Ok(was_new);
            }
            let root = tree.root;
            let (path, leaf_id) = Self::walk_loading(&mut self.pages, self.manager, root, key)?;
            return tree.insert_at_leaf(
                &mut self.pages,
                &mut self.alloc,
                self.txn_id,
                key,
                ValueType::Inline,
                value,
                path,
                leaf_id,
            );
        }
        self.ensure_table(table)?;
        let tree = self.named_trees.get_mut(table).unwrap();
        let root = tree.root;
        let (path, leaf_id) = Self::walk_loading(&mut self.pages, self.manager, root, key)?;
        tree.insert_at_leaf(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            key,
            ValueType::Inline,
            value,
            path,
            leaf_id,
        )
    }

    #[inline]
    pub fn table_insert_if_absent(
        &mut self,
        table: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<bool> {
        Self::validate_key_value(key, value)?;

        if let Some(tree) = self.named_trees.get_mut(table) {
            let root = tree.root;
            if tree.lil_would_hit(&self.pages, key) {
                return tree.insert_if_absent(
                    &mut self.pages,
                    &mut self.alloc,
                    self.txn_id,
                    key,
                    ValueType::Inline,
                    value,
                );
            }
            let (path, leaf_id) = Self::walk_loading(&mut self.pages, self.manager, root, key)?;
            return tree.insert_if_absent_at_leaf(
                &mut self.pages,
                &mut self.alloc,
                self.txn_id,
                key,
                ValueType::Inline,
                value,
                path,
                leaf_id,
            );
        }
        self.ensure_table(table)?;
        let tree = self.named_trees.get_mut(table).unwrap();
        let root = tree.root;
        let (path, leaf_id) = Self::walk_loading(&mut self.pages, self.manager, root, key)?;
        tree.insert_if_absent_at_leaf(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            key,
            ValueType::Inline,
            value,
            path,
            leaf_id,
        )
    }

    #[inline]
    pub fn table_upsert_with<F, E>(
        &mut self,
        table: &[u8],
        key: &[u8],
        default_value: &[u8],
        f: F,
    ) -> std::result::Result<UpsertOutcome, E>
    where
        F: FnMut(&[u8]) -> std::result::Result<UpsertAction, E>,
        E: From<Error>,
    {
        Self::validate_key_value(key, default_value)?;

        if let Some(tree) = self.named_trees.get_mut(table) {
            let root = tree.root;
            if tree.lil_would_hit(&self.pages, key) {
                return tree.upsert_with(
                    &mut self.pages,
                    &mut self.alloc,
                    self.txn_id,
                    key,
                    ValueType::Inline,
                    default_value,
                    f,
                );
            }
            let (path, leaf_id) = Self::walk_loading(&mut self.pages, self.manager, root, key)?;
            return tree.upsert_with_at_leaf(
                &mut self.pages,
                &mut self.alloc,
                self.txn_id,
                key,
                ValueType::Inline,
                default_value,
                path,
                leaf_id,
                f,
            );
        }
        self.ensure_table(table)?;
        let tree = self.named_trees.get_mut(table).unwrap();
        let root = tree.root;
        let (path, leaf_id) = Self::walk_loading(&mut self.pages, self.manager, root, key)?;
        tree.upsert_with_at_leaf(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            key,
            ValueType::Inline,
            default_value,
            path,
            leaf_id,
            f,
        )
    }

    pub fn table_insert_or_fetch(
        &mut self,
        table: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertOutcome> {
        Self::validate_key_value(key, value)?;

        if let Some(tree) = self.named_trees.get_mut(table) {
            let root = tree.root;
            let lil_hit = tree.lil_would_hit(&self.pages, key);
            if !lil_hit {
                Self::preload_path_raw(&mut self.pages, self.manager, root, key)?;
            }
            return tree
                .insert_or_fetch(
                    &mut self.pages,
                    &mut self.alloc,
                    self.txn_id,
                    key,
                    ValueType::Inline,
                    value,
                )
                .map(InsertOutcome::from);
        }
        self.ensure_table(table)?;
        let tree = self.named_trees.get_mut(table).unwrap();
        let root = tree.root;
        Self::preload_path_raw(&mut self.pages, self.manager, root, key)?;
        tree.insert_or_fetch(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            key,
            ValueType::Inline,
            value,
        )
        .map(InsertOutcome::from)
    }

    /// Batch-update existing keys. Keys must be sorted.
    pub fn table_update_sorted(&mut self, table: &[u8], pairs: &[(&[u8], &[u8])]) -> Result<u64> {
        if pairs.is_empty() {
            return Ok(0);
        }
        self.ensure_table(table)?;
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

        let mut view = WritePages {
            pages: &mut self.pages,
            manager: self.manager,
        };
        let mut cursor = Cursor::seek_lazy(&mut view, root, start_key)?;

        let mut count: u64 = 0;
        let mut cow_leaf = PageId::INVALID;

        while cursor.is_valid() {
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
                    cursor.set_leaf_page_id(new_id);
                }
                cow_leaf = new_id;
            }

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

    /// Drop all pages, reset to an empty leaf. Returns pre-truncation entry count.
    pub fn table_truncate(&mut self, table: &[u8]) -> Result<u64> {
        self.ensure_table(table)?;

        let old_tree = self.named_trees[table].clone();
        self.free_tree_pages(old_tree.root)?;

        let new_root = self.alloc.allocate();
        let mut leaf = Page::new(new_root, PageType::Leaf, self.txn_id);
        leaf.update_checksum();
        self.pages.insert(new_root, leaf);

        self.named_trees
            .insert(table.to_vec(), BTree::from_existing(new_root, 1, 0));
        Ok(old_tree.entry_count)
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
        let pre_savepoint_alloc_len = snap.alloc_checkpoint.allocated_this_txn_len();
        for &page_id in self.alloc.allocated_since(pre_savepoint_alloc_len) {
            self.pages.remove(&page_id);
        }
        self.tree = snap.tree;
        self.alloc.restore(snap.alloc_checkpoint);
        self.named_trees = snap.named_trees;
        self.catalog = snap.catalog;
        self.catalog_dirty = snap.catalog_dirty;
        self.loaded_tree_meta = snap.loaded_tree_meta;
        self.deferred_free = snap.deferred_free;
        self.txn_id = self.manager.next_write_txn_id();
    }

    fn capture_snapshot(&self) -> WriteTxnSnapshot {
        WriteTxnSnapshot {
            tree: self.tree.clone(),
            alloc_checkpoint: self.alloc.checkpoint(),
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

        if let Some((root, depth)) = self.old_slot.named_entry_root(name) {
            let entry_count = self.old_slot.named_entry_count(name).unwrap_or(0);
            let tree = BTree::from_existing(root, depth, entry_count);
            self.loaded_tree_meta.insert(name.to_vec(), (root, depth));
            self.named_trees.insert(name.to_vec(), tree);
            return Ok(());
        }

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
        Self::preload_path_raw(&mut self.pages, self.manager, root, key)
    }

    fn preload_path_raw(
        pages: &mut FxHashMap<PageId, Page>,
        manager: &TxnManager,
        root: PageId,
        key: &[u8],
    ) -> Result<()> {
        let mut current = root;
        loop {
            let page = match pages.entry(current) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let page = manager.fetch_page_owned(current)?;
                    e.insert(page)
                }
            };
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

    fn walk_loading(
        pages: &mut FxHashMap<PageId, Page>,
        manager: &TxnManager,
        root: PageId,
        key: &[u8],
    ) -> Result<(Vec<(PageId, usize)>, PageId)> {
        let mut path = Vec::new();
        let mut current = root;
        loop {
            let page = match pages.entry(current) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let page = manager.fetch_page_owned(current)?;
                    e.insert(page)
                }
            };
            match page.page_type() {
                Some(PageType::Leaf) => return Ok((path, current)),
                Some(PageType::Branch) => {
                    let idx = branch_node::search_child_index(page, key);
                    let child = branch_node::get_child(page, idx);
                    path.push((current, idx));
                    current = child;
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

impl<'db> Drop for WriteTxn<'db> {
    fn drop(&mut self) {
        if !self.committed {
            self.manager.abort_write();
        }
    }
}

/// Scan adapter wrapping a `&mut WriteTxn` for use with [`crate::TableIter`].
pub struct WriteTxnScanAdapter<'a, 'db: 'a> {
    txn: &'a mut WriteTxn<'db>,
}

impl<'a, 'db: 'a> crate::scan_iter::TxnScanAdapter for WriteTxnScanAdapter<'a, 'db> {
    fn with_loader<R>(&mut self, f: &mut dyn FnMut(&mut dyn PageLoader) -> Result<R>) -> Result<R> {
        let mut view = WritePages {
            pages: &mut self.txn.pages,
            manager: self.txn.manager,
        };
        f(&mut view)
    }
}

#[cfg(test)]
#[path = "write_txn_tests.rs"]
mod tests;
