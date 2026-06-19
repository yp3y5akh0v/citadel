//! Read transaction: MVCC snapshot isolation. RAII reader registration.

use rustc_hash::FxHashMap;
use std::sync::Arc;

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result};
use citadel_io::file_manager::CommitSlot;
use citadel_page::leaf_node::OverflowRef;
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};

use citadel_buffer::cursor::{Cursor, PageLoader, PageMap};

use crate::catalog::TableDescriptor;
use crate::manager::TxnManager;
use crate::overflow_io;

struct ReadPages<'a> {
    cache: &'a mut FxHashMap<PageId, Arc<Page>>,
    manager: &'a TxnManager,
}

impl PageMap for ReadPages<'_> {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.cache.get(id).map(|a| a.as_ref())
    }
}

impl PageLoader for ReadPages<'_> {
    fn ensure_loaded(&mut self, id: PageId) -> Result<()> {
        if !self.cache.contains_key(&id) {
            let arc = self.manager.fetch_page(id)?;
            self.cache.insert(id, arc);
        }
        Ok(())
    }
}

/// Read-only transaction with snapshot isolation.
pub struct ReadTxn<'a> {
    manager: &'a TxnManager,
    txn_id: TxnId,
    snapshot: CommitSlot,
    commit_generation: u64,
    page_cache: FxHashMap<PageId, Arc<Page>>,
}

impl<'db> ReadTxn<'db> {
    pub(crate) fn new(
        manager: &'db TxnManager,
        txn_id: TxnId,
        snapshot: CommitSlot,
        commit_generation: u64,
    ) -> Self {
        Self {
            manager,
            txn_id,
            snapshot,
            commit_generation,
            page_cache: FxHashMap::default(),
        }
    }

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    pub fn commit_generation(&self) -> u64 {
        self.commit_generation
    }

    pub fn root(&self) -> PageId {
        self.snapshot.tree_root
    }

    pub fn entry_count(&self) -> u64 {
        self.snapshot.tree_entries
    }

    /// The table's catalog root in this txn (a lookup, no scan); a version stamp.
    pub fn table_root_page(&self, table: &[u8]) -> Result<Option<PageId>> {
        self.manager.table_root(table)
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.search_tree(self.snapshot.tree_root, key)
    }

    pub fn contains_key(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    pub fn for_each<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        let root = self.snapshot.tree_root;
        self.preload_all_pages(root)?;
        let mut cursor = Cursor::first(&self.page_cache, root)?;
        while cursor.is_valid() {
            let overflow = cursor
                .current_ref(&self.page_cache)
                .and_then(|c| match c.val_type {
                    ValueType::Overflow => Some((c.key.to_vec(), OverflowRef::from_bytes(c.value))),
                    _ => None,
                });
            if let Some((key, oref)) = overflow {
                let materialized = self.materialize_overflow(&oref)?;
                f(&key, &materialized)?;
            } else if let Some(entry) = cursor.current_ref(&self.page_cache) {
                if entry.val_type != ValueType::Tombstone {
                    f(entry.key, entry.value)?;
                }
            }
            cursor.next(&self.page_cache)?;
        }
        Ok(())
    }

    fn materialize_overflow(&mut self, oref: &OverflowRef) -> Result<Vec<u8>> {
        let mut view = ReadPages {
            cache: &mut self.page_cache,
            manager: self.manager,
        };
        overflow_io::read_chain_value(&mut view, oref)
    }

    pub fn table_entry_count(&mut self, table: &[u8]) -> Result<u64> {
        Ok(self.lookup_table(table)?.entry_count)
    }

    pub fn table_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        let desc = self.lookup_table(table)?;
        self.search_tree(desc.root_page, key)
    }

    pub fn table_contains_key(&mut self, table: &[u8], key: &[u8]) -> Result<bool> {
        Ok(self.table_get(table, key)?.is_some())
    }

    pub fn table_for_each<F>(&mut self, table: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        let desc = self.lookup_table(table)?;
        self.preload_all_pages(desc.root_page)?;
        let mut cursor = Cursor::first(&self.page_cache, desc.root_page)?;
        while cursor.is_valid() {
            let overflow = cursor
                .current_ref(&self.page_cache)
                .and_then(|c| match c.val_type {
                    ValueType::Overflow => Some((c.key.to_vec(), OverflowRef::from_bytes(c.value))),
                    _ => None,
                });
            if let Some((key, oref)) = overflow {
                let materialized = self.materialize_overflow(&oref)?;
                f(&key, &materialized)?;
            } else if let Some(entry) = cursor.current_ref(&self.page_cache) {
                if entry.val_type != ValueType::Tombstone {
                    f(entry.key, entry.value)?;
                }
            }
            cursor.next(&self.page_cache)?;
        }
        Ok(())
    }

    /// Lazy scan from `start_key`. Callback returns `false` to stop.
    pub fn table_scan_from<F>(&mut self, table: &[u8], start_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let desc = self.lookup_table(table)?;
        let root = desc.root_page;
        let mut view = ReadPages {
            cache: &mut self.page_cache,
            manager: self.manager,
        };
        let mut cursor = Cursor::seek_lazy(&mut view, root, start_key)?;
        while let Some(c) = cursor.current_ref_lazy(&mut view) {
            let kind = c.val_type;
            match kind {
                ValueType::Tombstone => {}
                ValueType::Inline => {
                    let entry = cursor.current_ref_lazy(&mut view).unwrap();
                    if !f(entry.key, entry.value)? {
                        break;
                    }
                }
                ValueType::Overflow => {
                    let (key, oref) = {
                        let c = cursor.current_ref_lazy(&mut view).unwrap();
                        (c.key.to_vec(), OverflowRef::from_bytes(c.value))
                    };
                    let materialized = overflow_io::read_chain_value(&mut view, &oref)?;
                    if !f(&key, &materialized)? {
                        break;
                    }
                }
            }
            cursor.next_lazy(&mut view)?;
        }
        Ok(())
    }

    pub fn table_scan_from_fast<F>(
        &mut self,
        table: &[u8],
        start_key: &[u8],
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let desc = self.lookup_table(table)?;
        let root = desc.root_page;
        let mut view = ReadPages {
            cache: &mut self.page_cache,
            manager: self.manager,
        };
        let mut cursor = Cursor::seek_lazy(&mut view, root, start_key)?;
        if !cursor.is_valid() {
            return Ok(());
        }
        loop {
            view.ensure_loaded(cursor.leaf_page_id())?;
            let leaf_page = view
                .get_page(&cursor.leaf_page_id())
                .ok_or(Error::PageOutOfBounds(cursor.leaf_page_id()))?
                .clone();
            let n = leaf_page.num_cells();
            let mut idx = cursor.cell_index();
            while idx < n {
                let cell = leaf_node::read_cell(&leaf_page, idx);
                let continue_scan = match cell.val_type {
                    ValueType::Tombstone => true,
                    ValueType::Inline => f(cell.key, cell.value)?,
                    ValueType::Overflow => {
                        let oref = OverflowRef::from_bytes(cell.value);
                        let key_owned = cell.key.to_vec();
                        let materialized = overflow_io::read_chain_value(&mut view, &oref)?;
                        f(&key_owned, &materialized)?
                    }
                };
                if !continue_scan {
                    return Ok(());
                }
                idx += 1;
            }
            cursor.set_cell_index(n);
            if !cursor.advance_to_next_leaf(&mut view)? {
                break;
            }
        }
        Ok(())
    }

    /// Pull-based scan from `start_key`. Returns a lending iterator.
    pub fn table_scan_iter<'a>(
        &'a mut self,
        table: &[u8],
        start_key: &[u8],
    ) -> Result<crate::scan_iter::TableIter<ReadTxnScanAdapter<'a, 'db>>> {
        let desc = self.lookup_table(table)?;
        let root = desc.root_page;
        let cursor = {
            let mut view = ReadPages {
                cache: &mut self.page_cache,
                manager: self.manager,
            };
            Cursor::seek_lazy(&mut view, root, start_key)?
        };
        let adapter = ReadTxnScanAdapter { txn: self };
        Ok(crate::scan_iter::TableIter::new(adapter, cursor))
    }

    /// Consume self and return a lending iterator that owns the read txn.
    pub fn into_table_scan_iter(
        mut self,
        table: &[u8],
        start_key: &[u8],
    ) -> Result<crate::scan_iter::TableIter<OwnedReadTxnAdapter<'db>>> {
        let desc = self.lookup_table(table)?;
        let root = desc.root_page;
        let cursor = {
            let mut view = ReadPages {
                cache: &mut self.page_cache,
                manager: self.manager,
            };
            Cursor::seek_lazy(&mut view, root, start_key)?
        };
        let adapter = OwnedReadTxnAdapter { txn: self };
        Ok(crate::scan_iter::TableIter::new(adapter, cursor))
    }

    /// Full table scan via direct leaf iteration. Callback returns `false` to stop.
    pub fn table_scan_raw<F>(&mut self, table: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        let desc = self.lookup_table(table)?;
        let mut leaves = Vec::new();
        self.load_and_collect_leaves(desc.root_page, &mut leaves)?;
        for page in &leaves {
            let n = page.num_cells();
            for i in 0..n {
                let cell = leaf_node::read_cell(page, i);
                match cell.val_type {
                    ValueType::Tombstone => continue,
                    ValueType::Inline => {
                        if !f(cell.key, cell.value) {
                            return Ok(());
                        }
                    }
                    ValueType::Overflow => {
                        let oref = OverflowRef::from_bytes(cell.value);
                        let key_owned = cell.key.to_vec();
                        let mut view = ReadPages {
                            cache: &mut self.page_cache,
                            manager: self.manager,
                        };
                        let materialized = overflow_io::read_chain_value(&mut view, &oref)?;
                        if !f(&key_owned, &materialized) {
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// DFS pass that loads each page into the cache and collects leaves in left-to-right order.
    fn load_and_collect_leaves(
        &mut self,
        page_id: PageId,
        leaves: &mut Vec<Arc<Page>>,
    ) -> Result<()> {
        let page = if let Some(p) = self.page_cache.get(&page_id) {
            Arc::clone(p)
        } else {
            let arc = self.manager.fetch_page(page_id)?;
            self.page_cache.insert(page_id, Arc::clone(&arc));
            arc
        };
        match page.page_type() {
            Some(PageType::Leaf) => {
                leaves.push(page);
            }
            Some(PageType::Branch) => {
                let n = page.num_cells() as usize;
                for i in 0..n {
                    let child = branch_node::get_child(&page, i);
                    self.load_and_collect_leaves(child, leaves)?;
                }
                let right = page.right_child();
                if right.is_valid() {
                    self.load_and_collect_leaves(right, leaves)?;
                }
            }
            _ => return Err(Error::InvalidPageType(page.page_type_raw(), page_id)),
        }
        Ok(())
    }

    fn lookup_table(&mut self, name: &[u8]) -> Result<TableDescriptor> {
        if let Some((root, depth)) = self.snapshot.named_entry_root(name) {
            let entry_count = self.snapshot.named_entry_count(name).unwrap_or(0);
            return Ok(TableDescriptor {
                root_page: root,
                entry_count,
                depth,
                flags: 0,
            });
        }

        let catalog_root = self.snapshot.catalog_root;
        if !catalog_root.is_valid() {
            return Err(Error::TableNotFound(
                String::from_utf8_lossy(name).into_owned(),
            ));
        }

        let mut current = catalog_root;
        loop {
            let page = self.load_page(current)?;
            match page.page_type() {
                Some(PageType::Leaf) => {
                    return match leaf_node::search(page, name) {
                        Ok(idx) => {
                            let cell = leaf_node::read_cell(page, idx);
                            if cell.val_type == ValueType::Tombstone {
                                Err(Error::TableNotFound(
                                    String::from_utf8_lossy(name).into_owned(),
                                ))
                            } else {
                                Ok(TableDescriptor::deserialize(cell.value))
                            }
                        }
                        Err(_) => Err(Error::TableNotFound(
                            String::from_utf8_lossy(name).into_owned(),
                        )),
                    };
                }
                Some(PageType::Branch) => {
                    let idx = branch_node::search_child_index(page, name);
                    current = branch_node::get_child(page, idx);
                }
                _ => {
                    return Err(Error::InvalidPageType(page.page_type_raw(), current));
                }
            }
        }
    }

    /// Search for a key in an arbitrary B+ tree starting at `root`.
    fn search_tree(&mut self, root: PageId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut current = root;
        let snapshot: Option<(ValueType, Vec<u8>)> = loop {
            let page = self.load_page(current)?;
            match page.page_type() {
                Some(PageType::Leaf) => {
                    break match leaf_node::search(page, key) {
                        Ok(idx) => {
                            let cell = leaf_node::read_cell(page, idx);
                            match cell.val_type {
                                ValueType::Tombstone => None,
                                _ => Some((cell.val_type, cell.value.to_vec())),
                            }
                        }
                        Err(_) => None,
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
        };
        match snapshot {
            None => Ok(None),
            Some((ValueType::Overflow, payload)) => {
                let oref = OverflowRef::from_bytes(&payload);
                let mut view = ReadPages {
                    cache: &mut self.page_cache,
                    manager: self.manager,
                };
                overflow_io::read_chain_value(&mut view, &oref).map(Some)
            }
            Some((_, value)) => Ok(Some(value)),
        }
    }

    fn load_page(&mut self, page_id: PageId) -> Result<&Page> {
        if !self.page_cache.contains_key(&page_id) {
            let arc = self.manager.fetch_page(page_id)?;
            self.page_cache.insert(page_id, arc);
        }
        Ok(self.page_cache.get(&page_id).unwrap())
    }

    fn preload_all_pages(&mut self, root: PageId) -> Result<()> {
        let mut stack = vec![root];
        while let Some(current) = stack.pop() {
            if !self.page_cache.contains_key(&current) {
                let arc = self.manager.fetch_page(current)?;
                self.page_cache.insert(current, arc);
            }
            let page: &Page = self.page_cache.get(&current).unwrap();
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

impl<'db> Drop for ReadTxn<'db> {
    fn drop(&mut self) {
        self.manager.unregister_reader(self.txn_id);
    }
}

/// Scan adapter wrapping a `&mut ReadTxn` for use with [`crate::TableIter`].
pub struct ReadTxnScanAdapter<'a, 'db: 'a> {
    txn: &'a mut ReadTxn<'db>,
}

impl<'a, 'db: 'a> crate::scan_iter::TxnScanAdapter for ReadTxnScanAdapter<'a, 'db> {
    fn with_loader<R>(&mut self, f: &mut dyn FnMut(&mut dyn PageLoader) -> Result<R>) -> Result<R> {
        let mut view = ReadPages {
            cache: &mut self.txn.page_cache,
            manager: self.txn.manager,
        };
        f(&mut view)
    }
}

/// Scan adapter owning a `ReadTxn` for iterators that outlive a borrow scope.
pub struct OwnedReadTxnAdapter<'db> {
    txn: ReadTxn<'db>,
}

impl<'db> crate::scan_iter::TxnScanAdapter for OwnedReadTxnAdapter<'db> {
    fn with_loader<R>(&mut self, f: &mut dyn FnMut(&mut dyn PageLoader) -> Result<R>) -> Result<R> {
        let mut view = ReadPages {
            cache: &mut self.txn.page_cache,
            manager: self.txn.manager,
        };
        f(&mut view)
    }
}

#[cfg(test)]
#[path = "read_txn_tests.rs"]
mod tests;
