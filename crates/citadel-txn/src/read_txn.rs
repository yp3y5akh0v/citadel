//! Read transaction: MVCC snapshot isolation. RAII reader registration.

use rustc_hash::FxHashMap;
use std::sync::Arc;

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result};
use citadel_io::file_manager::CommitSlot;
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};

use citadel_buffer::cursor::{Cursor, PageLoader, PageMap};

use crate::catalog::TableDescriptor;
use crate::manager::TxnManager;

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
    page_cache: FxHashMap<PageId, Arc<Page>>,
}

impl<'db> ReadTxn<'db> {
    pub(crate) fn new(manager: &'db TxnManager, txn_id: TxnId, snapshot: CommitSlot) -> Self {
        Self {
            manager,
            txn_id,
            snapshot,
            page_cache: FxHashMap::default(),
        }
    }

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    pub fn root(&self) -> PageId {
        self.snapshot.tree_root
    }

    pub fn entry_count(&self) -> u64 {
        self.snapshot.tree_entries
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
        self.preload_all_pages(self.snapshot.tree_root)?;
        let mut cursor = Cursor::first(&self.page_cache, self.snapshot.tree_root)?;
        while cursor.is_valid() {
            if let Some(entry) = cursor.current_ref(&self.page_cache) {
                if entry.val_type != ValueType::Tombstone {
                    f(entry.key, entry.value)?;
                }
            }
            cursor.next(&self.page_cache)?;
        }
        Ok(())
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
            if let Some(entry) = cursor.current_ref(&self.page_cache) {
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
    ///
    /// Useful when the caller needs the iterator to outlive a borrow scope —
    /// the txn's snapshot is pinned for the iterator's lifetime.
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
        self.preload_all_pages(desc.root_page)?;
        let leaves = self.collect_leaves_ordered(desc.root_page)?;
        for page in &leaves {
            let n = page.num_cells();
            for i in 0..n {
                let cell = leaf_node::read_cell(page, i);
                if cell.val_type != ValueType::Tombstone && !f(cell.key, cell.value) {
                    return Ok(());
                }
            }
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
        loop {
            let page = self.load_page(current)?;
            match page.page_type() {
                Some(PageType::Leaf) => {
                    return match leaf_node::search(page, key) {
                        Ok(idx) => {
                            let cell = leaf_node::read_cell(page, idx);
                            match cell.val_type {
                                ValueType::Tombstone => Ok(None),
                                _ => Ok(Some(cell.value.to_vec())),
                            }
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

    fn load_page(&mut self, page_id: PageId) -> Result<&Page> {
        if !self.page_cache.contains_key(&page_id) {
            let arc = self.manager.fetch_page(page_id)?;
            self.page_cache.insert(page_id, arc);
        }
        Ok(self.page_cache.get(&page_id).unwrap())
    }

    fn collect_leaves_ordered(&self, root: PageId) -> Result<Vec<Arc<Page>>> {
        let mut leaves = Vec::new();
        self.collect_leaves_recursive(root, &mut leaves)?;
        Ok(leaves)
    }

    fn collect_leaves_recursive(&self, page_id: PageId, leaves: &mut Vec<Arc<Page>>) -> Result<()> {
        let page = self
            .page_cache
            .get(&page_id)
            .ok_or(Error::PageOutOfBounds(page_id))?;
        match page.page_type() {
            Some(PageType::Leaf) => {
                leaves.push(Arc::clone(page));
            }
            Some(PageType::Branch) => {
                let n = page.num_cells() as usize;
                for i in 0..n {
                    let child = branch_node::get_child(page, i);
                    self.collect_leaves_recursive(child, leaves)?;
                }
                let right = page.right_child();
                if right.is_valid() {
                    self.collect_leaves_recursive(right, leaves)?;
                }
            }
            _ => return Err(Error::InvalidPageType(page.page_type_raw(), page_id)),
        }
        Ok(())
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
