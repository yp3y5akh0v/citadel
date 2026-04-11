//! Read transaction: snapshot isolation via MVCC.
//!
//! Snapshots the active commit slot at creation time.
//! Reads pages through the buffer pool (cached, decrypted).
//! Auto-unregisters from the reader table on Drop (RAII).

use std::collections::HashMap;
use std::sync::Arc;

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result};
use citadel_io::file_manager::CommitSlot;
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};

use citadel_buffer::cursor::Cursor;

use crate::catalog::TableDescriptor;
use crate::manager::TxnManager;

/// A read-only transaction with snapshot isolation.
///
/// Reads from the B+ tree root captured at transaction start.
/// Multiple ReadTxns can coexist with each other and with a WriteTxn.
pub struct ReadTxn<'a> {
    manager: &'a TxnManager,
    txn_id: TxnId,
    snapshot: CommitSlot,
    page_cache: HashMap<PageId, Arc<Page>>,
}

impl<'a> ReadTxn<'a> {
    pub(crate) fn new(manager: &'a TxnManager, txn_id: TxnId, snapshot: CommitSlot) -> Self {
        Self {
            manager,
            txn_id,
            snapshot,
            page_cache: HashMap::new(),
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

    // ── Default table operations ──────────────────────────────────────

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

    // ── Named table operations ────────────────────────────────────────

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

    /// Seek to `start_key` in a named table and iterate forward.
    /// The callback returns `true` to continue or `false` to stop.
    pub fn table_scan_from<F>(&mut self, table: &[u8], start_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let desc = self.lookup_table(table)?;
        self.preload_all_pages(desc.root_page)?;
        let mut cursor = if start_key.is_empty() {
            Cursor::first(&self.page_cache, desc.root_page)?
        } else {
            Cursor::seek(&self.page_cache, desc.root_page, start_key)?
        };
        while cursor.is_valid() {
            if let Some(entry) = cursor.current_ref(&self.page_cache) {
                if entry.val_type != ValueType::Tombstone && !f(entry.key, entry.value)? {
                    break;
                }
            }
            cursor.next(&self.page_cache)?;
        }
        Ok(())
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

    // ── Internal helpers ──────────────────────────────────────────────

    /// Look up a table descriptor in the catalog.
    fn lookup_table(&mut self, name: &[u8]) -> Result<TableDescriptor> {
        let catalog_root = self.snapshot.catalog_root;
        if !catalog_root.is_valid() {
            return Err(Error::TableNotFound(
                String::from_utf8_lossy(name).into_owned(),
            ));
        }

        // Search the catalog B+ tree for the table name
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

impl<'a> Drop for ReadTxn<'a> {
    fn drop(&mut self) {
        self.manager.unregister_reader(self.txn_id);
    }
}

#[cfg(test)]
mod tests {
    use crate::manager::tests::create_test_manager;

    #[test]
    fn read_empty_tree() {
        let mgr = create_test_manager();
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.entry_count(), 0);
        assert_eq!(rtx.get(b"anything").unwrap(), None);
    }

    #[test]
    fn read_after_write_commit() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"hello", b"world").unwrap();
            wtx.commit().unwrap();
        }

        {
            let mut rtx = mgr.begin_read();
            assert_eq!(rtx.get(b"hello").unwrap(), Some(b"world".to_vec()));
            assert_eq!(rtx.get(b"missing").unwrap(), None);
            assert_eq!(rtx.entry_count(), 1);
        }
    }

    #[test]
    fn snapshot_isolation() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"key1", b"v1").unwrap();
            wtx.commit().unwrap();
        }

        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"key1").unwrap(), Some(b"v1".to_vec()));

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"key2", b"v2").unwrap();
            wtx.commit().unwrap();
        }

        assert_eq!(rtx.get(b"key2").unwrap(), None);

        let mut rtx2 = mgr.begin_read();
        assert_eq!(rtx2.get(b"key1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(rtx2.get(b"key2").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn contains_key() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"exists", b"yes").unwrap();
            wtx.commit().unwrap();
        }

        let mut rtx = mgr.begin_read();
        assert!(rtx.contains_key(b"exists").unwrap());
        assert!(!rtx.contains_key(b"nope").unwrap());
    }

    #[test]
    fn read_named_table() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.create_table(b"mydata").unwrap();
            wtx.table_insert(b"mydata", b"hello", b"world").unwrap();
            wtx.commit().unwrap();
        }

        let mut rtx = mgr.begin_read();
        assert_eq!(
            rtx.table_get(b"mydata", b"hello").unwrap(),
            Some(b"world".to_vec())
        );
        assert_eq!(rtx.table_get(b"mydata", b"missing").unwrap(), None);
    }

    #[test]
    fn read_nonexistent_table() {
        let mgr = create_test_manager();
        let mut rtx = mgr.begin_read();
        assert!(matches!(
            rtx.table_get(b"nope", b"key"),
            Err(citadel_core::Error::TableNotFound(_))
        ));
    }

    #[test]
    fn for_each_default_table() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"c", b"3").unwrap();
            wtx.insert(b"a", b"1").unwrap();
            wtx.insert(b"b", b"2").unwrap();
            wtx.commit().unwrap();
        }

        let mut rtx = mgr.begin_read();
        let mut pairs = Vec::new();
        rtx.for_each(|k, v| {
            pairs.push((k.to_vec(), v.to_vec()));
            Ok(())
        })
        .unwrap();

        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(pairs[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(pairs[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn for_each_empty_table() {
        let mgr = create_test_manager();
        let mut rtx = mgr.begin_read();
        let mut count = 0;
        rtx.for_each(|_, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn table_for_each_named_table() {
        let mgr = create_test_manager();

        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.create_table(b"items").unwrap();
            wtx.table_insert(b"items", b"x", b"10").unwrap();
            wtx.table_insert(b"items", b"y", b"20").unwrap();
            wtx.table_insert(b"items", b"z", b"30").unwrap();
            wtx.commit().unwrap();
        }

        let mut rtx = mgr.begin_read();
        let mut pairs = Vec::new();
        rtx.table_for_each(b"items", |k, v| {
            pairs.push((k.to_vec(), v.to_vec()));
            Ok(())
        })
        .unwrap();

        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (b"x".to_vec(), b"10".to_vec()));
        assert_eq!(pairs[1], (b"y".to_vec(), b"20".to_vec()));
        assert_eq!(pairs[2], (b"z".to_vec(), b"30".to_vec()));
    }
}
