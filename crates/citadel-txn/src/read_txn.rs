//! Read transaction: snapshot isolation via MVCC.
//!
//! Snapshots the active commit slot at creation time.
//! Reads pages through the buffer pool (cached, decrypted).
//! Auto-unregisters from the reader table on Drop (RAII).

use std::collections::HashMap;
use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result};
use citadel_io::file_manager::CommitSlot;
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};

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
    page_cache: HashMap<PageId, Page>,
}

impl<'a> ReadTxn<'a> {
    pub(crate) fn new(
        manager: &'a TxnManager,
        txn_id: TxnId,
        snapshot: CommitSlot,
    ) -> Self {
        Self {
            manager,
            txn_id,
            snapshot,
            page_cache: HashMap::new(),
        }
    }

    /// Get the transaction ID.
    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    /// Get the snapshot's tree root.
    pub fn root(&self) -> PageId {
        self.snapshot.tree_root
    }

    /// Get the snapshot's entry count for the default table.
    pub fn entry_count(&self) -> u64 {
        self.snapshot.tree_entries
    }

    // ── Default table operations ──────────────────────────────────────

    /// Look up a key in the default table.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.search_tree(self.snapshot.tree_root, key)
    }

    /// Check if a key exists in the default table.
    pub fn contains_key(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    // ── Named table operations ────────────────────────────────────────

    /// Look up a key in a named table.
    pub fn table_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        let desc = self.lookup_table(table)?;
        self.search_tree(desc.root_page, key)
    }

    /// Check if a key exists in a named table.
    pub fn table_contains_key(&mut self, table: &[u8], key: &[u8]) -> Result<bool> {
        Ok(self.table_get(table, key)?.is_some())
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
                                Ok(TableDescriptor::deserialize(&cell.value))
                            }
                        }
                        Err(_) => Err(Error::TableNotFound(
                            String::from_utf8_lossy(name).into_owned(),
                        )),
                    };
                }
                Some(PageType::Branch) => {
                    current = branch_node::search(page, name);
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
                    current = branch_node::search(page, key);
                }
                _ => {
                    return Err(Error::InvalidPageType(page.page_type_raw(), current));
                }
            }
        }
    }

    fn load_page(&mut self, page_id: PageId) -> Result<&Page> {
        if !self.page_cache.contains_key(&page_id) {
            let page = self.manager.fetch_page(page_id)?;
            self.page_cache.insert(page_id, page);
        }
        Ok(self.page_cache.get(&page_id).unwrap())
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
}
