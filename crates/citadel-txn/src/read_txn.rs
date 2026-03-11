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

use crate::manager::TxnManager;

/// A read-only transaction with snapshot isolation.
///
/// Reads from the B+ tree root captured at transaction start.
/// Multiple ReadTxns can coexist with each other and with a WriteTxn.
pub struct ReadTxn<'a> {
    manager: &'a TxnManager,
    txn_id: TxnId,
    snapshot: CommitSlot,
    /// Local page cache for this read transaction.
    /// Pages are cloned from the buffer pool on first access.
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

    /// Get the snapshot's entry count.
    pub fn entry_count(&self) -> u64 {
        self.snapshot.tree_entries
    }

    /// Look up a key. Returns the value if found.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut current = self.snapshot.tree_root;

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

    /// Check if a key exists.
    pub fn contains_key(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Load a page into the local cache and return a reference to it.
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

        // Write some data
        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"hello", b"world").unwrap();
            wtx.commit().unwrap();
        }

        // Read it back
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

        // Write initial data
        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"key1", b"v1").unwrap();
            wtx.commit().unwrap();
        }

        // Start a read — should see key1
        let mut rtx = mgr.begin_read();
        assert_eq!(rtx.get(b"key1").unwrap(), Some(b"v1".to_vec()));

        // Write more data after the read started
        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.insert(b"key2", b"v2").unwrap();
            wtx.commit().unwrap();
        }

        // The read should NOT see key2 (snapshot isolation)
        // Note: In our implementation, the snapshot is captured at begin_read time.
        // The read sees the tree root at that point. The write created a new root
        // via CoW, so the old root is unchanged.
        assert_eq!(rtx.get(b"key2").unwrap(), None);

        // A new read should see both
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
}
