//! Write transaction: CoW mutations with full commit protocol.
//!
//! Maintains a local HashMap of pages as the "write set."
//! Pages needed by the B+ tree are pre-loaded from disk before operations.
//! On commit, executes the 6-step god byte commit protocol.
//! On Drop without commit, automatically aborts.

use std::collections::HashMap;
use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result, MAX_KEY_SIZE, MAX_INLINE_VALUE_SIZE};
use citadel_io::file_manager::CommitSlot;
use citadel_page::page::Page;
use citadel_page::branch_node;

use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::btree::BTree;
use citadel_buffer::cursor::Cursor;

use crate::manager::TxnManager;

/// A read-write transaction.
///
/// Supports insert, delete, and get operations on the B+ tree.
/// Changes are buffered in memory until commit().
/// Abort on Drop if not committed.
pub struct WriteTxn<'a> {
    manager: &'a TxnManager,
    txn_id: TxnId,
    old_slot: CommitSlot,
    pages: HashMap<PageId, Page>,
    tree: BTree,
    alloc: PageAllocator,
    committed: bool,
    deferred_free: Vec<PageId>,
}

impl<'a> WriteTxn<'a> {
    pub(crate) fn new(
        manager: &'a TxnManager,
        txn_id: TxnId,
        snapshot: CommitSlot,
        tree: BTree,
        alloc: PageAllocator,
        deferred_free: Vec<PageId>,
    ) -> Self {
        Self {
            manager,
            txn_id,
            old_slot: snapshot,
            pages: HashMap::new(),
            tree,
            alloc,
            committed: false,
            deferred_free,
        }
    }

    /// Get the transaction ID.
    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    /// Get the current entry count.
    pub fn entry_count(&self) -> u64 {
        self.tree.entry_count
    }

    /// Look up a key within this write transaction.
    /// Sees both committed data and uncommitted changes.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.preload_path(self.tree.root, key)?;

        match self.tree.search(&self.pages, key)? {
            Some((ValueType::Tombstone, _)) => Ok(None),
            Some((_, value)) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    /// Insert a key-value pair. Returns true if the key is new.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        if key.len() > MAX_KEY_SIZE {
            return Err(Error::KeyTooLarge { size: key.len(), max: MAX_KEY_SIZE });
        }
        if value.len() > MAX_INLINE_VALUE_SIZE {
            return Err(Error::ValueTooLarge { size: value.len(), max: MAX_INLINE_VALUE_SIZE });
        }

        self.preload_path(self.tree.root, key)?;
        self.tree.insert(
            &mut self.pages, &mut self.alloc, self.txn_id,
            key, ValueType::Inline, value,
        )
    }

    /// Delete a key. Returns true if the key existed.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.preload_path(self.tree.root, key)?;
        self.tree.delete(&mut self.pages, &mut self.alloc, self.txn_id, key)
    }

    /// Commit this transaction. Writes all changes to disk atomically.
    /// Returns an error if the commit fails (data file is NOT corrupted on failure).
    pub fn commit(mut self) -> Result<()> {
        self.manager.commit_write(
            self.txn_id,
            &mut self.pages,
            &mut self.alloc,
            &self.tree,
            &self.old_slot,
            &self.deferred_free,
        )?;
        self.committed = true;
        Ok(())
    }

    /// Explicitly abort the transaction, discarding all changes.
    pub fn abort(mut self) {
        self.committed = true; // Prevent double-abort in Drop
        self.manager.abort_write();
    }

    /// Iterate all key-value pairs in sorted order.
    /// Calls the provided closure for each entry.
    pub fn for_each<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        // Load ALL pages in the tree. Cursor traversal needs branch pages
        // and sibling leaves that preload_leftmost_path would miss.
        // This is O(N) which is fine since for_each is already O(N).
        self.preload_all_pages(self.tree.root)?;

        let mut cursor = Cursor::first(&self.pages, self.tree.root)?;
        while cursor.is_valid() {
            if let Some(entry) = cursor.current(&self.pages) {
                if entry.val_type != ValueType::Tombstone {
                    f(&entry.key, &entry.value)?;
                }
            }
            cursor.next(&self.pages)?;
        }
        Ok(())
    }

    /// Pre-load all pages along the path from `root` to the leaf containing `key`.
    fn preload_path(&mut self, root: PageId, key: &[u8]) -> Result<()> {
        let mut current = root;
        loop {
            if !self.pages.contains_key(&current) {
                let page = self.manager.read_page_from_disk(current)?;
                self.pages.insert(current, page);
            }
            let page = self.pages.get(&current).unwrap();
            match page.page_type() {
                Some(PageType::Leaf) => return Ok(()),
                Some(PageType::Branch) => {
                    current = branch_node::search(page, key);
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }
    }

    /// Pre-load ALL pages in the subtree rooted at `root`.
    /// Required for cursor traversal which accesses branch pages and
    /// sibling leaves during advance_leaf/retreat_leaf.
    fn preload_all_pages(&mut self, root: PageId) -> Result<()> {
        // Use an explicit stack to avoid deep recursion on large trees
        let mut stack = vec![root];
        while let Some(current) = stack.pop() {
            if !self.pages.contains_key(&current) {
                let page = self.manager.read_page_from_disk(current)?;
                self.pages.insert(current, page);
            }
            let page = self.pages.get(&current).unwrap();
            match page.page_type() {
                Some(PageType::Branch) => {
                    let num_cells = page.num_cells() as usize;
                    // Push all children: cells 0..num_cells + right_child
                    for i in 0..num_cells {
                        stack.push(branch_node::get_child(page, i));
                    }
                    let right = page.right_child();
                    if right.is_valid() {
                        stack.push(right);
                    }
                }
                Some(PageType::Leaf) => {} // Leaf — already loaded, no children
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
        assert!(wtx.insert(b"key", b"v1").unwrap());   // new
        assert!(!wtx.insert(b"key", b"v2").unwrap());  // update
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

    use citadel_core::MAX_KEY_SIZE;
    use citadel_core::MAX_INLINE_VALUE_SIZE;
}
