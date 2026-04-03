//! Write transaction: CoW mutations with full commit protocol.
//!
//! Maintains a local HashMap of pages as the "write set."
//! Pages needed by the B+ tree are pre-loaded from disk before operations.
//! On commit, executes the 6-step god byte commit protocol.
//! On Drop without commit, automatically aborts.

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result, MAX_INLINE_VALUE_SIZE, MAX_KEY_SIZE};
use citadel_io::file_manager::CommitSlot;
use citadel_page::branch_node;
use citadel_page::page::Page;
use std::collections::HashMap;

use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::btree::BTree;
use citadel_buffer::cursor::Cursor;

use crate::catalog::TableDescriptor;
use crate::manager::TxnManager;

/// A read-write transaction.
///
/// Supports insert, delete, and get operations on the default B+ tree
/// and on named tables via the catalog.
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
    named_trees: HashMap<Vec<u8>, BTree>,
    catalog: Option<BTree>,
    catalog_dirty: bool,
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
            named_trees: HashMap::new(),
            catalog: None,
            catalog_dirty: false,
        }
    }

    /// Get the transaction ID.
    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    /// Get the current entry count of the default table.
    pub fn entry_count(&self) -> u64 {
        self.tree.entry_count
    }

    // ── Default table operations ──────────────────────────────────────

    /// Look up a key in the default table.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.preload_path(self.tree.root, key)?;
        self.search_in_tree(&self.tree.clone(), key)
    }

    /// Insert a key-value pair into the default table. Returns true if the key is new.
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

    /// Delete a key from the default table. Returns true if the key existed.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.preload_path(self.tree.root, key)?;
        self.tree
            .delete(&mut self.pages, &mut self.alloc, self.txn_id, key)
    }

    /// Iterate all key-value pairs in the default table in sorted order.
    pub fn for_each<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
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

    /// Iterate all key-value pairs in a named table in sorted order.
    pub fn table_for_each<F>(&mut self, table: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        self.ensure_table(table)?;
        let root = self.named_trees[table].root;
        self.preload_all_pages(root)?;
        let mut cursor = Cursor::first(&self.pages, root)?;
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

    /// Seek to `start_key` in a named table and iterate forward.
    /// The callback returns `true` to continue or `false` to stop.
    pub fn table_scan_from<F>(&mut self, table: &[u8], start_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        self.ensure_table(table)?;
        let root = self.named_trees[table].root;
        self.preload_all_pages(root)?;
        let mut cursor = if start_key.is_empty() {
            Cursor::first(&self.pages, root)?
        } else {
            Cursor::seek(&self.pages, root, start_key)?
        };
        while cursor.is_valid() {
            if let Some(entry) = cursor.current(&self.pages) {
                if entry.val_type != ValueType::Tombstone && !f(&entry.key, &entry.value)? {
                    break;
                }
            }
            cursor.next(&self.pages)?;
        }
        Ok(())
    }

    // ── Named table operations ────────────────────────────────────────

    /// Create a new named table. Fails if the table already exists.
    pub fn create_table(&mut self, name: &[u8]) -> Result<()> {
        self.ensure_catalog()?;

        // Check if table already exists in named_trees (created in this txn)
        if self.named_trees.contains_key(name) {
            return Err(Error::TableAlreadyExists(
                String::from_utf8_lossy(name).into_owned(),
            ));
        }

        // Check if table exists in catalog on disk
        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, name)?;
        if let Some((vt, _)) = self.catalog.as_ref().unwrap().search(&self.pages, name)? {
            if vt != ValueType::Tombstone {
                return Err(Error::TableAlreadyExists(
                    String::from_utf8_lossy(name).into_owned(),
                ));
            }
        }

        // Allocate an empty leaf page for the new table
        let page_id = self.alloc.allocate();
        let mut leaf = Page::new(page_id, PageType::Leaf, self.txn_id);
        leaf.update_checksum();
        self.pages.insert(page_id, leaf);

        let new_tree = BTree::from_existing(page_id, 1, 0);
        self.named_trees.insert(name.to_vec(), new_tree);
        self.catalog_dirty = true;
        Ok(())
    }

    /// Drop a named table and free its pages. Fails if the table doesn't exist.
    pub fn drop_table(&mut self, name: &[u8]) -> Result<()> {
        self.ensure_table(name)?;

        // Get the tree and free all its pages
        let tree = self.named_trees.remove(name).unwrap();
        self.free_tree_pages(tree.root)?;

        // Delete from catalog
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

    /// Insert a key-value pair into a named table. Returns true if the key is new.
    pub fn table_insert(&mut self, table: &[u8], key: &[u8], value: &[u8]) -> Result<bool> {
        Self::validate_key_value(key, value)?;
        self.ensure_table(table)?;

        let root = self.named_trees[table].root;
        self.preload_path(root, key)?;

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

    /// Delete a key from a named table. Returns true if the key existed.
    pub fn table_delete(&mut self, table: &[u8], key: &[u8]) -> Result<bool> {
        self.ensure_table(table)?;

        let root = self.named_trees[table].root;
        self.preload_path(root, key)?;

        let tree = self.named_trees.get_mut(table).unwrap();
        tree.delete(&mut self.pages, &mut self.alloc, self.txn_id, key)
    }

    /// Look up a key in a named table.
    pub fn table_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.ensure_table(table)?;

        let tree = self.named_trees[table].clone();
        self.preload_path(tree.root, key)?;
        self.search_in_tree(&tree, key)
    }

    // ── Commit / Abort ────────────────────────────────────────────────

    /// Commit this transaction. Writes all changes to disk atomically.
    pub fn commit(mut self) -> Result<()> {
        let catalog_root = self.finalize_catalog()?;
        self.manager.commit_write(
            self.txn_id,
            &mut self.pages,
            &mut self.alloc,
            &self.tree,
            &self.old_slot,
            &self.deferred_free,
            catalog_root,
            &self.named_trees,
        )?;
        self.committed = true;
        Ok(())
    }

    /// Explicitly abort the transaction, discarding all changes.
    pub fn abort(mut self) {
        self.committed = true;
        self.manager.abort_write();
    }

    // ── Internal helpers ──────────────────────────────────────────────

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

    /// Search for a key in a B+ tree (already preloaded into pages).
    fn search_in_tree(&self, tree: &BTree, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match tree.search(&self.pages, key)? {
            Some((ValueType::Tombstone, _)) => Ok(None),
            Some((_, value)) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    /// Ensure the catalog B+ tree is loaded. Creates an empty one if none exists.
    fn ensure_catalog(&mut self) -> Result<()> {
        if self.catalog.is_some() {
            return Ok(());
        }

        if self.old_slot.catalog_root.is_valid() {
            // Load existing catalog
            self.preload_path(self.old_slot.catalog_root, &[])?;
            let slot = self.catalog_slot_from_disk()?;
            self.catalog = Some(BTree::from_existing(
                slot.root_page,
                slot.depth,
                slot.entry_count,
            ));
        } else {
            // Create a new empty catalog
            let page_id = self.alloc.allocate();
            let mut leaf = Page::new(page_id, PageType::Leaf, self.txn_id);
            leaf.update_checksum();
            self.pages.insert(page_id, leaf);
            self.catalog = Some(BTree::from_existing(page_id, 1, 0));
            self.catalog_dirty = true;
        }
        Ok(())
    }

    /// Read the catalog tree metadata from the commit slot.
    /// The catalog root is the only metadata we store in the commit slot;
    /// depth and entry_count must be discovered from the tree itself.
    fn catalog_slot_from_disk(&mut self) -> Result<TableDescriptor> {
        // We know the catalog root. We need to discover depth and count
        // by walking the tree. For simplicity, use depth=0 and count=0
        // as placeholders — the BTree will update them during operations.
        // Actually, we should compute depth by walking root to leftmost leaf.
        let root = self.old_slot.catalog_root;
        let mut depth: u16 = 1;
        let mut current = root;
        loop {
            if !self.pages.contains_key(&current) {
                let page = self.manager.read_page_from_disk(current)?;
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

    /// Ensure a named table is loaded into named_trees. Errors if not found.
    fn ensure_table(&mut self, name: &[u8]) -> Result<()> {
        if self.named_trees.contains_key(name) {
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
                let tree = BTree::from_existing(desc.root_page, desc.depth, desc.entry_count);
                self.named_trees.insert(name.to_vec(), tree);
            }
        }
        Ok(())
    }

    /// Write all modified named table descriptors to the catalog tree.
    /// Returns the catalog root for the new commit slot.
    fn finalize_catalog(&mut self) -> Result<PageId> {
        if !self.catalog_dirty && self.named_trees.is_empty() {
            return Ok(self.old_slot.catalog_root);
        }

        // Nothing to do if no catalog was created/loaded
        if self.catalog.is_none() {
            return Ok(self.old_slot.catalog_root);
        }

        // Update catalog entries for all open named tables
        let table_entries: Vec<(Vec<u8>, [u8; 20])> = self
            .named_trees
            .iter()
            .map(|(name, tree)| {
                let desc = TableDescriptor::from_tree(tree);
                (name.clone(), desc.serialize())
            })
            .collect();

        for (name, value) in &table_entries {
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

    /// Free all pages in a B+ tree subtree (for drop_table).
    fn free_tree_pages(&mut self, root: PageId) -> Result<()> {
        let mut stack = vec![root];
        while let Some(current) = stack.pop() {
            if !self.pages.contains_key(&current) {
                let page = self.manager.read_page_from_disk(current)?;
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
                let page = self.manager.read_page_from_disk(current)?;
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
                let page = self.manager.read_page_from_disk(current)?;
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
                let page = self.manager.read_page_from_disk(current)?;
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
