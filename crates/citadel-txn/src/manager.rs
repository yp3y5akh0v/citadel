//! Transaction manager: coordinates read/write transactions with MVCC.
//!
//! - Single writer, multiple concurrent readers
//! - Reader registration for oldest_active_reader tracking
//! - Interior mutability via parking_lot::Mutex for concurrent access

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use parking_lot::Mutex;

use citadel_core::types::{PageId, TxnId};
use citadel_core::{
    DEK_SIZE, MAC_KEY_SIZE, PAGE_SIZE, BODY_SIZE,
    GOD_BIT_ACTIVE_SLOT, GOD_BIT_RECOVERY,
    Error, Result,
};
use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::btree::BTree;
use citadel_buffer::pool::BufferPool;
use citadel_crypto::page_cipher;
use citadel_io::file_manager::{
    self, CommitSlot, page_offset, read_god_byte, write_god_byte,
    write_commit_slot, ensure_file_size,
};
use citadel_io::traits::PageIO;
use citadel_page::page::Page;

use crate::catalog::TableDescriptor;
use crate::integrity::{self, IntegrityReport};
use crate::pending_free;
use crate::read_txn::ReadTxn;
use crate::write_txn::WriteTxn;

/// Transaction manager for the Citadel database engine.
///
/// Provides MVCC with single-writer / multi-reader concurrency.
/// All state is protected by fine-grained locks for concurrent access.
pub struct TxnManager {
    io: Box<dyn PageIO>,
    dek: [u8; DEK_SIZE],
    mac_key: [u8; MAC_KEY_SIZE],
    epoch: u32,
    pool: Mutex<BufferPool>,
    next_txn_id: AtomicU64,
    write_active: AtomicBool,
    state: Mutex<ManagerState>,
}

struct ManagerState {
    active_slot: usize,
    current_slot: CommitSlot,
    reader_table: BTreeMap<TxnId, usize>,
    deferred_free: Vec<PageId>,
    /// Pages reclaimed from pending-free chain during the last commit.
    /// Fed to the next writer's allocator so they're actually reused.
    reclaimed_pages: Vec<PageId>,
}

impl TxnManager {
    /// Open a TxnManager on an existing database file.
    ///
    /// Runs the recovery state machine to determine the active commit slot,
    /// then initializes the buffer pool and allocator state.
    pub fn open(
        io: Box<dyn PageIO>,
        dek: [u8; DEK_SIZE],
        mac_key: [u8; MAC_KEY_SIZE],
        epoch: u32,
        cache_size: usize,
    ) -> Result<Self> {
        let (active_slot, slot) = file_manager::recover(&*io)?;

        let next_txn_id = slot.txn_id.as_u64() + 1;

        Ok(Self {
            io,
            dek,
            mac_key,
            epoch,
            pool: Mutex::new(BufferPool::new(cache_size)),
            next_txn_id: AtomicU64::new(next_txn_id),
            write_active: AtomicBool::new(false),
            state: Mutex::new(ManagerState {
                active_slot,
                current_slot: slot,
                reader_table: BTreeMap::new(),
                deferred_free: Vec::new(),
                reclaimed_pages: Vec::new(),
            }),
        })
    }

    /// Create a TxnManager for a brand new (empty) database.
    ///
    /// Writes the initial file header and empty root page.
    pub fn create(
        io: Box<dyn PageIO>,
        dek: [u8; DEK_SIZE],
        mac_key: [u8; MAC_KEY_SIZE],
        epoch: u32,
        file_id: u64,
        dek_id: [u8; 32],
        cache_size: usize,
    ) -> Result<Self> {
        // Write file header
        let header = file_manager::FileHeader::new(file_id, dek_id);
        file_manager::write_file_header(&*io, &header)?;

        // Allocate and write the initial empty root page (leaf)
        let root_id = PageId(0);
        let mut root_page = Page::new(root_id, citadel_core::types::PageType::Leaf, TxnId(1));
        root_page.update_checksum();

        let offset = page_offset(root_id);
        ensure_file_size(&*io, offset)?;
        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(&dek, &mac_key, root_id, epoch, root_page.as_bytes(), &mut encrypted);
        io.write_page(offset, &encrypted)?;

        // Write initial commit slot (slot 0)
        let slot = CommitSlot {
            txn_id: TxnId(1),
            tree_root: root_id,
            tree_depth: 1,
            tree_entries: 0,
            catalog_root: PageId::INVALID,
            total_pages: 1,
            high_water_mark: 1,
            pending_free_root: PageId::INVALID,
            encryption_epoch: epoch,
            dek_id,
            checksum: 0,
        };
        write_commit_slot(&*io, 0, &slot)?;
        io.fsync()?;

        Ok(Self {
            io,
            dek,
            mac_key,
            epoch,
            pool: Mutex::new(BufferPool::new(cache_size)),
            next_txn_id: AtomicU64::new(2),
            write_active: AtomicBool::new(false),
            state: Mutex::new(ManagerState {
                active_slot: 0,
                current_slot: slot,
                reader_table: BTreeMap::new(),
                deferred_free: Vec::new(),
                reclaimed_pages: Vec::new(),
            }),
        })
    }

    /// Begin a read transaction. Snapshots the current commit slot.
    pub fn begin_read(&self) -> ReadTxn<'_> {
        let mut state = self.state.lock();
        let txn_id = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));
        let snapshot = state.current_slot.clone();

        // Register reader
        *state.reader_table.entry(txn_id).or_insert(0) += 1;

        ReadTxn::new(self, txn_id, snapshot)
    }

    /// Begin a write transaction. Only one can be active at a time.
    pub fn begin_write(&self) -> Result<WriteTxn<'_>> {
        if self.write_active.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Err(Error::WriteTransactionActive);
        }

        let mut state = self.state.lock();
        let txn_id = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));
        let snapshot = state.current_slot.clone();
        let deferred = state.deferred_free.clone();
        let reclaimed = std::mem::take(&mut state.reclaimed_pages);
        drop(state);

        // Initialize allocator from current commit slot state
        let mut alloc = PageAllocator::new(snapshot.high_water_mark);
        // Feed previously reclaimed pages so they're actually reused
        if !reclaimed.is_empty() {
            alloc.add_ready_to_use(reclaimed);
        }

        // Initialize BTree from snapshot
        let tree = BTree::from_existing(snapshot.tree_root, snapshot.tree_depth, snapshot.tree_entries);

        Ok(WriteTxn::new(self, txn_id, snapshot, tree, alloc, deferred))
    }

    /// Fetch a decrypted page from the buffer pool (for read transactions).
    pub(crate) fn fetch_page(&self, page_id: PageId) -> Result<Page> {
        let mut pool = self.pool.lock();
        let page = pool.fetch(&*self.io, page_id, &self.dek, &self.mac_key, self.epoch)?;
        Ok(page.clone())
    }

    /// Commit a write transaction using the shadow-paging commit protocol.
    ///
    /// Sets recovery flag, processes pending-free chain, flushes dirty pages,
    /// writes the inactive commit slot, fsyncs, flips the god byte, and fsyncs
    /// again. Returns success only after the final fsync completes.
    pub(crate) fn commit_write(
        &self,
        txn_id: TxnId,
        pages: &mut std::collections::HashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        tree: &BTree,
        old_slot: &CommitSlot,
        deferred_free: &[PageId],
        catalog_root: PageId,
    ) -> Result<()> {
        let mut state = self.state.lock();

        // Set recovery flag
        let current_god_byte = read_god_byte(&*self.io)?;
        let recovery_god_byte = current_god_byte | GOD_BIT_RECOVERY;
        write_god_byte(&*self.io, recovery_god_byte)?;
        self.io.fsync()?;

        // Process pending-free chain (GC + write new chain)
        let freed_this_txn = alloc.commit();
        let oldest_active = self.oldest_active_reader_locked(&state);

        // Load existing pending-free chain pages into the HashMap for reading
        self.load_pending_free_chain(pages, old_slot.pending_free_root)?;

        let (new_pf_root, reclaimed, old_chain_pages) = pending_free::process_chain(
            pages, alloc, txn_id,
            old_slot.pending_free_root,
            &freed_this_txn,
            deferred_free,
            oldest_active,
        )?;

        let mut encrypted_pages: Vec<(u64, [u8; PAGE_SIZE])> = Vec::new();
        for page in pages.values_mut() {
            if page.txn_id() == txn_id {
                page.update_checksum();
                let page_id = page.page_id();
                let offset = page_offset(page_id);
                ensure_file_size(&*self.io, offset)?;

                let mut encrypted = [0u8; PAGE_SIZE];
                page_cipher::encrypt_page(
                    &self.dek, &self.mac_key, page_id,
                    self.epoch, page.as_bytes(), &mut encrypted,
                );
                encrypted_pages.push((offset, encrypted));
            }
        }

        if !encrypted_pages.is_empty() {
            self.io.flush_pages(&encrypted_pages)?;
        }

        // Write new commit slot to inactive slot
        let inactive_slot_idx = 1 - state.active_slot;
        let new_slot = CommitSlot {
            txn_id,
            tree_root: tree.root,
            tree_depth: tree.depth,
            tree_entries: tree.entry_count,
            catalog_root,
            total_pages: alloc.high_water_mark(),
            high_water_mark: alloc.high_water_mark(),
            pending_free_root: new_pf_root,
            encryption_epoch: self.epoch,
            dek_id: old_slot.dek_id,
            checksum: 0, // computed during serialize
        };
        write_commit_slot(&*self.io, inactive_slot_idx, &new_slot)?;

        self.io.fsync()?;

        // Flip god byte (atomic commit point)
        let new_god_byte = inactive_slot_idx as u8 & GOD_BIT_ACTIVE_SLOT;
        write_god_byte(&*self.io, new_god_byte)?;

        // Final fsync — commit is durable after this returns
        self.io.fsync()?;

        // Update manager state
        state.active_slot = inactive_slot_idx;
        state.current_slot = new_slot;
        state.deferred_free = old_chain_pages;
        state.reclaimed_pages = reclaimed;

        // Invalidate buffer pool cache (stale entries from CoW)
        self.pool.lock().discard_dirty();

        self.write_active.store(false, Ordering::SeqCst);

        Ok(())
    }

    /// Abort a write transaction — just release the write lock.
    pub(crate) fn abort_write(&self) {
        self.write_active.store(false, Ordering::SeqCst);
    }

    /// Unregister a reader from the reader table.
    pub(crate) fn unregister_reader(&self, txn_id: TxnId) {
        let mut state = self.state.lock();
        if let Some(count) = state.reader_table.get_mut(&txn_id) {
            *count -= 1;
            if *count == 0 {
                state.reader_table.remove(&txn_id);
            }
        }
    }

    /// Get the oldest active reader's txn_id.
    /// If no readers, returns current txn_id (all freed pages are reclaimable).
    pub fn oldest_active_reader(&self) -> TxnId {
        let state = self.state.lock();
        self.oldest_active_reader_locked(&state)
    }

    fn oldest_active_reader_locked(&self, state: &ManagerState) -> TxnId {
        state.reader_table.keys().next().copied().unwrap_or(
            TxnId(self.next_txn_id.load(Ordering::SeqCst))
        )
    }

    /// Get the current active commit slot (for testing/inspection).
    pub fn current_slot(&self) -> CommitSlot {
        self.state.lock().current_slot.clone()
    }

    /// Get the number of active readers (for testing/inspection).
    pub fn reader_count(&self) -> usize {
        self.state.lock().reader_table.len()
    }

    /// Run integrity check on the database.
    pub fn integrity_check(&self) -> Result<IntegrityReport> {
        integrity::run_integrity_check(self)
    }

    /// Copy a consistent snapshot to the destination I/O.
    pub fn backup_to(&self, dest_io: &dyn PageIO) -> Result<()> {
        use std::collections::HashSet;
        let slot = self.current_slot();

        // Collect all reachable pages
        let mut reachable = HashSet::new();
        self.collect_tree_pages(slot.tree_root, &mut reachable)?;

        if slot.catalog_root.is_valid() {
            let table_roots = self.collect_catalog_pages(slot.catalog_root, &mut reachable)?;
            for root in table_roots {
                self.collect_tree_pages(root, &mut reachable)?;
            }
        }

        if slot.pending_free_root.is_valid() {
            self.collect_chain_pages(slot.pending_free_root, &mut reachable)?;
        }

        // Read source file header
        let mut header_buf = [0u8; citadel_core::FILE_HEADER_SIZE];
        self.io.read_at(0, &mut header_buf)?;
        let mut header = file_manager::FileHeader::deserialize(&header_buf)?;

        // Set both commit slots to the snapshot, slot 0 active
        header.slots = [slot.clone(), slot];
        header.god_byte = 0;

        // Calculate needed file size
        let max_page = reachable.iter().map(|p| p.as_u32()).max().unwrap_or(0);
        let needed_size = citadel_core::FILE_HEADER_SIZE as u64
            + (max_page as u64 + 1) * PAGE_SIZE as u64;
        dest_io.truncate(needed_size)?;

        // Write header
        dest_io.write_at(0, &header.serialize())?;

        // Copy all reachable pages (raw encrypted bytes — no decrypt/re-encrypt)
        for &page_id in &reachable {
            let offset = page_offset(page_id);
            let mut buf = [0u8; PAGE_SIZE];
            self.io.read_page(offset, &mut buf)?;
            dest_io.write_page(offset, &buf)?;
        }

        dest_io.fsync()?;
        Ok(())
    }

    /// Compact the database into a new file with sequential page IDs.
    pub fn compact_to(&self, dest_io: &dyn PageIO) -> Result<()> {
        use std::collections::HashMap as StdMap;
        use std::collections::HashSet;
        use citadel_page::{branch_node, leaf_node};
        use citadel_core::types::ValueType;

        let slot = self.current_slot();
        let mut next_id: u32 = 0;
        let mut old_to_new: StdMap<PageId, PageId> = StdMap::new();
        let mut catalog_leaves: HashSet<PageId> = HashSet::new();

        // Walk all trees, assign new sequential page IDs
        self.assign_new_ids(slot.tree_root, &mut old_to_new, &mut next_id)?;

        if slot.catalog_root.is_valid() {
            let table_roots = {
                let mut reachable = HashSet::new();
                let roots = self.collect_catalog_pages(slot.catalog_root, &mut reachable)?;
                roots
            };

            self.assign_new_ids(slot.catalog_root, &mut old_to_new, &mut next_id)?;

            // Collect catalog leaf page IDs (they contain TableDescriptors that need fixup)
            self.collect_catalog_leaf_pages(slot.catalog_root, &mut catalog_leaves)?;

            for root in &table_roots {
                self.assign_new_ids(*root, &mut old_to_new, &mut next_id)?;
            }
        }

        // Copy each page with remapped IDs
        let total_pages = next_id;
        let needed_size = citadel_core::FILE_HEADER_SIZE as u64
            + total_pages as u64 * PAGE_SIZE as u64;
        dest_io.truncate(needed_size)?;

        for (&old_id, &new_id) in &old_to_new {
            let mut page = self.read_page_from_disk(old_id)?;

            // Update page_id
            page.set_page_id(new_id);

            // Remap child pointers in branch pages
            if page.page_type() == Some(citadel_core::types::PageType::Branch) {
                for i in 0..page.num_cells() as usize {
                    let old_child = branch_node::get_child(&page, i);
                    if let Some(&new_child) = old_to_new.get(&old_child) {
                        let offset = page.cell_offset(i as u16) as usize;
                        page.data[offset..offset + 4]
                            .copy_from_slice(&new_child.as_u32().to_le_bytes());
                    }
                }
                let old_right = page.right_child();
                if old_right.is_valid() {
                    if let Some(&new_right) = old_to_new.get(&old_right) {
                        page.set_right_child(new_right);
                    }
                }
            }

            // Remap table root pages in catalog leaf cells
            if catalog_leaves.contains(&old_id) {
                for i in 0..page.num_cells() {
                    let cell = leaf_node::read_cell(&page, i);
                    if cell.val_type != ValueType::Tombstone
                        && cell.value.len() >= crate::catalog::TABLE_DESCRIPTOR_SIZE
                    {
                        let desc = TableDescriptor::deserialize(cell.value);
                        if let Some(&new_root) = old_to_new.get(&desc.root_page) {
                            let cell_off = page.cell_offset(i) as usize;
                            let key_len = u16::from_le_bytes(
                                page.data[cell_off..cell_off + 2].try_into().unwrap(),
                            ) as usize;
                            let value_start = cell_off + 6 + key_len + 1;
                            page.data[value_start..value_start + 4]
                                .copy_from_slice(&new_root.as_u32().to_le_bytes());
                        }
                    }
                }
            }

            page.update_checksum();

            // Encrypt with fresh IV and write to new location
            let offset = page_offset(new_id);
            let mut encrypted = [0u8; PAGE_SIZE];
            page_cipher::encrypt_page(
                &self.dek, &self.mac_key, new_id, self.epoch,
                page.as_bytes(), &mut encrypted,
            );
            dest_io.write_page(offset, &encrypted)?;
        }

        // Write file header
        let mut header_buf = [0u8; citadel_core::FILE_HEADER_SIZE];
        self.io.read_at(0, &mut header_buf)?;
        let mut header = file_manager::FileHeader::deserialize(&header_buf)?;

        let new_tree_root = old_to_new.get(&slot.tree_root).copied().unwrap_or(PageId(0));
        let new_catalog_root = if slot.catalog_root.is_valid() {
            old_to_new.get(&slot.catalog_root).copied().unwrap_or(PageId::INVALID)
        } else {
            PageId::INVALID
        };

        let new_slot = CommitSlot {
            txn_id: slot.txn_id,
            tree_root: new_tree_root,
            tree_depth: slot.tree_depth,
            tree_entries: slot.tree_entries,
            catalog_root: new_catalog_root,
            total_pages,
            high_water_mark: total_pages,
            pending_free_root: PageId::INVALID,
            encryption_epoch: slot.encryption_epoch,
            dek_id: slot.dek_id,
            checksum: 0,
        };

        header.slots = [new_slot.clone(), new_slot];
        header.god_byte = 0;

        dest_io.write_at(0, &header.serialize())?;
        dest_io.fsync()?;

        Ok(())
    }

    /// Collect all page IDs in a B+ tree.
    fn collect_tree_pages(
        &self,
        root: PageId,
        reachable: &mut std::collections::HashSet<PageId>,
    ) -> Result<()> {
        use citadel_page::branch_node;

        let mut stack = vec![root];
        while let Some(page_id) = stack.pop() {
            if !reachable.insert(page_id) {
                continue;
            }
            let page = self.read_page_from_disk(page_id)?;
            if page.page_type() == Some(citadel_core::types::PageType::Branch) {
                for i in 0..page.num_cells() as usize {
                    stack.push(branch_node::get_child(&page, i));
                }
                let right = page.right_child();
                if right.is_valid() {
                    stack.push(right);
                }
            }
        }
        Ok(())
    }

    /// Collect catalog pages and return named table root pages.
    fn collect_catalog_pages(
        &self,
        catalog_root: PageId,
        reachable: &mut std::collections::HashSet<PageId>,
    ) -> Result<Vec<PageId>> {
        use citadel_page::{branch_node, leaf_node};
        use citadel_core::types::ValueType;

        let mut table_roots = Vec::new();
        let mut stack = vec![catalog_root];
        while let Some(page_id) = stack.pop() {
            if !reachable.insert(page_id) {
                continue;
            }
            let page = self.read_page_from_disk(page_id)?;
            match page.page_type() {
                Some(citadel_core::types::PageType::Leaf) => {
                    for i in 0..page.num_cells() {
                        let cell = leaf_node::read_cell(&page, i);
                        if cell.val_type != ValueType::Tombstone && cell.value.len() >= 4 {
                            let desc = TableDescriptor::deserialize(cell.value);
                            if desc.root_page.is_valid() {
                                table_roots.push(desc.root_page);
                            }
                        }
                    }
                }
                Some(citadel_core::types::PageType::Branch) => {
                    for i in 0..page.num_cells() as usize {
                        stack.push(branch_node::get_child(&page, i));
                    }
                    let right = page.right_child();
                    if right.is_valid() {
                        stack.push(right);
                    }
                }
                _ => {}
            }
        }
        Ok(table_roots)
    }

    /// Collect pending-free chain page IDs.
    fn collect_chain_pages(
        &self,
        root: PageId,
        reachable: &mut std::collections::HashSet<PageId>,
    ) -> Result<()> {
        let mut current = root;
        while current.is_valid() {
            if !reachable.insert(current) {
                break;
            }
            let page = self.read_page_from_disk(current)?;
            current = page.right_child();
        }
        Ok(())
    }

    /// Collect catalog leaf page IDs (pages that contain TableDescriptor values).
    fn collect_catalog_leaf_pages(
        &self,
        catalog_root: PageId,
        leaves: &mut std::collections::HashSet<PageId>,
    ) -> Result<()> {
        use citadel_page::branch_node;

        let mut stack = vec![catalog_root];
        while let Some(page_id) = stack.pop() {
            let page = self.read_page_from_disk(page_id)?;
            match page.page_type() {
                Some(citadel_core::types::PageType::Leaf) => {
                    leaves.insert(page_id);
                }
                Some(citadel_core::types::PageType::Branch) => {
                    for i in 0..page.num_cells() as usize {
                        stack.push(branch_node::get_child(&page, i));
                    }
                    let right = page.right_child();
                    if right.is_valid() {
                        stack.push(right);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Walk a tree depth-first, assigning new sequential page IDs.
    fn assign_new_ids(
        &self,
        root: PageId,
        mapping: &mut std::collections::HashMap<PageId, PageId>,
        next_id: &mut u32,
    ) -> Result<()> {
        use citadel_page::branch_node;

        let mut stack = vec![root];
        while let Some(page_id) = stack.pop() {
            if mapping.contains_key(&page_id) {
                continue;
            }
            mapping.insert(page_id, PageId(*next_id));
            *next_id += 1;

            let page = self.read_page_from_disk(page_id)?;
            if page.page_type() == Some(citadel_core::types::PageType::Branch) {
                for i in 0..page.num_cells() as usize {
                    stack.push(branch_node::get_child(&page, i));
                }
                let right = page.right_child();
                if right.is_valid() {
                    stack.push(right);
                }
            }
        }
        Ok(())
    }

    /// Load pending-free chain pages from disk into the HashMap.
    fn load_pending_free_chain(
        &self,
        pages: &mut std::collections::HashMap<PageId, Page>,
        root: PageId,
    ) -> Result<()> {
        if !root.is_valid() {
            return Ok(());
        }

        let mut current = root;
        while current.is_valid() {
            if !pages.contains_key(&current) {
                let page = self.read_page_from_disk(current)?;
                let next = page.right_child();
                pages.insert(current, page);
                if !next.is_valid() {
                    break;
                }
                current = next;
            } else {
                let next = pages.get(&current).unwrap().right_child();
                if !next.is_valid() {
                    break;
                }
                current = next;
            }
        }

        Ok(())
    }

    /// Read and decrypt a single page from disk.
    pub fn read_page_from_disk(&self, page_id: PageId) -> Result<Page> {
        let offset = page_offset(page_id);
        let mut encrypted = [0u8; PAGE_SIZE];
        self.io.read_page(offset, &mut encrypted)?;

        let mut body = [0u8; BODY_SIZE];
        page_cipher::decrypt_page(
            &self.dek, &self.mac_key, page_id, self.epoch,
            &encrypted, &mut body,
        )?;

        let page = Page::from_bytes(body);
        if !page.verify_checksum() {
            return Err(Error::ChecksumMismatch(page_id));
        }

        Ok(page)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use citadel_crypto::hkdf_utils::derive_keys_from_rek;
    use citadel_crypto::page_cipher::compute_dek_id;
    use std::sync::Mutex as StdMutex;

    /// In-memory PageIO for testing.
    pub struct MemIO {
        data: StdMutex<Vec<u8>>,
    }

    impl MemIO {
        pub fn new(size: usize) -> Self {
            Self { data: StdMutex::new(vec![0u8; size]) }
        }
    }

    impl PageIO for MemIO {
        fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
            let data = self.data.lock().unwrap();
            let start = offset as usize;
            let end = start + PAGE_SIZE;
            if end > data.len() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "read past end",
                )));
            }
            buf.copy_from_slice(&data[start..end]);
            Ok(())
        }

        fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
            let mut data = self.data.lock().unwrap();
            let start = offset as usize;
            let end = start + PAGE_SIZE;
            if end > data.len() {
                data.resize(end, 0);
            }
            data[start..end].copy_from_slice(buf);
            Ok(())
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
            let data = self.data.lock().unwrap();
            let start = offset as usize;
            let end = start + buf.len();
            if end > data.len() {
                let available = data.len().saturating_sub(start);
                if available > 0 {
                    buf[..available].copy_from_slice(&data[start..start + available]);
                }
                buf[available..].fill(0);
                return Ok(());
            }
            buf.copy_from_slice(&data[start..end]);
            Ok(())
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
            let mut data = self.data.lock().unwrap();
            let start = offset as usize;
            let end = start + buf.len();
            if end > data.len() {
                data.resize(end, 0);
            }
            data[start..end].copy_from_slice(buf);
            Ok(())
        }

        fn fsync(&self) -> Result<()> { Ok(()) }

        fn file_size(&self) -> Result<u64> {
            Ok(self.data.lock().unwrap().len() as u64)
        }

        fn truncate(&self, size: u64) -> Result<()> {
            let mut data = self.data.lock().unwrap();
            data.resize(size as usize, 0);
            Ok(())
        }
    }

    pub fn test_keys() -> ([u8; DEK_SIZE], [u8; MAC_KEY_SIZE], [u8; 32]) {
        let rek = [0x42u8; 32];
        let keys = derive_keys_from_rek(&rek);
        let dek_id = compute_dek_id(&keys.mac_key, &keys.dek);
        (keys.dek, keys.mac_key, dek_id)
    }

    pub fn create_test_manager() -> TxnManager {
        let (dek, mac_key, dek_id) = test_keys();
        let io = Box::new(MemIO::new(1024 * 1024));
        TxnManager::create(io, dek, mac_key, 1, 0x1234, dek_id, 256).unwrap()
    }

    #[test]
    fn create_and_open() {
        let (dek, mac_key, dek_id) = test_keys();
        let io = Box::new(MemIO::new(1024 * 1024));

        let mgr = TxnManager::create(io, dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
        let slot = mgr.current_slot();
        assert_eq!(slot.txn_id, TxnId(1));
        assert_eq!(slot.tree_root, PageId(0));
        assert_eq!(slot.tree_depth, 1);
        assert_eq!(slot.tree_entries, 0);
        assert_eq!(slot.high_water_mark, 1);
    }

    #[test]
    fn begin_read_registers_reader() {
        let mgr = create_test_manager();
        assert_eq!(mgr.reader_count(), 0);

        let _rtx = mgr.begin_read();
        assert_eq!(mgr.reader_count(), 1);
    }

    #[test]
    fn drop_read_unregisters_reader() {
        let mgr = create_test_manager();
        {
            let _rtx = mgr.begin_read();
            assert_eq!(mgr.reader_count(), 1);
        }
        assert_eq!(mgr.reader_count(), 0);
    }

    #[test]
    fn multiple_concurrent_readers() {
        let mgr = create_test_manager();
        let _r1 = mgr.begin_read();
        let _r2 = mgr.begin_read();
        let _r3 = mgr.begin_read();
        assert_eq!(mgr.reader_count(), 3);
    }

    #[test]
    fn single_writer_enforcement() {
        let mgr = create_test_manager();
        let _wtx = mgr.begin_write().unwrap();
        let result = mgr.begin_write();
        assert!(matches!(result, Err(Error::WriteTransactionActive)));
    }

    #[test]
    fn writer_released_after_drop() {
        let mgr = create_test_manager();
        {
            let _wtx = mgr.begin_write().unwrap();
        }
        // Should be able to begin another write after drop
        let _wtx2 = mgr.begin_write().unwrap();
    }

    #[test]
    fn oldest_active_reader_with_no_readers() {
        let mgr = create_test_manager();
        // No readers — oldest should be current next_txn_id
        let oldest = mgr.oldest_active_reader();
        assert!(oldest.as_u64() >= 2); // At least 2 since create used txn 1
    }

    #[test]
    fn oldest_active_reader_tracks_minimum() {
        let mgr = create_test_manager();
        let r1 = mgr.begin_read(); // Gets some txn_id
        let _r2 = mgr.begin_read(); // Gets higher txn_id
        let oldest = mgr.oldest_active_reader();
        assert_eq!(oldest, r1.txn_id());
    }
}
