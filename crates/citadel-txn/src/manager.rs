//! Transaction manager: single-writer MVCC with shadow-paging commit.

use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use std::sync::Arc;

use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::btree::BTree;
use citadel_buffer::pool::BufferPool;
use citadel_core::types::{PageId, TxnId};
use citadel_core::{
    Error, Result, BODY_SIZE, DEK_SIZE, GOD_BIT_ACTIVE_SLOT, GOD_BIT_RECOVERY, MAC_KEY_SIZE,
    PAGE_SIZE, SLOT_ENTRY_STALE, SLOT_NAMED_MAX_ENTRIES_V1,
};
use citadel_crypto::page_cipher;
use citadel_io::file_manager::{
    self, ensure_file_size, page_offset, write_commit_slot, write_god_byte, CommitSlot,
};
use citadel_io::traits::PageIO;
use citadel_page::page::Page;

use crate::catalog::TableDescriptor;
use crate::integrity::{self, IntegrityReport};
use crate::pending_free;
use crate::read_txn::ReadTxn;
use crate::write_txn::WriteTxn;

pub struct TxnManager {
    io: Box<dyn PageIO>,
    dek: [u8; DEK_SIZE],
    mac_key: [u8; MAC_KEY_SIZE],
    epoch: u32,
    pool: Mutex<BufferPool>,
    next_txn_id: AtomicU64,
    commit_generation: AtomicU64,
    write_active: AtomicBool,
    /// HEADER_FLAG_SLOTS_V1 state, cached at open/create and refreshed by
    /// mark_slots_v1: a flagged file must never receive a legacy slot.
    slots_flagged: AtomicBool,
    state: Mutex<ManagerState>,
    sync_mode: citadel_core::types::SyncMode,
    hmac_state: page_cipher::HmacState,
    /// When true, freed pages past all readers are zero-filled on commit
    /// (secure delete).
    secure_delete: AtomicBool,
    /// Reusable encrypt output buffer, capped at COMMIT_ARENA_PAGES pages.
    commit_arena: Mutex<Vec<u8>>,
}

/// Commit encrypt/write chunk size; bounds arena retention and transient
/// memory.
const COMMIT_ARENA_PAGES: usize = 64;

struct ManagerState {
    active_slot: usize,
    current_slot: Arc<CommitSlot>,
    cached_god_byte: u8,
    cached_file_size: u64,
    /// Active readers keyed by SNAPSHOT txn id (not the reader's own id), so
    /// the reclaim horizon is the min snapshot still referenced. Values are
    /// refcounts: concurrent readers share a snapshot.
    reader_table: BTreeMap<TxnId, usize>,
    /// Reusable free pages, a RAM cache of the durable pending-free chain:
    /// loaned to the writer by clone and re-derived every commit, so an
    /// abort/no-op/shutdown never strands a page.
    reclaimed_pages: Vec<PageId>,
    /// Secure delete: highest freed_at_txn whose available pages have been
    /// zero-filled. RAM-only; a reopen re-zeroes once, which is harmless.
    zeroed_up_to: TxnId,
    recycled_pages: Option<FxHashMap<PageId, Page>>,
}

impl TxnManager {
    pub fn open(
        io: Box<dyn PageIO>,
        dek: [u8; DEK_SIZE],
        mac_key: [u8; MAC_KEY_SIZE],
        epoch: u32,
        cache_size: usize,
    ) -> Result<Self> {
        Self::open_with_sync(io, dek, mac_key, epoch, cache_size, Default::default())
    }

    pub fn open_with_sync(
        io: Box<dyn PageIO>,
        dek: [u8; DEK_SIZE],
        mac_key: [u8; MAC_KEY_SIZE],
        epoch: u32,
        cache_size: usize,
        sync_mode: citadel_core::types::SyncMode,
    ) -> Result<Self> {
        let (active_slot, slot) = file_manager::recover(&*io, &mac_key)?;
        // One-way: once both slots are sealed V1, legacy slots are rejected.
        let slots_flagged = file_manager::mark_slots_v1_if_upgraded(&*io)?;
        let file_size = io.file_size()?;

        let next_txn_id = slot.txn_id.as_u64() + 1;

        Ok(Self {
            io,
            dek,
            mac_key,
            epoch,
            pool: Mutex::new(BufferPool::new(cache_size)),
            next_txn_id: AtomicU64::new(next_txn_id),
            commit_generation: AtomicU64::new(0),
            write_active: AtomicBool::new(false),
            slots_flagged: AtomicBool::new(slots_flagged),
            state: Mutex::new(ManagerState {
                active_slot,
                current_slot: Arc::new(slot),
                cached_god_byte: active_slot as u8 & GOD_BIT_ACTIVE_SLOT,
                cached_file_size: file_size,
                reader_table: BTreeMap::new(),
                reclaimed_pages: Vec::new(),
                zeroed_up_to: TxnId(0),
                recycled_pages: None,
            }),
            sync_mode,
            hmac_state: page_cipher::HmacState::new(&mac_key, epoch),
            secure_delete: AtomicBool::new(false),
            commit_arena: Mutex::new(Vec::new()),
        })
    }

    pub fn create(
        io: Box<dyn PageIO>,
        dek: [u8; DEK_SIZE],
        mac_key: [u8; MAC_KEY_SIZE],
        epoch: u32,
        file_id: u64,
        dek_id: [u8; 32],
        cache_size: usize,
    ) -> Result<Self> {
        Self::create_with_sync(
            io,
            dek,
            mac_key,
            epoch,
            file_id,
            dek_id,
            cache_size,
            Default::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_with_sync(
        io: Box<dyn PageIO>,
        dek: [u8; DEK_SIZE],
        mac_key: [u8; MAC_KEY_SIZE],
        epoch: u32,
        file_id: u64,
        dek_id: [u8; 32],
        cache_size: usize,
        sync_mode: citadel_core::types::SyncMode,
    ) -> Result<Self> {
        let mut header = file_manager::FileHeader::new(file_id, dek_id);
        for slot in &mut header.slots {
            slot.seal(&mac_key);
        }
        file_manager::write_file_header(&*io, &header)?;

        let root_id = PageId(0);
        let root_page = Page::new(root_id, citadel_core::types::PageType::Leaf, TxnId(1));

        let mut init_pages = FxHashMap::default();
        init_pages.insert(root_id, root_page);
        let merkle_root_hash =
            crate::merkle::compute_tree_merkle(&mut init_pages, root_id, TxnId(1), &|_| {
                unreachable!("no clean pages for new database")
            })?;
        let mut root_page = init_pages.remove(&root_id).unwrap();
        root_page.update_checksum();

        let offset = page_offset(root_id);
        ensure_file_size(&*io, offset)?;
        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(
            &dek,
            &mac_key,
            root_id,
            epoch,
            root_page.as_bytes(),
            &mut encrypted,
        );
        io.write_page(offset, &encrypted)?;

        let mut slot = CommitSlot {
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
            merkle_root: merkle_root_hash,
            ..Default::default()
        };
        slot.seal(&mac_key);
        write_commit_slot(&*io, 0, &slot)?;
        io.fsync()?;
        let file_size = io.file_size()?;

        Ok(Self {
            io,
            dek,
            mac_key,
            epoch,
            pool: Mutex::new(BufferPool::new(cache_size)),
            next_txn_id: AtomicU64::new(2),
            commit_generation: AtomicU64::new(0),
            write_active: AtomicBool::new(false),
            // New files are flagged at birth (FileHeader::new).
            slots_flagged: AtomicBool::new(true),
            state: Mutex::new(ManagerState {
                active_slot: 0,
                current_slot: Arc::new(slot),
                cached_god_byte: 0,
                cached_file_size: file_size,
                reader_table: BTreeMap::new(),
                reclaimed_pages: Vec::new(),
                zeroed_up_to: TxnId(0),
                recycled_pages: None,
            }),
            sync_mode,
            hmac_state: page_cipher::HmacState::new(&mac_key, epoch),
            secure_delete: AtomicBool::new(false),
            commit_arena: Mutex::new(Vec::new()),
        })
    }

    pub fn sync_mode(&self) -> citadel_core::types::SyncMode {
        self.sync_mode
    }

    /// Enable/disable secure delete: zero-fill freed pages once they are past
    /// all readers.
    pub fn set_secure_delete(&self, on: bool) {
        self.secure_delete.store(on, Ordering::Release);
    }

    pub fn begin_read(&self) -> ReadTxn<'_> {
        let mut state = self.state.lock();
        let txn_id = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));
        let snapshot = state.current_slot.clone();
        let commit_generation = self.commit_generation.load(Ordering::Acquire);

        // Key by snapshot id (see reader_table): a reader beginning mid-write
        // gets an id above the writer's but a snapshot predating its commit.
        *state.reader_table.entry(snapshot.txn_id).or_insert(0) += 1;

        ReadTxn::new(self, txn_id, snapshot, commit_generation)
    }

    pub fn commit_generation(&self) -> u64 {
        self.commit_generation.load(Ordering::Acquire)
    }

    /// One-way HEADER_FLAG_SLOTS_V1 stamp, callable mid-session by the
    /// facade's upgrade_format after it reseals both slots (open() also runs
    /// it). Returns whether the flag is set afterwards.
    pub fn mark_slots_v1(&self) -> Result<bool> {
        let flagged = file_manager::mark_slots_v1_if_upgraded(&*self.io)?;
        self.slots_flagged.store(flagged, Ordering::Release);
        Ok(flagged)
    }

    /// Cached HEADER_FLAG_SLOTS_V1 state of the underlying file.
    pub fn slots_flagged(&self) -> bool {
        self.slots_flagged.load(Ordering::Acquire)
    }

    pub fn begin_write(&self) -> Result<WriteTxn<'_>> {
        if self
            .write_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::WriteTransactionActive);
        }

        let mut state = self.state.lock();
        let txn_id = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));
        let snapshot = state.current_slot.clone();
        // Loan the available batch by clone: state keeps ownership, and the
        // pages stay listed in the durable pending-free chain until a commit
        // records their consumption, so nothing is ever stranded.
        let reclaimed = state.reclaimed_pages.clone();
        let recycled = state.recycled_pages.take();
        drop(state);

        let mut alloc = PageAllocator::new(snapshot.high_water_mark);
        if !reclaimed.is_empty() {
            alloc.add_ready_to_use(reclaimed);
        }

        let tree = BTree::from_existing(
            snapshot.tree_root,
            snapshot.tree_depth,
            snapshot.tree_entries,
        );

        Ok(WriteTxn::new(self, txn_id, snapshot, tree, alloc, recycled))
    }

    pub(crate) fn fetch_page(&self, page_id: PageId) -> Result<Arc<Page>> {
        if let Some(arc) = self.pool.lock().get_cached(page_id) {
            return Ok(arc);
        }

        let offset = page_offset(page_id);
        let page = citadel_buffer::pool::read_and_decrypt(
            &*self.io,
            page_id,
            offset,
            &self.dek,
            &self.mac_key,
            self.epoch,
        )?;

        let arc = Arc::new(page);
        self.pool.lock().insert_if_absent(page_id, Arc::clone(&arc));

        Ok(arc)
    }

    pub(crate) fn next_write_txn_id(&self) -> TxnId {
        TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Build the new slot's named-table entries. Stale entries (SLOT_ENTRY_
    /// STALE, the sole record of a root) are never dropped; fresh ones are
    /// droppable cache, trimmed to seal V1 whenever the stale set fits.
    fn build_slot_entries(
        named_trees: &FxHashMap<Vec<u8>, BTree>,
        loaded_tree_meta: &FxHashMap<Vec<u8>, (PageId, u16)>,
        old_slot: &CommitSlot,
        catalog_refreshed: &FxHashSet<u32>,
    ) -> Vec<(u32, u64, u32, u16)> {
        let mut stale_entries: Vec<(u32, u64, u32, u16)> = Vec::new();
        let mut fresh_entries: Vec<(u32, u64, u32, u16)> = Vec::new();
        for (name, tree) in named_trees {
            let hash = file_manager::table_name_hash(name);
            let moved = match loaded_tree_meta.get(name) {
                Some(&(root, depth)) => tree.root != root || tree.depth != depth,
                None => true,
            };
            let stale =
                !catalog_refreshed.contains(&hash) && (moved || old_slot.entry_is_stale(hash));
            let entry = (
                hash,
                if stale {
                    tree.entry_count | SLOT_ENTRY_STALE
                } else {
                    tree.entry_count
                },
                tree.root.as_u32(),
                tree.depth,
            );
            if stale {
                stale_entries.push(entry);
            } else {
                fresh_entries.push(entry);
            }
        }
        let known_hashes: FxHashSet<u32> = named_trees
            .keys()
            .chain(loaded_tree_meta.keys())
            .map(|name| file_manager::table_name_hash(name))
            .collect();
        for &(hash, count, root, depth) in &old_slot.named_table_entries {
            if known_hashes.contains(&hash) {
                continue;
            }
            if old_slot.entry_is_stale(hash) {
                stale_entries.push((hash, count | SLOT_ENTRY_STALE, root, depth));
            } else {
                fresh_entries.push((hash, count, root, depth));
            }
        }
        let room = SLOT_NAMED_MAX_ENTRIES_V1.saturating_sub(stale_entries.len());
        stale_entries.extend(fresh_entries.into_iter().take(room));
        stale_entries
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_write(
        &self,
        base_txn_id: TxnId,
        txn_id: TxnId,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        tree: &BTree,
        old_slot: &CommitSlot,
        catalog_root: PageId,
        named_trees: &FxHashMap<Vec<u8>, BTree>,
        loaded_tree_meta: &FxHashMap<Vec<u8>, (PageId, u16)>,
        catalog_refreshed: &FxHashSet<u32>,
        force_commit: bool,
    ) -> Result<()> {
        let is_noop = !force_commit
            && pages.is_empty()
            && alloc.freed_this_txn().is_empty()
            && tree.root == old_slot.tree_root
            && tree.depth == old_slot.tree_depth
            && tree.entry_count == old_slot.tree_entries
            && catalog_root == old_slot.catalog_root;
        if is_noop {
            self.write_active.store(false, Ordering::SeqCst);
            return Ok(());
        }

        let (active_slot, reclaim_horizon, current_god_byte, cached_file_size) = {
            let state = self.state.lock();
            (
                state.active_slot,
                self.reclaim_horizon_locked(&state),
                state.cached_god_byte,
                state.cached_file_size,
            )
        };
        let inactive_slot_idx = 1 - active_slot;

        if self.sync_mode != citadel_core::types::SyncMode::Off {
            let recovery_god_byte = current_god_byte | GOD_BIT_RECOVERY;
            write_god_byte(&*self.io, recovery_god_byte)?;
        }

        let freed_this_txn = alloc.commit();

        // Freed pages are unreachable via tree; don't encrypt+write them.
        for &page_id in &freed_this_txn {
            pages.remove(&page_id);
        }

        // Every sync mode gates reuse on the reader horizon (Off relaxes
        // durability, not isolation). Consumption is the loan minus this
        // remainder, which also supplies the chain rewrite's structure pages.
        let mut loan_pool = alloc.take_ready_to_use();
        let consumed: FxHashSet<PageId> = {
            // Set lookup: a Vec::contains scan here is quadratic in the
            // reclaimed batch (a bulk DELETE can loan 100k+ pages).
            let remainder: FxHashSet<PageId> = loan_pool.iter().copied().collect();
            let state = self.state.lock();
            state
                .reclaimed_pages
                .iter()
                .filter(|page_id| !remainder.contains(page_id))
                .copied()
                .collect()
        };
        let (new_pf_root, available) = {
            self.load_pending_free_chain(pages, old_slot.pending_free_root)?;
            pending_free::process_chain(
                pages,
                alloc,
                &mut loan_pool,
                &pending_free::ChainCommit {
                    txn_id,
                    current_root: old_slot.pending_free_root,
                    freed_this_txn: &freed_this_txn,
                    consumed: &consumed,
                    reclaim_horizon,
                },
            )?
        };

        let merkle_root_hash = if self.sync_mode != citadel_core::types::SyncMode::Off {
            let hash =
                crate::merkle::compute_tree_merkle(pages, tree.root, base_txn_id, &|page_id| {
                    self.fetch_merkle_hash(page_id)
                })?;

            let read_hash = &|page_id| self.fetch_merkle_hash(page_id);
            for named_tree in named_trees.values() {
                if named_tree.root != PageId::INVALID {
                    crate::merkle::compute_tree_merkle(
                        pages,
                        named_tree.root,
                        base_txn_id,
                        read_hash,
                    )?;
                }
            }

            if catalog_root != PageId::INVALID && catalog_root != old_slot.catalog_root {
                crate::merkle::compute_tree_merkle(pages, catalog_root, base_txn_id, read_hash)?;
            }
            hash
        } else {
            // Off skips Merkle recompute, but dirty pages keep their pre-edit
            // hash (cow_page clones the header). Zero it so merkle_diff can't
            // prune a changed subtree as identical (zero forces traversal).
            for page in pages.values_mut() {
                if page.txn_id() >= base_txn_id {
                    page.set_merkle_hash(&[0u8; citadel_core::MERKLE_HASH_SIZE]);
                }
            }
            [0u8; citadel_core::MERKLE_HASH_SIZE]
        };

        let mut dirty_page_info: Vec<(u64, PageId)> = Vec::with_capacity(pages.len());
        let mut max_offset = 0u64;
        for page in pages.values_mut() {
            if page.txn_id() >= base_txn_id {
                page.update_checksum();
                let page_id = page.page_id();
                let offset = page_offset(page_id);
                max_offset = max_offset.max(offset);
                dirty_page_info.push((offset, page_id));
            }
        }
        let mut new_file_size = cached_file_size;
        if !dirty_page_info.is_empty() {
            let needed = max_offset + PAGE_SIZE as u64;
            if cached_file_size < needed {
                ensure_file_size(&*self.io, max_offset)?;
                new_file_size = self.io.file_size()?;
            }
        }

        let hmac_state = &self.hmac_state;
        if !dirty_page_info.is_empty() {
            // encrypt_page_with_hmac overwrites every output byte: no
            // re-zeroing.
            let mut arena = self.commit_arena.lock();
            let arena_len = COMMIT_ARENA_PAGES.min(dirty_page_info.len()) * PAGE_SIZE;
            if arena.len() < arena_len {
                arena.resize(arena_len, 0);
            }
            for chunk in dirty_page_info.chunks(COMMIT_ARENA_PAGES) {
                let bufs = &mut arena[..chunk.len() * PAGE_SIZE];
                let encrypt_one = |(dst, &(_, page_id)): (&mut [u8], &(u64, PageId))| {
                    let page = &pages[&page_id];
                    page_cipher::encrypt_page_with_hmac(
                        &self.dek,
                        hmac_state,
                        page_id,
                        page.as_bytes(),
                        dst.try_into().expect("arena chunk is PAGE_SIZE"),
                    );
                };
                #[cfg(feature = "parallel")]
                {
                    use rayon::prelude::*;
                    bufs.par_chunks_exact_mut(PAGE_SIZE)
                        .zip(chunk.par_iter())
                        .for_each(encrypt_one);
                }
                #[cfg(not(feature = "parallel"))]
                bufs.chunks_exact_mut(PAGE_SIZE)
                    .zip(chunk.iter())
                    .for_each(encrypt_one);

                if let [(offset, _)] = chunk {
                    let buf: &[u8; PAGE_SIZE] = (&arena[..PAGE_SIZE]).try_into().unwrap();
                    self.io.write_page(*offset, buf)?;
                } else {
                    let refs: Vec<(u64, &[u8; PAGE_SIZE])> = chunk
                        .iter()
                        .zip(arena.chunks_exact(PAGE_SIZE))
                        .map(|(&(offset, _), buf)| (offset, buf.try_into().unwrap()))
                        .collect();
                    self.io.write_pages_ref(&refs)?;
                }
            }
        }

        // Secure delete: zero freed pages past all readers (reader- and
        // crash-safe: unreferenced by either slot). Zeros ride the commit
        // fsync; the watermark zeroes each page once as it becomes available.
        let zeroed_watermark = if self.secure_delete.load(Ordering::Relaxed) {
            let zeroed_up_to = self.state.lock().zeroed_up_to;
            let zeros = [0u8; PAGE_SIZE];
            let mut high = zeroed_up_to;
            for entry in &available {
                if entry.freed_at_txn.as_u64() > zeroed_up_to.as_u64() {
                    self.io.write_page(page_offset(entry.page_id), &zeros)?;
                    high = high.max(entry.freed_at_txn);
                }
            }
            Some(high)
        } else {
            None
        };

        let named_table_entries =
            Self::build_slot_entries(named_trees, loaded_tree_meta, old_slot, catalog_refreshed);

        let mut new_slot = CommitSlot {
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
            merkle_root: merkle_root_hash,
            named_table_entries,
            ..Default::default()
        };
        new_slot.seal(&self.mac_key);
        // Backstop: writer paths bound the stale set so seal() picks V1, but
        // if one ever slips a legacy slot into a flagged file, fail loudly -
        // a silent legacy slot bricks the file at the next open.
        if new_slot.slot_format != file_manager::SlotFormat::V1
            && self.slots_flagged.load(Ordering::Acquire)
        {
            return Err(Error::LegacySlotWriteOnV1File);
        }
        let new_god_byte = inactive_slot_idx as u8 & GOD_BIT_ACTIVE_SLOT;

        if self.sync_mode == citadel_core::types::SyncMode::Off {
            let slot_offset = citadel_core::COMMIT_SLOT_OFFSET
                + inactive_slot_idx * citadel_core::COMMIT_SLOT_SIZE;
            let god_offset = citadel_core::GOD_BYTE_OFFSET;
            let slot_buf = new_slot.serialize();
            self.io.write_commit_meta(
                god_offset as u64,
                new_god_byte,
                slot_offset as u64,
                &slot_buf,
            )?;
        } else {
            write_commit_slot(&*self.io, inactive_slot_idx, &new_slot)?;
            self.io.fsync()?;
            write_god_byte(&*self.io, new_god_byte)?;
        }

        if self.sync_mode == citadel_core::types::SyncMode::Full {
            if let Err(e) = self.io.fsync() {
                let _ = write_god_byte(&*self.io, current_god_byte);
                let _ = self.io.fsync();
                return Err(e);
            }
        }

        {
            let mut pool = self.pool.lock();
            for &(_, page_id) in &dirty_page_info {
                pool.invalidate(page_id);
                if let Some(page) = pages.remove(&page_id) {
                    pool.insert_if_absent(page_id, Arc::new(page));
                }
            }
        }

        {
            let mut state = self.state.lock();
            state.active_slot = inactive_slot_idx;
            state.current_slot = Arc::new(new_slot);
            state.cached_god_byte = new_god_byte;
            state.cached_file_size = new_file_size;
            // Availability is re-derived from the durable chain every commit,
            // so an abort, no-op commit, or shutdown strands nothing.
            state.reclaimed_pages = available.iter().map(|entry| entry.page_id).collect();
            if let Some(watermark) = zeroed_watermark {
                state.zeroed_up_to = watermark;
            }
            state.recycled_pages = Some(std::mem::take(pages));
            self.commit_generation.fetch_add(1, Ordering::Release);
        }

        self.write_active.store(false, Ordering::SeqCst);

        Ok(())
    }

    pub(crate) fn abort_write(&self) {
        self.write_active.store(false, Ordering::SeqCst);
    }

    pub(crate) fn unregister_reader(&self, snapshot_txn_id: TxnId) {
        let mut state = self.state.lock();
        if let Some(count) = state.reader_table.get_mut(&snapshot_txn_id) {
            *count -= 1;
            if *count == 0 {
                state.reader_table.remove(&snapshot_txn_id);
            }
        }
    }

    /// Min snapshot id over active readers (unbounded with none). A page
    /// freed by txn F is referenced only by snapshots S < F, so reclaim is
    /// safe iff F <= this horizon.
    pub fn reclaim_horizon(&self) -> TxnId {
        let state = self.state.lock();
        self.reclaim_horizon_locked(&state)
    }

    fn reclaim_horizon_locked(&self, state: &ManagerState) -> TxnId {
        state
            .reader_table
            .keys()
            .next()
            .copied()
            .unwrap_or(TxnId(u64::MAX))
    }

    pub fn current_slot(&self) -> CommitSlot {
        self.state.lock().current_slot.as_ref().clone()
    }

    pub fn reader_count(&self) -> usize {
        // Refcount sum, not key count: readers sharing a snapshot share a key.
        self.state.lock().reader_table.values().sum()
    }

    pub fn list_tables(&self) -> Result<Vec<(Vec<u8>, TableDescriptor)>> {
        use citadel_core::types::ValueType;
        use citadel_page::{branch_node, leaf_node};

        let slot = self.current_slot();
        if !slot.catalog_root.is_valid() {
            return Ok(Vec::new());
        }

        let mut tables = Vec::new();
        let mut stack = vec![slot.catalog_root];
        while let Some(page_id) = stack.pop() {
            let page = self.read_page_from_disk(page_id)?;
            match page.page_type() {
                Some(citadel_core::types::PageType::Leaf) => {
                    for i in 0..page.num_cells() {
                        let cell = leaf_node::read_cell(&page, i);
                        if cell.val_type != ValueType::Tombstone
                            && cell.value.len() >= crate::catalog::TABLE_DESCRIPTOR_SIZE
                        {
                            let desc = TableDescriptor::deserialize(cell.value);
                            tables.push((cell.key.to_vec(), desc));
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
        Ok(tables)
    }

    pub fn table_root(&self, name: &[u8]) -> Result<Option<PageId>> {
        use citadel_core::types::ValueType;
        use citadel_page::{branch_node, leaf_node};

        let slot = self.current_slot();
        if !slot.catalog_root.is_valid() {
            return Ok(None);
        }

        let mut stack = vec![slot.catalog_root];
        while let Some(page_id) = stack.pop() {
            let page = self.read_page_from_disk(page_id)?;
            match page.page_type() {
                Some(citadel_core::types::PageType::Leaf) => {
                    for i in 0..page.num_cells() {
                        let cell = leaf_node::read_cell(&page, i);
                        if cell.key == name
                            && cell.val_type != ValueType::Tombstone
                            && cell.value.len() >= crate::catalog::TABLE_DESCRIPTOR_SIZE
                        {
                            let desc = TableDescriptor::deserialize(cell.value);
                            return Ok(Some(desc.root_page));
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
        Ok(None)
    }

    pub fn integrity_check(&self) -> Result<IntegrityReport> {
        integrity::run_integrity_check(self)
    }

    pub fn backup_to(&self, dest_io: &dyn PageIO) -> Result<()> {
        use std::collections::HashSet;
        let slot = self.current_slot();

        let mut reachable = HashSet::new();
        self.collect_tree_pages(slot.tree_root, &mut reachable)?;

        if slot.catalog_root.is_valid() {
            let table_roots = self.collect_catalog_pages(slot.catalog_root, &mut reachable)?;
            for root in table_roots {
                self.collect_tree_pages(root, &mut reachable)?;
            }
        }

        // After a SyncMode::Off catalog skip, a slot entry is the sole record
        // of a table's CURRENT root; the (stale) catalog descriptor alone
        // would omit the live subtree from the backup.
        for &(_, _, root, _) in &slot.named_table_entries {
            if root != 0 {
                self.collect_tree_pages(PageId(root), &mut reachable)?;
            }
        }

        if slot.pending_free_root.is_valid() {
            self.collect_chain_pages(slot.pending_free_root, &mut reachable)?;
        }

        let mut header_buf = [0u8; citadel_core::FILE_HEADER_SIZE];
        self.io.read_at(0, &mut header_buf)?;
        let mut header = file_manager::FileHeader::deserialize(&header_buf)?;
        // Re-seal: upgrades a slot inherited from a legacy-format source file.
        let mut slot = slot;
        slot.seal(&self.mac_key);
        header.slots = [slot.clone(), slot];
        header.god_byte = 0;

        let max_page = reachable.iter().map(|p| p.as_u32()).max().unwrap_or(0);
        let needed_size =
            citadel_core::FILE_HEADER_SIZE as u64 + (max_page as u64 + 1) * PAGE_SIZE as u64;
        dest_io.truncate(needed_size)?;
        dest_io.write_at(0, &header.serialize())?;

        for &page_id in &reachable {
            let offset = page_offset(page_id);
            let mut buf = [0u8; PAGE_SIZE];
            self.io.read_page(offset, &mut buf)?;
            dest_io.write_page(offset, &buf)?;
        }

        dest_io.fsync()?;
        Ok(())
    }

    pub fn compact_to(&self, dest_io: &dyn PageIO) -> Result<()> {
        use citadel_core::types::ValueType;
        use citadel_page::{branch_node, leaf_node};
        use std::collections::HashSet;

        let slot = self.current_slot();
        let mut next_id: u32 = 0;
        let mut old_to_new: FxHashMap<PageId, PageId> = FxHashMap::default();
        let mut catalog_leaves: HashSet<PageId> = HashSet::new();

        // After an Off catalog skip the slot entry, not the stale descriptor,
        // holds the current root/count/depth; rewrite the compacted catalog
        // from it or the skip commits' rows vanish from the copy.
        let slot_overrides: FxHashMap<u32, (PageId, u64, u16)> = slot
            .named_table_entries
            .iter()
            .filter(|&&(_, _, root, _)| root != 0)
            .map(|&(hash, count, root, depth)| {
                (hash, (PageId(root), count & !SLOT_ENTRY_STALE, depth))
            })
            .collect();

        self.assign_new_ids(slot.tree_root, &mut old_to_new, &mut next_id)?;

        if slot.catalog_root.is_valid() {
            let table_roots = {
                let mut reachable = HashSet::new();
                self.collect_catalog_pages(slot.catalog_root, &mut reachable)?
            };

            self.assign_new_ids(slot.catalog_root, &mut old_to_new, &mut next_id)?;

            self.collect_catalog_leaf_pages(slot.catalog_root, &mut catalog_leaves)?;

            for root in &table_roots {
                self.assign_new_ids(*root, &mut old_to_new, &mut next_id)?;
            }
        }
        for &(root, ..) in slot_overrides.values() {
            self.assign_new_ids(root, &mut old_to_new, &mut next_id)?;
        }

        let total_pages = next_id;
        let needed_size =
            citadel_core::FILE_HEADER_SIZE as u64 + total_pages as u64 * PAGE_SIZE as u64;
        dest_io.truncate(needed_size)?;

        let mut root_merkle = [0u8; citadel_core::MERKLE_HASH_SIZE];
        for (&old_id, &new_id) in &old_to_new {
            let mut page = self.read_page_from_disk(old_id)?;

            page.set_page_id(new_id);

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

            if catalog_leaves.contains(&old_id) {
                for i in 0..page.num_cells() {
                    let cell = leaf_node::read_cell(&page, i);
                    if cell.val_type != ValueType::Tombstone
                        && cell.value.len() >= crate::catalog::TABLE_DESCRIPTOR_SIZE
                    {
                        let desc = TableDescriptor::deserialize(cell.value);
                        let hash = file_manager::table_name_hash(cell.key);
                        let cell_off = page.cell_offset(i) as usize;
                        let key_len = u16::from_le_bytes(
                            page.data[cell_off..cell_off + 2].try_into().unwrap(),
                        ) as usize;
                        let value_start = cell_off + 6 + key_len + 1;
                        // Slot entry wins over a (possibly stale) descriptor:
                        // rewrite root, count, and depth from the entry so
                        // the compacted catalog is current.
                        if let Some(&(cur_root, cur_count, cur_depth)) = slot_overrides.get(&hash) {
                            let new_root = old_to_new
                                .get(&cur_root)
                                .copied()
                                .ok_or(citadel_core::Error::PageOutOfBounds(cur_root))?;
                            page.data[value_start..value_start + 4]
                                .copy_from_slice(&new_root.as_u32().to_le_bytes());
                            page.data[value_start + 4..value_start + 12]
                                .copy_from_slice(&cur_count.to_le_bytes());
                            page.data[value_start + 12..value_start + 14]
                                .copy_from_slice(&cur_depth.to_le_bytes());
                        } else if let Some(&new_root) = old_to_new.get(&desc.root_page) {
                            page.data[value_start..value_start + 4]
                                .copy_from_slice(&new_root.as_u32().to_le_bytes());
                        }
                    }
                }
            }

            page.update_checksum();

            if old_id == slot.tree_root {
                root_merkle = page.merkle_hash();
            }

            let offset = page_offset(new_id);
            let mut encrypted = [0u8; PAGE_SIZE];
            page_cipher::encrypt_page(
                &self.dek,
                &self.mac_key,
                new_id,
                self.epoch,
                page.as_bytes(),
                &mut encrypted,
            );
            dest_io.write_page(offset, &encrypted)?;
        }

        let mut header_buf = [0u8; citadel_core::FILE_HEADER_SIZE];
        self.io.read_at(0, &mut header_buf)?;
        let mut header = file_manager::FileHeader::deserialize(&header_buf)?;

        let new_tree_root = old_to_new
            .get(&slot.tree_root)
            .copied()
            .unwrap_or(PageId(0));
        let new_catalog_root = if slot.catalog_root.is_valid() {
            old_to_new
                .get(&slot.catalog_root)
                .copied()
                .unwrap_or(PageId::INVALID)
        } else {
            PageId::INVALID
        };

        let mut new_slot = CommitSlot {
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
            merkle_root: root_merkle,
            // Root/depth zeroed (cache rebuilt on demand) and the stale flag
            // stripped: the compacted catalog was rewritten from the slot
            // entries above, so every descriptor is current again.
            named_table_entries: slot
                .named_table_entries
                .iter()
                .map(|&(hash, count, _, _)| (hash, count & !SLOT_ENTRY_STALE, 0, 0))
                .collect(),
            ..Default::default()
        };
        new_slot.seal(&self.mac_key);

        header.slots = [new_slot.clone(), new_slot];
        header.god_byte = 0;

        dest_io.write_at(0, &header.serialize())?;
        dest_io.fsync()?;

        Ok(())
    }

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

    fn collect_catalog_pages(
        &self,
        catalog_root: PageId,
        reachable: &mut std::collections::HashSet<PageId>,
    ) -> Result<Vec<PageId>> {
        use citadel_core::types::ValueType;
        use citadel_page::{branch_node, leaf_node};

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

    fn assign_new_ids(
        &self,
        root: PageId,
        mapping: &mut FxHashMap<PageId, PageId>,
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

    fn load_pending_free_chain(
        &self,
        pages: &mut FxHashMap<PageId, Page>,
        root: PageId,
    ) -> Result<()> {
        if !root.is_valid() {
            return Ok(());
        }

        let mut current = root;
        while current.is_valid() {
            if let std::collections::hash_map::Entry::Vacant(e) = pages.entry(current) {
                let page = self.fetch_page_owned(current)?;
                let next = page.right_child();
                e.insert(page);
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

    pub(crate) fn fetch_page_owned(&self, page_id: PageId) -> Result<Page> {
        {
            let mut pool = self.pool.lock();
            if let Some(arc) = pool.get_cached(page_id) {
                return Ok((*arc).clone());
            }
        }
        self.read_page_from_disk(page_id)
    }

    pub(crate) fn fetch_merkle_hash(
        &self,
        page_id: PageId,
    ) -> Result<[u8; citadel_core::MERKLE_HASH_SIZE]> {
        {
            let mut pool = self.pool.lock();
            if let Some(arc) = pool.get_cached(page_id) {
                return Ok(arc.merkle_hash());
            }
        }
        let page = self.read_page_from_disk(page_id)?;
        Ok(page.merkle_hash())
    }

    pub fn read_page_from_disk(&self, page_id: PageId) -> Result<Page> {
        let offset = page_offset(page_id);
        let mut encrypted = [0u8; PAGE_SIZE];
        self.io.read_page(offset, &mut encrypted)?;

        let mut body = [0u8; BODY_SIZE];
        page_cipher::decrypt_page(
            &self.dek,
            &self.mac_key,
            page_id,
            self.epoch,
            &encrypted,
            &mut body,
        )?;

        let page = Page::from_bytes(body);
        if !page.verify_checksum() {
            return Err(Error::ChecksumMismatch(page_id));
        }

        Ok(page)
    }
}

impl TxnManager {
    fn wipe_keys(&mut self) {
        use zeroize::Zeroize;
        self.dek.zeroize();
        self.mac_key.zeroize();
        // hmac_state still holds a key-derived HMAC inner state; the hmac
        // crate exposes no way to wipe it, so that residue remains.
    }
}

impl Drop for TxnManager {
    fn drop(&mut self) {
        self.wipe_keys();
    }
}

#[cfg(test)]
#[path = "manager_tests.rs"]
pub(crate) mod tests;
