//! Transaction manager: single-writer MVCC with shadow-paging commit.

use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use std::sync::{Arc, OnceLock};

use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::btree::BTree;
use citadel_buffer::pool::BufferPool;
use citadel_core::types::{PageId, PageType, TxnId};
use citadel_core::{
    CancelToken, Error, Result, BODY_SIZE, DEK_SIZE, GOD_BIT_ACTIVE_SLOT, GOD_BIT_RECOVERY,
    MAC_KEY_SIZE, PAGE_SIZE, SLOT_ENTRY_STALE, SLOT_NAMED_MAX_ENTRIES_V1,
};
use citadel_crypto::page_cipher;
use citadel_io::file_manager::{
    self, ensure_file_size, page_offset, write_commit_slot, write_god_byte, CommitSlot,
    MerkleScheme,
};
use citadel_io::traits::PageIO;
use citadel_page::page::Page;

use crate::catalog::{ResolvedCatalog, TableDescriptor};
use crate::integrity::{self, IntegrityReport};
use crate::pending_free;
use crate::read_txn::ReadTxn;
use crate::write_txn::WriteTxn;

static NEXT_MANAGER_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) const SCAN_CACHE_BATCH_SIZE: usize = 32;

type NamedTableHashCollisions = FxHashMap<u32, (Vec<u8>, Vec<u8>)>;

fn allocate_manager_id() -> u64 {
    NEXT_MANAGER_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .expect("transaction-manager id space exhausted")
}

fn record_named_table_hash(
    name: &[u8],
    first_by_hash: &mut FxHashMap<u32, Vec<u8>>,
    collisions: &mut NamedTableHashCollisions,
) {
    let hash = file_manager::table_name_hash(name);
    if let Some(first) = first_by_hash.get(&hash) {
        if first.as_slice() != name {
            collisions
                .entry(hash)
                .or_insert_with(|| (first.clone(), name.to_vec()));
        }
    } else {
        first_by_hash.insert(hash, name.to_vec());
    }
}

/// Catalog values are fixed-size inline records. Runtime walkers must reject
/// malformed cells instead of either treating an overflow reference as a
/// descriptor or letting the indexing contract of `deserialize` panic.
fn decode_catalog_descriptor(
    value_type: citadel_core::types::ValueType,
    value: &[u8],
) -> Result<TableDescriptor> {
    if value_type != citadel_core::types::ValueType::Inline {
        return Err(Error::DatabaseCorrupted);
    }
    TableDescriptor::try_deserialize(value).ok_or(Error::DatabaseCorrupted)
}

#[derive(Clone, Copy)]
struct CheckedBranchCellLocation {
    child: PageId,
    child_offset: usize,
}

#[derive(Clone, Copy)]
struct CheckedLeafCellLocation {
    key_start: usize,
    key_len: usize,
    value_type: citadel_core::types::ValueType,
    value_start: usize,
    value_len: usize,
}

impl CheckedLeafCellLocation {
    fn key<'a>(&self, page: &'a Page) -> &'a [u8] {
        &page.data[self.key_start..self.key_start + self.key_len]
    }

    fn value<'a>(&self, page: &'a Page) -> &'a [u8] {
        &page.data[self.value_start..self.value_start + self.value_len]
    }
}

fn checked_cell_offsets(page: &Page) -> Result<Vec<usize>> {
    let count = page.num_cells() as usize;
    let pointer_end = citadel_core::PAGE_HEADER_SIZE
        .checked_add(count.checked_mul(2).ok_or(Error::DatabaseCorrupted)?)
        .ok_or(Error::DatabaseCorrupted)?;
    if pointer_end > BODY_SIZE {
        return Err(Error::DatabaseCorrupted);
    }

    let cell_area_start = page.cell_area_start() as usize;
    if cell_area_start < pointer_end || cell_area_start > BODY_SIZE {
        return Err(Error::DatabaseCorrupted);
    }

    let mut offsets = Vec::with_capacity(count);
    for index in 0..count {
        let pointer = citadel_core::PAGE_HEADER_SIZE + index * 2;
        let offset = u16::from_le_bytes([page.data[pointer], page.data[pointer + 1]]) as usize;
        if offset < cell_area_start || offset >= BODY_SIZE {
            return Err(Error::DatabaseCorrupted);
        }
        offsets.push(offset);
    }
    Ok(offsets)
}

fn checked_branch_cell_locations(page: &Page) -> Result<Vec<CheckedBranchCellLocation>> {
    let offsets = checked_cell_offsets(page)?;
    let mut cells = Vec::with_capacity(offsets.len());
    let mut spans = Vec::with_capacity(offsets.len());
    for offset in offsets {
        let fixed_end = offset.checked_add(6).ok_or(Error::DatabaseCorrupted)?;
        if fixed_end > BODY_SIZE {
            return Err(Error::DatabaseCorrupted);
        }
        let key_len = u16::from_le_bytes([page.data[offset + 4], page.data[offset + 5]]) as usize;
        let end = fixed_end
            .checked_add(key_len)
            .filter(|&end| end <= BODY_SIZE)
            .ok_or(Error::DatabaseCorrupted)?;
        spans.push((offset, end));
        cells.push(CheckedBranchCellLocation {
            child: PageId(u32::from_le_bytes([
                page.data[offset],
                page.data[offset + 1],
                page.data[offset + 2],
                page.data[offset + 3],
            ])),
            child_offset: offset,
        });
    }
    spans.sort_unstable_by_key(|&(start, _)| start);
    if spans.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(Error::DatabaseCorrupted);
    }
    Ok(cells)
}

fn checked_leaf_cell_locations(page: &Page) -> Result<Vec<CheckedLeafCellLocation>> {
    let offsets = checked_cell_offsets(page)?;
    let mut cells = Vec::with_capacity(offsets.len());
    let mut spans = Vec::with_capacity(offsets.len());
    for offset in offsets {
        let fixed_end = offset.checked_add(6).ok_or(Error::DatabaseCorrupted)?;
        if fixed_end > BODY_SIZE {
            return Err(Error::DatabaseCorrupted);
        }
        let key_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
        let value_len = u32::from_le_bytes([
            page.data[offset + 2],
            page.data[offset + 3],
            page.data[offset + 4],
            page.data[offset + 5],
        ]) as usize;
        let value_type_offset = fixed_end
            .checked_add(key_len)
            .filter(|&offset| offset < BODY_SIZE)
            .ok_or(Error::DatabaseCorrupted)?;
        let end = value_type_offset
            .checked_add(1)
            .and_then(|start| start.checked_add(value_len))
            .filter(|&end| end <= BODY_SIZE)
            .ok_or(Error::DatabaseCorrupted)?;
        let value_type = citadel_core::types::ValueType::from_u8(page.data[value_type_offset])
            .ok_or(Error::DatabaseCorrupted)?;
        spans.push((offset, end));
        cells.push(CheckedLeafCellLocation {
            key_start: fixed_end,
            key_len,
            value_type,
            value_start: value_type_offset + 1,
            value_len,
        });
    }
    spans.sort_unstable_by_key(|&(start, _)| start);
    if spans.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(Error::DatabaseCorrupted);
    }
    Ok(cells)
}

fn checked_overflow_reference(page: &Page, cell: CheckedLeafCellLocation) -> Result<(PageId, u32)> {
    if cell.value_len != 8 {
        return Err(Error::CorruptOverflowChain(format!(
            "overflow reference on page {} has {} bytes instead of 8",
            page.page_id(),
            cell.value_len
        )));
    }
    let value = cell.value(page);
    let first_page = PageId(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
    let total_len = u32::from_le_bytes([value[4], value[5], value[6], value[7]]);
    Ok((first_page, total_len))
}

struct ActiveScanMeasurement {
    manager_id: u64,
    counter: Arc<AtomicU64>,
}

thread_local! {
    static ACTIVE_SCAN_MEASUREMENTS: RefCell<Vec<ActiveScanMeasurement>> =
        const { RefCell::new(Vec::new()) };
}

/// Operation-local count of storage entries examined by scans.
///
/// Thread-bound: nested guards each receive the rows scanned while they are
/// active, and never observe another thread's or another database's scans.
#[must_use = "dropping the guard ends the scan measurement"]
pub struct ScanMeasurement {
    manager_id: u64,
    counter: Arc<AtomicU64>,
    _not_send: PhantomData<Rc<()>>,
}

impl ScanMeasurement {
    /// Rows flushed by completed scans in this operation. Counts publish when a
    /// scan guard or pull iterator drops, so this is not a live progress counter.
    pub fn rows_scanned(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    pub(crate) fn weak_counter(&self) -> std::sync::Weak<AtomicU64> {
        Arc::downgrade(&self.counter)
    }
}

impl Drop for ScanMeasurement {
    fn drop(&mut self) {
        let _ = ACTIVE_SCAN_MEASUREMENTS.try_with(|measurements| {
            let mut measurements = measurements.borrow_mut();
            let index = measurements.iter().rposition(|entry| {
                entry.manager_id == self.manager_id && Arc::ptr_eq(&entry.counter, &self.counter)
            });
            debug_assert!(
                index.is_some(),
                "scan measurement missing from its thread-local stack"
            );
            if let Some(index) = index {
                measurements.remove(index);
            }
        });
    }
}

pub struct TxnManager {
    id: u64,
    io: Box<dyn PageIO>,
    dek: [u8; DEK_SIZE],
    mac_key: [u8; MAC_KEY_SIZE],
    epoch: u32,
    pool: Mutex<BufferPool>,
    next_txn_id: AtomicU64,
    commit_generation: AtomicU64,
    /// Database-wide storage entries examined since this manager opened.
    /// Monotonic telemetry only; operation-local measurements use the
    /// thread-local counters above. Each scan flushes both sets once on drop.
    rows_scanned: AtomicU64,
    /// Full catalog names for hashes that are ambiguous in legacy databases.
    /// New DDL prevents these; the first named-table access populates the map,
    /// so open stays O(1) and later hash-only lookups need no catalog scan.
    named_table_hash_collisions: OnceLock<NamedTableHashCollisions>,
    named_table_hash_collision_init: Mutex<()>,
    write_active: AtomicBool,
    /// Effective V1 requirement: either the compatibility data-header bit or
    /// an authenticated key-file requirement supplied by the facade.
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

/// Exclusive access to commit-slot metadata while writers are blocked.
///
/// Slot promotion spans multiple reads and durable writes, so keeping them on
/// this guard makes it impossible to run part of that sequence uncommitted.
#[must_use = "dropping the guard releases writer exclusion"]
pub struct WriterExclusion<'a> {
    manager: &'a TxnManager,
}

impl WriterExclusion<'_> {
    pub fn both_slots_v1(&self) -> Result<bool> {
        (0..2).try_fold(true, |all_v1, idx| {
            file_manager::read_commit_slot(&*self.manager.io, idx).map(|slot| {
                all_v1
                    && slot.slot_format == file_manager::SlotFormat::V1
                    && slot.verify_checksum()
                    && slot.verify_mac(&self.manager.mac_key)
            })
        })
    }

    /// Verify both slots and activate the in-memory V1 backstop.
    pub fn require_authenticated_v1(&self) -> Result<()> {
        if !self.both_slots_v1()? {
            return Err(Error::DatabaseCorrupted);
        }
        self.manager.slots_flagged.store(true, Ordering::Release);
        Ok(())
    }

    /// Stamp the one-way compatibility flag after both slots are V1.
    pub fn mark_slots_v1(&self) -> Result<bool> {
        let flagged =
            file_manager::mark_slots_v1_if_upgraded(&*self.manager.io, &self.manager.mac_key)?;
        if flagged {
            self.manager.slots_flagged.store(true, Ordering::Release);
        }
        Ok(flagged)
    }
}

impl Drop for WriterExclusion<'_> {
    fn drop(&mut self) {
        self.manager.write_active.store(false, Ordering::SeqCst);
    }
}

/// Commit encrypt/write chunk size; bounds arena retention and transient
/// memory.
const COMMIT_ARENA_PAGES: usize = 64;

enum PendingFreePage<'a> {
    Borrowed(&'a Page),
    Cached(Arc<Page>),
}

impl std::ops::Deref for PendingFreePage<'_> {
    type Target = Page;

    fn deref(&self) -> &Page {
        match self {
            Self::Borrowed(page) => page,
            Self::Cached(page) => page,
        }
    }
}

struct ManagerState {
    active_slot: usize,
    current_slot: Arc<CommitSlot>,
    /// Replaced on every root transition, even when a physical root ID is reused.
    resolved_catalog: Arc<ResolvedCatalog>,
    cached_god_byte: u8,
    cached_file_size: u64,
    /// Active readers keyed by SNAPSHOT txn id (not the reader's own id), so
    /// the reclaim horizon is the min snapshot still referenced. Values are
    /// refcounts: concurrent readers share a snapshot.
    reader_table: BTreeMap<TxnId, usize>,
    /// Reusable free pages, a RAM cache of the durable pending-free chain:
    /// shared immutably with the writer and re-derived every commit, so an
    /// abort/no-op/shutdown never strands a page.
    reclaimed_pages: Arc<Vec<PageId>>,
    /// Known metadata retirements retain their durable age but need no data
    /// reader horizon. Empty on reopen: no pre-open reader can survive it.
    retired_chain_pages: FxHashMap<PageId, TxnId>,
    /// Secure delete: highest freed_at_txn whose available data pages have been
    /// zero-filled. RAM-only; a reopen re-zeroes once, which is harmless.
    zeroed_up_to: TxnId,
    zeroed_chain_up_to: TxnId,
    recycled_pages: Option<FxHashMap<PageId, Page>>,
}

/// A stable on-disk commit-slot snapshot held while writers are excluded.
///
/// Both slots come from one header read, and the exclusion is held for this
/// value's lifetime, so no commit can flip the active slot or recycle pages.
pub(crate) struct IntegritySnapshot<'a> {
    exclusion: WriterExclusion<'a>,
    active_slot: usize,
    v1_required: bool,
    slots: [CommitSlot; 2],
}

impl IntegritySnapshot<'_> {
    pub(crate) fn active_slot(&self) -> usize {
        self.active_slot
    }

    pub(crate) fn slots(&self) -> &[CommitSlot; 2] {
        &self.slots
    }

    pub(crate) fn v1_required(&self) -> bool {
        self.v1_required
    }

    pub(crate) fn slot_mac_valid(&self, slot: usize) -> bool {
        self.slots[slot].verify_mac(&self.exclusion.manager.mac_key)
    }
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

    /// Open while enforcing a V1 requirement authenticated by a higher layer.
    pub fn open_with_v1_requirement(
        io: Box<dyn PageIO>,
        dek: [u8; DEK_SIZE],
        mac_key: [u8; MAC_KEY_SIZE],
        epoch: u32,
        cache_size: usize,
        authenticated_v1_required: bool,
    ) -> Result<Self> {
        Self::open_with_sync_and_v1_requirement(
            io,
            dek,
            mac_key,
            epoch,
            cache_size,
            Default::default(),
            authenticated_v1_required,
        )
    }

    pub fn open_with_sync(
        io: Box<dyn PageIO>,
        dek: [u8; DEK_SIZE],
        mac_key: [u8; MAC_KEY_SIZE],
        epoch: u32,
        cache_size: usize,
        sync_mode: citadel_core::types::SyncMode,
    ) -> Result<Self> {
        Self::open_with_sync_and_v1_requirement(
            io, dek, mac_key, epoch, cache_size, sync_mode, false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn open_with_sync_and_v1_requirement(
        io: Box<dyn PageIO>,
        dek: [u8; DEK_SIZE],
        mac_key: [u8; MAC_KEY_SIZE],
        epoch: u32,
        cache_size: usize,
        sync_mode: citadel_core::types::SyncMode,
        authenticated_v1_required: bool,
    ) -> Result<Self> {
        let (active_slot, slot) =
            file_manager::recover_with_v1_requirement(&*io, &mac_key, authenticated_v1_required)?;
        // One-way: once both slots are sealed V1, legacy slots are rejected.
        let header_flagged = file_manager::mark_slots_v1_if_upgraded(&*io, &mac_key)?;
        let slots_flagged = authenticated_v1_required || header_flagged;
        let file_size = io.file_size()?;

        let next_txn_id = slot.txn_id.as_u64() + 1;

        Ok(Self {
            id: allocate_manager_id(),
            io,
            dek,
            mac_key,
            epoch,
            pool: Mutex::new(BufferPool::new(cache_size)),
            next_txn_id: AtomicU64::new(next_txn_id),
            commit_generation: AtomicU64::new(0),
            rows_scanned: AtomicU64::new(0),
            named_table_hash_collisions: OnceLock::new(),
            named_table_hash_collision_init: Mutex::new(()),
            write_active: AtomicBool::new(false),
            slots_flagged: AtomicBool::new(slots_flagged),
            state: Mutex::new(ManagerState {
                active_slot,
                current_slot: Arc::new(slot),
                resolved_catalog: Arc::default(),
                cached_god_byte: active_slot as u8 & GOD_BIT_ACTIVE_SLOT,
                cached_file_size: file_size,
                reader_table: BTreeMap::new(),
                reclaimed_pages: Arc::new(Vec::new()),
                retired_chain_pages: FxHashMap::default(),
                zeroed_up_to: TxnId(0),
                zeroed_chain_up_to: TxnId(0),
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
            merkle_scheme: citadel_io::file_manager::MerkleScheme::LogicalOverflowV1,
            ..Default::default()
        };
        slot.seal(&mac_key);
        write_commit_slot(&*io, 0, &slot)?;
        io.fsync()?;
        let file_size = io.file_size()?;

        Ok(Self {
            id: allocate_manager_id(),
            io,
            dek,
            mac_key,
            epoch,
            pool: Mutex::new(BufferPool::new(cache_size)),
            next_txn_id: AtomicU64::new(2),
            commit_generation: AtomicU64::new(0),
            rows_scanned: AtomicU64::new(0),
            named_table_hash_collisions: OnceLock::new(),
            named_table_hash_collision_init: Mutex::new(()),
            write_active: AtomicBool::new(false),
            // New files are flagged at birth (FileHeader::new).
            slots_flagged: AtomicBool::new(true),
            state: Mutex::new(ManagerState {
                active_slot: 0,
                current_slot: Arc::new(slot),
                resolved_catalog: Arc::default(),
                cached_god_byte: 0,
                cached_file_size: file_size,
                reader_table: BTreeMap::new(),
                reclaimed_pages: Arc::new(Vec::new()),
                retired_chain_pages: FxHashMap::default(),
                zeroed_up_to: TxnId(0),
                zeroed_chain_up_to: TxnId(0),
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
        let resolved_catalog = Arc::clone(&state.resolved_catalog);
        let commit_generation = self.commit_generation.load(Ordering::Acquire);

        // Key by snapshot id (see reader_table): a reader beginning mid-write
        // gets an id above the writer's but a snapshot predating its commit.
        *state.reader_table.entry(snapshot.txn_id).or_insert(0) += 1;

        ReadTxn::new(self, txn_id, snapshot, resolved_catalog, commit_generation)
    }

    pub fn commit_generation(&self) -> u64 {
        self.commit_generation.load(Ordering::Acquire)
    }

    /// Effective V1 requirement, including an authenticated key-file marker.
    pub fn slots_flagged(&self) -> bool {
        self.slots_flagged.load(Ordering::Acquire)
    }

    /// Exclude commits while a caller coordinates commit-slot metadata with
    /// another durable policy marker.
    pub fn exclude_writers(&self) -> Result<WriterExclusion<'_>> {
        if self
            .write_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::WriteTransactionActive);
        }
        Ok(WriterExclusion { manager: self })
    }

    pub fn begin_write(&self) -> Result<WriteTxn<'_>> {
        Ok(self
            .begin_write_inner(None)?
            .expect("an unconditional writer has no generation mismatch"))
    }

    /// Begin a writer only if no commit has occurred since `expected_generation`.
    /// The generation check happens while holding single-writer exclusion, so a
    /// writer cannot commit between the check and the returned transaction.
    #[doc(hidden)]
    pub fn begin_write_if_generation(
        &self,
        expected_generation: u64,
    ) -> Result<Option<WriteTxn<'_>>> {
        self.begin_write_inner(Some(expected_generation))
    }

    fn begin_write_inner(&self, expected_generation: Option<u64>) -> Result<Option<WriteTxn<'_>>> {
        if self
            .write_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::WriteTransactionActive);
        }

        if expected_generation
            .is_some_and(|expected| self.commit_generation.load(Ordering::Acquire) != expected)
        {
            self.write_active.store(false, Ordering::SeqCst);
            return Ok(None);
        }

        let mut state = self.state.lock();
        let txn_id = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));
        let snapshot = state.current_slot.clone();
        // Keep the shared loan in state and the durable chain until a commit
        // records its consumption.
        let reclaimed =
            (!state.reclaimed_pages.is_empty()).then(|| Arc::clone(&state.reclaimed_pages));
        let recycled = state.recycled_pages.take();
        drop(state);

        let alloc = match reclaimed {
            Some(pages) => PageAllocator::with_ready_pages(snapshot.high_water_mark, pages),
            None => PageAllocator::new(snapshot.high_water_mark),
        };

        let tree = BTree::from_existing(
            snapshot.tree_root,
            snapshot.tree_depth,
            snapshot.tree_entries,
        );

        Ok(Some(WriteTxn::new(
            self, txn_id, snapshot, tree, alloc, recycled,
        )))
    }

    pub(crate) fn fetch_page(&self, page_id: PageId) -> Result<Arc<Page>> {
        if let Some(arc) = self.pool.lock().get_cached(page_id) {
            return Ok(arc);
        }

        self.read_page_into_pool(page_id)
    }

    fn read_page_into_pool(&self, page_id: PageId) -> Result<Arc<Page>> {
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

    /// Pin a bounded run of cached leaves in traversal order. Missing pages
    /// are read only on demand; prefetched headers are checked on consumption.
    pub(crate) fn fetch_scan_page(
        &self,
        page_id: PageId,
        high_water_mark: u32,
        pending: &[PageId],
        cached_leaves: &mut Vec<(PageId, Arc<Page>)>,
    ) -> Result<Arc<Page>> {
        if page_id.as_u32() >= high_water_mark {
            return Err(Error::PageOutOfBounds(page_id));
        }
        debug_assert!(cached_leaves.is_empty());
        let cached = {
            let mut pool = self.pool.lock();
            let page = pool.get_cached(page_id);
            if let Some(page) = &page {
                if page.page_id() != page_id {
                    return Err(Error::DatabaseCorrupted);
                }
                if page.page_type() == Some(PageType::Leaf) {
                    for &id in pending.iter().rev().take(SCAN_CACHE_BATCH_SIZE - 1) {
                        if id.as_u32() >= high_water_mark {
                            break;
                        }
                        let Some(leaf) = pool.get_cached(id) else {
                            break;
                        };
                        if leaf.page_type() != Some(PageType::Leaf) {
                            break;
                        }
                        cached_leaves.push((id, leaf));
                    }
                    cached_leaves.reverse();
                }
            }
            page
        };
        let page = match cached {
            Some(page) => page,
            None => self.read_page_into_pool(page_id)?,
        };
        if page.page_id() != page_id {
            return Err(Error::DatabaseCorrupted);
        }
        Ok(page)
    }

    pub(crate) fn fetch_reachable_page(
        &self,
        page_id: PageId,
        high_water_mark: u32,
    ) -> Result<Arc<Page>> {
        if page_id.as_u32() >= high_water_mark {
            return Err(Error::PageOutOfBounds(page_id));
        }
        let page = self.fetch_page(page_id)?;
        if page.page_id() != page_id {
            return Err(Error::DatabaseCorrupted);
        }
        Ok(page)
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
    ) -> Result<u64> {
        // Write transactions also cache snapshot pages for reads. Only pages
        // owned by this transaction (new/COW pages carry a txn id at or above
        // its base id) are dirty; cached older pages must not turn an equal
        // CRDT comparison or other read-only writer into a physical commit.
        let has_dirty_pages = pages.values().any(|page| page.txn_id() >= base_txn_id);
        let is_noop = !force_commit
            && !has_dirty_pages
            && alloc.freed_this_txn().is_empty()
            && tree.root == old_slot.tree_root
            && tree.depth == old_slot.tree_depth
            && tree.entry_count == old_slot.tree_entries
            && catalog_root == old_slot.catalog_root;
        if is_noop {
            let generation = self.commit_generation.load(Ordering::Acquire);
            self.write_active.store(false, Ordering::SeqCst);
            return Ok(generation);
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

        // Validate durable reclaim metadata before touching allocator state or
        // the recovery marker, so a structural error leaves this process and
        // the next open on the unchanged committed slot.
        let pending_free = self.load_pending_free_chain(
            pages,
            old_slot.pending_free_root,
            old_slot.high_water_mark,
            old_slot.txn_id,
            alloc.ready_count(),
        )?;
        // Publish provenance only with the new slot. Failed commits must leave
        // the current slot's classifications unchanged.
        let mut retired_chain_pages = self.state.lock().retired_chain_pages.clone();

        if self.sync_mode != citadel_core::types::SyncMode::Off {
            let recovery_god_byte = current_god_byte | GOD_BIT_RECOVERY;
            write_god_byte(&*self.io, recovery_god_byte)?;
        }

        // Reclaimed allocations are below the committed high water mark;
        // fresh allocations start at it. The allocation log also follows
        // savepoint rollback, so discovery scales with this transaction's
        // allocations rather than the entire reusable-page pool.
        let consumed: FxHashSet<PageId> = alloc
            .allocated_this_txn()
            .iter()
            .copied()
            .filter(|page_id| page_id.as_u32() < old_slot.high_water_mark)
            .collect();
        let freed_this_txn = alloc.commit();

        // Freed pages are unreachable via tree; don't encrypt+write them.
        for &page_id in &freed_this_txn {
            pages.remove(&page_id);
        }

        // Data-page reuse respects readers; metadata needs only recovery-slot
        // protection. The unconsumed loan remainder supplies the chain
        // rewrite's structure pages.
        let mut loan_pool = alloc.take_ready_to_use();
        let (new_pf_root, available) = {
            pending_free.process_with_metadata(
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
                &mut retired_chain_pages,
            )?
        };

        let merkle_root_hash = if self.sync_mode != citadel_core::types::SyncMode::Off
            && old_slot.merkle_scheme == citadel_io::file_manager::MerkleScheme::LogicalOverflowV1
        {
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
            // Off mode and legacy Merkle slots cannot certify dirty subtrees.
            // Legacy hashes cover only the physical overflow reference, not
            // the payload, so an incremental rewrite cannot safely promote
            // the slot to the logical-overflow scheme. Zero dirty tree hashes
            // and keep the slot untrusted until a full compaction rebuild.
            for page in pages.values_mut() {
                if page.txn_id() >= base_txn_id
                    && matches!(
                        page.page_type(),
                        Some(citadel_core::types::PageType::Leaf)
                            | Some(citadel_core::types::PageType::Branch)
                    )
                {
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
                // The destination is a page-sized array by type, so the length is a
                // guarantee rather than a runtime check inside the encrypt loop.
                let encrypt_one = |(dst, &(_, page_id)): (&mut [u8; PAGE_SIZE], &(u64, PageId))| {
                    let page = &pages[&page_id];
                    page_cipher::encrypt_page_with_hmac(
                        &self.dek,
                        hmac_state,
                        page_id,
                        page.as_bytes(),
                        dst,
                    );
                };
                #[cfg(feature = "parallel")]
                {
                    use rayon::prelude::*;
                    // Rayon has no const-generic chunker, so the conversion lives here.
                    bufs.par_chunks_exact_mut(PAGE_SIZE)
                        .map(|dst| {
                            <&mut [u8; PAGE_SIZE]>::try_from(dst).expect("arena chunk is PAGE_SIZE")
                        })
                        .zip(chunk.par_iter())
                        .for_each(encrypt_one);
                }
                #[cfg(not(feature = "parallel"))]
                bufs.as_chunks_mut::<PAGE_SIZE>()
                    .0
                    .iter_mut()
                    .zip(chunk.iter())
                    .for_each(encrypt_one);

                if let [(offset, _)] = chunk {
                    let buf: &[u8; PAGE_SIZE] = (&arena[..PAGE_SIZE]).try_into().unwrap();
                    self.io.write_page(*offset, buf)?;
                } else {
                    let refs: Vec<(u64, &[u8; PAGE_SIZE])> = chunk
                        .iter()
                        .zip(arena.as_chunks::<PAGE_SIZE>().0)
                        .map(|(&(offset, _), buf)| (offset, buf))
                        .collect();
                    self.io.write_pages_ref(&refs)?;
                }
            }
        }

        // Metadata can retire ahead of reader-pinned data. Separate watermarks
        // prevent early metadata erasure from skipping that data later.
        let zeroed_watermark = if self.secure_delete.load(Ordering::Relaxed) {
            let (zeroed_up_to, zeroed_chain_up_to) = {
                let state = self.state.lock();
                (state.zeroed_up_to, state.zeroed_chain_up_to)
            };
            let zeros = [0u8; PAGE_SIZE];
            let mut high = zeroed_up_to;
            let mut chain_high = zeroed_chain_up_to;
            for entry in &available {
                let is_chain = retired_chain_pages.get(&entry.page_id) == Some(&entry.freed_at_txn);
                let watermark = if is_chain {
                    zeroed_chain_up_to
                } else {
                    zeroed_up_to
                };
                if entry.freed_at_txn > watermark {
                    self.io.write_page(page_offset(entry.page_id), &zeros)?;
                    if is_chain {
                        chain_high = chain_high.max(entry.freed_at_txn);
                    } else {
                        high = high.max(entry.freed_at_txn);
                    }
                }
            }
            Some((high, chain_high))
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
            merkle_scheme: old_slot.merkle_scheme,
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

        let generation = {
            let mut state = self.state.lock();
            state.active_slot = inactive_slot_idx;
            if new_slot.catalog_root != state.current_slot.catalog_root {
                state.resolved_catalog = Arc::default();
            }
            state.current_slot = Arc::new(new_slot);
            state.cached_god_byte = new_god_byte;
            state.cached_file_size = new_file_size;
            // Availability is re-derived from the durable chain every commit,
            // so an abort, no-op commit, or shutdown strands nothing.
            // The allocator pops from the end. Prefer entries near the chain
            // head so small commits can share the unchanged metadata tail.
            state.reclaimed_pages =
                Arc::new(available.iter().rev().map(|entry| entry.page_id).collect());
            state.retired_chain_pages = retired_chain_pages;
            if let Some((watermark, chain_watermark)) = zeroed_watermark {
                state.zeroed_up_to = watermark;
                state.zeroed_chain_up_to = chain_watermark;
            }
            state.recycled_pages = Some(std::mem::take(pages));
            self.commit_generation.fetch_add(1, Ordering::Release) + 1
        };
        self.write_active.store(false, Ordering::SeqCst);
        Ok(generation)
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

    /// Database-wide storage-scan telemetry since this manager opened. Monotonic
    /// across threads; use [`TxnManager::measure_scans`] for one operation.
    pub fn rows_scanned(&self) -> u64 {
        self.rows_scanned.load(Ordering::Relaxed)
    }

    /// Begin an operation-local scan measurement on the current thread.
    ///
    /// Measurements nest; other managers and other threads stay isolated.
    pub fn measure_scans(&self) -> ScanMeasurement {
        let counter = Arc::new(AtomicU64::new(0));
        ACTIVE_SCAN_MEASUREMENTS.with(|measurements| {
            measurements.borrow_mut().push(ActiveScanMeasurement {
                manager_id: self.id,
                counter: Arc::clone(&counter),
            });
        });
        ScanMeasurement {
            manager_id: self.id,
            counter,
            _not_send: PhantomData,
        }
    }

    pub(crate) fn active_scan_measurements(&self) -> Vec<Arc<AtomicU64>> {
        ACTIVE_SCAN_MEASUREMENTS.with(|measurements| {
            measurements
                .borrow()
                .iter()
                .filter(|entry| entry.manager_id == self.id)
                .map(|entry| Arc::clone(&entry.counter))
                .collect()
        })
    }

    pub(crate) fn add_rows_scanned_to(&self, rows: u64, measurements: &[Arc<AtomicU64>]) {
        if rows == 0 {
            return;
        }
        self.rows_scanned.fetch_add(rows, Ordering::Relaxed);
        for measurement in measurements {
            measurement.fetch_add(rows, Ordering::Relaxed);
        }
    }

    pub fn current_slot(&self) -> CommitSlot {
        self.state.lock().current_slot.as_ref().clone()
    }

    /// Refuse a hash-only named-table lookup when an opened catalog contains
    /// more than one full name for that 32-bit slot hash.
    pub(crate) fn reject_named_table_hash_collision(
        &self,
        requested: &[u8],
        cancel: Option<&CancelToken>,
    ) -> Result<()> {
        let collisions = self.named_table_hash_collisions(cancel)?;
        if collisions.is_empty() {
            return Ok(());
        }
        let hash = file_manager::table_name_hash(requested);
        let Some((first, second)) = collisions.get(&hash) else {
            return Ok(());
        };
        let existing = if first.as_slice() == requested {
            second
        } else {
            first
        };
        Err(Error::NamedTableHashCollision {
            requested: String::from_utf8_lossy(requested).into_owned(),
            existing: String::from_utf8_lossy(existing).into_owned(),
            hash,
        })
    }

    /// Lazily build the legacy-catalog collision index once. Initialization
    /// failures are returned and leave the cell unset so a later call can
    /// retry; after success, the fast path is a lock-free `OnceLock::get`.
    fn named_table_hash_collisions(
        &self,
        cancel: Option<&CancelToken>,
    ) -> Result<&NamedTableHashCollisions> {
        if let Some(token) = cancel {
            token.check()?;
        }
        if let Some(collisions) = self.named_table_hash_collisions.get() {
            return Ok(collisions);
        }
        let _init = self.named_table_hash_collision_init.lock();
        if let Some(token) = cancel {
            token.check()?;
        }
        if self.named_table_hash_collisions.get().is_none() {
            let collisions = self.scan_named_table_hash_collisions(cancel)?;
            let _ = self.named_table_hash_collisions.set(collisions);
        }
        Ok(self
            .named_table_hash_collisions
            .get()
            .expect("named-table collision index initialized while holding its lock"))
    }

    fn scan_named_table_hash_collisions(
        &self,
        cancel: Option<&CancelToken>,
    ) -> Result<NamedTableHashCollisions> {
        use citadel_core::types::{PageType, ValueType};
        use citadel_page::{branch_node, leaf_node};

        let root = self.current_slot().catalog_root;
        if !root.is_valid() {
            return Ok(FxHashMap::default());
        }

        let mut first_by_hash: FxHashMap<u32, Vec<u8>> = FxHashMap::default();
        let mut collisions = FxHashMap::default();
        let mut visited = FxHashSet::default();
        let mut stack = vec![root];
        while let Some(page_id) = stack.pop() {
            if let Some(token) = cancel {
                token.check()?;
            }
            if !visited.insert(page_id) {
                return Err(Error::DatabaseCorrupted);
            }
            let page = self.read_page_from_disk(page_id)?;
            match page.page_type() {
                Some(PageType::Leaf) => {
                    for index in 0..page.num_cells() {
                        if let Some(token) = cancel {
                            token.check()?;
                        }
                        let cell = leaf_node::read_cell(&page, index);
                        if cell.val_type == ValueType::Tombstone {
                            continue;
                        }
                        record_named_table_hash(cell.key, &mut first_by_hash, &mut collisions);
                    }
                }
                Some(PageType::Branch) => {
                    for index in 0..page.num_cells() as usize {
                        stack.push(branch_node::get_child(&page, index));
                    }
                    let right = page.right_child();
                    if right.is_valid() {
                        stack.push(right);
                    }
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), page_id)),
            }
        }
        Ok(collisions)
    }

    /// Exclude writers and read both raw commit slots from one header image.
    ///
    /// The guard holds single-writer exclusion until dropped, so slot selection
    /// and every page reached from those slots form one coherent snapshot.
    pub(crate) fn integrity_snapshot(&self) -> Result<IntegritySnapshot<'_>> {
        let exclusion = self.exclude_writers()?;
        // Move the guard into the snapshot before doing I/O so every error
        // path releases the writer exclusion.
        let mut snapshot = IntegritySnapshot {
            exclusion,
            active_slot: 0,
            v1_required: false,
            slots: std::array::from_fn(|_| CommitSlot::default()),
        };
        let mut header_buf = [0u8; citadel_core::FILE_HEADER_SIZE];
        self.io.read_at(0, &mut header_buf)?;
        let header = file_manager::FileHeader::deserialize(&header_buf)?;
        snapshot.active_slot = header.active_slot();
        snapshot.v1_required = self.slots_flagged.load(Ordering::Acquire)
            || header.flags & citadel_core::HEADER_FLAG_SLOTS_V1 != 0;
        snapshot.slots = header.slots;
        Ok(snapshot)
    }

    pub fn reader_count(&self) -> usize {
        // Refcount sum, not key count: readers sharing a snapshot share a key.
        self.state.lock().reader_table.values().sum()
    }

    pub fn list_tables(&self) -> Result<Vec<(Vec<u8>, TableDescriptor)>> {
        use citadel_core::types::ValueType;
        use citadel_page::{branch_node, leaf_node};

        let _collision_init = if self.named_table_hash_collisions.get().is_none() {
            Some(self.named_table_hash_collision_init.lock())
        } else {
            None
        };
        let populate_collisions =
            _collision_init.is_some() && self.named_table_hash_collisions.get().is_none();
        let mut first_by_hash = FxHashMap::default();
        let mut collisions = FxHashMap::default();
        let mut collision_scan_complete = true;
        let slot = self.current_slot();
        if !slot.catalog_root.is_valid() {
            if populate_collisions {
                let _ = self.named_table_hash_collisions.set(collisions);
            }
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
                        if cell.val_type == ValueType::Tombstone {
                            continue;
                        }
                        if populate_collisions {
                            record_named_table_hash(cell.key, &mut first_by_hash, &mut collisions);
                        }
                        let desc = decode_catalog_descriptor(cell.val_type, cell.value)?;
                        tables.push((cell.key.to_vec(), desc));
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
                _ => collision_scan_complete = false,
            }
        }
        if populate_collisions && collision_scan_complete {
            let _ = self.named_table_hash_collisions.set(collisions);
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
                        if cell.key == name {
                            if cell.val_type == ValueType::Tombstone {
                                return Ok(None);
                            }
                            let desc = decode_catalog_descriptor(cell.val_type, cell.value)?;
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

    /// Run the integrity walk with cooperative cancellation. The writer
    /// exclusion is held by an RAII snapshot, so an interrupted walk releases
    /// it on the same path as every other early return.
    pub fn integrity_check_with_cancel(
        &self,
        cancel: Option<&CancelToken>,
    ) -> Result<IntegrityReport> {
        integrity::run_integrity_check_with_cancel(self, cancel)
    }

    /// Materialize and authenticate one overflow reference.
    ///
    /// Callers that obtained `reference` from a raw page must keep a stable
    /// transaction snapshot or writer exclusion across both operations.
    pub(crate) fn read_overflow_value(
        &self,
        reference: &citadel_page::leaf_node::OverflowRef,
        high_water_mark: u32,
        merkle_scheme: MerkleScheme,
        cancel: Option<&CancelToken>,
        budget: Option<&crate::ReadBudget>,
    ) -> Result<Vec<u8>> {
        if let Some(token) = cancel {
            token.check()?;
        }
        let total_len = reference.total_len as usize;
        if total_len > citadel_core::MAX_VALUE_SIZE {
            return Err(Error::CorruptOverflowChain(format!(
                "declared length {total_len} exceeds maximum {}",
                citadel_core::MAX_VALUE_SIZE
            )));
        }
        if let Some(budget) = budget {
            budget.try_charge(total_len)?;
        }
        let mut value = Vec::with_capacity(total_len);
        let require_digest = match merkle_scheme {
            MerkleScheme::Legacy => false,
            MerkleScheme::LogicalOverflowV1 => true,
            MerkleScheme::Unknown => return Err(Error::DatabaseCorrupted),
        };
        match cancel {
            Some(token) => {
                self.walk_overflow_chain_checked(
                    reference.first_page,
                    reference.total_len,
                    high_water_mark,
                    require_digest,
                    || token.check(),
                    |_, chunk| {
                        value.extend_from_slice(chunk);
                        Ok(())
                    },
                )?;
            }
            None => {
                self.walk_overflow_chain_checked(
                    reference.first_page,
                    reference.total_len,
                    high_water_mark,
                    require_digest,
                    || Ok(()),
                    |_, chunk| {
                        value.extend_from_slice(chunk);
                        Ok(())
                    },
                )?;
            }
        }
        Ok(value)
    }

    pub fn backup_to(&self, dest_io: &dyn PageIO) -> Result<()> {
        use std::collections::HashSet;

        let _writer_exclusion = self.exclude_writers()?;
        let slot = self.current_slot();

        let mut reachable = HashSet::new();
        self.collect_tree_pages(slot.tree_root, &mut reachable)?;

        if slot.catalog_root.is_valid() {
            let table_roots = self.collect_catalog_pages(slot.catalog_root, &mut reachable)?;
            for (_, root) in table_roots {
                self.collect_tree_pages(root, &mut reachable)?;
            }
        }

        // After a SyncMode::Off catalog skip, a slot entry is the sole record
        // of a table's CURRENT root; the (stale) catalog descriptor alone
        // would omit the live subtree from the backup.
        for &(_, _, root, depth) in &slot.named_table_entries {
            if root != 0 || depth != 0 {
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
        use std::collections::HashSet;

        let _writer_exclusion = self.exclude_writers()?;
        let slot = self.current_slot();
        let mut next_id: u32 = 0;
        let mut old_to_new: FxHashMap<PageId, PageId> = FxHashMap::default();
        let mut catalog_leaves: HashSet<PageId> = HashSet::new();
        let mut table_roots = Vec::new();
        let mut catalog_table_hashes = FxHashSet::default();

        // After an Off catalog skip the slot entry, not the stale descriptor,
        // holds the current root/count/depth; rewrite the compacted catalog
        // from it or the skip commits' rows vanish from the copy.
        let mut slot_overrides: FxHashMap<u32, (PageId, u64, u16)> = FxHashMap::default();
        for &(hash, count, root, depth) in &slot.named_table_entries {
            if root == 0 && depth == 0 {
                continue;
            }
            if slot_overrides
                .insert(hash, (PageId(root), count & !SLOT_ENTRY_STALE, depth))
                .is_some()
            {
                return Err(Error::DatabaseCorrupted);
            }
        }

        self.assign_new_ids(slot.tree_root, &mut old_to_new, &mut next_id)?;

        if slot.catalog_root.is_valid() {
            let catalog_tables = {
                let mut reachable = HashSet::new();
                self.collect_catalog_pages(slot.catalog_root, &mut reachable)?
            };
            catalog_table_hashes.extend(catalog_tables.iter().map(|&(hash, _)| hash));
            table_roots = catalog_tables
                .into_iter()
                .filter_map(|(hash, root)| (!slot_overrides.contains_key(&hash)).then_some(root))
                .collect();

            self.assign_new_ids(slot.catalog_root, &mut old_to_new, &mut next_id)?;

            self.collect_catalog_leaf_pages(slot.catalog_root, &mut catalog_leaves)?;

            for &root in &table_roots {
                self.assign_new_ids(root, &mut old_to_new, &mut next_id)?;
            }
        }
        for &(root, ..) in slot_overrides.values() {
            self.assign_new_ids(root, &mut old_to_new, &mut next_id)?;
        }

        // Only the hash memo is retained; decrypted pages are read on demand
        // and discarded, so a large tree does not have to fit in memory.
        let mut compacted_hashes = FxHashMap::default();
        let mut overflow_digests = FxHashMap::default();
        let mut hashing = FxHashSet::default();
        let mut logical_roots = vec![slot.tree_root];
        if slot.catalog_root.is_valid() {
            logical_roots.push(slot.catalog_root);
        }
        logical_roots.extend(table_roots.iter().copied());
        logical_roots.extend(slot_overrides.values().map(|&(root, ..)| root));
        for root in logical_roots {
            self.compute_compacted_tree_merkle(
                root,
                &old_to_new,
                &catalog_leaves,
                &slot_overrides,
                &mut compacted_hashes,
                &mut overflow_digests,
                &mut hashing,
                slot.high_water_mark,
            )?;
        }
        let root_merkle = compacted_hashes
            .get(&slot.tree_root)
            .copied()
            .ok_or(Error::DatabaseCorrupted)?;

        let total_pages = next_id;
        let needed_size =
            citadel_core::FILE_HEADER_SIZE as u64 + total_pages as u64 * PAGE_SIZE as u64;
        dest_io.truncate(needed_size)?;

        for (&old_id, &new_id) in &old_to_new {
            let page = self.read_reachable_page(old_id, slot.high_water_mark)?;
            let mut page = self.rewrite_compacted_page(
                old_id,
                page,
                &old_to_new,
                &catalog_leaves,
                &slot_overrides,
            )?;
            debug_assert_eq!(page.page_id(), new_id);
            if let Some(hash) = compacted_hashes.get(&old_id) {
                page.set_merkle_hash(hash);
            } else if let Some(digest) = overflow_digests.get(&old_id) {
                page.set_merkle_hash(digest);
            }

            page.update_checksum();

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

        let named_table_entries = slot
            .named_table_entries
            .iter()
            .map(|&(hash, count, root, depth)| {
                if catalog_table_hashes.contains(&hash) || (root == 0 && depth == 0) {
                    return Ok((hash, count & !SLOT_ENTRY_STALE, 0, 0));
                }

                // No catalog descriptor was available to receive this root.
                // Keep its remapped slot entry as the sole durable locator;
                // marking it stale prevents a later cache-capacity trim from
                // dropping it before a repair can restore the full name.
                let remapped = old_to_new
                    .get(&PageId(root))
                    .copied()
                    .ok_or(Error::PageOutOfBounds(PageId(root)))?;
                Ok((
                    hash,
                    (count & !SLOT_ENTRY_STALE) | SLOT_ENTRY_STALE,
                    remapped.as_u32(),
                    depth,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

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
            merkle_scheme: citadel_io::file_manager::MerkleScheme::LogicalOverflowV1,
            // Catalog-backed entries become ordinary rebuildable caches.
            // Slot-only roots retain their remapped locator above.
            named_table_entries,
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
        use citadel_core::types::{PageType, ValueType};

        let high_water_mark = self.current_slot().high_water_mark;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![root];
        while let Some(page_id) = stack.pop() {
            if !seen.insert(page_id) {
                return Err(Error::DatabaseCorrupted);
            }
            if reachable.contains(&page_id) {
                continue;
            }
            let page = self.read_reachable_page(page_id, high_water_mark)?;
            reachable.insert(page_id);
            match page.page_type() {
                Some(PageType::Leaf) => {
                    for cell in checked_leaf_cell_locations(&page)? {
                        if cell.value_type != ValueType::Overflow {
                            continue;
                        }
                        let (first_page, total_len) = checked_overflow_reference(&page, cell)?;
                        self.walk_overflow_chain(
                            first_page,
                            total_len,
                            high_water_mark,
                            |overflow_id, _| {
                                if !seen.insert(overflow_id) {
                                    return Err(Error::CorruptOverflowChain(format!(
                                        "overflow page {overflow_id} is referenced more than once"
                                    )));
                                }
                                reachable.insert(overflow_id);
                                Ok(())
                            },
                        )?;
                    }
                }
                Some(PageType::Branch) => {
                    for cell in checked_branch_cell_locations(&page)? {
                        stack.push(cell.child);
                    }
                    let right = page.right_child();
                    if !right.is_valid() {
                        return Err(Error::DatabaseCorrupted);
                    }
                    stack.push(right);
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), page_id)),
            }
        }
        Ok(())
    }

    fn collect_catalog_pages(
        &self,
        catalog_root: PageId,
        reachable: &mut std::collections::HashSet<PageId>,
    ) -> Result<Vec<(u32, PageId)>> {
        use citadel_core::types::{PageType, ValueType};

        let high_water_mark = self.current_slot().high_water_mark;
        let mut table_roots = Vec::new();
        let mut first_name_by_hash: FxHashMap<u32, Vec<u8>> = FxHashMap::default();
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![catalog_root];
        while let Some(page_id) = stack.pop() {
            if !seen.insert(page_id) || !reachable.insert(page_id) {
                return Err(Error::DatabaseCorrupted);
            }
            let page = self.read_reachable_page(page_id, high_water_mark)?;
            match page.page_type() {
                Some(PageType::Leaf) => {
                    for cell in checked_leaf_cell_locations(&page)? {
                        if cell.value_type == ValueType::Tombstone {
                            continue;
                        }
                        let desc = decode_catalog_descriptor(cell.value_type, cell.value(&page))?;
                        if desc.root_page.is_valid() {
                            let name = cell.key(&page);
                            let hash = file_manager::table_name_hash(name);
                            if let Some(first) = first_name_by_hash.get(&hash) {
                                if first.as_slice() != name {
                                    return Err(Error::NamedTableHashCollision {
                                        requested: String::from_utf8_lossy(name).into_owned(),
                                        existing: String::from_utf8_lossy(first).into_owned(),
                                        hash,
                                    });
                                }
                            } else {
                                first_name_by_hash.insert(hash, name.to_vec());
                            }
                            table_roots.push((hash, desc.root_page));
                        } else {
                            return Err(Error::DatabaseCorrupted);
                        }
                    }
                }
                Some(PageType::Branch) => {
                    for cell in checked_branch_cell_locations(&page)? {
                        stack.push(cell.child);
                    }
                    let right = page.right_child();
                    if !right.is_valid() {
                        return Err(Error::DatabaseCorrupted);
                    }
                    stack.push(right);
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), page_id)),
            }
        }
        Ok(table_roots)
    }

    fn collect_chain_pages(
        &self,
        root: PageId,
        reachable: &mut std::collections::HashSet<PageId>,
    ) -> Result<()> {
        use citadel_core::types::PageType;

        let high_water_mark = self.current_slot().high_water_mark;
        let mut seen = std::collections::HashSet::new();
        let mut current = root;
        while current.is_valid() {
            if !seen.insert(current) || !reachable.insert(current) {
                return Err(Error::DatabaseCorrupted);
            }
            let page = self.read_reachable_page(current, high_water_mark)?;
            if page.page_type() != Some(PageType::PendingFree) {
                return Err(Error::InvalidPageType(page.page_type_raw(), current));
            }
            let count = u32::from_le_bytes(
                page.data[citadel_core::PAGE_HEADER_SIZE..citadel_core::PAGE_HEADER_SIZE + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            if count > citadel_core::PENDING_FREE_ENTRIES_PER_PAGE {
                return Err(Error::DatabaseCorrupted);
            }
            current = page.right_child();
        }
        Ok(())
    }

    fn collect_catalog_leaf_pages(
        &self,
        catalog_root: PageId,
        leaves: &mut std::collections::HashSet<PageId>,
    ) -> Result<()> {
        use citadel_core::types::PageType;

        let high_water_mark = self.current_slot().high_water_mark;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![catalog_root];
        while let Some(page_id) = stack.pop() {
            if !seen.insert(page_id) {
                return Err(Error::DatabaseCorrupted);
            }
            let page = self.read_reachable_page(page_id, high_water_mark)?;
            match page.page_type() {
                Some(PageType::Leaf) => {
                    checked_leaf_cell_locations(&page)?;
                    leaves.insert(page_id);
                }
                Some(PageType::Branch) => {
                    for cell in checked_branch_cell_locations(&page)? {
                        stack.push(cell.child);
                    }
                    let right = page.right_child();
                    if !right.is_valid() {
                        return Err(Error::DatabaseCorrupted);
                    }
                    stack.push(right);
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), page_id)),
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
        use citadel_core::types::{PageType, ValueType};

        if mapping.contains_key(&root) {
            return Ok(());
        }
        let high_water_mark = self.current_slot().high_water_mark;
        let mut seen = FxHashSet::default();
        let mut stack = vec![root];
        while let Some(page_id) = stack.pop() {
            if !seen.insert(page_id) {
                return Err(Error::DatabaseCorrupted);
            }
            if mapping.contains_key(&page_id) {
                continue;
            }
            let page = self.read_reachable_page(page_id, high_water_mark)?;
            mapping.insert(page_id, PageId(*next_id));
            *next_id = next_id.checked_add(1).ok_or(Error::DatabaseCorrupted)?;

            match page.page_type() {
                Some(PageType::Leaf) => {
                    for cell in checked_leaf_cell_locations(&page)? {
                        if cell.value_type != ValueType::Overflow {
                            continue;
                        }
                        let (first_page, total_len) = checked_overflow_reference(&page, cell)?;
                        self.walk_overflow_chain(
                            first_page,
                            total_len,
                            high_water_mark,
                            |overflow_id, _| {
                                if !seen.insert(overflow_id) {
                                    return Err(Error::CorruptOverflowChain(format!(
                                        "overflow page {overflow_id} is referenced more than once"
                                    )));
                                }
                                if let std::collections::hash_map::Entry::Vacant(entry) =
                                    mapping.entry(overflow_id)
                                {
                                    entry.insert(PageId(*next_id));
                                    *next_id =
                                        next_id.checked_add(1).ok_or(Error::DatabaseCorrupted)?;
                                }
                                Ok(())
                            },
                        )?;
                    }
                }
                Some(PageType::Branch) => {
                    for cell in checked_branch_cell_locations(&page)? {
                        stack.push(cell.child);
                    }
                    let right = page.right_child();
                    if !right.is_valid() {
                        return Err(Error::DatabaseCorrupted);
                    }
                    stack.push(right);
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), page_id)),
            }
        }
        Ok(())
    }

    pub(crate) fn read_reachable_page(
        &self,
        page_id: PageId,
        high_water_mark: u32,
    ) -> Result<Page> {
        if page_id.as_u32() >= high_water_mark {
            return Err(Error::PageOutOfBounds(page_id));
        }
        let page = self.read_page_from_disk(page_id)?;
        if page.page_id() != page_id {
            return Err(Error::DatabaseCorrupted);
        }
        Ok(page)
    }

    fn walk_overflow_chain<F>(
        &self,
        first_page: PageId,
        total_len: u32,
        high_water_mark: u32,
        visit: F,
    ) -> Result<[u8; citadel_core::MERKLE_HASH_SIZE]>
    where
        F: FnMut(PageId, &[u8]) -> Result<()>,
    {
        self.walk_overflow_chain_checked(
            first_page,
            total_len,
            high_water_mark,
            false,
            || Ok(()),
            visit,
        )
    }

    fn walk_overflow_chain_checked<C, F>(
        &self,
        first_page: PageId,
        total_len: u32,
        high_water_mark: u32,
        require_digest: bool,
        mut check: C,
        mut visit: F,
    ) -> Result<[u8; citadel_core::MERKLE_HASH_SIZE]>
    where
        C: FnMut() -> Result<()>,
        F: FnMut(PageId, &[u8]) -> Result<()>,
    {
        use citadel_core::types::PageType;
        use citadel_page::overflow;

        if first_page.as_u32() == 0 || total_len as usize > citadel_core::MAX_VALUE_SIZE {
            return Err(Error::CorruptOverflowChain(
                "overflow reference has an invalid first page or length".to_owned(),
            ));
        }

        let expected_max = overflow::pages_needed(total_len as usize);
        let mut current = first_page;
        let mut seen = FxHashSet::default();
        let mut actual_len = 0u64;
        let mut page_count = 0usize;
        let mut stored_digest = None;
        let mut payload_digest = crate::merkle::OverflowPayloadDigest::new(total_len);
        check()?;
        while current.as_u32() != 0 {
            check()?;
            if !seen.insert(current) {
                return Err(Error::CorruptOverflowChain(format!(
                    "overflow chain beginning at {first_page} contains a cycle"
                )));
            }
            let page = self.read_reachable_page(current, high_water_mark)?;
            if page.page_type() != Some(PageType::Overflow) {
                return Err(Error::InvalidPageType(page.page_type_raw(), current));
            }
            let data_len = overflow::data_len(&page) as usize;
            if data_len > overflow::OVERFLOW_DATA_CAPACITY {
                return Err(Error::CorruptOverflowChain(format!(
                    "overflow page {current} declares {data_len} bytes"
                )));
            }
            let data = overflow::read_data(&page);
            page_count += 1;
            if page_count > expected_max {
                return Err(Error::CorruptOverflowChain(format!(
                    "overflow chain beginning at {first_page} has too many pages"
                )));
            }
            actual_len = actual_len
                .checked_add(data_len as u64)
                .ok_or_else(|| Error::CorruptOverflowChain("overflow length overflow".into()))?;
            if stored_digest.is_none() {
                stored_digest = Some(page.merkle_hash());
            }
            payload_digest.update(data);
            visit(current, data)?;
            current = overflow::next_page(&page);
        }
        check()?;
        if actual_len != u64::from(total_len) {
            return Err(Error::CorruptOverflowChain(format!(
                "overflow chain beginning at {first_page} stores {actual_len} bytes, expected {total_len}"
            )));
        }
        let actual_digest = payload_digest.finalize();
        let stored_digest = stored_digest.ok_or_else(|| {
            Error::CorruptOverflowChain(format!(
                "overflow chain beginning at {first_page} has no head page"
            ))
        })?;
        if require_digest && stored_digest == [0u8; citadel_core::MERKLE_HASH_SIZE] {
            return Err(Error::CorruptOverflowChain(format!(
                "overflow head {first_page} is missing its logical payload digest"
            )));
        }
        if stored_digest != [0u8; citadel_core::MERKLE_HASH_SIZE] && stored_digest != actual_digest
        {
            return Err(Error::CorruptOverflowChain(format!(
                "overflow head {first_page} payload digest does not match its contents"
            )));
        }
        Ok(actual_digest)
    }

    fn rewrite_compacted_page(
        &self,
        old_id: PageId,
        mut page: Page,
        mapping: &FxHashMap<PageId, PageId>,
        catalog_leaves: &std::collections::HashSet<PageId>,
        slot_overrides: &FxHashMap<u32, (PageId, u64, u16)>,
    ) -> Result<Page> {
        use citadel_core::types::{PageType, ValueType};

        let new_id = mapping
            .get(&old_id)
            .copied()
            .ok_or(Error::PageOutOfBounds(old_id))?;
        page.set_page_id(new_id);

        match page.page_type() {
            Some(PageType::Branch) => {
                for cell in checked_branch_cell_locations(&page)? {
                    let new_child = mapping
                        .get(&cell.child)
                        .copied()
                        .ok_or(Error::PageOutOfBounds(cell.child))?;
                    page.data[cell.child_offset..cell.child_offset + 4]
                        .copy_from_slice(&new_child.as_u32().to_le_bytes());
                }
                let old_right = page.right_child();
                if !old_right.is_valid() {
                    return Err(Error::DatabaseCorrupted);
                }
                let new_right = mapping
                    .get(&old_right)
                    .copied()
                    .ok_or(Error::PageOutOfBounds(old_right))?;
                page.set_right_child(new_right);
            }
            Some(PageType::Leaf) => {
                let cells = checked_leaf_cell_locations(&page)?;
                for cell in &cells {
                    if cell.value_type != ValueType::Overflow {
                        continue;
                    }
                    let (old_first, _) = checked_overflow_reference(&page, *cell)?;
                    let new_first = mapping
                        .get(&old_first)
                        .copied()
                        .ok_or(Error::PageOutOfBounds(old_first))?;
                    page.data[cell.value_start..cell.value_start + 4]
                        .copy_from_slice(&new_first.as_u32().to_le_bytes());
                }

                if catalog_leaves.contains(&old_id) {
                    for cell in cells {
                        if cell.value_type == ValueType::Tombstone {
                            continue;
                        }
                        let descriptor =
                            decode_catalog_descriptor(cell.value_type, cell.value(&page))?;
                        let table_hash = file_manager::table_name_hash(cell.key(&page));
                        let (source_root, override_meta) = match slot_overrides.get(&table_hash) {
                            Some(&(root, count, depth)) => (root, Some((count, depth))),
                            None => (descriptor.root_page, None),
                        };
                        if !source_root.is_valid() {
                            return Err(Error::DatabaseCorrupted);
                        }
                        let new_root = mapping
                            .get(&source_root)
                            .copied()
                            .ok_or(Error::PageOutOfBounds(source_root))?;
                        page.data[cell.value_start..cell.value_start + 4]
                            .copy_from_slice(&new_root.as_u32().to_le_bytes());
                        if let Some((count, depth)) = override_meta {
                            page.data[cell.value_start + 4..cell.value_start + 12]
                                .copy_from_slice(&count.to_le_bytes());
                            page.data[cell.value_start + 12..cell.value_start + 14]
                                .copy_from_slice(&depth.to_le_bytes());
                        }
                    }
                }
            }
            Some(PageType::Overflow) => {
                let old_next = page.right_child();
                if old_next.as_u32() != 0 {
                    let new_next = mapping
                        .get(&old_next)
                        .copied()
                        .ok_or(Error::PageOutOfBounds(old_next))?;
                    page.set_right_child(new_next);
                }
            }
            _ => return Err(Error::InvalidPageType(page.page_type_raw(), old_id)),
        }
        Ok(page)
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_compacted_tree_merkle(
        &self,
        page_id: PageId,
        mapping: &FxHashMap<PageId, PageId>,
        catalog_leaves: &std::collections::HashSet<PageId>,
        slot_overrides: &FxHashMap<u32, (PageId, u64, u16)>,
        hashes: &mut FxHashMap<PageId, [u8; citadel_core::MERKLE_HASH_SIZE]>,
        overflow_digests: &mut FxHashMap<PageId, [u8; citadel_core::MERKLE_HASH_SIZE]>,
        hashing: &mut FxHashSet<PageId>,
        high_water_mark: u32,
    ) -> Result<[u8; citadel_core::MERKLE_HASH_SIZE]> {
        use citadel_core::types::PageType;

        if let Some(hash) = hashes.get(&page_id) {
            return Ok(*hash);
        }
        if !hashing.insert(page_id) {
            return Err(Error::DatabaseCorrupted);
        }

        let page = self.read_reachable_page(page_id, high_water_mark)?;
        let hash = match page.page_type() {
            Some(PageType::Leaf) => {
                let page = if catalog_leaves.contains(&page_id) {
                    self.rewrite_compacted_page(
                        page_id,
                        page,
                        mapping,
                        catalog_leaves,
                        slot_overrides,
                    )?
                } else {
                    page
                };
                self.hash_leaf_page(&page, high_water_mark, overflow_digests)?
            }
            Some(PageType::Branch) => {
                let cells = checked_branch_cell_locations(&page)?;
                let right = page.right_child();
                if !right.is_valid() {
                    return Err(Error::DatabaseCorrupted);
                }
                let mut hasher = blake3::Hasher::new();
                for child in cells
                    .into_iter()
                    .map(|cell| cell.child)
                    .chain(std::iter::once(right))
                {
                    let child_hash = self.compute_compacted_tree_merkle(
                        child,
                        mapping,
                        catalog_leaves,
                        slot_overrides,
                        hashes,
                        overflow_digests,
                        hashing,
                        high_water_mark,
                    )?;
                    hasher.update(&child_hash);
                }
                let full = hasher.finalize();
                let mut hash = [0u8; citadel_core::MERKLE_HASH_SIZE];
                hash.copy_from_slice(&full.as_bytes()[..citadel_core::MERKLE_HASH_SIZE]);
                hash
            }
            _ => return Err(Error::InvalidPageType(page.page_type_raw(), page_id)),
        };
        hashing.remove(&page_id);
        hashes.insert(page_id, hash);
        Ok(hash)
    }

    fn hash_leaf_page(
        &self,
        page: &Page,
        high_water_mark: u32,
        overflow_digests: &mut FxHashMap<PageId, [u8; citadel_core::MERKLE_HASH_SIZE]>,
    ) -> Result<[u8; citadel_core::MERKLE_HASH_SIZE]> {
        let cells = checked_leaf_cell_locations(page)?;
        crate::merkle::hash_logical_leaf_cells(
            cells
                .iter()
                .map(|cell| (cell.key(page), cell.value_type, cell.value(page))),
            |reference| {
                let digest = self.walk_overflow_chain(
                    reference.first_page,
                    reference.total_len,
                    high_water_mark,
                    |_, _| Ok(()),
                )?;
                overflow_digests.insert(reference.first_page, digest);
                Ok(digest)
            },
        )
    }

    fn load_pending_free_chain(
        &self,
        pages: &FxHashMap<PageId, Page>,
        root: PageId,
        high_water_mark: u32,
        slot_txn: TxnId,
        capacity_hint: usize,
    ) -> Result<pending_free::ChainSnapshot> {
        // Local reclaim invariants only. Proving an entry is absent from every
        // live tree needs an O(database) walk per commit, so that stays behind
        // the explicit integrity_check boundary.
        pending_free::ChainSnapshot::read_committed(
            root,
            high_water_mark,
            slot_txn,
            capacity_hint,
            |page_id| {
                if page_id.as_u32() >= high_water_mark {
                    return Err(Error::PageOutOfBounds(page_id));
                }
                let page = match pages.get(&page_id) {
                    Some(page) => PendingFreePage::Borrowed(page),
                    None => PendingFreePage::Cached(self.fetch_page(page_id)?),
                };
                if page.txn_id() > slot_txn {
                    return Err(Error::DatabaseCorrupted);
                }
                Ok(page)
            },
        )
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
