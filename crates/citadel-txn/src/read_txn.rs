//! Read transaction: MVCC snapshot isolation. RAII reader registration.

use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{CancelToken, Error, Result};
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
    high_water_mark: u32,
}

impl PageMap for ReadPages<'_> {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.cache.get(id).map(|a| a.as_ref())
    }
}

impl PageLoader for ReadPages<'_> {
    fn ensure_loaded(&mut self, id: PageId) -> Result<()> {
        if !self.cache.contains_key(&id) {
            let arc = self
                .manager
                .fetch_reachable_page(id, self.high_water_mark)?;
            self.cache.insert(id, arc);
        }
        Ok(())
    }
}

/// Counts rows a scan saw and adds them to the manager when the scan ends.
///
/// A guard rather than a line before each `return`: a scan ends exhausted,
/// stopped by its callback, cancelled, or failed on a torn page, and all of
/// them read rows worth reporting. The count accumulates in a plain field and
/// flushes once, so the hot loop pays no atomic cost.
pub(crate) struct ScanCount<'m> {
    manager: &'m TxnManager,
    measurements: Vec<Arc<AtomicU64>>,
    pub rows: u64,
}

impl<'m> ScanCount<'m> {
    pub(crate) fn new(manager: &'m TxnManager) -> Self {
        Self::with_measurements(manager, manager.active_scan_measurements())
    }

    pub(crate) fn with_measurements(
        manager: &'m TxnManager,
        measurements: Vec<Arc<AtomicU64>>,
    ) -> Self {
        Self {
            manager,
            measurements,
            rows: 0,
        }
    }
}

impl Drop for ScanCount<'_> {
    fn drop(&mut self) {
        self.manager
            .add_rows_scanned_to(self.rows, &self.measurements);
    }
}

/// Cell iteration over a leaf slice (materializing overflow through `view`).
/// Callback returns `false` to stop.
///
/// Checked once per leaf, not per cell: a leaf bounds the work between checks,
/// and an atomic load per cell would show up in the scan benchmarks.
fn scan_leaf_cells<F>(
    view: &mut ReadPages<'_>,
    leaves: &[Arc<Page>],
    cancel: Option<&CancelToken>,
    count: &mut ScanCount<'_>,
    mut f: F,
) -> Result<()>
where
    F: FnMut(&[u8], &[u8]) -> bool,
{
    for page in leaves {
        if let Some(c) = cancel {
            c.check()?;
        }
        let n = page.num_cells();
        for i in 0..n {
            count.rows += 1;
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
                    let materialized =
                        overflow_io::read_chain_value_with_cancel(view, &oref, cancel)?;
                    if !f(&key_owned, &materialized) {
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

/// Leaf-slice scanner detached from the transaction's page cache so shards
/// can run concurrently, borrow-tied to the registered read txn. Touches
/// exactly the pages the serial [`ReadTxn::scan_leaves`] would; overflow
/// chains are read through the manager into a shard-local cache.
pub struct LeafShardScanner<'t> {
    manager: &'t TxnManager,
    cache: FxHashMap<PageId, Arc<Page>>,
    measurements: Vec<Arc<AtomicU64>>,
    high_water_mark: u32,
    /// Inherited from the producing txn so a cancel reaches every shard; a
    /// per-shard flag would let the rest run on after one stopped.
    cancel: Option<CancelToken>,
}

impl LeafShardScanner<'_> {
    /// Iterate the cells of `leaves` (materializing overflow). Callback
    /// returns `false` to stop.
    pub fn scan_leaves<F>(&mut self, leaves: &[Arc<Page>], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        let Self {
            manager,
            cache,
            measurements,
            high_water_mark,
            cancel,
        } = self;
        if let Some(token) = cancel.as_ref() {
            token.check()?;
        }
        let mut count = ScanCount::with_measurements(manager, measurements.clone());
        let mut view = ReadPages {
            cache,
            manager,
            high_water_mark: *high_water_mark,
        };
        scan_leaf_cells(&mut view, leaves, cancel.as_ref(), &mut count, f)
    }
}

/// A table's leaf pages in left-to-right order; cacheable across reads at one
/// commit gen.
pub type LeafPages = Vec<Arc<Page>>;

/// Cache form of [`LeafPages`]: weak handles, so caching does not pin pages in
/// the pool.
pub type LeafPagesWeak = Vec<Weak<Page>>;

/// Downgrade live leaves to their cacheable weak form.
pub fn downgrade_leaves(leaves: &[Arc<Page>]) -> LeafPagesWeak {
    leaves.iter().map(Arc::downgrade).collect()
}

/// Upgrade cached weak leaves to live handles; `None` if any was evicted
/// (caller rebuilds).
pub fn upgrade_leaves(weak: &[Weak<Page>]) -> Option<LeafPages> {
    weak.iter().map(Weak::upgrade).collect()
}

/// Read-only transaction with snapshot isolation.
pub struct ReadTxn<'a> {
    manager: &'a TxnManager,
    txn_id: TxnId,
    snapshot: Arc<CommitSlot>,
    commit_generation: u64,
    page_cache: FxHashMap<PageId, Arc<Page>>,
    /// Exact catalog resolutions for this immutable snapshot. Commit-slot
    /// entries are keyed by a 32-bit hash, so a slot root is trusted only
    /// after this cache has proved the requested name itself is live.
    resolved_tables: FxHashMap<Vec<u8>, TableDescriptor>,
    /// Operation counters inherited before this transaction is handed to
    /// parallel workers. Weak handles expire when their measurement guard
    /// ends, so reusing the transaction cannot charge a completed span.
    scan_measurements: Vec<Weak<AtomicU64>>,
    /// A field rather than a scan parameter: threading it through would change
    /// every scan signature, and a new scan cannot forget to accept it.
    cancel: Option<CancelToken>,
}

impl<'db> ReadTxn<'db> {
    pub(crate) fn new(
        manager: &'db TxnManager,
        txn_id: TxnId,
        snapshot: Arc<CommitSlot>,
        commit_generation: u64,
    ) -> Self {
        Self {
            manager,
            txn_id,
            snapshot,
            commit_generation,
            page_cache: FxHashMap::default(),
            resolved_tables: FxHashMap::default(),
            scan_measurements: manager
                .active_scan_measurements()
                .iter()
                .map(Arc::downgrade)
                .collect(),
            cancel: None,
        }
    }

    pub fn set_cancel(&mut self, token: Option<CancelToken>) {
        self.cancel = token;
    }

    pub fn cancel_token(&self) -> Option<&CancelToken> {
        self.cancel.as_ref()
    }

    #[inline]
    fn check_cancel(&self) -> Result<()> {
        match &self.cancel {
            Some(token) => token.check(),
            None => Ok(()),
        }
    }

    /// Database-wide scan telemetry across all transactions and threads.
    /// Monotonic; use [`ReadTxn::measure_scans`] for an isolated operation.
    pub fn rows_scanned(&self) -> u64 {
        self.manager.rows_scanned()
    }

    /// Begin an operation-local scan measurement for this transaction. The weak
    /// copy propagates it through worker handoffs and pull-iterator lifetimes.
    pub fn measure_scans(&mut self) -> crate::manager::ScanMeasurement {
        self.scan_measurements
            .retain(|measurement| measurement.strong_count() > 0);
        let measurement = self.manager.measure_scans();
        self.scan_measurements.push(measurement.weak_counter());
        measurement
    }

    fn captured_scan_measurements(&self) -> Vec<Arc<AtomicU64>> {
        let mut measurements = self.manager.active_scan_measurements();
        for inherited in &self.scan_measurements {
            let Some(inherited) = inherited.upgrade() else {
                continue;
            };
            if !measurements
                .iter()
                .any(|active| Arc::ptr_eq(active, &inherited))
            {
                measurements.push(inherited);
            }
        }
        measurements
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

    /// Merkle root captured with this transaction's default-tree root.
    pub fn root_hash(&self) -> [u8; citadel_core::MERKLE_HASH_SIZE] {
        if self.has_logical_merkle_hashes() {
            self.snapshot.merkle_root
        } else {
            [0u8; citadel_core::MERKLE_HASH_SIZE]
        }
    }

    /// Whether every nonzero page hash in this snapshot uses the logical
    /// overflow-value scheme. Legacy snapshots must expose all hashes as
    /// unknown to sync because their hashes do not cover overflow payloads.
    pub fn has_logical_merkle_hashes(&self) -> bool {
        self.snapshot.merkle_scheme == citadel_io::file_manager::MerkleScheme::LogicalOverflowV1
    }

    pub fn entry_count(&self) -> u64 {
        self.snapshot.tree_entries
    }

    /// The table's catalog root in this transaction (a lookup, no scan).
    /// Use [`ReadTxn::table_root_stamp`] when allocator reuse must be detected.
    pub fn table_root_page(&self, table: &[u8]) -> Result<Option<PageId>> {
        self.check_cancel()?;
        let root = match self.lookup_table_uncached(table) {
            Ok(desc) => Some(desc.root_page),
            Err(Error::TableNotFound(_)) => None,
            Err(err) => return Err(err),
        };
        self.check_cancel()?;
        Ok(root)
    }

    /// The table root together with the transaction id stored in that root
    /// page. Unlike a bare [`PageId`], this stamp does not alias when the page
    /// allocator later recycles the same physical id for different contents.
    pub fn table_root_stamp(&mut self, table: &[u8]) -> Result<Option<(PageId, TxnId)>> {
        self.check_cancel()?;
        let root = match self.lookup_table(table) {
            Ok(desc) => desc.root_page,
            Err(Error::TableNotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        let root_txn = self.load_page(root)?.txn_id();
        self.check_cancel()?;
        Ok(Some((root, root_txn)))
    }

    /// List named tables exactly as they exist in this transaction's catalog
    /// snapshot, including commit-slot root overrides for that same snapshot.
    pub fn list_tables(&self) -> Result<Vec<(Vec<u8>, TableDescriptor)>> {
        self.check_cancel()?;
        let catalog_root = self.snapshot.catalog_root;
        if !catalog_root.is_valid() {
            return Ok(Vec::new());
        }

        let mut tables = Vec::new();
        let mut visited = FxHashSet::default();
        let mut names = FxHashSet::default();
        let mut names_by_hash = FxHashMap::<u32, Vec<u8>>::default();
        let snapshot = Arc::clone(&self.snapshot);
        let mut stack = vec![catalog_root];
        while let Some(page_id) = stack.pop() {
            self.check_cancel()?;
            if !visited.insert(page_id) {
                return Err(Error::DatabaseCorrupted);
            }
            let page = self.read_reachable_page(page_id)?;
            match page.page_type() {
                Some(PageType::Leaf) => {
                    let cells = leaf_node::read_cells_checked(&page)
                        .map_err(|_| Error::DatabaseCorrupted)?;
                    for cell in cells {
                        if cell.val_type == ValueType::Tombstone {
                            continue;
                        }
                        if cell.val_type != ValueType::Inline {
                            return Err(Error::DatabaseCorrupted);
                        }
                        let name = cell.key.to_vec();
                        if !names.insert(name.clone()) {
                            return Err(Error::DatabaseCorrupted);
                        }
                        let hash = citadel_io::file_manager::table_name_hash(&name);
                        if let Some(existing) = names_by_hash.insert(hash, name.clone()) {
                            if existing != name {
                                return Err(Error::NamedTableHashCollision {
                                    requested: String::from_utf8_lossy(&name).into_owned(),
                                    existing: String::from_utf8_lossy(&existing).into_owned(),
                                    hash,
                                });
                            }
                        }
                        let mut descriptor = TableDescriptor::try_deserialize(cell.value)
                            .ok_or(Error::DatabaseCorrupted)?;
                        if let Some((root, depth)) = snapshot.named_entry_root(&name) {
                            descriptor.root_page = root;
                            descriptor.depth = depth;
                            descriptor.entry_count = snapshot
                                .named_entry_count(&name)
                                .ok_or(Error::DatabaseCorrupted)?;
                        }
                        tables.push((name, descriptor));
                    }
                }
                Some(PageType::Branch) => {
                    let cells = branch_node::read_cells_checked(&page)
                        .map_err(|_| Error::DatabaseCorrupted)?;
                    for cell in cells {
                        stack.push(cell.child);
                    }
                    stack.push(page.right_child());
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), page_id)),
            }
        }

        self.check_cancel()?;
        Ok(tables)
    }

    /// Read a page reached from a root or branch in this transaction.
    ///
    /// This is a low-level snapshot bridge: `page_id` must come from the same
    /// transaction's tree walk. The method enforces the snapshot high-water
    /// bound and embedded page ID, but cannot prove that caller-side ancestry.
    pub fn read_reachable_page(&self, page_id: PageId) -> Result<Page> {
        self.check_cancel()?;
        let page = self
            .manager
            .read_reachable_page(page_id, self.snapshot.high_water_mark)?;
        self.check_cancel()?;
        Ok(page)
    }

    /// Materialize an overflow reference read from a reachable leaf in this
    /// transaction.
    pub fn read_reachable_overflow_value(&self, reference: &OverflowRef) -> Result<Vec<u8>> {
        self.manager.read_overflow_value(
            reference,
            self.snapshot.high_water_mark,
            self.snapshot.merkle_scheme,
            self.cancel.as_ref(),
        )
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_cancel()?;
        let value = self.search_tree(self.snapshot.tree_root, key)?;
        self.check_cancel()?;
        Ok(value)
    }

    pub fn contains_key(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    pub fn for_each<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        self.check_cancel()?;
        let root = self.snapshot.tree_root;
        self.preload_all_pages(root)?;
        let measurements = self.captured_scan_measurements();
        let mut count = ScanCount::with_measurements(self.manager, measurements);
        let mut cursor = Cursor::first(&self.page_cache, root)?;
        while cursor.is_valid() {
            if let Some(t) = self.cancel.as_ref() {
                t.check()?;
            }
            count.rows += 1;
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
        self.read_reachable_overflow_value(oref)
    }

    pub fn table_entry_count(&mut self, table: &[u8]) -> Result<u64> {
        self.check_cancel()?;
        let count = self.lookup_table(table)?.entry_count;
        self.check_cancel()?;
        Ok(count)
    }

    pub fn table_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_cancel()?;
        let desc = self.lookup_table(table)?;
        let value = self.search_tree(desc.root_page, key)?;
        self.check_cancel()?;
        Ok(value)
    }

    pub fn table_contains_key(&mut self, table: &[u8], key: &[u8]) -> Result<bool> {
        Ok(self.table_get(table, key)?.is_some())
    }

    pub fn table_for_each<F>(&mut self, table: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        self.check_cancel()?;
        let desc = self.lookup_table(table)?;
        self.preload_all_pages(desc.root_page)?;
        let measurements = self.captured_scan_measurements();
        let mut count = ScanCount::with_measurements(self.manager, measurements);
        let mut cursor = Cursor::first(&self.page_cache, desc.root_page)?;
        while cursor.is_valid() {
            if let Some(t) = self.cancel.as_ref() {
                t.check()?;
            }
            count.rows += 1;
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
        self.check_cancel()?;
        let desc = self.lookup_table(table)?;
        let root = desc.root_page;
        let cancel = self.cancel.clone();
        let measurements = self.captured_scan_measurements();
        let mut count = ScanCount::with_measurements(self.manager, measurements);
        let mut view = ReadPages {
            cache: &mut self.page_cache,
            manager: self.manager,
            high_water_mark: self.snapshot.high_water_mark,
        };
        let mut cursor = Cursor::seek_lazy(&mut view, root, start_key)?;
        while let Some(c) = cursor.current_ref_lazy(&mut view) {
            if let Some(t) = cancel.as_ref() {
                t.check()?;
            }
            count.rows += 1;
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
                    let materialized = overflow_io::read_chain_value_with_cancel(
                        &mut view,
                        &oref,
                        cancel.as_ref(),
                    )?;
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
        self.check_cancel()?;
        let desc = self.lookup_table(table)?;
        let root = desc.root_page;
        let cancel = self.cancel.clone();
        let measurements = self.captured_scan_measurements();
        let mut count = ScanCount::with_measurements(self.manager, measurements);
        let mut view = ReadPages {
            cache: &mut self.page_cache,
            manager: self.manager,
            high_water_mark: self.snapshot.high_water_mark,
        };
        let mut cursor = Cursor::seek_lazy(&mut view, root, start_key)?;
        if !cursor.is_valid() {
            return Ok(());
        }
        loop {
            if let Some(t) = cancel.as_ref() {
                t.check()?;
            }
            view.ensure_loaded(cursor.leaf_page_id())?;
            let leaf_page = view
                .cache
                .get(&cursor.leaf_page_id())
                .map(Arc::clone)
                .ok_or(Error::PageOutOfBounds(cursor.leaf_page_id()))?;
            let n = leaf_page.num_cells();
            let mut idx = cursor.cell_index();
            while idx < n {
                count.rows += 1;
                let cell = leaf_node::read_cell(&leaf_page, idx);
                let continue_scan = match cell.val_type {
                    ValueType::Tombstone => true,
                    ValueType::Inline => f(cell.key, cell.value)?,
                    ValueType::Overflow => {
                        let oref = OverflowRef::from_bytes(cell.value);
                        let key_owned = cell.key.to_vec();
                        let materialized = overflow_io::read_chain_value_with_cancel(
                            &mut view,
                            &oref,
                            cancel.as_ref(),
                        )?;
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
        self.check_cancel()?;
        let desc = self.lookup_table(table)?;
        let root = desc.root_page;
        let cursor = {
            let mut view = ReadPages {
                cache: &mut self.page_cache,
                manager: self.manager,
                high_water_mark: self.snapshot.high_water_mark,
            };
            Cursor::seek_lazy(&mut view, root, start_key)?
        };
        let measurements = self.captured_scan_measurements();
        let adapter = ReadTxnScanAdapter {
            txn: self,
            measurements,
        };
        Ok(crate::scan_iter::TableIter::new(adapter, cursor))
    }

    /// Consume self and return a lending iterator that owns the read txn.
    pub fn into_table_scan_iter(
        mut self,
        table: &[u8],
        start_key: &[u8],
    ) -> Result<crate::scan_iter::TableIter<OwnedReadTxnAdapter<'db>>> {
        self.check_cancel()?;
        let desc = self.lookup_table(table)?;
        let root = desc.root_page;
        let cursor = {
            let mut view = ReadPages {
                cache: &mut self.page_cache,
                manager: self.manager,
                high_water_mark: self.snapshot.high_water_mark,
            };
            Cursor::seek_lazy(&mut view, root, start_key)?
        };
        let measurements = self.captured_scan_measurements();
        let adapter = OwnedReadTxnAdapter {
            txn: self,
            measurements,
        };
        Ok(crate::scan_iter::TableIter::new(adapter, cursor))
    }

    /// Collect a table's leaf pages left-to-right (the DFS prelude of a full
    /// scan), so a caller can cache them and skip this walk on repeated scans
    /// at the same commit gen.
    pub fn collect_table_leaves(&mut self, table: &[u8]) -> Result<LeafPages> {
        let desc = self.lookup_table(table)?;
        let mut leaves = Vec::new();
        self.load_and_collect_leaves(desc.root_page, &mut leaves)?;
        Ok(leaves)
    }

    /// Iterate the cells of `leaves` (materializing overflow). Callback returns
    /// `false` to stop.
    pub fn scan_leaves<F>(&mut self, leaves: &[Arc<Page>], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        self.check_cancel()?;
        let cancel = self.cancel.clone();
        let measurements = self.captured_scan_measurements();
        let mut count = ScanCount::with_measurements(self.manager, measurements);
        let mut view = ReadPages {
            cache: &mut self.page_cache,
            manager: self.manager,
            high_water_mark: self.snapshot.high_water_mark,
        };
        scan_leaf_cells(&mut view, leaves, cancel.as_ref(), &mut count, f)
    }

    /// A scanner for parallel leaf iteration, borrow-tied to this txn so the
    /// snapshot registration outlives every shard using it.
    pub fn shard_scanner(&self) -> LeafShardScanner<'_> {
        // A scanner built on a Rayon worker has no caller thread-local stack:
        // counters inherited before handoff bridge that, thread-local ones cover
        // direct use, and both sources can name the same measurement.
        let measurements = self.captured_scan_measurements();
        LeafShardScanner {
            manager: self.manager,
            cache: FxHashMap::default(),
            measurements,
            high_water_mark: self.snapshot.high_water_mark,
            cancel: self.cancel.clone(),
        }
    }

    /// Full table scan via direct leaf iteration. Callback returns `false` to
    /// stop.
    pub fn table_scan_raw<F>(&mut self, table: &[u8], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        let leaves = self.collect_table_leaves(table)?;
        self.scan_leaves(&leaves, f)
    }

    /// DFS pass that loads each page into the cache and collects leaves in
    /// left-to-right order.
    fn load_and_collect_leaves(
        &mut self,
        page_id: PageId,
        leaves: &mut Vec<Arc<Page>>,
    ) -> Result<()> {
        // Runs to completion before `table_scan_raw` emits a row, so skipping it
        // leaves the whole descent of a large table uncancellable.
        if let Some(t) = self.cancel.as_ref() {
            t.check()?;
        }
        let page = if let Some(p) = self.page_cache.get(&page_id) {
            Arc::clone(p)
        } else {
            let arc = self
                .manager
                .fetch_reachable_page(page_id, self.snapshot.high_water_mark)?;
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
        self.check_cancel()?;
        if let Some(desc) = self.resolved_tables.get(name) {
            return Ok(desc.clone());
        }

        let desc = self.lookup_table_uncached(name)?;
        self.resolved_tables.insert(name.to_vec(), desc.clone());
        Ok(desc)
    }

    fn lookup_table_uncached(&self, name: &[u8]) -> Result<TableDescriptor> {
        self.check_cancel()?;

        let catalog_root = self.snapshot.catalog_root;
        if !catalog_root.is_valid() {
            return Err(Error::TableNotFound(
                String::from_utf8_lossy(name).into_owned(),
            ));
        }

        let mut current = catalog_root;
        let mut visited = FxHashSet::default();
        let mut desc = loop {
            self.check_cancel()?;
            if !visited.insert(current) {
                return Err(Error::DatabaseCorrupted);
            }
            let page = self.read_reachable_page(current)?;
            match page.page_type() {
                Some(PageType::Leaf) => {
                    let cells = leaf_node::read_cells_checked(&page)
                        .map_err(|_| Error::DatabaseCorrupted)?;
                    break match cells.binary_search_by(|cell| cell.key.cmp(name)) {
                        Ok(idx) => {
                            let cell = cells[idx];
                            if cell.val_type == ValueType::Tombstone {
                                Err(Error::TableNotFound(
                                    String::from_utf8_lossy(name).into_owned(),
                                ))
                            } else if cell.val_type != ValueType::Inline {
                                Err(Error::DatabaseCorrupted)
                            } else {
                                TableDescriptor::try_deserialize(cell.value)
                                    .ok_or(Error::DatabaseCorrupted)
                            }
                        }
                        Err(_) => Err(Error::TableNotFound(
                            String::from_utf8_lossy(name).into_owned(),
                        )),
                    };
                }
                Some(PageType::Branch) => {
                    let cells = branch_node::read_cells_checked(&page)
                        .map_err(|_| Error::DatabaseCorrupted)?;
                    // Find the first separator strictly greater than the name.
                    let mut lo = 0usize;
                    let mut hi = cells.len();
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        if name < cells[mid].key {
                            hi = mid;
                        } else {
                            lo = mid + 1;
                        }
                    }
                    current = if lo < cells.len() {
                        cells[lo].child
                    } else {
                        page.right_child()
                    };
                }
                _ => {
                    return Err(Error::InvalidPageType(page.page_type_raw(), current));
                }
            }
        }?;

        // The catalog proves the exact name exists. Only now is it safe to
        // consult the hash-only slot cache; in Off mode this root/count can be
        // newer than the descriptor. A legacy catalog with two live names for
        // this hash remains ambiguous and is rejected by the lazy index.
        if let Some((root, depth)) = self.snapshot.named_entry_root(name) {
            self.manager
                .reject_named_table_hash_collision(name, self.cancel.as_ref())?;
            let Some(entry_count) = self.snapshot.named_entry_count(name) else {
                return Err(Error::DatabaseCorrupted);
            };
            desc.root_page = root;
            desc.depth = depth;
            desc.entry_count = entry_count;
        }

        self.check_cancel()?;
        Ok(desc)
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
                self.materialize_overflow(&oref).map(Some)
            }
            Some((_, value)) => Ok(Some(value)),
        }
    }

    fn load_page(&mut self, page_id: PageId) -> Result<&Page> {
        if !self.page_cache.contains_key(&page_id) {
            let arc = self
                .manager
                .fetch_reachable_page(page_id, self.snapshot.high_water_mark)?;
            self.page_cache.insert(page_id, arc);
        }
        Ok(self.page_cache.get(&page_id).unwrap())
    }

    fn preload_all_pages(&mut self, root: PageId) -> Result<()> {
        let mut stack = vec![root];
        while let Some(current) = stack.pop() {
            // Runs to completion before `for_each` yields anything, so without
            // its own check a cancel waits out the whole tree walk.
            if let Some(t) = self.cancel.as_ref() {
                t.check()?;
            }
            if !self.page_cache.contains_key(&current) {
                let arc = self
                    .manager
                    .fetch_reachable_page(current, self.snapshot.high_water_mark)?;
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
        // Registration is keyed by snapshot txn id (see begin_read).
        self.manager.unregister_reader(self.snapshot.txn_id);
    }
}

/// Scan adapter wrapping a `&mut ReadTxn` for use with [`crate::TableIter`].
pub struct ReadTxnScanAdapter<'a, 'db: 'a> {
    txn: &'a mut ReadTxn<'db>,
    measurements: Vec<Arc<AtomicU64>>,
}

impl<'a, 'db: 'a> crate::scan_iter::TxnScanAdapter for ReadTxnScanAdapter<'a, 'db> {
    fn with_loader<R>(&mut self, f: &mut dyn FnMut(&mut dyn PageLoader) -> Result<R>) -> Result<R> {
        let mut view = ReadPages {
            cache: &mut self.txn.page_cache,
            manager: self.txn.manager,
            high_water_mark: self.txn.snapshot.high_water_mark,
        };
        f(&mut view)
    }

    fn cancel(&self) -> Option<&CancelToken> {
        self.txn.cancel.as_ref()
    }

    fn record_rows_scanned(&self, rows: u64) {
        self.txn
            .manager
            .add_rows_scanned_to(rows, &self.measurements);
    }
}

/// Scan adapter owning a `ReadTxn` for iterators that outlive a borrow scope.
pub struct OwnedReadTxnAdapter<'db> {
    txn: ReadTxn<'db>,
    measurements: Vec<Arc<AtomicU64>>,
}

impl<'db> crate::scan_iter::TxnScanAdapter for OwnedReadTxnAdapter<'db> {
    fn with_loader<R>(&mut self, f: &mut dyn FnMut(&mut dyn PageLoader) -> Result<R>) -> Result<R> {
        let mut view = ReadPages {
            cache: &mut self.txn.page_cache,
            manager: self.txn.manager,
            high_water_mark: self.txn.snapshot.high_water_mark,
        };
        f(&mut view)
    }

    fn cancel(&self) -> Option<&CancelToken> {
        self.txn.cancel.as_ref()
    }

    fn record_rows_scanned(&self, rows: u64) {
        self.txn
            .manager
            .add_rows_scanned_to(rows, &self.measurements);
    }
}

#[cfg(test)]
#[path = "read_txn_tests.rs"]
mod tests;
