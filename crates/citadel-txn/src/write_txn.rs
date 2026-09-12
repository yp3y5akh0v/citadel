//! Write transaction: CoW mutations with shadow-paging commit.

use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{
    CancelToken, Error, Result, MAX_INLINE_VALUE_SIZE, MAX_KEY_SIZE, MAX_VALUE_SIZE,
};
use citadel_io::file_manager::CommitSlot;
use citadel_page::branch_node;
use citadel_page::leaf_node::OverflowRef;
use citadel_page::overflow;
use citadel_page::page::Page;
use rustc_hash::{FxHashMap, FxHashSet};
use std::borrow::Cow;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use citadel_buffer::allocator::{AllocCheckpoint, PageAllocator};
use citadel_buffer::btree::{self, BTree, UpsertAction, UpsertOutcome};
use citadel_buffer::cursor::{Cursor, PageLoader, PageMap};

use crate::catalog::TableDescriptor;
use crate::manager::TxnManager;
use crate::merkle;
use crate::overflow_io;
use crate::read_txn::ScanCount;
use crate::ReadBudget;

thread_local! {
    static PATH_BUF: std::cell::RefCell<Vec<(PageId, usize)>> =
        std::cell::RefCell::new(Vec::with_capacity(8));
}

#[cfg(test)]
thread_local! {
    /// Deterministic cancellation injection for loops that expose no callback
    /// or I/O hook (notably catalog reconstruction and commit finalization).
    static CANCEL_ON_NTH_WRITE_CHECK: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    static CANCEL_ON_NTH_TREE_FREE: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(crate) struct CancelOnNthWriteCheckGuard {
    previous: Option<usize>,
}

#[cfg(test)]
impl Drop for CancelOnNthWriteCheckGuard {
    fn drop(&mut self) {
        CANCEL_ON_NTH_WRITE_CHECK.with(|remaining| remaining.set(self.previous));
    }
}

#[cfg(test)]
pub(crate) fn cancel_on_nth_write_check(nth: usize) -> CancelOnNthWriteCheckGuard {
    assert!(nth > 0);
    let previous = CANCEL_ON_NTH_WRITE_CHECK.with(|remaining| remaining.replace(Some(nth)));
    CancelOnNthWriteCheckGuard { previous }
}

#[cfg(test)]
pub(crate) struct CancelOnNthTreeFreeGuard {
    previous: Option<usize>,
}

#[cfg(test)]
impl Drop for CancelOnNthTreeFreeGuard {
    fn drop(&mut self) {
        CANCEL_ON_NTH_TREE_FREE.with(|remaining| remaining.set(self.previous));
    }
}

#[cfg(test)]
pub(crate) fn cancel_on_nth_tree_free(nth: usize) -> CancelOnNthTreeFreeGuard {
    assert!(nth > 0);
    let previous = CANCEL_ON_NTH_TREE_FREE.with(|remaining| remaining.replace(Some(nth)));
    CancelOnNthTreeFreeGuard { previous }
}

#[derive(Debug, Clone)]
pub enum InsertOutcome {
    Inserted,
    Existed(Vec<u8>),
}

/// A path loaded before value staging, valid until this operation mutates the tree.
struct LoadedLeaf {
    path: Vec<(PageId, usize)>,
    id: PageId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteFailure {
    Cancelled,
    Failed,
}

/// A callback panic can unwind after earlier rows were patched in place.
struct WriteScanPanicGuard<'a> {
    failure: &'a mut Option<WriteFailure>,
}

impl Drop for WriteScanPanicGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() && self.failure.is_none() {
            *self.failure = Some(WriteFailure::Failed);
        }
    }
}

#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct MutationMarker(u64);

impl WriteFailure {
    fn error(self) -> Error {
        match self {
            Self::Cancelled => Error::Interrupted,
            Self::Failed => Error::TransactionFailed,
        }
    }
}

struct WritePages<'a> {
    pages: &'a mut FxHashMap<PageId, Page>,
    manager: &'a TxnManager,
}

impl PageMap for WritePages<'_> {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.pages.get(id)
    }
}

impl PageLoader for WritePages<'_> {
    fn ensure_loaded(&mut self, id: PageId) -> Result<()> {
        if !self.pages.contains_key(&id) {
            let page = self.manager.fetch_page_owned(id)?;
            self.pages.insert(id, page);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DeferredFkCheck {
    /// Physical identity of the child whose final reference must be checked.
    pub child_table: Vec<u8>,
    pub child_key: Vec<u8>,
    /// Column names keep the constraint identity stable when ordinals shift.
    pub child_columns: Vec<String>,
    pub foreign_table: String,
    pub referred_columns: Vec<String>,
}

pub struct WriteTxn<'a> {
    manager: &'a TxnManager,
    base_txn_id: TxnId,
    txn_id: TxnId,
    old_slot: Arc<CommitSlot>,
    pages: FxHashMap<PageId, Page>,
    tree: BTree,
    alloc: PageAllocator,
    committed: bool,
    named_trees: FxHashMap<Vec<u8>, BTree>,
    catalog: Option<BTree>,
    catalog_dirty: bool,
    loaded_tree_meta: FxHashMap<Vec<u8>, (PageId, u16)>,
    deferred_fk_checks: Vec<DeferredFkCheck>,
    fk_check_cache: FxHashMap<Vec<u8>, Vec<u8>>,
    /// Set by refresh_all_catalog_descriptors: commit even when nothing
    /// changed, so the inactive slot is rewritten (and resealed V1).
    force_commit: bool,
    /// UPDATE and DELETE scan through here, so a cancellation covering only the
    /// read path would stop the quickest queries and leave the long ones running.
    cancel: Option<CancelToken>,
    read_budget: Option<ReadBudget>,
    /// A failed mutation can leave a prefix in the CoW page set or allocator.
    /// Keep cancellation distinct from other failures so commit reports the
    /// cause accurately while refusing both states.
    failure: Option<WriteFailure>,
    /// Advances after each successful low-level mutation. Statement executors
    /// use it to distinguish an error before any write from an error after a
    /// partially applied batch.
    mutation_sequence: u64,
}

#[derive(Clone)]
pub struct WriteTxnSnapshot {
    tree: BTree,
    alloc_checkpoint: AllocCheckpoint,
    named_trees: FxHashMap<Vec<u8>, BTree>,
    catalog: Option<BTree>,
    catalog_dirty: bool,
    loaded_tree_meta: FxHashMap<Vec<u8>, (PageId, u16)>,
    deferred_fk_checks_len: usize,
    failure: Option<WriteFailure>,
    mutation_sequence: u64,
}

impl<'db> WriteTxn<'db> {
    pub(crate) fn new(
        manager: &'db TxnManager,
        txn_id: TxnId,
        snapshot: Arc<CommitSlot>,
        tree: BTree,
        alloc: PageAllocator,
        recycled_pages: Option<FxHashMap<PageId, Page>>,
    ) -> Self {
        // Recycling reuses only the map's allocation: entries are keyed by
        // page ids that CoW retires every txn, so contents are always stale.
        let pages = match recycled_pages {
            Some(mut m) => {
                m.clear();
                m
            }
            None => FxHashMap::with_capacity_and_hasher(16, Default::default()),
        };
        Self {
            manager,
            base_txn_id: txn_id,
            txn_id,
            old_slot: snapshot,
            pages,
            tree,
            alloc,
            committed: false,
            named_trees: FxHashMap::default(),
            catalog: None,
            catalog_dirty: false,
            loaded_tree_meta: FxHashMap::default(),
            deferred_fk_checks: Vec::new(),
            fk_check_cache: FxHashMap::default(),
            force_commit: false,
            cancel: None,
            read_budget: None,
            failure: None,
            mutation_sequence: 0,
        }
    }

    pub fn set_cancel(&mut self, token: Option<CancelToken>) {
        self.cancel = token;
    }

    /// Record that cancellation may have left a partially applied operation.
    #[doc(hidden)]
    pub fn mark_cancelled(&mut self) {
        if self.failure.is_none() {
            self.failure = Some(WriteFailure::Cancelled);
        }
    }

    /// Refuse commit after a higher layer reports a non-cancellation failure
    /// that may have followed one or more successful low-level mutations.
    #[doc(hidden)]
    pub fn mark_failed(&mut self) {
        Self::mark_failed_with(&mut self.failure);
    }

    pub fn is_poisoned(&self) -> bool {
        self.failure.is_some()
    }

    /// Verify that an earlier mutation has not made this transaction unusable.
    /// Unlike `is_poisoned`, this preserves whether that operation was
    /// cancelled or failed for another reason.
    pub fn check_usable(&self) -> Result<()> {
        match self.failure {
            Some(failure) => Err(failure.error()),
            None => Ok(()),
        }
    }

    /// Opaque checkpoint used by statement executors to detect partial writes.
    #[doc(hidden)]
    pub fn mutation_marker(&self) -> MutationMarker {
        MutationMarker(self.mutation_sequence)
    }

    #[doc(hidden)]
    pub fn mutated_since(&self, marker: MutationMarker) -> bool {
        self.mutation_sequence != marker.0
    }

    pub fn cancel_token(&self) -> Option<&CancelToken> {
        self.cancel.as_ref()
    }

    pub fn set_read_budget(&mut self, budget: Option<ReadBudget>) {
        self.read_budget = budget;
    }

    pub fn read_budget(&self) -> Option<&ReadBudget> {
        self.read_budget.as_ref()
    }

    /// Refuse a mutation once the token is tripped.
    ///
    /// Checking at the mutation covers every apply loop across the executor,
    /// including ones added later, for one relaxed load per row applied.
    ///
    /// It does NOT poison: this transaction cannot see statement
    /// boundaries, so it cannot tell a refusal that changed nothing from one
    /// that stopped part-way. The statement layer knows, and poisons there.
    #[inline]
    fn check_cancel(&self) -> Result<()> {
        self.check_usable()?;
        match &self.cancel {
            Some(t) => {
                #[cfg(test)]
                CANCEL_ON_NTH_WRITE_CHECK.with(|remaining| {
                    if let Some(checks) = remaining.get() {
                        if checks == 1 {
                            remaining.set(None);
                            t.cancel();
                        } else {
                            remaining.set(Some(checks - 1));
                        }
                    }
                });
                t.check()
            }
            None => Ok(()),
        }
    }

    /// Check at the far side of a mutation. Once work may have been applied,
    /// observing cancellation must also make the transaction uncommittable.
    #[inline]
    fn finish_mutation<T>(&mut self, value: T, changed: bool) -> Result<T> {
        let Self {
            cancel,
            failure,
            mutation_sequence,
            ..
        } = self;
        Self::finish_mutation_with(cancel.as_ref(), failure, mutation_sequence, value, changed)
    }

    #[inline]
    fn finish_mutation_with<T>(
        cancel: Option<&CancelToken>,
        failure: &mut Option<WriteFailure>,
        mutation_sequence: &mut u64,
        value: T,
        changed: bool,
    ) -> Result<T> {
        if changed {
            *mutation_sequence = mutation_sequence.wrapping_add(1);
        }
        match cancel {
            Some(token) => match token.check() {
                Ok(()) => Ok(value),
                Err(err) => {
                    Self::record_failure(failure, &err);
                    Err(err)
                }
            },
            None => Ok(value),
        }
    }

    #[inline]
    fn record_failure(failure: &mut Option<WriteFailure>, err: &Error) {
        if failure.is_none() {
            *failure = Some(if matches!(err, Error::Interrupted) {
                WriteFailure::Cancelled
            } else {
                WriteFailure::Failed
            });
        }
    }

    #[inline]
    fn fail<T>(&mut self, err: Error) -> Result<T> {
        Self::record_failure(&mut self.failure, &err);
        Err(err)
    }

    #[inline]
    fn fail_with<T>(failure: &mut Option<WriteFailure>, err: Error) -> Result<T> {
        Self::record_failure(failure, &err);
        Err(err)
    }

    #[inline]
    fn mark_failed_with(failure: &mut Option<WriteFailure>) {
        if failure.is_none() {
            *failure = Some(WriteFailure::Failed);
        }
    }

    /// Freeing an overflow chain mutates the allocator a page at a time. Any
    /// error can therefore leave a prefix in the pending-free set; make that
    /// transaction uncommittable before propagating the error.
    fn free_overflow_chain(&mut self, first: PageId) -> Result<()> {
        let Self {
            pages,
            alloc,
            manager,
            cancel,
            failure,
            ..
        } = self;
        let mut view = WritePages { pages, manager };
        Self::free_overflow_chain_with_parts(&mut view, alloc, first, cancel.as_ref(), failure)
    }

    fn free_overflow_chain_with_parts(
        loader: &mut dyn PageLoader,
        alloc: &mut PageAllocator,
        first: PageId,
        cancel: Option<&CancelToken>,
        failure: &mut Option<WriteFailure>,
    ) -> Result<()> {
        let result = overflow_io::free_chain_with_cancel(loader, alloc, first, cancel);
        if let Err(err) = &result {
            Self::record_failure(failure, err);
        }
        result
    }

    /// Database-wide scan telemetry across all transactions and threads.
    /// Monotonic; use [`WriteTxn::measure_scans`] for an isolated operation.
    pub fn rows_scanned(&self) -> u64 {
        self.manager.rows_scanned()
    }

    /// Begin an operation-local scan measurement on this transaction's thread.
    pub fn measure_scans(&self) -> crate::manager::ScanMeasurement {
        self.manager.measure_scans()
    }

    #[inline]
    pub fn fk_check_cached(&self, foreign_table: &[u8], key: &[u8]) -> bool {
        self.fk_check_cache
            .get(foreign_table)
            .map(|cached| cached.as_slice() == key)
            .unwrap_or(false)
    }

    #[inline]
    pub fn mark_fk_verified(&mut self, foreign_table: &[u8], key: &[u8]) {
        self.fk_check_cache
            .insert(foreign_table.to_vec(), key.to_vec());
    }

    #[inline]
    fn invalidate_fk_cache_for(&mut self, table: &[u8]) {
        if !self.fk_check_cache.is_empty() {
            self.fk_check_cache.remove(table);
        }
    }

    pub fn defer_fk_check(&mut self, check: DeferredFkCheck) {
        self.deferred_fk_checks.push(check);
    }

    pub fn take_deferred_fk_checks(&mut self) -> Vec<DeferredFkCheck> {
        std::mem::take(&mut self.deferred_fk_checks)
    }

    pub fn deferred_fk_check_count(&self) -> usize {
        self.deferred_fk_checks.len()
    }

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    pub fn entry_count(&self) -> u64 {
        self.tree.entry_count
    }

    /// The table's catalog root in its committed view (a lookup, no scan).
    /// Use [`WriteTxn::table_root_stamp`] for the non-ABA write-view identity.
    pub fn table_root_page(&self, table: &[u8]) -> Result<Option<PageId>> {
        self.check_cancel()?;
        let root = self.manager.table_root(table)?;
        self.check_cancel()?;
        Ok(root)
    }

    /// The table root in this transaction's write view together with the
    /// transaction id stored in that root page. The page transaction id keeps
    /// this stamp distinct after physical page-id recycling.
    pub fn table_root_stamp(&mut self, table: &[u8]) -> Result<Option<(PageId, TxnId)>> {
        self.check_cancel()?;
        match self.ensure_table(table) {
            Ok(()) => {}
            Err(Error::TableNotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        }
        let root = self
            .named_trees
            .get(table)
            .expect("ensure_table installed the requested tree")
            .root;
        if !self.pages.contains_key(&root) {
            let page = self.manager.fetch_page_owned(root)?;
            self.pages.insert(root, page);
        }
        let root_txn = self
            .pages
            .get(&root)
            .expect("root page was loaded into the write view")
            .txn_id();
        self.check_cancel()?;
        Ok(Some((root, root_txn)))
    }

    pub fn pending_free_count(&self) -> usize {
        self.alloc.freed_count()
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_cancel()?;
        let leaf_id = Self::descend_to_leaf(&mut self.pages, self.manager, self.tree.root, key)?;
        let found = BTree::search_at_leaf(&self.pages, leaf_id, key)?;
        let value = self.materialize_value(found)?;
        self.check_cancel()?;
        Ok(value)
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        self.check_cancel()?;
        Self::validate_key_value(key, value)?;
        let leaf = Self::load_insert_leaf(&self.tree, &mut self.pages, self.manager, key, value)?;
        let (val_type, val_payload) = self.stage_value(value)?;
        let inserted = Self::insert_into_tree(
            &mut self.tree,
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            key,
            val_type,
            val_payload.as_ref(),
            leaf,
        );
        let (inserted, replaced) = match inserted {
            Ok(result) => result,
            Err(err) => return self.fail(err),
        };
        if let Some(head) = replaced {
            self.free_overflow_chain(head)?;
        }
        self.finish_mutation(inserted, true)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_into_tree(
        tree: &mut BTree,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        txn_id: TxnId,
        key: &[u8],
        val_type: ValueType,
        val_bytes: &[u8],
        leaf: Option<LoadedLeaf>,
    ) -> Result<(bool, Option<PageId>)> {
        match leaf {
            Some(LoadedLeaf { path, id }) => {
                tree.insert_at_leaf(pages, alloc, txn_id, key, val_type, val_bytes, path, id)
            }
            None => {
                let inserted = tree
                    .try_lil_insert(pages, alloc, txn_id, key, val_type, val_bytes)?
                    .expect("staging an inline value preserves the loaded append path");
                Ok((inserted, None))
            }
        }
    }

    /// Resolve all fallible reads before staging overflow pages. A missing leaf
    /// denotes a proven inline append through the tree's existing rightmost path.
    fn load_insert_leaf(
        tree: &BTree,
        pages: &mut FxHashMap<PageId, Page>,
        manager: &TxnManager,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<LoadedLeaf>> {
        if value.len() <= MAX_INLINE_VALUE_SIZE && tree.lil_would_hit(pages, key) {
            return Ok(None);
        }
        let (path, id) = Self::walk_loading(pages, manager, tree.root, key)?;
        Ok(Some(LoadedLeaf { path, id }))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.check_cancel()?;
        let (mut path, leaf_id) =
            Self::walk_loading(&mut self.pages, self.manager, self.tree.root, key)?;
        let deleted = self.tree.delete_at_leaf_with_overflow(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            key,
            &mut path,
            leaf_id,
        );
        let (deleted, overflow_head) = match deleted {
            Ok(result) => result,
            Err(err) => return self.fail(err),
        };
        if let Some(head) = overflow_head {
            self.free_overflow_chain(head)?;
        }
        self.finish_mutation(deleted, deleted)
    }

    pub fn for_each<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        self.check_cancel()?;
        let root = self.tree.root;
        self.preload_all_pages(root)?;
        let mut count = ScanCount::new(self.manager);
        let mut cursor = Cursor::first(&self.pages, root)?;
        while cursor.is_valid() {
            if let Some(t) = self.cancel.as_ref() {
                t.check()?;
            }
            count.rows += 1;
            let overflow = cursor
                .current_ref(&self.pages)
                .and_then(|c| match c.val_type {
                    ValueType::Overflow => Some((c.key.to_vec(), OverflowRef::from_bytes(c.value))),
                    _ => None,
                });
            if let Some((key, oref)) = overflow {
                let materialized = self.materialize_overflow(&oref)?;
                f(&key, &materialized)?;
            } else if let Some(entry) = cursor.current_ref(&self.pages) {
                if entry.val_type != ValueType::Tombstone {
                    if let Some(budget) = &self.read_budget {
                        budget.try_charge(entry.value.len())?;
                    }
                    f(entry.key, entry.value)?;
                }
            }
            cursor.next(&self.pages)?;
        }
        Ok(())
    }

    /// Turn an owned leaf value into the caller-visible value, admitting the
    /// complete payload against the read budget before exposing it.
    fn materialize_value(
        &mut self,
        found: Option<(ValueType, Vec<u8>)>,
    ) -> Result<Option<Vec<u8>>> {
        match found {
            None | Some((ValueType::Tombstone, _)) => Ok(None),
            Some((ValueType::Overflow, payload)) => self
                .materialize_overflow(&OverflowRef::from_bytes(&payload))
                .map(Some),
            Some((_, value)) => {
                if let Some(budget) = &self.read_budget {
                    budget.try_charge(value.len())?;
                }
                Ok(Some(value))
            }
        }
    }

    fn materialize_overflow(&mut self, oref: &OverflowRef) -> Result<Vec<u8>> {
        let Self {
            pages,
            manager,
            cancel,
            read_budget,
            ..
        } = self;
        let mut view = WritePages { pages, manager };
        overflow_io::read_chain_value_with_budget(
            &mut view,
            oref,
            cancel.as_ref(),
            read_budget.as_ref(),
        )
    }

    pub fn table_entry_count(&mut self, table: &[u8]) -> Result<u64> {
        self.check_cancel()?;
        self.ensure_table(table)?;
        let count = self.named_trees[table].entry_count;
        self.check_cancel()?;
        Ok(count)
    }

    pub fn table_for_each<F>(&mut self, table: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        self.check_cancel()?;
        self.ensure_table(table)?;
        let root = self.named_trees[table].root;
        self.preload_all_pages(root)?;
        let mut count = ScanCount::new(self.manager);
        let mut cursor = Cursor::first(&self.pages, root)?;
        while cursor.is_valid() {
            if let Some(t) = self.cancel.as_ref() {
                t.check()?;
            }
            count.rows += 1;
            let overflow = cursor
                .current_ref(&self.pages)
                .and_then(|c| match c.val_type {
                    ValueType::Overflow => Some((c.key.to_vec(), OverflowRef::from_bytes(c.value))),
                    _ => None,
                });
            if let Some((key, oref)) = overflow {
                let materialized = self.materialize_overflow(&oref)?;
                f(&key, &materialized)?;
            } else if let Some(entry) = cursor.current_ref(&self.pages) {
                if entry.val_type != ValueType::Tombstone {
                    if let Some(budget) = &self.read_budget {
                        budget.try_charge(entry.value.len())?;
                    }
                    f(entry.key, entry.value)?;
                }
            }
            cursor.next(&self.pages)?;
        }
        Ok(())
    }

    pub fn table_scan_from<F>(&mut self, table: &[u8], start_key: &[u8], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        self.table_scan_from_impl(table, start_key, None, f)
    }

    /// Lazy prefix scan. Out-of-prefix values are not materialized or charged.
    pub fn table_scan_prefix<F>(&mut self, table: &[u8], prefix: &[u8], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        self.table_scan_from_impl(table, prefix, Some(prefix), f)
    }

    fn table_scan_from_impl<F>(
        &mut self,
        table: &[u8],
        start_key: &[u8],
        prefix: Option<&[u8]>,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        self.check_cancel()?;
        self.ensure_table(table)?;
        let root = self.named_trees[table].root;
        let cancel = self.cancel.clone();
        let budget = self.read_budget.clone();
        let mut count = ScanCount::new(self.manager);
        let mut view = WritePages {
            pages: &mut self.pages,
            manager: self.manager,
        };
        let mut cursor = Cursor::seek_lazy(&mut view, root, start_key)?;
        while let Some(cell) = cursor.current_ref_lazy(&mut view) {
            if let Some(t) = cancel.as_ref() {
                t.check()?;
            }
            if prefix.is_some_and(|prefix| !cell.key.starts_with(prefix)) {
                break;
            }
            count.rows += 1;
            match cell.val_type {
                ValueType::Tombstone => {}
                ValueType::Inline => {
                    let entry = cursor.current_ref_lazy(&mut view).unwrap();
                    if let Some(budget) = &budget {
                        budget.try_charge(entry.value.len())?;
                    }
                    if !f(entry.key, entry.value)? {
                        break;
                    }
                }
                ValueType::Overflow => {
                    let (key, oref) = {
                        let c = cursor.current_ref_lazy(&mut view).unwrap();
                        (c.key.to_vec(), OverflowRef::from_bytes(c.value))
                    };
                    let materialized = overflow_io::read_chain_value_with_budget(
                        &mut view,
                        &oref,
                        cancel.as_ref(),
                        budget.as_ref(),
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

    /// Pull-based scan from `start_key`. Returns a lending iterator.
    pub fn table_scan_iter<'a>(
        &'a mut self,
        table: &[u8],
        start_key: &[u8],
    ) -> Result<crate::scan_iter::TableIter<WriteTxnScanAdapter<'a, 'db>>> {
        self.check_cancel()?;
        self.ensure_table(table)?;
        let root = self.named_trees[table].root;
        let cursor = {
            let mut view = WritePages {
                pages: &mut self.pages,
                manager: self.manager,
            };
            Cursor::seek_lazy(&mut view, root, start_key)?
        };
        let measurements = self.manager.active_scan_measurements();
        let adapter = WriteTxnScanAdapter {
            txn: self,
            measurements,
        };
        Ok(crate::scan_iter::TableIter::new(adapter, cursor))
    }

    pub fn create_table(&mut self, name: &[u8]) -> Result<()> {
        self.create_table_impl(name, true)
    }

    #[cfg(test)]
    pub(crate) fn create_table_without_hash_guard_for_test(&mut self, name: &[u8]) -> Result<()> {
        self.create_table_impl(name, false)
    }

    fn create_table_impl(&mut self, name: &[u8], reject_hash_collision: bool) -> Result<()> {
        self.check_cancel()?;
        self.fk_check_cache.clear();
        self.ensure_catalog()?;

        if self.named_trees.contains_key(name) {
            return Err(Error::TableAlreadyExists(
                String::from_utf8_lossy(name).into_owned(),
            ));
        }

        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, name)?;
        if let Some((vt, _)) = self.catalog.as_ref().unwrap().search(&self.pages, name)? {
            if vt != ValueType::Tombstone {
                return Err(Error::TableAlreadyExists(
                    String::from_utf8_lossy(name).into_owned(),
                ));
            }
        }
        if reject_hash_collision {
            self.reject_table_name_hash_collision(name, None)?;
        }

        let page_id = self.alloc.allocate();
        let mut leaf = Page::new(page_id, PageType::Leaf, self.txn_id);
        leaf.update_checksum();
        self.pages.insert(page_id, leaf);

        let new_tree = BTree::from_existing(page_id, 1, 0);
        self.named_trees.insert(name.to_vec(), new_tree);
        self.catalog_dirty = true;
        self.finish_mutation((), true)
    }

    /// Format-upgrade primitive: rewrite every listed table's catalog
    /// descriptor this commit so its entry sheds SLOT_ENTRY_STALE and the
    /// slot can reseal V1. Dropping loaded_tree_meta forces the rewrite (the
    /// rename trick); force_commit lets an unchanged pass still rewrite the
    /// other physical slot.
    pub fn refresh_all_catalog_descriptors(&mut self, names: &[Vec<u8>]) -> Result<()> {
        self.check_cancel()?;
        // Resolve every name before changing descriptor state. A missing or
        // unreadable later table then leaves the transaction unchanged.
        for name in names {
            self.ensure_table(name)?;
        }
        for name in names {
            self.loaded_tree_meta.remove(name.as_slice());
            self.catalog_dirty = true;
            self.finish_mutation((), true)?;
        }
        self.force_commit = true;
        self.finish_mutation((), true)
    }

    pub fn drop_table(&mut self, name: &[u8]) -> Result<()> {
        self.check_cancel()?;
        // Cache keys are parent-table names: only the dropped table's entry
        // can go stale.
        self.invalidate_fk_cache_for(name);
        self.ensure_table(name)?;
        self.ensure_catalog()?;

        let tree = self.named_trees[name].clone();
        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, name)?;

        self.free_tree_pages(tree.root)?;
        self.named_trees.remove(name);

        let deleted = self.catalog.as_mut().unwrap().delete(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            name,
        );
        if let Err(err) = deleted {
            return self.fail(err);
        }
        self.catalog_dirty = true;
        self.finish_mutation((), true)
    }

    pub fn rename_table(&mut self, old_name: &[u8], new_name: &[u8]) -> Result<()> {
        self.check_cancel()?;
        self.fk_check_cache.clear();
        self.ensure_table(old_name)?;

        if self.named_trees.contains_key(new_name) {
            return Err(Error::TableAlreadyExists(
                String::from_utf8_lossy(new_name).into_owned(),
            ));
        }

        self.ensure_catalog()?;
        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, new_name)?;
        if let Some((vt, _)) = self
            .catalog
            .as_ref()
            .unwrap()
            .search(&self.pages, new_name)?
        {
            if vt != ValueType::Tombstone {
                return Err(Error::TableAlreadyExists(
                    String::from_utf8_lossy(new_name).into_owned(),
                ));
            }
        }
        self.reject_table_name_hash_collision(new_name, Some(old_name))?;

        // Both catalog paths are now resident, so no cold read can fail after
        // the in-memory name map has moved.
        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, old_name)?;

        let tree = self.named_trees.remove(old_name).unwrap();
        self.named_trees.insert(new_name.to_vec(), tree);

        // Keep the old name in loaded_tree_meta (like drop_table) so its hash
        // reaches build_slot_entries' known set and the old slot entry is
        // dropped; removing it would let ensure_table resurrect the old name
        // as an alias. finalize_catalog still writes the new name (it iterates
        // named_trees).

        let deleted = self.catalog.as_mut().unwrap().delete(
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            old_name,
        );
        if let Err(err) = deleted {
            return self.fail(err);
        }
        self.catalog_dirty = true;
        self.finish_mutation((), true)
    }

    pub fn table_insert(&mut self, table: &[u8], key: &[u8], value: &[u8]) -> Result<bool> {
        self.table_insert_impl(table, key, value, true)
    }

    /// Insert into an index tree, skipping FK-cache invalidation (never an FK
    /// parent).
    pub fn table_insert_index(&mut self, table: &[u8], key: &[u8], value: &[u8]) -> Result<bool> {
        self.table_insert_impl(table, key, value, false)
    }

    #[inline]
    fn table_insert_impl(
        &mut self,
        table: &[u8],
        key: &[u8],
        value: &[u8],
        invalidate_fk: bool,
    ) -> Result<bool> {
        self.check_cancel()?;
        Self::validate_key_value(key, value)?;
        if invalidate_fk {
            self.invalidate_fk_cache_for(table);
        }
        self.ensure_table(table)?;
        let inserted = self.stage_and_insert(table, key, value)?;
        self.finish_mutation(inserted, true)
    }

    /// Stage `value` and insert through the split-safe tree path, freeing any
    /// replaced overflow chain. Table must already be ensured.
    fn stage_and_insert(&mut self, table: &[u8], key: &[u8], value: &[u8]) -> Result<bool> {
        let leaf = Self::load_insert_leaf(
            &self.named_trees[table],
            &mut self.pages,
            self.manager,
            key,
            value,
        )?;
        self.stage_and_insert_at_leaf(table, key, value, leaf)
    }

    fn stage_and_insert_at_leaf(
        &mut self,
        table: &[u8],
        key: &[u8],
        value: &[u8],
        leaf: Option<LoadedLeaf>,
    ) -> Result<bool> {
        let (val_type, val_payload) = self.stage_value(value)?;
        let tree = self.named_trees.get_mut(table).unwrap();
        let inserted = Self::insert_into_tree(
            tree,
            &mut self.pages,
            &mut self.alloc,
            self.txn_id,
            key,
            val_type,
            val_payload.as_ref(),
            leaf,
        );
        let (inserted, replaced) = match inserted {
            Ok(result) => result,
            Err(err) => return self.fail(err),
        };
        if let Some(head) = replaced {
            self.free_overflow_chain(head)?;
        }
        Ok(inserted)
    }

    #[inline]
    pub fn table_insert_if_absent(
        &mut self,
        table: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<bool> {
        self.check_cancel()?;
        Self::validate_key_value(key, value)?;
        self.invalidate_fk_cache_for(table);
        // Resolve the table before staging an oversized value. Catalog lookup
        // can fail or be cancelled; staging first would leave allocated
        // overflow pages in an otherwise committable transaction.
        self.ensure_table(table)?;
        let leaf = Self::load_insert_leaf(
            &self.named_trees[table],
            &mut self.pages,
            self.manager,
            key,
            value,
        )?;
        let (val_type, val_payload) = self.stage_value(value)?;
        let val_bytes = val_payload.as_ref();
        let tree = self.named_trees.get_mut(table).unwrap();
        let inserted = match leaf {
            Some(LoadedLeaf { path, id }) => tree.insert_if_absent_at_leaf(
                &mut self.pages,
                &mut self.alloc,
                self.txn_id,
                key,
                val_type,
                val_bytes,
                path,
                id,
            ),
            None => tree
                .try_lil_insert(
                    &mut self.pages,
                    &mut self.alloc,
                    self.txn_id,
                    key,
                    val_type,
                    val_bytes,
                )
                .map(|inserted| inserted.expect("staging preserves the loaded append path")),
        };
        let inserted = match inserted {
            Ok(inserted) => inserted,
            Err(err) => return self.fail(err),
        };
        if !inserted && val_type == ValueType::Overflow {
            let oref = OverflowRef::from_bytes(val_bytes);
            self.free_overflow_chain(oref.first_page)?;
        }
        self.finish_mutation(inserted, inserted)
    }

    /// Update an existing value through one loaded tree path. The callback
    /// receives an owned, fully materialized value and may resize it; overflow
    /// staging and replacement are handled after the callback succeeds.
    ///
    /// Missing and tombstoned keys return `None` without calling `f`. A callback
    /// error or panic leaves the stored value unchanged. Errors after staging
    /// starts use the same transaction poisoning rules as [`Self::table_insert`].
    pub fn table_update_with<F, R, E>(
        &mut self,
        table: &[u8],
        key: &[u8],
        f: F,
    ) -> std::result::Result<Option<R>, E>
    where
        F: FnOnce(&mut Vec<u8>) -> std::result::Result<R, E>,
        E: From<Error>,
    {
        self.check_cancel()?;
        Self::validate_key_value(key, &[])?;
        self.ensure_table(table)?;
        let leaf = Self::load_insert_leaf(
            &self.named_trees[table],
            &mut self.pages,
            self.manager,
            key,
            &[],
        )?;
        let found = match &leaf {
            Some(leaf) => BTree::search_at_leaf(&self.pages, leaf.id, key)?,
            None => None,
        };
        let value = self.materialize_value(found)?;
        self.check_cancel()?;
        let Some(mut value) = value else {
            return Ok(None);
        };
        let result = f(&mut value)?;
        Self::validate_key_value(key, &value)?;
        self.check_cancel()?;
        self.invalidate_fk_cache_for(table);
        self.stage_and_insert_at_leaf(table, key, &value, leaf)?;
        self.finish_mutation(Some(result), true).map_err(E::from)
    }

    /// Upsert via callback. The existing value handed to `f` is fully
    /// materialized (overflow chains included), and both the replacement and
    /// `default_value` are staged, so values of any size are safe here.
    pub fn table_upsert_with<F, E>(
        &mut self,
        table: &[u8],
        key: &[u8],
        default_value: &[u8],
        mut f: F,
    ) -> std::result::Result<UpsertOutcome, E>
    where
        F: FnMut(&[u8]) -> std::result::Result<UpsertAction, E>,
        E: From<Error>,
    {
        self.table_upsert_with_owned(table, key, default_value, |old| f(&old))
    }

    /// Upsert while allowing the callback to reuse the materialized value's
    /// allocation. The callback runs only for an existing, non-tombstoned key.
    /// Its owned bytes are detached from stored pages: an error, panic or Skip
    /// leaves the stored value unchanged. Replacement and default values are
    /// validated and staged using the same rules as [`Self::table_upsert_with`].
    pub fn table_upsert_with_owned<F, E>(
        &mut self,
        table: &[u8],
        key: &[u8],
        default_value: &[u8],
        f: F,
    ) -> std::result::Result<UpsertOutcome, E>
    where
        F: FnOnce(Vec<u8>) -> std::result::Result<UpsertAction, E>,
        E: From<Error>,
    {
        self.check_cancel()?;
        Self::validate_key_value(key, default_value)?;
        self.invalidate_fk_cache_for(table);
        self.ensure_table(table)?;

        let leaf = Self::load_insert_leaf(
            &self.named_trees[table],
            &mut self.pages,
            self.manager,
            key,
            default_value,
        )?;
        let found = match &leaf {
            Some(leaf) => BTree::search_at_leaf(&self.pages, leaf.id, key)?,
            None => None,
        };
        let existing = self.materialize_value(found)?;

        let outcome = match existing {
            Some(old) => match f(old)? {
                UpsertAction::Skip => Ok(UpsertOutcome::Skipped),
                UpsertAction::Replace(new_bytes) => {
                    Self::validate_key_value(key, &new_bytes)?;
                    self.stage_and_insert_at_leaf(table, key, &new_bytes, leaf)?;
                    Ok(UpsertOutcome::Updated)
                }
            },
            None => {
                self.stage_and_insert_at_leaf(table, key, default_value, leaf)?;
                Ok(UpsertOutcome::Inserted)
            }
        }?;
        let changed = !matches!(outcome, UpsertOutcome::Skipped);
        self.finish_mutation(outcome, changed).map_err(E::from)
    }

    pub fn table_insert_or_fetch(
        &mut self,
        table: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertOutcome> {
        self.check_cancel()?;
        Self::validate_key_value(key, value)?;
        self.invalidate_fk_cache_for(table);
        if !self.named_trees.contains_key(table) {
            self.ensure_table(table)?;
        }
        // Load every fallible tree path before staging an overflow chain. A
        // failed cold-page walk must not leave allocated pages behind.
        let leaf = Self::load_insert_leaf(
            &self.named_trees[table],
            &mut self.pages,
            self.manager,
            key,
            value,
        )?;
        let (val_type, val_payload) = self.stage_value(value)?;
        let val_bytes = val_payload.as_ref();

        let Self {
            named_trees,
            pages,
            alloc,
            manager,
            txn_id,
            cancel,
            read_budget,
            failure,
            mutation_sequence,
            ..
        } = self;
        let tree = named_trees.get_mut(table).unwrap();
        let manager = *manager;
        let txn_id = *txn_id;

        let outcome = match leaf {
            Some(LoadedLeaf { path, id }) => tree
                .insert_or_fetch_at_leaf(pages, alloc, txn_id, key, val_type, val_bytes, path, id),
            None => tree
                .try_lil_insert(pages, alloc, txn_id, key, val_type, val_bytes)
                .map(|inserted| {
                    inserted.expect("staging preserves the loaded append path");
                    None
                }),
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(err) => return Self::fail_with(failure, err),
        };
        if outcome.is_some() && val_type == ValueType::Overflow {
            // Key already existed: drop the chain staged for the new value.
            let oref = OverflowRef::from_bytes(val_bytes);
            let mut view = WritePages { pages, manager };
            Self::free_overflow_chain_with_parts(
                &mut view,
                alloc,
                oref.first_page,
                cancel.as_ref(),
                failure,
            )?;
        }
        let result: Result<InsertOutcome> = match outcome {
            None => Ok(InsertOutcome::Inserted),
            Some((ValueType::Overflow, payload)) => {
                let oref = OverflowRef::from_bytes(&payload);
                let mut view = WritePages { pages, manager };
                overflow_io::read_chain_value_with_budget(
                    &mut view,
                    &oref,
                    cancel.as_ref(),
                    read_budget.as_ref(),
                )
                .map(InsertOutcome::Existed)
            }
            Some((_, value)) => read_budget
                .as_ref()
                .map_or(Ok(()), |budget| budget.try_charge(value.len()))
                .map(|()| InsertOutcome::Existed(value)),
        };
        match result {
            Ok(result) => {
                let changed = matches!(result, InsertOutcome::Inserted);
                Self::finish_mutation_with(
                    cancel.as_ref(),
                    failure,
                    mutation_sequence,
                    result,
                    changed,
                )
            }
            Err(err) => {
                // `insert_or_fetch` may already have inserted, or staged and
                // freed an unused overflow value, before it materializes the
                // existing one returned to the caller.
                Self::fail_with(failure, err)
            }
        }
    }

    /// Batch-update existing keys. Keys must be sorted.
    pub fn table_update_sorted(&mut self, table: &[u8], pairs: &[(&[u8], &[u8])]) -> Result<u64> {
        self.check_cancel()?;
        if pairs.is_empty() {
            return Ok(0);
        }
        self.invalidate_fk_cache_for(table);
        self.ensure_table(table)?;
        let cancel = self.cancel.clone();

        // `update_sorted_with` walks again when a key crosses a leaf boundary.
        // Load the union of all batch paths while the transaction is unchanged,
        // or a cold later leaf fails after an earlier pair has been applied.
        let root = self.named_trees[table].root;
        let mut pair_index = 0;
        while pair_index < pairs.len() {
            self.check_cancel()?;
            let leaf_id =
                Self::descend_to_leaf(&mut self.pages, self.manager, root, pairs[pair_index].0)?;
            let last_key = {
                let page = self.pages.get(&leaf_id).unwrap();
                let cell_count = page.num_cells();
                (cell_count > 0).then(|| {
                    citadel_page::leaf_node::read_cell(page, cell_count - 1)
                        .key
                        .to_vec()
                })
            };
            pair_index += 1;
            if let Some(last_key) = last_key {
                while pair_index < pairs.len() && pairs[pair_index].0 <= last_key.as_slice() {
                    pair_index += 1;
                }
            }
        }
        self.check_cancel()?;

        // Stage every value first: anything above the inline threshold goes
        // to an overflow chain, so each leaf cell stays page-sized.
        let allocated_before_staging = self.alloc.allocated_this_txn().len();
        let mut staged: Vec<(ValueType, Cow<'_, [u8]>)> = Vec::with_capacity(pairs.len());
        for &(key, value) in pairs {
            if let Some(token) = &cancel {
                if let Err(err) = token.check() {
                    // A prior value may already have allocated an overflow
                    // chain. Refuse a later commit rather than publishing that
                    // incomplete staging work.
                    if self.alloc.allocated_this_txn().len() > allocated_before_staging {
                        return self.fail(err);
                    }
                    return Err(err);
                }
            }
            if let Err(err) = Self::validate_key_value(key, value) {
                if self.alloc.allocated_this_txn().len() > allocated_before_staging {
                    return self.fail(err);
                }
                return Err(err);
            }
            match self.stage_value(value) {
                Ok(staged_value) => staged.push(staged_value),
                Err(err) => {
                    if self.alloc.allocated_this_txn().len() > allocated_before_staging {
                        return self.fail(err);
                    }
                    return Err(err);
                }
            }
        }
        let staged_pairs: Vec<(&[u8], ValueType, &[u8])> = pairs
            .iter()
            .zip(staged.iter())
            .map(|(&(key, _), (val_type, payload))| (key, *val_type, payload.as_ref()))
            .collect();

        let Self {
            named_trees,
            pages,
            alloc,
            manager,
            txn_id,
            failure,
            mutation_sequence,
            ..
        } = self;
        let tree = named_trees.get_mut(table).unwrap();
        let manager = *manager;
        let mut replaced_overflow = Vec::new();
        let mut skipped = Vec::new();
        let count = match tree.update_sorted_with(
            pages,
            alloc,
            *txn_id,
            &staged_pairs,
            &mut replaced_overflow,
            &mut skipped,
            || match &cancel {
                Some(token) => token.check(),
                None => Ok(()),
            },
        ) {
            Ok(count) => count,
            Err(err) => {
                return Self::fail_with(failure, err);
            }
        };
        for head in replaced_overflow {
            let mut view = WritePages { pages, manager };
            Self::free_overflow_chain_with_parts(&mut view, alloc, head, cancel.as_ref(), failure)?;
        }
        // Chains staged for pairs update_sorted did not apply (absent or
        // duplicate key) would otherwise orphan their pages.
        for i in skipped {
            if let (ValueType::Overflow, payload) = &staged[i] {
                let oref = OverflowRef::from_bytes(payload);
                let mut view = WritePages { pages, manager };
                Self::free_overflow_chain_with_parts(
                    &mut view,
                    alloc,
                    oref.first_page,
                    cancel.as_ref(),
                    failure,
                )?;
            }
        }
        if let Some(token) = &cancel {
            if let Err(err) = token.check() {
                return Self::fail_with(failure, err);
            }
        }
        Self::finish_mutation_with(
            cancel.as_ref(),
            failure,
            mutation_sequence,
            count,
            count > 0,
        )
    }

    /// Fused scan + in-place patch from `start_key`. Callback:
    /// `Some(true)`=modified, `None`=stop.
    pub fn table_update_range<F, E>(
        &mut self,
        table: &[u8],
        start_key: &[u8],
        mut f: F,
    ) -> std::result::Result<u64, E>
    where
        F: FnMut(&[u8], &mut [u8]) -> std::result::Result<Option<bool>, E>,
        E: From<Error>,
    {
        self.check_cancel()?;
        self.invalidate_fk_cache_for(table);
        self.ensure_table(table)?;
        let Self {
            named_trees,
            pages,
            alloc,
            manager,
            txn_id,
            cancel,
            read_budget,
            failure,
            mutation_sequence,
            ..
        } = self;
        let tree = named_trees.get_mut(table).unwrap();
        let root = tree.root;
        let manager = *manager;
        let txn_id = *txn_id;

        let mut scanned = ScanCount::new(manager);
        let mut view = WritePages { pages, manager };
        let mut cursor = Cursor::seek_lazy(&mut view, root, start_key)?;

        let mut count: u64 = 0;
        let mut cow_leaf = PageId::INVALID;

        // Poisoned on EVERY error, not only a cancel: this is the only scan that
        // mutates as it walks, so any error can leave a patched prefix that a
        // caller could otherwise clear the token and commit.
        let panic_guard = WriteScanPanicGuard { failure };
        let walked = (|| -> std::result::Result<u64, E> {
            while cursor.is_valid() {
                if let Some(t) = cancel.as_ref() {
                    if let Err(err) = t.check() {
                        Self::record_failure(panic_guard.failure, &err);
                        return Err(E::from(err));
                    }
                }
                scanned.rows += 1;
                let leaf_id = cursor.leaf_page_id();
                view.ensure_loaded(leaf_id)?;

                let val_type = {
                    let page = view.pages.get(&leaf_id).unwrap();
                    citadel_page::leaf_node::read_cell(page, cursor.cell_index()).val_type
                };
                if val_type == ValueType::Tombstone {
                    cursor.next_lazy(&mut view)?;
                    continue;
                }

                if cow_leaf != leaf_id {
                    let new_id = btree::cow_page(view.pages, alloc, leaf_id, txn_id);
                    if new_id != leaf_id {
                        let cell = citadel_page::leaf_node::read_cell(
                            view.pages.get(&new_id).unwrap(),
                            cursor.cell_index(),
                        );
                        let key_for_walk = cell.key.to_vec();
                        let (mut path, _) = tree.walk_to_leaf(view.pages, &key_for_walk)?;
                        let new_root =
                            btree::propagate_cow_up(view.pages, alloc, txn_id, &mut path, new_id);
                        tree.reroot_after_external_cow(new_root);
                        cursor.set_leaf_page_id(new_id);
                    }
                    cow_leaf = new_id;
                }

                if val_type == ValueType::Overflow {
                    // The ref is overwritten in place, so cell indices are
                    // unaffected.
                    let (key, oref) = {
                        let page = view.pages.get(&cow_leaf).unwrap();
                        let cell = citadel_page::leaf_node::read_cell(page, cursor.cell_index());
                        (cell.key.to_vec(), OverflowRef::from_bytes(cell.value))
                    };
                    let mut scratch = overflow_io::read_chain_value_with_budget(
                        &mut view,
                        &oref,
                        cancel.as_ref(),
                        read_budget.as_ref(),
                    )?;
                    match f(&key, &mut scratch)? {
                        Some(true) => {
                            let mut payload_digest =
                                merkle::OverflowPayloadDigest::new(scratch.len() as u32);
                            let first = overflow::write_chain_with_cancel(
                                &scratch,
                                txn_id,
                                || alloc.allocate_nonzero(),
                                |pid, page| {
                                    payload_digest.update(overflow::read_data(&page));
                                    view.pages.insert(pid, page);
                                },
                                cancel.as_ref(),
                            );
                            let first = match first {
                                Ok(first) => first,
                                Err(err) => {
                                    Self::record_failure(panic_guard.failure, &err);
                                    return Err(E::from(err));
                                }
                            };
                            let payload_digest = payload_digest.finalize();
                            view.pages
                                .get_mut(&first)
                                .expect("new overflow head is staged")
                                .set_merkle_hash(&payload_digest);
                            let new_ref = OverflowRef {
                                first_page: first,
                                total_len: scratch.len() as u32,
                            };
                            let page = view.pages.get_mut(&cow_leaf).unwrap();
                            let replaced = citadel_page::leaf_node::update_value_in_place(
                                page,
                                cursor.cell_index(),
                                ValueType::Overflow,
                                &new_ref.to_bytes(),
                            );
                            debug_assert!(replaced, "8-byte overflow ref must overwrite in place");
                            let freed = overflow_io::free_chain_with_cancel(
                                &mut view,
                                alloc,
                                oref.first_page,
                                cancel.as_ref(),
                            );
                            if let Err(err) = freed {
                                Self::record_failure(panic_guard.failure, &err);
                                return Err(E::from(err));
                            }
                            count += 1;
                        }
                        Some(false) => {}
                        None => break,
                    }
                    cursor.next_lazy(&mut view)?;
                    continue;
                }

                let page = view.pages.get_mut(&cow_leaf).unwrap();
                let ci = cursor.cell_index();
                let cell_off = page.cell_offset(ci) as usize;
                let key_len =
                    u16::from_le_bytes(page.data[cell_off..cell_off + 2].try_into().unwrap())
                        as usize;
                let val_len =
                    u32::from_le_bytes(page.data[cell_off + 2..cell_off + 6].try_into().unwrap())
                        as usize;
                let key_start = cell_off + 6;
                let val_start = cell_off + 7 + key_len;

                // Split borrow: key immutable, value mutable, non-overlapping.
                let (before_val, from_val) = page.data.split_at_mut(val_start);
                let key = &before_val[key_start..key_start + key_len];
                let value = &mut from_val[..val_len];

                if let Some(budget) = read_budget.as_ref() {
                    budget.try_charge(value.len())?;
                }

                match f(key, value)? {
                    Some(true) => count += 1,
                    Some(false) => {}
                    None => break,
                }

                cursor.next_lazy(&mut view)?;
            }
            Ok(count)
        })();
        match walked {
            Ok(count) => Self::finish_mutation_with(
                cancel.as_ref(),
                panic_guard.failure,
                mutation_sequence,
                count,
                count > 0,
            )
            .map_err(E::from),
            Err(err) => {
                Self::mark_failed_with(panic_guard.failure);
                Err(err)
            }
        }
    }

    pub fn table_delete(&mut self, table: &[u8], key: &[u8]) -> Result<bool> {
        self.check_cancel()?;
        self.invalidate_fk_cache_for(table);
        self.ensure_table(table)?;
        let Self {
            named_trees,
            pages,
            alloc,
            manager,
            txn_id,
            cancel,
            failure,
            mutation_sequence,
            ..
        } = self;
        let tree = named_trees.get_mut(table).unwrap();
        let manager = *manager;
        let txn_id = *txn_id;

        // LIL fast path: most cascade deletes hit the same leaf as the previous
        // delete. `try_lil_delete` returns Some on cache hit, None on miss.
        let lil_deleted = tree.try_lil_delete(pages, alloc, txn_id, key);
        let lil_deleted = match lil_deleted {
            Ok(result) => result,
            Err(err) => return Self::fail_with(failure, err),
        };
        if let Some((deleted, overflow_head)) = lil_deleted {
            if let Some(head) = overflow_head {
                let mut view = WritePages { pages, manager };
                Self::free_overflow_chain_with_parts(
                    &mut view,
                    alloc,
                    head,
                    cancel.as_ref(),
                    failure,
                )?;
            }
            return Self::finish_mutation_with(
                cancel.as_ref(),
                failure,
                mutation_sequence,
                deleted,
                deleted,
            );
        }

        // Slow path: walk + delete.
        let root = tree.root;
        let deleted = PATH_BUF.with(|pb| -> Result<_> {
            let mut path = pb.borrow_mut();
            path.clear();
            let leaf_id = Self::walk_loading_into(pages, manager, root, key, &mut path)?;
            tree.delete_at_leaf_with_overflow(pages, alloc, txn_id, key, &mut path, leaf_id)
        });
        let (deleted, overflow_head) = match deleted {
            Ok(result) => result,
            Err(err) => return Self::fail_with(failure, err),
        };

        if let Some(head) = overflow_head {
            let mut view = WritePages { pages, manager };
            Self::free_overflow_chain_with_parts(&mut view, alloc, head, cancel.as_ref(), failure)?;
        }
        Self::finish_mutation_with(
            cancel.as_ref(),
            failure,
            mutation_sequence,
            deleted,
            deleted,
        )
    }

    /// Drop all pages, reset to an empty leaf. Returns pre-truncation entry
    /// count.
    pub fn table_truncate(&mut self, table: &[u8]) -> Result<u64> {
        self.check_cancel()?;
        self.invalidate_fk_cache_for(table);
        self.ensure_table(table)?;

        let old_tree = self.named_trees[table].clone();
        self.free_tree_pages(old_tree.root)?;

        let new_root = self.alloc.allocate();
        let mut leaf = Page::new(new_root, PageType::Leaf, self.txn_id);
        leaf.update_checksum();
        self.pages.insert(new_root, leaf);

        self.named_trees
            .insert(table.to_vec(), BTree::from_existing(new_root, 1, 0));
        self.finish_mutation(old_tree.entry_count, true)
    }

    pub fn table_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_cancel()?;
        self.ensure_table(table)?;
        let root = self.named_trees[table].root;
        let leaf_id = Self::descend_to_leaf(&mut self.pages, self.manager, root, key)?;
        let found = BTree::search_at_leaf(&self.pages, leaf_id, key)?;
        let value = self.materialize_value(found)?;
        self.check_cancel()?;
        Ok(value)
    }

    pub fn commit(self) -> Result<()> {
        self.commit_with_generation().map(|_| ())
    }

    /// Commit and return the exact manager generation produced while writer
    /// exclusion is still held. Sync uses this to distinguish a no-op commit
    /// from a durable commit without racing a later writer.
    #[doc(hidden)]
    pub fn commit_with_generation(mut self) -> Result<u64> {
        if let Some(failure) = self.failure {
            return Err(failure.error());
        }
        // The only check a statement that never enters a scan loop gets: a
        // VALUES insert or a point update reaches here directly.
        if let Some(token) = &self.cancel {
            token.check()?;
        }
        let (catalog_root, catalog_refreshed) = self.finalize_catalog()?;
        // Last point before durable commit: after commit_write starts a cancel
        // races with completion, and reporting Interrupted on success is a lie.
        if let Some(token) = &self.cancel {
            token.check()?;
        }
        let generation = self.manager.commit_write(
            self.base_txn_id,
            self.txn_id,
            &mut self.pages,
            &mut self.alloc,
            &self.tree,
            &self.old_slot,
            catalog_root,
            &self.named_trees,
            &self.loaded_tree_meta,
            &catalog_refreshed,
            self.force_commit,
        )?;
        self.committed = true;
        Ok(generation)
    }

    pub fn abort(mut self) {
        self.committed = true;
        self.manager.abort_write();
    }

    /// SAVEPOINT: snapshot state and advance txn_id.
    pub fn begin_savepoint(&mut self) -> WriteTxnSnapshot {
        let snap = self.capture_snapshot();
        self.txn_id = self.manager.next_write_txn_id();
        snap
    }

    /// ROLLBACK TO SAVEPOINT: restore state and drop post-savepoint pages.
    pub fn restore_snapshot(&mut self, snap: WriteTxnSnapshot) {
        let pre_savepoint_alloc_len = snap.alloc_checkpoint.allocated_this_txn_len();
        for &page_id in self.alloc.allocated_since(pre_savepoint_alloc_len) {
            self.pages.remove(&page_id);
        }
        self.tree = snap.tree;
        self.alloc.restore(snap.alloc_checkpoint);
        self.named_trees = snap.named_trees;
        self.catalog = snap.catalog;
        self.catalog_dirty = snap.catalog_dirty;
        self.loaded_tree_meta = snap.loaded_tree_meta;
        self.failure = snap.failure;
        self.mutation_sequence = snap.mutation_sequence;
        self.deferred_fk_checks
            .truncate(snap.deferred_fk_checks_len);
        self.fk_check_cache.clear();
        self.txn_id = self.manager.next_write_txn_id();
    }

    fn capture_snapshot(&self) -> WriteTxnSnapshot {
        WriteTxnSnapshot {
            tree: self.tree.clone(),
            alloc_checkpoint: self.alloc.checkpoint(),
            named_trees: self.named_trees.clone(),
            catalog: self.catalog.clone(),
            catalog_dirty: self.catalog_dirty,
            loaded_tree_meta: self.loaded_tree_meta.clone(),
            deferred_fk_checks_len: self.deferred_fk_checks.len(),
            failure: self.failure,
            mutation_sequence: self.mutation_sequence,
        }
    }

    pub fn base_txn_id(&self) -> TxnId {
        self.base_txn_id
    }

    fn validate_key_value(key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() > MAX_KEY_SIZE {
            return Err(Error::KeyTooLarge {
                size: key.len(),
                max: MAX_KEY_SIZE,
            });
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::ValueTooLarge {
                size: value.len(),
                max: MAX_VALUE_SIZE,
            });
        }
        Ok(())
    }

    /// Stage a value: borrow inline (the tree copies it anyway), own the
    /// overflow ref.
    fn stage_value<'v>(&mut self, value: &'v [u8]) -> Result<(ValueType, Cow<'v, [u8]>)> {
        if value.len() <= MAX_INLINE_VALUE_SIZE {
            return Ok((ValueType::Inline, Cow::Borrowed(value)));
        }
        let allocated_before = self.alloc.allocated_this_txn().len();
        let Self {
            txn_id,
            pages,
            alloc,
            cancel,
            failure,
            ..
        } = self;
        let txn_id = *txn_id;
        let mut payload_digest = merkle::OverflowPayloadDigest::new(value.len() as u32);
        let staged = overflow::write_chain_with_cancel(
            value,
            txn_id,
            || alloc.allocate_nonzero(),
            |pid, page| {
                payload_digest.update(overflow::read_data(&page));
                pages.insert(pid, page);
            },
            cancel.as_ref(),
        );
        let first = match staged {
            Ok(first) => first,
            Err(err) => {
                if alloc.allocated_this_txn().len() > allocated_before {
                    Self::record_failure(failure, &err);
                }
                return Err(err);
            }
        };
        let payload_digest = payload_digest.finalize();
        pages
            .get_mut(&first)
            .expect("new overflow head is staged")
            .set_merkle_hash(&payload_digest);
        let oref = OverflowRef {
            first_page: first,
            total_len: value.len() as u32,
        };
        Ok((ValueType::Overflow, Cow::Owned(oref.to_bytes().to_vec())))
    }

    /// Reject a name the commit slot's 32-bit named-table cache cannot hold
    /// unambiguously. DDL is rare, so scan the catalog rather than add lookup
    /// cost or mutable global state to every ordinary table access.
    fn reject_table_name_hash_collision(
        &mut self,
        requested: &[u8],
        excluded: Option<&[u8]>,
    ) -> Result<()> {
        use citadel_io::file_manager::table_name_hash;

        let requested_hash = table_name_hash(requested);
        for existing in self.named_trees.keys() {
            self.check_cancel()?;
            if existing.as_slice() != requested
                && excluded != Some(existing.as_slice())
                && table_name_hash(existing) == requested_hash
            {
                return Err(Error::NamedTableHashCollision {
                    requested: String::from_utf8_lossy(requested).into_owned(),
                    existing: String::from_utf8_lossy(existing).into_owned(),
                    hash: requested_hash,
                });
            }
        }

        self.ensure_catalog()?;
        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_all_pages(catalog_root)?;
        let mut cursor = Cursor::first(&self.pages, catalog_root)?;
        while cursor.is_valid() {
            self.check_cancel()?;
            if let Some(cell) = cursor.current_ref(&self.pages) {
                let existing = cell.key;
                if cell.val_type != ValueType::Tombstone
                    && existing != requested
                    && excluded != Some(existing)
                    && table_name_hash(existing) == requested_hash
                {
                    return Err(Error::NamedTableHashCollision {
                        requested: String::from_utf8_lossy(requested).into_owned(),
                        existing: String::from_utf8_lossy(existing).into_owned(),
                        hash: requested_hash,
                    });
                }
            }
            cursor.next(&self.pages)?;
        }
        self.check_cancel()
    }

    fn ensure_catalog(&mut self) -> Result<()> {
        if self.catalog.is_some() {
            return Ok(());
        }

        if self.old_slot.catalog_root.is_valid() {
            self.preload_path(self.old_slot.catalog_root, &[])?;
            let slot = self.catalog_slot_from_disk()?;
            self.catalog = Some(BTree::from_existing(
                slot.root_page,
                slot.depth,
                slot.entry_count,
            ));
        } else {
            let page_id = self.alloc.allocate();
            let mut leaf = Page::new(page_id, PageType::Leaf, self.txn_id);
            leaf.update_checksum();
            self.pages.insert(page_id, leaf);
            self.catalog = Some(BTree::from_existing(page_id, 1, 0));
            self.catalog_dirty = true;
        }
        Ok(())
    }

    fn catalog_slot_from_disk(&mut self) -> Result<TableDescriptor> {
        let root = self.old_slot.catalog_root;
        let mut depth: u16 = 1;
        let mut current = root;
        loop {
            self.check_cancel()?;
            if !self.pages.contains_key(&current) {
                let page = self.manager.fetch_page_owned(current)?;
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

    fn ensure_table(&mut self, name: &[u8]) -> Result<()> {
        self.check_cancel()?;
        if self.named_trees.contains_key(name) {
            return Ok(());
        }

        // Prove the exact name exists before consulting the commit slot. Slot
        // entries carry only a 32-bit name hash, so looking there first can
        // turn a nonexistent colliding name into an alias for a live table.
        self.ensure_catalog()?;
        let catalog_root = self.catalog.as_ref().unwrap().root;
        self.preload_path(catalog_root, name)?;

        let mut desc = match self.catalog.as_ref().unwrap().search(&self.pages, name)? {
            Some((ValueType::Tombstone, _)) | None => {
                return Err(Error::TableNotFound(
                    String::from_utf8_lossy(name).into_owned(),
                ));
            }
            Some((ValueType::Inline, desc_bytes)) => {
                TableDescriptor::try_deserialize(&desc_bytes).ok_or(Error::DatabaseCorrupted)?
            }
            Some(_) => return Err(Error::DatabaseCorrupted),
        };

        // Even when this snapshot has no slot entry yet, loading a table from
        // an already-collided catalog and committing it would create one. Keep
        // legacy collisions read-only until an explicit repair path exists.
        self.manager
            .reject_named_table_hash_collision(name, self.cancel.as_ref())?;

        // In SyncMode::Off the slot may be the only durable record of the
        // table's current root/count while the exact catalog descriptor lags.
        if let Some((root, depth)) = self.old_slot.named_entry_root(name) {
            let Some(entry_count) = self.old_slot.named_entry_count(name) else {
                return Err(Error::DatabaseCorrupted);
            };
            desc.root_page = root;
            desc.depth = depth;
            desc.entry_count = entry_count;
        } else if let Some(entry_count) = self.old_slot.named_entry_count(name) {
            desc.entry_count = entry_count;
        }

        let tree = BTree::from_existing(desc.root_page, desc.depth, desc.entry_count);
        self.loaded_tree_meta
            .insert(name.to_vec(), (desc.root_page, desc.depth));
        self.named_trees.insert(name.to_vec(), tree);
        self.check_cancel()
    }

    /// Returns the new catalog root and the hashes whose descriptors were
    /// written this commit; the manager marks every other moved/stale table's
    /// slot entry SLOT_ENTRY_STALE so it survives serialize.
    fn finalize_catalog(&mut self) -> Result<(PageId, FxHashSet<u32>)> {
        use citadel_io::file_manager::table_name_hash;

        self.check_cancel()?;
        if !self.catalog_dirty && self.named_trees.is_empty() {
            return Ok((self.old_slot.catalog_root, FxHashSet::default()));
        }

        // SyncMode::Off: skip catalog update if only roots changed (cached in
        // slot)
        if !self.catalog_dirty && self.manager.sync_mode() == citadel_core::types::SyncMode::Off {
            let mut needs_catalog = false;
            for (name, tree) in &self.named_trees {
                self.check_cancel()?;
                let structurally_changed = match self.loaded_tree_meta.get(name.as_slice()) {
                    Some(&(_, old_depth)) => tree.depth != old_depth,
                    None => true, // new table
                };
                if structurally_changed {
                    needs_catalog = true;
                    break;
                }
            }
            // The skip makes the slot the moved roots' only record, so it is
            // legal only when the touched trees plus carried stale entries fit
            // the V1 capacity (else a dropped entry's stale descriptor wins on
            // reopen).
            if !needs_catalog && self.slot_entries_fit()? {
                return Ok((self.old_slot.catalog_root, FxHashSet::default()));
            }
        }

        if self.catalog.is_none() {
            self.ensure_catalog()?;
        }

        let mut structural_entries: Vec<(Vec<u8>, [u8; 20])> = Vec::new();
        for (name, tree) in &self.named_trees {
            self.check_cancel()?;
            let structurally_changed = match self.loaded_tree_meta.get(name.as_slice()) {
                Some(&(old_root, old_depth)) => tree.root != old_root || tree.depth != old_depth,
                None => true,
            };
            if structurally_changed {
                let desc = TableDescriptor::from_tree(tree);
                structural_entries.push((name.clone(), desc.serialize()));
            }
        }

        if structural_entries.is_empty() {
            return Ok((self.catalog.as_ref().unwrap().root, FxHashSet::default()));
        }

        for (name, value) in &structural_entries {
            self.check_cancel()?;
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
        self.check_cancel()?;

        let mut refreshed = FxHashSet::default();
        for (name, _) in &structural_entries {
            self.check_cancel()?;
            refreshed.insert(table_name_hash(name));
        }
        Ok((self.catalog.as_ref().unwrap().root, refreshed))
    }

    /// Whether the Off-mode catalog skip is legal: touched trees plus carried
    /// stale entries must fit the V1 capacity (stale entries are never dropped
    /// by serialize). Counting all touched trees as stale over-approximates,
    /// so it can only refuse a skip early, never admit an unsafe one.
    fn slot_entries_fit(&self) -> Result<bool> {
        use citadel_io::file_manager::table_name_hash;
        let mut known: rustc_hash::FxHashSet<u32> = FxHashSet::default();
        for name in self.named_trees.keys().chain(self.loaded_tree_meta.keys()) {
            self.check_cancel()?;
            known.insert(table_name_hash(name));
        }
        let mut carried_stale = 0usize;
        for &(hash, ..) in &self.old_slot.named_table_entries {
            self.check_cancel()?;
            if !known.contains(&hash) && self.old_slot.entry_is_stale(hash) {
                carried_stale += 1;
            }
        }
        Ok(self.named_trees.len() + carried_stale <= citadel_core::SLOT_NAMED_MAX_ENTRIES_V1)
    }

    fn free_tree_pages(&mut self, root: PageId) -> Result<()> {
        let mut stack = vec![root];
        let mut overflow_heads: Vec<PageId> = Vec::new();
        let mut tree_pages = Vec::new();
        let mut tree_seen = FxHashSet::default();

        // Preflight the complete tree before changing allocator state. A cold
        // read or malformed later page must leave the table fully committable.
        while let Some(current) = stack.pop() {
            self.check_cancel()?;
            if !tree_seen.insert(current) {
                return Err(Error::DatabaseCorrupted);
            }
            tree_pages.push(current);
            if !self.pages.contains_key(&current) {
                let page = self.manager.fetch_page_owned(current)?;
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
                    for i in 0..page.num_cells() {
                        let cell = citadel_page::leaf_node::read_cell(page, i);
                        if cell.val_type == ValueType::Overflow {
                            let oref = OverflowRef::from_bytes(cell.value);
                            overflow_heads.push(oref.first_page);
                        }
                    }
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }

        // Overflow pages are not in the B-tree walk. Validate and load every
        // chain before freeing either kind of page, and reject shared/cyclic
        // references rather than adding the same page to pending-free twice.
        let mut overflow_pages = Vec::new();
        let mut overflow_seen = FxHashSet::default();
        for head in overflow_heads {
            self.check_cancel()?;
            let chain = {
                let mut view = WritePages {
                    pages: &mut self.pages,
                    manager: self.manager,
                };
                overflow_io::collect_chain_pages_with_cancel(&mut view, head, self.cancel.as_ref())?
            };
            for page_id in chain {
                if tree_seen.contains(&page_id) || !overflow_seen.insert(page_id) {
                    return Err(Error::CorruptOverflowChain(format!(
                        "page {page_id} is referenced more than once"
                    )));
                }
                overflow_pages.push(page_id);
            }
        }

        let cancel = self.cancel.clone();
        let Self {
            alloc,
            failure,
            mutation_sequence,
            ..
        } = self;
        let mut freed_any = false;
        for page_id in tree_pages.into_iter().chain(overflow_pages) {
            if let Some(token) = &cancel {
                #[cfg(test)]
                CANCEL_ON_NTH_TREE_FREE.with(|remaining| {
                    if let Some(frees) = remaining.get() {
                        if frees == 1 {
                            remaining.set(None);
                            token.cancel();
                        } else {
                            remaining.set(Some(frees - 1));
                        }
                    }
                });
                if let Err(err) = token.check() {
                    if freed_any {
                        Self::record_failure(failure, &err);
                    }
                    return Err(err);
                }
            }
            alloc.free(page_id);
            freed_any = true;
        }
        Self::finish_mutation_with(cancel.as_ref(), failure, mutation_sequence, (), freed_any)
    }

    fn count_leaf_entries(&mut self, root: PageId) -> Result<u64> {
        let mut count: u64 = 0;
        let mut stack = vec![root];
        while let Some(current) = stack.pop() {
            self.check_cancel()?;
            if !self.pages.contains_key(&current) {
                let page = self.manager.fetch_page_owned(current)?;
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
        self.check_cancel()?;
        Ok(count)
    }

    fn preload_path(&mut self, root: PageId, key: &[u8]) -> Result<()> {
        Self::descend_to_leaf(&mut self.pages, self.manager, root, key).map(|_| ())
    }

    fn descend_to_leaf(
        pages: &mut FxHashMap<PageId, Page>,
        manager: &TxnManager,
        root: PageId,
        key: &[u8],
    ) -> Result<PageId> {
        let mut current = root;
        loop {
            let page = match pages.entry(current) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let page = manager.fetch_page_owned(current)?;
                    e.insert(page)
                }
            };
            match page.page_type() {
                Some(PageType::Leaf) => return Ok(current),
                Some(PageType::Branch) => {
                    let idx = branch_node::search_child_index(page, key);
                    current = branch_node::get_child(page, idx);
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }
    }

    fn walk_loading(
        pages: &mut FxHashMap<PageId, Page>,
        manager: &TxnManager,
        root: PageId,
        key: &[u8],
    ) -> Result<(Vec<(PageId, usize)>, PageId)> {
        let mut path = Vec::new();
        let leaf_id = Self::walk_loading_into(pages, manager, root, key, &mut path)?;
        Ok((path, leaf_id))
    }

    fn walk_loading_into(
        pages: &mut FxHashMap<PageId, Page>,
        manager: &TxnManager,
        root: PageId,
        key: &[u8],
        path: &mut Vec<(PageId, usize)>,
    ) -> Result<PageId> {
        path.clear();
        let mut current = root;
        loop {
            let page = match pages.entry(current) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let page = manager.fetch_page_owned(current)?;
                    e.insert(page)
                }
            };
            match page.page_type() {
                Some(PageType::Leaf) => return Ok(current),
                Some(PageType::Branch) => {
                    let idx = branch_node::search_child_index(page, key);
                    let child = branch_node::get_child(page, idx);
                    path.push((current, idx));
                    current = child;
                }
                _ => return Err(Error::InvalidPageType(page.page_type_raw(), current)),
            }
        }
    }

    fn preload_all_pages(&mut self, root: PageId) -> Result<()> {
        let mut stack = vec![root];
        while let Some(current) = stack.pop() {
            // Completes before `for_each` yields anything; see the read side.
            if let Some(t) = self.cancel.as_ref() {
                t.check()?;
            }
            if !self.pages.contains_key(&current) {
                let page = self.manager.fetch_page_owned(current)?;
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

impl<'db> Drop for WriteTxn<'db> {
    fn drop(&mut self) {
        if !self.committed {
            self.manager.abort_write();
        }
    }
}

/// Scan adapter wrapping a `&mut WriteTxn` for use with [`crate::TableIter`].
pub struct WriteTxnScanAdapter<'a, 'db: 'a> {
    txn: &'a mut WriteTxn<'db>,
    measurements: Vec<Arc<AtomicU64>>,
}

impl<'a, 'db: 'a> crate::scan_iter::TxnScanAdapter for WriteTxnScanAdapter<'a, 'db> {
    fn with_loader<R>(&mut self, f: &mut dyn FnMut(&mut dyn PageLoader) -> Result<R>) -> Result<R> {
        let mut view = WritePages {
            pages: &mut self.txn.pages,
            manager: self.txn.manager,
        };
        f(&mut view)
    }

    fn cancel(&self) -> Option<&CancelToken> {
        self.txn.cancel.as_ref()
    }

    fn read_budget(&self) -> Option<&ReadBudget> {
        self.txn.read_budget.as_ref()
    }

    fn record_rows_scanned(&self, rows: u64) {
        self.txn
            .manager
            .add_rows_scanned_to(rows, &self.measurements);
    }
}

#[cfg(test)]
#[path = "write_txn_tests.rs"]
mod tests;
