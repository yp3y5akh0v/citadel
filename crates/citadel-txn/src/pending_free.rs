//! Durable retirement records for data pages and pending-free chain pages.
//!
//! Format: linked list of PendingFree pages on disk.
//! Each page contains an array of PendingFreeEntry structs.
//! Chain head stored in CommitSlot.pending_free_root.

use citadel_buffer::allocator::{PageAllocator, ReadyPages};
use citadel_buffer::cursor::MutablePageMap;
use citadel_core::types::{PageId, PageType, TxnId};
use citadel_core::{
    Error, Result, PAGE_HEADER_SIZE, PENDING_FREE_ENTRIES_PER_PAGE, PENDING_FREE_ENTRY_SIZE,
};
use citadel_page::page::Page;
use rustc_hash::{FxHashMap, FxHashSet};
use std::ops::Deref;

/// A pending-free entry: a page that was freed at a specific transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingFreeEntry {
    pub page_id: PageId,
    pub freed_at_txn: TxnId,
}

/// Maximum entries per pending-free page.
/// Body layout: [entry_count: u32 (4B)] [entries: 12B each] [padding]
pub(crate) const MAX_ENTRIES_PER_PAGE: usize = PENDING_FREE_ENTRIES_PER_PAGE;

/// Decode one pending-free page without trusting its entry count.
pub(crate) fn read_page_entries(
    page: &Page,
) -> Result<impl ExactSizeIterator<Item = PendingFreeEntry> + '_> {
    if page.page_type() != Some(PageType::PendingFree) {
        return Err(Error::InvalidPageType(page.page_type_raw(), page.page_id()));
    }
    let entry_count = read_entry_count(page);
    if entry_count > MAX_ENTRIES_PER_PAGE {
        return Err(Error::DatabaseCorrupted);
    }

    let data_start = PAGE_HEADER_SIZE + 4;
    Ok((0..entry_count)
        .map(move |index| read_entry_at(&page.data, data_start + index * PENDING_FREE_ENTRY_SIZE)))
}

/// Read all entries from the pending-free chain stored in the page map.
pub fn read_chain(pages: &FxHashMap<PageId, Page>, root: PageId) -> Result<Vec<PendingFreeEntry>> {
    Ok(ChainSnapshot::read(root, |id| pages.get(&id).ok_or(Error::PageOutOfBounds(id)))?.entries)
}

/// Validated entries and structure used for reclamation and CoW updates.
pub(crate) struct ChainSnapshot {
    entries: Vec<PendingFreeEntry>,
    page_ids: Vec<PageId>,
    page_lengths: Vec<usize>,
    max_txn: TxnId,
    #[cfg(test)]
    entry_indices: FxHashMap<PageId, usize>,
}

impl ChainSnapshot {
    pub(crate) fn read<P: Deref<Target = Page>>(
        root: PageId,
        load: impl FnMut(PageId) -> Result<P>,
    ) -> Result<Self> {
        Self::read_checked(root, 0, load, |_| Ok(()))
    }

    pub(crate) fn read_committed<P: Deref<Target = Page>>(
        root: PageId,
        high_water_mark: u32,
        slot_txn: TxnId,
        capacity_hint: usize,
        load: impl FnMut(PageId) -> Result<P>,
    ) -> Result<Self> {
        Self::read_checked(root, capacity_hint, load, |entry| {
            if !entry.page_id.is_valid()
                || entry.page_id.as_u32() >= high_water_mark
                || entry.freed_at_txn == TxnId::ZERO
                || entry.freed_at_txn > slot_txn
            {
                return Err(Error::DatabaseCorrupted);
            }
            Ok(())
        })
    }

    fn read_checked<P: Deref<Target = Page>>(
        root: PageId,
        capacity_hint: usize,
        mut load: impl FnMut(PageId) -> Result<P>,
        mut check_entry: impl FnMut(PendingFreeEntry) -> Result<()>,
    ) -> Result<Self> {
        // The caller's existing loan count can avoid repeated allocation and
        // rehashing. Bound speculative reservation; this is neither a trusted
        // entry count nor a limit on the chain we must read and validate.
        let capacity = if root.is_valid() {
            capacity_hint.min(64 * 1024)
        } else {
            0
        };
        let mut entries = Vec::with_capacity(capacity);
        let mut page_ids = Vec::new();
        let mut page_lengths = Vec::new();
        let mut max_txn = TxnId::ZERO;
        let mut entry_indices = FxHashMap::with_capacity_and_hasher(capacity, Default::default());
        let mut seen = FxHashSet::default();
        let mut current = root;

        while current.is_valid() {
            if !seen.insert(current) {
                return Err(Error::DatabaseCorrupted);
            }
            let page = load(current)?;
            if page.page_id() != current {
                return Err(Error::DatabaseCorrupted);
            }
            max_txn = max_txn.max(page.txn_id());
            page_ids.push(current);
            let page_entries = read_page_entries(&page)?;
            page_lengths.push(page_entries.len());
            for entry in page_entries {
                check_entry(entry)?;
                max_txn = max_txn.max(entry.freed_at_txn);
                if entry_indices.insert(entry.page_id, entries.len()).is_some() {
                    return Err(Error::DatabaseCorrupted);
                }
                entries.push(entry);
            }
            current = page.right_child();
        }

        if page_ids.iter().any(|id| entry_indices.contains_key(id)) {
            return Err(Error::DatabaseCorrupted);
        }
        Ok(Self {
            entries,
            page_ids,
            page_lengths,
            max_txn,
            #[cfg(test)]
            entry_indices,
        })
    }
}

/// Number of chain pages needed to hold `entry_count` entries.
fn chain_pages_needed(entry_count: usize) -> usize {
    entry_count.div_ceil(MAX_ENTRIES_PER_PAGE)
}

/// Write a new pending-free chain into the page map using the given
/// pre-allocated structure pages (never reuses old chain pages).
/// Returns the root PageId of the new chain (PageId::INVALID if empty).
pub fn write_chain(
    pages: &mut impl MutablePageMap,
    txn_id: TxnId,
    entries: &[PendingFreeEntry],
    page_ids: &[PageId],
) -> PageId {
    if entries.is_empty() {
        return PageId::INVALID;
    }

    let num_pages = chain_pages_needed(entries.len());
    debug_assert_eq!(num_pages, page_ids.len());

    for (i, chunk) in entries.chunks(MAX_ENTRIES_PER_PAGE).enumerate() {
        let next = page_ids.get(i + 1).copied().unwrap_or(PageId::INVALID);
        write_chain_page(pages, txn_id, page_ids[i], next, chunk);
    }

    page_ids[0]
}

fn write_chain_page(
    pages: &mut impl MutablePageMap,
    txn_id: TxnId,
    page_id: PageId,
    next: PageId,
    entries: &[PendingFreeEntry],
) {
    let mut page = build_chain_page(txn_id, page_id, next, entries);
    page.update_checksum();
    pages.insert_page(page_id, page);
}

/// Build a writer-private page. Its publication boundary seals the checksum.
fn build_chain_page(
    txn_id: TxnId,
    page_id: PageId,
    next: PageId,
    entries: &[PendingFreeEntry],
) -> Page {
    let mut page = Page::new_for_write(page_id, PageType::PendingFree, txn_id);
    page.set_right_child(next);
    page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
        .copy_from_slice(&(entries.len() as u32).to_le_bytes());
    for (index, entry) in entries.iter().enumerate() {
        let offset = PAGE_HEADER_SIZE + 4 + index * PENDING_FREE_ENTRY_SIZE;
        write_entry_at(&mut page.data, offset, entry);
    }
    page
}

/// Collect all page IDs that form the chain (for deferred freeing after write).
pub fn collect_chain_page_ids(
    pages: &FxHashMap<PageId, Page>,
    root: PageId,
) -> Result<Vec<PageId>> {
    if !root.is_valid() {
        return Ok(Vec::new());
    }

    let mut ids = Vec::new();
    let mut current = root;
    let mut seen = FxHashSet::default();

    while current.is_valid() {
        if !seen.insert(current) {
            return Err(Error::DatabaseCorrupted);
        }
        ids.push(current);
        let page = pages.get(&current).ok_or(Error::PageOutOfBounds(current))?;
        if page.page_id() != current {
            return Err(Error::DatabaseCorrupted);
        }
        let _ = read_page_entries(page)?;
        current = page.right_child();
        if !current.is_valid() {
            break;
        }
    }

    Ok(ids)
}

/// Commit-time inputs to [`process_chain`]. `loan_pool` must be the
/// allocator's drained ready_to_use remainder: those pages were freed >= 2
/// commits back, so overwriting them now cannot damage either recovery slot.
pub struct ChainCommit<'a> {
    pub txn_id: TxnId,
    pub current_root: PageId,
    pub freed_this_txn: &'a [PageId],
    pub consumed: &'a FxHashSet<PageId>,
    pub reclaim_horizon: TxnId,
}

/// Remove consumed entries and record new frees. Without consumption or loans,
/// share the unchanged tail and pack new entries at the head. Head-local
/// consumption can replace just the head; otherwise rewrite the chain using
/// loan pages first. Replaced structure pages remain pending;
/// entries leave the durable chain only when a commit records their consumption.
///
/// Returns `(new_chain_root, available_entries)`; entries carry freed_at_txn
/// so the caller can zero each page once for secure delete.
pub fn process_chain(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    loan_pool: &mut Vec<PageId>,
    commit: &ChainCommit<'_>,
) -> Result<(PageId, Vec<PendingFreeEntry>)> {
    ChainSnapshot::read_committed(
        commit.current_root,
        alloc.high_water_mark(),
        commit.txn_id,
        0,
        |id| pages.get(&id).ok_or(Error::PageOutOfBounds(id)),
    )?
    .process(pages, alloc, loan_pool, commit)
}

impl ChainSnapshot {
    pub(crate) fn process(
        self,
        pages: &mut impl MutablePageMap,
        alloc: &mut PageAllocator,
        loan_pool: &mut Vec<PageId>,
        commit: &ChainCommit<'_>,
    ) -> Result<(PageId, Vec<PendingFreeEntry>)> {
        // The public helper receives pages rather than a commit-slot stamp.
        // Use their explicit maximum generation, never txn_id subtraction.
        let prior_txn = self.max_txn;
        let state = CommittedReclaim::from_snapshot(
            self,
            alloc.high_water_mark(),
            prior_txn,
            &FxHashMap::default(),
        );
        let mut loans = ReadyPages::from_pop_order(loan_pool.iter().rev().copied().collect());
        let result = state.prepare(pages, alloc, &mut loans, commit);
        loan_pool.clear();
        while let Some(id) = loans.pop() {
            loan_pool.push(id);
        }
        loan_pool.reverse();
        let prepared = result?;
        prepared.seal_staged_pages(pages);
        let root = prepared.root();
        let available = state.available_after(&prepared, commit.reclaim_horizon);
        Ok((root, available))
    }
}

mod committed;
pub(crate) use committed::CommittedReclaim;
#[cfg(test)]
pub(crate) use committed::ReclaimWork;

fn read_entry_count(page: &Page) -> usize {
    u32::from_le_bytes(
        page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
            .try_into()
            .unwrap(),
    ) as usize
}

fn read_entry_at(data: &[u8], offset: usize) -> PendingFreeEntry {
    PendingFreeEntry {
        page_id: PageId(u32::from_le_bytes(
            data[offset..offset + 4].try_into().unwrap(),
        )),
        freed_at_txn: TxnId(u64::from_le_bytes(
            data[offset + 4..offset + 12].try_into().unwrap(),
        )),
    }
}

fn write_entry_at(data: &mut [u8], offset: usize, entry: &PendingFreeEntry) {
    data[offset..offset + 4].copy_from_slice(&entry.page_id.as_u32().to_le_bytes());
    data[offset + 4..offset + 12].copy_from_slice(&entry.freed_at_txn.as_u64().to_le_bytes());
}

#[cfg(test)]
#[path = "pending_free_tests.rs"]
mod tests;
