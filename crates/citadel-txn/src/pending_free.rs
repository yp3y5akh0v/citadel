//! Durable retirement records for data pages and pending-free chain pages.
//!
//! Format: linked list of PendingFree pages on disk.
//! Each page contains an array of PendingFreeEntry structs.
//! Chain head stored in CommitSlot.pending_free_root.

use citadel_buffer::allocator::PageAllocator;
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
    head_entry_count: usize,
    entry_indices: FxHashMap<PageId, usize>,
}

impl ChainSnapshot {
    pub(crate) fn read<P: Deref<Target = Page>>(
        root: PageId,
        load: impl FnMut(PageId) -> Result<P>,
    ) -> Result<Self> {
        Self::read_checked(root, load, |_| Ok(()))
    }

    pub(crate) fn read_committed<P: Deref<Target = Page>>(
        root: PageId,
        high_water_mark: u32,
        slot_txn: TxnId,
        load: impl FnMut(PageId) -> Result<P>,
    ) -> Result<Self> {
        Self::read_checked(root, load, |entry| {
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
        mut load: impl FnMut(PageId) -> Result<P>,
        mut check_entry: impl FnMut(PendingFreeEntry) -> Result<()>,
    ) -> Result<Self> {
        let mut entries = Vec::new();
        let mut page_ids = Vec::new();
        let mut head_entry_count = 0;
        let mut entry_indices = FxHashMap::default();
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
            page_ids.push(current);
            let page_entries = read_page_entries(&page)?;
            if page_ids.len() == 1 {
                head_entry_count = page_entries.len();
            }
            for entry in page_entries {
                check_entry(entry)?;
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
            head_entry_count,
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
    pages: &mut FxHashMap<PageId, Page>,
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
    pages: &mut FxHashMap<PageId, Page>,
    txn_id: TxnId,
    page_id: PageId,
    next: PageId,
    entries: &[PendingFreeEntry],
) {
    let mut page = Page::new(page_id, PageType::PendingFree, txn_id);
    page.set_right_child(next);
    page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
        .copy_from_slice(&(entries.len() as u32).to_le_bytes());
    for (index, entry) in entries.iter().enumerate() {
        let offset = PAGE_HEADER_SIZE + 4 + index * PENDING_FREE_ENTRY_SIZE;
        write_entry_at(&mut page.data, offset, entry);
    }
    page.update_checksum();
    pages.insert(page_id, page);
}

fn prepend_chain(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    entries: &[PendingFreeEntry],
    mut tail: PageId,
) -> PageId {
    // Keep spare capacity at the head, so later appends replace at most one page.
    for chunk in entries.rchunks(MAX_ENTRIES_PER_PAGE) {
        let page_id = alloc.allocate();
        write_chain_page(pages, txn_id, page_id, tail, chunk);
        tail = page_id;
    }
    tail
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
/// share the unchanged tail and pack new entries at the head. Otherwise rewrite
/// the chain using loan pages first. Replaced structure pages remain pending;
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
    ChainSnapshot::read(commit.current_root, |id| {
        pages.get(&id).ok_or(Error::PageOutOfBounds(id))
    })?
    .process(pages, alloc, loan_pool, commit)
}

impl ChainSnapshot {
    pub(crate) fn process(
        self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        loan_pool: &mut Vec<PageId>,
        commit: &ChainCommit<'_>,
    ) -> Result<(PageId, Vec<PendingFreeEntry>)> {
        self.process_with_metadata(pages, alloc, loan_pool, commit, &mut FxHashMap::default())
    }

    pub(crate) fn process_with_metadata(
        self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        loan_pool: &mut Vec<PageId>,
        commit: &ChainCommit<'_>,
        retired_chain_pages: &mut FxHashMap<PageId, TxnId>,
    ) -> Result<(PageId, Vec<PendingFreeEntry>)> {
        if self.page_ids.first().copied().unwrap_or(PageId::INVALID) != commit.current_root
            || alloc.ready_count() != 0
        {
            return Err(Error::DatabaseCorrupted);
        }
        if commit.consumed.is_empty() && loan_pool.is_empty() {
            return Ok(self.prepend_frees(pages, alloc, commit, retired_chain_pages));
        }
        let Self {
            mut entries,
            page_ids,
            mut entry_indices,
            ..
        } = self;
        let ChainCommit {
            txn_id,
            freed_this_txn,
            consumed,
            reclaim_horizon,
            ..
        } = *commit;

        if !consumed.is_empty() {
            let mut next_index = 0;
            entries.retain(|entry| {
                if consumed.contains(&entry.page_id) {
                    entry_indices.remove(&entry.page_id);
                    retired_chain_pages.remove(&entry.page_id);
                    false
                } else {
                    *entry_indices.get_mut(&entry.page_id).unwrap() = next_index;
                    next_index += 1;
                    true
                }
            });
        }
        let new_count = page_ids.len() + freed_this_txn.len();

        // Every loan page has one surviving entry. Reuse its validated index
        // and repair the index of the entry moved by swap_remove.
        let mut structure = Vec::new();
        let mut taken = Vec::new();
        while structure.len() < chain_pages_needed(entries.len() + new_count) {
            let Some(page_id) = loan_pool.pop() else {
                break;
            };
            let idx = entry_indices
                .remove(&page_id)
                .ok_or(Error::DatabaseCorrupted)?;
            taken.push(entries.swap_remove(idx));
            if let Some(moved) = entries.get(idx) {
                *entry_indices.get_mut(&moved.page_id).unwrap() = idx;
            }
            structure.push(page_id);
        }
        // Removing an entry can reduce the required number of chain pages.
        while structure.len() > chain_pages_needed(entries.len() + new_count) {
            let page_id = structure.pop().unwrap();
            let entry = taken.pop().unwrap();
            debug_assert_eq!(entry.page_id, page_id);
            entry_indices.insert(page_id, entries.len());
            entries.push(entry);
            loan_pool.push(page_id);
        }
        drop(entry_indices);
        for page_id in &structure {
            retired_chain_pages.remove(page_id);
        }
        while structure.len() < chain_pages_needed(entries.len() + new_count) {
            structure.push(alloc.allocate());
        }

        let surviving_len = entries.len();
        for &page_id in page_ids.iter().chain(freed_this_txn) {
            entries.push(PendingFreeEntry {
                page_id,
                freed_at_txn: txn_id,
            });
        }
        let new_root = write_chain(pages, txn_id, &entries, &structure);

        // Old structure and current frees remain referenced by the previous
        // slot. Only surviving entries may be returned to the allocator.
        entries.truncate(surviving_len);
        entries.retain(|entry| {
            entry.freed_at_txn <= reclaim_horizon
                || retired_chain_pages.get(&entry.page_id) == Some(&entry.freed_at_txn)
        });
        for page_id in page_ids {
            retired_chain_pages.insert(page_id, txn_id);
        }
        Ok((new_root, entries))
    }

    fn prepend_frees(
        self,
        pages: &mut FxHashMap<PageId, Page>,
        alloc: &mut PageAllocator,
        commit: &ChainCommit<'_>,
        retired_chain_pages: &mut FxHashMap<PageId, TxnId>,
    ) -> (PageId, Vec<PendingFreeEntry>) {
        let Self {
            mut entries,
            page_ids,
            head_entry_count,
            entry_indices,
        } = self;
        drop(entry_indices);
        let mut new_root = commit.current_root;
        let mut retired_head = None;
        if !commit.freed_this_txn.is_empty() {
            let replace_head = new_root.is_valid() && head_entry_count < MAX_ENTRIES_PER_PAGE;
            let copied = if replace_head { head_entry_count } else { 0 };
            let mut prefix = Vec::with_capacity(
                copied + usize::from(replace_head) + commit.freed_this_txn.len(),
            );
            prefix.extend_from_slice(&entries[..copied]);
            let tail = if replace_head {
                retired_head = Some(new_root);
                prefix.push(PendingFreeEntry {
                    page_id: new_root,
                    freed_at_txn: commit.txn_id,
                });
                page_ids.get(1).copied().unwrap_or(PageId::INVALID)
            } else {
                new_root
            };
            prefix.extend(
                commit
                    .freed_this_txn
                    .iter()
                    .map(|&page_id| PendingFreeEntry {
                        page_id,
                        freed_at_txn: commit.txn_id,
                    }),
            );
            new_root = prepend_chain(pages, alloc, commit.txn_id, &prefix, tail);
        }

        // Reader release must make old entries available even when the chain is
        // shared. Current frees and the replaced head are not reusable yet.
        entries.retain(|entry| {
            entry.freed_at_txn <= commit.reclaim_horizon
                || retired_chain_pages.get(&entry.page_id) == Some(&entry.freed_at_txn)
        });
        if let Some(page_id) = retired_head {
            retired_chain_pages.insert(page_id, commit.txn_id);
        }
        (new_root, entries)
    }
}

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
