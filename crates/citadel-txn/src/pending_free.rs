//! Pending-free chain: freed pages that can't be reused until no older reader
//! exists.
//!
//! Format: linked list of PendingFree pages on disk.
//! Each page contains an array of PendingFreeEntry structs.
//! Chain head stored in CommitSlot.pending_free_root.

use citadel_buffer::allocator::PageAllocator;
use citadel_core::types::{PageId, PageType, TxnId};
use citadel_core::{Error, Result, PAGE_HEADER_SIZE, PENDING_FREE_ENTRY_SIZE, USABLE_SIZE};
use citadel_page::page::Page;
use rustc_hash::{FxHashMap, FxHashSet};

/// A pending-free entry: a page that was freed at a specific transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingFreeEntry {
    pub page_id: PageId,
    pub freed_at_txn: TxnId,
}

/// Maximum entries per pending-free page.
/// Body layout: [entry_count: u32 (4B)] [entries: 12B each] [padding]
const MAX_ENTRIES_PER_PAGE: usize = (USABLE_SIZE - 4) / PENDING_FREE_ENTRY_SIZE;

/// Read all entries from the pending-free chain stored in the page map.
pub fn read_chain(pages: &FxHashMap<PageId, Page>, root: PageId) -> Result<Vec<PendingFreeEntry>> {
    if !root.is_valid() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    let mut current = root;

    while current.is_valid() {
        let page = pages.get(&current).ok_or(Error::PageOutOfBounds(current))?;

        let entry_count = read_entry_count(page);
        let data_start = PAGE_HEADER_SIZE + 4;

        for i in 0..entry_count {
            let offset = data_start + i * PENDING_FREE_ENTRY_SIZE;
            entries.push(read_entry_at(&page.data, offset));
        }

        // Next page in chain via right_child field (INVALID = end of chain)
        current = page.right_child();
        if !current.is_valid() {
            break;
        }
    }

    Ok(entries)
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

    let mut entry_idx = 0;
    for (i, &page_id) in page_ids.iter().enumerate() {
        let mut page = Page::new(page_id, PageType::PendingFree, txn_id);

        let next = if i + 1 < num_pages {
            page_ids[i + 1]
        } else {
            PageId::INVALID
        };
        page.set_right_child(next);

        let entries_this_page = std::cmp::min(MAX_ENTRIES_PER_PAGE, entries.len() - entry_idx);

        page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&(entries_this_page as u32).to_le_bytes());

        let data_start = PAGE_HEADER_SIZE + 4;
        for j in 0..entries_this_page {
            let offset = data_start + j * PENDING_FREE_ENTRY_SIZE;
            write_entry_at(&mut page.data, offset, &entries[entry_idx + j]);
        }

        entry_idx += entries_this_page;
        page.update_checksum();
        pages.insert(page_id, page);
    }

    page_ids[0]
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

    while current.is_valid() {
        ids.push(current);
        let page = pages.get(&current).ok_or(Error::PageOutOfBounds(current))?;
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

/// Drop consumed entries, draw new structure pages from the loan first (CoW,
/// never reusing old chain pages), and add this txn's frees plus the old
/// chain pages as new entries. Entries stay listed until a commit records
/// their consumption, so an abort/no-op/shutdown strands nothing.
///
/// Returns `(new_chain_root, available_entries)`; entries carry freed_at_txn
/// so the caller can zero each page once for secure delete.
pub fn process_chain(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    loan_pool: &mut Vec<PageId>,
    commit: &ChainCommit<'_>,
) -> Result<(PageId, Vec<PendingFreeEntry>)> {
    let ChainCommit {
        txn_id,
        current_root,
        freed_this_txn,
        consumed,
        reclaim_horizon,
    } = *commit;
    let existing = read_chain(pages, current_root)?;
    let old_chain_pages = collect_chain_page_ids(pages, current_root)?;

    let mut surviving: Vec<PendingFreeEntry> = existing
        .into_iter()
        .filter(|entry| !consumed.contains(&entry.page_id))
        .collect();
    let new_count = old_chain_pages.len() + freed_this_txn.len();

    // Structure pages, loan pool first. Every loan page has exactly one
    // surviving entry (it came from the chain and was not consumed by the
    // txn body); taking it removes that entry.
    let mut structure: Vec<PageId> = Vec::new();
    let mut taken: FxHashMap<PageId, TxnId> = FxHashMap::default();
    while structure.len() < chain_pages_needed(surviving.len() + new_count) {
        let Some(page_id) = loan_pool.pop() else {
            break;
        };
        let idx = surviving
            .iter()
            .position(|entry| entry.page_id == page_id)
            .expect("loan page must have an unconsumed chain entry");
        taken.insert(page_id, surviving.swap_remove(idx).freed_at_txn);
        structure.push(page_id);
    }
    // Removing an entry can lower the page count below what was already
    // taken; hand the overshoot back (at most one page).
    while structure.len() > chain_pages_needed(surviving.len() + new_count) {
        let page_id = structure.pop().unwrap();
        surviving.push(PendingFreeEntry {
            page_id,
            freed_at_txn: taken.remove(&page_id).unwrap(),
        });
        loan_pool.push(page_id);
    }
    while structure.len() < chain_pages_needed(surviving.len() + new_count) {
        structure.push(alloc.allocate());
    }

    // Reuse is safe iff freed_at <= horizon (see reclaim_horizon). A page
    // freed at this txn is excluded: the previous slot still references it
    // until the next commit rewrites its location.
    let available = surviving
        .iter()
        .filter(|entry| entry.freed_at_txn.as_u64() <= reclaim_horizon.as_u64())
        .copied()
        .collect();

    let mut entries = surviving;
    for &page_id in old_chain_pages.iter().chain(freed_this_txn) {
        entries.push(PendingFreeEntry {
            page_id,
            freed_at_txn: txn_id,
        });
    }

    let new_root = write_chain(pages, txn_id, &entries, &structure);

    Ok((new_root, available))
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
