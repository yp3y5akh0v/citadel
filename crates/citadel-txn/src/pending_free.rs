//! Pending-free chain: tracks freed pages that can't be reused until no older readers exist.
//!
//! Format: linked list of PendingFree pages on disk.
//! Each page contains an array of PendingFreeEntry structs.
//! Chain head stored in CommitSlot.pending_free_root.
//!
//! Two-phase model:
//! - Phase A: scan existing chain, reclaim entries older than oldest_active_reader
//! - Phase B: write new chain with remaining + newly freed entries
//! - Old chain pages are deferred to the NEXT commit (breaks circular dependency)

use citadel_buffer::allocator::PageAllocator;
use citadel_core::types::{PageId, PageType, TxnId};
use citadel_core::{Error, Result, PAGE_HEADER_SIZE, PENDING_FREE_ENTRY_SIZE, USABLE_SIZE};
use citadel_page::page::Page;
use rustc_hash::FxHashMap;

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

/// Write a new pending-free chain into the page map.
/// Allocates new pages from the allocator (never reuses old chain pages).
/// Returns the root PageId of the new chain (PageId::INVALID if empty).
pub fn write_chain(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    entries: &[PendingFreeEntry],
) -> PageId {
    if entries.is_empty() {
        return PageId::INVALID;
    }

    let num_pages = entries.len().div_ceil(MAX_ENTRIES_PER_PAGE);

    // Allocate all pages up front so `right_child` links can reference them.
    let page_ids: Vec<PageId> = (0..num_pages).map(|_| alloc.allocate()).collect();

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

/// Process the pending-free chain during commit.
///
/// 1. Reads existing chain entries
/// 2. Reclaims entries with freed_at_txn < oldest_active_reader
/// 3. Adds freed_this_txn + deferred_free as new entries
/// 4. Writes new chain (CoW - never reuses old chain pages)
///
/// Returns: (new_chain_root, reclaimed_page_ids, old_chain_page_ids)
///
/// The old_chain_page_ids should be added to deferred_free for the NEXT commit.
/// The reclaimed_page_ids can be added to alloc.ready_to_use for future txns.
pub fn process_chain(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    current_root: PageId,
    freed_this_txn: &[PageId],
    deferred_free: &[PageId],
    oldest_active_reader: TxnId,
) -> Result<(PageId, Vec<PageId>, Vec<PageId>)> {
    let existing = read_chain(pages, current_root)?;
    let old_chain_pages = collect_chain_page_ids(pages, current_root)?;

    let mut still_pending = Vec::new();
    let mut reclaimed = Vec::new();

    for entry in existing {
        if entry.freed_at_txn.as_u64() < oldest_active_reader.as_u64() {
            reclaimed.push(entry.page_id);
        } else {
            still_pending.push(entry);
        }
    }

    for &page_id in deferred_free {
        still_pending.push(PendingFreeEntry {
            page_id,
            freed_at_txn: txn_id,
        });
    }

    for &page_id in freed_this_txn {
        still_pending.push(PendingFreeEntry {
            page_id,
            freed_at_txn: txn_id,
        });
    }

    let new_root = write_chain(pages, alloc, txn_id, &still_pending);

    Ok((new_root, reclaimed, old_chain_pages))
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
