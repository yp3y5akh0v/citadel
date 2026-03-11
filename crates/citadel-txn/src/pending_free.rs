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

use std::collections::HashMap;
use citadel_core::types::{PageId, PageType, TxnId};
use citadel_core::{Error, Result, PAGE_HEADER_SIZE, PENDING_FREE_ENTRY_SIZE, USABLE_SIZE};
use citadel_page::page::Page;
use citadel_buffer::allocator::PageAllocator;

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
pub fn read_chain(
    pages: &HashMap<PageId, Page>,
    root: PageId,
) -> Result<Vec<PendingFreeEntry>> {
    if !root.is_valid() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    let mut current = root;

    while current.is_valid() {
        let page = pages.get(&current)
            .ok_or(Error::PageOutOfBounds(current))?;

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
    pages: &mut HashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    entries: &[PendingFreeEntry],
) -> PageId {
    if entries.is_empty() {
        return PageId::INVALID;
    }

    let num_pages = (entries.len() + MAX_ENTRIES_PER_PAGE - 1) / MAX_ENTRIES_PER_PAGE;

    // Allocate all pages first so we can link them
    let page_ids: Vec<PageId> = (0..num_pages).map(|_| alloc.allocate()).collect();

    let mut entry_idx = 0;
    for (i, &page_id) in page_ids.iter().enumerate() {
        let mut page = Page::new(page_id, PageType::PendingFree, txn_id);

        // Link to next page (INVALID = end of chain)
        let next = if i + 1 < num_pages {
            page_ids[i + 1]
        } else {
            PageId::INVALID
        };
        page.set_right_child(next);

        let entries_this_page = std::cmp::min(MAX_ENTRIES_PER_PAGE, entries.len() - entry_idx);

        // Write entry count
        page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&(entries_this_page as u32).to_le_bytes());

        // Write entries
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
    pages: &HashMap<PageId, Page>,
    root: PageId,
) -> Result<Vec<PageId>> {
    if !root.is_valid() {
        return Ok(Vec::new());
    }

    let mut ids = Vec::new();
    let mut current = root;

    while current.is_valid() {
        ids.push(current);
        let page = pages.get(&current)
            .ok_or(Error::PageOutOfBounds(current))?;
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
/// 4. Writes new chain (CoW — never reuses old chain pages)
///
/// Returns: (new_chain_root, reclaimed_page_ids, old_chain_page_ids)
///
/// The old_chain_page_ids should be added to deferred_free for the NEXT commit.
/// The reclaimed_page_ids can be added to alloc.ready_to_use for future txns.
pub fn process_chain(
    pages: &mut HashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    txn_id: TxnId,
    current_root: PageId,
    freed_this_txn: &[PageId],
    deferred_free: &[PageId],
    oldest_active_reader: TxnId,
) -> Result<(PageId, Vec<PageId>, Vec<PageId>)> {
    // Phase A: Read existing chain and separate reclaimable vs. still-pending
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

    // Add deferred_free entries (old chain pages from previous commit)
    for &page_id in deferred_free {
        still_pending.push(PendingFreeEntry {
            page_id,
            freed_at_txn: txn_id,
        });
    }

    // Add freed_this_txn entries
    for &page_id in freed_this_txn {
        still_pending.push(PendingFreeEntry {
            page_id,
            freed_at_txn: txn_id,
        });
    }

    // Phase B: Write new chain (new pages from HWM or ready_to_use)
    let new_root = write_chain(pages, alloc, txn_id, &still_pending);

    Ok((new_root, reclaimed, old_chain_pages))
}

// --- Internal helpers ---

fn read_entry_count(page: &Page) -> usize {
    u32::from_le_bytes(
        page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4].try_into().unwrap()
    ) as usize
}

fn read_entry_at(data: &[u8], offset: usize) -> PendingFreeEntry {
    PendingFreeEntry {
        page_id: PageId(u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())),
        freed_at_txn: TxnId(u64::from_le_bytes(data[offset + 4..offset + 12].try_into().unwrap())),
    }
}

fn write_entry_at(data: &mut [u8], offset: usize, entry: &PendingFreeEntry) {
    data[offset..offset + 4].copy_from_slice(&entry.page_id.as_u32().to_le_bytes());
    data[offset + 4..offset + 12].copy_from_slice(&entry.freed_at_txn.as_u64().to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_chain() {
        let pages = HashMap::new();
        let entries = read_chain(&pages, PageId::INVALID).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn write_and_read_chain() {
        let mut pages = HashMap::new();
        let mut alloc = PageAllocator::new(0);

        let entries = vec![
            PendingFreeEntry { page_id: PageId(10), freed_at_txn: TxnId(1) },
            PendingFreeEntry { page_id: PageId(20), freed_at_txn: TxnId(2) },
            PendingFreeEntry { page_id: PageId(30), freed_at_txn: TxnId(3) },
        ];

        let root = write_chain(&mut pages, &mut alloc, TxnId(5), &entries);
        assert!(root.is_valid());

        let read_back = read_chain(&pages, root).unwrap();
        assert_eq!(read_back.len(), 3);
        assert_eq!(read_back[0], entries[0]);
        assert_eq!(read_back[1], entries[1]);
        assert_eq!(read_back[2], entries[2]);
    }

    #[test]
    fn write_chain_multi_page() {
        let mut pages = HashMap::new();
        let mut alloc = PageAllocator::new(0);

        // Create enough entries to span multiple pages
        let count = MAX_ENTRIES_PER_PAGE + 10;
        let entries: Vec<PendingFreeEntry> = (0..count).map(|i| {
            PendingFreeEntry {
                page_id: PageId(100 + i as u32),
                freed_at_txn: TxnId(i as u64),
            }
        }).collect();

        let root = write_chain(&mut pages, &mut alloc, TxnId(999), &entries);
        let read_back = read_chain(&pages, root).unwrap();
        assert_eq!(read_back.len(), count);

        for (i, entry) in read_back.iter().enumerate() {
            assert_eq!(entry.page_id, PageId(100 + i as u32));
            assert_eq!(entry.freed_at_txn, TxnId(i as u64));
        }
    }

    #[test]
    fn write_empty_chain() {
        let mut pages = HashMap::new();
        let mut alloc = PageAllocator::new(0);

        let root = write_chain(&mut pages, &mut alloc, TxnId(1), &[]);
        assert_eq!(root, PageId::INVALID);
    }

    #[test]
    fn collect_chain_pages() {
        let mut pages = HashMap::new();
        let mut alloc = PageAllocator::new(0);

        let entries: Vec<PendingFreeEntry> = (0..MAX_ENTRIES_PER_PAGE + 10).map(|i| {
            PendingFreeEntry {
                page_id: PageId(100 + i as u32),
                freed_at_txn: TxnId(1),
            }
        }).collect();

        let root = write_chain(&mut pages, &mut alloc, TxnId(1), &entries);
        let chain_pages = collect_chain_page_ids(&pages, root).unwrap();
        assert_eq!(chain_pages.len(), 2); // Should span 2 pages
    }

    #[test]
    fn process_chain_gc() {
        let mut pages = HashMap::new();
        let mut alloc = PageAllocator::new(0);

        // Write initial chain with entries at txn 1, 2, 3
        let initial_entries = vec![
            PendingFreeEntry { page_id: PageId(10), freed_at_txn: TxnId(1) },
            PendingFreeEntry { page_id: PageId(20), freed_at_txn: TxnId(2) },
            PendingFreeEntry { page_id: PageId(30), freed_at_txn: TxnId(3) },
        ];
        let root = write_chain(&mut pages, &mut alloc, TxnId(3), &initial_entries);

        // Process with oldest_active_reader = TxnId(3)
        // Entries at txn 1 and 2 should be reclaimed
        let freed_this_txn = vec![PageId(40)];
        let (new_root, reclaimed, old_chain) = process_chain(
            &mut pages, &mut alloc, TxnId(4),
            root, &freed_this_txn, &[], TxnId(3),
        ).unwrap();

        // Reclaimed: pages freed at txn 1, 2 (< oldest_active_reader 3)
        assert_eq!(reclaimed.len(), 2);
        assert!(reclaimed.contains(&PageId(10)));
        assert!(reclaimed.contains(&PageId(20)));

        // New chain should have: entry at txn 3 (still pending) + entry at txn 4 (newly freed)
        let new_entries = read_chain(&pages, new_root).unwrap();
        assert_eq!(new_entries.len(), 2);

        // Old chain pages should be returned for deferred freeing
        assert!(!old_chain.is_empty());
    }

    #[test]
    fn process_chain_with_deferred_free() {
        let mut pages = HashMap::new();
        let mut alloc = PageAllocator::new(0);

        // Process with empty existing chain but with deferred_free entries
        let deferred = vec![PageId(50), PageId(51)];
        let freed = vec![PageId(60)];
        let (new_root, reclaimed, old_chain) = process_chain(
            &mut pages, &mut alloc, TxnId(1),
            PageId::INVALID, &freed, &deferred, TxnId(1),
        ).unwrap();

        assert!(reclaimed.is_empty());
        assert!(old_chain.is_empty());

        let entries = read_chain(&pages, new_root).unwrap();
        assert_eq!(entries.len(), 3); // 2 deferred + 1 freed
    }

    #[test]
    fn max_entries_per_page_correct() {
        // Verify the constant matches what we expect
        assert_eq!(MAX_ENTRIES_PER_PAGE, (8096 - 4) / 12); // (USABLE_SIZE - 4) / 12
    }
}
