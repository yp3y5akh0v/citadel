//! Page allocator for the CoW B+ tree.
//!
//! Uses a two-phase pending-free model (inspired by redb):
//! - New pages are allocated from `ready_to_use` (reclaimed) or high water mark
//! - Freed pages go into `freed_this_txn` (not reusable until committed + no older readers)
//! - On-disk pending-free chain persistence happens in Phase 3 (commit protocol)

use citadel_core::types::PageId;

/// In-memory page allocator state.
pub struct PageAllocator {
    /// Next page ID to allocate from (high water mark).
    next_page_id: u32,
    /// Pages reclaimed from pending-free chain (safe to reuse).
    ready_to_use: Vec<PageId>,
    /// Pages freed in the current write transaction.
    freed_this_txn: Vec<PageId>,
}

impl PageAllocator {
    /// Create a new allocator starting from the given high water mark.
    pub fn new(high_water_mark: u32) -> Self {
        Self {
            next_page_id: high_water_mark,
            ready_to_use: Vec::new(),
            freed_this_txn: Vec::new(),
        }
    }

    /// Allocate a new page ID.
    /// Prefers reusing reclaimed pages; falls back to incrementing the high water mark.
    pub fn allocate(&mut self) -> PageId {
        if let Some(id) = self.ready_to_use.pop() {
            id
        } else {
            let id = PageId(self.next_page_id);
            self.next_page_id += 1;
            id
        }
    }

    /// Mark a page as freed in the current transaction.
    /// The page is NOT immediately reusable — it goes into the pending-free list.
    pub fn free(&mut self, page_id: PageId) {
        self.freed_this_txn.push(page_id);
    }

    /// Current high water mark (next page ID that would be allocated from disk).
    pub fn high_water_mark(&self) -> u32 {
        self.next_page_id
    }

    /// Pages freed in the current transaction.
    pub fn freed_this_txn(&self) -> &[PageId] {
        &self.freed_this_txn
    }

    /// Add pages that are safe to reuse (reclaimed from pending-free chain).
    pub fn add_ready_to_use(&mut self, pages: Vec<PageId>) {
        self.ready_to_use.extend(pages);
    }

    /// Commit: take ownership of freed pages (for writing to pending-free chain).
    /// Returns the list of pages freed in this transaction.
    pub fn commit(&mut self) -> Vec<PageId> {
        std::mem::take(&mut self.freed_this_txn)
    }

    /// Rollback: discard freed pages (transaction aborted).
    pub fn rollback(&mut self) {
        self.freed_this_txn.clear();
    }

    /// Number of pages available for immediate reuse.
    pub fn ready_count(&self) -> usize {
        self.ready_to_use.len()
    }

    /// Number of pages freed in the current transaction.
    pub fn freed_count(&self) -> usize {
        self.freed_this_txn.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_from_hwm() {
        let mut alloc = PageAllocator::new(0);
        assert_eq!(alloc.allocate(), PageId(0));
        assert_eq!(alloc.allocate(), PageId(1));
        assert_eq!(alloc.allocate(), PageId(2));
        assert_eq!(alloc.high_water_mark(), 3);
    }

    #[test]
    fn allocate_from_ready_to_use() {
        let mut alloc = PageAllocator::new(10);
        alloc.add_ready_to_use(vec![PageId(3), PageId(7)]);
        // Should use ready_to_use first (LIFO)
        assert_eq!(alloc.allocate(), PageId(7));
        assert_eq!(alloc.allocate(), PageId(3));
        // Now falls back to HWM
        assert_eq!(alloc.allocate(), PageId(10));
    }

    #[test]
    fn free_and_commit() {
        let mut alloc = PageAllocator::new(5);
        alloc.free(PageId(1));
        alloc.free(PageId(3));
        assert_eq!(alloc.freed_count(), 2);

        let freed = alloc.commit();
        assert_eq!(freed.len(), 2);
        assert_eq!(alloc.freed_count(), 0);
    }

    #[test]
    fn rollback_clears_freed() {
        let mut alloc = PageAllocator::new(5);
        alloc.free(PageId(1));
        alloc.free(PageId(3));
        alloc.rollback();
        assert_eq!(alloc.freed_count(), 0);
    }
}
