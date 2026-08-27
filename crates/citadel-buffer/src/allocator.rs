//! Page allocator with two-phase pending-free model for CoW B+ tree.

use citadel_core::types::PageId;

#[derive(Clone)]
pub struct PageAllocator {
    /// Next page ID to allocate from (high water mark).
    next_page_id: u32,
    /// Pages reclaimed from pending-free chain (safe to reuse).
    ready_to_use: Vec<PageId>,
    /// Pages freed in the current write transaction.
    freed_this_txn: Vec<PageId>,
    /// All page IDs allocated this txn (in allocation order). Used to bound
    /// O(allocated) page-cache cleanup on ROLLBACK TO SAVEPOINT.
    allocated_this_txn: Vec<PageId>,
}

impl PageAllocator {
    pub fn new(high_water_mark: u32) -> Self {
        Self {
            next_page_id: high_water_mark,
            ready_to_use: Vec::new(),
            freed_this_txn: Vec::new(),
            allocated_this_txn: Vec::new(),
        }
    }

    /// Prefers reusing reclaimed pages over incrementing the high water mark.
    pub fn allocate(&mut self) -> PageId {
        let id = if let Some(id) = self.ready_to_use.pop() {
            id
        } else {
            let id = PageId(self.next_page_id);
            self.next_page_id += 1;
            id
        };
        self.allocated_this_txn.push(id);
        id
    }

    /// Allocate a page whose id is not zero.
    ///
    /// Overflow chains use page zero as their on-disk terminator, but page zero
    /// is also a real page that can re-enter the reclaim pool, so overflow
    /// writers take it from here and leave it for types that can represent it.
    pub fn allocate_nonzero(&mut self) -> PageId {
        if self.next_page_id == 0 {
            self.ready_to_use.push(PageId(0));
            self.next_page_id = 1;
        }

        let id = match self
            .ready_to_use
            .iter()
            .rposition(|page_id| page_id.as_u32() != 0)
        {
            Some(index) => self.ready_to_use.swap_remove(index),
            None => {
                let id = PageId(self.next_page_id);
                self.next_page_id += 1;
                id
            }
        };
        self.allocated_this_txn.push(id);
        id
    }

    /// Not immediately reusable - goes into pending-free list.
    pub fn free(&mut self, page_id: PageId) {
        self.freed_this_txn.push(page_id);
    }

    pub fn high_water_mark(&self) -> u32 {
        self.next_page_id
    }

    pub fn freed_this_txn(&self) -> &[PageId] {
        &self.freed_this_txn
    }

    pub fn allocated_this_txn(&self) -> &[PageId] {
        &self.allocated_this_txn
    }

    pub fn add_ready_to_use(&mut self, pages: Vec<PageId>) {
        self.ready_to_use.extend(pages);
    }

    /// Drain the unconsumed reclaimed pages so the caller can carry them over
    /// to the next transaction instead of leaking them.
    pub fn take_ready_to_use(&mut self) -> Vec<PageId> {
        std::mem::take(&mut self.ready_to_use)
    }

    pub fn commit(&mut self) -> Vec<PageId> {
        self.allocated_this_txn.clear();
        std::mem::take(&mut self.freed_this_txn)
    }

    pub fn rollback(&mut self) {
        self.freed_this_txn.clear();
        self.allocated_this_txn.clear();
    }

    pub fn ready_count(&self) -> usize {
        self.ready_to_use.len()
    }

    pub fn freed_count(&self) -> usize {
        self.freed_this_txn.len()
    }

    pub fn checkpoint(&self) -> AllocCheckpoint {
        AllocCheckpoint {
            next_page_id: self.next_page_id,
            ready_to_use: self.ready_to_use.clone(),
            freed_this_txn_len: self.freed_this_txn.len(),
            allocated_this_txn_len: self.allocated_this_txn.len(),
        }
    }

    pub fn restore(&mut self, cp: AllocCheckpoint) {
        self.next_page_id = cp.next_page_id;
        self.ready_to_use = cp.ready_to_use;
        self.freed_this_txn.truncate(cp.freed_this_txn_len);
        self.allocated_this_txn.truncate(cp.allocated_this_txn_len);
    }

    pub fn allocated_since(&self, checkpoint_len: usize) -> &[PageId] {
        &self.allocated_this_txn[checkpoint_len..]
    }
}

#[derive(Clone)]
pub struct AllocCheckpoint {
    next_page_id: u32,
    ready_to_use: Vec<PageId>,
    freed_this_txn_len: usize,
    allocated_this_txn_len: usize,
}

impl AllocCheckpoint {
    pub fn allocated_this_txn_len(&self) -> usize {
        self.allocated_this_txn_len
    }
}

#[cfg(test)]
#[path = "allocator_tests.rs"]
mod tests;
