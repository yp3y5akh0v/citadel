//! Page allocator with two-phase pending-free model for CoW B+ tree.

use citadel_core::types::PageId;
use std::sync::Arc;

#[derive(Clone, Default)]
struct ReadyPages {
    pages: Option<Arc<Vec<PageId>>>,
    remaining: usize,
    /// The logical pool is `pages[..remaining]` followed by these zero IDs.
    trailing_zeros: usize,
}

impl ReadyPages {
    fn shared(pages: Arc<Vec<PageId>>) -> Self {
        if pages.is_empty() {
            return Self::default();
        }
        Self {
            remaining: pages.len(),
            pages: Some(pages),
            trailing_zeros: 0,
        }
    }

    fn len(&self) -> usize {
        self.remaining + self.trailing_zeros
    }

    fn pop(&mut self) -> Option<PageId> {
        if self.trailing_zeros != 0 {
            self.trailing_zeros -= 1;
            return Some(PageId(0));
        }
        self.remaining = self.remaining.checked_sub(1)?;
        Some(self.pages.as_ref().unwrap()[self.remaining])
    }

    fn pop_nonzero(&mut self) -> Option<PageId> {
        while self.remaining != 0 {
            self.remaining -= 1;
            let page = self.pages.as_ref().unwrap()[self.remaining];
            if page.as_u32() != 0 {
                return Some(page);
            }
            self.trailing_zeros += 1;
        }
        None
    }

    fn append(&mut self, pages: Vec<PageId>) {
        if pages.is_empty() {
            return;
        }
        let combined = if self.len() == 0 {
            pages
        } else {
            let mut combined = self.take();
            combined.extend(pages);
            combined
        };
        *self = Self::shared(Arc::new(combined));
    }

    fn take(&mut self) -> Vec<PageId> {
        let previous = std::mem::take(self);
        let mut pages = match previous.pages {
            Some(pages) => match Arc::try_unwrap(pages) {
                Ok(mut pages) => {
                    pages.truncate(previous.remaining);
                    pages
                }
                Err(pages) => pages[..previous.remaining].to_vec(),
            },
            None => Vec::new(),
        };
        pages.resize(pages.len() + previous.trailing_zeros, PageId(0));
        pages
    }
}

#[derive(Clone)]
pub struct PageAllocator {
    /// Next page ID to allocate from (high water mark).
    next_page_id: u32,
    /// Pages reclaimed from pending-free chain (safe to reuse).
    ready_to_use: ReadyPages,
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
            ready_to_use: ReadyPages::default(),
            freed_this_txn: Vec::new(),
            allocated_this_txn: Vec::new(),
        }
    }

    /// Use an immutable reclaimed batch without copying it into the allocator.
    pub fn with_ready_pages(high_water_mark: u32, pages: Arc<Vec<PageId>>) -> Self {
        Self {
            ready_to_use: ReadyPages::shared(pages),
            ..Self::new(high_water_mark)
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
            self.ready_to_use.trailing_zeros += 1;
            self.next_page_id = 1;
        }

        let id = match self.ready_to_use.pop_nonzero() {
            Some(page) => page,
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
        self.ready_to_use.append(pages);
    }

    /// Drain the unconsumed reclaimed pages so the caller can carry them over
    /// to the next transaction instead of leaking them.
    pub fn take_ready_to_use(&mut self) -> Vec<PageId> {
        self.ready_to_use.take()
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
    ready_to_use: ReadyPages,
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
