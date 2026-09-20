//! Page allocator with two-phase pending-free model for CoW B+ tree.

use citadel_core::types::PageId;
use citadel_core::{Error, Result};
use std::sync::Arc;

/// Immutable reclaimed-page batches with an independent allocation cursor.
/// Cloning a cursor or appending a batch shares all untouched pages.
#[derive(Clone, Default)]
pub struct ReadyPages {
    pages: Option<Arc<Vec<PageId>>>,
    remaining: usize,
    tail: Option<Arc<ReadyPages>>,
    total_len: usize,
    trailing_zeros: usize,
}

impl Drop for ReadyPages {
    fn drop(&mut self) {
        // A long-lived loan can contain many small batches. Release unique
        // tails iteratively rather than recursing once per batch.
        while let Some(tail) = self.tail.take() {
            match Arc::try_unwrap(tail) {
                Ok(mut tail) => self.tail = tail.tail.take(),
                Err(_) => break,
            }
        }
    }
}

impl ReadyPages {
    fn shared(pages: Arc<Vec<PageId>>) -> Self {
        let len = pages.len();
        if len == 0 {
            return Self::default();
        }
        Self {
            pages: Some(pages),
            remaining: len,
            total_len: len,
            tail: None,
            trailing_zeros: 0,
        }
    }

    /// Construct a batch whose first input page is allocated first.
    pub fn from_pop_order(mut pages: Vec<PageId>) -> Self {
        pages.reverse();
        Self::shared(Arc::new(pages))
    }

    /// Number of unconsumed pages, including page zero.
    pub fn len(&self) -> usize {
        self.total_len
    }

    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    /// The next page that `pop` will return.
    pub fn last(&self) -> Option<PageId> {
        let mut current = self;
        loop {
            if current.trailing_zeros != 0 {
                return Some(PageId(0));
            }
            if current.remaining != 0 {
                return Some(current.pages.as_ref().unwrap()[current.remaining - 1]);
            }
            current = current.tail.as_deref()?;
        }
    }

    fn normalize(&mut self) {
        while self.remaining == 0 && self.trailing_zeros == 0 {
            let Some(tail) = self.tail.take() else {
                self.pages = None;
                return;
            };
            *self = Arc::try_unwrap(tail).unwrap_or_else(|tail| (*tail).clone());
        }
    }

    /// Consume the next page without copying the remaining batches.
    pub fn pop(&mut self) -> Option<PageId> {
        self.normalize();
        if self.trailing_zeros != 0 {
            self.trailing_zeros -= 1;
            self.total_len -= 1;
            return Some(PageId(0));
        }
        self.remaining = self.remaining.checked_sub(1)?;
        self.total_len -= 1;
        let page = self.pages.as_ref().unwrap()[self.remaining];
        self.normalize();
        Some(page)
    }

    fn pop_nonzero(&mut self) -> Option<PageId> {
        let mut zeros = 0;
        let found = loop {
            self.normalize();
            zeros += self.trailing_zeros;
            self.total_len -= self.trailing_zeros;
            self.trailing_zeros = 0;
            // Removing a zero-only prefix may reveal another batch.
            self.normalize();
            if self.trailing_zeros != 0 {
                continue;
            }
            match self.pop() {
                Some(PageId(0)) => zeros += 1,
                page => break page,
            }
        };
        self.trailing_zeros += zeros;
        self.total_len += zeros;
        found
    }

    /// Put a page back at the front of this cursor.
    pub fn push(&mut self, page: PageId) {
        self.append(vec![page]);
    }

    /// Add pages before the current remainder, first input page first.
    pub fn prepend_pop_order(&mut self, mut pages: Vec<PageId>) {
        pages.reverse();
        self.append(pages);
    }

    fn append(&mut self, pages: Vec<PageId>) {
        if pages.is_empty() {
            return;
        }
        self.normalize();
        let previous = std::mem::take(self);
        let len = pages.len();
        self.total_len = previous.len() + len;
        self.remaining = len;
        self.pages = Some(Arc::new(pages));
        if !previous.is_empty() {
            self.tail = Some(Arc::new(previous));
        }
    }

    fn take(&mut self) -> Vec<PageId> {
        let mut previous = std::mem::take(self);
        previous.normalize();
        if previous.tail.is_none() {
            let mut pages = match previous.pages.take() {
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
            return pages;
        }

        // The Vec adapter stores the oldest batch first, opposite to cursor
        // traversal. Copy each whole slice into its final position; zero IDs
        // already occupy the gaps between batches.
        let mut pages = vec![PageId(0); previous.len()];
        let mut end = pages.len();
        let mut current = &previous;
        loop {
            end -= current.trailing_zeros;
            let start = end - current.remaining;
            if let Some(batch) = &current.pages {
                pages[start..end].copy_from_slice(&batch[..current.remaining]);
            }
            end = start;
            let Some(tail) = current.tail.as_deref() else {
                break;
            };
            current = tail;
        }
        debug_assert_eq!(end, 0);
        pages
    }
}

impl std::fmt::Debug for ReadyPages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for ReadyPages {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl Eq for ReadyPages {}

impl ReadyPages {
    /// Check whether an unconsumed loan contains this page.
    pub fn contains(&self, page: &PageId) -> bool {
        self.iter().any(|candidate| candidate == page)
    }

    /// Visit the remaining pages in allocation order without consuming them.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &PageId> {
        ReadyPagesIter {
            current: Some(self),
            remaining: self.remaining,
            zeros: self.trailing_zeros,
            total: self.len(),
        }
    }
}

struct ReadyPagesIter<'a> {
    current: Option<&'a ReadyPages>,
    remaining: usize,
    zeros: usize,
    total: usize,
}

impl<'a> Iterator for ReadyPagesIter<'a> {
    type Item = &'a PageId;

    fn next(&mut self) -> Option<Self::Item> {
        const ZERO: PageId = PageId(0);
        loop {
            let current = self.current?;
            if self.zeros != 0 {
                self.zeros -= 1;
                self.total -= 1;
                return Some(&ZERO);
            }
            if self.remaining != 0 {
                self.remaining -= 1;
                self.total -= 1;
                return Some(&current.pages.as_ref().unwrap()[self.remaining]);
            }
            self.current = current.tail.as_deref();
            if let Some(tail) = self.current {
                self.remaining = tail.remaining;
                self.zeros = tail.trailing_zeros;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.total, Some(self.total))
    }
}

impl ExactSizeIterator for ReadyPagesIter<'_> {}

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
        Self::with_ready(high_water_mark, ReadyPages::shared(pages))
    }

    /// Carry a segmented committed loan into a writer without flattening it.
    pub fn with_ready(high_water_mark: u32, pages: ReadyPages) -> Self {
        Self {
            ready_to_use: pages,
            ..Self::new(high_water_mark)
        }
    }

    /// Prefers reusing reclaimed pages over incrementing the high water mark.
    pub fn allocate(&mut self) -> Result<PageId> {
        let id = if let Some(id) = self.ready_to_use.pop() {
            id
        } else {
            self.allocate_fresh()?
        };
        self.allocated_this_txn.push(id);
        Ok(id)
    }

    /// Allocate a page whose id is not zero.
    ///
    /// Overflow chains use page zero as their on-disk terminator, but page zero
    /// is also a real page that can re-enter the reclaim pool, so overflow
    /// writers take it from here and leave it for types that can represent it.
    pub fn allocate_nonzero(&mut self) -> Result<PageId> {
        if self.next_page_id == 0 {
            self.ready_to_use.trailing_zeros += 1;
            self.ready_to_use.total_len += 1;
            self.next_page_id = 1;
        }

        let id = match self.ready_to_use.pop_nonzero() {
            Some(page) => page,
            None => self.allocate_fresh()?,
        };
        self.allocated_this_txn.push(id);
        Ok(id)
    }

    fn allocate_fresh(&mut self) -> Result<PageId> {
        let id = PageId(self.next_page_id);
        // MAX is the invalid-page sentinel, and remains the high water mark
        // after allocating the final valid page. Reclaimed IDs still work.
        let Some(next_page_id) = self.next_page_id.checked_add(1) else {
            return Err(Error::PageIdExhausted);
        };
        self.next_page_id = next_page_id;
        Ok(id)
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

    /// Transfer the unconsumed loan cursor without copying its page IDs.
    pub fn take_ready(&mut self) -> ReadyPages {
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
