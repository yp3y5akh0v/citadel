//! Page owners retained by a writer, with static map access for tree kernels.

use std::collections::hash_map::Entry;
use std::sync::Arc;

use citadel_buffer::cursor::{MutablePageMap, PageMap};
use citadel_core::PageId;
use citadel_page::page::Page;
use rustc_hash::FxHashMap;

/// Writer page references plus reusable pointer buckets. Mutable access isolates
/// shared allocations, but does not allocate a new physical page ID: callers
/// must retain the existing transaction-ID CoW rule before changing a page.
#[derive(Default)]
pub(crate) struct OwnedPages {
    pages: FxHashMap<PageId, Arc<Page>>,
}

impl PageMap for OwnedPages {
    #[inline]
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.pages.get(id).map(Arc::as_ref)
    }
}

impl MutablePageMap for OwnedPages {
    #[inline]
    fn get_page_mut(&mut self, id: &PageId) -> Option<&mut Page> {
        self.pages.get_mut(id).map(Arc::make_mut)
    }

    #[inline]
    fn insert_page(&mut self, id: PageId, page: Page) {
        self.pages.insert(id, Arc::new(page));
    }

    #[inline]
    fn remove_page(&mut self, id: &PageId) {
        self.pages.remove(id);
    }
}

impl OwnedPages {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            pages: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn insert_shared(&mut self, id: PageId, page: Arc<Page>) {
        self.pages.insert(id, page);
    }

    #[cfg(test)]
    pub(crate) fn get_shared(&self, id: &PageId) -> Option<&Arc<Page>> {
        self.pages.get(id)
    }

    /// A loader error leaves an absent entry absent. The loader is responsible
    /// for authentication, validation, identity and snapshot bounds as today.
    #[inline]
    pub(crate) fn get_or_try_insert_shared<E>(
        &mut self,
        id: PageId,
        load: impl FnOnce() -> Result<Arc<Page>, E>,
    ) -> Result<&Page, E> {
        let page = match self.pages.entry(id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(load()?),
        };
        Ok(Arc::as_ref(page))
    }

    /// Transfer a complete immutable allocation to the committed pool without
    /// copying its body. This is deliberately separate from discard removal.
    #[inline]
    pub(crate) fn remove_shared(&mut self, id: &PageId) -> Option<Arc<Page>> {
        self.pages.remove(id)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&PageId, &Page)> {
        self.pages.iter().map(|(id, page)| (id, page.as_ref()))
    }

    pub(crate) fn len(&self) -> usize {
        self.pages.len()
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.pages.capacity()
    }

    /// Release every page reference before parking only the bucket allocation.
    pub(crate) fn clear(&mut self) {
        self.pages.clear();
    }
}

#[cfg(test)]
#[path = "owned_pages_tests.rs"]
mod tests;
