//! Overflow-chain read + free walks, layered over `citadel-page::overflow`.

use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::cursor::PageLoader;
use citadel_core::types::{PageId, PageType};
use citadel_core::{CancelToken, Error, Result, MAX_VALUE_SIZE};
use citadel_page::leaf_node::OverflowRef;
use citadel_page::overflow;
use rustc_hash::FxHashSet;

use crate::ReadBudget;

#[cfg(test)]
pub(crate) fn read_chain_value(loader: &mut dyn PageLoader, oref: &OverflowRef) -> Result<Vec<u8>> {
    read_chain_value_checked(loader, oref, None, || Ok(()))
}

/// Read an overflow value, observing `cancel` once per page and once after the
/// final page. The `None` lane calls the original no-check walk, so transactions
/// without cancellation enabled do not pay an atomic load per overflow page.
#[cfg(test)]
pub(crate) fn read_chain_value_with_cancel(
    loader: &mut dyn PageLoader,
    oref: &OverflowRef,
    cancel: Option<&CancelToken>,
) -> Result<Vec<u8>> {
    match cancel {
        Some(token) => read_chain_value_checked(loader, oref, None, || token.check()),
        None => read_chain_value(loader, oref),
    }
}

pub(crate) fn read_chain_value_with_budget(
    loader: &mut dyn PageLoader,
    oref: &OverflowRef,
    cancel: Option<&CancelToken>,
    budget: Option<&ReadBudget>,
) -> Result<Vec<u8>> {
    read_chain_value_checked(loader, oref, budget, || match cancel {
        Some(token) => token.check(),
        None => Ok(()),
    })
}

fn read_chain_value_checked<F>(
    loader: &mut dyn PageLoader,
    oref: &OverflowRef,
    budget: Option<&ReadBudget>,
    mut check: F,
) -> Result<Vec<u8>>
where
    F: FnMut() -> Result<()>,
{
    let total = oref.total_len as usize;
    if total > MAX_VALUE_SIZE {
        return Err(Error::CorruptOverflowChain(format!(
            "declared length {total} exceeds maximum {MAX_VALUE_SIZE}"
        )));
    }
    check()?;
    if let Some(budget) = budget {
        budget.try_charge(total)?;
    }
    let mut buf = Vec::with_capacity(total);
    let mut cur = oref.first_page;
    let mut pages_seen = 0usize;
    let max_pages = overflow::pages_needed(total);
    while cur.as_u32() != 0 {
        check()?;
        pages_seen += 1;
        if pages_seen > max_pages {
            return Err(Error::CorruptOverflowChain(format!(
                "chain has more than {max_pages} pages for {total} bytes"
            )));
        }
        loader.ensure_loaded(cur)?;
        let page = loader.get_page(&cur).ok_or(Error::PageOutOfBounds(cur))?;
        if page.page_type() != Some(PageType::Overflow) {
            return Err(Error::InvalidPageType(page.page_type_raw(), cur));
        }
        let data_len = overflow::data_len(page) as usize;
        if data_len > overflow::OVERFLOW_DATA_CAPACITY {
            return Err(Error::CorruptOverflowChain(format!(
                "page {cur} declares {data_len} data bytes (capacity {})",
                overflow::OVERFLOW_DATA_CAPACITY
            )));
        }
        let remaining = total.saturating_sub(buf.len());
        if data_len > remaining {
            return Err(Error::CorruptOverflowChain(format!(
                "page {cur} exceeds declared total length {total}"
            )));
        }
        buf.extend_from_slice(overflow::read_data(page));
        cur = overflow::next_page(page);
    }
    // Without this a one-page value has no observation point after its
    // potentially blocking I/O.
    check()?;
    if buf.len() != total {
        return Err(Error::CorruptOverflowChain(format!(
            "chain length mismatch (expected {} bytes, got {})",
            total,
            buf.len()
        )));
    }
    Ok(buf)
}

pub(crate) fn free_chain(
    loader: &mut dyn PageLoader,
    alloc: &mut PageAllocator,
    first: PageId,
) -> Result<()> {
    free_chain_checked(loader, alloc, first, || Ok(()))
}

/// Free an overflow chain, observing `cancel` once per page and at the exit.
/// Callers must make their transaction uncommittable if this returns an error:
/// earlier pages may already have been added to the pending-free set.
pub(crate) fn free_chain_with_cancel(
    loader: &mut dyn PageLoader,
    alloc: &mut PageAllocator,
    first: PageId,
    cancel: Option<&CancelToken>,
) -> Result<()> {
    match cancel {
        Some(token) => free_chain_checked(loader, alloc, first, || token.check()),
        None => free_chain(loader, alloc, first),
    }
}

/// Load and validate an overflow chain without changing the allocator.
///
/// Destructive tree operations use this preflight so a missing or malformed
/// later page is reported before any page in the tree enters pending-free.
pub(crate) fn collect_chain_pages_with_cancel(
    loader: &mut dyn PageLoader,
    first: PageId,
    cancel: Option<&CancelToken>,
) -> Result<Vec<PageId>> {
    match cancel {
        Some(token) => collect_chain_pages_checked(loader, first, || token.check()),
        None => collect_chain_pages_checked(loader, first, || Ok(())),
    }
}

fn collect_chain_pages_checked<F>(
    loader: &mut dyn PageLoader,
    first: PageId,
    mut check: F,
) -> Result<Vec<PageId>>
where
    F: FnMut() -> Result<()>,
{
    let mut cur = first;
    let mut visited = FxHashSet::default();
    let mut pages = Vec::new();
    while cur.as_u32() != 0 {
        check()?;
        if !visited.insert(cur) {
            return Err(Error::CorruptOverflowChain(format!(
                "cycle or duplicate reference to page {cur}"
            )));
        }
        pages.push(cur);
        loader.ensure_loaded(cur)?;
        let page = loader.get_page(&cur).ok_or(Error::PageOutOfBounds(cur))?;
        if page.page_type() != Some(PageType::Overflow) {
            return Err(Error::InvalidPageType(page.page_type_raw(), cur));
        }
        cur = overflow::next_page(page);
    }
    check()?;
    Ok(pages)
}

fn free_chain_checked<F>(
    loader: &mut dyn PageLoader,
    alloc: &mut PageAllocator,
    first: PageId,
    mut check: F,
) -> Result<()>
where
    F: FnMut() -> Result<()>,
{
    let pages = collect_chain_pages_checked(loader, first, &mut check)?;
    for page_id in pages {
        check()?;
        alloc.free(page_id);
    }
    check()?;
    Ok(())
}

#[cfg(test)]
#[path = "overflow_io_tests.rs"]
mod tests;
