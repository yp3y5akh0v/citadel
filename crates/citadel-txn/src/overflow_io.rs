//! Overflow-chain read + free walks, layered over `citadel-page::overflow`.

use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::cursor::PageLoader;
use citadel_core::types::PageId;
use citadel_core::{Error, Result};
use citadel_page::leaf_node::OverflowRef;
use citadel_page::overflow;

pub(crate) fn read_chain_value(loader: &mut dyn PageLoader, oref: &OverflowRef) -> Result<Vec<u8>> {
    let total = oref.total_len as usize;
    let mut buf = Vec::with_capacity(total);
    let mut cur = oref.first_page;
    while cur.as_u32() != 0 {
        loader.ensure_loaded(cur)?;
        let page = loader.get_page(&cur).ok_or(Error::PageOutOfBounds(cur))?;
        buf.extend_from_slice(overflow::read_data(page));
        cur = overflow::next_page(page);
    }
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
    let mut cur = first;
    while cur.as_u32() != 0 {
        loader.ensure_loaded(cur)?;
        let next = {
            let page = loader.get_page(&cur).ok_or(Error::PageOutOfBounds(cur))?;
            overflow::next_page(page)
        };
        alloc.free(cur);
        cur = next;
    }
    Ok(())
}

#[cfg(test)]
#[path = "overflow_io_tests.rs"]
mod tests;
