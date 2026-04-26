//! Overflow page operations for values exceeding the inline limit (1920 bytes).
//!
//! Overflow page format (within decrypted 8160-byte page body):
//!   Standard 64-byte header (page_type = Overflow), with:
//!     - right_child field (bytes 30..34): stores next_page_id (u32), 0 = last page
//!   Body (bytes 64..8160):
//!     [data_len: u32][data: up to 8092 bytes]
//!
//! The chain forms a singly-linked list via next_page_id.
//! Read: follow chain, concatenate data[0..data_len] from each page.

use crate::page::Page;
use citadel_core::types::PageId;
use citadel_core::PAGE_HEADER_SIZE;

/// Maximum data payload per overflow page.
/// 8160 (body) - 64 (header) - 4 (data_len field) = 8092 bytes.
pub const OVERFLOW_DATA_CAPACITY: usize = 8092;

/// Offset of data_len field within the page body (right after header).
const DATA_LEN_OFFSET: usize = PAGE_HEADER_SIZE; // 64

/// Offset where overflow data starts.
const DATA_OFFSET: usize = DATA_LEN_OFFSET + 4; // 68

pub fn next_page(page: &Page) -> PageId {
    page.right_child()
}

pub fn set_next_page(page: &mut Page, next: PageId) {
    page.set_right_child(next);
}

pub fn data_len(page: &Page) -> u32 {
    u32::from_le_bytes(
        page.data[DATA_LEN_OFFSET..DATA_LEN_OFFSET + 4]
            .try_into()
            .unwrap(),
    )
}

pub fn set_data_len(page: &mut Page, len: u32) {
    page.data[DATA_LEN_OFFSET..DATA_LEN_OFFSET + 4].copy_from_slice(&len.to_le_bytes());
}

pub fn read_data(page: &Page) -> &[u8] {
    let len = data_len(page) as usize;
    &page.data[DATA_OFFSET..DATA_OFFSET + len]
}

/// Write data into page, returns bytes written.
pub fn write_data(page: &mut Page, data: &[u8]) -> usize {
    let len = data.len().min(OVERFLOW_DATA_CAPACITY);
    page.data[DATA_OFFSET..DATA_OFFSET + len].copy_from_slice(&data[..len]);
    set_data_len(page, len as u32);
    len
}

pub fn pages_needed(total_len: usize) -> usize {
    if total_len == 0 {
        return 1; // at least one page even for empty data
    }
    total_len.div_ceil(OVERFLOW_DATA_CAPACITY)
}

#[cfg(test)]
#[path = "overflow_tests.rs"]
mod tests;
