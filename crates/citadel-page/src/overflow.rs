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
mod tests {
    use super::*;
    use citadel_core::types::{PageType, TxnId};

    #[test]
    fn overflow_page_write_read() {
        let mut page = Page::new(PageId(10), PageType::Overflow, TxnId(1));
        let data = b"overflow value data here";
        let written = write_data(&mut page, data);
        assert_eq!(written, data.len());
        assert_eq!(data_len(&page), data.len() as u32);
        assert_eq!(read_data(&page), data);
    }

    #[test]
    fn overflow_chain_links() {
        let mut page = Page::new(PageId(10), PageType::Overflow, TxnId(1));
        set_next_page(&mut page, PageId(11));
        assert_eq!(next_page(&page), PageId(11));

        // Last page in chain
        let page2 = Page::new(PageId(11), PageType::Overflow, TxnId(1));
        assert_eq!(next_page(&page2), PageId(0));
    }

    #[test]
    fn overflow_max_capacity() {
        let mut page = Page::new(PageId(10), PageType::Overflow, TxnId(1));
        let data = vec![0xAB; OVERFLOW_DATA_CAPACITY];
        let written = write_data(&mut page, &data);
        assert_eq!(written, OVERFLOW_DATA_CAPACITY);
        assert_eq!(read_data(&page), &data[..]);
    }

    #[test]
    fn pages_needed_calculation() {
        assert_eq!(pages_needed(0), 1);
        assert_eq!(pages_needed(1), 1);
        assert_eq!(pages_needed(OVERFLOW_DATA_CAPACITY), 1);
        assert_eq!(pages_needed(OVERFLOW_DATA_CAPACITY + 1), 2);
        assert_eq!(pages_needed(OVERFLOW_DATA_CAPACITY * 3), 3);
        assert_eq!(pages_needed(OVERFLOW_DATA_CAPACITY * 3 + 500), 4);
    }

    #[test]
    fn overflow_truncates_to_capacity() {
        let mut page = Page::new(PageId(10), PageType::Overflow, TxnId(1));
        let data = vec![0xFF; OVERFLOW_DATA_CAPACITY + 100];
        let written = write_data(&mut page, &data);
        assert_eq!(written, OVERFLOW_DATA_CAPACITY);
    }
}
