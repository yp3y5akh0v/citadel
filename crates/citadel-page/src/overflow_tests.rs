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
