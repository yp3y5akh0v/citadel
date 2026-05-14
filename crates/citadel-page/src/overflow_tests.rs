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

#[test]
fn write_chain_single_page() {
    let mut pages = std::collections::HashMap::new();
    let mut next_id = 100u32;
    let data = b"single page payload".to_vec();
    let first = write_chain(
        &data,
        TxnId(1),
        || {
            let id = PageId(next_id);
            next_id += 1;
            id
        },
        |pid, page| {
            pages.insert(pid, page);
        },
    );
    assert_eq!(first, PageId(100));
    assert_eq!(pages.len(), 1);
    let p = &pages[&first];
    assert_eq!(read_data(p), &data[..]);
    assert_eq!(next_page(p), PageId(0));
}

#[test]
fn write_chain_multi_page_links() {
    let mut pages = std::collections::HashMap::new();
    let mut next_id = 200u32;
    let data = vec![0x5A; OVERFLOW_DATA_CAPACITY * 3 + 7];
    let first = write_chain(
        &data,
        TxnId(2),
        || {
            let id = PageId(next_id);
            next_id += 1;
            id
        },
        |pid, page| {
            pages.insert(pid, page);
        },
    );
    assert_eq!(pages.len(), 4);
    let mut cur = first;
    let mut acc = Vec::new();
    while cur.as_u32() != 0 {
        let p = &pages[&cur];
        acc.extend_from_slice(read_data(p));
        cur = next_page(p);
    }
    assert_eq!(acc, data);
}
