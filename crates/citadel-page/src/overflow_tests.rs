use super::*;
use citadel_core::types::{PageType, TxnId};
use citadel_core::{CancelToken, Error};

#[test]
fn allocation_failure_stops_before_emitting_any_chain_pages() {
    let data = vec![0x5a; OVERFLOW_DATA_CAPACITY * 2 + 1];
    let token = CancelToken::new();
    for cancel in [None, Some(&token)] {
        for fail_at in 0..pages_needed(data.len()) {
            let mut allocated = 0;
            let mut emitted = 0;
            let result = write_chain_with_cancel(
                &data,
                TxnId(7),
                || {
                    if allocated == fail_at {
                        return Err(Error::PageIdExhausted);
                    }
                    allocated += 1;
                    Ok(PageId(allocated as u32))
                },
                |_, _| emitted += 1,
                cancel,
            );
            assert!(matches!(result, Err(Error::PageIdExhausted)));
            assert_eq!(allocated, fail_at);
            assert_eq!(emitted, 0);
        }
    }
}

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
            Ok(id)
        },
        |pid, page| {
            pages.insert(pid, page);
        },
    )
    .unwrap();
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
            Ok(id)
        },
        |pid, page| {
            pages.insert(pid, page);
        },
    )
    .unwrap();
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

#[test]
fn cancellable_write_stops_during_the_allocation_pass() {
    let data = vec![0x71; OVERFLOW_DATA_CAPACITY * 5 + 1];
    let token = CancelToken::new();
    let cancel_from_alloc = token.clone();
    let mut allocated = 0u32;
    let mut pages = std::collections::HashMap::new();

    let err = write_chain_with_cancel(
        &data,
        TxnId(3),
        || {
            allocated += 1;
            if allocated == 2 {
                cancel_from_alloc.cancel();
            }
            Ok(PageId(300 + allocated))
        },
        |pid, page| {
            pages.insert(pid, page);
        },
        Some(&token),
    )
    .unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert_eq!(allocated, 2);
    assert!(
        pages.is_empty(),
        "construction ran after allocation cancelled"
    );
}

#[test]
fn cancellable_write_stops_during_the_page_build_pass() {
    let data = vec![0x72; OVERFLOW_DATA_CAPACITY * 5 + 1];
    let token = CancelToken::new();
    let cancel_from_sink = token.clone();
    let mut allocated = 0u32;
    let mut sunk = 0usize;
    let mut pages = std::collections::HashMap::new();

    let err = write_chain_with_cancel(
        &data,
        TxnId(4),
        || {
            allocated += 1;
            Ok(PageId(400 + allocated))
        },
        |pid, page| {
            sunk += 1;
            pages.insert(pid, page);
            if sunk == 2 {
                cancel_from_sink.cancel();
            }
        },
        Some(&token),
    )
    .unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert_eq!(allocated as usize, pages_needed(data.len()));
    assert_eq!(sunk, 2);
    assert_eq!(pages.len(), 2);
}

#[test]
fn no_token_write_is_byte_for_byte_equivalent_to_the_fast_path() {
    let data = vec![0x73; OVERFLOW_DATA_CAPACITY * 3 + 19];
    let mut fast_pages = std::collections::HashMap::new();
    let mut checked_pages = std::collections::HashMap::new();
    let mut fast_id = 500u32;
    let mut checked_id = 500u32;

    let fast_first = write_chain(
        &data,
        TxnId(5),
        || {
            let id = PageId(fast_id);
            fast_id += 1;
            Ok(id)
        },
        |pid, page| {
            fast_pages.insert(pid, page);
        },
    )
    .unwrap();
    let checked_first = write_chain_with_cancel(
        &data,
        TxnId(5),
        || {
            let id = PageId(checked_id);
            checked_id += 1;
            Ok(id)
        },
        |pid, page| {
            checked_pages.insert(pid, page);
        },
        None,
    )
    .unwrap();

    assert_eq!(checked_first, fast_first);
    assert_eq!(checked_pages.len(), fast_pages.len());
    for (id, expected) in fast_pages {
        assert_eq!(checked_pages[&id].data, expected.data, "page {id}");
    }
}
