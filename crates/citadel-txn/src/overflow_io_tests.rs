use super::*;
use citadel_buffer::allocator::PageAllocator;
use citadel_core::types::{PageType, TxnId};
use citadel_page::overflow as pg_overflow;
use citadel_page::page::Page;
use rustc_hash::FxHashMap;

struct LocalLoader<'a> {
    pages: &'a mut FxHashMap<PageId, Page>,
}

impl citadel_buffer::cursor::PageMap for LocalLoader<'_> {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.pages.get(id)
    }
}

impl citadel_buffer::cursor::PageLoader for LocalLoader<'_> {
    fn ensure_loaded(&mut self, _id: PageId) -> Result<()> {
        Ok(())
    }
}

struct CancellingLoader<'a> {
    pages: &'a mut FxHashMap<PageId, Page>,
    token: CancelToken,
    loads: usize,
    cancel_after: usize,
}

impl citadel_buffer::cursor::PageMap for CancellingLoader<'_> {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.pages.get(id)
    }
}

impl citadel_buffer::cursor::PageLoader for CancellingLoader<'_> {
    fn ensure_loaded(&mut self, _id: PageId) -> Result<()> {
        self.loads += 1;
        if self.loads == self.cancel_after {
            self.token.cancel();
        }
        Ok(())
    }
}

fn build_chain(data: &[u8]) -> (FxHashMap<PageId, Page>, PageId, PageAllocator) {
    let mut pages: FxHashMap<PageId, Page> = FxHashMap::default();
    let mut alloc = PageAllocator::new(100);
    let txn_id = TxnId(1);
    let first = pg_overflow::write_chain(
        data,
        txn_id,
        || alloc.allocate(),
        |pid, page| {
            pages.insert(pid, page);
        },
    );
    (pages, first, alloc)
}

#[test]
fn read_chain_single_page() {
    let data = b"single-page payload".to_vec();
    let (mut pages, first, _) = build_chain(&data);
    let oref = OverflowRef {
        first_page: first,
        total_len: data.len() as u32,
    };
    let mut loader = LocalLoader { pages: &mut pages };
    let out = read_chain_value(&mut loader, &oref).unwrap();
    assert_eq!(out, data);
}

#[test]
fn read_chain_multi_page() {
    let data = vec![0xAB; pg_overflow::OVERFLOW_DATA_CAPACITY * 3 + 17];
    let (mut pages, first, _) = build_chain(&data);
    let oref = OverflowRef {
        first_page: first,
        total_len: data.len() as u32,
    };
    let mut loader = LocalLoader { pages: &mut pages };
    let out = read_chain_value(&mut loader, &oref).unwrap();
    assert_eq!(out, data);
}

#[test]
fn read_chain_length_mismatch_detected() {
    let data = vec![0; 100];
    let (mut pages, first, _) = build_chain(&data);
    let oref = OverflowRef {
        first_page: first,
        total_len: 999, // wrong length
    };
    let mut loader = LocalLoader { pages: &mut pages };
    let err = read_chain_value(&mut loader, &oref).unwrap_err();
    assert!(matches!(err, citadel_core::Error::CorruptOverflowChain(_)));
}

#[test]
fn free_chain_releases_pages() {
    let data = vec![0xCC; pg_overflow::OVERFLOW_DATA_CAPACITY * 2 + 5];
    let (mut pages, first, mut alloc) = build_chain(&data);
    let pages_before = pages.len();
    let mut loader = LocalLoader { pages: &mut pages };
    free_chain(&mut loader, &mut alloc, first).unwrap();
    assert_eq!(pages_before, 3);
}

#[test]
fn cancellable_read_stops_between_overflow_pages() {
    let data = vec![0xA5; pg_overflow::OVERFLOW_DATA_CAPACITY * 5 + 17];
    let (mut pages, first, _) = build_chain(&data);
    let oref = OverflowRef {
        first_page: first,
        total_len: data.len() as u32,
    };
    let token = CancelToken::new();
    let mut loader = CancellingLoader {
        pages: &mut pages,
        token: token.clone(),
        loads: 0,
        cancel_after: 2,
    };

    let err = read_chain_value_with_cancel(&mut loader, &oref, Some(&token)).unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert_eq!(loader.loads, 2, "the rest of the chain was still read");
}

#[test]
fn cancellable_free_preflights_before_changing_the_allocator() {
    let data = vec![0x5A; pg_overflow::OVERFLOW_DATA_CAPACITY * 5 + 17];
    let (mut pages, first, mut alloc) = build_chain(&data);
    let token = CancelToken::new();
    let mut loader = CancellingLoader {
        pages: &mut pages,
        token: token.clone(),
        loads: 0,
        cancel_after: 2,
    };

    let err = free_chain_with_cancel(&mut loader, &mut alloc, first, Some(&token)).unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert_eq!(loader.loads, 2, "the rest of the chain was still walked");
    assert_eq!(
        alloc.freed_count(),
        0,
        "a cancelled preflight must not free a prefix"
    );
}

#[test]
fn no_token_variants_keep_the_original_full_walk() {
    let data = vec![0x3C; pg_overflow::OVERFLOW_DATA_CAPACITY * 3 + 17];
    let (mut pages, first, mut alloc) = build_chain(&data);
    let oref = OverflowRef {
        first_page: first,
        total_len: data.len() as u32,
    };
    let page_count = pages.len();
    let mut loader = LocalLoader { pages: &mut pages };

    assert_eq!(
        read_chain_value_with_cancel(&mut loader, &oref, None).unwrap(),
        data
    );
    free_chain_with_cancel(&mut loader, &mut alloc, first, None).unwrap();
    assert_eq!(alloc.freed_count(), page_count);
}

#[test]
fn read_rejects_a_non_overflow_page_before_interpreting_its_body() {
    let data = vec![0x11; pg_overflow::OVERFLOW_DATA_CAPACITY + 1];
    let (mut pages, first, _) = build_chain(&data);
    pages.get_mut(&first).unwrap().set_page_type(PageType::Leaf);
    let oref = OverflowRef {
        first_page: first,
        total_len: data.len() as u32,
    };
    let mut loader = LocalLoader { pages: &mut pages };

    let err = read_chain_value(&mut loader, &oref).unwrap_err();

    assert!(matches!(err, Error::InvalidPageType(_, id) if id == first));
}

#[test]
fn read_rejects_oversized_page_data_without_a_slice_panic() {
    let data = vec![0x22; pg_overflow::OVERFLOW_DATA_CAPACITY + 1];
    let (mut pages, first, _) = build_chain(&data);
    pg_overflow::set_data_len(
        pages.get_mut(&first).unwrap(),
        (pg_overflow::OVERFLOW_DATA_CAPACITY + 1) as u32,
    );
    let oref = OverflowRef {
        first_page: first,
        total_len: data.len() as u32,
    };
    let mut loader = LocalLoader { pages: &mut pages };

    let err = read_chain_value(&mut loader, &oref).unwrap_err();

    assert!(matches!(err, Error::CorruptOverflowChain(_)), "got {err:?}");
}

#[test]
fn read_rejects_bytes_beyond_the_declared_total_before_growing_the_buffer() {
    let data = vec![0x33; pg_overflow::OVERFLOW_DATA_CAPACITY + 1];
    let (mut pages, first, _) = build_chain(&data);
    let oref = OverflowRef {
        first_page: first,
        total_len: (pg_overflow::OVERFLOW_DATA_CAPACITY - 1) as u32,
    };
    let mut loader = LocalLoader { pages: &mut pages };

    let err = read_chain_value(&mut loader, &oref).unwrap_err();

    assert!(matches!(err, Error::CorruptOverflowChain(_)), "got {err:?}");
}

#[test]
fn read_rejects_an_impossible_declared_length_before_allocating_it() {
    let data = vec![0x44; 1];
    let (mut pages, first, _) = build_chain(&data);
    let oref = OverflowRef {
        first_page: first,
        total_len: (citadel_core::MAX_VALUE_SIZE + 1) as u32,
    };
    let mut loader = LocalLoader { pages: &mut pages };

    let err = read_chain_value(&mut loader, &oref).unwrap_err();

    assert!(matches!(err, Error::CorruptOverflowChain(_)), "got {err:?}");
}

#[test]
fn read_rejects_a_cycle_instead_of_walking_forever() {
    let data = vec![0x55; pg_overflow::OVERFLOW_DATA_CAPACITY * 3];
    let (mut pages, first, _) = build_chain(&data);
    pg_overflow::set_next_page(pages.get_mut(&first).unwrap(), first);
    let oref = OverflowRef {
        first_page: first,
        total_len: data.len() as u32,
    };
    let mut loader = LocalLoader { pages: &mut pages };

    let err = read_chain_value(&mut loader, &oref).unwrap_err();

    assert!(matches!(err, Error::CorruptOverflowChain(_)), "got {err:?}");
}

#[test]
fn free_rejects_a_cycle_before_freeing_any_page() {
    let data = vec![0x66; pg_overflow::OVERFLOW_DATA_CAPACITY * 3];
    let (mut pages, first, mut alloc) = build_chain(&data);
    pg_overflow::set_next_page(pages.get_mut(&first).unwrap(), first);
    let mut loader = LocalLoader { pages: &mut pages };

    let err = free_chain(&mut loader, &mut alloc, first).unwrap_err();

    assert!(matches!(err, Error::CorruptOverflowChain(_)), "got {err:?}");
    assert_eq!(alloc.freed_count(), 0);
    assert!(alloc.freed_this_txn().is_empty());
}

fn _page_type_smoke() {
    let _ = PageType::Overflow;
}
