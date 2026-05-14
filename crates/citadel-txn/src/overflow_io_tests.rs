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
    matches!(err, citadel_core::Error::CorruptOverflowChain(_));
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

fn _page_type_smoke() {
    let _ = PageType::Overflow;
}
