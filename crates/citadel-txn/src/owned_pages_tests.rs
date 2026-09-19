use super::*;
use citadel_core::{PageType, TxnId};

fn page(id: u32) -> Page {
    Page::new_for_write(PageId(id), PageType::Leaf, TxnId(1))
}

#[test]
fn acquisition_is_shared_and_failed_load_is_not_cached() {
    let mut pages = OwnedPages::default();
    assert!(pages
        .get_or_try_insert_shared(PageId(1), || Err::<Arc<Page>, _>("missing"))
        .is_err());
    assert_eq!(pages.len(), 0);
    let source = Arc::new(page(1));
    pages
        .get_or_try_insert_shared(PageId(1), || Ok::<_, ()>(Arc::clone(&source)))
        .unwrap();
    assert!(Arc::ptr_eq(&source, pages.get_shared(&PageId(1)).unwrap()));
    pages
        .get_or_try_insert_shared(PageId(1), || panic!("cached entry must not load"))
        .map_err(|_: ()| ())
        .unwrap();
}

#[test]
fn mutable_access_isolates_memory_but_does_not_replace_version_cow() {
    let mut pages = OwnedPages::default();
    let source = Arc::new(page(1));
    pages.insert_shared(PageId(1), Arc::clone(&source));
    pages.get_page_mut(&PageId(1)).unwrap().data[200] = 0x52;
    assert_eq!(source.data[200], 0);
    assert_eq!(source.txn_id(), TxnId(1));
    assert_eq!(pages.get_page(&PageId(1)).unwrap().txn_id(), TxnId(1));
    assert_eq!(pages.get_page(&PageId(1)).unwrap().page_id(), PageId(1));
    assert!(!Arc::ptr_eq(&source, pages.get_shared(&PageId(1)).unwrap()));
}

#[test]
fn publication_transfers_allocation_and_clear_releases_sources() {
    let mut pages = OwnedPages::default();
    pages.insert_page(PageId(2), page(2));
    let pointer = Arc::as_ptr(pages.get_shared(&PageId(2)).unwrap());
    let published = pages.remove_shared(&PageId(2)).unwrap();
    assert_eq!(Arc::as_ptr(&published), pointer);
    assert_eq!(Arc::strong_count(&published), 1);
    let source = Arc::new(page(3));
    let weak = Arc::downgrade(&source);
    pages.insert_shared(PageId(3), source);
    let capacity = pages.capacity();
    pages.clear();
    assert_eq!(pages.len(), 0);
    assert_eq!(pages.capacity(), capacity);
    assert!(weak.upgrade().is_none());
}

#[test]
fn discard_does_not_clone_a_shared_page() {
    let mut pages = OwnedPages::default();
    let source = Arc::new(page(1));
    pages.insert_shared(PageId(1), Arc::clone(&source));
    pages.remove_page(&PageId(1));
    assert_eq!(Arc::strong_count(&source), 1);
    assert_eq!(pages.len(), 0);
}

#[test]
fn unique_old_generation_requires_physical_cow_but_current_generation_does_not() {
    use citadel_buffer::allocator::PageAllocator;
    use citadel_buffer::btree;

    let mut pages = OwnedPages::default();
    let mut alloc = PageAllocator::new(2);
    pages.insert_page(PageId(1), page(1));
    let old_pointer = Arc::as_ptr(pages.get_shared(&PageId(1)).unwrap());
    assert_eq!(Arc::strong_count(pages.get_shared(&PageId(1)).unwrap()), 1);
    let current = btree::cow_page(&mut pages, &mut alloc, PageId(1), TxnId(2)).unwrap();
    assert_ne!(current, PageId(1));
    assert_eq!(
        Arc::as_ptr(pages.get_shared(&PageId(1)).unwrap()),
        old_pointer
    );
    assert_eq!(pages.get_page(&PageId(1)).unwrap().txn_id(), TxnId(1));
    assert_eq!(pages.get_page(&current).unwrap().page_id(), current);
    assert_eq!(pages.get_page(&current).unwrap().txn_id(), TxnId(2));
    let current_pointer = Arc::as_ptr(pages.get_shared(&current).unwrap());
    assert_eq!(
        btree::cow_page(&mut pages, &mut alloc, current, TxnId(2)).unwrap(),
        current
    );
    pages.get_page_mut(&current).unwrap().data[200] = 0x5a;
    assert_eq!(
        Arc::as_ptr(pages.get_shared(&current).unwrap()),
        current_pointer
    );
    assert_eq!(pages.get_page(&PageId(1)).unwrap().data[200], 0);
    assert_eq!(alloc.freed_this_txn(), &[PageId(1)]);
}
