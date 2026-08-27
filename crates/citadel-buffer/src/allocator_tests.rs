use super::*;

#[test]
fn allocate_from_hwm() {
    let mut alloc = PageAllocator::new(0);
    assert_eq!(alloc.allocate(), PageId(0));
    assert_eq!(alloc.allocate(), PageId(1));
    assert_eq!(alloc.allocate(), PageId(2));
    assert_eq!(alloc.high_water_mark(), 3);
}

#[test]
fn allocate_from_ready_to_use() {
    let mut alloc = PageAllocator::new(10);
    alloc.add_ready_to_use(vec![PageId(3), PageId(7)]);
    assert_eq!(alloc.allocate(), PageId(7));
    assert_eq!(alloc.allocate(), PageId(3));
    assert_eq!(alloc.allocate(), PageId(10));
}

#[test]
fn nonzero_allocation_preserves_page_zero_for_other_page_types() {
    let mut alloc = PageAllocator::new(0);

    assert_eq!(alloc.allocate_nonzero(), PageId(1));
    assert_eq!(alloc.allocate(), PageId(0));
    assert_eq!(alloc.high_water_mark(), 2);
}

#[test]
fn nonzero_allocation_skips_reclaimed_page_zero() {
    let mut alloc = PageAllocator::new(10);
    alloc.add_ready_to_use(vec![PageId(7), PageId(0)]);

    assert_eq!(alloc.allocate_nonzero(), PageId(7));
    assert_eq!(alloc.allocate(), PageId(0));
}

#[test]
fn free_and_commit() {
    let mut alloc = PageAllocator::new(5);
    alloc.free(PageId(1));
    alloc.free(PageId(3));
    assert_eq!(alloc.freed_count(), 2);

    let freed = alloc.commit();
    assert_eq!(freed.len(), 2);
    assert_eq!(alloc.freed_count(), 0);
}

#[test]
fn rollback_clears_freed() {
    let mut alloc = PageAllocator::new(5);
    alloc.free(PageId(1));
    alloc.free(PageId(3));
    alloc.rollback();
    assert_eq!(alloc.freed_count(), 0);
}
