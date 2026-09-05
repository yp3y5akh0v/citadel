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

#[test]
fn allocator_checkpoints_share_the_immutable_reclaimed_batch() {
    let pages = Arc::new((1..=4096).map(PageId).collect::<Vec<_>>());
    let mut alloc = PageAllocator::with_ready_pages(5000, Arc::clone(&pages));
    let before = alloc.checkpoint();
    assert_eq!(alloc.allocate(), PageId(4096));
    let after = alloc.checkpoint();
    for ready in [
        &alloc.ready_to_use,
        &before.ready_to_use,
        &after.ready_to_use,
    ] {
        assert!(Arc::ptr_eq(ready.pages.as_ref().unwrap(), &pages));
    }
    assert_eq!(before.ready_to_use.len(), 4096);
    assert_eq!(after.ready_to_use.len(), 4095);
    assert_eq!(pages.len(), 4096);
}

#[test]
fn checkpoint_restores_reclaimed_pages_hwm_and_retirement_log() {
    let pages = Arc::new(vec![PageId(3), PageId(0), PageId(7)]);
    let mut alloc = PageAllocator::with_ready_pages(100, Arc::clone(&pages));
    assert_eq!(alloc.allocate(), PageId(7));
    alloc.free(PageId(90));
    let snapshot = alloc.checkpoint();
    for _ in 0..8 {
        assert_eq!(alloc.allocate_nonzero(), PageId(3));
        assert_eq!(alloc.allocate_nonzero(), PageId(100));
        assert_eq!(alloc.allocate(), PageId(0));
        alloc.free(PageId(3));
        let inner = alloc.checkpoint();
        assert_eq!(alloc.allocate(), PageId(101));
        alloc.restore(inner);
        assert_eq!(alloc.high_water_mark(), 101);
        alloc.restore(snapshot.clone());
        assert_eq!(alloc.high_water_mark(), 100);
        assert_eq!(alloc.allocated_this_txn(), &[PageId(7)]);
        assert_eq!(alloc.freed_this_txn(), &[PageId(90)]);
        assert_eq!(alloc.ready_count(), 2);
    }
    assert_eq!(alloc.take_ready_to_use(), vec![PageId(3), PageId(0)]);
    assert_eq!(alloc.ready_count(), 0);
    assert_eq!(*pages, vec![PageId(3), PageId(0), PageId(7)]);
}

#[test]
fn checkpoint_restores_the_initial_nonzero_allocation() {
    let mut alloc = PageAllocator::new(0);
    let snapshot = alloc.checkpoint();
    for _ in 0..4 {
        assert_eq!(alloc.allocate_nonzero(), PageId(1));
        let after_nonzero = alloc.checkpoint();
        assert_eq!(alloc.allocate(), PageId(0));
        alloc.restore(after_nonzero);
        assert_eq!(alloc.allocate(), PageId(0));
        alloc.restore(snapshot.clone());
        assert_eq!(alloc.high_water_mark(), 0);
        assert_eq!(alloc.ready_count(), 0);
        assert!(alloc.allocated_this_txn().is_empty());
    }
}

#[test]
fn ready_pages_match_vec_allocation_order_through_restore_append_and_drain() {
    let mut actual = ReadyPages::default();
    let mut expected = Vec::new();
    let mut snapshots = Vec::new();
    let mut random = 0x8df0_981a_cbd4_2397u64;
    for step in 0..5000 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        match (random >> 32) % 6 {
            0 => assert_eq!(actual.pop(), expected.pop()),
            1 => {
                let next = expected
                    .iter()
                    .rposition(|page: &PageId| page.as_u32() != 0)
                    .map(|index| expected.swap_remove(index));
                assert_eq!(actual.pop_nonzero(), next);
            }
            2 => {
                let pages = vec![PageId(step + 1), PageId(0), PageId(0)];
                actual.append(pages.clone());
                expected.extend(pages);
            }
            3 if snapshots.len() < 8 => snapshots.push((actual.clone(), expected.clone())),
            4 if !snapshots.is_empty() => {
                let index = random as usize % snapshots.len();
                (actual, expected) = snapshots[index].clone();
                snapshots.truncate(index + 1);
            }
            5 => assert_eq!(actual.take(), std::mem::take(&mut expected)),
            _ => {}
        }
        assert_eq!(actual.len(), expected.len(), "step {step}");
        assert_eq!(actual.clone().take(), expected, "step {step}");
    }
}
