use super::*;

#[test]
fn exhaustion_preserves_the_final_valid_id_and_checkpoint() {
    for nonzero in [false, true] {
        let mut alloc = PageAllocator::new(u32::MAX - 1);
        let checkpoint = alloc.checkpoint();
        for _ in 0..2 {
            let last = if nonzero {
                alloc.allocate_nonzero()
            } else {
                alloc.allocate()
            }
            .unwrap();
            assert_eq!(last, PageId(u32::MAX - 1));
            assert!(last.is_valid());
            for _ in 0..2 {
                let result = if nonzero {
                    alloc.allocate_nonzero()
                } else {
                    alloc.allocate()
                };
                assert!(matches!(result, Err(Error::PageIdExhausted)));
                assert_eq!(alloc.high_water_mark(), u32::MAX);
                assert_eq!(alloc.allocated_this_txn(), &[last]);
                assert_eq!(alloc.ready_count(), 0);
                assert_eq!(alloc.freed_count(), 0);
            }
            alloc.restore(checkpoint.clone());
            assert!(alloc.allocated_this_txn().is_empty());
        }
    }
}

#[test]
fn exhaustion_still_allows_reclaimed_pages_and_preserves_skipped_zero() {
    let mut alloc = PageAllocator::with_ready_pages(
        u32::MAX,
        Arc::new(vec![PageId(7), PageId(0), PageId(3), PageId(0)]),
    );
    assert_eq!(alloc.allocate_nonzero().unwrap(), PageId(3));
    assert_eq!(alloc.allocate_nonzero().unwrap(), PageId(7));
    let checkpoint = alloc.checkpoint();
    for _ in 0..2 {
        assert!(matches!(
            alloc.allocate_nonzero(),
            Err(Error::PageIdExhausted)
        ));
        assert_eq!(alloc.ready_count(), 2);
        assert_eq!(alloc.allocated_this_txn(), &[PageId(3), PageId(7)]);
        assert_eq!(alloc.allocate().unwrap(), PageId(0));
        assert_eq!(alloc.allocate().unwrap(), PageId(0));
        assert!(matches!(alloc.allocate(), Err(Error::PageIdExhausted)));
        assert_eq!(alloc.high_water_mark(), u32::MAX);
        alloc.restore(checkpoint.clone());
    }
}

#[test]
fn allocate_from_hwm() {
    let mut alloc = PageAllocator::new(0);
    assert_eq!(alloc.allocate().unwrap(), PageId(0));
    assert_eq!(alloc.allocate().unwrap(), PageId(1));
    assert_eq!(alloc.allocate().unwrap(), PageId(2));
    assert_eq!(alloc.high_water_mark(), 3);
}

#[test]
fn allocate_from_ready_to_use() {
    let mut alloc = PageAllocator::new(10);
    alloc.add_ready_to_use(vec![PageId(3), PageId(7)]);
    assert_eq!(alloc.allocate().unwrap(), PageId(7));
    assert_eq!(alloc.allocate().unwrap(), PageId(3));
    assert_eq!(alloc.allocate().unwrap(), PageId(10));
}

#[test]
fn nonzero_allocation_preserves_page_zero_for_other_page_types() {
    let mut alloc = PageAllocator::new(0);

    assert_eq!(alloc.allocate_nonzero().unwrap(), PageId(1));
    assert_eq!(alloc.allocate().unwrap(), PageId(0));
    assert_eq!(alloc.high_water_mark(), 2);
}

#[test]
fn nonzero_allocation_skips_reclaimed_page_zero() {
    let mut alloc = PageAllocator::new(10);
    alloc.add_ready_to_use(vec![PageId(7), PageId(0)]);

    assert_eq!(alloc.allocate_nonzero().unwrap(), PageId(7));
    assert_eq!(alloc.allocate().unwrap(), PageId(0));
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
    assert_eq!(alloc.allocate().unwrap(), PageId(4096));
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
    assert_eq!(alloc.allocate().unwrap(), PageId(7));
    alloc.free(PageId(90));
    let snapshot = alloc.checkpoint();
    for _ in 0..8 {
        assert_eq!(alloc.allocate_nonzero().unwrap(), PageId(3));
        assert_eq!(alloc.allocate_nonzero().unwrap(), PageId(100));
        assert_eq!(alloc.allocate().unwrap(), PageId(0));
        alloc.free(PageId(3));
        let inner = alloc.checkpoint();
        assert_eq!(alloc.allocate().unwrap(), PageId(101));
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
        assert_eq!(alloc.allocate_nonzero().unwrap(), PageId(1));
        let after_nonzero = alloc.checkpoint();
        assert_eq!(alloc.allocate().unwrap(), PageId(0));
        alloc.restore(after_nonzero);
        assert_eq!(alloc.allocate().unwrap(), PageId(0));
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

#[test]
fn segmented_loans_share_batches_and_restore_across_boundaries() {
    let original = Arc::new((1..=4096).map(PageId).collect::<Vec<_>>());
    let mut loan = ReadyPages::shared(Arc::clone(&original));
    assert_eq!(loan.pop(), Some(PageId(4096)));
    loan.prepend_pop_order(vec![PageId(5001), PageId(0), PageId(5002)]);
    let checkpoint = loan.clone();
    for _ in 0..3 {
        assert_eq!(loan.pop_nonzero(), Some(PageId(5001)));
        assert_eq!(loan.pop_nonzero(), Some(PageId(5002)));
        assert_eq!(loan.pop_nonzero(), Some(PageId(4095)));
        assert_eq!(loan.pop(), Some(PageId(0)));
        assert!(Arc::ptr_eq(loan.pages.as_ref().unwrap(), &original));
        loan = checkpoint.clone();
    }
    assert_eq!(
        checkpoint.iter().take(4).copied().collect::<Vec<_>>(),
        vec![PageId(5001), PageId(0), PageId(5002), PageId(4095)]
    );
    let mut allocator = PageAllocator::with_ready(6000, loan);
    assert_eq!(allocator.allocate().unwrap(), PageId(5001));
    let mut remainder = allocator.take_ready();
    assert_eq!(allocator.ready_count(), 0);
    assert_eq!(remainder.pop(), Some(PageId(0)));
    assert_eq!(remainder.pop(), Some(PageId(5002)));
    assert!(Arc::ptr_eq(remainder.pages.as_ref().unwrap(), &original));
    assert_eq!(original.len(), 4096);
}

#[test]
fn segmented_loans_retain_zeroes_and_report_exact_iteration_length() {
    let mut loan = ReadyPages::default();
    loan.prepend_pop_order(vec![PageId(0), PageId(2), PageId(0)]);
    loan.prepend_pop_order(vec![PageId(0), PageId(0)]);
    assert_eq!(loan.pop_nonzero(), Some(PageId(2)));
    for _ in 0..3 {
        assert_eq!(loan.pop_nonzero(), None);
        assert_eq!(loan.len(), 4);
        let mut iter = loan.iter();
        for remaining in (1..=4).rev() {
            assert_eq!(iter.len(), remaining);
            assert_eq!(iter.next(), Some(&PageId(0)));
        }
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
    }
    loan.prepend_pop_order(vec![PageId(7)]);
    assert_eq!(loan.last(), Some(PageId(7)));
    assert_eq!(loan.pop(), Some(PageId(7)));
    for _ in 0..4 {
        assert_eq!(loan.pop(), Some(PageId(0)));
    }
    assert!(loan.is_empty());
    assert_eq!(loan.last(), None);
    assert_eq!(loan.pop(), None);
}

#[test]
fn deeply_segmented_loan_drop_is_iterative_with_unique_and_shared_tails() {
    let mut loan = ReadyPages::default();
    for id in 1..=100_000 {
        loan.push(PageId(id));
    }
    let mut shared = loan.clone();
    drop(loan);
    assert_eq!(shared.pop(), Some(PageId(100_000)));
    assert_eq!(shared.len(), 99_999);
    drop(shared);
}

#[test]
fn taking_a_unique_batch_preserves_its_vec_allocation_and_skipped_zero() {
    let mut source = Vec::with_capacity(64);
    source.extend([PageId(1), PageId(2), PageId(0), PageId(9)]);
    let pointer = source.as_ptr();
    let capacity = source.capacity();
    let mut alloc = PageAllocator::with_ready_pages(100, Arc::new(source));
    assert_eq!(alloc.allocate().unwrap(), PageId(9));
    assert_eq!(alloc.allocate_nonzero().unwrap(), PageId(2));
    let remainder = alloc.take_ready_to_use();
    assert_eq!(remainder, [PageId(1), PageId(0)]);
    assert_eq!(remainder.as_ptr(), pointer);
    assert_eq!(remainder.capacity(), capacity);
    assert_eq!(alloc.ready_count(), 0);
    assert_eq!(alloc.allocate().unwrap(), PageId(100));
}

#[test]
fn taking_shared_or_segmented_batches_preserves_the_source_and_checkpoint() {
    for segmented in [false, true] {
        let source = Arc::new(vec![PageId(1), PageId(2), PageId(0), PageId(9)]);
        let mut alloc = PageAllocator::with_ready_pages(100, Arc::clone(&source));
        assert_eq!(alloc.allocate().unwrap(), PageId(9));
        assert_eq!(alloc.allocate_nonzero().unwrap(), PageId(2));
        let mut expected = vec![PageId(1), PageId(0)];
        if segmented {
            alloc.add_ready_to_use(vec![PageId(3), PageId(0), PageId(4)]);
            expected.extend([PageId(3), PageId(0), PageId(4)]);
        }
        let checkpoint = alloc.checkpoint();
        let remainder = alloc.take_ready_to_use();
        assert_eq!(remainder, expected);
        assert_ne!(remainder.as_ptr(), source.as_ptr());
        assert_eq!(
            source.as_slice(),
            [PageId(1), PageId(2), PageId(0), PageId(9)]
        );
        assert_eq!(alloc.ready_count(), 0);
        alloc.restore(checkpoint);
        for &id in expected.iter().rev() {
            assert_eq!(alloc.allocate().unwrap(), id);
        }
        assert_eq!(alloc.ready_count(), 0);
    }
}
