use super::*;

fn fixture(count: usize, age: TxnId) -> (FxHashMap<PageId, Page>, PageAllocator, CommittedReclaim) {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(50_000);
    let entries: Vec<_> = (0..count)
        .map(|index| PendingFreeEntry {
            page_id: PageId(index as u32),
            freed_at_txn: age,
        })
        .collect();
    let ids: Vec<_> = (0..chain_pages_needed(count))
        .map(|_| alloc.allocate().unwrap())
        .collect();
    let root = write_chain(&mut pages, TxnId(10), &entries, &ids);
    alloc.commit();
    let state = CommittedReclaim::read_committed(root, alloc.high_water_mark(), TxnId(10), |id| {
        pages.get(&id).ok_or(Error::PageOutOfBounds(id))
    })
    .unwrap();
    (pages, alloc, state)
}

fn assert_matches_disk(
    state: &CommittedReclaim,
    pages: &FxHashMap<PageId, Page>,
    ready: &ReadyPages,
) {
    let disk = read_chain(pages, state.root).unwrap();
    assert_eq!(disk, state.chain_entries().collect::<Vec<_>>());
    assert_eq!(disk.len(), state.entries.len());
    assert_eq!(
        state.data_by_age.len() + state.metadata_by_age.len(),
        disk.len()
    );
    for entry in &disk {
        let location = &state.entries[&entry.page_id];
        assert_eq!(location.entry, *entry);
        assert_eq!(
            state.segments[&location.segment].entries[location.offset],
            *entry
        );
        let ages = if location.metadata {
            &state.metadata_by_age
        } else {
            &state.data_by_age
        };
        assert!(ages.contains(&(entry.freed_at_txn, entry.page_id)));
    }
    let mut seen = FxHashSet::default();
    let mut cursor = ready.clone();
    while let Some(id) = cursor.pop() {
        assert!(seen.insert(id), "duplicate ready page {id:?}");
        assert!(!state.segments.contains_key(&id));
        let entry = &state.entries[&id];
        let through = if entry.metadata {
            state.ready_metadata_through
        } else {
            state.ready_data_through
        };
        assert!(entry.entry.freed_at_txn <= through);
    }
}

fn continue_pinned_head_commits(
    pages: &mut FxHashMap<PageId, Page>,
    alloc: &mut PageAllocator,
    state: &mut CommittedReclaim,
    mut ready: ReadyPages,
) {
    for step in 0..96u32 {
        let consumed = ready.pop().into_iter().collect();
        let prepared = state
            .prepare(
                pages,
                alloc,
                &mut ready,
                &ChainCommit {
                    txn_id: TxnId(state.slot_txn.as_u64() + 1),
                    current_root: state.root,
                    freed_this_txn: &[PageId(40_000 + step)],
                    consumed: &consumed,
                    reclaim_horizon: TxnId(1),
                },
            )
            .unwrap();
        let work = prepared.work();
        assert_eq!(work.rewrite_entries, 0, "step {step}: {work:?}");
        assert!(work.head_entries <= 3 * MAX_ENTRIES_PER_PAGE, "{work:?}");
        assert!(work.eligibility_entries <= MAX_ENTRIES_PER_PAGE, "{work:?}");
        ready = prepared.ready_pages();
        state.publish(prepared, alloc.high_water_mark());
        assert_matches_disk(state, pages, &ready);
        for id in ready.iter() {
            assert_eq!(state.entries[id].segment, state.root);
        }
    }
}

#[test]
fn pinned_head_splits_and_fresh_replacements_remain_bounded_at_page_boundaries() {
    for count in [
        MAX_ENTRIES_PER_PAGE - 1,
        MAX_ENTRIES_PER_PAGE,
        MAX_ENTRIES_PER_PAGE + 1,
        2 * MAX_ENTRIES_PER_PAGE - 1,
        2 * MAX_ENTRIES_PER_PAGE,
        2 * MAX_ENTRIES_PER_PAGE + 1,
    ] {
        let (mut pages, mut alloc, mut state) = fixture(count, TxnId(5));
        continue_pinned_head_commits(&mut pages, &mut alloc, &mut state, ReadyPages::default());
    }
}

#[test]
fn full_rewrite_moves_retired_metadata_before_pinned_tail_for_following_commits() {
    for count in [
        2 * MAX_ENTRIES_PER_PAGE - 3,
        2 * MAX_ENTRIES_PER_PAGE - 2,
        2 * MAX_ENTRIES_PER_PAGE - 1,
        8_192,
    ] {
        let (mut pages, mut alloc, old) = fixture(count, TxnId(5));
        let tail_loan = PageId(count as u32 - 1);
        let metadata = [(tail_loan, TxnId(5))].into_iter().collect();
        let snapshot = ChainSnapshot::read_committed(
            old.root,
            alloc.high_water_mark(),
            old.slot_txn,
            0,
            |id| pages.get(&id).ok_or(Error::PageOutOfBounds(id)),
        )
        .unwrap();
        let mut state = CommittedReclaim::from_snapshot(
            snapshot,
            alloc.high_water_mark(),
            old.slot_txn,
            &metadata,
        );
        assert_ne!(state.entries[&tail_loan].segment, state.root);
        let retired = state.chain_ids();
        let mut ready = ReadyPages::default();
        let prepared = state
            .prepare(
                &mut pages,
                &mut alloc,
                &mut ready,
                &ChainCommit {
                    txn_id: TxnId(11),
                    current_root: state.root,
                    freed_this_txn: &[PageId(39_999)],
                    consumed: &[tail_loan].into_iter().collect(),
                    reclaim_horizon: TxnId(1),
                },
            )
            .unwrap();
        assert!(prepared.work().rewrite_entries >= count);
        ready = prepared.ready_pages();
        assert!(ready.is_empty(), "current metadata is not yet a loan");
        state.publish(prepared, alloc.high_water_mark());
        assert_eq!(
            state.segments[&state.root]
                .entries
                .iter()
                .take(retired.len())
                .map(|entry| entry.page_id)
                .collect::<Vec<_>>(),
            retired
        );
        assert_matches_disk(&state, &pages, &ready);
        continue_pinned_head_commits(&mut pages, &mut alloc, &mut state, ready);
    }
}

#[test]
fn empty_state_is_invalid_root_and_first_retirements_are_deferred() {
    let mut state = CommittedReclaim::default();
    assert!(state.matches(PageId::INVALID, 0, TxnId::ZERO));
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(20);
    let mut loans = ReadyPages::default();
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(3),
                current_root: PageId::INVALID,
                freed_this_txn: &[PageId(0)],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(u64::MAX),
            },
        )
        .unwrap();
    assert!(prepared.ready_pages().is_empty());
    assert!(state
        .zero_candidates(&prepared, TxnId::ZERO, TxnId::ZERO)
        .is_empty());
    let ready = prepared.ready_pages();
    state.publish(prepared, alloc.high_water_mark());
    assert_matches_disk(&state, &pages, &ready);
    assert_eq!(state.entries[&PageId(0)].entry.freed_at_txn, TxnId(3));
}

#[test]
fn warm_pinned_tail_is_neither_decoded_nor_scanned() {
    for count in [0, 1_024, 8_192] {
        let (mut pages, mut alloc, mut state) = fixture(count, TxnId(5));
        assert_eq!(state.initial_decoded_entries(), count);
        let mut loans = ReadyPages::default();
        // Establish the eligibility marker once, without changing the chain.
        let prepared = state
            .prepare(
                &mut pages,
                &mut alloc,
                &mut loans,
                &ChainCommit {
                    txn_id: TxnId(11),
                    current_root: state.root,
                    freed_this_txn: &[],
                    consumed: &FxHashSet::default(),
                    reclaim_horizon: TxnId(1),
                },
            )
            .unwrap();
        assert_eq!(prepared.work().rewrite_entries, 0);
        state.publish(prepared, alloc.high_water_mark());
        let old_root = state.root;
        let old_count = state.entries.len();
        let prepared = state
            .prepare(
                &mut pages,
                &mut alloc,
                &mut loans,
                &ChainCommit {
                    txn_id: TxnId(14),
                    current_root: state.root,
                    freed_this_txn: &[PageId(40_000)],
                    consumed: &FxHashSet::default(),
                    reclaim_horizon: TxnId(1),
                },
            )
            .unwrap();
        let work = prepared.work();
        assert_eq!(work.rewrite_entries, 0);
        assert_eq!(work.eligibility_entries, 0);
        assert!(work.head_entries <= 3 * MAX_ENTRIES_PER_PAGE);
        assert_eq!(state.root, old_root, "preparation must not publish");
        assert_eq!(state.entries.len(), old_count);
        let ready = prepared.ready_pages();
        assert!(ready.is_empty());
        state.publish(prepared, alloc.high_water_mark());
        assert_matches_disk(&state, &pages, &ready);
    }
}

#[test]
fn reusable_tail_keeps_head_loans_and_bounded_warm_work() {
    for count in [1_024, 8_192] {
        let (mut pages, mut alloc, mut state) = fixture(count, TxnId(1));
        let mut ready = ReadyPages::default();
        let prepared = state
            .prepare(
                &mut pages,
                &mut alloc,
                &mut ready,
                &ChainCommit {
                    txn_id: TxnId(11),
                    current_root: state.root,
                    freed_this_txn: &[],
                    consumed: &FxHashSet::default(),
                    reclaim_horizon: TxnId(u64::MAX),
                },
            )
            .unwrap();
        assert_eq!(prepared.work().eligibility_entries, count);
        assert_eq!(
            prepared.work().readiness_sort_entries,
            count - state.segments[&state.root].entries.len(),
            "only newly eligible tail entries need sorting"
        );
        ready = prepared.ready_pages();
        assert_eq!(
            ready.last(),
            Some(PageId(0)),
            "ready order follows durable head, including page zero"
        );
        state.publish(prepared, alloc.high_water_mark());
        for step in 0..32u64 {
            let issued = ready.clone();
            let issued_ids: Vec<_> = issued.iter().copied().collect();
            let body = ready.pop().unwrap();
            let structure = ready.last().unwrap();
            assert_eq!(state.entries[&body].segment, state.root);
            assert_eq!(state.entries[&structure].segment, state.root);
            let consumed = [body].into_iter().collect();
            // An old retirement's identity must not make the same ID immediately
            // reusable after this writer consumes and retires it again.
            let refreed = step % 2 == 1;
            let freed = if refreed {
                body
            } else {
                PageId(40_000 + step as u32)
            };
            let prepared = state
                .prepare(
                    &mut pages,
                    &mut alloc,
                    &mut ready,
                    &ChainCommit {
                        txn_id: TxnId(12 + step),
                        current_root: state.root,
                        freed_this_txn: &[freed],
                        consumed: &consumed,
                        reclaim_horizon: TxnId(u64::MAX),
                    },
                )
                .unwrap();
            let work = prepared.work();
            assert_eq!(work.rewrite_entries, 0);
            assert!(work.head_entries <= 3 * MAX_ENTRIES_PER_PAGE);
            assert!(work.eligibility_entries <= 2);
            assert_eq!(
                work.readiness_sort_entries, 0,
                "warm head must merge linearly: {work:?}"
            );
            assert_eq!(prepared.root(), structure);
            ready = prepared.ready_pages();
            assert!(!ready.iter().any(|id| *id == body));
            assert_eq!(issued.iter().copied().collect::<Vec<_>>(), issued_ids);
            state.publish(prepared, alloc.high_water_mark());
            if refreed {
                assert_eq!(state.entries[&body].entry.freed_at_txn, TxnId(12 + step));
                assert_eq!(state.metadata_age(body), None);
            } else {
                assert!(!state.entries.contains_key(&body));
            }
            assert!(!state.entries.contains_key(&structure));
            assert_matches_disk(&state, &pages, &ready);
        }
    }
}

#[test]
fn invalid_deltas_fail_before_pages_loans_or_committed_state_change() {
    let (pages, _, state) = fixture(MAX_ENTRIES_PER_PAGE + 3, TxnId(2));
    let cases = [
        (vec![PageId(40_000), PageId(40_000)], vec![]),
        (vec![PageId::INVALID], vec![]),
        (vec![PageId(60_000)], vec![]),
        (vec![PageId(0)], vec![]),
        (vec![state.root], vec![]),
        (vec![], vec![PageId(40_000)]),
    ];
    for (freed, consumed) in cases {
        let mut pages = pages.clone();
        let before: FxHashMap<_, _> = pages.iter().map(|(&id, page)| (id, page.data)).collect();
        let mut alloc = PageAllocator::new(state.high_water_mark);
        let mut loans = ReadyPages::from_pop_order(vec![PageId(1)]);
        let consumed = consumed.into_iter().collect();
        let result = state.prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(20),
                current_root: state.root,
                freed_this_txn: &freed,
                consumed: &consumed,
                reclaim_horizon: TxnId(u64::MAX),
            },
        );
        assert!(matches!(result, Err(Error::DatabaseCorrupted)));
        assert_eq!(loans.len(), 1);
        assert_eq!(loans.last(), Some(PageId(1)));
        assert_eq!(
            before,
            pages.iter().map(|(&id, page)| (id, page.data)).collect()
        );
        assert!(alloc.allocated_this_txn().is_empty());
        assert_matches_disk(&state, &pages, &ReadyPages::default());
    }
    let mut pages = pages.clone();
    let mut alloc = PageAllocator::new(state.high_water_mark);
    let mut loans = ReadyPages::from_pop_order(vec![PageId(45_000)]);
    let result = state.prepare(
        &mut pages,
        &mut alloc,
        &mut loans,
        &ChainCommit {
            txn_id: TxnId(20),
            current_root: state.root,
            freed_this_txn: &[PageId(40_000)],
            consumed: &FxHashSet::default(),
            reclaim_horizon: TxnId(u64::MAX),
        },
    );
    assert!(matches!(result, Err(Error::DatabaseCorrupted)));
    assert_eq!(
        loans.last(),
        Some(PageId(45_000)),
        "invalid structure loan must remain detached"
    );
    assert!(alloc.allocated_this_txn().is_empty());
}

#[test]
fn consumed_metadata_refreed_as_data_loses_early_eligibility() {
    let (mut pages, mut alloc, mut state) = fixture(4, TxnId(2));
    let mut loans = ReadyPages::default();
    let old_root = state.root;
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(11),
                current_root: old_root,
                freed_this_txn: &[PageId(40_000)],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(1),
            },
        )
        .unwrap();
    state.publish(prepared, alloc.high_water_mark());
    assert_eq!(state.metadata_age(old_root), Some(TxnId(11)));
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(19),
                current_root: state.root,
                freed_this_txn: &[],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(1),
            },
        )
        .unwrap();
    loans = prepared.ready_pages();
    assert_eq!(loans.pop(), Some(old_root));
    state.publish(prepared, alloc.high_water_mark());
    let consumed = [old_root].into_iter().collect();
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(25),
                current_root: state.root,
                freed_this_txn: &[old_root],
                consumed: &consumed,
                reclaim_horizon: TxnId(1),
            },
        )
        .unwrap();
    let ready = prepared.ready_pages();
    assert!(ready.is_empty());
    state.publish(prepared, alloc.high_water_mark());
    assert_eq!(state.metadata_age(old_root), None);
    assert_eq!(state.entries[&old_root].entry.freed_at_txn, TxnId(25));
    assert_matches_disk(&state, &pages, &ready);
}

#[test]
fn eligibility_and_erasure_use_separate_exact_ages_across_horizon_changes() {
    let (mut pages, mut alloc, mut state) = fixture(3, TxnId(5));
    let mut loans = ReadyPages::default();
    let old_root = state.root;
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(11),
                current_root: old_root,
                freed_this_txn: &[PageId(40_000)],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(2),
            },
        )
        .unwrap();
    state.publish(prepared, alloc.high_water_mark());
    // Metadata retired by the old slot is loanable after this commit, but is
    // still too young to zero while that slot remains the recovery alternative.
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(17),
                current_root: state.root,
                freed_this_txn: &[],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(2),
            },
        )
        .unwrap();
    assert_eq!(prepared.ready_pages().last(), Some(old_root));
    assert!(state
        .zero_candidates(&prepared, TxnId::ZERO, TxnId::ZERO)
        .is_empty());
    state.publish(prepared, alloc.high_water_mark());
    // Reset only readiness as a deliberately dropped RAM loan cache would;
    // provenance and secure-delete watermarks retain independent identities.
    state.reset_ready_progress();
    loans = ReadyPages::default();
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(23),
                current_root: state.root,
                freed_this_txn: &[],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(5),
            },
        )
        .unwrap();
    let zero = state.zero_candidates(&prepared, TxnId::ZERO, TxnId::ZERO);
    assert_eq!(zero.len(), 4);
    assert_eq!(zero.iter().filter(|(_, metadata)| *metadata).count(), 1);
    assert!(zero
        .iter()
        .any(|(entry, metadata)| entry.page_id == old_root && *metadata));
    assert!(!zero
        .iter()
        .any(|(entry, _)| entry.page_id == PageId(40_000)));
    assert!(state
        .zero_candidates(&prepared, TxnId(5), TxnId(11))
        .is_empty());
    let ready = prepared.ready_pages();
    assert_eq!(ready.len(), 4);
    state.publish(prepared, alloc.high_water_mark());
    assert_matches_disk(&state, &pages, &ready);
}

#[test]
fn discarded_full_rewrite_preserves_state_and_retry_publishes_exactly_once() {
    let (original_pages, original_alloc, mut state) =
        fixture(2 * MAX_ENTRIES_PER_PAGE + 7, TxnId(1));
    let original_root = state.root;
    let original_entries = state.chain_entries().collect::<Vec<_>>();
    let tail_id = PageId(MAX_ENTRIES_PER_PAGE as u32 + 1);
    let consumed = [tail_id].into_iter().collect();
    for publish in [false, true] {
        let mut pages = original_pages.clone();
        let mut alloc = PageAllocator::new(original_alloc.high_water_mark());
        let mut loans =
            ReadyPages::from_pop_order(vec![PageId(0), PageId(1), PageId(2), PageId(3)]);
        let prepared = state
            .prepare(
                &mut pages,
                &mut alloc,
                &mut loans,
                &ChainCommit {
                    txn_id: TxnId(23),
                    current_root: original_root,
                    freed_this_txn: &[PageId(40_000)],
                    consumed: &consumed,
                    reclaim_horizon: TxnId(u64::MAX),
                },
            )
            .unwrap();
        assert!(prepared.work().rewrite_entries > original_entries.len());
        assert_eq!(state.root, original_root);
        assert_eq!(state.chain_entries().collect::<Vec<_>>(), original_entries);
        assert!(state.entries.contains_key(&tail_id));
        assert!(state.metadata_retirements().is_empty());
        let zero = state.zero_candidates(&prepared, TxnId::ZERO, TxnId::ZERO);
        assert!(!zero.iter().any(|(entry, _)| entry.page_id == tail_id));
        if publish {
            let ready = prepared.ready_pages();
            state.publish(prepared, alloc.high_water_mark());
            assert!(!state.entries.contains_key(&tail_id));
            assert_eq!(state.metadata_age(original_root), Some(TxnId(23)));
            assert_matches_disk(&state, &pages, &ready);
        } else {
            drop(prepared);
            assert_matches_disk(&state, &original_pages, &ReadyPages::default());
        }
    }
}

#[test]
fn single_head_replacement_and_unchanged_loan_pool_keep_bounded_state() {
    let (mut pages, mut alloc, mut state) = fixture(8, TxnId(1));
    let mut ready = ReadyPages::default();
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut ready,
            &ChainCommit {
                txn_id: TxnId(11),
                current_root: state.root,
                freed_this_txn: &[],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(u64::MAX),
            },
        )
        .unwrap();
    ready = prepared.ready_pages();
    state.publish(prepared, alloc.high_water_mark());
    let root = state.root;
    let before = pages.len();
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut ready,
            &ChainCommit {
                txn_id: TxnId(17),
                current_root: root,
                freed_this_txn: &[],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(u64::MAX),
            },
        )
        .unwrap();
    assert_eq!(prepared.root(), root);
    assert_eq!(prepared.work(), ReclaimWork::default());
    assert_eq!(ready.len(), 8);
    assert_eq!(prepared.ready_pages().len(), 8);
    assert_eq!(pages.len(), before);
    state.publish(prepared, alloc.high_water_mark());
    let body = ready.pop().unwrap();
    let structure = ready.last().unwrap();
    let consumed = [body].into_iter().collect();
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut ready,
            &ChainCommit {
                txn_id: TxnId(23),
                current_root: root,
                freed_this_txn: &[PageId(40_000)],
                consumed: &consumed,
                reclaim_horizon: TxnId(u64::MAX),
            },
        )
        .unwrap();
    assert_eq!(prepared.root(), structure);
    assert_eq!(prepared.work().rewrite_entries, 0);
    assert_eq!(prepared.work().head_entries, 8);
    assert_eq!(pages[&structure].right_child(), PageId::INVALID);
    let ready = prepared.ready_pages();
    state.publish(prepared, alloc.high_water_mark());
    assert_eq!(state.metadata_age(root), Some(TxnId(23)));
    assert_eq!(state.segments.len(), 1);
    assert_matches_disk(&state, &pages, &ready);
}

#[test]
fn horizon_advance_keeps_existing_head_loans_before_new_tail_data() {
    let (mut pages, mut alloc, mut state) = fixture(8_192, TxnId(5));
    // First make a bounded partial head and retire it, giving the state a
    // metadata entry independent of the reader-pinned data in the large tail.
    let mut loans = ReadyPages::default();
    for txn in [11, 12] {
        let prepared = state
            .prepare(
                &mut pages,
                &mut alloc,
                &mut loans,
                &ChainCommit {
                    txn_id: TxnId(txn),
                    current_root: state.root,
                    freed_this_txn: &[PageId(40_000 + txn as u32)],
                    consumed: &FxHashSet::default(),
                    reclaim_horizon: TxnId(1),
                },
            )
            .unwrap();
        loans = prepared.ready_pages();
        state.publish(prepared, alloc.high_water_mark());
    }
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(13),
                current_root: state.root,
                freed_this_txn: &[],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(1),
            },
        )
        .unwrap();
    loans = prepared.ready_pages();
    state.publish(prepared, alloc.high_water_mark());
    let head_loan = loans.last().unwrap();
    assert_eq!(state.entries[&head_loan].segment, state.root);
    let existing_loans: Vec<_> = loans.iter().copied().collect();
    assert_eq!(existing_loans.len(), 2);
    let head_entries: Vec<_> = read_page_entries(&pages[&state.root]).unwrap().collect();
    assert_eq!(head_entries.len(), 4);
    assert!(
        head_entries
            .iter()
            .all(|entry| entry.page_id.as_u32() >= 8_192),
        "the original age-5 data is entirely in the unchanged tail"
    );
    let newly_eligible_head = PageId(40_011);
    let still_pinned_head = PageId(40_012);
    assert!(head_entries.contains(&PendingFreeEntry {
        page_id: newly_eligible_head,
        freed_at_txn: TxnId(11),
    }));
    assert!(head_entries.contains(&PendingFreeEntry {
        page_id: still_pinned_head,
        freed_at_txn: TxnId(12),
    }));
    let expected_head: Vec<_> = head_entries
        .iter()
        .filter_map(|entry| {
            (existing_loans.contains(&entry.page_id) || entry.page_id == newly_eligible_head)
                .then_some(entry.page_id)
        })
        .collect();
    assert_eq!(expected_head.len(), 3);
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(20),
                current_root: state.root,
                freed_this_txn: &[],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(11),
            },
        )
        .unwrap();
    assert_eq!(prepared.work().eligibility_entries, 8_193);
    assert_eq!(prepared.work().rewrite_entries, 0);
    assert_eq!(
        prepared.work().readiness_sort_entries,
        8_192,
        "newly eligible head entries must not enter the tail sort"
    );
    let ready = prepared.ready_pages();
    assert_eq!(ready.last(), Some(head_loan));
    assert_eq!(ready.len(), 8_195);
    assert_eq!(
        ready.iter().take(3).copied().collect::<Vec<_>>(),
        expected_head
    );
    assert!(ready.iter().skip(3).copied().eq((0..8_192).map(PageId)));
    assert!(!ready.iter().any(|id| *id == still_pinned_head));
    state.publish(prepared, alloc.high_water_mark());
    assert_matches_disk(&state, &pages, &ready);
}

#[test]
fn rebuilding_readiness_does_not_duplicate_a_retained_loan_cursor() {
    let (mut pages, mut alloc, mut state) = fixture(8, TxnId(1));
    let mut loans = ReadyPages::from_pop_order(vec![PageId(0), PageId(1)]);
    let prepared = state
        .prepare(
            &mut pages,
            &mut alloc,
            &mut loans,
            &ChainCommit {
                txn_id: TxnId(11),
                current_root: state.root,
                freed_this_txn: &[],
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(u64::MAX),
            },
        )
        .unwrap();
    let ready = prepared.ready_pages();
    assert_eq!(ready.len(), 8);
    assert_eq!(prepared.work().readiness_sort_entries, 0);
    assert_eq!(
        ready.iter().copied().collect::<Vec<_>>(),
        (0..8).map(PageId).collect::<Vec<_>>()
    );
    state.publish(prepared, alloc.high_water_mark());
    assert_matches_disk(&state, &pages, &ready);
}

#[test]
fn staged_chain_sealing_preserves_public_bytes_and_unrelated_writer_pages() {
    for rewrite in [false, true] {
        let (mut pages, mut alloc, state) = fixture(2 * MAX_ENTRIES_PER_PAGE + 3, TxnId(1));
        let old_ids = collect_chain_page_ids(&pages, state.root).unwrap();
        let old_bytes: Vec<_> = old_ids.iter().map(|id| (*id, pages[id].data)).collect();
        assert!(old_ids.iter().all(|id| pages[id].verify_checksum()));
        let unrelated = PageId(49_000);
        pages.insert(
            unrelated,
            Page::new_for_write(unrelated, PageType::Leaf, TxnId(11)),
        );

        let consumed: FxHashSet<_> = if rewrite {
            [PageId(MAX_ENTRIES_PER_PAGE as u32 + 1)]
                .into_iter()
                .collect()
        } else {
            FxHashSet::default()
        };
        let loan_ids: Vec<_> = if rewrite {
            state
                .chain_entries()
                .map(|entry| entry.page_id)
                .filter(|id| !consumed.contains(id))
                .collect()
        } else {
            Vec::new()
        };
        let commit = ChainCommit {
            txn_id: TxnId(11),
            current_root: state.root,
            freed_this_txn: &[PageId(40_000)],
            consumed: &consumed,
            reclaim_horizon: TxnId(1),
        };
        let mut public_pages = pages.clone();
        let mut public_alloc = alloc.clone();
        let mut public_loans: Vec<_> = loan_ids.iter().rev().copied().collect();
        let mut loans = ReadyPages::from_pop_order(loan_ids);
        let prepared = state
            .prepare(&mut pages, &mut alloc, &mut loans, &commit)
            .unwrap();
        assert_eq!(matches!(&prepared.change, Change::Rewrite(_)), rewrite);
        let ids = collect_chain_page_ids(&pages, prepared.root()).unwrap();
        assert!(ids.iter().any(|id| !old_ids.contains(id)));
        for id in ids.iter().filter(|id| !old_ids.contains(id)) {
            assert_eq!(pages[id].checksum(), 0, "prepare sealed a private page");
        }
        prepared.seal_staged_pages(&mut pages);
        assert!(ids.iter().all(|id| pages[id].verify_checksum()));
        assert_eq!(pages[&unrelated].checksum(), 0);
        for (id, bytes) in old_bytes {
            assert_eq!(pages[&id].data, bytes, "old-slot chain page was modified");
        }

        let (public_root, public_available) = process_chain(
            &mut public_pages,
            &mut public_alloc,
            &mut public_loans,
            &commit,
        )
        .unwrap();
        assert_eq!(public_root, prepared.root());
        assert_eq!(
            public_available,
            state.available_after(&prepared, commit.reclaim_horizon)
        );
        assert_eq!(public_alloc.high_water_mark(), alloc.high_water_mark());
        assert_eq!(
            public_loans.iter().rev().copied().collect::<Vec<_>>(),
            loans.iter().copied().collect::<Vec<_>>()
        );
        assert_eq!(public_pages.len(), pages.len());
        for (id, page) in &pages {
            assert_eq!(
                public_pages[id].data, page.data,
                "public boundary differs for {id:?}"
            );
        }
    }
}
