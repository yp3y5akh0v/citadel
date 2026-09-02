use super::*;

fn structure_for(alloc: &mut PageAllocator, entries: &[PendingFreeEntry]) -> Vec<PageId> {
    (0..chain_pages_needed(entries.len()))
        .map(|_| alloc.allocate())
        .collect()
}

#[test]
fn empty_chain() {
    let pages = FxHashMap::default();
    let entries = read_chain(&pages, PageId::INVALID).unwrap();
    assert!(entries.is_empty());
}

#[test]
fn write_and_read_chain() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(0);

    let entries = vec![
        PendingFreeEntry {
            page_id: PageId(10),
            freed_at_txn: TxnId(1),
        },
        PendingFreeEntry {
            page_id: PageId(20),
            freed_at_txn: TxnId(2),
        },
        PendingFreeEntry {
            page_id: PageId(30),
            freed_at_txn: TxnId(3),
        },
    ];

    let ids = structure_for(&mut alloc, &entries);
    let root = write_chain(&mut pages, TxnId(5), &entries, &ids);
    assert!(root.is_valid());

    let read_back = read_chain(&pages, root).unwrap();
    assert_eq!(read_back.len(), 3);
    assert_eq!(read_back[0], entries[0]);
    assert_eq!(read_back[1], entries[1]);
    assert_eq!(read_back[2], entries[2]);
}

#[test]
fn write_chain_multi_page() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(0);

    let count = MAX_ENTRIES_PER_PAGE + 10;
    let entries: Vec<PendingFreeEntry> = (0..count)
        .map(|i| PendingFreeEntry {
            page_id: PageId(100 + i as u32),
            freed_at_txn: TxnId(i as u64),
        })
        .collect();

    let ids = structure_for(&mut alloc, &entries);
    let root = write_chain(&mut pages, TxnId(999), &entries, &ids);
    let read_back = read_chain(&pages, root).unwrap();
    assert_eq!(read_back.len(), count);

    for (i, entry) in read_back.iter().enumerate() {
        assert_eq!(entry.page_id, PageId(100 + i as u32));
        assert_eq!(entry.freed_at_txn, TxnId(i as u64));
    }
}

#[test]
fn write_empty_chain() {
    let mut pages = FxHashMap::default();
    let root = write_chain(&mut pages, TxnId(1), &[], &[]);
    assert_eq!(root, PageId::INVALID);
}

#[test]
fn collect_chain_pages() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(0);

    let entries: Vec<PendingFreeEntry> = (0..MAX_ENTRIES_PER_PAGE + 10)
        .map(|i| PendingFreeEntry {
            page_id: PageId(100 + i as u32),
            freed_at_txn: TxnId(1),
        })
        .collect();

    let ids = structure_for(&mut alloc, &entries);
    let root = write_chain(&mut pages, TxnId(1), &entries, &ids);
    let chain_pages = collect_chain_page_ids(&pages, root).unwrap();
    assert_eq!(chain_pages.len(), 2); // Should span 2 pages
}

/// Entries past the horizon become available but stay in the durable chain
/// until consumed; the old chain's structure pages become entries freed at
/// this txn.
#[test]
fn process_chain_availability_retains_entries() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(0);

    let initial_entries = vec![
        PendingFreeEntry {
            page_id: PageId(10),
            freed_at_txn: TxnId(1),
        },
        PendingFreeEntry {
            page_id: PageId(20),
            freed_at_txn: TxnId(2),
        },
        PendingFreeEntry {
            page_id: PageId(30),
            freed_at_txn: TxnId(3),
        },
    ];
    let ids = structure_for(&mut alloc, &initial_entries);
    let root = write_chain(&mut pages, TxnId(3), &initial_entries, &ids);

    let freed_this_txn = vec![PageId(40)];
    let (new_root, available) = process_chain(
        &mut pages,
        &mut alloc,
        &mut Vec::new(),
        &ChainCommit {
            txn_id: TxnId(4),
            current_root: root,
            freed_this_txn: &freed_this_txn,
            consumed: &FxHashSet::default(),
            reclaim_horizon: TxnId(2),
        },
    )
    .unwrap();

    let available_ids: Vec<PageId> = available.iter().map(|e| e.page_id).collect();
    assert_eq!(available_ids.len(), 2);
    assert!(available_ids.contains(&PageId(10)));
    assert!(available_ids.contains(&PageId(20)));

    // 3 retained + 1 old structure page + 1 freed this txn.
    let new_entries = read_chain(&pages, new_root).unwrap();
    assert_eq!(new_entries.len(), 5);
    assert!(new_entries
        .iter()
        .any(|e| e.page_id == ids[0] && e.freed_at_txn == TxnId(4)));
    assert!(new_entries
        .iter()
        .any(|e| e.page_id == PageId(40) && e.freed_at_txn == TxnId(4)));
}

/// Consumed pages (allocated into live data this txn) leave the chain and
/// are never reported available again.
#[test]
fn process_chain_drops_consumed_entries() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(0);

    let initial_entries = vec![
        PendingFreeEntry {
            page_id: PageId(10),
            freed_at_txn: TxnId(1),
        },
        PendingFreeEntry {
            page_id: PageId(20),
            freed_at_txn: TxnId(1),
        },
    ];
    let ids = structure_for(&mut alloc, &initial_entries);
    let root = write_chain(&mut pages, TxnId(1), &initial_entries, &ids);

    let consumed: FxHashSet<PageId> = [PageId(10)].into_iter().collect();
    let (new_root, available) = process_chain(
        &mut pages,
        &mut alloc,
        &mut Vec::new(),
        &ChainCommit {
            txn_id: TxnId(2),
            current_root: root,
            freed_this_txn: &[],
            consumed: &consumed,
            reclaim_horizon: TxnId(u64::MAX),
        },
    )
    .unwrap();

    let available_ids: Vec<PageId> = available.iter().map(|e| e.page_id).collect();
    assert_eq!(available_ids, vec![PageId(20)]);

    let new_entries = read_chain(&pages, new_root).unwrap();
    assert!(!new_entries.iter().any(|e| e.page_id == PageId(10)));
    assert!(new_entries.iter().any(|e| e.page_id == PageId(20)));
}

/// The new chain's structure pages come from the loan pool first; a page
/// used as structure loses its own entry, so bookkeeping never grows the
/// chain.
#[test]
fn process_chain_structure_consumes_loan_pool() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(100);

    let initial_entries = vec![
        PendingFreeEntry {
            page_id: PageId(10),
            freed_at_txn: TxnId(1),
        },
        PendingFreeEntry {
            page_id: PageId(20),
            freed_at_txn: TxnId(1),
        },
    ];
    let ids = structure_for(&mut alloc, &initial_entries);
    let root = write_chain(&mut pages, TxnId(1), &initial_entries, &ids);

    let mut loan_pool = vec![PageId(10)];
    let (new_root, available) = process_chain(
        &mut pages,
        &mut alloc,
        &mut loan_pool,
        &ChainCommit {
            txn_id: TxnId(2),
            current_root: root,
            freed_this_txn: &[],
            consumed: &FxHashSet::default(),
            reclaim_horizon: TxnId(u64::MAX),
        },
    )
    .unwrap();

    assert_eq!(new_root, PageId(10), "structure page taken from the loan");
    let new_entries = read_chain(&pages, new_root).unwrap();
    assert!(
        !new_entries.iter().any(|e| e.page_id == PageId(10)),
        "a structure page must not list itself as free"
    );
    assert!(new_entries.iter().any(|e| e.page_id == PageId(20)));
    assert!(available.iter().all(|e| e.page_id != PageId(10)));
}

/// A page freed at this txn is never available for reuse this commit, even
/// with an unbounded horizon: the previous slot still references it.
#[test]
fn process_chain_freed_this_txn_never_available() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(0);

    let (new_root, available) = process_chain(
        &mut pages,
        &mut alloc,
        &mut Vec::new(),
        &ChainCommit {
            txn_id: TxnId(5),
            current_root: PageId::INVALID,
            freed_this_txn: &[PageId(70), PageId(71)],
            consumed: &FxHashSet::default(),
            reclaim_horizon: TxnId(u64::MAX),
        },
    )
    .unwrap();

    assert!(
        available.is_empty(),
        "pages freed this txn must not be reusable yet"
    );
    let entries = read_chain(&pages, new_root).unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|e| e.freed_at_txn == TxnId(5)));
}

/// Consuming every data entry with nothing freed drops them all: the new
/// chain lists only the old structure page (freed at this txn, still
/// referenced by the previous slot), and nothing is available.
#[test]
fn process_chain_all_consumed_drops_all_entries() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(0);

    let initial = vec![
        PendingFreeEntry {
            page_id: PageId(10),
            freed_at_txn: TxnId(1),
        },
        PendingFreeEntry {
            page_id: PageId(20),
            freed_at_txn: TxnId(1),
        },
    ];
    let ids = structure_for(&mut alloc, &initial);
    let root = write_chain(&mut pages, TxnId(1), &initial, &ids);

    // Both loaned pages were consumed, so the loan remainder is empty.
    let consumed: FxHashSet<PageId> = [PageId(10), PageId(20)].into_iter().collect();
    let (new_root, available) = process_chain(
        &mut pages,
        &mut alloc,
        &mut Vec::new(),
        &ChainCommit {
            txn_id: TxnId(2),
            current_root: root,
            freed_this_txn: &[],
            consumed: &consumed,
            reclaim_horizon: TxnId(u64::MAX),
        },
    )
    .unwrap();

    assert!(available.is_empty());
    assert_ne!(new_root, ids[0], "chain rewrite is CoW");
    let entries = read_chain(&pages, new_root).unwrap();
    assert_eq!(
        entries,
        vec![PendingFreeEntry {
            page_id: ids[0],
            freed_at_txn: TxnId(2),
        }]
    );
}

/// Multi-page chain: the loan pool (unconsumed loaned data entries) supplies
/// every structure page, each taken page drops its own entry, and the
/// rewrite never grows the chain.
#[test]
fn process_chain_multipage_structure_from_loan_pool() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(10_000);

    // Two full pages' worth of carried entries, all past the horizon.
    let count = MAX_ENTRIES_PER_PAGE + 5;
    let initial: Vec<PendingFreeEntry> = (0..count)
        .map(|i| PendingFreeEntry {
            page_id: PageId(1000 + i as u32),
            freed_at_txn: TxnId(1),
        })
        .collect();
    let ids = structure_for(&mut alloc, &initial);
    assert_eq!(ids.len(), 2, "test expects a 2-page chain");
    let root = write_chain(&mut pages, TxnId(1), &initial, &ids);

    // Two data entries were loaned and not consumed; the rewrite draws both
    // structure pages from them.
    let mut loan_pool = vec![PageId(1000), PageId(1001)];
    let (new_root, available) = process_chain(
        &mut pages,
        &mut alloc,
        &mut loan_pool,
        &ChainCommit {
            txn_id: TxnId(2),
            current_root: root,
            freed_this_txn: &[],
            consumed: &FxHashSet::default(),
            reclaim_horizon: TxnId(5),
        },
    )
    .unwrap();

    assert!(loan_pool.is_empty(), "both loan pages become structure");
    assert!(new_root == PageId(1000) || new_root == PageId(1001));

    let entries = read_chain(&pages, new_root).unwrap();
    // Surviving data entries plus the 2 old structure pages; the two loan
    // pages dropped their entries, so the chain does not grow.
    assert_eq!(entries.len(), count);
    for id in [PageId(1000), PageId(1001)] {
        assert!(
            !entries.iter().any(|e| e.page_id == id),
            "a structure page must not list itself as free"
        );
    }
    for id in &ids {
        assert!(
            entries
                .iter()
                .any(|e| e.page_id == *id && e.freed_at_txn == TxnId(2)),
            "old structure pages become entries freed at this txn"
        );
    }
    // Every surviving data entry is available (freed at txn 1 <= horizon 5).
    assert_eq!(available.len(), count - 2);
    assert!(available
        .iter()
        .all(|e| e.page_id != PageId(1000) && e.page_id != PageId(1001)));
}

#[test]
fn max_entries_per_page_correct() {
    assert_eq!(MAX_ENTRIES_PER_PAGE, (8096 - 4) / 12);
}

fn assert_same_entries(actual: &[PendingFreeEntry], expected: &[PendingFreeEntry]) {
    let mut actual = actual.to_vec();
    let mut expected = expected.to_vec();
    let key = |entry: &PendingFreeEntry| (entry.page_id.as_u32(), entry.freed_at_txn.as_u64());
    actual.sort_unstable_by_key(key);
    expected.sort_unstable_by_key(key);
    assert_eq!(actual, expected);
}

#[test]
fn chain_snapshot_reads_each_page_once_and_processes_without_old_pages_in_write_set() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(10_000);
    let initial: Vec<_> = (0..MAX_ENTRIES_PER_PAGE + 3)
        .map(|index| PendingFreeEntry {
            page_id: PageId(1000 + index as u32),
            freed_at_txn: TxnId(1),
        })
        .collect();
    let old_ids = structure_for(&mut alloc, &initial);
    let root = write_chain(&mut pages, TxnId(7), &initial, &old_ids);
    let mut loaded = Vec::new();
    let snapshot =
        ChainSnapshot::read_committed(root, alloc.high_water_mark(), TxnId(7), |page_id| {
            loaded.push(page_id);
            pages.get(&page_id).ok_or(Error::PageOutOfBounds(page_id))
        })
        .unwrap();
    assert_eq!(loaded, old_ids);
    let stored_pages = std::mem::take(&mut pages);
    assert!(pages.is_empty());

    let freed = [PageId(9000)];
    let (new_root, available) = snapshot
        .process(
            &mut pages,
            &mut alloc,
            &mut Vec::new(),
            &ChainCommit {
                txn_id: TxnId(9),
                current_root: root,
                freed_this_txn: &freed,
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(u64::MAX),
            },
        )
        .unwrap();

    let mut expected = initial.clone();
    expected.extend(freed.iter().map(|&page_id| PendingFreeEntry {
        page_id,
        freed_at_txn: TxnId(9),
    }));
    let result = ChainSnapshot::read(new_root, |id| {
        pages
            .get(&id)
            .or_else(|| stored_pages.get(&id))
            .ok_or(Error::PageOutOfBounds(id))
    })
    .unwrap();
    assert_same_entries(&result.entries, &expected);
    assert_same_entries(&available, &initial);
    assert!(old_ids.iter().all(|page_id| !pages.contains_key(page_id)));
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[&new_root].right_child(), root);
}

#[test]
fn prepend_packs_the_head_and_preserves_sparse_tails() {
    for head_count in [0, 1, MAX_ENTRIES_PER_PAGE - 1, MAX_ENTRIES_PER_PAGE] {
        for freed_count in [0, 1, 2, MAX_ENTRIES_PER_PAGE, 2 * MAX_ENTRIES_PER_PAGE + 7] {
            for horizon in [TxnId(3), TxnId(u64::MAX)] {
                let mut pages = FxHashMap::default();
                let mut alloc = PageAllocator::new(10_000);
                let old_ids = [alloc.allocate(), alloc.allocate(), alloc.allocate()];
                let initial: Vec<_> = (0..head_count + 3)
                    .map(|i| PendingFreeEntry {
                        page_id: PageId(i as u32),
                        freed_at_txn: TxnId(i as u64 % 5 + 1),
                    })
                    .collect();
                write_chain_page(
                    &mut pages,
                    TxnId(7),
                    old_ids[0],
                    old_ids[1],
                    &initial[..head_count],
                );
                write_chain_page(&mut pages, TxnId(7), old_ids[1], old_ids[2], &[]);
                write_chain_page(
                    &mut pages,
                    TxnId(7),
                    old_ids[2],
                    PageId::INVALID,
                    &initial[head_count..],
                );
                let old_bytes: Vec<_> = old_ids
                    .iter()
                    .map(|id| pages[id].as_bytes().to_vec())
                    .collect();
                let freed: Vec<_> = (0..freed_count).map(|i| PageId(4000 + i as u32)).collect();
                let hwm = alloc.high_water_mark();
                let (root, available) = process_chain(
                    &mut pages,
                    &mut alloc,
                    &mut Vec::new(),
                    &ChainCommit {
                        txn_id: TxnId(9),
                        current_root: old_ids[0],
                        freed_this_txn: &freed,
                        consumed: &FxHashSet::default(),
                        reclaim_horizon: horizon,
                    },
                )
                .unwrap();

                let replaced = freed_count != 0 && head_count < MAX_ENTRIES_PER_PAGE;
                let mut expected = initial.clone();
                if replaced {
                    expected.push(PendingFreeEntry {
                        page_id: old_ids[0],
                        freed_at_txn: TxnId(9),
                    });
                }
                expected.extend(freed.iter().map(|&page_id| PendingFreeEntry {
                    page_id,
                    freed_at_txn: TxnId(9),
                }));
                assert_same_entries(&read_chain(&pages, root).unwrap(), &expected);
                let eligible: Vec<_> = initial
                    .iter()
                    .filter(|entry| entry.freed_at_txn <= horizon)
                    .copied()
                    .collect();
                assert_same_entries(&available, &eligible);
                for (id, bytes) in old_ids.iter().zip(&old_bytes) {
                    assert_eq!(pages[id].as_bytes().as_slice(), bytes);
                }

                let prefix_entries = freed_count + if replaced { head_count + 1 } else { 0 };
                let prefix_pages = chain_pages_needed(prefix_entries);
                assert_eq!(alloc.high_water_mark(), hwm + prefix_pages as u32);
                let mut current = root;
                for i in 0..prefix_pages {
                    assert!(current.as_u32() >= hwm);
                    let page = &pages[&current];
                    let expected_count = if i == 0 {
                        prefix_entries - (prefix_pages - 1) * MAX_ENTRIES_PER_PAGE
                    } else {
                        MAX_ENTRIES_PER_PAGE
                    };
                    assert_eq!(read_entry_count(page), expected_count);
                    current = page.right_child();
                }
                assert_eq!(current, old_ids[usize::from(replaced)]);
            }
        }
    }
}

#[test]
fn a_pinned_horizon_retires_at_most_one_chain_page_per_append() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(0);
    let mut root = PageId::INVALID;
    let mut ordinary_frees = 0;
    let mut retired_heads = 0;
    for step in 1..=3000 {
        let old_ids = collect_chain_page_ids(&pages, root).unwrap();
        let old_bytes: Vec<_> = old_ids
            .iter()
            .map(|id| pages[id].as_bytes().to_vec())
            .collect();
        let replaced = root.is_valid() && read_entry_count(&pages[&root]) < MAX_ENTRIES_PER_PAGE;
        let freed = [alloc.allocate(), alloc.allocate()];
        ordinary_frees += freed.len();
        retired_heads += usize::from(replaced);
        assert!(pages.len() * citadel_core::PAGE_SIZE < 2 * 1024 * 1024);
        let (next, available) = process_chain(
            &mut pages,
            &mut alloc,
            &mut Vec::new(),
            &ChainCommit {
                txn_id: TxnId(step),
                current_root: root,
                freed_this_txn: &freed,
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId::ZERO,
            },
        )
        .unwrap();
        assert!(available.is_empty());
        let entries = read_chain(&pages, next).unwrap();
        assert_eq!(
            entries.len(),
            ordinary_frees + retired_heads,
            "commit {step}"
        );
        let ids = collect_chain_page_ids(&pages, next).unwrap();
        assert_eq!(ids.len(), chain_pages_needed(entries.len()));
        for (id, bytes) in old_ids.iter().zip(&old_bytes) {
            assert_eq!(pages[id].as_bytes().as_slice(), bytes);
        }
        if replaced {
            assert!(!ids.contains(&root));
            pages.remove(&root).unwrap();
        }
        assert_eq!(pages.len(), ids.len());
        assert!(alloc.high_water_mark() as usize <= ordinary_frees + step as usize + ids.len());
        alloc.commit();
        root = next;
    }
}

#[test]
fn an_unchanged_chain_becomes_loanable_after_reader_release() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(10_000);
    let initial: Vec<_> = (0..MAX_ENTRIES_PER_PAGE + 3)
        .map(|i| PendingFreeEntry {
            page_id: PageId(i as u32),
            freed_at_txn: TxnId(1),
        })
        .collect();
    let old_ids = structure_for(&mut alloc, &initial);
    let root = write_chain(&mut pages, TxnId(2), &initial, &old_ids);
    let old_bytes: Vec<_> = old_ids
        .iter()
        .map(|id| pages[id].as_bytes().to_vec())
        .collect();
    let hwm = alloc.high_water_mark();
    let mut available = Vec::new();
    for (txn, horizon) in [(3, TxnId::ZERO), (4, TxnId(u64::MAX))] {
        let before = pages.len();
        let (same_root, eligible) = process_chain(
            &mut pages,
            &mut alloc,
            &mut Vec::new(),
            &ChainCommit {
                txn_id: TxnId(txn),
                current_root: root,
                freed_this_txn: &[],
                consumed: &FxHashSet::default(),
                reclaim_horizon: horizon,
            },
        )
        .unwrap();
        assert_eq!(same_root, root);
        assert_eq!(pages.len(), before);
        assert_eq!(alloc.high_water_mark(), hwm);
        if txn == 3 {
            assert!(eligible.is_empty());
        }
        available = eligible;
    }
    assert_same_entries(&available, &initial);
    let mut loans: Vec<_> = available.iter().map(|entry| entry.page_id).collect();
    let (next, available) = process_chain(
        &mut pages,
        &mut alloc,
        &mut loans,
        &ChainCommit {
            txn_id: TxnId(5),
            current_root: root,
            freed_this_txn: &[],
            consumed: &FxHashSet::default(),
            reclaim_horizon: TxnId(u64::MAX),
        },
    )
    .unwrap();
    let new_ids = collect_chain_page_ids(&pages, next).unwrap();
    assert!(new_ids
        .iter()
        .all(|id| initial.iter().any(|entry| entry.page_id == *id)));
    assert_eq!(alloc.high_water_mark(), hwm);
    let surviving: Vec<_> = initial
        .iter()
        .filter(|entry| !new_ids.contains(&entry.page_id))
        .copied()
        .collect();
    assert_same_entries(&available, &surviving);
    let mut expected = surviving;
    expected.extend(old_ids.iter().map(|&page_id| PendingFreeEntry {
        page_id,
        freed_at_txn: TxnId(5),
    }));
    assert_same_entries(&read_chain(&pages, next).unwrap(), &expected);
    for (id, bytes) in old_ids.iter().zip(&old_bytes) {
        assert_eq!(pages[id].as_bytes().as_slice(), bytes);
    }
}

#[test]
fn an_empty_chain_without_frees_allocates_nothing() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(100);
    let (root, available) = process_chain(
        &mut pages,
        &mut alloc,
        &mut Vec::new(),
        &ChainCommit {
            txn_id: TxnId(1),
            current_root: PageId::INVALID,
            freed_this_txn: &[],
            consumed: &FxHashSet::default(),
            reclaim_horizon: TxnId(u64::MAX),
        },
    )
    .unwrap();
    assert_eq!(root, PageId::INVALID);
    assert!(available.is_empty());
    assert!(pages.is_empty());
    assert_eq!(alloc.high_water_mark(), 100);
}

#[test]
fn an_undrained_allocator_is_rejected_before_chain_mutation() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(100);
    let root = write_chain(
        &mut pages,
        TxnId(1),
        &[PendingFreeEntry {
            page_id: PageId(10),
            freed_at_txn: TxnId(1),
        }],
        &[alloc.allocate()],
    );
    let old_bytes = pages[&root].as_bytes().to_vec();
    alloc.add_ready_to_use(vec![PageId(10)]);
    let result = process_chain(
        &mut pages,
        &mut alloc,
        &mut Vec::new(),
        &ChainCommit {
            txn_id: TxnId(3),
            current_root: root,
            freed_this_txn: &[PageId(20)],
            consumed: &FxHashSet::default(),
            reclaim_horizon: TxnId(u64::MAX),
        },
    );
    assert!(matches!(result, Err(Error::DatabaseCorrupted)));
    assert_eq!(alloc.ready_count(), 1);
    assert_eq!(alloc.high_water_mark(), 101);
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[&root].as_bytes().as_slice(), old_bytes);
}

#[test]
fn prepending_does_not_skip_validation_of_the_shared_tail() {
    for freed in [&[][..], &[PageId(20)][..]] {
        let mut pages = FxHashMap::default();
        let mut alloc = PageAllocator::new(100);
        let head = alloc.allocate();
        let tail = alloc.allocate();
        write_chain_page(&mut pages, TxnId(1), head, tail, &[]);
        write_chain_page(&mut pages, TxnId(1), tail, tail, &[]);
        let result = process_chain(
            &mut pages,
            &mut alloc,
            &mut Vec::new(),
            &ChainCommit {
                txn_id: TxnId(3),
                current_root: head,
                freed_this_txn: freed,
                consumed: &FxHashSet::default(),
                reclaim_horizon: TxnId(u64::MAX),
            },
        );
        assert!(matches!(result, Err(Error::DatabaseCorrupted)));
        assert_eq!(alloc.high_water_mark(), 102);
        assert_eq!(pages.len(), 2);
    }
}

#[test]
fn chain_snapshot_validates_committed_entry_bounds_and_ages() {
    let high_water_mark = 101;
    let slot_txn = TxnId(6);
    for (page_id, freed_at_txn, valid) in [
        (PageId(0), TxnId(1), true),
        (PageId(99), slot_txn, true),
        (PageId::INVALID, TxnId(1), false),
        (PageId(high_water_mark), TxnId(1), false),
        (PageId(50), TxnId::ZERO, false),
        (PageId(50), TxnId(7), false),
    ] {
        let mut pages = FxHashMap::default();
        let root = write_chain(
            &mut pages,
            slot_txn,
            &[PendingFreeEntry {
                page_id,
                freed_at_txn,
            }],
            &[PageId(100)],
        );
        let result = ChainSnapshot::read_committed(root, high_water_mark, slot_txn, |id| {
            pages.get(&id).ok_or(Error::PageOutOfBounds(id))
        });
        assert_eq!(
            result.is_ok(),
            valid,
            "entry {page_id:?} at {freed_at_txn:?}"
        );
    }
}

#[test]
fn metadata_eligibility_requires_the_exact_retirement_and_an_intervening_commit() {
    for rewrite in [false, true] {
        let mut pages = FxHashMap::default();
        let mut alloc = PageAllocator::new(100);
        let initial: Vec<_> = (10..14)
            .map(|id| PendingFreeEntry {
                page_id: PageId(id),
                freed_at_txn: TxnId(4),
            })
            .collect();
        let root = write_chain(&mut pages, TxnId(4), &initial, &[alloc.allocate()]);
        let mut metadata: FxHashMap<_, _> = [
            (PageId(10), TxnId(4)),
            (PageId(11), TxnId(3)),
            (PageId(12), TxnId(4)),
        ]
        .into_iter()
        .collect();
        let consumed = if rewrite {
            [PageId(12)].into_iter().collect()
        } else {
            FxHashSet::default()
        };
        let snapshot =
            ChainSnapshot::read(root, |id| pages.get(&id).ok_or(Error::DatabaseCorrupted)).unwrap();
        let (next, available) = snapshot
            .process_with_metadata(
                &mut pages,
                &mut alloc,
                &mut Vec::new(),
                &ChainCommit {
                    txn_id: TxnId(5),
                    current_root: root,
                    freed_this_txn: &[PageId(20)],
                    consumed: &consumed,
                    reclaim_horizon: TxnId(1),
                },
                &mut metadata,
            )
            .unwrap();
        let expected: Vec<_> = initial
            .iter()
            .filter(|entry| {
                (entry.page_id == PageId(10) || entry.page_id == PageId(12))
                    && !consumed.contains(&entry.page_id)
            })
            .copied()
            .collect();
        assert_same_entries(&available, &expected);
        assert_eq!(metadata.get(&root), Some(&TxnId(5)));
        assert!(!metadata.contains_key(&PageId(20)));
        if rewrite {
            assert!(!metadata.contains_key(&PageId(12)));
        }
        let stored = read_chain(&pages, next).unwrap();
        assert!(stored.contains(&PendingFreeEntry {
            page_id: root,
            freed_at_txn: TxnId(5),
        }));

        let snapshot =
            ChainSnapshot::read(next, |id| pages.get(&id).ok_or(Error::DatabaseCorrupted)).unwrap();
        let (same, available) = snapshot
            .process_with_metadata(
                &mut pages,
                &mut alloc,
                &mut Vec::new(),
                &ChainCommit {
                    txn_id: TxnId(6),
                    current_root: next,
                    freed_this_txn: &[],
                    consumed: &FxHashSet::default(),
                    reclaim_horizon: TxnId(1),
                },
                &mut metadata,
            )
            .unwrap();
        assert_eq!(same, next);
        let mut expected = expected;
        expected.push(PendingFreeEntry {
            page_id: root,
            freed_at_txn: TxnId(5),
        });
        assert_same_entries(&available, &expected);
    }
}

#[test]
fn metadata_tags_follow_final_structure_consumption_at_the_loan_boundary() {
    for count in [
        MAX_ENTRIES_PER_PAGE - 1,
        MAX_ENTRIES_PER_PAGE,
        MAX_ENTRIES_PER_PAGE + 1,
    ] {
        let mut pages = FxHashMap::default();
        let mut alloc = PageAllocator::new(10_000);
        let entries: Vec<_> = (0..count)
            .map(|index| PendingFreeEntry {
                page_id: PageId(index as u32),
                freed_at_txn: TxnId(4),
            })
            .collect();
        let old_ids = structure_for(&mut alloc, &entries);
        let root = write_chain(&mut pages, TxnId(4), &entries, &old_ids);
        let first = entries[0].page_id;
        let last = entries[count - 1].page_id;
        let mut metadata: FxHashMap<_, _> =
            [(first, TxnId(4)), (last, TxnId(4))].into_iter().collect();
        let mut loans = vec![last, first];
        let snapshot =
            ChainSnapshot::read(root, |id| pages.get(&id).ok_or(Error::DatabaseCorrupted)).unwrap();
        let (next, available) = snapshot
            .process_with_metadata(
                &mut pages,
                &mut alloc,
                &mut loans,
                &ChainCommit {
                    txn_id: TxnId(5),
                    current_root: root,
                    freed_this_txn: &[PageId(9000)],
                    consumed: &FxHashSet::default(),
                    reclaim_horizon: TxnId(1),
                },
                &mut metadata,
            )
            .unwrap();
        let structure = collect_chain_page_ids(&pages, next).unwrap();
        for id in [first, last] {
            assert_eq!(metadata.contains_key(&id), !structure.contains(&id));
            assert_eq!(
                available.iter().any(|entry| entry.page_id == id),
                !structure.contains(&id)
            );
        }
        for old_id in old_ids {
            assert_eq!(metadata.get(&old_id), Some(&TxnId(5)));
            assert!(!available.iter().any(|entry| entry.page_id == old_id));
        }
    }
}

#[test]
fn an_invalid_committed_entry_stops_before_loading_the_next_page() {
    let mut pages = FxHashMap::default();
    let root = PageId(100);
    write_chain(
        &mut pages,
        TxnId(6),
        &[PendingFreeEntry {
            page_id: PageId(101),
            freed_at_txn: TxnId::ZERO,
        }],
        &[root],
    );
    pages.get_mut(&root).unwrap().set_right_child(PageId(102));
    let mut loaded = Vec::new();
    let result = ChainSnapshot::read_committed(root, 103, TxnId(6), |id| {
        loaded.push(id);
        pages.get(&id).ok_or(Error::PageOutOfBounds(id))
    });
    assert!(matches!(result, Err(Error::DatabaseCorrupted)));
    assert_eq!(loaded, vec![root]);
}

#[test]
fn chain_snapshot_conserves_entries_across_the_loan_overshoot_boundary() {
    for count in [
        MAX_ENTRIES_PER_PAGE - 1,
        MAX_ENTRIES_PER_PAGE,
        MAX_ENTRIES_PER_PAGE + 1,
    ] {
        let mut pages = FxHashMap::default();
        let mut alloc = PageAllocator::new(10_000);
        let mut initial: Vec<_> = (0..count)
            .map(|index| PendingFreeEntry {
                page_id: PageId(1000 + index as u32),
                freed_at_txn: TxnId(index as u64 % 6 + 1),
            })
            .collect();
        initial[0].freed_at_txn = TxnId(1);
        initial[count - 1].freed_at_txn = TxnId(3);
        let first = initial[0].page_id;
        let last = initial[count - 1].page_id;
        let old_ids = structure_for(&mut alloc, &initial);
        let root = write_chain(&mut pages, TxnId(7), &initial, &old_ids);
        let old_bytes: Vec<_> = old_ids
            .iter()
            .map(|id| pages[id].as_bytes().to_vec())
            .collect();
        let high_water_mark = alloc.high_water_mark();
        let snapshot = ChainSnapshot::read_committed(root, high_water_mark, TxnId(7), |id| {
            pages.get(&id).ok_or(Error::PageOutOfBounds(id))
        })
        .unwrap();
        let freed = [PageId(9000)];
        let mut loans = vec![last, first];
        let (new_root, available) = snapshot
            .process(
                &mut pages,
                &mut alloc,
                &mut loans,
                &ChainCommit {
                    txn_id: TxnId(9),
                    current_root: root,
                    freed_this_txn: &freed,
                    consumed: &FxHashSet::default(),
                    reclaim_horizon: TxnId(4),
                },
            )
            .unwrap();

        let used_loans = if count > MAX_ENTRIES_PER_PAGE {
            vec![first, last]
        } else {
            vec![first]
        };
        let mut expected_structure = used_loans.clone();
        if count == MAX_ENTRIES_PER_PAGE {
            expected_structure.push(PageId(high_water_mark));
        }
        assert_eq!(
            collect_chain_page_ids(&pages, new_root).unwrap(),
            expected_structure,
            "structure at entry count {count}"
        );
        assert_eq!(
            alloc.high_water_mark(),
            high_water_mark + u32::from(count == MAX_ENTRIES_PER_PAGE)
        );
        assert_eq!(
            loans,
            if count > MAX_ENTRIES_PER_PAGE {
                Vec::new()
            } else {
                vec![last]
            }
        );

        let surviving: Vec<_> = initial
            .iter()
            .filter(|entry| !used_loans.contains(&entry.page_id))
            .copied()
            .collect();
        let expected_available: Vec<_> = surviving
            .iter()
            .filter(|entry| entry.freed_at_txn <= TxnId(4))
            .copied()
            .collect();
        let mut expected = surviving;
        expected.extend(
            old_ids
                .iter()
                .chain(&freed)
                .map(|&page_id| PendingFreeEntry {
                    page_id,
                    freed_at_txn: TxnId(9),
                }),
        );
        assert_same_entries(&read_chain(&pages, new_root).unwrap(), &expected);
        assert_same_entries(&available, &expected_available);
        for (id, bytes) in old_ids.iter().zip(&old_bytes) {
            assert_eq!(pages[id].as_bytes().as_slice(), bytes.as_slice());
        }
    }
}

#[test]
fn chain_snapshot_updates_swapped_indices_and_handles_exhausted_loans() {
    let count = 2 * MAX_ENTRIES_PER_PAGE + 5;
    for loan_count in [0, 1, 3] {
        let mut pages = FxHashMap::default();
        let mut alloc = PageAllocator::new(10_000);
        let mut initial: Vec<_> = (0..count)
            .map(|index| PendingFreeEntry {
                page_id: PageId(index as u32),
                freed_at_txn: TxnId(if index % 2 == 0 { 1 } else { 4 }),
            })
            .collect();
        for index in [0, 1, MAX_ENTRIES_PER_PAGE, count - 2, count - 1] {
            initial[index].freed_at_txn = TxnId(1);
        }
        let old_ids = structure_for(&mut alloc, &initial);
        let root = write_chain(&mut pages, TxnId(5), &initial, &old_ids);
        let high_water_mark = alloc.high_water_mark();
        let snapshot =
            ChainSnapshot::read(root, |id| pages.get(&id).ok_or(Error::PageOutOfBounds(id)))
                .unwrap();
        let consumed: FxHashSet<_> = [initial[1].page_id, initial[MAX_ENTRIES_PER_PAGE].page_id]
            .into_iter()
            .collect();
        let mut loans = match loan_count {
            0 => Vec::new(),
            1 => vec![PageId(0)],
            _ => vec![
                initial[count - 2].page_id,
                initial[count - 1].page_id,
                PageId(0),
            ],
        };
        let used_loans: Vec<_> = loans.iter().rev().copied().collect();
        let (new_root, available) = snapshot
            .process(
                &mut pages,
                &mut alloc,
                &mut loans,
                &ChainCommit {
                    txn_id: TxnId(7),
                    current_root: root,
                    freed_this_txn: &[],
                    consumed: &consumed,
                    reclaim_horizon: TxnId(2),
                },
            )
            .unwrap();
        assert!(loans.is_empty());
        let mut expected_structure = used_loans.clone();
        expected_structure.extend((0..3 - loan_count).map(|index| PageId(high_water_mark + index)));
        assert_eq!(
            collect_chain_page_ids(&pages, new_root).unwrap(),
            expected_structure
        );
        assert_eq!(alloc.high_water_mark(), high_water_mark + 3 - loan_count);
        if loan_count != 0 {
            assert_eq!(new_root, PageId(0));
        }

        let surviving: Vec<_> = initial
            .iter()
            .filter(|entry| {
                !consumed.contains(&entry.page_id) && !used_loans.contains(&entry.page_id)
            })
            .copied()
            .collect();
        let expected_available: Vec<_> = surviving
            .iter()
            .filter(|entry| entry.freed_at_txn <= TxnId(2))
            .copied()
            .collect();
        let mut expected = surviving;
        expected.extend(old_ids.iter().map(|&page_id| PendingFreeEntry {
            page_id,
            freed_at_txn: TxnId(7),
        }));
        assert_same_entries(&read_chain(&pages, new_root).unwrap(), &expected);
        assert_same_entries(&available, &expected_available);
    }
}
