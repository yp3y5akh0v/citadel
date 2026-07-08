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
