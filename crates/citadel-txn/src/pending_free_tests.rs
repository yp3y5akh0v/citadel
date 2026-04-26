use super::*;

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

    let root = write_chain(&mut pages, &mut alloc, TxnId(5), &entries);
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

    let root = write_chain(&mut pages, &mut alloc, TxnId(999), &entries);
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
    let mut alloc = PageAllocator::new(0);

    let root = write_chain(&mut pages, &mut alloc, TxnId(1), &[]);
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

    let root = write_chain(&mut pages, &mut alloc, TxnId(1), &entries);
    let chain_pages = collect_chain_page_ids(&pages, root).unwrap();
    assert_eq!(chain_pages.len(), 2); // Should span 2 pages
}

#[test]
fn process_chain_gc() {
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
    let root = write_chain(&mut pages, &mut alloc, TxnId(3), &initial_entries);

    let freed_this_txn = vec![PageId(40)];
    let (new_root, reclaimed, old_chain) = process_chain(
        &mut pages,
        &mut alloc,
        TxnId(4),
        root,
        &freed_this_txn,
        &[],
        TxnId(3),
    )
    .unwrap();

    assert_eq!(reclaimed.len(), 2);
    assert!(reclaimed.contains(&PageId(10)));
    assert!(reclaimed.contains(&PageId(20)));

    let new_entries = read_chain(&pages, new_root).unwrap();
    assert_eq!(new_entries.len(), 2);

    assert!(!old_chain.is_empty());
}

#[test]
fn process_chain_with_deferred_free() {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(0);

    let deferred = vec![PageId(50), PageId(51)];
    let freed = vec![PageId(60)];
    let (new_root, reclaimed, old_chain) = process_chain(
        &mut pages,
        &mut alloc,
        TxnId(1),
        PageId::INVALID,
        &freed,
        &deferred,
        TxnId(1),
    )
    .unwrap();

    assert!(reclaimed.is_empty());
    assert!(old_chain.is_empty());

    let entries = read_chain(&pages, new_root).unwrap();
    assert_eq!(entries.len(), 3); // 2 deferred + 1 freed
}

#[test]
fn max_entries_per_page_correct() {
    assert_eq!(MAX_ENTRIES_PER_PAGE, (8096 - 4) / 12);
}
