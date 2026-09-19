use super::*;
use citadel_buffer::allocator::PageAllocator;
use citadel_buffer::btree::BTree;
use citadel_buffer::cursor::PageMap;
use citadel_core::{Error, PageType, TxnId};
use citadel_page::overflow;
use rustc_hash::{FxHashMap, FxHashSet};
use std::cell::Cell;
use std::sync::Arc;

struct TestPages {
    source: FxHashMap<PageId, Arc<Page>>,
    loaded: FxHashMap<PageId, Arc<Page>>,
    lookups: Cell<usize>,
    leaf_loads: Vec<PageId>,
    fail_on: Option<PageId>,
}

impl PageMap for TestPages {
    fn get_page(&self, id: &PageId) -> Option<&Page> {
        self.lookups.set(self.lookups.get() + 1);
        self.loaded.get(id).map(AsRef::as_ref)
    }
}

impl PageLoader for TestPages {
    fn ensure_loaded(&mut self, id: PageId) -> Result<()> {
        if self.fail_on == Some(id) {
            return Err(Error::PageOutOfBounds(id));
        }
        if !self.loaded.contains_key(&id) {
            let Some(page) = self.source.get(&id) else {
                return Err(Error::PageOutOfBounds(id));
            };
            self.loaded.insert(id, Arc::clone(page));
        }
        Ok(())
    }
}

impl LeafLoader for TestPages {
    type Leaf = Arc<Page>;

    fn load_leaf(&mut self, id: PageId) -> Result<Self::Leaf> {
        self.ensure_loaded(id)?;
        self.leaf_loads.push(id);
        Ok(Arc::clone(&self.loaded[&id]))
    }
}

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn fixture() -> (TestPages, PageId, Rows) {
    let mut pages = FxHashMap::default();
    let mut alloc = PageAllocator::new(1);
    let txn = TxnId(1);
    let mut tree = BTree::new(&mut pages, &mut alloc, txn).unwrap();
    let mut expected = Vec::new();
    for index in 0..300u16 {
        let key = [b'a', index.to_be_bytes()[0], index.to_be_bytes()[1]];
        if index % 13 == 0 {
            tree.insert(&mut pages, &mut alloc, txn, &key, ValueType::Tombstone, &[])
                .unwrap();
            continue;
        }
        let value = vec![index as u8; if matches!(index, 17 | 89) { 9_000 } else { 128 }];
        if value.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
            let head = overflow::write_chain(
                &value,
                txn,
                || alloc.allocate_nonzero(),
                |id, page| {
                    pages.insert(id, page);
                },
            )
            .unwrap();
            let reference = OverflowRef {
                first_page: head,
                total_len: value.len() as u32,
            };
            tree.insert(
                &mut pages,
                &mut alloc,
                txn,
                &key,
                ValueType::Overflow,
                &reference.to_bytes(),
            )
            .unwrap();
        } else {
            tree.insert(&mut pages, &mut alloc, txn, &key, ValueType::Inline, &value)
                .unwrap();
        }
        expected.push((key.to_vec(), value));
    }
    // This out-of-prefix value would fail if materialized. Its reference must
    // not consume budget, increment telemetry, or enter the overflow loader.
    let outside = OverflowRef {
        first_page: PageId(999_999),
        total_len: 20_000,
    };
    tree.insert(
        &mut pages,
        &mut alloc,
        txn,
        b"b",
        ValueType::Overflow,
        &outside.to_bytes(),
    )
    .unwrap();
    (
        TestPages {
            source: pages
                .into_iter()
                .map(|(id, page)| (id, Arc::new(page)))
                .collect(),
            loaded: FxHashMap::default(),
            lookups: Cell::new(0),
            leaf_loads: Vec::new(),
            fail_on: None,
        },
        tree.root,
        expected,
    )
}

#[test]
fn range_leaf_handles_cover_seek_overflow_tombstones_prefix_and_exact_budget() {
    let (mut view, root, expected) = fixture();
    let start = [b'a', 0, 5];
    let expected: Rows = expected
        .into_iter()
        .filter(|(key, _)| key.as_slice() >= start.as_slice())
        .collect();
    let total = expected.iter().map(|(_, value)| value.len()).sum();
    let budget = ReadBudget::new(9_000, total);
    let mut scanned = 0;
    let mut rows = Vec::new();
    scan_from::<true, _, _>(
        &mut view,
        root,
        &start,
        Some(b"a"),
        None,
        Some(&budget),
        &mut scanned,
        |key, value| {
            rows.push((key.to_vec(), value.to_vec()));
            Ok(true)
        },
    )
    .unwrap();
    assert_eq!(rows, expected);
    assert_eq!(
        scanned, 295,
        "tombstones count; the prefix boundary does not"
    );
    assert_eq!(budget.remaining(), 0);
    assert!(!view.loaded.contains_key(&PageId(999_999)));
    assert!(view.leaf_loads.len() > 1);
    assert_eq!(
        view.leaf_loads
            .iter()
            .copied()
            .collect::<FxHashSet<_>>()
            .len(),
        view.leaf_loads.len()
    );
    assert!(
        view.lookups.get() < 100,
        "page-map work must not repeat per row: {}",
        view.lookups.get()
    );
}

#[test]
fn range_stop_error_and_budget_failure_keep_reached_row_counts() {
    for outcome in ["stop", "callback error", "budget"] {
        let (mut view, root, _) = fixture();
        let budget = ReadBudget::new(128, if outcome == "budget" { 127 } else { 128 });
        let mut scanned = 0;
        let mut called = 0;
        let result = scan_from::<true, _, _>(
            &mut view,
            root,
            b"a",
            Some(b"a"),
            None,
            Some(&budget),
            &mut scanned,
            |_, _| {
                called += 1;
                if outcome == "callback error" {
                    Err(Error::TransactionFailed)
                } else {
                    Ok(false)
                }
            },
        );
        match outcome {
            "stop" => assert!(result.is_ok()),
            "callback error" => assert!(matches!(result, Err(Error::TransactionFailed))),
            "budget" => assert!(matches!(
                result,
                Err(Error::ReadBudgetExceeded {
                    size: 128,
                    remaining: 127,
                    ..
                })
            )),
            _ => unreachable!(),
        }
        assert_eq!(called, usize::from(outcome != "budget"));
        assert_eq!(
            scanned, 2,
            "the initial tombstone and first live row were reached"
        );
        assert_eq!(view.leaf_loads.len(), 1);
        assert_eq!(
            view.loaded
                .values()
                .filter(|page| page.page_type() == Some(PageType::Leaf))
                .count(),
            1
        );
    }
}

#[test]
fn range_next_leaf_load_error_preserves_the_completed_prefix() {
    let (mut view, root, _) = fixture();
    let mut leaves: Vec<_> = view
        .source
        .values()
        .filter(|page| page.page_type() == Some(PageType::Leaf))
        .map(|page| {
            (
                leaf_node::read_cell(page, 0).key.to_vec(),
                page.page_id(),
                page.num_cells(),
            )
        })
        .collect();
    leaves.sort_by(|left, right| left.0.cmp(&right.0));
    let failed = leaves[1].1;
    view.fail_on = Some(failed);
    let mut scanned = 0;
    let result = scan_from::<true, _, _>(
        &mut view,
        root,
        b"a",
        Some(b"a"),
        None,
        None,
        &mut scanned,
        |_, _| Ok(true),
    );
    assert!(matches!(result, Err(Error::PageOutOfBounds(id)) if id == failed));
    assert_eq!(scanned, u64::from(leaves[0].2));
    assert_eq!(view.leaf_loads, [leaves[0].1]);
}

#[test]
fn range_entry_points_preserve_row_and_leaf_cancellation_cadence() {
    let manager = crate::manager::tests::create_test_manager();
    let mut writer = manager.begin_write().unwrap();
    writer.create_table(b"range").unwrap();
    for key in [b"aa", b"ab", b"ac"] {
        writer.table_insert(b"range", key, b"v").unwrap();
    }
    writer.commit().unwrap();
    for mode in ["read", "read fast", "write", "read prefix", "write prefix"] {
        for keep_scanning in [false, true] {
            let token = CancelToken::new();
            let mut called = 0;
            let before = manager.rows_scanned();
            let mut visit = |_: &[u8], _: &[u8]| {
                called += 1;
                token.cancel();
                Ok(keep_scanning)
            };
            let result = if mode.starts_with("write") {
                let mut writer = manager.begin_write().unwrap();
                writer.set_cancel(Some(token.clone()));
                let result = if mode == "write prefix" {
                    writer.table_scan_prefix(b"range", b"a", &mut visit)
                } else {
                    writer.table_scan_from(b"range", b"", &mut visit)
                };
                assert!(
                    !writer.is_poisoned(),
                    "read-only cancellation must not poison"
                );
                result
            } else {
                let mut reader = manager.begin_read();
                reader.set_cancel(Some(token.clone()));
                match mode {
                    "read fast" => reader.table_scan_from_fast(b"range", b"", &mut visit),
                    "read prefix" => reader.table_scan_prefix(b"range", b"a", &mut visit),
                    _ => reader.table_scan_from(b"range", b"", &mut visit),
                }
            };
            let finishes_leaf = mode == "read fast" && keep_scanning;
            let expected = if finishes_leaf { 3 } else { 1 };
            assert_eq!(called, expected, "{mode}");
            assert_eq!(manager.rows_scanned() - before, expected as u64, "{mode}");
            if finishes_leaf {
                assert!(
                    result.is_ok(),
                    "the fast scan checks only before another leaf"
                );
            } else if keep_scanning {
                assert!(matches!(result, Err(Error::Interrupted)), "{mode}");
            } else {
                assert!(
                    result.is_ok(),
                    "callback stop ends the scan before a next-row check"
                );
            }
        }
    }
}
