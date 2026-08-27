//! Every scan shape must stop when the token is tripped.
//!
//! Five shapes do not share a loop: the lazy cursor scan, the leaf-at-a-time
//! `_fast` scan indexed reads use, the pull-based iterator, the write-side scan
//! behind UPDATE and DELETE, and the sharded scanner on worker threads. Cover
//! only the obvious one and cancellation is dead on exactly the long queries it
//! exists for, with every test still green.

use crate::manager::tests::create_test_manager;
use crate::manager::TxnManager;
use crate::write_txn::cancel_on_nth_write_check;
use citadel_core::{CancelToken, Error};

const TABLE: &[u8] = b"rows";
const ROWS: u32 = 4_000;

fn seeded() -> TxnManager {
    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(TABLE).unwrap();
    for i in 0..ROWS {
        wtx.table_insert(TABLE, format!("k{i:06}").as_bytes(), b"v")
            .unwrap();
    }
    wtx.commit().unwrap();
    mgr
}

const OVERFLOW_KEY: &[u8] = b"a_overflow";

fn seeded_overflow() -> (TxnManager, Vec<u8>) {
    let mgr = create_test_manager();
    let value = vec![b'O'; citadel_page::overflow::OVERFLOW_DATA_CAPACITY * 6 + 17];
    let mut wtx = mgr.begin_write().unwrap();
    wtx.create_table(TABLE).unwrap();
    wtx.table_insert(TABLE, OVERFLOW_KEY, &value).unwrap();
    // Keeps the scan live after the overflow callback cancels, so the next
    // storage observation deterministically reports the interruption.
    wtx.table_insert(TABLE, b"z_tail", b"tail").unwrap();
    wtx.commit().unwrap();
    (mgr, value)
}

fn catalog_depth(mgr: &TxnManager) -> usize {
    let mut current = mgr.current_slot().catalog_root;
    let mut depth = 0usize;
    loop {
        depth += 1;
        let page = mgr.fetch_page(current).unwrap();
        match page.page_type() {
            Some(citadel_core::types::PageType::Leaf) => return depth,
            Some(citadel_core::types::PageType::Branch) => {
                current = citadel_page::branch_node::get_child(&page, 0);
            }
            other => panic!("unexpected catalog page type {other:?}"),
        }
    }
}

/// Trip the token partway in, so the scan is genuinely interrupted rather than
/// refused before it starts.
const CANCEL_AFTER: usize = 50;

#[test]
fn a_lazy_cursor_scan_stops() {
    let mgr = seeded();
    let mut rtx = mgr.begin_read();
    let token = CancelToken::new();
    rtx.set_cancel(Some(token.clone()));

    let mut seen = 0usize;
    let err = rtx
        .table_scan_from(TABLE, b"", |_, _| {
            seen += 1;
            if seen == CANCEL_AFTER {
                token.cancel();
            }
            Ok(true)
        })
        .unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert!(seen < ROWS as usize, "the scan ran to completion anyway");
}

/// This duplicates the lazy loop independently, and is what indexed reads use.
#[test]
fn the_fast_scan_stops() {
    let mgr = seeded();
    let mut rtx = mgr.begin_read();
    let token = CancelToken::new();
    rtx.set_cancel(Some(token.clone()));

    let mut seen = 0usize;
    let err = rtx
        .table_scan_from_fast(TABLE, b"", |_, _| {
            seen += 1;
            if seen == CANCEL_AFTER {
                token.cancel();
            }
            Ok(true)
        })
        .unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert!(seen < ROWS as usize, "the scan ran to completion anyway");
}

#[test]
fn the_raw_leaf_scan_stops() {
    let mgr = seeded();
    let mut rtx = mgr.begin_read();
    let token = CancelToken::new();
    rtx.set_cancel(Some(token.clone()));

    let mut seen = 0usize;
    let err = rtx
        .table_scan_raw(TABLE, |_, _| {
            seen += 1;
            if seen == 50 {
                token.cancel();
            }
            true
        })
        .unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert!(seen < ROWS as usize, "the scan ran to completion anyway");
}

/// A pull-based iterator has no loop of its own, so it needs the adapter's.
#[test]
fn the_pull_iterator_stops() {
    let mgr = seeded();
    let mut rtx = mgr.begin_read();
    let token = CancelToken::new();
    rtx.set_cancel(Some(token.clone()));

    let mut iter = rtx.table_scan_iter(TABLE, b"").unwrap();
    let mut seen = 0usize;
    let err = loop {
        match iter.next() {
            Ok(Some(_)) => {
                seen += 1;
                if seen == 50 {
                    token.cancel();
                }
            }
            Ok(None) => panic!("the iterator finished instead of stopping"),
            Err(e) => break e,
        }
    };

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert!(seen < ROWS as usize);
}

#[test]
fn the_owned_pull_iterator_stops_too() {
    let mgr = seeded();
    let mut rtx = mgr.begin_read();
    let token = CancelToken::new();
    rtx.set_cancel(Some(token.clone()));

    let mut iter = rtx.into_table_scan_iter(TABLE, b"").unwrap();
    let mut seen = 0usize;
    let err = loop {
        match iter.next() {
            Ok(Some(_)) => {
                seen += 1;
                if seen == 50 {
                    token.cancel();
                }
            }
            Ok(None) => panic!("the iterator finished instead of stopping"),
            Err(e) => break e,
        }
    };

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
}

#[test]
fn a_pull_iterator_checks_cancellation_before_first_eof_but_not_after_completion() {
    let mgr = seeded();

    let token = CancelToken::new();
    let mut rtx = mgr.begin_read();
    rtx.set_cancel(Some(token.clone()));
    let mut iter = rtx.table_scan_iter(TABLE, b"zzzzzz").unwrap();
    token.cancel();
    assert!(matches!(iter.next(), Err(Error::Interrupted)));

    let token = CancelToken::new();
    let mut rtx = mgr.begin_read();
    rtx.set_cancel(Some(token.clone()));
    let mut iter = rtx.table_scan_iter(TABLE, b"zzzzzz").unwrap();
    assert!(matches!(iter.next(), Ok(None)));
    token.cancel();
    assert!(
        matches!(iter.next(), Ok(None)),
        "cancellation after observed completion must stay harmless"
    );
}

/// UPDATE and DELETE scan through the write txn: cancelling only the read path
/// compiles, passes, and leaves the feature half dead.
#[test]
fn a_write_scan_stops_and_its_transaction_rolls_back() {
    let mgr = seeded();

    let mut wtx = mgr.begin_write().unwrap();
    let token = CancelToken::new();
    wtx.set_cancel(Some(token.clone()));

    let mut seen = 0usize;
    let err = wtx
        .table_scan_from(TABLE, b"", |_, _| {
            seen += 1;
            if seen == 50 {
                token.cancel();
            }
            Ok(true)
        })
        .unwrap_err();
    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert!(seen < ROWS as usize, "the write scan ran to completion");

    // An interrupted write transaction was never committed, so dropping it must
    // leave the table exactly as it was. The token is cleared first because
    // mutations refuse under a tripped one, and what is being shown here is the
    // rollback rather than that refusal.
    wtx.set_cancel(None);
    wtx.table_insert(TABLE, b"k000000", b"CLOBBERED").unwrap();
    drop(wtx);

    let mut rtx = mgr.begin_read();
    assert_eq!(
        rtx.table_get(TABLE, b"k000000").unwrap(),
        Some(b"v".to_vec()),
        "the interrupted transaction's write survived the rollback"
    );
}

/// The one scan that mutates as it walks. By the time it sees the token, the
/// rows behind its cursor are already patched and nothing puts them back, so
/// the transaction it stopped inside may only be aborted.
///
/// The discriminating step is clearing the token before COMMIT: with no token
/// left, `commit`'s own check passes and only the poison can refuse.
#[test]
fn an_interrupted_mutating_scan_poisons_its_transaction() {
    let mgr = seeded();

    let mut wtx = mgr.begin_write().unwrap();
    let token = CancelToken::new();
    wtx.set_cancel(Some(token.clone()));

    let mut patched = 0usize;
    let err = wtx
        .table_update_range::<_, Error>(TABLE, b"", |_, value| {
            value[0] = b'X';
            patched += 1;
            if patched == CANCEL_AFTER {
                token.cancel();
            }
            Ok(Some(true))
        })
        .unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert_eq!(patched, CANCEL_AFTER, "the scan stopped somewhere else");
    assert!(wtx.is_poisoned(), "an interrupted write scan left no mark");

    wtx.set_cancel(None);
    let err = wtx.commit().unwrap_err();
    assert!(matches!(err, Error::Interrupted), "got {err:?}");

    let mut rtx = mgr.begin_read();
    assert_eq!(
        rtx.table_get(TABLE, b"k000000").unwrap(),
        Some(b"v".to_vec()),
        "the refused commit still let the patched prefix through"
    );
}

/// Stopping from the callback is still an operation exit. If the same callback
/// trips the token, the scan must observe it before reporting success; otherwise
/// a caller can install the next operation's fresh token and commit this prefix.
#[test]
fn a_cancelled_callback_stop_poisons_its_mutating_scan() {
    let mgr = seeded();

    let mut wtx = mgr.begin_write().unwrap();
    let token = CancelToken::new();
    wtx.set_cancel(Some(token.clone()));

    let err = wtx
        .table_update_range::<_, Error>(TABLE, b"", |_, value| {
            value[0] = b'X';
            token.cancel();
            Ok(None)
        })
        .unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert!(wtx.is_poisoned(), "the stopped write scan left no mark");

    wtx.set_cancel(None);
    assert!(matches!(wtx.commit(), Err(Error::Interrupted)));

    let mut rtx = mgr.begin_read();
    assert_eq!(
        rtx.table_get(TABLE, b"k000000").unwrap(),
        Some(b"v".to_vec()),
        "the refused commit still let the callback's mutation through"
    );
}

#[test]
fn cancelling_an_overflow_rewrite_poisons_the_partially_mutated_transaction() {
    let (mgr, original) = seeded_overflow();
    let mut wtx = mgr.begin_write().unwrap();
    let token = CancelToken::new();
    wtx.set_cancel(Some(token.clone()));

    let err = wtx
        .table_update_range::<_, Error>(TABLE, OVERFLOW_KEY, |key, value| {
            assert_eq!(key, OVERFLOW_KEY);
            assert_eq!(value.len(), original.len());
            value[0] = b'X';
            // The callback has selected a mutation. The implementation writes
            // its replacement chain and cell reference before freeing the old
            // multi-page chain, whose entry check observes this cancellation.
            token.cancel();
            Ok(Some(true))
        })
        .unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert!(
        wtx.is_poisoned(),
        "the rewritten overflow cell was committable"
    );
    wtx.set_cancel(None);
    assert!(matches!(wtx.commit(), Err(Error::Interrupted)));

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.table_get(TABLE, OVERFLOW_KEY).unwrap(), Some(original));
}

#[test]
fn cancelling_a_read_only_overflow_scan_does_not_poison_the_write_txn() {
    let (mgr, original) = seeded_overflow();
    let mut wtx = mgr.begin_write().unwrap();
    let token = CancelToken::new();
    wtx.set_cancel(Some(token.clone()));
    let mut seen = 0usize;

    let err = wtx
        .table_for_each(TABLE, |key, value| {
            seen += 1;
            assert_eq!(key, OVERFLOW_KEY);
            assert_eq!(value, original);
            token.cancel();
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert_eq!(seen, 1);
    assert!(
        !wtx.is_poisoned(),
        "materializing a value changed no transactional state"
    );
    wtx.set_cancel(None);
    wtx.commit().unwrap();
}

#[test]
fn catalog_depth_and_count_walks_observe_cancellation_without_poisoning() {
    let mgr = create_test_manager();
    let mut seed = mgr.begin_write().unwrap();
    for i in 0..400u32 {
        seed.create_table(format!("catalog_{i:04}").as_bytes())
            .unwrap();
    }
    seed.commit().unwrap();
    let depth = catalog_depth(&mgr);
    assert!(depth > 1, "the fixture needs a multi-page catalog");

    let mut wtx = mgr.begin_write().unwrap();
    let first = CancelToken::new();
    wtx.set_cancel(Some(first));
    {
        // create_table's entry check is #1; #2 is the first page in
        // catalog_slot_from_disk's depth walk.
        let _cancel = cancel_on_nth_write_check(2);
        let err = wtx.create_table(b"cancel_in_depth").unwrap_err();
        assert!(matches!(err, Error::Interrupted), "got {err:?}");
    }
    assert!(!wtx.is_poisoned(), "catalog reconstruction is read-only");

    let second = CancelToken::new();
    wtx.set_cancel(Some(second));
    {
        // One public entry check plus `depth` catalog-slot checks puts this
        // exactly at count_leaf_entries' first page.
        let _cancel = cancel_on_nth_write_check(depth + 2);
        let err = wtx.create_table(b"cancel_in_count").unwrap_err();
        assert!(matches!(err, Error::Interrupted), "got {err:?}");
    }
    assert!(!wtx.is_poisoned(), "counting catalog leaves is read-only");

    wtx.set_cancel(None);
    wtx.create_table(b"catalog_retry").unwrap();
    wtx.commit().unwrap();
    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.table_entry_count(b"catalog_retry").unwrap(), 0);
}

#[test]
fn cancellation_during_partial_catalog_finalization_aborts_the_commit() {
    const TABLES: usize = 8;

    let mgr = create_test_manager();
    let mut wtx = mgr.begin_write().unwrap();
    for i in 0..TABLES {
        wtx.create_table(format!("pending_{i}").as_bytes()).unwrap();
    }
    let token = CancelToken::new();
    wtx.set_cancel(Some(token));

    let err = {
        // finalize_catalog's entry check is #1, collecting TABLES descriptors
        // is #2..=TABLES+1, and the insert checks follow. Trip at the second
        // insert, after one catalog entry has already been mutated in memory.
        let _cancel = cancel_on_nth_write_check(TABLES + 3);
        wtx.commit().unwrap_err()
    };
    assert!(matches!(err, Error::Interrupted), "got {err:?}");

    let mut rtx = mgr.begin_read();
    assert!(matches!(
        rtx.table_entry_count(b"pending_0"),
        Err(Error::TableNotFound(_))
    ));
}

#[test]
fn cancelled_insert_if_absent_does_not_stage_before_catalog_lookup() {
    let mgr = seeded();
    let high_water_before = mgr.current_slot().high_water_mark;
    let mut wtx = mgr.begin_write().unwrap();
    let token = CancelToken::new();
    wtx.set_cancel(Some(token));
    let large = vec![b'L'; citadel_page::overflow::OVERFLOW_DATA_CAPACITY * 3 + 1];

    let err = {
        // #1 is the public mutation entry. #2 is the first catalog depth
        // check while resolving a table absent from both the slot and catalog.
        let _cancel = cancel_on_nth_write_check(2);
        wtx.table_insert_if_absent(b"missing_table", b"key", &large)
            .unwrap_err()
    };
    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert!(
        !wtx.is_poisoned(),
        "catalog lookup is read-only and should run before staging"
    );

    // Discriminate this from merely checking commit's live token: the failed
    // operation must not leave staged orphan pages that advance the allocator.
    wtx.set_cancel(None);
    wtx.commit().unwrap();
    assert_eq!(mgr.current_slot().high_water_mark, high_water_before);
}

/// The scans that CHOOSE rows are cancellable; the loops that APPLY them live
/// in the SQL executor, spread across index maintenance, primary-key moves and
/// the delete pass. Checking at the mutation itself is what covers all of them
/// at once, so a cancel cannot be swallowed by whichever loop was running.
#[test]
fn every_mutation_primitive_refuses_once_the_token_is_tripped() {
    let mgr = seeded();
    let mut wtx = mgr.begin_write().unwrap();
    let token = CancelToken::new();
    token.cancel();
    wtx.set_cancel(Some(token));

    let stopped = |name: &str, err: Error| {
        assert!(matches!(err, Error::Interrupted), "{name}: got {err:?}");
    };
    stopped(
        "table_insert",
        wtx.table_insert(TABLE, b"zz", b"v").unwrap_err(),
    );
    stopped("root insert", wtx.insert(b"root", b"v").unwrap_err());
    stopped("root delete", wtx.delete(b"root").unwrap_err());
    stopped(
        "table_insert_index",
        wtx.table_insert_index(TABLE, b"zz", b"v").unwrap_err(),
    );
    stopped(
        "table_insert_if_absent",
        wtx.table_insert_if_absent(TABLE, b"zz", b"v").unwrap_err(),
    );
    stopped(
        "table_delete",
        wtx.table_delete(TABLE, b"k000000").unwrap_err(),
    );
    stopped(
        "table_update_sorted",
        wtx.table_update_sorted(TABLE, &[(b"k000000".as_slice(), b"x".as_slice())])
            .unwrap_err(),
    );
    stopped("table_truncate", wtx.table_truncate(TABLE).unwrap_err());
    stopped("create_table", wtx.create_table(b"new_table").unwrap_err());
    stopped("drop_table", wtx.drop_table(TABLE).unwrap_err());
    stopped(
        "rename_table",
        wtx.rename_table(TABLE, b"renamed").unwrap_err(),
    );

    // Refused before touching anything, so the transaction itself is intact -
    // the primitive cannot tell a statement's first mutation from its hundredth,
    // so poisoning is left to the layer that knows.
    assert!(!wtx.is_poisoned());
}

#[test]
fn point_reads_and_empty_pull_scans_refuse_at_the_door() {
    let mgr = seeded();
    let token = CancelToken::new();
    token.cancel();

    let mut rtx = mgr.begin_read();
    rtx.set_cancel(Some(token.clone()));
    assert!(matches!(rtx.get(b"root"), Err(Error::Interrupted)));
    assert!(matches!(
        rtx.table_get(TABLE, b"missing"),
        Err(Error::Interrupted)
    ));
    assert!(matches!(
        rtx.table_scan_iter(TABLE, b"zzzzzz"),
        Err(Error::Interrupted)
    ));

    let mut owned = mgr.begin_read();
    owned.set_cancel(Some(token.clone()));
    assert!(matches!(
        owned.into_table_scan_iter(TABLE, b"zzzzzz"),
        Err(Error::Interrupted)
    ));

    let mut wtx = mgr.begin_write().unwrap();
    wtx.set_cancel(Some(token));
    assert!(matches!(wtx.get(b"root"), Err(Error::Interrupted)));
    assert!(matches!(
        wtx.table_get(TABLE, b"missing"),
        Err(Error::Interrupted)
    ));
    assert!(matches!(
        wtx.table_scan_iter(TABLE, b"zzzzzz"),
        Err(Error::Interrupted)
    ));
    assert!(!wtx.is_poisoned(), "nothing ran, so the txn stays usable");
}

#[test]
fn fast_scan_counts_only_rows_reached_before_callback_stop() {
    let mgr = seeded();
    let before = mgr.rows_scanned();
    let mut rtx = mgr.begin_read();
    rtx.table_scan_from_fast(TABLE, b"", |_, _| Ok(false))
        .unwrap();
    assert_eq!(mgr.rows_scanned() - before, 1);
}

/// A write scan that was never interrupted commits normally. Without this the
/// poison could be set unconditionally and nothing would fail.
#[test]
fn an_uninterrupted_mutating_scan_commits() {
    let mgr = seeded();

    let mut wtx = mgr.begin_write().unwrap();
    wtx.set_cancel(Some(CancelToken::new()));
    let patched = wtx
        .table_update_range::<_, Error>(TABLE, b"", |_, value| {
            value[0] = b'X';
            Ok(Some(true))
        })
        .unwrap();

    assert_eq!(patched, u64::from(ROWS));
    assert!(!wtx.is_poisoned());
    wtx.commit().unwrap();

    let mut rtx = mgr.begin_read();
    assert_eq!(
        rtx.table_get(TABLE, b"k000000").unwrap(),
        Some(b"X".to_vec())
    );
}

/// Shards really do run on worker threads, each with its own scanner.
/// A flag held per shard would let the rest run on after one stopped, so the
/// token has to be the shared one and every shard has to see it.
#[test]
fn every_shard_stops_on_its_own_worker_thread() {
    let mgr = seeded();
    let mut rtx = mgr.begin_read();
    let token = CancelToken::new();
    rtx.set_cancel(Some(token.clone()));

    let leaves = rtx.collect_table_leaves(TABLE).unwrap();
    assert!(
        leaves.len() > 1,
        "a single leaf would not exercise sharding at all"
    );
    let (first, rest) = leaves.split_at(1);

    // Cancelled from this thread while the shards are the ones reading.
    token.cancel();

    let outcomes = std::thread::scope(|s| {
        let shards = [first, rest].map(|shard| {
            let mut scanner = rtx.shard_scanner();
            s.spawn(move || {
                let mut seen = 0usize;
                let result = scanner.scan_leaves(shard, |_, _| {
                    seen += 1;
                    true
                });
                (seen, result)
            })
        });
        shards.map(|h| h.join().unwrap())
    });

    for (seen, result) in outcomes {
        let err = result.expect_err("a shard ran to completion after the cancel");
        assert!(matches!(err, Error::Interrupted), "got {err:?}");
        assert_eq!(seen, 0, "an already-cancelled shard emitted rows");
    }
}

/// The six loops a name-based sweep for "scan" misses. `UPDATE` with no WHERE
/// reaches `table_for_each` through a fused fast lane, so leaving these out
/// makes the commonest bulk UPDATE uncancellable with every test still green.
#[test]
fn the_for_each_loops_stop_too() {
    let mgr = seeded();

    for on_write in [false, true] {
        let token = CancelToken::new();
        let mut seen = 0usize;
        let result = if on_write {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.set_cancel(Some(token.clone()));
            wtx.table_for_each(TABLE, |_, _| {
                seen += 1;
                if seen == CANCEL_AFTER {
                    token.cancel();
                }
                Ok(())
            })
        } else {
            let mut rtx = mgr.begin_read();
            rtx.set_cancel(Some(token.clone()));
            rtx.table_for_each(TABLE, |_, _| {
                seen += 1;
                if seen == CANCEL_AFTER {
                    token.cancel();
                }
                Ok(())
            })
        };

        let err = result.expect_err("table_for_each ran to completion");
        assert!(matches!(err, Error::Interrupted), "got {err:?}");
        assert!(seen < ROWS as usize, "on_write={on_write}");
    }
}

/// `preload_all_pages` completes before `for_each` yields anything, so a scan
/// that only checked its emit loop would sit through the whole tree walk.
#[test]
fn the_preload_walk_stops_before_the_first_row() {
    let mgr = seeded();
    let mut rtx = mgr.begin_read();
    let token = CancelToken::new();
    token.cancel();
    rtx.set_cancel(Some(token));

    let mut seen = 0usize;
    let err = rtx
        .table_for_each(TABLE, |_, _| {
            seen += 1;
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
    assert_eq!(seen, 0, "rows were emitted after the cancel");
}

/// The default path must be untouched: no token, no interruption, same results.
#[test]
fn a_scan_without_a_token_still_runs_to_completion() {
    let mgr = seeded();
    let mut rtx = mgr.begin_read();
    assert!(rtx.cancel_token().is_none());

    let mut seen = 0usize;
    rtx.table_scan_raw(TABLE, |_, _| {
        seen += 1;
        true
    })
    .unwrap();

    assert_eq!(seen, ROWS as usize);
}

/// The descent that collects leaves runs to completion before `table_scan_raw`
/// emits its first row, so it needs its own check.
#[test]
fn collecting_leaves_stops_before_a_single_row_is_emitted() {
    let mgr = seeded();
    let mut rtx = mgr.begin_read();
    let token = CancelToken::new();
    token.cancel();
    rtx.set_cancel(Some(token));

    let err = rtx.collect_table_leaves(TABLE).unwrap_err();

    assert!(matches!(err, Error::Interrupted), "got {err:?}");
}
