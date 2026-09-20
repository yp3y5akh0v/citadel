// Deterministic reclamation transitions checked against an independent model.
// The oracle derives membership and provenance from committed page bytes and
// transaction events, never from CommittedReclaim's indexes or change enum.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

fn durable_chain(
    pages: &FxHashMap<PageId, Page>,
    root: PageId,
    high_water: u32,
    txn: TxnId,
) -> (BTreeSet<PageId>, BTreeMap<PageId, TxnId>) {
    let mut structure = BTreeSet::new();
    let mut entries = BTreeMap::new();
    let mut next = root;
    while next.is_valid() {
        assert!(next.as_u32() < high_water);
        assert!(structure.insert(next), "cycle or repeated structure page");
        let page = &pages[&next];
        assert_eq!(page.page_id(), next);
        assert!(page.verify_checksum(), "published chain page is not sealed");
        assert!(page.txn_id() <= txn);
        for entry in read_page_entries(page).unwrap() {
            assert!(entry.page_id.is_valid() && entry.page_id.as_u32() < high_water);
            assert!(entry.freed_at_txn > TxnId::ZERO && entry.freed_at_txn <= txn);
            assert!(
                entries.insert(entry.page_id, entry.freed_at_txn).is_none(),
                "duplicate retirement"
            );
        }
        next = page.right_child();
    }
    assert!(structure.iter().all(|id| !entries.contains_key(id)));
    (structure, entries)
}

#[test]
fn reclaim_protocol_matches_durable_membership_through_resets_and_aborts() {
    for count in [
        MAX_ENTRIES_PER_PAGE - 1,
        MAX_ENTRIES_PER_PAGE + 1,
        3 * MAX_ENTRIES_PER_PAGE + 17,
    ] {
        let mut random = 0x789a_bcde_1234_5678_u64 ^ count as u64;
        let mut next_random = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        let mut pages = FxHashMap::default();
        let mut allocator = PageAllocator::new(50_000);
        let initial: Vec<_> = (0..count)
            .map(|index| PendingFreeEntry {
                page_id: PageId(index as u32),
                freed_at_txn: TxnId(5),
            })
            .collect();
        let initial_ids: Vec<_> = (0..chain_pages_needed(count))
            .map(|_| allocator.allocate().unwrap())
            .collect();
        let mut root = write_chain(&mut pages, TxnId(10), &initial, &initial_ids);
        let mut high_water = allocator.high_water_mark();
        let mut slot_txn = TxnId(10);
        let mut state = CommittedReclaim::read_committed(root, high_water, slot_txn, |id| {
            pages.get(&id).ok_or(Error::PageOutOfBounds(id))
        })
        .unwrap();
        // The bool is independent RAM provenance: only structure retired by a
        // successfully published transition receives early metadata eligibility.
        let mut model: BTreeMap<_, _> = initial
            .iter()
            .map(|entry| (entry.page_id, (entry.freed_at_txn, false)))
            .collect();
        let mut published_loans = ReadyPages::default();
        let mut live = Vec::new();

        for step in 0..96_u64 {
            if step % 13 == 12 {
                // Revalidation can coexist with a retained, already-issued loan.
                // Durable reload deliberately forgets RAM-only provenance.
                state = CommittedReclaim::read_committed(root, high_water, slot_txn, |id| {
                    pages.get(&id).ok_or(Error::PageOutOfBounds(id))
                })
                .unwrap();
                for (_, metadata) in model.values_mut() {
                    *metadata = false;
                }
            } else if step % 11 == 10 {
                // Rebuilding just readiness must not duplicate the retained loan.
                state.reset_ready_progress();
            }
            let old_loans: Vec<_> = published_loans.iter().copied().collect();
            let (old_structure, old_entries) = durable_chain(&pages, root, high_water, slot_txn);
            assert_eq!(
                old_entries,
                model.iter().map(|(&id, &(age, _))| (id, age)).collect()
            );
            let old_bytes: Vec<_> = old_structure
                .iter()
                .map(|&id| (id, pages[&id].data))
                .collect();
            let mut writer = PageAllocator::with_ready(high_water, published_loans.clone());
            if step % 5 == 0 {
                let checkpoint = writer.checkpoint();
                for _ in 0..17 {
                    writer.allocate().unwrap();
                }
                writer.restore(checkpoint);
                assert!(writer.allocated_this_txn().is_empty());
                assert_eq!(writer.ready_count(), old_loans.len());
            }
            let allocations = match step % 8 {
                0 => 0,
                1 => 1,
                2 => 2,
                3 => MAX_ENTRIES_PER_PAGE + 3,
                _ => (next_random() % 19) as usize,
            };
            let mut candidate_live = live.clone();
            for _ in 0..allocations {
                candidate_live.push(writer.allocate().unwrap());
            }
            let free_count = candidate_live.len().min((next_random() % 23) as usize);
            for _ in 0..free_count {
                let index = next_random() as usize % candidate_live.len();
                writer.free(candidate_live.swap_remove(index));
            }
            let consumed: FxHashSet<_> = writer
                .allocated_this_txn()
                .iter()
                .copied()
                .filter(|id| id.as_u32() < high_water)
                .collect();
            assert!(consumed.iter().all(|id| old_loans.contains(id)));
            let freed = writer.commit();
            let mut remainder = writer.take_ready();
            let txn = TxnId(11 + step);
            let horizon = TxnId((1 + step / 8).min(slot_txn.as_u64()));
            let mut candidate_pages = pages.clone();
            let prepared = state
                .prepare(
                    &mut candidate_pages,
                    &mut writer,
                    &mut remainder,
                    &ChainCommit {
                        txn_id: txn,
                        current_root: root,
                        freed_this_txn: &freed,
                        consumed: &consumed,
                        reclaim_horizon: horizon,
                    },
                )
                .unwrap();
            assert!(
                state.matches(root, high_water, slot_txn),
                "prepare published cache state"
            );
            assert_eq!(
                published_loans.iter().copied().collect::<Vec<_>>(),
                old_loans
            );
            for (id, bytes) in old_bytes {
                assert_eq!(
                    candidate_pages[&id].data, bytes,
                    "prepare overwrote an active-slot chain page"
                );
            }
            // Emulate the manager's final seal before inspecting durable bytes.
            prepared.seal_staged_pages(&mut candidate_pages);
            let new_root = prepared.root();
            let (new_structure, new_entries) =
                durable_chain(&candidate_pages, new_root, writer.high_water_mark(), txn);
            let mut expected = model.clone();
            for id in &consumed {
                expected.remove(id);
            }
            for id in &new_structure {
                expected.remove(id);
            }
            for &id in &freed {
                assert!(expected.insert(id, (txn, false)).is_none());
            }
            for &id in old_structure.difference(&new_structure) {
                assert!(expected.insert(id, (txn, true)).is_none());
            }
            assert_eq!(
                new_entries,
                expected.iter().map(|(&id, &(age, _))| (id, age)).collect(),
                "count {count}, step {step}"
            );
            let expected_ready: BTreeSet<_> = model
                .iter()
                .filter_map(|(&id, &(age, metadata))| {
                    (!consumed.contains(&id)
                        && !new_structure.contains(&id)
                        && (age <= horizon || metadata))
                        .then_some(id)
                })
                .collect();
            let ready = prepared.ready_pages();
            let ready_ids: Vec<_> = ready.iter().copied().collect();
            let unique_ready: BTreeSet<_> = ready_ids.iter().copied().collect();
            assert_eq!(
                ready_ids.len(),
                unique_ready.len(),
                "duplicate reusable page"
            );
            assert_eq!(unique_ready, expected_ready, "count {count}, step {step}");
            assert!(candidate_live
                .iter()
                .all(|id| !new_entries.contains_key(id) && !unique_ready.contains(id)));

            if step % 7 == 6 {
                // Dropping prepared state simulates a failure before publication.
                drop(prepared);
                assert!(state.matches(root, high_water, slot_txn));
                continue;
            }
            state.publish(prepared, writer.high_water_mark());
            pages = candidate_pages;
            root = new_root;
            high_water = writer.high_water_mark();
            slot_txn = txn;
            published_loans = ready;
            live = candidate_live;
            model = expected;
        }
    }
}
