use super::*;

const MAC_KEY: [u8; KEY_SIZE] = [0x7c; KEY_SIZE];
const FILE_ID: u64 = 0xA70A_0000_0000_0001;

fn wrapped(b: u8) -> [u8; WRAPPED_KEY_SIZE] {
    [b; WRAPPED_KEY_SIZE]
}

fn store(dir: &std::path::Path) -> AtomKeyStore {
    AtomKeyStore::create_or_open(&dir.join("db.citadel-atomkeys"), FILE_ID, MAC_KEY).unwrap()
}

fn install_authenticated_slot(
    store: &AtomKeyStore,
    slot: u32,
    state: SlotState,
    owner: u64,
    generation: u64,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
) {
    let block = build_slot_block(&MAC_KEY, state, owner, generation, wrapped);
    for copy_b in [false, true] {
        overwrite_in_place(&store.path, slot_offset(slot, copy_b), &block).unwrap();
    }
}

#[test]
fn atom_store_header_format_has_a_stable_known_answer() {
    let key = std::array::from_fn(|index| index as u8);
    let expected = [
        0x53, 0x4d, 0x54, 0x41, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
        0xa7, 0x07, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x63,
        0xdf, 0x21, 0x18, 0xa7, 0x20, 0xcb, 0xc5, 0xb1, 0x81, 0xa1, 0x1b, 0x23, 0x42, 0xe6, 0xfd,
        0x4c, 0xee, 0xec, 0xc4, 0xcd, 0x83, 0x5d, 0x56, 0xce, 0xd7, 0x89, 0x8e, 0xd2, 0xbc, 0xda,
    ];

    let encoded = build_header(&key, 0xA70A_0000_0000_0001, 7, 42);
    assert_eq!(&encoded[..key_codec::HEADER_MAC_INPUT + 32], &expected);
    assert_eq!(
        parse_header(&key, 0xA70A_0000_0000_0001, &expected),
        Some((7, 42))
    );
}

#[test]
fn create_preallocates_empty_slots() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path());
    assert_eq!(s.slot_count(), ATOM_STORE_PREALLOC_SLOTS);
    for i in 0..s.slot_count() {
        assert_eq!(s.read_slot(i).unwrap().state, SlotState::Empty);
    }
}

#[test]
fn allocate_write_read_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let gen = s.write_live(slot, 42, &wrapped(0xAB)).unwrap();
    let rec = s.read_slot(slot).unwrap();
    assert_eq!(rec.state, SlotState::Live);
    assert_eq!(rec.region_id, 42);
    assert_eq!(rec.gen, gen);
    assert_eq!(rec.wrapped, wrapped(0xAB));
}

#[test]
fn selected_slot_batch_preserves_order_and_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path());
    let first = store.allocate_write(41, &wrapped(0xA1)).unwrap().0;
    let second = store.allocate_write(42, &wrapped(0xA2)).unwrap().0;

    let records = store.read_slots(&[second, first, second]).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.region_id)
            .collect::<Vec<_>>(),
        [42, 41, 42]
    );
    assert_eq!(records[0].wrapped, wrapped(0xA2));
    assert_eq!(records[1].wrapped, wrapped(0xA1));
}

#[test]
fn allocate_write_scrubs_and_releases_a_post_write_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let secret = wrapped(0xD1);

    FAIL_LIVE_READBACK.with(|fail| fail.set(true));
    let error = s.allocate_write(41, &secret).unwrap_err();
    assert!(matches!(error, Error::RegionStoreCorrupt(_)));

    let record = s.read_slot(0).unwrap();
    assert_eq!(record.state, SlotState::Tombstone);
    let bytes = std::fs::read(&s.path).unwrap();
    assert!(
        !bytes
            .windows(WRAPPED_KEY_SIZE)
            .any(|window| window == secret),
        "the failed allocation must scrub the wrapped ACK before returning"
    );
    assert_eq!(
        s.allocate_write(42, &wrapped(0xE2)).unwrap().0,
        0,
        "the failed free-list reservation is immediately reusable"
    );
}

#[test]
fn allocate_write_batch_scrubs_every_post_write_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let items = vec![
        (51, wrapped(0xA1)),
        (52, wrapped(0xA2)),
        (53, wrapped(0xA3)),
    ];

    FAIL_BATCH_READBACK.with(|fail| fail.set(true));
    let error = s.allocate_write_batch(&items).unwrap_err();
    assert!(matches!(error, Error::RegionStoreCorrupt(_)));

    for slot in 0..items.len() as u32 {
        assert_eq!(s.read_slot(slot).unwrap().state, SlotState::Tombstone);
    }
    let bytes = std::fs::read(&s.path).unwrap();
    for (_, secret) in &items {
        assert!(
            !bytes
                .windows(WRAPPED_KEY_SIZE)
                .any(|window| window == secret),
            "every failed batch key must be scrubbed before return"
        );
    }

    let rebound = s
        .allocate_write_batch(&[
            (61, wrapped(0xB1)),
            (62, wrapped(0xB2)),
            (63, wrapped(0xB3)),
        ])
        .unwrap();
    let mut slots: Vec<u32> = rebound.into_iter().map(|(slot, _)| slot).collect();
    slots.sort_unstable();
    assert_eq!(slots, vec![0, 1, 2], "all batch reservations were released");
}

/// Torn-erase crash shape: open scan, tombstone retry, and batch retry each scrub.
#[test]
fn interrupted_erasure_sibling_is_scrubbed_on_open_retry_and_batch_retry() {
    let key = wrapped(0x88);
    let crash_shape = |s: &AtomKeyStore, slot: u32| {
        let live = build_slot_block(&MAC_KEY, SlotState::Live, 9, 1, &key);
        let tomb = build_slot_block(
            &MAC_KEY,
            SlotState::Tombstone,
            0,
            2,
            &[0u8; WRAPPED_KEY_SIZE],
        );
        overwrite_in_place(&s.path, slot_offset(slot, true), &live).unwrap();
        overwrite_in_place(&s.path, slot_offset(slot, false), &tomb).unwrap();
    };
    let key_gone = |s: &AtomKeyStore| {
        let raw = std::fs::read(&s.path).unwrap();
        !raw.windows(WRAPPED_KEY_SIZE).any(|w| w == key)
    };

    // Reopen scrubs; both copies parse as tombstones and the slot stays free.
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path());
    crash_shape(&s, 0);
    assert!(!key_gone(&s), "crash shape holds the key pre-scrub");
    drop(s);
    let mut reopened = store(dir.path());
    assert!(key_gone(&reopened), "open scrubbed the stale sibling");
    let raw = std::fs::read(&reopened.path).unwrap();
    for copy_b in [false, true] {
        let o = slot_offset(0, copy_b) as usize;
        let rec = parse_slot_block(&MAC_KEY, &raw[o..o + BLOCK]).unwrap();
        assert_eq!(rec.state, SlotState::Tombstone);
    }
    assert_eq!(reopened.allocate_slot().unwrap(), 0, "slot still free");

    // A tombstone retry on the same shape (no reopen) scrubs too.
    let dir2 = tempfile::tempdir().unwrap();
    let mut s2 = store(dir2.path());
    crash_shape(&s2, 3);
    s2.tombstone(3, 9, 1).unwrap();
    assert!(key_gone(&s2), "retry scrubbed the stale sibling");

    // Batch path: an already-tombstoned entry yields no receipt but still scrubs.
    let dir3 = tempfile::tempdir().unwrap();
    let mut s3 = store(dir3.path());
    crash_shape(&s3, 5);
    let receipts = s3.tombstone_batch(&[(5, 9, 1)]).unwrap();
    assert!(receipts.is_empty(), "no state transition, no receipt");
    assert!(key_gone(&s3), "batch retry scrubbed the stale sibling");
}

/// Recycled slot: the erase retry skips without touching the new owner's key.
#[test]
fn batch_skips_recycled_slot_and_spares_the_new_owner() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let old_gen = s.write_live(slot, 1, &wrapped(0x11)).unwrap();
    s.tombstone(slot, 1, old_gen).unwrap();
    let reused = s.allocate_slot().unwrap();
    assert_eq!(reused, slot, "tombstoned slot is reused");
    s.write_live(slot, 2, &wrapped(0x22)).unwrap();

    // Retry of atom 1's erase against its old binding: skipped, no receipt.
    let receipts = s.tombstone_batch(&[(slot, 1, old_gen)]).unwrap();
    assert!(receipts.is_empty(), "recycled slot yields no receipt");
    let rec = s.read_slot(slot).unwrap();
    assert_eq!(rec.state, SlotState::Live, "new owner's key untouched");
    assert_eq!(rec.region_id, 2);
    assert_eq!(rec.wrapped, wrapped(0x22));
}

/// Same owner at an unexpected generation is illegitimate - the batch fails loud.
#[test]
fn batch_same_owner_wrong_gen_fails_loud() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let gen = s.write_live(slot, 7, &wrapped(0x33)).unwrap();
    let err = s.tombstone_batch(&[(slot, 7, gen + 5)]).unwrap_err();
    assert!(matches!(err, Error::RegionStoreCorrupt(_)));
    assert_eq!(
        s.read_slot(slot).unwrap().state,
        SlotState::Live,
        "nothing was written"
    );
}

#[test]
fn tombstone_erases_and_frees_slot() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let live_gen = s.write_live(slot, 7, &wrapped(0x11)).unwrap();
    s.tombstone(slot, 7, live_gen).unwrap();
    let rec = s.read_slot(slot).unwrap();
    assert_eq!(rec.state, SlotState::Tombstone);
    assert_eq!(
        rec.wrapped, [0u8; WRAPPED_KEY_SIZE],
        "wrapped key zeroed on tombstone"
    );
    // The freed slot is reused by the next allocation.
    assert_eq!(
        s.allocate_slot().unwrap(),
        slot,
        "tombstoned slot is reused"
    );
}

#[test]
fn tombstone_wrong_atom_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let live_gen = s.write_live(slot, 100, &wrapped(0x22)).unwrap();
    assert!(
        s.tombstone(slot, 999, live_gen).is_err(),
        "atom-id mismatch is rejected"
    );
    assert_eq!(
        s.read_slot(slot).unwrap().state,
        SlotState::Live,
        "slot still live after a rejected tombstone"
    );
}

#[test]
fn tombstone_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let live_gen = s.write_live(slot, 5, &wrapped(0x33)).unwrap();
    s.tombstone(slot, 5, live_gen).unwrap();
    s.tombstone(slot, 5, live_gen).unwrap(); // no-op, no double-free of the slot
}

#[test]
fn max_binding_generation_is_retired_before_and_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-atomkeys");
    let exhausted;
    {
        let mut s = store(dir.path());
        exhausted = s.allocate_slot().unwrap();
        install_authenticated_slot(
            &s,
            exhausted,
            SlotState::Live,
            41,
            MAX_BINDING_GENERATION,
            &wrapped(0x41),
        );

        s.tombstone(exhausted, 41, MAX_BINDING_GENERATION).unwrap();
        let retired = s.read_slot(exhausted).unwrap();
        assert_eq!(retired.state, SlotState::Tombstone);
        assert_eq!(retired.gen, MAX_BINDING_GENERATION + 1);

        let (next, generation) = s.allocate_write(42, &wrapped(0x42)).unwrap();
        assert_ne!(next, exhausted, "an exhausted slot cannot hold another key");
        assert!(generation <= MAX_BINDING_GENERATION);
        let still_retired = s.read_slot(exhausted).unwrap();
        assert_eq!(still_retired.state, retired.state);
        assert_eq!(still_retired.gen, retired.gen);
        assert_eq!(still_retired.wrapped, retired.wrapped);
    }

    let mut reopened = AtomKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap();
    assert_eq!(
        reopened.read_slot(exhausted).unwrap().gen,
        MAX_BINDING_GENERATION + 1
    );
    let rebound = reopened
        .allocate_write_batch(&[(51, wrapped(0x51)), (52, wrapped(0x52))])
        .unwrap();
    assert!(
        rebound.iter().all(|&(slot, _)| slot != exhausted),
        "free-list reconstruction must keep the exhausted slot retired"
    );
}

#[test]
fn batch_generation_overflow_is_an_error_without_partial_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let exhausted = s.allocate_slot().unwrap();
    let unrelated = s.allocate_slot().unwrap();
    install_authenticated_slot(
        &s,
        exhausted,
        SlotState::Tombstone,
        0,
        MAX_BINDING_GENERATION,
        &[0u8; WRAPPED_KEY_SIZE],
    );

    let before = std::fs::read(&s.path).unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        s.write_live_batch(&[
            (unrelated, 71, wrapped(0x71)),
            (exhausted, 72, wrapped(0x72)),
        ])
    }));
    let error = outcome
        .expect("write_live_batch must not panic on generation exhaustion")
        .unwrap_err();
    assert!(matches!(error, Error::RegionStoreCorrupt(_)));
    assert_eq!(
        std::fs::read(&s.path).unwrap(),
        before,
        "validation must finish before any batch write"
    );

    let unrelated_gen = s.write_live(unrelated, 71, &wrapped(0x71)).unwrap();
    install_authenticated_slot(&s, exhausted, SlotState::Live, 72, u64::MAX, &wrapped(0x72));
    let before = std::fs::read(&s.path).unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        s.tombstone_batch(&[(unrelated, 71, unrelated_gen), (exhausted, 72, u64::MAX)])
    }));
    let error = outcome
        .expect("tombstone_batch must not panic on generation exhaustion")
        .unwrap_err();
    assert!(matches!(error, Error::RegionStoreCorrupt(_)));
    assert_eq!(
        std::fs::read(&s.path).unwrap(),
        before,
        "a later overflowing item must not erase an earlier live item"
    );
    assert_eq!(s.read_slot(unrelated).unwrap().state, SlotState::Live);
}

#[test]
fn stale_same_owner_tombstone_cannot_erase_a_recycled_slot() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let old_gen = s.write_live(slot, 5, &wrapped(0x33)).unwrap();
    s.tombstone(slot, 5, old_gen).unwrap();

    assert_eq!(s.allocate_slot().unwrap(), slot);
    let successor = wrapped(0x44);
    let successor_gen = s.write_live(slot, 5, &successor).unwrap();
    assert!(successor_gen > old_gen);

    let err = s.tombstone(slot, 5, old_gen).unwrap_err();
    assert!(matches!(err, Error::RegionStoreCorrupt(_)));
    let record = s.read_slot(slot).unwrap();
    assert_eq!(record.state, SlotState::Live);
    assert_eq!(record.gen, successor_gen);
    assert_eq!(record.wrapped, successor);

    s.tombstone(slot, 5, successor_gen).unwrap();
    s.tombstone(slot, 5, old_gen)
        .expect("a later tombstone proves the stale binding is already erased");
}

#[test]
fn stale_batch_retry_accepts_a_later_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let old_gen = s.write_live(slot, 7, &wrapped(0x55)).unwrap();
    s.tombstone(slot, 7, old_gen).unwrap();
    assert_eq!(s.allocate_slot().unwrap(), slot);
    let successor_gen = s.write_live(slot, 7, &wrapped(0x66)).unwrap();
    s.tombstone(slot, 7, successor_gen).unwrap();

    let receipts = s.tombstone_batch(&[(slot, 7, old_gen)]).unwrap();
    assert!(receipts.is_empty());
    assert_eq!(s.read_slot(slot).unwrap().state, SlotState::Tombstone);
}

#[test]
fn tombstone_batch_rejects_duplicate_slots_before_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let live_gen = s.write_live(slot, 7, &wrapped(0x55)).unwrap();

    let err = s
        .tombstone_batch(&[(slot, 7, live_gen), (slot, 7, live_gen)])
        .unwrap_err();
    assert!(matches!(err, Error::RegionStoreCorrupt(_)));
    assert_eq!(s.read_slot(slot).unwrap().state, SlotState::Live);

    let first = s.allocate_slot().unwrap();
    let second = s.allocate_slot().unwrap();
    assert_ne!(
        first, second,
        "one physical slot must not enter the free list twice"
    );
}

#[test]
fn allocate_batch_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let mut slots = s.allocate_batch(10).unwrap();
    slots.sort_unstable();
    slots.dedup();
    assert_eq!(slots.len(), 10, "batch slots are distinct");
}

#[test]
fn grows_past_prealloc() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let n = ATOM_STORE_PREALLOC_SLOTS as usize + 5;
    let mut slots = Vec::new();
    for i in 0..n {
        let slot = s.allocate_slot().unwrap();
        s.write_live(slot, i as u64 + 1, &wrapped((i & 0xff) as u8))
            .unwrap();
        slots.push(slot);
    }
    assert!(
        s.slot_count() > ATOM_STORE_PREALLOC_SLOTS,
        "store grew past prealloc"
    );
    for (i, &slot) in slots.iter().enumerate() {
        assert_eq!(s.read_slot(slot).unwrap().region_id, i as u64 + 1);
    }
}

#[test]
fn live_wrapped_returns_only_live_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let s1 = s.allocate_slot().unwrap();
    let s2 = s.allocate_slot().unwrap();
    let s3 = s.allocate_slot().unwrap();
    s.write_live(s1, 11, &wrapped(0x01)).unwrap();
    let gen2 = s.write_live(s2, 22, &wrapped(0x02)).unwrap();
    s.write_live(s3, 33, &wrapped(0x03)).unwrap();
    s.tombstone(s2, 22, gen2).unwrap();

    let live = s.live_wrapped().unwrap();
    assert_eq!(live.len(), 2);
    assert_eq!(live.get(&11), Some(&wrapped(0x01)));
    assert_eq!(live.get(&33), Some(&wrapped(0x03)));
    assert!(!live.contains_key(&22), "tombstoned atom is not live");
}

#[test]
fn reopen_recovers_state_and_reuses_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-atomkeys");
    let (live_slot, tomb_slot);
    {
        let mut s = AtomKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap();
        live_slot = s.allocate_slot().unwrap();
        tomb_slot = s.allocate_slot().unwrap();
        s.write_live(live_slot, 71, &wrapped(0x71)).unwrap();
        let tomb_gen = s.write_live(tomb_slot, 72, &wrapped(0x72)).unwrap();
        s.tombstone(tomb_slot, 72, tomb_gen).unwrap();
    }
    let mut s = AtomKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap();
    assert_eq!(
        s.read_slot(live_slot).unwrap().region_id,
        71,
        "live slot recovered"
    );
    assert_eq!(s.read_slot(tomb_slot).unwrap().state, SlotState::Tombstone);
    // The rebuilt free list reuses the tombstoned slot and never the live one.
    let reused: Vec<u32> = (0..3).map(|_| s.allocate_slot().unwrap()).collect();
    assert!(
        reused.contains(&tomb_slot),
        "tombstoned slot reused after reopen"
    );
    assert!(!reused.contains(&live_slot), "live slot is not handed out");
}

#[test]
fn normalize_torn_tombstones_finishes_an_interrupted_batch_erase() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let bound = s
        .allocate_write_batch(&[(41, wrapped(0x41)), (42, wrapped(0x42))])
        .unwrap();
    let items: Vec<(u32, u64, u64)> = bound
        .iter()
        .zip([41u64, 42u64])
        .map(|(&(slot, gen), atom)| (slot, atom, gen))
        .collect();

    // Die after the first durable update (live copies), before the siblings.
    fail_next_batch_before_sibling();
    let err = s.tombstone_batch(&items).unwrap_err();
    assert!(
        err.to_string().contains("before the sibling scrub"),
        "{err}"
    );
    for &(slot, _) in &bound {
        assert_eq!(s.read_slot(slot).unwrap().state, SlotState::Tombstone);
        let copies = s.slot_copies(slot).unwrap();
        assert!(
            copies
                .iter()
                .any(|c| c.as_ref().map(|r| r.state) != Some(SlotState::Tombstone)),
            "precondition: a stale sibling record, got {copies:?}"
        );
    }

    let repaired = s.normalize_torn_tombstones().unwrap();
    assert_eq!(repaired, 2, "both torn slots repaired");
    assert_eq!(s.normalize_torn_tombstones().unwrap(), 0, "idempotent");
    for &(slot, _) in &bound {
        let copies = s.slot_copies(slot).unwrap();
        assert!(
            copies
                .iter()
                .all(|c| c.as_ref().is_some_and(|r| r.state == SlotState::Tombstone)),
            "both duplicate records normalized, got {copies:?}"
        );
    }

    // The torn erase failed before its free push; normalization must restore the slots.
    let mut reused = vec![s.allocate_slot().unwrap(), s.allocate_slot().unwrap()];
    reused.sort_unstable();
    let mut torn: Vec<u32> = bound.iter().map(|&(slot, _)| slot).collect();
    torn.sort_unstable();
    assert_eq!(reused, torn, "repaired slots are allocatable again");
}

#[test]
fn tombstone_retries_restore_a_stranded_slot() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let (slot_a, gen_a) = s.allocate_write(71, &wrapped(0x71)).unwrap();
    let (slot_b, gen_b) = s.allocate_write(72, &wrapped(0x72)).unwrap();

    // Strand both slots: torn erases that failed before their free push.
    fail_next_batch_before_sibling();
    s.tombstone_batch(&[(slot_a, 71, gen_a)]).unwrap_err();
    fail_next_batch_before_sibling();
    s.tombstone_batch(&[(slot_b, 72, gen_b)]).unwrap_err();
    assert!(!s.free.contains(&slot_a) && !s.free.contains(&slot_b));

    // The single retry converges and restores its slot...
    s.tombstone(slot_a, 71, gen_a).unwrap();
    assert!(s.free.contains(&slot_a));
    // ...and so does the batch retry (a no-op receipt, never a wedge).
    assert!(s
        .tombstone_batch(&[(slot_b, 72, gen_b)])
        .unwrap()
        .is_empty());
    assert!(s.free.contains(&slot_b));
    for slot in [slot_a, slot_b] {
        let copies = s.slot_copies(slot).unwrap();
        assert!(copies
            .iter()
            .all(|c| c.as_ref().is_some_and(|r| r.state == SlotState::Tombstone)));
    }
}
