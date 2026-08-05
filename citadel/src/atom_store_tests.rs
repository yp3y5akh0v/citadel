use super::*;

const MAC_KEY: [u8; KEY_SIZE] = [0x7c; KEY_SIZE];
const FILE_ID: u64 = 0xA70A_0000_0000_0001;

fn wrapped(b: u8) -> [u8; WRAPPED_KEY_SIZE] {
    [b; WRAPPED_KEY_SIZE]
}

fn store(dir: &std::path::Path) -> AtomKeyStore {
    AtomKeyStore::create_or_open(&dir.join("db.citadel-atomkeys"), FILE_ID, MAC_KEY).unwrap()
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
    s2.tombstone(3, 9).unwrap();
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
    s.tombstone(slot, 1).unwrap();
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
    s.write_live(slot, 7, &wrapped(0x11)).unwrap();
    s.tombstone(slot, 7).unwrap();
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
    s.write_live(slot, 100, &wrapped(0x22)).unwrap();
    assert!(
        s.tombstone(slot, 999).is_err(),
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
    s.write_live(slot, 5, &wrapped(0x33)).unwrap();
    s.tombstone(slot, 5).unwrap();
    s.tombstone(slot, 5).unwrap(); // no-op, no double-free of the slot
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
    s.write_live(s2, 22, &wrapped(0x02)).unwrap();
    s.write_live(s3, 33, &wrapped(0x03)).unwrap();
    s.tombstone(s2, 22).unwrap();

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
        s.write_live(tomb_slot, 72, &wrapped(0x72)).unwrap();
        s.tombstone(tomb_slot, 72).unwrap();
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
