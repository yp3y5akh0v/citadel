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
