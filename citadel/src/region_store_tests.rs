use super::*;

const MAC_KEY: [u8; KEY_SIZE] = [0x5a; KEY_SIZE];
const FILE_ID: u64 = 0xDEAD_BEEF_0000_0001;

fn store(dir: &std::path::Path) -> RegionKeyStore {
    RegionKeyStore::create_or_open(&dir.join("db.citadel-regions"), FILE_ID, MAC_KEY).unwrap()
}

#[test]
fn create_preallocates_empty_slots() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path());
    assert_eq!(s.slot_count(), REGION_STORE_PREALLOC_SLOTS);
    for i in 0..s.slot_count() {
        assert_eq!(s.read_slot(i).unwrap().state, SlotState::Empty);
    }
}

#[test]
fn reopen_recovers_slot_count_and_state() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut s = store(dir.path());
        let slot = s.allocate_slot().unwrap();
        s.write_live(slot, 7, &[0xCD; WRAPPED_KEY_SIZE]).unwrap();
    }
    let s = store(dir.path());
    let rec = s.read_slot(0).unwrap();
    assert_eq!(rec.state, SlotState::Live);
    assert_eq!(rec.region_id, 7);
    assert_eq!(rec.wrapped, [0xCD; WRAPPED_KEY_SIZE]);
}

#[test]
fn allocate_skips_live_and_recycles_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());

    let a = s.allocate_slot().unwrap();
    s.write_live(a, 1, &[0x11; WRAPPED_KEY_SIZE]).unwrap();
    assert_eq!(a, 0);

    let b = s.allocate_slot().unwrap();
    assert_eq!(b, 1, "live slot 0 must be skipped");
    s.write_live(b, 2, &[0x22; WRAPPED_KEY_SIZE]).unwrap();

    s.tombstone(0, 1).unwrap();
    let recycled = s.allocate_slot().unwrap();
    assert_eq!(recycled, 0, "tombstoned slot 0 is the lowest free slot");
}

#[test]
fn tombstone_makes_wrapped_key_unrecoverable() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let wrapped = [0xC3; WRAPPED_KEY_SIZE];
    s.write_live(slot, 9, &wrapped).unwrap();

    // Present before forget.
    assert_eq!(s.read_slot(slot).unwrap().wrapped, wrapped);
    let before = std::fs::read(&s.path).unwrap();
    assert!(
        contains_window(&before, &wrapped),
        "harness sanity: key present pre-forget"
    );

    s.tombstone(slot, 9).unwrap();

    assert_eq!(s.read_slot(slot).unwrap().state, SlotState::Tombstone);
    let after = std::fs::read(&s.path).unwrap();
    assert!(
        !contains_window(&after, &wrapped),
        "wrapped key residue must be absent from the live store after forget"
    );
}

#[test]
fn tombstone_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    s.write_live(slot, 3, &[0x44; WRAPPED_KEY_SIZE]).unwrap();
    s.tombstone(slot, 3).unwrap();
    s.tombstone(slot, 3).unwrap(); // second call is a no-op success
    assert_eq!(s.read_slot(slot).unwrap().state, SlotState::Tombstone);
}

#[test]
fn grow_appends_when_prealloc_exhausted() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let prealloc = REGION_STORE_PREALLOC_SLOTS;
    for i in 0..prealloc {
        let slot = s.allocate_slot().unwrap();
        assert_eq!(slot, i);
        s.write_live(slot, i as u64 + 1, &[i as u8; WRAPPED_KEY_SIZE])
            .unwrap();
    }
    let grown = s.allocate_slot().unwrap();
    assert_eq!(grown, prealloc, "first slot of the appended run");
    assert_eq!(s.slot_count(), prealloc + GROW_SLOTS);

    // Siblings survive growth and remain attachable after reopen.
    let s2 = store(dir.path());
    for i in 0..prealloc {
        let rec = s2.read_slot(i).unwrap();
        assert_eq!(rec.state, SlotState::Live);
        assert_eq!(rec.region_id, i as u64 + 1);
    }
}

#[test]
fn torn_fill_falls_back_to_prior_valid_copy() {
    // A fresh slot is EMPTY/EMPTY; write_live lands in copy B. Corrupting copy B
    // (torn write) must make the reader fall back to the valid EMPTY copy A rather
    // than read garbage as a key.
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    s.write_live(slot, 5, &[0x77; WRAPPED_KEY_SIZE]).unwrap();

    // Corrupt the authoritative (copy B) block's MAC region in place.
    let mut raw = std::fs::read(&s.path).unwrap();
    let off = slot_offset(slot, true) as usize;
    raw[off + SLOT_MAC_INPUT] ^= 0xFF;
    std::fs::write(&s.path, &raw).unwrap();

    let rec = s.read_slot(slot).unwrap();
    assert_eq!(
        rec.state,
        SlotState::Empty,
        "torn copy ignored; EMPTY sibling wins"
    );
}

#[test]
fn higher_gen_tombstone_wins_over_torn_live_sibling() {
    // Simulate a crash after the commit-point tombstone of the live copy but before
    // the sibling overwrite: copy A = TOMBSTONE(gen2), copy B = LIVE(gen1). The
    // higher-gen tombstone must win -> region reads as forgotten.
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path());
    let slot = 0u32;

    let live = build_slot_block(&MAC_KEY, SlotState::Live, 4, 1, &[0x88; WRAPPED_KEY_SIZE]);
    let tomb = build_slot_block(
        &MAC_KEY,
        SlotState::Tombstone,
        0,
        2,
        &[0u8; WRAPPED_KEY_SIZE],
    );
    overwrite_in_place(&s.path, slot_offset(slot, true), &live).unwrap();
    overwrite_in_place(&s.path, slot_offset(slot, false), &tomb).unwrap();

    assert_eq!(s.read_slot(slot).unwrap().state, SlotState::Tombstone);
}

#[test]
fn wrong_mac_key_rejects_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-regions");
    RegionKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap();
    let err = RegionKeyStore::create_or_open(&path, FILE_ID, [0x01; KEY_SIZE]).unwrap_err();
    assert!(matches!(err, Error::RegionStoreCorrupt(_)));
}

#[test]
fn wrong_file_id_rejects_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-regions");
    RegionKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap();
    let err = RegionKeyStore::create_or_open(&path, FILE_ID ^ 1, MAC_KEY).unwrap_err();
    assert!(matches!(err, Error::RegionStoreCorrupt(_)));
}

#[test]
fn torn_grow_misaligned_file_normalized_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-regions");
    {
        store(dir.path()); // create the 64-slot store, then drop
    }
    let prealloc_len = (2 + 2 * REGION_STORE_PREALLOC_SLOTS as usize) * BLOCK;
    let mut bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes.len(), prealloc_len);
    // Simulate a torn sub-block append (crash mid-grow): partial bytes, no header bump.
    bytes.append(&mut vec![0u8; BLOCK + 37]);
    std::fs::write(&path, &bytes).unwrap();

    let mut s = RegionKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap();
    assert_eq!(s.slot_count(), REGION_STORE_PREALLOC_SLOTS);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len() as usize,
        prealloc_len,
        "torn tail normalized away on open"
    );

    // Fill the prealloc run, then grow: the grown slot must land block-aligned.
    for i in 0..REGION_STORE_PREALLOC_SLOTS {
        let slot = s.allocate_slot().unwrap();
        s.write_live(slot, i as u64 + 1, &[0x33; WRAPPED_KEY_SIZE])
            .unwrap();
    }
    let grown = s.allocate_slot().unwrap();
    assert_eq!(grown, REGION_STORE_PREALLOC_SLOTS);
    s.write_live(grown, 999, &[0x44; WRAPPED_KEY_SIZE]).unwrap();
    let rec = s.read_slot(grown).unwrap();
    assert_eq!(rec.state, SlotState::Live);
    assert_eq!(rec.region_id, 999);
    assert_eq!(rec.wrapped, [0x44; WRAPPED_KEY_SIZE]);
}

#[test]
fn tombstone_guards_empty_and_region_id_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();

    // (a) tombstone of an EMPTY slot is rejected (nothing to erase).
    let err = s.tombstone(slot, 1).unwrap_err();
    assert!(
        matches!(err, Error::RegionStoreCorrupt(ref m) if m.contains("no live key")),
        "got {err}"
    );

    // (b) a wrong expected_region_id is rejected and must NOT destroy the live key.
    s.write_live(slot, 9, &[0xAB; WRAPPED_KEY_SIZE]).unwrap();
    let err = s.tombstone(slot, 8).unwrap_err();
    assert!(
        matches!(err, Error::RegionStoreCorrupt(ref m) if m.contains("region 9 not 8")),
        "got {err}"
    );
    let rec = s.read_slot(slot).unwrap();
    assert_eq!(
        rec.state,
        SlotState::Live,
        "mismatched forget must not erase"
    );
    assert_eq!(rec.wrapped, [0xAB; WRAPPED_KEY_SIZE]);
}

#[test]
fn recycle_tombstone_then_write_read_gen_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut s = store(dir.path());
        let slot = s.allocate_slot().unwrap();
        s.write_live(slot, 1, &[0x11; WRAPPED_KEY_SIZE]).unwrap();
        s.tombstone(slot, 1).unwrap();
        let tomb_gen = s.read_slot(slot).unwrap().gen;

        let recycled = s.allocate_slot().unwrap();
        assert_eq!(recycled, slot);
        let g = s
            .write_live(recycled, 2, &[0x22; WRAPPED_KEY_SIZE])
            .unwrap();
        assert!(
            g > tomb_gen,
            "recycled slot gen must exceed the tombstone gen"
        );
        let rec = s.read_slot(recycled).unwrap();
        assert_eq!(rec.state, SlotState::Live);
        assert_eq!(rec.region_id, 2);
        assert_eq!(rec.wrapped, [0x22; WRAPPED_KEY_SIZE]);
    }
    // The recycled slot is fully usable after reopen.
    let s = store(dir.path());
    let rec = s.read_slot(0).unwrap();
    assert_eq!(rec.state, SlotState::Live);
    assert_eq!(rec.region_id, 2);
}

#[test]
fn orphan_whole_slot_tail_clamped_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-regions");
    {
        store(dir.path());
    }
    // Append one full MAC'd slot-pair WITHOUT bumping the header (a durable-but-
    // uncommitted grow tail). It must be clamped away on reopen.
    let blk = build_slot_block(&MAC_KEY, SlotState::Empty, 0, 0, &[0u8; WRAPPED_KEY_SIZE]);
    let mut pair = blk.to_vec();
    pair.extend_from_slice(&blk);
    append_and_sync(&path, &pair).unwrap();

    let prealloc_len = (2 + 2 * REGION_STORE_PREALLOC_SLOTS as usize) * BLOCK;
    let mut s = RegionKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap();
    assert_eq!(
        s.slot_count(),
        REGION_STORE_PREALLOC_SLOTS,
        "uncommitted 65th slot is clamped away"
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().len() as usize,
        prealloc_len,
        "the uncommitted orphan pair is physically truncated, not just logically ignored"
    );
    for i in 0..REGION_STORE_PREALLOC_SLOTS {
        let slot = s.allocate_slot().unwrap();
        s.write_live(slot, i as u64 + 1, &[0x55; WRAPPED_KEY_SIZE])
            .unwrap();
    }
    let grown = s.allocate_slot().unwrap();
    assert_eq!(
        grown, REGION_STORE_PREALLOC_SLOTS,
        "grown slot is the first of the appended run"
    );
    s.write_live(grown, 999, &[0x66; WRAPPED_KEY_SIZE]).unwrap();
    let rec = s.read_slot(grown).unwrap();
    assert_eq!(rec.state, SlotState::Live);
    assert_eq!(rec.region_id, 999);
    assert_eq!(rec.wrapped, [0x66; WRAPPED_KEY_SIZE]);
}

#[test]
fn tombstone_fails_safely_when_overwrite_cannot_persist() {
    // If the sidecar overwrite cannot land (here: a read-only file standing in for any
    // I/O failure), forget must return Err - never silently report the key destroyed -
    // and the live key must remain intact and recoverable. This exercises tombstone's
    // error path; the read-back gate additionally defends against a lying fsync (write
    // reports success but does not persist), which is fault-injection-only.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-regions");
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    s.write_live(slot, 1, &[0x55; WRAPPED_KEY_SIZE]).unwrap();

    set_readonly(&path, true);
    let result = s.tombstone(slot, 1);
    set_readonly(&path, false); // restore so the slot is readable / tempdir can clean up

    assert!(
        result.is_err(),
        "forget must fail if the key overwrite cannot persist"
    );
    let rec = s.read_slot(slot).unwrap();
    assert_eq!(
        rec.state,
        SlotState::Live,
        "key intact after a failed forget"
    );
    assert_eq!(rec.wrapped, [0x55; WRAPPED_KEY_SIZE]);
}

fn set_readonly(path: &std::path::Path, readonly: bool) {
    let mut perms = std::fs::metadata(path).unwrap().permissions();
    perms.set_readonly(readonly);
    std::fs::set_permissions(path, perms).unwrap();
}

/// True if `needle` appears as a contiguous window of `hay`.
fn contains_window(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn tombstone_gen_is_exactly_one_above_live() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let slot = s.allocate_slot().unwrap();
    let live_gen = s.write_live(slot, 7, &[0x9E; WRAPPED_KEY_SIZE]).unwrap();

    s.tombstone(slot, 7).unwrap();

    let rec = s.read_slot(slot).unwrap();
    assert_eq!(
        rec.state,
        SlotState::Tombstone,
        "slot must read back as tombstone after forget"
    );
    assert_eq!(
        rec.gen,
        live_gen + 1,
        "tombstone gen must be exactly one above the live gen so a torn tombstone outranks a surviving live sibling"
    );
}

#[test]
fn tombstone_erases_sibling_copy_residue() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path());
    let slot = 0u32;

    let w_a = [0xA1; WRAPPED_KEY_SIZE];
    let w_b = [0xB2; WRAPPED_KEY_SIZE];
    let copy_a = build_slot_block(&MAC_KEY, SlotState::Live, 7, 1, &w_a);
    let copy_b = build_slot_block(&MAC_KEY, SlotState::Live, 7, 2, &w_b);
    overwrite_in_place(&s.path, slot_offset(slot, false), &copy_a).unwrap();
    overwrite_in_place(&s.path, slot_offset(slot, true), &copy_b).unwrap();

    let before = std::fs::read(&s.path).unwrap();
    assert!(
        contains_window(&before, &w_a) && contains_window(&before, &w_b),
        "harness sanity: both wrapped keys present before forget"
    );

    s.tombstone(slot, 7).unwrap();

    assert_eq!(s.read_slot(slot).unwrap().state, SlotState::Tombstone);
    let after = std::fs::read(&s.path).unwrap();
    assert!(
        !contains_window(&after, &w_b),
        "authoritative-copy residue must be gone"
    );
    assert!(
        !contains_window(&after, &w_a),
        "sibling-copy residue must be erased by the second overwrite"
    );
}

#[test]
fn live_owners_returns_exact_live_slot_region_pairs_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path());
    s.write_live(2, 100, &[0xA2; WRAPPED_KEY_SIZE]).unwrap();
    s.write_live(3, 150, &[0xA3; WRAPPED_KEY_SIZE]).unwrap();
    s.write_live(4, 200, &[0xA4; WRAPPED_KEY_SIZE]).unwrap();
    s.tombstone(3, 150).unwrap();
    let owners = s.live_owners().unwrap();
    assert_eq!(owners, vec![(2u32, 100u64), (4u32, 200u64)]);
}

#[test]
fn parse_header_block_rejects_slice_shorter_than_mac_input() {
    let block = build_header_block(&MAC_KEY, FILE_ID, 7, 42);
    let short = &block[..HEADER_MAC_INPUT + 22];
    assert_eq!(short.len(), 50);
    assert!(
        parse_header_block(&MAC_KEY, FILE_ID, short).is_none(),
        "a slice shorter than the header MAC input + tag must parse to None"
    );
}

#[test]
fn parse_header_block_accepts_exact_mac_input_length() {
    let block = build_header_block(&MAC_KEY, FILE_ID, 7, 42);
    let exact = &block[..HEADER_MAC_INPUT + 32];
    assert_eq!(exact.len(), 60);
    assert_eq!(
        parse_header_block(&MAC_KEY, FILE_ID, exact),
        Some((7u32, 42u64))
    );
}

#[test]
fn parse_header_block_full_block_parses_below_mutated_bound() {
    let block = build_header_block(&MAC_KEY, FILE_ID, 13, 100);
    assert_eq!(
        parse_header_block(&MAC_KEY, FILE_ID, &block),
        Some((13u32, 100u64)),
        "a full 512-byte header (between the real 60 and mutated 896 bounds) must parse"
    );
}

#[test]
fn parse_slot_block_rejects_slice_shorter_than_mac_input() {
    let block = build_slot_block(
        &MAC_KEY,
        SlotState::Live,
        0x1122_3344_5566_7788,
        99,
        &[0xAB; WRAPPED_KEY_SIZE],
    );
    let short = &block[..SLOT_MAC_INPUT + 20];
    assert_eq!(short.len(), 80);
    assert!(
        parse_slot_block(&MAC_KEY, short).is_none(),
        "a slice shorter than the slot MAC input + tag must parse to None"
    );
}

#[test]
fn parse_slot_block_accepts_exact_mac_input_length() {
    let region_id = 0x1122_3344_5566_7788u64;
    let block = build_slot_block(
        &MAC_KEY,
        SlotState::Live,
        region_id,
        99,
        &[0xAB; WRAPPED_KEY_SIZE],
    );
    let exact = &block[..SLOT_MAC_INPUT + 32];
    assert_eq!(exact.len(), 92);
    let rec = parse_slot_block(&MAC_KEY, exact).expect("exact-length slot slice must parse");
    assert_eq!(rec.state, SlotState::Live);
    assert_eq!(rec.region_id, region_id);
    assert_eq!(rec.gen, 99u64);
    assert_eq!(rec.wrapped, [0xAB; WRAPPED_KEY_SIZE]);
}

#[test]
fn open_rejects_subheader_file_with_smaller_than_header_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-regions");
    std::fs::write(&path, vec![0u8; 600]).unwrap();

    let err = RegionKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap_err();
    assert!(
        matches!(err, Error::RegionStoreCorrupt(ref m) if m.contains("smaller than header")),
        "got {err}"
    );
}

#[test]
fn open_accepts_file_exactly_two_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-regions");
    let hdr = build_header_block(&MAC_KEY, FILE_ID, 0u32, 1u64);
    let mut buf = hdr.to_vec();
    buf.extend_from_slice(&hdr);
    assert_eq!(
        buf.len(),
        2 * BLOCK,
        "harness sanity: exactly two header blocks"
    );
    std::fs::write(&path, &buf).unwrap();

    let s = RegionKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap();
    assert_eq!(s.slot_count(), 0, "a 2*BLOCK store opens with zero slots");
}

#[test]
fn open_picks_higher_gen_header_slot_count() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-regions");
    {
        store(dir.path()); // create the 64-slot prealloc store, then drop
    }
    overwrite_in_place(
        &path,
        header_offset(false),
        &build_header_block(&MAC_KEY, FILE_ID, 10u32, 5u64),
    )
    .unwrap();
    overwrite_in_place(
        &path,
        header_offset(true),
        &build_header_block(&MAC_KEY, FILE_ID, 64u32, 2u64),
    )
    .unwrap();

    let s = RegionKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap();
    assert_eq!(
        s.slot_count(),
        10,
        "higher-gen header copy (gen 5, count 10) is authoritative"
    );
}

#[test]
fn open_clamps_overcommitted_header_to_on_disk_slot_count() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.citadel-regions");
    {
        store(dir.path()); // 64-slot store: len = (2 + 2*64)*BLOCK = 66560
    }
    append_and_sync(&path, &vec![0u8; 600]).unwrap();
    let hdr = build_header_block(&MAC_KEY, FILE_ID, 200u32, 2u64);
    overwrite_in_place(&path, header_offset(false), &hdr).unwrap();
    overwrite_in_place(&path, header_offset(true), &hdr).unwrap();

    let s = RegionKeyStore::create_or_open(&path, FILE_ID, MAC_KEY).unwrap();
    assert_eq!(
        s.slot_count(),
        64,
        "on_disk = (67160 - 1024) / 1024 = 64 clamps the over-committed header"
    );
}

#[test]
fn header_offset_a_and_b_do_not_alias() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path());

    let hdr_a = build_header_block(&MAC_KEY, FILE_ID, 11, 5);
    let hdr_b = build_header_block(&MAC_KEY, FILE_ID, 22, 9);
    overwrite_in_place(&s.path, header_offset(false), &hdr_a).unwrap();
    overwrite_in_place(&s.path, header_offset(true), &hdr_b).unwrap();

    let raw = std::fs::read(&s.path).unwrap();
    assert_eq!(
        parse_header_block(&MAC_KEY, FILE_ID, &raw[0..]),
        Some((11u32, 5u64)),
        "copy A must live at file offset 0"
    );
    assert_eq!(
        parse_header_block(&MAC_KEY, FILE_ID, &raw[BLOCK..]),
        Some((22u32, 9u64)),
        "copy B must live at file offset BLOCK, not alias copy A"
    );
}

#[test]
fn header_gen_increments_across_two_grows() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let prealloc = REGION_STORE_PREALLOC_SLOTS;

    for i in 0..prealloc {
        let slot = s.allocate_slot().unwrap();
        s.write_live(slot, i as u64 + 1, &[0x11; WRAPPED_KEY_SIZE])
            .unwrap();
    }
    let _ = s.allocate_slot().unwrap();
    assert_eq!(s.slot_count(), prealloc + GROW_SLOTS);
    let raw1 = std::fs::read(&s.path).unwrap();
    assert_eq!(
        parse_header_block(&MAC_KEY, FILE_ID, &raw1[0..]).map(|(_, g)| g),
        Some(2u64),
        "header gen must be 2 after the first grow"
    );

    for i in 0..GROW_SLOTS {
        let slot = s.allocate_slot().unwrap();
        s.write_live(
            slot,
            prealloc as u64 + i as u64 + 1,
            &[0x22; WRAPPED_KEY_SIZE],
        )
        .unwrap();
    }
    let _ = s.allocate_slot().unwrap();
    assert_eq!(s.slot_count(), prealloc + 2 * GROW_SLOTS);
    let raw2 = std::fs::read(&s.path).unwrap();
    assert_eq!(
        parse_header_block(&MAC_KEY, FILE_ID, &raw2[0..]).map(|(_, g)| g),
        Some(3u64),
        "header gen must be 3 after the second grow"
    );
}

#[test]
fn view_bounds_guard_rejects_slot_past_eof() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path());
    let past_eof = REGION_STORE_PREALLOC_SLOTS;
    let res = s.read_slot(past_eof);
    assert!(
        matches!(res, Err(Error::RegionStoreCorrupt(ref m)) if m.contains("out of bounds")),
        "slot past EOF must be reported out of bounds, got {res:?}"
    );
}
