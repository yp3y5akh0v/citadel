use super::*;

fn test_mac_key() -> [u8; MAC_KEY_SIZE] {
    [0x5A; MAC_KEY_SIZE]
}

fn sample_slot() -> CommitSlot {
    CommitSlot {
        txn_id: TxnId(42),
        tree_root: PageId(10),
        tree_depth: 3,
        tree_entries: 1000,
        catalog_root: PageId(11),
        total_pages: 100,
        high_water_mark: 99,
        pending_free_root: PageId(50),
        encryption_epoch: 1,
        dek_id: [0xAA; MAC_SIZE],
        merkle_root: [0xBB; MERKLE_HASH_SIZE],
        named_table_entries: vec![(0x12345678, 500, 77, 3)],
        ..Default::default()
    }
}

/// Serialize `slot` exactly as the pre-v1 (citadeldb <= 1.12) code did:
/// checksum over [0..SLOT_CHECKSUM], no marker, no MAC.
fn serialize_legacy(slot: &CommitSlot) -> [u8; COMMIT_SLOT_SIZE] {
    let mut legacy = slot.clone();
    legacy.merkle_scheme = MerkleScheme::Legacy;
    legacy.slot_format = SlotFormat::Legacy;
    legacy.slot_mac = [0u8; SLOT_MAC_SIZE];
    legacy.serialize()
}

#[test]
fn commit_slot_serialize_roundtrip() {
    let slot = sample_slot();

    let buf = slot.serialize();
    let slot2 = CommitSlot::deserialize(&buf);

    assert_eq!(slot2.txn_id, TxnId(42));
    assert_eq!(slot2.tree_root, PageId(10));
    assert_eq!(slot2.tree_depth, 3);
    assert_eq!(slot2.merkle_scheme, MerkleScheme::Legacy);
    assert_eq!(slot2.tree_entries, 1000);
    assert_eq!(slot2.catalog_root, PageId(11));
    assert_eq!(slot2.total_pages, 100);
    assert_eq!(slot2.high_water_mark, 99);
    assert_eq!(slot2.pending_free_root, PageId(50));
    assert_eq!(slot2.encryption_epoch, 1);
    assert_eq!(slot2.dek_id, [0xAA; MAC_SIZE]);
    assert_eq!(slot2.merkle_root, [0xBB; MERKLE_HASH_SIZE]);
    assert_eq!(slot2.named_table_entries, vec![(0x12345678, 500, 77, 3)]);
}

#[test]
fn logical_overflow_merkle_scheme_roundtrips_in_checksum_covered_bytes() {
    assert_eq!(SLOT_MERKLE_SCHEME, 14);
    const { assert!(SLOT_MERKLE_SCHEME + 2 <= SLOT_CHECKSUM) };

    let mut slot = sample_slot();
    slot.merkle_scheme = MerkleScheme::LogicalOverflowV1;
    slot.seal(&test_mac_key());

    let buf = slot.serialize();
    assert_eq!(
        u16::from_le_bytes(
            buf[SLOT_MERKLE_SCHEME..SLOT_MERKLE_SCHEME + 2]
                .try_into()
                .unwrap()
        ),
        SLOT_MERKLE_SCHEME_LOGICAL_OVERFLOW_V1
    );
    let round_tripped = CommitSlot::deserialize(&buf);
    assert_eq!(round_tripped.merkle_scheme, MerkleScheme::LogicalOverflowV1);
    assert!(round_tripped.verify_checksum());
    assert!(round_tripped.verify_mac(&test_mac_key()));
}

#[test]
fn commit_slot_checksum() {
    let slot = CommitSlot {
        txn_id: TxnId(1),
        tree_root: PageId(5),
        tree_depth: 1,
        tree_entries: 10,
        catalog_root: PageId(0),
        total_pages: 5,
        high_water_mark: 4,
        pending_free_root: PageId::INVALID,
        encryption_epoch: 1,
        dek_id: [0; MAC_SIZE],
        merkle_root: [0; MERKLE_HASH_SIZE],
        ..Default::default()
    };

    let buf = slot.serialize();
    let slot2 = CommitSlot::deserialize(&buf);
    assert!(slot2.verify_checksum());

    let mut tampered = buf;
    tampered[0] ^= 0x01;
    let slot3 = CommitSlot::deserialize(&tampered);
    assert!(!slot3.verify_checksum());
}

#[test]
fn sealed_slot_roundtrip_verifies_mac() {
    let mut slot = sample_slot();
    slot.seal(&test_mac_key());
    assert_eq!(slot.slot_format, SlotFormat::V1);

    let buf = slot.serialize();
    let slot2 = CommitSlot::deserialize(&buf);
    assert_eq!(slot2.slot_format, SlotFormat::V1);
    assert!(slot2.verify_checksum());
    assert!(slot2.verify_mac(&test_mac_key()));
    assert!(!slot2.verify_mac(&[0xEE; MAC_KEY_SIZE]));
    assert_eq!(slot2.named_table_entries, slot.named_table_entries);
}

/// Regression (unauthenticated commit slot): a slot written by the pre-v1
/// serializer (no marker, no MAC) must still deserialize and verify, so
/// existing database files keep opening.
#[test]
fn legacy_format_slot_still_accepted() {
    let buf = serialize_legacy(&sample_slot());
    assert_eq!(&buf[SLOT_MERKLE_SCHEME..SLOT_MERKLE_SCHEME + 2], &[0, 0]);
    assert!(buf[SLOT_FORMAT_MARKER..].iter().all(|&b| b == 0));

    let slot = CommitSlot::deserialize(&buf);
    assert_eq!(slot.merkle_scheme, MerkleScheme::Legacy);
    assert_eq!(slot.slot_format, SlotFormat::Legacy);
    assert!(slot.verify_checksum());
    assert!(slot.verify_mac(&test_mac_key()));
    assert_eq!(slot.named_table_entries, sample_slot().named_table_entries);
}

/// A slot that must carry the full legacy entry capacity has no room for the
/// MAC tail: seal falls back to the legacy format and keeps every entry.
#[test]
fn seal_with_overflowing_entries_keeps_legacy_capacity() {
    let mut slot = sample_slot();
    slot.named_table_entries = (0..SLOT_NAMED_MAX_ENTRIES as u32)
        .map(|i| (i + 1, 10 * i as u64, 100 + i, 2))
        .collect();
    slot.seal(&test_mac_key());
    assert_eq!(slot.slot_format, SlotFormat::Legacy);

    let slot2 = CommitSlot::deserialize(&slot.serialize());
    assert_eq!(slot2.slot_format, SlotFormat::Legacy);
    assert_eq!(slot2.named_table_entries.len(), SLOT_NAMED_MAX_ENTRIES);
    assert!(slot2.verify_checksum());
    assert!(slot2.verify_mac(&test_mac_key()));
}

/// Regression (slot checksum covered only [0..76]): corruption in merkle_root
/// or the named entries - both past the checksummed range - must fail
/// verification on v1 slots via the MAC.
#[test]
fn v1_slot_detects_tail_corruption() {
    let mut slot = sample_slot();
    slot.seal(&test_mac_key());
    let buf = slot.serialize();

    for offset in [SLOT_MERKLE_ROOT + 4, SLOT_NAMED_ENTRIES + 2 + 12] {
        let mut tampered = buf;
        tampered[offset] ^= 0x01;
        let slot2 = CommitSlot::deserialize(&tampered);
        // The keyless checksum cannot see the tail; the MAC must.
        assert!(slot2.verify_checksum(), "offset {offset}");
        assert!(!slot2.verify_mac(&test_mac_key()), "offset {offset}");
    }
}

/// A fabricated v1 slot with a valid keyless checksum but a wrong MAC is
/// rejected: the xxh64 alone no longer authenticates slot contents.
#[test]
fn v1_slot_with_forged_mac_rejected() {
    let mut slot = sample_slot();
    slot.seal(&test_mac_key());
    let mut buf = slot.serialize();
    buf[SLOT_MAC] ^= 0xFF;

    let slot2 = CommitSlot::deserialize(&buf);
    assert!(slot2.verify_checksum());
    assert!(!slot2.verify_mac(&test_mac_key()));
}

/// An unrecognized format marker (corruption or a future layout) fails closed.
#[test]
fn unknown_slot_marker_never_verifies() {
    let mut slot = sample_slot();
    slot.seal(&test_mac_key());
    let mut buf = slot.serialize();
    buf[SLOT_FORMAT_MARKER..SLOT_FORMAT_MARKER + 2].copy_from_slice(&0xBEEFu16.to_le_bytes());

    let slot2 = CommitSlot::deserialize(&buf);
    assert_eq!(slot2.slot_format, SlotFormat::Unknown);
    assert!(!slot2.verify_checksum());
    assert!(!slot2.verify_mac(&test_mac_key()));
}

#[test]
fn unknown_merkle_scheme_never_verifies_or_recovers() {
    use crate::memory_io::MemoryPageIO;

    let mac_key = test_mac_key();
    let io = MemoryPageIO::new();
    let mut header = FileHeader::new(0xA5, [0x77; MAC_SIZE]);
    header.slots[0].merkle_scheme = MerkleScheme::Unknown;
    for slot in &mut header.slots {
        slot.seal(&mac_key);
    }
    write_file_header(&io, &header).unwrap();

    let slot = read_commit_slot(&io, 0).unwrap();
    assert_eq!(slot.merkle_scheme, MerkleScheme::Unknown);
    assert!(!slot.verify_checksum());
    assert!(!slot.verify_mac(&mac_key));
    assert_eq!(
        recover(&io, &mac_key).unwrap().0,
        1,
        "the unsupported active slot must not hide an older supported slot"
    );

    header.slots[1].merkle_scheme = MerkleScheme::Unknown;
    header.slots[1].seal(&mac_key);
    write_commit_slot(&io, 1, &header.slots[1]).unwrap();
    assert!(matches!(
        recover(&io, &mac_key),
        Err(Error::DatabaseCorrupted)
    ));
}

/// Counts past the legacy capacity stay Unknown; a legacy-full count reads
/// as Legacy even if a tampered v1 count byte produced it (re-routing gains
/// nothing - flagged files reject non-V1 slots anyway).
#[test]
fn count_corruption_never_reads_past_legacy_capacity() {
    let mut slot = sample_slot();
    slot.seal(&test_mac_key());
    let buf = slot.serialize();

    let mut tampered = buf;
    tampered[SLOT_NAMED_ENTRIES..SLOT_NAMED_ENTRIES + 2]
        .copy_from_slice(&(SLOT_NAMED_MAX_ENTRIES as u16).to_le_bytes());
    let slot2 = CommitSlot::deserialize(&tampered);
    assert_eq!(slot2.slot_format, SlotFormat::Legacy);

    let mut tampered = buf;
    tampered[SLOT_NAMED_ENTRIES..SLOT_NAMED_ENTRIES + 2].copy_from_slice(&8u16.to_le_bytes());
    let slot2 = CommitSlot::deserialize(&tampered);
    assert_eq!(slot2.slot_format, SlotFormat::Unknown);
    assert!(!slot2.verify_checksum());
}

/// Regression: a released-format slot with 7 entries whose 7th name-hash low
/// bytes collide with the v1 marker (~2^-16 per table name) must still open
/// as Legacy. A steady-state 7-table file carries the same entries in both
/// slots, so rejecting the collision would brick the database permanently.
#[test]
fn legacy_full_slot_with_marker_colliding_hash_reads_as_legacy() {
    let entry = |hash: u32| (hash, 1u64, 100u32, 1u16);
    let mut slot = sample_slot();
    slot.named_table_entries = (1..=6).map(entry).collect();
    slot.named_table_entries.push(entry(0x0000_C17A));
    slot.seal(&test_mac_key());
    assert_eq!(slot.slot_format, SlotFormat::Legacy);

    let buf = slot.serialize();
    // The colliding hash's low bytes sit exactly at the marker offset.
    assert_eq!(
        u16::from_le_bytes(
            buf[SLOT_FORMAT_MARKER..SLOT_FORMAT_MARKER + 2]
                .try_into()
                .unwrap()
        ),
        SLOT_MARKER_V1
    );

    let rt = CommitSlot::deserialize(&buf);
    assert_eq!(rt.slot_format, SlotFormat::Legacy);
    assert!(rt.verify_checksum());
    assert!(rt.verify_mac(&test_mac_key()));
    assert_eq!(rt.named_table_entries.len(), 7);
    assert_eq!(rt.named_table_entries[6].0, 0x0000_C17A);
}

/// SLOT_ENTRY_STALE rides in the count field: named_entry_count strips it,
/// entry_is_stale reads it, and every entry carried by a legacy slot counts
/// as stale while absent hashes never do.
#[test]
fn stale_entry_flag_roundtrip() {
    let mut slot = sample_slot();
    slot.named_table_entries = vec![
        (table_name_hash(b"stale"), 5 | SLOT_ENTRY_STALE, 42, 3),
        (table_name_hash(b"fresh"), 7, 43, 2),
        (table_name_hash(b"page-zero-root"), 1, 0, 1),
        (table_name_hash(b"catalog-only"), 1, 0, 0),
    ];
    slot.seal(&test_mac_key());
    let slot2 = CommitSlot::deserialize(&slot.serialize());
    assert!(slot2.verify_mac(&test_mac_key()));
    assert_eq!(slot2.named_entry_root(b"stale"), Some((PageId(42), 3)));
    assert_eq!(slot2.named_entry_root(b"fresh"), Some((PageId(43), 2)));
    assert_eq!(
        slot2.named_entry_root(b"page-zero-root"),
        Some((PageId(0), 1))
    );
    assert_eq!(slot2.named_entry_root(b"catalog-only"), None);
    assert_eq!(slot2.named_entry_count(b"stale"), Some(5));
    assert!(slot2.entry_is_stale(table_name_hash(b"stale")));
    assert!(!slot2.entry_is_stale(table_name_hash(b"fresh")));

    let legacy = CommitSlot::deserialize(&serialize_legacy(&slot));
    assert!(legacy.entry_is_stale(table_name_hash(b"fresh")));
    // A hash the legacy slot does not carry has no entry to preserve.
    assert!(!legacy.entry_is_stale(table_name_hash(b"absent")));
}

/// A legacy-sealed slot strips SLOT_ENTRY_STALE from the wire: readers count
/// every carried legacy entry as stale anyway, and a pre-v1 binary opening
/// the file mid-upgrade must read clean counts.
#[test]
fn legacy_slot_wire_carries_no_stale_bits() {
    let entry = |hash: u32| (hash, 1u64 | SLOT_ENTRY_STALE, 100u32, 2u16);
    let mut slot = sample_slot();
    slot.named_table_entries = (1..=7).map(entry).collect();
    slot.seal(&test_mac_key());
    assert_eq!(slot.slot_format, SlotFormat::Legacy);

    let buf = slot.serialize();
    for i in 0..7 {
        let off = SLOT_NAMED_ENTRIES + 2 + i * SLOT_NAMED_ENTRY_SIZE + 4;
        let count = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        assert_eq!(count, 1, "wire count must be clean of the stale bit");
    }

    let rt = CommitSlot::deserialize(&buf);
    assert!(rt.verify_checksum());
    assert!(rt.entry_is_stale(1), "carried legacy entries stay stale");
}

/// recover() must reject a v1 slot whose unprotected-by-checksum region was
/// tampered with, falling back to the other (valid) slot.
#[test]
fn recover_falls_back_when_v1_slot_tampered() {
    use crate::memory_io::MemoryPageIO;

    let mac_key = test_mac_key();
    let io = MemoryPageIO::new();

    let mut header = FileHeader::new(0x77, [0xCC; MAC_SIZE]);
    for slot in &mut header.slots {
        slot.seal(&mac_key);
    }
    write_file_header(&io, &header).unwrap();

    let mut newer = sample_slot();
    newer.seal(&mac_key);
    write_commit_slot(&io, 0, &newer).unwrap();

    let (idx, slot) = recover(&io, &mac_key).unwrap();
    assert_eq!(idx, 0);
    assert_eq!(slot.txn_id, newer.txn_id);

    // Tamper a named entry's root page in the active slot on disk.
    let offset = COMMIT_SLOT_OFFSET + SLOT_NAMED_ENTRIES + 2 + 12;
    let mut byte = [0u8; 1];
    io.read_at(offset as u64, &mut byte).unwrap();
    byte[0] ^= 0x01;
    io.write_at(offset as u64, &byte).unwrap();

    let (idx, slot) = recover(&io, &mac_key).unwrap();
    assert_eq!(idx, 1, "tampered active slot must be rejected");
    assert_eq!(slot.txn_id, TxnId(0));
}

/// A flagged file (HEADER_FLAG_SLOTS_V1) rejects a slot re-encoded in the
/// legacy layout, closing the MAC-stripping downgrade: recover falls back to
/// the other (still V1) slot instead of accepting the tampered one.
#[test]
fn flagged_file_rejects_legacy_downgrade() {
    use crate::memory_io::MemoryPageIO;

    let mac_key = test_mac_key();
    let io = MemoryPageIO::new();

    let mut header = FileHeader::new(0x99, [0xEE; MAC_SIZE]);
    assert_ne!(header.flags & HEADER_FLAG_SLOTS_V1, 0, "new files flagged");
    for slot in &mut header.slots {
        slot.seal(&mac_key);
    }
    write_file_header(&io, &header).unwrap();

    let mut newer = sample_slot();
    newer.seal(&mac_key);
    write_commit_slot(&io, 0, &newer).unwrap();
    let (idx, _) = recover(&io, &mac_key).unwrap();
    assert_eq!(idx, 0);

    // Downgrade attack: rewrite the active slot in the legacy layout with a
    // freshly valid keyless checksum (content intact, MAC stripped).
    let mut legacy = newer.clone();
    legacy.slot_format = SlotFormat::Legacy;
    legacy.slot_mac = [0u8; SLOT_MAC_SIZE];
    let mut buf = legacy.serialize();
    let cs = xxhash_rust::xxh64::xxh64(&buf[..SLOT_CHECKSUM], 0);
    buf[SLOT_CHECKSUM..SLOT_CHECKSUM + 8].copy_from_slice(&cs.to_le_bytes());
    io.write_at(COMMIT_SLOT_OFFSET as u64, &buf).unwrap();

    // Refuse loudly: silently opening the older generation would hand a
    // rollback attacker the stale state they staged, and would discard a
    // legitimate pre-v1-binary commit without notice.
    let err = recover(&io, &mac_key).unwrap_err();
    assert!(
        matches!(err, Error::SlotDowngradeDetected),
        "downgraded slot must fail loudly, got: {err}"
    );

    // The same bytes are accepted on an unflagged (pre-v1) file.
    io.write_at(HEADER_FLAGS_OFFSET as u64, &[0]).unwrap();
    let (idx, _) = recover(&io, &mac_key).unwrap();
    assert_eq!(idx, 0, "unflagged pre-v1 files keep accepting legacy slots");
}

#[test]
fn authenticated_requirement_rejects_downgrade_after_header_flag_is_cleared() {
    use crate::memory_io::MemoryPageIO;

    let mac_key = test_mac_key();
    let io = MemoryPageIO::new();
    let mut header = FileHeader::new(0x99, [0xEE; MAC_SIZE]);
    for slot in &mut header.slots {
        slot.seal(&mac_key);
    }
    write_file_header(&io, &header).unwrap();

    // The attacker can rewrite the mutable header byte and generate a valid
    // keyless legacy checksum, but cannot clear the key-file requirement that
    // the caller supplied after authenticating it.
    io.write_at(HEADER_FLAGS_OFFSET as u64, &[0]).unwrap();
    for idx in 0..2 {
        let mut legacy = read_commit_slot(&io, idx).unwrap();
        legacy.slot_format = SlotFormat::Legacy;
        legacy.slot_mac = [0u8; SLOT_MAC_SIZE];
        write_commit_slot(&io, idx, &legacy).unwrap();
    }

    assert!(recover(&io, &mac_key).is_ok(), "direct compatibility path");
    assert!(matches!(
        recover_with_v1_requirement(&io, &mac_key, true),
        Err(Error::SlotDowngradeDetected)
    ));
}

/// The one-way upgrade stamps the flag only once both physical slots are V1.
#[test]
fn mark_slots_v1_waits_for_both_slots() {
    use crate::memory_io::MemoryPageIO;

    let mac_key = test_mac_key();
    let io = MemoryPageIO::new();

    let mut header = FileHeader::new(0x55, [0xDD; MAC_SIZE]);
    header.flags = 0;
    for slot in &mut header.slots {
        slot.slot_format = SlotFormat::Legacy;
        slot.slot_mac = [0u8; SLOT_MAC_SIZE];
    }
    write_file_header(&io, &header).unwrap();

    mark_slots_v1_if_upgraded(&io, &mac_key).unwrap();
    assert_eq!(read_header_flags(&io).unwrap() & HEADER_FLAG_SLOTS_V1, 0);

    let mut sealed = sample_slot();
    sealed.seal(&mac_key);
    write_commit_slot(&io, 0, &sealed).unwrap();
    mark_slots_v1_if_upgraded(&io, &mac_key).unwrap();
    assert_eq!(
        read_header_flags(&io).unwrap() & HEADER_FLAG_SLOTS_V1,
        0,
        "one V1 slot is not enough"
    );

    write_commit_slot(&io, 1, &sealed).unwrap();
    mark_slots_v1_if_upgraded(&io, &mac_key).unwrap();
    assert_ne!(read_header_flags(&io).unwrap() & HEADER_FLAG_SLOTS_V1, 0);
}

/// A flagged file whose slots have both been downgraded to legacy must fail
/// closed with the downgrade diagnosis, never silently accept a MAC-stripped
/// slot.
#[test]
fn flagged_file_both_slots_downgraded_is_corrupted() {
    use crate::memory_io::MemoryPageIO;

    let mac_key = test_mac_key();
    let io = MemoryPageIO::new();

    let mut header = FileHeader::new(0xA1, [0x33; MAC_SIZE]);
    for slot in &mut header.slots {
        slot.seal(&mac_key);
    }
    write_file_header(&io, &header).unwrap();
    assert!(recover(&io, &mac_key).is_ok());

    // Re-encode both slots in the legacy layout with valid keyless checksums.
    for idx in 0..2 {
        let mut legacy = sample_slot();
        legacy.slot_format = SlotFormat::Legacy;
        legacy.slot_mac = [0u8; SLOT_MAC_SIZE];
        let mut buf = legacy.serialize();
        let cs = xxhash_rust::xxh64::xxh64(&buf[..SLOT_CHECKSUM], 0);
        buf[SLOT_CHECKSUM..SLOT_CHECKSUM + 8].copy_from_slice(&cs.to_le_bytes());
        io.write_at((COMMIT_SLOT_OFFSET + idx * COMMIT_SLOT_SIZE) as u64, &buf)
            .unwrap();
    }

    assert!(
        matches!(recover(&io, &mac_key), Err(Error::SlotDowngradeDetected)),
        "flagged file with two legacy slots must be rejected"
    );
}

/// A flagged file also rejects a V1 slot whose MAC was forged, falling back to
/// the intact slot (the flag path must not weaken MAC checking).
#[test]
fn flagged_file_rejects_forged_v1_mac() {
    use crate::memory_io::MemoryPageIO;

    let mac_key = test_mac_key();
    let io = MemoryPageIO::new();

    let mut header = FileHeader::new(0xB2, [0x44; MAC_SIZE]);
    for slot in &mut header.slots {
        slot.seal(&mac_key);
    }
    write_file_header(&io, &header).unwrap();

    let mut newer = sample_slot();
    newer.seal(&mac_key);
    write_commit_slot(&io, 0, &newer).unwrap();
    assert_eq!(recover(&io, &mac_key).unwrap().0, 0);

    // Forge the active slot's MAC in place (still V1 format, bad MAC).
    let mut byte = [0u8; 1];
    io.read_at((COMMIT_SLOT_OFFSET + SLOT_MAC) as u64, &mut byte)
        .unwrap();
    byte[0] ^= 0xFF;
    io.write_at((COMMIT_SLOT_OFFSET + SLOT_MAC) as u64, &byte)
        .unwrap();

    let (idx, slot) = recover(&io, &mac_key).unwrap();
    assert_eq!(
        idx, 1,
        "forged-MAC V1 slot must be rejected on a flagged file"
    );
    assert_eq!(slot.txn_id, TxnId(0));
}

/// Exactly the V1 capacity seals V1 and preserves every entry; one more entry
/// tips it to the legacy fallback. Pins the seal boundary both sides.
#[test]
fn seal_capacity_boundary_v1_then_legacy() {
    let entry = |i: u32| (i + 1, 10 * i as u64, 100 + i, 2u16);

    let mut at_cap = sample_slot();
    at_cap.named_table_entries = (0..SLOT_NAMED_MAX_ENTRIES_V1 as u32).map(entry).collect();
    at_cap.seal(&test_mac_key());
    assert_eq!(at_cap.slot_format, SlotFormat::V1);
    let rt = CommitSlot::deserialize(&at_cap.serialize());
    assert_eq!(rt.slot_format, SlotFormat::V1);
    assert!(rt.verify_mac(&test_mac_key()));
    assert_eq!(rt.named_table_entries.len(), SLOT_NAMED_MAX_ENTRIES_V1);

    let mut over_cap = sample_slot();
    over_cap.named_table_entries = (0..SLOT_NAMED_MAX_ENTRIES_V1 as u32 + 1)
        .map(entry)
        .collect();
    over_cap.seal(&test_mac_key());
    assert_eq!(over_cap.slot_format, SlotFormat::Legacy);
    let rt = CommitSlot::deserialize(&over_cap.serialize());
    assert_eq!(rt.slot_format, SlotFormat::Legacy);
    assert!(rt.verify_mac(&test_mac_key()));
    assert_eq!(rt.named_table_entries.len(), SLOT_NAMED_MAX_ENTRIES_V1 + 1);
}

/// entry_is_stale: V1 reads the per-entry flag; legacy treats every carried
/// hash as stale but an absent hash as fresh (a phantom entry would overflow
/// the capacity and drop a sole-record root). Absent hashes are never stale.
#[test]
fn entry_is_stale_semantics() {
    let mut v1 = sample_slot();
    v1.named_table_entries = vec![
        (table_name_hash(b"stale"), 1 | SLOT_ENTRY_STALE, 9, 1),
        (table_name_hash(b"fresh"), 1, 10, 1),
    ];
    v1.seal(&test_mac_key());
    let v1 = CommitSlot::deserialize(&v1.serialize());
    assert!(v1.entry_is_stale(table_name_hash(b"stale")));
    assert!(!v1.entry_is_stale(table_name_hash(b"fresh")));
    assert!(!v1.entry_is_stale(table_name_hash(b"absent")));

    let legacy = CommitSlot::deserialize(&serialize_legacy(&sample_slot()));
    // sample_slot carries exactly one entry (hash 0x12345678).
    assert!(legacy.entry_is_stale(0x12345678));
    assert!(!legacy.entry_is_stale(table_name_hash(b"absent")));
}

/// The one-way upgrade is a no-op when already flagged, and refuses to flag a
/// file whose slot is Unknown (corrupt) rather than V1.
#[test]
fn mark_slots_v1_idempotent_and_skips_corrupt() {
    use crate::memory_io::MemoryPageIO;

    let mac_key = test_mac_key();
    let io = MemoryPageIO::new();

    // Unflagged file, slot 0 V1, slot 1 Unknown (corrupt marker).
    let mut header = FileHeader::new(0xC3, [0x55; MAC_SIZE]);
    header.flags = 0;
    for slot in &mut header.slots {
        slot.seal(&mac_key);
    }
    write_file_header(&io, &header).unwrap();
    io.write_at(
        (COMMIT_SLOT_OFFSET + COMMIT_SLOT_SIZE + SLOT_FORMAT_MARKER) as u64,
        &0xBEEFu16.to_le_bytes(),
    )
    .unwrap();

    mark_slots_v1_if_upgraded(&io, &mac_key).unwrap();
    assert_eq!(
        read_header_flags(&io).unwrap() & HEADER_FLAG_SLOTS_V1,
        0,
        "a corrupt (Unknown) slot must block the flag"
    );

    // Pre-set the flag: mark is a one-way no-op even with a legacy slot.
    io.write_at(HEADER_FLAGS_OFFSET as u64, &[HEADER_FLAG_SLOTS_V1])
        .unwrap();
    let mut legacy = sample_slot();
    legacy.slot_format = SlotFormat::Legacy;
    legacy.slot_mac = [0u8; SLOT_MAC_SIZE];
    write_commit_slot(&io, 0, &legacy).unwrap();
    mark_slots_v1_if_upgraded(&io, &mac_key).unwrap();
    assert_ne!(
        read_header_flags(&io).unwrap() & HEADER_FLAG_SLOTS_V1,
        0,
        "the flag is one-way: never cleared"
    );
}

#[test]
fn mark_slots_v1_requires_authenticated_slots_not_only_format_markers() {
    use crate::memory_io::MemoryPageIO;

    let mac_key = test_mac_key();
    let io = MemoryPageIO::new();
    let mut header = FileHeader::new(0xC4, [0x66; MAC_SIZE]);
    header.flags = 0;
    for slot in &mut header.slots {
        slot.seal(&mac_key);
    }

    // Keep the V1 marker and keyless checksum valid while invalidating the
    // HMAC. A format-only check would permanently stamp this incomplete state.
    header.slots[1].tree_entries = header.slots[1].tree_entries.wrapping_add(1);
    write_file_header(&io, &header).unwrap();

    assert!(!mark_slots_v1_if_upgraded(&io, &mac_key).unwrap());
    assert_eq!(read_header_flags(&io).unwrap() & HEADER_FLAG_SLOTS_V1, 0);
}

#[test]
fn file_header_serialize_roundtrip() {
    let dek_id = [0xBB; MAC_SIZE];
    let header = FileHeader::new(0x1234, dek_id);

    let buf = header.serialize();
    let header2 = FileHeader::deserialize(&buf).unwrap();

    assert_eq!(header2.magic, MAGIC);
    assert_eq!(header2.format_version, FORMAT_VERSION);
    assert_eq!(header2.page_size, PAGE_SIZE as u32);
    assert_eq!(header2.file_id, 0x1234);
    assert_eq!(header2.god_byte, 0);
    assert_ne!(header2.flags & HEADER_FLAG_SLOTS_V1, 0);
    assert!(header2
        .slots
        .iter()
        .all(|slot| slot.merkle_scheme == MerkleScheme::LogicalOverflowV1));
    assert_eq!(header2.active_slot(), 0);
    assert!(!header2.recovery_required());
}

/// The field-by-field test above covers the fields someone remembered to list.
/// This one covers every field, including both commit slots, so a field the
/// serializer drops fails here instead of surviving to a reopen.
#[test]
fn file_header_roundtrip_preserves_every_field() {
    let mut header = FileHeader::new(0x1234, [0xBB; MAC_SIZE]);
    header.god_byte = 0x01;
    header.slots[0].txn_id = TxnId(77);
    header.slots[0].tree_root = PageId(9);
    header.slots[0].tree_entries = 4242;
    header.slots[0].merkle_root = [0x5A; MERKLE_HASH_SIZE];
    header.slots[0].named_table_entries = vec![(1, 2, 3, 4), (5, 6, 7, 8)];
    header.slots[1].txn_id = TxnId(76);
    header.slots[1].high_water_mark = 31;

    let round_tripped = FileHeader::deserialize(&header.serialize()).unwrap();

    // `checksum` is computed by serialize(), so it is the one field that cannot
    // match what went in. Adopt it deliberately, after checking it was computed.
    for (i, slot) in round_tripped.slots.iter().enumerate() {
        assert_ne!(slot.checksum, 0, "slot {i} checksum was never computed");
        header.slots[i].checksum = slot.checksum;
    }

    assert_eq!(round_tripped, header);
}

#[test]
fn file_header_invalid_magic() {
    let mut buf = [0u8; FILE_HEADER_SIZE];
    buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
    let result = FileHeader::deserialize(&buf);
    assert!(matches!(result, Err(Error::InvalidMagic { .. })));
}

#[test]
fn god_byte_active_slot() {
    let mut header = FileHeader::new(0, [0; MAC_SIZE]);
    assert_eq!(header.active_slot(), 0);
    assert_eq!(header.inactive_slot(), 1);

    header.god_byte = 0x01; // active = slot 1
    assert_eq!(header.active_slot(), 1);
    assert_eq!(header.inactive_slot(), 0);
}

#[test]
fn god_byte_recovery_flag() {
    let mut header = FileHeader::new(0, [0; MAC_SIZE]);
    assert!(!header.recovery_required());

    header.god_byte = GOD_BIT_RECOVERY; // recovery + slot 0
    assert!(header.recovery_required());
    assert_eq!(header.active_slot(), 0);

    header.god_byte = GOD_BIT_RECOVERY | GOD_BIT_ACTIVE_SLOT; // recovery + slot 1
    assert!(header.recovery_required());
    assert_eq!(header.active_slot(), 1);
}

#[test]
fn page_offset_calculation() {
    assert_eq!(page_offset(PageId(0)), FILE_HEADER_SIZE as u64);
    assert_eq!(
        page_offset(PageId(1)),
        FILE_HEADER_SIZE as u64 + PAGE_SIZE as u64
    );
    assert_eq!(
        page_offset(PageId(10)),
        FILE_HEADER_SIZE as u64 + 10 * PAGE_SIZE as u64
    );
}

#[test]
fn growth_chunk_sizes() {
    assert_eq!(growth_chunk(0), GROWTH_CHUNK_1MB);
    assert_eq!(growth_chunk(1_000_000), GROWTH_CHUNK_1MB);
    assert_eq!(growth_chunk(GROWTH_THRESHOLD_4MB), GROWTH_CHUNK_4MB);
    assert_eq!(growth_chunk(GROWTH_THRESHOLD_64MB), GROWTH_CHUNK_16MB);
    assert_eq!(growth_chunk(GROWTH_THRESHOLD_1GB), GROWTH_CHUNK_16MB);
    assert_eq!(
        growth_chunk(10 * GROWTH_THRESHOLD_1GB),
        10 * GROWTH_THRESHOLD_1GB / 100
    );
}
