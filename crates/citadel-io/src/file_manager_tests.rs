use super::*;

#[test]
fn commit_slot_serialize_roundtrip() {
    let slot = CommitSlot {
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
        checksum: 0,
        merkle_root: [0xBB; MERKLE_HASH_SIZE],
        named_table_entries: vec![(0x12345678, 500, 77, 3)],
    };

    let buf = slot.serialize();
    let slot2 = CommitSlot::deserialize(&buf);

    assert_eq!(slot2.txn_id, TxnId(42));
    assert_eq!(slot2.tree_root, PageId(10));
    assert_eq!(slot2.tree_depth, 3);
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
        checksum: 0,
        merkle_root: [0; MERKLE_HASH_SIZE],
        named_table_entries: Vec::new(),
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
    assert_eq!(header2.active_slot(), 0);
    assert!(!header2.recovery_required());
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
