//! Integration test: full encryption round-trip through all Phase 1 layers.
//!
//! Creates a database file, writes encrypted pages via buffer pool,
//! flushes to disk, reads back, verifies data integrity.

use citadel_core::types::*;
use citadel_core::*;
use citadel_crypto::key_manager::{create_key_file, open_key_file};
use citadel_crypto::page_cipher;
use citadel_io::file_manager::*;
use citadel_io::sync_io::SyncPageIO;
use citadel_io::traits::PageIO;
use citadel_buffer::pool::BufferPool;
use citadel_page::page::Page;

use std::fs::File;

#[test]
fn full_encryption_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.citadel");
    let key_path = dir.path().join("test.citadel-keys");

    let passphrase = b"integration-test-passphrase";
    let file_id: u64 = 0xDEAD_CAFE_1234;

    // 1. Create key file
    let (key_file, keys) = create_key_file(
        passphrase,
        file_id,
        CipherId::Aes256Ctr,
        64, 1, 1, // minimal params for test speed
    ).unwrap();
    let key_buf = key_file.serialize();
    std::fs::write(&key_path, &key_buf).unwrap();

    // 2. Create data file with header
    let file = File::options()
        .read(true).write(true).create(true)
        .open(&db_path).unwrap();
    let io = SyncPageIO::new(file);

    let dek_id = page_cipher::compute_dek_id(&keys.mac_key, &keys.dek);
    let header = FileHeader::new(file_id, dek_id);
    write_file_header(&io, &header).unwrap();
    io.fsync().unwrap();

    // 3. Create and encrypt pages via buffer pool
    let mut pool = BufferPool::new(64);
    let epoch = 1u32;

    // Create 10 leaf pages with test data
    for i in 0..10u32 {
        let mut page = Page::new(PageId(i), PageType::Leaf, TxnId(1));
        let cell = format!("key-{i}:value-{i}");
        page.write_cell(cell.as_bytes()).unwrap();
        page.update_checksum();

        let offset = page_offset(PageId(i));
        ensure_file_size(&io, offset).unwrap();
        pool.insert_new(PageId(i), page).unwrap();
    }

    assert_eq!(pool.dirty_count(), 10);

    // 4. Flush dirty pages (encrypts and writes to disk)
    pool.flush_dirty(&io, &keys.dek, &keys.mac_key, epoch).unwrap();
    io.fsync().unwrap();
    assert_eq!(pool.dirty_count(), 0);

    // 5. Verify no plaintext on disk
    let raw_bytes = std::fs::read(&db_path).unwrap();
    for i in 0..10u32 {
        let needle = format!("key-{i}:value-{i}");
        let found = raw_bytes.windows(needle.len())
            .any(|w| w == needle.as_bytes());
        assert!(!found, "Plaintext found on disk for page {i}!");
    }

    // 6. Create a new buffer pool (simulating reopen) and read back
    let mut pool2 = BufferPool::new(64);

    for i in 0..10u32 {
        let page = pool2.fetch(&io, PageId(i), &keys.dek, &keys.mac_key, epoch).unwrap();
        assert_eq!(page.page_id(), PageId(i));
        assert_eq!(page.page_type(), Some(PageType::Leaf));
        assert_eq!(page.txn_id(), TxnId(1));
        assert_eq!(page.num_cells(), 1);
        assert!(page.verify_checksum());

        // Read cell data back
        let offset = page.cell_offset(0);
        let expected = format!("key-{i}:value-{i}");
        let data = page.cell_data(offset, expected.len());
        assert_eq!(data, expected.as_bytes());
    }

    // 7. Verify tamper detection: flip a bit in encrypted page on disk
    {
        let tamper_offset = page_offset(PageId(5));
        let mut encrypted = [0u8; PAGE_SIZE];
        io.read_page(tamper_offset, &mut encrypted).unwrap();
        encrypted[100] ^= 0x01; // flip bit in ciphertext
        io.write_page(tamper_offset, &encrypted).unwrap();
    }

    // New pool, should detect tamper on page 5
    let mut pool3 = BufferPool::new(64);
    let result = pool3.fetch(&io, PageId(5), &keys.dek, &keys.mac_key, epoch);
    assert!(matches!(result, Err(Error::PageTampered(PageId(5)))));

    // Other pages should still be readable
    let page0 = pool3.fetch(&io, PageId(0), &keys.dek, &keys.mac_key, epoch).unwrap();
    assert_eq!(page0.page_id(), PageId(0));

    // 8. Verify key file re-open with correct password
    let key_bytes: [u8; KEY_FILE_SIZE] = key_buf.try_into().unwrap();
    let (_kf2, keys2) = open_key_file(&key_bytes, passphrase, file_id).unwrap();
    assert_eq!(keys2.dek, keys.dek);
    assert_eq!(keys2.mac_key, keys.mac_key);

    // 9. Wrong password should fail
    let result = open_key_file(&key_bytes, b"wrong-password", file_id);
    assert!(result.is_err());

    // 10. Wrong file_id should fail
    let result = open_key_file(&key_bytes, passphrase, 0xBAAD);
    assert!(matches!(result, Err(Error::KeyFileMismatch)));
}

#[test]
fn file_header_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("recovery.citadel");

    let file = File::options()
        .read(true).write(true).create(true)
        .open(&db_path).unwrap();
    let io = SyncPageIO::new(file);

    let dek_id = [0xAAu8; MAC_SIZE];
    let header = FileHeader::new(0x42, dek_id);
    write_file_header(&io, &header).unwrap();
    io.fsync().unwrap();

    // Normal recovery — both slots valid (both empty/zero)
    let (slot_idx, slot) = recover(&io).unwrap();
    assert_eq!(slot_idx, 0);
    assert_eq!(slot.txn_id, TxnId(0));

    // Write to inactive slot and flip god byte
    let new_slot = CommitSlot {
        txn_id: TxnId(1),
        tree_root: PageId(0),
        tree_depth: 1,
        tree_entries: 100,
        catalog_root: PageId(0),
        total_pages: 10,
        high_water_mark: 9,
        pending_free_root: PageId::INVALID,
        encryption_epoch: 1,
        dek_id,
        checksum: 0,
    };

    write_commit_slot(&io, 1, &new_slot).unwrap(); // write to inactive slot 1
    io.fsync().unwrap();
    write_god_byte(&io, GOD_BIT_ACTIVE_SLOT).unwrap(); // flip to slot 1
    io.fsync().unwrap();

    // Recovery should find slot 1
    let (slot_idx, slot) = recover(&io).unwrap();
    assert_eq!(slot_idx, 1);
    assert_eq!(slot.txn_id, TxnId(1));
    assert_eq!(slot.tree_entries, 100);
}

#[test]
fn sieve_eviction_dirty_never_evicted() {
    use citadel_buffer::sieve::SieveCache;

    let mut cache = SieveCache::<String>::new(3);

    // Fill cache
    cache.insert(1, "one".into()).unwrap();
    cache.insert(2, "two".into()).unwrap();
    cache.insert(3, "three".into()).unwrap();

    // Mark entry 2 as dirty (pinned — must never be evicted)
    cache.set_dirty(2);

    // Insert multiple entries — dirty entry 2 must survive all evictions
    for i in 10..15 {
        cache.insert(i, format!("val-{i}")).unwrap();
        assert!(cache.contains(2), "dirty entry 2 should never be evicted");
        assert_eq!(cache.get(2), Some(&"two".to_string()));
    }

    assert_eq!(cache.dirty_count(), 1);
    assert!(cache.is_dirty(2));
}
