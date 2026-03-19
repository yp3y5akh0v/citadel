//! Integration test: full encryption round-trip through all layers.
//!
//! Creates a database file, writes encrypted pages via buffer pool,
//! flushes to disk, reads back, verifies data integrity.

use citadel_buffer::pool::BufferPool;
use citadel_core::types::*;
use citadel_core::*;
use citadel_crypto::key_manager::{create_key_file, open_key_file};
use citadel_crypto::page_cipher;
use citadel_io::file_manager::*;
use citadel_io::sync_io::SyncPageIO;
use citadel_io::traits::PageIO;
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
        KdfAlgorithm::Argon2id,
        64,
        1,
        1, // minimal params for test speed
    )
    .unwrap();
    let key_buf = key_file.serialize();
    std::fs::write(&key_path, key_buf).unwrap();

    // 2. Create data file with header
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&db_path)
        .unwrap();
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
    pool.flush_dirty(&io, &keys.dek, &keys.mac_key, epoch)
        .unwrap();
    io.fsync().unwrap();
    assert_eq!(pool.dirty_count(), 0);

    // 5. Verify no plaintext on disk
    let raw_bytes = std::fs::read(&db_path).unwrap();
    for i in 0..10u32 {
        let needle = format!("key-{i}:value-{i}");
        let found = raw_bytes
            .windows(needle.len())
            .any(|w| w == needle.as_bytes());
        assert!(!found, "Plaintext found on disk for page {i}!");
    }

    // 6. Create a new buffer pool (simulating reopen) and read back
    let mut pool2 = BufferPool::new(64);

    for i in 0..10u32 {
        let page = pool2
            .fetch(&io, PageId(i), &keys.dek, &keys.mac_key, epoch)
            .unwrap();
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
    let page0 = pool3
        .fetch(&io, PageId(0), &keys.dek, &keys.mac_key, epoch)
        .unwrap();
    assert_eq!(page0.page_id(), PageId(0));

    // 8. Verify key file re-open with correct password
    let key_bytes: [u8; KEY_FILE_SIZE] = key_buf;
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
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&db_path)
        .unwrap();
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
        merkle_root: [0u8; citadel_core::MERKLE_HASH_SIZE],
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

// === Edge Case Tests ===

#[test]
fn wrong_epoch_detected_on_fetch() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("epoch.citadel");

    let (key_file, keys) = create_key_file(
        b"password",
        0x1111,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();
    let _ = key_file;

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&db_path)
        .unwrap();
    let io = SyncPageIO::new(file);

    let dek_id = page_cipher::compute_dek_id(&keys.mac_key, &keys.dek);
    let header = FileHeader::new(0x1111, dek_id);
    write_file_header(&io, &header).unwrap();

    // Write page with epoch 1
    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    page.update_checksum();
    let mut encrypted = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        &keys.dek,
        &keys.mac_key,
        PageId(0),
        1,
        page.as_bytes(),
        &mut encrypted,
    );
    let offset = page_offset(PageId(0));
    ensure_file_size(&io, offset).unwrap();
    io.write_page(offset, &encrypted).unwrap();
    io.fsync().unwrap();

    // Read with wrong epoch (epoch 2) — HMAC includes epoch, so it should fail
    let mut pool = BufferPool::new(64);
    let result = pool.fetch(&io, PageId(0), &keys.dek, &keys.mac_key, 2);
    assert!(
        matches!(result, Err(Error::PageTampered(PageId(0)))),
        "wrong epoch should be detected as tampered"
    );

    // Read with correct epoch should succeed
    let page_back = pool
        .fetch(&io, PageId(0), &keys.dek, &keys.mac_key, 1)
        .unwrap();
    assert_eq!(page_back.page_id(), PageId(0));
}

#[test]
fn buffer_pool_eviction_under_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("pressure.citadel");

    let (_, keys) = create_key_file(
        b"pass",
        0x2222,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&db_path)
        .unwrap();
    let io = SyncPageIO::new(file);

    let dek_id = page_cipher::compute_dek_id(&keys.mac_key, &keys.dek);
    let header = FileHeader::new(0x2222, dek_id);
    write_file_header(&io, &header).unwrap();

    // Write 50 pages to disk
    let epoch = 1u32;
    for i in 0..50u32 {
        let mut page = Page::new(PageId(i), PageType::Leaf, TxnId(1));
        let cell = format!("data-{i:04}");
        page.write_cell(cell.as_bytes()).unwrap();
        page.update_checksum();
        let offset = page_offset(PageId(i));
        ensure_file_size(&io, offset).unwrap();
        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(
            &keys.dek,
            &keys.mac_key,
            PageId(i),
            epoch,
            page.as_bytes(),
            &mut encrypted,
        );
        io.write_page(offset, &encrypted).unwrap();
    }
    io.fsync().unwrap();

    // Create pool with capacity 10 (much less than 50 pages)
    let mut pool = BufferPool::new(10);

    // Fetch all 50 pages — forces eviction of older pages
    for i in 0..50u32 {
        let page = pool
            .fetch(&io, PageId(i), &keys.dek, &keys.mac_key, epoch)
            .unwrap();
        assert_eq!(page.page_id(), PageId(i));
    }

    // Re-fetch first page (was evicted, must re-read from disk)
    let page0 = pool
        .fetch(&io, PageId(0), &keys.dek, &keys.mac_key, epoch)
        .unwrap();
    assert_eq!(page0.page_id(), PageId(0));
    assert_eq!(page0.num_cells(), 1);
}

#[test]
fn tamper_iv_region_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("tamper_iv.citadel");

    let (_, keys) = create_key_file(
        b"pass",
        0x3333,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&db_path)
        .unwrap();
    let io = SyncPageIO::new(file);

    let dek_id = page_cipher::compute_dek_id(&keys.mac_key, &keys.dek);
    let header = FileHeader::new(0x3333, dek_id);
    write_file_header(&io, &header).unwrap();

    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    page.update_checksum();
    let mut encrypted = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        &keys.dek,
        &keys.mac_key,
        PageId(0),
        1,
        page.as_bytes(),
        &mut encrypted,
    );
    let offset = page_offset(PageId(0));
    ensure_file_size(&io, offset).unwrap();
    io.write_page(offset, &encrypted).unwrap();
    io.fsync().unwrap();

    // Tamper with the IV region (first 16 bytes of the on-disk page)
    let mut tampered = [0u8; PAGE_SIZE];
    io.read_page(offset, &mut tampered).unwrap();
    tampered[0] ^= 0xFF; // Flip bits in IV
    io.write_page(offset, &tampered).unwrap();

    let mut pool = BufferPool::new(64);
    let result = pool.fetch(&io, PageId(0), &keys.dek, &keys.mac_key, 1);
    assert!(
        matches!(result, Err(Error::PageTampered(PageId(0)))),
        "tampered IV should be detected by HMAC"
    );
}

#[test]
fn tamper_mac_region_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("tamper_mac.citadel");

    let (_, keys) = create_key_file(
        b"pass",
        0x4444,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&db_path)
        .unwrap();
    let io = SyncPageIO::new(file);

    let dek_id = page_cipher::compute_dek_id(&keys.mac_key, &keys.dek);
    let header = FileHeader::new(0x4444, dek_id);
    write_file_header(&io, &header).unwrap();

    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    page.update_checksum();
    let mut encrypted = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        &keys.dek,
        &keys.mac_key,
        PageId(0),
        1,
        page.as_bytes(),
        &mut encrypted,
    );
    let offset = page_offset(PageId(0));
    ensure_file_size(&io, offset).unwrap();
    io.write_page(offset, &encrypted).unwrap();
    io.fsync().unwrap();

    // Tamper with the MAC region (last 32 bytes)
    let mut tampered = [0u8; PAGE_SIZE];
    io.read_page(offset, &mut tampered).unwrap();
    tampered[PAGE_SIZE - 1] ^= 0x01; // Flip last byte of MAC
    io.write_page(offset, &tampered).unwrap();

    let mut pool = BufferPool::new(64);
    let result = pool.fetch(&io, PageId(0), &keys.dek, &keys.mac_key, 1);
    assert!(
        matches!(result, Err(Error::PageTampered(PageId(0)))),
        "tampered MAC should be detected"
    );
}

#[test]
fn different_keys_produce_different_ciphertext() {
    let (_, keys1) = create_key_file(
        b"password1",
        0x5555,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();
    let (_, keys2) = create_key_file(
        b"password2",
        0x5555,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    page.update_checksum();

    let mut enc1 = [0u8; PAGE_SIZE];
    let mut enc2 = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        &keys1.dek,
        &keys1.mac_key,
        PageId(0),
        1,
        page.as_bytes(),
        &mut enc1,
    );
    page_cipher::encrypt_page(
        &keys2.dek,
        &keys2.mac_key,
        PageId(0),
        1,
        page.as_bytes(),
        &mut enc2,
    );

    // Ciphertext should differ (different keys)
    assert_ne!(
        &enc1[16..PAGE_SIZE - 32],
        &enc2[16..PAGE_SIZE - 32],
        "different DEKs must produce different ciphertext"
    );
    // MACs should also differ
    assert_ne!(
        &enc1[PAGE_SIZE - 32..],
        &enc2[PAGE_SIZE - 32..],
        "different MAC keys must produce different MACs"
    );
}

#[test]
fn same_page_encrypted_differently_each_write() {
    let (_, keys) = create_key_file(
        b"pass",
        0x6666,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    page.update_checksum();

    let mut enc1 = [0u8; PAGE_SIZE];
    let mut enc2 = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        &keys.dek,
        &keys.mac_key,
        PageId(0),
        1,
        page.as_bytes(),
        &mut enc1,
    );
    page_cipher::encrypt_page(
        &keys.dek,
        &keys.mac_key,
        PageId(0),
        1,
        page.as_bytes(),
        &mut enc2,
    );

    // Random IV means same plaintext encrypts differently each time
    assert_ne!(
        &enc1[0..16],
        &enc2[0..16],
        "IVs should be different (random)"
    );
    assert_ne!(
        &enc1[16..PAGE_SIZE - 32],
        &enc2[16..PAGE_SIZE - 32],
        "ciphertext should differ due to different random IV"
    );
}

#[test]
fn cache_hit_returns_identical_data() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("cache_hit.citadel");

    let (_, keys) = create_key_file(
        b"pass",
        0x7777,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&db_path)
        .unwrap();
    let io = SyncPageIO::new(file);

    let dek_id = page_cipher::compute_dek_id(&keys.mac_key, &keys.dek);
    let header = FileHeader::new(0x7777, dek_id);
    write_file_header(&io, &header).unwrap();

    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    let cell = b"test-cell-data";
    page.write_cell(cell).unwrap();
    page.update_checksum();
    let offset = page_offset(PageId(0));
    ensure_file_size(&io, offset).unwrap();
    let mut encrypted = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        &keys.dek,
        &keys.mac_key,
        PageId(0),
        1,
        page.as_bytes(),
        &mut encrypted,
    );
    io.write_page(offset, &encrypted).unwrap();
    io.fsync().unwrap();

    let mut pool = BufferPool::new(64);

    // First fetch (cache miss — reads from disk)
    let p1 = pool
        .fetch(&io, PageId(0), &keys.dek, &keys.mac_key, 1)
        .unwrap()
        .clone();
    // Second fetch (cache hit — returns cached)
    let p2 = pool
        .fetch(&io, PageId(0), &keys.dek, &keys.mac_key, 1)
        .unwrap();

    assert_eq!(p1.page_id(), p2.page_id());
    assert_eq!(p1.num_cells(), p2.num_cells());
    assert_eq!(p1.as_bytes(), p2.as_bytes());
}

#[test]
fn multiple_page_types_all_encrypted() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("types.citadel");

    let (_, keys) = create_key_file(
        b"pass",
        0x8888,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&db_path)
        .unwrap();
    let io = SyncPageIO::new(file);

    let dek_id = page_cipher::compute_dek_id(&keys.mac_key, &keys.dek);
    let header = FileHeader::new(0x8888, dek_id);
    write_file_header(&io, &header).unwrap();

    let epoch = 1u32;
    let page_types = [(PageId(0), PageType::Leaf), (PageId(1), PageType::Branch)];

    // Write different page types
    for &(page_id, page_type) in &page_types {
        let mut page = Page::new(page_id, page_type, TxnId(1));
        page.update_checksum();
        let offset = page_offset(page_id);
        ensure_file_size(&io, offset).unwrap();
        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(
            &keys.dek,
            &keys.mac_key,
            page_id,
            epoch,
            page.as_bytes(),
            &mut encrypted,
        );
        io.write_page(offset, &encrypted).unwrap();
    }
    io.fsync().unwrap();

    // Read back and verify types preserved
    let mut pool = BufferPool::new(64);
    for &(page_id, page_type) in &page_types {
        let page = pool
            .fetch(&io, page_id, &keys.dek, &keys.mac_key, epoch)
            .unwrap();
        assert_eq!(
            page.page_type(),
            Some(page_type),
            "page type should survive encrypt/decrypt for {:?}",
            page_id
        );
    }
}

// === Additional Encryption Edge Cases ===

#[test]
fn page_swap_attack_detected() {
    // Real attack vector: an adversary with disk access swaps two encrypted
    // pages on disk. The HMAC includes page_id, so swapping page 0 and page 1
    // positions should cause HMAC verification to fail for both.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swap.citadel");

    let (_, keys) = create_key_file(
        b"pass",
        0xAAAA,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&db_path)
        .unwrap();
    let io = SyncPageIO::new(file);

    let dek_id = page_cipher::compute_dek_id(&keys.mac_key, &keys.dek);
    let header = FileHeader::new(0xAAAA, dek_id);
    write_file_header(&io, &header).unwrap();

    let epoch = 1u32;

    // Write page 0 and page 1 with different data
    for i in 0..2u32 {
        let mut page = Page::new(PageId(i), PageType::Leaf, TxnId(1));
        let cell = format!("page-{i}-data");
        page.write_cell(cell.as_bytes()).unwrap();
        page.update_checksum();
        let offset = page_offset(PageId(i));
        ensure_file_size(&io, offset).unwrap();
        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(
            &keys.dek,
            &keys.mac_key,
            PageId(i),
            epoch,
            page.as_bytes(),
            &mut encrypted,
        );
        io.write_page(offset, &encrypted).unwrap();
    }
    io.fsync().unwrap();

    // Read both pages' raw bytes
    let offset0 = page_offset(PageId(0));
    let offset1 = page_offset(PageId(1));
    let mut raw0 = [0u8; PAGE_SIZE];
    let mut raw1 = [0u8; PAGE_SIZE];
    io.read_page(offset0, &mut raw0).unwrap();
    io.read_page(offset1, &mut raw1).unwrap();

    // Swap them on disk
    io.write_page(offset0, &raw1).unwrap();
    io.write_page(offset1, &raw0).unwrap();
    io.fsync().unwrap();

    // Try to read — HMAC verification should fail because:
    // Page at offset0 has MAC computed with page_id=1, but we pass page_id=0
    let mut pool = BufferPool::new(64);
    let result0 = pool.fetch(&io, PageId(0), &keys.dek, &keys.mac_key, epoch);
    assert!(
        matches!(result0, Err(Error::PageTampered(PageId(0)))),
        "page swap attack should be detected for page 0: {:?}",
        result0
    );

    let result1 = pool.fetch(&io, PageId(1), &keys.dek, &keys.mac_key, epoch);
    assert!(
        matches!(result1, Err(Error::PageTampered(PageId(1)))),
        "page swap attack should be detected for page 1: {:?}",
        result1
    );
}

#[test]
fn iv_uniqueness_across_many_writes() {
    // AES-CTR with the same IV and key produces the same keystream.
    // Verify that every encryption generates a unique random IV.
    let (_, keys) = create_key_file(
        b"pass",
        0xBBBB,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    page.update_checksum();

    let mut seen_ivs = std::collections::HashSet::new();
    let num_encryptions = 1000;

    for _ in 0..num_encryptions {
        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(
            &keys.dek,
            &keys.mac_key,
            PageId(0),
            1,
            page.as_bytes(),
            &mut encrypted,
        );
        let iv: [u8; 16] = encrypted[0..16].try_into().unwrap();
        let is_new = seen_ivs.insert(iv);
        assert!(
            is_new,
            "IV must be unique across all encryptions — duplicate detected!"
        );
    }

    assert_eq!(
        seen_ivs.len(),
        num_encryptions,
        "all {num_encryptions} IVs should be unique"
    );
}

#[test]
fn ctr_bit_flip_caught_before_decrypt() {
    // AES-CTR is a stream cipher: flipping bit N in ciphertext flips bit N
    // in plaintext. Without HMAC-before-decrypt, an attacker could control
    // plaintext changes. This test verifies the HMAC catches the tamper.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("bitflip.citadel");

    let (_, keys) = create_key_file(
        b"pass",
        0xCCCC,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&db_path)
        .unwrap();
    let io = SyncPageIO::new(file);

    let dek_id = page_cipher::compute_dek_id(&keys.mac_key, &keys.dek);
    let header = FileHeader::new(0xCCCC, dek_id);
    write_file_header(&io, &header).unwrap();

    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    page.write_cell(b"sensitive-data").unwrap();
    page.update_checksum();
    let offset = page_offset(PageId(0));
    ensure_file_size(&io, offset).unwrap();
    let mut encrypted = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        &keys.dek,
        &keys.mac_key,
        PageId(0),
        1,
        page.as_bytes(),
        &mut encrypted,
    );
    io.write_page(offset, &encrypted).unwrap();
    io.fsync().unwrap();

    // Flip various bits in the ciphertext region (bytes 16..PAGE_SIZE-32)
    // Each should be caught by HMAC — the MAC covers the ciphertext
    let ciphertext_offsets = [16, 100, 500, 4000, PAGE_SIZE - 33];
    for &flip_offset in &ciphertext_offsets {
        let mut tampered = [0u8; PAGE_SIZE];
        io.read_page(offset, &mut tampered).unwrap();
        tampered[flip_offset] ^= 0x01; // Single bit flip
        io.write_page(offset, &tampered).unwrap();

        let mut pool = BufferPool::new(64);
        let result = pool.fetch(&io, PageId(0), &keys.dek, &keys.mac_key, 1);
        assert!(
            matches!(result, Err(Error::PageTampered(PageId(0)))),
            "bit flip at offset {flip_offset} should be detected by HMAC"
        );

        // Restore original for next iteration
        io.write_page(offset, &encrypted).unwrap();
    }
}
