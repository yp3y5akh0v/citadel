use super::*;
use citadel_core::types::PageType;
use citadel_core::types::TxnId;
use citadel_crypto::hkdf_utils::derive_keys_from_rek;

struct MockIO {
    pages: parking_lot::Mutex<rustc_hash::FxHashMap<u64, [u8; PAGE_SIZE]>>,
}

impl MockIO {
    fn new() -> Self {
        Self {
            pages: parking_lot::Mutex::new(rustc_hash::FxHashMap::default()),
        }
    }
}

impl PageIO for MockIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let pages = self.pages.lock();
        if let Some(data) = pages.get(&offset) {
            buf.copy_from_slice(data);
            Ok(())
        } else {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no page at offset {offset}"),
            )))
        }
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.pages.lock().insert(offset, *buf);
        Ok(())
    }

    fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> Result<()> {
        Ok(())
    }
    fn write_at(&self, _offset: u64, _buf: &[u8]) -> Result<()> {
        Ok(())
    }
    fn fsync(&self) -> Result<()> {
        Ok(())
    }
    fn file_size(&self) -> Result<u64> {
        Ok(0)
    }
    fn truncate(&self, _size: u64) -> Result<()> {
        Ok(())
    }
}

fn test_keys() -> ([u8; DEK_SIZE], [u8; MAC_KEY_SIZE]) {
    let rek = [0x42u8; 32];
    let keys = derive_keys_from_rek(&rek);
    (keys.dek, keys.mac_key)
}

fn write_encrypted_page(
    io: &MockIO,
    page: &Page,
    dek: &[u8; DEK_SIZE],
    mac_key: &[u8; MAC_KEY_SIZE],
    epoch: u32,
) {
    let page_id = page.page_id();
    let offset = page_offset(page_id);
    let mut encrypted = [0u8; PAGE_SIZE];
    page_cipher::encrypt_page(
        dek,
        mac_key,
        page_id,
        epoch,
        page.as_bytes(),
        &mut encrypted,
    );
    io.write_page(offset, &encrypted).unwrap();
}

#[test]
fn fetch_reads_and_caches() {
    let (dek, mac_key) = test_keys();
    let io = MockIO::new();
    let epoch = 1;

    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    page.update_checksum();
    write_encrypted_page(&io, &page, &dek, &mac_key, epoch);

    let mut pool = BufferPool::new(16);
    let fetched = pool.fetch(&io, PageId(0), &dek, &mac_key, epoch).unwrap();
    assert_eq!(fetched.page_id(), PageId(0));
    assert!(pool.is_cached(PageId(0)));
}

#[test]
fn fetch_from_cache_on_second_call() {
    let (dek, mac_key) = test_keys();
    let io = MockIO::new();
    let epoch = 1;

    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    page.update_checksum();
    write_encrypted_page(&io, &page, &dek, &mac_key, epoch);

    let mut pool = BufferPool::new(16);
    pool.fetch(&io, PageId(0), &dek, &mac_key, epoch).unwrap();

    io.pages.lock().clear();
    let fetched = pool.fetch(&io, PageId(0), &dek, &mac_key, epoch).unwrap();
    assert_eq!(fetched.page_id(), PageId(0));
}

#[test]
fn tampered_page_detected_on_fetch() {
    let (dek, mac_key) = test_keys();
    let io = MockIO::new();
    let epoch = 1;

    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    page.update_checksum();
    write_encrypted_page(&io, &page, &dek, &mac_key, epoch);

    let offset = page_offset(PageId(0));
    {
        let mut pages = io.pages.lock();
        let data = pages.get_mut(&offset).unwrap();
        data[100] ^= 0x01;
    }

    let mut pool = BufferPool::new(16);
    let result = pool.fetch(&io, PageId(0), &dek, &mac_key, epoch);
    assert!(matches!(result, Err(Error::PageTampered(_))));
}

#[test]
fn dirty_pages_survive_eviction() {
    let mut pool = BufferPool::new(3);

    for i in 0..3 {
        let mut page = Page::new(PageId(i), PageType::Leaf, TxnId(1));
        page.update_checksum();
        pool.insert_new(PageId(i), page).unwrap();
    }

    assert_eq!(pool.dirty_count(), 3);

    pool.cache.clear_dirty(page_offset(PageId(0)));
    pool.cache.clear_dirty(page_offset(PageId(2)));

    let mut page3 = Page::new(PageId(3), PageType::Leaf, TxnId(1));
    page3.update_checksum();
    pool.insert_new(PageId(3), page3).unwrap();
    assert!(pool.is_cached(PageId(1)));
}

#[test]
fn flush_dirty_writes_encrypted() {
    let (dek, mac_key) = test_keys();
    let io = MockIO::new();
    let epoch = 1;

    let mut pool = BufferPool::new(16);
    let mut page = Page::new(PageId(5), PageType::Leaf, TxnId(1));
    page.update_checksum();
    pool.insert_new(PageId(5), page).unwrap();

    assert_eq!(pool.dirty_count(), 1);

    pool.flush_dirty(&io, &dek, &mac_key, epoch).unwrap();
    assert_eq!(pool.dirty_count(), 0);

    let offset = page_offset(PageId(5));
    assert!(io.pages.lock().contains_key(&offset));
}

#[test]
fn discard_dirty_removes_from_cache() {
    let mut pool = BufferPool::new(16);
    let mut page = Page::new(PageId(1), PageType::Leaf, TxnId(1));
    page.update_checksum();
    pool.insert_new(PageId(1), page).unwrap();

    assert_eq!(pool.len(), 1);
    pool.discard_dirty();
    assert_eq!(pool.len(), 0);
}

#[test]
fn clear_releases_cached_pages_without_invalidating_readers() {
    let mut pool = BufferPool::new(2);
    let first = Arc::new(Page::new(PageId(1), PageType::Leaf, TxnId(1)));
    let second = Arc::new(Page::new(PageId(2), PageType::Leaf, TxnId(1)));
    let first_weak = Arc::downgrade(&first);
    let second_weak = Arc::downgrade(&second);
    pool.insert_if_absent(PageId(1), first);
    pool.insert_if_absent(PageId(2), second);
    let reader = pool.get_cached(PageId(2)).unwrap();

    pool.clear();
    assert!(pool.is_empty());
    assert!(first_weak.upgrade().is_none());
    assert_eq!(reader.page_id(), PageId(2));
    assert!(second_weak.upgrade().is_some());
    drop(reader);
    assert!(second_weak.upgrade().is_none());
}

#[test]
fn cached_hmac_loaders_preserve_auth_checksum_and_layout_boundaries() {
    let io = MockIO::new();
    let page_id = PageId(4);
    let offset = page_offset(page_id);
    for (dek, mac_key, epoch) in [
        ([0x12; DEK_SIZE], [0x34; MAC_KEY_SIZE], 0),
        ([0x56; DEK_SIZE], [0x78; MAC_KEY_SIZE], u32::MAX),
    ] {
        let state = page_cipher::HmacState::new(&mac_key, epoch);
        let mut page = Page::new(page_id, PageType::Leaf, TxnId(1));
        page.update_checksum();
        write_encrypted_page(&io, &page, &dek, &mac_key, epoch);
        for loaded in [
            read_and_decrypt(&io, page_id, offset, &dek, &mac_key, epoch),
            read_and_decrypt_with_hmac(&io, page_id, offset, &dek, &state),
            read_and_validate(&io, page_id, offset, &dek, &mac_key, epoch),
            read_and_validate_with_hmac(&io, page_id, offset, &dek, &state),
        ] {
            assert_eq!(loaded.unwrap().as_bytes(), page.as_bytes());
        }

        // A valid MAC does not replace the plaintext checksum check.
        page.as_bytes_mut()[BODY_SIZE - 1] ^= 1;
        write_encrypted_page(&io, &page, &dek, &mac_key, epoch);
        for loaded in [
            read_and_decrypt(&io, page_id, offset, &dek, &mac_key, epoch),
            read_and_decrypt_with_hmac(&io, page_id, offset, &dek, &state),
            read_and_validate(&io, page_id, offset, &dek, &mac_key, epoch),
            read_and_validate_with_hmac(&io, page_id, offset, &dek, &state),
        ] {
            assert!(matches!(loaded, Err(Error::ChecksumMismatch(id)) if id == page_id));
        }

        // Integrity callers must receive authenticated malformed layout bytes.
        page.set_num_cells(u16::MAX);
        page.update_checksum();
        write_encrypted_page(&io, &page, &dek, &mac_key, epoch);
        for loaded in [
            read_and_decrypt(&io, page_id, offset, &dek, &mac_key, epoch),
            read_and_decrypt_with_hmac(&io, page_id, offset, &dek, &state),
        ] {
            assert_eq!(loaded.unwrap().as_bytes(), page.as_bytes());
        }
        for loaded in [
            read_and_validate(&io, page_id, offset, &dek, &mac_key, epoch),
            read_and_validate_with_hmac(&io, page_id, offset, &dek, &state),
        ] {
            assert!(matches!(loaded, Err(Error::DatabaseCorrupted)));
        }

        // Authentication takes priority over that malformed plaintext layout.
        io.pages.lock().get_mut(&offset).unwrap()[100] ^= 1;
        for loaded in [
            read_and_decrypt(&io, page_id, offset, &dek, &mac_key, epoch),
            read_and_decrypt_with_hmac(&io, page_id, offset, &dek, &state),
            read_and_validate(&io, page_id, offset, &dek, &mac_key, epoch),
            read_and_validate_with_hmac(&io, page_id, offset, &dek, &state),
        ] {
            assert!(matches!(loaded, Err(Error::PageTampered(id)) if id == page_id));
        }
        let mut pool = BufferPool::new(2);
        assert!(matches!(pool.fetch(&io, page_id, &dek, &mac_key, epoch),
            Err(Error::PageTampered(id)) if id == page_id));
        assert!(!pool.is_cached(page_id));
        let mut repaired = Page::new(page_id, PageType::Leaf, TxnId(1));
        repaired.update_checksum();
        write_encrypted_page(&io, &repaired, &dek, &mac_key, epoch);
        assert_eq!(
            pool.fetch(&io, page_id, &dek, &mac_key, epoch)
                .unwrap()
                .as_bytes(),
            repaired.as_bytes()
        );
    }
}

#[test]
fn pool_cold_misses_use_each_supplied_key_and_epoch() {
    let io = MockIO::new();
    let contexts = [
        (PageId(3), [0x12; DEK_SIZE], [0x34; MAC_KEY_SIZE], 0),
        (PageId(7), [0x56; DEK_SIZE], [0x78; MAC_KEY_SIZE], u32::MAX),
    ];
    for (id, dek, mac_key, epoch) in &contexts {
        let mut page = Page::new(*id, PageType::Leaf, TxnId(1));
        page.update_checksum();
        write_encrypted_page(&io, &page, dek, mac_key, *epoch);
    }
    let mut pool = BufferPool::new(4);
    for (position, (id, dek, mac_key, epoch)) in contexts.iter().enumerate() {
        let other_key = &contexts[1 - position].2;
        assert!(matches!(pool.fetch(&io, *id, dek, other_key, *epoch),
            Err(Error::PageTampered(actual)) if actual == *id));
        assert!(!pool.is_cached(*id));
        assert!(
            matches!(pool.fetch_mut(&io, *id, dek, mac_key, epoch.wrapping_add(1)),
            Err(Error::PageTampered(actual)) if actual == *id)
        );
        assert!(!pool.is_cached(*id));
        if position == 0 {
            assert_eq!(
                pool.fetch(&io, *id, dek, mac_key, *epoch)
                    .unwrap()
                    .page_id(),
                *id
            );
        } else {
            assert_eq!(
                pool.fetch_mut(&io, *id, dek, mac_key, *epoch)
                    .unwrap()
                    .page_id(),
                *id
            );
        }
    }
    assert_eq!(pool.len(), 2);
}
