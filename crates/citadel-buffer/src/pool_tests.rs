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
