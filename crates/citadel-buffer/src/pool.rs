use citadel_core::{
    BODY_SIZE, DEK_SIZE, MAC_KEY_SIZE, PAGE_SIZE,
};
use citadel_core::types::PageId;
use citadel_core::{Error, Result};

use citadel_crypto::page_cipher;
use citadel_io::file_manager::page_offset;
use citadel_io::traits::PageIO;
use citadel_page::page::Page;

use crate::sieve::SieveCache;

/// Buffer pool: caches decrypted pages in memory with SIEVE eviction.
///
/// Keyed by physical disk offset (not logical page_id) because under CoW/MVCC
/// the same logical page_id can exist at different disk locations.
///
/// Invariants:
/// - HMAC is verified BEFORE decryption on every page fetch (cache miss).
/// - Dirty pages are PINNED and never evictable until commit.
/// - Transaction size is bounded by buffer pool capacity.
pub struct BufferPool {
    cache: SieveCache<Page>,
    capacity: usize,
}

impl BufferPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: SieveCache::new(capacity),
            capacity,
        }
    }

    /// Fetch a page by page_id. Reads from cache or disk.
    ///
    /// On cache miss: reads from disk, verifies HMAC BEFORE decrypting,
    /// verifies xxHash64 checksum after decrypting.
    pub fn fetch(
        &mut self,
        io: &dyn PageIO,
        page_id: PageId,
        dek: &[u8; DEK_SIZE],
        mac_key: &[u8; MAC_KEY_SIZE],
        encryption_epoch: u32,
    ) -> Result<&Page> {
        let offset = page_offset(page_id);

        // Cache hit
        if self.cache.contains(offset) {
            return Ok(self.cache.get(offset).unwrap());
        }

        // Cache miss: read from disk
        let page = self.read_and_decrypt(io, page_id, offset, dek, mac_key, encryption_epoch)?;

        // Insert into cache (may evict)
        self.cache.insert(offset, page)
            .map_err(|()| Error::BufferPoolFull)?;

        Ok(self.cache.get(offset).unwrap())
    }

    /// Fetch a page mutably (for modification during write transaction).
    pub fn fetch_mut(
        &mut self,
        io: &dyn PageIO,
        page_id: PageId,
        dek: &[u8; DEK_SIZE],
        mac_key: &[u8; MAC_KEY_SIZE],
        encryption_epoch: u32,
    ) -> Result<&mut Page> {
        let offset = page_offset(page_id);

        if !self.cache.contains(offset) {
            let page = self.read_and_decrypt(io, page_id, offset, dek, mac_key, encryption_epoch)?;
            self.cache.insert(offset, page)
                .map_err(|()| Error::BufferPoolFull)?;
        }

        Ok(self.cache.get_mut(offset).unwrap())
    }

    /// Insert a new page directly into the buffer pool (for newly allocated pages).
    /// Marks it as dirty immediately.
    pub fn insert_new(&mut self, page_id: PageId, page: Page) -> Result<()> {
        let offset = page_offset(page_id);

        // Check if we'd exceed capacity with all dirty pages
        if self.cache.len() >= self.capacity && !self.cache.contains(offset) {
            // Try to insert (may evict a clean page)
            self.cache.insert(offset, page)
                .map_err(|()| Error::TransactionTooLarge { capacity: self.capacity })?;
        } else {
            self.cache.insert(offset, page)
                .map_err(|()| Error::BufferPoolFull)?;
        }

        self.cache.set_dirty(offset);
        Ok(())
    }

    /// Mark a page as dirty (modified in current write transaction).
    pub fn mark_dirty(&mut self, page_id: PageId) {
        let offset = page_offset(page_id);
        self.cache.set_dirty(offset);
    }

    /// Flush all dirty pages to disk: encrypt + compute MAC + write.
    /// Clears dirty flags after successful flush.
    pub fn flush_dirty(
        &mut self,
        io: &dyn PageIO,
        dek: &[u8; DEK_SIZE],
        mac_key: &[u8; MAC_KEY_SIZE],
        encryption_epoch: u32,
    ) -> Result<()> {
        // Collect dirty page offsets and data
        let dirty: Vec<(u64, PageId, [u8; BODY_SIZE])> = self.cache.dirty_entries()
            .map(|(offset, page)| {
                let page_id = page.page_id();
                let body = *page.as_bytes();
                (offset, page_id, body)
            })
            .collect();

        for (offset, page_id, body) in &dirty {
            let mut encrypted = [0u8; PAGE_SIZE];
            page_cipher::encrypt_page(dek, mac_key, *page_id, encryption_epoch, body, &mut encrypted);
            io.write_page(*offset, &encrypted)?;
        }

        self.cache.clear_all_dirty();
        Ok(())
    }

    /// Discard all dirty pages (on transaction abort).
    /// Removes dirty entries from the cache.
    pub fn discard_dirty(&mut self) {
        let dirty_offsets: Vec<u64> = self.cache.dirty_entries()
            .map(|(offset, _)| offset)
            .collect();

        for offset in dirty_offsets {
            self.cache.remove(offset);
        }
    }

    fn read_and_decrypt(
        &self,
        io: &dyn PageIO,
        page_id: PageId,
        offset: u64,
        dek: &[u8; DEK_SIZE],
        mac_key: &[u8; MAC_KEY_SIZE],
        encryption_epoch: u32,
    ) -> Result<Page> {
        let mut encrypted = [0u8; PAGE_SIZE];
        io.read_page(offset, &mut encrypted)?;

        // INVARIANT: Verify HMAC BEFORE decryption
        let mut body = [0u8; BODY_SIZE];
        page_cipher::decrypt_page(dek, mac_key, page_id, encryption_epoch, &encrypted, &mut body)?;

        let page = Page::from_bytes(body);

        // Defense-in-depth: verify xxHash64 checksum
        if !page.verify_checksum() {
            return Err(Error::ChecksumMismatch(page_id));
        }

        Ok(page)
    }

    /// Number of pages currently in the cache.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Number of dirty pages.
    pub fn dirty_count(&self) -> usize {
        self.cache.dirty_count()
    }

    /// Cache capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Check if a page is cached.
    pub fn is_cached(&self, page_id: PageId) -> bool {
        let offset = page_offset(page_id);
        self.cache.contains(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel_core::types::PageType;
    use citadel_core::types::TxnId;
    use citadel_crypto::hkdf_utils::derive_keys_from_rek;

    struct MockIO {
        pages: std::sync::Mutex<std::collections::HashMap<u64, [u8; PAGE_SIZE]>>,
    }

    impl MockIO {
        fn new() -> Self {
            Self { pages: std::sync::Mutex::new(std::collections::HashMap::new()) }
        }
    }

    impl PageIO for MockIO {
        fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
            let pages = self.pages.lock().unwrap();
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
            self.pages.lock().unwrap().insert(offset, *buf);
            Ok(())
        }

        fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> Result<()> { Ok(()) }
        fn write_at(&self, _offset: u64, _buf: &[u8]) -> Result<()> { Ok(()) }
        fn fsync(&self) -> Result<()> { Ok(()) }
        fn file_size(&self) -> Result<u64> { Ok(0) }
        fn truncate(&self, _size: u64) -> Result<()> { Ok(()) }
    }

    fn test_keys() -> ([u8; DEK_SIZE], [u8; MAC_KEY_SIZE]) {
        let rek = [0x42u8; 32];
        let keys = derive_keys_from_rek(&rek);
        (keys.dek, keys.mac_key)
    }

    fn write_encrypted_page(io: &MockIO, page: &Page, dek: &[u8; DEK_SIZE], mac_key: &[u8; MAC_KEY_SIZE], epoch: u32) {
        let page_id = page.page_id();
        let offset = page_offset(page_id);
        let mut encrypted = [0u8; PAGE_SIZE];
        page_cipher::encrypt_page(dek, mac_key, page_id, epoch, page.as_bytes(), &mut encrypted);
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

        // Remove from "disk" — should still be in cache
        io.pages.lock().unwrap().clear();
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

        // Tamper with encrypted data
        let offset = page_offset(PageId(0));
        {
            let mut pages = io.pages.lock().unwrap();
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

        // Insert 3 pages (all dirty from insert_new)
        for i in 0..3 {
            let mut page = Page::new(PageId(i), PageType::Leaf, TxnId(1));
            page.update_checksum();
            pool.insert_new(PageId(i), page).unwrap();
        }

        assert_eq!(pool.dirty_count(), 3);

        // Clear dirty on pages 0 and 2, making them evictable
        pool.cache.clear_dirty(page_offset(PageId(0)));
        pool.cache.clear_dirty(page_offset(PageId(2)));

        // Insert page 3 — should evict page 0 or 2 (not dirty page 1)
        let mut page3 = Page::new(PageId(3), PageType::Leaf, TxnId(1));
        page3.update_checksum();
        pool.insert_new(PageId(3), page3).unwrap();
        // Dirty page 1 must still be in the cache
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

        // Verify we can read it back from disk
        let offset = page_offset(PageId(5));
        assert!(io.pages.lock().unwrap().contains_key(&offset));
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
}
