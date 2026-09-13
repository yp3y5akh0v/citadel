use std::sync::Arc;

use citadel_core::types::PageId;
use citadel_core::{Error, Result};
use citadel_core::{BODY_SIZE, DEK_SIZE, MAC_KEY_SIZE, PAGE_SIZE};

use citadel_crypto::page_cipher;
use citadel_io::file_manager::page_offset;
use citadel_io::traits::PageIO;
use citadel_page::page::Page;

use crate::sieve::SieveCache;

/// Authenticate and decrypt raw bytes without interpreting the page layout.
/// Integrity tools use this boundary to retain precise corruption diagnostics.
pub fn read_and_decrypt(
    io: &dyn PageIO,
    page_id: PageId,
    offset: u64,
    dek: &[u8; DEK_SIZE],
    mac_key: &[u8; MAC_KEY_SIZE],
    encryption_epoch: u32,
) -> Result<Page> {
    read_with_decrypt::<false>(io, page_id, offset, |encrypted, body| {
        page_cipher::decrypt_page(dek, mac_key, page_id, encryption_epoch, encrypted, body)
    })
}

/// Authenticate, decrypt and check the checksum using an exact key/epoch state.
/// Page layout is left uninterpreted for integrity and maintenance callers.
pub fn read_and_decrypt_with_hmac(
    io: &dyn PageIO,
    page_id: PageId,
    offset: u64,
    dek: &[u8; DEK_SIZE],
    hmac_state: &page_cipher::HmacState,
) -> Result<Page> {
    read_with_decrypt::<false>(io, page_id, offset, |encrypted, body| {
        page_cipher::decrypt_page_with_hmac(dek, hmac_state, page_id, encrypted, body)
    })
}

#[inline]
fn read_with_decrypt<const VALIDATE: bool>(
    io: &dyn PageIO,
    page_id: PageId,
    offset: u64,
    decrypt: impl FnOnce(&[u8; PAGE_SIZE], &mut [u8; BODY_SIZE]) -> Result<()>,
) -> Result<Page> {
    let mut encrypted = [0u8; PAGE_SIZE];
    io.read_page(offset, &mut encrypted)?;

    // Decrypt into the page that checksum and layout validation will inspect.
    // Keeping a separate body would move a full page before those checks.
    let mut page = Page::default();
    decrypt(&encrypted, page.as_bytes_mut())?;

    if !page.verify_checksum() {
        return Err(Error::ChecksumMismatch(page_id));
    }

    // Validate the decrypted body before returning the large owned page. Both
    // load modes share authentication/checksum ordering without materializing
    // another intermediate Result<Page> around the raw loader.
    if VALIDATE {
        page.validate_for_read(page_id)?;
    }

    Ok(page)
}

/// Load a source page for normal reads, validating its identity and structural
/// layout once before it can enter a trusted cache or a writer's local page map.
pub fn read_and_validate(
    io: &dyn PageIO,
    page_id: PageId,
    offset: u64,
    dek: &[u8; DEK_SIZE],
    mac_key: &[u8; MAC_KEY_SIZE],
    encryption_epoch: u32,
) -> Result<Page> {
    read_with_decrypt::<true>(io, page_id, offset, |encrypted, body| {
        page_cipher::decrypt_page(dek, mac_key, page_id, encryption_epoch, encrypted, body)
    })
}

/// Load a normal source page using an exact key/epoch HMAC state, then validate
/// identity and layout after authentication, decryption and checksum verification.
pub fn read_and_validate_with_hmac(
    io: &dyn PageIO,
    page_id: PageId,
    offset: u64,
    dek: &[u8; DEK_SIZE],
    hmac_state: &page_cipher::HmacState,
) -> Result<Page> {
    read_with_decrypt::<true>(io, page_id, offset, |encrypted, body| {
        page_cipher::decrypt_page_with_hmac(dek, hmac_state, page_id, encrypted, body)
    })
}

/// Buffer pool: caches decrypted pages in memory with SIEVE eviction.
///
/// Entries are keyed by physical disk offsets derived from `PageId`.
///
/// Invariants:
/// - HMAC is verified BEFORE decryption on every page fetch (cache miss).
/// - Dirty entries are excluded from eviction until their dirty mark is cleared.
/// - Capacity bounds resident cache entries. Transaction writers keep changed
///   pages separately, so this cache does not bound transaction size.
pub struct BufferPool {
    cache: SieveCache<Arc<Page>>,
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
    /// verifies xxHash64 checksum and page layout after decrypting.
    pub fn fetch(
        &mut self,
        io: &dyn PageIO,
        page_id: PageId,
        dek: &[u8; DEK_SIZE],
        mac_key: &[u8; MAC_KEY_SIZE],
        encryption_epoch: u32,
    ) -> Result<&Page> {
        let offset = page_offset(page_id);

        if self.cache.contains(offset) {
            return Ok(self.cache.get(offset).unwrap());
        }

        let page = read_and_validate(io, page_id, offset, dek, mac_key, encryption_epoch)?;
        self.cache
            .insert(offset, Arc::new(page))
            .map_err(|()| Error::BufferPoolFull)?;

        Ok(self.cache.get(offset).unwrap())
    }

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
            let page = read_and_validate(io, page_id, offset, dek, mac_key, encryption_epoch)?;
            self.cache
                .insert(offset, Arc::new(page))
                .map_err(|()| Error::BufferPoolFull)?;
        }

        Ok(Arc::make_mut(self.cache.get_mut(offset).unwrap()))
    }

    /// Insert a newly allocated page. Marks it dirty immediately.
    pub fn insert_new(&mut self, page_id: PageId, page: Page) -> Result<()> {
        let offset = page_offset(page_id);

        if self.cache.len() >= self.capacity && !self.cache.contains(offset) {
            self.cache
                .insert(offset, Arc::new(page))
                .map_err(|()| Error::TransactionTooLarge {
                    capacity: self.capacity,
                })?;
        } else {
            self.cache
                .insert(offset, Arc::new(page))
                .map_err(|()| Error::BufferPoolFull)?;
        }

        self.cache.set_dirty(offset);
        Ok(())
    }

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
        let dirty: Vec<(u64, PageId, [u8; BODY_SIZE])> = self
            .cache
            .dirty_entries()
            .map(|(offset, arc)| {
                let page_id = arc.page_id();
                let body = *arc.as_bytes();
                (offset, page_id, body)
            })
            .collect();

        for (offset, page_id, body) in &dirty {
            let mut encrypted = [0u8; PAGE_SIZE];
            page_cipher::encrypt_page(
                dek,
                mac_key,
                *page_id,
                encryption_epoch,
                body,
                &mut encrypted,
            );
            io.write_page(*offset, &encrypted)?;
        }

        self.cache.clear_all_dirty();
        Ok(())
    }

    /// Discard all dirty pages (on transaction abort).
    /// Removes dirty entries from the cache.
    pub fn discard_dirty(&mut self) {
        let dirty_offsets: Vec<u64> = self
            .cache
            .dirty_entries()
            .map(|(offset, _)| offset)
            .collect();

        for offset in dirty_offsets {
            self.cache.remove(offset);
        }
    }

    pub fn get_cached(&mut self, page_id: PageId) -> Option<Arc<Page>> {
        let offset = page_offset(page_id);
        self.cache.get(offset).map(Arc::clone)
    }

    pub fn insert_if_absent(&mut self, page_id: PageId, page: Arc<Page>) {
        let offset = page_offset(page_id);
        if !self.cache.contains(offset) {
            let _ = self.cache.insert(offset, page);
        }
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn dirty_count(&self) -> usize {
        self.cache.dirty_count()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn is_cached(&self, page_id: PageId) -> bool {
        let offset = page_offset(page_id);
        self.cache.contains(offset)
    }

    pub fn invalidate(&mut self, page_id: PageId) {
        let offset = page_offset(page_id);
        self.cache.remove(offset);
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

#[cfg(test)]
#[path = "pool_tests.rs"]
mod tests;
