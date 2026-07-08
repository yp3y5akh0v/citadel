use citadel_core::{Result, PAGE_SIZE};

/// Trait for page-level I/O operations.
///
/// Operations use the on-disk page format (8208B = IV + ciphertext + MAC).
/// The offset parameter is the byte offset in the file.
pub trait PageIO: Send + Sync {
    /// Read a page from disk at the given byte offset.
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()>;

    /// Write a page to disk at the given byte offset.
    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()>;

    /// Read arbitrary bytes from the file (used for file header).
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()>;

    /// Write arbitrary bytes to the file (used for file header).
    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()>;

    /// Flush all pending writes to durable storage.
    fn fsync(&self) -> Result<()>;

    /// Get the current file size in bytes.
    fn file_size(&self) -> Result<u64>;

    /// Extend the file to the given size.
    fn truncate(&self, size: u64) -> Result<()>;

    fn write_pages(&self, pages: &[(u64, [u8; PAGE_SIZE])]) -> Result<()> {
        for (offset, buf) in pages {
            self.write_page(*offset, buf)?;
        }
        Ok(())
    }

    fn write_pages_ref(&self, pages: &[(u64, &[u8; PAGE_SIZE])]) -> Result<()> {
        for &(offset, buf) in pages {
            self.write_page(offset, buf)?;
        }
        Ok(())
    }

    fn flush_pages(&self, pages: &[(u64, [u8; PAGE_SIZE])]) -> Result<()> {
        self.write_pages(pages)?;
        self.fsync()
    }

    /// Publish a commit's metadata (SyncMode::Off path). The slot bytes must
    /// land before the god byte: a crash between the two then leaves the god
    /// byte selecting the previous commit, not a stale two-generations-old
    /// slot.
    fn write_commit_meta(
        &self,
        god_offset: u64,
        god_byte: u8,
        slot_offset: u64,
        slot_buf: &[u8],
    ) -> Result<()> {
        self.write_at(slot_offset, slot_buf)?;
        self.write_at(god_offset, &[god_byte])
    }
}

#[cfg(test)]
#[path = "traits_tests.rs"]
mod tests;
