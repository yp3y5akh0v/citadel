use citadel_core::{PAGE_SIZE, Result};

/// Trait for page-level I/O operations.
///
/// All operations work with on-disk page format (8208 bytes = IV + ciphertext + MAC).
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
}
