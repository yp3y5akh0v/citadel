use parking_lot::Mutex;

use crate::traits::PageIO;
use citadel_core::{Error, Result, PAGE_SIZE};

/// In-memory page I/O backend.
///
/// Stores all data in a growable byte vector protected by a Mutex.
/// Useful for testing, in-memory databases, and WASM environments
/// where filesystem access is unavailable.
///
/// `fsync` is a no-op since there is no durable storage.
pub struct MemoryPageIO {
    data: Mutex<Vec<u8>>,
}

impl MemoryPageIO {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(Vec::new()),
        }
    }
}

impl Default for MemoryPageIO {
    fn default() -> Self {
        Self::new()
    }
}

impl PageIO for MemoryPageIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let data = self.data.lock();
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > data.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end of memory",
            )));
        }
        buf.copy_from_slice(&data[start..end]);
        Ok(())
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        let mut data = self.data.lock();
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > data.len() {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(buf);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let data = self.data.lock();
        let start = offset as usize;
        let end = start + buf.len();
        if end > data.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end of memory",
            )));
        }
        buf.copy_from_slice(&data[start..end]);
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        let mut data = self.data.lock();
        let start = offset as usize;
        let end = start + buf.len();
        if end > data.len() {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(buf);
        Ok(())
    }

    fn fsync(&self) -> Result<()> {
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        let data = self.data.lock();
        Ok(data.len() as u64)
    }

    fn truncate(&self, size: u64) -> Result<()> {
        let mut data = self.data.lock();
        data.resize(size as usize, 0);
        Ok(())
    }
}

#[cfg(test)]
#[path = "memory_io_tests.rs"]
mod tests;
