use parking_lot::Mutex;

use crate::ranges::{checked_batch_end, checked_range, checked_size};
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

impl MemoryPageIO {
    fn resize(data: &mut Vec<u8>, size: usize) -> Result<()> {
        if size > data.len() {
            data.try_reserve(size - data.len()).map_err(|error| {
                Error::Io(std::io::Error::new(std::io::ErrorKind::OutOfMemory, error))
            })?;
        }
        data.resize(size, 0);
        Ok(())
    }

    fn write_batch<'a>(
        &self,
        pages: impl Iterator<Item = (u64, &'a [u8; PAGE_SIZE])> + Clone,
    ) -> Result<()> {
        let end = checked_batch_end(pages.clone().map(|(offset, _)| offset))?;
        let mut data = self.data.lock();
        if end > data.len() {
            Self::resize(&mut data, end)?;
        }
        for (offset, page) in pages {
            // The complete immutable input was checked before growth/mutation.
            let start = offset as usize;
            data[start..start + PAGE_SIZE].copy_from_slice(page);
        }
        Ok(())
    }
}

impl PageIO for MemoryPageIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.read_at(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.write_at(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let range = checked_range(offset, buf.len())?;
        let data = self.data.lock();
        if range.end > data.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end of memory",
            )));
        }
        buf.copy_from_slice(&data[range]);
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        let range = checked_range(offset, buf.len())?;
        let mut data = self.data.lock();
        if range.end > data.len() {
            Self::resize(&mut data, range.end)?;
        }
        data[range].copy_from_slice(buf);
        Ok(())
    }

    fn write_pages(&self, pages: &[(u64, [u8; PAGE_SIZE])]) -> Result<()> {
        self.write_batch(pages.iter().map(|(offset, page)| (*offset, page)))
    }

    fn write_pages_ref(&self, pages: &[(u64, &[u8; PAGE_SIZE])]) -> Result<()> {
        self.write_batch(pages.iter().copied())
    }

    fn write_commit_meta(
        &self,
        god_offset: u64,
        god_byte: u8,
        slot_offset: u64,
        slot_buf: &[u8],
    ) -> Result<()> {
        let slot = checked_range(slot_offset, slot_buf.len())?;
        let god = checked_range(god_offset, 1)?;
        let end = slot.end.max(god.end);
        let mut data = self.data.lock();
        if end > data.len() {
            Self::resize(&mut data, end)?;
        }
        data[slot].copy_from_slice(slot_buf);
        data[god.start] = god_byte;
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
        let size = checked_size(size)?;
        Self::resize(&mut self.data.lock(), size)
    }
}

#[cfg(test)]
#[path = "memory_io_tests.rs"]
mod tests;
