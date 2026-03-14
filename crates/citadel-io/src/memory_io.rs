use std::sync::Mutex;

use citadel_core::{Error, PAGE_SIZE, Result};
use crate::traits::PageIO;

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
        let data = self.data.lock().unwrap();
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
        let mut data = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > data.len() {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(buf);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let data = self.data.lock().unwrap();
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
        let mut data = self.data.lock().unwrap();
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
        let data = self.data.lock().unwrap();
        Ok(data.len() as u64)
    }

    fn truncate(&self, size: u64) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data.resize(size as usize, 0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_write_page_roundtrip() {
        let io = MemoryPageIO::new();

        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0xAA;
        page[PAGE_SIZE - 1] = 0xBB;

        io.write_page(0, &page).unwrap();

        let mut read_buf = [0u8; PAGE_SIZE];
        io.read_page(0, &mut read_buf).unwrap();
        assert_eq!(read_buf, page);
    }

    #[test]
    fn read_write_at() {
        let io = MemoryPageIO::new();

        let header = [0x42u8; 512];
        io.write_at(0, &header).unwrap();

        let mut read_buf = [0u8; 512];
        io.read_at(0, &mut read_buf).unwrap();
        assert_eq!(read_buf, header);
    }

    #[test]
    fn file_size_and_truncate() {
        let io = MemoryPageIO::new();

        assert_eq!(io.file_size().unwrap(), 0);
        io.truncate(8208).unwrap();
        assert_eq!(io.file_size().unwrap(), 8208);

        io.truncate(100).unwrap();
        assert_eq!(io.file_size().unwrap(), 100);
    }

    #[test]
    fn read_past_end_fails() {
        let io = MemoryPageIO::new();

        let mut buf = [0u8; PAGE_SIZE];
        let result = io.read_page(0, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn write_auto_extends() {
        let io = MemoryPageIO::new();
        assert_eq!(io.file_size().unwrap(), 0);

        let page = [0xCCu8; PAGE_SIZE];
        io.write_page(512, &page).unwrap();
        assert_eq!(io.file_size().unwrap(), 512 + PAGE_SIZE as u64);
    }

    #[test]
    fn multiple_pages() {
        let io = MemoryPageIO::new();

        let mut page0 = [0u8; PAGE_SIZE];
        let mut page1 = [0u8; PAGE_SIZE];
        page0[0] = 0x01;
        page1[0] = 0x02;

        io.write_page(0, &page0).unwrap();
        io.write_page(PAGE_SIZE as u64, &page1).unwrap();

        let mut read0 = [0u8; PAGE_SIZE];
        let mut read1 = [0u8; PAGE_SIZE];
        io.read_page(0, &mut read0).unwrap();
        io.read_page(PAGE_SIZE as u64, &mut read1).unwrap();

        assert_eq!(read0[0], 0x01);
        assert_eq!(read1[0], 0x02);
    }

    #[test]
    fn fsync_is_noop() {
        let io = MemoryPageIO::new();
        io.fsync().unwrap();
    }

    #[test]
    fn flush_pages_batch() {
        let io = MemoryPageIO::new();

        let mut page0 = [0u8; PAGE_SIZE];
        let mut page1 = [0u8; PAGE_SIZE];
        page0[0] = 0xAA;
        page1[0] = 0xBB;

        io.truncate(2 * PAGE_SIZE as u64).unwrap();
        io.flush_pages(&[
            (0, page0),
            (PAGE_SIZE as u64, page1),
        ]).unwrap();

        let mut read0 = [0u8; PAGE_SIZE];
        let mut read1 = [0u8; PAGE_SIZE];
        io.read_page(0, &mut read0).unwrap();
        io.read_page(PAGE_SIZE as u64, &mut read1).unwrap();

        assert_eq!(read0[0], 0xAA);
        assert_eq!(read1[0], 0xBB);
    }

    #[test]
    fn overwrite_existing_data() {
        let io = MemoryPageIO::new();

        let page_v1 = [0x11u8; PAGE_SIZE];
        io.write_page(0, &page_v1).unwrap();

        let page_v2 = [0x22u8; PAGE_SIZE];
        io.write_page(0, &page_v2).unwrap();

        let mut read = [0u8; PAGE_SIZE];
        io.read_page(0, &mut read).unwrap();
        assert_eq!(read, page_v2);
    }

    #[test]
    fn default_trait() {
        let io = MemoryPageIO::default();
        assert_eq!(io.file_size().unwrap(), 0);
    }
}
