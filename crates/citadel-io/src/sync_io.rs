use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom};
use std::sync::Mutex;

use citadel_core::{PAGE_SIZE, Result};
use crate::traits::PageIO;

/// Synchronous page I/O using standard file operations.
///
/// Uses a Mutex around the File handle to allow shared access
/// (multiple readers can call read_page concurrently from different threads).
pub struct SyncPageIO {
    file: Mutex<File>,
}

impl SyncPageIO {
    pub fn new(file: File) -> Self {
        Self {
            file: Mutex::new(file),
        }
    }

    pub fn into_file(self) -> File {
        self.file.into_inner().unwrap()
    }
}

impl PageIO for SyncPageIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(buf)?;
        Ok(())
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf)?;
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(buf)?;
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf)?;
        Ok(())
    }

    fn fsync(&self) -> Result<()> {
        let file = self.file.lock().unwrap();
        // sync_data() maps to fdatasync() on Linux, F_FULLFSYNC on macOS,
        // FlushFileBuffers on Windows. Skips unnecessary metadata (timestamps)
        // while still flushing file size changes on Linux (POSIX requirement).
        file.sync_data()?;
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        let file = self.file.lock().unwrap();
        Ok(file.metadata()?.len())
    }

    fn truncate(&self, size: u64) -> Result<()> {
        let file = self.file.lock().unwrap();
        file.set_len(size)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_write_page_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let file = File::options().read(true).write(true).create(true).open(&path).unwrap();
        let io = SyncPageIO::new(file);

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
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let file = File::options().read(true).write(true).create(true).open(&path).unwrap();
        let io = SyncPageIO::new(file);

        let header = [0x42u8; 512];
        io.write_at(0, &header).unwrap();

        let mut read_buf = [0u8; 512];
        io.read_at(0, &mut read_buf).unwrap();
        assert_eq!(read_buf, header);
    }

    #[test]
    fn file_size_and_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let file = File::options().read(true).write(true).create(true).open(&path).unwrap();
        let io = SyncPageIO::new(file);

        assert_eq!(io.file_size().unwrap(), 0);
        io.truncate(8208).unwrap();
        assert_eq!(io.file_size().unwrap(), 8208);
    }
}
