use std::fs::File;
use std::io;
use std::os::unix::io::{IntoRawFd, RawFd};
use std::sync::Mutex;

use io_uring::{opcode, types, IoUring};

use crate::traits::PageIO;
use citadel_core::{Error, Result, PAGE_SIZE};

/// io_uring-backed page I/O for Linux.
///
/// Individual operations use submit-one-wait-one (no faster than sync I/O).
/// The performance win comes from `flush_pages()`, which batches all dirty
/// page writes + fsync into a single io_uring submission during commit.
pub struct UringPageIO {
    ring: Mutex<IoUring>,
    fd: RawFd,
}

impl UringPageIO {
    /// Try to create an io_uring-backed I/O instance.
    ///
    /// Returns `None` if io_uring is unavailable (old kernel, restricted container).
    /// The caller should fall back to `SyncPageIO`.
    pub fn try_new(file: File) -> Option<Self> {
        let fd = file.into_raw_fd();

        // No setup_defer_taskrun/setup_single_issuer: our Mutex allows
        // multiple threads to submit (serialized), but single_issuer
        // restricts to the creating thread's task_struct (EEXIST otherwise).
        let ring = IoUring::builder()
            .setup_coop_taskrun()
            .setup_clamp()
            .build(256)
            .or_else(|_| IoUring::builder().setup_clamp().build(256));

        match ring {
            Ok(ring) => Some(Self {
                ring: Mutex::new(ring),
                fd,
            }),
            Err(_) => {
                unsafe {
                    libc::close(fd);
                }
                None
            }
        }
    }

    fn drain_cqes(ring: &mut IoUring, expected: usize) -> Result<()> {
        let mut completed = 0;
        while completed < expected {
            let result = ring.completion().next().map(|cqe| cqe.result());
            if let Some(r) = result {
                if r < 0 {
                    while ring.completion().next().is_some() {}
                    return Err(Error::Io(io::Error::from_raw_os_error(-r)));
                }
                completed += 1;
            }
        }
        Ok(())
    }

    fn submit_one(&self, sqe: io_uring::squeue::Entry) -> Result<i32> {
        let mut ring = self.ring.lock().unwrap();

        unsafe {
            ring.submission().push(&sqe).map_err(|_| sq_full_err())?;
        }

        ring.submit_and_wait(1)?;

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| Error::Io(io::Error::new(io::ErrorKind::Other, "missing completion")))?;

        let result = cqe.result();
        if result < 0 {
            return Err(Error::Io(io::Error::from_raw_os_error(-result)));
        }
        Ok(result)
    }
}

impl Drop for UringPageIO {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

impl PageIO for UringPageIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let sqe = opcode::Read::new(types::Fd(self.fd), buf.as_mut_ptr(), PAGE_SIZE as u32)
            .offset(offset)
            .build();
        let n = self.submit_one(sqe)? as usize;
        if n < PAGE_SIZE {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short read",
            )));
        }
        Ok(())
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        let sqe = opcode::Write::new(types::Fd(self.fd), buf.as_ptr(), PAGE_SIZE as u32)
            .offset(offset)
            .build();
        let n = self.submit_one(sqe)? as usize;
        if n < PAGE_SIZE {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "short write",
            )));
        }
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let sqe = opcode::Read::new(types::Fd(self.fd), buf.as_mut_ptr(), buf.len() as u32)
            .offset(offset)
            .build();
        let n = self.submit_one(sqe)? as usize;
        if n < buf.len() {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short read",
            )));
        }
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        let sqe = opcode::Write::new(types::Fd(self.fd), buf.as_ptr(), buf.len() as u32)
            .offset(offset)
            .build();
        let n = self.submit_one(sqe)? as usize;
        if n < buf.len() {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "short write",
            )));
        }
        Ok(())
    }

    fn fsync(&self) -> Result<()> {
        let sqe = opcode::Fsync::new(types::Fd(self.fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build();
        self.submit_one(sqe)?;
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        if unsafe { libc::fstat(self.fd, &mut stat) } < 0 {
            return Err(Error::Io(io::Error::last_os_error()));
        }
        Ok(stat.st_size as u64)
    }

    fn truncate(&self, size: u64) -> Result<()> {
        if unsafe { libc::ftruncate(self.fd, size as libc::off_t) } < 0 {
            return Err(Error::Io(io::Error::last_os_error()));
        }
        Ok(())
    }

    fn write_pages(&self, pages: &[(u64, [u8; PAGE_SIZE])]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }

        let max_end = pages
            .iter()
            .map(|(offset, _)| offset + PAGE_SIZE as u64)
            .max()
            .unwrap();
        if max_end > self.file_size()? {
            self.truncate(max_end)?;
        }

        let mut ring = self.ring.lock().unwrap();
        let sq_cap = ring.submission().capacity();
        let batch_size = sq_cap.saturating_sub(1).max(1);

        for chunk in pages.chunks(batch_size) {
            for (i, (offset, buf)) in chunk.iter().enumerate() {
                let sqe = opcode::Write::new(types::Fd(self.fd), buf.as_ptr(), PAGE_SIZE as u32)
                    .offset(*offset)
                    .build()
                    .user_data(i as u64);

                unsafe {
                    ring.submission().push(&sqe).map_err(|_| sq_full_err())?;
                }
            }

            ring.submit_and_wait(chunk.len())?;
            Self::drain_cqes(&mut ring, chunk.len())?;
        }

        Ok(())
    }

    fn flush_pages(&self, pages: &[(u64, [u8; PAGE_SIZE])]) -> Result<()> {
        self.write_pages(pages)?;
        self.fsync()
    }
}

fn sq_full_err() -> Error {
    Error::Io(io::Error::new(
        io::ErrorKind::Other,
        "submission queue full",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_file() -> (tempfile::TempDir, File) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        (dir, file)
    }

    #[test]
    fn try_new_succeeds() {
        let (_dir, file) = create_test_file();
        let io = UringPageIO::try_new(file);
        assert!(io.is_some(), "io_uring should be available on this kernel");
    }

    #[test]
    fn read_write_page_roundtrip() {
        let (_dir, file) = create_test_file();
        let io = UringPageIO::try_new(file).unwrap();

        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0xAA;
        page[PAGE_SIZE - 1] = 0xBB;

        io.truncate(PAGE_SIZE as u64).unwrap();
        io.write_page(0, &page).unwrap();

        let mut read_buf = [0u8; PAGE_SIZE];
        io.read_page(0, &mut read_buf).unwrap();
        assert_eq!(read_buf, page);
    }

    #[test]
    fn read_write_at() {
        let (_dir, file) = create_test_file();
        let io = UringPageIO::try_new(file).unwrap();

        let header = [0x42u8; 512];
        io.truncate(512).unwrap();
        io.write_at(0, &header).unwrap();

        let mut read_buf = [0u8; 512];
        io.read_at(0, &mut read_buf).unwrap();
        assert_eq!(read_buf, header);
    }

    #[test]
    fn file_size_and_truncate() {
        let (_dir, file) = create_test_file();
        let io = UringPageIO::try_new(file).unwrap();

        assert_eq!(io.file_size().unwrap(), 0);
        io.truncate(PAGE_SIZE as u64).unwrap();
        assert_eq!(io.file_size().unwrap(), PAGE_SIZE as u64);
    }

    #[test]
    fn flush_pages_batch() {
        let (_dir, file) = create_test_file();
        let io = UringPageIO::try_new(file).unwrap();

        let mut pages = Vec::new();
        for i in 0..10u8 {
            let offset = i as u64 * PAGE_SIZE as u64;
            let mut page = [0u8; PAGE_SIZE];
            page[0] = i;
            page[PAGE_SIZE - 1] = 0xFF - i;
            pages.push((offset, page));
        }

        io.flush_pages(&pages).unwrap();

        for (offset, expected) in &pages {
            let mut buf = [0u8; PAGE_SIZE];
            io.read_page(*offset, &mut buf).unwrap();
            assert_eq!(&buf[..], &expected[..]);
        }
    }

    #[test]
    fn flush_pages_empty() {
        let (_dir, file) = create_test_file();
        let io = UringPageIO::try_new(file).unwrap();
        io.flush_pages(&[]).unwrap();
    }

    #[test]
    fn fsync_works() {
        let (_dir, file) = create_test_file();
        let io = UringPageIO::try_new(file).unwrap();
        io.truncate(PAGE_SIZE as u64).unwrap();

        let page = [0xAB; PAGE_SIZE];
        io.write_page(0, &page).unwrap();
        io.fsync().unwrap();
    }
}
