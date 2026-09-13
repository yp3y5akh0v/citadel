use parking_lot::Mutex;
use std::fs::File;
use std::io;
use std::os::unix::io::{IntoRawFd, RawFd};

use io_uring::{opcode, types, IoUring};

use crate::traits::PageIO;
use citadel_core::{Error, Result, PAGE_SIZE};

/// io_uring-backed page I/O for Linux. `flush_pages()` batches all dirty-page
/// writes + fsync into a single submission.
pub struct UringPageIO {
    ring: Mutex<IoUring>,
    fd: RawFd,
}

impl UringPageIO {
    /// Try to create an io_uring-backed I/O instance.
    ///
    /// `None` if io_uring is unavailable (old kernel, restricted container).
    /// The caller should fall back to `MmapPageIO`.
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

    /// Drain `expected` completions, requiring each to have transferred
    /// exactly `expected_len` bytes - a short (torn) write must fail the
    /// batch just like the single-op paths reject `n < len`.
    fn drain_cqes(ring: &mut IoUring, expected: usize, expected_len: usize) -> Result<()> {
        let mut completed = 0;
        while completed < expected {
            let result = ring.completion().next().map(|cqe| cqe.result());
            if let Some(r) = result {
                if r < 0 {
                    while ring.completion().next().is_some() {}
                    return Err(Error::Io(io::Error::from_raw_os_error(-r)));
                }
                if (r as usize) < expected_len {
                    while ring.completion().next().is_some() {}
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "short write",
                    )));
                }
                completed += 1;
            }
        }
        Ok(())
    }

    fn submit_one(&self, sqe: io_uring::squeue::Entry) -> Result<i32> {
        let mut ring = self.ring.lock();

        unsafe {
            ring.submission().push(&sqe).map_err(|_| sq_full_err())?;
        }

        ring.submit_and_wait(1)?;

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| Error::Io(io::Error::other("missing completion")))?;

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
        self.read_at(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.write_at(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let length = checked_io_length(offset, buf.len())?;
        let sqe = opcode::Read::new(types::Fd(self.fd), buf.as_mut_ptr(), length)
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
        let length = checked_io_length(offset, buf.len())?;
        let sqe = opcode::Write::new(types::Fd(self.fd), buf.as_ptr(), length)
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
        let length = checked_file_length(size)?;
        if unsafe { libc::ftruncate(self.fd, length) } < 0 {
            return Err(Error::Io(io::Error::last_os_error()));
        }
        Ok(())
    }

    fn write_pages(&self, pages: &[(u64, [u8; PAGE_SIZE])]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }

        let max_end = checked_page_batch_end(pages.iter().map(|(offset, _)| *offset))?;
        if max_end > self.file_size()? {
            self.truncate(max_end)?;
        }

        let mut ring = self.ring.lock();
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
            Self::drain_cqes(&mut ring, chunk.len(), PAGE_SIZE)?;
        }

        Ok(())
    }

    fn write_pages_ref(&self, pages: &[(u64, &[u8; PAGE_SIZE])]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }

        let max_end = checked_page_batch_end(pages.iter().map(|(offset, _)| *offset))?;
        if max_end > self.file_size()? {
            self.truncate(max_end)?;
        }

        let mut ring = self.ring.lock();
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
            Self::drain_cqes(&mut ring, chunk.len(), PAGE_SIZE)?;
        }

        Ok(())
    }

    fn write_commit_meta(
        &self,
        god_offset: u64,
        god_byte: u8,
        slot_offset: u64,
        slot_buf: &[u8],
    ) -> Result<()> {
        // Admit the entire operation before its first write, retaining the
        // required slot-before-god-byte publication order.
        checked_io_length(slot_offset, slot_buf.len())?;
        checked_io_length(god_offset, 1)?;
        self.write_at(slot_offset, slot_buf)?;
        self.write_at(god_offset, &[god_byte])
    }

    fn flush_pages(&self, pages: &[(u64, [u8; PAGE_SIZE])]) -> Result<()> {
        self.write_pages(pages)?;
        self.fsync()
    }
}

fn invalid_range() -> Error {
    Error::Io(io::Error::new(
        io::ErrorKind::InvalidInput,
        "I/O range exceeds the kernel offset or transfer-length limit",
    ))
}

/// io_uring offsets are signed kernel file positions. In particular, all-one
/// bits request the shared file cursor; PageIO always requires an exact offset.
fn checked_offset_end(offset: u64, length: usize) -> Result<u64> {
    let length = u64::try_from(length).map_err(|_| invalid_range())?;
    let end = offset.checked_add(length).ok_or_else(invalid_range)?;
    if end > i64::MAX as u64 {
        return Err(invalid_range());
    }
    Ok(end)
}

fn checked_io_length(offset: u64, length: usize) -> Result<u32> {
    checked_offset_end(offset, length)?;
    u32::try_from(length).map_err(|_| invalid_range())
}

fn checked_file_length(length: u64) -> Result<libc::off_t> {
    libc::off_t::try_from(length).map_err(|_| invalid_range())
}

fn checked_page_batch_end(mut offsets: impl Iterator<Item = u64>) -> Result<u64> {
    offsets.try_fold(0, |end, offset| {
        checked_offset_end(offset, PAGE_SIZE).map(|page_end| end.max(page_end))
    })
}

fn sq_full_err() -> Error {
    Error::Io(io::Error::other("submission queue full"))
}

#[cfg(test)]
#[path = "uring_io_tests.rs"]
mod tests;
