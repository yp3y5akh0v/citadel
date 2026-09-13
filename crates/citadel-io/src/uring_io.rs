use parking_lot::Mutex;
use std::fs::File;
use std::io;
use std::os::unix::io::{IntoRawFd, RawFd};

use io_uring::{opcode, types, IoUring};

use crate::traits::PageIO;
use citadel_core::{Error, Result, PAGE_SIZE};

/// io_uring-backed page I/O for Linux. Dirty pages are written in bounded
/// batches; `flush_pages()` completes them before submitting fsync.
///
/// Ordinary I/O failures are returned only after every published operation has
/// completed. If an unrecoverable ring-driver failure prevents proving that
/// borrowed buffers are no longer in use, the process aborts rather than return
/// or unwind and expose those buffers to a use-after-free.
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

    /// Caller holds the ring lock, including across any preceding size check.
    /// Kept separate from PageIO::truncate to avoid recursive mutex acquisition.
    fn truncate_file(&self, length: libc::off_t) -> Result<()> {
        if unsafe { libc::ftruncate(self.fd, length) } < 0 {
            return Err(Error::Io(io::Error::last_os_error()));
        }
        Ok(())
    }

    fn submit_one(&self, sqe: io_uring::squeue::Entry) -> Result<i32> {
        let mut ring = self.ring.lock();
        require_idle_ring(&mut ring);
        let mut pending = PendingRequests::new(None);
        {
            let mut queue = ring.submission();
            unsafe {
                queue.push(&sqe.user_data(0)).map_err(|_| sq_full_err())?;
            }
            // Track the borrowed operation before the queue guard publishes it.
            pending.published += 1;
        }
        complete_pending(&mut *ring, pending)
    }

    fn write_batch<'a>(
        &self,
        pages: impl Iterator<Item = (u64, &'a [u8; PAGE_SIZE])> + Clone,
    ) -> Result<()> {
        let max_end = checked_page_batch_end(pages.clone().map(|(offset, _)| offset))?;
        let mut ring = self.ring.lock();
        require_idle_ring(&mut ring);
        // Sizing and writes form one serialized operation. A smaller batch
        // must not truncate away a larger batch using a stale size observation.
        if max_end > self.file_size()? {
            self.truncate_file(checked_file_length(max_end)?)?;
        }
        let capacity = ring.submission().capacity().min(MAX_PENDING);
        let batch_size = capacity.saturating_sub(1).max(1);
        let mut pages = pages.peekable();
        while pages.peek().is_some() {
            let mut pending = PendingRequests::new(Some(PAGE_SIZE));
            {
                let mut queue = ring.submission();
                for (offset, buf) in pages.by_ref().take(batch_size) {
                    let sqe =
                        opcode::Write::new(types::Fd(self.fd), buf.as_ptr(), PAGE_SIZE as u32)
                            .offset(offset)
                            .build()
                            .user_data(pending.published as u64);
                    if unsafe { queue.push(&sqe) }.is_err() {
                        pending.record_error(sq_full_err());
                        break;
                    }
                    pending.published += 1;
                }
            }
            complete_pending(&mut *ring, pending)?;
        }
        Ok(())
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
        let _ring = self.ring.lock();
        self.truncate_file(length)
    }

    fn write_pages(&self, pages: &[(u64, [u8; PAGE_SIZE])]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        self.write_batch(pages.iter().map(|(offset, page)| (*offset, page)))
    }

    fn write_pages_ref(&self, pages: &[(u64, &[u8; PAGE_SIZE])]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        self.write_batch(pages.iter().copied())
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

fn require_idle_ring(ring: &mut IoUring) {
    if !ring.submission().is_empty() || ring.completion().next().is_some() {
        abort_pending_io();
    }
}

const MAX_PENDING: usize = 256;

/// The guard covers queued as well as submitted operations. Closing an io_uring
/// is not a synchronous cancellation fence, so unwinding with pending borrowed
/// buffers is unsound. Normal CQE errors are retained until all requests retire.
struct PendingRequests {
    published: usize,
    completed: usize,
    seen: [bool; MAX_PENDING],
    expected_write_len: Option<usize>,
    first_result: i32,
    error: Option<Error>,
}

impl PendingRequests {
    fn new(expected_write_len: Option<usize>) -> Self {
        Self {
            published: 0,
            completed: 0,
            seen: [false; MAX_PENDING],
            expected_write_len,
            first_result: 0,
            error: None,
        }
    }

    fn record_error(&mut self, error: Error) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    fn record_completion(&mut self, id: u64, result: i32) {
        let Ok(index) = usize::try_from(id) else {
            abort_pending_io();
        };
        if index >= self.published || index >= MAX_PENDING || self.seen[index] {
            abort_pending_io();
        }
        self.seen[index] = true;
        self.completed += 1;
        if index == 0 {
            self.first_result = result;
        }
        if result < 0 {
            let Some(code) = result.checked_neg() else {
                abort_pending_io();
            };
            self.record_error(Error::Io(io::Error::from_raw_os_error(code)));
        } else if self
            .expected_write_len
            .is_some_and(|length| (result as usize) < length)
        {
            self.record_error(Error::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "short write",
            )));
        }
    }

    fn is_complete(&self) -> bool {
        self.completed == self.published
    }
}

impl Drop for PendingRequests {
    fn drop(&mut self) {
        if !self.is_complete() {
            abort_pending_io();
        }
    }
}

#[cold]
fn abort_pending_io() -> ! {
    // Do not panic: unwinding could release buffers still referenced by SQEs.
    std::process::abort()
}

/// A small adapter allows deterministic tests to drive the exact production
/// retirement loop, including partial submission and delayed error completions.
trait CompletionDriver {
    fn next_completion(&mut self) -> Option<(u64, i32)>;
    fn submit_and_wait_one(&mut self) -> io::Result<usize>;
}

impl CompletionDriver for IoUring {
    fn next_completion(&mut self) -> Option<(u64, i32)> {
        self.completion()
            .next()
            .map(|cqe| (cqe.user_data(), cqe.result()))
    }

    fn submit_and_wait_one(&mut self) -> io::Result<usize> {
        self.submit_and_wait(1)
    }
}

fn complete_pending(
    driver: &mut impl CompletionDriver,
    mut pending: PendingRequests,
) -> Result<i32> {
    loop {
        while let Some((id, result)) = driver.next_completion() {
            pending.record_completion(id, result);
        }
        if pending.is_complete() {
            return match pending.error.take() {
                Some(error) => Err(error),
                None => Ok(pending.first_result),
            };
        }
        // A failed SQE may stop submission before the rest of the SQ is
        // consumed. Waiting for the whole batch can then deadlock; waiting for
        // one CQE resubmits remaining SQEs and advances each terminal result.
        if let Err(error) = driver.submit_and_wait_one() {
            match error.raw_os_error() {
                // io_uring_enter(2): interrupted wait; transient resource
                // shortage; CQ overflow/backpressure. Drain before retrying.
                Some(libc::EINTR | libc::EAGAIN | libc::EBUSY) => {
                    std::thread::yield_now();
                }
                _ => {
                    pending.record_error(error.into());
                    // An enter error can race final completions. Returning is
                    // safe only if every published identity is now retired.
                    while let Some((id, result)) = driver.next_completion() {
                        pending.record_completion(id, result);
                    }
                    if !pending.is_complete() {
                        abort_pending_io();
                    }
                }
            }
        }
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
