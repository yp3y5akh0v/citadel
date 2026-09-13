use std::fs::File;
use std::io;

use memmap2::{MmapMut, MmapOptions};
use parking_lot::{Mutex, RwLock};

use crate::ranges::{checked_batch_end, checked_range, checked_size};
use crate::traits::PageIO;
use citadel_core::{Error, Result, PAGE_SIZE};

/// mmap(0) fails on Windows; pre-size to avoid it.
const INITIAL_MAPPING_SIZE: u64 = 1 << 20;

/// Memory-mapped page I/O.
pub struct MmapPageIO {
    file: Mutex<File>,
    inner: RwLock<MmapInner>,
}

struct MmapInner {
    mmap: MmapMut,
    size: u64,
}

impl MmapPageIO {
    pub fn try_new(file: File) -> Result<Self> {
        let current = file.metadata()?.len();
        let initial = current.max(INITIAL_MAPPING_SIZE);
        let mapping_len = checked_size(initial)?;
        if current < initial {
            file.set_len(initial)?;
        }
        let mmap = unsafe { MmapOptions::new().len(mapping_len).map_mut(&file)? };
        Ok(Self {
            file: Mutex::new(file),
            inner: RwLock::new(MmapInner {
                mmap,
                size: initial,
            }),
        })
    }

    fn ensure_mapped(&self, needed: u64) -> Result<()> {
        if self.inner.read().size >= needed {
            return Ok(());
        }
        let file = self.file.lock();
        let mut inner = self.inner.write();
        if inner.size >= needed {
            return Ok(());
        }
        // Never below the real file length: after a failed remap the tracked
        // size can lag it, and set_len() under it would destroy data.
        let target = needed.max(file.metadata()?.len());
        Self::remap_locked(&file, &mut inner, target)
    }

    fn remap_to(&self, new_size: u64) -> Result<()> {
        let file = self.file.lock();
        let mut inner = self.inner.write();
        Self::remap_locked(&file, &mut inner, new_size)
    }

    fn remap_locked(file: &File, inner: &mut MmapInner, new_size: u64) -> Result<()> {
        let mapping_len = checked_size(new_size)?;
        if inner.size == new_size {
            return Ok(());
        }
        let _ = inner.mmap.flush_async();
        // Windows forbids set_len() while mapped - drop old mmap first.
        let dummy = MmapOptions::new().len(1).map_anon()?;
        let old = std::mem::replace(&mut inner.mmap, dummy);
        drop(old);
        let mapped = file
            .set_len(new_size)
            .and_then(|()| unsafe { MmapOptions::new().len(mapping_len).map_mut(file) });
        match mapped {
            Ok(mmap) => {
                inner.mmap = mmap;
                inner.size = new_size;
                Ok(())
            }
            Err(e) => {
                // Old mapping is gone; remap at the real length. If that too
                // fails, size = 0 makes later accesses fail bounds checks
                // rather than index the dummy map.
                inner.size = 0;
                if let Ok(len) = file.metadata().map(|m| m.len()) {
                    if len > 0 {
                        if let Ok(mapping_len) = checked_size(len) {
                            if let Ok(mmap) =
                                unsafe { MmapOptions::new().len(mapping_len).map_mut(file) }
                            {
                                inner.mmap = mmap;
                                inner.size = len;
                            }
                        }
                    }
                }
                Err(e.into())
            }
        }
    }
}

impl MmapPageIO {
    /// Check capacity under the same lock as the copy. A concurrent truncate
    /// may shrink the mapping after ensure_mapped returns, so retry growth
    /// without holding the mapping lock across the file -> mapping lock order.
    fn with_mapping_mut<T>(
        &self,
        needed: usize,
        write: impl FnOnce(&mut MmapMut) -> T,
    ) -> Result<T> {
        loop {
            let mut inner = self.inner.write();
            if needed as u64 <= inner.size {
                return Ok(write(&mut inner.mmap));
            }
            drop(inner);
            self.ensure_mapped(needed as u64)?;
        }
    }

    fn write_batch<'a>(
        &self,
        pages: impl Iterator<Item = (u64, &'a [u8; PAGE_SIZE])> + Clone,
    ) -> Result<()> {
        let end = checked_batch_end(pages.clone().map(|(offset, _)| offset))?;
        self.with_mapping_mut(end, |mapping| {
            for (offset, page) in pages {
                // The complete immutable input was checked before remapping.
                let start = offset as usize;
                mapping[start..start + PAGE_SIZE].copy_from_slice(page);
            }
        })
    }
}

impl PageIO for MmapPageIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.read_at(offset, buf)
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.write_at(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let range = checked_range(offset, buf.len())?;
        let inner = self.inner.read();
        if range.end as u64 > inner.size {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end of mapping",
            )));
        }
        buf.copy_from_slice(&inner.mmap[range]);
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        let range = checked_range(offset, buf.len())?;
        self.with_mapping_mut(range.end, |mapping| mapping[range].copy_from_slice(buf))
    }

    fn write_pages(&self, pages: &[(u64, [u8; PAGE_SIZE])]) -> Result<()> {
        self.write_batch(pages.iter().map(|(offset, page)| (*offset, page)))
    }

    fn write_pages_ref(&self, pages: &[(u64, &[u8; PAGE_SIZE])]) -> Result<()> {
        self.write_batch(pages.iter().copied())
    }

    fn fsync(&self) -> Result<()> {
        let inner = self.inner.read();
        inner.mmap.flush()?;
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        // Real file length, not the tracked mapping size: after a failed
        // remap the mapping can lag the file, and a sizing decision made from
        // the smaller value could set_len() below live data.
        let file = self.file.lock();
        Ok(file.metadata()?.len())
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.remap_to(size)
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
        self.with_mapping_mut(slot.end.max(god.end), |mapping| {
            // Slot before god byte, including overlapping caller ranges.
            mapping[slot].copy_from_slice(slot_buf);
            mapping[god.start] = god_byte;
        })
    }
}

#[cfg(test)]
#[path = "mmap_io_tests.rs"]
mod tests;
