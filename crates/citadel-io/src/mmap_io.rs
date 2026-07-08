use std::fs::File;
use std::io;

use memmap2::{MmapMut, MmapOptions};
use parking_lot::{Mutex, RwLock};

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
        if current < initial {
            file.set_len(initial)?;
        }
        let mmap = unsafe { MmapOptions::new().len(initial as usize).map_mut(&file)? };
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
            .and_then(|()| unsafe { MmapOptions::new().len(new_size as usize).map_mut(file) });
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
                        if let Ok(mmap) =
                            unsafe { MmapOptions::new().len(len as usize).map_mut(file) }
                        {
                            inner.mmap = mmap;
                            inner.size = len;
                        }
                    }
                }
                Err(e.into())
            }
        }
    }
}

impl PageIO for MmapPageIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let inner = self.inner.read();
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > inner.size as usize {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end of mapping",
            )));
        }
        buf.copy_from_slice(&inner.mmap[start..end]);
        Ok(())
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        let end = offset + PAGE_SIZE as u64;
        self.ensure_mapped(end)?;
        let mut inner = self.inner.write();
        inner.mmap[offset as usize..end as usize].copy_from_slice(buf);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let inner = self.inner.read();
        let start = offset as usize;
        let end = start + buf.len();
        if end > inner.size as usize {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end of mapping",
            )));
        }
        buf.copy_from_slice(&inner.mmap[start..end]);
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        let end = offset + buf.len() as u64;
        self.ensure_mapped(end)?;
        let mut inner = self.inner.write();
        inner.mmap[offset as usize..end as usize].copy_from_slice(buf);
        Ok(())
    }

    fn write_pages(&self, pages: &[(u64, [u8; PAGE_SIZE])]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        let max_end = pages
            .iter()
            .map(|(o, _)| o + PAGE_SIZE as u64)
            .max()
            .unwrap();
        self.ensure_mapped(max_end)?;
        let mut inner = self.inner.write();
        for (offset, buf) in pages {
            let start = *offset as usize;
            inner.mmap[start..start + PAGE_SIZE].copy_from_slice(buf);
        }
        Ok(())
    }

    fn write_pages_ref(&self, pages: &[(u64, &[u8; PAGE_SIZE])]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        let max_end = pages
            .iter()
            .map(|(o, _)| o + PAGE_SIZE as u64)
            .max()
            .unwrap();
        self.ensure_mapped(max_end)?;
        let mut inner = self.inner.write();
        for &(offset, buf) in pages {
            let start = offset as usize;
            inner.mmap[start..start + PAGE_SIZE].copy_from_slice(buf);
        }
        Ok(())
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
        let max_end = (god_offset + 1).max(slot_offset + slot_buf.len() as u64);
        self.ensure_mapped(max_end)?;
        let mut inner = self.inner.write();
        // Slot before god byte: a crash between the two must leave the god
        // byte selecting the previous commit, not a stale two-generations-old
        // slot.
        let slot_start = slot_offset as usize;
        let slot_end = slot_start + slot_buf.len();
        inner.mmap[slot_start..slot_end].copy_from_slice(slot_buf);
        inner.mmap[god_offset as usize] = god_byte;
        Ok(())
    }
}

#[cfg(test)]
#[path = "mmap_io_tests.rs"]
mod tests;
