use super::*;
use std::sync::Mutex;

/// Records the (offset, len) of every write_at call.
struct RecordingIO {
    writes: Mutex<Vec<(u64, usize)>>,
}

impl PageIO for RecordingIO {
    fn read_page(&self, _offset: u64, _buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        Ok(())
    }

    fn write_page(&self, offset: u64, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        self.writes.lock().unwrap().push((offset, buf.len()));
        Ok(())
    }

    fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> Result<()> {
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        self.writes.lock().unwrap().push((offset, buf.len()));
        Ok(())
    }

    fn fsync(&self) -> Result<()> {
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        Ok(0)
    }

    fn truncate(&self, _size: u64) -> Result<()> {
        Ok(())
    }
}

/// A crash between the two commit-meta writes must leave the god byte
/// selecting the previous commit, so the slot bytes have to land first.
#[test]
fn write_commit_meta_writes_slot_before_god_byte() {
    let io = RecordingIO {
        writes: Mutex::new(Vec::new()),
    };
    io.write_commit_meta(20, 0x01, 100, &[0xAB; 64]).unwrap();
    let writes = io.writes.lock().unwrap();
    assert_eq!(writes.as_slice(), &[(100u64, 64usize), (20u64, 1usize)]);
}
