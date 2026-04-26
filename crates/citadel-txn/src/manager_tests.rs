use super::*;
use citadel_crypto::hkdf_utils::derive_keys_from_rek;
use citadel_crypto::page_cipher::compute_dek_id;
use std::sync::Mutex as StdMutex;

pub struct MemIO {
    data: StdMutex<Vec<u8>>,
}

impl MemIO {
    pub fn new(size: usize) -> Self {
        Self {
            data: StdMutex::new(vec![0u8; size]),
        }
    }
}

impl PageIO for MemIO {
    fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let data = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > data.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end",
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
            let available = data.len().saturating_sub(start);
            if available > 0 {
                buf[..available].copy_from_slice(&data[start..start + available]);
            }
            buf[available..].fill(0);
            return Ok(());
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
        Ok(self.data.lock().unwrap().len() as u64)
    }

    fn truncate(&self, size: u64) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data.resize(size as usize, 0);
        Ok(())
    }
}

pub fn test_keys() -> ([u8; DEK_SIZE], [u8; MAC_KEY_SIZE], [u8; 32]) {
    let rek = [0x42u8; 32];
    let keys = derive_keys_from_rek(&rek);
    let dek_id = compute_dek_id(&keys.mac_key, &keys.dek);
    (keys.dek, keys.mac_key, dek_id)
}

pub fn create_test_manager() -> TxnManager {
    let (dek, mac_key, dek_id) = test_keys();
    let io = Box::new(MemIO::new(1024 * 1024));
    TxnManager::create(io, dek, mac_key, 1, 0x1234, dek_id, 256).unwrap()
}

#[test]
fn create_and_open() {
    let (dek, mac_key, dek_id) = test_keys();
    let io = Box::new(MemIO::new(1024 * 1024));

    let mgr = TxnManager::create(io, dek, mac_key, 1, 0x1234, dek_id, 256).unwrap();
    let slot = mgr.current_slot();
    assert_eq!(slot.txn_id, TxnId(1));
    assert_eq!(slot.tree_root, PageId(0));
    assert_eq!(slot.tree_depth, 1);
    assert_eq!(slot.tree_entries, 0);
    assert_eq!(slot.high_water_mark, 1);
}

#[test]
fn begin_read_registers_reader() {
    let mgr = create_test_manager();
    assert_eq!(mgr.reader_count(), 0);

    let _rtx = mgr.begin_read();
    assert_eq!(mgr.reader_count(), 1);
}

#[test]
fn drop_read_unregisters_reader() {
    let mgr = create_test_manager();
    {
        let _rtx = mgr.begin_read();
        assert_eq!(mgr.reader_count(), 1);
    }
    assert_eq!(mgr.reader_count(), 0);
}

#[test]
fn multiple_concurrent_readers() {
    let mgr = create_test_manager();
    let _r1 = mgr.begin_read();
    let _r2 = mgr.begin_read();
    let _r3 = mgr.begin_read();
    assert_eq!(mgr.reader_count(), 3);
}

#[test]
fn single_writer_enforcement() {
    let mgr = create_test_manager();
    let _wtx = mgr.begin_write().unwrap();
    let result = mgr.begin_write();
    assert!(matches!(result, Err(Error::WriteTransactionActive)));
}

#[test]
fn writer_released_after_drop() {
    let mgr = create_test_manager();
    {
        let _wtx = mgr.begin_write().unwrap();
    }
    let _wtx2 = mgr.begin_write().unwrap();
}

#[test]
fn oldest_active_reader_with_no_readers() {
    let mgr = create_test_manager();
    let oldest = mgr.oldest_active_reader();
    assert!(oldest.as_u64() >= 2); // create used txn 1
}

#[test]
fn oldest_active_reader_tracks_minimum() {
    let mgr = create_test_manager();
    let r1 = mgr.begin_read(); // Gets some txn_id
    let _r2 = mgr.begin_read(); // Gets higher txn_id
    let oldest = mgr.oldest_active_reader();
    assert_eq!(oldest, r1.txn_id());
}
