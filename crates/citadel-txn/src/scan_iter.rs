//! Pull-based table scan iterator backed by a B+ tree cursor.

use citadel_buffer::cursor::{Cursor, PageLoader};
use citadel_core::types::ValueType;
use citadel_core::Result;

/// Lending iterator over `(key, value)` byte pairs for a table scan.
///
/// Created by [`crate::ReadTxn::table_scan_iter`] or [`crate::WriteTxn::table_scan_iter`].
/// The adapter `T` carries whatever txn borrow or ownership is required.
pub struct TableIter<T: TxnScanAdapter> {
    inner: T,
    cursor: Cursor,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
}

/// Internal hook letting `TableIter` talk to either a `ReadTxn` or a `WriteTxn`
/// without knowing which.
pub trait TxnScanAdapter {
    fn with_loader<R>(&mut self, f: &mut dyn FnMut(&mut dyn PageLoader) -> Result<R>) -> Result<R>;
}

impl<T: TxnScanAdapter> TableIter<T> {
    pub(crate) fn new(inner: T, cursor: Cursor) -> Self {
        Self {
            inner,
            cursor,
            key_buf: Vec::new(),
            value_buf: Vec::new(),
        }
    }

    /// Advance to the next non-tombstone entry; returns `None` when exhausted.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        let key_buf = &mut self.key_buf;
        let value_buf = &mut self.value_buf;
        let cursor = &mut self.cursor;
        loop {
            if !cursor.is_valid() {
                return Ok(None);
            }
            let found = self.inner.with_loader(&mut |pages| {
                let mut emit = false;
                if let Some(entry) = cursor.current_ref_lazy(pages) {
                    if entry.val_type != ValueType::Tombstone {
                        key_buf.clear();
                        key_buf.extend_from_slice(entry.key);
                        value_buf.clear();
                        value_buf.extend_from_slice(entry.value);
                        emit = true;
                    }
                }
                cursor.next_lazy(pages)?;
                Ok(emit)
            })?;
            if found {
                return Ok(Some((&self.key_buf, &self.value_buf)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::manager::tests::create_test_manager;

    #[test]
    fn table_scan_iter_walks_all_entries() {
        let mgr = create_test_manager();
        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.create_table(b"t").unwrap();
            for i in 0..10u32 {
                wtx.table_insert(b"t", &i.to_be_bytes(), &[i as u8])
                    .unwrap();
            }
            wtx.commit().unwrap();
        }
        let mut rtx = mgr.begin_read();
        let mut iter = rtx.table_scan_iter(b"t", b"").unwrap();
        let mut collected: Vec<(u32, u8)> = Vec::new();
        while let Some((k, v)) = iter.next().unwrap() {
            let mut kbuf = [0u8; 4];
            kbuf.copy_from_slice(k);
            collected.push((u32::from_be_bytes(kbuf), v[0]));
        }
        assert_eq!(collected.len(), 10);
        for (i, (k, v)) in collected.iter().enumerate() {
            assert_eq!(*k as usize, i);
            assert_eq!(*v as usize, i);
        }
    }

    #[test]
    fn table_scan_iter_start_key() {
        let mgr = create_test_manager();
        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.create_table(b"t").unwrap();
            for i in 0..10u32 {
                wtx.table_insert(b"t", &i.to_be_bytes(), &[i as u8])
                    .unwrap();
            }
            wtx.commit().unwrap();
        }
        let mut rtx = mgr.begin_read();
        let mut iter = rtx.table_scan_iter(b"t", &5u32.to_be_bytes()).unwrap();
        let mut count = 0;
        let mut first_key: Option<u32> = None;
        while let Some((k, _)) = iter.next().unwrap() {
            if first_key.is_none() {
                let mut kbuf = [0u8; 4];
                kbuf.copy_from_slice(k);
                first_key = Some(u32::from_be_bytes(kbuf));
            }
            count += 1;
        }
        assert_eq!(count, 5);
        assert_eq!(first_key, Some(5));
    }

    #[test]
    fn table_scan_iter_empty() {
        let mgr = create_test_manager();
        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.create_table(b"t").unwrap();
            wtx.commit().unwrap();
        }
        let mut rtx = mgr.begin_read();
        let mut iter = rtx.table_scan_iter(b"t", b"").unwrap();
        assert!(iter.next().unwrap().is_none());
    }

    #[test]
    fn table_scan_iter_skips_tombstones() {
        let mgr = create_test_manager();
        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.create_table(b"t").unwrap();
            for i in 0..5u32 {
                wtx.table_insert(b"t", &i.to_be_bytes(), &[i as u8])
                    .unwrap();
            }
            wtx.commit().unwrap();
        }
        {
            let mut wtx = mgr.begin_write().unwrap();
            wtx.table_delete(b"t", &2u32.to_be_bytes()).unwrap();
            wtx.commit().unwrap();
        }
        let mut rtx = mgr.begin_read();
        let mut iter = rtx.table_scan_iter(b"t", b"").unwrap();
        let mut keys: Vec<u32> = Vec::new();
        while let Some((k, _)) = iter.next().unwrap() {
            let mut kbuf = [0u8; 4];
            kbuf.copy_from_slice(k);
            keys.push(u32::from_be_bytes(kbuf));
        }
        assert_eq!(keys, vec![0, 1, 3, 4]);
    }

    #[test]
    fn table_scan_iter_write_txn() {
        let mgr = create_test_manager();
        let mut wtx = mgr.begin_write().unwrap();
        wtx.create_table(b"t").unwrap();
        for i in 0..4u32 {
            wtx.table_insert(b"t", &i.to_be_bytes(), b"v").unwrap();
        }
        let mut iter = wtx.table_scan_iter(b"t", b"").unwrap();
        let mut count = 0;
        while iter.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 4);
    }
}
