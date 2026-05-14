//! Pull-based table scan iterator backed by a B+ tree cursor.

use citadel_buffer::cursor::{Cursor, PageLoader};
use citadel_core::types::ValueType;
use citadel_core::Result;
use citadel_page::leaf_node::OverflowRef;

use crate::overflow_io;

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
                let kind = cursor.current_ref_lazy(pages).map(|c| c.val_type);
                match kind {
                    Some(ValueType::Tombstone) | None => {}
                    Some(ValueType::Inline) => {
                        let entry = cursor.current_ref_lazy(pages).unwrap();
                        key_buf.clear();
                        key_buf.extend_from_slice(entry.key);
                        value_buf.clear();
                        value_buf.extend_from_slice(entry.value);
                        emit = true;
                    }
                    Some(ValueType::Overflow) => {
                        let oref = {
                            let c = cursor.current_ref_lazy(pages).unwrap();
                            key_buf.clear();
                            key_buf.extend_from_slice(c.key);
                            OverflowRef::from_bytes(c.value)
                        };
                        let materialized = overflow_io::read_chain_value(pages, &oref)?;
                        value_buf.clear();
                        value_buf.extend_from_slice(&materialized);
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
#[path = "scan_iter_tests.rs"]
mod tests;
