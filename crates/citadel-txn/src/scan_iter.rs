//! Pull-based table scan iterator backed by a B+ tree cursor.

use citadel_buffer::cursor::{Cursor, PageLoader};
use citadel_core::types::ValueType;
use citadel_core::Result;
use citadel_page::leaf_node::OverflowRef;

use crate::overflow_io;
use crate::ReadBudget;

/// Lending iterator over `(key, value)` byte pairs for a table scan.
///
/// Created by [`crate::ReadTxn::table_scan_iter`] or [`crate::WriteTxn::table_scan_iter`].
/// The adapter `T` carries the txn borrow or ownership.
///
/// Operation-local scan measurements are captured when the iterator is
/// constructed and receive its batched count when the iterator is dropped.
/// A measurement that needs the complete count must therefore enclose both
/// iterator construction and its lifetime; one iterator is not split between
/// spans that start or end midway through it.
pub struct TableIter<T: TxnScanAdapter> {
    inner: T,
    /// The token cannot change while the iterator owns or borrows its adapter,
    /// so capture it once rather than cloning the `Arc` for every row.
    cancel: Option<citadel_core::CancelToken>,
    budget: Option<ReadBudget>,
    cursor: Cursor,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
    /// Rows this iterator produced, reported on drop. A pull-based scan has no
    /// end of its own, so drop is the only point every use reaches.
    rows: u64,
    /// Once EOF has been reported, later cancellation must not rewrite an
    /// already-completed operation into an interruption.
    finished: bool,
}

impl<T: TxnScanAdapter> Drop for TableIter<T> {
    fn drop(&mut self) {
        self.inner.record_rows_scanned(self.rows);
    }
}

/// Internal hook letting `TableIter` talk to either a `ReadTxn` or a `WriteTxn`
/// without knowing which.
pub trait TxnScanAdapter {
    fn with_loader<R>(&mut self, f: &mut dyn FnMut(&mut dyn PageLoader) -> Result<R>) -> Result<R>;

    /// A pull-based scan has no loop of its own, so the check lives here or
    /// `table_scan_iter` and `into_table_scan_iter` stay uncancellable.
    fn cancel(&self) -> Option<&citadel_core::CancelToken> {
        None
    }

    fn read_budget(&self) -> Option<&ReadBudget> {
        None
    }

    fn record_rows_scanned(&self, _rows: u64) {}
}

impl<T: TxnScanAdapter> TableIter<T> {
    pub(crate) fn new(inner: T, cursor: Cursor) -> Self {
        let cancel = inner.cancel().cloned();
        let budget = inner.read_budget().cloned();
        Self {
            inner,
            cancel,
            budget,
            cursor,
            key_buf: Vec::new(),
            value_buf: Vec::new(),
            rows: 0,
            finished: false,
        }
    }

    /// Advance to the next non-tombstone entry; returns `None` when exhausted.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        let key_buf = &mut self.key_buf;
        let value_buf = &mut self.value_buf;
        let cursor = &mut self.cursor;
        let rows = &mut self.rows;
        let finished = &mut self.finished;
        if *finished {
            return Ok(None);
        }
        loop {
            if let Some(t) = &self.cancel {
                t.check()?;
            }
            if !cursor.is_valid() {
                *finished = true;
                return Ok(None);
            }
            let cancel = self.cancel.as_ref();
            *rows += 1;
            let found = self.inner.with_loader(&mut |pages| {
                let mut emit = false;
                match cursor.current_ref_lazy(pages)? {
                    None => {}
                    Some(entry) => match entry.val_type {
                        ValueType::Tombstone => {}
                        ValueType::Inline => {
                            if let Some(budget) = &self.budget {
                                budget.try_charge(entry.value.len())?;
                            }
                            key_buf.clear();
                            key_buf.extend_from_slice(entry.key);
                            value_buf.clear();
                            value_buf.extend_from_slice(entry.value);
                            emit = true;
                        }
                        ValueType::Overflow => {
                            let oref = OverflowRef::from_bytes(entry.value);
                            key_buf.clear();
                            key_buf.extend_from_slice(entry.key);
                            let materialized = overflow_io::read_chain_value_with_budget(
                                pages,
                                &oref,
                                cancel,
                                self.budget.as_ref(),
                            )?;
                            *value_buf = materialized;
                            emit = true;
                        }
                    },
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
