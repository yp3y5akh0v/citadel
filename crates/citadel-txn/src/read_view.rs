//! Data-only access shared by committed readers and frozen writer snapshots.

use citadel_core::{CancelToken, PageId, Result, TxnId};
use citadel_page::page::Page;
use std::sync::Arc;

use super::{LeafPages, LeafShardScanner, ReadTxn, ReadTxnScanAdapter};
use crate::ReadBudget;

/// An immutable view of a writer's data at one statement boundary.
///
/// The private reader owns a committed-page pin and immutable page references.
/// It can outlive its originating writer, but is never a committed transaction:
/// no commit slot, transaction identity, or Merkle proof is exposed.
pub struct StatementReadTxn<'db> {
    reader: ReadTxn<'db>,
}

impl<'db> StatementReadTxn<'db> {
    pub(super) fn new(reader: ReadTxn<'db>) -> Self {
        Self { reader }
    }

    pub fn view(&mut self) -> ReadView<'_, 'db> {
        ReadView {
            reader: &mut self.reader,
        }
    }
}

/// Borrowed data access without an authenticated-commit identity.
///
/// Committed and statement readers use the same scan machinery. Shared caches
/// must use [`Self::cache_generation`]: a pending view returns `None`, and must
/// neither consume nor publish entries keyed by a committed generation.
pub struct ReadView<'tx, 'db> {
    reader: &'tx mut ReadTxn<'db>,
}

impl<'db> ReadTxn<'db> {
    pub fn view(&mut self) -> ReadView<'_, 'db> {
        ReadView { reader: self }
    }
}

impl<'db> ReadView<'_, 'db> {
    pub fn reborrow(&mut self) -> ReadView<'_, 'db> {
        ReadView {
            reader: self.reader,
        }
    }

    /// `Some` only for a real committed snapshot; never a synthetic epoch.
    pub fn cache_generation(&self) -> Option<u64> {
        self.reader
            .pending
            .is_none()
            .then_some(self.reader.commit_generation)
    }

    pub fn cancel_token(&self) -> Option<&CancelToken> {
        self.reader.cancel_token()
    }

    pub fn set_cancel(&mut self, token: Option<CancelToken>) {
        self.reader.set_cancel(token);
    }

    pub fn read_budget(&self) -> Option<&ReadBudget> {
        self.reader.read_budget()
    }

    pub fn set_read_budget(&mut self, budget: Option<ReadBudget>) {
        self.reader.set_read_budget(budget);
    }

    pub fn rows_scanned(&self) -> u64 {
        self.reader.rows_scanned()
    }

    pub fn measure_scans(&mut self) -> crate::manager::ScanMeasurement {
        self.reader.measure_scans()
    }

    pub fn entry_count(&self) -> u64 {
        self.reader.entry_count()
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.reader.get(key)
    }

    pub fn table_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.reader.table_get(table, key)
    }

    pub fn table_entry_count(&mut self, table: &[u8]) -> Result<u64> {
        self.reader.table_entry_count(table)
    }

    /// A local root stamp, not a committed-cache identity. Pending snapshots
    /// with the same stamp can contain different versions of a private page.
    pub fn table_root_stamp(&mut self, table: &[u8]) -> Result<Option<(PageId, TxnId)>> {
        self.reader.table_root_stamp(table)
    }

    pub fn table_for_each<F>(&mut self, table: &[u8], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        self.reader.table_for_each(table, f)
    }

    pub fn table_scan_from<F>(&mut self, table: &[u8], start_key: &[u8], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        self.reader.table_scan_from(table, start_key, f)
    }

    pub fn table_scan_prefix<F>(&mut self, table: &[u8], prefix: &[u8], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        self.reader.table_scan_prefix(table, prefix, f)
    }

    pub fn table_scan_from_fast<F>(&mut self, table: &[u8], start_key: &[u8], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        self.reader.table_scan_from_fast(table, start_key, f)
    }

    pub fn table_scan_raw<F>(&mut self, table: &[u8], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        self.reader.table_scan_raw(table, f)
    }

    pub fn table_scan_iter<'a>(
        &'a mut self,
        table: &[u8],
        start_key: &[u8],
    ) -> Result<crate::TableIter<ReadTxnScanAdapter<'a, 'db>>> {
        self.reader.table_scan_iter(table, start_key)
    }

    /// Leaves belong to this view. Only a committed cache generation permits
    /// sharing them with a later reader; pending pages carry no Merkle proof.
    pub fn collect_table_leaves(&mut self, table: &[u8]) -> Result<LeafPages> {
        self.reader.collect_table_leaves(table)
    }

    pub fn scan_leaves<F>(&mut self, leaves: &[Arc<Page>], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        self.reader.scan_leaves(leaves, f)
    }

    pub fn shard_scanner(&self) -> LeafShardScanner<'_> {
        self.reader.shard_scanner()
    }
}
