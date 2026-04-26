use citadel_core::Result;
use citadel_txn::manager::TxnManager;
use citadel_txn::write_txn::WriteTxn;

use crate::crdt::{decode_lww_value, lww_merge, EntryKind, MergeResult};
use crate::patch::SyncPatch;

/// Result of applying a sync patch to a database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyResult {
    /// Entries written (remote won or key was new).
    pub entries_applied: u64,
    /// Entries skipped (local won LWW comparison).
    pub entries_skipped: u64,
    /// Entries where local and remote are identical (no-op).
    pub entries_equal: u64,
}

impl ApplyResult {
    pub fn empty() -> Self {
        Self {
            entries_applied: 0,
            entries_skipped: 0,
            entries_equal: 0,
        }
    }
}

/// Apply a sync patch to a database via TxnManager.
///
/// Opens a write transaction, applies entries, and commits.
/// For CRDT-aware patches: reads existing values and uses LWW merge.
/// For non-CRDT patches: unconditionally writes all entries.
pub fn apply_patch(manager: &TxnManager, patch: &SyncPatch) -> Result<ApplyResult> {
    if patch.is_empty() {
        return Ok(ApplyResult::empty());
    }

    let mut wtx = manager.begin_write()?;
    let result = apply_patch_to_txn(&mut wtx, patch)?;
    wtx.commit()?;
    Ok(result)
}

/// Apply a sync patch within an existing write transaction.
///
/// The caller is responsible for committing or aborting the transaction.
pub fn apply_patch_to_txn(wtx: &mut WriteTxn<'_>, patch: &SyncPatch) -> Result<ApplyResult> {
    let mut result = ApplyResult::empty();

    for entry in &patch.entries {
        if patch.crdt_aware {
            if let Some(ref remote_meta) = entry.crdt_meta {
                let existing = wtx.get(&entry.key)?;
                if let Some(local_data) = existing {
                    if let Ok(local_decoded) = decode_lww_value(&local_data) {
                        match lww_merge(&local_decoded.meta, remote_meta) {
                            MergeResult::Local => {
                                result.entries_skipped += 1;
                                continue;
                            }
                            MergeResult::Equal => {
                                result.entries_equal += 1;
                                continue;
                            }
                            MergeResult::Remote => {
                                // Remote wins - fall through to write
                            }
                        }
                    }
                    // Local value doesn't have valid CRDT header - remote wins
                }
                // Key doesn't exist locally - remote wins
            }
        }

        // Write the entry (either non-CRDT unconditional, or CRDT remote-wins)
        match entry.kind {
            EntryKind::Put => {
                wtx.insert(&entry.key, &entry.value)?;
            }
            EntryKind::Tombstone => {
                // Tombstone: write the CRDT header as the value so it participates
                // in future LWW merges. The key remains with a tombstone marker.
                wtx.insert(&entry.key, &entry.value)?;
            }
        }
        result.entries_applied += 1;
    }

    Ok(result)
}

/// Apply a sync patch to a named table, creating it if needed.
pub fn apply_patch_to_table(
    manager: &TxnManager,
    table_name: &[u8],
    patch: &SyncPatch,
) -> Result<ApplyResult> {
    if patch.is_empty() {
        return Ok(ApplyResult::empty());
    }

    let mut wtx = manager.begin_write()?;
    match wtx.create_table(table_name) {
        Ok(()) => {}
        Err(citadel_core::Error::TableAlreadyExists(_)) => {}
        Err(e) => return Err(e),
    }
    let result = apply_patch_to_table_txn(&mut wtx, table_name, patch)?;
    wtx.commit()?;
    Ok(result)
}

/// Apply a sync patch to a named table within an existing write transaction.
pub fn apply_patch_to_table_txn(
    wtx: &mut WriteTxn<'_>,
    table_name: &[u8],
    patch: &SyncPatch,
) -> Result<ApplyResult> {
    let mut result = ApplyResult::empty();

    for entry in &patch.entries {
        if patch.crdt_aware {
            if let Some(ref remote_meta) = entry.crdt_meta {
                let existing = wtx.table_get(table_name, &entry.key)?;
                if let Some(local_data) = existing {
                    if let Ok(local_decoded) = decode_lww_value(&local_data) {
                        match lww_merge(&local_decoded.meta, remote_meta) {
                            MergeResult::Local => {
                                result.entries_skipped += 1;
                                continue;
                            }
                            MergeResult::Equal => {
                                result.entries_equal += 1;
                                continue;
                            }
                            MergeResult::Remote => {}
                        }
                    }
                }
            }
        }

        match entry.kind {
            EntryKind::Put | EntryKind::Tombstone => {
                wtx.table_insert(table_name, &entry.key, &entry.value)?;
            }
        }
        result.entries_applied += 1;
    }

    Ok(result)
}

#[cfg(test)]
#[path = "apply_tests.rs"]
mod tests;
