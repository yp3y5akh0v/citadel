use std::cmp::Ordering;

use citadel_core::{Error, Result};
use citadel_txn::manager::TxnManager;
use citadel_txn::write_txn::WriteTxn;

use crate::crdt::{decode_lww_value, encode_lww_value, lww_merge, EntryKind, MergeResult};
use crate::patch::{PatchEntry, SyncPatch};

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
/// For non-CRDT patches: unconditionally applies each physical put/delete.
pub fn apply_patch(manager: &TxnManager, patch: &SyncPatch) -> Result<ApplyResult> {
    validate_patch(patch)?;
    if patch.is_empty() {
        return Ok(ApplyResult::empty());
    }

    let mut wtx = manager.begin_write()?;
    let result = apply_patch_to_txn_validated(&mut wtx, patch)?;
    wtx.commit()?;
    Ok(result)
}

pub(crate) fn apply_patch_if_generation(
    manager: &TxnManager,
    patch: &SyncPatch,
    expected_generation: u64,
) -> Result<Option<(ApplyResult, u64)>> {
    validate_patch(patch)?;
    if patch.is_empty() {
        return Ok(Some((ApplyResult::empty(), expected_generation)));
    }
    let Some(mut wtx) = manager.begin_write_if_generation(expected_generation)? else {
        return Ok(None);
    };
    let result = apply_patch_to_txn_validated(&mut wtx, patch)?;
    let generation = wtx.commit_with_generation()?;
    Ok(Some((result, generation)))
}

/// Apply a sync patch within an existing write transaction.
///
/// The caller is responsible for committing or aborting the transaction.
pub fn apply_patch_to_txn(wtx: &mut WriteTxn<'_>, patch: &SyncPatch) -> Result<ApplyResult> {
    validate_patch(patch)?;
    apply_patch_to_txn_validated(wtx, patch)
}

fn apply_patch_to_txn_validated(wtx: &mut WriteTxn<'_>, patch: &SyncPatch) -> Result<ApplyResult> {
    let marker = wtx.mutation_marker();
    let applied = apply_patch_entries(wtx, patch);
    poison_after_partial_apply(wtx, marker, &applied);
    applied
}

fn apply_patch_entries(wtx: &mut WriteTxn<'_>, patch: &SyncPatch) -> Result<ApplyResult> {
    let mut result = ApplyResult::empty();

    for entry in &patch.entries {
        if patch.crdt_aware {
            match crdt_decision(wtx.get(&entry.key)?, entry) {
                CrdtDecision::Skip { canonical_local } => {
                    if let Some(value) = canonical_local {
                        wtx.insert(&entry.key, &value)?;
                    }
                    result.entries_skipped += 1;
                    continue;
                }
                CrdtDecision::Equal { canonical_local } => {
                    if let Some(value) = canonical_local {
                        wtx.insert(&entry.key, &value)?;
                    }
                    result.entries_equal += 1;
                    continue;
                }
                CrdtDecision::Apply => {}
            }
        }

        // Write the entry (either non-CRDT unconditional, or CRDT remote-wins)
        match entry.kind {
            EntryKind::Put => {
                wtx.insert(&entry.key, &entry.value)?;
            }
            EntryKind::Tombstone if !patch.crdt_aware => {
                wtx.delete(&entry.key)?;
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
    validate_patch(patch)?;
    let mut wtx = manager.begin_write()?;
    match wtx.create_table(table_name) {
        Ok(()) => {}
        Err(citadel_core::Error::TableAlreadyExists(_)) => {}
        Err(e) => return Err(e),
    }
    let result = apply_patch_to_table_txn_validated(&mut wtx, table_name, patch)?;
    wtx.commit()?;
    Ok(result)
}

pub(crate) fn apply_patch_to_table_if_generation(
    manager: &TxnManager,
    table_name: &[u8],
    patch: &SyncPatch,
    expected_generation: u64,
) -> Result<Option<(ApplyResult, u64)>> {
    validate_patch(patch)?;
    let Some(mut wtx) = manager.begin_write_if_generation(expected_generation)? else {
        return Ok(None);
    };
    match wtx.create_table(table_name) {
        Ok(()) => {}
        Err(citadel_core::Error::TableAlreadyExists(_)) => {}
        Err(error) => return Err(error),
    }
    let result = apply_patch_to_table_txn_validated(&mut wtx, table_name, patch)?;
    let generation = wtx.commit_with_generation()?;
    Ok(Some((result, generation)))
}

/// Apply a sync patch to a named table within an existing write transaction.
pub fn apply_patch_to_table_txn(
    wtx: &mut WriteTxn<'_>,
    table_name: &[u8],
    patch: &SyncPatch,
) -> Result<ApplyResult> {
    validate_patch(patch)?;
    apply_patch_to_table_txn_validated(wtx, table_name, patch)
}

fn apply_patch_to_table_txn_validated(
    wtx: &mut WriteTxn<'_>,
    table_name: &[u8],
    patch: &SyncPatch,
) -> Result<ApplyResult> {
    let marker = wtx.mutation_marker();
    let applied = apply_patch_to_table_entries(wtx, table_name, patch);
    poison_after_partial_apply(wtx, marker, &applied);
    applied
}

fn apply_patch_to_table_entries(
    wtx: &mut WriteTxn<'_>,
    table_name: &[u8],
    patch: &SyncPatch,
) -> Result<ApplyResult> {
    let mut result = ApplyResult::empty();

    for entry in &patch.entries {
        if patch.crdt_aware {
            match crdt_decision(wtx.table_get(table_name, &entry.key)?, entry) {
                CrdtDecision::Skip { canonical_local } => {
                    if let Some(value) = canonical_local {
                        wtx.table_insert(table_name, &entry.key, &value)?;
                    }
                    result.entries_skipped += 1;
                    continue;
                }
                CrdtDecision::Equal { canonical_local } => {
                    if let Some(value) = canonical_local {
                        wtx.table_insert(table_name, &entry.key, &value)?;
                    }
                    result.entries_equal += 1;
                    continue;
                }
                CrdtDecision::Apply => {}
            }
        }

        match entry.kind {
            EntryKind::Put => {
                wtx.table_insert(table_name, &entry.key, &entry.value)?;
            }
            EntryKind::Tombstone if !patch.crdt_aware => {
                wtx.table_delete(table_name, &entry.key)?;
            }
            EntryKind::Tombstone => {
                wtx.table_insert(table_name, &entry.key, &entry.value)?;
            }
        }
        result.entries_applied += 1;
    }

    Ok(result)
}

fn validate_patch(patch: &SyncPatch) -> Result<()> {
    patch
        .validate()
        .map_err(|error| Error::Sync(error.to_string()))
}

enum CrdtDecision {
    Apply,
    Skip { canonical_local: Option<Vec<u8>> },
    Equal { canonical_local: Option<Vec<u8>> },
}

fn crdt_decision(existing: Option<Vec<u8>>, remote: &PatchEntry) -> CrdtDecision {
    let Some(local_data) = existing else {
        return CrdtDecision::Apply;
    };
    let Ok(local) = decode_lww_value(&local_data) else {
        return CrdtDecision::Apply;
    };
    let remote_meta = remote
        .crdt_meta
        .as_ref()
        .expect("validated CRDT patches carry metadata");
    match lww_merge(&local.meta, remote_meta) {
        MergeResult::Local => CrdtDecision::Skip {
            canonical_local: canonicalized_if_needed(&local_data, &local),
        },
        MergeResult::Remote => CrdtDecision::Apply,
        MergeResult::Equal => {
            // Metadata alone cannot order contradictory replicas: canonical bytes
            // break the tie, and the kind byte makes a tombstone beat a put.
            let repaired = canonicalized_if_needed(&local_data, &local);
            let canonical_local = repaired.as_deref().unwrap_or(&local_data);
            match remote.value.as_slice().cmp(canonical_local) {
                Ordering::Less => CrdtDecision::Skip {
                    canonical_local: repaired,
                },
                Ordering::Equal => CrdtDecision::Equal {
                    canonical_local: repaired,
                },
                Ordering::Greater => CrdtDecision::Apply,
            }
        }
    }
}

fn canonicalized_if_needed(
    stored: &[u8],
    decoded: &crate::crdt::DecodedValue<'_>,
) -> Option<Vec<u8>> {
    let is_canonical = stored[1..4] == [0u8; 3]
        && (decoded.kind == EntryKind::Put || stored.len() == crate::crdt::CRDT_HEADER_SIZE);
    if is_canonical {
        return None;
    }
    let canonical = encode_lww_value(&decoded.meta, decoded.kind, decoded.user_value);
    Some(canonical)
}

fn poison_after_partial_apply(
    wtx: &mut WriteTxn<'_>,
    marker: citadel_txn::write_txn::MutationMarker,
    result: &Result<ApplyResult>,
) {
    let Err(error) = result else {
        return;
    };
    if !wtx.mutated_since(marker) {
        return;
    }
    if matches!(error, Error::Interrupted) {
        wtx.mark_cancelled();
    } else {
        wtx.mark_failed();
    }
}

#[cfg(test)]
#[path = "apply_tests.rs"]
mod tests;
