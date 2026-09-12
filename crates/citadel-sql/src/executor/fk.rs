//! Resolve foreign-key references to the primary or unique storage key they name.

use citadel_txn::write_txn::{DeferredFkCheck, WriteTxn};

use crate::encoding::{encode_composite_key_from_indices, encode_key_value_collated_into};
use crate::error::{Result, SqlError};
use crate::schema::SchemaManager;
use crate::types::{Collation, ForeignKeySchemaEntry, IndexDef, TableSchema, Value};

use super::helpers::decode_full_row_with_cancel;

/// The integer insert program can keep its direct table probe for this shape.
pub(super) fn references_primary_key(schema: &SchemaManager, fk: &ForeignKeySchemaEntry) -> bool {
    schema
        .get(&fk.foreign_table)
        .is_some_and(|parent| refers_to_primary_key(parent, fk))
}

fn refers_to_primary_key(parent: &TableSchema, fk: &ForeignKeySchemaEntry) -> bool {
    parent.primary_key_columns.len() == fk.referred_columns.len()
        && parent
            .primary_key_columns
            .iter()
            .zip(&fk.referred_columns)
            .all(|(&column, name)| {
                parent.columns[column as usize]
                    .name
                    .eq_ignore_ascii_case(name)
            })
}

fn unique_reference<'a>(
    parent: &'a TableSchema,
    fk: &ForeignKeySchemaEntry,
) -> Result<(&'a IndexDef, Vec<u16>)> {
    let columns = fk
        .referred_columns
        .iter()
        .map(|name| {
            parent
                .column_index(name)
                .map(|column| column as u16)
                .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    let index = parent
        .indices
        .iter()
        .find(|index| index.unique && index.is_full_column_btree(&columns))
        .ok_or_else(|| {
            SqlError::ForeignKeyViolation(format!(
                "no unique index backs the referenced columns in '{}'",
                parent.name
            ))
        })?;
    Ok((index, columns))
}

/// Check MATCH SIMPLE using reusable key storage, or enqueue the child identity.
pub(super) fn check_row_reference(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    child: &TableSchema,
    fk: &ForeignKeySchemaEntry,
    row: &[Value],
    key_buf: &mut Vec<u8>,
) -> Result<()> {
    if fk
        .columns
        .iter()
        .any(|&column| row[column as usize].is_null())
    {
        return Ok(());
    }
    if fk.deferrable && fk.initially_deferred {
        defer_row_reference(wtx, child, fk, row);
        return Ok(());
    }
    if fk.foreign_table == child.name {
        // The candidate row may satisfy its own reference before its physical
        // PK/UNIQUE entry is inserted. Never cache this provisional existence.
        let referred = if refers_to_primary_key(child, fk) {
            child.primary_key_columns.clone()
        } else {
            unique_reference(child, fk)?.1
        };
        encode_composite_key_from_indices(&fk.columns, row, key_buf);
        let mut candidate_key = Vec::new();
        encode_composite_key_from_indices(&referred, row, &mut candidate_key);
        if *key_buf == candidate_key {
            return Ok(());
        }
    }
    check_row_reference_now(wtx, schema, fk, row, key_buf)
}

/// Queue the child identity, so commit checks its final surviving reference.
pub(super) fn defer_row_reference(
    wtx: &mut WriteTxn<'_>,
    child: &TableSchema,
    fk: &ForeignKeySchemaEntry,
    row: &[Value],
) {
    let mut child_key = Vec::new();
    encode_composite_key_from_indices(&child.primary_key_columns, row, &mut child_key);
    wtx.defer_fk_check(DeferredFkCheck {
        child_table: child.name.as_bytes().to_vec(),
        child_key,
        child_columns: fk
            .columns
            .iter()
            .map(|&column| child.columns[column as usize].name.clone())
            .collect(),
        foreign_table: fk.foreign_table.clone(),
        referred_columns: fk.referred_columns.clone(),
    });
}

/// Validate the current reference even when the constraint is initially deferred.
pub(super) fn check_row_reference_now(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    fk: &ForeignKeySchemaEntry,
    row: &[Value],
    key_buf: &mut Vec<u8>,
) -> Result<()> {
    if fk
        .columns
        .iter()
        .any(|&column| row[column as usize].is_null())
    {
        return Ok(());
    }
    let parent = schema
        .get(&fk.foreign_table)
        .ok_or_else(|| SqlError::TableNotFound(fk.foreign_table.clone()))?;
    let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
    if refers_to_primary_key(parent, fk) {
        encode_composite_key_from_indices(&fk.columns, row, key_buf);
        return check_reference(wtx, name, parent.name.as_bytes(), key_buf, None, true);
    }

    let (index, columns) = unique_reference(parent, fk)?;
    key_buf.clear();
    for (position, &column) in fk.columns.iter().enumerate() {
        encode_key_value_collated_into(
            &row[column as usize],
            index.collation_at(position),
            key_buf,
        );
    }
    let storage_table = TableSchema::index_table_name(&parent.name, &index.name);
    let recheck = if (0..index.keys.len()).any(|i| index.collation_at(i) != Collation::Binary) {
        let mut expected_key = Vec::new();
        encode_composite_key_from_indices(&fk.columns, row, &mut expected_key);
        Some(ParentRecheck {
            parent,
            columns: &columns,
            expected_key,
        })
    } else {
        None
    };
    check_reference(wtx, name, &storage_table, key_buf, recheck.as_ref(), false)
}

struct ParentRecheck<'a> {
    parent: &'a TableSchema,
    columns: &'a [u16],
    expected_key: Vec<u8>,
}

/// Resolve the surviving child's current value, not an intermediate parent key.
pub(super) fn check_deferred_reference(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    check: &DeferredFkCheck,
) -> Result<()> {
    let table_name = std::str::from_utf8(&check.child_table)
        .map_err(|_| SqlError::ForeignKeyViolation(check.foreign_table.clone()))?;
    let Some(child) = schema.get(table_name) else {
        return Ok(());
    };
    let Some(fk) = child.foreign_keys.iter().find(|fk| {
        fk.foreign_table == check.foreign_table
            && fk.referred_columns == check.referred_columns
            && fk.columns.len() == check.child_columns.len()
            && fk
                .columns
                .iter()
                .zip(&check.child_columns)
                .all(|(&column, name)| child.columns[column as usize].name == *name)
    }) else {
        return Ok(());
    };
    let Some(value) = wtx
        .table_get(&check.child_table, &check.child_key)
        .map_err(SqlError::Storage)?
    else {
        return Ok(());
    };
    let row = decode_full_row_with_cancel(child, &check.child_key, &value, wtx.cancel_token())?;
    check_row_reference_now(wtx, schema, fk, &row, &mut Vec::new())
}

fn check_reference(
    wtx: &mut WriteTxn<'_>,
    name: &str,
    storage_table: &[u8],
    key: &[u8],
    parent_recheck: Option<&ParentRecheck<'_>>,
    cache_primary_key: bool,
) -> Result<()> {
    // Base-table mutations invalidate the PK cache. Specialized index writes do
    // not, so UNIQUE probes must inspect the current index and row every time.
    if cache_primary_key && wtx.fk_check_cached(storage_table, key) {
        return Ok(());
    }
    let value = wtx
        .table_get(storage_table, key)
        .map_err(SqlError::Storage)?
        .ok_or_else(|| SqlError::ForeignKeyViolation(name.into()))?;
    if let Some(recheck) = parent_recheck {
        // A non-NULL UNIQUE entry stores the physical parent primary key.
        let parent_value = wtx
            .table_get(recheck.parent.name.as_bytes(), &value)
            .map_err(SqlError::Storage)?
            .ok_or_else(|| SqlError::ForeignKeyViolation(name.into()))?;
        let row =
            decode_full_row_with_cancel(recheck.parent, &value, &parent_value, wtx.cancel_token())?;
        let mut actual_key = Vec::new();
        encode_composite_key_from_indices(recheck.columns, &row, &mut actual_key);
        if actual_key != recheck.expected_key {
            return Err(SqlError::ForeignKeyViolation(name.into()));
        }
    }
    if cache_primary_key {
        wtx.mark_fk_verified(storage_table, key);
    }
    Ok(())
}

/// Add renamed identities without changing entries protected by a savepoint.
pub(super) fn rename_pending_table(wtx: &mut WriteTxn<'_>, old: &str, new: &str) {
    retarget_pending(wtx, |check| {
        let mut changed = false;
        if check.child_table == old.as_bytes() {
            check.child_table = new.as_bytes().to_vec();
            changed = true;
        }
        if check.foreign_table == old {
            check.foreign_table = new.into();
            changed = true;
        }
        changed
    });
}

pub(super) fn rename_pending_column(wtx: &mut WriteTxn<'_>, table: &str, old: &str, new: &str) {
    retarget_pending(wtx, |check| {
        let mut changed = false;
        if check.child_table == table.as_bytes() {
            for column in &mut check.child_columns {
                if column == old {
                    *column = new.into();
                    changed = true;
                }
            }
        }
        if check.foreign_table == table {
            for column in &mut check.referred_columns {
                if column == old {
                    *column = new.into();
                    changed = true;
                }
            }
        }
        changed
    });
}

fn retarget_pending(wtx: &mut WriteTxn<'_>, mut rename: impl FnMut(&mut DeferredFkCheck) -> bool) {
    let checks = wtx.take_deferred_fk_checks();
    let renamed = checks
        .iter()
        .filter_map(|check| {
            let mut updated = check.clone();
            rename(&mut updated).then_some(updated)
        })
        .collect::<Vec<_>>();
    // Savepoints restore a queue prefix, so originals must remain in the same order.
    for check in checks.into_iter().chain(renamed) {
        wtx.defer_fk_check(check);
    }
}
