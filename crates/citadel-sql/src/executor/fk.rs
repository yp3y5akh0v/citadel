//! Resolve foreign-key references to the primary or unique storage key they name.

use citadel_txn::write_txn::{DeferredFkCheck, WriteTxn};

use crate::encoding::{encode_composite_key_from_indices, encode_key_value_collated_into};
use crate::error::{Result, SqlError};
use crate::schema::SchemaManager;
use crate::types::{
    Collation, DataType, ForeignKeySchemaEntry, IndexDef, IndexKind, TableSchema, Value,
};

use super::helpers::decode_full_row_with_cancel;

/// Equality of a foreign key is defined by the referenced parent columns.
/// This descriptor never changes physical row identities or deferred locators.
pub(super) struct ReferenceKey {
    pub(super) columns: Vec<u16>,
    collations: Vec<Collation>,
}

impl ReferenceKey {
    pub(super) fn new(parent: &TableSchema, fk: &ForeignKeySchemaEntry) -> Result<Self> {
        let columns = fk
            .referred_columns
            .iter()
            .map(|name| {
                parent
                    .column_index(name)
                    .map(|i| i as u16)
                    .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        if columns.len() != fk.columns.len() {
            return Err(SqlError::ForeignKeyViolation(
                "foreign key column count mismatch".into(),
            ));
        }
        let collations = columns
            .iter()
            .map(|&i| {
                let column = &parent.columns[i as usize];
                if column.data_type == DataType::Text {
                    column.collation
                } else {
                    Collation::Binary
                }
            })
            .collect();
        Ok(Self {
            columns,
            collations,
        })
    }

    /// A probe must contain every parent-equal value. A Binary comparison may
    /// use a broader text index, followed by a parent-semantic residual check.
    pub(super) fn covered_by(&self, index: &IndexDef, columns: &[u16]) -> bool {
        self.collations.len() == columns.len()
            && index.is_full_column_btree(columns)
            && self
                .collations
                .iter()
                .enumerate()
                .all(|(i, &parent)| parent == Collation::Binary || parent == index.collation_at(i))
    }

    pub(super) fn exact_index_equality(&self, index: &IndexDef) -> bool {
        self.collations
            .iter()
            .enumerate()
            .all(|(i, &c)| c == index.collation_at(i))
    }

    pub(super) fn collations(&self) -> &[Collation] {
        &self.collations
    }

    pub(super) fn encode_row(&self, columns: &[u16], row: &[Value], out: &mut Vec<u8>) {
        out.clear();
        for (&column, &collation) in columns.iter().zip(&self.collations) {
            encode_key_value_collated_into(&row[column as usize], collation, out);
        }
    }

    pub(super) fn encode_values(&self, values: &[Value], out: &mut Vec<u8>) {
        out.clear();
        for (value, &collation) in values.iter().zip(&self.collations) {
            encode_key_value_collated_into(value, collation, out);
        }
    }

    pub(super) fn value_equal(&self, position: usize, a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Text(a), Value::Text(b)) => self.collations[position].eq_text(a, b),
            _ => a.bit_eq(b),
        }
    }
}

/// The integer insert program can keep its direct table probe only when the
/// parent's logical primary-key equality matches its physical key encoding.
pub(super) fn references_primary_key(schema: &SchemaManager, fk: &ForeignKeySchemaEntry) -> bool {
    schema.get(&fk.foreign_table).is_some_and(|parent| {
        refers_to_primary_key(parent, fk) && parent.primary_key_has_binary_collation()
    })
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

/// Resolve the actual storage path after catalog admission. None is the proven
/// Binary physical PK route; an index route always reads the current writer.
fn parent_index<'a>(parent: &'a TableSchema, key: &ReferenceKey) -> Result<Option<&'a IndexDef>> {
    if key.columns == parent.primary_key_columns {
        if parent.primary_key_has_binary_collation() {
            return Ok(None);
        }
        return parent
            .primary_key_equality_index()
            .map(|i| Some(&parent.indices[i]))
            .ok_or_else(|| {
                SqlError::ForeignKeyViolation(format!(
                    "no equality index backs the primary key in '{}'",
                    parent.name
                ))
            });
    }
    parent
        .indices
        .iter()
        .find(|index| index.unique && key.covered_by(index, &key.columns))
        .map(Some)
        .ok_or_else(|| {
            SqlError::ForeignKeyViolation(format!(
                "no compatible unique index backs the referenced columns in '{}'",
                parent.name
            ))
        })
}

pub(super) fn validate_parent_reference(
    parent: &TableSchema,
    fk: &ForeignKeySchemaEntry,
) -> Result<ReferenceKey> {
    let key = ReferenceKey::new(parent, fk)?;
    parent_index(parent, &key)?;
    Ok(key)
}

pub(super) fn has_complete_foreign_key_indexes(
    schema: &SchemaManager,
    table: &TableSchema,
) -> bool {
    table.foreign_keys.iter().all(|fk| {
        let parent = if fk.foreign_table == table.name {
            Some(table)
        } else {
            schema.get(&fk.foreign_table)
        };
        parent
            .and_then(|parent| validate_parent_reference(parent, fk).ok())
            .is_some_and(|reference| {
                table
                    .indices
                    .iter()
                    .any(|index| reference.covered_by(index, &fk.columns))
            })
    })
}

/// Construct missing child lookup indexes from parent equality, without trusting
/// a reserved name or replacing an existing user definition.
pub(super) fn child_indexes_to_add(
    schema: &SchemaManager,
    table: &TableSchema,
    name_in_use: impl Fn(&str) -> bool,
) -> Result<Vec<IndexDef>> {
    let mut pending: Vec<IndexDef> = Vec::new();
    for (ordinal, fk) in table.foreign_keys.iter().enumerate() {
        let parent = if fk.foreign_table == table.name {
            table
        } else {
            schema
                .get(&fk.foreign_table)
                .ok_or_else(|| SqlError::TableNotFound(fk.foreign_table.clone()))?
        };
        let reference = ReferenceKey::new(parent, fk)?;
        if table
            .indices
            .iter()
            .chain(&pending)
            .any(|index| reference.covered_by(index, &fk.columns))
        {
            continue;
        }
        let prefix = format!(
            "__fk_{}_{}",
            table.name,
            fk.name.clone().unwrap_or_else(|| ordinal.to_string())
        );
        let mut name = prefix.clone();
        let mut suffix = 0_u64;
        while table.index_by_name(&name).is_some()
            || pending
                .iter()
                .any(|index| index.name.eq_ignore_ascii_case(&name))
            || schema
                .all_schemas()
                .any(|other| other.index_by_name(&name).is_some())
            || name_in_use(&name)
        {
            suffix += 1;
            name = format!("{prefix}_{suffix}");
        }
        pending.push(IndexDef::from_column_lists(
            name,
            fk.columns.clone(),
            reference.collations().to_vec(),
            false,
            None,
            None,
            IndexKind::BTree,
        ));
    }
    Ok(pending)
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
        let reference = validate_parent_reference(child, fk)?;
        reference.encode_row(&fk.columns, row, key_buf);
        let mut candidate_key = Vec::new();
        reference.encode_row(&reference.columns, row, &mut candidate_key);
        if *key_buf == candidate_key {
            return Ok(());
        }
    }
    check_row_reference_now(wtx, schema, fk, row, key_buf)
}

/// Validate newly admitted references against the same writer in bounded row
/// batches. Physical scan cursors remain raw; deferred checks keep raw child IDs.
pub(super) fn check_table_references(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    child: &TableSchema,
    foreign_keys: &[&ForeignKeySchemaEntry],
) -> Result<()> {
    const ROW_BATCH: usize = 256;
    let mut cursor: Option<Vec<u8>> = None;
    let mut reference_key = Vec::new();
    loop {
        let mut batch = Vec::with_capacity(ROW_BATCH);
        wtx.table_scan_from(
            child.name.as_bytes(),
            cursor.as_deref().unwrap_or(b""),
            |key, value| {
                if cursor.as_deref() == Some(key) {
                    return Ok(true);
                }
                batch.push((key.to_vec(), value.to_vec()));
                Ok(batch.len() < ROW_BATCH)
            },
        )
        .map_err(SqlError::Storage)?;
        if batch.is_empty() {
            return Ok(());
        }
        let full = batch.len() == ROW_BATCH;
        cursor = batch.last().map(|(key, _)| key.clone());
        for (key, value) in batch {
            let row = decode_full_row_with_cancel(child, &key, &value, wtx.cancel_token())?;
            for fk in foreign_keys {
                check_row_reference(wtx, schema, child, fk, &row, &mut reference_key)?;
            }
        }
        if !full {
            return Ok(());
        }
    }
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
    let reference = ReferenceKey::new(parent, fk)?;
    let Some(index) = parent_index(parent, &reference)? else {
        encode_composite_key_from_indices(&fk.columns, row, key_buf);
        return check_reference(wtx, name, parent.name.as_bytes(), key_buf, None, true);
    };
    key_buf.clear();
    for (position, &column) in fk.columns.iter().enumerate() {
        encode_key_value_collated_into(
            &row[column as usize],
            index.collation_at(position),
            key_buf,
        );
    }
    let storage_table = TableSchema::index_table_name(&parent.name, &index.name);
    let recheck = if reference.exact_index_equality(index) {
        None
    } else {
        let mut expected_key = Vec::new();
        reference.encode_row(&fk.columns, row, &mut expected_key);
        Some(ParentRecheck {
            parent,
            reference: &reference,
            expected_key,
        })
    };
    check_reference(wtx, name, &storage_table, key_buf, recheck.as_ref(), false)
}

struct ParentRecheck<'a> {
    parent: &'a TableSchema,
    reference: &'a ReferenceKey,
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
        recheck
            .reference
            .encode_row(&recheck.reference.columns, &row, &mut actual_key);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_reference_validation_checks_default_values_after_first_batch() {
        let db = citadel::DatabaseBuilder::new("")
            .passphrase(b"fk-reference-batch-boundary")
            .argon2_profile(citadel::Argon2Profile::Iot)
            .create_in_memory()
            .unwrap();
        let connection = crate::Connection::open(&db).unwrap();
        connection
            .execute("CREATE TABLE parent(id TEXT COLLATE NOCASE PRIMARY KEY)")
            .unwrap();
        connection
            .execute("INSERT INTO parent VALUES('Alpha')")
            .unwrap();
        connection
            .execute("CREATE TABLE child(id INTEGER PRIMARY KEY, p TEXT DEFAULT 'missing')")
            .unwrap();
        connection
            .execute(
                "CREATE TABLE definition(id INTEGER PRIMARY KEY, p TEXT REFERENCES parent(id))",
            )
            .unwrap();
        connection.execute("BEGIN").unwrap();
        let insert = connection
            .prepare("INSERT INTO child VALUES($1, 'alpha')")
            .unwrap();
        for id in 1..=256 {
            insert.execute(&[Value::Integer(id)]).unwrap();
        }
        connection
            .execute("INSERT INTO child(id) VALUES(257)")
            .unwrap();
        connection.execute("COMMIT").unwrap();
        let schema = SchemaManager::load(&db).unwrap();
        let mut child = schema.get("child").unwrap().clone();
        // Model admitting a new reference over existing rows. The first batch
        // is valid; only the default-valued row in the next batch violates it.
        child.foreign_keys = schema.get("definition").unwrap().foreign_keys.clone();
        let references = child.foreign_keys.iter().collect::<Vec<_>>();
        let mut writer = db.begin_write().unwrap();
        assert!(matches!(
            check_table_references(&mut writer, &schema, &child, &references),
            Err(SqlError::ForeignKeyViolation(_))
        ));
        drop(writer);
        connection
            .execute("UPDATE child SET p='ALPHA' WHERE id=257")
            .unwrap();
        let mut writer = db.begin_write().unwrap();
        check_table_references(&mut writer, &schema, &child, &references).unwrap();
    }
}
