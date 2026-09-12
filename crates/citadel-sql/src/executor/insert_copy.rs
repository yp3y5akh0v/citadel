//! Bounded copies between tables whose SQL rows have the same physical layout.

use citadel_txn::write_txn::WriteTxn;

use crate::encoding::{
    decode_key_value, encode_composite_key_into, encode_row_into, row_matches_layout,
};
use crate::error::{Result, SqlError};
use crate::parser::{Expr, InsertSource, InsertStmt, QueryBody, SelectColumn, SelectStmt};
use crate::schema::SchemaManager;
use crate::types::{DataType, ExecutionResult, TableSchema, Value};

use super::helpers::{coerce_for_column, decode_full_row_into_with_cancel};
use super::CteContext;

// One oversized row may exceed the target, but rows are already bounded by the
// storage value limit. The flat buffer avoids allocating two vectors per row.
const BATCH_BYTES: usize = 256 * 1024;
const BATCH_ROWS: usize = 4096;

pub(super) struct CopyPlan<'a> {
    source: &'a TableSchema,
    destination: &'a TableSchema,
    value_layout: Vec<(DataType, bool)>,
}

impl<'a> CopyPlan<'a> {
    pub(super) fn new(
        schema: &'a SchemaManager,
        insert: &InsertStmt,
        outer_ctes: &CteContext,
    ) -> Option<Self> {
        if insert.on_conflict.is_some() || insert.returning.is_some() || !outer_ctes.is_empty() {
            return None;
        }
        let InsertSource::Select(query) = &insert.source else {
            return None;
        };
        if query.recursive || !query.ctes.is_empty() {
            return None;
        }
        let QueryBody::Select(select) = &query.body else {
            return None;
        };
        if !plain_scan(select) {
            return None;
        }
        let source_name = select.from.to_ascii_lowercase();
        let destination_name = insert.table.to_ascii_lowercase();
        for name in [&source_name, &destination_name] {
            if schema.get_view(name).is_some()
                || schema.get_matview(name).is_some()
                || schema.get_virtual(name).is_some()
            {
                return None;
            }
        }
        let source = schema.get(&source_name)?;
        let destination = schema.get(&destination_name)?;
        // Transition relations and TEMP aliases keep their normal resolver and
        // write guards. This path handles direct stored-table names only.
        if source.name != source_name || destination.name != destination_name {
            return None;
        }
        // Resolve storage names before rejecting self-copy (including aliases).
        // With no destination triggers, another table cannot change the source
        // while this write transaction reads and writes successive batches.
        if source.name == destination.name
            || !destination.indices.is_empty()
            || !destination.foreign_keys.is_empty()
            || destination.has_checks()
            || schema
                .triggers_for(&destination.name)
                .iter()
                .any(|t| t.enabled)
            || !compatible_layouts(source, destination)
        {
            return None;
        }
        if !insert.columns.is_empty()
            && (insert.columns.len() != destination.columns.len()
                || !insert
                    .columns
                    .iter()
                    .zip(&destination.columns)
                    .all(|(name, col)| name.eq_ignore_ascii_case(&col.name)))
        {
            return None;
        }
        if !identity_projection(select, source) {
            return None;
        }
        Some(Self {
            source,
            destination,
            value_layout: destination
                .non_pk_indices()
                .iter()
                .map(|&index| {
                    let column = &destination.columns[index];
                    (column.data_type, column.nullable)
                })
                .collect(),
        })
    }

    pub(super) fn execute(
        &self,
        wtx: &mut WriteTxn<'_>,
        schema: &SchemaManager,
    ) -> Result<ExecutionResult> {
        let cancel = wtx.cancel_token().cloned();
        let mut batch = CopyBatch::default();
        let mut resume = Vec::new();
        let mut logical = Vec::new();
        let mut normalized = Vec::new();
        let mut normalized_key = Vec::new();
        let mut primary_key = Vec::new();
        let mut values = Vec::new();
        let mut count = 0;
        loop {
            batch.clear();
            let mut error = None;
            wtx.table_scan_from(self.source.name.as_bytes(), &resume, |key, value| {
                let copied = (|| {
                    // A legacy row can predate ADD COLUMN or constraint fixes.
                    // Borrowed validation keeps the usual copy allocation-free
                    // while rows requiring coercion use the ordinary decoder.
                    if self.key_matches_layout(key)?
                        && row_matches_layout(value, &self.value_layout)?
                    {
                        batch.push(key, value);
                    } else {
                        decode_full_row_into_with_cancel(
                            self.source,
                            key,
                            value,
                            &mut logical,
                            cancel.as_ref(),
                        )?;
                        for (index, column) in self.destination.columns.iter().enumerate() {
                            logical[index] = coerce_for_column(
                                std::mem::replace(&mut logical[index], Value::Null),
                                column,
                                self.destination.is_strict(),
                            )?;
                            if logical[index].is_null() && !column.nullable {
                                return Err(SqlError::NotNullViolation(column.name.clone()));
                            }
                        }
                        primary_key.clear();
                        for &index in self.destination.pk_indices() {
                            primary_key.push(std::mem::replace(&mut logical[index], Value::Null));
                        }
                        encode_composite_key_into(&primary_key, &mut normalized_key);
                        if normalized_key.len() > citadel_core::MAX_KEY_SIZE {
                            return Err(SqlError::KeyTooLarge {
                                size: normalized_key.len(),
                                max: citadel_core::MAX_KEY_SIZE,
                            });
                        }
                        values.clear();
                        for &index in self.destination.non_pk_indices() {
                            values.push(std::mem::replace(&mut logical[index], Value::Null));
                        }
                        encode_row_into(&values, &mut normalized);
                        if normalized.len() > citadel_core::MAX_VALUE_SIZE {
                            return Err(SqlError::RowTooLarge {
                                size: normalized.len(),
                                max: citadel_core::MAX_VALUE_SIZE,
                            });
                        }
                        batch.push(&normalized_key, &normalized);
                    }
                    if batch.full() {
                        batch.last_source_key.clear();
                        batch.last_source_key.extend_from_slice(key);
                    }
                    Ok(())
                })();
                if let Err(e) = copied {
                    error = Some(e);
                }
                Ok(error.is_none() && !batch.full())
            })
            .map_err(SqlError::Storage)?;
            if let Some(error) = error {
                return Err(error);
            }
            if batch.rows.is_empty() {
                break;
            }
            // Appending zero is the smallest byte string strictly after this
            // key. Seeking it avoids re-materializing or re-charging the final
            // source row when the next batch starts.
            if batch.full() {
                resume.clear();
                resume.extend_from_slice(&batch.last_source_key);
                resume.push(0);
            }
            for (key, value) in batch.iter() {
                if !wtx
                    .table_insert_if_absent(self.destination.name.as_bytes(), key, value)
                    .map_err(SqlError::Storage)?
                {
                    return Err(SqlError::DuplicateKey);
                }
                count += 1;
            }
            if !batch.full() {
                break;
            }
        }
        if count != 0 {
            schema.mark_dml(&self.destination.name);
        }
        Ok(ExecutionResult::RowsAffected(count))
    }

    fn key_matches_layout(&self, key: &[u8]) -> Result<bool> {
        let mut position = 0;
        for &index in self.destination.pk_indices() {
            let (value, length) = decode_key_value(&key[position..])?;
            let column = &self.destination.columns[index];
            if value.is_null() {
                if !column.nullable {
                    return Ok(false);
                }
            } else if value.data_type() != column.data_type {
                return Ok(false);
            }
            position += length;
        }
        Ok(position == key.len())
    }
}

fn plain_scan(select: &SelectStmt) -> bool {
    !select.from.is_empty()
        && select.from_subquery.is_none()
        && select.from_args.is_none()
        && select.from_json_table.is_none()
        && select.joins.is_empty()
        && !select.distinct
        && select.where_clause.is_none()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.offset.is_none()
        && select.group_by.is_empty()
        && select.having.is_none()
}

fn compatible_layouts(source: &TableSchema, destination: &TableSchema) -> bool {
    if source.columns.len() != destination.columns.len()
        || source.is_strict() != destination.is_strict()
        || source.primary_key_columns != destination.primary_key_columns
        || source.encoding_positions() != destination.encoding_positions()
        || source.physical_non_pk_count() != destination.physical_non_pk_count()
        || !source.dropped_non_pk_slots().is_empty()
        || !destination.dropped_non_pk_slots().is_empty()
    {
        return false;
    }
    source
        .columns
        .iter()
        .zip(&destination.columns)
        .all(|(a, b)| {
            a.position == b.position
                && a.data_type == b.data_type
                && a.collation == b.collation
                && (!a.nullable || b.nullable)
                && a.generated_kind.is_none()
                && b.generated_kind.is_none()
                && a.default_expr.is_none()
                && b.default_expr.is_none()
        })
}

fn identity_projection(select: &SelectStmt, source: &TableSchema) -> bool {
    if matches!(select.columns.as_slice(), [SelectColumn::AllColumns]) {
        return true;
    }
    let qualifier = select.from_alias.as_deref().unwrap_or(&select.from);
    select.columns.len() == source.columns.len()
        && select
            .columns
            .iter()
            .zip(&source.columns)
            .all(|(projection, column)| match projection {
                SelectColumn::Expr {
                    expr: Expr::Column(name),
                    ..
                } => name.eq_ignore_ascii_case(&column.name),
                SelectColumn::Expr {
                    expr:
                        Expr::QualifiedColumn {
                            table,
                            column: name,
                        },
                    ..
                } => {
                    table.eq_ignore_ascii_case(qualifier) && name.eq_ignore_ascii_case(&column.name)
                }
                _ => false,
            })
}

#[derive(Default)]
struct CopyBatch {
    bytes: Vec<u8>,
    rows: Vec<(usize, usize)>,
    last_source_key: Vec<u8>,
}

impl CopyBatch {
    fn clear(&mut self) {
        self.bytes.clear();
        self.rows.clear();
        self.last_source_key.clear();
    }

    fn push(&mut self, key: &[u8], value: &[u8]) {
        self.bytes.extend_from_slice(key);
        let key_end = self.bytes.len();
        self.bytes.extend_from_slice(value);
        self.rows.push((key_end, self.bytes.len()));
    }

    fn full(&self) -> bool {
        self.bytes.len() >= BATCH_BYTES || self.rows.len() >= BATCH_ROWS
    }

    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.rows.iter().scan(0, |start, &(key_end, value_end)| {
            let row = (
                &self.bytes[*start..key_end],
                &self.bytes[key_end..value_end],
            );
            *start = value_end;
            Some(row)
        })
    }
}

#[cfg(test)]
#[path = "insert_copy_tests.rs"]
mod tests;
