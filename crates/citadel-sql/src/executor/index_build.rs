use citadel::CancelToken;
use citadel_txn::write_txn::WriteTxn;

use crate::encoding::encode_composite_key;
use crate::error::{Result, SqlError};
use crate::eval::{referenced_columns, ColumnMap};
use crate::types::{DataType, IndexDef, IndexKey, IndexKind, InvertedKind, TableSchema, Value};

use super::helpers::{
    build_inverted_key, check_cancel, encode_index_key_with_schema_and_cancel, encode_index_value,
    extract_inverted_entries_with_values_and_cancel, row_matches_partial_with_cancel,
    PartialDecodeCtx,
};

type ScanRow<'a> = dyn FnMut(&[u8], &[u8]) -> citadel_core::Result<bool> + 'a;

pub(super) struct IndexEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

pub(super) struct InvertedRow {
    primary_key: Vec<u8>,
    value: Value,
}

pub(super) enum IndexEntries {
    Btree(Vec<IndexEntry>),
    Inverted {
        column: usize,
        kind: InvertedKind,
        rows: Vec<InvertedRow>,
    },
}

pub(super) struct IndexBuildPlan<'a> {
    schema: &'a TableSchema,
    index: &'a IndexDef,
    predicate: Option<(PartialDecodeCtx, &'a ColumnMap)>,
    keys: PartialDecodeCtx,
    pk_columns: &'a [usize],
    inverted: Option<(usize, InvertedKind)>,
    cancel: Option<&'a CancelToken>,
}

impl<'a> IndexBuildPlan<'a> {
    pub(super) fn new(
        schema: &'a TableSchema,
        index: &'a IndexDef,
        cancel: Option<&'a CancelToken>,
    ) -> Result<Self> {
        check_cancel(cancel)?;
        let inverted = validate_inverted_key(schema, index)?;
        let pk_columns = schema.pk_indices();
        let mut needed = pk_columns.to_vec();
        for key in &index.keys {
            match key {
                IndexKey::Column { idx, .. } => needed.push(*idx as usize),
                IndexKey::Expr { expr, .. } => {
                    needed.extend(referenced_columns(expr, &schema.columns));
                }
            }
        }
        needed.sort_unstable();
        needed.dedup();
        let mut keys = PartialDecodeCtx::new_with_cancel(schema, &needed, cancel)?;
        let predicate = if let Some(expr) = &index.predicate_expr {
            let input = PartialDecodeCtx::new_with_cancel(
                schema,
                &referenced_columns(expr, &schema.columns),
                cancel,
            )?;
            keys = keys.remaining_after(&input);
            Some((input, schema.column_map()))
        } else {
            None
        };
        Ok(Self {
            schema,
            index,
            predicate,
            keys,
            pk_columns,
            inverted,
            cancel,
        })
    }

    pub(super) fn collect(
        &self,
        scan: impl FnOnce(&mut ScanRow<'_>) -> citadel_core::Result<()>,
    ) -> Result<IndexEntries> {
        let mut row = Vec::new();
        let mut pk = Vec::with_capacity(self.pk_columns.len());
        let mut entries = match self.inverted {
            Some((column, kind)) => IndexEntries::Inverted {
                column,
                kind,
                rows: Vec::new(),
            },
            None => IndexEntries::Btree(Vec::new()),
        };
        let mut error = None;
        scan(
            &mut |key, value| match self.collect_row(key, value, &mut row, &mut pk, &mut entries) {
                Ok(()) => Ok(true),
                Err(err) => {
                    error = Some(err);
                    Ok(false)
                }
            },
        )
        .map_err(SqlError::Storage)?;
        if let Some(error) = error {
            return Err(error);
        }
        check_cancel(self.cancel)?;
        Ok(entries)
    }

    fn collect_row(
        &self,
        key: &[u8],
        value: &[u8],
        row: &mut Vec<Value>,
        pk: &mut Vec<Value>,
        entries: &mut IndexEntries,
    ) -> Result<()> {
        check_cancel(self.cancel)?;
        if let Some((input, columns)) = &self.predicate {
            input.decode_into_with_cancel(key, value, row, self.cancel)?;
            if !row_matches_partial_with_cancel(self.index, row, columns, self.cancel)? {
                return Ok(());
            }
        }
        self.keys
            .decode_into_with_cancel(key, value, row, self.cancel)?;
        pk.clear();
        pk.extend(self.pk_columns.iter().map(|&column| row[column].clone()));
        match entries {
            IndexEntries::Inverted { column, rows, .. } => {
                if !row[*column].is_null() {
                    rows.push(InvertedRow {
                        primary_key: encode_composite_key(pk),
                        value: std::mem::take(&mut row[*column]),
                    });
                }
            }
            IndexEntries::Btree(entries) => entries.push(IndexEntry {
                key: encode_index_key_with_schema_and_cancel(
                    self.index,
                    row,
                    pk,
                    self.schema,
                    self.cancel,
                )?,
                value: encode_index_value(self.index, row, pk),
            }),
        }
        Ok(())
    }

    pub(super) fn insert(
        &self,
        wtx: &mut WriteTxn<'_>,
        table: &[u8],
        entries: IndexEntries,
    ) -> Result<()> {
        match entries {
            IndexEntries::Btree(entries) => {
                for entry in entries {
                    check_cancel(self.cancel)?;
                    let inserted = wtx
                        .table_insert_index(table, &entry.key, &entry.value)
                        .map_err(SqlError::Storage)?;
                    // NULL keys include the row's PK, so only non-NULL duplicates collide.
                    if self.index.unique && !inserted {
                        return Err(SqlError::UniqueViolation(self.index.name.clone()));
                    }
                }
            }
            IndexEntries::Inverted { kind, rows, .. } => {
                for row in rows {
                    check_cancel(self.cancel)?;
                    let inverted = extract_inverted_entries_with_values_and_cancel(
                        &row.value,
                        kind,
                        self.cancel,
                    )?;
                    for (entry, value) in inverted {
                        check_cancel(self.cancel)?;
                        let key = build_inverted_key(&entry, &row.primary_key);
                        wtx.table_insert_index(table, &key, &value)
                            .map_err(SqlError::Storage)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "index_build_tests.rs"]
mod tests;

fn validate_inverted_key(
    schema: &TableSchema,
    index: &IndexDef,
) -> Result<Option<(usize, InvertedKind)>> {
    let IndexKind::Inverted(kind) = index.kind else {
        return Ok(None);
    };
    let [IndexKey::Column { idx, .. }] = index.keys.as_slice() else {
        return Err(SqlError::Unsupported(
            "inverted index requires exactly one column key".into(),
        ));
    };
    let column = *idx as usize;
    let data_type = schema.columns[column].data_type;
    match kind {
        InvertedKind::Gin(_) if !matches!(data_type, DataType::Json | DataType::Jsonb) => {
            return Err(SqlError::Unsupported(
                "GIN index requires a JSON or JSONB column".into(),
            ));
        }
        InvertedKind::Fts { .. } if !matches!(data_type, DataType::Text | DataType::TsVector) => {
            return Err(SqlError::Unsupported(format!(
                "FTS index requires a TEXT or TSVECTOR column, got {data_type}"
            )));
        }
        InvertedKind::Ann { .. } if !matches!(data_type, DataType::Vector { .. }) => {
            return Err(SqlError::Unsupported(format!(
                "ANN index requires a VECTOR column, got {data_type}"
            )));
        }
        _ => {}
    }
    if index.unique {
        return Err(SqlError::Unsupported(
            "UNIQUE not supported on inverted indexes".into(),
        ));
    }
    Ok(Some((column, kind)))
}
