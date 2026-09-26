use citadel_txn::write_txn::WriteTxn;

use crate::error::{Result, SqlError};
use crate::parser::ReindexTarget;
use crate::schema::SchemaManager;
use crate::types::{ExecutionResult, IndexDef, IndexKey, IndexKind, InvertedKind, TableSchema};

use super::constraint_indexes::{name_primary_key_duplicate, reconcile_constraint_indexes_in_txn};
use super::index_build::IndexBuildPlan;

enum Scope {
    Tables(Vec<String>),
    Index { table: String, index: String },
}

pub(super) fn exec_reindex_in_txn(
    wtx: &mut WriteTxn<'_>,
    schema: &mut SchemaManager,
    target: &ReindexTarget,
) -> Result<ExecutionResult> {
    let scope = resolve(schema, target)?;
    super::with_statement_savepoint(wtx, schema, |wtx, schema| {
        match scope {
            Scope::Tables(tables) => {
                for table in &tables {
                    reindex_table(wtx, schema, table)?;
                }
                // Interval keys compared by length need the equality indexes collated keys use.
                reconcile_constraint_indexes_in_txn(wtx, schema)?;
            }
            Scope::Index { table, index } => {
                let table = schema
                    .get(&table)
                    .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;
                let index = table
                    .index_by_name(&index)
                    .ok_or_else(|| SqlError::IndexNotFound(index.clone()))?;
                rebuild(wtx, table, index)?;
            }
        }
        Ok(ExecutionResult::Ok)
    })
}

fn resolve(schema: &SchemaManager, target: &ReindexTarget) -> Result<Scope> {
    let table = |name: &str| {
        let lower = name.to_ascii_lowercase();
        schema
            .get(&lower)
            .or_else(|| schema.get(&schema.resolve_temp(&lower)))
            .map(|table| Scope::Tables(vec![table.name.clone()]))
    };
    let index = |name: &str| {
        let lower = name.to_ascii_lowercase();
        super::ddl::find_index_in_schemas(schema, &lower).map(|(table, _)| Scope::Index {
            table,
            index: lower,
        })
    };
    match target {
        ReindexTarget::All => {
            let mut tables: Vec<String> = schema.all_schemas().map(|t| t.name.clone()).collect();
            tables.sort_unstable();
            Ok(Scope::Tables(tables))
        }
        ReindexTarget::Table(name) => {
            table(name).ok_or_else(|| SqlError::TableNotFound(name.clone()))
        }
        ReindexTarget::Index(name) => {
            index(name).ok_or_else(|| SqlError::IndexNotFound(name.clone()))
        }
        ReindexTarget::TableOrIndex(name) => table(name)
            .or_else(|| index(name))
            .ok_or_else(|| SqlError::TableNotFound(name.clone())),
    }
}

/// A table's INTERVAL keys take the collation new INTERVAL columns have before its
/// indexes are rebuilt, so REINDEX brings an older table's keys to length equality.
fn reindex_table(wtx: &mut WriteTxn<'_>, schema: &mut SchemaManager, name: &str) -> Result<()> {
    let mut table = schema
        .get(name)
        .cloned()
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))?;
    for column in &mut table.columns {
        if let Some(fixed) = column.data_type.fixed_collation() {
            column.collation = fixed;
        }
    }
    for key in table.indices.iter_mut().flat_map(|index| &mut index.keys) {
        if let IndexKey::Column { idx, collate } = key {
            if let Some(fixed) = table.columns[*idx as usize].data_type.fixed_collation() {
                *collate = fixed;
            }
        }
    }
    for index in &table.indices {
        rebuild(wtx, &table, index)?;
    }
    SchemaManager::save_schema(wtx, &table)?;
    schema.register(table);
    Ok(())
}

fn rebuild(wtx: &mut WriteTxn<'_>, table: &TableSchema, index: &IndexDef) -> Result<()> {
    let storage = TableSchema::index_table_name(&table.name, &index.name);
    wtx.table_truncate(&storage).map_err(SqlError::Storage)?;
    if matches!(index.kind, IndexKind::Inverted(InvertedKind::Ann { .. })) {
        super::ann_persist::purge_segment(wtx, &table.name)?;
    }
    let cancel = wtx.cancel_token().cloned();
    IndexBuildPlan::new(table, index, cancel.as_ref())?
        .build(wtx, &storage)
        .map_err(|error| name_primary_key_duplicate(table, index, error))
}
