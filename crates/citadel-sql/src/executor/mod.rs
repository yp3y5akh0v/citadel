//! SQL executor: DDL and DML operations.

mod aggregate;
mod correlated;
mod cte;
mod ddl;
mod dml;
mod explain;
mod helpers;
mod join;
mod scan;
mod select;
mod view;
mod window;
mod write;
use cte::*;
use ddl::*;
use dml::*;
pub use dml::{exec_insert_in_txn, InsertBufs};
use explain::*;
use join::*;
use scan::*;
use select::*;
use view::*;
use window::*;
use write::*;
pub use write::{compile_update, exec_update_compiled, CompiledUpdate, UpdateBufs};

use std::collections::HashMap;

use citadel::Database;

use crate::error::{Result, SqlError};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

type CteContext = HashMap<String, QueryResult>;
type ScanTableFn<'a> = &'a mut dyn FnMut(&str) -> Result<(TableSchema, Vec<Vec<Value>>)>;

pub fn execute(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &Statement,
    params: &[Value],
) -> Result<ExecutionResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table(db, schema, ct),
        Statement::DropTable(dt) => exec_drop_table(db, schema, dt),
        Statement::CreateIndex(ci) => exec_create_index(db, schema, ci),
        Statement::DropIndex(di) => exec_drop_index(db, schema, di),
        Statement::CreateView(cv) => exec_create_view(db, schema, cv),
        Statement::DropView(dv) => exec_drop_view(db, schema, dv),
        Statement::AlterTable(at) => exec_alter_table(db, schema, at),
        Statement::Insert(ins) => exec_insert(db, schema, ins, params),
        Statement::Select(sq) => exec_select_query(db, schema, sq),
        Statement::Update(upd) => exec_update(db, schema, upd),
        Statement::Delete(del) => exec_delete(db, schema, del),
        Statement::Explain(inner) => explain(schema, inner),
        Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::Savepoint(_)
        | Statement::ReleaseSavepoint(_)
        | Statement::RollbackTo(_) => Err(SqlError::Unsupported(
            "transaction control in auto-commit mode".into(),
        )),
    }
}

/// Execute a parsed SQL statement within an existing write transaction.
pub fn execute_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &Statement,
    params: &[Value],
) -> Result<ExecutionResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table_in_txn(wtx, schema, ct),
        Statement::DropTable(dt) => exec_drop_table_in_txn(wtx, schema, dt),
        Statement::CreateIndex(ci) => exec_create_index_in_txn(wtx, schema, ci),
        Statement::DropIndex(di) => exec_drop_index_in_txn(wtx, schema, di),
        Statement::CreateView(cv) => exec_create_view_in_txn(wtx, schema, cv),
        Statement::DropView(dv) => exec_drop_view_in_txn(wtx, schema, dv),
        Statement::AlterTable(at) => exec_alter_table_in_txn(wtx, schema, at),
        Statement::Insert(ins) => {
            let mut bufs = InsertBufs::new();
            exec_insert_in_txn(wtx, schema, ins, params, &mut bufs)
        }
        Statement::Select(sq) => exec_select_query_in_txn(wtx, schema, sq),
        Statement::Update(upd) => exec_update_in_txn(wtx, schema, upd),
        Statement::Delete(del) => exec_delete_in_txn(wtx, schema, del),
        Statement::Explain(inner) => explain(schema, inner),
        Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::Savepoint(_)
        | Statement::ReleaseSavepoint(_)
        | Statement::RollbackTo(_) => {
            Err(SqlError::Unsupported("nested transaction control".into()))
        }
    }
}

// ── Table scanning ──────────────────────────────────────────────────

pub(super) fn scan_table_read(
    db: &Database,
    schema: &SchemaManager,
    name: &str,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    let table_schema = schema
        .get(name)
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))?;
    let (rows, _) = collect_rows_read(db, table_schema, &None, None)?;
    Ok((table_schema.clone(), rows))
}

pub(super) fn scan_table_read_or_view(
    db: &Database,
    schema: &SchemaManager,
    name: &str,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    if let Some(ts) = schema.get(name) {
        let (rows, _) = collect_rows_read(db, ts, &None, None)?;
        return Ok((ts.clone(), rows));
    }
    if let Some(vd) = schema.get_view(name) {
        let qr = exec_view_read(db, schema, vd)?;
        let vs = build_view_schema(name, &qr);
        return Ok((vs, qr.rows));
    }
    Err(SqlError::TableNotFound(name.to_string()))
}

pub(super) fn scan_table_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    name: &str,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    let table_schema = schema
        .get(name)
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))?;
    let (rows, _) = collect_rows_write(wtx, table_schema, &None, None)?;
    Ok((table_schema.clone(), rows))
}

pub(super) fn scan_table_write_or_view(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    name: &str,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    if let Some(ts) = schema.get(name) {
        let (rows, _) = collect_rows_write(wtx, ts, &None, None)?;
        return Ok((ts.clone(), rows));
    }
    if let Some(vd) = schema.get_view(name) {
        let qr = exec_view_write(wtx, schema, vd)?;
        let vs = build_view_schema(name, &qr);
        return Ok((vs, qr.rows));
    }
    Err(SqlError::TableNotFound(name.to_string()))
}

pub(super) fn resolve_table_or_cte(
    name: &str,
    ctes: &CteContext,
    scan_table: ScanTableFn<'_>,
) -> Result<(TableSchema, Vec<Vec<Value>>)> {
    let lower = name.to_ascii_lowercase();
    if let Some(cte_result) = ctes.get(&lower) {
        let schema = build_cte_schema(&lower, cte_result);
        Ok((schema, cte_result.rows.clone()))
    } else {
        scan_table(&lower)
    }
}

pub(super) fn exec_select_join_with_ctes(
    stmt: &SelectStmt,
    ctes: &CteContext,
    scan_table: ScanTableFn<'_>,
) -> Result<ExecutionResult> {
    let (from_schema, from_rows) = resolve_table_or_cte(&stmt.from, ctes, scan_table)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);

    let mut tables: Vec<(String, TableSchema)> = vec![(from_alias.clone(), from_schema)];
    let mut join_rows: Vec<Vec<Vec<Value>>> = Vec::new();

    for join in &stmt.joins {
        let jname = &join.table.name;
        let (js, jrows) = resolve_table_or_cte(jname, ctes, scan_table)?;
        let jalias = table_alias_or_name(jname, &join.table.alias);
        tables.push((jalias, js));
        join_rows.push(jrows);
    }

    let mut outer_rows = from_rows;
    let mut cur_tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), &tables[0].1)];

    for (ji, join) in stmt.joins.iter().enumerate() {
        let inner_schema = &tables[ji + 1].1;
        let inner_alias = &tables[ji + 1].0;
        let inner_rows = &join_rows[ji];

        let mut preview_tables = cur_tables.clone();
        preview_tables.push((inner_alias.clone(), inner_schema));
        let combined_cols = build_joined_columns(&preview_tables);

        let outer_col_count = if outer_rows.is_empty() {
            cur_tables.iter().map(|(_, s)| s.columns.len()).sum()
        } else {
            outer_rows[0].len()
        };
        let inner_col_count = inner_schema.columns.len();

        outer_rows = exec_join_step(
            outer_rows,
            inner_rows,
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            None,
            None,
        );
        cur_tables.push((inner_alias.clone(), inner_schema));
    }

    let joined_cols = build_joined_columns(&cur_tables);
    process_select(&joined_cols, outer_rows, stmt, false)
}

// ── SELECT execution ────────────────────────────────────────────────
