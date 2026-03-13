//! SQL executor: DDL and DML operations.

use std::collections::BTreeMap;

use citadel::Database;

use crate::encoding::{decode_composite_key, decode_key_value, decode_row, encode_composite_key, encode_row};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy};
use crate::parser::*;
use crate::planner::{self, ScanPlan};
use crate::schema::SchemaManager;
use crate::types::*;

// ── Index helpers ────────────────────────────────────────────────────

fn encode_index_key(
    idx: &IndexDef,
    row: &[Value],
    pk_values: &[Value],
) -> Vec<u8> {
    let indexed_values: Vec<Value> = idx.columns.iter()
        .map(|&col_idx| row[col_idx as usize].clone())
        .collect();

    if idx.unique {
        let any_null = indexed_values.iter().any(|v| v.is_null());
        if !any_null {
            return encode_composite_key(&indexed_values);
        }
    }

    let mut all_values = indexed_values;
    all_values.extend_from_slice(pk_values);
    encode_composite_key(&all_values)
}

fn encode_index_value(
    idx: &IndexDef,
    row: &[Value],
    pk_values: &[Value],
) -> Vec<u8> {
    if idx.unique {
        let indexed_values: Vec<Value> = idx.columns.iter()
            .map(|&col_idx| row[col_idx as usize].clone())
            .collect();
        let any_null = indexed_values.iter().any(|v| v.is_null());
        if !any_null {
            return encode_composite_key(pk_values);
        }
    }
    vec![]
}

fn insert_index_entries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    row: &[Value],
    pk_values: &[Value],
) -> Result<()> {
    for idx in &table_schema.indices {
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        let key = encode_index_key(idx, row, pk_values);
        let value = encode_index_value(idx, row, pk_values);

        let is_new = wtx.table_insert(&idx_table, &key, &value)
            .map_err(SqlError::Storage)?;

        if idx.unique && !is_new {
            let indexed_values: Vec<Value> = idx.columns.iter()
                .map(|&col_idx| row[col_idx as usize].clone())
                .collect();
            let any_null = indexed_values.iter().any(|v| v.is_null());
            if !any_null {
                return Err(SqlError::UniqueViolation(idx.name.clone()));
            }
        }
    }
    Ok(())
}

fn delete_index_entries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    row: &[Value],
    pk_values: &[Value],
) -> Result<()> {
    for idx in &table_schema.indices {
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        let key = encode_index_key(idx, row, pk_values);
        wtx.table_delete(&idx_table, &key).map_err(SqlError::Storage)?;
    }
    Ok(())
}

fn index_columns_changed(idx: &IndexDef, old_row: &[Value], new_row: &[Value]) -> bool {
    idx.columns.iter().any(|&col_idx| {
        old_row[col_idx as usize] != new_row[col_idx as usize]
    })
}

/// Execute a parsed SQL statement in auto-commit mode.
pub fn execute(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &Statement,
) -> Result<ExecutionResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table(db, schema, ct),
        Statement::DropTable(dt) => exec_drop_table(db, schema, dt),
        Statement::CreateIndex(ci) => exec_create_index(db, schema, ci),
        Statement::DropIndex(di) => exec_drop_index(db, schema, di),
        Statement::Insert(ins) => exec_insert(db, schema, ins),
        Statement::Select(sel) => exec_select(db, schema, sel),
        Statement::Update(upd) => exec_update(db, schema, upd),
        Statement::Delete(del) => exec_delete(db, schema, del),
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Err(SqlError::Unsupported("transaction control in auto-commit mode".into()))
        }
    }
}

/// Execute a parsed SQL statement within an existing write transaction.
pub fn execute_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &Statement,
) -> Result<ExecutionResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table_in_txn(wtx, schema, ct),
        Statement::DropTable(dt) => exec_drop_table_in_txn(wtx, schema, dt),
        Statement::CreateIndex(ci) => exec_create_index_in_txn(wtx, schema, ci),
        Statement::DropIndex(di) => exec_drop_index_in_txn(wtx, schema, di),
        Statement::Insert(ins) => exec_insert_in_txn(wtx, schema, ins),
        Statement::Select(sel) => exec_select_in_txn(wtx, schema, sel),
        Statement::Update(upd) => exec_update_in_txn(wtx, schema, upd),
        Statement::Delete(del) => exec_delete_in_txn(wtx, schema, del),
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Err(SqlError::Unsupported("nested transaction control".into()))
        }
    }
}

// ── DDL ─────────────────────────────────────────────────────────────

fn exec_create_table(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &CreateTableStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if schema.contains(&lower_name) {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::TableAlreadyExists(stmt.name.clone()));
    }

    if stmt.primary_key.is_empty() {
        return Err(SqlError::PrimaryKeyRequired);
    }

    let mut seen = std::collections::HashSet::new();
    for col in &stmt.columns {
        let lower = col.name.to_ascii_lowercase();
        if !seen.insert(lower.clone()) {
            return Err(SqlError::DuplicateColumn(col.name.clone()));
        }
    }

    let columns: Vec<ColumnDef> = stmt.columns.iter().enumerate().map(|(i, c)| {
        ColumnDef {
            name: c.name.to_ascii_lowercase(),
            data_type: c.data_type,
            nullable: c.nullable,
            position: i as u16,
        }
    }).collect();

    let primary_key_columns: Vec<u16> = stmt.primary_key.iter().map(|pk_name| {
        let lower = pk_name.to_ascii_lowercase();
        columns.iter().position(|c| c.name == lower)
            .map(|i| i as u16)
            .ok_or_else(|| SqlError::ColumnNotFound(pk_name.clone()))
    }).collect::<Result<_>>()?;

    let table_schema = TableSchema {
        name: lower_name.clone(),
        columns,
        primary_key_columns,
        indices: vec![],
    };

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    SchemaManager::ensure_schema_table(&mut wtx)?;
    wtx.create_table(lower_name.as_bytes()).map_err(SqlError::Storage)?;
    SchemaManager::save_schema(&mut wtx, &table_schema)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.register(table_schema);
    Ok(ExecutionResult::Ok)
}

fn exec_drop_table(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &DropTableStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if !schema.contains(&lower_name) {
        if stmt.if_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::TableNotFound(stmt.name.clone()));
    }

    let table_schema = schema.get(&lower_name).unwrap();
    let idx_tables: Vec<Vec<u8>> = table_schema.indices.iter()
        .map(|idx| TableSchema::index_table_name(&lower_name, &idx.name))
        .collect();

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    for idx_table in &idx_tables {
        wtx.drop_table(idx_table).map_err(SqlError::Storage)?;
    }
    wtx.drop_table(lower_name.as_bytes()).map_err(SqlError::Storage)?;
    SchemaManager::delete_schema(&mut wtx, &lower_name)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.remove(&lower_name);
    Ok(ExecutionResult::Ok)
}

fn exec_create_table_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &CreateTableStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if schema.contains(&lower_name) {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::TableAlreadyExists(stmt.name.clone()));
    }

    if stmt.primary_key.is_empty() {
        return Err(SqlError::PrimaryKeyRequired);
    }

    let mut seen = std::collections::HashSet::new();
    for col in &stmt.columns {
        let lower = col.name.to_ascii_lowercase();
        if !seen.insert(lower.clone()) {
            return Err(SqlError::DuplicateColumn(col.name.clone()));
        }
    }

    let columns: Vec<ColumnDef> = stmt.columns.iter().enumerate().map(|(i, c)| {
        ColumnDef {
            name: c.name.to_ascii_lowercase(),
            data_type: c.data_type,
            nullable: c.nullable,
            position: i as u16,
        }
    }).collect();

    let primary_key_columns: Vec<u16> = stmt.primary_key.iter().map(|pk_name| {
        let lower = pk_name.to_ascii_lowercase();
        columns.iter().position(|c| c.name == lower)
            .map(|i| i as u16)
            .ok_or_else(|| SqlError::ColumnNotFound(pk_name.clone()))
    }).collect::<Result<_>>()?;

    let table_schema = TableSchema {
        name: lower_name.clone(),
        columns,
        primary_key_columns,
        indices: vec![],
    };

    SchemaManager::ensure_schema_table(wtx)?;
    wtx.create_table(lower_name.as_bytes()).map_err(SqlError::Storage)?;
    SchemaManager::save_schema(wtx, &table_schema)?;

    schema.register(table_schema);
    Ok(ExecutionResult::Ok)
}

fn exec_drop_table_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &DropTableStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.name.to_ascii_lowercase();

    if !schema.contains(&lower_name) {
        if stmt.if_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::TableNotFound(stmt.name.clone()));
    }

    let table_schema = schema.get(&lower_name).unwrap();
    let idx_tables: Vec<Vec<u8>> = table_schema.indices.iter()
        .map(|idx| TableSchema::index_table_name(&lower_name, &idx.name))
        .collect();

    for idx_table in &idx_tables {
        wtx.drop_table(idx_table).map_err(SqlError::Storage)?;
    }
    wtx.drop_table(lower_name.as_bytes()).map_err(SqlError::Storage)?;
    SchemaManager::delete_schema(wtx, &lower_name)?;

    schema.remove(&lower_name);
    Ok(ExecutionResult::Ok)
}

fn exec_create_index(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &CreateIndexStmt,
) -> Result<ExecutionResult> {
    let lower_table = stmt.table_name.to_ascii_lowercase();
    let lower_idx = stmt.index_name.to_ascii_lowercase();

    let table_schema = schema.get(&lower_table)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table_name.clone()))?;

    if table_schema.index_by_name(&lower_idx).is_some() {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::IndexAlreadyExists(stmt.index_name.clone()));
    }

    let col_indices: Vec<u16> = stmt.columns.iter().map(|col_name| {
        let lower = col_name.to_ascii_lowercase();
        table_schema.column_index(&lower)
            .map(|i| i as u16)
            .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))
    }).collect::<Result<_>>()?;

    let idx_def = IndexDef {
        name: lower_idx.clone(),
        columns: col_indices,
        unique: stmt.unique,
    };

    let idx_table = TableSchema::index_table_name(&lower_table, &lower_idx);

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    SchemaManager::ensure_schema_table(&mut wtx)?;
    wtx.create_table(&idx_table).map_err(SqlError::Storage)?;

    // Populate index from existing rows
    let pk_indices = table_schema.pk_indices();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    {
        let mut scan_err: Option<SqlError> = None;
        wtx.table_for_each(lower_table.as_bytes(), |key, value| {
            match decode_full_row(table_schema, key, value) {
                Ok(row) => rows.push(row),
                Err(e) => scan_err = Some(e),
            }
            Ok(())
        }).map_err(SqlError::Storage)?;
        if let Some(e) = scan_err { return Err(e); }
    }

    for row in &rows {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        let key = encode_index_key(&idx_def, row, &pk_values);
        let value = encode_index_value(&idx_def, row, &pk_values);
        let is_new = wtx.table_insert(&idx_table, &key, &value)
            .map_err(SqlError::Storage)?;
        if idx_def.unique && !is_new {
            let indexed_values: Vec<Value> = idx_def.columns.iter()
                .map(|&col_idx| row[col_idx as usize].clone())
                .collect();
            let any_null = indexed_values.iter().any(|v| v.is_null());
            if !any_null {
                return Err(SqlError::UniqueViolation(stmt.index_name.clone()));
            }
        }
    }

    let mut updated_schema = table_schema.clone();
    updated_schema.indices.push(idx_def);
    SchemaManager::save_schema(&mut wtx, &updated_schema)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.register(updated_schema);
    Ok(ExecutionResult::Ok)
}

fn exec_drop_index(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &DropIndexStmt,
) -> Result<ExecutionResult> {
    let lower_idx = stmt.index_name.to_ascii_lowercase();

    let (table_name, _idx_pos) = match find_index_in_schemas(schema, &lower_idx) {
        Some(found) => found,
        None => {
            if stmt.if_exists {
                return Ok(ExecutionResult::Ok);
            }
            return Err(SqlError::IndexNotFound(stmt.index_name.clone()));
        }
    };

    let idx_table = TableSchema::index_table_name(&table_name, &lower_idx);

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    wtx.drop_table(&idx_table).map_err(SqlError::Storage)?;

    let table_schema = schema.get(&table_name).unwrap();
    let mut updated_schema = table_schema.clone();
    updated_schema.indices.retain(|i| i.name != lower_idx);
    SchemaManager::save_schema(&mut wtx, &updated_schema)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.register(updated_schema);
    Ok(ExecutionResult::Ok)
}

fn exec_create_index_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &CreateIndexStmt,
) -> Result<ExecutionResult> {
    let lower_table = stmt.table_name.to_ascii_lowercase();
    let lower_idx = stmt.index_name.to_ascii_lowercase();

    let table_schema = schema.get(&lower_table)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table_name.clone()))?;

    if table_schema.index_by_name(&lower_idx).is_some() {
        if stmt.if_not_exists {
            return Ok(ExecutionResult::Ok);
        }
        return Err(SqlError::IndexAlreadyExists(stmt.index_name.clone()));
    }

    let col_indices: Vec<u16> = stmt.columns.iter().map(|col_name| {
        let lower = col_name.to_ascii_lowercase();
        table_schema.column_index(&lower)
            .map(|i| i as u16)
            .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))
    }).collect::<Result<_>>()?;

    let idx_def = IndexDef {
        name: lower_idx.clone(),
        columns: col_indices,
        unique: stmt.unique,
    };

    let idx_table = TableSchema::index_table_name(&lower_table, &lower_idx);

    SchemaManager::ensure_schema_table(wtx)?;
    wtx.create_table(&idx_table).map_err(SqlError::Storage)?;

    let pk_indices = table_schema.pk_indices();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    {
        let mut scan_err: Option<SqlError> = None;
        wtx.table_for_each(lower_table.as_bytes(), |key, value| {
            match decode_full_row(table_schema, key, value) {
                Ok(row) => rows.push(row),
                Err(e) => scan_err = Some(e),
            }
            Ok(())
        }).map_err(SqlError::Storage)?;
        if let Some(e) = scan_err { return Err(e); }
    }

    for row in &rows {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        let key = encode_index_key(&idx_def, row, &pk_values);
        let value = encode_index_value(&idx_def, row, &pk_values);
        let is_new = wtx.table_insert(&idx_table, &key, &value)
            .map_err(SqlError::Storage)?;
        if idx_def.unique && !is_new {
            let indexed_values: Vec<Value> = idx_def.columns.iter()
                .map(|&col_idx| row[col_idx as usize].clone())
                .collect();
            let any_null = indexed_values.iter().any(|v| v.is_null());
            if !any_null {
                return Err(SqlError::UniqueViolation(stmt.index_name.clone()));
            }
        }
    }

    let mut updated_schema = table_schema.clone();
    updated_schema.indices.push(idx_def);
    SchemaManager::save_schema(wtx, &updated_schema)?;

    schema.register(updated_schema);
    Ok(ExecutionResult::Ok)
}

fn exec_drop_index_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &mut SchemaManager,
    stmt: &DropIndexStmt,
) -> Result<ExecutionResult> {
    let lower_idx = stmt.index_name.to_ascii_lowercase();

    let (table_name, _idx_pos) = match find_index_in_schemas(schema, &lower_idx) {
        Some(found) => found,
        None => {
            if stmt.if_exists {
                return Ok(ExecutionResult::Ok);
            }
            return Err(SqlError::IndexNotFound(stmt.index_name.clone()));
        }
    };

    let idx_table = TableSchema::index_table_name(&table_name, &lower_idx);
    wtx.drop_table(&idx_table).map_err(SqlError::Storage)?;

    let table_schema = schema.get(&table_name).unwrap();
    let mut updated_schema = table_schema.clone();
    updated_schema.indices.retain(|i| i.name != lower_idx);
    SchemaManager::save_schema(wtx, &updated_schema)?;

    schema.register(updated_schema);
    Ok(ExecutionResult::Ok)
}

fn find_index_in_schemas(schema: &SchemaManager, index_name: &str) -> Option<(String, usize)> {
    for table_name in schema.table_names() {
        if let Some(ts) = schema.get(table_name) {
            if let Some(pos) = ts.indices.iter().position(|i| i.name == index_name) {
                return Some((table_name.to_string(), pos));
            }
        }
    }
    None
}

// ── Index scan helpers ───────────────────────────────────────────────

fn extract_pk_key(
    idx_key: &[u8],
    idx_value: &[u8],
    is_unique: bool,
    num_index_cols: usize,
    num_pk_cols: usize,
) -> Result<Vec<u8>> {
    if is_unique && !idx_value.is_empty() {
        Ok(idx_value.to_vec())
    } else {
        let total_cols = num_index_cols + num_pk_cols;
        let all_values = decode_composite_key(idx_key, total_cols)?;
        let pk_values = &all_values[num_index_cols..];
        Ok(encode_composite_key(pk_values))
    }
}

fn check_range_conditions(
    idx_key: &[u8],
    num_prefix_cols: usize,
    range_conds: &[(BinOp, Value)],
    num_index_cols: usize,
) -> Result<RangeCheck> {
    if range_conds.is_empty() {
        return Ok(RangeCheck::Match);
    }

    let num_to_decode = num_prefix_cols + 1;
    if num_to_decode > num_index_cols {
        return Ok(RangeCheck::Match);
    }

    // Decode just enough columns to check the range column
    let mut pos = 0;
    for _ in 0..num_prefix_cols {
        let (_, n) = decode_key_value(&idx_key[pos..])?;
        pos += n;
    }
    let (range_val, _) = decode_key_value(&idx_key[pos..])?;

    let mut exceeds_upper = false;
    let mut below_lower = false;

    for (op, val) in range_conds {
        match op {
            BinOp::Lt => { if range_val >= *val { exceeds_upper = true; } }
            BinOp::LtEq => { if range_val > *val { exceeds_upper = true; } }
            BinOp::Gt => { if range_val <= *val { below_lower = true; } }
            BinOp::GtEq => { if range_val < *val { below_lower = true; } }
            _ => {}
        }
    }

    if exceeds_upper {
        Ok(RangeCheck::ExceedsUpper)
    } else if below_lower {
        Ok(RangeCheck::BelowLower)
    } else {
        Ok(RangeCheck::Match)
    }
}

enum RangeCheck {
    Match,
    BelowLower,
    ExceedsUpper,
}

/// Collect rows via ReadTxn using the scan plan.
fn collect_rows_read(
    db: &Database,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<Vec<Value>>> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;

    match plan {
        ScanPlan::SeqScan => {
            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            let mut scan_err: Option<SqlError> = None;
            rtx.table_for_each(lower_name.as_bytes(), |key, value| {
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => rows.push(row),
                    Err(e) => scan_err = Some(e),
                }
                Ok(())
            }).map_err(SqlError::Storage)?;
            if let Some(e) = scan_err { return Err(e); }
            Ok(rows)
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            let mut rtx = db.begin_read();
            match rtx.table_get(lower_name.as_bytes(), &key).map_err(SqlError::Storage)? {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    Ok(vec![row])
                }
                None => Ok(vec![]),
            }
        }

        ScanPlan::IndexScan {
            idx_table, prefix, num_prefix_cols,
            range_conds, is_unique, index_columns, ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut rtx = db.begin_read();
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols) {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => { scan_err = Some(e); return Ok(false); }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => { scan_err = Some(e); return Ok(false); }
                    }
                    Ok(true)
                }).map_err(SqlError::Storage)?;
                if let Some(e) = scan_err { return Err(e); }
            }

            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            for pk_key in &pk_keys {
                if let Some(value) = rtx.table_get(lower_name.as_bytes(), pk_key).map_err(SqlError::Storage)? {
                    rows.push(decode_full_row(table_schema, pk_key, &value)?);
                }
            }
            Ok(rows)
        }
    }
}

/// Collect rows via WriteTxn using the scan plan.
fn collect_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<Vec<Value>>> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;

    match plan {
        ScanPlan::SeqScan => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            wtx.table_for_each(lower_name.as_bytes(), |key, value| {
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => rows.push(row),
                    Err(e) => scan_err = Some(e),
                }
                Ok(())
            }).map_err(SqlError::Storage)?;
            if let Some(e) = scan_err { return Err(e); }
            Ok(rows)
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            match wtx.table_get(lower_name.as_bytes(), &key).map_err(SqlError::Storage)? {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    Ok(vec![row])
                }
                None => Ok(vec![]),
            }
        }

        ScanPlan::IndexScan {
            idx_table, prefix, num_prefix_cols,
            range_conds, is_unique, index_columns, ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut scan_err: Option<SqlError> = None;
                wtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols) {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => { scan_err = Some(e); return Ok(false); }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => { scan_err = Some(e); return Ok(false); }
                    }
                    Ok(true)
                }).map_err(SqlError::Storage)?;
                if let Some(e) = scan_err { return Err(e); }
            }

            let mut rows = Vec::new();
            for pk_key in &pk_keys {
                if let Some(value) = wtx.table_get(lower_name.as_bytes(), pk_key).map_err(SqlError::Storage)? {
                    rows.push(decode_full_row(table_schema, pk_key, &value)?);
                }
            }
            Ok(rows)
        }
    }
}

/// Collect (encoded_key, full_row) pairs via ReadTxn using the scan plan.
/// Used by DELETE and UPDATE which need the encoded PK key.
fn collect_keyed_rows_read(
    db: &Database,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;

    match plan {
        ScanPlan::SeqScan => {
            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            let mut scan_err: Option<SqlError> = None;
            rtx.table_for_each(lower_name.as_bytes(), |key, value| {
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => scan_err = Some(e),
                }
                Ok(())
            }).map_err(SqlError::Storage)?;
            if let Some(e) = scan_err { return Err(e); }
            Ok(rows)
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            let mut rtx = db.begin_read();
            match rtx.table_get(lower_name.as_bytes(), &key).map_err(SqlError::Storage)? {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    Ok(vec![(key, row)])
                }
                None => Ok(vec![]),
            }
        }

        ScanPlan::IndexScan {
            idx_table, prefix, num_prefix_cols,
            range_conds, is_unique, index_columns, ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut rtx = db.begin_read();
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols) {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => { scan_err = Some(e); return Ok(false); }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => { scan_err = Some(e); return Ok(false); }
                    }
                    Ok(true)
                }).map_err(SqlError::Storage)?;
                if let Some(e) = scan_err { return Err(e); }
            }

            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            for pk_key in &pk_keys {
                if let Some(value) = rtx.table_get(lower_name.as_bytes(), pk_key).map_err(SqlError::Storage)? {
                    rows.push((pk_key.clone(), decode_full_row(table_schema, pk_key, &value)?));
                }
            }
            Ok(rows)
        }
    }
}

/// Collect (encoded_key, full_row) pairs via WriteTxn using the scan plan.
fn collect_keyed_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;

    match plan {
        ScanPlan::SeqScan => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            wtx.table_for_each(lower_name.as_bytes(), |key, value| {
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => scan_err = Some(e),
                }
                Ok(())
            }).map_err(SqlError::Storage)?;
            if let Some(e) = scan_err { return Err(e); }
            Ok(rows)
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            match wtx.table_get(lower_name.as_bytes(), &key).map_err(SqlError::Storage)? {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    Ok(vec![(key, row)])
                }
                None => Ok(vec![]),
            }
        }

        ScanPlan::IndexScan {
            idx_table, prefix, num_prefix_cols,
            range_conds, is_unique, index_columns, ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut scan_err: Option<SqlError> = None;
                wtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols) {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => { scan_err = Some(e); return Ok(false); }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => { scan_err = Some(e); return Ok(false); }
                    }
                    Ok(true)
                }).map_err(SqlError::Storage)?;
                if let Some(e) = scan_err { return Err(e); }
            }

            let mut rows = Vec::new();
            for pk_key in &pk_keys {
                if let Some(value) = wtx.table_get(lower_name.as_bytes(), pk_key).map_err(SqlError::Storage)? {
                    rows.push((pk_key.clone(), decode_full_row(table_schema, pk_key, &value)?));
                }
            }
            Ok(rows)
        }
    }
}

// ── DML ─────────────────────────────────────────────────────────────

fn exec_insert(
    db: &Database,
    schema: &SchemaManager,
    stmt: &InsertStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if insert_has_subquery(stmt) {
        materialized = materialize_insert(stmt, &mut |sub| exec_subquery_read(db, schema, sub))?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let insert_columns = if stmt.columns.is_empty() {
        table_schema.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>()
    } else {
        stmt.columns.iter().map(|c| c.to_ascii_lowercase()).collect()
    };

    let col_indices: Vec<usize> = insert_columns.iter().map(|name| {
        table_schema.column_index(name)
            .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
    }).collect::<Result<_>>()?;

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let mut count: u64 = 0;

    for value_row in &stmt.values {
        if value_row.len() != insert_columns.len() {
            return Err(SqlError::InvalidValue(format!(
                "expected {} values, got {}",
                insert_columns.len(),
                value_row.len()
            )));
        }

        let mut row = vec![Value::Null; table_schema.columns.len()];
        for (i, expr) in value_row.iter().enumerate() {
            let val = eval_const_expr(expr)?;
            let col_idx = col_indices[i];
            let col = &table_schema.columns[col_idx];

            let coerced = if val.is_null() {
                Value::Null
            } else {
                val.coerce_to(col.data_type).ok_or_else(|| SqlError::TypeMismatch {
                    expected: col.data_type.to_string(),
                    got: val.data_type().to_string(),
                })?
            };

            row[col_idx] = coerced;
        }

        for col in &table_schema.columns {
            if !col.nullable && row[col.position as usize].is_null() {
                return Err(SqlError::NotNullViolation(col.name.clone()));
            }
        }

        let pk_values: Vec<Value> = table_schema.pk_indices()
            .iter()
            .map(|&i| row[i].clone())
            .collect();
        let key = encode_composite_key(&pk_values);

        let non_pk = table_schema.non_pk_indices();
        let value_values: Vec<Value> = non_pk.iter().map(|&i| row[i].clone()).collect();
        let value = encode_row(&value_values);

        if key.len() > citadel_core::MAX_KEY_SIZE {
            return Err(SqlError::KeyTooLarge { size: key.len(), max: citadel_core::MAX_KEY_SIZE });
        }
        if value.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
            return Err(SqlError::RowTooLarge { size: value.len(), max: citadel_core::MAX_INLINE_VALUE_SIZE });
        }

        let is_new = wtx.table_insert(lower_name.as_bytes(), &key, &value)
            .map_err(SqlError::Storage)?;
        if !is_new {
            return Err(SqlError::DuplicateKey);
        }

        insert_index_entries(&mut wtx, table_schema, &row, &pk_values)?;
        count += 1;
    }

    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

fn has_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::ScalarSubquery(_) => true,
        Expr::BinaryOp { left, right, .. } => has_subquery(left) || has_subquery(right),
        Expr::UnaryOp { expr, .. } => has_subquery(expr),
        Expr::IsNull(e) | Expr::IsNotNull(e) => has_subquery(e),
        Expr::InList { expr, list, .. } => {
            has_subquery(expr) || list.iter().any(has_subquery)
        }
        Expr::InSet { expr, .. } => has_subquery(expr),
        Expr::Between { expr, low, high, .. } => {
            has_subquery(expr) || has_subquery(low) || has_subquery(high)
        }
        Expr::Like { expr, pattern, escape, .. } => {
            has_subquery(expr) || has_subquery(pattern) || escape.as_ref().map_or(false, |e| has_subquery(e))
        }
        Expr::Case { operand, conditions, else_result } => {
            operand.as_ref().map_or(false, |e| has_subquery(e))
                || conditions.iter().any(|(c, r)| has_subquery(c) || has_subquery(r))
                || else_result.as_ref().map_or(false, |e| has_subquery(e))
        }
        Expr::Coalesce(args) => args.iter().any(has_subquery),
        Expr::Cast { expr, .. } => has_subquery(expr),
        Expr::Function { args, .. } => args.iter().any(has_subquery),
        _ => false,
    }
}

fn stmt_has_subquery(stmt: &SelectStmt) -> bool {
    if let Some(ref w) = stmt.where_clause { if has_subquery(w) { return true; } }
    if let Some(ref h) = stmt.having { if has_subquery(h) { return true; } }
    for col in &stmt.columns {
        if let SelectColumn::Expr { expr, .. } = col {
            if has_subquery(expr) { return true; }
        }
    }
    for ob in &stmt.order_by {
        if has_subquery(&ob.expr) { return true; }
    }
    for join in &stmt.joins {
        if let Some(ref on_expr) = join.on_clause {
            if has_subquery(on_expr) { return true; }
        }
    }
    false
}

fn materialize_expr(
    expr: &Expr,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<Expr> {
    match expr {
        Expr::InSubquery { expr: e, subquery, negated } => {
            let inner = materialize_expr(e, exec_sub)?;
            let qr = exec_sub(subquery)?;
            if !qr.columns.is_empty() && qr.columns.len() != 1 {
                return Err(SqlError::SubqueryMultipleColumns);
            }
            let mut values = std::collections::HashSet::new();
            let mut has_null = false;
            for row in &qr.rows {
                if row[0].is_null() {
                    has_null = true;
                } else {
                    values.insert(row[0].clone());
                }
            }
            Ok(Expr::InSet {
                expr: Box::new(inner),
                values,
                has_null,
                negated: *negated,
            })
        }
        Expr::ScalarSubquery(subquery) => {
            let qr = exec_sub(subquery)?;
            if qr.rows.len() > 1 {
                return Err(SqlError::SubqueryMultipleRows);
            }
            let val = if qr.rows.is_empty() {
                Value::Null
            } else {
                qr.rows[0][0].clone()
            };
            Ok(Expr::Literal(val))
        }
        Expr::Exists { subquery, negated } => {
            let qr = exec_sub(subquery)?;
            let exists = !qr.rows.is_empty();
            let result = if *negated { !exists } else { exists };
            Ok(Expr::Literal(Value::Boolean(result)))
        }
        Expr::InList { expr: e, list, negated } => {
            let inner = materialize_expr(e, exec_sub)?;
            let items = list.iter()
                .map(|item| materialize_expr(item, exec_sub))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::InList { expr: Box::new(inner), list: items, negated: *negated })
        }
        Expr::BinaryOp { left, op, right } => {
            Ok(Expr::BinaryOp {
                left: Box::new(materialize_expr(left, exec_sub)?),
                op: *op,
                right: Box::new(materialize_expr(right, exec_sub)?),
            })
        }
        Expr::UnaryOp { op, expr: e } => {
            Ok(Expr::UnaryOp {
                op: *op,
                expr: Box::new(materialize_expr(e, exec_sub)?),
            })
        }
        Expr::IsNull(e) => Ok(Expr::IsNull(Box::new(materialize_expr(e, exec_sub)?))),
        Expr::IsNotNull(e) => Ok(Expr::IsNotNull(Box::new(materialize_expr(e, exec_sub)?))),
        Expr::InSet { expr: e, values, has_null, negated } => {
            Ok(Expr::InSet {
                expr: Box::new(materialize_expr(e, exec_sub)?),
                values: values.clone(),
                has_null: *has_null,
                negated: *negated,
            })
        }
        Expr::Between { expr: e, low, high, negated } => {
            Ok(Expr::Between {
                expr: Box::new(materialize_expr(e, exec_sub)?),
                low: Box::new(materialize_expr(low, exec_sub)?),
                high: Box::new(materialize_expr(high, exec_sub)?),
                negated: *negated,
            })
        }
        Expr::Like { expr: e, pattern, escape, negated } => {
            let esc = escape.as_ref()
                .map(|es| materialize_expr(es, exec_sub).map(Box::new))
                .transpose()?;
            Ok(Expr::Like {
                expr: Box::new(materialize_expr(e, exec_sub)?),
                pattern: Box::new(materialize_expr(pattern, exec_sub)?),
                escape: esc,
                negated: *negated,
            })
        }
        Expr::Case { operand, conditions, else_result } => {
            let op = operand.as_ref()
                .map(|e| materialize_expr(e, exec_sub).map(Box::new))
                .transpose()?;
            let conds = conditions.iter()
                .map(|(c, r)| Ok((materialize_expr(c, exec_sub)?, materialize_expr(r, exec_sub)?)))
                .collect::<Result<Vec<_>>>()?;
            let else_r = else_result.as_ref()
                .map(|e| materialize_expr(e, exec_sub).map(Box::new))
                .transpose()?;
            Ok(Expr::Case { operand: op, conditions: conds, else_result: else_r })
        }
        Expr::Coalesce(args) => {
            let materialized = args.iter()
                .map(|a| materialize_expr(a, exec_sub))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::Coalesce(materialized))
        }
        Expr::Cast { expr: e, data_type } => {
            Ok(Expr::Cast {
                expr: Box::new(materialize_expr(e, exec_sub)?),
                data_type: *data_type,
            })
        }
        Expr::Function { name, args } => {
            let materialized = args.iter()
                .map(|a| materialize_expr(a, exec_sub))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::Function { name: name.clone(), args: materialized })
        }
        other => Ok(other.clone()),
    }
}

fn materialize_stmt(
    stmt: &SelectStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<SelectStmt> {
    let where_clause = stmt.where_clause.as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    let having = stmt.having.as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    let columns = stmt.columns.iter().map(|c| match c {
        SelectColumn::AllColumns => Ok(SelectColumn::AllColumns),
        SelectColumn::Expr { expr, alias } => {
            Ok(SelectColumn::Expr {
                expr: materialize_expr(expr, exec_sub)?,
                alias: alias.clone(),
            })
        }
    }).collect::<Result<Vec<_>>>()?;
    let order_by = stmt.order_by.iter().map(|ob| {
        Ok(OrderByItem {
            expr: materialize_expr(&ob.expr, exec_sub)?,
            descending: ob.descending,
            nulls_first: ob.nulls_first,
        })
    }).collect::<Result<Vec<_>>>()?;
    let joins = stmt.joins.iter().map(|j| {
        let on_clause = j.on_clause.as_ref()
            .map(|e| materialize_expr(e, exec_sub))
            .transpose()?;
        Ok(JoinClause {
            join_type: j.join_type,
            table: j.table.clone(),
            on_clause,
        })
    }).collect::<Result<Vec<_>>>()?;
    let group_by = stmt.group_by.iter()
        .map(|e| materialize_expr(e, exec_sub))
        .collect::<Result<Vec<_>>>()?;
    Ok(SelectStmt {
        columns,
        from: stmt.from.clone(),
        from_alias: stmt.from_alias.clone(),
        joins,
        distinct: stmt.distinct,
        where_clause,
        order_by,
        limit: stmt.limit.clone(),
        offset: stmt.offset.clone(),
        group_by,
        having,
    })
}

fn exec_subquery_read(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<QueryResult> {
    match exec_select(db, schema, stmt)? {
        ExecutionResult::Query(qr) => Ok(qr),
        _ => Ok(QueryResult { columns: vec![], rows: vec![] }),
    }
}

fn exec_subquery_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<QueryResult> {
    match exec_select_in_txn(wtx, schema, stmt)? {
        ExecutionResult::Query(qr) => Ok(qr),
        _ => Ok(QueryResult { columns: vec![], rows: vec![] }),
    }
}

fn update_has_subquery(stmt: &UpdateStmt) -> bool {
    stmt.where_clause.as_ref().map_or(false, has_subquery)
        || stmt.assignments.iter().any(|(_, e)| has_subquery(e))
}

fn materialize_update(
    stmt: &UpdateStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<UpdateStmt> {
    let where_clause = stmt.where_clause.as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    let assignments = stmt.assignments.iter()
        .map(|(name, expr)| Ok((name.clone(), materialize_expr(expr, exec_sub)?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(UpdateStmt {
        table: stmt.table.clone(),
        assignments,
        where_clause,
    })
}

fn delete_has_subquery(stmt: &DeleteStmt) -> bool {
    stmt.where_clause.as_ref().map_or(false, has_subquery)
}

fn materialize_delete(
    stmt: &DeleteStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<DeleteStmt> {
    let where_clause = stmt.where_clause.as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    Ok(DeleteStmt {
        table: stmt.table.clone(),
        where_clause,
    })
}

fn insert_has_subquery(stmt: &InsertStmt) -> bool {
    stmt.values.iter().any(|row| row.iter().any(has_subquery))
}

fn materialize_insert(
    stmt: &InsertStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<InsertStmt> {
    let values = stmt.values.iter()
        .map(|row| row.iter().map(|e| materialize_expr(e, exec_sub)).collect::<Result<Vec<_>>>())
        .collect::<Result<Vec<_>>>()?;
    Ok(InsertStmt {
        table: stmt.table.clone(),
        columns: stmt.columns.clone(),
        values,
    })
}

fn exec_select(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if stmt_has_subquery(stmt) {
        materialized = materialize_stmt(stmt, &mut |sub| exec_subquery_read(db, schema, sub))?;
        &materialized
    } else {
        stmt
    };

    if stmt.from.is_empty() {
        return exec_select_no_from(stmt);
    }

    let lower_name = stmt.from.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;

    if !stmt.joins.is_empty() {
        return exec_select_join(db, schema, stmt);
    }

    let rows = collect_rows_read(db, table_schema, &stmt.where_clause)?;
    process_select(&table_schema.columns, rows, stmt)
}

fn exec_select_no_from(stmt: &SelectStmt) -> Result<ExecutionResult> {
    let empty_cols: Vec<ColumnDef> = vec![];
    let empty_row: Vec<Value> = vec![];
    let (col_names, projected) = project_rows(&empty_cols, &stmt.columns, &[empty_row])?;
    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: projected,
    }))
}

/// Shared SELECT processing: WHERE, aggregation, ORDER BY, LIMIT/OFFSET, projection.
fn process_select(
    columns: &[ColumnDef],
    mut rows: Vec<Vec<Value>>,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    if let Some(ref where_expr) = stmt.where_clause {
        rows.retain(|row| {
            match eval_expr(where_expr, columns, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            }
        });
    }

    let has_aggregates = stmt.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    });

    if has_aggregates || !stmt.group_by.is_empty() {
        return exec_aggregate(columns, &rows, stmt);
    }

    if stmt.distinct {
        let (col_names, mut projected) = project_rows(columns, &stmt.columns, &rows)?;

        let mut seen = std::collections::HashSet::new();
        projected.retain(|row| seen.insert(row.clone()));

        if !stmt.order_by.is_empty() {
            let output_cols = build_output_columns(&stmt.columns, columns);
            sort_rows(&mut projected, &stmt.order_by, &output_cols)?;
        }

        if let Some(ref offset_expr) = stmt.offset {
            let offset = eval_const_int(offset_expr)? as usize;
            if offset < projected.len() {
                projected = projected.split_off(offset);
            } else {
                projected.clear();
            }
        }

        if let Some(ref limit_expr) = stmt.limit {
            let limit = eval_const_int(limit_expr)? as usize;
            projected.truncate(limit);
        }

        return Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: projected,
        }));
    }

    if !stmt.order_by.is_empty() {
        sort_rows(&mut rows, &stmt.order_by, columns)?;
    }

    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_const_int(offset_expr)? as usize;
        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }
    }

    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_const_int(limit_expr)? as usize;
        rows.truncate(limit);
    }

    let (col_names, projected) = project_rows(columns, &stmt.columns, &rows)?;

    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: projected,
    }))
}


fn resolve_table_name<'a>(
    schema: &'a SchemaManager,
    name: &str,
) -> Result<&'a TableSchema> {
    let lower = name.to_ascii_lowercase();
    schema.get(&lower)
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))
}

fn build_joined_columns(
    tables: &[(String, &TableSchema)],
) -> Vec<ColumnDef> {
    let mut result = Vec::new();
    let mut pos: u16 = 0;

    for (alias, schema) in tables {
        for col in &schema.columns {
            result.push(ColumnDef {
                name: format!("{}.{}", alias.to_ascii_lowercase(), col.name),
                data_type: col.data_type,
                nullable: col.nullable,
                position: pos,
            });
            pos += 1;
        }
    }

    result
}

fn table_alias_or_name(name: &str, alias: &Option<String>) -> String {
    alias.as_ref().unwrap_or(&name.to_string()).to_ascii_lowercase()
}

fn collect_all_rows_read(
    db: &Database,
    table_schema: &TableSchema,
) -> Result<Vec<Vec<Value>>> {
    collect_rows_read(db, table_schema, &None)
}

fn collect_all_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
) -> Result<Vec<Vec<Value>>> {
    collect_rows_write(wtx, table_schema, &None)
}

fn exec_select_join(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let from_schema = resolve_table_name(schema, &stmt.from)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);
    let mut outer_rows = collect_all_rows_read(db, from_schema)?;

    let mut tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), from_schema)];

    for join in &stmt.joins {
        let inner_schema = resolve_table_name(schema, &join.table.name)?;
        let inner_alias = table_alias_or_name(&join.table.name, &join.table.alias);
        let inner_rows = collect_all_rows_read(db, inner_schema)?;

        let mut preview_tables = tables.clone();
        preview_tables.push((inner_alias.clone(), inner_schema));
        let combined_cols = build_joined_columns(&preview_tables);

        let mut new_rows = Vec::new();

        match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                for outer in &outer_rows {
                    for inner in &inner_rows {
                        let combined: Vec<Value> = outer.iter()
                            .chain(inner.iter())
                            .cloned()
                            .collect();
                        if let Some(ref on_expr) = join.on_clause {
                            match eval_expr(on_expr, &combined_cols, &combined) {
                                Ok(val) if is_truthy(&val) => new_rows.push(combined),
                                _ => {}
                            }
                        } else {
                            new_rows.push(combined);
                        }
                    }
                }
            }
            JoinType::Left => {
                let inner_col_count = inner_schema.columns.len();
                for outer in &outer_rows {
                    let mut matched = false;
                    for inner in &inner_rows {
                        let combined: Vec<Value> = outer.iter()
                            .chain(inner.iter())
                            .cloned()
                            .collect();
                        if let Some(ref on_expr) = join.on_clause {
                            match eval_expr(on_expr, &combined_cols, &combined) {
                                Ok(val) if is_truthy(&val) => {
                                    new_rows.push(combined);
                                    matched = true;
                                }
                                _ => {}
                            }
                        } else {
                            new_rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut padded = outer.clone();
                        padded.extend(std::iter::repeat(Value::Null).take(inner_col_count));
                        new_rows.push(padded);
                    }
                }
            }
            JoinType::Right => {
                let outer_col_count = if outer_rows.is_empty() {
                    tables.iter().map(|(_, s)| s.columns.len()).sum()
                } else {
                    outer_rows[0].len()
                };
                let mut inner_matched = vec![false; inner_rows.len()];
                for outer in &outer_rows {
                    for (j, inner) in inner_rows.iter().enumerate() {
                        let combined: Vec<Value> = outer.iter()
                            .chain(inner.iter())
                            .cloned()
                            .collect();
                        if let Some(ref on_expr) = join.on_clause {
                            match eval_expr(on_expr, &combined_cols, &combined) {
                                Ok(val) if is_truthy(&val) => {
                                    new_rows.push(combined);
                                    inner_matched[j] = true;
                                }
                                _ => {}
                            }
                        } else {
                            new_rows.push(combined);
                            inner_matched[j] = true;
                        }
                    }
                }
                for (j, inner) in inner_rows.iter().enumerate() {
                    if !inner_matched[j] {
                        let mut padded: Vec<Value> = std::iter::repeat(Value::Null)
                            .take(outer_col_count)
                            .collect();
                        padded.extend(inner.iter().cloned());
                        new_rows.push(padded);
                    }
                }
            }
        }

        tables.push((inner_alias, inner_schema));
        outer_rows = new_rows;
    }

    let joined_cols = build_joined_columns(&tables);
    process_select(&joined_cols, outer_rows, stmt)
}

fn exec_select_join_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let from_schema = resolve_table_name(schema, &stmt.from)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);
    let mut outer_rows = collect_all_rows_write(wtx, from_schema)?;

    let mut tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), from_schema)];

    for join in &stmt.joins {
        let inner_schema = resolve_table_name(schema, &join.table.name)?;
        let inner_alias = table_alias_or_name(&join.table.name, &join.table.alias);
        let inner_rows = collect_all_rows_write(wtx, inner_schema)?;

        let mut preview_tables = tables.clone();
        preview_tables.push((inner_alias.clone(), inner_schema));
        let combined_cols = build_joined_columns(&preview_tables);

        let mut new_rows = Vec::new();

        match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                for outer in &outer_rows {
                    for inner in &inner_rows {
                        let combined: Vec<Value> = outer.iter()
                            .chain(inner.iter())
                            .cloned()
                            .collect();
                        if let Some(ref on_expr) = join.on_clause {
                            match eval_expr(on_expr, &combined_cols, &combined) {
                                Ok(val) if is_truthy(&val) => new_rows.push(combined),
                                _ => {}
                            }
                        } else {
                            new_rows.push(combined);
                        }
                    }
                }
            }
            JoinType::Left => {
                let inner_col_count = inner_schema.columns.len();
                for outer in &outer_rows {
                    let mut matched = false;
                    for inner in &inner_rows {
                        let combined: Vec<Value> = outer.iter()
                            .chain(inner.iter())
                            .cloned()
                            .collect();
                        if let Some(ref on_expr) = join.on_clause {
                            match eval_expr(on_expr, &combined_cols, &combined) {
                                Ok(val) if is_truthy(&val) => {
                                    new_rows.push(combined);
                                    matched = true;
                                }
                                _ => {}
                            }
                        } else {
                            new_rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut padded = outer.clone();
                        padded.extend(std::iter::repeat(Value::Null).take(inner_col_count));
                        new_rows.push(padded);
                    }
                }
            }
            JoinType::Right => {
                let outer_col_count = if outer_rows.is_empty() {
                    tables.iter().map(|(_, s)| s.columns.len()).sum()
                } else {
                    outer_rows[0].len()
                };
                let mut inner_matched = vec![false; inner_rows.len()];
                for outer in &outer_rows {
                    for (j, inner) in inner_rows.iter().enumerate() {
                        let combined: Vec<Value> = outer.iter()
                            .chain(inner.iter())
                            .cloned()
                            .collect();
                        if let Some(ref on_expr) = join.on_clause {
                            match eval_expr(on_expr, &combined_cols, &combined) {
                                Ok(val) if is_truthy(&val) => {
                                    new_rows.push(combined);
                                    inner_matched[j] = true;
                                }
                                _ => {}
                            }
                        } else {
                            new_rows.push(combined);
                            inner_matched[j] = true;
                        }
                    }
                }
                for (j, inner) in inner_rows.iter().enumerate() {
                    if !inner_matched[j] {
                        let mut padded: Vec<Value> = std::iter::repeat(Value::Null)
                            .take(outer_col_count)
                            .collect();
                        padded.extend(inner.iter().cloned());
                        new_rows.push(padded);
                    }
                }
            }
        }

        tables.push((inner_alias, inner_schema));
        outer_rows = new_rows;
    }

    let joined_cols = build_joined_columns(&tables);
    process_select(&joined_cols, outer_rows, stmt)
}

fn exec_update(
    db: &Database,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if update_has_subquery(stmt) {
        materialized = materialize_update(stmt, &mut |sub| exec_subquery_read(db, schema, sub))?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let all_candidates = collect_keyed_rows_read(db, table_schema, &stmt.where_clause)?;
    let matching_rows: Vec<(Vec<u8>, Vec<Value>)> = all_candidates.into_iter()
        .filter(|(_, row)| {
            match &stmt.where_clause {
                Some(where_expr) => {
                    match eval_expr(where_expr, &table_schema.columns, row) {
                        Ok(val) => is_truthy(&val),
                        Err(_) => false,
                    }
                }
                None => true,
            }
        })
        .collect();

    if matching_rows.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    struct UpdateChange {
        old_key: Vec<u8>,
        new_key: Vec<u8>,
        new_value: Vec<u8>,
        pk_changed: bool,
        old_row: Vec<Value>,
        new_row: Vec<Value>,
    }

    let pk_indices = table_schema.pk_indices();
    let mut changes: Vec<UpdateChange> = Vec::new();

    for (old_key, row) in &matching_rows {
        let mut new_row = row.clone();
        let mut pk_changed = false;
        for (col_name, expr) in &stmt.assignments {
            let col_idx = table_schema.column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let new_val = eval_expr(expr, &table_schema.columns, &new_row)?;
            let col = &table_schema.columns[col_idx];

            let coerced = if new_val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
                Value::Null
            } else {
                new_val.coerce_to(col.data_type).ok_or_else(|| SqlError::TypeMismatch {
                    expected: col.data_type.to_string(),
                    got: new_val.data_type().to_string(),
                })?
            };

            if table_schema.primary_key_columns.contains(&(col_idx as u16)) {
                pk_changed = true;
            }
            new_row[col_idx] = coerced;
        }

        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| new_row[i].clone()).collect();
        let new_key = encode_composite_key(&pk_values);

        let non_pk = table_schema.non_pk_indices();
        let value_values: Vec<Value> = non_pk.iter().map(|&i| new_row[i].clone()).collect();
        let new_value = encode_row(&value_values);

        changes.push(UpdateChange {
            old_key: old_key.clone(), new_key, new_value, pk_changed,
            old_row: row.clone(), new_row,
        });
    }

    {
        use std::collections::HashSet;
        let mut new_keys: HashSet<Vec<u8>> = HashSet::new();
        for c in &changes {
            if c.pk_changed && c.new_key != c.old_key {
                if !new_keys.insert(c.new_key.clone()) {
                    return Err(SqlError::DuplicateKey);
                }
            }
        }
    }

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;

    for c in &changes {
        let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();

        for idx in &table_schema.indices {
            if index_columns_changed(idx, &c.old_row, &c.new_row) || c.pk_changed {
                let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
                let old_idx_key = encode_index_key(idx, &c.old_row, &old_pk);
                wtx.table_delete(&idx_table, &old_idx_key).map_err(SqlError::Storage)?;
            }
        }

        if c.pk_changed {
            wtx.table_delete(lower_name.as_bytes(), &c.old_key).map_err(SqlError::Storage)?;
        }
    }

    for c in &changes {
        let new_pk: Vec<Value> = pk_indices.iter().map(|&i| c.new_row[i].clone()).collect();

        if c.pk_changed {
            let is_new = wtx.table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
            if !is_new {
                return Err(SqlError::DuplicateKey);
            }
        } else {
            wtx.table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
        }

        for idx in &table_schema.indices {
            if index_columns_changed(idx, &c.old_row, &c.new_row) || c.pk_changed {
                let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
                let new_idx_key = encode_index_key(idx, &c.new_row, &new_pk);
                let new_idx_val = encode_index_value(idx, &c.new_row, &new_pk);
                let is_new = wtx.table_insert(&idx_table, &new_idx_key, &new_idx_val)
                    .map_err(SqlError::Storage)?;
                if idx.unique && !is_new {
                    let indexed_values: Vec<Value> = idx.columns.iter()
                        .map(|&col_idx| c.new_row[col_idx as usize].clone())
                        .collect();
                    let any_null = indexed_values.iter().any(|v| v.is_null());
                    if !any_null {
                        return Err(SqlError::UniqueViolation(idx.name.clone()));
                    }
                }
            }
        }
    }

    let count = changes.len() as u64;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

fn exec_delete(
    db: &Database,
    schema: &SchemaManager,
    stmt: &DeleteStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if delete_has_subquery(stmt) {
        materialized = materialize_delete(stmt, &mut |sub| exec_subquery_read(db, schema, sub))?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let all_candidates = collect_keyed_rows_read(db, table_schema, &stmt.where_clause)?;
    let rows_to_delete: Vec<(Vec<u8>, Vec<Value>)> = all_candidates.into_iter()
        .filter(|(_, row)| {
            match &stmt.where_clause {
                Some(where_expr) => {
                    match eval_expr(where_expr, &table_schema.columns, row) {
                        Ok(val) => is_truthy(&val),
                        Err(_) => false,
                    }
                }
                None => true,
            }
        })
        .collect();

    if rows_to_delete.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    let pk_indices = table_schema.pk_indices();
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    for (key, row) in &rows_to_delete {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        delete_index_entries(&mut wtx, table_schema, row, &pk_values)?;
        wtx.table_delete(lower_name.as_bytes(), key).map_err(SqlError::Storage)?;
    }
    let count = rows_to_delete.len() as u64;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

// ── DML (in-transaction) ────────────────────────────────────────────

fn exec_insert_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if insert_has_subquery(stmt) {
        materialized = materialize_insert(stmt, &mut |sub| exec_subquery_write(wtx, schema, sub))?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let insert_columns = if stmt.columns.is_empty() {
        table_schema.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>()
    } else {
        stmt.columns.iter().map(|c| c.to_ascii_lowercase()).collect()
    };

    let col_indices: Vec<usize> = insert_columns.iter().map(|name| {
        table_schema.column_index(name)
            .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
    }).collect::<Result<_>>()?;

    let mut count: u64 = 0;

    for value_row in &stmt.values {
        if value_row.len() != insert_columns.len() {
            return Err(SqlError::InvalidValue(format!(
                "expected {} values, got {}",
                insert_columns.len(),
                value_row.len()
            )));
        }

        let mut row = vec![Value::Null; table_schema.columns.len()];
        for (i, expr) in value_row.iter().enumerate() {
            let val = eval_const_expr(expr)?;
            let col_idx = col_indices[i];
            let col = &table_schema.columns[col_idx];

            let coerced = if val.is_null() {
                Value::Null
            } else {
                val.coerce_to(col.data_type).ok_or_else(|| SqlError::TypeMismatch {
                    expected: col.data_type.to_string(),
                    got: val.data_type().to_string(),
                })?
            };

            row[col_idx] = coerced;
        }

        for col in &table_schema.columns {
            if !col.nullable && row[col.position as usize].is_null() {
                return Err(SqlError::NotNullViolation(col.name.clone()));
            }
        }

        let pk_values: Vec<Value> = table_schema.pk_indices()
            .iter()
            .map(|&i| row[i].clone())
            .collect();
        let key = encode_composite_key(&pk_values);

        let non_pk = table_schema.non_pk_indices();
        let value_values: Vec<Value> = non_pk.iter().map(|&i| row[i].clone()).collect();
        let value = encode_row(&value_values);

        if key.len() > citadel_core::MAX_KEY_SIZE {
            return Err(SqlError::KeyTooLarge { size: key.len(), max: citadel_core::MAX_KEY_SIZE });
        }
        if value.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
            return Err(SqlError::RowTooLarge { size: value.len(), max: citadel_core::MAX_INLINE_VALUE_SIZE });
        }

        let is_new = wtx.table_insert(lower_name.as_bytes(), &key, &value)
            .map_err(SqlError::Storage)?;
        if !is_new {
            return Err(SqlError::DuplicateKey);
        }

        insert_index_entries(wtx, table_schema, &row, &pk_values)?;
        count += 1;
    }

    Ok(ExecutionResult::RowsAffected(count))
}

fn exec_select_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if stmt_has_subquery(stmt) {
        materialized = materialize_stmt(stmt, &mut |sub| exec_subquery_write(wtx, schema, sub))?;
        &materialized
    } else {
        stmt
    };

    if stmt.from.is_empty() {
        return exec_select_no_from(stmt);
    }

    if !stmt.joins.is_empty() {
        return exec_select_join_in_txn(wtx, schema, stmt);
    }

    let lower_name = stmt.from.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;

    let rows = collect_rows_write(wtx, table_schema, &stmt.where_clause)?;
    process_select(&table_schema.columns, rows, stmt)
}

fn exec_update_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if update_has_subquery(stmt) {
        materialized = materialize_update(stmt, &mut |sub| exec_subquery_write(wtx, schema, sub))?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let all_candidates = collect_keyed_rows_write(wtx, table_schema, &stmt.where_clause)?;
    let matching_rows: Vec<(Vec<u8>, Vec<Value>)> = all_candidates.into_iter()
        .filter(|(_, row)| {
            match &stmt.where_clause {
                Some(where_expr) => {
                    match eval_expr(where_expr, &table_schema.columns, row) {
                        Ok(val) => is_truthy(&val),
                        Err(_) => false,
                    }
                }
                None => true,
            }
        })
        .collect();

    if matching_rows.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    struct UpdateChange {
        old_key: Vec<u8>,
        new_key: Vec<u8>,
        new_value: Vec<u8>,
        pk_changed: bool,
        old_row: Vec<Value>,
        new_row: Vec<Value>,
    }

    let pk_indices = table_schema.pk_indices();
    let mut changes: Vec<UpdateChange> = Vec::new();

    for (old_key, row) in &matching_rows {
        let mut new_row = row.clone();
        let mut pk_changed = false;
        for (col_name, expr) in &stmt.assignments {
            let col_idx = table_schema.column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let new_val = eval_expr(expr, &table_schema.columns, &new_row)?;
            let col = &table_schema.columns[col_idx];

            let coerced = if new_val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
                Value::Null
            } else {
                new_val.coerce_to(col.data_type).ok_or_else(|| SqlError::TypeMismatch {
                    expected: col.data_type.to_string(),
                    got: new_val.data_type().to_string(),
                })?
            };

            if table_schema.primary_key_columns.contains(&(col_idx as u16)) {
                pk_changed = true;
            }
            new_row[col_idx] = coerced;
        }

        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| new_row[i].clone()).collect();
        let new_key = encode_composite_key(&pk_values);

        let non_pk = table_schema.non_pk_indices();
        let value_values: Vec<Value> = non_pk.iter().map(|&i| new_row[i].clone()).collect();
        let new_value = encode_row(&value_values);

        changes.push(UpdateChange {
            old_key: old_key.clone(), new_key, new_value, pk_changed,
            old_row: row.clone(), new_row,
        });
    }

    {
        use std::collections::HashSet;
        let mut new_keys: HashSet<Vec<u8>> = HashSet::new();
        for c in &changes {
            if c.pk_changed && c.new_key != c.old_key {
                if !new_keys.insert(c.new_key.clone()) {
                    return Err(SqlError::DuplicateKey);
                }
            }
        }
    }

    for c in &changes {
        let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();

        for idx in &table_schema.indices {
            if index_columns_changed(idx, &c.old_row, &c.new_row) || c.pk_changed {
                let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
                let old_idx_key = encode_index_key(idx, &c.old_row, &old_pk);
                wtx.table_delete(&idx_table, &old_idx_key).map_err(SqlError::Storage)?;
            }
        }

        if c.pk_changed {
            wtx.table_delete(lower_name.as_bytes(), &c.old_key).map_err(SqlError::Storage)?;
        }
    }

    for c in &changes {
        let new_pk: Vec<Value> = pk_indices.iter().map(|&i| c.new_row[i].clone()).collect();

        if c.pk_changed {
            let is_new = wtx.table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
            if !is_new {
                return Err(SqlError::DuplicateKey);
            }
        } else {
            wtx.table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
        }

        for idx in &table_schema.indices {
            if index_columns_changed(idx, &c.old_row, &c.new_row) || c.pk_changed {
                let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
                let new_idx_key = encode_index_key(idx, &c.new_row, &new_pk);
                let new_idx_val = encode_index_value(idx, &c.new_row, &new_pk);
                let is_new = wtx.table_insert(&idx_table, &new_idx_key, &new_idx_val)
                    .map_err(SqlError::Storage)?;
                if idx.unique && !is_new {
                    let indexed_values: Vec<Value> = idx.columns.iter()
                        .map(|&col_idx| c.new_row[col_idx as usize].clone())
                        .collect();
                    let any_null = indexed_values.iter().any(|v| v.is_null());
                    if !any_null {
                        return Err(SqlError::UniqueViolation(idx.name.clone()));
                    }
                }
            }
        }
    }

    let count = changes.len() as u64;
    Ok(ExecutionResult::RowsAffected(count))
}

fn exec_delete_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &DeleteStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if delete_has_subquery(stmt) {
        materialized = materialize_delete(stmt, &mut |sub| exec_subquery_write(wtx, schema, sub))?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let all_candidates = collect_keyed_rows_write(wtx, table_schema, &stmt.where_clause)?;
    let rows_to_delete: Vec<(Vec<u8>, Vec<Value>)> = all_candidates.into_iter()
        .filter(|(_, row)| {
            match &stmt.where_clause {
                Some(where_expr) => {
                    match eval_expr(where_expr, &table_schema.columns, row) {
                        Ok(val) => is_truthy(&val),
                        Err(_) => false,
                    }
                }
                None => true,
            }
        })
        .collect();

    if rows_to_delete.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    let pk_indices = table_schema.pk_indices();
    for (key, row) in &rows_to_delete {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        delete_index_entries(wtx, table_schema, row, &pk_values)?;
        wtx.table_delete(lower_name.as_bytes(), key).map_err(SqlError::Storage)?;
    }
    let count = rows_to_delete.len() as u64;
    Ok(ExecutionResult::RowsAffected(count))
}

// ── Aggregation ─────────────────────────────────────────────────────

fn exec_aggregate(
    columns: &[ColumnDef],
    rows: &[Vec<Value>],
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let groups: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = if stmt.group_by.is_empty() {
        let mut m = BTreeMap::new();
        m.insert(vec![], rows.iter().collect());
        m
    } else {
        let mut m: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = BTreeMap::new();
        for row in rows {
            let group_key: Vec<Value> = stmt.group_by.iter()
                .map(|expr| eval_expr(expr, columns, row))
                .collect::<Result<_>>()?;
            m.entry(group_key).or_default().push(row);
        }
        m
    };

    let mut result_rows = Vec::new();
    let output_cols = build_output_columns(&stmt.columns, columns);

    for (_group_key, group_rows) in &groups {
        let mut result_row = Vec::new();

        for sel_col in &stmt.columns {
            match sel_col {
                SelectColumn::AllColumns => {
                    return Err(SqlError::Unsupported(
                        "SELECT * with GROUP BY".into()
                    ));
                }
                SelectColumn::Expr { expr, .. } => {
                    let val = eval_aggregate_expr(
                        expr, columns, group_rows,
                    )?;
                    result_row.push(val);
                }
            }
        }

        if let Some(ref having) = stmt.having {
            let passes = match eval_aggregate_expr(having, columns, group_rows) {
                Ok(val) => is_truthy(&val),
                Err(SqlError::ColumnNotFound(_)) => {
                    match eval_expr(having, &output_cols, &result_row) {
                        Ok(val) => is_truthy(&val),
                        Err(_) => false,
                    }
                }
                Err(e) => return Err(e),
            };
            if !passes {
                continue;
            }
        }

        result_rows.push(result_row);
    }

    if stmt.distinct {
        let mut seen = std::collections::HashSet::new();
        result_rows.retain(|row| seen.insert(row.clone()));
    }

    if !stmt.order_by.is_empty() {
        let output_cols = build_output_columns(&stmt.columns, columns);
        sort_rows(&mut result_rows, &stmt.order_by, &output_cols)?;
    }

    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_const_int(offset_expr)? as usize;
        if offset < result_rows.len() {
            result_rows = result_rows.split_off(offset);
        } else {
            result_rows.clear();
        }
    }
    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_const_int(limit_expr)? as usize;
        result_rows.truncate(limit);
    }

    let col_names = stmt.columns.iter().map(|c| match c {
        SelectColumn::AllColumns => "*".into(),
        SelectColumn::Expr { alias: Some(a), .. } => a.clone(),
        SelectColumn::Expr { expr, .. } => expr_display_name(expr),
    }).collect();

    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: result_rows,
    }))
}

fn eval_aggregate_expr(
    expr: &Expr,
    columns: &[ColumnDef],
    group_rows: &[&Vec<Value>],
) -> Result<Value> {
    match expr {
        Expr::CountStar => Ok(Value::Integer(group_rows.len() as i64)),

        Expr::Function { name, args } if is_aggregate_function(name, args.len()) => {
            let func = name.to_ascii_uppercase();
            if args.len() != 1 {
                return Err(SqlError::Unsupported(format!(
                    "{func} with {} args", args.len()
                )));
            }
            let arg = &args[0];
            let values: Vec<Value> = group_rows.iter()
                .map(|row| eval_expr(arg, columns, row))
                .collect::<Result<_>>()?;

            match func.as_str() {
                "COUNT" => {
                    let count = values.iter().filter(|v| !v.is_null()).count();
                    Ok(Value::Integer(count as i64))
                }
                "SUM" => {
                    let mut int_sum: i64 = 0;
                    let mut real_sum: f64 = 0.0;
                    let mut has_real = false;
                    let mut all_null = true;
                    for v in &values {
                        match v {
                            Value::Integer(i) => { int_sum += i; all_null = false; }
                            Value::Real(r) => { real_sum += r; has_real = true; all_null = false; }
                            Value::Null => {}
                            _ => return Err(SqlError::TypeMismatch {
                                expected: "numeric".into(),
                                got: v.data_type().to_string(),
                            }),
                        }
                    }
                    if all_null { return Ok(Value::Null); }
                    if has_real {
                        Ok(Value::Real(real_sum + int_sum as f64))
                    } else {
                        Ok(Value::Integer(int_sum))
                    }
                }
                "AVG" => {
                    let mut sum: f64 = 0.0;
                    let mut count: i64 = 0;
                    for v in &values {
                        match v {
                            Value::Integer(i) => { sum += *i as f64; count += 1; }
                            Value::Real(r) => { sum += r; count += 1; }
                            Value::Null => {}
                            _ => return Err(SqlError::TypeMismatch {
                                expected: "numeric".into(),
                                got: v.data_type().to_string(),
                            }),
                        }
                    }
                    if count == 0 { Ok(Value::Null) } else { Ok(Value::Real(sum / count as f64)) }
                }
                "MIN" => {
                    let mut min: Option<&Value> = None;
                    for v in &values {
                        if v.is_null() { continue; }
                        min = Some(match min {
                            None => v,
                            Some(m) => if v < m { v } else { m },
                        });
                    }
                    Ok(min.cloned().unwrap_or(Value::Null))
                }
                "MAX" => {
                    let mut max: Option<&Value> = None;
                    for v in &values {
                        if v.is_null() { continue; }
                        max = Some(match max {
                            None => v,
                            Some(m) => if v > m { v } else { m },
                        });
                    }
                    Ok(max.cloned().unwrap_or(Value::Null))
                }
                _ => Err(SqlError::Unsupported(format!("aggregate function: {func}"))),
            }
        }

        Expr::Column(_) | Expr::QualifiedColumn { .. } => {
            if let Some(first) = group_rows.first() {
                eval_expr(expr, columns, first)
            } else {
                Ok(Value::Null)
            }
        }

        Expr::Literal(v) => Ok(v.clone()),

        Expr::BinaryOp { left, op, right } => {
            let l = eval_aggregate_expr(left, columns, group_rows)?;
            let r = eval_aggregate_expr(right, columns, group_rows)?;
            crate::eval::eval_expr(
                &Expr::BinaryOp {
                    left: Box::new(Expr::Literal(l)),
                    op: *op,
                    right: Box::new(Expr::Literal(r)),
                },
                columns,
                &[],
            )
        }

        Expr::UnaryOp { op, expr: e } => {
            let v = eval_aggregate_expr(e, columns, group_rows)?;
            crate::eval::eval_expr(
                &Expr::UnaryOp { op: *op, expr: Box::new(Expr::Literal(v)) },
                columns, &[],
            )
        }

        Expr::IsNull(e) => {
            let v = eval_aggregate_expr(e, columns, group_rows)?;
            Ok(Value::Boolean(v.is_null()))
        }

        Expr::IsNotNull(e) => {
            let v = eval_aggregate_expr(e, columns, group_rows)?;
            Ok(Value::Boolean(!v.is_null()))
        }

        Expr::Cast { expr: e, data_type } => {
            let v = eval_aggregate_expr(e, columns, group_rows)?;
            crate::eval::eval_expr(
                &Expr::Cast { expr: Box::new(Expr::Literal(v)), data_type: *data_type },
                columns, &[],
            )
        }

        Expr::Case { operand, conditions, else_result } => {
            let op_val = operand.as_ref()
                .map(|e| eval_aggregate_expr(e, columns, group_rows))
                .transpose()?;
            if let Some(ov) = &op_val {
                for (cond, result) in conditions {
                    let cv = eval_aggregate_expr(cond, columns, group_rows)?;
                    if !ov.is_null() && !cv.is_null() && *ov == cv {
                        return eval_aggregate_expr(result, columns, group_rows);
                    }
                }
            } else {
                for (cond, result) in conditions {
                    let cv = eval_aggregate_expr(cond, columns, group_rows)?;
                    if crate::eval::is_truthy(&cv) {
                        return eval_aggregate_expr(result, columns, group_rows);
                    }
                }
            }
            match else_result {
                Some(e) => eval_aggregate_expr(e, columns, group_rows),
                None => Ok(Value::Null),
            }
        }

        Expr::Coalesce(args) => {
            for arg in args {
                let v = eval_aggregate_expr(arg, columns, group_rows)?;
                if !v.is_null() { return Ok(v); }
            }
            Ok(Value::Null)
        }

        Expr::Between { expr: e, low, high, negated } => {
            let v = eval_aggregate_expr(e, columns, group_rows)?;
            let lo = eval_aggregate_expr(low, columns, group_rows)?;
            let hi = eval_aggregate_expr(high, columns, group_rows)?;
            crate::eval::eval_expr(
                &Expr::Between {
                    expr: Box::new(Expr::Literal(v)),
                    low: Box::new(Expr::Literal(lo)),
                    high: Box::new(Expr::Literal(hi)),
                    negated: *negated,
                },
                columns, &[],
            )
        }

        Expr::Like { expr: e, pattern, escape, negated } => {
            let v = eval_aggregate_expr(e, columns, group_rows)?;
            let p = eval_aggregate_expr(pattern, columns, group_rows)?;
            let esc = escape.as_ref()
                .map(|es| eval_aggregate_expr(es, columns, group_rows))
                .transpose()?;
            let esc_box = esc.map(|v| Box::new(Expr::Literal(v)));
            crate::eval::eval_expr(
                &Expr::Like {
                    expr: Box::new(Expr::Literal(v)),
                    pattern: Box::new(Expr::Literal(p)),
                    escape: esc_box,
                    negated: *negated,
                },
                columns, &[],
            )
        }

        Expr::Function { name, args } => {
            let evaluated: Vec<Value> = args.iter()
                .map(|a| eval_aggregate_expr(a, columns, group_rows))
                .collect::<Result<_>>()?;
            let literal_args: Vec<Expr> = evaluated.into_iter().map(Expr::Literal).collect();
            crate::eval::eval_expr(
                &Expr::Function { name: name.clone(), args: literal_args },
                columns, &[],
            )
        }

        _ => Err(SqlError::Unsupported(format!("expression in aggregate: {expr:?}"))),
    }
}

fn is_aggregate_function(name: &str, arg_count: usize) -> bool {
    let u = name.to_ascii_uppercase();
    matches!(u.as_str(), "COUNT" | "SUM" | "AVG")
        || (matches!(u.as_str(), "MIN" | "MAX") && arg_count == 1)
}

fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::CountStar => true,
        Expr::Function { name, args } => {
            is_aggregate_function(name, args.len())
                || args.iter().any(is_aggregate_expr)
        }
        Expr::BinaryOp { left, right, .. } => {
            is_aggregate_expr(left) || is_aggregate_expr(right)
        }
        Expr::UnaryOp { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => is_aggregate_expr(expr),
        Expr::Case { operand, conditions, else_result } => {
            operand.as_ref().map_or(false, |e| is_aggregate_expr(e))
                || conditions.iter().any(|(c, r)| is_aggregate_expr(c) || is_aggregate_expr(r))
                || else_result.as_ref().map_or(false, |e| is_aggregate_expr(e))
        }
        Expr::Coalesce(args) => args.iter().any(is_aggregate_expr),
        Expr::Between { expr, low, high, .. } => {
            is_aggregate_expr(expr) || is_aggregate_expr(low) || is_aggregate_expr(high)
        }
        Expr::Like { expr, pattern, escape, .. } => {
            is_aggregate_expr(expr) || is_aggregate_expr(pattern)
                || escape.as_ref().map_or(false, |e| is_aggregate_expr(e))
        }
        _ => false,
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Decode a full row from B+ tree key + value.
fn decode_full_row(
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
) -> Result<Vec<Value>> {
    let pk_values = decode_composite_key(key, schema.primary_key_columns.len())?;
    let non_pk_values = decode_row(value)?;

    let mut row = vec![Value::Null; schema.columns.len()];

    for (i, &col_idx) in schema.primary_key_columns.iter().enumerate() {
        row[col_idx as usize] = pk_values[i].clone();
    }

    let non_pk = schema.non_pk_indices();
    for (i, &col_idx) in non_pk.iter().enumerate() {
        if i < non_pk_values.len() {
            row[col_idx] = non_pk_values[i].clone();
        }
    }

    Ok(row)
}

/// Evaluate a constant expression (no column references).
fn eval_const_expr(expr: &Expr) -> Result<Value> {
    eval_expr(expr, &[], &[])
}

fn eval_const_int(expr: &Expr) -> Result<i64> {
    match eval_const_expr(expr)? {
        Value::Integer(i) => Ok(i),
        other => Err(SqlError::TypeMismatch {
            expected: "INTEGER".into(),
            got: other.data_type().to_string(),
        }),
    }
}

fn sort_rows(
    rows: &mut [Vec<Value>],
    order_by: &[OrderByItem],
    columns: &[ColumnDef],
) -> Result<()> {
    rows.sort_by(|a, b| {
        for item in order_by {
            let a_val = eval_expr(&item.expr, columns, a).unwrap_or(Value::Null);
            let b_val = eval_expr(&item.expr, columns, b).unwrap_or(Value::Null);

            let nulls_first = item.nulls_first.unwrap_or(!item.descending);

            let ord = match (a_val.is_null(), b_val.is_null()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => {
                    if nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater }
                }
                (false, true) => {
                    if nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less }
                }
                (false, false) => {
                    let cmp = a_val.cmp(&b_val);
                    if item.descending { cmp.reverse() } else { cmp }
                }
            };

            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

fn project_rows(
    columns: &[ColumnDef],
    select_cols: &[SelectColumn],
    rows: &[Vec<Value>],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let mut col_names = Vec::new();
    let mut projectors: Vec<Box<dyn Fn(&[Value]) -> Result<Value>>> = Vec::new();

    for sel_col in select_cols {
        match sel_col {
            SelectColumn::AllColumns => {
                for col in columns {
                    let idx = col.position as usize;
                    col_names.push(col.name.clone());
                    projectors.push(Box::new(move |row: &[Value]| Ok(row[idx].clone())));
                }
            }
            SelectColumn::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                col_names.push(name);
                let expr = expr.clone();
                let owned_cols = columns.to_vec();
                projectors.push(Box::new(move |row: &[Value]| {
                    eval_expr(&expr, &owned_cols, row)
                }));
            }
        }
    }

    let projected = rows.iter().map(|row| {
        projectors.iter().map(|p| p(row)).collect::<Result<Vec<_>>>()
    }).collect::<Result<Vec<_>>>()?;

    Ok((col_names, projected))
}

fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::QualifiedColumn { table, column } => format!("{table}.{column}"),
        Expr::Literal(v) => format!("{v}"),
        Expr::CountStar => "COUNT(*)".into(),
        Expr::Function { name, args } => {
            let arg_strs: Vec<String> = args.iter().map(expr_display_name).collect();
            format!("{name}({})", arg_strs.join(", "))
        }
        Expr::BinaryOp { left, op, right } => {
            format!("{} {} {}", expr_display_name(left), op_symbol(op), expr_display_name(right))
        }
        _ => "?".into(),
    }
}

fn op_symbol(op: &BinOp) -> &'static str {
    match op {
        BinOp::Add => "+", BinOp::Sub => "-", BinOp::Mul => "*",
        BinOp::Div => "/", BinOp::Mod => "%",
        BinOp::Eq => "=", BinOp::NotEq => "<>",
        BinOp::Lt => "<", BinOp::Gt => ">",
        BinOp::LtEq => "<=", BinOp::GtEq => ">=",
        BinOp::And => "AND", BinOp::Or => "OR", BinOp::Concat => "||",
    }
}

fn build_output_columns(
    select_cols: &[SelectColumn],
    columns: &[ColumnDef],
) -> Vec<ColumnDef> {
    let mut out = Vec::new();
    for (i, col) in select_cols.iter().enumerate() {
        let (name, data_type) = match col {
            SelectColumn::AllColumns => (format!("col{i}"), DataType::Null),
            SelectColumn::Expr { alias: Some(a), expr } => {
                (a.clone(), infer_expr_type(expr, columns))
            }
            SelectColumn::Expr { expr, .. } => {
                (expr_display_name(expr), infer_expr_type(expr, columns))
            }
        };
        out.push(ColumnDef {
            name,
            data_type,
            nullable: true,
            position: i as u16,
        });
    }
    out
}

fn infer_expr_type(expr: &Expr, columns: &[ColumnDef]) -> DataType {
    match expr {
        Expr::Column(name) => {
            let lower = name.to_ascii_lowercase();
            columns.iter()
                .find(|c| c.name.to_ascii_lowercase() == lower)
                .map(|c| c.data_type)
                .unwrap_or(DataType::Null)
        }
        Expr::QualifiedColumn { table, column } => {
            let qualified = format!("{}.{}", table.to_ascii_lowercase(), column.to_ascii_lowercase());
            columns.iter()
                .find(|c| c.name.to_ascii_lowercase() == qualified)
                .map(|c| c.data_type)
                .unwrap_or(DataType::Null)
        }
        Expr::Literal(v) => v.data_type(),
        Expr::CountStar => DataType::Integer,
        Expr::Function { name, .. } => {
            match name.to_ascii_uppercase().as_str() {
                "COUNT" => DataType::Integer,
                "AVG" => DataType::Real,
                "SUM" | "MIN" | "MAX" => DataType::Null,
                _ => DataType::Null,
            }
        }
        _ => DataType::Null,
    }
}
