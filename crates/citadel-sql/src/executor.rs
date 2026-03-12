//! SQL executor: DDL and DML operations.

use std::collections::BTreeMap;

use citadel::Database;

use crate::encoding::{decode_composite_key, decode_row, encode_composite_key, encode_row};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

/// Execute a parsed SQL statement.
pub fn execute(
    db: &Database,
    schema: &mut SchemaManager,
    stmt: &Statement,
) -> Result<ExecutionResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table(db, schema, ct),
        Statement::DropTable(dt) => exec_drop_table(db, schema, dt),
        Statement::Insert(ins) => exec_insert(db, schema, ins),
        Statement::Select(sel) => exec_select(db, schema, sel),
        Statement::Update(upd) => exec_update(db, schema, upd),
        Statement::Delete(del) => exec_delete(db, schema, del),
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

    // Check for duplicate columns
    let mut seen = std::collections::HashSet::new();
    for col in &stmt.columns {
        let lower = col.name.to_ascii_lowercase();
        if !seen.insert(lower.clone()) {
            return Err(SqlError::DuplicateColumn(col.name.clone()));
        }
    }

    // Build TableSchema
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
    };

    // Create the data table and persist schema
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

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    wtx.drop_table(lower_name.as_bytes()).map_err(SqlError::Storage)?;
    SchemaManager::delete_schema(&mut wtx, &lower_name)?;
    wtx.commit().map_err(SqlError::Storage)?;

    schema.remove(&lower_name);
    Ok(ExecutionResult::Ok)
}

// ── DML ─────────────────────────────────────────────────────────────

fn exec_insert(
    db: &Database,
    schema: &SchemaManager,
    stmt: &InsertStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    // Determine column ordering
    let insert_columns = if stmt.columns.is_empty() {
        // All columns in schema order
        table_schema.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>()
    } else {
        stmt.columns.iter().map(|c| c.to_ascii_lowercase()).collect()
    };

    // Map insert columns to schema indices
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

        // Build full row (all columns)
        let mut row = vec![Value::Null; table_schema.columns.len()];
        for (i, expr) in value_row.iter().enumerate() {
            let val = eval_const_expr(expr)?;
            let col_idx = col_indices[i];
            let col = &table_schema.columns[col_idx];

            // Type coercion
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

        // Check NOT NULL constraints
        for col in &table_schema.columns {
            if !col.nullable && row[col.position as usize].is_null() {
                return Err(SqlError::NotNullViolation(col.name.clone()));
            }
        }

        // Encode PK and value
        let pk_values: Vec<Value> = table_schema.pk_indices()
            .iter()
            .map(|&i| row[i].clone())
            .collect();
        let key = encode_composite_key(&pk_values);

        let non_pk = table_schema.non_pk_indices();
        let value_values: Vec<Value> = non_pk.iter().map(|&i| row[i].clone()).collect();
        let value = encode_row(&value_values);

        // Check key/value size limits
        if key.len() > citadel_core::MAX_KEY_SIZE {
            return Err(SqlError::KeyTooLarge { size: key.len(), max: citadel_core::MAX_KEY_SIZE });
        }
        if value.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
            return Err(SqlError::RowTooLarge { size: value.len(), max: citadel_core::MAX_INLINE_VALUE_SIZE });
        }

        // Insert, checking for duplicate
        let is_new = wtx.table_insert(lower_name.as_bytes(), &key, &value)
            .map_err(SqlError::Storage)?;
        if !is_new {
            return Err(SqlError::DuplicateKey);
        }
        count += 1;
    }

    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

fn exec_select(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.from.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;

    // Scan all rows
    let mut rows = Vec::new();
    {
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
    }

    // WHERE filter
    if let Some(ref where_expr) = stmt.where_clause {
        rows.retain(|row| {
            match eval_expr(where_expr, &table_schema.columns, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            }
        });
    }

    // Check for aggregation
    let has_aggregates = stmt.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    });

    if has_aggregates || !stmt.group_by.is_empty() {
        return exec_aggregate(table_schema, &rows, stmt);
    }

    // ORDER BY
    if !stmt.order_by.is_empty() {
        sort_rows(&mut rows, &stmt.order_by, &table_schema.columns)?;
    }

    // OFFSET
    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_const_int(offset_expr)? as usize;
        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }
    }

    // LIMIT
    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_const_int(limit_expr)? as usize;
        rows.truncate(limit);
    }

    // Projection
    let (col_names, projected) = project_rows(table_schema, &stmt.columns, &rows)?;

    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: projected,
    }))
}

fn exec_update(
    db: &Database,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    // Scan and collect rows that match WHERE
    let mut matching_rows: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
    {
        let mut rtx = db.begin_read();
        let mut scan_err: Option<SqlError> = None;
        rtx.table_for_each(lower_name.as_bytes(), |key, value| {
            match decode_full_row(table_schema, key, value) {
                Ok(row) => {
                    let matches = match &stmt.where_clause {
                        Some(where_expr) => {
                            match eval_expr(where_expr, &table_schema.columns, &row) {
                                Ok(val) => is_truthy(&val),
                                Err(_) => false,
                            }
                        }
                        None => true,
                    };
                    if matches {
                        matching_rows.push((key.to_vec(), row));
                    }
                }
                Err(e) => scan_err = Some(e),
            }
            Ok(())
        }).map_err(SqlError::Storage)?;
        if let Some(e) = scan_err { return Err(e); }
    }

    if matching_rows.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    // Build all changes first, then validate, then apply
    let mut changes: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, bool)> = Vec::new(); // (old_key, new_key, new_value, pk_changed)

    for (old_key, row) in &matching_rows {
        let mut row = row.clone();
        let mut pk_changed = false;
        for (col_name, expr) in &stmt.assignments {
            let col_idx = table_schema.column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let new_val = eval_expr(expr, &table_schema.columns, &row)?;
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
            row[col_idx] = coerced;
        }

        let pk_values: Vec<Value> = table_schema.pk_indices()
            .iter()
            .map(|&i| row[i].clone())
            .collect();
        let new_key = encode_composite_key(&pk_values);

        let non_pk = table_schema.non_pk_indices();
        let value_values: Vec<Value> = non_pk.iter().map(|&i| row[i].clone()).collect();
        let new_value = encode_row(&value_values);

        changes.push((old_key.clone(), new_key, new_value, pk_changed));
    }

    // Validate: detect PK conflicts before applying any changes
    {
        use std::collections::HashSet;
        let old_keys: HashSet<&[u8]> = changes.iter().map(|(ok, _, _, _)| ok.as_slice()).collect();
        let mut new_keys: HashSet<Vec<u8>> = HashSet::new();
        for (old_key, new_key, _, pk_changed) in &changes {
            if *pk_changed && new_key != old_key {
                // New key must not collide with an existing key that isn't being moved
                if !old_keys.contains(new_key.as_slice()) {
                    // New key doesn't belong to any row being updated — check if it exists in DB
                    // We'll check during apply below
                }
                // New key must not collide with another new key in this batch
                if !new_keys.insert(new_key.clone()) {
                    return Err(SqlError::DuplicateKey);
                }
            }
        }
    }

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;

    // Phase 1: Delete all old keys that are changing PK
    for (old_key, _, _, pk_changed) in &changes {
        if *pk_changed {
            wtx.table_delete(lower_name.as_bytes(), old_key).map_err(SqlError::Storage)?;
        }
    }

    // Phase 2: Insert/update all new keys
    for (_, new_key, new_value, pk_changed) in &changes {
        if *pk_changed {
            let is_new = wtx.table_insert(lower_name.as_bytes(), &new_key, &new_value)
                .map_err(SqlError::Storage)?;
            if !is_new {
                return Err(SqlError::DuplicateKey);
            }
        } else {
            wtx.table_insert(lower_name.as_bytes(), &new_key, &new_value)
                .map_err(SqlError::Storage)?;
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
    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema.get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    // Scan and collect keys that match WHERE
    let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();
    {
        let mut rtx = db.begin_read();
        let mut scan_err: Option<SqlError> = None;
        rtx.table_for_each(lower_name.as_bytes(), |key, value| {
            match decode_full_row(table_schema, key, value) {
                Ok(row) => {
                    let matches = match &stmt.where_clause {
                        Some(where_expr) => {
                            match eval_expr(where_expr, &table_schema.columns, &row) {
                                Ok(val) => is_truthy(&val),
                                Err(_) => false,
                            }
                        }
                        None => true,
                    };
                    if matches {
                        keys_to_delete.push(key.to_vec());
                    }
                }
                Err(e) => scan_err = Some(e),
            }
            Ok(())
        }).map_err(SqlError::Storage)?;
        if let Some(e) = scan_err { return Err(e); }
    }

    if keys_to_delete.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    for key in &keys_to_delete {
        wtx.table_delete(lower_name.as_bytes(), key).map_err(SqlError::Storage)?;
    }
    let count = keys_to_delete.len() as u64;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

// ── Aggregation ─────────────────────────────────────────────────────

fn exec_aggregate(
    table_schema: &TableSchema,
    rows: &[Vec<Value>],
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    // Group rows by GROUP BY keys
    let groups: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = if stmt.group_by.is_empty() {
        // No GROUP BY: all rows are one group
        let mut m = BTreeMap::new();
        m.insert(vec![], rows.iter().collect());
        m
    } else {
        let mut m: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = BTreeMap::new();
        for row in rows {
            let group_key: Vec<Value> = stmt.group_by.iter()
                .map(|expr| eval_expr(expr, &table_schema.columns, row))
                .collect::<Result<_>>()?;
            m.entry(group_key).or_default().push(row);
        }
        m
    };

    let mut result_rows = Vec::new();

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
                        expr, &table_schema.columns, group_rows,
                    )?;
                    result_row.push(val);
                }
            }
        }

        result_rows.push(result_row);
    }

    // HAVING filter
    if let Some(ref having) = stmt.having {
        // For HAVING, we need column defs matching the output columns
        let output_cols = build_output_columns(&stmt.columns, table_schema);
        result_rows.retain(|row| {
            // Re-evaluate HAVING as aggregate expression
            // For simplicity, we evaluate HAVING against the group
            // This requires special handling — for now, we evaluate
            // against the output row using output column defs
            match eval_expr(having, &output_cols, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            }
        });
    }

    // ORDER BY on aggregate results
    if !stmt.order_by.is_empty() {
        let output_cols = build_output_columns(&stmt.columns, table_schema);
        sort_rows(&mut result_rows, &stmt.order_by, &output_cols)?;
    }

    // LIMIT/OFFSET
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

    // Column names
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

        Expr::Function { name, args } => {
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

        // Non-aggregate expression: evaluate against first row in group
        Expr::Column(_) => {
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
                &[], // not used for literals
            )
        }

        _ => Err(SqlError::Unsupported(format!("expression in aggregate: {expr:?}"))),
    }
}

fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::CountStar => true,
        Expr::Function { name, .. } => {
            matches!(name.to_ascii_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
        }
        Expr::BinaryOp { left, right, .. } => {
            is_aggregate_expr(left) || is_aggregate_expr(right)
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

    // Place PK values
    for (i, &col_idx) in schema.primary_key_columns.iter().enumerate() {
        row[col_idx as usize] = pk_values[i].clone();
    }

    // Place non-PK values
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
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::UnaryOp { op: UnaryOp::Neg, expr } => {
            let val = eval_const_expr(expr)?;
            match val {
                Value::Integer(i) => i.checked_neg()
                    .map(Value::Integer)
                    .ok_or(SqlError::IntegerOverflow),
                Value::Real(r) => Ok(Value::Real(-r)),
                _ => Err(SqlError::InvalidValue("cannot negate non-numeric".into())),
            }
        }
        _ => Err(SqlError::InvalidValue(
            "expected constant expression".into(),
        )),
    }
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
    schema: &TableSchema,
    select_cols: &[SelectColumn],
    rows: &[Vec<Value>],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    // Determine output columns
    let mut col_names = Vec::new();
    let mut projectors: Vec<Box<dyn Fn(&[Value]) -> Result<Value>>> = Vec::new();

    for sel_col in select_cols {
        match sel_col {
            SelectColumn::AllColumns => {
                for col in &schema.columns {
                    let idx = col.position as usize;
                    col_names.push(col.name.clone());
                    projectors.push(Box::new(move |row: &[Value]| Ok(row[idx].clone())));
                }
            }
            SelectColumn::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                col_names.push(name);
                let expr = expr.clone();
                let columns = schema.columns.clone();
                projectors.push(Box::new(move |row: &[Value]| {
                    eval_expr(&expr, &columns, row)
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
        BinOp::And => "AND", BinOp::Or => "OR",
    }
}

fn build_output_columns(
    select_cols: &[SelectColumn],
    table_schema: &TableSchema,
) -> Vec<ColumnDef> {
    let mut out = Vec::new();
    for (i, col) in select_cols.iter().enumerate() {
        let name = match col {
            SelectColumn::AllColumns => format!("col{i}"),
            SelectColumn::Expr { alias: Some(a), .. } => a.clone(),
            SelectColumn::Expr { expr, .. } => expr_display_name(expr),
        };
        out.push(ColumnDef {
            name,
            data_type: DataType::Null, // type not important for evaluation
            nullable: true,
            position: i as u16,
        });
    }
    let _ = table_schema;
    out
}
