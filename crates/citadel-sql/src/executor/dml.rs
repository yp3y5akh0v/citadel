use std::cell::RefCell;
use std::sync::Arc;

use citadel::Database;
use citadel_buffer::btree::{UpsertAction, UpsertOutcome};
use citadel_txn::write_txn::WriteTxn;
use rustc_hash::FxHashMap;

use crate::encoding::{encode_composite_key_into, encode_row_into};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::types::*;

use crate::schema::SchemaManager;

use super::compile::CompiledPlan;
use super::helpers::*;
use super::CteContext;

pub(super) fn exec_insert(
    db: &Database,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
) -> Result<ExecutionResult> {
    let empty_ctes = CteContext::default();
    let materialized;
    let stmt = if insert_has_subquery(stmt) {
        materialized = materialize_insert(stmt, &mut |sub| {
            exec_subquery_read(db, schema, sub, &empty_ctes)
        })?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    if schema.get_view(&lower_name).is_some() {
        return Err(SqlError::CannotModifyView(stmt.table.clone()));
    }
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let insert_columns = if stmt.columns.is_empty() {
        table_schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
    } else {
        stmt.columns
            .iter()
            .map(|c| c.to_ascii_lowercase())
            .collect()
    };

    let col_indices: Vec<usize> = insert_columns
        .iter()
        .map(|name| {
            table_schema
                .column_index(name)
                .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
        })
        .collect::<Result<_>>()?;

    let defaults: Vec<(usize, &Expr)> = table_schema
        .columns
        .iter()
        .filter(|c| c.default_expr.is_some() && !col_indices.contains(&(c.position as usize)))
        .map(|c| (c.position as usize, c.default_expr.as_ref().unwrap()))
        .collect();

    // ColumnMap for CHECK evaluation
    let has_checks = table_schema.has_checks();
    let check_col_map = if has_checks {
        Some(ColumnMap::new(&table_schema.columns))
    } else {
        None
    };

    let select_rows = match &stmt.source {
        InsertSource::Select(sq) => {
            let insert_ctes =
                super::materialize_all_ctes(&sq.ctes, sq.recursive, &mut |body, ctx| {
                    exec_query_body_read(db, schema, body, ctx)
                })?;
            let qr = exec_query_body_read(db, schema, &sq.body, &insert_ctes)?;
            Some(qr.rows)
        }
        InsertSource::Values(_) => None,
    };

    let compiled_conflict: Option<Arc<CompiledOnConflict>> = stmt
        .on_conflict
        .as_ref()
        .map(|oc| compile_on_conflict(oc, table_schema).map(Arc::new))
        .transpose()?;

    let row_col_map = compiled_conflict
        .as_ref()
        .map(|_| ColumnMap::new(&table_schema.columns));

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let mut count: u64 = 0;

    let pk_indices = table_schema.pk_indices();
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let phys_count = table_schema.physical_non_pk_count();
    let mut row = vec![Value::Null; table_schema.columns.len()];
    let mut pk_values: Vec<Value> = vec![Value::Null; pk_indices.len()];
    let mut value_values: Vec<Value> = vec![Value::Null; phys_count];
    let mut key_buf: Vec<u8> = Vec::with_capacity(64);
    let mut value_buf: Vec<u8> = Vec::with_capacity(256);
    let mut fk_key_buf: Vec<u8> = Vec::with_capacity(64);

    let values = match &stmt.source {
        InsertSource::Values(rows) => Some(rows.as_slice()),
        InsertSource::Select(_) => None,
    };
    let sel_rows = select_rows.as_deref();

    let total = match (values, sel_rows) {
        (Some(rows), _) => rows.len(),
        (_, Some(rows)) => rows.len(),
        _ => 0,
    };

    if let Some(sel) = sel_rows {
        if !sel.is_empty() && sel[0].len() != insert_columns.len() {
            return Err(SqlError::InvalidValue(format!(
                "INSERT ... SELECT column count mismatch: expected {}, got {}",
                insert_columns.len(),
                sel[0].len()
            )));
        }
    }

    for idx in 0..total {
        for v in row.iter_mut() {
            *v = Value::Null;
        }

        if let Some(value_rows) = values {
            let value_row = &value_rows[idx];
            if value_row.len() != insert_columns.len() {
                return Err(SqlError::InvalidValue(format!(
                    "expected {} values, got {}",
                    insert_columns.len(),
                    value_row.len()
                )));
            }
            for (i, expr) in value_row.iter().enumerate() {
                let val = if let Expr::Parameter(n) = expr {
                    params
                        .get(n - 1)
                        .cloned()
                        .ok_or_else(|| SqlError::Parse(format!("unbound parameter ${n}")))?
                } else {
                    eval_const_expr(expr)?
                };
                let col_idx = col_indices[i];
                let col = &table_schema.columns[col_idx];
                let got_type = val.data_type();
                row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    val.coerce_into(col.data_type)
                        .ok_or_else(|| SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        })?
                };
            }
        } else if let Some(sel) = sel_rows {
            let sel_row = &sel[idx];
            for (i, val) in sel_row.iter().enumerate() {
                let col_idx = col_indices[i];
                let col = &table_schema.columns[col_idx];
                let got_type = val.data_type();
                row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    val.clone().coerce_into(col.data_type).ok_or_else(|| {
                        SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        }
                    })?
                };
            }
        }

        for &(pos, def_expr) in &defaults {
            let val = eval_const_expr(def_expr)?;
            let col = &table_schema.columns[pos];
            if val.is_null() {
                // row[pos] already Null from init
            } else {
                let got_type = val.data_type();
                row[pos] =
                    val.coerce_into(col.data_type)
                        .ok_or_else(|| SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        })?;
            }
        }

        for col in &table_schema.columns {
            if !col.nullable && row[col.position as usize].is_null() {
                return Err(SqlError::NotNullViolation(col.name.clone()));
            }
        }

        if let Some(ref col_map) = check_col_map {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(check, &EvalCtx::new(col_map, &row))?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, &EvalCtx::new(col_map, &row))?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        for fk in &table_schema.foreign_keys {
            let any_null = fk.columns.iter().any(|&ci| row[ci as usize].is_null());
            if any_null {
                continue; // MATCH SIMPLE: skip if any FK col is NULL
            }
            let fk_vals: Vec<Value> = fk
                .columns
                .iter()
                .map(|&ci| row[ci as usize].clone())
                .collect();
            fk_key_buf.clear();
            encode_composite_key_into(&fk_vals, &mut fk_key_buf);
            let found = wtx
                .table_get(fk.foreign_table.as_bytes(), &fk_key_buf)
                .map_err(SqlError::Storage)?;
            if found.is_none() {
                let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                return Err(SqlError::ForeignKeyViolation(name.to_string()));
            }
        }

        for (j, &i) in pk_indices.iter().enumerate() {
            pk_values[j] = std::mem::replace(&mut row[i], Value::Null);
        }
        encode_composite_key_into(&pk_values, &mut key_buf);

        for (j, &i) in non_pk.iter().enumerate() {
            value_values[enc_pos[j] as usize] = std::mem::replace(&mut row[i], Value::Null);
        }
        encode_row_into(&value_values, &mut value_buf);

        if key_buf.len() > citadel_core::MAX_KEY_SIZE {
            return Err(SqlError::KeyTooLarge {
                size: key_buf.len(),
                max: citadel_core::MAX_KEY_SIZE,
            });
        }
        if value_buf.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
            return Err(SqlError::RowTooLarge {
                size: value_buf.len(),
                max: citadel_core::MAX_INLINE_VALUE_SIZE,
            });
        }

        match compiled_conflict.as_ref() {
            None => {
                let is_new = wtx
                    .table_insert(stmt.table.as_bytes(), &key_buf, &value_buf)
                    .map_err(SqlError::Storage)?;
                if !is_new {
                    return Err(SqlError::DuplicateKey);
                }
                if !table_schema.indices.is_empty() {
                    for (j, &i) in pk_indices.iter().enumerate() {
                        row[i] = pk_values[j].clone();
                    }
                    for (j, &i) in non_pk.iter().enumerate() {
                        row[i] =
                            std::mem::replace(&mut value_values[enc_pos[j] as usize], Value::Null);
                    }
                    insert_index_entries(&mut wtx, table_schema, &row, &pk_values)?;
                }
                count += 1;
            }
            Some(oc) => {
                let oc_ref: &CompiledOnConflict = oc;
                let needs_row = upsert_needs_row(oc_ref, table_schema);
                if needs_row {
                    for (j, &i) in pk_indices.iter().enumerate() {
                        row[i] = pk_values[j].clone();
                    }
                    for (j, &i) in non_pk.iter().enumerate() {
                        row[i] =
                            std::mem::replace(&mut value_values[enc_pos[j] as usize], Value::Null);
                    }
                }
                match apply_insert_with_conflict(
                    &mut wtx,
                    table_schema,
                    &key_buf,
                    &value_buf,
                    &row,
                    &pk_values,
                    oc_ref,
                    row_col_map.as_ref().unwrap(),
                )? {
                    InsertRowOutcome::Inserted => count += 1,
                    InsertRowOutcome::Skipped => {}
                }
            }
        }
    }

    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

pub(super) fn has_subquery(expr: &Expr) -> bool {
    crate::parser::has_subquery(expr)
}

pub(super) fn stmt_has_subquery(stmt: &SelectStmt) -> bool {
    if let Some(ref w) = stmt.where_clause {
        if has_subquery(w) {
            return true;
        }
    }
    if let Some(ref h) = stmt.having {
        if has_subquery(h) {
            return true;
        }
    }
    for col in &stmt.columns {
        if let SelectColumn::Expr { expr, .. } = col {
            if has_subquery(expr) {
                return true;
            }
        }
    }
    for ob in &stmt.order_by {
        if has_subquery(&ob.expr) {
            return true;
        }
    }
    for join in &stmt.joins {
        if let Some(ref on_expr) = join.on_clause {
            if has_subquery(on_expr) {
                return true;
            }
        }
    }
    false
}

pub(super) fn materialize_expr(
    expr: &Expr,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<Expr> {
    match expr {
        Expr::InSubquery {
            expr: e,
            subquery,
            negated,
        } => {
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
        Expr::InList {
            expr: e,
            list,
            negated,
        } => {
            let inner = materialize_expr(e, exec_sub)?;
            let items = list
                .iter()
                .map(|item| materialize_expr(item, exec_sub))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::InList {
                expr: Box::new(inner),
                list: items,
                negated: *negated,
            })
        }
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(materialize_expr(left, exec_sub)?),
            op: *op,
            right: Box::new(materialize_expr(right, exec_sub)?),
        }),
        Expr::UnaryOp { op, expr: e } => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(materialize_expr(e, exec_sub)?),
        }),
        Expr::IsNull(e) => Ok(Expr::IsNull(Box::new(materialize_expr(e, exec_sub)?))),
        Expr::IsNotNull(e) => Ok(Expr::IsNotNull(Box::new(materialize_expr(e, exec_sub)?))),
        Expr::InSet {
            expr: e,
            values,
            has_null,
            negated,
        } => Ok(Expr::InSet {
            expr: Box::new(materialize_expr(e, exec_sub)?),
            values: values.clone(),
            has_null: *has_null,
            negated: *negated,
        }),
        Expr::Between {
            expr: e,
            low,
            high,
            negated,
        } => Ok(Expr::Between {
            expr: Box::new(materialize_expr(e, exec_sub)?),
            low: Box::new(materialize_expr(low, exec_sub)?),
            high: Box::new(materialize_expr(high, exec_sub)?),
            negated: *negated,
        }),
        Expr::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => {
            let esc = escape
                .as_ref()
                .map(|es| materialize_expr(es, exec_sub).map(Box::new))
                .transpose()?;
            Ok(Expr::Like {
                expr: Box::new(materialize_expr(e, exec_sub)?),
                pattern: Box::new(materialize_expr(pattern, exec_sub)?),
                escape: esc,
                negated: *negated,
            })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let op = operand
                .as_ref()
                .map(|e| materialize_expr(e, exec_sub).map(Box::new))
                .transpose()?;
            let conds = conditions
                .iter()
                .map(|(c, r)| {
                    Ok((
                        materialize_expr(c, exec_sub)?,
                        materialize_expr(r, exec_sub)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_r = else_result
                .as_ref()
                .map(|e| materialize_expr(e, exec_sub).map(Box::new))
                .transpose()?;
            Ok(Expr::Case {
                operand: op,
                conditions: conds,
                else_result: else_r,
            })
        }
        Expr::Coalesce(args) => {
            let materialized = args
                .iter()
                .map(|a| materialize_expr(a, exec_sub))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::Coalesce(materialized))
        }
        Expr::Cast { expr: e, data_type } => Ok(Expr::Cast {
            expr: Box::new(materialize_expr(e, exec_sub)?),
            data_type: *data_type,
        }),
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            let materialized = args
                .iter()
                .map(|a| materialize_expr(a, exec_sub))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::Function {
                name: name.clone(),
                args: materialized,
                distinct: *distinct,
            })
        }
        other => Ok(other.clone()),
    }
}

pub(super) fn materialize_stmt(
    stmt: &SelectStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<SelectStmt> {
    let where_clause = stmt
        .where_clause
        .as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    let having = stmt
        .having
        .as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    let columns = stmt
        .columns
        .iter()
        .map(|c| match c {
            SelectColumn::AllColumns => Ok(SelectColumn::AllColumns),
            SelectColumn::Expr { expr, alias } => Ok(SelectColumn::Expr {
                expr: materialize_expr(expr, exec_sub)?,
                alias: alias.clone(),
            }),
        })
        .collect::<Result<Vec<_>>>()?;
    let order_by = stmt
        .order_by
        .iter()
        .map(|ob| {
            Ok(OrderByItem {
                expr: materialize_expr(&ob.expr, exec_sub)?,
                descending: ob.descending,
                nulls_first: ob.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let joins = stmt
        .joins
        .iter()
        .map(|j| {
            let on_clause = j
                .on_clause
                .as_ref()
                .map(|e| materialize_expr(e, exec_sub))
                .transpose()?;
            Ok(JoinClause {
                join_type: j.join_type,
                table: j.table.clone(),
                on_clause,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let group_by = stmt
        .group_by
        .iter()
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

pub(super) fn exec_subquery_read(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<QueryResult> {
    match super::exec_select(db, schema, stmt, ctes)? {
        ExecutionResult::Query(qr) => Ok(qr),
        _ => Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        }),
    }
}

pub(super) fn exec_subquery_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<QueryResult> {
    match super::exec_select_in_txn(wtx, schema, stmt, ctes)? {
        ExecutionResult::Query(qr) => Ok(qr),
        _ => Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        }),
    }
}

pub(super) fn update_has_subquery(stmt: &UpdateStmt) -> bool {
    stmt.where_clause.as_ref().is_some_and(has_subquery)
        || stmt.assignments.iter().any(|(_, e)| has_subquery(e))
}

pub(super) fn materialize_update(
    stmt: &UpdateStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<UpdateStmt> {
    let where_clause = stmt
        .where_clause
        .as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    let assignments = stmt
        .assignments
        .iter()
        .map(|(name, expr)| Ok((name.clone(), materialize_expr(expr, exec_sub)?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(UpdateStmt {
        table: stmt.table.clone(),
        assignments,
        where_clause,
    })
}

pub(super) fn delete_has_subquery(stmt: &DeleteStmt) -> bool {
    stmt.where_clause.as_ref().is_some_and(has_subquery)
}

pub(super) fn materialize_delete(
    stmt: &DeleteStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<DeleteStmt> {
    let where_clause = stmt
        .where_clause
        .as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    Ok(DeleteStmt {
        table: stmt.table.clone(),
        where_clause,
    })
}

pub(super) fn insert_has_subquery(stmt: &InsertStmt) -> bool {
    match &stmt.source {
        InsertSource::Values(rows) => rows.iter().any(|row| row.iter().any(has_subquery)),
        // SELECT source subqueries are handled by exec_select's correlated/non-correlated paths
        InsertSource::Select(_) => false,
    }
}

pub(super) fn materialize_insert(
    stmt: &InsertStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<InsertStmt> {
    let source = match &stmt.source {
        InsertSource::Values(rows) => {
            let mat = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|e| materialize_expr(e, exec_sub))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            InsertSource::Values(mat)
        }
        InsertSource::Select(sq) => {
            let ctes = sq
                .ctes
                .iter()
                .map(|c| {
                    Ok(CteDefinition {
                        name: c.name.clone(),
                        column_aliases: c.column_aliases.clone(),
                        body: materialize_query_body(&c.body, exec_sub)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let body = materialize_query_body(&sq.body, exec_sub)?;
            InsertSource::Select(Box::new(SelectQuery {
                ctes,
                recursive: sq.recursive,
                body,
            }))
        }
    };
    Ok(InsertStmt {
        table: stmt.table.clone(),
        columns: stmt.columns.clone(),
        source,
        on_conflict: stmt.on_conflict.clone(),
    })
}

pub(super) fn materialize_query_body(
    body: &QueryBody,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<QueryBody> {
    match body {
        QueryBody::Select(sel) => Ok(QueryBody::Select(Box::new(materialize_stmt(
            sel, exec_sub,
        )?))),
        QueryBody::Compound(comp) => Ok(QueryBody::Compound(Box::new(CompoundSelect {
            op: comp.op.clone(),
            all: comp.all,
            left: Box::new(materialize_query_body(&comp.left, exec_sub)?),
            right: Box::new(materialize_query_body(&comp.right, exec_sub)?),
            order_by: comp.order_by.clone(),
            limit: comp.limit.clone(),
            offset: comp.offset.clone(),
        }))),
    }
}

pub(super) fn exec_query_body(
    db: &Database,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    match body {
        QueryBody::Select(sel) => super::exec_select(db, schema, sel, ctes),
        QueryBody::Compound(comp) => exec_compound_select(db, schema, comp, ctes),
    }
}

pub(super) fn exec_query_body_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    match body {
        QueryBody::Select(sel) => super::exec_select_in_txn(wtx, schema, sel, ctes),
        QueryBody::Compound(comp) => exec_compound_select_in_txn(wtx, schema, comp, ctes),
    }
}

pub(super) fn exec_query_body_read(
    db: &Database,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<QueryResult> {
    match exec_query_body(db, schema, body, ctes)? {
        ExecutionResult::Query(qr) => Ok(qr),
        _ => Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        }),
    }
}

pub(super) fn exec_query_body_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<QueryResult> {
    match exec_query_body_in_txn(wtx, schema, body, ctes)? {
        ExecutionResult::Query(qr) => Ok(qr),
        _ => Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        }),
    }
}

pub(super) fn exec_compound_select(
    db: &Database,
    schema: &SchemaManager,
    comp: &CompoundSelect,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let left_qr = match exec_query_body(db, schema, &comp.left, ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => QueryResult {
            columns: vec![],
            rows: vec![],
        },
    };
    let right_qr = match exec_query_body(db, schema, &comp.right, ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => QueryResult {
            columns: vec![],
            rows: vec![],
        },
    };
    apply_set_operation(comp, left_qr, right_qr)
}

pub(super) fn exec_compound_select_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    comp: &CompoundSelect,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let left_qr = match exec_query_body_in_txn(wtx, schema, &comp.left, ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => QueryResult {
            columns: vec![],
            rows: vec![],
        },
    };
    let right_qr = match exec_query_body_in_txn(wtx, schema, &comp.right, ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => QueryResult {
            columns: vec![],
            rows: vec![],
        },
    };
    apply_set_operation(comp, left_qr, right_qr)
}

pub(super) fn apply_set_operation(
    comp: &CompoundSelect,
    left_qr: QueryResult,
    right_qr: QueryResult,
) -> Result<ExecutionResult> {
    if !left_qr.columns.is_empty()
        && !right_qr.columns.is_empty()
        && left_qr.columns.len() != right_qr.columns.len()
    {
        return Err(SqlError::CompoundColumnCountMismatch {
            left: left_qr.columns.len(),
            right: right_qr.columns.len(),
        });
    }

    let columns = left_qr.columns;

    let mut rows = match (&comp.op, comp.all) {
        (SetOp::Union, true) => {
            let mut rows = left_qr.rows;
            rows.extend(right_qr.rows);
            rows
        }
        (SetOp::Union, false) => {
            let mut seen: std::collections::HashSet<Vec<Value>> = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for row in left_qr.rows.into_iter().chain(right_qr.rows) {
                if !seen.contains(&row) {
                    seen.insert(row.clone());
                    rows.push(row);
                }
            }
            rows
        }
        (SetOp::Intersect, true) => {
            let mut right_counts: FxHashMap<Vec<Value>, usize> = FxHashMap::default();
            for row in &right_qr.rows {
                *right_counts.entry(row.clone()).or_insert(0) += 1;
            }
            let mut rows = Vec::new();
            for row in left_qr.rows {
                if let Some(count) = right_counts.get_mut(&row) {
                    if *count > 0 {
                        *count -= 1;
                        rows.push(row);
                    }
                }
            }
            rows
        }
        (SetOp::Intersect, false) => {
            let right_set: std::collections::HashSet<Vec<Value>> =
                right_qr.rows.into_iter().collect();
            let mut seen: std::collections::HashSet<Vec<Value>> = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for row in left_qr.rows {
                if right_set.contains(&row) && !seen.contains(&row) {
                    seen.insert(row.clone());
                    rows.push(row);
                }
            }
            rows
        }
        (SetOp::Except, true) => {
            let mut right_counts: FxHashMap<Vec<Value>, usize> = FxHashMap::default();
            for row in &right_qr.rows {
                *right_counts.entry(row.clone()).or_insert(0) += 1;
            }
            let mut rows = Vec::new();
            for row in left_qr.rows {
                if let Some(count) = right_counts.get_mut(&row) {
                    if *count > 0 {
                        *count -= 1;
                        continue;
                    }
                }
                rows.push(row);
            }
            rows
        }
        (SetOp::Except, false) => {
            let right_set: std::collections::HashSet<Vec<Value>> =
                right_qr.rows.into_iter().collect();
            let mut seen: std::collections::HashSet<Vec<Value>> = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for row in left_qr.rows {
                if !right_set.contains(&row) && !seen.contains(&row) {
                    seen.insert(row.clone());
                    rows.push(row);
                }
            }
            rows
        }
    };

    if !comp.order_by.is_empty() {
        let col_defs: Vec<crate::types::ColumnDef> = columns
            .iter()
            .enumerate()
            .map(|(i, name)| crate::types::ColumnDef {
                name: name.clone(),
                data_type: crate::types::DataType::Null,
                nullable: true,
                position: i as u16,
                default_expr: None,
                default_sql: None,
                check_expr: None,
                check_sql: None,
                check_name: None,
                is_with_timezone: false,
            })
            .collect();
        sort_rows(&mut rows, &comp.order_by, &col_defs)?;
    }

    if let Some(ref offset_expr) = comp.offset {
        let offset = eval_const_int(offset_expr)?.max(0) as usize;
        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }
    }

    if let Some(ref limit_expr) = comp.limit {
        let limit = eval_const_int(limit_expr)?.max(0) as usize;
        rows.truncate(limit);
    }

    Ok(ExecutionResult::Query(QueryResult { columns, rows }))
}

struct InsertBufs {
    row: Vec<Value>,
    pk_values: Vec<Value>,
    value_values: Vec<Value>,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
    col_indices: Vec<usize>,
    fk_key_buf: Vec<u8>,
}

impl InsertBufs {
    fn new() -> Self {
        Self {
            row: Vec::new(),
            pk_values: Vec::new(),
            value_values: Vec::new(),
            key_buf: Vec::with_capacity(64),
            value_buf: Vec::with_capacity(256),
            col_indices: Vec::new(),
            fk_key_buf: Vec::with_capacity(64),
        }
    }
}

thread_local! {
    static INSERT_SCRATCH: RefCell<InsertBufs> = RefCell::new(InsertBufs::new());
    static UPSERT_SCRATCH: RefCell<UpsertBufs> = RefCell::new(UpsertBufs::new());
}

fn with_insert_scratch<R>(f: impl FnOnce(&mut InsertBufs) -> R) -> R {
    INSERT_SCRATCH.with(|slot| f(&mut slot.borrow_mut()))
}

pub(super) struct UpsertBufs {
    old_row: Vec<Value>,
    new_row: Vec<Value>,
    value_values: Vec<Value>,
    new_value_buf: Vec<u8>,
}

impl UpsertBufs {
    pub(super) fn new() -> Self {
        Self {
            old_row: Vec::new(),
            new_row: Vec::new(),
            value_values: Vec::new(),
            new_value_buf: Vec::with_capacity(256),
        }
    }
}

pub fn exec_insert_in_txn(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
) -> Result<ExecutionResult> {
    with_insert_scratch(|bufs| exec_insert_in_txn_impl(wtx, schema, stmt, params, bufs, None))
}

fn exec_insert_in_txn_cached(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
    cache: &InsertCache,
) -> Result<ExecutionResult> {
    with_insert_scratch(|bufs| {
        exec_insert_in_txn_impl(wtx, schema, stmt, params, bufs, Some(cache))
    })
}

fn exec_insert_in_txn_impl(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
    bufs: &mut InsertBufs,
    cache: Option<&InsertCache>,
) -> Result<ExecutionResult> {
    let empty_ctes = CteContext::default();
    let materialized;
    let has_sub = match cache {
        Some(c) => c.has_subquery,
        None => insert_has_subquery(stmt),
    };
    let stmt = if has_sub {
        materialized = materialize_insert(stmt, &mut |sub| {
            exec_subquery_write(wtx, schema, sub, &empty_ctes)
        })?;
        &materialized
    } else {
        stmt
    };

    let table_schema = schema
        .get(&stmt.table)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let default_columns;
    let insert_columns: &[String] = if stmt.columns.is_empty() {
        default_columns = table_schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        &default_columns
    } else {
        &stmt.columns
    };

    bufs.col_indices.clear();
    if let Some(c) = cache {
        bufs.col_indices.extend_from_slice(&c.col_indices);
    } else {
        for name in insert_columns {
            bufs.col_indices.push(
                table_schema
                    .column_index(name)
                    .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))?,
            );
        }
    }

    let any_defaults = match cache {
        Some(c) => c.any_defaults,
        None => table_schema
            .columns
            .iter()
            .any(|c| c.default_expr.is_some()),
    };
    let defaults: Vec<(usize, &Expr)> = if any_defaults {
        table_schema
            .columns
            .iter()
            .filter(|c| {
                c.default_expr.is_some() && !bufs.col_indices.contains(&(c.position as usize))
            })
            .map(|c| (c.position as usize, c.default_expr.as_ref().unwrap()))
            .collect()
    } else {
        Vec::new()
    };

    let has_checks = match cache {
        Some(c) => c.has_checks,
        None => table_schema.has_checks(),
    };
    let check_col_map = if has_checks {
        Some(ColumnMap::new(&table_schema.columns))
    } else {
        None
    };

    let pk_indices = table_schema.pk_indices();
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let phys_count = table_schema.physical_non_pk_count();
    let dropped = table_schema.dropped_non_pk_slots();

    bufs.row.resize(table_schema.columns.len(), Value::Null);
    bufs.pk_values.resize(pk_indices.len(), Value::Null);
    bufs.value_values.resize(phys_count, Value::Null);

    let table_bytes = stmt.table.as_bytes();
    let has_fks = !table_schema.foreign_keys.is_empty();
    let has_indices = !table_schema.indices.is_empty();
    let has_defaults = !defaults.is_empty();

    let compiled_conflict: Option<Arc<CompiledOnConflict>> = match (cache, &stmt.on_conflict) {
        (Some(c), Some(_)) if c.on_conflict.is_some() => c.on_conflict.clone(),
        (_, Some(oc)) => Some(Arc::new(compile_on_conflict(oc, table_schema)?)),
        (_, None) => None,
    };

    let row_col_map_owned: Option<ColumnMap> =
        if compiled_conflict.is_some() && cache.and_then(|c| c.row_col_map.as_ref()).is_none() {
            Some(ColumnMap::new(&table_schema.columns))
        } else {
            None
        };
    let row_col_map: Option<&ColumnMap> = cache
        .and_then(|c| c.row_col_map.as_ref())
        .or(row_col_map_owned.as_ref());

    let select_rows = match &stmt.source {
        InsertSource::Select(sq) => {
            let insert_ctes =
                super::materialize_all_ctes(&sq.ctes, sq.recursive, &mut |body, ctx| {
                    exec_query_body_write(wtx, schema, body, ctx)
                })?;
            let qr = exec_query_body_write(wtx, schema, &sq.body, &insert_ctes)?;
            Some(qr.rows)
        }
        InsertSource::Values(_) => None,
    };

    let mut count: u64 = 0;

    let values = match &stmt.source {
        InsertSource::Values(rows) => Some(rows.as_slice()),
        InsertSource::Select(_) => None,
    };
    let sel_rows = select_rows.as_deref();

    let total = match (values, sel_rows) {
        (Some(rows), _) => rows.len(),
        (_, Some(rows)) => rows.len(),
        _ => 0,
    };

    if let Some(sel) = sel_rows {
        if !sel.is_empty() && sel[0].len() != insert_columns.len() {
            return Err(SqlError::InvalidValue(format!(
                "INSERT ... SELECT column count mismatch: expected {}, got {}",
                insert_columns.len(),
                sel[0].len()
            )));
        }
    }

    for idx in 0..total {
        for v in bufs.row.iter_mut() {
            *v = Value::Null;
        }

        if let Some(value_rows) = values {
            let value_row = &value_rows[idx];
            if value_row.len() != insert_columns.len() {
                return Err(SqlError::InvalidValue(format!(
                    "expected {} values, got {}",
                    insert_columns.len(),
                    value_row.len()
                )));
            }
            for (i, expr) in value_row.iter().enumerate() {
                let val = match expr {
                    Expr::Parameter(n) => params
                        .get(n - 1)
                        .cloned()
                        .ok_or_else(|| SqlError::Parse(format!("unbound parameter ${n}")))?,
                    Expr::Literal(v) => v.clone(),
                    _ => eval_const_expr(expr)?,
                };
                let col_idx = bufs.col_indices[i];
                let col = &table_schema.columns[col_idx];
                let got_type = val.data_type();
                bufs.row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    val.coerce_into(col.data_type)
                        .ok_or_else(|| SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        })?
                };
            }
        } else if let Some(sel) = sel_rows {
            let sel_row = &sel[idx];
            for (i, val) in sel_row.iter().enumerate() {
                let col_idx = bufs.col_indices[i];
                let col = &table_schema.columns[col_idx];
                let got_type = val.data_type();
                bufs.row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    val.clone().coerce_into(col.data_type).ok_or_else(|| {
                        SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        }
                    })?
                };
            }
        }

        if has_defaults {
            for &(pos, def_expr) in &defaults {
                let val = eval_const_expr(def_expr)?;
                let col = &table_schema.columns[pos];
                if !val.is_null() {
                    let got_type = val.data_type();
                    bufs.row[pos] =
                        val.coerce_into(col.data_type)
                            .ok_or_else(|| SqlError::TypeMismatch {
                                expected: col.data_type.to_string(),
                                got: got_type.to_string(),
                            })?;
                }
            }
        }

        for col in &table_schema.columns {
            if !col.nullable && bufs.row[col.position as usize].is_null() {
                return Err(SqlError::NotNullViolation(col.name.clone()));
            }
        }

        if let Some(ref col_map) = check_col_map {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(check, &EvalCtx::new(col_map, &bufs.row))?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, &EvalCtx::new(col_map, &bufs.row))?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        if has_fks {
            for fk in &table_schema.foreign_keys {
                let any_null = fk.columns.iter().any(|&ci| bufs.row[ci as usize].is_null());
                if any_null {
                    continue;
                }
                let fk_vals: Vec<Value> = fk
                    .columns
                    .iter()
                    .map(|&ci| bufs.row[ci as usize].clone())
                    .collect();
                bufs.fk_key_buf.clear();
                encode_composite_key_into(&fk_vals, &mut bufs.fk_key_buf);
                let found = wtx
                    .table_get(fk.foreign_table.as_bytes(), &bufs.fk_key_buf)
                    .map_err(SqlError::Storage)?;
                if found.is_none() {
                    let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                    return Err(SqlError::ForeignKeyViolation(name.to_string()));
                }
            }
        }

        for (j, &i) in pk_indices.iter().enumerate() {
            bufs.pk_values[j] = std::mem::replace(&mut bufs.row[i], Value::Null);
        }
        encode_composite_key_into(&bufs.pk_values, &mut bufs.key_buf);

        for &slot in dropped {
            bufs.value_values[slot as usize] = Value::Null;
        }
        for (j, &i) in non_pk.iter().enumerate() {
            bufs.value_values[enc_pos[j] as usize] =
                std::mem::replace(&mut bufs.row[i], Value::Null);
        }
        encode_row_into(&bufs.value_values, &mut bufs.value_buf);

        if bufs.key_buf.len() > citadel_core::MAX_KEY_SIZE {
            return Err(SqlError::KeyTooLarge {
                size: bufs.key_buf.len(),
                max: citadel_core::MAX_KEY_SIZE,
            });
        }
        if bufs.value_buf.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
            return Err(SqlError::RowTooLarge {
                size: bufs.value_buf.len(),
                max: citadel_core::MAX_INLINE_VALUE_SIZE,
            });
        }

        match compiled_conflict.as_ref() {
            None => {
                let is_new = wtx
                    .table_insert(table_bytes, &bufs.key_buf, &bufs.value_buf)
                    .map_err(SqlError::Storage)?;
                if !is_new {
                    return Err(SqlError::DuplicateKey);
                }
                if has_indices {
                    for (j, &i) in pk_indices.iter().enumerate() {
                        bufs.row[i] = bufs.pk_values[j].clone();
                    }
                    for (j, &i) in non_pk.iter().enumerate() {
                        bufs.row[i] = std::mem::replace(
                            &mut bufs.value_values[enc_pos[j] as usize],
                            Value::Null,
                        );
                    }
                    insert_index_entries(wtx, table_schema, &bufs.row, &bufs.pk_values)?;
                }
                count += 1;
            }
            Some(oc) => {
                let oc_ref: &CompiledOnConflict = oc;
                let needs_row = upsert_needs_row(oc_ref, table_schema);
                if needs_row {
                    for (j, &i) in pk_indices.iter().enumerate() {
                        bufs.row[i] = bufs.pk_values[j].clone();
                    }
                    for (j, &i) in non_pk.iter().enumerate() {
                        bufs.row[i] = std::mem::replace(
                            &mut bufs.value_values[enc_pos[j] as usize],
                            Value::Null,
                        );
                    }
                }
                match apply_insert_with_conflict(
                    wtx,
                    table_schema,
                    &bufs.key_buf,
                    &bufs.value_buf,
                    &bufs.row,
                    &bufs.pk_values,
                    oc_ref,
                    row_col_map.unwrap(),
                )? {
                    InsertRowOutcome::Inserted => count += 1,
                    InsertRowOutcome::Skipped => {}
                }
            }
        }
    }

    Ok(ExecutionResult::RowsAffected(count))
}

pub struct CompiledInsert {
    table_lower: String,
    cached: Option<InsertCache>,
}

struct InsertCache {
    col_indices: Vec<usize>,
    has_subquery: bool,
    any_defaults: bool,
    has_checks: bool,
    on_conflict: Option<Arc<CompiledOnConflict>>,
    row_col_map: Option<ColumnMap>,
}

#[derive(Clone)]
pub(super) enum CompiledOnConflict {
    DoNothing {
        target: Option<ConflictKind>,
    },
    DoUpdate {
        target: ConflictKind,
        assignments: Vec<(usize, Expr)>,
        where_clause: Option<Expr>,
        fast_paths: Option<Vec<DoUpdateFastPath>>,
    },
}

#[derive(Clone, Copy)]
pub(super) enum DoUpdateFastPath {
    IntAddConst { phys_idx: usize, delta: i64 },
}

#[derive(Clone, Debug)]
pub(super) enum ConflictKind {
    PrimaryKey,
    UniqueIndex { index_idx: usize },
}

fn resolve_conflict_target(target: &ConflictTarget, ts: &TableSchema) -> Result<ConflictKind> {
    match target {
        ConflictTarget::Columns(cols) => {
            let col_idx_set: Vec<u16> = cols
                .iter()
                .map(|name| {
                    ts.column_index(name)
                        .map(|i| i as u16)
                        .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
                })
                .collect::<Result<_>>()?;
            let pk_set = ts.primary_key_columns.clone();
            if set_equal(&col_idx_set, &pk_set) {
                return Ok(ConflictKind::PrimaryKey);
            }
            for (index_idx, idx) in ts.indices.iter().enumerate() {
                if idx.unique && set_equal(&col_idx_set, &idx.columns) {
                    return Ok(ConflictKind::UniqueIndex { index_idx });
                }
            }
            Err(SqlError::Plan(
                "ON CONFLICT target does not match any unique constraint".into(),
            ))
        }
        ConflictTarget::Constraint(name) => {
            let lower = name.to_ascii_lowercase();
            for (index_idx, idx) in ts.indices.iter().enumerate() {
                if idx.name.eq_ignore_ascii_case(&lower) {
                    if idx.unique {
                        return Ok(ConflictKind::UniqueIndex { index_idx });
                    }
                    return Err(SqlError::Plan(format!(
                        "ON CONFLICT ON CONSTRAINT '{name}' requires a unique index"
                    )));
                }
            }
            Err(SqlError::Plan(format!(
                "unknown constraint '{name}'; primary keys cannot be referenced by name, use ON CONFLICT (col_list)"
            )))
        }
    }
}

fn set_equal(a: &[u16], b: &[u16]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_sorted = a.to_vec();
    let mut b_sorted = b.to_vec();
    a_sorted.sort_unstable();
    b_sorted.sort_unstable();
    a_sorted == b_sorted
}

pub(super) enum InsertRowOutcome {
    Inserted,
    Skipped,
}

#[allow(clippy::too_many_arguments)]
#[inline]
pub(super) fn apply_insert_with_conflict(
    wtx: &mut WriteTxn<'_>,
    table_schema: &TableSchema,
    key_buf: &[u8],
    value_buf: &[u8],
    row: &[Value],
    pk_values: &[Value],
    on_conflict: &CompiledOnConflict,
    col_map: &ColumnMap,
) -> Result<InsertRowOutcome> {
    let table_bytes = table_schema.name.as_bytes();

    if let CompiledOnConflict::DoNothing { target } = on_conflict {
        let pk_target = matches!(target, None | Some(ConflictKind::PrimaryKey));
        if pk_target && table_schema.indices.is_empty() && table_schema.foreign_keys.is_empty() {
            let inserted = wtx
                .table_insert_if_absent(table_bytes, key_buf, value_buf)
                .map_err(SqlError::Storage)?;
            return Ok(if inserted {
                InsertRowOutcome::Inserted
            } else {
                InsertRowOutcome::Skipped
            });
        }
    }

    if let CompiledOnConflict::DoUpdate {
        target: ConflictKind::PrimaryKey,
        assignments,
        where_clause,
        fast_paths,
    } = on_conflict
    {
        if can_fuse_do_update(table_schema, assignments) {
            return apply_do_update_fused(
                wtx,
                table_schema,
                table_bytes,
                key_buf,
                value_buf,
                row,
                assignments,
                where_clause.as_ref(),
                col_map,
                fast_paths.as_deref(),
            );
        }
    }

    let primary_outcome = wtx
        .table_insert_or_fetch(table_bytes, key_buf, value_buf)
        .map_err(SqlError::Storage)?;

    match primary_outcome {
        citadel_txn::write_txn::InsertOutcome::Inserted => {
            if table_schema.indices.is_empty() {
                return Ok(InsertRowOutcome::Inserted);
            }
            let mut inserted_keys: Vec<(usize, Vec<u8>)> = Vec::new();
            match insert_index_entries_or_fetch(
                wtx,
                table_schema,
                row,
                pk_values,
                &mut inserted_keys,
            )? {
                None => Ok(InsertRowOutcome::Inserted),
                Some(conflicting_idx) => {
                    let matches_target =
                        matches!(on_conflict, CompiledOnConflict::DoNothing { target: None })
                            || matches!(
                                on_conflict,
                                CompiledOnConflict::DoNothing {
                                    target: Some(ConflictKind::UniqueIndex { index_idx }),
                                } | CompiledOnConflict::DoUpdate {
                                    target: ConflictKind::UniqueIndex { index_idx },
                                    ..
                                } if *index_idx == conflicting_idx
                            );
                    undo_partial_insert(wtx, table_schema, key_buf, &inserted_keys)?;
                    if !matches_target {
                        return Err(SqlError::UniqueViolation(
                            table_schema.indices[conflicting_idx].name.clone(),
                        ));
                    }
                    match on_conflict {
                        CompiledOnConflict::DoNothing { .. } => Ok(InsertRowOutcome::Skipped),
                        CompiledOnConflict::DoUpdate {
                            assignments,
                            where_clause,
                            ..
                        } => {
                            let existing_pk =
                                fetch_unique_index_pk(wtx, table_schema, conflicting_idx, row)?;
                            apply_do_update(
                                wtx,
                                table_schema,
                                &existing_pk,
                                row,
                                assignments,
                                where_clause.as_ref(),
                                col_map,
                            )
                        }
                    }
                }
            }
        }
        citadel_txn::write_txn::InsertOutcome::Existed(old_bytes) => {
            let matches_target = matches!(
                on_conflict,
                CompiledOnConflict::DoNothing { target: None }
                    | CompiledOnConflict::DoNothing {
                        target: Some(ConflictKind::PrimaryKey),
                    }
                    | CompiledOnConflict::DoUpdate {
                        target: ConflictKind::PrimaryKey,
                        ..
                    }
            );
            if !matches_target {
                return Err(SqlError::DuplicateKey);
            }
            match on_conflict {
                CompiledOnConflict::DoNothing { .. } => Ok(InsertRowOutcome::Skipped),
                CompiledOnConflict::DoUpdate {
                    assignments,
                    where_clause,
                    ..
                } => {
                    let old_row = decode_full_row(table_schema, key_buf, &old_bytes)?;
                    apply_do_update_with_old_row(
                        wtx,
                        table_schema,
                        key_buf,
                        &old_row,
                        row,
                        assignments,
                        where_clause.as_ref(),
                        col_map,
                    )
                }
            }
        }
    }
}

#[inline]
fn apply_fast_path_patch(
    old_bytes: &[u8],
    fast_paths: &[DoUpdateFastPath],
) -> Result<UpsertAction> {
    UPSERT_SCRATCH.with(|slot| {
        let mut bufs = slot.borrow_mut();
        bufs.new_value_buf.clear();
        bufs.new_value_buf.extend_from_slice(old_bytes);

        let mut patch_scratch: Vec<u8> = Vec::new();

        for fp in fast_paths {
            match fp {
                DoUpdateFastPath::IntAddConst { phys_idx, delta } => {
                    let decoded =
                        crate::encoding::decode_columns(&bufs.new_value_buf, &[*phys_idx])?;
                    let old_val = &decoded[0];
                    let new_val = match old_val {
                        Value::Integer(i) => Value::Integer(i.wrapping_add(*delta)),
                        Value::Null => Value::Null,
                        _ => {
                            return Err(SqlError::TypeMismatch {
                                expected: "INTEGER".into(),
                                got: old_val.data_type().to_string(),
                            });
                        }
                    };
                    if !crate::encoding::patch_column_in_place(
                        &mut bufs.new_value_buf,
                        *phys_idx,
                        &new_val,
                    )? {
                        patch_scratch.clear();
                        crate::encoding::patch_row_column(
                            &bufs.new_value_buf,
                            *phys_idx,
                            &new_val,
                            &mut patch_scratch,
                        )?;
                        std::mem::swap(&mut bufs.new_value_buf, &mut patch_scratch);
                    }
                }
            }
        }

        if bufs.new_value_buf.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
            return Err(SqlError::RowTooLarge {
                size: bufs.new_value_buf.len(),
                max: citadel_core::MAX_INLINE_VALUE_SIZE,
            });
        }

        Ok(UpsertAction::Replace(bufs.new_value_buf.clone()))
    })
}

fn upsert_needs_row(oc: &CompiledOnConflict, ts: &TableSchema) -> bool {
    if !ts.indices.is_empty() {
        return true;
    }
    match oc {
        CompiledOnConflict::DoNothing { .. } => false,
        CompiledOnConflict::DoUpdate { fast_paths, .. } => fast_paths.is_none() || ts.has_checks(),
    }
}

fn can_fuse_do_update(ts: &TableSchema, assignments: &[(usize, Expr)]) -> bool {
    if !ts.indices.is_empty() {
        return false;
    }
    if !ts.foreign_keys.is_empty() {
        return false;
    }
    let pk = ts.pk_indices();
    !assignments.iter().any(|(ci, _)| pk.contains(ci))
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn apply_do_update_fused(
    wtx: &mut WriteTxn<'_>,
    table_schema: &TableSchema,
    table_bytes: &[u8],
    key_buf: &[u8],
    value_buf: &[u8],
    proposed_row: &[Value],
    assignments: &[(usize, Expr)],
    where_clause: Option<&Expr>,
    col_map: &ColumnMap,
    fast_paths: Option<&[DoUpdateFastPath]>,
) -> Result<InsertRowOutcome> {
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let phys_count = table_schema.physical_non_pk_count();
    let dropped = table_schema.dropped_non_pk_slots();
    let has_checks = table_schema.has_checks();
    let has_fks = !table_schema.foreign_keys.is_empty();

    let outcome =
        wtx.table_upsert_with::<_, SqlError>(table_bytes, key_buf, value_buf, |old_bytes| {
            if let Some(fps) = fast_paths {
                if !has_checks {
                    return apply_fast_path_patch(old_bytes, fps);
                }
            }
            UPSERT_SCRATCH.with(|slot| {
                let mut bufs = slot.borrow_mut();
                let UpsertBufs {
                    old_row,
                    new_row,
                    value_values,
                    new_value_buf,
                } = &mut *bufs;

                old_row.clear();
                old_row.resize(table_schema.columns.len(), Value::Null);
                decode_full_row_into(table_schema, key_buf, old_bytes, old_row)?;

                if let Some(w) = where_clause {
                    let ctx = EvalCtx::with_excluded(col_map, old_row, col_map, proposed_row);
                    let result = eval_expr(w, &ctx)?;
                    if result.is_null() || !is_truthy(&result) {
                        return Ok(UpsertAction::Skip);
                    }
                }

                new_row.clear();
                new_row.extend_from_slice(old_row);
                for (col_idx, expr) in assignments {
                    let ctx = EvalCtx::with_excluded(col_map, old_row, col_map, proposed_row);
                    let val = eval_expr(expr, &ctx)?;
                    let col = &table_schema.columns[*col_idx];
                    new_row[*col_idx] = if val.is_null() {
                        Value::Null
                    } else {
                        let got = val.data_type();
                        val.coerce_into(col.data_type)
                            .ok_or_else(|| SqlError::TypeMismatch {
                                expected: col.data_type.to_string(),
                                got: got.to_string(),
                            })?
                    };
                }

                for (assigned_idx, _) in assignments {
                    let col = &table_schema.columns[*assigned_idx];
                    if !col.nullable && new_row[col.position as usize].is_null() {
                        return Err(SqlError::NotNullViolation(col.name.clone()));
                    }
                }
                if has_checks {
                    for col in &table_schema.columns {
                        if let Some(ref check) = col.check_expr {
                            let ctx = EvalCtx::new(col_map, new_row);
                            let result = eval_expr(check, &ctx)?;
                            if !is_truthy(&result) && !result.is_null() {
                                let name = col.check_name.as_deref().unwrap_or(&col.name);
                                return Err(SqlError::CheckViolation(name.to_string()));
                            }
                        }
                    }
                    for tc in &table_schema.check_constraints {
                        let ctx = EvalCtx::new(col_map, new_row);
                        let result = eval_expr(&tc.expr, &ctx)?;
                        if !is_truthy(&result) && !result.is_null() {
                            let name = tc.name.as_deref().unwrap_or(&tc.sql);
                            return Err(SqlError::CheckViolation(name.to_string()));
                        }
                    }
                }
                let _ = has_fks;

                value_values.clear();
                value_values.resize(phys_count, Value::Null);
                for &slot in dropped {
                    value_values[slot as usize] = Value::Null;
                }
                for (j, &i) in non_pk.iter().enumerate() {
                    value_values[enc_pos[j] as usize] = new_row[i].clone();
                }
                new_value_buf.clear();
                crate::encoding::encode_row_into(value_values, new_value_buf);

                if new_value_buf.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
                    return Err(SqlError::RowTooLarge {
                        size: new_value_buf.len(),
                        max: citadel_core::MAX_INLINE_VALUE_SIZE,
                    });
                }

                Ok(UpsertAction::Replace(new_value_buf.clone()))
            })
        })?;

    match outcome {
        UpsertOutcome::Inserted | UpsertOutcome::Updated => Ok(InsertRowOutcome::Inserted),
        UpsertOutcome::Skipped => Ok(InsertRowOutcome::Skipped),
    }
}

fn fetch_unique_index_pk(
    wtx: &mut WriteTxn<'_>,
    table_schema: &TableSchema,
    index_idx: usize,
    row: &[Value],
) -> Result<Vec<u8>> {
    let idx = &table_schema.indices[index_idx];
    let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
    let indexed: Vec<Value> = idx
        .columns
        .iter()
        .map(|&col_idx| row[col_idx as usize].clone())
        .collect();
    let key = crate::encoding::encode_composite_key(&indexed);
    let value = wtx
        .table_get(&idx_table, &key)
        .map_err(SqlError::Storage)?
        .ok_or_else(|| {
            SqlError::InvalidValue("unique index missing expected collision entry".into())
        })?;
    Ok(value)
}

fn apply_do_update(
    wtx: &mut WriteTxn<'_>,
    table_schema: &TableSchema,
    pk_key: &[u8],
    proposed_row: &[Value],
    assignments: &[(usize, Expr)],
    where_clause: Option<&Expr>,
    col_map: &ColumnMap,
) -> Result<InsertRowOutcome> {
    let old_value = wtx
        .table_get(table_schema.name.as_bytes(), pk_key)
        .map_err(SqlError::Storage)?
        .ok_or_else(|| SqlError::InvalidValue("primary row missing for DO UPDATE target".into()))?;
    let old_row = decode_full_row(table_schema, pk_key, &old_value)?;
    apply_do_update_with_old_row(
        wtx,
        table_schema,
        pk_key,
        &old_row,
        proposed_row,
        assignments,
        where_clause,
        col_map,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_do_update_with_old_row(
    wtx: &mut WriteTxn<'_>,
    table_schema: &TableSchema,
    old_pk_key: &[u8],
    old_row: &[Value],
    proposed_row: &[Value],
    assignments: &[(usize, Expr)],
    where_clause: Option<&Expr>,
    col_map: &ColumnMap,
) -> Result<InsertRowOutcome> {
    if let Some(w) = where_clause {
        let ctx = EvalCtx::with_excluded(col_map, old_row, col_map, proposed_row);
        let result = eval_expr(w, &ctx)?;
        if result.is_null() || !is_truthy(&result) {
            return Ok(InsertRowOutcome::Skipped);
        }
    }

    let mut new_row = old_row.to_vec();
    for (col_idx, expr) in assignments {
        let ctx = EvalCtx::with_excluded(col_map, old_row, col_map, proposed_row);
        let val = eval_expr(expr, &ctx)?;
        let col = &table_schema.columns[*col_idx];
        new_row[*col_idx] = if val.is_null() {
            Value::Null
        } else {
            let got = val.data_type();
            val.coerce_into(col.data_type)
                .ok_or_else(|| SqlError::TypeMismatch {
                    expected: col.data_type.to_string(),
                    got: got.to_string(),
                })?
        };
    }

    let pk_indices = table_schema.pk_indices();
    let assigned_pk = assignments.iter().any(|(ci, _)| pk_indices.contains(ci));
    let pk_changed = assigned_pk && pk_indices.iter().any(|&i| old_row[i] != new_row[i]);

    for (assigned_idx, _) in assignments {
        let col = &table_schema.columns[*assigned_idx];
        if !col.nullable && new_row[col.position as usize].is_null() {
            return Err(SqlError::NotNullViolation(col.name.clone()));
        }
    }
    if table_schema.has_checks() {
        for col in &table_schema.columns {
            if let Some(ref check) = col.check_expr {
                let ctx = EvalCtx::new(col_map, &new_row);
                let result = eval_expr(check, &ctx)?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = col.check_name.as_deref().unwrap_or(&col.name);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }
        for tc in &table_schema.check_constraints {
            let ctx = EvalCtx::new(col_map, &new_row);
            let result = eval_expr(&tc.expr, &ctx)?;
            if !is_truthy(&result) && !result.is_null() {
                let name = tc.name.as_deref().unwrap_or(&tc.sql);
                return Err(SqlError::CheckViolation(name.to_string()));
            }
        }
    }
    for fk in &table_schema.foreign_keys {
        let changed = fk
            .columns
            .iter()
            .any(|&ci| old_row[ci as usize] != new_row[ci as usize]);
        if !changed {
            continue;
        }
        let any_null = fk.columns.iter().any(|&ci| new_row[ci as usize].is_null());
        if any_null {
            continue;
        }
        let fk_vals: Vec<Value> = fk
            .columns
            .iter()
            .map(|&ci| new_row[ci as usize].clone())
            .collect();
        let fk_key = crate::encoding::encode_composite_key(&fk_vals);
        let found = wtx
            .table_get(fk.foreign_table.as_bytes(), &fk_key)
            .map_err(SqlError::Storage)?;
        if found.is_none() {
            let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
            return Err(SqlError::ForeignKeyViolation(name.to_string()));
        }
    }

    let has_indices = !table_schema.indices.is_empty();
    let old_pk_values: Vec<Value> = if has_indices || pk_changed {
        pk_indices.iter().map(|&i| old_row[i].clone()).collect()
    } else {
        Vec::new()
    };
    let new_pk_values: Vec<Value> = if has_indices || pk_changed {
        pk_indices.iter().map(|&i| new_row[i].clone()).collect()
    } else {
        Vec::new()
    };

    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let phys_count = table_schema.physical_non_pk_count();
    let dropped = table_schema.dropped_non_pk_slots();
    let mut value_values: Vec<Value> = vec![Value::Null; phys_count];
    for &slot in dropped {
        value_values[slot as usize] = Value::Null;
    }
    for (j, &i) in non_pk.iter().enumerate() {
        value_values[enc_pos[j] as usize] = new_row[i].clone();
    }
    let mut new_value_buf = Vec::with_capacity(256);
    crate::encoding::encode_row_into(&value_values, &mut new_value_buf);

    if new_value_buf.len() > citadel_core::MAX_INLINE_VALUE_SIZE {
        return Err(SqlError::RowTooLarge {
            size: new_value_buf.len(),
            max: citadel_core::MAX_INLINE_VALUE_SIZE,
        });
    }

    if pk_changed {
        let new_pk_key = crate::encoding::encode_composite_key(&new_pk_values);
        let inserted = wtx
            .table_insert(table_schema.name.as_bytes(), &new_pk_key, &new_value_buf)
            .map_err(SqlError::Storage)?;
        if !inserted {
            return Err(SqlError::DuplicateKey);
        }
        wtx.table_delete(table_schema.name.as_bytes(), old_pk_key)
            .map_err(SqlError::Storage)?;
        for idx in &table_schema.indices {
            let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
            let old_idx_key = encode_index_key(idx, old_row, &old_pk_values);
            wtx.table_delete(&idx_table, &old_idx_key)
                .map_err(SqlError::Storage)?;
            let new_idx_key = encode_index_key(idx, &new_row, &new_pk_values);
            let new_idx_val = encode_index_value(idx, &new_row, &new_pk_values);
            let is_new = wtx
                .table_insert(&idx_table, &new_idx_key, &new_idx_val)
                .map_err(SqlError::Storage)?;
            if idx.unique && !is_new {
                let any_null = idx.columns.iter().any(|&c| new_row[c as usize].is_null());
                if !any_null {
                    return Err(SqlError::UniqueViolation(idx.name.clone()));
                }
            }
        }
    } else {
        wtx.table_update_sorted(
            table_schema.name.as_bytes(),
            &[(old_pk_key, new_value_buf.as_slice())],
        )
        .map_err(SqlError::Storage)?;
        for idx in &table_schema.indices {
            if !index_columns_changed(idx, old_row, &new_row) {
                continue;
            }
            let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
            let old_idx_key = encode_index_key(idx, old_row, &old_pk_values);
            wtx.table_delete(&idx_table, &old_idx_key)
                .map_err(SqlError::Storage)?;
            let new_idx_key = encode_index_key(idx, &new_row, &new_pk_values);
            let new_idx_val = encode_index_value(idx, &new_row, &new_pk_values);
            let is_new = wtx
                .table_insert(&idx_table, &new_idx_key, &new_idx_val)
                .map_err(SqlError::Storage)?;
            if idx.unique && !is_new {
                let any_null = idx.columns.iter().any(|&c| new_row[c as usize].is_null());
                if !any_null {
                    return Err(SqlError::UniqueViolation(idx.name.clone()));
                }
            }
        }
    }

    Ok(InsertRowOutcome::Inserted)
}

fn detect_fast_paths(
    ts: &TableSchema,
    assignments: &[(usize, Expr)],
) -> Option<Vec<DoUpdateFastPath>> {
    let non_pk = ts.non_pk_indices();
    let enc_pos = ts.encoding_positions();
    let mut out = Vec::with_capacity(assignments.len());
    for (col_idx, expr) in assignments {
        let col = &ts.columns[*col_idx];
        if col.data_type != DataType::Integer {
            return None;
        }
        let nonpk_order = non_pk.iter().position(|&i| i == *col_idx)?;
        let phys_idx = enc_pos[nonpk_order] as usize;

        if let Expr::BinaryOp { left, op, right } = expr {
            if !matches!(op, BinOp::Add | BinOp::Sub) {
                return None;
            }
            let reads_target =
                matches!(left.as_ref(), Expr::Column(n) if n.eq_ignore_ascii_case(&col.name));
            if !reads_target {
                return None;
            }
            if let Expr::Literal(Value::Integer(n)) = right.as_ref() {
                let delta = if matches!(op, BinOp::Sub) { -n } else { *n };
                let _ = col_idx;
                out.push(DoUpdateFastPath::IntAddConst { phys_idx, delta });
                continue;
            }
            return None;
        }
        return None;
    }
    Some(out)
}

fn compile_on_conflict(oc: &OnConflictClause, ts: &TableSchema) -> Result<CompiledOnConflict> {
    let target = oc
        .target
        .as_ref()
        .map(|t| resolve_conflict_target(t, ts))
        .transpose()?;
    match &oc.action {
        OnConflictAction::DoNothing => Ok(CompiledOnConflict::DoNothing { target }),
        OnConflictAction::DoUpdate {
            assignments,
            where_clause,
        } => {
            let target = target.ok_or_else(|| {
                SqlError::Plan("ON CONFLICT without target requires DO NOTHING".into())
            })?;
            let compiled_assignments: Vec<(usize, Expr)> = assignments
                .iter()
                .map(|(name, expr)| {
                    let col_idx = ts
                        .column_index(name)
                        .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))?;
                    Ok((col_idx, expr.clone()))
                })
                .collect::<Result<_>>()?;
            let fast_paths = if where_clause.is_none() {
                detect_fast_paths(ts, &compiled_assignments)
            } else {
                None
            };
            Ok(CompiledOnConflict::DoUpdate {
                target,
                assignments: compiled_assignments,
                where_clause: where_clause.clone(),
                fast_paths,
            })
        }
    }
}

impl CompiledInsert {
    pub fn try_compile(schema: &SchemaManager, stmt: &InsertStmt) -> Option<Self> {
        let lower = stmt.table.to_ascii_lowercase();
        let cached = if let Some(ts) = schema.get(&lower) {
            let insert_columns: Vec<&str> = if stmt.columns.is_empty() {
                ts.columns.iter().map(|c| c.name.as_str()).collect()
            } else {
                stmt.columns.iter().map(|s| s.as_str()).collect()
            };
            let mut col_indices = Vec::with_capacity(insert_columns.len());
            for name in &insert_columns {
                col_indices.push(ts.column_index(name)?);
            }
            let on_conflict = stmt
                .on_conflict
                .as_ref()
                .map(|oc| compile_on_conflict(oc, ts))
                .transpose()
                .ok()
                .flatten()
                .map(Arc::new);
            let row_col_map = on_conflict.as_ref().map(|_| ColumnMap::new(&ts.columns));
            Some(InsertCache {
                col_indices,
                has_subquery: insert_has_subquery(stmt),
                any_defaults: ts.columns.iter().any(|c| c.default_expr.is_some()),
                has_checks: ts.has_checks(),
                on_conflict,
                row_col_map,
            })
        } else if schema.get_view(&lower).is_some() {
            None
        } else {
            return None;
        };
        Some(Self {
            table_lower: lower,
            cached,
        })
    }
}

impl CompiledPlan for CompiledInsert {
    fn execute(
        &self,
        db: &Database,
        schema: &SchemaManager,
        stmt: &Statement,
        params: &[Value],
        wtx: Option<&mut WriteTxn<'_>>,
    ) -> Result<ExecutionResult> {
        let ins = match stmt {
            Statement::Insert(i) => i,
            _ => {
                return Err(SqlError::Unsupported(
                    "CompiledInsert received non-INSERT statement".into(),
                ))
            }
        };
        let _ = &self.table_lower;
        match wtx {
            None => exec_insert(db, schema, ins, params),
            Some(outer) => match self.cached.as_ref() {
                Some(c) => exec_insert_in_txn_cached(outer, schema, ins, params, c),
                None => exec_insert_in_txn(outer, schema, ins, params),
            },
        }
    }
}

pub struct CompiledDelete {
    table_lower: String,
}

impl CompiledDelete {
    pub fn try_compile(schema: &SchemaManager, stmt: &DeleteStmt) -> Option<Self> {
        let lower = stmt.table.to_ascii_lowercase();
        schema.get(&lower)?;
        Some(Self { table_lower: lower })
    }
}

impl CompiledPlan for CompiledDelete {
    fn execute(
        &self,
        db: &Database,
        schema: &SchemaManager,
        stmt: &Statement,
        _params: &[Value],
        wtx: Option<&mut WriteTxn<'_>>,
    ) -> Result<ExecutionResult> {
        let del = match stmt {
            Statement::Delete(d) => d,
            _ => {
                return Err(SqlError::Unsupported(
                    "CompiledDelete received non-DELETE statement".into(),
                ))
            }
        };
        let _ = &self.table_lower;
        match wtx {
            None => super::write::exec_delete(db, schema, del),
            Some(outer) => super::write::exec_delete_in_txn(outer, schema, del),
        }
    }
}
