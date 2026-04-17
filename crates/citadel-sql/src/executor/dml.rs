use citadel::Database;

use crate::encoding::{encode_composite_key_into, encode_row_into};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap};
use crate::parser::*;
use crate::types::*;

use crate::schema::SchemaManager;

use super::helpers::*;
use super::CteContext;

// ── DML + materialization ───────────────────────────────────────────

pub(super) fn exec_insert(
    db: &Database,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
) -> Result<ExecutionResult> {
    let empty_ctes = CteContext::new();
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
                    let result = eval_expr(check, col_map, &row)?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, col_map, &row)?;
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
                row[i] = std::mem::replace(&mut value_values[enc_pos[j] as usize], Value::Null);
            }
            insert_index_entries(&mut wtx, table_schema, &row, &pk_values)?;
        }
        count += 1;
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
        Expr::Function { name, args } => {
            let materialized = args
                .iter()
                .map(|a| materialize_expr(a, exec_sub))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::Function {
                name: name.clone(),
                args: materialized,
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
            let mut seen = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for row in left_qr.rows.into_iter().chain(right_qr.rows) {
                if seen.insert(row.clone()) {
                    rows.push(row);
                }
            }
            rows
        }
        (SetOp::Intersect, true) => {
            let mut right_counts: std::collections::HashMap<Vec<Value>, usize> =
                std::collections::HashMap::new();
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
            let mut seen = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for row in left_qr.rows {
                if right_set.contains(&row) && seen.insert(row.clone()) {
                    rows.push(row);
                }
            }
            rows
        }
        (SetOp::Except, true) => {
            let mut right_counts: std::collections::HashMap<Vec<Value>, usize> =
                std::collections::HashMap::new();
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
            let mut seen = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for row in left_qr.rows {
                if !right_set.contains(&row) && seen.insert(row.clone()) {
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

#[derive(Default)]
pub struct InsertBufs {
    row: Vec<Value>,
    pk_values: Vec<Value>,
    value_values: Vec<Value>,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
    col_indices: Vec<usize>,
    fk_key_buf: Vec<u8>,
}

impl InsertBufs {
    pub fn new() -> Self {
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

pub fn exec_insert_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
    bufs: &mut InsertBufs,
) -> Result<ExecutionResult> {
    let empty_ctes = CteContext::new();
    let materialized;
    let stmt = if insert_has_subquery(stmt) {
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
    for name in insert_columns {
        bufs.col_indices.push(
            table_schema
                .column_index(name)
                .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))?,
        );
    }

    let defaults: Vec<(usize, &Expr)> = table_schema
        .columns
        .iter()
        .filter(|c| c.default_expr.is_some() && !bufs.col_indices.contains(&(c.position as usize)))
        .map(|c| (c.position as usize, c.default_expr.as_ref().unwrap()))
        .collect();

    let has_checks = table_schema.has_checks();
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
                let val = if let Expr::Parameter(n) = expr {
                    params
                        .get(n - 1)
                        .cloned()
                        .ok_or_else(|| SqlError::Parse(format!("unbound parameter ${n}")))?
                } else {
                    eval_const_expr(expr)?
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

        for &(pos, def_expr) in &defaults {
            let val = eval_const_expr(def_expr)?;
            let col = &table_schema.columns[pos];
            if val.is_null() {
                // bufs.row[pos] already Null from init
            } else {
                let got_type = val.data_type();
                bufs.row[pos] =
                    val.coerce_into(col.data_type)
                        .ok_or_else(|| SqlError::TypeMismatch {
                            expected: col.data_type.to_string(),
                            got: got_type.to_string(),
                        })?;
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
                    let result = eval_expr(check, col_map, &bufs.row)?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, col_map, &bufs.row)?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

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

        let is_new = wtx
            .table_insert(stmt.table.as_bytes(), &bufs.key_buf, &bufs.value_buf)
            .map_err(SqlError::Storage)?;
        if !is_new {
            return Err(SqlError::DuplicateKey);
        }

        if !table_schema.indices.is_empty() {
            for (j, &i) in pk_indices.iter().enumerate() {
                bufs.row[i] = bufs.pk_values[j].clone();
            }
            for (j, &i) in non_pk.iter().enumerate() {
                bufs.row[i] =
                    std::mem::replace(&mut bufs.value_values[enc_pos[j] as usize], Value::Null);
            }
            insert_index_entries(wtx, table_schema, &bufs.row, &bufs.pk_values)?;
        }
        count += 1;
    }

    Ok(ExecutionResult::RowsAffected(count))
}
