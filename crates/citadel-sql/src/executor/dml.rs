use std::cell::RefCell;
use std::sync::Arc;

use citadel::Database;
use citadel_buffer::btree::{UpsertAction, UpsertOutcome};
use citadel_txn::read_txn::ReadTxn;
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
use super::{CteContext, CteRows};

/// (before-insert, after-insert, after-update) row-trigger presence, hoisted once.
fn row_insert_trigger_flags(schema: &SchemaManager, table_name: &str) -> (bool, bool, bool) {
    use crate::parser::{TriggerEvent, TriggerGranularity, TriggerTiming};
    let triggers = schema.triggers_for(table_name);
    let row =
        |t: &crate::types::TriggerDef| t.enabled && t.granularity == TriggerGranularity::ForEachRow;
    let ev = |t: &crate::types::TriggerDef, update: bool| {
        t.events.iter().any(|e| {
            if update {
                matches!(e, TriggerEvent::Update(_))
            } else {
                matches!(e, TriggerEvent::Insert)
            }
        })
    };
    (
        triggers
            .iter()
            .any(|t| row(t) && t.timing == TriggerTiming::Before && ev(t, false)),
        triggers
            .iter()
            .any(|t| row(t) && t.timing == TriggerTiming::After && ev(t, false)),
        triggers
            .iter()
            .any(|t| row(t) && t.timing == TriggerTiming::After && ev(t, true)),
    )
}

/// Classify an INSERT for cache invalidation: a pure append (single-INTEGER pk,
/// no conflict) stays append-retainable; anything else hard-invalidates.
fn mark_insert_dml(
    schema: &SchemaManager,
    table_name: &str,
    on_conflict: bool,
    single_int_pk: bool,
    min_inserted_pk: Option<i64>,
    rows_written: u64,
) {
    if on_conflict {
        schema.mark_dml(table_name);
    } else if single_int_pk {
        if let Some(m) = min_inserted_pk {
            schema.mark_dml_append(table_name, m);
        }
    } else if rows_written > 0 {
        schema.mark_dml(table_name);
    }
}

/// Single INTEGER pk - the only shape an ANN plan indexes (and append-retains).
fn is_single_int_pk(table_schema: &TableSchema) -> bool {
    table_schema.primary_key_columns.len() == 1
        && matches!(
            table_schema.columns[table_schema.primary_key_columns[0] as usize].data_type,
            DataType::Integer
        )
}

fn insert_select_rows(result: QueryResult, expected: usize) -> Result<Vec<Vec<Value>>> {
    if result.columns.len() != expected {
        return Err(SqlError::InvalidValue(format!(
            "INSERT ... SELECT column count mismatch: expected {expected}, got {}",
            result.columns.len()
        )));
    }
    Ok(result.rows)
}

fn bind_selected_row(
    source: &mut Vec<Value>,
    row: &mut [Value],
    indices: &[usize],
    schema: &TableSchema,
) -> Result<()> {
    if source.len() != indices.len() {
        return Err(SqlError::InvalidValue(format!(
            "INSERT ... SELECT column count mismatch: expected {}, got {}",
            indices.len(),
            source.len()
        )));
    }
    for (value, &index) in source.drain(..).zip(indices) {
        row[index] = coerce_for_column(value, &schema.columns[index], schema.is_strict())?;
    }
    Ok(())
}

pub(super) fn exec_insert(
    db: &Database,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
) -> Result<ExecutionResult> {
    let empty_ctes = CteContext::default();
    if let Some(plan) = super::insert_copy::CopyPlan::new(schema, stmt, &empty_ctes) {
        let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
        let result = plan.execute(&mut wtx, schema)?;
        super::commit_with_ann_publication(wtx, schema)?;
        return Ok(result);
    }
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
    if let Some(view_def) = schema.get_view(&lower_name) {
        if super::triggers::has_instead_of(schema, &lower_name, super::triggers::FireEvent::Insert)
        {
            let aliases = view_def.column_aliases.clone();
            return exec_instead_of_view_insert_auto(
                db,
                schema,
                &lower_name,
                &aliases,
                stmt,
                params,
            );
        }
        return Err(SqlError::CannotModifyView(stmt.table.clone()));
    }
    if schema.get_matview(&lower_name).is_some() {
        return Err(SqlError::CannotModifyView(format!(
            "materialized view '{}' is read-only — use REFRESH MATERIALIZED VIEW",
            stmt.table
        )));
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

    for &ci in &col_indices {
        if table_schema.columns[ci].generated_kind.is_some() {
            return Err(SqlError::CannotInsertIntoGeneratedColumn(
                table_schema.columns[ci].name.clone(),
            ));
        }
    }

    let defaults: Vec<(usize, &Expr)> = table_schema
        .columns
        .iter()
        .filter(|c| c.default_expr.is_some() && !col_indices.contains(&(c.position as usize)))
        .map(|c| (c.position as usize, c.default_expr.as_ref().unwrap()))
        .collect();

    let required_virtuals = required_insert_virtuals(schema, table_schema, stmt);
    let generated_cols: Vec<(usize, &Expr)> = table_schema
        .columns
        .iter()
        .filter(|c| {
            matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored))
                || required_virtuals
                    .before_insert
                    .contains(&(c.position as usize))
        })
        .map(|c| (c.position as usize, c.generated_expr.as_ref().unwrap()))
        .collect();

    let has_checks = table_schema.has_checks();
    let strict = table_schema.is_strict();
    let row_col_map_for_gen = (!generated_cols.is_empty()).then(|| table_schema.column_map());
    let check_col_map = has_checks.then(|| table_schema.column_map());

    let cancel = db.cancel_token();
    let mut select_rows = match &stmt.source {
        InsertSource::Select(sq) => {
            let insert_ctes = super::materialize_all_ctes(
                &sq.ctes,
                sq.recursive,
                cancel.as_ref(),
                &mut |body, ctx| {
                    let result = exec_query_body_read(db, schema, body, ctx)?;
                    let collations =
                        body_output_collations(schema, ctx, body, result.columns.len());
                    Ok(CteRows::new(result, collations))
                },
            )?;
            let qr = exec_query_body_read(db, schema, &sq.body, &insert_ctes)?;
            Some(insert_select_rows(qr, insert_columns.len())?)
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
        .map(|_| table_schema.column_map());

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    // DML invalidates the table's persisted ANN segment in the SAME txn
    // (rollback restores it; commit makes table-changed-but-segment-survives
    // unrepresentable for this path).
    if table_schema.has_ann_index() {
        super::ann_persist::purge_segment(&mut wtx, &table_schema.name)?;
    }
    let mut count: u64 = 0;
    let mut returning_rows: Option<Vec<super::helpers::ReturningRow>> =
        stmt.returning.as_ref().map(|_| Vec::new());

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
    let total = match (values, select_rows.as_deref()) {
        (Some(rows), _) => rows.len(),
        (_, Some(rows)) => rows.len(),
        _ => 0,
    };

    let has_insert_statement_triggers = schema.triggers_for(&table_schema.name).iter().any(|t| {
        t.enabled
            && t.granularity == crate::parser::TriggerGranularity::ForEachStatement
            && t.events
                .iter()
                .any(|e| matches!(e, crate::parser::TriggerEvent::Insert))
    });
    let mut stmt_new_rows: Vec<Vec<Value>> = if has_insert_statement_triggers {
        Vec::with_capacity(total)
    } else {
        Vec::new()
    };

    if has_insert_statement_triggers {
        super::triggers::fire_statement_triggers(
            &mut wtx,
            schema,
            &table_schema.name,
            crate::parser::TriggerTiming::Before,
            super::triggers::FireEvent::Insert,
            &table_schema.columns,
            &[],
            &[],
        )?;
    }

    let plain_insert = compiled_conflict.is_none();
    let single_int_pk = is_single_int_pk(table_schema);
    let mut min_inserted_pk: Option<i64> = None;
    let (has_before_insert_triggers, has_after_insert_triggers, has_after_update_triggers) =
        row_insert_trigger_flags(schema, &table_schema.name);
    let capture_insert_row =
        returning_rows.is_some() || has_insert_statement_triggers || has_after_insert_triggers;

    // The autocommit twin of the in-transaction row loop, and cancellable for
    // the same reason: neither reaches a scan.
    for idx in 0..total {
        if let Some(t) = &cancel {
            t.check().map_err(SqlError::Storage)?;
        }
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
                    eval_const_expr_with_cancel(expr, cancel.as_ref())?
                };
                let col_idx = col_indices[i];
                let col = &table_schema.columns[col_idx];
                row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    coerce_for_column(val, col, strict)?
                };
            }
        } else if let Some(sel) = select_rows.as_mut() {
            bind_selected_row(&mut sel[idx], &mut row, &col_indices, table_schema)?;
        }

        for &(pos, def_expr) in &defaults {
            let val = eval_const_expr_with_cancel(def_expr, cancel.as_ref())?;
            let col = &table_schema.columns[pos];
            if !val.is_null() {
                row[pos] = coerce_for_column(val, col, strict)?;
            }
        }

        if let Some(gen_map) = row_col_map_for_gen {
            for &(pos, gen_expr) in &generated_cols {
                let val = eval_expr(
                    gen_expr,
                    &EvalCtx::new(gen_map, &row).with_cancel(cancel.as_ref()),
                )?;
                let col = &table_schema.columns[pos];
                row[pos] = if val.is_null() {
                    Value::Null
                } else {
                    coerce_for_column(val, col, strict)?
                };
            }
        }

        for col in &table_schema.columns {
            if !col.nullable && row[col.position as usize].is_null() {
                return Err(SqlError::NotNullViolation(col.name.clone()));
            }
        }

        if let Some(col_map) = check_col_map {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(
                        check,
                        &EvalCtx::new(col_map, &row).with_cancel(cancel.as_ref()),
                    )?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(
                    &tc.expr,
                    &EvalCtx::new(col_map, &row).with_cancel(cancel.as_ref()),
                )?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        for fk in &table_schema.foreign_keys {
            super::fk::check_row_reference(
                &mut wtx,
                schema,
                table_schema,
                fk,
                &row,
                &mut fk_key_buf,
            )?;
        }

        if has_before_insert_triggers {
            super::triggers::fire_row_triggers(
                &mut wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::Before,
                super::triggers::FireEvent::Insert,
                None,
                Some(row.clone()),
                &table_schema.columns,
            )?;
        }

        for (j, &i) in pk_indices.iter().enumerate() {
            pk_values[j] = std::mem::replace(&mut row[i], Value::Null);
        }
        encode_composite_key_into(&pk_values, &mut key_buf);
        if plain_insert && single_int_pk {
            if let Value::Integer(id) = &pk_values[0] {
                min_inserted_pk = Some(min_inserted_pk.map_or(*id, |m| m.min(*id)));
            }
        }

        for (j, &i) in non_pk.iter().enumerate() {
            let col = &table_schema.columns[i];
            if matches!(
                col.generated_kind,
                Some(crate::parser::GeneratedKind::Virtual)
            ) {
                value_values[enc_pos[j] as usize] = Value::Null;
            } else {
                value_values[enc_pos[j] as usize] = std::mem::replace(&mut row[i], Value::Null);
            }
        }
        encode_row_into(&value_values, &mut value_buf);

        if key_buf.len() > citadel_core::MAX_KEY_SIZE {
            return Err(SqlError::KeyTooLarge {
                size: key_buf.len(),
                max: citadel_core::MAX_KEY_SIZE,
            });
        }
        if value_buf.len() > citadel_core::MAX_VALUE_SIZE {
            return Err(SqlError::RowTooLarge {
                size: value_buf.len(),
                max: citadel_core::MAX_VALUE_SIZE,
            });
        }

        match compiled_conflict.as_ref() {
            None => {
                let is_new = wtx
                    .table_insert_if_absent(table_schema.name.as_bytes(), &key_buf, &value_buf)
                    .map_err(SqlError::Storage)?;
                if !is_new {
                    return Err(SqlError::DuplicateKey);
                }
                if !table_schema.indices.is_empty() || capture_insert_row {
                    restore_insert_row(table_schema, &pk_values, &mut value_values, &mut row);
                    if !table_schema.indices.is_empty() {
                        insert_index_entries(&mut wtx, table_schema, &row, &pk_values)?;
                    }
                }
                if capture_insert_row {
                    materialize_insert_result_virtuals(
                        table_schema,
                        &required_virtuals.after_insert,
                        &mut row,
                        cancel.as_ref(),
                    )?;
                }
                if has_after_insert_triggers {
                    super::triggers::fire_row_triggers(
                        &mut wtx,
                        schema,
                        &table_schema.name,
                        crate::parser::TriggerTiming::After,
                        super::triggers::FireEvent::Insert,
                        None,
                        Some(row.clone()),
                        &table_schema.columns,
                    )?;
                }
                if has_insert_statement_triggers {
                    stmt_new_rows.push(row.clone());
                }
                count += 1;
                if let Some(buf) = returning_rows.as_mut() {
                    buf.push((None, Some(row.clone())));
                }
            }
            Some(oc) => {
                let oc_ref: &CompiledOnConflict = oc;
                let needs_row = upsert_needs_row(oc_ref, table_schema);
                if needs_row {
                    restore_insert_row(table_schema, &pk_values, &mut value_values, &mut row);
                }
                let outcome = apply_insert_with_conflict(
                    &mut wtx,
                    schema,
                    table_schema,
                    &key_buf,
                    &value_buf,
                    &row,
                    &pk_values,
                    oc_ref,
                    row_col_map.unwrap(),
                    cancel.as_ref(),
                    // Trigger dispatch needs the Updated outcome's rows too.
                    stmt.returning.is_some() || has_after_update_triggers,
                )?;
                match outcome {
                    InsertRowOutcome::Inserted => {
                        if capture_insert_row {
                            if !needs_row {
                                restore_insert_row(
                                    table_schema,
                                    &pk_values,
                                    &mut value_values,
                                    &mut row,
                                );
                            }
                            materialize_insert_result_virtuals(
                                table_schema,
                                &required_virtuals.after_insert,
                                &mut row,
                                cancel.as_ref(),
                            )?;
                        }
                        count += 1;
                        if let Some(buf) = returning_rows.as_mut() {
                            buf.push((None, Some(row.clone())));
                        }
                        if has_insert_statement_triggers {
                            stmt_new_rows.push(row.clone());
                        }
                        if has_after_insert_triggers {
                            super::triggers::fire_row_triggers(
                                &mut wtx,
                                schema,
                                &table_schema.name,
                                crate::parser::TriggerTiming::After,
                                super::triggers::FireEvent::Insert,
                                None,
                                Some(row.clone()),
                                &table_schema.columns,
                            )?;
                        }
                    }
                    InsertRowOutcome::Updated { rows } => {
                        count += 1;
                        if let Some((old, new)) = rows {
                            if let Some(buf) = returning_rows.as_mut() {
                                buf.push((Some(old.clone()), Some(new.clone())));
                            }
                            if has_after_update_triggers {
                                let changed_cols: Vec<String> = match oc_ref {
                                    CompiledOnConflict::DoUpdate { assignments, .. } => assignments
                                        .iter()
                                        .map(|(col_idx, _)| {
                                            table_schema.columns[*col_idx].name.clone()
                                        })
                                        .collect(),
                                    _ => Vec::new(),
                                };
                                super::triggers::fire_row_triggers(
                                    &mut wtx,
                                    schema,
                                    &table_schema.name,
                                    crate::parser::TriggerTiming::After,
                                    super::triggers::FireEvent::Update {
                                        changed_columns: &changed_cols,
                                    },
                                    Some(old),
                                    Some(new),
                                    &table_schema.columns,
                                )?;
                            }
                        }
                    }
                    InsertRowOutcome::Skipped => {}
                }
            }
        }
    }

    if has_insert_statement_triggers {
        super::triggers::fire_statement_triggers(
            &mut wtx,
            schema,
            &table_schema.name,
            crate::parser::TriggerTiming::After,
            super::triggers::FireEvent::Insert,
            &table_schema.columns,
            &[],
            &stmt_new_rows,
        )?;
    }

    mark_insert_dml(
        schema,
        &table_schema.name,
        !plain_insert,
        single_int_pk,
        min_inserted_pk,
        count,
    );

    if let (Some(returning_cols), Some(rows)) = (stmt.returning.as_ref(), returning_rows) {
        let qr = super::helpers::project_returning(
            table_schema,
            returning_cols,
            &rows,
            wtx.cancel_token(),
        )?;
        super::helpers::drain_deferred_fk_checks(&mut wtx, schema)?;
        super::commit_with_ann_publication(wtx, schema)?;
        return Ok(ExecutionResult::Query(qr));
    }

    super::helpers::drain_deferred_fk_checks(&mut wtx, schema)?;
    super::commit_with_ann_publication(wtx, schema)?;
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
    for gb in &stmt.group_by {
        if has_subquery(gb) {
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
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
) -> Result<Expr> {
    match expr {
        Expr::InSubquery {
            expr: e,
            subquery,
            negated,
        } => {
            let inner = materialize_expr(e, exec_sub)?;
            let selected = exec_sub(subquery)?;
            let qr = &selected.result;
            if !qr.columns.is_empty() && qr.columns.len() != 1 {
                return Err(SqlError::SubqueryMultipleColumns);
            }
            let mut values = rustc_hash::FxHashSet::default();
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
                // Carried because the set is bare values from here on: `x IN (SELECT y)`
                // compares as `x = y` does, and there the collation may come from `y`.
                collation: selected.collation_at(0),
            })
        }
        Expr::ScalarSubquery(subquery) => {
            let qr = exec_sub(subquery)?.result;
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
            let qr = exec_sub(subquery)?.result;
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
            collation,
        } => Ok(Expr::InSet {
            expr: Box::new(materialize_expr(e, exec_sub)?),
            values: values.clone(),
            has_null: *has_null,
            negated: *negated,
            collation: *collation,
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
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => Ok(Expr::IsDistinctFrom {
            left: Box::new(materialize_expr(left, exec_sub)?),
            right: Box::new(materialize_expr(right, exec_sub)?),
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
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
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
            SelectColumn::AllFromOld => Ok(SelectColumn::AllFromOld),
            SelectColumn::AllFromNew => Ok(SelectColumn::AllFromNew),
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
                output_name: ob.output_name.clone(),
                output_ordinal: ob.output_ordinal,
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
                subquery: j.subquery.clone(),
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
        from_subquery: stmt.from_subquery.clone(),
        from_args: stmt.from_args.clone(),
        from_json_table: stmt.from_json_table.clone(),
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
) -> Result<CteRows> {
    let mut rtx = db.begin_read();
    exec_subquery_with_read(&mut rtx, schema, stmt, ctes)
}

/// A subquery's rows carry the collation of the columns they were selected from, because
/// `x IN (SELECT y ...)` has to compare the way `x = y` does - and there the collation may
/// come from `y` rather than from `x`.
fn subquery_rows(
    schema: &SchemaManager,
    ctes: &CteContext,
    stmt: &SelectStmt,
    result: ExecutionResult,
) -> CteRows {
    let ExecutionResult::Query(result) = result else {
        return CteRows::binary(QueryResult {
            columns: vec![],
            rows: vec![],
        });
    };
    let body = QueryBody::Select(Box::new(stmt.clone()));
    let collations = body_output_collations(schema, ctes, &body, result.columns.len());
    CteRows::new(result, collations)
}

pub(super) fn exec_subquery_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<CteRows> {
    let result = super::exec_select_with_read(rtx, schema, stmt, ctes)?;
    Ok(subquery_rows(schema, ctes, stmt, result))
}

pub(super) fn exec_subquery_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<CteRows> {
    let result = super::exec_select_in_txn(wtx, schema, stmt, ctes)?;
    Ok(subquery_rows(schema, ctes, stmt, result))
}

pub(super) fn update_has_subquery(stmt: &UpdateStmt) -> bool {
    stmt.where_clause.as_ref().is_some_and(has_subquery)
        || stmt.assignments.iter().any(|(_, e)| has_subquery(e))
}

pub(super) fn materialize_update(
    stmt: &UpdateStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
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
        returning: stmt.returning.clone(),
    })
}

pub(super) fn delete_has_subquery(stmt: &DeleteStmt) -> bool {
    stmt.where_clause.as_ref().is_some_and(has_subquery)
}

pub(super) fn materialize_delete(
    stmt: &DeleteStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
) -> Result<DeleteStmt> {
    let where_clause = stmt
        .where_clause
        .as_ref()
        .map(|e| materialize_expr(e, exec_sub))
        .transpose()?;
    Ok(DeleteStmt {
        table: stmt.table.clone(),
        where_clause,
        returning: stmt.returning.clone(),
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
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
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
        returning: stmt.returning.clone(),
    })
}

pub(super) fn materialize_query_body(
    body: &QueryBody,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
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
        QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => Ok(body.clone()),
    }
}

pub(super) fn exec_query_body_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    match body {
        QueryBody::Select(sel) => super::exec_select_with_read(rtx, schema, sel, ctes),
        QueryBody::Compound(comp) => exec_compound_select_with_read(rtx, schema, comp, ctes),
        QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => Err(
            SqlError::Unsupported("DML CTE bodies require an active write transaction".into()),
        ),
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
        QueryBody::Insert(ins) => exec_insert_in_txn_with_ctes(wtx, schema, ins, &[], ctes),
        QueryBody::Update(upd) => super::exec_update_in_txn(wtx, schema, upd),
        QueryBody::Delete(del) => super::exec_delete_in_txn(wtx, schema, del),
    }
}

pub(super) fn exec_query_body_read(
    db: &Database,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<QueryResult> {
    let mut rtx = db.begin_read();
    exec_query_body_with_read_qr(&mut rtx, schema, body, ctes)
}

pub(super) fn exec_query_body_with_read_qr(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    body: &QueryBody,
    ctes: &CteContext,
) -> Result<QueryResult> {
    match exec_query_body_with_read(rtx, schema, body, ctes)? {
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

pub(super) fn exec_compound_select_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    comp: &CompoundSelect,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let cancel = rtx.cancel_token().cloned();
    let left_qr = match exec_query_body_with_read(rtx, schema, &comp.left, ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => QueryResult {
            columns: vec![],
            rows: vec![],
        },
    };
    let right_qr = match exec_query_body_with_read(rtx, schema, &comp.right, ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => QueryResult {
            columns: vec![],
            rows: vec![],
        },
    };
    apply_set_operation(schema, ctes, comp, left_qr, right_qr, cancel.as_ref())
}

pub(super) fn exec_compound_select_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    comp: &CompoundSelect,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let cancel = wtx.cancel_token().cloned();
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
    apply_set_operation(schema, ctes, comp, left_qr, right_qr, cancel.as_ref())
}

/// The collation of each output column of a query body, so a set operation compares its rows
/// the way `=` would. SQL takes a compound's collation from its leftmost branch.
/// `width` is the row width the caller will compare; a disagreeing resolution is
/// discarded rather than misapplied.
pub(super) fn body_output_collations(
    schema: &SchemaManager,
    ctes: &CteContext,
    body: &QueryBody,
    width: usize,
) -> Vec<crate::types::Collation> {
    let binary = || vec![crate::types::Collation::Binary; width];
    match body_output_columns(schema, ctes, body) {
        Some(columns) if columns.len() == width => {
            columns.iter().map(|col| col.collation).collect()
        }
        _ => binary(),
    }
}

/// The collations of a whole query's output, for a caller holding a `SelectQuery` rather
/// than a bare body - a view, or a derived table.
pub(super) fn query_output_collations(
    schema: &SchemaManager,
    ctes: &CteContext,
    query: &SelectQuery,
    width: usize,
) -> Vec<crate::types::Collation> {
    match query_output_columns(schema, ctes, query) {
        Some(columns) if columns.len() == width => {
            columns.iter().map(|col| col.collation).collect()
        }
        _ => vec![crate::types::Collation::Binary; width],
    }
}

/// The columns a whole query produces, resolving its own WITH definitions first so a body
/// reading from one of them knows what its columns collate as. Nothing is executed here:
/// only the shape of each relation is needed.
fn query_output_columns(
    schema: &SchemaManager,
    outer: &CteContext,
    query: &SelectQuery,
) -> Option<Vec<ColumnDef>> {
    if query.ctes.is_empty() {
        return body_output_columns(schema, outer, &query.body);
    }
    let mut ctx = outer.clone();
    for cte in &query.ctes {
        // A recursive CTE takes its shape from its anchor, which is the leftmost branch
        // `body_output_columns` already reads.
        let Some(columns) = body_output_columns(schema, &ctx, &cte.body) else {
            continue;
        };
        let names = if cte.column_aliases.is_empty() {
            columns.iter().map(|col| col.name.clone()).collect()
        } else {
            cte.column_aliases.clone()
        };
        let collations = columns.iter().map(|col| col.collation).collect();
        ctx.insert(cte.name.clone(), CteRows::shape(names, collations).shared());
    }
    body_output_columns(schema, &ctx, &query.body)
}

/// The columns a query body produces: their names, and the collation each one inherits from
/// the column it was projected from.
fn body_output_columns(
    schema: &SchemaManager,
    ctes: &CteContext,
    body: &QueryBody,
) -> Option<Vec<ColumnDef>> {
    let (projection, source) = match body {
        QueryBody::Select(sel) => (&sel.columns, select_source_columns(schema, ctes, sel)?),
        QueryBody::Compound(inner) => return body_output_columns(schema, ctes, &inner.left),
        QueryBody::Insert(stmt) => (
            stmt.returning.as_ref()?,
            schema.get(&stmt.table)?.columns.clone(),
        ),
        QueryBody::Update(stmt) => (
            stmt.returning.as_ref()?,
            schema.get(&stmt.table)?.columns.clone(),
        ),
        QueryBody::Delete(stmt) => (
            stmt.returning.as_ref()?,
            schema.get(&stmt.table)?.columns.clone(),
        ),
    };
    let col_map = crate::eval::ColumnMap::new(&source);
    // `*` stands for every source column, so it contributes one output column each. Naming
    // it as a single placeholder the way `build_output_columns` does would make the widths
    // disagree, and a resolution that does not match the row it describes is discarded.
    let mut out = Vec::with_capacity(projection.len());
    for col in projection {
        match col {
            SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                out.extend(source.iter().cloned());
            }
            SelectColumn::Expr { expr, alias } => {
                let name = alias
                    .clone()
                    .unwrap_or_else(|| super::helpers::expr_display_name(expr));
                out.push(super::helpers::projected_column(
                    name,
                    out.len(),
                    super::helpers::expr_collation(expr, &col_map),
                ));
            }
        }
    }
    for (position, col) in out.iter_mut().enumerate() {
        col.position = position as u16;
    }
    Some(out)
}

/// The columns a select projects FROM: its own relation, then each joined one in order,
/// named the way the join executor names them so a qualified reference still resolves.
fn select_source_columns(
    schema: &SchemaManager,
    ctes: &CteContext,
    sel: &SelectStmt,
) -> Option<Vec<ColumnDef>> {
    // A table function or a JSON table invents its columns rather than reading them from a
    // relation, so none of them carries a collation to inherit.
    if sel.from_json_table.is_some() || sel.from_args.is_some() {
        return None;
    }
    if sel.from.is_empty() && sel.from_subquery.is_none() {
        return Some(Vec::new());
    }
    let mut out = relation_columns(
        schema,
        ctes,
        sel.from_subquery.as_deref(),
        &sel.from,
        sel.from_alias.as_deref().unwrap_or(&sel.from),
    )?;
    for join in &sel.joins {
        let alias = join.table.alias.as_deref().unwrap_or(&join.table.name);
        let joined = relation_columns(
            schema,
            ctes,
            join.subquery.as_deref(),
            &join.table.name,
            alias,
        )?;
        out.extend(joined);
    }
    for (position, col) in out.iter_mut().enumerate() {
        col.position = position as u16;
    }
    Some(out)
}

/// One relation of a FROM or a JOIN: a base table, a materialized CTE, or a derived table
/// resolved from its own query.
fn relation_columns(
    schema: &SchemaManager,
    ctes: &CteContext,
    derived: Option<&DerivedTable>,
    name: &str,
    alias: &str,
) -> Option<Vec<ColumnDef>> {
    let table = match derived {
        Some(table) => TableSchema::new(
            alias.into(),
            query_output_columns(schema, ctes, &table.query)?,
            vec![],
            vec![],
            vec![],
            vec![],
        ),
        None => match ctes.get(&name.to_ascii_lowercase()) {
            Some(cte) => super::cte::build_cte_schema(alias, cte),
            None => schema.get(&schema.resolve_temp(name))?.clone(),
        },
    };
    let mut out = Vec::new();
    super::join::extend_joined_columns(&mut out, &(alias.to_string(), &table));
    Some(out)
}

pub(super) fn apply_set_operation(
    schema: &SchemaManager,
    ctes: &CteContext,
    comp: &CompoundSelect,
    left_qr: QueryResult,
    right_qr: QueryResult,
    cancel: Option<&citadel::CancelToken>,
) -> Result<ExecutionResult> {
    super::check_cancelled(cancel)?;
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
    // A set operation decides equality by hashing whole rows, so a column collation has to
    // be folded into the key exactly as GROUP BY and DISTINCT fold theirs.
    let key_colls = body_output_collations(schema, ctes, &comp.left, columns.len());
    let key = |row: &Vec<Value>| fold_key(row, &key_colls);
    let mut work = 0usize;
    let mut check_work = || -> Result<()> {
        // Set operations can spend most of their time hashing materialized
        // rows. Keep cancellation responsive without adding an atomic load to
        // every hash-table probe.
        if work & 0xff == 0 {
            super::check_cancelled(cancel)?;
        }
        work = work.wrapping_add(1);
        Ok(())
    };

    let mut rows = match (&comp.op, comp.all) {
        (SetOp::Union, true) => {
            // Reuse the left branch's allocation. Building a third vector
            // copies every row header even though UNION ALL does no hashing.
            let mut rows = left_qr.rows;
            rows.reserve(right_qr.rows.len());
            if cancel.is_none() {
                rows.extend(right_qr.rows);
            } else {
                for row in right_qr.rows {
                    check_work()?;
                    rows.push(row);
                }
            }
            rows
        }
        (SetOp::Union, false) => {
            let mut seen = super::helpers::RowKeys::new(key_colls.clone());
            let mut rows = Vec::new();
            for row in left_qr.rows.into_iter().chain(right_qr.rows) {
                check_work()?;
                if seen.insert(&row) {
                    rows.push(row);
                }
            }
            rows
        }
        (SetOp::Intersect, true) => {
            let mut right_counts: FxHashMap<Vec<Value>, usize> = FxHashMap::default();
            for row in &right_qr.rows {
                check_work()?;
                *right_counts.entry(key(row)).or_insert(0) += 1;
            }
            let mut rows = Vec::new();
            for row in left_qr.rows {
                check_work()?;
                if let Some(count) = right_counts.get_mut(&key(&row)) {
                    if *count > 0 {
                        *count -= 1;
                        rows.push(row);
                    }
                }
            }
            rows
        }
        (SetOp::Intersect, false) => {
            let mut right_set = super::helpers::RowKeys::new(key_colls.clone());
            for row in &right_qr.rows {
                check_work()?;
                right_set.insert(row);
            }
            let mut seen = super::helpers::RowKeys::new(key_colls.clone());
            let mut rows = Vec::new();
            for row in left_qr.rows {
                check_work()?;
                if right_set.contains_row(&row) && seen.insert(&row) {
                    rows.push(row);
                }
            }
            rows
        }
        (SetOp::Except, true) => {
            let mut right_counts: FxHashMap<Vec<Value>, usize> = FxHashMap::default();
            for row in &right_qr.rows {
                check_work()?;
                *right_counts.entry(key(row)).or_insert(0) += 1;
            }
            let mut rows = Vec::new();
            for row in left_qr.rows {
                check_work()?;
                if let Some(count) = right_counts.get_mut(&key(&row)) {
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
            let mut right_set = super::helpers::RowKeys::new(key_colls.clone());
            for row in &right_qr.rows {
                check_work()?;
                right_set.insert(row);
            }
            let mut seen = super::helpers::RowKeys::new(key_colls.clone());
            let mut rows = Vec::new();
            for row in left_qr.rows {
                check_work()?;
                if !right_set.contains_row(&row) && seen.insert(&row) {
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
                generated_expr: None,
                generated_sql: None,
                generated_kind: None,
                // Carried from the branch, so ORDER BY over a compound sorts the way the
                // same column sorts inside it.
                collation: key_colls
                    .get(i)
                    .copied()
                    .unwrap_or(crate::types::Collation::Binary),
            })
            .collect();
        sort_rows(&mut rows, &comp.order_by, &col_defs, cancel)?;
    }

    if let Some(ref offset_expr) = comp.offset {
        let offset = eval_row_count(offset_expr)?;
        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }
    }

    if let Some(ref limit_expr) = comp.limit {
        let limit = eval_row_count(limit_expr)?;
        rows.truncate(limit);
    }

    super::check_cancelled(cancel)?;
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
    INSERT_SCRATCH.with(|slot| match slot.try_borrow_mut() {
        Ok(mut borrowed) => f(&mut borrowed),
        Err(_) => {
            let mut local = InsertBufs::new();
            f(&mut local)
        }
    })
}

pub(super) struct UpsertBufs {
    old_row: Vec<Value>,
    new_row: Vec<Value>,
    value_values: Vec<Value>,
    new_value_buf: Vec<u8>,
    materializer: UpdateRowMaterializer,
}

impl UpsertBufs {
    pub(super) fn new() -> Self {
        Self {
            old_row: Vec::new(),
            new_row: Vec::new(),
            value_values: Vec::new(),
            new_value_buf: Vec::with_capacity(256),
            materializer: UpdateRowMaterializer::default(),
        }
    }
}

pub fn exec_insert_in_txn(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
) -> Result<ExecutionResult> {
    // This public lane is also called directly, without `Connection::guarded`
    // or `execute_in_txn` around it.
    super::reject_legacy_volatile_schema(schema)?;
    wtx.check_usable().map_err(SqlError::Storage)?;
    super::check_cancelled(wtx.cancel_token())?;
    let mutation_marker = wtx.mutation_marker();
    let mut outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_insert_scratch(|bufs| {
            exec_insert_in_txn_impl(
                wtx,
                schema,
                stmt,
                params,
                bufs,
                None,
                &CteContext::default(),
            )
        })
    })) {
        Ok(outcome) => outcome,
        Err(payload) => {
            wtx.mark_failed();
            std::panic::resume_unwind(payload)
        }
    };
    if outcome.is_ok() {
        if let Err(err) = super::check_cancelled(wtx.cancel_token()) {
            outcome = Err(err);
        }
    }
    // `Connection` reaches this public lane directly. A later duplicate,
    // trigger, or cancellation error must not leave an earlier row committable.
    if wtx.mutated_since(mutation_marker) {
        if let Err(error) = &outcome {
            super::mark_write_statement_failed(wtx, error);
        }
    }
    outcome
}

pub(super) fn exec_insert_in_txn_with_ctes(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
    outer_ctes: &CteContext,
) -> Result<ExecutionResult> {
    with_insert_scratch(|bufs| {
        exec_insert_in_txn_impl(wtx, schema, stmt, params, bufs, None, outer_ctes)
    })
}

fn exec_insert_in_txn_cached(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
    cache: &InsertCache,
) -> Result<ExecutionResult> {
    with_insert_scratch(|bufs| {
        exec_insert_in_txn_impl(
            wtx,
            schema,
            stmt,
            params,
            bufs,
            Some(cache),
            &CteContext::default(),
        )
    })
}

fn exec_insert_in_txn_impl(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &InsertStmt,
    params: &[Value],
    bufs: &mut InsertBufs,
    cache: Option<&InsertCache>,
    outer_ctes: &CteContext,
) -> Result<ExecutionResult> {
    if let Some(plan) = super::insert_copy::CopyPlan::new(schema, stmt, outer_ctes) {
        return plan.execute(wtx, schema);
    }
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

    let view_lookup_key = stmt.table.to_ascii_lowercase();
    if let Some(view_def) = schema.get_view(&view_lookup_key) {
        if super::triggers::has_instead_of(
            schema,
            &view_lookup_key,
            super::triggers::FireEvent::Insert,
        ) {
            let aliases = view_def.column_aliases.clone();
            return exec_instead_of_view_insert_in_txn(
                wtx,
                schema,
                &view_lookup_key,
                &aliases,
                stmt,
                params,
            );
        }
        return Err(SqlError::CannotModifyView(stmt.table.clone()));
    }
    if schema.get_matview(&view_lookup_key).is_some() {
        return Err(SqlError::CannotModifyView(format!(
            "materialized view '{}' is read-only — use REFRESH MATERIALIZED VIEW",
            stmt.table
        )));
    }

    let table_schema = schema
        .get(&stmt.table)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
    let strict = table_schema.is_strict();
    if table_schema.has_ann_index() {
        super::ann_persist::purge_segment(wtx, &table_schema.name)?;
    }

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

    if cache.is_none() {
        for &ci in &bufs.col_indices {
            if table_schema.columns[ci].generated_kind.is_some() {
                return Err(SqlError::CannotInsertIntoGeneratedColumn(
                    table_schema.columns[ci].name.clone(),
                ));
            }
        }
    }

    let generated_cols_uncached: Vec<(usize, &Expr, FastGenEval)>;
    let cached_gen_positions: &[usize];
    let cached_gen_fast_evals: &[FastGenEval];
    let late_virtual_positions: &[usize];
    let uncached_virtuals;
    if let Some(c) = cache {
        cached_gen_positions = &c.generated_col_positions;
        cached_gen_fast_evals = &c.generated_fast_evals;
        late_virtual_positions = &c.late_virtual_positions;
        generated_cols_uncached = Vec::new();
    } else {
        cached_gen_positions = &[];
        cached_gen_fast_evals = &[];
        uncached_virtuals = required_insert_virtuals(schema, table_schema, stmt);
        late_virtual_positions = &uncached_virtuals.after_insert;
        generated_cols_uncached = table_schema
            .columns
            .iter()
            .filter(|c| {
                matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored))
                    || uncached_virtuals
                        .before_insert
                        .contains(&(c.position as usize))
            })
            .map(|c| {
                let expr = c.generated_expr.as_ref().unwrap();
                let fe = detect_fast_gen_eval(expr, table_schema);
                (c.position as usize, expr, fe)
            })
            .collect();
    }
    let has_gen_cols = !cached_gen_positions.is_empty() || !generated_cols_uncached.is_empty();
    let row_col_map_for_gen: Option<&ColumnMap> = has_gen_cols.then(|| table_schema.column_map());

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
        Some(table_schema.column_map())
    } else {
        None
    };

    let (pk_indices, non_pk, enc_pos, phys_count, dropped): (
        &[usize],
        &[usize],
        &[u16],
        usize,
        &[u16],
    ) = if let Some(c) = cache {
        (
            &c.pk_indices,
            &c.non_pk_indices,
            &c.encoding_positions,
            c.phys_count,
            &c.dropped_non_pk_slots,
        )
    } else {
        (
            table_schema.pk_indices(),
            table_schema.non_pk_indices(),
            table_schema.encoding_positions(),
            table_schema.physical_non_pk_count(),
            table_schema.dropped_non_pk_slots(),
        )
    };

    bufs.row.resize(table_schema.columns.len(), Value::Null);
    bufs.pk_values.resize(pk_indices.len(), Value::Null);
    bufs.value_values.resize(phys_count, Value::Null);

    let table_bytes = table_schema.name.as_bytes();
    let has_fks = !table_schema.foreign_keys.is_empty();
    let has_indices = !table_schema.indices.is_empty();
    let has_defaults = !defaults.is_empty();

    let compiled_conflict: Option<Arc<CompiledOnConflict>> = match (cache, &stmt.on_conflict) {
        (Some(c), Some(_)) if c.on_conflict.is_some() => c.on_conflict.clone(),
        (_, Some(oc)) => Some(Arc::new(compile_on_conflict(oc, table_schema)?)),
        (_, None) => None,
    };

    let row_col_map: Option<&ColumnMap> = compiled_conflict
        .is_some()
        .then(|| table_schema.column_map());

    let cancel = wtx.cancel_token().cloned();
    let mut select_rows = match &stmt.source {
        InsertSource::Select(sq) => {
            let insert_ctes = super::materialize_all_ctes_with_outer(
                &sq.ctes,
                sq.recursive,
                outer_ctes,
                cancel.as_ref(),
                &mut |body, ctx| {
                    let result = exec_query_body_write(wtx, schema, body, ctx)?;
                    let collations =
                        body_output_collations(schema, ctx, body, result.columns.len());
                    Ok(CteRows::new(result, collations))
                },
            )?;
            let qr = exec_query_body_write(wtx, schema, &sq.body, &insert_ctes)?;
            Some(insert_select_rows(qr, insert_columns.len())?)
        }
        InsertSource::Values(_) => None,
    };

    let mut count: u64 = 0;
    let mut returning_rows: Option<Vec<super::helpers::ReturningRow>> =
        stmt.returning.as_ref().map(|_| Vec::new());

    let plain_insert = compiled_conflict.is_none();
    let single_int_pk = is_single_int_pk(table_schema);
    let mut min_inserted_pk: Option<i64> = None;

    let values = match &stmt.source {
        InsertSource::Values(rows) => Some(rows.as_slice()),
        InsertSource::Select(_) => None,
    };
    let total = match (values, select_rows.as_deref()) {
        (Some(rows), _) => rows.len(),
        (_, Some(rows)) => rows.len(),
        _ => 0,
    };

    let has_insert_statement_triggers_impl =
        schema.triggers_for(&table_schema.name).iter().any(|t| {
            t.enabled
                && t.granularity == crate::parser::TriggerGranularity::ForEachStatement
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Insert))
        });
    let mut stmt_new_rows_impl: Vec<Vec<Value>> = if has_insert_statement_triggers_impl {
        Vec::with_capacity(total)
    } else {
        Vec::new()
    };
    if has_insert_statement_triggers_impl {
        super::triggers::fire_statement_triggers(
            wtx,
            schema,
            &table_schema.name,
            crate::parser::TriggerTiming::Before,
            super::triggers::FireEvent::Insert,
            &table_schema.columns,
            &[],
            &[],
        )?;
    }

    let (has_before_insert_triggers, has_after_insert_triggers, has_after_update_triggers) =
        row_insert_trigger_flags(schema, &table_schema.name);

    let capture_insert_row =
        returning_rows.is_some() || has_insert_statement_triggers_impl || has_after_insert_triggers;
    let skip_row_clear = cache.is_some_and(|c| c.row_fully_overwritten);
    // A wide VALUES list or an INSERT ... SELECT is a row loop like any scan,
    // and it never enters one, so this is the only place a cancel can land.
    let cancel = wtx.cancel_token().cloned();
    for idx in 0..total {
        if let Some(t) = &cancel {
            t.check().map_err(SqlError::Storage)?;
        }
        if !skip_row_clear {
            for v in bufs.row.iter_mut() {
                *v = Value::Null;
            }
        }

        if let Some(value_rows) = values {
            if let Some(plan) = cache.and_then(|c| c.bind_plan.as_ref()) {
                for action in plan {
                    match action {
                        BindAction::Param {
                            param_idx,
                            col_idx,
                            target,
                        } => {
                            let v = &params[*param_idx];
                            bufs.row[*col_idx] = if v.is_null() {
                                Value::Null
                            } else if v.data_type() == *target {
                                v.clone()
                            } else {
                                coerce_for_column(
                                    v.clone(),
                                    &table_schema.columns[*col_idx],
                                    strict,
                                )?
                            };
                        }
                        BindAction::Literal { value, col_idx } => {
                            bufs.row[*col_idx] = value.clone();
                        }
                    }
                }
            } else {
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
                        _ => eval_const_expr_with_cancel(expr, cancel.as_ref())?,
                    };
                    let col_idx = bufs.col_indices[i];
                    let col = &table_schema.columns[col_idx];
                    bufs.row[col_idx] = coerce_for_column(val, col, strict)?;
                }
            }
        } else if let Some(sel) = select_rows.as_mut() {
            bind_selected_row(
                &mut sel[idx],
                &mut bufs.row,
                &bufs.col_indices,
                table_schema,
            )?;
        }

        if has_defaults {
            for &(pos, def_expr) in &defaults {
                let val = eval_const_expr_with_cancel(def_expr, cancel.as_ref())?;
                let col = &table_schema.columns[pos];
                if !val.is_null() {
                    bufs.row[pos] = coerce_for_column(val, col, strict)?;
                }
            }
        }

        if let Some(gen_map) = row_col_map_for_gen {
            if cache.is_some() {
                for (pos, fast) in cached_gen_positions
                    .iter()
                    .copied()
                    .zip(cached_gen_fast_evals.iter())
                {
                    let gen_expr = table_schema.columns[pos].generated_expr.as_ref().unwrap();
                    let val = eval_fast_gen_with_cancel(
                        fast,
                        gen_expr,
                        &bufs.row,
                        gen_map,
                        cancel.as_ref(),
                    )?;
                    let col = &table_schema.columns[pos];
                    bufs.row[pos] = coerce_for_column(val, col, strict)?;
                }
            } else {
                for (pos, gen_expr, fast) in &generated_cols_uncached {
                    let val = eval_fast_gen_with_cancel(
                        fast,
                        gen_expr,
                        &bufs.row,
                        gen_map,
                        cancel.as_ref(),
                    )?;
                    let col = &table_schema.columns[*pos];
                    bufs.row[*pos] = coerce_for_column(val, col, strict)?;
                }
            }
        }

        if let Some(c) = cache {
            for &pos in &c.not_null_indices {
                if bufs.row[pos as usize].is_null() {
                    return Err(SqlError::NotNullViolation(
                        table_schema.columns[pos as usize].name.clone(),
                    ));
                }
            }
        } else {
            for col in &table_schema.columns {
                if !col.nullable && bufs.row[col.position as usize].is_null() {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
            }
        }

        if let Some(col_map) = check_col_map {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(
                        check,
                        &EvalCtx::new(col_map, &bufs.row).with_cancel(cancel.as_ref()),
                    )?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(
                    &tc.expr,
                    &EvalCtx::new(col_map, &bufs.row).with_cancel(cancel.as_ref()),
                )?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        if has_fks {
            for fk in &table_schema.foreign_keys {
                super::fk::check_row_reference(
                    wtx,
                    schema,
                    table_schema,
                    fk,
                    &bufs.row,
                    &mut bufs.fk_key_buf,
                )?;
            }
        }

        if has_before_insert_triggers {
            super::triggers::fire_row_triggers(
                wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::Before,
                super::triggers::FireEvent::Insert,
                None,
                Some(bufs.row.clone()),
                &table_schema.columns,
            )?;
        }

        for (j, &i) in pk_indices.iter().enumerate() {
            bufs.pk_values[j] = std::mem::replace(&mut bufs.row[i], Value::Null);
        }
        match cache.map(|c| c.single_int_pk).unwrap_or(false) {
            true => match bufs.pk_values[0] {
                Value::Integer(v) => crate::encoding::encode_int_key_into(v, &mut bufs.key_buf),
                _ => encode_composite_key_into(&bufs.pk_values, &mut bufs.key_buf),
            },
            false => encode_composite_key_into(&bufs.pk_values, &mut bufs.key_buf),
        }
        if plain_insert && single_int_pk {
            if let Value::Integer(id) = &bufs.pk_values[0] {
                min_inserted_pk = Some(min_inserted_pk.map_or(*id, |m| m.min(*id)));
            }
        }

        for &slot in dropped {
            bufs.value_values[slot as usize] = Value::Null;
        }
        for (j, &i) in non_pk.iter().enumerate() {
            let col = &table_schema.columns[i];
            if matches!(
                col.generated_kind,
                Some(crate::parser::GeneratedKind::Virtual)
            ) {
                bufs.value_values[enc_pos[j] as usize] = Value::Null;
            } else {
                bufs.value_values[enc_pos[j] as usize] =
                    std::mem::replace(&mut bufs.row[i], Value::Null);
            }
        }
        match cache.and_then(|c| c.row_encoder.as_ref()) {
            Some(tmpl) => crate::encoding::encode_row_with_template(
                tmpl,
                &bufs.value_values,
                &mut bufs.value_buf,
            )?,
            None => encode_row_into(&bufs.value_values, &mut bufs.value_buf),
        }

        if bufs.key_buf.len() > citadel_core::MAX_KEY_SIZE {
            return Err(SqlError::KeyTooLarge {
                size: bufs.key_buf.len(),
                max: citadel_core::MAX_KEY_SIZE,
            });
        }
        if bufs.value_buf.len() > citadel_core::MAX_VALUE_SIZE {
            return Err(SqlError::RowTooLarge {
                size: bufs.value_buf.len(),
                max: citadel_core::MAX_VALUE_SIZE,
            });
        }

        match compiled_conflict.as_ref() {
            None => {
                let is_new = wtx
                    .table_insert_if_absent(table_bytes, &bufs.key_buf, &bufs.value_buf)
                    .map_err(SqlError::Storage)?;
                if !is_new {
                    return Err(SqlError::DuplicateKey);
                }
                if has_indices || capture_insert_row {
                    restore_insert_row(
                        table_schema,
                        &bufs.pk_values,
                        &mut bufs.value_values,
                        &mut bufs.row,
                    );
                    if has_indices {
                        insert_index_entries(wtx, table_schema, &bufs.row, &bufs.pk_values)?;
                    }
                }
                if capture_insert_row {
                    materialize_insert_result_virtuals(
                        table_schema,
                        late_virtual_positions,
                        &mut bufs.row,
                        cancel.as_ref(),
                    )?;
                }
                if has_after_insert_triggers {
                    super::triggers::fire_row_triggers(
                        wtx,
                        schema,
                        &table_schema.name,
                        crate::parser::TriggerTiming::After,
                        super::triggers::FireEvent::Insert,
                        None,
                        Some(bufs.row.clone()),
                        &table_schema.columns,
                    )?;
                }
                if has_insert_statement_triggers_impl {
                    stmt_new_rows_impl.push(bufs.row.clone());
                }
                count += 1;
                if let Some(buf) = returning_rows.as_mut() {
                    buf.push((None, Some(bufs.row.clone())));
                }
            }
            Some(oc) => {
                let oc_ref: &CompiledOnConflict = oc;
                let needs_row = upsert_needs_row(oc_ref, table_schema);
                if needs_row {
                    restore_insert_row(
                        table_schema,
                        &bufs.pk_values,
                        &mut bufs.value_values,
                        &mut bufs.row,
                    );
                }
                let outcome = apply_insert_with_conflict(
                    wtx,
                    schema,
                    table_schema,
                    &bufs.key_buf,
                    &bufs.value_buf,
                    &bufs.row,
                    &bufs.pk_values,
                    oc_ref,
                    row_col_map.unwrap(),
                    cancel.as_ref(),
                    // Trigger dispatch needs the Updated outcome's rows too.
                    stmt.returning.is_some() || has_after_update_triggers,
                )?;
                match outcome {
                    InsertRowOutcome::Inserted => {
                        if capture_insert_row {
                            if !needs_row {
                                restore_insert_row(
                                    table_schema,
                                    &bufs.pk_values,
                                    &mut bufs.value_values,
                                    &mut bufs.row,
                                );
                            }
                            materialize_insert_result_virtuals(
                                table_schema,
                                late_virtual_positions,
                                &mut bufs.row,
                                cancel.as_ref(),
                            )?;
                        }
                        count += 1;
                        if let Some(buf) = returning_rows.as_mut() {
                            buf.push((None, Some(bufs.row.clone())));
                        }
                        if has_insert_statement_triggers_impl {
                            stmt_new_rows_impl.push(bufs.row.clone());
                        }
                        if has_after_insert_triggers {
                            super::triggers::fire_row_triggers(
                                wtx,
                                schema,
                                &table_schema.name,
                                crate::parser::TriggerTiming::After,
                                super::triggers::FireEvent::Insert,
                                None,
                                Some(bufs.row.clone()),
                                &table_schema.columns,
                            )?;
                        }
                    }
                    InsertRowOutcome::Updated { rows } => {
                        count += 1;
                        if let Some((old, new)) = rows {
                            if let Some(buf) = returning_rows.as_mut() {
                                buf.push((Some(old.clone()), Some(new.clone())));
                            }
                            if has_after_update_triggers {
                                let changed_cols: Vec<String> = match oc_ref {
                                    CompiledOnConflict::DoUpdate { assignments, .. } => assignments
                                        .iter()
                                        .map(|(col_idx, _)| {
                                            table_schema.columns[*col_idx].name.clone()
                                        })
                                        .collect(),
                                    _ => Vec::new(),
                                };
                                super::triggers::fire_row_triggers(
                                    wtx,
                                    schema,
                                    &table_schema.name,
                                    crate::parser::TriggerTiming::After,
                                    super::triggers::FireEvent::Update {
                                        changed_columns: &changed_cols,
                                    },
                                    Some(old),
                                    Some(new),
                                    &table_schema.columns,
                                )?;
                            }
                        }
                    }
                    InsertRowOutcome::Skipped => {}
                }
            }
        }
    }

    mark_insert_dml(
        schema,
        &table_schema.name,
        !plain_insert,
        single_int_pk,
        min_inserted_pk,
        count,
    );

    if let (Some(returning_cols), Some(rows)) = (stmt.returning.as_ref(), returning_rows) {
        if has_insert_statement_triggers_impl {
            super::triggers::fire_statement_triggers(
                wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::After,
                super::triggers::FireEvent::Insert,
                &table_schema.columns,
                &[],
                &stmt_new_rows_impl,
            )?;
        }
        return Ok(ExecutionResult::Query(super::helpers::project_returning(
            table_schema,
            returning_cols,
            &rows,
            wtx.cancel_token(),
        )?));
    }

    if has_insert_statement_triggers_impl {
        super::triggers::fire_statement_triggers(
            wtx,
            schema,
            &table_schema.name,
            crate::parser::TriggerTiming::After,
            super::triggers::FireEvent::Insert,
            &table_schema.columns,
            &[],
            &stmt_new_rows_impl,
        )?;
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
    generated_col_positions: Vec<usize>,
    generated_fast_evals: Vec<FastGenEval>,
    late_virtual_positions: Vec<usize>,
    pk_indices: Vec<usize>,
    non_pk_indices: Vec<usize>,
    encoding_positions: Vec<u16>,
    dropped_non_pk_slots: Vec<u16>,
    phys_count: usize,
    single_int_pk: bool,
    not_null_indices: Vec<u16>,
    bind_plan: Option<Vec<BindAction>>,
    row_fully_overwritten: bool,
    row_encoder: Option<crate::encoding::RowTemplate>,
    is_trivial_fast: bool,
    trivial_fast_program: Option<TrivialFastProgram>,
    needs_scoped_params: bool,
}

#[derive(Clone)]
enum BindAction {
    Param {
        param_idx: usize,
        col_idx: usize,
        target: DataType,
    },
    Literal {
        value: Value,
        col_idx: usize,
    },
}

#[derive(Clone)]
struct TrivialFastProgram {
    template: Vec<u8>,
    ops: Vec<WriteOp>,
    pk_param: u8,
    fk_checks: Vec<FkCheckSpec>,
    index_inserts: Vec<IndexInsertSpec>,
    on_dup: DupPolicy,
}

/// PK-dup policy: Error = plain INSERT, Skip = DO NOTHING, Patch = DO UPDATE.
#[derive(Clone)]
enum DupPolicy {
    Error,
    Skip,
    Patch(Vec<DoUpdateFastPath>),
}

/// A foreign-key existence check encodable straight from bound params.
#[derive(Clone)]
struct FkCheckSpec {
    foreign_table: Vec<u8>,
    col_params: Vec<u8>,
}

/// A pure-column non-unique secondary index insert encodable from bound params.
#[derive(Clone)]
struct IndexInsertSpec {
    table: Vec<u8>,
    key_params: Vec<(u8, crate::types::Collation)>,
}

#[derive(Clone)]
enum WriteOp {
    ParamI64 {
        param_idx: u8,
        off: u32,
    },
    LiteralI64 {
        value: i64,
        off: u32,
    },
    GenAddParamsI64 {
        a_param: u8,
        b_param: u8,
        off: u32,
    },
    GenMulAddParamI64 {
        param_idx: u8,
        mul: i64,
        add: i64,
        off: u32,
    },
}

/// Restore stored values without replacing logical virtuals with physical NULLs.
fn restore_insert_row(ts: &TableSchema, pk: &[Value], values: &mut [Value], row: &mut [Value]) {
    for (&i, value) in ts.pk_indices().iter().zip(pk) {
        row[i] = value.clone();
    }
    for (&i, &slot) in ts.non_pk_indices().iter().zip(ts.encoding_positions()) {
        if !matches!(
            ts.columns[i].generated_kind,
            Some(crate::parser::GeneratedKind::Virtual)
        ) {
            row[i] = std::mem::replace(&mut values[slot as usize], Value::Null);
        }
    }
}

#[derive(Default)]
struct InsertVirtuals {
    before_insert: Vec<usize>,
    after_insert: Vec<usize>,
}

/// Keep INSERT constraints separate from consumers that require an inserted row.
fn required_insert_virtuals(
    schema: &SchemaManager,
    ts: &TableSchema,
    stmt: &InsertStmt,
) -> InsertVirtuals {
    if !ts.has_virtual_columns() {
        return InsertVirtuals::default();
    }
    let mut required = vec![false; ts.columns.len()];
    let mut after_insert = vec![false; ts.columns.len()];
    for trigger in schema.triggers_for(&ts.name) {
        if !trigger.enabled
            || !trigger
                .events
                .iter()
                .any(|event| matches!(event, TriggerEvent::Insert))
        {
            continue;
        }
        match trigger.timing {
            TriggerTiming::Before if trigger.granularity == TriggerGranularity::ForEachRow => {
                required.fill(true);
            }
            TriggerTiming::After => after_insert.fill(true),
            _ => {}
        }
    }
    for col in &ts.columns {
        if !col.nullable {
            required[col.position as usize] = true;
        }
        if let Some(expr) = &col.check_expr {
            require_virtual_refs(ts, expr, &mut required, |_| true);
        }
    }
    for check in &ts.check_constraints {
        require_virtual_refs(ts, &check.expr, &mut required, |_| true);
    }
    for fk in &ts.foreign_keys {
        for &col in &fk.columns {
            required[col as usize] = true;
        }
    }
    for index in &ts.indices {
        for key in &index.keys {
            match key {
                IndexKey::Column { idx, .. } => required[*idx as usize] = true,
                IndexKey::Expr { expr, .. } => {
                    require_virtual_refs(ts, expr, &mut required, |_| true)
                }
            }
        }
        if let Some(expr) = &index.predicate_expr {
            require_virtual_refs(ts, expr, &mut required, |_| true);
        }
    }
    if let Some(returning) = &stmt.returning {
        for col in returning {
            match col {
                SelectColumn::AllColumns | SelectColumn::AllFromNew => after_insert.fill(true),
                SelectColumn::AllFromOld => {}
                SelectColumn::Expr { expr, .. } => {
                    require_virtual_refs(ts, expr, &mut after_insert, |qualifier| {
                        !qualifier.is_some_and(|q| q.eq_ignore_ascii_case("old"))
                    })
                }
            }
        }
    }
    let mut positions = InsertVirtuals::default();
    for (i, col) in ts.columns.iter().enumerate() {
        if matches!(col.generated_kind, Some(GeneratedKind::Virtual)) {
            if required[i] {
                positions.before_insert.push(i);
            } else if after_insert[i] {
                positions.after_insert.push(i);
            }
        }
    }
    positions
}

fn materialize_insert_result_virtuals(
    ts: &TableSchema,
    positions: &[usize],
    row: &mut [Value],
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    if positions.is_empty() {
        return Ok(());
    }
    let col_map = ts.column_map();
    for &pos in positions {
        let col = &ts.columns[pos];
        let value = eval_expr(
            col.generated_expr.as_ref().unwrap(),
            &EvalCtx::new(col_map, row).with_cancel(cancel),
        )?;
        row[pos] = coerce_for_column(value, col, ts.is_strict())?;
    }
    Ok(())
}

fn require_virtual_refs(
    ts: &TableSchema,
    expr: &Expr,
    required: &mut [bool],
    include: impl Fn(Option<&str>) -> bool,
) {
    crate::parser::visit_expr(expr, &mut |expr| {
        let (qualifier, name) = match expr {
            Expr::Column(name) => (None, name),
            Expr::QualifiedColumn { table, column } => (Some(table.as_str()), column),
            _ => return,
        };
        if include(qualifier) {
            if let Some(i) = ts.column_index(name) {
                required[i] = true;
            }
        }
    });
}

fn build_trivial_fast_program(
    bind_plan: &[BindAction],
    phys_count: usize,
    non_virtual_pairs: &[(usize, usize)],
    generated_col_positions: &[usize],
    generated_fast_evals: &[FastGenEval],
    ts: &TableSchema,
    on_conflict: Option<&CompiledOnConflict>,
) -> Option<TrivialFastProgram> {
    let columns = &ts.columns;
    let pk_col = ts.pk_indices()[0];

    // PK-arbiter shapes on index/FK-free tables only; anything else bails.
    let on_dup = match on_conflict {
        None => DupPolicy::Error,
        Some(CompiledOnConflict::DoNothing { target })
            if matches!(target, None | Some(ConflictKind::PrimaryKey))
                && ts.indices.is_empty()
                && ts.foreign_keys.is_empty() =>
        {
            DupPolicy::Skip
        }
        Some(CompiledOnConflict::DoUpdate {
            target: ConflictKind::PrimaryKey,
            where_clause: None,
            fast_paths: Some(fps),
            ..
        }) => DupPolicy::Patch(fps.clone()),
        _ => return None,
    };

    let mut col_to_bind: rustc_hash::FxHashMap<usize, &BindAction> = Default::default();
    for action in bind_plan {
        let col = match action {
            BindAction::Param { col_idx, .. } | BindAction::Literal { col_idx, .. } => *col_idx,
        };
        col_to_bind.insert(col, action);
    }

    // Freeze literals into the template; int params/generated cols stay holes.
    let mut slots: Vec<crate::encoding::TemplateSlot> = (0..phys_count)
        .map(|_| crate::encoding::TemplateSlot::Null)
        .collect();
    for &(col, slot) in non_virtual_pairs {
        slots[slot] = match col_to_bind.get(&col) {
            Some(BindAction::Literal { value, .. }) => {
                crate::encoding::TemplateSlot::Const(value.clone())
            }
            Some(BindAction::Param { target, .. }) => {
                if *target != DataType::Integer {
                    return None;
                }
                crate::encoding::TemplateSlot::IntHole
            }
            None => {
                if columns[col].data_type != DataType::Integer {
                    return None;
                }
                crate::encoding::TemplateSlot::IntHole
            }
        };
    }
    let tmpl = crate::encoding::build_row_template(phys_count, &slots);
    let col_to_slot: rustc_hash::FxHashMap<usize, usize> =
        non_virtual_pairs.iter().copied().collect();
    let slot_to_off: rustc_hash::FxHashMap<usize, usize> =
        tmpl.slot_offsets.iter().copied().collect();

    let mut col_to_param: rustc_hash::FxHashMap<usize, u8> = Default::default();
    let mut col_to_lit_int: rustc_hash::FxHashMap<usize, i64> = Default::default();
    let mut pk_param: Option<u8> = None;
    let mut ops: Vec<WriteOp> = Vec::with_capacity(bind_plan.len() + generated_col_positions.len());
    let mut not_null_param_indices: Vec<u8> = Vec::new();

    for action in bind_plan {
        match action {
            BindAction::Param {
                param_idx,
                col_idx,
                target,
            } => {
                if *target != DataType::Integer {
                    return None;
                }
                let pi: u8 = u8::try_from(*param_idx).ok()?;
                col_to_param.insert(*col_idx, pi);
                if *col_idx == pk_col {
                    pk_param = Some(pi);
                } else {
                    let slot = *col_to_slot.get(col_idx)?;
                    let off = u32::try_from(*slot_to_off.get(&slot)?).ok()?;
                    ops.push(WriteOp::ParamI64 { param_idx: pi, off });
                    if !columns[*col_idx].nullable {
                        not_null_param_indices.push(pi);
                    }
                }
            }
            // Already in the template; record ints only for generated-col refs.
            BindAction::Literal { value, col_idx } => {
                if *col_idx == pk_col {
                    return None;
                }
                if let Value::Integer(v) = value {
                    col_to_lit_int.insert(*col_idx, *v);
                }
            }
        }
    }

    let pk_param = pk_param?;

    for (i, &gen_pos) in generated_col_positions.iter().enumerate() {
        let gen_slot = *col_to_slot.get(&gen_pos)?;
        let gen_off = u32::try_from(*slot_to_off.get(&gen_slot)?).ok()?;
        let gen_col_nullable = columns[gen_pos].nullable;

        match &generated_fast_evals[i] {
            FastGenEval::IntColAddCol {
                left_idx,
                right_idx,
            } => {
                let a_param = col_to_param.get(left_idx).copied();
                let b_param = col_to_param.get(right_idx).copied();
                match (a_param, b_param) {
                    (Some(ap), Some(bp)) => {
                        let deps_safe = gen_col_nullable
                            || (not_null_param_indices.contains(&ap)
                                && not_null_param_indices.contains(&bp));
                        if !deps_safe {
                            return None;
                        }
                        ops.push(WriteOp::GenAddParamsI64 {
                            a_param: ap,
                            b_param: bp,
                            off: gen_off,
                        });
                    }
                    (Some(p), None) => {
                        let lit = col_to_lit_int.get(right_idx).copied()?;
                        if !gen_col_nullable && !not_null_param_indices.contains(&p) {
                            return None;
                        }
                        ops.push(WriteOp::GenMulAddParamI64 {
                            param_idx: p,
                            mul: 1,
                            add: lit,
                            off: gen_off,
                        });
                    }
                    (None, Some(p)) => {
                        let lit = col_to_lit_int.get(left_idx).copied()?;
                        if !gen_col_nullable && !not_null_param_indices.contains(&p) {
                            return None;
                        }
                        ops.push(WriteOp::GenMulAddParamI64 {
                            param_idx: p,
                            mul: 1,
                            add: lit,
                            off: gen_off,
                        });
                    }
                    (None, None) => {
                        let la = col_to_lit_int.get(left_idx).copied()?;
                        let lb = col_to_lit_int.get(right_idx).copied()?;
                        ops.push(WriteOp::LiteralI64 {
                            value: la.checked_add(lb)?,
                            off: gen_off,
                        });
                    }
                }
            }
            FastGenEval::IntColMulAdd {
                col_schema_idx,
                mul,
                add,
            } => {
                if let Some(p) = col_to_param.get(col_schema_idx).copied() {
                    if !gen_col_nullable && !not_null_param_indices.contains(&p) {
                        return None;
                    }
                    ops.push(WriteOp::GenMulAddParamI64 {
                        param_idx: p,
                        mul: *mul,
                        add: *add,
                        off: gen_off,
                    });
                } else {
                    let lit = col_to_lit_int.get(col_schema_idx).copied()?;
                    ops.push(WriteOp::LiteralI64 {
                        value: checked_gen_mul_add(lit, *mul, *add).ok()?,
                        off: gen_off,
                    });
                }
            }
            FastGenEval::None => return None,
        }
    }

    let mut fk_checks: Vec<FkCheckSpec> = Vec::with_capacity(ts.foreign_keys.len());
    for fk in &ts.foreign_keys {
        if fk.deferrable && fk.initially_deferred {
            return None;
        }
        let mut col_params = Vec::with_capacity(fk.columns.len());
        for &c in &fk.columns {
            col_params.push(col_to_param.get(&(c as usize)).copied()?);
        }
        fk_checks.push(FkCheckSpec {
            foreign_table: fk.foreign_table.as_bytes().to_vec(),
            col_params,
        });
    }

    let mut index_inserts: Vec<IndexInsertSpec> = Vec::with_capacity(ts.indices.len());
    for idx in &ts.indices {
        if idx.unique
            || !idx.is_pure_column_index()
            || idx.predicate_expr.is_some()
            || idx.predicate_sql.is_some()
        {
            return None;
        }
        let mut key_params = Vec::with_capacity(idx.keys.len());
        for (i, key) in idx.keys.iter().enumerate() {
            let crate::types::IndexKey::Column { idx: col_idx, .. } = key else {
                return None;
            };
            key_params.push((
                col_to_param.get(&(*col_idx as usize)).copied()?,
                idx.collation_at(i),
            ));
        }
        index_inserts.push(IndexInsertSpec {
            table: TableSchema::index_table_name(&ts.name, &idx.name),
            key_params,
        });
    }

    Some(TrivialFastProgram {
        template: tmpl.template,
        ops,
        pk_param,
        fk_checks,
        index_inserts,
        on_dup,
    })
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
pub(super) struct DoUpdateFastPath {
    col_idx: usize,
    phys_idx: usize,
    arithmetic: IntPatchArithmetic,
}

#[derive(Clone, Copy)]
enum IntPatchArithmetic {
    Add(i64),
    Sub(i64),
}

impl IntPatchArithmetic {
    fn eval(self, value: i64) -> Result<Value> {
        let result = match self {
            Self::Add(rhs) => value.checked_add(rhs),
            Self::Sub(rhs) => value.checked_sub(rhs),
        };
        result.map(Value::Integer).ok_or(SqlError::IntegerOverflow)
    }

    fn parts(self) -> (BinOp, Value) {
        match self {
            Self::Add(rhs) => (BinOp::Add, Value::Integer(rhs)),
            Self::Sub(rhs) => (BinOp::Sub, Value::Integer(rhs)),
        }
    }
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
                if idx.unique && set_equal(&col_idx_set, &idx.columns_vec()) {
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
    Updated { rows: Option<UpsertRows> },
    Skipped,
}

#[allow(clippy::too_many_arguments)]
#[inline]
pub(super) fn apply_insert_with_conflict(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    table_schema: &TableSchema,
    key_buf: &[u8],
    value_buf: &[u8],
    row: &[Value],
    pk_values: &[Value],
    on_conflict: &CompiledOnConflict,
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
    capture_returning: bool,
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
                cancel,
                capture_returning,
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
                                schema,
                                table_schema,
                                &existing_pk,
                                row,
                                assignments,
                                where_clause.as_ref(),
                                col_map,
                                cancel,
                                capture_returning,
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
                    let old_row =
                        decode_full_row_with_cancel(table_schema, key_buf, &old_bytes, cancel)?;
                    apply_do_update_with_old_row(
                        wtx,
                        schema,
                        table_schema,
                        key_buf,
                        &old_row,
                        row,
                        assignments,
                        where_clause.as_ref(),
                        col_map,
                        cancel,
                        capture_returning,
                    )
                }
            }
        }
    }
}

#[inline]
fn apply_fast_path_patch(
    schema: &TableSchema,
    key: &[u8],
    old_bytes: Vec<u8>,
    fast_paths: &[DoUpdateFastPath],
    cancel: Option<&citadel::CancelToken>,
    captured: Option<&RefCell<Option<UpsertRows>>>,
) -> Result<UpsertAction> {
    use crate::encoding::{
        decode_column_with_offset, patch_at_offset, patch_row_column, RawColumn,
    };

    let normalized = UPSERT_SCRATCH.with(|slot| {
        slot.borrow_mut()
            .materializer
            .expand(schema, key, &old_bytes, cancel)
    })?;
    let old_row = captured
        .map(|_| {
            decode_full_row_with_cancel(
                schema,
                key,
                normalized.as_deref().unwrap_or(&old_bytes),
                cancel,
            )
        })
        .transpose()?;
    let mut bytes = normalized.unwrap_or(old_bytes);
    let mut scratch = Vec::new();
    let mut null_violation = None;
    for fp in fast_paths {
        // Admission permits distinct targets that each read only their own old value.
        let (old, offset) = decode_column_with_offset(&bytes, fp.phys_idx)?;
        let value = match old {
            RawColumn::Integer(i) => fp.arithmetic.eval(i)?,
            RawColumn::Null => Value::Null,
            _ => {
                let decoded = crate::encoding::decode_columns(&bytes, &[fp.phys_idx])?;
                let (op, rhs) = fp.arithmetic.parts();
                let value = crate::eval::eval_binary_op_with_cancel(&decoded[0], op, &rhs, cancel)?;
                coerce_for_column(value, &schema.columns[fp.col_idx], schema.is_strict())?
            }
        };
        let col = &schema.columns[fp.col_idx];
        if value.is_null() && !col.nullable && null_violation.is_none() {
            null_violation = Some(&col.name);
        }
        if !patch_at_offset(&mut bytes, offset, &value)? {
            scratch.clear();
            patch_row_column(&bytes, fp.phys_idx, &value, &mut scratch)?;
            std::mem::swap(&mut bytes, &mut scratch);
        }
    }
    if let Some(name) = null_violation {
        return Err(SqlError::NotNullViolation(name.clone()));
    }
    if bytes.len() > citadel_core::MAX_VALUE_SIZE {
        return Err(SqlError::RowTooLarge {
            size: bytes.len(),
            max: citadel_core::MAX_VALUE_SIZE,
        });
    }
    if let (Some(captured), Some(old)) = (captured, old_row) {
        let new = decode_full_row_with_cancel(schema, key, &bytes, cancel)?;
        *captured.borrow_mut() = Some((old, new));
    }
    Ok(UpsertAction::Replace(bytes))
}

type UpsertRows = (Vec<Value>, Vec<Value>);

fn upsert_needs_row(oc: &CompiledOnConflict, ts: &TableSchema) -> bool {
    if !ts.indices.is_empty() {
        return true;
    }
    match oc {
        CompiledOnConflict::DoNothing { .. } => false,
        CompiledOnConflict::DoUpdate { fast_paths, .. } => fast_paths.is_none(),
    }
}

fn can_fuse_do_update(ts: &TableSchema, assignments: &[(usize, Expr)]) -> bool {
    if !ts.indices.is_empty() {
        return false;
    }
    if !ts.foreign_keys.is_empty() {
        return false;
    }
    if ts.columns.iter().any(|c| c.generated_kind.is_some()) {
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
    cancel: Option<&citadel::CancelToken>,
    capture_returning: bool,
) -> Result<InsertRowOutcome> {
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let phys_count = table_schema.physical_non_pk_count();
    let dropped = table_schema.dropped_non_pk_slots();
    let has_checks = table_schema.has_checks();
    let captured = RefCell::new(None);

    let outcome =
        wtx.table_upsert_with_owned::<_, SqlError>(table_bytes, key_buf, value_buf, |old_bytes| {
            if let Some(fps) = fast_paths {
                return apply_fast_path_patch(
                    table_schema,
                    key_buf,
                    old_bytes,
                    fps,
                    cancel,
                    capture_returning.then_some(&captured),
                );
            }
            UPSERT_SCRATCH.with(|slot| {
                let mut bufs = slot.borrow_mut();
                let UpsertBufs {
                    old_row,
                    new_row,
                    value_values,
                    new_value_buf,
                    ..
                } = &mut *bufs;

                old_row.clear();
                old_row.resize(table_schema.columns.len(), Value::Null);
                decode_full_row_into_with_cancel(
                    table_schema,
                    key_buf,
                    &old_bytes,
                    old_row,
                    cancel,
                )?;

                if let Some(w) = where_clause {
                    let ctx = EvalCtx::with_excluded(col_map, old_row, col_map, proposed_row)
                        .with_cancel(cancel);
                    let result = eval_expr(w, &ctx)?;
                    if result.is_null() || !is_truthy(&result) {
                        return Ok(UpsertAction::Skip);
                    }
                }

                new_row.clear();
                new_row.extend_from_slice(old_row);
                for (col_idx, expr) in assignments {
                    let ctx = EvalCtx::with_excluded(col_map, old_row, col_map, proposed_row)
                        .with_cancel(cancel);
                    let val = eval_expr(expr, &ctx)?;
                    let col = &table_schema.columns[*col_idx];
                    new_row[*col_idx] = coerce_for_column(val, col, table_schema.is_strict())?;
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
                            let ctx = EvalCtx::new(col_map, new_row).with_cancel(cancel);
                            let result = eval_expr(check, &ctx)?;
                            if !is_truthy(&result) && !result.is_null() {
                                let name = col.check_name.as_deref().unwrap_or(&col.name);
                                return Err(SqlError::CheckViolation(name.to_string()));
                            }
                        }
                    }
                    for tc in &table_schema.check_constraints {
                        let ctx = EvalCtx::new(col_map, new_row).with_cancel(cancel);
                        let result = eval_expr(&tc.expr, &ctx)?;
                        if !is_truthy(&result) && !result.is_null() {
                            let name = tc.name.as_deref().unwrap_or(&tc.sql);
                            return Err(SqlError::CheckViolation(name.to_string()));
                        }
                    }
                }

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

                if new_value_buf.len() > citadel_core::MAX_VALUE_SIZE {
                    return Err(SqlError::RowTooLarge {
                        size: new_value_buf.len(),
                        max: citadel_core::MAX_VALUE_SIZE,
                    });
                }

                if capture_returning {
                    *captured.borrow_mut() = Some((old_row.clone(), new_row.clone()));
                }
                Ok(UpsertAction::Replace(new_value_buf.clone()))
            })
        })?;

    match outcome {
        UpsertOutcome::Inserted => Ok(InsertRowOutcome::Inserted),
        UpsertOutcome::Updated => {
            let rows = if capture_returning {
                Some(captured.into_inner().ok_or_else(|| {
                    SqlError::InvalidValue("DO UPDATE produced no captured rows".into())
                })?)
            } else {
                None
            };
            Ok(InsertRowOutcome::Updated { rows })
        }
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
        .column_positions_iter()
        .map(|col_idx| row[col_idx as usize].clone())
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

#[allow(clippy::too_many_arguments)]
fn apply_do_update(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    table_schema: &TableSchema,
    pk_key: &[u8],
    proposed_row: &[Value],
    assignments: &[(usize, Expr)],
    where_clause: Option<&Expr>,
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
    capture_returning: bool,
) -> Result<InsertRowOutcome> {
    let old_value = wtx
        .table_get(table_schema.name.as_bytes(), pk_key)
        .map_err(SqlError::Storage)?
        .ok_or_else(|| SqlError::InvalidValue("primary row missing for DO UPDATE target".into()))?;
    let old_row = decode_full_row_with_cancel(table_schema, pk_key, &old_value, cancel)?;
    apply_do_update_with_old_row(
        wtx,
        schema,
        table_schema,
        pk_key,
        &old_row,
        proposed_row,
        assignments,
        where_clause,
        col_map,
        cancel,
        capture_returning,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_do_update_with_old_row(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    table_schema: &TableSchema,
    old_pk_key: &[u8],
    old_row: &[Value],
    proposed_row: &[Value],
    assignments: &[(usize, Expr)],
    where_clause: Option<&Expr>,
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
    capture_returning: bool,
) -> Result<InsertRowOutcome> {
    let resolve_excluded = |idx: usize| {
        let col = &table_schema.columns[idx];
        if matches!(col.generated_kind, Some(GeneratedKind::Virtual)) {
            let value = eval_expr(
                col.generated_expr.as_ref().unwrap(),
                &EvalCtx::new(col_map, proposed_row).with_cancel(cancel),
            )?;
            coerce_for_column(value, col, table_schema.is_strict())
        } else {
            Ok(proposed_row[idx].clone())
        }
    };
    let mut ctx =
        EvalCtx::with_excluded(col_map, old_row, col_map, proposed_row).with_cancel(cancel);
    if table_schema.has_virtual_columns() {
        ctx = ctx.with_excluded_resolver(&resolve_excluded);
    }
    if let Some(w) = where_clause {
        let result = eval_expr(w, &ctx)?;
        if result.is_null() || !is_truthy(&result) {
            return Ok(InsertRowOutcome::Skipped);
        }
    }

    let mut new_row = old_row.to_vec();
    for (col_idx, expr) in assignments {
        let val = eval_expr(expr, &ctx)?;
        let col = &table_schema.columns[*col_idx];
        new_row[*col_idx] = coerce_for_column(val, col, table_schema.is_strict())?;
    }

    for col in &table_schema.columns {
        if col.generated_kind.is_some() {
            let val = eval_expr(
                col.generated_expr.as_ref().unwrap(),
                &EvalCtx::new(col_map, &new_row).with_cancel(cancel),
            )?;
            let pos = col.position as usize;
            new_row[pos] = if val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
                Value::Null
            } else {
                coerce_for_column(val, col, table_schema.is_strict())?
            };
        }
    }

    let pk_indices = table_schema.pk_indices();
    let assigned_pk = assignments.iter().any(|(ci, _)| pk_indices.contains(ci));
    let pk_changed = assigned_pk && pk_indices.iter().any(|&i| !old_row[i].bit_eq(&new_row[i]));

    for (assigned_idx, _) in assignments {
        let col = &table_schema.columns[*assigned_idx];
        if !col.nullable && new_row[col.position as usize].is_null() {
            return Err(SqlError::NotNullViolation(col.name.clone()));
        }
    }
    if table_schema.has_checks() {
        for col in &table_schema.columns {
            if let Some(ref check) = col.check_expr {
                let ctx = EvalCtx::new(col_map, &new_row).with_cancel(cancel);
                let result = eval_expr(check, &ctx)?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = col.check_name.as_deref().unwrap_or(&col.name);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }
        for tc in &table_schema.check_constraints {
            let ctx = EvalCtx::new(col_map, &new_row).with_cancel(cancel);
            let result = eval_expr(&tc.expr, &ctx)?;
            if !is_truthy(&result) && !result.is_null() {
                let name = tc.name.as_deref().unwrap_or(&tc.sql);
                return Err(SqlError::CheckViolation(name.to_string()));
            }
        }
    }
    let mut fk_key = Vec::new();
    for fk in &table_schema.foreign_keys {
        if pk_changed
            || fk
                .columns
                .iter()
                .any(|&ci| !old_row[ci as usize].bit_eq(&new_row[ci as usize]))
        {
            super::fk::check_row_reference(wtx, schema, table_schema, fk, &new_row, &mut fk_key)?;
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
        let col = &table_schema.columns[i];
        value_values[enc_pos[j] as usize] = if matches!(
            col.generated_kind,
            Some(crate::parser::GeneratedKind::Virtual)
        ) {
            Value::Null
        } else {
            new_row[i].clone()
        };
    }
    let mut new_value_buf = Vec::with_capacity(256);
    crate::encoding::encode_row_into(&value_values, &mut new_value_buf);

    if new_value_buf.len() > citadel_core::MAX_VALUE_SIZE {
        return Err(SqlError::RowTooLarge {
            size: new_value_buf.len(),
            max: citadel_core::MAX_VALUE_SIZE,
        });
    }

    let col_map_partial = any_partial_index(table_schema).then(|| table_schema.column_map());
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
            let cols_changed = index_columns_changed(idx, old_row, &new_row, table_schema);
            let (del, ins) = partial_idx_update_actions_with_cancel(
                idx,
                old_row,
                &new_row,
                cols_changed,
                true,
                col_map_partial,
                cancel,
            )?;
            let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
            if del {
                let old_idx_key = encode_index_key_with_schema_and_cancel(
                    idx,
                    old_row,
                    &old_pk_values,
                    table_schema,
                    cancel,
                )?;
                wtx.table_delete(&idx_table, &old_idx_key)
                    .map_err(SqlError::Storage)?;
            }
            if ins {
                let new_idx_key = encode_index_key_with_schema_and_cancel(
                    idx,
                    &new_row,
                    &new_pk_values,
                    table_schema,
                    cancel,
                )?;
                let new_idx_val = encode_index_value(idx, &new_row, &new_pk_values);
                let is_new = wtx
                    .table_insert(&idx_table, &new_idx_key, &new_idx_val)
                    .map_err(SqlError::Storage)?;
                if idx.unique && !is_new {
                    let any_null = idx
                        .column_positions_iter()
                        .any(|c| new_row[c as usize].is_null());
                    if !any_null {
                        return Err(SqlError::UniqueViolation(idx.name.clone()));
                    }
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
            let cols_changed = index_columns_changed(idx, old_row, &new_row, table_schema);
            let (del, ins) = partial_idx_update_actions_with_cancel(
                idx,
                old_row,
                &new_row,
                cols_changed,
                false,
                col_map_partial,
                cancel,
            )?;
            let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
            if del {
                let old_idx_key = encode_index_key_with_schema_and_cancel(
                    idx,
                    old_row,
                    &old_pk_values,
                    table_schema,
                    cancel,
                )?;
                wtx.table_delete(&idx_table, &old_idx_key)
                    .map_err(SqlError::Storage)?;
            }
            if ins {
                let new_idx_key = encode_index_key_with_schema_and_cancel(
                    idx,
                    &new_row,
                    &new_pk_values,
                    table_schema,
                    cancel,
                )?;
                let new_idx_val = encode_index_value(idx, &new_row, &new_pk_values);
                let is_new = wtx
                    .table_insert(&idx_table, &new_idx_key, &new_idx_val)
                    .map_err(SqlError::Storage)?;
                if idx.unique && !is_new {
                    let any_null = idx
                        .column_positions_iter()
                        .any(|c| new_row[c as usize].is_null());
                    if !any_null {
                        return Err(SqlError::UniqueViolation(idx.name.clone()));
                    }
                }
            }
        }
    }

    Ok(InsertRowOutcome::Updated {
        rows: capture_returning.then(|| (old_row.to_vec(), new_row)),
    })
}

fn detect_fast_paths(
    ts: &TableSchema,
    assignments: &[(usize, Expr)],
) -> Option<Vec<DoUpdateFastPath>> {
    if !can_fuse_do_update(ts, assignments) || ts.has_checks() {
        return None;
    }
    let non_pk = ts.non_pk_indices();
    let enc_pos = ts.encoding_positions();
    let mut out: Vec<DoUpdateFastPath> = Vec::with_capacity(assignments.len());
    for (col_idx, expr) in assignments {
        if out.iter().any(|p| p.col_idx == *col_idx) {
            return None;
        }
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
                let arithmetic = if matches!(op, BinOp::Sub) {
                    IntPatchArithmetic::Sub(*n)
                } else {
                    IntPatchArithmetic::Add(*n)
                };
                out.push(DoUpdateFastPath {
                    col_idx: *col_idx,
                    phys_idx,
                    arithmetic,
                });
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
                    if ts.columns[col_idx].generated_kind.is_some() {
                        return Err(SqlError::CannotUpdateGeneratedColumn(name.clone()));
                    }
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

/// Integer-only template; other values use the validated cached lane.
fn exec_insert_trivial_fast(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    table_lower: &str,
    cache: &InsertCache,
    bufs: &mut InsertBufs,
    params: &[Value],
) -> Result<Option<ExecutionResult>> {
    let prog = cache
        .trivial_fast_program
        .as_ref()
        .expect("trivial fast: program");

    match &params[prog.pk_param as usize] {
        Value::Integer(v) => crate::encoding::encode_int_key_into(*v, &mut bufs.key_buf),
        _ => return Ok(None),
    }

    bufs.value_buf.clear();
    bufs.value_buf.extend_from_slice(&prog.template);

    for op in &prog.ops {
        match op {
            WriteOp::ParamI64 { param_idx, off } => match &params[*param_idx as usize] {
                Value::Integer(v) => {
                    let off = *off as usize;
                    bufs.value_buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
                }
                _ => return Ok(None),
            },
            WriteOp::LiteralI64 { value, off } => {
                let off = *off as usize;
                bufs.value_buf[off..off + 8].copy_from_slice(&value.to_le_bytes());
            }
            WriteOp::GenAddParamsI64 {
                a_param,
                b_param,
                off,
            } => match (&params[*a_param as usize], &params[*b_param as usize]) {
                (Value::Integer(a), Value::Integer(b)) => {
                    let value = a.checked_add(*b).ok_or(SqlError::IntegerOverflow)?;
                    let off = *off as usize;
                    bufs.value_buf[off..off + 8].copy_from_slice(&value.to_le_bytes());
                }
                _ => return Ok(None),
            },
            WriteOp::GenMulAddParamI64 {
                param_idx,
                mul,
                add,
                off,
            } => match &params[*param_idx as usize] {
                Value::Integer(v) => {
                    let value = checked_gen_mul_add(*v, *mul, *add)?;
                    let off = *off as usize;
                    bufs.value_buf[off..off + 8].copy_from_slice(&value.to_le_bytes());
                }
                _ => return Ok(None),
            },
        }
    }

    for fk in &prog.fk_checks {
        if fk.col_params.iter().any(|&p| params[p as usize].is_null()) {
            continue;
        }
        bufs.fk_key_buf.clear();
        for &p in &fk.col_params {
            crate::encoding::encode_key_value_into(&params[p as usize], &mut bufs.fk_key_buf);
        }
        if !wtx.fk_check_cached(&fk.foreign_table, &bufs.fk_key_buf) {
            let found = wtx
                .table_get(&fk.foreign_table, &bufs.fk_key_buf)
                .map_err(SqlError::Storage)?;
            if found.is_none() {
                return Err(SqlError::ForeignKeyViolation(
                    String::from_utf8_lossy(&fk.foreign_table).into_owned(),
                ));
            }
            wtx.mark_fk_verified(&fk.foreign_table, &bufs.fk_key_buf);
        }
    }

    if let DupPolicy::Patch(fps) = &prog.on_dup {
        let cancel = wtx.cancel_token().cloned();
        let outcome = wtx.table_upsert_with_owned::<_, SqlError>(
            table_lower.as_bytes(),
            &bufs.key_buf,
            &bufs.value_buf,
            |old_bytes| {
                let table_schema = schema
                    .get(table_lower)
                    .ok_or_else(|| SqlError::TableNotFound(table_lower.into()))?;
                apply_fast_path_patch(
                    table_schema,
                    &bufs.key_buf,
                    old_bytes,
                    fps,
                    cancel.as_ref(),
                    None,
                )
            },
        )?;
        return Ok(Some(match outcome {
            UpsertOutcome::Inserted | UpsertOutcome::Updated => ExecutionResult::RowsAffected(1),
            UpsertOutcome::Skipped => ExecutionResult::RowsAffected(0),
        }));
    }

    let is_new = wtx
        .table_insert_if_absent(table_lower.as_bytes(), &bufs.key_buf, &bufs.value_buf)
        .map_err(SqlError::Storage)?;
    if !is_new {
        return match &prog.on_dup {
            DupPolicy::Error => Err(SqlError::DuplicateKey),
            DupPolicy::Skip => Ok(Some(ExecutionResult::RowsAffected(0))),
            DupPolicy::Patch(_) => unreachable!("handled above"),
        };
    }

    for idx in &prog.index_inserts {
        bufs.fk_key_buf.clear();
        for &(p, coll) in &idx.key_params {
            crate::encoding::encode_key_value_collated_into(
                &params[p as usize],
                coll,
                &mut bufs.fk_key_buf,
            );
        }
        crate::encoding::encode_key_value_into(
            &params[prog.pk_param as usize],
            &mut bufs.fk_key_buf,
        );
        wtx.table_insert_index(&idx.table, &bufs.fk_key_buf, &[])
            .map_err(SqlError::Storage)?;
    }

    Ok(Some(ExecutionResult::RowsAffected(1)))
}

fn build_bind_plan(
    stmt: &InsertStmt,
    col_indices: &[usize],
    table_schema: &TableSchema,
) -> Option<Vec<BindAction>> {
    let rows = match &stmt.source {
        InsertSource::Values(rows) => rows,
        _ => return None,
    };
    if rows.len() != 1 {
        return None;
    }
    let value_row = &rows[0];
    if value_row.len() != col_indices.len() {
        return None;
    }
    let mut plan = Vec::with_capacity(value_row.len());
    for (i, expr) in value_row.iter().enumerate() {
        let col_idx = col_indices[i];
        let col = &table_schema.columns[col_idx];
        let target = col.data_type;
        match expr {
            Expr::Parameter(n) => {
                if *n == 0 {
                    return None;
                }
                plan.push(BindAction::Param {
                    param_idx: n - 1,
                    col_idx,
                    target,
                });
            }
            Expr::Literal(v) => {
                if v.is_null() && !col.nullable {
                    return None;
                }
                let value = coerce_for_column(v.clone(), col, table_schema.is_strict()).ok()?;
                plan.push(BindAction::Literal { value, col_idx });
            }
            _ => return None,
        }
    }
    Some(plan)
}

impl CompiledInsert {
    pub fn try_compile(schema: &SchemaManager, stmt: &InsertStmt) -> Option<Self> {
        let lower = stmt.table.to_ascii_lowercase();
        // Matview names resolve to their backing table; only the interpreted
        // path raises the modification error.
        let cached = if schema.get_matview(&lower).is_some() {
            None
        } else if let Some(ts) = schema.get(&lower) {
            let insert_columns: Vec<&str> = if stmt.columns.is_empty() {
                ts.columns.iter().map(|c| c.name.as_str()).collect()
            } else {
                stmt.columns.iter().map(|s| s.as_str()).collect()
            };
            let mut col_indices = Vec::with_capacity(insert_columns.len());
            for name in &insert_columns {
                col_indices.push(ts.column_index(name)?);
            }
            if col_indices
                .iter()
                .any(|&ci| ts.columns[ci].generated_kind.is_some())
            {
                return None;
            }
            let on_conflict = stmt
                .on_conflict
                .as_ref()
                .map(|oc| compile_on_conflict(oc, ts))
                .transpose()
                .ok()?
                .map(Arc::new);
            let required_virtuals = required_insert_virtuals(schema, ts, stmt);
            let generated_col_positions: Vec<usize> = ts
                .columns
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    (matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored))
                        || required_virtuals.before_insert.contains(&i))
                    .then_some(i)
                })
                .collect();
            let generated_fast_evals: Vec<FastGenEval> = generated_col_positions
                .iter()
                .map(|&pos| {
                    detect_fast_gen_eval(ts.columns[pos].generated_expr.as_ref().unwrap(), ts)
                })
                .collect();
            let pk_indices: Vec<usize> = ts.pk_indices().to_vec();
            let non_pk_indices: Vec<usize> = ts.non_pk_indices().to_vec();
            let encoding_positions: Vec<u16> = ts.encoding_positions().to_vec();
            let dropped_non_pk_slots: Vec<u16> = ts.dropped_non_pk_slots().to_vec();
            let phys_count = ts.physical_non_pk_count();
            let single_int_pk =
                pk_indices.len() == 1 && ts.columns[pk_indices[0]].data_type == DataType::Integer;
            let not_null_indices: Vec<u16> = ts
                .columns
                .iter()
                .filter(|c| !c.nullable)
                .map(|c| c.position)
                .collect();
            let bind_plan = build_bind_plan(stmt, &col_indices, ts);
            let any_defaults_flag = ts.columns.iter().any(|c| c.default_expr.is_some());
            let row_fully_overwritten = if any_defaults_flag {
                false
            } else {
                let mut covered: rustc_hash::FxHashSet<usize> =
                    col_indices.iter().copied().collect();
                covered.extend(generated_col_positions.iter().copied());
                for (j, &i) in non_pk_indices.iter().enumerate() {
                    let _ = j;
                    if matches!(
                        ts.columns[i].generated_kind,
                        Some(crate::parser::GeneratedKind::Virtual)
                    ) {
                        covered.insert(i);
                    }
                }
                bind_plan.is_some() && covered.len() == ts.columns.len()
            };
            let mut non_virtual_pairs: Vec<(usize, usize)> = Vec::new();
            let mut null_value_slots: Vec<usize> =
                dropped_non_pk_slots.iter().map(|&s| s as usize).collect();
            for (j, &i) in non_pk_indices.iter().enumerate() {
                let slot = encoding_positions[j] as usize;
                if matches!(
                    ts.columns[i].generated_kind,
                    Some(crate::parser::GeneratedKind::Virtual)
                ) {
                    null_value_slots.push(slot);
                } else {
                    non_virtual_pairs.push((i, slot));
                }
            }
            let row_encoder = {
                let all_int_or_null = non_pk_indices.iter().enumerate().all(|(j, &i)| {
                    let col = &ts.columns[i];
                    if matches!(
                        col.generated_kind,
                        Some(crate::parser::GeneratedKind::Virtual)
                    ) {
                        true
                    } else {
                        col.data_type == DataType::Integer && encoding_positions[j] != u16::MAX
                    }
                });
                if all_int_or_null {
                    let mut slots: Vec<crate::encoding::TemplateSlot> = (0..phys_count)
                        .map(|_| crate::encoding::TemplateSlot::IntHole)
                        .collect();
                    for &s in &dropped_non_pk_slots {
                        slots[s as usize] = crate::encoding::TemplateSlot::Null;
                    }
                    for (j, &i) in non_pk_indices.iter().enumerate() {
                        if matches!(
                            ts.columns[i].generated_kind,
                            Some(crate::parser::GeneratedKind::Virtual)
                        ) {
                            slots[encoding_positions[j] as usize] =
                                crate::encoding::TemplateSlot::Null;
                        }
                    }
                    Some(crate::encoding::build_row_template(phys_count, &slots))
                } else {
                    None
                }
            };
            // build_trivial_fast_program rejects any shape it can't compile.
            let is_trivial_fast_eligible = !insert_has_subquery(stmt)
                && required_virtuals.before_insert.is_empty()
                && required_virtuals.after_insert.is_empty()
                && !ts.columns.iter().any(|c| c.default_expr.is_some())
                && !ts.has_checks()
                && stmt.returning.is_none()
                && bind_plan.is_some()
                && row_fully_overwritten
                && single_int_pk
                && !super::triggers::has_insert_triggers(schema, &ts.name)
                // A DO UPDATE dup hit fires UPDATE row triggers on the slow path.
                && (stmt.on_conflict.is_none()
                    || !super::triggers::has_update_triggers(schema, &ts.name))
                && generated_fast_evals
                    .iter()
                    .all(|fe| !matches!(fe, FastGenEval::None));
            let trivial_fast_program = if is_trivial_fast_eligible
                && ts.foreign_keys.iter().all(|fk| {
                    fk.foreign_table != ts.name && super::fk::references_primary_key(schema, fk)
                }) {
                build_trivial_fast_program(
                    bind_plan.as_ref().unwrap(),
                    phys_count,
                    &non_virtual_pairs,
                    &generated_col_positions,
                    &generated_fast_evals,
                    ts,
                    on_conflict.as_deref(),
                )
            } else {
                None
            };
            let is_trivial_fast = trivial_fast_program.is_some();
            let has_checks = ts.has_checks();
            let any_defaults = ts.columns.iter().any(|c| c.default_expr.is_some());
            let needs_scoped_params = bind_plan.is_none()
                || has_checks
                || any_defaults
                || !generated_col_positions.is_empty()
                || on_conflict.is_some()
                || stmt.returning.is_some()
                || insert_has_subquery(stmt)
                || super::helpers::any_partial_index(ts);
            Some(InsertCache {
                col_indices,
                has_subquery: insert_has_subquery(stmt),
                any_defaults,
                has_checks,
                on_conflict,
                generated_col_positions,
                generated_fast_evals,
                late_virtual_positions: required_virtuals.after_insert,
                pk_indices,
                non_pk_indices,
                encoding_positions,
                dropped_non_pk_slots,
                phys_count,
                single_int_pk,
                not_null_indices,
                bind_plan,
                row_fully_overwritten,
                row_encoder,
                is_trivial_fast,
                trivial_fast_program,
                needs_scoped_params,
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
        txn: super::compile::ActiveTxnRef<'_, '_>,
    ) -> Result<ExecutionResult> {
        let ins = match stmt {
            Statement::Insert(i) => i,
            _ => {
                return Err(SqlError::Unsupported(
                    "CompiledInsert received non-INSERT statement".into(),
                ))
            }
        };
        use super::compile::ActiveTxnRef;
        match txn {
            ActiveTxnRef::None => exec_insert(db, schema, ins, params),
            ActiveTxnRef::Read(_) => Err(SqlError::Unsupported(
                "cannot execute mutating statement inside a read-only transaction".into(),
            )),
            ActiveTxnRef::Write(outer) => match self.cached.as_ref() {
                Some(c) if c.is_trivial_fast => {
                    // Patch mutates existing rows: mark like every DO UPDATE path.
                    if matches!(
                        c.trivial_fast_program.as_ref().map(|p| &p.on_dup),
                        Some(DupPolicy::Patch(_))
                    ) {
                        schema.mark_dml(&self.table_lower);
                    }
                    match with_insert_scratch(|bufs| {
                        exec_insert_trivial_fast(outer, schema, &self.table_lower, c, bufs, params)
                    })? {
                        Some(r) => Ok(r),
                        None => exec_insert_in_txn_cached(outer, schema, ins, params, c),
                    }
                }
                Some(c) => exec_insert_in_txn_cached(outer, schema, ins, params, c),
                None => exec_insert_in_txn(outer, schema, ins, params),
            },
        }
    }

    fn uses_scoped_params(&self) -> bool {
        match self.cached.as_ref() {
            Some(c) => !c.is_trivial_fast && c.needs_scoped_params,
            None => true,
        }
    }
}

fn exec_instead_of_view_insert_auto(
    db: &Database,
    schema: &SchemaManager,
    view_name: &str,
    aliases: &[String],
    stmt: &InsertStmt,
    params: &[Value],
) -> Result<ExecutionResult> {
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let r = exec_instead_of_view_insert_in_txn(&mut wtx, schema, view_name, aliases, stmt, params)?;
    super::commit_with_ann_publication(wtx, schema)?;
    Ok(r)
}

fn exec_instead_of_view_insert_in_txn(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    view_name: &str,
    aliases: &[String],
    stmt: &InsertStmt,
    params: &[Value],
) -> Result<ExecutionResult> {
    // CREATE VIEW without explicit aliases stores an empty vec; derive at runtime.
    let resolved_aliases: Vec<String> = if aliases.is_empty() {
        derive_view_columns(wtx, schema, view_name)?
    } else {
        aliases.to_vec()
    };
    let view_cols = super::triggers::view_columns_from_aliases(&resolved_aliases);
    let alias_map: rustc_hash::FxHashMap<String, usize> = resolved_aliases
        .iter()
        .enumerate()
        .map(|(i, name)| (name.to_ascii_lowercase(), i))
        .collect();
    let cancel = wtx.cancel_token().cloned();

    let target_positions: Vec<usize> = if stmt.columns.is_empty() {
        (0..resolved_aliases.len()).collect()
    } else {
        stmt.columns
            .iter()
            .map(|c| {
                alias_map
                    .get(&c.to_ascii_lowercase())
                    .copied()
                    .ok_or_else(|| SqlError::ColumnNotFound(c.clone()))
            })
            .collect::<Result<_>>()?
    };

    let source_rows: Vec<Vec<Value>> = match &stmt.source {
        InsertSource::Values(rows) => {
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                if row.len() != target_positions.len() {
                    return Err(SqlError::InvalidValue(format!(
                        "expected {} values, got {}",
                        target_positions.len(),
                        row.len()
                    )));
                }
                let mut vals = Vec::with_capacity(row.len());
                for expr in row {
                    let v = match expr {
                        Expr::Parameter(n) => params
                            .get(n - 1)
                            .cloned()
                            .ok_or_else(|| SqlError::Parse(format!("unbound parameter ${n}")))?,
                        Expr::Literal(v) => v.clone(),
                        other => eval_const_expr_with_cancel(other, cancel.as_ref())?,
                    };
                    vals.push(v);
                }
                out.push(vals);
            }
            out
        }
        InsertSource::Select(sq) => {
            let empty_ctes = CteContext::default();
            let qr = exec_query_body_write(wtx, schema, &sq.body, &empty_ctes)?;
            insert_select_rows(qr, target_positions.len())?
        }
    };

    let mut count: u64 = 0;
    for row in source_rows {
        if row.len() != target_positions.len() {
            return Err(SqlError::InvalidValue(format!(
                "expected {} values, got {}",
                target_positions.len(),
                row.len()
            )));
        }
        let mut new_row = vec![Value::Null; resolved_aliases.len()];
        for (slot, val) in target_positions.iter().zip(row) {
            new_row[*slot] = val;
        }
        super::triggers::fire_row_triggers(
            wtx,
            schema,
            view_name,
            crate::parser::TriggerTiming::InsteadOf,
            super::triggers::FireEvent::Insert,
            None,
            Some(new_row),
            &view_cols,
        )?;
        count += 1;
    }
    Ok(ExecutionResult::RowsAffected(count))
}

fn derive_view_columns(
    wtx: &mut WriteTxn<'_>,
    schema: &SchemaManager,
    view_name: &str,
) -> Result<Vec<String>> {
    use crate::parser::{QueryBody, SelectColumn, SelectQuery, SelectStmt};
    let sel = SelectStmt {
        columns: vec![SelectColumn::AllColumns],
        from: view_name.to_string(),
        from_alias: None,
        from_subquery: None,
        from_args: None,
        from_json_table: None,
        joins: vec![],
        distinct: false,
        where_clause: None,
        order_by: vec![],
        limit: Some(Expr::Literal(Value::Integer(1))),
        offset: None,
        group_by: vec![],
        having: None,
    };
    let sq = SelectQuery {
        ctes: vec![],
        recursive: false,
        body: QueryBody::Select(Box::new(sel)),
    };
    let qr = super::cte::exec_select_query_in_txn(wtx, schema, &sq)?;
    match qr {
        ExecutionResult::Query(q) => Ok(q.columns),
        _ => Ok(Vec::new()),
    }
}

#[cfg(test)]
#[path = "dml_tests.rs"]
mod tests;
