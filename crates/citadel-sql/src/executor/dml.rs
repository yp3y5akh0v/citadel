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
use super::CteContext;

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

    let generated_cols: Vec<(usize, &Expr)> = table_schema
        .columns
        .iter()
        .filter(|c| matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored)))
        .map(|c| (c.position as usize, c.generated_expr.as_ref().unwrap()))
        .collect();

    let has_checks = table_schema.has_checks();
    let strict = table_schema.is_strict();
    let row_col_map_for_gen = if !generated_cols.is_empty() {
        Some(ColumnMap::new(&table_schema.columns))
    } else {
        None
    };
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
    // DML invalidates the table's persisted ANN segment in the SAME txn
    // (rollback restores it; commit makes table-changed-but-segment-survives
    // unrepresentable for this path).
    super::ann_persist::purge_segment(&mut wtx, &table_schema.name)?;
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
                row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    coerce_for_column(val, col, strict)?
                };
            }
        } else if let Some(sel) = sel_rows {
            let sel_row = &sel[idx];
            for (i, val) in sel_row.iter().enumerate() {
                let col_idx = col_indices[i];
                let col = &table_schema.columns[col_idx];
                row[col_idx] = if val.is_null() {
                    Value::Null
                } else {
                    coerce_for_column(val.clone(), col, strict)?
                };
            }
        }

        for &(pos, def_expr) in &defaults {
            let val = eval_const_expr(def_expr)?;
            let col = &table_schema.columns[pos];
            if !val.is_null() {
                row[pos] = coerce_for_column(val, col, strict)?;
            }
        }

        if let Some(ref gen_map) = row_col_map_for_gen {
            for &(pos, gen_expr) in &generated_cols {
                let val = eval_expr(gen_expr, &EvalCtx::new(gen_map, &row))?;
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
            if fk.deferrable && fk.initially_deferred {
                let name = fk.name.as_deref().unwrap_or(&fk.foreign_table).to_string();
                wtx.defer_fk_check(citadel_txn::write_txn::DeferredFkCheck {
                    fk_name: name,
                    foreign_table: fk.foreign_table.as_bytes().to_vec(),
                    parent_key: fk_key_buf.clone(),
                });
                continue;
            }
            if !wtx.fk_check_cached(fk.foreign_table.as_bytes(), &fk_key_buf) {
                let found = wtx
                    .table_get(fk.foreign_table.as_bytes(), &fk_key_buf)
                    .map_err(SqlError::Storage)?;
                if found.is_none() {
                    let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                    return Err(SqlError::ForeignKeyViolation(name.to_string()));
                }
                wtx.mark_fk_verified(fk.foreign_table.as_bytes(), &fk_key_buf);
            }
        }

        let proposed_row_for_returning: Option<Vec<Value>> =
            returning_rows.as_ref().map(|_| row.clone());
        let row_for_stmt_trigger: Option<Vec<Value>> = if has_insert_statement_triggers {
            Some(row.clone())
        } else {
            None
        };

        let has_before_insert_triggers = schema.triggers_for(&table_schema.name).iter().any(|t| {
            t.enabled
                && t.timing == crate::parser::TriggerTiming::Before
                && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Insert))
        });
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
                row[i] = Value::Null;
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
                    .table_insert(table_schema.name.as_bytes(), &key_buf, &value_buf)
                    .map_err(SqlError::Storage)?;
                if !is_new {
                    return Err(SqlError::DuplicateKey);
                }
                let has_after_insert_triggers =
                    schema.triggers_for(&table_schema.name).iter().any(|t| {
                        t.enabled
                            && t.timing == crate::parser::TriggerTiming::After
                            && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                            && t.events
                                .iter()
                                .any(|e| matches!(e, crate::parser::TriggerEvent::Insert))
                    });
                if !table_schema.indices.is_empty() || has_after_insert_triggers {
                    for (j, &i) in pk_indices.iter().enumerate() {
                        row[i] = pk_values[j].clone();
                    }
                    for (j, &i) in non_pk.iter().enumerate() {
                        row[i] =
                            std::mem::replace(&mut value_values[enc_pos[j] as usize], Value::Null);
                    }
                    if !table_schema.indices.is_empty() {
                        insert_index_entries(&mut wtx, table_schema, &row, &pk_values)?;
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
                if let Some(r) = row_for_stmt_trigger.clone() {
                    stmt_new_rows.push(r);
                }
                count += 1;
                if let Some(buf) = returning_rows.as_mut() {
                    buf.push((None, proposed_row_for_returning));
                }
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
                let outcome = apply_insert_with_conflict(
                    &mut wtx,
                    table_schema,
                    &key_buf,
                    &value_buf,
                    &row,
                    &pk_values,
                    oc_ref,
                    row_col_map.as_ref().unwrap(),
                    stmt.returning.is_some(),
                )?;
                match outcome {
                    InsertRowOutcome::Inserted => {
                        count += 1;
                        if let Some(buf) = returning_rows.as_mut() {
                            buf.push((None, proposed_row_for_returning));
                        }
                        if let Some(r) = row_for_stmt_trigger.clone() {
                            stmt_new_rows.push(r);
                        }
                        let has_after_insert_triggers =
                            schema.triggers_for(&table_schema.name).iter().any(|t| {
                                t.enabled
                                    && t.timing == crate::parser::TriggerTiming::After
                                    && t.granularity
                                        == crate::parser::TriggerGranularity::ForEachRow
                                    && t.events
                                        .iter()
                                        .any(|e| matches!(e, crate::parser::TriggerEvent::Insert))
                            });
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
                    InsertRowOutcome::Updated { old, new } => {
                        count += 1;
                        if let Some(buf) = returning_rows.as_mut() {
                            buf.push((Some(old.clone()), Some(new.clone())));
                        }
                        let has_after_update_triggers =
                            schema.triggers_for(&table_schema.name).iter().any(|t| {
                                t.enabled
                                    && t.timing == crate::parser::TriggerTiming::After
                                    && t.granularity
                                        == crate::parser::TriggerGranularity::ForEachRow
                                    && t.events.iter().any(|e| {
                                        matches!(e, crate::parser::TriggerEvent::Update(_))
                                    })
                            });
                        if has_after_update_triggers {
                            let changed_cols: Vec<String> = match oc_ref {
                                CompiledOnConflict::DoUpdate { assignments, .. } => assignments
                                    .iter()
                                    .map(|(col_idx, _)| table_schema.columns[*col_idx].name.clone())
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
        let qr = super::helpers::project_returning(table_schema, returning_cols, &rows)?;
        super::helpers::drain_deferred_fk_checks(&mut wtx)?;
        wtx.commit().map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::Query(qr));
    }

    super::helpers::drain_deferred_fk_checks(&mut wtx)?;
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
) -> Result<QueryResult> {
    let mut rtx = db.begin_read();
    exec_subquery_with_read(&mut rtx, schema, stmt, ctes)
}

pub(super) fn exec_subquery_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<QueryResult> {
    match super::exec_select_with_read(rtx, schema, stmt, ctes)? {
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
        returning: stmt.returning.clone(),
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
        returning: stmt.returning.clone(),
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
            let total = left_qr.rows.len().saturating_add(right_qr.rows.len());
            let mut rows = Vec::with_capacity(total);
            rows.extend(left_qr.rows);
            rows.extend(right_qr.rows);
            rows
        }
        (SetOp::Union, false) => {
            let mut seen: rustc_hash::FxHashSet<Vec<Value>> = rustc_hash::FxHashSet::default();
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
            let right_set: rustc_hash::FxHashSet<Vec<Value>> = right_qr.rows.into_iter().collect();
            let mut seen: rustc_hash::FxHashSet<Vec<Value>> = rustc_hash::FxHashSet::default();
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
            let right_set: rustc_hash::FxHashSet<Vec<Value>> = right_qr.rows.into_iter().collect();
            let mut seen: rustc_hash::FxHashSet<Vec<Value>> = rustc_hash::FxHashSet::default();
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
                generated_expr: None,
                generated_sql: None,
                generated_kind: None,
                collation: crate::types::Collation::Binary,
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

    let table_schema = schema
        .get(&stmt.table)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
    super::ann_persist::purge_segment(wtx, &table_schema.name)?;

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
    if let Some(c) = cache {
        cached_gen_positions = &c.generated_col_positions;
        cached_gen_fast_evals = &c.generated_fast_evals;
        generated_cols_uncached = Vec::new();
    } else {
        cached_gen_positions = &[];
        cached_gen_fast_evals = &[];
        generated_cols_uncached = table_schema
            .columns
            .iter()
            .filter(|c| matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored)))
            .map(|c| {
                let expr = c.generated_expr.as_ref().unwrap();
                let fe = detect_fast_gen_eval(expr, table_schema);
                (c.position as usize, expr, fe)
            })
            .collect();
    }
    let has_gen_cols = !cached_gen_positions.is_empty() || !generated_cols_uncached.is_empty();
    let row_col_map_for_gen_owned: Option<ColumnMap> = if !has_gen_cols || cache.is_some() {
        None
    } else {
        Some(ColumnMap::new(&table_schema.columns))
    };
    let row_col_map_for_gen: Option<&ColumnMap> = if !has_gen_cols {
        None
    } else if let Some(c) = cache {
        c.row_col_map.as_ref()
    } else {
        row_col_map_for_gen_owned.as_ref()
    };

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
            let insert_ctes = super::materialize_all_ctes_with_outer(
                &sq.ctes,
                sq.recursive,
                outer_ctes,
                &mut |body, ctx| exec_query_body_write(wtx, schema, body, ctx),
            )?;
            let qr = exec_query_body_write(wtx, schema, &sq.body, &insert_ctes)?;
            Some(qr.rows)
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

    let skip_row_clear = cache.is_some_and(|c| c.row_fully_overwritten);
    for idx in 0..total {
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
                                let got = v.data_type();
                                v.clone().coerce_into(*target).ok_or_else(|| {
                                    SqlError::TypeMismatch {
                                        expected: target.to_string(),
                                        got: got.to_string(),
                                    }
                                })?
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

        if let Some(gen_map) = row_col_map_for_gen {
            if cache.is_some() {
                for (pos, fast) in cached_gen_positions
                    .iter()
                    .copied()
                    .zip(cached_gen_fast_evals.iter())
                {
                    let gen_expr = table_schema.columns[pos].generated_expr.as_ref().unwrap();
                    let val = eval_fast_gen(fast, gen_expr, &bufs.row, gen_map)?;
                    let col = &table_schema.columns[pos];
                    bufs.row[pos] = if val.is_null() {
                        Value::Null
                    } else {
                        let got_type = val.data_type();
                        val.coerce_into(col.data_type)
                            .ok_or_else(|| SqlError::TypeMismatch {
                                expected: col.data_type.to_string(),
                                got: got_type.to_string(),
                            })?
                    };
                }
            } else {
                for (pos, gen_expr, fast) in &generated_cols_uncached {
                    let val = eval_fast_gen(fast, gen_expr, &bufs.row, gen_map)?;
                    let col = &table_schema.columns[*pos];
                    bufs.row[*pos] = if val.is_null() {
                        Value::Null
                    } else {
                        let got_type = val.data_type();
                        val.coerce_into(col.data_type)
                            .ok_or_else(|| SqlError::TypeMismatch {
                                expected: col.data_type.to_string(),
                                got: got_type.to_string(),
                            })?
                    };
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
                crate::encoding::encode_composite_key_from_indices(
                    &fk.columns,
                    &bufs.row,
                    &mut bufs.fk_key_buf,
                );
                if fk.deferrable && fk.initially_deferred {
                    let name = fk.name.as_deref().unwrap_or(&fk.foreign_table).to_string();
                    wtx.defer_fk_check(citadel_txn::write_txn::DeferredFkCheck {
                        fk_name: name,
                        foreign_table: fk.foreign_table.as_bytes().to_vec(),
                        parent_key: bufs.fk_key_buf.clone(),
                    });
                    continue;
                }
                if !wtx.fk_check_cached(fk.foreign_table.as_bytes(), &bufs.fk_key_buf) {
                    let found = wtx
                        .table_get(fk.foreign_table.as_bytes(), &bufs.fk_key_buf)
                        .map_err(SqlError::Storage)?;
                    if found.is_none() {
                        let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                        return Err(SqlError::ForeignKeyViolation(name.to_string()));
                    }
                    wtx.mark_fk_verified(fk.foreign_table.as_bytes(), &bufs.fk_key_buf);
                }
            }
        }

        let proposed_row_for_returning: Option<Vec<Value>> =
            returning_rows.as_ref().map(|_| bufs.row.clone());
        let row_for_stmt_trigger_impl: Option<Vec<Value>> = if has_insert_statement_triggers_impl {
            Some(bufs.row.clone())
        } else {
            None
        };

        let has_before_insert_triggers = schema.triggers_for(&table_schema.name).iter().any(|t| {
            t.enabled
                && t.timing == crate::parser::TriggerTiming::Before
                && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Insert))
        });
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
                bufs.row[i] = Value::Null;
            } else {
                bufs.value_values[enc_pos[j] as usize] =
                    std::mem::replace(&mut bufs.row[i], Value::Null);
            }
        }
        match cache.and_then(|c| c.row_encoder.as_ref()) {
            Some(tmpl) => crate::encoding::encode_int_row_with_template(
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
                    .table_insert(table_bytes, &bufs.key_buf, &bufs.value_buf)
                    .map_err(SqlError::Storage)?;
                if !is_new {
                    return Err(SqlError::DuplicateKey);
                }
                let has_after_insert_triggers =
                    schema.triggers_for(&table_schema.name).iter().any(|t| {
                        t.enabled
                            && t.timing == crate::parser::TriggerTiming::After
                            && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                            && t.events
                                .iter()
                                .any(|e| matches!(e, crate::parser::TriggerEvent::Insert))
                    });
                if has_indices || has_after_insert_triggers {
                    for (j, &i) in pk_indices.iter().enumerate() {
                        bufs.row[i] = bufs.pk_values[j].clone();
                    }
                    for (j, &i) in non_pk.iter().enumerate() {
                        bufs.row[i] = std::mem::replace(
                            &mut bufs.value_values[enc_pos[j] as usize],
                            Value::Null,
                        );
                    }
                    if has_indices {
                        insert_index_entries(wtx, table_schema, &bufs.row, &bufs.pk_values)?;
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
                if let Some(r) = row_for_stmt_trigger_impl.clone() {
                    stmt_new_rows_impl.push(r);
                }
                count += 1;
                if let Some(buf) = returning_rows.as_mut() {
                    buf.push((None, proposed_row_for_returning));
                }
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
                let outcome = apply_insert_with_conflict(
                    wtx,
                    table_schema,
                    &bufs.key_buf,
                    &bufs.value_buf,
                    &bufs.row,
                    &bufs.pk_values,
                    oc_ref,
                    row_col_map.unwrap(),
                    stmt.returning.is_some(),
                )?;
                match outcome {
                    InsertRowOutcome::Inserted => {
                        count += 1;
                        if let Some(buf) = returning_rows.as_mut() {
                            buf.push((None, proposed_row_for_returning));
                        }
                        if let Some(r) = row_for_stmt_trigger_impl.clone() {
                            stmt_new_rows_impl.push(r);
                        }
                        let has_after_insert_triggers =
                            schema.triggers_for(&table_schema.name).iter().any(|t| {
                                t.enabled
                                    && t.timing == crate::parser::TriggerTiming::After
                                    && t.granularity
                                        == crate::parser::TriggerGranularity::ForEachRow
                                    && t.events
                                        .iter()
                                        .any(|e| matches!(e, crate::parser::TriggerEvent::Insert))
                            });
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
                    InsertRowOutcome::Updated { old, new } => {
                        count += 1;
                        if let Some(buf) = returning_rows.as_mut() {
                            buf.push((Some(old.clone()), Some(new.clone())));
                        }
                        let has_after_update_triggers =
                            schema.triggers_for(&table_schema.name).iter().any(|t| {
                                t.enabled
                                    && t.timing == crate::parser::TriggerTiming::After
                                    && t.granularity
                                        == crate::parser::TriggerGranularity::ForEachRow
                                    && t.events.iter().any(|e| {
                                        matches!(e, crate::parser::TriggerEvent::Update(_))
                                    })
                            });
                        if has_after_update_triggers {
                            let changed_cols: Vec<String> = match oc_ref {
                                CompiledOnConflict::DoUpdate { assignments, .. } => assignments
                                    .iter()
                                    .map(|(col_idx, _)| table_schema.columns[*col_idx].name.clone())
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
    row_col_map: Option<ColumnMap>,
    generated_col_positions: Vec<usize>,
    generated_fast_evals: Vec<FastGenEval>,
    pk_indices: Vec<usize>,
    non_pk_indices: Vec<usize>,
    encoding_positions: Vec<u16>,
    dropped_non_pk_slots: Vec<u16>,
    phys_count: usize,
    single_int_pk: bool,
    not_null_indices: Vec<u16>,
    bind_plan: Option<Vec<BindAction>>,
    row_fully_overwritten: bool,
    row_encoder: Option<crate::encoding::IntRowTemplate>,
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
    not_null_param_indices: Vec<u8>,
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
        bitmap_byte_off: u32,
        bitmap_bit_mask: u8,
    },
    GenMulAddParamI64 {
        param_idx: u8,
        mul: i64,
        add: i64,
        off: u32,
        bitmap_byte_off: u32,
        bitmap_bit_mask: u8,
    },
}

fn build_trivial_fast_program(
    bind_plan: &[BindAction],
    row_encoder: &crate::encoding::IntRowTemplate,
    non_virtual_pairs: &[(usize, usize)],
    generated_col_positions: &[usize],
    generated_fast_evals: &[FastGenEval],
    pk_indices: &[usize],
    columns: &[crate::types::ColumnDef],
) -> Option<TrivialFastProgram> {
    let pk_col = pk_indices[0];
    let col_to_slot: rustc_hash::FxHashMap<usize, usize> =
        non_virtual_pairs.iter().copied().collect();
    let slot_to_off: rustc_hash::FxHashMap<usize, usize> =
        row_encoder.slot_offsets.iter().copied().collect();

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
            BindAction::Literal { value, col_idx } => match value {
                Value::Integer(v) => {
                    col_to_lit_int.insert(*col_idx, *v);
                    if *col_idx == pk_col {
                        return None;
                    }
                    let slot = *col_to_slot.get(col_idx)?;
                    let off = u32::try_from(*slot_to_off.get(&slot)?).ok()?;
                    ops.push(WriteOp::LiteralI64 { value: *v, off });
                }
                _ => return None,
            },
        }
    }

    let pk_param = pk_param?;

    for (i, &gen_pos) in generated_col_positions.iter().enumerate() {
        let gen_slot = *col_to_slot.get(&gen_pos)?;
        let gen_off = u32::try_from(*slot_to_off.get(&gen_slot)?).ok()?;
        let bitmap_byte_off = u32::try_from(2 + gen_slot / 8).ok()?;
        let bitmap_bit_mask: u8 = 1u8 << (gen_slot % 8);
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
                            bitmap_byte_off,
                            bitmap_bit_mask,
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
                            bitmap_byte_off,
                            bitmap_bit_mask,
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
                            bitmap_byte_off,
                            bitmap_bit_mask,
                        });
                    }
                    (None, None) => {
                        let la = col_to_lit_int.get(left_idx).copied()?;
                        let lb = col_to_lit_int.get(right_idx).copied()?;
                        ops.push(WriteOp::LiteralI64 {
                            value: la.wrapping_add(lb),
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
                        bitmap_byte_off,
                        bitmap_bit_mask,
                    });
                } else if let Some(lit) = col_to_lit_int.get(col_schema_idx).copied() {
                    ops.push(WriteOp::LiteralI64 {
                        value: lit.wrapping_mul(*mul).wrapping_add(*add),
                        off: gen_off,
                    });
                } else {
                    return None;
                }
            }
            FastGenEval::None => return None,
        }
    }

    Some(TrivialFastProgram {
        template: row_encoder.template.clone(),
        ops,
        pk_param,
        not_null_param_indices,
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
    Updated { old: Vec<Value>, new: Vec<Value> },
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
                                table_schema,
                                &existing_pk,
                                row,
                                assignments,
                                where_clause.as_ref(),
                                col_map,
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
                        capture_returning,
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

        if bufs.new_value_buf.len() > citadel_core::MAX_VALUE_SIZE {
            return Err(SqlError::RowTooLarge {
                size: bufs.new_value_buf.len(),
                max: citadel_core::MAX_VALUE_SIZE,
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
    capture_returning: bool,
) -> Result<InsertRowOutcome> {
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let phys_count = table_schema.physical_non_pk_count();
    let dropped = table_schema.dropped_non_pk_slots();
    let has_checks = table_schema.has_checks();
    let has_fks = !table_schema.foreign_keys.is_empty();

    let captured: std::cell::RefCell<Option<(Vec<Value>, Vec<Value>)>> =
        std::cell::RefCell::new(None);

    let outcome =
        wtx.table_upsert_with::<_, SqlError>(table_bytes, key_buf, value_buf, |old_bytes| {
            if let Some(fps) = fast_paths {
                if !has_checks {
                    let action = apply_fast_path_patch(old_bytes, fps)?;
                    if capture_returning {
                        if let UpsertAction::Replace(ref new_bytes) = action {
                            let old_row = decode_full_row(table_schema, key_buf, old_bytes)?;
                            let new_row = decode_full_row(table_schema, key_buf, new_bytes)?;
                            *captured.borrow_mut() = Some((old_row, new_row));
                        }
                    }
                    return Ok(action);
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
            if capture_returning {
                let (old, new) = captured.into_inner().ok_or_else(|| {
                    SqlError::InvalidValue("DO UPDATE produced no captured rows".into())
                })?;
                Ok(InsertRowOutcome::Updated { old, new })
            } else {
                Ok(InsertRowOutcome::Inserted)
            }
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
    table_schema: &TableSchema,
    pk_key: &[u8],
    proposed_row: &[Value],
    assignments: &[(usize, Expr)],
    where_clause: Option<&Expr>,
    col_map: &ColumnMap,
    capture_returning: bool,
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
        capture_returning,
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
    capture_returning: bool,
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

    for col in &table_schema.columns {
        if matches!(
            col.generated_kind,
            Some(crate::parser::GeneratedKind::Stored)
        ) {
            let val = eval_expr(
                col.generated_expr.as_ref().unwrap(),
                &EvalCtx::new(col_map, &new_row),
            )?;
            let pos = col.position as usize;
            new_row[pos] = if val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
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
        if fk.deferrable && fk.initially_deferred {
            let name = fk.name.as_deref().unwrap_or(&fk.foreign_table).to_string();
            wtx.defer_fk_check(citadel_txn::write_txn::DeferredFkCheck {
                fk_name: name,
                foreign_table: fk.foreign_table.as_bytes().to_vec(),
                parent_key: fk_key,
            });
            continue;
        }
        if !wtx.fk_check_cached(fk.foreign_table.as_bytes(), &fk_key) {
            let found = wtx
                .table_get(fk.foreign_table.as_bytes(), &fk_key)
                .map_err(SqlError::Storage)?;
            if found.is_none() {
                let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                return Err(SqlError::ForeignKeyViolation(name.to_string()));
            }
            wtx.mark_fk_verified(fk.foreign_table.as_bytes(), &fk_key);
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
            let old_idx_key =
                encode_index_key_with_schema(idx, old_row, &old_pk_values, table_schema);
            wtx.table_delete(&idx_table, &old_idx_key)
                .map_err(SqlError::Storage)?;
            let new_idx_key =
                encode_index_key_with_schema(idx, &new_row, &new_pk_values, table_schema);
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
    } else {
        wtx.table_update_sorted(
            table_schema.name.as_bytes(),
            &[(old_pk_key, new_value_buf.as_slice())],
        )
        .map_err(SqlError::Storage)?;
        let col_map_partial = any_partial_index(table_schema).then(|| table_schema.column_map());
        for idx in &table_schema.indices {
            let cols_changed = index_columns_changed(idx, old_row, &new_row);
            let (del, ins) = partial_idx_update_actions(
                idx,
                old_row,
                &new_row,
                cols_changed,
                false,
                col_map_partial,
            );
            let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
            if del {
                let old_idx_key =
                    encode_index_key_with_schema(idx, old_row, &old_pk_values, table_schema);
                wtx.table_delete(&idx_table, &old_idx_key)
                    .map_err(SqlError::Storage)?;
            }
            if ins {
                let new_idx_key =
                    encode_index_key_with_schema(idx, &new_row, &new_pk_values, table_schema);
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

    if capture_returning {
        Ok(InsertRowOutcome::Updated {
            old: old_row.to_vec(),
            new: new_row,
        })
    } else {
        Ok(InsertRowOutcome::Inserted)
    }
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

/// Caller MUST check `cache.is_trivial_fast` first.
fn exec_insert_trivial_fast(
    wtx: &mut WriteTxn<'_>,
    table_lower: &str,
    cache: &InsertCache,
    bufs: &mut InsertBufs,
    params: &[Value],
) -> Result<ExecutionResult> {
    let prog = cache
        .trivial_fast_program
        .as_ref()
        .expect("trivial fast: program");

    for &p in &prog.not_null_param_indices {
        if params[p as usize].is_null() {
            return Err(SqlError::NotNullViolation(format!("param@{p}")));
        }
    }

    match &params[prog.pk_param as usize] {
        Value::Integer(v) => crate::encoding::encode_int_key_into(*v, &mut bufs.key_buf),
        _ => return Err(SqlError::InvalidValue("non-integer PK in fast path".into())),
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
                other => {
                    return Err(SqlError::TypeMismatch {
                        expected: "Integer".into(),
                        got: other.data_type().to_string(),
                    });
                }
            },
            WriteOp::LiteralI64 { value, off } => {
                let off = *off as usize;
                bufs.value_buf[off..off + 8].copy_from_slice(&value.to_le_bytes());
            }
            WriteOp::GenAddParamsI64 {
                a_param,
                b_param,
                off,
                bitmap_byte_off,
                bitmap_bit_mask,
            } => match (&params[*a_param as usize], &params[*b_param as usize]) {
                (Value::Integer(a), Value::Integer(b)) => {
                    let off = *off as usize;
                    bufs.value_buf[off..off + 8].copy_from_slice(&a.wrapping_add(*b).to_le_bytes());
                }
                _ => {
                    bufs.value_buf[*bitmap_byte_off as usize] |= *bitmap_bit_mask;
                }
            },
            WriteOp::GenMulAddParamI64 {
                param_idx,
                mul,
                add,
                off,
                bitmap_byte_off,
                bitmap_bit_mask,
            } => match &params[*param_idx as usize] {
                Value::Integer(v) => {
                    let r = v.wrapping_mul(*mul).wrapping_add(*add);
                    let off = *off as usize;
                    bufs.value_buf[off..off + 8].copy_from_slice(&r.to_le_bytes());
                }
                _ => {
                    bufs.value_buf[*bitmap_byte_off as usize] |= *bitmap_bit_mask;
                }
            },
        }
    }

    let is_new = wtx
        .table_insert(table_lower.as_bytes(), &bufs.key_buf, &bufs.value_buf)
        .map_err(SqlError::Storage)?;
    if !is_new {
        return Err(SqlError::DuplicateKey);
    }
    Ok(ExecutionResult::RowsAffected(1))
}

fn build_bind_plan(
    stmt: &InsertStmt,
    col_indices: &[usize],
    col_data_types: &[DataType],
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
        let target = col_data_types[col_idx];
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
            Expr::Literal(v) => plan.push(BindAction::Literal {
                value: v.clone(),
                col_idx,
            }),
            _ => return None,
        }
    }
    Some(plan)
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
                .ok()
                .flatten()
                .map(Arc::new);
            let generated_col_positions: Vec<usize> = ts
                .columns
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored))
                        .then_some(i)
                })
                .collect();
            let generated_fast_evals: Vec<FastGenEval> = generated_col_positions
                .iter()
                .map(|&pos| {
                    detect_fast_gen_eval(ts.columns[pos].generated_expr.as_ref().unwrap(), ts)
                })
                .collect();
            let row_col_map = if on_conflict.is_some() || !generated_col_positions.is_empty() {
                Some(ColumnMap::new(&ts.columns))
            } else {
                None
            };
            let pk_indices: Vec<usize> = ts.pk_indices().to_vec();
            let non_pk_indices: Vec<usize> = ts.non_pk_indices().to_vec();
            let encoding_positions: Vec<u16> = ts.encoding_positions().to_vec();
            let dropped_non_pk_slots: Vec<u16> = ts.dropped_non_pk_slots().to_vec();
            let phys_count = ts.physical_non_pk_count();
            let col_data_types: Vec<DataType> = ts.columns.iter().map(|c| c.data_type).collect();
            let single_int_pk =
                pk_indices.len() == 1 && ts.columns[pk_indices[0]].data_type == DataType::Integer;
            let not_null_indices: Vec<u16> = ts
                .columns
                .iter()
                .filter(|c| !c.nullable)
                .map(|c| c.position)
                .collect();
            let bind_plan = build_bind_plan(stmt, &col_indices, &col_data_types);
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
            let has_fks = !ts.foreign_keys.is_empty();
            let has_indices = !ts.indices.is_empty();
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
                    let mut null_slots: Vec<usize> =
                        dropped_non_pk_slots.iter().map(|&s| s as usize).collect();
                    for (j, &i) in non_pk_indices.iter().enumerate() {
                        if matches!(
                            ts.columns[i].generated_kind,
                            Some(crate::parser::GeneratedKind::Virtual)
                        ) {
                            null_slots.push(encoding_positions[j] as usize);
                        }
                    }
                    Some(crate::encoding::build_int_row_template(
                        phys_count,
                        &null_slots,
                    ))
                } else {
                    None
                }
            };
            let is_trivial_fast_eligible = !insert_has_subquery(stmt)
                && !ts.columns.iter().any(|c| c.default_expr.is_some())
                && !ts.has_checks()
                && !has_fks
                && !has_indices
                && stmt.on_conflict.is_none()
                && stmt.returning.is_none()
                && bind_plan.is_some()
                && row_encoder.is_some()
                && row_fully_overwritten
                && single_int_pk
                && generated_fast_evals
                    .iter()
                    .all(|fe| !matches!(fe, FastGenEval::None));
            let trivial_fast_program = if is_trivial_fast_eligible {
                build_trivial_fast_program(
                    bind_plan.as_ref().unwrap(),
                    row_encoder.as_ref().unwrap(),
                    &non_virtual_pairs,
                    &generated_col_positions,
                    &generated_fast_evals,
                    &pk_indices,
                    &ts.columns,
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
                row_col_map,
                generated_col_positions,
                generated_fast_evals,
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
                Some(c) if c.is_trivial_fast => with_insert_scratch(|bufs| {
                    exec_insert_trivial_fast(outer, &self.table_lower, c, bufs, params)
                }),
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
        txn: super::compile::ActiveTxnRef<'_, '_>,
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
        use super::compile::ActiveTxnRef;
        match txn {
            ActiveTxnRef::None => super::write::exec_delete(db, schema, del),
            ActiveTxnRef::Read(_) => Err(SqlError::Unsupported(
                "cannot execute mutating statement inside a read-only transaction".into(),
            )),
            ActiveTxnRef::Write(outer) => super::write::exec_delete_in_txn(outer, schema, del),
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
    wtx.commit().map_err(SqlError::Storage)?;
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
                        other => eval_const_expr(other)?,
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
            qr.rows
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
