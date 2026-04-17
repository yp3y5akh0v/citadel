use std::collections::HashMap;

use citadel::Database;

use crate::encoding::{
    decode_column_raw, decode_column_with_offset, decode_composite_key, decode_pk_integer,
    encode_composite_key, encode_row, patch_at_offset, patch_column_in_place, patch_row_column,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::correlated::*;
use super::dml::*;
use super::helpers::*;
use super::scan::*;
use super::select::*;
use super::view::*;
use super::CteContext;

// ── Compiled UPDATE plan cache ──────────────────────────────────────

pub struct UpdateBufs {
    partial_row: Vec<Value>,
    patch_buf: Vec<u8>,
    offsets: Vec<usize>,
}

impl Default for UpdateBufs {
    fn default() -> Self {
        Self {
            partial_row: Vec::new(),
            patch_buf: Vec::with_capacity(256),
            offsets: Vec::new(),
        }
    }
}

impl UpdateBufs {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct CompiledUpdate {
    table_name_lower: String,
    is_view: bool,
    has_correlated_where: bool,
    has_subquery: bool,
    can_fast_path: bool,
    fast: Option<CompiledFastPath>,
}

struct CompiledFastPath {
    num_pk_cols: usize,
    num_columns: usize,
    single_int_pk: bool,
    targets: Vec<CompiledTarget>,
    scan_plan: crate::planner::ScanPlan,
    pk_idx_cache: Vec<usize>,
    col_map: ColumnMap,
    range_bounds_i64: Option<Vec<(BinOp, i64)>>,
}

enum FastEval {
    None,
    IntAdd(i64),
    IntSub(i64),
    IntMul(i64),
    IntSet(i64),
}

struct CompiledTarget {
    schema_idx: usize,
    phys_idx: usize,
    expr: Expr,
    col: ColumnDef,
    fast_eval: FastEval,
}

fn detect_fast_eval(expr: &Expr, col_name: &str) -> FastEval {
    let lower = col_name.to_ascii_lowercase();
    match expr {
        Expr::Literal(Value::Integer(n)) => FastEval::IntSet(*n),
        Expr::BinaryOp { left, op, right } => {
            let col_match =
                |e: &Expr| matches!(e, Expr::Column(c) if c.to_ascii_lowercase() == lower);
            let int_lit = |e: &Expr| match e {
                Expr::Literal(Value::Integer(n)) => Some(*n),
                _ => None,
            };
            if col_match(left) {
                if let Some(n) = int_lit(right) {
                    return match op {
                        BinOp::Add => FastEval::IntAdd(n),
                        BinOp::Sub => FastEval::IntSub(n),
                        BinOp::Mul => FastEval::IntMul(n),
                        _ => FastEval::None,
                    };
                }
            }
            if col_match(right) {
                if let Some(n) = int_lit(left) {
                    return match op {
                        BinOp::Add => FastEval::IntAdd(n),
                        BinOp::Mul => FastEval::IntMul(n),
                        _ => FastEval::None,
                    };
                }
            }
            FastEval::None
        }
        _ => FastEval::None,
    }
}

pub fn compile_update(schema: &SchemaManager, stmt: &UpdateStmt) -> Result<CompiledUpdate> {
    let table_name_lower = stmt.table.to_ascii_lowercase();
    let is_view = schema.get_view(&table_name_lower).is_some();
    if is_view {
        return Ok(CompiledUpdate {
            table_name_lower,
            is_view: true,
            has_correlated_where: false,
            has_subquery: false,
            can_fast_path: false,
            fast: None,
        });
    }

    let table_schema = schema
        .get(&table_name_lower)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let corr_ctx = CorrelationCtx {
        outer_schema: table_schema,
        outer_alias: None,
    };
    let has_correlated = has_correlated_where(&stmt.where_clause, &corr_ctx, schema);
    let has_sub = update_has_subquery(stmt);

    if has_correlated || has_sub {
        return Ok(CompiledUpdate {
            table_name_lower,
            is_view: false,
            has_correlated_where: has_correlated,
            has_subquery: has_sub,
            can_fast_path: false,
            fast: None,
        });
    }

    let pk_indices = table_schema.pk_indices();
    let pk_changed_by_set = stmt.assignments.iter().any(|(col_name, _)| {
        table_schema
            .column_index(col_name)
            .is_some_and(|idx| table_schema.primary_key_columns.contains(&(idx as u16)))
    });
    let has_fk = !table_schema.foreign_keys.is_empty();
    let has_indices = !table_schema.indices.is_empty();
    let has_child_fk = !schema.child_fks_for(&table_name_lower).is_empty();
    let can_fast_path = !pk_changed_by_set
        && !has_fk
        && !has_indices
        && !has_child_fk
        && !table_schema.has_checks();

    let fast = if can_fast_path {
        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let num_pk_cols = table_schema.primary_key_columns.len();

        let mut targets = Vec::with_capacity(stmt.assignments.len());
        for (col_name, expr) in &stmt.assignments {
            let schema_idx = table_schema
                .column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let nonpk_order = non_pk
                .iter()
                .position(|&i| i == schema_idx)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let phys_idx = enc_pos[nonpk_order] as usize;
            let fast_eval = detect_fast_eval(expr, col_name);
            targets.push(CompiledTarget {
                schema_idx,
                phys_idx,
                expr: expr.clone(),
                col: table_schema.columns[schema_idx].clone(),
                fast_eval,
            });
        }

        let plan = crate::planner::plan_select(table_schema, &stmt.where_clause);
        let single_int_pk = num_pk_cols == 1
            && table_schema.columns[table_schema.primary_key_columns[0] as usize].data_type
                == DataType::Integer;

        let range_bounds_i64 = if single_int_pk {
            if let crate::planner::ScanPlan::PkRangeScan {
                ref range_conds, ..
            } = plan
            {
                let bounds: Vec<(BinOp, i64)> = range_conds
                    .iter()
                    .filter_map(|(op, val)| match val {
                        Value::Integer(i) => Some((*op, *i)),
                        _ => None,
                    })
                    .collect();
                if bounds.len() == range_conds.len() {
                    Some(bounds)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Some(CompiledFastPath {
            num_pk_cols,
            num_columns: table_schema.columns.len(),
            single_int_pk,
            targets,
            scan_plan: plan,
            pk_idx_cache: pk_indices.to_vec(),
            col_map: ColumnMap::new(&table_schema.columns),
            range_bounds_i64,
        })
    } else {
        None
    };

    Ok(CompiledUpdate {
        table_name_lower,
        is_view: false,
        has_correlated_where: false,
        has_subquery: false,
        can_fast_path,
        fast,
    })
}

pub fn exec_update_compiled(
    db: &Database,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
    compiled: &CompiledUpdate,
    bufs: &mut UpdateBufs,
) -> Result<ExecutionResult> {
    if compiled.is_view {
        return Err(SqlError::CannotModifyView(stmt.table.clone()));
    }
    if compiled.has_correlated_where || compiled.has_subquery || !compiled.can_fast_path {
        return exec_update(db, schema, stmt);
    }

    let fast = compiled.fast.as_ref().unwrap();
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;

    if let crate::planner::ScanPlan::PkRangeScan {
        ref start_key,
        ref range_conds,
        ..
    } = fast.scan_plan
    {
        bufs.partial_row.clear();
        bufs.partial_row.resize(fast.num_columns, Value::Null);
        bufs.offsets.clear();
        bufs.offsets.resize(fast.targets.len(), usize::MAX);

        let count = wtx.table_update_range(
            compiled.table_name_lower.as_bytes(),
            start_key,
            |key, value| {
                if let Some(ref bounds) = fast.range_bounds_i64 {
                    let pk = decode_pk_integer(key)?;
                    for &(op, bound) in bounds {
                        match op {
                            BinOp::Lt if pk >= bound => return Ok(None),
                            BinOp::LtEq if pk > bound => return Ok(None),
                            BinOp::Gt if pk <= bound => return Ok(Some(false)),
                            BinOp::GtEq if pk < bound => return Ok(Some(false)),
                            _ => {}
                        }
                    }
                    bufs.partial_row[fast.pk_idx_cache[0]] = Value::Integer(pk);
                } else if fast.single_int_pk {
                    let pk = decode_pk_integer(key)?;
                    let pk_val = Value::Integer(pk);
                    for (op, bound) in range_conds {
                        match op {
                            BinOp::Lt if &pk_val >= bound => return Ok(None),
                            BinOp::LtEq if &pk_val > bound => return Ok(None),
                            BinOp::Gt if &pk_val <= bound => return Ok(Some(false)),
                            BinOp::GtEq if &pk_val < bound => return Ok(Some(false)),
                            _ => {}
                        }
                    }
                    bufs.partial_row[fast.pk_idx_cache[0]] = pk_val;
                } else {
                    let pk_vals = decode_composite_key(key, fast.num_pk_cols)?;
                    for (op, bound) in range_conds {
                        match op {
                            BinOp::Lt if &pk_vals[0] >= bound => return Ok(None),
                            BinOp::LtEq if &pk_vals[0] > bound => return Ok(None),
                            BinOp::Gt if &pk_vals[0] <= bound => return Ok(Some(false)),
                            BinOp::GtEq if &pk_vals[0] < bound => return Ok(Some(false)),
                            _ => {}
                        }
                    }
                    for (i, &pi) in fast.pk_idx_cache.iter().enumerate() {
                        bufs.partial_row[pi] = pk_vals[i].clone();
                    }
                }
                for (i, target) in fast.targets.iter().enumerate() {
                    let (raw, off) = decode_column_with_offset(value, target.phys_idx)?;
                    bufs.partial_row[target.schema_idx] = raw.to_value();
                    bufs.offsets[i] = off;
                }
                for (i, target) in fast.targets.iter().enumerate() {
                    let new_val = match target.fast_eval {
                        FastEval::IntAdd(n) => {
                            if let Value::Integer(v) = bufs.partial_row[target.schema_idx] {
                                Value::Integer(v.wrapping_add(n))
                            } else {
                                eval_expr(&target.expr, &fast.col_map, &bufs.partial_row)?
                            }
                        }
                        FastEval::IntSub(n) => {
                            if let Value::Integer(v) = bufs.partial_row[target.schema_idx] {
                                Value::Integer(v.wrapping_sub(n))
                            } else {
                                eval_expr(&target.expr, &fast.col_map, &bufs.partial_row)?
                            }
                        }
                        FastEval::IntMul(n) => {
                            if let Value::Integer(v) = bufs.partial_row[target.schema_idx] {
                                Value::Integer(v.wrapping_mul(n))
                            } else {
                                eval_expr(&target.expr, &fast.col_map, &bufs.partial_row)?
                            }
                        }
                        FastEval::IntSet(n) => Value::Integer(n),
                        FastEval::None => {
                            eval_expr(&target.expr, &fast.col_map, &bufs.partial_row)?
                        }
                    };
                    let coerced = if new_val.is_null() {
                        if !target.col.nullable {
                            return Err(SqlError::NotNullViolation(target.col.name.clone()));
                        }
                        Value::Null
                    } else {
                        let got_type = new_val.data_type();
                        new_val.coerce_into(target.col.data_type).ok_or_else(|| {
                            SqlError::TypeMismatch {
                                expected: target.col.data_type.to_string(),
                                got: got_type.to_string(),
                            }
                        })?
                    };
                    if !patch_at_offset(value, bufs.offsets[i], &coerced)?
                        && !patch_column_in_place(value, target.phys_idx, &coerced)?
                    {
                        patch_row_column(value, target.phys_idx, &coerced, &mut bufs.patch_buf)?;
                        value[..bufs.patch_buf.len()].copy_from_slice(&bufs.patch_buf);
                        for off in bufs.offsets.iter_mut().skip(i + 1) {
                            *off = usize::MAX;
                        }
                    }
                }
                Ok(Some(true))
            },
        )?;

        wtx.commit().map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::RowsAffected(count));
    }

    // PkLookup / SeqScan — fall back to full exec_update for now
    drop(wtx);
    exec_update(db, schema, stmt)
}

// ── UPDATE / DELETE execution ───────────────────────────────────────

pub(super) fn exec_update(
    db: &Database,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.table.to_ascii_lowercase();
    if schema.get_view(&lower_name).is_some() {
        return Err(SqlError::CannotModifyView(stmt.table.clone()));
    }
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    // Correlated subquery in UPDATE WHERE — check BEFORE materialization
    let corr_ctx = CorrelationCtx {
        outer_schema: table_schema,
        outer_alias: None,
    };
    if has_correlated_where(&stmt.where_clause, &corr_ctx, schema) {
        let select_stmt = SelectStmt {
            columns: vec![SelectColumn::AllColumns],
            from: stmt.table.clone(),
            from_alias: None,
            joins: vec![],
            distinct: false,
            where_clause: stmt.where_clause.clone(),
            order_by: vec![],
            limit: None,
            offset: None,
            group_by: vec![],
            having: None,
        };
        let (mut rows, _) = collect_rows_read(db, table_schema, &None, None)?;
        let remaining =
            handle_correlated_where_read(db, schema, &select_stmt, &corr_ctx, &mut rows)?;

        if let Some(ref w) = remaining {
            let col_map = ColumnMap::new(&table_schema.columns);
            rows.retain(|row| match eval_expr(w, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            });
        }

        let pk_indices = table_schema.pk_indices();
        let pk_values: Vec<Value> = rows.iter().map(|row| row[pk_indices[0]].clone()).collect();
        let pk_col = &table_schema.columns[pk_indices[0]].name;
        let in_set: std::collections::HashSet<Value> = pk_values.into_iter().collect();
        let new_where = if in_set.is_empty() {
            Some(Expr::Literal(Value::Boolean(false)))
        } else {
            Some(Expr::InSet {
                expr: Box::new(Expr::Column(pk_col.clone())),
                values: in_set,
                has_null: false,
                negated: false,
            })
        };

        let rewritten = UpdateStmt {
            table: stmt.table.clone(),
            assignments: stmt.assignments.clone(),
            where_clause: new_where,
        };
        return exec_update(db, schema, &rewritten);
    }

    let materialized;
    let stmt = if update_has_subquery(stmt) {
        materialized = materialize_update(stmt, &mut |sub| {
            exec_subquery_read(db, schema, sub, &HashMap::new())
        })?;
        &materialized
    } else {
        stmt
    };

    let col_map = ColumnMap::new(&table_schema.columns);
    let pk_indices = table_schema.pk_indices();

    let pk_changed_by_set = stmt.assignments.iter().any(|(col_name, _)| {
        table_schema
            .column_index(col_name)
            .is_some_and(|idx| table_schema.primary_key_columns.contains(&(idx as u16)))
    });

    // Fast path: no FK, no indices, no PK change → raw-byte scan + patch
    let has_fk = !table_schema.foreign_keys.is_empty();
    let has_indices = !table_schema.indices.is_empty();
    let has_child_fk = !schema.child_fks_for(&lower_name).is_empty();
    if !pk_changed_by_set && !has_fk && !has_indices && !has_child_fk && !table_schema.has_checks()
    {
        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let num_pk_cols = table_schema.primary_key_columns.len();

        struct AssignTarget {
            schema_idx: usize,
            phys_idx: usize,
            expr: Expr,
            col: ColumnDef,
        }
        let mut targets: Vec<AssignTarget> = Vec::with_capacity(stmt.assignments.len());
        for (col_name, expr) in &stmt.assignments {
            let schema_idx = table_schema
                .column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let nonpk_order = non_pk
                .iter()
                .position(|&i| i == schema_idx)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let phys_idx = enc_pos[nonpk_order] as usize;
            targets.push(AssignTarget {
                schema_idx,
                phys_idx,
                expr: expr.clone(),
                col: table_schema.columns[schema_idx].clone(),
            });
        }

        let plan = crate::planner::plan_select(table_schema, &stmt.where_clause);
        let single_int_pk = num_pk_cols == 1
            && table_schema.columns[table_schema.primary_key_columns[0] as usize].data_type
                == DataType::Integer;

        let mut wtx = db.begin_write().map_err(SqlError::Storage)?;

        // Fused PkRangeScan: scan + patch in a single leaf pass, zero allocs
        if let crate::planner::ScanPlan::PkRangeScan {
            ref start_key,
            ref range_conds,
            ..
        } = plan
        {
            let range_conds = range_conds.clone();
            let mut partial_row = vec![Value::Null; table_schema.columns.len()];
            let pk_idx_cache = table_schema.pk_indices().to_vec();
            let mut patch_buf: Vec<u8> = Vec::with_capacity(256);

            let count =
                wtx.table_update_range(lower_name.as_bytes(), start_key, |key, value| {
                    // Range check: None = stop, Some(false) = skip, fall through = in range
                    if single_int_pk {
                        let pk_int = Value::Integer(decode_pk_integer(key)?);
                        for (op, bound) in &range_conds {
                            match op {
                                BinOp::Lt if &pk_int >= bound => return Ok(None),
                                BinOp::LtEq if &pk_int > bound => return Ok(None),
                                BinOp::Gt if &pk_int <= bound => return Ok(Some(false)),
                                BinOp::GtEq if &pk_int < bound => return Ok(Some(false)),
                                _ => {}
                            }
                        }
                    } else {
                        let pk_vals = decode_composite_key(key, num_pk_cols)?;
                        for (op, bound) in &range_conds {
                            match op {
                                BinOp::Lt if &pk_vals[0] >= bound => return Ok(None),
                                BinOp::LtEq if &pk_vals[0] > bound => return Ok(None),
                                BinOp::Gt if &pk_vals[0] <= bound => return Ok(Some(false)),
                                BinOp::GtEq if &pk_vals[0] < bound => return Ok(Some(false)),
                                _ => {}
                            }
                        }
                    }

                    if single_int_pk {
                        partial_row[pk_idx_cache[0]] = Value::Integer(decode_pk_integer(key)?);
                    } else {
                        let pk_vals = decode_composite_key(key, num_pk_cols)?;
                        for (i, &pi) in pk_idx_cache.iter().enumerate() {
                            partial_row[pi] = pk_vals[i].clone();
                        }
                    }
                    for target in &targets {
                        partial_row[target.schema_idx] =
                            decode_column_raw(value, target.phys_idx)?.to_value();
                    }
                    // Eval + patch directly in the leaf cell's value bytes
                    for target in &targets {
                        let new_val = eval_expr(&target.expr, &col_map, &partial_row)?;
                        let coerced = if new_val.is_null() {
                            if !target.col.nullable {
                                return Err(SqlError::NotNullViolation(target.col.name.clone()));
                            }
                            Value::Null
                        } else {
                            let got_type = new_val.data_type();
                            new_val.coerce_into(target.col.data_type).ok_or_else(|| {
                                SqlError::TypeMismatch {
                                    expected: target.col.data_type.to_string(),
                                    got: got_type.to_string(),
                                }
                            })?
                        };
                        if !patch_column_in_place(value, target.phys_idx, &coerced)? {
                            patch_row_column(value, target.phys_idx, &coerced, &mut patch_buf)?;
                            value[..patch_buf.len()].copy_from_slice(&patch_buf);
                        }
                    }
                    Ok(Some(true))
                })?;

            wtx.commit().map_err(SqlError::Storage)?;
            return Ok(ExecutionResult::RowsAffected(count));
        }

        // Collect-then-write path for PkLookup and SeqScan
        let mut kv_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        {
            match &plan {
                crate::planner::ScanPlan::PkLookup { pk_values } => {
                    let key = crate::encoding::encode_composite_key(pk_values);
                    if let Some(value) = wtx
                        .table_get(lower_name.as_bytes(), &key)
                        .map_err(SqlError::Storage)?
                    {
                        kv_pairs.push((key, value));
                    }
                }
                _ => {
                    wtx.table_for_each(lower_name.as_bytes(), |key, value| {
                        kv_pairs.push((key.to_vec(), value.to_vec()));
                        Ok(())
                    })
                    .map_err(SqlError::Storage)?;
                }
            }
        }

        let mut patch_buf: Vec<u8> = Vec::with_capacity(256);
        let mut partial_row = vec![Value::Null; table_schema.columns.len()];
        let pk_idx_cache = table_schema.pk_indices().to_vec();
        let mut patched: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(kv_pairs.len());

        for (key, raw_value) in &mut kv_pairs {
            if matches!(plan, crate::planner::ScanPlan::SeqScan) {
                if let Some(ref w) = stmt.where_clause {
                    let row = decode_full_row(table_schema, key, raw_value)?;
                    if !eval_expr(w, &col_map, &row).is_ok_and(|v| is_truthy(&v)) {
                        continue;
                    }
                }
            }
            if single_int_pk {
                partial_row[pk_idx_cache[0]] = Value::Integer(decode_pk_integer(key)?);
            } else {
                let pk_vals = decode_composite_key(key, num_pk_cols)?;
                for (i, &pi) in pk_idx_cache.iter().enumerate() {
                    partial_row[pi] = pk_vals[i].clone();
                }
            }
            for target in &targets {
                partial_row[target.schema_idx] =
                    decode_column_raw(raw_value, target.phys_idx)?.to_value();
            }
            for target in &targets {
                let new_val = eval_expr(&target.expr, &col_map, &partial_row)?;
                let coerced = if new_val.is_null() {
                    if !target.col.nullable {
                        return Err(SqlError::NotNullViolation(target.col.name.clone()));
                    }
                    Value::Null
                } else {
                    let got_type = new_val.data_type();
                    new_val.coerce_into(target.col.data_type).ok_or_else(|| {
                        SqlError::TypeMismatch {
                            expected: target.col.data_type.to_string(),
                            got: got_type.to_string(),
                        }
                    })?
                };
                if !patch_column_in_place(raw_value, target.phys_idx, &coerced)? {
                    patch_row_column(raw_value, target.phys_idx, &coerced, &mut patch_buf)?;
                    std::mem::swap(raw_value, &mut patch_buf);
                }
            }
            patched.push((std::mem::take(key), std::mem::take(raw_value)));
        }

        if !patched.is_empty() {
            let refs: Vec<(&[u8], &[u8])> = patched
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            wtx.table_update_sorted(lower_name.as_bytes(), &refs)
                .map_err(SqlError::Storage)?;
        }
        let count = patched.len() as u64;
        wtx.commit().map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::RowsAffected(count));
    }

    // Slow path: has FK/indices/PK changes — materialize all changes for validation
    let all_candidates = collect_keyed_rows_read(db, table_schema, &stmt.where_clause)?;
    let matching_rows: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => eval_expr(where_expr, &col_map, row).is_ok_and(|v| is_truthy(&v)),
            None => true,
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

    let mut changes: Vec<UpdateChange> = Vec::new();

    for (old_key, row) in &matching_rows {
        let mut new_row = row.clone();

        let mut evaluated: Vec<(usize, Value)> = Vec::with_capacity(stmt.assignments.len());
        for (col_name, expr) in &stmt.assignments {
            let col_idx = table_schema
                .column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let new_val = eval_expr(expr, &col_map, row)?;
            let col = &table_schema.columns[col_idx];

            let got_type = new_val.data_type();
            let coerced = if new_val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
                Value::Null
            } else {
                new_val
                    .coerce_into(col.data_type)
                    .ok_or_else(|| SqlError::TypeMismatch {
                        expected: col.data_type.to_string(),
                        got: got_type.to_string(),
                    })?
            };

            evaluated.push((col_idx, coerced));
        }

        for (col_idx, coerced) in evaluated {
            new_row[col_idx] = coerced;
        }

        if table_schema.has_checks() {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(check, &col_map, &new_row)?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, &col_map, &new_row)?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| new_row[i].clone()).collect();
        let new_key = encode_composite_key(&pk_values);

        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let phys_count = table_schema.physical_non_pk_count();
        let mut value_values = vec![Value::Null; phys_count];
        for (j, &i) in non_pk.iter().enumerate() {
            value_values[enc_pos[j] as usize] = new_row[i].clone();
        }
        let new_value = encode_row(&value_values);

        changes.push(UpdateChange {
            old_key: old_key.clone(),
            new_key,
            new_value,
            pk_changed: pk_changed_by_set,
            old_row: row.clone(),
            new_row,
        });
    }

    {
        use std::collections::HashSet;
        let mut new_keys: HashSet<Vec<u8>> = HashSet::new();
        for c in &changes {
            if c.pk_changed && c.new_key != c.old_key && !new_keys.insert(c.new_key.clone()) {
                return Err(SqlError::DuplicateKey);
            }
        }
    }

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;

    // FK child-side: validate new FK values exist in parent
    if !table_schema.foreign_keys.is_empty() {
        for c in &changes {
            for fk in &table_schema.foreign_keys {
                let fk_changed = fk
                    .columns
                    .iter()
                    .any(|&ci| c.old_row[ci as usize] != c.new_row[ci as usize]);
                if !fk_changed {
                    continue;
                }
                let any_null = fk
                    .columns
                    .iter()
                    .any(|&ci| c.new_row[ci as usize].is_null());
                if any_null {
                    continue;
                }
                let fk_vals: Vec<Value> = fk
                    .columns
                    .iter()
                    .map(|&ci| c.new_row[ci as usize].clone())
                    .collect();
                let fk_key = encode_composite_key(&fk_vals);
                let found = wtx
                    .table_get(fk.foreign_table.as_bytes(), &fk_key)
                    .map_err(SqlError::Storage)?;
                if found.is_none() {
                    let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                    return Err(SqlError::ForeignKeyViolation(name.to_string()));
                }
            }
        }
    }

    // FK parent-side: if PK changed, check no child references old PK
    let child_fks = schema.child_fks_for(&lower_name);
    if !child_fks.is_empty() {
        for c in &changes {
            if !c.pk_changed {
                continue;
            }
            let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();
            let old_pk_key = encode_composite_key(&old_pk);
            for &(child_table, fk) in &child_fks {
                let child_schema = schema.get(child_table).unwrap();
                let fk_idx = child_schema
                    .indices
                    .iter()
                    .find(|idx| idx.columns == fk.columns);
                if let Some(idx) = fk_idx {
                    let idx_table = TableSchema::index_table_name(child_table, &idx.name);
                    let mut has_child = false;
                    wtx.table_scan_from(&idx_table, &old_pk_key, |key, _| {
                        if key.starts_with(&old_pk_key) {
                            has_child = true;
                            Ok(false) // stop scanning
                        } else {
                            Ok(false) // past prefix, stop
                        }
                    })
                    .map_err(SqlError::Storage)?;
                    if has_child {
                        return Err(SqlError::ForeignKeyViolation(format!(
                            "cannot update PK in '{}': referenced by '{}'",
                            lower_name, child_table
                        )));
                    }
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
                wtx.table_delete(&idx_table, &old_idx_key)
                    .map_err(SqlError::Storage)?;
            }
        }

        if c.pk_changed {
            wtx.table_delete(lower_name.as_bytes(), &c.old_key)
                .map_err(SqlError::Storage)?;
        }
    }

    for c in &changes {
        let new_pk: Vec<Value> = pk_indices.iter().map(|&i| c.new_row[i].clone()).collect();

        if c.pk_changed {
            let is_new = wtx
                .table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
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
                let is_new = wtx
                    .table_insert(&idx_table, &new_idx_key, &new_idx_val)
                    .map_err(SqlError::Storage)?;
                if idx.unique && !is_new {
                    let indexed_values: Vec<Value> = idx
                        .columns
                        .iter()
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

pub(super) fn exec_delete(
    db: &Database,
    schema: &SchemaManager,
    stmt: &DeleteStmt,
) -> Result<ExecutionResult> {
    let lower_name = stmt.table.to_ascii_lowercase();
    if schema.get_view(&lower_name).is_some() {
        return Err(SqlError::CannotModifyView(stmt.table.clone()));
    }
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let corr_ctx = CorrelationCtx {
        outer_schema: table_schema,
        outer_alias: None,
    };
    if has_correlated_where(&stmt.where_clause, &corr_ctx, schema) {
        let select_stmt = SelectStmt {
            columns: vec![SelectColumn::AllColumns],
            from: stmt.table.clone(),
            from_alias: None,
            joins: vec![],
            distinct: false,
            where_clause: stmt.where_clause.clone(),
            order_by: vec![],
            limit: None,
            offset: None,
            group_by: vec![],
            having: None,
        };
        let (mut rows, _) = collect_rows_read(db, table_schema, &None, None)?;
        let remaining =
            handle_correlated_where_read(db, schema, &select_stmt, &corr_ctx, &mut rows)?;

        if let Some(ref w) = remaining {
            let col_map = ColumnMap::new(&table_schema.columns);
            rows.retain(|row| match eval_expr(w, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            });
        }

        let pk_indices = table_schema.pk_indices();
        let pk_values: Vec<Value> = rows.iter().map(|row| row[pk_indices[0]].clone()).collect();
        let pk_col = &table_schema.columns[pk_indices[0]].name;
        let in_set: std::collections::HashSet<Value> = pk_values.into_iter().collect();
        let new_where = if in_set.is_empty() {
            Some(Expr::Literal(Value::Boolean(false)))
        } else {
            Some(Expr::InSet {
                expr: Box::new(Expr::Column(pk_col.clone())),
                values: in_set,
                has_null: false,
                negated: false,
            })
        };

        let rewritten = DeleteStmt {
            table: stmt.table.clone(),
            where_clause: new_where,
        };
        return exec_delete(db, schema, &rewritten);
    }

    let materialized;
    let stmt = if delete_has_subquery(stmt) {
        materialized = materialize_delete(stmt, &mut |sub| {
            exec_subquery_read(db, schema, sub, &HashMap::new())
        })?;
        &materialized
    } else {
        stmt
    };

    let col_map = ColumnMap::new(&table_schema.columns);
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let all_candidates = collect_keyed_rows_write(&mut wtx, table_schema, &stmt.where_clause)?;
    let rows_to_delete: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => match eval_expr(where_expr, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            },
            None => true,
        })
        .collect();

    if rows_to_delete.is_empty() {
        wtx.commit().map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::RowsAffected(0));
    }

    let pk_indices = table_schema.pk_indices();

    // FK parent-side: check no child rows reference deleted PKs
    let child_fks = schema.child_fks_for(&lower_name);
    if !child_fks.is_empty() {
        for (_key, row) in &rows_to_delete {
            let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
            let pk_key = encode_composite_key(&pk_values);
            for &(child_table, fk) in &child_fks {
                let child_schema = schema.get(child_table).unwrap();
                let fk_idx = child_schema
                    .indices
                    .iter()
                    .find(|idx| idx.columns == fk.columns);
                if let Some(idx) = fk_idx {
                    let idx_table = TableSchema::index_table_name(child_table, &idx.name);
                    let mut has_child = false;
                    wtx.table_scan_from(&idx_table, &pk_key, |key, _| {
                        if key.starts_with(&pk_key) {
                            has_child = true;
                            Ok(false)
                        } else {
                            Ok(false)
                        }
                    })
                    .map_err(SqlError::Storage)?;
                    if has_child {
                        return Err(SqlError::ForeignKeyViolation(format!(
                            "cannot delete from '{}': referenced by '{}'",
                            lower_name, child_table
                        )));
                    }
                }
            }
        }
    }

    for (key, row) in &rows_to_delete {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        delete_index_entries(&mut wtx, table_schema, row, &pk_values)?;
        wtx.table_delete(lower_name.as_bytes(), key)
            .map_err(SqlError::Storage)?;
    }
    let count = rows_to_delete.len() as u64;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

pub(super) fn exec_select_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    if stmt.from.is_empty() {
        let materialized;
        let stmt = if stmt_has_subquery(stmt) {
            materialized =
                materialize_stmt(stmt, &mut |sub| exec_subquery_write(wtx, schema, sub, ctes))?;
            &materialized
        } else {
            stmt
        };
        return super::exec_select_no_from(stmt);
    }

    let lower_name = stmt.from.to_ascii_lowercase();

    if let Some(cte_result) = ctes.get(&lower_name) {
        if stmt.joins.is_empty() {
            return super::exec_select_from_cte(cte_result, stmt, &mut |sub| {
                exec_subquery_write(wtx, schema, sub, ctes)
            });
        } else {
            return super::exec_select_join_with_ctes(stmt, ctes, &mut |name| {
                super::scan_table_write(wtx, schema, name)
            });
        }
    }

    if !ctes.is_empty()
        && stmt
            .joins
            .iter()
            .any(|j| ctes.contains_key(&j.table.name.to_ascii_lowercase()))
    {
        return super::exec_select_join_with_ctes(stmt, ctes, &mut |name| {
            super::scan_table_write_or_view(wtx, schema, name)
        });
    }

    // ── View resolution (in-txn) ────────────────────────────────────
    if let Some(view_def) = schema.get_view(&lower_name) {
        if let Some(fused) = try_fuse_view(stmt, schema, view_def)? {
            return super::exec_select_in_txn(wtx, schema, &fused, ctes);
        }
        let view_qr = exec_view_write(wtx, schema, view_def)?;
        if stmt.joins.is_empty() {
            return super::exec_select_from_cte(&view_qr, stmt, &mut |sub| {
                exec_subquery_write(wtx, schema, sub, ctes)
            });
        } else {
            let mut view_ctes = ctes.clone();
            view_ctes.insert(lower_name.clone(), view_qr);
            return super::exec_select_join_with_ctes(stmt, &view_ctes, &mut |name| {
                super::scan_table_write_or_view(wtx, schema, name)
            });
        }
    }

    let any_join_view = stmt.joins.iter().any(|j| {
        schema
            .get_view(&j.table.name.to_ascii_lowercase())
            .is_some()
    });
    if any_join_view {
        let mut view_ctes = ctes.clone();
        for j in &stmt.joins {
            let jname = j.table.name.to_ascii_lowercase();
            if let Some(vd) = schema.get_view(&jname) {
                if let std::collections::hash_map::Entry::Vacant(e) = view_ctes.entry(jname) {
                    let vqr = exec_view_write(wtx, schema, vd)?;
                    e.insert(vqr);
                }
            }
        }
        return super::exec_select_join_with_ctes(stmt, &view_ctes, &mut |name| {
            super::scan_table_write(wtx, schema, name)
        });
    }

    if !stmt.joins.is_empty() {
        return super::exec_select_join_in_txn(wtx, schema, stmt);
    }

    let lower_name = stmt.from.to_ascii_lowercase();
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;

    // Correlated subquery handling (in-txn)
    let corr_ctx = CorrelationCtx {
        outer_schema: table_schema,
        outer_alias: stmt.from_alias.as_deref(),
    };
    if has_correlated_where(&stmt.where_clause, &corr_ctx, schema) {
        let (mut rows, _) = collect_rows_write(wtx, table_schema, &None, None)?;
        let remaining_where =
            handle_correlated_where_write(wtx, schema, stmt, &corr_ctx, &mut rows)?;
        let clean_stmt = SelectStmt {
            where_clause: remaining_where,
            columns: stmt.columns.clone(),
            from: stmt.from.clone(),
            from_alias: stmt.from_alias.clone(),
            joins: stmt.joins.clone(),
            distinct: stmt.distinct,
            order_by: stmt.order_by.clone(),
            limit: stmt.limit.clone(),
            offset: stmt.offset.clone(),
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
        };
        let final_stmt;
        let s = if stmt_has_subquery(&clean_stmt) {
            final_stmt = materialize_stmt(&clean_stmt, &mut |sub| {
                exec_subquery_write(wtx, schema, sub, ctes)
            })?;
            &final_stmt
        } else {
            &clean_stmt
        };
        return super::process_select(&table_schema.columns, rows, s, false);
    }

    let materialized;
    let stmt = if stmt_has_subquery(stmt) {
        materialized =
            materialize_stmt(stmt, &mut |sub| exec_subquery_write(wtx, schema, sub, ctes))?;
        &materialized
    } else {
        stmt
    };

    if let Some(result) = try_count_star_shortcut(stmt, || {
        wtx.table_entry_count(lower_name.as_bytes())
            .map_err(SqlError::Storage)
    })? {
        return Ok(result);
    }

    if let Some(plan) = StreamAggPlan::try_new(stmt, table_schema)? {
        let mut states: Vec<AggState> = plan.ops.iter().map(|(op, _)| AggState::new(op)).collect();
        let mut scan_err: Option<SqlError> = None;
        if stmt.where_clause.is_none() {
            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                Ok(plan.feed_row_raw(key, value, &mut states, &mut scan_err))
            })
            .map_err(SqlError::Storage)?;
        } else {
            let col_map = ColumnMap::new(&table_schema.columns);
            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                Ok(plan.feed_row(
                    key,
                    value,
                    table_schema,
                    &col_map,
                    &stmt.where_clause,
                    &mut states,
                    &mut scan_err,
                ))
            })
            .map_err(SqlError::Storage)?;
        }
        if let Some(e) = scan_err {
            return Err(e);
        }
        return Ok(plan.finish(states));
    }

    if let Some(plan) = StreamGroupByPlan::try_new(stmt, table_schema)? {
        let lower = lower_name.clone();
        return plan.execute_scan(|cb| {
            wtx.table_scan_from(lower.as_bytes(), b"", |key, value| Ok(cb(key, value)))
        });
    }

    if let Some(plan) = TopKScanPlan::try_new(stmt, table_schema)? {
        let lower = lower_name.clone();
        return plan.execute_scan(table_schema, stmt, |cb| {
            wtx.table_scan_from(lower.as_bytes(), b"", |key, value| Ok(cb(key, value)))
        });
    }

    let scan_limit = compute_scan_limit(stmt);
    let (rows, predicate_applied) =
        collect_rows_write(wtx, table_schema, &stmt.where_clause, scan_limit)?;
    super::process_select(&table_schema.columns, rows, stmt, predicate_applied)
}

pub(super) fn exec_update_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if update_has_subquery(stmt) {
        materialized = materialize_update(stmt, &mut |sub| {
            exec_subquery_write(wtx, schema, sub, &HashMap::new())
        })?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let col_map = ColumnMap::new(&table_schema.columns);
    let all_candidates = collect_keyed_rows_write(wtx, table_schema, &stmt.where_clause)?;
    let matching_rows: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => match eval_expr(where_expr, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            },
            None => true,
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

        // Evaluate all SET expressions against the original row (SQL standard).
        let mut evaluated: Vec<(usize, Value)> = Vec::with_capacity(stmt.assignments.len());
        for (col_name, expr) in &stmt.assignments {
            let col_idx = table_schema
                .column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let new_val = eval_expr(expr, &col_map, row)?;
            let col = &table_schema.columns[col_idx];

            let got_type = new_val.data_type();
            let coerced = if new_val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
                Value::Null
            } else {
                new_val
                    .coerce_into(col.data_type)
                    .ok_or_else(|| SqlError::TypeMismatch {
                        expected: col.data_type.to_string(),
                        got: got_type.to_string(),
                    })?
            };

            evaluated.push((col_idx, coerced));
        }

        for (col_idx, coerced) in evaluated {
            if table_schema.primary_key_columns.contains(&(col_idx as u16)) {
                pk_changed = true;
            }
            new_row[col_idx] = coerced;
        }

        if table_schema.has_checks() {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(check, &col_map, &new_row)?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, &col_map, &new_row)?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| new_row[i].clone()).collect();
        let new_key = encode_composite_key(&pk_values);

        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let phys_count = table_schema.physical_non_pk_count();
        let mut value_values = vec![Value::Null; phys_count];
        for (j, &i) in non_pk.iter().enumerate() {
            value_values[enc_pos[j] as usize] = new_row[i].clone();
        }
        let new_value = encode_row(&value_values);

        changes.push(UpdateChange {
            old_key: old_key.clone(),
            new_key,
            new_value,
            pk_changed,
            old_row: row.clone(),
            new_row,
        });
    }

    {
        use std::collections::HashSet;
        let mut new_keys: HashSet<Vec<u8>> = HashSet::new();
        for c in &changes {
            if c.pk_changed && c.new_key != c.old_key && !new_keys.insert(c.new_key.clone()) {
                return Err(SqlError::DuplicateKey);
            }
        }
    }

    // FK child-side: validate new FK values exist in parent
    if !table_schema.foreign_keys.is_empty() {
        for c in &changes {
            for fk in &table_schema.foreign_keys {
                let fk_changed = fk
                    .columns
                    .iter()
                    .any(|&ci| c.old_row[ci as usize] != c.new_row[ci as usize]);
                if !fk_changed {
                    continue;
                }
                let any_null = fk
                    .columns
                    .iter()
                    .any(|&ci| c.new_row[ci as usize].is_null());
                if any_null {
                    continue;
                }
                let fk_vals: Vec<Value> = fk
                    .columns
                    .iter()
                    .map(|&ci| c.new_row[ci as usize].clone())
                    .collect();
                let fk_key = encode_composite_key(&fk_vals);
                let found = wtx
                    .table_get(fk.foreign_table.as_bytes(), &fk_key)
                    .map_err(SqlError::Storage)?;
                if found.is_none() {
                    let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                    return Err(SqlError::ForeignKeyViolation(name.to_string()));
                }
            }
        }
    }

    // FK parent-side: if PK changed, check no child references old PK
    let child_fks = schema.child_fks_for(&lower_name);
    if !child_fks.is_empty() {
        for c in &changes {
            if !c.pk_changed {
                continue;
            }
            let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();
            let old_pk_key = encode_composite_key(&old_pk);
            for &(child_table, fk) in &child_fks {
                let child_schema = schema.get(child_table).unwrap();
                let fk_idx = child_schema
                    .indices
                    .iter()
                    .find(|idx| idx.columns == fk.columns);
                if let Some(idx) = fk_idx {
                    let idx_table = TableSchema::index_table_name(child_table, &idx.name);
                    let mut has_child = false;
                    wtx.table_scan_from(&idx_table, &old_pk_key, |key, _| {
                        if key.starts_with(&old_pk_key) {
                            has_child = true;
                            Ok(false)
                        } else {
                            Ok(false)
                        }
                    })
                    .map_err(SqlError::Storage)?;
                    if has_child {
                        return Err(SqlError::ForeignKeyViolation(format!(
                            "cannot update PK in '{}': referenced by '{}'",
                            lower_name, child_table
                        )));
                    }
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
                wtx.table_delete(&idx_table, &old_idx_key)
                    .map_err(SqlError::Storage)?;
            }
        }

        if c.pk_changed {
            wtx.table_delete(lower_name.as_bytes(), &c.old_key)
                .map_err(SqlError::Storage)?;
        }
    }

    for c in &changes {
        let new_pk: Vec<Value> = pk_indices.iter().map(|&i| c.new_row[i].clone()).collect();

        if c.pk_changed {
            let is_new = wtx
                .table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
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
                let is_new = wtx
                    .table_insert(&idx_table, &new_idx_key, &new_idx_val)
                    .map_err(SqlError::Storage)?;
                if idx.unique && !is_new {
                    let indexed_values: Vec<Value> = idx
                        .columns
                        .iter()
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

pub(super) fn exec_delete_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &DeleteStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if delete_has_subquery(stmt) {
        materialized = materialize_delete(stmt, &mut |sub| {
            exec_subquery_write(wtx, schema, sub, &HashMap::new())
        })?;
        &materialized
    } else {
        stmt
    };

    let lower_name = stmt.table.to_ascii_lowercase();
    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let col_map = ColumnMap::new(&table_schema.columns);
    let all_candidates = collect_keyed_rows_write(wtx, table_schema, &stmt.where_clause)?;
    let rows_to_delete: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => match eval_expr(where_expr, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            },
            None => true,
        })
        .collect();

    if rows_to_delete.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    let pk_indices = table_schema.pk_indices();

    // FK parent-side: check no child rows reference deleted PKs
    let child_fks = schema.child_fks_for(&lower_name);
    if !child_fks.is_empty() {
        for (_key, row) in &rows_to_delete {
            let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
            let pk_key = encode_composite_key(&pk_values);
            for &(child_table, fk) in &child_fks {
                let child_schema = schema.get(child_table).unwrap();
                let fk_idx = child_schema
                    .indices
                    .iter()
                    .find(|idx| idx.columns == fk.columns);
                if let Some(idx) = fk_idx {
                    let idx_table = TableSchema::index_table_name(child_table, &idx.name);
                    let mut has_child = false;
                    wtx.table_scan_from(&idx_table, &pk_key, |key, _| {
                        if key.starts_with(&pk_key) {
                            has_child = true;
                            Ok(false)
                        } else {
                            Ok(false)
                        }
                    })
                    .map_err(SqlError::Storage)?;
                    if has_child {
                        return Err(SqlError::ForeignKeyViolation(format!(
                            "cannot delete from '{}': referenced by '{}'",
                            lower_name, child_table
                        )));
                    }
                }
            }
        }
    }

    for (key, row) in &rows_to_delete {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        delete_index_entries(wtx, table_schema, row, &pk_values)?;
        wtx.table_delete(lower_name.as_bytes(), key)
            .map_err(SqlError::Storage)?;
    }
    let count = rows_to_delete.len() as u64;
    Ok(ExecutionResult::RowsAffected(count))
}
