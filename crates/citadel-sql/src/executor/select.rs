use std::collections::HashMap;

use citadel::Database;

use crate::encoding::{
    decode_column_raw, decode_column_with_offset, decode_composite_key, decode_pk_integer,
    row_non_pk_count, RawColumn,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, referenced_columns, ColumnMap};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::aggregate::*;
use super::correlated::*;
use super::cte::*;
use super::dml::*;
use super::helpers::*;
use super::scan::*;
use super::view::*;
use super::window::*;
use super::CteContext;

// ── SELECT execution ────────────────────────────────────────────────

pub(super) fn exec_select(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    if stmt.from.is_empty() {
        let materialized;
        let stmt = if stmt_has_subquery(stmt) {
            materialized =
                materialize_stmt(stmt, &mut |sub| exec_subquery_read(db, schema, sub, ctes))?;
            &materialized
        } else {
            stmt
        };
        return exec_select_no_from(stmt);
    }

    let lower_name = stmt.from.to_ascii_lowercase();

    if let Some(cte_result) = ctes.get(&lower_name) {
        if stmt.joins.is_empty() {
            return exec_select_from_cte(cte_result, stmt, &mut |sub| {
                exec_subquery_read(db, schema, sub, ctes)
            });
        } else {
            return super::exec_select_join_with_ctes(stmt, ctes, &mut |name| {
                super::scan_table_read(db, schema, name)
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
            super::scan_table_read_or_view(db, schema, name)
        });
    }

    // ── View resolution ─────────────────────────────────────────────
    if let Some(view_def) = schema.get_view(&lower_name) {
        if let Some(fused) = try_fuse_view(stmt, schema, view_def)? {
            return exec_select(db, schema, &fused, ctes);
        }
        let view_qr = exec_view_read(db, schema, view_def)?;
        if stmt.joins.is_empty() {
            // Check for correlated subqueries on view result
            let view_schema = build_view_schema(&lower_name, &view_qr);
            let view_ctx = CorrelationCtx {
                outer_schema: &view_schema,
                outer_alias: stmt.from_alias.as_deref(),
            };
            if has_correlated_where(&stmt.where_clause, &view_ctx, schema) {
                let mut rows = view_qr.rows.clone();
                let remaining =
                    handle_correlated_where_read(db, schema, stmt, &view_ctx, &mut rows)?;
                let clean_stmt = SelectStmt {
                    where_clause: remaining,
                    columns: stmt.columns.clone(),
                    from: stmt.from.clone(),
                    from_alias: stmt.from_alias.clone(),
                    joins: vec![],
                    distinct: stmt.distinct,
                    order_by: stmt.order_by.clone(),
                    limit: stmt.limit.clone(),
                    offset: stmt.offset.clone(),
                    group_by: stmt.group_by.clone(),
                    having: stmt.having.clone(),
                };
                return process_select(&view_schema.columns, rows, &clean_stmt, false);
            }
            return exec_select_from_cte(&view_qr, stmt, &mut |sub| {
                exec_subquery_read(db, schema, sub, ctes)
            });
        } else {
            let mut view_ctes = ctes.clone();
            view_ctes.insert(lower_name.clone(), view_qr);
            return super::exec_select_join_with_ctes(stmt, &view_ctes, &mut |name| {
                super::scan_table_read_or_view(db, schema, name)
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
                    let vqr = exec_view_read(db, schema, vd)?;
                    e.insert(vqr);
                }
            }
        }
        return super::exec_select_join_with_ctes(stmt, &view_ctes, &mut |name| {
            super::scan_table_read(db, schema, name)
        });
    }

    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;

    // Correlated subquery handling: decorrelate before materialization
    let corr_ctx = CorrelationCtx {
        outer_schema: table_schema,
        outer_alias: stmt.from_alias.as_deref(),
    };
    if has_correlated_where(&stmt.where_clause, &corr_ctx, schema) {
        // Phase 1: decorrelation maps (inner only), Phase 2: scan outer with probing
        let (mut rows, remaining_where) =
            build_and_scan_correlated_read(db, schema, stmt, table_schema, &corr_ctx)?;
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
        // Handle correlated scalar in SELECT
        let mut ext_cols = table_schema.columns.clone();
        let clean_stmt = handle_correlated_select_read(
            db,
            schema,
            &clean_stmt,
            &corr_ctx,
            &mut rows,
            &mut ext_cols,
        )?;

        let final_stmt;
        let s = if stmt_has_subquery(&clean_stmt) {
            final_stmt = materialize_stmt(&clean_stmt, &mut |sub| {
                exec_subquery_read(db, schema, sub, ctes)
            })?;
            &final_stmt
        } else {
            &clean_stmt
        };
        return process_select(&ext_cols, rows, s, false);
    }

    // Check for correlated scalar in SELECT (no correlated WHERE)
    if has_correlated_select(&stmt.columns, &corr_ctx, schema) {
        let (mut rows, _) = collect_rows_read(db, table_schema, &stmt.where_clause, None)?;
        let mut ext_cols = table_schema.columns.clone();
        let clean_stmt =
            handle_correlated_select_read(db, schema, stmt, &corr_ctx, &mut rows, &mut ext_cols)?;
        let final_stmt;
        let s = if stmt_has_subquery(&clean_stmt) {
            final_stmt = materialize_stmt(&clean_stmt, &mut |sub| {
                exec_subquery_read(db, schema, sub, ctes)
            })?;
            &final_stmt
        } else {
            &clean_stmt
        };
        return process_select(&ext_cols, rows, s, true);
    }

    let materialized;
    let stmt = if stmt_has_subquery(stmt) {
        materialized =
            materialize_stmt(stmt, &mut |sub| exec_subquery_read(db, schema, sub, ctes))?;
        &materialized
    } else {
        stmt
    };

    if !stmt.joins.is_empty() {
        return super::exec_select_join(db, schema, stmt);
    }

    if let Some(result) = try_count_star_shortcut(stmt, || {
        let mut rtx = db.begin_read();
        rtx.table_entry_count(lower_name.as_bytes())
            .map_err(SqlError::Storage)
    })? {
        return Ok(result);
    }

    if let Some(plan) = StreamAggPlan::try_new(stmt, table_schema)? {
        let mut states: Vec<AggState> = plan.ops.iter().map(|(op, _)| AggState::new(op)).collect();
        let mut scan_err: Option<SqlError> = None;
        let mut rtx = db.begin_read();
        if stmt.where_clause.is_none() {
            rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
                plan.feed_row_raw(key, value, &mut states, &mut scan_err)
            })
            .map_err(SqlError::Storage)?;
        } else {
            let col_map = ColumnMap::new(&table_schema.columns);
            rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
                plan.feed_row(
                    key,
                    value,
                    table_schema,
                    &col_map,
                    &stmt.where_clause,
                    &mut states,
                    &mut scan_err,
                )
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
        let mut rtx = db.begin_read();
        return plan
            .execute_scan(|cb| rtx.table_scan_raw(lower.as_bytes(), |key, value| cb(key, value)));
    }

    if let Some(plan) = TopKScanPlan::try_new(stmt, table_schema)? {
        let lower = lower_name.clone();
        let mut rtx = db.begin_read();
        return plan.execute_scan(table_schema, stmt, |cb| {
            rtx.table_scan_raw(lower.as_bytes(), |key, value| cb(key, value))
        });
    }

    if let Some(result) = try_streaming_distinct(stmt, table_schema, db)? {
        return Ok(result);
    }

    let scan_limit = compute_scan_limit(stmt);
    let (rows, predicate_applied) =
        collect_rows_read(db, table_schema, &stmt.where_clause, scan_limit)?;
    process_select(&table_schema.columns, rows, stmt, predicate_applied)
}

pub(super) fn compute_scan_limit(stmt: &SelectStmt) -> Option<usize> {
    if !stmt.order_by.is_empty()
        || !stmt.group_by.is_empty()
        || stmt.distinct
        || stmt.having.is_some()
    {
        return None;
    }
    if has_any_window_function(stmt) {
        return None;
    }
    let has_aggregates = stmt.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    });
    if has_aggregates {
        return None;
    }
    let limit = stmt.limit.as_ref()?;
    let limit_val = eval_const_int(limit).ok()?.max(0) as usize;
    let offset_val = stmt
        .offset
        .as_ref()
        .and_then(|e| eval_const_int(e).ok())
        .unwrap_or(0)
        .max(0) as usize;
    Some(limit_val.saturating_add(offset_val))
}

pub(super) fn try_count_star_shortcut(
    stmt: &SelectStmt,
    get_count: impl FnOnce() -> Result<u64>,
) -> Result<Option<ExecutionResult>> {
    if stmt.columns.len() != 1
        || stmt.where_clause.is_some()
        || !stmt.group_by.is_empty()
        || stmt.having.is_some()
    {
        return Ok(None);
    }
    let col = match &stmt.columns[0] {
        SelectColumn::Expr { expr, alias } => (expr, alias),
        _ => return Ok(None),
    };
    if !matches!(col.0, Expr::CountStar) {
        return Ok(None);
    }
    let count = get_count()? as i64;
    let col_name = col.1.as_deref().unwrap_or("COUNT(*)").to_string();
    Ok(Some(ExecutionResult::Query(QueryResult {
        columns: vec![col_name],
        rows: vec![vec![Value::Integer(count)]],
    })))
}

pub(super) enum StreamAgg {
    CountStar,
    Count(usize),
    Sum(usize),
    Avg(usize),
    Min(usize),
    Max(usize),
}

pub(super) enum RawAggTarget {
    CountStar,
    Pk(usize),
    NonPk(usize),
}

pub(super) enum AggState {
    CountStar(i64),
    Count(i64),
    Sum {
        int_sum: i64,
        real_sum: f64,
        has_real: bool,
        all_null: bool,
    },
    Avg {
        sum: f64,
        count: i64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl AggState {
    pub(super) fn new(op: &StreamAgg) -> Self {
        match op {
            StreamAgg::CountStar => AggState::CountStar(0),
            StreamAgg::Count(_) => AggState::Count(0),
            StreamAgg::Sum(_) => AggState::Sum {
                int_sum: 0,
                real_sum: 0.0,
                has_real: false,
                all_null: true,
            },
            StreamAgg::Avg(_) => AggState::Avg { sum: 0.0, count: 0 },
            StreamAgg::Min(_) => AggState::Min(None),
            StreamAgg::Max(_) => AggState::Max(None),
        }
    }

    pub(super) fn feed_val(&mut self, val: &Value) -> Result<()> {
        match self {
            AggState::CountStar(c) => {
                *c += 1;
            }
            AggState::Count(c) => {
                if !val.is_null() {
                    *c += 1;
                }
            }
            AggState::Sum {
                int_sum,
                real_sum,
                has_real,
                all_null,
            } => match val {
                Value::Integer(i) => {
                    *int_sum += i;
                    *all_null = false;
                }
                Value::Real(r) => {
                    *real_sum += r;
                    *has_real = true;
                    *all_null = false;
                }
                Value::Null => {}
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric".into(),
                        got: val.data_type().to_string(),
                    })
                }
            },
            AggState::Avg { sum, count } => match val {
                Value::Integer(i) => {
                    *sum += *i as f64;
                    *count += 1;
                }
                Value::Real(r) => {
                    *sum += r;
                    *count += 1;
                }
                Value::Null => {}
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric".into(),
                        got: val.data_type().to_string(),
                    })
                }
            },
            AggState::Min(cur) => {
                if !val.is_null() {
                    *cur = Some(match cur.take() {
                        None => val.clone(),
                        Some(m) => {
                            if val < &m {
                                val.clone()
                            } else {
                                m
                            }
                        }
                    });
                }
            }
            AggState::Max(cur) => {
                if !val.is_null() {
                    *cur = Some(match cur.take() {
                        None => val.clone(),
                        Some(m) => {
                            if val > &m {
                                val.clone()
                            } else {
                                m
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) fn feed_raw(&mut self, raw: &RawColumn) -> Result<()> {
        match self {
            AggState::CountStar(c) => {
                *c += 1;
            }
            AggState::Count(c) => {
                if !matches!(raw, RawColumn::Null) {
                    *c += 1;
                }
            }
            AggState::Sum {
                int_sum,
                real_sum,
                has_real,
                all_null,
            } => match raw {
                RawColumn::Integer(i) => {
                    *int_sum += i;
                    *all_null = false;
                }
                RawColumn::Real(r) => {
                    *real_sum += r;
                    *has_real = true;
                    *all_null = false;
                }
                RawColumn::Null => {}
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric".into(),
                        got: "non-numeric".into(),
                    })
                }
            },
            AggState::Avg { sum, count } => match raw {
                RawColumn::Integer(i) => {
                    *sum += *i as f64;
                    *count += 1;
                }
                RawColumn::Real(r) => {
                    *sum += r;
                    *count += 1;
                }
                RawColumn::Null => {}
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric".into(),
                        got: "non-numeric".into(),
                    })
                }
            },
            AggState::Min(cur) => {
                if !matches!(raw, RawColumn::Null) {
                    let val = raw.to_value();
                    *cur = Some(match cur.take() {
                        None => val,
                        Some(m) => {
                            if val < m {
                                val
                            } else {
                                m
                            }
                        }
                    });
                }
            }
            AggState::Max(cur) => {
                if !matches!(raw, RawColumn::Null) {
                    let val = raw.to_value();
                    *cur = Some(match cur.take() {
                        None => val,
                        Some(m) => {
                            if val > m {
                                val
                            } else {
                                m
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Value {
        match self {
            AggState::CountStar(c) | AggState::Count(c) => Value::Integer(c),
            AggState::Sum {
                int_sum,
                real_sum,
                has_real,
                all_null,
            } => {
                if all_null {
                    Value::Null
                } else if has_real {
                    Value::Real(real_sum + int_sum as f64)
                } else {
                    Value::Integer(int_sum)
                }
            }
            AggState::Avg { sum, count } => {
                if count == 0 {
                    Value::Null
                } else {
                    Value::Real(sum / count as f64)
                }
            }
            AggState::Min(v) | AggState::Max(v) => v.unwrap_or(Value::Null),
        }
    }
}

pub(super) struct StreamAggPlan {
    pub(super) ops: Vec<(StreamAgg, String)>,
    partial_ctx: Option<PartialDecodeCtx>,
    raw_targets: Vec<RawAggTarget>,
    num_pk_cols: usize,
    nonpk_agg_defaults: Vec<Option<Value>>,
}

impl StreamAggPlan {
    pub(super) fn try_new(stmt: &SelectStmt, table_schema: &TableSchema) -> Result<Option<Self>> {
        if !stmt.group_by.is_empty() || stmt.having.is_some() || !stmt.joins.is_empty() {
            return Ok(None);
        }

        let col_map = ColumnMap::new(&table_schema.columns);
        let mut ops: Vec<(StreamAgg, String)> = Vec::new();
        for sel_col in &stmt.columns {
            let (expr, alias) = match sel_col {
                SelectColumn::Expr { expr, alias } => (expr, alias),
                _ => return Ok(None),
            };
            let name = alias
                .as_deref()
                .unwrap_or(&expr_display_name(expr))
                .to_string();
            match expr {
                Expr::CountStar => ops.push((StreamAgg::CountStar, name)),
                Expr::Function {
                    name: func_name,
                    args,
                } if args.len() == 1 => {
                    let func = func_name.to_ascii_uppercase();
                    let col_idx = match resolve_simple_col(&args[0], &col_map) {
                        Some(idx) => idx,
                        None => return Ok(None),
                    };
                    match func.as_str() {
                        "COUNT" => ops.push((StreamAgg::Count(col_idx), name)),
                        "SUM" => ops.push((StreamAgg::Sum(col_idx), name)),
                        "AVG" => ops.push((StreamAgg::Avg(col_idx), name)),
                        "MIN" => ops.push((StreamAgg::Min(col_idx), name)),
                        "MAX" => ops.push((StreamAgg::Max(col_idx), name)),
                        _ => return Ok(None),
                    }
                }
                _ => return Ok(None),
            }
        }

        let mut needed: Vec<usize> = ops
            .iter()
            .filter_map(|(op, _)| match op {
                StreamAgg::CountStar => None,
                StreamAgg::Count(i)
                | StreamAgg::Sum(i)
                | StreamAgg::Avg(i)
                | StreamAgg::Min(i)
                | StreamAgg::Max(i) => Some(*i),
            })
            .collect();
        if let Some(ref where_expr) = stmt.where_clause {
            needed.extend(referenced_columns(where_expr, &table_schema.columns));
        }
        needed.sort_unstable();
        needed.dedup();

        let partial_ctx = if needed.len() < table_schema.columns.len() {
            Some(PartialDecodeCtx::new(table_schema, &needed))
        } else {
            None
        };

        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let raw_targets: Vec<RawAggTarget> = ops
            .iter()
            .map(|(op, _)| match op {
                StreamAgg::CountStar => RawAggTarget::CountStar,
                StreamAgg::Count(idx)
                | StreamAgg::Sum(idx)
                | StreamAgg::Avg(idx)
                | StreamAgg::Min(idx)
                | StreamAgg::Max(idx) => {
                    if let Some(pk_pos) = table_schema
                        .primary_key_columns
                        .iter()
                        .position(|&i| i as usize == *idx)
                    {
                        RawAggTarget::Pk(pk_pos)
                    } else {
                        let nonpk_order = non_pk.iter().position(|&i| i == *idx).unwrap();
                        RawAggTarget::NonPk(enc_pos[nonpk_order] as usize)
                    }
                }
            })
            .collect();

        let num_pk_cols = table_schema.primary_key_columns.len();

        let mapping = table_schema.decode_col_mapping();
        let nonpk_agg_defaults: Vec<Option<Value>> = raw_targets
            .iter()
            .map(|t| match t {
                RawAggTarget::NonPk(phys_idx) => {
                    let schema_col = mapping[*phys_idx];
                    if schema_col == usize::MAX {
                        return None;
                    }
                    table_schema.columns[schema_col]
                        .default_expr
                        .as_ref()
                        .and_then(|expr| eval_const_expr(expr).ok())
                }
                _ => None,
            })
            .collect();

        Ok(Some(Self {
            ops,
            partial_ctx,
            raw_targets,
            num_pk_cols,
            nonpk_agg_defaults,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn feed_row(
        &self,
        key: &[u8],
        value: &[u8],
        table_schema: &TableSchema,
        col_map: &ColumnMap,
        where_clause: &Option<Expr>,
        states: &mut [AggState],
        scan_err: &mut Option<SqlError>,
    ) -> bool {
        let row = match &self.partial_ctx {
            Some(ctx) => match ctx.decode(key, value) {
                Ok(r) => r,
                Err(e) => {
                    *scan_err = Some(e);
                    return false;
                }
            },
            None => match decode_full_row(table_schema, key, value) {
                Ok(r) => r,
                Err(e) => {
                    *scan_err = Some(e);
                    return false;
                }
            },
        };

        if let Some(expr) = where_clause {
            match eval_expr(expr, col_map, &row) {
                Ok(val) if !is_truthy(&val) => return true,
                Err(e) => {
                    *scan_err = Some(e);
                    return false;
                }
                _ => {}
            }
        }

        for (i, (op, _)) in self.ops.iter().enumerate() {
            let val = match op {
                StreamAgg::CountStar => &Value::Null,
                StreamAgg::Count(idx)
                | StreamAgg::Sum(idx)
                | StreamAgg::Avg(idx)
                | StreamAgg::Min(idx)
                | StreamAgg::Max(idx) => &row[*idx],
            };
            if let Err(e) = states[i].feed_val(val) {
                *scan_err = Some(e);
                return false;
            }
        }
        true
    }

    pub(super) fn feed_row_raw(
        &self,
        key: &[u8],
        value: &[u8],
        states: &mut [AggState],
        scan_err: &mut Option<SqlError>,
    ) -> bool {
        for (i, target) in self.raw_targets.iter().enumerate() {
            let raw = match target {
                RawAggTarget::CountStar => {
                    if let Err(e) = states[i].feed_raw(&RawColumn::Null) {
                        *scan_err = Some(e);
                        return false;
                    }
                    continue;
                }
                RawAggTarget::Pk(pk_pos) => {
                    if self.num_pk_cols == 1 && *pk_pos == 0 {
                        match decode_pk_integer(key) {
                            Ok(v) => RawColumn::Integer(v),
                            Err(e) => {
                                *scan_err = Some(e);
                                return false;
                            }
                        }
                    } else {
                        match decode_composite_key(key, self.num_pk_cols) {
                            Ok(pk) => RawColumn::Integer(match &pk[*pk_pos] {
                                Value::Integer(i) => *i,
                                _ => {
                                    *scan_err =
                                        Some(SqlError::InvalidValue("PK not integer".into()));
                                    return false;
                                }
                            }),
                            Err(e) => {
                                *scan_err = Some(e);
                                return false;
                            }
                        }
                    }
                }
                RawAggTarget::NonPk(idx) => {
                    let stored = row_non_pk_count(value);
                    if *idx >= stored {
                        if let Some(ref default) = self.nonpk_agg_defaults[i] {
                            if let Err(e) = states[i].feed_val(default) {
                                *scan_err = Some(e);
                                return false;
                            }
                        } else if let Err(e) = states[i].feed_raw(&RawColumn::Null) {
                            *scan_err = Some(e);
                            return false;
                        }
                        continue;
                    }
                    match decode_column_raw(value, *idx) {
                        Ok(v) => v,
                        Err(e) => {
                            *scan_err = Some(e);
                            return false;
                        }
                    }
                }
            };
            if let Err(e) = states[i].feed_raw(&raw) {
                *scan_err = Some(e);
                return false;
            }
        }
        true
    }

    pub(super) fn finish(self, states: Vec<AggState>) -> ExecutionResult {
        let col_names: Vec<String> = self.ops.iter().map(|(_, name)| name.clone()).collect();
        let result_row: Vec<Value> = states.into_iter().map(|s| s.finish()).collect();
        ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: vec![result_row],
        })
    }
}

pub(super) fn resolve_simple_col(expr: &Expr, col_map: &ColumnMap) -> Option<usize> {
    match expr {
        Expr::Column(name) => col_map.resolve(name).ok(),
        Expr::QualifiedColumn { table, column } => col_map.resolve_qualified(table, column).ok(),
        _ => None,
    }
}

pub(super) enum GroupByOutputCol {
    GroupKey,
    Agg(usize),
}

pub(super) struct StreamGroupByPlan {
    group_target: RawAggTarget,
    num_pk_cols: usize,
    agg_ops: Vec<StreamAgg>,
    raw_targets: Vec<RawAggTarget>,
    output: Vec<(GroupByOutputCol, String)>,
    where_pred: Option<SimplePredicate>,
}

impl StreamGroupByPlan {
    pub(super) fn try_new(stmt: &SelectStmt, schema: &TableSchema) -> Result<Option<Self>> {
        if stmt.group_by.len() != 1
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || !stmt.order_by.is_empty()
            || stmt.limit.is_some()
        {
            return Ok(None);
        }

        let where_pred = stmt
            .where_clause
            .as_ref()
            .map(|expr| try_simple_predicate(expr, schema));
        // If WHERE exists but isn't a simple predicate, bail out
        if stmt.where_clause.is_some() && where_pred.as_ref().unwrap().is_none() {
            return Ok(None);
        }
        let where_pred = where_pred.flatten();

        let col_map = ColumnMap::new(&schema.columns);

        let group_col_idx = match &stmt.group_by[0] {
            Expr::Column(name) => col_map.resolve(name).ok(),
            _ => None,
        };
        let group_col_idx = match group_col_idx {
            Some(idx) => idx,
            None => return Ok(None),
        };

        if schema.columns[group_col_idx].data_type != DataType::Integer {
            return Ok(None);
        }

        let non_pk = schema.non_pk_indices();
        let enc_pos = schema.encoding_positions();
        let group_target = if let Some(pk_pos) = schema
            .primary_key_columns
            .iter()
            .position(|&i| i as usize == group_col_idx)
        {
            RawAggTarget::Pk(pk_pos)
        } else {
            let nonpk_order = non_pk.iter().position(|&i| i == group_col_idx).unwrap();
            RawAggTarget::NonPk(enc_pos[nonpk_order] as usize)
        };

        let mut agg_ops = Vec::new();
        let mut raw_targets = Vec::new();
        let mut output = Vec::new();

        for sel_col in &stmt.columns {
            let (expr, alias) = match sel_col {
                SelectColumn::Expr { expr, alias } => (expr, alias),
                _ => return Ok(None),
            };
            let name = alias
                .as_deref()
                .unwrap_or(&expr_display_name(expr))
                .to_string();

            if let Some(idx) = resolve_simple_col(expr, &col_map) {
                if idx == group_col_idx {
                    output.push((GroupByOutputCol::GroupKey, name));
                    continue;
                }
            }

            match expr {
                Expr::CountStar => {
                    let agg_idx = agg_ops.len();
                    agg_ops.push(StreamAgg::CountStar);
                    raw_targets.push(RawAggTarget::CountStar);
                    output.push((GroupByOutputCol::Agg(agg_idx), name));
                }
                Expr::Function {
                    name: func_name,
                    args,
                } if args.len() == 1 => {
                    let func = func_name.to_ascii_uppercase();
                    let col_idx = match resolve_simple_col(&args[0], &col_map) {
                        Some(idx) => idx,
                        None => return Ok(None),
                    };
                    let target = if let Some(pk_pos) = schema
                        .primary_key_columns
                        .iter()
                        .position(|&i| i as usize == col_idx)
                    {
                        RawAggTarget::Pk(pk_pos)
                    } else {
                        let nonpk_order = non_pk.iter().position(|&i| i == col_idx).unwrap();
                        RawAggTarget::NonPk(enc_pos[nonpk_order] as usize)
                    };
                    let agg_idx = agg_ops.len();
                    match func.as_str() {
                        "COUNT" => agg_ops.push(StreamAgg::Count(col_idx)),
                        "SUM" => agg_ops.push(StreamAgg::Sum(col_idx)),
                        "AVG" => agg_ops.push(StreamAgg::Avg(col_idx)),
                        "MIN" => agg_ops.push(StreamAgg::Min(col_idx)),
                        "MAX" => agg_ops.push(StreamAgg::Max(col_idx)),
                        _ => return Ok(None),
                    }
                    raw_targets.push(target);
                    output.push((GroupByOutputCol::Agg(agg_idx), name));
                }
                _ => return Ok(None),
            }
        }

        Ok(Some(Self {
            group_target,
            num_pk_cols: schema.primary_key_columns.len(),
            agg_ops,
            raw_targets,
            output,
            where_pred,
        }))
    }

    pub(super) fn execute_scan(
        &self,
        scan: impl FnOnce(
            &mut dyn FnMut(&[u8], &[u8]) -> bool,
        ) -> std::result::Result<(), citadel::Error>,
    ) -> Result<ExecutionResult> {
        let mut groups: HashMap<i64, Vec<AggState>> = HashMap::new();
        let mut null_group: Option<Vec<AggState>> = None;
        let mut scan_err: Option<SqlError> = None;

        scan(&mut |key, value| {
            if let Some(ref pred) = self.where_pred {
                match pred.matches_raw(key, value) {
                    Ok(true) => {}
                    Ok(false) => return true,
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                }
            }

            let group_key: Option<i64> = match &self.group_target {
                RawAggTarget::Pk(pk_pos) => {
                    if self.num_pk_cols == 1 && *pk_pos == 0 {
                        match decode_pk_integer(key) {
                            Ok(v) => Some(v),
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        }
                    } else {
                        match decode_composite_key(key, self.num_pk_cols) {
                            Ok(pk) => match &pk[*pk_pos] {
                                Value::Integer(i) => Some(*i),
                                Value::Null => None,
                                _ => {
                                    scan_err = Some(SqlError::InvalidValue(
                                        "GROUP BY key not integer".into(),
                                    ));
                                    return false;
                                }
                            },
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        }
                    }
                }
                RawAggTarget::NonPk(idx) => match decode_column_raw(value, *idx) {
                    Ok(RawColumn::Integer(i)) => Some(i),
                    Ok(RawColumn::Null) => None,
                    Ok(_) => {
                        scan_err = Some(SqlError::InvalidValue("GROUP BY key not integer".into()));
                        return false;
                    }
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                },
                RawAggTarget::CountStar => unreachable!(),
            };

            let states = match group_key {
                Some(k) => groups
                    .entry(k)
                    .or_insert_with(|| self.agg_ops.iter().map(AggState::new).collect()),
                None => null_group
                    .get_or_insert_with(|| self.agg_ops.iter().map(AggState::new).collect()),
            };

            for (i, target) in self.raw_targets.iter().enumerate() {
                let raw = match target {
                    RawAggTarget::CountStar => {
                        if let Err(e) = states[i].feed_raw(&RawColumn::Null) {
                            scan_err = Some(e);
                            return false;
                        }
                        continue;
                    }
                    RawAggTarget::Pk(pk_pos) => {
                        if self.num_pk_cols == 1 && *pk_pos == 0 {
                            match decode_pk_integer(key) {
                                Ok(v) => RawColumn::Integer(v),
                                Err(e) => {
                                    scan_err = Some(e);
                                    return false;
                                }
                            }
                        } else {
                            match decode_composite_key(key, self.num_pk_cols) {
                                Ok(pk) => match &pk[*pk_pos] {
                                    Value::Integer(i) => RawColumn::Integer(*i),
                                    _ => {
                                        scan_err = Some(SqlError::InvalidValue(
                                            "agg column not integer".into(),
                                        ));
                                        return false;
                                    }
                                },
                                Err(e) => {
                                    scan_err = Some(e);
                                    return false;
                                }
                            }
                        }
                    }
                    RawAggTarget::NonPk(idx) => match decode_column_raw(value, *idx) {
                        Ok(v) => v,
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    },
                };
                if let Err(e) = states[i].feed_raw(&raw) {
                    scan_err = Some(e);
                    return false;
                }
            }
            true
        })
        .map_err(SqlError::Storage)?;

        if let Some(e) = scan_err {
            return Err(e);
        }

        let col_names: Vec<String> = self.output.iter().map(|(_, name)| name.clone()).collect();
        let null_extra = if null_group.is_some() { 1 } else { 0 };
        let mut result_rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len() + null_extra);
        if let Some(states) = null_group {
            let mut row = Vec::with_capacity(self.output.len());
            let finished: Vec<Value> = states.into_iter().map(|s| s.finish()).collect();
            for (col, _) in &self.output {
                match col {
                    GroupByOutputCol::GroupKey => row.push(Value::Null),
                    GroupByOutputCol::Agg(idx) => row.push(finished[*idx].clone()),
                }
            }
            result_rows.push(row);
        }
        for (group_key, states) in groups {
            let mut row = Vec::with_capacity(self.output.len());
            let finished: Vec<Value> = states.into_iter().map(|s| s.finish()).collect();
            for (col, _) in &self.output {
                match col {
                    GroupByOutputCol::GroupKey => row.push(Value::Integer(group_key)),
                    GroupByOutputCol::Agg(idx) => row.push(finished[*idx].clone()),
                }
            }
            result_rows.push(row);
        }

        Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: result_rows,
        }))
    }
}

pub(super) struct TopKScanPlan {
    sort_target: RawAggTarget,
    num_pk_cols: usize,
    descending: bool,
    nulls_first: bool,
    keep: usize,
}

impl TopKScanPlan {
    pub(super) fn try_new(stmt: &SelectStmt, schema: &TableSchema) -> Result<Option<Self>> {
        if stmt.order_by.len() != 1
            || stmt.limit.is_none()
            || stmt.where_clause.is_some()
            || !stmt.group_by.is_empty()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || stmt.distinct
        {
            return Ok(None);
        }

        if has_any_window_function(stmt) {
            return Ok(None);
        }

        let has_aggregates = stmt.columns.iter().any(|c| match c {
            SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
            _ => false,
        });
        if has_aggregates {
            return Ok(None);
        }

        let ob = &stmt.order_by[0];
        let col_map = ColumnMap::new(&schema.columns);
        let col_idx = match resolve_simple_col(&ob.expr, &col_map) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let non_pk = schema.non_pk_indices();
        let enc_pos_arr = schema.encoding_positions();
        let sort_target = if let Some(pk_pos) = schema
            .primary_key_columns
            .iter()
            .position(|&i| i as usize == col_idx)
        {
            RawAggTarget::Pk(pk_pos)
        } else {
            let nonpk_order = non_pk.iter().position(|&i| i == col_idx).unwrap();
            RawAggTarget::NonPk(enc_pos_arr[nonpk_order] as usize)
        };

        let limit = eval_const_int(stmt.limit.as_ref().unwrap())?.max(0) as usize;
        let offset = stmt
            .offset
            .as_ref()
            .map(eval_const_int)
            .transpose()?
            .unwrap_or(0)
            .max(0) as usize;
        let keep = limit.saturating_add(offset);
        if keep == 0 {
            return Ok(None);
        }

        Ok(Some(Self {
            sort_target,
            num_pk_cols: schema.primary_key_columns.len(),
            descending: ob.descending,
            nulls_first: ob.nulls_first.unwrap_or(!ob.descending),
            keep,
        }))
    }

    pub(super) fn execute_scan(
        &self,
        schema: &TableSchema,
        stmt: &SelectStmt,
        scan: impl FnOnce(
            &mut dyn FnMut(&[u8], &[u8]) -> bool,
        ) -> std::result::Result<(), citadel::Error>,
    ) -> Result<ExecutionResult> {
        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        struct Candidate {
            sort_key: Value,
            raw_key: Vec<u8>,
            raw_value: Vec<u8>,
        }

        struct CandWrapper {
            c: Candidate,
            descending: bool,
            nulls_first: bool,
        }

        impl PartialEq for CandWrapper {
            fn eq(&self, other: &Self) -> bool {
                self.cmp(other) == Ordering::Equal
            }
        }
        impl Eq for CandWrapper {}

        impl PartialOrd for CandWrapper {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        // Max-heap: worst candidate on top for eviction.
        impl Ord for CandWrapper {
            fn cmp(&self, other: &Self) -> Ordering {
                let ord = match (self.c.sort_key.is_null(), other.c.sort_key.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => {
                        if self.nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (false, true) => {
                        if self.nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    (false, false) => self.c.sort_key.cmp(&other.c.sort_key),
                };
                if self.descending {
                    ord.reverse()
                } else {
                    ord
                }
            }
        }

        let k = self.keep;
        let mut heap: BinaryHeap<CandWrapper> = BinaryHeap::with_capacity(k + 1);
        let mut scan_err: Option<SqlError> = None;

        scan(&mut |key, value| {
            let sort_key: Value = match &self.sort_target {
                RawAggTarget::Pk(pk_pos) => {
                    if self.num_pk_cols == 1 && *pk_pos == 0 {
                        match decode_pk_integer(key) {
                            Ok(v) => Value::Integer(v),
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        }
                    } else {
                        match decode_composite_key(key, self.num_pk_cols) {
                            Ok(mut pk) => std::mem::replace(&mut pk[*pk_pos], Value::Null),
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        }
                    }
                }
                RawAggTarget::NonPk(idx) => match decode_column_raw(value, *idx) {
                    Ok(raw) => raw.to_value(),
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                },
                RawAggTarget::CountStar => unreachable!(),
            };

            // Heap full and can't beat worst - skip
            if heap.len() >= k {
                if let Some(top) = heap.peek() {
                    let ord = match (sort_key.is_null(), top.c.sort_key.is_null()) {
                        (true, true) => Ordering::Equal,
                        (true, false) => {
                            if self.nulls_first {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                        (false, true) => {
                            if self.nulls_first {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        (false, false) => sort_key.cmp(&top.c.sort_key),
                    };
                    let cmp = if self.descending { ord.reverse() } else { ord };
                    if cmp != Ordering::Less {
                        return true;
                    }
                }
            }

            let cand = CandWrapper {
                c: Candidate {
                    sort_key,
                    raw_key: key.to_vec(),
                    raw_value: value.to_vec(),
                },
                descending: self.descending,
                nulls_first: self.nulls_first,
            };

            if heap.len() < k {
                heap.push(cand);
            } else if let Some(mut top) = heap.peek_mut() {
                *top = cand;
            }

            true
        })
        .map_err(SqlError::Storage)?;

        if let Some(e) = scan_err {
            return Err(e);
        }

        let mut winners: Vec<CandWrapper> = heap.into_vec();
        winners.sort();

        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(winners.len());
        for w in &winners {
            rows.push(decode_full_row(schema, &w.c.raw_key, &w.c.raw_value)?);
        }

        if let Some(ref offset_expr) = stmt.offset {
            let offset = eval_const_int(offset_expr)?.max(0) as usize;
            if offset < rows.len() {
                rows = rows.split_off(offset);
            } else {
                rows.clear();
            }
        }
        if let Some(ref limit_expr) = stmt.limit {
            let limit = eval_const_int(limit_expr)?.max(0) as usize;
            rows.truncate(limit);
        }

        let (col_names, projected) = project_rows(&schema.columns, &stmt.columns, rows)?;
        Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: projected,
        }))
    }
}

/// Streaming DISTINCT: extract only needed columns from raw scan, dedup inline.
fn try_streaming_distinct(
    stmt: &SelectStmt,
    table_schema: &TableSchema,
    db: &Database,
) -> Result<Option<ExecutionResult>> {
    if !stmt.distinct
        || stmt.where_clause.is_some()
        || !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || !stmt.joins.is_empty()
        || !stmt.order_by.is_empty()
    {
        return Ok(None);
    }

    let col_map = ColumnMap::new(&table_schema.columns);
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let num_pk_cols = table_schema.primary_key_columns.len();

    // Resolve each SELECT column to a RawAggTarget
    let mut targets: Vec<RawAggTarget> = Vec::new();
    let mut col_names: Vec<String> = Vec::new();

    for sel_col in &stmt.columns {
        let (expr, alias) = match sel_col {
            SelectColumn::Expr { expr, alias } => (expr, alias),
            _ => return Ok(None),
        };
        let name = alias
            .as_deref()
            .unwrap_or(&expr_display_name(expr))
            .to_string();
        let col_idx = match resolve_simple_col(expr, &col_map) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let target = if let Some(pk_pos) = table_schema
            .primary_key_columns
            .iter()
            .position(|&i| i as usize == col_idx)
        {
            RawAggTarget::Pk(pk_pos)
        } else {
            let nonpk_order = non_pk.iter().position(|&i| i == col_idx).unwrap();
            RawAggTarget::NonPk(enc_pos[nonpk_order] as usize)
        };
        targets.push(target);
        col_names.push(name);
    }

    let lower_name = &table_schema.name;
    let mut seen = std::collections::HashSet::<Vec<u8>>::new();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut scan_err: Option<SqlError> = None;
    let mut raw_key_buf: Vec<u8> = Vec::with_capacity(64);

    let mut rtx = db.begin_read();
    rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
        raw_key_buf.clear();
        for target in &targets {
            match target {
                RawAggTarget::CountStar => {}
                RawAggTarget::Pk(_) => raw_key_buf.extend_from_slice(key),
                RawAggTarget::NonPk(idx) => match decode_column_with_offset(value, *idx) {
                    Ok((_, offset)) => {
                        if offset == usize::MAX {
                            raw_key_buf.push(0xFF);
                        } else if offset + 5 <= value.len() {
                            let data_len = u32::from_le_bytes(
                                value[offset + 1..offset + 5].try_into().unwrap(),
                            ) as usize;
                            let end = (offset + 5 + data_len).min(value.len());
                            raw_key_buf.extend_from_slice(&value[offset..end]);
                        }
                    }
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                },
            }
        }
        if seen.insert(raw_key_buf.clone()) {
            let mut row_val: Vec<Value> = Vec::with_capacity(targets.len());
            for target in &targets {
                let val = match target {
                    RawAggTarget::CountStar => Value::Null,
                    RawAggTarget::Pk(pk_pos) => {
                        if num_pk_cols == 1 && *pk_pos == 0 {
                            match decode_pk_integer(key) {
                                Ok(v) => Value::Integer(v),
                                Err(e) => {
                                    scan_err = Some(e);
                                    return false;
                                }
                            }
                        } else {
                            match decode_composite_key(key, num_pk_cols) {
                                Ok(pk) => pk[*pk_pos].clone(),
                                Err(e) => {
                                    scan_err = Some(e);
                                    return false;
                                }
                            }
                        }
                    }
                    RawAggTarget::NonPk(idx) => match decode_column_raw(value, *idx) {
                        Ok(raw) => raw.to_value(),
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    },
                };
                row_val.push(val);
            }
            rows.push(row_val);
        }
        scan_err.is_none()
    })
    .map_err(SqlError::Storage)?;

    if let Some(e) = scan_err {
        return Err(e);
    }

    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_const_int(offset_expr)?.max(0) as usize;
        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }
    }
    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_const_int(limit_expr)?.max(0) as usize;
        rows.truncate(limit);
    }

    Ok(Some(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows,
    })))
}

pub(super) fn exec_select_no_from(stmt: &SelectStmt) -> Result<ExecutionResult> {
    let empty_cols: Vec<ColumnDef> = vec![];
    let empty_row: Vec<Value> = vec![];
    let (col_names, projected) = project_rows(&empty_cols, &stmt.columns, vec![empty_row])?;
    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: projected,
    }))
}

pub(super) fn process_select(
    columns: &[ColumnDef],
    mut rows: Vec<Vec<Value>>,
    stmt: &SelectStmt,
    predicate_applied: bool,
) -> Result<ExecutionResult> {
    if !predicate_applied {
        if let Some(ref where_expr) = stmt.where_clause {
            let col_map = ColumnMap::new(columns);
            rows.retain(|row| match eval_expr(where_expr, &col_map, row) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            });
        }
    }

    if has_any_window_function(stmt) {
        return eval_window_select(columns, rows, stmt);
    }

    let has_aggregates = stmt.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    });

    if has_aggregates || !stmt.group_by.is_empty() {
        return exec_aggregate(columns, &rows, stmt);
    }

    if stmt.distinct {
        let (col_names, mut projected) = project_rows(columns, &stmt.columns, rows)?;

        let mut seen = std::collections::HashSet::new();
        projected.retain(|row| seen.insert(row.clone()));

        if !stmt.order_by.is_empty() {
            let output_cols = build_output_columns(&stmt.columns, columns);
            sort_rows(&mut projected, &stmt.order_by, &output_cols)?;
        }

        if let Some(ref offset_expr) = stmt.offset {
            let offset = eval_const_int(offset_expr)?.max(0) as usize;
            if offset < projected.len() {
                projected = projected.split_off(offset);
            } else {
                projected.clear();
            }
        }

        if let Some(ref limit_expr) = stmt.limit {
            let limit = eval_const_int(limit_expr)?.max(0) as usize;
            projected.truncate(limit);
        }

        return Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: projected,
        }));
    }

    if !stmt.order_by.is_empty() {
        if let Some(ref limit_expr) = stmt.limit {
            let limit = eval_const_int(limit_expr)?.max(0) as usize;
            let offset = match stmt.offset {
                Some(ref e) => eval_const_int(e)?.max(0) as usize,
                None => 0,
            };
            let keep = limit.saturating_add(offset);
            if keep == 0 {
                rows.clear();
            } else if keep < rows.len() {
                topk_rows(&mut rows, &stmt.order_by, columns, keep)?;
                rows.truncate(keep);
            } else {
                sort_rows(&mut rows, &stmt.order_by, columns)?;
            }
        } else {
            sort_rows(&mut rows, &stmt.order_by, columns)?;
        }
    }

    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_const_int(offset_expr)?.max(0) as usize;
        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }
    }

    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_const_int(limit_expr)?.max(0) as usize;
        rows.truncate(limit);
    }

    let (col_names, projected) = project_rows(columns, &stmt.columns, rows)?;

    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: projected,
    }))
}
