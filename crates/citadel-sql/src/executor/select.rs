use std::sync::Arc;

use citadel::{CancelToken, Database};
use citadel_txn::read_txn::ReadTxn;
use rustc_hash::FxHashMap;

use crate::encoding::{
    decode_column_raw, decode_column_with_offset, decode_composite_key, decode_pk_integer,
    decode_stored_column_raw, RawColumn,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, referenced_columns, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::aggregate::*;
use super::compile::CompiledPlan;
use super::correlated::*;
use super::cte::*;
use super::dml::*;
use super::helpers::*;
use super::scan::*;
pub(super) use super::topk::TopKScanPlan;
use super::view::*;
use super::window::*;
use super::{CteContext, CteRows, SelectCtx};

fn try_virtual_table(
    name: &str,
    schema: &SchemaManager,
    cancel: Option<&CancelToken>,
) -> Option<Result<QueryResult>> {
    let canonical = match name {
        "timezone_names" => "pg_timezone_names",
        "timezone_abbrevs" => "pg_timezone_abbrevs",
        other => other,
    };
    let vt = schema.get_virtual(canonical)?;
    Some(vt.scan(schema, cancel))
}

/// Which strategy a single-table SELECT actually takes.
///
/// One decision procedure, consulted by the executor and by EXPLAIN, so the two
/// cannot describe different plans. Plans are boxed to keep the enum small.
pub(super) enum Strategy {
    CountStar,
    StreamAgg(Box<StreamAggPlan>),
    StreamGroupBy(Box<StreamGroupByPlan>),
    AnnTopK(Box<super::ann_topk::AnnTopKPlan>),
    VectorTopK(Box<super::ann_topk::VectorTopKPlan>),
    TopKScan(Box<TopKScanPlan>),
    /// A row scan, reading at most `limit` rows when the shape allows one.
    /// Streaming DISTINCT and the two inverted-index lanes may still claim this
    /// query; their eligibility is interleaved with execution, so it is not
    /// decided here and `Strategy::label` says so.
    Scan {
        limit: Option<usize>,
    },
}

impl Strategy {
    /// The EXPLAIN line for this strategy, naming what will run.
    pub(super) fn label(&self) -> Option<&'static str> {
        match self {
            Self::CountStar => Some("COUNT(*) FROM CATALOG"),
            Self::StreamAgg(_) => Some("STREAM AGGREGATE (fused scan+aggregate)"),
            Self::StreamGroupBy(_) => Some("STREAM GROUP BY (fused scan+group)"),
            Self::AnnTopK(_) => Some("ANN TOP-K (approximate index)"),
            Self::VectorTopK(_) => Some("VECTOR TOP-K (exact, streaming)"),
            Self::TopKScan(_) => Some("TOPK SCAN (fused scan+filter+sort+limit)"),
            Self::Scan { .. } => None,
        }
    }
}

/// Pick the strategy for a single-table SELECT.
///
/// Pure over `(stmt, table_schema)`: no transaction, no rows read. That is what
/// lets EXPLAIN report the same answer the executor acts on.
pub(super) fn choose_strategy(stmt: &SelectStmt, table_schema: &TableSchema) -> Result<Strategy> {
    choose_strategy_with_cancel(stmt, table_schema, None)
}

fn choose_strategy_with_cancel(
    stmt: &SelectStmt,
    table_schema: &TableSchema,
    cancel: Option<&CancelToken>,
) -> Result<Strategy> {
    if stmt.order_by.iter().any(order_by_uses_projected_output) {
        let output_columns = build_output_columns(&stmt.columns, &table_schema.columns);
        let output_map = ColumnMap::new(&output_columns);
        for item in &stmt.order_by {
            order_by_output_position(item, &output_map)?;
        }
    }

    if count_star_output_name(stmt).is_some() {
        return Ok(Strategy::CountStar);
    }
    if let Some(p) = StreamAggPlan::try_new_with_cancel(stmt, table_schema, cancel)? {
        return Ok(Strategy::StreamAgg(Box::new(p)));
    }
    if let Some(p) = StreamGroupByPlan::try_new(stmt, table_schema)? {
        return Ok(Strategy::StreamGroupBy(Box::new(p)));
    }
    if let Some(p) = super::ann_topk::AnnTopKPlan::try_new(stmt, table_schema)? {
        return Ok(Strategy::AnnTopK(Box::new(p)));
    }
    if let Some(p) = super::ann_topk::VectorTopKPlan::try_new(stmt, table_schema)? {
        return Ok(Strategy::VectorTopK(Box::new(p)));
    }
    if let Some(p) = TopKScanPlan::try_new(stmt, table_schema)? {
        return Ok(Strategy::TopKScan(Box::new(p)));
    }
    Ok(Strategy::Scan {
        limit: compute_scan_limit(stmt, table_schema),
    })
}

pub(super) fn exec_select_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    // Cloned once so the post-scan phases can hold it without borrowing `rtx`,
    // which stays mutably borrowed for the scan itself.
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    if stmt.from.is_empty() && stmt.from_subquery.is_none() {
        let materialized;
        let stmt = if stmt_has_subquery(stmt) {
            materialized = materialize_stmt(stmt, &mut |sub| {
                exec_subquery_with_read(rtx, schema, sub, ctes)
            })?;
            &materialized
        } else {
            stmt
        };
        return exec_select_no_from(stmt, cancel);
    }

    if has_lateral(stmt) {
        return exec_select_lateral_with_read(rtx, schema, stmt, ctes);
    }
    if has_non_lateral_derived(stmt) {
        return exec_select_with_derived_with_read(rtx, schema, stmt, ctes);
    }

    if stmt.from_args.is_some() && crate::json::is_srf_name(&stmt.from) {
        return exec_select_with_srf_with_read(rtx, schema, stmt, ctes, cancel);
    }
    if stmt.from_json_table.is_some() {
        return exec_select_with_json_table_with_read(rtx, schema, stmt, ctes, cancel);
    }

    let lower_name = stmt.from.to_ascii_lowercase();

    if let Some(vt_result) = try_virtual_table(&lower_name, schema, cancel) {
        let vt_result = CteRows::binary(vt_result?);
        if stmt.joins.is_empty() {
            return exec_select_from_cte(
                &vt_result,
                stmt,
                &mut |sub| exec_subquery_with_read(rtx, schema, sub, ctes),
                cancel,
            );
        }
        let mut vt_ctes = ctes.clone();
        vt_ctes.insert(lower_name.clone(), vt_result.shared());
        return super::exec_select_join_with_ctes(
            stmt,
            &vt_ctes,
            &mut |name| super::scan_table_with_read_or_view(rtx, schema, name),
            cancel,
        );
    }

    if let Some(cte_result) = ctes.get(&lower_name) {
        if stmt.joins.is_empty() {
            return exec_select_from_cte(
                cte_result,
                stmt,
                &mut |sub| exec_subquery_with_read(rtx, schema, sub, ctes),
                cancel,
            );
        } else {
            return super::exec_select_join_with_ctes(
                stmt,
                ctes,
                &mut |name| super::scan_table_with_read(rtx, schema, name),
                cancel,
            );
        }
    }

    if !ctes.is_empty()
        && stmt
            .joins
            .iter()
            .any(|j| ctes.contains_key(&j.table.name.to_ascii_lowercase()))
    {
        return super::exec_select_join_with_ctes(
            stmt,
            ctes,
            &mut |name| super::scan_table_with_read_or_view(rtx, schema, name),
            cancel,
        );
    }

    if let Some(view_def) = schema.get_view(&lower_name) {
        if let Some(fused) = try_fuse_view(stmt, schema, view_def)? {
            return exec_select_with_read(rtx, schema, &fused, ctes);
        }
        let view_qr = exec_view_with_read(rtx, schema, view_def)?;
        if stmt.joins.is_empty() {
            let view_schema = build_view_schema(&lower_name, &view_qr);
            let view_ctx = CorrelationCtx {
                outer_schema: &view_schema,
                outer_alias: stmt.from_alias.as_deref(),
            };
            if has_correlated_where(&stmt.where_clause, &view_ctx, schema) {
                let mut rows = super::clone_cte_rows_with_cancel(&view_qr.result.rows, cancel)?;
                let remaining =
                    handle_correlated_where_with_read(rtx, schema, stmt, &view_ctx, &mut rows)?;
                let clean_stmt = SelectStmt {
                    where_clause: remaining,
                    columns: stmt.columns.clone(),
                    from: stmt.from.clone(),
                    from_alias: stmt.from_alias.clone(),
                    from_subquery: stmt.from_subquery.clone(),
                    from_args: stmt.from_args.clone(),
                    from_json_table: stmt.from_json_table.clone(),
                    joins: vec![],
                    distinct: stmt.distinct,
                    order_by: stmt.order_by.clone(),
                    limit: stmt.limit.clone(),
                    offset: stmt.offset.clone(),
                    group_by: stmt.group_by.clone(),
                    having: stmt.having.clone(),
                };
                return process_select(
                    rows,
                    SelectCtx::new(&view_schema.columns, &clean_stmt, cancel),
                );
            }
            return exec_select_from_cte(
                &view_qr,
                stmt,
                &mut |sub| exec_subquery_with_read(rtx, schema, sub, ctes),
                cancel,
            );
        } else {
            let mut view_ctes = ctes.clone();
            view_ctes.insert(lower_name.clone(), view_qr.shared());
            return super::exec_select_join_with_ctes(
                stmt,
                &view_ctes,
                &mut |name| super::scan_table_with_read_or_view(rtx, schema, name),
                cancel,
            );
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
                    let vqr = exec_view_with_read(rtx, schema, vd)?;
                    e.insert(vqr.shared());
                }
            }
        }
        return super::exec_select_join_with_ctes(
            stmt,
            &view_ctes,
            &mut |name| super::scan_table_with_read(rtx, schema, name),
            cancel,
        );
    }

    let table_schema = schema
        .get(&lower_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;
    // Storage operations below must use the resolved name (post-TEMP-alias).
    let lower_name = table_schema.name.clone();

    // Correlated subquery handling: decorrelate before materialization
    let corr_ctx = CorrelationCtx {
        outer_schema: table_schema,
        outer_alias: stmt.from_alias.as_deref(),
    };
    if has_correlated_where(&stmt.where_clause, &corr_ctx, schema) {
        let (mut rows, remaining_where) =
            build_and_scan_correlated_with_read(rtx, schema, stmt, table_schema, &corr_ctx)?;
        let clean_stmt = SelectStmt {
            where_clause: remaining_where,
            columns: stmt.columns.clone(),
            from: stmt.from.clone(),
            from_alias: stmt.from_alias.clone(),
            from_subquery: stmt.from_subquery.clone(),
            from_args: stmt.from_args.clone(),
            from_json_table: stmt.from_json_table.clone(),
            joins: stmt.joins.clone(),
            distinct: stmt.distinct,
            order_by: stmt.order_by.clone(),
            limit: stmt.limit.clone(),
            offset: stmt.offset.clone(),
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
        };
        let mut ext_cols = table_schema.columns.clone();
        let clean_stmt = handle_correlated_select_with_read(
            rtx,
            schema,
            &clean_stmt,
            &corr_ctx,
            &mut rows,
            &mut ext_cols,
        )?;

        let final_stmt;
        let s = if stmt_has_subquery(&clean_stmt) {
            final_stmt = materialize_stmt(&clean_stmt, &mut |sub| {
                exec_subquery_with_read(rtx, schema, sub, ctes)
            })?;
            &final_stmt
        } else {
            &clean_stmt
        };
        return process_select(rows, SelectCtx::new(&ext_cols, s, cancel));
    }

    if has_correlated_select(&stmt.columns, &corr_ctx, schema) {
        let (mut rows, _) = collect_rows_with_read(rtx, table_schema, &stmt.where_clause, None)?;
        let mut ext_cols = table_schema.columns.clone();
        let clean_stmt = handle_correlated_select_with_read(
            rtx,
            schema,
            stmt,
            &corr_ctx,
            &mut rows,
            &mut ext_cols,
        )?;
        let final_stmt;
        let s = if stmt_has_subquery(&clean_stmt) {
            final_stmt = materialize_stmt(&clean_stmt, &mut |sub| {
                exec_subquery_with_read(rtx, schema, sub, ctes)
            })?;
            &final_stmt
        } else {
            &clean_stmt
        };
        return process_select(
            rows,
            SelectCtx::new(&ext_cols, s, cancel).predicate_applied(true),
        );
    }

    let materialized;
    let stmt = if stmt_has_subquery(stmt) {
        materialized = materialize_stmt(stmt, &mut |sub| {
            exec_subquery_with_read(rtx, schema, sub, ctes)
        })?;
        &materialized
    } else {
        stmt
    };

    if !stmt.joins.is_empty() {
        return super::exec_select_join_with_read(rtx, schema, stmt);
    }

    // One decision, shared with EXPLAIN. The arms below consume what it picked
    // rather than re-deciding, so the two cannot drift apart.
    let strategy = choose_strategy_with_cancel(stmt, table_schema, cancel)?;

    let scan_limit = match strategy {
        Strategy::CountStar => {
            return try_count_star_shortcut(stmt, || {
                rtx.table_entry_count(lower_name.as_bytes())
                    .map_err(SqlError::Storage)
            })?
            .ok_or_else(|| {
                SqlError::Unsupported(
                    "count-star strategy was chosen but the shortcut declined".into(),
                )
            });
        }
        Strategy::StreamAgg(plan) => {
            return exec_stream_agg(rtx, stmt, table_schema, &lower_name, *plan)
        }
        Strategy::StreamGroupBy(plan) => {
            let lower = lower_name.clone();
            return plan.execute_scan(cancel, |cb| {
                rtx.table_scan_raw(lower.as_bytes(), |key, value| cb(key, value))
            });
        }
        Strategy::AnnTopK(plan) => return plan.execute_with_read(rtx, schema, stmt, table_schema),
        Strategy::VectorTopK(plan) => return plan.execute(rtx, table_schema, stmt),
        Strategy::TopKScan(plan) => {
            let lower = lower_name.clone();
            return plan.execute_scan(table_schema, stmt, cancel, |cb| {
                rtx.table_scan_raw(lower.as_bytes(), |key, value| cb(key, value))
            });
        }
        Strategy::Scan { limit } => limit,
    };

    if let Some(result) = try_streaming_distinct_with_read(rtx, stmt, table_schema)? {
        return Ok(result);
    }

    if let Some(result) = try_inverted_ts_rank_topk_with_read(rtx, table_schema, stmt)? {
        return Ok(result);
    }

    if let Some(result) = try_inverted_index_only_with_read(rtx, table_schema, stmt)? {
        return Ok(result);
    }

    let (rows, predicate_applied) =
        collect_rows_with_read(rtx, table_schema, &stmt.where_clause, scan_limit)?;
    process_select(
        rows,
        SelectCtx::new(&table_schema.columns, stmt, cancel).predicate_applied(predicate_applied),
    )
}

/// The streaming-aggregate lane, lifted out of the strategy match so that arm
/// stays one line like the others.
fn exec_stream_agg(
    rtx: &mut ReadTxn<'_>,
    stmt: &SelectStmt,
    table_schema: &TableSchema,
    lower_name: &str,
    plan: StreamAggPlan,
) -> Result<ExecutionResult> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let mut states: Vec<AggState> = plan.ops.iter().map(|(op, _)| AggState::new(op)).collect();
    let mut scan_err: Option<SqlError> = None;

    if stmt.where_clause.is_none() {
        let leaves = rtx
            .collect_table_leaves(lower_name.as_bytes())
            .map_err(SqlError::Storage)?;
        match try_parallel_stream_agg(rtx, &plan, &leaves)? {
            Some(merged) => states = merged,
            None => {
                rtx.scan_leaves(&leaves, |key, value| {
                    plan.feed_row_raw(key, value, &mut states, &mut scan_err)
                })
                .map_err(SqlError::Storage)?;
            }
        }
    } else if let Some(w) = &stmt.where_clause {
        let all_count_star = plan
            .ops
            .iter()
            .all(|(op, _)| matches!(op, StreamAgg::CountStar));
        let mut counted_from_index = false;
        if all_count_star {
            let scan_plan = crate::planner::plan_select(table_schema, &stmt.where_clause);
            if crate::planner::index_scan_full_cover(table_schema, w, &scan_plan) {
                if let Some(n) =
                    super::scan::covered_index_count_read(rtx, table_schema, &scan_plan)?
                {
                    states = plan
                        .ops
                        .iter()
                        .map(|_| AggState::CountStar(n as i64))
                        .collect();
                    counted_from_index = true;
                }
            }
        }
        if !counted_from_index {
            let col_map = table_schema.column_map();
            rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
                plan.feed_row(
                    key,
                    value,
                    table_schema,
                    col_map,
                    &stmt.where_clause,
                    &mut states,
                    &mut scan_err,
                    cancel,
                )
            })
            .map_err(SqlError::Storage)?;
        }
    }

    if let Some(e) = scan_err {
        return Err(e);
    }
    let mut result = plan.finish(states);
    if let ExecutionResult::Query(query) = &mut result {
        apply_offset_limit(&mut query.rows, stmt)?;
    }
    Ok(result)
}

fn fts_phrase_ast_from_predicate(expr: &Expr) -> Option<crate::fts::TsQueryAst> {
    match expr {
        Expr::BinaryOp {
            op: BinOp::JsonPathMatch,
            right,
            ..
        } => {
            let col_map = crate::eval::ColumnMap::new(&[]);
            let ctx = crate::eval::EvalCtx::new(&col_map, &[]);
            match crate::eval::eval_expr(right, &ctx).ok()? {
                Value::TsQuery(bytes) => crate::fts::TsQueryAst::decode(&bytes).ok(),
                _ => None,
            }
        }
        _ => None,
    }
}

fn fts_phrase_lexemes(ast: &crate::fts::TsQueryAst) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    fn walk(ast: &crate::fts::TsQueryAst, out: &mut Vec<Vec<u8>>) {
        match ast {
            crate::fts::TsQueryAst::Lexeme { lexeme, .. } => out.push(lexeme.clone()),
            crate::fts::TsQueryAst::Phrase { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
            _ => {}
        }
    }
    walk(ast, &mut out);
    out
}

fn compile_phrase_eval(ast: &crate::fts::TsQueryAst, phrase_lexemes: &[Vec<u8>]) -> CompiledPhrase {
    fn walk(ast: &crate::fts::TsQueryAst, phrase_lexemes: &[Vec<u8>]) -> CompiledPhrase {
        match ast {
            crate::fts::TsQueryAst::Lexeme { lexeme, .. } => {
                let idx = phrase_lexemes
                    .iter()
                    .position(|l| l == lexeme)
                    .expect("phrase lexeme not in list");
                CompiledPhrase::Leaf(idx)
            }
            crate::fts::TsQueryAst::Phrase {
                distance,
                left,
                right,
            } => CompiledPhrase::Phrase {
                distance: *distance,
                left: Box::new(walk(left, phrase_lexemes)),
                right: Box::new(walk(right, phrase_lexemes)),
            },
            _ => unreachable!("pure-phrase AST only"),
        }
    }
    walk(ast, phrase_lexemes)
}

enum CompiledPhrase {
    Leaf(usize),
    Phrase {
        distance: u16,
        left: Box<CompiledPhrase>,
        right: Box<CompiledPhrase>,
    },
}

fn eval_compiled(
    eval: &CompiledPhrase,
    per_probe_positions: &[Vec<u16>],
    out: &mut Vec<u16>,
    cancel: Option<&CancelToken>,
    work: &mut usize,
) -> Result<()> {
    out.clear();
    match eval {
        CompiledPhrase::Leaf(idx) => {
            let positions = &per_probe_positions[*idx];
            if cancel.is_none() {
                out.extend_from_slice(positions);
            } else {
                for chunk in positions.chunks(CANCEL_CHECK_INTERVAL) {
                    check_cancel(cancel)?;
                    out.extend_from_slice(chunk);
                    *work += chunk.len();
                }
            }
        }
        CompiledPhrase::Phrase {
            distance,
            left,
            right,
        } => {
            let mut lp = Vec::new();
            let mut rp = Vec::new();
            eval_compiled(left, per_probe_positions, &mut lp, cancel, work)?;
            eval_compiled(right, per_probe_positions, &mut rp, cancel, work)?;
            if lp.is_empty() || rp.is_empty() {
                return Ok(());
            }
            let (mut i, mut j) = (0usize, 0usize);
            while i < lp.len() && j < rp.len() {
                check_cancel_at(cancel, *work)?;
                *work += 1;
                let l = lp[i] & 0x3FFF;
                let r = rp[j] & 0x3FFF;
                let target = l.saturating_add(*distance);
                if r == target {
                    if out.last().copied() != Some(rp[j]) {
                        out.push(rp[j]);
                    }
                    j += 1;
                } else if r < target {
                    j += 1;
                } else {
                    i += 1;
                }
            }
        }
    }
    Ok(())
}

fn weight_default_score(packed: u16) -> f64 {
    match packed >> 14 {
        3 => 1.0,
        2 => 0.4,
        1 => 0.2,
        _ => 0.1,
    }
}

fn ts_rank_from_index_positions(
    positions_per_lex: &[&[u16]],
    rank_probe_indices: &[usize],
    cancel: Option<&CancelToken>,
    work: &mut usize,
) -> Result<f64> {
    let mut score = 0.0_f64;
    for &probe_index in rank_probe_indices {
        let positions = positions_per_lex[probe_index];
        if positions.is_empty() {
            continue;
        }
        let mut weight_sum = 0.0;
        for &position in positions {
            check_cancel_at(cancel, *work)?;
            *work += 1;
            weight_sum += weight_default_score(position);
        }
        let tf = (positions.len() as f64).ln_1p();
        score += weight_sum * (1.0 + tf);
    }
    Ok(score)
}

// The posting lists can reproduce an unweighted conjunction's rank only when
// they contain every ranking term. Preserve AST order and repeated terms: both
// affect scalar TS_RANK's accumulation, even though the WHERE probes are unique.
fn ts_rank_probe_indices(
    ast: &crate::fts::TsQueryAst,
    probe_entries: &[Vec<u8>],
    cancel: Option<&CancelToken>,
) -> Result<Option<Vec<usize>>> {
    use crate::fts::TsQueryAst;
    let mut pending = vec![ast];
    let mut indices = Vec::new();
    let mut work = 0;
    while let Some(node) = pending.pop() {
        check_cancel_at(cancel, work)?;
        work += 1;
        match node {
            TsQueryAst::Lexeme {
                lexeme,
                prefix: false,
                weight_mask: 0,
            } => match probe_entries.binary_search(lexeme) {
                Ok(index) => indices.push(index),
                Err(_) => return Ok(None),
            },
            TsQueryAst::And(left, right) => {
                pending.push(right);
                pending.push(left);
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(indices))
}

fn try_inverted_ts_rank_topk_with_read(
    rtx: &mut ReadTxn<'_>,
    table_schema: &TableSchema,
    stmt: &SelectStmt,
) -> Result<Option<ExecutionResult>> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    if !stmt.joins.is_empty()
        || !stmt.group_by.is_empty()
        || stmt.distinct
        || stmt.having.is_some()
        || stmt.from_subquery.is_some()
        || stmt.from_json_table.is_some()
        || has_any_window_function(stmt)
    {
        return Ok(None);
    }
    if stmt.where_clause.is_none() {
        return Ok(None);
    }
    let plan = crate::planner::plan_select_inverted(table_schema, &stmt.where_clause);
    let (idx_table, probe_entries, fts_col_idx, _kind) = match plan {
        crate::planner::ScanPlan::InvertedScan {
            kind: crate::types::InvertedKind::Fts { .. },
            idx_table,
            probe_entries,
            recheck_needed,
            column_idx,
            ..
        } if !recheck_needed => (
            idx_table,
            probe_entries,
            column_idx as usize,
            crate::types::InvertedKind::Fts { config_id: 0 },
        ),
        _ => return Ok(None),
    };
    // TEXT FTS indexes tokenize their source, but scalar TS_RANK requires a
    // TSVECTOR. Do not turn its type error into an index-only numeric result.
    if table_schema.columns[fts_col_idx].data_type != DataType::TsVector {
        return Ok(None);
    }
    let fts_col_name = table_schema.columns[fts_col_idx].name.to_ascii_lowercase();

    let pk_col_indices: Vec<usize> = table_schema
        .primary_key_columns
        .iter()
        .map(|&i| i as usize)
        .collect();
    enum OutCol {
        Pk,
        TsRank,
    }
    let mut out_cols: Vec<OutCol> = Vec::with_capacity(stmt.columns.len());
    let mut rank_output = None;
    let mut rank_probe_indices = Vec::new();
    for sc in &stmt.columns {
        let expr = match sc {
            SelectColumn::Expr { expr, .. } => expr,
            _ => return Ok(None),
        };
        match expr {
            Expr::Column(n) | Expr::QualifiedColumn { column: n, .. } => {
                let lower = n.to_ascii_lowercase();
                let schema_idx = match table_schema.column_index(&lower) {
                    Some(i) => i,
                    None => return Ok(None),
                };
                if !pk_col_indices.contains(&schema_idx) {
                    return Ok(None);
                }
                out_cols.push(OutCol::Pk);
            }
            Expr::Function { name, args, .. }
                if name.eq_ignore_ascii_case("ts_rank") && args.len() == 2 =>
            {
                if rank_output.is_some() || !crate::eval::is_statement_constant(&args[1]) {
                    return Ok(None);
                }
                let arg_col = match &args[0] {
                    Expr::Column(c) => c.to_ascii_lowercase(),
                    Expr::QualifiedColumn { column, .. } => column.to_ascii_lowercase(),
                    _ => return Ok(None),
                };
                if arg_col != fts_col_name {
                    return Ok(None);
                }
                let col_map = crate::eval::ColumnMap::new(&[]);
                let ctx = crate::eval::EvalCtx::new(&col_map, &[]).with_cancel(cancel);
                let q = match crate::eval::eval_expr(&args[1], &ctx) {
                    Ok(Value::TsQuery(b)) => b,
                    _ => return Ok(None),
                };
                let ast = match crate::fts::TsQueryAst::decode_with_cancel(&q, cancel) {
                    Ok(ast) => ast,
                    Err(error @ SqlError::Storage(citadel::Error::Interrupted)) => {
                        return Err(error)
                    }
                    // Let ordinary projection preserve lazy errors on empty
                    // results rather than reject a query before scanning.
                    Err(_) => return Ok(None),
                };
                rank_probe_indices = match ts_rank_probe_indices(&ast, &probe_entries, cancel)? {
                    Some(indices) => indices,
                    None => return Ok(None),
                };
                rank_output = Some(out_cols.len());
                out_cols.push(OutCol::TsRank);
            }
            _ => return Ok(None),
        }
    }
    let Some(rank_output) = rank_output else {
        return Ok(None);
    };

    if stmt.order_by.len() != 1 {
        return Ok(None);
    }
    let order = &stmt.order_by[0];
    let output_columns = build_output_columns(&stmt.columns, &table_schema.columns);
    let output_map = ColumnMap::new(&output_columns);
    if order_by_output_position(order, &output_map)? != Some(rank_output) {
        return Ok(None);
    }
    let out_col_names: Vec<_> = output_columns
        .into_iter()
        .map(|column| column.name)
        .collect();
    let limit = match stmt.limit.as_ref() {
        Some(expr) => eval_const_int(expr)?.max(0) as usize,
        None => return Ok(None),
    };
    if limit == 0 || stmt.offset.is_some() {
        return Ok(None);
    }
    let single_int_pk = pk_col_indices.len() == 1
        && table_schema.columns[pk_col_indices[0]].data_type == DataType::Integer;
    if !single_int_pk {
        return Ok(None);
    }

    struct Probe {
        pks: Vec<i64>,
        offs: Vec<u32>,
        data: Vec<u16>,
    }
    let mut probes: Vec<Probe> = Vec::with_capacity(probe_entries.len());
    for (entry_idx, entry) in probe_entries.iter().enumerate() {
        check_cancel_at(cancel, entry_idx)?;
        let mut prefix = entry.clone();
        prefix.push(0x1F);
        let mut p = Probe {
            pks: Vec::with_capacity(1024),
            offs: Vec::with_capacity(1025),
            data: Vec::with_capacity(2048),
        };
        p.offs.push(0);
        let mut scan_err: Option<SqlError> = None;
        rtx.table_scan_from_fast(&idx_table, &prefix, |key, value| {
            if !key.starts_with(&prefix) {
                return Ok(false);
            }
            match crate::encoding::decode_pk_integer(&key[prefix.len()..]) {
                Ok(id) => p.pks.push(id),
                Err(e) => {
                    scan_err = Some(e);
                    return Ok(false);
                }
            }
            let mut i = 0;
            while i + 2 <= value.len() {
                if i != 0 {
                    if let Err(e) = check_cancel_at(cancel, i / 2) {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                }
                p.data.push(u16::from_le_bytes([value[i], value[i + 1]]));
                i += 2;
            }
            p.offs.push(p.data.len() as u32);
            Ok(true)
        })
        .map_err(SqlError::Storage)?;
        if let Some(e) = scan_err {
            return Err(e);
        }
        if p.pks.is_empty() {
            return Ok(Some(ExecutionResult::Query(QueryResult {
                columns: out_col_names,
                rows: Vec::new(),
            })));
        }
        probes.push(p);
    }

    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    let driver_idx = probes
        .iter()
        .enumerate()
        .min_by_key(|(_, p)| p.pks.len())
        .map(|(i, _)| i)
        .unwrap();
    let driver = &probes[driver_idx];
    let limit = limit.min(driver.pks.len());
    let mut heap: BinaryHeap<Reverse<(u64, Reverse<i64>)>> = BinaryHeap::with_capacity(limit);
    let mut indices = vec![0usize; probes.len()];
    let mut positions_per_lex: Vec<&[u16]> = vec![&[]; probe_entries.len()];
    let mut rank_work = 0usize;

    'outer: for di in 0..driver.pks.len() {
        check_cancel_at(cancel, di)?;
        let pk = driver.pks[di];
        indices[driver_idx] = di;
        for (pi, probe) in probes.iter().enumerate() {
            if pi == driver_idx {
                continue;
            }
            while indices[pi] < probe.pks.len() && probe.pks[indices[pi]] < pk {
                indices[pi] += 1;
            }
            if indices[pi] >= probe.pks.len() || probe.pks[indices[pi]] != pk {
                continue 'outer;
            }
        }
        for (pi, probe) in probes.iter().enumerate() {
            let idx = indices[pi];
            let s = probe.offs[idx] as usize;
            let e = probe.offs[idx + 1] as usize;
            positions_per_lex[pi] = &probe.data[s..e];
        }
        let score = ts_rank_from_index_positions(
            &positions_per_lex,
            &rank_probe_indices,
            cancel,
            &mut rank_work,
        )?;
        // Scores here are finite and nonnegative, so their unsigned IEEE bits
        // have numeric order. Larger keys always mean better requested rank.
        let key = if order.descending {
            score.to_bits()
        } else {
            !score.to_bits()
        };
        let candidate = (key, Reverse(pk));
        if heap.len() < limit {
            heap.push(Reverse(candidate));
        } else if let Some(Reverse(worst)) = heap.peek() {
            if candidate > *worst {
                heap.pop();
                heap.push(Reverse(candidate));
            }
        }
    }

    let heap_rows: Vec<(u64, Reverse<i64>)> = heap.into_iter().map(|r| r.0).collect();
    let mut sorted_indices: Vec<usize> = (0..heap_rows.len()).collect();
    sort_indices_by(&mut sorted_indices, cancel, |a, b| {
        heap_rows[b].cmp(&heap_rows[a])
    })?;

    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(sorted_indices.len());
    for (row_idx, source_idx) in sorted_indices.into_iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let (key, Reverse(pk)) = heap_rows[source_idx];
        let score = f64::from_bits(if order.descending { key } else { !key });
        let mut row = Vec::with_capacity(out_cols.len());
        for col in &out_cols {
            match col {
                OutCol::Pk => row.push(Value::Integer(pk)),
                OutCol::TsRank => row.push(Value::Real(score)),
            }
        }
        rows.push(row);
    }
    check_cancel(cancel)?;
    Ok(Some(ExecutionResult::Query(QueryResult {
        columns: out_col_names,
        rows,
    })))
}

fn try_inverted_index_only_with_read(
    rtx: &mut ReadTxn<'_>,
    table_schema: &TableSchema,
    stmt: &SelectStmt,
) -> Result<Option<ExecutionResult>> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    if !stmt.joins.is_empty()
        || !stmt.group_by.is_empty()
        || stmt.distinct
        || stmt.having.is_some()
        || stmt.from_subquery.is_some()
        || stmt.from_json_table.is_some()
        || has_any_window_function(stmt)
    {
        return Ok(None);
    }
    let where_expr = match &stmt.where_clause {
        Some(e) => e,
        None => return Ok(None),
    };
    let plan = crate::planner::plan_select_inverted(table_schema, &stmt.where_clause);
    let (idx_table, probe_entries, phrase_ast) = match plan {
        crate::planner::ScanPlan::InvertedScan {
            kind: crate::types::InvertedKind::Fts { .. },
            idx_table,
            probe_entries,
            recheck_needed,
            recheck_expr,
            ..
        } => {
            let ast = fts_phrase_ast_from_predicate(&recheck_expr);
            if !recheck_needed {
                (idx_table, probe_entries, None)
            } else if let Some(ast) = ast {
                if crate::planner::fts_ast_is_pure_phrase(&ast) {
                    (idx_table, probe_entries, Some(ast))
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }
        crate::planner::ScanPlan::InvertedScan {
            idx_table,
            probe_entries,
            recheck_needed,
            ..
        } if !recheck_needed => (idx_table, probe_entries, None),
        _ => return Ok(None),
    };
    let pk_col_indices: Vec<usize> = table_schema
        .primary_key_columns
        .iter()
        .map(|&i| i as usize)
        .collect();
    let mut out_col_names: Vec<String> = Vec::with_capacity(stmt.columns.len());
    let mut out_col_to_pk_pos: Vec<usize> = Vec::with_capacity(stmt.columns.len());
    for sc in &stmt.columns {
        match sc {
            SelectColumn::Expr { expr, alias } => {
                let col_name = match expr {
                    Expr::Column(n) => n.clone(),
                    Expr::QualifiedColumn { column, .. } => column.clone(),
                    _ => return Ok(None),
                };
                let schema_idx = match table_schema.column_index(&col_name) {
                    Some(i) => i,
                    None => return Ok(None),
                };
                let pk_pos = match pk_col_indices.iter().position(|&i| i == schema_idx) {
                    Some(p) => p,
                    None => return Ok(None),
                };
                out_col_to_pk_pos.push(pk_pos);
                out_col_names.push(alias.clone().unwrap_or(col_name));
            }
            _ => return Ok(None),
        }
    }
    for order in &stmt.order_by {
        let col_name = match &order.expr {
            Expr::Column(n) => n,
            Expr::QualifiedColumn { column, .. } => column,
            _ => return Ok(None),
        };
        let schema_idx = match table_schema.column_index(col_name) {
            Some(i) => i,
            None => return Ok(None),
        };
        if !pk_col_indices.contains(&schema_idx) {
            return Ok(None);
        }
    }
    let _ = where_expr;

    let single_int_pk = pk_col_indices.len() == 1
        && table_schema.columns[pk_col_indices[0]].data_type == DataType::Integer;

    let mut int_acc: Option<Vec<i64>> = None;

    let acc = if let Some(ast) = phrase_ast.as_ref() {
        let phrase_lexemes = fts_phrase_lexemes(ast);
        let compiled = compile_phrase_eval(ast, &phrase_lexemes);

        if single_int_pk {
            struct Probe {
                pks: Vec<i64>,
                offs: Vec<u32>,
                data: Vec<u16>,
            }
            let mut probes2: Vec<(Vec<u8>, Probe)> = Vec::with_capacity(phrase_lexemes.len());
            for (entry_idx, entry) in phrase_lexemes.iter().enumerate() {
                check_cancel_at(cancel, entry_idx)?;
                let mut prefix = entry.clone();
                prefix.push(0x1F);
                let mut p = Probe {
                    pks: Vec::with_capacity(1024),
                    offs: Vec::with_capacity(1025),
                    data: Vec::with_capacity(2048),
                };
                p.offs.push(0);
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_from_fast(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match crate::encoding::decode_pk_integer(&key[prefix.len()..]) {
                        Ok(id) => p.pks.push(id),
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    let mut i = 0;
                    while i + 2 <= value.len() {
                        if i != 0 {
                            if let Err(e) = check_cancel_at(cancel, i / 2) {
                                scan_err = Some(e);
                                return Ok(false);
                            }
                        }
                        p.data.push(u16::from_le_bytes([value[i], value[i + 1]]));
                        i += 2;
                    }
                    p.offs.push(p.data.len() as u32);
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
                if p.pks.is_empty() {
                    return Ok(Some(ExecutionResult::Query(QueryResult {
                        columns: out_col_names,
                        rows: Vec::new(),
                    })));
                }
                probes2.push((entry.clone(), p));
            }
            let driver_idx = probes2
                .iter()
                .enumerate()
                .min_by_key(|(_, (_, p))| p.pks.len())
                .map(|(i, _)| i)
                .unwrap();
            let (driver_lex, driver) = probes2.swap_remove(driver_idx);
            let driver_phrase_idx = phrase_lexemes
                .iter()
                .position(|l| l == &driver_lex)
                .unwrap();
            let other_phrase_idx: Vec<usize> = probes2
                .iter()
                .map(|(lex, _)| phrase_lexemes.iter().position(|l| l == lex).unwrap())
                .collect();

            let mut matched: Vec<i64> = Vec::with_capacity(driver.pks.len() / 4);
            let mut indices = vec![0usize; probes2.len()];

            let two_lex: Option<(usize, usize, u16)> = match &compiled {
                CompiledPhrase::Phrase {
                    distance,
                    left,
                    right,
                } => match (left.as_ref(), right.as_ref()) {
                    (CompiledPhrase::Leaf(l), CompiledPhrase::Leaf(r)) => Some((*l, *r, *distance)),
                    _ => None,
                },
                _ => None,
            };

            if let Some((l_lex, r_lex, dist)) = two_lex {
                let l_is_driver = driver_phrase_idx == l_lex;
                let r_is_driver = driver_phrase_idx == r_lex;
                let l_pi = if l_is_driver {
                    usize::MAX
                } else {
                    other_phrase_idx.iter().position(|&p| p == l_lex).unwrap()
                };
                let r_pi = if r_is_driver {
                    usize::MAX
                } else {
                    other_phrase_idx.iter().position(|&p| p == r_lex).unwrap()
                };

                let mut position_work = 0usize;
                'outer: for di in 0..driver.pks.len() {
                    check_cancel_at(cancel, di)?;
                    let pk = driver.pks[di];
                    for (pi, (_, probe)) in probes2.iter().enumerate() {
                        while indices[pi] < probe.pks.len() && probe.pks[indices[pi]] < pk {
                            indices[pi] += 1;
                        }
                        if indices[pi] >= probe.pks.len() || probe.pks[indices[pi]] != pk {
                            continue 'outer;
                        }
                    }
                    let l_slice: &[u16] = if l_is_driver {
                        let s = driver.offs[di] as usize;
                        let e = driver.offs[di + 1] as usize;
                        &driver.data[s..e]
                    } else {
                        let probe = &probes2[l_pi].1;
                        let idx = indices[l_pi];
                        let s = probe.offs[idx] as usize;
                        let e = probe.offs[idx + 1] as usize;
                        &probe.data[s..e]
                    };
                    let r_slice: &[u16] = if r_is_driver {
                        let s = driver.offs[di] as usize;
                        let e = driver.offs[di + 1] as usize;
                        &driver.data[s..e]
                    } else {
                        let probe = &probes2[r_pi].1;
                        let idx = indices[r_pi];
                        let s = probe.offs[idx] as usize;
                        let e = probe.offs[idx + 1] as usize;
                        &probe.data[s..e]
                    };
                    let (mut i, mut j) = (0usize, 0usize);
                    while i < l_slice.len() && j < r_slice.len() {
                        check_cancel_at(cancel, position_work)?;
                        position_work += 1;
                        let l = l_slice[i] & 0x3FFF;
                        let r = r_slice[j] & 0x3FFF;
                        let target = l + dist;
                        if r == target {
                            matched.push(pk);
                            continue 'outer;
                        } else if r < target {
                            j += 1;
                        } else {
                            i += 1;
                        }
                    }
                }
            } else {
                let mut probe_positions: Vec<Vec<u16>> = vec![Vec::new(); phrase_lexemes.len()];
                let mut out_pos: Vec<u16> = Vec::new();
                let mut phrase_work = 0usize;
                'outer2: for di in 0..driver.pks.len() {
                    check_cancel_at(cancel, di)?;
                    let pk = driver.pks[di];
                    for (pi, (_, probe)) in probes2.iter().enumerate() {
                        while indices[pi] < probe.pks.len() && probe.pks[indices[pi]] < pk {
                            indices[pi] += 1;
                        }
                        if indices[pi] >= probe.pks.len() || probe.pks[indices[pi]] != pk {
                            continue 'outer2;
                        }
                    }
                    let dr_start = driver.offs[di] as usize;
                    let dr_end = driver.offs[di + 1] as usize;
                    probe_positions[driver_phrase_idx].clear();
                    probe_positions[driver_phrase_idx]
                        .extend_from_slice(&driver.data[dr_start..dr_end]);
                    for (pi, (_, probe)) in probes2.iter().enumerate() {
                        let idx = indices[pi];
                        let s = probe.offs[idx] as usize;
                        let e = probe.offs[idx + 1] as usize;
                        let lex_idx = other_phrase_idx[pi];
                        probe_positions[lex_idx].clear();
                        probe_positions[lex_idx].extend_from_slice(&probe.data[s..e]);
                    }
                    eval_compiled(
                        &compiled,
                        &probe_positions,
                        &mut out_pos,
                        cancel,
                        &mut phrase_work,
                    )?;
                    if !out_pos.is_empty() {
                        matched.push(pk);
                    }
                }
            }
            int_acc = Some(matched);
            Vec::new()
        } else {
            let mut per_probe: Vec<Vec<(Vec<u8>, Vec<u16>)>> =
                Vec::with_capacity(phrase_lexemes.len());
            for (entry_idx, entry) in phrase_lexemes.iter().enumerate() {
                check_cancel_at(cancel, entry_idx)?;
                let mut prefix = entry.clone();
                prefix.push(0x1F);
                let mut list: Vec<(Vec<u8>, Vec<u16>)> = Vec::new();
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_from_fast(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    let pk = key[prefix.len()..].to_vec();
                    let mut positions = Vec::with_capacity(value.len() / 2);
                    let mut i = 0;
                    while i + 2 <= value.len() {
                        if i != 0 {
                            if let Err(e) = check_cancel_at(cancel, i / 2) {
                                scan_err = Some(e);
                                return Ok(false);
                            }
                        }
                        positions.push(u16::from_le_bytes([value[i], value[i + 1]]));
                        i += 2;
                    }
                    list.push((pk, positions));
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
                if list.is_empty() {
                    return Ok(Some(ExecutionResult::Query(QueryResult {
                        columns: out_col_names,
                        rows: Vec::new(),
                    })));
                }
                per_probe.push(list);
            }
            per_probe = sort_lists_by_len(per_probe, cancel)?;
            let first = per_probe.remove(0);
            let mut candidates: Vec<(Vec<u8>, Vec<Vec<u16>>)> = first
                .into_iter()
                .map(|(pk, positions)| (pk, vec![positions]))
                .collect();
            for (probe_idx, other) in per_probe.into_iter().enumerate() {
                check_cancel_at(cancel, probe_idx)?;
                let mut out: Vec<(Vec<u8>, Vec<Vec<u16>>)> =
                    Vec::with_capacity(candidates.len().min(other.len()));
                let (mut i, mut j) = (0usize, 0usize);
                let mut merge_work = 0usize;
                while i < candidates.len() && j < other.len() {
                    check_cancel_at(cancel, merge_work)?;
                    merge_work += 1;
                    match candidates[i].0.cmp(&other[j].0) {
                        std::cmp::Ordering::Equal => {
                            let mut entry = std::mem::take(&mut candidates[i]);
                            entry.1.push(other[j].1.clone());
                            out.push(entry);
                            i += 1;
                            j += 1;
                        }
                        std::cmp::Ordering::Less => i += 1,
                        std::cmp::Ordering::Greater => j += 1,
                    }
                }
                candidates = out;
                if candidates.is_empty() {
                    break;
                }
            }
            let mut matched: Vec<Vec<u8>> = Vec::with_capacity(candidates.len());
            let mut out_positions: Vec<u16> = Vec::new();
            let mut phrase_work = 0usize;
            for (candidate_idx, (pk, per_probe_positions)) in candidates.into_iter().enumerate() {
                check_cancel_at(cancel, candidate_idx)?;
                eval_compiled(
                    &compiled,
                    &per_probe_positions,
                    &mut out_positions,
                    cancel,
                    &mut phrase_work,
                )?;
                if !out_positions.is_empty() {
                    matched.push(pk);
                }
            }
            matched
        }
    } else if single_int_pk {
        let mut lists: Vec<Vec<i64>> = Vec::with_capacity(probe_entries.len());
        for (entry_idx, entry) in probe_entries.iter().enumerate() {
            check_cancel_at(cancel, entry_idx)?;
            let mut prefix = entry.clone();
            prefix.push(0x1F);
            let mut list: Vec<i64> = Vec::with_capacity(1024);
            let mut scan_err: Option<SqlError> = None;
            rtx.table_scan_from_fast(&idx_table, &prefix, |key, _v| {
                if !key.starts_with(&prefix) {
                    return Ok(false);
                }
                match crate::encoding::decode_pk_integer(&key[prefix.len()..]) {
                    Ok(id) => list.push(id),
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                }
                Ok(true)
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            if list.is_empty() {
                return Ok(Some(ExecutionResult::Query(QueryResult {
                    columns: out_col_names,
                    rows: Vec::new(),
                })));
            }
            lists.push(list);
        }
        lists = sort_lists_by_len(lists, cancel)?;
        let mut acc = lists.remove(0);
        for (list_idx, other) in lists.into_iter().enumerate() {
            check_cancel_at(cancel, list_idx)?;
            let mut out: Vec<i64> = Vec::with_capacity(acc.len().min(other.len()));
            let (mut i, mut j) = (0usize, 0usize);
            let mut merge_work = 0usize;
            while i < acc.len() && j < other.len() {
                check_cancel_at(cancel, merge_work)?;
                merge_work += 1;
                match acc[i].cmp(&other[j]) {
                    std::cmp::Ordering::Equal => {
                        out.push(acc[i]);
                        i += 1;
                        j += 1;
                    }
                    std::cmp::Ordering::Less => i += 1,
                    std::cmp::Ordering::Greater => j += 1,
                }
            }
            acc = out;
            if acc.is_empty() {
                break;
            }
        }
        int_acc = Some(acc);
        Vec::new()
    } else {
        let mut lists: Vec<Vec<Vec<u8>>> = Vec::with_capacity(probe_entries.len());
        for (entry_idx, entry) in probe_entries.iter().enumerate() {
            check_cancel_at(cancel, entry_idx)?;
            let mut prefix = entry.clone();
            prefix.push(0x1F);
            let mut list: Vec<Vec<u8>> = Vec::new();
            rtx.table_scan_from_fast(&idx_table, &prefix, |key, _v| {
                if !key.starts_with(&prefix) {
                    return Ok(false);
                }
                list.push(key[prefix.len()..].to_vec());
                Ok(true)
            })
            .map_err(SqlError::Storage)?;
            if list.is_empty() {
                return Ok(Some(ExecutionResult::Query(QueryResult {
                    columns: out_col_names,
                    rows: Vec::new(),
                })));
            }
            lists.push(list);
        }
        lists = sort_lists_by_len(lists, cancel)?;
        let mut acc = lists.remove(0);
        for (list_idx, other) in lists.into_iter().enumerate() {
            check_cancel_at(cancel, list_idx)?;
            let mut out = Vec::with_capacity(acc.len().min(other.len()));
            let (mut i, mut j) = (0usize, 0usize);
            let mut merge_work = 0usize;
            while i < acc.len() && j < other.len() {
                check_cancel_at(cancel, merge_work)?;
                merge_work += 1;
                match acc[i].cmp(&other[j]) {
                    std::cmp::Ordering::Equal => {
                        out.push(std::mem::take(&mut acc[i]));
                        i += 1;
                        j += 1;
                    }
                    std::cmp::Ordering::Less => i += 1,
                    std::cmp::Ordering::Greater => j += 1,
                }
            }
            acc = out;
            if acc.is_empty() {
                break;
            }
        }
        acc
    };

    let num_pk_cols = pk_col_indices.len();
    let single_int_fast = num_pk_cols == 1
        && out_col_to_pk_pos.len() == 1
        && out_col_to_pk_pos[0] == 0
        && table_schema.columns[pk_col_indices[0]].data_type == DataType::Integer;
    let mut result_rows: Vec<Vec<Value>> = if let Some(ints) = int_acc.take() {
        let mut rows = Vec::with_capacity(ints.len());
        for (row_idx, id) in ints.into_iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            rows.push(vec![Value::Integer(id)]);
        }
        rows
    } else if single_int_fast {
        let mut rows = Vec::with_capacity(acc.len());
        for (row_idx, pk_bytes) in acc.iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            let id = decode_pk_integer(pk_bytes)?;
            rows.push(vec![Value::Integer(id)]);
        }
        rows
    } else {
        let mut rows = Vec::with_capacity(acc.len());
        for (row_idx, pk_bytes) in acc.iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            let pk_vals = decode_composite_key(pk_bytes, num_pk_cols)?;
            let mut out_row = Vec::with_capacity(out_col_to_pk_pos.len());
            for &pos in &out_col_to_pk_pos {
                out_row.push(pk_vals[pos].clone());
            }
            rows.push(out_row);
        }
        rows
    };

    if !stmt.order_by.is_empty() {
        let order_cols: Vec<(usize, bool)> = stmt
            .order_by
            .iter()
            .map(|o| {
                let col_name = match &o.expr {
                    Expr::Column(n) => n.clone(),
                    Expr::QualifiedColumn { column, .. } => column.clone(),
                    _ => unreachable!(),
                };
                let schema_idx = table_schema.column_index(&col_name).unwrap();
                let pk_pos = pk_col_indices
                    .iter()
                    .position(|&i| i == schema_idx)
                    .unwrap();
                let out_pos = out_col_to_pk_pos
                    .iter()
                    .position(|&p| p == pk_pos)
                    .unwrap_or(usize::MAX);
                (out_pos, o.descending)
            })
            .collect();
        if order_cols.iter().any(|&(p, _)| p == usize::MAX) {
            return Ok(None);
        }
        result_rows = sort_vec_by(result_rows, cancel, |a, b| {
            for &(pos, desc) in &order_cols {
                let cmp = a[pos].cmp(&b[pos]);
                if cmp != std::cmp::Ordering::Equal {
                    return if desc { cmp.reverse() } else { cmp };
                }
            }
            std::cmp::Ordering::Equal
        })?;
    }

    apply_offset_limit(&mut result_rows, stmt)?;

    check_cancel(cancel)?;

    Ok(Some(ExecutionResult::Query(QueryResult {
        columns: out_col_names,
        rows: result_rows,
    })))
}

pub(super) fn compute_scan_limit(stmt: &SelectStmt, table_schema: &TableSchema) -> Option<usize> {
    if !stmt.group_by.is_empty() || stmt.distinct || stmt.having.is_some() {
        return None;
    }
    // Pk-prefix ASC order matches tree order; index-order arms ignore the limit.
    if !stmt.order_by.is_empty() && !order_by_is_pk_prefix_asc(stmt, table_schema) {
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

fn order_by_is_pk_prefix_asc(stmt: &SelectStmt, table_schema: &TableSchema) -> bool {
    let [ob] = &stmt.order_by[..] else {
        return false;
    };
    if ob.descending {
        return false;
    }
    let Expr::Column(name) = &ob.expr else {
        return false;
    };
    let Some(&pk0) = table_schema.primary_key_columns.first() else {
        return false;
    };
    let Some(idx) = table_schema.column_index(name) else {
        return false;
    };
    idx as u16 == pk0 && table_schema.columns[idx].collation == Collation::Binary
}

/// The `SELECT COUNT(*)` shortcut's eligibility, with no counting done.
///
/// Split out so the strategy decision can be made without a transaction, and
/// so EXPLAIN and the executor read the same predicate rather than two copies.
pub(super) fn count_star_output_name(stmt: &SelectStmt) -> Option<String> {
    if stmt.columns.len() != 1
        || stmt.where_clause.is_some()
        || !stmt.group_by.is_empty()
        || stmt.having.is_some()
    {
        return None;
    }
    let SelectColumn::Expr { expr, alias } = &stmt.columns[0] else {
        return None;
    };
    if !matches!(expr, Expr::CountStar) {
        return None;
    }
    Some(alias.as_deref().unwrap_or("COUNT(*)").to_string())
}

pub(super) fn try_count_star_shortcut(
    stmt: &SelectStmt,
    get_count: impl FnOnce() -> Result<u64>,
) -> Result<Option<ExecutionResult>> {
    let Some(col_name) = count_star_output_name(stmt) else {
        return Ok(None);
    };
    let count = get_count()? as i64;
    let mut rows = vec![vec![Value::Integer(count)]];
    apply_offset_limit(&mut rows, stmt)?;
    Ok(Some(ExecutionResult::Query(QueryResult {
        columns: vec![col_name],
        rows,
    })))
}

fn apply_offset_limit(rows: &mut Vec<Vec<Value>>, stmt: &SelectStmt) -> Result<()> {
    if let Some(offset_expr) = &stmt.offset {
        let offset = eval_const_int(offset_expr)?.max(0) as usize;
        if offset < rows.len() {
            *rows = rows.split_off(offset);
        } else {
            rows.clear();
        }
    }
    if let Some(limit_expr) = &stmt.limit {
        rows.truncate(eval_const_int(limit_expr)?.max(0) as usize);
    }
    Ok(())
}

pub(super) enum StreamAgg {
    CountStar,
    Count(usize),
    Sum(usize),
    Avg(usize),
    Min(usize, Collation),
    Max(usize, Collation),
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
        interval_months: i32,
        interval_days: i32,
        interval_micros: i64,
        is_interval: bool,
    },
    Avg {
        sum: f64,
        count: i64,
        interval_months: i64,
        interval_days: i64,
        interval_micros: i128,
        is_interval: bool,
    },
    Min {
        current: Option<Value>,
        collation: Collation,
    },
    Max {
        current: Option<Value>,
        collation: Collation,
    },
}

fn numeric_aggregate_type_error(is_interval: bool, got: String) -> SqlError {
    SqlError::TypeMismatch {
        expected: if is_interval { "INTERVAL" } else { "numeric" }.into(),
        got,
    }
}

fn check_numeric_aggregate_family(
    raw: &RawColumn,
    has_values: bool,
    is_interval: bool,
) -> Result<()> {
    let got = match raw {
        RawColumn::Integer(_) if is_interval => "INTEGER",
        RawColumn::Real(_) if is_interval => "REAL",
        RawColumn::Interval { .. } if has_values && !is_interval => "INTERVAL",
        _ => return Ok(()),
    };
    Err(numeric_aggregate_type_error(is_interval, got.into()))
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
                interval_months: 0,
                interval_days: 0,
                interval_micros: 0,
                is_interval: false,
            },
            StreamAgg::Avg(_) => AggState::Avg {
                sum: 0.0,
                count: 0,
                interval_months: 0,
                interval_days: 0,
                interval_micros: 0,
                is_interval: false,
            },
            StreamAgg::Min(_, collation) => AggState::Min {
                current: None,
                collation: *collation,
            },
            StreamAgg::Max(_, collation) => AggState::Max {
                current: None,
                collation: *collation,
            },
        }
    }

    /// Fold `other` (a later shard in leaf order) into `self`. Only gate-admitted
    /// states reach here: counts, integer Sum (wrapping add is associative), and
    /// Min/Max over non-REAL (strict compare keeps the earlier value on ties).
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn merge(&mut self, other: AggState) {
        match (self, other) {
            (AggState::CountStar(a), AggState::CountStar(b)) => *a += b,
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            (
                AggState::Sum {
                    int_sum,
                    real_sum,
                    has_real,
                    all_null,
                    interval_months,
                    interval_days,
                    interval_micros,
                    is_interval,
                },
                AggState::Sum {
                    int_sum: b_int,
                    real_sum: b_real,
                    has_real: b_has_real,
                    all_null: b_all_null,
                    interval_months: b_months,
                    interval_days: b_days,
                    interval_micros: b_micros,
                    is_interval: b_is_interval,
                },
            ) => {
                // Total over every variant field; only the integer fields are
                // reachable under the gate.
                *int_sum += b_int;
                *real_sum += b_real;
                *has_real |= b_has_real;
                *all_null &= b_all_null;
                *interval_months = interval_months.saturating_add(b_months);
                *interval_days = interval_days.saturating_add(b_days);
                *interval_micros = interval_micros.saturating_add(b_micros);
                *is_interval |= b_is_interval;
            }
            (
                AggState::Min {
                    current: a,
                    collation,
                },
                AggState::Min {
                    current: b,
                    collation: b_collation,
                },
            ) => {
                debug_assert_eq!(*collation, b_collation);
                if let Some(bv) = b {
                    *a = Some(match a.take() {
                        None => bv,
                        Some(av) => {
                            if collation.cmp_value(&bv, &av).is_lt() {
                                bv
                            } else {
                                av
                            }
                        }
                    });
                }
            }
            (
                AggState::Max {
                    current: a,
                    collation,
                },
                AggState::Max {
                    current: b,
                    collation: b_collation,
                },
            ) => {
                debug_assert_eq!(*collation, b_collation);
                if let Some(bv) = b {
                    *a = Some(match a.take() {
                        None => bv,
                        Some(av) => {
                            if collation.cmp_value(&bv, &av).is_gt() {
                                bv
                            } else {
                                av
                            }
                        }
                    });
                }
            }
            // Avg is order-sensitive f64 accumulation, excluded by the gate;
            // mismatched pairs cannot occur (shards build from the same ops).
            _ => unreachable!("merge on non-parallel aggregate state"),
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
            AggState::Sum { is_interval, .. } | AggState::Avg { is_interval, .. } => {
                let raw = match val {
                    Value::Integer(value) => RawColumn::Integer(*value),
                    Value::Real(value) => RawColumn::Real(*value),
                    Value::Interval {
                        months,
                        days,
                        micros,
                    } => RawColumn::Interval {
                        months: *months,
                        days: *days,
                        micros: *micros,
                    },
                    Value::Null => RawColumn::Null,
                    _ => {
                        return Err(numeric_aggregate_type_error(
                            *is_interval,
                            val.data_type().to_string(),
                        ));
                    }
                };
                self.feed_raw(&raw)?;
            }
            AggState::Min {
                current: cur,
                collation,
            } => {
                if !val.is_null() {
                    *cur = Some(match cur.take() {
                        None => val.clone(),
                        Some(m) => {
                            if collation.cmp_value(val, &m).is_lt() {
                                val.clone()
                            } else {
                                m
                            }
                        }
                    });
                }
            }
            AggState::Max {
                current: cur,
                collation,
            } => {
                if !val.is_null() {
                    *cur = Some(match cur.take() {
                        None => val.clone(),
                        Some(m) => {
                            if collation.cmp_value(val, &m).is_gt() {
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
                interval_months,
                interval_days,
                interval_micros,
                is_interval,
            } => {
                check_numeric_aggregate_family(raw, !*all_null, *is_interval)?;
                match raw {
                    RawColumn::Integer(i) => {
                        *int_sum += i;
                        *all_null = false;
                    }
                    RawColumn::Real(r) => {
                        *real_sum += r;
                        *has_real = true;
                        *all_null = false;
                    }
                    RawColumn::Interval {
                        months,
                        days,
                        micros,
                    } => {
                        *interval_months = interval_months.saturating_add(*months);
                        *interval_days = interval_days.saturating_add(*days);
                        *interval_micros = interval_micros.saturating_add(*micros);
                        *all_null = false;
                        *is_interval = true;
                    }
                    RawColumn::Null => {}
                    _ => {
                        return Err(numeric_aggregate_type_error(
                            *is_interval,
                            "non-numeric".into(),
                        ));
                    }
                }
            }
            AggState::Avg {
                sum,
                count,
                interval_months,
                interval_days,
                interval_micros,
                is_interval,
            } => {
                check_numeric_aggregate_family(raw, *count != 0, *is_interval)?;
                match raw {
                    RawColumn::Integer(i) => {
                        *sum += *i as f64;
                        *count += 1;
                    }
                    RawColumn::Real(r) => {
                        *sum += r;
                        *count += 1;
                    }
                    RawColumn::Interval {
                        months,
                        days,
                        micros,
                    } => {
                        *interval_months += *months as i64;
                        *interval_days += *days as i64;
                        *interval_micros += *micros as i128;
                        *count += 1;
                        *is_interval = true;
                    }
                    RawColumn::Null => {}
                    _ => {
                        return Err(numeric_aggregate_type_error(
                            *is_interval,
                            "non-numeric".into(),
                        ));
                    }
                }
            }
            AggState::Min {
                current: cur,
                collation,
            } => {
                if !matches!(raw, RawColumn::Null) {
                    let val = raw.to_value();
                    *cur = Some(match cur.take() {
                        None => val,
                        Some(m) => {
                            if collation.cmp_value(&val, &m).is_lt() {
                                val
                            } else {
                                m
                            }
                        }
                    });
                }
            }
            AggState::Max {
                current: cur,
                collation,
            } => {
                if !matches!(raw, RawColumn::Null) {
                    let val = raw.to_value();
                    *cur = Some(match cur.take() {
                        None => val,
                        Some(m) => {
                            if collation.cmp_value(&val, &m).is_gt() {
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
                interval_months,
                interval_days,
                interval_micros,
                is_interval,
            } => {
                if all_null {
                    Value::Null
                } else if is_interval {
                    Value::Interval {
                        months: interval_months,
                        days: interval_days,
                        micros: interval_micros,
                    }
                } else if has_real {
                    Value::Real(real_sum + int_sum as f64)
                } else {
                    Value::Integer(int_sum)
                }
            }
            AggState::Avg {
                sum,
                count,
                interval_months,
                interval_days,
                interval_micros,
                is_interval,
            } => {
                if count == 0 {
                    Value::Null
                } else if is_interval {
                    Value::Interval {
                        months: (interval_months / count).clamp(i32::MIN as i64, i32::MAX as i64)
                            as i32,
                        days: (interval_days / count).clamp(i32::MIN as i64, i32::MAX as i64)
                            as i32,
                        micros: (interval_micros / count as i128) as i64,
                    }
                } else {
                    Value::Real(sum / count as f64)
                }
            }
            AggState::Min { current, .. } | AggState::Max { current, .. } => {
                current.unwrap_or(Value::Null)
            }
        }
    }
}

pub(super) struct StreamAggPlan {
    pub(super) ops: Vec<(StreamAgg, String)>,
    partial_ctx: Option<PartialDecodeCtx>,
    raw_targets: Vec<RawAggTarget>,
    num_pk_cols: usize,
    nonpk_agg_defaults: Vec<Option<Value>>,
    /// When `Some`, evaluates WHERE on raw column bytes without decoding the row.
    fast_pred: Option<FastPredicate>,
    /// Every aggregate is order-insensitive (see `AggState::merge`), so the
    /// no-WHERE scan may fan leaves across shards.
    #[cfg(not(target_arch = "wasm32"))]
    parallel_ok: bool,
}

/// The borrowed pieces of a [`StreamAggPlan`] that raw-row feeding needs;
/// shard tasks capture this instead of the whole plan.
#[derive(Clone, Copy)]
pub(super) struct RawFeed<'a> {
    raw_targets: &'a [RawAggTarget],
    num_pk_cols: usize,
    nonpk_agg_defaults: &'a [Option<Value>],
}

impl RawFeed<'_> {
    pub(super) fn feed(
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
                RawAggTarget::NonPk(idx) => match decode_stored_column_raw(value, *idx) {
                    Ok(Some(raw)) => raw,
                    Ok(None) => {
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
                    Err(e) => {
                        *scan_err = Some(e);
                        return false;
                    }
                },
            };
            if let Err(e) = states[i].feed_raw(&raw) {
                *scan_err = Some(e);
                return false;
            }
        }
        true
    }
}

/// Leaves per rayon shard: large enough to amortize task overhead, small
/// enough to balance across cores.
#[cfg(not(target_arch = "wasm32"))]
const LEAVES_PER_SHARD: usize = 32;
/// Below this many leaves the serial scan wins.
#[cfg(not(target_arch = "wasm32"))]
const MIN_PARALLEL_LEAVES: usize = 256;

/// Fan a no-WHERE streaming aggregation across rayon shards when the plan's
/// ops are order-insensitive and the table is large enough to pay for it.
/// `Ok(None)` means "run the serial scan".
#[cfg(not(target_arch = "wasm32"))]
fn try_parallel_stream_agg(
    rtx: &ReadTxn<'_>,
    plan: &StreamAggPlan,
    leaves: &citadel_txn::read_txn::LeafPages,
) -> Result<Option<Vec<AggState>>> {
    if !plan.parallel_ok || leaves.len() < MIN_PARALLEL_LEAVES || rayon::current_num_threads() < 2 {
        return Ok(None);
    }
    parallel_stream_agg_sharded(rtx, plan, leaves, LEAVES_PER_SHARD).map(Some)
}

#[cfg(target_arch = "wasm32")]
fn try_parallel_stream_agg(
    _rtx: &ReadTxn<'_>,
    _plan: &StreamAggPlan,
    _leaves: &citadel_txn::read_txn::LeafPages,
) -> Result<Option<Vec<AggState>>> {
    Ok(None)
}

/// Scan leaf chunks concurrently (each through its own shard scanner tied to
/// `rtx`'s snapshot) and fold the per-shard states in leaf order. Exposed with
/// an explicit shard size so tests can exercise multi-shard merging on small
/// tables; production dispatch goes through `try_parallel_stream_agg`.
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn parallel_stream_agg_sharded(
    rtx: &ReadTxn<'_>,
    plan: &StreamAggPlan,
    leaves: &citadel_txn::read_txn::LeafPages,
    leaves_per_shard: usize,
) -> Result<Vec<AggState>> {
    use rayon::prelude::*;

    let feed = plan.raw_feed();
    let ops = &plan.ops;
    let shard_states: Vec<Result<Vec<AggState>>> = leaves
        .par_chunks(leaves_per_shard.max(1))
        .map(|chunk| {
            let mut scanner = rtx.shard_scanner();
            let mut states: Vec<AggState> = ops.iter().map(|(op, _)| AggState::new(op)).collect();
            let mut scan_err: Option<SqlError> = None;
            scanner
                .scan_leaves(chunk, |key, value| {
                    feed.feed(key, value, &mut states, &mut scan_err)
                })
                .map_err(SqlError::Storage)?;
            match scan_err {
                Some(e) => Err(e),
                None => Ok(states),
            }
        })
        .collect();

    let mut merged: Option<Vec<AggState>> = None;
    for shard in shard_states {
        let shard = shard?;
        match &mut merged {
            None => merged = Some(shard),
            Some(acc) => {
                for (a, b) in acc.iter_mut().zip(shard) {
                    a.merge(b);
                }
            }
        }
    }
    Ok(merged.expect("at least one shard"))
}

impl StreamAggPlan {
    pub(super) fn try_new(stmt: &SelectStmt, table_schema: &TableSchema) -> Result<Option<Self>> {
        Self::try_new_with_cancel(stmt, table_schema, None)
    }

    fn try_new_with_cancel(
        stmt: &SelectStmt,
        table_schema: &TableSchema,
        cancel: Option<&CancelToken>,
    ) -> Result<Option<Self>> {
        if !stmt.group_by.is_empty() || stmt.having.is_some() || !stmt.joins.is_empty() {
            return Ok(None);
        }

        let col_map = table_schema.column_map();
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
                    distinct,
                } if args.len() == 1 => {
                    if *distinct {
                        return Ok(None);
                    }
                    let func = func_name.to_ascii_uppercase();
                    let col_idx = match resolve_simple_col(&args[0], col_map) {
                        Some(idx) => idx,
                        None => return Ok(None),
                    };
                    // Virtual generated columns are stored as NULL placeholders;
                    // the raw-bytes scan cannot compute them.
                    if matches!(
                        table_schema.columns[col_idx].generated_kind,
                        Some(crate::parser::GeneratedKind::Virtual)
                    ) {
                        return Ok(None);
                    }
                    match func.as_str() {
                        "COUNT" => ops.push((StreamAgg::Count(col_idx), name)),
                        "SUM" => ops.push((StreamAgg::Sum(col_idx), name)),
                        "AVG" => ops.push((StreamAgg::Avg(col_idx), name)),
                        "MIN" => ops.push((
                            StreamAgg::Min(col_idx, table_schema.columns[col_idx].collation),
                            name,
                        )),
                        "MAX" => ops.push((
                            StreamAgg::Max(col_idx, table_schema.columns[col_idx].collation),
                            name,
                        )),
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
                | StreamAgg::Min(i, _)
                | StreamAgg::Max(i, _) => Some(*i),
            })
            .collect();
        if let Some(ref where_expr) = stmt.where_clause {
            needed.extend(referenced_columns(where_expr, &table_schema.columns));
        }
        needed.sort_unstable();
        needed.dedup();

        let partial_ctx = if needed.len() < table_schema.columns.len() {
            Some(PartialDecodeCtx::new_with_cancel(
                table_schema,
                &needed,
                cancel,
            )?)
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
                | StreamAgg::Min(idx, _)
                | StreamAgg::Max(idx, _) => {
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
            .map(|t| -> Result<Option<Value>> {
                Ok(match t {
                    RawAggTarget::NonPk(phys_idx) => {
                        let schema_col = mapping[*phys_idx];
                        if schema_col == usize::MAX {
                            return Ok(None);
                        }
                        table_schema.columns[schema_col]
                            .default_expr
                            .as_ref()
                            .map(|expr| eval_const_expr_with_cancel(expr, cancel))
                            .transpose()?
                    }
                    _ => None,
                })
            })
            .collect::<Result<_>>()?;

        // Raw-bytes predicate is only safe when every agg is CountStar.
        let all_count_star = ops.iter().all(|(op, _)| matches!(op, StreamAgg::CountStar));
        let fast_pred = if all_count_star {
            stmt.where_clause
                .as_ref()
                .and_then(|expr| FastPredicate::try_new(expr, table_schema))
        } else {
            None
        };

        // Shard-mergeable ops only (see AggState::merge): AVG and REAL fold
        // order-sensitively (f64, NaN compares Equal), INTERVAL saturates.
        // Defaults fed for pre-ALTER rows join the fold: same bounds apply.
        #[cfg(not(target_arch = "wasm32"))]
        let parallel_ok = ops
            .iter()
            .zip(&nonpk_agg_defaults)
            .all(|((op, _), default)| {
                let default_ok = matches!(
                    default,
                    None | Some(
                        Value::Null
                            | Value::Integer(_)
                            | Value::Text(_)
                            | Value::Blob(_)
                            | Value::Boolean(_)
                            | Value::Time(_)
                            | Value::Date(_)
                            | Value::Timestamp(_)
                    )
                );
                match op {
                    StreamAgg::CountStar | StreamAgg::Count(_) => true,
                    StreamAgg::Sum(idx) => {
                        table_schema.columns[*idx].data_type == DataType::Integer
                            && matches!(default, None | Some(Value::Null | Value::Integer(_)))
                    }
                    StreamAgg::Min(idx, _) | StreamAgg::Max(idx, _) => {
                        matches!(
                            table_schema.columns[*idx].data_type,
                            DataType::Integer
                                | DataType::Text
                                | DataType::Blob
                                | DataType::Boolean
                                | DataType::Time
                                | DataType::Date
                                | DataType::Timestamp
                        ) && default_ok
                    }
                    StreamAgg::Avg(_) => false,
                }
            });

        Ok(Some(Self {
            ops,
            partial_ctx,
            raw_targets,
            num_pk_cols,
            nonpk_agg_defaults,
            fast_pred,
            #[cfg(not(target_arch = "wasm32"))]
            parallel_ok,
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
        cancel: Option<&CancelToken>,
    ) -> bool {
        if let Some(ref pred) = self.fast_pred {
            match pred.matches_raw(key, value) {
                Ok(true) => {
                    for state in states.iter_mut() {
                        if let AggState::CountStar(ref mut c) = state {
                            *c += 1;
                        }
                    }
                    return true;
                }
                Ok(false) => return true,
                Err(e) => {
                    *scan_err = Some(e);
                    return false;
                }
            }
        }

        let row = match &self.partial_ctx {
            Some(ctx) => match ctx.decode_with_cancel(key, value, cancel) {
                Ok(r) => r,
                Err(e) => {
                    *scan_err = Some(e);
                    return false;
                }
            },
            None => match decode_full_row_with_cancel(table_schema, key, value, cancel) {
                Ok(r) => r,
                Err(e) => {
                    *scan_err = Some(e);
                    return false;
                }
            },
        };

        if let Some(expr) = where_clause {
            match eval_expr(expr, &EvalCtx::new(col_map, &row).with_cancel(cancel)) {
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
                | StreamAgg::Min(idx, _)
                | StreamAgg::Max(idx, _) => &row[*idx],
            };
            if let Err(e) = states[i].feed_val(val) {
                *scan_err = Some(e);
                return false;
            }
        }
        true
    }

    pub(super) fn raw_feed(&self) -> RawFeed<'_> {
        RawFeed {
            raw_targets: &self.raw_targets,
            num_pk_cols: self.num_pk_cols,
            nonpk_agg_defaults: &self.nonpk_agg_defaults,
        }
    }

    pub(super) fn feed_row_raw(
        &self,
        key: &[u8],
        value: &[u8],
        states: &mut [AggState],
        scan_err: &mut Option<SqlError>,
    ) -> bool {
        self.raw_feed().feed(key, value, states, scan_err)
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
    group_default: Option<i64>,
    num_pk_cols: usize,
    agg_ops: Vec<StreamAgg>,
    raw_targets: Vec<RawAggTarget>,
    nonpk_agg_defaults: Vec<Option<Value>>,
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
            || stmt.offset.is_some()
            || stmt.distinct
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

        let col_map = schema.column_map();

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
        if matches!(
            schema.columns[group_col_idx].generated_kind,
            Some(crate::parser::GeneratedKind::Virtual)
        ) {
            return Ok(None);
        }

        let non_pk = schema.non_pk_indices();
        let enc_pos = schema.encoding_positions();
        let nonpk_default = |col_idx: usize| -> Option<Option<Value>> {
            let expr = schema.columns[col_idx].default_expr.as_ref();
            if expr.is_some_and(|expr| volatile_function_in_expr(expr).is_some()) {
                return None;
            }
            expr.map(eval_const_expr).transpose().ok()
        };
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
        let group_default = if matches!(group_target, RawAggTarget::NonPk(_)) {
            match nonpk_default(group_col_idx) {
                Some(Some(Value::Integer(value))) => Some(value),
                Some(None) | Some(Some(Value::Null)) => None,
                _ => return Ok(None),
            }
        } else {
            None
        };

        let mut agg_ops = Vec::new();
        let mut raw_targets = Vec::new();
        let mut nonpk_agg_defaults = Vec::new();
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

            if let Some(idx) = resolve_simple_col(expr, col_map) {
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
                    nonpk_agg_defaults.push(None);
                    output.push((GroupByOutputCol::Agg(agg_idx), name));
                }
                Expr::Function {
                    name: func_name,
                    args,
                    distinct,
                } if args.len() == 1 => {
                    if *distinct {
                        return Ok(None);
                    }
                    let func = func_name.to_ascii_uppercase();
                    let col_idx = match resolve_simple_col(&args[0], col_map) {
                        Some(idx) => idx,
                        None => return Ok(None),
                    };
                    if matches!(
                        schema.columns[col_idx].generated_kind,
                        Some(crate::parser::GeneratedKind::Virtual)
                    ) {
                        return Ok(None);
                    }
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
                    let default = match &target {
                        RawAggTarget::Pk(_)
                            if schema.columns[col_idx].data_type != DataType::Integer =>
                        {
                            return Ok(None);
                        }
                        RawAggTarget::NonPk(_) => match nonpk_default(col_idx) {
                            Some(default) => default,
                            None => return Ok(None),
                        },
                        _ => None,
                    };
                    let agg_idx = agg_ops.len();
                    match func.as_str() {
                        "COUNT" => agg_ops.push(StreamAgg::Count(col_idx)),
                        "SUM" => agg_ops.push(StreamAgg::Sum(col_idx)),
                        "AVG" => agg_ops.push(StreamAgg::Avg(col_idx)),
                        "MIN" => {
                            agg_ops.push(StreamAgg::Min(col_idx, schema.columns[col_idx].collation))
                        }
                        "MAX" => {
                            agg_ops.push(StreamAgg::Max(col_idx, schema.columns[col_idx].collation))
                        }
                        _ => return Ok(None),
                    }
                    raw_targets.push(target);
                    nonpk_agg_defaults.push(default);
                    output.push((GroupByOutputCol::Agg(agg_idx), name));
                }
                _ => return Ok(None),
            }
        }

        Ok(Some(Self {
            group_target,
            group_default,
            num_pk_cols: schema.primary_key_columns.len(),
            agg_ops,
            raw_targets,
            nonpk_agg_defaults,
            output,
            where_pred,
        }))
    }

    pub(super) fn execute_scan(
        &self,
        cancel: Option<&CancelToken>,
        scan: impl FnOnce(
            &mut dyn FnMut(&[u8], &[u8]) -> bool,
        ) -> std::result::Result<(), citadel::Error>,
    ) -> Result<ExecutionResult> {
        check_cancel(cancel)?;
        let mut groups: FxHashMap<i64, Vec<AggState>> = FxHashMap::default();
        let mut null_group: Option<Vec<AggState>> = None;
        let mut scan_err: Option<SqlError> = None;
        let raw_feed = RawFeed {
            raw_targets: &self.raw_targets,
            num_pk_cols: self.num_pk_cols,
            nonpk_agg_defaults: &self.nonpk_agg_defaults,
        };

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
                RawAggTarget::NonPk(idx) => match decode_stored_column_raw(value, *idx) {
                    Ok(Some(RawColumn::Integer(i))) => Some(i),
                    Ok(Some(RawColumn::Null)) => None,
                    Ok(None) => self.group_default,
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

            raw_feed.feed(key, value, states, &mut scan_err)
        })
        .map_err(SqlError::Storage)?;

        if let Some(e) = scan_err {
            return Err(e);
        }
        check_cancel(cancel)?;

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
        for (group_idx, (group_key, states)) in groups.into_iter().enumerate() {
            check_cancel_at(cancel, group_idx)?;
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
        check_cancel(cancel)?;

        Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: result_rows,
        }))
    }
}

/// Streaming DISTINCT: extract only needed columns from raw scan, dedup inline.
fn try_streaming_distinct_with_read(
    rtx: &mut ReadTxn<'_>,
    stmt: &SelectStmt,
    table_schema: &TableSchema,
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

    let col_map = table_schema.column_map();
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let num_pk_cols = table_schema.primary_key_columns.len();

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
        let col_idx = match resolve_simple_col(expr, col_map) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        // The dedup key below is raw stored bytes, which cannot express a collation that
        // calls two spellings equal. The general path folds the key instead, so leave
        // collated columns to it rather than returning both spellings as distinct.
        if table_schema.columns[col_idx].collation != crate::types::Collation::Binary {
            return Ok(None);
        }
        let target = if let Some(pk_pos) = table_schema
            .primary_key_columns
            .iter()
            .position(|&i| i as usize == col_idx)
        {
            // The dedup key below is the whole encoded row key, which identifies the selected
            // column only when the primary key has one. A composite key would make every row
            // unique and stop deduplicating, so leave those to the general DISTINCT path.
            if table_schema.primary_key_columns.len() > 1 {
                return Ok(None);
            }
            RawAggTarget::Pk(pk_pos)
        } else {
            let nonpk_order = non_pk.iter().position(|&i| i == col_idx).unwrap();
            RawAggTarget::NonPk(enc_pos[nonpk_order] as usize)
        };
        targets.push(target);
        col_names.push(name);
    }

    let lower_name = &table_schema.name;
    let mut seen: rustc_hash::FxHashSet<Vec<u8>> = rustc_hash::FxHashSet::default();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut scan_err: Option<SqlError> = None;
    let mut raw_key_buf: Vec<u8> = Vec::with_capacity(64);

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
        if seen.contains(raw_key_buf.as_slice()) {
            return true;
        }
        seen.insert(raw_key_buf.clone());
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

pub(super) trait LateralIo {
    fn exec_select(&mut self, schema: &SchemaManager, sq: &SelectQuery) -> Result<QueryResult>;
    fn scan_table(
        &mut self,
        schema: &SchemaManager,
        name: &str,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)>;
}

pub(super) struct ReadHeldIo<'a, 'db: 'a> {
    pub rtx: &'a mut ReadTxn<'db>,
}

impl LateralIo for ReadHeldIo<'_, '_> {
    fn exec_select(&mut self, schema: &SchemaManager, sq: &SelectQuery) -> Result<QueryResult> {
        match super::cte::exec_select_query_with_read(self.rtx, schema, sq)? {
            ExecutionResult::Query(qr) => Ok(qr),
            _ => Err(SqlError::Plan("expected Query result".into())),
        }
    }
    fn scan_table(
        &mut self,
        schema: &SchemaManager,
        name: &str,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        super::scan_table_with_read_or_view(self.rtx, schema, name)
    }
}

pub(super) struct WriteIo<'a, 'b> {
    pub wtx: &'a mut citadel_txn::write_txn::WriteTxn<'b>,
}

impl LateralIo for WriteIo<'_, '_> {
    fn exec_select(&mut self, schema: &SchemaManager, sq: &SelectQuery) -> Result<QueryResult> {
        match super::cte::exec_select_query_in_txn(self.wtx, schema, sq)? {
            ExecutionResult::Query(qr) => Ok(qr),
            _ => Err(SqlError::Plan("expected Query result".into())),
        }
    }
    fn scan_table(
        &mut self,
        schema: &SchemaManager,
        name: &str,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        super::scan_table_write_or_view(self.wtx, schema, name)
    }
}

fn has_lateral(stmt: &SelectStmt) -> bool {
    stmt.joins
        .iter()
        .any(|j| j.subquery.as_ref().is_some_and(|s| s.lateral))
}

fn has_non_lateral_derived(stmt: &SelectStmt) -> bool {
    let from_has = stmt.from_subquery.as_ref().is_some_and(|s| !s.lateral);
    let join_has = stmt
        .joins
        .iter()
        .any(|j| j.subquery.as_ref().is_some_and(|s| !s.lateral));
    from_has || join_has
}

/// A derived table's rows keep the collations of the columns its query selected, so reading
/// a NOCASE column through `(SELECT ...) AS x` compares the way reading it directly does.
fn materialize_derived(
    schema: &SchemaManager,
    ctes: &CteContext,
    derived: &DerivedTable,
    io: &mut dyn LateralIo,
) -> Result<CteRows> {
    let result = io.exec_select(schema, &derived.query)?;
    let collations =
        super::dml::query_output_collations(schema, ctes, &derived.query, result.columns.len());
    Ok(CteRows::new(result, collations))
}

fn exec_select_with_srf_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
    cancel: Option<&CancelToken>,
) -> Result<ExecutionResult> {
    let args_exprs = stmt
        .from_args
        .as_ref()
        .expect("from_args present when exec_select_with_srf called");

    let upper_name = stmt.from.to_ascii_uppercase();
    let (columns, rows) = match upper_name.as_str() {
        "JSONB_POPULATE_RECORD" | "JSONB_POPULATE_RECORDSET" => {
            populate_record_dispatch(&upper_name, args_exprs, schema, cancel)?
        }
        _ => {
            let col_map = ColumnMap::new(&[]);
            let mut arg_values = Vec::with_capacity(args_exprs.len());
            for (arg_idx, expr) in args_exprs.iter().enumerate() {
                check_cancel_at(cancel, arg_idx)?;
                arg_values.push(eval_expr(
                    expr,
                    &EvalCtx::new(&col_map, &[]).with_cancel(cancel),
                )?);
            }
            crate::json::dispatch_srf_with_cancel(&stmt.from, &arg_values, cancel)?
        }
    };

    let alias = stmt
        .from_alias
        .clone()
        .unwrap_or_else(|| stmt.from.to_ascii_lowercase());

    let mut new_ctes = ctes.clone();
    // The columns are invented by the source rather than read from a relation, so none of
    // them carries a collation.
    new_ctes.insert(
        alias.to_ascii_lowercase(),
        CteRows::binary(QueryResult { columns, rows }).shared(),
    );

    let mut new_stmt = stmt.clone();
    new_stmt.from = alias;
    new_stmt.from_args = None;
    exec_select_with_read(rtx, schema, &new_stmt, &new_ctes)
}

fn populate_record_dispatch(
    upper_name: &str,
    args_exprs: &[Expr],
    schema: &SchemaManager,
    cancel: Option<&CancelToken>,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    if args_exprs.len() != 2 {
        return Err(SqlError::InvalidValue(format!(
            "{upper_name} requires 2 arguments: NULL::table_type, jsonb"
        )));
    }
    let table_name = match &args_exprs[0] {
        Expr::TypedNullRecord(name) => name,
        _ => {
            return Err(SqlError::InvalidValue(format!(
                "{upper_name}: first argument must be NULL::table_type"
            )))
        }
    };
    let target_schema = schema
        .get(&table_name.to_ascii_lowercase())
        .ok_or_else(|| {
            SqlError::TableNotFound(format!("row type '{table_name}' (used in {upper_name})"))
        })?;
    let col_map = ColumnMap::new(&[]);
    let jsonb_val = eval_expr(
        &args_exprs[1],
        &EvalCtx::new(&col_map, &[]).with_cancel(cancel),
    )?;
    let columns: Vec<String> = target_schema
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    if jsonb_val.is_null() {
        return Ok((columns, vec![]));
    }
    let j = crate::json::value_to_serde_with_cancel(&jsonb_val, cancel)?;
    let rows = match upper_name {
        "JSONB_POPULATE_RECORD" => {
            let obj = j.as_object().ok_or_else(|| {
                SqlError::InvalidValue("jsonb_populate_record requires JSON object".into())
            })?;
            vec![crate::json::populate_record_row_with_cancel(
                obj,
                &target_schema.columns,
                cancel,
            )?]
        }
        "JSONB_POPULATE_RECORDSET" => {
            let arr = j.as_array().ok_or_else(|| {
                SqlError::InvalidValue("jsonb_populate_recordset requires JSON array".into())
            })?;
            arr.iter()
                .enumerate()
                .map(|(row_idx, elem)| {
                    check_cancel_at(cancel, row_idx)?;
                    let obj = elem.as_object().ok_or_else(|| {
                        SqlError::InvalidValue(
                            "jsonb_populate_recordset array elements must be objects".into(),
                        )
                    })?;
                    crate::json::populate_record_row_with_cancel(
                        obj,
                        &target_schema.columns,
                        cancel,
                    )
                })
                .collect::<Result<Vec<_>>>()?
        }
        _ => unreachable!(),
    };
    Ok((columns, rows))
}

fn exec_select_with_json_table_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
    cancel: Option<&CancelToken>,
) -> Result<ExecutionResult> {
    let spec = stmt
        .from_json_table
        .as_ref()
        .expect("from_json_table present when exec_select_with_json_table called");
    let col_map = ColumnMap::new(&[]);
    let source_val = eval_expr(
        &spec.source,
        &EvalCtx::new(&col_map, &[]).with_cancel(cancel),
    )?;
    let (columns, rows) =
        crate::json::materialize_json_table_with_cancel(&source_val, spec, cancel)?;

    let alias = stmt.from_alias.clone().unwrap_or_else(|| stmt.from.clone());
    let mut new_ctes = ctes.clone();
    // The columns are invented by the source rather than read from a relation, so none of
    // them carries a collation.
    new_ctes.insert(
        alias.to_ascii_lowercase(),
        CteRows::binary(QueryResult { columns, rows }).shared(),
    );

    let mut new_stmt = stmt.clone();
    new_stmt.from = alias;
    new_stmt.from_json_table = None;
    exec_select_with_read(rtx, schema, &new_stmt, &new_ctes)
}

fn exec_select_with_derived_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let mut new_ctes = ctes.clone();
    let mut new_stmt = stmt.clone();

    {
        let mut io = ReadHeldIo { rtx: &mut *rtx };

        if let Some(d) = stmt.from_subquery.as_ref() {
            let qr = materialize_derived(schema, &new_ctes, d, &mut io)?;
            new_ctes.insert(d.alias.to_ascii_lowercase(), qr.shared());
            new_stmt.from = d.alias.clone();
            new_stmt.from_alias = None;
            new_stmt.from_subquery = None;
        }
        for j in new_stmt.joins.iter_mut() {
            if let Some(d) = j.subquery.take() {
                let qr = materialize_derived(schema, &new_ctes, &d, &mut io)?;
                new_ctes.insert(d.alias.to_ascii_lowercase(), qr.shared());
                j.table = TableRef {
                    name: d.alias.clone(),
                    alias: None,
                    args: None,
                };
            }
        }
    }

    exec_select_with_read(rtx, schema, &new_stmt, &new_ctes)
}

fn exec_select_lateral_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let cancel = rtx.cancel_token().cloned();
    let mut io = ReadHeldIo { rtx };
    exec_select_lateral_with_io(schema, stmt, ctes, &mut io, cancel.as_ref())
}

pub(super) fn exec_select_lateral_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let cancel = wtx.cancel_token().cloned();
    let mut io = WriteIo { wtx };
    exec_select_lateral_with_io(schema, stmt, ctes, &mut io, cancel.as_ref())
}

fn exec_select_lateral_with_io(
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
    io: &mut dyn LateralIo,
    cancel: Option<&CancelToken>,
) -> Result<ExecutionResult> {
    check_cancel(cancel)?;
    if !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || stmt.distinct
        || stmt
            .columns
            .iter()
            .any(|c| matches!(c, SelectColumn::Expr { expr, .. } if is_aggregate_expr(expr)))
    {
        return Err(SqlError::Unsupported(
            "GROUP BY / HAVING / DISTINCT / aggregates with LATERAL".into(),
        ));
    }

    let mut new_ctes = ctes.clone();
    let mut from_name = stmt.from.clone();
    let mut from_alias = stmt.from_alias.clone();
    if let Some(d) = stmt.from_subquery.as_ref() {
        if d.lateral {
            return Err(SqlError::Unsupported(
                "LATERAL is not allowed as the first FROM item".into(),
            ));
        }
        let qr = materialize_derived(schema, &new_ctes, d, io)?;
        new_ctes.insert(d.alias.to_ascii_lowercase(), qr.shared());
        from_name = d.alias.clone();
        from_alias = None;
    }

    let (outer_schema, mut outer_rows) = match new_ctes.get(&from_name.to_ascii_lowercase()) {
        Some(cte) => (
            super::cte::build_cte_schema(&from_name, cte),
            super::clone_cte_rows_with_cancel(&cte.result.rows, cancel)?,
        ),
        None => io.scan_table(schema, &from_name)?,
    };
    let outer_alias_str = super::join::table_alias_or_name(&from_name, &from_alias);

    let mut combined_cols: Vec<ColumnDef> =
        super::join::build_joined_columns(&[(outer_alias_str.clone(), &outer_schema)]);
    let mut current_alias = outer_alias_str;

    for join in &stmt.joins {
        let derived = join.subquery.as_ref().ok_or_else(|| {
            SqlError::Plan("exec_select_lateral encountered non-subquery join".into())
        })?;
        if !derived.lateral {
            let qr = materialize_derived(schema, &new_ctes, derived, io)?;
            new_ctes.insert(derived.alias.to_ascii_lowercase(), qr.shared());
            current_alias = derived.alias.clone();
            let mini = SelectStmt {
                columns: vec![SelectColumn::AllColumns],
                from: format!("__lateral_outer_{}", join.table.name),
                from_alias: None,
                from_subquery: None,
                from_args: None,
                from_json_table: None,
                joins: vec![JoinClause {
                    join_type: join.join_type,
                    table: TableRef {
                        name: derived.alias.clone(),
                        alias: None,
                        args: None,
                    },
                    subquery: None,
                    on_clause: join.on_clause.clone(),
                }],
                distinct: false,
                where_clause: None,
                order_by: vec![],
                limit: None,
                offset: None,
                group_by: vec![],
                having: None,
            };
            let outer_qr = CteRows::new(
                QueryResult {
                    columns: combined_cols.iter().map(|c| c.name.clone()).collect(),
                    rows: std::mem::take(&mut outer_rows),
                },
                combined_cols.iter().map(|c| c.collation).collect(),
            );
            new_ctes.insert(mini.from.clone(), outer_qr.shared());
            let qr = match super::exec_select_join_with_ctes(
                &mini,
                &new_ctes,
                &mut |n| io.scan_table(schema, n),
                cancel,
            )? {
                ExecutionResult::Query(qr) => qr,
                _ => unreachable!(),
            };
            outer_rows = qr.rows;
            combined_cols = qr
                .columns
                .iter()
                .enumerate()
                .map(|(i, name)| ColumnDef {
                    name: name.clone(),
                    data_type: DataType::Null,
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
            continue;
        }

        let outer_col_map = ColumnMap::new(&combined_cols);

        if let Some(fast) = try_lateral_decorrelated(
            schema,
            derived,
            &combined_cols,
            &outer_col_map,
            &outer_rows,
            join.join_type,
            join.on_clause.as_ref(),
            io,
            cancel,
        )? {
            outer_rows = fast.0;
            let alias_lc = derived.alias.to_ascii_lowercase();
            let qualified: Vec<String> = fast.1.iter().map(|n| format!("{alias_lc}.{n}")).collect();
            combined_cols = extend_lateral_cols(&combined_cols, &qualified)
                .into_iter()
                .enumerate()
                .map(|(i, mut c)| {
                    c.position = i as u16;
                    c
                })
                .collect();
            current_alias = derived.alias.clone();
            continue;
        }

        let mut new_rows: Vec<Vec<Value>> = Vec::new();
        let mut probe_columns: Vec<String> = Vec::new();
        let mut combined_col_map: Option<ColumnMap> = None;

        let mut expansion_work = 0usize;
        for (outer_idx, outer_row) in outer_rows.drain(..).enumerate() {
            check_cancel_at(cancel, outer_idx)?;
            let bound_query = bind_query_with_outer(&derived.query, &outer_row, &outer_col_map)?;
            let inner_qr = io.exec_select(schema, &bound_query)?;
            check_cancel(cancel)?;
            if probe_columns.is_empty() {
                probe_columns = inner_qr.columns.clone();
                if join.on_clause.is_some() {
                    combined_col_map = Some(ColumnMap::new(&extend_lateral_cols(
                        &combined_cols,
                        &probe_columns,
                    )));
                }
            }
            let inner_count = inner_qr.columns.len();
            let on_filter_needed = join.on_clause.is_some();
            let mut matched = false;
            for inner_row in &inner_qr.rows {
                check_cancel_at(cancel, expansion_work)?;
                expansion_work += 1;
                let mut combined = outer_row.clone();
                combined.extend(inner_row.iter().cloned());
                if on_filter_needed {
                    let on = join.on_clause.as_ref().unwrap();
                    let cm = combined_col_map.as_ref().unwrap();
                    if !is_truthy(&eval_expr(
                        on,
                        &EvalCtx::new(cm, &combined).with_cancel(cancel),
                    )?) {
                        continue;
                    }
                }
                matched = true;
                new_rows.push(combined);
            }
            if !matched && matches!(join.join_type, JoinType::Left) {
                let mut combined = outer_row;
                combined.resize(combined.len() + inner_count, Value::Null);
                new_rows.push(combined);
            }
        }

        let alias_lc = derived.alias.to_ascii_lowercase();
        let qualified: Vec<String> = probe_columns
            .iter()
            .map(|n| format!("{alias_lc}.{n}"))
            .collect();
        combined_cols = extend_lateral_cols(&combined_cols, &qualified)
            .into_iter()
            .enumerate()
            .map(|(i, mut c)| {
                c.position = i as u16;
                c
            })
            .collect();
        outer_rows = new_rows;
        current_alias = derived.alias.clone();
        check_cancel(cancel)?;
    }

    let clean_stmt = SelectStmt {
        where_clause: stmt.where_clause.clone(),
        columns: stmt.columns.clone(),
        from: current_alias,
        from_alias: None,
        from_subquery: None,
        from_args: None,
        from_json_table: None,
        joins: vec![],
        distinct: stmt.distinct,
        order_by: stmt.order_by.clone(),
        limit: stmt.limit.clone(),
        offset: stmt.offset.clone(),
        group_by: stmt.group_by.clone(),
        having: stmt.having.clone(),
    };
    process_select(
        outer_rows,
        SelectCtx::new(&combined_cols, &clean_stmt, cancel),
    )
}

type LateralRows = (Vec<Vec<Value>>, Vec<String>);

#[allow(clippy::too_many_arguments)]
fn try_lateral_decorrelated(
    schema: &SchemaManager,
    derived: &DerivedTable,
    outer_cols: &[ColumnDef],
    outer_col_map: &ColumnMap,
    outer_rows: &[Vec<Value>],
    join_type: JoinType,
    on_clause: Option<&Expr>,
    io: &mut dyn LateralIo,
    cancel: Option<&CancelToken>,
) -> Result<Option<LateralRows>> {
    check_cancel(cancel)?;
    if !derived.query.ctes.is_empty() {
        return Ok(None);
    }
    if on_clause.is_some() {
        return Ok(None);
    }
    let sel = match &derived.query.body {
        QueryBody::Select(s) => s,
        _ => return Ok(None),
    };
    if !sel.joins.is_empty()
        || !sel.group_by.is_empty()
        || sel.having.is_some()
        || sel.distinct
        || sel.from_subquery.is_some()
    {
        return Ok(None);
    }
    let inner_table = sel.from.to_ascii_lowercase();
    let inner_schema = match schema.get(&inner_table) {
        Some(s) => s,
        None => return Ok(None),
    };
    let inner_alias = sel
        .from_alias
        .clone()
        .unwrap_or_else(|| inner_table.clone());

    let where_expr = match &sel.where_clause {
        Some(w) => w,
        None => return Ok(None),
    };
    let conjuncts = super::correlated::flatten_and_exprs(where_expr);
    let mut corr: Vec<(usize, usize)> = Vec::new();
    let mut residual: Vec<Expr> = Vec::new();
    for c in conjuncts {
        if let Some(pair) = try_extract_corr(c, outer_col_map, &inner_alias, inner_schema) {
            corr.push(pair);
        } else if expr_uses_outer(c, outer_col_map, &inner_alias, inner_schema) {
            return Ok(None);
        } else {
            residual.push(c.clone());
        }
    }
    if corr.is_empty() {
        return Ok(None);
    }
    if sel
        .order_by
        .iter()
        .any(|o| expr_uses_outer(&o.expr, outer_col_map, &inner_alias, inner_schema))
    {
        return Ok(None);
    }
    if sel.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => {
            expr_uses_outer(expr, outer_col_map, &inner_alias, inner_schema)
                || is_aggregate_expr(expr)
        }
        _ => false,
    }) {
        return Ok(None);
    }

    let limit_n = match &sel.limit {
        Some(Expr::Literal(Value::Integer(n))) if *n >= 0 => Some(*n as usize),
        Some(_) => return Ok(None),
        None => None,
    };

    let residual_where = if residual.is_empty() {
        None
    } else {
        let mut combined = residual.remove(0);
        for r in residual {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinOp::And,
                right: Box::new(r),
            };
        }
        Some(combined)
    };

    let inner_stmt = SelectStmt {
        columns: vec![SelectColumn::AllColumns],
        from: inner_table.clone(),
        from_alias: sel.from_alias.clone(),
        from_subquery: None,
        from_args: None,
        from_json_table: None,
        joins: vec![],
        distinct: false,
        where_clause: residual_where,
        order_by: sel.order_by.clone(),
        limit: None,
        offset: None,
        group_by: vec![],
        having: None,
    };
    let inner_qr = io.exec_select(
        schema,
        &SelectQuery {
            ctes: vec![],
            recursive: false,
            body: QueryBody::Select(Box::new(inner_stmt)),
        },
    )?;
    check_cancel(cancel)?;

    let proj_plan = build_projection_indices(&sel.columns, &inner_qr.columns);
    let probe_columns: Vec<String> = match proj_plan.as_ref() {
        Some(p) => p.iter().map(|(name, _)| name.clone()).collect(),
        None => inner_qr.columns.clone(),
    };

    let mut groups: FxHashMap<Vec<Value>, Vec<Vec<Value>>> = FxHashMap::default();
    let inner_col_idx: Vec<usize> = corr.iter().map(|&(_, inner_idx)| inner_idx).collect();
    for (row_idx, row) in inner_qr.rows.into_iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let key: Vec<Value> = inner_col_idx.iter().map(|&i| row[i].clone()).collect();
        if key.iter().any(|v| matches!(v, Value::Null)) {
            continue;
        }
        groups.entry(key).or_default().push(row);
    }
    if let Some(n) = limit_n {
        for (group_idx, v) in groups.values_mut().enumerate() {
            check_cancel_at(cancel, group_idx)?;
            v.truncate(n);
        }
    }

    let outer_idx: Vec<usize> = corr.iter().map(|&(o, _)| o).collect();
    let mut new_rows: Vec<Vec<Value>> = Vec::new();
    let mut expansion_work = 0usize;
    for (outer_row_idx, outer_row) in outer_rows.iter().enumerate() {
        check_cancel_at(cancel, outer_row_idx)?;
        let key: Vec<Value> = outer_idx.iter().map(|&i| outer_row[i].clone()).collect();
        let inner_rows = groups.get(&key);
        match inner_rows {
            Some(rows) if !rows.is_empty() => {
                for inner_row in rows {
                    check_cancel_at(cancel, expansion_work)?;
                    expansion_work += 1;
                    let mut combined = outer_row.clone();
                    if let Some(plan) = &proj_plan {
                        for &(_, idx) in plan {
                            combined.push(inner_row[idx].clone());
                        }
                    } else {
                        combined.extend(inner_row.iter().cloned());
                    }
                    new_rows.push(combined);
                }
            }
            _ => {
                if matches!(join_type, JoinType::Left) {
                    let mut combined = outer_row.clone();
                    combined.resize(combined.len() + probe_columns.len(), Value::Null);
                    new_rows.push(combined);
                }
            }
        }
    }
    let _ = outer_cols;
    check_cancel(cancel)?;
    Ok(Some((new_rows, probe_columns)))
}

fn try_extract_corr(
    expr: &Expr,
    outer_col_map: &ColumnMap,
    inner_alias: &str,
    inner_schema: &TableSchema,
) -> Option<(usize, usize)> {
    let (left, right) = match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => (left.as_ref(), right.as_ref()),
        _ => return None,
    };
    let try_pair = |a: &Expr, b: &Expr| -> Option<(usize, usize)> {
        let outer_idx = match a {
            Expr::QualifiedColumn { table, column } => {
                let q = format!(
                    "{}.{}",
                    table.to_ascii_lowercase(),
                    column.to_ascii_lowercase()
                );
                outer_col_map.resolve(&q).ok()
            }
            _ => None,
        }?;
        let inner_idx = match b {
            Expr::Column(name) => inner_schema.column_index(&name.to_ascii_lowercase()),
            Expr::QualifiedColumn { table, column } => {
                let t = table.to_ascii_lowercase();
                if t == inner_alias.to_ascii_lowercase()
                    || t == inner_schema.name.to_ascii_lowercase()
                {
                    inner_schema.column_index(&column.to_ascii_lowercase())
                } else {
                    None
                }
            }
            _ => None,
        }?;
        Some((outer_idx, inner_idx))
    };
    try_pair(left, right).or_else(|| try_pair(right, left))
}

fn expr_uses_outer(
    expr: &Expr,
    outer_col_map: &ColumnMap,
    inner_alias: &str,
    inner_schema: &TableSchema,
) -> bool {
    let inner_alias_lc = inner_alias.to_ascii_lowercase();
    let inner_name_lc = inner_schema.name.to_ascii_lowercase();
    fn walk(e: &Expr, f: &mut dyn FnMut(&Expr) -> bool) -> bool {
        if f(e) {
            return true;
        }
        match e {
            Expr::BinaryOp { left, right, .. } => walk(left, f) || walk(right, f),
            Expr::UnaryOp { expr, .. } => walk(expr, f),
            Expr::IsNull(x) | Expr::IsNotNull(x) => walk(x, f),
            Expr::Function { args, .. } | Expr::Coalesce(args) => args.iter().any(|a| walk(a, f)),
            Expr::Cast { expr, .. } => walk(expr, f),
            Expr::Between {
                expr, low, high, ..
            } => walk(expr, f) || walk(low, f) || walk(high, f),
            Expr::InList { expr, list, .. } => walk(expr, f) || list.iter().any(|a| walk(a, f)),
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => walk(expr, f) || walk(pattern, f) || escape.as_ref().is_some_and(|e| walk(e, f)),
            Expr::IsDistinctFrom { left, right, .. } => walk(left, f) || walk(right, f),
            _ => false,
        }
    }
    let mut probe = |e: &Expr| -> bool {
        match e {
            Expr::QualifiedColumn { table, column } => {
                let t = table.to_ascii_lowercase();
                if t == inner_alias_lc || t == inner_name_lc {
                    return false;
                }
                let q = format!("{}.{}", t, column.to_ascii_lowercase());
                outer_col_map.resolve(&q).is_ok()
            }
            Expr::Column(name) => {
                if inner_schema
                    .column_index(&name.to_ascii_lowercase())
                    .is_some()
                {
                    return false;
                }
                outer_col_map.resolve(&name.to_ascii_lowercase()).is_ok()
            }
            _ => false,
        }
    };
    walk(expr, &mut probe)
}

fn build_projection_indices(
    select_cols: &[SelectColumn],
    inner_columns: &[String],
) -> Option<Vec<(String, usize)>> {
    let mut out = Vec::new();
    for c in select_cols {
        match c {
            SelectColumn::AllColumns => return None,
            SelectColumn::Expr { expr, alias } => {
                let (col_name, idx) = match expr {
                    Expr::Column(name) => {
                        let lower = name.to_ascii_lowercase();
                        let idx = inner_columns
                            .iter()
                            .position(|c| c.to_ascii_lowercase() == lower)?;
                        (alias.clone().unwrap_or_else(|| name.clone()), idx)
                    }
                    Expr::QualifiedColumn { column, .. } => {
                        let lower = column.to_ascii_lowercase();
                        let idx = inner_columns
                            .iter()
                            .position(|c| c.to_ascii_lowercase() == lower)?;
                        (alias.clone().unwrap_or_else(|| column.clone()), idx)
                    }
                    _ => return None,
                };
                out.push((col_name, idx));
            }
            _ => return None,
        }
    }
    Some(out)
}

fn extend_lateral_cols(base: &[ColumnDef], probe_columns: &[String]) -> Vec<ColumnDef> {
    let mut out: Vec<ColumnDef> = base.to_vec();
    for name in probe_columns {
        out.push(ColumnDef {
            name: name.clone(),
            data_type: DataType::Null,
            nullable: true,
            position: 0,
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
        });
    }
    out
}

fn bind_query_with_outer(
    query: &SelectQuery,
    outer_row: &[Value],
    outer_col_map: &ColumnMap,
) -> Result<SelectQuery> {
    let body = match &query.body {
        QueryBody::Select(sel) => QueryBody::Select(Box::new(bind_select_with_outer(
            sel,
            outer_row,
            outer_col_map,
        )?)),
        _ => {
            return Err(SqlError::Unsupported(
                "LATERAL subquery body must be a SELECT".into(),
            ));
        }
    };
    Ok(SelectQuery {
        ctes: query.ctes.clone(),
        recursive: query.recursive,
        body,
    })
}

fn bind_select_with_outer(
    sel: &SelectStmt,
    outer_row: &[Value],
    outer_col_map: &ColumnMap,
) -> Result<SelectStmt> {
    let where_clause = sel
        .where_clause
        .as_ref()
        .map(|w| bind_expr_with_outer(w, outer_row, outer_col_map))
        .transpose()?;
    let columns = sel
        .columns
        .iter()
        .map(|c| match c {
            SelectColumn::Expr { expr, alias } => Ok(SelectColumn::Expr {
                expr: bind_expr_with_outer(expr, outer_row, outer_col_map)?,
                alias: alias.clone(),
            }),
            other => Ok(other.clone()),
        })
        .collect::<Result<Vec<_>>>()?;
    let order_by = sel
        .order_by
        .iter()
        .map(|o| {
            Ok(OrderByItem {
                expr: bind_expr_with_outer(&o.expr, outer_row, outer_col_map)?,
                output_name: o.output_name.clone(),
                output_ordinal: o.output_ordinal,
                descending: o.descending,
                nulls_first: o.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SelectStmt {
        columns,
        from: sel.from.clone(),
        from_alias: sel.from_alias.clone(),
        from_subquery: sel.from_subquery.clone(),
        from_args: sel.from_args.clone(),
        from_json_table: sel.from_json_table.clone(),
        joins: sel.joins.clone(),
        distinct: sel.distinct,
        where_clause,
        order_by,
        limit: sel.limit.clone(),
        offset: sel.offset.clone(),
        group_by: sel.group_by.clone(),
        having: sel.having.clone(),
    })
}

fn bind_expr_with_outer(
    expr: &Expr,
    outer_row: &[Value],
    outer_col_map: &ColumnMap,
) -> Result<Expr> {
    use Expr::*;
    match expr {
        Column(_) => Ok(expr.clone()),
        QualifiedColumn { table, column } => {
            let qualified = format!("{table}.{column}");
            if let Ok(idx) = outer_col_map.resolve(&qualified) {
                Ok(Literal(outer_row[idx].clone()))
            } else {
                Ok(expr.clone())
            }
        }
        BinaryOp { left, op, right } => Ok(BinaryOp {
            left: Box::new(bind_expr_with_outer(left, outer_row, outer_col_map)?),
            op: *op,
            right: Box::new(bind_expr_with_outer(right, outer_row, outer_col_map)?),
        }),
        IsDistinctFrom {
            left,
            right,
            negated,
        } => Ok(IsDistinctFrom {
            left: Box::new(bind_expr_with_outer(left, outer_row, outer_col_map)?),
            right: Box::new(bind_expr_with_outer(right, outer_row, outer_col_map)?),
            negated: *negated,
        }),
        UnaryOp { op, expr: inner } => Ok(UnaryOp {
            op: *op,
            expr: Box::new(bind_expr_with_outer(inner, outer_row, outer_col_map)?),
        }),
        Function {
            name,
            args,
            distinct,
        } => Ok(Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| bind_expr_with_outer(a, outer_row, outer_col_map))
                .collect::<Result<Vec<_>>>()?,
            distinct: *distinct,
        }),
        Cast {
            expr: inner,
            data_type,
        } => Ok(Cast {
            expr: Box::new(bind_expr_with_outer(inner, outer_row, outer_col_map)?),
            data_type: *data_type,
        }),
        IsNull(inner) => Ok(IsNull(Box::new(bind_expr_with_outer(
            inner,
            outer_row,
            outer_col_map,
        )?))),
        IsNotNull(inner) => Ok(IsNotNull(Box::new(bind_expr_with_outer(
            inner,
            outer_row,
            outer_col_map,
        )?))),
        Between {
            expr: inner,
            low,
            high,
            negated,
        } => Ok(Between {
            expr: Box::new(bind_expr_with_outer(inner, outer_row, outer_col_map)?),
            low: Box::new(bind_expr_with_outer(low, outer_row, outer_col_map)?),
            high: Box::new(bind_expr_with_outer(high, outer_row, outer_col_map)?),
            negated: *negated,
        }),
        InList {
            expr: inner,
            list,
            negated,
        } => Ok(InList {
            expr: Box::new(bind_expr_with_outer(inner, outer_row, outer_col_map)?),
            list: list
                .iter()
                .map(|e| bind_expr_with_outer(e, outer_row, outer_col_map))
                .collect::<Result<Vec<_>>>()?,
            negated: *negated,
        }),
        Like {
            expr: inner,
            pattern,
            escape,
            negated,
        } => Ok(Like {
            expr: Box::new(bind_expr_with_outer(inner, outer_row, outer_col_map)?),
            pattern: Box::new(bind_expr_with_outer(pattern, outer_row, outer_col_map)?),
            escape: match escape {
                Some(e) => Some(Box::new(bind_expr_with_outer(e, outer_row, outer_col_map)?)),
                None => None,
            },
            negated: *negated,
        }),
        _ => Ok(expr.clone()),
    }
}

pub(super) fn exec_select_no_from(
    stmt: &SelectStmt,
    cancel: Option<&CancelToken>,
) -> Result<ExecutionResult> {
    let empty_cols: Vec<ColumnDef> = vec![];
    process_select(
        vec![Vec::new()],
        SelectCtx::new(&empty_cols, stmt, cancel).predicate_applied(false),
    )
}

fn extract_pre_projection_sort_keys(
    rows: &[Vec<Value>],
    order_by: &[OrderByItem],
    columns: &[ColumnDef],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<Value>>> {
    let col_map = ColumnMap::new(columns);
    let mut keys = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.iter().enumerate() {
        check_cancel_at(cancel, row_index)?;
        keys.push(
            order_by
                .iter()
                .map(|item| {
                    if order_by_uses_projected_output(item) {
                        Ok(Value::Null)
                    } else {
                        eval_expr(&item.expr, &EvalCtx::new(&col_map, row).with_cancel(cancel))
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }
    check_cancel(cancel)?;
    Ok(keys)
}

fn fill_projected_sort_keys(
    keys: &mut [Vec<Value>],
    projected: &[Vec<Value>],
    order_by: &[OrderByItem],
    output_columns: &[ColumnDef],
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    debug_assert_eq!(keys.len(), projected.len());
    let output_map = ColumnMap::new(output_columns);
    let output_positions = order_by
        .iter()
        .map(|item| order_by_output_position(item, &output_map))
        .collect::<Result<Vec<_>>>()?;
    for (row_index, (key, row)) in keys.iter_mut().zip(projected).enumerate() {
        check_cancel_at(cancel, row_index)?;
        for (key_index, output_index) in output_positions.iter().enumerate() {
            if let Some(output_index) = output_index {
                key[key_index] = row.get(*output_index).cloned().unwrap_or(Value::Null);
            }
        }
    }
    check_cancel(cancel)
}

fn projection_sort_collations(
    order_by: &[OrderByItem],
    source_columns: &[ColumnDef],
    output_columns: &[ColumnDef],
) -> Result<Vec<Collation>> {
    let source_map = ColumnMap::new(source_columns);
    let output_map = ColumnMap::new(output_columns);
    order_by
        .iter()
        .map(|item| {
            Ok(order_by_output_position(item, &output_map)?.map_or_else(
                || expr_collation(&item.expr, &source_map),
                |position| output_map.collation_at(position),
            ))
        })
        .collect()
}

/// Everything after the scan: filter, window, aggregate, distinct, sort, project.
/// These run over rows already in memory and can outlast the read that produced
/// them, so cancellation stopping at the scan would do nothing on long queries.
pub(super) fn process_select(
    mut rows: Vec<Vec<Value>>,
    ctx: SelectCtx<'_>,
) -> Result<ExecutionResult> {
    let SelectCtx {
        columns,
        stmt,
        predicate_applied,
        ..
    } = ctx;
    ctx.check()?;

    if stmt
        .order_by
        .iter()
        .any(|item| item.output_ordinal.is_some())
    {
        let output_columns = build_output_columns(&stmt.columns, columns);
        validate_order_by_ordinals(&stmt.order_by, output_columns.len())?;
    }

    if !predicate_applied {
        if let Some(ref where_expr) = stmt.where_clause {
            let col_map = ColumnMap::new(columns);
            let mut keep = Vec::with_capacity(rows.len());
            for (row_idx, row) in rows.iter().enumerate() {
                check_cancel_at(ctx.cancel, row_idx)?;
                let value = eval_expr(
                    where_expr,
                    &EvalCtx::new(&col_map, row).with_cancel(ctx.cancel),
                )?;
                keep.push(is_truthy(&value));
            }
            let mut keep = keep.into_iter();
            rows.retain(|_| keep.next().expect("one filter decision per row"));
            debug_assert!(keep.next().is_none());
            ctx.check()?;
        }
    }

    ctx.check()?;

    if has_any_window_function(stmt) {
        return eval_window_select(rows, ctx);
    }

    let has_aggregates = stmt.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    });

    if has_aggregates || !stmt.group_by.is_empty() {
        return exec_aggregate(&rows, ctx);
    }

    if stmt.distinct {
        // Extracted BEFORE the projection, which discards the source columns an
        // ORDER BY expression names. Sorting projected rows resolves only against
        // output names and keys every row `Value::Null`.
        let col_map = ColumnMap::new(columns);
        let output_columns = build_output_columns(&stmt.columns, columns);
        let mut sort_keys = if stmt.order_by.is_empty() {
            None
        } else {
            Some(extract_pre_projection_sort_keys(
                &rows,
                &stmt.order_by,
                columns,
                ctx.cancel,
            )?)
        };
        let (col_names, mut projected) =
            project_rows_with_cancel(columns, &stmt.columns, rows, ctx.cancel)?;
        if let Some(keys) = &mut sort_keys {
            fill_projected_sort_keys(
                keys,
                &projected,
                &stmt.order_by,
                &output_columns,
                ctx.cancel,
            )?;
        }
        ctx.check()?;

        // Keyed by the projected column's collation, so a column that calls two spellings
        // equal does not return both of them as distinct rows.
        let mut seen = RowKeys::with_capacity(
            output_collations(&stmt.columns, &col_map),
            projected.len().min(1024),
        );
        let mut kept_keys: Vec<Vec<Value>> = Vec::new();
        if ctx.cancel.is_none() {
            let mut row_idx = 0;
            projected.retain(|row| {
                let keep = seen.insert(row);
                if keep {
                    if let Some(keys) = &mut sort_keys {
                        kept_keys.push(std::mem::take(&mut keys[row_idx]));
                    }
                }
                row_idx += 1;
                keep
            });
        } else {
            let original = std::mem::take(&mut projected);
            projected.reserve(original.len());
            for (i, row) in original.into_iter().enumerate() {
                check_cancel_at(ctx.cancel, i)?;
                if seen.insert(&row) {
                    if let Some(keys) = &mut sort_keys {
                        kept_keys.push(std::mem::take(&mut keys[i]));
                    }
                    projected.push(row);
                }
            }
        }
        ctx.check()?;

        if !stmt.order_by.is_empty() {
            let collations = projection_sort_collations(&stmt.order_by, columns, &output_columns)?;
            sort_rows_by_keys(
                &mut projected,
                &kept_keys,
                &stmt.order_by,
                &collations,
                ctx.cancel,
            )?;
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

        ctx.check()?;
        return Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: projected,
        }));
    }

    ctx.check()?;

    if stmt.order_by.iter().any(order_by_uses_projected_output) {
        let output_columns = build_output_columns(&stmt.columns, columns);
        let mut sort_keys =
            extract_pre_projection_sort_keys(&rows, &stmt.order_by, columns, ctx.cancel)?;
        let (col_names, mut projected) =
            project_rows_with_cancel(columns, &stmt.columns, rows, ctx.cancel)?;
        fill_projected_sort_keys(
            &mut sort_keys,
            &projected,
            &stmt.order_by,
            &output_columns,
            ctx.cancel,
        )?;
        let collations = projection_sort_collations(&stmt.order_by, columns, &output_columns)?;

        if let Some(ref limit_expr) = stmt.limit {
            let limit = eval_const_int(limit_expr)?.max(0) as usize;
            let offset = stmt
                .offset
                .as_ref()
                .map(eval_const_int)
                .transpose()?
                .unwrap_or(0)
                .max(0) as usize;
            let keep = limit.saturating_add(offset);
            if keep == 0 {
                projected.clear();
            } else if keep < projected.len() {
                topk_rows_by_keys(
                    &mut projected,
                    &sort_keys,
                    &stmt.order_by,
                    &collations,
                    keep,
                    ctx.cancel,
                )?;
                projected.truncate(keep);
            } else {
                sort_rows_by_keys(
                    &mut projected,
                    &sort_keys,
                    &stmt.order_by,
                    &collations,
                    ctx.cancel,
                )?;
            }
        } else {
            sort_rows_by_keys(
                &mut projected,
                &sort_keys,
                &stmt.order_by,
                &collations,
                ctx.cancel,
            )?;
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
            projected.truncate(eval_const_int(limit_expr)?.max(0) as usize);
        }
        ctx.check()?;
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
                topk_rows(&mut rows, &stmt.order_by, columns, keep, ctx.cancel)?;
                rows.truncate(keep);
            } else {
                sort_rows(&mut rows, &stmt.order_by, columns, ctx.cancel)?;
            }
        } else {
            sort_rows(&mut rows, &stmt.order_by, columns, ctx.cancel)?;
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

    let (col_names, projected) =
        project_rows_with_cancel(columns, &stmt.columns, rows, ctx.cancel)?;
    ctx.check()?;

    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: projected,
    }))
}

/// Scanned table's leaf pages cached across re-executions, keyed by commit generation, so
/// repeated prepared scans skip the per-scan leaf-collection DFS + buffer-pool locks.
type LeafScanCache = parking_lot::RwLock<Option<(u64, citadel_txn::read_txn::LeafPagesWeak)>>;

pub struct CompiledSelect {
    join_plan: Option<Arc<JoinPlanStatic>>,
    join_cache: Option<parking_lot::RwLock<Option<Arc<CachedJoin>>>>,
    compound_plan: Option<Arc<CompoundPlanStatic>>,
    compound_cache: Option<parking_lot::RwLock<Option<Arc<CachedCompound>>>>,
    leaf_cache: LeafScanCache,
    /// `Some` iff the statement is deterministic and read-only
    /// (`result_cache::is_result_cacheable`); memoizes the materialized
    /// result keyed by (commit generation, params).
    result_cache: Option<super::result_cache::ResultCacheSlot>,
    lane: Option<CompiledSelectLane>,
}

enum CompiledSelectLane {
    Point(PkPointPlan),
    Scan(SimpleScanPlan),
}

/// Full-pk-equality SELECT compiled to a table_get; key values re-resolve per execute.
struct PkPointPlan {
    table_lower: String,
    table_schema: TableSchema,
    proj: StreamProj,
    columns: Vec<String>,
    where_expr: Expr,
    pk_sources: Vec<PointSource>,
}

/// Single-table WHERE-only SELECT; planning still runs per execute.
struct SimpleScanPlan {
    table_schema: TableSchema,
    proj: StreamProj,
    columns: Vec<String>,
    where_expr: Option<Expr>,
    /// Schema columns the projection and WHERE read; the covered-index gate.
    needed: Vec<usize>,
}

enum PointSource {
    Literal(Value),
    Param(usize),
}

fn resolve_point_key(sources: &[PointSource], schema: &TableSchema) -> Result<Option<Vec<u8>>> {
    let mut values = sources
        .iter()
        .map(|source| match source {
            PointSource::Literal(value) => Ok(value.clone()),
            PointSource::Param(n) => crate::eval::resolve_scoped_param(*n),
        })
        .collect::<Result<Vec<_>>>()?;
    for (value, &column) in values.iter_mut().zip(&schema.primary_key_columns) {
        let Some((_, normalized)) = crate::planner::key_predicate(
            schema.columns[column as usize].data_type,
            BinOp::Eq,
            value,
        ) else {
            return Ok(None);
        };
        *value = normalized;
    }
    Ok(Some(crate::encoding::encode_composite_key(&values)))
}

/// Every conjunct must be `pk_col = Literal|Parameter`, each pk col once.
fn detect_pk_point_sources(
    where_expr: &Expr,
    table_schema: &TableSchema,
) -> Option<Vec<PointSource>> {
    let pk_cols = &table_schema.primary_key_columns;
    if pk_cols.is_empty() {
        return None;
    }
    let mut sources: Vec<Option<PointSource>> = (0..pk_cols.len()).map(|_| None).collect();
    if !collect_pk_eq_conjuncts(where_expr, table_schema, &mut sources) {
        return None;
    }
    sources.into_iter().collect()
}

fn collect_pk_eq_conjuncts(
    expr: &Expr,
    table_schema: &TableSchema,
    sources: &mut [Option<PointSource>],
) -> bool {
    let Expr::BinaryOp { left, op, right } = expr else {
        return false;
    };
    if *op == BinOp::And {
        return collect_pk_eq_conjuncts(left, table_schema, sources)
            && collect_pk_eq_conjuncts(right, table_schema, sources);
    }
    if *op != BinOp::Eq {
        return false;
    }
    // Bare columns only: qualifier validity is the generic path's concern.
    let pk_pos = |e: &Expr| {
        let Expr::Column(name) = e else { return None };
        let idx = table_schema.column_index(name)? as u16;
        table_schema
            .primary_key_columns
            .iter()
            .position(|&c| c == idx)
    };
    let source = |e: &Expr| match e {
        Expr::Literal(v) => Some(PointSource::Literal(v.clone())),
        Expr::Parameter(n) => Some(PointSource::Param(*n)),
        _ => None,
    };
    let (pos, src) = if let (Some(p), Some(s)) = (pk_pos(left), source(right)) {
        (p, s)
    } else if let (Some(p), Some(s)) = (pk_pos(right), source(left)) {
        (p, s)
    } else {
        return false;
    };
    if sources[pos].is_some() {
        return false;
    }
    sources[pos] = Some(src);
    true
}

/// Inverted-index operators have specialized ladder paths the lane lacks.
fn where_has_inverted_op(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            matches!(op, BinOp::JsonContains | BinOp::JsonPathMatch)
                || where_has_inverted_op(left)
                || where_has_inverted_op(right)
        }
        Expr::UnaryOp { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            where_has_inverted_op(expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            where_has_inverted_op(expr) || where_has_inverted_op(low) || where_has_inverted_op(high)
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            where_has_inverted_op(left) || where_has_inverted_op(right)
        }
        _ => false,
    }
}

fn build_select_lane(schema: &SchemaManager, sel: &SelectStmt) -> Option<CompiledSelectLane> {
    if !sel.joins.is_empty()
        || !sel.group_by.is_empty()
        || sel.having.is_some()
        || !sel.order_by.is_empty()
        || sel.limit.is_some()
        || sel.offset.is_some()
        || sel.distinct
        || sel.from_subquery.is_some()
        || sel.from_args.is_some()
        || sel.from_json_table.is_some()
        || sel.where_clause.as_ref().is_some_and(where_has_inverted_op)
    {
        return None;
    }
    let lower = sel.from.to_ascii_lowercase();
    if schema.get_view(&lower).is_some()
        || schema.get_matview(&lower).is_some()
        || schema.get_virtual(&lower).is_some()
    {
        return None;
    }
    let table_schema = schema.get(&lower)?;
    // Virtual columns are stored as NULL placeholders the raw decode keeps.
    if table_schema.has_virtual_columns() {
        return None;
    }
    // The no-WHERE shape is already served by the leaf-cached collect_scan.
    let where_expr = sel.where_clause.as_ref()?;
    let proj = build_stream_proj(&sel.columns, table_schema)?;
    let columns = projection_column_names(&sel.columns, &table_schema.columns);
    if let Some(pk_sources) = detect_pk_point_sources(where_expr, table_schema) {
        return Some(CompiledSelectLane::Point(PkPointPlan {
            table_lower: table_schema.name.clone(),
            table_schema: table_schema.clone(),
            proj,
            columns,
            where_expr: where_expr.clone(),
            pk_sources,
        }));
    }
    let mut needed: Vec<usize> = match &proj {
        StreamProj::Identity { .. } => (0..table_schema.columns.len()).collect(),
        StreamProj::Columns { idxs, .. } => idxs.clone(),
        StreamProj::Exprs { exprs, .. } => exprs
            .iter()
            .flat_map(|e| referenced_columns(e, &table_schema.columns))
            .collect(),
    };
    needed.extend(referenced_columns(where_expr, &table_schema.columns));
    needed.sort_unstable();
    needed.dedup();
    Some(CompiledSelectLane::Scan(SimpleScanPlan {
        table_schema: table_schema.clone(),
        proj,
        columns,
        where_expr: Some(where_expr.clone()),
        needed,
    }))
}

impl CompiledSelectLane {
    fn run(&self, rtx: &mut ReadTxn<'_>) -> Result<QueryResult> {
        match self {
            CompiledSelectLane::Point(p) => p.run(rtx),
            CompiledSelectLane::Scan(s) => s.run(rtx),
        }
    }
}

/// EXPLAIN marker: true when the scan lane would serve this select covered.
pub(super) fn select_would_cover(schema: &SchemaManager, sel: &SelectStmt) -> bool {
    match build_select_lane(schema, sel) {
        Some(CompiledSelectLane::Scan(s)) => {
            let plan = crate::planner::plan_select_inverted(&s.table_schema, &s.where_expr);
            super::scan::covered_index_components(&s.table_schema, &plan, &s.needed).is_some()
        }
        _ => false,
    }
}

impl SimpleScanPlan {
    fn run(&self, rtx: &mut ReadTxn<'_>) -> Result<QueryResult> {
        let cancel = rtx.cancel_token().cloned();
        let cancel = cancel.as_ref();
        let plan = crate::planner::plan_select_inverted(&self.table_schema, &self.where_expr);
        let col_map = self.table_schema.column_map();
        // A fully consumed WHERE needs no per-row re-eval on covered rows.
        let where_for_scan = match &self.where_expr {
            Some(w) if crate::planner::index_scan_full_cover(&self.table_schema, w, &plan) => &None,
            other => other,
        };
        let emit_direct = match &self.proj {
            StreamProj::Columns { idxs, .. } => Some(idxs.as_slice()),
            _ => None,
        };
        if let Some(rows) = super::scan::try_covered_index_collect_read(
            rtx,
            &self.table_schema,
            &plan,
            where_for_scan,
            &self.needed,
            None,
            emit_direct,
        )? {
            // Direct emission is already projected; residual paths are not.
            if emit_direct.is_some() && where_for_scan.is_none() {
                return Ok(QueryResult {
                    columns: self.columns.clone(),
                    rows,
                });
            }
            let mut out = Vec::with_capacity(rows.len());
            for mut row in rows {
                out.push(self.proj.project_decoded(&mut row, cancel)?);
            }
            return Ok(QueryResult {
                columns: self.columns.clone(),
                rows: out,
            });
        }
        let (rows, filtered) = super::scan::collect_rows_with_read_planned(
            rtx,
            &self.table_schema,
            &self.where_expr,
            None,
            plan,
        )?;
        let mut out = Vec::with_capacity(rows.len());
        for mut row in rows {
            if !filtered {
                if let Some(w) = &self.where_expr {
                    if !is_truthy(&eval_expr(
                        w,
                        &EvalCtx::new(col_map, &row).with_cancel(cancel),
                    )?) {
                        continue;
                    }
                }
            }
            out.push(self.proj.project_decoded(&mut row, cancel)?);
        }
        Ok(QueryResult {
            columns: self.columns.clone(),
            rows: out,
        })
    }
}

impl PkPointPlan {
    fn run(&self, rtx: &mut ReadTxn<'_>) -> Result<QueryResult> {
        let cancel = rtx.cancel_token().cloned();
        let cancel = cancel.as_ref();
        let Some(key) = resolve_point_key(&self.pk_sources, &self.table_schema)? else {
            let (candidates, _) = super::scan::collect_rows_with_read_planned(
                rtx,
                &self.table_schema,
                &Some(self.where_expr.clone()),
                None,
                crate::planner::ScanPlan::SeqScan,
            )?;
            let rows = candidates
                .into_iter()
                .map(|mut row| self.proj.project_decoded(&mut row, cancel))
                .collect::<Result<_>>()?;
            return Ok(QueryResult {
                columns: self.columns.clone(),
                rows,
            });
        };
        let rows = match rtx
            .table_get(self.table_lower.as_bytes(), &key)
            .map_err(SqlError::Storage)?
        {
            Some(value) => {
                let mut row =
                    decode_full_row_with_cancel(&self.table_schema, &key, &value, cancel)?;
                let col_map = self.table_schema.column_map();
                match eval_expr(
                    &self.where_expr,
                    &EvalCtx::new(col_map, &row).with_cancel(cancel),
                ) {
                    Ok(v) if is_truthy(&v) => vec![self.proj.project_decoded(&mut row, cancel)?],
                    Ok(_) => Vec::new(),
                    Err(e) => return Err(e),
                }
            }
            None => Vec::new(),
        };
        Ok(QueryResult {
            columns: self.columns.clone(),
            rows,
        })
    }
}

struct JoinPlanStatic {
    table_lowers: Vec<String>,
    table_schemas: Vec<Arc<TableSchema>>,
    needed_per_table: Vec<Vec<usize>>,
    output_combined: Option<Vec<usize>>,
    /// Per join step: (equi_pairs, is_pure_equi), fixed by the statement.
    step_equi: Vec<super::join::EquiJoin>,
    /// Full-pk-eq WHERE on the outer table: fetch one row instead of a scan.
    outer_point: Option<Vec<PointSource>>,
}

/// Pk-eq sources for the outer table; other conjuncts stay post-join filtered.
fn detect_outer_point_sources(
    where_expr: &Expr,
    outer_ref: &str,
    outer_schema: &TableSchema,
    unqualified_is_ambiguous: &dyn Fn(&str) -> bool,
) -> Option<Vec<PointSource>> {
    let pk_cols = &outer_schema.primary_key_columns;
    if pk_cols.is_empty() {
        return None;
    }
    let pk_pos = |e: &Expr| -> Option<usize> {
        let name = match e {
            Expr::Column(n) if !unqualified_is_ambiguous(n) => n,
            Expr::QualifiedColumn { table, column } if table.eq_ignore_ascii_case(outer_ref) => {
                column
            }
            _ => return None,
        };
        let idx = outer_schema.column_index(name)? as u16;
        pk_cols.iter().position(|&c| c == idx)
    };
    let source = |e: &Expr| match e {
        Expr::Literal(v) => Some(PointSource::Literal(v.clone())),
        Expr::Parameter(n) => Some(PointSource::Param(*n)),
        _ => None,
    };
    let mut sources: Vec<Option<PointSource>> = (0..pk_cols.len()).map(|_| None).collect();
    let mut stack = vec![where_expr];
    while let Some(e) = stack.pop() {
        let Expr::BinaryOp { left, op, right } = e else {
            continue;
        };
        if *op == BinOp::And {
            stack.push(left);
            stack.push(right);
            continue;
        }
        if *op != BinOp::Eq {
            continue;
        }
        let (pos, src) = if let (Some(p), Some(s)) = (pk_pos(left), source(right)) {
            (p, s)
        } else if let (Some(p), Some(s)) = (pk_pos(right), source(left)) {
            (p, s)
        } else {
            continue;
        };
        if sources[pos].is_none() {
            sources[pos] = Some(src);
        }
    }
    sources.into_iter().collect()
}

struct CachedJoin {
    cached_gen: u64,
    inner_per_table: Vec<Vec<Vec<Value>>>,
    probes: Vec<super::join::ProbeIndex>,
}

/// Retention cap for the per-generation join cache (values, not bytes).
const JOIN_CACHE_MAX_CELLS: usize = 262_144;

impl CachedJoin {
    fn cell_count(&self) -> usize {
        self.inner_per_table
            .iter()
            .map(|rows| rows.len() * rows.first().map_or(0, Vec::len))
            .sum()
    }
}

struct CompoundPlanStatic {
    op: SetOp,
    all: bool,
    branches: Vec<BranchPlan>,
    columns: Vec<String>,
    /// This lane runs the same six set operations as `apply_set_operation`, so it folds its
    /// keys by the same rule: without it, `UNION` and `UNION ... ORDER BY` deduplicated a
    /// collated column differently, the ORDER BY deciding which lane ran.
    key_colls: Vec<crate::types::Collation>,
}

struct BranchPlan {
    table_schema: Arc<TableSchema>,
    needed_cols: Vec<usize>,
}

struct CachedCompound {
    cached_gen: u64,
    branch_rows: Vec<Vec<Vec<Value>>>,
}

/// A plan object whose only job is carrying the result-cache slot; every
/// execution routes through the generic executors.
fn cache_carrier(
    result_cache: Option<super::result_cache::ResultCacheSlot>,
) -> Option<CompiledSelect> {
    result_cache.map(|rc| CompiledSelect {
        join_plan: None,
        join_cache: None,
        compound_plan: None,
        compound_cache: None,
        leaf_cache: parking_lot::RwLock::new(None),
        result_cache: Some(rc),
        lane: None,
    })
}

impl CompiledSelect {
    pub fn try_compile(schema: &SchemaManager, sq: &SelectQuery) -> Option<Self> {
        let result_cache = if super::result_cache::is_result_cacheable(schema, sq) {
            Some(super::result_cache::ResultCacheSlot::new())
        } else {
            None
        };

        if sq.recursive || !sq.ctes.is_empty() {
            return cache_carrier(result_cache);
        }

        let (compound_plan, compound_cache) = match &sq.body {
            QueryBody::Compound(comp) => {
                if let Some(plan) = build_compound_plan_static(schema, comp) {
                    (Some(Arc::new(plan)), Some(parking_lot::RwLock::new(None)))
                } else {
                    return cache_carrier(result_cache);
                }
            }
            QueryBody::Select(_) => (None, None),
            _ => return None,
        };

        if compound_plan.is_some() {
            return Some(Self {
                join_plan: None,
                join_cache: None,
                compound_plan,
                compound_cache,
                leaf_cache: parking_lot::RwLock::new(None),
                result_cache,
                lane: None,
            });
        }

        let sel = match &sq.body {
            QueryBody::Select(s) => s,
            _ => return None,
        };
        if has_any_window_function(sel)
            || sel.columns.iter().any(|c| match c {
                SelectColumn::Expr { expr, .. } => crate::parser::has_subquery(expr),
                SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                    false
                }
            })
            || sel
                .where_clause
                .as_ref()
                .is_some_and(crate::parser::has_subquery)
        {
            return cache_carrier(result_cache);
        }

        let (join_plan, join_cache) = if sel.joins.is_empty() {
            (None, None)
        } else if let Some(plan) = build_join_plan_static(schema, sel) {
            (Some(Arc::new(plan)), Some(parking_lot::RwLock::new(None)))
        } else {
            (None, None)
        };
        let lane = if sel.joins.is_empty() {
            build_select_lane(schema, sel)
        } else {
            None
        };

        Some(Self {
            join_plan,
            join_cache,
            compound_plan: None,
            compound_cache: None,
            leaf_cache: parking_lot::RwLock::new(None),
            result_cache,
            lane,
        })
    }

    /// Serve or fill the result memo against one read snapshot: the lookup
    /// generation and the executing transaction are the same snapshot, so a
    /// stored result is exactly what re-execution at that generation returns.
    fn execute_cached_read(
        &self,
        schema: &SchemaManager,
        sq: &SelectQuery,
        params: &[Value],
        slot: &super::result_cache::ResultCacheSlot,
        rtx: &mut ReadTxn<'_>,
    ) -> Result<ExecutionResult> {
        let gen = rtx.commit_generation();
        if let Some(qr) = slot.lookup(gen, params) {
            return Ok(ExecutionResult::Query(qr));
        }
        if let Some(lane) = &self.lane {
            let qr = lane.run(rtx)?;
            slot.store(gen, params, &qr);
            return Ok(ExecutionResult::Query(qr));
        }
        let result = if let (Some(plan), Some(cache)) = (&self.compound_plan, &self.compound_cache)
        {
            execute_cached_compound_with_read(rtx, plan, cache)?
        } else if let (Some(plan), Some(cache)) = (&self.join_plan, &self.join_cache) {
            let sel = match &sq.body {
                QueryBody::Select(s) => s,
                _ => unreachable!("cached join plan implies a plain select body"),
            };
            execute_cached_join_with_read(rtx, plan, cache, sel)?
        } else {
            exec_select_query_with_read(rtx, schema, sq)?
        };
        if let ExecutionResult::Query(ref qr) = result {
            slot.store(gen, params, qr);
        }
        Ok(result)
    }
}

impl CompiledPlan for CompiledSelect {
    fn execute(
        &self,
        db: &Database,
        schema: &SchemaManager,
        stmt: &Statement,
        params: &[Value],
        txn: super::compile::ActiveTxnRef<'_, '_>,
    ) -> Result<ExecutionResult> {
        let sq = match stmt {
            Statement::Select(s) => s,
            _ => {
                return Err(SqlError::Unsupported(
                    "CompiledSelect received non-SELECT statement".into(),
                ))
            }
        };

        use super::compile::ActiveTxnRef;

        if matches!(txn, ActiveTxnRef::None | ActiveTxnRef::Read(_)) {
            // Never serve under ActiveTxnRef::Write: read-your-writes.
            if let Some(slot) = &self.result_cache {
                return match txn {
                    ActiveTxnRef::Read(rtx) => {
                        self.execute_cached_read(schema, sq, params, slot, rtx)
                    }
                    _ => {
                        let mut rtx = db.begin_read();
                        self.execute_cached_read(schema, sq, params, slot, &mut rtx)
                    }
                };
            }
            if let Some(lane) = &self.lane {
                let qr = match txn {
                    ActiveTxnRef::Read(rtx) => lane.run(rtx)?,
                    _ => lane.run(&mut db.begin_read())?,
                };
                return Ok(ExecutionResult::Query(qr));
            }
            if let (Some(plan), Some(cache)) = (&self.compound_plan, &self.compound_cache) {
                return match txn {
                    ActiveTxnRef::Read(rtx) => execute_cached_compound_with_read(rtx, plan, cache),
                    _ => execute_cached_compound(db, plan, cache),
                };
            }
            if let (Some(plan), Some(cache)) = (&self.join_plan, &self.join_cache) {
                let sel = match &sq.body {
                    QueryBody::Select(s) => s,
                    _ => unreachable!("cached plan implies SelectBody::Select"),
                };
                return match txn {
                    ActiveTxnRef::Read(rtx) => execute_cached_join_with_read(rtx, plan, cache, sel),
                    _ => execute_cached_join(db, plan, cache, sel),
                };
            }
        }

        match txn {
            ActiveTxnRef::None => exec_select_query(db, schema, sq),
            ActiveTxnRef::Read(rtx) => exec_select_query_with_read(rtx, schema, sq),
            ActiveTxnRef::Write(outer) => exec_select_query_in_txn(outer, schema, sq),
        }
    }

    fn try_stream<'db>(
        &self,
        db: &'db Database,
        schema: &SchemaManager,
        stmt: &Statement,
        _params: &[Value],
    ) -> Option<Box<dyn super::compile::RowSourceIter + 'db>> {
        let (lower, table_schema, proj, columns) = stream_scan_setup(schema, stmt)?;
        let mut rtx = db.begin_read();
        let cancel = rtx.cancel_token().cloned();
        let row_count = rtx.table_entry_count(lower.as_bytes()).unwrap_or(0) as usize;
        let iter = rtx.into_table_scan_iter(lower.as_bytes(), b"").ok()?;
        Some(Box::new(StreamingSelect {
            iter,
            table_schema: Arc::new(table_schema),
            proj,
            columns,
            scratch: Vec::new(),
            row_count,
            cancel,
        }))
    }

    fn try_collect(
        &self,
        db: &Database,
        schema: &SchemaManager,
        stmt: &Statement,
        _params: &[Value],
    ) -> Option<Result<QueryResult>> {
        let (lower, table_schema, proj, columns) = stream_scan_setup(schema, stmt)?;
        Some(collect_scan(
            db,
            &lower,
            &table_schema,
            &proj,
            columns,
            &self.leaf_cache,
        ))
    }

    fn needs_txn_clock(&self) -> bool {
        // Cacheable statements are volatile-free: the clock is never read.
        self.result_cache.is_none()
    }
}

enum StreamProj {
    Identity {
        /// Push-build the full row; precomputed eligibility.
        full_push: bool,
    },
    Columns {
        idxs: Vec<usize>,
        /// Compact path: decodes straight into the output row, no scratch.
        proj_decoder: Option<ProjectedDecoder>,
        /// Fallback partial decode; None when all columns are needed.
        ctx: Option<PartialDecodeCtx>,
        /// idxs has no repeats, so projected values can be moved out.
        unique: bool,
    },
    Exprs {
        col_map: ColumnMap,
        exprs: Vec<Expr>,
        /// Decodes only the referenced columns; None when all are needed.
        ctx: Option<PartialDecodeCtx>,
    },
}

impl StreamProj {
    /// Project an already decoded row, transferring owned values where possible.
    /// Column/expression projections retain the scratch allocation for reuse.
    fn project_decoded(
        &self,
        row: &mut Vec<Value>,
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Value>> {
        check_cancel(cancel)?;
        match self {
            Self::Identity { .. } => Ok(std::mem::take(row)),
            Self::Columns { idxs, unique, .. } => {
                if *unique {
                    Ok(idxs.iter().map(|&i| std::mem::take(&mut row[i])).collect())
                } else {
                    Ok(idxs.iter().map(|&i| row[i].clone()).collect())
                }
            }
            Self::Exprs { col_map, exprs, .. } => {
                let ectx = EvalCtx::new(col_map, row).with_cancel(cancel);
                exprs.iter().map(|expr| eval_expr(expr, &ectx)).collect()
            }
        }
    }
}

struct StreamingSelect<'db> {
    iter: citadel_txn::TableIter<citadel_txn::read_txn::OwnedReadTxnAdapter<'db>>,
    table_schema: Arc<TableSchema>,
    proj: StreamProj,
    columns: Vec<String>,
    /// Reused decode buffer for projections that build a separate output row.
    scratch: Vec<Value>,
    row_count: usize,
    cancel: Option<CancelToken>,
}

impl<'db> super::compile::RowSourceIter for StreamingSelect<'db> {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>> {
        let Some((key, value)) = self.iter.next().map_err(SqlError::Storage)? else {
            return Ok(None);
        };
        Ok(Some(decode_and_project(
            &self.proj,
            &self.table_schema,
            key,
            value,
            &mut self.scratch,
            self.cancel.as_ref(),
        )?))
    }

    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn size_hint(&self) -> usize {
        self.row_count
    }
}

/// Decode `(key, value)` and project; `scratch` is reused by Columns/Exprs only.
fn decode_and_project(
    proj: &StreamProj,
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
    scratch: &mut Vec<Value>,
    cancel: Option<&CancelToken>,
) -> Result<Vec<Value>> {
    match proj {
        StreamProj::Identity { full_push } => {
            if *full_push {
                if let Some(row) = decode_full_row_push(schema, key, value)? {
                    return Ok(row);
                }
            }
            decode_full_row_with_cancel(schema, key, value, cancel)
        }
        StreamProj::Columns {
            proj_decoder: Some(pd),
            ..
        } => pd.decode(key, value),
        StreamProj::Columns { ctx, .. } | StreamProj::Exprs { ctx, .. } => {
            match ctx {
                Some(c) => c.decode_into_with_cancel(key, value, scratch, cancel)?,
                None => decode_full_row_into_with_cancel(schema, key, value, scratch, cancel)?,
            }
            proj.project_decoded(scratch, cancel)
        }
    }
}

/// Shared setup for the stream/collect fast paths (single-table SELECT, no clauses).
fn stream_scan_setup(
    schema: &SchemaManager,
    stmt: &Statement,
) -> Option<(String, TableSchema, StreamProj, Vec<String>)> {
    let sq = match stmt {
        Statement::Select(s) => s,
        _ => return None,
    };
    // Cache-carrier plans compile for CTE/derived-FROM shapes too; this scan
    // path must only ever see a plain base-table select.
    if sq.recursive || !sq.ctes.is_empty() {
        return None;
    }
    let sel = match &sq.body {
        QueryBody::Select(s) => s,
        _ => return None,
    };
    if sel.where_clause.is_some()
        || !sel.order_by.is_empty()
        || sel.limit.is_some()
        || sel.offset.is_some()
        || !sel.joins.is_empty()
        || !sel.group_by.is_empty()
        || sel.having.is_some()
        || sel.distinct
        || sel.from_subquery.is_some()
        || sel.from_args.is_some()
        || sel.from_json_table.is_some()
    {
        return None;
    }
    let lower = sel.from.to_ascii_lowercase();
    let table_schema = schema.get(&lower)?.clone();
    let proj = build_stream_proj(&sel.columns, &table_schema)?;
    let columns = projection_column_names(&sel.columns, &table_schema.columns);
    Some((lower, table_schema, proj, columns))
}

/// Materialize a full scan off borrowed page cells (no per-row key/value copy).
fn collect_scan(
    db: &Database,
    table_lower: &str,
    table_schema: &TableSchema,
    proj: &StreamProj,
    columns: Vec<String>,
    leaf_cache: &LeafScanCache,
) -> Result<QueryResult> {
    let mut rtx = db.begin_read();
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let gen = rtx.commit_generation();
    let row_count = rtx.table_entry_count(table_lower.as_bytes()).unwrap_or(0) as usize;

    // A write bumps the gen and invalidates; DDL recompiles the plan upstream.
    let cached = match &*leaf_cache.read() {
        Some((cached_gen, weak)) if *cached_gen == gen => {
            citadel_txn::read_txn::upgrade_leaves(weak)
        }
        _ => None,
    };
    let leaves = match cached {
        Some(l) => l,
        None => {
            let l = rtx
                .collect_table_leaves(table_lower.as_bytes())
                .map_err(SqlError::Storage)?;
            *leaf_cache.write() = Some((gen, citadel_txn::read_txn::downgrade_leaves(&l)));
            l
        }
    };

    let mut rows = Vec::with_capacity(row_count);
    let mut scratch: Vec<Value> = Vec::new();
    let mut err: Option<SqlError> = None;
    rtx.scan_leaves(&leaves, |key, value| {
        match decode_and_project(proj, table_schema, key, value, &mut scratch, cancel) {
            Ok(row) => {
                rows.push(row);
                true
            }
            Err(e) => {
                err = Some(e);
                false
            }
        }
    })
    .map_err(SqlError::Storage)?;
    if let Some(e) = err {
        return Err(e);
    }
    Ok(QueryResult { columns, rows })
}

fn build_projection(select_cols: &[SelectColumn], columns: &[ColumnDef]) -> Option<Vec<usize>> {
    let mut out = Vec::new();
    for col in select_cols {
        match col {
            SelectColumn::AllColumns => {
                for (i, _) in columns.iter().enumerate() {
                    out.push(i);
                }
            }
            SelectColumn::AllFromOld | SelectColumn::AllFromNew => return None,
            SelectColumn::Expr { expr, .. } => match expr {
                Expr::Column(name) => {
                    let idx = columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(name))?;
                    out.push(idx);
                }
                Expr::QualifiedColumn { column, .. } => {
                    let idx = columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(column))?;
                    out.push(idx);
                }
                _ => return None,
            },
        }
    }
    Some(out)
}

fn build_stream_proj(select_cols: &[SelectColumn], schema: &TableSchema) -> Option<StreamProj> {
    if let Some(idxs) = build_projection(select_cols, &schema.columns) {
        let identity =
            idxs.len() == schema.columns.len() && idxs.iter().enumerate().all(|(i, &p)| i == p);
        if identity {
            return Some(StreamProj::Identity {
                full_push: full_row_push_eligible(schema),
            });
        }
        let mut needed = idxs.clone();
        needed.sort_unstable();
        needed.dedup();
        let unique = needed.len() == idxs.len();
        let proj_decoder = unique
            .then(|| ProjectedDecoder::try_new(schema, &idxs))
            .flatten();
        let ctx = proj_decoder
            .is_none()
            .then(|| {
                (needed.len() < schema.columns.len())
                    .then(|| PartialDecodeCtx::new(schema, &needed))
            })
            .flatten();
        return Some(StreamProj::Columns {
            idxs,
            proj_decoder,
            ctx,
            unique,
        });
    }
    let exprs = build_expr_projection(select_cols)?;
    let mut needed: Vec<usize> = Vec::new();
    for e in &exprs {
        needed.extend(referenced_columns(e, &schema.columns));
    }
    needed.sort_unstable();
    needed.dedup();
    let ctx = (needed.len() < schema.columns.len()).then(|| PartialDecodeCtx::new(schema, &needed));
    Some(StreamProj::Exprs {
        col_map: ColumnMap::new(&schema.columns),
        exprs,
        ctx,
    })
}

fn build_expr_projection(select_cols: &[SelectColumn]) -> Option<Vec<Expr>> {
    let mut out = Vec::with_capacity(select_cols.len());
    for col in select_cols {
        match col {
            SelectColumn::Expr { expr, .. } if is_streamable_scalar(expr) => out.push(expr.clone()),
            _ => return None,
        }
    }
    (!out.is_empty()).then_some(out)
}

fn is_streamable_scalar(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Column(_) | Expr::QualifiedColumn { .. } => true,
        Expr::BinaryOp { left, op, right } => {
            !matches!(
                op,
                BinOp::JsonPathExists
                    | BinOp::JsonPathMatch
                    | BinOp::JsonPathExistsTz
                    | BinOp::JsonPathMatchTz
            ) && is_streamable_scalar(left)
                && is_streamable_scalar(right)
        }
        Expr::UnaryOp { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            is_streamable_scalar(expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => is_streamable_scalar(expr) && is_streamable_scalar(low) && is_streamable_scalar(high),
        Expr::Coalesce(args) => args.iter().all(is_streamable_scalar),
        Expr::InList { expr, list, .. } => {
            is_streamable_scalar(expr) && list.iter().all(is_streamable_scalar)
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            is_streamable_scalar(left) && is_streamable_scalar(right)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand.as_ref().is_none_or(|e| is_streamable_scalar(e))
                && conditions
                    .iter()
                    .all(|(c, r)| is_streamable_scalar(c) && is_streamable_scalar(r))
                && else_result.as_ref().is_none_or(|e| is_streamable_scalar(e))
        }
        _ => false,
    }
}

fn projection_column_names(select_cols: &[SelectColumn], columns: &[ColumnDef]) -> Vec<String> {
    let mut out = Vec::new();
    for col in select_cols {
        match col {
            SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                for c in columns {
                    out.push(c.name.clone());
                }
            }
            SelectColumn::Expr { alias: Some(a), .. } => out.push(a.clone()),
            SelectColumn::Expr { expr, alias: None } => out.push(expr_display_name(expr)),
        }
    }
    out
}

fn build_join_plan_static(schema: &SchemaManager, sel: &SelectStmt) -> Option<JoinPlanStatic> {
    for join in &sel.joins {
        if join.subquery.is_some() {
            return None;
        }
    }
    if sel.from_subquery.is_some() || sel.from_json_table.is_some() || sel.from_args.is_some() {
        return None;
    }
    let from_lower = sel.from.to_ascii_lowercase();
    let from_schema = schema.get(&from_lower)?.clone();

    let mut table_lowers = vec![from_lower];
    let mut table_schemas = vec![Arc::new(from_schema.clone())];
    let mut all_refs: Vec<(String, &TableSchema)> = vec![(
        super::join::table_alias_or_name(&sel.from, &sel.from_alias),
        &from_schema,
    )];
    let mut inner_schemas: Vec<TableSchema> = Vec::with_capacity(sel.joins.len());
    for join in &sel.joins {
        let lname = join.table.name.to_ascii_lowercase();
        let inner_schema = schema.get(&lname)?.clone();
        table_lowers.push(lname);
        inner_schemas.push(inner_schema);
    }
    for (idx, join) in sel.joins.iter().enumerate() {
        let alias = super::join::table_alias_or_name(&join.table.name, &join.table.alias);
        all_refs.push((alias, &inner_schemas[idx]));
        table_schemas.push(Arc::new(inner_schemas[idx].clone()));
    }

    let needed_plan = super::join::compute_join_needed_columns(sel, &all_refs)?;
    let mut combined_cols = super::join::build_joined_columns(&all_refs[..1]);
    let mut step_equi = Vec::with_capacity(sel.joins.len());
    for (ji, join) in sel.joins.iter().enumerate() {
        super::join::extend_joined_columns(&mut combined_cols, &all_refs[ji + 1]);
        let outer_col_count: usize = all_refs[..ji + 1]
            .iter()
            .map(|(_, s)| s.columns.len())
            .sum();
        step_equi.push(super::join::compute_equi_join_meta(
            join,
            &combined_cols,
            outer_col_count,
        ));
    }
    // Outer prefilter commutes with preserved-outer joins only.
    let outer_point = if sel.joins.iter().all(|j| {
        matches!(
            j.join_type,
            JoinType::Inner | JoinType::Cross | JoinType::Left
        )
    }) && !from_schema.has_virtual_columns()
    {
        let ambiguous = |name: &str| inner_schemas.iter().any(|s| s.column_index(name).is_some());
        sel.where_clause
            .as_ref()
            .and_then(|w| detect_outer_point_sources(w, &all_refs[0].0, &from_schema, &ambiguous))
    } else {
        None
    };
    Some(JoinPlanStatic {
        table_lowers,
        table_schemas,
        needed_per_table: needed_plan.per_table,
        output_combined: Some(needed_plan.output_combined),
        step_equi,
        outer_point,
    })
}

fn execute_cached_join(
    db: &Database,
    plan: &Arc<JoinPlanStatic>,
    cache: &parking_lot::RwLock<Option<Arc<CachedJoin>>>,
    sel: &SelectStmt,
) -> Result<ExecutionResult> {
    let mut rtx = db.begin_read();
    execute_cached_join_with_read(&mut rtx, plan, cache, sel)
}

fn execute_cached_join_with_read(
    rtx: &mut ReadTxn<'_>,
    plan: &Arc<JoinPlanStatic>,
    cache: &parking_lot::RwLock<Option<Arc<CachedJoin>>>,
    sel: &SelectStmt,
) -> Result<ExecutionResult> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let snapshot_gen = rtx.commit_generation();

    let cached: Arc<CachedJoin> = {
        let mut slot = cache.write();
        match slot.as_ref() {
            Some(c) if c.cached_gen == snapshot_gen => Arc::clone(c),
            _ => {
                let inner = build_inner_data(rtx, plan)?;
                let probes = inner
                    .iter()
                    .zip(&plan.step_equi)
                    .map(|(rows, equi)| super::join::build_probe_index(rows, equi, cancel))
                    .collect::<Result<Vec<_>>>()?;
                let arc = Arc::new(CachedJoin {
                    cached_gen: snapshot_gen,
                    inner_per_table: inner,
                    probes,
                });
                if arc.cell_count() <= JOIN_CACHE_MAX_CELLS {
                    *slot = Some(Arc::clone(&arc));
                }
                arc
            }
        }
    };

    let outer_schema = &plan.table_schemas[0];
    let outer_key = match &plan.outer_point {
        Some(sources) => resolve_point_key(sources, outer_schema)?,
        None => None,
    };
    let mut outer_rows = if let Some(key) = outer_key {
        match rtx
            .table_get(outer_schema.name.as_bytes(), &key)
            .map_err(SqlError::Storage)?
        {
            Some(value) => vec![decode_full_row_with_cancel(
                outer_schema,
                &key,
                &value,
                cancel,
            )?],
            None => Vec::new(),
        }
    } else {
        super::join::collect_rows_partial(rtx, outer_schema, &plan.needed_per_table[0])?
    };

    let mut cur_outer_pk_col: Option<usize> = if outer_schema.primary_key_columns.len() == 1 {
        Some(outer_schema.primary_key_columns[0] as usize)
    } else {
        None
    };

    let from_alias = super::join::table_alias_or_name(&sel.from, &sel.from_alias);
    let mut all_refs: Vec<(String, &TableSchema)> =
        vec![(from_alias, plan.table_schemas[0].as_ref())];
    for (idx, join) in sel.joins.iter().enumerate() {
        let alias = super::join::table_alias_or_name(&join.table.name, &join.table.alias);
        all_refs.push((alias, plan.table_schemas[idx + 1].as_ref()));
    }
    let mut combined_cols = super::join::build_joined_columns(&all_refs[..1]);

    let num_joins = sel.joins.len();
    for (ji, join) in sel.joins.iter().enumerate() {
        super::join::extend_joined_columns(&mut combined_cols, &all_refs[ji + 1]);

        let outer_col_count = if outer_rows.is_empty() {
            all_refs[..ji + 1]
                .iter()
                .map(|(_, s)| s.columns.len())
                .sum()
        } else {
            outer_rows[0].len()
        };
        let inner_col_count = all_refs[ji + 1].1.columns.len();
        let is_last = ji == num_joins - 1;
        let proj = if is_last {
            plan.output_combined
                .as_ref()
                .map(|oc| super::join::build_combine_projection(oc, outer_col_count))
        } else {
            None
        };

        outer_rows = super::join::exec_join_step_borrowed(
            outer_rows,
            &cached.inner_per_table[ji],
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            cur_outer_pk_col,
            proj.as_ref(),
            &plan.step_equi[ji],
            Some(&cached.probes[ji]),
            cancel,
        )?;
        cur_outer_pk_col = None;
    }

    if let Some(ref oc) = plan.output_combined {
        let actual_width = outer_rows.first().map_or(0, |r| r.len());
        if actual_width == oc.len() {
            let projected_cols = super::join::build_projected_columns(&combined_cols, oc);
            return process_select(outer_rows, SelectCtx::new(&projected_cols, sel, cancel));
        }
    }
    process_select(outer_rows, SelectCtx::new(&combined_cols, sel, cancel))
}

fn build_inner_data(
    rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    plan: &Arc<JoinPlanStatic>,
) -> Result<Vec<Vec<Vec<Value>>>> {
    let mut out = Vec::with_capacity(plan.table_lowers.len() - 1);
    for ji in 1..plan.table_lowers.len() {
        let schema = &plan.table_schemas[ji];
        let needed = &plan.needed_per_table[ji];
        let rows = super::join::collect_rows_partial(rtx, schema, needed)?;
        out.push(rows);
    }
    Ok(out)
}

fn build_compound_plan_static(
    schema: &SchemaManager,
    comp: &CompoundSelect,
) -> Option<CompoundPlanStatic> {
    if !comp.order_by.is_empty() || comp.limit.is_some() || comp.offset.is_some() {
        return None;
    }
    let left_branch = compound_branch_plan(schema, &comp.left)?;
    let right_branch = compound_branch_plan(schema, &comp.right)?;

    let columns = compound_branch_columns(schema, &comp.left)?;
    if columns.len() != left_branch.needed_cols.len()
        || columns.len() != right_branch.needed_cols.len()
    {
        return None;
    }

    // This lane is only reached for a query with no WITH clause, so there is nothing for a
    // branch to reference besides real tables.
    let key_colls = super::dml::body_output_collations(
        schema,
        &CteContext::default(),
        &comp.left,
        columns.len(),
    );
    Some(CompoundPlanStatic {
        op: comp.op.clone(),
        all: comp.all,
        branches: vec![left_branch, right_branch],
        columns,
        key_colls,
    })
}

fn compound_branch_plan(schema: &SchemaManager, body: &QueryBody) -> Option<BranchPlan> {
    let sel = match body {
        QueryBody::Select(s) => s,
        _ => return None,
    };
    if !sel.joins.is_empty()
        || !sel.group_by.is_empty()
        || sel.having.is_some()
        || sel.distinct
        || sel.where_clause.is_some()
        || !sel.order_by.is_empty()
        || sel.limit.is_some()
        || sel.offset.is_some()
        || sel.from_subquery.is_some()
        || sel.from_json_table.is_some()
        || sel.from_args.is_some()
        || has_any_window_function(sel)
    {
        return None;
    }
    if sel.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => {
            is_aggregate_expr(expr) || crate::parser::has_subquery(expr)
        }
        _ => false,
    }) {
        return None;
    }

    let table_lower = sel.from.to_ascii_lowercase();
    let table_schema = schema.get(&table_lower)?.clone();
    let needed_cols = resolve_branch_needed_cols(&sel.columns, &table_schema.columns)?;

    Some(BranchPlan {
        table_schema: Arc::new(table_schema),
        needed_cols,
    })
}

fn resolve_branch_needed_cols(
    select_cols: &[SelectColumn],
    table_cols: &[ColumnDef],
) -> Option<Vec<usize>> {
    let mut out = Vec::with_capacity(select_cols.len());
    for sc in select_cols {
        match sc {
            SelectColumn::AllColumns => {
                out.extend(0..table_cols.len());
            }
            SelectColumn::Expr { expr, .. } => match expr {
                Expr::Column(name) => {
                    let lname = name.to_ascii_lowercase();
                    let idx = table_cols.iter().position(|c| c.name == lname)?;
                    out.push(idx);
                }
                Expr::QualifiedColumn { column, .. } => {
                    let lname = column.to_ascii_lowercase();
                    let idx = table_cols.iter().position(|c| c.name == lname)?;
                    out.push(idx);
                }
                _ => return None,
            },
            _ => return None,
        }
    }
    Some(out)
}

fn compound_branch_columns(schema: &SchemaManager, body: &QueryBody) -> Option<Vec<String>> {
    let sel = match body {
        QueryBody::Select(s) => s,
        _ => return None,
    };
    let table_lower = sel.from.to_ascii_lowercase();
    let table_schema = schema.get(&table_lower)?;
    let mut out = Vec::with_capacity(sel.columns.len());
    for sc in &sel.columns {
        match sc {
            SelectColumn::AllColumns => {
                out.extend(table_schema.columns.iter().map(|c| c.name.clone()));
            }
            SelectColumn::Expr { alias: Some(a), .. } => out.push(a.clone()),
            SelectColumn::Expr {
                expr: Expr::Column(name),
                alias: None,
            } => out.push(name.clone()),
            SelectColumn::Expr {
                expr: Expr::QualifiedColumn { column, .. },
                alias: None,
            } => out.push(column.clone()),
            _ => return None,
        }
    }
    Some(out)
}

fn execute_cached_compound(
    db: &Database,
    plan: &Arc<CompoundPlanStatic>,
    cache: &parking_lot::RwLock<Option<Arc<CachedCompound>>>,
) -> Result<ExecutionResult> {
    let mut rtx = db.begin_read();
    execute_cached_compound_with_read(&mut rtx, plan, cache)
}

fn execute_cached_compound_with_read(
    rtx: &mut ReadTxn<'_>,
    plan: &Arc<CompoundPlanStatic>,
    cache: &parking_lot::RwLock<Option<Arc<CachedCompound>>>,
) -> Result<ExecutionResult> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let snapshot_gen = rtx.commit_generation();

    let cached: Arc<CachedCompound> = {
        let mut slot = cache.write();
        match slot.as_ref() {
            Some(c) if c.cached_gen == snapshot_gen => Arc::clone(c),
            _ => {
                let branch_rows = build_compound_branches(rtx, plan, cancel)?;
                let arc = Arc::new(CachedCompound {
                    cached_gen: snapshot_gen,
                    branch_rows,
                });
                *slot = Some(Arc::clone(&arc));
                arc
            }
        }
    };

    let total: usize = cached.branch_rows.iter().map(|b| b.len()).sum();
    let key = |row: &Vec<Value>| fold_key(row, &plan.key_colls);
    let mut work = 0usize;
    let rows = match (&plan.op, plan.all) {
        (SetOp::Union, true) => {
            let mut out = Vec::with_capacity(total);
            for branch in &cached.branch_rows {
                for row in branch {
                    check_cancel_at(cancel, work)?;
                    work += 1;
                    out.push(row.clone());
                }
            }
            out
        }
        (SetOp::Union, false) => {
            let mut seen = super::helpers::RowKeys::with_capacity(plan.key_colls.clone(), total);
            let mut out = Vec::with_capacity(total);
            for branch in &cached.branch_rows {
                for row in branch {
                    check_cancel_at(cancel, work)?;
                    work += 1;
                    if seen.insert(row) {
                        out.push(row.clone());
                    }
                }
            }
            out
        }
        (SetOp::Intersect, true) => {
            let left = &cached.branch_rows[0];
            let right = &cached.branch_rows[1];
            let mut right_counts: FxHashMap<Vec<Value>, usize> = FxHashMap::default();
            for row in right {
                check_cancel_at(cancel, work)?;
                work += 1;
                *right_counts.entry(key(row)).or_insert(0) += 1;
            }
            let mut out = Vec::new();
            for row in left {
                check_cancel_at(cancel, work)?;
                work += 1;
                if let Some(count) = right_counts.get_mut(&key(row)) {
                    if *count > 0 {
                        *count -= 1;
                        out.push(row.clone());
                    }
                }
            }
            out
        }
        (SetOp::Intersect, false) => {
            let left = &cached.branch_rows[0];
            let right = &cached.branch_rows[1];
            let mut right_set = super::helpers::RowKeys::new(plan.key_colls.clone());
            for row in right {
                check_cancel_at(cancel, work)?;
                work += 1;
                right_set.insert(row);
            }
            let mut seen = super::helpers::RowKeys::new(plan.key_colls.clone());
            let mut out = Vec::new();
            for row in left {
                check_cancel_at(cancel, work)?;
                work += 1;
                if right_set.contains_row(row) && seen.insert(row) {
                    out.push(row.clone());
                }
            }
            out
        }
        (SetOp::Except, true) => {
            let left = &cached.branch_rows[0];
            let right = &cached.branch_rows[1];
            let mut right_counts: FxHashMap<Vec<Value>, usize> = FxHashMap::default();
            for row in right {
                check_cancel_at(cancel, work)?;
                work += 1;
                *right_counts.entry(key(row)).or_insert(0) += 1;
            }
            let mut out = Vec::new();
            for row in left {
                check_cancel_at(cancel, work)?;
                work += 1;
                if let Some(count) = right_counts.get_mut(&key(row)) {
                    if *count > 0 {
                        *count -= 1;
                        continue;
                    }
                }
                out.push(row.clone());
            }
            out
        }
        (SetOp::Except, false) => {
            let left = &cached.branch_rows[0];
            let right = &cached.branch_rows[1];
            let mut right_set = super::helpers::RowKeys::new(plan.key_colls.clone());
            for row in right {
                check_cancel_at(cancel, work)?;
                work += 1;
                right_set.insert(row);
            }
            let mut seen = super::helpers::RowKeys::new(plan.key_colls.clone());
            let mut out = Vec::new();
            for row in left {
                check_cancel_at(cancel, work)?;
                work += 1;
                if !right_set.contains_row(row) && seen.insert(row) {
                    out.push(row.clone());
                }
            }
            out
        }
    };
    check_cancel(cancel)?;

    Ok(ExecutionResult::Query(QueryResult {
        columns: plan.columns.clone(),
        rows,
    }))
}

fn build_compound_branches(
    rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    plan: &Arc<CompoundPlanStatic>,
    cancel: Option<&CancelToken>,
) -> Result<Vec<Vec<Vec<Value>>>> {
    let mut out = Vec::with_capacity(plan.branches.len());
    for (branch_idx, branch) in plan.branches.iter().enumerate() {
        check_cancel_at(cancel, branch_idx)?;
        // Projection positions may repeat (`SELECT *, id`), but the partial
        // decoder expects a set of schema columns. Preserve repetitions for
        // projection while deduplicating only the scan input.
        let mut scan_cols = branch.needed_cols.clone();
        scan_cols.sort_unstable();
        scan_cols.dedup();
        let raw = super::join::collect_rows_partial(rtx, &branch.table_schema, &scan_cols)?;
        let mut projected = Vec::with_capacity(raw.len());
        for (row_idx, row) in raw.into_iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            projected.push(branch.needed_cols.iter().map(|&i| row[i].clone()).collect());
        }
        out.push(projected);
    }
    check_cancel(cancel)?;
    Ok(out)
}

#[cfg(test)]
#[path = "select_tests.rs"]
mod tests;
