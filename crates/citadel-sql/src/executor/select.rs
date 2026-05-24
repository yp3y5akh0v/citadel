use std::sync::Arc;

use citadel::Database;
use rustc_hash::FxHashMap;

use crate::encoding::{
    decode_column_raw, decode_column_with_offset, decode_composite_key, decode_pk_integer,
    row_non_pk_count, RawColumn,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, referenced_columns, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use citadel_txn::write_txn::WriteTxn;

use super::aggregate::*;
use super::compile::CompiledPlan;
use super::correlated::*;
use super::cte::*;
use super::dml::*;
use super::helpers::*;
use super::scan::*;
use super::view::*;
use super::window::*;
use super::CteContext;

fn try_virtual_table(
    name: &str,
    db: &Database,
    schema: &SchemaManager,
) -> Option<Result<QueryResult>> {
    let canonical = match name {
        "timezone_names" => "pg_timezone_names",
        "timezone_abbrevs" => "pg_timezone_abbrevs",
        other => other,
    };
    let vt = schema.get_virtual(canonical)?;
    Some(vt.scan(db, schema))
}

pub(super) fn exec_select(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    if stmt.from.is_empty() && stmt.from_subquery.is_none() {
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

    if has_lateral(stmt) {
        return exec_select_lateral(db, schema, stmt, ctes);
    }
    if has_non_lateral_derived(stmt) {
        return exec_select_with_derived(db, schema, stmt, ctes);
    }

    if stmt.from_args.is_some() && crate::json::is_srf_name(&stmt.from) {
        return exec_select_with_srf(db, schema, stmt, ctes);
    }
    if stmt.from_json_table.is_some() {
        return exec_select_with_json_table(db, schema, stmt, ctes);
    }

    let lower_name = stmt.from.to_ascii_lowercase();

    if let Some(vt_result) = try_virtual_table(&lower_name, db, schema) {
        let vt_result = vt_result?;
        if stmt.joins.is_empty() {
            return exec_select_from_cte(&vt_result, stmt, &mut |sub| {
                exec_subquery_read(db, schema, sub, ctes)
            });
        }
        let mut vt_ctes = ctes.clone();
        vt_ctes.insert(lower_name.clone(), vt_result);
        return super::exec_select_join_with_ctes(stmt, &vt_ctes, &mut |name| {
            super::scan_table_read_or_view(db, schema, name)
        });
    }

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

    if let Some(view_def) = schema.get_view(&lower_name) {
        if let Some(fused) = try_fuse_view(stmt, schema, view_def)? {
            return exec_select(db, schema, &fused, ctes);
        }
        let view_qr = exec_view_read(db, schema, view_def)?;
        if stmt.joins.is_empty() {
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
        let (mut rows, remaining_where) =
            build_and_scan_correlated_read(db, schema, stmt, table_schema, &corr_ctx)?;
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

    if let Some(result) = try_inverted_ts_rank_topk(db, table_schema, stmt)? {
        return Ok(result);
    }

    if let Some(result) = try_inverted_index_only(db, table_schema, stmt)? {
        return Ok(result);
    }

    let scan_limit = compute_scan_limit(stmt);
    let (rows, predicate_applied) =
        collect_rows_read(db, table_schema, &stmt.where_clause, scan_limit)?;
    process_select(&table_schema.columns, rows, stmt, predicate_applied)
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

fn eval_compiled(eval: &CompiledPhrase, per_probe_positions: &[Vec<u16>], out: &mut Vec<u16>) {
    out.clear();
    match eval {
        CompiledPhrase::Leaf(idx) => {
            out.extend_from_slice(&per_probe_positions[*idx]);
        }
        CompiledPhrase::Phrase {
            distance,
            left,
            right,
        } => {
            let mut lp = Vec::new();
            let mut rp = Vec::new();
            eval_compiled(left, per_probe_positions, &mut lp);
            eval_compiled(right, per_probe_positions, &mut rp);
            if lp.is_empty() || rp.is_empty() {
                return;
            }
            let (mut i, mut j) = (0usize, 0usize);
            while i < lp.len() && j < rp.len() {
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
}

fn weight_default_score(packed: u16) -> f64 {
    match packed >> 14 {
        3 => 1.0,
        2 => 0.4,
        1 => 0.2,
        _ => 0.1,
    }
}

fn ts_rank_from_index_positions(positions_per_lex: &[&[u16]]) -> f64 {
    let mut score = 0.0_f64;
    for positions in positions_per_lex {
        if positions.is_empty() {
            continue;
        }
        let weight_sum: f64 = positions.iter().map(|&p| weight_default_score(p)).sum();
        let tf = (positions.len() as f64).ln_1p();
        score += weight_sum * (1.0 + tf);
    }
    score
}

fn try_inverted_ts_rank_topk(
    db: &Database,
    table_schema: &TableSchema,
    stmt: &SelectStmt,
) -> Result<Option<ExecutionResult>> {
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
    let mut out_col_names: Vec<String> = Vec::with_capacity(stmt.columns.len());
    let mut rank_alias: Option<String> = None;
    let mut saw_rank = false;
    for sc in &stmt.columns {
        let (expr, alias) = match sc {
            SelectColumn::Expr { expr, alias } => (expr, alias.clone()),
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
                out_col_names.push(alias.unwrap_or_else(|| n.clone()));
            }
            Expr::Function { name, args, .. }
                if name.eq_ignore_ascii_case("ts_rank") && args.len() == 2 =>
            {
                let arg_col = match &args[0] {
                    Expr::Column(c) => c.to_ascii_lowercase(),
                    Expr::QualifiedColumn { column, .. } => column.to_ascii_lowercase(),
                    _ => return Ok(None),
                };
                if arg_col != fts_col_name {
                    return Ok(None);
                }
                let col_map = crate::eval::ColumnMap::new(&[]);
                let ctx = crate::eval::EvalCtx::new(&col_map, &[]);
                let q = match crate::eval::eval_expr(&args[1], &ctx) {
                    Ok(Value::TsQuery(b)) => b,
                    _ => return Ok(None),
                };
                let _ = q;
                out_cols.push(OutCol::TsRank);
                let name = alias.clone().unwrap_or_else(|| "ts_rank".to_string());
                rank_alias = Some(name.clone());
                out_col_names.push(name);
                saw_rank = true;
            }
            _ => return Ok(None),
        }
    }
    if !saw_rank {
        return Ok(None);
    }

    let rank_alias = rank_alias.unwrap();
    if stmt.order_by.len() != 1 {
        return Ok(None);
    }
    let order = &stmt.order_by[0];
    let order_name = match &order.expr {
        Expr::Column(n) => n.to_ascii_lowercase(),
        Expr::QualifiedColumn { column, .. } => column.to_ascii_lowercase(),
        _ => return Ok(None),
    };
    if order_name != rank_alias.to_ascii_lowercase() {
        return Ok(None);
    }
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
    let mut rtx = db.begin_read();
    for entry in &probe_entries {
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

    fn score_to_key(s: f64) -> i64 {
        let bits = s.to_bits() as i64;
        if bits < 0 {
            !bits
        } else {
            bits ^ i64::MIN
        }
    }
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    let mut heap: BinaryHeap<Reverse<(i64, i64)>> = BinaryHeap::with_capacity(limit + 1);

    let driver_idx = probes
        .iter()
        .enumerate()
        .min_by_key(|(_, p)| p.pks.len())
        .map(|(i, _)| i)
        .unwrap();
    let driver = probes.swap_remove(driver_idx);
    let mut indices = vec![0usize; probes.len()];
    let mut positions_per_lex: Vec<&[u16]> = vec![&[]; probe_entries.len()];

    'outer: for di in 0..driver.pks.len() {
        let pk = driver.pks[di];
        for (pi, probe) in probes.iter().enumerate() {
            while indices[pi] < probe.pks.len() && probe.pks[indices[pi]] < pk {
                indices[pi] += 1;
            }
            if indices[pi] >= probe.pks.len() || probe.pks[indices[pi]] != pk {
                continue 'outer;
            }
        }
        let dr_s = driver.offs[di] as usize;
        let dr_e = driver.offs[di + 1] as usize;
        positions_per_lex[0] = &driver.data[dr_s..dr_e];
        for (pi, probe) in probes.iter().enumerate() {
            let idx = indices[pi];
            let s = probe.offs[idx] as usize;
            let e = probe.offs[idx + 1] as usize;
            positions_per_lex[pi + 1] = &probe.data[s..e];
        }
        let score = ts_rank_from_index_positions(&positions_per_lex);
        let key = score_to_key(score);
        if heap.len() < limit {
            heap.push(Reverse((key, pk)));
        } else if let Some(Reverse((min_key, _))) = heap.peek() {
            if key > *min_key {
                heap.pop();
                heap.push(Reverse((key, pk)));
            }
        }
    }

    fn key_to_score(k: i64) -> f64 {
        let bits = if k < 0 { !k } else { k ^ i64::MIN };
        f64::from_bits(bits as u64)
    }
    let mut sorted: Vec<(i64, i64)> = heap.into_iter().map(|r| r.0).collect();
    sorted.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));

    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(sorted.len());
    for (key, pk) in sorted {
        let score = key_to_score(key);
        let mut row = Vec::with_capacity(out_cols.len());
        for col in &out_cols {
            match col {
                OutCol::Pk => row.push(Value::Integer(pk)),
                OutCol::TsRank => row.push(Value::Real(score)),
            }
        }
        rows.push(row);
    }
    Ok(Some(ExecutionResult::Query(QueryResult {
        columns: out_col_names,
        rows,
    })))
}

fn try_inverted_index_only(
    db: &Database,
    table_schema: &TableSchema,
    stmt: &SelectStmt,
) -> Result<Option<ExecutionResult>> {
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

    let mut rtx = db.begin_read();
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
            for entry in &phrase_lexemes {
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

                'outer: for di in 0..driver.pks.len() {
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
                'outer2: for di in 0..driver.pks.len() {
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
                    eval_compiled(&compiled, &probe_positions, &mut out_pos);
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
            for entry in &phrase_lexemes {
                let mut prefix = entry.clone();
                prefix.push(0x1F);
                let mut list: Vec<(Vec<u8>, Vec<u16>)> = Vec::new();
                rtx.table_scan_from_fast(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    let pk = key[prefix.len()..].to_vec();
                    let mut positions = Vec::with_capacity(value.len() / 2);
                    let mut i = 0;
                    while i + 2 <= value.len() {
                        positions.push(u16::from_le_bytes([value[i], value[i + 1]]));
                        i += 2;
                    }
                    list.push((pk, positions));
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if list.is_empty() {
                    return Ok(Some(ExecutionResult::Query(QueryResult {
                        columns: out_col_names,
                        rows: Vec::new(),
                    })));
                }
                per_probe.push(list);
            }
            per_probe.sort_by_key(|l| l.len());
            let first = per_probe.remove(0);
            let mut candidates: Vec<(Vec<u8>, Vec<Vec<u16>>)> = first
                .into_iter()
                .map(|(pk, positions)| (pk, vec![positions]))
                .collect();
            for other in per_probe {
                let mut out: Vec<(Vec<u8>, Vec<Vec<u16>>)> =
                    Vec::with_capacity(candidates.len().min(other.len()));
                let (mut i, mut j) = (0usize, 0usize);
                while i < candidates.len() && j < other.len() {
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
            for (pk, per_probe_positions) in candidates {
                eval_compiled(&compiled, &per_probe_positions, &mut out_positions);
                if !out_positions.is_empty() {
                    matched.push(pk);
                }
            }
            matched
        }
    } else if single_int_pk {
        let mut lists: Vec<Vec<i64>> = Vec::with_capacity(probe_entries.len());
        for entry in &probe_entries {
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
        lists.sort_by_key(|l| l.len());
        let mut acc = lists.remove(0);
        for other in lists {
            let mut out: Vec<i64> = Vec::with_capacity(acc.len().min(other.len()));
            let (mut i, mut j) = (0usize, 0usize);
            while i < acc.len() && j < other.len() {
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
        for entry in &probe_entries {
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
        lists.sort_by_key(|l| l.len());
        let mut acc = lists.remove(0);
        for other in lists {
            let mut out = Vec::with_capacity(acc.len().min(other.len()));
            let (mut i, mut j) = (0usize, 0usize);
            while i < acc.len() && j < other.len() {
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
        ints.into_iter()
            .map(|id| vec![Value::Integer(id)])
            .collect()
    } else if single_int_fast {
        let mut rows = Vec::with_capacity(acc.len());
        for pk_bytes in &acc {
            let id = decode_pk_integer(pk_bytes)?;
            rows.push(vec![Value::Integer(id)]);
        }
        rows
    } else {
        let mut rows = Vec::with_capacity(acc.len());
        for pk_bytes in &acc {
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
        result_rows.sort_by(|a, b| {
            for &(pos, desc) in &order_cols {
                let cmp = a[pos].cmp(&b[pos]);
                if cmp != std::cmp::Ordering::Equal {
                    return if desc { cmp.reverse() } else { cmp };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_const_int(limit_expr)?.max(0) as usize;
        result_rows.truncate(limit);
    }
    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_const_int(offset_expr)?.max(0) as usize;
        if offset >= result_rows.len() {
            result_rows.clear();
        } else {
            result_rows = result_rows.split_off(offset);
        }
    }

    Ok(Some(ExecutionResult::Query(QueryResult {
        columns: out_col_names,
        rows: result_rows,
    })))
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
                interval_months,
                interval_days,
                interval_micros,
                is_interval,
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
                Value::Interval {
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
                Value::Null => {}
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric or INTERVAL".into(),
                        got: val.data_type().to_string(),
                    })
                }
            },
            AggState::Avg {
                sum,
                count,
                interval_months,
                interval_days,
                interval_micros,
                is_interval,
            } => match val {
                Value::Integer(i) => {
                    *sum += *i as f64;
                    *count += 1;
                }
                Value::Real(r) => {
                    *sum += r;
                    *count += 1;
                }
                Value::Interval {
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
                Value::Null => {}
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric or INTERVAL".into(),
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
                interval_months,
                interval_days,
                interval_micros,
                is_interval,
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
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric or INTERVAL".into(),
                        got: "non-numeric".into(),
                    })
                }
            },
            AggState::Avg {
                sum,
                count,
                interval_months,
                interval_days,
                interval_micros,
                is_interval,
            } => match raw {
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
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric or INTERVAL".into(),
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
    /// When `Some`, evaluates WHERE on raw column bytes without decoding the row.
    fast_pred: Option<FastPredicate>,
}

pub(super) enum FastPredicate {
    Simple(SimplePredicate),
    Between(BetweenPredicate),
}

impl FastPredicate {
    fn matches_raw(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        match self {
            FastPredicate::Simple(p) => p.matches_raw(key, value),
            FastPredicate::Between(p) => p.matches_raw(key, value),
        }
    }
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
                    distinct,
                } if args.len() == 1 => {
                    if *distinct {
                        return Ok(None);
                    }
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

        // Raw-bytes predicate is only safe when every agg is CountStar.
        let all_count_star = ops.iter().all(|(op, _)| matches!(op, StreamAgg::CountStar));
        let fast_pred = if all_count_star {
            stmt.where_clause.as_ref().and_then(|expr| {
                try_simple_predicate(expr, table_schema)
                    .map(FastPredicate::Simple)
                    .or_else(|| {
                        try_between_predicate(expr, table_schema).map(FastPredicate::Between)
                    })
            })
        } else {
            None
        };

        Ok(Some(Self {
            ops,
            partial_ctx,
            raw_targets,
            num_pk_cols,
            nonpk_agg_defaults,
            fast_pred,
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
            match eval_expr(expr, &EvalCtx::new(col_map, &row)) {
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
                    distinct,
                } if args.len() == 1 => {
                    if *distinct {
                        return Ok(None);
                    }
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
        let mut groups: FxHashMap<i64, Vec<AggState>> = FxHashMap::default();
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
    collation: crate::types::Collation,
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
        let (sort_expr, explicit_coll): (&Expr, Option<crate::types::Collation>) = match &ob.expr {
            Expr::Collate { expr: e, collation } => (e.as_ref(), Some(*collation)),
            other => (other, None),
        };
        let col_idx = match resolve_simple_col(sort_expr, &col_map) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let collation = explicit_coll.unwrap_or_else(|| schema.columns[col_idx].collation);

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
            collation,
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
            collation: crate::types::Collation,
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
                    (false, false) => {
                        if self.collation != crate::types::Collation::Binary {
                            if let (Value::Text(a), Value::Text(b)) =
                                (&self.c.sort_key, &other.c.sort_key)
                            {
                                self.collation.cmp_text(a, b)
                            } else {
                                self.c.sort_key.cmp(&other.c.sort_key)
                            }
                        } else {
                            self.c.sort_key.cmp(&other.c.sort_key)
                        }
                    }
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
                        (false, false) => {
                            if self.collation != crate::types::Collation::Binary {
                                if let (Value::Text(a), Value::Text(b)) =
                                    (&sort_key, &top.c.sort_key)
                                {
                                    self.collation.cmp_text(a, b)
                                } else {
                                    sort_key.cmp(&top.c.sort_key)
                                }
                            } else {
                                sort_key.cmp(&top.c.sort_key)
                            }
                        }
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
                collation: self.collation,
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
    let mut seen: rustc_hash::FxHashSet<Vec<u8>> = rustc_hash::FxHashSet::default();
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

pub(super) struct ReadIo<'a> {
    pub db: &'a Database,
}

impl LateralIo for ReadIo<'_> {
    fn exec_select(&mut self, schema: &SchemaManager, sq: &SelectQuery) -> Result<QueryResult> {
        match super::cte::exec_select_query(self.db, schema, sq)? {
            ExecutionResult::Query(qr) => Ok(qr),
            _ => Err(SqlError::Plan("expected Query result".into())),
        }
    }
    fn scan_table(
        &mut self,
        schema: &SchemaManager,
        name: &str,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        super::scan_table_read_or_view(self.db, schema, name)
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

fn materialize_derived(
    schema: &SchemaManager,
    derived: &DerivedTable,
    io: &mut dyn LateralIo,
) -> Result<QueryResult> {
    io.exec_select(schema, &derived.query)
}

fn exec_select_with_srf(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let args_exprs = stmt
        .from_args
        .as_ref()
        .expect("from_args present when exec_select_with_srf called");

    let upper_name = stmt.from.to_ascii_uppercase();
    let (columns, rows) = match upper_name.as_str() {
        "JSONB_POPULATE_RECORD" | "JSONB_POPULATE_RECORDSET" => {
            populate_record_dispatch(&upper_name, args_exprs, schema)?
        }
        _ => {
            let arg_values: Vec<Value> = args_exprs
                .iter()
                .map(|e| {
                    let col_map = ColumnMap::new(&[]);
                    eval_expr(e, &EvalCtx::new(&col_map, &[]))
                })
                .collect::<Result<_>>()?;
            crate::json::dispatch_srf(&stmt.from, &arg_values)?
        }
    };

    let alias = stmt
        .from_alias
        .clone()
        .unwrap_or_else(|| stmt.from.to_ascii_lowercase());

    let mut new_ctes = ctes.clone();
    new_ctes.insert(alias.to_ascii_lowercase(), QueryResult { columns, rows });

    let mut new_stmt = stmt.clone();
    new_stmt.from = alias;
    new_stmt.from_args = None;
    exec_select(db, schema, &new_stmt, &new_ctes)
}

fn populate_record_dispatch(
    upper_name: &str,
    args_exprs: &[Expr],
    schema: &SchemaManager,
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
    let jsonb_val = eval_expr(&args_exprs[1], &EvalCtx::new(&col_map, &[]))?;
    let columns: Vec<String> = target_schema
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    if jsonb_val.is_null() {
        return Ok((columns, vec![]));
    }
    let j = crate::json::value_to_serde(&jsonb_val)?;
    let rows = match upper_name {
        "JSONB_POPULATE_RECORD" => {
            let obj = j.as_object().ok_or_else(|| {
                SqlError::InvalidValue("jsonb_populate_record requires JSON object".into())
            })?;
            vec![crate::json::populate_record_row(
                obj,
                &target_schema.columns,
            )?]
        }
        "JSONB_POPULATE_RECORDSET" => {
            let arr = j.as_array().ok_or_else(|| {
                SqlError::InvalidValue("jsonb_populate_recordset requires JSON array".into())
            })?;
            arr.iter()
                .map(|elem| {
                    let obj = elem.as_object().ok_or_else(|| {
                        SqlError::InvalidValue(
                            "jsonb_populate_recordset array elements must be objects".into(),
                        )
                    })?;
                    crate::json::populate_record_row(obj, &target_schema.columns)
                })
                .collect::<Result<Vec<_>>>()?
        }
        _ => unreachable!(),
    };
    Ok((columns, rows))
}

fn exec_select_with_json_table(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let spec = stmt
        .from_json_table
        .as_ref()
        .expect("from_json_table present when exec_select_with_json_table called");
    let col_map = ColumnMap::new(&[]);
    let source_val = eval_expr(&spec.source, &EvalCtx::new(&col_map, &[]))?;
    let (columns, rows) = crate::json::materialize_json_table(&source_val, spec)?;

    let alias = stmt.from_alias.clone().unwrap_or_else(|| stmt.from.clone());
    let mut new_ctes = ctes.clone();
    new_ctes.insert(alias.to_ascii_lowercase(), QueryResult { columns, rows });

    let mut new_stmt = stmt.clone();
    new_stmt.from = alias;
    new_stmt.from_json_table = None;
    exec_select(db, schema, &new_stmt, &new_ctes)
}

fn exec_select_with_derived(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let mut new_ctes = ctes.clone();
    let mut new_stmt = stmt.clone();
    let mut io = ReadIo { db };

    if let Some(d) = stmt.from_subquery.as_ref() {
        let qr = materialize_derived(schema, d, &mut io)?;
        new_ctes.insert(d.alias.to_ascii_lowercase(), qr);
        new_stmt.from = d.alias.clone();
        new_stmt.from_alias = None;
        new_stmt.from_subquery = None;
    }
    for j in new_stmt.joins.iter_mut() {
        if let Some(d) = j.subquery.take() {
            let qr = materialize_derived(schema, &d, &mut io)?;
            new_ctes.insert(d.alias.to_ascii_lowercase(), qr);
            j.table = TableRef {
                name: d.alias.clone(),
                alias: None,
                args: None,
            };
        }
    }

    exec_select(db, schema, &new_stmt, &new_ctes)
}

fn exec_select_lateral(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let mut io = ReadIo { db };
    exec_select_lateral_with_io(schema, stmt, ctes, &mut io)
}

pub(super) fn exec_select_lateral_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    let mut io = WriteIo { wtx };
    exec_select_lateral_with_io(schema, stmt, ctes, &mut io)
}

fn exec_select_lateral_with_io(
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
    io: &mut dyn LateralIo,
) -> Result<ExecutionResult> {
    if !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || stmt.distinct
        || stmt
            .columns
            .iter()
            .any(|c| matches!(c, SelectColumn::Expr { expr, .. } if is_aggregate_expr(expr)))
    {
        return Err(SqlError::Unsupported(
            "GROUP BY / HAVING / DISTINCT / aggregates with LATERAL are not supported in v0.13"
                .into(),
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
        let qr = materialize_derived(schema, d, io)?;
        new_ctes.insert(d.alias.to_ascii_lowercase(), qr);
        from_name = d.alias.clone();
        from_alias = None;
    }

    let (outer_schema, mut outer_rows) = match new_ctes.get(&from_name.to_ascii_lowercase()) {
        Some(cte_qr) => (
            super::cte::build_cte_schema(&from_name, cte_qr),
            cte_qr.rows.clone(),
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
            let qr = materialize_derived(schema, derived, io)?;
            new_ctes.insert(derived.alias.to_ascii_lowercase(), qr);
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
            let outer_qr = QueryResult {
                columns: combined_cols.iter().map(|c| c.name.clone()).collect(),
                rows: std::mem::take(&mut outer_rows),
            };
            new_ctes.insert(mini.from.clone(), outer_qr);
            let qr = match super::exec_select_join_with_ctes(&mini, &new_ctes, &mut |n| {
                io.scan_table(schema, n)
            })? {
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

        for outer_row in outer_rows.drain(..) {
            let bound_query = bind_query_with_outer(&derived.query, &outer_row, &outer_col_map)?;
            let inner_qr = io.exec_select(schema, &bound_query)?;
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
            if inner_qr.rows.is_empty() {
                if matches!(join.join_type, JoinType::Left) {
                    let mut combined = outer_row.clone();
                    combined.resize(combined.len() + inner_count, Value::Null);
                    if let (Some(on), Some(cm)) = (&join.on_clause, &combined_col_map) {
                        if !matches!(
                            eval_expr(on, &EvalCtx::new(cm, &combined)),
                            Ok(v) if is_truthy(&v)
                        ) {
                            continue;
                        }
                    }
                    new_rows.push(combined);
                }
                continue;
            }
            let on_filter_needed = join.on_clause.is_some();
            for inner_row in &inner_qr.rows {
                let mut combined = outer_row.clone();
                combined.extend(inner_row.iter().cloned());
                if on_filter_needed {
                    let on = join.on_clause.as_ref().unwrap();
                    let cm = combined_col_map.as_ref().unwrap();
                    if !matches!(
                        eval_expr(on, &EvalCtx::new(cm, &combined)),
                        Ok(v) if is_truthy(&v)
                    ) {
                        continue;
                    }
                }
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
    process_select(&combined_cols, outer_rows, &clean_stmt, false)
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
) -> Result<Option<LateralRows>> {
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

    let proj_plan = build_projection_indices(&sel.columns, &inner_qr.columns);
    let probe_columns: Vec<String> = match proj_plan.as_ref() {
        Some(p) => p.iter().map(|(name, _)| name.clone()).collect(),
        None => inner_qr.columns.clone(),
    };

    let mut groups: FxHashMap<Vec<Value>, Vec<Vec<Value>>> = FxHashMap::default();
    let inner_col_idx: Vec<usize> = corr.iter().map(|&(_, inner_idx)| inner_idx).collect();
    for row in inner_qr.rows {
        let key: Vec<Value> = inner_col_idx.iter().map(|&i| row[i].clone()).collect();
        if key.iter().any(|v| matches!(v, Value::Null)) {
            continue;
        }
        groups.entry(key).or_default().push(row);
    }
    if let Some(n) = limit_n {
        for v in groups.values_mut() {
            v.truncate(n);
        }
    }

    let outer_idx: Vec<usize> = corr.iter().map(|&(o, _)| o).collect();
    let mut new_rows: Vec<Vec<Value>> = Vec::new();
    for outer_row in outer_rows {
        let key: Vec<Value> = outer_idx.iter().map(|&i| outer_row[i].clone()).collect();
        let inner_rows = groups.get(&key);
        match inner_rows {
            Some(rows) if !rows.is_empty() => {
                for inner_row in rows {
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
            rows.retain(
                |row| match eval_expr(where_expr, &EvalCtx::new(&col_map, row)) {
                    Ok(val) => is_truthy(&val),
                    Err(_) => false,
                },
            );
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

        let mut seen: rustc_hash::FxHashSet<Vec<Value>> =
            rustc_hash::FxHashSet::with_capacity_and_hasher(
                projected.len().min(1024),
                Default::default(),
            );
        projected.retain(|row| {
            if seen.contains(row) {
                false
            } else {
                seen.insert(row.clone());
                true
            }
        });

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

pub struct CompiledSelect {
    join_plan: Option<Arc<JoinPlanStatic>>,
    join_cache: Option<parking_lot::RwLock<Option<Arc<CachedJoin>>>>,
    compound_plan: Option<Arc<CompoundPlanStatic>>,
    compound_cache: Option<parking_lot::RwLock<Option<Arc<CachedCompound>>>>,
}

struct JoinPlanStatic {
    table_lowers: Vec<String>,
    table_schemas: Vec<Arc<TableSchema>>,
    needed_per_table: Vec<Vec<usize>>,
    output_combined: Option<Vec<usize>>,
}

struct CachedJoin {
    cached_gen: u64,
    inner_per_table: Vec<Vec<Vec<Value>>>,
}

struct CompoundPlanStatic {
    op: SetOp,
    all: bool,
    branches: Vec<BranchPlan>,
    columns: Vec<String>,
}

struct BranchPlan {
    table_schema: Arc<TableSchema>,
    needed_cols: Vec<usize>,
}

struct CachedCompound {
    cached_gen: u64,
    branch_rows: Vec<Vec<Vec<Value>>>,
}

impl CompiledSelect {
    pub fn try_compile(schema: &SchemaManager, sq: &SelectQuery) -> Option<Self> {
        if sq.recursive || !sq.ctes.is_empty() {
            return None;
        }

        let (compound_plan, compound_cache) = match &sq.body {
            QueryBody::Compound(comp) => {
                if let Some(plan) = build_compound_plan_static(schema, comp) {
                    (Some(Arc::new(plan)), Some(parking_lot::RwLock::new(None)))
                } else {
                    return None;
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
            return None;
        }

        let (join_plan, join_cache) = if sel.joins.is_empty() {
            (None, None)
        } else if let Some(plan) = build_join_plan_static(schema, sel) {
            (Some(Arc::new(plan)), Some(parking_lot::RwLock::new(None)))
        } else {
            (None, None)
        };

        Some(Self {
            join_plan,
            join_cache,
            compound_plan: None,
            compound_cache: None,
        })
    }
}

impl CompiledPlan for CompiledSelect {
    fn execute(
        &self,
        db: &Database,
        schema: &SchemaManager,
        stmt: &Statement,
        _params: &[Value],
        wtx: Option<&mut WriteTxn<'_>>,
    ) -> Result<ExecutionResult> {
        let sq = match stmt {
            Statement::Select(s) => s,
            _ => {
                return Err(SqlError::Unsupported(
                    "CompiledSelect received non-SELECT statement".into(),
                ))
            }
        };

        if let (Some(plan), Some(cache), None) =
            (&self.compound_plan, &self.compound_cache, wtx.as_ref())
        {
            return execute_cached_compound(db, plan, cache);
        }

        if let (Some(plan), Some(cache), None) = (&self.join_plan, &self.join_cache, wtx.as_ref()) {
            let sel = match &sq.body {
                QueryBody::Select(s) => s,
                _ => unreachable!("cached plan implies SelectBody::Select"),
            };
            return execute_cached_join(db, plan, cache, sel);
        }

        match wtx {
            None => exec_select_query(db, schema, sq),
            Some(outer) => exec_select_query_in_txn(outer, schema, sq),
        }
    }

    fn try_stream<'db>(
        &self,
        db: &'db Database,
        schema: &SchemaManager,
        stmt: &Statement,
        _params: &[Value],
    ) -> Option<Box<dyn super::compile::RowSourceIter + 'db>> {
        let sq = match stmt {
            Statement::Select(s) => s,
            _ => return None,
        };
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
        {
            return None;
        }
        let lower = sel.from.to_ascii_lowercase();
        let table_schema = schema.get(&lower)?.clone();
        let projection = build_projection(&sel.columns, &table_schema.columns)?;
        let columns = projection_column_names(&sel.columns, &table_schema.columns);
        let rtx = db.begin_read();
        let iter = rtx.into_table_scan_iter(lower.as_bytes(), b"").ok()?;
        Some(Box::new(StreamingSelect {
            iter,
            table_schema: Arc::new(table_schema),
            projection,
            columns,
        }))
    }
}

struct StreamingSelect<'db> {
    iter: citadel_txn::TableIter<citadel_txn::read_txn::OwnedReadTxnAdapter<'db>>,
    table_schema: Arc<TableSchema>,
    projection: Vec<usize>,
    columns: Vec<String>,
}

impl<'db> super::compile::RowSourceIter for StreamingSelect<'db> {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>> {
        match self.iter.next().map_err(SqlError::Storage)? {
            Some((key, value)) => {
                let full = decode_full_row(&self.table_schema, key, value)?;
                let out: Vec<Value> = self.projection.iter().map(|&i| full[i].clone()).collect();
                Ok(Some(out))
            }
            None => Ok(None),
        }
    }

    fn columns(&self) -> &[String] {
        &self.columns
    }
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
    Some(JoinPlanStatic {
        table_lowers,
        table_schemas,
        needed_per_table: needed_plan.per_table,
        output_combined: Some(needed_plan.output_combined),
    })
}

fn execute_cached_join(
    db: &Database,
    plan: &Arc<JoinPlanStatic>,
    cache: &parking_lot::RwLock<Option<Arc<CachedJoin>>>,
    sel: &SelectStmt,
) -> Result<ExecutionResult> {
    let mut rtx = db.begin_read();
    let snapshot_gen = rtx.commit_generation();

    let cached: Arc<CachedJoin> = {
        let mut slot = cache.write();
        match slot.as_ref() {
            Some(c) if c.cached_gen == snapshot_gen => Arc::clone(c),
            _ => {
                let inner = build_inner_data(&mut rtx, plan)?;
                let arc = Arc::new(CachedJoin {
                    cached_gen: snapshot_gen,
                    inner_per_table: inner,
                });
                *slot = Some(Arc::clone(&arc));
                arc
            }
        }
    };

    let outer_schema = &plan.table_schemas[0];
    let mut outer_rows =
        super::join::collect_rows_partial(&mut rtx, outer_schema, &plan.needed_per_table[0])?;

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

        let (equi_pairs, is_pure_equi) =
            super::join::compute_equi_join_meta(join, &combined_cols, outer_col_count);

        outer_rows = super::join::exec_join_step_borrowed(
            outer_rows,
            &cached.inner_per_table[ji],
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            cur_outer_pk_col,
            proj.as_ref(),
            &equi_pairs,
            is_pure_equi,
        );
        cur_outer_pk_col = None;
    }
    drop(rtx);

    if let Some(ref oc) = plan.output_combined {
        let actual_width = outer_rows.first().map_or(0, |r| r.len());
        if actual_width == oc.len() {
            let projected_cols = super::join::build_projected_columns(&combined_cols, oc);
            return process_select(&projected_cols, outer_rows, sel, false);
        }
    }
    process_select(&combined_cols, outer_rows, sel, false)
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

    Some(CompoundPlanStatic {
        op: comp.op.clone(),
        all: comp.all,
        branches: vec![left_branch, right_branch],
        columns,
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
                out.clear();
                for i in 0..table_cols.len() {
                    out.push(i);
                }
                return Some(out);
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
                out.clear();
                for c in &table_schema.columns {
                    out.push(c.name.clone());
                }
                return Some(out);
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
    let snapshot_gen = rtx.commit_generation();

    let cached: Arc<CachedCompound> = {
        let mut slot = cache.write();
        match slot.as_ref() {
            Some(c) if c.cached_gen == snapshot_gen => Arc::clone(c),
            _ => {
                let branch_rows = build_compound_branches(&mut rtx, plan)?;
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
    let rows = match (&plan.op, plan.all) {
        (SetOp::Union, true) => {
            let mut out = Vec::with_capacity(total);
            for branch in &cached.branch_rows {
                for row in branch {
                    out.push(row.clone());
                }
            }
            out
        }
        (SetOp::Union, false) => {
            let mut seen: rustc_hash::FxHashSet<Vec<Value>> =
                rustc_hash::FxHashSet::with_capacity_and_hasher(total, Default::default());
            let mut out = Vec::with_capacity(total);
            for branch in &cached.branch_rows {
                for row in branch {
                    if seen.insert(row.clone()) {
                        out.push(row.clone());
                    }
                }
            }
            out
        }
        (SetOp::Intersect, true) => {
            let left = &cached.branch_rows[0];
            let right = &cached.branch_rows[1];
            let mut right_counts: FxHashMap<&Vec<Value>, usize> = FxHashMap::default();
            for row in right {
                *right_counts.entry(row).or_insert(0) += 1;
            }
            let mut out = Vec::new();
            for row in left {
                if let Some(count) = right_counts.get_mut(row) {
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
            let right_set: rustc_hash::FxHashSet<&Vec<Value>> = right.iter().collect();
            let mut seen: rustc_hash::FxHashSet<Vec<Value>> = rustc_hash::FxHashSet::default();
            let mut out = Vec::new();
            for row in left {
                if right_set.contains(row) && seen.insert(row.clone()) {
                    out.push(row.clone());
                }
            }
            out
        }
        (SetOp::Except, true) => {
            let left = &cached.branch_rows[0];
            let right = &cached.branch_rows[1];
            let mut right_counts: FxHashMap<&Vec<Value>, usize> = FxHashMap::default();
            for row in right {
                *right_counts.entry(row).or_insert(0) += 1;
            }
            let mut out = Vec::new();
            for row in left {
                if let Some(count) = right_counts.get_mut(row) {
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
            let right_set: rustc_hash::FxHashSet<&Vec<Value>> = right.iter().collect();
            let mut seen: rustc_hash::FxHashSet<Vec<Value>> = rustc_hash::FxHashSet::default();
            let mut out = Vec::new();
            for row in left {
                if !right_set.contains(row) && seen.insert(row.clone()) {
                    out.push(row.clone());
                }
            }
            out
        }
    };

    Ok(ExecutionResult::Query(QueryResult {
        columns: plan.columns.clone(),
        rows,
    }))
}

fn build_compound_branches(
    rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    plan: &Arc<CompoundPlanStatic>,
) -> Result<Vec<Vec<Vec<Value>>>> {
    let mut out = Vec::with_capacity(plan.branches.len());
    for branch in &plan.branches {
        let raw =
            super::join::collect_rows_partial(rtx, &branch.table_schema, &branch.needed_cols)?;
        let projected: Vec<Vec<Value>> = raw
            .into_iter()
            .map(|row| branch.needed_cols.iter().map(|&i| row[i].clone()).collect())
            .collect();
        out.push(projected);
    }
    Ok(out)
}
