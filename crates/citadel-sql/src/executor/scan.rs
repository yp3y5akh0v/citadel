use citadel::Database;
use citadel_txn::read_txn::ReadTxn;

use crate::encoding::{
    decode_composite_key, decode_key_value, decode_stored_column_raw, encode_composite_key,
    RawColumn,
};
use crate::error::{Result, SqlError};
use crate::eval::{
    eval_binary_op_public, eval_expr, is_truthy, referenced_columns, ColumnMap, CompiledExpr,
    EvalCtx,
};
use crate::parser::*;
use crate::planner::{self, ScanPlan};
use crate::types::*;

use super::helpers::*;

/// Seek key for the tightest lower range bound; BelowLower re-checks keep
/// correctness when bounds conflict.
fn index_scan_start(prefix: &[u8], range_conds: &[(BinOp, Value)]) -> Option<Vec<u8>> {
    range_conds
        .iter()
        .filter(|(op, _)| matches!(op, BinOp::Gt | BinOp::GtEq))
        .map(|(_, v)| {
            let mut k = prefix.to_vec();
            crate::encoding::encode_key_value_into(v, &mut k);
            k
        })
        .max()
}

type IndexKeyVisitor<'a> = dyn FnMut(&[u8], &[u8]) -> citadel_core::Result<bool> + 'a;
// Amortize seeks when a small LIMIT has a selective residual predicate.
const INDEX_KEY_MIN_BATCH_SIZE: usize = 32;
const INDEX_KEY_BATCH_SIZE: usize = 256;

struct IndexKeyBatch {
    keys: Vec<Vec<u8>>,
    resume: Option<Vec<u8>>,
}

/// Collect a bounded group of row keys without borrowing the transaction while
/// their base rows are filtered. The exact index key resumes the same snapshot;
/// the inclusive seek skips only that previously visited key.
fn collect_index_key_batch(
    schema: &TableSchema,
    plan: &ScanPlan,
    resume: Option<&[u8]>,
    limit: Option<usize>,
    scan: impl FnOnce(&[u8], &[u8], &mut IndexKeyVisitor<'_>) -> citadel_core::Result<()>,
) -> Result<IndexKeyBatch> {
    let ScanPlan::IndexScan {
        index_name,
        idx_table,
        prefix,
        num_prefix_cols,
        range_conds,
        is_unique,
        index_columns,
        ..
    } = plan
    else {
        unreachable!("index-key collection requires an index scan")
    };
    let num_pk_cols = schema.primary_key_columns.len();
    let num_index_cols = schema
        .index_by_name(index_name)
        .map_or(index_columns.len(), |index| index.keys.len());
    let lower = index_scan_start(prefix, range_conds);
    let start = resume.unwrap_or_else(|| lower.as_deref().unwrap_or(prefix));
    let mut batch = IndexKeyBatch {
        keys: Vec::new(),
        resume: None,
    };
    let mut error = None;
    scan(idx_table, start, &mut |key, value| {
        if !key.starts_with(prefix) {
            return Ok(false);
        }
        if resume == Some(key) {
            return Ok(true);
        }
        match check_range_conditions(key, *num_prefix_cols, range_conds, num_index_cols) {
            Ok(RangeCheck::ExceedsUpper) => return Ok(false),
            Ok(RangeCheck::BelowLower) => return Ok(true),
            Ok(RangeCheck::Match) => {}
            Err(cause) => {
                error = Some(cause);
                return Ok(false);
            }
        }
        match extract_pk_key(key, value, *is_unique, num_index_cols, num_pk_cols) {
            Ok(pk) => batch.keys.push(pk),
            Err(cause) => {
                error = Some(cause);
                return Ok(false);
            }
        }
        if limit.is_some_and(|limit| batch.keys.len() >= limit) {
            batch.resume = Some(key.to_vec());
            Ok(false)
        } else {
            Ok(true)
        }
    })
    .map_err(SqlError::Storage)?;
    if let Some(error) = error {
        return Err(error);
    }
    Ok(batch)
}

/// Column index -> key-component position when the index covers every need.
pub(super) fn covered_index_components(
    table_schema: &TableSchema,
    plan: &ScanPlan,
    needed: &[usize],
) -> Option<rustc_hash::FxHashMap<usize, usize>> {
    let ScanPlan::IndexScan { index_name, .. } = plan else {
        return None;
    };
    let idx = table_schema
        .indices
        .iter()
        .find(|i| &i.name == index_name)?;
    if idx.predicate_expr.is_some() || idx.kind != IndexKind::BTree {
        return None;
    }
    let mut comp_of: rustc_hash::FxHashMap<usize, usize> = Default::default();
    for (pos, key) in idx.keys.iter().enumerate() {
        if let IndexKey::Column {
            idx: ci,
            collate: Collation::Binary,
        } = key
        {
            comp_of.insert(*ci as usize, pos);
        }
    }
    let pk_cols = &table_schema.primary_key_columns;
    for &n in needed {
        if !pk_cols.iter().any(|&c| c as usize == n) && !comp_of.contains_key(&n) {
            return None;
        }
    }
    Some(comp_of)
}

/// Index-only row service; only Binary components and pk columns reconstruct.
pub(super) fn try_covered_index_collect_read(
    rtx: &mut ReadTxn<'_>,
    table_schema: &TableSchema,
    plan: &ScanPlan,
    where_clause: &Option<Expr>,
    needed: &[usize],
    limit: Option<usize>,
    emit_direct: Option<&[usize]>,
) -> Result<Option<Vec<Vec<Value>>>> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let Some(comp_of) = covered_index_components(table_schema, plan, needed) else {
        return Ok(None);
    };
    let ScanPlan::IndexScan {
        idx_table,
        prefix,
        num_prefix_cols,
        range_conds,
        is_unique,
        index_columns,
        ..
    } = plan
    else {
        return Ok(None);
    };
    let pk_cols = &table_schema.primary_key_columns;
    // Direct emission has no full-width row for a residual re-eval.
    let emit_direct = if where_clause.is_none() {
        emit_direct
    } else {
        None
    };

    let num_index_cols = index_columns.len();
    let num_pk_cols = pk_cols.len();
    let ncols = table_schema.columns.len();
    let col_map = table_schema.column_map();
    let start = index_scan_start(prefix, range_conds);
    let start: &[u8] = start.as_deref().unwrap_or(prefix);
    // Output slots per stream position: index components then pk columns.
    let stream_slots: Option<Vec<Vec<usize>>> = emit_direct.map(|order| {
        let mut slots = vec![Vec::new(); num_index_cols + num_pk_cols];
        for (out_i, ci) in order.iter().enumerate() {
            match pk_cols.iter().position(|&pc| pc as usize == *ci) {
                Some(p) => slots[num_index_cols + p].push(out_i),
                None => slots[comp_of[ci]].push(out_i),
            }
        }
        slots
    });
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut scan_err: Option<SqlError> = None;
    rtx.table_scan_from_fast(idx_table, start, |key, value| {
        if !key.starts_with(prefix) {
            return Ok(false);
        }
        match check_range_conditions(key, *num_prefix_cols, range_conds, num_index_cols) {
            Ok(RangeCheck::ExceedsUpper) => return Ok(false),
            Ok(RangeCheck::BelowLower) => return Ok(true),
            Ok(RangeCheck::Match) => {}
            Err(e) => {
                scan_err = Some(e);
                return Ok(false);
            }
        }
        let built = (|| -> Result<Vec<Value>> {
            if let (Some(slots), Some(order)) = (&stream_slots, emit_direct) {
                // One pass over the key/value bytes straight into the output.
                let mut out = vec![Value::Null; order.len()];
                let mut pos = 0;
                for slot in &slots[..num_index_cols] {
                    if slot.is_empty() {
                        pos += crate::encoding::skip_key_value(&key[pos..])?;
                    } else {
                        let (v, n) = decode_key_value(&key[pos..])?;
                        pos += n;
                        match slot.as_slice() {
                            [oi] => out[*oi] = v,
                            many => {
                                for &oi in many {
                                    out[oi] = v.clone();
                                }
                            }
                        }
                    }
                }
                let (pk_stream, mut ppos) = if *is_unique && !value.is_empty() {
                    (value, 0)
                } else {
                    (key, pos)
                };
                for slot in &slots[num_index_cols..] {
                    if slot.is_empty() {
                        ppos += crate::encoding::skip_key_value(&pk_stream[ppos..])?;
                    } else {
                        let (v, n) = decode_key_value(&pk_stream[ppos..])?;
                        ppos += n;
                        match slot.as_slice() {
                            [oi] => out[*oi] = v,
                            many => {
                                for &oi in many {
                                    out[oi] = v.clone();
                                }
                            }
                        }
                    }
                }
                return Ok(out);
            }
            let (mut comps, pk_vals) = if *is_unique && !value.is_empty() {
                (
                    decode_composite_key(key, num_index_cols)?,
                    decode_composite_key(value, num_pk_cols)?,
                )
            } else {
                let mut all = decode_composite_key(key, num_index_cols + num_pk_cols)?;
                let pk_vals = all.split_off(num_index_cols);
                (all, pk_vals)
            };
            let mut row = vec![Value::Null; ncols];
            for (&ci, &pos) in &comp_of {
                row[ci] = std::mem::replace(&mut comps[pos], Value::Null);
            }
            for (i, &pc) in pk_cols.iter().enumerate() {
                row[pc as usize] = pk_vals[i].clone();
            }
            Ok(row)
        })();
        let row = match built {
            Ok(r) => r,
            Err(e) => {
                scan_err = Some(e);
                return Ok(false);
            }
        };
        if let Some(expr) = where_clause {
            match eval_expr(expr, &EvalCtx::new(col_map, &row).with_cancel(cancel)) {
                Ok(v) if is_truthy(&v) => {}
                Ok(_) => return Ok(true),
                Err(e) => {
                    scan_err = Some(e);
                    return Ok(false);
                }
            }
        }
        rows.push(row);
        Ok(limit.is_none_or(|n| rows.len() < n))
    })
    .map_err(SqlError::Storage)?;
    if let Some(e) = scan_err {
        return Err(e);
    }
    Ok(Some(rows))
}

/// Entry count in bounds; caller proves full cover, NULL components skip.
pub(super) fn covered_index_count_read(
    rtx: &mut ReadTxn<'_>,
    table_schema: &TableSchema,
    plan: &ScanPlan,
) -> Result<Option<u64>> {
    let ScanPlan::IndexScan {
        index_name,
        idx_table,
        prefix,
        num_prefix_cols,
        range_conds,
        index_columns,
        ..
    } = plan
    else {
        return Ok(None);
    };
    let Some(idx) = table_schema.indices.iter().find(|i| &i.name == index_name) else {
        return Ok(None);
    };
    if idx.kind != IndexKind::BTree {
        return Ok(None);
    }
    let has_range_col = *num_prefix_cols < index_columns.len();
    let start = index_scan_start(prefix, range_conds);
    let start: &[u8] = start.as_deref().unwrap_or(prefix);
    // Key encoding is order-preserving and prefix-free: bounds compare as
    // raw component bytes, so the loop never decodes.
    let enc = |v: &Value| {
        let mut b = Vec::new();
        crate::encoding::encode_key_value_into(v, &mut b);
        b
    };
    let strict_lower: Option<Vec<u8>> = range_conds
        .iter()
        .filter(|(op, _)| matches!(op, BinOp::Gt))
        .map(|(_, v)| enc(v))
        .max();
    let uppers: Vec<(Vec<u8>, bool)> = range_conds
        .iter()
        .filter_map(|(op, v)| match op {
            BinOp::Lt => Some((enc(v), false)),
            BinOp::LtEq => Some((enc(v), true)),
            _ => None,
        })
        .collect();
    let check_bounds = !range_conds.is_empty() && has_range_col;
    let mut count = 0u64;
    rtx.table_scan_from_fast(idx_table, start, |key, _value| {
        if !key.starts_with(prefix) {
            return Ok(false);
        }
        if check_bounds {
            let rest = &key[prefix.len()..];
            // NULL never satisfies a comparison; it sorts below every bound.
            if rest.first() == Some(&crate::encoding::TAG_NULL) {
                return Ok(true);
            }
            if let Some(lb) = &strict_lower {
                if rest.starts_with(lb) {
                    return Ok(true);
                }
            }
            for (ub, inclusive) in &uppers {
                if rest.starts_with(ub.as_slice()) {
                    if !*inclusive {
                        return Ok(false);
                    }
                } else if rest > ub.as_slice() {
                    return Ok(false);
                }
            }
        }
        count += 1;
        Ok(true)
    })
    .map_err(SqlError::Storage)?;
    Ok(Some(count))
}

/// Check PK range conditions. Returns: 0 = match, 1 = below lower (skip), 2 = above upper (stop).
pub(super) fn check_pk_range(pk_val: &Value, range_conds: &[(BinOp, Value)]) -> u8 {
    for (op, bound) in range_conds {
        match op {
            BinOp::Lt if pk_val >= bound => return 2,
            BinOp::LtEq if pk_val > bound => return 2,
            BinOp::Gt if pk_val <= bound => return 1,
            BinOp::GtEq if pk_val < bound => return 1,
            _ => {}
        }
    }
    0
}

pub(super) fn extract_pk_key(
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

pub(super) fn check_range_conditions(
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
            BinOp::Lt if range_val >= *val => exceeds_upper = true,
            BinOp::LtEq if range_val > *val => exceeds_upper = true,
            BinOp::Gt if range_val <= *val => below_lower = true,
            BinOp::GtEq if range_val < *val => below_lower = true,
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

pub(super) enum RangeCheck {
    Match,
    BelowLower,
    ExceedsUpper,
}

/// Owned expression/decode metadata, reusable for a compiled schema generation.
pub(super) struct SelectScanDecodePlan {
    output: PartialDecodeCtx,
    predicate: Option<PredicateDecode>,
}

pub(super) struct SelectScanDecoder<'a> {
    plan: &'a SelectScanDecodePlan,
    where_clause: Option<&'a Expr>,
    col_map: &'a ColumnMap,
}

struct PredicateDecode {
    input: PartialDecodeCtx,
    remaining: PartialDecodeCtx,
}

impl SelectScanDecodePlan {
    pub(super) fn new(
        schema: &TableSchema,
        stmt: &SelectStmt,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Option<Self>> {
        if !schema.has_virtual_columns()
            && !schema
                .columns
                .iter()
                .any(|column| column.default_expr.is_some())
        {
            return Ok(None);
        }
        let output = SelectRowDecoder::new(schema, stmt, cancel)?;
        let predicate = stmt
            .where_clause
            .as_ref()
            .map(|expr| {
                let input = PartialDecodeCtx::new_with_cancel(
                    schema,
                    &referenced_columns(expr, &schema.columns),
                    cancel,
                )?;
                let remaining = output.remaining_after(&input);
                Ok::<_, SqlError>(PredicateDecode { input, remaining })
            })
            .transpose()?;
        Ok(output
            .into_partial()
            .map(|output| Self { output, predicate }))
    }

    pub(super) fn bind<'a>(
        &'a self,
        schema: &'a TableSchema,
        where_clause: Option<&'a Expr>,
    ) -> SelectScanDecoder<'a> {
        SelectScanDecoder {
            plan: self,
            where_clause,
            col_map: schema.column_map(),
        }
    }
}

impl SelectScanDecoder<'_> {
    fn read(
        &self,
        key: &[u8],
        value: &[u8],
        filter: Option<&Expr>,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Option<Vec<Value>>> {
        if let Some(expr) = filter {
            let predicate = self
                .plan
                .predicate
                .as_ref()
                .expect("scan filter has a decode plan");
            let mut row = predicate.input.decode_with_cancel(key, value, cancel)?;
            if !is_truthy(&eval_expr(
                expr,
                &EvalCtx::new(self.col_map, &row).with_cancel(cancel),
            )?) {
                return Ok(None);
            }
            predicate
                .remaining
                .decode_additional_into_with_cancel(key, value, &mut row, cancel)?;
            return Ok(Some(row));
        }
        self.plan
            .output
            .decode_with_cancel(key, value, cancel)
            .map(Some)
    }
}

pub(super) fn read_scan_row(
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
    filter: Option<&Expr>,
    projection: Option<&SelectScanDecoder<'_>>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Option<Vec<Value>>> {
    if let Some(projection) = projection {
        return projection.read(key, value, filter, cancel);
    }
    let row = decode_full_row_with_cancel(schema, key, value, cancel)?;
    if let Some(expr) = filter {
        if !is_truthy(&eval_expr(
            expr,
            &EvalCtx::new(schema.column_map(), &row).with_cancel(cancel),
        )?) {
            return Ok(None);
        }
    }
    Ok(Some(row))
}

#[allow(clippy::too_many_arguments)]
fn scan_step(
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
    compiled: Option<&CompiledExpr>,
    simple_pred: Option<&SimplePredicate>,
    between_pred: Option<&BetweenPredicate>,
    jsonb_pred: Option<&JsonbContainsPredicate<'_>>,
    col_map: Option<&ColumnMap>,
    partial_ctx: Option<&PartialDecodeCtx>,
    projection: Option<&SelectScanDecoder<'_>>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Option<Vec<Value>>> {
    let decode_output = || match projection {
        Some(projection) => projection
            .plan
            .output
            .decode_with_cancel(key, value, cancel),
        None => decode_full_row_with_cancel(schema, key, value, cancel),
    };
    if let Some(pred) = simple_pred {
        return if pred.matches_raw(key, value)? {
            decode_output().map(Some)
        } else {
            Ok(None)
        };
    }
    if let Some(pred) = between_pred {
        return if pred.matches_raw(key, value)? {
            decode_output().map(Some)
        } else {
            Ok(None)
        };
    }
    if let Some(pred) = jsonb_pred {
        return match pred.matches_raw(value, cancel)? {
            Some(true) => decode_output().map(Some),
            Some(false) => Ok(None),
            // Missing or differently typed stored values need the same
            // default/virtual materialization and operator semantics as an
            // ordinary scan. Keep that work in the existing row evaluator.
            None => read_scan_row(schema, key, value, Some(pred.expr), projection, cancel),
        };
    }
    if let Some(projection) = projection {
        return projection.read(key, value, projection.where_clause, cancel);
    }
    match (compiled, col_map, partial_ctx) {
        (Some(pred), Some(map), Some(pctx)) => {
            let partial = pctx.decode_with_cancel(key, value, cancel)?;
            if is_truthy(&pred.eval(&EvalCtx::new(map, &partial).with_cancel(cancel))?) {
                Ok(Some(
                    pctx.complete_with_cancel(partial, key, value, cancel)?,
                ))
            } else {
                Ok(None)
            }
        }
        (Some(pred), Some(map), None) => {
            let row = decode_full_row_with_cancel(schema, key, value, cancel)?;
            if is_truthy(&pred.eval(&EvalCtx::new(map, &row).with_cancel(cancel))?) {
                Ok(Some(row))
            } else {
                Ok(None)
            }
        }
        _ => Ok(Some(decode_full_row_with_cancel(
            schema, key, value, cancel,
        )?)),
    }
}

pub(super) fn collect_rows_read(
    db: &Database,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
    limit: Option<usize>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let mut rtx = db.begin_read();
    collect_rows_with_read(&mut rtx, table_schema, where_clause, limit)
}

pub(super) fn collect_rows_with_read(
    rtx: &mut ReadTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
    limit: Option<usize>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let plan = planner::plan_select_inverted(table_schema, where_clause);
    collect_rows_with_read_planned(rtx, table_schema, where_clause, limit, plan)
}

pub(super) fn collect_rows_with_read_planned(
    rtx: &mut ReadTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
    limit: Option<usize>,
    plan: ScanPlan,
) -> Result<(Vec<Vec<Value>>, bool)> {
    collect_rows_with_read_decoded(rtx, table_schema, where_clause, limit, plan, None)
}

pub(super) fn collect_select_rows_with_read(
    rtx: &mut ReadTxn<'_>,
    table_schema: &TableSchema,
    stmt: &SelectStmt,
    limit: Option<usize>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let cancel = rtx.cancel_token().cloned();
    let decode_plan = SelectScanDecodePlan::new(table_schema, stmt, cancel.as_ref())?;
    let projection = decode_plan
        .as_ref()
        .map(|plan| plan.bind(table_schema, stmt.where_clause.as_ref()));
    let plan = planner::plan_select_inverted(table_schema, &stmt.where_clause);
    collect_rows_with_read_decoded(
        rtx,
        table_schema,
        &stmt.where_clause,
        limit,
        plan,
        projection.as_ref(),
    )
}

pub(super) fn collect_rows_with_read_decoded(
    rtx: &mut ReadTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
    limit: Option<usize>,
    plan: ScanPlan,
    projection: Option<&SelectScanDecoder<'_>>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let lower_name = &table_schema.name;
    let columns = &table_schema.columns;
    let read_row = |key: &[u8], value: &[u8], filter: Option<&Expr>| {
        read_scan_row(table_schema, key, value, filter, projection, cancel)
    };

    match plan {
        ScanPlan::SeqScan => {
            let simple_pred = where_clause
                .as_ref()
                .and_then(|expr| try_simple_predicate(expr, table_schema));
            let between_pred = if simple_pred.is_none() {
                where_clause
                    .as_ref()
                    .and_then(|expr| try_between_predicate(expr, table_schema))
            } else {
                None
            };
            let jsonb_pred = if simple_pred.is_none() && between_pred.is_none() {
                where_clause
                    .as_ref()
                    .and_then(|expr| try_jsonb_contains_predicate(expr, table_schema))
            } else {
                None
            };
            let needs_generic_eval = where_clause.is_some()
                && simple_pred.is_none()
                && between_pred.is_none()
                && jsonb_pred.is_none();

            let col_map = needs_generic_eval.then(|| table_schema.column_map());
            let partial_ctx = if needs_generic_eval {
                if let Some(expr) = where_clause.as_ref() {
                    let needed = referenced_columns(expr, columns);
                    if needed.len() < columns.len() {
                        Some(PartialDecodeCtx::new_with_cancel(
                            table_schema,
                            &needed,
                            cancel,
                        )?)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };
            let compiled = match (col_map, where_clause.as_ref()) {
                (Some(cm), Some(expr)) => Some(CompiledExpr::compile(expr, cm)),
                _ => None,
            };

            let entry_count = rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
            let capacity = if where_clause.is_some() {
                entry_count / 4
            } else {
                entry_count
            };
            let mut rows = Vec::with_capacity(capacity);
            let mut scan_err: Option<SqlError> = None;

            rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
                let step = scan_step(
                    table_schema,
                    key,
                    value,
                    compiled.as_ref(),
                    simple_pred.as_ref(),
                    between_pred.as_ref(),
                    jsonb_pred.as_ref(),
                    col_map,
                    partial_ctx.as_ref(),
                    projection,
                    cancel,
                );
                match step {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => {}
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                }
                limit.is_none_or(|n| rows.len() < n)
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok((rows, where_clause.is_some()))
        }

        ScanPlan::PkLookup { pk_values, .. } => {
            let key = encode_composite_key(&pk_values);
            match rtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = read_row(&key, &value, where_clause.as_ref())?;
                    Ok((row.into_iter().collect(), where_clause.is_some()))
                }
                None => Ok((vec![], true)),
            }
        }

        ScanPlan::PkPrefixScan { prefix, .. } => {
            let mut rows = Vec::new();
            let mut scan_err = None;
            rtx.table_scan_prefix(lower_name.as_bytes(), &prefix, |key, value| {
                let row = read_row(key, value, where_clause.as_ref());
                match row {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => {}
                    Err(error) => scan_err = Some(error),
                }
                Ok(scan_err.is_none() && limit.is_none_or(|limit| rows.len() < limit))
            })
            .map_err(SqlError::Storage)?;
            if let Some(error) = scan_err {
                return Err(error);
            }
            Ok((rows, true))
        }

        ScanPlan::PkRangeScan {
            ref start_key,
            ref range_conds,
            num_pk_cols,
            ..
        } => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            rtx.table_scan_from(lower_name.as_bytes(), start_key, |key, value| {
                let pk_vals = match decode_composite_key(key, num_pk_cols) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                };
                match check_pk_range(&pk_vals[0], range_conds) {
                    2 => return Ok(false),
                    1 => return Ok(true),
                    _ => {}
                }
                match read_row(key, value, where_clause.as_ref()) {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => {}
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                }
                Ok(scan_err.is_none() && limit.is_none_or(|n| rows.len() < n))
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok((rows, true))
        }

        ScanPlan::IndexScan { .. } => {
            // Other index orders must still collect fully before ORDER BY.
            let ordered_limit =
                limit.filter(|_| planner::index_scan_preserves_pk_order(table_schema, &plan));
            let mut rows = Vec::new();
            let mut resume = None;
            loop {
                let remaining = ordered_limit.map(|limit| limit.saturating_sub(rows.len()));
                if remaining == Some(0) {
                    break;
                }
                let batch = collect_index_key_batch(
                    table_schema,
                    &plan,
                    resume.as_deref(),
                    remaining.map(|remaining| {
                        remaining.clamp(INDEX_KEY_MIN_BATCH_SIZE, INDEX_KEY_BATCH_SIZE)
                    }),
                    |table, start, visit| rtx.table_scan_from_fast(table, start, visit),
                )?;
                for pk_key in &batch.keys {
                    if let Some(value) = rtx
                        .table_get(lower_name.as_bytes(), pk_key)
                        .map_err(SqlError::Storage)?
                    {
                        if let Some(row) = read_row(pk_key, &value, where_clause.as_ref())? {
                            rows.push(row);
                            if ordered_limit.is_some_and(|limit| rows.len() >= limit) {
                                break;
                            }
                        }
                    }
                }
                resume = batch.resume;
                if resume.is_none() {
                    break;
                }
            }
            Ok((rows, where_clause.is_some()))
        }

        ScanPlan::InvertedScan {
            idx_table,
            probe_entries,
            recheck_expr,
            recheck_needed,
            ..
        } => {
            let candidate_pks = inverted_intersect_candidates(rtx, &idx_table, &probe_entries)?;
            let mut rows = Vec::new();
            for pk_key in &candidate_pks {
                if let Some(value) = rtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    if let Some(row) =
                        read_row(pk_key, &value, recheck_needed.then_some(&recheck_expr))?
                    {
                        rows.push(row);
                    }
                }
            }
            Ok((rows, true))
        }
    }
}

fn inverted_intersect_candidates(
    rtx: &mut ReadTxn<'_>,
    idx_table: &[u8],
    probe_entries: &[Vec<u8>],
) -> Result<Vec<Vec<u8>>> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let mut lists: Vec<Vec<Vec<u8>>> = Vec::with_capacity(probe_entries.len());
    for (entry_idx, entry) in probe_entries.iter().enumerate() {
        check_cancel_at(cancel, entry_idx)?;
        let mut prefix = entry.clone();
        prefix.push(0x1F);
        let mut list: Vec<Vec<u8>> = Vec::new();
        rtx.table_scan_from(idx_table, &prefix, |key, _value| {
            if !key.starts_with(&prefix) {
                return Ok(false);
            }
            list.push(key[prefix.len()..].to_vec());
            Ok(true)
        })
        .map_err(SqlError::Storage)?;
        if list.is_empty() {
            return Ok(Vec::new());
        }
        lists.push(list);
    }
    lists = sort_lists_by_len(lists, cancel)?;
    let mut acc = lists.remove(0);
    for (list_idx, other) in lists.into_iter().enumerate() {
        check_cancel_at(cancel, list_idx)?;
        acc = sorted_intersect(&acc, &other, cancel)?;
        if acc.is_empty() {
            return Ok(acc);
        }
    }
    check_cancel(cancel)?;
    Ok(acc)
}

fn sorted_intersect(
    a: &[Vec<u8>],
    b: &[Vec<u8>],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<u8>>> {
    check_cancel(cancel)?;
    if cancel.is_none() {
        return Ok(sorted_intersect_unchecked(a, b));
    }
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0usize, 0usize);
    let mut comparisons = 0usize;
    while i < a.len() && j < b.len() {
        check_cancel_at(cancel, comparisons)?;
        comparisons += 1;
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => {
                out.push(a[i].clone());
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }
    check_cancel(cancel)?;
    Ok(out)
}

fn sorted_intersect_unchecked(a: &[Vec<u8>], b: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => {
                out.push(a[i].clone());
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }
    out
}

pub(super) fn collect_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
    limit: Option<usize>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    collect_rows_write_decoded(wtx, table_schema, where_clause, limit, None)
}

pub(super) fn collect_select_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    stmt: &SelectStmt,
    limit: Option<usize>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let cancel = wtx.cancel_token().cloned();
    let decode_plan = SelectScanDecodePlan::new(table_schema, stmt, cancel.as_ref())?;
    let projection = decode_plan
        .as_ref()
        .map(|plan| plan.bind(table_schema, stmt.where_clause.as_ref()));
    collect_rows_write_decoded(
        wtx,
        table_schema,
        &stmt.where_clause,
        limit,
        projection.as_ref(),
    )
}

fn collect_rows_write_decoded(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
    limit: Option<usize>,
    projection: Option<&SelectScanDecoder<'_>>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;
    let columns = &table_schema.columns;
    let decode =
        |key: &[u8], value: &[u8]| decode_full_row_with_cancel(table_schema, key, value, cancel);
    let read_row = |key: &[u8], value: &[u8], filter: Option<&Expr>| {
        read_scan_row(table_schema, key, value, filter, projection, cancel)
    };

    match plan {
        ScanPlan::SeqScan => {
            if let Some(projection) = projection {
                let fast_pred = where_clause
                    .as_ref()
                    .and_then(|expr| FastPredicate::try_new(expr, table_schema));
                let mut rows = Vec::new();
                let mut scan_err = None;
                wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                    let result = match &fast_pred {
                        Some(pred) => pred.matches_raw(key, value).and_then(|matched| {
                            if matched {
                                projection.read(key, value, None, cancel)
                            } else {
                                Ok(None)
                            }
                        }),
                        None => projection.read(key, value, where_clause.as_ref(), cancel),
                    };
                    match result {
                        Ok(Some(row)) => rows.push(row),
                        Ok(None) => {}
                        Err(error) => scan_err = Some(error),
                    }
                    Ok(scan_err.is_none() && limit.is_none_or(|n| rows.len() < n))
                })
                .map_err(SqlError::Storage)?;
                if let Some(error) = scan_err {
                    return Err(error);
                }
                return Ok((rows, where_clause.is_some()));
            }
            let simple_pred = where_clause
                .as_ref()
                .and_then(|expr| try_simple_predicate(expr, table_schema));

            if let Some(ref pred) = simple_pred {
                let mut rows = Vec::new();
                let mut scan_err: Option<SqlError> = None;
                wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                    match pred.matches_raw(key, value) {
                        Ok(true) => match decode(key, value) {
                            Ok(row) => rows.push(row),
                            Err(e) => scan_err = Some(e),
                        },
                        Ok(false) => {}
                        Err(e) => scan_err = Some(e),
                    }
                    let keep_going = scan_err.is_none() && limit.is_none_or(|n| rows.len() < n);
                    Ok(keep_going)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
                return Ok((rows, true));
            }

            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;

            let col_map = table_schema.column_map();
            let partial_ctx = if let Some(expr) = where_clause.as_ref() {
                let needed = referenced_columns(expr, columns);
                if needed.len() < columns.len() {
                    Some(PartialDecodeCtx::new_with_cancel(
                        table_schema,
                        &needed,
                        cancel,
                    )?)
                } else {
                    None
                }
            } else {
                None
            };

            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                match (&where_clause, &partial_ctx) {
                    (Some(expr), Some(ctx)) => match ctx.decode_with_cancel(key, value, cancel) {
                        Ok(partial) => match eval_expr(
                            expr,
                            &EvalCtx::new(col_map, &partial).with_cancel(cancel),
                        ) {
                            Ok(val) if is_truthy(&val) => {
                                match ctx.complete_with_cancel(partial, key, value, cancel) {
                                    Ok(row) => rows.push(row),
                                    Err(e) => scan_err = Some(e),
                                }
                            }
                            Err(e) => scan_err = Some(e),
                            _ => {}
                        },
                        Err(e) => scan_err = Some(e),
                    },
                    (Some(expr), None) => match decode(key, value) {
                        Ok(row) => {
                            match eval_expr(expr, &EvalCtx::new(col_map, &row).with_cancel(cancel))
                            {
                                Ok(val) if is_truthy(&val) => rows.push(row),
                                Err(e) => scan_err = Some(e),
                                _ => {}
                            }
                        }
                        Err(e) => scan_err = Some(e),
                    },
                    _ => match decode(key, value) {
                        Ok(row) => rows.push(row),
                        Err(e) => scan_err = Some(e),
                    },
                }
                let keep_going = scan_err.is_none() && limit.is_none_or(|n| rows.len() < n);
                Ok(keep_going)
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok((rows, where_clause.is_some()))
        }

        ScanPlan::PkLookup { pk_values, .. } => {
            let key = encode_composite_key(&pk_values);
            match wtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = read_row(&key, &value, where_clause.as_ref())?;
                    Ok((row.into_iter().collect(), where_clause.is_some()))
                }
                None => Ok((vec![], true)),
            }
        }

        ScanPlan::PkPrefixScan { prefix, .. } => {
            let mut rows = Vec::new();
            let mut scan_err = None;
            wtx.table_scan_prefix(lower_name.as_bytes(), &prefix, |key, value| {
                let row = read_row(key, value, where_clause.as_ref());
                match row {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => {}
                    Err(error) => scan_err = Some(error),
                }
                Ok(scan_err.is_none() && limit.is_none_or(|limit| rows.len() < limit))
            })
            .map_err(SqlError::Storage)?;
            if let Some(error) = scan_err {
                return Err(error);
            }
            Ok((rows, true))
        }

        ScanPlan::PkRangeScan {
            ref start_key,
            ref range_conds,
            num_pk_cols,
            ..
        } => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            wtx.table_scan_from(lower_name.as_bytes(), start_key, |key, value| {
                let pk_vals = match decode_composite_key(key, num_pk_cols) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                };
                match check_pk_range(&pk_vals[0], range_conds) {
                    2 => return Ok(false),
                    1 => return Ok(true),
                    _ => {}
                }
                match read_row(key, value, where_clause.as_ref()) {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => {}
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                }
                Ok(scan_err.is_none() && limit.is_none_or(|n| rows.len() < n))
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok((rows, true))
        }

        ScanPlan::IndexScan { .. } => {
            // Other index orders must still collect fully before ORDER BY.
            let ordered_limit =
                limit.filter(|_| planner::index_scan_preserves_pk_order(table_schema, &plan));
            let mut rows = Vec::new();
            let mut resume = None;
            loop {
                let remaining = ordered_limit.map(|limit| limit.saturating_sub(rows.len()));
                if remaining == Some(0) {
                    break;
                }
                let batch = collect_index_key_batch(
                    table_schema,
                    &plan,
                    resume.as_deref(),
                    remaining.map(|remaining| {
                        remaining.clamp(INDEX_KEY_MIN_BATCH_SIZE, INDEX_KEY_BATCH_SIZE)
                    }),
                    |table, start, visit| wtx.table_scan_from(table, start, visit),
                )?;
                for pk_key in &batch.keys {
                    if let Some(value) = wtx
                        .table_get(lower_name.as_bytes(), pk_key)
                        .map_err(SqlError::Storage)?
                    {
                        if let Some(row) = read_row(pk_key, &value, where_clause.as_ref())? {
                            rows.push(row);
                            if ordered_limit.is_some_and(|limit| rows.len() >= limit) {
                                break;
                            }
                        }
                    }
                }
                resume = batch.resume;
                if resume.is_none() {
                    break;
                }
            }
            Ok((rows, where_clause.is_some()))
        }

        ScanPlan::InvertedScan { .. } => {
            unreachable!("InvertedScan only from plan_select_inverted")
        }
    }
}

pub(super) fn collect_keyed_rows_read(
    db: &Database,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let mut rtx = db.begin_read();
    collect_keyed_rows_with_read(&mut rtx, table_schema, where_clause)
}

pub(super) fn collect_keyed_rows_with_read(
    rtx: &mut ReadTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let decode =
        |key: &[u8], value: &[u8]| decode_full_row_with_cancel(table_schema, key, value, cancel);
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;

    match plan {
        ScanPlan::SeqScan => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            rtx.table_for_each(lower_name.as_bytes(), |key, value| {
                match decode(key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => scan_err = Some(e),
                }
                Ok(())
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok(rows)
        }

        ScanPlan::PkLookup { pk_values, .. } => {
            let key = encode_composite_key(&pk_values);
            match rtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = decode(&key, &value)?;
                    Ok(vec![(key, row)])
                }
                None => Ok(vec![]),
            }
        }

        ScanPlan::PkPrefixScan { prefix, .. } => {
            let mut rows = Vec::new();
            let mut scan_err = None;
            rtx.table_scan_prefix(lower_name.as_bytes(), &prefix, |key, value| {
                match decode(key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(error) => scan_err = Some(error),
                }
                Ok(scan_err.is_none())
            })
            .map_err(SqlError::Storage)?;
            if let Some(error) = scan_err {
                return Err(error);
            }
            Ok(rows)
        }

        ScanPlan::PkRangeScan {
            ref start_key,
            ref range_conds,
            num_pk_cols,
            ..
        } => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            rtx.table_scan_from(lower_name.as_bytes(), start_key, |key, value| {
                let pk_vals = match decode_composite_key(key, num_pk_cols) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                };
                match check_pk_range(&pk_vals[0], range_conds) {
                    2 => return Ok(false),
                    1 => return Ok(true),
                    _ => {}
                }
                match decode(key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                }
                Ok(scan_err.is_none())
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok(rows)
        }

        ScanPlan::IndexScan {
            index_name,
            idx_table,
            prefix,
            num_prefix_cols,
            range_conds,
            is_unique,
            index_columns,
            ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = table_schema
                .index_by_name(&index_name)
                .map_or(index_columns.len(), |idx| idx.keys.len());
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();
            {
                let start = index_scan_start(&prefix, &range_conds);
                let start: &[u8] = start.as_deref().unwrap_or(&prefix);
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_from_fast(&idx_table, start, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols)
                    {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
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
            }
            let mut rows = Vec::new();
            for pk_key in &pk_keys {
                if let Some(value) = rtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    rows.push((pk_key.clone(), decode(pk_key, &value)?));
                }
            }
            Ok(rows)
        }

        ScanPlan::InvertedScan { .. } => {
            unreachable!("InvertedScan only from plan_select_inverted")
        }
    }
}

pub(super) fn collect_keyed_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let decode =
        |key: &[u8], value: &[u8]| decode_full_row_with_cancel(table_schema, key, value, cancel);
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;

    match plan {
        ScanPlan::SeqScan => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            wtx.table_for_each(lower_name.as_bytes(), |key, value| {
                match decode(key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => scan_err = Some(e),
                }
                Ok(())
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok(rows)
        }

        ScanPlan::PkLookup { pk_values, .. } => {
            let key = encode_composite_key(&pk_values);
            match wtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = decode(&key, &value)?;
                    Ok(vec![(key, row)])
                }
                None => Ok(vec![]),
            }
        }

        ScanPlan::PkPrefixScan { prefix, .. } => {
            let mut rows = Vec::new();
            let mut scan_err = None;
            wtx.table_scan_prefix(lower_name.as_bytes(), &prefix, |key, value| {
                match decode(key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(error) => scan_err = Some(error),
                }
                Ok(scan_err.is_none())
            })
            .map_err(SqlError::Storage)?;
            if let Some(error) = scan_err {
                return Err(error);
            }
            Ok(rows)
        }

        ScanPlan::PkRangeScan {
            ref start_key,
            ref range_conds,
            num_pk_cols,
            ..
        } => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            wtx.table_scan_from(lower_name.as_bytes(), start_key, |key, value| {
                let pk_vals = match decode_composite_key(key, num_pk_cols) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                };
                match check_pk_range(&pk_vals[0], range_conds) {
                    2 => return Ok(false),
                    1 => return Ok(true),
                    _ => {}
                }
                match decode(key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                }
                Ok(scan_err.is_none())
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok(rows)
        }

        ScanPlan::IndexScan {
            index_name,
            idx_table,
            prefix,
            num_prefix_cols,
            range_conds,
            is_unique,
            index_columns,
            ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = table_schema
                .index_by_name(&index_name)
                .map_or(index_columns.len(), |idx| idx.keys.len());
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let start = index_scan_start(&prefix, &range_conds);
                let start: &[u8] = start.as_deref().unwrap_or(&prefix);
                let mut scan_err: Option<SqlError> = None;
                wtx.table_scan_from(&idx_table, start, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols)
                    {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
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
            }

            let mut rows = Vec::new();
            for pk_key in &pk_keys {
                if let Some(value) = wtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    rows.push((pk_key.clone(), decode(pk_key, &value)?));
                }
            }
            Ok(rows)
        }

        ScanPlan::InvertedScan { .. } => {
            unreachable!("InvertedScan only from plan_select_inverted")
        }
    }
}

pub(super) enum FastPredicate {
    Simple(SimplePredicate),
    Between(BetweenPredicate),
}

impl FastPredicate {
    pub(super) fn try_new(expr: &Expr, schema: &TableSchema) -> Option<Self> {
        try_simple_predicate(expr, schema)
            .map(Self::Simple)
            .or_else(|| try_between_predicate(expr, schema).map(Self::Between))
    }

    pub(super) fn matches_raw(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        match self {
            Self::Simple(predicate) => predicate.matches_raw(key, value),
            Self::Between(predicate) => predicate.matches_raw(key, value),
        }
    }
}

pub(super) struct SimplePredicate {
    is_pk: bool,
    pk_pos: usize,
    nonpk_idx: usize,
    op: BinOp,
    literal: Value,
    arithmetic: Option<Box<ArithmeticTransform>>,
    num_pk_cols: usize,
    default_val: Option<Value>,
}

struct ArithmeticTransform {
    op: BinOp,
    offset: Value,
    comparison_reversed: bool,
}

impl SimplePredicate {
    pub(super) fn matches_raw(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        if self.is_pk {
            if self.num_pk_cols == 1 {
                return self.matches_value(&decode_key_value(key)?.0);
            }
            let pk = decode_composite_key(key, self.num_pk_cols)?;
            return self.matches_value(&pk[self.pk_pos]);
        }
        match decode_stored_column_raw(value, self.nonpk_idx)? {
            Some(raw) if self.arithmetic.is_none() => raw_matches_op(&raw, self.op, &self.literal),
            Some(raw) => self.matches_value(&raw.to_value()?),
            None => self.matches_value(self.default_val.as_ref().unwrap_or(&Value::Null)),
        }
    }

    fn matches_value(&self, value: &Value) -> Result<bool> {
        let Some(arithmetic) = self.arithmetic.as_deref() else {
            return Ok(raw_matches_op_value(value, self.op, &self.literal));
        };
        let computed = eval_binary_op_public(value, arithmetic.op, &arithmetic.offset)?;
        let result = if arithmetic.comparison_reversed {
            eval_binary_op_public(&self.literal, self.op, &computed)?
        } else {
            eval_binary_op_public(&computed, self.op, &self.literal)?
        };
        Ok(is_truthy(&result))
    }
}

/// Single-column `BETWEEN` compiled into raw-bytes comparisons.
pub(super) struct BetweenPredicate {
    is_pk: bool,
    pk_pos: usize,
    nonpk_idx: usize,
    low: Value,
    high: Value,
    negated: bool,
    num_pk_cols: usize,
    default_val: Option<Value>,
}

impl BetweenPredicate {
    pub(super) fn matches_raw(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        if self.is_pk {
            if self.num_pk_cols == 1 {
                return Ok(self.matches_value(&decode_key_value(key)?.0));
            }
            let pk = decode_composite_key(key, self.num_pk_cols)?;
            return Ok(self.matches_value(&pk[self.pk_pos]));
        }
        let Some(raw) = decode_stored_column_raw(value, self.nonpk_idx)? else {
            return Ok(self
                .default_val
                .as_ref()
                .is_some_and(|default| self.matches_value(default)));
        };
        if matches!(raw, RawColumn::Null) {
            return Ok(false);
        }
        let ge = raw_matches_op(&raw, BinOp::GtEq, &self.low)?;
        let le = raw_matches_op(&raw, BinOp::LtEq, &self.high)?;
        let in_range = ge && le;
        Ok(if self.negated { !in_range } else { in_range })
    }

    fn matches_value(&self, value: &Value) -> bool {
        if value.is_null() {
            return false;
        }
        let in_range = raw_matches_op_value(value, BinOp::GtEq, &self.low)
            && raw_matches_op_value(value, BinOp::LtEq, &self.high);
        if self.negated {
            !in_range
        } else {
            in_range
        }
    }
}

pub(super) fn try_between_predicate(expr: &Expr, schema: &TableSchema) -> Option<BetweenPredicate> {
    let (col_name, low, high, negated) = match expr {
        Expr::Between {
            expr: col_expr,
            low,
            high,
            negated,
        } => match (col_expr.as_ref(), low.as_ref(), high.as_ref()) {
            (Expr::Column(name), Expr::Literal(lo), Expr::Literal(hi)) => {
                (name.as_str(), lo.clone(), hi.clone(), *negated)
            }
            _ => return None,
        },
        _ => return None,
    };

    let col_idx = schema.column_index(col_name)?;
    if matches!(
        schema.columns[col_idx].generated_kind,
        Some(crate::parser::GeneratedKind::Virtual)
    ) {
        return None;
    }
    if schema.columns[col_idx].collation != crate::types::Collation::Binary {
        return None;
    }

    // Coerce TEXT/INTEGER bounds to the column's temporal type for same-typed compare.
    let col_type = schema.columns[col_idx].data_type;
    let coerce_bound = |v: Value| -> Option<Value> {
        if matches!(
            col_type,
            DataType::Date | DataType::Time | DataType::Timestamp | DataType::Interval
        ) && matches!(v, Value::Text(_) | Value::Integer(_))
        {
            v.coerce_into(col_type)
        } else {
            Some(v)
        }
    };
    let low = coerce_bound(low)?;
    let high = coerce_bound(high)?;
    if !raw_comparison_supported(col_type, &low) || !raw_comparison_supported(col_type, &high) {
        return None;
    }

    let non_pk = schema.non_pk_indices();

    if let Some(pk_pos) = schema
        .primary_key_columns
        .iter()
        .position(|&i| i as usize == col_idx)
    {
        Some(BetweenPredicate {
            is_pk: true,
            pk_pos,
            nonpk_idx: 0,
            low,
            high,
            negated,
            num_pk_cols: schema.primary_key_columns.len(),
            default_val: None,
        })
    } else {
        let nonpk_order = non_pk.iter().position(|&i| i == col_idx)?;
        let nonpk_idx = schema.encoding_positions()[nonpk_order] as usize;
        let default_val =
            try_cached_column_default(&schema.columns[col_idx], schema.is_strict(), None)?;
        Some(BetweenPredicate {
            is_pk: false,
            pk_pos: 0,
            nonpk_idx,
            low,
            high,
            negated,
            num_pk_cols: schema.primary_key_columns.len(),
            default_val,
        })
    }
}

pub(super) fn try_simple_predicate(expr: &Expr, schema: &TableSchema) -> Option<SimplePredicate> {
    let (operand, mut op, literal, reversed) = match expr {
        Expr::BinaryOp { left, op, right } => match (left.as_ref(), right.as_ref()) {
            (operand, Expr::Literal(lit)) => (operand, *op, lit.clone(), false),
            (Expr::Literal(lit), operand) => (operand, *op, lit.clone(), true),
            _ => return None,
        },
        _ => return None,
    };

    if !matches!(
        op,
        BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::Gt | BinOp::LtEq | BinOp::GtEq
    ) {
        return None;
    }

    let (col_name, arithmetic) = match operand {
        Expr::Column(name) => (name.as_str(), None),
        Expr::BinaryOp { left, op, right } if matches!(op, BinOp::Add | BinOp::Sub) => {
            let (Expr::Column(name), Expr::Literal(offset)) = (left.as_ref(), right.as_ref())
            else {
                return None;
            };
            (
                name.as_str(),
                Some(Box::new(ArithmeticTransform {
                    op: *op,
                    offset: offset.clone(),
                    comparison_reversed: reversed,
                })),
            )
        }
        _ => return None,
    };

    let col_idx = schema.column_index(col_name)?;
    if matches!(
        schema.columns[col_idx].generated_kind,
        Some(crate::parser::GeneratedKind::Virtual)
    ) {
        return None;
    }
    if schema.columns[col_idx].collation != crate::types::Collation::Binary {
        return None;
    }

    let col_type = schema.columns[col_idx].data_type;
    let literal = if arithmetic.is_some() {
        if !matches!(
            col_type,
            DataType::Integer
                | DataType::Real
                | DataType::Date
                | DataType::Time
                | DataType::Timestamp
        ) {
            return None;
        }
        literal
    } else {
        if reversed {
            op = flip_cmp_op(op)?;
        }
        let literal = if matches!(
            col_type,
            DataType::Date | DataType::Time | DataType::Timestamp | DataType::Interval
        ) && matches!(literal, Value::Text(_) | Value::Integer(_))
        {
            literal.coerce_into(col_type)?
        } else {
            literal
        };
        if !raw_comparison_supported(col_type, &literal) {
            return None;
        }
        literal
    };
    let non_pk = schema.non_pk_indices();

    if let Some(pk_pos) = schema
        .primary_key_columns
        .iter()
        .position(|&i| i as usize == col_idx)
    {
        Some(SimplePredicate {
            is_pk: true,
            pk_pos,
            nonpk_idx: 0,
            op,
            literal,
            arithmetic,
            num_pk_cols: schema.primary_key_columns.len(),
            default_val: None,
        })
    } else {
        let nonpk_order = non_pk.iter().position(|&i| i == col_idx)?;
        let nonpk_idx = schema.encoding_positions()[nonpk_order] as usize;
        let default_val =
            try_cached_column_default(&schema.columns[col_idx], schema.is_strict(), None)?;
        if arithmetic.is_some()
            && default_val.as_ref().is_some_and(|value| {
                !matches!(
                    value,
                    Value::Null
                        | Value::Integer(_)
                        | Value::Real(_)
                        | Value::Date(_)
                        | Value::Time(_)
                        | Value::Timestamp(_)
                        | Value::Interval { .. }
                )
            })
        {
            return None;
        }
        Some(SimplePredicate {
            is_pk: false,
            pk_pos: 0,
            nonpk_idx,
            op,
            literal,
            arithmetic,
            num_pk_cols: schema.primary_key_columns.len(),
            default_val,
        })
    }
}

pub(super) struct JsonbContainsPredicate<'a> {
    nonpk_idx: usize,
    literal: std::sync::Arc<[u8]>,
    expr: &'a Expr,
}

impl JsonbContainsPredicate<'_> {
    /// None requests ordinary row materialization; it is not SQL NULL.
    pub(super) fn matches_raw(
        &self,
        value: &[u8],
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Option<bool>> {
        match decode_stored_column_raw(value, self.nonpk_idx)? {
            Some(RawColumn::Jsonb(bytes)) => {
                crate::json::jsonb_contains_bytes_with_cancel(bytes, &self.literal, cancel)
                    .map(Some)
            }
            Some(RawColumn::Null) => Ok(Some(false)),
            _ => Ok(None),
        }
    }
}

pub(super) fn try_jsonb_contains_predicate<'a>(
    expr: &'a Expr,
    schema: &TableSchema,
) -> Option<JsonbContainsPredicate<'a>> {
    let (col_name, lit_expr) = match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::JsonContains,
            right,
        } => match left.as_ref() {
            Expr::Column(name) => (name.as_str(), right.as_ref()),
            _ => return None,
        },
        _ => return None,
    };
    let col_idx = schema.column_index(col_name)?;
    if schema.columns[col_idx].data_type != DataType::Jsonb {
        return None;
    }
    if matches!(
        schema.columns[col_idx].generated_kind,
        Some(crate::parser::GeneratedKind::Virtual)
    ) {
        return None;
    }
    let nonpk_order = schema.non_pk_indices().iter().position(|&i| i == col_idx)?;
    let nonpk_idx = schema.encoding_positions()[nonpk_order] as usize;
    // Successful evaluation without a row alone does not prove that an
    // expression may be evaluated once. Reuse the planner's shared proof.
    if !crate::eval::is_statement_constant(lit_expr) {
        return None;
    }
    let literal = match eval_const_expr(lit_expr).ok()? {
        Value::Jsonb(b) => b,
        _ => return None,
    };
    Some(JsonbContainsPredicate {
        nonpk_idx,
        literal,
        expr,
    })
}

pub(super) fn flip_cmp_op(op: BinOp) -> Option<BinOp> {
    match op {
        BinOp::Eq => Some(BinOp::Eq),
        BinOp::NotEq => Some(BinOp::NotEq),
        BinOp::Lt => Some(BinOp::Gt),
        BinOp::Gt => Some(BinOp::Lt),
        BinOp::LtEq => Some(BinOp::GtEq),
        BinOp::GtEq => Some(BinOp::LtEq),
        _ => None,
    }
}

fn raw_comparison_supported(column_type: DataType, literal: &Value) -> bool {
    if literal.is_null() || matches!(column_type, DataType::Interval | DataType::Vector { .. }) {
        return false;
    }
    let literal_type = literal.data_type();
    column_type == literal_type
        || (matches!(column_type, DataType::Integer | DataType::Real)
            && matches!(literal_type, DataType::Integer | DataType::Real))
}

pub(super) fn raw_matches_op(raw: &RawColumn, op: BinOp, literal: &Value) -> Result<bool> {
    if matches!(raw, RawColumn::Null) || literal.is_null() {
        return Ok(false);
    }
    // Keep mixed numeric and NaN comparisons identical to expression evaluation.
    match raw {
        RawColumn::Integer(value) => {
            return Ok(raw_matches_op_value(&Value::Integer(*value), op, literal));
        }
        RawColumn::Real(value) => {
            return Ok(raw_matches_op_value(&Value::Real(*value), op, literal));
        }
        _ => {}
    }
    Ok(match op {
        BinOp::Eq => raw.eq_value(literal)?,
        BinOp::NotEq => !raw.eq_value(literal)?,
        BinOp::Lt => raw.cmp_value(literal)? == Some(std::cmp::Ordering::Less),
        BinOp::Gt => raw.cmp_value(literal)? == Some(std::cmp::Ordering::Greater),
        BinOp::LtEq => raw
            .cmp_value(literal)?
            .is_some_and(|o| o != std::cmp::Ordering::Greater),
        BinOp::GtEq => raw
            .cmp_value(literal)?
            .is_some_and(|o| o != std::cmp::Ordering::Less),
        _ => false,
    })
}

pub(super) fn raw_matches_op_value(val: &Value, op: BinOp, literal: &Value) -> bool {
    if val.is_null() || literal.is_null() {
        return false;
    }
    match op {
        BinOp::Eq => val == literal,
        BinOp::NotEq => val != literal,
        BinOp::Lt => val < literal,
        BinOp::Gt => val > literal,
        BinOp::LtEq => val <= literal,
        BinOp::GtEq => val >= literal,
        _ => false,
    }
}

#[cfg(test)]
#[path = "scan_tests.rs"]
mod tests;
