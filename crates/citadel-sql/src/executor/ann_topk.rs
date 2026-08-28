//! Plans for `SELECT ... ORDER BY col <dist> :q LIMIT k`: [`AnnTopKPlan`] uses a
//! cached PRISM index; [`VectorTopKPlan`] streams a bounded-heap top-k when no
//! index applies or inside a write txn (uncommitted rows).

use std::any::Any;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;

use citadel::CancelToken;
use citadel_txn::read_txn::ReadTxn;
use citadel_txn::write_txn::WriteTxn;
use citadel_vector::segment::SegmentOperationError;
use citadel_vector::{AnnIndex, Filter, Metric};
use rustc_hash::{FxHashMap, FxHashSet};
use zeroize::Zeroizing;

use crate::encoding::{
    decode_column_raw, decode_pk_integer, encode_int_key_into, encode_key_value,
    encode_key_value_collated_into,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::aggregate::is_aggregate_expr;
use super::ann_persist;
use super::helpers::{
    check_cancel, check_cancel_at, decode_full_row_with_cancel, eval_const_expr, eval_const_int,
    project_rows, project_rows_with_cancel, sort_vec_by,
};
use super::window::has_any_window_function;

type StorageResult<T> = std::result::Result<T, citadel_core::Error>;
type ScanRow<'a> = dyn FnMut(&[u8], &[u8]) -> Result<bool> + 'a;
type RawScanRow<'a> = dyn FnMut(&[u8], &[u8]) -> StorageResult<bool> + 'a;
/// Recall candidate: (distance in SQL operator units, row id, decoded row).
type RankedRow = (f64, i64, Vec<Value>);

/// Scan + point-get over a read or write txn, materializing overflow values.
pub(super) trait AnnScan {
    fn ann_scan(&mut self, table: &[u8], f: &mut ScanRow<'_>) -> Result<()>;
    /// Forward scan from `start_key` (inclusive); O(tail) for the tail merge.
    fn ann_scan_from(&mut self, table: &[u8], start_key: &[u8], f: &mut ScanRow<'_>) -> Result<()>;
    fn ann_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Commit generation this snapshot reflects; `None` when the view has uncommitted
    /// writes - such an index cannot enter the shared cache.
    fn cache_generation(&self) -> Option<u64>;
    /// The table's live non-ABA CoW stamp (root page id, root page txn id) - a
    /// lookup plus one page read, not a table scan.
    fn ann_table_root_stamp(&mut self, table: &[u8]) -> Result<Option<(u64, u64)>>;
    /// Clone the operation token so post-scan work can poll it without
    /// retaining an immutable borrow of the transaction.
    fn ann_cancel_token(&self) -> Option<CancelToken>;
}

/// Adapt a storage-level scan to report `SqlError`, surfacing the first callback error.
fn bridge_scan(
    scan: impl FnOnce(&mut RawScanRow<'_>) -> StorageResult<()>,
    f: &mut ScanRow<'_>,
) -> Result<()> {
    let mut cb_err: Option<SqlError> = None;
    scan(&mut |key, value| match f(key, value) {
        Ok(go) => Ok(go),
        Err(e) => {
            cb_err = Some(e);
            Ok(false)
        }
    })
    .map_err(SqlError::Storage)?;
    match cb_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

impl AnnScan for ReadTxn<'_> {
    fn ann_scan(&mut self, table: &[u8], f: &mut ScanRow<'_>) -> Result<()> {
        bridge_scan(|cb| self.table_scan_from(table, b"", cb), f)
    }

    fn ann_scan_from(&mut self, table: &[u8], start_key: &[u8], f: &mut ScanRow<'_>) -> Result<()> {
        bridge_scan(|cb| self.table_scan_from(table, start_key, cb), f)
    }

    fn ann_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.table_get(table, key).map_err(SqlError::Storage)
    }

    fn cache_generation(&self) -> Option<u64> {
        Some(self.commit_generation())
    }

    fn ann_table_root_stamp(&mut self, table: &[u8]) -> Result<Option<(u64, u64)>> {
        ReadTxn::table_root_stamp(self, table)
            .map(|stamp| stamp.map(|(page, txn)| (u64::from(page.0), txn.as_u64())))
            .map_err(SqlError::Storage)
    }

    fn ann_cancel_token(&self) -> Option<CancelToken> {
        self.cancel_token().cloned()
    }
}

impl AnnScan for WriteTxn<'_> {
    fn ann_scan(&mut self, table: &[u8], f: &mut ScanRow<'_>) -> Result<()> {
        bridge_scan(|cb| self.table_scan_from(table, b"", cb), f)
    }

    fn ann_scan_from(&mut self, table: &[u8], start_key: &[u8], f: &mut ScanRow<'_>) -> Result<()> {
        bridge_scan(|cb| self.table_scan_from(table, start_key, cb), f)
    }

    fn ann_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.table_get(table, key).map_err(SqlError::Storage)
    }

    fn cache_generation(&self) -> Option<u64> {
        None
    }

    fn ann_table_root_stamp(&mut self, table: &[u8]) -> Result<Option<(u64, u64)>> {
        WriteTxn::table_root_stamp(self, table)
            .map(|stamp| stamp.map(|(page, txn)| (u64::from(page.0), txn.as_u64())))
            .map_err(SqlError::Storage)
    }

    fn ann_cancel_token(&self) -> Option<CancelToken> {
        self.cancel_token().cloned()
    }
}

/// Provenance of a cached index; queryable via `ann_cache_status` and carries a
/// load-refusal reason so a refused segment's cause stays visible, not log-only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnnIndexSource {
    /// Built from a table scan this process; `refusal` records why a persisted
    /// segment was rejected, if one existed.
    Built { refusal: Option<String> },
    /// Loaded from a persisted segment (body BLAKE3 `segment_b3`) the freshness gate accepted.
    Loaded { segment_b3: [u8; 32] },
}

/// A cached ANN index plus the metadata needed to push SQL filters into it.
struct CachedAnnIndex {
    index: AnnIndex,
    /// Per attribute dim: maps an encoded filter-column value to its PRISM code.
    dicts: Vec<FxHashMap<Vec<u8>, u32>>,
    source: AnnIndexSource,
    /// Commit generation the index reflects; a cache insert is declined if the DB
    /// moved past it, so a cached index never describes a superseded snapshot.
    cached_gen: u64,
    /// Full logical identity. The cache key is compact and stable
    /// for diagnostics, so a DROP/CREATE that changes filter columns, collations,
    /// dimensions, or PRISM geometry must be rejected here rather than reusing the
    /// previous declaration's dictionaries/index.
    identity: AnnCacheIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AnnCacheIdentity {
    dim: u16,
    metric: AnnMetric,
    filter_cols: Vec<u16>,
    filter_collations: Vec<Collation>,
    prism_config_hash: [u8; 32],
}

pub(super) struct AnnTopKPlan {
    col_idx: usize,
    dim: u16,
    metric: AnnMetric,
    query_vec: Vec<f32>,
    k: usize,
    offset: usize,
    /// Schema column indices declared filterable on the index, in attr-dim order.
    filter_cols: Vec<u16>,
    /// Pushable conjuncts: `(attr_dim, canonical_allowed_values)` from `col = v` /
    /// `col IN (...)`. Canonicalizing once keeps tail checks O(1) per conjunct.
    pushable: Vec<(usize, FxHashSet<Vec<u8>>)>,
    /// Remaining WHERE predicate evaluated as a recheck on decoded candidates.
    residual: Option<Expr>,
}

/// Gate for single-key ascending ORDER BY ... LIMIT k (no group/having/join/distinct/window/agg).
fn topk_shape_ok(stmt: &SelectStmt) -> bool {
    stmt.order_by.len() == 1
        && !stmt.order_by[0].descending
        && stmt.limit.is_some()
        && stmt.group_by.is_empty()
        && stmt.having.is_none()
        && stmt.joins.is_empty()
        && !stmt.distinct
        && !has_any_window_function(stmt)
        && !stmt
            .columns
            .iter()
            .any(|c| matches!(c, SelectColumn::Expr { expr, .. } if is_aggregate_expr(expr)))
}

/// A finished result, or a request to rebuild the cache (tail too long to merge).
enum RunOutcome {
    Done(ExecutionResult),
    Rebuild,
}

/// Tail-row distance in SQL operator units; None for a zero vector under cosine.
fn tail_distance(metric: AnnMetric, q: &[f32], v: &[f32]) -> Option<f64> {
    let d = match metric {
        AnnMetric::L2 => {
            let mut sum = 0.0f64;
            for (x, y) in q.iter().zip(v.iter()) {
                let diff = (*x as f64) - (*y as f64);
                sum += diff * diff;
            }
            sum.sqrt()
        }
        AnnMetric::Inner => {
            let mut sum = 0.0f64;
            for (x, y) in q.iter().zip(v.iter()) {
                sum += (*x as f64) * (*y as f64);
            }
            -sum
        }
        AnnMetric::Cosine => {
            let mut dot = 0.0f64;
            let mut nq = 0.0f64;
            let mut nv = 0.0f64;
            for (x, y) in q.iter().zip(v.iter()) {
                let xf = *x as f64;
                let yf = *y as f64;
                dot += xf * yf;
                nq += xf * xf;
                nv += yf * yf;
            }
            let denom = nq.sqrt() * nv.sqrt();
            if denom == 0.0 {
                return None;
            }
            1.0 - dot / denom
        }
    };
    Some(d)
}

fn tail_distance_with_cancel(
    metric: AnnMetric,
    q: &[f32],
    v: &[f32],
    cancel: Option<&CancelToken>,
    work: &mut usize,
) -> Result<Option<f64>> {
    if cancel.is_none() {
        return Ok(tail_distance(metric, q, v));
    }
    tail_distance_checked(metric, q, v, work, |work| check_cancel_at(cancel, work))
}

fn tail_distance_checked(
    metric: AnnMetric,
    q: &[f32],
    v: &[f32],
    work: &mut usize,
    mut check: impl FnMut(usize) -> Result<()>,
) -> Result<Option<f64>> {
    let d = match metric {
        AnnMetric::L2 => {
            let mut sum = 0.0f64;
            for (x, y) in q.iter().zip(v.iter()) {
                check(*work)?;
                *work += 1;
                let diff = (*x as f64) - (*y as f64);
                sum += diff * diff;
            }
            sum.sqrt()
        }
        AnnMetric::Inner => {
            let mut sum = 0.0f64;
            for (x, y) in q.iter().zip(v.iter()) {
                check(*work)?;
                *work += 1;
                sum += (*x as f64) * (*y as f64);
            }
            -sum
        }
        AnnMetric::Cosine => {
            let mut dot = 0.0f64;
            let mut nq = 0.0f64;
            let mut nv = 0.0f64;
            for (x, y) in q.iter().zip(v.iter()) {
                check(*work)?;
                *work += 1;
                let xf = *x as f64;
                let yf = *y as f64;
                dot += xf * yf;
                nq += xf * xf;
                nv += yf * yf;
            }
            let denom = nq.sqrt() * nv.sqrt();
            if denom == 0.0 {
                return Ok(None);
            }
            1.0 - dot / denom
        }
    };
    Ok(Some(d))
}

impl AnnTopKPlan {
    pub(super) fn try_new(stmt: &SelectStmt, table_schema: &TableSchema) -> Result<Option<Self>> {
        if !topk_shape_ok(stmt) {
            return Ok(None);
        }
        let ob = &stmt.order_by[0];

        let (col_idx, dim, op_metric, query_vec) = match &ob.expr {
            Expr::BinaryOp { left, op, right } => {
                let op_metric = match op {
                    BinOp::VectorL2 => AnnMetric::L2,
                    BinOp::VectorInner => AnnMetric::Inner,
                    BinOp::VectorCosine => AnnMetric::Cosine,
                    _ => return Ok(None),
                };
                let col_name = match left.as_ref() {
                    Expr::Column(name) => name.to_ascii_lowercase(),
                    _ => return Ok(None),
                };
                let (col_idx, dim) = match table_schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, c)| c.name.to_ascii_lowercase() == col_name)
                {
                    Some((i, c)) => match c.data_type {
                        DataType::Vector { dim } => (i, dim),
                        _ => return Ok(None),
                    },
                    None => return Ok(None),
                };
                let col_map = ColumnMap::new(&table_schema.columns);
                let ctx = EvalCtx::new(&col_map, &[]);
                let v = match eval_expr(right, &ctx) {
                    Ok(Value::Vector(v)) => v,
                    _ => return Ok(None),
                };
                if v.len() != dim as usize {
                    return Err(SqlError::InvalidValue(format!(
                        "ANN query vector dim {} does not match column dim {}",
                        v.len(),
                        dim
                    )));
                }
                (col_idx, dim, op_metric, v.to_vec())
            }
            _ => return Ok(None),
        };

        let ann_index = table_schema.indices.iter().find(|ix| {
            matches!(ix.kind,
                IndexKind::Inverted(InvertedKind::Ann { metric }) if metric == op_metric
            ) && ix.keys.len() == 1
                && matches!(ix.keys[0],
                    IndexKey::Column { idx, .. } if idx as usize == col_idx
                )
        });
        let Some(ann_index) = ann_index else {
            return Ok(None);
        };
        let filter_cols = ann_index.ann_filter_cols.clone();

        if table_schema.primary_key_columns.len() != 1 {
            return Ok(None);
        }
        let pk_col = &table_schema.columns[table_schema.primary_key_columns[0] as usize];
        if !matches!(pk_col.data_type, DataType::Integer) {
            return Ok(None);
        }

        // No pushable predicate = no index leverage; decline for the exact filtered scan.
        let mut pushable: Vec<(usize, Vec<Value>)> = Vec::new();
        let mut residual_leaves: Vec<Expr> = Vec::new();
        if let Some(w) = &stmt.where_clause {
            split_where(
                w,
                &filter_cols,
                table_schema,
                &mut pushable,
                &mut residual_leaves,
            );
            if pushable.is_empty() {
                return Ok(None);
            }
        }
        let residual = fold_and(residual_leaves);
        let pushable = pushable
            .into_iter()
            .map(|(dim, values)| {
                let column = filter_cols[dim] as usize;
                let collation = table_schema.columns[column].collation;
                let mut canonical = FxHashSet::default();
                for value in values {
                    let mut encoded = Vec::with_capacity(16);
                    encode_key_value_collated_into(&value, collation, &mut encoded);
                    canonical.insert(encoded);
                }
                (dim, canonical)
            })
            .collect();

        let k_limit = eval_const_int(stmt.limit.as_ref().unwrap())?.max(0) as usize;
        let offset = stmt
            .offset
            .as_ref()
            .map(eval_const_int)
            .transpose()?
            .unwrap_or(0)
            .max(0) as usize;
        if k_limit == 0 {
            return Ok(None);
        }

        Ok(Some(Self {
            col_idx,
            dim,
            metric: op_metric,
            query_vec,
            k: k_limit,
            offset,
            filter_cols,
            pushable,
            residual,
        }))
    }

    pub(super) fn execute_with_read(
        &self,
        rtx: &mut ReadTxn<'_>,
        schema: &SchemaManager,
        stmt: &SelectStmt,
        table_schema: &TableSchema,
    ) -> Result<ExecutionResult> {
        let cache_key = cache_key(&table_schema.name, self.col_idx, self.metric);
        // One rebuild at most; the rebuilt snapshot has an empty tail.
        let mut force_rebuild = false;
        loop {
            if force_rebuild {
                schema.sql_caches.lock().remove(&cache_key);
            }
            let Some(cached) = self.load_or_build_index(rtx, schema, &cache_key, table_schema)?
            else {
                return empty_result(table_schema, stmt);
            };
            match self.run_query(rtx, &cached, stmt, table_schema, !force_rebuild)? {
                RunOutcome::Done(result) => return Ok(result),
                RunOutcome::Rebuild => force_rebuild = true,
            }
        }
    }

    /// Merge index hits with the brute-forced tail; `Rebuild` when the tail is too long.
    fn run_query(
        &self,
        txn: &mut dyn AnnScan,
        cached: &CachedAnnIndex,
        stmt: &SelectStmt,
        table_schema: &TableSchema,
        allow_rebuild: bool,
    ) -> Result<RunOutcome> {
        let cancel = txn.ann_cancel_token();
        let cancel = cancel.as_ref();
        check_cancel(cancel)?;
        // A filter value absent from the dict matches no indexed row, but a fresh
        // tail row still might, so skip only the index search (not the tail).
        let mut constraints: Vec<(usize, Vec<u32>)> = Vec::with_capacity(self.pushable.len());
        let mut index_unsat = false;
        for (dim, values) in &self.pushable {
            let dict = &cached.dicts[*dim];
            let mut codes = Vec::with_capacity(values.len());
            for value in values {
                if let Some(&code) = dict.get(value.as_slice()) {
                    codes.push(code);
                }
            }
            codes.sort_unstable();
            codes.dedup();
            if codes.is_empty() {
                index_unsat = true;
            }
            constraints.push((*dim, codes));
        }

        let want = self.k.saturating_add(self.offset).max(1);
        let mut merged: Vec<RankedRow> = if index_unsat {
            Vec::new()
        } else {
            let filter = if constraints.is_empty() {
                Filter::none()
            } else {
                Filter::new(constraints)
            };
            self.collect_survivors(txn, &cached.index, &filter, table_schema, want, cancel)?
        };

        match self.collect_tail(txn, &cached.index, table_schema, allow_rebuild, cancel)? {
            Some(tail) if cancel.is_none() => merged.extend(tail),
            Some(tail) => {
                merged.reserve(tail.len());
                for (tail_idx, row) in tail.into_iter().enumerate() {
                    check_cancel_at(cancel, tail_idx)?;
                    merged.push(row);
                }
            }
            None => return Ok(RunOutcome::Rebuild),
        }

        // Global distance order; ties broken by id for determinism.
        let merged = sort_vec_by(merged, cancel, |a, b| {
            a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1))
        })?;
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(merged.len());
        for (row_idx, (_, _, row)) in merged.into_iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            rows.push(row);
        }

        if self.offset >= rows.len() {
            rows.clear();
        } else if self.offset > 0 {
            rows = rows.split_off(self.offset);
        }
        rows.truncate(self.k);

        let (col_names, projected) =
            project_rows_with_cancel(&table_schema.columns, &stmt.columns, rows, cancel)?;
        Ok(RunOutcome::Done(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: projected,
        })))
    }

    /// Index hits passing the residual recheck, over-fetched until `want` survive.
    fn collect_survivors(
        &self,
        txn: &mut dyn AnnScan,
        index: &AnnIndex,
        filter: &Filter,
        table_schema: &TableSchema,
        want: usize,
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<RankedRow>> {
        let col_map = ColumnMap::new(&table_schema.columns);
        let max_target = index.indexed_len().max(1);
        let mut key_buf: Vec<u8> = Vec::with_capacity(10);
        let mut target = want;
        loop {
            check_cancel(cancel)?;
            target = target.min(max_target);
            let hits = index
                .search_filtered_default_ef(&self.query_vec, target, filter)
                .map_err(|e| SqlError::InvalidValue(format!("ANN search failed: {e}")))?;
            check_cancel(cancel)?;
            let mut survivors: Vec<RankedRow> = Vec::with_capacity(want);
            for (hit_idx, (id, dist)) in hits.iter().enumerate() {
                check_cancel_at(cancel, hit_idx)?;
                encode_int_key_into(*id as i64, &mut key_buf);
                let Some(row_bytes) = txn.ann_get(table_schema.name.as_bytes(), &key_buf)? else {
                    continue;
                };
                let row = decode_full_row_with_cancel(table_schema, &key_buf, &row_bytes, cancel)?;
                let keep = match &self.residual {
                    None => true,
                    Some(expr) => {
                        let ctx = EvalCtx::new(&col_map, &row).with_cancel(cancel);
                        is_truthy(&eval_expr(expr, &ctx)?)
                    }
                };
                if keep {
                    survivors.push((*dist as f64, *id as i64, row));
                    if survivors.len() >= want {
                        break;
                    }
                }
            }
            // Stop when satisfied, the index is exhausted, or PRISM returns fewer than asked.
            if survivors.len() >= want || target >= max_target || hits.len() < target {
                return Ok(survivors);
            }
            target = target.saturating_mul(2);
        }
    }

    /// Exact-rank rows appended past the snapshot; `None` when the tail is too long.
    fn collect_tail(
        &self,
        txn: &mut dyn AnnScan,
        index: &AnnIndex,
        table_schema: &TableSchema,
        allow_rebuild: bool,
        cancel: Option<&CancelToken>,
    ) -> Result<Option<Vec<RankedRow>>> {
        let snapshot_max = index.snapshot_max;
        // Negative pks (snapshot_max reads negative as i64) make the pk>snapshot_max
        // boundary unsound; those tables hard-invalidate on append, so the tail is empty.
        let first_tail_pk = match (snapshot_max as i64).checked_add(1) {
            Some(pk) if (snapshot_max as i64) >= 0 => pk,
            _ => return Ok(Some(Vec::new())),
        };
        let mut start_key: Vec<u8> = Vec::with_capacity(10);
        encode_int_key_into(first_tail_pk, &mut start_key);

        let col_map = ColumnMap::new(&table_schema.columns);
        let mut out: Vec<RankedRow> = Vec::new();
        let mut seen: u64 = 0;
        let mut over_threshold = false;
        let mut distance_work = 0usize;

        txn.ann_scan_from(
            table_schema.name.as_bytes(),
            &start_key,
            &mut |key, value| {
                seen += 1;
                if allow_rebuild && index.tail_is_stale(snapshot_max.saturating_add(seen)) {
                    over_threshold = true;
                    return Ok(false);
                }
                let row = decode_full_row_with_cancel(table_schema, key, value, cancel)?;
                if !self.tail_passes_pushable(&row, table_schema) {
                    return Ok(true);
                }
                if let Some(expr) = &self.residual {
                    let ctx = EvalCtx::new(&col_map, &row).with_cancel(cancel);
                    if !is_truthy(&eval_expr(expr, &ctx)?) {
                        return Ok(true);
                    }
                }
                let dist = match &row[self.col_idx] {
                    Value::Vector(v) => match tail_distance_with_cancel(
                        self.metric,
                        &self.query_vec,
                        v,
                        cancel,
                        &mut distance_work,
                    )? {
                        Some(d) => d,
                        None => return Ok(true), // undefined distance (zero vector under cosine)
                    },
                    Value::Null => return Ok(true), // null vectors are unindexable
                    _ => {
                        return Err(SqlError::InvalidValue(
                            "ANN column produced non-vector value".into(),
                        ))
                    }
                };
                out.push((dist, decode_pk_integer(key)?, row));
                Ok(true)
            },
        )?;

        if over_threshold {
            return Ok(None);
        }
        check_cancel(cancel)?;
        Ok(Some(out))
    }

    /// Pushable conjuncts checked on decoded tail values (the tail has no PRISM codes).
    fn tail_passes_pushable(&self, row: &[Value], table_schema: &TableSchema) -> bool {
        for (dim, values) in &self.pushable {
            let col = self.filter_cols[*dim] as usize;
            let coll = table_schema.columns[col].collation;
            let mut canon_row = Vec::with_capacity(16);
            encode_key_value_collated_into(&row[col], coll, &mut canon_row);
            if !values.contains(canon_row.as_slice()) {
                return false;
            }
        }
        true
    }

    fn load_or_build_index(
        &self,
        txn: &mut dyn AnnScan,
        schema: &SchemaManager,
        cache_key: &str,
        table_schema: &TableSchema,
    ) -> Result<Option<Arc<CachedAnnIndex>>> {
        let spec = AnnSpec {
            col_idx: self.col_idx,
            dim: self.dim,
            metric: self.metric,
            filter_cols: self.filter_cols.clone(),
        };
        let identity = spec.cache_identity(table_schema);
        if let Some(existing) = lookup_cached(
            schema,
            cache_key,
            &table_schema.name,
            &identity,
            txn.cache_generation(),
        )? {
            return Ok(Some(existing));
        }
        load_or_build(txn, schema, cache_key, table_schema, &spec)
    }
}

/// The index identity build/load/persist operates on, resolved from the statement
/// (`AnnTopKPlan`) or the declared index (`persist_ann_index`).
pub(super) struct AnnSpec {
    pub col_idx: usize,
    pub dim: u16,
    pub metric: AnnMetric,
    pub filter_cols: Vec<u16>,
}

impl AnnSpec {
    fn metric_tag(&self) -> u8 {
        citadel_vector::segment::metric_tag(ann_metric_to_prism(self.metric))
    }

    fn cache_identity(&self, table_schema: &TableSchema) -> AnnCacheIdentity {
        AnnCacheIdentity {
            dim: self.dim,
            metric: self.metric,
            filter_cols: self.filter_cols.clone(),
            filter_collations: self
                .filter_cols
                .iter()
                .map(|&col| table_schema.columns[col as usize].collation)
                .collect(),
            prism_config_hash: ann_persist::active_config_hash(ann_metric_to_prism(self.metric)),
        }
    }
}

/// One scan pass: build rows, filter dicts (codes in first-seen order), and the
/// injective content fingerprint; the single decode path for build/persist/load.
struct ScanOutcome {
    rows: Vec<(u64, Vec<f32>, Vec<u32>)>,
    dicts: Vec<FxHashMap<Vec<u8>, u32>>,
    fingerprint: [u8; 32],
}

fn scan_rows(
    txn: &mut dyn AnnScan,
    table_schema: &TableSchema,
    spec: &AnnSpec,
) -> Result<ScanOutcome> {
    let cancel = txn.ann_cancel_token();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let nonpk_order = non_pk
        .iter()
        .position(|&i| i == spec.col_idx)
        .ok_or_else(|| {
            SqlError::InvalidValue("vector column must be non-PK for ANN build".into())
        })?;
    let enc_idx = enc_pos[nonpk_order] as usize;

    let num_attrs = spec.filter_cols.len();
    let extracts: Vec<Extract> = spec
        .filter_cols
        .iter()
        .map(|&c| extract_plan(c, table_schema, non_pk, enc_pos))
        .collect::<Result<_>>()?;
    // Dict keys are collation-canonical so collation-equal values share a code (matching
    // eval equality); the forensic fingerprint retains raw source encodings.
    let collations: Vec<Collation> = spec
        .filter_cols
        .iter()
        .map(|&c| table_schema.columns[c as usize].collation)
        .collect();
    let mut dicts: Vec<FxHashMap<Vec<u8>, u32>> = vec![FxHashMap::default(); num_attrs];
    let mut fp = ann_persist::FingerprintHasher::new(
        &table_schema.name,
        spec.col_idx as u32,
        &spec
            .filter_cols
            .iter()
            .map(|&c| c as u32)
            .collect::<Vec<_>>(),
        spec.dim,
        spec.metric_tag(),
    );
    let mut rows: Vec<(u64, Vec<f32>, Vec<u32>)> = Vec::new();
    let mut vector_work = 0usize;

    txn.ann_scan(table_schema.name.as_bytes(), &mut |key, value| {
        let vector = match decode_column_raw(value, enc_idx)?.to_value() {
            Value::Vector(arr) if cancel.is_none() => Some(arr.to_vec()),
            Value::Vector(arr) => {
                let mut vector = Vec::with_capacity(arr.len());
                for &component in arr.iter() {
                    check_cancel_at(cancel, vector_work)?;
                    vector_work += 1;
                    vector.push(component);
                }
                Some(vector)
            }
            Value::Null => None, // null vectors are content, but not indexed
            _ => {
                return Err(SqlError::InvalidValue(
                    "ANN column produced non-vector value".into(),
                ))
            }
        };
        let mut filter_vals: Vec<Value> = Vec::with_capacity(num_attrs);
        for ex in &extracts {
            filter_vals.push(ex.extract(key, value)?);
        }
        let encoded_filters: Vec<Vec<u8>> = filter_vals.iter().map(encode_key_value).collect();
        let vector_slice = vector.as_deref().unwrap_or(&[]);
        let vec_bytes: Vec<u8> = if cancel.is_none() {
            vector_slice.iter().flat_map(|f| f.to_le_bytes()).collect()
        } else {
            let mut bytes = Vec::with_capacity(vector_slice.len().saturating_mul(4));
            for &component in vector_slice {
                check_cancel_at(cancel, vector_work)?;
                vector_work += 1;
                bytes.extend_from_slice(&component.to_le_bytes());
            }
            bytes
        };
        fp.row(
            key,
            &vec_bytes,
            &encoded_filters
                .iter()
                .map(Vec::as_slice)
                .collect::<Vec<_>>(),
        );
        let Some(vector) = vector else {
            return Ok(true);
        };
        let id = decode_pk_integer(key)? as u64;
        let mut codes: Vec<u32> = Vec::with_capacity(num_attrs);
        for (j, v) in filter_vals.iter().enumerate() {
            let mut canon = Vec::with_capacity(16);
            encode_key_value_collated_into(v, collations[j], &mut canon);
            let next = dicts[j].len() as u32;
            codes.push(*dicts[j].entry(canon).or_insert(next));
        }
        rows.push((id, vector, codes));
        Ok(true)
    })?;
    check_cancel(cancel)?;

    Ok(ScanOutcome {
        rows,
        dicts,
        fingerprint: fp.finish(),
    })
}

/// Count a full O(N) rebuild; thrash tests assert this stays 0 on pure appends.
#[cfg(test)]
fn note_ann_rebuild() {
    ANN_REBUILD_COUNT.with(|c| c.set(c.get() + 1));
}

#[cfg(test)]
thread_local! {
    static ANN_REBUILD_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn take_ann_rebuilds() -> u64 {
    ANN_REBUILD_COUNT.with(|c| c.replace(0))
}

/// Build the index from a scan; `None` if there are no indexable rows.
fn build_index(
    txn: &mut dyn AnnScan,
    table_schema: &TableSchema,
    spec: &AnnSpec,
    refusal: Option<String>,
    cached_gen: u64,
) -> Result<Option<CachedAnnIndex>> {
    let cancel = txn.ann_cancel_token();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let outcome = scan_rows(txn, table_schema, spec)?;
    check_cancel(cancel)?;
    if outcome.rows.is_empty() {
        return Ok(None);
    }
    let index = AnnIndex::build_with_attrs(
        outcome.rows,
        spec.filter_cols.len(),
        ann_metric_to_prism(spec.metric),
        spec.dim,
    )
    .map_err(|e| SqlError::InvalidValue(format!("ANN build failed: {e}")))?;
    check_cancel(cancel)?;
    #[cfg(test)]
    note_ann_rebuild();
    Ok(Some(CachedAnnIndex {
        index,
        dicts: outcome.dicts,
        source: AnnIndexSource::Built { refusal },
        cached_gen,
        identity: spec.cache_identity(table_schema),
    }))
}

/// Outcome of a persisted-segment load. `Refused` triggers a rebuild; corrupt
/// segments also warn (HMAC-authenticated page + failing BLAKE3 = writer bug).
enum LoadOutcome {
    Loaded(Box<CachedAnnIndex>),
    NoSegment,
    Refused { reason: String, corrupt: bool },
}

fn classify_segment_header_read(result: Result<Option<Vec<u8>>>) -> Result<Option<Vec<u8>>> {
    match result {
        Ok(value) => Ok(value),
        Err(SqlError::Storage(citadel_core::Error::TableNotFound(_))) => Ok(None),
        Err(error) => Err(error),
    }
}

fn segment_operation_error(error: SegmentOperationError) -> SqlError {
    match error {
        SegmentOperationError::Interrupted => SqlError::Storage(citadel_core::Error::Interrupted),
        SegmentOperationError::Allocation(where_) => {
            SqlError::InvalidValue(format!("ANN segment allocation failed in {where_}"))
        }
        SegmentOperationError::Segment(error) => {
            SqlError::InvalidValue(format!("ANN segment operation failed: {error}"))
        }
    }
}

/// Try to serve the table's persisted segment: header pins, body decode, and the
/// table-root freshness gate confirming it matches this snapshot.
fn try_load_segment(
    txn: &mut dyn AnnScan,
    table_schema: &TableSchema,
    spec: &AnnSpec,
    cached_gen: u64,
) -> Result<LoadOutcome> {
    let cancel = txn.ann_cancel_token();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let seg_table = ann_persist::segment_table_name(&table_schema.name);
    let header_bytes = match classify_segment_header_read(
        txn.ann_get(&seg_table, &ann_persist::segment_key(0)),
    )? {
        Some(bytes) => bytes,
        // Missing tree and missing header are both "never persisted".
        None => return Ok(LoadOutcome::NoSegment),
    };
    let refuse = |reason: String, corrupt: bool| Ok(LoadOutcome::Refused { reason, corrupt });
    let header = match ann_persist::SegmentHeader::decode(&header_bytes) {
        Ok(h) => h,
        Err(e) => return refuse(format!("header: {e}"), true),
    };
    check_cancel(cancel)?;
    if header.format_version != ann_persist::ANNSEG_FORMAT_VERSION {
        return refuse(
            format!(
                "format v{} (this binary reads v{})",
                header.format_version,
                ann_persist::ANNSEG_FORMAT_VERSION
            ),
            false,
        );
    }
    let active_cfg = citadel_vector::segment::prism_config_hash(&AnnIndex::active_config(
        ann_metric_to_prism(spec.metric),
    ));
    if header.prism_config_hash != active_cfg {
        return refuse(
            "PRISM config drift (segment built by another geometry)".into(),
            false,
        );
    }
    if header.dim != spec.dim
        || header.metric_tag != spec.metric_tag()
        || header.col_idx != spec.col_idx as u32
        || header.filter_cols
            != spec
                .filter_cols
                .iter()
                .map(|&c| c as u32)
                .collect::<Vec<_>>()
    {
        return refuse(
            "index identity mismatch (column/metric/filter set)".into(),
            false,
        );
    }

    // Reject a stale segment before reading/decrypting its potentially large
    // body. The root is snapshot-relative, so this is safe before decode.
    match txn.ann_table_root_stamp(table_schema.name.as_bytes())? {
        Some(live) if root_stamp_matches(live, (header.table_root, header.table_root_txn)) => {}
        _ => {
            return refuse(
                "stale: table root stamp changed since the segment was persisted".into(),
                false,
            )
        }
    }

    let chunk_count = usize::try_from(header.chunk_count)
        .map_err(|_| SqlError::InvalidValue("ANN segment chunk count is too large".into()))?;
    if chunk_count == 0 {
        return refuse("segment has no body chunks".into(), true);
    }
    let max_body_len = chunk_count
        .checked_mul(ann_persist::CHUNK_BYTES)
        .ok_or_else(|| SqlError::InvalidValue("ANN segment body size overflow".into()))?;
    // Reserve once before plaintext enters the Vec. A growing Vec can otherwise
    // free old allocations without zeroing them, leaving prior vector bytes in
    // the allocator; this also turns allocation failure into a regular error.
    let mut body_vec = Vec::new();
    body_vec
        .try_reserve_exact(max_body_len)
        .map_err(|_| segment_operation_error(SegmentOperationError::Allocation("segment body")))?;
    let mut body = Zeroizing::new(body_vec);
    for chunk_no in 1..=header.chunk_count {
        check_cancel_at(cancel, chunk_no as usize)?;
        match txn.ann_get(&seg_table, &ann_persist::segment_key(chunk_no)) {
            Ok(Some(c)) => {
                let chunk = Zeroizing::new(c);
                let is_last = chunk_no == header.chunk_count;
                if chunk.is_empty()
                    || chunk.len() > ann_persist::CHUNK_BYTES
                    || (!is_last && chunk.len() != ann_persist::CHUNK_BYTES)
                {
                    return refuse(format!("invalid chunk {chunk_no} length"), true);
                }
                body.extend_from_slice(&chunk);
            }
            Err(SqlError::Storage(citadel_core::Error::Interrupted)) => {
                return Err(SqlError::Storage(citadel_core::Error::Interrupted))
            }
            _ => return refuse(format!("missing chunk {chunk_no}"), true),
        }
    }
    let body_digest = citadel_vector::segment::digest_with_cancel(&body, cancel)
        .map_err(segment_operation_error)?;
    if body_digest != header.segment_b3 {
        return refuse("segment body BLAKE3 mismatch (corrupt)".into(), true);
    }
    let parts = match citadel_vector::segment::decode_with_cancel(&body, cancel) {
        Ok(p) => p,
        Err(SegmentOperationError::Interrupted) => {
            return Err(SqlError::Storage(citadel_core::Error::Interrupted))
        }
        Err(error @ SegmentOperationError::Allocation(_)) => {
            return Err(segment_operation_error(error))
        }
        Err(SegmentOperationError::Segment(error)) => {
            return refuse(format!("segment decode: {error}"), true)
        }
    };
    check_cancel(cancel)?;
    if parts.n() as u64 != header.n
        || parts.dim() != header.dim
        || parts.metric() != ann_metric_to_prism(spec.metric)
        || parts.snapshot_max() != header.snapshot_max
    {
        return refuse("segment body disagrees with header identity".into(), true);
    }
    let attribute_domains: Vec<usize> = header.dicts.iter().map(Vec::len).collect();
    if !parts
        .attributes_fit_domains_with_cancel(&attribute_domains, cancel)
        .map_err(segment_operation_error)?
    {
        return refuse(
            "segment attribute codes disagree with header dictionaries".into(),
            true,
        );
    }

    // Vectors ride in the segment (TAG_VECTORS), so the load is a bulk read, no rescan.
    let index = match parts.into_index_embedded() {
        Ok(index) => index,
        Err(e) => return refuse(format!("index assembly: {e}"), true),
    };
    check_cancel(cancel)?;
    let dicts = if cancel.is_none() {
        header.dict_maps()
    } else {
        let mut dicts = Vec::with_capacity(header.dicts.len());
        let mut dict_work = 0usize;
        for entries in &header.dicts {
            let mut dict = FxHashMap::default();
            dict.reserve(entries.len());
            for (key, code) in entries {
                check_cancel_at(cancel, dict_work)?;
                dict_work += 1;
                dict.insert(key.clone(), *code);
            }
            dicts.push(dict);
        }
        check_cancel(cancel)?;
        dicts
    };
    Ok(LoadOutcome::Loaded(Box::new(CachedAnnIndex {
        index,
        dicts,
        source: AnnIndexSource::Loaded {
            segment_b3: header.segment_b3,
        },
        cached_gen,
        identity: spec.cache_identity(table_schema),
    })))
}

fn root_stamp_matches(live: (u64, u64), persisted: (u64, u64)) -> bool {
    live == persisted
}

/// Shared load-then-build flow: try the segment, else scan-build carrying the refusal
/// as a diagnostic; cache only if no DML committed past the snapshot, never from a write txn.
fn load_or_build(
    txn: &mut dyn AnnScan,
    schema: &SchemaManager,
    cache_key: &str,
    table_schema: &TableSchema,
    spec: &AnnSpec,
) -> Result<Option<Arc<CachedAnnIndex>>> {
    let gen = txn.cache_generation();
    let cached_gen = gen.unwrap_or(u64::MAX);
    let loaded = match try_load_segment(txn, table_schema, spec, cached_gen)? {
        LoadOutcome::Loaded(c) => Some(*c),
        LoadOutcome::NoSegment => None,
        LoadOutcome::Refused { reason, corrupt } => {
            if corrupt {
                eprintln!(
                    "citadel-sql: ANN segment for `{}` REFUSED as corrupt ({reason}); \
                     rebuilding from scan - investigate before re-persisting",
                    table_schema.name
                );
            }
            // Stale/drift refusals are the expected degradation; the reason stays queryable on the rebuild.
            match build_index(txn, table_schema, spec, Some(reason), cached_gen)? {
                Some(c) => Some(c),
                None => return Ok(None),
            }
        }
    };
    let built = match loaded {
        Some(c) => c,
        None => match build_index(txn, table_schema, spec, None, cached_gen)? {
            Some(c) => c,
            None => return Ok(None),
        },
    };
    let arc: Arc<CachedAnnIndex> = Arc::new(built);
    if gen.is_none() {
        // A write-txn view may include uncommitted rows: serve, never cache.
        return Ok(Some(arc));
    }
    let mut guard = schema.sql_caches.lock();
    if !cached_passes_dml_barriers_locked(&guard, &table_schema.name, &arc) {
        // DML committed during the build: a superseded snapshot. Serve this query, decline the cache.
        return Ok(Some(arc));
    }
    if let Some(existing) = guard.get(cache_key) {
        let existing = Arc::clone(existing)
            .downcast::<CachedAnnIndex>()
            .map_err(|_| {
                SqlError::InvalidValue(format!("ANN cache type mismatch for {cache_key}"))
            })?;
        if existing.identity == arc.identity
            && existing.cached_gen == arc.cached_gen
            && cached_passes_dml_barriers_locked(&guard, &table_schema.name, &existing)
        {
            // Another thread published the same snapshot and declaration.
            return Ok(Some(existing));
        }
        if existing.cached_gen > arc.cached_gen {
            // This is an explicit old reader racing a current build. Keep the
            // future shared entry and serve the old reader's private result.
            return Ok(Some(arc));
        }
        guard.remove(cache_key);
    }
    let as_any: Arc<dyn Any + Send + Sync> = arc.clone();
    guard.insert(cache_key.to_string(), as_any);
    Ok(Some(arc))
}

/// Streaming brute-force top-k for `ORDER BY <distance> LIMIT k` when no ANN
/// index applies (or inside a write txn); bounded heap, O(k) memory.
pub(super) struct VectorTopKPlan {
    order_expr: Expr,
    where_clause: Option<Expr>,
    k: usize,
    offset: usize,
    nulls_first: bool,
}

/// A candidate keyed by (distance, scan position); `seq` breaks ties by scan
/// order so the bounded heap matches the stable sort.
struct Ranked {
    dist: f64,
    seq: u64,
    row: Vec<Value>,
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist
            .total_cmp(&other.dist)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl VectorTopKPlan {
    pub(super) fn try_new(stmt: &SelectStmt, table_schema: &TableSchema) -> Result<Option<Self>> {
        if !topk_shape_ok(stmt) {
            return Ok(None);
        }
        let ob = &stmt.order_by[0];
        let Expr::BinaryOp { left, op, .. } = &ob.expr else {
            return Ok(None);
        };
        if !matches!(
            op,
            BinOp::VectorL2 | BinOp::VectorInner | BinOp::VectorCosine
        ) {
            return Ok(None);
        }
        // Only claim a vector-distance sort key; anything else uses the general path.
        let Expr::Column(name) = left.as_ref() else {
            return Ok(None);
        };
        let name = name.to_ascii_lowercase();
        let is_vector_col = table_schema.columns.iter().any(|c| {
            c.name.to_ascii_lowercase() == name && matches!(c.data_type, DataType::Vector { .. })
        });
        if !is_vector_col {
            return Ok(None);
        }

        let k = eval_const_int(stmt.limit.as_ref().unwrap())?.max(0) as usize;
        if k == 0 {
            return Ok(None);
        }
        let offset = stmt
            .offset
            .as_ref()
            .map(eval_const_int)
            .transpose()?
            .unwrap_or(0)
            .max(0) as usize;

        Ok(Some(Self {
            order_expr: ob.expr.clone(),
            where_clause: stmt.where_clause.clone(),
            k,
            offset,
            // citadel defaults to NULLS FIRST for ascending order.
            nulls_first: ob.nulls_first.unwrap_or(true),
        }))
    }

    pub(super) fn execute(
        &self,
        txn: &mut dyn AnnScan,
        table_schema: &TableSchema,
        stmt: &SelectStmt,
    ) -> Result<ExecutionResult> {
        let cancel = txn.ann_cancel_token();
        let cancel = cancel.as_ref();
        check_cancel(cancel)?;
        let want = self.k.saturating_add(self.offset);
        let col_map = ColumnMap::new(&table_schema.columns);
        // NULL distances sort like NULLs under the requested ordering.
        let null_dist = if self.nulls_first {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
        let mut heap: BinaryHeap<Ranked> = BinaryHeap::new();
        let mut seq: u64 = 0;

        txn.ann_scan(table_schema.name.as_bytes(), &mut |key, value| {
            let row = decode_full_row_with_cancel(table_schema, key, value, cancel)?;
            let ctx = EvalCtx::new(&col_map, &row).with_cancel(cancel);
            if let Some(w) = &self.where_clause {
                if !is_truthy(&eval_expr(w, &ctx)?) {
                    return Ok(true);
                }
            }
            let dist = match eval_expr(&self.order_expr, &ctx)? {
                Value::Real(d) => d,
                Value::Integer(i) => i as f64,
                Value::Null => null_dist,
                other => {
                    return Err(SqlError::InvalidValue(format!(
                        "ORDER BY vector distance produced a non-numeric {}",
                        other.data_type()
                    )))
                }
            };
            let cand = Ranked { dist, seq, row };
            seq += 1;
            // `seq` only grows, so ties never evict an earlier row (stable-sort order).
            if heap.len() < want {
                heap.push(cand);
            } else if heap.peek().is_some_and(|top| cand < *top) {
                heap.pop();
                heap.push(cand);
            }
            Ok(true)
        })?;
        check_cancel(cancel)?;

        let ranked = if cancel.is_none() {
            heap.into_sorted_vec()
        } else {
            sort_vec_by(heap.into_vec(), cancel, Ranked::cmp)?
        };
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(ranked.len());
        for (row_idx, ranked) in ranked.into_iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            rows.push(ranked.row);
        }
        if self.offset >= rows.len() {
            rows.clear();
        } else if self.offset > 0 {
            rows = rows.split_off(self.offset);
        }
        rows.truncate(self.k);

        let (col_names, projected) =
            project_rows_with_cancel(&table_schema.columns, &stmt.columns, rows, cancel)?;
        Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: projected,
        }))
    }
}

/// How to read a filter column's value out of a raw row during the build scan.
enum Extract {
    /// The single integer primary key, read from the row key.
    Pk,
    /// A non-PK column at the given encoding position in the row value.
    NonPk(usize),
}

impl Extract {
    fn extract(&self, key: &[u8], value: &[u8]) -> Result<Value> {
        match self {
            Extract::Pk => Ok(Value::Integer(decode_pk_integer(key)?)),
            Extract::NonPk(ei) => Ok(decode_column_raw(value, *ei)?.to_value()),
        }
    }
}

fn extract_plan(
    col: u16,
    table_schema: &TableSchema,
    non_pk: &[usize],
    enc_pos: &[u16],
) -> Result<Extract> {
    if table_schema.primary_key_columns.contains(&col) {
        return Ok(Extract::Pk);
    }
    let order = non_pk
        .iter()
        .position(|&i| i == col as usize)
        .ok_or_else(|| SqlError::InvalidValue("ANN filter column not found in row".into()))?;
    Ok(Extract::NonPk(enc_pos[order] as usize))
}

/// Walk the AND-tree, sorting each leaf into a pushable attribute predicate or
/// the recheck residual.
fn split_where(
    expr: &Expr,
    filter_cols: &[u16],
    table_schema: &TableSchema,
    pushable: &mut Vec<(usize, Vec<Value>)>,
    residual: &mut Vec<Expr>,
) {
    if let Expr::BinaryOp {
        left,
        op: BinOp::And,
        right,
    } = expr
    {
        split_where(left, filter_cols, table_schema, pushable, residual);
        split_where(right, filter_cols, table_schema, pushable, residual);
        return;
    }
    match classify_leaf(expr, filter_cols, table_schema) {
        Some(constraint) => pushable.push(constraint),
        None => residual.push(expr.clone()),
    }
}

/// Outcome of coercing a pushdown literal to the filter column's stored type.
enum Coerced {
    /// Encodes exactly like a stored value; safe for the dictionary lookup.
    Exact(Value),
    /// Can never equal any stored value of this column (e.g. a fractional
    /// literal vs INTEGER); contributes no codes.
    NeverMatches,
    /// Eval equality may diverge from encoded-byte equality (NULL three-valued
    /// logic, cross-type comparisons, floats past 2^53); the whole leaf must
    /// stay in the residual so the eval path decides.
    Residual,
}

fn coerce_pushdown_literal(val: Value, col_type: DataType) -> Coerced {
    // Past 2^53 int<->f64 is not 1:1, so encoded and numeric equality diverge.
    const EXACT_F64_INT: f64 = 9_007_199_254_740_992.0;
    if val.is_null() {
        return Coerced::Residual;
    }
    if val.data_type() == col_type {
        return Coerced::Exact(val);
    }
    match (val, col_type) {
        (Value::Real(r), DataType::Integer) => {
            if r.is_nan() || r.is_infinite() {
                Coerced::NeverMatches
            } else if r.abs() > EXACT_F64_INT {
                Coerced::Residual
            } else if r.fract() == 0.0 {
                Coerced::Exact(Value::Integer(r as i64))
            } else {
                Coerced::NeverMatches
            }
        }
        (Value::Integer(i), DataType::Real) => {
            if i.unsigned_abs() <= EXACT_F64_INT as u64 {
                Coerced::Exact(Value::Real(i as f64))
            } else {
                Coerced::Residual
            }
        }
        _ => Coerced::Residual,
    }
}

/// A leaf is pushable if it is `col = literal` or `col IN (literal, ...)` on a
/// declared filter column whose constant right-hand side coerces exactly to
/// the column's stored type. An empty value list means the leaf is provably
/// unsatisfiable (the caller short-circuits to an empty result).
fn classify_leaf(
    leaf: &Expr,
    filter_cols: &[u16],
    table_schema: &TableSchema,
) -> Option<(usize, Vec<Value>)> {
    let (col_expr, rhs): (&Expr, Vec<&Expr>) = match leaf {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => (left, vec![right.as_ref()]),
        Expr::InList {
            expr,
            list,
            negated: false,
        } => (expr, list.iter().collect()),
        _ => return None,
    };
    let dim = filter_dim(col_expr, filter_cols, table_schema)?;
    let col_type = table_schema.columns[filter_cols[dim] as usize].data_type;
    let mut vals = Vec::with_capacity(rhs.len());
    for e in rhs {
        match coerce_pushdown_literal(eval_const_expr(e).ok()?, col_type) {
            Coerced::Exact(v) => vals.push(v),
            Coerced::NeverMatches => {}
            Coerced::Residual => return None,
        }
    }
    Some((dim, vals))
}

/// Resolve a column expression to its attribute-dim index (position in
/// `filter_cols`), or `None` if it is not a declared filter column.
fn filter_dim(expr: &Expr, filter_cols: &[u16], table_schema: &TableSchema) -> Option<usize> {
    let name = match expr {
        Expr::Column(c) => c.to_ascii_lowercase(),
        Expr::QualifiedColumn { column, .. } => column.to_ascii_lowercase(),
        _ => return None,
    };
    let col_idx = table_schema
        .columns
        .iter()
        .position(|c| c.name.to_ascii_lowercase() == name)? as u16;
    filter_cols.iter().position(|&c| c == col_idx)
}

fn fold_and(mut leaves: Vec<Expr>) -> Option<Expr> {
    if leaves.is_empty() {
        return None;
    }
    let first = leaves.remove(0);
    Some(leaves.into_iter().fold(first, |acc, e| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinOp::And,
        right: Box::new(e),
    }))
}

fn empty_result(table_schema: &TableSchema, stmt: &SelectStmt) -> Result<ExecutionResult> {
    let (col_names, projected) = project_rows(&table_schema.columns, &stmt.columns, Vec::new())?;
    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: projected,
    }))
}

/// Freeze behind `Connection::persist_ann_index`: build off a read snapshot
/// without the writer lock, verify the non-ABA table-root stamp in a write txn,
/// then replace the segment atomically and warm the shared cache.
pub(crate) fn persist_ann_index(
    db: &citadel::Database,
    schema: &SchemaManager,
    table_schema: &TableSchema,
    column: &str,
) -> Result<ann_persist::AnnSegmentInfo> {
    let col_lower = column.to_ascii_lowercase();
    let col_idx = table_schema
        .columns
        .iter()
        .position(|c| c.name == col_lower)
        .ok_or_else(|| SqlError::ColumnNotFound(column.to_string()))?;
    let DataType::Vector { dim } = table_schema.columns[col_idx].data_type else {
        return Err(SqlError::InvalidValue(format!(
            "column `{column}` is not VECTOR(N)"
        )));
    };
    // Same admission as AnnTopKPlan::try_new: an unservable table gets no segment
    // (dead weight with mis-decoded row ids).
    if table_schema.primary_key_columns.len() != 1
        || !matches!(
            table_schema.columns[table_schema.primary_key_columns[0] as usize].data_type,
            DataType::Integer
        )
    {
        return Err(SqlError::InvalidValue(
            "ANN persistence requires a single INTEGER primary key (same rule as the \
             ANN query plan)"
                .into(),
        ));
    }
    let ann_index = table_schema
        .indices
        .iter()
        .find(|ix| {
            matches!(ix.kind, IndexKind::Inverted(InvertedKind::Ann { .. }))
                && ix.keys.len() == 1
                && matches!(ix.keys[0], IndexKey::Column { idx, .. } if idx as usize == col_idx)
        })
        .ok_or_else(|| SqlError::InvalidValue(format!("no ANN index declared on `{column}`")))?;
    let IndexKind::Inverted(InvertedKind::Ann { metric }) = ann_index.kind else {
        unreachable!("matched above");
    };
    let spec = AnnSpec {
        col_idx,
        dim,
        metric,
        filter_cols: ann_index.ann_filter_cols.clone(),
    };

    let mut rtx = db.begin_read();
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let source_stamp = rtx
        .table_root_stamp(table_schema.name.as_bytes())
        .map_err(SqlError::Storage)?
        .ok_or_else(|| SqlError::InvalidValue("table vanished during ANN persist".into()))?;
    let (source_root, source_root_txn) = source_stamp;
    let outcome = scan_rows(&mut rtx, table_schema, &spec)?;
    drop(rtx);
    check_cancel(cancel)?;
    if outcome.rows.is_empty() {
        return Err(SqlError::InvalidValue(
            "nothing to persist: the table has no indexable (non-NULL) vectors".into(),
        ));
    }
    let n = outcome.rows.len() as u64;
    let index = AnnIndex::build_with_attrs(
        outcome.rows,
        spec.filter_cols.len(),
        ann_metric_to_prism(spec.metric),
        spec.dim,
    )
    .map_err(|e| SqlError::InvalidValue(format!("ANN build failed: {e}")))?;
    check_cancel(cancel)?;

    let body = citadel_vector::segment::encode_with_cancel(&index, cancel)
        .map_err(segment_operation_error)?;
    let segment_b3 = citadel_vector::segment::digest_with_cancel(&body, cancel)
        .map_err(segment_operation_error)?;
    // Order dict entries by code; codes are first-seen order, so by-code is scan order.
    let mut dict_work = 0usize;
    let mut dicts_ordered: Vec<Vec<(Vec<u8>, u32)>> = Vec::with_capacity(outcome.dicts.len());
    for dict in &outcome.dicts {
        let mut entries = Vec::with_capacity(dict.len());
        for (key, &code) in dict {
            check_cancel_at(cancel, dict_work)?;
            dict_work += 1;
            entries.push((key.clone(), code));
        }
        let entries = sort_vec_by(entries, cancel, |a, b| a.1.cmp(&b.1))?;
        dicts_ordered.push(entries);
    }
    check_cancel(cancel)?;
    let header = ann_persist::SegmentHeader {
        format_version: ann_persist::ANNSEG_FORMAT_VERSION,
        prism_config_hash: ann_persist::active_config_hash(ann_metric_to_prism(spec.metric)),
        dim: spec.dim,
        metric_tag: spec.metric_tag(),
        n,
        snapshot_max: index.snapshot_max,
        table_root: u64::from(source_root.0),
        table_root_txn: source_root_txn.as_u64(),
        col_idx: spec.col_idx as u32,
        filter_cols: spec.filter_cols.iter().map(|&c| c as u32).collect(),
        dicts: dicts_ordered,
        content_fingerprint: outcome.fingerprint,
        segment_b3,
        chunk_count: body.len().div_ceil(ann_persist::CHUNK_BYTES) as u32,
        writer: format!("citadel-sql {}", env!("CARGO_PKG_VERSION")),
    };
    let header_bytes = header.encode();

    #[cfg(test)]
    pause_after_ann_build_before_write();
    check_cancel(cancel)?;
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    check_cancel(cancel)?;
    let live_stamp = wtx
        .table_root_stamp(table_schema.name.as_bytes())
        .map_err(SqlError::Storage)?
        .ok_or_else(|| SqlError::InvalidValue("table vanished during ANN persist".into()))?;
    if live_stamp != (source_root, source_root_txn) {
        return Err(SqlError::InvalidValue(
            "table changed while the ANN index was being built; retry persistence".into(),
        ));
    }
    let seg_table = ann_persist::segment_table_name(&table_schema.name);
    check_cancel(cancel)?;
    ann_persist::purge_segment(&mut wtx, &table_schema.name)?;
    wtx.create_table(&seg_table).map_err(SqlError::Storage)?;
    wtx.table_insert(&seg_table, &ann_persist::segment_key(0), &header_bytes)
        .map_err(SqlError::Storage)?;
    for (chunk_no, chunk) in ann_persist::chunks(&body) {
        check_cancel_at(cancel, chunk_no as usize)?;
        wtx.table_insert(&seg_table, &ann_persist::segment_key(chunk_no), chunk)
            .map_err(SqlError::Storage)?;
    }
    check_cancel(cancel)?;
    let cached_gen = wtx.commit_with_generation().map_err(SqlError::Storage)?;

    // Warm the shared cache with the exact generation returned by this commit.
    // Sampling the manager after commit can mislabel this index with a later
    // writer's generation and let it survive that writer's invalidation marker.
    let cached: Arc<CachedAnnIndex> = Arc::new(CachedAnnIndex {
        index,
        dicts: outcome.dicts,
        source: AnnIndexSource::Built { refusal: None },
        cached_gen,
        identity: spec.cache_identity(table_schema),
    });
    let key = cache_key(&table_schema.name, spec.col_idx, spec.metric);
    let mut guard = schema.sql_caches.lock();
    if cached_passes_dml_barriers_locked(&guard, &table_schema.name, &cached) {
        let keep_newer = guard
            .get(&key)
            .and_then(|entry| entry.downcast_ref::<CachedAnnIndex>())
            .is_some_and(|existing| existing.cached_gen > cached.cached_gen);
        if !keep_newer {
            let as_any: Arc<dyn Any + Send + Sync> = cached;
            guard.insert(key, as_any);
        }
    }

    Ok(ann_persist::AnnSegmentInfo {
        segment_b3,
        content_fingerprint: header.content_fingerprint,
        n,
        dim: spec.dim,
        metric_tag: header.metric_tag,
        chunk_count: header.chunk_count,
    })
}

/// The queryable identity of the index currently cached for `table.column`:
/// `(source, snapshot generation)`, or `None` when nothing is cached.
pub(crate) fn ann_cache_status(
    schema: &SchemaManager,
    table_schema: &TableSchema,
    column: &str,
) -> Result<Option<(AnnIndexSource, u64)>> {
    let col_lower = column.to_ascii_lowercase();
    let col_idx = table_schema
        .columns
        .iter()
        .position(|c| c.name == col_lower)
        .ok_or_else(|| SqlError::ColumnNotFound(column.to_string()))?;
    let guard = schema.sql_caches.lock();
    for metric in [AnnMetric::L2, AnnMetric::Inner, AnnMetric::Cosine] {
        let key = cache_key(&table_schema.name, col_idx, metric);
        if let Some(entry) = guard.get(&key) {
            if let Ok(c) = Arc::clone(entry).downcast::<CachedAnnIndex>() {
                return Ok(Some((c.source.clone(), c.cached_gen)));
            }
        }
    }
    Ok(None)
}

/// The per-table last-DML generation marker's cache key. The central SQL
/// commit helper stamps it under the cache publication barrier; lookups refuse
/// any index whose snapshot predates the most recent DML commit on its table.
fn ann_dml_gen_key(table_name: &str) -> String {
    format!("ann_dml_gen:{table_name}")
}

fn ann_pending_key(table_name: &str) -> String {
    format!("ann_pending:{}", table_name.to_ascii_lowercase())
}

fn ann_failed_commit_key(table_name: &str) -> String {
    format!("ann_failed_commit:{}", table_name.to_ascii_lowercase())
}

#[derive(Debug, Clone, Default)]
struct AnnPendingCommits {
    tokens: Vec<u64>,
}

#[derive(Debug, Clone, Copy)]
struct AnnFailedCommit {
    token: u64,
}

fn ann_commit_pending_locked(
    entries: &FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    table: &str,
) -> bool {
    entries.contains_key(&ann_pending_key(table))
}

fn ann_failed_commit_locked(
    entries: &FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    table: &str,
) -> bool {
    entries.contains_key(&ann_failed_commit_key(table))
}

fn install_pending_locked(
    entries: &mut FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    table: &str,
    token: u64,
) {
    let key = ann_pending_key(table);
    let mut pending = entries
        .get(&key)
        .and_then(|entry| entry.downcast_ref::<AnnPendingCommits>())
        .cloned()
        .unwrap_or_default();
    if !pending.tokens.contains(&token) {
        pending.tokens.push(token);
    }
    entries.insert(key, Arc::new(pending));
}

fn clear_pending_locked(
    entries: &mut FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    table: &str,
    token: u64,
) {
    let key = ann_pending_key(table);
    let Some(mut pending) = entries
        .get(&key)
        .and_then(|entry| entry.downcast_ref::<AnnPendingCommits>())
        .cloned()
    else {
        // An absent token is already clear. A wrong-typed internal entry stays
        // in place so lookup continues to fail closed.
        return;
    };
    pending.tokens.retain(|candidate| *candidate != token);
    if pending.tokens.is_empty() {
        entries.remove(&key);
    } else {
        entries.insert(key, Arc::new(pending));
    }
}

fn install_dirty_pending_locked(
    entries: &mut FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    dirty: &crate::schema::DmlDirty,
    token: u64,
) {
    for table in &dirty.mutating {
        install_pending_locked(entries, table, token);
    }
    for (table, _) in &dirty.appends {
        install_pending_locked(entries, table, token);
    }
}

/// Clear a failed-commit barrier only when this successful writer is newer than
/// the writer which installed it. Write transaction ids are allocated from a
/// process-local monotonic sequence even when a transaction later fails.
fn clear_recoverable_failure_locked(
    entries: &mut FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    table: &str,
    successful_token: u64,
) -> bool {
    let key = ann_failed_commit_key(table);
    let recoverable = entries
        .get(&key)
        .and_then(|entry| entry.downcast_ref::<AnnFailedCommit>())
        .is_some_and(|failed| failed.token < successful_token);
    if recoverable {
        entries.remove(&key);
    }
    recoverable
}

const ANN_APPEND_HISTORY_LIMIT: usize = 256;

fn ann_append_history_key(table_name: &str) -> String {
    format!("ann_append_history:{}", table_name.to_ascii_lowercase())
}

#[derive(Debug, Clone, Copy)]
struct AnnAppendEvent {
    generation: u64,
    min_pk: i64,
}

/// Bounded append history used as a publication barrier for in-flight builds.
/// Once old events are compacted, indexes older than compacted_through are
/// conservatively rebuilt; this keeps memory bounded without ever accepting a
/// build that may have missed a gap-fill append.
#[derive(Debug, Clone, Default)]
struct AnnAppendHistory {
    compacted_through: u64,
    compacted_min_pk: Option<i64>,
    events: VecDeque<AnnAppendEvent>,
}

impl AnnAppendHistory {
    fn record(&mut self, generation: u64, min_pk: i64) {
        if let Some(event) = self
            .events
            .iter_mut()
            .find(|event| event.generation == generation)
        {
            event.min_pk = event.min_pk.min(min_pk);
            return;
        }
        let pos = self
            .events
            .iter()
            .position(|event| event.generation > generation)
            .unwrap_or(self.events.len());
        self.events
            .insert(pos, AnnAppendEvent { generation, min_pk });
        while self.events.len() > ANN_APPEND_HISTORY_LIMIT {
            if let Some(event) = self.events.pop_front() {
                self.compacted_through = self.compacted_through.max(event.generation);
                self.compacted_min_pk = Some(
                    self.compacted_min_pk
                        .map_or(event.min_pk, |min_pk| min_pk.min(event.min_pk)),
                );
            }
        }
    }

    fn allows(&self, cached: &CachedAnnIndex) -> bool {
        let snapshot_max = cached.index.snapshot_max as i64;
        if cached.cached_gen < self.compacted_through
            && self
                .compacted_min_pk
                .is_none_or(|min_pk| snapshot_max < 0 || min_pk <= snapshot_max)
        {
            return false;
        }
        self.events
            .iter()
            .filter(|event| event.generation > cached.cached_gen)
            .all(|event| snapshot_max >= 0 && event.min_pk > snapshot_max)
    }
}

fn append_history_locked<'a>(
    entries: &'a FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    table_name: &str,
) -> Option<&'a AnnAppendHistory> {
    entries
        .get(&ann_append_history_key(table_name))
        .and_then(|entry| entry.downcast_ref::<AnnAppendHistory>())
}

fn record_append_locked(
    entries: &mut FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    table: &str,
    min_pk: i64,
    generation: u64,
) -> bool {
    let prefix = format!("ann:{}:", table.to_ascii_lowercase());
    let history_key = ann_append_history_key(table);
    let mut history = entries
        .get(&history_key)
        .and_then(|entry| entry.downcast_ref::<AnnAppendHistory>())
        .cloned()
        .unwrap_or_default();
    history.record(generation, min_pk);
    let history_any: Arc<dyn Any + Send + Sync> = Arc::new(history.clone());
    entries.insert(history_key, history_any);
    entries.iter().all(|(key, val)| {
        !key.starts_with(&prefix)
            || val
                .downcast_ref::<CachedAnnIndex>()
                .is_none_or(|cached| history.allows(cached))
    })
}

fn hard_invalidate_locked(
    entries: &mut FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    table: &str,
    generation: u64,
) {
    let prefix = format!("ann:{}:", table.to_ascii_lowercase());
    entries.retain(|key, _| !key.starts_with(&prefix));
    // Finalization can be reordered after storage releases the single-writer
    // lock. Never let an older finalizer lower a newer table marker.
    let generation =
        marker_gen_locked(entries, table).map_or(generation, |old| old.max(generation));
    let marker: Arc<dyn Any + Send + Sync> = Arc::new(generation);
    entries.insert(ann_dml_gen_key(table), marker);
}

fn publish_dml_locked(
    entries: &mut FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    dirty: &crate::schema::DmlDirty,
    generation: u64,
    token: u64,
) {
    for table in &dirty.mutating {
        clear_recoverable_failure_locked(entries, table, token);
        hard_invalidate_locked(entries, table, generation);
        clear_pending_locked(entries, table, token);
    }
    for (table, min_pk) in &dirty.appends {
        let recovering = clear_recoverable_failure_locked(entries, table, token);
        if recovering || !record_append_locked(entries, table, *min_pk, generation) {
            hard_invalidate_locked(entries, table, generation);
        }
        clear_pending_locked(entries, table, token);
    }
}

fn fail_dml_locked(
    entries: &mut FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    dirty: &crate::schema::DmlDirty,
    token: u64,
) {
    let mut fail_table = |table: &str| {
        let prefix = format!("ann:{}:", table.to_ascii_lowercase());
        entries.retain(|key, _| !key.starts_with(&prefix));
        let key = ann_failed_commit_key(table);
        let failure_token = entries
            .get(&key)
            .and_then(|entry| entry.downcast_ref::<AnnFailedCommit>())
            .map_or(token, |failed| failed.token.max(token));
        entries.insert(
            key,
            Arc::new(AnnFailedCommit {
                token: failure_token,
            }),
        );
        clear_pending_locked(entries, table, token);
    };
    for table in &dirty.mutating {
        fail_table(table);
    }
    for (table, _) in &dirty.appends {
        fail_table(table);
    }
}

#[cfg(test)]
thread_local! {
    static PAUSE_AFTER_ANN_COMMIT: std::cell::RefCell<Option<(
        Arc<std::sync::Barrier>,
        Arc<std::sync::Barrier>,
    )>> = const { std::cell::RefCell::new(None) };
    static PAUSE_AFTER_ANN_BUILD: std::cell::RefCell<Option<(
        Arc<std::sync::Barrier>,
        Arc<std::sync::Barrier>,
    )>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn pause_after_ann_commit_before_publish() {
    PAUSE_AFTER_ANN_COMMIT.with(|slot| {
        if let Some((committed, resume)) = slot.borrow().as_ref() {
            committed.wait();
            resume.wait();
        }
    });
}

#[cfg(test)]
struct AnnCommitPauseGuard;

#[cfg(test)]
impl Drop for AnnCommitPauseGuard {
    fn drop(&mut self) {
        PAUSE_AFTER_ANN_COMMIT.with(|slot| slot.borrow_mut().take());
    }
}

#[cfg(test)]
fn pause_next_ann_commit(
    committed: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
) -> AnnCommitPauseGuard {
    PAUSE_AFTER_ANN_COMMIT.with(|slot| {
        assert!(slot.borrow_mut().replace((committed, resume)).is_none());
    });
    AnnCommitPauseGuard
}

#[cfg(test)]
fn pause_after_ann_build_before_write() {
    PAUSE_AFTER_ANN_BUILD.with(|slot| {
        if let Some((built, resume)) = slot.borrow().as_ref() {
            built.wait();
            resume.wait();
        }
    });
}

#[cfg(test)]
struct AnnBuildPauseGuard;

#[cfg(test)]
impl Drop for AnnBuildPauseGuard {
    fn drop(&mut self) {
        PAUSE_AFTER_ANN_BUILD.with(|slot| slot.borrow_mut().take());
    }
}

#[cfg(test)]
fn pause_next_ann_build(
    built: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
) -> AnnBuildPauseGuard {
    PAUSE_AFTER_ANN_BUILD.with(|slot| {
        assert!(slot.borrow_mut().replace((built, resume)).is_none());
    });
    AnnBuildPauseGuard
}

/// RAII owner for pre-commit pending tokens. Any unwind after installation is
/// conservatively finalized as an ambiguous failed commit, so a caught panic
/// cannot strand a permanent pending token or expose an old cache entry.
struct AnnPublicationGuard<'a> {
    schema: &'a SchemaManager,
    dirty: &'a crate::schema::DmlDirty,
    token: u64,
    armed: bool,
}

impl<'a> AnnPublicationGuard<'a> {
    fn install(schema: &'a SchemaManager, dirty: &'a crate::schema::DmlDirty, token: u64) -> Self {
        let guard = Self {
            schema,
            dirty,
            token,
            armed: true,
        };
        {
            let mut entries = schema.sql_caches.lock();
            install_dirty_pending_locked(&mut entries, dirty, token);
        }
        guard
    }

    fn finish_success(&mut self, generation: u64) {
        let mut entries = self.schema.sql_caches.lock();
        publish_dml_locked(&mut entries, self.dirty, generation, self.token);
        self.armed = false;
    }

    fn finish_failure(&mut self) {
        let mut entries = self.schema.sql_caches.lock();
        fail_dml_locked(&mut entries, self.dirty, self.token);
        self.armed = false;
    }
}

impl Drop for AnnPublicationGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let mut entries = self.schema.sql_caches.lock();
            fail_dml_locked(&mut entries, self.dirty, self.token);
        }
    }
}

/// Commit a SQL write with a short per-table ANN publication barrier. Tokens are
/// installed under the cache mutex before storage commit; lookups fail closed
/// until finalization replaces the token with the exact commit generation.
pub(crate) fn commit_with_ann_publication(
    wtx: WriteTxn<'_>,
    schema: &SchemaManager,
) -> Result<u64> {
    if !schema.has_dml_dirty() {
        return wtx.commit_with_generation().map_err(SqlError::Storage);
    }

    let token = wtx.txn_id().as_u64();
    let dirty = schema.drain_dml_dirty();
    let mut publication = AnnPublicationGuard::install(schema, &dirty, token);

    match wtx.commit_with_generation() {
        Ok(generation) => {
            #[cfg(test)]
            pause_after_ann_commit_before_publish();
            publication.finish_success(generation);
            Ok(generation)
        }
        Err(error) => {
            publication.finish_failure();
            Err(SqlError::Storage(error))
        }
    }
}

/// Read the marker under an already-held cache lock.
fn marker_gen_locked(
    entries: &FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    table_name: &str,
) -> Option<u64> {
    entries
        .get(&ann_dml_gen_key(table_name))
        .and_then(|e| e.downcast_ref::<u64>())
        .copied()
}

fn cached_passes_dml_barriers_locked(
    entries: &FxHashMap<String, Arc<dyn Any + Send + Sync>>,
    table_name: &str,
    cached: &CachedAnnIndex,
) -> bool {
    if ann_commit_pending_locked(entries, table_name)
        || ann_failed_commit_locked(entries, table_name)
    {
        return false;
    }
    if marker_gen_locked(entries, table_name).is_some_and(|g| cached.cached_gen < g) {
        return false;
    }
    append_history_locked(entries, table_name).is_none_or(|history| history.allows(cached))
}

fn lookup_cached(
    schema: &SchemaManager,
    cache_key: &str,
    table_name: &str,
    expected_identity: &AnnCacheIdentity,
    snapshot_gen: Option<u64>,
) -> Result<Option<Arc<CachedAnnIndex>>> {
    let Some(snapshot_gen) = snapshot_gen else {
        // A write view can contain uncommitted rows and must never consume a
        // committed shared index.
        return Ok(None);
    };
    let mut guard = schema.sql_caches.lock();
    if ann_commit_pending_locked(&guard, table_name) || ann_failed_commit_locked(&guard, table_name)
    {
        // Preserve an old entry while the writer is pending; a right-edge
        // append may make it usable again once exact-generation publication
        // completes. Builds during this window remain private.
        return Ok(None);
    }
    let Some(entry) = guard.get(cache_key) else {
        return Ok(None);
    };
    let entry = Arc::clone(entry)
        .downcast::<CachedAnnIndex>()
        .map_err(|_| SqlError::InvalidValue(format!("ANN cache type mismatch for {cache_key}")))?;
    if entry.cached_gen > snapshot_gen {
        // An explicit old read snapshot must not consume a cache built from a
        // future commit. Keep the future entry for current readers and build a
        // private index for this old snapshot.
        return Ok(None);
    }
    if &entry.identity != expected_identity {
        // Same compact key, different declaration (for example DROP/CREATE with
        // a new filter list or collation): never reuse its dictionaries/index.
        guard.remove(cache_key);
        return Ok(None);
    }
    if !cached_passes_dml_barriers_locked(&guard, table_name, &entry) {
        // Entry predates a DML commit (a build that raced eviction): drop and rebuild.
        guard.remove(cache_key);
        return Ok(None);
    }
    Ok(Some(entry))
}

pub(super) fn cache_key(table_name: &str, col_idx: usize, metric: AnnMetric) -> String {
    let tag = match metric {
        AnnMetric::L2 => "l2",
        AnnMetric::Inner => "inner",
        AnnMetric::Cosine => "cosine",
    };
    format!(
        "ann:{}:{}:{}",
        table_name.to_ascii_lowercase(),
        col_idx,
        tag
    )
}

fn ann_metric_to_prism(m: AnnMetric) -> Metric {
    match m {
        AnnMetric::L2 => Metric::L2,
        AnnMetric::Inner => Metric::InnerProduct,
        AnnMetric::Cosine => Metric::Cosine,
    }
}

#[cfg(test)]
mod thrash_tests {
    use super::{tail_distance, tail_distance_checked, take_ann_rebuilds, AnnMetric};
    use crate::{Connection, ExecutionResult, Value};
    use citadel::{Argon2Profile, CancelToken, DatabaseBuilder};

    const DIM: usize = 8;

    #[test]
    fn recycled_root_id_with_changed_txn_is_refused() {
        let persisted = (41, 7);
        assert!(super::root_stamp_matches((41, 7), persisted));
        assert!(
            !super::root_stamp_matches((41, 19), persisted),
            "reusing the same physical root page under a new page txn must not pass freshness"
        );
    }

    #[test]
    fn ann_root_stamp_propagates_storage_interruption() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup(&conn);

        let token = CancelToken::new();
        token.cancel();
        let mut rtx = db.begin_read();
        rtx.set_cancel(Some(token));
        let error = super::AnnScan::ann_table_root_stamp(&mut rtx, b"t").unwrap_err();
        assert!(matches!(
            error,
            crate::error::SqlError::Storage(citadel_core::Error::Interrupted)
        ));
    }

    #[test]
    fn tail_distance_stops_after_vector_work_has_started() {
        let query = vec![1.0; 1_024];
        let vector = vec![2.0; 1_024];
        let expected = tail_distance(AnnMetric::L2, &query, &vector);
        let token = CancelToken::new();
        let mut work = 0;

        let err =
            tail_distance_checked(AnnMetric::L2, &query, &vector, &mut work, |current_work| {
                if current_work == 512 {
                    token.cancel();
                }
                super::check_cancel_at(Some(&token), current_work)
            })
            .unwrap_err();

        assert!(matches!(
            err,
            crate::error::SqlError::Storage(citadel_core::Error::Interrupted)
        ));
        assert_eq!(work, 512, "cancellation must stop the vector loop early");

        let mut untripped_work = 0;
        let actual =
            tail_distance_checked(AnnMetric::L2, &query, &vector, &mut untripped_work, |_| {
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn persist_ann_honors_the_database_cancel_token() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup(&conn);
        insert(&conn, 1, &vec_for(1));
        build_index(&conn);

        let token = CancelToken::new();
        token.cancel();
        db.set_cancel(Some(token));
        let err = conn.persist_ann_index("missing", "v").unwrap_err();
        assert!(matches!(
            err,
            crate::error::SqlError::Storage(citadel_core::Error::Interrupted)
        ));

        let err = conn.persist_ann_index("t", "v").unwrap_err();

        assert!(matches!(
            err,
            crate::error::SqlError::Storage(citadel_core::Error::Interrupted)
        ));
    }

    #[test]
    fn segment_interruption_is_not_classified_as_corruption() {
        let error = super::segment_operation_error(
            citadel_vector::segment::SegmentOperationError::Interrupted,
        );
        assert!(matches!(
            error,
            crate::error::SqlError::Storage(citadel_core::Error::Interrupted)
        ));
    }

    #[test]
    fn only_a_missing_segment_table_is_treated_as_no_segment() {
        let missing = super::classify_segment_header_read(Err(crate::error::SqlError::Storage(
            citadel_core::Error::TableNotFound("__annseg_t".into()),
        )));
        assert!(matches!(missing, Ok(None)));

        let corrupt = super::classify_segment_header_read(Err(crate::error::SqlError::Storage(
            citadel_core::Error::CorruptOverflowChain("injected".into()),
        )));
        assert!(matches!(
            corrupt,
            Err(crate::error::SqlError::Storage(
                citadel_core::Error::CorruptOverflowChain(_)
            ))
        ));

        let interrupted = super::classify_segment_header_read(Err(
            crate::error::SqlError::Storage(citadel_core::Error::Interrupted),
        ));
        assert!(matches!(
            interrupted,
            Err(crate::error::SqlError::Storage(
                citadel_core::Error::Interrupted
            ))
        ));
    }

    fn vec_for(i: u64) -> Vec<f32> {
        (0..DIM)
            .map(|d| {
                let x = (i.wrapping_mul(2654435761).wrapping_add(d as u64 * 40503) % 1000) as f32;
                x / 1000.0
            })
            .collect()
    }

    fn vec_literal(v: &[f32]) -> String {
        let parts: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
        format!("'[{}]'::VECTOR({})", parts.join(", "), DIM)
    }

    fn recall_ids(conn: &Connection<'_>, qvec: &[f32], k: usize) -> Vec<i64> {
        let sql = format!(
            "SELECT id FROM t WHERE category = 0 ORDER BY v <-> {} LIMIT {k}",
            vec_literal(qvec)
        );
        match conn.execute(&sql).unwrap() {
            ExecutionResult::Query(qr) => qr
                .rows
                .iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => *i,
                    other => panic!("expected Integer id, got {other:?}"),
                })
                .collect(),
            _ => panic!("expected query result"),
        }
    }

    /// Interleaved append+recall must tail-merge, not rebuild per recall (thrash).
    #[test]
    fn interleaved_append_recall_does_not_thrash() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("test.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score REAL, v VECTOR(8))",
        )
        .unwrap();
        // category 0 for everything so the pushable filter keeps all rows.
        let base = 200u64;
        for i in 1..=base {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 0, 1.0, {})",
                vec_literal(&vec_for(i))
            ))
            .unwrap();
        }
        conn.execute(
            "CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2', filters = 'category')",
        )
        .unwrap();

        // Warm: first recall builds/loads + caches.
        let _ = recall_ids(&conn, &vec_for(7), 5);
        let _ = take_ann_rebuilds(); // reset after warm-up

        // Each append is a unique off-grid vector, queried exactly -> it's the nearest.
        let appends = 10u64;
        let mut total_rebuilds = 0u64;
        for j in 0..appends {
            let new_id = base + 1 + j;
            let qvec = vec![0.50005f32 + (j as f32) * 0.0001; DIM];
            conn.execute(&format!(
                "INSERT INTO t VALUES ({new_id}, 0, 1.0, {})",
                vec_literal(&qvec)
            ))
            .unwrap();
            let ids = recall_ids(&conn, &qvec, 5);
            total_rebuilds += take_ann_rebuilds();
            assert_eq!(
                ids.first().copied(),
                Some(new_id as i64),
                "freshly appended exact-match row must rank #0 (I1 fresh-visibility)"
            );
        }
        assert_eq!(
            total_rebuilds, 0,
            "appends must not trigger PRISM rebuilds (got {total_rebuilds} over {appends} recalls = thrash)"
        );
    }

    fn fresh_db(dir: &std::path::Path) -> citadel::Database {
        DatabaseBuilder::new(dir.join("t.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap()
    }

    fn setup(conn: &Connection<'_>) {
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, score REAL, v VECTOR(8))",
        )
        .unwrap();
    }

    fn insert(conn: &Connection<'_>, id: u64, v: &[f32]) {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({id}, 0, 1.0, {})",
            vec_literal(v)
        ))
        .unwrap();
    }

    fn build_index(conn: &Connection<'_>) {
        conn.execute(
            "CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2', filters = 'category')",
        )
        .unwrap();
    }

    #[test]
    fn commit_pending_barrier_does_not_hold_the_global_cache_mutex() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup(&conn);
        for i in 1..=50 {
            insert(&conn, i, &vec_for(i));
        }
        for i in 60..=100 {
            insert(&conn, i, &vec_for(i));
        }
        build_index(&conn);
        let _ = recall_ids(&conn, &vec_for(7), 1);

        let committed = std::sync::Arc::new(std::sync::Barrier::new(2));
        let resume = std::sync::Arc::new(std::sync::Barrier::new(2));
        let qvec = vec![0.50013f32; DIM];
        let (cache_available, pending_visible, reader_before_publish) =
            std::thread::scope(|scope| {
                let writer_db = &db;
                let writer = scope.spawn({
                    let committed = std::sync::Arc::clone(&committed);
                    let resume = std::sync::Arc::clone(&resume);
                    let qvec = qvec.clone();
                    move || {
                        let _pause = super::pause_next_ann_commit(committed, resume);
                        let writer = Connection::open(writer_db).unwrap();
                        insert(&writer, 55, &qvec);
                    }
                });

                committed.wait();
                // Storage has committed, but publication is intentionally paused.
                // The global mutex must be free while the per-table pending token
                // prevents a post-commit reader from cloning the old ANN entry.
                let cache = db.sql_cache_handle();
                let (cache_available, pending_visible) = match cache.try_lock() {
                    Some(guard) => (true, super::ann_commit_pending_locked(&guard, "t")),
                    None => (false, false),
                };
                let (reader_tx, reader_rx) = std::sync::mpsc::channel();
                let reader_db = &db;
                let reader_qvec = qvec.clone();
                let reader = scope.spawn(move || {
                    let reader = Connection::open(reader_db).unwrap();
                    reader_tx
                        .send(recall_ids(&reader, &reader_qvec, 1))
                        .unwrap();
                });
                let reader_before_publish =
                    reader_rx.recv_timeout(std::time::Duration::from_secs(5));
                resume.wait();
                writer.join().unwrap();
                reader.join().unwrap();
                (cache_available, pending_visible, reader_before_publish)
            });

        assert!(
            cache_available,
            "storage commit/fsync must not hold the global SQL cache mutex"
        );
        assert!(
            pending_visible,
            "the dirty table must stay behind a pending publication token"
        );
        let ids = reader_before_publish
            .expect("a post-commit ANN reader must finish while publication is pending");
        assert_eq!(
            ids.first().copied(),
            Some(55),
            "a reader whose snapshot starts after commit must see the gap-fill row"
        );
    }

    #[test]
    fn persist_build_rechecks_stamp_before_taking_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup(&conn);
        for i in 1..=40 {
            insert(&conn, i, &vec_for(i));
        }
        build_index(&conn);

        let built = std::sync::Arc::new(std::sync::Barrier::new(2));
        let resume = std::sync::Arc::new(std::sync::Barrier::new(2));
        let result = std::thread::scope(|scope| {
            let persist = scope.spawn({
                let built = std::sync::Arc::clone(&built);
                let resume = std::sync::Arc::clone(&resume);
                || {
                    let _pause = super::pause_next_ann_build(built, resume);
                    let persister = Connection::open(&db).unwrap();
                    persister.persist_ann_index("t", "v")
                }
            });

            built.wait();
            let writer = Connection::open(&db).unwrap();
            insert(&writer, 41, &vec_for(41));
            resume.wait();
            persist.join().unwrap()
        });

        let error = result.expect_err("a build from the old stamp must not be persisted");
        assert!(
            error.to_string().contains("table changed"),
            "unexpected persistence error: {error}"
        );
        let mut rtx = db.begin_read();
        assert!(matches!(
            rtx.table_get(b"__annseg_t", &0u32.to_be_bytes()),
            Err(citadel_core::Error::TableNotFound(_))
        ));
    }

    /// I2: an in-place vector UPDATE must hard-invalidate (new vector reflected).
    #[test]
    fn inplace_vector_update_is_reflected() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup(&conn);
        for i in 1..=200 {
            insert(&conn, i, &vec_for(i));
        }
        build_index(&conn);
        let qvec = vec![0.50007f32; DIM];
        let _ = recall_ids(&conn, &vec_for(7), 5); // warm
        let _ = take_ann_rebuilds();

        conn.execute(&format!(
            "UPDATE t SET v = {} WHERE id = 50",
            vec_literal(&qvec)
        ))
        .unwrap();
        let ids = recall_ids(&conn, &qvec, 5);
        assert!(
            take_ann_rebuilds() >= 1,
            "an in-place vector UPDATE must invalidate the cached index"
        );
        assert_eq!(ids.first().copied(), Some(50), "updated row must rank #0");
    }

    /// A DELETE of an indexed row must hard-invalidate so the row stops appearing.
    #[test]
    fn delete_indexed_row_disappears() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup(&conn);
        for i in 1..=200 {
            insert(&conn, i, &vec_for(i));
        }
        build_index(&conn);
        let q = vec_for(7);
        let before = recall_ids(&conn, &q, 5);
        assert_eq!(before.first().copied(), Some(7), "id 7 is the exact match");
        let _ = take_ann_rebuilds();

        conn.execute("DELETE FROM t WHERE id = 7").unwrap();
        let after = recall_ids(&conn, &q, 5);
        assert!(
            take_ann_rebuilds() >= 1,
            "a DELETE must invalidate the cached index"
        );
        assert!(
            !after.contains(&7),
            "deleted row must not appear: {after:?}"
        );
    }

    /// I3: a gap-fill INSERT below the snapshot must hard-invalidate (tail misses it).
    #[test]
    fn gap_fill_below_snapshot_is_visible() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup(&conn);
        // Leave a gap at ids 51..=59; snapshot_max becomes 100.
        for i in 1..=50 {
            insert(&conn, i, &vec_for(i));
        }
        for i in 60..=100 {
            insert(&conn, i, &vec_for(i));
        }
        build_index(&conn);
        let _ = recall_ids(&conn, &vec_for(7), 5); // warm, snapshot_max = 100
        let _ = take_ann_rebuilds();

        let qvec = vec![0.50009f32; DIM];
        insert(&conn, 55, &qvec); // gap-fill: 55 < snapshot_max
        let ids = recall_ids(&conn, &qvec, 5);
        assert!(
            take_ann_rebuilds() >= 1,
            "a gap-fill insert below snapshot must invalidate, not tail-merge"
        );
        assert_eq!(
            ids.first().copied(),
            Some(55),
            "gap-fill row must be visible at rank #0: {ids:?}"
        );
    }

    /// A tail past the threshold triggers exactly one rebuild on recall.
    #[test]
    fn long_tail_triggers_single_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup(&conn);
        for i in 1..=40 {
            insert(&conn, i, &vec_for(i));
        }
        build_index(&conn);
        let _ = recall_ids(&conn, &vec_for(7), 5); // warm, snapshot_max = 40, indexed_len/4 = 10
        let _ = take_ann_rebuilds();

        // Append 15 rows (> indexed_len/4) with no recall between: all retained.
        let qvec = vec![0.50011f32; DIM];
        for i in 41..=55u64 {
            let v = if i == 55 {
                qvec.clone()
            } else {
                vec_for(i + 1000)
            };
            insert(&conn, i, &v);
        }
        assert_eq!(
            take_ann_rebuilds(),
            0,
            "appends alone must not rebuild (retained for tail merge)"
        );

        let ids = recall_ids(&conn, &qvec, 5);
        assert_eq!(
            take_ann_rebuilds(),
            1,
            "a tail past the threshold must trigger exactly one rebuild on recall"
        );
        assert_eq!(
            ids.first().copied(),
            Some(55),
            "post-rebuild result correct"
        );
    }

    #[test]
    fn recreated_ann_declaration_does_not_reuse_old_filter_dictionaries() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup(&conn);
        for i in 1..=40 {
            insert(&conn, i, &vec_for(i));
        }
        build_index(&conn);
        let _ = recall_ids(&conn, &vec_for(7), 5);

        conn.execute("DROP INDEX ix_v").unwrap();
        conn.execute(
            "CREATE INDEX ix_v ON t USING ann (v) WITH \
             (metric = 'l2', filters = 'category,score')",
        )
        .unwrap();
        let sql = format!(
            "SELECT id FROM t WHERE score = 1.0 ORDER BY v <-> {} LIMIT 1",
            vec_literal(&vec_for(7))
        );
        let ExecutionResult::Query(result) = conn.execute(&sql).unwrap() else {
            panic!("expected query result");
        };
        assert_eq!(result.rows, vec![vec![Value::Integer(7)]]);
    }

    #[test]
    fn old_read_snapshot_does_not_consume_future_ann_cache() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let current = Connection::open(&db).unwrap();
        setup(&current);
        for i in 1..=40 {
            insert(&current, i, &vec_for(i));
        }
        build_index(&current);

        let old = Connection::open(&db).unwrap();
        old.execute("BEGIN READ ONLY").unwrap();
        current.execute("DELETE FROM t").unwrap();
        insert(&current, 1_000, &vec_for(1_000));
        let future = recall_ids(&current, &vec_for(1_000), 1);
        assert_eq!(future, vec![1_000]);

        let snapshot = recall_ids(&old, &vec_for(7), 1);
        assert_eq!(
            snapshot,
            vec![7],
            "old snapshot must build/serve its own index, not the future cache"
        );
        old.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn append_history_blocks_a_gap_fill_that_raced_an_empty_cache() {
        let index = citadel_vector::AnnIndex::build_with_attrs(
            (1..=10)
                .map(|id| (id, vec![id as f32; DIM], vec![0]))
                .collect(),
            1,
            citadel_vector::Metric::L2,
            DIM as u16,
        )
        .unwrap();
        let cached = super::CachedAnnIndex {
            index,
            dicts: vec![rustc_hash::FxHashMap::default()],
            source: super::AnnIndexSource::Built { refusal: None },
            cached_gen: 10,
            identity: super::AnnCacheIdentity {
                dim: DIM as u16,
                metric: AnnMetric::L2,
                filter_cols: vec![1],
                filter_collations: vec![crate::types::Collation::Binary],
                prism_config_hash: super::ann_persist::active_config_hash(
                    citadel_vector::Metric::L2,
                ),
            },
        };

        let mut history = super::AnnAppendHistory::default();
        history.record(11, 11);
        assert!(history.allows(&cached), "right-edge append can tail-merge");
        history.record(12, 5);
        assert!(
            !history.allows(&cached),
            "gap-fill committed after the build snapshot must block publication"
        );

        let mut long_right_edge_history = super::AnnAppendHistory::default();
        for generation in 11..(11 + super::ANN_APPEND_HISTORY_LIMIT as u64 + 8) {
            long_right_edge_history.record(generation, 11 + generation as i64);
        }
        assert!(
            long_right_edge_history.allows(&cached),
            "bounded history compaction must preserve safe right-edge appends"
        );
    }

    #[test]
    fn failed_commit_barrier_survives_older_finalization_and_then_recovers() {
        let dirty = crate::schema::DmlDirty {
            mutating: vec!["t".into()],
            appends: Vec::new(),
        };
        let mut entries: rustc_hash::FxHashMap<
            String,
            std::sync::Arc<dyn std::any::Any + Send + Sync>,
        > = rustc_hash::FxHashMap::default();
        entries.insert("ann:t:0:l2".into(), std::sync::Arc::new(1u64));

        super::install_dirty_pending_locked(&mut entries, &dirty, 10);
        super::fail_dml_locked(&mut entries, &dirty, 10);
        assert!(super::ann_failed_commit_locked(&entries, "t"));
        assert!(!super::ann_commit_pending_locked(&entries, "t"));
        assert!(
            !entries.contains_key("ann:t:0:l2"),
            "ambiguous failure must evict the old ANN entry"
        );

        // A delayed finalizer from an older successful writer cannot clear a
        // newer ambiguous failure.
        super::publish_dml_locked(&mut entries, &dirty, 9, 9);
        assert!(super::ann_failed_commit_locked(&entries, "t"));

        // A newer success is authoritative and replaces the conservative barrier.
        super::install_dirty_pending_locked(&mut entries, &dirty, 11);
        super::publish_dml_locked(&mut entries, &dirty, 10, 11);
        assert!(!super::ann_failed_commit_locked(&entries, "t"));
        assert!(!super::ann_commit_pending_locked(&entries, "t"));
        assert_eq!(super::marker_gen_locked(&entries, "t"), Some(10));
    }

    #[test]
    fn publication_guard_drop_converts_pending_to_failed_barrier() {
        let schema = crate::schema::SchemaManager::empty();
        let dirty = crate::schema::DmlDirty {
            mutating: vec!["t".into()],
            appends: Vec::new(),
        };

        {
            let _publication = super::AnnPublicationGuard::install(&schema, &dirty, 42);
            let entries = schema.sql_caches.lock();
            assert!(super::ann_commit_pending_locked(&entries, "t"));
            assert!(!super::ann_failed_commit_locked(&entries, "t"));
        }

        let entries = schema.sql_caches.lock();
        assert!(
            !super::ann_commit_pending_locked(&entries, "t"),
            "guard unwind/drop must not strand a pending token"
        );
        assert!(
            super::ann_failed_commit_locked(&entries, "t"),
            "guard unwind/drop must fail closed after an ambiguous commit"
        );
    }
}
