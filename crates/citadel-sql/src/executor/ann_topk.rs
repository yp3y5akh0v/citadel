//! Plans for `SELECT ... ORDER BY col <dist> :q LIMIT k`: [`AnnTopKPlan`] uses a
//! cached PRISM index; [`VectorTopKPlan`] streams a bounded-heap top-k when no
//! index applies or inside a write txn (uncommitted rows).

use std::any::Any;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use citadel_txn::read_txn::ReadTxn;
use citadel_txn::write_txn::WriteTxn;
use citadel_vector::{AnnIndex, Filter, Metric};
use rustc_hash::FxHashMap;

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
use super::helpers::{decode_full_row, eval_const_expr, eval_const_int, project_rows};
use super::window::has_any_window_function;

type StorageResult<T> = std::result::Result<T, citadel_core::Error>;
type ScanRow<'a> = dyn FnMut(&[u8], &[u8]) -> Result<bool> + 'a;
type RawScanRow<'a> = dyn FnMut(&[u8], &[u8]) -> StorageResult<bool> + 'a;

/// Scan + point-get over a read or write txn, materializing overflow values.
pub(super) trait AnnScan {
    fn ann_scan(&mut self, table: &[u8], f: &mut ScanRow<'_>) -> Result<()>;
    fn ann_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Commit generation this snapshot reflects; `None` when the view has uncommitted
    /// writes - such an index cannot enter the shared cache.
    fn cache_generation(&self) -> Option<u64>;
    /// The table's live catalog root (the CoW freshness anchor) - a lookup, not a scan.
    fn ann_table_root(&self, table: &[u8]) -> Option<u64>;
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

    fn ann_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.table_get(table, key).map_err(SqlError::Storage)
    }

    fn cache_generation(&self) -> Option<u64> {
        Some(self.commit_generation())
    }

    fn ann_table_root(&self, table: &[u8]) -> Option<u64> {
        self.table_root_page(table)
            .ok()
            .flatten()
            .map(|p| u64::from(p.0))
    }
}

impl AnnScan for WriteTxn<'_> {
    fn ann_scan(&mut self, table: &[u8], f: &mut ScanRow<'_>) -> Result<()> {
        bridge_scan(|cb| self.table_scan_from(table, b"", cb), f)
    }

    fn ann_get(&mut self, table: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.table_get(table, key).map_err(SqlError::Storage)
    }

    fn cache_generation(&self) -> Option<u64> {
        None
    }

    fn ann_table_root(&self, table: &[u8]) -> Option<u64> {
        self.table_root_page(table)
            .ok()
            .flatten()
            .map(|p| u64::from(p.0))
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
    /// Pushable conjuncts: `(attr_dim, allowed_values)` from `col = v` / `col IN (...)`.
    pushable: Vec<(usize, Vec<Value>)>,
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
        // Empty table: nothing to build, ORDER BY ... LIMIT yields no rows.
        let Some(cached) = self.load_or_build_index(rtx, schema, &cache_key, table_schema)? else {
            return empty_result(table_schema, stmt);
        };
        self.run_query(rtx, &cached, stmt, table_schema)
    }

    /// Search the index, apply filters and the residual recheck, then page and project.
    fn run_query(
        &self,
        txn: &mut dyn AnnScan,
        cached: &CachedAnnIndex,
        stmt: &SelectStmt,
        table_schema: &TableSchema,
    ) -> Result<ExecutionResult> {
        // Map values to codes via the dict's collation-canonical encoding; an absent value matches nothing.
        let mut constraints: Vec<(usize, Vec<u32>)> = Vec::with_capacity(self.pushable.len());
        for (dim, values) in &self.pushable {
            let dict = &cached.dicts[*dim];
            let coll = table_schema.columns[self.filter_cols[*dim] as usize].collation;
            let mut codes = Vec::with_capacity(values.len());
            let mut canon = Vec::with_capacity(16);
            for v in values {
                canon.clear();
                encode_key_value_collated_into(v, coll, &mut canon);
                if let Some(&code) = dict.get(canon.as_slice()) {
                    codes.push(code);
                }
            }
            if codes.is_empty() {
                return empty_result(table_schema, stmt);
            }
            constraints.push((*dim, codes));
        }
        let filter = if constraints.is_empty() {
            Filter::none()
        } else {
            Filter::new(constraints)
        };

        let want = self.k.saturating_add(self.offset).max(1);
        let mut rows = self.collect_survivors(txn, &cached.index, &filter, table_schema, want)?;

        if self.offset >= rows.len() {
            rows.clear();
        } else if self.offset > 0 {
            rows = rows.split_off(self.offset);
        }
        rows.truncate(self.k);

        let (col_names, projected) = project_rows(&table_schema.columns, &stmt.columns, rows)?;
        Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: projected,
        }))
    }

    /// Search the index (with `filter` pushed in) and recheck the residual
    /// predicate on decoded rows, over-fetching until `want` rows survive or the
    /// index is exhausted. Distance order is preserved.
    fn collect_survivors(
        &self,
        txn: &mut dyn AnnScan,
        index: &AnnIndex,
        filter: &Filter,
        table_schema: &TableSchema,
        want: usize,
    ) -> Result<Vec<Vec<Value>>> {
        let col_map = ColumnMap::new(&table_schema.columns);
        let max_target = index.indexed_len().max(1);
        let mut key_buf: Vec<u8> = Vec::with_capacity(10);
        let mut target = want;
        loop {
            target = target.min(max_target);
            let hits = index.search_filtered_default_ef(&self.query_vec, target, filter);
            let mut survivors: Vec<Vec<Value>> = Vec::with_capacity(want);
            for (id, _dist) in &hits {
                encode_int_key_into(*id as i64, &mut key_buf);
                let Some(row_bytes) = txn.ann_get(table_schema.name.as_bytes(), &key_buf)? else {
                    continue;
                };
                let row = decode_full_row(table_schema, &key_buf, &row_bytes)?;
                let keep = match &self.residual {
                    None => true,
                    Some(expr) => {
                        let ctx = EvalCtx::new(&col_map, &row);
                        is_truthy(&eval_expr(expr, &ctx)?)
                    }
                };
                if keep {
                    survivors.push(row);
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

    fn load_or_build_index(
        &self,
        txn: &mut dyn AnnScan,
        schema: &SchemaManager,
        cache_key: &str,
        table_schema: &TableSchema,
    ) -> Result<Option<Arc<CachedAnnIndex>>> {
        if let Some(existing) = lookup_cached(schema, cache_key, &table_schema.name)? {
            return Ok(Some(existing));
        }
        let spec = AnnSpec {
            col_idx: self.col_idx,
            dim: self.dim,
            metric: self.metric,
            filter_cols: self.filter_cols.clone(),
        };
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
    // eval equality); the fingerprint keeps raw encodings to still detect content edits.
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

    txn.ann_scan(table_schema.name.as_bytes(), &mut |key, value| {
        let vector = match decode_column_raw(value, enc_idx)?.to_value() {
            Value::Vector(arr) => Some(arr.to_vec()),
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
        let vec_bytes: Vec<u8> = vector
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
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

    Ok(ScanOutcome {
        rows,
        dicts,
        fingerprint: fp.finish(),
    })
}

/// Build the index from a scan; `None` if there are no indexable rows.
fn build_index(
    txn: &mut dyn AnnScan,
    table_schema: &TableSchema,
    spec: &AnnSpec,
    refusal: Option<String>,
    cached_gen: u64,
) -> Result<Option<CachedAnnIndex>> {
    let outcome = scan_rows(txn, table_schema, spec)?;
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
    Ok(Some(CachedAnnIndex {
        index,
        dicts: outcome.dicts,
        source: AnnIndexSource::Built { refusal },
        cached_gen,
    }))
}

/// Outcome of a persisted-segment load. `Refused` triggers a rebuild; corrupt
/// segments also warn (HMAC-authenticated page + failing BLAKE3 = writer bug).
enum LoadOutcome {
    Loaded(Box<CachedAnnIndex>),
    NoSegment,
    Refused { reason: String, corrupt: bool },
}

/// Try to serve the table's persisted segment: header pins, body decode, and the
/// table-root freshness gate confirming it matches this snapshot.
fn try_load_segment(
    txn: &mut dyn AnnScan,
    table_schema: &TableSchema,
    spec: &AnnSpec,
    cached_gen: u64,
) -> Result<LoadOutcome> {
    let seg_table = ann_persist::segment_table_name(&table_schema.name);
    let header_bytes = match txn.ann_get(&seg_table, &ann_persist::segment_key(0)) {
        Ok(Some(b)) => b,
        // Missing tree and missing header are both "never persisted".
        Ok(None) | Err(_) => return Ok(LoadOutcome::NoSegment),
    };
    let refuse = |reason: String, corrupt: bool| Ok(LoadOutcome::Refused { reason, corrupt });
    let header = match ann_persist::SegmentHeader::decode(&header_bytes) {
        Ok(h) => h,
        Err(e) => return refuse(format!("header: {e}"), true),
    };
    if header.format_version != ann_persist::ANNSEG_FORMAT_VERSION {
        return refuse(
            format!("format v{} (this binary reads v2)", header.format_version),
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

    let mut body = Vec::new();
    for chunk_no in 1..=header.chunk_count {
        match txn.ann_get(&seg_table, &ann_persist::segment_key(chunk_no)) {
            Ok(Some(c)) => body.extend_from_slice(&c),
            _ => return refuse(format!("missing chunk {chunk_no}"), true),
        }
    }
    if *blake3::hash(&body).as_bytes() != header.segment_b3 {
        return refuse("segment body BLAKE3 mismatch (corrupt)".into(), true);
    }
    let parts = match citadel_vector::segment::decode(&body) {
        Ok(p) => p,
        Err(e) => return refuse(format!("segment decode: {e}"), true),
    };
    if parts.n() as u64 != header.n || parts.dim() != header.dim {
        return refuse("segment body disagrees with header counts".into(), true);
    }

    // CoW freshness gate: a committed DML rewrites the root, so live root != stamp means stale.
    match txn.ann_table_root(table_schema.name.as_bytes()) {
        Some(live) if live == header.table_root => {}
        _ => {
            return refuse(
                "stale: table root moved since the segment was persisted".into(),
                false,
            )
        }
    }

    // Vectors ride in the segment (TAG_VECTORS), so the load is a bulk read, no rescan.
    let index = parts.into_index_embedded();
    Ok(LoadOutcome::Loaded(Box::new(CachedAnnIndex {
        index,
        dicts: header.dict_maps(),
        source: AnnIndexSource::Loaded {
            segment_b3: header.segment_b3,
        },
        cached_gen,
    })))
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
    if let Some(existing) = guard.get(cache_key) {
        // Another thread won the race; prefer that one and drop ours.
        return Arc::clone(existing)
            .downcast::<CachedAnnIndex>()
            .map(Some)
            .map_err(|_| {
                SqlError::InvalidValue(format!("ANN cache type mismatch for {cache_key}"))
            });
    }
    let marker = marker_gen_locked(&guard, &table_schema.name);
    if marker.is_some_and(|g| arc.cached_gen < g) {
        // DML committed during the build: a superseded snapshot. Serve this query, decline the cache.
        return Ok(Some(arc));
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
            let row = decode_full_row(table_schema, key, value)?;
            let ctx = EvalCtx::new(&col_map, &row);
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

        let mut rows: Vec<Vec<Value>> = heap.into_sorted_vec().into_iter().map(|r| r.row).collect();
        if self.offset >= rows.len() {
            rows.clear();
        } else if self.offset > 0 {
            rows = rows.split_off(self.offset);
        }
        rows.truncate(self.k);

        let (col_names, projected) = project_rows(&table_schema.columns, &stmt.columns, rows)?;
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

/// Freeze behind `Connection::persist_ann_index`: one write txn scans the table
/// (computing the fingerprint), builds PRISM, serializes + replaces the segment, and
/// commits (atomic by shadow paging). Holds the writer lock for the full build (minutes
/// on large tables) - an offline operation. Warms the shared cache so the next attach
/// loads fast and this process serves queries immediately.
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

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let outcome = scan_rows(&mut wtx, table_schema, &spec)?;
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

    let body = citadel_vector::segment::encode(&index);
    let segment_b3 = *blake3::hash(&body).as_bytes();
    // Order dict entries by code; codes are first-seen order, so by-code is scan order.
    let dicts_ordered: Vec<Vec<(Vec<u8>, u32)>> = outcome
        .dicts
        .iter()
        .map(|d| {
            let mut entries: Vec<(Vec<u8>, u32)> = d.iter().map(|(k, &v)| (k.clone(), v)).collect();
            entries.sort_by_key(|&(_, code)| code);
            entries
        })
        .collect();
    // Stamp the table's CoW root; the loader refuses a segment whose root != live.
    let table_root = wtx
        .table_root_page(table_schema.name.as_bytes())
        .map_err(SqlError::Storage)?
        .map(|p| u64::from(p.0))
        .ok_or_else(|| SqlError::InvalidValue("table vanished during ANN persist".into()))?;
    let header = ann_persist::SegmentHeader {
        format_version: ann_persist::ANNSEG_FORMAT_VERSION,
        prism_config_hash: ann_persist::active_config_hash(ann_metric_to_prism(spec.metric)),
        dim: spec.dim,
        metric_tag: spec.metric_tag(),
        n,
        snapshot_max: index.snapshot_max,
        table_root,
        col_idx: spec.col_idx as u32,
        filter_cols: spec.filter_cols.iter().map(|&c| c as u32).collect(),
        dicts: dicts_ordered,
        content_fingerprint: outcome.fingerprint,
        segment_b3,
        chunk_count: body.len().div_ceil(ann_persist::CHUNK_BYTES) as u32,
        writer: format!("citadel-sql {}", env!("CARGO_PKG_VERSION")),
    };

    let seg_table = ann_persist::segment_table_name(&table_schema.name);
    ann_persist::purge_segment(&mut wtx, &table_schema.name)?;
    wtx.create_table(&seg_table).map_err(SqlError::Storage)?;
    wtx.table_insert(&seg_table, &ann_persist::segment_key(0), &header.encode())
        .map_err(SqlError::Storage)?;
    for (chunk_no, chunk) in ann_persist::chunks(&body) {
        wtx.table_insert(&seg_table, &ann_persist::segment_key(chunk_no), chunk)
            .map_err(SqlError::Storage)?;
    }
    wtx.commit().map_err(SqlError::Storage)?;

    // Warm the shared cache: this index reflects the just-committed state
    // (single writer, so the commit is the current generation).
    let cached = CachedAnnIndex {
        index,
        dicts: outcome.dicts,
        source: AnnIndexSource::Built { refusal: None },
        cached_gen: db.manager().commit_generation(),
    };
    let key = cache_key(&table_schema.name, spec.col_idx, spec.metric);
    let as_any: Arc<dyn Any + Send + Sync> = Arc::new(cached);
    schema.sql_caches.lock().insert(key, as_any);

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

/// The per-table last-DML generation marker's cache key. Stamped by the
/// commit-time invalidation in `connection.rs`; read here to refuse any index
/// whose snapshot predates the most recent DML commit on its table.
pub(crate) fn ann_dml_gen_key(table_name: &str) -> String {
    format!("ann_dml_gen:{table_name}")
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

fn lookup_cached(
    schema: &SchemaManager,
    cache_key: &str,
    table_name: &str,
) -> Result<Option<Arc<CachedAnnIndex>>> {
    let mut guard = schema.sql_caches.lock();
    let Some(entry) = guard.get(cache_key) else {
        return Ok(None);
    };
    let entry = Arc::clone(entry)
        .downcast::<CachedAnnIndex>()
        .map_err(|_| SqlError::InvalidValue(format!("ANN cache type mismatch for {cache_key}")))?;
    if marker_gen_locked(&guard, table_name).is_some_and(|g| entry.cached_gen < g) {
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
