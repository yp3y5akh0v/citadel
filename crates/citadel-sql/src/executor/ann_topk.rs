//! Routes `SELECT ... ORDER BY col <dist> :q LIMIT k` to a cached PRISM index.
//!
//! `col = v` / `col IN (...)` on the index's declared filter columns push into
//! the PRISM cell filter; remaining predicates recheck decoded candidates. A
//! WHERE with no pushable predicate declines so the exact filtered scan runs.
//! Single INTEGER primary key only; no JOIN/GROUP/HAVING/DISTINCT/window/agg.

use std::any::Any;
use std::sync::Arc;

use citadel_txn::read_txn::ReadTxn;
use citadel_vector::{AnnIndex, Filter, Metric};
use rustc_hash::FxHashMap;

use crate::encoding::{
    decode_column_raw, decode_pk_integer, encode_int_key_into, encode_key_value,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::aggregate::is_aggregate_expr;
use super::helpers::{decode_full_row, eval_const_expr, eval_const_int, project_rows};
use super::window::has_any_window_function;

/// A cached ANN index plus the metadata needed to push SQL filters into it.
struct CachedAnnIndex {
    index: AnnIndex,
    /// Per attribute dim: maps an encoded filter-column value to its PRISM code.
    dicts: Vec<FxHashMap<Vec<u8>, u32>>,
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

impl AnnTopKPlan {
    pub(super) fn try_new(stmt: &SelectStmt, table_schema: &TableSchema) -> Result<Option<Self>> {
        if stmt.order_by.len() != 1
            || stmt.limit.is_none()
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
        let has_agg = stmt.columns.iter().any(|c| {
            matches!(c,
                SelectColumn::Expr { expr, .. } if is_aggregate_expr(expr)
            )
        });
        if has_agg {
            return Ok(None);
        }

        let ob = &stmt.order_by[0];
        if ob.descending {
            return Ok(None);
        }

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

        // No pushable predicate means the index gives no leverage; decline so
        // the exact filtered scan runs instead.
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
        let cached = self.load_or_build_index(rtx, schema, &cache_key, table_schema)?;

        // Map values to codes; a value absent from the dictionary matches no row.
        let mut constraints: Vec<(usize, Vec<u32>)> = Vec::with_capacity(self.pushable.len());
        for (dim, values) in &self.pushable {
            let dict = &cached.dicts[*dim];
            let mut codes = Vec::with_capacity(values.len());
            for v in values {
                if let Some(&code) = dict.get(encode_key_value(v).as_slice()) {
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
        let mut rows = self.collect_survivors(rtx, &cached.index, &filter, table_schema, want)?;

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
        rtx: &mut ReadTxn<'_>,
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
                let Some(row_bytes) = rtx
                    .table_get(table_schema.name.as_bytes(), &key_buf)
                    .map_err(SqlError::Storage)?
                else {
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
            // Stop when satisfied, when the index is exhausted, or when PRISM
            // returns fewer candidates than asked (no more to find).
            if survivors.len() >= want || target >= max_target || hits.len() < target {
                return Ok(survivors);
            }
            target = target.saturating_mul(2);
        }
    }

    fn load_or_build_index(
        &self,
        rtx: &mut ReadTxn<'_>,
        schema: &SchemaManager,
        cache_key: &str,
        table_schema: &TableSchema,
    ) -> Result<Arc<CachedAnnIndex>> {
        if let Some(existing) = lookup_cached(schema, cache_key)? {
            return Ok(existing);
        }
        let built = self.build_index(rtx, table_schema)?;
        let arc: Arc<CachedAnnIndex> = Arc::new(built);
        let mut guard = schema.sql_caches.lock();
        if let Some(existing) = guard.get(cache_key) {
            // Another thread won the race; prefer that one and drop ours.
            return Arc::clone(existing)
                .downcast::<CachedAnnIndex>()
                .map_err(|_| {
                    SqlError::InvalidValue(format!("ANN cache type mismatch for {cache_key}"))
                });
        }
        let as_any: Arc<dyn Any + Send + Sync> = arc.clone();
        guard.insert(cache_key.to_string(), as_any);
        Ok(arc)
    }

    fn build_index(
        &self,
        rtx: &mut ReadTxn<'_>,
        table_schema: &TableSchema,
    ) -> Result<CachedAnnIndex> {
        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let nonpk_order = non_pk
            .iter()
            .position(|&i| i == self.col_idx)
            .ok_or_else(|| {
                SqlError::InvalidValue("vector column must be non-PK for ANN build".into())
            })?;
        let enc_idx = enc_pos[nonpk_order] as usize;

        let num_attrs = self.filter_cols.len();
        let extracts: Vec<Extract> = self
            .filter_cols
            .iter()
            .map(|&c| extract_plan(c, table_schema, non_pk, enc_pos))
            .collect::<Result<_>>()?;
        let mut dicts: Vec<FxHashMap<Vec<u8>, u32>> = vec![FxHashMap::default(); num_attrs];

        let mut rows: Vec<(u64, Vec<f32>, Vec<u32>)> = Vec::new();
        let mut scan_err: Option<SqlError> = None;

        rtx.table_scan_raw(table_schema.name.as_bytes(), |key, value| {
            let id = match decode_pk_integer(key) {
                Ok(v) => v as u64,
                Err(e) => {
                    scan_err = Some(e);
                    return false;
                }
            };
            let vector = match decode_column_raw(value, enc_idx) {
                Ok(raw) => match raw.to_value() {
                    Value::Vector(arr) => arr.to_vec(),
                    Value::Null => return true, // null vectors are not indexed
                    _ => {
                        scan_err = Some(SqlError::InvalidValue(
                            "ANN column produced non-vector value".into(),
                        ));
                        return false;
                    }
                },
                Err(e) => {
                    scan_err = Some(e);
                    return false;
                }
            };

            let mut codes: Vec<u32> = Vec::with_capacity(num_attrs);
            for (j, ex) in extracts.iter().enumerate() {
                let v = match ex.extract(key, value) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                };
                let encoded = encode_key_value(&v);
                let next = dicts[j].len() as u32;
                let code = *dicts[j].entry(encoded).or_insert(next);
                codes.push(code);
            }

            rows.push((id, vector, codes));
            true
        })
        .map_err(SqlError::Storage)?;

        if let Some(e) = scan_err {
            return Err(e);
        }
        if rows.is_empty() {
            return Err(SqlError::InvalidValue(
                "ANN build requires at least one non-null vector row".into(),
            ));
        }

        let index =
            AnnIndex::build_with_attrs(rows, num_attrs, ann_metric_to_prism(self.metric), self.dim)
                .map_err(|e| SqlError::InvalidValue(format!("ANN build failed: {e}")))?;
        Ok(CachedAnnIndex { index, dicts })
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

/// A leaf is pushable if it is `col = literal` or `col IN (literal, ...)` on a
/// declared filter column with all-constant right-hand side.
fn classify_leaf(
    leaf: &Expr,
    filter_cols: &[u16],
    table_schema: &TableSchema,
) -> Option<(usize, Vec<Value>)> {
    match leaf {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let dim = filter_dim(left, filter_cols, table_schema)?;
            let val = eval_const_expr(right).ok()?;
            Some((dim, vec![val]))
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let dim = filter_dim(expr, filter_cols, table_schema)?;
            let mut vals = Vec::with_capacity(list.len());
            for e in list {
                vals.push(eval_const_expr(e).ok()?);
            }
            Some((dim, vals))
        }
        _ => None,
    }
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

fn lookup_cached(schema: &SchemaManager, cache_key: &str) -> Result<Option<Arc<CachedAnnIndex>>> {
    let guard = schema.sql_caches.lock();
    match guard.get(cache_key) {
        Some(entry) => Arc::clone(entry)
            .downcast::<CachedAnnIndex>()
            .map(Some)
            .map_err(|_| {
                SqlError::InvalidValue(format!("ANN cache type mismatch for {cache_key}"))
            }),
        None => Ok(None),
    }
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
