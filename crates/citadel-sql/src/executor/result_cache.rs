//! Generation-keyed memoization of deterministic read-only SELECT results.
//!
//! Sound because commit-generation equality proves identical visible state
//! (the counter bumps under the snapshot-publishing lock), the slot lives on
//! a per-connection plan discarded on schema changes, serving is gated to
//! `ActiveTxnRef::None | Read`, and [`is_result_cacheable`] admits only
//! statements that are a pure function of (statement, params, generation).

use rustc_hash::FxHashSet;

use crate::parser::{
    BinOp, CteDefinition, Expr, QueryBody, SelectColumn, SelectQuery, SelectStmt, Statement,
    WindowFrameBound,
};
use crate::schema::SchemaManager;
use crate::types::{QueryResult, Value};

/// Upper bound on a cached entry (result rows + key params): large results
/// cost as much to clone as to recompute and would pin memory.
const RESULT_CACHE_MAX_BYTES: usize = 128 * 1024;

struct CachedResult {
    commit_gen: u64,
    params: Vec<Value>,
    result: QueryResult,
}

/// Single-slot, generation-keyed result memo (bounded by construction: one
/// entry per prepared statement, capped by `RESULT_CACHE_MAX_BYTES`).
pub(super) struct ResultCacheSlot {
    slot: parking_lot::RwLock<Option<CachedResult>>,
    /// Generation whose result exceeded the cap; skips the O(result) size
    /// walk on every re-execution until a commit changes the data.
    /// `u64::MAX` = none.
    rejected_gen: std::sync::atomic::AtomicU64,
}

impl ResultCacheSlot {
    pub(super) fn new() -> Self {
        Self {
            slot: parking_lot::RwLock::new(None),
            rejected_gen: std::sync::atomic::AtomicU64::new(u64::MAX),
        }
    }

    pub(super) fn lookup(&self, commit_gen: u64, params: &[Value]) -> Option<QueryResult> {
        let slot = self.slot.read();
        match &*slot {
            Some(c) if c.commit_gen == commit_gen && params_match(&c.params, params) => {
                Some(c.result.clone())
            }
            _ => None,
        }
    }

    pub(super) fn store(&self, commit_gen: u64, params: &[Value], result: &QueryResult) {
        use std::sync::atomic::Ordering;
        if self.rejected_gen.load(Ordering::Relaxed) == commit_gen {
            return;
        }
        if !within_cap(params, result) {
            self.rejected_gen.store(commit_gen, Ordering::Relaxed);
            return;
        }
        *self.slot.write() = Some(CachedResult {
            commit_gen,
            params: params.to_vec(),
            result: result.clone(),
        });
    }
}

/// Bit-exact param equality: `-0.0` vs `0.0` and NaN payloads must not be
/// conflated, or a hit could return the other representation's result.
fn params_match(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| value_bit_eq(x, y))
}

fn value_bit_eq(a: &Value, b: &Value) -> bool {
    if std::mem::discriminant(a) != std::mem::discriminant(b) {
        return false;
    }
    match (a, b) {
        (Value::Real(x), Value::Real(y)) => x.to_bits() == y.to_bits(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(v, w)| value_bit_eq(v, w))
        }
        (Value::Vector(x), Value::Vector(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|(v, w)| v.to_bits() == w.to_bits())
        }
        _ => a == b,
    }
}

fn within_cap(params: &[Value], result: &QueryResult) -> bool {
    // O(1) pre-reject: each row costs at least a Vec header + one Value.
    if result
        .rows
        .len()
        .checked_mul(56)
        .is_none_or(|bytes| bytes > RESULT_CACHE_MAX_BYTES)
    {
        return false;
    }
    let mut remaining = RESULT_CACHE_MAX_BYTES;
    for column in &result.columns {
        if !charge(&mut remaining, 24usize.saturating_add(column.len())) {
            return false;
        }
    }
    for v in params {
        if !charge(&mut remaining, 32) || !value_fits(v, &mut remaining) {
            return false;
        }
    }
    for row in &result.rows {
        let Some(row_bytes) = row.len().checked_mul(32).and_then(|n| n.checked_add(24)) else {
            return false;
        };
        if !charge(&mut remaining, row_bytes) {
            return false;
        }
        for v in row {
            if !value_fits(v, &mut remaining) {
                return false;
            }
        }
    }
    true
}

#[inline]
fn charge(remaining: &mut usize, bytes: usize) -> bool {
    if bytes > *remaining {
        false
    } else {
        *remaining -= bytes;
        true
    }
}

/// Charge a value's heap storage, stopping once the cache budget is exhausted. The
/// explicit stack avoids recursive sizing, and charging array slots before pushing
/// children bounds the walk.
fn value_fits(root: &Value, remaining: &mut usize) -> bool {
    let mut pending = vec![root];
    while let Some(value) = pending.pop() {
        let bytes = match value {
            Value::Text(s) | Value::Json(s) => {
                // CompactString stores up to 24 bytes inline.
                if s.len() > 24 {
                    s.len()
                } else {
                    0
                }
            }
            Value::Blob(b) => b.len(),
            Value::Jsonb(b) | Value::TsVector(b) | Value::TsQuery(b) => b.len(),
            Value::Array(values) => {
                let Some(bytes) = values.len().checked_mul(32) else {
                    return false;
                };
                if !charge(remaining, bytes) {
                    return false;
                }
                pending.extend(values.iter());
                continue;
            }
            Value::Vector(values) => {
                let Some(bytes) = values.len().checked_mul(4) else {
                    return false;
                };
                bytes
            }
            Value::Null
            | Value::Integer(_)
            | Value::Real(_)
            | Value::Boolean(_)
            | Value::Time(_)
            | Value::Date(_)
            | Value::Timestamp(_)
            | Value::Interval { .. } => 0,
        };
        if !charge(remaining, bytes) {
            return false;
        }
    }
    true
}

/// Deny-unless-known cacheability decision, made once at compile time. True
/// only when the statement is a pure read whose result is a function of
/// (statement, params, commit generation).
pub(super) fn is_result_cacheable(schema: &SchemaManager, sq: &SelectQuery) -> bool {
    let mut ctx = WalkCtx {
        schema,
        seen_views: FxHashSet::default(),
        cte_names: FxHashSet::default(),
    };
    cacheable_query(&mut ctx, sq)
}

struct WalkCtx<'a> {
    schema: &'a SchemaManager,
    seen_views: FxHashSet<String>,
    cte_names: FxHashSet<String>,
}

fn cacheable_query(ctx: &mut WalkCtx<'_>, sq: &SelectQuery) -> bool {
    for cte in &sq.ctes {
        if !cacheable_cte(ctx, cte) {
            return false;
        }
    }
    cacheable_body(ctx, &sq.body)
}

fn cacheable_cte(ctx: &mut WalkCtx<'_>, cte: &CteDefinition) -> bool {
    // Register the name first (recursive CTEs self-reference). Flat scoping
    // is over-broad only toward shadowing, where both resolutions are pure.
    ctx.cte_names.insert(cte.name.to_ascii_lowercase());
    cacheable_body(ctx, &cte.body)
}

fn cacheable_body(ctx: &mut WalkCtx<'_>, body: &QueryBody) -> bool {
    match body {
        QueryBody::Select(sel) => cacheable_select(ctx, sel),
        QueryBody::Compound(comp) => {
            cacheable_body(ctx, &comp.left)
                && cacheable_body(ctx, &comp.right)
                && comp.order_by.iter().all(|o| cacheable_expr(ctx, &o.expr))
                && comp
                    .limit
                    .as_ref()
                    .is_none_or_cacheable(ctx, cacheable_expr)
                && comp
                    .offset
                    .as_ref()
                    .is_none_or_cacheable(ctx, cacheable_expr)
        }
        // DML bodies (WITH ... INSERT/UPDATE/DELETE) mutate state.
        QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => false,
    }
}

fn cacheable_select(ctx: &mut WalkCtx<'_>, sel: &SelectStmt) -> bool {
    // Table-valued FROM forms stay uncached until each function is classified.
    if sel.from_args.is_some() || sel.from_json_table.is_some() {
        return false;
    }
    if let Some(sub) = &sel.from_subquery {
        if !cacheable_query(ctx, &sub.query) {
            return false;
        }
    } else if !sel.from.is_empty() && !cacheable_table_ref(ctx, &sel.from) {
        return false;
    }
    for join in &sel.joins {
        if let Some(sub) = &join.subquery {
            if !cacheable_query(ctx, &sub.query) {
                return false;
            }
        } else if !cacheable_table_ref(ctx, &join.table.name) {
            return false;
        }
        if let Some(on) = &join.on_clause {
            if !cacheable_expr(ctx, on) {
                return false;
            }
        }
    }

    sel.columns.iter().all(|c| match c {
        SelectColumn::Expr { expr, .. } => cacheable_expr(ctx, expr),
        SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => true,
    }) && sel
        .where_clause
        .as_ref()
        .is_none_or_cacheable(ctx, cacheable_expr)
        && sel.group_by.iter().all(|e| cacheable_expr(ctx, e))
        && sel
            .having
            .as_ref()
            .is_none_or_cacheable(ctx, cacheable_expr)
        && sel.order_by.iter().all(|o| cacheable_expr(ctx, &o.expr))
        && sel.limit.as_ref().is_none_or_cacheable(ctx, cacheable_expr)
        && sel
            .offset
            .as_ref()
            .is_none_or_cacheable(ctx, cacheable_expr)
}

/// `Option::is_none_or` with the walker context threaded through.
trait OptionCacheable<T> {
    fn is_none_or_cacheable(
        &self,
        ctx: &mut WalkCtx<'_>,
        f: fn(&mut WalkCtx<'_>, &T) -> bool,
    ) -> bool;
}

impl<T> OptionCacheable<T> for Option<&T> {
    fn is_none_or_cacheable(
        &self,
        ctx: &mut WalkCtx<'_>,
        f: fn(&mut WalkCtx<'_>, &T) -> bool,
    ) -> bool {
        match self {
            Some(v) => f(ctx, v),
            None => true,
        }
    }
}

fn cacheable_table_ref(ctx: &mut WalkCtx<'_>, name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    if ctx.cte_names.contains(&lower) {
        return true;
    }
    // Virtual tables read wall clock / OS tzdb state.
    if ctx.schema.get_virtual(&lower).is_some() {
        return false;
    }
    if let Some(ts) = ctx.schema.get(&lower) {
        // A volatile generated expr (possible in on-disk schemas predating
        // CREATE-time validation) evaluates on read and breaks purity.
        return ts.columns.iter().all(|c| match &c.generated_expr {
            Some(expr) => cacheable_expr(ctx, expr),
            None => true,
        });
    }
    if let Some(view) = ctx.schema.get_view(&lower) {
        if !ctx.seen_views.insert(lower) {
            // Already validated (or a definition cycle; be conservative and
            // let execution surface the error uncached).
            return true;
        }
        return match crate::parser::parse_sql(&view.sql) {
            Ok(Statement::Select(view_sq)) => cacheable_query(ctx, &view_sq),
            _ => false,
        };
    }
    if ctx.schema.get_matview(&lower).is_some() {
        // Matview reads hit its materialized backing table; refreshes commit
        // and bump the generation.
        return true;
    }
    // Unknown source: let execution fail/behave without the cache.
    false
}

fn cacheable_window_bound(ctx: &mut WalkCtx<'_>, bound: &WindowFrameBound) -> bool {
    match bound {
        WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
            cacheable_expr(ctx, expr)
        }
        WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedFollowing => true,
    }
}

fn cacheable_expr(ctx: &mut WalkCtx<'_>, expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. }
        | Expr::Parameter(_)
        | Expr::CountStar
        | Expr::TypedNullRecord(_) => true,
        Expr::Function { name, args, .. } => {
            let upper = name.to_ascii_uppercase();
            if crate::eval::is_volatile_function(&upper, args.len()) {
                return false;
            }
            if crate::eval::is_session_dependent_jsonpath_function(&upper, args) {
                return false;
            }
            // Stricter than the shared check: 'now' can also arrive from
            // column data at runtime; only literal first args are provably safe.
            if matches!(upper.as_str(), "DATE" | "TIME" | "DATETIME") {
                match args.first() {
                    Some(Expr::Literal(Value::Text(s))) => {
                        if s.trim().eq_ignore_ascii_case("now") {
                            return false;
                        }
                    }
                    Some(Expr::Literal(_)) => {}
                    _ => return false,
                }
            }
            args.iter().all(|a| cacheable_expr(ctx, a))
        }
        Expr::BinaryOp { left, op, right } => {
            // ANN index construction uses RNG; distance results can differ
            // across otherwise-identical executions.
            if matches!(
                op,
                BinOp::VectorL2 | BinOp::VectorInner | BinOp::VectorCosine
            ) || crate::eval::is_session_dependent_jsonpath_op(op, left, right)
            {
                return false;
            }
            cacheable_expr(ctx, left) && cacheable_expr(ctx, right)
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::Collate { expr, .. } => {
            cacheable_expr(ctx, expr)
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => cacheable_expr(ctx, e),
        Expr::InSubquery { expr, subquery, .. } => {
            cacheable_expr(ctx, expr) && cacheable_select(ctx, subquery)
        }
        Expr::Exists { subquery, .. } => cacheable_select(ctx, subquery),
        Expr::ScalarSubquery(subquery) => cacheable_select(ctx, subquery),
        Expr::InList { expr, list, .. } => {
            cacheable_expr(ctx, expr) && list.iter().all(|e| cacheable_expr(ctx, e))
        }
        Expr::InSet { expr, .. } => cacheable_expr(ctx, expr),
        Expr::Between {
            expr, low, high, ..
        } => cacheable_expr(ctx, expr) && cacheable_expr(ctx, low) && cacheable_expr(ctx, high),
        Expr::IsDistinctFrom { left, right, .. } => {
            cacheable_expr(ctx, left) && cacheable_expr(ctx, right)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            cacheable_expr(ctx, expr)
                && cacheable_expr(ctx, pattern)
                && match escape {
                    Some(e) => cacheable_expr(ctx, e),
                    None => true,
                }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            (match operand {
                Some(o) => cacheable_expr(ctx, o),
                None => true,
            }) && conditions
                .iter()
                .all(|(c, r)| cacheable_expr(ctx, c) && cacheable_expr(ctx, r))
                && (match else_result {
                    Some(e) => cacheable_expr(ctx, e),
                    None => true,
                })
        }
        Expr::Coalesce(items) | Expr::ArrayLiteral(items) => {
            items.iter().all(|e| cacheable_expr(ctx, e))
        }
        Expr::WindowFunction { name, args, spec } => {
            !crate::eval::is_volatile_function(&name.to_ascii_uppercase(), args.len())
                && args.iter().all(|a| cacheable_expr(ctx, a))
                && spec
                    .partition_by
                    .iter()
                    .all(|expr| cacheable_expr(ctx, expr))
                && spec
                    .order_by
                    .iter()
                    .all(|item| cacheable_expr(ctx, &item.expr))
                && spec.frame.as_ref().is_none_or(|frame| {
                    cacheable_window_bound(ctx, &frame.start)
                        && cacheable_window_bound(ctx, &frame.end)
                })
        }
        Expr::Quantified { left, right, .. } => {
            cacheable_expr(ctx, left)
                && match right {
                    crate::parser::QuantifiedRhs::Subquery(sub) => cacheable_select(ctx, sub),
                    crate::parser::QuantifiedRhs::Array(e) => cacheable_expr(ctx, e),
                }
        }
    }
}

#[cfg(test)]
#[path = "result_cache_tests.rs"]
mod tests;
