//! Expression evaluator with SQL three-valued logic.

use rustc_hash::FxHashMap;

use crate::error::{Result, SqlError};
use crate::parser::{BinOp, Expr, UnaryOp};
use crate::types::{ColumnDef, CompactString, DataType, Value};

pub struct ColumnMap {
    exact: FxHashMap<String, usize>,
    short: FxHashMap<String, ShortMatch>,
    collations: Vec<crate::types::Collation>,
    has_non_binary_collation: bool,
}

#[derive(Clone)]
enum ShortMatch {
    Unique(usize),
    Ambiguous,
}

impl Clone for ColumnMap {
    fn clone(&self) -> Self {
        Self {
            exact: self.exact.clone(),
            short: self.short.clone(),
            collations: self.collations.clone(),
            has_non_binary_collation: self.has_non_binary_collation,
        }
    }
}

impl ColumnMap {
    pub fn new(columns: &[ColumnDef]) -> Self {
        let mut exact = FxHashMap::with_capacity_and_hasher(columns.len() * 2, Default::default());
        let mut short: FxHashMap<String, ShortMatch> =
            FxHashMap::with_capacity_and_hasher(columns.len(), Default::default());
        let mut collations = Vec::with_capacity(columns.len());
        let mut has_non_binary_collation = false;

        for (i, col) in columns.iter().enumerate() {
            let lower = col.name.to_ascii_lowercase();
            exact.insert(lower.clone(), i);

            let unqualified = if let Some(dot) = lower.rfind('.') {
                &lower[dot + 1..]
            } else {
                &lower
            };
            short
                .entry(unqualified.to_string())
                .and_modify(|e| *e = ShortMatch::Ambiguous)
                .or_insert(ShortMatch::Unique(i));
            collations.push(col.collation);
            if col.collation != crate::types::Collation::Binary {
                has_non_binary_collation = true;
            }
        }

        Self {
            exact,
            short,
            collations,
            has_non_binary_collation,
        }
    }

    pub(crate) fn collation_at(&self, idx: usize) -> crate::types::Collation {
        self.collations
            .get(idx)
            .copied()
            .unwrap_or(crate::types::Collation::Binary)
    }

    #[inline]
    pub(crate) fn has_non_binary_collation(&self) -> bool {
        self.has_non_binary_collation
    }

    pub(crate) fn resolve(&self, name: &str) -> Result<usize> {
        if let Some(&idx) = self.exact.get(name) {
            return Ok(idx);
        }
        match self.short.get(name) {
            Some(ShortMatch::Unique(idx)) => Ok(*idx),
            Some(ShortMatch::Ambiguous) => Err(SqlError::AmbiguousColumn(name.to_string())),
            None => Err(SqlError::ColumnNotFound(name.to_string())),
        }
    }

    pub(crate) fn resolve_qualified(&self, table: &str, column: &str) -> Result<usize> {
        let qualified = format!("{table}.{column}");
        if let Some(&idx) = self.exact.get(&qualified) {
            return Ok(idx);
        }
        match self.short.get(column) {
            Some(ShortMatch::Unique(idx)) => Ok(*idx),
            _ => Err(SqlError::ColumnNotFound(format!("{table}.{column}"))),
        }
    }
}

pub struct EvalCtx<'a> {
    pub col_map: &'a ColumnMap,
    pub row: &'a [Value],
    pub params: &'a [Value],
    pub excluded: Option<ExcludedRow<'a>>,
    pub old_new: Option<OldNewRows<'a>>,
}

pub struct ExcludedRow<'a> {
    pub col_map: &'a ColumnMap,
    pub row: &'a [Value],
}

pub struct OldNewRows<'a> {
    pub col_map: &'a ColumnMap,
    pub old_row: Option<&'a [Value]>,
    pub new_row: Option<&'a [Value]>,
}

impl<'a> EvalCtx<'a> {
    pub fn new(col_map: &'a ColumnMap, row: &'a [Value]) -> Self {
        Self {
            col_map,
            row,
            params: &[],
            excluded: None,
            old_new: None,
        }
    }

    pub fn with_params(col_map: &'a ColumnMap, row: &'a [Value], params: &'a [Value]) -> Self {
        Self {
            col_map,
            row,
            params,
            excluded: None,
            old_new: None,
        }
    }

    pub fn with_excluded(
        col_map: &'a ColumnMap,
        row: &'a [Value],
        excluded_col_map: &'a ColumnMap,
        excluded_row: &'a [Value],
    ) -> Self {
        Self {
            col_map,
            row,
            params: &[],
            excluded: Some(ExcludedRow {
                col_map: excluded_col_map,
                row: excluded_row,
            }),
            old_new: None,
        }
    }

    pub fn with_old_new(
        col_map: &'a ColumnMap,
        row: &'a [Value],
        old_row: Option<&'a [Value]>,
        new_row: Option<&'a [Value]>,
    ) -> Self {
        Self {
            col_map,
            row,
            params: &[],
            excluded: None,
            old_new: Some(OldNewRows {
                col_map,
                old_row,
                new_row,
            }),
        }
    }
}

thread_local! {
    static SCOPED_PARAMS: std::cell::Cell<(*const Value, usize)> =
        const { std::cell::Cell::new((std::ptr::null(), 0)) };
}

/// Install positional parameters for `Expr::Parameter` resolution during `f`.
pub fn with_scoped_params<R>(params: &[Value], f: impl FnOnce() -> R) -> R {
    struct Guard((*const Value, usize));
    impl Drop for Guard {
        fn drop(&mut self) {
            SCOPED_PARAMS.with(|slot| slot.set(self.0));
        }
    }
    SCOPED_PARAMS.with(|slot| {
        let prev = slot.get();
        slot.set((params.as_ptr(), params.len()));
        let _guard = Guard(prev);
        f()
    })
}

fn resolve_parameter(n: usize, ctx_params: &[Value]) -> Result<Value> {
    if !ctx_params.is_empty() {
        if n == 0 || n > ctx_params.len() {
            return Err(SqlError::ParameterCountMismatch {
                expected: n,
                got: ctx_params.len(),
            });
        }
        return Ok(ctx_params[n - 1].clone());
    }
    resolve_scoped_param(n)
}

pub fn resolve_scoped_param(n: usize) -> Result<Value> {
    SCOPED_PARAMS.with(|slot| {
        let (ptr, len) = slot.get();
        if n == 0 || n > len {
            return Err(SqlError::ParameterCountMismatch {
                expected: n,
                got: len,
            });
        }
        // SAFETY: `with_scoped_params` keeps the slice alive for the duration of `f()`
        // and restores the previous pointer on return. Reads only happen inside `f()`.
        unsafe { Ok((*ptr.add(n - 1)).clone()) }
    })
}

pub fn eval_expr(expr: &Expr, ctx: &EvalCtx) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),

        Expr::Column(name) => {
            let idx = ctx.col_map.resolve(name)?;
            Ok(ctx.row[idx].clone())
        }

        Expr::QualifiedColumn { table, column } => {
            if let Some(excluded) = ctx.excluded.as_ref() {
                if table.eq_ignore_ascii_case("excluded") {
                    let lowered = column.to_ascii_lowercase();
                    let idx = excluded.col_map.resolve(&lowered)?;
                    return Ok(excluded.row[idx].clone());
                }
            }
            if let Some(on) = ctx.old_new.as_ref() {
                if table.eq_ignore_ascii_case("old") {
                    let lowered = column.to_ascii_lowercase();
                    let idx = on.col_map.resolve(&lowered)?;
                    return Ok(on.old_row.map(|r| r[idx].clone()).unwrap_or(Value::Null));
                }
                if table.eq_ignore_ascii_case("new") {
                    let lowered = column.to_ascii_lowercase();
                    let idx = on.col_map.resolve(&lowered)?;
                    return Ok(on.new_row.map(|r| r[idx].clone()).unwrap_or(Value::Null));
                }
            }
            let idx = ctx.col_map.resolve_qualified(table, column)?;
            Ok(ctx.row[idx].clone())
        }

        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, ctx)?;
            let rval = eval_expr(right, ctx)?;
            let needs_collation_check = ctx.col_map.has_non_binary_collation()
                || matches!(left.as_ref(), Expr::Collate { .. })
                || matches!(right.as_ref(), Expr::Collate { .. });
            if needs_collation_check {
                let coll = collation_of(left)
                    .or_else(|| collation_of(right))
                    .or_else(|| {
                        column_collation(left, ctx).or_else(|| column_collation(right, ctx))
                    });
                if let Some(c) = coll {
                    if c != crate::types::Collation::Binary {
                        if let Some(b) = eval_text_compare(&lval, *op, &rval, c) {
                            return Ok(Value::Boolean(b));
                        }
                    }
                }
            }
            eval_binary_op(&lval, *op, &rval)
        }

        Expr::UnaryOp { op, expr } => {
            let val = eval_expr(expr, ctx)?;
            eval_unary_op(*op, &val)
        }

        Expr::IsNull(e) => {
            let val = eval_expr(e, ctx)?;
            Ok(Value::Boolean(val.is_null()))
        }

        Expr::IsNotNull(e) => {
            let val = eval_expr(e, ctx)?;
            Ok(Value::Boolean(!val.is_null()))
        }

        Expr::Function { name, args, .. } => eval_scalar_function(name, args, ctx),

        Expr::CountStar => Err(SqlError::Unsupported(
            "COUNT(*) in non-aggregate context".into(),
        )),

        Expr::InList {
            expr: e,
            list,
            negated,
        } => {
            let lhs = eval_expr(e, ctx)?;
            eval_in_values(&lhs, list, ctx, *negated)
        }

        Expr::InSet {
            expr: e,
            values,
            has_null,
            negated,
        } => {
            let lhs = eval_expr(e, ctx)?;
            eval_in_set(&lhs, values, *has_null, *negated)
        }

        Expr::Between {
            expr: e,
            low,
            high,
            negated,
        } => {
            let val = eval_expr(e, ctx)?;
            let lo = eval_expr(low, ctx)?;
            let hi = eval_expr(high, ctx)?;
            eval_between(&val, &lo, &hi, *negated)
        }

        Expr::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => {
            let val = eval_expr(e, ctx)?;
            let pat = eval_expr(pattern, ctx)?;
            let esc = escape.as_ref().map(|e| eval_expr(e, ctx)).transpose()?;
            eval_like(&val, &pat, esc.as_ref(), *negated)
        }

        Expr::Case {
            operand,
            conditions,
            else_result,
        } => eval_case(operand.as_deref(), conditions, else_result.as_deref(), ctx),

        Expr::Coalesce(args) => {
            for arg in args {
                let val = eval_expr(arg, ctx)?;
                if !val.is_null() {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }

        Expr::Cast { expr: e, data_type } => {
            let val = eval_expr(e, ctx)?;
            eval_cast(&val, *data_type)
        }

        Expr::Collate { expr: e, .. } => eval_expr(e, ctx),

        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::ScalarSubquery(_) => Err(
            SqlError::Unsupported("subquery not materialized (internal error)".into()),
        ),

        Expr::Parameter(n) => resolve_parameter(*n, ctx.params),

        Expr::WindowFunction { .. } => Err(SqlError::Unsupported(
            "window functions are only allowed in SELECT columns".into(),
        )),
    }
}

/// Planner-level constant folding hook; shares semantics with row evaluation.
fn collation_of(expr: &Expr) -> Option<crate::types::Collation> {
    match expr {
        Expr::Collate { collation, .. } => Some(*collation),
        _ => None,
    }
}

fn column_collation(expr: &Expr, ctx: &EvalCtx<'_>) -> Option<crate::types::Collation> {
    match expr {
        Expr::Column(name) => ctx
            .col_map
            .resolve(name)
            .ok()
            .map(|i| ctx.col_map.collation_at(i)),
        Expr::QualifiedColumn { table, column } => ctx
            .col_map
            .resolve_qualified(table, column)
            .ok()
            .map(|i| ctx.col_map.collation_at(i)),
        _ => None,
    }
}

fn eval_text_compare(
    left: &Value,
    op: BinOp,
    right: &Value,
    coll: crate::types::Collation,
) -> Option<bool> {
    let (a, b) = match (left, right) {
        (Value::Null, _) | (_, Value::Null) => return None,
        (Value::Text(a), Value::Text(b)) => (a.as_str(), b.as_str()),
        _ => return None,
    };
    let ord = coll.cmp_text(a, b);
    Some(match op {
        BinOp::Eq => ord == std::cmp::Ordering::Equal,
        BinOp::NotEq => ord != std::cmp::Ordering::Equal,
        BinOp::Lt => ord == std::cmp::Ordering::Less,
        BinOp::Gt => ord == std::cmp::Ordering::Greater,
        BinOp::LtEq => ord != std::cmp::Ordering::Greater,
        BinOp::GtEq => ord != std::cmp::Ordering::Less,
        _ => return None,
    })
}

pub fn eval_binary_op_public(left: &Value, op: BinOp, right: &Value) -> Result<Value> {
    eval_binary_op(left, op, right)
}

fn eval_binary_op(left: &Value, op: BinOp, right: &Value) -> Result<Value> {
    match op {
        BinOp::And => return eval_and(left, right),
        BinOp::Or => return eval_or(left, right),
        _ => {}
    }

    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    if let Some(res) = eval_temporal_op(left, op, right) {
        return res;
    }

    match op {
        BinOp::Eq => Ok(Value::Boolean(left == right)),
        BinOp::NotEq => Ok(Value::Boolean(left != right)),
        BinOp::Lt => Ok(Value::Boolean(left < right)),
        BinOp::Gt => Ok(Value::Boolean(left > right)),
        BinOp::LtEq => Ok(Value::Boolean(left <= right)),
        BinOp::GtEq => Ok(Value::Boolean(left >= right)),
        BinOp::Add => eval_arithmetic(left, right, i64::checked_add, |a, b| a + b),
        BinOp::Sub => eval_arithmetic(left, right, i64::checked_sub, |a, b| a - b),
        BinOp::Mul => eval_arithmetic(left, right, i64::checked_mul, |a, b| a * b),
        BinOp::Div => {
            match right {
                Value::Integer(0) => return Err(SqlError::DivisionByZero),
                Value::Real(r) if *r == 0.0 => return Err(SqlError::DivisionByZero),
                _ => {}
            }
            eval_arithmetic(left, right, i64::checked_div, |a, b| a / b)
        }
        BinOp::Mod => {
            match right {
                Value::Integer(0) => return Err(SqlError::DivisionByZero),
                Value::Real(r) if *r == 0.0 => return Err(SqlError::DivisionByZero),
                _ => {}
            }
            eval_arithmetic(left, right, i64::checked_rem, |a, b| a % b)
        }
        BinOp::Concat => {
            let ls = value_to_text(left);
            let rs = value_to_text(right);
            Ok(Value::Text(format!("{ls}{rs}").into()))
        }
        BinOp::And | BinOp::Or => unreachable!(),
    }
}

/// Returns `Some` when `(left, op, right)` is a temporal operation; `None` to fall through.
fn eval_temporal_op(left: &Value, op: BinOp, right: &Value) -> Option<Result<Value>> {
    use crate::datetime as dt;
    use std::cmp::Ordering;

    let is_temporal = |v: &Value| {
        matches!(
            v,
            Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::Interval { .. }
        )
    };
    if matches!(op, BinOp::Add | BinOp::Sub)
        && ((is_temporal(left) && matches!(right, Value::Real(_)))
            || (matches!(left, Value::Real(_)) && is_temporal(right)))
    {
        return Some(Err(SqlError::TypeMismatch {
            expected: "INTEGER or INTERVAL for date/time arithmetic (use CAST for REAL)".into(),
            got: format!("{} and {}", left.data_type(), right.data_type()),
        }));
    }

    match (left, op, right) {
        (Value::Date(d), BinOp::Add, Value::Integer(n))
        | (Value::Integer(n), BinOp::Add, Value::Date(d)) => {
            Some(dt::add_days_to_date(*d, *n).map(Value::Date))
        }
        (Value::Date(d), BinOp::Sub, Value::Integer(n)) => {
            Some(dt::add_days_to_date(*d, -*n).map(Value::Date))
        }
        (Value::Date(a), BinOp::Sub, Value::Date(b)) => {
            Some(Ok(Value::Integer(*a as i64 - *b as i64)))
        }
        // DATE ± INTERVAL → TIMESTAMP (PG rule).
        (
            Value::Date(d),
            BinOp::Add,
            Value::Interval {
                months,
                days,
                micros,
            },
        )
        | (
            Value::Interval {
                months,
                days,
                micros,
            },
            BinOp::Add,
            Value::Date(d),
        ) => Some(dt::add_interval_to_date(*d, *months, *days, *micros).map(Value::Timestamp)),
        (
            Value::Date(d),
            BinOp::Sub,
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Some(dt::add_interval_to_date(*d, -*months, -*days, -*micros).map(Value::Timestamp)),
        (
            Value::Timestamp(t),
            BinOp::Add,
            Value::Interval {
                months,
                days,
                micros,
            },
        )
        | (
            Value::Interval {
                months,
                days,
                micros,
            },
            BinOp::Add,
            Value::Timestamp(t),
        ) => Some(dt::add_interval_to_timestamp(*t, *months, *days, *micros).map(Value::Timestamp)),
        (
            Value::Timestamp(t),
            BinOp::Sub,
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Some(
            dt::add_interval_to_timestamp(*t, -*months, -*days, -*micros).map(Value::Timestamp),
        ),
        (Value::Timestamp(a), BinOp::Sub, Value::Timestamp(b)) => {
            let (days, micros) = dt::subtract_timestamps(*a, *b);
            Some(Ok(Value::Interval {
                months: 0,
                days,
                micros,
            }))
        }
        (
            Value::Time(t),
            BinOp::Add,
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Some(dt::add_interval_to_time(*t, *months, *days, *micros).map(Value::Time)),
        (
            Value::Time(t),
            BinOp::Sub,
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Some(dt::add_interval_to_time(*t, -*months, -*days, -*micros).map(Value::Time)),
        (Value::Time(a), BinOp::Sub, Value::Time(b)) => Some(Ok(Value::Interval {
            months: 0,
            days: 0,
            micros: *a - *b,
        })),
        (
            Value::Interval {
                months: am,
                days: ad,
                micros: au,
            },
            BinOp::Add,
            Value::Interval {
                months: bm,
                days: bd,
                micros: bu,
            },
        ) => Some(Ok(Value::Interval {
            months: am.saturating_add(*bm),
            days: ad.saturating_add(*bd),
            micros: au.saturating_add(*bu),
        })),
        (
            Value::Interval {
                months: am,
                days: ad,
                micros: au,
            },
            BinOp::Sub,
            Value::Interval {
                months: bm,
                days: bd,
                micros: bu,
            },
        ) => Some(Ok(Value::Interval {
            months: am.saturating_sub(*bm),
            days: ad.saturating_sub(*bd),
            micros: au.saturating_sub(*bu),
        })),
        (
            Value::Interval {
                months,
                days,
                micros,
            },
            BinOp::Mul,
            Value::Integer(n),
        )
        | (
            Value::Integer(n),
            BinOp::Mul,
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => {
            let n32 = (*n).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
            Some(Ok(Value::Interval {
                months: months.saturating_mul(n32),
                days: days.saturating_mul(n32),
                micros: micros.saturating_mul(*n),
            }))
        }
        // INTERVAL * REAL — fractional months → days, fractional days → micros (PG).
        (
            Value::Interval {
                months,
                days,
                micros,
            },
            BinOp::Mul,
            Value::Real(r),
        )
        | (
            Value::Real(r),
            BinOp::Mul,
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Some(Ok(scale_interval_by_real(*months, *days, *micros, *r))),
        (
            Value::Interval {
                months,
                days,
                micros,
            },
            BinOp::Div,
            Value::Integer(n),
        ) if *n != 0 => Some(Ok(Value::Interval {
            months: (*months as i64 / *n) as i32,
            days: (*days as i64 / *n) as i32,
            micros: *micros / *n,
        })),
        (
            Value::Interval {
                months,
                days,
                micros,
            },
            BinOp::Div,
            Value::Real(r),
        ) if *r != 0.0 => Some(Ok(scale_interval_by_real(*months, *days, *micros, 1.0 / r))),
        // PG-normalized INTERVAL compare: 30-day month, 24-hour day.
        (
            Value::Interval {
                months: am,
                days: ad,
                micros: au,
            },
            op,
            Value::Interval {
                months: bm,
                days: bd,
                micros: bu,
            },
        ) if matches!(
            op,
            BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::Gt | BinOp::LtEq | BinOp::GtEq
        ) =>
        {
            let ord = dt::pg_normalized_interval_cmp((*am, *ad, *au), (*bm, *bd, *bu));
            let b = match op {
                BinOp::Eq => ord == Ordering::Equal,
                BinOp::NotEq => ord != Ordering::Equal,
                BinOp::Lt => ord == Ordering::Less,
                BinOp::Gt => ord == Ordering::Greater,
                BinOp::LtEq => ord != Ordering::Greater,
                BinOp::GtEq => ord != Ordering::Less,
                _ => unreachable!(),
            };
            Some(Ok(Value::Boolean(b)))
        }
        // PG rejects TIMESTAMP ± INTEGER; require CAST to INTERVAL.
        (Value::Timestamp(_), BinOp::Add | BinOp::Sub, Value::Integer(_))
        | (Value::Integer(_), BinOp::Add, Value::Timestamp(_)) => {
            Some(Err(SqlError::TypeMismatch {
                expected: "INTERVAL (use CAST or explicit unit)".into(),
                got: format!("{} and {}", left.data_type(), right.data_type()),
            }))
        }
        _ => None,
    }
}

/// PG fractional-propagation: month frac → days (×30), day frac → micros (×86.4G).
fn scale_interval_by_real(months: i32, days: i32, micros: i64, factor: f64) -> Value {
    let raw_months = months as f64 * factor;
    let whole_months = raw_months.trunc() as i64;
    let frac_months = raw_months - whole_months as f64;
    let months_frac_as_days = frac_months * 30.0;

    let raw_days = days as f64 * factor + months_frac_as_days;
    let whole_days = raw_days.trunc() as i64;
    let frac_days = raw_days - whole_days as f64;
    let days_frac_as_micros = (frac_days * crate::datetime::MICROS_PER_DAY as f64).round() as i64;

    let raw_micros = (micros as f64 * factor).round() as i64;
    let total_micros = raw_micros.saturating_add(days_frac_as_micros);

    let clamp_i32 = |n: i64| n.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
    Value::Interval {
        months: clamp_i32(whole_months),
        days: clamp_i32(whole_days),
        micros: total_micros,
    }
}

/// SQL three-valued AND: NULL AND false = false, NULL AND true = NULL
fn eval_and(left: &Value, right: &Value) -> Result<Value> {
    let l = to_bool_or_null(left)?;
    let r = to_bool_or_null(right)?;
    match (l, r) {
        (Some(false), _) | (_, Some(false)) => Ok(Value::Boolean(false)),
        (Some(true), Some(true)) => Ok(Value::Boolean(true)),
        _ => Ok(Value::Null),
    }
}

/// SQL three-valued OR: NULL OR true = true, NULL OR false = NULL
fn eval_or(left: &Value, right: &Value) -> Result<Value> {
    let l = to_bool_or_null(left)?;
    let r = to_bool_or_null(right)?;
    match (l, r) {
        (Some(true), _) | (_, Some(true)) => Ok(Value::Boolean(true)),
        (Some(false), Some(false)) => Ok(Value::Boolean(false)),
        _ => Ok(Value::Null),
    }
}

fn to_bool_or_null(val: &Value) -> Result<Option<bool>> {
    match val {
        Value::Boolean(b) => Ok(Some(*b)),
        Value::Null => Ok(None),
        Value::Integer(i) => Ok(Some(*i != 0)),
        _ => Err(SqlError::TypeMismatch {
            expected: "BOOLEAN".into(),
            got: format!("{}", val.data_type()),
        }),
    }
}

fn eval_arithmetic(
    left: &Value,
    right: &Value,
    int_op: fn(i64, i64) -> Option<i64>,
    real_op: fn(f64, f64) -> f64,
) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => int_op(*a, *b)
            .map(Value::Integer)
            .ok_or(SqlError::IntegerOverflow),
        (Value::Real(a), Value::Real(b)) => Ok(Value::Real(real_op(*a, *b))),
        (Value::Integer(a), Value::Real(b)) => Ok(Value::Real(real_op(*a as f64, *b))),
        (Value::Real(a), Value::Integer(b)) => Ok(Value::Real(real_op(*a, *b as f64))),
        _ => Err(SqlError::TypeMismatch {
            expected: "numeric".into(),
            got: format!("{} and {}", left.data_type(), right.data_type()),
        }),
    }
}

fn eval_in_values(lhs: &Value, list: &[Expr], ctx: &EvalCtx, negated: bool) -> Result<Value> {
    if list.is_empty() {
        return Ok(Value::Boolean(negated));
    }
    if lhs.is_null() {
        return Ok(Value::Null);
    }
    let mut has_null = false;
    for item in list {
        let rhs = eval_expr(item, ctx)?;
        if rhs.is_null() {
            has_null = true;
        } else if lhs == &rhs {
            return Ok(Value::Boolean(!negated));
        }
    }
    if has_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Boolean(negated))
    }
}

fn eval_in_set(
    lhs: &Value,
    values: &rustc_hash::FxHashSet<Value>,
    has_null: bool,
    negated: bool,
) -> Result<Value> {
    if values.is_empty() && !has_null {
        return Ok(Value::Boolean(negated));
    }
    if lhs.is_null() {
        return Ok(Value::Null);
    }
    if values.contains(lhs) {
        return Ok(Value::Boolean(!negated));
    }
    if has_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Boolean(negated))
    }
}

fn eval_unary_op(op: UnaryOp, val: &Value) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }
    match op {
        UnaryOp::Neg => match val {
            Value::Integer(i) => i
                .checked_neg()
                .map(Value::Integer)
                .ok_or(SqlError::IntegerOverflow),
            Value::Real(r) => Ok(Value::Real(-r)),
            Value::Interval {
                months,
                days,
                micros,
            } => {
                let m = months.checked_neg().ok_or(SqlError::IntegerOverflow)?;
                let d = days.checked_neg().ok_or(SqlError::IntegerOverflow)?;
                let u = micros.checked_neg().ok_or(SqlError::IntegerOverflow)?;
                Ok(Value::Interval {
                    months: m,
                    days: d,
                    micros: u,
                })
            }
            _ => Err(SqlError::TypeMismatch {
                expected: "numeric or INTERVAL".into(),
                got: format!("{}", val.data_type()),
            }),
        },
        UnaryOp::Not => match val {
            Value::Boolean(b) => Ok(Value::Boolean(!b)),
            Value::Integer(i) => Ok(Value::Boolean(*i == 0)),
            _ => Err(SqlError::TypeMismatch {
                expected: "BOOLEAN".into(),
                got: format!("{}", val.data_type()),
            }),
        },
    }
}

fn value_to_text(val: &Value) -> String {
    match val {
        Value::Text(s) => s.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => {
            if r.fract() == 0.0 && r.is_finite() {
                format!("{r:.1}")
            } else {
                format!("{r}")
            }
        }
        Value::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        Value::Null => String::new(),
        Value::Blob(b) => {
            let mut s = String::with_capacity(b.len() * 2);
            for byte in b {
                s.push_str(&format!("{byte:02X}"));
            }
            s
        }
        Value::Date(d) => crate::datetime::format_date(*d),
        Value::Time(t) => crate::datetime::format_time(*t),
        Value::Timestamp(t) => crate::datetime::format_timestamp(*t),
        Value::Interval {
            months,
            days,
            micros,
        } => crate::datetime::format_interval(*months, *days, *micros),
    }
}

fn eval_between(val: &Value, low: &Value, high: &Value, negated: bool) -> Result<Value> {
    if val.is_null() || low.is_null() || high.is_null() {
        let ge = if val.is_null() || low.is_null() {
            None
        } else {
            Some(*val >= *low)
        };
        let le = if val.is_null() || high.is_null() {
            None
        } else {
            Some(*val <= *high)
        };

        let result = match (ge, le) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        };

        return match result {
            Some(b) => Ok(Value::Boolean(if negated { !b } else { b })),
            None => Ok(Value::Null),
        };
    }

    let in_range = *val >= *low && *val <= *high;
    Ok(Value::Boolean(if negated { !in_range } else { in_range }))
}

const MAX_LIKE_PATTERN_LEN: usize = 10_000;

fn eval_like(val: &Value, pattern: &Value, escape: Option<&Value>, negated: bool) -> Result<Value> {
    if val.is_null() || pattern.is_null() {
        return Ok(Value::Null);
    }
    let text = match val {
        Value::Text(s) => s.as_str(),
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT".into(),
                got: val.data_type().to_string(),
            })
        }
    };
    let pat = match pattern {
        Value::Text(s) => s.as_str(),
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT".into(),
                got: pattern.data_type().to_string(),
            })
        }
    };

    if pat.len() > MAX_LIKE_PATTERN_LEN {
        return Err(SqlError::InvalidValue(format!(
            "LIKE pattern too long ({} chars, max {MAX_LIKE_PATTERN_LEN})",
            pat.len()
        )));
    }

    let esc_char = match escape {
        Some(Value::Text(s)) => {
            let mut chars = s.chars();
            let c = chars.next().ok_or_else(|| {
                SqlError::InvalidValue("ESCAPE must be a single character".into())
            })?;
            if chars.next().is_some() {
                return Err(SqlError::InvalidValue(
                    "ESCAPE must be a single character".into(),
                ));
            }
            Some(c)
        }
        Some(Value::Null) => return Ok(Value::Null),
        Some(_) => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT".into(),
                got: "non-text".into(),
            })
        }
        None => None,
    };

    let matched = like_match(text, pat, esc_char);
    Ok(Value::Boolean(if negated { !matched } else { matched }))
}

fn like_match(text: &str, pattern: &str, escape: Option<char>) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_match_impl(&t, &p, 0, 0, escape)
}

fn like_match_impl(
    t: &[char],
    p: &[char],
    mut ti: usize,
    mut pi: usize,
    esc: Option<char>,
) -> bool {
    let mut star_pi: Option<usize> = None;
    let mut star_ti: usize = 0;

    while ti < t.len() {
        if pi < p.len() {
            if let Some(ec) = esc {
                if p[pi] == ec && pi + 1 < p.len() {
                    pi += 1;
                    let pc_lower = p[pi].to_ascii_lowercase();
                    let tc_lower = t[ti].to_ascii_lowercase();
                    if pc_lower == tc_lower {
                        pi += 1;
                        ti += 1;
                        continue;
                    } else if let Some(sp) = star_pi {
                        pi = sp + 1;
                        star_ti += 1;
                        ti = star_ti;
                        continue;
                    } else {
                        return false;
                    }
                }
            }
            if p[pi] == '%' {
                star_pi = Some(pi);
                star_ti = ti;
                pi += 1;
                continue;
            }
            if p[pi] == '_' {
                pi += 1;
                ti += 1;
                continue;
            }
            if p[pi].eq_ignore_ascii_case(&t[ti]) {
                pi += 1;
                ti += 1;
                continue;
            }
        }
        if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

fn eval_case(
    operand: Option<&Expr>,
    conditions: &[(Expr, Expr)],
    else_result: Option<&Expr>,
    ctx: &EvalCtx,
) -> Result<Value> {
    if let Some(op_expr) = operand {
        let op_val = eval_expr(op_expr, ctx)?;
        for (cond, result) in conditions {
            let cond_val = eval_expr(cond, ctx)?;
            if !op_val.is_null() && !cond_val.is_null() && op_val == cond_val {
                return eval_expr(result, ctx);
            }
        }
    } else {
        for (cond, result) in conditions {
            let cond_val = eval_expr(cond, ctx)?;
            if is_truthy(&cond_val) {
                return eval_expr(result, ctx);
            }
        }
    }
    match else_result {
        Some(e) => eval_expr(e, ctx),
        None => Ok(Value::Null),
    }
}

fn eval_cast(val: &Value, target: DataType) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }
    match target {
        DataType::Integer => match val {
            Value::Integer(_) => Ok(val.clone()),
            Value::Real(r) => Ok(Value::Integer(*r as i64)),
            Value::Boolean(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
            Value::Text(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Integer)
                .or_else(|_| s.trim().parse::<f64>().map(|f| Value::Integer(f as i64)))
                .map_err(|_| SqlError::InvalidValue(format!("cannot cast '{s}' to INTEGER"))),
            _ => Err(SqlError::InvalidValue(format!(
                "cannot cast {} to INTEGER",
                val.data_type()
            ))),
        },
        DataType::Real => match val {
            Value::Real(_) => Ok(val.clone()),
            Value::Integer(i) => Ok(Value::Real(*i as f64)),
            Value::Boolean(b) => Ok(Value::Real(if *b { 1.0 } else { 0.0 })),
            Value::Text(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::Real)
                .map_err(|_| SqlError::InvalidValue(format!("cannot cast '{s}' to REAL"))),
            _ => Err(SqlError::InvalidValue(format!(
                "cannot cast {} to REAL",
                val.data_type()
            ))),
        },
        DataType::Text => Ok(Value::Text(value_to_text(val).into())),
        DataType::Boolean => match val {
            Value::Boolean(_) => Ok(val.clone()),
            Value::Integer(i) => Ok(Value::Boolean(*i != 0)),
            Value::Text(s) => {
                let lower = s.trim().to_ascii_lowercase();
                match lower.as_str() {
                    "true" | "1" | "yes" | "on" => Ok(Value::Boolean(true)),
                    "false" | "0" | "no" | "off" => Ok(Value::Boolean(false)),
                    _ => Err(SqlError::InvalidValue(format!(
                        "cannot cast '{s}' to BOOLEAN"
                    ))),
                }
            }
            _ => Err(SqlError::InvalidValue(format!(
                "cannot cast {} to BOOLEAN",
                val.data_type()
            ))),
        },
        DataType::Blob => match val {
            Value::Blob(_) => Ok(val.clone()),
            Value::Text(s) => Ok(Value::Blob(s.as_bytes().to_vec())),
            _ => Err(SqlError::InvalidValue(format!(
                "cannot cast {} to BLOB",
                val.data_type()
            ))),
        },
        DataType::Null => Ok(Value::Null),
        DataType::Date => val.clone().coerce_into(DataType::Date).ok_or_else(|| {
            SqlError::InvalidValue(format!("cannot cast {} to DATE", val.data_type()))
        }),
        DataType::Time => val.clone().coerce_into(DataType::Time).ok_or_else(|| {
            SqlError::InvalidValue(format!("cannot cast {} to TIME", val.data_type()))
        }),
        DataType::Timestamp => val.clone().coerce_into(DataType::Timestamp).ok_or_else(|| {
            SqlError::InvalidValue(format!("cannot cast {} to TIMESTAMP", val.data_type()))
        }),
        DataType::Interval => val.clone().coerce_into(DataType::Interval).ok_or_else(|| {
            SqlError::InvalidValue(format!("cannot cast {} to INTERVAL", val.data_type()))
        }),
    }
}

fn eval_scalar_function(name: &str, args: &[Expr], ctx: &EvalCtx) -> Result<Value> {
    let evaluated: Vec<Value> = args
        .iter()
        .map(|a| eval_expr(a, ctx))
        .collect::<Result<Vec<_>>>()?;

    match name {
        "LENGTH" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Integer(s.chars().count() as i64)),
                Value::Blob(b) => Ok(Value::Integer(b.len() as i64)),
                _ => Ok(Value::Integer(
                    value_to_text(&evaluated[0]).chars().count() as i64
                )),
            }
        }
        "UPPER" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(s.to_ascii_uppercase())),
                _ => Ok(Value::Text(
                    value_to_text(&evaluated[0]).to_ascii_uppercase().into(),
                )),
            }
        }
        "LOWER" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(s.to_ascii_lowercase())),
                _ => Ok(Value::Text(
                    value_to_text(&evaluated[0]).to_ascii_lowercase().into(),
                )),
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            if evaluated.len() < 2 || evaluated.len() > 3 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 2 or 3 arguments"
                )));
            }
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let s = value_to_text(&evaluated[0]);
            let chars: Vec<char> = s.chars().collect();
            let start = match &evaluated[1] {
                Value::Integer(i) => *i,
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "INTEGER".into(),
                        got: evaluated[1].data_type().to_string(),
                    })
                }
            };
            let len = chars.len() as i64;

            let (begin, count) = if evaluated.len() == 3 {
                let cnt = match &evaluated[2] {
                    Value::Integer(i) => *i,
                    _ => {
                        return Err(SqlError::TypeMismatch {
                            expected: "INTEGER".into(),
                            got: evaluated[2].data_type().to_string(),
                        })
                    }
                };
                if start >= 1 {
                    let b = (start - 1).min(len) as usize;
                    let c = cnt.max(0) as usize;
                    (b, c)
                } else if start == 0 {
                    let c = (cnt - 1).max(0) as usize;
                    (0usize, c)
                } else {
                    let adjusted_cnt = (cnt + start - 1).max(0) as usize;
                    (0usize, adjusted_cnt)
                }
            } else if start >= 1 {
                let b = (start - 1).min(len) as usize;
                (b, chars.len() - b)
            } else if start == 0 {
                (0usize, chars.len())
            } else {
                let b = (len + start).max(0) as usize;
                (b, chars.len() - b)
            };

            let result: String = chars.iter().skip(begin).take(count).collect();
            Ok(Value::Text(result.into()))
        }
        "TRIM" | "LTRIM" | "RTRIM" => {
            if evaluated.is_empty() || evaluated.len() > 2 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 or 2 arguments"
                )));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let s = value_to_text(&evaluated[0]);
            let trim_chars: Vec<char> = if evaluated.len() == 2 {
                if evaluated[1].is_null() {
                    return Ok(Value::Null);
                }
                value_to_text(&evaluated[1]).chars().collect()
            } else {
                vec![' ']
            };
            let result = match name {
                "TRIM" => s
                    .trim_matches(|c: char| trim_chars.contains(&c))
                    .to_string(),
                "LTRIM" => s
                    .trim_start_matches(|c: char| trim_chars.contains(&c))
                    .to_string(),
                "RTRIM" => s
                    .trim_end_matches(|c: char| trim_chars.contains(&c))
                    .to_string(),
                _ => unreachable!(),
            };
            Ok(Value::Text(result.into()))
        }
        "REPLACE" => {
            check_args(name, &evaluated, 3)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let s = value_to_text(&evaluated[0]);
            let from = value_to_text(&evaluated[1]);
            let to = value_to_text(&evaluated[2]);
            if from.is_empty() {
                return Ok(Value::Text(s.into()));
            }
            Ok(Value::Text(s.replace(&from, &to).into()))
        }
        "INSTR" => {
            check_args(name, &evaluated, 2)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let haystack = value_to_text(&evaluated[0]);
            let needle = value_to_text(&evaluated[1]);
            let pos = haystack
                .find(&needle)
                .map(|i| haystack[..i].chars().count() as i64 + 1)
                .unwrap_or(0);
            Ok(Value::Integer(pos))
        }
        "CONCAT" => {
            if evaluated.is_empty() {
                return Ok(Value::Text(CompactString::default()));
            }
            let mut result = String::new();
            for v in &evaluated {
                match v {
                    Value::Null => {}
                    _ => result.push_str(&value_to_text(v)),
                }
            }
            Ok(Value::Text(result.into()))
        }
        "ABS" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(i) => i
                    .checked_abs()
                    .map(Value::Integer)
                    .ok_or(SqlError::IntegerOverflow),
                Value::Real(r) => Ok(Value::Real(r.abs())),
                _ => Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: evaluated[0].data_type().to_string(),
                }),
            }
        }
        "ROUND" => {
            if evaluated.is_empty() || evaluated.len() > 2 {
                return Err(SqlError::InvalidValue(
                    "ROUND requires 1 or 2 arguments".into(),
                ));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let val = match &evaluated[0] {
                Value::Integer(i) => *i as f64,
                Value::Real(r) => *r,
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric".into(),
                        got: evaluated[0].data_type().to_string(),
                    })
                }
            };
            let places = if evaluated.len() == 2 {
                match &evaluated[1] {
                    Value::Null => return Ok(Value::Null),
                    Value::Integer(i) => *i,
                    _ => {
                        return Err(SqlError::TypeMismatch {
                            expected: "INTEGER".into(),
                            got: evaluated[1].data_type().to_string(),
                        })
                    }
                }
            } else {
                0
            };
            let factor = 10f64.powi(places as i32);
            let rounded = (val * factor).round() / factor;
            Ok(Value::Real(rounded))
        }
        "CEIL" | "CEILING" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(i) => Ok(Value::Integer(*i)),
                Value::Real(r) => Ok(Value::Integer(r.ceil() as i64)),
                _ => Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: evaluated[0].data_type().to_string(),
                }),
            }
        }
        "FLOOR" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(i) => Ok(Value::Integer(*i)),
                Value::Real(r) => Ok(Value::Integer(r.floor() as i64)),
                _ => Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: evaluated[0].data_type().to_string(),
                }),
            }
        }
        "SIGN" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(i) => Ok(Value::Integer(i.signum())),
                Value::Real(r) => {
                    if *r > 0.0 {
                        Ok(Value::Integer(1))
                    } else if *r < 0.0 {
                        Ok(Value::Integer(-1))
                    } else {
                        Ok(Value::Integer(0))
                    }
                }
                _ => Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: evaluated[0].data_type().to_string(),
                }),
            }
        }
        "SQRT" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(i) => {
                    if *i < 0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Real((*i as f64).sqrt()))
                    }
                }
                Value::Real(r) => {
                    if *r < 0.0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Real(r.sqrt()))
                    }
                }
                _ => Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: evaluated[0].data_type().to_string(),
                }),
            }
        }
        "RANDOM" => {
            check_args(name, &evaluated, 0)?;
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            use std::time::SystemTime;
            let mut hasher = DefaultHasher::new();
            SystemTime::now().hash(&mut hasher);
            std::thread::current().id().hash(&mut hasher);
            let mut val = hasher.finish() as i64;
            if val == i64::MIN {
                val = i64::MAX;
            }
            Ok(Value::Integer(val))
        }
        "TYPEOF" => {
            check_args(name, &evaluated, 1)?;
            let type_name = match &evaluated[0] {
                Value::Null => "null",
                Value::Integer(_) => "integer",
                Value::Real(_) => "real",
                Value::Text(_) => "text",
                Value::Blob(_) => "blob",
                Value::Boolean(_) => "boolean",
                Value::Date(_) => "date",
                Value::Time(_) => "time",
                Value::Timestamp(_) => "timestamp",
                Value::Interval { .. } => "interval",
            };
            Ok(Value::Text(type_name.into()))
        }
        "MIN" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() {
                return Ok(evaluated[1].clone());
            }
            if evaluated[1].is_null() {
                return Ok(evaluated[0].clone());
            }
            if evaluated[0] <= evaluated[1] {
                Ok(evaluated[0].clone())
            } else {
                Ok(evaluated[1].clone())
            }
        }
        "MAX" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() {
                return Ok(evaluated[1].clone());
            }
            if evaluated[1].is_null() {
                return Ok(evaluated[0].clone());
            }
            if evaluated[0] >= evaluated[1] {
                Ok(evaluated[0].clone())
            } else {
                Ok(evaluated[1].clone())
            }
        }
        "HEX" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Blob(b) => {
                    let mut s = String::with_capacity(b.len() * 2);
                    for byte in b {
                        s.push_str(&format!("{byte:02X}"));
                    }
                    Ok(Value::Text(s.into()))
                }
                Value::Text(s) => {
                    let mut r = String::with_capacity(s.len() * 2);
                    for byte in s.as_bytes() {
                        r.push_str(&format!("{byte:02X}"));
                    }
                    Ok(Value::Text(r.into()))
                }
                _ => Ok(Value::Text(value_to_text(&evaluated[0]).into())),
            }
        }
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" => {
            check_args(name, &evaluated, 0)?;
            Ok(Value::Timestamp(crate::datetime::txn_or_clock_micros()))
        }
        "CURRENT_DATE" => {
            check_args(name, &evaluated, 0)?;
            Ok(Value::Date(crate::datetime::ts_to_date_floor(
                crate::datetime::txn_or_clock_micros(),
            )))
        }
        "CURRENT_TIME" | "LOCALTIME" => {
            check_args(name, &evaluated, 0)?;
            Ok(Value::Time(
                crate::datetime::ts_split(crate::datetime::txn_or_clock_micros()).1,
            ))
        }
        "CLOCK_TIMESTAMP" | "STATEMENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" => {
            check_args(name, &evaluated, 0)?;
            let ts = match name {
                "CLOCK_TIMESTAMP" => crate::datetime::now_micros(),
                _ => crate::datetime::txn_or_clock_micros(),
            };
            Ok(Value::Timestamp(ts))
        }
        "EXTRACT" | "DATE_PART" | "DATEPART" => {
            check_args(name, &evaluated, 2)?;
            // Borrow the field str without allocating; datetime::extract accepts &str.
            let field: &str = match &evaluated[0] {
                Value::Null => return Ok(Value::Null),
                Value::Text(s) => s.as_str(),
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "TEXT field name".into(),
                        got: evaluated[0].data_type().to_string(),
                    })
                }
            };
            if evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::datetime::extract(field, &evaluated[1])
        }
        "DATE_TRUNC" => {
            if evaluated.len() < 2 || evaluated.len() > 3 {
                return Err(SqlError::InvalidValue(
                    "DATE_TRUNC requires 2 or 3 arguments".into(),
                ));
            }
            let unit = match &evaluated[0] {
                Value::Null => return Ok(Value::Null),
                Value::Text(s) => s.to_string(),
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "TEXT unit name".into(),
                        got: evaluated[0].data_type().to_string(),
                    })
                }
            };
            if evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            // Optional tz arg: truncate in that zone, then convert back to UTC.
            if evaluated.len() == 3 {
                if let Value::Text(tz) = &evaluated[2] {
                    if !tz.eq_ignore_ascii_case("UTC") {
                        if let Value::Timestamp(ts) = &evaluated[1] {
                            return date_trunc_in_zone(&unit, *ts, tz);
                        }
                    }
                }
            }
            crate::datetime::date_trunc(&unit, &evaluated[1])
        }
        "DATE_BIN" => {
            check_args(name, &evaluated, 3)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let stride = match &evaluated[0] {
                Value::Interval {
                    months: _,
                    days,
                    micros,
                } => *days as i64 * crate::datetime::MICROS_PER_DAY + *micros,
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "INTERVAL stride".into(),
                        got: evaluated[0].data_type().to_string(),
                    })
                }
            };
            if stride <= 0 {
                return Err(SqlError::InvalidValue(
                    "DATE_BIN stride must be positive".into(),
                ));
            }
            let (src, origin) = match (&evaluated[1], &evaluated[2]) {
                (Value::Timestamp(s), Value::Timestamp(o)) => (*s, *o),
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "TIMESTAMP, TIMESTAMP".into(),
                        got: format!("{}, {}", evaluated[1].data_type(), evaluated[2].data_type()),
                    })
                }
            };
            let diff = src - origin;
            let binned = origin + (diff.div_euclid(stride)) * stride;
            Ok(Value::Timestamp(binned))
        }
        "AGE" => {
            if evaluated.len() == 1 {
                if evaluated[0].is_null() {
                    return Ok(Value::Null);
                }
                let ts = match &evaluated[0] {
                    Value::Timestamp(t) => *t,
                    Value::Date(d) => crate::datetime::date_to_ts(*d),
                    _ => {
                        return Err(SqlError::TypeMismatch {
                            expected: "TIMESTAMP or DATE".into(),
                            got: evaluated[0].data_type().to_string(),
                        })
                    }
                };
                // Implicit reference: today at midnight UTC.
                let today = crate::datetime::today_days();
                let midnight = crate::datetime::date_to_ts(today);
                let (m, d, u) = crate::datetime::age(midnight, ts)?;
                return Ok(Value::Interval {
                    months: m,
                    days: d,
                    micros: u,
                });
            }
            check_args(name, &evaluated, 2)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let a = ts_of(&evaluated[0])?;
            let b = ts_of(&evaluated[1])?;
            let (m, d, u) = crate::datetime::age(a, b)?;
            Ok(Value::Interval {
                months: m,
                days: d,
                micros: u,
            })
        }
        "MAKE_DATE" => {
            check_args(name, &evaluated, 3)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let y = int_arg(&evaluated[0], "MAKE_DATE year")? as i32;
            let m = int_arg(&evaluated[1], "MAKE_DATE month")? as u8;
            let d = int_arg(&evaluated[2], "MAKE_DATE day")? as u8;
            crate::datetime::ymd_to_days(y, m, d)
                .map(Value::Date)
                .ok_or_else(|| SqlError::InvalidDateLiteral(format!("make_date({y}, {m}, {d})")))
        }
        "MAKE_TIME" => {
            check_args(name, &evaluated, 3)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let h = int_arg(&evaluated[0], "MAKE_TIME hour")? as u8;
            let mi = int_arg(&evaluated[1], "MAKE_TIME minute")? as u8;
            let (s, us) = real_sec_arg(&evaluated[2])?;
            crate::datetime::hmsn_to_micros(h, mi, s, us)
                .map(Value::Time)
                .ok_or_else(|| SqlError::InvalidTimeLiteral(format!("make_time({h}, {mi}, ...)")))
        }
        "MAKE_TIMESTAMP" => {
            check_args(name, &evaluated, 6)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let y = int_arg(&evaluated[0], "MAKE_TIMESTAMP year")? as i32;
            let mo = int_arg(&evaluated[1], "MAKE_TIMESTAMP month")? as u8;
            let d = int_arg(&evaluated[2], "MAKE_TIMESTAMP day")? as u8;
            let h = int_arg(&evaluated[3], "MAKE_TIMESTAMP hour")? as u8;
            let mi = int_arg(&evaluated[4], "MAKE_TIMESTAMP min")? as u8;
            let (s, us) = real_sec_arg(&evaluated[5])?;
            let days = crate::datetime::ymd_to_days(y, mo, d).ok_or_else(|| {
                SqlError::InvalidTimestampLiteral(format!("make_timestamp year={y}"))
            })?;
            let tmicros = crate::datetime::hmsn_to_micros(h, mi, s, us)
                .ok_or_else(|| SqlError::InvalidTimestampLiteral("time out of range".into()))?;
            Ok(Value::Timestamp(crate::datetime::ts_combine(days, tmicros)))
        }
        "MAKE_INTERVAL" => {
            // Positional args: years, months, weeks, days, hours, mins, secs.
            if evaluated.len() > 7 {
                return Err(SqlError::InvalidValue(
                    "MAKE_INTERVAL accepts at most 7 arguments".into(),
                ));
            }
            let mut months: i64 = 0;
            let mut days: i64 = 0;
            let mut micros: i64 = 0;
            for (i, v) in evaluated.iter().enumerate() {
                if v.is_null() {
                    continue;
                }
                let n = match v {
                    Value::Integer(n) => *n,
                    Value::Real(r) => *r as i64,
                    _ => {
                        return Err(SqlError::TypeMismatch {
                            expected: "numeric".into(),
                            got: v.data_type().to_string(),
                        })
                    }
                };
                match i {
                    0 => months = months.saturating_add(n.saturating_mul(12)),
                    1 => months = months.saturating_add(n),
                    2 => days = days.saturating_add(n.saturating_mul(7)),
                    3 => days = days.saturating_add(n),
                    4 => {
                        micros = micros
                            .saturating_add(n.saturating_mul(crate::datetime::MICROS_PER_HOUR))
                    }
                    5 => {
                        micros =
                            micros.saturating_add(n.saturating_mul(crate::datetime::MICROS_PER_MIN))
                    }
                    6 => {
                        // Seconds may be fractional — also check Real.
                        if let Value::Real(r) = v {
                            micros = micros.saturating_add(
                                (*r * crate::datetime::MICROS_PER_SEC as f64) as i64,
                            );
                        } else {
                            micros = micros
                                .saturating_add(n.saturating_mul(crate::datetime::MICROS_PER_SEC));
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Ok(Value::Interval {
                months: months.clamp(i32::MIN as i64, i32::MAX as i64) as i32,
                days: days.clamp(i32::MIN as i64, i32::MAX as i64) as i32,
                micros,
            })
        }
        "JUSTIFY_DAYS" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Interval {
                    months,
                    days,
                    micros,
                } => {
                    let (m, d, u) = crate::datetime::justify_days(*months, *days, *micros);
                    Ok(Value::Interval {
                        months: m,
                        days: d,
                        micros: u,
                    })
                }
                other => Err(SqlError::TypeMismatch {
                    expected: "INTERVAL".into(),
                    got: other.data_type().to_string(),
                }),
            }
        }
        "JUSTIFY_HOURS" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Interval {
                    months,
                    days,
                    micros,
                } => {
                    let (m, d, u) = crate::datetime::justify_hours(*months, *days, *micros);
                    Ok(Value::Interval {
                        months: m,
                        days: d,
                        micros: u,
                    })
                }
                other => Err(SqlError::TypeMismatch {
                    expected: "INTERVAL".into(),
                    got: other.data_type().to_string(),
                }),
            }
        }
        "JUSTIFY_INTERVAL" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Interval {
                    months,
                    days,
                    micros,
                } => {
                    let (m, d, u) = crate::datetime::justify_interval(*months, *days, *micros);
                    Ok(Value::Interval {
                        months: m,
                        days: d,
                        micros: u,
                    })
                }
                other => Err(SqlError::TypeMismatch {
                    expected: "INTERVAL".into(),
                    got: other.data_type().to_string(),
                }),
            }
        }
        "ISFINITE" => {
            check_args(name, &evaluated, 1)?;
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Boolean(evaluated[0].is_finite_temporal()))
        }
        "DATE" => {
            if evaluated.is_empty() {
                return Err(SqlError::InvalidValue(
                    "DATE requires at least 1 argument".into(),
                ));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let d = match &evaluated[0] {
                Value::Date(d) => *d,
                Value::Timestamp(t) => crate::datetime::ts_to_date_floor(*t),
                Value::Text(s) if s.eq_ignore_ascii_case("now") => crate::datetime::today_days(),
                Value::Text(s) => crate::datetime::parse_date(s)?,
                Value::Integer(n) => {
                    crate::datetime::ts_to_date_floor(*n * crate::datetime::MICROS_PER_SEC)
                }
                other => {
                    return Err(SqlError::TypeMismatch {
                        expected: "TIMESTAMP, DATE, TEXT, or INTEGER".into(),
                        got: other.data_type().to_string(),
                    })
                }
            };
            Ok(Value::Date(d))
        }
        "TIME" => {
            if evaluated.is_empty() {
                return Err(SqlError::InvalidValue(
                    "TIME requires at least 1 argument".into(),
                ));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let t = match &evaluated[0] {
                Value::Time(t) => *t,
                Value::Timestamp(t) => crate::datetime::ts_split(*t).1,
                Value::Text(s) if s.eq_ignore_ascii_case("now") => {
                    crate::datetime::current_time_micros()
                }
                Value::Text(s) => crate::datetime::parse_time(s)?,
                other => {
                    return Err(SqlError::TypeMismatch {
                        expected: "TIMESTAMP, TIME, or TEXT".into(),
                        got: other.data_type().to_string(),
                    })
                }
            };
            Ok(Value::Time(t))
        }
        "DATETIME" => {
            if evaluated.is_empty() {
                return Err(SqlError::InvalidValue(
                    "DATETIME requires at least 1 argument".into(),
                ));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let t = match &evaluated[0] {
                Value::Timestamp(t) => *t,
                Value::Date(d) => crate::datetime::date_to_ts(*d),
                Value::Text(s) if s.eq_ignore_ascii_case("now") => crate::datetime::now_micros(),
                Value::Text(s) => crate::datetime::parse_timestamp(s)?,
                Value::Integer(n) => n * crate::datetime::MICROS_PER_SEC,
                other => {
                    return Err(SqlError::TypeMismatch {
                        expected: "TIMESTAMP, DATE, TEXT, or INTEGER".into(),
                        got: other.data_type().to_string(),
                    })
                }
            };
            Ok(Value::Timestamp(t))
        }
        "STRFTIME" => {
            if evaluated.len() < 2 {
                return Err(SqlError::InvalidValue(
                    "STRFTIME requires format + value".into(),
                ));
            }
            if evaluated.iter().take(2).any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let fmt = match &evaluated[0] {
                Value::Text(s) => s.to_string(),
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "TEXT format".into(),
                        got: evaluated[0].data_type().to_string(),
                    })
                }
            };
            let out = crate::datetime::strftime(&fmt, &evaluated[1])?;
            Ok(Value::Text(out.into()))
        }
        "JULIANDAY" => {
            if evaluated.is_empty() {
                return Err(SqlError::InvalidValue(
                    "JULIANDAY requires at least 1 argument".into(),
                ));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let micros = ts_of(&evaluated[0])?;
            let (days, tmicros) = crate::datetime::ts_split(micros);
            // Julian Day 2440587.5 = 1970-01-01 00:00:00 UTC (Julian days start at noon).
            let julian =
                days as f64 + 2_440_587.5 + tmicros as f64 / crate::datetime::MICROS_PER_DAY as f64;
            Ok(Value::Real(julian))
        }
        "UNIXEPOCH" => {
            if evaluated.is_empty() {
                return Err(SqlError::InvalidValue(
                    "UNIXEPOCH requires at least 1 argument".into(),
                ));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let micros = ts_of(&evaluated[0])?;
            let subsec = evaluated
                .get(1)
                .and_then(|v| {
                    if let Value::Text(s) = v {
                        Some(s.to_string())
                    } else {
                        None
                    }
                })
                .map(|s| s.eq_ignore_ascii_case("subsec") || s.eq_ignore_ascii_case("subsecond"))
                .unwrap_or(false);
            if subsec {
                Ok(Value::Real(
                    micros as f64 / crate::datetime::MICROS_PER_SEC as f64,
                ))
            } else {
                Ok(Value::Integer(micros / crate::datetime::MICROS_PER_SEC))
            }
        }
        "TIMEDIFF" => {
            check_args(name, &evaluated, 2)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let a = ts_of(&evaluated[0])?;
            let b = ts_of(&evaluated[1])?;
            let (days, micros) = crate::datetime::subtract_timestamps(a, b);
            let sign = if days < 0 || (days == 0 && micros < 0) {
                "-"
            } else {
                "+"
            };
            let abs_days = days.unsigned_abs() as i64;
            let abs_us = micros.unsigned_abs() as i64;
            // PG-compat format string: "(+|-)YYYY-MM-DD HH:MM:SS.SSS", days-only.
            let (h, m, s, us) = crate::datetime::micros_to_hmsn(abs_us);
            Ok(Value::Text(
                format!("{sign}{abs_days:04}-00-00 {h:02}:{m:02}:{s:02}.{us:06}").into(),
            ))
        }
        "AT_TIMEZONE" => {
            check_args(name, &evaluated, 2)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let ts = match &evaluated[0] {
                Value::Timestamp(t) => *t,
                Value::Date(d) => crate::datetime::date_to_ts(*d),
                other => {
                    return Err(SqlError::TypeMismatch {
                        expected: "TIMESTAMP or DATE".into(),
                        got: other.data_type().to_string(),
                    })
                }
            };
            let zone = match &evaluated[1] {
                Value::Text(s) => s.to_string(),
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "TEXT time zone".into(),
                        got: evaluated[1].data_type().to_string(),
                    })
                }
            };
            // Reject POSIX-style 'UTC+5' (ambiguous sign convention).
            let upper = zone.to_ascii_uppercase();
            if (upper.starts_with("UTC+") || upper.starts_with("UTC-")) && zone.len() > 3 {
                return Err(SqlError::InvalidTimezone(format!(
                    "'{zone}' is ambiguous — use ISO-8601 offset like '+05:00' or named zone like 'Etc/GMT-5'"
                )));
            }
            let formatted = crate::datetime::format_timestamp_in_zone(ts, &zone)?;
            Ok(Value::Text(formatted.into()))
        }
        _ => Err(SqlError::Unsupported(format!("scalar function: {name}"))),
    }
}

/// Extract a timestamp (µs UTC) from a Value, coercing DATE → midnight.
fn ts_of(v: &Value) -> Result<i64> {
    match v {
        Value::Timestamp(t) => Ok(*t),
        Value::Date(d) => Ok(crate::datetime::date_to_ts(*d)),
        _ => Err(SqlError::TypeMismatch {
            expected: "TIMESTAMP or DATE".into(),
            got: v.data_type().to_string(),
        }),
    }
}

fn int_arg(v: &Value, label: &str) -> Result<i64> {
    match v {
        Value::Integer(n) => Ok(*n),
        _ => Err(SqlError::TypeMismatch {
            expected: format!("INTEGER ({label})"),
            got: v.data_type().to_string(),
        }),
    }
}

/// Extract (whole_seconds: u8, frac_micros: u32) from a numeric argument for MAKE_TIME-style calls.
fn real_sec_arg(v: &Value) -> Result<(u8, u32)> {
    match v {
        Value::Integer(n) => {
            if !(0..=60).contains(n) {
                return Err(SqlError::InvalidValue(format!("second out of range: {n}")));
            }
            Ok((*n as u8, 0))
        }
        Value::Real(r) => {
            let whole = r.trunc() as i64;
            if !(0..=60).contains(&whole) {
                return Err(SqlError::InvalidValue(format!("second out of range: {r}")));
            }
            let frac = ((r - whole as f64) * 1_000_000.0).round() as i64;
            Ok((whole as u8, frac.max(0) as u32))
        }
        _ => Err(SqlError::TypeMismatch {
            expected: "numeric seconds".into(),
            got: v.data_type().to_string(),
        }),
    }
}

/// DATE_TRUNC with a non-UTC IANA zone: convert → truncate in that zone → convert back to UTC.
fn date_trunc_in_zone(unit: &str, ts_utc: i64, tz: &str) -> Result<Value> {
    use jiff::{tz::TimeZone, Timestamp as JTimestamp};
    let zone = TimeZone::get(tz).map_err(|e| SqlError::InvalidTimezone(format!("{tz}: {e}")))?;
    let ts = JTimestamp::from_microsecond(ts_utc)
        .map_err(|e| SqlError::InvalidValue(format!("ts: {e}")))?;
    let zoned = ts.to_zoned(zone.clone());
    let unit_lower = unit.to_ascii_lowercase();
    let rounded = match unit_lower.as_str() {
        "microseconds" => return Ok(Value::Timestamp(ts_utc)),
        "second" => zoned
            .start_of_day()
            .map_err(|e| SqlError::InvalidValue(format!("{e}")))?,
        _ => {
            let naive_ts = zoned.timestamp().as_microsecond();
            return crate::datetime::date_trunc(unit, &Value::Timestamp(naive_ts));
        }
    };
    Ok(Value::Timestamp(rounded.timestamp().as_microsecond()))
}

fn check_args(name: &str, args: &[Value], expected: usize) -> Result<()> {
    if args.len() != expected {
        Err(SqlError::InvalidValue(format!(
            "{name} requires {expected} argument(s), got {}",
            args.len()
        )))
    } else {
        Ok(())
    }
}

pub fn referenced_columns(expr: &Expr, columns: &[ColumnDef]) -> Vec<usize> {
    let mut indices = Vec::new();
    collect_column_refs(expr, columns, &mut indices);
    indices.sort_unstable();
    indices.dedup();
    indices
}

fn collect_column_refs(expr: &Expr, columns: &[ColumnDef], out: &mut Vec<usize>) {
    match expr {
        Expr::Column(name) => {
            for (i, c) in columns.iter().enumerate() {
                if c.name == *name
                    || (c.name.len() > name.len()
                        && c.name.as_bytes()[c.name.len() - name.len() - 1] == b'.'
                        && c.name.ends_with(name.as_str()))
                {
                    out.push(i);
                    break;
                }
            }
        }
        Expr::QualifiedColumn { table, column } => {
            let mut found: Option<usize> = None;
            let mut bare_match: Option<usize> = None;
            let mut bare_count = 0usize;
            for (i, c) in columns.iter().enumerate() {
                if c.name.len() == table.len() + 1 + column.len()
                    && c.name.as_bytes()[table.len()] == b'.'
                    && c.name.starts_with(table.as_str())
                    && c.name.ends_with(column.as_str())
                {
                    found = Some(i);
                    break;
                }
                if c.name == *column {
                    bare_match = Some(i);
                    bare_count += 1;
                }
            }
            if let Some(idx) = found {
                out.push(idx);
            } else if bare_count == 1 {
                out.push(bare_match.unwrap());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_column_refs(left, columns, out);
            collect_column_refs(right, columns, out);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_column_refs(expr, columns, out);
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => {
            collect_column_refs(e, columns, out);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                collect_column_refs(arg, columns, out);
            }
        }
        Expr::InSubquery { expr, .. } => {
            collect_column_refs(expr, columns, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_column_refs(expr, columns, out);
            for item in list {
                collect_column_refs(item, columns, out);
            }
        }
        Expr::InSet { expr, .. } => {
            collect_column_refs(expr, columns, out);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_column_refs(expr, columns, out);
            collect_column_refs(low, columns, out);
            collect_column_refs(high, columns, out);
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_column_refs(expr, columns, out);
            collect_column_refs(pattern, columns, out);
            if let Some(esc) = escape {
                collect_column_refs(esc, columns, out);
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_column_refs(op, columns, out);
            }
            for (when, then) in conditions {
                collect_column_refs(when, columns, out);
                collect_column_refs(then, columns, out);
            }
            if let Some(e) = else_result {
                collect_column_refs(e, columns, out);
            }
        }
        Expr::Coalesce(args) => {
            for arg in args {
                collect_column_refs(arg, columns, out);
            }
        }
        Expr::Cast { expr, .. } => {
            collect_column_refs(expr, columns, out);
        }
        Expr::Collate { expr, .. } => {
            collect_column_refs(expr, columns, out);
        }
        Expr::WindowFunction { args, spec, .. } => {
            for arg in args {
                collect_column_refs(arg, columns, out);
            }
            for pb in &spec.partition_by {
                collect_column_refs(pb, columns, out);
            }
            for ob in &spec.order_by {
                collect_column_refs(&ob.expr, columns, out);
            }
        }
        Expr::Literal(_)
        | Expr::Parameter(_)
        | Expr::CountStar
        | Expr::Exists { .. }
        | Expr::ScalarSubquery(_) => {}
    }
}

/// Check if an expression result is truthy (for WHERE/HAVING).
pub fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Boolean(b) => *b,
        Value::Integer(i) => *i != 0,
        Value::Null => false,
        _ => true,
    }
}

#[cfg(test)]
#[path = "eval_tests.rs"]
mod tests;
