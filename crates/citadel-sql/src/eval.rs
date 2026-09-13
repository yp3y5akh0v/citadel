//! Expression evaluator with SQL three-valued logic.

use std::panic::RefUnwindSafe;

use rustc_hash::FxHashMap;

use crate::error::{Result, SqlError};
use crate::parser::{BinOp, Expr, QuantifiedRhs, UnaryOp};
use crate::types::{ColumnDef, CompactString, DataType, Value};

mod text_search;

#[derive(Debug)]
pub struct ColumnMap {
    exact: FxHashMap<String, ShortMatch>,
    short: FxHashMap<String, ShortMatch>,
    collations: Vec<crate::types::Collation>,
    has_non_binary_collation: bool,
}

#[derive(Clone, Debug)]
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
            exact
                .entry(lower.clone())
                .and_modify(|entry| *entry = ShortMatch::Ambiguous)
                .or_insert(ShortMatch::Unique(i));

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

    pub(crate) fn len(&self) -> usize {
        self.collations.len()
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
        match self.exact.get(name) {
            Some(ShortMatch::Unique(idx)) => return Ok(*idx),
            Some(ShortMatch::Ambiguous) => {
                return Err(SqlError::AmbiguousColumn(name.to_string()));
            }
            None => {}
        }
        match self.short.get(name) {
            Some(ShortMatch::Unique(idx)) => Ok(*idx),
            Some(ShortMatch::Ambiguous) => Err(SqlError::AmbiguousColumn(name.to_string())),
            None => Err(SqlError::ColumnNotFound(name.to_string())),
        }
    }

    pub(crate) fn resolve_qualified(&self, table: &str, column: &str) -> Result<usize> {
        let qualified = format!("{table}.{column}");
        match self.exact.get(&qualified) {
            Some(ShortMatch::Unique(idx)) => return Ok(*idx),
            Some(ShortMatch::Ambiguous) => {
                return Err(SqlError::AmbiguousColumn(qualified));
            }
            None => {}
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
    pub(crate) cancel: Option<&'a citadel::CancelToken>,
    pub excluded: Option<ExcludedRow<'a>>,
    excluded_resolver: Option<&'a (dyn Fn(usize) -> Result<Value> + Sync + RefUnwindSafe)>,
    pub old_new: Option<OldNewRows<'a>>,
    pub session_tz: Option<jiff::tz::TimeZone>,
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
            cancel: None,
            excluded: None,
            excluded_resolver: None,
            old_new: None,
            session_tz: None,
        }
    }

    pub fn with_session_tz(mut self, tz: Option<jiff::tz::TimeZone>) -> Self {
        self.session_tz = tz;
        self
    }

    pub(crate) fn with_cancel(mut self, cancel: Option<&'a citadel::CancelToken>) -> Self {
        self.cancel = cancel;
        self
    }

    pub fn with_params(col_map: &'a ColumnMap, row: &'a [Value], params: &'a [Value]) -> Self {
        Self {
            col_map,
            row,
            params,
            cancel: None,
            excluded: None,
            excluded_resolver: None,
            old_new: None,
            session_tz: None,
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
            cancel: None,
            excluded: Some(ExcludedRow {
                col_map: excluded_col_map,
                row: excluded_row,
            }),
            excluded_resolver: None,
            old_new: None,
            session_tz: None,
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
            cancel: None,
            excluded: None,
            excluded_resolver: None,
            old_new: Some(OldNewRows {
                col_map,
                old_row,
                new_row,
            }),
            session_tz: None,
        }
    }

    pub(crate) fn with_excluded_resolver(
        mut self,
        resolver: &'a (dyn Fn(usize) -> Result<Value> + Sync + RefUnwindSafe),
    ) -> Self {
        self.excluded_resolver = Some(resolver);
        self
    }
}

thread_local! {
    static SCOPED_PARAMS: std::cell::Cell<(*const Value, usize)> =
        const { std::cell::Cell::new((std::ptr::null(), 0)) };
}

pub fn with_scoped_params<R>(params: &[Value], f: impl FnOnce() -> R) -> R {
    struct Guard((*const Value, usize));
    impl Drop for Guard {
        fn drop(&mut self) {
            SCOPED_PARAMS.with(|slot| slot.set(self.0));
        }
    }
    SCOPED_PARAMS.with(|slot| {
        let prev = slot.get();
        // An empty statement scope must mask an enclosing nonempty scope.
        // Only the already-empty case can skip installing a restore guard.
        if params.is_empty() && prev.1 == 0 {
            return f();
        }
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
        // SAFETY: `with_scoped_params` keeps the slice alive for the body's run.
        unsafe { Ok((*ptr.add(n - 1)).clone()) }
    })
}

pub(crate) enum CompiledExpr<'a> {
    Const(Value),
    Slot(usize),
    Param(usize),
    BinaryOp {
        left: Box<CompiledExpr<'a>>,
        op: BinOp,
        right: Box<CompiledExpr<'a>>,
        collation: Option<crate::types::Collation>,
    },
    UnaryOp {
        op: UnaryOp,
        operand: Box<CompiledExpr<'a>>,
    },
    IsNull(Box<CompiledExpr<'a>>),
    IsNotNull(Box<CompiledExpr<'a>>),
    Dynamic(&'a Expr),
}

impl<'a> CompiledExpr<'a> {
    pub(crate) fn compile(expr: &'a Expr, col_map: &ColumnMap) -> Self {
        match expr {
            Expr::Literal(v) => CompiledExpr::Const(v.clone()),
            Expr::Parameter(n) => CompiledExpr::Param(*n),
            Expr::Column(name) => match col_map.resolve(name) {
                Ok(idx) => CompiledExpr::Slot(idx),
                Err(_) => CompiledExpr::Dynamic(expr),
            },
            Expr::IsNull(e) => CompiledExpr::IsNull(Box::new(Self::compile(e, col_map))),
            Expr::IsNotNull(e) => CompiledExpr::IsNotNull(Box::new(Self::compile(e, col_map))),
            Expr::UnaryOp { op, expr: e } => CompiledExpr::UnaryOp {
                op: *op,
                operand: Box::new(Self::compile(e, col_map)),
            },
            Expr::BinaryOp { left, op, right } => CompiledExpr::BinaryOp {
                collation: compile_collation(left, right, col_map),
                left: Box::new(Self::compile(left, col_map)),
                op: *op,
                right: Box::new(Self::compile(right, col_map)),
            },
            _ => CompiledExpr::Dynamic(expr),
        }
    }

    pub(crate) fn eval(&self, ctx: &EvalCtx) -> Result<Value> {
        match self {
            CompiledExpr::Const(v) => Ok(v.clone()),
            CompiledExpr::Slot(i) => Ok(ctx.row[*i].clone()),
            CompiledExpr::Param(n) => resolve_parameter(*n, ctx.params),
            CompiledExpr::IsNull(e) => Ok(Value::Boolean(e.eval(ctx)?.is_null())),
            CompiledExpr::IsNotNull(e) => Ok(Value::Boolean(!e.eval(ctx)?.is_null())),
            CompiledExpr::UnaryOp { op, operand } => {
                let val = operand.eval(ctx)?;
                eval_unary_op(*op, &val)
            }
            CompiledExpr::BinaryOp {
                left,
                op,
                right,
                collation,
            } => {
                let lval = left.eval(ctx)?;
                let rval = right.eval(ctx)?;
                collated_compare_with_cancel(&lval, *op, &rval, *collation, ctx.cancel)
            }
            CompiledExpr::Dynamic(e) => eval_expr(e, ctx),
        }
    }
}

pub(crate) fn compile_collation(
    left: &Expr,
    right: &Expr,
    col_map: &ColumnMap,
) -> Option<crate::types::Collation> {
    let left_explicit = collation_of(left);
    let right_explicit = collation_of(right);
    let needs_check =
        col_map.has_non_binary_collation() || left_explicit.is_some() || right_explicit.is_some();
    if !needs_check {
        return None;
    }
    let coll = left_explicit
        .or(right_explicit)
        .or_else(|| column_collation(left, col_map))
        .or_else(|| column_collation(right, col_map))?;
    (coll != crate::types::Collation::Binary).then_some(coll)
}

/// The collation an operand carries by itself, for a comparison whose other side is a bare
/// value rather than an expression - the result set of an `IN (subquery)`.
pub(crate) fn operand_collation(
    expr: &Expr,
    col_map: &ColumnMap,
) -> Option<crate::types::Collation> {
    collation_of(expr).or_else(|| column_collation(expr, col_map))
}

thread_local! {
    static JSONPATH_OVERRIDE_CTX: std::cell::Cell<*const ()> =
        const { std::cell::Cell::new(std::ptr::null()) };
}

pub fn eval_expr(expr: &Expr, ctx: &EvalCtx) -> Result<Value> {
    let Some(timezone) = ctx.session_tz.as_ref() else {
        return eval_expr_inner(expr, ctx);
    };
    let identity = std::ptr::from_ref(ctx).cast::<()>();
    JSONPATH_OVERRIDE_CTX.with(|slot| {
        if slot.get() == identity {
            return eval_expr_inner(expr, ctx);
        }
        struct Guard<'a> {
            slot: &'a std::cell::Cell<*const ()>,
            previous: *const (),
        }
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.slot.set(self.previous);
            }
        }
        let previous = slot.replace(identity);
        let _guard = Guard { slot, previous };
        crate::datetime::with_session_timezone(timezone.clone(), || {
            crate::json::with_jsonpath_timezone(timezone, || eval_expr_inner(expr, ctx))
        })
    })
}

fn eval_expr_inner(expr: &Expr, ctx: &EvalCtx) -> Result<Value> {
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
                    if let Some(resolve) = ctx.excluded_resolver {
                        return resolve(idx);
                    }
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
            // Trigger-body fallback: nested executor calls don't carry `ctx.old_new`.
            if table.eq_ignore_ascii_case("old") || table.eq_ignore_ascii_case("new") {
                if let Some(b) = crate::executor::triggers::current_bindings() {
                    let lowered = column.to_ascii_lowercase();
                    if table.eq_ignore_ascii_case("old") {
                        let cm = ColumnMap::new(&b.old_columns);
                        let idx = cm.resolve(&lowered)?;
                        return Ok(b.old_row.map(|r| r[idx].clone()).unwrap_or(Value::Null));
                    } else {
                        let cm = ColumnMap::new(&b.new_columns);
                        let idx = cm.resolve(&lowered)?;
                        return Ok(b.new_row.map(|r| r[idx].clone()).unwrap_or(Value::Null));
                    }
                }
            }
            let idx = ctx.col_map.resolve_qualified(table, column)?;
            Ok(ctx.row[idx].clone())
        }

        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, ctx)?;
            let rval = eval_expr(right, ctx)?;
            collated_compare_with_cancel(
                &lval,
                *op,
                &rval,
                compile_collation(left, right, ctx.col_map),
                ctx.cancel,
            )
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
            eval_in_values(e, &lhs, list, ctx, *negated)
        }

        Expr::InSet {
            expr: e,
            values,
            has_null,
            negated,
            collation,
        } => {
            let lhs = eval_expr(e, ctx)?;
            // `x IN (SELECT y)` collates as `x = y`, which takes it from either
            // operand, left first.
            let coll = operand_collation(e, ctx.col_map).unwrap_or(*collation);
            let coll = (coll != crate::types::Collation::Binary).then_some(coll);
            eval_in_set(&lhs, values, *has_null, *negated, coll)
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
            eval_between(
                &val,
                &lo,
                &hi,
                *negated,
                compile_collation(e, low, ctx.col_map),
                compile_collation(e, high, ctx.col_map),
            )
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

        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let lval = eval_expr(left, ctx)?;
            let rval = eval_expr(right, ctx)?;
            // NULL is a value here, so the answer is never unknown: two NULLs are alike and
            // a NULL beside anything else is not.
            let alike = match (lval.is_null(), rval.is_null()) {
                (true, true) => true,
                (true, false) | (false, true) => false,
                (false, false) => {
                    collated_eq(&lval, &rval, compile_collation(left, right, ctx.col_map))?
                }
            };
            Ok(Value::Boolean(if *negated { alike } else { !alike }))
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
            eval_cast_with_cancel(&val, *data_type, ctx.cancel)
        }

        Expr::Collate { expr: e, .. } => eval_expr(e, ctx),

        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::ScalarSubquery(_) => Err(
            SqlError::Unsupported("subquery not materialized (internal error)".into()),
        ),

        Expr::Parameter(n) => resolve_parameter(*n, ctx.params),

        Expr::WindowFunction { .. } => Err(SqlError::Unsupported(
            "window functions are only allowed in SELECT columns".into(),
        )),

        Expr::TypedNullRecord(_) => Ok(Value::Null),

        Expr::ArrayLiteral(elems) => {
            let mut out = Vec::with_capacity(elems.len());
            for e in elems {
                out.push(eval_expr(e, ctx)?);
            }
            Ok(Value::Array(std::sync::Arc::new(out)))
        }

        Expr::Quantified {
            left,
            op,
            quantifier,
            right,
        } => eval_quantified(left, *op, *quantifier, right, ctx),
    }
}

fn eval_quantified(
    left: &Expr,
    op: crate::parser::BinOp,
    quantifier: crate::parser::Quantifier,
    right: &crate::parser::QuantifiedRhs,
    ctx: &EvalCtx,
) -> Result<Value> {
    use crate::parser::{QuantifiedRhs, Quantifier};
    let lhs = eval_expr(left, ctx)?;
    let elems: Vec<Value> = match right {
        QuantifiedRhs::Array(e) => match eval_expr(e, ctx)? {
            Value::Array(a) => (*a).clone(),
            Value::Null => return Ok(Value::Null),
            other => {
                return Err(SqlError::TypeMismatch {
                    expected: "ARRAY".into(),
                    got: other.data_type().to_string(),
                });
            }
        },
        QuantifiedRhs::Subquery(_) => {
            return Err(SqlError::Unsupported(
                "ANY/ALL subquery not materialized (internal error)".into(),
            ));
        }
    };

    if lhs.is_null() {
        return if elems.is_empty() {
            match quantifier {
                Quantifier::Any => Ok(Value::Boolean(false)),
                Quantifier::All => Ok(Value::Boolean(true)),
            }
        } else {
            Ok(Value::Null)
        };
    }

    let mut any_unknown = false;
    let mut any_match = false;
    let mut any_mismatch = false;
    for elem in &elems {
        if elem.is_null() {
            any_unknown = true;
            continue;
        }
        let result = eval_binary_compare(&lhs, op, elem)?;
        match result {
            Value::Boolean(true) => any_match = true,
            Value::Boolean(false) => any_mismatch = true,
            Value::Null => any_unknown = true,
            _ => {
                return Err(SqlError::TypeMismatch {
                    expected: "BOOLEAN".into(),
                    got: result.data_type().to_string(),
                });
            }
        }
    }

    match quantifier {
        Quantifier::Any => {
            if any_match {
                Ok(Value::Boolean(true))
            } else if any_unknown {
                Ok(Value::Null)
            } else {
                Ok(Value::Boolean(false))
            }
        }
        Quantifier::All => {
            if any_mismatch {
                Ok(Value::Boolean(false))
            } else if any_unknown {
                Ok(Value::Null)
            } else {
                Ok(Value::Boolean(true))
            }
        }
    }
}

fn eval_binary_compare(left: &Value, op: crate::parser::BinOp, right: &Value) -> Result<Value> {
    use crate::parser::BinOp;
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    let cmp = match (left, right) {
        (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
        _ => left.partial_cmp(right),
    };
    let Some(cmp) = cmp else {
        return Ok(Value::Null);
    };
    use std::cmp::Ordering;
    let result = match op {
        BinOp::Eq => cmp == Ordering::Equal,
        BinOp::NotEq => cmp != Ordering::Equal,
        BinOp::Lt => cmp == Ordering::Less,
        BinOp::Gt => cmp == Ordering::Greater,
        BinOp::LtEq => cmp != Ordering::Greater,
        BinOp::GtEq => cmp != Ordering::Less,
        _ => {
            return Err(SqlError::Unsupported(format!(
                "ANY/ALL comparison op {op:?}"
            )));
        }
    };
    Ok(Value::Boolean(result))
}

pub(crate) fn collation_of(expr: &Expr) -> Option<crate::types::Collation> {
    match expr {
        Expr::Collate { collation, .. } => Some(*collation),
        Expr::BinaryOp { left, right, .. } | Expr::IsDistinctFrom { left, right, .. } => {
            collation_of(left).or_else(|| collation_of(right))
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. }
        | Expr::InSubquery { expr, .. }
        | Expr::InSet { expr, .. } => collation_of(expr),
        Expr::Function { args, .. }
        | Expr::Coalesce(args)
        | Expr::ArrayLiteral(args)
        | Expr::WindowFunction { args, .. } => args.iter().find_map(collation_of),
        Expr::InList { expr, list, .. } => {
            collation_of(expr).or_else(|| list.iter().find_map(collation_of))
        }
        Expr::Between {
            expr, low, high, ..
        } => collation_of(expr)
            .or_else(|| collation_of(low))
            .or_else(|| collation_of(high)),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => collation_of(expr)
            .or_else(|| collation_of(pattern))
            .or_else(|| escape.as_deref().and_then(collation_of)),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => operand
            .as_deref()
            .and_then(collation_of)
            .or_else(|| {
                conditions.iter().find_map(|(condition, result)| {
                    collation_of(condition).or_else(|| collation_of(result))
                })
            })
            .or_else(|| else_result.as_deref().and_then(collation_of)),
        Expr::Quantified { left, right, .. } => collation_of(left).or_else(|| match right {
            QuantifiedRhs::Array(expr) => collation_of(expr),
            QuantifiedRhs::Subquery(_) => None,
        }),
        Expr::Literal(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. }
        | Expr::CountStar
        | Expr::Exists { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Parameter(_)
        | Expr::TypedNullRecord(_) => None,
    }
}

fn column_collation(expr: &Expr, col_map: &ColumnMap) -> Option<crate::types::Collation> {
    match expr {
        Expr::Column(name) => col_map.resolve(name).ok().map(|i| col_map.collation_at(i)),
        Expr::QualifiedColumn { table, column } => col_map
            .resolve_qualified(table, column)
            .ok()
            .map(|i| col_map.collation_at(i)),
        // A CAST-wrapped column still counts as a column for implicit collation;
        // Neg/Not must not inherit it.
        Expr::Cast { expr, .. } => column_collation(expr, col_map),
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

/// Compare under `coll`, falling back to binary when it does not apply. Every comparison in
/// this module routes here, so a form that forgets its collation is one that does not call it.
fn collated_compare(
    left: &Value,
    op: BinOp,
    right: &Value,
    coll: Option<crate::types::Collation>,
) -> Result<Value> {
    collated_compare_with_cancel(left, op, right, coll, None)
}

fn collated_compare_with_cancel(
    left: &Value,
    op: BinOp,
    right: &Value,
    coll: Option<crate::types::Collation>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    if let Some(c) = coll {
        if let Some(b) = eval_text_compare(left, op, right, c) {
            return Ok(Value::Boolean(b));
        }
    }
    eval_binary_op_with_cancel(left, op, right, cancel)
}

/// Equality under `coll`, including the temporal normalization used by the `=` operator.
pub(crate) fn collated_eq(
    left: &Value,
    right: &Value,
    coll: Option<crate::types::Collation>,
) -> Result<bool> {
    Ok(matches!(
        collated_compare(left, BinOp::Eq, right, coll)?,
        Value::Boolean(true)
    ))
}

pub fn eval_binary_op_public(left: &Value, op: BinOp, right: &Value) -> Result<Value> {
    eval_binary_op(left, op, right)
}

fn eval_binary_op(left: &Value, op: BinOp, right: &Value) -> Result<Value> {
    eval_binary_op_with_cancel(left, op, right, None)
}

pub(crate) fn eval_binary_op_with_cancel(
    left: &Value,
    op: BinOp,
    right: &Value,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
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
        BinOp::Sub => match left {
            Value::Json(_) | Value::Jsonb(_) => {
                crate::json::op_delete_one_with_cancel(left, right, cancel)
            }
            _ => eval_arithmetic(left, right, i64::checked_sub, |a, b| a - b),
        },
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
        BinOp::Concat => match (left, right) {
            (Value::TsVector(a), Value::TsVector(b)) => {
                crate::fts::op_concat_with_cancel(a, b, cancel)
            }
            (Value::Json(_) | Value::Jsonb(_), _) | (_, Value::Json(_) | Value::Jsonb(_)) => {
                crate::json::op_concat_with_cancel(left, right, cancel)
            }
            _ => {
                let ls = value_to_text_with_cancel(left, cancel)?;
                let rs = value_to_text_with_cancel(right, cancel)?;
                Ok(Value::Text(format!("{ls}{rs}").into()))
            }
        },
        BinOp::JsonGet
        | BinOp::JsonGetText
        | BinOp::JsonPath
        | BinOp::JsonPathText
        | BinOp::JsonContains
        | BinOp::JsonContainedBy
        | BinOp::JsonHasKey
        | BinOp::JsonHasAnyKey
        | BinOp::JsonHasAllKeys
        | BinOp::JsonDeletePath
        | BinOp::JsonPathExists
        | BinOp::JsonPathMatch
        | BinOp::JsonPathExistsTz
        | BinOp::JsonPathMatchTz => eval_json_binary_op_with_cancel(left, op, right, cancel),
        BinOp::VectorL2 => eval_vector_distance(left, right, VectorMetric::L2),
        BinOp::VectorInner => eval_vector_distance(left, right, VectorMetric::Inner),
        BinOp::VectorCosine => eval_vector_distance(left, right, VectorMetric::Cosine),
        BinOp::And | BinOp::Or => unreachable!(),
    }
}

#[cold]
fn eval_json_binary_op_with_cancel(
    left: &Value,
    op: BinOp,
    right: &Value,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    match op {
        BinOp::JsonGet => crate::json::op_get_with_cancel(left, right, cancel),
        BinOp::JsonGetText => crate::json::op_get_text_with_cancel(left, right, cancel),
        BinOp::JsonPath => crate::json::op_path_with_cancel(left, right, cancel),
        BinOp::JsonPathText => crate::json::op_path_text_with_cancel(left, right, cancel),
        BinOp::JsonContains => crate::json::op_contains_with_cancel(left, right, cancel),
        BinOp::JsonContainedBy => crate::json::op_contained_by_with_cancel(left, right, cancel),
        BinOp::JsonHasKey => crate::json::op_has_key_with_cancel(left, right, cancel),
        BinOp::JsonHasAnyKey => crate::json::op_has_any_key_with_cancel(left, right, cancel),
        BinOp::JsonHasAllKeys => crate::json::op_has_all_keys_with_cancel(left, right, cancel),
        BinOp::JsonDeletePath => crate::json::op_delete_path_with_cancel(left, right, cancel),
        BinOp::JsonPathExists => crate::json::op_path_exists_with_cancel(left, right, cancel),
        BinOp::JsonPathMatch => eval_at_at_with_cancel(left, right, cancel),
        BinOp::JsonPathExistsTz => {
            crate::json::fn_jsonb_path_exists_tz_with_cancel(&[left.clone(), right.clone()], cancel)
        }
        BinOp::JsonPathMatchTz => {
            crate::json::fn_jsonb_path_match_tz_with_cancel(&[left.clone(), right.clone()], cancel)
        }
        BinOp::VectorL2 => eval_vector_distance(left, right, VectorMetric::L2),
        BinOp::VectorInner => eval_vector_distance(left, right, VectorMetric::Inner),
        BinOp::VectorCosine => eval_vector_distance(left, right, VectorMetric::Cosine),
        _ => unreachable!(),
    }
}

#[derive(Copy, Clone)]
enum VectorMetric {
    L2,
    Inner,
    Cosine,
}

fn eval_vector_distance(left: &Value, right: &Value, metric: VectorMetric) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    let (a, b) = match (left, right) {
        (Value::Vector(a), Value::Vector(b)) => (a.as_ref(), b.as_ref()),
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "VECTOR".into(),
                got: format!("{} vs {}", left.data_type(), right.data_type()),
            });
        }
    };
    if a.len() != b.len() {
        return Err(SqlError::InvalidValue(format!(
            "vector dimension mismatch: {} vs {}",
            a.len(),
            b.len()
        )));
    }
    let d = match metric {
        VectorMetric::L2 => {
            let mut sum = 0.0f64;
            for (x, y) in a.iter().zip(b.iter()) {
                let diff = (*x as f64) - (*y as f64);
                sum += diff * diff;
            }
            sum.sqrt()
        }
        VectorMetric::Inner => {
            let mut sum = 0.0f64;
            for (x, y) in a.iter().zip(b.iter()) {
                sum += (*x as f64) * (*y as f64);
            }
            -sum
        }
        VectorMetric::Cosine => {
            let mut dot = 0.0f64;
            let mut na = 0.0f64;
            let mut nb = 0.0f64;
            for (x, y) in a.iter().zip(b.iter()) {
                let xf = *x as f64;
                let yf = *y as f64;
                dot += xf * yf;
                na += xf * xf;
                nb += yf * yf;
            }
            let denom = na.sqrt() * nb.sqrt();
            if denom == 0.0 {
                return Ok(Value::Null);
            }
            1.0 - dot / denom
        }
    };
    Ok(Value::Real(d))
}

fn eval_at_at_with_cancel(
    left: &Value,
    right: &Value,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    use crate::types::DataType as D;
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    match (left.data_type(), right.data_type()) {
        (D::Json | D::Jsonb, D::Text) => {
            crate::json::op_path_match_with_cancel(left, right, cancel)
        }
        (D::TsVector, D::TsQuery) => match (left, right) {
            (Value::TsVector(v), Value::TsQuery(q)) => {
                crate::fts::op_match_with_cancel(v, q, cancel)
            }
            _ => unreachable!(),
        },
        (D::TsQuery, D::TsVector) => match (left, right) {
            (Value::TsQuery(q), Value::TsVector(v)) => {
                crate::fts::op_match_with_cancel(v, q, cancel)
            }
            _ => unreachable!(),
        },
        (D::Text, D::TsQuery) => {
            let s = match left {
                Value::Text(s) => s.as_str(),
                _ => unreachable!(),
            };
            let lhs = crate::fts::fn_to_tsvector_with_cancel(
                crate::fts::TokenizerKind::English,
                s,
                cancel,
            )?;
            eval_at_at_with_cancel(&lhs, right, cancel)
        }
        (D::TsVector, D::Text) => {
            let s = match right {
                Value::Text(s) => s.as_str(),
                _ => unreachable!(),
            };
            let rhs = crate::fts::fn_plainto_tsquery_with_cancel(
                crate::fts::TokenizerKind::English,
                s,
                cancel,
            )?;
            eval_at_at_with_cancel(left, &rhs, cancel)
        }
        (D::Text, D::Text) => {
            let ls = match left {
                Value::Text(s) => s.as_str(),
                _ => unreachable!(),
            };
            let rs = match right {
                Value::Text(s) => s.as_str(),
                _ => unreachable!(),
            };
            let lhs = crate::fts::fn_to_tsvector_with_cancel(
                crate::fts::TokenizerKind::English,
                ls,
                cancel,
            )?;
            let rhs = crate::fts::fn_plainto_tsquery_with_cancel(
                crate::fts::TokenizerKind::English,
                rs,
                cancel,
            )?;
            eval_at_at_with_cancel(&lhs, &rhs, cancel)
        }
        (lt, rt) => Err(SqlError::TypeMismatch {
            expected: "JSONB @@ text, tsvector @@ tsquery".into(),
            got: format!("{lt} @@ {rt}"),
        }),
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
    if !is_temporal(left) && !is_temporal(right) {
        return None;
    }
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
        // Comparison with one temporal side: coerce the literal to that type.
        (l, op, r)
            if matches!(
                op,
                BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::Gt | BinOp::LtEq | BinOp::GtEq
            ) =>
        {
            temporal_compare(l, op, r)
        }
        _ => None,
    }
}

/// Compares values where one side is temporal, coercing the other to match.
fn temporal_compare(left: &Value, op: BinOp, right: &Value) -> Option<Result<Value>> {
    let (a, b) = coerce_temporal_pair(left, right)?;
    let ord = a.cmp(&b);
    use std::cmp::Ordering;
    let result = match op {
        BinOp::Eq => ord == Ordering::Equal,
        BinOp::NotEq => ord != Ordering::Equal,
        BinOp::Lt => ord == Ordering::Less,
        BinOp::Gt => ord == Ordering::Greater,
        BinOp::LtEq => ord != Ordering::Greater,
        BinOp::GtEq => ord != Ordering::Less,
        _ => return None,
    };
    Some(Ok(Value::Boolean(result)))
}

/// Coerces a TEXT/INTEGER (or DATE/TIMESTAMP) operand to match the temporal side.
fn coerce_temporal_pair(left: &Value, right: &Value) -> Option<(Value, Value)> {
    use crate::types::DataType;
    let temporal_type = |v: &Value| match v {
        Value::Date(_) => Some(DataType::Date),
        Value::Time(_) => Some(DataType::Time),
        Value::Timestamp(_) => Some(DataType::Timestamp),
        Value::Interval { .. } => Some(DataType::Interval),
        _ => None,
    };
    match (temporal_type(left), temporal_type(right)) {
        (Some(DataType::Date), Some(DataType::Timestamp))
        | (Some(DataType::Timestamp), Some(DataType::Date)) => Some((
            left.clone().coerce_into(DataType::Timestamp)?,
            right.clone().coerce_into(DataType::Timestamp)?,
        )),
        (Some(_), Some(_)) => None,
        (Some(t), None) => {
            let coerced = right.clone().coerce_into(t)?;
            Some((left.clone(), coerced))
        }
        (None, Some(t)) => {
            let coerced = left.clone().coerce_into(t)?;
            Some((coerced, right.clone()))
        }
        (None, None) => None,
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

/// `x IN (a, b)` is `x = a OR x = b`, so each item collates exactly as its own `=` would.
fn eval_in_values(
    lhs_expr: &Expr,
    lhs: &Value,
    list: &[Expr],
    ctx: &EvalCtx,
    negated: bool,
) -> Result<Value> {
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
        } else if collated_eq(lhs, &rhs, compile_collation(lhs_expr, item, ctx.col_map))? {
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
    coll: Option<crate::types::Collation>,
) -> Result<Value> {
    if values.is_empty() && !has_null {
        return Ok(Value::Boolean(negated));
    }
    if lhs.is_null() {
        return Ok(Value::Null);
    }
    // The set is hashed on the raw value, so a collation that calls distinct bytes equal
    // cannot be answered by a lookup. Only a non-binary collation reaches the scan.
    let found = match (coll, lhs) {
        (Some(c), Value::Text(s)) => values.iter().any(|v| match v {
            Value::Text(t) => c.eq_text(s, t),
            _ => false,
        }),
        _ => values.contains(lhs),
    };
    if found {
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

fn value_to_text(val: &Value) -> Result<String> {
    Ok(match val {
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
        Value::Json(s) => s.to_string(),
        Value::Jsonb(b) => crate::json::decode_to_text(b)?,
        Value::TsVector(b) => crate::fts::tsvector_to_text_with_cancel(b, None)?,
        Value::TsQuery(b) => crate::fts::tsquery_to_text_with_cancel(b, None)?,
        Value::Array(values) => array_to_text_with_cancel(values, None)?,
        Value::Vector(_) => val.to_string(),
    })
}

fn value_to_text_with_cancel(
    value: &Value,
    cancel: Option<&citadel::CancelToken>,
) -> Result<String> {
    match value {
        Value::Jsonb(bytes) => crate::json::decode_to_text_with_cancel(bytes, cancel),
        Value::TsVector(bytes) => crate::fts::tsvector_to_text_with_cancel(bytes, cancel),
        Value::TsQuery(bytes) => crate::fts::tsquery_to_text_with_cancel(bytes, cancel),
        Value::Array(values) => array_to_text_with_cancel(values, cancel),
        _ => value_to_text(value),
    }
}

/// Match SQL array Display formatting without turning malformed nested values
/// into a diagnostic placeholder that a SQL statement could persist as data.
fn array_to_text_with_cancel(
    values: &[Value],
    cancel: Option<&citadel::CancelToken>,
) -> Result<String> {
    let mut text = String::from("{");
    for (index, value) in values.iter().enumerate() {
        if let Some(cancel) = cancel {
            cancel.check().map_err(SqlError::Storage)?;
        }
        if index != 0 {
            text.push(',');
        }
        match value {
            Value::Text(value) => {
                text.push('"');
                text.push_str(&value.replace('\\', "\\\\").replace('"', "\\\""));
                text.push('"');
            }
            Value::Jsonb(_) | Value::TsVector(_) | Value::TsQuery(_) => {
                text.push_str(&value_to_text_with_cancel(value, cancel)?);
            }
            Value::Array(values) => text.push_str(&array_to_text_with_cancel(values, cancel)?),
            other => text.push_str(&other.to_string()),
        }
    }
    text.push('}');
    Ok(text)
}

/// `x BETWEEN lo AND hi` is `x >= lo AND x <= hi`, so each bound collates as its own
/// comparison would.
fn eval_between(
    val: &Value,
    low: &Value,
    high: &Value,
    negated: bool,
    low_coll: Option<crate::types::Collation>,
    high_coll: Option<crate::types::Collation>,
) -> Result<Value> {
    let ge = collated_compare(val, BinOp::GtEq, low, low_coll)?;
    let le = collated_compare(val, BinOp::LtEq, high, high_coll)?;

    let result = match (as_bool(&ge), as_bool(&le)) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    };

    match result {
        Some(b) => Ok(Value::Boolean(if negated { !b } else { b })),
        None => Ok(Value::Null),
    }
}

fn as_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Boolean(b) => Some(*b),
        _ => None,
    }
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
        // `CASE x WHEN c` is `x = c`, down to the collation that comparison would use.
        let op_val = eval_expr(op_expr, ctx)?;
        for (cond, result) in conditions {
            let cond_val = eval_expr(cond, ctx)?;
            if !op_val.is_null()
                && !cond_val.is_null()
                && collated_eq(
                    &op_val,
                    &cond_val,
                    compile_collation(op_expr, cond, ctx.col_map),
                )?
            {
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

pub(crate) fn eval_cast(val: &Value, target: DataType) -> Result<Value> {
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
        DataType::Text => Ok(Value::Text(value_to_text(val)?.into())),
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
        DataType::Json => val.clone().coerce_into(DataType::Json).ok_or_else(|| {
            SqlError::InvalidValue(format!("cannot cast {} to JSON", val.data_type()))
        }),
        DataType::Jsonb => val.clone().coerce_into(DataType::Jsonb).ok_or_else(|| {
            SqlError::InvalidValue(format!("cannot cast {} to JSONB", val.data_type()))
        }),
        DataType::TsVector => val.clone().coerce_into(DataType::TsVector).ok_or_else(|| {
            SqlError::InvalidValue(format!("cannot cast {} to TSVECTOR", val.data_type()))
        }),
        DataType::TsQuery => val.clone().coerce_into(DataType::TsQuery).ok_or_else(|| {
            SqlError::InvalidValue(format!("cannot cast {} to TSQUERY", val.data_type()))
        }),
        DataType::Array => val.clone().coerce_into(DataType::Array).ok_or_else(|| {
            SqlError::InvalidValue(format!("cannot cast {} to ARRAY", val.data_type()))
        }),
        DataType::Vector { dim } => match val {
            Value::Vector(v) if v.len() as u16 == dim => Ok(val.clone()),
            Value::Vector(_) => Err(SqlError::InvalidValue(format!(
                "cannot cast {} to VECTOR({dim}) (dim mismatch)",
                val.data_type()
            ))),
            Value::Text(s) => parse_vector_literal(s.as_str(), dim).map(Value::Vector),
            _ => Err(SqlError::InvalidValue(format!(
                "cannot cast {} to VECTOR({dim})",
                val.data_type()
            ))),
        },
    }
}

fn eval_cast_with_cancel(
    value: &Value,
    target: DataType,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return eval_cast(value, target);
    };
    cancel.check().map_err(SqlError::Storage)?;
    if value.is_null() {
        return Ok(Value::Null);
    }

    let converted = match (value, target) {
        (Value::Text(text), DataType::Json) => {
            normalize_json_cast(
                crate::json::validate_text_with_cancel(text, Some(cancel)),
                value,
                target,
            )?;
            Value::Json(
                normalize_json_cast(
                    crate::json::clone_text_with_cancel(text, Some(cancel)),
                    value,
                    target,
                )?
                .into(),
            )
        }
        (Value::Json(text), DataType::Json) => Value::Json(
            normalize_json_cast(
                crate::json::clone_text_with_cancel(text, Some(cancel)),
                value,
                target,
            )?
            .into(),
        ),
        (Value::Jsonb(bytes), DataType::Json) => Value::Json(
            normalize_json_cast(
                crate::json::decode_to_text_with_cancel(bytes, Some(cancel)),
                value,
                target,
            )?
            .into(),
        ),
        (Value::Text(text), DataType::Jsonb) | (Value::Json(text), DataType::Jsonb) => {
            normalize_json_cast(
                crate::json::text_to_jsonb_with_cancel(text, Some(cancel)),
                value,
                target,
            )?
        }
        (Value::Jsonb(_), DataType::Jsonb) => value.clone(),
        (Value::Json(text), DataType::Text) => Value::Text(
            normalize_json_cast(
                crate::json::clone_text_with_cancel(text, Some(cancel)),
                value,
                target,
            )?
            .into(),
        ),
        (Value::Jsonb(bytes), DataType::Text) => Value::Text(
            normalize_json_cast(
                crate::json::decode_to_text_with_cancel(bytes, Some(cancel)),
                value,
                target,
            )?
            .into(),
        ),
        (Value::TsVector(bytes), DataType::Text) => {
            Value::Text(crate::fts::tsvector_to_text_with_cancel(bytes, Some(cancel))?.into())
        }
        (Value::TsQuery(bytes), DataType::Text) => {
            Value::Text(crate::fts::tsquery_to_text_with_cancel(bytes, Some(cancel))?.into())
        }
        (Value::Array(values), DataType::Text) => {
            Value::Text(array_to_text_with_cancel(values, Some(cancel))?.into())
        }
        _ => return eval_cast(value, target),
    };
    cancel.check().map_err(SqlError::Storage)?;
    Ok(converted)
}

fn normalize_json_cast<T>(result: Result<T>, value: &Value, target: DataType) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error @ SqlError::Storage(citadel_core::Error::Interrupted)) => Err(error),
        Err(_) => Err(SqlError::InvalidValue(format!(
            "cannot cast {} to {target}",
            value.data_type()
        ))),
    }
}

fn parse_vector_literal(s: &str, expected_dim: u16) -> Result<std::sync::Arc<[f32]>> {
    let trimmed = s.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(trimmed);
    let mut out: Vec<f32> = Vec::with_capacity(expected_dim as usize);
    for tok in inner.split(',') {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        let x: f32 = tok
            .parse()
            .map_err(|_| SqlError::InvalidValue(format!("invalid vector element: '{tok}'")))?;
        out.push(x);
    }
    if out.len() as u16 != expected_dim {
        return Err(SqlError::InvalidValue(format!(
            "vector literal has {} elements, expected {expected_dim}",
            out.len()
        )));
    }
    Ok(std::sync::Arc::from(out.into_boxed_slice()))
}

/// Scalar functions whose result can differ across statements for identical
/// arguments (wall clock, RNG, tzdb). Single source of truth for volatility;
/// any function added to `eval_scalar_function` MUST be classified here.
pub(crate) fn is_volatile_function(name_upper: &str, argc: usize) -> bool {
    match name_upper {
        "RANDOM"
        | "RAND"
        | "NOW"
        | "CURRENT_TIMESTAMP"
        | "LOCALTIMESTAMP"
        | "CURRENT_DATE"
        | "CURRENT_TIME"
        | "LOCALTIME"
        | "CLOCK_TIMESTAMP"
        | "STATEMENT_TIMESTAMP"
        | "TRANSACTION_TIMESTAMP"
        | "AT_TIMEZONE" => true,
        // 1-arg AGE measures from the current clock; 2-arg AGE is pure.
        "AGE" => argc == 1,
        _ => false,
    }
}

/// Name volatility plus the DATE/TIME/DATETIME clock forms (zero-arg or a
/// literal 'now'). A non-literal arg can reach 'now' via TEXT data at runtime;
/// callers needing that guarantee must exclude those shapes themselves.
pub(crate) fn is_volatile_function_expr(name_upper: &str, args: &[Expr]) -> bool {
    if is_volatile_function(name_upper, args.len()) {
        return true;
    }
    matches!(name_upper, "DATE" | "TIME" | "DATETIME")
        && match args.first() {
            None => true,
            Some(Expr::Literal(Value::Text(s))) => s.trim().eq_ignore_ascii_case("now"),
            Some(_) => false,
        }
}

/// Function-local proof that evaluation is independent of the session zone,
/// transaction/statement/live clocks, and JSONPath date context. Callers must
/// also prove every argument; runtime text that can become `now` fails closed.
/// RNG/tzdb volatility is conservatively excluded by the same shared rules.
pub(crate) fn is_function_context_independent(name_upper: &str, args: &[Expr]) -> bool {
    !is_volatile_function_expr(name_upper, args)
        && !is_session_dependent_jsonpath_function(name_upper, args)
        && (!matches!(name_upper, "DATE" | "TIME" | "DATETIME")
            || matches!(args.first(), Some(Expr::Literal(_))))
}

fn literal_jsonpath_text(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(Value::Text(path)) => Some(path),
        Expr::Cast {
            expr,
            data_type: DataType::Text,
        }
        | Expr::Collate { expr, .. } => literal_jsonpath_text(expr),
        _ => None,
    }
}

fn jsonpath_argument_depends_on_session_context(expr: Option<&Expr>) -> bool {
    let Some(path) = expr.and_then(literal_jsonpath_text) else {
        // Parameters, columns and computed paths can contain a temporal method
        // at runtime, so callers that need an immutable/cacheable answer must
        // fail closed.
        return true;
    };
    sql_json_path::JsonPath::new(path)
        .map(|path| path.depends_on_session_context_without_tz())
        // An invalid literal will fail at execution; treating it as dependent
        // keeps validation conservative without duplicating parser errors here.
        .unwrap_or(true)
}

/// Whether a SQL/JSON function call can depend on the session time zone or
/// transaction date.
///
/// `_TZ` entry points are always classified as dependent. Standard entry
/// points inspect their literal path; dynamic paths fail closed.
pub(crate) fn is_session_dependent_jsonpath_function(name_upper: &str, args: &[Expr]) -> bool {
    if matches!(
        name_upper,
        "JSONB_PATH_EXISTS_TZ"
            | "JSONB_PATH_MATCH_TZ"
            | "JSONB_PATH_QUERY_TZ"
            | "JSONB_PATH_QUERY_FIRST_TZ"
            | "JSONB_PATH_QUERY_ARRAY_TZ"
    ) {
        return true;
    }
    matches!(
        name_upper,
        "JSON_EXISTS"
            | "JSON_VALUE"
            | "JSON_QUERY"
            | "JSONB_PATH_EXISTS"
            | "JSONB_PATH_MATCH"
            | "JSONB_PATH_QUERY_FIRST"
            | "JSONB_PATH_QUERY_ARRAY"
    ) && jsonpath_argument_depends_on_session_context(args.get(1))
}

/// A successful non-NULL result type, when it is fixed without schema binding.
fn intrinsic_result_type(expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::Literal(value) => Some(value.data_type()),
        Expr::Cast { data_type, .. } => Some(*data_type),
        Expr::Collate { expr, .. } => intrinsic_result_type(expr),
        Expr::Function { name, .. } => {
            text_search::Constructor::from_name(name).map(|constructor| constructor.result_type())
        }
        _ => None,
    }
}

/// Whether an expression can be evaluated once for a statement without a row.
/// Parameters are fixed for the statement; contextual functions and expression
/// forms without a conservative proof must stay in the ordinary evaluator.
pub(crate) fn is_statement_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) | Expr::TypedNullRecord(_) => true,
        Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::InSet { expr, .. } => is_statement_constant(expr),
        Expr::BinaryOp { left, op, right } => {
            !is_session_dependent_jsonpath_op(op, left, right)
                && is_statement_constant(left)
                && is_statement_constant(right)
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            is_statement_constant(left) && is_statement_constant(right)
        }
        Expr::Function { name, args, .. } => {
            let upper = name.to_ascii_uppercase();
            is_function_context_independent(&upper, args) && args.iter().all(is_statement_constant)
        }
        Expr::Coalesce(args) | Expr::ArrayLiteral(args) => args.iter().all(is_statement_constant),
        Expr::InList { expr, list, .. } => {
            is_statement_constant(expr) && list.iter().all(is_statement_constant)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            is_statement_constant(expr) && is_statement_constant(low) && is_statement_constant(high)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            is_statement_constant(expr)
                && is_statement_constant(pattern)
                && escape.as_deref().is_none_or(is_statement_constant)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand.as_deref().is_none_or(is_statement_constant)
                && conditions.iter().all(|(condition, result)| {
                    is_statement_constant(condition) && is_statement_constant(result)
                })
                && else_result.as_deref().is_none_or(is_statement_constant)
        }
        Expr::Quantified {
            left,
            op,
            right: QuantifiedRhs::Array(right),
            ..
        } => {
            // The operator sees each array element, not the ARRAY value, so
            // the array's type cannot prove an overloaded operator safe.
            matches!(
                op,
                BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq
            ) && is_statement_constant(left)
                && is_statement_constant(right)
        }
        _ => false,
    }
}

/// Operator-local dependency; callers must also check both child expressions.
pub(crate) fn is_session_dependent_jsonpath_op(op: &BinOp, left: &Expr, right: &Expr) -> bool {
    match op {
        BinOp::JsonPathExistsTz | BinOp::JsonPathMatchTz => true,
        BinOp::JsonPathMatch
            if intrinsic_result_type(left)
                .is_some_and(|ty| !matches!(ty, DataType::Json | DataType::Jsonb))
                || intrinsic_result_type(right).is_some_and(|ty| ty != DataType::Text) =>
        {
            // Only JSON/JSONB @@ TEXT dispatches to JSONPath; other pairs use
            // full-text matching, propagate NULL, or return a type error.
            false
        }
        BinOp::JsonPathExists | BinOp::JsonPathMatch => {
            jsonpath_argument_depends_on_session_context(Some(right))
        }
        _ => false,
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
                Value::TsVector(b) => crate::fts::fn_length_tsvector_with_cancel(b, ctx.cancel),
                _ => Ok(Value::Integer(
                    value_to_text_with_cancel(&evaluated[0], ctx.cancel)?
                        .chars()
                        .count() as i64,
                )),
            }
        }
        "UPPER" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(s.to_ascii_uppercase())),
                _ => Ok(Value::Text(
                    value_to_text_with_cancel(&evaluated[0], ctx.cancel)?
                        .to_ascii_uppercase()
                        .into(),
                )),
            }
        }
        "LOWER" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(s.to_ascii_lowercase())),
                _ => Ok(Value::Text(
                    value_to_text_with_cancel(&evaluated[0], ctx.cancel)?
                        .to_ascii_lowercase()
                        .into(),
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
            let s = value_to_text_with_cancel(&evaluated[0], ctx.cancel)?;
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
            let s = value_to_text_with_cancel(&evaluated[0], ctx.cancel)?;
            let trim_chars: Vec<char> = if evaluated.len() == 2 {
                if evaluated[1].is_null() {
                    return Ok(Value::Null);
                }
                value_to_text_with_cancel(&evaluated[1], ctx.cancel)?
                    .chars()
                    .collect()
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
            let s = value_to_text_with_cancel(&evaluated[0], ctx.cancel)?;
            let from = value_to_text_with_cancel(&evaluated[1], ctx.cancel)?;
            let to = value_to_text_with_cancel(&evaluated[2], ctx.cancel)?;
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
            let haystack = value_to_text_with_cancel(&evaluated[0], ctx.cancel)?;
            let needle = value_to_text_with_cancel(&evaluated[1], ctx.cancel)?;
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
                    _ => result.push_str(&value_to_text_with_cancel(v, ctx.cancel)?),
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
            let mut hasher = DefaultHasher::new();
            crate::datetime::now_micros().hash(&mut hasher);
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
                Value::Json(_) => "json",
                Value::Jsonb(_) => "jsonb",
                Value::TsVector(_) => "tsvector",
                Value::TsQuery(_) => "tsquery",
                Value::Array(_) => "array",
                Value::Vector(_) => "vector",
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
                _ => Ok(Value::Text(
                    value_to_text_with_cancel(&evaluated[0], ctx.cancel)?.into(),
                )),
            }
        }
        "NOW" => {
            check_args(name, &evaluated, 0)?;
            Ok(Value::Timestamp(crate::datetime::txn_or_clock_micros()))
        }
        "CURRENT_TIMESTAMP" => {
            let precision = current_time_precision(name, &evaluated)?;
            let timestamp = crate::datetime::txn_or_clock_micros();
            precision
                .map(|precision| crate::datetime::round_time_precision(timestamp, precision))
                .transpose()
                .map(|rounded| Value::Timestamp(rounded.unwrap_or(timestamp)))
        }
        "LOCALTIMESTAMP" => {
            let precision = current_time_precision(name, &evaluated)?;
            let timestamp = crate::datetime::current_local_timestamp_micros()?;
            precision
                .map(|precision| crate::datetime::round_time_precision(timestamp, precision))
                .transpose()
                .map(|rounded| Value::Timestamp(rounded.unwrap_or(timestamp)))
        }
        "CURRENT_DATE" => {
            check_args(name, &evaluated, 0)?;
            crate::datetime::current_date_days().map(Value::Date)
        }
        "CURRENT_TIME" | "LOCALTIME" => {
            let precision = current_time_precision(name, &evaluated)?;
            let time = crate::datetime::current_local_time_micros()?;
            precision
                .map(|precision| crate::datetime::round_time_precision(time, precision))
                .transpose()
                .map(|rounded| Value::Time(rounded.unwrap_or(time)))
        }
        "CLOCK_TIMESTAMP" | "STATEMENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" => {
            check_args(name, &evaluated, 0)?;
            let ts = match name {
                "CLOCK_TIMESTAMP" => crate::datetime::now_micros(),
                "STATEMENT_TIMESTAMP" => crate::datetime::statement_or_clock_micros(),
                _ => crate::datetime::txn_or_clock_micros(),
            };
            Ok(Value::Timestamp(ts))
        }
        "EXTRACT" | "DATE_PART" | "DATEPART" => {
            check_args(name, &evaluated, 2)?;
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
                            return crate::datetime::date_trunc_timestamp_in_zone(&unit, *ts, tz)
                                .map(Value::Timestamp);
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
                    months,
                    days,
                    micros,
                } => {
                    if *months != 0 {
                        return Err(SqlError::Unsupported(
                            "DATE_BIN stride cannot contain months or years".into(),
                        ));
                    }
                    // Interval components and a valid final bin can exceed an
                    // i64 intermediate. Their complete range fits in i128.
                    i128::from(*days) * i128::from(crate::datetime::MICROS_PER_DAY)
                        + i128::from(*micros)
                }
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
            if crate::datetime::is_infinity_ts(src) {
                return Ok(Value::Timestamp(src));
            }
            if crate::datetime::is_infinity_ts(origin) {
                return Err(SqlError::InvalidValue(
                    "DATE_BIN origin must be finite".into(),
                ));
            }
            let source = i128::from(src);
            // Euclidean remainder rounds towards the beginning of the bin,
            // including when the origin is later than the source. Subtracting
            // the remainder avoids multiplying a potentially large quotient.
            let binned = source - (source - i128::from(origin)).rem_euclid(stride);
            // The endpoints are reserved infinity sentinels, not finite bins.
            if binned <= i128::from(i64::MIN) || binned >= i128::from(i64::MAX) {
                return Err(SqlError::InvalidValue(
                    "DATE_BIN result is out of timestamp range".into(),
                ));
            }
            Ok(Value::Timestamp(binned as i64))
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
                let today = crate::datetime::today_days()?;
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
                return crate::datetime::today_days().map(Value::Date);
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let d = match &evaluated[0] {
                Value::Date(d) => *d,
                Value::Timestamp(t) => crate::datetime::ts_to_date_floor(*t),
                Value::Text(s) if s.eq_ignore_ascii_case("now") => crate::datetime::today_days()?,
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
                return crate::datetime::current_time_micros().map(Value::Time);
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let t = match &evaluated[0] {
                Value::Time(t) => *t,
                Value::Timestamp(t) => crate::datetime::ts_split(*t).1,
                Value::Text(s) if s.eq_ignore_ascii_case("now") => {
                    crate::datetime::current_time_micros()?
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
                return crate::datetime::current_local_timestamp_micros().map(Value::Timestamp);
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let t = match &evaluated[0] {
                Value::Timestamp(t) => *t,
                Value::Date(d) => crate::datetime::date_to_ts(*d),
                Value::Text(s) if s.eq_ignore_ascii_case("now") => {
                    crate::datetime::current_local_timestamp_micros()?
                }
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
        "JSONB_TYPEOF" | "JSON_TYPEOF" => {
            check_args(name, &evaluated, 1)?;
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_typeof_with_cancel(&evaluated[0], ctx.cancel)
        }
        "JSONB_ARRAY_LENGTH" | "JSON_ARRAY_LENGTH" => {
            check_args(name, &evaluated, 1)?;
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_array_length_with_cancel(&evaluated[0], ctx.cancel)
        }
        "JSONB_OBJECT_LENGTH" | "JSON_OBJECT_LENGTH" => {
            check_args(name, &evaluated, 1)?;
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_object_length_with_cancel(&evaluated[0], ctx.cancel)
        }
        "JSONB_EXTRACT_PATH" | "JSON_EXTRACT_PATH" => {
            if evaluated.is_empty() {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires at least 1 argument"
                )));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let target = if name.eq_ignore_ascii_case("JSONB_EXTRACT_PATH") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            crate::json::fn_extract_path_with_cancel(&evaluated, target, false, ctx.cancel)
        }
        "JSONB_EXTRACT_PATH_TEXT" | "JSON_EXTRACT_PATH_TEXT" => {
            if evaluated.is_empty() {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires at least 1 argument"
                )));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_extract_path_with_cancel(
                &evaluated,
                crate::types::DataType::Text,
                true,
                ctx.cancel,
            )
        }
        "JSON_EXTRACT" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_sqlite_extract_with_cancel(&evaluated[0], &evaluated[1], ctx.cancel)
        }
        "JSON_VALID" => {
            check_args(name, &evaluated, 1)?;
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_valid_with_cancel(&evaluated[0], ctx.cancel)
        }
        "JSONB_STRIP_NULLS" | "JSON_STRIP_NULLS" => {
            check_args(name, &evaluated, 1)?;
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let target = if name.eq_ignore_ascii_case("JSONB_STRIP_NULLS") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            crate::json::fn_strip_nulls_with_cancel(&evaluated[0], target, ctx.cancel)
        }
        "JSONB_PRETTY" | "JSON_PRETTY" => {
            check_args(name, &evaluated, 1)?;
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_pretty_with_cancel(&evaluated[0], ctx.cancel)
        }
        "JSONB_BUILD_OBJECT" | "JSON_BUILD_OBJECT" => {
            let target = if name.eq_ignore_ascii_case("JSONB_BUILD_OBJECT") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            crate::json::fn_build_object_with_cancel(&evaluated, target, ctx.cancel)
        }
        "JSONB_BUILD_ARRAY" | "JSON_BUILD_ARRAY" => {
            let target = if name.eq_ignore_ascii_case("JSONB_BUILD_ARRAY") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            crate::json::fn_build_array_with_cancel(&evaluated, target, ctx.cancel)
        }
        "JSONB_SET" | "JSON_SET" => {
            if !(3..=4).contains(&evaluated.len()) {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 3 or 4 arguments"
                )));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let target = if name.eq_ignore_ascii_case("JSONB_SET") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            let create_missing = evaluated
                .get(3)
                .map(|v| matches!(v, Value::Boolean(true)))
                .unwrap_or(true);
            crate::json::fn_set_with_cancel(
                &evaluated[0],
                &evaluated[1],
                &evaluated[2],
                create_missing,
                target,
                ctx.cancel,
            )
        }
        "JSONB_INSERT" | "JSON_INSERT" => {
            if !(3..=4).contains(&evaluated.len()) {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 3 or 4 arguments"
                )));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let target = if name.eq_ignore_ascii_case("JSONB_INSERT") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            let insert_after = evaluated
                .get(3)
                .map(|v| matches!(v, Value::Boolean(true)))
                .unwrap_or(false);
            crate::json::fn_insert_with_cancel(
                &evaluated[0],
                &evaluated[1],
                &evaluated[2],
                insert_after,
                target,
                ctx.cancel,
            )
        }
        "TO_JSONB" | "TO_JSON" => {
            check_args(name, &evaluated, 1)?;
            let target = if name.eq_ignore_ascii_case("TO_JSONB") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            crate::json::fn_to_json_with_cancel(&evaluated[0], target, ctx.cancel)
        }
        "ROW_TO_JSON" | "ROW_TO_JSONB" => {
            check_args(name, &evaluated, 1)?;
            let target = if name.eq_ignore_ascii_case("ROW_TO_JSONB") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            crate::json::fn_to_json_with_cancel(&evaluated[0], target, ctx.cancel)
        }
        "JSON_OBJECT" => crate::json::fn_json_object_with_cancel(&evaluated, ctx.cancel),
        "JSON_EXISTS" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_json_exists_with_cancel(&evaluated[0], &evaluated[1], ctx.cancel)
        }
        "JSON_VALUE" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_json_value_with_cancel(&evaluated[0], &evaluated[1], ctx.cancel)
        }
        "JSON_QUERY" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_json_query_with_cancel(
                &evaluated[0],
                &evaluated[1],
                crate::types::DataType::Jsonb,
                ctx.cancel,
            )
        }
        "JSONB_PATH_EXISTS" => {
            check_min_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_jsonb_path_exists_with_cancel(&evaluated, ctx.cancel)
        }
        "JSONB_PATH_MATCH" => {
            check_min_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_jsonb_path_match_with_cancel(&evaluated, ctx.cancel)
        }
        "JSONB_PATH_QUERY_FIRST" => {
            check_min_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_jsonb_path_query_first_with_cancel(&evaluated, ctx.cancel)
        }
        "JSONB_PATH_QUERY_ARRAY" => {
            check_min_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_jsonb_path_query_array_with_cancel(&evaluated, ctx.cancel)
        }
        "JSONB_PATH_EXISTS_TZ" => {
            check_min_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_jsonb_path_exists_tz_with_cancel(&evaluated, ctx.cancel)
        }
        "JSONB_PATH_MATCH_TZ" => {
            check_min_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_jsonb_path_match_tz_with_cancel(&evaluated, ctx.cancel)
        }
        "JSONB_PATH_QUERY_TZ" => {
            check_min_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_jsonb_path_query_tz_with_cancel(&evaluated, ctx.cancel)
        }
        "JSONB_PATH_QUERY_FIRST_TZ" => {
            check_min_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_jsonb_path_query_first_tz_with_cancel(&evaluated, ctx.cancel)
        }
        "JSONB_PATH_QUERY_ARRAY_TZ" => {
            check_min_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::fn_jsonb_path_query_array_tz_with_cancel(&evaluated, ctx.cancel)
        }
        "JSONB_HAS_KEY" | "JSON_HAS_KEY" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::op_has_key_with_cancel(&evaluated[0], &evaluated[1], ctx.cancel)
        }
        "JSONB_HAS_ANY_KEY" | "JSON_HAS_ANY_KEY" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::op_has_any_key_with_cancel(&evaluated[0], &evaluated[1], ctx.cancel)
        }
        "JSONB_HAS_ALL_KEYS" | "JSON_HAS_ALL_KEYS" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() || evaluated[1].is_null() {
                return Ok(Value::Null);
            }
            crate::json::op_has_all_keys_with_cancel(&evaluated[0], &evaluated[1], ctx.cancel)
        }
        "TS_RANK" => fts_ts_rank(&evaluated, false, ctx.cancel),
        "TS_RANK_CD" => fts_ts_rank(&evaluated, true, ctx.cancel),
        "TS_HEADLINE" => fts_ts_headline(&evaluated, ctx.cancel),
        "TS_LEXIZE" => fts_ts_lexize(&evaluated, ctx.cancel),
        "NUMNODE" => fts_numnode(&evaluated, ctx.cancel),
        "SETWEIGHT" => fts_setweight(&evaluated, ctx.cancel),
        "STRIP" => fts_strip(&evaluated, ctx.cancel),
        _ => match text_search::Constructor::from_name(name) {
            Some(constructor) => constructor.evaluate(&evaluated, ctx.cancel),
            None => Err(SqlError::Unsupported(format!("scalar function: {name}"))),
        },
    }
}

fn fts_ts_rank(
    args: &[Value],
    cover_density: bool,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    let fname = if cover_density {
        "ts_rank_cd"
    } else {
        "ts_rank"
    };
    if args.len() != 2 && args.len() != 3 {
        return Err(SqlError::InvalidValue(format!(
            "{fname} requires 2 or 3 arguments"
        )));
    }
    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::Null);
    }
    let tsv = match &args[0] {
        Value::TsVector(b) => b,
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TSVECTOR".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    let tsq = match &args[1] {
        Value::TsQuery(b) => b,
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TSQUERY".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    let norm = if args.len() == 3 {
        match &args[2] {
            Value::Integer(n) => *n,
            Value::Null => return Ok(Value::Null),
            v => {
                return Err(SqlError::TypeMismatch {
                    expected: "INTEGER (norm)".into(),
                    got: v.data_type().to_string(),
                })
            }
        }
    } else {
        0
    };
    if cover_density {
        crate::fts::fn_ts_rank_cd_with_cancel(tsv, tsq, norm, cancel)
    } else {
        crate::fts::fn_ts_rank_with_cancel(tsv, tsq, norm, cancel)
    }
}

fn fts_ts_headline(args: &[Value], cancel: Option<&citadel::CancelToken>) -> Result<Value> {
    if args.len() < 2 || args.len() > 4 {
        return Err(SqlError::InvalidValue(
            "ts_headline requires 2 to 4 arguments".into(),
        ));
    }
    if args.iter().any(|v| v.is_null()) {
        return Ok(Value::Null);
    }
    let kind = if args.len() >= 3 {
        match &args[0] {
            Value::Text(s) => crate::fts::TokenizerKind::from_name(s.as_str())?,
            v => {
                return Err(SqlError::TypeMismatch {
                    expected: "TEXT (config)".into(),
                    got: v.data_type().to_string(),
                })
            }
        }
    } else {
        crate::fts::TokenizerKind::English
    };
    let text_idx = if args.len() >= 3 { 1 } else { 0 };
    let tsq_idx = if args.len() >= 3 { 2 } else { 1 };
    let text = match args.get(text_idx) {
        Some(Value::Text(s)) => s.as_str(),
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT".into(),
                got: "non-text".into(),
            })
        }
    };
    let tsq = match args.get(tsq_idx) {
        Some(Value::TsQuery(b)) => b.as_ref(),
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "TSQUERY".into(),
                got: "non-tsquery".into(),
            })
        }
    };
    crate::fts::fn_ts_headline_with_cancel(kind, text, tsq, cancel)
}

fn fts_ts_lexize(args: &[Value], cancel: Option<&citadel::CancelToken>) -> Result<Value> {
    if args.len() != 2 {
        return Err(SqlError::InvalidValue(
            "ts_lexize requires 2 arguments (config, word)".into(),
        ));
    }
    if args.iter().any(|v| v.is_null()) {
        return Ok(Value::Null);
    }
    let kind = match &args[0] {
        Value::Text(s) => crate::fts::TokenizerKind::from_name(s.as_str())?,
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT (config)".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    let word = match &args[1] {
        Value::Text(s) => s.as_str(),
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT (word)".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    crate::fts::fn_ts_lexize_with_cancel(kind, word, cancel)
}

fn fts_numnode(args: &[Value], cancel: Option<&citadel::CancelToken>) -> Result<Value> {
    check_args("numnode", args, 1)?;
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    let tsq = match &args[0] {
        Value::TsQuery(b) => b,
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TSQUERY".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    crate::fts::fn_numnode_with_cancel(tsq, cancel)
}

fn fts_setweight(args: &[Value], cancel: Option<&citadel::CancelToken>) -> Result<Value> {
    if args.len() == 3 {
        return fts_setweight_selective(args, cancel);
    }
    check_args("setweight", args, 2)?;
    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::Null);
    }
    let tsv = match &args[0] {
        Value::TsVector(b) => b,
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TSVECTOR".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    let weight_text = match &args[1] {
        Value::Text(s) => s.as_str(),
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    let weight = crate::fts::parse_weight_char(weight_text)?;
    crate::fts::fn_setweight_with_cancel(tsv, weight, cancel)
}

fn fts_setweight_selective(args: &[Value], cancel: Option<&citadel::CancelToken>) -> Result<Value> {
    check_args("setweight", args, 3)?;
    if args[0].is_null() || args[1].is_null() || args[2].is_null() {
        return Ok(Value::Null);
    }
    let tsv = match &args[0] {
        Value::TsVector(b) => b,
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TSVECTOR".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    let weight_text = match &args[1] {
        Value::Text(s) => s.as_str(),
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    let weight = crate::fts::parse_weight_char(weight_text)?;
    let filter = match &args[2] {
        Value::Array(a) => a.as_ref().as_slice(),
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT[]".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    crate::fts::fn_setweight_selective_with_cancel(tsv, weight, filter, cancel)
}

fn fts_strip(args: &[Value], cancel: Option<&citadel::CancelToken>) -> Result<Value> {
    check_args("strip", args, 1)?;
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    let tsv = match &args[0] {
        Value::TsVector(b) => b,
        v => {
            return Err(SqlError::TypeMismatch {
                expected: "TSVECTOR".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    crate::fts::fn_strip_with_cancel(tsv, cancel)
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

/// For functions with optional trailing arguments, whose callee validates the upper bound.
fn check_min_args(name: &str, args: &[Value], min: usize) -> Result<()> {
    if args.len() < min {
        Err(SqlError::InvalidValue(format!(
            "{name} requires at least {min} argument(s), got {}",
            args.len()
        )))
    } else {
        Ok(())
    }
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

fn current_time_precision(name: &str, args: &[Value]) -> Result<Option<u32>> {
    match args {
        [] => Ok(None),
        [Value::Integer(precision)] if (0..=6).contains(precision) => Ok(Some(*precision as u32)),
        [Value::Integer(precision)] => Err(SqlError::InvalidValue(format!(
            "{name} precision {precision} must be between 0 and 6"
        ))),
        [_] => Err(SqlError::TypeMismatch {
            expected: "INTEGER precision".into(),
            got: args[0].data_type().to_string(),
        }),
        _ => Err(SqlError::InvalidValue(format!(
            "{name} requires zero or one argument, got {}",
            args.len()
        ))),
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
        Expr::IsDistinctFrom { left, right, .. } => {
            collect_column_refs(left, columns, out);
            collect_column_refs(right, columns, out);
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
        Expr::ArrayLiteral(elems) => {
            for e in elems {
                collect_column_refs(e, columns, out);
            }
        }
        Expr::Quantified { left, right, .. } => {
            collect_column_refs(left, columns, out);
            if let crate::parser::QuantifiedRhs::Array(e) = right {
                collect_column_refs(e, columns, out);
            }
        }
        Expr::Literal(_)
        | Expr::Parameter(_)
        | Expr::CountStar
        | Expr::Exists { .. }
        | Expr::ScalarSubquery(_)
        | Expr::TypedNullRecord(_) => {}
    }
}

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
