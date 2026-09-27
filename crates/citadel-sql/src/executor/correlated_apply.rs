use super::super::CteRows;
use super::binding::{bind_outer, OuterScope};
use super::*;
use crate::eval::{eval_expr_with_resolver, InputResolver};
use std::cell::RefCell;
use std::sync::Arc;

/// A deferred subquery addressed by an unnamed execution slot.
struct Capture {
    node: Expr,
    array_result: bool,
    cache_row_value: bool,
    positions: Vec<usize>,
    memo: Option<RefCell<FxHashMap<Vec<Value>, Arc<Expr>>>>,
    row_values: RefCell<Vec<Option<Value>>>,
}

struct Extractor<'a> {
    schema: &'a SchemaManager,
    ctes: &'a CteContext,
    outer: &'a OuterScope,
    cancel: Option<&'a citadel::CancelToken>,
    first: usize,
    captures: Vec<Capture>,
}

impl Extractor<'_> {
    /// Operands at this query level are visited before their subquery, so a
    /// capture may read the hidden columns of captures made before it.
    fn expr(&mut self, expr: &mut Expr) -> Result<()> {
        match expr {
            Expr::Exists { .. } | Expr::ScalarSubquery(_) => {}
            Expr::InSubquery { expr, .. } => self.expr(expr)?,
            Expr::Quantified { left, right, .. } => {
                self.expr(left)?;
                if let QuantifiedRhs::Array(expr) = right {
                    self.expr(expr)?;
                }
            }
            Expr::BinaryOp { left, right, .. } | Expr::IsDistinctFrom { left, right, .. } => {
                self.expr(left)?;
                self.expr(right)?;
            }
            Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::Cast { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::InSet { expr, .. } => self.expr(expr)?,
            Expr::Function { args, filter, .. } => {
                for expr in args {
                    self.expr(expr)?;
                }
                if let Some(filter) = filter {
                    self.expr(filter)?;
                }
            }
            Expr::Coalesce(args) | Expr::ArrayLiteral(args) => {
                for expr in args {
                    self.expr(expr)?;
                }
            }
            Expr::InList { expr, list, .. } => {
                self.expr(expr)?;
                for expr in list {
                    self.expr(expr)?;
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr(expr)?;
                self.expr(low)?;
                self.expr(high)?;
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.expr(expr)?;
                self.expr(pattern)?;
                if let Some(expr) = escape {
                    self.expr(expr)?;
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(expr) = operand {
                    self.expr(expr)?;
                }
                for (condition, value) in conditions {
                    self.expr(condition)?;
                    self.expr(value)?;
                }
                if let Some(expr) = else_result {
                    self.expr(expr)?;
                }
            }
            Expr::WindowFunction { args, spec, .. } => {
                for expr in args {
                    self.expr(expr)?;
                }
                for expr in &mut spec.partition_by {
                    self.expr(expr)?;
                }
                for item in &mut spec.order_by {
                    self.expr(&mut item.expr)?;
                }
                if let Some(frame) = &mut spec.frame {
                    for bound in [&mut frame.start, &mut frame.end] {
                        if let WindowFrameBound::Preceding(expr)
                        | WindowFrameBound::Following(expr) = bound
                        {
                            self.expr(expr)?;
                        }
                    }
                }
            }
            Expr::InputRef { .. }
            | Expr::BoundColumn { .. }
            | Expr::Literal(_)
            | Expr::Column(_)
            | Expr::QualifiedColumn { .. }
            | Expr::CountStar
            | Expr::Parameter(_)
            | Expr::TypedNullRecord(_) => {}
        }
        // Aggregate/window arguments belong to the surrounding SELECT phase.
        // Keep such an IN/ANY operand visible, deferring only its query side.
        match expr {
            Expr::InSubquery {
                expr: left,
                subquery,
                negated,
            } if crate::parser::calls_aggregate_or_window(left) => {
                let array = self.capture_array(subquery)?;
                let left = std::mem::replace(left, Box::new(Expr::Literal(Value::Null)));
                *expr = Expr::Quantified {
                    left,
                    op: if *negated { BinOp::NotEq } else { BinOp::Eq },
                    quantifier: if *negated {
                        Quantifier::All
                    } else {
                        Quantifier::Any
                    },
                    right: QuantifiedRhs::Array(Box::new(array)),
                };
                return Ok(());
            }
            Expr::Quantified {
                left,
                right: QuantifiedRhs::Subquery(query),
                ..
            } if crate::parser::calls_aggregate_or_window(left) => {
                let array = self.capture_array(query)?;
                if let Expr::Quantified { right, .. } = expr {
                    *right = QuantifiedRhs::Array(Box::new(array));
                }
                return Ok(());
            }
            _ => {}
        }
        let is_query = matches!(
            expr,
            Expr::Exists { .. }
                | Expr::ScalarSubquery(_)
                | Expr::InSubquery { .. }
                | Expr::Quantified {
                    right: QuantifiedRhs::Subquery(_),
                    ..
                }
        );
        if is_query {
            // Without a row the binder only resolves names; it changes nothing.
            let positions =
                bind_outer(self.schema, self.ctes, expr, self.outer, None, self.cancel)?
                    .unwrap_or_default();
            // A scalar subquery compares as a value without an implicit
            // column collation. Boolean captures need none either.
            let replacement = Expr::InputRef {
                index: self.first + self.captures.len(),
                collation: None,
            };
            let node = std::mem::replace(expr, replacement);
            let volatile_operand = match &node {
                Expr::InSubquery { expr, .. } => calls_volatile(expr),
                Expr::Quantified { left, .. } => calls_volatile(left),
                _ => false,
            };
            let volatile = calls_volatile(&node);
            self.captures.push(Capture {
                cache_row_value: volatile_operand || (!positions.is_empty() && volatile),
                memo: (positions.is_empty() || !volatile)
                    .then(|| RefCell::new(FxHashMap::default())),
                row_values: RefCell::new(Vec::new()),
                array_result: false,
                node,
                positions,
            });
        }
        Ok(())
    }

    fn capture_array(&mut self, query: &SelectStmt) -> Result<Expr> {
        let node = Expr::ScalarSubquery(Box::new(query.clone()));
        let positions = bind_outer(
            self.schema,
            self.ctes,
            &mut node.clone(),
            self.outer,
            None,
            self.cancel,
        )?
        .unwrap_or_default();
        // Bind NULL placeholders for shape analysis so an outer column keeps
        // its collation even when it is the subquery's projected expression.
        let mut shape = node.clone();
        bind_outer(
            self.schema,
            self.ctes,
            &mut shape,
            self.outer,
            Some(&vec![Value::Null; self.first]),
            self.cancel,
        )?;
        let Expr::ScalarSubquery(query_shape) = shape else {
            unreachable!()
        };
        let collation = super::super::dml::body_output_collations(
            self.schema,
            self.ctes,
            &QueryBody::Select(query_shape),
            1,
        )[0];
        let index = self.first + self.captures.len();
        self.captures.push(Capture {
            cache_row_value: !positions.is_empty() && calls_volatile(&node),
            memo: (positions.is_empty() || !calls_volatile(&node))
                .then(|| RefCell::new(FxHashMap::default())),
            row_values: RefCell::new(Vec::new()),
            array_result: true,
            node,
            positions,
        });
        Ok(Expr::InputRef {
            index,
            collation: Some(collation),
        })
    }

    fn captured_by(&mut self, expr: &mut Expr) -> Result<bool> {
        let before = self.captures.len();
        self.expr(expr)?;
        Ok(self.captures.len() > before)
    }
}

fn conjunction(conjuncts: Vec<Expr>) -> Option<Expr> {
    conjuncts.into_iter().reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinOp::And,
        right: Box::new(right),
    })
}

type SubqueryExecutor<'a> = &'a mut dyn FnMut(&SelectStmt) -> Result<CteRows>;

/// Subqueries are immutable plans. Each memo holds materialized query results,
/// not the enclosing conditional expression or an IN operand's current value.
/// A runtime borrows the executor only while running a cache miss, and releases
/// it before evaluating a dependent input slot.
struct SubqueryRuntime<'a, 'e> {
    captures: &'a [Capture],
    first: usize,
    row_identity: Option<usize>,
    schema: &'a SchemaManager,
    ctes: &'a CteContext,
    outer: &'a OuterScope,
    cancel: Option<&'a citadel::CancelToken>,
    exec_sub: RefCell<SubqueryExecutor<'e>>,
}

impl InputResolver for SubqueryRuntime<'_, '_> {
    fn resolve(&self, index: usize, ctx: &EvalCtx<'_>) -> Result<Option<Value>> {
        let Some(capture) = index
            .checked_sub(self.first)
            .and_then(|i| self.captures.get(i))
        else {
            return Ok(None);
        };
        check_cancel(self.cancel)?;
        let row_id = self
            .row_identity
            .filter(|_| capture.cache_row_value)
            .map(|position| {
                match ctx.row.get(position) {
                    Some(Value::Integer(id)) if *id >= 0 => usize::try_from(*id)
                        .map_err(|_| SqlError::Plan("invalid subquery row identity".into())),
                    // An aggregate over no source rows has one synthetic NULL row.
                    Some(Value::Null) => capture
                        .row_values
                        .borrow()
                        .len()
                        .checked_sub(1)
                        .ok_or_else(|| SqlError::Plan("missing empty-group subquery slot".into())),
                    _ => Err(SqlError::Plan("missing subquery row identity".into())),
                }
            })
            .transpose()?;
        if let Some(row_id) = row_id {
            let values = capture.row_values.borrow();
            let cached = values.get(row_id).ok_or_else(|| {
                SqlError::Plan("subquery row identity outside its statement".into())
            })?;
            if let Some(value) = cached {
                return Ok(Some(value.clone()));
            }
        }
        let key = capture
            .positions
            .iter()
            .map(|&position| {
                ctx.row.get(position).cloned().ok_or_else(|| {
                    SqlError::Plan(format!("captured input slot {position} is outside the row"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let cached = capture
            .memo
            .as_ref()
            .and_then(|memo| memo.borrow().get(&key).cloned());
        let materialized = match cached {
            Some(expr) => expr,
            None => {
                let mut bound = capture.node.clone();
                bind_outer(
                    self.schema,
                    self.ctes,
                    &mut bound,
                    self.outer,
                    Some(ctx.row),
                    self.cancel,
                )?;
                let expr = {
                    let mut exec = self.exec_sub.borrow_mut();
                    if capture.array_result {
                        let Expr::ScalarSubquery(query) = &bound else {
                            return Err(SqlError::Plan("array capture has no query".into()));
                        };
                        let selected = exec(query)?;
                        if selected.result.columns.len() != 1 {
                            return Err(SqlError::SubqueryMultipleColumns);
                        }
                        let collation = selected.collation_at(0);
                        let values = selected
                            .result
                            .rows
                            .into_iter()
                            .map(|mut row| row.remove(0))
                            .collect();
                        Expr::BoundColumn {
                            value: Value::Array(Arc::new(values)),
                            collation,
                        }
                    } else {
                        super::super::dml::materialize_expr(&bound, &mut **exec)?
                    }
                };
                let expr = Arc::new(expr);
                if let Some(memo) = &capture.memo {
                    memo.borrow_mut().insert(key, Arc::clone(&expr));
                }
                expr
            }
        };
        let value = eval_expr_with_resolver(&materialized, ctx, Some(self))?;
        if let Some(row_id) = row_id {
            capture.row_values.borrow_mut()[row_id] = Some(value.clone());
        }
        Ok(Some(value))
    }
}

/// A collection of expressions sharing one outer row. The execution callback
/// is borrowed for each evaluation, so UPDATE can retain these memos while its
/// writer moves through the normal mutation phases.
pub(in crate::executor) struct RowExpressions {
    pub expressions: Vec<Expr>,
    captures: Vec<Capture>,
    outer: OuterScope,
    first: usize,
}

impl RowExpressions {
    pub(in crate::executor) fn captures_outer(&self) -> bool {
        self.captures
            .iter()
            .any(|capture| !capture.positions.is_empty())
    }

    pub(in crate::executor) fn new(
        schema: &SchemaManager,
        ctes: &CteContext,
        mut expressions: Vec<Expr>,
        outer: OuterScope,
        first: usize,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Self> {
        let mut extractor = Extractor {
            schema,
            ctes,
            outer: &outer,
            cancel,
            first,
            captures: Vec::new(),
        };
        for expr in &mut expressions {
            extractor.expr(expr)?;
        }
        let captures = extractor.captures;
        Ok(Self {
            expressions,
            captures,
            outer,
            first,
        })
    }

    pub(in crate::executor) fn eval(
        &self,
        index: usize,
        schema: &SchemaManager,
        ctes: &CteContext,
        ctx: &EvalCtx<'_>,
        exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
    ) -> Result<Value> {
        self.with_resolver(schema, ctes, ctx.cancel, exec_sub, |resolver| {
            eval_expr_with_resolver(&self.expressions[index], ctx, Some(resolver))
        })
    }

    pub(in crate::executor) fn with_resolver<R>(
        &self,
        schema: &SchemaManager,
        ctes: &CteContext,
        cancel: Option<&citadel::CancelToken>,
        exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
        evaluate: impl FnOnce(&dyn InputResolver) -> Result<R>,
    ) -> Result<R> {
        let runtime = SubqueryRuntime {
            captures: &self.captures,
            first: self.first,
            row_identity: None,
            schema,
            ctes,
            outer: &self.outer,
            cancel,
            exec_sub: RefCell::new(exec_sub),
        };
        evaluate(&runtime)
    }
}

/// Post-scan execution owns the runtime through filtering, grouping, windows,
/// ordering and projection. Only the expression evaluator decides whether a
/// conditional branch demands a subquery.
#[allow(clippy::too_many_arguments)]
pub(in crate::executor) fn finish_subqueries(
    schema: &SchemaManager,
    ctes: &CteContext,
    mut stmt: SelectStmt,
    outer: &OuterScope,
    mut rows: Vec<Vec<Value>>,
    columns: Vec<ColumnDef>,
    first: usize,
    cancel: Option<&citadel::CancelToken>,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
) -> Result<ExecutionResult> {
    let mut extractor = Extractor {
        schema,
        ctes,
        outer,
        cancel,
        first,
        captures: Vec::new(),
    };
    let mut prefilter = Vec::new();
    let mut deferred = Vec::new();
    if let Some(predicate) = stmt.where_clause.take() {
        for conjunct in flatten_and_exprs(&predicate) {
            let mut expr = conjunct.clone();
            if extractor.captured_by(&mut expr)? {
                deferred.push(expr);
            } else {
                prefilter.push(expr);
            }
        }
    }
    stmt.where_clause = conjunction(deferred);
    for column in &mut stmt.columns {
        if let SelectColumn::Expr { expr, alias } = column {
            let name = crate::parser::expr_display_name(expr);
            if extractor.captured_by(expr)? && alias.is_none() {
                *alias = Some(name);
            }
        }
    }
    for expr in stmt.group_by.iter_mut().chain(stmt.having.iter_mut()) {
        extractor.expr(expr)?;
    }
    for item in &mut stmt.order_by {
        extractor.expr(&mut item.expr)?;
    }
    let captures = extractor.captures;
    if let Some(filter) = conjunction(prefilter) {
        let col_map = ColumnMap::new(&columns);
        let mut kept = Vec::with_capacity(rows.len());
        for (index, row) in rows.into_iter().enumerate() {
            check_cancel_at(cancel, index)?;
            if is_truthy(&eval_expr(
                &filter,
                &EvalCtx::new(&col_map, &row).with_cancel(cancel),
            )?) {
                kept.push(row);
            }
        }
        rows = kept;
    }
    if captures.is_empty() {
        return super::super::process_select(
            rows,
            super::super::SelectCtx::new(&columns, &stmt, cancel).row_width(first),
        );
    }
    let row_identity = captures
        .iter()
        .any(|capture| capture.cache_row_value)
        .then_some(first + captures.len());
    for capture in &captures {
        if capture.cache_row_value {
            capture.row_values.borrow_mut().resize(rows.len() + 1, None);
        }
    }
    for (index, row) in rows.iter_mut().enumerate() {
        check_cancel_at(cancel, index)?;
        row.extend(std::iter::repeat_n(Value::Null, captures.len()));
        if row_identity.is_some() {
            row.push(Value::Integer(i64::try_from(index).map_err(|_| {
                SqlError::Plan("too many subquery input rows".into())
            })?));
        }
    }

    let runtime = SubqueryRuntime {
        captures: &captures,
        first,
        row_identity,
        schema,
        ctes,
        outer,
        cancel,
        exec_sub: RefCell::new(exec_sub),
    };
    super::super::process_select(
        rows,
        super::super::SelectCtx::new(&columns, &stmt, cancel)
            .row_width(first + captures.len() + usize::from(row_identity.is_some()))
            .with_resolver(Some(&runtime)),
    )
}
