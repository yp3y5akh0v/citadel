use super::super::CteRows;
use super::binding::{bind_outer, OuterScope};
use super::*;

/// A subquery that reads the outer row, evaluated into a hidden column.
struct Capture {
    node: Expr,
    positions: Vec<usize>,
    volatile: bool,
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
            Expr::Function { args, .. } | Expr::Coalesce(args) | Expr::ArrayLiteral(args) => {
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
            Expr::BoundColumn { .. }
            | Expr::Literal(_)
            | Expr::Column(_)
            | Expr::QualifiedColumn { .. }
            | Expr::CountStar
            | Expr::Parameter(_)
            | Expr::TypedNullRecord(_) => {}
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
            if let Some(positions) =
                bind_outer(self.schema, self.ctes, expr, self.outer, None, self.cancel)?
            {
                let hidden = Expr::Column(hidden_name(self.first + self.captures.len()));
                // A scalar subquery's value compares like a literal, without a
                // column's implicit collation; COALESCE reads it that way.
                let replacement = if matches!(expr, Expr::ScalarSubquery(_)) {
                    Expr::Coalesce(vec![hidden])
                } else {
                    hidden
                };
                let node = std::mem::replace(expr, replacement);
                self.captures.push(Capture {
                    volatile: calls_volatile(&node),
                    node,
                    positions,
                });
            }
        }
        Ok(())
    }

    fn captured_by(&mut self, expr: &mut Expr) -> Result<bool> {
        let before = self.captures.len();
        self.expr(expr)?;
        Ok(self.captures.len() > before)
    }
}

fn hidden_name(position: usize) -> String {
    format!("__captured_{position}")
}

fn calls_volatile(expr: &Expr) -> bool {
    let mut volatile = false;
    crate::parser::visit_expr(expr, &mut |node| {
        if let Expr::Function { name, args, .. } = node {
            volatile |= crate::eval::is_volatile_function_expr(&name.to_ascii_uppercase(), args);
        }
    });
    volatile
}

fn conjunction(conjuncts: Vec<Expr>) -> Option<Expr> {
    conjuncts.into_iter().reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinOp::And,
        right: Box::new(right),
    })
}

/// Evaluate every subquery in `stmt`'s per-row clauses that reads the `outer`
/// row once per row of `rows`, binding that row into the subquery's lexical
/// scopes. Each result becomes a hidden column the returned statement reads
/// instead of the subquery. WHERE conjuncts that read no outer row filter
/// `rows` first. Rows with equal captured values share one execution unless
/// the subquery calls a volatile function. `ctes` are the CTEs visible to
/// `stmt`. Returns None when no subquery reads the outer row.
#[allow(clippy::too_many_arguments)]
pub(in crate::executor) fn apply_captured_subqueries(
    schema: &SchemaManager,
    ctes: &CteContext,
    stmt: &SelectStmt,
    outer: &OuterScope,
    rows: &mut Vec<Vec<Value>>,
    columns: &mut Vec<ColumnDef>,
    cancel: Option<&citadel::CancelToken>,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
) -> Result<Option<SelectStmt>> {
    let mut rewritten = stmt.clone();
    let mut extractor = Extractor {
        schema,
        ctes,
        outer,
        cancel,
        first: columns.len(),
        captures: Vec::new(),
    };
    let mut prefilter = Vec::new();
    let mut deferred = Vec::new();
    if let Some(where_clause) = rewritten.where_clause.take() {
        for conjunct in flatten_and_exprs(&where_clause) {
            let mut conjunct = conjunct.clone();
            if extractor.captured_by(&mut conjunct)? {
                deferred.push(conjunct);
            } else {
                prefilter.push(conjunct);
            }
        }
    }
    rewritten.where_clause = conjunction(deferred);
    for column in &mut rewritten.columns {
        if let SelectColumn::Expr { expr, alias } = column {
            let name = crate::parser::expr_display_name(expr);
            // A projection keeps the name of the expression it was written as.
            if extractor.captured_by(expr)? && alias.is_none() {
                *alias = Some(name);
            }
        }
    }
    for expr in rewritten
        .group_by
        .iter_mut()
        .chain(rewritten.having.iter_mut())
    {
        extractor.expr(expr)?;
    }
    for item in &mut rewritten.order_by {
        extractor.expr(&mut item.expr)?;
    }
    let captures = extractor.captures;
    if captures.is_empty() {
        return Ok(None);
    }

    if let Some(filter) = conjunction(prefilter) {
        let filter = super::super::dml::materialize_expr(&filter, exec_sub)?;
        let col_map = ColumnMap::new(columns);
        let mut kept = Vec::with_capacity(rows.len());
        for (row_idx, row) in std::mem::take(rows).into_iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            if is_truthy(&eval_expr(
                &filter,
                &EvalCtx::new(&col_map, &row).with_cancel(cancel),
            )?) {
                kept.push(row);
            }
        }
        *rows = kept;
    }

    let first = columns.len();
    for position in first..first + captures.len() {
        columns.push(super::super::helpers::projected_column(
            hidden_name(position),
            position,
            Collation::Binary,
        ));
    }
    let col_map = ColumnMap::new(columns);
    let mut memos: Vec<Option<FxHashMap<Vec<Value>, Expr>>> = captures
        .iter()
        .map(|capture| (!capture.volatile).then(FxHashMap::default))
        .collect();
    for (row_idx, row) in rows.iter_mut().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        row.extend(std::iter::repeat_n(Value::Null, captures.len()));
        for (index, capture) in captures.iter().enumerate() {
            let key: Vec<Value> = capture
                .positions
                .iter()
                .map(|&position| row[position].clone())
                .collect();
            let memo = &mut memos[index];
            let materialized = match memo.as_ref().and_then(|memo| memo.get(&key)) {
                Some(materialized) => materialized.clone(),
                None => {
                    let mut bound = capture.node.clone();
                    bind_outer(
                        schema,
                        ctes,
                        &mut bound,
                        outer,
                        Some(row.as_slice()),
                        cancel,
                    )?;
                    let materialized = super::super::dml::materialize_expr(&bound, exec_sub)?;
                    if let Some(memo) = memo {
                        memo.insert(key, materialized.clone());
                    }
                    materialized
                }
            };
            let value = eval_expr(
                &materialized,
                &EvalCtx::new(&col_map, row).with_cancel(cancel),
            )?;
            row[first + index] = value;
        }
    }
    check_cancel(cancel)?;
    Ok(Some(rewritten))
}
