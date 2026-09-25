//! A qualified column reference must name a source of its query or of a query
//! around it. Execution resolves a column by its name alone once the qualifier
//! matches nothing, so an unchecked qualifier silently reads whichever source
//! has a column of that name.

use super::*;

/// Rejects a qualified column whose qualifier names no source in scope. A
/// source is visible by its alias, or by its name when it has none.
pub fn validate_qualifiers(stmt: &Statement) -> Result<()> {
    Validator::default().statement(stmt)
}

#[derive(Default)]
struct Validator {
    /// The source names of each enclosing query, innermost last.
    scopes: Vec<Vec<String>>,
    /// Rows the statement supplies without a source: OLD and NEW in a trigger
    /// body or a RETURNING clause.
    rows: Vec<&'static str>,
}

/// The names a source is visible by. A dotted name is also visible by its
/// last part, since a qualifier is one identifier.
fn source_names(name: &str, alias: Option<&str>) -> Vec<String> {
    let visible = alias.unwrap_or(name).to_ascii_lowercase();
    let last = visible.rsplit('.').next().unwrap_or_default().to_owned();
    if last == visible {
        vec![visible]
    } else {
        vec![visible, last]
    }
}

impl Validator {
    fn in_scope<T>(
        &mut self,
        sources: Vec<String>,
        check: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.scopes.push(sources);
        let result = check(self);
        self.scopes.pop();
        result
    }

    fn visible(&self, qualifier: &str) -> bool {
        let qualifier = qualifier.to_ascii_lowercase();
        self.rows.contains(&qualifier.as_str())
            || self
                .scopes
                .iter()
                .any(|sources| sources.contains(&qualifier))
    }

    fn statement(&mut self, stmt: &Statement) -> Result<()> {
        match stmt {
            Statement::Select(query) => self.query(query),
            Statement::Insert(insert) => self.insert(insert),
            Statement::Update(update) => self.update(update),
            Statement::Delete(delete) => self.delete(delete),
            Statement::Explain { inner, .. } => self.statement(inner),
            Statement::CreateView(view) => self.statement(&parse_sql(&view.sql)?),
            Statement::CreateMaterializedView(view) => self.query(&view.select_parsed),
            Statement::CreateTrigger(trigger) => {
                self.rows = vec!["old", "new"];
                if let Some(when) = &trigger.when_expr {
                    self.expr(when)?;
                }
                trigger
                    .body
                    .iter()
                    .try_for_each(|stmt| self.statement(stmt))
            }
            _ => Ok(()),
        }
    }

    fn query(&mut self, query: &SelectQuery) -> Result<()> {
        for cte in &query.ctes {
            self.body(&cte.body)?;
        }
        self.body(&query.body)
    }

    fn body(&mut self, body: &QueryBody) -> Result<()> {
        match body {
            QueryBody::Select(select) => self.select(select),
            // A compound's ORDER BY names its output columns.
            QueryBody::Compound(compound) => {
                self.body(&compound.left)?;
                self.body(&compound.right)
            }
            QueryBody::Insert(insert) => self.insert(insert),
            QueryBody::Update(update) => self.update(update),
            QueryBody::Delete(delete) => self.delete(delete),
        }
    }

    fn select(&mut self, select: &SelectStmt) -> Result<()> {
        // A derived table sees the queries around this one, and a LATERAL one
        // also the sources before it; a table function's arguments likewise.
        let mut sources = Vec::new();
        if let Some(derived) = &select.from_subquery {
            self.query(&derived.query)?;
        }
        for arg in select.from_args.iter().flatten() {
            self.expr(arg)?;
        }
        if let Some(json_table) = &select.from_json_table {
            self.expr(&json_table.source)?;
        }
        if !select.from.is_empty() {
            sources.extend(source_names(&select.from, select.from_alias.as_deref()));
        }
        for join in &select.joins {
            match &join.subquery {
                Some(derived) if derived.lateral => {
                    self.in_scope(sources.clone(), |v| v.query(&derived.query))?
                }
                Some(derived) => self.query(&derived.query)?,
                None => {
                    for arg in join.table.args.iter().flatten() {
                        self.in_scope(sources.clone(), |v| v.expr(arg))?;
                    }
                }
            }
            sources.extend(source_names(&join.table.name, join.table.alias.as_deref()));
        }
        self.in_scope(sources, |v| {
            for join in &select.joins {
                if let Some(on) = &join.on_clause {
                    v.expr(on)?;
                }
            }
            for column in &select.columns {
                if let SelectColumn::Expr { expr, .. } = column {
                    v.expr(expr)?;
                }
            }
            for expr in select
                .where_clause
                .iter()
                .chain(&select.group_by)
                .chain(&select.having)
                .chain(select.order_by.iter().map(|item| &item.expr))
                .chain(&select.limit)
                .chain(&select.offset)
            {
                v.expr(expr)?;
            }
            Ok(())
        })
    }

    fn insert(&mut self, insert: &InsertStmt) -> Result<()> {
        match &insert.source {
            InsertSource::Values(rows) => {
                for expr in rows.iter().flatten() {
                    self.expr(expr)?;
                }
            }
            InsertSource::Select(query) => self.query(query)?,
        }
        let target = source_names(&insert.table, None);
        if let Some(OnConflictClause {
            action:
                OnConflictAction::DoUpdate {
                    assignments,
                    where_clause,
                },
            ..
        }) = &insert.on_conflict
        {
            let mut conflict = target.clone();
            conflict.push("excluded".into());
            self.in_scope(conflict, |v| {
                for (_, expr) in assignments {
                    v.expr(expr)?;
                }
                where_clause.iter().try_for_each(|expr| v.expr(expr))
            })?;
        }
        self.in_scope(target, |v| v.returning(insert.returning.as_deref()))
    }

    fn update(&mut self, update: &UpdateStmt) -> Result<()> {
        self.in_scope(source_names(&update.table, None), |v| {
            for (_, expr) in &update.assignments {
                v.expr(expr)?;
            }
            if let Some(expr) = &update.where_clause {
                v.expr(expr)?;
            }
            v.returning(update.returning.as_deref())
        })
    }

    fn delete(&mut self, delete: &DeleteStmt) -> Result<()> {
        self.in_scope(source_names(&delete.table, None), |v| {
            if let Some(expr) = &delete.where_clause {
                v.expr(expr)?;
            }
            v.returning(delete.returning.as_deref())
        })
    }

    /// RETURNING may also read the row before and after the change as OLD and
    /// NEW.
    fn returning(&mut self, columns: Option<&[SelectColumn]>) -> Result<()> {
        let rows = std::mem::replace(&mut self.rows, vec!["old", "new"]);
        let result = columns
            .into_iter()
            .flatten()
            .try_for_each(|column| match column {
                SelectColumn::Expr { expr, .. } => self.expr(expr),
                _ => Ok(()),
            });
        self.rows = rows;
        result
    }

    fn expr(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::QualifiedColumn { table, column } => {
                if !self.visible(table) {
                    return Err(SqlError::ColumnNotFound(format!("{table}.{column}")));
                }
            }
            Expr::Exists { subquery, .. } | Expr::ScalarSubquery(subquery) => {
                self.select(subquery)?
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.expr(expr)?;
                self.select(subquery)?;
            }
            Expr::Quantified { left, right, .. } => {
                self.expr(left)?;
                match right {
                    QuantifiedRhs::Subquery(subquery) => self.select(subquery)?,
                    QuantifiedRhs::Array(array) => self.expr(array)?,
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
                for arg in args {
                    self.expr(arg)?;
                }
            }
            Expr::InList { expr, list, .. } => {
                self.expr(expr)?;
                for item in list {
                    self.expr(item)?;
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
                if let Some(escape) = escape {
                    self.expr(escape)?;
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(operand) = operand {
                    self.expr(operand)?;
                }
                for (condition, result) in conditions {
                    self.expr(condition)?;
                    self.expr(result)?;
                }
                if let Some(else_result) = else_result {
                    self.expr(else_result)?;
                }
            }
            Expr::WindowFunction { args, spec, .. } => {
                for arg in args {
                    self.expr(arg)?;
                }
                for expr in &spec.partition_by {
                    self.expr(expr)?;
                }
                for item in &spec.order_by {
                    self.expr(&item.expr)?;
                }
                if let Some(frame) = &spec.frame {
                    for bound in [&frame.start, &frame.end] {
                        if let WindowFrameBound::Preceding(expr)
                        | WindowFrameBound::Following(expr) = bound
                        {
                            self.expr(expr)?;
                        }
                    }
                }
            }
            Expr::Literal(_)
            | Expr::BoundColumn { .. }
            | Expr::Column(_)
            | Expr::CountStar
            | Expr::Parameter(_)
            | Expr::TypedNullRecord(_) => {}
        }
        Ok(())
    }
}
