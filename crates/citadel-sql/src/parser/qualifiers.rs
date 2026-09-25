//! A qualified column reference names a source of its query or of a query
//! around it. Execution matches a source by the name it gives the source, and
//! resolves a column by its name alone once the qualifier matches nothing, so
//! resolution rewrites every other spelling to that name, and SQL a caller
//! submits must not carry a qualifier that names no source.

use super::expr_name::expr_display_name;
use super::*;

/// What resolution does with a qualifier that names no source in scope, or
/// more than one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Unresolved {
    /// SQL a caller submits: the reference is an error.
    Reject,
    /// Stored views and trigger bodies are parsed again on every use; keeping
    /// the reference lets a definition saved before the check keep working.
    Keep,
}

pub(super) fn resolve_qualifiers(stmt: &mut Statement, unresolved: Unresolved) -> Result<()> {
    Resolver {
        scopes: Vec::new(),
        rows: Vec::new(),
        unresolved,
    }
    .statement(stmt)
}

#[derive(Clone)]
struct Source {
    /// The names a qualifier may use: the alias, or the name when there is
    /// none. A dotted name is also visible by its last part.
    names: Vec<String>,
    /// The name execution gives the source.
    canonical: String,
}

impl Source {
    fn new(name: &str, alias: Option<&str>) -> Self {
        let canonical = alias.unwrap_or(name).to_ascii_lowercase();
        let mut names = vec![canonical.clone()];
        if let Some((_, last)) = canonical.rsplit_once('.') {
            names.push(last.to_owned());
        }
        Self { names, canonical }
    }
}

enum Resolution<'a> {
    Source(&'a str),
    Row,
    Ambiguous,
    Unknown,
}

struct Resolver {
    /// The sources of each enclosing query, innermost last.
    scopes: Vec<Vec<Source>>,
    /// Rows the statement supplies without a source: OLD and NEW in a trigger
    /// body or a RETURNING clause. A source of the same name wins.
    rows: Vec<&'static str>,
    unresolved: Unresolved,
}

impl Resolver {
    fn in_scope<T>(
        &mut self,
        sources: Vec<Source>,
        resolve: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.scopes.push(sources);
        let result = resolve(self);
        self.scopes.pop();
        result
    }

    /// The innermost query with a source of this name decides; two sources
    /// of one query that share it are ambiguous.
    fn resolve(&self, qualifier: &str) -> Resolution<'_> {
        let qualifier = qualifier.to_ascii_lowercase();
        for sources in self.scopes.iter().rev() {
            let mut named = sources
                .iter()
                .filter(|source| source.names.contains(&qualifier));
            if let Some(source) = named.next() {
                return match named.next() {
                    Some(_) => Resolution::Ambiguous,
                    None => Resolution::Source(&source.canonical),
                };
            }
        }
        if self.rows.contains(&qualifier.as_str()) {
            Resolution::Row
        } else {
            Resolution::Unknown
        }
    }

    fn statement(&mut self, stmt: &mut Statement) -> Result<()> {
        match stmt {
            Statement::Select(query) => self.query(query),
            Statement::Insert(insert) => self.insert(insert),
            Statement::Update(update) => self.update(update),
            Statement::Delete(delete) => self.delete(delete),
            Statement::Explain { inner, .. } => self.statement(inner),
            // The body is stored as text and resolved each time it is parsed.
            Statement::CreateView(view) if self.unresolved == Unresolved::Reject => {
                parse_submitted(&view.sql).map(drop)
            }
            Statement::CreateMaterializedView(view) => self.query(&mut view.select_parsed),
            Statement::CreateTrigger(trigger) => {
                self.rows = vec!["old", "new"];
                if let Some(when) = &mut trigger.when_expr {
                    self.expr(when)?;
                }
                trigger
                    .body
                    .iter_mut()
                    .try_for_each(|stmt| self.statement(stmt))
            }
            _ => Ok(()),
        }
    }

    fn query(&mut self, query: &mut SelectQuery) -> Result<()> {
        for cte in &mut query.ctes {
            self.body(&mut cte.body)?;
        }
        self.body(&mut query.body)
    }

    fn body(&mut self, body: &mut QueryBody) -> Result<()> {
        match body {
            QueryBody::Select(select) => self.select(select),
            // A compound's ORDER BY names its output columns.
            QueryBody::Compound(compound) => {
                self.body(&mut compound.left)?;
                self.body(&mut compound.right)
            }
            QueryBody::Insert(insert) => self.insert(insert),
            QueryBody::Update(update) => self.update(update),
            QueryBody::Delete(delete) => self.delete(delete),
        }
    }

    fn select(&mut self, select: &mut SelectStmt) -> Result<()> {
        // A derived table sees the queries around this one, and a LATERAL one
        // also the sources before it; a table function's arguments likewise.
        let mut sources = Vec::new();
        if let Some(derived) = &mut select.from_subquery {
            self.query(&mut derived.query)?;
        }
        for arg in select.from_args.iter_mut().flatten() {
            self.expr(arg)?;
        }
        if let Some(json_table) = &mut select.from_json_table {
            self.expr(&mut json_table.source)?;
        }
        if !select.from.is_empty() {
            sources.push(Source::new(&select.from, select.from_alias.as_deref()));
        }
        for join in &mut select.joins {
            match &mut join.subquery {
                Some(derived) if derived.lateral => {
                    self.in_scope(sources.clone(), |r| r.query(&mut derived.query))?
                }
                Some(derived) => self.query(&mut derived.query)?,
                None => {
                    for arg in join.table.args.iter_mut().flatten() {
                        self.in_scope(sources.clone(), |r| r.expr(arg))?;
                    }
                }
            }
            sources.push(Source::new(&join.table.name, join.table.alias.as_deref()));
        }
        self.in_scope(sources, |r| {
            for join in &mut select.joins {
                if let Some(on) = &mut join.on_clause {
                    r.expr(on)?;
                }
            }
            r.projections(&mut select.columns)?;
            for expr in select
                .where_clause
                .iter_mut()
                .chain(&mut select.group_by)
                .chain(&mut select.having)
                .chain(select.order_by.iter_mut().map(|item| &mut item.expr))
                .chain(&mut select.limit)
                .chain(&mut select.offset)
            {
                r.expr(expr)?;
            }
            Ok(())
        })
    }

    fn insert(&mut self, insert: &mut InsertStmt) -> Result<()> {
        match &mut insert.source {
            InsertSource::Values(rows) => {
                for expr in rows.iter_mut().flatten() {
                    self.expr(expr)?;
                }
            }
            InsertSource::Select(query) => self.query(query)?,
        }
        if let Some(OnConflictClause {
            action:
                OnConflictAction::DoUpdate {
                    assignments,
                    where_clause,
                },
            ..
        }) = &mut insert.on_conflict
        {
            let conflict = vec![
                Source::new(&insert.table, None),
                Source::new("excluded", None),
            ];
            self.in_scope(conflict, |r| {
                for (_, expr) in assignments.iter_mut() {
                    r.expr(expr)?;
                }
                where_clause.iter_mut().try_for_each(|expr| r.expr(expr))
            })?;
        }
        let target = vec![Source::new(&insert.table, None)];
        self.in_scope(target, |r| r.returning(insert.returning.as_deref_mut()))
    }

    fn update(&mut self, update: &mut UpdateStmt) -> Result<()> {
        let target = vec![Source::new(&update.table, None)];
        self.in_scope(target, |r| {
            for (_, expr) in &mut update.assignments {
                r.expr(expr)?;
            }
            if let Some(expr) = &mut update.where_clause {
                r.expr(expr)?;
            }
            r.returning(update.returning.as_deref_mut())
        })
    }

    fn delete(&mut self, delete: &mut DeleteStmt) -> Result<()> {
        let target = vec![Source::new(&delete.table, None)];
        self.in_scope(target, |r| {
            if let Some(expr) = &mut delete.where_clause {
                r.expr(expr)?;
            }
            r.returning(delete.returning.as_deref_mut())
        })
    }

    /// RETURNING may also read the row before and after the change as OLD and
    /// NEW.
    fn returning(&mut self, columns: Option<&mut [SelectColumn]>) -> Result<()> {
        let rows = std::mem::replace(&mut self.rows, vec!["old", "new"]);
        let result = columns.map_or(Ok(()), |columns| self.projections(columns));
        self.rows = rows;
        result
    }

    /// A rewritten qualifier keeps the output name the query wrote.
    fn projections(&mut self, columns: &mut [SelectColumn]) -> Result<()> {
        for column in columns {
            if let SelectColumn::Expr { expr, alias } = column {
                let written = alias.is_none().then(|| expr_display_name(expr));
                self.expr(expr)?;
                if let Some(written) = written {
                    if written != expr_display_name(expr) {
                        *alias = Some(written);
                    }
                }
            }
        }
        Ok(())
    }

    fn expr(&mut self, expr: &mut Expr) -> Result<()> {
        match expr {
            Expr::QualifiedColumn { table, column } => match self.resolve(table) {
                Resolution::Source(canonical) => {
                    if table != canonical {
                        *table = canonical.to_owned();
                    }
                }
                Resolution::Row => {}
                Resolution::Ambiguous if self.unresolved == Unresolved::Reject => {
                    return Err(SqlError::AmbiguousColumn(format!("{table}.{column}")));
                }
                Resolution::Unknown if self.unresolved == Unresolved::Reject => {
                    return Err(SqlError::ColumnNotFound(format!("{table}.{column}")));
                }
                Resolution::Ambiguous | Resolution::Unknown => {}
            },
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
