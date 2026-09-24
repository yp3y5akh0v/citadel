//! Resolve DML target aliases before lowering to the public, alias-free AST.
//!
//! Names are resolved in lexical scopes. Inner bindings are renamed only when
//! necessary to keep a canonical outer-table reference from capturing them.

use rustc_hash::{FxHashMap, FxHashSet};

use super::expr_name::expr_display_name;
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Context {
    Predicate,
    Assignment,
    Returning,
    Conflict,
    Source,
}

struct Lowerer<'a> {
    table: &'a str,
    target: String,
    scopes: Vec<FxHashMap<String, String>>,
    used: FxHashSet<String>,
    generated: FxHashSet<String>,
    next_alias: usize,
    context: Context,
}

impl<'a> Lowerer<'a> {
    fn new(table: &'a str, alias: Option<&str>, context: Context) -> Self {
        let target = alias.unwrap_or(table).to_ascii_lowercase();
        Self {
            table,
            used: [table.to_ascii_lowercase(), target.clone()]
                .into_iter()
                .collect(),
            target,
            scopes: Vec::new(),
            generated: FxHashSet::default(),
            next_alias: 0,
            context,
        }
    }

    fn fresh_alias(&mut self) -> String {
        loop {
            let name = format!("__citadel_dml_scope_{}", self.next_alias);
            self.next_alias += 1;
            if self.used.insert(name.clone()) {
                self.generated.insert(name.clone());
                return name;
            }
        }
    }

    fn source_alias(&mut self, original: &str) -> Result<String> {
        let original = original.to_ascii_lowercase();
        let lowered = if self.context != Context::Source
            && ((self.target != self.table && original.eq_ignore_ascii_case(self.table))
                || matches!(original.as_str(), "old" | "new" | "excluded")
                || self.generated.contains(&original))
        {
            self.fresh_alias()
        } else {
            original.clone()
        };
        let scope = self.scopes.last_mut().expect("SELECT scope");
        if scope.insert(original.clone(), lowered.clone()).is_some() {
            return Err(SqlError::Unsupported(format!(
                "duplicate source qualifier '{original}'"
            )));
        }
        Ok(lowered)
    }

    fn expr(&mut self, expr: &mut Expr) -> Result<()> {
        if let Expr::QualifiedColumn { table, column } = expr {
            let name = table.to_ascii_lowercase();
            if let Some(lowered) = self.scopes.iter().rev().find_map(|scope| scope.get(&name)) {
                *table = lowered.clone();
            } else if self.context != Context::Source && name == self.target {
                if self.context == Context::Conflict && name == "excluded" {
                    return Err(SqlError::AmbiguousColumn(format!("{table}.{column}")));
                }
                if self.scopes.is_empty() {
                    *expr = Expr::Column(column.clone());
                } else if self.context == Context::Predicate {
                    *table = self.table.to_ascii_lowercase();
                } else {
                    return Err(SqlError::Unsupported(
                        "correlated subquery outside a mutation WHERE clause".into(),
                    ));
                }
            } else if self.context == Context::Source && name != self.target {
                // INSERT's source is an independent SELECT namespace. Its
                // regular bindings are owned by SELECT; the target alias is
                // never an additional source relation.
            } else if matches!(name.as_str(), "old" | "new")
                || (name == "excluded" && self.context == Context::Conflict)
            {
                // Trigger bodies use the ordinary parser. Their OLD/NEW rows
                // are supplied at execution; local/target bindings above win.
            } else {
                return Err(SqlError::ColumnNotFound(format!("{table}.{column}")));
            }
            return Ok(());
        }
        match expr {
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
            Expr::InSubquery { expr, subquery, .. } => {
                self.expr(expr)?;
                self.subquery(subquery)?;
            }
            Expr::Exists { subquery, .. } | Expr::ScalarSubquery(subquery) => {
                self.subquery(subquery)?
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
                for (condition, result) in conditions {
                    self.expr(condition)?;
                    self.expr(result)?;
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
                for order in &mut spec.order_by {
                    self.expr(&mut order.expr)?;
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
            Expr::Quantified { left, right, .. } => {
                self.expr(left)?;
                match right {
                    QuantifiedRhs::Subquery(select) => self.subquery(select)?,
                    QuantifiedRhs::Array(expr) => self.expr(expr)?,
                }
            }
            Expr::Literal(_)
            | Expr::Column(_)
            | Expr::BoundColumn { .. }
            | Expr::CountStar
            | Expr::Parameter(_)
            | Expr::TypedNullRecord(_)
            | Expr::QualifiedColumn { .. } => {}
        }
        Ok(())
    }

    fn subquery(&mut self, select: &mut SelectStmt) -> Result<()> {
        if matches!(self.context, Context::Returning | Context::Conflict) {
            return Err(SqlError::Unsupported(
                "subqueries in RETURNING or ON CONFLICT expressions".into(),
            ));
        }
        self.select(select)
    }

    fn projections(&mut self, columns: &mut [SelectColumn]) -> Result<()> {
        for column in columns {
            if let SelectColumn::Expr { expr, alias } = column {
                let original = alias.is_none().then(|| expr_display_name(expr));
                self.expr(expr)?;
                if let Some(original) = original {
                    if original != expr_display_name(expr) {
                        *alias = Some(original);
                    }
                }
            }
        }
        Ok(())
    }

    fn derived(&mut self, derived: &mut DerivedTable) -> Result<()> {
        let siblings = (!derived.lateral).then(|| self.scopes.pop().expect("SELECT scope"));
        self.query(&mut derived.query)?;
        if let Some(siblings) = siblings {
            self.scopes.push(siblings);
        }
        Ok(())
    }

    fn select(&mut self, select: &mut SelectStmt) -> Result<()> {
        // Reserve every sibling's original name before choosing any replacement.
        self.used.insert(
            select
                .from_alias
                .as_ref()
                .unwrap_or(&select.from)
                .to_ascii_lowercase(),
        );
        for join in &select.joins {
            self.used.insert(
                join.table
                    .alias
                    .as_ref()
                    .unwrap_or(&join.table.name)
                    .to_ascii_lowercase(),
            );
        }
        self.scopes.push(FxHashMap::default());
        if let Some(derived) = &mut select.from_subquery {
            self.derived(derived)?;
            derived.alias = self.source_alias(&derived.alias)?;
            select.from.clone_from(&derived.alias);
            if select.from_alias.is_some() {
                select.from_alias = Some(derived.alias.clone());
            }
        } else if !select.from.is_empty() {
            if let Some(args) = &mut select.from_args {
                for expr in args {
                    self.expr(expr)?;
                }
            }
            if let Some(json) = &mut select.from_json_table {
                self.expr(&mut json.source)?;
            }
            let original = select.from_alias.as_ref().unwrap_or(&select.from).clone();
            let lowered = self.source_alias(&original)?;
            if lowered != original.to_ascii_lowercase() {
                select.from_alias = Some(lowered);
            }
        }
        for join in &mut select.joins {
            if let Some(derived) = &mut join.subquery {
                self.derived(derived)?;
                derived.alias = self.source_alias(&derived.alias)?;
                join.table.name.clone_from(&derived.alias);
                if join.table.alias.is_some() {
                    join.table.alias = Some(derived.alias.clone());
                }
            } else {
                if let Some(args) = &mut join.table.args {
                    for expr in args {
                        self.expr(expr)?;
                    }
                }
                let original = join
                    .table
                    .alias
                    .as_ref()
                    .unwrap_or(&join.table.name)
                    .clone();
                let lowered = self.source_alias(&original)?;
                if lowered != original.to_ascii_lowercase() {
                    join.table.alias = Some(lowered);
                }
            }
            if let Some(on) = &mut join.on_clause {
                self.expr(on)?;
            }
        }
        if !select.joins.is_empty()
            && select
                .columns
                .iter()
                .any(|column| matches!(column, SelectColumn::AllColumns))
            && self
                .scopes
                .last()
                .unwrap()
                .iter()
                .any(|(old, new)| old != new)
        {
            return Err(SqlError::Unsupported(
                "joined wildcard projection whose source qualifier captures a DML target alias"
                    .into(),
            ));
        }
        self.projections(&mut select.columns)?;
        for expr in [
            &mut select.where_clause,
            &mut select.limit,
            &mut select.offset,
            &mut select.having,
        ]
        .into_iter()
        .flatten()
        {
            self.expr(expr)?;
        }
        for expr in &mut select.group_by {
            self.expr(expr)?;
        }
        for order in &mut select.order_by {
            self.expr(&mut order.expr)?;
        }
        self.scopes.pop();
        Ok(())
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
            QueryBody::Compound(compound) => {
                self.body(&mut compound.left)?;
                self.body(&mut compound.right)?;
                for order in &mut compound.order_by {
                    self.expr(&mut order.expr)?;
                }
                for expr in [&mut compound.limit, &mut compound.offset]
                    .into_iter()
                    .flatten()
                {
                    self.expr(expr)?;
                }
                Ok(())
            }
            QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_)
                if self.context == Context::Source =>
            {
                Ok(())
            }
            QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => Err(
                SqlError::Unsupported("DML source inside a target-alias expression".into()),
            ),
        }
    }
}

pub(super) fn lower_update(stmt: &mut UpdateStmt, alias: Option<&str>) -> Result<()> {
    if alias.is_none() {
        return reject_returning_subqueries(stmt.returning.as_deref());
    }
    let mut lower = Lowerer::new(&stmt.table, alias, Context::Assignment);
    for (_, expr) in &mut stmt.assignments {
        lower.expr(expr)?;
    }
    lower.context = Context::Predicate;
    if let Some(expr) = &mut stmt.where_clause {
        lower.expr(expr)?;
    }
    lower.context = Context::Returning;
    if let Some(columns) = &mut stmt.returning {
        lower.projections(columns)?;
    }
    Ok(())
}

pub(super) fn lower_delete(stmt: &mut DeleteStmt, alias: Option<&str>) -> Result<()> {
    if alias.is_none() {
        return reject_returning_subqueries(stmt.returning.as_deref());
    }
    let mut lower = Lowerer::new(&stmt.table, alias, Context::Predicate);
    if let Some(expr) = &mut stmt.where_clause {
        lower.expr(expr)?;
    }
    lower.context = Context::Returning;
    if let Some(columns) = &mut stmt.returning {
        lower.projections(columns)?;
    }
    Ok(())
}

pub(super) fn lower_insert(stmt: &mut InsertStmt, alias: Option<&str>) -> Result<()> {
    if alias.is_none() {
        if let Some(OnConflictClause {
            action:
                OnConflictAction::DoUpdate {
                    assignments,
                    where_clause,
                },
            ..
        }) = &stmt.on_conflict
        {
            for (_, expr) in assignments {
                reject_subqueries(expr, "ON CONFLICT")?;
            }
            if let Some(expr) = where_clause {
                reject_subqueries(expr, "ON CONFLICT")?;
            }
        }
        return reject_returning_subqueries(stmt.returning.as_deref());
    }
    let mut lower = Lowerer::new(&stmt.table, alias, Context::Source);
    match &mut stmt.source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    lower.expr(expr)?;
                }
            }
        }
        InsertSource::Select(query) => lower.query(query)?,
    }
    lower.context = Context::Conflict;
    if let Some(OnConflictClause {
        action:
            OnConflictAction::DoUpdate {
                assignments,
                where_clause,
            },
        ..
    }) = &mut stmt.on_conflict
    {
        for (_, expr) in assignments {
            lower.expr(expr)?;
        }
        if let Some(expr) = where_clause {
            lower.expr(expr)?;
        }
    }
    lower.context = Context::Returning;
    if let Some(columns) = &mut stmt.returning {
        lower.projections(columns)?;
    }
    Ok(())
}

pub(super) fn convert_returning(
    items: Option<&[sp::SelectItem]>,
    alias: Option<&str>,
) -> Result<Option<Vec<SelectColumn>>> {
    items
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    if let sp::SelectItem::QualifiedWildcard(
                        sp::SelectItemQualifiedWildcardKind::ObjectName(name),
                        options,
                    ) = item
                    {
                        let qualifier = object_name_to_string(name);
                        if alias.is_some_and(|alias| qualifier.eq_ignore_ascii_case(alias)) {
                            validate_wildcard_options(options)?;
                            return Ok(SelectColumn::AllColumns);
                        }
                    }
                    convert_returning_item(item)
                })
                .collect()
        })
        .transpose()
}

fn reject_returning_subqueries(columns: Option<&[SelectColumn]>) -> Result<()> {
    for column in columns.into_iter().flatten() {
        if let SelectColumn::Expr { expr, .. } = column {
            reject_subqueries(expr, "RETURNING")?;
        }
    }
    Ok(())
}

fn reject_subqueries(expr: &Expr, context: &str) -> Result<()> {
    if has_subquery(expr) {
        Err(SqlError::Unsupported(format!("subqueries in {context}")))
    } else {
        Ok(())
    }
}
