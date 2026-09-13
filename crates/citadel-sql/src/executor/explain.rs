use crate::error::{Result, SqlError};
use crate::parser::*;
use crate::planner::{self, ScanPlan};
use crate::schema::SchemaManager;
use crate::types::*;

use super::helpers::*;
use super::window::has_any_window_function;

/// How EXPLAIN learns a table's size. `Ok(None)` for anything with no catalog
/// descriptor to read: a CTE, a view, or a table that does not exist. The
/// result stays fallible so cancellation cannot be mistaken for an unknown
/// estimate.
pub(super) type RowCountFn<'a> = &'a mut dyn FnMut(&str) -> Result<Option<u64>>;

/// What an ANALYZE run observed. Flat rather than a tree mirroring the plan: a
/// fused strategy does the scan, filter, sort and limit in one pass, so there is
/// one duration to report and per-line times would be fabrication.
#[derive(Debug, Default, Clone)]
pub(super) struct Measured {
    pub elapsed: std::time::Duration,
    /// Rows the result actually returned.
    pub emitted: u64,
    /// Rows read from storage, when the strategy counts them. `None` when the
    /// lane that ran keeps no such counter, which is stated rather than guessed.
    pub scanned: Option<u64>,
}

impl Measured {
    /// The one constructor, so `emitted` always comes from the result the
    /// statement produced. `scanned` counts storage entries examined, including
    /// rows a filter discarded - which is why it sits next to `emitted`.
    pub(super) fn observed(
        elapsed: std::time::Duration,
        result: &ExecutionResult,
        scanned: u64,
    ) -> Self {
        let emitted = match result {
            ExecutionResult::Query(qr) => qr.rows.len() as u64,
            ExecutionResult::RowsAffected(n) => *n,
            ExecutionResult::Ok => 0,
        };
        Self {
            elapsed,
            emitted,
            // Zero is ambiguous: an empty counted scan and a lane such as a
            // point lookup that has no scan counter both produce it. Omitting
            // the field is honest; reporting `scanned=0 emitted=1` is not.
            scanned: (scanned > 0).then_some(scanned),
        }
    }

    /// The `(actual time=.. emitted=.. scanned=..)` suffix for a plan line.
    fn suffix(&self) -> String {
        let ms = self.elapsed.as_secs_f64() * 1000.0;
        match self.scanned {
            Some(n) => format!(
                " (actual time={ms:.3}ms scanned={n} emitted={})",
                self.emitted
            ),
            None => format!(" (actual time={ms:.3}ms emitted={})", self.emitted),
        }
    }
}

/// What every explain function needs.
pub(super) struct ExplainCtx<'a> {
    pub schema: &'a SchemaManager,
    pub rows: RowCountFn<'a>,
}

impl ExplainCtx<'_> {
    /// The exact entry count for `table`, or `None` when it has no descriptor.
    fn row_count(&mut self, table: &str) -> Result<Option<u64>> {
        let storage_name = self
            .schema
            .get(table)
            .map_or(table, |table_schema| table_schema.name.as_str());
        (self.rows)(storage_name)
    }

    /// Borrow the context for a nested call without moving it.
    fn reborrow(&mut self) -> ExplainCtx<'_> {
        ExplainCtx {
            schema: self.schema,
            rows: &mut *self.rows,
        }
    }
}

/// Attach one completed execution's measurements to its already-built plan.
/// Building first both validates the plan before side effects and preserves
/// pre-mutation row counts for ANALYZE of UPDATE or DELETE.
pub(super) fn attach_measurement(result: &mut ExecutionResult, measured: &Measured) {
    let ExecutionResult::Query(query) = result else {
        debug_assert!(false, "EXPLAIN returned a non-query result");
        return;
    };
    let Some(Value::Text(line)) = query.rows.first_mut().and_then(|row| row.first_mut()) else {
        debug_assert!(false, "EXPLAIN returned no root plan line");
        return;
    };
    line.push_str(&measured.suffix());
}

pub(super) fn explain(ctx: &mut ExplainCtx<'_>, stmt: &Statement) -> Result<ExecutionResult> {
    let lines = match stmt {
        Statement::Select(sq) => {
            let mut lines = Vec::new();
            let cte_names: Vec<&str> = sq.ctes.iter().map(|c| c.name.as_str()).collect();
            for cte in &sq.ctes {
                lines.push(format!("WITH {} AS", cte.name));
                lines.extend(
                    explain_query_body_cte(ctx, &cte.body, &cte_names)?
                        .into_iter()
                        .map(|l| format!("  {l}")),
                );
            }
            lines.extend(explain_query_body_cte(ctx, &sq.body, &cte_names)?);
            lines
        }
        Statement::Insert(ins) => match &ins.source {
            InsertSource::Values(rows) => {
                vec![format!(
                    "INSERT INTO {} ({} rows)",
                    ins.table.to_ascii_lowercase(),
                    rows.len()
                )]
            }
            InsertSource::Select(sq) => {
                let mut lines = vec![format!(
                    "INSERT INTO {} ... SELECT",
                    ins.table.to_ascii_lowercase()
                )];
                let cte_names: Vec<&str> = sq.ctes.iter().map(|c| c.name.as_str()).collect();
                for cte in &sq.ctes {
                    lines.push(format!("  WITH {} AS", cte.name));
                    lines.extend(
                        explain_query_body_cte(ctx, &cte.body, &cte_names)?
                            .into_iter()
                            .map(|l| format!("    {l}")),
                    );
                }
                lines.extend(explain_query_body_cte(ctx, &sq.body, &cte_names)?);
                lines
            }
        },
        Statement::Update(upd) => explain_dml(ctx, &upd.table, &upd.where_clause, "UPDATE")?,
        Statement::Delete(del) => explain_dml(ctx, &del.table, &del.where_clause, "DELETE FROM")?,
        Statement::AlterTable(at) => {
            let desc = match &at.op {
                AlterTableOp::AddColumn { column, .. } => {
                    format!("ALTER TABLE {} ADD COLUMN {}", at.table, column.name)
                }
                AlterTableOp::DropColumn { name, .. } => {
                    format!("ALTER TABLE {} DROP COLUMN {}", at.table, name)
                }
                AlterTableOp::RenameColumn {
                    old_name, new_name, ..
                } => {
                    format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        at.table, old_name, new_name
                    )
                }
                AlterTableOp::RenameTable { new_name } => {
                    format!("ALTER TABLE {} RENAME TO {}", at.table, new_name)
                }
                AlterTableOp::DisableTrigger { name } => {
                    format!("ALTER TABLE {} DISABLE TRIGGER {}", at.table, name)
                }
                AlterTableOp::EnableTrigger { name } => {
                    format!("ALTER TABLE {} ENABLE TRIGGER {}", at.table, name)
                }
                AlterTableOp::DisableAllTriggers => {
                    format!("ALTER TABLE {} DISABLE TRIGGER ALL", at.table)
                }
                AlterTableOp::EnableAllTriggers => {
                    format!("ALTER TABLE {} ENABLE TRIGGER ALL", at.table)
                }
            };
            vec![desc]
        }
        Statement::CreateView(cv) => {
            vec![format!("CREATE VIEW {}", cv.name.to_ascii_lowercase())]
        }
        Statement::DropView(dv) => {
            vec![format!("DROP VIEW {}", dv.name.to_ascii_lowercase())]
        }
        Statement::Explain { .. } => {
            return Err(SqlError::Unsupported("EXPLAIN EXPLAIN".into()));
        }
        _ => {
            return Err(SqlError::Unsupported(
                "EXPLAIN for this statement type".into(),
            ));
        }
    };

    let rows = lines
        .into_iter()
        .map(|line| vec![Value::Text(line.into())])
        .collect();
    Ok(ExecutionResult::Query(QueryResult {
        columns: vec!["plan".into()],
        rows,
    }))
}

pub(super) fn explain_dml(
    ctx: &mut ExplainCtx<'_>,
    table: &str,
    where_clause: &Option<Expr>,
    verb: &str,
) -> Result<Vec<String>> {
    let lower = table.to_ascii_lowercase();
    let table_schema = ctx
        .schema
        .get(&lower)
        .ok_or_else(|| SqlError::TableNotFound(table.to_string()))?;
    let plan = planner::plan_select(table_schema, where_clause);
    let rows = ctx.row_count(&table_schema.name)?;
    let scan_line = format_scan_line(&lower, &None, &plan, table_schema, rows);
    Ok(vec![format!("{verb} {}", scan_line)])
}

pub(super) fn explain_query_body_cte(
    ctx: &mut ExplainCtx<'_>,
    body: &QueryBody,
    cte_names: &[&str],
) -> Result<Vec<String>> {
    match body {
        QueryBody::Select(sel) => explain_select_cte(ctx, sel, cte_names),
        QueryBody::Compound(comp) => {
            let op_name = match (&comp.op, comp.all) {
                (SetOp::Union, true) => "UNION ALL",
                (SetOp::Union, false) => "UNION",
                (SetOp::Intersect, true) => "INTERSECT ALL",
                (SetOp::Intersect, false) => "INTERSECT",
                (SetOp::Except, true) => "EXCEPT ALL",
                (SetOp::Except, false) => "EXCEPT",
            };
            let mut lines = vec![op_name.to_string()];
            let left_lines = explain_query_body_cte(ctx, &comp.left, cte_names)?;
            for l in left_lines {
                lines.push(format!("  {l}"));
            }
            let right_lines = explain_query_body_cte(ctx, &comp.right, cte_names)?;
            for l in right_lines {
                lines.push(format!("  {l}"));
            }
            Ok(lines)
        }
        QueryBody::Insert(ins) => Ok(vec![format!("INSERT INTO {}", ins.table)]),
        QueryBody::Update(upd) => Ok(vec![format!("UPDATE {}", upd.table)]),
        QueryBody::Delete(del) => Ok(vec![format!("DELETE FROM {}", del.table)]),
    }
}

pub(super) fn explain_select_cte(
    ctx: &mut ExplainCtx<'_>,
    stmt: &SelectStmt,
    cte_names: &[&str],
) -> Result<Vec<String>> {
    let schema = ctx.schema;
    let mut lines = Vec::new();

    if stmt.from.is_empty() {
        lines.push("CONSTANT ROW".into());
        return Ok(lines);
    }

    let lower_from = stmt.from.to_ascii_lowercase();

    if cte_names
        .iter()
        .any(|n| n.eq_ignore_ascii_case(&lower_from))
    {
        lines.push(format!("SCAN CTE {lower_from}"));
        for join in &stmt.joins {
            let jname = join.table.name.to_ascii_lowercase();
            if cte_names.iter().any(|n| n.eq_ignore_ascii_case(&jname)) {
                lines.push(format!("SCAN CTE {jname}"));
            } else {
                let js = schema
                    .get(&jname)
                    .ok_or_else(|| SqlError::TableNotFound(join.table.name.clone()))?;
                let jp = planner::plan_select(js, &None);
                let n = ctx.row_count(&js.name)?;
                lines.push(format_scan_line(&jname, &join.table.alias, &jp, js, n));
            }
        }
        if !stmt.joins.is_empty() {
            lines.push("NESTED LOOP".into());
        }
        if has_any_window_function(stmt) {
            lines.push("WINDOW".into());
        }
        if !stmt.group_by.is_empty() {
            lines.push("GROUP BY".into());
        }
        if stmt.distinct {
            lines.push("DISTINCT".into());
        }
        if !stmt.order_by.is_empty() {
            lines.push("SORT".into());
        }
        if stmt.limit.is_some() {
            lines.push("LIMIT".into());
        }
        return Ok(lines);
    }

    if let Some(view_def) = schema.get_view(&lower_from) {
        if let Ok(Some(fused)) =
            super::try_fuse_view(stmt, schema, view_def, cte_names.iter().copied())
        {
            // Fused — explain against real table
            return explain_select_cte(&mut ctx.reborrow(), &fused, cte_names);
        }
        lines.push(format!("SCAN VIEW {lower_from}"));
        if !stmt.order_by.is_empty() {
            lines.push("SORT".into());
        }
        if stmt.limit.is_some() {
            lines.push("LIMIT".into());
        }
        return Ok(lines);
    }

    let from_schema = schema
        .get(&lower_from)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;

    if stmt.joins.is_empty() {
        // The same decision the executor acts on. Without this, a query that
        // runs a fused streaming aggregate was described here as an ordinary
        // scan plus an aggregate step - a plan that never runs.
        let strategy = super::select::choose_strategy(stmt, from_schema)?;
        let plan = planner::plan_select(from_schema, &stmt.where_clause);
        let n = ctx.row_count(&from_schema.name)?;
        let mut line = format_scan_line(&lower_from, &stmt.from_alias, &plan, from_schema, n);
        if super::select::select_would_cover(schema, stmt) {
            line.push_str(" COVERING");
        }
        match strategy.label() {
            // A fused strategy is one node: it does the scan and everything
            // above it in a single pass, so it is reported as one line rather
            // than as a scan with separate steps stacked on top.
            Some(label) => {
                lines.push(format!("{label} over {line}"));
                if stmt.limit.is_some() {
                    lines.push("LIMIT".into());
                }
                return Ok(lines);
            }
            None => lines.push(line),
        }
    } else {
        let from_plan = planner::plan_select(from_schema, &None);
        let n = ctx.row_count(&from_schema.name)?;
        lines.push(format_scan_line(
            &lower_from,
            &stmt.from_alias,
            &from_plan,
            from_schema,
            n,
        ));

        for join in &stmt.joins {
            let inner_lower = join.table.name.to_ascii_lowercase();
            if cte_names
                .iter()
                .any(|n| n.eq_ignore_ascii_case(&inner_lower))
            {
                lines.push(format!("SCAN CTE {inner_lower}"));
            } else {
                let inner_schema = schema
                    .get(&inner_lower)
                    .ok_or_else(|| SqlError::TableNotFound(join.table.name.clone()))?;
                let inner_plan = planner::plan_select(inner_schema, &None);
                let n = ctx.row_count(&inner_schema.name)?;
                lines.push(format_scan_line(
                    &inner_lower,
                    &join.table.alias,
                    &inner_plan,
                    inner_schema,
                    n,
                ));
            }
        }

        let join_type_str = match stmt.joins.last().map(|j| j.join_type) {
            Some(JoinType::Left) => "LEFT JOIN",
            Some(JoinType::Right) => "RIGHT JOIN",
            Some(JoinType::Cross) => "CROSS JOIN",
            Some(JoinType::FullOuter) => "FULL OUTER JOIN",
            _ => "NESTED LOOP",
        };
        lines.push(join_type_str.into());
    }

    if stmt.where_clause.is_some() && stmt.joins.is_empty() {
        let plan = planner::plan_select(from_schema, &stmt.where_clause);
        if matches!(plan, ScanPlan::SeqScan) {
            lines.push("FILTER".into());
        }
    }

    if let Some(ref w) = stmt.where_clause {
        if !stmt.joins.is_empty() && has_subquery(w) {
            lines.push("SUBQUERY".into());
        }
    }

    explain_subqueries(stmt, &mut lines);

    if has_any_window_function(stmt) {
        lines.push("WINDOW".into());
    }

    if !stmt.group_by.is_empty() {
        lines.push("GROUP BY".into());
    }

    if stmt.distinct {
        lines.push("DISTINCT".into());
    }

    if !stmt.order_by.is_empty() {
        lines.push("SORT".into());
    }

    if let Some(ref offset_expr) = stmt.offset {
        if let Ok(n) = eval_const_int(offset_expr) {
            lines.push(format!("OFFSET {n}"));
        } else {
            lines.push("OFFSET".into());
        }
    }

    if let Some(ref limit_expr) = stmt.limit {
        if let Ok(n) = eval_const_int(limit_expr) {
            lines.push(format!("LIMIT {n}"));
        } else {
            lines.push("LIMIT".into());
        }
    }

    Ok(lines)
}

pub(super) fn explain_subqueries(stmt: &SelectStmt, lines: &mut Vec<String>) {
    let mut count = 0;
    if let Some(ref w) = stmt.where_clause {
        count += count_subqueries(w);
    }
    if let Some(ref h) = stmt.having {
        count += count_subqueries(h);
    }
    for col in &stmt.columns {
        if let SelectColumn::Expr { expr, .. } = col {
            count += count_subqueries(expr);
        }
    }
    for _ in 0..count {
        lines.push("SUBQUERY".into());
    }
}

pub(super) fn count_subqueries(expr: &Expr) -> usize {
    match expr {
        Expr::InSubquery { expr: e, .. } => 1 + count_subqueries(e),
        Expr::ScalarSubquery(_) => 1,
        Expr::Exists { .. } => 1,
        Expr::BinaryOp { left, right, .. } => count_subqueries(left) + count_subqueries(right),
        Expr::UnaryOp { expr: e, .. } => count_subqueries(e),
        Expr::IsNull(e) | Expr::IsNotNull(e) => count_subqueries(e),
        Expr::Function { args, .. } => args.iter().map(count_subqueries).sum(),
        Expr::Between {
            expr: e, low, high, ..
        } => count_subqueries(e) + count_subqueries(low) + count_subqueries(high),
        Expr::Like {
            expr: e, pattern, ..
        } => count_subqueries(e) + count_subqueries(pattern),
        Expr::IsDistinctFrom { left, right, .. } => {
            count_subqueries(left) + count_subqueries(right)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let mut n = 0;
            if let Some(op) = operand {
                n += count_subqueries(op);
            }
            for (c, r) in conditions {
                n += count_subqueries(c) + count_subqueries(r);
            }
            if let Some(el) = else_result {
                n += count_subqueries(el);
            }
            n
        }
        Expr::Coalesce(args) => args.iter().map(count_subqueries).sum(),
        Expr::Cast { expr: e, .. } => count_subqueries(e),
        Expr::InList { expr: e, list, .. } => {
            count_subqueries(e) + list.iter().map(count_subqueries).sum::<usize>()
        }
        _ => 0,
    }
}

pub(super) fn format_scan_line(
    table_name: &str,
    alias: &Option<String>,
    plan: &ScanPlan,
    table_schema: &TableSchema,
    rows: Option<u64>,
) -> String {
    let alias_part = match alias {
        Some(a) if !a.eq_ignore_ascii_case(table_name) => {
            format!(" AS {}", a.to_ascii_lowercase())
        }
        _ => String::new(),
    };

    let desc = planner::describe_plan(plan, table_schema);

    // The count is the table's exact size from the catalog, never an estimate
    // of what the line returns. On a full scan those are the same number, so it
    // reads as `rows=N`; on a search they are not, so it says what N is instead
    // of implying the search returns that many.
    match (desc.is_empty(), rows) {
        (true, Some(n)) => format!("SCAN TABLE {table_name}{alias_part} rows={n}"),
        (true, None) => format!("SCAN TABLE {table_name}{alias_part}"),
        (false, Some(n)) => format!("SEARCH TABLE {table_name}{alias_part} {desc} of {n} rows"),
        (false, None) => format!("SEARCH TABLE {table_name}{alias_part} {desc}"),
    }
}

#[cfg(test)]
#[path = "explain_tests.rs"]
mod tests;
