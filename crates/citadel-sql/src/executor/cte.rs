use citadel::Database;

use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::aggregate::*;
use super::CteContext;

// ── CTE support ──────────────────────────────────────────────────────

pub(super) fn exec_select_query(
    db: &Database,
    schema: &SchemaManager,
    sq: &SelectQuery,
) -> Result<ExecutionResult> {
    if let Some(fused) = try_fuse_cte(sq) {
        let empty = CteContext::new();
        return super::exec_query_body(db, schema, &fused, &empty);
    }
    let ctes = materialize_all_ctes(&sq.ctes, sq.recursive, &mut |body, ctx| {
        super::exec_query_body_read(db, schema, body, ctx)
    })?;
    super::exec_query_body(db, schema, &sq.body, &ctes)
}

pub(super) fn exec_select_query_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    sq: &SelectQuery,
) -> Result<ExecutionResult> {
    if let Some(fused) = try_fuse_cte(sq) {
        let empty = CteContext::new();
        return super::exec_query_body_in_txn(wtx, schema, &fused, &empty);
    }
    let ctes = materialize_all_ctes(&sq.ctes, sq.recursive, &mut |body, ctx| {
        super::exec_query_body_write(wtx, schema, body, ctx)
    })?;
    super::exec_query_body_in_txn(wtx, schema, &sq.body, &ctes)
}

/// Inline a single simple CTE into a direct query against the real table.
pub(super) fn try_fuse_cte(sq: &SelectQuery) -> Option<QueryBody> {
    if sq.ctes.len() != 1 || sq.recursive {
        return None;
    }
    let cte = &sq.ctes[0];
    if !cte.column_aliases.is_empty() {
        return None;
    }

    let inner = match &cte.body {
        QueryBody::Select(s) => s.as_ref(),
        _ => return None,
    };

    if !inner.joins.is_empty()
        || !inner.group_by.is_empty()
        || inner.distinct
        || inner.having.is_some()
        || inner.limit.is_some()
        || inner.offset.is_some()
        || !inner.order_by.is_empty()
        || super::stmt_has_subquery(inner)
    {
        return None;
    }

    let all_simple_refs = inner.columns.iter().all(|c| match c {
        SelectColumn::AllColumns => true,
        SelectColumn::Expr { expr, alias } => alias.is_none() && matches!(expr, Expr::Column(_)),
    });
    if !all_simple_refs {
        return None;
    }

    let outer = match &sq.body {
        QueryBody::Select(s) => s.as_ref(),
        _ => return None,
    };
    if !outer.from.eq_ignore_ascii_case(&cte.name) || !outer.joins.is_empty() {
        return None;
    }

    let merged_where = match (&inner.where_clause, &outer.where_clause) {
        (Some(iw), Some(ow)) => Some(Expr::BinaryOp {
            left: Box::new(iw.clone()),
            op: BinOp::And,
            right: Box::new(ow.clone()),
        }),
        (Some(w), None) | (None, Some(w)) => Some(w.clone()),
        (None, None) => None,
    };

    let fused = SelectStmt {
        columns: outer.columns.clone(),
        from: inner.from.clone(),
        from_alias: inner.from_alias.clone(),
        joins: vec![],
        distinct: outer.distinct,
        where_clause: merged_where,
        order_by: outer.order_by.clone(),
        limit: outer.limit.clone(),
        offset: outer.offset.clone(),
        group_by: outer.group_by.clone(),
        having: outer.having.clone(),
    };

    Some(QueryBody::Select(Box::new(fused)))
}

pub(super) fn materialize_all_ctes(
    defs: &[CteDefinition],
    recursive: bool,
    exec_body: &mut dyn FnMut(&QueryBody, &CteContext) -> Result<QueryResult>,
) -> Result<CteContext> {
    let mut ctx = CteContext::new();
    for cte in defs {
        let qr = if recursive && cte_body_references_self(&cte.body, &cte.name) {
            materialize_recursive_cte(cte, &ctx, exec_body)?
        } else {
            materialize_cte(cte, &ctx, exec_body)?
        };
        ctx.insert(cte.name.clone(), qr);
    }
    Ok(ctx)
}

pub(super) fn materialize_cte(
    cte: &CteDefinition,
    ctx: &CteContext,
    exec_body: &mut dyn FnMut(&QueryBody, &CteContext) -> Result<QueryResult>,
) -> Result<QueryResult> {
    let mut qr = exec_body(&cte.body, ctx)?;
    if !cte.column_aliases.is_empty() {
        if cte.column_aliases.len() != qr.columns.len() {
            return Err(SqlError::CteColumnAliasMismatch {
                name: cte.name.clone(),
                expected: cte.column_aliases.len(),
                got: qr.columns.len(),
            });
        }
        qr.columns = cte.column_aliases.clone();
    }
    Ok(qr)
}

const MAX_RECURSIVE_ITERATIONS: usize = 10_000;

pub(super) fn materialize_recursive_cte(
    cte: &CteDefinition,
    ctx: &CteContext,
    exec_body: &mut dyn FnMut(&QueryBody, &CteContext) -> Result<QueryResult>,
) -> Result<QueryResult> {
    let (anchor_body, recursive_body, union_all) = match &cte.body {
        QueryBody::Compound(comp) if matches!(comp.op, SetOp::Union) => {
            (&*comp.left, &*comp.right, comp.all)
        }
        _ => return Err(SqlError::RecursiveCteNoUnion(cte.name.clone())),
    };

    let anchor_qr = exec_body(anchor_body, ctx)?;
    let columns = if !cte.column_aliases.is_empty() {
        if cte.column_aliases.len() != anchor_qr.columns.len() {
            return Err(SqlError::CteColumnAliasMismatch {
                name: cte.name.clone(),
                expected: cte.column_aliases.len(),
                got: anchor_qr.columns.len(),
            });
        }
        cte.column_aliases.clone()
    } else {
        anchor_qr.columns
    };

    let mut accumulated = anchor_qr.rows;
    let mut work_start = 0;
    let mut work_end = accumulated.len();
    let mut seen = if !union_all {
        let mut s = std::collections::HashSet::new();
        for row in &accumulated {
            s.insert(row.clone());
        }
        Some(s)
    } else {
        None
    };

    let cte_key = cte.name.clone();

    let fast_sel = match recursive_body {
        QueryBody::Select(sel)
            if sel.from.eq_ignore_ascii_case(&cte_key)
                && sel.joins.is_empty()
                && sel.group_by.is_empty()
                && !sel.distinct
                && sel.having.is_none()
                && sel.limit.is_none()
                && sel.offset.is_none()
                && sel.order_by.is_empty()
                && !super::stmt_has_subquery(sel) =>
        {
            Some(sel.as_ref())
        }
        _ => None,
    };

    if let Some(sel) = fast_sel {
        let cte_cols: Vec<ColumnDef> = columns
            .iter()
            .enumerate()
            .map(|(i, name)| ColumnDef {
                name: name.clone(),
                data_type: DataType::Null,
                nullable: true,
                position: i as u16,
                default_expr: None,
                default_sql: None,
                check_expr: None,
                check_sql: None,
                check_name: None,
            })
            .collect();
        let col_map = ColumnMap::new(&cte_cols);
        let ncols = sel.columns.len();

        let mut step_rows: Vec<Vec<Value>> = Vec::new();
        let mut row_buf: Vec<Value> = Vec::with_capacity(ncols);
        for iteration in 0..MAX_RECURSIVE_ITERATIONS {
            if work_start >= work_end {
                break;
            }

            step_rows.clear();
            for row in &accumulated[work_start..work_end] {
                if let Some(ref w) = sel.where_clause {
                    match eval_expr(w, &col_map, row) {
                        Ok(val) if is_truthy(&val) => {}
                        Ok(_) => continue,
                        Err(e) => return Err(e),
                    }
                }
                row_buf.clear();
                for col in &sel.columns {
                    match col {
                        SelectColumn::Expr { expr, .. } => {
                            row_buf.push(eval_expr(expr, &col_map, row)?);
                        }
                        SelectColumn::AllColumns => {
                            row_buf.extend_from_slice(row);
                        }
                    }
                }
                step_rows.push(std::mem::replace(&mut row_buf, Vec::with_capacity(ncols)));
            }

            if step_rows.is_empty() {
                break;
            }

            if let Some(ref mut seen_set) = seen {
                step_rows.retain(|r| seen_set.insert(r.clone()));
            }

            if step_rows.is_empty() {
                break;
            }

            work_start = accumulated.len();
            accumulated.append(&mut step_rows);
            work_end = accumulated.len();

            if iteration == MAX_RECURSIVE_ITERATIONS - 1 {
                return Err(SqlError::RecursiveCteMaxIterations(
                    cte_key.clone(),
                    MAX_RECURSIVE_ITERATIONS,
                ));
            }
        }
    } else {
        let working_rows = accumulated[work_start..work_end].to_vec();
        let mut iter_ctx = ctx.clone();
        iter_ctx.insert(
            cte_key.clone(),
            QueryResult {
                columns: columns.clone(),
                rows: working_rows,
            },
        );

        for iteration in 0..MAX_RECURSIVE_ITERATIONS {
            if iter_ctx.get(&cte_key).unwrap().rows.is_empty() {
                break;
            }

            let iter_qr = exec_body(recursive_body, &iter_ctx)?;
            if iter_qr.rows.is_empty() {
                break;
            }

            let new_rows = if let Some(ref mut seen_set) = seen {
                iter_qr
                    .rows
                    .into_iter()
                    .filter(|r| seen_set.insert(r.clone()))
                    .collect::<Vec<_>>()
            } else {
                iter_qr.rows
            };

            if new_rows.is_empty() {
                break;
            }

            accumulated.extend_from_slice(&new_rows);
            iter_ctx.get_mut(&cte_key).unwrap().rows = new_rows;

            if iteration == MAX_RECURSIVE_ITERATIONS - 1 {
                return Err(SqlError::RecursiveCteMaxIterations(
                    cte_key.clone(),
                    MAX_RECURSIVE_ITERATIONS,
                ));
            }
        }

        iter_ctx.remove(&cte_key);
    }

    Ok(QueryResult {
        columns,
        rows: accumulated,
    })
}

pub(super) fn cte_body_references_self(body: &QueryBody, name: &str) -> bool {
    match body {
        QueryBody::Select(sel) => {
            sel.from.eq_ignore_ascii_case(name)
                || sel
                    .joins
                    .iter()
                    .any(|j| j.table.name.eq_ignore_ascii_case(name))
        }
        QueryBody::Compound(comp) => {
            cte_body_references_self(&comp.left, name)
                || cte_body_references_self(&comp.right, name)
        }
    }
}

pub(super) fn build_cte_schema(name: &str, qr: &QueryResult) -> TableSchema {
    let columns: Vec<ColumnDef> = qr
        .columns
        .iter()
        .enumerate()
        .map(|(i, col_name)| ColumnDef {
            name: col_name.clone(),
            data_type: DataType::Null,
            nullable: true,
            position: i as u16,
            default_expr: None,
            default_sql: None,
            check_expr: None,
            check_sql: None,
            check_name: None,
        })
        .collect();
    TableSchema::new(name.into(), columns, vec![], vec![], vec![], vec![])
}

pub(super) fn exec_select_from_cte(
    cte_result: &QueryResult,
    stmt: &SelectStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<QueryResult>,
) -> Result<ExecutionResult> {
    let cte_schema = build_cte_schema(&stmt.from, cte_result);
    let actual_stmt;
    let s = if super::stmt_has_subquery(stmt) {
        actual_stmt = super::materialize_stmt(stmt, exec_sub)?;
        &actual_stmt
    } else {
        stmt
    };

    let has_aggregates = s.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    });

    if has_aggregates || !s.group_by.is_empty() {
        if let Some(ref where_expr) = s.where_clause {
            let col_map = ColumnMap::new(&cte_schema.columns);
            let filtered: Vec<Vec<Value>> = cte_result
                .rows
                .iter()
                .filter(|row| match eval_expr(where_expr, &col_map, row) {
                    Ok(val) => is_truthy(&val),
                    _ => false,
                })
                .cloned()
                .collect();
            return exec_aggregate(&cte_schema.columns, &filtered, s);
        }
        return exec_aggregate(&cte_schema.columns, &cte_result.rows, s);
    }

    super::process_select(&cte_schema.columns, cte_result.rows.clone(), s, false)
}
