use citadel::Database;
use citadel_txn::read_txn::ReadTxn;

use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::aggregate::*;
use super::{CteContext, CteRows};

pub(super) fn exec_select_query(
    db: &Database,
    schema: &SchemaManager,
    sq: &SelectQuery,
) -> Result<ExecutionResult> {
    if any_dml_cte(sq) {
        let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
        let result = exec_select_query_in_txn(&mut wtx, schema, sq)?;
        super::commit_with_ann_publication(wtx, schema)?;
        return Ok(result);
    }
    let mut rtx = db.begin_read();
    exec_select_query_with_read(&mut rtx, schema, sq)
}

pub(super) fn exec_select_query_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    sq: &SelectQuery,
) -> Result<ExecutionResult> {
    if any_dml_cte(sq) {
        return Err(SqlError::Unsupported(
            "DML CTE bodies require an active write transaction".into(),
        ));
    }
    if sq.ctes.is_empty() {
        let empty = CteContext::default();
        return super::exec_query_body_with_read(rtx, schema, &sq.body, &empty);
    }
    if let Some(fused) = try_fuse_cte(sq) {
        let empty = CteContext::default();
        return super::exec_query_body_with_read(rtx, schema, &fused, &empty);
    }
    let cancel = rtx.cancel_token().cloned();
    let ctes = materialize_all_ctes(&sq.ctes, sq.recursive, cancel.as_ref(), &mut |body, ctx| {
        let result = super::exec_query_body_with_read_qr(rtx, schema, body, ctx)?;
        let collations =
            super::dml::body_output_collations(schema, ctx, body, result.columns.len());
        Ok(CteRows::new(result, collations))
    })?;
    super::exec_query_body_with_read(rtx, schema, &sq.body, &ctes)
}

fn any_dml_cte(sq: &SelectQuery) -> bool {
    sq.ctes.iter().any(|cte| {
        matches!(
            &cte.body,
            QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_)
        )
    })
}

pub(super) fn exec_select_query_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    sq: &SelectQuery,
) -> Result<ExecutionResult> {
    if sq.ctes.is_empty() {
        let empty = CteContext::default();
        return super::exec_query_body_in_txn(wtx, schema, &sq.body, &empty);
    }
    if let Some(fused) = try_fuse_cte(sq) {
        let empty = CteContext::default();
        return super::exec_query_body_in_txn(wtx, schema, &fused, &empty);
    }
    let cancel = wtx.cancel_token().cloned();
    let ctes = materialize_all_ctes(&sq.ctes, sq.recursive, cancel.as_ref(), &mut |body, ctx| {
        let result = super::exec_query_body_write(wtx, schema, body, ctx)?;
        let collations =
            super::dml::body_output_collations(schema, ctx, body, result.columns.len());
        Ok(CteRows::new(result, collations))
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
        SelectColumn::AllFromOld | SelectColumn::AllFromNew => false,
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
        from_subquery: inner.from_subquery.clone(),
        from_args: inner.from_args.clone(),
        from_json_table: inner.from_json_table.clone(),
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
    cancel: Option<&citadel::CancelToken>,
    exec_body: &mut dyn FnMut(&QueryBody, &CteContext) -> Result<CteRows>,
) -> Result<CteContext> {
    materialize_all_ctes_with_outer(defs, recursive, &CteContext::default(), cancel, exec_body)
}

pub(super) fn materialize_all_ctes_with_outer(
    defs: &[CteDefinition],
    recursive: bool,
    outer: &CteContext,
    cancel: Option<&citadel::CancelToken>,
    exec_body: &mut dyn FnMut(&QueryBody, &CteContext) -> Result<CteRows>,
) -> Result<CteContext> {
    super::check_cancelled(cancel)?;
    if recursive {
        for cte in defs {
            if matches!(
                &cte.body,
                QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_)
            ) {
                return Err(SqlError::Unsupported(
                    "Recursive self-references in data-modifying statements are not allowed".into(),
                ));
            }
        }
    }
    let mut ctx = outer.clone();
    for cte in defs {
        super::check_cancelled(cancel)?;
        let qr = if recursive && cte_body_references_self(&cte.body, &cte.name) {
            materialize_recursive_cte(cte, &ctx, cancel, exec_body)?
        } else {
            materialize_cte(cte, &ctx, exec_body)?
        };
        ctx.insert(cte.name.clone(), qr.shared());
    }
    Ok(ctx)
}

pub(super) fn materialize_cte(
    cte: &CteDefinition,
    ctx: &CteContext,
    exec_body: &mut dyn FnMut(&QueryBody, &CteContext) -> Result<CteRows>,
) -> Result<CteRows> {
    let mut rows = exec_body(&cte.body, ctx)?;
    if !cte.column_aliases.is_empty() {
        if cte.column_aliases.len() != rows.result.columns.len() {
            return Err(SqlError::CteColumnAliasMismatch {
                name: cte.name.clone(),
                expected: cte.column_aliases.len(),
                got: rows.result.columns.len(),
            });
        }
        // Renaming a column does not change what it was projected from, so the collations
        // stay as they are.
        rows.result.columns = cte.column_aliases.clone();
    }
    Ok(rows)
}

const MAX_RECURSIVE_ITERATIONS: usize = 10_000;

pub(super) fn materialize_recursive_cte(
    cte: &CteDefinition,
    ctx: &CteContext,
    cancel: Option<&citadel::CancelToken>,
    exec_body: &mut dyn FnMut(&QueryBody, &CteContext) -> Result<CteRows>,
) -> Result<CteRows> {
    super::check_cancelled(cancel)?;
    if matches!(
        &cte.body,
        QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_)
    ) {
        return Err(SqlError::Unsupported(
            "Recursive self-references in data-modifying statements are not allowed".into(),
        ));
    }
    let (anchor_body, recursive_body, union_all) = match &cte.body {
        QueryBody::Compound(comp) if matches!(comp.op, SetOp::Union) => {
            (&*comp.left, &*comp.right, comp.all)
        }
        _ => return Err(SqlError::RecursiveCteNoUnion(cte.name.clone())),
    };

    // The anchor decides the shape, so its collations are the whole CTE's: the recursive
    // arm is required to union-compatible with it.
    let anchor = exec_body(anchor_body, ctx)?;
    let collations = anchor.collations;
    let columns = if !cte.column_aliases.is_empty() {
        if cte.column_aliases.len() != anchor.result.columns.len() {
            return Err(SqlError::CteColumnAliasMismatch {
                name: cte.name.clone(),
                expected: cte.column_aliases.len(),
                got: anchor.result.columns.len(),
            });
        }
        cte.column_aliases.clone()
    } else {
        anchor.result.columns
    };

    let mut accumulated = anchor.result.rows;
    let mut work_start = 0;
    let mut work_end = accumulated.len();
    let mut seen = if !union_all {
        let mut s = super::helpers::RowKeys::with_capacity(collations.clone(), accumulated.len());
        for (i, row) in accumulated.iter().enumerate() {
            if i & 0xff == 0 {
                super::check_cancelled(cancel)?;
            }
            s.insert(row);
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
                is_with_timezone: false,
                generated_expr: None,
                generated_sql: None,
                generated_kind: None,
                collation: collations.get(i).copied().unwrap_or_default(),
            })
            .collect();
        let col_map = ColumnMap::new(&cte_cols);
        let ncols = sel.columns.len();

        let mut step_rows: Vec<Vec<Value>> = Vec::new();
        let mut row_buf: Vec<Value> = Vec::with_capacity(ncols);
        for iteration in 0..MAX_RECURSIVE_ITERATIONS {
            super::check_cancelled(cancel)?;
            if work_start >= work_end {
                break;
            }

            step_rows.clear();
            for (i, row) in accumulated[work_start..work_end].iter().enumerate() {
                if i & 0xff == 0 {
                    super::check_cancelled(cancel)?;
                }
                let ctx = EvalCtx::new(&col_map, row).with_cancel(cancel);
                if let Some(ref w) = sel.where_clause {
                    match eval_expr(w, &ctx) {
                        Ok(val) if is_truthy(&val) => {}
                        Ok(_) => continue,
                        Err(e) => return Err(e),
                    }
                }
                row_buf.clear();
                for col in &sel.columns {
                    match col {
                        SelectColumn::Expr { expr, .. } => {
                            row_buf.push(eval_expr(expr, &ctx)?);
                        }
                        SelectColumn::AllColumns
                        | SelectColumn::AllFromOld
                        | SelectColumn::AllFromNew => {
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
                let mut kept = 0;
                for i in 0..step_rows.len() {
                    if i & 0xff == 0 {
                        super::check_cancelled(cancel)?;
                    }
                    if seen_set.insert(&step_rows[i]) {
                        step_rows.swap(kept, i);
                        kept += 1;
                    }
                }
                step_rows.truncate(kept);
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
        let working_rows =
            super::clone_cte_rows_with_cancel(&accumulated[work_start..work_end], cancel)?;
        let mut iter_ctx = ctx.clone();
        let working = CteRows::new(
            QueryResult {
                columns: columns.clone(),
                rows: working_rows,
            },
            collations.clone(),
        )
        .shared();
        iter_ctx.insert(cte_key.clone(), std::sync::Arc::clone(&working));

        for iteration in 0..MAX_RECURSIVE_ITERATIONS {
            super::check_cancelled(cancel)?;
            if iter_ctx.get(&cte_key).unwrap().result.rows.is_empty() {
                break;
            }

            let iter_rows = exec_body(recursive_body, &iter_ctx)?;
            if iter_rows.result.rows.is_empty() {
                break;
            }

            let new_rows = if let Some(ref mut seen_set) = seen {
                let mut new_rows = Vec::new();
                for (i, row) in iter_rows.result.rows.into_iter().enumerate() {
                    if i & 0xff == 0 {
                        super::check_cancelled(cancel)?;
                    }
                    if seen_set.insert(&row) {
                        new_rows.push(row);
                    }
                }
                new_rows
            } else {
                iter_rows.result.rows
            };

            if new_rows.is_empty() {
                break;
            }

            super::extend_cte_rows_with_cancel(&mut accumulated, &new_rows, cancel)?;
            iter_ctx.insert(cte_key.clone(), working.with_rows(new_rows).shared());

            if iteration == MAX_RECURSIVE_ITERATIONS - 1 {
                return Err(SqlError::RecursiveCteMaxIterations(
                    cte_key.clone(),
                    MAX_RECURSIVE_ITERATIONS,
                ));
            }
        }

        iter_ctx.remove(&cte_key);
    }

    super::check_cancelled(cancel)?;
    Ok(CteRows::new(
        QueryResult {
            columns,
            rows: accumulated,
        },
        collations,
    ))
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
        QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => false,
    }
}

/// The rows of a CTE, derived table or view, presented as a table. Collations come
/// from the columns the rows were projected from; reporting binary loses them at the
/// boundary, so a NOCASE column would compare byte-exact through a derived table.
pub(super) fn build_cte_schema(name: &str, cte: &CteRows) -> Result<TableSchema> {
    TableSchema::validate_column_count(cte.result.columns.len())?;
    let columns: Vec<ColumnDef> = cte
        .result
        .columns
        .iter()
        .enumerate()
        .map(|(i, col_name)| {
            super::helpers::projected_column(col_name.clone(), i, cte.collation_at(i))
        })
        .collect();
    Ok(TableSchema::new(
        name.into(),
        columns,
        vec![],
        vec![],
        vec![],
        vec![],
    ))
}

pub(super) fn exec_select_from_cte(
    cte: &CteRows,
    stmt: &SelectStmt,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<CteRows>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<ExecutionResult> {
    let cte_schema = build_cte_schema(&stmt.from, cte)?;
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

    let ctx = super::SelectCtx::new(&cte_schema.columns, s, cancel);

    if has_aggregates || !s.group_by.is_empty() {
        if let Some(ref where_expr) = s.where_clause {
            let col_map = ColumnMap::new(&cte_schema.columns);
            let mut filtered = Vec::new();
            for (row_idx, row) in cte.result.rows.iter().enumerate() {
                super::check_cte_cancel_at(cancel, row_idx)?;
                if is_truthy(&eval_expr(
                    where_expr,
                    &EvalCtx::new(&col_map, row).with_cancel(cancel),
                )?) {
                    filtered.push(row.clone());
                }
            }
            super::check_cancelled(cancel)?;
            return exec_aggregate(&filtered, ctx);
        }
        return exec_aggregate(&cte.result.rows, ctx);
    }

    super::process_select(
        super::clone_cte_rows_with_cancel(&cte.result.rows, cancel)?,
        ctx,
    )
}

#[cfg(test)]
#[path = "cte_tests.rs"]
mod tests;
