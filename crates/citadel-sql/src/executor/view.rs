use std::cell::Cell;

use citadel_txn::read_txn::ReadTxn;

use crate::error::{Result, SqlError};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::CteRows;

thread_local! {
    static VIEW_DEPTH: Cell<u32> = const { Cell::new(0) };
}

const MAX_VIEW_DEPTH: u32 = 32;

pub(super) fn exec_view_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    view_def: &ViewDef,
) -> Result<CteRows> {
    let depth = VIEW_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    if depth > MAX_VIEW_DEPTH {
        VIEW_DEPTH.with(|d| d.set(d.get() - 1));
        return Err(SqlError::CircularViewReference(view_def.name.clone()));
    }

    let result = (|| {
        let stmt = crate::parser::parse_sql(&view_def.sql)?;
        let sq = match stmt {
            Statement::Select(sq) => sq,
            _ => return Err(SqlError::InvalidValue("view body is not a SELECT".into())),
        };
        match super::exec_select_query_with_read(rtx, schema, &sq)? {
            ExecutionResult::Query(mut qr) => {
                apply_view_aliases(&mut qr, &view_def.column_aliases);
                Ok(view_rows(schema, &sq, qr))
            }
            _ => Err(SqlError::InvalidValue(
                "view query did not return results".into(),
            )),
        }
    })();

    VIEW_DEPTH.with(|d| d.set(d.get() - 1));
    result
}

pub(super) fn exec_view_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    view_def: &ViewDef,
) -> Result<CteRows> {
    let depth = VIEW_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    if depth > MAX_VIEW_DEPTH {
        VIEW_DEPTH.with(|d| d.set(d.get() - 1));
        return Err(SqlError::CircularViewReference(view_def.name.clone()));
    }

    let result = (|| {
        let stmt = crate::parser::parse_sql(&view_def.sql)?;
        let sq = match stmt {
            Statement::Select(sq) => sq,
            _ => return Err(SqlError::InvalidValue("view body is not a SELECT".into())),
        };
        match super::exec_select_query_in_txn(wtx, schema, &sq)? {
            ExecutionResult::Query(mut qr) => {
                apply_view_aliases(&mut qr, &view_def.column_aliases);
                Ok(view_rows(schema, &sq, qr))
            }
            _ => Err(SqlError::InvalidValue(
                "view query did not return results".into(),
            )),
        }
    })();

    VIEW_DEPTH.with(|d| d.set(d.get() - 1));
    result
}

/// A view's rows keep the collations of the columns its query selected, so reading a NOCASE
/// column through a view compares the way reading it directly does.
fn view_rows(schema: &SchemaManager, sq: &SelectQuery, qr: QueryResult) -> CteRows {
    let collations = super::dml::query_output_collations(
        schema,
        &super::CteContext::default(),
        sq,
        qr.columns.len(),
    );
    CteRows::new(qr, collations)
}

pub(super) fn apply_view_aliases(qr: &mut QueryResult, aliases: &[String]) {
    for (i, alias) in aliases.iter().enumerate() {
        if i < qr.columns.len() {
            qr.columns[i] = alias.clone();
        }
    }
}

/// Merge a simple view into the outer query, replacing FROM with the real table.
pub(super) fn try_fuse_view<'a>(
    outer: &SelectStmt,
    schema: &SchemaManager,
    view_def: &ViewDef,
    cte_names: impl IntoIterator<Item = &'a str>,
) -> Result<Option<SelectStmt>> {
    let stmt = crate::parser::parse_sql(&view_def.sql)?;
    let sq = match stmt {
        Statement::Select(sq) => sq,
        _ => return Ok(None),
    };

    // Column aliases require materialization
    if !view_def.column_aliases.is_empty() {
        return Ok(None);
    }

    // Must be a simple SELECT body (no CTEs, no compound)
    if !sq.ctes.is_empty() || sq.recursive {
        return Ok(None);
    }
    let inner = match &sq.body {
        QueryBody::Select(s) => s.as_ref(),
        _ => return Ok(None),
    };

    if !inner.joins.is_empty()
        || !inner.group_by.is_empty()
        || inner.distinct
        || inner.having.is_some()
        || inner.limit.is_some()
        || inner.offset.is_some()
        || !inner.order_by.is_empty()
        || super::stmt_has_subquery(inner)
        || super::has_any_window_function(inner)
    {
        return Ok(None);
    }

    // Only fuse SELECT * views
    let is_select_star =
        inner.columns.len() == 1 && matches!(inner.columns[0], SelectColumn::AllColumns);
    if !is_select_star {
        return Ok(None);
    }

    // Outer query must not have JOINs on this view
    if !outer.joins.is_empty() {
        return Ok(None);
    }

    let real_table = inner.from.to_ascii_lowercase();
    if schema.get(&real_table).is_none() {
        return Ok(None);
    }

    // View definitions resolve outside the caller's CTE namespace. A fused
    // scan keeps that namespace for the outer expressions, so materialize
    // when it would redirect the view's physical base table to a caller CTE.
    if cte_names
        .into_iter()
        .any(|name| name.eq_ignore_ascii_case(&real_table))
    {
        return Ok(None);
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

    // Preserve the view name as alias so qualified refs (e.g., view_name.col) still resolve
    let fused_alias = outer
        .from_alias
        .clone()
        .or_else(|| Some(outer.from.to_ascii_lowercase()));

    Ok(Some(SelectStmt {
        columns: outer.columns.clone(),
        from: inner.from.clone(),
        from_alias: fused_alias,
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
    }))
}

pub(super) fn build_view_schema(name: &str, view: &CteRows) -> TableSchema {
    super::build_cte_schema(name, view)
}

#[cfg(test)]
#[path = "view_tests.rs"]
mod tests;
