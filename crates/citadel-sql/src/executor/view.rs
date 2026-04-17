use std::cell::Cell;

use citadel::Database;

use crate::error::{Result, SqlError};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

thread_local! {
    static VIEW_DEPTH: Cell<u32> = const { Cell::new(0) };
}

const MAX_VIEW_DEPTH: u32 = 32;

pub(super) fn exec_view_read(
    db: &Database,
    schema: &SchemaManager,
    view_def: &ViewDef,
) -> Result<QueryResult> {
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
        match super::exec_select_query(db, schema, &sq)? {
            ExecutionResult::Query(mut qr) => {
                apply_view_aliases(&mut qr, &view_def.column_aliases);
                Ok(qr)
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
) -> Result<QueryResult> {
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
                Ok(qr)
            }
            _ => Err(SqlError::InvalidValue(
                "view query did not return results".into(),
            )),
        }
    })();

    VIEW_DEPTH.with(|d| d.set(d.get() - 1));
    result
}

pub(super) fn apply_view_aliases(qr: &mut QueryResult, aliases: &[String]) {
    for (i, alias) in aliases.iter().enumerate() {
        if i < qr.columns.len() {
            qr.columns[i] = alias.clone();
        }
    }
}

/// Merge a simple view into the outer query, replacing FROM with the real table.
pub(super) fn try_fuse_view(
    outer: &SelectStmt,
    schema: &SchemaManager,
    view_def: &ViewDef,
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

pub(super) fn build_view_schema(name: &str, qr: &QueryResult) -> TableSchema {
    super::build_cte_schema(name, qr)
}
