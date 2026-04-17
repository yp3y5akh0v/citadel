use std::collections::HashMap;

use citadel::Database;

use crate::encoding::{decode_column_raw, decode_composite_key, decode_pk_integer};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::helpers::decode_full_row;
use super::CteContext;

pub(super) type InMap = (HashMap<Vec<Value>, std::collections::HashSet<Value>>, bool);

#[allow(clippy::type_complexity)]
pub(super) fn handle_correlated_select_read(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctx: &CorrelationCtx,
    rows: &mut [Vec<Value>],
    columns: &mut Vec<ColumnDef>,
) -> Result<SelectStmt> {
    let mut new_columns = Vec::new();
    let mut scalar_maps: Vec<(HashMap<Vec<Value>, Value>, Vec<usize>)> = Vec::new();
    let mut corr_col_idx = columns.len();

    for col in &stmt.columns {
        match col {
            SelectColumn::Expr {
                expr: Expr::ScalarSubquery(sub),
                alias,
            } => {
                if is_correlated_subquery(sub, ctx, schema) {
                    let inner_name = sub.from.to_ascii_lowercase();
                    if let Some(inner_schema) = schema.get(&inner_name) {
                        let (corr_pairs, _) = extract_correlation_predicates(
                            sub.where_clause
                                .as_ref()
                                .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                            ctx,
                            inner_schema,
                            sub.from_alias.as_deref(),
                        );
                        if !corr_pairs.is_empty() {
                            let map = decorrelate_scalar_read(db, schema, sub, &corr_pairs, ctx)?;
                            let outer_indices: Vec<usize> =
                                corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                            scalar_maps.push((map, outer_indices));

                            let col_name = alias
                                .clone()
                                .unwrap_or_else(|| format!("__corr_{corr_col_idx}"));
                            columns.push(ColumnDef {
                                name: col_name.clone(),
                                data_type: DataType::Null,
                                nullable: true,
                                position: corr_col_idx as u16,
                                default_expr: None,
                                default_sql: None,
                                check_expr: None,
                                check_sql: None,
                                check_name: None,
                            });
                            new_columns.push(SelectColumn::Expr {
                                expr: Expr::Column(col_name),
                                alias: alias.clone(),
                            });
                            corr_col_idx += 1;
                            continue;
                        }
                    }
                }
                new_columns.push(col.clone());
            }
            _ => new_columns.push(col.clone()),
        }
    }

    if scalar_maps.is_empty() {
        return Ok(stmt.clone());
    }

    for row in rows.iter_mut() {
        for (map, outer_indices) in &scalar_maps {
            let key: Vec<Value> = outer_indices.iter().map(|&i| row[i].clone()).collect();
            let val = if key.iter().any(|v| v.is_null()) {
                Value::Null
            } else {
                map.get(&key).cloned().unwrap_or(Value::Null)
            };
            row.push(val);
        }
    }

    Ok(SelectStmt {
        columns: new_columns,
        from: stmt.from.clone(),
        from_alias: stmt.from_alias.clone(),
        joins: stmt.joins.clone(),
        distinct: stmt.distinct,
        where_clause: stmt.where_clause.clone(),
        order_by: stmt.order_by.clone(),
        limit: stmt.limit.clone(),
        offset: stmt.offset.clone(),
        group_by: stmt.group_by.clone(),
        having: stmt.having.clone(),
    })
}

/// Resolve a table or view name to its schema.
pub(super) fn resolve_inner_schema(
    db: &Database,
    schema: &SchemaManager,
    name: &str,
) -> Result<TableSchema> {
    if let Some(ts) = schema.get(name) {
        return Ok(ts.clone());
    }
    if let Some(vd) = schema.get_view(name) {
        let qr = super::exec_view_read(db, schema, vd)?;
        return Ok(super::build_view_schema(name, &qr));
    }
    Err(SqlError::TableNotFound(name.to_string()))
}

pub(super) fn resolve_inner_schema_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    name: &str,
) -> Result<TableSchema> {
    if let Some(ts) = schema.get(name) {
        return Ok(ts.clone());
    }
    if let Some(vd) = schema.get_view(name) {
        let qr = super::exec_view_write(wtx, schema, vd)?;
        return Ok(super::build_view_schema(name, &qr));
    }
    Err(SqlError::TableNotFound(name.to_string()))
}

// ── Correlated subquery support ─────────────────────────────────────

/// Context for correlation detection — carries outer table info.
pub(super) struct CorrelationCtx<'a> {
    pub(super) outer_schema: &'a TableSchema,
    pub(super) outer_alias: Option<&'a str>,
}

impl<'a> CorrelationCtx<'a> {
    fn outer_name(&self) -> &str {
        &self.outer_schema.name
    }

    fn matches_outer(&self, table_part: &str) -> bool {
        table_part == self.outer_name()
            || self
                .outer_alias
                .is_some_and(|a| a.eq_ignore_ascii_case(table_part))
    }
}

/// Check if a column name resolves in the given schema.
pub(super) fn resolves_in(name: &str, schema: &TableSchema) -> bool {
    let lower = name.to_ascii_lowercase();
    schema.columns.iter().any(|c| c.name == lower)
}

/// Collect column names referenced in an expression.
pub(super) fn collect_column_names(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Column(name) => out.push(name.to_ascii_lowercase()),
        Expr::QualifiedColumn { table, column } => {
            out.push(format!(
                "{}.{}",
                table.to_ascii_lowercase(),
                column.to_ascii_lowercase()
            ));
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_column_names(left, out);
            collect_column_names(right, out);
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::Cast { expr: e, .. } => {
            collect_column_names(e, out);
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) => {
            for a in args {
                collect_column_names(a, out);
            }
        }
        Expr::InList { expr: e, list, .. } => {
            collect_column_names(e, out);
            for item in list {
                collect_column_names(item, out);
            }
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            collect_column_names(e, out);
            collect_column_names(low, out);
            collect_column_names(high, out);
        }
        Expr::Like {
            expr: e, pattern, ..
        } => {
            collect_column_names(e, out);
            collect_column_names(pattern, out);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_column_names(op, out);
            }
            for (c, r) in conditions {
                collect_column_names(c, out);
                collect_column_names(r, out);
            }
            if let Some(el) = else_result {
                collect_column_names(el, out);
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            for a in args {
                collect_column_names(a, out);
            }
            for p in &spec.partition_by {
                collect_column_names(p, out);
            }
            for o in &spec.order_by {
                collect_column_names(&o.expr, out);
            }
        }
        Expr::InSubquery { expr: e, .. } => {
            collect_column_names(e, out);
        }
        Expr::InSet { expr: e, .. } => {
            collect_column_names(e, out);
        }
        _ => {}
    }
}

/// Check if a subquery references outer columns not in the inner table.
pub(super) fn is_correlated_subquery(
    subquery: &SelectStmt,
    ctx: &CorrelationCtx,
    schema: &SchemaManager,
) -> bool {
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema.get(&inner_name);
    // Inner must be a known table or view
    if inner_schema.is_none() && schema.get_view(&inner_name).is_none() {
        return false;
    }
    let inner_alias = subquery
        .from_alias
        .as_deref()
        .map(|a| a.to_ascii_lowercase());

    let mut col_names = Vec::new();
    if let Some(ref w) = subquery.where_clause {
        collect_column_names(w, &mut col_names);
    }
    for col in &subquery.columns {
        if let SelectColumn::Expr { expr, .. } = col {
            collect_column_names(expr, &mut col_names);
        }
    }

    for name in &col_names {
        if let Some(dot) = name.find('.') {
            let table_part = &name[..dot];
            let col_part = &name[dot + 1..];
            // Skip if matches inner table/view name or inner alias
            if table_part == inner_name || inner_alias.as_deref() == Some(table_part) {
                continue;
            }
            // Matches outer → correlated
            if ctx.matches_outer(table_part) && resolves_in(col_part, ctx.outer_schema) {
                return true;
            }
        } else if let Some(is) = inner_schema {
            // Unqualified: check against inner schema (only when inner is a real table)
            if !resolves_in(name, is) && resolves_in(name, ctx.outer_schema) {
                return true;
            }
        }
        // Views as inner: qualified refs detect correlation for common patterns
    }
    false
}

/// A correlation equality predicate: outer_col = inner_col
pub(super) struct CorrEqPair {
    outer_col_name: String,
    outer_col_idx: usize,
    inner_col_name: String,
}

/// Extract equality correlation predicates. Returns (pairs, remaining inner-only WHERE).
pub(super) fn extract_correlation_predicates(
    where_clause: &Expr,
    ctx: &CorrelationCtx,
    inner_schema: &TableSchema,
    inner_alias: Option<&str>,
) -> (Vec<CorrEqPair>, Option<Expr>) {
    let conjuncts = flatten_and_exprs(where_clause);
    let mut corr_pairs = Vec::new();
    let mut remaining = Vec::new();

    for conj in conjuncts {
        if let Some(pair) = try_extract_corr_eq(conj, ctx, inner_schema, inner_alias) {
            corr_pairs.push(pair);
        } else {
            remaining.push(conj.clone());
        }
    }

    let remaining_expr = if remaining.is_empty() {
        None
    } else {
        let mut combined = remaining.remove(0);
        for r in remaining {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinOp::And,
                right: Box::new(r),
            };
        }
        Some(combined)
    };

    (corr_pairs, remaining_expr)
}

/// Flatten AND-connected expressions into a list.
pub(super) fn flatten_and_exprs(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut v = flatten_and_exprs(left);
            v.extend(flatten_and_exprs(right));
            v
        }
        _ => vec![expr],
    }
}

/// Try to extract a correlation equality from an expression like `t2.x = t1.x` or `inner_col = outer_col`.
pub(super) fn try_extract_corr_eq(
    expr: &Expr,
    ctx: &CorrelationCtx,
    inner_schema: &TableSchema,
    inner_alias: Option<&str>,
) -> Option<CorrEqPair> {
    let (left, right) = match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => (left.as_ref(), right.as_ref()),
        _ => return None,
    };

    if let Some(pair) = try_match_corr_pair(left, right, ctx, inner_schema, inner_alias) {
        return Some(pair);
    }
    try_match_corr_pair(right, left, ctx, inner_schema, inner_alias)
}

pub(super) fn try_match_corr_pair(
    maybe_outer: &Expr,
    maybe_inner: &Expr,
    ctx: &CorrelationCtx,
    inner_schema: &TableSchema,
    inner_alias: Option<&str>,
) -> Option<CorrEqPair> {
    let outer_col = match maybe_outer {
        Expr::QualifiedColumn { table, column } => {
            let t = table.to_ascii_lowercase();
            if ctx.matches_outer(&t) {
                column.to_ascii_lowercase()
            } else {
                return None;
            }
        }
        Expr::Column(name) => {
            let lower = name.to_ascii_lowercase();
            if resolves_in(&lower, inner_schema) || !resolves_in(&lower, ctx.outer_schema) {
                return None;
            }
            lower
        }
        _ => return None,
    };

    let inner_col = match maybe_inner {
        Expr::QualifiedColumn { table, column } => {
            let t = table.to_ascii_lowercase();
            let inner_name = &inner_schema.name;
            if t == *inner_name || inner_alias.is_some_and(|a| a.eq_ignore_ascii_case(&t)) {
                column.to_ascii_lowercase()
            } else {
                return None;
            }
        }
        Expr::Column(name) => {
            let lower = name.to_ascii_lowercase();
            if !resolves_in(&lower, inner_schema) {
                return None;
            }
            lower
        }
        _ => return None,
    };

    let outer_col_idx = ctx.outer_schema.column_index(&outer_col)?;

    Some(CorrEqPair {
        outer_col_name: outer_col,
        outer_col_idx,
        inner_col_name: inner_col,
    })
}

/// Strip correlation predicates from WHERE, returning (inner-only WHERE, non-equality predicates).
pub(super) fn strip_correlation_predicates(
    where_clause: &Option<Expr>,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
    inner_schema: &TableSchema,
) -> (Option<Expr>, Vec<Expr>) {
    let w = match where_clause {
        Some(w) => w,
        None => return (None, vec![]),
    };
    let conjuncts = flatten_and_exprs(w);
    let corr_outer: std::collections::HashSet<&str> = corr_pairs
        .iter()
        .map(|p| p.outer_col_name.as_str())
        .collect();
    let corr_inner: std::collections::HashSet<&str> = corr_pairs
        .iter()
        .map(|p| p.inner_col_name.as_str())
        .collect();

    let mut inner_only: Vec<Expr> = Vec::new();
    let mut non_eq_corr: Vec<Expr> = Vec::new();

    for c in conjuncts {
        if let Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = c
        {
            let l = col_name_lower(left);
            let r = col_name_lower(right);
            let l_is_corr = l
                .as_deref()
                .is_some_and(|n| corr_outer.contains(n) || corr_inner.contains(n));
            let r_is_corr = r
                .as_deref()
                .is_some_and(|n| corr_outer.contains(n) || corr_inner.contains(n));
            if l_is_corr && r_is_corr {
                // Equality correlation → already a hash key, skip
                continue;
            }
        }
        // Check if this predicate references outer columns
        let mut refs = Vec::new();
        collect_column_names(c, &mut refs);
        let refs_outer = refs.iter().any(|name| {
            if let Some(dot) = name.find('.') {
                let table_part = &name[..dot];
                ctx.matches_outer(table_part)
            } else {
                !resolves_in(name, inner_schema) && resolves_in(name, ctx.outer_schema)
            }
        });
        if refs_outer {
            non_eq_corr.push(c.clone());
        } else {
            inner_only.push(c.clone());
        }
    }

    let inner_where = if inner_only.is_empty() {
        None
    } else {
        let mut combined = inner_only.remove(0);
        for c in inner_only {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinOp::And,
                right: Box::new(c),
            };
        }
        Some(combined)
    };

    (inner_where, non_eq_corr)
}

pub(super) fn col_name_lower(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(n) => Some(n.to_ascii_lowercase()),
        Expr::QualifiedColumn { column, .. } => Some(column.to_ascii_lowercase()),
        _ => None,
    }
}

/// Replace outer column references in an expression with literal values from the outer row.
pub(super) fn bind_outer_values_in_expr(
    expr: &Expr,
    outer_row: &[Value],
    outer_col_map: &ColumnMap,
    ctx: &CorrelationCtx,
) -> Expr {
    match expr {
        Expr::QualifiedColumn { table, column } => {
            if ctx.matches_outer(&table.to_ascii_lowercase()) {
                if let Ok(idx) = outer_col_map.resolve(&column.to_ascii_lowercase()) {
                    return Expr::Literal(outer_row[idx].clone());
                }
            }
            expr.clone()
        }
        Expr::Column(name) => {
            let lower = name.to_ascii_lowercase();
            if let Ok(idx) = outer_col_map.resolve(&lower) {
                return Expr::Literal(outer_row[idx].clone());
            }
            expr.clone()
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(bind_outer_values_in_expr(
                left,
                outer_row,
                outer_col_map,
                ctx,
            )),
            op: *op,
            right: Box::new(bind_outer_values_in_expr(
                right,
                outer_row,
                outer_col_map,
                ctx,
            )),
        },
        Expr::UnaryOp { op, expr: e } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(bind_outer_values_in_expr(e, outer_row, outer_col_map, ctx)),
        },
        _ => expr.clone(),
    }
}

pub(super) enum ExistsResult {
    Simple(std::collections::HashSet<Vec<Value>>),
    WithFilter(Box<ExistsFilterData>),
}

pub(super) struct ExistsFilterData {
    rows_by_key: HashMap<Vec<Value>, Vec<Vec<Value>>>,
    non_eq_predicates: Vec<Expr>,
    inner_schema: TableSchema,
}

pub(super) fn decorrelate_exists_read(
    db: &Database,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<ExistsResult> {
    let inner_name = subquery.from.to_ascii_lowercase();

    let (inner_schema_owned, inner_rows) = if let Some(ts) = schema.get(&inner_name) {
        let (inner_where, _) =
            strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, ts);
        let (rows, _) = super::collect_rows_read(db, ts, &inner_where, None)?;
        (ts.clone(), rows)
    } else if let Some(vd) = schema.get_view(&inner_name) {
        let vqr = super::exec_view_read(db, schema, vd)?;
        let vs = super::build_view_schema(&inner_name, &vqr);
        let (inner_where, _) =
            strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, &vs);
        let col_map = ColumnMap::new(&vs.columns);
        let rows: Vec<Vec<Value>> = if let Some(ref w) = inner_where {
            vqr.rows
                .into_iter()
                .filter(|row| match eval_expr(w, &col_map, row) {
                    Ok(v) => is_truthy(&v),
                    _ => false,
                })
                .collect()
        } else {
            vqr.rows
        };
        (vs, rows)
    } else {
        return Err(SqlError::TableNotFound(subquery.from.clone()));
    };
    let inner_schema = &inner_schema_owned;

    let (_, non_eq) =
        strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, inner_schema);

    let inner_col_indices: Vec<usize> = corr_pairs
        .iter()
        .map(|p| inner_schema.column_index(&p.inner_col_name).unwrap_or(0))
        .collect();

    if non_eq.is_empty() {
        let mut key_set = std::collections::HashSet::new();
        for row in &inner_rows {
            let key: Vec<Value> = inner_col_indices.iter().map(|&i| row[i].clone()).collect();
            if key.iter().any(|v| v.is_null()) {
                continue;
            }
            key_set.insert(key);
        }
        Ok(ExistsResult::Simple(key_set))
    } else {
        let mut rows_by_key: HashMap<Vec<Value>, Vec<Vec<Value>>> = HashMap::new();
        for row in inner_rows {
            let key: Vec<Value> = inner_col_indices.iter().map(|&i| row[i].clone()).collect();
            if key.iter().any(|v| v.is_null()) {
                continue;
            }
            rows_by_key.entry(key).or_default().push(row);
        }
        Ok(ExistsResult::WithFilter(Box::new(ExistsFilterData {
            rows_by_key,
            non_eq_predicates: non_eq,
            inner_schema: inner_schema.clone(),
        })))
    }
}

/// Decorrelate IN/NOT IN subquery. Returns correlation key → IN-column value set.
pub(super) fn decorrelate_in_read(
    db: &Database,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<InMap> {
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema
        .get(&inner_name)
        .ok_or_else(|| SqlError::TableNotFound(subquery.from.clone()))?;

    // The IN column is the first (and should be only) SELECT column
    let in_col_name = match &subquery.columns[0] {
        SelectColumn::Expr {
            expr: Expr::Column(name),
            ..
        } => name.to_ascii_lowercase(),
        _ => return Err(SqlError::Unsupported("complex IN subquery column".into())),
    };
    let in_col_idx = inner_schema
        .column_index(&in_col_name)
        .ok_or_else(|| SqlError::ColumnNotFound(in_col_name.clone()))?;

    let (inner_where, _non_eq) =
        strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, inner_schema);
    let (inner_rows, _) = super::collect_rows_read(db, inner_schema, &inner_where, None)?;

    let inner_corr_indices: Vec<usize> = corr_pairs
        .iter()
        .map(|p| inner_schema.column_index(&p.inner_col_name).unwrap_or(0))
        .collect();

    let mut map: HashMap<Vec<Value>, std::collections::HashSet<Value>> = HashMap::new();
    let mut has_null_in_values = false;

    for row in &inner_rows {
        let key: Vec<Value> = inner_corr_indices.iter().map(|&i| row[i].clone()).collect();
        if key.iter().any(|v| v.is_null()) {
            continue;
        }
        let in_val = row[in_col_idx].clone();
        if in_val.is_null() {
            has_null_in_values = true;
        } else {
            map.entry(key).or_default().insert(in_val);
        }
    }

    Ok((map, has_null_in_values))
}

/// Decorrelate scalar subquery. Returns correlation key → scalar result.
pub(super) fn decorrelate_scalar_read(
    db: &Database,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<HashMap<Vec<Value>, Value>> {
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema
        .get(&inner_name)
        .ok_or_else(|| SqlError::TableNotFound(subquery.from.clone()))?;

    let corr_col_names: Vec<String> = corr_pairs
        .iter()
        .map(|p| p.inner_col_name.clone())
        .collect();

    let group_by: Vec<Expr> = corr_col_names
        .iter()
        .map(|name| Expr::Column(name.clone()))
        .collect();

    let (inner_where, _non_eq) =
        strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, inner_schema);

    let mut select_cols: Vec<SelectColumn> = corr_col_names
        .iter()
        .map(|name| SelectColumn::Expr {
            expr: Expr::Column(name.clone()),
            alias: None,
        })
        .collect();
    select_cols.extend(subquery.columns.clone());

    let rewritten = SelectStmt {
        columns: select_cols,
        from: subquery.from.clone(),
        from_alias: subquery.from_alias.clone(),
        joins: vec![],
        distinct: false,
        where_clause: inner_where,
        order_by: vec![],
        limit: None,
        offset: None,
        group_by,
        having: None,
    };

    let empty_ctes = CteContext::new();
    let qr = match super::exec_select(db, schema, &rewritten, &empty_ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => return Ok(HashMap::new()),
    };

    // Build HashMap: first N columns are corr keys, last column is the scalar result
    let num_corr = corr_pairs.len();
    let mut map = HashMap::new();
    for row in &qr.rows {
        let key: Vec<Value> = row[..num_corr].to_vec();
        if key.iter().any(|v| v.is_null()) {
            continue;
        }
        let val = if row.len() > num_corr {
            row[num_corr].clone()
        } else {
            Value::Null
        };
        map.insert(key, val);
    }

    Ok(map)
}

// ── Write-transaction variants (same logic, use collect_rows_write) ──

pub(super) fn decorrelate_exists_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<std::collections::HashSet<Vec<Value>>> {
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema
        .get(&inner_name)
        .ok_or_else(|| SqlError::TableNotFound(subquery.from.clone()))?;
    let (inner_where, _non_eq) =
        strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, inner_schema);
    let (inner_rows, _) = super::collect_rows_write(wtx, inner_schema, &inner_where, None)?;
    let inner_col_indices: Vec<usize> = corr_pairs
        .iter()
        .map(|p| inner_schema.column_index(&p.inner_col_name).unwrap_or(0))
        .collect();
    let mut key_set = std::collections::HashSet::new();
    for row in &inner_rows {
        let key: Vec<Value> = inner_col_indices.iter().map(|&i| row[i].clone()).collect();
        if key.iter().any(|v| v.is_null()) {
            continue;
        }
        key_set.insert(key);
    }
    Ok(key_set)
}

pub(super) fn decorrelate_in_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<InMap> {
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema
        .get(&inner_name)
        .ok_or_else(|| SqlError::TableNotFound(subquery.from.clone()))?;
    let in_col_name = match &subquery.columns[0] {
        SelectColumn::Expr {
            expr: Expr::Column(name),
            ..
        } => name.to_ascii_lowercase(),
        _ => return Err(SqlError::Unsupported("complex IN subquery column".into())),
    };
    let in_col_idx = inner_schema
        .column_index(&in_col_name)
        .ok_or_else(|| SqlError::ColumnNotFound(in_col_name.clone()))?;
    let (inner_where, _non_eq) =
        strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, inner_schema);
    let (inner_rows, _) = super::collect_rows_write(wtx, inner_schema, &inner_where, None)?;
    let inner_corr_indices: Vec<usize> = corr_pairs
        .iter()
        .map(|p| inner_schema.column_index(&p.inner_col_name).unwrap_or(0))
        .collect();
    let mut map: HashMap<Vec<Value>, std::collections::HashSet<Value>> = HashMap::new();
    let mut has_null_in_values = false;
    for row in &inner_rows {
        let key: Vec<Value> = inner_corr_indices.iter().map(|&i| row[i].clone()).collect();
        if key.iter().any(|v| v.is_null()) {
            continue;
        }
        let in_val = row[in_col_idx].clone();
        if in_val.is_null() {
            has_null_in_values = true;
        } else {
            map.entry(key).or_default().insert(in_val);
        }
    }
    Ok((map, has_null_in_values))
}

pub(super) fn decorrelate_scalar_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<HashMap<Vec<Value>, Value>> {
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema
        .get(&inner_name)
        .ok_or_else(|| SqlError::TableNotFound(subquery.from.clone()))?;
    let corr_col_names: Vec<String> = corr_pairs
        .iter()
        .map(|p| p.inner_col_name.clone())
        .collect();
    let group_by: Vec<Expr> = corr_col_names
        .iter()
        .map(|n| Expr::Column(n.clone()))
        .collect();
    let (inner_where, _non_eq) =
        strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, inner_schema);
    let mut select_cols: Vec<SelectColumn> = corr_col_names
        .iter()
        .map(|name| SelectColumn::Expr {
            expr: Expr::Column(name.clone()),
            alias: None,
        })
        .collect();
    select_cols.extend(subquery.columns.clone());
    let rewritten = SelectStmt {
        columns: select_cols,
        from: subquery.from.clone(),
        from_alias: subquery.from_alias.clone(),
        joins: vec![],
        distinct: false,
        where_clause: inner_where,
        order_by: vec![],
        limit: None,
        offset: None,
        group_by,
        having: None,
    };
    let empty_ctes = CteContext::new();
    let qr = match super::exec_select_in_txn(wtx, schema, &rewritten, &empty_ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => return Ok(HashMap::new()),
    };
    let num_corr = corr_pairs.len();
    let mut map = HashMap::new();
    for row in &qr.rows {
        let key: Vec<Value> = row[..num_corr].to_vec();
        if key.iter().any(|v| v.is_null()) {
            continue;
        }
        let val = if row.len() > num_corr {
            row[num_corr].clone()
        } else {
            Value::Null
        };
        map.insert(key, val);
    }
    Ok(map)
}

/// Write-transaction variant of handle_correlated_where_read.
pub(super) fn handle_correlated_where_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctx: &CorrelationCtx,
    rows: &mut Vec<Vec<Value>>,
) -> Result<Option<Expr>> {
    let where_clause = match &stmt.where_clause {
        Some(w) => w,
        None => return Ok(None),
    };
    let conjuncts = flatten_and_exprs(where_clause);
    let mut remaining_conjuncts: Vec<Expr> = Vec::new();

    for conj in conjuncts {
        match conj {
            Expr::Exists { subquery, negated } => {
                if is_correlated_subquery(subquery, ctx, schema) {
                    let inner_schema = resolve_inner_schema_write(
                        wtx,
                        schema,
                        &subquery.from.to_ascii_lowercase(),
                    )?;
                    let (corr_pairs, _) = extract_correlation_predicates(
                        subquery
                            .where_clause
                            .as_ref()
                            .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                        ctx,
                        &inner_schema,
                        subquery.from_alias.as_deref(),
                    );
                    if corr_pairs.is_empty() {
                        remaining_conjuncts.push(conj.clone());
                        continue;
                    }
                    let key_set =
                        decorrelate_exists_write(wtx, schema, subquery, &corr_pairs, ctx)?;
                    let outer_col_indices: Vec<usize> =
                        corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                    let is_negated = *negated;
                    rows.retain(|row| {
                        let key: Vec<Value> =
                            outer_col_indices.iter().map(|&i| row[i].clone()).collect();
                        if key.iter().any(|v| v.is_null()) {
                            return is_negated;
                        }
                        let found = key_set.contains(&key);
                        if is_negated {
                            !found
                        } else {
                            found
                        }
                    });
                } else {
                    remaining_conjuncts.push(conj.clone());
                }
            }
            Expr::InSubquery {
                expr: in_expr,
                subquery,
                negated,
            } => {
                if is_correlated_subquery(subquery, ctx, schema) {
                    let inner_schema = resolve_inner_schema_write(
                        wtx,
                        schema,
                        &subquery.from.to_ascii_lowercase(),
                    )?;
                    let (corr_pairs, _) = extract_correlation_predicates(
                        subquery
                            .where_clause
                            .as_ref()
                            .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                        ctx,
                        &inner_schema,
                        subquery.from_alias.as_deref(),
                    );
                    if corr_pairs.is_empty() {
                        remaining_conjuncts.push(conj.clone());
                        continue;
                    }
                    let (in_map, has_null) =
                        decorrelate_in_write(wtx, schema, subquery, &corr_pairs, ctx)?;
                    let outer_col_indices: Vec<usize> =
                        corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                    let is_negated = *negated;
                    let col_map = ColumnMap::new(&ctx.outer_schema.columns);
                    rows.retain(|row| {
                        let key: Vec<Value> =
                            outer_col_indices.iter().map(|&i| row[i].clone()).collect();
                        let in_val = match eval_expr(in_expr, &col_map, row) {
                            Ok(v) => v,
                            Err(_) => return false,
                        };
                        if in_val.is_null() {
                            return false;
                        }
                        if key.iter().any(|v| v.is_null()) {
                            return is_negated;
                        }
                        let found = in_map.get(&key).is_some_and(|vals| vals.contains(&in_val));
                        if is_negated {
                            if has_null && !found {
                                false
                            } else {
                                !found
                            }
                        } else {
                            found
                        }
                    });
                } else {
                    remaining_conjuncts.push(conj.clone());
                }
            }
            _ => {
                let mut handled = false;
                if let Expr::BinaryOp { left, op, right } = conj {
                    if let Expr::ScalarSubquery(sub) = right.as_ref() {
                        if is_correlated_subquery(sub, ctx, schema) {
                            let inner_schema = resolve_inner_schema_write(
                                wtx,
                                schema,
                                &sub.from.to_ascii_lowercase(),
                            )?;
                            let (corr_pairs, _) = extract_correlation_predicates(
                                sub.where_clause
                                    .as_ref()
                                    .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                                ctx,
                                &inner_schema,
                                sub.from_alias.as_deref(),
                            );
                            if !corr_pairs.is_empty() {
                                let scalar_map =
                                    decorrelate_scalar_write(wtx, schema, sub, &corr_pairs, ctx)?;
                                let outer_col_indices: Vec<usize> =
                                    corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                                let cmp_op = *op;
                                let left_expr = left.clone();
                                let col_map = ColumnMap::new(&ctx.outer_schema.columns);
                                rows.retain(|row| {
                                    let key: Vec<Value> =
                                        outer_col_indices.iter().map(|&i| row[i].clone()).collect();
                                    let scalar_val =
                                        scalar_map.get(&key).cloned().unwrap_or(Value::Null);
                                    let left_val = match eval_expr(&left_expr, &col_map, row) {
                                        Ok(v) => v,
                                        Err(_) => return false,
                                    };
                                    let cmp = Expr::BinaryOp {
                                        left: Box::new(Expr::Literal(left_val)),
                                        op: cmp_op,
                                        right: Box::new(Expr::Literal(scalar_val)),
                                    };
                                    match eval_expr(&cmp, &col_map, row) {
                                        Ok(val) => is_truthy(&val),
                                        Err(_) => false,
                                    }
                                });
                                handled = true;
                            }
                        }
                    }
                }
                if !handled {
                    remaining_conjuncts.push(conj.clone());
                }
            }
        }
    }

    if remaining_conjuncts.is_empty() {
        Ok(None)
    } else {
        let mut combined = remaining_conjuncts.remove(0);
        for r in remaining_conjuncts {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinOp::And,
                right: Box::new(r),
            };
        }
        Ok(Some(combined))
    }
}

/// Check if a WHERE clause has any correlated subquery (top-level AND conjuncts).
pub(super) fn has_correlated_where(
    where_clause: &Option<Expr>,
    ctx: &CorrelationCtx,
    schema: &SchemaManager,
) -> bool {
    let w = match where_clause {
        Some(w) => w,
        None => return false,
    };
    let conjuncts = flatten_and_exprs(w);
    for conj in conjuncts {
        match conj {
            Expr::Exists { subquery, .. } | Expr::InSubquery { subquery, .. }
                if is_correlated_subquery(subquery, ctx, schema) =>
            {
                return true;
            }
            Expr::BinaryOp { left, right, .. } => {
                if let Expr::ScalarSubquery(sub) = left.as_ref() {
                    if is_correlated_subquery(sub, ctx, schema) {
                        return true;
                    }
                }
                if let Expr::ScalarSubquery(sub) = right.as_ref() {
                    if is_correlated_subquery(sub, ctx, schema) {
                        return true;
                    }
                }
            }
            _ => {}
        }
    }
    false
}

/// Check if SELECT columns have any correlated scalar subqueries.
pub(super) fn has_correlated_select(
    columns: &[SelectColumn],
    ctx: &CorrelationCtx,
    schema: &SchemaManager,
) -> bool {
    for col in columns {
        if let SelectColumn::Expr { expr, .. } = col {
            if has_correlated_in_expr(expr, ctx, schema) {
                return true;
            }
        }
    }
    false
}

pub(super) fn has_correlated_in_expr(
    expr: &Expr,
    ctx: &CorrelationCtx,
    schema: &SchemaManager,
) -> bool {
    match expr {
        Expr::ScalarSubquery(sub) => is_correlated_subquery(sub, ctx, schema),
        Expr::BinaryOp { left, right, .. } => {
            has_correlated_in_expr(left, ctx, schema) || has_correlated_in_expr(right, ctx, schema)
        }
        Expr::UnaryOp { expr: e, .. } | Expr::Cast { expr: e, .. } => {
            has_correlated_in_expr(e, ctx, schema)
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) => {
            args.iter().any(|a| has_correlated_in_expr(a, ctx, schema))
        }
        _ => false,
    }
}

/// Decorrelate + partial-decode scan: only fully decode rows matching correlation.
pub(super) fn build_and_scan_correlated_read(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    outer_schema: &TableSchema,
    ctx: &CorrelationCtx,
) -> Result<(Vec<Vec<Value>>, Option<Expr>)> {
    let where_clause = match &stmt.where_clause {
        Some(w) => w,
        None => {
            let (rows, _) = super::collect_rows_read(db, outer_schema, &None, None)?;
            return Ok((rows, None));
        }
    };

    // Phase 1: Pre-build all decorrelation maps (scans inner tables only — cheap).
    let conjuncts = flatten_and_exprs(where_clause);
    let mut exists_filters: Vec<ExistsFilter> = Vec::new();
    let mut in_filters: Vec<InFilter> = Vec::new();
    let mut remaining_conjuncts: Vec<Expr> = Vec::new();

    for conj in &conjuncts {
        match conj {
            Expr::Exists { subquery, negated } if is_correlated_subquery(subquery, ctx, schema) => {
                let inner_schema =
                    resolve_inner_schema(db, schema, &subquery.from.to_ascii_lowercase())?;
                let (corr_pairs, _) = extract_correlation_predicates(
                    subquery
                        .where_clause
                        .as_ref()
                        .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                    ctx,
                    &inner_schema,
                    subquery.from_alias.as_deref(),
                );
                if corr_pairs.is_empty() {
                    remaining_conjuncts.push((*conj).clone());
                    continue;
                }
                let result = decorrelate_exists_read(db, schema, subquery, &corr_pairs, ctx)?;
                let outer_col_indices: Vec<usize> =
                    corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                exists_filters.push(ExistsFilter {
                    result,
                    outer_col_indices,
                    negated: *negated,
                });
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } if is_correlated_subquery(subquery, ctx, schema) => {
                let inner_schema =
                    resolve_inner_schema(db, schema, &subquery.from.to_ascii_lowercase())?;
                let (corr_pairs, _) = extract_correlation_predicates(
                    subquery
                        .where_clause
                        .as_ref()
                        .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                    ctx,
                    &inner_schema,
                    subquery.from_alias.as_deref(),
                );
                if corr_pairs.is_empty() {
                    remaining_conjuncts.push((*conj).clone());
                    continue;
                }
                let (map, has_null) = decorrelate_in_read(db, schema, subquery, &corr_pairs, ctx)?;
                let outer_col_indices: Vec<usize> =
                    corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                in_filters.push(InFilter {
                    map,
                    has_null,
                    outer_col_indices,
                    in_expr: (**expr).clone(),
                    negated: *negated,
                });
            }
            _ => remaining_conjuncts.push((*conj).clone()),
        }
    }

    // If no optimizable filters, fall back to generic path
    if exists_filters.is_empty() && in_filters.is_empty() {
        let (mut rows, _) = super::collect_rows_read(db, outer_schema, &None, None)?;
        let remaining = handle_correlated_where_read(db, schema, stmt, ctx, &mut rows)?;
        return Ok((rows, remaining));
    }

    // Phase 2: Scan outer table — only fully decode rows that pass all correlation filters.
    let lower = &outer_schema.name;
    let num_pk_cols = outer_schema.primary_key_columns.len();
    let non_pk = outer_schema.non_pk_indices();
    let enc_pos = outer_schema.encoding_positions();
    let outer_col_map = ColumnMap::new(&outer_schema.columns);

    // Pre-compute how to extract each needed outer column from raw bytes
    let mut needed_raw: Vec<(usize, RawColTarget)> = Vec::new();
    for ef in &exists_filters {
        for &oci in &ef.outer_col_indices {
            if !needed_raw.iter().any(|(idx, _)| *idx == oci) {
                needed_raw.push((oci, raw_col_target(oci, outer_schema, non_pk, enc_pos)));
            }
        }
    }
    for inf in &in_filters {
        for &oci in &inf.outer_col_indices {
            if !needed_raw.iter().any(|(idx, _)| *idx == oci) {
                needed_raw.push((oci, raw_col_target(oci, outer_schema, non_pk, enc_pos)));
            }
        }
    }

    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut scan_err: Option<SqlError> = None;
    let mut rtx = db.begin_read();

    rtx.table_scan_raw(lower.as_bytes(), |key, value| {
        // Extract only the correlation columns from raw bytes (fast partial decode)
        let mut col_vals: Vec<(usize, Value)> = Vec::with_capacity(needed_raw.len());
        for &(col_idx, ref target) in &needed_raw {
            let val = match extract_raw_value(key, value, target, num_pk_cols) {
                Ok(v) => v,
                Err(e) => {
                    scan_err = Some(e);
                    return false;
                }
            };
            col_vals.push((col_idx, val));
        }

        // Check EXISTS filters
        for ef in &exists_filters {
            let outer_key: Vec<Value> = ef
                .outer_col_indices
                .iter()
                .map(|&oci| {
                    col_vals
                        .iter()
                        .find(|(idx, _)| *idx == oci)
                        .unwrap()
                        .1
                        .clone()
                })
                .collect();
            if outer_key.iter().any(|v| v.is_null()) {
                if !ef.negated {
                    return true;
                } else {
                    continue;
                }
            }
            let found = match &ef.result {
                ExistsResult::Simple(set) => set.contains(&outer_key),
                ExistsResult::WithFilter(filter_data) => {
                    // Non-equality correlation — need full decode for predicate eval
                    let row = match decode_full_row(outer_schema, key, value) {
                        Ok(r) => r,
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    };
                    let inner_col_map = ColumnMap::new(&filter_data.inner_schema.columns);
                    let matched = if let Some(inner_rows) = filter_data.rows_by_key.get(&outer_key)
                    {
                        inner_rows.iter().any(|inner_row| {
                            filter_data.non_eq_predicates.iter().all(|pred| {
                                let bound =
                                    bind_outer_values_in_expr(pred, &row, &outer_col_map, ctx);
                                match eval_expr(&bound, &inner_col_map, inner_row) {
                                    Ok(v) => is_truthy(&v),
                                    Err(_) => false,
                                }
                            })
                        })
                    } else {
                        false
                    };
                    let passes = if ef.negated { !matched } else { matched };
                    if passes {
                        rows.push(row);
                    }
                    return true; // Already handled — continue scan
                }
            };
            if ef.negated == found {
                return true; // Filtered out
            }
        }

        // Check IN filters
        for inf in &in_filters {
            let corr_key: Vec<Value> = inf
                .outer_col_indices
                .iter()
                .map(|&oci| {
                    col_vals
                        .iter()
                        .find(|(idx, _)| *idx == oci)
                        .unwrap()
                        .1
                        .clone()
                })
                .collect();
            if corr_key.iter().any(|v| v.is_null()) {
                if !inf.negated {
                    return true;
                } else {
                    continue;
                }
            }
            if let Some(values) = inf.map.get(&corr_key) {
                // Full decode needed for IN eval (subset: matching correlation keys only)
                let row = match decode_full_row(outer_schema, key, value) {
                    Ok(r) => r,
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                };
                let in_val = match eval_expr(&inf.in_expr, &outer_col_map, &row) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                };
                let found = if in_val.is_null() {
                    false
                } else {
                    values.contains(&in_val)
                };
                let passes = if inf.negated {
                    !found && !inf.has_null
                } else {
                    found
                };
                if !passes {
                    return true;
                }
                rows.push(row);
                return true;
            } else if !inf.negated {
                return true; // No matching correlation key → not in set
            }
        }

        // Row passed all filters — fully decode
        match decode_full_row(outer_schema, key, value) {
            Ok(row) => rows.push(row),
            Err(e) => {
                scan_err = Some(e);
                return false;
            }
        }
        scan_err.is_none()
    })
    .map_err(SqlError::Storage)?;

    if let Some(e) = scan_err {
        return Err(e);
    }

    let remaining = if remaining_conjuncts.is_empty() {
        None
    } else {
        Some(
            remaining_conjuncts
                .into_iter()
                .reduce(|a, b| Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinOp::And,
                    right: Box::new(b),
                })
                .unwrap(),
        )
    };
    Ok((rows, remaining))
}

enum RawColTarget {
    Pk(usize),    // PK position
    NonPk(usize), // Physical encoding position
}

fn raw_col_target(
    col_idx: usize,
    schema: &TableSchema,
    non_pk: &[usize],
    enc_pos: &[u16],
) -> RawColTarget {
    if let Some(pk_pos) = schema
        .primary_key_columns
        .iter()
        .position(|&c| c as usize == col_idx)
    {
        RawColTarget::Pk(pk_pos)
    } else {
        let nonpk_order = non_pk.iter().position(|&i| i == col_idx).unwrap();
        RawColTarget::NonPk(enc_pos[nonpk_order] as usize)
    }
}

fn extract_raw_value(
    key: &[u8],
    value: &[u8],
    target: &RawColTarget,
    num_pk_cols: usize,
) -> Result<Value> {
    match target {
        RawColTarget::Pk(pk_pos) => {
            if num_pk_cols == 1 && *pk_pos == 0 {
                Ok(Value::Integer(decode_pk_integer(key)?))
            } else {
                let pk = decode_composite_key(key, num_pk_cols)?;
                Ok(pk[*pk_pos].clone())
            }
        }
        RawColTarget::NonPk(idx) => Ok(decode_column_raw(value, *idx)?.to_value()),
    }
}

struct ExistsFilter {
    result: ExistsResult,
    outer_col_indices: Vec<usize>,
    negated: bool,
}

struct InFilter {
    map: HashMap<Vec<Value>, std::collections::HashSet<Value>>,
    has_null: bool,
    outer_col_indices: Vec<usize>,
    in_expr: Expr,
    negated: bool,
}

pub(super) fn handle_correlated_where_read(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctx: &CorrelationCtx,
    rows: &mut Vec<Vec<Value>>,
) -> Result<Option<Expr>> {
    let where_clause = match &stmt.where_clause {
        Some(w) => w,
        None => return Ok(None),
    };

    let conjuncts = flatten_and_exprs(where_clause);
    let mut remaining_conjuncts: Vec<Expr> = Vec::new();

    for conj in conjuncts {
        match conj {
            Expr::Exists { subquery, negated } => {
                if is_correlated_subquery(subquery, ctx, schema) {
                    let inner_schema =
                        resolve_inner_schema(db, schema, &subquery.from.to_ascii_lowercase())?;
                    let (corr_pairs, _) = extract_correlation_predicates(
                        subquery
                            .where_clause
                            .as_ref()
                            .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                        ctx,
                        &inner_schema,
                        subquery.from_alias.as_deref(),
                    );
                    if corr_pairs.is_empty() {
                        remaining_conjuncts.push(conj.clone());
                        continue;
                    }
                    let exists_result =
                        decorrelate_exists_read(db, schema, subquery, &corr_pairs, ctx)?;
                    let outer_col_indices: Vec<usize> =
                        corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                    let is_negated = *negated;
                    match &exists_result {
                        ExistsResult::Simple(key_set) => {
                            rows.retain(|row| {
                                let key: Vec<Value> =
                                    outer_col_indices.iter().map(|&i| row[i].clone()).collect();
                                if key.iter().any(|v| v.is_null()) {
                                    return is_negated;
                                }
                                let found = key_set.contains(&key);
                                if is_negated {
                                    !found
                                } else {
                                    found
                                }
                            });
                        }
                        ExistsResult::WithFilter(filter_data) => {
                            let inner_col_map = ColumnMap::new(&filter_data.inner_schema.columns);
                            let outer_col_map = ColumnMap::new(&ctx.outer_schema.columns);
                            rows.retain(|outer_row| {
                                let key: Vec<Value> = outer_col_indices
                                    .iter()
                                    .map(|&i| outer_row[i].clone())
                                    .collect();
                                if key.iter().any(|v| v.is_null()) {
                                    return is_negated;
                                }
                                let found =
                                    if let Some(inner_rows) = filter_data.rows_by_key.get(&key) {
                                        inner_rows.iter().any(|inner_row| {
                                            filter_data.non_eq_predicates.iter().all(|pred| {
                                                // Bind outer column refs to outer_row values
                                                let bound = bind_outer_values_in_expr(
                                                    pred,
                                                    outer_row,
                                                    &outer_col_map,
                                                    ctx,
                                                );
                                                match eval_expr(&bound, &inner_col_map, inner_row) {
                                                    Ok(val) => is_truthy(&val),
                                                    Err(_) => false,
                                                }
                                            })
                                        })
                                    } else {
                                        false
                                    };
                                if is_negated {
                                    !found
                                } else {
                                    found
                                }
                            });
                        }
                    }
                } else {
                    remaining_conjuncts.push(conj.clone());
                }
            }
            Expr::InSubquery {
                expr: in_expr,
                subquery,
                negated,
            } => {
                if is_correlated_subquery(subquery, ctx, schema) {
                    let inner_schema =
                        resolve_inner_schema(db, schema, &subquery.from.to_ascii_lowercase())?;
                    let (corr_pairs, _) = extract_correlation_predicates(
                        subquery
                            .where_clause
                            .as_ref()
                            .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                        ctx,
                        &inner_schema,
                        subquery.from_alias.as_deref(),
                    );
                    if corr_pairs.is_empty() {
                        remaining_conjuncts.push(conj.clone());
                        continue;
                    }
                    let (in_map, has_null) =
                        decorrelate_in_read(db, schema, subquery, &corr_pairs, ctx)?;
                    let outer_col_indices: Vec<usize> =
                        corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                    let is_negated = *negated;
                    let col_map = ColumnMap::new(&ctx.outer_schema.columns);
                    rows.retain(|row| {
                        let key: Vec<Value> =
                            outer_col_indices.iter().map(|&i| row[i].clone()).collect();
                        let in_val = match eval_expr(in_expr, &col_map, row) {
                            Ok(v) => v,
                            Err(_) => return false,
                        };
                        if in_val.is_null() {
                            return false; // NULL IN (...) is UNKNOWN
                        }
                        if key.iter().any(|v| v.is_null()) {
                            return is_negated;
                        }
                        let found = in_map.get(&key).is_some_and(|vals| vals.contains(&in_val));
                        if is_negated {
                            if has_null && !found {
                                false // NOT IN with NULL in set → UNKNOWN
                            } else {
                                !found
                            }
                        } else {
                            found
                        }
                    });
                } else {
                    remaining_conjuncts.push(conj.clone());
                }
            }
            _ => {
                // Check for scalar subquery comparisons: col > (SELECT ...)
                let mut handled = false;
                if let Expr::BinaryOp { left, op, right } = conj {
                    if let Expr::ScalarSubquery(sub) = right.as_ref() {
                        if is_correlated_subquery(sub, ctx, schema) {
                            let inner_schema =
                                resolve_inner_schema(db, schema, &sub.from.to_ascii_lowercase())?;
                            let (corr_pairs, _) = extract_correlation_predicates(
                                sub.where_clause
                                    .as_ref()
                                    .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                                ctx,
                                &inner_schema,
                                sub.from_alias.as_deref(),
                            );
                            if !corr_pairs.is_empty() {
                                let scalar_map =
                                    decorrelate_scalar_read(db, schema, sub, &corr_pairs, ctx)?;
                                let outer_col_indices: Vec<usize> =
                                    corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                                let cmp_op = *op;
                                let left_expr = left.clone();
                                let col_map = ColumnMap::new(&ctx.outer_schema.columns);
                                rows.retain(|row| {
                                    let key: Vec<Value> =
                                        outer_col_indices.iter().map(|&i| row[i].clone()).collect();
                                    let scalar_val =
                                        scalar_map.get(&key).cloned().unwrap_or(Value::Null);
                                    let left_val = match eval_expr(&left_expr, &col_map, row) {
                                        Ok(v) => v,
                                        Err(_) => return false,
                                    };
                                    let cmp_expr = Expr::BinaryOp {
                                        left: Box::new(Expr::Literal(left_val)),
                                        op: cmp_op,
                                        right: Box::new(Expr::Literal(scalar_val)),
                                    };
                                    match eval_expr(&cmp_expr, &col_map, row) {
                                        Ok(val) => is_truthy(&val),
                                        Err(_) => false,
                                    }
                                });
                                handled = true;
                            }
                        }
                    }
                }
                if !handled {
                    remaining_conjuncts.push(conj.clone());
                }
            }
        }
    }

    // Rebuild remaining WHERE
    if remaining_conjuncts.is_empty() {
        Ok(None)
    } else {
        let mut combined = remaining_conjuncts.remove(0);
        for r in remaining_conjuncts {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinOp::And,
                right: Box::new(r),
            };
        }
        Ok(Some(combined))
    }
}
