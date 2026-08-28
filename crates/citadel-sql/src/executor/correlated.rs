use citadel::Database;
use citadel_txn::read_txn::ReadTxn;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::encoding::{decode_column_raw, decode_composite_key, decode_pk_integer};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::helpers::{check_cancel, check_cancel_at, decode_full_row_with_cancel};
use super::CteContext;

#[derive(Default)]
pub(super) struct InValues {
    values: FxHashSet<Value>,
    has_null: bool,
}

pub(super) type InMap = FxHashMap<Vec<Value>, InValues>;

fn correlated_in_passes(
    group: Option<&InValues>,
    in_value: Value,
    value_collation: Collation,
    negated: bool,
) -> bool {
    let Some(group) = group else {
        // The correlated subquery is empty for this key.  In particular,
        // NULL NOT IN (empty) is true, unlike NULL NOT IN (nonempty).
        return negated;
    };
    if in_value.is_null() {
        return false;
    }

    let found = group.values.contains(&value_collation.fold(in_value));
    if found {
        !negated
    } else if group.has_null {
        false
    } else {
        negated
    }
}

fn correlation_collations(corr_pairs: &[CorrEqPair]) -> Vec<Collation> {
    corr_pairs.iter().map(|pair| pair.collation).collect()
}

fn correlation_key(row: &[Value], indices: &[usize], collations: &[Collation]) -> Vec<Value> {
    indices
        .iter()
        .enumerate()
        .map(|(position, &index)| {
            collations
                .get(position)
                .copied()
                .unwrap_or(Collation::Binary)
                .fold(row[index].clone())
        })
        .collect()
}

fn in_subquery_value_collation(
    subquery: &SelectStmt,
    inner_schema: &TableSchema,
) -> Result<Collation> {
    let expr = match &subquery.columns[0] {
        SelectColumn::Expr { expr, .. } => expr,
        _ => return Err(SqlError::Unsupported("complex IN subquery column".into())),
    };
    let index = in_subquery_value_column_index(expr, inner_schema)?;
    let col_map = inner_schema.column_map();
    Ok(crate::eval::operand_collation(expr, col_map)
        .unwrap_or(inner_schema.columns[index].collation))
}

fn in_subquery_value_column_index(expr: &Expr, inner_schema: &TableSchema) -> Result<usize> {
    let name = match expr {
        Expr::Column(name) => name,
        Expr::QualifiedColumn { column, .. } => column,
        Expr::Collate { expr, .. } => {
            return in_subquery_value_column_index(expr, inner_schema);
        }
        _ => return Err(SqlError::Unsupported("complex IN subquery column".into())),
    };
    inner_schema
        .column_index(name)
        .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
}

fn retain_cancellable<T>(
    values: &mut Vec<T>,
    cancel: Option<&citadel::CancelToken>,
    mut keep: impl FnMut(&T) -> Result<bool>,
) -> Result<()> {
    if cancel.is_none() {
        let mut error = None;
        values.retain(|item| {
            if error.is_some() {
                return false;
            }
            match keep(item) {
                Ok(keep) => keep,
                Err(err) => {
                    error = Some(err);
                    false
                }
            }
        });
        return error.map_or(Ok(()), Err);
    }

    let original = std::mem::take(values);
    values.reserve(original.len());
    for (item_idx, item) in original.into_iter().enumerate() {
        check_cancel_at(cancel, item_idx)?;
        if keep(&item)? {
            values.push(item);
        }
    }
    check_cancel(cancel)
}

fn any_cancellable<T>(
    values: &[T],
    cancel: Option<&citadel::CancelToken>,
    mut predicate: impl FnMut(&T) -> Result<bool>,
) -> Result<bool> {
    for (item_idx, item) in values.iter().enumerate() {
        check_cancel_at(cancel, item_idx)?;
        if predicate(item)? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[allow(clippy::type_complexity)]
pub(super) fn handle_correlated_select_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctx: &CorrelationCtx,
    rows: &mut [Vec<Value>],
    columns: &mut Vec<ColumnDef>,
) -> Result<SelectStmt> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let mut new_columns = Vec::new();
    let mut scalar_maps: Vec<(FxHashMap<Vec<Value>, Value>, Vec<usize>, Vec<Collation>)> =
        Vec::new();
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
                            let map =
                                decorrelate_scalar_with_read(rtx, schema, sub, &corr_pairs, ctx)?;
                            let outer_indices: Vec<usize> =
                                corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                            scalar_maps.push((
                                map,
                                outer_indices,
                                correlation_collations(&corr_pairs),
                            ));

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
                                is_with_timezone: false,
                                generated_expr: None,
                                generated_sql: None,
                                generated_kind: None,
                                collation: crate::types::Collation::Binary,
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

    for (row_idx, row) in rows.iter_mut().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        for (map, outer_indices, key_collations) in &scalar_maps {
            let key = correlation_key(row, outer_indices, key_collations);
            let val = if key.iter().any(|v| v.is_null()) {
                Value::Null
            } else {
                map.get(&key).cloned().unwrap_or(Value::Null)
            };
            row.push(val);
        }
    }

    check_cancel(cancel)?;

    Ok(SelectStmt {
        columns: new_columns,
        from: stmt.from.clone(),
        from_alias: stmt.from_alias.clone(),
        from_subquery: stmt.from_subquery.clone(),
        from_args: stmt.from_args.clone(),
        from_json_table: stmt.from_json_table.clone(),
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

pub(super) fn resolve_inner_schema_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    name: &str,
) -> Result<TableSchema> {
    if let Some(ts) = schema.get(name) {
        return Ok(ts.clone());
    }
    if let Some(vd) = schema.get_view(name) {
        let qr = super::exec_view_with_read(rtx, schema, vd)?;
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

pub(super) fn resolves_in(name: &str, schema: &TableSchema) -> bool {
    let lower = name.to_ascii_lowercase();
    schema.columns.iter().any(|c| c.name == lower)
}

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
        | Expr::Cast { expr: e, .. }
        | Expr::Collate { expr: e, .. } => {
            collect_column_names(e, out);
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) | Expr::ArrayLiteral(args) => {
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
        Expr::IsDistinctFrom { left, right, .. } => {
            collect_column_names(left, out);
            collect_column_names(right, out);
        }
        Expr::Like {
            expr: e,
            pattern,
            escape,
            ..
        } => {
            collect_column_names(e, out);
            collect_column_names(pattern, out);
            if let Some(escape) = escape {
                collect_column_names(escape, out);
            }
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
            if let Some(frame) = &spec.frame {
                for bound in [&frame.start, &frame.end] {
                    match bound {
                        WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
                            collect_column_names(expr, out);
                        }
                        _ => {}
                    }
                }
            }
        }
        Expr::InSubquery { expr: e, .. } => {
            collect_column_names(e, out);
        }
        Expr::InSet { expr: e, .. } => {
            collect_column_names(e, out);
        }
        Expr::Quantified { left, right, .. } => {
            collect_column_names(left, out);
            if let QuantifiedRhs::Array(expr) = right {
                collect_column_names(expr, out);
            }
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
            if table_part == inner_name || inner_alias.as_deref() == Some(table_part) {
                continue;
            }
            if ctx.matches_outer(table_part) && resolves_in(col_part, ctx.outer_schema) {
                return true;
            }
        } else if let Some(is) = inner_schema {
            if !resolves_in(name, is) && resolves_in(name, ctx.outer_schema) {
                return true;
            }
        }
    }
    false
}

/// A correlation equality predicate: outer_col = inner_col
pub(super) struct CorrEqPair {
    outer_col_name: String,
    outer_col_idx: usize,
    inner_col_name: String,
    /// Collation of the syntactic left operand of the extracted `=` predicate.
    collation: Collation,
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

    if let Some(pair) = try_match_corr_pair(left, right, ctx, inner_schema, inner_alias, true) {
        return Some(pair);
    }
    try_match_corr_pair(right, left, ctx, inner_schema, inner_alias, false)
}

pub(super) fn try_match_corr_pair(
    maybe_outer: &Expr,
    maybe_inner: &Expr,
    ctx: &CorrelationCtx,
    inner_schema: &TableSchema,
    inner_alias: Option<&str>,
    outer_is_left: bool,
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
            let inner_name = inner_schema.name.to_ascii_lowercase();
            if t == inner_name || inner_alias.is_some_and(|a| a.eq_ignore_ascii_case(&t)) {
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
    let inner_col_idx = inner_schema.column_index(&inner_col)?;
    let collation = if outer_is_left {
        ctx.outer_schema.columns[outer_col_idx].collation
    } else {
        inner_schema.columns[inner_col_idx].collation
    };

    Some(CorrEqPair {
        outer_col_name: outer_col,
        outer_col_idx,
        inner_col_name: inner_col,
        collation,
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
    let corr_outer: FxHashSet<&str> = corr_pairs
        .iter()
        .map(|p| p.outer_col_name.as_str())
        .collect();
    let corr_inner: FxHashSet<&str> = corr_pairs
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
    inner_col_map: &ColumnMap,
    ctx: &CorrelationCtx,
) -> Expr {
    let bind =
        |expr: &Expr| bind_outer_values_in_expr(expr, outer_row, outer_col_map, inner_col_map, ctx);
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
            if matches!(
                inner_col_map.resolve(&lower),
                Err(SqlError::ColumnNotFound(_))
            ) {
                if let Ok(idx) = outer_col_map.resolve(&lower) {
                    return Expr::Literal(outer_row[idx].clone());
                }
            }
            expr.clone()
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(bind(left)),
            op: *op,
            right: Box::new(bind(right)),
        },
        Expr::UnaryOp { op, expr: e } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(bind(e)),
        },
        Expr::IsNull(expr) => Expr::IsNull(Box::new(bind(expr))),
        Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(bind(expr))),
        Expr::Function {
            name,
            args,
            distinct,
        } => Expr::Function {
            name: name.clone(),
            args: args.iter().map(bind).collect(),
            distinct: *distinct,
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(bind(expr)),
            subquery: subquery.clone(),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(bind(expr)),
            list: list.iter().map(bind).collect(),
            negated: *negated,
        },
        Expr::InSet {
            expr,
            values,
            has_null,
            negated,
            collation,
        } => Expr::InSet {
            expr: Box::new(bind(expr)),
            values: values.clone(),
            has_null: *has_null,
            negated: *negated,
            collation: *collation,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(bind(expr)),
            low: Box::new(bind(low)),
            high: Box::new(bind(high)),
            negated: *negated,
        },
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => Expr::IsDistinctFrom {
            left: Box::new(bind(left)),
            right: Box::new(bind(right)),
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Like {
            expr: Box::new(bind(expr)),
            pattern: Box::new(bind(pattern)),
            escape: escape.as_ref().map(|expr| Box::new(bind(expr))),
            negated: *negated,
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            operand: operand.as_ref().map(|expr| Box::new(bind(expr))),
            conditions: conditions
                .iter()
                .map(|(condition, result)| (bind(condition), bind(result)))
                .collect(),
            else_result: else_result.as_ref().map(|expr| Box::new(bind(expr))),
        },
        Expr::Coalesce(args) => Expr::Coalesce(args.iter().map(bind).collect()),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(bind(expr)),
            data_type: *data_type,
        },
        Expr::WindowFunction { name, args, spec } => {
            let mut spec = spec.clone();
            spec.partition_by = spec.partition_by.iter().map(bind).collect();
            for item in &mut spec.order_by {
                item.expr = bind(&item.expr);
            }
            if let Some(frame) = &mut spec.frame {
                for bound in [&mut frame.start, &mut frame.end] {
                    match bound {
                        WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
                            **expr = bind(expr);
                        }
                        _ => {}
                    }
                }
            }
            Expr::WindowFunction {
                name: name.clone(),
                args: args.iter().map(bind).collect(),
                spec,
            }
        }
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: Box::new(bind(expr)),
            collation: *collation,
        },
        Expr::ArrayLiteral(values) => Expr::ArrayLiteral(values.iter().map(bind).collect()),
        Expr::Quantified {
            left,
            op,
            quantifier,
            right,
        } => Expr::Quantified {
            left: Box::new(bind(left)),
            op: *op,
            quantifier: *quantifier,
            right: match right {
                QuantifiedRhs::Subquery(subquery) => QuantifiedRhs::Subquery(subquery.clone()),
                QuantifiedRhs::Array(expr) => QuantifiedRhs::Array(Box::new(bind(expr))),
            },
        },
        Expr::Literal(_)
        | Expr::CountStar
        | Expr::Exists { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Parameter(_)
        | Expr::TypedNullRecord(_) => expr.clone(),
    }
}

pub(super) enum ExistsResult {
    Simple(FxHashSet<Vec<Value>>),
    WithFilter(Box<ExistsFilterData>),
}

pub(super) struct ExistsFilterData {
    rows_by_key: FxHashMap<Vec<Value>, Vec<Vec<Value>>>,
    non_eq_predicates: Vec<Expr>,
    inner_schema: TableSchema,
}

pub(super) fn decorrelate_exists_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<ExistsResult> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let inner_name = subquery.from.to_ascii_lowercase();

    let (inner_schema_owned, inner_rows) = if let Some(ts) = schema.get(&inner_name) {
        let (inner_where, _) =
            strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, ts);
        let (rows, _) = super::collect_rows_with_read(rtx, ts, &inner_where, None)?;
        (ts.clone(), rows)
    } else if let Some(vd) = schema.get_view(&inner_name) {
        let vqr = super::exec_view_with_read(rtx, schema, vd)?;
        let vs = super::build_view_schema(&inner_name, &vqr);
        let (inner_where, _) =
            strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, &vs);
        let col_map = ColumnMap::new(&vs.columns);
        let rows: Vec<Vec<Value>> = if let Some(ref w) = inner_where {
            let mut filtered = Vec::new();
            for (row_idx, row) in vqr.result.rows.into_iter().enumerate() {
                check_cancel_at(cancel, row_idx)?;
                if is_truthy(&eval_expr(
                    w,
                    &EvalCtx::new(&col_map, &row).with_cancel(cancel),
                )?) {
                    filtered.push(row);
                }
            }
            filtered
        } else {
            vqr.result.rows
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
    let key_collations = correlation_collations(corr_pairs);

    if non_eq.is_empty() {
        let mut key_set = FxHashSet::default();
        for (row_idx, row) in inner_rows.iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            let key = correlation_key(row, &inner_col_indices, &key_collations);
            if key.iter().any(|v| v.is_null()) {
                continue;
            }
            key_set.insert(key);
        }
        check_cancel(cancel)?;
        Ok(ExistsResult::Simple(key_set))
    } else {
        let mut rows_by_key: FxHashMap<Vec<Value>, Vec<Vec<Value>>> = FxHashMap::default();
        for (row_idx, row) in inner_rows.into_iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            let key = correlation_key(&row, &inner_col_indices, &key_collations);
            if key.iter().any(|v| v.is_null()) {
                continue;
            }
            rows_by_key.entry(key).or_default().push(row);
        }
        check_cancel(cancel)?;
        Ok(ExistsResult::WithFilter(Box::new(ExistsFilterData {
            rows_by_key,
            non_eq_predicates: non_eq,
            inner_schema: inner_schema.clone(),
        })))
    }
}

/// Decorrelate IN/NOT IN subquery. Returns correlation key → IN-column value set.
pub(super) fn decorrelate_in_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
    value_collation: Collation,
) -> Result<InMap> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema
        .get(&inner_name)
        .ok_or_else(|| SqlError::TableNotFound(subquery.from.clone()))?;

    let in_expr = match &subquery.columns[0] {
        SelectColumn::Expr { expr, .. } => expr,
        _ => return Err(SqlError::Unsupported("complex IN subquery column".into())),
    };
    let in_col_idx = in_subquery_value_column_index(in_expr, inner_schema)?;

    let (inner_where, _non_eq) =
        strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, inner_schema);
    let (inner_rows, _) = super::collect_rows_with_read(rtx, inner_schema, &inner_where, None)?;

    let inner_corr_indices: Vec<usize> = corr_pairs
        .iter()
        .map(|p| inner_schema.column_index(&p.inner_col_name).unwrap_or(0))
        .collect();
    let key_collations = correlation_collations(corr_pairs);

    let mut map: InMap = FxHashMap::default();

    for (row_idx, row) in inner_rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let key = correlation_key(row, &inner_corr_indices, &key_collations);
        if key.iter().any(|v| v.is_null()) {
            continue;
        }
        let in_val = row[in_col_idx].clone();
        let entry = map.entry(key).or_default();
        if in_val.is_null() {
            entry.has_null = true;
        } else {
            entry.values.insert(value_collation.fold(in_val));
        }
    }

    check_cancel(cancel)?;
    Ok(map)
}

/// Decorrelate scalar subquery. Returns correlation key → scalar result.
pub(super) fn decorrelate_scalar_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<FxHashMap<Vec<Value>, Value>> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
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
        from_subquery: subquery.from_subquery.clone(),
        from_args: subquery.from_args.clone(),
        from_json_table: subquery.from_json_table.clone(),
        joins: vec![],
        distinct: false,
        where_clause: inner_where,
        order_by: vec![],
        limit: None,
        offset: None,
        group_by,
        having: None,
    };

    let empty_ctes = CteContext::default();
    let qr = match super::exec_select_with_read(rtx, schema, &rewritten, &empty_ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => return Ok(FxHashMap::default()),
    };

    let num_corr = corr_pairs.len();
    let key_collations = correlation_collations(corr_pairs);
    let key_indices: Vec<usize> = (0..num_corr).collect();
    let mut map = FxHashMap::default();
    for (row_idx, row) in qr.rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let key = correlation_key(row, &key_indices, &key_collations);
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

    check_cancel(cancel)?;
    Ok(map)
}

// Write-transaction variants below — same logic, use collect_rows_write.

pub(super) fn decorrelate_exists_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<FxHashSet<Vec<Value>>> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
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
    let key_collations = correlation_collations(corr_pairs);
    let mut key_set = FxHashSet::default();
    for (row_idx, row) in inner_rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let key = correlation_key(row, &inner_col_indices, &key_collations);
        if key.iter().any(|v| v.is_null()) {
            continue;
        }
        key_set.insert(key);
    }
    check_cancel(cancel)?;
    Ok(key_set)
}

pub(super) fn decorrelate_in_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
    value_collation: Collation,
) -> Result<InMap> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema
        .get(&inner_name)
        .ok_or_else(|| SqlError::TableNotFound(subquery.from.clone()))?;
    let in_expr = match &subquery.columns[0] {
        SelectColumn::Expr { expr, .. } => expr,
        _ => return Err(SqlError::Unsupported("complex IN subquery column".into())),
    };
    let in_col_idx = in_subquery_value_column_index(in_expr, inner_schema)?;
    let (inner_where, _non_eq) =
        strip_correlation_predicates(&subquery.where_clause, corr_pairs, ctx, inner_schema);
    let (inner_rows, _) = super::collect_rows_write(wtx, inner_schema, &inner_where, None)?;
    let inner_corr_indices: Vec<usize> = corr_pairs
        .iter()
        .map(|p| inner_schema.column_index(&p.inner_col_name).unwrap_or(0))
        .collect();
    let key_collations = correlation_collations(corr_pairs);
    let mut map: InMap = FxHashMap::default();
    for (row_idx, row) in inner_rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let key = correlation_key(row, &inner_corr_indices, &key_collations);
        if key.iter().any(|v| v.is_null()) {
            continue;
        }
        let in_val = row[in_col_idx].clone();
        let entry = map.entry(key).or_default();
        if in_val.is_null() {
            entry.has_null = true;
        } else {
            entry.values.insert(value_collation.fold(in_val));
        }
    }
    check_cancel(cancel)?;
    Ok(map)
}

pub(super) fn decorrelate_scalar_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<FxHashMap<Vec<Value>, Value>> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
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
        from_subquery: subquery.from_subquery.clone(),
        from_args: subquery.from_args.clone(),
        from_json_table: subquery.from_json_table.clone(),
        joins: vec![],
        distinct: false,
        where_clause: inner_where,
        order_by: vec![],
        limit: None,
        offset: None,
        group_by,
        having: None,
    };
    let empty_ctes = CteContext::default();
    let qr = match super::exec_select_in_txn(wtx, schema, &rewritten, &empty_ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => return Ok(FxHashMap::default()),
    };
    let num_corr = corr_pairs.len();
    let key_collations = correlation_collations(corr_pairs);
    let key_indices: Vec<usize> = (0..num_corr).collect();
    let mut map = FxHashMap::default();
    for (row_idx, row) in qr.rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let key = correlation_key(row, &key_indices, &key_collations);
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
    check_cancel(cancel)?;
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
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
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
                    let key_collations = correlation_collations(&corr_pairs);
                    let is_negated = *negated;
                    retain_cancellable(rows, cancel, |row| {
                        let key = correlation_key(row, &outer_col_indices, &key_collations);
                        if key.iter().any(|v| v.is_null()) {
                            return Ok(is_negated);
                        }
                        let found = key_set.contains(&key);
                        Ok(if is_negated { !found } else { found })
                    })?;
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
                    let col_map = ColumnMap::new(&ctx.outer_schema.columns);
                    let selected_collation = in_subquery_value_collation(subquery, &inner_schema)?;
                    let value_collation = crate::eval::operand_collation(in_expr, &col_map)
                        .unwrap_or(selected_collation);
                    let in_map = decorrelate_in_write(
                        wtx,
                        schema,
                        subquery,
                        &corr_pairs,
                        ctx,
                        value_collation,
                    )?;
                    let outer_col_indices: Vec<usize> =
                        corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                    let key_collations = correlation_collations(&corr_pairs);
                    let is_negated = *negated;
                    retain_cancellable(rows, cancel, |row| {
                        let key = correlation_key(row, &outer_col_indices, &key_collations);
                        let in_val =
                            eval_expr(in_expr, &EvalCtx::new(&col_map, row).with_cancel(cancel))?;
                        let group = if key.iter().any(|v| v.is_null()) {
                            None
                        } else {
                            in_map.get(&key)
                        };
                        Ok(correlated_in_passes(
                            group,
                            in_val,
                            value_collation,
                            is_negated,
                        ))
                    })?;
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
                                let key_collations = correlation_collations(&corr_pairs);
                                let cmp_op = *op;
                                let left_expr = left.clone();
                                let col_map = ColumnMap::new(&ctx.outer_schema.columns);
                                retain_cancellable(rows, cancel, |row| {
                                    let key =
                                        correlation_key(row, &outer_col_indices, &key_collations);
                                    let scalar_val =
                                        scalar_map.get(&key).cloned().unwrap_or(Value::Null);
                                    let left_val = eval_expr(
                                        &left_expr,
                                        &EvalCtx::new(&col_map, row).with_cancel(cancel),
                                    )?;
                                    let cmp = Expr::BinaryOp {
                                        left: Box::new(Expr::Literal(left_val)),
                                        op: cmp_op,
                                        right: Box::new(Expr::Literal(scalar_val)),
                                    };
                                    Ok(is_truthy(&eval_expr(
                                        &cmp,
                                        &EvalCtx::new(&col_map, row).with_cancel(cancel),
                                    )?))
                                })?;
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

    check_cancel(cancel)?;
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
pub(super) fn build_and_scan_correlated_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    outer_schema: &TableSchema,
    ctx: &CorrelationCtx,
) -> Result<(Vec<Vec<Value>>, Option<Expr>)> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let where_clause = match &stmt.where_clause {
        Some(w) => w,
        None => {
            let (rows, _) = super::collect_rows_with_read(rtx, outer_schema, &None, None)?;
            return Ok((rows, None));
        }
    };

    let conjuncts = flatten_and_exprs(where_clause);
    let mut exists_filters: Vec<ExistsFilter> = Vec::new();
    let mut in_filters: Vec<InFilter> = Vec::new();
    let mut remaining_conjuncts: Vec<Expr> = Vec::new();
    let outer_col_map = ColumnMap::new(&outer_schema.columns);

    for conj in &conjuncts {
        match conj {
            Expr::Exists { subquery, negated } if is_correlated_subquery(subquery, ctx, schema) => {
                let inner_schema = resolve_inner_schema_with_read(
                    rtx,
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
                    remaining_conjuncts.push((*conj).clone());
                    continue;
                }
                let result = decorrelate_exists_with_read(rtx, schema, subquery, &corr_pairs, ctx)?;
                let outer_col_indices: Vec<usize> =
                    corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                exists_filters.push(ExistsFilter {
                    result,
                    outer_col_indices,
                    key_collations: correlation_collations(&corr_pairs),
                    negated: *negated,
                });
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } if is_correlated_subquery(subquery, ctx, schema) => {
                let inner_schema = resolve_inner_schema_with_read(
                    rtx,
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
                    remaining_conjuncts.push((*conj).clone());
                    continue;
                }
                let selected_collation = in_subquery_value_collation(subquery, &inner_schema)?;
                let value_collation = crate::eval::operand_collation(expr, &outer_col_map)
                    .unwrap_or(selected_collation);
                let map = decorrelate_in_with_read(
                    rtx,
                    schema,
                    subquery,
                    &corr_pairs,
                    ctx,
                    value_collation,
                )?;
                let outer_col_indices: Vec<usize> =
                    corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                in_filters.push(InFilter {
                    map,
                    outer_col_indices,
                    key_collations: correlation_collations(&corr_pairs),
                    value_collation,
                    in_expr: (**expr).clone(),
                    negated: *negated,
                });
            }
            _ => remaining_conjuncts.push((*conj).clone()),
        }
    }

    // If no optimizable filters, fall back to generic path
    if exists_filters.is_empty() && in_filters.is_empty() {
        let (mut rows, _) = super::collect_rows_with_read(rtx, outer_schema, &None, None)?;
        let remaining = handle_correlated_where_with_read(rtx, schema, stmt, ctx, &mut rows)?;
        return Ok((rows, remaining));
    }

    let lower = &outer_schema.name;
    let num_pk_cols = outer_schema.primary_key_columns.len();
    let non_pk = outer_schema.non_pk_indices();
    let enc_pos = outer_schema.encoding_positions();
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

    let mut col_vals: Vec<(usize, Value)> = Vec::with_capacity(needed_raw.len());
    let max_key_cols = exists_filters
        .iter()
        .map(|ef| ef.outer_col_indices.len())
        .chain(in_filters.iter().map(|inf| inf.outer_col_indices.len()))
        .max()
        .unwrap_or(0);
    let mut outer_key: Vec<Value> = Vec::with_capacity(max_key_cols);
    let mut corr_key: Vec<Value> = Vec::with_capacity(max_key_cols);

    rtx.table_scan_raw(lower.as_bytes(), |key, value| {
        // Extract only the correlation columns from raw bytes (fast partial decode)
        col_vals.clear();
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
        let mut decoded_row: Option<Vec<Value>> = None;

        for ef in &exists_filters {
            outer_key.clear();
            for (position, &oci) in ef.outer_col_indices.iter().enumerate() {
                let val = col_vals
                    .iter()
                    .find(|(idx, _)| *idx == oci)
                    .unwrap()
                    .1
                    .clone();
                outer_key.push(
                    ef.key_collations
                        .get(position)
                        .copied()
                        .unwrap_or(Collation::Binary)
                        .fold(val),
                );
            }
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
                    if decoded_row.is_none() {
                        decoded_row =
                            match decode_full_row_with_cancel(outer_schema, key, value, cancel) {
                                Ok(row) => Some(row),
                                Err(e) => {
                                    scan_err = Some(e);
                                    return false;
                                }
                            };
                    }
                    let row = decoded_row.as_ref().unwrap();
                    let inner_col_map = filter_data.inner_schema.column_map();
                    let matched = match filter_data.rows_by_key.get(&outer_key) {
                        Some(inner_rows) if !inner_rows.is_empty() => {
                            // The binding varies only with the outer row, so rebuilding the
                            // predicate tree per inner row is pure allocator traffic.
                            let bound: Vec<_> = filter_data
                                .non_eq_predicates
                                .iter()
                                .map(|pred| {
                                    bind_outer_values_in_expr(
                                        pred,
                                        row,
                                        &outer_col_map,
                                        inner_col_map,
                                        ctx,
                                    )
                                })
                                .collect();
                            match any_cancellable(inner_rows, cancel, |inner_row| {
                                for predicate in &bound {
                                    if !is_truthy(&eval_expr(
                                        predicate,
                                        &EvalCtx::new(inner_col_map, inner_row).with_cancel(cancel),
                                    )?) {
                                        return Ok(false);
                                    }
                                }
                                Ok(true)
                            }) {
                                Ok(matched) => matched,
                                Err(err) => {
                                    scan_err = Some(err);
                                    return false;
                                }
                            }
                        }
                        _ => false,
                    };
                    matched
                }
            };
            if ef.negated == found {
                return true; // Filtered out
            }
        }

        for inf in &in_filters {
            corr_key.clear();
            for (position, &oci) in inf.outer_col_indices.iter().enumerate() {
                let val = col_vals
                    .iter()
                    .find(|(idx, _)| *idx == oci)
                    .unwrap()
                    .1
                    .clone();
                corr_key.push(
                    inf.key_collations
                        .get(position)
                        .copied()
                        .unwrap_or(Collation::Binary)
                        .fold(val),
                );
            }
            let group = if corr_key.iter().any(|v| v.is_null()) {
                None
            } else {
                inf.map.get(&corr_key)
            };
            if group.is_some() {
                // Full decode needed for IN eval (subset: matching correlation keys only)
                if decoded_row.is_none() {
                    decoded_row =
                        match decode_full_row_with_cancel(outer_schema, key, value, cancel) {
                            Ok(row) => Some(row),
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        };
                }
                let row = decoded_row.as_ref().unwrap();
                let in_val = match eval_expr(
                    &inf.in_expr,
                    &EvalCtx::new(&outer_col_map, row).with_cancel(cancel),
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                };
                if !correlated_in_passes(group, in_val, inf.value_collation, inf.negated) {
                    return true;
                }
            } else if !correlated_in_passes(None, Value::Null, inf.value_collation, inf.negated) {
                return true;
            }
        }

        // Row passed every filter. Reuse a full decode performed by a filtered
        // EXISTS/IN clause, or decode once now for the output.
        let row = match decoded_row {
            Some(row) => row,
            None => match decode_full_row_with_cancel(outer_schema, key, value, cancel) {
                Ok(row) => row,
                Err(e) => {
                    scan_err = Some(e);
                    return false;
                }
            },
        };
        rows.push(row);
        scan_err.is_none()
    })
    .map_err(SqlError::Storage)?;

    if let Some(e) = scan_err {
        return Err(e);
    }

    check_cancel(cancel)?;

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
    key_collations: Vec<Collation>,
    negated: bool,
}

struct InFilter {
    map: InMap,
    outer_col_indices: Vec<usize>,
    key_collations: Vec<Collation>,
    value_collation: Collation,
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
    let mut rtx = db.begin_read();
    handle_correlated_where_with_read(&mut rtx, schema, stmt, ctx, rows)
}

pub(super) fn handle_correlated_where_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctx: &CorrelationCtx,
    rows: &mut Vec<Vec<Value>>,
) -> Result<Option<Expr>> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
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
                    let inner_schema = resolve_inner_schema_with_read(
                        rtx,
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
                    let exists_result =
                        decorrelate_exists_with_read(rtx, schema, subquery, &corr_pairs, ctx)?;
                    let outer_col_indices: Vec<usize> =
                        corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                    let key_collations = correlation_collations(&corr_pairs);
                    let is_negated = *negated;
                    match &exists_result {
                        ExistsResult::Simple(key_set) => {
                            retain_cancellable(rows, cancel, |row| {
                                let key = correlation_key(row, &outer_col_indices, &key_collations);
                                if key.iter().any(|v| v.is_null()) {
                                    return Ok(is_negated);
                                }
                                let found = key_set.contains(&key);
                                Ok(if is_negated { !found } else { found })
                            })?;
                        }
                        ExistsResult::WithFilter(filter_data) => {
                            let inner_col_map = ColumnMap::new(&filter_data.inner_schema.columns);
                            let outer_col_map = ColumnMap::new(&ctx.outer_schema.columns);
                            retain_cancellable(rows, cancel, |outer_row| {
                                let key =
                                    correlation_key(outer_row, &outer_col_indices, &key_collations);
                                if key.iter().any(|v| v.is_null()) {
                                    return Ok(is_negated);
                                }
                                let found = match filter_data.rows_by_key.get(&key) {
                                    Some(inner_rows) if !inner_rows.is_empty() => {
                                        // Bind once per outer row, not once per inner row.
                                        let bound: Vec<_> = filter_data
                                            .non_eq_predicates
                                            .iter()
                                            .map(|pred| {
                                                bind_outer_values_in_expr(
                                                    pred,
                                                    outer_row,
                                                    &outer_col_map,
                                                    &inner_col_map,
                                                    ctx,
                                                )
                                            })
                                            .collect();
                                        any_cancellable(inner_rows, cancel, |inner_row| {
                                            for predicate in &bound {
                                                if !is_truthy(&eval_expr(
                                                    predicate,
                                                    &EvalCtx::new(&inner_col_map, inner_row)
                                                        .with_cancel(cancel),
                                                )?) {
                                                    return Ok(false);
                                                }
                                            }
                                            Ok(true)
                                        })?
                                    }
                                    _ => false,
                                };
                                Ok(if is_negated { !found } else { found })
                            })?;
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
                    let inner_schema = resolve_inner_schema_with_read(
                        rtx,
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
                    let col_map = ColumnMap::new(&ctx.outer_schema.columns);
                    let selected_collation = in_subquery_value_collation(subquery, &inner_schema)?;
                    let value_collation = crate::eval::operand_collation(in_expr, &col_map)
                        .unwrap_or(selected_collation);
                    let in_map = decorrelate_in_with_read(
                        rtx,
                        schema,
                        subquery,
                        &corr_pairs,
                        ctx,
                        value_collation,
                    )?;
                    let outer_col_indices: Vec<usize> =
                        corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                    let is_negated = *negated;
                    let key_collations = correlation_collations(&corr_pairs);
                    retain_cancellable(rows, cancel, |row| {
                        let key = correlation_key(row, &outer_col_indices, &key_collations);
                        let in_val =
                            eval_expr(in_expr, &EvalCtx::new(&col_map, row).with_cancel(cancel))?;
                        let group = if key.iter().any(|v| v.is_null()) {
                            None
                        } else {
                            in_map.get(&key)
                        };
                        Ok(correlated_in_passes(
                            group,
                            in_val,
                            value_collation,
                            is_negated,
                        ))
                    })?;
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
                            let inner_schema = resolve_inner_schema_with_read(
                                rtx,
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
                                let scalar_map = decorrelate_scalar_with_read(
                                    rtx,
                                    schema,
                                    sub,
                                    &corr_pairs,
                                    ctx,
                                )?;
                                let outer_col_indices: Vec<usize> =
                                    corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                                let key_collations = correlation_collations(&corr_pairs);
                                let cmp_op = *op;
                                let left_expr = left.clone();
                                let col_map = ColumnMap::new(&ctx.outer_schema.columns);
                                retain_cancellable(rows, cancel, |row| {
                                    let key =
                                        correlation_key(row, &outer_col_indices, &key_collations);
                                    let scalar_val =
                                        scalar_map.get(&key).cloned().unwrap_or(Value::Null);
                                    let left_val = eval_expr(
                                        &left_expr,
                                        &EvalCtx::new(&col_map, row).with_cancel(cancel),
                                    )?;
                                    let cmp_expr = Expr::BinaryOp {
                                        left: Box::new(Expr::Literal(left_val)),
                                        op: cmp_op,
                                        right: Box::new(Expr::Literal(scalar_val)),
                                    };
                                    Ok(is_truthy(&eval_expr(
                                        &cmp_expr,
                                        &EvalCtx::new(&col_map, row).with_cancel(cancel),
                                    )?))
                                })?;
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

    check_cancel(cancel)?;
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

#[cfg(test)]
#[path = "correlated_tests.rs"]
mod tests;
