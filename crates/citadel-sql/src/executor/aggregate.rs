use std::collections::BTreeMap;

use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, operand_collation, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::types::*;

use super::helpers::*;

/// Takes the same `(rows, ctx)` shape as `process_select`: it is the other
/// post-scan entry point, over the same columns, statement and token.
pub(super) fn exec_aggregate(
    rows: &[Vec<Value>],
    ctx: super::SelectCtx<'_>,
) -> Result<ExecutionResult> {
    let super::SelectCtx {
        columns,
        stmt,
        cancel,
        ..
    } = ctx;
    check_cancel(cancel)?;
    let col_map = ColumnMap::new(columns);
    let group_exprs = resolve_group_by_exprs(&stmt.group_by, &stmt.columns, &col_map)?;
    let groups = group_rows(rows, &group_exprs, &col_map, cancel)?;

    let mut result_rows = Vec::new();
    let output_cols = build_output_columns(&stmt.columns, columns);
    let output_map = ColumnMap::new(&output_cols);
    let order_output_positions = stmt
        .order_by
        .iter()
        .map(|item| order_by_output_position(item, &output_map))
        .collect::<Result<Vec<_>>>()?;
    let order_collations: Vec<Collation> = stmt
        .order_by
        .iter()
        .zip(&order_output_positions)
        .map(|(item, output_position)| {
            output_position.map_or_else(
                || expr_collation(&item.expr, &col_map),
                |position| output_map.collation_at(position),
            )
        })
        .collect();
    let mut result_sort_keys = Vec::with_capacity(groups.len());

    for (group_idx, group_rows) in groups.iter().enumerate() {
        check_cancel_at(cancel, group_idx)?;
        let mut result_row = Vec::new();

        for sel_col in &stmt.columns {
            match sel_col {
                SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                    return Err(SqlError::Unsupported("SELECT * with GROUP BY".into()));
                }
                SelectColumn::Expr { expr, .. } => {
                    let val = eval_aggregate_expr_with_cancel(expr, &col_map, group_rows, cancel)?;
                    result_row.push(val);
                }
            }
        }

        if let Some(ref having) = stmt.having {
            let passes = match eval_aggregate_expr_with_cancel(having, &col_map, group_rows, cancel)
            {
                Ok(val) => is_truthy(&val),
                Err(SqlError::ColumnNotFound(_)) => {
                    let output_map = ColumnMap::new(&output_cols);
                    is_truthy(&eval_expr(
                        having,
                        &EvalCtx::new(&output_map, &result_row).with_cancel(cancel),
                    )?)
                }
                Err(e) => return Err(e),
            };
            if !passes {
                continue;
            }
        }

        if !stmt.order_by.is_empty() {
            let mut key = Vec::with_capacity(stmt.order_by.len());
            for (item, output_position) in stmt.order_by.iter().zip(&order_output_positions) {
                let value = match output_position {
                    Some(position) => result_row[*position].clone(),
                    None => {
                        eval_aggregate_expr_with_cancel(&item.expr, &col_map, group_rows, cancel)?
                    }
                };
                key.push(value);
            }
            result_sort_keys.push(key);
        }
        result_rows.push(result_row);
    }

    if stmt.distinct {
        let out_colls = output_collations(&stmt.columns, &col_map);
        let mut seen: rustc_hash::FxHashSet<Vec<Value>> = rustc_hash::FxHashSet::default();
        if !stmt.order_by.is_empty() {
            let original_rows = std::mem::take(&mut result_rows);
            let original_keys = std::mem::take(&mut result_sort_keys);
            result_rows.reserve(original_rows.len());
            result_sort_keys.reserve(original_keys.len());
            for (row_idx, (row, key)) in original_rows.into_iter().zip(original_keys).enumerate() {
                check_cancel_at(cancel, row_idx)?;
                if seen.insert(fold_key(&row, &out_colls)) {
                    result_rows.push(row);
                    result_sort_keys.push(key);
                }
            }
            check_cancel(cancel)?;
        } else if cancel.is_none() {
            result_rows.retain(|row| seen.insert(fold_key(row, &out_colls)));
        } else {
            let original = std::mem::take(&mut result_rows);
            result_rows.reserve(original.len());
            for (row_idx, row) in original.into_iter().enumerate() {
                check_cancel_at(cancel, row_idx)?;
                if seen.insert(fold_key(&row, &out_colls)) {
                    result_rows.push(row);
                }
            }
            check_cancel(cancel)?;
        }
    }

    if !stmt.order_by.is_empty() {
        sort_rows_by_keys(
            &mut result_rows,
            &result_sort_keys,
            &stmt.order_by,
            &order_collations,
            cancel,
        )?;
    }

    check_cancel(cancel)?;

    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_row_count(offset_expr)?;
        if offset < result_rows.len() {
            result_rows = result_rows.split_off(offset);
        } else {
            result_rows.clear();
        }
    }
    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_row_count(limit_expr)?;
        result_rows.truncate(limit);
    }

    let col_names = stmt
        .columns
        .iter()
        .map(|c| match c {
            SelectColumn::AllColumns => "*".into(),
            SelectColumn::AllFromOld => "old.*".into(),
            SelectColumn::AllFromNew => "new.*".into(),
            SelectColumn::Expr { alias: Some(a), .. } => a.clone(),
            SelectColumn::Expr { expr, .. } => expr_display_name(expr),
        })
        .collect();

    check_cancel(cancel)?;
    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: result_rows,
    }))
}

/// Whether a query aggregates its rows into groups: it has GROUP BY or
/// HAVING, or its select list or ORDER BY calls an aggregate. Without GROUP
/// BY, all its rows are one group.
pub(super) fn is_grouped(stmt: &SelectStmt) -> bool {
    !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || stmt.columns.iter().any(
            |column| matches!(column, SelectColumn::Expr { expr, .. } if is_aggregate_expr(expr)),
        )
        || stmt
            .order_by
            .iter()
            .any(|item| is_aggregate_expr(&item.expr))
}

/// What a grouped query's window functions run over. They run after GROUP BY
/// and HAVING, so each row is a group HAVING keeps: the group's first row, or
/// NULLs for the one group of an ungrouped query over no rows, followed by the
/// value of each aggregate call.
pub(super) struct GroupedWindowInput {
    pub(super) columns: Vec<ColumnDef>,
    pub(super) rows: Vec<Vec<Value>>,
    /// Reads each aggregate call from its column. WHERE, GROUP BY and HAVING
    /// are gone: the rows already reflect them.
    pub(super) stmt: SelectStmt,
}

/// None when the query does not group.
pub(super) fn group_for_windows(
    rows: &[Vec<Value>],
    ctx: super::SelectCtx<'_>,
) -> Result<Option<GroupedWindowInput>> {
    let super::SelectCtx {
        columns,
        stmt,
        cancel,
        ..
    } = ctx;
    let mut select = stmt.clone();
    let mut calls = Vec::new();
    for column in &mut select.columns {
        if let SelectColumn::Expr { expr, alias } = column {
            let written = alias.is_none().then(|| expr_display_name(expr));
            lift_aggregates(expr, &mut calls);
            if let Some(written) = written {
                if written != expr_display_name(expr) {
                    *alias = Some(written);
                }
            }
        }
    }
    for item in &mut select.order_by {
        lift_aggregates(&mut item.expr, &mut calls);
    }
    if calls.is_empty() && stmt.group_by.is_empty() && stmt.having.is_none() {
        return Ok(None);
    }
    if stmt
        .columns
        .iter()
        .any(|column| !matches!(column, SelectColumn::Expr { .. }))
    {
        return Err(SqlError::Unsupported("SELECT * with GROUP BY".into()));
    }
    check_cancel(cancel)?;
    let col_map = ColumnMap::new(columns);
    let group_exprs = resolve_group_by_exprs(&stmt.group_by, &stmt.columns, &col_map)?;
    if group_exprs
        .iter()
        .any(|expr| super::window::has_window_function(expr))
    {
        return Err(SqlError::Unsupported("window functions in GROUP BY".into()));
    }
    let mut grouped = Vec::new();
    let groups = group_rows(rows, &group_exprs, &col_map, cancel)?;
    for (group_idx, group) in groups.iter().enumerate() {
        check_cancel_at(cancel, group_idx)?;
        if let Some(having) = &stmt.having {
            if !having_keeps(having, stmt, columns, &col_map, group, cancel)? {
                continue;
            }
        }
        let mut row = match group.first() {
            Some(first) => (*first).clone(),
            None => vec![Value::Null; columns.len()],
        };
        for call in &calls {
            row.push(eval_aggregate_expr_with_cancel(
                call, &col_map, group, cancel,
            )?);
        }
        grouped.push(row);
    }
    let mut extended = columns.to_vec();
    for (index, call) in calls.iter().enumerate() {
        let collation = expr_collation(call, &col_map);
        extended.push(projected_column(
            aggregate_column(index),
            extended.len(),
            collation,
        ));
    }
    select.where_clause = None;
    select.group_by.clear();
    select.having = None;
    Ok(Some(GroupedWindowInput {
        columns: extended,
        rows: grouped,
        stmt: select,
    }))
}

fn aggregate_column(index: usize) -> String {
    format!("__aggregate_{index}")
}

/// Replaces each aggregate call in `expr`, including those a window function
/// reads, with the column holding its value, and appends the call to `calls`.
/// A subquery's calls aggregate the subquery's rows and stay.
fn lift_aggregates(expr: &mut Expr, calls: &mut Vec<Expr>) {
    let aggregate = match expr {
        Expr::CountStar => true,
        Expr::Function { name, args, .. } => is_aggregate_function(name, args.len()),
        _ => false,
    };
    if aggregate {
        let column = Expr::Column(aggregate_column(calls.len()));
        calls.push(std::mem::replace(expr, column));
        return;
    }
    let mut lift = |expr: &mut Expr| lift_aggregates(expr, calls);
    match expr {
        Expr::Function { args, .. } | Expr::Coalesce(args) | Expr::ArrayLiteral(args) => {
            args.iter_mut().for_each(&mut lift);
        }
        Expr::WindowFunction { args, spec, .. } => {
            args.iter_mut().for_each(&mut lift);
            spec.partition_by.iter_mut().for_each(&mut lift);
            spec.order_by
                .iter_mut()
                .for_each(|item| lift(&mut item.expr));
        }
        Expr::BinaryOp { left, right, .. } | Expr::IsDistinctFrom { left, right, .. } => {
            lift(left);
            lift(right);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::InSet { expr, .. }
        | Expr::InSubquery { expr, .. } => lift(expr),
        Expr::InList { expr, list, .. } => {
            lift(expr);
            list.iter_mut().for_each(&mut lift);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            lift(expr);
            lift(low);
            lift(high);
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            lift(expr);
            lift(pattern);
            if let Some(escape) = escape {
                lift(escape);
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(operand) = operand {
                lift(operand);
            }
            for (condition, result) in conditions {
                lift(condition);
                lift(result);
            }
            if let Some(else_result) = else_result {
                lift(else_result);
            }
        }
        Expr::Quantified { left, right, .. } => {
            lift(left);
            if let QuantifiedRhs::Array(array) = right {
                lift(array);
            }
        }
        Expr::CountStar
        | Expr::Exists { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Literal(_)
        | Expr::BoundColumn { .. }
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. }
        | Expr::Parameter(_)
        | Expr::TypedNullRecord(_) => {}
    }
}

/// Whether HAVING keeps a group. HAVING may name an output column, as it
/// may without window functions; one that holds a window function has no
/// value yet, since windows run after HAVING.
fn having_keeps(
    having: &Expr,
    stmt: &SelectStmt,
    columns: &[ColumnDef],
    col_map: &ColumnMap,
    group: &[&Vec<Value>],
    cancel: Option<&citadel::CancelToken>,
) -> Result<bool> {
    match eval_aggregate_expr_with_cancel(having, col_map, group, cancel) {
        Ok(value) => Ok(is_truthy(&value)),
        Err(SqlError::ColumnNotFound(_)) => {
            let outputs: Vec<SelectColumn> = stmt
                .columns
                .iter()
                .filter(|column| {
                    matches!(column, SelectColumn::Expr { expr, .. }
                        if !super::window::has_window_function(expr))
                })
                .cloned()
                .collect();
            let mut values = Vec::with_capacity(outputs.len());
            for column in &outputs {
                if let SelectColumn::Expr { expr, .. } = column {
                    values.push(eval_aggregate_expr_with_cancel(
                        expr, col_map, group, cancel,
                    )?);
                }
            }
            let output_columns = build_output_columns(&outputs, columns);
            let output_map = ColumnMap::new(&output_columns);
            let ctx = EvalCtx::new(&output_map, &values).with_cancel(cancel);
            Ok(is_truthy(&eval_expr(having, &ctx)?))
        }
        Err(error) => Err(error),
    }
}

/// Each group's rows, in group key order. Without GROUP BY every row is in
/// one group, which exists even when there are no rows.
fn group_rows<'r>(
    rows: &'r [Vec<Value>],
    group_exprs: &[&Expr],
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<&'r Vec<Value>>>> {
    if group_exprs.is_empty() {
        if cancel.is_none() {
            return Ok(vec![rows.iter().collect()]);
        }
        let mut group = Vec::with_capacity(rows.len());
        for (row_idx, row) in rows.iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            group.push(row);
        }
        check_cancel(cancel)?;
        return Ok(vec![group]);
    }
    // Keyed, so a column whose collation calls two spellings equal groups them, as
    // do intervals of one length. The key decides equality by comparing, not by an
    // operator, so both have to be baked into it.
    let group_colls: Vec<Collation> = group_exprs
        .iter()
        .map(|expr| expr_collation(expr, col_map))
        .collect();
    let mut groups: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = BTreeMap::new();
    for (row_idx, row) in rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let ctx = EvalCtx::new(col_map, row).with_cancel(cancel);
        let group_key: Vec<Value> = group_exprs
            .iter()
            .zip(&group_colls)
            .map(|(expr, coll)| eval_expr(expr, &ctx).map(|v| coll.group_key(v)))
            .collect::<Result<_>>()?;
        groups.entry(group_key).or_default().push(row);
    }
    Ok(groups.into_values().collect())
}

/// Resolves GROUP BY ordinals (1-based) and output aliases to their expressions.
fn resolve_group_by_exprs<'a>(
    group_by: &'a [Expr],
    select_cols: &'a [SelectColumn],
    col_map: &ColumnMap,
) -> Result<Vec<&'a Expr>> {
    group_by
        .iter()
        .map(|expr| match expr {
            Expr::Literal(Value::Integer(n)) => {
                let idx = usize::try_from(*n)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .filter(|&i| i < select_cols.len())
                    .ok_or_else(|| {
                        SqlError::InvalidValue(format!("GROUP BY position {n} out of range"))
                    })?;
                match &select_cols[idx] {
                    SelectColumn::Expr { expr, .. } => Ok(expr),
                    _ => Err(SqlError::Unsupported(
                        "GROUP BY position references SELECT *".into(),
                    )),
                }
            }
            // Bare identifier: base column wins (PG precedence), else a matching alias.
            Expr::Column(name) if col_map.resolve(name).is_err() => {
                let aliased = select_cols.iter().find_map(|sc| match sc {
                    SelectColumn::Expr {
                        expr,
                        alias: Some(a),
                    } if a.eq_ignore_ascii_case(name) => Some(expr),
                    _ => None,
                });
                Ok(aliased.unwrap_or(expr))
            }
            _ => Ok(expr),
        })
        .collect()
}

#[cfg(test)]
pub(super) fn eval_aggregate_expr(
    expr: &Expr,
    col_map: &ColumnMap,
    group_rows: &[&Vec<Value>],
) -> Result<Value> {
    eval_aggregate_expr_with_cancel(expr, col_map, group_rows, None)
}

fn first_non_null_is_interval(
    values: &[Value],
    cancel: Option<&citadel::CancelToken>,
) -> Result<bool> {
    for (value_idx, value) in values.iter().enumerate() {
        check_cancel_at(cancel, value_idx)?;
        if !value.is_null() {
            return Ok(matches!(value, Value::Interval { .. }));
        }
    }
    Ok(false)
}

/// One group's value of `expr`. Each aggregate call reduces the group's rows,
/// and the expression around the calls reads the group's first row, so columns
/// keep their collations and every expression form evaluates as it does per row.
fn eval_aggregate_expr_with_cancel(
    expr: &Expr,
    col_map: &ColumnMap,
    group_rows: &[&Vec<Value>],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    check_cancel(cancel)?;
    let reduced;
    let expr = if is_aggregate_expr(expr) {
        reduced = reduce_aggregates(expr, col_map, group_rows, cancel)?;
        &reduced
    } else {
        expr
    };
    let nulls;
    let row: &[Value] = match group_rows.first() {
        Some(row) => row,
        None => {
            nulls = vec![Value::Null; col_map.len()];
            &nulls
        }
    };
    eval_expr(expr, &EvalCtx::new(col_map, row).with_cancel(cancel))
}

/// A copy of `expr` with each aggregate call replaced by its value over the
/// group. A subquery aggregates its own rows, so it is kept as it is.
fn reduce_aggregates(
    expr: &Expr,
    col_map: &ColumnMap,
    group_rows: &[&Vec<Value>],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Expr> {
    let reduce = |expr: &Expr| reduce_aggregates(expr, col_map, group_rows, cancel);
    let boxed = |expr: &Expr| reduce(expr).map(Box::new);
    let all = |exprs: &[Expr]| exprs.iter().map(reduce).collect::<Result<Vec<_>>>();
    Ok(match expr {
        Expr::CountStar => Expr::Literal(Value::Integer(group_rows.len() as i64)),
        Expr::Function {
            name,
            args,
            distinct,
            filter,
        } if is_aggregate_function(name, args.len()) => Expr::Literal(aggregate_value(
            name,
            args,
            *distinct,
            filter.as_deref(),
            col_map,
            group_rows,
            cancel,
        )?),
        Expr::Function {
            name,
            args,
            distinct,
            filter,
        } => Expr::Function {
            name: name.clone(),
            args: all(args)?,
            distinct: *distinct,
            filter: filter.clone(),
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: boxed(left)?,
            op: *op,
            right: boxed(right)?,
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: boxed(expr)?,
        },
        Expr::IsNull(expr) => Expr::IsNull(boxed(expr)?),
        Expr::IsNotNull(expr) => Expr::IsNotNull(boxed(expr)?),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: boxed(expr)?,
            subquery: subquery.clone(),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: boxed(expr)?,
            list: all(list)?,
            negated: *negated,
        },
        Expr::InSet {
            expr,
            values,
            families,
            has_null,
            negated,
            collation,
        } => Expr::InSet {
            expr: boxed(expr)?,
            values: values.clone(),
            families: *families,
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
            expr: boxed(expr)?,
            low: boxed(low)?,
            high: boxed(high)?,
            negated: *negated,
        },
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => Expr::IsDistinctFrom {
            left: boxed(left)?,
            right: boxed(right)?,
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Like {
            expr: boxed(expr)?,
            pattern: boxed(pattern)?,
            escape: escape.as_deref().map(boxed).transpose()?,
            negated: *negated,
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            operand: operand.as_deref().map(boxed).transpose()?,
            conditions: conditions
                .iter()
                .map(|(when, then)| Ok((reduce(when)?, reduce(then)?)))
                .collect::<Result<_>>()?,
            else_result: else_result.as_deref().map(boxed).transpose()?,
        },
        Expr::Coalesce(args) => Expr::Coalesce(all(args)?),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: boxed(expr)?,
            data_type: *data_type,
        },
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: boxed(expr)?,
            collation: *collation,
        },
        Expr::ArrayLiteral(items) => Expr::ArrayLiteral(all(items)?),
        Expr::Quantified {
            left,
            op,
            quantifier,
            right,
        } => Expr::Quantified {
            left: boxed(left)?,
            op: *op,
            quantifier: *quantifier,
            right: match right {
                QuantifiedRhs::Array(array) => QuantifiedRhs::Array(boxed(array)?),
                QuantifiedRhs::Subquery(_) => right.clone(),
            },
        },
        Expr::Literal(_)
        | Expr::BoundColumn { .. }
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. }
        | Expr::Exists { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Parameter(_)
        | Expr::WindowFunction { .. }
        | Expr::TypedNullRecord(_) => expr.clone(),
    })
}

/// The group's rows a FILTER condition holds for; NULL counts as false.
fn filter_rows<'r>(
    filter: &Expr,
    col_map: &ColumnMap,
    group_rows: &[&'r Vec<Value>],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<&'r Vec<Value>>> {
    let mut passing = Vec::with_capacity(group_rows.len());
    for (row_idx, row) in group_rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let ctx = EvalCtx::new(col_map, row).with_cancel(cancel);
        if is_truthy(&eval_expr(filter, &ctx)?) {
            passing.push(*row);
        }
    }
    Ok(passing)
}

/// The value of aggregate `name` over the group's rows, or over those its
/// FILTER holds for.
fn aggregate_value(
    name: &str,
    args: &[Expr],
    distinct: bool,
    filter: Option<&Expr>,
    col_map: &ColumnMap,
    group_rows: &[&Vec<Value>],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    let passing;
    let group_rows = match filter {
        Some(filter) => {
            passing = filter_rows(filter, col_map, group_rows, cancel)?;
            passing.as_slice()
        }
        None => group_rows,
    };
    let func = name.to_ascii_uppercase();
    if func == "COUNT" && args.is_empty() {
        return Ok(Value::Integer(group_rows.len() as i64));
    }
    if matches!(func.as_str(), "JSON_OBJECT_AGG" | "JSONB_OBJECT_AGG") {
        if args.len() != 2 {
            return Err(SqlError::Unsupported(format!(
                "{func} requires 2 arguments"
            )));
        }
        if distinct {
            return Err(SqlError::Unsupported(format!(
                "DISTINCT not supported with {func}"
            )));
        }
        // Like every aggregate but COUNT, NULL over no rows.
        if group_rows.is_empty() {
            return Ok(Value::Null);
        }
        let mut pairs: Vec<(Value, Value)> = Vec::with_capacity(group_rows.len());
        for (row_idx, row) in group_rows.iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            let ctx = EvalCtx::new(col_map, row).with_cancel(cancel);
            let k = eval_expr(&args[0], &ctx)?;
            let v = eval_expr(&args[1], &ctx)?;
            pairs.push((k, v));
        }
        let target = if func == "JSONB_OBJECT_AGG" {
            crate::types::DataType::Jsonb
        } else {
            crate::types::DataType::Json
        };
        let result = crate::json::agg_object_with_cancel(&pairs, target, cancel)?;
        check_cancel(cancel)?;
        return Ok(result);
    }
    if args.len() != 1 {
        return Err(SqlError::Unsupported(format!(
            "{func} with {} args",
            args.len()
        )));
    }
    let arg = &args[0];
    let mut values: Vec<Value> = Vec::with_capacity(group_rows.len());
    for (row_idx, row) in group_rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        values.push(eval_expr(
            arg,
            &EvalCtx::new(col_map, row).with_cancel(cancel),
        )?);
    }
    if distinct {
        // `COUNT(DISTINCT s)` counts the values `s = s` calls equal, so the key is the
        // argument's group key, as for GROUP BY. NULL stays once: the other aggregates
        // skip it, and JSON_AGG keeps it as a value.
        let coll = expr_collation(arg, col_map);
        let mut seen: rustc_hash::FxHashSet<Value> = rustc_hash::FxHashSet::default();
        let mut distinct_values = Vec::with_capacity(values.len());
        for (value_idx, value) in values.into_iter().enumerate() {
            check_cancel_at(cancel, value_idx)?;
            if seen.insert(coll.group_key(value.clone())) {
                distinct_values.push(value);
            }
        }
        values = distinct_values;
    }

    match func.as_str() {
        "COUNT" => {
            let mut count = 0;
            for (value_idx, value) in values.iter().enumerate() {
                check_cancel_at(cancel, value_idx)?;
                count += usize::from(!value.is_null());
            }
            Ok(Value::Integer(count as i64))
        }
        "SUM" => {
            // INTERVAL sum: field by field, an overflow an error (PG semantic).
            let is_interval = first_non_null_is_interval(&values, cancel)?;
            if is_interval {
                let mut months: i32 = 0;
                let mut days: i32 = 0;
                let mut micros: i64 = 0;
                let mut all_null = true;
                for (value_idx, v) in values.iter().enumerate() {
                    check_cancel_at(cancel, value_idx)?;
                    match v {
                        Value::Null => {}
                        Value::Interval {
                            months: m,
                            days: d,
                            micros: u,
                        } => {
                            (months, days, micros) = crate::datetime::add_intervals(
                                (months, days, micros),
                                (*m, *d, *u),
                            )?;
                            all_null = false;
                        }
                        _ => {
                            return Err(SqlError::TypeMismatch {
                                expected: "INTERVAL".into(),
                                got: v.data_type().to_string(),
                            })
                        }
                    }
                }
                return if all_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Interval {
                        months,
                        days,
                        micros,
                    })
                };
            }
            // Exact, so only a total outside i64 overflows, whatever the order.
            let mut int_sum: i128 = 0;
            let mut real_sum: f64 = 0.0;
            let mut has_real = false;
            let mut all_null = true;
            for (value_idx, v) in values.iter().enumerate() {
                check_cancel_at(cancel, value_idx)?;
                match v {
                    Value::Integer(i) => {
                        int_sum += i128::from(*i);
                        all_null = false;
                    }
                    Value::Real(r) => {
                        real_sum += r;
                        has_real = true;
                        all_null = false;
                    }
                    Value::Null => {}
                    _ => {
                        return Err(SqlError::TypeMismatch {
                            expected: "numeric".into(),
                            got: v.data_type().to_string(),
                        })
                    }
                }
            }
            if all_null {
                return Ok(Value::Null);
            }
            if has_real {
                Ok(Value::Real(real_sum + int_sum as f64))
            } else {
                i64::try_from(int_sum)
                    .map(Value::Integer)
                    .map_err(|_| SqlError::IntegerOverflow)
            }
        }
        "AVG" => {
            // INTERVAL avg: the sum divided by the count, as interval division.
            let is_interval = first_non_null_is_interval(&values, cancel)?;
            if is_interval {
                let mut months: i64 = 0;
                let mut days: i64 = 0;
                let mut micros: i128 = 0;
                let mut count: i64 = 0;
                for (value_idx, v) in values.iter().enumerate() {
                    check_cancel_at(cancel, value_idx)?;
                    match v {
                        Value::Null => {}
                        Value::Interval {
                            months: m,
                            days: d,
                            micros: u,
                        } => {
                            months += *m as i64;
                            days += *d as i64;
                            micros += *u as i128;
                            count += 1;
                        }
                        _ => {
                            return Err(SqlError::TypeMismatch {
                                expected: "INTERVAL".into(),
                                got: v.data_type().to_string(),
                            })
                        }
                    }
                }
                if count == 0 {
                    return Ok(Value::Null);
                }
                let (months, days, micros) =
                    crate::datetime::average_interval(months, days, micros, count)?;
                return Ok(Value::Interval {
                    months,
                    days,
                    micros,
                });
            }
            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;
            for (value_idx, v) in values.iter().enumerate() {
                check_cancel_at(cancel, value_idx)?;
                match v {
                    Value::Integer(i) => {
                        sum += *i as f64;
                        count += 1;
                    }
                    Value::Real(r) => {
                        sum += r;
                        count += 1;
                    }
                    Value::Null => {}
                    _ => {
                        return Err(SqlError::TypeMismatch {
                            expected: "numeric".into(),
                            got: v.data_type().to_string(),
                        })
                    }
                }
            }
            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Real(sum / count as f64))
            }
        }
        "MIN" => {
            let collation = operand_collation(arg, col_map).unwrap_or_default();
            let mut min: Option<&Value> = None;
            for (value_idx, v) in values.iter().enumerate() {
                check_cancel_at(cancel, value_idx)?;
                if v.is_null() {
                    continue;
                }
                min = Some(match min {
                    None => v,
                    Some(m) => {
                        if collation.cmp_value(v, m).is_lt() {
                            v
                        } else {
                            m
                        }
                    }
                });
            }
            Ok(min.cloned().unwrap_or(Value::Null))
        }
        "MAX" => {
            let collation = operand_collation(arg, col_map).unwrap_or_default();
            let mut max: Option<&Value> = None;
            for (value_idx, v) in values.iter().enumerate() {
                check_cancel_at(cancel, value_idx)?;
                if v.is_null() {
                    continue;
                }
                max = Some(match max {
                    None => v,
                    Some(m) => {
                        if collation.cmp_value(v, m).is_gt() {
                            v
                        } else {
                            m
                        }
                    }
                });
            }
            Ok(max.cloned().unwrap_or(Value::Null))
        }
        "JSON_AGG" | "JSONB_AGG" => {
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let target = if func.eq_ignore_ascii_case("JSONB_AGG") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            let result = crate::json::agg_array_with_cancel(&values, target, cancel)?;
            check_cancel(cancel)?;
            Ok(result)
        }
        _ => Err(SqlError::Unsupported(format!("aggregate function: {func}"))),
    }
}

#[cfg(test)]
#[path = "aggregate_tests.rs"]
mod tests;
