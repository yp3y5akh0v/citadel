use std::collections::BTreeMap;

use crate::error::{Result, SqlError};
use crate::eval::{
    collated_eq, compile_collation, eval_expr, is_truthy, operand_collation, ColumnMap, EvalCtx,
};
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
    let groups: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = if group_exprs.is_empty() {
        let mut m = BTreeMap::new();
        let group_rows = if cancel.is_none() {
            rows.iter().collect()
        } else {
            let mut group_rows = Vec::with_capacity(rows.len());
            for (row_idx, row) in rows.iter().enumerate() {
                check_cancel_at(cancel, row_idx)?;
                group_rows.push(row);
            }
            check_cancel(cancel)?;
            group_rows
        };
        m.insert(vec![], group_rows);
        m
    } else {
        // Folded, so a column whose collation calls two spellings equal groups them.
        // The key decides equality by comparing, not by an operator, so the
        // collation has to be baked into it.
        let group_colls: Vec<crate::types::Collation> = group_exprs
            .iter()
            .map(|expr| expr_collation(expr, &col_map))
            .collect();
        let mut m: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = BTreeMap::new();
        for (row_idx, row) in rows.iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            let ctx = EvalCtx::new(&col_map, row).with_cancel(cancel);
            let group_key: Vec<Value> = group_exprs
                .iter()
                .zip(&group_colls)
                .map(|(expr, coll)| eval_expr(expr, &ctx).map(|v| coll.fold(v)))
                .collect::<Result<_>>()?;
            m.entry(group_key).or_default().push(row);
        }
        m
    };

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

    for (group_idx, group_rows) in groups.values().enumerate() {
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

fn eval_aggregate_expr_with_cancel(
    expr: &Expr,
    col_map: &ColumnMap,
    group_rows: &[&Vec<Value>],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    check_cancel(cancel)?;
    match expr {
        Expr::CountStar => Ok(Value::Integer(group_rows.len() as i64)),

        Expr::Function {
            name,
            args,
            distinct,
        } if is_aggregate_function(name, args.len()) => {
            let func = name.to_ascii_uppercase();
            if matches!(func.as_str(), "JSON_OBJECT_AGG" | "JSONB_OBJECT_AGG") {
                if args.len() != 2 {
                    return Err(SqlError::Unsupported(format!(
                        "{func} requires 2 arguments"
                    )));
                }
                if *distinct {
                    return Err(SqlError::Unsupported(format!(
                        "DISTINCT not supported with {func}"
                    )));
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
            if *distinct {
                // `COUNT(DISTINCT s)` counts the values `s = s` calls equal, so the argument's
                // collation folds the key here as it does for GROUP BY.
                let coll = expr_collation(arg, col_map);
                let mut seen: rustc_hash::FxHashSet<Value> = rustc_hash::FxHashSet::default();
                let mut distinct_values = Vec::with_capacity(values.len());
                for (value_idx, value) in values.into_iter().enumerate() {
                    check_cancel_at(cancel, value_idx)?;
                    if !value.is_null() && seen.insert(coll.fold(value.clone())) {
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
                    // INTERVAL sum: field-wise saturating add (PG semantic).
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
                                    months = months.saturating_add(*m);
                                    days = days.saturating_add(*d);
                                    micros = micros.saturating_add(*u);
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
                    let mut int_sum: i64 = 0;
                    let mut real_sum: f64 = 0.0;
                    let mut has_real = false;
                    let mut all_null = true;
                    for (value_idx, v) in values.iter().enumerate() {
                        check_cancel_at(cancel, value_idx)?;
                        match v {
                            Value::Integer(i) => {
                                int_sum += i;
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
                        Ok(Value::Integer(int_sum))
                    }
                }
                "AVG" => {
                    // INTERVAL avg: field-wise sum / count.
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
                        return if count == 0 {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Interval {
                                months: (months / count).clamp(i32::MIN as i64, i32::MAX as i64)
                                    as i32,
                                days: (days / count).clamp(i32::MIN as i64, i32::MAX as i64) as i32,
                                micros: (micros / count as i128) as i64,
                            })
                        };
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

        Expr::Column(_) | Expr::QualifiedColumn { .. } => {
            if let Some(first) = group_rows.first() {
                eval_expr(expr, &EvalCtx::new(col_map, first).with_cancel(cancel))
            } else {
                Ok(Value::Null)
            }
        }

        Expr::Literal(v) | Expr::BoundColumn { value: v, .. } => Ok(v.clone()),

        Expr::BinaryOp { left, op, right } => {
            let l = eval_aggregate_expr_with_cancel(left, col_map, group_rows, cancel)?;
            let r = eval_aggregate_expr_with_cancel(right, col_map, group_rows, cancel)?;
            eval_expr(
                &Expr::BinaryOp {
                    left: Box::new(Expr::Literal(l)),
                    op: *op,
                    right: Box::new(Expr::Literal(r)),
                },
                &EvalCtx::new(col_map, &[]).with_cancel(cancel),
            )
        }

        Expr::UnaryOp { op, expr: e } => {
            let v = eval_aggregate_expr_with_cancel(e, col_map, group_rows, cancel)?;
            eval_expr(
                &Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(Expr::Literal(v)),
                },
                &EvalCtx::new(col_map, &[]).with_cancel(cancel),
            )
        }

        Expr::IsNull(e) => {
            let v = eval_aggregate_expr_with_cancel(e, col_map, group_rows, cancel)?;
            Ok(Value::Boolean(v.is_null()))
        }

        Expr::IsNotNull(e) => {
            let v = eval_aggregate_expr_with_cancel(e, col_map, group_rows, cancel)?;
            Ok(Value::Boolean(!v.is_null()))
        }

        Expr::Cast { expr: e, data_type } => {
            let v = eval_aggregate_expr_with_cancel(e, col_map, group_rows, cancel)?;
            eval_expr(
                &Expr::Cast {
                    expr: Box::new(Expr::Literal(v)),
                    data_type: *data_type,
                },
                &EvalCtx::new(col_map, &[]).with_cancel(cancel),
            )
        }

        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let op_val = operand
                .as_ref()
                .map(|e| eval_aggregate_expr_with_cancel(e, col_map, group_rows, cancel))
                .transpose()?;
            if let Some(ov) = &op_val {
                for (cond, result) in conditions {
                    let cv = eval_aggregate_expr_with_cancel(cond, col_map, group_rows, cancel)?;
                    if !ov.is_null() && !cv.is_null() && *ov == cv {
                        return eval_aggregate_expr_with_cancel(
                            result, col_map, group_rows, cancel,
                        );
                    }
                }
            } else {
                for (cond, result) in conditions {
                    let cv = eval_aggregate_expr_with_cancel(cond, col_map, group_rows, cancel)?;
                    if is_truthy(&cv) {
                        return eval_aggregate_expr_with_cancel(
                            result, col_map, group_rows, cancel,
                        );
                    }
                }
            }
            match else_result {
                Some(e) => eval_aggregate_expr_with_cancel(e, col_map, group_rows, cancel),
                None => Ok(Value::Null),
            }
        }

        Expr::Coalesce(args) => {
            for arg in args {
                let v = eval_aggregate_expr_with_cancel(arg, col_map, group_rows, cancel)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }

        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let l = eval_aggregate_expr_with_cancel(left, col_map, group_rows, cancel)?;
            let r = eval_aggregate_expr_with_cancel(right, col_map, group_rows, cancel)?;
            let alike = match (l.is_null(), r.is_null()) {
                (true, true) => true,
                (true, false) | (false, true) => false,
                (false, false) => collated_eq(&l, &r, compile_collation(left, right, col_map))?,
            };
            Ok(Value::Boolean(if *negated { alike } else { !alike }))
        }
        Expr::Between {
            expr: e,
            low,
            high,
            negated,
        } => {
            let v = eval_aggregate_expr_with_cancel(e, col_map, group_rows, cancel)?;
            let lo = eval_aggregate_expr_with_cancel(low, col_map, group_rows, cancel)?;
            let hi = eval_aggregate_expr_with_cancel(high, col_map, group_rows, cancel)?;
            eval_expr(
                &Expr::Between {
                    expr: Box::new(Expr::Literal(v)),
                    low: Box::new(Expr::Literal(lo)),
                    high: Box::new(Expr::Literal(hi)),
                    negated: *negated,
                },
                &EvalCtx::new(col_map, &[]).with_cancel(cancel),
            )
        }

        Expr::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => {
            let v = eval_aggregate_expr_with_cancel(e, col_map, group_rows, cancel)?;
            let p = eval_aggregate_expr_with_cancel(pattern, col_map, group_rows, cancel)?;
            let esc = escape
                .as_ref()
                .map(|es| eval_aggregate_expr_with_cancel(es, col_map, group_rows, cancel))
                .transpose()?;
            let esc_box = esc.map(|v| Box::new(Expr::Literal(v)));
            eval_expr(
                &Expr::Like {
                    expr: Box::new(Expr::Literal(v)),
                    pattern: Box::new(Expr::Literal(p)),
                    escape: esc_box,
                    negated: *negated,
                },
                &EvalCtx::new(col_map, &[]).with_cancel(cancel),
            )
        }

        Expr::Function { name, args, .. } => {
            let evaluated: Vec<Value> = args
                .iter()
                .map(|a| eval_aggregate_expr_with_cancel(a, col_map, group_rows, cancel))
                .collect::<Result<_>>()?;
            let literal_args: Vec<Expr> = evaluated.into_iter().map(Expr::Literal).collect();
            eval_expr(
                &Expr::Function {
                    name: name.clone(),
                    args: literal_args,
                    distinct: false,
                },
                &EvalCtx::new(col_map, &[]).with_cancel(cancel),
            )
        }

        Expr::Parameter(_) => eval_expr(expr, &EvalCtx::new(col_map, &[]).with_cancel(cancel)),

        _ => Err(SqlError::Unsupported(format!(
            "expression in aggregate: {expr:?}"
        ))),
    }
}

pub(super) fn is_aggregate_function(name: &str, arg_count: usize) -> bool {
    let u = name.to_ascii_uppercase();
    matches!(
        u.as_str(),
        "COUNT" | "SUM" | "AVG" | "JSON_AGG" | "JSONB_AGG"
    ) || (matches!(u.as_str(), "MIN" | "MAX") && arg_count == 1)
        || (matches!(u.as_str(), "JSON_OBJECT_AGG" | "JSONB_OBJECT_AGG") && arg_count == 2)
}

pub(super) fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::CountStar => true,
        Expr::Function { name, args, .. } => {
            is_aggregate_function(name, args.len()) || args.iter().any(is_aggregate_expr)
        }
        Expr::BinaryOp { left, right, .. } => is_aggregate_expr(left) || is_aggregate_expr(right),
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => is_aggregate_expr(expr),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| is_aggregate_expr(e))
                || conditions
                    .iter()
                    .any(|(c, r)| is_aggregate_expr(c) || is_aggregate_expr(r))
                || else_result.as_ref().is_some_and(|e| is_aggregate_expr(e))
        }
        Expr::Coalesce(args) => args.iter().any(is_aggregate_expr),
        Expr::Between {
            expr, low, high, ..
        } => is_aggregate_expr(expr) || is_aggregate_expr(low) || is_aggregate_expr(high),
        Expr::IsDistinctFrom { left, right, .. } => {
            is_aggregate_expr(left) || is_aggregate_expr(right)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            is_aggregate_expr(expr)
                || is_aggregate_expr(pattern)
                || escape.as_ref().is_some_and(|e| is_aggregate_expr(e))
        }
        Expr::WindowFunction { .. } => false,
        _ => false,
    }
}

#[cfg(test)]
#[path = "aggregate_tests.rs"]
mod tests;
