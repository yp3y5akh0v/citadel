use std::collections::BTreeMap;

use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap};
use crate::parser::*;
use crate::types::*;

use super::helpers::*;

// ── Aggregation ─────────────────────────────────────────────────────

pub(super) fn exec_aggregate(
    columns: &[ColumnDef],
    rows: &[Vec<Value>],
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let col_map = ColumnMap::new(columns);
    let groups: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = if stmt.group_by.is_empty() {
        let mut m = BTreeMap::new();
        m.insert(vec![], rows.iter().collect());
        m
    } else {
        let mut m: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = BTreeMap::new();
        for row in rows {
            let group_key: Vec<Value> = stmt
                .group_by
                .iter()
                .map(|expr| eval_expr(expr, &col_map, row))
                .collect::<Result<_>>()?;
            m.entry(group_key).or_default().push(row);
        }
        m
    };

    let mut result_rows = Vec::new();
    let output_cols = build_output_columns(&stmt.columns, columns);

    for group_rows in groups.values() {
        let mut result_row = Vec::new();

        for sel_col in &stmt.columns {
            match sel_col {
                SelectColumn::AllColumns => {
                    return Err(SqlError::Unsupported("SELECT * with GROUP BY".into()));
                }
                SelectColumn::Expr { expr, .. } => {
                    let val = eval_aggregate_expr(expr, &col_map, group_rows)?;
                    result_row.push(val);
                }
            }
        }

        if let Some(ref having) = stmt.having {
            let passes = match eval_aggregate_expr(having, &col_map, group_rows) {
                Ok(val) => is_truthy(&val),
                Err(SqlError::ColumnNotFound(_)) => {
                    let output_map = ColumnMap::new(&output_cols);
                    match eval_expr(having, &output_map, &result_row) {
                        Ok(val) => is_truthy(&val),
                        Err(_) => false,
                    }
                }
                Err(e) => return Err(e),
            };
            if !passes {
                continue;
            }
        }

        result_rows.push(result_row);
    }

    if stmt.distinct {
        let mut seen = std::collections::HashSet::new();
        result_rows.retain(|row| seen.insert(row.clone()));
    }

    if !stmt.order_by.is_empty() {
        let output_cols = build_output_columns(&stmt.columns, columns);
        sort_rows(&mut result_rows, &stmt.order_by, &output_cols)?;
    }

    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_const_int(offset_expr)?.max(0) as usize;
        if offset < result_rows.len() {
            result_rows = result_rows.split_off(offset);
        } else {
            result_rows.clear();
        }
    }
    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_const_int(limit_expr)?.max(0) as usize;
        result_rows.truncate(limit);
    }

    let col_names = stmt
        .columns
        .iter()
        .map(|c| match c {
            SelectColumn::AllColumns => "*".into(),
            SelectColumn::Expr { alias: Some(a), .. } => a.clone(),
            SelectColumn::Expr { expr, .. } => expr_display_name(expr),
        })
        .collect();

    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: result_rows,
    }))
}

pub(super) fn eval_aggregate_expr(
    expr: &Expr,
    col_map: &ColumnMap,
    group_rows: &[&Vec<Value>],
) -> Result<Value> {
    match expr {
        Expr::CountStar => Ok(Value::Integer(group_rows.len() as i64)),

        Expr::Function { name, args } if is_aggregate_function(name, args.len()) => {
            let func = name.to_ascii_uppercase();
            if args.len() != 1 {
                return Err(SqlError::Unsupported(format!(
                    "{func} with {} args",
                    args.len()
                )));
            }
            let arg = &args[0];
            let values: Vec<Value> = group_rows
                .iter()
                .map(|row| eval_expr(arg, col_map, row))
                .collect::<Result<_>>()?;

            match func.as_str() {
                "COUNT" => {
                    let count = values.iter().filter(|v| !v.is_null()).count();
                    Ok(Value::Integer(count as i64))
                }
                "SUM" => {
                    let mut int_sum: i64 = 0;
                    let mut real_sum: f64 = 0.0;
                    let mut has_real = false;
                    let mut all_null = true;
                    for v in &values {
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
                    let mut sum: f64 = 0.0;
                    let mut count: i64 = 0;
                    for v in &values {
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
                    let mut min: Option<&Value> = None;
                    for v in &values {
                        if v.is_null() {
                            continue;
                        }
                        min = Some(match min {
                            None => v,
                            Some(m) => {
                                if v < m {
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
                    let mut max: Option<&Value> = None;
                    for v in &values {
                        if v.is_null() {
                            continue;
                        }
                        max = Some(match max {
                            None => v,
                            Some(m) => {
                                if v > m {
                                    v
                                } else {
                                    m
                                }
                            }
                        });
                    }
                    Ok(max.cloned().unwrap_or(Value::Null))
                }
                _ => Err(SqlError::Unsupported(format!("aggregate function: {func}"))),
            }
        }

        Expr::Column(_) | Expr::QualifiedColumn { .. } => {
            if let Some(first) = group_rows.first() {
                eval_expr(expr, col_map, first)
            } else {
                Ok(Value::Null)
            }
        }

        Expr::Literal(v) => Ok(v.clone()),

        Expr::BinaryOp { left, op, right } => {
            let l = eval_aggregate_expr(left, col_map, group_rows)?;
            let r = eval_aggregate_expr(right, col_map, group_rows)?;
            eval_expr(
                &Expr::BinaryOp {
                    left: Box::new(Expr::Literal(l)),
                    op: *op,
                    right: Box::new(Expr::Literal(r)),
                },
                col_map,
                &[],
            )
        }

        Expr::UnaryOp { op, expr: e } => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            eval_expr(
                &Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(Expr::Literal(v)),
                },
                col_map,
                &[],
            )
        }

        Expr::IsNull(e) => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            Ok(Value::Boolean(v.is_null()))
        }

        Expr::IsNotNull(e) => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            Ok(Value::Boolean(!v.is_null()))
        }

        Expr::Cast { expr: e, data_type } => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            eval_expr(
                &Expr::Cast {
                    expr: Box::new(Expr::Literal(v)),
                    data_type: *data_type,
                },
                col_map,
                &[],
            )
        }

        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let op_val = operand
                .as_ref()
                .map(|e| eval_aggregate_expr(e, col_map, group_rows))
                .transpose()?;
            if let Some(ov) = &op_val {
                for (cond, result) in conditions {
                    let cv = eval_aggregate_expr(cond, col_map, group_rows)?;
                    if !ov.is_null() && !cv.is_null() && *ov == cv {
                        return eval_aggregate_expr(result, col_map, group_rows);
                    }
                }
            } else {
                for (cond, result) in conditions {
                    let cv = eval_aggregate_expr(cond, col_map, group_rows)?;
                    if is_truthy(&cv) {
                        return eval_aggregate_expr(result, col_map, group_rows);
                    }
                }
            }
            match else_result {
                Some(e) => eval_aggregate_expr(e, col_map, group_rows),
                None => Ok(Value::Null),
            }
        }

        Expr::Coalesce(args) => {
            for arg in args {
                let v = eval_aggregate_expr(arg, col_map, group_rows)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }

        Expr::Between {
            expr: e,
            low,
            high,
            negated,
        } => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            let lo = eval_aggregate_expr(low, col_map, group_rows)?;
            let hi = eval_aggregate_expr(high, col_map, group_rows)?;
            eval_expr(
                &Expr::Between {
                    expr: Box::new(Expr::Literal(v)),
                    low: Box::new(Expr::Literal(lo)),
                    high: Box::new(Expr::Literal(hi)),
                    negated: *negated,
                },
                col_map,
                &[],
            )
        }

        Expr::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => {
            let v = eval_aggregate_expr(e, col_map, group_rows)?;
            let p = eval_aggregate_expr(pattern, col_map, group_rows)?;
            let esc = escape
                .as_ref()
                .map(|es| eval_aggregate_expr(es, col_map, group_rows))
                .transpose()?;
            let esc_box = esc.map(|v| Box::new(Expr::Literal(v)));
            eval_expr(
                &Expr::Like {
                    expr: Box::new(Expr::Literal(v)),
                    pattern: Box::new(Expr::Literal(p)),
                    escape: esc_box,
                    negated: *negated,
                },
                col_map,
                &[],
            )
        }

        Expr::Function { name, args } => {
            let evaluated: Vec<Value> = args
                .iter()
                .map(|a| eval_aggregate_expr(a, col_map, group_rows))
                .collect::<Result<_>>()?;
            let literal_args: Vec<Expr> = evaluated.into_iter().map(Expr::Literal).collect();
            eval_expr(
                &Expr::Function {
                    name: name.clone(),
                    args: literal_args,
                },
                col_map,
                &[],
            )
        }

        _ => Err(SqlError::Unsupported(format!(
            "expression in aggregate: {expr:?}"
        ))),
    }
}

pub(super) fn is_aggregate_function(name: &str, arg_count: usize) -> bool {
    let u = name.to_ascii_uppercase();
    matches!(u.as_str(), "COUNT" | "SUM" | "AVG")
        || (matches!(u.as_str(), "MIN" | "MAX") && arg_count == 1)
}

pub(super) fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::CountStar => true,
        Expr::Function { name, args } => {
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
