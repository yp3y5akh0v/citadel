//! Query planner: chooses between seq scan, PK lookup, or index scan.

use crate::encoding::encode_composite_key;
use crate::parser::{BinOp, Expr};
use crate::types::{IndexDef, TableSchema, Value};

#[derive(Debug, Clone)]
pub enum ScanPlan {
    SeqScan,
    PkLookup {
        pk_values: Vec<Value>,
    },
    PkRangeScan {
        start_key: Vec<u8>,
        range_conds: Vec<(BinOp, Value)>,
        num_pk_cols: usize,
    },
    IndexScan {
        index_name: String,
        idx_table: Vec<u8>,
        prefix: Vec<u8>,
        num_prefix_cols: usize,
        range_conds: Vec<(BinOp, Value)>,
        is_unique: bool,
        index_columns: Vec<u16>,
    },
}

struct SimplePredicate {
    col_idx: usize,
    op: BinOp,
    value: Value,
}

fn flatten_and(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut v = flatten_and(left);
            v.extend(flatten_and(right));
            v
        }
        _ => vec![expr],
    }
}

fn is_comparison(op: BinOp) -> bool {
    matches!(
        op,
        BinOp::Eq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq
    )
}

fn is_range_op(op: BinOp) -> bool {
    matches!(op, BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq)
}

fn flip_op(op: BinOp) -> BinOp {
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::LtEq => BinOp::GtEq,
        BinOp::Gt => BinOp::Lt,
        BinOp::GtEq => BinOp::LtEq,
        other => other,
    }
}

fn resolve_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(name) => Some(name.as_str()),
        Expr::QualifiedColumn { column, .. } => Some(column.as_str()),
        _ => None,
    }
}

fn resolve_literal(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(v) => Some(v.clone()),
        Expr::Parameter(n) => crate::eval::resolve_scoped_param(*n).ok(),
        _ => None,
    }
}

fn extract_simple_predicate(expr: &Expr, schema: &TableSchema) -> Option<SimplePredicate> {
    match expr {
        Expr::BinaryOp { left, op, right } if is_comparison(*op) => {
            if let (Some(name), Some(val)) = (resolve_column_name(left), resolve_literal(right)) {
                let col_idx = schema.column_index(name)?;
                return Some(SimplePredicate {
                    col_idx,
                    op: *op,
                    value: val,
                });
            }
            if let (Some(val), Some(name)) = (resolve_literal(left), resolve_column_name(right)) {
                let col_idx = schema.column_index(name)?;
                return Some(SimplePredicate {
                    col_idx,
                    op: flip_op(*op),
                    value: val,
                });
            }
            None
        }
        _ => None,
    }
}

/// Decompose BETWEEN into two range predicates for planner use.
fn flatten_between(expr: &Expr, schema: &TableSchema, out: &mut Vec<SimplePredicate>) {
    match expr {
        Expr::Between {
            expr: col_expr,
            low,
            high,
            negated: false,
        } => {
            if let (Some(name), Some(lo), Some(hi)) = (
                resolve_column_name(col_expr),
                resolve_literal(low),
                resolve_literal(high),
            ) {
                if let Some(col_idx) = schema.column_index(name) {
                    out.push(SimplePredicate {
                        col_idx,
                        op: BinOp::GtEq,
                        value: lo,
                    });
                    out.push(SimplePredicate {
                        col_idx,
                        op: BinOp::LtEq,
                        value: hi,
                    });
                }
            }
        }
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            flatten_between(left, schema, out);
            flatten_between(right, schema, out);
        }
        _ => {}
    }
}

pub fn plan_select(schema: &TableSchema, where_clause: &Option<Expr>) -> ScanPlan {
    let where_expr = match where_clause {
        Some(e) => e,
        None => return ScanPlan::SeqScan,
    };

    let predicates = flatten_and(where_expr);
    let simple: Vec<Option<SimplePredicate>> = predicates
        .iter()
        .map(|p| extract_simple_predicate(p, schema))
        .collect();

    if let Some(plan) = try_pk_lookup(schema, &simple) {
        return plan;
    }

    let mut range_preds: Vec<SimplePredicate> = simple
        .iter()
        .filter_map(|p| {
            let p = p.as_ref()?;
            if is_range_op(p.op) {
                Some(SimplePredicate {
                    col_idx: p.col_idx,
                    op: p.op,
                    value: p.value.clone(),
                })
            } else {
                None
            }
        })
        .collect();
    flatten_between(where_expr, schema, &mut range_preds);

    if let Some(plan) = try_pk_range_scan(schema, &range_preds) {
        return plan;
    }

    if let Some(plan) = try_best_index(schema, where_expr, &simple) {
        return plan;
    }

    ScanPlan::SeqScan
}

fn try_pk_range_scan(schema: &TableSchema, range_preds: &[SimplePredicate]) -> Option<ScanPlan> {
    if schema.primary_key_columns.len() != 1 {
        return None; // Only single-column PK for now
    }
    let pk_col = schema.primary_key_columns[0] as usize;
    let conds: Vec<(BinOp, Value)> = range_preds
        .iter()
        .filter(|p| p.col_idx == pk_col)
        .map(|p| (p.op, p.value.clone()))
        .collect();
    if conds.is_empty() {
        return None;
    }
    let start_key = conds
        .iter()
        .filter(|(op, _)| matches!(op, BinOp::GtEq | BinOp::Gt))
        .map(|(_, v)| encode_composite_key(std::slice::from_ref(v)))
        .min_by(|a, b| a.cmp(b))
        .unwrap_or_default();
    Some(ScanPlan::PkRangeScan {
        start_key,
        range_conds: conds,
        num_pk_cols: 1,
    })
}

fn try_pk_lookup(schema: &TableSchema, predicates: &[Option<SimplePredicate>]) -> Option<ScanPlan> {
    let pk_cols = &schema.primary_key_columns;
    let mut pk_values: Vec<Option<Value>> = vec![None; pk_cols.len()];

    for pred in predicates.iter().flatten() {
        if pred.op == BinOp::Eq {
            if let Some(pk_pos) = pk_cols.iter().position(|&c| c == pred.col_idx as u16) {
                pk_values[pk_pos] = Some(pred.value.clone());
            }
        }
    }

    if pk_values.iter().all(|v| v.is_some()) {
        let values: Vec<Value> = pk_values.into_iter().map(|v| v.unwrap()).collect();
        Some(ScanPlan::PkLookup { pk_values: values })
    } else {
        None
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct IndexScore {
    num_equality: usize,
    has_range: bool,
    is_unique: bool,
}

fn try_best_index(
    schema: &TableSchema,
    where_expr: &Expr,
    predicates: &[Option<SimplePredicate>],
) -> Option<ScanPlan> {
    let mut best_score: Option<IndexScore> = None;
    let mut best_plan: Option<ScanPlan> = None;

    let conjuncts = flatten_and(where_expr);
    for idx in &schema.indices {
        if !partial_predicate_implied(idx, where_expr, &conjuncts) {
            continue;
        }
        if let Some((score, plan)) = try_index_scan(schema, idx, predicates) {
            if best_score.is_none() || score > *best_score.as_ref().unwrap() {
                best_score = Some(score);
                best_plan = Some(plan);
            }
        }
    }

    best_plan
}

fn partial_predicate_implied(idx: &IndexDef, where_expr: &Expr, conjuncts: &[&Expr]) -> bool {
    let Some(pred) = idx.predicate_expr.as_ref() else {
        return true;
    };
    if expr_structurally_eq(pred, where_expr) {
        return true;
    }
    if conjuncts.iter().any(|c| expr_structurally_eq(pred, c)) {
        return true;
    }
    if let Expr::IsNotNull(target) = pred {
        if let Expr::Column(col) = target.as_ref() {
            return conjuncts.iter().any(|c| conjunct_proves_not_null(c, col));
        }
    }
    false
}

fn expr_structurally_eq(a: &Expr, b: &Expr) -> bool {
    format!("{a:?}") == format!("{b:?}")
}

fn conjunct_proves_not_null(expr: &Expr, col: &str) -> bool {
    let mentions = |e: &Expr| matches!(e, Expr::Column(n) if n.eq_ignore_ascii_case(col));
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq,
            right,
        } => mentions(left) || mentions(right),
        Expr::IsNotNull(inner) => mentions(inner),
        _ => false,
    }
}

fn try_index_scan(
    schema: &TableSchema,
    idx: &IndexDef,
    predicates: &[Option<SimplePredicate>],
) -> Option<(IndexScore, ScanPlan)> {
    let mut used = Vec::new();
    let mut equality_values: Vec<Value> = Vec::new();
    let mut range_conds: Vec<(BinOp, Value)> = Vec::new();

    for &col_idx in &idx.columns {
        let mut found_eq = false;
        for (i, pred) in predicates.iter().enumerate() {
            if used.contains(&i) {
                continue;
            }
            if let Some(sp) = pred {
                if sp.col_idx == col_idx as usize && sp.op == BinOp::Eq {
                    equality_values.push(sp.value.clone());
                    used.push(i);
                    found_eq = true;
                    break;
                }
            }
        }
        if !found_eq {
            for (i, pred) in predicates.iter().enumerate() {
                if used.contains(&i) {
                    continue;
                }
                if let Some(sp) = pred {
                    if sp.col_idx == col_idx as usize && is_range_op(sp.op) {
                        range_conds.push((sp.op, sp.value.clone()));
                        used.push(i);
                    }
                }
            }
            break;
        }
    }

    if equality_values.is_empty() && range_conds.is_empty() {
        return None;
    }

    let score = IndexScore {
        num_equality: equality_values.len(),
        has_range: !range_conds.is_empty(),
        is_unique: idx.unique,
    };

    let prefix = encode_composite_key(&equality_values);
    let idx_table = TableSchema::index_table_name(&schema.name, &idx.name);

    Some((
        score,
        ScanPlan::IndexScan {
            index_name: idx.name.clone(),
            idx_table,
            prefix,
            num_prefix_cols: equality_values.len(),
            range_conds,
            is_unique: idx.unique,
            index_columns: idx.columns.clone(),
        },
    ))
}

pub fn describe_plan(plan: &ScanPlan, table_schema: &TableSchema) -> String {
    match plan {
        ScanPlan::SeqScan => String::new(),

        ScanPlan::PkLookup { pk_values } => {
            let pk_cols: Vec<&str> = table_schema
                .primary_key_columns
                .iter()
                .map(|&idx| table_schema.columns[idx as usize].name.as_str())
                .collect();
            let conditions: Vec<String> = pk_cols
                .iter()
                .zip(pk_values.iter())
                .map(|(col, val)| format!("{col} = {}", format_value(val)))
                .collect();
            format!("USING PRIMARY KEY ({})", conditions.join(", "))
        }

        ScanPlan::PkRangeScan { range_conds, .. } => {
            let pk_col = &table_schema.columns[table_schema.primary_key_columns[0] as usize].name;
            let conditions: Vec<String> = range_conds
                .iter()
                .map(|(op, val)| format!("{pk_col} {} {}", op_symbol(*op), format_value(val)))
                .collect();
            format!("USING PRIMARY KEY RANGE ({})", conditions.join(", "))
        }

        ScanPlan::IndexScan {
            index_name,
            num_prefix_cols,
            range_conds,
            index_columns,
            ..
        } => {
            let mut conditions = Vec::new();
            for &col in index_columns.iter().take(*num_prefix_cols) {
                let col_idx = col as usize;
                let col_name = &table_schema.columns[col_idx].name;
                conditions.push(format!("{col_name} = ?"));
            }
            if !range_conds.is_empty() && *num_prefix_cols < index_columns.len() {
                let col_idx = index_columns[*num_prefix_cols] as usize;
                let col_name = &table_schema.columns[col_idx].name;
                for (op, _) in range_conds {
                    conditions.push(format!("{col_name} {} ?", op_symbol(*op)));
                }
            }
            if conditions.is_empty() {
                format!("USING INDEX {index_name}")
            } else {
                format!("USING INDEX {index_name} ({})", conditions.join(", "))
            }
        }
    }
}

fn format_value(val: &Value) -> String {
    match val {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format!("{f}"),
        Value::Text(s) => format!("'{s}'"),
        Value::Blob(_) => "BLOB".into(),
        Value::Boolean(b) => b.to_string(),
        Value::Date(d) => format!("DATE '{}'", crate::datetime::format_date(*d)),
        Value::Time(t) => format!("TIME '{}'", crate::datetime::format_time(*t)),
        Value::Timestamp(t) => format!("TIMESTAMP '{}'", crate::datetime::format_timestamp(*t)),
        Value::Interval {
            months,
            days,
            micros,
        } => format!(
            "INTERVAL '{}'",
            crate::datetime::format_interval(*months, *days, *micros)
        ),
    }
}

fn op_symbol(op: BinOp) -> &'static str {
    match op {
        BinOp::Lt => "<",
        BinOp::LtEq => "<=",
        BinOp::Gt => ">",
        BinOp::GtEq => ">=",
        BinOp::Eq => "=",
        BinOp::NotEq => "!=",
        _ => "?",
    }
}

#[cfg(test)]
#[path = "planner_tests.rs"]
mod tests;
