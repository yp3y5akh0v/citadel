//! Rule-based query planner with index selection.
//!
//! Analyzes WHERE clauses to choose between sequential scan, PK lookup,
//! or index scan. Uses leftmost-prefix rule for composite indexes.

use crate::encoding::encode_composite_key;
use crate::parser::{BinOp, Expr};
use crate::types::{IndexDef, TableSchema, Value};

// ── Scan plan ────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ScanPlan {
    SeqScan,
    PkLookup {
        pk_values: Vec<Value>,
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

// ── Simple predicate extraction ──────────────────────────────────────

struct SimplePredicate {
    col_idx: usize,
    op: BinOp,
    value: Value,
}

fn flatten_and<'a>(expr: &'a Expr) -> Vec<&'a Expr> {
    match expr {
        Expr::BinaryOp { left, op: BinOp::And, right } => {
            let mut v = flatten_and(left);
            v.extend(flatten_and(right));
            v
        }
        _ => vec![expr],
    }
}

fn is_comparison(op: BinOp) -> bool {
    matches!(op, BinOp::Eq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq)
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

fn extract_simple_predicate(expr: &Expr, schema: &TableSchema) -> Option<SimplePredicate> {
    match expr {
        Expr::BinaryOp { left, op, right } if is_comparison(*op) => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(name), Expr::Literal(val)) => {
                    let col_idx = schema.column_index(name)?;
                    Some(SimplePredicate { col_idx, op: *op, value: val.clone() })
                }
                (Expr::Literal(val), Expr::Column(name)) => {
                    let col_idx = schema.column_index(name)?;
                    Some(SimplePredicate { col_idx, op: flip_op(*op), value: val.clone() })
                }
                _ => None,
            }
        }
        _ => None,
    }
}

// ── Plan selection ───────────────────────────────────────────────────

pub fn plan_select(schema: &TableSchema, where_clause: &Option<Expr>) -> ScanPlan {
    let where_expr = match where_clause {
        Some(e) => e,
        None => return ScanPlan::SeqScan,
    };

    let predicates = flatten_and(where_expr);
    let simple: Vec<Option<SimplePredicate>> = predicates.iter()
        .map(|p| extract_simple_predicate(p, schema))
        .collect();

    if let Some(plan) = try_pk_lookup(schema, &simple) {
        return plan;
    }

    if let Some(plan) = try_best_index(schema, &simple) {
        return plan;
    }

    ScanPlan::SeqScan
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

// ── Index scoring and selection ──────────────────────────────────────

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct IndexScore {
    num_equality: usize,
    has_range: bool,
    is_unique: bool,
}

fn try_best_index(schema: &TableSchema, predicates: &[Option<SimplePredicate>]) -> Option<ScanPlan> {
    let mut best_score: Option<IndexScore> = None;
    let mut best_plan: Option<ScanPlan> = None;

    for idx in &schema.indices {
        if let Some((score, plan)) = try_index_scan(schema, idx, predicates) {
            if best_score.is_none() || score > *best_score.as_ref().unwrap() {
                best_score = Some(score);
                best_plan = Some(plan);
            }
        }
    }

    best_plan
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
            if used.contains(&i) { continue; }
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
                if used.contains(&i) { continue; }
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

    Some((score, ScanPlan::IndexScan {
        index_name: idx.name.clone(),
        idx_table,
        prefix,
        num_prefix_cols: equality_values.len(),
        range_conds,
        is_unique: idx.unique,
        index_columns: idx.columns.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, DataType};

    fn test_schema() -> TableSchema {
        TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnDef { name: "id".into(), data_type: DataType::Integer, nullable: false, position: 0 },
                ColumnDef { name: "name".into(), data_type: DataType::Text, nullable: true, position: 1 },
                ColumnDef { name: "age".into(), data_type: DataType::Integer, nullable: true, position: 2 },
                ColumnDef { name: "email".into(), data_type: DataType::Text, nullable: true, position: 3 },
            ],
            primary_key_columns: vec![0],
            indices: vec![
                IndexDef { name: "idx_name".into(), columns: vec![1], unique: false },
                IndexDef { name: "idx_email".into(), columns: vec![3], unique: true },
                IndexDef { name: "idx_name_age".into(), columns: vec![1, 2], unique: false },
            ],
        }
    }

    #[test]
    fn no_where_is_seq_scan() {
        let schema = test_schema();
        let plan = plan_select(&schema, &None);
        assert!(matches!(plan, ScanPlan::SeqScan));
    }

    #[test]
    fn pk_equality_is_pk_lookup() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Integer(42))),
        });
        let plan = plan_select(&schema, &where_clause);
        match plan {
            ScanPlan::PkLookup { pk_values } => {
                assert_eq!(pk_values, vec![Value::Integer(42)]);
            }
            other => panic!("expected PkLookup, got {other:?}"),
        }
    }

    #[test]
    fn unique_index_equality() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("email".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("alice@test.com".into()))),
        });
        let plan = plan_select(&schema, &where_clause);
        match plan {
            ScanPlan::IndexScan { index_name, is_unique, num_prefix_cols, .. } => {
                assert_eq!(index_name, "idx_email");
                assert!(is_unique);
                assert_eq!(num_prefix_cols, 1);
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }
    }

    #[test]
    fn non_unique_index_equality() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
        });
        let plan = plan_select(&schema, &where_clause);
        match plan {
            ScanPlan::IndexScan { index_name, num_prefix_cols, .. } => {
                assert!(index_name == "idx_name" || index_name == "idx_name_age");
                assert_eq!(num_prefix_cols, 1);
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }
    }

    #[test]
    fn composite_index_full_prefix() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("name".into())),
                op: BinOp::Eq,
                right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
            }),
            op: BinOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("age".into())),
                op: BinOp::Eq,
                right: Box::new(Expr::Literal(Value::Integer(30))),
            }),
        });
        let plan = plan_select(&schema, &where_clause);
        match plan {
            ScanPlan::IndexScan { index_name, num_prefix_cols, .. } => {
                assert_eq!(index_name, "idx_name_age");
                assert_eq!(num_prefix_cols, 2);
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }
    }

    #[test]
    fn range_scan_on_indexed_column() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Gt,
            right: Box::new(Expr::Literal(Value::Text("M".into()))),
        });
        let plan = plan_select(&schema, &where_clause);
        match plan {
            ScanPlan::IndexScan { range_conds, num_prefix_cols, .. } => {
                assert_eq!(num_prefix_cols, 0);
                assert_eq!(range_conds.len(), 1);
                assert_eq!(range_conds[0].0, BinOp::Gt);
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }
    }

    #[test]
    fn composite_equality_plus_range() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("name".into())),
                op: BinOp::Eq,
                right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
            }),
            op: BinOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("age".into())),
                op: BinOp::Gt,
                right: Box::new(Expr::Literal(Value::Integer(25))),
            }),
        });
        let plan = plan_select(&schema, &where_clause);
        match plan {
            ScanPlan::IndexScan { index_name, num_prefix_cols, range_conds, .. } => {
                assert_eq!(index_name, "idx_name_age");
                assert_eq!(num_prefix_cols, 1);
                assert_eq!(range_conds.len(), 1);
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }
    }

    #[test]
    fn or_condition_falls_back_to_seq_scan() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("name".into())),
                op: BinOp::Eq,
                right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
            }),
            op: BinOp::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("name".into())),
                op: BinOp::Eq,
                right: Box::new(Expr::Literal(Value::Text("Bob".into()))),
            }),
        });
        let plan = plan_select(&schema, &where_clause);
        assert!(matches!(plan, ScanPlan::SeqScan));
    }

    #[test]
    fn non_indexed_column_is_seq_scan() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Integer(30))),
        });
        let plan = plan_select(&schema, &where_clause);
        assert!(matches!(plan, ScanPlan::SeqScan));
    }

    #[test]
    fn reversed_literal_column() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Literal(Value::Integer(42))),
            op: BinOp::Eq,
            right: Box::new(Expr::Column("id".into())),
        });
        let plan = plan_select(&schema, &where_clause);
        assert!(matches!(plan, ScanPlan::PkLookup { .. }));
    }

    #[test]
    fn reversed_comparison_flips_op() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Literal(Value::Integer(5))),
            op: BinOp::Lt,
            right: Box::new(Expr::Column("name".into())),
        });
        let plan = plan_select(&schema, &where_clause);
        match plan {
            ScanPlan::IndexScan { range_conds, .. } => {
                assert_eq!(range_conds[0].0, BinOp::Gt);
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }
    }

    #[test]
    fn prefers_unique_index() {
        let schema = TableSchema {
            name: "t".into(),
            columns: vec![
                ColumnDef { name: "id".into(), data_type: DataType::Integer, nullable: false, position: 0 },
                ColumnDef { name: "code".into(), data_type: DataType::Text, nullable: false, position: 1 },
            ],
            primary_key_columns: vec![0],
            indices: vec![
                IndexDef { name: "idx_code".into(), columns: vec![1], unique: false },
                IndexDef { name: "idx_code_uniq".into(), columns: vec![1], unique: true },
            ],
        };
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("code".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("X".into()))),
        });
        let plan = plan_select(&schema, &where_clause);
        match plan {
            ScanPlan::IndexScan { index_name, is_unique, .. } => {
                assert_eq!(index_name, "idx_code_uniq");
                assert!(is_unique);
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }
    }

    #[test]
    fn prefers_more_equality_columns() {
        let schema = test_schema();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("name".into())),
                op: BinOp::Eq,
                right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
            }),
            op: BinOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("age".into())),
                op: BinOp::Eq,
                right: Box::new(Expr::Literal(Value::Integer(30))),
            }),
        });
        let plan = plan_select(&schema, &where_clause);
        match plan {
            ScanPlan::IndexScan { index_name, num_prefix_cols, .. } => {
                assert_eq!(index_name, "idx_name_age");
                assert_eq!(num_prefix_cols, 2);
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }
    }
}
