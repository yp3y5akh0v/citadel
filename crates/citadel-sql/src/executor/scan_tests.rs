use super::*;
use crate::encoding::RawColumn;
use crate::parser::{BinOp, Expr};
use crate::types::{Collation, ColumnDef, DataType, TableSchema, Value};

fn columns(specs: &[(&str, DataType)]) -> Vec<ColumnDef> {
    specs
        .iter()
        .enumerate()
        .map(|(i, (name, dt))| ColumnDef {
            name: (*name).to_string(),
            data_type: *dt,
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
            collation: Collation::Binary,
        })
        .collect()
}

fn schema(name: &str, cols: Vec<ColumnDef>, pk: Vec<u16>) -> TableSchema {
    TableSchema::new(name.into(), cols, pk, vec![], vec![], vec![])
}

fn i(n: i64) -> Value {
    Value::Integer(n)
}

#[test]
fn check_pk_range_empty_matches() {
    assert_eq!(check_pk_range(&i(5), &[]), 0);
}

#[test]
fn check_pk_range_lt_below_matches() {
    assert_eq!(check_pk_range(&i(5), &[(BinOp::Lt, i(10))]), 0);
}

#[test]
fn check_pk_range_lt_at_bound_stops() {
    assert_eq!(check_pk_range(&i(10), &[(BinOp::Lt, i(10))]), 2);
}

#[test]
fn check_pk_range_lteq_equal_matches() {
    assert_eq!(check_pk_range(&i(10), &[(BinOp::LtEq, i(10))]), 0);
}

#[test]
fn check_pk_range_lteq_above_stops() {
    assert_eq!(check_pk_range(&i(11), &[(BinOp::LtEq, i(10))]), 2);
}

#[test]
fn check_pk_range_gt_at_bound_skips() {
    assert_eq!(check_pk_range(&i(5), &[(BinOp::Gt, i(5))]), 1);
}

#[test]
fn check_pk_range_gt_above_matches() {
    assert_eq!(check_pk_range(&i(6), &[(BinOp::Gt, i(5))]), 0);
}

#[test]
fn check_pk_range_gteq_equal_matches() {
    assert_eq!(check_pk_range(&i(5), &[(BinOp::GtEq, i(5))]), 0);
}

#[test]
fn check_pk_range_gteq_below_skips() {
    assert_eq!(check_pk_range(&i(4), &[(BinOp::GtEq, i(5))]), 1);
}

#[test]
fn check_pk_range_combined_lower_upper() {
    let conds = vec![(BinOp::GtEq, i(5)), (BinOp::Lt, i(10))];
    assert_eq!(check_pk_range(&i(4), &conds), 1);
    assert_eq!(check_pk_range(&i(5), &conds), 0);
    assert_eq!(check_pk_range(&i(9), &conds), 0);
    assert_eq!(check_pk_range(&i(10), &conds), 2);
}

#[test]
fn flip_cmp_op_symmetric() {
    assert_eq!(flip_cmp_op(BinOp::Eq), Some(BinOp::Eq));
    assert_eq!(flip_cmp_op(BinOp::NotEq), Some(BinOp::NotEq));
}

#[test]
fn flip_cmp_op_asymmetric() {
    assert_eq!(flip_cmp_op(BinOp::Lt), Some(BinOp::Gt));
    assert_eq!(flip_cmp_op(BinOp::Gt), Some(BinOp::Lt));
    assert_eq!(flip_cmp_op(BinOp::LtEq), Some(BinOp::GtEq));
    assert_eq!(flip_cmp_op(BinOp::GtEq), Some(BinOp::LtEq));
}

#[test]
fn flip_cmp_op_non_comparison_returns_none() {
    assert_eq!(flip_cmp_op(BinOp::And), None);
    assert_eq!(flip_cmp_op(BinOp::Add), None);
}

#[test]
fn raw_matches_op_value_eq() {
    assert!(raw_matches_op_value(&i(5), BinOp::Eq, &i(5)));
    assert!(!raw_matches_op_value(&i(5), BinOp::Eq, &i(6)));
}

#[test]
fn raw_matches_op_value_neq() {
    assert!(raw_matches_op_value(&i(5), BinOp::NotEq, &i(6)));
    assert!(!raw_matches_op_value(&i(5), BinOp::NotEq, &i(5)));
}

#[test]
fn raw_matches_op_value_neq_null_lhs_is_false() {
    assert!(!raw_matches_op_value(&Value::Null, BinOp::NotEq, &i(5)));
}

#[test]
fn raw_matches_op_value_ordering() {
    assert!(raw_matches_op_value(&i(4), BinOp::Lt, &i(5)));
    assert!(raw_matches_op_value(&i(5), BinOp::LtEq, &i(5)));
    assert!(raw_matches_op_value(&i(6), BinOp::Gt, &i(5)));
    assert!(raw_matches_op_value(&i(5), BinOp::GtEq, &i(5)));
}

#[test]
fn raw_matches_op_value_non_comparison_returns_false() {
    assert!(!raw_matches_op_value(&i(5), BinOp::And, &i(5)));
    assert!(!raw_matches_op_value(&i(5), BinOp::Add, &i(5)));
}

#[test]
fn raw_matches_op_raw_null_short_circuits() {
    assert!(!raw_matches_op(&RawColumn::Null, BinOp::Eq, &i(5)));
}

#[test]
fn raw_matches_op_literal_null_short_circuits() {
    assert!(!raw_matches_op(
        &RawColumn::Integer(5),
        BinOp::Eq,
        &Value::Null
    ));
}

#[test]
fn raw_matches_op_eq_integer() {
    assert!(raw_matches_op(&RawColumn::Integer(5), BinOp::Eq, &i(5)));
    assert!(!raw_matches_op(&RawColumn::Integer(5), BinOp::Eq, &i(6)));
}

#[test]
fn raw_matches_op_neq_integer() {
    assert!(raw_matches_op(&RawColumn::Integer(5), BinOp::NotEq, &i(6)));
    assert!(!raw_matches_op(&RawColumn::Integer(5), BinOp::NotEq, &i(5)));
}

#[test]
fn raw_matches_op_lt_integer() {
    assert!(raw_matches_op(&RawColumn::Integer(4), BinOp::Lt, &i(5)));
    assert!(!raw_matches_op(&RawColumn::Integer(5), BinOp::Lt, &i(5)));
}

#[test]
fn raw_matches_op_gteq_integer() {
    assert!(raw_matches_op(&RawColumn::Integer(5), BinOp::GtEq, &i(5)));
    assert!(raw_matches_op(&RawColumn::Integer(6), BinOp::GtEq, &i(5)));
    assert!(!raw_matches_op(&RawColumn::Integer(4), BinOp::GtEq, &i(5)));
}

#[test]
fn try_simple_predicate_integer_eq() {
    let ts = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Integer)]),
        vec![0],
    );
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("v".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(7))),
    };
    assert!(try_simple_predicate(&expr, &ts).is_some());
}

#[test]
fn try_simple_predicate_unknown_column_returns_none() {
    let ts = schema("t", columns(&[("id", DataType::Integer)]), vec![0]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("missing".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(1))),
    };
    assert!(try_simple_predicate(&expr, &ts).is_none());
}

#[test]
fn try_between_predicate_basic() {
    let ts = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Integer)]),
        vec![0],
    );
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("v".into())),
        low: Box::new(Expr::Literal(i(1))),
        high: Box::new(Expr::Literal(i(10))),
        negated: false,
    };
    assert!(try_between_predicate(&expr, &ts).is_some());
}

#[test]
fn try_between_predicate_unknown_column_returns_none() {
    let ts = schema("t", columns(&[("id", DataType::Integer)]), vec![0]);
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("missing".into())),
        low: Box::new(Expr::Literal(i(1))),
        high: Box::new(Expr::Literal(i(2))),
        negated: false,
    };
    assert!(try_between_predicate(&expr, &ts).is_none());
}

#[test]
fn fold_temporal_offset_non_temporal_returns_none() {
    let expr = Expr::Literal(i(1));
    assert!(fold_temporal_offset(&expr).is_none());
}
