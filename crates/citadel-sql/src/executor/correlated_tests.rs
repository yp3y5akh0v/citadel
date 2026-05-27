use super::*;
use crate::parser::{BinOp, Expr};
use crate::types::{Collation, ColumnDef, DataType, TableSchema, Value};

fn col(name: &str, dt: DataType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: dt,
        nullable: true,
        position: 0,
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
    }
}

fn cols(specs: &[(&str, DataType)]) -> Vec<ColumnDef> {
    specs
        .iter()
        .enumerate()
        .map(|(i, (n, t))| {
            let mut c = col(n, *t);
            c.position = i as u16;
            c
        })
        .collect()
}

fn schema(name: &str, cs: Vec<ColumnDef>, pk: Vec<u16>) -> TableSchema {
    TableSchema::new(name.into(), cs, pk, vec![], vec![], vec![])
}

fn i(n: i64) -> Value {
    Value::Integer(n)
}

#[test]
fn resolves_in_lowercases_input() {
    let ts = schema(
        "t",
        cols(&[("name", DataType::Text), ("id", DataType::Integer)]),
        vec![1],
    );
    assert!(resolves_in("name", &ts));
    assert!(resolves_in("NAME", &ts));
    assert!(resolves_in("id", &ts));
}

#[test]
fn resolves_in_unknown_column_false() {
    let ts = schema("t", cols(&[("id", DataType::Integer)]), vec![0]);
    assert!(!resolves_in("missing", &ts));
}

#[test]
fn col_name_lower_column() {
    assert_eq!(col_name_lower(&Expr::Column("X".into())), Some("x".into()));
}

#[test]
fn col_name_lower_qualified() {
    assert_eq!(
        col_name_lower(&Expr::QualifiedColumn {
            table: "T".into(),
            column: "Col".into(),
        }),
        Some("col".into())
    );
}

#[test]
fn col_name_lower_non_column_returns_none() {
    assert_eq!(col_name_lower(&Expr::Literal(i(1))), None);
}

#[test]
fn collect_column_names_single_column() {
    let mut out = Vec::new();
    collect_column_names(&Expr::Column("X".into()), &mut out);
    assert_eq!(out, vec!["x"]);
}

#[test]
fn collect_column_names_qualified_lowercase() {
    let mut out = Vec::new();
    collect_column_names(
        &Expr::QualifiedColumn {
            table: "T".into(),
            column: "Col".into(),
        },
        &mut out,
    );
    assert_eq!(out, vec!["t.col"]);
}

#[test]
fn collect_column_names_binary_op_collects_both_sides() {
    let mut out = Vec::new();
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Column("b".into())),
    };
    collect_column_names(&e, &mut out);
    assert_eq!(out, vec!["a", "b"]);
}

#[test]
fn collect_column_names_literal_yields_empty() {
    let mut out = Vec::new();
    collect_column_names(&Expr::Literal(i(1)), &mut out);
    assert!(out.is_empty());
}

#[test]
fn collect_column_names_function_args() {
    let mut out = Vec::new();
    let e = Expr::Function {
        name: "ABS".into(),
        args: vec![Expr::Column("x".into())],
        distinct: false,
    };
    collect_column_names(&e, &mut out);
    assert_eq!(out, vec!["x"]);
}

#[test]
fn collect_column_names_coalesce_collects_all() {
    let mut out = Vec::new();
    let e = Expr::Coalesce(vec![
        Expr::Column("a".into()),
        Expr::Column("b".into()),
        Expr::Column("c".into()),
    ]);
    collect_column_names(&e, &mut out);
    assert_eq!(out, vec!["a", "b", "c"]);
}

#[test]
fn collect_column_names_case_branches() {
    let mut out = Vec::new();
    let e = Expr::Case {
        operand: Some(Box::new(Expr::Column("op".into()))),
        conditions: vec![(Expr::Column("c".into()), Expr::Column("r".into()))],
        else_result: Some(Box::new(Expr::Column("el".into()))),
    };
    collect_column_names(&e, &mut out);
    assert_eq!(out, vec!["op", "c", "r", "el"]);
}

#[test]
fn collect_column_names_between() {
    let mut out = Vec::new();
    let e = Expr::Between {
        expr: Box::new(Expr::Column("x".into())),
        low: Box::new(Expr::Column("lo".into())),
        high: Box::new(Expr::Column("hi".into())),
        negated: false,
    };
    collect_column_names(&e, &mut out);
    assert_eq!(out, vec!["x", "lo", "hi"]);
}

#[test]
fn collect_column_names_unary_and_isnull() {
    let mut out = Vec::new();
    let inner = Expr::Column("x".into());
    collect_column_names(&Expr::IsNull(Box::new(inner.clone())), &mut out);
    collect_column_names(&Expr::IsNotNull(Box::new(inner)), &mut out);
    assert_eq!(out, vec!["x", "x"]);
}

#[test]
fn flatten_and_exprs_no_and_returns_single() {
    let e = Expr::Literal(i(1));
    let v = flatten_and_exprs(&e);
    assert_eq!(v.len(), 1);
}

#[test]
fn flatten_and_exprs_chained_and_flattens() {
    let inner = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::And,
        right: Box::new(Expr::Column("b".into())),
    };
    let outer = Expr::BinaryOp {
        left: Box::new(inner),
        op: BinOp::And,
        right: Box::new(Expr::Column("c".into())),
    };
    let v = flatten_and_exprs(&outer);
    assert_eq!(v.len(), 3);
}

#[test]
fn flatten_and_exprs_or_does_not_flatten() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Or,
        right: Box::new(Expr::Column("b".into())),
    };
    let v = flatten_and_exprs(&e);
    assert_eq!(v.len(), 1);
}

#[test]
fn has_correlated_in_expr_with_scalar_subquery() {
    let outer = schema("o", cols(&[("x", DataType::Integer)]), vec![]);
    let ctx = CorrelationCtx {
        outer_schema: &outer,
        outer_alias: None,
    };
    let mgr = crate::schema::SchemaManager::empty();
    let e = Expr::Literal(i(0));
    assert!(!has_correlated_in_expr(&e, &ctx, &mgr));
}

#[test]
fn has_correlated_in_expr_binary_op_propagates() {
    let outer = schema("o", cols(&[("x", DataType::Integer)]), vec![]);
    let ctx = CorrelationCtx {
        outer_schema: &outer,
        outer_alias: None,
    };
    let mgr = crate::schema::SchemaManager::empty();
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(1))),
    };
    assert!(!has_correlated_in_expr(&e, &ctx, &mgr));
}

#[test]
fn has_correlated_select_no_subquery_in_columns() {
    let outer = schema("o", cols(&[("x", DataType::Integer)]), vec![]);
    let ctx = CorrelationCtx {
        outer_schema: &outer,
        outer_alias: None,
    };
    let mgr = crate::schema::SchemaManager::empty();
    let columns = vec![crate::parser::SelectColumn::Expr {
        expr: Expr::Column("a".into()),
        alias: None,
    }];
    assert!(!has_correlated_select(&columns, &ctx, &mgr));
}

#[test]
fn has_correlated_where_no_where_clause() {
    let outer = schema("o", cols(&[("x", DataType::Integer)]), vec![]);
    let ctx = CorrelationCtx {
        outer_schema: &outer,
        outer_alias: None,
    };
    let mgr = crate::schema::SchemaManager::empty();
    assert!(!has_correlated_where(&None, &ctx, &mgr));
}
