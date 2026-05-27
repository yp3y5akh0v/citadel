use super::*;
use crate::parser::{BinOp, Expr, GeneratedKind};
use crate::types::{Collation, ColumnDef, DataType, Value};

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

fn i(n: i64) -> Value {
    Value::Integer(n)
}

#[test]
fn collect_column_refs_simple_column() {
    let mut out = Vec::new();
    collect_column_refs(&Expr::Column("X".into()), &mut out);
    assert_eq!(out, vec!["x"]);
}

#[test]
fn collect_column_refs_qualified_uses_column_only() {
    let mut out = Vec::new();
    collect_column_refs(
        &Expr::QualifiedColumn {
            table: "T".into(),
            column: "Y".into(),
        },
        &mut out,
    );
    assert_eq!(out, vec!["y"]);
}

#[test]
fn collect_column_refs_binary_op() {
    let mut out = Vec::new();
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Mul,
        right: Box::new(Expr::Column("b".into())),
    };
    collect_column_refs(&e, &mut out);
    assert_eq!(out, vec!["a", "b"]);
}

#[test]
fn collect_column_refs_function_args() {
    let mut out = Vec::new();
    let e = Expr::Function {
        name: "ABS".into(),
        args: vec![Expr::Column("v".into())],
        distinct: false,
    };
    collect_column_refs(&e, &mut out);
    assert_eq!(out, vec!["v"]);
}

#[test]
fn collect_column_refs_literal_yields_empty() {
    let mut out = Vec::new();
    collect_column_refs(&Expr::Literal(i(1)), &mut out);
    assert!(out.is_empty());
}

#[test]
fn collect_column_refs_case_branches() {
    let mut out = Vec::new();
    let e = Expr::Case {
        operand: None,
        conditions: vec![(Expr::Column("c".into()), Expr::Column("r".into()))],
        else_result: Some(Box::new(Expr::Column("el".into()))),
    };
    collect_column_refs(&e, &mut out);
    assert_eq!(out, vec!["c", "r", "el"]);
}

#[test]
fn validate_no_chained_generated_no_generated_columns_ok() {
    let cs = vec![col("a", DataType::Integer), col("b", DataType::Integer)];
    assert!(validate_no_chained_generated(&cs).is_ok());
}

#[test]
fn validate_no_chained_generated_self_reference_ok() {
    let mut gen_col = col("g", DataType::Integer);
    gen_col.generated_kind = Some(GeneratedKind::Stored);
    gen_col.generated_expr = Some(Expr::Column("g".into()));
    let cs = vec![col("a", DataType::Integer), gen_col];
    assert!(validate_no_chained_generated(&cs).is_ok());
}

#[test]
fn validate_no_chained_generated_references_non_generated_ok() {
    let mut gen_col = col("g", DataType::Integer);
    gen_col.generated_kind = Some(GeneratedKind::Stored);
    gen_col.generated_expr = Some(Expr::Column("a".into()));
    let cs = vec![col("a", DataType::Integer), gen_col];
    assert!(validate_no_chained_generated(&cs).is_ok());
}

#[test]
fn validate_no_chained_generated_chain_rejected() {
    let mut g1 = col("g1", DataType::Integer);
    g1.generated_kind = Some(GeneratedKind::Stored);
    g1.generated_expr = Some(Expr::Column("a".into()));
    let mut g2 = col("g2", DataType::Integer);
    g2.generated_kind = Some(GeneratedKind::Stored);
    g2.generated_expr = Some(Expr::Column("g1".into()));
    let cs = vec![col("a", DataType::Integer), g1, g2];
    assert!(validate_no_chained_generated(&cs).is_err());
}
