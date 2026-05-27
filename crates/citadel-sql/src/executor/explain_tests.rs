use super::*;
use crate::parser::{BinOp, Expr, SelectColumn, SelectStmt};
use crate::types::Value;

fn empty_select(from: &str) -> SelectStmt {
    SelectStmt {
        columns: vec![SelectColumn::AllColumns],
        from: from.into(),
        from_alias: None,
        from_subquery: None,
        from_args: None,
        from_json_table: None,
        joins: vec![],
        distinct: false,
        where_clause: None,
        order_by: vec![],
        limit: None,
        offset: None,
        group_by: vec![],
        having: None,
    }
}

fn scalar_subq(from: &str) -> Expr {
    Expr::ScalarSubquery(Box::new(empty_select(from)))
}

#[test]
fn count_subqueries_in_literal_is_zero() {
    assert_eq!(count_subqueries(&Expr::Literal(Value::Integer(1))), 0);
}

#[test]
fn count_subqueries_in_scalar_subquery_is_one() {
    assert_eq!(count_subqueries(&scalar_subq("t")), 1);
}

#[test]
fn count_subqueries_in_exists() {
    let e = Expr::Exists {
        subquery: Box::new(empty_select("t")),
        negated: false,
    };
    assert_eq!(count_subqueries(&e), 1);
}

#[test]
fn count_subqueries_in_binary_op_sums_both_sides() {
    let e = Expr::BinaryOp {
        left: Box::new(scalar_subq("a")),
        op: BinOp::Eq,
        right: Box::new(scalar_subq("b")),
    };
    assert_eq!(count_subqueries(&e), 2);
}

#[test]
fn count_subqueries_in_nested_in_subquery() {
    let e = Expr::InSubquery {
        expr: Box::new(scalar_subq("inner")),
        subquery: Box::new(empty_select("outer")),
        negated: false,
    };
    assert_eq!(count_subqueries(&e), 2);
}

#[test]
fn count_subqueries_in_function_args() {
    let e = Expr::Function {
        name: "COALESCE".into(),
        args: vec![scalar_subq("a"), scalar_subq("b"), scalar_subq("c")],
        distinct: false,
    };
    assert_eq!(count_subqueries(&e), 3);
}

#[test]
fn count_subqueries_in_case_includes_all_branches() {
    let e = Expr::Case {
        operand: Some(Box::new(scalar_subq("a"))),
        conditions: vec![(scalar_subq("b"), scalar_subq("c"))],
        else_result: Some(Box::new(scalar_subq("d"))),
    };
    assert_eq!(count_subqueries(&e), 4);
}

#[test]
fn explain_subqueries_appends_one_line_per_subquery() {
    let mut sel = empty_select("t");
    sel.where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("x".into())),
        op: BinOp::Eq,
        right: Box::new(scalar_subq("inner")),
    });
    let mut lines: Vec<String> = vec![];
    explain_subqueries(&sel, &mut lines);
    assert_eq!(lines, vec!["SUBQUERY".to_string()]);
}
