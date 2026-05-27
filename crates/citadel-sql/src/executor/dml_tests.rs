use super::*;
use crate::parser::{
    CompoundSelect, DeleteStmt, Expr, InsertSource, InsertStmt, QueryBody, SelectColumn,
    SelectStmt, SetOp, UpdateStmt,
};
use crate::types::{ExecutionResult, QueryResult, Value};

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

fn i(n: i64) -> Value {
    Value::Integer(n)
}

fn qr(columns: Vec<&str>, rows: Vec<Vec<Value>>) -> QueryResult {
    QueryResult {
        columns: columns.into_iter().map(String::from).collect(),
        rows,
    }
}

fn scalar_subq(from: &str) -> Expr {
    Expr::ScalarSubquery(Box::new(empty_select(from)))
}

#[test]
fn has_subquery_literal_false() {
    assert!(!has_subquery(&Expr::Literal(i(1))));
}

#[test]
fn has_subquery_scalar_subquery() {
    assert!(has_subquery(&scalar_subq("t")));
}

#[test]
fn stmt_has_subquery_in_where() {
    let mut s = empty_select("t");
    s.where_clause = Some(scalar_subq("inner"));
    assert!(stmt_has_subquery(&s));
}

#[test]
fn stmt_has_subquery_in_columns() {
    let mut s = empty_select("t");
    s.columns = vec![SelectColumn::Expr {
        expr: scalar_subq("inner"),
        alias: None,
    }];
    assert!(stmt_has_subquery(&s));
}

#[test]
fn stmt_has_subquery_in_having() {
    let mut s = empty_select("t");
    s.having = Some(scalar_subq("inner"));
    assert!(stmt_has_subquery(&s));
}

#[test]
fn stmt_has_subquery_none_returns_false() {
    let s = empty_select("t");
    assert!(!stmt_has_subquery(&s));
}

#[test]
fn update_has_subquery_in_where() {
    let s = UpdateStmt {
        table: "t".into(),
        assignments: vec![("v".into(), Expr::Literal(i(1)))],
        where_clause: Some(scalar_subq("inner")),
        returning: None,
    };
    assert!(update_has_subquery(&s));
}

#[test]
fn update_has_subquery_in_assignment() {
    let s = UpdateStmt {
        table: "t".into(),
        assignments: vec![("v".into(), scalar_subq("inner"))],
        where_clause: None,
        returning: None,
    };
    assert!(update_has_subquery(&s));
}

#[test]
fn update_has_subquery_none() {
    let s = UpdateStmt {
        table: "t".into(),
        assignments: vec![("v".into(), Expr::Literal(i(1)))],
        where_clause: Some(Expr::Literal(i(1))),
        returning: None,
    };
    assert!(!update_has_subquery(&s));
}

#[test]
fn delete_has_subquery_in_where() {
    let s = DeleteStmt {
        table: "t".into(),
        where_clause: Some(scalar_subq("inner")),
        returning: None,
    };
    assert!(delete_has_subquery(&s));
}

#[test]
fn delete_has_subquery_none() {
    let s = DeleteStmt {
        table: "t".into(),
        where_clause: None,
        returning: None,
    };
    assert!(!delete_has_subquery(&s));
}

#[test]
fn insert_has_subquery_in_values() {
    let s = InsertStmt {
        table: "t".into(),
        columns: vec!["id".into()],
        source: InsertSource::Values(vec![vec![scalar_subq("inner")]]),
        on_conflict: None,
        returning: None,
    };
    assert!(insert_has_subquery(&s));
}

#[test]
fn insert_has_subquery_select_source_returns_false() {
    let sq = crate::parser::SelectQuery {
        ctes: vec![],
        body: QueryBody::Select(Box::new(empty_select("src"))),
        recursive: false,
    };
    let s = InsertStmt {
        table: "t".into(),
        columns: vec!["id".into()],
        source: InsertSource::Select(Box::new(sq)),
        on_conflict: None,
        returning: None,
    };
    assert!(!insert_has_subquery(&s));
}

#[test]
fn apply_set_operation_union_all_concatenates() {
    let left = qr(vec!["x"], vec![vec![i(1)], vec![i(2)]]);
    let right = qr(vec!["x"], vec![vec![i(2)], vec![i(3)]]);
    let comp = CompoundSelect {
        op: SetOp::Union,
        all: true,
        left: Box::new(QueryBody::Select(Box::new(empty_select("a")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("b")))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let result = apply_set_operation(&comp, left, right).unwrap();
    if let ExecutionResult::Query(q) = result {
        assert_eq!(q.rows.len(), 4);
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn apply_set_operation_union_dedupes() {
    let left = qr(vec!["x"], vec![vec![i(1)], vec![i(2)]]);
    let right = qr(vec!["x"], vec![vec![i(2)], vec![i(3)]]);
    let comp = CompoundSelect {
        op: SetOp::Union,
        all: false,
        left: Box::new(QueryBody::Select(Box::new(empty_select("a")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("b")))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let result = apply_set_operation(&comp, left, right).unwrap();
    if let ExecutionResult::Query(q) = result {
        assert_eq!(q.rows.len(), 3);
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn apply_set_operation_intersect_keeps_common() {
    let left = qr(vec!["x"], vec![vec![i(1)], vec![i(2)], vec![i(3)]]);
    let right = qr(vec!["x"], vec![vec![i(2)], vec![i(3)], vec![i(4)]]);
    let comp = CompoundSelect {
        op: SetOp::Intersect,
        all: false,
        left: Box::new(QueryBody::Select(Box::new(empty_select("a")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("b")))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let result = apply_set_operation(&comp, left, right).unwrap();
    if let ExecutionResult::Query(q) = result {
        assert_eq!(q.rows.len(), 2);
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn apply_set_operation_except_removes_right() {
    let left = qr(vec!["x"], vec![vec![i(1)], vec![i(2)], vec![i(3)]]);
    let right = qr(vec!["x"], vec![vec![i(2)]]);
    let comp = CompoundSelect {
        op: SetOp::Except,
        all: false,
        left: Box::new(QueryBody::Select(Box::new(empty_select("a")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("b")))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let result = apply_set_operation(&comp, left, right).unwrap();
    if let ExecutionResult::Query(q) = result {
        assert_eq!(q.rows.len(), 2);
        assert!(q.rows.contains(&vec![i(1)]));
        assert!(q.rows.contains(&vec![i(3)]));
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn apply_set_operation_column_count_mismatch_errors() {
    let left = qr(vec!["a", "b"], vec![vec![i(1), i(2)]]);
    let right = qr(vec!["c"], vec![vec![i(3)]]);
    let comp = CompoundSelect {
        op: SetOp::Union,
        all: false,
        left: Box::new(QueryBody::Select(Box::new(empty_select("a")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("b")))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    assert!(apply_set_operation(&comp, left, right).is_err());
}

#[test]
fn materialize_expr_in_subquery_converts_to_in_set() {
    let inner_qr = qr(vec!["x"], vec![vec![i(1)], vec![i(2)]]);
    let e = Expr::InSubquery {
        expr: Box::new(Expr::Column("v".into())),
        subquery: Box::new(empty_select("inner")),
        negated: false,
    };
    let mut exec_sub = |_: &SelectStmt| Ok(inner_qr.clone());
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::InSet { .. }));
}

#[test]
fn materialize_expr_scalar_subquery_becomes_literal() {
    let inner_qr = qr(vec!["x"], vec![vec![i(42)]]);
    let e = scalar_subq("inner");
    let mut exec_sub = |_: &SelectStmt| Ok(inner_qr.clone());
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Integer(42))));
}

#[test]
fn materialize_expr_scalar_subquery_empty_becomes_null() {
    let inner_qr = qr(vec!["x"], vec![]);
    let e = scalar_subq("inner");
    let mut exec_sub = |_: &SelectStmt| Ok(inner_qr.clone());
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Null)));
}

#[test]
fn materialize_expr_exists_true() {
    let inner_qr = qr(vec!["x"], vec![vec![i(1)]]);
    let e = Expr::Exists {
        subquery: Box::new(empty_select("inner")),
        negated: false,
    };
    let mut exec_sub = |_: &SelectStmt| Ok(inner_qr.clone());
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Boolean(true))));
}

#[test]
fn materialize_expr_not_exists_false_when_rows_present() {
    let inner_qr = qr(vec!["x"], vec![vec![i(1)]]);
    let e = Expr::Exists {
        subquery: Box::new(empty_select("inner")),
        negated: true,
    };
    let mut exec_sub = |_: &SelectStmt| Ok(inner_qr.clone());
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Boolean(false))));
}

#[test]
fn materialize_expr_exists_false_for_empty_subquery() {
    let inner_qr = qr(vec!["x"], vec![]);
    let e = Expr::Exists {
        subquery: Box::new(empty_select("inner")),
        negated: false,
    };
    let mut exec_sub = |_: &SelectStmt| Ok(inner_qr.clone());
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Boolean(false))));
}

#[test]
fn materialize_expr_scalar_multiple_rows_errors() {
    let inner_qr = qr(vec!["x"], vec![vec![i(1)], vec![i(2)]]);
    let e = scalar_subq("inner");
    let mut exec_sub = |_: &SelectStmt| Ok(inner_qr.clone());
    assert!(materialize_expr(&e, &mut exec_sub).is_err());
}

#[test]
fn materialize_expr_pass_through_literal() {
    let mut exec_sub = |_: &SelectStmt| {
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    };
    let e = Expr::Literal(i(5));
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Integer(5))));
}

#[test]
fn materialize_query_body_pass_through_dml() {
    let body = QueryBody::Insert(Box::new(InsertStmt {
        table: "t".into(),
        columns: vec![],
        source: InsertSource::Values(vec![]),
        on_conflict: None,
        returning: None,
    }));
    let mut exec_sub = |_: &SelectStmt| {
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    };
    let result = materialize_query_body(&body, &mut exec_sub).unwrap();
    assert!(matches!(result, QueryBody::Insert(_)));
}
