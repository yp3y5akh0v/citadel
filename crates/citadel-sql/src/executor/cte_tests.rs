use super::*;
use crate::parser::{
    CteDefinition, Expr, InsertSource, InsertStmt, JoinClause, JoinType, QueryBody, SelectColumn,
    SelectQuery, SelectStmt, TableRef,
};
use crate::types::{QueryResult, Value};

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

fn select_query(ctes: Vec<CteDefinition>, body: QueryBody, recursive: bool) -> SelectQuery {
    SelectQuery {
        ctes,
        body,
        recursive,
    }
}

fn cte_def(name: &str, body: QueryBody) -> CteDefinition {
    CteDefinition {
        name: name.into(),
        column_aliases: vec![],
        body,
    }
}

#[test]
fn build_cte_schema_columns_from_query_result() {
    let qr = QueryResult {
        columns: vec!["a".into(), "b".into()],
        rows: vec![],
    };
    let ts = build_cte_schema("c", &qr);
    assert_eq!(ts.name, "c");
    assert_eq!(ts.columns.len(), 2);
    assert_eq!(ts.columns[0].name, "a");
    assert_eq!(ts.columns[1].name, "b");
}

#[test]
fn build_cte_schema_empty() {
    let qr = QueryResult {
        columns: vec![],
        rows: vec![Vec::<Value>::new()],
    };
    let ts = build_cte_schema("c", &qr);
    assert!(ts.columns.is_empty());
}

#[test]
fn cte_body_references_self_select_from() {
    let body = QueryBody::Select(Box::new(empty_select("c")));
    assert!(cte_body_references_self(&body, "c"));
}

#[test]
fn cte_body_references_self_select_join() {
    let mut sel = empty_select("other");
    sel.joins.push(JoinClause {
        join_type: JoinType::Inner,
        table: TableRef {
            name: "c".into(),
            alias: None,
            args: None,
        },
        subquery: None,
        on_clause: Some(Expr::Literal(Value::Boolean(true))),
    });
    let body = QueryBody::Select(Box::new(sel));
    assert!(cte_body_references_self(&body, "c"));
}

#[test]
fn cte_body_references_self_case_insensitive() {
    let body = QueryBody::Select(Box::new(empty_select("Cte_Name")));
    assert!(cte_body_references_self(&body, "cte_name"));
}

#[test]
fn cte_body_references_self_unrelated_returns_false() {
    let body = QueryBody::Select(Box::new(empty_select("other")));
    assert!(!cte_body_references_self(&body, "c"));
}

#[test]
fn cte_body_references_self_dml_never_references() {
    let body = QueryBody::Insert(Box::new(InsertStmt {
        table: "c".into(),
        columns: vec![],
        source: InsertSource::Values(vec![]),
        on_conflict: None,
        returning: None,
    }));
    assert!(!cte_body_references_self(&body, "c"));
}

#[test]
fn try_fuse_cte_requires_single_non_recursive_cte() {
    let sq = select_query(
        vec![cte_def("c", QueryBody::Select(Box::new(empty_select("t"))))],
        QueryBody::Select(Box::new(empty_select("c"))),
        true,
    );
    assert!(try_fuse_cte(&sq).is_none());
}

#[test]
fn try_fuse_cte_with_two_ctes_returns_none() {
    let sq = select_query(
        vec![
            cte_def("c1", QueryBody::Select(Box::new(empty_select("t")))),
            cte_def("c2", QueryBody::Select(Box::new(empty_select("t")))),
        ],
        QueryBody::Select(Box::new(empty_select("c1"))),
        false,
    );
    assert!(try_fuse_cte(&sq).is_none());
}

#[test]
fn try_fuse_cte_with_column_aliases_returns_none() {
    let mut def = cte_def("c", QueryBody::Select(Box::new(empty_select("t"))));
    def.column_aliases = vec!["x".into()];
    let sq = select_query(
        vec![def],
        QueryBody::Select(Box::new(empty_select("c"))),
        false,
    );
    assert!(try_fuse_cte(&sq).is_none());
}

#[test]
fn try_fuse_cte_simple_passthrough_fuses() {
    let sq = select_query(
        vec![cte_def("c", QueryBody::Select(Box::new(empty_select("t"))))],
        QueryBody::Select(Box::new(empty_select("c"))),
        false,
    );
    let fused = try_fuse_cte(&sq);
    assert!(fused.is_some());
    if let Some(QueryBody::Select(s)) = fused {
        assert_eq!(s.from, "t");
    } else {
        panic!("expected fused Select body");
    }
}
