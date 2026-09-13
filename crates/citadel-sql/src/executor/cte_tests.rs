use super::*;
use crate::parser::{
    CompoundSelect, CteDefinition, Expr, InsertSource, InsertStmt, JoinClause, JoinType, QueryBody,
    SelectColumn, SelectQuery, SelectStmt, SetOp, TableRef,
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

fn many_cte_rows() -> CteRows {
    CteRows::binary(QueryResult {
        columns: vec!["x".into()],
        rows: (0..1_024).map(|i| vec![Value::Integer(i)]).collect(),
    })
}

fn assert_interrupted<T>(outcome: Result<T>) {
    let err = match outcome {
        Err(err) => err,
        Ok(_) => panic!("CTE materialization ignored cancellation"),
    };
    assert!(matches!(
        err,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn cloned_cte_context_shares_materialized_rows() {
    let mut original = CteContext::default();
    original.insert("c".into(), many_cte_rows().shared());

    let cloned = original.clone();
    let original_rows = original.get("c").unwrap();
    let cloned_rows = cloned.get("c").unwrap();

    assert!(std::sync::Arc::ptr_eq(original_rows, cloned_rows));
    assert_eq!(original_rows.result.rows, cloned_rows.result.rows);
}

#[test]
fn build_cte_schema_columns_from_query_result() {
    let qr = QueryResult {
        columns: vec!["a".into(), "b".into()],
        rows: vec![],
    };
    let ts = build_cte_schema("c", &CteRows::binary(qr)).unwrap();
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
    let ts = build_cte_schema("c", &CteRows::binary(qr)).unwrap();
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

#[test]
fn recursive_cte_stops_while_materializing_anchor_rows() {
    let token = citadel::CancelToken::new();
    let cte = CteDefinition {
        name: "r".into(),
        column_aliases: vec![],
        body: QueryBody::Compound(Box::new(CompoundSelect {
            op: SetOp::Union,
            all: false,
            left: Box::new(QueryBody::Select(Box::new(empty_select("seed")))),
            right: Box::new(QueryBody::Select(Box::new(empty_select("r")))),
            order_by: vec![],
            limit: None,
            offset: None,
        })),
    };
    let mut calls = 0;

    let outcome =
        materialize_recursive_cte(&cte, &CteContext::default(), Some(&token), &mut |_, _| {
            calls += 1;
            token.cancel();
            Ok(CteRows::binary(QueryResult {
                columns: vec!["x".into()],
                rows: (0..4_096).map(|i| vec![Value::Integer(i)]).collect(),
            }))
        });
    let err = match outcome {
        Err(err) => err,
        Ok(_) => panic!("recursive materialization ignored cancellation"),
    };

    assert_eq!(
        calls, 1,
        "the recursive arm should not start after cancellation"
    );
    assert!(matches!(
        err,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

fn general_recursive_cte() -> CteDefinition {
    let recursive_body = QueryBody::Compound(Box::new(CompoundSelect {
        op: SetOp::Union,
        all: true,
        left: Box::new(QueryBody::Select(Box::new(empty_select("r")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("r")))),
        order_by: vec![],
        limit: None,
        offset: None,
    }));
    CteDefinition {
        name: "r".into(),
        column_aliases: vec![],
        body: QueryBody::Compound(Box::new(CompoundSelect {
            op: SetOp::Union,
            all: true,
            left: Box::new(QueryBody::Select(Box::new(empty_select("seed")))),
            right: Box::new(recursive_body),
            order_by: vec![],
            limit: None,
            offset: None,
        })),
    }
}

#[test]
fn general_recursive_cte_replaces_its_shared_working_set() {
    let cte = general_recursive_cte();

    let mut calls = 0usize;
    let mut working_sets = Vec::new();
    let result = materialize_recursive_cte(&cte, &CteContext::default(), None, &mut |_, ctx| {
        calls += 1;
        let rows = if calls == 1 {
            vec![vec![Value::Integer(1)]]
        } else {
            let working = ctx.get("r").expect("recursive working set");
            working_sets.push(std::sync::Arc::as_ptr(working));
            let current = match &working.result.rows[0][0] {
                Value::Integer(current) => *current,
                _ => panic!("expected integer working row"),
            };
            if current < 3 {
                vec![vec![Value::Integer(current + 1)]]
            } else {
                Vec::new()
            }
        };
        Ok(CteRows::binary(QueryResult {
            columns: vec!["n".into()],
            rows,
        }))
    })
    .unwrap();

    assert_eq!(
        result.result.rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ]
    );
    assert_eq!(working_sets.len(), 3);
    assert!(working_sets.windows(2).all(|pair| pair[0] != pair[1]));
}

#[test]
fn general_recursive_cte_stops_while_cloning_its_working_set() {
    let token = citadel::CancelToken::new();
    let _cancel = super::super::cancel_on_nth_cte_row(token.clone(), 2);
    let cte = general_recursive_cte();
    let mut calls = 0usize;

    let outcome =
        materialize_recursive_cte(&cte, &CteContext::default(), Some(&token), &mut |_, _| {
            calls += 1;
            assert_eq!(calls, 1, "the recursive arm ran after cancellation");
            Ok(CteRows::binary(QueryResult {
                columns: vec!["n".into()],
                rows: (0..1_024).map(|n| vec![Value::Integer(n)]).collect(),
            }))
        });

    assert_interrupted(outcome);
    assert_eq!(calls, 1);
}

#[test]
fn general_recursive_cte_stops_while_appending_new_rows() {
    let token = citadel::CancelToken::new();
    // One clone builds the initial working set; the third cloned row is the
    // second row appended from the recursive result.
    let _cancel = super::super::cancel_on_nth_cte_row(token.clone(), 3);
    let cte = general_recursive_cte();
    let mut calls = 0usize;

    let outcome =
        materialize_recursive_cte(&cte, &CteContext::default(), Some(&token), &mut |_, _| {
            calls += 1;
            let rows = match calls {
                1 => vec![vec![Value::Integer(0)]],
                2 => (1..=1_024).map(|n| vec![Value::Integer(n)]).collect(),
                _ => panic!("the next recursive iteration ran after cancellation"),
            };
            Ok(CteRows::binary(QueryResult {
                columns: vec!["n".into()],
                rows,
            }))
        });

    assert_interrupted(outcome);
    assert_eq!(calls, 2);
}

#[test]
fn aggregate_cte_filter_stops_when_cancelled_during_materialization() {
    let token = citadel::CancelToken::new();
    let _cancel = super::super::cancel_on_nth_cte_row(token.clone(), 2);
    let cte = many_cte_rows();
    let mut stmt = empty_select("c");
    stmt.columns = vec![SelectColumn::Expr {
        expr: Expr::CountStar,
        alias: None,
    }];
    stmt.where_clause = Some(Expr::Literal(Value::Boolean(true)));

    let outcome = exec_select_from_cte(
        &cte,
        &stmt,
        &mut |_| panic!("the statement has no subquery"),
        Some(&token),
    );

    assert_interrupted(outcome);
    assert!(token.is_cancelled());
}

#[test]
fn ordinary_cte_clone_stops_when_cancelled_during_materialization() {
    let token = citadel::CancelToken::new();
    let _cancel = super::super::cancel_on_nth_cte_row(token.clone(), 2);
    let cte = many_cte_rows();

    let outcome = exec_select_from_cte(
        &cte,
        &empty_select("c"),
        &mut |_| panic!("the statement has no subquery"),
        Some(&token),
    );

    assert_interrupted(outcome);
    assert!(token.is_cancelled());
}

#[test]
fn joined_cte_clone_stops_when_cancelled_during_resolution() {
    fn unexpected_table_scan(_: &str) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        panic!("a materialized CTE must not scan a table")
    }

    let token = citadel::CancelToken::new();
    let _cancel = super::super::cancel_on_nth_cte_row(token.clone(), 2);
    let mut ctes = CteContext::default();
    ctes.insert("c".into(), many_cte_rows().shared());
    let mut unexpected_scan = unexpected_table_scan;

    let outcome =
        super::super::resolve_table_or_cte("c", &ctes, &mut unexpected_scan, Some(&token));

    assert_interrupted(outcome);
    assert!(token.is_cancelled());
}

#[test]
fn cte_aggregate_filter_passes_cancellation_into_scalar_evaluation() {
    let cte = CteRows::binary(QueryResult {
        columns: vec!["body".into()],
        rows: vec![vec![Value::Text("several words to tokenize".into())]],
    });
    let mut stmt = empty_select("c");
    stmt.columns = vec![SelectColumn::Expr {
        expr: Expr::CountStar,
        alias: None,
    }];
    stmt.where_clause = Some(Expr::IsNotNull(Box::new(Expr::Function {
        name: "TO_TSVECTOR".into(),
        args: vec![Expr::Column("body".into())],
        distinct: false,
    })));
    let token = citadel::CancelToken::new();
    let _cancel = crate::fts::cancel_tokenize_after(token.clone(), 1);

    let outcome = exec_select_from_cte(
        &cte,
        &stmt,
        &mut |_| panic!("the statement has no subquery"),
        Some(&token),
    );

    assert_interrupted(outcome);
}

#[test]
fn cte_schema_count_boundary_preserves_wide_metadata_and_returns_typed_error() {
    for count in [32768, 65535, 65536] {
        let rows = CteRows::binary(QueryResult {
            columns: vec!["x".into(); count],
            rows: vec![],
        });
        let result = build_cte_schema("wide", &rows);
        if count <= 65535 {
            let schema = result.unwrap();
            assert_eq!(schema.columns.len(), count);
            assert_eq!(usize::from(schema.columns[count - 1].position), count - 1);
        } else {
            assert!(
                matches!(result, Err(SqlError::InvalidValue(message)) if message.contains("65535"))
            );
        }
    }
}
