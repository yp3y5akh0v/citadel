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

fn schema(name: &str, cs: Vec<ColumnDef>) -> TableSchema {
    TableSchema::new(name.into(), cs, vec![], vec![], vec![], vec![])
}

fn i(n: i64) -> Value {
    Value::Integer(n)
}

#[test]
fn table_alias_or_name_uses_alias_when_present() {
    assert_eq!(table_alias_or_name("customers", &Some("c".into())), "c");
}

#[test]
fn table_alias_or_name_falls_back_to_table_name_lowercased() {
    assert_eq!(table_alias_or_name("Customers", &None), "customers");
}

#[test]
fn build_joined_columns_two_tables_prefixes_with_alias() {
    let a = schema("a", cols(&[("x", DataType::Integer)]));
    let b = schema("b", cols(&[("y", DataType::Text)]));
    let result = build_joined_columns(&[("a".into(), &a), ("b".into(), &b)]);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].name, "a.x");
    assert_eq!(result[1].name, "b.y");
}

#[test]
fn build_joined_columns_alias_lowercased() {
    let a = schema("a", cols(&[("x", DataType::Integer)]));
    let result = build_joined_columns(&[("UpperAlias".into(), &a)]);
    assert_eq!(result[0].name, "upperalias.x");
}

#[test]
fn build_joined_columns_position_is_sequential() {
    let a = schema(
        "a",
        cols(&[("x", DataType::Integer), ("y", DataType::Text)]),
    );
    let b = schema("b", cols(&[("z", DataType::Integer)]));
    let result = build_joined_columns(&[("a".into(), &a), ("b".into(), &b)]);
    for (i, c) in result.iter().enumerate() {
        assert_eq!(c.position as usize, i);
    }
}

#[test]
fn build_joined_columns_data_types_preserved() {
    let a = schema(
        "a",
        cols(&[("x", DataType::Integer), ("y", DataType::Text)]),
    );
    let result = build_joined_columns(&[("a".into(), &a)]);
    assert_eq!(result[0].data_type, DataType::Integer);
    assert_eq!(result[1].data_type, DataType::Text);
}

#[test]
fn extend_joined_columns_appends_alias_prefixed() {
    let mut out: Vec<ColumnDef> = vec![];
    let a = schema("a", cols(&[("x", DataType::Integer)]));
    extend_joined_columns(&mut out, &("a".into(), &a));
    assert_eq!(out[0].name, "a.x");
    assert_eq!(out[0].position, 0);
}

#[test]
fn extend_joined_columns_continues_position_count() {
    let a = schema(
        "a",
        cols(&[("x", DataType::Integer), ("y", DataType::Integer)]),
    );
    let mut out: Vec<ColumnDef> = build_joined_columns(&[("a".into(), &a)]);
    let b = schema("b", cols(&[("z", DataType::Integer)]));
    extend_joined_columns(&mut out, &("b".into(), &b));
    assert_eq!(out.len(), 3);
    assert_eq!(out[2].name, "b.z");
    assert_eq!(out[2].position, 2);
}

#[test]
fn resolve_col_idx_unqualified_unique() {
    let cs = cols(&[("a.x", DataType::Integer), ("a.y", DataType::Integer)]);
    let r = resolve_col_idx(&Expr::Column("x".into()), &cs);
    assert_eq!(r, Some(0));
}

#[test]
fn resolve_col_idx_ambiguous_returns_none() {
    let cs = cols(&[("a.x", DataType::Integer), ("b.x", DataType::Integer)]);
    let r = resolve_col_idx(&Expr::Column("x".into()), &cs);
    assert_eq!(r, None);
}

#[test]
fn resolve_col_idx_qualified_finds_exact_match() {
    let cs = cols(&[("a.x", DataType::Integer), ("b.x", DataType::Integer)]);
    let r = resolve_col_idx(
        &Expr::QualifiedColumn {
            table: "b".into(),
            column: "x".into(),
        },
        &cs,
    );
    assert_eq!(r, Some(1));
}

#[test]
fn resolve_col_idx_unknown_returns_none() {
    let cs = cols(&[("a.x", DataType::Integer)]);
    assert_eq!(resolve_col_idx(&Expr::Column("missing".into()), &cs), None);
}

#[test]
fn hash_key_extracts_indices_in_order() {
    let row = vec![i(1), i(2), i(3), i(4)];
    let key = hash_key(&row, &[2, 0], &[]);
    assert_eq!(key, vec![i(3), i(1)]);
}

#[test]
fn hash_key_empty_indices_yields_empty_key() {
    let row = vec![i(1), i(2)];
    let key = hash_key(&row, &[], &[]);
    assert!(key.is_empty());
}

/// A collated key column folds, so two spellings the collation calls equal produce one key
/// and land in the same hash bucket.
#[test]
fn hash_key_folds_a_collated_column() {
    let upper = vec![Value::Text("A".into()), i(1)];
    let lower = vec![Value::Text("a".into()), i(2)];
    let colls = [crate::types::Collation::NoCase];

    assert_eq!(
        hash_key(&upper, &[0], &colls),
        hash_key(&lower, &[0], &colls)
    );
    assert_ne!(
        hash_key(&upper, &[0], &[crate::types::Collation::Binary]),
        hash_key(&lower, &[0], &[crate::types::Collation::Binary])
    );
}

/// The syntactic left operand supplies the collation even when it is BINARY. Join build/probe
/// order must not change the comparison that the ON expression denotes.
#[test]
fn equi_key_collations_follow_syntactic_left_precedence() {
    let mut cols = cols(&[("l", DataType::Text), ("r", DataType::Text)]);
    cols[1].collation = crate::types::Collation::NoCase;
    assert_eq!(
        equi_key_collations(
            &[KeyPair {
                outer: 0,
                inner: 0,
                left_is_outer: true,
            }],
            &cols,
            1,
        ),
        vec![crate::types::Collation::Binary],
        "a BINARY left operand still wins over a NOCASE right operand"
    );
    assert_eq!(
        equi_key_collations(
            &[KeyPair {
                outer: 0,
                inner: 0,
                left_is_outer: false,
            }],
            &cols,
            1,
        ),
        vec![crate::types::Collation::NoCase],
        "the inner column wins when it was written on the left"
    );

    cols[0].collation = crate::types::Collation::Rtrim;
    assert_eq!(
        equi_key_collations(
            &[KeyPair {
                outer: 0,
                inner: 0,
                left_is_outer: true,
            }],
            &cols,
            1,
        ),
        vec![crate::types::Collation::Rtrim],
        "a collated left wins"
    );
}

#[test]
fn equi_key_collations_offsets_inner_columns_past_the_outer_row() {
    let mut cols = cols(&[
        ("outer_id", DataType::Integer),
        ("outer_text", DataType::Text),
        ("inner_id", DataType::Integer),
        ("inner_text", DataType::Text),
    ]);
    cols[3].collation = crate::types::Collation::NoCase;

    assert_eq!(
        equi_key_collations(
            &[KeyPair {
                outer: 1,
                inner: 1,
                left_is_outer: false,
            }],
            &cols,
            2,
        ),
        vec![crate::types::Collation::NoCase]
    );
}

#[test]
fn count_conjuncts_single() {
    assert_eq!(count_conjuncts(&Expr::Literal(i(1))), 1);
}

#[test]
fn count_conjuncts_nested_and() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("a".into())),
            op: BinOp::And,
            right: Box::new(Expr::Column("b".into())),
        }),
        op: BinOp::And,
        right: Box::new(Expr::Column("c".into())),
    };
    assert_eq!(count_conjuncts(&e), 3);
}

#[test]
fn count_conjuncts_or_does_not_split() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Or,
        right: Box::new(Expr::Column("b".into())),
    };
    assert_eq!(count_conjuncts(&e), 1);
}

fn cancellable_integer_join() -> (JoinClause, Vec<ColumnDef>, EquiJoin) {
    let combined = cols(&[("a.id", DataType::Integer), ("b.id", DataType::Integer)]);
    let join = JoinClause {
        join_type: JoinType::Inner,
        table: TableRef {
            name: "b".into(),
            alias: None,
            args: None,
        },
        subquery: None,
        on_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::QualifiedColumn {
                table: "a".into(),
                column: "id".into(),
            }),
            op: BinOp::Eq,
            right: Box::new(Expr::QualifiedColumn {
                table: "b".into(),
                column: "id".into(),
            }),
        }),
    };
    let equi = compute_equi_join_meta(&join, &combined, 1);
    (join, combined, equi)
}

fn assert_interrupted(err: SqlError) {
    assert!(
        matches!(err, SqlError::Storage(citadel_core::Error::Interrupted)),
        "got {err:?}"
    );
}

#[test]
fn materialized_integer_join_observes_an_async_cancel() {
    let (join, combined, equi) = cancellable_integer_join();
    // One key with many matches forces the integer lane to stay in its output
    // loop long after both inputs have been materialized.
    let outer = vec![vec![i(7)]; 1024];
    let mut inner = vec![vec![i(7)]; 1024];
    let token = citadel::CancelToken::new();
    let trip = token.clone();
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(1));
        trip.cancel();
    });

    let err = exec_join_step(
        outer,
        &mut inner,
        &join,
        &combined,
        1,
        1,
        None,
        None,
        &equi,
        Some(&token),
    )
    .expect_err("the materialized join ignored cancellation");
    stopper.join().unwrap();
    assert_interrupted(err);
}

#[test]
fn cached_borrowed_integer_join_observes_an_async_cancel() {
    let (join, combined, equi) = cancellable_integer_join();
    let outer = vec![vec![i(7)]; 1024];
    let inner = vec![vec![i(7)]; 1024];
    let probe = build_probe_index(&inner, &equi, None).unwrap();
    assert!(matches!(probe, ProbeIndex::Int(_)));

    let token = citadel::CancelToken::new();
    let trip = token.clone();
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(1));
        trip.cancel();
    });

    let err = exec_join_step_borrowed(
        outer,
        &inner,
        &join,
        &combined,
        1,
        1,
        None,
        None,
        &equi,
        Some(&probe),
        Some(&token),
    )
    .expect_err("the cached borrowed join ignored cancellation");
    stopper.join().unwrap();
    assert_interrupted(err);
}

#[test]
fn combine_row_concatenates() {
    let combined = combine_row(&[i(1), i(2)], &[i(3), i(4)], 4);
    assert_eq!(combined, vec![i(1), i(2), i(3), i(4)]);
}

#[test]
fn extract_equi_join_keys_simple_equi() {
    let combined = cols(&[("a.id", DataType::Integer), ("b.a_id", DataType::Integer)]);
    let on = Expr::BinaryOp {
        left: Box::new(Expr::QualifiedColumn {
            table: "a".into(),
            column: "id".into(),
        }),
        op: BinOp::Eq,
        right: Box::new(Expr::QualifiedColumn {
            table: "b".into(),
            column: "a_id".into(),
        }),
    };
    let pairs = extract_equi_join_keys(&on, &combined, 1);
    assert_eq!(
        pairs,
        vec![KeyPair {
            outer: 0,
            inner: 0,
            left_is_outer: true,
        }]
    );
}

#[test]
fn extract_equi_join_keys_non_equi_returns_empty() {
    let combined = cols(&[("a.id", DataType::Integer), ("b.a_id", DataType::Integer)]);
    let on = Expr::BinaryOp {
        left: Box::new(Expr::QualifiedColumn {
            table: "a".into(),
            column: "id".into(),
        }),
        op: BinOp::Lt,
        right: Box::new(Expr::QualifiedColumn {
            table: "b".into(),
            column: "a_id".into(),
        }),
    };
    let pairs = extract_equi_join_keys(&on, &combined, 1);
    assert!(pairs.is_empty());
}

#[test]
fn residual_join_predicate_propagates_scalar_cancellation() {
    let combined = cols(&[("a.body", DataType::Text), ("b.body", DataType::Text)]);
    let vector = |table: &str| Expr::Function {
        name: "TO_TSVECTOR".into(),
        args: vec![Expr::QualifiedColumn {
            table: table.into(),
            column: "body".into(),
        }],
        distinct: false,
    };
    let join = JoinClause {
        join_type: JoinType::Inner,
        table: TableRef {
            name: "b".into(),
            alias: None,
            args: None,
        },
        subquery: None,
        on_clause: Some(Expr::BinaryOp {
            left: Box::new(vector("a")),
            op: BinOp::Eq,
            right: Box::new(vector("b")),
        }),
    };
    let equi = compute_equi_join_meta(&join, &combined, 1);
    assert!(equi.is_empty());
    let mut inner = vec![vec![Value::Text("inner words".into())]];
    let token = citadel::CancelToken::new();
    let _cancel = crate::fts::cancel_tokenize_after(token.clone(), 1);

    let err = exec_join_step(
        vec![vec![Value::Text("outer words".into())]],
        &mut inner,
        &join,
        &combined,
        1,
        1,
        None,
        None,
        &equi,
        Some(&token),
    )
    .expect_err("the residual ON expression discarded its cancellation token");

    assert_interrupted(err);
}
