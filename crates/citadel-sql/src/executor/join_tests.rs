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
fn join_key_hash_preserves_selected_column_order() {
    let row = vec![i(1), i(2), i(3), i(4)];
    assert_eq!(
        join_key_hash(&row, &[2, 0], &[]),
        join_key_hash(&[i(3), i(1)], &[0, 1], &[])
    );
    assert_ne!(
        join_key_hash(&row, &[2, 0], &[]),
        join_key_hash(&row, &[0, 2], &[])
    );
}

#[test]
fn join_key_hash_empty_tuple_is_independent_of_row_contents() {
    let row = vec![i(1), i(2)];
    assert_eq!(join_key_hash(&row, &[], &[]), join_key_hash(&[], &[], &[]));
}

/// A collated key column folds, so two spellings the collation calls equal produce one key
/// and land in the same hash bucket.
#[test]
fn join_key_hash_folds_a_collated_column() {
    let upper = vec![Value::Text("A".into()), i(1)];
    let lower = vec![Value::Text("a".into()), i(2)];
    let colls = [crate::types::Collation::NoCase];

    assert_eq!(
        join_key_hash(&upper, &[0], &colls),
        join_key_hash(&lower, &[0], &colls)
    );
    assert_ne!(
        join_key_hash(&upper, &[0], &[crate::types::Collation::Binary]),
        join_key_hash(&lower, &[0], &[crate::types::Collation::Binary])
    );
}

#[test]
fn join_key_hash_collision_does_not_imply_numeric_equality() {
    let first = i(9_007_199_254_740_992);
    let second = i(9_007_199_254_740_993);
    let real = Value::Real(9_007_199_254_740_992.0);
    assert_ne!(first, second);
    assert_eq!(first, real);
    assert_eq!(second, real);
    assert_eq!(
        join_key_hash(&[first], &[0], &[]),
        join_key_hash(&[second], &[0], &[])
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

fn scalar_join_rows(
    outer: &[Vec<Value>],
    inner: &[Vec<Value>],
    join: &JoinClause,
    columns: &[ColumnDef],
) -> Vec<Vec<Value>> {
    let column_map = ColumnMap::new(columns);
    let mut rows = Vec::new();
    let mut inner_matched = vec![false; inner.len()];
    for left in outer {
        let mut matched = false;
        for (index, right) in inner.iter().enumerate() {
            let combined: Vec<_> = left.iter().chain(right).cloned().collect();
            if is_truthy(
                &eval_expr(
                    join.on_clause.as_ref().unwrap(),
                    &EvalCtx::new(&column_map, &combined),
                )
                .unwrap(),
            ) {
                rows.push(combined);
                matched = true;
                inner_matched[index] = true;
            }
        }
        if !matched && matches!(join.join_type, JoinType::Left | JoinType::FullOuter) {
            let mut padded = left.clone();
            padded.resize(6, Value::Null);
            rows.push(padded);
        }
    }
    if matches!(join.join_type, JoinType::Right | JoinType::FullOuter) {
        for (index, right) in inner.iter().enumerate() {
            if !inner_matched[index] {
                let mut padded = vec![Value::Null; 3];
                padded.extend(right.iter().cloned());
                rows.push(padded);
            }
        }
    }
    rows
}

fn sorted_row_debug(rows: &[Vec<Value>]) -> Vec<String> {
    let mut rendered: Vec<_> = rows.iter().map(|row| format!("{row:?}")).collect();
    rendered.sort();
    rendered
}

fn check_numeric_join_paths(
    outer: Vec<Vec<Value>>,
    inner: Vec<Vec<Value>>,
    two_keys: bool,
    sorted_outer: bool,
) {
    let columns = cols(&[
        ("a.key", DataType::Null),
        ("a.guard", DataType::Integer),
        ("a.id", DataType::Integer),
        ("b.key", DataType::Null),
        ("b.guard", DataType::Integer),
        ("b.id", DataType::Integer),
    ]);
    let projected_columns = [2, 5, 2, 5, 0];
    let projection = build_combine_projection(&projected_columns, 3);
    for join_type in [
        JoinType::Inner,
        JoinType::Cross,
        JoinType::Left,
        JoinType::Right,
        JoinType::FullOuter,
    ] {
        for residual in [false, true] {
            let mut predicate = "a.key = b.key".to_string();
            if two_keys {
                predicate.push_str(" AND a.guard = b.guard");
            }
            if residual {
                predicate.push_str(" AND a.id < b.id");
            }
            let join = JoinClause {
                join_type,
                table: TableRef {
                    name: "b".into(),
                    alias: None,
                    args: None,
                },
                subquery: None,
                on_clause: Some(crate::parser::parse_sql_expr(&predicate).unwrap()),
            };
            let equi = compute_equi_join_meta(&join, &columns, 3);
            assert_eq!(equi.len(), if two_keys { 2 } else { 1 });
            assert_eq!(equi.is_pure(), !residual);
            let reference = scalar_join_rows(&outer, &inner, &join, &columns);
            let probe = build_probe_index(&inner, &equi, None).unwrap();
            if !residual && !two_keys {
                assert!(matches!(probe, ProbeIndex::Int(_)));
            }
            for projected in [false, true] {
                let expected = if projected && !residual {
                    reference
                        .iter()
                        .map(|row| {
                            projected_columns
                                .iter()
                                .map(|&index| row[index].clone())
                                .collect()
                        })
                        .collect()
                } else {
                    reference.clone()
                };
                let expected = sorted_row_debug(&expected);
                let projection = projected.then_some(&projection);
                for outer_pk in [None, Some(0)] {
                    if outer_pk.is_some() && !sorted_outer {
                        continue;
                    }
                    let mut owned_inner = inner.clone();
                    let owned = exec_join_step(
                        outer.clone(),
                        &mut owned_inner,
                        &join,
                        &columns,
                        3,
                        3,
                        outer_pk,
                        projection,
                        &equi,
                        None,
                    )
                    .unwrap();
                    assert_eq!(
                        sorted_row_debug(&owned),
                        expected,
                        "owned {join_type:?}, {predicate}, projected={projected}, pk={outer_pk:?}"
                    );
                    for (mode, cached) in [
                        ("borrowed", None),
                        ("cached", Some(&probe)),
                        ("reused", Some(&probe)),
                    ] {
                        let actual = exec_join_step_borrowed(
                            outer.clone(),
                            &inner,
                            &join,
                            &columns,
                            3,
                            3,
                            outer_pk,
                            projection,
                            &equi,
                            cached,
                            None,
                        )
                        .unwrap();
                        assert_eq!(sorted_row_debug(&actual), expected,
                            "{mode} {join_type:?}, {predicate}, projected={projected}, pk={outer_pk:?}");
                    }
                }
            }
        }
    }
}

#[test]
fn numeric_join_paths_match_real_outer_to_integer_inner() {
    check_numeric_join_paths(
        vec![vec![Value::Real(1.0), i(0), i(10)]],
        vec![vec![i(1), i(0), i(20)]],
        false,
        true,
    );
}

#[test]
fn numeric_join_paths_recheck_colliding_large_integer_pairs() {
    let first = 9_007_199_254_740_992;
    check_numeric_join_paths(
        vec![vec![i(first), i(0), i(10)], vec![i(first + 1), i(0), i(30)]],
        vec![vec![i(first), i(0), i(20)], vec![i(first + 1), i(0), i(25)]],
        true,
        false,
    );
}

#[test]
fn numeric_join_paths_keep_both_large_integer_matches_for_real_probe() {
    let first = 9_007_199_254_740_992;
    let real = Value::Real(first as f64);
    assert_ne!(i(first), i(first + 1));
    assert_eq!(real, i(first));
    assert_eq!(real, i(first + 1));
    check_numeric_join_paths(
        vec![vec![real, i(0), i(10)]],
        vec![vec![i(first), i(0), i(20)], vec![i(first + 1), i(0), i(30)]],
        true,
        false,
    );
}

#[test]
fn numeric_join_paths_pad_unmatched_collisions_and_null_keys() {
    let first = 9_007_199_254_740_992;
    check_numeric_join_paths(
        vec![vec![i(first), i(0), i(10)], vec![Value::Null, i(0), i(11)]],
        vec![
            vec![i(first + 1), i(0), i(20)],
            vec![Value::Null, i(0), i(21)],
        ],
        true,
        false,
    );
}

#[test]
fn numeric_join_paths_do_not_drop_late_real_outer_rows() {
    check_numeric_join_paths(
        vec![vec![i(1), i(0), i(10)], vec![Value::Real(2.0), i(0), i(30)]],
        vec![vec![i(1), i(0), i(20)], vec![i(2), i(0), i(25)]],
        false,
        false,
    );
}

#[test]
fn numeric_join_paths_preserve_repeated_projected_columns() {
    check_numeric_join_paths(
        vec![vec![i(1), i(0), i(10)], vec![i(2), i(0), i(30)]],
        vec![vec![i(1), i(0), i(20)], vec![i(2), i(0), i(40)]],
        false,
        true,
    );
}

#[test]
fn interval_probe_hashes_normalize_before_collation_folding() {
    for (left, right) in [
        ((1, 0, 0), (0, 30, 0)),
        ((0, 1, 0), (0, 0, 86_400_000_000)),
        ((1, -30, 0), (0, 0, 0)),
        ((i32::MAX - 1, 0, i64::MAX), (i32::MAX, -30, i64::MAX)),
        ((i32::MIN + 1, 0, i64::MIN), (i32::MIN, 30, i64::MIN)),
    ] {
        let value = |(months, days, micros)| Value::Interval {
            months,
            days,
            micros,
        };
        let left = vec![value(left), i(7)];
        let right = vec![value(right), i(7)];
        for collation in [Collation::Binary, Collation::NoCase, Collation::Rtrim] {
            assert!(crate::eval::collated_eq(&left[0], &right[0], Some(collation)).unwrap());
            for columns in [&[0][..], &[0, 1][..]] {
                assert_eq!(
                    join_key_hash(&left, columns, &[collation, Collation::Binary]),
                    join_key_hash(&right, columns, &[collation, Collation::Binary]),
                    "{left:?}/{right:?}, {collation:?}, {columns:?}"
                );
            }
        }
    }
}

fn projected_text(label: &str) -> Value {
    Value::Text(format!("{label}:{}", "payload".repeat(16)).into())
}

fn text_pointer(value: &Value) -> *const u8 {
    match value {
        Value::Text(text) => text.as_ptr(),
        _ => panic!("expected text"),
    }
}

#[test]
fn projected_outer_reuses_sized_buffer_and_moves_text() {
    let mut outer = Vec::with_capacity(4);
    outer.extend([i(1), projected_text("outer"), i(3)]);
    let allocation = outer.as_ptr();
    let payload = text_pointer(&outer[1]);
    let inner = vec![projected_text("inner")];
    let before = inner.clone();
    let projection = build_combine_projection(&[1, 3], 3);
    let result = projection.finish_outer(outer, Some(&inner));
    assert_eq!(
        result,
        vec![projected_text("outer"), projected_text("inner")]
    );
    assert_eq!(result.as_ptr(), allocation);
    assert_eq!(result.capacity(), 4);
    assert_eq!(text_pointer(&result[0]), payload);
    assert_ne!(text_pointer(&result[1]), text_pointer(&inner[0]));
    assert_eq!(inner, before);
}

#[test]
fn projected_outer_compacts_sparse_prefix_and_inner_only_output() {
    let outer = vec![
        i(0),
        projected_text("first"),
        i(2),
        projected_text("second"),
        i(4),
    ];
    let allocation = outer.as_ptr();
    let payloads = [text_pointer(&outer[1]), text_pointer(&outer[3])];
    let projection = build_combine_projection(&[1, 3, 5], 5);
    let actual = projection.finish_outer(outer, Some(&[i(9)]));
    assert_eq!(
        actual,
        vec![projected_text("first"), projected_text("second"), i(9)]
    );
    assert_eq!(actual.as_ptr(), allocation);
    assert_eq!(text_pointer(&actual[0]), payloads[0]);
    assert_eq!(text_pointer(&actual[1]), payloads[1]);

    let outer = vec![projected_text("unused")];
    let allocation = outer.as_ptr();
    let projection = build_combine_projection(&[1], 1);
    let actual = projection.finish_outer(outer, Some(&[i(7)]));
    assert_eq!(actual, vec![i(7)]);
    assert_eq!(actual.as_ptr(), allocation);
}

#[test]
fn projected_outer_compacts_oversized_and_undersized_buffers() {
    for capacity in [1, 128] {
        let mut outer = Vec::with_capacity(capacity);
        outer.push(projected_text("outer"));
        let allocation = outer.as_ptr();
        let payload = text_pointer(&outer[0]);
        let projection = build_combine_projection(&[0, 1, 2], 1);
        let result = projection.finish_outer(outer, Some(&[i(2), i(3)]));
        assert_eq!(result, vec![projected_text("outer"), i(2), i(3)]);
        assert_ne!(result.as_ptr(), allocation);
        assert!(result.capacity() <= 6);
        assert_eq!(text_pointer(&result[0]), payload);
    }
    let projection = build_combine_projection(&[], 2);
    let empty = projection.finish_outer(vec![i(1), i(2)], Some(&[i(3)]));
    assert!(empty.is_empty());
    assert_eq!(empty.capacity(), 0);
}

#[test]
fn projected_outer_moves_unique_nonmonotone_sources() {
    let outer = vec![projected_text("first"), projected_text("second")];
    let first = text_pointer(&outer[0]);
    let second = text_pointer(&outer[1]);
    let projection = build_combine_projection(&[1, 2, 0], 2);
    assert!(projection.outer_prefix.is_none());
    let result = projection.finish_outer(outer, Some(&[i(7)]));
    assert_eq!(
        result,
        vec![projected_text("second"), i(7), projected_text("first")]
    );
    assert_eq!(text_pointer(&result[0]), second);
    assert_eq!(text_pointer(&result[2]), first);
}

#[test]
fn projected_outer_preserves_repeated_sources_and_null_padding() {
    for columns in [&[1, 2, 1, 2, 0][..], &[0, 0, 2][..], &[1, 2, 2][..]] {
        let projection = build_combine_projection(columns, 2);
        let outer = vec![projected_text("first"), projected_text("second")];
        let inner = vec![projected_text("inner")];
        let expected = combine_row_projected(&outer, &inner, &projection);
        assert_eq!(
            projection.finish_outer(outer.clone(), Some(&inner)),
            expected
        );
        let padded = combine_row_projected(&outer, &[Value::Null], &projection);
        assert_eq!(projection.finish_outer(outer, None), padded);
        let padded = combine_row_projected(&[Value::Null, Value::Null], &inner, &projection);
        assert_eq!(projection.unmatched_inner(&inner), padded);
    }
    let mut outer = Vec::with_capacity(4);
    outer.extend([i(0), projected_text("outer")]);
    let allocation = outer.as_ptr();
    let payload = text_pointer(&outer[1]);
    let inner = vec![projected_text("inner")];
    let projection = build_combine_projection(&[1, 2, 2], 2);
    let expected = combine_row_projected(&outer, &inner, &projection);
    let actual = projection.finish_outer(outer, Some(&inner));
    assert_eq!(actual, expected);
    assert_eq!(actual.as_ptr(), allocation);
    assert_eq!(text_pointer(&actual[0]), payload);
    assert_ne!(text_pointer(&actual[1]), text_pointer(&inner[0]));
    assert_ne!(text_pointer(&actual[2]), text_pointer(&inner[0]));
}

#[test]
fn borrowed_join_fanout_reuses_last_outer_without_mutating_inner() {
    let columns = cols(&[
        ("a.key", DataType::Null),
        ("a.guard", DataType::Integer),
        ("a.value", DataType::Text),
        ("b.key", DataType::Null),
        ("b.guard", DataType::Integer),
        ("b.value", DataType::Text),
    ]);
    let projection = build_combine_projection(&[2, 5], 3);
    for mode in ["integer", "composite", "temporal"] {
        let key = |number| {
            if mode == "temporal" {
                Value::Date(number)
            } else {
                i(i64::from(number))
            }
        };
        let inner_key = |number| {
            if mode == "temporal" {
                Value::Text(Value::Date(number).to_string().into())
            } else {
                i(i64::from(number))
            }
        };
        let inner = vec![
            vec![inner_key(1), i(0), projected_text("match-one")],
            vec![inner_key(1), i(0), projected_text("match-two")],
            vec![inner_key(3), i(0), projected_text("unmatched-inner")],
            vec![Value::Null, i(0), projected_text("null-inner")],
        ];
        let inner_before = inner.clone();
        for join_type in [
            JoinType::Inner,
            JoinType::Cross,
            JoinType::Left,
            JoinType::Right,
            JoinType::FullOuter,
        ] {
            let join = JoinClause {
                join_type,
                table: TableRef {
                    name: "b".into(),
                    alias: None,
                    args: None,
                },
                subquery: None,
                on_clause: Some(
                    crate::parser::parse_sql_expr(if mode == "composite" {
                        "a.key = b.key AND a.guard = b.guard"
                    } else {
                        "a.key = b.key"
                    })
                    .unwrap(),
                ),
            };
            let equi = compute_equi_join_meta(&join, &columns, 3);
            let probe = build_probe_index(&inner, &equi, None).unwrap();
            for cached in [None, Some(&probe), Some(&probe)] {
                let outer = vec![
                    vec![key(1), i(0), projected_text("matched-outer")],
                    vec![key(2), i(0), projected_text("unmatched-outer")],
                    vec![Value::Null, i(0), projected_text("null-outer")],
                ];
                let allocation = outer[0].as_ptr();
                let payload = text_pointer(&outer[0][2]);
                let expected: Vec<_> = scalar_join_rows(&outer, &inner, &join, &columns)
                    .iter()
                    .map(|row| vec![row[2].clone(), row[5].clone()])
                    .collect();
                let actual = exec_join_step_borrowed(
                    outer,
                    &inner,
                    &join,
                    &columns,
                    3,
                    3,
                    None,
                    Some(&projection),
                    &equi,
                    cached,
                    None,
                )
                .unwrap();
                assert_eq!(actual, expected, "{mode}, {join_type:?}");
                assert_eq!(actual[1].as_ptr(), allocation);
                assert_eq!(text_pointer(&actual[1][0]), payload);
                assert_ne!(text_pointer(&actual[0][0]), payload);
                assert_eq!(inner, inner_before);
            }
        }
    }
}

#[test]
fn generic_projected_join_reuses_last_accepted_not_last_candidate() {
    let first = 1i64 << 53;
    let columns = cols(&[
        ("a.key", DataType::Integer),
        ("a.guard", DataType::Integer),
        ("a.value", DataType::Text),
        ("b.key", DataType::Integer),
        ("b.guard", DataType::Integer),
        ("b.value", DataType::Text),
    ]);
    let join = JoinClause {
        join_type: JoinType::Inner,
        table: TableRef {
            name: "b".into(),
            alias: None,
            args: None,
        },
        subquery: None,
        on_clause: Some(
            crate::parser::parse_sql_expr("a.key = b.key AND a.guard = b.guard").unwrap(),
        ),
    };
    let equi = compute_equi_join_meta(&join, &columns, 3);
    let outer = vec![vec![i(first), i(0), projected_text("outer")]];
    let allocation = outer[0].as_ptr();
    let inner = vec![
        vec![i(first), i(0), projected_text("match")],
        vec![i(first + 1), i(0), projected_text("collision")],
    ];
    let projection = build_combine_projection(&[2, 5], 3);
    let actual = exec_join_step_borrowed(
        outer,
        &inner,
        &join,
        &columns,
        3,
        3,
        None,
        Some(&projection),
        &equi,
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        actual,
        vec![vec![projected_text("outer"), projected_text("match")]]
    );
    assert_eq!(actual[0].as_ptr(), allocation);
}

#[test]
fn sorted_borrowed_integer_join_reuses_projected_outer_rows() {
    for has_null in [false, true] {
        let mut inner = vec![
            vec![i(1), i(0), projected_text("first")],
            vec![i(1), i(0), projected_text("second")],
            vec![i(3), i(0), projected_text("third")],
        ];
        if has_null {
            inner.push(vec![Value::Null, i(0), projected_text("null")]);
        }
        let before = inner.clone();
        let outer = vec![
            vec![i(1), i(0), projected_text("outer-one")],
            vec![i(3), i(0), projected_text("outer-three")],
        ];
        let allocations = [outer[0].as_ptr(), outer[1].as_ptr()];
        let projection = build_combine_projection(&[2, 5], 3);
        let actual = try_integer_join_borrowed(
            outer,
            &inner,
            &JoinType::Inner,
            0,
            0,
            3,
            3,
            true,
            Some(&projection),
            &mut JoinCancel::new(None).unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            actual,
            vec![
                vec![projected_text("outer-one"), projected_text("first")],
                vec![projected_text("outer-one"), projected_text("second")],
                vec![projected_text("outer-three"), projected_text("third")],
            ]
        );
        assert_eq!(actual[1].as_ptr(), allocations[0]);
        assert_eq!(actual[2].as_ptr(), allocations[1]);
        assert_eq!(inner, before);
    }
}

#[test]
fn empty_generic_joins_do_not_reserve_result_rows() {
    let first = 1i64 << 53;
    let columns = cols(&[
        ("a.key", DataType::Integer),
        ("a.guard", DataType::Integer),
        ("b.key", DataType::Integer),
        ("b.guard", DataType::Integer),
    ]);
    let join = JoinClause {
        join_type: JoinType::Inner,
        table: TableRef {
            name: "b".into(),
            alias: None,
            args: None,
        },
        subquery: None,
        on_clause: Some(
            crate::parser::parse_sql_expr("a.key = b.key AND a.guard = b.guard").unwrap(),
        ),
    };
    let equi = compute_equi_join_meta(&join, &columns, 2);
    let inner = vec![vec![i(first), i(0)]];
    let probe = build_probe_index(&inner, &equi, None).unwrap();
    let projection = build_combine_projection(&[0, 2], 2);
    for key in [i(first + 1), i(99), Value::Null] {
        for selected in [None, Some(&projection)] {
            for cached in [None, Some(&probe)] {
                let result = exec_join_step_borrowed(
                    vec![vec![key.clone(), i(0)]],
                    &inner,
                    &join,
                    &columns,
                    2,
                    2,
                    None,
                    selected,
                    &equi,
                    cached,
                    None,
                )
                .unwrap();
                assert!(result.is_empty());
                assert_eq!(result.capacity(), 0);
            }
        }
    }
}
