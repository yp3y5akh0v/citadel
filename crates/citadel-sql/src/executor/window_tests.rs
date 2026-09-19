use super::*;
use crate::parser::{
    BinOp, Expr, SelectColumn, SelectStmt, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowSpec,
};
use crate::types::{Collation, ColumnDef, DataType, Value};

fn i(n: i64) -> Value {
    Value::Integer(n)
}

fn column(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type,
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

fn empty_window_fn(name: &str) -> Expr {
    Expr::WindowFunction {
        name: name.into(),
        args: vec![],
        spec: WindowSpec {
            partition_by: vec![],
            order_by: vec![],
            frame: None,
        },
    }
}

fn empty_select() -> SelectStmt {
    SelectStmt {
        columns: vec![],
        from: "t".into(),
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

#[test]
fn has_window_function_direct() {
    assert!(has_window_function(&empty_window_fn("ROW_NUMBER")));
}

#[test]
fn has_window_function_literal_false() {
    assert!(!has_window_function(&Expr::Literal(i(1))));
}

#[test]
fn has_window_function_column_false() {
    assert!(!has_window_function(&Expr::Column("x".into())));
}

#[test]
fn has_window_function_binary_op_propagates_left() {
    let e = Expr::BinaryOp {
        left: Box::new(empty_window_fn("ROW_NUMBER")),
        op: BinOp::Add,
        right: Box::new(Expr::Literal(i(1))),
    };
    assert!(has_window_function(&e));
}

#[test]
fn has_window_function_binary_op_propagates_right() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Literal(i(1))),
        op: BinOp::Add,
        right: Box::new(empty_window_fn("RANK")),
    };
    assert!(has_window_function(&e));
}

#[test]
fn has_window_function_inside_function_args() {
    let e = Expr::Function {
        name: "ABS".into(),
        args: vec![empty_window_fn("LAG")],
        distinct: false,
    };
    assert!(has_window_function(&e));
}

#[test]
fn has_window_function_in_case_branch() {
    let e = Expr::Case {
        operand: None,
        conditions: vec![(
            Expr::Literal(Value::Boolean(true)),
            empty_window_fn("ROW_NUMBER"),
        )],
        else_result: None,
    };
    assert!(has_window_function(&e));
}

#[test]
fn has_any_window_function_on_select() {
    let mut sel = empty_select();
    sel.columns = vec![SelectColumn::Expr {
        expr: empty_window_fn("ROW_NUMBER"),
        alias: None,
    }];
    assert!(has_any_window_function(&sel));
}

#[test]
fn has_any_window_function_no_window_columns_returns_false() {
    let mut sel = empty_select();
    sel.columns = vec![SelectColumn::Expr {
        expr: Expr::Column("x".into()),
        alias: None,
    }];
    assert!(!has_any_window_function(&sel));
}

#[test]
fn has_any_window_function_all_columns_ignored() {
    let mut sel = empty_select();
    sel.columns = vec![SelectColumn::AllColumns];
    assert!(!has_any_window_function(&sel));
}

#[test]
fn resolve_frame_explicit_passes_through() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::Preceding(Box::new(Expr::Literal(i(1)))),
        end: WindowFrameBound::Following(Box::new(Expr::Literal(i(1)))),
    };
    let spec = WindowSpec {
        partition_by: vec![],
        order_by: vec![],
        frame: Some(frame.clone()),
    };
    let r = resolve_frame(&spec);
    assert!(matches!(r.units, WindowFrameUnits::Rows));
}

#[test]
fn resolve_frame_default_no_order_by_unbounded() {
    let spec = WindowSpec {
        partition_by: vec![],
        order_by: vec![],
        frame: None,
    };
    let r = resolve_frame(&spec);
    assert!(matches!(r.start, WindowFrameBound::UnboundedPreceding));
    assert!(matches!(r.end, WindowFrameBound::UnboundedFollowing));
}

#[test]
fn resolve_frame_default_with_order_by_ends_at_current_row() {
    use crate::parser::OrderByItem;
    let spec = WindowSpec {
        partition_by: vec![],
        order_by: vec![OrderByItem {
            expr: Expr::Column("x".into()),
            output_name: None,
            output_ordinal: None,
            descending: false,
            nulls_first: None,
        }],
        frame: None,
    };
    let r = resolve_frame(&spec);
    assert!(matches!(r.start, WindowFrameBound::UnboundedPreceding));
    assert!(matches!(r.end, WindowFrameBound::CurrentRow));
}

#[test]
fn rows_frame_indices_unbounded_both_sides() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::UnboundedPreceding,
        end: WindowFrameBound::UnboundedFollowing,
    };
    assert_eq!(
        ResolvedFrame::new(&frame, None).unwrap().indices(2, 5, &[]),
        0..5
    );
}

#[test]
fn rows_frame_indices_current_row_only() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::CurrentRow,
        end: WindowFrameBound::CurrentRow,
    };
    assert_eq!(
        ResolvedFrame::new(&frame, None).unwrap().indices(2, 5, &[]),
        2..3
    );
}

#[test]
fn rows_frame_indices_preceding_following() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::Preceding(Box::new(Expr::Literal(i(1)))),
        end: WindowFrameBound::Following(Box::new(Expr::Literal(i(1)))),
    };
    assert_eq!(
        ResolvedFrame::new(&frame, None).unwrap().indices(2, 5, &[]),
        1..4
    );
}

#[test]
fn rows_frame_indices_preceding_clamps_to_zero() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::Preceding(Box::new(Expr::Literal(i(10)))),
        end: WindowFrameBound::CurrentRow,
    };
    assert_eq!(
        ResolvedFrame::new(&frame, None).unwrap().indices(2, 5, &[]),
        0..3
    );
}

#[test]
fn rows_frame_indices_following_clamps_to_partition_end() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::CurrentRow,
        end: WindowFrameBound::Following(Box::new(Expr::Literal(i(10)))),
    };
    assert_eq!(
        ResolvedFrame::new(&frame, None).unwrap().indices(2, 5, &[]),
        2..5
    );
}

#[test]
fn rows_frame_ranges_match_membership_at_every_partition_boundary() {
    for n in 0..8 {
        for start in [
            -i128::from(i64::MAX),
            -9,
            -2,
            -1,
            0,
            1,
            2,
            9,
            i128::from(i64::MAX),
        ] {
            for end in [
                -i128::from(i64::MAX),
                -9,
                -2,
                -1,
                0,
                1,
                2,
                9,
                i128::from(i64::MAX),
            ] {
                let frame = ResolvedFrame::Rows {
                    start: Some(start),
                    end: Some(end),
                    sliding: false,
                };
                for pos in 0..n.max(1) {
                    let expected: Vec<_> = (0..n)
                        .filter(|&row| {
                            (pos as i128 + start..=pos as i128 + end).contains(&(row as i128))
                        })
                        .collect();
                    let range = frame.indices(pos, n, &[]);
                    assert!(range.start <= range.end && range.end <= n);
                    assert_eq!(
                        range.collect::<Vec<_>>(),
                        expected,
                        "n={n}, pos={pos}, start={start}, end={end}"
                    );
                }
            }
        }
    }
}

#[test]
fn rows_frame_bounds_remain_valid_near_usize_max() {
    let frame = ResolvedFrame::Rows {
        start: Some(1),
        end: Some(i128::from(i64::MAX)),
        sliding: false,
    };
    assert_eq!(
        frame.indices(usize::MAX - 1, usize::MAX, &[]),
        usize::MAX..usize::MAX
    );
}

#[test]
fn window_frame_rejects_row_dependencies_in_unevaluated_branches() {
    for sql in [
        "CASE WHEN TRUE THEN 1 ELSE x END",
        "COALESCE(1, SUM(1))",
        "CASE x WHEN 0 THEN 1 ELSE 1 END",
    ] {
        let frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowFrameBound::Preceding(Box::new(
                crate::parser::parse_sql_expr(sql).unwrap(),
            )),
            end: WindowFrameBound::CurrentRow,
        };
        assert!(ResolvedFrame::new(&frame, None).is_err(), "{sql}");
    }
}

#[test]
fn extract_window_fns_replaces_with_slot_column() {
    let original = empty_window_fn("ROW_NUMBER");
    let mut counter = 0;
    let mut out = Vec::new();
    let rewritten = extract_window_fns(&original, &mut counter, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(counter, 1);
    if let Expr::Column(name) = rewritten {
        assert_eq!(name, "__win_0");
    } else {
        panic!("expected column reference for slot");
    }
}

#[test]
fn extract_window_fns_passes_non_window_expressions_through() {
    let e = Expr::Literal(i(5));
    let mut counter = 0;
    let mut out = Vec::new();
    let rewritten = extract_window_fns(&e, &mut counter, &mut out);
    assert!(out.is_empty());
    assert!(matches!(rewritten, Expr::Literal(Value::Integer(5))));
}

#[test]
fn extract_window_fns_inside_binary_op() {
    let e = Expr::BinaryOp {
        left: Box::new(empty_window_fn("RANK")),
        op: BinOp::Add,
        right: Box::new(Expr::Literal(i(1))),
    };
    let mut counter = 0;
    let mut out = Vec::new();
    let _ = extract_window_fns(&e, &mut counter, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(counter, 1);
}

#[test]
fn extract_window_fns_inside_coalesce() {
    let e = Expr::Coalesce(vec![empty_window_fn("LAG"), Expr::Literal(i(0))]);
    let mut counter = 0;
    let mut out = Vec::new();
    let _ = extract_window_fns(&e, &mut counter, &mut out);
    assert_eq!(out.len(), 1);
}

#[test]
fn extract_window_fns_multiple_slots_incrementing() {
    let e = Expr::BinaryOp {
        left: Box::new(empty_window_fn("FIRST_VALUE")),
        op: BinOp::Sub,
        right: Box::new(empty_window_fn("LAST_VALUE")),
    };
    let mut counter = 0;
    let mut out = Vec::new();
    let _ = extract_window_fns(&e, &mut counter, &mut out);
    assert_eq!(out.len(), 2);
    assert_eq!(counter, 2);
    assert_eq!(out[0].0, "__win_0");
    assert_eq!(out[1].0, "__win_1");
}

#[test]
fn extract_window_fns_inside_case_else() {
    let e = Expr::Case {
        operand: None,
        conditions: vec![(Expr::Literal(Value::Boolean(true)), Expr::Literal(i(0)))],
        else_result: Some(Box::new(empty_window_fn("ROW_NUMBER"))),
    };
    let mut counter = 0;
    let mut out = Vec::new();
    let _ = extract_window_fns(&e, &mut counter, &mut out);
    assert_eq!(out.len(), 1);
}

#[test]
fn extract_window_fns_inside_function_args() {
    let e = Expr::Function {
        name: "ABS".into(),
        args: vec![empty_window_fn("ROW_NUMBER")],
        distinct: false,
    };
    let mut counter = 0;
    let mut out = Vec::new();
    let _ = extract_window_fns(&e, &mut counter, &mut out);
    assert_eq!(out.len(), 1);
}

#[test]
fn extract_window_fns_inside_unary_op() {
    let e = Expr::UnaryOp {
        op: crate::parser::UnaryOp::Neg,
        expr: Box::new(empty_window_fn("RANK")),
    };
    let mut counter = 0;
    let mut out = Vec::new();
    let _ = extract_window_fns(&e, &mut counter, &mut out);
    assert_eq!(out.len(), 1);
}

#[test]
fn extract_window_fns_inside_cast() {
    let e = Expr::Cast {
        expr: Box::new(empty_window_fn("ROW_NUMBER")),
        data_type: crate::types::DataType::Real,
    };
    let mut counter = 0;
    let mut out = Vec::new();
    let _ = extract_window_fns(&e, &mut counter, &mut out);
    assert_eq!(out.len(), 1);
}

#[test]
fn window_order_keys_are_evaluated_once_per_row() {
    use crate::parser::OrderByItem;

    let columns = vec![column("x", DataType::Integer)];
    let mut stmt = empty_select();
    stmt.columns = vec![SelectColumn::Expr {
        expr: Expr::WindowFunction {
            name: "RANK".into(),
            args: vec![],
            spec: WindowSpec {
                partition_by: vec![],
                order_by: vec![OrderByItem {
                    expr: Expr::Column("x".into()),
                    output_name: None,
                    output_ordinal: None,
                    descending: false,
                    nulls_first: None,
                }],
                frame: None,
            },
        },
        alias: None,
    }];
    let rows: Vec<Vec<Value>> = (0..1_024).rev().map(|n| vec![i(n)]).collect();
    let _ = take_window_key_evaluations();

    eval_window_select(rows, crate::executor::SelectCtx::new(&columns, &stmt, None)).unwrap();

    assert_eq!(take_window_key_evaluations(), 1_024);
}

#[test]
fn one_large_peer_group_is_indexed_with_linear_comparisons() {
    let n = 4_096;
    let indices: Vec<usize> = (0..n).collect();
    let mut keys = WindowValues::with_capacity(n, 1).unwrap();
    for position in 0..n {
        let spelling = if position.is_multiple_of(2) { "A" } else { "a" };
        keys.push_row(std::iter::once(Ok(Value::Text(spelling.into()))))
            .unwrap();
    }
    let _ = take_window_peer_comparisons();

    let bounds = peer_group_bounds(&indices, &keys, 0, &[Collation::NoCase], None).unwrap();

    assert_eq!(take_window_peer_comparisons(), n - 1);
    assert!(bounds.iter().all(|bound| *bound == (0, n - 1)));
}

#[test]
fn window_argument_passes_cancellation_into_scalar_evaluation() {
    let columns = vec![column("body", DataType::Text)];
    let mut stmt = empty_select();
    stmt.columns = vec![SelectColumn::Expr {
        expr: Expr::WindowFunction {
            name: "LAG".into(),
            args: vec![
                Expr::Function {
                    name: "TO_TSVECTOR".into(),
                    args: vec![Expr::Column("body".into())],
                    distinct: false,
                },
                Expr::Literal(i(1)),
            ],
            spec: WindowSpec {
                partition_by: vec![],
                order_by: vec![],
                frame: None,
            },
        },
        alias: None,
    }];
    let token = citadel::CancelToken::new();
    let _cancel = crate::fts::cancel_tokenize_after(token.clone(), 1);

    let err = eval_window_select(
        vec![vec![Value::Text("several words to tokenize".into())]],
        crate::executor::SelectCtx::new(&columns, &stmt, Some(&token)),
    )
    .expect_err("the window argument discarded its cancellation token");

    assert!(matches!(
        err,
        crate::error::SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn forward_range_aggregates_visit_each_input_once() {
    let n = 64_i64;
    let mut partition = column("p", DataType::Integer);
    partition.position = 1;
    let mut key = column("k", DataType::Integer);
    key.position = 2;
    let columns = [column("x", DataType::Integer), partition, key];
    for peer_size in [1, 7, 128] {
        let rows: Vec<Vec<Value>> = (0..n)
            .rev()
            .map(|id| {
                let local = id / 2;
                vec![
                    if local % 7 == 0 {
                        Value::Null
                    } else {
                        i(local - 16)
                    },
                    i(id % 2),
                    i(local / peer_size),
                ]
            })
            .collect();
        for (frame, prefix) in [
            ("", true),
            ("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", true),
            ("RANGE BETWEEN CURRENT ROW AND CURRENT ROW", false),
        ] {
            let select = [
                "SUM(x)", "COUNT(*)", "COUNT(x)", "AVG(x)", "MIN(x)", "MAX(x)",
            ]
            .map(|function| format!("{function} OVER (PARTITION BY p ORDER BY k {frame})"))
            .join(", ");
            let expected: Vec<Vec<Value>> = rows
                .iter()
                .map(|row| {
                    let members: Vec<&Vec<Value>> = rows
                        .iter()
                        .filter(|other| {
                            other[1] == row[1]
                                && if prefix {
                                    other[2] <= row[2]
                                } else {
                                    other[2] == row[2]
                                }
                        })
                        .collect();
                    let values: Vec<i64> = members
                        .iter()
                        .filter_map(|row| {
                            if let Value::Integer(value) = &row[0] {
                                Some(*value)
                            } else {
                                None
                            }
                        })
                        .collect();
                    let sum: i64 = values.iter().sum();
                    vec![
                        if values.is_empty() {
                            Value::Null
                        } else {
                            i(sum)
                        },
                        i(members.len() as i64),
                        i(values.len() as i64),
                        if values.is_empty() {
                            Value::Null
                        } else {
                            Value::Real(sum as f64 / values.len() as f64)
                        },
                        values.iter().min().copied().map_or(Value::Null, i),
                        values.iter().max().copied().map_or(Value::Null, i),
                    ]
                })
                .collect();
            let _ = take_window_aggregate_steps();
            let ExecutionResult::Query(result) =
                evaluate_window_query(&format!("SELECT {select} FROM t"), &columns, rows.clone())
                    .unwrap()
            else {
                panic!("expected rows")
            };
            assert_eq!(result.rows, expected, "peer_size={peer_size}, {frame}");
            assert_eq!(
                take_window_aggregate_steps(),
                6 * n as usize,
                "peer_size={peer_size}, {frame}"
            );
        }
    }
}

#[test]
fn forward_range_preserves_floating_addition_order_and_peer_resets() {
    let mut position = column("position", DataType::Integer);
    position.position = 1;
    let columns = [column("x", DataType::Null), position];
    let values = [
        Value::Real(1e20),
        Value::Real(-1e20),
        Value::Real(3.5),
        i(1),
    ];
    let rows = values
        .into_iter()
        .enumerate()
        .map(|(position, value)| vec![value, i(position as i64)])
        .collect::<Vec<_>>();
    let r = Value::Real;
    for (spec, expected) in [
        (
            "ORDER BY position",
            vec![
                vec![r(1e20), r(1e20)],
                vec![r(0.0), r(0.0)],
                vec![r(3.5), r(3.5 / 3.0)],
                vec![r(4.5), r(1.125)],
            ],
        ),
        (
            "ORDER BY position DESC",
            vec![
                vec![r(1.0), r(0.25)],
                vec![r(-1e20), r(-1e20 / 3.0)],
                vec![r(4.5), r(2.25)],
                vec![i(1), r(1.0)],
            ],
        ),
        (
            "ORDER BY CASE WHEN position < 3 THEN 0 ELSE 1 END \
             RANGE BETWEEN CURRENT ROW AND CURRENT ROW",
            vec![
                vec![r(3.5), r(3.5 / 3.0)],
                vec![r(3.5), r(3.5 / 3.0)],
                vec![r(3.5), r(3.5 / 3.0)],
                vec![i(1), r(1.0)],
            ],
        ),
    ] {
        let ExecutionResult::Query(result) = evaluate_window_query(
            &format!("SELECT SUM(x) OVER ({spec}), AVG(x) OVER ({spec}) FROM t"),
            &columns,
            rows.clone(),
        )
        .unwrap() else {
            panic!("expected rows")
        };
        assert_eq!(result.rows.len(), expected.len());
        for (actual, expected) in result.rows.iter().zip(expected) {
            assert_eq!(actual.len(), expected.len());
            assert!(
                actual.iter().zip(expected.iter()).all(|(a, e)| a.bit_eq(e)),
                "{spec}: {actual:?}, expected {expected:?}"
            );
        }
    }
}

#[test]
fn forward_range_preserves_result_error_boundaries() {
    let mut position = column("position", DataType::Integer);
    position.position = 1;
    let columns = [column("x", DataType::Null), position];
    for later in [Value::Real(0.0), Value::Text("not numeric".into())] {
        assert!(matches!(
            evaluate_window_query(
                "SELECT SUM(x) OVER (ORDER BY position) FROM t",
                &columns,
                vec![vec![i(i64::MAX), i(0)], vec![i(1), i(1)], vec![later, i(2)]],
            ),
            Err(SqlError::IntegerOverflow)
        ));
    }
    // A peer contributes its entire frame before result(), so temporary
    // integer overflow inside a peer must not reject its valid final sum.
    for frame in ["", "RANGE BETWEEN CURRENT ROW AND CURRENT ROW"] {
        let ExecutionResult::Query(result) = evaluate_window_query(
            &format!("SELECT SUM(x) OVER (ORDER BY position {frame}) FROM t"),
            &columns,
            vec![vec![i(i64::MAX), i(0)], vec![i(1), i(0)], vec![i(-1), i(0)]],
        )
        .unwrap() else {
            panic!("expected rows")
        };
        assert_eq!(result.rows, vec![vec![i(i64::MAX)]; 3]);
    }
    assert!(matches!(
        evaluate_window_query(
            "SELECT AVG(x) OVER (ORDER BY position RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
            &columns,
            vec![vec![i(1), i(0)], vec![Value::Text("not numeric".into()), i(1)]],
        ),
        Err(SqlError::TypeMismatch { .. })
    ));
}

#[test]
fn forward_range_extrema_preserve_nan_representatives_across_peers() {
    let nan1 = Value::Real(f64::from_bits(0x7ff8_0000_0000_0001));
    let nan2 = Value::Real(f64::from_bits(0x7ff8_0000_0000_0002));
    let mut key = column("k", DataType::Integer);
    key.position = 1;
    let columns = [column("x", DataType::Null), key];
    let rows = vec![
        vec![Value::Real(1.0), i(0)],
        vec![nan1, i(0)],
        vec![Value::Real(0.0), i(0)],
        vec![nan2.clone(), i(1)],
        vec![Value::Real(3.0), i(1)],
        vec![Value::Real(2.0), i(1)],
    ];
    let first_peer = [Value::Real(0.0), Value::Real(1.0)];
    for (frame, second_peer) in [
        ("", vec![Value::Real(0.0), Value::Real(3.0)]),
        (
            "RANGE BETWEEN CURRENT ROW AND CURRENT ROW",
            vec![nan2.clone(), nan2],
        ),
    ] {
        let ExecutionResult::Query(result) = evaluate_window_query(
            &format!(
                "SELECT MIN(x) OVER (ORDER BY k {frame}), MAX(x) OVER (ORDER BY k {frame}) FROM t"
            ),
            &columns,
            rows.clone(),
        )
        .unwrap() else {
            panic!("expected rows")
        };
        assert_eq!(result.rows.len(), 6);
        for (position, row) in result.rows.iter().enumerate() {
            let expected = if position < 3 {
                &first_peer[..]
            } else {
                &second_peer
            };
            assert_eq!(row.len(), expected.len());
            assert!(
                row.iter().zip(expected).all(|(a, e)| a.bit_eq(e)),
                "{frame}, row {position}: {row:?}, expected {expected:?}"
            );
        }
    }
}

fn evaluate_window_query(
    sql: &str,
    columns: &[ColumnDef],
    rows: Vec<Vec<Value>>,
) -> Result<ExecutionResult> {
    let Statement::Select(query) = crate::parser::parse_sql(sql).unwrap() else {
        panic!("expected SELECT");
    };
    let QueryBody::Select(stmt) = query.body else {
        panic!("expected plain SELECT");
    };
    eval_window_select(rows, crate::executor::SelectCtx::new(columns, &stmt, None))
}

#[test]
fn whole_partition_aggregates_visit_each_input_once_per_function() {
    let n = 128;
    let rows: Vec<Vec<Value>> = (0..n)
        .map(|index| {
            vec![if index % 7 == 0 {
                Value::Null
            } else {
                i(index)
            }]
        })
        .collect();
    let sum: i64 = (0..n).filter(|index| index % 7 != 0).sum();
    let count = (0..n).filter(|index| index % 7 != 0).count() as i64;
    let expected = vec![
        i(sum),
        i(n),
        i(count),
        Value::Real(sum as f64 / count as f64),
        i(1),
        i(n - 1),
    ];
    for spec in [
        "",
        "ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        "ORDER BY x DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
    ] {
        let sql = [
            "SUM(x)", "COUNT(*)", "COUNT(x)", "AVG(x)", "MIN(x)", "MAX(x)",
        ]
        .map(|function| format!("{function} OVER ({spec})"))
        .join(", ");
        let _ = take_window_aggregate_steps();
        let ExecutionResult::Query(result) = evaluate_window_query(
            &format!("SELECT {sql} FROM t"),
            &[column("x", DataType::Integer)],
            rows.clone(),
        )
        .unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(result.rows, vec![expected.clone(); n as usize]);
        assert_eq!(take_window_aggregate_steps(), 6 * n as usize, "{spec}");
    }
}

#[test]
fn full_partition_float_results_follow_existing_sorted_accumulation_order() {
    let mut position = column("position", DataType::Integer);
    position.position = 1;
    let columns = [column("x", DataType::Real), position];
    // Include a stored integer alongside real values: SlidingSum preserves
    // separate integer and real subtotals before combining them at result().
    let rows = vec![
        vec![Value::Real(1e20), i(0)],
        vec![Value::Real(-1e20), i(1)],
        vec![Value::Real(3.5), i(2)],
        vec![i(1), i(3)],
    ];
    for (direction, sum, average) in [("ASC", 4.5, 1.125), ("DESC", 1.0, 0.25)] {
        let spec = format!("ORDER BY position {direction} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");
        let ExecutionResult::Query(result) = evaluate_window_query(
            &format!("SELECT SUM(x) OVER ({spec}), AVG(x) OVER ({spec}) FROM t"),
            &columns,
            rows.clone(),
        )
        .unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(
            result.rows,
            vec![vec![Value::Real(sum), Value::Real(average)]; 4]
        );
    }
}

#[test]
fn full_partition_aggregate_errors_match_existing_accumulator_rules() {
    let columns = [column("x", DataType::Integer)];
    for spec in [
        "",
        "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
    ] {
        assert!(matches!(
            evaluate_window_query(
                &format!("SELECT SUM(x) OVER ({spec}) FROM t"),
                &columns,
                vec![vec![i(i64::MAX)], vec![i(1)]],
            ),
            Err(SqlError::IntegerOverflow)
        ));
        assert!(matches!(
            evaluate_window_query(
                &format!("SELECT AVG(x) OVER ({spec}) FROM t"),
                &columns,
                vec![vec![i(1)], vec![Value::Text("not numeric".into())]],
            ),
            Err(SqlError::TypeMismatch { .. })
        ));
    }
}

#[test]
fn full_partition_extrema_preserve_the_first_equal_value_representation() {
    let mut x = column("x", DataType::Null);
    x.collation = Collation::NoCase;
    for values in [
        vec![i(1), Value::Real(1.0)],
        vec![Value::Real(1.0), i(1)],
        vec![Value::Real(-0.0), Value::Real(0.0)],
        vec![
            Value::Real(f64::from_bits(0x7ff8_0000_0000_0001)),
            Value::Real(f64::from_bits(0x7ff8_0000_0000_0002)),
            Value::Real(1.0),
        ],
        vec![
            Value::Text("First_heap_text_value_with_equal_collation".into()),
            Value::Text("first_heap_text_value_with_equal_collation".into()),
        ],
    ] {
        for frame in [
            "",
            "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        ] {
            let rows = values.iter().cloned().map(|value| vec![value]).collect();
            let ExecutionResult::Query(result) = evaluate_window_query(
                &format!("SELECT MIN(x) OVER ({frame}), MAX(x) OVER ({frame}) FROM t"),
                std::slice::from_ref(&x),
                rows,
            )
            .unwrap() else {
                panic!("expected rows");
            };
            assert_eq!(result.rows.len(), values.len());
            for row in result.rows {
                assert!(
                    row[0].bit_eq(&values[0]),
                    "MIN changed the equal representative: {row:?}"
                );
                assert!(
                    row[1].bit_eq(&values[0]),
                    "MAX changed the equal representative: {row:?}"
                );
            }
        }
    }
}

// These tests order by a distinct integer position so the extrema arguments
// are not sort keys.
fn assert_sliding_extreme_crosses_equal_bridge(function: &str, values: [Value; 3]) {
    let mut position = column("position", DataType::Integer);
    position.position = 1;
    let columns = [column("x", DataType::Null), position];
    for frame in [
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
    ] {
        let rows = values
            .iter()
            .cloned()
            .enumerate()
            .map(|(position, value)| vec![value, i(position as i64)])
            .collect();
        let ExecutionResult::Query(result) = evaluate_window_query(
            &format!("SELECT {function}(x) OVER (ORDER BY position {frame}) FROM t"),
            &columns,
            rows,
        )
        .unwrap() else {
            panic!("expected rows");
        };
        // The middle value compares equal to both neighbors. It must not stop
        // the strictly better third value from replacing the first value.
        assert_eq!(result.rows.len(), values.len());
        let expected = [&values[0], &values[0], &values[2]];
        for (position, (row, expected)) in result.rows.iter().zip(expected).enumerate() {
            assert!(
                row[0].bit_eq(expected),
                "{function}, {frame}, row {position}: {:?}, expected {expected:?}",
                row[0],
            );
        }
    }
}

#[test]
fn sliding_extrema_do_not_treat_nan_as_a_transitive_equal_bridge() {
    let nan = Value::Real(f64::from_bits(0x7ff8_0000_0000_0001));
    assert_sliding_extreme_crosses_equal_bridge(
        "MIN",
        [Value::Real(1.0), nan.clone(), Value::Real(0.0)],
    );
    assert_sliding_extreme_crosses_equal_bridge("MAX", [Value::Real(1.0), nan, Value::Real(2.0)]);
}

#[test]
fn sliding_extrema_preserve_large_integer_order_across_real_values() {
    let boundary = 1_i64 << 53;
    assert_sliding_extreme_crosses_equal_bridge(
        "MIN",
        [i(boundary + 1), Value::Real(boundary as f64), i(boundary)],
    );
    assert_sliding_extreme_crosses_equal_bridge(
        "MAX",
        [i(boundary), Value::Real(boundary as f64), i(boundary + 1)],
    );
}

#[test]
fn sliding_extrema_preserve_nested_array_comparison_order() {
    let nested = |value| {
        Value::Array(std::sync::Arc::new(vec![Value::Array(
            std::sync::Arc::new(vec![value]),
        )]))
    };
    assert_sliding_extreme_crosses_equal_bridge(
        "MIN",
        [
            nested(Value::Real(1.0)),
            nested(Value::Real(f64::from_bits(0x7ff8_0000_0000_0001))),
            nested(Value::Real(0.0)),
        ],
    );
}

#[test]
fn sliding_extrema_match_frame_order_with_expiry_and_lookahead() {
    let nan1 = Value::Real(f64::from_bits(0x7ff8_0000_0000_0001));
    let nan2 = Value::Real(f64::from_bits(0x7ff8_0000_0000_0002));
    let boundary = 1_i64 << 53;
    let nested = |value| {
        Value::Array(std::sync::Arc::new(vec![Value::Array(
            std::sync::Arc::new(vec![value]),
        )]))
    };
    let cases = [
        vec![
            Value::Null,
            Value::Real(1.0),
            nan1.clone(),
            Value::Real(0.0),
            nan2.clone(),
            Value::Real(2.0),
            Value::Null,
        ],
        vec![
            nan1.clone(),
            Value::Real(1.0),
            nan2.clone(),
            Value::Real(0.0),
            Value::Real(2.0),
        ],
        vec![
            i(boundary + 1),
            Value::Real(boundary as f64),
            i(boundary),
            Value::Null,
            i(-boundary - 1),
            Value::Real(-boundary as f64),
            i(-boundary),
        ],
        vec![
            i(i64::MAX - 1),
            Value::Real(i64::MAX as f64),
            i(i64::MAX),
            i(i64::MIN),
            Value::Real(i64::MIN as f64),
        ],
        vec![
            nested(Value::Real(1.0)),
            nested(nan1),
            nested(Value::Real(0.0)),
            Value::Null,
            nested(nan2),
            nested(Value::Real(2.0)),
        ],
        vec![
            Value::Real(-0.0),
            Value::Real(0.0),
            Value::Null,
            i(0),
            Value::Real(-0.0),
        ],
        vec![
            Value::Text("First_heap_spelling_with_equal_collation".into()),
            Value::Text("first_heap_spelling_with_equal_collation".into()),
            Value::Null,
            Value::Text("Another_long_heap_spelling".into()),
            Value::Text("another_long_heap_spelling".into()),
        ],
    ];
    let mut x = column("x", DataType::Null);
    x.collation = Collation::NoCase;
    let mut position = column("position", DataType::Integer);
    position.position = 1;
    let columns = [x, position];

    for values in cases {
        for (frame, preceding, end_offset) in [
            ("UNBOUNDED PRECEDING AND CURRENT ROW", None, 0_i64),
            ("UNBOUNDED PRECEDING AND 1 PRECEDING", None, -1),
            ("UNBOUNDED PRECEDING AND 1 FOLLOWING", None, 1),
            ("2 PRECEDING AND CURRENT ROW", Some(2), 0),
            ("1 PRECEDING AND 1 FOLLOWING", Some(1), 1),
        ] {
            let rows = values
                .iter()
                .cloned()
                .enumerate()
                .map(|(position, value)| vec![value, i(position as i64)])
                .collect();
            let ExecutionResult::Query(result) = evaluate_window_query(
                &format!("SELECT MIN(x) OVER (ORDER BY position ROWS BETWEEN {frame}), MAX(x) OVER (ORDER BY position ROWS BETWEEN {frame}) FROM t"),
                &columns,
                rows,
            ).unwrap() else { panic!("expected rows") };
            assert_eq!(result.rows.len(), values.len());
            for (position, row) in result.rows.iter().enumerate() {
                let start = preceding.map_or(0, |count| position.saturating_sub(count));
                let end = (position as i64 + end_offset + 1).clamp(0, values.len() as i64) as usize;
                for (column, is_min) in [(0, true), (1, false)] {
                    // This explicit left fold is independent of deque pruning
                    // and keeps the first representation on equal comparisons.
                    let mut expected = Value::Null;
                    for value in &values[start..end] {
                        if value.is_null() {
                            continue;
                        }
                        let order = Collation::NoCase.cmp_value(value, &expected);
                        if expected.is_null()
                            || (is_min && order.is_lt())
                            || (!is_min && order.is_gt())
                        {
                            expected = value.clone();
                        }
                    }
                    assert!(row[column].bit_eq(&expected), "{frame}, row {position}, MIN={is_min}: {:?}, expected {expected:?}; input {values:?}", row[column]);
                }
            }
        }
    }
}

#[test]
fn sliding_extrema_certify_the_evaluated_comparison_domain() {
    let exact_large = 1_i64 << 60;
    for (values, expected) in [
        (vec![i(i64::MAX), i(i64::MIN), Value::Null], true),
        (vec![i(exact_large), Value::Real(exact_large as f64)], true),
        (vec![i(i64::MIN), Value::Real(i64::MIN as f64)], true),
        (
            vec![Value::Real(-0.0), i(0), Value::Real(f64::INFINITY)],
            true,
        ),
        (
            vec![
                Value::Vector(std::sync::Arc::from([f32::NAN])),
                Value::Vector(std::sync::Arc::from([1.0_f32])),
            ],
            true,
        ),
        (
            vec![
                Value::Json("{\"n\":1}".into()),
                Value::Json("{\"n\":0}".into()),
            ],
            true,
        ),
        (
            vec![i(exact_large + 1), Value::Real(exact_large as f64)],
            false,
        ),
        (vec![i(i64::MAX), Value::Real(i64::MAX as f64)], false),
        (vec![Value::Real(f64::NAN)], false),
        (vec![Value::Array(std::sync::Arc::new(vec![i(1)]))], false),
    ] {
        let mut arguments = WindowValues::with_capacity(values.len(), 1).unwrap();
        for value in &values {
            arguments
                .push_row(std::iter::once(Ok(value.clone())))
                .unwrap();
        }
        let indices: Vec<usize> = (0..values.len()).collect();
        assert_eq!(
            supports_monotonic_extrema(&indices, &arguments, None).unwrap(),
            expected,
            "{values:?}"
        );
    }
}

#[test]
fn growing_rows_extrema_visit_unsafe_values_once() {
    let n = 128;
    let nan = Value::Real(f64::from_bits(0x7ff8_0000_0000_0001));
    let rows = (0..n)
        .map(|position| {
            vec![
                if position % 2 == 0 {
                    nan.clone()
                } else {
                    i(i64::MAX)
                },
                i(position),
            ]
        })
        .collect();
    let mut position = column("position", DataType::Integer);
    position.position = 1;
    let _ = take_window_aggregate_steps();
    let ExecutionResult::Query(result) = evaluate_window_query(
        "SELECT MIN(x) OVER (ORDER BY position ROWS UNBOUNDED PRECEDING), MAX(x) OVER (ORDER BY position ROWS UNBOUNDED PRECEDING) FROM t",
        &[column("x", DataType::Null), position], rows,
    ).unwrap() else { panic!("expected rows") };
    assert_eq!(result.rows.len(), n as usize);
    assert_eq!(take_window_aggregate_steps(), 2 * n as usize);
    for row in result.rows {
        assert!(row[0].bit_eq(&nan));
        assert!(row[1].bit_eq(&nan));
    }
}

fn evaluate_bounded_real_sum_and_avg(values: [f64; 3]) -> Vec<Vec<Value>> {
    let mut position = column("position", DataType::Integer);
    position.position = 1;
    let rows = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| vec![Value::Real(value), i(index as i64)])
        .collect();
    let ExecutionResult::Query(result) = evaluate_window_query(
        "SELECT SUM(x) OVER (ORDER BY position ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), \
         AVG(x) OVER (ORDER BY position ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        &[column("x", DataType::Real), position],
        rows,
    )
    .unwrap() else {
        panic!("expected rows");
    };
    result.rows
}

#[test]
fn sliding_sum_and_avg_retain_small_values_after_a_large_real_expires() {
    assert_eq!(
        evaluate_bounded_real_sum_and_avg([1e20, 1.0, 1.0]),
        vec![
            vec![Value::Real(1e20), Value::Real(1e20)],
            vec![Value::Real(1e20), Value::Real(5e19)],
            vec![Value::Real(2.0), Value::Real(1.0)],
        ]
    );
}

#[test]
fn sliding_sum_and_avg_recover_after_nan_expires() {
    let rows = evaluate_bounded_real_sum_and_avg([f64::NAN, 1.0, 2.0]);
    assert_eq!(rows[2], vec![Value::Real(3.0), Value::Real(1.5)]);
}

#[test]
fn sliding_sum_and_avg_recover_after_infinity_expires() {
    for infinity in [f64::INFINITY, f64::NEG_INFINITY] {
        let rows = evaluate_bounded_real_sum_and_avg([infinity, 1.0, 2.0]);
        assert_eq!(rows[2], vec![Value::Real(3.0), Value::Real(1.5)]);
    }
}

fn evaluate_sum_and_avg_frame(values: Vec<Value>, frame: &str) -> Vec<Vec<Value>> {
    let mut position = column("position", DataType::Integer);
    position.position = 1;
    let rows = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| vec![value, i(index as i64)])
        .collect();
    let spec = format!("ORDER BY position ROWS BETWEEN {frame}");
    let ExecutionResult::Query(result) = evaluate_window_query(
        &format!("SELECT SUM(x) OVER ({spec}), AVG(x) OVER ({spec}) FROM t"),
        &[column("x", DataType::Null), position],
        rows,
    )
    .unwrap() else {
        panic!("expected rows");
    };
    result.rows
}

#[test]
fn sliding_sum_rebuild_preserves_integer_subtotals_and_following_bounds() {
    let rows = evaluate_sum_and_avg_frame(
        vec![
            Value::Real(1e20),
            i(7),
            Value::Real(1.0),
            i(11),
            Value::Real(2.0),
        ],
        "2 PRECEDING AND CURRENT ROW",
    );
    assert_eq!(rows[3], vec![Value::Real(19.0), Value::Real(19.0 / 3.0)]);
    assert_eq!(rows[4], vec![Value::Real(14.0), Value::Real(14.0 / 3.0)]);

    let rows = evaluate_sum_and_avg_frame(
        vec![
            Value::Real(1e20),
            Value::Real(1.0),
            Value::Real(1.0),
            Value::Real(2.0),
        ],
        "1 PRECEDING AND 1 FOLLOWING",
    );
    assert_eq!(rows[2], vec![Value::Real(4.0), Value::Real(4.0 / 3.0)]);
    assert_eq!(rows[3], vec![Value::Real(3.0), Value::Real(1.5)]);
}

#[test]
fn sliding_sum_and_avg_keep_exact_integer_frames_linear() {
    let n = 128usize;
    let _ = take_window_aggregate_steps();
    let rows = evaluate_sum_and_avg_frame(vec![i(1); n], "7 PRECEDING AND CURRENT ROW");
    assert_eq!(take_window_aggregate_steps(), 2 * n);
    for (index, row) in rows.into_iter().enumerate() {
        assert_eq!(row, vec![i((index + 1).min(8) as i64), Value::Real(1.0)]);
    }
}

#[test]
fn growing_real_sum_and_avg_keep_forward_order_and_linear_work() {
    let n = 128usize;
    let mut values = vec![Value::Real(1.0); n];
    values[0] = Value::Real(1e20);
    values[1] = Value::Real(-1e20);
    for frame in [
        "UNBOUNDED PRECEDING AND CURRENT ROW",
        "UNBOUNDED PRECEDING AND 1 FOLLOWING",
    ] {
        let _ = take_window_aggregate_steps();
        let rows = evaluate_sum_and_avg_frame(values.clone(), frame);
        assert_eq!(take_window_aggregate_steps(), 2 * n, "{frame}");
        assert_eq!(
            rows[n - 1],
            vec![Value::Real(126.0), Value::Real(126.0 / 128.0)]
        );
    }
}

#[test]
fn expiring_the_only_real_resets_its_subtotal_without_rescanning() {
    let _ = take_window_aggregate_steps();
    let rows = evaluate_sum_and_avg_frame(
        vec![Value::Real(f64::NAN), i(2), Value::Null, i(4), i(5)],
        "1 PRECEDING AND CURRENT ROW",
    );
    assert_eq!(take_window_aggregate_steps(), 10);
    assert_eq!(rows[2], vec![i(2), Value::Real(2.0)]);
    assert_eq!(rows[3], vec![i(4), Value::Real(4.0)]);
    assert_eq!(rows[4], vec![i(9), Value::Real(4.5)]);
}

#[test]
fn count_of_real_values_keeps_exact_linear_removal() {
    let n = 128usize;
    let mut position = column("position", DataType::Integer);
    position.position = 1;
    let rows = (0..n)
        .map(|index| vec![Value::Real(f64::NAN), i(index as i64)])
        .collect();
    let _ = take_window_aggregate_steps();
    let ExecutionResult::Query(result) = evaluate_window_query(
        "SELECT COUNT(x) OVER (ORDER BY position ROWS BETWEEN 7 PRECEDING AND CURRENT ROW), \
         COUNT(*) OVER (ORDER BY position ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) FROM t",
        &[column("x", DataType::Real), position],
        rows,
    )
    .unwrap() else {
        panic!("expected rows");
    };
    assert_eq!(take_window_aggregate_steps(), 2 * n);
    for (index, row) in result.rows.into_iter().enumerate() {
        assert_eq!(row, vec![i((index + 1).min(8) as i64); 2]);
    }
}
