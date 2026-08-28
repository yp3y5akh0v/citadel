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
    let (s, e) = rows_frame_indices(&frame, 2, 5).unwrap();
    assert_eq!((s, e), (0, 4));
}

#[test]
fn rows_frame_indices_current_row_only() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::CurrentRow,
        end: WindowFrameBound::CurrentRow,
    };
    let (s, e) = rows_frame_indices(&frame, 2, 5).unwrap();
    assert_eq!((s, e), (2, 2));
}

#[test]
fn rows_frame_indices_preceding_following() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::Preceding(Box::new(Expr::Literal(i(1)))),
        end: WindowFrameBound::Following(Box::new(Expr::Literal(i(1)))),
    };
    let (s, e) = rows_frame_indices(&frame, 2, 5).unwrap();
    assert_eq!((s, e), (1, 3));
}

#[test]
fn rows_frame_indices_preceding_clamps_to_zero() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::Preceding(Box::new(Expr::Literal(i(10)))),
        end: WindowFrameBound::CurrentRow,
    };
    let (s, e) = rows_frame_indices(&frame, 2, 5).unwrap();
    assert_eq!((s, e), (0, 2));
}

#[test]
fn rows_frame_indices_following_clamps_to_n_minus_1() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::CurrentRow,
        end: WindowFrameBound::Following(Box::new(Expr::Literal(i(10)))),
    };
    let (s, e) = rows_frame_indices(&frame, 2, 5).unwrap();
    assert_eq!((s, e), (2, 4));
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
    let keys: Vec<Vec<Value>> = (0..n)
        .map(|position| {
            let spelling = if position.is_multiple_of(2) { "A" } else { "a" };
            vec![Value::Text(spelling.into())]
        })
        .collect();
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
