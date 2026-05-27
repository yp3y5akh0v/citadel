use super::*;
use crate::parser::{
    BinOp, Expr, SelectColumn, SelectStmt, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowSpec,
};
use crate::types::Value;

fn i(n: i64) -> Value {
    Value::Integer(n)
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
