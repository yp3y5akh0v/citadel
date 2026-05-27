use super::*;
use crate::eval::ColumnMap;
use crate::parser::{BinOp, Expr};
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

fn i(n: i64) -> Value {
    Value::Integer(n)
}

#[test]
fn is_aggregate_function_count_any_arity() {
    assert!(is_aggregate_function("COUNT", 0));
    assert!(is_aggregate_function("COUNT", 1));
    assert!(is_aggregate_function("count", 1));
}

#[test]
fn is_aggregate_function_sum_avg() {
    assert!(is_aggregate_function("SUM", 1));
    assert!(is_aggregate_function("AVG", 1));
}

#[test]
fn is_aggregate_function_min_max_only_unary() {
    assert!(is_aggregate_function("MIN", 1));
    assert!(is_aggregate_function("MAX", 1));
    assert!(!is_aggregate_function("MIN", 2));
    assert!(!is_aggregate_function("MAX", 0));
}

#[test]
fn is_aggregate_function_json_aggs() {
    assert!(is_aggregate_function("JSON_AGG", 1));
    assert!(is_aggregate_function("JSONB_AGG", 1));
    assert!(is_aggregate_function("JSON_OBJECT_AGG", 2));
    assert!(!is_aggregate_function("JSON_OBJECT_AGG", 1));
}

#[test]
fn is_aggregate_function_unknown_returns_false() {
    assert!(!is_aggregate_function("UPPER", 1));
    assert!(!is_aggregate_function("ABS", 1));
}

#[test]
fn is_aggregate_expr_count_star() {
    assert!(is_aggregate_expr(&Expr::CountStar));
}

#[test]
fn is_aggregate_expr_aggregate_function() {
    let e = Expr::Function {
        name: "SUM".into(),
        args: vec![Expr::Column("x".into())],
        distinct: false,
    };
    assert!(is_aggregate_expr(&e));
}

#[test]
fn is_aggregate_expr_nested_aggregate_in_args() {
    let inner = Expr::Function {
        name: "MAX".into(),
        args: vec![Expr::Column("x".into())],
        distinct: false,
    };
    let outer = Expr::Function {
        name: "ABS".into(),
        args: vec![inner],
        distinct: false,
    };
    assert!(is_aggregate_expr(&outer));
}

#[test]
fn is_aggregate_expr_binary_op_propagates() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::CountStar),
        op: BinOp::Add,
        right: Box::new(Expr::Literal(i(1))),
    };
    assert!(is_aggregate_expr(&e));
}

#[test]
fn is_aggregate_expr_plain_column_false() {
    assert!(!is_aggregate_expr(&Expr::Column("x".into())));
}

#[test]
fn is_aggregate_expr_literal_false() {
    assert!(!is_aggregate_expr(&Expr::Literal(i(1))));
}

#[test]
fn is_aggregate_expr_window_function_is_not_aggregate() {
    let e = Expr::WindowFunction {
        name: "ROW_NUMBER".into(),
        args: vec![],
        spec: crate::parser::WindowSpec {
            partition_by: vec![],
            order_by: vec![],
            frame: None,
        },
    };
    assert!(!is_aggregate_expr(&e));
}

#[test]
fn is_aggregate_expr_case_with_aggregate_branch() {
    let e = Expr::Case {
        operand: None,
        conditions: vec![(
            Expr::Literal(Value::Boolean(true)),
            Expr::Function {
                name: "COUNT".into(),
                args: vec![Expr::Column("x".into())],
                distinct: false,
            },
        )],
        else_result: None,
    };
    assert!(is_aggregate_expr(&e));
}

#[test]
fn is_aggregate_expr_coalesce_with_aggregate() {
    let e = Expr::Coalesce(vec![Expr::CountStar, Expr::Literal(i(0))]);
    assert!(is_aggregate_expr(&e));
}

#[test]
fn eval_aggregate_expr_count_star() {
    let cs = cols(&[("x", DataType::Integer)]);
    let cm = ColumnMap::new(&cs);
    let r1 = vec![i(1)];
    let r2 = vec![i(2)];
    let r3 = vec![i(3)];
    let rows: Vec<&Vec<Value>> = vec![&r1, &r2, &r3];
    let result = eval_aggregate_expr(&Expr::CountStar, &cm, &rows).unwrap();
    assert_eq!(result, i(3));
}

#[test]
fn eval_aggregate_expr_sum_integer() {
    let cs = cols(&[("v", DataType::Integer)]);
    let cm = ColumnMap::new(&cs);
    let r1 = vec![i(10)];
    let r2 = vec![i(20)];
    let r3 = vec![i(30)];
    let rows: Vec<&Vec<Value>> = vec![&r1, &r2, &r3];
    let e = Expr::Function {
        name: "SUM".into(),
        args: vec![Expr::Column("v".into())],
        distinct: false,
    };
    let result = eval_aggregate_expr(&e, &cm, &rows).unwrap();
    assert_eq!(result, i(60));
}
