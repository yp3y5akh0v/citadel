use super::*;
use crate::types::DataType;

fn col(name: &str, dt: DataType, nullable: bool, pos: u16) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: dt,
        nullable,
        position: pos,
        default_expr: None,
        default_sql: None,
        check_expr: None,
        check_sql: None,
        check_name: None,
        is_with_timezone: false,
        generated_expr: None,
        generated_sql: None,
        generated_kind: None,
        collation: crate::types::Collation::Binary,
    }
}

fn test_columns() -> Vec<ColumnDef> {
    vec![
        col("id", DataType::Integer, false, 0),
        col("name", DataType::Text, true, 1),
        col("score", DataType::Real, true, 2),
        col("active", DataType::Boolean, false, 3),
    ]
}

fn test_row() -> Vec<Value> {
    vec![
        Value::Integer(1),
        Value::Text("Alice".into()),
        Value::Real(95.5),
        Value::Boolean(true),
    ]
}

#[test]
fn eval_literal() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = Expr::Literal(Value::Integer(42));
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Integer(42)
    );
}

#[test]
fn eval_column_ref() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = Expr::Column("name".into());
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Text("Alice".into())
    );
}

#[test]
fn eval_column_case_insensitive() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = Expr::Column("name".into());
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Text("Alice".into())
    );
}

#[test]
fn eval_arithmetic_int() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("id".into())),
        op: BinOp::Add,
        right: Box::new(Expr::Literal(Value::Integer(10))),
    };
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Integer(11)
    );
}

#[test]
fn eval_comparison() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("score".into())),
        op: BinOp::Gt,
        right: Box::new(Expr::Literal(Value::Real(90.0))),
    };
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(true)
    );
}

#[test]
fn eval_null_propagation() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = vec![
        Value::Integer(1),
        Value::Null,
        Value::Null,
        Value::Boolean(true),
    ];
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("name".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Text("test".into()))),
    };
    assert!(eval_expr(&expr, &EvalCtx::new(&cm, &row))
        .unwrap()
        .is_null());
}

#[test]
fn eval_and_three_valued() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = vec![
        Value::Integer(1),
        Value::Null,
        Value::Null,
        Value::Boolean(true),
    ];

    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("name".into())),
        op: BinOp::And,
        right: Box::new(Expr::Literal(Value::Boolean(false))),
    };
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(false)
    );

    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("name".into())),
        op: BinOp::And,
        right: Box::new(Expr::Literal(Value::Boolean(true))),
    };
    assert!(eval_expr(&expr, &EvalCtx::new(&cm, &row))
        .unwrap()
        .is_null());
}

#[test]
fn eval_or_three_valued() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = vec![
        Value::Integer(1),
        Value::Null,
        Value::Null,
        Value::Boolean(true),
    ];

    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("name".into())),
        op: BinOp::Or,
        right: Box::new(Expr::Literal(Value::Boolean(true))),
    };
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(true)
    );

    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("name".into())),
        op: BinOp::Or,
        right: Box::new(Expr::Literal(Value::Boolean(false))),
    };
    assert!(eval_expr(&expr, &EvalCtx::new(&cm, &row))
        .unwrap()
        .is_null());
}

#[test]
fn eval_is_null() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = vec![
        Value::Integer(1),
        Value::Null,
        Value::Null,
        Value::Boolean(true),
    ];
    let expr = Expr::IsNull(Box::new(Expr::Column("name".into())));
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(true)
    );

    let expr = Expr::IsNotNull(Box::new(Expr::Column("id".into())));
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(true)
    );
}

#[test]
fn eval_not() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = Expr::UnaryOp {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Column("active".into())),
    };
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(false)
    );
}

#[test]
fn eval_neg() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = Expr::UnaryOp {
        op: UnaryOp::Neg,
        expr: Box::new(Expr::Column("id".into())),
    };
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Integer(-1)
    );
}

#[test]
fn eval_division_by_zero() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("id".into())),
        op: BinOp::Div,
        right: Box::new(Expr::Literal(Value::Integer(0))),
    };
    assert!(matches!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)),
        Err(SqlError::DivisionByZero)
    ));
}

#[test]
fn eval_mixed_numeric() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("id".into())),
        op: BinOp::Add,
        right: Box::new(Expr::Column("score".into())),
    };
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Real(96.5)
    );
}

#[test]
fn is_truthy_values() {
    assert!(is_truthy(&Value::Boolean(true)));
    assert!(!is_truthy(&Value::Boolean(false)));
    assert!(!is_truthy(&Value::Null));
    assert!(is_truthy(&Value::Integer(1)));
    assert!(!is_truthy(&Value::Integer(0)));
}
