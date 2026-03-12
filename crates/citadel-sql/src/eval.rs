//! Expression evaluator with SQL three-valued logic.

use crate::error::{Result, SqlError};
use crate::parser::{BinOp, Expr, UnaryOp};
use crate::types::{ColumnDef, Value};

/// Evaluate an expression against a row.
///
/// `columns` maps column names to their positions.
/// `row` is the full row of values (all columns).
pub fn eval_expr(expr: &Expr, columns: &[ColumnDef], row: &[Value]) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),

        Expr::Column(name) => {
            let lower = name.to_ascii_lowercase();
            let matches: Vec<usize> = columns.iter().enumerate()
                .filter(|(_, c)| {
                    let cn = c.name.to_ascii_lowercase();
                    cn == lower || cn.ends_with(&format!(".{lower}"))
                })
                .map(|(i, _)| i)
                .collect();
            match matches.len() {
                0 => Err(SqlError::ColumnNotFound(name.clone())),
                1 => Ok(row[matches[0]].clone()),
                _ => Err(SqlError::AmbiguousColumn(name.clone())),
            }
        }

        Expr::QualifiedColumn { table, column } => {
            let qualified = format!("{}.{}", table.to_ascii_lowercase(), column.to_ascii_lowercase());
            let idx = columns.iter()
                .position(|c| c.name.to_ascii_lowercase() == qualified)
                .ok_or_else(|| SqlError::ColumnNotFound(format!("{table}.{column}")))?;
            Ok(row[idx].clone())
        }

        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, columns, row)?;
            let rval = eval_expr(right, columns, row)?;
            eval_binary_op(&lval, *op, &rval)
        }

        Expr::UnaryOp { op, expr } => {
            let val = eval_expr(expr, columns, row)?;
            eval_unary_op(*op, &val)
        }

        Expr::IsNull(e) => {
            let val = eval_expr(e, columns, row)?;
            Ok(Value::Boolean(val.is_null()))
        }

        Expr::IsNotNull(e) => {
            let val = eval_expr(e, columns, row)?;
            Ok(Value::Boolean(!val.is_null()))
        }

        Expr::Function { name, args: _ } => {
            // Aggregate functions are handled at a higher level (executor).
            // This handles scalar functions — none defined yet.
            Err(SqlError::Unsupported(format!("scalar function: {name}")))
        }

        Expr::CountStar => {
            // Aggregates are evaluated at the executor level, not here.
            Err(SqlError::Unsupported("COUNT(*) in non-aggregate context".into()))
        }
    }
}

fn eval_binary_op(left: &Value, op: BinOp, right: &Value) -> Result<Value> {
    // SQL three-valued logic for AND/OR
    match op {
        BinOp::And => return eval_and(left, right),
        BinOp::Or => return eval_or(left, right),
        _ => {}
    }

    // NULL propagation for all other ops
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    match op {
        BinOp::Eq => Ok(Value::Boolean(left == right)),
        BinOp::NotEq => Ok(Value::Boolean(left != right)),
        BinOp::Lt => Ok(Value::Boolean(left < right)),
        BinOp::Gt => Ok(Value::Boolean(left > right)),
        BinOp::LtEq => Ok(Value::Boolean(left <= right)),
        BinOp::GtEq => Ok(Value::Boolean(left >= right)),
        BinOp::Add => eval_arithmetic(left, right, i64::checked_add, |a, b| a + b),
        BinOp::Sub => eval_arithmetic(left, right, i64::checked_sub, |a, b| a - b),
        BinOp::Mul => eval_arithmetic(left, right, i64::checked_mul, |a, b| a * b),
        BinOp::Div => {
            match right {
                Value::Integer(0) => return Err(SqlError::DivisionByZero),
                Value::Real(r) if *r == 0.0 => return Err(SqlError::DivisionByZero),
                _ => {}
            }
            eval_arithmetic(left, right, i64::checked_div, |a, b| a / b)
        }
        BinOp::Mod => {
            match right {
                Value::Integer(0) => return Err(SqlError::DivisionByZero),
                Value::Real(r) if *r == 0.0 => return Err(SqlError::DivisionByZero),
                _ => {}
            }
            eval_arithmetic(left, right, i64::checked_rem, |a, b| a % b)
        }
        BinOp::And | BinOp::Or => unreachable!(),
    }
}

/// SQL three-valued AND: NULL AND false = false, NULL AND true = NULL
fn eval_and(left: &Value, right: &Value) -> Result<Value> {
    let l = to_bool_or_null(left)?;
    let r = to_bool_or_null(right)?;
    match (l, r) {
        (Some(false), _) | (_, Some(false)) => Ok(Value::Boolean(false)),
        (Some(true), Some(true)) => Ok(Value::Boolean(true)),
        _ => Ok(Value::Null),
    }
}

/// SQL three-valued OR: NULL OR true = true, NULL OR false = NULL
fn eval_or(left: &Value, right: &Value) -> Result<Value> {
    let l = to_bool_or_null(left)?;
    let r = to_bool_or_null(right)?;
    match (l, r) {
        (Some(true), _) | (_, Some(true)) => Ok(Value::Boolean(true)),
        (Some(false), Some(false)) => Ok(Value::Boolean(false)),
        _ => Ok(Value::Null),
    }
}

fn to_bool_or_null(val: &Value) -> Result<Option<bool>> {
    match val {
        Value::Boolean(b) => Ok(Some(*b)),
        Value::Null => Ok(None),
        Value::Integer(i) => Ok(Some(*i != 0)),
        _ => Err(SqlError::TypeMismatch {
            expected: "BOOLEAN".into(),
            got: format!("{}", val.data_type()),
        }),
    }
}

fn eval_arithmetic(
    left: &Value,
    right: &Value,
    int_op: fn(i64, i64) -> Option<i64>,
    real_op: fn(f64, f64) -> f64,
) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => {
            int_op(*a, *b).map(Value::Integer).ok_or(SqlError::IntegerOverflow)
        }
        (Value::Real(a), Value::Real(b)) => Ok(Value::Real(real_op(*a, *b))),
        (Value::Integer(a), Value::Real(b)) => Ok(Value::Real(real_op(*a as f64, *b))),
        (Value::Real(a), Value::Integer(b)) => Ok(Value::Real(real_op(*a, *b as f64))),
        _ => Err(SqlError::TypeMismatch {
            expected: "numeric".into(),
            got: format!("{} and {}", left.data_type(), right.data_type()),
        }),
    }
}

fn eval_unary_op(op: UnaryOp, val: &Value) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }
    match op {
        UnaryOp::Neg => match val {
            Value::Integer(i) => {
                i.checked_neg()
                    .map(Value::Integer)
                    .ok_or(SqlError::IntegerOverflow)
            }
            Value::Real(r) => Ok(Value::Real(-r)),
            _ => Err(SqlError::TypeMismatch {
                expected: "numeric".into(),
                got: format!("{}", val.data_type()),
            }),
        },
        UnaryOp::Not => match val {
            Value::Boolean(b) => Ok(Value::Boolean(!b)),
            Value::Integer(i) => Ok(Value::Boolean(*i == 0)),
            _ => Err(SqlError::TypeMismatch {
                expected: "BOOLEAN".into(),
                got: format!("{}", val.data_type()),
            }),
        },
    }
}

/// Check if an expression result is truthy (for WHERE/HAVING).
pub fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Boolean(b) => *b,
        Value::Integer(i) => *i != 0,
        Value::Null => false,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;

    fn test_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef { name: "id".into(), data_type: DataType::Integer, nullable: false, position: 0 },
            ColumnDef { name: "name".into(), data_type: DataType::Text, nullable: true, position: 1 },
            ColumnDef { name: "score".into(), data_type: DataType::Real, nullable: true, position: 2 },
            ColumnDef { name: "active".into(), data_type: DataType::Boolean, nullable: false, position: 3 },
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
        let row = test_row();
        let expr = Expr::Literal(Value::Integer(42));
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Integer(42));
    }

    #[test]
    fn eval_column_ref() {
        let cols = test_columns();
        let row = test_row();
        let expr = Expr::Column("name".into());
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Text("Alice".into()));
    }

    #[test]
    fn eval_column_case_insensitive() {
        let cols = test_columns();
        let row = test_row();
        let expr = Expr::Column("NAME".into());
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Text("Alice".into()));
    }

    #[test]
    fn eval_arithmetic_int() {
        let cols = test_columns();
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".into())),
            op: BinOp::Add,
            right: Box::new(Expr::Literal(Value::Integer(10))),
        };
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Integer(11));
    }

    #[test]
    fn eval_comparison() {
        let cols = test_columns();
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("score".into())),
            op: BinOp::Gt,
            right: Box::new(Expr::Literal(Value::Real(90.0))),
        };
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn eval_null_propagation() {
        let cols = test_columns();
        let row = vec![Value::Integer(1), Value::Null, Value::Null, Value::Boolean(true)];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("test".into()))),
        };
        assert!(eval_expr(&expr, &cols, &row).unwrap().is_null());
    }

    #[test]
    fn eval_and_three_valued() {
        let cols = test_columns();
        let row = vec![Value::Integer(1), Value::Null, Value::Null, Value::Boolean(true)];

        // NULL AND false = false
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::And,
            right: Box::new(Expr::Literal(Value::Boolean(false))),
        };
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Boolean(false));

        // NULL AND true = NULL
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::And,
            right: Box::new(Expr::Literal(Value::Boolean(true))),
        };
        assert!(eval_expr(&expr, &cols, &row).unwrap().is_null());
    }

    #[test]
    fn eval_or_three_valued() {
        let cols = test_columns();
        let row = vec![Value::Integer(1), Value::Null, Value::Null, Value::Boolean(true)];

        // NULL OR true = true
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Or,
            right: Box::new(Expr::Literal(Value::Boolean(true))),
        };
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Boolean(true));

        // NULL OR false = NULL
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Or,
            right: Box::new(Expr::Literal(Value::Boolean(false))),
        };
        assert!(eval_expr(&expr, &cols, &row).unwrap().is_null());
    }

    #[test]
    fn eval_is_null() {
        let cols = test_columns();
        let row = vec![Value::Integer(1), Value::Null, Value::Null, Value::Boolean(true)];
        let expr = Expr::IsNull(Box::new(Expr::Column("name".into())));
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Boolean(true));

        let expr = Expr::IsNotNull(Box::new(Expr::Column("id".into())));
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn eval_not() {
        let cols = test_columns();
        let row = test_row();
        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Column("active".into())),
        };
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Boolean(false));
    }

    #[test]
    fn eval_neg() {
        let cols = test_columns();
        let row = test_row();
        let expr = Expr::UnaryOp {
            op: UnaryOp::Neg,
            expr: Box::new(Expr::Column("id".into())),
        };
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Integer(-1));
    }

    #[test]
    fn eval_division_by_zero() {
        let cols = test_columns();
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".into())),
            op: BinOp::Div,
            right: Box::new(Expr::Literal(Value::Integer(0))),
        };
        assert!(matches!(eval_expr(&expr, &cols, &row), Err(SqlError::DivisionByZero)));
    }

    #[test]
    fn eval_mixed_numeric() {
        let cols = test_columns();
        let row = test_row();
        // id (int 1) + score (real 95.5) = real 96.5
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".into())),
            op: BinOp::Add,
            right: Box::new(Expr::Column("score".into())),
        };
        assert_eq!(eval_expr(&expr, &cols, &row).unwrap(), Value::Real(96.5));
    }

    #[test]
    fn is_truthy_values() {
        assert!(is_truthy(&Value::Boolean(true)));
        assert!(!is_truthy(&Value::Boolean(false)));
        assert!(!is_truthy(&Value::Null));
        assert!(is_truthy(&Value::Integer(1)));
        assert!(!is_truthy(&Value::Integer(0)));
    }
}
