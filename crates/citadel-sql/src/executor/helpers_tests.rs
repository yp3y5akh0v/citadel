use super::*;
use crate::eval::ColumnMap;
use crate::parser::{BinOp, Expr, GeneratedKind, SelectColumn};
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

fn schema(name: &str, cs: Vec<ColumnDef>, pk: Vec<u16>) -> TableSchema {
    TableSchema::new(name.into(), cs, pk, vec![], vec![], vec![])
}

fn i(n: i64) -> Value {
    Value::Integer(n)
}

#[test]
fn coerce_for_column_int_to_real() {
    let c = col("v", DataType::Real);
    let r = coerce_for_column(i(7), &c, false).unwrap();
    assert!(matches!(r, Value::Real(_)));
}

#[test]
fn coerce_for_column_null_passes_through_when_nullable() {
    let c = col("v", DataType::Integer);
    let r = coerce_for_column(Value::Null, &c, false).unwrap();
    assert!(matches!(r, Value::Null));
}

#[test]
fn coerce_for_column_strict_mismatch_errors() {
    let c = col("v", DataType::Integer);
    let r = coerce_for_column(Value::Text("notanumber".into()), &c, true);
    assert!(r.is_err());
}

#[test]
fn eval_const_int_basic() {
    let e = Expr::Literal(i(42));
    assert_eq!(eval_const_int(&e).unwrap(), 42);
}

#[test]
fn eval_const_int_wrong_type_errors() {
    let e = Expr::Literal(Value::Text("abc".into()));
    assert!(eval_const_int(&e).is_err());
}

#[test]
fn eval_const_int_from_arithmetic() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Literal(i(2))),
        op: BinOp::Add,
        right: Box::new(Expr::Literal(i(3))),
    };
    assert_eq!(eval_const_int(&e).unwrap(), 5);
}

#[test]
fn eval_const_expr_basic() {
    let e = Expr::Literal(i(99));
    assert_eq!(eval_const_expr(&e).unwrap(), i(99));
}

#[test]
fn expr_display_name_column() {
    assert_eq!(expr_display_name(&Expr::Column("x".into())), "x");
}

#[test]
fn expr_display_name_qualified() {
    assert_eq!(
        expr_display_name(&Expr::QualifiedColumn {
            table: "t".into(),
            column: "x".into()
        }),
        "t.x"
    );
}

#[test]
fn expr_display_name_count_star() {
    assert_eq!(expr_display_name(&Expr::CountStar), "COUNT(*)");
}

#[test]
fn expr_display_name_function() {
    let e = Expr::Function {
        name: "UPPER".into(),
        args: vec![Expr::Column("name".into())],
        distinct: false,
    };
    assert_eq!(expr_display_name(&e), "UPPER(name)");
}

#[test]
fn expr_display_name_function_distinct() {
    let e = Expr::Function {
        name: "COUNT".into(),
        args: vec![Expr::Column("id".into())],
        distinct: true,
    };
    assert_eq!(expr_display_name(&e), "COUNT(DISTINCT id)");
}

#[test]
fn expr_display_name_binary_op() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(1))),
    };
    assert_eq!(expr_display_name(&e), "a = 1");
}

#[test]
fn op_symbol_comparison() {
    assert_eq!(op_symbol(&BinOp::Eq), "=");
    assert_eq!(op_symbol(&BinOp::NotEq), "<>");
    assert_eq!(op_symbol(&BinOp::LtEq), "<=");
    assert_eq!(op_symbol(&BinOp::GtEq), ">=");
}

#[test]
fn op_symbol_arithmetic() {
    assert_eq!(op_symbol(&BinOp::Add), "+");
    assert_eq!(op_symbol(&BinOp::Sub), "-");
    assert_eq!(op_symbol(&BinOp::Mul), "*");
    assert_eq!(op_symbol(&BinOp::Div), "/");
    assert_eq!(op_symbol(&BinOp::Mod), "%");
}

#[test]
fn op_symbol_logical() {
    assert_eq!(op_symbol(&BinOp::And), "AND");
    assert_eq!(op_symbol(&BinOp::Or), "OR");
    assert_eq!(op_symbol(&BinOp::Concat), "||");
}

#[test]
fn op_symbol_json() {
    assert_eq!(op_symbol(&BinOp::JsonGet), "->");
    assert_eq!(op_symbol(&BinOp::JsonGetText), "->>");
    assert_eq!(op_symbol(&BinOp::JsonContains), "@>");
}

#[test]
fn infer_expr_type_column_in_columns() {
    let cs = cols(&[("x", DataType::Integer), ("y", DataType::Text)]);
    assert_eq!(
        infer_expr_type(&Expr::Column("x".into()), &cs),
        DataType::Integer
    );
    assert_eq!(
        infer_expr_type(&Expr::Column("y".into()), &cs),
        DataType::Text
    );
}

#[test]
fn infer_expr_type_unknown_column_returns_null() {
    let cs = cols(&[("x", DataType::Integer)]);
    assert_eq!(
        infer_expr_type(&Expr::Column("missing".into()), &cs),
        DataType::Null
    );
}

#[test]
fn infer_expr_type_literal() {
    assert_eq!(
        infer_expr_type(&Expr::Literal(i(1)), &[]),
        DataType::Integer
    );
    assert_eq!(
        infer_expr_type(&Expr::Literal(Value::Text("x".into())), &[]),
        DataType::Text
    );
}

#[test]
fn infer_expr_type_count_star() {
    assert_eq!(infer_expr_type(&Expr::CountStar, &[]), DataType::Integer);
}

#[test]
fn infer_expr_type_count_function() {
    let e = Expr::Function {
        name: "COUNT".into(),
        args: vec![Expr::Column("x".into())],
        distinct: false,
    };
    assert_eq!(infer_expr_type(&e, &[]), DataType::Integer);
}

#[test]
fn infer_expr_type_avg_function() {
    let e = Expr::Function {
        name: "AVG".into(),
        args: vec![Expr::Column("x".into())],
        distinct: false,
    };
    assert_eq!(infer_expr_type(&e, &[]), DataType::Real);
}

#[test]
fn detect_fast_gen_eval_col_add_col() {
    let ts = schema(
        "t",
        cols(&[("a", DataType::Integer), ("b", DataType::Integer)]),
        vec![],
    );
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Add,
        right: Box::new(Expr::Column("b".into())),
    };
    assert!(matches!(
        detect_fast_gen_eval(&e, &ts),
        FastGenEval::IntColAddCol { .. }
    ));
}

#[test]
fn detect_fast_gen_eval_col_mul_lit() {
    let ts = schema("t", cols(&[("a", DataType::Integer)]), vec![]);
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Mul,
        right: Box::new(Expr::Literal(i(3))),
    };
    assert!(matches!(
        detect_fast_gen_eval(&e, &ts),
        FastGenEval::IntColMulAdd { mul: 3, add: 0, .. }
    ));
}

#[test]
fn detect_fast_gen_eval_col_mul_add_lit() {
    let ts = schema("t", cols(&[("a", DataType::Integer)]), vec![]);
    let inner = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Mul,
        right: Box::new(Expr::Literal(i(2))),
    };
    let e = Expr::BinaryOp {
        left: Box::new(inner),
        op: BinOp::Add,
        right: Box::new(Expr::Literal(i(10))),
    };
    assert!(matches!(
        detect_fast_gen_eval(&e, &ts),
        FastGenEval::IntColMulAdd {
            mul: 2,
            add: 10,
            ..
        }
    ));
}

#[test]
fn detect_fast_gen_eval_non_matching_returns_none_variant() {
    let ts = schema("t", cols(&[("a", DataType::Integer)]), vec![]);
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Sub,
        right: Box::new(Expr::Literal(i(1))),
    };
    assert!(matches!(detect_fast_gen_eval(&e, &ts), FastGenEval::None));
}

#[test]
fn eval_fast_gen_col_add_col_int() {
    let ts = schema(
        "t",
        cols(&[("a", DataType::Integer), ("b", DataType::Integer)]),
        vec![],
    );
    let cm = ColumnMap::new(&ts.columns);
    let row = vec![i(3), i(4)];
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Add,
        right: Box::new(Expr::Column("b".into())),
    };
    let fast = detect_fast_gen_eval(&e, &ts);
    let result = eval_fast_gen(&fast, &e, &row, &cm).unwrap();
    assert_eq!(result, i(7));
}

#[test]
fn eval_fast_gen_col_mul_add() {
    let ts = schema("t", cols(&[("x", DataType::Integer)]), vec![]);
    let cm = ColumnMap::new(&ts.columns);
    let row = vec![i(5)];
    let inner = Expr::BinaryOp {
        left: Box::new(Expr::Column("x".into())),
        op: BinOp::Mul,
        right: Box::new(Expr::Literal(i(3))),
    };
    let e = Expr::BinaryOp {
        left: Box::new(inner),
        op: BinOp::Add,
        right: Box::new(Expr::Literal(i(1))),
    };
    let fast = detect_fast_gen_eval(&e, &ts);
    let result = eval_fast_gen(&fast, &e, &row, &cm).unwrap();
    assert_eq!(result, i(16));
}

#[test]
fn eval_fast_gen_falls_back_for_null_input() {
    let ts = schema(
        "t",
        cols(&[("a", DataType::Integer), ("b", DataType::Integer)]),
        vec![],
    );
    let cm = ColumnMap::new(&ts.columns);
    let row = vec![Value::Null, i(4)];
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Add,
        right: Box::new(Expr::Column("b".into())),
    };
    let fast = detect_fast_gen_eval(&e, &ts);
    let result = eval_fast_gen(&fast, &e, &row, &cm).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn materialize_virtual_evaluates_generated_columns() {
    let mut c2 = col("doubled", DataType::Integer);
    c2.position = 1;
    c2.generated_kind = Some(GeneratedKind::Virtual);
    c2.generated_expr = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("x".into())),
        op: BinOp::Mul,
        right: Box::new(Expr::Literal(i(2))),
    });
    let ts = schema(
        "t",
        {
            let mut v = cols(&[("x", DataType::Integer)]);
            v.push(c2);
            v
        },
        vec![0],
    );
    let mut row = vec![i(7), Value::Null];
    materialize_virtual(&ts, &mut row).unwrap();
    assert_eq!(row[1], i(14));
}

#[test]
fn materialize_virtual_no_op_when_no_virtual_columns() {
    let ts = schema(
        "t",
        cols(&[("x", DataType::Integer), ("y", DataType::Integer)]),
        vec![0],
    );
    let mut row = vec![i(1), i(2)];
    materialize_virtual(&ts, &mut row).unwrap();
    assert_eq!(row, vec![i(1), i(2)]);
}

#[test]
fn build_output_columns_all_columns_yields_col_n() {
    let cs = cols(&[("a", DataType::Integer)]);
    let out = build_output_columns(&[SelectColumn::AllColumns], &cs);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].name, "col0");
}

#[test]
fn build_output_columns_aliased_expr() {
    let cs = cols(&[("a", DataType::Integer)]);
    let out = build_output_columns(
        &[SelectColumn::Expr {
            expr: Expr::Column("a".into()),
            alias: Some("renamed".into()),
        }],
        &cs,
    );
    assert_eq!(out[0].name, "renamed");
    assert_eq!(out[0].data_type, DataType::Integer);
}
