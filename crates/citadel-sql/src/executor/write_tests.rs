use super::*;
use crate::parser::{
    BinOp, DerivedTable, Expr, JoinClause, JoinType, SelectColumn, SelectStmt, TableRef,
};
use crate::schema::SchemaManager;
use crate::types::{Collation, ColumnDef, DataType, TableSchema, Value};

fn col(name: &str, dt: DataType, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: dt,
        nullable,
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

fn i(n: i64) -> Value {
    Value::Integer(n)
}

fn schema(name: &str, cs: Vec<ColumnDef>, pk: Vec<u16>) -> TableSchema {
    let cs = cs
        .into_iter()
        .enumerate()
        .map(|(i, mut c)| {
            c.position = i as u16;
            c
        })
        .collect();
    TableSchema::new(name.into(), cs, pk, vec![], vec![], vec![])
}

fn empty_select() -> SelectStmt {
    SelectStmt {
        columns: vec![SelectColumn::AllColumns],
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
fn is_fixed_width_type_integer_real_boolean() {
    assert!(is_fixed_width_type(DataType::Integer));
    assert!(is_fixed_width_type(DataType::Real));
    assert!(is_fixed_width_type(DataType::Boolean));
}

#[test]
fn is_fixed_width_type_datetime_kinds() {
    assert!(is_fixed_width_type(DataType::Date));
    assert!(is_fixed_width_type(DataType::Time));
    assert!(is_fixed_width_type(DataType::Timestamp));
    assert!(is_fixed_width_type(DataType::Interval));
}

#[test]
fn is_fixed_width_type_text_and_blob_are_variable_width() {
    assert!(!is_fixed_width_type(DataType::Text));
    assert!(!is_fixed_width_type(DataType::Blob));
}

#[test]
fn pk_range_patch_safe_all_fixed_width_non_null() {
    let set_cols = vec![col("v", DataType::Integer, false)];
    let gen_cols = vec![col("g", DataType::Real, false)];
    assert!(pk_range_patch_safe(&set_cols, &gen_cols));
}

#[test]
fn pk_range_patch_safe_text_column_makes_unsafe() {
    let set_cols = vec![col("v", DataType::Text, false)];
    let gen_cols: Vec<ColumnDef> = vec![];
    assert!(!pk_range_patch_safe(&set_cols, &gen_cols));
}

#[test]
fn pk_range_patch_safe_nullable_column_makes_unsafe() {
    let set_cols = vec![col("v", DataType::Integer, true)];
    let gen_cols: Vec<ColumnDef> = vec![];
    assert!(!pk_range_patch_safe(&set_cols, &gen_cols));
}

#[test]
fn coerce_gen_value_null_into_nullable_column_ok() {
    let c = col("v", DataType::Integer, true);
    let v = coerce_gen_value(Value::Null, &c).unwrap();
    assert!(matches!(v, Value::Null));
}

#[test]
fn coerce_gen_value_null_into_not_null_column_errors() {
    let c = col("v", DataType::Integer, false);
    assert!(coerce_gen_value(Value::Null, &c).is_err());
}

#[test]
fn coerce_gen_value_int_to_real_succeeds() {
    let c = col("v", DataType::Real, false);
    let v = coerce_gen_value(i(7), &c).unwrap();
    assert!(matches!(v, Value::Real(_)));
}

#[test]
fn detect_fast_eval_int_set_literal() {
    let e = Expr::Literal(i(5));
    assert!(matches!(detect_fast_eval(&e, "v"), FastEval::IntSet(5)));
}

#[test]
fn detect_fast_eval_int_add() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("v".into())),
        op: BinOp::Add,
        right: Box::new(Expr::Literal(i(3))),
    };
    assert!(matches!(detect_fast_eval(&e, "v"), FastEval::IntAdd(3)));
}

#[test]
fn detect_fast_eval_int_sub_only_on_col_left() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("v".into())),
        op: BinOp::Sub,
        right: Box::new(Expr::Literal(i(2))),
    };
    assert!(matches!(detect_fast_eval(&e, "v"), FastEval::IntSub(2)));
}

#[test]
fn detect_fast_eval_int_mul_either_side() {
    let lhs = Expr::BinaryOp {
        left: Box::new(Expr::Column("v".into())),
        op: BinOp::Mul,
        right: Box::new(Expr::Literal(i(4))),
    };
    assert!(matches!(detect_fast_eval(&lhs, "v"), FastEval::IntMul(4)));
    let rhs = Expr::BinaryOp {
        left: Box::new(Expr::Literal(i(4))),
        op: BinOp::Mul,
        right: Box::new(Expr::Column("v".into())),
    };
    assert!(matches!(detect_fast_eval(&rhs, "v"), FastEval::IntMul(4)));
}

#[test]
fn detect_fast_eval_non_matching_returns_none() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("v".into())),
        op: BinOp::Div,
        right: Box::new(Expr::Literal(i(2))),
    };
    assert!(matches!(detect_fast_eval(&e, "v"), FastEval::None));
}

#[test]
fn detect_pk_lookup_fast_eq_literal() {
    let ts = schema("t", vec![col("id", DataType::Integer, false)], vec![0]);
    let w = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(7))),
    });
    assert!(detect_pk_lookup_fast(&w, &ts).is_some());
}

#[test]
fn detect_pk_lookup_fast_not_eq_returns_none() {
    let ts = schema("t", vec![col("id", DataType::Integer, false)], vec![0]);
    let w = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".into())),
        op: BinOp::Lt,
        right: Box::new(Expr::Literal(i(7))),
    });
    assert!(detect_pk_lookup_fast(&w, &ts).is_none());
}

#[test]
fn detect_pk_lookup_fast_no_where_clause_none() {
    let ts = schema("t", vec![col("id", DataType::Integer, false)], vec![0]);
    assert!(detect_pk_lookup_fast(&None, &ts).is_none());
}

#[test]
fn detect_pk_lookup_fast_composite_pk_none() {
    let ts = schema(
        "t",
        vec![
            col("a", DataType::Integer, false),
            col("b", DataType::Integer, false),
        ],
        vec![0, 1],
    );
    let w = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(1))),
    });
    assert!(detect_pk_lookup_fast(&w, &ts).is_none());
}

fn derived(alias: &str) -> Box<DerivedTable> {
    Box::new(DerivedTable {
        query: Box::new(crate::parser::SelectQuery {
            ctes: vec![],
            body: crate::parser::QueryBody::Select(Box::new(empty_select())),
            recursive: false,
        }),
        alias: alias.into(),
        lateral: false,
    })
}

#[test]
fn has_derived_in_stmt_from_subquery() {
    let mut s = empty_select();
    s.from_subquery = Some(derived("d"));
    assert!(has_derived_in_stmt(&s));
}

#[test]
fn has_derived_in_stmt_join_subquery() {
    let mut s = empty_select();
    s.joins.push(JoinClause {
        join_type: JoinType::Inner,
        table: TableRef {
            name: "t".into(),
            alias: None,
            args: None,
        },
        subquery: Some(derived("d")),
        on_clause: None,
    });
    assert!(has_derived_in_stmt(&s));
}

#[test]
fn has_derived_in_stmt_plain_select_false() {
    let s = empty_select();
    assert!(!has_derived_in_stmt(&s));
}

#[test]
fn compile_update_unknown_table_errors() {
    let mgr = SchemaManager::empty();
    let upd = crate::parser::UpdateStmt {
        table: "missing".into(),
        assignments: vec![("v".into(), Expr::Literal(i(1)))],
        where_clause: None,
        returning: None,
    };
    assert!(compile_update_impl(&mgr, &upd).is_err());
}
