use super::*;
use crate::encoding::encode_row;
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
fn coerce_update_value_null_into_nullable_column_ok() {
    let c = col("v", DataType::Integer, true);
    for strict in [false, true] {
        let v = coerce_update_value(Value::Null, &c, strict).unwrap();
        assert!(matches!(v, Value::Null));
    }
}

#[test]
fn coerce_update_value_null_into_not_null_column_errors() {
    let c = col("v", DataType::Integer, false);
    for strict in [false, true] {
        assert!(matches!(
            coerce_update_value(Value::Null, &c, strict),
            Err(SqlError::NotNullViolation(_))
        ));
    }
}

#[test]
fn coerce_update_value_int_to_real_succeeds() {
    let c = col("v", DataType::Real, false);
    for strict in [false, true] {
        let v = coerce_update_value(i(7), &c, strict).unwrap();
        assert!(matches!(v, Value::Real(7.0)));
    }
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

fn update_database() -> Database {
    citadel::DatabaseBuilder::new("")
        .passphrase(b"test-passphrase")
        .argon2_profile(citadel::Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

#[test]
fn compiled_update_rhs_nullable_range_reads_each_row_before_generated_columns() {
    for explicit in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER, \
             g INTEGER GENERATED ALWAYS AS (a + b) STORED)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO t (id, a, b) VALUES (1, 5, 20), (2, 6, 30), (3, 7, NULL), (4, 8, 40)",
        )
        .unwrap();
        let stmt = conn
            .prepare("UPDATE t SET a = b + 1 WHERE id BETWEEN 1 AND 3")
            .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert_eq!(stmt.execute(&[]).unwrap(), 3);
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        assert_eq!(
            conn.query("SELECT id, a, b, g FROM t ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![i(1), i(21), i(20), i(41)],
                vec![i(2), i(31), i(30), i(61)],
                vec![i(3), Value::Null, Value::Null, Value::Null],
                vec![i(4), i(8), i(40), i(48)],
            ],
            "explicit transaction: {explicit}"
        );
    }
}

#[test]
fn compiled_update_rhs_nullable_seq_scan_decodes_after_filtering() {
    for explicit in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 5, 10), (2, 6, 20), (3, 7, 30)")
            .unwrap();
        let stmt = conn
            .prepare("UPDATE t SET a = b + 1 WHERE b >= 20")
            .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert_eq!(stmt.execute(&[]).unwrap(), 2);
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        assert_eq!(
            conn.query("SELECT id, a, b FROM t ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![i(1), i(5), i(10)],
                vec![i(2), i(21), i(20)],
                vec![i(3), i(31), i(30)],
            ],
            "explicit transaction: {explicit}"
        );
    }
}

#[test]
fn compiled_update_rhs_composite_pk_decodes_non_target_column() {
    for explicit in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (p INTEGER NOT NULL, q INTEGER NOT NULL, a INTEGER, b INTEGER, \
             PRIMARY KEY (p, q))",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1, 5, 20), (1, 2, 6, 30), (2, 2, 7, 40)")
            .unwrap();
        let stmt = conn
            .prepare("UPDATE t SET a = b + $1 WHERE p = 1 AND q = 2")
            .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert_eq!(stmt.execute(&[i(3)]).unwrap(), 1);
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        assert_eq!(
            conn.query("SELECT p, q, a, b FROM t ORDER BY p, q")
                .unwrap()
                .rows,
            vec![
                vec![i(1), i(1), i(5), i(20)],
                vec![i(1), i(2), i(33), i(30)],
                vec![i(2), i(2), i(7), i(40)],
            ],
            "explicit transaction: {explicit}"
        );
    }
}

#[test]
fn compiled_update_rhs_simultaneous_uses_old_values_and_generated_uses_new_values() {
    for not_null in [true, false] {
        for explicit in [false, true] {
            let db = update_database();
            let conn = crate::Connection::open(&db).unwrap();
            let nullable = if not_null { "NOT NULL" } else { "" };
            conn.execute(&format!(
                "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER {nullable}, \
                 b INTEGER {nullable}, c INTEGER {nullable}, \
                 g INTEGER GENERATED ALWAYS AS (a * 100 + b) STORED {nullable})"
            ))
            .unwrap();
            conn.execute("INSERT INTO t (id, a, b, c) VALUES (1, 5, 20, 3), (2, 6, 30, 4)")
                .unwrap();
            let stmt = conn
                .prepare("UPDATE t SET a = b + c, b = a + c WHERE id BETWEEN 1 AND 2")
                .unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            assert_eq!(stmt.execute(&[]).unwrap(), 2);
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
            assert_eq!(
                conn.query("SELECT id, a, b, c, g FROM t ORDER BY id")
                    .unwrap()
                    .rows,
                vec![
                    vec![i(1), i(23), i(8), i(3), i(2308)],
                    vec![i(2), i(34), i(10), i(4), i(3410)],
                ],
                "not null: {not_null}; explicit transaction: {explicit}"
            );
        }
    }
}

fn arithmetic_update_target(
    op: BinOp,
    operand: i64,
    parameter: bool,
    commuted: bool,
) -> CompiledTarget {
    let column = Expr::Column("v".into());
    let operand = if parameter {
        Expr::Parameter(1)
    } else {
        Expr::Literal(i(operand))
    };
    let (left, right) = if commuted {
        (operand, column)
    } else {
        (column, operand)
    };
    let expr = Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    };
    CompiledTarget {
        schema_idx: 0,
        phys_idx: 0,
        fast_eval: detect_fast_eval(&expr, "v"),
        expr,
        col: col("v", DataType::Integer, true),
    }
}

#[test]
fn compiled_update_fast_arithmetic_overflow_matches_generic() {
    let columns = vec![col("v", DataType::Integer, true)];
    let col_map = ColumnMap::new(&columns);
    for (op, operand, overflowing, boundary) in [
        (BinOp::Add, 1, i64::MAX, i64::MAX - 1),
        (BinOp::Sub, 1, i64::MIN, i64::MIN + 1),
        (BinOp::Mul, 2, i64::MAX, i64::MAX / 2),
    ] {
        for parameter in [false, true] {
            for commuted in [false, true] {
                if commuted && op == BinOp::Sub {
                    continue;
                }
                let target = arithmetic_update_target(op, operand, parameter, commuted);
                assert!(!matches!(&target.fast_eval, FastEval::None));
                crate::eval::with_scoped_params(&[i(operand)], || {
                    let row = [i(overflowing)];
                    assert!(matches!(
                        eval_expr(&target.expr, &EvalCtx::new(&col_map, &row)),
                        Err(SqlError::IntegerOverflow)
                    ));
                    assert!(
                        matches!(
                            compiled_target_eval(&target, &row, &col_map, None),
                            Err(SqlError::IntegerOverflow)
                        ),
                        "operator: {op:?}; parameter: {parameter}; commuted: {commuted}"
                    );
                    for value in [
                        i(boundary),
                        Value::Null,
                        Value::Real(1.5),
                        Value::Text("not numeric".into()),
                    ] {
                        let row = [value];
                        let generic = eval_expr(&target.expr, &EvalCtx::new(&col_map, &row));
                        let compiled = compiled_target_eval(&target, &row, &col_map, None);
                        assert_eq!(
                            compiled.map_err(|error| error.to_string()),
                            generic.map_err(|error| error.to_string()),
                            "operator: {op:?}; parameter: {parameter}; commuted: {commuted}"
                        );
                    }
                });
                if parameter {
                    for operand in [Value::Null, Value::Real(1.5), Value::Text("bad".into())] {
                        crate::eval::with_scoped_params(&[operand], || {
                            let row = [i(5)];
                            let generic = eval_expr(&target.expr, &EvalCtx::new(&col_map, &row));
                            let compiled = compiled_target_eval(&target, &row, &col_map, None);
                            assert_eq!(
                                compiled.map_err(|error| error.to_string()),
                                generic.map_err(|error| error.to_string()),
                                "non-integer parameter; operator: {op:?}; commuted: {commuted}"
                            );
                        });
                    }
                }
            }
        }
    }
}

#[test]
fn compiled_update_later_overflow_aborts_autocommit() {
    for not_null in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        let constraint = if not_null { "NOT NULL" } else { "" };
        conn.execute(&format!(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER {constraint})"
        ))
        .unwrap();
        conn.execute(&format!("INSERT INTO t VALUES (1, 10), (2, {})", i64::MAX))
            .unwrap();
        let read = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
        let before = vec![vec![i(1), i(10)], vec![i(2), i(i64::MAX)]];
        assert_eq!(read.query_collect(&[]).unwrap().rows, before);
        let update = conn
            .prepare("UPDATE t SET v = v + $1 WHERE id BETWEEN 1 AND 2")
            .unwrap();
        assert!(
            matches!(update.execute(&[i(1)]), Err(SqlError::IntegerOverflow)),
            "not null: {not_null}"
        );
        assert_eq!(read.query_collect(&[]).unwrap().rows, before);
        // The failed autocommit releases its writer; a later successful write
        // must also invalidate the already-used SELECT result.
        conn.execute("UPDATE t SET v = 12 WHERE id = 1").unwrap();
        assert_eq!(
            read.query_collect(&[]).unwrap().rows,
            vec![vec![i(1), i(12)], vec![i(2), i(i64::MAX)]]
        );
    }
}

#[test]
fn compiled_update_later_overflow_refuses_explicit_prefix() {
    for commit in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER NOT NULL)")
            .unwrap();
        conn.execute(&format!("INSERT INTO t VALUES (1, 10), (2, {})", i64::MAX))
            .unwrap();
        let read = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
        let before = vec![vec![i(1), i(10)], vec![i(2), i(i64::MAX)]];
        assert_eq!(read.query_collect(&[]).unwrap().rows, before);
        let update = conn
            .prepare("UPDATE t SET v = v + 1 WHERE id BETWEEN 1 AND 2")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        assert!(matches!(
            update.execute(&[]),
            Err(SqlError::IntegerOverflow)
        ));
        assert!(matches!(
            conn.query("SELECT 1"),
            Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
        ));
        if commit {
            // A refused COMMIT aborts the failed transaction itself.
            assert!(matches!(
                conn.execute("COMMIT"),
                Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
            ));
        } else {
            conn.execute("ROLLBACK").unwrap();
        }
        assert!(!conn.in_transaction());
        assert_eq!(read.query_collect(&[]).unwrap().rows, before);
        conn.execute("UPDATE t SET v = 12 WHERE id = 1").unwrap();
        assert_eq!(
            read.query_collect(&[]).unwrap().rows,
            vec![vec![i(1), i(12)], vec![i(2), i(i64::MAX)]]
        );
    }
}

#[test]
fn compiled_update_reused_nullable_and_text_width_changes() {
    for explicit in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, n INTEGER, s TEXT, tail TEXT NOT NULL)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO t VALUES (1, NULL, NULL, 'first'), (2, 20, 'seed', 'second'), \
             (3, 30, 'untouched', 'third')",
        )
        .unwrap();
        let update = conn
            .prepare("UPDATE t SET n = $1, s = $2 WHERE id BETWEEN 1 AND 2")
            .unwrap();
        let read = conn
            .prepare("SELECT id, n, s, tail FROM t ORDER BY id")
            .unwrap();
        assert_eq!(read.query_collect(&[]).unwrap().rows.len(), 3);
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        for (number, text) in [
            (i(7), Value::Text("short".into())),
            (Value::Null, Value::Text("L".repeat(6000).into())),
            (i(8), Value::Null),
            (Value::Null, Value::Text("M".repeat(8000).into())),
            (i(9), Value::Text("x".into())),
            (Value::Null, Value::Null),
        ] {
            assert_eq!(update.execute(&[number.clone(), text.clone()]).unwrap(), 2);
            assert_eq!(
                read.query_collect(&[]).unwrap().rows,
                vec![
                    vec![
                        i(1),
                        number.clone(),
                        text.clone(),
                        Value::Text("first".into())
                    ],
                    vec![i(2), number, text, Value::Text("second".into())],
                    vec![
                        i(3),
                        i(30),
                        Value::Text("untouched".into()),
                        Value::Text("third".into())
                    ],
                ],
                "explicit transaction: {explicit}"
            );
        }
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        assert_eq!(read.query_collect(&[]).unwrap().rows[0][1], Value::Null);
    }
}

#[test]
fn compiled_update_returning_relocates_generated_columns_after_row_rebuilds() {
    for explicit in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, s TEXT, n INTEGER NOT NULL, \
             g TEXT GENERATED ALWAYS AS (COALESCE(s, 'nil') || ':') STORED, \
             h INTEGER GENERATED ALWAYS AS (LENGTH(COALESCE(s, '')) + n) STORED, \
             tail TEXT NOT NULL)",
        )
        .unwrap();
        conn.execute("INSERT INTO t (id, s, n, tail) VALUES (1, 'a', 10, 'first'), (2, 'seed', 20, 'second')")
            .unwrap();
        let update = conn
            .prepare(
                "UPDATE t SET s = $1, n = LENGTH(COALESCE(s, '')) + n WHERE id = $2 \
             RETURNING id, s, n, g, h, tail",
            )
            .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        let mut old_lengths = [1, 4];
        let mut numbers = [10, 20];
        for (id, text) in [
            (1, Some("L".repeat(6000))),
            (2, None),
            (1, None),
            (2, Some("x".into())),
            (1, Some("tail".into())),
            (2, Some("M".repeat(8000))),
        ] {
            let index = id as usize - 1;
            numbers[index] += old_lengths[index];
            let length = text.as_ref().map_or(0, |value| value.len() as i64);
            let generated = format!("{}:", text.as_deref().unwrap_or("nil"));
            let text = text.map_or(Value::Null, |value| Value::Text(value.into()));
            let expected = vec![
                i(id),
                text.clone(),
                i(numbers[index]),
                Value::Text(generated.into()),
                i(length + numbers[index]),
                Value::Text(if id == 1 { "first" } else { "second" }.into()),
            ];
            assert_eq!(
                update.query_collect(&[text, i(id)]).unwrap().rows,
                vec![expected.clone()],
                "explicit transaction: {explicit}; row: {id}",
            );
            assert_eq!(
                conn.query(&format!("SELECT * FROM t WHERE id = {id}"))
                    .unwrap()
                    .rows,
                vec![expected],
            );
            old_lengths[index] = length;
        }
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
    }
}

#[test]
fn compiled_update_nullable_patch_eligibility_is_narrow_and_runtime_bound() {
    for op in [BinOp::Add, BinOp::Sub, BinOp::Mul] {
        for parameter in [false, true] {
            for commuted in [false, true] {
                let target = arithmetic_update_target(op, 2, parameter, commuted);
                crate::eval::with_scoped_params(&[i(2)], || {
                    assert_eq!(
                        compiled_target_patch_safe(&target),
                        !(commuted && op == BinOp::Sub),
                        "operator: {op:?}; parameter: {parameter}; commuted: {commuted}"
                    );
                });
                if parameter {
                    for value in [Value::Null, Value::Real(2.0), Value::Text("2".into())] {
                        crate::eval::with_scoped_params(&[value], || {
                            assert!(!compiled_target_patch_safe(&target));
                        });
                    }
                    crate::eval::with_scoped_params(&[], || {
                        assert!(!compiled_target_patch_safe(&target));
                    });
                }
            }
        }
    }

    for fast_eval in [
        FastEval::None,
        FastEval::IntSet(2),
        FastEval::IntSetParam(1),
    ] {
        let mut target = arithmetic_update_target(BinOp::Add, 2, false, false);
        target.fast_eval = fast_eval;
        crate::eval::with_scoped_params(&[i(2)], || {
            assert!(!compiled_target_patch_safe(&target));
        });
    }
    for data_type in [DataType::Real, DataType::Boolean, DataType::Text] {
        let mut target = arithmetic_update_target(BinOp::Add, 2, false, false);
        target.col.data_type = data_type;
        assert!(!compiled_target_patch_safe(&target));
    }
    let mut target = arithmetic_update_target(BinOp::Add, 2, false, false);
    target.col.default_expr = Some(Expr::Literal(i(3)));
    assert!(compiled_target_patch_safe(&target));
    target.col.nullable = false;
    assert!(compiled_target_patch_safe(&target));
    for expr in [
        Expr::Literal(Value::Text("7".into())),
        Expr::Literal(Value::Real(7.0)),
        Expr::Literal(Value::Null),
        Expr::BinaryOp {
            left: Box::new(Expr::Literal(i(3))),
            op: BinOp::Add,
            right: Box::new(Expr::Literal(i(4))),
        },
    ] {
        target.col.default_expr = Some(expr);
        assert!(!compiled_target_patch_safe(&target));
        assert!(!pk_range_patch_safe(std::slice::from_ref(&target.col), &[]));
    }
    target.col.nullable = true;
    target.col.default_expr = Some(Expr::Literal(Value::Null));
    assert!(compiled_target_patch_safe(&target));
}

#[test]
fn compiled_update_nullable_self_arithmetic_preserves_nulls_and_sentinels() {
    for (expression, positive, negative) in [
        ("v + 2", 12, -2),
        ("2 + v", 12, -2),
        ("v - 2", 8, -6),
        ("v * 2", 20, -8),
        ("2 * v", 20, -8),
    ] {
        for explicit in [false, true] {
            let db = update_database();
            let conn = crate::Connection::open(&db).unwrap();
            conn.execute(
                "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER, tail TEXT NOT NULL)",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO t VALUES (1, NULL, 'null'), (2, 10, 'positive'), \
                 (3, -4, 'negative'), (4, 99, 'outside')",
            )
            .unwrap();
            let update = conn
                .prepare(&format!(
                    "UPDATE t SET v = {expression} WHERE id BETWEEN 1 AND 3"
                ))
                .unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            assert_eq!(update.execute(&[]).unwrap(), 3);
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
            assert_eq!(
                conn.query("SELECT id, v, tail FROM t ORDER BY id")
                    .unwrap()
                    .rows,
                vec![
                    vec![i(1), Value::Null, Value::Text("null".into())],
                    vec![i(2), i(positive), Value::Text("positive".into())],
                    vec![i(3), i(negative), Value::Text("negative".into())],
                    vec![i(4), i(99), Value::Text("outside".into())],
                ],
                "expression: {expression}; explicit transaction: {explicit}"
            );
        }
    }
}

#[test]
fn compiled_update_nullable_parameter_changes_integer_null_integer() {
    for explicit in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER, tail TEXT NOT NULL)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO t VALUES (1, NULL, 'first'), (2, 10, 'second'), (3, 30, 'outside')",
        )
        .unwrap();
        let update = conn
            .prepare("UPDATE t SET v = v + $1 WHERE id BETWEEN 1 AND 2")
            .unwrap();
        let read = conn
            .prepare("SELECT id, v, tail FROM t ORDER BY id")
            .unwrap();
        for (operand, expected) in [(i(2), i(12)), (Value::Null, Value::Null), (i(2), i(22))] {
            if expected == i(22) {
                conn.execute("UPDATE t SET v = 20 WHERE id = 2").unwrap();
            }
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            assert_eq!(update.execute(&[operand]).unwrap(), 2);
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
            assert_eq!(
                read.query_collect(&[]).unwrap().rows,
                vec![
                    vec![i(1), Value::Null, Value::Text("first".into())],
                    vec![i(2), expected.clone(), Value::Text("second".into())],
                    vec![i(3), i(30), Value::Text("outside".into())],
                ]
            );
            assert_eq!(
                stored_update_row(&db, 2),
                crate::encoding::encode_row(&[expected, Value::Text("second".into())]),
                "a NULL parameter must use the resizing path without trailing bytes"
            );
        }
    }
}

fn stored_update_row(db: &Database, id: i64) -> Vec<u8> {
    db.begin_read()
        .table_get(b"t", &encode_composite_key(&[i(id)]))
        .unwrap()
        .unwrap()
}

fn nullable_update_v1_row(value: Option<i64>, tail: &str, sentinel: i64) -> Vec<u8> {
    let mut bytes = 3u16.to_le_bytes().to_vec();
    bytes.push(u8::from(value.is_none()));
    if let Some(value) = value {
        bytes.push(DataType::Integer.type_tag());
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes.push(DataType::Text.type_tag());
    bytes.extend_from_slice(&(tail.len() as u32).to_le_bytes());
    bytes.extend_from_slice(tail.as_bytes());
    bytes.push(DataType::Integer.type_tag());
    bytes.extend_from_slice(&8u32.to_le_bytes());
    bytes.extend_from_slice(&sentinel.to_le_bytes());
    bytes
}

#[test]
fn compiled_update_nullable_v1_null_row_is_not_reencoded() {
    for explicit in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER, \
             tail TEXT NOT NULL, sentinel INTEGER NOT NULL)",
        )
        .unwrap();
        let null_row = nullable_update_v1_row(None, "unchanged bytes", 123);
        let integer_row = nullable_update_v1_row(Some(7), "integer tail", 456);
        let mut wtx = db.begin_write().unwrap();
        wtx.table_insert(b"t", &encode_composite_key(&[i(1)]), &null_row)
            .unwrap();
        wtx.table_insert(b"t", &encode_composite_key(&[i(2)]), &integer_row)
            .unwrap();
        wtx.commit().unwrap();
        let update = conn
            .prepare("UPDATE t SET v = v + 2 WHERE id BETWEEN 1 AND 2")
            .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert_eq!(update.execute(&[]).unwrap(), 2);
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        assert_eq!(stored_update_row(&db, 1), null_row);
        assert_eq!(
            stored_update_row(&db, 2),
            nullable_update_v1_row(Some(9), "integer tail", 456)
        );
        assert_eq!(
            conn.query("SELECT id, v, tail, sentinel FROM t ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![
                    i(1),
                    Value::Null,
                    Value::Text("unchanged bytes".into()),
                    i(123)
                ],
                vec![i(2), i(9), Value::Text("integer tail".into()), i(456)],
            ]
        );
    }
}

#[test]
fn compiled_update_nullable_missing_added_slot_materializes_as_null() {
    for explicit in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, c0 INTEGER, c1 INTEGER, \
             c2 INTEGER, c3 INTEGER, c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER)",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10, 11, 12, 13, 14, 15, 16, 17), (2, 20, 21, 22, 23, 24, 25, 26, 27)")
            .unwrap();
        let before = [stored_update_row(&db, 1), stored_update_row(&db, 2)];
        conn.execute("ALTER TABLE t ADD COLUMN v INTEGER").unwrap();
        let update = conn
            .prepare("UPDATE t SET v = v + 1 WHERE id BETWEEN 1 AND 2")
            .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert_eq!(update.execute(&[]).unwrap(), 2);
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        for (index, raw) in before.iter().enumerate() {
            let mut values = crate::encoding::decode_row(raw).unwrap();
            values.push(Value::Null);
            assert_eq!(
                stored_update_row(&db, index as i64 + 1),
                encode_row(&values)
            );
        }
        assert_eq!(
            conn.query("SELECT id, c0, c7, v FROM t ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![i(1), i(10), i(17), Value::Null],
                vec![i(2), i(20), i(27), Value::Null]
            ]
        );
    }
}

#[test]
fn compiled_update_nullable_multi_target_preserves_nulls_and_residual_filter() {
    for explicit in [false, true] {
        for residual in [false, true] {
            let db = update_database();
            let conn = crate::Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER, tail TEXT NOT NULL)")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, NULL, 5, 'keep'), (2, 7, NULL, 'skip'), (3, NULL, NULL, 'keep'), (4, 11, 12, 'outside')")
                .unwrap();
            let filter = if residual { " AND tail = 'keep'" } else { "" };
            let update = conn
                .prepare(&format!(
                    "UPDATE t SET a = a + 1, b = b * 2 WHERE id BETWEEN 1 AND 3{filter}"
                ))
                .unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            assert_eq!(update.execute(&[]).unwrap(), if residual { 2 } else { 3 });
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
            assert_eq!(
                conn.query("SELECT id, a, b, tail FROM t ORDER BY id")
                    .unwrap()
                    .rows,
                vec![
                    vec![i(1), Value::Null, i(10), Value::Text("keep".into())],
                    vec![
                        i(2),
                        i(if residual { 7 } else { 8 }),
                        Value::Null,
                        Value::Text("skip".into())
                    ],
                    vec![i(3), Value::Null, Value::Null, Value::Text("keep".into())],
                    vec![i(4), i(11), i(12), Value::Text("outside".into())],
                ]
            );
        }
    }
}

#[test]
fn compiled_update_nullable_first_and_later_overflow_refuses_explicit_prefix() {
    for (first, commit) in [(false, false), (false, true), (true, false), (true, true)] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER)")
            .unwrap();
        let (first_value, last_value) = if first {
            (i(i64::MAX), Value::Null)
        } else {
            (Value::Null, i(i64::MAX))
        };
        conn.execute(&format!(
            "INSERT INTO t VALUES (1, {first_value}), (2, 10), (3, {last_value})"
        ))
        .unwrap();
        let before = vec![
            vec![i(1), first_value],
            vec![i(2), i(10)],
            vec![i(3), last_value],
        ];
        let update = conn
            .prepare("UPDATE t SET v = v + 1 WHERE id BETWEEN 1 AND 3")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        assert!(matches!(
            update.execute(&[]),
            Err(SqlError::IntegerOverflow)
        ));
        assert!(matches!(
            conn.query("SELECT 1"),
            Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
        ));
        if commit {
            assert!(matches!(
                conn.execute("COMMIT"),
                Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
            ));
        } else {
            conn.execute("ROLLBACK").unwrap();
        }
        assert!(!conn.in_transaction());
        assert_eq!(
            conn.query("SELECT id, v FROM t ORDER BY id").unwrap().rows,
            before
        );
        conn.execute("UPDATE t SET v = 12 WHERE id = 2").unwrap();
        assert_eq!(
            conn.query("SELECT v FROM t WHERE id = 2").unwrap().rows,
            vec![vec![i(12)]]
        );
    }
}

#[test]
fn compiled_update_nullable_with_added_default_uses_resizing_path() {
    for explicit in [false, true] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, NULL), (2, 2), (3, 3)")
            .unwrap();
        let outside = stored_update_row(&db, 3);
        conn.execute("ALTER TABLE t ADD COLUMN b INTEGER NOT NULL DEFAULT 0")
            .unwrap();
        let update = conn
            .prepare("UPDATE t SET a = a + 1, b = 7 WHERE id BETWEEN 1 AND 2")
            .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert_eq!(update.execute(&[]).unwrap(), 2);
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        assert_eq!(
            conn.query("SELECT id, a, b FROM t ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![i(1), Value::Null, i(7)],
                vec![i(2), i(3), i(7)],
                vec![i(3), i(3), i(0)]
            ]
        );
        assert_eq!(stored_update_row(&db, 1), encode_row(&[Value::Null, i(7)]));
        assert_eq!(stored_update_row(&db, 2), encode_row(&[i(3), i(7)]));
        assert_eq!(stored_update_row(&db, 3), outside);
    }
}

#[test]
fn general_autocommit_update_materializes_added_defaults_in_both_fast_paths() {
    for fixed_width in [true, false] {
        let db = update_database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)")
            .unwrap();
        let outside = stored_update_row(&db, 3);
        conn.execute("ALTER TABLE t ADD COLUMN b INTEGER DEFAULT 10")
            .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN tail TEXT DEFAULT 'keep'")
            .unwrap();
        let schema = SchemaManager::load(&db).unwrap();
        let updates: &[(&str, u64)] = if fixed_width {
            &[("UPDATE t SET a = a + b WHERE id >= 1 AND id <= 2", 2)]
        } else {
            &[
                ("UPDATE t SET b = b + 1 WHERE id = 1", 1),
                ("UPDATE t SET b = b + 1 WHERE id >= 2 AND id <= 2", 1),
            ]
        };
        for &(sql, expected_count) in updates {
            let Statement::Update(update) = crate::parser::parse_sql(sql).unwrap() else {
                panic!("expected UPDATE");
            };
            // Connection::execute autocompiles autocommit UPDATE. Call this
            // executor directly to cover its fixed-slice and collected loops.
            assert!(matches!(
                exec_update(&db, &schema, &update).unwrap(),
                ExecutionResult::RowsAffected(count) if count == expected_count
            ));
        }
        let expected_b = if fixed_width { 10 } else { 11 };
        let expected_a = |id| if fixed_width { id + 10 } else { id };
        assert_eq!(
            conn.query("SELECT * FROM t ORDER BY id").unwrap().rows,
            vec![
                vec![
                    i(1),
                    i(expected_a(1)),
                    i(expected_b),
                    Value::Text("keep".into())
                ],
                vec![
                    i(2),
                    i(expected_a(2)),
                    i(expected_b),
                    Value::Text("keep".into())
                ],
                vec![i(3), i(3), i(10), Value::Text("keep".into())],
            ]
        );
        for id in 1..=2 {
            assert_eq!(
                stored_update_row(&db, id),
                encode_row(&[i(expected_a(id)), i(expected_b), Value::Text("keep".into())])
            );
        }
        assert_eq!(stored_update_row(&db, 3), outside);
    }
}

#[test]
fn compiled_update_repeated_target_keeps_final_null_assignment() {
    for explicit in [false, true] {
        for condition in ["id = $1", "id BETWEEN $1 AND $1"] {
            let db = update_database();
            let conn = crate::Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, x INTEGER, g INTEGER GENERATED ALWAYS AS (COALESCE(x, -1)) STORED)").unwrap();
            conn.execute("INSERT INTO t (id, x) VALUES (1, NULL), (2, 7)")
                .unwrap();
            let update = conn
                .prepare(&format!("UPDATE t SET x = 1, x = NULL WHERE {condition}"))
                .unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            assert_eq!(update.execute(&[i(1)]).unwrap(), 1);
            assert_eq!(
                conn.query("SELECT id, x, g FROM t ORDER BY id")
                    .unwrap()
                    .rows,
                vec![vec![i(1), Value::Null, i(-1)], vec![i(2), i(7), i(7)]]
            );
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
        }
    }
}
