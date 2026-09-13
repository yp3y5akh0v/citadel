use super::*;
use crate::types::DataType;

const TEXT_SEARCH_CONSTRUCTORS: [&str; 5] = [
    "to_tsvector",
    "to_tsquery",
    "plainto_tsquery",
    "phraseto_tsquery",
    "websearch_to_tsquery",
];

fn malformed_jsonb_text_values() -> Vec<Value> {
    [
        vec![],
        vec![0x30, 1],    // Truncated current-tag signed integer.
        vec![0x51, 0xff], // Current-tag string with invalid UTF-8.
        vec![0x61, 0x30], // Current-tag array containing a truncated integer.
        vec![0x71, 0x00], // Current-tag object with a non-string key.
        vec![0x00, 0x00], // Valid null followed by trailing bytes.
    ]
    .into_iter()
    .map(|bytes| Value::Jsonb(std::sync::Arc::from(bytes)))
    .collect()
}

fn malformed_tsvector_text_values() -> Vec<Value> {
    [
        vec![],
        vec![0, 1],                            // Truncated header.
        vec![0, 1, 0, 0, 0],                   // Declared lexeme is absent.
        vec![0, 1, 0, 0, 0, 1, 0, b'x'],       // Missing position count.
        vec![0, 1, 0, 0, 0, 1, 0, b'x', 1, 0], // Missing position.
    ]
    .into_iter()
    .map(|bytes| Value::TsVector(std::sync::Arc::from(bytes)))
    .collect()
}

fn malformed_tsquery_text_values() -> Vec<Value> {
    [
        vec![],
        vec![0],                      // Truncated lexeme length.
        vec![0, 1, 0, b'x'],          // Missing weight and prefix flags.
        vec![1, 0, 1, 0, b'x', 0, 0], // AND lacks its right operand.
        vec![0, 1, 0, b'x', 0, 0, 0], // Trailing byte after a valid leaf.
        vec![255],                    // Unknown query node tag.
    ]
    .into_iter()
    .map(|bytes| Value::TsQuery(std::sync::Arc::from(bytes)))
    .collect()
}

#[test]
fn malformed_jsonb_text_consumers_return_errors() {
    assert_malformed_text_consumers_return_errors(malformed_jsonb_text_values());
}

fn assert_malformed_text_consumers_return_errors(values: Vec<Value>) {
    let columns = ColumnMap::new(&[]);
    let token = citadel::CancelToken::new();
    for value in values {
        for cancel in [None, Some(&token)] {
            let params = [value.clone()];
            let context = EvalCtx::with_params(&columns, &[], &params).with_cancel(cancel);
            for sql in [
                "CAST($1 AS TEXT)",
                "LENGTH($1)",
                "UPPER($1)",
                "LOWER($1)",
                "SUBSTR($1, 1, 2)",
                "TRIM($1)",
                "LTRIM($1)",
                "RTRIM($1)",
                "LTRIM('value', $1)",
                "REPLACE($1, 'a', 'b')",
                "REPLACE('value', $1, 'b')",
                "REPLACE('value', 'a', $1)",
                "INSTR($1, 'a')",
                "INSTR('value', $1)",
                "CONCAT('prefix', $1, 'suffix')",
                "HEX($1)",
            ] {
                let expression = crate::parser::parse_sql_expr(sql).unwrap();
                let result = eval_expr(&expression, &context);
                assert!(
                    matches!(result, Err(SqlError::InvalidValue(_))),
                    "{sql}, value={value:?}, cancellable={}: {result:?}",
                    cancel.is_some(),
                );
            }
        }
    }
}

#[test]
fn malformed_jsonb_text_nested_array_conversion_returns_error() {
    assert_malformed_text_nested_array_conversion_returns_error(malformed_jsonb_text_values());
}

fn assert_malformed_text_nested_array_conversion_returns_error(values: Vec<Value>) {
    let token = citadel::CancelToken::new();
    for value in values {
        let nested = Value::Array(std::sync::Arc::from(vec![Value::Array(
            std::sync::Arc::from(vec![value]),
        )]));
        for cancel in [None, Some(&token)] {
            assert!(matches!(
                eval_cast_with_cancel(&nested, DataType::Text, cancel),
                Err(SqlError::InvalidValue(_))
            ));
            assert!(matches!(
                eval_binary_op_with_cancel(
                    &nested,
                    BinOp::Concat,
                    &Value::Text("suffix".into()),
                    cancel
                ),
                Err(SqlError::InvalidValue(_))
            ));
        }
    }
}

#[test]
fn malformed_jsonb_text_update_is_atomic() {
    assert_malformed_text_update_is_atomic(
        crate::json::text_to_jsonb(r#"{"n":1}"#).unwrap(),
        malformed_jsonb_text_values(),
    );
}

fn assert_malformed_text_update_is_atomic(valid: Value, malformed_values: Vec<Value>) {
    use crate::Connection;
    use citadel::{Argon2Profile, DatabaseBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("invalid-jsonb.cdl"))
        .passphrase(b"test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute(&format!(
        "CREATE TABLE invalid_jsonb_text (id INTEGER PRIMARY KEY, data {}, copied TEXT)",
        valid.data_type(),
    ))
    .unwrap();
    for cancellable in [false, true] {
        db.set_cancel(cancellable.then(citadel::CancelToken::new));
        for malformed in &malformed_values {
            conn.execute("DELETE FROM invalid_jsonb_text").unwrap();
            conn.execute_params(
                "INSERT INTO invalid_jsonb_text VALUES (1, $1, 'before-1'), (2, $2, 'before-2')",
                &[valid.clone(), malformed.clone()],
            )
            .unwrap();
            let before = conn
                .query("SELECT id, data, copied FROM invalid_jsonb_text ORDER BY id")
                .unwrap()
                .rows;
            for conversion in ["CAST(data AS TEXT)", "CAST(ARRAY[ARRAY[data]] AS TEXT)"] {
                assert!(matches!(
                    conn.query(&format!(
                        "SELECT {conversion} FROM invalid_jsonb_text ORDER BY id"
                    )),
                    Err(SqlError::InvalidValue(_))
                ));
                for explicit_transaction in [false, true] {
                    if explicit_transaction {
                        conn.execute("BEGIN").unwrap();
                        conn.execute(
                        "UPDATE invalid_jsonb_text SET copied = 'earlier-statement' WHERE id = 1",
                    )
                    .unwrap();
                    }
                    let statement_before = conn
                        .query("SELECT id, data, copied FROM invalid_jsonb_text ORDER BY id")
                        .unwrap()
                        .rows;
                    assert!(matches!(
                        conn.execute(&format!(
                            "UPDATE invalid_jsonb_text SET copied = {conversion}"
                        )),
                        Err(SqlError::InvalidValue(_))
                    ));
                    assert_eq!(
                        conn.query("SELECT id, data, copied FROM invalid_jsonb_text ORDER BY id")
                            .unwrap()
                            .rows,
                        statement_before,
                        "failed UPDATE must not persist a partial or empty conversion",
                    );
                    if explicit_transaction {
                        conn.execute("ROLLBACK").unwrap();
                    }
                }
            }
            assert_eq!(
                conn.query("SELECT id, data, copied FROM invalid_jsonb_text ORDER BY id")
                    .unwrap()
                    .rows,
                before
            );
        }
    }
}

#[test]
fn malformed_fts_text_consumers_return_errors() {
    assert_malformed_text_consumers_return_errors(malformed_tsvector_text_values());
    assert_malformed_text_consumers_return_errors(malformed_tsquery_text_values());
}

#[test]
fn malformed_fts_text_nested_array_conversion_returns_error() {
    assert_malformed_text_nested_array_conversion_returns_error(malformed_tsvector_text_values());
    assert_malformed_text_nested_array_conversion_returns_error(malformed_tsquery_text_values());
}

#[test]
fn malformed_fts_text_update_is_atomic() {
    for (valid, malformed) in [
        (
            crate::fts::fn_to_tsvector("cat dog").unwrap(),
            malformed_tsvector_text_values(),
        ),
        (
            crate::fts::fn_to_tsquery("cat & dog").unwrap(),
            malformed_tsquery_text_values(),
        ),
    ] {
        assert_malformed_text_update_is_atomic(valid, malformed);
    }
}

#[test]
fn fts_text_valid_conversions_and_diagnostic_display_are_preserved() {
    use crate::fts::{TsQueryAst, TsVectorBuilder, Weight};

    let mut builder = TsVectorBuilder::new();
    builder.push(b"cat", 1, Weight::A).unwrap();
    builder.push(b"cat", 5, Weight::D).unwrap();
    builder.push_no_position(b"dog").unwrap();
    let vector = Value::TsVector(builder.build());
    let leaf = |text: &[u8]| TsQueryAst::Lexeme {
        lexeme: text.to_vec(),
        weight_mask: 0,
        prefix: false,
    };
    let query = TsQueryAst::Phrase {
        distance: 2,
        left: Box::new(TsQueryAst::Lexeme {
            lexeme: b"cat".to_vec(),
            weight_mask: 9,
            prefix: true,
        }),
        right: Box::new(TsQueryAst::Or(
            Box::new(leaf(b"dog")),
            Box::new(TsQueryAst::Not(Box::new(leaf(b"bird")))),
        )),
    };
    let token = citadel::CancelToken::new();
    for (value, expected) in [
        (Value::TsVector(TsVectorBuilder::new().build()), ""),
        (vector, "'cat':1A,5 'dog'"),
        (
            Value::TsQuery(query.encode().unwrap()),
            "'cat':*AD <2> ('dog' | !'bird')",
        ),
    ] {
        assert_eq!(value.to_string(), expected);
        let nested = Value::Array(std::sync::Arc::from(vec![
            Value::Null,
            Value::Text("escaped\\\"text".into()),
            Value::Array(std::sync::Arc::from(vec![value.clone()])),
        ]));
        for cancel in [None, Some(&token)] {
            assert_eq!(
                eval_cast_with_cancel(&value, DataType::Text, cancel).unwrap(),
                Value::Text(expected.into())
            );
            assert_eq!(
                eval_cast_with_cancel(&nested, DataType::Text, cancel).unwrap(),
                Value::Text(nested.to_string().into())
            );
        }
    }
    for value in malformed_tsvector_text_values() {
        assert_eq!(value.to_string(), "<invalid tsvector>");
    }
    for value in malformed_tsquery_text_values() {
        assert_eq!(value.to_string(), "<invalid tsquery>");
    }
}

#[test]
fn jsonb_text_valid_conversions_are_unchanged() {
    let token = citadel::CancelToken::new();
    for text in [
        "null",
        "true",
        "42",
        "1.5",
        r#""hello""#,
        r#"{"a":[1,"b"]}"#,
    ] {
        let value = crate::json::text_to_jsonb(text).unwrap();
        let nested = Value::Array(std::sync::Arc::from(vec![
            Value::Null,
            Value::Text("escaped\\\"text".into()),
            Value::Boolean(true),
            Value::Real(1.0),
            Value::Array(std::sync::Arc::from(vec![value.clone()])),
        ]));
        for cancel in [None, Some(&token)] {
            assert_eq!(
                eval_cast_with_cancel(&value, DataType::Text, cancel).unwrap(),
                Value::Text(text.into())
            );
            assert_eq!(
                eval_cast_with_cancel(&nested, DataType::Text, cancel).unwrap(),
                Value::Text(nested.to_string().into())
            );
        }
    }
}

#[test]
fn jsonb_text_conversion_forwards_mid_decode_cancellation() {
    let columns = ColumnMap::new(&[]);
    let jsonb = crate::json::text_to_jsonb("[1,2,3,4]").unwrap();
    for value in [
        jsonb.clone(),
        Value::Array(std::sync::Arc::from(vec![jsonb])),
    ] {
        for sql in ["CAST($1 AS TEXT)", "LENGTH($1)", "CONCAT('prefix', $1)"] {
            let token = citadel::CancelToken::new();
            let _guard = crate::json::cancel_json_after(token.clone(), 2);
            let params = [value.clone()];
            let context = EvalCtx::with_params(&columns, &[], &params).with_cancel(Some(&token));
            let expression = crate::parser::parse_sql_expr(sql).unwrap();
            assert!(
                matches!(
                    eval_expr(&expression, &context),
                    Err(SqlError::Storage(citadel_core::Error::Interrupted))
                ),
                "{sql}, value={value:?}"
            );
        }
    }
}

#[test]
fn literal_jsonpath_analysis_only_unwraps_text_preserving_operations() {
    for sql in ["'$.x'", "CAST('$.x' AS TEXT)", "('$.x' COLLATE BINARY)"] {
        let expr = crate::parser::parse_sql_expr(sql).unwrap();
        assert_eq!(literal_jsonpath_text(&expr), Some("$.x"), "{sql}");
    }
    let expr = crate::parser::parse_sql_expr("CAST(CAST('$.x' AS TSQUERY) AS TEXT)").unwrap();
    assert_eq!(literal_jsonpath_text(&expr), None);
}

#[test]
fn text_search_constructor_types_and_default_configuration_match_evaluation() {
    let columns = ColumnMap::new(&[]);
    let context = EvalCtx::new(&columns, &[]);
    for name in TEXT_SEARCH_CONSTRUCTORS {
        let implicit = crate::parser::parse_sql_expr(&format!("{name}('running')")).unwrap();
        let english =
            crate::parser::parse_sql_expr(&format!("{name}('english', 'running')")).unwrap();
        let simple =
            crate::parser::parse_sql_expr(&format!("{name}('simple', 'running')")).unwrap();
        let implicit_value = eval_expr(&implicit, &context).unwrap();
        assert_eq!(
            intrinsic_result_type(&implicit),
            Some(implicit_value.data_type())
        );
        assert_eq!(
            implicit_value,
            eval_expr(&english, &context).unwrap(),
            "{name}"
        );
        assert_ne!(
            implicit_value,
            eval_expr(&simple, &context).unwrap(),
            "{name}"
        );
        let expected = if name == "to_tsvector" {
            DataType::TsVector
        } else {
            DataType::TsQuery
        };
        assert_eq!(implicit_value.data_type(), expected, "{name}");
    }
}

#[test]
fn text_search_constructor_arity_is_checked_before_null_propagation() {
    let columns = ColumnMap::new(&[]);
    let context = EvalCtx::new(&columns, &[]);
    for name in TEXT_SEARCH_CONSTRUCTORS {
        for args in ["", "NULL, 'english', 'rust'", "'simple', NULL, 'rust'"] {
            let expr = crate::parser::parse_sql_expr(&format!("{name}({args})")).unwrap();
            assert!(
                matches!(eval_expr(&expr, &context), Err(SqlError::InvalidValue(message))
                    if message == format!("{name} requires 1 or 2 arguments")),
                "{name}({args})"
            );
        }
        for args in ["NULL", "NULL, 'rust'", "'english', NULL"] {
            let expr = crate::parser::parse_sql_expr(&format!("{name}({args})")).unwrap();
            assert_eq!(
                eval_expr(&expr, &context).unwrap(),
                Value::Null,
                "{name}({args})"
            );
        }
    }
}

#[test]
fn text_search_constructors_reject_invalid_config_and_argument_types() {
    let columns = ColumnMap::new(&[]);
    let context = EvalCtx::new(&columns, &[]);
    for name in TEXT_SEARCH_CONSTRUCTORS {
        for args in ["1", "1, 'rust'", "'english', 1"] {
            let expr = crate::parser::parse_sql_expr(&format!("{name}({args})")).unwrap();
            assert!(
                matches!(
                    eval_expr(&expr, &context),
                    Err(SqlError::TypeMismatch { .. })
                ),
                "{name}({args})"
            );
        }
        let expr =
            crate::parser::parse_sql_expr(&format!("{name}('not_a_config', 'rust')")).unwrap();
        assert!(
            matches!(eval_expr(&expr, &context), Err(SqlError::Unsupported(message))
                if message == "unknown text search configuration: not_a_config"),
            "{name}"
        );
        let expr = crate::parser::parse_sql_expr(&format!("{name}(NULL, 1 / 0)")).unwrap();
        assert!(
            matches!(eval_expr(&expr, &context), Err(SqlError::DivisionByZero)),
            "{name}"
        );
    }
}

#[test]
fn text_search_constructors_forward_cancellation() {
    let cancel = citadel::CancelToken::new();
    cancel.cancel();
    for name in TEXT_SEARCH_CONSTRUCTORS {
        let constructor = text_search::Constructor::from_name(&name.to_ascii_uppercase()).unwrap();
        let result = constructor.evaluate(&[Value::Text("rust database".into())], Some(&cancel));
        assert!(
            matches!(result, Err(SqlError::Storage(citadel::Error::Interrupted))),
            "{name}: {result:?}"
        );
    }
}

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
fn eval_context_timezone_reaches_jsonpath() {
    let columns = test_columns();
    let column_map = ColumnMap::new(&columns);
    let row = test_row();
    let expression = crate::parser::parse_sql_expr(
        "JSONB_PATH_QUERY_FIRST_TZ(\
         CAST('\"2023-08-15T12:34:56+05:30\"' AS JSONB), \
         '$.time().string()')",
    )
    .unwrap();
    let timezone = crate::datetime::resolve_timezone("+10:00").unwrap();
    let context = EvalCtx::new(&column_map, &row).with_session_tz(Some(timezone));

    let value = eval_expr(&expression, &context).unwrap();
    assert_eq!(value, crate::json::text_to_jsonb(r#""17:04:56""#).unwrap());
}

#[test]
fn eval_context_preserves_auto_traits() {
    fn assert_traits<T: Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe>() {}
    assert_traits::<EvalCtx<'static>>();
}

#[test]
fn eval_excluded_resolver_skips_unread_branches_and_ordinary_columns() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let columns = test_columns();
    let column_map = ColumnMap::new(&columns);
    let row = test_row();
    let excluded = vec![Value::Null; columns.len()];
    let calls = AtomicUsize::new(0);
    let resolver = |_: usize| -> Result<Value> {
        calls.fetch_add(1, Ordering::Relaxed);
        Err(SqlError::IntegerOverflow)
    };
    let context = EvalCtx::with_excluded(&column_map, &row, &column_map, &excluded)
        .with_excluded_resolver(&resolver);

    for (sql, expected) in [
        ("CASE WHEN TRUE THEN 42 ELSE excluded.id END", 42),
        ("CASE WHEN FALSE THEN excluded.id ELSE 42 END", 42),
        ("CASE id WHEN 1 THEN 42 ELSE excluded.id END", 42),
        ("COALESCE(42, excluded.id)", 42),
        ("COALESCE(NULL, 42, excluded.id)", 42),
        ("id", 1),
        ("t.id", 1),
    ] {
        let expression = crate::parser::parse_sql_expr(sql).unwrap();
        assert_eq!(
            eval_expr(&expression, &context).unwrap(),
            Value::Integer(expected),
            "{sql}",
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0, "{sql}");
    }
}

#[test]
fn eval_excluded_resolver_receives_resolved_index_and_supplies_value() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let column_map = ColumnMap::new(&[col("id", DataType::Integer, false, 0)]);
    let excluded_map = ColumnMap::new(&[
        col("other", DataType::Integer, false, 0),
        col("id", DataType::Integer, false, 1),
    ]);
    let row = [Value::Integer(1)];
    let excluded = [Value::Integer(999), Value::Integer(888)];
    let calls = AtomicUsize::new(0);
    let resolver = |index: usize| -> Result<Value> {
        assert_eq!(index, 1);
        calls.fetch_add(1, Ordering::Relaxed);
        Ok(Value::Integer(42))
    };
    let context = EvalCtx::with_excluded(&column_map, &row, &excluded_map, &excluded)
        .with_excluded_resolver(&resolver);

    for (index, sql) in [
        "ExClUdEd.ID",
        "CASE WHEN TRUE THEN excluded.id ELSE 0 END",
        "COALESCE(NULL, excluded.id)",
    ]
    .into_iter()
    .enumerate()
    {
        let expression = crate::parser::parse_sql_expr(sql).unwrap();
        assert_eq!(
            eval_expr(&expression, &context).unwrap(),
            Value::Integer(42),
            "{sql}",
        );
        assert_eq!(calls.load(Ordering::Relaxed), index + 1, "{sql}");
    }
}

#[test]
fn eval_excluded_resolver_propagates_errors_after_column_resolution() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let column_map = ColumnMap::new(&[col("id", DataType::Integer, false, 0)]);
    let row = [Value::Integer(1)];
    let excluded = [Value::Integer(2)];
    let calls = AtomicUsize::new(0);
    let resolver = |_: usize| -> Result<Value> {
        calls.fetch_add(1, Ordering::Relaxed);
        Err(SqlError::IntegerOverflow)
    };
    let context = EvalCtx::with_excluded(&column_map, &row, &column_map, &excluded)
        .with_excluded_resolver(&resolver);

    let unknown = crate::parser::parse_sql_expr("excluded.missing").unwrap();
    assert!(matches!(
        eval_expr(&unknown, &context),
        Err(SqlError::ColumnNotFound(name)) if name == "missing"
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    let actual = crate::parser::parse_sql_expr("excluded.id").unwrap();
    assert!(matches!(
        eval_expr(&actual, &context),
        Err(SqlError::IntegerOverflow)
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn eval_excluded_without_resolver_clones_the_backing_row() {
    let column_map = ColumnMap::new(&[col("name", DataType::Text, true, 0)]);
    let row = [Value::Text("old".into())];
    let excluded = [Value::Text(
        "proposed value retained by the backing row".into(),
    )];
    let context = EvalCtx::with_excluded(&column_map, &row, &column_map, &excluded);
    let expression = crate::parser::parse_sql_expr("excluded.name").unwrap();
    let mut resolved = eval_expr(&expression, &context).unwrap();
    assert_eq!(resolved, excluded[0]);
    if let Value::Text(text) = &mut resolved {
        text.push_str(" changed");
    } else {
        panic!("expected a cloned text value");
    }
    assert_eq!(
        excluded[0],
        Value::Text("proposed value retained by the backing row".into()),
    );
    assert_ne!(resolved, excluded[0]);
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

fn array_lit(elems: Vec<Value>) -> Expr {
    Expr::ArrayLiteral(elems.into_iter().map(Expr::Literal).collect())
}

fn quantified(lhs: Value, op: BinOp, q: crate::parser::Quantifier, rhs: Expr) -> Expr {
    use crate::parser::QuantifiedRhs;
    Expr::Quantified {
        left: Box::new(Expr::Literal(lhs)),
        op,
        quantifier: q,
        right: QuantifiedRhs::Array(Box::new(rhs)),
    }
}

#[test]
fn array_literal_evaluates_to_value_array() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = array_lit(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    let val = eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap();
    match val {
        Value::Array(a) => {
            assert_eq!(a.len(), 3);
            assert_eq!(a[0], Value::Integer(1));
            assert_eq!(a[2], Value::Integer(3));
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn any_eq_matches() {
    use crate::parser::Quantifier::Any;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(
        Value::Integer(2),
        BinOp::Eq,
        Any,
        array_lit(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]),
    );
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(true)
    );
}

#[test]
fn any_eq_no_match_returns_false() {
    use crate::parser::Quantifier::Any;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(
        Value::Integer(99),
        BinOp::Eq,
        Any,
        array_lit(vec![Value::Integer(1), Value::Integer(2)]),
    );
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(false)
    );
}

#[test]
fn any_eq_no_match_with_null_returns_null() {
    use crate::parser::Quantifier::Any;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(
        Value::Integer(99),
        BinOp::Eq,
        Any,
        array_lit(vec![Value::Integer(1), Value::Null]),
    );
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Null
    );
}

#[test]
fn all_eq_all_match_returns_true() {
    use crate::parser::Quantifier::All;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(
        Value::Integer(5),
        BinOp::Eq,
        All,
        array_lit(vec![Value::Integer(5), Value::Integer(5)]),
    );
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(true)
    );
}

#[test]
fn all_eq_one_mismatch_returns_false_short_circuit() {
    use crate::parser::Quantifier::All;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(
        Value::Integer(5),
        BinOp::Eq,
        All,
        array_lit(vec![Value::Integer(5), Value::Integer(6), Value::Null]),
    );
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(false)
    );
}

#[test]
fn all_eq_with_null_no_mismatch_returns_null() {
    use crate::parser::Quantifier::All;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(
        Value::Integer(5),
        BinOp::Eq,
        All,
        array_lit(vec![Value::Integer(5), Value::Null]),
    );
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Null
    );
}

#[test]
fn any_lhs_null_with_empty_returns_false() {
    use crate::parser::Quantifier::Any;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(Value::Null, BinOp::Eq, Any, array_lit(vec![]));
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(false)
    );
}

#[test]
fn all_lhs_null_with_empty_returns_true() {
    use crate::parser::Quantifier::All;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(Value::Null, BinOp::Eq, All, array_lit(vec![]));
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(true)
    );
}

#[test]
fn any_lhs_null_with_nonempty_returns_null() {
    use crate::parser::Quantifier::Any;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(
        Value::Null,
        BinOp::Eq,
        Any,
        array_lit(vec![Value::Integer(1)]),
    );
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Null
    );
}

#[test]
fn any_lt_finds_greater_element() {
    use crate::parser::Quantifier::Any;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(
        Value::Integer(2),
        BinOp::Lt,
        Any,
        array_lit(vec![
            Value::Integer(1),
            Value::Integer(3),
            Value::Integer(5),
        ]),
    );
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(true)
    );
}

#[test]
fn all_gt_threshold() {
    use crate::parser::Quantifier::All;
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let expr = quantified(
        Value::Integer(10),
        BinOp::Gt,
        All,
        array_lit(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]),
    );
    assert_eq!(
        eval_expr(&expr, &EvalCtx::new(&cm, &row)).unwrap(),
        Value::Boolean(true)
    );
}

fn lit(v: Value) -> Expr {
    Expr::Literal(v)
}

fn col_ref(name: &str) -> Expr {
    Expr::Column(name.into())
}

fn binop(left: Expr, op: BinOp, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

#[test]
fn compiled_expr_matches_interpreter() {
    let cols = test_columns();
    let cm = ColumnMap::new(&cols);
    let row = test_row();
    let params = vec![Value::Integer(95), Value::Text("Alice".into())];

    let cases: Vec<Expr> = vec![
        lit(Value::Integer(42)),
        col_ref("id"),
        col_ref("name"),
        Expr::Parameter(1),
        binop(col_ref("id"), BinOp::Eq, lit(Value::Integer(1))),
        binop(col_ref("score"), BinOp::Gt, lit(Value::Real(90.0))),
        binop(col_ref("score"), BinOp::Lt, lit(Value::Real(90.0))),
        binop(col_ref("name"), BinOp::Eq, lit(Value::Text("Bob".into()))),
        binop(col_ref("id"), BinOp::Add, lit(Value::Integer(10))),
        binop(
            binop(col_ref("id"), BinOp::Eq, lit(Value::Integer(1))),
            BinOp::And,
            binop(col_ref("score"), BinOp::Gt, lit(Value::Real(90.0))),
        ),
        binop(
            binop(col_ref("id"), BinOp::Eq, lit(Value::Integer(9))),
            BinOp::Or,
            binop(col_ref("active"), BinOp::Eq, lit(Value::Boolean(true))),
        ),
        Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(col_ref("active")),
        },
        Expr::UnaryOp {
            op: UnaryOp::Neg,
            expr: Box::new(col_ref("id")),
        },
        Expr::IsNull(Box::new(col_ref("name"))),
        Expr::IsNotNull(Box::new(col_ref("score"))),
        binop(lit(Value::Null), BinOp::Eq, lit(Value::Integer(1))),
        binop(col_ref("score"), BinOp::Gt, Expr::Parameter(1)),
        Expr::Between {
            expr: Box::new(col_ref("score")),
            low: Box::new(lit(Value::Real(0.0))),
            high: Box::new(lit(Value::Real(100.0))),
            negated: false,
        },
        col_ref("nonexistent"),
    ];

    for (i, expr) in cases.iter().enumerate() {
        let ctx = EvalCtx::with_params(&cm, &row, &params);
        let interp = eval_expr(expr, &ctx);
        let compiled = CompiledExpr::compile(expr, &cm).eval(&ctx);
        match (interp, compiled) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "value mismatch at case {i}"),
            (Err(_), Err(_)) => {}
            (a, b) => panic!("ok/err divergence at case {i}: interp={a:?} compiled={b:?}"),
        }
    }
}

#[test]
fn empty_parameter_scope_masks_outer_and_restores_on_error() {
    with_scoped_params(&[Value::Integer(99)], || {
        let result: Result<()> = with_scoped_params(&[], || {
            assert!(matches!(
                resolve_scoped_param(1),
                Err(SqlError::ParameterCountMismatch {
                    expected: 1,
                    got: 0
                })
            ));
            with_scoped_params(&[], || {
                with_scoped_params(&[Value::Integer(7)], || {
                    assert_eq!(resolve_scoped_param(1).unwrap(), Value::Integer(7));
                });
                assert!(resolve_scoped_param(1).is_err());
            });
            resolve_scoped_param(1).map(|_| ())
        });
        assert!(result.is_err());
        assert_eq!(resolve_scoped_param(1).unwrap(), Value::Integer(99));
    });
}

#[test]
fn empty_parameter_scope_restores_outer_after_unwind() {
    with_scoped_params(&[Value::Integer(99)], || {
        let outcome = std::panic::catch_unwind(|| {
            with_scoped_params(&[], || {
                assert!(resolve_scoped_param(1).is_err());
                panic!("parameter scope unwind probe");
            });
        });
        assert!(outcome.is_err());
        assert_eq!(resolve_scoped_param(1).unwrap(), Value::Integer(99));
    });
}
