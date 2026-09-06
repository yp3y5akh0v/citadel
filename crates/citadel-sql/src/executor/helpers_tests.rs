use super::*;
use crate::eval::ColumnMap;
use crate::parser::{BinOp, Expr, GeneratedKind, SelectColumn};
use crate::types::{Collation, ColumnDef, DataType, IndexDef, IndexKey, TableSchema, Value};

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
fn full_row_decode_rejects_truncated_headers_without_panicking() {
    let table = schema(
        "t",
        cols(&[("id", DataType::Integer), ("v", DataType::Integer)]),
        vec![0],
    );
    let key = encode_composite_key(&[i(1)]);
    for data in [&[][..], &[1][..], &[1, 0][..]] {
        let outcome =
            std::panic::catch_unwind(|| decode_full_row_with_cancel(&table, &key, data, None));
        assert!(matches!(outcome, Ok(Err(SqlError::InvalidValue(_)))));
    }
}

#[test]
fn projected_decode_fallback_preserves_keys_values_and_nulls() {
    use crate::encoding::encode_row;

    let encode_v1 = |values: &[Value]| {
        let mut row = (values.len() as u16).to_le_bytes().to_vec();
        if !values.is_empty() {
            row.push(0);
        }
        for (idx, value) in values.iter().enumerate() {
            let bytes = match value {
                Value::Integer(n) => n.to_le_bytes(),
                Value::Real(n) => n.to_le_bytes(),
                Value::Null => {
                    row[2] |= 1 << idx;
                    continue;
                }
                _ => panic!("numeric fixture required"),
            };
            row.push(value.data_type().type_tag());
            row.extend_from_slice(&8u32.to_le_bytes());
            row.extend_from_slice(&bytes);
        }
        row
    };

    let table = schema(
        "t",
        cols(&[
            ("id", DataType::Real),
            ("a", DataType::Integer),
            ("b", DataType::Real),
        ]),
        vec![0],
    );
    let key = encode_composite_key(&[Value::Real(1.5)]);
    for projection in [vec![0, 1, 2], vec![2, 0, 1], vec![0], vec![2]] {
        let decoder = ProjectedDecoder::try_new(&table, &projection).unwrap();
        for values in [
            vec![i(7), Value::Real(2.5)],
            vec![Value::Null, Value::Real(2.5)],
            vec![i(7), Value::Null],
            vec![i(7)],
            vec![],
            vec![i(7), i(2)],
        ] {
            for encoded in [encode_row(&values), encode_v1(&values)] {
                let full = decode_full_row_with_cancel(&table, &key, &encoded, None).unwrap();
                let expected: Vec<_> = projection.iter().map(|&col| full[col].clone()).collect();
                assert_eq!(decoder.decode(&key, &encoded).unwrap(), expected);
            }
        }
    }
}

#[test]
fn posting_lists_sort_by_length_stably() {
    let lists = vec![vec![1, 2], vec![3], vec![4, 5], vec![]];
    assert_eq!(
        sort_lists_by_len(lists, None).unwrap(),
        vec![vec![], vec![3], vec![1, 2], vec![4, 5]]
    );
}

#[test]
fn posting_list_sort_honours_a_pre_cancelled_token() {
    let token = citadel::CancelToken::new();
    token.cancel();
    let error = sort_lists_by_len(vec![vec![1], vec![]], Some(&token)).unwrap_err();
    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn scalar_to_tsvector_observes_cancellation_inside_one_value() {
    use citadel::{Argon2Profile, CancelToken, DatabaseBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("scalar-fts-value-cancel.citadel"))
        .passphrase(b"scalar-fts-value-cancel-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    let token = CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let text = "searchable ".repeat(CANCEL_CHECK_INTERVAL * 4);
    let _cancel = crate::fts::cancel_tokenize_after(token, CANCEL_CHECK_INTERVAL + 1);

    let error = conn
        .query_params("SELECT to_tsvector($1)", &[Value::Text(text.into())])
        .expect_err("scalar tokenization completed after the in-value hook tripped");

    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn implicit_text_match_observes_cancellation_inside_vectorization() {
    use citadel::{Argon2Profile, CancelToken, DatabaseBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("implicit-fts-match-cancel.citadel"))
        .passphrase(b"implicit-fts-match-cancel-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    let token = CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let text = "searchable ".repeat(CANCEL_CHECK_INTERVAL * 4);
    // The short right-hand query is evaluated first; trip only once the
    // implicit TEXT -> TSVECTOR conversion has begun.
    let _cancel = crate::fts::cancel_tokenize_after(token, CANCEL_CHECK_INTERVAL + 64);

    let error = conn
        .query_params(
            "SELECT $1 @@ plainto_tsquery('searchable')",
            &[Value::Text(text.into())],
        )
        .expect_err("implicit vectorization ignored in-value cancellation");

    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn cancelled_fts_value_update_cannot_commit_a_partial_index_change() {
    use citadel::{Argon2Profile, CancelToken, DatabaseBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("fts-value-cancel.citadel"))
        .passphrase(b"fts-value-cancel-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts (body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'original marker')")
        .unwrap();
    conn.execute("BEGIN").unwrap();

    let token = CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let replacement = "replacement ".repeat(CANCEL_CHECK_INTERVAL * 4);
    let error = {
        // The old index value is extracted first. The hook trips only after
        // that deletion and the base-row update, inside the replacement text.
        let _cancel = crate::fts::cancel_tokenize_after(token, CANCEL_CHECK_INTERVAL + 64);
        conn.execute_params(
            "UPDATE docs SET body = $1 WHERE id = 1",
            &[Value::Text(replacement.into())],
        )
        .expect_err("the large FTS value ignored its in-value cancellation")
    };
    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));

    // A cleared handle token cannot make the interrupted transaction
    // committable again.
    db.set_cancel(None);
    let commit_error = conn
        .execute("COMMIT")
        .expect_err("the partially updated transaction remained committable");
    assert!(matches!(
        commit_error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
    drop(conn);

    let conn = crate::Connection::open(&db).unwrap();
    let row = conn.query("SELECT body FROM docs WHERE id = 1").unwrap();
    assert_eq!(row.rows, vec![vec![Value::Text("original marker".into())]]);
    let old_hit = conn
        .query("SELECT id FROM docs WHERE body @@ to_tsquery('original')")
        .unwrap();
    assert_eq!(old_hit.rows, vec![vec![Value::Integer(1)]]);
    let replacement_hit = conn
        .query("SELECT id FROM docs WHERE body @@ to_tsquery('replacement')")
        .unwrap();
    assert!(replacement_hit.rows.is_empty());
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
fn nonnegative_row_counts_saturate_at_the_target_width() {
    for value in [
        i64::MIN,
        -1,
        0,
        1,
        i64::from(u32::MAX),
        i64::from(u32::MAX) + 1,
        i64::from(u32::MAX) + 2,
        i64::MAX,
    ] {
        let expected = (value.max(0) as u128).min(usize::MAX as u128) as usize;
        assert_eq!(nonnegative_row_count(value), expected, "{value}");
        assert_eq!(
            eval_row_count(&Expr::Literal(i(value))).unwrap(),
            expected,
            "{value}"
        );
    }
}

#[test]
fn eval_row_count_preserves_integer_expression_semantics() {
    let expression = Expr::BinaryOp {
        left: Box::new(Expr::Literal(i(i64::from(u32::MAX)))),
        op: BinOp::Add,
        right: Box::new(Expr::Literal(i(2))),
    };
    assert_eq!(
        eval_row_count(&expression).unwrap(),
        nonnegative_row_count(i64::from(u32::MAX) + 2)
    );
    for value in [Value::Null, Value::Real(1.0), Value::Text("1".into())] {
        let expression = Expr::Literal(value);
        assert_eq!(
            eval_row_count(&expression).unwrap_err().to_string(),
            eval_const_int(&expression).unwrap_err().to_string()
        );
    }
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
    materialize_virtual_with_cancel(&ts, &mut row, None).unwrap();
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
    materialize_virtual_with_cancel(&ts, &mut row, None).unwrap();
    assert_eq!(row, vec![i(1), i(2)]);
}

#[test]
fn decoding_an_old_row_passes_cancellation_to_its_default_expression() {
    use crate::encoding::{encode_composite_key, encode_row};

    let mut search = col("search", DataType::TsVector);
    search.position = 1;
    search.default_expr = Some(Expr::Function {
        name: "TO_TSVECTOR".into(),
        args: vec![Expr::Literal(Value::Text(
            "several words to tokenize".into(),
        ))],
        distinct: false,
    });
    let ts = schema(
        "docs",
        {
            let mut columns = cols(&[("id", DataType::Integer)]);
            columns.push(search);
            columns
        },
        vec![0],
    );
    let key = encode_composite_key(&[i(1)]);
    let old_value = encode_row(&[]);
    let token = citadel::CancelToken::new();
    let _cancel = crate::fts::cancel_tokenize_after(token.clone(), 1);

    let error = decode_full_row_with_cancel(&ts, &key, &old_value, Some(&token))
        .expect_err("the old-row default discarded its cancellation token");

    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn expression_index_key_evaluation_propagates_cancellation() {
    use citadel::{Argon2Profile, DatabaseBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("expression-index-cancel.citadel"))
        .passphrase(b"expression-index-cancel-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_terms ON docs (TO_TSVECTOR(body))")
        .unwrap();

    let token = citadel::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let _cancel = crate::fts::cancel_tokenize_after(token, 1);
    let error = conn
        .execute("INSERT INTO docs VALUES (1, 'several words to tokenize')")
        .expect_err("expression-index key evaluation discarded its cancellation token");

    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
    db.set_cancel(None);
    let result = conn.query("SELECT COUNT(*) FROM docs").unwrap();
    assert_eq!(result.rows, vec![vec![Value::Integer(0)]]);
}

#[test]
fn partial_index_predicate_evaluation_propagates_cancellation() {
    use citadel::{Argon2Profile, DatabaseBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("partial-index-cancel.citadel"))
        .passphrase(b"partial-index-cancel-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute(
        "CREATE INDEX docs_nonempty ON docs (id) \
         WHERE TO_TSVECTOR(body) IS NOT NULL",
    )
    .unwrap();

    let token = citadel::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let _cancel = crate::fts::cancel_tokenize_after(token, 1);
    let error = conn
        .execute("INSERT INTO docs VALUES (1, 'several words to tokenize')")
        .expect_err("partial-index predicate evaluation discarded its cancellation token");

    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
    db.set_cancel(None);
    let result = conn.query("SELECT COUNT(*) FROM docs").unwrap();
    assert_eq!(result.rows, vec![vec![Value::Integer(0)]]);
}

#[test]
fn expression_index_change_detection_uses_only_its_dependencies() {
    let table = schema(
        "users",
        cols(&[
            ("id", DataType::Integer),
            ("email", DataType::Text),
            ("note", DataType::Text),
        ]),
        vec![0],
    );
    let index = IndexDef {
        name: "users_lower_email".into(),
        keys: vec![IndexKey::Expr {
            expr: crate::parser::parse_sql_expr("LOWER(email)").unwrap(),
            original_sql: "LOWER(email)".into(),
        }],
        unique: false,
        predicate_sql: None,
        predicate_expr: None,
        kind: crate::types::IndexKind::BTree,
        ann_filter_cols: Vec::new(),
    };
    let old = vec![i(1), Value::Text("old@example.test".into()), Value::Null];
    let email_changed = vec![i(1), Value::Text("new@example.test".into()), Value::Null];
    let unrelated_changed = vec![
        i(1),
        Value::Text("old@example.test".into()),
        Value::Text("changed".into()),
    ];

    assert!(index_columns_changed(&index, &old, &email_changed, &table));
    assert!(!index_columns_changed(
        &index,
        &old,
        &unrelated_changed,
        &table
    ));
}

#[test]
fn build_output_columns_expands_all_columns() {
    let cs = cols(&[("a", DataType::Integer), ("b", DataType::Text)]);
    let out = build_output_columns(&[SelectColumn::AllColumns], &cs);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].name, "a");
    assert_eq!(out[1].name, "b");
    assert_eq!(out[0].position, 0);
    assert_eq!(out[1].position, 1);
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
