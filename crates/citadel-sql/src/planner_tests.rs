use super::*;
use crate::types::{Collation, ColumnDef, DataType, IndexKind};

#[test]
fn literal_resolution_does_not_evaluate_volatile_or_contextual_expressions() {
    for expression in [
        "RANDOM()",
        "ABS(RANDOM())",
        "CAST(NOW() AS TEXT)",
        "TO_TSQUERY(CASE WHEN RANDOM() > 0 THEN 'cat' ELSE 'dog' END)",
        "JSONB_PATH_QUERY_FIRST_TZ('{}'::JSONB, '$.a')",
        "DATE(CAST('now' AS TEXT))",
    ] {
        let expression = crate::parser::parse_sql_expr(expression).unwrap();
        assert!(resolve_literal(&expression).is_none(), "{expression:?}");
    }
    let query = crate::parser::parse_sql_expr("TO_TSQUERY('cat')").unwrap();
    assert!(matches!(resolve_literal(&query), Some(Value::TsQuery(_))));
    for expression in [
        "CAST(1 + 2 AS INTEGER)",
        "CAST(CASE WHEN 1 < 2 THEN 3 ELSE 4 END AS INTEGER)",
        "CAST(COALESCE(NULL, 3) AS INTEGER)",
    ] {
        let expression = crate::parser::parse_sql_expr(expression).unwrap();
        assert_eq!(resolve_literal(&expression), Some(Value::Integer(3)));
    }
    let quantified_path = Expr::Quantified {
        left: Box::new(Expr::Literal(Value::Json("{}".into()))),
        op: BinOp::JsonPathMatch,
        quantifier: crate::parser::Quantifier::Any,
        right: crate::parser::QuantifiedRhs::Array(Box::new(Expr::Literal(Value::Array(
            vec![Value::Text("$.time_tz()".into())].into(),
        )))),
    };
    assert!(!crate::eval::is_statement_constant(&quantified_path));
}

#[test]
fn typed_key_bounds_preserve_numeric_comparison_sets() {
    use std::cmp::Ordering;

    let matches = |actual: &Value, op: BinOp, bound: &Value| {
        let order = actual.cmp(bound);
        match op {
            BinOp::Eq => order == Ordering::Equal,
            BinOp::Lt => order == Ordering::Less,
            BinOp::LtEq => order != Ordering::Greater,
            BinOp::Gt => order == Ordering::Greater,
            BinOp::GtEq => order != Ordering::Less,
            _ => unreachable!(),
        }
    };
    let mut integers = vec![i64::MIN, i64::MIN + 1, i64::MAX - 1, i64::MAX, 0];
    for exponent in [0, 1, 31, 52, 53, 54, 62] {
        for sign in [-1, 1] {
            let center = sign * (1i64 << exponent);
            integers.extend([center - 1, center, center + 1]);
        }
    }
    let mut reals = vec![-0.0, 0.0];
    for &integer in &integers {
        reals.extend([integer as f64 - 0.5, integer as f64, integer as f64 + 0.5]);
    }
    let integer_values: Vec<_> = integers.into_iter().map(Value::Integer).collect();
    let real_values: Vec<_> = reals.into_iter().map(Value::Real).collect();
    for (data_type, values) in [
        (DataType::Integer, &integer_values),
        (DataType::Real, &real_values),
    ] {
        for bound in integer_values.iter().chain(&real_values) {
            for op in [BinOp::Eq, BinOp::Lt, BinOp::LtEq, BinOp::Gt, BinOp::GtEq] {
                if let Some((key_op, key_bound)) = key_predicate(data_type, op, bound) {
                    assert_eq!(key_bound.data_type(), data_type);
                    for actual in values {
                        assert_eq!(
                            matches(actual, op, bound),
                            matches(actual, key_op, &key_bound),
                            "{actual:?} {op:?} {bound:?} -> {key_op:?} {key_bound:?}"
                        );
                    }
                }
            }
        }
    }
    assert_eq!(
        key_predicate(DataType::Integer, BinOp::Eq, &Value::Real(2.0)),
        Some((BinOp::Eq, Value::Integer(2)))
    );
    assert!(key_predicate(DataType::Integer, BinOp::Eq, &Value::Real(2f64.powi(53))).is_none());
    assert!(key_predicate(DataType::Real, BinOp::Eq, &Value::Real(-0.0)).is_none());
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
        collation: Collation::Binary,
    }
}

fn test_schema() -> TableSchema {
    TableSchema::new(
        "users".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("name", DataType::Text, true, 1),
            col("age", DataType::Integer, true, 2),
            col("email", DataType::Text, true, 3),
        ],
        vec![0],
        vec![
            IndexDef::from_column_lists(
                "idx_name".into(),
                vec![1],
                vec![],
                false,
                None,
                None,
                IndexKind::default(),
            ),
            IndexDef::from_column_lists(
                "idx_email".into(),
                vec![3],
                vec![],
                true,
                None,
                None,
                IndexKind::default(),
            ),
            IndexDef::from_column_lists(
                "idx_name_age".into(),
                vec![1, 2],
                vec![],
                false,
                None,
                None,
                IndexKind::default(),
            ),
        ],
        vec![],
        vec![],
    )
}

fn expression_schema(expression: &str) -> TableSchema {
    let mut schema = test_schema();
    schema.indices.truncate(1);
    schema.indices[0].keys = vec![IndexKey::Expr {
        expr: crate::parser::parse_sql_expr(expression).unwrap(),
        original_sql: expression.into(),
    }];
    schema
}

#[test]
fn numeric_cast_expression_probes_use_proven_key_types() {
    for (expression, value, encoded) in [
        ("CAST(name AS INTEGER)", Value::Real(2.0), Value::Integer(2)),
        ("CAST(name AS REAL)", Value::Integer(2), Value::Real(2.0)),
    ] {
        let schema = expression_schema(expression);
        for predicate in [format!("{expression} = $1"), format!("$1 = {expression}")] {
            let where_clause = Some(crate::parser::parse_sql_expr(&predicate).unwrap());
            let plan = crate::eval::with_scoped_params(std::slice::from_ref(&value), || {
                plan_select(&schema, &where_clause)
            });
            let ScanPlan::IndexScan { prefix, .. } = plan else {
                panic!("expected expression index: {predicate}")
            };
            assert_eq!(prefix, encode_composite_key(std::slice::from_ref(&encoded)));
        }
    }
}

#[test]
fn expression_probes_decline_unmatched_unbound_and_ambiguous_numeric_keys() {
    for (expression, predicate, params) in [
        (
            "CAST(name AS INTEGER)",
            "CAST(email AS INTEGER) = $1",
            vec![],
        ),
        (
            "CAST(name AS INTEGER)",
            "CAST(name AS INTEGER) = $1",
            vec![],
        ),
        (
            "CAST(name AS INTEGER)",
            "CAST(name AS INTEGER) = $1",
            vec![Value::Real(9_007_199_254_740_992.0)],
        ),
        (
            "CAST(name AS REAL)",
            "CAST(name AS REAL) = $1",
            vec![Value::Integer(0)],
        ),
        (
            "CAST(name AS REAL)",
            "CAST(name AS REAL) = $1",
            vec![Value::Real(f64::NAN)],
        ),
        (
            "CAST(name AS INTEGER)",
            "CAST(name AS INTEGER) = $1",
            vec![Value::Null],
        ),
        (
            "CAST($1 AS INTEGER)",
            "CAST($1 AS INTEGER) = $2",
            vec![Value::Integer(9), Value::Integer(9)],
        ),
        (
            "CAST(CASE WHEN age > 0 THEN $1 ELSE 0 END AS INTEGER)",
            "CAST(CASE WHEN age > 0 THEN $1 ELSE 0 END AS INTEGER) = $2",
            vec![Value::Integer(9), Value::Integer(9)],
        ),
        ("age + 0", "age + 0 = $1", vec![Value::Real(2.0)]),
        ("age + 0", "age + 0 = 2", vec![]),
    ] {
        let schema = expression_schema(expression);
        let where_clause = Some(crate::parser::parse_sql_expr(predicate).unwrap());
        let plan = crate::eval::with_scoped_params(&params, || plan_select(&schema, &where_clause));
        assert!(matches!(plan, ScanPlan::SeqScan), "{predicate}: {plan:?}");
    }
}

#[test]
fn no_where_is_seq_scan() {
    let schema = test_schema();
    let plan = plan_select(&schema, &None);
    assert!(matches!(plan, ScanPlan::SeqScan));
}

#[test]
fn pk_equality_is_pk_lookup() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Integer(42))),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::PkLookup {
            pk_values,
            full_cover,
        } => {
            assert_eq!(pk_values, vec![Value::Integer(42)]);
            assert!(full_cover);
        }
        other => panic!("expected PkLookup, got {other:?}"),
    }
}

#[test]
fn unique_index_equality() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("email".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Text("alice@test.com".into()))),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            is_unique,
            num_prefix_cols,
            ..
        } => {
            assert_eq!(index_name, "idx_email");
            assert!(is_unique);
            assert_eq!(num_prefix_cols, 1);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn non_unique_index_equality() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("name".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            num_prefix_cols,
            ..
        } => {
            assert!(index_name == "idx_name" || index_name == "idx_name_age");
            assert_eq!(num_prefix_cols, 1);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn composite_index_full_prefix() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
        }),
        op: BinOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Integer(30))),
        }),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            num_prefix_cols,
            ..
        } => {
            assert_eq!(index_name, "idx_name_age");
            assert_eq!(num_prefix_cols, 2);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn range_scan_on_indexed_column() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("name".into())),
        op: BinOp::Gt,
        right: Box::new(Expr::Literal(Value::Text("M".into()))),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            range_conds,
            num_prefix_cols,
            ..
        } => {
            assert_eq!(num_prefix_cols, 0);
            assert_eq!(range_conds.len(), 1);
            assert_eq!(range_conds[0].0, BinOp::Gt);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn composite_equality_plus_range() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
        }),
        op: BinOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".into())),
            op: BinOp::Gt,
            right: Box::new(Expr::Literal(Value::Integer(25))),
        }),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            num_prefix_cols,
            range_conds,
            ..
        } => {
            assert_eq!(index_name, "idx_name_age");
            assert_eq!(num_prefix_cols, 1);
            assert_eq!(range_conds.len(), 1);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn or_condition_falls_back_to_seq_scan() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
        }),
        op: BinOp::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Bob".into()))),
        }),
    });
    let plan = plan_select(&schema, &where_clause);
    assert!(matches!(plan, ScanPlan::SeqScan));
}

#[test]
fn non_indexed_column_is_seq_scan() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("age".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Integer(30))),
    });
    let plan = plan_select(&schema, &where_clause);
    assert!(matches!(plan, ScanPlan::SeqScan));
}

#[test]
fn reversed_literal_column() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Literal(Value::Integer(42))),
        op: BinOp::Eq,
        right: Box::new(Expr::Column("id".into())),
    });
    let plan = plan_select(&schema, &where_clause);
    assert!(matches!(plan, ScanPlan::PkLookup { .. }));
}

#[test]
fn reversed_comparison_flips_op() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Literal(Value::Text("m".into()))),
        op: BinOp::Lt,
        right: Box::new(Expr::Column("name".into())),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan { range_conds, .. } => {
            assert_eq!(range_conds, vec![(BinOp::Gt, Value::Text("m".into()))]);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn prefers_unique_index() {
    let schema = TableSchema::new(
        "t".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("code", DataType::Text, false, 1),
        ],
        vec![0],
        vec![
            IndexDef::from_column_lists(
                "idx_code".into(),
                vec![1],
                vec![],
                false,
                None,
                None,
                IndexKind::default(),
            ),
            IndexDef::from_column_lists(
                "idx_code_uniq".into(),
                vec![1],
                vec![],
                true,
                None,
                None,
                IndexKind::default(),
            ),
        ],
        vec![],
        vec![],
    );
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("code".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Text("X".into()))),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            is_unique,
            ..
        } => {
            assert_eq!(index_name, "idx_code_uniq");
            assert!(is_unique);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn prefers_more_equality_columns() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
        }),
        op: BinOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Integer(30))),
        }),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            num_prefix_cols,
            ..
        } => {
            assert_eq!(index_name, "idx_name_age");
            assert_eq!(num_prefix_cols, 2);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

fn schema_with_partial_index(name: &str, predicate_sql: &str) -> TableSchema {
    let predicate_expr = crate::parser::parse_sql_expr(predicate_sql).unwrap();
    TableSchema::new(
        "users".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("email", DataType::Text, true, 1),
            col("deleted_at", DataType::Integer, true, 2),
        ],
        vec![0],
        vec![IndexDef::from_column_lists(
            name.into(),
            vec![1],
            vec![],
            true,
            Some(predicate_sql.into()),
            Some(predicate_expr),
            IndexKind::default(),
        )],
        vec![],
        vec![],
    )
}

#[test]
fn partial_index_picked_when_predicate_matches_exactly() {
    let schema = schema_with_partial_index("u_active", "deleted_at IS NULL");
    let where_clause =
        Some(crate::parser::parse_sql_expr("email = 'a@x' AND deleted_at IS NULL").unwrap());
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan { index_name, .. } => assert_eq!(index_name, "u_active"),
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn partial_index_skipped_when_predicate_missing() {
    let schema = schema_with_partial_index("u_active", "deleted_at IS NULL");
    let where_clause = Some(crate::parser::parse_sql_expr("email = 'a@x'").unwrap());
    let plan = plan_select(&schema, &where_clause);
    assert!(
        !matches!(plan, ScanPlan::IndexScan { .. }),
        "expected non-index plan, got IndexScan"
    );
}

#[test]
fn partial_index_picked_via_is_not_null_implication() {
    let schema = schema_with_partial_index("u_present", "email IS NOT NULL");
    let where_clause = Some(crate::parser::parse_sql_expr("email = 'a@x'").unwrap());
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan { index_name, .. } => assert_eq!(index_name, "u_present"),
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn partial_index_skipped_when_unrelated_predicate() {
    let schema = schema_with_partial_index("u_active", "deleted_at IS NULL");
    let where_clause =
        Some(crate::parser::parse_sql_expr("email = 'a@x' AND deleted_at = 100").unwrap());
    let plan = plan_select(&schema, &where_clause);
    assert!(
        !matches!(plan, ScanPlan::IndexScan { .. }),
        "expected non-index plan, got IndexScan"
    );
}
