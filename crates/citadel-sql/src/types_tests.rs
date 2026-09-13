use super::*;

#[test]
fn rebuilding_schema_caches_preserves_flags_and_dropped_slots() {
    let mut schema = TableSchema::new(
        "t".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("a", DataType::Text, true, 1),
        ],
        vec![0],
        vec![],
        vec![],
        vec![],
    )
    .without_column(1);
    schema.flags = TABLE_FLAG_STRICT | 0x80;
    schema.columns.push(col("b", DataType::Real, true, 1));
    let rebuilt = schema.rebuild();
    assert_eq!(rebuilt.flags, TABLE_FLAG_STRICT | 0x80);
    assert!(rebuilt.is_strict());
    assert_eq!(rebuilt.dropped_non_pk_slots(), &[0]);
    assert_eq!(rebuilt.decode_col_mapping(), &[usize::MAX, 1]);
    assert_eq!(rebuilt.encoding_positions(), &[1]);
    let restored = TableSchema::deserialize(&rebuilt.serialize()).unwrap();
    assert_eq!(restored.flags, rebuilt.flags);
    assert_eq!(restored.decode_col_mapping(), rebuilt.decode_col_mapping());
}

#[test]
fn removing_schema_columns_preserves_flags_and_physical_mapping() {
    let mut schema = TableSchema::new(
        "t".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("a", DataType::Text, true, 1),
            col("b", DataType::Real, true, 2),
        ],
        vec![0],
        vec![],
        vec![],
        vec![],
    );
    schema.flags = TABLE_FLAG_STRICT | 0x80;
    let reduced = schema.without_column(1);
    assert_eq!(reduced.flags, schema.flags);
    assert!(reduced.is_strict());
    assert_eq!(reduced.dropped_non_pk_slots(), &[0]);
    assert_eq!(reduced.decode_col_mapping(), &[usize::MAX, 1]);
    assert_eq!(reduced.encoding_positions(), &[1]);
    assert_eq!(
        TableSchema::deserialize(&reduced.serialize())
            .unwrap()
            .flags,
        schema.flags
    );
}

#[test]
fn value_ordering() {
    assert!(Value::Null < Value::Boolean(false));
    assert!(Value::Boolean(false) < Value::Boolean(true));
    assert!(Value::Boolean(true) < Value::Integer(0));
    assert!(Value::Integer(-1) < Value::Integer(0));
    assert!(Value::Integer(0) < Value::Real(0.5));
    assert!(Value::Real(1.0) < Value::Text("".into()));
    assert!(Value::Text("a".into()) < Value::Text("b".into()));
    assert!(Value::Text("z".into()) < Value::Blob(vec![]));
    assert!(Value::Blob(vec![0]) < Value::Blob(vec![1]));
}

#[test]
fn value_numeric_mixed() {
    assert_eq!(Value::Integer(1), Value::Real(1.0));
    assert!(Value::Integer(1) < Value::Real(1.5));
    assert!(Value::Real(0.5) < Value::Integer(1));
}

#[test]
fn equal_numeric_zeros_have_the_same_hash() {
    use std::collections::hash_map::DefaultHasher;

    let hash = |value: &Value| {
        let mut state = DefaultHasher::new();
        value.hash(&mut state);
        state.finish()
    };
    let values = [Value::Integer(0), Value::Real(0.0), Value::Real(-0.0)];
    for a in &values {
        for b in &values {
            assert_eq!(a, b);
            assert_eq!(hash(a), hash(b), "{a:?}, {b:?}");
            assert_eq!(
                hash(&Value::Array(vec![a.clone()].into())),
                hash(&Value::Array(vec![b.clone()].into()))
            );
        }
    }
}

#[test]
fn strict_real_to_integer_checks_the_exclusive_upper_bound() {
    let upper = -(i64::MIN as f64);
    for real in [
        upper,
        -upper - 2048.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        1.5,
    ] {
        assert!(
            Value::Real(real).strict_coerce(DataType::Integer).is_none(),
            "{real}"
        );
    }
    for (real, integer) in [
        (i64::MIN as f64, i64::MIN),
        (upper - 1024.0, i64::MAX - 1023),
        (-0.0, 0),
        (42.0, 42),
    ] {
        assert_eq!(
            Value::Real(real).strict_coerce(DataType::Integer),
            Some(Value::Integer(integer))
        );
    }
}

#[test]
fn strict_integer_to_real_checks_significand_precision() {
    for integer in [i64::MIN, i64::MAX, i64::MAX - 1023, i64::MIN + 1] {
        let real = integer as f64;
        let expected = ((real as i128) == i128::from(integer)).then_some(Value::Real(real));
        assert_eq!(
            Value::Integer(integer).strict_coerce(DataType::Real),
            expected,
            "{integer}"
        );
    }
    for exponent in 0..=62 {
        for sign in [-1, 1] {
            for offset in -3..=3 {
                let integer = sign * (1i64 << exponent) + offset;
                let real = integer as f64;
                let expected = ((real as i128) == i128::from(integer)).then_some(Value::Real(real));
                assert_eq!(
                    Value::Integer(integer).strict_coerce(DataType::Real),
                    expected,
                    "{integer}"
                );
            }
        }
    }
}

#[test]
fn value_display() {
    assert_eq!(format!("{}", Value::Null), "NULL");
    assert_eq!(format!("{}", Value::Integer(42)), "42");
    assert_eq!(format!("{}", Value::Real(3.15)), "3.15");
    assert_eq!(format!("{}", Value::Real(1.0)), "1.0");
    assert_eq!(format!("{}", Value::Text("hello".into())), "hello");
    assert_eq!(format!("{}", Value::Blob(vec![0xDE, 0xAD])), "X'DEAD'");
    assert_eq!(format!("{}", Value::Boolean(true)), "TRUE");
    assert_eq!(format!("{}", Value::Boolean(false)), "FALSE");
}

#[test]
fn value_coerce() {
    assert_eq!(
        Value::Integer(42).coerce_to(DataType::Real),
        Some(Value::Real(42.0))
    );
    assert_eq!(
        Value::Boolean(true).coerce_to(DataType::Integer),
        Some(Value::Integer(1))
    );
    assert_eq!(Value::Null.coerce_to(DataType::Integer), Some(Value::Null));
    assert_eq!(Value::Text("x".into()).coerce_to(DataType::Integer), None);
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

#[test]
fn schema_roundtrip() {
    let schema = TableSchema::new(
        "users".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("name", DataType::Text, true, 1),
            col("active", DataType::Boolean, false, 2),
        ],
        vec![0],
        vec![],
        vec![],
        vec![],
    );

    let data = schema.serialize();
    let restored = TableSchema::deserialize(&data).unwrap();

    assert_eq!(restored.name, "users");
    assert_eq!(restored.columns.len(), 3);
    assert_eq!(restored.columns[0].name, "id");
    assert_eq!(restored.columns[0].data_type, DataType::Integer);
    assert!(!restored.columns[0].nullable);
    assert_eq!(restored.columns[1].name, "name");
    assert_eq!(restored.columns[1].data_type, DataType::Text);
    assert!(restored.columns[1].nullable);
    assert_eq!(restored.columns[2].name, "active");
    assert_eq!(restored.columns[2].data_type, DataType::Boolean);
    assert_eq!(restored.primary_key_columns, vec![0]);
}

#[test]
fn legacy_schema_loads_and_marks_volatile_generated_expressions() {
    use crate::parser::{parse_sql_expr, GeneratedKind};

    let sql = "RANDOM()";
    let mut columns = vec![
        col("id", DataType::Integer, false, 0),
        col("g", DataType::Real, true, 1),
    ];
    columns[1].generated_sql = Some(sql.into());
    columns[1].generated_expr = Some(parse_sql_expr(sql).unwrap());
    columns[1].generated_kind = Some(GeneratedKind::Stored);
    let schema = TableSchema::new(
        "legacy_generated".into(),
        columns,
        vec![0],
        vec![],
        vec![],
        vec![],
    );

    let mut legacy_bytes = schema.serialize();
    legacy_bytes[0] = 5; // First schema version that persisted generated expressions.
    let restored = TableSchema::deserialize(&legacy_bytes).unwrap();
    assert_eq!(
        restored.volatile_persisted_expression(),
        Some("generated column \"legacy_generated.g\" calls volatile function RANDOM()".into())
    );
}

#[test]
fn legacy_schema_loads_and_marks_volatile_expression_index_keys() {
    use crate::parser::parse_sql_expr;

    let sql = "RANDOM()";
    let schema = TableSchema::new(
        "legacy_expression_index".into(),
        vec![col("id", DataType::Integer, false, 0)],
        vec![0],
        vec![IndexDef {
            name: "legacy_expr_idx".into(),
            keys: vec![IndexKey::Expr {
                expr: parse_sql_expr(sql).unwrap(),
                original_sql: sql.into(),
            }],
            unique: false,
            predicate_sql: None,
            predicate_expr: None,
            kind: IndexKind::BTree,
            ann_filter_cols: vec![],
        }],
        vec![],
        vec![],
    );

    let mut legacy_bytes = schema.serialize();
    legacy_bytes[0] = 12; // First schema version that persisted expression-index keys.
    let restored = TableSchema::deserialize(&legacy_bytes).unwrap();
    assert_eq!(
        restored.volatile_persisted_expression(),
        Some(
            "expression key of index \"legacy_expr_idx\" on table \"legacy_expression_index\" calls volatile function RANDOM()"
                .into()
        )
    );
}

#[test]
fn legacy_schema_loads_and_marks_volatile_partial_index_predicates() {
    use crate::parser::parse_sql_expr;

    let sql = "id > RANDOM()";
    let schema = TableSchema::new(
        "legacy_partial_index".into(),
        vec![col("id", DataType::Integer, false, 0)],
        vec![0],
        vec![IndexDef::from_column_lists(
            "legacy_partial_idx".into(),
            vec![0],
            vec![],
            false,
            Some(sql.into()),
            Some(parse_sql_expr(sql).unwrap()),
            IndexKind::BTree,
        )],
        vec![],
        vec![],
    );

    let mut legacy_bytes = schema.serialize();
    legacy_bytes[0] = 6; // First schema version that persisted partial-index predicates.
    let restored = TableSchema::deserialize(&legacy_bytes).unwrap();
    assert_eq!(
        restored.volatile_persisted_expression(),
        Some(
            "predicate of partial index \"legacy_partial_idx\" on table \"legacy_partial_index\" calls volatile function RANDOM()"
                .into()
        )
    );
}

#[test]
fn legacy_session_dependent_jsonpath_loads_but_is_identified_for_utc_guard() {
    use crate::parser::{parse_sql_expr, GeneratedKind};

    let generated_sql = r#"JSONB_PATH_MATCH_TZ(j, '$.timestamp_tz()')"#;
    let mut columns = vec![
        col("id", DataType::Integer, false, 0),
        col("j", DataType::Jsonb, true, 1),
        col("g", DataType::Boolean, true, 2),
    ];
    columns[2].generated_sql = Some(generated_sql.into());
    columns[2].generated_expr = Some(parse_sql_expr(generated_sql).unwrap());
    columns[2].generated_kind = Some(GeneratedKind::Stored);
    let generated = TableSchema::new(
        "legacy_generated".into(),
        columns,
        vec![0],
        vec![],
        vec![],
        vec![],
    );
    let mut generated_bytes = generated.serialize();
    generated_bytes[0] = 5;
    let restored_generated = TableSchema::deserialize(&generated_bytes).unwrap();
    assert_eq!(
        restored_generated.session_dependent_persisted_expression(),
        Some("generated column \"legacy_generated.g\"".into())
    );

    let key_sql = r#"JSONB_PATH_QUERY_FIRST(j, '$.time_tz()')"#;
    let expression_index = TableSchema::new(
        "legacy_expression_index".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("j", DataType::Jsonb, true, 1),
        ],
        vec![0],
        vec![IndexDef {
            name: "legacy_expr_idx".into(),
            keys: vec![IndexKey::Expr {
                expr: parse_sql_expr(key_sql).unwrap(),
                original_sql: key_sql.into(),
            }],
            unique: false,
            predicate_sql: None,
            predicate_expr: None,
            kind: IndexKind::BTree,
            ann_filter_cols: vec![],
        }],
        vec![],
        vec![],
    );
    let mut expression_bytes = expression_index.serialize();
    expression_bytes[0] = 12;
    let restored_expression = TableSchema::deserialize(&expression_bytes).unwrap();
    assert_eq!(
        restored_expression.session_dependent_persisted_expression(),
        Some(
            "expression key of index \"legacy_expr_idx\" on table \"legacy_expression_index\""
                .into()
        )
    );

    let predicate_sql = r#"JSONB_PATH_EXISTS(j, '$.time_tz()')"#;
    let partial_index = TableSchema::new(
        "legacy_partial_index".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("j", DataType::Jsonb, true, 1),
        ],
        vec![0],
        vec![IndexDef::from_column_lists(
            "legacy_partial_idx".into(),
            vec![0],
            vec![],
            false,
            Some(predicate_sql.into()),
            Some(parse_sql_expr(predicate_sql).unwrap()),
            IndexKind::BTree,
        )],
        vec![],
        vec![],
    );
    let mut predicate_bytes = partial_index.serialize();
    predicate_bytes[0] = 6;
    let restored_predicate = TableSchema::deserialize(&predicate_bytes).unwrap();
    assert_eq!(
        restored_predicate.session_dependent_persisted_expression(),
        Some(
            "predicate of partial index \"legacy_partial_idx\" on table \"legacy_partial_index\""
                .into()
        )
    );
}

#[test]
fn immutable_persisted_schema_expressions_still_roundtrip() {
    use crate::parser::{parse_sql_expr, GeneratedKind};

    let generated_sql = "LOWER(name)";
    let mut columns = vec![
        col("id", DataType::Integer, false, 0),
        col("name", DataType::Text, true, 1),
        col("payload", DataType::Jsonb, true, 2),
        col("normalized", DataType::Text, true, 3),
    ];
    columns[3].generated_sql = Some(generated_sql.into());
    columns[3].generated_expr = Some(parse_sql_expr(generated_sql).unwrap());
    columns[3].generated_kind = Some(GeneratedKind::Stored);

    let key_sql = "JSONB_PATH_QUERY_FIRST(payload, '$.account')";
    let predicate_sql = "JSONB_PATH_MATCH(payload, '$.active == true')";
    let schema = TableSchema::new(
        "safe_schema".into(),
        columns,
        vec![0],
        vec![IndexDef {
            name: "safe_idx".into(),
            keys: vec![IndexKey::Expr {
                expr: parse_sql_expr(key_sql).unwrap(),
                original_sql: key_sql.into(),
            }],
            unique: false,
            predicate_sql: Some(predicate_sql.into()),
            predicate_expr: Some(parse_sql_expr(predicate_sql).unwrap()),
            kind: IndexKind::BTree,
            ann_filter_cols: vec![],
        }],
        vec![],
        vec![],
    );

    let restored = TableSchema::deserialize(&schema.serialize()).unwrap();
    assert!(restored.columns[3].generated_expr.is_some());
    assert!(matches!(restored.indices[0].keys[0], IndexKey::Expr { .. }));
    assert!(restored.indices[0].predicate_expr.is_some());
    assert!(restored.session_dependent_persisted_expression().is_none());
}

#[test]
fn schema_roundtrip_with_indices() {
    let schema = TableSchema::new(
        "orders".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("customer", DataType::Text, false, 1),
            col("amount", DataType::Real, true, 2),
        ],
        vec![0],
        vec![
            IndexDef::from_column_lists(
                "idx_customer".into(),
                vec![1],
                vec![],
                false,
                None,
                None,
                IndexKind::default(),
            ),
            IndexDef::from_column_lists(
                "idx_amount_uniq".into(),
                vec![2],
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

    let data = schema.serialize();
    let restored = TableSchema::deserialize(&data).unwrap();

    assert_eq!(restored.indices.len(), 2);
    assert_eq!(restored.indices[0].name, "idx_customer");
    assert_eq!(restored.indices[0].columns_vec(), vec![1]);
    assert!(!restored.indices[0].unique);
    assert_eq!(restored.indices[1].name, "idx_amount_uniq");
    assert_eq!(restored.indices[1].columns_vec(), vec![2]);
    assert!(restored.indices[1].unique);
}

#[test]
fn schema_roundtrip_ann_filter_cols() {
    let mut ann = IndexDef::from_column_lists(
        "ix_v".into(),
        vec![3],
        vec![],
        false,
        None,
        None,
        IndexKind::Inverted(InvertedKind::Ann {
            metric: AnnMetric::Cosine,
        }),
    );
    ann.ann_filter_cols = vec![1, 2];

    let schema = TableSchema::new(
        "atoms".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("region", DataType::Integer, false, 1),
            col("kind", DataType::Text, false, 2),
            col("v", DataType::Vector { dim: 4 }, false, 3),
        ],
        vec![0],
        vec![ann],
        vec![],
        vec![],
    );

    let restored = TableSchema::deserialize(&schema.serialize()).unwrap();
    assert_eq!(restored.indices.len(), 1);
    assert_eq!(restored.indices[0].ann_filter_cols, vec![1, 2]);
    assert!(matches!(
        restored.indices[0].kind,
        IndexKind::Inverted(InvertedKind::Ann {
            metric: AnnMetric::Cosine
        })
    ));
}

#[test]
fn schema_roundtrip_no_filter_cols_is_empty() {
    let schema = TableSchema::new(
        "t".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("v", DataType::Vector { dim: 4 }, false, 1),
        ],
        vec![0],
        vec![IndexDef::from_column_lists(
            "ix_v".into(),
            vec![1],
            vec![],
            false,
            None,
            None,
            IndexKind::Inverted(InvertedKind::Ann {
                metric: AnnMetric::L2,
            }),
        )],
        vec![],
        vec![],
    );
    let restored = TableSchema::deserialize(&schema.serialize()).unwrap();
    assert!(restored.indices[0].ann_filter_cols.is_empty());
}

#[test]
fn schema_v1_backward_compat() {
    let old_schema = TableSchema::new(
        "test".into(),
        vec![col("id", DataType::Integer, false, 0)],
        vec![0],
        vec![],
        vec![],
        vec![],
    );
    let mut data = old_schema.serialize();
    data[0] = 1;
    let v1_len = 1 + 2 + 4 + 2 + (2 + 2 + 1 + 1 + 2) + 2 + 2;
    data.truncate(v1_len);

    let restored = TableSchema::deserialize(&data).unwrap();
    assert_eq!(restored.name, "test");
    assert!(restored.indices.is_empty());
    assert!(restored.check_constraints.is_empty());
    assert!(restored.foreign_keys.is_empty());
}

#[test]
fn schema_v2_backward_compat() {
    let schema = TableSchema::new(
        "test".into(),
        vec![col("id", DataType::Integer, false, 0)],
        vec![0],
        vec![],
        vec![],
        vec![],
    );
    let mut data = schema.serialize();
    data[0] = 2;
    let v2_len = 1 + 2 + 4 + 2 + 8 + 2 + 2 + 2;
    data.truncate(v2_len);

    let restored = TableSchema::deserialize(&data).unwrap();
    assert_eq!(restored.name, "test");
    assert!(restored.check_constraints.is_empty());
    assert!(restored.foreign_keys.is_empty());
    assert!(restored.columns[0].default_expr.is_none());
    assert!(restored.columns[0].check_expr.is_none());
}

#[test]
fn schema_roundtrip_with_defaults_and_checks() {
    use crate::parser::parse_sql_expr;

    let mut columns = vec![
        col("id", DataType::Integer, false, 0),
        col("val", DataType::Integer, true, 1),
        col("name", DataType::Text, true, 2),
    ];
    columns[1].default_sql = Some("42".into());
    columns[1].default_expr = Some(parse_sql_expr("42").unwrap());
    columns[2].check_sql = Some("LENGTH(name) > 0".into());
    columns[2].check_expr = Some(parse_sql_expr("LENGTH(name) > 0").unwrap());
    columns[2].check_name = Some("chk_name_len".into());

    let schema = TableSchema::new(
        "t".into(),
        columns,
        vec![0],
        vec![],
        vec![TableCheckDef {
            name: Some("chk_val_pos".into()),
            expr: parse_sql_expr("val > 0").unwrap(),
            sql: "val > 0".into(),
        }],
        vec![],
    );

    let data = schema.serialize();
    let restored = TableSchema::deserialize(&data).unwrap();

    assert_eq!(restored.columns[1].default_sql.as_deref(), Some("42"));
    assert!(restored.columns[1].default_expr.is_some());
    assert_eq!(
        restored.columns[2].check_sql.as_deref(),
        Some("LENGTH(name) > 0")
    );
    assert!(restored.columns[2].check_expr.is_some());
    assert_eq!(
        restored.columns[2].check_name.as_deref(),
        Some("chk_name_len")
    );
    assert_eq!(restored.check_constraints.len(), 1);
    assert_eq!(
        restored.check_constraints[0].name.as_deref(),
        Some("chk_val_pos")
    );
    assert_eq!(restored.check_constraints[0].sql, "val > 0");
}

#[test]
fn schema_roundtrip_with_foreign_keys() {
    let schema = TableSchema::new(
        "orders".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("user_id", DataType::Integer, false, 1),
        ],
        vec![0],
        vec![],
        vec![],
        vec![ForeignKeySchemaEntry {
            name: Some("fk_user".into()),
            columns: vec![1],
            foreign_table: "users".into(),
            referred_columns: vec!["id".into()],
            on_delete: crate::parser::ReferentialAction::NoAction,
            on_update: crate::parser::ReferentialAction::NoAction,
            deferrable: false,
            initially_deferred: false,
        }],
    );

    let data = schema.serialize();
    let restored = TableSchema::deserialize(&data).unwrap();

    assert_eq!(restored.foreign_keys.len(), 1);
    assert_eq!(restored.foreign_keys[0].name.as_deref(), Some("fk_user"));
    assert_eq!(restored.foreign_keys[0].columns, vec![1]);
    assert_eq!(restored.foreign_keys[0].foreign_table, "users");
    assert_eq!(restored.foreign_keys[0].referred_columns, vec!["id"]);
}

#[test]
fn data_type_display() {
    assert_eq!(format!("{}", DataType::Integer), "INTEGER");
    assert_eq!(format!("{}", DataType::Text), "TEXT");
    assert_eq!(format!("{}", DataType::Boolean), "BOOLEAN");
}

fn sample_trigger(name: &str, target: &str) -> TriggerDef {
    TriggerDef {
        name: name.into(),
        timing: crate::parser::TriggerTiming::After,
        events: vec![crate::parser::TriggerEvent::Insert],
        target: target.into(),
        granularity: crate::parser::TriggerGranularity::ForEachRow,
        referencing: None,
        when_sql: None,
        body_sql: "BEGIN INSERT INTO audit VALUES (1); END".into(),
        enabled: true,
        created_at_micros: 1234567,
    }
}

#[test]
fn trigger_def_roundtrip_simple() {
    use crate::parser::{TriggerGranularity, TriggerTiming};
    let td = sample_trigger("t1", "users");
    let bytes = td.serialize();
    let back = TriggerDef::deserialize(&bytes).unwrap();
    assert_eq!(back.name, "t1");
    assert_eq!(back.target, "users");
    assert!(matches!(back.timing, TriggerTiming::After));
    assert!(matches!(back.granularity, TriggerGranularity::ForEachRow));
    assert!(back.enabled);
    assert!(back.referencing.is_none());
    assert!(back.when_sql.is_none());
    assert_eq!(back.body_sql, td.body_sql);
}

#[test]
fn trigger_def_roundtrip_all_timings() {
    use crate::parser::TriggerTiming;
    for timing in [
        TriggerTiming::Before,
        TriggerTiming::After,
        TriggerTiming::InsteadOf,
    ] {
        let mut td = sample_trigger("t", "x");
        td.timing = timing;
        let back = TriggerDef::deserialize(&td.serialize()).unwrap();
        assert!(matches!(back.timing, t if t == timing));
    }
}

#[test]
fn trigger_def_roundtrip_update_with_columns() {
    use crate::parser::TriggerEvent;
    let mut td = sample_trigger("t", "x");
    td.events = vec![TriggerEvent::Update(vec!["email".into(), "name".into()])];
    let back = TriggerDef::deserialize(&td.serialize()).unwrap();
    match &back.events[0] {
        TriggerEvent::Update(cols) => {
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0], "email");
            assert_eq!(cols[1], "name");
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn trigger_def_roundtrip_multiple_events() {
    use crate::parser::TriggerEvent;
    let mut td = sample_trigger("t", "x");
    td.events = vec![
        TriggerEvent::Insert,
        TriggerEvent::Update(vec![]),
        TriggerEvent::Delete,
    ];
    let back = TriggerDef::deserialize(&td.serialize()).unwrap();
    assert_eq!(back.events.len(), 3);
    assert!(matches!(back.events[0], TriggerEvent::Insert));
    assert!(matches!(back.events[2], TriggerEvent::Delete));
}

#[test]
fn trigger_def_roundtrip_with_referencing_and_when() {
    use crate::parser::{TransitionTables, TriggerGranularity};
    let mut td = sample_trigger("t", "x");
    td.granularity = TriggerGranularity::ForEachStatement;
    td.referencing = Some(TransitionTables {
        new_table_alias: Some("new_t".into()),
        old_table_alias: Some("old_t".into()),
    });
    td.when_sql = Some("NEW.age > 18".into());
    let back = TriggerDef::deserialize(&td.serialize()).unwrap();
    assert!(matches!(
        back.granularity,
        TriggerGranularity::ForEachStatement
    ));
    let r = back.referencing.as_ref().unwrap();
    assert_eq!(r.new_table_alias.as_deref(), Some("new_t"));
    assert_eq!(r.old_table_alias.as_deref(), Some("old_t"));
    assert_eq!(back.when_sql.as_deref(), Some("NEW.age > 18"));
}

#[test]
fn trigger_def_roundtrip_disabled() {
    let mut td = sample_trigger("t", "x");
    td.enabled = false;
    let back = TriggerDef::deserialize(&td.serialize()).unwrap();
    assert!(!back.enabled);
}

#[test]
fn exact_value_identity_preserves_nested_numeric_representations() {
    let nested = |value| Value::Array(vec![Value::Array(vec![value].into())].into());
    let nan = f64::from_bits(0x7ff8_0000_0000_0042);
    assert!(nested(Value::Real(nan)).bit_eq(&nested(Value::Real(nan))));
    assert!(!nested(Value::Real(nan))
        .bit_eq(&nested(Value::Real(f64::from_bits(0x7ff8_0000_0000_0043)))));
    assert!(!nested(Value::Integer(1)).bit_eq(&nested(Value::Real(1.0))));
    assert!(!nested(Value::Real(0.0)).bit_eq(&nested(Value::Real(-0.0))));
    let vector = Value::Vector(vec![f32::from_bits(0x7fc0_0042), -0.0].into());
    assert!(vector.bit_eq(&vector.clone()));
    assert!(!vector.bit_eq(&Value::Vector(
        vec![f32::from_bits(0x7fc0_0042), 0.0].into()
    )));
    // SQL-level equality remains deliberately distinct.
    assert_eq!(Value::Integer(1), Value::Real(1.0));
    assert_eq!(Value::Real(0.0), Value::Real(-0.0));
}

#[test]
fn schema_count_boundary_distinguishes_logical_metadata_and_stored_slots() {
    assert!(TableSchema::validate_column_count(65535).is_ok());
    assert!(TableSchema::validate_column_count(65536).is_err());
    assert!(TableSchema::validate_column_count(usize::MAX).is_err());
    let columns = (0..32768)
        .map(|i| col("x", DataType::Integer, true, i))
        .collect();
    let schema = TableSchema::new("derived".into(), columns, vec![], vec![], vec![], vec![]);
    assert_eq!(schema.physical_non_pk_count(), 32768);
    assert!(matches!(
        schema.validate_storage_layout(),
        Err(crate::error::SqlError::InvalidValue(_))
    ));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| schema.serialize())).is_err());
}

#[test]
fn schema_count_boundary_deserialize_is_fallible_for_legacy_oversized_rows() {
    // Build V1 independently: no current serializer may create the rejected fixture.
    fn legacy(count: u16) -> Vec<u8> {
        let mut bytes = vec![1, 1, 0, b't'];
        bytes.extend_from_slice(&count.to_le_bytes());
        for position in 0..count {
            bytes.extend_from_slice(&[1, 0, b'x', DataType::Integer.type_tag(), 1]);
            bytes.extend_from_slice(&position.to_le_bytes());
        }
        bytes.extend_from_slice(&0u16.to_le_bytes()); // No PK columns.
        bytes
    }
    let accepted = TableSchema::deserialize(&legacy(32767)).unwrap();
    assert_eq!(accepted.physical_non_pk_count(), 32767);
    assert!(
        matches!(TableSchema::deserialize(&legacy(32768)), Err(crate::error::SqlError::InvalidValue(message)) if message.contains("32767"))
    );
}

#[test]
fn schema_count_boundary_revalidates_holes_after_public_field_mutation() {
    let mut schema = TableSchema::with_drops(
        "t".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("v", DataType::Integer, true, 1),
        ],
        vec![0],
        vec![],
        vec![],
        vec![],
        vec![1],
    );
    assert!(schema.validate_storage_layout().is_ok());
    // Turning the remaining live non-PK into a PK makes physical hole 1 out of range.
    schema.primary_key_columns.push(1);
    assert!(
        matches!(schema.validate_storage_layout(), Err(crate::error::SqlError::InvalidValue(message)) if message.contains("dropped physical"))
    );
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| schema.serialize())).is_err());
}
