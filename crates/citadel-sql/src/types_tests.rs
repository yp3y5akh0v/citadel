use super::*;

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
            IndexDef {
                name: "idx_customer".into(),
                columns: vec![1],
                unique: false,
            },
            IndexDef {
                name: "idx_amount_uniq".into(),
                columns: vec![2],
                unique: true,
            },
        ],
        vec![],
        vec![],
    );

    let data = schema.serialize();
    let restored = TableSchema::deserialize(&data).unwrap();

    assert_eq!(restored.indices.len(), 2);
    assert_eq!(restored.indices[0].name, "idx_customer");
    assert_eq!(restored.indices[0].columns, vec![1]);
    assert!(!restored.indices[0].unique);
    assert_eq!(restored.indices[1].name, "idx_amount_uniq");
    assert_eq!(restored.indices[1].columns, vec![2]);
    assert!(restored.indices[1].unique);
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
