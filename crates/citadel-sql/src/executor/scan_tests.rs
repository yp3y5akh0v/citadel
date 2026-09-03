use super::*;
use crate::encoding::RawColumn;
use crate::parser::{BinOp, Expr};
use crate::types::{Collation, ColumnDef, DataType, TableSchema, Value};

fn columns(specs: &[(&str, DataType)]) -> Vec<ColumnDef> {
    specs
        .iter()
        .enumerate()
        .map(|(i, (name, dt))| ColumnDef {
            name: (*name).to_string(),
            data_type: *dt,
            nullable: true,
            position: i as u16,
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
        })
        .collect()
}

fn schema(name: &str, cols: Vec<ColumnDef>, pk: Vec<u16>) -> TableSchema {
    TableSchema::new(name.into(), cols, pk, vec![], vec![], vec![])
}

fn i(n: i64) -> Value {
    Value::Integer(n)
}

#[test]
fn check_pk_range_empty_matches() {
    assert_eq!(check_pk_range(&i(5), &[]), 0);
}

#[test]
fn check_pk_range_lt_below_matches() {
    assert_eq!(check_pk_range(&i(5), &[(BinOp::Lt, i(10))]), 0);
}

#[test]
fn check_pk_range_lt_at_bound_stops() {
    assert_eq!(check_pk_range(&i(10), &[(BinOp::Lt, i(10))]), 2);
}

#[test]
fn check_pk_range_lteq_equal_matches() {
    assert_eq!(check_pk_range(&i(10), &[(BinOp::LtEq, i(10))]), 0);
}

#[test]
fn check_pk_range_lteq_above_stops() {
    assert_eq!(check_pk_range(&i(11), &[(BinOp::LtEq, i(10))]), 2);
}

#[test]
fn check_pk_range_gt_at_bound_skips() {
    assert_eq!(check_pk_range(&i(5), &[(BinOp::Gt, i(5))]), 1);
}

#[test]
fn check_pk_range_gt_above_matches() {
    assert_eq!(check_pk_range(&i(6), &[(BinOp::Gt, i(5))]), 0);
}

#[test]
fn check_pk_range_gteq_equal_matches() {
    assert_eq!(check_pk_range(&i(5), &[(BinOp::GtEq, i(5))]), 0);
}

#[test]
fn check_pk_range_gteq_below_skips() {
    assert_eq!(check_pk_range(&i(4), &[(BinOp::GtEq, i(5))]), 1);
}

#[test]
fn check_pk_range_combined_lower_upper() {
    let conds = vec![(BinOp::GtEq, i(5)), (BinOp::Lt, i(10))];
    assert_eq!(check_pk_range(&i(4), &conds), 1);
    assert_eq!(check_pk_range(&i(5), &conds), 0);
    assert_eq!(check_pk_range(&i(9), &conds), 0);
    assert_eq!(check_pk_range(&i(10), &conds), 2);
}

#[test]
fn flip_cmp_op_symmetric() {
    assert_eq!(flip_cmp_op(BinOp::Eq), Some(BinOp::Eq));
    assert_eq!(flip_cmp_op(BinOp::NotEq), Some(BinOp::NotEq));
}

#[test]
fn flip_cmp_op_asymmetric() {
    assert_eq!(flip_cmp_op(BinOp::Lt), Some(BinOp::Gt));
    assert_eq!(flip_cmp_op(BinOp::Gt), Some(BinOp::Lt));
    assert_eq!(flip_cmp_op(BinOp::LtEq), Some(BinOp::GtEq));
    assert_eq!(flip_cmp_op(BinOp::GtEq), Some(BinOp::LtEq));
}

#[test]
fn flip_cmp_op_non_comparison_returns_none() {
    assert_eq!(flip_cmp_op(BinOp::And), None);
    assert_eq!(flip_cmp_op(BinOp::Add), None);
}

#[test]
fn raw_matches_op_value_eq() {
    assert!(raw_matches_op_value(&i(5), BinOp::Eq, &i(5)));
    assert!(!raw_matches_op_value(&i(5), BinOp::Eq, &i(6)));
}

#[test]
fn raw_matches_op_value_neq() {
    assert!(raw_matches_op_value(&i(5), BinOp::NotEq, &i(6)));
    assert!(!raw_matches_op_value(&i(5), BinOp::NotEq, &i(5)));
}

#[test]
fn raw_matches_op_value_neq_null_lhs_is_false() {
    assert!(!raw_matches_op_value(&Value::Null, BinOp::NotEq, &i(5)));
}

#[test]
fn raw_matches_op_value_ordering() {
    assert!(raw_matches_op_value(&i(4), BinOp::Lt, &i(5)));
    assert!(raw_matches_op_value(&i(5), BinOp::LtEq, &i(5)));
    assert!(raw_matches_op_value(&i(6), BinOp::Gt, &i(5)));
    assert!(raw_matches_op_value(&i(5), BinOp::GtEq, &i(5)));
}

#[test]
fn raw_matches_op_value_non_comparison_returns_false() {
    assert!(!raw_matches_op_value(&i(5), BinOp::And, &i(5)));
    assert!(!raw_matches_op_value(&i(5), BinOp::Add, &i(5)));
}

#[test]
fn raw_matches_op_raw_null_short_circuits() {
    assert!(!raw_matches_op(&RawColumn::Null, BinOp::Eq, &i(5)));
}

#[test]
fn raw_matches_op_literal_null_short_circuits() {
    assert!(!raw_matches_op(
        &RawColumn::Integer(5),
        BinOp::Eq,
        &Value::Null
    ));
}

#[test]
fn raw_matches_op_eq_integer() {
    assert!(raw_matches_op(&RawColumn::Integer(5), BinOp::Eq, &i(5)));
    assert!(!raw_matches_op(&RawColumn::Integer(5), BinOp::Eq, &i(6)));
}

#[test]
fn raw_matches_op_neq_integer() {
    assert!(raw_matches_op(&RawColumn::Integer(5), BinOp::NotEq, &i(6)));
    assert!(!raw_matches_op(&RawColumn::Integer(5), BinOp::NotEq, &i(5)));
}

#[test]
fn raw_matches_op_lt_integer() {
    assert!(raw_matches_op(&RawColumn::Integer(4), BinOp::Lt, &i(5)));
    assert!(!raw_matches_op(&RawColumn::Integer(5), BinOp::Lt, &i(5)));
}

#[test]
fn raw_matches_op_gteq_integer() {
    assert!(raw_matches_op(&RawColumn::Integer(5), BinOp::GtEq, &i(5)));
    assert!(raw_matches_op(&RawColumn::Integer(6), BinOp::GtEq, &i(5)));
    assert!(!raw_matches_op(&RawColumn::Integer(4), BinOp::GtEq, &i(5)));
}

#[test]
fn try_simple_predicate_integer_eq() {
    let ts = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Integer)]),
        vec![0],
    );
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("v".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(7))),
    };
    assert!(try_simple_predicate(&expr, &ts).is_some());
}

#[test]
fn try_simple_predicate_unknown_column_returns_none() {
    let ts = schema("t", columns(&[("id", DataType::Integer)]), vec![0]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("missing".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(1))),
    };
    assert!(try_simple_predicate(&expr, &ts).is_none());
}

fn typed_predicate_row(values: &[Value], v2: bool) -> Vec<u8> {
    if v2 {
        return crate::encoding::encode_row(values);
    }
    let mut encoded = (values.len() as u16).to_le_bytes().to_vec();
    encoded.resize(2 + values.len().div_ceil(8), 0);
    for (idx, value) in values.iter().enumerate() {
        let (tag, body) = match value {
            Value::Null => {
                encoded[2 + idx / 8] |= 1 << (idx % 8);
                continue;
            }
            Value::Integer(value) => (DataType::Integer.type_tag(), value.to_le_bytes().to_vec()),
            Value::Real(value) => (DataType::Real.type_tag(), value.to_le_bytes().to_vec()),
            Value::Text(value) => (DataType::Text.type_tag(), value.as_bytes().to_vec()),
            _ => panic!("unsupported fixture value: {value:?}"),
        };
        encoded.push(tag);
        encoded.extend_from_slice(&(body.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&body);
    }
    assert_eq!(crate::encoding::decode_row(&encoded).unwrap(), values);
    encoded
}

fn assert_typed_predicate_comparisons(
    table: &TableSchema,
    column: &str,
    key: &[u8],
    encoded: &[u8],
    value: &Value,
    literal: &Value,
) {
    let eval_columns = columns(&[(
        column,
        table.columns[table.column_index(column).unwrap()].data_type,
    )]);
    let col_map = ColumnMap::new(&eval_columns);
    let row = [value.clone()];
    let ctx = crate::eval::EvalCtx::new(&col_map, &row);
    for op in [
        BinOp::Eq,
        BinOp::NotEq,
        BinOp::Lt,
        BinOp::LtEq,
        BinOp::Gt,
        BinOp::GtEq,
    ] {
        for reversed in [false, true] {
            let mut left = Expr::Column(column.into());
            let mut right = Expr::Literal(literal.clone());
            if reversed {
                std::mem::swap(&mut left, &mut right);
            }
            let expr = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
            let expected = crate::eval::eval_expr(&expr, &ctx).unwrap();
            let predicate = try_simple_predicate(&expr, table).unwrap();
            assert_eq!(
                predicate.matches_raw(key, encoded).unwrap(),
                matches!(expected, Value::Boolean(true)),
                "value={value:?}, literal={literal:?}, op={op:?}, reversed={reversed}"
            );
        }
    }
}

fn check_real_column_integer_literal(v2: bool) {
    let table = schema(
        "t",
        columns(&[
            ("id", DataType::Integer),
            ("prefix", DataType::Text),
            ("v", DataType::Real),
        ]),
        vec![0],
    );
    let key = crate::encoding::encode_composite_key(&[i(7)]);
    for value in [
        Value::Real(1.0),
        Value::Real(-1.0),
        Value::Real(1.5),
        Value::Null,
    ] {
        for prefix in [
            Value::Text("preceding variable-width cell".into()),
            Value::Null,
        ] {
            let encoded = typed_predicate_row(&[prefix, value.clone()], v2);
            assert_typed_predicate_comparisons(&table, "v", &key, &encoded, &value, &i(1));
        }
    }
}

#[test]
fn typed_simple_predicate_real_column_integer_literal_v1() {
    check_real_column_integer_literal(false);
}

#[test]
fn typed_simple_predicate_real_column_integer_literal_v2() {
    check_real_column_integer_literal(true);
}

#[test]
fn typed_simple_predicate_integer_column_and_mixed_literals() {
    let table = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Integer)]),
        vec![0],
    );
    let key = crate::encoding::encode_composite_key(&[i(7)]);
    for v2 in [false, true] {
        for value in [i(-1), i(1), i(2), Value::Null] {
            let encoded = typed_predicate_row(std::slice::from_ref(&value), v2);
            for literal in [i(1), Value::Real(1.0), Value::Real(1.5)] {
                assert_typed_predicate_comparisons(&table, "v", &key, &encoded, &value, &literal);
            }
        }
    }
}

#[test]
fn typed_simple_predicate_uses_real_and_integer_defaults_only_for_missing_cells() {
    let key = crate::encoding::encode_composite_key(&[i(7)]);
    for (data_type, default) in [
        (DataType::Real, Value::Real(1.0)),
        (DataType::Integer, i(1)),
    ] {
        let mut cols = columns(&[
            ("id", DataType::Integer),
            ("prefix", DataType::Text),
            ("v", data_type),
        ]);
        cols[2].default_expr = Some(Expr::Literal(default.clone()));
        let table = schema("t", cols, vec![0]);
        for v2 in [false, true] {
            let prefix = Value::Text("older row".into());
            let missing = typed_predicate_row(std::slice::from_ref(&prefix), v2);
            assert_typed_predicate_comparisons(&table, "v", &key, &missing, &default, &i(1));
            let stored_null = typed_predicate_row(&[prefix, Value::Null], v2);
            assert_typed_predicate_comparisons(
                &table,
                "v",
                &key,
                &stored_null,
                &Value::Null,
                &i(1),
            );
        }
    }
}

#[test]
fn typed_simple_predicate_single_real_primary_key() {
    let table = schema("t", columns(&[("v", DataType::Real)]), vec![0]);
    for value in [Value::Real(-1.0), Value::Real(1.0), Value::Real(1.5)] {
        let key = crate::encoding::encode_composite_key(std::slice::from_ref(&value));
        assert_typed_predicate_comparisons(&table, "v", &key, &[], &value, &i(1));
    }
}

#[test]
fn typed_simple_predicate_compound_real_primary_key() {
    let table = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Real)]),
        vec![0, 1],
    );
    for value in [Value::Real(-1.0), Value::Real(1.0), Value::Real(1.5)] {
        let key = crate::encoding::encode_composite_key(&[i(7), value.clone()]);
        assert_typed_predicate_comparisons(&table, "v", &key, &[], &value, &i(1));
    }
}

#[test]
fn typed_simple_predicate_single_integer_primary_key() {
    let table = schema("t", columns(&[("v", DataType::Integer)]), vec![0]);
    for value in [i(-1), i(1), i(2)] {
        let key = crate::encoding::encode_composite_key(std::slice::from_ref(&value));
        assert_typed_predicate_comparisons(&table, "v", &key, &[], &value, &Value::Real(1.0));
    }
}

#[test]
fn typed_simple_predicate_truncated_integer_rows_return_errors() {
    let table = schema(
        "t",
        columns(&[
            ("id", DataType::Integer),
            ("prefix", DataType::Text),
            ("v", DataType::Integer),
        ]),
        vec![0],
    );
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("v".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(1))),
    };
    let predicate = try_simple_predicate(&expr, &table).unwrap();
    for v2 in [false, true] {
        let encoded = typed_predicate_row(&[Value::Text("prefix".into()), i(1)], v2);
        for end in 0..encoded.len() {
            assert!(
                matches!(
                    predicate.matches_raw(&[], &encoded[..end]),
                    Err(SqlError::InvalidValue(_))
                ),
                "truncated row must return InvalidValue: v2={v2}, end={end}"
            );
        }
    }
}

#[test]
fn typed_simple_predicate_invalid_cell_tags_and_lengths_return_errors() {
    let table = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Integer)]),
        vec![0],
    );
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("v".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(1))),
    };
    let predicate = try_simple_predicate(&expr, &table).unwrap();
    for v2 in [false, true] {
        let mut encoded = typed_predicate_row(&[i(1)], v2);
        encoded[3] = u8::MAX;
        assert!(matches!(
            predicate.matches_raw(&[], &encoded),
            Err(SqlError::InvalidValue(_))
        ));
    }
    let mut invalid_length = typed_predicate_row(&[i(1)], false);
    invalid_length[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        predicate.matches_raw(&[], &invalid_length),
        Err(SqlError::InvalidValue(_))
    ));
}

#[test]
fn typed_simple_predicate_short_v1_integer_payload_returns_error() {
    let table = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Integer)]),
        vec![0],
    );
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("v".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(i(1))),
    };
    let predicate = try_simple_predicate(&expr, &table).unwrap();
    let mut encoded = typed_predicate_row(&[i(1)], false);
    encoded[4..8].copy_from_slice(&4u32.to_le_bytes());
    encoded.truncate(12);
    assert!(matches!(
        predicate.matches_raw(&[], &encoded),
        Err(SqlError::InvalidValue(_))
    ));
}

fn typed_between_expr(negated: bool) -> Expr {
    Expr::Between {
        expr: Box::new(Expr::Column("v".into())),
        low: Box::new(Expr::Literal(i(1))),
        high: Box::new(Expr::Literal(i(2))),
        negated,
    }
}

#[test]
fn typed_between_predicate_real_rows_and_missing_defaults() {
    let mut cols = columns(&[("id", DataType::Integer), ("v", DataType::Real)]);
    cols[1].default_expr = Some(Expr::Literal(Value::Real(1.5)));
    let table = schema("t", cols, vec![0]);
    for negated in [false, true] {
        let predicate = try_between_predicate(&typed_between_expr(negated), &table).unwrap();
        for v2 in [false, true] {
            for (value, in_range) in [(Value::Real(0.5), false), (Value::Real(1.5), true)] {
                let encoded = typed_predicate_row(&[value], v2);
                assert_eq!(
                    predicate.matches_raw(&[], &encoded).unwrap(),
                    in_range != negated
                );
            }
            let missing = typed_predicate_row(&[], v2);
            assert_eq!(predicate.matches_raw(&[], &missing).unwrap(), !negated);
            let stored_null = typed_predicate_row(&[Value::Null], v2);
            assert!(!predicate.matches_raw(&[], &stored_null).unwrap());
        }
    }
}

#[test]
fn typed_between_predicate_single_real_primary_key() {
    let table = schema("t", columns(&[("v", DataType::Real)]), vec![0]);
    for negated in [false, true] {
        let predicate = try_between_predicate(&typed_between_expr(negated), &table).unwrap();
        for (value, in_range) in [(Value::Real(0.5), false), (Value::Real(1.5), true)] {
            let key = crate::encoding::encode_composite_key(&[value]);
            assert_eq!(
                predicate.matches_raw(&key, &[]).unwrap(),
                in_range != negated
            );
        }
    }
}

#[test]
fn typed_between_predicate_truncated_rows_return_errors() {
    let table = schema(
        "t",
        columns(&[
            ("id", DataType::Integer),
            ("prefix", DataType::Text),
            ("v", DataType::Real),
        ]),
        vec![0],
    );
    let predicate = try_between_predicate(&typed_between_expr(false), &table).unwrap();
    for v2 in [false, true] {
        let encoded = typed_predicate_row(&[Value::Text("prefix".into()), Value::Real(1.5)], v2);
        for end in 0..encoded.len() {
            assert!(
                matches!(
                    predicate.matches_raw(&[], &encoded[..end]),
                    Err(SqlError::InvalidValue(_))
                ),
                "truncated BETWEEN row: v2={v2}, end={end}"
            );
        }
    }
}

#[test]
fn typed_between_predicate_short_v1_real_payload_returns_error() {
    let table = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Real)]),
        vec![0],
    );
    let predicate = try_between_predicate(&typed_between_expr(false), &table).unwrap();
    let mut encoded = typed_predicate_row(&[Value::Real(1.5)], false);
    encoded[4..8].copy_from_slice(&4u32.to_le_bytes());
    encoded.truncate(12);
    assert!(matches!(
        predicate.matches_raw(&[], &encoded),
        Err(SqlError::InvalidValue(_))
    ));
}

fn assert_optional_predicate_matches_generic(
    table: &TableSchema,
    key: &[u8],
    encoded: &[u8],
    value: &Value,
    expr: &Expr,
) {
    let eval_columns = columns(&[(
        "v",
        table.columns[table.column_index("v").unwrap()].data_type,
    )]);
    let col_map = ColumnMap::new(&eval_columns);
    let row = [value.clone()];
    let expected =
        crate::eval::eval_expr(expr, &crate::eval::EvalCtx::new(&col_map, &row)).unwrap();
    let actual = if let Some(predicate) = try_simple_predicate(expr, table) {
        predicate.matches_raw(key, encoded).unwrap()
    } else if let Some(predicate) = try_between_predicate(expr, table) {
        predicate.matches_raw(key, encoded).unwrap()
    } else {
        return;
    };
    assert_eq!(
        actual,
        matches!(expected, Value::Boolean(true)),
        "value={value:?}, expr={expr:?}"
    );
}

#[test]
fn typed_predicate_not_between_null_bound_preserves_unknown() {
    let table = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Real)]),
        vec![0],
    );
    let value = Value::Real(1.5);
    let encoded = typed_predicate_row(std::slice::from_ref(&value), true);
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("v".into())),
        low: Box::new(Expr::Literal(Value::Null)),
        high: Box::new(Expr::Literal(i(2))),
        negated: true,
    };
    assert_optional_predicate_matches_generic(&table, &[], &encoded, &value, &expr);
}

#[test]
fn typed_predicate_null_literals_and_bounds_preserve_generic_truth() {
    let value = Value::Real(1.5);
    let mut cols = columns(&[("id", DataType::Integer), ("v", DataType::Real)]);
    cols[1].default_expr = Some(Expr::Literal(value.clone()));
    let nonpk_table = schema("t", cols, vec![0]);
    let pk_table = schema("t", columns(&[("v", DataType::Real)]), vec![0]);
    let stored = typed_predicate_row(std::slice::from_ref(&value), true);
    let missing = typed_predicate_row(&[], true);
    let key = crate::encoding::encode_composite_key(std::slice::from_ref(&value));
    let mut expressions = Vec::new();
    for op in [
        BinOp::Eq,
        BinOp::NotEq,
        BinOp::Lt,
        BinOp::LtEq,
        BinOp::Gt,
        BinOp::GtEq,
    ] {
        expressions.push(Expr::BinaryOp {
            left: Box::new(Expr::Column("v".into())),
            op,
            right: Box::new(Expr::Literal(Value::Null)),
        });
    }
    for (low, high) in [
        (Value::Null, i(2)),
        (i(1), Value::Null),
        (Value::Null, Value::Null),
        (Value::Null, i(1)),
        (i(2), Value::Null),
    ] {
        for negated in [false, true] {
            expressions.push(Expr::Between {
                expr: Box::new(Expr::Column("v".into())),
                low: Box::new(Expr::Literal(low.clone())),
                high: Box::new(Expr::Literal(high.clone())),
                negated,
            });
        }
    }
    for expr in expressions {
        for (table, row_key, encoded) in [
            (&nonpk_table, &[][..], stored.as_slice()),
            (&nonpk_table, &[][..], missing.as_slice()),
            (&pk_table, key.as_slice(), &[][..]),
        ] {
            assert_optional_predicate_matches_generic(table, row_key, encoded, &value, &expr);
        }
    }
}

#[test]
fn typed_predicate_mixed_type_ordering_preserves_generic_semantics() {
    let table = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Integer)]),
        vec![0],
    );
    let value = i(1);
    let encoded = typed_predicate_row(std::slice::from_ref(&value), true);
    for op in [
        BinOp::Eq,
        BinOp::NotEq,
        BinOp::Lt,
        BinOp::LtEq,
        BinOp::Gt,
        BinOp::GtEq,
    ] {
        for reversed in [false, true] {
            let mut left = Expr::Column("v".into());
            let mut right = Expr::Literal(Value::Text("1".into()));
            if reversed {
                std::mem::swap(&mut left, &mut right);
            }
            let expr = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
            assert_optional_predicate_matches_generic(&table, &[], &encoded, &value, &expr);
        }
    }
}

#[test]
fn typed_predicate_nan_comparisons_preserve_generic_semantics() {
    let table = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Real)]),
        vec![0],
    );
    for (value, literal) in [
        (Value::Real(f64::NAN), i(1)),
        (Value::Real(1.5), Value::Real(f64::NAN)),
    ] {
        let encoded = typed_predicate_row(std::slice::from_ref(&value), true);
        for op in [
            BinOp::Eq,
            BinOp::NotEq,
            BinOp::Lt,
            BinOp::LtEq,
            BinOp::Gt,
            BinOp::GtEq,
        ] {
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Column("v".into())),
                op,
                right: Box::new(Expr::Literal(literal.clone())),
            };
            assert_optional_predicate_matches_generic(&table, &[], &encoded, &value, &expr);
        }
        for negated in [false, true] {
            let expr = Expr::Between {
                expr: Box::new(Expr::Column("v".into())),
                low: Box::new(Expr::Literal(literal.clone())),
                high: Box::new(Expr::Literal(i(2))),
                negated,
            };
            assert_optional_predicate_matches_generic(&table, &[], &encoded, &value, &expr);
        }
    }
}

#[test]
fn typed_simple_predicate_real_sql_scan_matches_integer_literals() {
    use citadel::{Argon2Profile, DatabaseBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("typed-predicate.citadel"))
        .passphrase(b"typed-predicate-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v REAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, -1.0), (2, 1.0), (3, 1.5), (4, NULL)")
        .unwrap();
    for (predicate, ids) in [
        ("v = 1", vec![2]),
        ("1 = v", vec![2]),
        ("v != 1", vec![1, 3]),
        ("1 != v", vec![1, 3]),
        ("v < 1", vec![1]),
        ("1 > v", vec![1]),
        ("v <= 1", vec![1, 2]),
        ("1 >= v", vec![1, 2]),
        ("v > 1", vec![3]),
        ("1 < v", vec![3]),
        ("v >= 1", vec![2, 3]),
        ("1 <= v", vec![2, 3]),
    ] {
        let result = conn
            .query(&format!("SELECT id FROM t WHERE {predicate} ORDER BY id"))
            .unwrap();
        let expected = ids.into_iter().map(|id| vec![i(id)]).collect::<Vec<_>>();
        assert_eq!(result.rows, expected, "predicate: {predicate}");
    }
    for predicate in [
        "v = NULL",
        "v != NULL",
        "v NOT BETWEEN NULL AND 2",
        "v NOT BETWEEN 1 AND NULL",
        "v < '1'",
        "'1' > v",
    ] {
        let evaluated = conn
            .query(&format!("SELECT id, ({predicate}) FROM t ORDER BY id"))
            .unwrap();
        let expected: Vec<_> = evaluated
            .rows
            .into_iter()
            .filter(|row| matches!(row[1], Value::Boolean(true)))
            .map(|mut row| vec![std::mem::take(&mut row[0])])
            .collect();
        let filtered = conn
            .query(&format!("SELECT id FROM t WHERE {predicate} ORDER BY id"))
            .unwrap();
        assert_eq!(filtered.rows, expected, "predicate: {predicate}");
    }
}

#[test]
fn try_between_predicate_basic() {
    let ts = schema(
        "t",
        columns(&[("id", DataType::Integer), ("v", DataType::Integer)]),
        vec![0],
    );
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("v".into())),
        low: Box::new(Expr::Literal(i(1))),
        high: Box::new(Expr::Literal(i(10))),
        negated: false,
    };
    assert!(try_between_predicate(&expr, &ts).is_some());
}

#[test]
fn try_between_predicate_unknown_column_returns_none() {
    let ts = schema("t", columns(&[("id", DataType::Integer)]), vec![0]);
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("missing".into())),
        low: Box::new(Expr::Literal(i(1))),
        high: Box::new(Expr::Literal(i(2))),
        negated: false,
    };
    assert!(try_between_predicate(&expr, &ts).is_none());
}

#[test]
fn fold_temporal_offset_non_temporal_returns_none() {
    let expr = Expr::Literal(i(1));
    assert!(fold_temporal_offset(&expr).is_none());
}

#[test]
fn inverted_intersection_honors_cancellation_and_preserves_results() {
    let keys = |values: &[u16]| {
        values
            .iter()
            .map(|value| value.to_be_bytes().to_vec())
            .collect::<Vec<_>>()
    };
    let left = keys(&[1, 2, 4, 8]);
    let right = keys(&[2, 3, 4, 9]);
    assert_eq!(
        sorted_intersect(&left, &right, None).unwrap(),
        keys(&[2, 4])
    );

    let token = citadel::CancelToken::new();
    token.cancel();
    let err = sorted_intersect(&left, &right, Some(&token)).unwrap_err();
    assert!(matches!(
        err,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn scan_predicate_passes_cancellation_into_scalar_evaluation() {
    use crate::encoding::{encode_composite_key, encode_row};

    let table = schema(
        "docs",
        columns(&[("id", DataType::Integer), ("body", DataType::Text)]),
        vec![0],
    );
    let predicate = Expr::IsNotNull(Box::new(Expr::Function {
        name: "TO_TSVECTOR".into(),
        args: vec![Expr::Column("body".into())],
        distinct: false,
    }));
    let col_map = ColumnMap::new(&table.columns);
    let compiled = CompiledExpr::compile(&predicate, &col_map);
    let key = encode_composite_key(&[i(1)]);
    let value = encode_row(&[Value::Text("several words to tokenize".into())]);
    let token = citadel::CancelToken::new();
    let _cancel = crate::fts::cancel_tokenize_after(token.clone(), 1);

    let err = scan_step(
        &table,
        &key,
        &value,
        Some(&compiled),
        None,
        None,
        None,
        Some(&col_map),
        None,
        Some(&token),
    )
    .expect_err("the scan predicate discarded its cancellation token");

    assert!(matches!(
        err,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn point_lookup_does_not_turn_scalar_cancellation_into_no_match() {
    use citadel::{Argon2Profile, DatabaseBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("point-value-cancel.citadel"))
        .passphrase(b"point-value-cancel-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'several words to tokenize')")
        .unwrap();
    let schemas = crate::schema::SchemaManager::load(&db).unwrap();
    let table = schemas.get("docs").unwrap();
    let where_clause = Some(Expr::IsNotNull(Box::new(Expr::Function {
        name: "TO_TSVECTOR".into(),
        args: vec![Expr::Column("body".into())],
        distinct: false,
    })));

    let token = citadel::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let mut rtx = db.begin_read();
    let _cancel = crate::fts::cancel_tokenize_after(token, 1);
    let err = collect_rows_with_read_planned(
        &mut rtx,
        table,
        &where_clause,
        None,
        crate::planner::ScanPlan::PkLookup {
            pk_values: vec![i(1)],
            full_cover: false,
        },
    )
    .expect_err("the point lookup treated Interrupted as a false predicate");

    assert!(matches!(
        err,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn scan_decode_passes_cancellation_to_a_virtual_generated_value() {
    use citadel::{Argon2Profile, DatabaseBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("virtual-value-cancel.citadel"))
        .passphrase(b"virtual-value-cancel-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE docs (\
         id INTEGER PRIMARY KEY, \
         body TEXT, \
         search TSVECTOR GENERATED ALWAYS AS (TO_TSVECTOR(body)) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO docs (id, body) VALUES (1, 'several words to tokenize')")
        .unwrap();

    let token = citadel::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let _cancel = crate::fts::cancel_tokenize_after(token, 1);
    let error = conn
        .query("SELECT id FROM docs WHERE search IS NOT NULL")
        .expect_err("the VIRTUAL generated expression discarded its cancellation token");

    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}
