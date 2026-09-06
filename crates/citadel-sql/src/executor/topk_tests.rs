use std::collections::HashSet;

use super::{SortKey, SortOrder};
use crate::encoding::{decode_stored_column_raw, encode_row, RawColumn};
use crate::types::{Collation, Value};

fn raw_kind(value: RawColumn<'_>) -> &'static str {
    match value {
        RawColumn::Null => "null",
        RawColumn::Integer(_) => "integer",
        RawColumn::Real(_) => "real",
        RawColumn::Boolean(_) => "boolean",
        RawColumn::Text(_) => "text",
        RawColumn::Blob(_) => "blob",
        RawColumn::Time(_) => "time",
        RawColumn::Date(_) => "date",
        RawColumn::Timestamp(_) => "timestamp",
        RawColumn::Interval { .. } => "interval",
        RawColumn::Json(_) => "json",
        RawColumn::Jsonb(_) => "jsonb",
        RawColumn::TsVector(_) => "tsvector",
        RawColumn::TsQuery(_) => "tsquery",
        RawColumn::Array(_) => "array",
        RawColumn::Vector(_) => "vector",
    }
}

fn values() -> Vec<Value> {
    let mut values = vec![Value::Null, Value::Boolean(false), Value::Boolean(true)];
    values.extend(
        [
            i64::MIN,
            -(1i64 << 53) - 1,
            -(1i64 << 53),
            -1,
            0,
            1,
            (1i64 << 53) - 1,
            1i64 << 53,
            (1i64 << 53) + 1,
            i64::MAX,
        ]
        .into_iter()
        .map(Value::Integer),
    );
    values.extend(
        [
            f64::NEG_INFINITY,
            i64::MIN as f64,
            -1.0,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            0.5,
            1.0,
            (1u64 << 53) as f64,
            i64::MAX as f64,
            f64::INFINITY,
            f64::NAN,
            f64::from_bits(0xfff8_0000_0000_0001),
        ]
        .into_iter()
        .map(Value::Real),
    );
    values.extend(
        ["", "A", "a", "a ", "Z", "é", "É"]
            .into_iter()
            .map(|text| Value::Text(text.into())),
    );
    values.push(Value::Text("long-sort-value-".repeat(16).into()));
    values.extend([
        Value::Blob(vec![]),
        Value::Blob(vec![0, 1]),
        Value::Blob(vec![0, 2]),
        Value::Time(i64::MIN),
        Value::Time(0),
        Value::Time(i64::MAX),
        Value::Date(i32::MIN),
        Value::Date(0),
        Value::Date(i32::MAX),
        Value::Timestamp(i64::MIN),
        Value::Timestamp(0),
        Value::Timestamp(i64::MAX),
        Value::Interval {
            months: 1,
            days: 0,
            micros: 0,
        },
        Value::Interval {
            months: 0,
            days: 30,
            micros: 0,
        },
        Value::Interval {
            months: i32::MIN,
            days: i32::MAX,
            micros: i64::MIN,
        },
        Value::Interval {
            months: i32::MAX,
            days: i32::MIN,
            micros: i64::MAX,
        },
        Value::Json("null".into()),
        Value::Json("{\"a\":1}".into()),
        Value::Jsonb(vec![0].into()),
        Value::Jsonb(vec![1, 2].into()),
        Value::TsVector(vec![0].into()),
        Value::TsVector(vec![1, 2].into()),
        Value::TsQuery(vec![0].into()),
        Value::TsQuery(vec![1, 2].into()),
        Value::Array(vec![].into()),
        Value::Array(vec![Value::Null, Value::Integer(1)].into()),
        Value::Array(vec![Value::Null, Value::Real(1.0)].into()),
        Value::Array(vec![Value::Text("A".into()), Value::Integer(2)].into()),
        Value::Vector(vec![].into()),
        Value::Vector(vec![-0.0, 1.0].into()),
        Value::Vector(vec![0.0, 1.0].into()),
        Value::Vector(vec![1.0, f32::INFINITY].into()),
        Value::Vector(vec![1.0, f32::NAN].into()),
    ]);
    values
}

#[test]
fn raw_sort_comparisons_match_owned_values_for_every_runtime_type() {
    let values = values();
    let encoded: Vec<_> = values
        .iter()
        .map(|value| encode_row(std::slice::from_ref(value)))
        .collect();
    let raw: Vec<_> = encoded
        .iter()
        .map(|row| decode_stored_column_raw(row, 0).unwrap().unwrap())
        .collect();
    let kinds: HashSet<_> = raw.iter().copied().map(raw_kind).collect();
    assert_eq!(kinds.len(), 16);

    for collation in [Collation::Binary, Collation::NoCase, Collation::Rtrim] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let order = SortOrder {
                    descending,
                    nulls_first,
                    collation,
                };
                for (&left, owned_left) in raw.iter().zip(&values) {
                    for right in &values {
                        let expected = order.compare(owned_left, right);
                        assert_eq!(
                            order.compare_raw(left, right),
                            expected,
                            "{left:?} / {right:?}, {collation:?}, \
                             descending={descending}, nulls_first={nulls_first}"
                        );
                        assert_eq!(
                            SortKey::Borrowed(left).compare(right, order),
                            expected,
                            "borrowed sort key {left:?} / {right:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn raw_sort_comparisons_preserve_malformed_array_and_vector_nulls() {
    let truncated_element = [1, 0, 0, 0];
    let malformed = [
        RawColumn::Array(&[]),
        RawColumn::Array(&truncated_element),
        RawColumn::Vector(&[]),
        RawColumn::Vector(&truncated_element),
    ];
    let values = values();
    for raw in malformed {
        let owned = raw.to_value();
        assert!(owned.is_null(), "fixture must fail decoding: {raw:?}");
        for collation in [Collation::Binary, Collation::NoCase, Collation::Rtrim] {
            for descending in [false, true] {
                for nulls_first in [false, true] {
                    let order = SortOrder {
                        descending,
                        nulls_first,
                        collation,
                    };
                    for right in &values {
                        let expected = order.compare(&owned, right);
                        assert_eq!(
                            order.compare_raw(raw, right),
                            expected,
                            "{raw:?} / {right:?}, {collation:?}, \
                             descending={descending}, nulls_first={nulls_first}"
                        );
                        assert_eq!(SortKey::Borrowed(raw).compare(right, order), expected);
                    }
                }
            }
        }
    }
}
