use super::*;

#[test]
fn key_null() {
    let encoded = encode_key_value(&Value::Null);
    let (decoded, n) = decode_key_value(&encoded).unwrap();
    assert_eq!(n, 1);
    assert_eq!(decoded, Value::Null);
}

#[test]
fn key_boolean() {
    let f_enc = encode_key_value(&Value::Boolean(false));
    let t_enc = encode_key_value(&Value::Boolean(true));
    assert!(f_enc < t_enc);

    let (f_dec, _) = decode_key_value(&f_enc).unwrap();
    let (t_dec, _) = decode_key_value(&t_enc).unwrap();
    assert_eq!(f_dec, Value::Boolean(false));
    assert_eq!(t_dec, Value::Boolean(true));
}

#[test]
fn key_integer_roundtrip() {
    let test_values = [
        i64::MIN,
        -1_000_000,
        -256,
        -1,
        0,
        1,
        127,
        128,
        255,
        256,
        65535,
        1_000_000,
        i64::MAX,
    ];
    for &v in &test_values {
        let encoded = encode_key_value(&Value::Integer(v));
        let (decoded, _) = decode_key_value(&encoded).unwrap();
        assert_eq!(decoded, Value::Integer(v), "roundtrip failed for {v}");
    }
}

#[test]
fn key_integer_sort_order() {
    let values: Vec<i64> = vec![i64::MIN, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX];
    let encoded: Vec<Vec<u8>> = values
        .iter()
        .map(|&v| encode_key_value(&Value::Integer(v)))
        .collect();

    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "sort order broken: {} vs {}",
            values[i],
            values[i + 1]
        );
    }
}

#[test]
fn key_real_roundtrip() {
    let test_values = [
        f64::NEG_INFINITY,
        -1e100,
        -1.0,
        -f64::MIN_POSITIVE,
        -0.0,
        0.0,
        f64::MIN_POSITIVE,
        0.5,
        1.0,
        1e100,
        f64::INFINITY,
    ];
    for &v in &test_values {
        let encoded = encode_key_value(&Value::Real(v));
        let (decoded, _) = decode_key_value(&encoded).unwrap();
        match decoded {
            Value::Real(r) => {
                assert!(
                    v.to_bits() == r.to_bits(),
                    "roundtrip failed for {v}: got {r}"
                );
            }
            _ => panic!("expected Real"),
        }
    }
}

#[test]
fn key_real_sort_order() {
    let values = [
        f64::NEG_INFINITY,
        -100.0,
        -1.0,
        -0.0,
        0.0,
        1.0,
        100.0,
        f64::INFINITY,
    ];
    let encoded: Vec<Vec<u8>> = values
        .iter()
        .map(|&v| encode_key_value(&Value::Real(v)))
        .collect();

    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] <= encoded[i + 1],
            "sort order broken: {} vs {}",
            values[i],
            values[i + 1]
        );
    }
}

#[test]
fn key_text_roundtrip() {
    let test_values = ["", "hello", "world", "hello\0world", "\0\0\0"];
    for &v in &test_values {
        let encoded = encode_key_value(&Value::Text(v.into()));
        let (decoded, _) = decode_key_value(&encoded).unwrap();
        assert_eq!(decoded, Value::Text(v.into()), "roundtrip failed for {v:?}");
    }
}

#[test]
fn key_text_sort_order() {
    let values = ["", "a", "ab", "b", "ba", "z"];
    let encoded: Vec<Vec<u8>> = values
        .iter()
        .map(|&v| encode_key_value(&Value::Text(v.into())))
        .collect();

    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "sort order broken: {:?} vs {:?}",
            values[i],
            values[i + 1]
        );
    }
}

#[test]
fn key_blob_roundtrip() {
    let test_values: Vec<Vec<u8>> = vec![
        vec![],
        vec![0x00],
        vec![0x00, 0xFF],
        vec![0xFF, 0x00],
        vec![0x00, 0x00, 0x00],
    ];
    for v in &test_values {
        let encoded = encode_key_value(&Value::Blob(v.clone()));
        let (decoded, _) = decode_key_value(&encoded).unwrap();
        assert_eq!(decoded, Value::Blob(v.clone()));
    }
}

#[test]
fn key_composite_roundtrip() {
    let values = vec![
        Value::Integer(42),
        Value::Text("hello".into()),
        Value::Boolean(true),
    ];
    let encoded = encode_composite_key(&values);
    let decoded = decode_composite_key(&encoded, 3).unwrap();
    assert_eq!(decoded[0], Value::Integer(42));
    assert_eq!(decoded[1], Value::Text("hello".into()));
    assert_eq!(decoded[2], Value::Boolean(true));
}

#[test]
fn key_composite_sort_order() {
    let k1 = encode_composite_key(&[Value::Integer(1), Value::Text("b".into())]);
    let k2 = encode_composite_key(&[Value::Integer(1), Value::Text("c".into())]);
    let k3 = encode_composite_key(&[Value::Integer(2), Value::Text("a".into())]);
    assert!(k1 < k2);
    assert!(k2 < k3);
}

#[test]
fn key_cross_type_ordering() {
    let null = encode_key_value(&Value::Null);
    let bool_val = encode_key_value(&Value::Boolean(false));
    let int = encode_key_value(&Value::Integer(0));
    let text = encode_key_value(&Value::Text("".into()));
    let blob = encode_key_value(&Value::Blob(vec![]));

    assert!(null < blob);
    assert!(blob < text);
    assert!(text < bool_val);
    assert!(bool_val < int);
}

#[test]
fn row_roundtrip_simple() {
    let values = vec![
        Value::Integer(42),
        Value::Text("hello".into()),
        Value::Boolean(true),
    ];
    let encoded = encode_row(&values);
    let decoded = decode_row(&encoded).unwrap();
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[0], Value::Integer(42));
    assert_eq!(decoded[1], Value::Text("hello".into()));
    assert_eq!(decoded[2], Value::Boolean(true));
}

#[test]
fn row_roundtrip_with_nulls() {
    let values = vec![
        Value::Integer(1),
        Value::Null,
        Value::Text("test".into()),
        Value::Null,
    ];
    let encoded = encode_row(&values);
    let decoded = decode_row(&encoded).unwrap();
    assert_eq!(decoded.len(), 4);
    assert_eq!(decoded[0], Value::Integer(1));
    assert!(decoded[1].is_null());
    assert_eq!(decoded[2], Value::Text("test".into()));
    assert!(decoded[3].is_null());
}

#[test]
fn row_roundtrip_empty() {
    let values: Vec<Value> = vec![];
    let encoded = encode_row(&values);
    let decoded = decode_row(&encoded).unwrap();
    assert!(decoded.is_empty());
}

#[test]
fn row_roundtrip_all_types() {
    let values = vec![
        Value::Integer(-100),
        Value::Real(3.15),
        Value::Text("hello world".into()),
        Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        Value::Boolean(false),
        Value::Null,
    ];
    let encoded = encode_row(&values);
    let decoded = decode_row(&encoded).unwrap();
    assert_eq!(decoded.len(), 6);
    assert_eq!(decoded[0], Value::Integer(-100));
    assert_eq!(decoded[1], Value::Real(3.15));
    assert_eq!(decoded[2], Value::Text("hello world".into()));
    assert_eq!(decoded[3], Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    assert_eq!(decoded[4], Value::Boolean(false));
    assert!(decoded[5].is_null());
}

#[test]
fn null_escaped_with_embedded_nulls() {
    let text = "before\0after";
    let encoded = encode_key_value(&Value::Text(text.into()));
    let (decoded, _) = decode_key_value(&encoded).unwrap();
    assert_eq!(decoded, Value::Text(text.into()));
}

#[test]
fn key_integer_edge_cases() {
    for v in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
        let encoded = encode_key_value(&Value::Integer(v));
        let (decoded, n) = decode_key_value(&encoded).unwrap();
        assert_eq!(n, encoded.len());
        assert_eq!(decoded, Value::Integer(v), "edge case failed for {v}");
    }
}

#[test]
fn decode_columns_single() {
    let values = vec![
        Value::Integer(42),
        Value::Text("hello".into()),
        Value::Boolean(true),
    ];
    let encoded = encode_row(&values);
    let cols = decode_columns(&encoded, &[1]).unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], Value::Text("hello".into()));
}

#[test]
fn decode_columns_multiple() {
    let values = vec![
        Value::Integer(1),
        Value::Real(2.5),
        Value::Text("skip".into()),
        Value::Boolean(false),
        Value::Blob(vec![0xAB]),
    ];
    let encoded = encode_row(&values);
    let cols = decode_columns(&encoded, &[0, 3, 4]).unwrap();
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0], Value::Integer(1));
    assert_eq!(cols[1], Value::Boolean(false));
    assert_eq!(cols[2], Value::Blob(vec![0xAB]));
}

#[test]
fn decode_columns_with_nulls() {
    let values = vec![
        Value::Integer(10),
        Value::Null,
        Value::Text("after_null".into()),
        Value::Null,
        Value::Boolean(true),
    ];
    let encoded = encode_row(&values);
    let cols = decode_columns(&encoded, &[1, 2, 4]).unwrap();
    assert_eq!(cols.len(), 3);
    assert!(cols[0].is_null());
    assert_eq!(cols[1], Value::Text("after_null".into()));
    assert_eq!(cols[2], Value::Boolean(true));
}

#[test]
fn decode_columns_first_and_last() {
    let values = vec![
        Value::Text("first".into()),
        Value::Integer(99),
        Value::Boolean(false),
        Value::Real(3.125),
    ];
    let encoded = encode_row(&values);
    let cols = decode_columns(&encoded, &[0, 3]).unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0], Value::Text("first".into()));
    assert_eq!(cols[1], Value::Real(3.125));
}

#[test]
fn decode_columns_empty_targets() {
    let values = vec![Value::Integer(1)];
    let encoded = encode_row(&values);
    let cols = decode_columns(&encoded, &[]).unwrap();
    assert!(cols.is_empty());
}

#[test]
fn decode_columns_all_matches_full_decode() {
    let values = vec![
        Value::Integer(-100),
        Value::Real(3.15),
        Value::Text("hello world".into()),
        Value::Blob(vec![0xDE, 0xAD]),
        Value::Boolean(false),
        Value::Null,
    ];
    let encoded = encode_row(&values);
    let full = decode_row(&encoded).unwrap();
    let selective = decode_columns(&encoded, &[0, 1, 2, 3, 4, 5]).unwrap();
    assert_eq!(full, selective);
}

#[test]
fn raw_column_integer() {
    let values = vec![Value::Integer(42), Value::Text("hello".into())];
    let encoded = encode_row(&values);
    let raw = decode_column_raw(&encoded, 0).unwrap();
    assert!(matches!(raw, RawColumn::Integer(42)));
    assert_eq!(raw.to_value(), Value::Integer(42));
}

#[test]
fn raw_column_text_borrows() {
    let values = vec![Value::Integer(1), Value::Text("hello".into())];
    let encoded = encode_row(&values);
    let raw = decode_column_raw(&encoded, 1).unwrap();
    match raw {
        RawColumn::Text(s) => assert_eq!(s, "hello"),
        other => panic!("expected Text, got {other:?}"),
    }
}

#[test]
fn raw_column_null() {
    let values = vec![Value::Integer(1), Value::Null, Value::Boolean(true)];
    let encoded = encode_row(&values);
    let raw = decode_column_raw(&encoded, 1).unwrap();
    assert!(matches!(raw, RawColumn::Null));
}

#[test]
fn raw_column_last() {
    let values = vec![
        Value::Integer(1),
        Value::Text("skip".into()),
        Value::Real(3.15),
    ];
    let encoded = encode_row(&values);
    let raw = decode_column_raw(&encoded, 2).unwrap();
    match raw {
        RawColumn::Real(r) => assert!((r - 3.15).abs() < 1e-10),
        other => panic!("expected Real, got {other:?}"),
    }
}

#[test]
fn raw_column_out_of_bounds_returns_null() {
    let values = vec![Value::Integer(1)];
    let encoded = encode_row(&values);
    assert!(matches!(
        decode_column_raw(&encoded, 1).unwrap(),
        RawColumn::Null
    ));
}

#[test]
fn raw_column_eq_value() {
    let raw_int = RawColumn::Integer(42);
    assert!(raw_int.eq_value(&Value::Integer(42)));
    assert!(!raw_int.eq_value(&Value::Integer(43)));
    assert!(raw_int.eq_value(&Value::Real(42.0)));

    let raw_text = RawColumn::Text("hello");
    assert!(raw_text.eq_value(&Value::Text("hello".into())));
    assert!(!raw_text.eq_value(&Value::Text("world".into())));
}

#[test]
fn raw_column_cmp_value() {
    use std::cmp::Ordering;
    let raw = RawColumn::Integer(42);
    assert_eq!(raw.cmp_value(&Value::Integer(42)), Some(Ordering::Equal));
    assert_eq!(raw.cmp_value(&Value::Integer(50)), Some(Ordering::Less));
    assert_eq!(raw.cmp_value(&Value::Integer(10)), Some(Ordering::Greater));
    assert_eq!(raw.cmp_value(&Value::Null), None);
}

#[test]
fn raw_column_as_numeric() {
    assert_eq!(RawColumn::Integer(42).as_i64(), Some(42));
    assert_eq!(RawColumn::Integer(42).as_f64(), Some(42.0));
    assert_eq!(RawColumn::Real(3.15).as_f64(), Some(3.15));
    assert_eq!(RawColumn::Real(3.15).as_i64(), None);
    assert_eq!(RawColumn::Text("x").as_f64(), None);
    assert_eq!(RawColumn::Null.as_i64(), None);
}

#[test]
fn decode_pk_integer_roundtrip() {
    for v in [0i64, 1, -1, 42, -1000, i64::MIN, i64::MAX] {
        let encoded = encode_key_value(&Value::Integer(v));
        let decoded = decode_pk_integer(&encoded).unwrap();
        assert_eq!(decoded, v);
    }
}

#[test]
fn decode_pk_integer_rejects_non_integer() {
    let encoded = encode_key_value(&Value::Text("hello".into()));
    assert!(decode_pk_integer(&encoded).is_err());
}

#[test]
fn raw_column_blob() {
    let values = vec![Value::Blob(vec![0xDE, 0xAD])];
    let encoded = encode_row(&values);
    let raw = decode_column_raw(&encoded, 0).unwrap();
    match raw {
        RawColumn::Blob(b) => assert_eq!(b, &[0xDE, 0xAD]),
        other => panic!("expected Blob, got {other:?}"),
    }
}

#[test]
fn raw_column_matches_full_decode() {
    let values = vec![
        Value::Integer(-100),
        Value::Real(3.15),
        Value::Text("hello world".into()),
        Value::Blob(vec![0xDE, 0xAD]),
        Value::Boolean(false),
        Value::Null,
    ];
    let encoded = encode_row(&values);
    let full = decode_row(&encoded).unwrap();
    for (i, expected) in full.iter().enumerate() {
        let raw = decode_column_raw(&encoded, i).unwrap();
        assert_eq!(raw.to_value(), *expected, "mismatch at column {i}");
    }
}
