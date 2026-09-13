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

#[test]
fn int_row_template_matches_generic_encoder() {
    let mut state: u64 = 0x00C1_ADE7_BABE_u64.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state as i64
    };

    for trial in 0..2_000 {
        let phys_count = (trial % 6) + 1;
        let virtual_mask = trial % (1 << phys_count);

        let mut null_slots: Vec<usize> = Vec::new();
        let mut values: Vec<Value> = Vec::with_capacity(phys_count);
        for slot in 0..phys_count {
            if (virtual_mask >> slot) & 1 == 1 {
                null_slots.push(slot);
                values.push(Value::Null);
            } else {
                values.push(Value::Integer(next()));
            }
        }

        let mut generic = Vec::new();
        encode_row_into(&values, &mut generic);

        let slots: Vec<TemplateSlot> = (0..phys_count)
            .map(|s| {
                if null_slots.contains(&s) {
                    TemplateSlot::Null
                } else {
                    TemplateSlot::IntHole
                }
            })
            .collect();
        let tmpl = build_row_template(phys_count, &slots);
        let mut specialized = Vec::new();
        encode_row_with_template(&tmpl, &values, &mut specialized).unwrap();

        assert_eq!(
            specialized, generic,
            "trial {trial}: phys_count={phys_count} null_slots={null_slots:?} \
             values={values:?}",
        );
    }
}

fn arr(elems: Vec<Value>) -> Value {
    Value::Array(std::sync::Arc::new(elems))
}

#[test]
fn key_array_empty_roundtrip() {
    let v = arr(vec![]);
    let encoded = encode_key_value(&v);
    let (decoded, n) = decode_key_value(&encoded).unwrap();
    assert_eq!(decoded, v);
    assert_eq!(n, encoded.len());
}

#[test]
fn key_array_int_roundtrip() {
    let v = arr(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    let encoded = encode_key_value(&v);
    let (decoded, _) = decode_key_value(&encoded).unwrap();
    assert_eq!(decoded, v);
}

#[test]
fn key_array_mixed_roundtrip() {
    let v = arr(vec![
        Value::Integer(42),
        Value::Text("hello".into()),
        Value::Null,
        Value::Real(3.25),
        Value::Boolean(true),
    ]);
    let encoded = encode_key_value(&v);
    let (decoded, _) = decode_key_value(&encoded).unwrap();
    assert_eq!(decoded, v);
}

#[test]
fn key_array_nested_roundtrip() {
    let v = arr(vec![
        arr(vec![Value::Integer(1), Value::Integer(2)]),
        arr(vec![Value::Integer(3), Value::Integer(4)]),
        arr(vec![]),
    ]);
    let encoded = encode_key_value(&v);
    let (decoded, _) = decode_key_value(&encoded).unwrap();
    assert_eq!(decoded, v);
}

#[test]
fn key_array_sort_order_lexicographic() {
    let a = arr(vec![Value::Integer(1)]);
    let b = arr(vec![Value::Integer(2)]);
    let c = arr(vec![Value::Integer(1), Value::Integer(2)]);
    let ea = encode_key_value(&a);
    let eb = encode_key_value(&b);
    let ec = encode_key_value(&c);
    assert!(ea < eb, "[1] < [2] in lex order");
    assert!(ea < ec, "[1] < [1, 2] (shorter sorts first)");
    assert!(ec < eb, "[1, 2] < [2]");
}

#[test]
fn row_array_int_roundtrip() {
    let values = vec![
        Value::Integer(1),
        arr(vec![
            Value::Integer(10),
            Value::Integer(20),
            Value::Integer(30),
        ]),
        Value::Text("after".into()),
    ];
    let encoded = encode_row(&values);
    let decoded = decode_row(&encoded).unwrap();
    assert_eq!(decoded, values);
}

#[test]
fn row_array_mixed_roundtrip() {
    let values = vec![arr(vec![
        Value::Integer(1),
        Value::Null,
        Value::Text("two".into()),
        Value::Boolean(false),
        Value::Real(3.5),
    ])];
    let encoded = encode_row(&values);
    let decoded = decode_row(&encoded).unwrap();
    assert_eq!(decoded, values);
}

#[test]
fn row_array_nested_roundtrip() {
    let values = vec![arr(vec![
        arr(vec![Value::Integer(1), Value::Integer(2)]),
        arr(vec![Value::Text("a".into()), Value::Text("b".into())]),
        Value::Null,
    ])];
    let encoded = encode_row(&values);
    let decoded = decode_row(&encoded).unwrap();
    assert_eq!(decoded, values);
}

#[test]
fn row_array_empty_roundtrip() {
    let values = vec![arr(vec![])];
    let encoded = encode_row(&values);
    let decoded = decode_row(&encoded).unwrap();
    assert_eq!(decoded, values);
}

#[test]
fn patch_array_same_size_in_place_succeeds() {
    let original = arr(vec![Value::Integer(1), Value::Integer(2)]);
    let replacement = arr(vec![Value::Integer(99), Value::Integer(100)]);
    let mut encoded = encode_row(&[original]);
    let ok = patch_column_in_place(&mut encoded, 0, &replacement).unwrap();
    assert!(ok, "same-size array patch must succeed in place");
    let decoded = decode_row(&encoded).unwrap();
    assert_eq!(decoded[0], replacement);
}

#[test]
fn patch_array_different_size_returns_false() {
    let original = arr(vec![Value::Integer(1), Value::Integer(2)]);
    let replacement = arr(vec![Value::Integer(1)]);
    let mut encoded = encode_row(&[original]);
    let ok = patch_column_in_place(&mut encoded, 0, &replacement).unwrap();
    assert!(!ok, "different-size array patch must report size mismatch");
}

#[test]
fn patch_row_column_initializes_missing_bitmap_slots() {
    for (stored_count, target) in [(1, 2), (6, 8), (0, 2)] {
        let values: Vec<Value> = (0..stored_count)
            .map(|column| {
                if column == 1 {
                    Value::Null
                } else {
                    Value::Integer(column as i64 + 10)
                }
            })
            .collect();
        let encoded = encode_row(&values);
        for replacement in [Value::Integer(99), Value::Null] {
            let mut patched = Vec::new();
            patch_row_column(&encoded, target, &replacement, &mut patched).unwrap();
            let mut expected = values.clone();
            expected.resize(target + 1, Value::Null);
            expected[target] = replacement;
            assert_eq!(
                decode_row(&patched).unwrap(),
                expected,
                "stored count: {stored_count}; target: {target}"
            );
        }
    }
}

fn assert_v2_framing_change_rejected(at_offset: bool) {
    for (original, replacement) in [
        (Value::Text("12345678".into()), Value::Integer(9)),
        (Value::Integer(9), Value::Text("12345678".into())),
    ] {
        let mut values = vec![Value::Null, original, Value::Integer(77)];
        let mut encoded = encode_row(&values);
        let before = encoded.clone();
        let patched = if at_offset {
            let (_, offset) = decode_column_with_offset(&encoded, 1).unwrap();
            patch_at_offset(&mut encoded, offset, &replacement).unwrap()
        } else {
            patch_column_in_place(&mut encoded, 1, &replacement).unwrap()
        };
        assert!(
            !patched,
            "equal payload sizes cannot change V2 length framing: {:?} -> {replacement:?}",
            values[1]
        );
        assert_eq!(encoded, before, "a rejected patch must not change the row");

        let mut rewritten = Vec::new();
        patch_row_column(&encoded, 1, &replacement, &mut rewritten).unwrap();
        values[1] = replacement;
        assert_eq!(decode_row(&rewritten).unwrap(), values);
        assert_eq!(rewritten, encode_row(&values));
    }
}

#[test]
fn patch_column_in_place_rejects_v2_framing_changes() {
    assert_v2_framing_change_rejected(false);
}

#[test]
fn patch_at_offset_rejects_v2_framing_changes() {
    assert_v2_framing_change_rejected(true);
}

#[test]
fn in_place_patches_preserve_v1_framing() {
    for (original, replacement) in [
        (Value::Text("12345678".into()), Value::Integer(9)),
        (Value::Integer(9), Value::Text("12345678".into())),
    ] {
        // V1 retains the length field for both fixed and variable-width cells.
        let mut encoded = vec![2, 0, 0, original.data_type().type_tag()];
        encoded.extend_from_slice(&8u32.to_le_bytes());
        match &original {
            Value::Text(text) => encoded.extend_from_slice(text.as_bytes()),
            Value::Integer(value) => encoded.extend_from_slice(&value.to_le_bytes()),
            _ => unreachable!(),
        }
        encoded.push(DataType::Boolean.type_tag());
        encoded.extend_from_slice(&1u32.to_le_bytes());
        encoded.push(1);
        assert_eq!(
            decode_row(&encoded).unwrap(),
            vec![original, Value::Boolean(true)]
        );

        for at_offset in [false, true] {
            let mut patched = encoded.clone();
            let applied = if at_offset {
                let (_, offset) = decode_column_with_offset(&patched, 0).unwrap();
                patch_at_offset(&mut patched, offset, &replacement).unwrap()
            } else {
                patch_column_in_place(&mut patched, 0, &replacement).unwrap()
            };
            assert!(applied);
            assert_eq!(patched.len(), encoded.len());
            assert_eq!(
                decode_row(&patched).unwrap(),
                vec![replacement.clone(), Value::Boolean(true)]
            );
        }
    }
}

#[test]
fn in_place_patches_allow_equal_width_fixed_types() {
    for (original, replacement) in [
        (Value::Integer(42), Value::Real(3.5)),
        (Value::Real(3.5), Value::Timestamp(42)),
        (Value::Timestamp(42), Value::Time(56)),
        (Value::Date(1), Value::Date(2)),
        (Value::Boolean(false), Value::Boolean(true)),
    ] {
        let encoded = encode_row(&[original, Value::Integer(77)]);
        for at_offset in [false, true] {
            let mut patched = encoded.clone();
            let applied = if at_offset {
                let (_, offset) = decode_column_with_offset(&patched, 0).unwrap();
                patch_at_offset(&mut patched, offset, &replacement).unwrap()
            } else {
                patch_column_in_place(&mut patched, 0, &replacement).unwrap()
            };
            assert!(applied);
            assert_eq!(patched.len(), encoded.len());
            assert_eq!(
                decode_row(&patched).unwrap(),
                vec![replacement.clone(), Value::Integer(77)]
            );
        }
    }
}

fn assert_truncated_patch_payload_is_rejected(at_offset: bool) {
    for version in [RowVersion::V1, RowVersion::V2] {
        for value in [Value::Integer(42), Value::Text("12345678".into())] {
            let mut encoded = encode_row(std::slice::from_ref(&value));
            if version == RowVersion::V1 {
                encoded[..2].copy_from_slice(&1u16.to_le_bytes());
                if matches!(value, Value::Integer(_)) {
                    encoded.splice(4..4, 8u32.to_le_bytes());
                }
            }
            let (_, offset) = decode_column_with_offset(&encoded, 0).unwrap();
            encoded.pop();
            let before = encoded.clone();
            let replacement = match &value {
                Value::Integer(_) => Value::Real(3.5),
                Value::Text(_) => Value::Blob(vec![9; 8]),
                _ => unreachable!(),
            };
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if at_offset {
                    patch_at_offset(&mut encoded, offset, &replacement)
                } else {
                    patch_column_in_place(&mut encoded, 0, &replacement)
                }
            }));
            assert!(
                matches!(outcome, Ok(Err(SqlError::InvalidValue(_)))),
                "{version:?} truncated {value:?} must return an error: {outcome:?}"
            );
            assert_eq!(encoded, before, "an invalid row must remain unchanged");
        }
    }
}

#[test]
fn patch_column_in_place_rejects_truncated_payloads_without_mutation() {
    assert_truncated_patch_payload_is_rejected(false);
}

#[test]
fn patch_at_offset_rejects_truncated_payloads_without_mutation() {
    assert_truncated_patch_payload_is_rejected(true);
}

fn row_for_layout_patch_test(values: &[Value], version: RowVersion) -> Vec<u8> {
    let encoded = encode_row(values);
    if version == RowVersion::V2 {
        return encoded;
    }
    let (_, count, bitmap, mut pos) = parse_row_header(&encoded).unwrap();
    let mut legacy = (count as u16).to_le_bytes().to_vec();
    legacy.extend_from_slice(bitmap);
    for value in values {
        if value.is_null() {
            continue;
        }
        let (tag, body, next) = read_cell(&encoded, pos, RowVersion::V2).unwrap();
        legacy.push(tag);
        legacy.extend_from_slice(&(body.len() as u32).to_le_bytes());
        legacy.extend_from_slice(body);
        pos = next;
    }
    legacy
}

#[test]
fn row_layout_patches_match_public_paths_across_successive_type_changes() {
    for version in [RowVersion::V1, RowVersion::V2] {
        for (original, replacements) in [
            (
                Value::Integer(42),
                vec![
                    (Value::Real(-0.0), [true, true]),
                    (Value::Timestamp(52), [true, true]),
                    (Value::Time(56), [true, true]),
                    (Value::Text("12345678".into()), [true, false]),
                    (Value::Blob(vec![9; 8]), [true, false]),
                    (Value::Integer(-1), [true, true]),
                    (Value::Null, [false, false]),
                    (Value::Date(2), [false, false]),
                ],
            ),
            (
                Value::Text("éééé".into()),
                vec![
                    (Value::Blob(vec![7; 8]), [true, true]),
                    (Value::Json("\"123456\"".into()), [true, true]),
                    (Value::Integer(9), [true, false]),
                    (Value::Text("ññññ".into()), [true, true]),
                    (Value::Text("longer replacement".into()), [false, false]),
                    (Value::Null, [false, false]),
                ],
            ),
            (
                arr(vec![
                    Value::Integer(1),
                    Value::Null,
                    Value::Text("é".into()),
                ]),
                vec![
                    (
                        arr(vec![
                            Value::Integer(9),
                            Value::Null,
                            Value::Text("ñ".into()),
                        ]),
                        [true, true],
                    ),
                    (arr(vec![Value::Integer(3)]), [false, false]),
                    (Value::Null, [false, false]),
                ],
            ),
            (
                Value::Vector(vec![1.0, 2.0].into()),
                vec![
                    (Value::Vector(vec![-0.0, 4.0].into()), [true, true]),
                    (Value::Vector(vec![5.0].into()), [false, false]),
                    (Value::Null, [false, false]),
                ],
            ),
        ] {
            let mut expected = vec![
                Value::Null,
                original,
                Value::Boolean(true),
                Value::Text("neighbor".into()),
            ];
            let mut located = row_for_layout_patch_test(&expected, version);
            let mut by_column = located.clone();
            let mut by_offset = located.clone();
            let mut layout = RowLayout::default();
            // Prime all locations before any mutation, including the suffix.
            assert_eq!(layout.column(&located, 3).unwrap().to_value(), expected[3]);
            for (replacement, admitted) in replacements {
                let before = located.clone();
                let (_, offset) = decode_column_with_offset(&by_offset, 1).unwrap();
                let applied = layout.patch(&mut located, 1, &replacement).unwrap();
                assert_eq!(applied, admitted[usize::from(version == RowVersion::V2)]);
                assert_eq!(
                    applied,
                    patch_column_in_place(&mut by_column, 1, &replacement).unwrap()
                );
                assert_eq!(
                    applied,
                    patch_at_offset(&mut by_offset, offset, &replacement).unwrap()
                );
                assert_eq!(located, by_column);
                assert_eq!(located, by_offset);
                if applied {
                    expected[1] = replacement;
                } else {
                    assert_eq!(located, before, "declined patches must not mutate bytes");
                }
                let decoded = decode_row(&located).unwrap();
                assert_eq!(decoded.len(), expected.len());
                assert!(decoded.iter().zip(&expected).all(|(a, b)| a.bit_eq(b)));
                assert!(layout
                    .column(&located, 1)
                    .unwrap()
                    .to_value()
                    .bit_eq(&expected[1]));
                assert_eq!(layout.column(&located, 3).unwrap().to_value(), expected[3]);
            }
            for target in [0, expected.len()] {
                let before = located.clone();
                assert!(!layout
                    .patch(&mut located, target, &Value::Integer(9))
                    .unwrap());
                assert!(!patch_column_in_place(&mut located, target, &Value::Integer(9)).unwrap());
                assert_eq!(
                    located, before,
                    "stored NULL and absent cells need a rebuild"
                );
            }
        }
    }
}

#[test]
fn row_layout_patch_preserves_truncation_and_null_admission_errors() {
    for version in [RowVersion::V1, RowVersion::V2] {
        for value in [Value::Integer(42), Value::Text("12345678".into())] {
            let mut encoded = row_for_layout_patch_test(std::slice::from_ref(&value), version);
            let (_, offset) = decode_column_with_offset(&encoded, 0).unwrap();
            encoded.pop();
            for path in 0..3 {
                let mut bytes = encoded.clone();
                let outcome = match path {
                    0 => RowLayout::default().patch(&mut bytes, 0, &value),
                    1 => patch_column_in_place(&mut bytes, 0, &value),
                    _ => patch_at_offset(&mut bytes, offset, &value),
                };
                let Err(SqlError::InvalidValue(message)) = outcome else {
                    panic!("truncated {version:?} row must fail: {outcome:?}");
                };
                // These existing checked parsers intentionally have distinct
                // diagnostics; sharing the writer must not change either one.
                assert_eq!(
                    message,
                    if path == 0 {
                        "truncated column value"
                    } else {
                        "truncated column data"
                    }
                );
                assert_eq!(bytes, encoded);
            }
        }
    }
    let mut empty = Vec::new();
    assert!(!RowLayout::default()
        .patch(&mut empty, 0, &Value::Null)
        .unwrap());
    assert!(!patch_at_offset(&mut empty, 0, &Value::Null).unwrap());
    assert!(!patch_at_offset(&mut empty, usize::MAX, &Value::Integer(9)).unwrap());
    assert!(matches!(
        patch_column_in_place(&mut empty, 0, &Value::Null),
        Err(SqlError::InvalidValue(message)) if message == "row data too short"
    ));
    assert!(matches!(
        patch_at_offset(&mut empty, 0, &Value::Integer(9)),
        Err(SqlError::InvalidValue(message)) if message == "truncated column data"
    ));
    assert!(empty.is_empty());
}

#[test]
fn raw_column_array_decodes() {
    let v = arr(vec![Value::Integer(7), Value::Text("x".into())]);
    let encoded = encode_row(std::slice::from_ref(&v));
    let raw = decode_column_raw(&encoded, 0).unwrap();
    assert!(matches!(raw, RawColumn::Array(_)));
    assert_eq!(raw.to_value(), v);
}

#[test]
fn vector_key_round_trips() {
    let mut buf = Vec::new();
    let v = Value::Vector(std::sync::Arc::from(vec![1.5f32, -2.25, 0.0]));
    encode_key_value_into(&v, &mut buf);
    let (decoded, n) = decode_key_value(&buf).unwrap();
    assert_eq!(n, buf.len());
    match decoded {
        Value::Vector(d) => assert_eq!(&d[..], &[1.5f32, -2.25, 0.0]),
        other => panic!("expected vector, got {other:?}"),
    }
}

#[test]
fn composite_key_with_vector_component() {
    let mut buf = Vec::new();
    encode_key_value_into(&Value::Vector(std::sync::Arc::from(vec![3.0f32])), &mut buf);
    encode_key_value_into(&Value::Integer(42), &mut buf);
    let vals = decode_composite_key(&buf, 2).unwrap();
    assert_eq!(vals[1], Value::Integer(42));
}

#[test]
fn fixed_width_v1_cells_require_exact_payload_lengths() {
    for (kind, width) in [
        (DataType::Integer, 8),
        (DataType::Real, 8),
        (DataType::Boolean, 1),
        (DataType::Date, 4),
        (DataType::Time, 8),
        (DataType::Timestamp, 8),
        (DataType::Interval, 16),
    ] {
        for len in [0, width - 1, width, width + 1] {
            let mut data = vec![1, 0, 0, kind.type_tag()];
            data.extend_from_slice(&(len as u32).to_le_bytes());
            data.resize(data.len() + len, 0);
            for outcome in [
                std::panic::catch_unwind(|| decode_row(&data).map(|_| ())),
                std::panic::catch_unwind(|| decode_column_raw(&data, 0).map(|_| ())),
                std::panic::catch_unwind(|| decode_column_with_offset(&data, 0).map(|_| ())),
            ] {
                if len == width {
                    assert!(matches!(outcome, Ok(Ok(()))), "{kind:?}: {outcome:?}");
                    continue;
                }
                assert!(
                    matches!(outcome, Ok(Err(SqlError::InvalidValue(_)))),
                    "{kind:?} payload of {len} bytes must return an error: {outcome:?}"
                );
            }
        }
    }
}

#[test]
fn skipped_cell_bodies_are_checked_before_a_null_target() {
    for version in [0, V2_FLAG] {
        let mut data = (2u16 | version).to_le_bytes().to_vec();
        data.extend_from_slice(&[0b10, DataType::Text.type_tag()]);
        data.extend_from_slice(&u32::MAX.to_le_bytes());
        for outcome in [
            std::panic::catch_unwind(|| decode_columns(&data, &[1]).map(|_| ())),
            std::panic::catch_unwind(|| decode_column_raw(&data, 1).map(|_| ())),
            std::panic::catch_unwind(|| decode_column_with_offset(&data, 1).map(|_| ())),
        ] {
            assert!(
                matches!(outcome, Ok(Err(SqlError::InvalidValue(_)))),
                "a NULL target must not hide a truncated preceding cell: {outcome:?}"
            );
        }
    }
}

#[test]
fn key_decoders_reject_invalid_signed_width_markers() {
    for tag in [TAG_INTEGER, TAG_TIME, TAG_DATE, TAG_TIMESTAMP] {
        for marker in 0..=u8::MAX {
            if (0x78..=0x88).contains(&marker) {
                continue;
            }
            // Satisfy even the largest bogus width, so truncation cannot hide
            // indexing outside the signed decoder's eight-byte destination.
            let mut encoded = vec![0; 130];
            encoded[0] = tag;
            encoded[1] = marker;
            assert!(matches!(
                decode_key_value(&encoded),
                Err(SqlError::InvalidValue(_))
            ));
            assert!(matches!(
                skip_key_value(&encoded),
                Err(SqlError::InvalidValue(_))
            ));
        }
    }
}

#[test]
fn key_skip_rejects_truncated_fixed_width_values() {
    for value in [
        Value::Boolean(true),
        Value::Interval {
            months: 1,
            days: -2,
            micros: 3,
        },
    ] {
        let encoded = encode_key_value(&value);
        for length in 0..encoded.len() {
            assert!(
                matches!(
                    skip_key_value(&encoded[..length]),
                    Err(SqlError::InvalidValue(_))
                ),
                "value: {value:?}; available bytes: {length}"
            );
        }
        assert_eq!(skip_key_value(&encoded).unwrap(), encoded.len());
    }
}

#[test]
fn key_decoders_reject_out_of_range_signed_magnitudes() {
    for tag in [TAG_INTEGER, TAG_TIME, TAG_DATE, TAG_TIMESTAMP] {
        for (marker, magnitudes) in [
            (0x88, [1u64 << 63, u64::MAX]),
            (0x78, [(1u64 << 63) + 1, u64::MAX]),
        ] {
            for magnitude in magnitudes {
                let mut encoded = vec![tag, marker];
                let payload = magnitude.to_be_bytes();
                if marker == 0x78 {
                    encoded.extend(payload.map(|byte| !byte));
                } else {
                    encoded.extend(payload);
                }
                assert!(
                    matches!(decode_key_value(&encoded), Err(SqlError::InvalidValue(_))),
                    "tag: {tag}; marker: {marker}; magnitude: {magnitude}"
                );
                assert!(matches!(
                    skip_key_value(&encoded),
                    Err(SqlError::InvalidValue(_))
                ));
            }
        }
    }
}

#[test]
fn key_skip_rejects_unknown_type_tags() {
    for tag in TAG_VECTOR + 1..=u8::MAX {
        // A terminator must not turn an unrecognized type into a byte string.
        let encoded = [tag, 0];
        assert!(matches!(
            decode_key_value(&encoded),
            Err(SqlError::InvalidValue(_))
        ));
        assert!(matches!(
            skip_key_value(&encoded),
            Err(SqlError::InvalidValue(_))
        ));
    }
}
