use super::*;

fn roundtrip(input: &str) {
    let v: serde_json::Value = serde_json::from_str(input).unwrap();
    let mut buf = Vec::new();
    encode_canonical(&v, &mut buf).unwrap();
    let back = decode_to_serde(&buf).unwrap();
    let canonical = serde_json::to_string(&v).unwrap();
    let decoded = serde_json::to_string(&back).unwrap();
    assert_eq!(reorder(&canonical), reorder(&decoded), "input: {input}");
}

fn reorder(s: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(s).unwrap();
    serde_json::to_string(&canonicalize(v)).unwrap()
}

fn canonicalize(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => {
            let mut sorted: Vec<(String, serde_json::Value)> = m.into_iter().collect();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            let map: serde_json::Map<String, serde_json::Value> = sorted
                .into_iter()
                .map(|(k, v)| (k, canonicalize(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(canonicalize).collect())
        }
        other => other,
    }
}

#[test]
fn null_true_false() {
    roundtrip("null");
    roundtrip("true");
    roundtrip("false");
}

#[test]
fn integers() {
    roundtrip("0");
    roundtrip("42");
    roundtrip("-100");
    roundtrip("9223372036854775807");
    roundtrip("-9223372036854775808");
}

#[test]
fn strings() {
    roundtrip(r#""hello""#);
    roundtrip(r#""""#);
    roundtrip(r#""with \"quotes\" and \\ slash""#);
}

#[test]
fn arrays() {
    roundtrip("[]");
    roundtrip("[1, 2, 3]");
    roundtrip(r#"[null, true, false, "x", 1, 2.5]"#);
}

#[test]
fn objects() {
    roundtrip(r#"{}"#);
    roundtrip(r#"{"a": 1, "b": 2}"#);
    roundtrip(r#"{"z": [1, 2, {"x": "y"}], "a": null}"#);
}

#[test]
fn nested() {
    roundtrip(r#"{"a": {"b": {"c": [1, [2, [3, [4]]]]}}}"#);
}

#[test]
fn canonical_key_sort() {
    let v: serde_json::Value = serde_json::from_str(r#"{"z": 1, "a": 2, "m": 3}"#).unwrap();
    let mut buf1 = Vec::new();
    encode_canonical(&v, &mut buf1).unwrap();
    let v2: serde_json::Value = serde_json::from_str(r#"{"m": 3, "a": 2, "z": 1}"#).unwrap();
    let mut buf2 = Vec::new();
    encode_canonical(&v2, &mut buf2).unwrap();
    assert_eq!(buf1, buf2);
}

#[test]
fn large_string() {
    let big = "x".repeat(300);
    roundtrip(&format!(r#""{big}""#));
}

#[test]
fn very_large_string() {
    let big = "y".repeat(70_000);
    roundtrip(&format!(r#""{big}""#));
}

#[test]
fn extract_gin_entries_object_basic() {
    let v = text_to_jsonb(r#"{"role":"admin","city":"NYC"}"#).unwrap();
    let entries = extract_gin_entries(&v, crate::types::GinOpsClass::JsonbOps).unwrap();
    let has_key_entry = entries.iter().any(|e| e.starts_with(&[0x01]));
    let has_pair_entry = entries.iter().any(|e| e.starts_with(&[0x02]));
    assert!(has_key_entry, "expected at least one key entry");
    assert!(has_pair_entry, "expected at least one pair entry");
}

#[test]
fn extract_gin_entries_null_is_empty() {
    let entries = extract_gin_entries(&Value::Null, crate::types::GinOpsClass::JsonbOps).unwrap();
    assert!(entries.is_empty());
}

#[test]
fn jsonb_contains_bytes_top_pair_match() {
    let big = text_to_jsonb(r#"{"role":"admin","city":"NYC"}"#).unwrap();
    let probe = text_to_jsonb(r#"{"role":"admin"}"#).unwrap();
    let big_b = match &big {
        Value::Jsonb(b) => b.clone(),
        _ => panic!(),
    };
    let probe_b = match &probe {
        Value::Jsonb(b) => b.clone(),
        _ => panic!(),
    };
    assert!(jsonb_contains_bytes(&big_b, &probe_b).unwrap());
}

#[test]
fn jsonb_contains_bytes_missing_key_returns_false() {
    let big = text_to_jsonb(r#"{"role":"admin"}"#).unwrap();
    let probe = text_to_jsonb(r#"{"role":"member"}"#).unwrap();
    let big_b = match &big {
        Value::Jsonb(b) => b.clone(),
        _ => panic!(),
    };
    let probe_b = match &probe {
        Value::Jsonb(b) => b.clone(),
        _ => panic!(),
    };
    assert!(!jsonb_contains_bytes(&big_b, &probe_b).unwrap());
}

#[test]
fn containment_preserves_container_structure_on_all_paths() {
    let cases = [
        ("[1,2,3]", "[3,1,1]", true),
        ("[1,2,3]", "[]", true),
        ("[]", "[1]", false),
        ("[[1,2]]", "[1]", false),
        ("[1,2,[1,3]]", "[1,3]", false),
        ("[1,2,[1,3]]", "[[1,3]]", true),
        ("[[1,2]]", "[[2]]", true),
        ("[[[1]]]", "[[1]]", false),
        ("[[1]]", "[[[1]]]", false),
        ("[[1]]", "[[]]", true),
        ("[1]", "[[]]", false),
        (r#"[{"a":1}]"#, r#"{"a":1}"#, false),
        (r#"[{"a":1,"b":2}]"#, r#"{"a":1}"#, false),
        (r#"[{"a":1,"b":2}]"#, r#"[{"a":1}]"#, true),
        (r#"[{"a":1},{"b":2}]"#, r#"[{"a":1,"b":2}]"#, false),
        (r#"[[{"a":1}]]"#, r#"[{"a":1}]"#, false),
        (r#"[{"a":1}]"#, r#"[[{"a":1}]]"#, false),
        (r#"[{"a":1}]"#, "{}", false),
        (r#"[{"a":1}]"#, "[{}]", true),
        (r#"{"a":1}"#, "{}", true),
        (r#"{"a":1}"#, "[]", false),
        (r#"{"a":[1,2]}"#, r#"{"a":1}"#, false),
        (r#"{"a":[1,2]}"#, r#"{"a":[1]}"#, true),
        (r#"{"a":[{"b":1}]}"#, r#"{"a":{"b":1}}"#, false),
        (r#"{"a":[{"b":1}]}"#, r#"{"a":[{"b":1}]}"#, true),
        (r#"{"foo":{"bar":"baz"}}"#, r#"{"bar":"baz"}"#, false),
        (r#"{"foo":{"bar":"baz"}}"#, r#"{"foo":{}}"#, true),
        (r#"["foo","bar"]"#, r#""bar""#, true),
        (r#""bar""#, r#"["bar"]"#, false),
        (r#"[["bar"]]"#, r#""bar""#, false),
        ("[1,2]", "1", true),
        ("[[1,2]]", "1", false),
        ("[true,false]", "true", true),
        ("[[true]]", "true", false),
        ("[null]", "null", true),
        ("[[null]]", "null", false),
        ("null", "null", true),
        ("1", "1", true),
        ("1.5", "1.5", true),
        ("1", r#""1""#, false),
    ];
    let token = CancelToken::new();
    let mut failures = Vec::new();
    for (left, right, expected) in cases {
        let left_json = Value::Json(left.into());
        let right_json = Value::Json(right.into());
        let left_jsonb = text_to_jsonb(left).unwrap();
        let right_jsonb = text_to_jsonb(right).unwrap();
        for (lhs, rhs) in [
            (&left_json, &right_json),
            (&left_jsonb, &right_jsonb),
            (&left_json, &right_jsonb),
            (&left_jsonb, &right_json),
        ] {
            for cancel in [None, Some(&token)] {
                for (operator, actual) in [
                    ("@>", op_contains_with_cancel(lhs, rhs, cancel).unwrap()),
                    ("<@", op_contained_by_with_cancel(rhs, lhs, cancel).unwrap()),
                ] {
                    if actual != Value::Boolean(expected) {
                        failures.push(format!(
                            "{left} @> {right}: expected {expected}, got {actual:?} \
                             ({operator}, lhs_jsonb={}, rhs_jsonb={}, cancel={})",
                            matches!(lhs, Value::Jsonb(_)),
                            matches!(rhs, Value::Jsonb(_)),
                            cancel.is_some(),
                        ));
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    assert!(!token.is_cancelled());
}

#[test]
fn containment_array_scan_remains_cancellable() {
    let left = serde_json::json!(vec!["a"; 1024]);
    let right = serde_json::json!("missing");
    let mut left_bytes = Vec::new();
    let mut right_bytes = Vec::new();
    encode_canonical(&left, &mut left_bytes).unwrap();
    encode_canonical(&right, &mut right_bytes).unwrap();
    for encoded in [false, true] {
        let token = CancelToken::new();
        let _guard = cancel_json_after(token.clone(), 32);
        let result = if encoded {
            jsonb_contains_bytes_with_cancel(&left_bytes, &right_bytes, Some(&token))
        } else {
            run_json_work(Some(&token), |work| {
                json_contains_with_work(&left, &right, work)
            })
        };
        assert_interrupted(result);
        assert!(token.is_cancelled());
    }
}

#[test]
fn find_object_key_streaming_returns_slice() {
    let v = text_to_jsonb(r#"{"role":"admin","name":"alice"}"#).unwrap();
    let bytes = match &v {
        Value::Jsonb(b) => b.clone(),
        _ => panic!(),
    };
    let slice = find_object_key(&bytes, "role").unwrap().unwrap();
    let (ty, _, _) = read_header(slice).unwrap();
    assert_eq!(ty, JsonbType::String);
}

#[test]
fn find_object_key_missing_returns_none() {
    let v = text_to_jsonb(r#"{"role":"admin"}"#).unwrap();
    let bytes = match &v {
        Value::Jsonb(b) => b.clone(),
        _ => panic!(),
    };
    assert!(find_object_key(&bytes, "missing").unwrap().is_none());
}

#[test]
fn parse_dollar_path_basic() {
    let segs = parse_dollar_path("$.foo.bar").unwrap();
    assert_eq!(segs.len(), 2);
    matches!(segs[0], PathSeg::Key(_));
    matches!(segs[1], PathSeg::Key(_));
}

#[test]
fn parse_dollar_path_array_index() {
    let segs = parse_dollar_path("$.items[3]").unwrap();
    assert_eq!(segs.len(), 2);
    matches!(segs[0], PathSeg::Key(_));
    matches!(segs[1], PathSeg::Index(3));
}

#[test]
fn parse_dollar_path_wildcard() {
    let segs = parse_dollar_path("$[*]").unwrap();
    assert_eq!(segs.len(), 1);
    matches!(segs[0], PathSeg::Wildcard);
}

fn assert_interrupted<T>(result: Result<T>) {
    assert!(
        matches!(
            result,
            Err(SqlError::Storage(citadel_core::Error::Interrupted))
        ),
        "expected an interrupted error"
    );
}

fn json_text(value: serde_json::Value) -> Value {
    Value::Json(serde_json::to_string(&value).unwrap().into())
}

#[test]
fn gin_extraction_can_cancel_after_traversal_starts() {
    let object = (0..512)
        .map(|i| (format!("key_{i}"), serde_json::Value::from(i)))
        .collect();
    let value = json_text(serde_json::Value::Object(object));
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 32);

    assert!(!token.is_cancelled());
    assert_interrupted(extract_gin_entries_with_cancel(
        &value,
        crate::types::GinOpsClass::JsonbOps,
        Some(&token),
    ));
    assert!(token.is_cancelled());
}

#[test]
fn gin_extraction_can_cancel_inside_one_large_scalar_copy() {
    let value = serde_json::Value::Array(vec![serde_json::Value::String("x".repeat(128 * 1024))]);
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 4);
    let mut work = JsonWork::new(Some(&token)).unwrap();
    let mut entries = Vec::new();

    assert!(!token.is_cancelled());
    assert_interrupted(extract_jsonb_ops_walk(&value, &mut entries, &mut work));
    assert!(token.is_cancelled());
}

#[test]
fn jsonb_decode_can_cancel_after_value_traversal_starts() {
    let value = json_text(serde_json::Value::Array(
        (0..512).map(serde_json::Value::from).collect(),
    ));
    let Value::Json(text) = value else {
        unreachable!();
    };
    let jsonb = text_to_jsonb(&text).unwrap();
    let Value::Jsonb(bytes) = jsonb else {
        unreachable!();
    };
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 32);

    assert!(!token.is_cancelled());
    assert_interrupted(decode_to_serde_with_cancel(&bytes, Some(&token)));
    assert!(token.is_cancelled());
}

#[test]
fn json_text_parse_can_cancel_after_bytes_are_consumed() {
    let value = Value::Json(
        serde_json::to_string(&(0..100_000).collect::<Vec<i64>>())
            .unwrap()
            .into(),
    );
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 4);

    assert!(!token.is_cancelled());
    assert_interrupted(value_to_serde_with_cancel(&value, Some(&token)));
    assert!(token.is_cancelled());
}

#[test]
fn json_table_can_cancel_during_row_materialization() {
    let source = json_text(serde_json::Value::Array(
        (0..512).map(serde_json::Value::from).collect(),
    ));
    let spec = crate::parser::JsonTableSpec {
        source: crate::parser::Expr::Literal(Value::Null),
        root_path: "$[*]".into(),
        columns: vec![crate::parser::JsonTableCol::Named {
            name: "value".into(),
            ty: crate::types::DataType::Integer,
            path: "$".into(),
            exists: false,
        }],
    };
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 32);

    assert!(!token.is_cancelled());
    assert_interrupted(materialize_json_table_with_cancel(
        &source,
        &spec,
        Some(&token),
    ));
    assert!(token.is_cancelled());
}

#[test]
fn json_srf_can_cancel_during_row_materialization() {
    let source = json_text(serde_json::Value::Array(
        (0..512).map(serde_json::Value::from).collect(),
    ));
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 32);

    assert!(!token.is_cancelled());
    assert_interrupted(dispatch_srf_with_cancel(
        "json_array_elements",
        &[source],
        Some(&token),
    ));
    assert!(token.is_cancelled());
}

#[test]
fn json_srf_can_cancel_inside_one_large_output_value() {
    let source =
        text_to_jsonb(&serde_json::to_string(&vec!["x".repeat(128 * 1024)]).unwrap()).unwrap();
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 24);

    assert!(!token.is_cancelled());
    assert_interrupted(dispatch_srf_with_cancel(
        "jsonb_array_elements",
        &[source],
        Some(&token),
    ));
    assert!(token.is_cancelled());
}

#[test]
fn populate_record_can_cancel_between_columns() {
    let object: serde_json::Map<String, serde_json::Value> = (0..64)
        .map(|i| (format!("col_{i}"), serde_json::Value::from(i)))
        .collect();
    let columns: Vec<crate::types::ColumnDef> = (0..64)
        .map(|i| crate::types::ColumnDef {
            name: format!("col_{i}"),
            data_type: crate::types::DataType::Integer,
            nullable: true,
            position: i,
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
        })
        .collect();
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 8);

    assert!(!token.is_cancelled());
    assert_interrupted(populate_record_row_with_cancel(
        &object,
        &columns,
        Some(&token),
    ));
    assert!(token.is_cancelled());
}

#[test]
fn untripped_token_preserves_json_binary_operator_results() {
    let token = CancelToken::new();
    let json = Value::Json(r#"{"a":[1,2,3],"flag":true,"name":"citadel"}"#.into());
    let jsonb = text_to_jsonb(r#"{"a":[1,2,3],"flag":true,"name":"citadel"}"#).unwrap();
    let probe = text_to_jsonb(r#"{"flag":true}"#).unwrap();
    let key = Value::Text("name".into());
    let path = Value::Text("{a,1}".into());
    let keys = Value::Json(r#"["missing","flag"]"#.into());
    let all_keys = Value::Json(r#"["name","flag"]"#.into());
    let json_path = Value::Text("$.flag".into());

    assert_eq!(
        op_get(&jsonb, &key).unwrap(),
        op_get_with_cancel(&jsonb, &key, Some(&token)).unwrap()
    );
    assert_eq!(
        op_get_text(&jsonb, &key).unwrap(),
        op_get_text_with_cancel(&jsonb, &key, Some(&token)).unwrap()
    );
    assert_eq!(
        op_path(&json, &path).unwrap(),
        op_path_with_cancel(&json, &path, Some(&token)).unwrap()
    );
    assert_eq!(
        op_path_text(&jsonb, &path).unwrap(),
        op_path_text_with_cancel(&jsonb, &path, Some(&token)).unwrap()
    );
    assert_eq!(
        op_contains(&jsonb, &probe).unwrap(),
        op_contains_with_cancel(&jsonb, &probe, Some(&token)).unwrap()
    );
    assert_eq!(
        op_contained_by(&probe, &jsonb).unwrap(),
        op_contained_by_with_cancel(&probe, &jsonb, Some(&token)).unwrap()
    );
    assert_eq!(
        op_has_key(&json, &key).unwrap(),
        op_has_key_with_cancel(&json, &key, Some(&token)).unwrap()
    );
    assert_eq!(
        op_has_any_key(&json, &keys).unwrap(),
        op_has_any_key_with_cancel(&json, &keys, Some(&token)).unwrap()
    );
    assert_eq!(
        op_has_all_keys(&json, &all_keys).unwrap(),
        op_has_all_keys_with_cancel(&json, &all_keys, Some(&token)).unwrap()
    );
    assert_eq!(
        op_delete_path(&jsonb, &path).unwrap(),
        op_delete_path_with_cancel(&jsonb, &path, Some(&token)).unwrap()
    );
    assert_eq!(
        op_delete_one(&json, &key).unwrap(),
        op_delete_one_with_cancel(&json, &key, Some(&token)).unwrap()
    );
    assert_eq!(
        op_concat(&json, &probe).unwrap(),
        op_concat_with_cancel(&json, &probe, Some(&token)).unwrap()
    );
    assert_eq!(
        op_path_exists(&json, &json_path).unwrap(),
        op_path_exists_with_cancel(&json, &json_path, Some(&token)).unwrap()
    );
    assert_eq!(
        op_path_match(&json, &json_path).unwrap(),
        op_path_match_with_cancel(&json, &json_path, Some(&token)).unwrap()
    );
    assert!(!token.is_cancelled());
}

#[test]
fn jsonb_get_text_can_cancel_inside_one_large_output_copy() {
    let source = text_to_jsonb(
        &serde_json::to_string(&serde_json::json!({ "payload": "x".repeat(256 * 1024) })).unwrap(),
    )
    .unwrap();
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 8);

    assert_interrupted(op_get_text_with_cancel(
        &source,
        &Value::Text("payload".into()),
        Some(&token),
    ));
    assert!(token.is_cancelled());
}

#[test]
fn evaluator_threads_cancellation_into_json_binary_operators() {
    let left =
        text_to_jsonb(&serde_json::to_string(&(0..2_048).collect::<Vec<i64>>()).unwrap()).unwrap();
    let right =
        text_to_jsonb(&serde_json::to_string(&(1_024..2_048).collect::<Vec<i64>>()).unwrap())
            .unwrap();
    let expression = crate::parser::Expr::BinaryOp {
        left: Box::new(crate::parser::Expr::Literal(left)),
        op: crate::parser::BinOp::JsonContains,
        right: Box::new(crate::parser::Expr::Literal(right)),
    };
    let columns = crate::eval::ColumnMap::new(&[]);
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 64);

    assert_interrupted(crate::eval::eval_expr(
        &expression,
        &crate::eval::EvalCtx::new(&columns, &[]).with_cancel(Some(&token)),
    ));
    assert!(token.is_cancelled());
}

#[test]
fn untripped_token_preserves_json_cast_results_and_errors() {
    use crate::parser::Expr;
    use crate::types::DataType;

    let columns = crate::eval::ColumnMap::new(&[]);
    let token = CancelToken::new();
    let evaluate = |value: Value, target| {
        let expression = Expr::Cast {
            expr: Box::new(Expr::Literal(value)),
            data_type: target,
        };
        let normal = crate::eval::eval_expr(&expression, &crate::eval::EvalCtx::new(&columns, &[]));
        let cancellable = crate::eval::eval_expr(
            &expression,
            &crate::eval::EvalCtx::new(&columns, &[]).with_cancel(Some(&token)),
        );
        (normal, cancellable)
    };

    let text = Value::Text(r#"{"items":[1,2,3]}"#.into());
    for target in [DataType::Json, DataType::Jsonb] {
        let (normal, cancellable) = evaluate(text.clone(), target);
        assert_eq!(normal.unwrap(), cancellable.unwrap());
    }
    let jsonb = text_to_jsonb(r#"{"items":[1,2,3]}"#).unwrap();
    for target in [DataType::Text, DataType::Json] {
        let (normal, cancellable) = evaluate(jsonb.clone(), target);
        assert_eq!(normal.unwrap(), cancellable.unwrap());
    }
    let (normal, cancellable) = evaluate(Value::Text("{".into()), DataType::Jsonb);
    assert_eq!(
        normal.unwrap_err().to_string(),
        cancellable.unwrap_err().to_string()
    );
    assert!(!token.is_cancelled());
}

#[test]
fn evaluator_can_cancel_inside_one_large_jsonb_cast() {
    let text = serde_json::to_string(&(0..100_000).collect::<Vec<i64>>()).unwrap();
    let expression = crate::parser::Expr::Cast {
        expr: Box::new(crate::parser::Expr::Literal(Value::Text(text.into()))),
        data_type: crate::types::DataType::Jsonb,
    };
    let columns = crate::eval::ColumnMap::new(&[]);
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 4);

    assert_interrupted(crate::eval::eval_expr(
        &expression,
        &crate::eval::EvalCtx::new(&columns, &[]).with_cancel(Some(&token)),
    ));
    assert!(token.is_cancelled());
}

#[test]
fn json_text_path_parsing_can_cancel_inside_one_large_segment() {
    let source = text_to_jsonb(r#"{"value":1}"#).unwrap();
    let path = Value::Text(format!("$.{}", "x".repeat(128 * 1024)).into());
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 16);

    assert_interrupted(op_path_with_cancel(&source, &path, Some(&token)));
    assert!(token.is_cancelled());
}

#[test]
fn json_aggregate_serialization_can_cancel_inside_one_large_value() {
    let values = vec![Value::Text("x".repeat(256 * 1024).into())];
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 16);

    assert_interrupted(agg_array_with_cancel(
        &values,
        crate::types::DataType::Jsonb,
        Some(&token),
    ));
    assert!(token.is_cancelled());
}

#[test]
fn untripped_token_preserves_json_aggregate_results() {
    let values = vec![Value::Integer(1), Value::Text("two".into())];
    let pairs = vec![
        (Value::Text("a".into()), Value::Integer(1)),
        (Value::Text("b".into()), Value::Text("two".into())),
    ];
    let token = CancelToken::new();

    assert_eq!(
        agg_array(&values, crate::types::DataType::Jsonb).unwrap(),
        agg_array_with_cancel(&values, crate::types::DataType::Jsonb, Some(&token)).unwrap()
    );
    assert_eq!(
        agg_object(&pairs, crate::types::DataType::Json).unwrap(),
        agg_object_with_cancel(&pairs, crate::types::DataType::Json, Some(&token)).unwrap()
    );
    assert!(!token.is_cancelled());
}

#[test]
fn evaluator_threads_cancellation_into_json_has_key_functions() {
    let object: serde_json::Map<String, serde_json::Value> = (0..1_024)
        .map(|index| (format!("key_{index:04}"), serde_json::Value::from(index)))
        .collect();
    let source = text_to_jsonb(&serde_json::to_string(&object).unwrap()).unwrap();
    let expression = crate::parser::Expr::Function {
        name: "JSONB_HAS_KEY".into(),
        args: vec![
            crate::parser::Expr::Literal(source),
            crate::parser::Expr::Literal(Value::Text("missing".into())),
        ],
        distinct: false,
    };
    let columns = crate::eval::ColumnMap::new(&[]);
    let token = CancelToken::new();
    let _guard = cancel_json_after(token.clone(), 32);

    assert_interrupted(crate::eval::eval_expr(
        &expression,
        &crate::eval::EvalCtx::new(&columns, &[]).with_cancel(Some(&token)),
    ));
    assert!(token.is_cancelled());
}
