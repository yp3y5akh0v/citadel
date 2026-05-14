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
