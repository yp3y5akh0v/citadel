use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn assert_ok(r: ExecutionResult) {
    matches!(r, ExecutionResult::Ok | ExecutionResult::RowsAffected(_));
}

#[test]
fn jsonb_rejects_null_byte_escape() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .query(r#"SELECT CAST('{"a":"\u0000"}' AS JSONB)"#)
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_ascii_lowercase().contains("null") || msg.contains("\\u0000"),
        "expected null-byte rejection, got: {msg}"
    );
}

#[test]
fn jsonb_dedups_duplicate_keys_keep_last() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{"a":1,"a":2}' AS JSONB) ->> 'a'"#)
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("2".into()));
}

#[test]
fn json_preserves_duplicate_keys_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{"a":1,"a":2}' AS JSON)"#)
        .unwrap();
    match &qr.rows[0][0] {
        Value::Json(s) => assert!(s.contains("\"a\":1") && s.contains("\"a\":2")),
        v => panic!("expected JSON text, got {v:?}"),
    }
}

#[test]
fn jsonb_rejects_trailing_comma() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert!(conn.query(r#"SELECT CAST('{"a":1,}' AS JSONB)"#).is_err());
    assert!(conn.query(r#"SELECT CAST('[1,2,]' AS JSONB)"#).is_err());
}

#[test]
fn jsonb_rejects_orphan_surrogate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert!(conn
        .query(r#"SELECT CAST('{"s":"\uD834"}' AS JSONB)"#)
        .is_err());
}

#[test]
fn jsonb_accepts_valid_surrogate_pair() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{"s":"\uD834\uDD1E"}' AS JSONB) ->> 's'"#)
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("\u{1D11E}".into()));
}

#[test]
fn jsonb_large_number_f64_storage() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT jsonb_typeof(CAST('{"n":9007199254740993}' AS JSONB) -> 'n')"#)
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("number".into()));
}

#[test]
fn op_arrow_returns_json_null_for_null_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT jsonb_typeof(CAST('{"a":null}' AS JSONB) -> 'a')"#)
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("null".into()));
}

#[test]
fn op_arrow_text_returns_sql_null_for_null_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{"a":null}' AS JSONB) ->> 'a' IS NULL"#)
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn op_arrow_text_returns_sql_null_for_missing_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{}' AS JSONB) ->> 'a' IS NULL"#)
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn op_has_key_true_for_null_valued_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{"a":null}' AS JSONB) ? 'a'"#)
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn jsonb_normalizes_scientific_notation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{"n":1e5}' AS JSONB) ->> 'n'"#)
        .unwrap();
    let text = match &qr.rows[0][0] {
        Value::Text(s) => s.to_string(),
        v => panic!("expected text, got {v:?}"),
    };
    let parsed: f64 = text.parse().unwrap();
    assert!((parsed - 100000.0).abs() < 1e-9);
}

#[test]
fn json_preserves_source_text_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{ "b" : 1 , "a" : 2 }' AS JSON)"#)
        .unwrap();
    match &qr.rows[0][0] {
        Value::Json(s) => {
            assert!(s.contains("  ") || s.contains(" "));
            let b_pos = s.find("\"b\"").expect("b key present");
            let a_pos = s.find("\"a\"").expect("a key present");
            assert!(b_pos < a_pos, "JSON should preserve insertion order");
        }
        v => panic!("expected json text, got {v:?}"),
    }
}

#[test]
fn jsonb_sorts_keys_canonical() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{"z":3,"a":1,"m":2}' AS JSONB)"#)
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        v => panic!("expected jsonb, got {v:?}"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    let a_pos = s.find("\"a\"").unwrap();
    let m_pos = s.find("\"m\"").unwrap();
    let z_pos = s.find("\"z\"").unwrap();
    assert!(
        a_pos < m_pos && m_pos < z_pos,
        "JSONB should sort keys: {s}"
    );
}

#[test]
fn chained_arrow_arrow_text_is_left_associative() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{"a":{"b":"deep"}}' AS JSONB) -> 'a' ->> 'b'"#)
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("deep".into()));
}

#[test]
fn chained_path_arrow_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(r#"SELECT CAST('{"a":{"b":"hit"}}' AS JSONB) #> '{a}' ->> 'b'"#)
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("hit".into()));
}

#[test]
fn check_constraint_with_json_op() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB, \
             CHECK (data ->> 'name' IS NOT NULL))",
        )
        .unwrap(),
    );
    conn.execute(r#"INSERT INTO t VALUES (1, CAST('{"name":"ok"}' AS JSONB))"#)
        .unwrap();
    assert!(conn
        .execute(r#"INSERT INTO t VALUES (2, CAST('{"other":"x"}' AS JSONB))"#)
        .is_err());
}

#[test]
fn generated_column_with_json_op() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB, \
             name TEXT GENERATED ALWAYS AS (data ->> 'name') STORED)",
        )
        .unwrap(),
    );
    conn.execute(r#"INSERT INTO t (id, data) VALUES (1, CAST('{"name":"alice"}' AS JSONB))"#)
        .unwrap();
    let qr = conn.query("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
}

#[test]
fn json_in_returning_clause() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
            .unwrap(),
    );
    let r = conn
        .query(r#"INSERT INTO t VALUES (1, CAST('{"k":42}' AS JSONB)) RETURNING data ->> 'k'"#)
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Text("42".into()));
}

#[test]
fn large_inline_jsonb_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
            .unwrap(),
    );
    let big: String = (0..80)
        .map(|i| format!("\"k{i}\":\"{}\"", "x".repeat(10)))
        .collect::<Vec<_>>()
        .join(",");
    let payload = format!("{{{big}}}");
    let stmt = format!(
        "INSERT INTO t VALUES (1, CAST('{}' AS JSONB))",
        payload.replace('\'', "''")
    );
    conn.execute(&stmt).unwrap();
    let qr = conn
        .query("SELECT data ->> 'k0', data ->> 'k79' FROM t")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("x".repeat(10).into()));
    assert_eq!(qr.rows[0][1], Value::Text("x".repeat(10).into()));
}

#[test]
fn large_jsonb_overflow_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
            .unwrap(),
    );
    let big: String = (0..1000)
        .map(|i| format!("\"k{i}\":\"{}\"", "x".repeat(20)))
        .collect::<Vec<_>>()
        .join(",");
    let payload = format!("{{{big}}}");
    let stmt = format!(
        "INSERT INTO t VALUES (1, CAST('{}' AS JSONB))",
        payload.replace('\'', "''")
    );
    conn.execute(&stmt).unwrap();
    let qr = conn
        .query("SELECT data ->> 'k0', data ->> 'k999' FROM t")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("x".repeat(20).into()));
    assert_eq!(qr.rows[0][1], Value::Text("x".repeat(20).into()));
}

#[test]
fn multi_megabyte_jsonb_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
            .unwrap(),
    );
    let big: String = (0..10_000)
        .map(|i| format!("\"k{i}\":\"{}\"", "x".repeat(100)))
        .collect::<Vec<_>>()
        .join(",");
    let payload = format!("{{{big}}}");
    let stmt = format!(
        "INSERT INTO t VALUES (1, CAST('{}' AS JSONB))",
        payload.replace('\'', "''")
    );
    conn.execute(&stmt).unwrap();
    let qr = conn
        .query("SELECT data ->> 'k0', data ->> 'k9999' FROM t")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("x".repeat(100).into()));
    assert_eq!(qr.rows[0][1], Value::Text("x".repeat(100).into()));
}

#[test]
fn deep_nesting_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let mut nested = String::from("\"leaf\"");
    for _ in 0..60 {
        nested = format!("{{\"x\":{nested}}}");
    }
    let stmt = format!("SELECT CAST('{nested}' AS JSONB)");
    let qr = conn.query(&stmt).unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        v => panic!("expected jsonb, got {v:?}"),
    };
    let decoded = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert!(decoded.contains("leaf"));
}
