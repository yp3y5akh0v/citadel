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
fn jsonb_typeof_object() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_typeof(CAST('{\"a\":1}' AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("object".into()));
}

#[test]
fn jsonb_typeof_each_variant() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_typeof(CAST('[1]' AS JSONB)), jsonb_typeof(CAST('1' AS JSONB)), jsonb_typeof(CAST('\"s\"' AS JSONB)), jsonb_typeof(CAST('true' AS JSONB)), jsonb_typeof(CAST('null' AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("array".into()));
    assert_eq!(qr.rows[0][1], Value::Text("number".into()));
    assert_eq!(qr.rows[0][2], Value::Text("string".into()));
    assert_eq!(qr.rows[0][3], Value::Text("boolean".into()));
    assert_eq!(qr.rows[0][4], Value::Text("null".into()));
}

#[test]
fn jsonb_array_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_array_length(CAST('[1,2,3,4]' AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(4));
}

#[test]
fn jsonb_object_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_object_length(CAST('{\"a\":1,\"b\":2,\"c\":3}' AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn jsonb_extract_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_extract_path(CAST('{\"a\":{\"b\":42}}' AS JSONB), 'a', 'b')")
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert_eq!(s, "42");
}

#[test]
fn jsonb_extract_path_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_extract_path_text(CAST('{\"a\":{\"b\":\"hello\"}}' AS JSONB), 'a', 'b')",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
}

#[test]
fn json_extract_dollar_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT json_extract(CAST('{\"a\":{\"b\":42}}' AS JSONB), '$.a.b')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("42".into()));
}

#[test]
fn json_valid() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT json_valid('{\"ok\":true}'), json_valid('not json')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
    assert_eq!(qr.rows[0][1], Value::Boolean(false));
}

#[test]
fn jsonb_strip_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_strip_nulls(CAST('{\"a\":1,\"b\":null,\"c\":3}' AS JSONB))")
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert!(!s.contains("\"b\""));
    assert!(s.contains("\"a\":1"));
    assert!(s.contains("\"c\":3"));
}

#[test]
fn jsonb_build_object() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_build_object('name', 'alice', 'age', 30)")
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert!(s.contains("\"name\":\"alice\""));
    assert!(s.contains("\"age\":30"));
}

#[test]
fn jsonb_build_array() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_build_array(1, 'two', true, NULL)")
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert_eq!(s, "[1,\"two\",true,null]");
}

#[test]
fn jsonb_set_object() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_set(CAST('{\"a\":1}' AS JSONB), 'a', CAST('2' AS JSONB))")
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert_eq!(s, "{\"a\":2}");
}

#[test]
fn to_jsonb_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT to_jsonb(42)").unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert_eq!(s, "42");
}

#[test]
fn to_jsonb_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT to_jsonb('hello')").unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert_eq!(s, "\"hello\"");
}

#[test]
fn jsonb_pretty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_pretty(CAST('{\"a\":1,\"b\":2}' AS JSONB))")
        .unwrap();
    let txt = match &qr.rows[0][0] {
        Value::Text(s) => s.to_string(),
        _ => panic!("expected text"),
    };
    assert!(txt.contains('\n'));
}

#[test]
fn jsonb_has_key_function() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_has_key(CAST('{\"a\":1}' AS JSONB), 'a'), jsonb_has_key(CAST('{\"a\":1}' AS JSONB), 'b')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
    assert_eq!(qr.rows[0][1], Value::Boolean(false));
}

#[test]
fn jsonb_agg_collects_array() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .unwrap();
    let qr = conn.query("SELECT jsonb_agg(name) FROM t").unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert!(s.contains("alice"));
    assert!(s.contains("bob"));
    assert!(s.contains("carol"));
}

#[test]
fn jsonb_agg_with_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, dept TEXT, name TEXT)")
            .unwrap(),
    );
    conn.execute(
        "INSERT INTO t VALUES (1, 'eng', 'alice'), (2, 'eng', 'bob'), (3, 'sales', 'carol')",
    )
    .unwrap();
    let qr = conn
        .query("SELECT dept, jsonb_agg(name) FROM t GROUP BY dept ORDER BY dept")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn jsonb_object_agg_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20)")
        .unwrap();
    let qr = conn.query("SELECT jsonb_object_agg(k, v) FROM t").unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert!(s.contains("\"a\":10"));
    assert!(s.contains("\"b\":20"));
}

#[test]
fn jsonb_object_agg_drops_null_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'a', 10), (2, NULL, 20), (3, 'b', 30)")
        .unwrap();
    let qr = conn.query("SELECT jsonb_object_agg(k, v) FROM t").unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert!(s.contains("\"a\":10"));
    assert!(s.contains("\"b\":30"));
    assert!(!s.contains("20"));
}

#[test]
fn jsonb_object_agg_keeps_null_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'a', NULL)").unwrap();
    let qr = conn.query("SELECT jsonb_object_agg(k, v) FROM t").unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert_eq!(s, "{\"a\":null}");
}

#[test]
fn jsonb_object_agg_duplicate_last_wins() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 99)")
        .unwrap();
    let qr = conn
        .query("SELECT jsonb_object_agg(k, v) FROM (SELECT k, v FROM t ORDER BY id) s")
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert_eq!(s, "{\"a\":99}");
}

#[test]
fn json_object_agg_returns_json_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
    let qr = conn.query("SELECT json_object_agg(k, v) FROM t").unwrap();
    match &qr.rows[0][0] {
        Value::Json(s) => assert!(s.contains("\"a\":10")),
        _ => panic!("expected json text variant"),
    }
}
