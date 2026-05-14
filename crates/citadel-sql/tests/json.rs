use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

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
fn create_table_with_json_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, j JSON, jb JSONB)")
            .unwrap(),
    );
}

#[test]
fn cast_text_to_jsonb() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT CAST('{\"a\":1}' AS JSONB)").unwrap();
    assert!(matches!(qr.rows[0][0], Value::Jsonb(_)));
}

#[test]
fn cast_text_to_json() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT CAST('{\"a\":1}' AS JSON)").unwrap();
    assert!(matches!(qr.rows[0][0], Value::Json(_)));
}

#[test]
fn cast_invalid_json_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn.query("SELECT CAST('not json' AS JSONB)").unwrap_err();
    assert!(matches!(err, SqlError::InvalidValue(_)));
}

#[test]
fn op_arrow_object_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":1, \"b\":\"x\"}' AS JSONB) -> 'a'")
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert_eq!(s, "1");
}

#[test]
fn op_arrow_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"name\":\"alice\"}' AS JSONB) ->> 'name'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
}

#[test]
fn op_arrow_text_missing_key_is_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":1}' AS JSONB) ->> 'missing'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn op_arrow_text_json_null_yields_sql_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":null}' AS JSONB) ->> 'a'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn op_array_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('[10,20,30]' AS JSONB) ->> 1")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("20".into()));
}

#[test]
fn op_contains_true() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":1,\"b\":2}' AS JSONB) @> CAST('{\"a\":1}' AS JSONB)")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn op_contains_false() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":1}' AS JSONB) @> CAST('{\"a\":2}' AS JSONB)")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(false));
}

#[test]
fn op_has_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":1}' AS JSONB) ? 'a'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn op_has_key_on_null_value_still_true() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":null}' AS JSONB) ? 'a'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn op_concat_objects() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":1}' AS JSONB) || CAST('{\"b\":2}' AS JSONB)")
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert!(s.contains("\"a\":1"));
    assert!(s.contains("\"b\":2"));
}

#[test]
fn op_delete_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":1,\"b\":2}' AS JSONB) - 'a'")
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert!(!s.contains("\"a\""));
    assert!(s.contains("\"b\":2"));
}

#[test]
fn op_null_propagation_on_get() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT CAST(NULL AS JSONB) -> 'a'").unwrap();
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn round_trip_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, jb JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"foo\":\"bar\"}' AS JSONB))")
        .unwrap();
    let qr = conn
        .query("SELECT jb ->> 'foo' FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("bar".into()));
}

#[test]
fn schema_persists_jsonb_value_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, jb JSONB)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, CAST('{\"k\":42}' AS JSONB))")
            .unwrap();
    }
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT jb ->> 'k' FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("42".into()));
}
