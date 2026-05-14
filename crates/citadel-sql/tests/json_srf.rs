use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn jsonb_array_elements_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT * FROM jsonb_array_elements(CAST('[10,20,30]' AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn jsonb_array_elements_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT value FROM jsonb_array_elements_text(CAST('[\"a\",\"b\",\"c\"]' AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("a".into()));
    assert_eq!(qr.rows[1][0], Value::Text("b".into()));
    assert_eq!(qr.rows[2][0], Value::Text("c".into()));
}

#[test]
fn jsonb_each_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT key FROM jsonb_each(CAST('{\"x\":1,\"y\":2}' AS JSONB)) ORDER BY key")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("x".into()));
    assert_eq!(qr.rows[1][0], Value::Text("y".into()));
}

#[test]
fn jsonb_object_keys_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT * FROM jsonb_object_keys(CAST('{\"a\":1,\"b\":2,\"c\":3}' AS JSONB)) ORDER BY 1")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("a".into()));
    assert_eq!(qr.rows[1][0], Value::Text("b".into()));
    assert_eq!(qr.rows[2][0], Value::Text("c".into()));
}

#[test]
fn srf_null_arg_returns_empty_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT * FROM jsonb_array_elements(CAST(NULL AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);
}
