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
fn populate_record_projects_full_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    let qr = conn
        .query(
            "SELECT id, name, age FROM jsonb_populate_record(\
                NULL::users, \
                CAST('{\"id\": 1, \"name\": \"Alice\", \"age\": 30}' AS JSONB))",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(30));
}

#[test]
fn populate_record_missing_keys_become_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    let qr = conn
        .query(
            "SELECT id, name, age FROM jsonb_populate_record(\
                NULL::users, CAST('{\"id\": 5}' AS JSONB))",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(5));
    assert_eq!(qr.rows[0][1], Value::Null);
    assert_eq!(qr.rows[0][2], Value::Null);
}

#[test]
fn populate_record_extra_keys_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let qr = conn
        .query(
            "SELECT * FROM jsonb_populate_record(\
                NULL::t, \
                CAST('{\"id\": 1, \"name\": \"x\", \"unknown\": \"ignored\"}' AS JSONB))",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("x".into()));
}

#[test]
fn populate_record_string_to_int_coerces() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    let qr = conn
        .query(
            "SELECT n FROM jsonb_populate_record(\
                NULL::t, CAST('{\"n\": \"42\"}' AS JSONB))",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn populate_record_unknown_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .query(
            "SELECT * FROM jsonb_populate_record(\
                NULL::nonexistent, CAST('{}' AS JSONB))",
        )
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("nonexistent"),
        "expected error to mention 'nonexistent', got: {msg}"
    );
}

#[test]
fn populate_recordset_array_projects_many_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE pt (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let qr = conn
        .query(
            "SELECT id, name FROM jsonb_populate_recordset(\
                NULL::pt, \
                CAST('[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"},{\"id\":3,\"name\":\"c\"}]' AS JSONB))",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("a".into()));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
    assert_eq!(qr.rows[2][1], Value::Text("c".into()));
}

#[test]
fn populate_recordset_empty_array_returns_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let qr = conn
        .query("SELECT * FROM jsonb_populate_recordset(NULL::t, CAST('[]' AS JSONB))")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn populate_recordset_null_jsonb_returns_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let qr = conn
        .query("SELECT * FROM jsonb_populate_recordset(NULL::t, NULL)")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn populate_record_bad_type_coercion_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    let err = conn
        .query(
            "SELECT n FROM jsonb_populate_record(\
                NULL::t, CAST('{\"n\": \"not_a_number\"}' AS JSONB))",
        )
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_ascii_lowercase().contains("integer") || msg.contains("cast"),
        "expected coercion error, got: {msg}"
    );
}
