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
fn create_gin_index_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, CAST('{\"role\":\"admin\"}' AS JSONB))")
        .unwrap();
    conn.execute("CREATE INDEX idx_data ON users USING gin (data)")
        .unwrap();
}

#[test]
fn gin_index_accelerates_contains_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..100 {
        let role = if i % 10 == 0 { "admin" } else { "member" };
        let payload = format!(r#"{{"id":{i},"role":"{role}"}}"#);
        conn.execute(&format!(
            "INSERT INTO users VALUES ({i}, CAST('{payload}' AS JSONB))"
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE INDEX idx_data ON users USING gin (data)")
        .unwrap();

    let qr = conn
        .query("SELECT id FROM users WHERE data @> CAST('{\"role\":\"admin\"}' AS JSONB)")
        .unwrap();
    assert_eq!(qr.rows.len(), 10);
    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!("expected integer"),
        })
        .collect();
    for id in ids {
        assert_eq!(id % 10, 0);
    }
}

#[test]
fn gin_index_maintained_on_insert_after_create() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("CREATE INDEX idx_data ON users USING gin (data)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, CAST('{\"role\":\"admin\"}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (2, CAST('{\"role\":\"member\"}' AS JSONB))")
        .unwrap();
    let qr = conn
        .query("SELECT id FROM users WHERE data @> CAST('{\"role\":\"admin\"}' AS JSONB)")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn gin_index_maintained_on_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, CAST('{\"role\":\"admin\"}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (2, CAST('{\"role\":\"admin\"}' AS JSONB))")
        .unwrap();
    conn.execute("CREATE INDEX idx_data ON users USING gin (data)")
        .unwrap();
    conn.execute("DELETE FROM users WHERE id = 1").unwrap();
    let qr = conn
        .query("SELECT id FROM users WHERE data @> CAST('{\"role\":\"admin\"}' AS JSONB)")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn gin_rejects_on_non_jsonb_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let result = conn.execute("CREATE INDEX idx_name ON t USING gin (name)");
    assert!(result.is_err(), "GIN on TEXT column should be rejected");
}

#[test]
fn gin_rejects_unique() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    let result = conn.execute("CREATE UNIQUE INDEX idx_data ON users USING gin (data)");
    assert!(result.is_err(), "UNIQUE GIN should be rejected");
}

#[test]
fn jsonb_path_ops_index_creates_and_filters_contains() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..50 {
        let role = if i % 5 == 0 { "admin" } else { "user" };
        let payload = format!(r#"{{"id":{i},"role":"{role}","tags":["a","b"]}}"#);
        conn.execute(&format!(
            "INSERT INTO docs VALUES ({i}, CAST('{payload}' AS JSONB))"
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE INDEX idx_data ON docs USING gin (data) WITH (ops = 'jsonb_path_ops')")
        .unwrap();

    let qr = conn
        .query("SELECT id FROM docs WHERE data @> CAST('{\"role\":\"admin\"}' AS JSONB)")
        .unwrap();
    assert_eq!(qr.rows.len(), 10);
    for r in &qr.rows {
        match &r[0] {
            Value::Integer(i) => assert_eq!(i % 5, 0),
            _ => panic!("expected integer"),
        }
    }
}

#[test]
fn jsonb_path_ops_matches_jsonb_ops_for_contains() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..40 {
        let payload = format!(r#"{{"k":{i},"nested":{{"v":{}}}}}"#, i * 2);
        conn.execute(&format!(
            "INSERT INTO a VALUES ({i}, CAST('{payload}' AS JSONB))"
        ))
        .unwrap();
        conn.execute(&format!(
            "INSERT INTO b VALUES ({i}, CAST('{payload}' AS JSONB))"
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE INDEX a_idx ON a USING gin (data)")
        .unwrap();
    conn.execute("CREATE INDEX b_idx ON b USING gin (data) WITH (ops = 'jsonb_path_ops')")
        .unwrap();

    let probe = "CAST('{\"nested\":{\"v\":20}}' AS JSONB)";
    let qa = conn
        .query(&format!(
            "SELECT id FROM a WHERE data @> {probe} ORDER BY id"
        ))
        .unwrap();
    let qb = conn
        .query(&format!(
            "SELECT id FROM b WHERE data @> {probe} ORDER BY id"
        ))
        .unwrap();
    assert_eq!(qa.rows, qb.rows);
    assert_eq!(qa.rows.len(), 1);
}

#[test]
fn jsonb_path_ops_rejects_unknown_opclass() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    let err = conn
        .execute("CREATE INDEX idx ON t USING gin (data) WITH (ops = 'bogus_ops')")
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("bogus_ops"),
        "expected error to mention 'bogus_ops', got: {msg}"
    );
}
