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
