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
fn create_table_with_tsvector_and_tsquery_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TSVECTOR, q TSQUERY)")
        .unwrap();
    // Re-open to confirm schema round-trips through the catalog.
    drop(conn);
    drop(db);
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("SELECT id FROM docs WHERE id = 0").unwrap();
}

#[test]
fn text_at_at_text_auto_tokenizes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT 'hello world' @@ 'hello'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(true));

    let rows = conn
        .prepare("SELECT 'hello world' @@ 'mouse'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(false));
}

#[test]
fn json_at_at_still_works_after_overload() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT CAST('{\"a\":1}' AS JSONB) @@ '$.a == 1'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(true));
}
