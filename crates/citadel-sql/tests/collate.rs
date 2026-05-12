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

fn setup(conn: &Connection) {
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'BOB'), (3, 'charlie'), (4, 'alice')")
        .unwrap();
}

#[test]
fn collate_nocase_expression_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let qr = conn
        .query("SELECT id FROM t WHERE name = 'alice' COLLATE NOCASE ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(4));
}

#[test]
fn collate_binary_expression_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let qr = conn
        .query("SELECT id FROM t WHERE name = 'alice' COLLATE BINARY")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(4));
}

#[test]
fn collate_rtrim_expression_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, s TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO r VALUES (1, 'abc'), (2, 'abc  '), (3, 'abcx')")
        .unwrap();
    let qr = conn
        .query("SELECT id FROM r WHERE s = 'abc' COLLATE RTRIM ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
}

#[test]
fn collate_unsupported_name_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let err = conn
        .execute("SELECT * FROM t WHERE name = 'a' COLLATE FRENCH")
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(_)));
}

#[test]
fn order_by_column_collation_nocase() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'banana'), (2, 'Apple'), (3, 'cherry'), (4, 'apricot')")
        .unwrap();
    let qr = conn.query("SELECT id FROM t ORDER BY name").unwrap();
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(4));
    assert_eq!(qr.rows[2][0], Value::Integer(1));
    assert_eq!(qr.rows[3][0], Value::Integer(3));
}

#[test]
fn order_by_explicit_collate_nocase() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let qr = conn
        .query("SELECT id FROM t ORDER BY name COLLATE NOCASE")
        .unwrap();
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(4));
    assert_eq!(qr.rows[2][0], Value::Integer(2));
    assert_eq!(qr.rows[3][0], Value::Integer(3));
}

#[test]
fn unique_index_nocase_rejects_case_variant() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO u VALUES (1, 'Alice')").unwrap();
    let err = conn
        .execute("INSERT INTO u VALUES (2, 'ALICE')")
        .unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)), "{err:?}");
}

#[test]
fn unique_index_explicit_collate_in_create_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX u_name ON u (name COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO u VALUES (1, 'Alice')").unwrap();
    let err = conn
        .execute("INSERT INTO u VALUES (2, 'alice')")
        .unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)), "{err:?}");
}

#[test]
fn create_table_with_column_collate_persists() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)")
            .unwrap();
    }
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    let qr = conn.query("SELECT id FROM t WHERE name = 'alice'").unwrap();
    assert_eq!(qr.rows.len(), 1);
}
