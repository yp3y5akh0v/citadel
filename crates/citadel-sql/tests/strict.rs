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
fn strict_table_accepts_exact_type() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
            .unwrap(),
    );
    assert_ok(conn.execute("INSERT INTO t VALUES (1, 42)").unwrap());
    let qr = conn.query("SELECT n FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn strict_table_rejects_text_to_integer_lossy() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    let err = conn.execute("INSERT INTO t VALUES (1, 'xyz')").unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn strict_table_rejects_lossy_leading_zeros() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    let err = conn
        .execute("INSERT INTO t VALUES (1, '000123')")
        .unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn strict_table_accepts_lossless_text_to_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '42')").unwrap();
    let qr = conn.query("SELECT n FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn strict_table_rejects_real_to_integer_lossy() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    let err = conn.execute("INSERT INTO t VALUES (1, 5.5)").unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn strict_table_accepts_real_to_integer_whole() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5.0)").unwrap();
    let qr = conn.query("SELECT n FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn strict_table_accepts_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let qr = conn.query("SELECT n FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn strict_table_update_rejects_lossy() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 42)").unwrap();
    let err = conn
        .execute("UPDATE t SET n = 'xyz' WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn non_strict_table_truncates_real_to_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5.5)").unwrap();
    let qr = conn.query("SELECT n FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn strict_table_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
            .unwrap();
    }
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let err = conn.execute("INSERT INTO t VALUES (1, 'xyz')").unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}
