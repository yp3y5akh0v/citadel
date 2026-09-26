use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn second_writer_rejected_with_write_transaction_active() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    a.execute("BEGIN").unwrap();
    let b = Connection::open(&db).unwrap();
    let err = b.execute("BEGIN").unwrap_err();
    assert!(matches!(err, SqlError::Storage(_)), "got: {err:?}");
    a.execute("ROLLBACK").unwrap();
}

#[test]
fn second_connection_reads_after_first_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    let b = Connection::open(&db).unwrap();
    let qr = b.query("SELECT n FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(100));
}

#[test]
fn read_after_commit_sees_new_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let writer = Connection::open(&db).unwrap();
    writer
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    writer.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    let reader = Connection::open(&db).unwrap();
    let before = reader.query("SELECT n FROM t WHERE id = 1").unwrap();
    assert_eq!(before.rows[0][0], Value::Integer(100));
    writer.execute("UPDATE t SET n = 200 WHERE id = 1").unwrap();
    let after = reader.query("SELECT n FROM t WHERE id = 1").unwrap();
    assert_eq!(after.rows[0][0], Value::Integer(200));
}

#[test]
fn many_readers_share_database_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let setup = Connection::open(&db).unwrap();
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    for i in 0..10i64 {
        setup
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }
    let readers: Vec<Connection<'_>> = (0..5).map(|_| Connection::open(&db).unwrap()).collect();
    for r in &readers {
        let qr = r.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(10));
    }
}

#[test]
fn writer_proceeds_after_readers_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let setup = Connection::open(&db).unwrap();
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    {
        let _readers: Vec<Connection<'_>> =
            (0..3).map(|_| Connection::open(&db).unwrap()).collect();
        for r in &_readers {
            let _ = r.query("SELECT COUNT(*) FROM t").unwrap();
        }
    }
    setup.execute("INSERT INTO t VALUES (1)").unwrap();
    let qr = setup.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn fts_writes_visible_to_second_reader() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let writer = Connection::open(&db).unwrap();
    writer
        .execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TSVECTOR)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_body ON docs USING fts (body)")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (1, to_tsvector('alpha bravo charlie'))")
        .unwrap();
    let reader = Connection::open(&db).unwrap();
    let qr = reader
        .query("SELECT id FROM docs WHERE body @@ to_tsquery('bravo')")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn write_lock_released_on_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("INSERT INTO t VALUES (1)").unwrap();
    a.execute("COMMIT").unwrap();
    let b = Connection::open(&db).unwrap();
    b.execute("BEGIN").unwrap();
    b.execute("INSERT INTO t VALUES (2)").unwrap();
    b.execute("COMMIT").unwrap();
    let qr = b.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn write_lock_released_on_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("INSERT INTO t VALUES (1)").unwrap();
    a.execute("ROLLBACK").unwrap();
    let b = Connection::open(&db).unwrap();
    b.execute("INSERT INTO t VALUES (2)").unwrap();
    let qr = b.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}
