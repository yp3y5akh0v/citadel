use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"truncate-test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn assert_ok(r: ExecutionResult) {
    match r {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn assert_rows(r: ExecutionResult, expected: u64) {
    match r {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn count(conn: &Connection<'_>, sql: &str) -> i64 {
    let qr = conn.query(sql).unwrap();
    match &qr.rows[0][0] {
        Value::Integer(n) => *n,
        v => panic!("expected integer count, got {v:?}"),
    }
}

fn setup_t(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    for i in 1..=5 {
        assert_rows(
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'r{i}')"))
                .unwrap(),
            1,
        );
    }
}

#[test]
fn truncate_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    assert_rows(conn.execute("TRUNCATE TABLE t").unwrap(), 0);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn truncate_populated_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_t(&conn);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 5);
    assert_rows(conn.execute("TRUNCATE TABLE t").unwrap(), 5);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn truncate_table_keyword_optional() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_t(&conn);
    assert_rows(conn.execute("TRUNCATE t").unwrap(), 5);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn truncate_unknown_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn.execute("TRUNCATE TABLE nope").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn truncate_multi_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")
        .unwrap();
    assert_rows(
        conn.execute("INSERT INTO a VALUES (1), (2), (3)").unwrap(),
        3,
    );
    assert_rows(conn.execute("INSERT INTO b VALUES (10), (20)").unwrap(), 2);
    assert_rows(conn.execute("TRUNCATE TABLE a, b").unwrap(), 5);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM a"), 0);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM b"), 0);
}

#[test]
fn truncate_referenced_table_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER, FOREIGN KEY (p) REFERENCES parent(id))",
    )
    .unwrap();
    assert_rows(conn.execute("INSERT INTO parent VALUES (1)").unwrap(), 1);
    assert_rows(conn.execute("INSERT INTO child VALUES (10, 1)").unwrap(), 1);

    let err = conn.execute("TRUNCATE TABLE parent").unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(_)));
}

#[test]
fn truncate_with_referencing_in_list_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER, FOREIGN KEY (p) REFERENCES parent(id))",
    )
    .unwrap();
    assert_rows(conn.execute("INSERT INTO parent VALUES (1)").unwrap(), 1);
    assert_rows(conn.execute("INSERT INTO child VALUES (10, 1)").unwrap(), 1);

    assert_rows(conn.execute("TRUNCATE TABLE child, parent").unwrap(), 2);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM parent"), 0);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM child"), 0);
}

#[test]
fn truncate_self_referential_fk_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE node (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES node(id))",
    )
    .unwrap();
    assert_rows(
        conn.execute("INSERT INTO node VALUES (1, NULL)").unwrap(),
        1,
    );
    assert_rows(conn.execute("INSERT INTO node VALUES (2, 1)").unwrap(), 1);
    assert_rows(conn.execute("TRUNCATE TABLE node").unwrap(), 2);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM node"), 0);
}

#[test]
fn post_truncate_insert_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_t(&conn);
    assert_rows(conn.execute("TRUNCATE TABLE t").unwrap(), 5);
    assert_rows(
        conn.execute("INSERT INTO t VALUES (100, 'after')").unwrap(),
        1,
    );
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 1);
    let qr = conn.query("SELECT id, val FROM t WHERE id = 100").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(100));
    assert_eq!(qr.rows[0][1], Value::Text("after".into()));
}

#[test]
fn post_truncate_index_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_name ON t(name)").unwrap();
    assert_rows(
        conn.execute("INSERT INTO t VALUES (1, 'alice')").unwrap(),
        1,
    );
    assert_rows(conn.execute("INSERT INTO t VALUES (2, 'bob')").unwrap(), 1);
    assert_rows(conn.execute("TRUNCATE TABLE t").unwrap(), 2);

    assert_rows(
        conn.execute("INSERT INTO t VALUES (3, 'alice')").unwrap(),
        1,
    );
    let qr = conn.query("SELECT id FROM t WHERE name = 'alice'").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn truncate_inside_txn_rollback_restores() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_t(&conn);

    assert_ok(conn.execute("BEGIN").unwrap());
    assert_rows(conn.execute("TRUNCATE TABLE t").unwrap(), 5);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 0);
    assert_ok(conn.execute("ROLLBACK").unwrap());

    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 5);
}

#[test]
fn truncate_inside_savepoint_rollback_restores() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_t(&conn);

    assert_ok(conn.execute("BEGIN").unwrap());
    assert_ok(conn.execute("SAVEPOINT sp1").unwrap());
    assert_rows(conn.execute("TRUNCATE TABLE t").unwrap(), 5);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 0);
    assert_ok(conn.execute("ROLLBACK TO sp1").unwrap());
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 5);
    assert_ok(conn.execute("COMMIT").unwrap());
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 5);
}

#[test]
fn truncate_cascade_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_t(&conn);
    let err = conn.execute("TRUNCATE TABLE t CASCADE").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("v0.13")));
}

#[test]
fn truncate_restart_identity_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_t(&conn);
    assert_rows(
        conn.execute("TRUNCATE TABLE t RESTART IDENTITY").unwrap(),
        5,
    );
    assert_rows(conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap(), 1);
}

#[test]
fn truncate_continue_identity_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_t(&conn);
    assert_rows(
        conn.execute("TRUNCATE TABLE t CONTINUE IDENTITY").unwrap(),
        5,
    );
}

#[test]
fn truncate_only_keyword_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_t(&conn);
    assert_rows(conn.execute("TRUNCATE TABLE ONLY t").unwrap(), 5);
}
