use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"indexes-torture")
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

#[test]
fn partial_bulk_predicate_match_distribution() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, status INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_active ON t(val) WHERE status = 1")
        .unwrap();

    assert_ok(conn.execute("BEGIN").unwrap());
    let mut active = 0i64;
    for i in 0..10_000 {
        let status = if i % 2 == 0 { 1 } else { 0 };
        if status == 1 {
            active += 1;
        }
        assert_rows(
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {i}, {status})"))
                .unwrap(),
            1,
        );
    }
    assert_ok(conn.execute("COMMIT").unwrap());

    let total = count(&conn, "SELECT COUNT(*) FROM t WHERE status = 1");
    assert_eq!(total, active);
}

#[test]
fn partial_update_traverses_quadrants() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, active INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_active_val ON t(val) WHERE active = 1")
        .unwrap();

    assert_ok(conn.execute("BEGIN").unwrap());
    for i in 0..1000 {
        let active = if i < 500 { 1 } else { 0 };
        assert_rows(
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {i}, {active})"))
                .unwrap(),
            1,
        );
    }
    assert_ok(conn.execute("COMMIT").unwrap());

    assert_rows(
        conn.execute("UPDATE t SET active = 1 - active").unwrap(),
        1000,
    );

    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t WHERE active = 1"), 500);

    assert_rows(
        conn.execute("UPDATE t SET val = val + 10000").unwrap(),
        1000,
    );

    assert_eq!(
        count(&conn, "SELECT COUNT(*) FROM t WHERE val >= 10000"),
        1000
    );
}

#[test]
fn partial_index_create_drop_churn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, flag INTEGER)")
        .unwrap();
    for i in 0..100 {
        let flag = if i % 3 == 0 { 1 } else { 0 };
        assert_rows(
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {i}, {flag})"))
                .unwrap(),
            1,
        );
    }

    for cycle in 0..50 {
        conn.execute("CREATE INDEX t_partial ON t(val) WHERE flag = 1")
            .unwrap();
        let n = count(&conn, "SELECT COUNT(*) FROM t WHERE flag = 1 AND val < 50");
        assert!(n >= 0, "cycle {cycle}: count returned {n}");
        conn.execute("DROP INDEX t_partial").unwrap();
    }
}

#[test]
fn partial_unique_plus_full_unique_independent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT, deleted_at INTEGER)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX t_email ON t(email)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX t_email_active ON t(email) WHERE deleted_at IS NULL")
        .unwrap();

    assert_rows(
        conn.execute("INSERT INTO t VALUES (1, 'a@x', NULL)")
            .unwrap(),
        1,
    );
    let err = conn
        .execute("INSERT INTO t VALUES (2, 'a@x', 100)")
        .unwrap_err();
    assert!(matches!(err, citadel_sql::SqlError::UniqueViolation(_)));
}

#[test]
fn many_partial_indexes_on_one_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, bucket INTEGER)")
        .unwrap();

    for b in 0..50 {
        conn.execute(&format!("CREATE INDEX t_b{b} ON t(val) WHERE bucket = {b}"))
            .unwrap();
    }

    assert_ok(conn.execute("BEGIN").unwrap());
    for i in 0..500 {
        let bucket = i % 50;
        assert_rows(
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {i}, {bucket})"))
                .unwrap(),
            1,
        );
    }
    assert_ok(conn.execute("COMMIT").unwrap());

    for b in 0..50 {
        let n = count(&conn, &format!("SELECT COUNT(*) FROM t WHERE bucket = {b}"));
        assert_eq!(n, 10, "bucket {b}");
    }
}
