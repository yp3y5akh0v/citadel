use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"truncate-torture")
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
fn truncate_large_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    assert_ok(conn.execute("BEGIN").unwrap());
    for i in 0..10_000 {
        assert_rows(
            conn.execute(&format!("INSERT INTO big VALUES ({i}, {})", i * 2))
                .unwrap(),
            1,
        );
    }
    assert_ok(conn.execute("COMMIT").unwrap());

    assert_eq!(count(&conn, "SELECT COUNT(*) FROM big"), 10_000);
    assert_rows(conn.execute("TRUNCATE TABLE big").unwrap(), 10_000);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM big"), 0);
}

#[test]
fn truncate_resource_cycling() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE cycle (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    for cycle in 0..50 {
        for i in 0..100 {
            assert_rows(
                conn.execute(&format!("INSERT INTO cycle VALUES ({i}, 'c{cycle}_{i}')"))
                    .unwrap(),
                1,
            );
        }
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM cycle"), 100);
        assert_rows(conn.execute("TRUNCATE TABLE cycle").unwrap(), 100);
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM cycle"), 0);
    }
}

#[test]
fn truncate_multi_table_fk_closure() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, FOREIGN KEY (a_id) REFERENCES a(id))",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, b_id INTEGER, FOREIGN KEY (b_id) REFERENCES b(id))",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE d (id INTEGER PRIMARY KEY, c_id INTEGER, FOREIGN KEY (c_id) REFERENCES c(id))",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE e (id INTEGER PRIMARY KEY, d_id INTEGER, FOREIGN KEY (d_id) REFERENCES d(id))",
    )
    .unwrap();

    assert_rows(conn.execute("INSERT INTO a VALUES (1)").unwrap(), 1);
    assert_rows(conn.execute("INSERT INTO b VALUES (10, 1)").unwrap(), 1);
    assert_rows(conn.execute("INSERT INTO c VALUES (100, 10)").unwrap(), 1);
    assert_rows(conn.execute("INSERT INTO d VALUES (1000, 100)").unwrap(), 1);
    assert_rows(
        conn.execute("INSERT INTO e VALUES (10000, 1000)").unwrap(),
        1,
    );

    assert_rows(conn.execute("TRUNCATE TABLE a, b, c, d, e").unwrap(), 5);
    for t in ["a", "b", "c", "d", "e"] {
        assert_eq!(count(&conn, &format!("SELECT COUNT(*) FROM {t}")), 0);
    }
}

#[test]
fn truncate_savepoint_nest_rollback_each_level() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    for i in 1..=10 {
        assert_rows(
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 10))
                .unwrap(),
            1,
        );
    }

    assert_ok(conn.execute("BEGIN").unwrap());
    assert_ok(conn.execute("SAVEPOINT sp1").unwrap());
    assert_rows(conn.execute("TRUNCATE TABLE t").unwrap(), 10);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 0);

    assert_rows(conn.execute("INSERT INTO t VALUES (100, 1000)").unwrap(), 1);
    assert_ok(conn.execute("SAVEPOINT sp2").unwrap());
    assert_rows(conn.execute("TRUNCATE TABLE t").unwrap(), 1);
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 0);

    assert_ok(conn.execute("ROLLBACK TO sp2").unwrap());
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 1);

    assert_ok(conn.execute("ROLLBACK TO sp1").unwrap());
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 10);

    assert_ok(conn.execute("COMMIT").unwrap());
    assert_eq!(count(&conn, "SELECT COUNT(*) FROM t"), 10);
}
