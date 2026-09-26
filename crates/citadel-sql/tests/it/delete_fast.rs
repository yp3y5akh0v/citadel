use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn rows_affected(result: ExecutionResult) -> u64 {
    match result {
        ExecutionResult::RowsAffected(n) => n,
        other => panic!("expected RowsAffected, got {other:?}"),
    }
}

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn setup(conn: &Connection, n: i64) {
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER, tag TEXT)")
        .unwrap();
    for id in 1..=n {
        conn.execute(&format!("INSERT INTO t VALUES ({id}, {}, 'x{id}')", id % 3))
            .unwrap();
    }
}

fn ids(conn: &Connection) -> Vec<i64> {
    conn.query("SELECT id FROM t ORDER BY id")
        .unwrap()
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Integer(v) => v,
            _ => unreachable!(),
        })
        .collect()
}

#[test]
fn prepared_pk_point_delete_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn, 6);

    let stmt = conn.prepare("DELETE FROM t WHERE id = $1").unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(stmt.execute(&[Value::Integer(2)]).unwrap(), 1);
    assert_eq!(stmt.execute(&[Value::Integer(2)]).unwrap(), 0);
    assert_eq!(stmt.execute(&[Value::Integer(5)]).unwrap(), 1);
    conn.execute("COMMIT").unwrap();
    assert_eq!(ids(&conn), vec![1, 3, 4, 6]);
}

#[test]
fn pk_point_delete_returning_both_paths() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn, 4);

    let stmt = conn
        .prepare("DELETE FROM t WHERE id = $1 RETURNING id, tag")
        .unwrap();
    let qr = stmt.query_collect(&[Value::Integer(3)]).unwrap();
    assert_eq!(qr.columns, vec!["id", "tag"]);
    assert_eq!(
        qr.rows,
        vec![vec![Value::Integer(3), Value::Text("x3".into())]]
    );
    // Autocommit zero-match keeps the Query shape with columns.
    let qr = stmt.query_collect(&[Value::Integer(3)]).unwrap();
    assert_eq!(qr.columns, vec!["id", "tag"]);
    assert!(qr.rows.is_empty());

    conn.execute("BEGIN").unwrap();
    let qr = stmt.query_collect(&[Value::Integer(4)]).unwrap();
    assert_eq!(
        qr.rows,
        vec![vec![Value::Integer(4), Value::Text("x4".into())]]
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(ids(&conn), vec![1, 2]);
}

#[test]
fn pk_range_delete_all_ops_and_reversed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn, 10);

    let stmt = conn
        .prepare("DELETE FROM t WHERE id >= $1 AND id < $2")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(
        stmt.execute(&[Value::Integer(3), Value::Integer(6)])
            .unwrap(),
        3
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(ids(&conn), vec![1, 2, 6, 7, 8, 9, 10]);

    assert_eq!(
        rows_affected(conn.execute("DELETE FROM t WHERE 9 <= id").unwrap()),
        2
    );
    assert_eq!(ids(&conn), vec![1, 2, 6, 7, 8]);

    let stmt = conn.prepare("DELETE FROM t WHERE id > $1").unwrap();
    assert_eq!(stmt.execute(&[Value::Integer(7)]).unwrap(), 1);
    assert_eq!(ids(&conn), vec![1, 2, 6, 7]);

    assert_eq!(
        rows_affected(conn.execute("DELETE FROM t WHERE id <= 2").unwrap()),
        2
    );
    assert_eq!(ids(&conn), vec![6, 7]);
}

#[test]
fn delete_residual_conjunct_falls_back_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn, 6);

    let stmt = conn
        .prepare("DELETE FROM t WHERE id >= $1 AND val = 1")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(stmt.execute(&[Value::Integer(1)]).unwrap(), 2);
    conn.execute("COMMIT").unwrap();
    assert_eq!(ids(&conn), vec![2, 3, 5, 6]);

    let stmt = conn
        .prepare("DELETE FROM t WHERE id = $1 AND val = 0")
        .unwrap();
    assert_eq!(stmt.execute(&[Value::Integer(2)]).unwrap(), 0);
    assert_eq!(stmt.execute(&[Value::Integer(3)]).unwrap(), 1);
    assert_eq!(ids(&conn), vec![2, 5, 6]);
}

#[test]
fn delete_trigger_table_falls_back_and_fires() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn, 3);
    conn.execute("CREATE TABLE audit (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER trg AFTER DELETE ON t FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (OLD.id); END",
    )
    .unwrap();

    let stmt = conn.prepare("DELETE FROM t WHERE id = $1").unwrap();
    conn.execute("BEGIN").unwrap();
    stmt.execute(&[Value::Integer(2)]).unwrap();
    conn.execute("COMMIT").unwrap();
    let qr = conn.query("SELECT id FROM audit").unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(2)]]);
}

#[test]
fn delete_cascade_table_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE p (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE c (id INTEGER NOT NULL PRIMARY KEY, pid INTEGER \
         REFERENCES p(id) ON DELETE CASCADE)",
    )
    .unwrap();
    conn.execute("CREATE INDEX c_pid ON c (pid)").unwrap();
    conn.execute("INSERT INTO p VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO c VALUES (10, 1), (20, 2)")
        .unwrap();

    let stmt = conn.prepare("DELETE FROM p WHERE id = $1").unwrap();
    stmt.execute(&[Value::Integer(1)]).unwrap();
    let qr = conn.query("SELECT id FROM c ORDER BY id").unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(20)]]);
}

#[test]
fn create_index_between_executes_recompiles() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn, 4);

    let stmt = conn.prepare("DELETE FROM t WHERE id = $1").unwrap();
    assert_eq!(stmt.execute(&[Value::Integer(1)]).unwrap(), 1);
    conn.execute("CREATE INDEX t_val ON t (val)").unwrap();
    assert_eq!(stmt.execute(&[Value::Integer(2)]).unwrap(), 1);
    let qr = conn
        .query("SELECT id FROM t WHERE val = 0 ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(3)]]);
}

#[test]
fn delete_savepoint_rollback_restores_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn, 5);

    let stmt = conn.prepare("DELETE FROM t WHERE id = $1").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT s").unwrap();
    stmt.execute(&[Value::Integer(2)]).unwrap();
    stmt.execute(&[Value::Integer(3)]).unwrap();
    conn.execute("ROLLBACK TO s").unwrap();
    stmt.execute(&[Value::Integer(4)]).unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(ids(&conn), vec![1, 2, 3, 5]);
}

#[test]
fn composite_pk_delete_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t2 (a INTEGER NOT NULL, b INTEGER NOT NULL, PRIMARY KEY (a, b))")
        .unwrap();
    conn.execute("INSERT INTO t2 VALUES (1, 1), (1, 2), (2, 1)")
        .unwrap();
    let stmt = conn
        .prepare("DELETE FROM t2 WHERE a = $1 AND b = $2")
        .unwrap();
    assert_eq!(
        stmt.execute(&[Value::Integer(1), Value::Integer(2)])
            .unwrap(),
        1
    );
    let qr = conn.query("SELECT COUNT(*) FROM t2").unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(2)]]);
}

#[test]
fn overflow_row_delete_returning() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE big (id INTEGER NOT NULL PRIMARY KEY, blob TEXT)")
        .unwrap();
    let payload = "z".repeat(20_000);
    conn.execute(&format!("INSERT INTO big VALUES (1, '{payload}')"))
        .unwrap();

    let stmt = conn
        .prepare("DELETE FROM big WHERE id = $1 RETURNING blob")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let qr = stmt.query_collect(&[Value::Integer(1)]).unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Text(payload.into())]]);
    let qr = conn.query("SELECT COUNT(*) FROM big").unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(0)]]);
}
