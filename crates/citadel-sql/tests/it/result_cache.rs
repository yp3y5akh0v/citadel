//! Integration tests for the generation-keyed result cache: repeated
//! deterministic reads may be memoized, but every observable behavior must be
//! indistinguishable from fresh execution.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"result-cache")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn setup(conn: &Connection) {
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
}

#[test]
fn write_invalidates_cached_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);

    let sum = conn.prepare("SELECT SUM(v) FROM t").unwrap();
    assert_eq!(
        sum.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(60)]]
    );
    // Warm hit.
    assert_eq!(
        sum.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(60)]]
    );
    conn.execute("INSERT INTO t VALUES (4, 40)").unwrap();
    assert_eq!(
        sum.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(100)]]
    );
    conn.execute("UPDATE t SET v = 0 WHERE id = 1").unwrap();
    assert_eq!(
        sum.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(90)]]
    );
    conn.execute("DELETE FROM t WHERE id = 4").unwrap();
    assert_eq!(
        sum.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(50)]]
    );
}

#[test]
fn cross_connection_write_invalidates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn1 = Connection::open(&db).unwrap();
    setup(&conn1);
    let conn2 = Connection::open(&db).unwrap();

    let sum = conn1.prepare("SELECT SUM(v) FROM t").unwrap();
    assert_eq!(
        sum.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(60)]]
    );
    conn2.execute("INSERT INTO t VALUES (9, 100)").unwrap();
    assert_eq!(
        sum.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(160)]]
    );
}

#[test]
fn in_txn_reads_see_own_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);

    let sum = conn.prepare("SELECT SUM(v) FROM t").unwrap();
    // Warm the cache outside the txn.
    assert_eq!(
        sum.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(60)]]
    );

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 40)").unwrap();
    // Uncommitted write must be visible; the memo must not serve 60.
    assert_eq!(
        sum.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(100)]]
    );
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(
        sum.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(60)]]
    );
}

#[test]
fn params_key_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);

    let over = conn
        .prepare("SELECT COUNT(*) FROM t WHERE v >= $1")
        .unwrap();
    for (param, expected) in [(10, 3), (20, 2), (30, 1), (10, 3), (20, 2)] {
        assert_eq!(
            over.query_collect(&[Value::Integer(param)]).unwrap().rows,
            vec![vec![Value::Integer(expected)]],
            "param {param}"
        );
    }
}

#[test]
fn volatile_functions_bypass_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let r = conn.prepare("SELECT RANDOM()").unwrap();
    let a = r.query_collect(&[]).unwrap().rows[0][0].clone();
    let b = r.query_collect(&[]).unwrap().rows[0][0].clone();
    assert_ne!(a, b, "two RANDOM() calls served an identical memo");
}

#[test]
fn view_reads_invalidate_on_base_write() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute("CREATE VIEW big AS SELECT id, v FROM t WHERE v >= 20")
        .unwrap();

    let cnt = conn.prepare("SELECT COUNT(*) FROM big").unwrap();
    assert_eq!(
        cnt.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(2)]]
    );
    conn.execute("INSERT INTO t VALUES (4, 25)").unwrap();
    assert_eq!(
        cnt.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(3)]]
    );
}

#[test]
fn cached_and_fresh_results_are_identical() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);

    let stmt = conn
        .prepare("SELECT id, v FROM t WHERE v > 5 ORDER BY v DESC LIMIT 2")
        .unwrap();
    let first = stmt.query_collect(&[]).unwrap();
    let warm = stmt.query_collect(&[]).unwrap();
    assert_eq!(first.columns, warm.columns);
    assert_eq!(first.rows, warm.rows);

    let fresh_conn = Connection::open(&db).unwrap();
    let fresh = fresh_conn
        .query("SELECT id, v FROM t WHERE v > 5 ORDER BY v DESC LIMIT 2")
        .unwrap();
    assert_eq!(fresh.rows, warm.rows);
}

#[test]
fn cte_and_window_shapes_are_cached_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);

    let cte = conn
        .prepare("WITH big AS (SELECT v FROM t WHERE v >= 20) SELECT SUM(v) FROM big")
        .unwrap();
    assert_eq!(
        cte.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(50)]]
    );
    assert_eq!(
        cte.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(50)]]
    );
    conn.execute("INSERT INTO t VALUES (4, 40)").unwrap();
    assert_eq!(
        cte.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(90)]]
    );

    let win = conn
        .prepare("SELECT id, RANK() OVER (ORDER BY v DESC) FROM t ORDER BY id")
        .unwrap();
    let r1 = win.query_collect(&[]).unwrap().rows;
    let r2 = win.query_collect(&[]).unwrap().rows;
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 4);
}

#[test]
fn ddl_after_caching_recompiles() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);

    let all = conn.prepare("SELECT COUNT(*) FROM t WHERE v > 0").unwrap();
    assert_eq!(
        all.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(3)]]
    );
    conn.execute("ALTER TABLE t ADD COLUMN w INTEGER").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 40, 1)").unwrap();
    assert_eq!(
        all.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Integer(4)]]
    );
}
