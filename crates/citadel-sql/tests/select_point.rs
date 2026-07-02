use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn prepared_point_select_varying_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER, s TEXT)")
        .unwrap();
    for id in 1..=5 {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({id}, {}, 's{id}')",
            id * 10
        ))
        .unwrap();
    }

    let stmt = conn.prepare("SELECT v, s FROM t WHERE id = $1").unwrap();
    for id in [3i64, 1, 5, 3] {
        let qr = stmt.query_collect(&[Value::Integer(id)]).unwrap();
        assert_eq!(
            qr.rows,
            vec![vec![
                Value::Integer(id * 10),
                Value::Text(format!("s{id}").into()),
            ]]
        );
    }
    let qr = stmt.query_collect(&[Value::Integer(99)]).unwrap();
    assert!(qr.rows.is_empty());
    assert_eq!(qr.columns, vec!["v", "s"]);
}

#[test]
fn point_select_composite_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (a INTEGER NOT NULL, b TEXT NOT NULL, v INTEGER, PRIMARY KEY (a, b))",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'x', 10), (1, 'y', 20), (2, 'x', 30)")
        .unwrap();

    let stmt = conn
        .prepare("SELECT v FROM t WHERE b = $2 AND a = $1")
        .unwrap();
    let qr = stmt
        .query_collect(&[Value::Integer(1), Value::Text("y".into())])
        .unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(20)]]);
    let qr = stmt
        .query_collect(&[Value::Integer(2), Value::Text("x".into())])
        .unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(30)]]);
}

#[test]
fn point_select_null_param_matches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let stmt = conn.prepare("SELECT v FROM t WHERE id = $1").unwrap();
    let qr = stmt.query_collect(&[Value::Null]).unwrap();
    assert!(qr.rows.is_empty());
}

#[test]
fn point_select_read_your_writes_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let sel = conn.prepare("SELECT v FROM t WHERE id = $1").unwrap();
    let upd = conn.prepare("UPDATE t SET v = $2 WHERE id = $1").unwrap();
    conn.execute("BEGIN").unwrap();
    upd.execute(&[Value::Integer(1), Value::Integer(77)])
        .unwrap();
    let qr = sel.query_collect(&[Value::Integer(1)]).unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(77)]]);
    conn.execute("ROLLBACK").unwrap();
    let qr = sel.query_collect(&[Value::Integer(1)]).unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(10)]]);
}

#[test]
fn point_select_sees_committed_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let stmt = conn.prepare("SELECT v FROM t WHERE id = $1").unwrap();
    assert_eq!(
        stmt.query_collect(&[Value::Integer(1)]).unwrap().rows,
        vec![vec![Value::Integer(10)]]
    );
    conn.execute("UPDATE t SET v = 42 WHERE id = 1").unwrap();
    assert_eq!(
        stmt.query_collect(&[Value::Integer(1)]).unwrap().rows,
        vec![vec![Value::Integer(42)]]
    );
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    assert!(stmt
        .query_collect(&[Value::Integer(1)])
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn point_select_star_and_expr_projection() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (7, 6)").unwrap();

    let stmt = conn.prepare("SELECT * FROM t WHERE id = $1").unwrap();
    let qr = stmt.query_collect(&[Value::Integer(7)]).unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(7), Value::Integer(6)]]);

    let stmt = conn.prepare("SELECT v * 2 FROM t WHERE id = $1").unwrap();
    let qr = stmt.query_collect(&[Value::Integer(7)]).unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(12)]]);
}

#[test]
fn point_select_residual_conjunct_filters() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let stmt = conn
        .prepare("SELECT v FROM t WHERE id = $1 AND v > 15")
        .unwrap();
    assert!(stmt
        .query_collect(&[Value::Integer(1)])
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        stmt.query_collect(&[Value::Integer(2)]).unwrap().rows,
        vec![vec![Value::Integer(20)]]
    );
}

#[test]
fn point_select_drop_column_recompiles() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER, w INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, 100)").unwrap();

    let stmt = conn.prepare("SELECT v FROM t WHERE id = $1").unwrap();
    assert_eq!(
        stmt.query_collect(&[Value::Integer(1)]).unwrap().rows,
        vec![vec![Value::Integer(10)]]
    );
    conn.execute("ALTER TABLE t DROP COLUMN w").unwrap();
    assert_eq!(
        stmt.query_collect(&[Value::Integer(1)]).unwrap().rows,
        vec![vec![Value::Integer(10)]]
    );
}
