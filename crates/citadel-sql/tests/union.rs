use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn setup_two_tables(conn: &Connection) {
    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        .unwrap();
    conn.execute("INSERT INTO t2 (id, name) VALUES (2, 'Bob'), (3, 'Carol'), (4, 'Dave')")
        .unwrap();
}

#[test]
fn union_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = conn
        .query("SELECT id, name FROM t1 UNION SELECT id, name FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 4);
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[test]
fn union_all_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = conn
        .query("SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 6);
}

#[test]
fn intersect_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = conn
        .query("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 3]);
}

#[test]
fn intersect_all_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2), (3)")
        .unwrap();
    conn.execute("INSERT INTO t2 (v) VALUES (2), (3), (4)")
        .unwrap();

    let qr = conn
        .query("SELECT v FROM t1 INTERSECT ALL SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn except_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = conn
        .query("SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn except_all_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2), (3)")
        .unwrap();
    conn.execute("INSERT INTO t2 (v) VALUES (2)").unwrap();

    let qr = conn
        .query("SELECT v FROM t1 EXCEPT ALL SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    let mut vals: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        })
        .collect();
    vals.sort();
    assert_eq!(vals, vec![1, 3]);
}

#[test]
fn union_order_by_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = conn
        .query("SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id LIMIT 3")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
}

#[test]
fn union_column_count_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let err = conn
        .execute("SELECT id FROM t1 UNION SELECT id, name FROM t2")
        .unwrap_err();
    assert!(
        matches!(
            err,
            SqlError::CompoundColumnCountMismatch { left: 1, right: 2 }
        ),
        "expected CompoundColumnCountMismatch, got {err:?}"
    );
}

#[test]
fn union_three_way() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t3 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO t2 (v) VALUES (2), (3)").unwrap();
    conn.execute("INSERT INTO t3 (v) VALUES (3), (4)").unwrap();

    let qr = conn
        .query("SELECT v FROM t1 UNION SELECT v FROM t2 UNION SELECT v FROM t3")
        .unwrap();
    assert_eq!(qr.rows.len(), 4);
}

#[test]
fn intersect_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t3 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO t2 (v) VALUES (2), (3)").unwrap();
    conn.execute("INSERT INTO t3 (v) VALUES (3), (4)").unwrap();

    // INTERSECT binds tighter: A UNION (B INTERSECT C) = {1,2} UNION {3} = {1,2,3}
    let qr = conn
        .query("SELECT v FROM t1 UNION SELECT v FROM t2 INTERSECT SELECT v FROM t3")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    let mut vals: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        })
        .collect();
    vals.sort();
    assert_eq!(vals, vec![1, 2, 3]);
}

#[test]
fn union_with_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = conn
        .query("SELECT id, name FROM t1 WHERE id = 1 UNION SELECT id, name FROM t2 WHERE id = 4")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 4]);
}

#[test]
fn union_different_column_names() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (a INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (b INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (a) VALUES (1)").unwrap();
    conn.execute("INSERT INTO t2 (b) VALUES (2)").unwrap();

    let qr = conn
        .query("SELECT a AS col_left FROM t1 UNION ALL SELECT b AS col_right FROM t2")
        .unwrap();
    assert_eq!(qr.columns, vec!["col_left"]);
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn except_empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2)").unwrap();

    let qr = conn
        .query("SELECT v FROM t1 EXCEPT SELECT v FROM t1")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn union_with_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (id, v) VALUES (1, NULL)")
        .unwrap();
    conn.execute("INSERT INTO t2 (id, v) VALUES (2, NULL)")
        .unwrap();

    let qr = conn
        .query("SELECT id, v FROM t1 UNION SELECT id, v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);

    // NULL = NULL for set dedup
    let qr = conn
        .query("SELECT v FROM t1 UNION SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn insert_select_union() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );

    let result = conn
        .execute("INSERT INTO dst SELECT id, name FROM t1 UNION SELECT id, name FROM t2")
        .unwrap();
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, 4),
        other => panic!("expected RowsAffected(4), got {other:?}"),
    }

    let qr = conn.query("SELECT COUNT(*) FROM dst").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(4));
}

#[test]
fn union_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = conn
        .query_params(
            "SELECT id, name FROM t1 WHERE id > $1 UNION SELECT id, name FROM t2 WHERE id > $1",
            &[Value::Integer(2)],
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![3, 4]);
}

#[test]
fn union_distinct_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = conn
        .query("SELECT id, name FROM t1 UNION DISTINCT SELECT id, name FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 4);
}

#[test]
fn union_with_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2), (3)")
        .unwrap();
    let qr = conn
        .query("SELECT v FROM t1 UNION SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);

    let qr = conn
        .query("SELECT v FROM t2 UNION SELECT v FROM t1")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);

    let qr = conn
        .query("SELECT v FROM t1 INTERSECT SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);

    let qr = conn
        .query("SELECT v FROM t1 EXCEPT SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn union_order_by_desc() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = conn
        .query("SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id DESC")
        .unwrap();
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][0], Value::Integer(4));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
    assert_eq!(qr.rows[2][0], Value::Integer(2));
    assert_eq!(qr.rows[3][0], Value::Integer(1));
}

#[test]
fn except_not_commutative() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = conn
        .query("SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));

    let qr = conn
        .query("SELECT id, name FROM t2 EXCEPT SELECT id, name FROM t1")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(4));
}

#[test]
fn union_insert_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO dst SELECT id, name FROM t1 UNION SELECT id, name FROM t2")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM dst").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

#[test]
fn union_in_subquery_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );

    let err = conn
        .execute("SELECT * FROM t1 WHERE v IN (SELECT v FROM t1 UNION SELECT v FROM t1)")
        .unwrap_err();
    assert!(
        err.to_string().contains("UNION")
            || err.to_string().contains("unsupported")
            || err.to_string().contains("Unsupported"),
        "expected UNION-in-subquery error, got: {err}"
    );
}
