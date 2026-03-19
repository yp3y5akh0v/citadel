use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, SqlError, Value};

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

fn assert_rows_affected(result: ExecutionResult, expected: u64) {
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn query(conn: &mut Connection, sql: &str) -> QueryResult {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

fn setup_two_tables(conn: &mut Connection) {
    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
            .unwrap(),
        5,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t2 (id, val) VALUES (2, 200), (4, 400), (6, 600)")
            .unwrap(),
        3,
    );
}

// ── IN subquery basics ────────────────────────────────────────────

#[test]
fn in_subquery_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)",
    );
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected int, got {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 4]);
}

#[test]
fn not_in_subquery_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2)",
    );
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 3, 5]);
}

#[test]
fn in_subquery_empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE val > 9999)",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn not_in_subquery_empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2 WHERE val > 9999)",
    );
    assert_eq!(qr.rows.len(), 5);
}

#[test]
fn in_subquery_with_where_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE val >= 400)",
    );
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![4]);
}

// ── NULL handling (three-valued logic) ────────────────────────────

#[test]
fn in_subquery_with_null_in_subquery_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, cat INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE cats (id INTEGER NOT NULL PRIMARY KEY, cat_id INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items (id, cat) VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO cats (id, cat_id) VALUES (1, 10), (2, NULL), (3, 30)")
            .unwrap(),
        3,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM items WHERE cat IN (SELECT cat_id FROM cats)",
    );
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn not_in_subquery_with_null_returns_zero_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO a (id) VALUES (1), (2), (3)")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO b (id, val) VALUES (1, 10), (2, NULL)")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM a WHERE id NOT IN (SELECT val FROM b)",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn null_lhs_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE s (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, NULL), (2, 10)")
            .unwrap(),
        2,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO s (id) VALUES (10), (20)")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE val IN (SELECT id FROM s)",
    );
    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![2]);
}

#[test]
fn not_in_all_null_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE s (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id) VALUES (1), (2)").unwrap(),
        2,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO s (id, val) VALUES (1, NULL), (2, NULL)")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE id NOT IN (SELECT val FROM s)",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn in_empty_subquery_with_null_lhs() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE s (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, NULL)")
            .unwrap(),
        1,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE val IN (SELECT id FROM s)",
    );
    assert_eq!(qr.rows.len(), 0);
}

// ── Scalar subquery ──────────────────────────────────────────────

#[test]
fn scalar_subquery_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE val > (SELECT MIN(val) FROM t1 WHERE id <= 2)",
    );
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 3, 4, 5]);
}

#[test]
fn scalar_subquery_in_projection() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id, (SELECT COUNT(*) FROM t2) FROM t1 WHERE id = 1",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(3));
}

#[test]
fn scalar_subquery_empty_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE val = (SELECT val FROM t2 WHERE id = 999)",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn scalar_subquery_multiple_rows_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let result = conn.execute("SELECT id FROM t1 WHERE val = (SELECT val FROM t2)");
    assert!(matches!(result, Err(SqlError::SubqueryMultipleRows)));
}

// ── EXISTS / NOT EXISTS ──────────────────────────────────────────

#[test]
fn exists_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = 2)",
    );
    assert_eq!(qr.rows.len(), 5);
}

#[test]
fn not_exists_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = 999)",
    );
    assert_eq!(qr.rows.len(), 5);
}

#[test]
fn exists_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 (id) VALUES (1), (2), (3)")
            .unwrap(),
        3,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2)",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn exists_never_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t1 (id) VALUES (1)").unwrap(), 1);
    assert_rows_affected(
        conn.execute("INSERT INTO t2 (id, val) VALUES (1, NULL)")
            .unwrap(),
        1,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE EXISTS (SELECT val FROM t2)",
    );
    assert_eq!(qr.rows.len(), 1);
}

// ── IN list (no subquery) ────────────────────────────────────────

#[test]
fn in_list_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(&mut conn, "SELECT id FROM t1 WHERE id IN (1, 3, 5)");
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 3, 5]);
}

#[test]
fn not_in_list_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(&mut conn, "SELECT id FROM t1 WHERE id NOT IN (1, 3, 5)");
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 4]);
}

#[test]
fn in_list_with_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(&mut conn, "SELECT id FROM t1 WHERE id IN (1, NULL, 3)");
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 3]);
}

// ── Validation ───────────────────────────────────────────────────

#[test]
fn in_subquery_multiple_columns_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let result = conn.execute("SELECT id FROM t1 WHERE id IN (SELECT id, val FROM t2)");
    assert!(matches!(result, Err(SqlError::SubqueryMultipleColumns)));
}

#[test]
fn subquery_table_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let result = conn.execute("SELECT id FROM t1 WHERE id IN (SELECT id FROM nonexistent)");
    assert!(matches!(result, Err(SqlError::TableNotFound(_))));
}

// ── Integration with other features ──────────────────────────────

#[test]
fn in_subquery_with_order_by_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2) ORDER BY id DESC LIMIT 1",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(4));
}

#[test]
fn in_subquery_with_group_by_having() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, product INTEGER NOT NULL, qty INTEGER NOT NULL)"
    ).unwrap());
    assert_ok(
        conn.execute("CREATE TABLE vip_products (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(conn.execute(
        "INSERT INTO orders (id, product, qty) VALUES (1, 1, 5), (2, 1, 10), (3, 2, 3), (4, 3, 7)"
    ).unwrap(), 4);
    assert_rows_affected(
        conn.execute("INSERT INTO vip_products (id) VALUES (1), (2)")
            .unwrap(),
        2,
    );

    let qr = query(&mut conn,
        "SELECT product, SUM(qty) FROM orders WHERE product IN (SELECT id FROM vip_products) GROUP BY product HAVING SUM(qty) > 5"
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(15));
}

#[test]
fn in_subquery_with_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL)",
        )
        .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE vip (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap(),
        2,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO orders (id, user_id) VALUES (10, 1), (11, 2)")
            .unwrap(),
        2,
    );
    assert_rows_affected(conn.execute("INSERT INTO vip (id) VALUES (1)").unwrap(), 1);

    let qr = query(&mut conn,
        "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id IN (SELECT id FROM vip)"
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
}

#[test]
fn subquery_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    conn.execute("BEGIN").unwrap();
    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)",
    );
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 4]);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn delete_with_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    assert_rows_affected(
        conn.execute("DELETE FROM t1 WHERE id IN (SELECT id FROM t2)")
            .unwrap(),
        2,
    );

    let qr = query(&mut conn, "SELECT id FROM t1 ORDER BY id");
    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![1, 3, 5]);
}

#[test]
fn update_with_scalar_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    assert_rows_affected(
        conn.execute("UPDATE t1 SET val = (SELECT MAX(val) FROM t2) WHERE id = 1")
            .unwrap(),
        1,
    );

    let qr = query(&mut conn, "SELECT val FROM t1 WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(600));
}

#[test]
fn update_where_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_two_tables(&mut conn);

    assert_rows_affected(
        conn.execute("UPDATE t1 SET val = 999 WHERE id IN (SELECT id FROM t2)")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT id, val FROM t1 WHERE val = 999 ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(4));
}

#[test]
fn persistence_after_subquery_operations() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    {
        let mut conn = Connection::open(&db).unwrap();
        setup_two_tables(&mut conn);
        assert_rows_affected(
            conn.execute("DELETE FROM t1 WHERE id NOT IN (SELECT id FROM t2)")
                .unwrap(),
            3,
        );
    }
    drop(db);

    let db_path = dir.path().join("test.db");
    let db = DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let mut conn = Connection::open(&db).unwrap();
    let qr = query(&mut conn, "SELECT id FROM t1 ORDER BY id");
    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![2, 4]);
}
