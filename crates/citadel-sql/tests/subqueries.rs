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

fn query(conn: &Connection, sql: &str) -> QueryResult {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

fn setup_two_tables(conn: &Connection) {
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

#[test]
fn in_subquery_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(&conn, "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)");
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
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(
        &conn,
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
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(
        &conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE val > 9999)",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn not_in_subquery_empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(
        &conn,
        "SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2 WHERE val > 9999)",
    );
    assert_eq!(qr.rows.len(), 5);
}

#[test]
fn in_subquery_with_where_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(
        &conn,
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

#[test]
fn in_subquery_with_null_in_subquery_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

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
        &conn,
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
    let conn = Connection::open(&db).unwrap();

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
        &conn,
        "SELECT id FROM a WHERE id NOT IN (SELECT val FROM b)",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn null_lhs_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

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

    let qr = query(&conn, "SELECT id FROM t WHERE val IN (SELECT id FROM s)");
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
    let conn = Connection::open(&db).unwrap();

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
        &conn,
        "SELECT id FROM t WHERE id NOT IN (SELECT val FROM s)",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn in_empty_subquery_with_null_lhs() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

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

    let qr = query(&conn, "SELECT id FROM t WHERE val IN (SELECT id FROM s)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn scalar_subquery_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(
        &conn,
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
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(
        &conn,
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
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(
        &conn,
        "SELECT id FROM t1 WHERE val = (SELECT val FROM t2 WHERE id = 999)",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn scalar_subquery_multiple_rows_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let result = conn.execute("SELECT id FROM t1 WHERE val = (SELECT val FROM t2)");
    assert!(matches!(result, Err(SqlError::SubqueryMultipleRows)));
}

#[test]
fn exists_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(
        &conn,
        "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = 2)",
    );
    assert_eq!(qr.rows.len(), 5);
}

#[test]
fn not_exists_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(
        &conn,
        "SELECT id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = 999)",
    );
    assert_eq!(qr.rows.len(), 5);
}

#[test]
fn exists_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

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

    let qr = query(&conn, "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn exists_never_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

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

    let qr = query(&conn, "SELECT id FROM t1 WHERE EXISTS (SELECT val FROM t2)");
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn in_list_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(&conn, "SELECT id FROM t1 WHERE id IN (1, 3, 5)");
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
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(&conn, "SELECT id FROM t1 WHERE id NOT IN (1, 3, 5)");
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
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(&conn, "SELECT id FROM t1 WHERE id IN (1, NULL, 3)");
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
fn in_subquery_multiple_columns_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let result = conn.execute("SELECT id FROM t1 WHERE id IN (SELECT id, val FROM t2)");
    assert!(matches!(result, Err(SqlError::SubqueryMultipleColumns)));
}

#[test]
fn subquery_table_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let result = conn.execute("SELECT id FROM t1 WHERE id IN (SELECT id FROM nonexistent)");
    assert!(matches!(result, Err(SqlError::TableNotFound(_))));
}

#[test]
fn in_subquery_with_order_by_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    let qr = query(
        &conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2) ORDER BY id DESC LIMIT 1",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(4));
}

#[test]
fn in_subquery_with_group_by_having() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

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

    let qr = query(&conn,
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
    let conn = Connection::open(&db).unwrap();

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

    let qr = query(&conn,
        "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id IN (SELECT id FROM vip)"
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
}

#[test]
fn subquery_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    conn.execute("BEGIN").unwrap();
    let qr = query(&conn, "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)");
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
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    assert_rows_affected(
        conn.execute("DELETE FROM t1 WHERE id IN (SELECT id FROM t2)")
            .unwrap(),
        2,
    );

    let qr = query(&conn, "SELECT id FROM t1 ORDER BY id");
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
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    assert_rows_affected(
        conn.execute("UPDATE t1 SET val = (SELECT MAX(val) FROM t2) WHERE id = 1")
            .unwrap(),
        1,
    );

    let qr = query(&conn, "SELECT val FROM t1 WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(600));
}

#[test]
fn update_where_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);

    assert_rows_affected(
        conn.execute("UPDATE t1 SET val = 999 WHERE id IN (SELECT id FROM t2)")
            .unwrap(),
        2,
    );

    let qr = query(&conn, "SELECT id, val FROM t1 WHERE val = 999 ORDER BY id");
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(4));
}

#[test]
fn persistence_after_subquery_operations() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    {
        let conn = Connection::open(&db).unwrap();
        setup_two_tables(&conn);
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
    let conn = Connection::open(&db).unwrap();
    let qr = query(&conn, "SELECT id FROM t1 ORDER BY id");
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

#[test]
fn join_clauses_run_their_subqueries() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);
    let cases: [(&str, &[i64]); 3] = [
        (
            "SELECT t1.id FROM t1 JOIN t2 \
             ON t1.id = t2.id AND t2.id IN (SELECT id FROM t1 WHERE val > 25)",
            &[4],
        ),
        (
            "SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id \
             ORDER BY (SELECT COUNT(*) FROM t2) - t1.id",
            &[4, 2],
        ),
        (
            "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.id = t2.id \
             HAVING COUNT(*) > (SELECT COUNT(*) FROM t2 WHERE id > 5)",
            &[2],
        ),
    ];
    let ints = |rows: &[Vec<Value>]| -> Vec<i64> {
        rows.iter()
            .map(|row| match &row[0] {
                Value::Integer(value) => *value,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect()
    };
    for (sql, expected) in cases {
        assert_eq!(ints(&conn.query(sql).unwrap().rows), expected, "{sql}");
        let prepared = conn.prepare(sql).unwrap().query_collect(&[]).unwrap();
        assert_eq!(ints(&prepared.rows), expected, "prepared {sql}");
        conn.execute("BEGIN").unwrap();
        assert_eq!(
            ints(&conn.query(sql).unwrap().rows),
            expected,
            "transaction {sql}"
        );
        conn.execute("ROLLBACK").unwrap();
    }
}

#[test]
fn quantified_subqueries_compare_against_every_selected_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_two_tables(&conn);
    conn.execute("CREATE TABLE n (id INTEGER NOT NULL PRIMARY KEY, name TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO n VALUES (1, 'abc')").unwrap();
    let rows = |sql: &str| -> Vec<Vec<Value>> {
        let execute = conn
            .query(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .rows;
        let prepared = conn.prepare(sql).unwrap().query_collect(&[]).unwrap().rows;
        conn.execute("BEGIN").unwrap();
        let transaction = conn.query(sql).unwrap().rows;
        conn.execute("ROLLBACK").unwrap();
        assert_eq!(prepared, execute, "prepared {sql}");
        assert_eq!(transaction, execute, "transaction {sql}");
        execute
    };
    let ids = |values: &[i64]| -> Vec<Vec<Value>> {
        values
            .iter()
            .map(|&value| vec![Value::Integer(value)])
            .collect()
    };
    let flags = |values: &[Option<bool>]| -> Vec<Vec<Value>> {
        vec![values
            .iter()
            .map(|value| value.map_or(Value::Null, Value::Boolean))
            .collect()]
    };

    assert_eq!(
        rows("SELECT id FROM t1 WHERE id = ANY (SELECT id FROM t2) ORDER BY id"),
        ids(&[2, 4])
    );
    assert_eq!(
        rows("SELECT id FROM t1 WHERE id > ALL (SELECT id FROM t2 WHERE id < 5) ORDER BY id"),
        ids(&[5])
    );
    // Over no rows ANY is false and ALL is true.
    assert_eq!(
        rows("SELECT id FROM t1 WHERE id = ANY (SELECT id FROM t2 WHERE id > 100)"),
        ids(&[])
    );
    assert_eq!(
        rows("SELECT COUNT(*) FROM t1 WHERE id > ALL (SELECT id FROM t2 WHERE id > 100)"),
        ids(&[5])
    );
    assert_eq!(
        rows(
            "SELECT 2 = ANY (SELECT id FROM t2), 3 = ANY (SELECT id FROM t2), \
             3 <> ALL (SELECT id FROM t2), 1 = ANY (SELECT NULL)"
        ),
        flags(&[Some(true), Some(false), Some(true), None])
    );
    // Each outer row compares against its own subquery rows.
    assert_eq!(
        rows(
            "SELECT id FROM t1 WHERE val * 10 > ALL \
             (SELECT t2.val FROM t2 WHERE t2.id <= t1.id) ORDER BY id"
        ),
        ids(&[1, 3, 5])
    );
    // The selected column's collation applies unless the left side has its own.
    assert_eq!(
        rows(
            "SELECT 'ABC' = ANY (SELECT name FROM n), \
             'ABC' COLLATE BINARY = ANY (SELECT name FROM n)"
        ),
        flags(&[Some(true), Some(false)])
    );
    assert_eq!(
        rows("SELECT id FROM n WHERE name = ANY (SELECT 'ABC')"),
        ids(&[1])
    );
    assert_eq!(
        rows("SELECT id FROM n WHERE name = ANY (ARRAY['ABC'])"),
        ids(&[1])
    );
    let error = conn
        .query("SELECT 1 = ANY (SELECT id, val FROM t2)")
        .unwrap_err();
    assert!(
        matches!(error, SqlError::SubqueryMultipleColumns),
        "{error}"
    );

    for transaction in [false, true] {
        if transaction {
            conn.execute("BEGIN").unwrap();
        }
        assert_rows_affected(
            conn.execute("UPDATE t1 SET val = -val WHERE id = ANY (SELECT id FROM t2)")
                .unwrap(),
            2,
        );
        assert_rows_affected(
            conn.execute("DELETE FROM t1 WHERE val < ALL (SELECT val FROM t1 WHERE val > 0)")
                .unwrap(),
            2,
        );
        assert_eq!(
            conn.query("SELECT id FROM t1 ORDER BY id").unwrap().rows,
            ids(&[1, 3, 5]),
            "transaction {transaction}"
        );
        if transaction {
            conn.execute("ROLLBACK").unwrap();
        } else {
            conn.execute("UPDATE t1 SET val = id * 10").unwrap();
            conn.execute("INSERT INTO t1 VALUES (2, 20), (4, 40)")
                .unwrap();
        }
    }
}

#[test]
fn subquery_keys_and_members_compare_as_equals_does() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // `=` converts a DATE to the TIMESTAMP at its midnight and TEXT to a date,
    // and compares intervals by length: none of these pairs is the same value.
    for sql in [
        "CREATE TABLE days (id INTEGER PRIMARY KEY, d DATE)",
        "INSERT INTO days VALUES (1, '2024-01-01'), (2, '2024-01-02')",
        "CREATE TABLE moments (id INTEGER PRIMARY KEY, t TIMESTAMP, tag INTEGER)",
        "INSERT INTO moments VALUES (5, '2024-01-01 00:00:00', 1), (6, '2024-01-02 12:00:00', 2)",
        "CREATE TABLE texts (id INTEGER PRIMARY KEY, s TEXT)",
        "INSERT INTO texts VALUES (9, '2024-01-01')",
        "CREATE TABLE ia (id INTEGER PRIMARY KEY, v INTERVAL)",
        "INSERT INTO ia VALUES (1, INTERVAL '1 month')",
        "CREATE TABLE ib (id INTEGER PRIMARY KEY, v INTERVAL)",
        "INSERT INTO ib VALUES (7, INTERVAL '30 days'), (8, INTERVAL '1 month')",
    ] {
        conn.execute(sql).unwrap();
    }
    let int = |value: i64| Value::Integer(value);
    let cases: [(&str, Vec<Vec<Value>>); 13] = [
        (
            "SELECT id FROM days WHERE EXISTS (SELECT 1 FROM moments WHERE moments.t = days.d)",
            vec![vec![int(1)]],
        ),
        (
            "SELECT id FROM days WHERE NOT EXISTS (SELECT 1 FROM moments WHERE moments.t = days.d)",
            vec![vec![int(2)]],
        ),
        (
            "SELECT id FROM days WHERE EXISTS \
             (SELECT 1 FROM moments WHERE moments.t = days.d AND moments.id > days.id)",
            vec![vec![int(1)]],
        ),
        (
            "SELECT id FROM days WHERE EXISTS (SELECT 1 FROM texts WHERE texts.s = days.d)",
            vec![vec![int(1)]],
        ),
        (
            "SELECT days.id, (SELECT moments.id FROM moments WHERE moments.t = days.d) \
             FROM days ORDER BY 1",
            vec![vec![int(1), int(5)], vec![int(2), Value::Null]],
        ),
        (
            "SELECT ia.id, (SELECT COUNT(*) FROM ib WHERE ib.v = ia.v) FROM ia",
            vec![vec![int(1), int(2)]],
        ),
        (
            "SELECT id FROM ia WHERE 2 = (SELECT COUNT(*) FROM ib WHERE ib.v = ia.v)",
            vec![vec![int(1)]],
        ),
        (
            "SELECT id FROM days WHERE id IN (SELECT tag FROM moments WHERE moments.t = days.d)",
            vec![vec![int(1)]],
        ),
        (
            "SELECT id FROM days WHERE id NOT IN \
             (SELECT tag FROM moments WHERE moments.t = days.d) ORDER BY 1",
            vec![vec![int(2)]],
        ),
        (
            "SELECT id FROM days WHERE d IN (SELECT t FROM moments)",
            vec![vec![int(1)]],
        ),
        (
            "SELECT id FROM days WHERE d NOT IN (SELECT t FROM moments)",
            vec![vec![int(2)]],
        ),
        (
            "SELECT id FROM days WHERE d IN (SELECT s FROM texts)",
            vec![vec![int(1)]],
        ),
        (
            "SELECT id FROM ia WHERE v IN (SELECT v FROM ib WHERE id = 7)",
            vec![vec![int(1)]],
        ),
    ];
    for transaction in [false, true] {
        if transaction {
            assert_ok(conn.execute("BEGIN").unwrap());
        }
        for (sql, expected) in &cases {
            assert_eq!(
                &conn.query(sql).unwrap().rows,
                expected,
                "{sql} (transaction: {transaction})"
            );
        }
        if transaction {
            assert_rows_affected(
                conn.execute(
                    "DELETE FROM days WHERE EXISTS (SELECT 1 FROM moments WHERE moments.t = days.d)",
                )
                .unwrap(),
                1,
            );
            assert_rows_affected(
                conn.execute("UPDATE days SET d = d WHERE d NOT IN (SELECT t FROM moments)")
                    .unwrap(),
                1,
            );
            assert_ok(conn.execute("ROLLBACK").unwrap());
        }
    }
}
