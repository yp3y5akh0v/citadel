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

fn setup_users_orders(conn: &mut Connection) {
    assert_ok(
        conn.execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, amount REAL NOT NULL)"
    ).unwrap());

    assert_rows_affected(
        conn.execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        )
        .unwrap(),
        3,
    );

    assert_rows_affected(conn.execute(
        "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 50.0), (11, 1, 30.0), (12, 2, 100.0)"
    ).unwrap(), 3);
}

#[test]
fn basic_inner_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(&mut conn,
        "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY o.id"
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("Alice".into()), Value::Real(50.0)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("Alice".into()), Value::Real(30.0)]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Text("Bob".into()), Value::Real(100.0)]
    );
}

#[test]
fn inner_join_excludes_non_matching() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id",
    );
    let names: Vec<&Value> = qr.rows.iter().map(|r| &r[0]).collect();
    assert!(!names.contains(&&Value::Text("Charlie".into())));
}

#[test]
fn join_bare_keyword_is_inner() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id",
    );
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn one_to_many_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(&mut conn,
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.name = 'Alice' ORDER BY o.amount"
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Real(30.0));
    assert_eq!(qr.rows[1][1], Value::Real(50.0));
}

#[test]
fn cross_join_cartesian_product() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (x INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (y INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO a (x) VALUES (1), (2)").unwrap(),
        2,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO b (y) VALUES (10), (20), (30)")
            .unwrap(),
        3,
    );

    let qr = query(
        &mut conn,
        "SELECT a.x, b.y FROM a CROSS JOIN b ORDER BY a.x, b.y",
    );
    assert_eq!(qr.rows.len(), 6);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(10)]);
    assert_eq!(qr.rows[5], vec![Value::Integer(2), Value::Integer(30)]);
}

#[test]
fn left_join_includes_unmatched() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(&mut conn,
        "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id, o.id"
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("Alice".into()), Value::Real(50.0)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("Alice".into()), Value::Real(30.0)]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Text("Bob".into()), Value::Real(100.0)]
    );
    assert_eq!(qr.rows[3], vec![Value::Text("Charlie".into()), Value::Null]);
}

#[test]
fn self_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE employees (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, manager_id INTEGER)"
    ).unwrap());
    assert_rows_affected(conn.execute(
        "INSERT INTO employees (id, name, manager_id) VALUES (1, 'Boss', NULL), (2, 'Alice', 1), (3, 'Bob', 1)"
    ).unwrap(), 3);

    let qr = query(&mut conn,
        "SELECT e.name, m.name FROM employees e JOIN employees m ON e.manager_id = m.id ORDER BY e.id"
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("Alice".into()), Value::Text("Boss".into())]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("Bob".into()), Value::Text("Boss".into())]
    );
}

#[test]
fn three_table_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE customers (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
        )
        .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE products (id INTEGER NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
        )
        .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE purchases (id INTEGER NOT NULL PRIMARY KEY, cust_id INTEGER NOT NULL, prod_id INTEGER NOT NULL)"
    ).unwrap());

    assert_rows_affected(
        conn.execute("INSERT INTO customers (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap(),
        2,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO products (id, title) VALUES (100, 'Widget'), (200, 'Gadget')")
            .unwrap(),
        2,
    );
    assert_rows_affected(conn.execute(
        "INSERT INTO purchases (id, cust_id, prod_id) VALUES (1, 1, 100), (2, 1, 200), (3, 2, 100)"
    ).unwrap(), 3);

    let qr = query(
        &mut conn,
        "SELECT c.name, p.title FROM customers c \
         JOIN purchases pu ON c.id = pu.cust_id \
         JOIN products p ON pu.prod_id = p.id \
         ORDER BY c.name, p.title",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("Alice".into()), Value::Text("Gadget".into())]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("Alice".into()), Value::Text("Widget".into())]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Text("Bob".into()), Value::Text("Widget".into())]
    );
}

#[test]
fn join_with_where_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(&mut conn,
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 40.0 ORDER BY o.amount"
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Real(50.0));
    assert_eq!(qr.rows[1][1], Value::Real(100.0));
}

#[test]
fn join_with_order_limit_offset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(&mut conn,
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount LIMIT 2 OFFSET 1"
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Real(50.0));
    assert_eq!(qr.rows[1][1], Value::Real(100.0));
}

#[test]
fn join_with_aggregation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(&mut conn,
        "SELECT u.name, COUNT(*), SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name"
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(2));
    assert_eq!(qr.rows[0][2], Value::Real(80.0));
    assert_eq!(qr.rows[1][0], Value::Text("Bob".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(1));
    assert_eq!(qr.rows[1][2], Value::Real(100.0));
}

#[test]
fn join_with_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT DISTINCT u.name FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.name",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Bob".into()));
}

#[test]
fn qualified_column_resolution() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(10)]);
}

#[test]
fn unqualified_unique_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT name, amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][1], Value::Real(50.0));
}

#[test]
fn ambiguous_column_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let result = conn.execute("SELECT id FROM users u JOIN orders o ON u.id = o.user_id");
    assert!(matches!(result, Err(SqlError::AmbiguousColumn(_))));
}

#[test]
fn select_star_with_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.columns.len(), 5);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(10));
    assert_eq!(qr.rows[0][3], Value::Integer(1));
    assert_eq!(qr.rows[0][4], Value::Real(50.0));
}

#[test]
fn join_on_non_pk_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20)")
            .unwrap(),
        2,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t2 (id, val) VALUES (100, 10), (200, 30)")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.val = t2.val",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(100)]);
}

#[test]
fn join_null_equality_excludes_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 (id, ref_id) VALUES (1, NULL), (2, 100)")
            .unwrap(),
        2,
    );
    assert_rows_affected(conn.execute("INSERT INTO t2 (id) VALUES (100)").unwrap(), 1);

    let qr = query(
        &mut conn,
        "SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.ref_id = t2.id",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0], vec![Value::Integer(2), Value::Integer(100)]);
}

#[test]
fn join_with_empty_table() {
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
        conn.execute("INSERT INTO t1 (id) VALUES (1), (2)").unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.id = t2.id",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn join_with_complex_on_clause() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, x INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, y INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 (id, x) VALUES (1, 10), (2, 20), (3, 5)")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t2 (id, y) VALUES (100, 10), (200, 20)")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.x = t2.y AND t1.x > 5 ORDER BY t1.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(100)]);
    assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Integer(200)]);
}

#[test]
fn join_same_table_twice() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items (id, name) VALUES (1, 'A'), (2, 'B'), (3, 'C')")
            .unwrap(),
        3,
    );

    let qr = query(&mut conn,
        "SELECT a.name, b.name FROM items a CROSS JOIN items b WHERE a.id < b.id ORDER BY a.id, b.id"
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("A".into()), Value::Text("B".into())]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("A".into()), Value::Text("C".into())]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Text("B".into()), Value::Text("C".into())]
    );
}

#[test]
fn join_scale_100x1000() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, uid INTEGER NOT NULL, amt INTEGER NOT NULL)"
    ).unwrap());

    for i in 0..100 {
        conn.execute(&format!(
            "INSERT INTO users (id, name) VALUES ({i}, 'user_{i}')"
        ))
        .unwrap();
    }
    for i in 0..1000 {
        let uid = i % 100;
        conn.execute(&format!(
            "INSERT INTO orders (id, uid, amt) VALUES ({i}, {uid}, {})",
            i * 10
        ))
        .unwrap();
    }

    let qr = query(
        &mut conn,
        "SELECT COUNT(*) FROM users u JOIN orders o ON u.id = o.uid",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(1000));
}

#[test]
fn join_within_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER NOT NULL)")
            .unwrap(),
    );

    assert_rows_affected(
        conn.execute("INSERT INTO t1 (id, val) VALUES (1, 'x'), (2, 'y')")
            .unwrap(),
        2,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t2 (id, ref_id) VALUES (10, 1), (20, 2)")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT t1.val, t2.id FROM t1 JOIN t2 ON t1.id = t2.ref_id ORDER BY t2.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("x".into()), Value::Integer(10)]
    );

    conn.execute("COMMIT").unwrap();
}

#[test]
fn right_join_includes_unmatched_right() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT u.name, o.id FROM users u RIGHT JOIN orders o ON u.id = o.user_id ORDER BY o.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("Alice".into()), Value::Integer(10)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("Alice".into()), Value::Integer(11)]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Text("Bob".into()), Value::Integer(12)]
    );
}

#[test]
fn right_join_null_pads_left_side() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 (id, val) VALUES (1, 'A')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t2 (id, ref_id) VALUES (10, 1), (20, 999)")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT t1.val, t2.id FROM t1 RIGHT JOIN t2 ON t1.id = t2.ref_id ORDER BY t2.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("A".into()), Value::Integer(10)]
    );
    assert_eq!(qr.rows[1], vec![Value::Null, Value::Integer(20)]);
}

#[test]
fn right_join_empty_left_table() {
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
        conn.execute("INSERT INTO t2 (id) VALUES (1), (2), (3)")
            .unwrap(),
        3,
    );

    let qr = query(
        &mut conn,
        "SELECT t1.id, t2.id FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t2.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0], vec![Value::Null, Value::Integer(1)]);
    assert_eq!(qr.rows[1], vec![Value::Null, Value::Integer(2)]);
    assert_eq!(qr.rows[2], vec![Value::Null, Value::Integer(3)]);
}

#[test]
fn right_join_within_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 (id, val) VALUES (1, 'x')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t2 (id, ref_id) VALUES (10, 1), (20, 999)")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT t1.val, t2.id FROM t1 RIGHT JOIN t2 ON t1.id = t2.ref_id ORDER BY t2.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("x".into()), Value::Integer(10)]
    );
    assert_eq!(qr.rows[1], vec![Value::Null, Value::Integer(20)]);

    conn.execute("COMMIT").unwrap();
}

#[test]
fn join_table_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );

    let result = conn.execute("SELECT * FROM t1 JOIN nonexistent ON t1.id = nonexistent.id");
    assert!(matches!(result, Err(SqlError::TableNotFound(_))));
}
