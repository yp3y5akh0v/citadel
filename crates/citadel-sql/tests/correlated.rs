use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn assert_rows_affected(result: ExecutionResult, expected: u64) {
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn setup_users_orders(conn: &mut Connection) {
    assert_ok(
        conn.execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT, dept TEXT)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER, total REAL)",
        )
        .unwrap(),
    );
    for &(id, name, dept) in &[
        (1, "Alice", "eng"),
        (2, "Bob", "sales"),
        (3, "Charlie", "eng"),
        (4, "Dave", "sales"),
        (5, "Eve", "eng"),
    ] {
        assert_rows_affected(
            conn.execute(&format!(
                "INSERT INTO users VALUES ({id}, '{name}', '{dept}')"
            ))
            .unwrap(),
            1,
        );
    }
    for &(id, uid, total) in &[(1, 1, 100.0), (2, 1, 200.0), (3, 3, 50.0), (4, 5, 300.0)] {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO orders VALUES ({id}, {uid}, {total})"))
                .unwrap(),
            1,
        );
    }
}

fn setup_emp(conn: &mut Connection) {
    assert_ok(conn.execute(
        "CREATE TABLE emp (id INTEGER NOT NULL PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)"
    ).unwrap());
    for &(id, name, dept, sal) in &[
        (1, "Alice", "eng", 100),
        (2, "Bob", "eng", 80),
        (3, "Charlie", "sales", 90),
        (4, "Dave", "sales", 70),
        (5, "Eve", "eng", 120),
        (6, "Frank", "hr", 85),
    ] {
        assert_rows_affected(
            conn.execute(&format!(
                "INSERT INTO emp VALUES ({id}, '{name}', '{dept}', {sal})"
            ))
            .unwrap(),
            1,
        );
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Correlated EXISTS
// ═══════════════════════════════════════════════════════════════════

#[test]
fn exists_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3); // Alice, Charlie, Eve
}

#[test]
fn exists_with_inner_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.total > 150.0) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2); // Alice(200), Eve(300)
}

#[test]
fn exists_with_outer_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE dept = 'eng' AND EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3); // Alice, Charlie, Eve (all eng with orders)
}

#[test]
fn exists_empty_inner() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.total > 9999.0)"
    ).unwrap();
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn exists_all_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    // Give everyone an order
    assert_rows_affected(
        conn.execute("INSERT INTO orders VALUES (10, 2, 10.0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO orders VALUES (11, 4, 10.0)")
            .unwrap(),
        1,
    );

    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
    ).unwrap();
    assert_eq!(qr.rows.len(), 5);
}

#[test]
fn exists_no_duplicate_outer_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    // Alice has 2 orders — should appear once
    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY id"
    ).unwrap();
    let names: Vec<&Value> = qr.rows.iter().map(|r| &r[0]).collect();
    assert_eq!(
        names
            .iter()
            .filter(|n| ***n == Value::Text("Alice".into()))
            .count(),
        1
    );
}

#[test]
fn exists_with_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name DESC"
    ).unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("Eve".into()));
}

#[test]
fn exists_with_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY id LIMIT 2"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════
//  Correlated NOT EXISTS
// ═══════════════════════════════════════════════════════════════════

#[test]
fn not_exists_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2); // Bob, Dave
    assert_eq!(qr.rows[0][0], Value::Text("Bob".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Dave".into()));
}

#[test]
fn not_exists_all_have_orders() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);
    assert_rows_affected(
        conn.execute("INSERT INTO orders VALUES (10, 2, 10.0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO orders VALUES (11, 4, 10.0)")
            .unwrap(),
        1,
    );

    let qr = conn.query(
        "SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
    ).unwrap();
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn not_exists_empty_inner() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.total > 9999.0)"
    ).unwrap();
    assert_eq!(qr.rows.len(), 5); // all pass
}

#[test]
fn not_exists_with_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE dept = 'sales' AND NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name"
    ).unwrap();
    // Bob(sales, no order) and Dave(sales, no order)
    assert_eq!(qr.rows.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════
//  Correlated IN
// ═══════════════════════════════════════════════════════════════════

#[test]
fn in_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE orders.user_id = users.id) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3); // Alice, Charlie, Eve
}

#[test]
fn in_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE orders.user_id = users.id AND orders.total > 9999.0)"
    ).unwrap();
    assert_eq!(qr.rows.len(), 0);
}

// ═══════════════════════════════════════════════════════════════════
//  Correlated NOT IN
// ═══════════════════════════════════════════════════════════════════

#[test]
fn not_in_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE orders.user_id = users.id) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2); // Bob, Dave
}

// ═══════════════════════════════════════════════════════════════════
//  Correlated scalar subqueries in WHERE
// ═══════════════════════════════════════════════════════════════════

#[test]
fn scalar_where_above_dept_avg() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Use separate tables to avoid self-join alias complexity
    assert_ok(conn.execute(
        "CREATE TABLE staff (id INTEGER NOT NULL PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)"
    ).unwrap());
    assert_ok(
        conn.execute(
            "CREATE TABLE dept_avg (id INTEGER NOT NULL PRIMARY KEY, dept TEXT, avg_sal REAL)",
        )
        .unwrap(),
    );
    for &(id, name, dept, sal) in &[
        (1, "Alice", "eng", 100),
        (2, "Bob", "eng", 80),
        (3, "Charlie", "sales", 90),
        (4, "Dave", "sales", 70),
        (5, "Eve", "eng", 120),
        (6, "Frank", "hr", 85),
    ] {
        assert_rows_affected(
            conn.execute(&format!(
                "INSERT INTO staff VALUES ({id}, '{name}', '{dept}', {sal})"
            ))
            .unwrap(),
            1,
        );
    }
    // eng avg=100, sales avg=80, hr avg=85
    assert_rows_affected(
        conn.execute("INSERT INTO dept_avg VALUES (1, 'eng', 100.0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dept_avg VALUES (2, 'sales', 80.0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dept_avg VALUES (3, 'hr', 85.0)")
            .unwrap(),
        1,
    );

    let qr = conn.query(
        "SELECT name FROM staff WHERE salary > (SELECT avg_sal FROM dept_avg WHERE dept_avg.dept = staff.dept) ORDER BY name"
    ).unwrap();
    // Alice(100)>100? no, Bob(80)<100 no, Charlie(90)>80 yes, Dave(70)<80 no, Eve(120)>100 yes, Frank(85)=85 no
    assert_eq!(qr.rows.len(), 2); // Charlie, Eve
}

#[test]
fn scalar_where_max_in_dept() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE staff2 (id INTEGER NOT NULL PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)"
    ).unwrap());
    assert_ok(
        conn.execute(
            "CREATE TABLE dept_max (id INTEGER NOT NULL PRIMARY KEY, dept TEXT, max_sal INTEGER)",
        )
        .unwrap(),
    );
    for &(id, name, dept, sal) in &[
        (1, "Alice", "eng", 100),
        (2, "Bob", "eng", 80),
        (3, "Charlie", "sales", 90),
        (4, "Dave", "sales", 70),
        (5, "Eve", "eng", 120),
        (6, "Frank", "hr", 85),
    ] {
        assert_rows_affected(
            conn.execute(&format!(
                "INSERT INTO staff2 VALUES ({id}, '{name}', '{dept}', {sal})"
            ))
            .unwrap(),
            1,
        );
    }
    assert_rows_affected(
        conn.execute("INSERT INTO dept_max VALUES (1, 'eng', 120)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dept_max VALUES (2, 'sales', 90)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dept_max VALUES (3, 'hr', 85)")
            .unwrap(),
        1,
    );

    let qr = conn.query(
        "SELECT name FROM staff2 WHERE salary = (SELECT max_sal FROM dept_max WHERE dept_max.dept = staff2.dept) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3); // Eve(120), Charlie(90), Frank(85)
}

// ═══════════════════════════════════════════════════════════════════
//  Real-world SQL patterns
// ═══════════════════════════════════════════════════════════════════

#[test]
fn pattern_customers_with_orders() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn pattern_customers_without_orders() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn pattern_find_with_lookup_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Use separate tables: items + email_counts (pre-computed)
    assert_ok(
        conn.execute("CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, email TEXT)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE email_counts (id INTEGER NOT NULL PRIMARY KEY, email TEXT, cnt INTEGER)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items VALUES (1, 'a@test.com')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items VALUES (2, 'b@test.com')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items VALUES (3, 'a@test.com')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items VALUES (4, 'c@test.com')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO email_counts VALUES (1, 'a@test.com', 2)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO email_counts VALUES (2, 'b@test.com', 1)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO email_counts VALUES (3, 'c@test.com', 1)")
            .unwrap(),
        1,
    );

    // Find items whose email has count > 1 using correlated scalar
    let qr = conn.query(
        "SELECT id FROM items WHERE 1 < (SELECT cnt FROM email_counts WHERE email_counts.email = items.email) ORDER BY id"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn pattern_above_dept_average() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_emp(&mut conn);

    // Use separate table for averages to avoid self-join alias issue
    assert_ok(
        conn.execute(
            "CREATE TABLE dept_avgs (id INTEGER NOT NULL PRIMARY KEY, dept TEXT, avg_sal REAL)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dept_avgs VALUES (1, 'eng', 100.0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dept_avgs VALUES (2, 'sales', 80.0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dept_avgs VALUES (3, 'hr', 85.0)")
            .unwrap(),
        1,
    );

    let qr = conn.query(
        "SELECT name, salary FROM emp WHERE salary > (SELECT avg_sal FROM dept_avgs WHERE dept_avgs.dept = emp.dept) ORDER BY salary DESC"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Eve".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  Mixed: correlated + non-correlated
// ═══════════════════════════════════════════════════════════════════

#[test]
fn mixed_correlated_exists_and_regular_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE dept = 'eng' AND EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.total > 50.0) ORDER BY name"
    ).unwrap();
    // eng users with orders > 50: Alice(100,200), Eve(300)
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn two_correlated_exists_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);
    assert_ok(
        conn.execute("CREATE TABLE reviews (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO reviews VALUES (1, 1)").unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO reviews VALUES (2, 3)").unwrap(),
        1,
    );

    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) AND EXISTS (SELECT 1 FROM reviews WHERE reviews.user_id = users.id) ORDER BY name"
    ).unwrap();
    // Users with both orders AND reviews: Alice(orders+review), Charlie(orders+review)
    assert_eq!(qr.rows.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════
//  DML with correlated subqueries
// ═══════════════════════════════════════════════════════════════════

#[test]
fn update_with_correlated_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    assert_rows_affected(conn.execute(
        "UPDATE users SET name = 'BUYER' WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
    ).unwrap(), 3);

    let qr = conn
        .query("SELECT COUNT(*) FROM users WHERE name = 'BUYER'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn delete_with_correlated_not_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    assert_rows_affected(conn.execute(
        "DELETE FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
    ).unwrap(), 2);

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn insert_select_with_correlated_not_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);
    assert_ok(
        conn.execute(
            "CREATE TABLE archive (id INTEGER NOT NULL PRIMARY KEY, name TEXT, dept TEXT)",
        )
        .unwrap(),
    );

    assert_rows_affected(conn.execute(
        "INSERT INTO archive SELECT id, name, dept FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
    ).unwrap(), 2);

    let qr = conn
        .query("SELECT name FROM archive ORDER BY name")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Bob".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Dave".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  Data integrity: compare with JOIN equivalents
// ═══════════════════════════════════════════════════════════════════

#[test]
fn exists_matches_inner_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr_exists = conn.query(
        "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY id"
    ).unwrap();
    let qr_join = conn.query(
        "SELECT DISTINCT users.id FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY users.id"
    ).unwrap();
    assert_eq!(qr_exists.rows, qr_join.rows);
}

#[test]
fn not_exists_matches_left_join_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr_ne = conn.query(
        "SELECT id FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY id"
    ).unwrap();
    let qr_lj = conn.query(
        "SELECT users.id FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE orders.id IS NULL ORDER BY users.id"
    ).unwrap();
    assert_eq!(qr_ne.rows, qr_lj.rows);
}

// ═══════════════════════════════════════════════════════════════════
//  Interaction with views
// ═══════════════════════════════════════════════════════════════════

#[test]
fn correlated_exists_with_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT DISTINCT dept FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY dept"
    ).unwrap();
    // eng (Alice, Charlie, Eve all have orders)
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("eng".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  Transactions
// ═══════════════════════════════════════════════════════════════════

#[test]
fn correlated_exists_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    conn.execute("BEGIN").unwrap();
    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3);
    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════
//  Large data correctness
// ═══════════════════════════════════════════════════════════════════

#[test]
fn exists_large_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    for i in 0..200 {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO t1 VALUES ({i}, {i})"))
                .unwrap(),
            1,
        );
    }
    // Only even IDs have matches in t2
    for i in (0..200).step_by(2) {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO t2 VALUES ({i}, {i})"))
                .unwrap(),
            1,
        );
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query("SELECT COUNT(*) FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref_id = t1.id)")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(100));

    let qr = conn
        .query(
            "SELECT COUNT(*) FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref_id = t1.id)",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(100));
}

#[test]
fn self_join_above_dept_avg() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_emp(&mut conn);

    // Self-join with aliases: employees above their department average
    let qr = conn.query(
        "SELECT name FROM emp e1 WHERE salary > (SELECT AVG(salary) FROM emp e2 WHERE e2.dept = e1.dept) ORDER BY name"
    ).unwrap();
    // eng avg=100: Eve(120)>100 yes, Alice(100)=100 no, Bob(80)<100 no
    // sales avg=80: Charlie(90)>80 yes, Dave(70)<80 no
    // hr avg=85: Frank(85)=85 no
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Charlie".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Eve".into()));
}

#[test]
fn scalar_in_select_max() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name, (SELECT MAX(total) FROM orders WHERE orders.user_id = users.id) AS max_order FROM users ORDER BY id"
    ).unwrap();
    assert_eq!(qr.rows.len(), 5);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][1], Value::Real(200.0)); // Alice max order
    assert_eq!(qr.rows[1][0], Value::Text("Bob".into()));
    assert!(qr.rows[1][1].is_null()); // Bob has no orders → NULL
    assert_eq!(qr.rows[2][0], Value::Text("Charlie".into()));
    assert_eq!(qr.rows[2][1], Value::Real(50.0)); // Charlie max order
}

#[test]
fn scalar_in_select_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users ORDER BY id"
    ).unwrap();
    assert_eq!(qr.rows.len(), 5);
    assert_eq!(qr.rows[0][1], Value::Integer(2)); // Alice has 2 orders
                                                  // Bob has 0 orders — decorrelation returns NULL (no matching group in GROUP BY)
    assert!(qr.rows[1][1].is_null());
    assert_eq!(qr.rows[2][1], Value::Integer(1)); // Charlie has 1
}

#[test]
fn self_join_scalar_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_emp(&mut conn);

    let qr = conn.query(
        "SELECT name, salary, (SELECT AVG(salary) FROM emp e2 WHERE e2.dept = e1.dept) AS dept_avg FROM emp e1 ORDER BY id"
    ).unwrap();
    assert_eq!(qr.rows.len(), 6);
    // Alice(eng, 100), eng avg=100
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][2], Value::Real(100.0));
    // Charlie(sales, 90), sales avg=80
    assert_eq!(qr.rows[2][0], Value::Text("Charlie".into()));
    assert_eq!(qr.rows[2][2], Value::Real(80.0));
}

// ═══════════════════════════════════════════════════════════════════
//  Error cases
// ═══════════════════════════════════════════════════════════════════

#[test]
fn error_inner_table_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let err = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM ghost WHERE ghost.id = users.id)",
    );
    assert!(err.is_err());
}

// ═══════════════════════════════════════════════════════════════════
//  Prepared params with correlated
// ═══════════════════════════════════════════════════════════════════

#[test]
fn correlated_with_prepared_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query_params(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.total > $1) ORDER BY name",
        &[Value::Real(150.0)],
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Eve".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  Correlated + DISTINCT / GROUP BY / ORDER BY
// ═══════════════════════════════════════════════════════════════════

#[test]
fn correlated_with_group_by_having() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT dept, COUNT(*) AS cnt FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) GROUP BY dept ORDER BY dept"
    ).unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("eng".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(3));
}

#[test]
fn correlated_with_order_by_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name LIMIT 2"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Charlie".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  Multiple correlation columns
// ═══════════════════════════════════════════════════════════════════

#[test]
fn multi_column_correlation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 VALUES (1, 10, 20)").unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 VALUES (2, 10, 30)").unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 VALUES (3, 20, 20)").unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t2 VALUES (1, 10, 20)").unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t2 VALUES (2, 20, 30)").unwrap(),
        1,
    );

    let qr = conn.query(
        "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.a = t1.a AND t2.b = t1.b) ORDER BY id"
    ).unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn non_eq_find_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, email TEXT)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items VALUES (1, 'a@test.com')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items VALUES (2, 'b@test.com')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items VALUES (3, 'a@test.com')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items VALUES (4, 'c@test.com')")
            .unwrap(),
        1,
    );

    // Find duplicates: same email, different id (uses != non-equality correlation)
    let qr = conn.query(
        "SELECT id FROM items t1 WHERE EXISTS (SELECT 1 FROM items t2 WHERE t2.email = t1.email AND t2.id != t1.id) ORDER BY id"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn non_eq_greater_than_correlation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_emp(&mut conn);

    // Employees who have someone in their dept earning more than them
    let qr = conn.query(
        "SELECT name FROM emp e1 WHERE EXISTS (SELECT 1 FROM emp e2 WHERE e2.dept = e1.dept AND e2.salary > e1.salary) ORDER BY name"
    ).unwrap();
    // eng: Alice(100)<Eve(120), Bob(80)<Alice,Eve → Alice, Bob
    // sales: Dave(70)<Charlie(90) → Dave
    // hr: Frank alone → nobody
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Bob".into()));
    assert_eq!(qr.rows[2][0], Value::Text("Dave".into()));
}

#[test]
fn correlated_exists_on_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    // View with WHERE (non-fusable)
    assert_ok(
        conn.execute("CREATE VIEW eng_users AS SELECT * FROM users WHERE dept = 'eng'")
            .unwrap(),
    );

    let qr = conn.query(
        "SELECT name FROM eng_users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = eng_users.id) ORDER BY name"
    ).unwrap();
    // Eng users with orders: Alice, Charlie, Eve
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
}

#[test]
fn correlated_not_exists_on_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW all_users AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query(
        "SELECT name FROM all_users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = all_users.id) ORDER BY name"
    ).unwrap();
    // Users without orders: Bob, Dave
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Bob".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Dave".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  Self-join alias — additional
// ═══════════════════════════════════════════════════════════════════

#[test]
fn self_join_not_exists_highest_paid() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_emp(&mut conn);

    let qr = conn.query(
        "SELECT name FROM emp e1 WHERE NOT EXISTS (SELECT 1 FROM emp e2 WHERE e2.dept = e1.dept AND e2.salary > e1.salary) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("Charlie".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Eve".into()));
    assert_eq!(qr.rows[2][0], Value::Text("Frank".into()));
}

#[test]
fn self_join_max_salary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_emp(&mut conn);

    let qr = conn.query(
        "SELECT name FROM emp e1 WHERE salary = (SELECT MAX(salary) FROM emp e2 WHERE e2.dept = e1.dept) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3);
}

// ═══════════════════════════════════════════════════════════════════
//  Scalar SELECT — additional
// ═══════════════════════════════════════════════════════════════════

#[test]
fn scalar_select_sum_with_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name, (SELECT SUM(total) FROM orders WHERE orders.user_id = users.id) AS total_spent FROM users ORDER BY id"
    ).unwrap();
    assert_eq!(qr.columns[1], "total_spent");
    assert_eq!(qr.rows[0][1], Value::Real(300.0));
}

#[test]
fn multiple_scalar_in_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT name, (SELECT MIN(total) FROM orders WHERE orders.user_id = users.id) AS mn, (SELECT MAX(total) FROM orders WHERE orders.user_id = users.id) AS mx FROM users WHERE id = 1"
    ).unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][1], Value::Real(100.0));
    assert_eq!(qr.rows[0][2], Value::Real(200.0));
}

// ═══════════════════════════════════════════════════════════════════
//  Non-equality — additional
// ═══════════════════════════════════════════════════════════════════

#[test]
fn non_eq_not_exists_lowest_paid() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_emp(&mut conn);

    let qr = conn.query(
        "SELECT name FROM emp e1 WHERE NOT EXISTS (SELECT 1 FROM emp e2 WHERE e2.dept = e1.dept AND e2.salary < e1.salary) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("Bob".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Dave".into()));
    assert_eq!(qr.rows[2][0], Value::Text("Frank".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  NULL edge cases
// ═══════════════════════════════════════════════════════════════════

#[test]
fn null_outer_exists_excluded() {
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
    assert_rows_affected(conn.execute("INSERT INTO t1 VALUES (1, 10)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO t1 VALUES (2, NULL)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO t1 VALUES (3, 20)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO t2 VALUES (10)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO t2 VALUES (20)").unwrap(), 1);

    let qr = conn
        .query(
            "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.ref_id) ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn null_not_exists_passes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE p1 (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE p2 (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO p1 VALUES (1, 10)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO p1 VALUES (2, NULL)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO p2 VALUES (10)").unwrap(), 1);

    let qr = conn.query(
        "SELECT id FROM p1 WHERE NOT EXISTS (SELECT 1 FROM p2 WHERE p2.id = p1.ref_id) ORDER BY id"
    ).unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

// ═══════════════════════════════════════════════════════════════════
//  Data freshness
// ═══════════════════════════════════════════════════════════════════

#[test]
fn correlated_reflects_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    let qr = conn.query(
        "SELECT COUNT(*) FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
    ).unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));

    assert_rows_affected(
        conn.execute("INSERT INTO orders VALUES (10, 2, 75.0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO orders VALUES (11, 4, 25.0)")
            .unwrap(),
        1,
    );

    let qr = conn.query(
        "SELECT COUNT(*) FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
    ).unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

// ═══════════════════════════════════════════════════════════════════
//  View as inner table
// ═══════════════════════════════════════════════════════════════════

#[test]
fn view_as_inner_correlated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users_orders(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW big_orders AS SELECT * FROM orders WHERE total > 100.0")
            .unwrap(),
    );

    // Correlated EXISTS with view as inner table
    let qr = conn.query(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM big_orders WHERE big_orders.user_id = users.id) ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Eve".into()));
}
