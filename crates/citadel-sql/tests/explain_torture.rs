use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn query_result(result: ExecutionResult) -> QueryResult {
    match result {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

fn explain_lines(conn: &mut Connection<'_>, sql: &str) -> Vec<String> {
    let qr = query_result(conn.execute(sql).unwrap());
    assert_eq!(qr.columns, vec!["plan"]);
    qr.rows
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(s) => s.to_string(),
            other => panic!("expected Text, got {other:?}"),
        })
        .collect()
}

fn setup_full_schema(conn: &mut Connection<'_>) {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, email TEXT, dept TEXT)").unwrap();
    conn.execute("CREATE INDEX idx_name ON users (name)")
        .unwrap();
    conn.execute("CREATE INDEX idx_name_age ON users (name, age)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)")
        .unwrap();
    conn.execute("CREATE INDEX idx_dept ON users (dept)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount REAL, status TEXT)").unwrap();
    conn.execute("CREATE INDEX idx_user_id ON orders (user_id)")
        .unwrap();
    conn.execute("CREATE INDEX idx_status ON orders (status)")
        .unwrap();
    conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)")
        .unwrap();
    conn.execute("CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL, product_id INTEGER NOT NULL)").unwrap();
    conn.execute("CREATE INDEX idx_order_id ON order_items (order_id)")
        .unwrap();
    conn.execute("CREATE INDEX idx_product_id ON order_items (product_id)")
        .unwrap();
}

// ── Scan plan accuracy ──────────────────────────────────────────────

#[test]
fn pk_lookup_shows_actual_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users WHERE id = 42");
    assert!(lines[0].contains("id = 42"));
}

#[test]
fn pk_lookup_text_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)")
        .unwrap();

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM kv WHERE k = 'hello'");
    assert!(lines[0].contains("SEARCH TABLE kv"));
    assert!(lines[0].contains("USING PRIMARY KEY"));
    assert!(lines[0].contains("k = 'hello'"));
}

#[test]
fn composite_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE assoc (a INTEGER, b INTEGER, val TEXT, PRIMARY KEY (a, b))")
        .unwrap();

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM assoc WHERE a = 1 AND b = 2",
    );
    assert!(lines[0].contains("SEARCH TABLE assoc"));
    assert!(lines[0].contains("USING PRIMARY KEY"));
    assert!(lines[0].contains("a = 1"));
    assert!(lines[0].contains("b = 2"));
}

#[test]
fn partial_composite_pk_is_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE assoc (a INTEGER, b INTEGER, val TEXT, PRIMARY KEY (a, b))")
        .unwrap();

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM assoc WHERE a = 1");
    assert!(lines[0].contains("SCAN TABLE assoc"));
}

#[test]
fn index_equality_plus_range() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users WHERE name = 'Alice' AND age > 20",
    );
    assert!(lines[0].contains("USING INDEX idx_name_age"));
    assert!(lines[0].contains("name = ?"));
    assert!(lines[0].contains("age > ?"));
}

#[test]
fn unindexed_column_forces_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users WHERE age = 30");
    assert!(lines[0].contains("SCAN TABLE users"));
    assert!(lines.contains(&"FILTER".to_string()));
}

#[test]
fn or_forces_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users WHERE name = 'A' OR name = 'B'",
    );
    assert!(lines[0].contains("SCAN TABLE users"));
}

// ── Join plans ───────────────────────────────────────────────────────

#[test]
fn join_shows_all_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(&mut conn,
        "EXPLAIN SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN order_items oi ON o.id = oi.order_id");
    assert!(lines.iter().any(|l| l.contains("users")));
    assert!(lines.iter().any(|l| l.contains("orders")));
    assert!(lines.iter().any(|l| l.contains("order_items")));
}

#[test]
fn self_join_different_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users a JOIN users b ON a.id = b.id",
    );
    assert!(lines.iter().any(|l| l.contains("users AS a")));
    assert!(lines.iter().any(|l| l.contains("users AS b")));
}

#[test]
fn join_with_where_and_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(&mut conn,
        "EXPLAIN SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount LIMIT 5");
    assert!(lines.contains(&"SORT".to_string()));
    assert!(lines.contains(&"LIMIT 5".to_string()));
}

// ── Complex query features ──────────────────────────────────────────

#[test]
fn distinct_sort_limit_offset_all_present() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT DISTINCT name FROM users ORDER BY name LIMIT 5 OFFSET 2",
    );
    let scan_pos = lines.iter().position(|l| l.contains("SCAN TABLE")).unwrap();
    let distinct_pos = lines.iter().position(|l| l == "DISTINCT").unwrap();
    let sort_pos = lines.iter().position(|l| l == "SORT").unwrap();
    let offset_pos = lines.iter().position(|l| l == "OFFSET 2").unwrap();
    let limit_pos = lines.iter().position(|l| l == "LIMIT 5").unwrap();

    assert!(scan_pos < distinct_pos);
    assert!(distinct_pos < sort_pos);
    assert!(sort_pos < offset_pos);
    assert!(offset_pos < limit_pos);
}

#[test]
fn group_by_with_having() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT dept, COUNT(*) FROM users GROUP BY dept HAVING COUNT(*) > 3",
    );
    assert!(lines.contains(&"GROUP BY".to_string()));
}

#[test]
fn group_by_with_order_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT dept, COUNT(*) FROM users GROUP BY dept ORDER BY dept LIMIT 3",
    );
    assert!(lines.contains(&"GROUP BY".to_string()));
    assert!(lines.contains(&"SORT".to_string()));
    assert!(lines.contains(&"LIMIT 3".to_string()));
}

// ── Subqueries ───────────────────────────────────────────────────────

#[test]
fn in_subquery_shows_subquery_line() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)",
    );
    assert!(lines.contains(&"SUBQUERY".to_string()));
}

#[test]
fn exists_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = 1)",
    );
    assert!(lines.contains(&"SUBQUERY".to_string()));
}

#[test]
fn scalar_subquery_in_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT name, (SELECT COUNT(*) FROM orders) FROM users",
    );
    assert!(lines.contains(&"SUBQUERY".to_string()));
}

#[test]
fn multiple_subqueries() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(&mut conn,
        "EXPLAIN SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) AND name IN (SELECT name FROM products)");
    let subquery_count = lines.iter().filter(|l| l.as_str() == "SUBQUERY").count();
    assert_eq!(subquery_count, 2);
}

// ── DML plans ────────────────────────────────────────────────────────

#[test]
fn update_with_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN UPDATE users SET age = 30 WHERE email = 'a@b.com'",
    );
    assert!(lines[0].contains("UPDATE"));
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING INDEX idx_email"));
}

#[test]
fn delete_full_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN DELETE FROM users");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("DELETE FROM"));
    assert!(lines[0].contains("SCAN TABLE users"));
}

#[test]
fn delete_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN DELETE FROM users WHERE id = 99");
    assert!(lines[0].contains("DELETE FROM"));
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING PRIMARY KEY"));
}

// ── Error handling ───────────────────────────────────────────────────

#[test]
fn explain_nonexistent_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let result = conn.execute("EXPLAIN SELECT * FROM nonexistent");
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("not found"));
}

#[test]
fn explain_drop_table_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let result = conn.execute("EXPLAIN DROP TABLE users");
    assert!(result.is_err());
}

#[test]
fn explain_create_index_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let result = conn.execute("EXPLAIN CREATE INDEX idx ON t (a)");
    assert!(result.is_err());
}

#[test]
fn explain_begin_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let result = conn.execute("EXPLAIN BEGIN");
    assert!(result.is_err());
}

// ── Transaction interaction ──────────────────────────────────────────

#[test]
fn explain_in_txn_does_not_modify() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("EXPLAIN INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();
    conn.execute("EXPLAIN DELETE FROM users WHERE id = 1")
        .unwrap();
    conn.execute("EXPLAIN UPDATE users SET name = 'X' WHERE id = 1")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM users").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn explain_sees_uncommitted_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE temp (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM temp WHERE id = 1");
    assert!(lines[0].contains("SEARCH TABLE temp"));
    conn.execute("ROLLBACK").unwrap();
}

// ── No alias when same as table name ─────────────────────────────────

#[test]
fn no_alias_shown_when_same_as_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users users");
    assert!(lines[0].contains("SCAN TABLE users"));
    assert!(!lines[0].contains("AS"));
}

// ── Plan stability ──────────────────────────────────────────────────

#[test]
fn plan_same_before_and_after_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let before = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users WHERE id = 1");

    for i in 1..=100 {
        conn.execute(&format!(
            "INSERT INTO users (id, name, age, email) VALUES ({i}, 'user{i}', {}, 'user{i}@test.com')",
            20 + i % 50
        )).unwrap();
    }

    let after = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users WHERE id = 1");
    assert_eq!(before, after);
}

// ── BETWEEN / LIKE in WHERE ──────────────────────────────────────────

#[test]
fn explain_with_between() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users WHERE age BETWEEN 20 AND 30",
    );
    assert!(lines[0].contains("SCAN TABLE users"));
    assert!(lines.contains(&"FILTER".to_string()));
}

#[test]
fn explain_with_like() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users WHERE name LIKE 'A%'",
    );
    assert!(lines[0].contains("SCAN TABLE users"));
    assert!(lines.contains(&"FILTER".to_string()));
}

// ── via query() method ──────────────────────────────────────────────

#[test]
fn explain_via_query_method() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_full_schema(&mut conn);

    let qr = conn
        .query("EXPLAIN SELECT * FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(qr.columns, vec!["plan"]);
    assert!(!qr.rows.is_empty());
    match &qr.rows[0][0] {
        Value::Text(s) => assert!(s.contains("SEARCH TABLE users")),
        other => panic!("expected Text, got {other:?}"),
    }
}
