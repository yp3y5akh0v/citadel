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

fn setup_schema(conn: &mut Connection<'_>) {
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, email TEXT)",
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_name ON users (name)")
        .unwrap();
    conn.execute("CREATE INDEX idx_name_age ON users (name, age)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)")
        .unwrap();
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount REAL)",
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_user_id ON orders (user_id)")
        .unwrap();
}

// ── Column name ──────────────────────────────────────────────────────

#[test]
fn explain_returns_plan_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let qr = query_result(conn.execute("EXPLAIN SELECT * FROM users").unwrap());
    assert_eq!(qr.columns, vec!["plan"]);
}

// ── Single-table scans ──────────────────────────────────────────────

#[test]
fn explain_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users");
    assert_eq!(lines, vec!["SCAN TABLE users"]);
}

#[test]
fn explain_seq_scan_with_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users WHERE age > 30");
    assert!(lines.contains(&"SCAN TABLE users".to_string()));
    assert!(lines.contains(&"FILTER".to_string()));
}

#[test]
fn explain_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users WHERE id = 5");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING PRIMARY KEY"));
    assert!(lines[0].contains("id = 5"));
}

#[test]
fn explain_index_scan_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users WHERE name = 'Alice'",
    );
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING INDEX"));
    assert!(lines[0].contains("name = ?"));
}

#[test]
fn explain_unique_index_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users WHERE email = 'alice@test.com'",
    );
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING INDEX idx_email"));
}

#[test]
fn explain_composite_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users WHERE name = 'Alice' AND age = 30",
    );
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("USING INDEX idx_name_age"));
    assert!(lines[0].contains("name = ?"));
    assert!(lines[0].contains("age = ?"));
}

#[test]
fn explain_index_range_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users WHERE name > 'M'");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING INDEX"));
    assert!(lines[0].contains("name > ?"));
}

// ── Joins ────────────────────────────────────────────────────────────

#[test]
fn explain_inner_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
    );
    assert!(lines.iter().any(|l| l.contains("SCAN TABLE users AS u")));
    assert!(lines.iter().any(|l| l.contains("SCAN TABLE orders AS o")));
    assert!(lines.contains(&"NESTED LOOP".to_string()));
}

#[test]
fn explain_left_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id",
    );
    assert!(lines.contains(&"LEFT JOIN".to_string()));
}

#[test]
fn explain_right_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id",
    );
    assert!(lines.contains(&"RIGHT JOIN".to_string()));
}

#[test]
fn explain_cross_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users CROSS JOIN orders");
    assert!(lines.contains(&"CROSS JOIN".to_string()));
}

#[test]
fn explain_multi_way_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER)")
        .unwrap();

    let lines = explain_lines(&mut conn,
        "EXPLAIN SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN items i ON o.id = i.order_id");
    assert!(lines.iter().any(|l| l.contains("users")));
    assert!(lines.iter().any(|l| l.contains("orders")));
    assert!(lines.iter().any(|l| l.contains("items")));
}

// ── Query features ──────────────────────────────────────────────────

#[test]
fn explain_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT DISTINCT name FROM users");
    assert!(lines.contains(&"DISTINCT".to_string()));
}

#[test]
fn explain_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users ORDER BY name");
    assert!(lines.contains(&"SORT".to_string()));
}

#[test]
fn explain_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users LIMIT 10");
    assert!(lines.contains(&"LIMIT 10".to_string()));
}

#[test]
fn explain_offset_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users LIMIT 10 OFFSET 5");
    assert!(lines.contains(&"OFFSET 5".to_string()));
    assert!(lines.contains(&"LIMIT 10".to_string()));
}

#[test]
fn explain_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT name, COUNT(*) FROM users GROUP BY name",
    );
    assert!(lines.contains(&"GROUP BY".to_string()));
}

#[test]
fn explain_all_features() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT DISTINCT name FROM users ORDER BY name LIMIT 10 OFFSET 5",
    );
    assert!(lines.contains(&"SCAN TABLE users".to_string()));
    assert!(lines.contains(&"DISTINCT".to_string()));
    assert!(lines.contains(&"SORT".to_string()));
    assert!(lines.contains(&"OFFSET 5".to_string()));
    assert!(lines.contains(&"LIMIT 10".to_string()));
}

// ── DML ──────────────────────────────────────────────────────────────

#[test]
fn explain_update_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN UPDATE users SET name = 'Bob' WHERE id = 1",
    );
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("UPDATE"));
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING PRIMARY KEY"));
}

#[test]
fn explain_update_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN UPDATE users SET name = 'Bob' WHERE age > 30",
    );
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("UPDATE"));
    assert!(lines[0].contains("SCAN TABLE users"));
}

#[test]
fn explain_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN DELETE FROM users WHERE name = 'Alice'");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("DELETE FROM"));
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING INDEX"));
}

#[test]
fn explain_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN INSERT INTO users (id, name) VALUES (1, 'Alice')",
    );
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("INSERT INTO users"));
}

// ── Edge cases ──────────────────────────────────────────────────────

#[test]
fn explain_no_from() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT 1 + 2");
    assert_eq!(lines, vec!["CONSTANT ROW"]);
}

#[test]
fn explain_explain_is_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let result = conn.execute("EXPLAIN EXPLAIN SELECT * FROM users");
    assert!(result.is_err());
}

#[test]
fn explain_create_table_is_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let result = conn.execute("EXPLAIN CREATE TABLE t (id INTEGER PRIMARY KEY)");
    assert!(result.is_err());
}

#[test]
fn explain_table_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users AS u");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("SCAN TABLE users AS u"));
}

#[test]
fn explain_within_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    conn.execute("BEGIN").unwrap();
    let lines = explain_lines(&mut conn, "EXPLAIN SELECT * FROM users WHERE id = 1");
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING PRIMARY KEY"));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn explain_does_not_execute() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("EXPLAIN DELETE FROM users WHERE id = 1")
        .unwrap();

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM users").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn explain_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_schema(&mut conn);

    let lines = explain_lines(
        &mut conn,
        "EXPLAIN SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
    );
    assert!(lines.contains(&"SUBQUERY".to_string()));
}
