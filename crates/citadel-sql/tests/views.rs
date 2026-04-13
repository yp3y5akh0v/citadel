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

fn open_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
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

fn setup_users(conn: &mut Connection) {
    assert_ok(conn.execute(
        "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT, email TEXT, age INTEGER)"
    ).unwrap());
    assert_rows_affected(
        conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@test.com', 30)",
        )
        .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@test.com', 25)",
        )
        .unwrap(),
        1,
    );
    assert_rows_affected(conn.execute(
        "INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'charlie@test.com', 35)"
    ).unwrap(), 1);
}

// ═══════════════════════════════════════════════════════════════════
//  CREATE VIEW — basic
// ═══════════════════════════════════════════════════════════════════

#[test]
fn create_simple_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW all_users AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM all_users").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn create_view_with_column_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW user_info (user_id, user_name) AS SELECT id, name FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM user_info").unwrap();
    assert_eq!(qr.columns[0], "user_id");
    assert_eq!(qr.columns[1], "user_name");
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn create_view_if_not_exists_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW IF NOT EXISTS v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn create_view_if_not_exists_existing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE VIEW IF NOT EXISTS v AS SELECT id FROM users")
            .unwrap(),
    );

    // Original view preserved
    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.columns.len(), 4);
}

#[test]
fn create_view_or_replace_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE OR REPLACE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn create_view_or_replace_existing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE OR REPLACE VIEW v AS SELECT id, name FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.columns.len(), 2);
}

#[test]
fn create_view_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW MyView AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM myview").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn create_view_with_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW adults AS SELECT * FROM users WHERE age >= 30")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM adults").unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn create_view_with_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);
    assert_ok(
        conn.execute(
            "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER, total REAL)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO orders (id, user_id, total) VALUES (1, 1, 99.99)")
            .unwrap(),
        1,
    );

    assert_ok(conn.execute(
        "CREATE VIEW user_orders AS SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id"
    ).unwrap());

    let qr = conn.query("SELECT * FROM user_orders").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
}

#[test]
fn create_view_with_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute(
            "CREATE VIEW age_stats AS SELECT COUNT(*) AS cnt, AVG(age) AS avg_age FROM users",
        )
        .unwrap(),
    );

    let qr = conn.query("SELECT * FROM age_stats").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[0][1], Value::Real(30.0));
}

#[test]
fn create_view_with_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);
    assert_rows_affected(
        conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'dave@test.com', 30)",
        )
        .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE VIEW distinct_ages AS SELECT DISTINCT age FROM users")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT * FROM distinct_ages ORDER BY age")
        .unwrap();
    assert_eq!(qr.rows.len(), 3); // 25, 30, 35
}

#[test]
fn create_view_with_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(conn.execute(
        "CREATE VIEW user_labels AS SELECT id, name || ' (age ' || age || ')' AS label FROM users"
    ).unwrap());

    let qr = conn
        .query("SELECT label FROM user_labels WHERE id = 1")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("Alice (age 30)".into()));
}

#[test]
fn create_view_with_union() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(conn.execute(
        "CREATE VIEW young_or_old AS SELECT * FROM users WHERE age < 28 UNION ALL SELECT * FROM users WHERE age > 32"
    ).unwrap());

    let qr = conn.query("SELECT * FROM young_or_old").unwrap();
    assert_eq!(qr.rows.len(), 2); // Bob(25) + Charlie(35)
}

#[test]
fn create_view_with_cte_in_body() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(conn.execute(
        "CREATE VIEW top_users AS WITH ranked AS (SELECT * FROM users WHERE age >= 30) SELECT * FROM ranked"
    ).unwrap());

    let qr = conn.query("SELECT * FROM top_users").unwrap();
    assert_eq!(qr.rows.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════
//  CREATE VIEW — errors
// ═══════════════════════════════════════════════════════════════════

#[test]
fn create_view_on_nonexistent_table_lazy() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // CREATE succeeds (lazy validation)
    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM ghost")
            .unwrap(),
    );

    // SELECT fails
    let err = conn.query("SELECT * FROM v").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn create_view_same_name_as_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    let err = conn
        .execute("CREATE VIEW users AS SELECT * FROM users")
        .unwrap_err();
    assert!(matches!(err, SqlError::TableAlreadyExists(_)));
}

#[test]
fn create_table_same_name_as_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let err = conn
        .execute("CREATE TABLE v (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap_err();
    assert!(matches!(err, SqlError::ViewAlreadyExists(_)));
}

#[test]
fn create_duplicate_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    let err = conn
        .execute("CREATE VIEW v AS SELECT id FROM users")
        .unwrap_err();
    assert!(matches!(err, SqlError::ViewAlreadyExists(_)));
}

// ═══════════════════════════════════════════════════════════════════
//  DROP VIEW
// ═══════════════════════════════════════════════════════════════════

#[test]
fn drop_view_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(conn.execute("DROP VIEW v").unwrap());

    let err = conn.query("SELECT * FROM v").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn drop_nonexistent_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn.execute("DROP VIEW ghost").unwrap_err();
    assert!(matches!(err, SqlError::ViewNotFound(_)));
}

#[test]
fn drop_view_if_exists_existing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(conn.execute("DROP VIEW IF EXISTS v").unwrap());
}

#[test]
fn drop_view_if_exists_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute("DROP VIEW IF EXISTS ghost").unwrap());
}

#[test]
fn drop_and_recreate_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(conn.execute("DROP VIEW v").unwrap());
    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT id FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.columns.len(), 1);
}

#[test]
fn drop_view_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW MyView AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(conn.execute("DROP VIEW myview").unwrap());
}

#[test]
fn drop_view_table_intact() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(conn.execute("DROP VIEW v").unwrap());

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

// ═══════════════════════════════════════════════════════════════════
//  SELECT from view
// ═══════════════════════════════════════════════════════════════════

#[test]
fn select_star_from_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.columns.len(), 4);
}

#[test]
fn select_columns_from_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT id, name FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT name FROM v WHERE id = 1").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
}

#[test]
fn select_with_where_on_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v WHERE age > 28").unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn select_with_order_by_on_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT name FROM v ORDER BY age DESC").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("Charlie".into()));
    assert_eq!(qr.rows[2][0], Value::Text("Bob".into()));
}

#[test]
fn select_with_limit_offset_on_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v ORDER BY id LIMIT 2").unwrap();
    assert_eq!(qr.rows.len(), 2);

    let qr = conn
        .query("SELECT * FROM v ORDER BY id LIMIT 1 OFFSET 1")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn select_with_group_by_on_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);
    assert_rows_affected(
        conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'dave@test.com', 30)",
        )
        .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT age, COUNT(*) FROM v GROUP BY age ORDER BY age")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn select_aggregate_on_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT COUNT(*), SUM(age) FROM v").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[0][1], Value::Integer(90));
}

#[test]
fn select_distinct_on_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);
    assert_rows_affected(
        conn.execute(
            "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'dave@test.com', 30)",
        )
        .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT DISTINCT age FROM v ORDER BY age")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn count_star_on_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users WHERE age >= 30")
            .unwrap(),
    );

    let qr = conn.query("SELECT COUNT(*) FROM v").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

// ═══════════════════════════════════════════════════════════════════
//  JOINs with views
// ═══════════════════════════════════════════════════════════════════

#[test]
fn view_inner_join_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);
    assert_ok(
        conn.execute(
            "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER, total REAL)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO orders (id, user_id, total) VALUES (1, 1, 50.0)")
            .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT v.name, orders.total FROM v INNER JOIN orders ON v.id = orders.user_id")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
}

#[test]
fn table_left_join_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);
    assert_ok(
        conn.execute(
            "CREATE TABLE scores (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER, score INTEGER)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO scores (id, user_id, score) VALUES (1, 1, 100)")
            .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM scores")
            .unwrap(),
    );

    let qr = conn.query(
        "SELECT users.name, v.score FROM users LEFT JOIN v ON users.id = v.user_id ORDER BY users.id"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][1], Value::Integer(100));
    assert!(qr.rows[1][1].is_null());
}

#[test]
fn view_join_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);
    assert_ok(
        conn.execute(
            "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER, total REAL)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO orders (id, user_id, total) VALUES (1, 1, 50.0)")
            .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE VIEW vu AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE VIEW vo AS SELECT * FROM orders")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT vu.name, vo.total FROM vu INNER JOIN vo ON vu.id = vo.user_id")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
}

// ═══════════════════════════════════════════════════════════════════
//  Subqueries with views
// ═══════════════════════════════════════════════════════════════════

#[test]
fn view_in_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW adults AS SELECT * FROM users WHERE age >= 30")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT name FROM users WHERE id IN (SELECT id FROM adults)")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn view_in_exists_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW adults AS SELECT * FROM users WHERE age >= 30")
            .unwrap(),
    );

    // Non-correlated EXISTS with view — all users returned when adults view has rows
    let qr = conn
        .query("SELECT name FROM users WHERE EXISTS (SELECT 1 FROM adults)")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);

    // NOT EXISTS with empty view result
    assert_ok(
        conn.execute("CREATE VIEW centenarians AS SELECT * FROM users WHERE age >= 100")
            .unwrap(),
    );
    let qr = conn
        .query("SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM centenarians)")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn view_in_scalar_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute(
            "CREATE VIEW adult_count AS SELECT COUNT(*) AS cnt FROM users WHERE age >= 30",
        )
        .unwrap(),
    );

    let qr = conn
        .query("SELECT (SELECT cnt FROM adult_count) AS n")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn view_in_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);
    assert_ok(conn.execute(
        "CREATE TABLE archive (id INTEGER NOT NULL PRIMARY KEY, name TEXT, email TEXT, age INTEGER)"
    ).unwrap());

    assert_ok(
        conn.execute("CREATE VIEW adults AS SELECT * FROM users WHERE age >= 30")
            .unwrap(),
    );

    assert_rows_affected(
        conn.execute("INSERT INTO archive SELECT * FROM adults")
            .unwrap(),
        2,
    );

    let qr = conn.query("SELECT COUNT(*) FROM archive").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

// ═══════════════════════════════════════════════════════════════════
//  Nested views
// ═══════════════════════════════════════════════════════════════════

#[test]
fn nested_view_two_levels() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v1 AS SELECT * FROM users WHERE age >= 25")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE VIEW v2 AS SELECT * FROM v1 WHERE age <= 35")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v2").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn nested_view_three_levels() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v1 AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE VIEW v2 AS SELECT * FROM v1 WHERE age >= 25")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE VIEW v3 AS SELECT name FROM v2 WHERE age <= 30")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v3").unwrap();
    assert_eq!(qr.rows.len(), 2); // Alice(30) + Bob(25)
}

#[test]
fn circular_view_detection() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW va AS SELECT * FROM users")
            .unwrap(),
    );
    // Replace va to reference vb
    assert_ok(conn.execute("CREATE VIEW vb AS SELECT * FROM va").unwrap());
    assert_ok(
        conn.execute("CREATE OR REPLACE VIEW va AS SELECT * FROM vb")
            .unwrap(),
    );

    let err = conn.query("SELECT * FROM va").unwrap_err();
    assert!(matches!(err, SqlError::CircularViewReference(_)));
}

#[test]
fn self_referencing_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute("CREATE VIEW v AS SELECT 1").unwrap());
    assert_ok(
        conn.execute("CREATE OR REPLACE VIEW v AS SELECT * FROM v")
            .unwrap(),
    );

    let err = conn.query("SELECT * FROM v").unwrap_err();
    assert!(matches!(err, SqlError::CircularViewReference(_)));
}

// ═══════════════════════════════════════════════════════════════════
//  DML on views — errors
// ═══════════════════════════════════════════════════════════════════

#[test]
fn insert_into_view_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let err = conn
        .execute("INSERT INTO v (id, name, email, age) VALUES (4, 'Dave', 'd@t.com', 40)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CannotModifyView(_)));
}

#[test]
fn update_view_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let err = conn
        .execute("UPDATE v SET age = 99 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::CannotModifyView(_)));
}

#[test]
fn delete_from_view_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let err = conn.execute("DELETE FROM v WHERE id = 1").unwrap_err();
    assert!(matches!(err, SqlError::CannotModifyView(_)));
}

// ═══════════════════════════════════════════════════════════════════
//  Persistence
// ═══════════════════════════════════════════════════════════════════

#[test]
fn view_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users(&mut conn);
        assert_ok(
            conn.execute("CREATE VIEW v AS SELECT * FROM users WHERE age >= 30")
                .unwrap(),
        );
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT * FROM v").unwrap();
        assert_eq!(qr.rows.len(), 2);
    }
}

#[test]
fn view_drop_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users(&mut conn);
        assert_ok(
            conn.execute("CREATE VIEW v AS SELECT * FROM users")
                .unwrap(),
        );
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        assert_ok(conn.execute("DROP VIEW v").unwrap());
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let err = conn.query("SELECT * FROM v").unwrap_err();
        assert!(matches!(err, SqlError::TableNotFound(_)));
    }
}

#[test]
fn view_reflects_new_data_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users(&mut conn);
        assert_ok(
            conn.execute("CREATE VIEW v AS SELECT * FROM users")
                .unwrap(),
        );
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        assert_rows_affected(
            conn.execute(
                "INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'd@t.com', 40)",
            )
            .unwrap(),
            1,
        );

        let qr = conn.query("SELECT COUNT(*) FROM v").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(4));
    }
}

#[test]
fn multiple_views_persist() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_users(&mut conn);
        assert_ok(
            conn.execute("CREATE VIEW v1 AS SELECT * FROM users WHERE age < 30")
                .unwrap(),
        );
        assert_ok(
            conn.execute("CREATE VIEW v2 AS SELECT * FROM users WHERE age >= 30")
                .unwrap(),
        );
    }
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr1 = conn.query("SELECT COUNT(*) FROM v1").unwrap();
        assert_eq!(qr1.rows[0][0], Value::Integer(1));
        let qr2 = conn.query("SELECT COUNT(*) FROM v2").unwrap();
        assert_eq!(qr2.rows[0][0], Value::Integer(2));
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Transactions
// ═══════════════════════════════════════════════════════════════════

#[test]
fn create_view_in_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn create_view_in_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    conn.execute("ROLLBACK").unwrap();

    let err = conn.query("SELECT * FROM v").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn drop_view_in_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    assert_ok(conn.execute("DROP VIEW v").unwrap());
    conn.execute("ROLLBACK").unwrap();

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn select_view_inside_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);
    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.rows.len(), 3);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn create_table_and_view_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 42)")
            .unwrap(),
        1,
    );
    assert_ok(conn.execute("CREATE VIEW v AS SELECT * FROM t").unwrap());
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Integer(42));
}

// ═══════════════════════════════════════════════════════════════════
//  DROP TABLE cascade
// ═══════════════════════════════════════════════════════════════════

#[test]
fn drop_table_view_breaks() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(conn.execute("DROP TABLE users").unwrap());

    let err = conn.query("SELECT * FROM v").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn drop_table_recreate_view_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(conn.execute("DROP TABLE users").unwrap());

    // Recreate with same schema
    setup_users(&mut conn);

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn drop_table_recreate_different_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );
    assert_ok(conn.execute("DROP TABLE users").unwrap());

    // Recreate with different schema
    assert_ok(
        conn.execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, username TEXT)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO users (id, username) VALUES (1, 'alice')")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.columns.len(), 2);
    assert_eq!(qr.rows.len(), 1);
}

// ═══════════════════════════════════════════════════════════════════
//  CTE + View interaction
// ═══════════════════════════════════════════════════════════════════

#[test]
fn cte_body_references_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users WHERE age >= 30")
            .unwrap(),
    );

    let qr = conn
        .query("WITH filtered AS (SELECT * FROM v WHERE name = 'Alice') SELECT * FROM filtered")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn cte_same_name_as_view_takes_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    // CTE named 'v' should override the view
    let qr = conn
        .query("WITH v AS (SELECT 42 AS val) SELECT * FROM v")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn view_body_contains_cte() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(conn.execute(
        "CREATE VIEW v AS WITH adults AS (SELECT * FROM users WHERE age >= 30) SELECT * FROM adults"
    ).unwrap());

    let qr = conn.query("SELECT * FROM v").unwrap();
    assert_eq!(qr.rows.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════
//  View fusion verification
// ═══════════════════════════════════════════════════════════════════

#[test]
fn simple_view_fuses_with_outer_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users WHERE age >= 25")
            .unwrap(),
    );

    // This should fuse: SELECT * FROM users WHERE age >= 25 AND age <= 30
    let qr = conn.query("SELECT * FROM v WHERE age <= 30").unwrap();
    assert_eq!(qr.rows.len(), 2); // Alice(30) + Bob(25)
}

#[test]
fn view_new_data_visible() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT COUNT(*) FROM v").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));

    assert_rows_affected(
        conn.execute("INSERT INTO users (id, name, email, age) VALUES (4, 'Dave', 'd@t.com', 40)")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT COUNT(*) FROM v").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(4));
}
