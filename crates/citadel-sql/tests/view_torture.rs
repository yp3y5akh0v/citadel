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
//  Stress tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn stress_many_views() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    for i in 0..20 {
        assert_ok(
            conn.execute(&format!(
                "CREATE VIEW v{i} AS SELECT * FROM users WHERE age >= {}",
                20 + i
            ))
            .unwrap(),
        );
    }

    for i in 0..20 {
        let qr = conn.query(&format!("SELECT COUNT(*) FROM v{i}")).unwrap();
        let count = match &qr.rows[0][0] {
            Value::Integer(n) => *n,
            _ => panic!("expected integer"),
        };
        assert!((0..=3).contains(&count));
    }
}

#[test]
fn stress_deeply_nested_views() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v0 AS SELECT * FROM users")
            .unwrap(),
    );
    for i in 1..=5 {
        assert_ok(
            conn.execute(&format!("CREATE VIEW v{i} AS SELECT * FROM v{}", i - 1))
                .unwrap(),
        );
    }

    let qr = conn.query("SELECT COUNT(*) FROM v5").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn stress_view_on_large_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE big (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    for i in 0..200 {
        assert_rows_affected(
            conn.execute(&format!(
                "INSERT INTO big (id, val) VALUES ({i}, {})",
                i % 10
            ))
            .unwrap(),
            1,
        );
    }
    conn.execute("COMMIT").unwrap();

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM big WHERE val < 5")
            .unwrap(),
    );

    let qr = conn.query("SELECT COUNT(*) FROM v").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(100));
}

#[test]
fn stress_create_drop_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    for _ in 0..10 {
        assert_ok(
            conn.execute("CREATE VIEW v AS SELECT * FROM users")
                .unwrap(),
        );
        let qr = conn.query("SELECT COUNT(*) FROM v").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(3));
        assert_ok(conn.execute("DROP VIEW v").unwrap());
    }
}

#[test]
fn stress_multiple_selects_different_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v WHERE age = 25").unwrap();
    assert_eq!(qr.rows.len(), 1);

    let qr = conn.query("SELECT * FROM v WHERE age = 30").unwrap();
    assert_eq!(qr.rows.len(), 1);

    let qr = conn.query("SELECT * FROM v WHERE age > 24").unwrap();
    assert_eq!(qr.rows.len(), 3);

    let qr = conn.query("SELECT * FROM v WHERE name LIKE 'A%'").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn stress_view_over_indexed_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(conn.execute("CREATE INDEX idx_age ON users (age)").unwrap());
    assert_ok(
        conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)")
            .unwrap(),
    );

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v WHERE age = 30").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  Complex interactions
// ═══════════════════════════════════════════════════════════════════

#[test]
fn view_with_prepared_stmt() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn
        .query_params("SELECT * FROM v WHERE age > $1", &[Value::Integer(28)])
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn view_union_with_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(conn.execute(
        "CREATE TABLE admins (id INTEGER NOT NULL PRIMARY KEY, name TEXT, email TEXT, age INTEGER)"
    ).unwrap());
    assert_rows_affected(
        conn.execute(
            "INSERT INTO admins (id, name, email, age) VALUES (100, 'Root', 'root@test.com', 50)",
        )
        .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT id, name FROM users")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT id, name FROM v UNION ALL SELECT id, name FROM admins")
        .unwrap();
    assert_eq!(qr.rows.len(), 4);
}

#[test]
fn view_in_cte_body() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW adults AS SELECT * FROM users WHERE age >= 30")
            .unwrap(),
    );

    let qr = conn.query(
        "WITH adult_names AS (SELECT name FROM adults) SELECT name FROM adult_names ORDER BY name"
    ).unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Charlie".into()));
}

#[test]
fn view_with_case_between_like() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT name, CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END AS tier FROM v")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);

    let qr = conn
        .query("SELECT * FROM v WHERE age BETWEEN 25 AND 30")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);

    let qr = conn
        .query("SELECT * FROM v WHERE name LIKE '%li%'")
        .unwrap();
    assert_eq!(qr.rows.len(), 2); // Alice, Charlie
}

#[test]
fn view_with_coalesce_cast() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT COALESCE(email, 'none') FROM v WHERE id = 1")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("alice@test.com".into()));

    let qr = conn
        .query("SELECT CAST(age AS REAL) FROM v WHERE id = 1")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Real(30.0));
}

#[test]
fn view_over_constrained_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, price REAL CHECK (price > 0))"
    ).unwrap());
    assert_rows_affected(
        conn.execute("INSERT INTO items (id, name, price) VALUES (1, 'Widget', 9.99)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO items (id, name, price) VALUES (2, 'Gadget', 19.99)")
            .unwrap(),
        1,
    );

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM items")
            .unwrap(),
    );

    let qr = conn.query("SELECT * FROM v ORDER BY price").unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Text("Widget".into()));
}

#[test]
fn explain_view_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    let qr = conn.query("EXPLAIN SELECT * FROM v WHERE id = 1").unwrap();
    assert!(!qr.rows.is_empty());
    // After fusion, should reference real table "users"
    let plan = match &qr.rows[0][0] {
        Value::Text(s) => s.to_string(),
        _ => panic!("expected text"),
    };
    assert!(
        plan.contains("users"),
        "plan should reference real table: {plan}"
    );
}

// ═══════════════════════════════════════════════════════════════════
//  Performance: fusion verification
// ═══════════════════════════════════════════════════════════════════

#[test]
fn fusion_simple_view_merges_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW active AS SELECT * FROM users WHERE age >= 25")
            .unwrap(),
    );

    // After fusion: SELECT * FROM users WHERE age >= 25 AND age <= 30
    let qr = conn
        .query("SELECT name FROM active WHERE age <= 30 ORDER BY name")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Bob".into()));
}

#[test]
fn fusion_view_with_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    assert_ok(
        conn.execute("CREATE VIEW v AS SELECT * FROM users")
            .unwrap(),
    );

    // After fusion: SELECT * FROM users WHERE id = 1 → PK lookup
    let qr = conn.query("SELECT name FROM v WHERE id = 1").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));

    // Verify via EXPLAIN
    let explain = conn
        .query("EXPLAIN SELECT name FROM v WHERE id = 1")
        .unwrap();
    let plan = match &explain.rows[0][0] {
        Value::Text(s) => s.to_string(),
        _ => panic!("expected text"),
    };
    assert!(
        plan.contains("PRIMARY KEY") || plan.contains("SEARCH"),
        "should use PK: {plan}"
    );
}

#[test]
fn complex_view_does_not_fuse() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_users(&mut conn);

    // View with GROUP BY — cannot fuse
    assert_ok(
        conn.execute(
            "CREATE VIEW age_counts AS SELECT age, COUNT(*) AS cnt FROM users GROUP BY age",
        )
        .unwrap(),
    );

    let qr = conn.query("SELECT * FROM age_counts ORDER BY age").unwrap();
    assert_eq!(qr.rows.len(), 3);

    // View with DISTINCT — cannot fuse
    assert_ok(
        conn.execute("CREATE VIEW unique_ages AS SELECT DISTINCT age FROM users")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT * FROM unique_ages ORDER BY age")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn persistence_cycle_with_views() {
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

    for _ in 0..3 {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT COUNT(*) FROM v").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(2));
    }
}
