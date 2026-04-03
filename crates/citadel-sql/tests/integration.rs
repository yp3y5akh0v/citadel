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

// ── Basic CRUD workflow ────────────────────────────────────────────

#[test]
fn full_crud_workflow() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // CREATE TABLE
    assert_ok(
        conn.execute(
            "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)",
        )
        .unwrap(),
    );

    // INSERT
    assert_rows_affected(
        conn.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")
            .unwrap(),
        1,
    );

    // SELECT all
    let qr = conn.query("SELECT * FROM users").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.columns.len(), 3);
    assert_eq!(qr.columns[0], "id");
    assert_eq!(qr.columns[1], "name");
    assert_eq!(qr.columns[2], "age");

    // SELECT WHERE
    let qr = conn.query("SELECT name FROM users WHERE age > 28").unwrap();
    assert_eq!(qr.rows.len(), 2);

    // UPDATE
    assert_rows_affected(
        conn.execute("UPDATE users SET age = 31 WHERE id = 1")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT age FROM users WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(31));

    // DELETE
    assert_rows_affected(conn.execute("DELETE FROM users WHERE id = 2").unwrap(), 1);

    let qr = conn.query("SELECT * FROM users").unwrap();
    assert_eq!(qr.rows.len(), 2);

    // DROP TABLE
    assert_ok(conn.execute("DROP TABLE users").unwrap());
    assert!(conn.tables().is_empty());
}

// ── Persistence across reopen ──────────────────────────────────────

#[test]
fn persistence_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    // Create and populate
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, label TEXT NOT NULL)")
            .unwrap();
        conn.execute("INSERT INTO items (id, label) VALUES (1, 'alpha')")
            .unwrap();
        conn.execute("INSERT INTO items (id, label) VALUES (2, 'beta')")
            .unwrap();
        conn.execute("INSERT INTO items (id, label) VALUES (3, 'gamma')")
            .unwrap();
    }

    // Reopen and verify
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let tables = conn.tables();
        assert_eq!(tables.len(), 1);

        let qr = conn.query("SELECT * FROM items ORDER BY id").unwrap();
        assert_eq!(qr.rows.len(), 3);
        assert_eq!(qr.rows[0][0], Value::Integer(1));
        assert_eq!(qr.rows[0][1], Value::Text("alpha".into()));
        assert_eq!(qr.rows[2][0], Value::Integer(3));
        assert_eq!(qr.rows[2][1], Value::Text("gamma".into()));
    }
}

// ── Multi-table isolation ──────────────────────────────────────────

#[test]
fn multi_table_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE products (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, product_id INTEGER)")
        .unwrap();

    conn.execute("INSERT INTO products (id, name) VALUES (1, 'Widget')")
        .unwrap();
    conn.execute("INSERT INTO products (id, name) VALUES (2, 'Gadget')")
        .unwrap();
    conn.execute("INSERT INTO orders (id, product_id) VALUES (100, 1)")
        .unwrap();

    let products = conn.query("SELECT * FROM products").unwrap();
    let orders = conn.query("SELECT * FROM orders").unwrap();
    assert_eq!(products.rows.len(), 2);
    assert_eq!(orders.rows.len(), 1);

    // Dropping one table doesn't affect the other
    conn.execute("DROP TABLE orders").unwrap();
    let products = conn.query("SELECT * FROM products").unwrap();
    assert_eq!(products.rows.len(), 2);

    assert_eq!(conn.tables().len(), 1);
}

// ── ORDER BY ───────────────────────────────────────────────────────

#[test]
fn order_by_ascending_descending() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE scores (id INTEGER NOT NULL PRIMARY KEY, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO scores VALUES (1, 80)").unwrap();
    conn.execute("INSERT INTO scores VALUES (2, 95)").unwrap();
    conn.execute("INSERT INTO scores VALUES (3, 70)").unwrap();
    conn.execute("INSERT INTO scores VALUES (4, 90)").unwrap();

    // ASC
    let qr = conn
        .query("SELECT id, score FROM scores ORDER BY score ASC")
        .unwrap();
    assert_eq!(qr.rows[0][1], Value::Integer(70));
    assert_eq!(qr.rows[3][1], Value::Integer(95));

    // DESC
    let qr = conn
        .query("SELECT id, score FROM scores ORDER BY score DESC")
        .unwrap();
    assert_eq!(qr.rows[0][1], Value::Integer(95));
    assert_eq!(qr.rows[3][1], Value::Integer(70));
}

// ── LIMIT / OFFSET ─────────────────────────────────────────────────

#[test]
fn limit_and_offset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE nums (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO nums VALUES ({i})"))
            .unwrap();
    }

    let qr = conn
        .query("SELECT id FROM nums ORDER BY id LIMIT 3")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[2][0], Value::Integer(3));

    let qr = conn
        .query("SELECT id FROM nums ORDER BY id LIMIT 3 OFFSET 5")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(6));
    assert_eq!(qr.rows[2][0], Value::Integer(8));

    // OFFSET past end
    let qr = conn
        .query("SELECT id FROM nums ORDER BY id OFFSET 100")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);

    // Negative LIMIT clamped to 0
    let qr = conn
        .query("SELECT id FROM nums ORDER BY id LIMIT -1")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);

    // Negative OFFSET clamped to 0
    let qr = conn
        .query("SELECT id FROM nums ORDER BY id LIMIT 3 OFFSET -5")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

// ── Aggregation ────────────────────────────────────────────────────

#[test]
fn aggregate_functions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE sales (id INTEGER NOT NULL PRIMARY KEY, amount REAL, region TEXT NOT NULL)",
    )
    .unwrap();
    conn.execute("INSERT INTO sales VALUES (1, 100.0, 'east')")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (2, 200.0, 'west')")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (3, 150.0, 'east')")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (4, 300.0, 'west')")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (5, 50.0, 'east')")
        .unwrap();

    // COUNT(*)
    let qr = conn.query("SELECT COUNT(*) FROM sales").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));

    // SUM
    let qr = conn.query("SELECT SUM(amount) FROM sales").unwrap();
    assert_eq!(qr.rows[0][0], Value::Real(800.0));

    // AVG
    let qr = conn.query("SELECT AVG(amount) FROM sales").unwrap();
    assert_eq!(qr.rows[0][0], Value::Real(160.0));

    // MIN / MAX
    let qr = conn
        .query("SELECT MIN(amount), MAX(amount) FROM sales")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Real(50.0));
    assert_eq!(qr.rows[0][1], Value::Real(300.0));
}

#[test]
fn group_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE sales (id INTEGER NOT NULL PRIMARY KEY, amount REAL, region TEXT NOT NULL)",
    )
    .unwrap();
    conn.execute("INSERT INTO sales VALUES (1, 100.0, 'east')")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (2, 200.0, 'west')")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (3, 150.0, 'east')")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (4, 300.0, 'west')")
        .unwrap();

    let qr = conn
        .query("SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    // east: 100+150=250
    assert_eq!(qr.rows[0][0], Value::Text("east".into()));
    assert_eq!(qr.rows[0][1], Value::Real(250.0));
    // west: 200+300=500
    assert_eq!(qr.rows[1][0], Value::Text("west".into()));
    assert_eq!(qr.rows[1][1], Value::Real(500.0));
}

#[test]
fn group_by_having() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE sales (id INTEGER NOT NULL PRIMARY KEY, amount INTEGER, region TEXT NOT NULL)"
    ).unwrap();
    conn.execute("INSERT INTO sales VALUES (1, 100, 'east')")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (2, 200, 'west')")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (3, 150, 'east')")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (4, 300, 'west')")
        .unwrap();

    let qr = conn
        .query("SELECT region, SUM(amount) AS total FROM sales GROUP BY region HAVING total > 300")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("west".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(500));
}

// ── NULL handling ──────────────────────────────────────────────────

#[test]
fn null_handling() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE data (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO data VALUES (1, 'hello')")
        .unwrap();
    conn.execute("INSERT INTO data VALUES (2, NULL)").unwrap();
    conn.execute("INSERT INTO data VALUES (3, 'world')")
        .unwrap();

    // IS NULL
    let qr = conn.query("SELECT id FROM data WHERE val IS NULL").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));

    // IS NOT NULL
    let qr = conn
        .query("SELECT id FROM data WHERE val IS NOT NULL")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);

    // COUNT(*) vs COUNT(col) — COUNT(col) skips NULLs
    let qr = conn.query("SELECT COUNT(*), COUNT(val) FROM data").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[0][1], Value::Integer(2));
}

// ── Boolean type ───────────────────────────────────────────────────

#[test]
fn boolean_type() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE flags (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO flags VALUES (1, TRUE)").unwrap();
    conn.execute("INSERT INTO flags VALUES (2, FALSE)").unwrap();
    conn.execute("INSERT INTO flags VALUES (3, TRUE)").unwrap();

    let qr = conn
        .query("SELECT id FROM flags WHERE active = TRUE")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);

    let qr = conn
        .query("SELECT COUNT(*) FROM flags WHERE active = FALSE")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

// ── Complex WHERE expressions ──────────────────────────────────────

#[test]
fn complex_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, price REAL, category TEXT NOT NULL)",
    )
    .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 10.0, 'food')")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (2, 50.0, 'electronics')")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (3, 5.0, 'food')")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (4, 100.0, 'electronics')")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (5, 25.0, 'clothing')")
        .unwrap();

    // AND
    let qr = conn
        .query("SELECT id FROM items WHERE category = 'food' AND price > 7.0")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));

    // OR
    let qr = conn
        .query("SELECT id FROM items WHERE category = 'food' OR category = 'clothing' ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);

    // Arithmetic in WHERE
    let qr = conn
        .query("SELECT id FROM items WHERE price * 2 > 90.0 ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(4));
}

// ── UPDATE multiple columns ────────────────────────────────────────

#[test]
fn update_multiple_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 100)")
        .unwrap();

    assert_rows_affected(
        conn.execute("UPDATE users SET name = 'Alicia', score = 200 WHERE id = 1")
            .unwrap(),
        1,
    );

    let qr = conn
        .query("SELECT name, score FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("Alicia".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(200));
}

// ── UPDATE evaluates SET against original row (SQL standard) ──────

#[test]
fn update_set_evaluates_against_original_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();

    // SET a = b, b = a should swap (both evaluated against original row)
    assert_rows_affected(
        conn.execute("UPDATE t SET a = b, b = a WHERE id = 1")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT a, b FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(20)); // a was 10, now has b's original value
    assert_eq!(qr.rows[0][1], Value::Integer(10)); // b was 20, now has a's original value
}

// ── UPDATE with no matches ─────────────────────────────────────────

#[test]
fn update_no_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    assert_rows_affected(
        conn.execute("UPDATE t SET v = 99 WHERE id = 999").unwrap(),
        0,
    );
}

// ── DELETE all rows ────────────────────────────────────────────────

#[test]
fn delete_all_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    for i in 1..=5 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    assert_rows_affected(conn.execute("DELETE FROM t").unwrap(), 5);

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

// ── Error cases ────────────────────────────────────────────────────

#[test]
fn error_table_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let result = conn.execute("SELECT * FROM nonexistent");
    assert!(matches!(result, Err(SqlError::TableNotFound(_))));
}

#[test]
fn error_table_already_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    let result = conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)");
    assert!(matches!(result, Err(SqlError::TableAlreadyExists(_))));
}

#[test]
fn if_not_exists_and_if_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // IF NOT EXISTS — no error on duplicate
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    assert_ok(
        conn.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );

    // IF EXISTS — no error on missing
    assert_ok(conn.execute("DROP TABLE IF EXISTS nonexistent").unwrap());
}

#[test]
fn error_duplicate_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let result = conn.execute("INSERT INTO t VALUES (1)");
    assert!(matches!(result, Err(SqlError::DuplicateKey)));
}

#[test]
fn error_not_null_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    let result = conn.execute("INSERT INTO t (id) VALUES (1)");
    assert!(matches!(result, Err(SqlError::NotNullViolation(_))));
}

#[test]
fn error_column_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    let result = conn.execute("INSERT INTO t (nonexistent) VALUES (1)");
    assert!(matches!(result, Err(SqlError::ColumnNotFound(_))));
}

#[test]
fn error_primary_key_required() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let result = conn.execute("CREATE TABLE t (id INTEGER NOT NULL)");
    assert!(matches!(result, Err(SqlError::PrimaryKeyRequired)));
}

#[test]
fn error_drop_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let result = conn.execute("DROP TABLE nonexistent");
    assert!(matches!(result, Err(SqlError::TableNotFound(_))));
}

// ── Edge cases ─────────────────────────────────────────────────────

#[test]
fn empty_string_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '')").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("".into()));
}

#[test]
fn negative_integers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (-100, -999)").unwrap();
    conn.execute("INSERT INTO t VALUES (0, 0)").unwrap();
    conn.execute("INSERT INTO t VALUES (100, 999)").unwrap();

    let qr = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(-100));
    assert_eq!(qr.rows[0][1], Value::Integer(-999));
    assert_eq!(qr.rows[1][0], Value::Integer(0));
    assert_eq!(qr.rows[2][0], Value::Integer(100));
}

#[test]
fn real_precision() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val REAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 3.15159265358979)")
        .unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    match &qr.rows[0][0] {
        Value::Real(r) => assert!((*r - 3.15159265358979).abs() < 1e-12),
        other => panic!("expected Real, got {other:?}"),
    }
}

#[test]
fn case_insensitive_identifiers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE MyTable (ID INTEGER NOT NULL PRIMARY KEY, Name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO MYTABLE (id, name) VALUES (1, 'test')")
        .unwrap();

    let qr = conn.query("SELECT NAME FROM mytable WHERE ID = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("test".into()));
}

#[test]
fn insert_many_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    for i in 0..100 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {0})", i * 10))
            .unwrap();
    }

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(100));

    // Verify ordering
    let qr = conn.query("SELECT id FROM t ORDER BY id LIMIT 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));

    let qr = conn
        .query("SELECT id FROM t ORDER BY id DESC LIMIT 1")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(99));
}

#[test]
fn select_with_expression_in_projection() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, price REAL, qty INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10.5, 3)").unwrap();

    let qr = conn
        .query("SELECT price * qty FROM t WHERE id = 1")
        .unwrap();
    match &qr.rows[0][0] {
        Value::Real(r) => assert!((*r - 31.5).abs() < 1e-10),
        other => panic!("expected Real, got {other:?}"),
    }
}

#[test]
fn composite_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (a INTEGER NOT NULL, b INTEGER NOT NULL, val TEXT, PRIMARY KEY (a, b))",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1, 'one-one')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2, 'one-two')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 1, 'two-one')")
        .unwrap();

    let qr = conn
        .query("SELECT val FROM t WHERE a = 1 ORDER BY b")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("one-one".into()));
    assert_eq!(qr.rows[1][0], Value::Text("one-two".into()));

    // Duplicate composite PK
    let result = conn.execute("INSERT INTO t VALUES (1, 1, 'dup')");
    assert!(matches!(result, Err(SqlError::DuplicateKey)));
}

#[test]
fn update_with_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    conn.execute("UPDATE t SET score = score + 50 WHERE id = 1")
        .unwrap();

    let qr = conn.query("SELECT score FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(150));
}

#[test]
fn select_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 0);
    assert_eq!(qr.columns, vec!["id"]);

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

#[test]
fn multi_row_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .unwrap(),
        3,
    );

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn comparison_operators() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    let qr = conn.query("SELECT COUNT(*) FROM t WHERE id >= 5").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(6));

    let qr = conn.query("SELECT COUNT(*) FROM t WHERE id <= 3").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));

    let qr = conn.query("SELECT COUNT(*) FROM t WHERE id <> 5").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(9));

    let qr = conn.query("SELECT COUNT(*) FROM t WHERE id = 7").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn not_operator() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, TRUE)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, FALSE)").unwrap();

    let qr = conn.query("SELECT id FROM t WHERE NOT active").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn modulo_and_division() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 17)").unwrap();

    let qr = conn.query("SELECT val % 5 FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));

    let qr = conn.query("SELECT val / 5 FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn tables_listing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert!(conn.tables().is_empty());

    conn.execute("CREATE TABLE alpha (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE beta (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE gamma (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();

    let mut tables = conn.tables();
    tables.sort();
    assert_eq!(tables, vec!["alpha", "beta", "gamma"]);

    conn.execute("DROP TABLE beta").unwrap();
    let mut tables = conn.tables();
    tables.sort();
    assert_eq!(tables, vec!["alpha", "gamma"]);
}

// ── HAVING with alias and aggregate expressions ─────────────────────

#[test]
fn having_with_count_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, customer TEXT NOT NULL, amount INTEGER)"
    ).unwrap();
    conn.execute("INSERT INTO orders VALUES (1, 'alice', 50)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (2, 'alice', 75)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (3, 'bob', 100)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (4, 'alice', 25)")
        .unwrap();

    let qr = conn
        .query("SELECT customer, COUNT(*) AS cnt FROM orders GROUP BY customer HAVING cnt > 1")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(3));
}

#[test]
fn having_with_avg_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE scores (id INTEGER NOT NULL PRIMARY KEY, team TEXT NOT NULL, score REAL NOT NULL)"
    ).unwrap();
    conn.execute("INSERT INTO scores VALUES (1, 'red', 8.5)")
        .unwrap();
    conn.execute("INSERT INTO scores VALUES (2, 'red', 9.0)")
        .unwrap();
    conn.execute("INSERT INTO scores VALUES (3, 'blue', 3.0)")
        .unwrap();
    conn.execute("INSERT INTO scores VALUES (4, 'blue', 4.0)")
        .unwrap();

    let qr = conn
        .query(
            "SELECT team, AVG(score) AS avg_score FROM scores GROUP BY team HAVING avg_score > 5.0",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("red".into()));
}

#[test]
fn having_with_min_max_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE temps (id INTEGER NOT NULL PRIMARY KEY, city TEXT NOT NULL, temp INTEGER NOT NULL)"
    ).unwrap();
    conn.execute("INSERT INTO temps VALUES (1, 'nyc', 30)")
        .unwrap();
    conn.execute("INSERT INTO temps VALUES (2, 'nyc', 90)")
        .unwrap();
    conn.execute("INSERT INTO temps VALUES (3, 'la', 60)")
        .unwrap();
    conn.execute("INSERT INTO temps VALUES (4, 'la', 70)")
        .unwrap();

    let qr = conn
        .query("SELECT city, MAX(temp) AS high FROM temps GROUP BY city HAVING high >= 90")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("nyc".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(90));

    let qr = conn
        .query("SELECT city, MIN(temp) AS low FROM temps GROUP BY city HAVING low > 50")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("la".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(60));
}

#[test]
fn having_aggregate_expr_and_alias_combined() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, cat TEXT NOT NULL, price INTEGER NOT NULL)"
    ).unwrap();
    for i in 1..=5 {
        conn.execute(&format!("INSERT INTO items VALUES ({i}, 'a', {i}0)"))
            .unwrap();
    }
    for i in 6..=7 {
        conn.execute(&format!("INSERT INTO items VALUES ({i}, 'b', 5)"))
            .unwrap();
    }

    let qr = conn
        .query("SELECT cat, SUM(price) AS total FROM items GROUP BY cat HAVING SUM(price) > 100")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("a".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(150));

    let qr = conn
        .query("SELECT cat, SUM(price) AS total FROM items GROUP BY cat HAVING total > 100")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("a".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(150));
}

// ── DISTINCT ──────────────────────────────────────────────────────

#[test]
fn distinct_basic_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, color TEXT NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'red')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'blue')").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'red')").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 'green')").unwrap();
    conn.execute("INSERT INTO t VALUES (5, 'blue')").unwrap();

    let qr = conn
        .query("SELECT DISTINCT color FROM t ORDER BY color")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Text("blue".into()));
    assert_eq!(qr.rows[1][0], Value::Text("green".into()));
    assert_eq!(qr.rows[2][0], Value::Text("red".into()));
}

#[test]
fn distinct_no_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    conn.execute("INSERT INTO t VALUES (3)").unwrap();

    let qr = conn.query("SELECT DISTINCT id FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn distinct_all_same() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 42)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 42)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 42)").unwrap();

    let qr = conn.query("SELECT DISTINCT val FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn distinct_with_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (5, 20)").unwrap();

    let qr = conn
        .query("SELECT DISTINCT val FROM t ORDER BY val")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert!(qr.rows[0][0].is_null());
    assert_eq!(qr.rows[1][0], Value::Integer(10));
    assert_eq!(qr.rows[2][0], Value::Integer(20));
}

#[test]
fn distinct_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a TEXT, b INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'y', 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'x', 10)").unwrap();

    let qr = conn.query("SELECT DISTINCT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn distinct_multi_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a TEXT NOT NULL, b INTEGER NOT NULL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'x', 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'x', 2)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'y', 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 'x', 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (5, 'y', 1)").unwrap();

    let qr = conn
        .query("SELECT DISTINCT a, b FROM t ORDER BY a, b")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0], vec![Value::Text("x".into()), Value::Integer(1)]);
    assert_eq!(qr.rows[1], vec![Value::Text("x".into()), Value::Integer(2)]);
    assert_eq!(qr.rows[2], vec![Value::Text("y".into()), Value::Integer(1)]);
}

#[test]
fn distinct_with_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", i % 3))
            .unwrap();
    }

    let qr = conn
        .query("SELECT DISTINCT val FROM t ORDER BY val DESC")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(1));
    assert_eq!(qr.rows[2][0], Value::Integer(0));
}

#[test]
fn distinct_with_limit_offset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();
    for i in 1..=20 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", i % 5))
            .unwrap();
    }

    let qr = conn
        .query("SELECT DISTINCT val FROM t ORDER BY val LIMIT 2")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(0));
    assert_eq!(qr.rows[1][0], Value::Integer(1));

    let qr = conn
        .query("SELECT DISTINCT val FROM t ORDER BY val LIMIT 2 OFFSET 2")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(3));

    let qr = conn
        .query("SELECT DISTINCT val FROM t ORDER BY val OFFSET 4")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(4));
}

#[test]
fn distinct_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();

    let qr = conn.query("SELECT DISTINCT val FROM t").unwrap();
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn distinct_with_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 30)").unwrap();

    let qr = conn
        .query("SELECT DISTINCT val * 2 FROM t ORDER BY val * 2")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(20));
    assert_eq!(qr.rows[1][0], Value::Integer(40));
    assert_eq!(qr.rows[2][0], Value::Integer(60));
}

#[test]
fn distinct_with_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, grp TEXT NOT NULL, val INTEGER NOT NULL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'a', 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'b', 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 'b', 20)").unwrap();

    let qr = conn
        .query("SELECT DISTINCT SUM(val) FROM t GROUP BY grp")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(30));
}

#[test]
fn distinct_boolean_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, flag BOOLEAN NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, TRUE)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, FALSE)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, TRUE)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, FALSE)").unwrap();
    conn.execute("INSERT INTO t VALUES (5, TRUE)").unwrap();

    let qr = conn
        .query("SELECT DISTINCT flag FROM t ORDER BY flag")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Boolean(false));
    assert_eq!(qr.rows[1][0], Value::Boolean(true));
}
