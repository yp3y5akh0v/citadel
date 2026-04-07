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

fn setup_and_populate(conn: &mut Connection<'_>) {
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL, active BOOLEAN)").unwrap();
    let sql = "INSERT INTO items (id, name, price, active) VALUES ($1, $2, $3, $4)";
    for i in 1..=100 {
        conn.execute_params(
            sql,
            &[
                Value::Integer(i),
                Value::Text(format!("item_{i}").into()),
                Value::Real(i as f64 * 1.5),
                Value::Boolean(i % 3 != 0),
            ],
        )
        .unwrap();
    }
}

// ── Scale ───────────────────────────────────────────────────────────

#[test]
fn repeated_parameterized_select_1000() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let sql = "SELECT name FROM items WHERE id = $1";
    for i in 1..=100 {
        let qr = query_result(conn.execute_params(sql, &[Value::Integer(i)]).unwrap());
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(qr.rows[0][0], Value::Text(format!("item_{i}").into()));
    }
}

#[test]
fn repeated_parameterized_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let sql = "INSERT INTO t (id, val) VALUES ($1, $2)";
    for i in 1..=200 {
        conn.execute_params(
            sql,
            &[Value::Integer(i), Value::Text(format!("v{i}").into())],
        )
        .unwrap();
    }

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(200));
}

// ── Type variation ──────────────────────────────────────────────────

#[test]
fn same_param_slot_different_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let qr = query_result(
        conn.execute_params("SELECT id FROM items WHERE id = $1", &[Value::Integer(5)])
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);

    let qr = query_result(
        conn.execute_params(
            "SELECT id FROM items WHERE name = $1",
            &[Value::Text("item_5".into())],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
}

// ── Interleaved DML ─────────────────────────────────────────────────

#[test]
fn interleave_insert_select_with_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let insert_sql = "INSERT INTO t (id, name) VALUES ($1, $2)";
    let select_sql = "SELECT name FROM t WHERE id = $1";

    for i in 1..=10 {
        conn.execute_params(
            insert_sql,
            &[Value::Integer(i), Value::Text(format!("n{i}").into())],
        )
        .unwrap();
        let qr = query_result(
            conn.execute_params(select_sql, &[Value::Integer(i)])
                .unwrap(),
        );
        assert_eq!(qr.rows[0][0], Value::Text(format!("n{i}").into()));
    }
}

#[test]
fn all_dml_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    conn.execute_params(
        "INSERT INTO t (id, val) VALUES ($1, $2)",
        &[Value::Integer(1), Value::Text("a".into())],
    )
    .unwrap();
    conn.execute_params(
        "UPDATE t SET val = $2 WHERE id = $1",
        &[Value::Integer(1), Value::Text("b".into())],
    )
    .unwrap();

    let qr = query_result(
        conn.execute_params("SELECT val FROM t WHERE id = $1", &[Value::Integer(1)])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Text("b".into()));

    conn.execute_params("DELETE FROM t WHERE id = $1", &[Value::Integer(1)])
        .unwrap();
    let qr = query_result(conn.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

// ── Large param count ───────────────────────────────────────────────

#[test]
fn twenty_params_in_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE wide (id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT, c9 TEXT, c10 TEXT)").unwrap();

    let sql = "INSERT INTO wide (id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
    let params: Vec<Value> = std::iter::once(Value::Integer(1))
        .chain((1..=10).map(|i| Value::Text(format!("val{i}").into())))
        .collect();
    conn.execute_params(sql, &params).unwrap();

    let qr = query_result(conn.execute("SELECT * FROM wide").unwrap());
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][5], Value::Text("val5".into()));
}

// ── Cache capacity ──────────────────────────────────────────────────

#[test]
fn cache_eviction_under_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 100)")
        .unwrap();

    for i in 0..70 {
        let sql = format!("SELECT val + {i} FROM t WHERE id = 1");
        conn.execute(&sql).unwrap();
    }

    let qr = query_result(conn.execute("SELECT val FROM t WHERE id = 1").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(100));
}

// ── Transaction + params ────────────────────────────────────────────

#[test]
fn params_in_transaction_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'original')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute_params(
        "UPDATE t SET val = $1 WHERE id = $2",
        &[Value::Text("changed".into()), Value::Integer(1)],
    )
    .unwrap();
    conn.execute("ROLLBACK").unwrap();

    let qr = query_result(conn.execute("SELECT val FROM t WHERE id = 1").unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("original".into()));
}

#[test]
fn params_in_multi_statement_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 1..=5 {
        conn.execute_params(
            "INSERT INTO t (id, val) VALUES ($1, $2)",
            &[Value::Integer(i), Value::Text(format!("item{i}").into())],
        )
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

// ── Subquery with params ────────────────────────────────────────────

#[test]
fn param_with_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();
    conn.execute("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100.0)")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > $1)",
            &[Value::Real(50.0)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
}

// ── CASE/COALESCE with params ───────────────────────────────────────

#[test]
fn param_in_case_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT CASE WHEN price > $1 THEN 'expensive' ELSE 'cheap' END FROM items WHERE id = 1",
            &[Value::Real(100.0)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Text("cheap".into()));
}

#[test]
fn param_in_coalesce() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, NULL)")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT COALESCE(val, $1) FROM t WHERE id = 1",
            &[Value::Text("default".into())],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Text("default".into()));
}

// ── CAST with params ────────────────────────────────────────────────

#[test]
fn param_in_cast() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val REAL)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 3.14)")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT CAST($1 AS REAL) FROM t WHERE id = 1",
            &[Value::Integer(42)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Real(42.0));
}

// ── Group by / having with params ───────────────────────────────────

#[test]
fn param_in_having() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT active, COUNT(*) FROM items GROUP BY active HAVING COUNT(*) > $1",
            &[Value::Integer(30)],
        )
        .unwrap(),
    );
    assert!(!qr.rows.is_empty());
}

// ── Persistence ─────────────────────────────────────────────────────

#[test]
fn params_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    {
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute_params(
            "INSERT INTO t (id, name) VALUES ($1, $2)",
            &[Value::Integer(1), Value::Text("persisted".into())],
        )
        .unwrap();
    }
    {
        let mut conn = Connection::open(&db).unwrap();
        let qr = query_result(
            conn.execute_params("SELECT name FROM t WHERE id = $1", &[Value::Integer(1)])
                .unwrap(),
        );
        assert_eq!(qr.rows[0][0], Value::Text("persisted".into()));
    }
}

// ── Mixed params and literals ───────────────────────────────────────

#[test]
fn mixed_params_and_literals() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT name FROM items WHERE price > $1 AND active = true AND id < 10",
            &[Value::Real(5.0)],
        )
        .unwrap(),
    );
    assert!(!qr.rows.is_empty());
}

// ── Duplicate key with params ───────────────────────────────────────

#[test]
fn duplicate_key_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute_params("INSERT INTO t (id) VALUES ($1)", &[Value::Integer(1)])
        .unwrap();
    let result = conn.execute_params("INSERT INTO t (id) VALUES ($1)", &[Value::Integer(1)]);
    assert!(result.is_err());
}

// ── SELECT $1 (no table) ────────────────────────────────────────────

#[test]
fn select_param_no_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = query_result(
        conn.execute_params("SELECT $1 + $2", &[Value::Integer(3), Value::Integer(4)])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(7));
}

// ── Boolean params ──────────────────────────────────────────────────

#[test]
fn boolean_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT COUNT(*) FROM items WHERE active = $1",
            &[Value::Boolean(true)],
        )
        .unwrap(),
    );
    assert!(matches!(qr.rows[0][0], Value::Integer(n) if n > 0));
}

// ── NOT NULL violation with params ──────────────────────────────────

#[test]
fn not_null_violation_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();

    let result = conn.execute_params(
        "INSERT INTO t (id, name) VALUES ($1, $2)",
        &[Value::Integer(1), Value::Null],
    );
    assert!(result.is_err());
}

// ── Schema generation / cache invalidation ─────────────────────────

#[test]
fn cache_invalidated_by_create_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t1 (id, val) VALUES (1, 'hello')")
        .unwrap();

    let sql = "SELECT val FROM t1 WHERE id = $1";
    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));

    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
        .unwrap();

    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
}

#[test]
fn cache_invalidated_by_drop_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_name ON t (name)").unwrap();
    conn.execute("INSERT INTO t (id, name) VALUES (1, 'Alice')")
        .unwrap();

    let sql = "SELECT id FROM t WHERE name = $1";
    let qr = query_result(
        conn.execute_params(sql, &[Value::Text("Alice".into())])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(1));

    conn.execute("DROP INDEX idx_name ON t").unwrap();

    let qr = query_result(
        conn.execute_params(sql, &[Value::Text("Alice".into())])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn multiple_ddl_operations_all_invalidate_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val, score) VALUES (1, 'x', 42)")
        .unwrap();

    let sql = "SELECT score FROM t WHERE id = $1";
    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(42));

    conn.execute("CREATE INDEX idx_val ON t (val)").unwrap();
    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(42));

    conn.execute("CREATE INDEX idx_score ON t (score)").unwrap();
    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(42));

    conn.execute("DROP INDEX idx_val ON t").unwrap();
    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(42));

    conn.execute("DROP INDEX idx_score ON t").unwrap();
    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn cache_survives_drop_of_unrelated_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t1 (id, val) VALUES (1, 'kept')")
        .unwrap();

    let sql = "SELECT val FROM t1 WHERE id = $1";
    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("kept".into()));

    conn.execute("DROP TABLE t2").unwrap();
    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("kept".into()));
}

// ── Rollback + cache ───────────────────────────────────────────────

#[test]
fn rollback_ddl_invalidates_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'original')")
        .unwrap();

    let sql = "SELECT val FROM t WHERE id = $1";
    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("original".into()));

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();

    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("original".into()));
}

// ── Parameter reuse and gaps ───────────────────────────────────────

#[test]
fn same_param_used_twice() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, lo INTEGER, hi INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t (id, lo, hi) VALUES (1, 5, 15)")
        .unwrap();
    conn.execute("INSERT INTO t (id, lo, hi) VALUES (2, 20, 30)")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT id FROM t WHERE lo <= $1 AND hi >= $1",
            &[Value::Integer(10)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn param_gap_in_indices() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t (id, a, b) VALUES (1, 10, 20)")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT id FROM t WHERE a = $1 AND b = $3",
            &[Value::Integer(10), Value::Integer(999), Value::Integer(20)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

// ── Param in EXISTS / NOT EXISTS / NOT IN / scalar subquery ────────

#[test]
fn param_in_exists_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();
    conn.execute("INSERT INTO orders (id, user_id, total) VALUES (1, 1, 200.0)")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE total > $1)",
            &[Value::Real(100.0)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn param_in_not_exists_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();
    conn.execute("INSERT INTO orders (id, user_id) VALUES (1, 1)")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.id > $1)",
            &[Value::Integer(999)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn param_in_not_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, ref_val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t1 (id, val) VALUES (1, 10)")
        .unwrap();
    conn.execute("INSERT INTO t1 (id, val) VALUES (2, 20)")
        .unwrap();
    conn.execute("INSERT INTO t1 (id, val) VALUES (3, 30)")
        .unwrap();
    conn.execute("INSERT INTO t2 (id, ref_val) VALUES (1, 10)")
        .unwrap();
    conn.execute("INSERT INTO t2 (id, ref_val) VALUES (2, 30)")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT val FROM t1 WHERE val NOT IN (SELECT ref_val FROM t2 WHERE ref_val > $1)",
            &[Value::Integer(5)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(20));
}

#[test]
fn param_in_scalar_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 100)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 200)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (3, 300)")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT id FROM t WHERE val > (SELECT val FROM t WHERE id = $1)",
            &[Value::Integer(1)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
}

// ── Param in IN list (no subquery) ─────────────────────────────────

#[test]
fn param_in_in_list_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, name) VALUES (1, 'a')")
        .unwrap();
    conn.execute("INSERT INTO t (id, name) VALUES (2, 'b')")
        .unwrap();
    conn.execute("INSERT INTO t (id, name) VALUES (3, 'c')")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT name FROM t WHERE id IN ($1, $2) ORDER BY id",
            &[Value::Integer(1), Value::Integer(3)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("a".into()));
    assert_eq!(qr.rows[1][0], Value::Text("c".into()));
}

// ── Param in IS NULL / IS NOT NULL / unary ─────────────────────────

#[test]
fn param_in_arithmetic_negation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 42)")
        .unwrap();

    let qr = query_result(
        conn.execute_params("SELECT -$1 FROM t WHERE id = 1", &[Value::Integer(10)])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(-10));
}

#[test]
fn param_in_not_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT COUNT(*) FROM items WHERE NOT (active = $1)",
            &[Value::Boolean(true)],
        )
        .unwrap(),
    );
    let count = match qr.rows[0][0] {
        Value::Integer(n) => n,
        _ => panic!(),
    };
    assert!(count > 0);
}

// ── Param with DISTINCT ────────────────────────────────────────────

#[test]
fn param_in_distinct_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT DISTINCT active FROM items WHERE price > $1",
            &[Value::Real(0.0)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
}

// ── Param in join ON + WHERE combined ──────────────────────────────

#[test]
fn param_in_join_on_and_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();
    conn.execute("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 50.0)")
        .unwrap();
    conn.execute("INSERT INTO orders (id, user_id, amount) VALUES (2, 1, 150.0)")
        .unwrap();
    conn.execute("INSERT INTO orders (id, user_id, amount) VALUES (3, 2, 75.0)")
        .unwrap();

    let qr = query_result(conn.execute_params(
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > $1 AND u.id = $2",
        &[Value::Real(100.0), Value::Integer(1)],
    ).unwrap());
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][1], Value::Real(150.0));
}

// ── Edge case values ───────────────────────────────────────────────

#[test]
fn empty_string_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute_params(
        "INSERT INTO t (id, val) VALUES ($1, $2)",
        &[Value::Integer(1), Value::Text("".into())],
    )
    .unwrap();

    let qr = query_result(
        conn.execute_params("SELECT val FROM t WHERE id = $1", &[Value::Integer(1)])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Text("".into()));
}

#[test]
fn large_integer_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, big INTEGER)")
        .unwrap();

    let big = i64::MAX;
    conn.execute_params(
        "INSERT INTO t (id, big) VALUES ($1, $2)",
        &[Value::Integer(1), Value::Integer(big)],
    )
    .unwrap();

    let qr = query_result(
        conn.execute_params("SELECT big FROM t WHERE id = $1", &[Value::Integer(1)])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(big));
}

#[test]
fn negative_real_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = query_result(
        conn.execute_params("SELECT $1 + $2", &[Value::Real(-3.15), Value::Real(3.15)])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Real(0.0));
}

// ── Multiple connections (independent caches) ──────────────────────

#[test]
fn independent_caches_per_connection() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());

    let mut conn1 = Connection::open(&db).unwrap();
    conn1
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn1
        .execute("INSERT INTO t (id, val) VALUES (1, 'hello')")
        .unwrap();

    let sql = "SELECT val FROM t WHERE id = $1";
    let qr = query_result(conn1.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));

    let mut conn2 = Connection::open(&db).unwrap();
    let qr = query_result(conn2.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
}

// ── DDL inside transaction + cache ─────────────────────────────────

#[test]
fn ddl_in_transaction_invalidates_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 'x')")
        .unwrap();

    let sql = "SELECT val FROM t WHERE id = $1";
    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("x".into()));

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE INDEX idx_val ON t (val)").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = query_result(conn.execute_params(sql, &[Value::Integer(1)]).unwrap());
    assert_eq!(qr.rows[0][0], Value::Text("x".into()));
}

// ── Cache hit then evict then re-execute ───────────────────────────

#[test]
fn evicted_query_re_parsed_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 42)")
        .unwrap();

    let target_sql = "SELECT val FROM t WHERE id = $1";
    let qr = query_result(
        conn.execute_params(target_sql, &[Value::Integer(1)])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(42));

    for i in 0..70 {
        conn.execute(&format!("SELECT val + {i} FROM t WHERE id = 1"))
            .unwrap();
    }

    let qr = query_result(
        conn.execute_params(target_sql, &[Value::Integer(1)])
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

// ── Param in BETWEEN (negated) ─────────────────────────────────────

#[test]
fn param_in_not_between() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT COUNT(*) FROM items WHERE id NOT BETWEEN $1 AND $2",
            &[Value::Integer(10), Value::Integer(90)],
        )
        .unwrap(),
    );
    let count = match qr.rows[0][0] {
        Value::Integer(n) => n,
        _ => panic!(),
    };
    assert_eq!(count, 19);
}

// ── Param in NOT LIKE ──────────────────────────────────────────────

#[test]
fn param_in_not_like() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, name) VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO t (id, name) VALUES (2, 'Bob')")
        .unwrap();
    conn.execute("INSERT INTO t (id, name) VALUES (3, 'Carol')")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT name FROM t WHERE name NOT LIKE $1 ORDER BY id",
            &[Value::Text("A%".into())],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Bob".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Carol".into()));
}

// ── Param in LIMIT and OFFSET ──────────────────────────────────────

#[test]
fn param_in_limit_and_offset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT id FROM items ORDER BY id LIMIT $1 OFFSET $2",
            &[Value::Integer(3), Value::Integer(5)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(6));
    assert_eq!(qr.rows[1][0], Value::Integer(7));
    assert_eq!(qr.rows[2][0], Value::Integer(8));
}

// ── Param in EXPLAIN ───────────────────────────────────────────────

#[test]
fn explain_with_multiple_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "EXPLAIN SELECT * FROM t WHERE id = $1 AND val > $2",
            &[Value::Integer(1), Value::Integer(50)],
        )
        .unwrap(),
    );
    let plan: Vec<String> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.to_string(),
            _ => panic!(),
        })
        .collect();
    assert!(plan[0].contains("SEARCH TABLE t"));
}

// ── Rapid alternation between cached queries ───────────────────────

#[test]
fn rapid_cache_alternation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, a, b) VALUES (1, 10, 'hello')")
        .unwrap();

    let sql_a = "SELECT a FROM t WHERE id = $1";
    let sql_b = "SELECT b FROM t WHERE id = $1";

    for _ in 0..50 {
        let qr = query_result(conn.execute_params(sql_a, &[Value::Integer(1)]).unwrap());
        assert_eq!(qr.rows[0][0], Value::Integer(10));

        let qr = query_result(conn.execute_params(sql_b, &[Value::Integer(1)]).unwrap());
        assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
    }
}

// ── Param in nested binary operations ──────────────────────────────

#[test]
fn deeply_nested_param_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT ($1 + $2) * ($3 - $4)",
            &[
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(10),
                Value::Integer(4),
            ],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(30));
}

// ── Param in UPDATE SET + WHERE combined ───────────────────────────

#[test]
fn param_in_update_set_and_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER, label TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, score, label) VALUES (1, 10, 'old')")
        .unwrap();
    conn.execute("INSERT INTO t (id, score, label) VALUES (2, 20, 'old')")
        .unwrap();

    conn.execute_params(
        "UPDATE t SET score = $1, label = $2 WHERE id = $3",
        &[
            Value::Integer(99),
            Value::Text("new".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    let qr = query_result(
        conn.execute("SELECT score, label FROM t WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(99));
    assert_eq!(qr.rows[0][1], Value::Text("new".into()));

    let qr = query_result(
        conn.execute("SELECT score, label FROM t WHERE id = 2")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(20));
    assert_eq!(qr.rows[0][1], Value::Text("old".into()));
}

// ── Param in DELETE with complex WHERE ──────────────────────────────

#[test]
fn param_in_delete_complex_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    conn.execute_params(
        "DELETE FROM items WHERE price > $1 AND active = $2 AND id < $3",
        &[Value::Real(50.0), Value::Boolean(true), Value::Integer(50)],
    )
    .unwrap();

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM items").unwrap());
    let remaining = match qr.rows[0][0] {
        Value::Integer(n) => n,
        _ => panic!(),
    };
    assert!(remaining < 100);
}

// ── Null param in various positions ────────────────────────────────

#[test]
fn null_param_in_coalesce_chain() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT COALESCE($1, $2, $3) FROM t WHERE id = 1",
            &[Value::Null, Value::Null, Value::Text("fallback".into())],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Text("fallback".into()));
}

#[test]
fn null_param_in_is_null_check() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, NULL)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (2, 'x')")
        .unwrap();

    let qr = query_result(
        conn.execute_params(
            "SELECT id FROM t WHERE val IS NULL AND id >= $1",
            &[Value::Integer(1)],
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

// ── Param in aggregate function argument ───────────────────────────

#[test]
fn param_in_case_inside_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_and_populate(&mut conn);

    let qr = query_result(
        conn.execute_params(
            "SELECT SUM(CASE WHEN price > $1 THEN 1 ELSE 0 END) FROM items",
            &[Value::Real(75.0)],
        )
        .unwrap(),
    );
    let count = match qr.rows[0][0] {
        Value::Integer(n) => n,
        _ => panic!(),
    };
    assert_eq!(count, 50);
}
