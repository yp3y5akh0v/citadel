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

fn count_rows(conn: &mut Connection, table: &str) -> i64 {
    let qr = query(conn, &format!("SELECT COUNT(*) FROM {table}"));
    match &qr.rows[0][0] {
        Value::Integer(n) => *n,
        other => panic!("expected integer count, got {other:?}"),
    }
}

fn get_ints(qr: &QueryResult, col: usize) -> Vec<i64> {
    let mut vals: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[col] {
            Value::Integer(i) => *i,
            other => panic!("expected int, got {other:?}"),
        })
        .collect();
    vals.sort();
    vals
}

// ── 1. Self-referencing doubling ──────────────────────────────────────

#[test]
fn self_ref_doubling() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40)")
            .unwrap(),
        4,
    );
    assert_eq!(count_rows(&mut conn, "t"), 4);

    // Round 1: 4 -> 8 (ids 5-8)
    assert_rows_affected(
        conn.execute("INSERT INTO t SELECT id + 4, name, age FROM t")
            .unwrap(),
        4,
    );
    assert_eq!(count_rows(&mut conn, "t"), 8);

    // Round 2: 8 -> 16 (ids 9-16)
    assert_rows_affected(
        conn.execute("INSERT INTO t SELECT id + 8, name, age FROM t")
            .unwrap(),
        8,
    );
    assert_eq!(count_rows(&mut conn, "t"), 16);

    // Round 3: 16 -> 32 (ids 17-32)
    assert_rows_affected(
        conn.execute("INSERT INTO t SELECT id + 16, name, age FROM t")
            .unwrap(),
        16,
    );
    assert_eq!(count_rows(&mut conn, "t"), 32);

    let qr = query(&mut conn, "SELECT id FROM t ORDER BY id");
    let ids = get_ints(&qr, 0);
    assert_eq!(ids, (1..=32).collect::<Vec<i64>>());
}

// ── 2. Large cross-table copy ─────────────────────────────────────────

#[test]
fn large_cross_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER)")
            .unwrap(),
    );

    for i in 1..=1000 {
        conn.execute(&format!(
            "INSERT INTO src VALUES ({i}, 'row_{i}', {})",
            i * 10
        ))
        .unwrap();
    }
    assert_eq!(count_rows(&mut conn, "src"), 1000);

    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT * FROM src").unwrap(),
        1000,
    );
    assert_eq!(count_rows(&mut conn, "dst"), 1000);

    // Spot-check first, last, and middle rows
    let qr = query(&mut conn, "SELECT name, val FROM dst WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("row_1".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(10));

    let qr = query(&mut conn, "SELECT name, val FROM dst WHERE id = 500");
    assert_eq!(qr.rows[0][0], Value::Text("row_500".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(5000));

    let qr = query(&mut conn, "SELECT name, val FROM dst WHERE id = 1000");
    assert_eq!(qr.rows[0][0], Value::Text("row_1000".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(10000));
}

// ── 3. Partial failure rollback with CHECK ────────────────────────────

#[test]
fn partial_failure_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE dst (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL CHECK(val > 0))",
        )
        .unwrap(),
    );

    assert_rows_affected(
        conn.execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, -1), (4, 40)")
            .unwrap(),
        4,
    );

    let err = conn
        .execute("INSERT INTO dst SELECT * FROM src")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    assert_eq!(count_rows(&mut conn, "dst"), 0);
}

// ── 4. Chained copies in a transaction ────────────────────────────────

#[test]
fn chained_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE c (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER)")
            .unwrap(),
    );

    assert_rows_affected(
        conn.execute("INSERT INTO a VALUES (1, 'x', 10), (2, 'y', 20), (3, 'z', 30)")
            .unwrap(),
        3,
    );

    assert_rows_affected(conn.execute("INSERT INTO b SELECT * FROM a").unwrap(), 3);
    assert_rows_affected(conn.execute("INSERT INTO c SELECT * FROM b").unwrap(), 3);
    assert_rows_affected(conn.execute("INSERT INTO d SELECT * FROM c").unwrap(), 3);

    conn.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut conn, "d"), 3);
    let qr = query(&mut conn, "SELECT id, name, val FROM d ORDER BY id");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("x".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(10));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
    assert_eq!(qr.rows[2][1], Value::Text("z".into()));
    assert_eq!(qr.rows[2][2], Value::Integer(30));
}

// ── 5. INSERT SELECT after ALTER TABLE ADD COLUMN ─────────────────────

#[test]
fn insert_select_after_alter_add_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO src VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
            .unwrap(),
        3,
    );

    assert_ok(
        conn.execute("ALTER TABLE src ADD COLUMN age INTEGER DEFAULT 42")
            .unwrap(),
    );

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)")
            .unwrap(),
    );

    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT * FROM src").unwrap(),
        3,
    );

    let qr = query(&mut conn, "SELECT age FROM dst ORDER BY id");
    for row in &qr.rows {
        assert_eq!(row[0], Value::Integer(42));
    }
    assert_eq!(qr.rows.len(), 3);
}

// ── 6. INSERT SELECT with complex expressions ─────────────────────────

#[test]
fn insert_select_with_complex_exprs() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute(
            "INSERT INTO src VALUES (1, 'alice', 25), (2, 'bob', 35), (3, 'carol', NULL), (4, 'dave', 45)",
        )
        .unwrap(),
        4,
    );

    assert_ok(
        conn.execute(
            "CREATE TABLE dst (id INTEGER NOT NULL PRIMARY KEY, label TEXT, score INTEGER)",
        )
        .unwrap(),
    );

    assert_rows_affected(
        conn.execute(
            "INSERT INTO dst SELECT id, \
             CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END, \
             COALESCE(age, 0) * 2 \
             FROM src",
        )
        .unwrap(),
        4,
    );

    let qr = query(&mut conn, "SELECT label, score FROM dst ORDER BY id");

    // alice: age=25 <= 30 -> junior, 25*2=50
    assert_eq!(qr.rows[0][0], Value::Text("junior".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(50));

    // bob: age=35 > 30 -> senior, 35*2=70
    assert_eq!(qr.rows[1][0], Value::Text("senior".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(70));

    // carol: age=NULL -> CASE treats NULL as not > 30 -> junior, COALESCE(NULL,0)*2=0
    assert_eq!(qr.rows[2][0], Value::Text("junior".into()));
    assert_eq!(qr.rows[2][1], Value::Integer(0));

    // dave: age=45 > 30 -> senior, 45*2=90
    assert_eq!(qr.rows[3][0], Value::Text("senior".into()));
    assert_eq!(qr.rows[3][1], Value::Integer(90));
}

// ── 7. INSERT SELECT with ORDER BY and LIMIT ──────────────────────────

#[test]
fn insert_select_order_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute(
            "INSERT INTO src VALUES \
             (1, 'a', 10), (2, 'b', 50), (3, 'c', 30), (4, 'd', 80), (5, 'e', 20), \
             (6, 'f', 90), (7, 'g', 40), (8, 'h', 70), (9, 'i', 60), (10, 'j', 100)",
        )
        .unwrap(),
        10,
    );

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)")
            .unwrap(),
    );

    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT * FROM src ORDER BY age DESC LIMIT 5")
            .unwrap(),
        5,
    );

    assert_eq!(count_rows(&mut conn, "dst"), 5);

    let qr = query(&mut conn, "SELECT age FROM dst ORDER BY age DESC");
    let ages = get_ints(&qr, 0);
    // Top 5 ages descending: 100, 90, 80, 70, 60 -> sorted ascending by get_ints
    assert_eq!(ages, vec![60, 70, 80, 90, 100]);
}

// ── 8. INSERT SELECT with aggregates ──────────────────────────────────

#[test]
fn insert_select_with_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute(
            "INSERT INTO src VALUES \
             (1, 'alice', 10), (2, 'alice', 20), (3, 'bob', 30), \
             (4, 'bob', 40), (5, 'bob', 50), (6, 'carol', 100)",
        )
        .unwrap(),
        6,
    );

    assert_ok(
        conn.execute("CREATE TABLE dst (name TEXT NOT NULL PRIMARY KEY, total INTEGER)")
            .unwrap(),
    );

    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT name, SUM(val) FROM src GROUP BY name")
            .unwrap(),
        3,
    );

    let qr = query(&mut conn, "SELECT name, total FROM dst ORDER BY name");
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(30));
    assert_eq!(qr.rows[1][0], Value::Text("bob".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(120));
    assert_eq!(qr.rows[2][0], Value::Text("carol".into()));
    assert_eq!(qr.rows[2][1], Value::Integer(100));
}

// ── 9. INSERT SELECT from JOIN ────────────────────────────────────────

#[test]
fn insert_select_from_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER, score INTEGER)",
        )
        .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE dst (id INTEGER NOT NULL PRIMARY KEY, name TEXT, score INTEGER)",
        )
        .unwrap(),
    );

    assert_rows_affected(
        conn.execute("INSERT INTO a VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO b VALUES (1, 1, 95), (2, 2, 87), (3, 3, 72), (4, 1, 88)")
            .unwrap(),
        4,
    );

    assert_rows_affected(
        conn.execute(
            "INSERT INTO dst SELECT b.id, a.name, b.score FROM a INNER JOIN b ON a.id = b.a_id",
        )
        .unwrap(),
        4,
    );

    assert_eq!(count_rows(&mut conn, "dst"), 4);

    let qr = query(&mut conn, "SELECT name, score FROM dst ORDER BY score");
    assert_eq!(qr.rows[0][0], Value::Text("carol".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(72));
    assert_eq!(qr.rows[3][0], Value::Text("alice".into()));
    assert_eq!(qr.rows[3][1], Value::Integer(95));
}

// ── 10. Mixed type coercion ───────────────────────────────────────────

#[test]
fn insert_select_mixed_types_coercion() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, int_val INTEGER, bool_val BOOLEAN)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO src VALUES (1, 42, TRUE), (2, 7, FALSE), (3, 100, TRUE)")
            .unwrap(),
        3,
    );

    assert_ok(
        conn.execute(
            "CREATE TABLE dst (id INTEGER NOT NULL PRIMARY KEY, real_val REAL, int_from_bool INTEGER)",
        )
        .unwrap(),
    );

    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT id, int_val, bool_val FROM src")
            .unwrap(),
        3,
    );

    let qr = query(
        &mut conn,
        "SELECT real_val, int_from_bool FROM dst ORDER BY id",
    );

    // int -> real coercion
    assert_eq!(qr.rows[0][0], Value::Real(42.0));
    assert_eq!(qr.rows[1][0], Value::Real(7.0));
    assert_eq!(qr.rows[2][0], Value::Real(100.0));

    // bool -> int coercion: TRUE=1, FALSE=0
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    assert_eq!(qr.rows[1][1], Value::Integer(0));
    assert_eq!(qr.rows[2][1], Value::Integer(1));
}

// ── 11. All constraints enforced ──────────────────────────────────────

#[test]
fn insert_select_all_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Parent table for FK
    assert_ok(
        conn.execute("CREATE TABLE parents (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO parents VALUES (1), (2), (3)")
            .unwrap(),
        3,
    );

    assert_ok(
        conn.execute(
            "CREATE TABLE dst (\
            id INTEGER NOT NULL PRIMARY KEY, \
            name TEXT NOT NULL, \
            val INTEGER NOT NULL CHECK(val >= 0), \
            parent_id INTEGER NOT NULL REFERENCES parents(id))",
        )
        .unwrap(),
    );
    conn.execute("CREATE UNIQUE INDEX idx_dst_name ON dst (name)")
        .unwrap();

    // Valid source
    assert_ok(
        conn.execute(
            "CREATE TABLE good_src (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER, parent_id INTEGER)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO good_src VALUES (1, 'alice', 10, 1), (2, 'bob', 20, 2)")
            .unwrap(),
        2,
    );

    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT * FROM good_src")
            .unwrap(),
        2,
    );
    assert_eq!(count_rows(&mut conn, "dst"), 2);

    // NOT NULL violation
    assert_ok(
        conn.execute(
            "CREATE TABLE null_src (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER, parent_id INTEGER)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO null_src VALUES (10, NULL, 5, 1)")
            .unwrap(),
        1,
    );
    let err = conn
        .execute("INSERT INTO dst SELECT * FROM null_src")
        .unwrap_err();
    assert!(matches!(err, SqlError::NotNullViolation(..)));

    // CHECK violation
    assert_ok(
        conn.execute(
            "CREATE TABLE chk_src (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER, parent_id INTEGER)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO chk_src VALUES (20, 'zara', -5, 1)")
            .unwrap(),
        1,
    );
    let err = conn
        .execute("INSERT INTO dst SELECT * FROM chk_src")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    // UNIQUE violation
    assert_ok(
        conn.execute(
            "CREATE TABLE dup_src (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER, parent_id INTEGER)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO dup_src VALUES (30, 'alice', 99, 3)")
            .unwrap(),
        1,
    );
    let err = conn
        .execute("INSERT INTO dst SELECT * FROM dup_src")
        .unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(..)));

    // FK violation
    assert_ok(
        conn.execute(
            "CREATE TABLE fk_src (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER, parent_id INTEGER)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO fk_src VALUES (40, 'fk_fail', 5, 999)")
            .unwrap(),
        1,
    );
    let err = conn
        .execute("INSERT INTO dst SELECT * FROM fk_src")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));

    // dst should still have only the 2 valid rows
    assert_eq!(count_rows(&mut conn, "dst"), 2);
}

// ── 12. Transaction rollback leaves no trace ──────────────────────────

#[test]
fn insert_select_txn_rollback_no_trace() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );

    for i in 1..=100 {
        conn.execute(&format!("INSERT INTO src VALUES ({i}, {})", i * 10))
            .unwrap();
    }

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT * FROM src").unwrap(),
        100,
    );
    // Visible within transaction
    assert_eq!(count_rows(&mut conn, "dst"), 100);
    conn.execute("ROLLBACK").unwrap();

    assert_eq!(count_rows(&mut conn, "dst"), 0);
}

// ── 13. INSERT SELECT with subquery in WHERE ──────────────────────────

#[test]
fn insert_select_with_subquery_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE filter_table (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER)")
            .unwrap(),
    );

    assert_rows_affected(
        conn.execute(
            "INSERT INTO src VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), \
             (4, 'd', 40), (5, 'e', 50)",
        )
        .unwrap(),
        5,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO filter_table VALUES (2), (4)")
            .unwrap(),
        2,
    );

    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT * FROM src WHERE id IN (SELECT id FROM filter_table)")
            .unwrap(),
        2,
    );

    assert_eq!(count_rows(&mut conn, "dst"), 2);
    let qr = query(&mut conn, "SELECT id FROM dst ORDER BY id");
    let ids = get_ints(&qr, 0);
    assert_eq!(ids, vec![2, 4]);
}

// ── 14. INSERT SELECT DISTINCT ────────────────────────────────────────

#[test]
fn insert_select_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute(
            "INSERT INTO src VALUES (1, 'alice'), (2, 'bob'), (3, 'alice'), \
             (4, 'carol'), (5, 'bob'), (6, 'bob'), (7, 'dave')",
        )
        .unwrap(),
        7,
    );

    assert_ok(
        conn.execute("CREATE TABLE dst (name TEXT NOT NULL PRIMARY KEY)")
            .unwrap(),
    );

    assert_rows_affected(
        conn.execute("INSERT INTO dst SELECT DISTINCT name FROM src")
            .unwrap(),
        4,
    );

    assert_eq!(count_rows(&mut conn, "dst"), 4);
    let qr = query(&mut conn, "SELECT name FROM dst ORDER BY name");
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("bob".into()));
    assert_eq!(qr.rows[2][0], Value::Text("carol".into()));
    assert_eq!(qr.rows[3][0], Value::Text("dave".into()));
}

// ── 15. EXPLAIN INSERT SELECT ─────────────────────────────────────────

#[test]
fn explain_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO src VALUES (1, 'x'), (2, 'y')")
            .unwrap(),
        2,
    );

    let qr = query(&mut conn, "EXPLAIN INSERT INTO dst SELECT * FROM src");

    let plan_text: String = qr
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Text(s) => s.to_string(),
            other => panic!("expected Text, got {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        plan_text.contains("INSERT INTO"),
        "plan should mention INSERT INTO, got:\n{plan_text}"
    );
    assert!(
        plan_text.contains("SELECT") || plan_text.contains("Scan"),
        "plan should mention SELECT or Scan, got:\n{plan_text}"
    );

    // EXPLAIN should not actually insert anything
    assert_eq!(count_rows(&mut conn, "dst"), 0);
}
