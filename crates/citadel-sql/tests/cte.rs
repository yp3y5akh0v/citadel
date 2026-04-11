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

fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

// ── 1. Basic CTE ────────────────────────────────────────────────────

#[test]
fn cte_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query("WITH t AS (SELECT 1 AS x) SELECT x FROM t")
        .unwrap();
    assert_eq!(qr.columns, vec!["x"]);
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

// ── 2. CTE over real table ──────────────────────────────────────────

#[test]
fn cte_from_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO employees (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        .unwrap();

    let qr = conn
        .query("WITH t AS (SELECT * FROM employees) SELECT id, name FROM t ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Text("Alice".into())]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(2), Value::Text("Bob".into())]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Integer(3), Value::Text("Carol".into())]
    );
}

// ── 3. WHERE on outer query filters CTE ─────────────────────────────

#[test]
fn cte_with_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query("WITH t AS (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT x FROM t WHERE x > 1 ORDER BY x")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

// ── 4. Column aliases ───────────────────────────────────────────────

#[test]
fn cte_column_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query("WITH t(a, b) AS (SELECT 1, 2) SELECT a, b FROM t")
        .unwrap();
    assert_eq!(qr.columns, vec!["a", "b"]);
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(2)]);
}

// ── 5. Multiple independent CTEs ────────────────────────────────────

#[test]
fn cte_multiple() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query("WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a JOIN b ON 1=1")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(2)]);
}

// ── 6. Chained CTE (later references earlier) ──────────────────────

#[test]
fn cte_chained() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query("WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a) SELECT y FROM b")
        .unwrap();
    assert_eq!(qr.columns, vec!["y"]);
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

// ── 7. CTE shadows real table ───────────────────────────────────────

#[test]
fn cte_shadows_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t (id, val) VALUES (1, 10), (2, 20)")
        .unwrap();

    let qr = conn
        .query("WITH t AS (SELECT 99 AS val) SELECT val FROM t")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(99));
}

// ── 8. CTE as left side of JOIN ─────────────────────────────────────

#[test]
fn cte_with_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO items (id, value) VALUES (1, 'alpha'), (2, 'beta')")
        .unwrap();

    let qr = conn
        .query("WITH t AS (SELECT 1 AS id, 'alice' AS name) SELECT t.name, items.value FROM t JOIN items ON t.id = items.id")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("alice".into()), Value::Text("alpha".into())]
    );
}

// ── 9. CTE as right side of JOIN ────────────────────────────────────

#[test]
fn cte_as_join_rhs() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    let qr = conn
        .query("WITH t AS (SELECT 1 AS id, 100 AS score) SELECT people.name, t.score FROM people JOIN t ON people.id = t.id")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("alice".into()), Value::Integer(100)]
    );
}

// ── 10. CTE body is a UNION ────────────────────────────────────────

#[test]
fn cte_union_body() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query("WITH t AS (SELECT 1 AS x UNION SELECT 2) SELECT x FROM t ORDER BY x")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
}

// ── 11. ORDER BY + LIMIT on outer query over CTE ───────────────────

#[test]
fn cte_order_by_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query(
            "WITH t AS (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) \
             SELECT x FROM t ORDER BY x DESC LIMIT 3",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(5));
    assert_eq!(qr.rows[1][0], Value::Integer(4));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
}

// ── 12. INSERT...SELECT with CTE ───────────────────────────────────

#[test]
fn cte_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );

    let result = conn
        .execute(
            "INSERT INTO dst WITH t AS (SELECT 1 AS id, 'test' AS name) SELECT id, name FROM t",
        )
        .unwrap();
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, 1),
        other => panic!("expected RowsAffected(1), got {other:?}"),
    }

    let qr = conn.query("SELECT id, name FROM dst").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Text("test".into())]
    );
}

// ── 13. Parameters in CTE body ─────────────────────────────────────

#[test]
fn cte_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query_params(
            "WITH t AS (SELECT $1 AS x) SELECT x FROM t",
            &[Value::Integer(42)],
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

// ── 14. Aggregates on CTE ──────────────────────────────────────────

#[test]
fn cte_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO data (id, val) VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let qr = conn
        .query("WITH t AS (SELECT * FROM data) SELECT COUNT(*), SUM(val) FROM t")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[0][1], Value::Integer(60));
}

// ── 15. GROUP BY on CTE ────────────────────────────────────────────

#[test]
fn cte_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT NOT NULL, amount INTEGER NOT NULL)")
            .unwrap(),
    );
    conn.execute(
        "INSERT INTO sales (id, category, amount) VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'B', 40)",
    )
    .unwrap();

    let qr = conn
        .query("WITH t AS (SELECT * FROM sales) SELECT category, SUM(amount) FROM t GROUP BY category ORDER BY category")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("A".into()), Value::Integer(40)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("B".into()), Value::Integer(60)]
    );
}

// ── 16. DISTINCT on CTE ────────────────────────────────────────────

#[test]
fn cte_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query(
            "WITH t AS (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 2 UNION ALL SELECT 3) \
             SELECT DISTINCT x FROM t ORDER BY x",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
}

// ── 17. Basic recursive CTE ────────────────────────────────────────

#[test]
fn recursive_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query(
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10) \
             SELECT x FROM cnt ORDER BY x",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 10);
    for i in 0..10 {
        assert_eq!(qr.rows[i][0], Value::Integer(i as i64 + 1));
    }
}

// ── 18. Recursive tree traversal ───────────────────────────────────

#[test]
fn recursive_tree() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE tree (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT NOT NULL)",
        )
        .unwrap(),
    );
    conn.execute(
        "INSERT INTO tree (id, parent_id, name) VALUES \
         (1, NULL, 'root'), \
         (2, 1, 'child_a'), \
         (3, 1, 'child_b'), \
         (4, 2, 'grandchild_a1'), \
         (5, 3, 'grandchild_b1')",
    )
    .unwrap();

    let qr = conn
        .query(
            "WITH RECURSIVE hier(id, name, lvl) AS (\
               SELECT id, name, 0 FROM tree WHERE parent_id IS NULL \
               UNION ALL \
               SELECT t.id, t.name, h.lvl + 1 FROM tree t JOIN hier h ON t.parent_id = h.id\
             ) SELECT id, name, lvl FROM hier ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 5);
    assert_eq!(
        qr.rows[0],
        vec![
            Value::Integer(1),
            Value::Text("root".into()),
            Value::Integer(0)
        ]
    );
    assert_eq!(
        qr.rows[1],
        vec![
            Value::Integer(2),
            Value::Text("child_a".into()),
            Value::Integer(1)
        ]
    );
    assert_eq!(
        qr.rows[2],
        vec![
            Value::Integer(3),
            Value::Text("child_b".into()),
            Value::Integer(1)
        ]
    );
    assert_eq!(
        qr.rows[3],
        vec![
            Value::Integer(4),
            Value::Text("grandchild_a1".into()),
            Value::Integer(2)
        ]
    );
    assert_eq!(
        qr.rows[4],
        vec![
            Value::Integer(5),
            Value::Text("grandchild_b1".into()),
            Value::Integer(2)
        ]
    );
}

// ── 19. EXPLAIN shows CTE in plan ──────────────────────────────────

#[test]
fn cte_explain() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query("EXPLAIN WITH t AS (SELECT 1 AS x) SELECT x FROM t")
        .unwrap();
    assert_eq!(qr.columns, vec!["plan"]);
    assert!(!qr.rows.is_empty());

    let text: Vec<String> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.to_string(),
            other => panic!("expected Text, got {other:?}"),
        })
        .collect();
    let joined = text.join("\n");
    assert!(
        joined.contains("CTE"),
        "EXPLAIN output should mention CTE, got:\n{joined}"
    );
}

// ── 20. CTE within transaction ─────────────────────────────────────

#[test]
fn cte_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();

    assert_ok(
        conn.execute("CREATE TABLE txn_data (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO txn_data (id, val) VALUES (1, 'hello'), (2, 'world')")
        .unwrap();

    let qr = conn
        .query("WITH t AS (SELECT * FROM txn_data) SELECT id, val FROM t ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Text("hello".into())]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(2), Value::Text("world".into())]
    );

    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query("WITH t AS (SELECT * FROM txn_data) SELECT id, val FROM t ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Text("hello".into())]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(2), Value::Text("world".into())]
    );
}
