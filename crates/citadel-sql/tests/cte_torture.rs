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

// -- 1. Column alias count mismatch --------------------------------------

#[test]
fn error_column_alias_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn
        .query("WITH t(a, b, c) AS (SELECT 1, 2) SELECT * FROM t")
        .unwrap_err();
    assert!(
        err.to_string().contains("column alias count mismatch") || err.to_string().contains("CTE"),
        "expected column alias mismatch error, got: {err}"
    );
}

// -- 2. Duplicate CTE name -----------------------------------------------

#[test]
fn error_duplicate_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn
        .query("WITH t AS (SELECT 1), t AS (SELECT 2) SELECT * FROM t")
        .unwrap_err();
    assert!(
        err.to_string().contains("duplicate CTE") || err.to_string().contains("Duplicate"),
        "expected duplicate CTE error, got: {err}"
    );
}

// -- 3. RECURSIVE without UNION ------------------------------------------

#[test]
fn error_recursive_no_union() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn
        .query("WITH RECURSIVE t(x) AS (SELECT x + 1 FROM t WHERE x < 10) SELECT * FROM t")
        .unwrap_err();
    assert!(
        err.to_string().contains("requires UNION"),
        "expected recursive-requires-UNION error, got: {err}"
    );
}

// -- 4. Recursive CTE exceeds max iterations -----------------------------

#[test]
fn error_recursive_max_iterations() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn
        .query("WITH RECURSIVE t(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM t) SELECT * FROM t")
        .unwrap_err();
    assert!(
        err.to_string().contains("maximum iterations") || err.to_string().contains("exceeded"),
        "expected max iterations error, got: {err}"
    );
}

// -- 5. CTE inside subquery not supported --------------------------------

#[test]
fn error_cte_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn
        .query("SELECT * FROM (WITH t AS (SELECT 1) SELECT * FROM t)")
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported")
            || err.to_string().contains("Unsupported")
            || err.to_string().contains("CTEs in subqueries"),
        "expected CTEs-in-subqueries error, got: {err}"
    );
}

// -- 6. CTE returning empty result set -----------------------------------

#[test]
fn cte_empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE tbl (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    let qr = conn
        .query("WITH t AS (SELECT * FROM tbl WHERE 1 = 0) SELECT * FROM t")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);
}

// -- 7. CTE over large table --------------------------------------------

#[test]
fn cte_large_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE big (val INTEGER PRIMARY KEY)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    for i in 0..5000 {
        conn.execute(&format!("INSERT INTO big (val) VALUES ({i})"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query("WITH t AS (SELECT * FROM big) SELECT COUNT(*) FROM t")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5000));
}

// -- 8. CTE self-join ----------------------------------------------------

#[test]
fn cte_self_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query(
            "WITH t AS (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) \
             SELECT a.x, b.x FROM t a JOIN t b ON a.x <= b.x ORDER BY a.x, b.x",
        )
        .unwrap();

    assert_eq!(qr.rows.len(), 6);

    let pairs: Vec<(i64, i64)> = qr
        .rows
        .iter()
        .map(|r| {
            let a = match &r[0] {
                Value::Integer(i) => *i,
                other => panic!("expected Integer, got {other:?}"),
            };
            let b = match &r[1] {
                Value::Integer(i) => *i,
                other => panic!("expected Integer, got {other:?}"),
            };
            (a, b)
        })
        .collect();

    assert_eq!(pairs, vec![(1, 1), (1, 2), (1, 3), (2, 2), (2, 3), (3, 3)]);
}

// -- 9. Three chained CTEs ----------------------------------------------

#[test]
fn cte_three_chained() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query(
            "WITH a AS (SELECT 1 AS x), \
                  b AS (SELECT x + 10 AS y FROM a), \
                  c AS (SELECT y + 100 AS z FROM b) \
             SELECT z FROM c",
        )
        .unwrap();

    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(111));
}

// -- 10. Unused CTE does not error ---------------------------------------

#[test]
fn cte_unused() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query("WITH t AS (SELECT 1 AS x) SELECT 42 AS val")
        .unwrap();

    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

// -- 11. Recursive fibonacci ---------------------------------------------

#[test]
fn recursive_fibonacci() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query(
            "WITH RECURSIVE fib(n, a, b) AS ( \
                 SELECT 0, 0, 1 \
                 UNION ALL \
                 SELECT n + 1, b, a + b FROM fib WHERE n < 10 \
             ) SELECT n, a FROM fib ORDER BY n",
        )
        .unwrap();

    let expected: Vec<(i64, i64)> = vec![
        (0, 0),
        (1, 1),
        (2, 1),
        (3, 2),
        (4, 3),
        (5, 5),
        (6, 8),
        (7, 13),
        (8, 21),
        (9, 34),
        (10, 55),
    ];

    assert_eq!(qr.rows.len(), expected.len());
    for (row, (exp_n, exp_a)) in qr.rows.iter().zip(expected.iter()) {
        assert_eq!(row[0], Value::Integer(*exp_n));
        assert_eq!(row[1], Value::Integer(*exp_a));
    }
}

// -- 12. CTE with NULLs -------------------------------------------------

#[test]
fn cte_with_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query(
            "WITH t AS (SELECT NULL AS x UNION ALL SELECT 1 UNION ALL SELECT NULL) \
             SELECT x FROM t ORDER BY x",
        )
        .unwrap();

    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Null);
    assert_eq!(qr.rows[1][0], Value::Null);
    assert_eq!(qr.rows[2][0], Value::Integer(1));
}

// -- 13. CTE with mixed types -------------------------------------------

#[test]
fn cte_mixed_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let qr = conn
        .query("WITH t AS (SELECT 1 AS x UNION ALL SELECT 'hello') SELECT x FROM t")
        .unwrap();

    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Text("hello".into()));
}

// -- 14. INSERT from joined CTEs ----------------------------------------

#[test]
fn cte_insert_multiple() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap(),
    );

    let result = conn
        .execute(
            "INSERT INTO dst \
             WITH a AS (SELECT 1 AS id), b AS (SELECT 'test' AS name) \
             SELECT a.id, b.name FROM a JOIN b ON 1=1",
        )
        .unwrap();
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, 1),
        other => panic!("expected RowsAffected(1), got {other:?}"),
    }

    let qr = conn.query("SELECT id, name FROM dst").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("test".into()));
}

// -- 15. Recursive CTE inside a transaction ------------------------------

#[test]
fn recursive_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();

    assert_ok(
        conn.execute("CREATE TABLE tree (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)")
            .unwrap(),
    );

    conn.execute("INSERT INTO tree (id, parent_id, name) VALUES (1, NULL, 'root')")
        .unwrap();
    conn.execute("INSERT INTO tree (id, parent_id, name) VALUES (2, 1, 'child1')")
        .unwrap();
    conn.execute("INSERT INTO tree (id, parent_id, name) VALUES (3, 1, 'child2')")
        .unwrap();
    conn.execute("INSERT INTO tree (id, parent_id, name) VALUES (4, 2, 'grandchild1')")
        .unwrap();
    conn.execute("INSERT INTO tree (id, parent_id, name) VALUES (5, 3, 'grandchild2')")
        .unwrap();

    let qr = conn
        .query(
            "WITH RECURSIVE descendants(id, name, depth) AS ( \
                 SELECT id, name, 0 FROM tree WHERE parent_id IS NULL \
                 UNION ALL \
                 SELECT t.id, t.name, d.depth + 1 \
                 FROM tree t JOIN descendants d ON t.parent_id = d.id \
             ) SELECT id, name, depth FROM descendants ORDER BY id",
        )
        .unwrap();

    conn.execute("COMMIT").unwrap();

    assert_eq!(qr.rows.len(), 5);

    let expected: Vec<(i64, &str, i64)> = vec![
        (1, "root", 0),
        (2, "child1", 1),
        (3, "child2", 1),
        (4, "grandchild1", 2),
        (5, "grandchild2", 2),
    ];

    for (row, (exp_id, exp_name, exp_depth)) in qr.rows.iter().zip(expected.iter()) {
        assert_eq!(row[0], Value::Integer(*exp_id));
        assert_eq!(row[1], Value::Text((*exp_name).into()));
        assert_eq!(row[2], Value::Integer(*exp_depth));
    }

    // Verify results persist after COMMIT
    let qr2 = conn.query("SELECT COUNT(*) FROM tree").unwrap();
    assert_eq!(qr2.rows[0][0], Value::Integer(5));
}
