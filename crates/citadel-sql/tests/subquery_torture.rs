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

fn setup_tables(conn: &mut Connection) {
    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
            .unwrap(),
        5,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t2 (id, val) VALUES (2, 200), (4, 400), (6, 600)")
            .unwrap(),
        3,
    );
}

fn get_ids(qr: &QueryResult) -> Vec<i64> {
    let mut ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected int, got {other:?}"),
        })
        .collect();
    ids.sort();
    ids
}

// ── IN subquery edge cases ───────────────────────────────────────

#[test]
fn in_subquery_all_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO a (id) VALUES (1), (2), (3)")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO b (id) VALUES (1), (2), (3)")
            .unwrap(),
        3,
    );

    let qr = query(&mut conn, "SELECT id FROM a WHERE id IN (SELECT id FROM b)");
    assert_eq!(get_ids(&qr), vec![1, 2, 3]);
}

#[test]
fn in_subquery_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO a (id) VALUES (1), (2), (3)")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO b (id) VALUES (4), (5), (6)")
            .unwrap(),
        3,
    );

    let qr = query(&mut conn, "SELECT id FROM a WHERE id IN (SELECT id FROM b)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn in_subquery_both_tables_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );

    let qr = query(&mut conn, "SELECT id FROM a WHERE id IN (SELECT id FROM b)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn in_subquery_outer_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO b (id) VALUES (1)").unwrap(), 1);

    let qr = query(&mut conn, "SELECT id FROM a WHERE id IN (SELECT id FROM b)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn in_subquery_inner_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO a (id) VALUES (1), (2)").unwrap(),
        2,
    );

    let qr = query(&mut conn, "SELECT id FROM a WHERE id IN (SELECT id FROM b)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn in_subquery_duplicate_values_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO a (id) VALUES (1), (2), (3)")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO b (id, ref_id) VALUES (1, 1), (2, 1), (3, 2), (4, 2)")
            .unwrap(),
        4,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM a WHERE id IN (SELECT ref_id FROM b)",
    );
    assert_eq!(get_ids(&qr), vec![1, 2]);
}

#[test]
fn in_subquery_with_text_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE allowed (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO allowed (id, name) VALUES (1, 'Alice'), (2, 'Charlie')")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE name IN (SELECT name FROM allowed)",
    );
    assert_eq!(get_ids(&qr), vec![1, 3]);
}

// ── NULL three-valued logic exhaustive ───────────────────────────

#[test]
fn three_valued_in_true() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
            .unwrap(),
        1,
    );

    let qr = query(&mut conn, "SELECT val FROM t WHERE val IN (10, 20)");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(10));
}

#[test]
fn three_valued_in_false() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
            .unwrap(),
        1,
    );

    let qr = query(&mut conn, "SELECT val FROM t WHERE val IN (20, 30)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn three_valued_in_null_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
            .unwrap(),
        1,
    );

    let qr = query(&mut conn, "SELECT val FROM t WHERE val IN (20, NULL)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn three_valued_not_in_true() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
            .unwrap(),
        1,
    );

    let qr = query(&mut conn, "SELECT val FROM t WHERE val NOT IN (20, 30)");
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn three_valued_not_in_false() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
            .unwrap(),
        1,
    );

    let qr = query(&mut conn, "SELECT val FROM t WHERE val NOT IN (10, 20)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn three_valued_not_in_null_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
            .unwrap(),
        1,
    );

    let qr = query(&mut conn, "SELECT val FROM t WHERE val NOT IN (20, NULL)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn null_lhs_in_list_always_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, NULL)")
            .unwrap(),
        1,
    );

    let qr = query(&mut conn, "SELECT id FROM t WHERE val IN (1, 2, 3)");
    assert_eq!(qr.rows.len(), 0);

    let qr = query(&mut conn, "SELECT id FROM t WHERE val NOT IN (1, 2, 3)");
    assert_eq!(qr.rows.len(), 0);
}

// ── EXISTS edge cases ────────────────────────────────────────────

#[test]
fn exists_with_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2)",
    );
    assert_eq!(qr.rows.len(), 5);
}

#[test]
fn not_exists_with_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2)",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn exists_short_circuit_multi_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE EXISTS (SELECT val FROM t2 WHERE val > 100)",
    );
    assert_eq!(qr.rows.len(), 5);
}

// ── Scalar subquery edge cases ───────────────────────────────────

#[test]
fn scalar_subquery_with_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE val < (SELECT AVG(val) FROM t1)",
    );
    assert_eq!(get_ids(&qr), vec![1, 2]);
}

#[test]
fn scalar_subquery_null_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE s (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
            .unwrap(),
        1,
    );

    let qr = query(&mut conn, "SELECT id FROM t WHERE val = (SELECT id FROM s)");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn scalar_subquery_in_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE val > (SELECT MIN(val) FROM t1) + 5",
    );
    assert_eq!(get_ids(&qr), vec![2, 3, 4, 5]);
}

// ── IN subquery combined with other clauses ──────────────────────

#[test]
fn in_subquery_with_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT DISTINCT val FROM t1 WHERE id IN (SELECT id FROM t2)",
    );
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn in_subquery_with_order_by_desc() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2) ORDER BY id DESC",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(4));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
}

#[test]
fn in_subquery_with_offset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2) ORDER BY id LIMIT 2 OFFSET 1",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[1][0], Value::Integer(5));
}

#[test]
fn in_subquery_combined_with_and() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2) AND val > 25",
    );
    assert_eq!(get_ids(&qr), vec![4]);
}

#[test]
fn in_subquery_combined_with_or() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2) OR val = 10",
    );
    assert_eq!(get_ids(&qr), vec![1, 2, 4]);
}

// ── DELETE / UPDATE with subqueries ──────────────────────────────

#[test]
fn delete_with_not_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    assert_rows_affected(
        conn.execute("DELETE FROM t1 WHERE id NOT IN (SELECT id FROM t2)")
            .unwrap(),
        3,
    );

    let qr = query(&mut conn, "SELECT id FROM t1 ORDER BY id");
    assert_eq!(get_ids(&qr), vec![2, 4]);
}

#[test]
fn delete_with_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    assert_rows_affected(
        conn.execute("DELETE FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = 999)")
            .unwrap(),
        0,
    );

    let qr = query(&mut conn, "SELECT COUNT(*) FROM t1");
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn update_set_with_scalar_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    assert_rows_affected(
        conn.execute("UPDATE t1 SET val = (SELECT MIN(val) FROM t2)")
            .unwrap(),
        5,
    );

    let qr = query(&mut conn, "SELECT DISTINCT val FROM t1");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(200));
}

#[test]
fn update_where_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    assert_rows_affected(
        conn.execute("UPDATE t1 SET val = 0 WHERE EXISTS (SELECT 1 FROM t2)")
            .unwrap(),
        5,
    );

    let qr = query(&mut conn, "SELECT DISTINCT val FROM t1");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

#[test]
fn update_where_not_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    assert_rows_affected(
        conn.execute("UPDATE t1 SET val = 0 WHERE NOT EXISTS (SELECT 1 FROM t2)")
            .unwrap(),
        0,
    );
}

// ── IN list edge cases ───────────────────────────────────────────

#[test]
fn in_list_single_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(&mut conn, "SELECT id FROM t1 WHERE id IN (3)");
    assert_eq!(get_ids(&qr), vec![3]);
}

#[test]
fn in_list_with_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(&mut conn, "SELECT id FROM t1 WHERE id IN (1 + 1, 2 + 2)");
    assert_eq!(get_ids(&qr), vec![2, 4]);
}

#[test]
fn not_in_list_with_null_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(&mut conn, "SELECT id FROM t1 WHERE id NOT IN (NULL)");
    assert_eq!(qr.rows.len(), 0);
}

// ── Transaction context ──────────────────────────────────────────

#[test]
fn in_subquery_sees_uncommitted_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO t2 (id, val) VALUES (1, 100)")
            .unwrap(),
        1,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)",
    );
    assert_eq!(get_ids(&qr), vec![1, 2, 4]);

    conn.execute("ROLLBACK").unwrap();

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)",
    );
    assert_eq!(get_ids(&qr), vec![2, 4]);
}

#[test]
fn delete_in_subquery_within_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("DELETE FROM t1 WHERE id IN (SELECT id FROM t2)")
            .unwrap(),
        2,
    );

    let qr = query(&mut conn, "SELECT id FROM t1 ORDER BY id");
    assert_eq!(get_ids(&qr), vec![1, 3, 5]);

    conn.execute("ROLLBACK").unwrap();

    let qr = query(&mut conn, "SELECT id FROM t1 ORDER BY id");
    assert_eq!(get_ids(&qr), vec![1, 2, 3, 4, 5]);
}

#[test]
fn update_in_subquery_within_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("UPDATE t1 SET val = 999 WHERE id IN (SELECT id FROM t2)")
            .unwrap(),
        2,
    );

    let qr = query(&mut conn, "SELECT val FROM t1 WHERE id = 2");
    assert_eq!(qr.rows[0][0], Value::Integer(999));

    conn.execute("COMMIT").unwrap();

    let qr = query(&mut conn, "SELECT val FROM t1 WHERE id = 2");
    assert_eq!(qr.rows[0][0], Value::Integer(999));
}

// ── Scale ────────────────────────────────────────────────────────

#[test]
fn in_subquery_scale() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE big (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE filter (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    for i in 0..200 {
        conn.execute(&format!(
            "INSERT INTO big (id, val) VALUES ({i}, {val})",
            val = i * 10
        ))
        .unwrap();
    }
    for i in (0..200).step_by(3) {
        conn.execute(&format!("INSERT INTO filter (id) VALUES ({i})"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = query(
        &mut conn,
        "SELECT COUNT(*) FROM big WHERE id IN (SELECT id FROM filter)",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(67));
}

// ── Mixed subquery types ─────────────────────────────────────────

#[test]
fn in_and_exists_combined() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(&mut conn,
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2) AND EXISTS (SELECT 1 FROM t2 WHERE val > 300)"
    );
    assert_eq!(get_ids(&qr), vec![2, 4]);
}

#[test]
fn scalar_subquery_and_in_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE val > (SELECT MIN(val) FROM t1) * 2 OR id IN (1, 5)",
    );
    assert_eq!(get_ids(&qr), vec![1, 3, 4, 5]);
}

#[test]
fn not_in_and_not_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(&mut conn,
        "SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2) AND NOT EXISTS (SELECT 1 FROM t2 WHERE val > 9999)"
    );
    assert_eq!(get_ids(&qr), vec![1, 3, 5]);
}

// ── Same table subquery ──────────────────────────────────────────

#[test]
fn in_subquery_same_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE val IN (SELECT val FROM t1 WHERE id <= 2)",
    );
    assert_eq!(get_ids(&qr), vec![1, 2]);
}

#[test]
fn scalar_subquery_same_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_tables(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t1 WHERE val = (SELECT MAX(val) FROM t1)",
    );
    assert_eq!(get_ids(&qr), vec![5]);
}

// ── Nested subqueries ────────────────────────────────────────────

#[test]
fn nested_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE c (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO a (id) VALUES (1), (2), (3), (4), (5)")
            .unwrap(),
        5,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO b (id) VALUES (1), (2), (3)")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO c (id) VALUES (1), (2)").unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM a WHERE id IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))",
    );
    assert_eq!(get_ids(&qr), vec![1, 2]);
}

// ── Boolean context ──────────────────────────────────────────────

#[test]
fn in_with_boolean_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE flags (id INTEGER NOT NULL PRIMARY KEY, flag BOOLEAN NOT NULL)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, active) VALUES (1, TRUE), (2, FALSE), (3, TRUE)")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO flags (id, flag) VALUES (1, TRUE)")
            .unwrap(),
        1,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE active IN (SELECT flag FROM flags)",
    );
    assert_eq!(get_ids(&qr), vec![1, 3]);
}

// ── HAVING with subquery ─────────────────────────────────────────

#[test]
fn having_with_scalar_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE sales (id INTEGER NOT NULL PRIMARY KEY, region TEXT NOT NULL, amount INTEGER NOT NULL)"
    ).unwrap());
    assert_ok(
        conn.execute(
            "CREATE TABLE thresholds (id INTEGER NOT NULL PRIMARY KEY, min_total INTEGER NOT NULL)",
        )
        .unwrap(),
    );
    assert_rows_affected(conn.execute(
        "INSERT INTO sales (id, region, amount) VALUES (1, 'east', 100), (2, 'east', 200), (3, 'west', 50), (4, 'west', 25)"
    ).unwrap(), 4);
    assert_rows_affected(
        conn.execute("INSERT INTO thresholds (id, min_total) VALUES (1, 100)")
            .unwrap(),
        1,
    );

    let qr = query(&mut conn,
        "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > (SELECT min_total FROM thresholds WHERE id = 1)"
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("east".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(300));
}

// ── Real-type values ─────────────────────────────────────────────

#[test]
fn in_subquery_with_real_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE prices (id INTEGER NOT NULL PRIMARY KEY, price REAL NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE discounts (id INTEGER NOT NULL PRIMARY KEY, price REAL NOT NULL)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO prices (id, price) VALUES (1, 9.99), (2, 19.99), (3, 29.99)")
            .unwrap(),
        3,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO discounts (id, price) VALUES (1, 9.99), (2, 29.99)")
            .unwrap(),
        2,
    );

    let qr = query(
        &mut conn,
        "SELECT id FROM prices WHERE price IN (SELECT price FROM discounts)",
    );
    assert_eq!(get_ids(&qr), vec![1, 3]);
}
