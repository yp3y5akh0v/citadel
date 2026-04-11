use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};
use std::collections::HashSet;

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

// ── 1. Large UNION dedup ─────────────────────────────────────────────

#[test]
fn union_large_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );

    // 50% overlap
    conn.execute("BEGIN").unwrap();
    for i in 1..=1000 {
        conn.execute(&format!("INSERT INTO t1 (v) VALUES ({i})"))
            .unwrap();
    }
    for i in 501..=1500 {
        conn.execute(&format!("INSERT INTO t2 (v) VALUES ({i})"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query("SELECT v FROM t1 UNION SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 1500);
}

// ── 2. UNION ALL preserves all ───────────────────────────────────────

#[test]
fn union_all_preserves_all() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    for i in 1..=500 {
        conn.execute(&format!("INSERT INTO t1 (v) VALUES ({i})"))
            .unwrap();
    }
    for i in 501..=1000 {
        conn.execute(&format!("INSERT INTO t2 (v) VALUES ({i})"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query("SELECT v FROM t1 UNION ALL SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 1000);
}

// ── 3. INTERSECT ALL multiset ────────────────────────────────────────

#[test]
fn intersect_all_multiset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );

    // t1: v=10 x3, v=20 x2; t2: v=10 x2, v=20 x3
    conn.execute("INSERT INTO t1 (id, v) VALUES (1, 10), (2, 10), (3, 10), (4, 20), (5, 20)")
        .unwrap();
    conn.execute("INSERT INTO t2 (id, v) VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 20)")
        .unwrap();

    // min(3,2)=2 of v=10, min(2,3)=2 of v=20
    let qr = conn
        .query("SELECT v FROM t1 INTERSECT ALL SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 4);

    let tens = qr
        .rows
        .iter()
        .filter(|r| r[0] == Value::Integer(10))
        .count();
    let twenties = qr
        .rows
        .iter()
        .filter(|r| r[0] == Value::Integer(20))
        .count();
    assert_eq!(tens, 2);
    assert_eq!(twenties, 2);
}

// ── 4. EXCEPT ALL multiset ──────────────────────────────────────────

#[test]
fn except_all_multiset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );

    // t1: v=10 x3, v=20 x2; t2: v=10 x1
    conn.execute("INSERT INTO t1 (id, v) VALUES (1, 10), (2, 10), (3, 10), (4, 20), (5, 20)")
        .unwrap();
    conn.execute("INSERT INTO t2 (id, v) VALUES (1, 10)")
        .unwrap();

    // (3-1)=2 of v=10, (2-0)=2 of v=20
    let qr = conn
        .query("SELECT v FROM t1 EXCEPT ALL SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 4);

    let tens = qr
        .rows
        .iter()
        .filter(|r| r[0] == Value::Integer(10))
        .count();
    let twenties = qr
        .rows
        .iter()
        .filter(|r| r[0] == Value::Integer(20))
        .count();
    assert_eq!(tens, 2);
    assert_eq!(twenties, 2);
}

// ── 5. Five-way chain with precedence ────────────────────────────────

#[test]
fn five_way_chain() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    for t in ["t1", "t2", "t3", "t4", "t5"] {
        assert_ok(
            conn.execute(&format!("CREATE TABLE {t} (v INTEGER PRIMARY KEY)"))
                .unwrap(),
        );
    }
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2), (3)")
        .unwrap();
    conn.execute("INSERT INTO t2 (v) VALUES (2), (3), (4)")
        .unwrap();
    conn.execute("INSERT INTO t3 (v) VALUES (3), (4), (5)")
        .unwrap();
    conn.execute("INSERT INTO t4 (v) VALUES (4), (5), (6)")
        .unwrap();
    conn.execute("INSERT INTO t5 (v) VALUES (5), (6), (7)")
        .unwrap();

    // Parsed as: ((A UNION B) EXCEPT (C INTERSECT D)) UNION E
    // = ({1,2,3,4} EXCEPT {4,5}) UNION {5,6,7} = {1,2,3,5,6,7}
    let qr = conn
        .query(
            "SELECT v FROM t1 UNION SELECT v FROM t2 EXCEPT \
             SELECT v FROM t3 INTERSECT SELECT v FROM t4 UNION SELECT v FROM t5",
        )
        .unwrap();
    let mut vals: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        })
        .collect();
    vals.sort();
    assert_eq!(vals, vec![1, 2, 3, 5, 6, 7]);
}

// ── 6. UNION with mixed types (INTEGER vs REAL) ─────────────────────

#[test]
fn union_mixed_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, v REAL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (id, v) VALUES (1, 42)")
        .unwrap();
    conn.execute("INSERT INTO t2 (id, v) VALUES (1, 42.0)")
        .unwrap();

    // Integer(42) == Real(42.0) for dedup
    let qr = conn
        .query("SELECT v FROM t1 UNION SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
}

// ── 7. UNION with BLOB, TEXT, BOOLEAN, NULL ─────────────────────────

#[test]
fn union_all_types_blob_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, flag BOOLEAN, data BLOB)",
        )
        .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT, flag BOOLEAN, data BLOB)",
        )
        .unwrap(),
    );

    conn.execute("INSERT INTO t1 (id, name, flag) VALUES (1, 'hello', true)")
        .unwrap();
    conn.execute("INSERT INTO t2 (id, name, flag) VALUES (2, 'world', false)")
        .unwrap();

    let qr = conn
        .query("SELECT name, flag FROM t1 UNION ALL SELECT name, flag FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
    assert_eq!(qr.rows[0][1], Value::Boolean(true));
    assert_eq!(qr.rows[1][0], Value::Text("world".into()));
    assert_eq!(qr.rows[1][1], Value::Boolean(false));
}

// ── 8. UNION ORDER BY + OFFSET + LIMIT ──────────────────────────────

#[test]
fn union_order_by_offset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2), (3)")
        .unwrap();
    conn.execute("INSERT INTO t2 (v) VALUES (4), (5), (6)")
        .unwrap();

    let qr = conn
        .query("SELECT v FROM t1 UNION ALL SELECT v FROM t2 ORDER BY v LIMIT 3 OFFSET 2")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[1][0], Value::Integer(4));
    assert_eq!(qr.rows[2][0], Value::Integer(5));
}

// ── 9. UNION in transaction ─────────────────────────────────────────

#[test]
fn union_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE dst (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO t2 (v) VALUES (2), (3)").unwrap();

    conn.execute("BEGIN").unwrap();
    let result = conn
        .execute("INSERT INTO dst SELECT v FROM t1 UNION SELECT v FROM t2")
        .unwrap();
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, 3),
        other => panic!("expected RowsAffected(3), got {other:?}"),
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM dst").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

// ── 10. Self-referential UNION INSERT ────────────────────────────────

#[test]
fn union_self_referential() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t (v) VALUES (1), (2), (3)")
        .unwrap();

    // Snapshot: reads {1,2,3}, inserts {11,12,13} UNION {21,22,23}
    let result = conn
        .execute("INSERT INTO t SELECT v + 10 FROM t UNION SELECT v + 20 FROM t")
        .unwrap();
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, 6),
        other => panic!("expected RowsAffected(6), got {other:?}"),
    }

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(9)); // 3 original + 6 new
}

// ── 11. EXCEPT ALL with no overlap ──────────────────────────────────

#[test]
fn except_all_no_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2), (3)")
        .unwrap();
    conn.execute("INSERT INTO t2 (v) VALUES (4), (5), (6)")
        .unwrap();

    let qr = conn
        .query("SELECT v FROM t1 EXCEPT ALL SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
}

// ── 12. INTERSECT with zero overlap ─────────────────────────────────

#[test]
fn intersect_disjoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (v) VALUES (1), (2), (3)")
        .unwrap();
    conn.execute("INSERT INTO t2 (v) VALUES (4), (5), (6)")
        .unwrap();

    let qr = conn
        .query("SELECT v FROM t1 INTERSECT SELECT v FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);
}

// ── 13. UNION with aggregates ────────────────────────────────────────

#[test]
fn union_with_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t1 (id, v) VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute("INSERT INTO t2 (id, v) VALUES (1, 100), (2, 200)")
        .unwrap();

    let qr = conn
        .query("SELECT SUM(v) FROM t1 UNION ALL SELECT COUNT(*) FROM t2")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);

    let mut vals: HashSet<i64> = HashSet::new();
    for row in &qr.rows {
        match &row[0] {
            Value::Integer(i) => {
                vals.insert(*i);
            }
            other => panic!("expected Integer, got {other:?}"),
        }
    }
    assert!(vals.contains(&60)); // SUM(10+20+30)
    assert!(vals.contains(&2)); // COUNT(*)
}

// ── 14. UNION with JOIN ─────────────────────────────────────────────

#[test]
fn union_with_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap(),
    );

    conn.execute("INSERT INTO a (id, v) VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("INSERT INTO b (id, a_id) VALUES (1, 1), (2, 2)")
        .unwrap();
    conn.execute("INSERT INTO c (id, v) VALUES (1, 30)")
        .unwrap();

    let qr = conn
        .query("SELECT a.v FROM a JOIN b ON a.id = b.a_id UNION SELECT v FROM c")
        .unwrap();
    assert_eq!(qr.rows.len(), 3); // 10, 20, 30
}

// ── 15. EXPLAIN shows compound operations ───────────────────────────

#[test]
fn explain_compound() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t1 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE t2 (v INTEGER PRIMARY KEY)")
            .unwrap(),
    );

    let qr = conn
        .query("EXPLAIN SELECT v FROM t1 UNION SELECT v FROM t2")
        .unwrap();
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
        joined.contains("UNION"),
        "EXPLAIN output should mention UNION, got:\n{joined}"
    );
}
