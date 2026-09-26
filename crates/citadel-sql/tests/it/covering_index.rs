use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn covered_projection_round_trips_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER, tag TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX t_vt ON t (val, tag)").unwrap();
    conn.execute(&format!(
        "INSERT INTO t VALUES (1, {}, 'alpha'), (2, {}, 'beta'), (3, 7, 'gamma')",
        i64::MAX,
        i64::MIN
    ))
    .unwrap();

    let stmt = conn
        .prepare("SELECT val, tag, id FROM t WHERE val = $1")
        .unwrap();
    let qr = stmt.query_collect(&[Value::Integer(i64::MAX)]).unwrap();
    assert_eq!(
        qr.rows,
        vec![vec![
            Value::Integer(i64::MAX),
            Value::Text("alpha".into()),
            Value::Integer(1),
        ]]
    );
    let qr = stmt.query_collect(&[Value::Integer(i64::MIN)]).unwrap();
    assert_eq!(
        qr.rows,
        vec![vec![
            Value::Integer(i64::MIN),
            Value::Text("beta".into()),
            Value::Integer(2),
        ]]
    );
}

#[test]
fn covered_range_with_rotating_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_val ON t (val)").unwrap();
    for id in 1..=20 {
        conn.execute(&format!("INSERT INTO t VALUES ({id}, {})", id * 10))
            .unwrap();
    }

    let stmt = conn
        .prepare("SELECT id FROM t WHERE val >= $1 AND val < $2")
        .unwrap();
    for lo in [30i64, 100, 150] {
        let qr = stmt
            .query_collect(&[Value::Integer(lo), Value::Integer(lo + 30)])
            .unwrap();
        let expect: Vec<i64> = (1..=20)
            .filter(|id| id * 10 >= lo && id * 10 < lo + 30)
            .collect();
        let got: Vec<i64> = qr
            .rows
            .iter()
            .map(|r| match r[0] {
                Value::Integer(v) => v,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(got, expect);
    }
}

#[test]
fn nocase_component_not_served_folded() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("CREATE INDEX t_name ON t (name)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'MixedCase'), (2, 'lower')")
        .unwrap();

    let stmt = conn.prepare("SELECT name FROM t WHERE name = $1").unwrap();
    let qr = stmt
        .query_collect(&[Value::Text("mixedcase".into())])
        .unwrap();
    // Original casing proves the value came from the base row, not folded key bytes.
    assert_eq!(qr.rows, vec![vec![Value::Text("MixedCase".into())]]);
}

#[test]
fn covered_unique_index_pk_from_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, code TEXT)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX t_code ON t (code)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (10, 'a'), (20, 'b')")
        .unwrap();

    let stmt = conn
        .prepare("SELECT id, code FROM t WHERE code = $1")
        .unwrap();
    let qr = stmt.query_collect(&[Value::Text("b".into())]).unwrap();
    assert_eq!(
        qr.rows,
        vec![vec![Value::Integer(20), Value::Text("b".into())]]
    );
}

#[test]
fn covered_null_component() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_val ON t (val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL), (2, 5)")
        .unwrap();

    let stmt = conn
        .prepare("SELECT id, val FROM t WHERE val >= $1")
        .unwrap();
    let qr = stmt.query_collect(&[Value::Integer(0)]).unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(2), Value::Integer(5)]]);
}

#[test]
fn covered_sees_dml_between_executes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_val ON t (val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();

    let stmt = conn.prepare("SELECT id FROM t WHERE val = $1").unwrap();
    assert_eq!(
        stmt.query_collect(&[Value::Integer(5)]).unwrap().rows.len(),
        1
    );
    conn.execute("INSERT INTO t VALUES (2, 5)").unwrap();
    assert_eq!(
        stmt.query_collect(&[Value::Integer(5)]).unwrap().rows.len(),
        2
    );
    conn.execute("UPDATE t SET val = 6 WHERE id = 1").unwrap();
    assert_eq!(
        stmt.query_collect(&[Value::Integer(5)]).unwrap().rows.len(),
        1
    );
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    assert_eq!(
        stmt.query_collect(&[Value::Integer(5)]).unwrap().rows.len(),
        0
    );
}

#[test]
fn covered_drop_index_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_val ON t (val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5), (2, 6)").unwrap();

    let stmt = conn.prepare("SELECT id FROM t WHERE val = $1").unwrap();
    assert_eq!(
        stmt.query_collect(&[Value::Integer(6)]).unwrap().rows.len(),
        1
    );
    conn.execute("DROP INDEX t_val").unwrap();
    assert_eq!(
        stmt.query_collect(&[Value::Integer(6)]).unwrap().rows.len(),
        1
    );
}

#[test]
fn residual_on_non_covered_column_uses_base_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER, extra TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX t_val ON t (val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5, 'keep'), (2, 5, 'drop')")
        .unwrap();

    let stmt = conn
        .prepare("SELECT id FROM t WHERE val = $1 AND extra = 'keep'")
        .unwrap();
    let qr = stmt.query_collect(&[Value::Integer(5)]).unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(1)]]);
}

#[test]
fn covered_composite_pk_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (a INTEGER NOT NULL, b TEXT NOT NULL, val INTEGER, PRIMARY KEY (a, b))",
    )
    .unwrap();
    conn.execute("CREATE INDEX t_val ON t (val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'x', 5), (2, 'y', 5), (3, 'z', 6)")
        .unwrap();

    let stmt = conn.prepare("SELECT a, b FROM t WHERE val = $1").unwrap();
    let qr = stmt.query_collect(&[Value::Integer(5)]).unwrap();
    assert_eq!(
        qr.rows,
        vec![
            vec![Value::Integer(1), Value::Text("x".into())],
            vec![Value::Integer(2), Value::Text("y".into())],
        ]
    );
}

#[test]
fn partial_index_not_served_covered() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER, live INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_val ON t (val) WHERE live = 1")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5, 1), (2, 5, 0)")
        .unwrap();

    let stmt = conn
        .prepare("SELECT id FROM t WHERE val = $1 AND live = 1")
        .unwrap();
    let qr = stmt.query_collect(&[Value::Integer(5)]).unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(1)]]);
}

#[test]
fn covered_count_matches_base_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_val ON t (val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5), (2, 5), (3, 6), (4, NULL)")
        .unwrap();

    let stmt = conn
        .prepare("SELECT COUNT(*) FROM t WHERE val = $1")
        .unwrap();
    let qr = stmt.query_collect(&[Value::Integer(5)]).unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(2)]]);

    let stmt = conn
        .prepare("SELECT COUNT(*) FROM t WHERE val >= $1")
        .unwrap();
    let qr = stmt.query_collect(&[Value::Integer(5)]).unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(3)]]);

    let stmt = conn
        .prepare("SELECT COUNT(*) FROM t WHERE val < $1")
        .unwrap();
    let qr = stmt.query_collect(&[Value::Integer(7)]).unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(3)]]);
}

#[test]
fn covered_count_residual_and_error_parity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, k INTEGER, x INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_k ON t (k)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1, 0), (2, 2, 1)")
        .unwrap();

    let qr = conn
        .query("SELECT COUNT(*) FROM t WHERE k >= 1 AND x = 1")
        .unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(1)]]);

    let err = conn
        .query("SELECT COUNT(*) FROM t WHERE k >= 1 AND 1 / (k - 1) = 0")
        .unwrap_err();
    let _ = err;
}

#[test]
fn covered_count_dup_eq_conjunct_stays_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, k INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_k ON t (k)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1), (2, 2)").unwrap();

    let qr = conn
        .query("SELECT COUNT(*) FROM t WHERE k = 1 AND k = 2")
        .unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(0)]]);

    let qr = conn.query("SELECT COUNT(*) FROM t WHERE k = NULL").unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(0)]]);
}

#[test]
fn explain_marks_covering_scans() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER, extra TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX t_val ON t (val)").unwrap();

    let qr = conn
        .query("EXPLAIN SELECT id FROM t WHERE val = 5")
        .unwrap();
    let text = format!("{:?}", qr.rows);
    assert!(text.contains("COVERING"), "expected marker in {text}");

    let qr = conn
        .query("EXPLAIN SELECT extra FROM t WHERE val = 5")
        .unwrap();
    let text = format!("{:?}", qr.rows);
    assert!(!text.contains("COVERING"), "unexpected marker in {text}");
}
