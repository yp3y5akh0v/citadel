use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, QueryResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"pw")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn query(c: &Connection, sql: &str) -> QueryResult {
    c.query(sql).unwrap()
}

#[test]
fn stored_1000_row_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 2 + 1) STORED)",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..1000i64 {
        conn.execute(&format!("INSERT INTO t (id, a) VALUES ({i}, {i})"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let qr = query(&conn, "SELECT COUNT(*), SUM(d) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1000));
    let expected_sum: i64 = (0..1000i64).map(|i| i * 2 + 1).sum();
    assert_eq!(qr.rows[0][1], Value::Integer(expected_sum));
}

#[test]
fn deep_nested_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
         d INTEGER GENERATED ALWAYS AS ((a + b) * (a - b) + ((a * 2) - (b / 2))) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a, b) VALUES (1, 10, 4)")
        .unwrap();
    let qr = query(&conn, "SELECT d FROM t");
    let expected = (10 + 4) * (10 - 4) + (10 * 2 - 4 / 2);
    assert_eq!(qr.rows[0][0], Value::Integer(expected));
}

#[test]
fn virtual_in_transaction_with_updates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, \
         d INTEGER GENERATED ALWAYS AS (x + 100) VIRTUAL)",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, x) VALUES (1, 5)").unwrap();
    let q1 = query(&conn, "SELECT d FROM t");
    assert_eq!(q1.rows[0][0], Value::Integer(105));
    conn.execute("UPDATE t SET x = 10 WHERE id = 1").unwrap();
    let q2 = query(&conn, "SELECT d FROM t");
    assert_eq!(q2.rows[0][0], Value::Integer(110));
    conn.execute("COMMIT").unwrap();
    let q3 = query(&conn, "SELECT d FROM t");
    assert_eq!(q3.rows[0][0], Value::Integer(110));
}

#[test]
fn alter_add_virtual_then_select_mixed_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN d INTEGER GENERATED ALWAYS AS (x * 5) VIRTUAL")
        .unwrap();
    conn.execute("INSERT INTO t (id, x) VALUES (3, 30)")
        .unwrap();
    let qr = query(&conn, "SELECT id, d FROM t ORDER BY id");
    assert_eq!(qr.rows[0][1], Value::Integer(50));
    assert_eq!(qr.rows[1][1], Value::Integer(100));
    assert_eq!(qr.rows[2][1], Value::Integer(150));
}

#[test]
fn savepoint_rollback_after_insert_with_generated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 3) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 2)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (2, 7)").unwrap();
    let qr = query(&conn, "SELECT d FROM t WHERE id = 2");
    assert_eq!(qr.rows[0][0], Value::Integer(21));
    conn.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
    conn.execute("COMMIT").unwrap();
    let after = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(after.rows[0][0], Value::Integer(1));
}

#[test]
fn three_base_columns_stored() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, \
         total INTEGER GENERATED ALWAYS AS (a + b + c) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a, b, c) VALUES (1, 5, 10, 15)")
        .unwrap();
    let qr = query(&conn, "SELECT total FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(30));
    conn.execute("UPDATE t SET b = 100 WHERE id = 1").unwrap();
    let qr2 = query(&conn, "SELECT total FROM t");
    assert_eq!(qr2.rows[0][0], Value::Integer(120));
}

#[test]
fn generated_real_type() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x REAL, \
         doubled REAL GENERATED ALWAYS AS (x * 2.0) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, x) VALUES (1, 1.5)")
        .unwrap();
    let qr = query(&conn, "SELECT doubled FROM t");
    assert_eq!(qr.rows[0][0], Value::Real(3.0));
}

#[test]
fn generated_text_concat_long() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, prefix TEXT, suffix TEXT, \
         full TEXT GENERATED ALWAYS AS (prefix || ':' || suffix || ':' || prefix) STORED)",
    )
    .unwrap();
    let big = "x".repeat(500);
    conn.execute(&format!(
        "INSERT INTO t (id, prefix, suffix) VALUES (1, '{}', 'tag')",
        big
    ))
    .unwrap();
    let qr = query(&conn, "SELECT full FROM t");
    if let Value::Text(s) = &qr.rows[0][0] {
        assert!(s.contains(":tag:"));
        assert_eq!(s.matches('x').count(), 1000);
    } else {
        panic!("expected text");
    }
}

#[test]
fn generated_with_cast() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, \
         s TEXT GENERATED ALWAYS AS (CAST(n AS TEXT)) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, n) VALUES (1, 42)")
        .unwrap();
    let qr = query(&conn, "SELECT s FROM t");
    assert_eq!(qr.rows[0][0], Value::Text("42".into()));
}

#[test]
fn generated_coalesce() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
         pick INTEGER GENERATED ALWAYS AS (COALESCE(a, b, -1)) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a, b) VALUES (1, 10, 20), (2, NULL, 5), (3, NULL, NULL)")
        .unwrap();
    let qr = query(&conn, "SELECT id, pick FROM t ORDER BY id");
    assert_eq!(qr.rows[0][1], Value::Integer(10));
    assert_eq!(qr.rows[1][1], Value::Integer(5));
    assert_eq!(qr.rows[2][1], Value::Integer(-1));
}

#[test]
fn prepared_select_through_generated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 7) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 3), (2, 5)")
        .unwrap();
    let stmt = conn.prepare("SELECT d FROM t WHERE id = $1").unwrap();
    let qr = stmt.query_collect(&[Value::Integer(1)]).unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(21));
    let qr2 = stmt.query_collect(&[Value::Integer(2)]).unwrap();
    assert_eq!(qr2.rows[0][0], Value::Integer(35));
}

#[test]
fn upsert_with_stored_generated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, \
         d INTEGER GENERATED ALWAYS AS (n * 10) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, n) VALUES (1, 5)").unwrap();
    conn.execute(
        "INSERT INTO t (id, n) VALUES (1, 9) ON CONFLICT (id) DO UPDATE SET n = excluded.n",
    )
    .unwrap();
    let qr = query(&conn, "SELECT d FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(90));
}

#[test]
fn delete_with_stored_generated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, \
         d INTEGER GENERATED ALWAYS AS (n * 2) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, n) VALUES (1, 5), (2, 10), (3, 15)")
        .unwrap();
    let qr = query(&conn, "DELETE FROM t WHERE d > 12 RETURNING id, d");
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn many_virtual_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, \
         v1 INTEGER GENERATED ALWAYS AS (x + 1) VIRTUAL, \
         v2 INTEGER GENERATED ALWAYS AS (x + 2) VIRTUAL, \
         v3 INTEGER GENERATED ALWAYS AS (x + 3) VIRTUAL, \
         v4 INTEGER GENERATED ALWAYS AS (x + 4) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, x) VALUES (1, 100)")
        .unwrap();
    let qr = query(&conn, "SELECT v1, v2, v3, v4 FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(101));
    assert_eq!(qr.rows[0][1], Value::Integer(102));
    assert_eq!(qr.rows[0][2], Value::Integer(103));
    assert_eq!(qr.rows[0][3], Value::Integer(104));
}

#[test]
fn mixed_stored_and_virtual() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
         s INTEGER GENERATED ALWAYS AS (a + b) STORED, \
         v INTEGER GENERATED ALWAYS AS (a * b) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a, b) VALUES (1, 4, 5)")
        .unwrap();
    let qr = query(&conn, "SELECT s, v FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(9));
    assert_eq!(qr.rows[0][1], Value::Integer(20));
    conn.execute("UPDATE t SET a = 10 WHERE id = 1").unwrap();
    let qr2 = query(&conn, "SELECT s, v FROM t");
    assert_eq!(qr2.rows[0][0], Value::Integer(15));
    assert_eq!(qr2.rows[0][1], Value::Integer(50));
}
