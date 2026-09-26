//! Regression tests for temporal grouping and comparison.
//!
//! Covers: GROUP BY on an expression key via output ordinal and via output
//! alias; WHERE/BETWEEN comparing a TIMESTAMP column against string literals;
//! and per-row DATE_TRUNC. String literals INSERTed into a TIMESTAMP column are
//! coerced to typed Timestamp values, so grouping and comparison must treat a
//! TIMESTAMP consistently against computed timestamps and string-literal bounds.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"ts-groupby")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn setup(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT, ts TIMESTAMP)")
        .unwrap();
    // 5 rows in Feb 2024, 3 rows in Mar 2024.
    let rows = [
        (1, "a", "2024-02-03 14:02:11"),
        (2, "b", "2024-02-10 09:00:00"),
        (3, "a", "2024-02-15 18:30:00"),
        (4, "c", "2024-02-20 00:00:00"),
        (5, "b", "2024-02-28 23:59:59"),
        (6, "a", "2024-03-01 00:00:01"),
        (7, "c", "2024-03-15 12:00:00"),
        (8, "b", "2024-03-31 23:59:59"),
    ];
    for (id, kind, ts) in rows {
        conn.execute(&format!(
            "INSERT INTO events (id, kind, ts) VALUES ({id}, '{kind}', '{ts}')"
        ))
        .unwrap();
    }
}

fn feb() -> Value {
    Value::Timestamp(1_706_745_600_000_000) // 2024-02-01 00:00:00 UTC
}
fn mar() -> Value {
    Value::Timestamp(1_709_251_200_000_000) // 2024-03-01 00:00:00 UTC
}

#[test]
fn string_literal_stored_as_typed_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let qr = conn.query("SELECT ts FROM events WHERE id = 1").unwrap();
    assert!(
        matches!(qr.rows[0][0], Value::Timestamp(_)),
        "string literal INSERTed into TIMESTAMP column must store a typed Timestamp, got {:?}",
        qr.rows[0][0]
    );
}

#[test]
fn per_row_date_trunc_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let qr = conn
        .query("SELECT id, DATE_TRUNC('month', ts) FROM events ORDER BY id")
        .unwrap();
    // Rows 1-5 truncate to Feb, rows 6-8 to Mar.
    for r in &qr.rows[..5] {
        assert_eq!(r[1], feb());
    }
    for r in &qr.rows[5..] {
        assert_eq!(r[1], mar());
    }
}

#[test]
fn group_by_expr_ordinal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let qr = conn
        .query("SELECT DATE_TRUNC('month', ts) AS m, COUNT(*) FROM events GROUP BY 1 ORDER BY 1")
        .unwrap();
    assert_eq!(
        qr.rows.len(),
        2,
        "expected two month groups, got {:?}",
        qr.rows
    );
    assert_eq!(qr.rows[0][0], feb());
    assert_eq!(qr.rows[0][1], Value::Integer(5));
    assert_eq!(qr.rows[1][0], mar());
    assert_eq!(qr.rows[1][1], Value::Integer(3));
}

#[test]
fn group_by_expr_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let qr = conn
        .query("SELECT DATE_TRUNC('month', ts) AS m, COUNT(*) FROM events GROUP BY m ORDER BY m")
        .unwrap();
    assert_eq!(
        qr.rows.len(),
        2,
        "expected two month groups, got {:?}",
        qr.rows
    );
    assert_eq!(qr.rows[0][0], feb());
    assert_eq!(qr.rows[0][1], Value::Integer(5));
    assert_eq!(qr.rows[1][0], mar());
    assert_eq!(qr.rows[1][1], Value::Integer(3));
}

#[test]
fn group_by_text_column_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let qr = conn
        .query("SELECT kind, COUNT(*) FROM events GROUP BY kind ORDER BY kind")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0], vec![Value::Text("a".into()), Value::Integer(3)]);
    assert_eq!(qr.rows[1], vec![Value::Text("b".into()), Value::Integer(3)]);
    assert_eq!(qr.rows[2], vec![Value::Text("c".into()), Value::Integer(2)]);
}

#[test]
fn where_timestamp_ge_string_literal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let qr = conn
        .query("SELECT id FROM events WHERE ts >= '2024-03-01' ORDER BY id")
        .unwrap();
    let ids: Vec<Value> = qr.rows.iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        ids,
        vec![Value::Integer(6), Value::Integer(7), Value::Integer(8)]
    );
}

#[test]
fn where_timestamp_between_string_literals() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let qr = conn
        .query(
            "SELECT id FROM events \
             WHERE ts BETWEEN '2024-03-01' AND '2024-03-31 23:59:59' ORDER BY id",
        )
        .unwrap();
    let ids: Vec<Value> = qr.rows.iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        ids,
        vec![Value::Integer(6), Value::Integer(7), Value::Integer(8)]
    );
}

#[test]
fn where_timestamp_between_with_index() {
    // Same BETWEEN, but with an index on the temporal column to exercise the
    // index-scan fast path's literal coercion.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute("CREATE INDEX idx_ts ON events (ts)").unwrap();
    let qr = conn
        .query(
            "SELECT id FROM events \
             WHERE ts BETWEEN '2024-03-01' AND '2024-03-31 23:59:59' ORDER BY id",
        )
        .unwrap();
    let ids: Vec<Value> = qr.rows.iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        ids,
        vec![Value::Integer(6), Value::Integer(7), Value::Integer(8)]
    );
}
