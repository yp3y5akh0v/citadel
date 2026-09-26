//! Integration tests for morsel-parallel streaming aggregation: a table large
//! enough to cross the parallel-dispatch threshold, aggregated through the
//! public API, must match arithmetically-computed expectations - including
//! rows from before an ALTER ADD COLUMN whose default feeds the aggregates.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"parallel-agg")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn large_table_aggregates_match_expectations() {
    const OLD_ROWS: i64 = 80_000;
    const NEW_ROWS: i64 = 1_000;
    const W_DEFAULT: i64 = 7;

    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    let ins = conn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    for k in 0..OLD_ROWS {
        ins.execute(&[Value::Integer(k), Value::Integer(k % 1000)])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    conn.execute(&format!(
        "ALTER TABLE t ADD COLUMN w INTEGER DEFAULT {W_DEFAULT}"
    ))
    .unwrap();

    conn.execute("BEGIN").unwrap();
    let ins3 = conn.prepare("INSERT INTO t VALUES ($1, $2, $3)").unwrap();
    for k in OLD_ROWS..OLD_ROWS + NEW_ROWS {
        ins3.execute(&[
            Value::Integer(k),
            Value::Integer(k % 1000),
            Value::Integer(k * 2 + 1),
        ])
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let total = OLD_ROWS + NEW_ROWS;
    let sum_v: i64 = (0..total).map(|k| k % 1000).sum();
    let sum_w: i64 = OLD_ROWS * W_DEFAULT
        + (OLD_ROWS..OLD_ROWS + NEW_ROWS)
            .map(|k| k * 2 + 1)
            .sum::<i64>();
    let max_w = (OLD_ROWS + NEW_ROWS - 1) * 2 + 1;

    let qr = conn
        .query(
            "SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), \
             COUNT(w), SUM(w), MIN(w), MAX(w) FROM t",
        )
        .unwrap();
    assert_eq!(
        qr.rows,
        vec![vec![
            Value::Integer(total),
            Value::Integer(total),
            Value::Integer(sum_v),
            Value::Integer(0),
            Value::Integer(999),
            Value::Integer(total),
            Value::Integer(sum_w),
            Value::Integer(W_DEFAULT),
            Value::Integer(max_w),
        ]]
    );
}
