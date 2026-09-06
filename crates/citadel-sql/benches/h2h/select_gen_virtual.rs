use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("select_gen_virtual");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER, \
         s INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL)",
    )
    .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in 0..100i64 {
        cc.execute(&format!(
            "INSERT INTO t (id, a, b) VALUES ({i}, {i}, {})",
            i * 2
        ))
        .unwrap();
    }
    cc.execute("COMMIT").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER, \
         s INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL)",
        [],
    )
    .unwrap();
    sc.execute_batch("BEGIN").unwrap();
    for i in 0..100i64 {
        sc.execute(
            "INSERT INTO t (id, a, b) VALUES (?1, ?1, ?2)",
            rusqlite::params![i, i * 2],
        )
        .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let cs = cc.prepare("SELECT id, s FROM t WHERE s > $1").unwrap();
    let mut ss = sc.prepare("SELECT id, s FROM t WHERE s > ?1").unwrap();

    let mut citadel_rows: Vec<_> = cs
        .query_collect(&[Value::Integer(50)])
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Integer(id), Value::Integer(s)] => (*id, *s),
            _ => panic!("unexpected Citadel generated-column row: {row:?}"),
        })
        .collect();
    let mut sqlite_rows: Vec<_> = sqlite_collect_params(&mut ss, rusqlite::params![50])
        .into_iter()
        .map(|row| match row.as_slice() {
            [rusqlite::types::Value::Integer(id), rusqlite::types::Value::Integer(s)] => (*id, *s),
            _ => panic!("unexpected SQLite generated-column row: {row:?}"),
        })
        .collect();
    citadel_rows.sort_unstable();
    sqlite_rows.sort_unstable();
    // s = 3 * id, so s > 50 selects exactly the 83 ids from 17 through 99.
    let expected: Vec<_> = (17..100).map(|id| (id, id * 3)).collect();
    assert_eq!(citadel_rows, expected);
    assert_eq!(sqlite_rows, citadel_rows);

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cs.query_collect(&[Value::Integer(50)]).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect_params(&mut ss, rusqlite::params![50]));
    });
    g.finish();
}
