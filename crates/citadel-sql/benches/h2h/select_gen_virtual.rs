use citadel_sql::Connection;
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

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            let _ = cs
                .query_collect(&[citadel_sql::Value::Integer(50)])
                .unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            let mut rows = ss.query(rusqlite::params![50]).unwrap();
            while rows.next().unwrap().is_some() {}
        });
    });
    g.finish();
}
