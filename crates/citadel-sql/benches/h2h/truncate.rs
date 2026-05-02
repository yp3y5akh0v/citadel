use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("truncate");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)",
        [],
    )
    .unwrap();

    let ci = cc
        .prepare("INSERT INTO t (id, val) VALUES ($1, $2)")
        .unwrap();
    let ct = cc.prepare("TRUNCATE TABLE t").unwrap();
    let mut si = sc
        .prepare("INSERT INTO t (id, val) VALUES (?1, ?2)")
        .unwrap();
    let mut sd = sc.prepare("DELETE FROM t").unwrap();

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..100i64 {
                ci.execute(&[Value::Integer(j), Value::Integer(j)]).unwrap();
            }
            cc.execute("COMMIT").unwrap();
            ct.execute(&[]).unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..100i64 {
                si.execute(rusqlite::params![j, j]).unwrap();
            }
            sc.execute_batch("COMMIT").unwrap();
            sd.execute([]).unwrap();
        });
    });
    g.finish();
}
