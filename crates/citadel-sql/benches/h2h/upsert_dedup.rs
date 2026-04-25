use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("upsert_dedup");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in 0..800i64 {
        cc.execute(&format!("INSERT INTO t VALUES ({i}, 'v')"))
            .unwrap();
    }
    cc.execute("COMMIT").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
        [],
    )
    .unwrap();
    sc.execute_batch("BEGIN").unwrap();
    for i in 0..800i64 {
        sc.execute("INSERT INTO t VALUES (?1, 'v')", rusqlite::params![i])
            .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let cs = cc
        .prepare("INSERT INTO t VALUES ($1, 'x') ON CONFLICT (id) DO NOTHING")
        .unwrap();
    let mut ss = sc
        .prepare("INSERT INTO t VALUES (?1, 'x') ON CONFLICT (id) DO NOTHING")
        .unwrap();

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..100i64 {
                cs.execute(&[Value::Integer(j % 1000)]).unwrap();
            }
            cc.execute("COMMIT").unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..100i64 {
                ss.execute(rusqlite::params![j % 1000]).unwrap();
            }
            sc.execute_batch("COMMIT").unwrap();
        });
    });
    g.finish();
}
