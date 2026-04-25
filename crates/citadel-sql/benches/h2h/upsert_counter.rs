use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("upsert_counter");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE ct (k TEXT NOT NULL PRIMARY KEY, c INTEGER)")
        .unwrap();
    cc.execute("INSERT INTO ct VALUES ('hot', 0)").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE ct (k TEXT NOT NULL PRIMARY KEY, c INTEGER)",
        [],
    )
    .unwrap();
    sc.execute("INSERT INTO ct VALUES ('hot', 0)", []).unwrap();

    let cs = cc
        .prepare(
            "INSERT INTO ct VALUES ($1, 1) \
             ON CONFLICT (k) DO UPDATE SET c = c + 1",
        )
        .unwrap();
    let mut ss = sc
        .prepare(
            "INSERT INTO ct VALUES (?1, 1) \
             ON CONFLICT (k) DO UPDATE SET c = c + 1",
        )
        .unwrap();

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for _ in 0..100 {
                cs.execute(&[Value::Text("hot".into())]).unwrap();
            }
            cc.execute("COMMIT").unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for _ in 0..100 {
                ss.execute(rusqlite::params!["hot"]).unwrap();
            }
            sc.execute_batch("COMMIT").unwrap();
        });
    });
    g.finish();
}
