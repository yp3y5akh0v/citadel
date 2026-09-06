use citadel_sql::{Connection, Value};
use criterion::{black_box, BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("upsert_returning");

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
             ON CONFLICT (k) DO UPDATE SET c = c + 1 RETURNING c",
        )
        .unwrap();
    let mut ss = sc
        .prepare(
            "INSERT INTO ct VALUES (?1, 1) \
             ON CONFLICT (k) DO UPDATE SET c = c + 1 RETURNING c",
        )
        .unwrap();

    assert_eq!(
        cs.query_collect(&[Value::Text("hot".into())]).unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        sqlite_collect_params(&mut ss, ["hot"]),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    cc.execute("UPDATE ct SET c = 0 WHERE k = 'hot'").unwrap();
    sc.execute("UPDATE ct SET c = 0 WHERE k = 'hot'", [])
        .unwrap();

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for _ in 0..100 {
                black_box(cs.query_collect(&[Value::Text("hot".into())]).unwrap());
            }
            cc.execute("COMMIT").unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for _ in 0..100 {
                black_box(sqlite_collect_params(&mut ss, rusqlite::params!["hot"]));
            }
            sc.execute_batch("COMMIT").unwrap();
        });
    });
    g.finish();
}
