use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("update");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    citadel_100k(&cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    let sql = "UPDATE t SET age = age + 1 WHERE id BETWEEN 10000 AND 10099";
    let cs = cc.prepare(sql).unwrap();
    let mut ss = sc.prepare(sql).unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cs.execute(&[]).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| ss.execute([]).unwrap());
    });
    g.finish();
}
