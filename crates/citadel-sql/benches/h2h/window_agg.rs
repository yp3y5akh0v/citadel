use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("window_agg");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    citadel_100k(&cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    let sql = "SELECT id, age, \
               SUM(age) OVER (ORDER BY id ROWS BETWEEN 50 PRECEDING AND CURRENT ROW), \
               MIN(age) OVER (ORDER BY id ROWS BETWEEN 50 PRECEDING AND CURRENT ROW) \
               FROM t";
    let cs = cc.prepare(sql).unwrap();
    let mut ss = sc.prepare(sql).unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cs.query_collect(&[]).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect_stmt(&mut ss));
    });
    g.finish();
}
