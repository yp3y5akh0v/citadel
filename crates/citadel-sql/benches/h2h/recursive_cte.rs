use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("recursive_cte");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());

    let sql = "WITH RECURSIVE seq(x) AS (\
                   SELECT 1 \
                   UNION ALL \
                   SELECT x + 1 FROM seq WHERE x < 1000\
               ) SELECT SUM(x) FROM seq";
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
