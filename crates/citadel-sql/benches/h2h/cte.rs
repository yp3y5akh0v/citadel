use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("cte");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    citadel_100k(&cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    let sql = "WITH filtered AS (SELECT id, name, age FROM t WHERE age < 50) \
               SELECT age, COUNT(*) FROM filtered GROUP BY age";
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
