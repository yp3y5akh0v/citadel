use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

// Projection-width sweep over a 24-column, 10k-row table: pk / 2col / 3col / full.
pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("wide_proj");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    citadel_wide(&cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_wide(&sc);

    let queries = [
        ("pk", "SELECT id FROM wide"),
        ("2col", "SELECT id, k1 FROM wide"),
        ("3col", "SELECT id, k1, t1 FROM wide"),
        ("full", "SELECT * FROM wide"),
    ];
    for (name, sql) in queries {
        let cs = cc.prepare(sql).unwrap();
        g.bench_function(BenchmarkId::new("citadel", name), |b| {
            b.iter(|| cs.query_collect(&[]).unwrap());
        });
        let mut ss = sc.prepare(sql).unwrap();
        g.bench_function(BenchmarkId::new("sqlite", name), |b| {
            b.iter(|| sqlite_collect_stmt(&mut ss));
        });
    }
    g.finish();
}
