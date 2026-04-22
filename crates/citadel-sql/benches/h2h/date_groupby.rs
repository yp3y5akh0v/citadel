use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("date_groupby");
    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    citadel_date_table(&cc);
    let s = cc
        .prepare("SELECT DATE_TRUNC('month', ts), COUNT(*) FROM events GROUP BY 1")
        .unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| s.query_collect(&[]).unwrap());
    });
    g.finish();
}
