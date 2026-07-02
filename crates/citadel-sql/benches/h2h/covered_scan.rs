use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

fn sqlite_collect_params(
    stmt: &mut rusqlite::Statement<'_>,
    p: i64,
) -> Vec<Vec<rusqlite::types::Value>> {
    let ncols = stmt.column_count();
    let mut rows = stmt.query(rusqlite::params![p]).unwrap();
    let mut out = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        out.push((0..ncols).map(|i| row.get(i).unwrap()).collect());
    }
    out
}

pub fn bench(c: &mut Criterion) {
    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    citadel_100k(&cc);
    cc.execute("CREATE INDEX t_age ON t (age)").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);
    sc.execute_batch("CREATE INDEX t_age ON t (age)").unwrap();

    // Rotating params keep the single-slot result memo cold: execution speed.
    let mut g = c.benchmark_group("covered_range");
    let sql = "SELECT age, id FROM t WHERE age = $1";
    let cs = cc.prepare(sql).unwrap();
    let mut ss = sc.prepare("SELECT age, id FROM t WHERE age = ?1").unwrap();
    let mut ci = 0i64;
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            ci = (ci + 1) % 100;
            cs.query_collect(&[Value::Integer(ci)]).unwrap()
        });
    });
    let mut si = 0i64;
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            si = (si + 1) % 100;
            sqlite_collect_params(&mut ss, si)
        });
    });
    g.finish();

    let mut g = c.benchmark_group("covered_count");
    let sql = "SELECT COUNT(*) FROM t WHERE age >= $1";
    let cs = cc.prepare(sql).unwrap();
    let mut ss = sc
        .prepare("SELECT COUNT(*) FROM t WHERE age >= ?1")
        .unwrap();
    let mut ci = 0i64;
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            ci = (ci + 1) % 100;
            cs.query_collect(&[Value::Integer(ci)]).unwrap()
        });
    });
    let mut si = 0i64;
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            si = (si + 1) % 100;
            sqlite_collect_params(&mut ss, si)
        });
    });
    g.finish();

    let mut g = c.benchmark_group("sort_paginate_pk");
    let sql = "SELECT id, name FROM t WHERE id > $1 ORDER BY id LIMIT 20";
    let cs = cc.prepare(sql).unwrap();
    let mut ss = sc
        .prepare("SELECT id, name FROM t WHERE id > ?1 ORDER BY id LIMIT 20")
        .unwrap();
    let mut ci = 0i64;
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            ci = (ci + 20) % 99_000;
            cs.query_collect(&[Value::Integer(ci)]).unwrap()
        });
    });
    let mut si = 0i64;
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            si = (si + 20) % 99_000;
            sqlite_collect_params(&mut ss, si)
        });
    });
    g.finish();
}
