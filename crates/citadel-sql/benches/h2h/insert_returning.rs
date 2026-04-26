use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert_returning");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    let mut c_offset = 0i64;

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
        [],
    )
    .unwrap();
    let mut s_offset = 0i64;

    let cs = cc
        .prepare("INSERT INTO t (id, val) VALUES ($1, 'v') RETURNING id, val")
        .unwrap();
    let mut ss = sc
        .prepare("INSERT INTO t (id, val) VALUES (?1, 'v') RETURNING id, val")
        .unwrap();

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..100i64 {
                let _ = cs.query_collect(&[Value::Integer(c_offset + j)]).unwrap();
            }
            cc.execute("COMMIT").unwrap();
            c_offset += 100;
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..100i64 {
                let mut rows = ss.query(rusqlite::params![s_offset + j]).unwrap();
                while rows.next().unwrap().is_some() {}
            }
            sc.execute_batch("COMMIT").unwrap();
            s_offset += 100;
        });
    });
    g.finish();
}
