use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("delete");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)",
        [],
    )
    .unwrap();

    let mut c_offset = 0i64;
    let mut s_offset = 0i64;

    let ci = cc
        .prepare("INSERT INTO d (id, val) VALUES ($1, $2)")
        .unwrap();
    let cd = cc
        .prepare("DELETE FROM d WHERE id >= $1 AND id < $2")
        .unwrap();
    let mut si = sc
        .prepare("INSERT INTO d (id, val) VALUES (?1, ?2)")
        .unwrap();
    let mut sd = sc
        .prepare("DELETE FROM d WHERE id >= ?1 AND id < ?2")
        .unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..100i64 {
                ci.execute(&[Value::Integer(c_offset + j), Value::Integer(j)])
                    .unwrap();
            }
            cc.execute("COMMIT").unwrap();
            cd.execute(&[Value::Integer(c_offset), Value::Integer(c_offset + 100)])
                .unwrap();
            c_offset += 100;
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..100i64 {
                si.execute(rusqlite::params![s_offset + j, j]).unwrap();
            }
            sc.execute_batch("COMMIT").unwrap();
            sd.execute(rusqlite::params![s_offset, s_offset + 100])
                .unwrap();
            s_offset += 100;
        });
    });
    g.finish();
}
