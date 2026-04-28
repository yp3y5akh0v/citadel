use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("update_gen_propagate");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 2 + 1) STORED)",
    )
    .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in 0..100i64 {
        cc.execute(&format!("INSERT INTO t (id, a) VALUES ({i}, {i})"))
            .unwrap();
    }
    cc.execute("COMMIT").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 2 + 1) STORED)",
        [],
    )
    .unwrap();
    sc.execute_batch("BEGIN").unwrap();
    for i in 0..100i64 {
        sc.execute(
            "INSERT INTO t (id, a) VALUES (?1, ?1)",
            rusqlite::params![i],
        )
        .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let cs = cc.prepare("UPDATE t SET a = a + $1 WHERE id = $2").unwrap();
    let mut ss = sc.prepare("UPDATE t SET a = a + ?1 WHERE id = ?2").unwrap();

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..100i64 {
                cs.execute(&[Value::Integer(1), Value::Integer(j)]).unwrap();
            }
            cc.execute("COMMIT").unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..100i64 {
                ss.execute(rusqlite::params![1, j]).unwrap();
            }
            sc.execute_batch("COMMIT").unwrap();
        });
    });
    g.finish();
}
