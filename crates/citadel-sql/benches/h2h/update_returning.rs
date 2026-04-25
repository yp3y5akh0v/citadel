use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("update_returning");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, c INTEGER)")
        .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in 0..100i64 {
        cc.execute(&format!("INSERT INTO t VALUES ({i}, 0)"))
            .unwrap();
    }
    cc.execute("COMMIT").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, c INTEGER)",
        [],
    )
    .unwrap();
    sc.execute_batch("BEGIN").unwrap();
    for i in 0..100i64 {
        sc.execute("INSERT INTO t VALUES (?1, 0)", rusqlite::params![i])
            .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let cs = cc
        .prepare("UPDATE t SET c = c + $1 WHERE id = $2 RETURNING c")
        .unwrap();
    let mut ss = sc
        .prepare("UPDATE t SET c = c + ?1 WHERE id = ?2 RETURNING c")
        .unwrap();

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..100i64 {
                let _ = cs
                    .query_collect(&[Value::Integer(1), Value::Integer(j)])
                    .unwrap();
            }
            cc.execute("COMMIT").unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..100i64 {
                let mut rows = ss.query(rusqlite::params![1, j]).unwrap();
                while rows.next().unwrap().is_some() {}
            }
            sc.execute_batch("COMMIT").unwrap();
        });
    });
    g.finish();
}
