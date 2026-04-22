use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("savepoint_nested");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    let mut c_off = 0i64;

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
        [],
    )
    .unwrap();
    let mut s_off = 0i64;

    let ci = cc
        .prepare("INSERT INTO t (id, val) VALUES ($1, 'x')")
        .unwrap();
    let mut si = sc
        .prepare("INSERT INTO t (id, val) VALUES (?1, 'x')")
        .unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for i in 0..10 {
                cc.execute(&format!("SAVEPOINT sp{i}")).unwrap();
                for _ in 0..100 {
                    ci.execute(&[Value::Integer(c_off)]).unwrap();
                    c_off += 1;
                }
                if i % 2 == 0 {
                    cc.execute(&format!("RELEASE SAVEPOINT sp{i}")).unwrap();
                } else {
                    cc.execute(&format!("ROLLBACK TO SAVEPOINT sp{i}")).unwrap();
                    cc.execute(&format!("RELEASE SAVEPOINT sp{i}")).unwrap();
                }
            }
            cc.execute("COMMIT").unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for i in 0..10 {
                sc.execute_batch(&format!("SAVEPOINT sp{i}")).unwrap();
                for _ in 0..100 {
                    si.execute(rusqlite::params![s_off]).unwrap();
                    s_off += 1;
                }
                if i % 2 == 0 {
                    sc.execute_batch(&format!("RELEASE SAVEPOINT sp{i}"))
                        .unwrap();
                } else {
                    sc.execute_batch(&format!("ROLLBACK TO SAVEPOINT sp{i}"))
                        .unwrap();
                    sc.execute_batch(&format!("RELEASE SAVEPOINT sp{i}"))
                        .unwrap();
                }
            }
            sc.execute_batch("COMMIT").unwrap();
        });
    });
    g.finish();
}
