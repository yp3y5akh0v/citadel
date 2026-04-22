use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("savepoint_rollback");

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

    let c_pre = cc
        .prepare("INSERT INTO t (id, val) VALUES ($1, 'pre')")
        .unwrap();
    let c_post = cc
        .prepare("INSERT INTO t (id, val) VALUES ($1, 'post')")
        .unwrap();
    let c_del = cc.prepare("DELETE FROM t").unwrap();
    let mut s_pre = sc
        .prepare("INSERT INTO t (id, val) VALUES (?1, 'pre')")
        .unwrap();
    let mut s_post = sc
        .prepare("INSERT INTO t (id, val) VALUES (?1, 'post')")
        .unwrap();
    let mut s_del = sc.prepare("DELETE FROM t").unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for _ in 0..1_000 {
                c_pre.execute(&[Value::Integer(c_off)]).unwrap();
                c_off += 1;
            }
            cc.execute("SAVEPOINT sp").unwrap();
            for _ in 0..10_000 {
                c_post.execute(&[Value::Integer(c_off)]).unwrap();
                c_off += 1;
            }
            cc.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
            c_off -= 10_000;
            cc.execute("COMMIT").unwrap();
            c_del.execute(&[]).unwrap();
            c_off = 0;
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for _ in 0..1_000 {
                s_pre.execute(rusqlite::params![s_off]).unwrap();
                s_off += 1;
            }
            sc.execute_batch("SAVEPOINT sp").unwrap();
            for _ in 0..10_000 {
                s_post.execute(rusqlite::params![s_off]).unwrap();
                s_off += 1;
            }
            sc.execute_batch("ROLLBACK TO SAVEPOINT sp").unwrap();
            s_off -= 10_000;
            sc.execute_batch("COMMIT").unwrap();
            s_del.execute([]).unwrap();
            s_off = 0;
        });
    });
    g.finish();
}
