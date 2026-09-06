use citadel_sql::{Connection, Value};
use criterion::{black_box, BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert_returning");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
        [],
    )
    .unwrap();

    let cs = cc
        .prepare("INSERT INTO t (id, val) VALUES ($1, 'v') RETURNING id, val")
        .unwrap();
    let mut ss = sc
        .prepare("INSERT INTO t (id, val) VALUES (?1, 'v') RETURNING id, val")
        .unwrap();

    assert_eq!(
        cs.query_collect(&[Value::Integer(-1)]).unwrap().rows,
        vec![vec![Value::Integer(-1), Value::Text("v".into())]]
    );
    assert_eq!(
        sqlite_collect_params(&mut ss, [-1i64]),
        vec![vec![
            rusqlite::types::Value::Integer(-1),
            rusqlite::types::Value::Text("v".into()),
        ]]
    );
    cc.execute("DELETE FROM t WHERE id = -1").unwrap();
    sc.execute("DELETE FROM t WHERE id = -1", []).unwrap();

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        iter_with_cleanup(
            b,
            || {
                cc.execute("BEGIN").unwrap();
                for j in 0..100i64 {
                    black_box(cs.query_collect(&[Value::Integer(j)]).unwrap());
                }
                cc.execute("COMMIT").unwrap();
            },
            || {
                cc.execute("DELETE FROM t").unwrap();
            },
        );
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        iter_with_cleanup(
            b,
            || {
                sc.execute_batch("BEGIN").unwrap();
                for j in 0..100i64 {
                    black_box(sqlite_collect_params(&mut ss, rusqlite::params![j]));
                }
                sc.execute_batch("COMMIT").unwrap();
            },
            || {
                sc.execute("DELETE FROM t", []).unwrap();
            },
        );
    });
    g.finish();
}
