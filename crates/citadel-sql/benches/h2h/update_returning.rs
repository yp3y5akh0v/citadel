use citadel_sql::{Connection, Value};
use criterion::{black_box, BenchmarkId, Criterion};

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

    assert_eq!(
        cs.query_collect(&[Value::Integer(1), Value::Integer(0)])
            .unwrap()
            .rows,
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        sqlite_collect_params(&mut ss, [1i64, 0]),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    cc.execute("UPDATE t SET c = 0 WHERE id = 0").unwrap();
    sc.execute("UPDATE t SET c = 0 WHERE id = 0", []).unwrap();

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..100i64 {
                black_box(
                    cs.query_collect(&[Value::Integer(1), Value::Integer(j)])
                        .unwrap(),
                );
            }
            cc.execute("COMMIT").unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..100i64 {
                black_box(sqlite_collect_params(&mut ss, rusqlite::params![1, j]));
            }
            sc.execute_batch("COMMIT").unwrap();
        });
    });
    g.finish();
}
