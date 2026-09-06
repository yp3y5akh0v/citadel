use citadel_sql::{Connection, Value};
use criterion::{black_box, BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("delete_returning");

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

    let cs_ins = cc
        .prepare("INSERT INTO t (id, val) VALUES ($1, 'v')")
        .unwrap();
    let mut ss_ins = sc
        .prepare("INSERT INTO t (id, val) VALUES (?1, 'v')")
        .unwrap();

    let cs_del = cc
        .prepare("DELETE FROM t WHERE id = $1 RETURNING id, val")
        .unwrap();
    let mut ss_del = sc
        .prepare("DELETE FROM t WHERE id = ?1 RETURNING id, val")
        .unwrap();

    cs_ins.execute(&[Value::Integer(-1)]).unwrap();
    ss_ins.execute([-1i64]).unwrap();
    assert_eq!(
        cs_del.query_collect(&[Value::Integer(-1)]).unwrap().rows,
        vec![vec![Value::Integer(-1), Value::Text("v".into())]]
    );
    assert_eq!(
        sqlite_collect_params(&mut ss_del, [-1i64]),
        vec![vec![
            rusqlite::types::Value::Integer(-1),
            rusqlite::types::Value::Text("v".into()),
        ]]
    );

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..100i64 {
                cs_ins.execute(&[Value::Integer(c_offset + j)]).unwrap();
            }
            for j in 0..100i64 {
                black_box(
                    cs_del
                        .query_collect(&[Value::Integer(c_offset + j)])
                        .unwrap(),
                );
            }
            cc.execute("COMMIT").unwrap();
            c_offset += 100;
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..100i64 {
                ss_ins.execute(rusqlite::params![s_offset + j]).unwrap();
            }
            for j in 0..100i64 {
                black_box(sqlite_collect_params(
                    &mut ss_del,
                    rusqlite::params![s_offset + j],
                ));
            }
            sc.execute_batch("COMMIT").unwrap();
            s_offset += 100;
        });
    });
    g.finish();
}
