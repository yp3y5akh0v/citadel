use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert_gen_virtual");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER, \
         s INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL)",
    )
    .unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER, \
         s INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL)",
        [],
    )
    .unwrap();

    let cs = cc
        .prepare("INSERT INTO t (id, a, b) VALUES ($1, $2, $3)")
        .unwrap();
    let mut ss = sc
        .prepare("INSERT INTO t (id, a, b) VALUES (?1, ?2, ?3)")
        .unwrap();

    let mut c_operation = || {
        cc.execute("BEGIN").unwrap();
        for j in 0..100i64 {
            cs.execute(&[Value::Integer(j), Value::Integer(j), Value::Integer(j * 2)])
                .unwrap();
        }
        cc.execute("COMMIT").unwrap();
    };
    let mut c_cleanup = || {
        cc.execute("DELETE FROM t").unwrap();
    };
    let mut s_operation = || {
        sc.execute_batch("BEGIN").unwrap();
        for j in 0..100i64 {
            ss.execute(rusqlite::params![j, j, j * 2]).unwrap();
        }
        sc.execute_batch("COMMIT").unwrap();
    };
    let mut s_cleanup = || {
        sc.execute("DELETE FROM t", []).unwrap();
    };

    let expected_c: Vec<_> = (0..100i64)
        .map(|j| {
            vec![
                Value::Integer(j),
                Value::Integer(j),
                Value::Integer(j * 2),
                Value::Integer(j * 3),
            ]
        })
        .collect();
    let expected_s: Vec<_> = (0..100i64)
        .map(|j| {
            vec![
                rusqlite::types::Value::Integer(j),
                rusqlite::types::Value::Integer(j),
                rusqlite::types::Value::Integer(j * 2),
                rusqlite::types::Value::Integer(j * 3),
            ]
        })
        .collect();
    let check_c = cc.prepare("SELECT id, a, b, s FROM t ORDER BY id").unwrap();
    let mut check_s = sc.prepare("SELECT id, a, b, s FROM t ORDER BY id").unwrap();
    for _ in 0..2 {
        c_operation();
        s_operation();
        assert_eq!(check_c.query_collect(&[]).unwrap().rows, expected_c);
        assert_eq!(sqlite_collect_stmt(&mut check_s), expected_s);
        c_cleanup();
        s_cleanup();
        assert!(check_c.query_collect(&[]).unwrap().rows.is_empty());
        assert!(sqlite_collect_stmt(&mut check_s).is_empty());
    }

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        iter_with_cleanup(b, &mut c_operation, &mut c_cleanup);
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        iter_with_cleanup(b, &mut s_operation, &mut s_cleanup);
    });
    g.finish();
}
