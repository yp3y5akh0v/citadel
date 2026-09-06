use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("upsert_all_new");

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
        .prepare("INSERT INTO t (id, val) VALUES ($1, 'v') ON CONFLICT (id) DO NOTHING")
        .unwrap();
    let mut ss = sc
        .prepare("INSERT INTO t (id, val) VALUES (?1, 'v') ON CONFLICT (id) DO NOTHING")
        .unwrap();
    let mut c_operation = || {
        cc.execute("BEGIN").unwrap();
        for j in 0..100i64 {
            cs.execute(&[Value::Integer(j)]).unwrap();
        }
        cc.execute("COMMIT").unwrap();
    };
    let mut c_cleanup = || {
        cc.execute("DELETE FROM t").unwrap();
    };
    let mut s_operation = || {
        sc.execute_batch("BEGIN").unwrap();
        for j in 0..100i64 {
            ss.execute(rusqlite::params![j]).unwrap();
        }
        sc.execute_batch("COMMIT").unwrap();
    };
    let mut s_cleanup = || {
        sc.execute("DELETE FROM t", []).unwrap();
    };

    let expected_c: Vec<_> = (0..100i64)
        .map(|id| vec![Value::Integer(id), Value::Text("v".into())])
        .collect();
    let expected_s: Vec<_> = (0..100i64)
        .map(|id| {
            vec![
                rusqlite::types::Value::Integer(id),
                rusqlite::types::Value::Text("v".into()),
            ]
        })
        .collect();
    let check_c = cc.prepare("SELECT id, val FROM t ORDER BY id").unwrap();
    let mut check_s = sc.prepare("SELECT id, val FROM t ORDER BY id").unwrap();
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
