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

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
        [],
    )
    .unwrap();

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
    let c_run = || {
        cc.execute("BEGIN").unwrap();
        for id in 0i64..1_000 {
            c_pre.execute(&[Value::Integer(id)]).unwrap();
        }
        cc.execute("SAVEPOINT sp").unwrap();
        for id in 1_000i64..11_000 {
            c_post.execute(&[Value::Integer(id)]).unwrap();
        }
        cc.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
        cc.execute("COMMIT").unwrap();
    };
    let mut s_run = || {
        sc.execute_batch("BEGIN").unwrap();
        for id in 0i64..1_000 {
            s_pre.execute(rusqlite::params![id]).unwrap();
        }
        sc.execute_batch("SAVEPOINT sp").unwrap();
        for id in 1_000i64..11_000 {
            s_post.execute(rusqlite::params![id]).unwrap();
        }
        sc.execute_batch("ROLLBACK TO SAVEPOINT sp").unwrap();
        sc.execute_batch("COMMIT").unwrap();
    };

    for _ in 0..2 {
        c_run();
        assert_eq!(
            cc.query("SELECT id, val FROM t ORDER BY id").unwrap().rows,
            (0..1_000)
                .map(|id| vec![Value::Integer(id), Value::Text("pre".into())])
                .collect::<Vec<_>>()
        );
        c_del.execute(&[]).unwrap();
        s_run();
        let s_rows = sc
            .prepare("SELECT id, val FROM t ORDER BY id")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            s_rows,
            (0..1_000i64)
                .map(|id| (id, "pre".to_owned()))
                .collect::<Vec<_>>()
        );
        s_del.execute([]).unwrap();
    }

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            c_run();
            c_del.execute(&[]).unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            s_run();
            s_del.execute([]).unwrap();
        });
    });
    g.finish();
}
