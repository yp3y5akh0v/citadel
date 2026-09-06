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

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
        [],
    )
    .unwrap();

    let ci = cc
        .prepare("INSERT INTO t (id, val) VALUES ($1, 'x')")
        .unwrap();
    let mut si = sc
        .prepare("INSERT INTO t (id, val) VALUES (?1, 'x')")
        .unwrap();
    let c_del = cc.prepare("DELETE FROM t").unwrap();
    let mut s_del = sc.prepare("DELETE FROM t").unwrap();
    let c_run = || {
        cc.execute("BEGIN").unwrap();
        for level in 0i64..10 {
            cc.execute(&format!("SAVEPOINT sp{level}")).unwrap();
            for id in level * 100..(level + 1) * 100 {
                ci.execute(&[Value::Integer(id)]).unwrap();
            }
        }
        cc.execute("ROLLBACK TO SAVEPOINT sp5").unwrap();
        for level in (0..=5).rev() {
            cc.execute(&format!("RELEASE SAVEPOINT sp{level}")).unwrap();
        }
        cc.execute("COMMIT").unwrap();
    };
    let mut s_run = || {
        sc.execute_batch("BEGIN").unwrap();
        for level in 0i64..10 {
            sc.execute_batch(&format!("SAVEPOINT sp{level}")).unwrap();
            for id in level * 100..(level + 1) * 100 {
                si.execute(rusqlite::params![id]).unwrap();
            }
        }
        sc.execute_batch("ROLLBACK TO SAVEPOINT sp5").unwrap();
        for level in (0..=5).rev() {
            sc.execute_batch(&format!("RELEASE SAVEPOINT sp{level}"))
                .unwrap();
        }
        sc.execute_batch("COMMIT").unwrap();
    };

    for _ in 0..2 {
        c_run();
        assert_eq!(
            cc.query("SELECT id, val FROM t ORDER BY id").unwrap().rows,
            (0..500)
                .map(|id| vec![Value::Integer(id), Value::Text("x".into())])
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
            (0..500i64)
                .map(|id| (id, "x".to_owned()))
                .collect::<Vec<_>>()
        );
        s_del.execute([]).unwrap();
    }

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        iter_with_cleanup(b, c_run, || {
            c_del.execute(&[]).unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        iter_with_cleanup(b, &mut s_run, || {
            s_del.execute([]).unwrap();
        });
    });
    g.finish();
}
