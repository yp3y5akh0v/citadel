use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("upsert_mixed");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, c INTEGER)")
        .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in 0..500i64 {
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
    for i in 0..500i64 {
        sc.execute("INSERT INTO t VALUES (?1, 0)", rusqlite::params![i])
            .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let cs = cc
        .prepare(
            "INSERT INTO t VALUES ($1, 1) \
             ON CONFLICT (id) DO UPDATE SET c = c + 1",
        )
        .unwrap();
    let mut ss = sc
        .prepare(
            "INSERT INTO t VALUES (?1, 1) \
             ON CONFLICT (id) DO UPDATE SET c = c + 1",
        )
        .unwrap();

    let initial_conflicts = "SELECT COUNT(*) FROM t WHERE id >= 450 AND id < 550";
    assert_eq!(
        cc.query(initial_conflicts).unwrap().rows,
        vec![vec![Value::Integer(50)]]
    );
    assert_eq!(
        sc.query_row(initial_conflicts, [], |row| row.get::<_, i64>(0))
            .unwrap(),
        50
    );

    let mut c_operation = || {
        cc.execute("BEGIN").unwrap();
        for j in 0..100i64 {
            cs.execute(&[Value::Integer(450 + j)]).unwrap();
        }
        cc.execute("COMMIT").unwrap();
    };
    let mut c_cleanup = || {
        cc.execute("BEGIN").unwrap();
        cc.execute("DELETE FROM t WHERE id >= 500").unwrap();
        cc.execute("UPDATE t SET c = 0 WHERE id >= 450 AND id < 500")
            .unwrap();
        cc.execute("COMMIT").unwrap();
    };
    let state_sql = "SELECT COUNT(*), SUM(c) FROM t";
    c_operation();
    assert_eq!(
        cc.query(state_sql).unwrap().rows,
        vec![vec![Value::Integer(550), Value::Integer(100)]]
    );
    c_cleanup();
    assert_eq!(
        cc.query(state_sql).unwrap().rows,
        vec![vec![Value::Integer(500), Value::Integer(0)]]
    );
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        iter_with_cleanup(b, &mut c_operation, &mut c_cleanup);
    });

    let mut s_operation = || {
        sc.execute_batch("BEGIN").unwrap();
        for j in 0..100i64 {
            ss.execute(rusqlite::params![450 + j]).unwrap();
        }
        sc.execute_batch("COMMIT").unwrap();
    };
    let mut s_cleanup = || {
        sc.execute_batch(
            "BEGIN; DELETE FROM t WHERE id >= 500; \
             UPDATE t SET c = 0 WHERE id >= 450 AND id < 500; COMMIT",
        )
        .unwrap();
    };
    s_operation();
    assert_eq!(
        sc.query_row(state_sql, [], |row| Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?
        )))
        .unwrap(),
        (550, 100)
    );
    s_cleanup();
    assert_eq!(
        sc.query_row(state_sql, [], |row| Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?
        )))
        .unwrap(),
        (500, 0)
    );
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        iter_with_cleanup(b, &mut s_operation, &mut s_cleanup);
    });
    g.finish();
}
