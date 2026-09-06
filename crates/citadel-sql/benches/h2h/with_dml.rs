use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

const ROWS: i64 = 100;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("with_dml");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    cc.execute("CREATE TABLE archive (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)",
        [],
    )
    .unwrap();
    sc.execute(
        "CREATE TABLE archive (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)",
        [],
    )
    .unwrap();

    let cins = cc
        .prepare("INSERT INTO src (id, val) VALUES ($1, $2)")
        .unwrap();
    let cmove = cc
        .prepare("WITH d AS (DELETE FROM src RETURNING *) INSERT INTO archive SELECT * FROM d")
        .unwrap();
    let mut sins = sc
        .prepare("INSERT INTO src (id, val) VALUES (?1, ?2)")
        .unwrap();
    let mut s_arch = sc.prepare("INSERT INTO archive SELECT * FROM src").unwrap();
    let mut s_clear_src = sc.prepare("DELETE FROM src").unwrap();
    let mut s_clear_archive = sc.prepare("DELETE FROM archive").unwrap();

    let cclear = cc.prepare("DELETE FROM archive").unwrap();

    let c_run = || {
        cc.execute("BEGIN").unwrap();
        for id in 0..ROWS {
            cins.execute(&[Value::Integer(id), Value::Integer(id * 10)])
                .unwrap();
        }
        cc.execute("COMMIT").unwrap();
        cmove.execute(&[]).unwrap();
    };
    let mut s_run = || {
        sc.execute_batch("BEGIN").unwrap();
        for id in 0..ROWS {
            sins.execute(rusqlite::params![id, id * 10]).unwrap();
        }
        sc.execute_batch("COMMIT").unwrap();
        sc.execute_batch("BEGIN").unwrap();
        s_arch.execute([]).unwrap();
        s_clear_src.execute([]).unwrap();
        sc.execute_batch("COMMIT").unwrap();
    };

    let expected_c = (0..ROWS)
        .map(|id| vec![Value::Integer(id), Value::Integer(id * 10)])
        .collect::<Vec<_>>();
    let expected_s = (0..ROWS)
        .map(|id| {
            vec![
                rusqlite::types::Value::Integer(id),
                rusqlite::types::Value::Integer(id * 10),
            ]
        })
        .collect::<Vec<_>>();
    let mut s_source_rows = sc.prepare("SELECT id FROM src").unwrap();
    let mut s_archive_rows = sc
        .prepare("SELECT id, val FROM archive ORDER BY id")
        .unwrap();
    for _ in 0..2 {
        c_run();
        assert!(cc.query("SELECT id FROM src").unwrap().rows.is_empty());
        assert_eq!(
            cc.query("SELECT id, val FROM archive ORDER BY id")
                .unwrap()
                .rows,
            expected_c
        );
        cclear.execute(&[]).unwrap();

        s_run();
        assert!(sqlite_collect_stmt(&mut s_source_rows).is_empty());
        assert_eq!(sqlite_collect_stmt(&mut s_archive_rows), expected_s);
        s_clear_archive.execute([]).unwrap();
    }

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            c_run();
            cclear.execute(&[]).unwrap();
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            s_run();
            s_clear_archive.execute([]).unwrap();
        });
    });
    g.finish();
}
