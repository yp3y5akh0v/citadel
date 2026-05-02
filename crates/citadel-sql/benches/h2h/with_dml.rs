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

    let mut c_off = 0i64;
    let mut s_off = 0i64;

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..ROWS {
                cins.execute(&[Value::Integer(c_off + j), Value::Integer(j * 10)])
                    .unwrap();
            }
            cc.execute("COMMIT").unwrap();
            cmove.execute(&[]).unwrap();
            cclear.execute(&[]).unwrap();
            c_off += ROWS;
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..ROWS {
                sins.execute(rusqlite::params![s_off + j, j * 10]).unwrap();
            }
            sc.execute_batch("COMMIT").unwrap();
            s_arch.execute([]).unwrap();
            s_clear_src.execute([]).unwrap();
            s_clear_archive.execute([]).unwrap();
            s_off += ROWS;
        });
    });
    g.finish();
}
