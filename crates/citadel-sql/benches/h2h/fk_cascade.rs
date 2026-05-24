use std::time::Instant;

use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

const CHILDREN: i64 = 100;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("fk_cascade");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    cc.execute(
        "CREATE TABLE child (id INTEGER NOT NULL PRIMARY KEY, p INTEGER, \
         FOREIGN KEY (p) REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    sc.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)", [])
        .unwrap();
    sc.execute(
        "CREATE TABLE child (id INTEGER NOT NULL PRIMARY KEY, p INTEGER, \
         FOREIGN KEY (p) REFERENCES parent(id) ON DELETE CASCADE)",
        [],
    )
    .unwrap();

    let cins_p = cc.prepare("INSERT INTO parent (id) VALUES ($1)").unwrap();
    let cins_c = cc
        .prepare("INSERT INTO child (id, p) VALUES ($1, $2)")
        .unwrap();
    let cdel = cc.prepare("DELETE FROM parent WHERE id = $1").unwrap();
    let mut sins_p = sc.prepare("INSERT INTO parent (id) VALUES (?1)").unwrap();
    let mut sins_c = sc
        .prepare("INSERT INTO child (id, p) VALUES (?1, ?2)")
        .unwrap();
    let mut sdel = sc.prepare("DELETE FROM parent WHERE id = ?1").unwrap();

    let mut c_off = 0i64;
    let mut s_off = 0i64;

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            cins_p.execute(&[Value::Integer(c_off)]).unwrap();
            for j in 0..CHILDREN {
                cins_c
                    .execute(&[Value::Integer(c_off * CHILDREN + j), Value::Integer(c_off)])
                    .unwrap();
            }
            cc.execute("COMMIT").unwrap();
            cdel.execute(&[Value::Integer(c_off)]).unwrap();
            c_off += 1;
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            sins_p.execute(rusqlite::params![s_off]).unwrap();
            for j in 0..CHILDREN {
                sins_c
                    .execute(rusqlite::params![s_off * CHILDREN + j, s_off])
                    .unwrap();
            }
            sc.execute_batch("COMMIT").unwrap();
            sdel.execute(rusqlite::params![s_off]).unwrap();
            s_off += 1;
        });
    });
    g.finish();

    bench_delete_only(c);
}

/// Diagnostic bench: only times the cascading DELETE. The setup (BEGIN +
/// 1 INSERT parent + 100 INSERTs child + COMMIT) is done before each timer
/// start, so the holistic-bench's parsing/insert/commit overhead is
/// excluded.
fn bench_delete_only(c: &mut Criterion) {
    let mut g = c.benchmark_group("fk_cascade_delete_only");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    cc.execute(
        "CREATE TABLE child (id INTEGER NOT NULL PRIMARY KEY, p INTEGER, \
         FOREIGN KEY (p) REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    let cins_p = cc.prepare("INSERT INTO parent (id) VALUES ($1)").unwrap();
    let cins_c = cc
        .prepare("INSERT INTO child (id, p) VALUES ($1, $2)")
        .unwrap();
    let cdel = cc.prepare("DELETE FROM parent WHERE id = $1").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    sc.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)", [])
        .unwrap();
    sc.execute(
        "CREATE TABLE child (id INTEGER NOT NULL PRIMARY KEY, p INTEGER, \
         FOREIGN KEY (p) REFERENCES parent(id) ON DELETE CASCADE)",
        [],
    )
    .unwrap();
    let mut sins_p = sc.prepare("INSERT INTO parent (id) VALUES (?1)").unwrap();
    let mut sins_c = sc
        .prepare("INSERT INTO child (id, p) VALUES (?1, ?2)")
        .unwrap();
    let mut sdel = sc.prepare("DELETE FROM parent WHERE id = ?1").unwrap();

    let mut c_off = 0i64;
    let mut s_off = 0i64;

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter_custom(|iters| {
            for _ in 0..iters {
                cc.execute("BEGIN").unwrap();
                cins_p.execute(&[Value::Integer(c_off)]).unwrap();
                for j in 0..CHILDREN {
                    cins_c
                        .execute(&[Value::Integer(c_off * CHILDREN + j), Value::Integer(c_off)])
                        .unwrap();
                }
                cc.execute("COMMIT").unwrap();
                c_off += 1;
            }
            let start_off = c_off - iters as i64;
            let start = Instant::now();
            for k in 0..iters {
                cdel.execute(&[Value::Integer(start_off + k as i64)])
                    .unwrap();
            }
            start.elapsed()
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter_custom(|iters| {
            for _ in 0..iters {
                sc.execute_batch("BEGIN").unwrap();
                sins_p.execute(rusqlite::params![s_off]).unwrap();
                for j in 0..CHILDREN {
                    sins_c
                        .execute(rusqlite::params![s_off * CHILDREN + j, s_off])
                        .unwrap();
                }
                sc.execute_batch("COMMIT").unwrap();
                s_off += 1;
            }
            let start_off = s_off - iters as i64;
            let start = Instant::now();
            for k in 0..iters {
                sdel.execute(rusqlite::params![start_off + k as i64])
                    .unwrap();
            }
            start.elapsed()
        });
    });
    g.finish();
}
