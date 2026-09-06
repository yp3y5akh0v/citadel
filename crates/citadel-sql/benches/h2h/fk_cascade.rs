use std::time::{Duration, Instant};

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

    validate_cascade(&cc, &sc);

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

/// Times cascading DELETE with one parent and 100 children for every iteration.
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
    validate_cascade(&cc, &sc);
    let mut sins_p = sc.prepare("INSERT INTO parent (id) VALUES (?1)").unwrap();
    let mut sins_c = sc
        .prepare("INSERT INTO child (id, p) VALUES (?1, ?2)")
        .unwrap();
    let mut sdel = sc.prepare("DELETE FROM parent WHERE id = ?1").unwrap();

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                cc.execute("BEGIN").unwrap();
                cins_p.execute(&[Value::Integer(0)]).unwrap();
                for j in 0..CHILDREN {
                    cins_c
                        .execute(&[Value::Integer(j), Value::Integer(0)])
                        .unwrap();
                }
                cc.execute("COMMIT").unwrap();
                let start = Instant::now();
                cdel.execute(&[Value::Integer(0)]).unwrap();
                elapsed += start.elapsed();
            }
            elapsed
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                sc.execute_batch("BEGIN").unwrap();
                sins_p.execute(rusqlite::params![0]).unwrap();
                for j in 0..CHILDREN {
                    sins_c.execute(rusqlite::params![j, 0]).unwrap();
                }
                sc.execute_batch("COMMIT").unwrap();
                let start = Instant::now();
                sdel.execute(rusqlite::params![0]).unwrap();
                elapsed += start.elapsed();
            }
            elapsed
        });
    });
    g.finish();
}

fn validate_cascade(cc: &Connection<'_>, sc: &rusqlite::Connection) {
    cc.execute("BEGIN").unwrap();
    cc.execute("INSERT INTO parent VALUES (-1)").unwrap();
    sc.execute_batch("BEGIN; INSERT INTO parent VALUES (-1)")
        .unwrap();
    for j in 0..CHILDREN {
        cc.execute(&format!("INSERT INTO child VALUES ({j}, -1)"))
            .unwrap();
        sc.execute("INSERT INTO child VALUES (?1, -1)", [j])
            .unwrap();
    }
    cc.execute("COMMIT").unwrap();
    sc.execute_batch("COMMIT").unwrap();
    assert_eq!(
        cc.prepare("DELETE FROM parent WHERE id = -1")
            .unwrap()
            .execute(&[])
            .unwrap(),
        1
    );
    assert_eq!(
        sc.execute("DELETE FROM parent WHERE id = -1", []).unwrap(),
        1
    );
    for table in ["parent", "child"] {
        let sql = format!("SELECT COUNT(*) FROM {table}");
        assert_eq!(cc.query(&sql).unwrap().rows, vec![vec![Value::Integer(0)]]);
        assert_eq!(
            sc.query_row(&sql, [], |row| row.get::<_, i64>(0)).unwrap(),
            0
        );
    }
}
