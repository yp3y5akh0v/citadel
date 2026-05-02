use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("partial_index_point");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, email TEXT, deleted_at INTEGER)")
        .unwrap();
    cc.execute("CREATE UNIQUE INDEX t_email_active ON t(email) WHERE deleted_at IS NULL")
        .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in 0..100_000i64 {
        let deleted = if i < 1_000 {
            "NULL".to_string()
        } else {
            i.to_string()
        };
        cc.execute(&format!(
            "INSERT INTO t (id, email, deleted_at) VALUES ({i}, 'u{i}@x', {deleted})"
        ))
        .unwrap();
    }
    cc.execute("COMMIT").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, email TEXT, deleted_at INTEGER)",
        [],
    )
    .unwrap();
    sc.execute(
        "CREATE UNIQUE INDEX t_email_active ON t(email) WHERE deleted_at IS NULL",
        [],
    )
    .unwrap();
    sc.execute_batch("BEGIN").unwrap();
    for i in 0..100_000i64 {
        let deleted: Option<i64> = if i < 1_000 { None } else { Some(i) };
        sc.execute(
            "INSERT INTO t (id, email, deleted_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, format!("u{i}@x"), deleted],
        )
        .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let sql = "SELECT id FROM t WHERE email = 'u500@x' AND deleted_at IS NULL";
    let cs = cc.prepare(sql).unwrap();
    let mut ss = sc.prepare(sql).unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cs.query_collect(&[]).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect_stmt(&mut ss));
    });
    g.finish();
}
