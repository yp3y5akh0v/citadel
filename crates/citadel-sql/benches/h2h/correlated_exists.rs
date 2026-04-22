use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("correlated_exists");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    citadel_100k(&cc);
    cc.execute("CREATE TABLE ref_table (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in (0..100_000i64).step_by(100) {
        cc.execute(&format!(
            "INSERT INTO ref_table (id, val) VALUES ({i}, {i})"
        ))
        .unwrap();
    }
    cc.execute("COMMIT").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);
    sc.execute(
        "CREATE TABLE ref_table (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)",
        [],
    )
    .unwrap();
    sc.execute_batch("BEGIN").unwrap();
    for i in (0..100_000i64).step_by(100) {
        sc.execute(
            "INSERT INTO ref_table (id, val) VALUES (?1, ?1)",
            rusqlite::params![i],
        )
        .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let sql =
        "SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM ref_table WHERE ref_table.id = t.id)";
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
