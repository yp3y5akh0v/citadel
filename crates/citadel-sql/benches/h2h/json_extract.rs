use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

const ROWS: i64 = 100_000;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("json_extract");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, data JSONB)")
        .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in 0..ROWS {
        let payload = format!(
            r#"{{"id":{i},"name":"user_{i}","age":{},"role":"member","city":"NYC"}}"#,
            i % 100
        );
        cc.execute(&format!(
            "INSERT INTO users (id, data) VALUES ({i}, '{payload}'::jsonb)"
        ))
        .unwrap();
    }
    cc.execute("COMMIT").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, data TEXT)",
        [],
    )
    .unwrap();
    sc.execute_batch("BEGIN").unwrap();
    for i in 0..ROWS {
        let payload = format!(
            r#"{{"id":{i},"name":"user_{i}","age":{},"role":"member","city":"NYC"}}"#,
            i % 100
        );
        sc.execute(
            "INSERT INTO users (id, data) VALUES (?1, ?2)",
            rusqlite::params![i, payload],
        )
        .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let cs = cc.prepare("SELECT data ->> 'name' FROM users").unwrap();
    let mut ss = sc
        .prepare("SELECT json_extract(data, '$.name') FROM users")
        .unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cs.query_collect(&[]).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect_stmt(&mut ss));
    });
    g.finish();
}
