use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

const ROWS: i64 = 100_000;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("json_gin");

    let dir_seq = tempfile::tempdir().unwrap();
    let db_seq = citadel_db(dir_seq.path());
    let conn_seq = Connection::open(&db_seq).unwrap();
    conn_seq
        .execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, data JSONB)")
        .unwrap();
    conn_seq.execute("BEGIN").unwrap();
    for i in 0..ROWS {
        let role = if i % 1000 == 0 { "admin" } else { "member" };
        let payload = format!(
            r#"{{"id":{i},"name":"user_{i}","role":"{role}","city":"NYC","age":{}}}"#,
            i % 100
        );
        conn_seq
            .execute(&format!(
                "INSERT INTO users (id, data) VALUES ({i}, '{payload}'::jsonb)"
            ))
            .unwrap();
    }
    conn_seq.execute("COMMIT").unwrap();

    let dir_gin = tempfile::tempdir().unwrap();
    let db_gin = citadel_db(dir_gin.path());
    let conn_gin = Connection::open(&db_gin).unwrap();
    conn_gin
        .execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, data JSONB)")
        .unwrap();
    conn_gin.execute("BEGIN").unwrap();
    for i in 0..ROWS {
        let role = if i % 1000 == 0 { "admin" } else { "member" };
        let payload = format!(
            r#"{{"id":{i},"name":"user_{i}","role":"{role}","city":"NYC","age":{}}}"#,
            i % 100
        );
        conn_gin
            .execute(&format!(
                "INSERT INTO users (id, data) VALUES ({i}, '{payload}'::jsonb)"
            ))
            .unwrap();
    }
    conn_gin.execute("COMMIT").unwrap();
    conn_gin
        .execute("CREATE INDEX idx_data ON users USING gin (data)")
        .unwrap();

    let q = "SELECT id FROM users WHERE data @> '{\"role\":\"admin\"}'::jsonb";
    let stmt_seq = conn_seq.prepare(q).unwrap();
    let stmt_gin = conn_gin.prepare(q).unwrap();
    g.bench_function(BenchmarkId::new("seq_scan", ""), |b| {
        b.iter(|| stmt_seq.query_collect(&[]).unwrap());
    });
    g.bench_function(BenchmarkId::new("gin_index", ""), |b| {
        b.iter(|| stmt_gin.query_collect(&[]).unwrap());
    });
    g.finish();
}
