use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

const ROWS: i64 = 100_000;

fn make_doc(seed: i64) -> String {
    let vocab = [
        "rust",
        "database",
        "encrypted",
        "index",
        "vector",
        "query",
        "search",
        "fast",
        "secure",
        "embedded",
        "table",
        "column",
        "row",
        "transaction",
        "atomic",
        "commit",
    ];
    let mut words = Vec::with_capacity(40);
    for i in 0..40 {
        let idx = ((seed.wrapping_mul(31) + i * 17) as usize) % vocab.len();
        words.push(vocab[idx]);
    }
    words.join(" ")
}

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("fts_index");

    let dir_seq = tempfile::tempdir().unwrap();
    let db_seq = citadel_db(dir_seq.path());
    let conn_seq = Connection::open(&db_seq).unwrap();
    conn_seq
        .execute("CREATE TABLE docs (id INTEGER NOT NULL PRIMARY KEY, body TEXT)")
        .unwrap();
    conn_seq.execute("BEGIN").unwrap();
    for i in 0..ROWS {
        let body = make_doc(i);
        conn_seq
            .execute(&format!("INSERT INTO docs VALUES ({i}, '{body}')"))
            .unwrap();
    }
    conn_seq.execute("COMMIT").unwrap();

    let dir_fts = tempfile::tempdir().unwrap();
    let db_fts = citadel_db(dir_fts.path());
    let conn_fts = Connection::open(&db_fts).unwrap();
    conn_fts
        .execute("CREATE TABLE docs (id INTEGER NOT NULL PRIMARY KEY, body TEXT)")
        .unwrap();
    conn_fts.execute("BEGIN").unwrap();
    for i in 0..ROWS {
        let body = make_doc(i);
        conn_fts
            .execute(&format!("INSERT INTO docs VALUES ({i}, '{body}')"))
            .unwrap();
    }
    conn_fts.execute("COMMIT").unwrap();
    conn_fts
        .execute("CREATE INDEX idx_body ON docs USING fts (body)")
        .unwrap();

    let q = "SELECT id FROM docs WHERE body @@ to_tsquery('rust & database')";
    let stmt_seq = conn_seq.prepare(q).unwrap();
    let stmt_fts = conn_fts.prepare(q).unwrap();
    g.bench_function(BenchmarkId::new("seq_scan", ""), |b| {
        b.iter(|| stmt_seq.query_collect(&[]).unwrap());
    });
    g.bench_function(BenchmarkId::new("fts_index", ""), |b| {
        b.iter(|| stmt_fts.query_collect(&[]).unwrap());
    });
    g.finish();
}
