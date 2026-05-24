use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

const ROWS: i64 = 100_000;

fn make_doc(seed: i64) -> String {
    let mut words: Vec<&str> = Vec::with_capacity(50);
    let filler = "alpha";
    let target = (seed as usize) % 50;
    for i in 0..50 {
        if i == target {
            words.push("rust");
        } else if i == target + 1 && i < 50 {
            words.push("database");
        } else {
            words.push(filler);
        }
    }
    words.join(" ")
}

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("fts_phrase");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE docs (id INTEGER NOT NULL PRIMARY KEY, body TSVECTOR)")
        .unwrap();
    let ins = cc
        .prepare("INSERT INTO docs VALUES ($1, to_tsvector($2))")
        .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in 0..ROWS {
        ins.query_collect(&[
            citadel_sql::Value::Integer(i),
            citadel_sql::Value::Text(make_doc(i).into()),
        ])
        .unwrap();
    }
    cc.execute("COMMIT").unwrap();
    cc.execute("CREATE INDEX idx_body ON docs USING fts (body)")
        .unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(body, tokenize='porter')")
        .unwrap();
    sc.execute_batch("BEGIN").unwrap();
    for i in 0..ROWS {
        let body = make_doc(i);
        sc.execute(
            "INSERT INTO docs (rowid, body) VALUES (?1, ?2)",
            rusqlite::params![i, body],
        )
        .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let cs = cc
        .prepare("SELECT id FROM docs WHERE body @@ phraseto_tsquery('rust database')")
        .unwrap();
    let mut ss = sc
        .prepare("SELECT rowid FROM docs WHERE docs MATCH '\"rust database\"'")
        .unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cs.query_collect(&[]).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect_stmt(&mut ss));
    });
    g.finish();
}
