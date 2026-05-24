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
        "rollback",
        "memory",
        "disk",
        "page",
        "buffer",
        "cache",
        "lock",
        "concurrent",
        "thread",
        "process",
        "fork",
        "join",
        "merge",
        "split",
        "tree",
        "graph",
        "list",
        "map",
        "set",
        "hash",
        "btree",
        "log",
        "wal",
        "snapshot",
        "replica",
        "shard",
        "cluster",
        "node",
        "edge",
        "leaf",
        "root",
        "branch",
        "depth",
        "height",
        "width",
        "size",
        "length",
        "count",
        "average",
        "median",
        "min",
        "max",
        "sum",
        "delta",
        "epsilon",
        "gamma",
        "alpha",
        "beta",
        "zeta",
        "omega",
        "lambda",
        "mu",
        "nu",
        "phi",
        "psi",
        "chi",
        "tau",
        "rho",
        "sigma",
        "kappa",
        "iota",
        "theta",
        "eta",
        "pi",
    ];
    let mut words = Vec::with_capacity(80);
    for i in 0..80 {
        let idx = ((seed.wrapping_mul(31) + i * 17) as usize) % vocab.len();
        words.push(vocab[idx]);
    }
    words.join(" ")
}

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("fts_match");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE docs (id INTEGER NOT NULL PRIMARY KEY, body TEXT)")
        .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in 0..ROWS {
        let body = make_doc(i).replace('\'', "''");
        cc.execute(&format!("INSERT INTO docs VALUES ({i}, '{body}')"))
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
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('rust & database')")
        .unwrap();
    let mut ss = sc
        .prepare("SELECT rowid FROM docs WHERE docs MATCH 'rust database'")
        .unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cs.query_collect(&[]).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect_stmt(&mut ss));
    });
    g.finish();
}
