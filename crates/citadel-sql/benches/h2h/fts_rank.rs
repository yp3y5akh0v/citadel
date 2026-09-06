use citadel_sql::Connection;
use criterion::{BatchSize, BenchmarkId, Criterion};

use super::common::*;

const ROWS: i64 = 100_000;
const CITADEL_SQL: &str = "SELECT id, ts_rank(body, to_tsquery('rust & database')) AS r \
    FROM docs WHERE body @@ to_tsquery('rust & database') \
    ORDER BY r DESC LIMIT 10";
const SQLITE_SQL: &str = "SELECT rowid, bm25(docs) AS r FROM docs \
    WHERE docs MATCH 'rust database' ORDER BY r LIMIT 10";

fn assert_ranking(ranks: impl Iterator<Item = f64>, descending: bool) {
    let ranks: Vec<_> = ranks.collect();
    assert_eq!(ranks.len(), 10);
    assert!(ranks.iter().all(|rank| rank.is_finite()));
    assert!(ranks.windows(2).all(|pair| if descending {
        pair[0] >= pair[1]
    } else {
        pair[0] <= pair[1]
    }));
}

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
    ];
    let mut words = Vec::with_capacity(40);
    for i in 0..40 {
        let idx = ((seed.wrapping_mul(31) + i * 17) as usize) % vocab.len();
        words.push(vocab[idx]);
    }
    words.join(" ")
}

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("fts_rank");

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

    let cs = cc.prepare(CITADEL_SQL).unwrap();
    let mut ss = sc.prepare(SQLITE_SQL).unwrap();
    let expected_c = cs.query_collect(&[]).unwrap();
    assert_ranking(
        expected_c.rows.iter().map(|row| {
            let [citadel_sql::Value::Integer(id), citadel_sql::Value::Real(rank)] = row.as_slice()
            else {
                panic!("expected an integer id and real rank");
            };
            assert!((0..ROWS).contains(id));
            *rank
        }),
        true,
    );
    let read_doc = cc.prepare("SELECT body FROM docs WHERE id = $1").unwrap();
    let rank_doc = cc
        .prepare("SELECT ts_rank($1, to_tsquery('rust & database'))")
        .unwrap();
    for row in &expected_c.rows {
        let doc = read_doc.query_collect(&[row[0].clone()]).unwrap();
        assert_eq!(doc.rows.len(), 1);
        let scalar_rank = rank_doc.query_collect(&[doc.rows[0][0].clone()]).unwrap();
        assert_eq!(scalar_rank.rows, vec![vec![row[1].clone()]]);
    }
    assert_eq!(cs.query_collect(&[]).unwrap().rows, expected_c.rows);
    assert_eq!(
        cc.prepare(CITADEL_SQL)
            .unwrap()
            .query_collect(&[])
            .unwrap()
            .rows,
        expected_c.rows
    );
    let expected_s = sqlite_collect_stmt(&mut ss);
    assert_ranking(
        expected_s.iter().map(|row| {
            let [rusqlite::types::Value::Integer(id), rusqlite::types::Value::Real(rank)] =
                row.as_slice()
            else {
                panic!("expected an integer id and real rank");
            };
            assert!((0..ROWS).contains(id));
            *rank
        }),
        false,
    );
    assert_eq!(sqlite_collect_stmt(&mut ss), expected_s);
    assert_eq!(
        sqlite_collect_stmt(&mut sc.prepare(SQLITE_SQL).unwrap()),
        expected_s
    );
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cs.query_collect(&[]).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect_stmt(&mut ss));
    });
    g.finish();

    let mut g = c.benchmark_group("fts_rank_first_execution");
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter_batched_ref(
            || cc.prepare(CITADEL_SQL).unwrap(),
            |stmt| stmt.query_collect(&[]).unwrap(),
            BatchSize::PerIteration,
        );
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter_batched_ref(
            || sc.prepare(SQLITE_SQL).unwrap(),
            sqlite_collect_stmt,
            BatchSize::PerIteration,
        );
    });
    g.finish();
}
