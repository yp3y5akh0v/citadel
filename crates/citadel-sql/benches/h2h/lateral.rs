use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("lateral");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    setup_citadel(&cc);

    let sql = "SELECT c.id, p.name FROM c, LATERAL (SELECT name FROM p WHERE p.cat_id = c.id ORDER BY price DESC LIMIT 1) p";
    let cs = cc.prepare(sql).unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cs.query_collect(&[]).unwrap());
    });
    g.finish();
}

fn setup_citadel(conn: &Connection) {
    conn.execute("CREATE TABLE c (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE p (id INTEGER NOT NULL PRIMARY KEY, cat_id INTEGER NOT NULL, name TEXT, price INTEGER)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..100i64 {
        conn.execute(&format!("INSERT INTO c (id, name) VALUES ({i}, 'cat_{i}')"))
            .unwrap();
    }
    for i in 0..10_000i64 {
        let cat = i % 100;
        conn.execute(&format!(
            "INSERT INTO p (id, cat_id, name, price) VALUES ({i}, {cat}, 'p_{i}', {})",
            i % 1000
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}
