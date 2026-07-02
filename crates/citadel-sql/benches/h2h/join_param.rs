use citadel_sql::{Connection, Value};
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("join_param");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    citadel_join_tables(&cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_join_tables(&sc);

    // Rotating pk param defeats the result memo: measures the probe path.
    let sql = "SELECT a.val, b.data FROM a JOIN b ON b.a_id = a.id WHERE a.id = $1";
    let cs = cc.prepare(sql).unwrap();
    let mut ss = sc
        .prepare("SELECT a.val, b.data FROM a JOIN b ON b.a_id = a.id WHERE a.id = ?1")
        .unwrap();
    let mut ci = 0i64;
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            ci = (ci + 1) % 1_000;
            cs.query_collect(&[Value::Integer(ci)]).unwrap()
        });
    });
    let mut si = 0i64;
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            si = (si + 1) % 1_000;
            let mut rows = ss.query(rusqlite::params![si]).unwrap();
            let mut out: Vec<Vec<rusqlite::types::Value>> = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                out.push(vec![row.get(0).unwrap(), row.get(1).unwrap()]);
            }
            out
        });
    });
    g.finish();
}
