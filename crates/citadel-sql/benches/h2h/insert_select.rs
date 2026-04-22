use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert_select");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    citadel_join_tables(&cc);
    let mut c_run = 0i64;

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_join_tables(&sc);
    let mut s_run = 0i64;

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            let tbl = format!("sink_{c_run}");
            cc.execute(&format!(
                "CREATE TABLE {tbl} (id INTEGER NOT NULL PRIMARY KEY, val TEXT)"
            ))
            .unwrap();
            cc.execute(&format!("INSERT INTO {tbl} SELECT id, val FROM a"))
                .unwrap();
            c_run += 1;
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            let tbl = format!("sink_{s_run}");
            sc.execute(
                &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL PRIMARY KEY, val TEXT)"),
                [],
            )
            .unwrap();
            sc.execute(&format!("INSERT INTO {tbl} SELECT id, val FROM a"), [])
                .unwrap();
            s_run += 1;
        });
    });
    g.finish();
}
