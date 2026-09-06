use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert_select");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let cc = Connection::open(&cdb).unwrap();
    citadel_join_tables(&cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_join_tables(&sc);

    let mut c_operation = || {
        cc.execute("CREATE TABLE sink (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
            .unwrap();
        cc.execute("INSERT INTO sink SELECT id, val FROM a")
            .unwrap();
    };
    let mut c_cleanup = || {
        cc.execute("DROP TABLE sink").unwrap();
    };
    let mut s_operation = || {
        sc.execute(
            "CREATE TABLE sink (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
            [],
        )
        .unwrap();
        sc.execute("INSERT INTO sink SELECT id, val FROM a", [])
            .unwrap();
    };
    let mut s_cleanup = || {
        sc.execute("DROP TABLE sink", []).unwrap();
    };

    let expected_c = cc.query("SELECT id, val FROM a ORDER BY id").unwrap().rows;
    let expected_s =
        sqlite_collect_stmt(&mut sc.prepare("SELECT id, val FROM a ORDER BY id").unwrap());
    assert_eq!(expected_c.len(), 1_000);
    assert_eq!(expected_s.len(), 1_000);
    for _ in 0..2 {
        c_operation();
        s_operation();
        assert_eq!(
            cc.query("SELECT id, val FROM sink ORDER BY id")
                .unwrap()
                .rows,
            expected_c
        );
        assert_eq!(
            sqlite_collect_stmt(&mut sc.prepare("SELECT id, val FROM sink ORDER BY id").unwrap()),
            expected_s
        );
        c_cleanup();
        s_cleanup();
    }

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        iter_with_cleanup(b, &mut c_operation, &mut c_cleanup);
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        iter_with_cleanup(b, &mut s_operation, &mut s_cleanup);
    });
    g.finish();
}
