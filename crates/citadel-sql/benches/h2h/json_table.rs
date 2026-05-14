use citadel_sql::Connection;
use criterion::{BenchmarkId, Criterion};

use super::common::*;

pub fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("json_table");

    let dir = tempfile::tempdir().unwrap();
    let db = citadel_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    const N: usize = 10_000;
    let mut items = Vec::with_capacity(N);
    for i in 0..N {
        items.push(format!(r#"{{"a":{i},"b":"row{i}","c":{}}}"#, i * 7));
    }
    let array_lit = format!("[{}]", items.join(","));
    let stmt_sql = format!(
        "SELECT jt.a, jt.b, jt.c FROM JSON_TABLE(\
            CAST('{}' AS JSONB), \
            '$[*]' COLUMNS (\
                a INT PATH '$.a', \
                b TEXT PATH '$.b', \
                c INT PATH '$.c'\
            )\
         ) AS jt",
        array_lit.replace('\'', "''")
    );
    let stmt = conn.prepare(&stmt_sql).unwrap();
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| stmt.query_collect(&[]).unwrap());
    });
    g.finish();
}
