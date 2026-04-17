use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use mimalloc::MiMalloc;

use citadel::{Argon2Profile, DatabaseBuilder, SyncMode};
use citadel_sql::{Connection, Value};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn citadel_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("bench.citadel"))
        .passphrase(b"bench-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(4096)
        .sync_mode(SyncMode::Off)
        .create()
        .unwrap()
}

fn citadel_100k(conn: &mut Connection) {
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..100_000i64 {
        conn.execute(&format!(
            "INSERT INTO t (id, name, age) VALUES ({i}, 'user_{i}', {})",
            i % 100
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

fn citadel_join_tables(conn: &mut Connection) {
    conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER, data TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..1_000i64 {
        conn.execute(&format!("INSERT INTO a (id, val) VALUES ({i}, 'a_{i}')"))
            .unwrap();
    }
    for i in 0..1_000i64 {
        conn.execute(&format!(
            "INSERT INTO b (id, a_id, data) VALUES ({i}, {}, 'b_{i}')",
            i % 1_000
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

fn sqlite_db(dir: &std::path::Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(dir.join("bench.db")).unwrap();
    conn.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=8000;")
        .unwrap();
    conn
}

fn sqlite_100k(conn: &rusqlite::Connection) {
    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)",
        [],
    )
    .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    for i in 0..100_000i64 {
        conn.execute(
            "INSERT INTO t (id, name, age) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, format!("user_{i}"), i % 100],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
}

fn sqlite_join_tables(conn: &rusqlite::Connection) {
    conn.execute(
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
        [],
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER, data TEXT)",
        [],
    )
    .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    for i in 0..1_000i64 {
        conn.execute(
            "INSERT INTO a (id, val) VALUES (?1, ?2)",
            rusqlite::params![i, format!("a_{i}")],
        )
        .unwrap();
    }
    for i in 0..1_000i64 {
        conn.execute(
            "INSERT INTO b (id, a_id, data) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, i % 1_000, format!("b_{i}")],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
}

fn sqlite_collect(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare_cached(sql).unwrap();
    let col_count = stmt.column_count();
    stmt.query_map([], |row| {
        let mut vals = Vec::with_capacity(col_count);
        for i in 0..col_count {
            vals.push(row.get::<_, String>(i).unwrap_or_default());
        }
        Ok(vals)
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

fn h2h_count(c: &mut Criterion) {
    let mut g = c.benchmark_group("count");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query("SELECT COUNT(*) FROM t").unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, "SELECT COUNT(*) FROM t"));
    });
    g.finish();
}

fn h2h_point(c: &mut Criterion) {
    let mut g = c.benchmark_group("point");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query("SELECT * FROM t WHERE id = 50000").unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, "SELECT * FROM t WHERE id = 50000"));
    });
    g.finish();
}

fn h2h_scan(c: &mut Criterion) {
    let mut g = c.benchmark_group("scan");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query("SELECT * FROM t").unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, "SELECT * FROM t"));
    });
    g.finish();
}

fn h2h_filter(c: &mut Criterion) {
    let mut g = c.benchmark_group("filter");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query("SELECT * FROM t WHERE age = 42").unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, "SELECT * FROM t WHERE age = 42"));
    });
    g.finish();
}

fn h2h_sort(c: &mut Criterion) {
    let mut g = c.benchmark_group("sort");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query("SELECT * FROM t ORDER BY age LIMIT 10").unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, "SELECT * FROM t ORDER BY age LIMIT 10"));
    });
    g.finish();
}

fn h2h_join(c: &mut Criterion) {
    let mut g = c.benchmark_group("join");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_join_tables(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_join_tables(&sc);

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.query("SELECT a.id, b.data FROM a INNER JOIN b ON a.id = b.a_id")
                .unwrap()
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sqlite_collect(
                &sc,
                "SELECT a.id, b.data FROM a INNER JOIN b ON a.id = b.a_id",
            )
        });
    });
    g.finish();
}

fn h2h_sum(c: &mut Criterion) {
    let mut g = c.benchmark_group("sum");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query("SELECT SUM(age) FROM t").unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, "SELECT SUM(age) FROM t"));
    });
    g.finish();
}

fn h2h_group_by(c: &mut Criterion) {
    let mut g = c.benchmark_group("group_by");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.query("SELECT age, COUNT(*) FROM t GROUP BY age")
                .unwrap()
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, "SELECT age, COUNT(*) FROM t GROUP BY age"));
    });
    g.finish();
}

fn h2h_insert(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    let mut c_offset = 0i64;

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
        [],
    )
    .unwrap();
    let mut s_offset = 0i64;

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..100i64 {
                cc.execute_params(
                    "INSERT INTO t (id, val) VALUES ($1, $2)",
                    &[Value::Integer(c_offset + j), Value::Text("v".into())],
                )
                .unwrap();
            }
            cc.execute("COMMIT").unwrap();
            c_offset += 100;
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..100i64 {
                sc.execute(
                    "INSERT INTO t (id, val) VALUES (?1, 'v')",
                    rusqlite::params![s_offset + j],
                )
                .unwrap();
            }
            sc.execute_batch("COMMIT").unwrap();
            s_offset += 100;
        });
    });
    g.finish();
}

fn h2h_cte(c: &mut Criterion) {
    let mut g = c.benchmark_group("cte");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    let sql = "WITH filtered AS (SELECT id, name, age FROM t WHERE age < 50) \
               SELECT age, COUNT(*) FROM filtered GROUP BY age";

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_recursive_cte(c: &mut Criterion) {
    let mut g = c.benchmark_group("recursive_cte");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());

    let sql = "WITH RECURSIVE seq(x) AS (\
                   SELECT 1 \
                   UNION ALL \
                   SELECT x + 1 FROM seq WHERE x < 1000\
               ) SELECT SUM(x) FROM seq";

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_window_rank(c: &mut Criterion) {
    let mut g = c.benchmark_group("window_rank");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    let sql = "SELECT id, name, age, \
               ROW_NUMBER() OVER (PARTITION BY age ORDER BY id), \
               RANK() OVER (PARTITION BY age ORDER BY name) \
               FROM t";

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_window_agg(c: &mut Criterion) {
    let mut g = c.benchmark_group("window_agg");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    let sql = "SELECT id, age, \
               SUM(age) OVER (ORDER BY id ROWS BETWEEN 50 PRECEDING AND CURRENT ROW), \
               MIN(age) OVER (ORDER BY id ROWS BETWEEN 50 PRECEDING AND CURRENT ROW) \
               FROM t";

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_view_filter(c: &mut Criterion) {
    let mut g = c.benchmark_group("view_filter");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);
    cc.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);
    sc.execute("CREATE VIEW v AS SELECT * FROM t", []).unwrap();

    let sql = "SELECT * FROM v WHERE age = 42";

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_view_point(c: &mut Criterion) {
    let mut g = c.benchmark_group("view_point");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);
    cc.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);
    sc.execute("CREATE VIEW v AS SELECT * FROM t", []).unwrap();

    let sql = "SELECT * FROM v WHERE id = 50000";

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_correlated_exists(c: &mut Criterion) {
    let mut g = c.benchmark_group("correlated_exists");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);
    cc.execute("CREATE TABLE ref_table (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in (0..100_000i64).step_by(100) {
        cc.execute(&format!(
            "INSERT INTO ref_table (id, val) VALUES ({i}, {i})"
        ))
        .unwrap();
    }
    cc.execute("COMMIT").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);
    sc.execute(
        "CREATE TABLE ref_table (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)",
        [],
    )
    .unwrap();
    sc.execute_batch("BEGIN").unwrap();
    for i in (0..100_000i64).step_by(100) {
        sc.execute(
            "INSERT INTO ref_table (id, val) VALUES (?1, ?1)",
            rusqlite::params![i],
        )
        .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let sql =
        "SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM ref_table WHERE ref_table.id = t.id)";

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_update(c: &mut Criterion) {
    let mut g = c.benchmark_group("update");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    let sql = "UPDATE t SET age = age + 1 WHERE id BETWEEN 10000 AND 10099";
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.execute(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sc.execute(sql, []).unwrap());
    });
    g.finish();
}

fn h2h_delete(c: &mut Criterion) {
    let mut g = c.benchmark_group("delete");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    cc.execute("CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sc.execute(
        "CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)",
        [],
    )
    .unwrap();

    let mut c_offset = 0i64;
    let mut s_offset = 0i64;

    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| {
            cc.execute("BEGIN").unwrap();
            for j in 0..100i64 {
                cc.execute(&format!(
                    "INSERT INTO d (id, val) VALUES ({}, {})",
                    c_offset + j,
                    j
                ))
                .unwrap();
            }
            cc.execute("COMMIT").unwrap();
            cc.execute(&format!(
                "DELETE FROM d WHERE id >= {} AND id < {}",
                c_offset,
                c_offset + 100
            ))
            .unwrap();
            c_offset += 100;
        });
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| {
            sc.execute_batch("BEGIN").unwrap();
            for j in 0..100i64 {
                sc.execute(
                    "INSERT INTO d (id, val) VALUES (?1, ?2)",
                    rusqlite::params![s_offset + j, j],
                )
                .unwrap();
            }
            sc.execute_batch("COMMIT").unwrap();
            sc.execute(
                &format!(
                    "DELETE FROM d WHERE id >= {} AND id < {}",
                    s_offset,
                    s_offset + 100
                ),
                [],
            )
            .unwrap();
            s_offset += 100;
        });
    });
    g.finish();
}

fn h2h_distinct(c: &mut Criterion) {
    let mut g = c.benchmark_group("distinct");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);

    let sql = "SELECT DISTINCT age FROM t";
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_union(c: &mut Criterion) {
    let mut g = c.benchmark_group("union");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_join_tables(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_join_tables(&sc);

    let sql = "SELECT id, val FROM a UNION ALL SELECT id, data FROM b";
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_correlated_in(c: &mut Criterion) {
    let mut g = c.benchmark_group("correlated_in");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_100k(&mut cc);
    cc.execute("CREATE TABLE ref_table (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    cc.execute("BEGIN").unwrap();
    for i in (0..100_000i64).step_by(100) {
        cc.execute(&format!(
            "INSERT INTO ref_table (id, val) VALUES ({i}, {i})"
        ))
        .unwrap();
    }
    cc.execute("COMMIT").unwrap();

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_100k(&sc);
    sc.execute(
        "CREATE TABLE ref_table (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)",
        [],
    )
    .unwrap();
    sc.execute_batch("BEGIN").unwrap();
    for i in (0..100_000i64).step_by(100) {
        sc.execute(
            "INSERT INTO ref_table (id, val) VALUES (?1, ?1)",
            rusqlite::params![i],
        )
        .unwrap();
    }
    sc.execute_batch("COMMIT").unwrap();

    let sql =
        "SELECT COUNT(*) FROM t WHERE id IN (SELECT id FROM ref_table WHERE ref_table.val = t.age)";
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_correlated_scalar(c: &mut Criterion) {
    let mut g = c.benchmark_group("correlated_scalar");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_join_tables(&mut cc);

    let sdir = tempfile::tempdir().unwrap();
    let sc = sqlite_db(sdir.path());
    sqlite_join_tables(&sc);

    let sql = "SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.a_id = a.id) AS cnt FROM a";
    g.bench_function(BenchmarkId::new("citadel", ""), |b| {
        b.iter(|| cc.query(sql).unwrap());
    });
    g.bench_function(BenchmarkId::new("sqlite", ""), |b| {
        b.iter(|| sqlite_collect(&sc, sql));
    });
    g.finish();
}

fn h2h_insert_select(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert_select");

    let cdir = tempfile::tempdir().unwrap();
    let cdb = citadel_db(cdir.path());
    let mut cc = Connection::open(&cdb).unwrap();
    citadel_join_tables(&mut cc);
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

criterion_group!(
    benches,
    h2h_count,
    h2h_point,
    h2h_scan,
    h2h_filter,
    h2h_sort,
    h2h_join,
    h2h_sum,
    h2h_group_by,
    h2h_insert,
    h2h_update,
    h2h_delete,
    h2h_distinct,
    h2h_union,
    h2h_cte,
    h2h_recursive_cte,
    h2h_window_rank,
    h2h_window_agg,
    h2h_view_filter,
    h2h_view_point,
    h2h_correlated_exists,
    h2h_correlated_in,
    h2h_correlated_scalar,
    h2h_insert_select,
);
criterion_main!(benches);
