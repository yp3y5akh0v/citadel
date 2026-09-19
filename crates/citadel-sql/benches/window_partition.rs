use std::hint::black_box;
use std::time::Duration;

use citadel::{Argon2Profile, DatabaseBuilder, SyncMode};
use citadel_sql::{Connection, Value};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const QUERY: &str = "SELECT id, payload, SUM(v) OVER (), COUNT(*) OVER (), \
                    AVG(v) OVER (), MIN(v) OVER (), MAX(v) OVER () \
                    FROM t ORDER BY id";

fn sqlite_collect(statement: &mut rusqlite::Statement<'_>) -> Vec<Vec<rusqlite::types::Value>> {
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            (0..columns)
                .map(|column| row.get::<_, rusqlite::types::Value>(column))
                .collect()
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn verify(rows: &[Vec<Value>], count: i64, payload: &str) {
    assert_eq!(rows.len(), count as usize);
    let sum: i64 = (0..count).map(|id| id % 97 - 48).sum();
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(
            row.as_slice(),
            &[
                Value::Integer(id as i64),
                Value::Text(payload.into()),
                Value::Integer(sum),
                Value::Integer(count),
                Value::Real(sum as f64 / count as f64),
                Value::Integer(-48),
                Value::Integer(48),
            ]
        );
    }
}

fn bench(c: &mut Criterion) {
    let payload = "p".repeat(512);
    for count in [1000i64, 3000] {
        let citadel_dir = tempfile::tempdir().unwrap();
        let database = DatabaseBuilder::new(citadel_dir.path().join("bench.citadel"))
            .passphrase(b"bench-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .cache_size(4096)
            .sync_mode(SyncMode::Off)
            .create()
            .unwrap();
        let connection = Connection::open(&database).unwrap();
        let sqlite_dir = tempfile::tempdir().unwrap();
        let sqlite = rusqlite::Connection::open(sqlite_dir.path().join("bench.db")).unwrap();
        sqlite
            .execute_batch(
                "PRAGMA page_size=8192; PRAGMA journal_mode=MEMORY; \
                 PRAGMA synchronous=OFF; PRAGMA cache_size=4096;",
            )
            .unwrap();
        let create = "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, payload TEXT, v INTEGER)";
        connection.execute(create).unwrap();
        sqlite.execute_batch(create).unwrap();
        connection.execute("BEGIN").unwrap();
        sqlite.execute_batch("BEGIN").unwrap();
        let insert = connection
            .prepare("INSERT INTO t VALUES ($1, $2, $3)")
            .unwrap();
        let mut sqlite_insert = sqlite.prepare("INSERT INTO t VALUES (?1, ?2, ?3)").unwrap();
        for id in 0..count {
            insert
                .execute(&[
                    Value::Integer(id),
                    Value::Text(payload.as_str().into()),
                    Value::Integer(id % 97 - 48),
                ])
                .unwrap();
            sqlite_insert
                .execute(rusqlite::params![id, payload, id % 97 - 48])
                .unwrap();
        }
        connection.execute("COMMIT").unwrap();
        sqlite.execute_batch("COMMIT").unwrap();
        let query = connection.prepare(QUERY).unwrap();
        let mut sqlite_query = sqlite.prepare(QUERY).unwrap();

        // Even the smaller result has 500 KiB of owned text, above the
        // current 128 KiB result-cache cap. Confirm both trials scan again.
        for _ in 0..2 {
            let measurement = database.measure_scans();
            let result = query.query_collect(&[]).unwrap();
            assert!(measurement.rows_scanned() >= count as u64);
            verify(&result.rows, count, &payload);
        }
        let sqlite_rows: Vec<Vec<Value>> = sqlite_collect(&mut sqlite_query)
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|value| match value {
                        rusqlite::types::Value::Null => Value::Null,
                        rusqlite::types::Value::Integer(value) => Value::Integer(value),
                        rusqlite::types::Value::Real(value) => Value::Real(value),
                        rusqlite::types::Value::Text(value) => Value::Text(value.into()),
                        rusqlite::types::Value::Blob(value) => Value::Blob(value),
                    })
                    .collect()
            })
            .collect();
        verify(&sqlite_rows, count, &payload);

        let mut group = c.benchmark_group(format!("window_partition_{count}"));
        group.warm_up_time(Duration::from_secs(1));
        group.measurement_time(Duration::from_secs(2));
        group.sample_size(30);
        group.bench_function(BenchmarkId::new("citadel", ""), |b| {
            b.iter(|| black_box(query.query_collect(&[]).unwrap()));
        });
        group.bench_function(BenchmarkId::new("sqlite", ""), |b| {
            b.iter(|| black_box(sqlite_collect(&mut sqlite_query)));
        });
        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
