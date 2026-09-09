#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::cell::Cell;
use std::fmt::Write as _;
use std::time::Duration;

use citadel::{Argon2Profile, DatabaseBuilder, SyncMode};
use citadel_sql::{Connection, Value};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

fn durable_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("batch.citadel"))
        .passphrase(b"bench-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(4096)
        .sync_mode(SyncMode::Full)
        .create()
        .unwrap()
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("durable_batch_insert");
    g.sample_size(10);

    for &k in &[20u64, 100] {
        g.throughput(Throughput::Elements(k));

        g.bench_with_input(BenchmarkId::new("autocommit", k), &k, |b, &k| {
            let dir = tempfile::tempdir().unwrap();
            let db = durable_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
                .unwrap();
            let off = Cell::new(0u64);
            b.iter(|| {
                let base = off.get();
                for i in 0..k {
                    conn.execute(&format!("INSERT INTO t VALUES ({}, {})", base + i, i))
                        .unwrap();
                }
                off.set(base + k);
            });
        });

        g.bench_with_input(BenchmarkId::new("execute_batch", k), &k, |b, &k| {
            let dir = tempfile::tempdir().unwrap();
            let db = durable_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
                .unwrap();
            let off = Cell::new(0u64);
            b.iter(|| {
                let base = off.get();
                let mut sql = String::with_capacity(k as usize * 32);
                for i in 0..k {
                    let _ = write!(sql, "INSERT INTO t VALUES ({}, {});", base + i, i);
                }
                conn.execute_batch(&sql).unwrap();
                off.set(base + k);
            });
        });
    }

    g.finish();
}

fn bench_create_index(c: &mut Criterion) {
    const ROWS: i64 = 20_000;
    let mut g = c.benchmark_group("create_index");
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));
    g.throughput(Throughput::Elements(ROWS as u64));

    for (name, extra_columns) in [
        ("narrow", ""),
        ("wide", ", payload TEXT NOT NULL"),
        (
            "unused_virtual",
            ", payload TEXT NOT NULL, normalized TEXT GENERATED ALWAYS AS (LOWER(payload)) VIRTUAL",
        ),
    ] {
        g.bench_function(BenchmarkId::new(name, ROWS), |b| {
            let dir = tempfile::tempdir().unwrap();
            let db = DatabaseBuilder::new(dir.path().join("create-index.citadel"))
                .passphrase(b"bench-passphrase")
                .argon2_profile(Argon2Profile::Iot)
                .cache_size(4096)
                .sync_mode(SyncMode::Off)
                .create()
                .unwrap();
            let conn = Connection::open(&db).unwrap();
            conn.execute(&format!(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER NOT NULL{extra_columns})"
            ))
            .unwrap();

            let has_payload = !extra_columns.is_empty();
            let insert = conn
                .prepare(if has_payload {
                    "INSERT INTO t (id, k, payload) VALUES ($1, $2, $3)"
                } else {
                    "INSERT INTO t (id, k) VALUES ($1, $2)"
                })
                .unwrap();
            // The wide cases store the same 512-byte unrelated text per row.
            let payload = "AbCdEf0123456789".repeat(32);
            conn.execute("BEGIN").unwrap();
            for id in 0..ROWS {
                // A deterministic permutation gives every case the same index keys.
                let key = (id * 73) % ROWS;
                if has_payload {
                    insert
                        .execute(&[
                            Value::Integer(id),
                            Value::Integer(key),
                            Value::Text(payload.clone().into()),
                        ])
                        .unwrap();
                } else {
                    insert
                        .execute(&[Value::Integer(id), Value::Integer(key)])
                        .unwrap();
                }
            }
            conn.execute("COMMIT").unwrap();

            // Rebuild over unchanged rows: population, DROP INDEX, and temp-file
            // cleanup stay outside the timed CREATE INDEX operation.
            b.iter_batched(
                || {
                    conn.execute("DROP INDEX IF EXISTS t_k").unwrap();
                },
                |()| conn.execute("CREATE INDEX t_k ON t (k)").unwrap(),
                BatchSize::PerIteration,
            );

            assert_eq!(
                conn.query("SELECT COUNT(*) FROM t WHERE k BETWEEN 100 AND 199")
                    .unwrap()
                    .rows,
                vec![vec![Value::Integer(100)]]
            );
        });
    }
    g.finish();
}

criterion_group!(benches, bench, bench_create_index);
criterion_main!(benches);
