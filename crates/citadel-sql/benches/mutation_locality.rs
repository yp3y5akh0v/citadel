//! Citadel-only controls for repeated point mutations at different key localities.

use std::hint::black_box;
use std::time::Duration;

use citadel::{Argon2Profile, DatabaseBuilder, SyncMode};
use citadel_sql::{Connection, PreparedStatement, Value};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const BATCH: usize = 100;

#[derive(Clone, Copy)]
enum Order {
    Sequential,
    Descending,
    Shuffled,
}

impl Order {
    fn name(self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Descending => "descending",
            Self::Shuffled => "shuffled",
        }
    }

    fn index(self, position: usize) -> usize {
        match self {
            Self::Sequential => position,
            Self::Descending => BATCH - 1 - position,
            // 37 is coprime to 100: this permutes exactly the same key set
            // without a RNG dependency or any timed permutation work.
            Self::Shuffled => (position * 37 + 11) % BATCH,
        }
    }
}

fn run_batch(
    connection: &Connection<'_>,
    update: &PreparedStatement<'_, '_>,
    bindings: &[[Value; 2]],
) -> u64 {
    connection.execute("BEGIN").unwrap();
    let mut affected = 0;
    for params in bindings {
        affected += update.execute(params).unwrap();
    }
    connection.execute("COMMIT").unwrap();
    affected
}

fn check_rows(connection: &Connection<'_>, row_count: usize, batches: i64) {
    let rows = connection
        .query("SELECT id, a, d FROM t ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), row_count);
    let stride = row_count / BATCH;
    for (id, row) in rows.iter().enumerate() {
        let increments = if id % stride == 0 { batches } else { 0 };
        let a = id as i64 + increments;
        assert_eq!(
            row.as_slice(),
            &[
                Value::Integer(id as i64),
                Value::Integer(a),
                Value::Integer(a * 2 + 1),
            ],
            "row {id} after {batches} batches"
        );
    }
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mutation_locality");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(30);
    group.throughput(Throughput::Elements(BATCH as u64));

    for row_count in [100usize, 10_000] {
        for order in [Order::Sequential, Order::Descending, Order::Shuffled] {
            let directory = tempfile::tempdir().unwrap();
            // Match h2h::common::citadel_db, without importing unrelated fixtures.
            let database = DatabaseBuilder::new(directory.path().join("bench.citadel"))
                .passphrase(b"bench-passphrase")
                .argon2_profile(Argon2Profile::Iot)
                .cache_size(4096)
                .sync_mode(SyncMode::Off)
                .create()
                .unwrap();
            let connection = Connection::open(&database).unwrap();
            connection
                .execute(
                    "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, \
                     d INTEGER GENERATED ALWAYS AS (a * 2 + 1) STORED)",
                )
                .unwrap();
            connection.execute("BEGIN").unwrap();
            let insert = connection
                .prepare("INSERT INTO t (id, a) VALUES ($1, $1)")
                .unwrap();
            for id in 0..row_count {
                assert_eq!(insert.execute(&[Value::Integer(id as i64)]).unwrap(), 1);
            }
            connection.execute("COMMIT").unwrap();
            check_rows(&connection, row_count, 0);

            let stride = row_count / BATCH;
            let bindings: Vec<[Value; 2]> = (0..BATCH)
                .map(|position| {
                    [
                        Value::Integer(1),
                        Value::Integer((order.index(position) * stride) as i64),
                    ]
                })
                .collect();
            let mut indices: Vec<_> = (0..BATCH).map(|position| order.index(position)).collect();
            indices.sort_unstable();
            assert_eq!(indices, (0..BATCH).collect::<Vec<_>>());

            let update = connection
                .prepare("UPDATE t SET a = a + $1 WHERE id = $2")
                .unwrap();
            assert_eq!(
                update
                    .execute(&[Value::Integer(1), Value::Integer(row_count as i64)])
                    .unwrap(),
                0
            );
            // Trials validate affected-row counts and generated propagation
            // outside timing. All timed iterations include BEGIN and COMMIT.
            for _ in 0..2 {
                assert_eq!(run_batch(&connection, &update, &bindings), BATCH as u64);
            }
            let mut completed_batches = 2i64;
            check_rows(&connection, row_count, completed_batches);

            group.bench_function(BenchmarkId::new(order.name(), row_count), |b| {
                b.iter(|| {
                    let affected = run_batch(&connection, &update, &bindings);
                    completed_batches += 1;
                    black_box(affected)
                });
            });
            check_rows(&connection, row_count, completed_batches);
        }
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
