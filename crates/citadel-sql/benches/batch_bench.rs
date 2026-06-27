#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::cell::Cell;
use std::fmt::Write as _;

use citadel::{Argon2Profile, DatabaseBuilder, SyncMode};
use citadel_sql::Connection;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

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

criterion_group!(benches, bench);
criterion_main!(benches);
