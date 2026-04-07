use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use citadel::{Argon2Profile, DatabaseBuilder};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("bench.citadel"))
        .passphrase(b"bench-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(4096)
        .create()
        .unwrap()
}

fn populate(db: &citadel::Database, count: u64) {
    let mut wtx = db.begin_write().unwrap();
    for i in 0..count {
        let key = i.to_be_bytes();
        let value = [0u8; 128];
        wtx.insert(&key, &value).unwrap();
    }
    wtx.commit().unwrap();
}

fn bench_point_read(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    populate(&db, 100_000);

    let mut group = c.benchmark_group("kv_read");

    group.bench_function("point_read_warm", |b| {
        let key = 50_000u64.to_be_bytes();
        b.iter(|| {
            let mut rtx = db.begin_read();
            rtx.get(&key).unwrap();
        });
    });

    group.bench_function("point_read_miss", |b| {
        let key = u64::MAX.to_be_bytes();
        b.iter(|| {
            let mut rtx = db.begin_read();
            rtx.get(&key).unwrap();
        });
    });

    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    populate(&db, 100_000);

    let mut group = c.benchmark_group("kv_scan");
    group.throughput(Throughput::Elements(100_000));

    group.bench_function("full_scan_100k", |b| {
        b.iter(|| {
            let mut rtx = db.begin_read();
            let mut count = 0u64;
            rtx.for_each(|_k, _v| {
                count += 1;
                Ok(())
            })
            .unwrap();
            assert_eq!(count, 100_000);
        });
    });

    group.finish();
}

fn bench_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_write");

    group.bench_function("insert_1_commit", |b| {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let mut i = 0u64;
        b.iter(|| {
            let mut wtx = db.begin_write().unwrap();
            wtx.insert(&i.to_be_bytes(), &[0u8; 128]).unwrap();
            wtx.commit().unwrap();
            i += 1;
        });
    });

    for &batch_size in &[100u64, 1000] {
        group.throughput(Throughput::Elements(batch_size));
        group.bench_with_input(
            BenchmarkId::new("batch_commit", batch_size),
            &batch_size,
            |b, &size| {
                let dir = tempfile::tempdir().unwrap();
                let db = create_db(dir.path());
                let mut offset = 0u64;
                b.iter(|| {
                    let mut wtx = db.begin_write().unwrap();
                    for j in 0..size {
                        wtx.insert(&(offset + j).to_be_bytes(), &[0u8; 128])
                            .unwrap();
                    }
                    wtx.commit().unwrap();
                    offset += size;
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_point_read, bench_scan, bench_write);
criterion_main!(benches);
