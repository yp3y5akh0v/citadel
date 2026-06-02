//! Recall latency: times the full `MemoryEngine::recall` end-to-end (embed + ANN + fusion).

use std::hint::black_box;
use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, MemoryEngine, MockEmbedder, RecallQuery};
use criterion::{criterion_group, criterion_main, Criterion};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DIM: usize = 384;

fn query_for(i: usize, n: usize) -> String {
    format!("memory atom number {} about topic {}", i % n, i % 97)
}

fn seed(n: usize) -> (tempfile::TempDir, MemoryEngine) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"bench-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    // Bulk-load in chunks: one transaction per chunk instead of one per atom.
    const CHUNK: usize = 2000;
    let mut i = 0;
    while i < n {
        let end = (i + CHUNK).min(n);
        let batch = (i..end)
            .map(|j| AtomInput::new("fact", query_for(j, n)))
            .collect();
        eng.remember_batch("r", batch).unwrap();
        i = end;
    }
    (dir, eng)
}

fn bench_recall(c: &mut Criterion) {
    let n: usize = std::env::var("CITADEL_BENCH_ATOMS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);

    let (_dir, eng) = seed(n);
    // Warm the ANN index cache so the build cost is not part of the measurement.
    let _ = eng
        .recall("r", RecallQuery::by_text(query_for(0, n), 10))
        .unwrap();

    let mut group = c.benchmark_group("recall");
    group.bench_function(format!("top10_n{n}"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            let q = query_for(i.wrapping_mul(2_654_435_761), n);
            i = i.wrapping_add(1);
            black_box(eng.recall("r", RecallQuery::by_text(q, 10)).unwrap())
        });
    });
    group.finish();
}

criterion_group!(benches, bench_recall);
criterion_main!(benches);
