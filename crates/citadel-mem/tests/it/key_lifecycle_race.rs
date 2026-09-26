//! Cross-handle key-lifecycle serialization: open-time reconcile on one engine
//! handle must never reclaim keys that another handle's in-flight sealed writes
//! just allocated (allocate-key -> commit-row spans are atomic vs reconcile).

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, MemoryEngine, MockEmbedder};
use std::sync::Arc;

const DIM: usize = 16;

#[test]
fn concurrent_reconcile_never_reclaims_inflight_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"pw")
            .enable_region_keys(true)
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let writer = MemoryEngine::open(Arc::clone(&db)).unwrap();
    writer
        .create_encrypted_region("r", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();

    const BATCHES: usize = 30;
    const PER_BATCH: usize = 5;
    let writer = Arc::new(writer);
    let w = Arc::clone(&writer);
    let handle = std::thread::spawn(move || {
        for b in 0..BATCHES {
            // Single-writer design: a probe open's bootstrap txn makes writes
            // fail transiently with WriteTransactionActive; callers retry.
            loop {
                let atoms: Vec<AtomInput> = (0..PER_BATCH)
                    .map(|i| AtomInput::new("turn", format!("note {b}-{i}")))
                    .collect();
                match w.remember_batch("r", atoms) {
                    Ok(_) => break,
                    Err(e)
                        if e.to_string()
                            .contains("write transaction is already active") =>
                    {
                        std::thread::yield_now();
                    }
                    Err(e) => panic!("writer failed: {e}"),
                }
            }
        }
    });
    // Race: every successful open runs the reconcilers over the shared
    // database. An open colliding with the writer's txn fails cleanly
    // (single-writer design); the invariant under test is that successful
    // reconciles never corrupt in-flight spans.
    let mut reconciles = 0;
    while reconciles < 10 {
        if MemoryEngine::open(Arc::clone(&db)).is_ok() {
            reconciles += 1;
        }
        std::thread::yield_now();
    }
    handle.join().unwrap();

    // Every written atom must still be fetchable: a lost key would surface as a
    // missing/undecryptable atom (or as reconcile-completed deletion of its
    // row).
    let total = writer.count("r", "turn").unwrap();
    assert_eq!(
        total,
        (BATCHES * PER_BATCH) as u64,
        "no in-flight write lost to a concurrent reconcile"
    );
}
