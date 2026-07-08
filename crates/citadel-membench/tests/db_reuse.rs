//! Persist-and-reuse: a reopened bench DB re-attaches its regions and recalls
//! from stored vectors with no re-ingest. Token-free, encrypted (real config).

use std::sync::Arc;

use citadel_mem::{AtomInput, Embedder, MemoryEngine, MockEmbedder, RecallProfile, RecallQuery};
use citadel_membench::open_bench_db;

const DIM: usize = 64;
const REGION: &str = "q_reuse";

fn mock() -> Arc<dyn Embedder> {
    Arc::new(MockEmbedder::new(DIM))
}

fn ingest_three(eng: &MemoryEngine) {
    eng.create_encrypted_region(REGION, mock()).unwrap();
    let atoms = [
        "My dog Rex is a golden retriever.",
        "The weather was nice today.",
        "I also have a cat named Mia.",
    ]
    .into_iter()
    .map(|t| AtomInput::new("turn", t.to_string()))
    .collect();
    eng.remember_batch(REGION, atoms).unwrap();
}

fn recall_ids(eng: &MemoryEngine) -> Vec<i64> {
    eng.recall(
        REGION,
        RecallProfile::default().apply(RecallQuery::by_text("what pet did I mention?", 10)),
    )
    .unwrap()
    .iter()
    .map(|h| h.id)
    .collect()
}

#[test]
fn reopened_bench_db_recalls_without_reingest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reuse.cdl");
    // Unique env key so this test does not clash on the shared process env.
    let key = "CITADEL_TEST_DB_REUSE_PATH";
    std::env::set_var(key, &path);

    // Build: create + ingest three atoms, capture the ranking, release the
    // file.
    let baseline = {
        let bench = open_bench_db(key, true).unwrap();
        assert!(!bench.reuse, "a missing path must create, not reuse");
        let eng = MemoryEngine::open(Arc::clone(&bench.db)).unwrap();
        ingest_three(&eng);
        recall_ids(&eng)
    };
    assert!(!baseline.is_empty(), "atoms recalled after ingest");

    // Reuse: reopen the same path, re-attach the region only (no
    // remember_batch).
    let bench = open_bench_db(key, true).unwrap();
    assert!(bench.reuse, "an existing path must reopen + reuse");
    let eng = MemoryEngine::open(Arc::clone(&bench.db)).unwrap();
    eng.create_encrypted_region(REGION, mock()).unwrap();
    let stored = eng.fetch(REGION, "turn", None, 100).unwrap();
    let reused = recall_ids(&eng);

    std::env::remove_var(key);
    assert_eq!(
        stored.len(),
        3,
        "three atoms persisted; reuse did not re-ingest"
    );
    assert_eq!(
        reused, baseline,
        "reopened DB recalls identically from stored vectors, no re-embed"
    );
}
