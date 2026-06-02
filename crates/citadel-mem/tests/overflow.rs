use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, EvictionPolicy, MemoryEngine, MockEmbedder, RecallQuery};
use std::sync::Arc;

// 512d vectors push each row past the inline limit into an overflow chain.
#[test]
fn large_dim_atoms_overflow_and_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(512)))
        .unwrap();

    let mut ids = Vec::new();
    for i in 0..20 {
        ids.push(
            eng.remember(
                "r",
                AtomInput::new("fact", format!("memory atom number {i}")),
            )
            .unwrap(),
        );
    }

    let hits = eng
        .recall("r", RecallQuery::by_text("memory atom number 3", 5))
        .unwrap();
    assert!(!hits.is_empty(), "recall over overflow rows returns hits");
    assert!(hits.iter().any(|h| h.text.contains("number 3")));

    let report = eng.evolve("r", ids[0], 5, 10.0).unwrap();
    assert!(report.score > 0.0, "evolve updates an overflow row");

    let removed = eng
        .evict("r", EvictionPolicy::Lru { keep_fraction: 0.5 })
        .unwrap();
    assert_eq!(removed.removed, 10);
    assert_eq!(
        eng.recall("r", RecallQuery::by_text("memory atom", 100))
            .unwrap()
            .len(),
        10,
        "half the overflow atoms survive eviction"
    );
}
