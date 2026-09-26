//! Sealed kind-filtered recall must see kinds written after the ANN snapshot.

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{
    AtomInput, MemoryEngine, MockEmbedder, MockReranker, RecallQuery, RerankStrategy,
};

const DIM: usize = 64;

fn engine(dir: &std::path::Path) -> MemoryEngine {
    let db = Arc::new(
        DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .unwrap(),
    );
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    eng
}

/// Enough turns to leave the exact-scan path for the PRISM index path.
fn seed_turns(eng: &MemoryEngine) {
    let turns: Vec<AtomInput> = (0..600)
        .map(|i| {
            AtomInput::new(
                "turn",
                format!("conversation turn number {i} about daily life"),
            )
        })
        .collect();
    eng.remember_batch("r", turns).unwrap();
}

#[test]
fn derived_kind_written_before_any_recall_is_filterable() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    seed_turns(&eng);
    eng.remember_derived(
        "r",
        AtomInput::new("derived", "a consolidated fact about daily life"),
        &[],
        None,
    )
    .unwrap();

    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("daily life", 5).with_kinds(vec!["derived".into()]),
        )
        .unwrap();
    assert!(!hits.is_empty(), "derived atoms must be kind-filterable");
    assert!(hits.iter().all(|h| h.kind == "derived"));
}

#[test]
fn kind_written_after_the_index_snapshot_still_recalls() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    seed_turns(&eng);

    // Build the sealed ANN snapshot while only "turn" atoms exist.
    let warm = eng
        .recall("r", RecallQuery::by_text("daily life", 5))
        .unwrap();
    assert!(!warm.is_empty());

    // The new kind arrives AFTER the snapshot (the live enrichment shape).
    eng.remember_derived(
        "r",
        AtomInput::new("derived", "a consolidated fact about daily life"),
        &[],
        None,
    )
    .unwrap();

    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("daily life", 5).with_kinds(vec!["derived".into()]),
        )
        .unwrap();
    assert!(
        !hits.is_empty(),
        "post-snapshot kinds must reach the tail scan, not early-return empty"
    );
    assert!(hits.iter().all(|h| h.kind == "derived"));
}

#[test]
fn turn_filtered_recall_first_does_not_poison_later_derived_filter() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    seed_turns(&eng);
    eng.remember_derived(
        "r",
        AtomInput::new("derived", "a consolidated fact about daily life"),
        &[],
        None,
    )
    .unwrap();
    eng.set_reranker(Arc::new(MockReranker), RerankStrategy::Rrf { k: 20.0 });

    // Live-run order: the first (index-building) recall is turn-filtered.
    let turns = eng
        .recall(
            "r",
            RecallQuery::by_text("daily life", 5).with_kinds(vec!["turn".into()]),
        )
        .unwrap();
    assert!(!turns.is_empty());

    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("daily life", 5).with_kinds(vec!["derived".into()]),
        )
        .unwrap();
    assert!(
        !hits.is_empty(),
        "derived filter empty after turn-filtered warm"
    );
    assert!(hits.iter().all(|h| h.kind == "derived"));
}
