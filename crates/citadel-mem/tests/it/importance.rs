//! set_importance: settable post-insert, reorders recall, invalidates the index.

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, MemoryEngine, MockEmbedder, RecallProfile, RecallQuery};

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

/// Two atoms identical in every fusion signal except importance.
fn twin_atoms(eng: &MemoryEngine) -> (i64, i64) {
    let t = 1_700_000_000_000_000i64;
    let a = eng
        .remember(
            "r",
            AtomInput::new("turn", "the cited fact about the garden").with_created_at(t),
        )
        .unwrap();
    let b = eng
        .remember(
            "r",
            AtomInput::new("turn", "the cited fact about the garden").with_created_at(t),
        )
        .unwrap();
    (a, b)
}

#[test]
fn importance_reorders_recall_and_invalidates_the_cached_index() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let (a, b) = twin_atoms(&eng);

    // Warm the index BEFORE the update: the reorder must come from invalidation.
    let warm = eng
        .recall(
            "r",
            RecallProfile::default().apply(RecallQuery::by_text("cited fact garden", 2)),
        )
        .unwrap();
    assert_eq!(warm.len(), 2);

    // The later twin gains the citation weight; every other signal is equal.
    let updated = eng.set_importance("r", &[(b, 3.0)]).unwrap();
    assert_eq!(updated, 1);

    let hits = eng
        .recall(
            "r",
            RecallProfile::default().apply(RecallQuery::by_text("cited fact garden", 2)),
        )
        .unwrap();
    assert_eq!(
        hits.iter().map(|h| h.id).collect::<Vec<_>>(),
        vec![b, a],
        "importance breaks the tie through the default profile weight"
    );
}

#[test]
fn importance_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = {
        let eng = engine(dir.path());
        let pair = twin_atoms(&eng);
        eng.set_importance("r", &[(pair.1, 3.0)]).unwrap();
        pair
    };

    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .open()
            .unwrap(),
    );
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let hits = eng
        .recall(
            "r",
            RecallProfile::default().apply(RecallQuery::by_text("cited fact garden", 2)),
        )
        .unwrap();
    assert_eq!(hits.iter().map(|h| h.id).collect::<Vec<_>>(), vec![b, a]);
}

#[test]
fn missing_ids_are_skipped_with_an_honest_count() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let (a, _b) = twin_atoms(&eng);
    let updated = eng
        .set_importance("r", &[(a, 1.0), (999_999, 5.0)])
        .unwrap();
    assert_eq!(updated, 1, "nonexistent id must not inflate the count");
    assert_eq!(eng.set_importance("r", &[]).unwrap(), 0);
}

#[test]
fn same_value_rewrite_is_a_counted_noop() {
    // Identical values must be skipped (count 0) so a converged pass cannot churn.
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let (a, b) = twin_atoms(&eng);
    assert_eq!(eng.set_importance("r", &[(a, 2.0), (b, 3.0)]).unwrap(), 2);
    assert_eq!(
        eng.set_importance("r", &[(a, 2.0), (b, 3.0)]).unwrap(),
        0,
        "identical values must not count as writes"
    );
    // A mixed batch counts only the value that changed.
    assert_eq!(eng.set_importance("r", &[(a, 2.0), (b, 4.0)]).unwrap(), 1);
}
