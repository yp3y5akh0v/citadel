//! Cross-engine erasure visibility: the cache epoch invalidates every handle's cache.

use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{AtomId, AtomInput, MemoryEngine, MockEmbedder, RecallQuery};

const DIM: usize = 64;

fn build_db(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(dir.join("m.cdl"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .unwrap(),
    )
}

fn engine(db: &Arc<Database>) -> MemoryEngine {
    let eng = MemoryEngine::open(Arc::clone(db)).unwrap();
    eng.create_encrypted_region("r", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    eng
}

fn recall_ids(eng: &MemoryEngine, text: &str) -> Vec<AtomId> {
    eng.recall("r", RecallQuery::by_text(text, 10))
        .unwrap()
        .into_iter()
        .map(|h| h.id)
        .collect()
}

#[test]
fn second_engine_cache_drops_atoms_forgotten_elsewhere() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path());
    let writer = engine(&db);
    let reader = engine(&db);
    let secret = writer
        .remember("r", AtomInput::new("fact", "the secret trip"))
        .unwrap();
    let keep = writer
        .remember("r", AtomInput::new("fact", "the harmless note"))
        .unwrap();

    // Warm the reader's decrypted cache, then erase through the writer.
    assert!(recall_ids(&reader, "the secret trip").contains(&secret));
    writer.forget_atoms("r", &[secret], false).unwrap();

    let after = recall_ids(&reader, "the secret trip");
    assert!(
        !after.contains(&secret),
        "warmed cache served an erased atom: {after:?}"
    );
    assert!(recall_ids(&reader, "the harmless note").contains(&keep));
}

#[test]
fn second_engine_cache_drops_cascade_and_delete_targets() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path());
    let writer = engine(&db);
    let reader = engine(&db);
    let turn = writer
        .remember("r", AtomInput::new("turn", "the source turn"))
        .unwrap();
    let fact = writer
        .remember_derived(
            "r",
            AtomInput::new("derived", "fact from the turn"),
            &[turn],
            None,
        )
        .unwrap();
    let other = writer
        .remember("r", AtomInput::new("turn", "unrelated matter"))
        .unwrap();

    assert!(recall_ids(&reader, "fact from the turn").contains(&fact));
    writer
        .forget_atoms_with_dependents("r", &[turn], false)
        .unwrap();
    let after = recall_ids(&reader, "fact from the turn");
    assert!(
        !after.contains(&turn) && !after.contains(&fact),
        "warmed cache survived the cascade: {after:?}"
    );

    assert!(recall_ids(&reader, "unrelated matter").contains(&other));
    writer.delete_atoms("r", &[other]).unwrap();
    assert!(!recall_ids(&reader, "unrelated matter").contains(&other));
}

#[test]
fn second_engine_segment_backed_cache_drops_forgotten_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path());
    let writer = engine(&db);
    let reader = engine(&db);
    let secret = writer
        .remember("r", AtomInput::new("fact", "sealed secret"))
        .unwrap();
    let filler = writer
        .remember("r", AtomInput::new("fact", "sealed filler"))
        .unwrap();
    writer.persist_ann_index("r").unwrap();

    // The reader warms from the persisted segment, then the writer erases.
    assert!(recall_ids(&reader, "sealed secret").contains(&secret));
    writer.forget_atoms("r", &[secret], false).unwrap();

    let after = recall_ids(&reader, "sealed secret");
    assert!(
        !after.contains(&secret),
        "segment-backed cache served an erased atom: {after:?}"
    );
    assert!(
        after.contains(&filler),
        "the rebuild must still serve the surviving atom"
    );
}

#[test]
fn second_engine_cache_sees_atoms_inserted_elsewhere() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path());
    let writer = engine(&db);
    let reader = engine(&db);
    let first = writer
        .remember("r", AtomInput::new("fact", "first entry"))
        .unwrap();
    assert!(recall_ids(&reader, "first entry").contains(&first));

    let second = writer
        .remember("r", AtomInput::new("fact", "second entry"))
        .unwrap();
    assert!(
        recall_ids(&reader, "second entry").contains(&second),
        "warmed cache must rebuild to include another handle's insert"
    );
}

#[test]
fn cache_status_reports_only_current_generation_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path());
    let writer = engine(&db);
    let reader = engine(&db);
    let atom = writer
        .remember("r", AtomInput::new("fact", "status probe"))
        .unwrap();

    assert!(recall_ids(&reader, "status probe").contains(&atom));
    assert!(reader.ann_cache_status("r").unwrap().is_some());
    let (_, current) = reader.ann_cache_status_current("r").unwrap().unwrap();
    assert!(current);

    writer.forget_atoms("r", &[atom], false).unwrap();
    assert!(
        reader.ann_cache_status("r").unwrap().is_none(),
        "a stale entry must not report as the serving index"
    );
    let (_, current) = reader.ann_cache_status_current("r").unwrap().unwrap();
    assert!(!current, "the detailed status must expose the staleness");
}

#[test]
fn second_engine_cache_sees_a_rewritten_payload() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_db(dir.path());
    let writer = engine(&db);
    let reader = engine(&db);
    let atom = writer
        .remember(
            "r",
            AtomInput::new("fact", "mutable note").with_payload(serde_json::json!({"v": 1})),
        )
        .unwrap();

    let payload_of = |eng: &MemoryEngine| {
        eng.recall("r", RecallQuery::by_text("mutable note", 10))
            .unwrap()
            .into_iter()
            .find(|h| h.id == atom)
            .unwrap()
            .payload
    };
    assert_eq!(payload_of(&reader), serde_json::json!({"v": 1}));

    writer
        .update_atom_payload("r", atom, &serde_json::json!({"v": 2}))
        .unwrap();
    assert_eq!(
        payload_of(&reader),
        serde_json::json!({"v": 2}),
        "warmed cache served the pre-rewrite payload"
    );
}
