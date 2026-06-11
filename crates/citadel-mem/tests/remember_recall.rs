use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, MemoryEngine, MockEmbedder, RecallQuery};
use serde_json::json;
use std::sync::Arc;

fn engine(dir: &std::path::Path) -> MemoryEngine {
    let db = Arc::new(
        DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    MemoryEngine::open(db).unwrap()
}

fn region(eng: &MemoryEngine, name: &str) {
    eng.create_region(name, Arc::new(MockEmbedder::new(64)))
        .unwrap();
}

#[test]
fn remember_then_recall_returns_top_hit() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    region(&eng, "notes");
    eng.remember("notes", AtomInput::new("fact", "the quick brown fox jumps"))
        .unwrap();
    eng.remember(
        "notes",
        AtomInput::new("fact", "lorem ipsum dolor sit amet"),
    )
    .unwrap();
    eng.remember(
        "notes",
        AtomInput::new("fact", "completely unrelated content here"),
    )
    .unwrap();

    let hits = eng
        .recall(
            "notes",
            RecallQuery::by_text("the quick brown fox jumps", 1),
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].text, "the quick brown fox jumps");
}

#[test]
fn count_counts_by_kind_without_materializing() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    region(&eng, "notes");
    for i in 0..5 {
        eng.remember("notes", AtomInput::new("fact", format!("fact {i}")))
            .unwrap();
    }
    eng.remember("notes", AtomInput::new("event", "one event"))
        .unwrap();

    assert_eq!(eng.count("notes", "fact").unwrap(), 5);
    assert_eq!(eng.count("notes", "event").unwrap(), 1);
    assert_eq!(eng.count("notes", "absent").unwrap(), 0);
}

#[test]
fn regions_are_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    region(&eng, "a");
    region(&eng, "b"); // same dim+metric -> shares the atoms table
    eng.remember("a", AtomInput::new("fact", "secret in region a"))
        .unwrap();

    let from_b = eng
        .recall("b", RecallQuery::by_text("secret in region a", 5))
        .unwrap();
    assert!(from_b.is_empty(), "region b must not see region a atoms");

    let from_a = eng
        .recall("a", RecallQuery::by_text("secret in region a", 5))
        .unwrap();
    assert_eq!(from_a.len(), 1);
}

#[test]
fn recall_filters_by_kind() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    region(&eng, "notes");
    eng.remember("notes", AtomInput::new("fact", "apple banana cherry"))
        .unwrap();
    eng.remember("notes", AtomInput::new("event", "apple banana cherry"))
        .unwrap();

    let hits = eng
        .recall(
            "notes",
            RecallQuery::by_text("apple banana cherry", 5).with_kinds(vec!["fact".into()]),
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert!(hits.iter().all(|h| h.kind == "fact"));
}

#[test]
fn recall_filters_by_payload_and_round_trips_json() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    region(&eng, "notes");
    eng.remember(
        "notes",
        AtomInput::new("fact", "data one").with_payload(json!({"topic": "rust"})),
    )
    .unwrap();
    eng.remember(
        "notes",
        AtomInput::new("fact", "data two").with_payload(json!({"topic": "python"})),
    )
    .unwrap();

    let hits = eng
        .recall(
            "notes",
            RecallQuery::by_text("data", 5).with_payload_filter(json!({"topic": "rust"})),
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].payload["topic"], "rust");
}

#[test]
fn recall_on_empty_region_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    region(&eng, "empty");
    let hits = eng
        .recall("empty", RecallQuery::by_text("anything", 5))
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn recall_unknown_region_errors() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let err = eng
        .recall("ghost", RecallQuery::by_text("x", 1))
        .unwrap_err();
    assert!(format!("{err}").contains("ghost"), "{err}");
}

#[test]
fn remember_batch_inserts_all_with_unique_ids() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    region(&eng, "notes");

    let batch = vec![
        AtomInput::new("fact", "alpha one").with_payload(json!({"n": 1})),
        AtomInput::new("fact", "alpha two").with_payload(json!({"n": 2})),
        AtomInput::new("fact", "alpha three"),
    ];
    let ids = eng.remember_batch("notes", batch).unwrap();
    assert_eq!(ids.len(), 3);
    let mut unique = ids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), 3, "batch ids are unique");

    let hits = eng
        .recall("notes", RecallQuery::by_text("alpha", 10))
        .unwrap();
    assert_eq!(hits.len(), 3, "all batched atoms are recallable");
    let one = hits
        .iter()
        .find(|h| h.text == "alpha one")
        .expect("batched atom present");
    assert_eq!(one.payload["n"], 1, "batched payload round-trips");

    assert!(
        eng.remember_batch("notes", vec![]).unwrap().is_empty(),
        "empty batch is a no-op"
    );
}
