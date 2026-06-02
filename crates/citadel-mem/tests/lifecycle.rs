use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{
    AtomInput, EdgeKind, EvictionPolicy, GraphExpand, MemoryEngine, MockEmbedder, RecallQuery,
};
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
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    eng
}

fn recall_count(eng: &MemoryEngine, text: &str) -> usize {
    eng.recall("r", RecallQuery::by_text(text, 100))
        .unwrap()
        .len()
}

#[test]
fn evict_purge_region_removes_all() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    for i in 0..5 {
        eng.remember("r", AtomInput::new("fact", format!("atom {i}")))
            .unwrap();
    }
    let report = eng.evict("r", EvictionPolicy::PurgeRegion).unwrap();
    assert_eq!(report.removed, 5);
    assert_eq!(recall_count(&eng, "atom"), 0);
}

#[test]
fn evict_low_score_keeps_high() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.remember("r", AtomInput::new("fact", "low one").with_score(0.1))
        .unwrap();
    eng.remember("r", AtomInput::new("fact", "high one").with_score(0.9))
        .unwrap();
    let report = eng
        .evict(
            "r",
            EvictionPolicy::LowScore {
                score_threshold: 0.5,
                confidence_threshold: 2.0, // confidence default 1.0 always passes
            },
        )
        .unwrap();
    assert_eq!(report.removed, 1);
    let hits = eng.recall("r", RecallQuery::by_text("one", 10)).unwrap();
    assert!(hits.iter().all(|h| h.text == "high one"));
}

#[test]
fn evict_predicate_match() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.remember(
        "r",
        AtomInput::new("fact", "rust doc").with_payload(json!({"lang": "rust"})),
    )
    .unwrap();
    eng.remember(
        "r",
        AtomInput::new("fact", "python doc").with_payload(json!({"lang": "python"})),
    )
    .unwrap();
    let report = eng
        .evict(
            "r",
            EvictionPolicy::PredicateMatch {
                predicate: json!({"lang": "python"}),
            },
        )
        .unwrap();
    assert_eq!(report.removed, 1);
    let hits = eng.recall("r", RecallQuery::by_text("doc", 10)).unwrap();
    assert!(hits.iter().all(|h| h.payload["lang"] == "rust"));
}

#[test]
fn evict_lru_keeps_fraction() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    for i in 0..10 {
        eng.remember("r", AtomInput::new("fact", format!("item {i}")))
            .unwrap();
    }
    let report = eng
        .evict("r", EvictionPolicy::Lru { keep_fraction: 0.5 })
        .unwrap();
    assert_eq!(report.removed, 5, "delete bottom 50% of 10");
    assert_eq!(recall_count(&eng, "item"), 5);
}

#[test]
fn evict_stale_spares_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.remember("r", AtomInput::new("fact", "fresh")).unwrap();
    let report = eng
        .evict(
            "r",
            EvictionPolicy::Stale {
                older_than_micros: 3_600_000_000,
            },
        )
        .unwrap();
    assert_eq!(report.removed, 0);
    assert_eq!(recall_count(&eng, "fresh"), 1);
}

#[test]
fn evict_purge_removes_immutable_too() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.remember("r", AtomInput::new("fact", "protected").immutable())
        .unwrap();
    let r1 = eng
        .evict(
            "r",
            EvictionPolicy::LowScore {
                score_threshold: 100.0,
                confidence_threshold: 100.0,
            },
        )
        .unwrap();
    assert_eq!(r1.removed, 0, "immutable spared by LowScore");
    let r2 = eng.evict("r", EvictionPolicy::PurgeRegion).unwrap();
    assert_eq!(r2.removed, 1, "PurgeRegion removes immutable");
}

#[test]
fn evolve_links_close_neighbors_and_sets_score() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let a = eng
        .remember("r", AtomInput::new("fact", "red green blue"))
        .unwrap();
    let _b = eng
        .remember("r", AtomInput::new("fact", "red green yellow"))
        .unwrap();
    let _c = eng
        .remember("r", AtomInput::new("fact", "alpha beta gamma"))
        .unwrap();

    let report = eng.evolve("r", a, 5, 0.5).unwrap();
    assert!(report.links_added >= 1, "should link the close neighbor");
    assert!(report.score > 0.0);

    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("red green blue", 1)
                .with_graph_expand(GraphExpand::new(1, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    assert!(hits.len() >= 2, "seed + at least one derived neighbor");
}

#[test]
fn summarize_rolls_up_per_kind() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.remember("r", AtomInput::new("fact", "f1").with_score(0.4))
        .unwrap();
    eng.remember("r", AtomInput::new("fact", "f2").with_score(0.6))
        .unwrap();
    eng.remember("r", AtomInput::new("event", "e1").with_score(1.0))
        .unwrap();

    let summary = eng.summarize("r", 0).unwrap();
    assert_eq!(summary.total, 3);
    let fact = summary.kinds.iter().find(|k| k.kind == "fact").unwrap();
    assert_eq!(fact.count, 2);
    assert!(
        (fact.avg_score - 0.5).abs() < 0.01,
        "avg score {}",
        fact.avg_score
    );
    let event = summary.kinds.iter().find(|k| k.kind == "event").unwrap();
    assert_eq!(event.count, 1);
}
