//! Superseded atoms stop ranking by default; with_superseded(true) restores them.

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{
    AtomInput, EdgeKind, GraphExpand, MemoryEngine, MockEmbedder, MultiRecallQuery, RecallQuery,
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
    MemoryEngine::open(db).unwrap()
}

fn ids_of(hits: &[citadel_mem::AtomHit]) -> Vec<i64> {
    let mut ids: Vec<i64> = hits.iter().map(|h| h.id).collect();
    ids.sort_unstable();
    ids
}

/// old fact -> newer fact superseding it; both texts share the query terms.
fn seed_versions(eng: &MemoryEngine, region: &str) -> (i64, i64) {
    let old = eng
        .remember(region, AtomInput::new("fact", "Bob lives in Berlin"))
        .unwrap();
    let new = eng
        .remember(region, AtomInput::new("fact", "Bob lives in Munich now"))
        .unwrap();
    eng.link(new, old, EdgeKind::Supersedes, 1.0).unwrap();
    (old, new)
}

#[test]
fn superseded_atom_stops_ranking_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let (old, new) = seed_versions(&eng, "notes");

    let hits = eng
        .recall("notes", RecallQuery::by_text("where does Bob live", 10))
        .unwrap();
    let ids = ids_of(&hits);
    assert!(ids.contains(&new), "current version ranks");
    assert!(!ids.contains(&old), "stale version is excluded");

    let all = eng
        .recall(
            "notes",
            RecallQuery::by_text("where does Bob live", 10).with_superseded(true),
        )
        .unwrap();
    let ids = ids_of(&all);
    assert!(ids.contains(&new) && ids.contains(&old), "opt-in restores");
}

#[test]
fn chain_leaves_only_the_head() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let (a, b) = seed_versions(&eng, "notes");
    let c = eng
        .remember("notes", AtomInput::new("fact", "Bob lives in Hamburg now"))
        .unwrap();
    eng.link(c, b, EdgeKind::Supersedes, 1.0).unwrap();

    let hits = eng
        .recall("notes", RecallQuery::by_text("where does Bob live", 10))
        .unwrap();
    let ids = ids_of(&hits);
    assert!(ids.contains(&c), "head of the chain ranks");
    assert!(
        !ids.contains(&a) && !ids.contains(&b),
        "whole tail excluded"
    );
}

#[test]
fn sealed_region_excludes_and_restores() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let (old, new) = seed_versions(&eng, "vault");

    let hits = eng
        .recall("vault", RecallQuery::by_text("where does Bob live", 10))
        .unwrap();
    let ids = ids_of(&hits);
    assert!(ids.contains(&new));
    assert!(!ids.contains(&old), "sealed path filters stale versions");

    let all = eng
        .recall(
            "vault",
            RecallQuery::by_text("where does Bob live", 10).with_superseded(true),
        )
        .unwrap();
    assert!(ids_of(&all).contains(&old));
}

#[test]
fn recall_many_inherits_the_filter() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let (old, new) = seed_versions(&eng, "notes");

    let mq = MultiRecallQuery::new(
        vec![
            RecallQuery::by_text("Bob city", 10),
            RecallQuery::by_text("lives in", 10),
        ],
        10,
    );
    let ids = ids_of(&eng.recall_many("notes", mq).unwrap());
    assert!(ids.contains(&new));
    assert!(!ids.contains(&old), "merged pool has no stale versions");
}

#[test]
fn history_stays_reachable_deliberately() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("notes", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let (old, new) = seed_versions(&eng, "notes");

    // Walking Supersedes from the head is the explicit way to read history.
    let hits = eng
        .recall(
            "notes",
            RecallQuery::by_text("where does Bob live", 10)
                .with_graph_expand(GraphExpand::new(1, vec![EdgeKind::Supersedes])),
        )
        .unwrap();
    let ids = ids_of(&hits);
    assert!(
        ids.contains(&new) && ids.contains(&old),
        "walk reaches history"
    );

    // fetch_range is a deterministic listing: supersession never hides rows.
    let listed = eng
        .fetch_range("notes", &citadel_mem::FetchQuery::new(100))
        .unwrap();
    assert!(ids_of(&listed).contains(&old));
}
