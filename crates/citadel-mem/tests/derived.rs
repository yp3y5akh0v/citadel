//! remember_derived: atom + DerivedFrom edges in one transaction, with evidence.

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, EdgeKind, MemError, MemoryEngine, MockEmbedder};
use serde_json::json;

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

fn plain_region(eng: &MemoryEngine, name: &str) {
    eng.create_region(name, Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
}

#[test]
fn links_all_sources_with_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let a = eng
        .remember(
            "notes",
            AtomInput::new("fact", "Alice's cat is named Mochi"),
        )
        .unwrap();
    let b = eng
        .remember(
            "notes",
            AtomInput::new("fact", "Alice adopted a cat in 2020"),
        )
        .unwrap();

    let evidence = json!({"quote": "my cat Mochi", "turn": 3});
    let d = eng
        .remember_derived(
            "notes",
            AtomInput::new("derived", "Alice has owned Mochi since 2020"),
            &[a, b],
            Some(evidence.clone()),
        )
        .unwrap();

    let edges = eng
        .fetch_edges_in_region("notes", Some(d), None, Some(EdgeKind::DerivedFrom), 10)
        .unwrap();
    let mut dsts: Vec<_> = edges.iter().map(|e| e.dst_id).collect();
    dsts.sort_unstable();
    assert_eq!(dsts, vec![a, b], "one edge per source");
    for e in &edges {
        assert_eq!(e.weight, 1.0);
        assert_eq!(e.evidence_ref.as_ref(), Some(&evidence));
    }
}

#[test]
fn missing_source_stores_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let a = eng
        .remember("notes", AtomInput::new("fact", "Bob lives in Berlin"))
        .unwrap();

    let err = eng
        .remember_derived(
            "notes",
            AtomInput::new("derived", "Bob is German"),
            &[a, 99_999],
            None,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        MemError::AtomNotLive {
            atom_id: 99_999,
            ref region,
        } if region == "notes"
    ));

    assert_eq!(
        eng.count("notes", "derived").unwrap(),
        0,
        "atom rolled back"
    );
    assert!(
        eng.fetch_edges_in_region("notes", None, Some(a), Some(EdgeKind::DerivedFrom), 10)
            .unwrap()
            .is_empty(),
        "no provenance edge survives the rollback"
    );
}

#[test]
fn source_from_another_region_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");
    plain_region(&eng, "other");

    let foreign = eng
        .remember("other", AtomInput::new("fact", "out of region"))
        .unwrap();
    assert!(eng
        .remember_derived(
            "notes",
            AtomInput::new("derived", "cross-region provenance"),
            &[foreign],
            None,
        )
        .is_err());
}

#[test]
fn empty_sources_equals_remember() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let id = eng
        .remember_derived("notes", AtomInput::new("fact", "plain"), &[], None)
        .unwrap();
    assert!(eng
        .fetch_edges_in_region("notes", Some(id), None, None, 10)
        .unwrap()
        .is_empty());
    assert_eq!(eng.count("notes", "fact").unwrap(), 1);
}

#[test]
fn duplicate_source_ids_write_one_edge() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let a = eng
        .remember("notes", AtomInput::new("fact", "the meeting is on Friday"))
        .unwrap();
    let d = eng
        .remember_derived(
            "notes",
            AtomInput::new("derived", "meeting day known"),
            &[a, a, a],
            None,
        )
        .unwrap();
    assert_eq!(
        eng.fetch_edges_in_region("notes", Some(d), None, Some(EdgeKind::DerivedFrom), 10)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn sealed_region_provenance_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();

    let a = eng
        .remember("vault", AtomInput::new("fact", "sealed source"))
        .unwrap();
    let evidence = json!({"quote": "sealed"});
    let d = eng
        .remember_derived(
            "vault",
            AtomInput::new("derived", "sealed derivation"),
            &[a],
            Some(evidence.clone()),
        )
        .unwrap();

    let edges = eng
        .fetch_edges_in_region("vault", Some(d), None, Some(EdgeKind::DerivedFrom), 10)
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].dst_id, a);
    assert_eq!(edges[0].evidence_ref.as_ref(), Some(&evidence));
}

#[test]
fn link_with_evidence_upsert_replaces_weight_and_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let a = eng
        .remember("notes", AtomInput::new("fact", "one"))
        .unwrap();
    let b = eng
        .remember("notes", AtomInput::new("fact", "two"))
        .unwrap();

    eng.link_with_evidence_in_region("notes", a, b, EdgeKind::Refines, 0.5, Some(json!({"v": 1})))
        .unwrap();
    let e = &eng
        .fetch_edges_in_region("notes", Some(a), Some(b), None, 1)
        .unwrap()[0];
    assert_eq!(e.weight, 0.5);
    assert_eq!(e.evidence_ref, Some(json!({"v": 1})));

    // Re-linking replaces both fields; plain link clears evidence back to NULL.
    eng.link_in_region("notes", a, b, EdgeKind::Refines, 0.9)
        .unwrap();
    let e = &eng
        .fetch_edges_in_region("notes", Some(a), Some(b), None, 1)
        .unwrap()[0];
    assert_eq!(e.weight, 0.9);
    assert_eq!(e.evidence_ref, None);
}

#[test]
fn evolve_writes_similar_to_not_derived_from() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let a = eng
        .remember("notes", AtomInput::new("fact", "red green blue"))
        .unwrap();
    eng.remember("notes", AtomInput::new("fact", "red green yellow"))
        .unwrap();

    let report = eng.evolve("notes", a, 5, f32::MAX).unwrap();
    assert!(report.links_added >= 1);

    assert!(
        !eng.fetch_edges_in_region("notes", Some(a), None, Some(EdgeKind::SimilarTo), 10)
            .unwrap()
            .is_empty(),
        "evolve neighbors are similar_to"
    );
    assert!(
        eng.fetch_edges_in_region("notes", Some(a), None, Some(EdgeKind::DerivedFrom), 10)
            .unwrap()
            .is_empty(),
        "evolve must not fabricate provenance"
    );
}
