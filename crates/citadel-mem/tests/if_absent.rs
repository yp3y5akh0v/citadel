//! remember_if_absent: (kind, exact text) identity; duplicates still upsert edges.

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, EdgeKind, MemoryEngine, MockEmbedder};
use citadel_sql::{Connection, Value};
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
fn inserts_when_absent_then_returns_existing() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let first = eng
        .remember_if_absent(
            "notes",
            AtomInput::new("fact", "Bob lives in Berlin"),
            &[],
            None,
        )
        .unwrap();
    assert!(first.inserted);

    let second = eng
        .remember_if_absent(
            "notes",
            AtomInput::new("fact", "Bob lives in Berlin"),
            &[],
            None,
        )
        .unwrap();
    assert!(!second.inserted);
    assert_eq!(second.id, first.id);
    assert_eq!(eng.count("notes", "fact").unwrap(), 1);
}

#[test]
fn kind_and_text_are_both_identity() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let base = eng
        .remember_if_absent("notes", AtomInput::new("fact", "same text"), &[], None)
        .unwrap();
    let other_kind = eng
        .remember_if_absent("notes", AtomInput::new("event", "same text"), &[], None)
        .unwrap();
    let other_text = eng
        .remember_if_absent("notes", AtomInput::new("fact", "other text"), &[], None)
        .unwrap();

    assert!(other_kind.inserted);
    assert!(other_text.inserted);
    assert_ne!(other_kind.id, base.id);
    assert_ne!(other_text.id, base.id);
}

#[test]
fn payload_is_not_identity_and_original_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let first = eng
        .remember_if_absent(
            "notes",
            AtomInput::new("fact", "the meeting is on Friday").with_payload(json!({"v": 1})),
            &[],
            None,
        )
        .unwrap();
    let second = eng
        .remember_if_absent(
            "notes",
            AtomInput::new("fact", "the meeting is on Friday").with_payload(json!({"v": 2})),
            &[],
            None,
        )
        .unwrap();

    assert!(!second.inserted);
    assert_eq!(second.id, first.id);
    let hit = eng.fetch_one("notes", first.id).unwrap().unwrap();
    assert_eq!(hit.payload, json!({"v": 1}), "existing atom is untouched");
}

#[test]
fn oldest_preexisting_duplicate_wins() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    // remember() never dedups, so two identical atoms can pre-exist.
    let a = eng
        .remember("notes", AtomInput::new("fact", "twice stored"))
        .unwrap();
    let b = eng
        .remember("notes", AtomInput::new("fact", "twice stored"))
        .unwrap();
    assert!(a < b);

    let out = eng
        .remember_if_absent("notes", AtomInput::new("fact", "twice stored"), &[], None)
        .unwrap();
    assert!(!out.inserted);
    assert_eq!(out.id, a, "deterministic: oldest id");
}

#[test]
fn expired_duplicate_counts_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let dead = eng
        .remember(
            "notes",
            AtomInput::new("fact", "short lived").with_expires_at(1),
        )
        .unwrap();
    let out = eng
        .remember_if_absent("notes", AtomInput::new("fact", "short lived"), &[], None)
        .unwrap();
    assert!(out.inserted);
    assert_ne!(out.id, dead);
}

#[test]
fn enriches_existing_atom_with_new_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let src = eng
        .remember(
            "notes",
            AtomInput::new("fact", "Alice's cat is named Mochi"),
        )
        .unwrap();
    let first = eng
        .remember_if_absent(
            "notes",
            AtomInput::new("derived", "Alice has a pet"),
            &[],
            None,
        )
        .unwrap();

    // Second pass lands the edge on the existing atom; a third changes nothing.
    let evidence = json!({"quote": "my cat Mochi"});
    let second = eng
        .remember_if_absent(
            "notes",
            AtomInput::new("derived", "Alice has a pet"),
            &[src],
            Some(evidence.clone()),
        )
        .unwrap();
    assert!(!second.inserted);
    assert_eq!(second.id, first.id);

    let third = eng
        .remember_if_absent(
            "notes",
            AtomInput::new("derived", "Alice has a pet"),
            &[src],
            Some(evidence.clone()),
        )
        .unwrap();
    assert!(!third.inserted);

    let edges = eng
        .fetch_edges(Some(first.id), None, Some(EdgeKind::DerivedFrom))
        .unwrap();
    assert_eq!(edges.len(), 1, "edge upsert converges");
    assert_eq!(edges[0].dst_id, src);
    assert_eq!(edges[0].evidence_ref.as_ref(), Some(&evidence));
}

#[test]
fn missing_source_stores_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    assert!(eng
        .remember_if_absent(
            "notes",
            AtomInput::new("derived", "unsupported"),
            &[99_999],
            None,
        )
        .is_err());
    assert_eq!(eng.count("notes", "derived").unwrap(), 0);
}

#[test]
fn interrupted_forget_residue_is_skipped_not_fatal() {
    // The dedup scan must treat erased-key residue as absent, not fail the region.
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .unwrap(),
    );
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let victim = eng
        .remember("vault", AtomInput::new("fact", "residue text"))
        .unwrap();

    // Forget then restore the row: the exact state an interrupted forget leaves.
    let table = "memory_atoms_d64_cosine_enc";
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!(
                "SELECT region_id, kind, sealed, key_slot, key_gen FROM {table} WHERE id = $1"
            ),
            &[Value::Integer(victim)],
        )
        .unwrap();
    let row = qr.rows.first().unwrap().clone();
    let receipt = eng.forget_atoms("vault", &[victim], false).unwrap();
    assert!(receipt.cryptographic_erasure);
    conn.execute_params(
        &format!(
            "INSERT INTO {table} \
             (id, region_id, kind, sealed, key_slot, key_gen, score, confidence, \
              access_count, immutable, created_at, accessed_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, 0.0, 1.0, 0, 0, \
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)"
        ),
        &[
            Value::Integer(victim),
            row[0].clone(),
            row[1].clone(),
            row[2].clone(),
            row[3].clone(),
            row[4].clone(),
        ],
    )
    .unwrap();

    let out = eng
        .remember_if_absent("vault", AtomInput::new("fact", "residue text"), &[], None)
        .unwrap();
    assert!(out.inserted, "residue with an erased key cannot match");
    assert_ne!(out.id, victim);

    let again = eng
        .remember_if_absent("vault", AtomInput::new("fact", "residue text"), &[], None)
        .unwrap();
    assert!(!again.inserted, "the fresh atom is now the live duplicate");
    assert_eq!(again.id, out.id);
}

#[test]
fn sealed_region_dedups_by_unsealed_text() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();

    let first = eng
        .remember_if_absent("vault", AtomInput::new("fact", "sealed secret"), &[], None)
        .unwrap();
    assert!(first.inserted);

    let second = eng
        .remember_if_absent("vault", AtomInput::new("fact", "sealed secret"), &[], None)
        .unwrap();
    assert!(!second.inserted);
    assert_eq!(second.id, first.id);

    let fresh = eng
        .remember_if_absent("vault", AtomInput::new("fact", "another secret"), &[], None)
        .unwrap();
    assert!(fresh.inserted);
    assert_eq!(eng.count("vault", "fact").unwrap(), 2);
}
