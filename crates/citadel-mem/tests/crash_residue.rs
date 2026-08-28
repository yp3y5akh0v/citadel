//! Erased/recycled-key residue must read as absent and never wedge a retry.

use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{AtomId, AtomInput, MemoryEngine, MockEmbedder};
use citadel_sql::{Connection, Value};

const DIM: usize = 64;
const TABLE: &str = "memory_atoms_d64_cosine_enc";

fn open_db(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .unwrap(),
    )
}

/// Forget then restore the row verbatim: the exact interrupted-forget state.
fn plant_residue(db: &Arc<Database>, eng: &MemoryEngine, region: &str, victim: AtomId) {
    let conn = Connection::open(db).unwrap();
    let qr = conn
        .query_params(
            &format!(
                "SELECT region_id, kind, sealed, key_slot, key_gen FROM {TABLE} WHERE id = $1"
            ),
            &[Value::Integer(victim)],
        )
        .unwrap();
    let row = qr.rows.first().unwrap().clone();
    let receipt = eng.forget_atoms(region, &[victim], false).unwrap();
    assert!(receipt.cryptographic_erasure);
    conn.execute_params(
        &format!(
            "INSERT INTO {TABLE} \
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
}

#[test]
fn fetch_one_reads_forget_residue_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let victim = eng
        .remember("vault", AtomInput::new("fact", "residue text"))
        .unwrap();
    plant_residue(&db, &eng, "vault", victim);

    let got = eng.fetch_one("vault", victim).unwrap();
    assert!(
        got.is_none(),
        "an erased-key residue row must read as absent, not error"
    );
}

#[test]
fn fetch_last_skips_residue_and_returns_the_genuine_latest() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let genuine = eng
        .remember("vault", AtomInput::new("fact", "the real latest fact"))
        .unwrap();
    let victim = eng
        .remember("vault", AtomInput::new("fact", "newer but interrupted"))
        .unwrap();
    plant_residue(&db, &eng, "vault", victim);

    let last = eng.fetch_last("vault", "fact").unwrap().unwrap();
    assert_eq!(
        last.id, genuine,
        "residue must not mask the genuine latest atom of the kind"
    );
    assert_eq!(last.text, "the real latest fact");
}

/// Residue over a RECYCLED slot must not wedge the retry or hurt the new atom.
#[test]
fn delete_retry_over_recycled_slot_converges_and_spares_the_new_atom() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let victim = eng
        .remember("vault", AtomInput::new("fact", "interrupted delete"))
        .unwrap();
    plant_residue(&db, &eng, "vault", victim);
    // The forget freed the victim's slot; the next insert recycles it.
    let survivor = eng
        .remember("vault", AtomInput::new("fact", "recycled the slot"))
        .unwrap();

    let report = eng.delete_atoms("vault", &[victim]).unwrap();
    assert_eq!(report.removed, 1, "the residue row is deleted");
    assert!(
        eng.fetch_one("vault", victim).unwrap().is_none(),
        "residue gone after the converged retry"
    );
    let kept = eng.fetch_one("vault", survivor).unwrap().unwrap();
    assert_eq!(
        kept.text, "recycled the slot",
        "the new owner of the slot is untouched"
    );
}

#[test]
fn drop_region_retry_after_tombstone_crash_converges() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    eng.remember("vault", AtomInput::new("fact", "content"))
        .unwrap();

    // Tombstone committed, row deletes did not: a retried drop must complete.
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            "SELECT id, rsk_slot FROM memory_regions WHERE name = $1",
            &[Value::Text("vault".into())],
        )
        .unwrap();
    let row = qr.rows.first().unwrap();
    let (region_id, slot) = (
        match &row[0] {
            Value::Integer(i) => *i,
            other => panic!("id: {other:?}"),
        },
        match &row[1] {
            Value::Integer(i) => *i as u32,
            other => panic!("rsk_slot: {other:?}"),
        },
    );
    db.region_store_tombstone(
        slot,
        region_id as u64,
        db.region_store_slot(slot).unwrap().gen,
    )
    .unwrap();

    eng.drop_region("vault").unwrap();

    let qr = conn
        .query_params(
            "SELECT id FROM memory_regions WHERE name = $1",
            &[Value::Text("vault".into())],
        )
        .unwrap();
    assert!(qr.rows.is_empty(), "the retried drop must remove the row");
    // The name is reusable afterwards - the lifecycle fully converged.
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    eng.remember("vault", AtomInput::new("fact", "fresh"))
        .unwrap();
}
