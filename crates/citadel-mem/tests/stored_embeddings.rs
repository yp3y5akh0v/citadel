//! Stored-vector identity/verification: storage bytes, not embedder recomputation.

use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{
    AtomId, AtomInput, Embedder, MemError, MemoryEngine, MockEmbedder, MultiRecallQuery,
    RecallQuery, StoredEmbeddingsIdentity, STORED_EMBEDDINGS_SCHEMA,
};
use citadel_sql::{Connection, Value};
use serde_json::json;

const DIM: usize = 8;
const PLAIN_TABLE: &str = "memory_atoms_d8_cosine";
const SEALED_TABLE: &str = "memory_atoms_d8_cosine_enc";

fn create_db(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .unwrap(),
    )
}

fn create_region(eng: &MemoryEngine, name: &str, encrypted: bool) {
    if encrypted {
        eng.create_encrypted_region(name, Arc::new(MockEmbedder::new(DIM)))
            .unwrap();
    } else {
        eng.create_region(name, Arc::new(MockEmbedder::new(DIM)))
            .unwrap();
    }
}

fn embedding(text: &str) -> Vec<f32> {
    MockEmbedder::new(DIM).embed(&[text]).unwrap().remove(0)
}

fn assert_invalid<T: std::fmt::Debug>(result: Result<T, MemError>, needle: &str) {
    let err = result.unwrap_err();
    assert!(
        matches!(err, MemError::Invalid(_)),
        "expected Invalid, got {err:?}"
    );
    assert!(
        err.to_string().contains(needle),
        "error {err:?} does not contain {needle:?}"
    );
}

fn assert_region_not_found<T: std::fmt::Debug>(result: Result<T, MemError>) {
    assert!(
        matches!(result, Err(MemError::RegionNotFound(_))),
        "dead region key must refuse the operation, got {result:?}"
    );
}

fn populate(eng: &MemoryEngine, region: &str) -> Vec<(AtomId, Vec<f32>)> {
    let first = eng
        .remember(region, AtomInput::new("fact", "alpha memory"))
        .unwrap();
    eng.remember(region, AtomInput::new("note", "ignored kind"))
        .unwrap();
    eng.remember(
        region,
        AtomInput::new("fact", "expired memory").with_expires_at(1),
    )
    .unwrap();
    let last = eng
        .remember(region, AtomInput::new("fact", "omega memory"))
        .unwrap();
    vec![
        (first, embedding("alpha memory")),
        (last, embedding("omega memory")),
    ]
}

fn assert_identity_shape(identity: &StoredEmbeddingsIdentity, region: &str) {
    assert_eq!(identity.schema(), STORED_EMBEDDINGS_SCHEMA);
    assert_eq!(identity.region(), region);
    assert_eq!(identity.kind(), "fact");
    assert_eq!(identity.count(), 2);
    assert_eq!(identity.dim(), DIM as u32);
    assert_eq!(identity.sha256().len(), 64);
    assert!(identity
        .sha256()
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit()));
}

#[test]
fn exact_verifier_rejects_every_structural_or_bit_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db).unwrap();

    for encrypted in [false, true] {
        let region = format!("Proof-{}", u8::from(encrypted));
        create_region(&eng, &region, encrypted);
        let expected = populate(&eng, &region);

        let identity = eng
            .verify_stored_embeddings_exact(&region.to_ascii_uppercase(), "fact", &expected)
            .unwrap();
        assert_identity_shape(&identity, &region.to_ascii_lowercase());
        assert_eq!(
            identity,
            eng.stored_embeddings_identity(&region, "fact").unwrap()
        );

        let mut one_bit = expected.clone();
        one_bit[0].1[0] = f32::from_bits(one_bit[0].1[0].to_bits() ^ 1);
        assert_invalid(
            eng.verify_stored_embeddings_exact(&region, "fact", &one_bit),
            "bit mismatch",
        );

        let mut wrong_id = expected.clone();
        wrong_id[1].0 += 10_000;
        assert_invalid(
            eng.verify_stored_embeddings_exact(&region, "fact", &wrong_id),
            "id mismatch",
        );

        let mut too_few = expected.clone();
        too_few.pop();
        assert_invalid(
            eng.verify_stored_embeddings_exact(&region, "fact", &too_few),
            "count",
        );

        let mut unsorted = expected.clone();
        unsorted.swap(0, 1);
        assert_invalid(
            eng.verify_stored_embeddings_exact(&region, "fact", &unsorted),
            "strictly ascending",
        );

        let mut duplicate = expected.clone();
        duplicate[1].0 = duplicate[0].0;
        assert_invalid(
            eng.verify_stored_embeddings_exact(&region, "fact", &duplicate),
            "strictly ascending",
        );

        let mut wrong_dim = expected.clone();
        wrong_dim[0].1.pop();
        assert_invalid(
            eng.verify_stored_embeddings_exact(&region, "fact", &wrong_dim),
            "dimension",
        );

        let mut nonfinite = expected.clone();
        nonfinite[0].1[0] = f32::NAN;
        assert_invalid(
            eng.verify_stored_embeddings_exact(&region, "fact", &nonfinite),
            "non-finite",
        );
    }
}

fn identity_for_storage(encrypted: bool) -> StoredEmbeddingsIdentity {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    create_region(&eng, "same-proof", encrypted);
    let expected = populate(&eng, "same-proof");
    eng.verify_stored_embeddings_exact("same-proof", "fact", &expected)
        .unwrap()
}

#[test]
fn plaintext_and_encrypted_storage_have_the_same_canonical_identity() {
    assert_eq!(identity_for_storage(false), identity_for_storage(true));
}

#[test]
fn canonical_encoding_has_a_pinned_digest() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    create_region(&eng, "pinned", false);
    let expected = populate(&eng, "pinned");
    assert_eq!(
        expected.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [1, 4],
        "the digest fixture binds these exact ordered ids"
    );
    let identity = eng
        .verify_stored_embeddings_exact("pinned", "fact", &expected)
        .unwrap();
    // An encoding change must bump STORED_EMBEDDINGS_SCHEMA and re-pin this digest.
    assert_eq!(
        identity.sha256(),
        "52ddbd6adc8bf1bc88e016f6ba06ef40576228342f7827dd29a560f45bf4765e"
    );
}

#[test]
fn plaintext_scan_rejects_nonfinite_stored_bits() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    create_region(&eng, "plain", false);
    let id = eng
        .remember("plain", AtomInput::new("fact", "finite first"))
        .unwrap();

    let mut corrupted = embedding("finite first");
    corrupted[3] = f32::INFINITY;
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {PLAIN_TABLE} SET embedding = $1 WHERE id = $2"),
            &[Value::Vector(corrupted.into()), Value::Integer(id)],
        )
        .unwrap();
    assert_invalid(
        eng.stored_embeddings_identity("plain", "fact"),
        "non-finite",
    );
}

#[test]
fn encrypted_scan_skips_an_atom_after_its_key_is_erased() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    create_region(&eng, "vault", true);
    let expected = populate(&eng, "vault");
    let before = eng.stored_embeddings_identity("vault", "fact").unwrap();

    let erased_id = expected[0].0;
    let qr = Connection::open(&db)
        .unwrap()
        .query_params(
            &format!("SELECT key_slot FROM {SEALED_TABLE} WHERE id = $1"),
            &[Value::Integer(erased_id)],
        )
        .unwrap();
    let Value::Integer(slot) = qr.rows[0][0] else {
        panic!("key_slot is not an integer")
    };
    db.atom_store_tombstone(slot as u32, erased_id as u64)
        .unwrap();

    let remaining = vec![expected[1].clone()];
    let after = eng
        .verify_stored_embeddings_exact("vault", "fact", &remaining)
        .unwrap();
    assert_eq!(after.count(), 1);
    assert_ne!(before.sha256(), after.sha256());
    assert_invalid(
        eng.verify_stored_embeddings_exact("vault", "fact", &expected),
        "id mismatch",
    );
}

#[test]
fn encrypted_scan_refuses_a_dead_region_key_even_when_rows_remain() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    create_region(&eng, "vault", true);
    populate(&eng, "vault");

    let qr = Connection::open(&db)
        .unwrap()
        .query_params(
            "SELECT id, rsk_slot FROM memory_regions WHERE name = $1",
            &[Value::Text("vault".into())],
        )
        .unwrap();
    let Value::Integer(region_id) = qr.rows[0][0] else {
        panic!("region id is not an integer")
    };
    let Value::Integer(slot) = qr.rows[0][1] else {
        panic!("rsk_slot is not an integer")
    };
    db.region_store_tombstone(slot as u32, region_id as u64)
        .unwrap();

    assert!(matches!(
        eng.stored_embeddings_identity("vault", "fact"),
        Err(MemError::RegionNotFound(_))
    ));
    assert!(matches!(
        eng.stored_embeddings_identity("vault", "fact"),
        Err(MemError::RegionNotFound(_))
    ));
}

#[test]
fn sealed_vector_proofs_and_ann_fingerprints_require_exact_row_key_generation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    create_region(&eng, "vault", true);
    let expected = populate(&eng, "vault");
    let stale_id = expected[0].0;

    // Corrupt one row's generation: an id-only lookup would silently accept it.
    eng.persist_ann_index("vault").unwrap();
    Connection::open(&db)
        .unwrap()
        .execute_params(
            &format!("UPDATE {SEALED_TABLE} SET key_gen = key_gen + 1 WHERE id = $1"),
            &[Value::Integer(stale_id)],
        )
        .unwrap();

    let identity = eng.stored_embeddings_identity("vault", "fact").unwrap();
    assert_eq!(identity.count(), 1, "the mismatched row is not live");
    eng.verify_stored_embeddings_exact("vault", "fact", &expected[1..])
        .unwrap();

    let hits = eng
        .recall(
            "vault",
            RecallQuery::by_embedding(expected[0].1.clone(), 10),
        )
        .unwrap();
    assert!(
        hits.iter().all(|hit| hit.id != stale_id),
        "the persisted ANN fingerprint must reject the mismatched key binding"
    );
}

#[test]
fn every_sealed_surface_refuses_a_cross_engine_partial_region_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let owner = MemoryEngine::open(db.clone()).unwrap();
    create_region(&owner, "vault", true);
    let atom = owner
        .remember("vault", AtomInput::new("fact", "cached secret"))
        .unwrap();

    // One handle per assertion: a failed guard evicts, so sharing tests only one.
    let mut clients = Vec::new();
    for _ in 0..13 {
        let client = MemoryEngine::open(db.clone()).unwrap();
        client
            .attach_existing_region("vault", Arc::new(MockEmbedder::new(DIM)))
            .unwrap();
        clients.push(client);
    }
    // Warm decrypted caches before the key dies; recall must still reject.
    assert_eq!(
        clients[4]
            .recall("vault", RecallQuery::by_text("cached secret", 5))
            .unwrap()
            .len(),
        1
    );

    let conn = Connection::open(&db).unwrap();
    let region = conn
        .query_params(
            "SELECT id, rsk_slot FROM memory_regions WHERE name = $1",
            &[Value::Text("vault".into())],
        )
        .unwrap();
    let Value::Integer(region_id) = region.rows[0][0] else {
        panic!("region id is not an integer")
    };
    let Value::Integer(rsk_slot) = region.rows[0][1] else {
        panic!("rsk_slot is not an integer")
    };
    let before = conn
        .query_params(
            &format!("SELECT sealed, score FROM {SEALED_TABLE} WHERE id = $1"),
            &[Value::Integer(atom)],
        )
        .unwrap()
        .rows[0]
        .clone();
    drop(conn);

    // Key-first crash window: RSK dead while row, ciphertext, and ACKs remain.
    db.region_store_tombstone(rsk_slot as u32, region_id as u64)
        .unwrap();

    assert_region_not_found(clients[0].fetch("vault", "fact", None, 10));
    assert_region_not_found(clients[1].fetch_one("vault", atom));
    assert_region_not_found(clients[2].fetch_last("vault", "fact"));
    assert_region_not_found(clients[3].count("vault", "fact"));
    assert_region_not_found(clients[4].recall("vault", RecallQuery::by_text("cached secret", 5)));
    assert_region_not_found(clients[5].recall_many(
        "vault",
        MultiRecallQuery::new(vec![RecallQuery::by_text("secret", 5)], 5),
    ));
    assert_region_not_found(clients[6].persist_ann_index("vault"));
    assert_region_not_found(clients[7].ann_cache_status("vault"));
    assert_region_not_found(clients[8].verify_atoms("vault", &[atom]));
    assert_region_not_found(clients[9].summarize("vault", 0));
    assert_region_not_found(clients[10].stored_embeddings_identity("vault", "fact"));
    assert_region_not_found(clients[11].update_atom_payload(
        "vault",
        atom,
        &json!({"must_not": "write"}),
    ));
    assert_region_not_found(clients[12].set_importance("vault", &[(atom, 99.0)]));

    let after = Connection::open(&db)
        .unwrap()
        .query_params(
            &format!("SELECT sealed, score FROM {SEALED_TABLE} WHERE id = $1"),
            &[Value::Integer(atom)],
        )
        .unwrap()
        .rows[0]
        .clone();
    assert_eq!(
        after, before,
        "refused mutators must leave the atom untouched"
    );
}

/// A failed successor attach must not leave reads on the predecessor's handle.
#[test]
fn failed_attach_to_different_successor_config_evicts_stale_handle() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let owner = MemoryEngine::open(db.clone()).unwrap();
    let client = MemoryEngine::open(db).unwrap();

    for encrypted in [false, true] {
        let region = format!("successor-{}", u8::from(encrypted));
        create_region(&owner, &region, encrypted);
        client
            .attach_existing_region(&region, Arc::new(MockEmbedder::new(DIM)))
            .unwrap();
        owner.drop_region(&region).unwrap();
        if encrypted {
            owner
                .create_encrypted_region(&region, Arc::new(MockEmbedder::new(16)))
                .unwrap();
        } else {
            owner
                .create_region(&region, Arc::new(MockEmbedder::new(16)))
                .unwrap();
        }

        assert!(matches!(
            client.attach_existing_region(&region, Arc::new(MockEmbedder::new(DIM))),
            Err(MemError::DimMismatch { .. })
        ));
        assert!(matches!(
            client.fetch(&region, "fact", None, 1),
            Err(MemError::RegionNotFound(_))
        ));
        client
            .attach_existing_region(&region, Arc::new(MockEmbedder::new(16)))
            .unwrap();
    }
}
