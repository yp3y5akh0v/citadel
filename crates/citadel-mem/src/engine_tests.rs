use super::*;
use crate::embed::MockEmbedder;
use crate::error::MemError;
use citadel::{Argon2Profile, Database, DatabaseBuilder};
use std::sync::Arc;

fn create_db(path: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    )
}

fn open_db(path: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .open()
            .unwrap(),
    )
}

fn create_enc_db(path: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path.join("m.db"))
            .passphrase(b"test-passphrase")
            .enable_region_keys(true)
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    )
}

#[test]
fn create_region_is_idempotent_reattach() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    let e = Arc::new(MockEmbedder::new(8));
    let id1 = eng.create_region("notes", e.clone()).unwrap();
    let id2 = eng.create_region("notes", e).unwrap();
    assert_eq!(id1, id2, "re-attaching the same region returns the same id");
}

#[test]
fn create_region_rejects_dim_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("notes", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let err = eng
        .create_region("notes", Arc::new(MockEmbedder::new(16)))
        .unwrap_err();
    assert!(
        matches!(
            err,
            MemError::DimMismatch {
                expected: 8,
                got: 16,
                ..
            }
        ),
        "got {err:?}"
    );
}

#[test]
fn drop_region_then_recreate_allocates_new_id() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    let id1 = eng
        .create_region("notes", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.drop_region("notes").unwrap();
    let id2 = eng
        .create_region("notes", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(id2 > id1, "ids monotonic: {id1} then {id2}");
}

#[test]
fn drop_missing_region_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.drop_region("ghost").unwrap();
}

#[test]
fn region_metadata_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let id1 = {
        let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
        eng.create_region("notes", Arc::new(MockEmbedder::new(8)))
            .unwrap()
    };
    let eng = MemoryEngine::open(open_db(dir.path())).unwrap();
    let id2 = eng
        .create_region("notes", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert_eq!(id1, id2, "region persists across reopen");
}

#[test]
fn supersedes_edges_reject_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let v1 = eng
        .remember("r", AtomInput::new("self_model", "v1"))
        .unwrap();
    let v2 = eng
        .remember("r", AtomInput::new("self_model", "v2"))
        .unwrap();

    eng.link(v2, v1, EdgeKind::Supersedes, 1.0).unwrap();
    assert!(matches!(
        eng.link(v1, v2, EdgeKind::Supersedes, 1.0),
        Err(MemError::Cycle { .. })
    ));
    assert!(matches!(
        eng.link(v1, v1, EdgeKind::Supersedes, 1.0),
        Err(MemError::Cycle { .. })
    ));
}

#[test]
fn reranker_reorders_recall_results() {
    let dir = tempfile::tempdir().unwrap();
    let mut eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("rr", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("rr", AtomInput::new("turn", "alpha beta gamma delta"))
        .unwrap();
    eng.remember("rr", AtomInput::new("turn", "zeta eta theta"))
        .unwrap();
    eng.remember("rr", AtomInput::new("turn", "beta gamma"))
        .unwrap();

    // Proves set_reranker -> recall uses the cross-encoder path (overlap wins).
    eng.set_reranker(
        Arc::new(crate::embed::MockReranker),
        RerankStrategy::Replace,
    );
    let hits = eng
        .recall("rr", RecallQuery::by_text("alpha beta gamma delta", 3))
        .unwrap();
    assert_eq!(hits[0].text, "alpha beta gamma delta", "best overlap first");
    assert!(hits[0].score >= hits[1].score, "scores descending");
}

#[test]
fn fetch_last_returns_highest_id_of_kind() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert!(
        eng.fetch_last("r", "audit").unwrap().is_none(),
        "no atoms yet"
    );

    eng.remember("r", AtomInput::new("audit", "first")).unwrap();
    let second = eng
        .remember("r", AtomInput::new("audit", "second"))
        .unwrap();
    eng.remember("r", AtomInput::new("note", "unrelated"))
        .unwrap();

    let last = eng.fetch_last("r", "audit").unwrap().unwrap();
    assert_eq!(last.id, second);
    assert_eq!(last.text, "second");
}

#[test]
fn delete_atoms_removes_rows_and_incident_edges() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("r", AtomInput::new("note", "a")).unwrap();
    let b = eng.remember("r", AtomInput::new("note", "b")).unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    let report = eng.delete_atoms("r", &[a]).unwrap();
    assert_eq!(report.removed, 1);
    assert!(
        eng.fetch_one("r", a).unwrap().is_none(),
        "target is deleted"
    );
    assert!(
        eng.fetch_one("r", b).unwrap().is_some(),
        "other atom survives"
    );
    assert!(
        eng.fetch_edges(Some(a), None, None).unwrap().is_empty(),
        "incident edge is removed with the atom"
    );
}

#[test]
fn delete_atoms_force_deletes_immutable_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let t = eng
        .remember("r", AtomInput::new("llm_trace", "resp").immutable())
        .unwrap();
    assert_eq!(eng.delete_atoms("r", &[t]).unwrap().removed, 1);
    assert!(eng.fetch_one("r", t).unwrap().is_none());
}

#[test]
fn delete_atoms_empty_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    assert_eq!(eng.delete_atoms("r", &[]).unwrap().removed, 0);
}

#[test]
fn delete_atoms_is_region_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    // same dim+metric -> shared atoms table, separated only by region_id.
    eng.create_region("a", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("b", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let in_b = eng.remember("b", AtomInput::new("note", "keep")).unwrap();

    eng.delete_atoms("a", &[in_b]).unwrap();
    assert!(
        eng.fetch_one("b", in_b).unwrap().is_some(),
        "an id from another region is never matched"
    );
}

#[test]
fn delete_atoms_region_scope_preserves_foreign_edges() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("a", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("b", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("a", AtomInput::new("note", "a")).unwrap();
    let b = eng.remember("b", AtomInput::new("note", "b")).unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    // edges live in one global table, so the no-match delete leaves a->b intact.
    eng.delete_atoms("a", &[b]).unwrap();
    assert!(
        eng.fetch_one("b", b).unwrap().is_some(),
        "foreign atom survives"
    );
    assert_eq!(
        eng.fetch_edges(Some(a), Some(b), None).unwrap().len(),
        1,
        "an edge to a foreign-region atom is not deleted"
    );
}

#[test]
fn delete_atoms_removes_all_listed_ids_not_just_first() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("r", AtomInput::new("note", "a")).unwrap();
    let b = eng.remember("r", AtomInput::new("note", "b")).unwrap();
    let c = eng.remember("r", AtomInput::new("note", "c")).unwrap();
    let keep = eng.remember("r", AtomInput::new("note", "keep")).unwrap();

    let report = eng.delete_atoms("r", &[a, b, c]).unwrap();
    assert_eq!(
        report.removed, 3,
        "every listed id is deleted, not just the first"
    );
    for id in [a, b, c] {
        assert!(
            eng.fetch_one("r", id).unwrap().is_none(),
            "atom {id} in the multi-id list is deleted"
        );
    }
    assert!(
        eng.fetch_one("r", keep).unwrap().is_some(),
        "an unlisted atom survives a multi-id delete"
    );
}

// --- Per-region cryptographic erasure (crate-internal, reaches the key store) ---

/// The headline adversary test: an adversary holding the passphrase (and thus the
/// REK, key file, and full DB image) recovers a sealed atom BEFORE forget by reading
/// the LIVE slot, unwrapping the RCK, deriving the seal keys, and opening the blob;
/// AFTER forget the slot is tombstoned, so the RCK cannot be unwrapped and the
/// (still-present) sealed bytes are permanently undecryptable.
#[test]
fn adversary_recovers_before_forget_then_fails_after() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let secret = "classified: rendezvous at the old pier at dawn";
    let atom_id = eng.remember("s", AtomInput::new("fact", secret)).unwrap();

    // The exact ciphertext an adversary sees: the page-decrypted `sealed` column.
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT sealed, key_slot FROM {table} WHERE id = $1"),
            &[Value::Integer(atom_id)],
        )
        .unwrap();
    let sealed = match &qr.rows[0][0] {
        Value::Blob(b) => b.clone(),
        other => panic!("sealed column is not a blob: {other:?}"),
    };
    let atom_slot = match &qr.rows[0][1] {
        Value::Integer(s) => *s as u32,
        other => panic!("key_slot is not an integer: {other:?}"),
    };
    let row = eng.load_region_row(&conn, "s").unwrap().unwrap();
    drop(conn);
    let slot = row.rsk_slot.expect("encrypted region records a key slot");

    // Adversary BEFORE forget: RCK -> atom-wrap key -> unwrap the atom's ACK -> open.
    let rec = db.region_store_slot(slot).unwrap();
    assert_eq!(rec.state, SlotState::Live);
    let mut rck = db.unwrap_region_key(&rec.wrapped).unwrap();
    let atom_wrap = derive_atom_wrap_key(&rck);
    rck.zeroize();
    let atom_rec = db.atom_store_slot(atom_slot).unwrap();
    let ack = atom_wrap.unwrap_atom_key(&atom_rec.wrapped).unwrap();
    let seal = derive_seal_keys(&ack);
    let blob = blob_seal::open(&seal, atom_id as u64, &sealed).unwrap();
    let (_emb, text, _payload) = decode_atom_blob(&blob).unwrap();
    assert_eq!(
        text, secret,
        "adversary with the passphrase recovers content while the region is live"
    );
    drop(seal); // the legitimate session ends; only on-disk state remains

    eng.drop_region("s").unwrap();

    // After forget: slot tombstoned -> RCK gone -> sealed bytes undecryptable.
    let rec2 = db.region_store_slot(slot).unwrap();
    assert_eq!(
        rec2.state,
        SlotState::Tombstone,
        "forget tombstones the key slot"
    );
    assert!(
        db.unwrap_region_key(&rec2.wrapped).is_err(),
        "the destroyed (zeroed) wrapped key cannot be unwrapped, so the RCK is gone"
    );
}

/// Per-atom adversary: forgetting ONE atom tombstones only its key slot, so the captured
/// sealed bytes become permanently undecryptable, while the region key and a sibling
/// atom's key are untouched and the sibling still decrypts.
#[test]
fn forget_atom_destroys_only_its_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let secret = "the vault combination is 19-77-42";
    let target = eng.remember("s", AtomInput::new("fact", secret)).unwrap();
    let sibling = eng
        .remember("s", AtomInput::new("fact", "an ordinary sibling memory"))
        .unwrap();

    // Capture the adversary's view of the target: sealed bytes + its key slot.
    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT sealed, key_slot FROM {table} WHERE id = $1"),
            &[Value::Integer(target)],
        )
        .unwrap();
    let sealed = match &qr.rows[0][0] {
        Value::Blob(b) => b.clone(),
        other => panic!("sealed column is not a blob: {other:?}"),
    };
    let target_slot = match &qr.rows[0][1] {
        Value::Integer(s) => *s as u32,
        other => panic!("key_slot is not an integer: {other:?}"),
    };
    let row = eng.load_region_row(&conn, "s").unwrap().unwrap();
    drop(conn);
    let region_slot = row.rsk_slot.expect("encrypted region records a key slot");

    let rec = db.region_store_slot(region_slot).unwrap();
    let mut rck = db.unwrap_region_key(&rec.wrapped).unwrap();
    let atom_wrap = derive_atom_wrap_key(&rck);
    rck.zeroize();

    // BEFORE forget: unwrap the target's ACK and recover its plaintext.
    let target_rec = db.atom_store_slot(target_slot).unwrap();
    assert_eq!(target_rec.state, SlotState::Live);
    let ack = atom_wrap.unwrap_atom_key(&target_rec.wrapped).unwrap();
    let blob = blob_seal::open(&derive_seal_keys(&ack), target as u64, &sealed).unwrap();
    assert_eq!(decode_atom_blob(&blob).unwrap().1, secret);

    eng.forget_atom("s", target).unwrap();

    // After forget: target's ACK destroyed, sealed bytes undecryptable; region key untouched.
    let target_rec2 = db.atom_store_slot(target_slot).unwrap();
    assert_eq!(
        target_rec2.state,
        SlotState::Tombstone,
        "forget_atom tombstones the atom's key slot"
    );
    assert!(
        atom_wrap.unwrap_atom_key(&target_rec2.wrapped).is_err(),
        "the destroyed ACK cannot be unwrapped"
    );
    assert_eq!(
        target_rec2.wrapped,
        [0u8; citadel_core::WRAPPED_KEY_SIZE],
        "the wrapped ACK is explicitly zeroed on tombstone, not just made un-unwrappable by chance"
    );
    assert_eq!(
        db.region_store_slot(region_slot).unwrap().state,
        SlotState::Live,
        "forget_atom leaves the region key untouched"
    );
    assert!(
        eng.fetch_one("s", sibling).unwrap().is_some(),
        "the sibling atom still decrypts after the target is forgotten"
    );
}

/// A region whose key was destroyed while its row survives (the crash window between
/// key-destroy and row-delete) must refuse to attach with `RegionForgotten`.
#[test]
fn attaching_a_forgotten_region_yields_region_forgotten() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("s", AtomInput::new("fact", "x")).unwrap();

    let conn = Connection::open(&db).unwrap();
    let slot = eng
        .load_region_row(&conn, "s")
        .unwrap()
        .unwrap()
        .rsk_slot
        .unwrap();
    drop(conn);

    // Destroy only the key, leaving the region row intact.
    db.region_store_tombstone(slot, region_id as u64).unwrap();

    // A fresh engine (empty in-process cache) must refuse to attach the region.
    let eng2 = MemoryEngine::open(db).unwrap();
    let err = eng2
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap_err();
    assert!(
        matches!(err, MemError::RegionForgotten(_)),
        "attaching a region whose key was destroyed must yield RegionForgotten, got: {err}"
    );
}

/// Multi-tenant residue isolation: forgetting one region must not let an adversary
/// (full secrets) recover it via any SIBLING region's still-live key, and the
/// forgotten region's wrapped key leaves no residue in the sidecar.
#[test]
fn adversary_full_image_reconstruction_with_live_siblings() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("victim", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_encrypted_region("survivor", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let survivor_secret = "survivor: lunch is at noon";
    let victim_atom = eng
        .remember(
            "victim",
            AtomInput::new("fact", "victim: vault code 31-7-19"),
        )
        .unwrap();
    eng.remember("survivor", AtomInput::new("fact", survivor_secret))
        .unwrap();

    // Capture the adversary's view BEFORE forget: victim's sealed ciphertext, its slot,
    // and its 40-byte wrapped key.
    let vtable = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT sealed FROM {vtable} WHERE id = $1"),
            &[Value::Integer(victim_atom)],
        )
        .unwrap();
    let victim_sealed = match &qr.rows[0][0] {
        Value::Blob(b) => b.clone(),
        o => panic!("sealed not blob: {o:?}"),
    };
    let victim_slot = eng
        .load_region_row(&conn, "victim")
        .unwrap()
        .unwrap()
        .rsk_slot
        .unwrap();
    drop(conn);
    let victim_wrapped = db.region_store_slot(victim_slot).unwrap().wrapped;
    let sidecar_path = db.region_store_path();

    eng.drop_region("victim").unwrap();

    // drop_region deletes the victim's atom rows, it does not merely orphan them.
    let conn = Connection::open(&db).unwrap();
    let cnt = conn
        .query_params(
            &format!("SELECT COUNT(*) FROM {vtable} WHERE id = $1"),
            &[Value::Integer(victim_atom)],
        )
        .unwrap();
    assert_eq!(
        as_int(&cnt.rows[0][0]).unwrap(),
        0,
        "victim atom row is deleted by drop_region, not left orphaned"
    );
    drop(conn);

    // Adversary, post-forget: try EVERY slot's key against the captured victim blob.
    let mut recovered = false;
    for slot in 0..citadel_core::REGION_STORE_PREALLOC_SLOTS {
        let rec = db.region_store_slot(slot).unwrap();
        if rec.state != SlotState::Live {
            continue;
        }
        if let Ok(mut rck) = db.unwrap_region_key(&rec.wrapped) {
            let seal = derive_seal_keys(&rck);
            rck.zeroize();
            if blob_seal::open(&seal, victim_atom as u64, &victim_sealed).is_ok() {
                recovered = true;
            }
        }
    }
    assert!(
        !recovered,
        "no surviving sibling key opens the forgotten victim ciphertext"
    );

    // Residue carve: the victim's pre-forget wrapped key is absent from the sidecar.
    let sidecar = std::fs::read(&sidecar_path).unwrap();
    assert!(
        !byte_window(&sidecar, &victim_wrapped),
        "victim wrapped-key residue absent"
    );

    // Collateral-free: the sibling still recalls its secret.
    let hits = eng
        .recall("survivor", crate::RecallQuery::by_text(survivor_secret, 3))
        .unwrap();
    assert!(
        hits.iter().any(|h| h.text == survivor_secret),
        "survivor unaffected"
    );
}

/// Recycling a tombstoned slot for a new region must bind a FRESH key, surface none
/// of the old region's content, and leave the old ciphertext permanently unopenable.
#[test]
fn engine_slot_recycle_after_forget_isolates_old_region() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();

    eng.create_encrypted_region("r1", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let s1 = "r1 secret: the password is hunter2";
    let r1_atom = eng.remember("r1", AtomInput::new("fact", s1)).unwrap();

    let r1table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query_params(
            &format!("SELECT sealed FROM {r1table} WHERE id = $1"),
            &[Value::Integer(r1_atom)],
        )
        .unwrap();
    let r1_sealed = match &qr.rows[0][0] {
        Value::Blob(b) => b.clone(),
        o => panic!("{o:?}"),
    };
    let r1_slot = eng
        .load_region_row(&conn, "r1")
        .unwrap()
        .unwrap()
        .rsk_slot
        .unwrap();
    drop(conn);
    let r1_wrapped = db.region_store_slot(r1_slot).unwrap().wrapped;

    eng.drop_region("r1").unwrap();
    // The slot is tombstoned and its key is gone.
    let rec = db.region_store_slot(r1_slot).unwrap();
    assert_eq!(rec.state, SlotState::Tombstone);
    assert!(db.unwrap_region_key(&rec.wrapped).is_err());
    let sidecar = std::fs::read(db.region_store_path()).unwrap();
    assert!(
        !byte_window(&sidecar, &r1_wrapped),
        "r1 wrapped key gone after forget"
    );

    // R2 recycles the slot with a brand-new key.
    eng.create_encrypted_region("r2", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let r2_slot = eng
        .load_region_row(&conn, "r2")
        .unwrap()
        .unwrap()
        .rsk_slot
        .unwrap();
    drop(conn);
    assert_eq!(r2_slot, r1_slot, "R2 recycled R1's freed slot");
    let s2 = "r2 secret: the meeting is tuesday";
    eng.remember("r2", AtomInput::new("fact", s2)).unwrap();

    // R2 works on the recycled slot and shows nothing of R1.
    assert!(eng
        .recall("r2", crate::RecallQuery::by_text(s2, 5))
        .unwrap()
        .iter()
        .any(|h| h.text == s2));
    assert!(!eng
        .recall("r2", crate::RecallQuery::by_text(s1, 5))
        .unwrap()
        .iter()
        .any(|h| h.text == s1));

    // R2's fresh key cannot open R1's captured old ciphertext.
    let rec2 = db.region_store_slot(r2_slot).unwrap();
    let mut rck2 = db.unwrap_region_key(&rec2.wrapped).unwrap();
    let seal2 = derive_seal_keys(&rck2);
    rck2.zeroize();
    assert!(
        blob_seal::open(&seal2, r1_atom as u64, &r1_sealed).is_err(),
        "the recycled region's key must not open the forgotten region's ciphertext"
    );
}

#[test]
fn reconcile_reclaims_orphan_live_slot_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    {
        let _eng = MemoryEngine::open(db.clone()).unwrap();
        let (slot, _gen) = db
            .region_store_allocate_write(4242, &[0x7u8; citadel_core::WRAPPED_KEY_SIZE])
            .unwrap();
        assert_eq!(slot, 0, "first allocation lands in slot 0");
        assert_eq!(db.region_store_slot(0).unwrap().state, SlotState::Live);
    }
    let _eng2 = MemoryEngine::open(db.clone()).unwrap();
    assert_eq!(
        db.region_store_slot(0).unwrap().state,
        SlotState::Tombstone,
        "orphan LIVE slot reclaimed on open"
    );
}

/// An atom key slot left LIVE by an interrupted insert (key fsync'd, row never
/// committed) is reclaimed on the next open, mirroring the region-store reconcile.
#[test]
fn reconcile_reclaims_orphan_atom_live_slot_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let slot = {
        let _eng = MemoryEngine::open(db.clone()).unwrap();
        // Orphan: a LIVE atom key slot with no referencing atom row.
        let (slot, _gen) = db
            .atom_store_allocate_write(999, &[0x7u8; citadel_core::WRAPPED_KEY_SIZE])
            .unwrap();
        assert_eq!(db.atom_store_slot(slot).unwrap().state, SlotState::Live);
        slot
    };
    let _eng2 = MemoryEngine::open(db.clone()).unwrap();
    assert_eq!(
        db.atom_store_slot(slot).unwrap().state,
        SlotState::Tombstone,
        "orphan LIVE atom slot reclaimed on open"
    );
}

/// Deleting an atom invalidates the cached PRISM index so it is not re-ranked later.
/// (`region_handle` shares the cached `ann` Arc, so we can observe the cache here.)
#[test]
fn delete_atoms_invalidates_ann_cache() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("s", AtomInput::new("fact", "alpha one"))
        .unwrap();
    eng.remember("s", AtomInput::new("fact", "beta two"))
        .unwrap();

    // First recall builds and caches the ephemeral index.
    eng.recall("s", RecallQuery::by_text("alpha one", 2))
        .unwrap();
    assert!(
        eng.region_handle("s")
            .unwrap()
            .ann
            .read()
            .unwrap()
            .is_some(),
        "the first sealed recall caches a PRISM index"
    );

    eng.delete_atoms("s", &[a]).unwrap();
    assert!(
        eng.region_handle("s")
            .unwrap()
            .ann
            .read()
            .unwrap()
            .is_none(),
        "delete_atoms invalidates the cached index so erased atoms are not re-ranked"
    );
}

/// Dropping an encrypted region reclaims its atoms' key slots (tombstones + frees them),
/// rather than leaking them LIVE-but-dead in the atom key store.
#[test]
fn drop_region_reclaims_atom_key_slots() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("s", AtomInput::new("fact", "one")).unwrap();
    let b = eng.remember("s", AtomInput::new("fact", "two")).unwrap();

    let table = atoms_table(8, EmbeddingMetric::Cosine, true);
    let conn = Connection::open(&db).unwrap();
    let slot_of = |id: i64| -> u32 {
        let qr = conn
            .query_params(
                &format!("SELECT key_slot FROM {table} WHERE id = $1"),
                &[Value::Integer(id)],
            )
            .unwrap();
        as_int(&qr.rows[0][0]).unwrap() as u32
    };
    let (sa, sb) = (slot_of(a), slot_of(b));
    drop(conn);
    assert_eq!(db.atom_store_slot(sa).unwrap().state, SlotState::Live);
    assert_eq!(db.atom_store_slot(sb).unwrap().state, SlotState::Live);

    eng.drop_region("s").unwrap();

    assert_eq!(
        db.atom_store_slot(sa).unwrap().state,
        SlotState::Tombstone,
        "atom a's key slot is reclaimed, not leaked LIVE"
    );
    assert_eq!(
        db.atom_store_slot(sb).unwrap().state,
        SlotState::Tombstone,
        "atom b's key slot is reclaimed, not leaked LIVE"
    );
    assert!(
        db.atom_store_live_wrapped().unwrap().is_empty(),
        "no live atom keys remain after drop_region"
    );
}

/// The `_enc` table holds only the opaque `sealed` blob - no plaintext content column.
#[test]
fn enc_table_has_no_plaintext_content_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("enc", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.create_region("plain", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let enc = atoms_table(8, EmbeddingMetric::Cosine, true);
    let plain = atoms_table(8, EmbeddingMetric::Cosine, false);
    let conn = Connection::open(&db).unwrap();
    let columns = |t: &str| -> Vec<String> {
        conn.table_schema(t)
            .expect("table exists")
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect()
    };
    let has = |cols: &[String], c: &str| cols.iter().any(|n| n == c);
    let enc_cols = columns(&enc);
    let plain_cols = columns(&plain);
    assert!(
        has(&enc_cols, "sealed"),
        "sealed column present on the _enc table"
    );
    assert!(
        !has(&enc_cols, "text_content"),
        "_enc must NOT have text_content"
    );
    assert!(!has(&enc_cols, "payload"), "_enc must NOT have payload");
    assert!(!has(&enc_cols, "embedding"), "_enc must NOT have embedding");
    assert!(
        has(&plain_cols, "text_content"),
        "plaintext table keeps text_content"
    );
    assert!(
        has(&plain_cols, "embedding"),
        "plaintext table keeps embedding"
    );
    assert!(has(&plain_cols, "payload"), "plaintext table keeps payload");
}

#[test]
fn open_rejects_incompatible_memory_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    {
        let conn = Connection::open(&db).unwrap();
        let res = conn.execute_script(
            "CREATE TABLE memory_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);\
             CREATE TABLE memory_regions (\
             id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, embedding_dim INTEGER NOT NULL,\
             embedding_metric TEXT NOT NULL, model_id TEXT NOT NULL, kek_epoch INTEGER NOT NULL,\
             created_at TIMESTAMP NOT NULL, metadata JSONB);",
        );
        assert!(res.error.is_none(), "schema setup failed: {:?}", res.error);
    }
    match MemoryEngine::open(db) {
        Ok(_) => panic!("incompatible schema must be rejected, but open succeeded"),
        Err(MemError::Invalid(m)) => {
            assert!(
                m.contains("incompatible memory schema"),
                "wrong rejection message: {m}"
            )
        }
        Err(e) => panic!("expected MemError::Invalid, got: {e:?}"),
    }
}

#[test]
fn json_contains_edge_cases() {
    use serde_json::json;
    // nested-object subset
    assert!(json_contains(
        &json!({"a": {"b": 1, "c": 2}}),
        &json!({"a": {"b": 1}})
    ));
    assert!(!json_contains(
        &json!({"a": {"b": 1}}),
        &json!({"a": {"b": 2}})
    ));
    // scalar contained in array
    assert!(json_contains(&json!([1, 2, 3]), &json!(2)));
    assert!(!json_contains(&json!([1, 2, 3]), &json!(9)));
    // order-independent array subset
    assert!(json_contains(&json!([1, 2, 3]), &json!([3, 1])));
    assert!(!json_contains(&json!([1, 2, 3]), &json!([3, 9])));
    // array of objects
    assert!(json_contains(
        &json!([{"x": 1}, {"y": 2}]),
        &json!([{"x": 1}])
    ));
    // empty needle is vacuously contained
    assert!(json_contains(&json!({"a": 1}), &json!({})));
    assert!(json_contains(&json!([1, 2]), &json!([])));
    // type mismatch and scalar equality
    assert!(!json_contains(&json!({"a": 1}), &json!([1])));
    assert!(json_contains(&json!("x"), &json!("x")));
    assert!(!json_contains(&json!("x"), &json!("y")));
}

#[test]
fn encode_decode_atom_blob_roundtrip_and_malformed() {
    let emb = vec![1.5f32, -2.0, 3.25];
    let blob = encode_atom_blob(&emb, "hello text", "{\"k\":1}");
    let (e, t, p) = decode_atom_blob(&blob).unwrap();
    assert_eq!(e, emb);
    assert_eq!(t, "hello text");
    assert_eq!(p, "{\"k\":1}");

    // empty everything round-trips
    let b0 = encode_atom_blob(&[], "", "");
    let (e0, t0, p0) = decode_atom_blob(&b0).unwrap();
    assert!(e0.is_empty() && t0.is_empty() && p0.is_empty());

    // malformed inputs return Err and never panic / over-allocate
    assert!(decode_atom_blob(&blob[..blob.len() - 1]).is_err());
    assert!(decode_atom_blob(&[]).is_err());
    assert!(decode_atom_blob(&[0x01]).is_err()); // partial dim header

    // invalid UTF-8 text
    let mut bad = Vec::new();
    bad.extend_from_slice(&0u16.to_le_bytes()); // dim 0
    bad.extend_from_slice(&2u32.to_le_bytes()); // text len 2
    bad.extend_from_slice(&[0xff, 0xff]); // not UTF-8
    bad.extend_from_slice(&0u32.to_le_bytes()); // payload len 0
    assert!(decode_atom_blob(&bad).is_err());

    // a huge length prefix is rejected by bounds check, not by a 4 GiB allocation
    let mut huge = Vec::new();
    huge.extend_from_slice(&0u16.to_le_bytes());
    huge.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
    huge.extend_from_slice(&[1, 2, 3]);
    assert!(decode_atom_blob(&huge).is_err());
}

#[test]
fn vec_distance_matches_sql_metrics() {
    // L2 = sqrt(sum sq): [3,4] vs [0,0] -> 5
    assert!((vec_distance(EmbeddingMetric::L2, &[3.0, 4.0], &[0.0, 0.0]) - 5.0).abs() < 1e-5);
    // Inner = -dot: -([1,2].[3,4]) = -11
    assert!(
        (vec_distance(EmbeddingMetric::InnerProduct, &[1.0, 2.0], &[3.0, 4.0]) - (-11.0)).abs()
            < 1e-5
    );
    // Cosine: identical -> 0, orthogonal -> 1
    assert!(vec_distance(EmbeddingMetric::Cosine, &[1.0, 0.0], &[1.0, 0.0]).abs() < 1e-6);
    assert!((vec_distance(EmbeddingMetric::Cosine, &[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-6);
    // Cosine zero-norm -> f32::MAX (the worst rank), NOT NaN
    let d = vec_distance(EmbeddingMetric::Cosine, &[3.0, 4.0], &[0.0, 0.0]);
    assert_eq!(d, f32::MAX);
    assert!(!d.is_nan());
}

/// True if `needle` occurs as a contiguous window of `hay`.
fn byte_window(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

fn set_atom_age_and_access(
    db: &Arc<Database>,
    dim: u16,
    region_id: i64,
    atom_id: AtomId,
    created_micros: i64,
    access_count: i64,
) {
    let table = atoms_table(dim, EmbeddingMetric::Cosine, false);
    let conn = citadel_sql::Connection::open(db).unwrap();
    conn.execute_params(
        &format!(
            "UPDATE {table} SET created_at = $1, access_count = $2 \
             WHERE id = $3 AND region_id = $4"
        ),
        &[
            citadel_sql::Value::Timestamp(created_micros),
            citadel_sql::Value::Integer(access_count),
            citadel_sql::Value::Integer(atom_id),
            citadel_sql::Value::Integer(region_id),
        ],
    )
    .unwrap();
}

fn micros_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

#[test]
fn evolve_score_pins_recency_decay_and_access_boost() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_region("ev", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng.remember("ev", AtomInput::new("note", "alpha")).unwrap();

    let age_micros = 30i64 * 86_400 * 1_000_000;
    set_atom_age_and_access(&db, 8, region_id, a, micros_now() - age_micros, 19);

    let report = eng.evolve("ev", a, 0, 10.0).unwrap();
    assert_eq!(report.links_added, 0, "self is filtered, no neighbors");

    let recency = 0.5f32;
    let expected = recency * (1.0 + (19f32).ln_1p());
    assert!(
        (report.score - expected).abs() < 1e-3,
        "score {} should equal recency*ln1p boost {}",
        report.score,
        expected
    );
    assert!(
        (expected - 1.997_86).abs() < 1e-2,
        "sanity: expected near 1.9979, got {expected}"
    );
}

#[test]
fn evolve_score_zero_age_no_access_is_unity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let region_id = eng
        .create_region("ev0", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("ev0", AtomInput::new("note", "alpha"))
        .unwrap();

    set_atom_age_and_access(&db, 8, region_id, a, micros_now(), 0);
    let report = eng.evolve("ev0", a, 0, 10.0).unwrap();
    assert!(
        (report.score - 1.0).abs() < 1e-4,
        "fresh, never-accessed atom scores 1.0, got {}",
        report.score
    );
}

#[test]
fn evolve_links_nearest_neighbor_with_inverse_distance_weight() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_region("evw", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let a = eng
        .remember("evw", AtomInput::new("note", "red green blue"))
        .unwrap();
    let b = eng
        .remember("evw", AtomInput::new("note", "red green yellow"))
        .unwrap();

    let emb = embed_one(
        &*eng.region_handle("evw").unwrap().embedder,
        "red green blue",
    )
    .unwrap();
    let hits = eng
        .recall("evw", RecallQuery::by_embedding(emb, 2))
        .unwrap();
    let d = hits
        .iter()
        .find(|h| h.id == b)
        .expect("b is recalled")
        .distance;
    assert!(
        d > 0.0 && d < 1.0,
        "distance must be a proper fraction: {d}"
    );

    let report = eng.evolve("evw", a, 1, 10.0).unwrap();
    assert_eq!(report.links_added, 1, "exactly one neighbor linked");

    let edges = eng.fetch_edges(Some(a), None, None).unwrap();
    assert_eq!(edges.len(), 1, "one outgoing edge");
    assert_eq!(edges[0].dst_id, b, "edge points at the neighbor");
    assert_eq!(edges[0].kind, EdgeKind::DerivedFrom);

    let expected = 1.0f32 / (1.0 + d);
    assert!(
        (edges[0].weight - expected).abs() < 1e-5,
        "weight {} should be 1/(1+dist) = {}",
        edges[0].weight,
        expected
    );
    assert!(
        edges[0].weight < 1.0 && edges[0].weight > 0.5,
        "inverse-distance weight is a proper fraction above 0.5: {}",
        edges[0].weight
    );
}

#[test]
fn evolve_retain_requires_both_id_and_distance() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_region("evr", Arc::new(MockEmbedder::new(64)))
        .unwrap();
    let a = eng
        .remember("evr", AtomInput::new("note", "red green blue"))
        .unwrap();
    let _b = eng
        .remember("evr", AtomInput::new("note", "alpha beta gamma"))
        .unwrap();

    let report = eng.evolve("evr", a, 5, -1.0).unwrap();
    assert_eq!(
        report.links_added, 0,
        "AND filter drops everything when the distance bound excludes all"
    );
    let edges = eng.fetch_edges(Some(a), None, None).unwrap();
    assert!(
        edges.is_empty(),
        "no edges created under the distance bound"
    );
}

#[test]
fn vec_distance_l2_uses_difference_not_sum() {
    let d = vec_distance(EmbeddingMetric::L2, &[1.0, 2.0], &[5.0, 10.0]);
    assert!(
        (d - 80.0_f32.sqrt()).abs() < 1e-3,
        "L2 = sqrt(80) ~ 8.944, not sqrt(180)"
    );
}

#[test]
fn vec_distance_cosine_divides_by_denominator() {
    let d = vec_distance(EmbeddingMetric::Cosine, &[1.0, 1.0], &[1.0, 0.0]);
    let expected = 1.0_f32 - 1.0 / 2.0_f32.sqrt();
    assert!(
        (d - expected).abs() < 1e-3,
        "cosine = 1 - 1/sqrt(2) ~ 0.293, not 1 - sqrt(2)"
    );
}

#[test]
fn dist_value_coerces_integer_to_f32() {
    assert_eq!(dist_value(&Value::Integer(-7)), -7.0_f32);
    assert_eq!(dist_value(&Value::Integer(42)), 42.0_f32);
}

#[test]
fn query_keyword_terms_tokenizes_lowercases_sorts_dedups() {
    assert_eq!(
        query_keyword_terms(Some("Beta alpha Beta gamma")),
        vec![
            String::from("alpha"),
            String::from("beta"),
            String::from("gamma")
        ]
    );
    assert_eq!(query_keyword_terms(None), Vec::<String>::new());
}

#[test]
fn bm25_idf_rewards_rare_terms_and_handles_tokenization() {
    use crate::fusion::Candidate;
    let mk = |id, text: &str| Candidate {
        id,
        kind: "fact".into(),
        text: text.into(),
        payload: serde_json::Value::Null,
        dist: 0.0,
        text_rank: 0.0,
        importance: 0.0,
        created_micros: 0,
        immutable: false,
    };
    // 'common' is in all three (low IDF); 'zebra' is in one (high IDF). The query has
    // mixed case + punctuation to exercise UAX#29 tokenization.
    let mut cands = vec![
        mk(1, "common zebra here"),
        mk(2, "common word two"),
        mk(3, "common word three"),
    ];
    assign_bm25_ranks(&mut cands, &query_keyword_terms(Some("Common, ZEBRA!")));
    assert!(
        cands[0].text_rank > cands[1].text_rank,
        "the rare-term match outscores common-only matches via IDF"
    );
    assert_eq!(
        cands[1].text_rank, cands[2].text_rank,
        "candidates matching only the pool-common term tie"
    );
    assert!(
        cands[1].text_rank > 0.0,
        "a pool-common term still carries a small positive IDF (Lucene +1 form)"
    );
    // No query terms or no candidates leaves ranks untouched.
    let mut none = vec![mk(9, "anything")];
    assign_bm25_ranks(&mut none, &query_keyword_terms(None));
    assert_eq!(none[0].text_rank, 0.0);
}

#[test]
fn graph_expand_plaintext_depth1_score_is_exactly_half() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("g", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("g", AtomInput::new("fact", "alpha unique one"))
        .unwrap();
    let b = eng
        .remember("g", AtomInput::new("fact", "beta unique two"))
        .unwrap();
    let c = eng
        .remember("g", AtomInput::new("fact", "gamma unique three"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();
    eng.link(b, c, EdgeKind::DerivedFrom, 1.0).unwrap();

    let hits = eng
        .recall(
            "g",
            RecallQuery::by_text("alpha unique one", 1)
                .with_graph_expand(GraphExpand::new(2, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let score_b = hits
        .iter()
        .find(|h| h.id == b)
        .map(|h| h.score)
        .expect("1-hop atom reached");
    let score_c = hits
        .iter()
        .find(|h| h.id == c)
        .map(|h| h.score)
        .expect("2-hop atom reached");
    assert_eq!(score_b, 0.5_f32, "depth-1 graph score is 1/(1+1)");
    assert_eq!(
        score_c,
        1.0_f32 / (2.0_f32 + 1.0),
        "depth-2 graph score is 1/(2+1)"
    );
    assert!(score_c.is_finite() && score_c < score_b);
}

#[test]
fn graph_expand_plaintext_depth_zero_returns_no_reached_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("g0", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("g0", AtomInput::new("fact", "alpha unique one"))
        .unwrap();
    let b = eng
        .remember("g0", AtomInput::new("fact", "beta unique two"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    let hits = eng
        .recall(
            "g0",
            RecallQuery::by_text("alpha unique one", 1)
                .with_graph_expand(GraphExpand::new(0, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let ids: Vec<i64> = hits.iter().map(|h| h.id).collect();
    assert!(ids.contains(&a), "seed survives recall");
    assert!(
        !ids.contains(&b),
        "depth 0 must short-circuit: no neighbor is expanded"
    );
}

#[test]
fn graph_expand_sealed_depth1_score_is_exactly_half() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("sg", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("sg", AtomInput::new("fact", "alpha unique one"))
        .unwrap();
    let b = eng
        .remember("sg", AtomInput::new("fact", "beta unique two"))
        .unwrap();
    let c = eng
        .remember("sg", AtomInput::new("fact", "gamma unique three"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();
    eng.link(b, c, EdgeKind::DerivedFrom, 1.0).unwrap();

    let hits = eng
        .recall(
            "sg",
            RecallQuery::by_text("alpha unique one", 1)
                .with_graph_expand(GraphExpand::new(2, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let score_b = hits
        .iter()
        .find(|h| h.id == b)
        .map(|h| h.score)
        .expect("1-hop sealed atom reached");
    let score_c = hits
        .iter()
        .find(|h| h.id == c)
        .map(|h| h.score)
        .expect("2-hop sealed atom reached");
    assert_eq!(score_b, 0.5_f32, "sealed depth-1 graph score is 1/(1+1)");
    assert_eq!(
        score_c,
        1.0_f32 / (2.0_f32 + 1.0),
        "sealed depth-2 graph score is 1/(2+1)"
    );
    assert_eq!(
        hits.iter().find(|h| h.id == b).unwrap().text,
        "beta unique two"
    );
}

#[test]
fn graph_expand_sealed_depth_zero_returns_no_reached_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("sg0", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let a = eng
        .remember("sg0", AtomInput::new("fact", "alpha unique one"))
        .unwrap();
    let b = eng
        .remember("sg0", AtomInput::new("fact", "beta unique two"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    let hits = eng
        .recall(
            "sg0",
            RecallQuery::by_text("alpha unique one", 1)
                .with_graph_expand(GraphExpand::new(0, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    let ids: Vec<i64> = hits.iter().map(|h| h.id).collect();
    assert!(ids.contains(&a), "seed survives sealed recall");
    assert!(
        !ids.contains(&b),
        "sealed depth 0 must short-circuit: no neighbor is expanded"
    );
}

#[test]
fn parse_candidate_rejects_short_row() {
    let row = vec![Value::Integer(1); 8];
    assert!(
        matches!(parse_candidate(&row), Err(MemError::Invalid(ref m)) if m.contains("recall row shape")),
        "row of len 8 (< 9) must be rejected by the length guard, not a later type error"
    );
    let ok = vec![
        Value::Integer(1),
        Value::Text("fact".into()),
        Value::Null,
        Value::Null,
        Value::Real(0.5),
        Value::Timestamp(0),
        Value::Real(0.1),
        Value::Real(0.0),
        Value::Integer(0),
    ];
    assert!(parse_candidate(&ok).is_ok(), "row of len 9 must parse");
}

#[test]
fn parse_fetched_rejects_short_row() {
    let row = vec![Value::Integer(1); 5];
    assert!(
        matches!(parse_fetched(&row), Err(MemError::Invalid(ref m)) if m.contains("fetch row shape")),
        "row of len 5 (< 6) must be rejected by the length guard, not a later type error"
    );
    let ok = vec![
        Value::Integer(1),
        Value::Text("fact".into()),
        Value::Null,
        Value::Null,
        Value::Real(0.5),
        Value::Integer(1),
    ];
    assert!(parse_fetched(&ok).is_ok(), "row of len 6 must parse");
}

#[test]
fn parse_edge_rejects_short_row() {
    let row = vec![Value::Integer(1); 4];
    assert!(
        matches!(parse_edge(&row), Err(MemError::Invalid(_))),
        "row of len 4 (< 5) must be rejected"
    );
    let ok = vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Text("causes".into()),
        Value::Real(1.0),
        Value::Null,
    ];
    assert!(parse_edge(&ok).is_ok(), "row of len 5 must parse");
}

#[test]
fn as_f32_coerces_integer() {
    assert_eq!(as_f32(&Value::Integer(7)), 7.0f32);
    assert_eq!(as_f32(&Value::Real(2.5)), 2.5f32);
    assert_eq!(as_f32(&Value::Null), 0.0f32);
}

#[test]
fn as_ts_coerces_integer() {
    assert_eq!(as_ts(&Value::Integer(123)), 123i64);
    assert_eq!(as_ts(&Value::Timestamp(456)), 456i64);
    assert_eq!(as_ts(&Value::Null), 0i64);
}

#[test]
fn recall_fusion_arm_uses_reranker_replace_scores() {
    let dir = tempfile::tempdir().unwrap();
    let mut eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("fa", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("fa", AtomInput::new("turn", "alpha beta gamma"))
        .unwrap();
    eng.remember("fa", AtomInput::new("turn", "delta epsilon"))
        .unwrap();

    eng.set_reranker(
        Arc::new(crate::embed::MockReranker),
        RerankStrategy::Replace,
    );
    let hits = eng
        .recall("fa", RecallQuery::by_text("alpha beta gamma", 2))
        .unwrap();
    assert_eq!(hits[0].text, "alpha beta gamma");
    assert_eq!(
        hits[0].score, 3.0_f32,
        "Replace score is the raw overlap count"
    );
    assert_eq!(hits[1].text, "delta epsilon");
    assert_eq!(hits[1].score, 0.0_f32);
}

#[test]
fn recall_sealed_fusion_arm_uses_reranker_replace_scores() {
    let dir = tempfile::tempdir().unwrap();
    let mut eng = MemoryEngine::open(create_enc_db(dir.path())).unwrap();
    eng.create_encrypted_region("fas", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("fas", AtomInput::new("turn", "alpha beta gamma"))
        .unwrap();
    eng.remember("fas", AtomInput::new("turn", "delta epsilon"))
        .unwrap();

    eng.set_reranker(
        Arc::new(crate::embed::MockReranker),
        RerankStrategy::Replace,
    );
    let hits = eng
        .recall("fas", RecallQuery::by_text("alpha beta gamma", 2))
        .unwrap();
    assert_eq!(hits[0].text, "alpha beta gamma");
    assert_eq!(
        hits[0].score, 3.0_f32,
        "sealed Replace score is the raw overlap count"
    );
    assert_eq!(hits[1].text, "delta epsilon");
    assert_eq!(hits[1].score, 0.0_f32);
}

#[test]
fn set_reranker_changes_recall_score_from_fusion() {
    let dir = tempfile::tempdir().unwrap();
    let mut eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("sr", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    eng.remember("sr", AtomInput::new("turn", "alpha beta gamma"))
        .unwrap();

    let before = eng
        .recall("sr", RecallQuery::by_text("alpha beta gamma", 1))
        .unwrap();
    assert!(
        before[0].score <= 1.0_f32,
        "fusion score is a normalized blend"
    );

    eng.set_reranker(
        Arc::new(crate::embed::MockReranker),
        RerankStrategy::Replace,
    );
    let after = eng
        .recall("sr", RecallQuery::by_text("alpha beta gamma", 1))
        .unwrap();
    assert_eq!(
        after[0].score, 3.0_f32,
        "reranker took effect: Replace overlap score"
    );
}

#[test]
fn remember_batch_returns_contiguous_ids_after_single() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    eng.create_region("rb", Arc::new(MockEmbedder::new(8)))
        .unwrap();
    let first = eng.remember("rb", AtomInput::new("note", "seed")).unwrap();

    let ids = eng
        .remember_batch(
            "rb",
            vec![
                AtomInput::new("note", "a"),
                AtomInput::new("note", "b"),
                AtomInput::new("note", "c"),
            ],
        )
        .unwrap();
    assert_eq!(
        ids,
        vec![first + 1, first + 2, first + 3],
        "batch ids are start + offset, contiguous after the prior single insert"
    );
}

#[test]
fn attach_region_key_rejects_stale_generation_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    let conn = Connection::open(&db).unwrap();
    let row = eng.load_region_row(&conn, "s").unwrap().unwrap();
    drop(conn);

    let stale = RegionRow {
        id,
        dim: row.dim,
        metric: row.metric,
        model_id: row.model_id.clone(),
        encrypted: row.encrypted,
        rsk_slot: row.rsk_slot,
        rsk_gen: Some(row.rsk_gen.unwrap() + 1),
    };
    assert!(
        matches!(
            eng.attach_region_key("s", &stale),
            Err(MemError::RegionForgotten(_))
        ),
        "a generation mismatch alone (state Live, owner matching) must be RegionForgotten"
    );
}

#[test]
fn attach_region_key_rejects_wrong_owner_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    let id = eng
        .create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    let conn = Connection::open(&db).unwrap();
    let row = eng.load_region_row(&conn, "s").unwrap().unwrap();
    drop(conn);

    let mut rck = [0x5au8; citadel_core::KEY_SIZE];
    let wrapped = db.wrap_region_key(&rck).unwrap();
    rck.zeroize();
    let other_owner = id as u64 + 1000;
    let (slot, gen) = db
        .region_store_allocate_write(other_owner, &wrapped)
        .unwrap();

    let wrong_owner = RegionRow {
        id,
        dim: row.dim,
        metric: row.metric,
        model_id: row.model_id.clone(),
        encrypted: row.encrypted,
        rsk_slot: Some(slot),
        rsk_gen: Some(gen),
    };
    assert!(
        matches!(
            eng.attach_region_key("s", &wrong_owner),
            Err(MemError::RegionForgotten(_))
        ),
        "an owner mismatch alone (state Live, generation matching) must be RegionForgotten"
    );
}

#[test]
fn attach_region_key_rejects_non_live_slot_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_enc_db(dir.path());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("s", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    let empty_slot = 1u32;
    let rec = db.region_store_slot(empty_slot).unwrap();
    assert_eq!(
        rec.state,
        SlotState::Empty,
        "an unallocated prealloc slot is Empty"
    );
    assert_eq!(rec.region_id, 0);
    assert_eq!(rec.gen, 0);

    let non_live = RegionRow {
        id: 0,
        dim: 8,
        metric: EmbeddingMetric::Cosine,
        model_id: "mock".to_string(),
        encrypted: true,
        rsk_slot: Some(empty_slot),
        rsk_gen: Some(0),
    };
    assert!(
        matches!(
            eng.attach_region_key("s", &non_live),
            Err(MemError::RegionForgotten(_))
        ),
        "a non-Live slot alone (owner 0 and gen 0 both matching) must be RegionForgotten"
    );
}

#[test]
fn verify_matches_rejects_each_field_mismatch() {
    let row = RegionRow {
        id: 42,
        dim: 8,
        metric: EmbeddingMetric::Cosine,
        model_id: "mock".to_string(),
        encrypted: true,
        rsk_slot: Some(0),
        rsk_gen: Some(1),
    };

    assert!(
        row.verify_matches("r", 8, EmbeddingMetric::Cosine, "mock", true)
            .is_ok(),
        "an exact match verifies"
    );
    assert!(
        matches!(
            row.verify_matches("r", 16, EmbeddingMetric::Cosine, "mock", true),
            Err(MemError::DimMismatch {
                expected: 8,
                got: 16,
                ..
            })
        ),
        "dim mismatch must be rejected"
    );
    assert!(
        matches!(
            row.verify_matches("r", 8, EmbeddingMetric::L2, "mock", true),
            Err(MemError::MetricMismatch { .. })
        ),
        "metric mismatch must be rejected"
    );
    assert!(
        matches!(
            row.verify_matches("r", 8, EmbeddingMetric::Cosine, "other", true),
            Err(MemError::ModelMismatch { .. })
        ),
        "model mismatch must be rejected"
    );
    assert!(
        matches!(
            row.verify_matches("r", 8, EmbeddingMetric::Cosine, "mock", false),
            Err(MemError::Invalid(_))
        ),
        "encrypted-flag mismatch must be rejected"
    );
}

#[test]
fn check_attached_rejects_mismatch_against_cached_region() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(create_db(dir.path())).unwrap();
    let id = eng
        .create_region("notes", Arc::new(MockEmbedder::new(8)))
        .unwrap();

    assert_eq!(
        eng.check_attached("notes", 8, EmbeddingMetric::Cosine, "mock", false)
            .unwrap(),
        Some(id),
        "an exact match returns the cached id"
    );
    assert!(
        matches!(
            eng.check_attached("notes", 16, EmbeddingMetric::Cosine, "mock", false),
            Err(MemError::DimMismatch {
                expected: 8,
                got: 16,
                ..
            })
        ),
        "cached dim mismatch must error, not fall through to Ok(None)"
    );
    assert!(
        matches!(
            eng.check_attached("notes", 8, EmbeddingMetric::L2, "mock", false),
            Err(MemError::MetricMismatch { .. })
        ),
        "cached metric mismatch must error"
    );
    assert!(
        matches!(
            eng.check_attached("notes", 8, EmbeddingMetric::Cosine, "other", false),
            Err(MemError::ModelMismatch { .. })
        ),
        "cached model mismatch must error"
    );
    assert!(
        matches!(
            eng.check_attached("notes", 8, EmbeddingMetric::Cosine, "mock", true),
            Err(MemError::Invalid(_))
        ),
        "cached encrypted-flag mismatch must error"
    );
}
