//! Per-atom cryptographic erasure (Tier B): forgetting one atom destroys its
//! key and removes it from recall/fetch, while sibling atoms and the region
//! survive. The "captured sealed bytes become undecryptable" adversary test
//! lives in the crate's internal unit tests (engine_tests.rs), which can reach
//! the key store.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, AttestVerdict, MemoryEngine, MockEmbedder, RecallQuery};
use std::sync::Arc;

const DIM: usize = 16;

fn embedder() -> Arc<MockEmbedder> {
    Arc::new(MockEmbedder::new(DIM))
}

fn engine(dir: &std::path::Path) -> MemoryEngine {
    let db = DatabaseBuilder::new(dir.join("m.db"))
        .passphrase(b"pw")
        .enable_region_keys(true)
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    MemoryEngine::open(Arc::new(db)).unwrap()
}

#[test]
fn count_in_sealed_region_excludes_erased_atoms() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("r", embedder()).unwrap();

    let target = eng
        .remember("r", AtomInput::new("fact", "to be erased"))
        .unwrap();
    eng.remember("r", AtomInput::new("fact", "kept one"))
        .unwrap();
    eng.remember("r", AtomInput::new("fact", "kept two"))
        .unwrap();
    eng.remember("r", AtomInput::new("event", "other kind"))
        .unwrap();
    assert_eq!(eng.count("r", "fact").unwrap(), 3);
    assert_eq!(eng.count("r", "event").unwrap(), 1);

    // A crypto-erased atom still has a row; its dead key must not count.
    eng.forget_atom("r", target).unwrap();
    assert_eq!(eng.count("r", "fact").unwrap(), 2);
}

#[test]
fn forget_atom_removes_one_keeps_siblings() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("r", embedder()).unwrap();

    let keep1 = eng
        .remember("r", AtomInput::new("fact", "alpha bravo charlie"))
        .unwrap();
    let target = eng
        .remember("r", AtomInput::new("fact", "secret delta echo foxtrot"))
        .unwrap();
    let keep2 = eng
        .remember("r", AtomInput::new("fact", "golf hotel india juliet"))
        .unwrap();

    eng.forget_atom("r", target).unwrap();

    assert!(
        eng.fetch_one("r", target).unwrap().is_none(),
        "the forgotten atom is gone"
    );
    assert!(
        eng.fetch_one("r", keep1).unwrap().is_some(),
        "sibling keep1 survives"
    );
    assert!(
        eng.fetch_one("r", keep2).unwrap().is_some(),
        "sibling keep2 survives"
    );

    let hits = eng
        .recall("r", RecallQuery::by_text("secret delta echo foxtrot", 5))
        .unwrap();
    assert!(
        !hits.iter().any(|h| h.id == target),
        "the forgotten atom is not recalled"
    );
    let hits2 = eng
        .recall("r", RecallQuery::by_text("alpha bravo charlie", 5))
        .unwrap();
    assert!(
        hits2.iter().any(|h| h.id == keep1),
        "a sibling is still recalled after the forget"
    );
}

#[test]
fn forget_then_remember_rotation_stays_correct() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("r", embedder()).unwrap();

    // Repeatedly add then forget; each live atom decrypts before forget and is
    // gone after (the freed key slots are reused by the atom store's
    // allocator).
    for round in 0..20 {
        let text = format!("rotating memory token {round}");
        let id = eng
            .remember("r", AtomInput::new("fact", text.clone()))
            .unwrap();
        let echo = eng.recall("r", RecallQuery::by_text(&text, 1)).unwrap();
        assert_eq!(echo[0].text, text, "live atom decrypts");
        eng.forget_atom("r", id).unwrap();
        assert!(
            eng.fetch_one("r", id).unwrap().is_none(),
            "forgotten atom is gone"
        );
    }
}

#[test]
fn forget_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let (target, keep);
    {
        let db = DatabaseBuilder::new(&path)
            .passphrase(b"pw")
            .enable_region_keys(true)
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let eng = MemoryEngine::open(Arc::new(db)).unwrap();
        eng.create_encrypted_region("r", embedder()).unwrap();
        keep = eng
            .remember("r", AtomInput::new("fact", "persistent keep"))
            .unwrap();
        target = eng
            .remember("r", AtomInput::new("fact", "persistent forget"))
            .unwrap();
        eng.forget_atom("r", target).unwrap();
    }

    // Reopen: the forgotten atom's key slot is durably tombstoned, so it stays
    // gone; the sibling is still recoverable.
    let db = DatabaseBuilder::new(&path)
        .passphrase(b"pw")
        .enable_region_keys(true)
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let eng = MemoryEngine::open(Arc::new(db)).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    assert!(
        eng.fetch_one("r", target).unwrap().is_none(),
        "the forgotten atom stays gone after reopen"
    );
    assert!(
        eng.fetch_one("r", keep).unwrap().is_some(),
        "the sibling recovers after reopen"
    );
}

/// Crash window of `forget_atom`: the key is destroyed first (fail-secure), the
/// row delete may never run. Open-time reconcile must complete the interrupted
/// erase - the orphan row (and its edges) disappear, siblings stay intact.
#[test]
fn interrupted_erase_is_completed_at_next_open() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"pw")
            .enable_region_keys(true)
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    let victim = eng
        .remember("r", AtomInput::new("fact", "doomed memory"))
        .unwrap();
    let kept = eng
        .remember("r", AtomInput::new("fact", "surviving memory"))
        .unwrap();

    // Simulate the crash: destroy the victim's key exactly as erase_and_delete
    // does, then "crash" before the row delete.
    let conn = citadel_sql::Connection::open(&db).unwrap();
    let table = format!("memory_atoms_d{DIM}_cosine_enc");
    let qr = match conn
        .execute(&format!("SELECT key_slot FROM {table} WHERE id = {victim}"))
        .unwrap()
    {
        citadel_sql::ExecutionResult::Query(qr) => qr,
        _ => panic!("query"),
    };
    let citadel_sql::Value::Integer(slot) = qr.rows[0][0] else {
        panic!("slot shape");
    };
    db.atom_store_tombstone(slot as u32, victim as u64).unwrap();
    drop(conn);
    drop(eng);

    // Reopen: reconcile completes the interrupted erase.
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    assert!(
        eng.fetch_one("r", victim).unwrap().is_none(),
        "orphan row is deleted at open"
    );
    let hits = eng
        .recall("r", RecallQuery::by_text("surviving memory", 5))
        .unwrap();
    assert!(
        hits.iter().any(|h| h.id == kept),
        "sibling atom survives reconcile"
    );
    assert!(
        hits.iter().all(|h| h.id != victim),
        "erased atom never resurfaces"
    );
}

/// A recycled key slot (victim forgotten, slot reused by a newer atom) must
/// attest a crash-orphaned row as KeyErased - the row's key is gone - never as
/// Tampered.
#[test]
fn recycled_slot_attests_key_erased() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("edge.db"))
            .passphrase(b"pw")
            .enable_region_keys(true)
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", embedder()).unwrap();
    let victim = eng.remember("v", AtomInput::new("fact", "victim")).unwrap();

    // Simulate the forget crash window: key tombstoned, row left behind.
    let conn = citadel_sql::Connection::open(&db).unwrap();
    let table = format!("memory_atoms_d{DIM}_cosine_enc");
    let qr = match conn
        .execute(&format!("SELECT key_slot FROM {table} WHERE id = {victim}"))
        .unwrap()
    {
        citadel_sql::ExecutionResult::Query(qr) => qr,
        _ => panic!("query"),
    };
    let citadel_sql::Value::Integer(slot) = qr.rows[0][0] else {
        panic!("slot shape");
    };
    db.atom_store_tombstone(slot as u32, victim as u64).unwrap();
    // New inserts may recycle the freed slot under a different
    // owner/generation.
    for i in 0..3 {
        eng.remember("v", AtomInput::new("fact", format!("newer {i}")))
            .unwrap();
    }

    let att = &eng.verify_atoms("v", &[victim]).unwrap()[0];
    assert_eq!(
        att.verdict,
        AttestVerdict::KeyErased,
        "dead/recycled key binding must read as erased"
    );
}

/// delete_atoms reports rows actually deleted; nonexistent ids do not inflate
/// it. An all-immutable forget produces a receipt that claims NO durability it
/// did not earn.
#[test]
fn honest_counts_and_empty_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("v", embedder()).unwrap();
    let real = eng.remember("v", AtomInput::new("fact", "real")).unwrap();
    let report = eng
        .delete_atoms("v", &[real, real + 100, real + 200])
        .unwrap();
    assert_eq!(report.removed, 1, "ghost ids not counted");

    let locked = eng
        .remember("v", AtomInput::new("fact", "locked").immutable())
        .unwrap();
    let receipt = eng.forget_atoms("v", &[locked], false).unwrap();
    assert_eq!(receipt.erased_count, 0);
    assert_eq!(receipt.immutable_skipped, vec![locked]);
    assert!(!receipt.fsync, "no fsync claimed for an empty erasure");
    assert!(!receipt.readback_confirmed, "no readback claimed either");
}
