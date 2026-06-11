//! Per-atom cryptographic erasure (Tier B): forgetting one atom destroys its key and
//! removes it from recall/fetch, while sibling atoms and the region survive. The
//! "captured sealed bytes become undecryptable" adversary test lives in the crate's
//! internal unit tests (engine_tests.rs), which can reach the key store.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, MemoryEngine, MockEmbedder, RecallQuery};
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

    // Repeatedly add then forget; each live atom decrypts before forget and is gone
    // after (the freed key slots are reused by the atom store's allocator).
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

    // Reopen: the forgotten atom's key slot is durably tombstoned, so it stays gone;
    // the sibling is still recoverable.
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
