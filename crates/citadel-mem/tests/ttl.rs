//! TTL semantics on encrypted regions: a lapsed `expires_at` hides the atom
//! from every read path, survives persisted-segment reloads, and the `Expired`
//! eviction policy physically erases it.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{AtomInput, EvictionPolicy, MemoryEngine, MockEmbedder, RecallQuery};
use std::sync::Arc;

const DIM: usize = 16;

fn open_db(path: &std::path::Path) -> Database {
    DatabaseBuilder::new(path)
        .passphrase(b"pw")
        .enable_region_keys(true)
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn embedder() -> Arc<MockEmbedder> {
    Arc::new(MockEmbedder::new(DIM))
}

const PAST: i64 = 1_000_000; // 1970: any wall clock is past this

/// Sealed region: a lapsed TTL hides the atom from recall and fetch_one, the
/// Expired policy erases it cryptographically, and the report reflects a real
/// erasure.
#[test]
fn sealed_ttl_hides_and_expired_policy_erases() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(Arc::new(open_db(&dir.path().join("m.db")))).unwrap();
    eng.create_encrypted_region("v", embedder()).unwrap();
    let dead = eng
        .remember("v", AtomInput::new("fact", "lapsed").with_expires_at(PAST))
        .unwrap();
    eng.remember("v", AtomInput::new("fact", "alive")).unwrap();

    let hits = eng.recall("v", RecallQuery::by_text("lapsed", 10)).unwrap();
    assert!(hits.iter().all(|h| h.id != dead), "sealed recall hides TTL");
    assert!(eng.fetch_one("v", dead).unwrap().is_none());
    assert_eq!(eng.count("v", "fact").unwrap(), 1);

    let report = eng.evict("v", EvictionPolicy::Expired).unwrap();
    assert_eq!(report.removed, 1);
}

/// TTL filtering survives the persisted-segment path: the loaded cache carries
/// expires_micros, so a reopened engine still hides the lapsed atom.
#[test]
fn ttl_enforced_through_persisted_segment_reload() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let db = Arc::new(open_db(&path));
    let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
    eng.create_encrypted_region("v", embedder()).unwrap();
    let dead = eng
        .remember(
            "v",
            AtomInput::new("fact", "lapsed note").with_expires_at(PAST),
        )
        .unwrap();
    eng.remember("v", AtomInput::new("fact", "evergreen note"))
        .unwrap();
    eng.persist_ann_index("v").unwrap();
    drop(eng);

    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("v", embedder()).unwrap();
    let hits = eng.recall("v", RecallQuery::by_text("note", 10)).unwrap();
    assert!(
        hits.iter().all(|h| h.id != dead),
        "segment-loaded cache still enforces TTL"
    );
    assert!(hits.iter().any(|h| h.text == "evergreen note"));
}
