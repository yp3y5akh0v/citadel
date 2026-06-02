use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomInput, EdgeKind, GraphExpand, MemoryEngine, MockEmbedder, RecallQuery};
use serde_json::json;
use std::sync::Arc;

fn open_db(
    path: &std::path::Path,
    passphrase: &[u8],
    create: bool,
) -> citadel::Result<citadel::Database> {
    let b = DatabaseBuilder::new(path)
        .passphrase(passphrase)
        .argon2_profile(Argon2Profile::Iot);
    if create {
        b.create()
    } else {
        b.open()
    }
}

#[test]
fn atoms_edges_payloads_survive_close_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let pass = b"correct-horse";

    let (seed_id, derived_id) = {
        let db = Arc::new(open_db(&path, pass, true).unwrap());
        let eng = MemoryEngine::open(db).unwrap();
        eng.create_region("r", Arc::new(MockEmbedder::new(64)))
            .unwrap();
        let seed = eng
            .remember(
                "r",
                AtomInput::new("fact", "red green blue").with_payload(json!({"lang": "rust"})),
            )
            .unwrap();
        let derived = eng
            .remember("r", AtomInput::new("fact", "red green derived"))
            .unwrap();
        eng.link(seed, derived, EdgeKind::DerivedFrom, 1.0).unwrap();
        (seed, derived)
    };

    let db = Arc::new(open_db(&path, pass, false).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_region("r", Arc::new(MockEmbedder::new(64)))
        .unwrap();

    let hits = eng
        .recall("r", RecallQuery::by_text("red green blue", 5))
        .unwrap();
    let seed_hit = hits
        .iter()
        .find(|h| h.id == seed_id)
        .expect("seed atom survives reopen");
    assert_eq!(seed_hit.text, "red green blue");
    assert_eq!(seed_hit.payload["lang"], "rust", "payload survives reopen");

    let expanded = eng
        .recall(
            "r",
            RecallQuery::by_text("red green blue", 1)
                .with_graph_expand(GraphExpand::new(1, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    assert!(
        expanded.iter().any(|h| h.id == derived_id),
        "edge survives reopen and is traversable"
    );
}

#[test]
fn reopen_with_wrong_passphrase_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");

    {
        let db = Arc::new(open_db(&path, b"correct-horse", true).unwrap());
        let eng = MemoryEngine::open(db).unwrap();
        eng.create_region("r", Arc::new(MockEmbedder::new(64)))
            .unwrap();
        eng.remember("r", AtomInput::new("fact", "secret memory"))
            .unwrap();
    }

    assert!(
        open_db(&path, b"battery-staple", false).is_err(),
        "a wrong passphrase must not open the encrypted database"
    );
}
