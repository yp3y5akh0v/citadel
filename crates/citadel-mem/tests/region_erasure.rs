//! Per-region cryptographic erasure: end-to-end behaviour, the documented backup
//! limit, recall/fusion parity with the plaintext path, and functional coverage of
//! the sealed read paths. The rigorous "same sealed bytes, key destroyed" adversary
//! test and the crash-ordering / residue assertions live in the crate-internal unit
//! tests (engine_tests.rs, region_store_tests.rs) which can reach the key store.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{
    AtomInput, EdgeKind, Embedder, EvictionPolicy, GraphExpand, MemoryEngine, MockEmbedder,
    MockReranker, RecallQuery, RerankStrategy,
};
use serde_json::json;
use std::sync::Arc;

const DIM: usize = 64;

fn open_enc_db(path: &std::path::Path, pass: &[u8], create: bool) -> citadel::Result<Database> {
    let b = DatabaseBuilder::new(path)
        .passphrase(pass)
        .enable_region_keys(true)
        .argon2_profile(Argon2Profile::Iot);
    if create {
        b.create()
    } else {
        b.open()
    }
}

fn embedder() -> Arc<MockEmbedder> {
    Arc::new(MockEmbedder::new(DIM))
}

#[test]
fn create_encrypted_region_requires_region_keys() {
    let dir = tempfile::tempdir().unwrap();
    // No enable_region_keys -> region keys are absent.
    let db = DatabaseBuilder::new(dir.path().join("m.db"))
        .passphrase(b"pw")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let eng = MemoryEngine::open(Arc::new(db)).unwrap();
    let err = eng.create_encrypted_region("r", embedder()).unwrap_err();
    assert!(
        err.to_string().contains("not enabled"),
        "expected RegionKeysDisabled, got: {err}"
    );
    // The plaintext path still works on the same database.
    eng.create_region("plain", embedder()).unwrap();
}

#[test]
fn recoverable_before_forget_unrecoverable_after_with_backup_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let backup = dir.path().join("backup.db");
    let pass = b"correct-horse";
    let secret = "the launch authorization code is orange-tango-7";

    {
        let db = Arc::new(open_enc_db(&path, pass, true).unwrap());
        let eng = MemoryEngine::open(db.clone()).unwrap();
        eng.create_encrypted_region("secret", embedder()).unwrap();
        eng.remember("secret", AtomInput::new("fact", secret))
            .unwrap();

        // Content is recoverable while the region is live.
        let hits = eng
            .recall("secret", RecallQuery::by_text(secret, 3))
            .unwrap();
        assert!(
            hits.iter().any(|h| h.text == secret),
            "secret must be recoverable before forget"
        );

        // A backup taken BEFORE forget is a self-contained recoverable copy.
        db.backup(&backup).unwrap();

        // Forget = cryptographic erasure.
        eng.drop_region("secret").unwrap();
    }

    // Live store after forget: the region is gone and its content is unrecoverable.
    {
        let db = Arc::new(open_enc_db(&path, pass, false).unwrap());
        let eng = MemoryEngine::open(db).unwrap();
        // Re-attaching by name creates a fresh, empty region (old key destroyed).
        eng.create_encrypted_region("secret", embedder()).unwrap();
        let hits = eng
            .recall("secret", RecallQuery::by_text(secret, 5))
            .unwrap();
        assert!(
            hits.is_empty(),
            "no content may be recovered from the live store after forget"
        );
    }

    // The pre-forget backup STILL recovers the secret: this is the documented limit
    // (destroy backups out of band). It also proves the adversary harness is real.
    {
        let db = Arc::new(open_enc_db(&backup, pass, false).unwrap());
        let eng = MemoryEngine::open(db).unwrap();
        eng.create_encrypted_region("secret", embedder()).unwrap();
        let hits = eng
            .recall("secret", RecallQuery::by_text(secret, 3))
            .unwrap();
        assert!(
            hits.iter().any(|h| h.text == secret),
            "a backup taken before forget remains recoverable (documented limit)"
        );
    }
}

#[test]
fn forget_overwrites_the_region_keystore() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let db = Arc::new(open_enc_db(&path, b"pw", true).unwrap());
    let sidecar = db.region_store_path();
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    eng.remember("r", AtomInput::new("fact", "to be forgotten"))
        .unwrap();

    let before = std::fs::read(&sidecar).unwrap();
    eng.drop_region("r").unwrap();
    let after = std::fs::read(&sidecar).unwrap();

    assert_ne!(
        before, after,
        "forget must overwrite the wrapped key slot in the sidecar"
    );
}

#[test]
fn encrypted_recall_ranks_correctly_and_deterministically() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_enc_db(&dir.path().join("m.db"), b"pw", true).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_region("plain", embedder()).unwrap();
    eng.create_encrypted_region("enc", embedder()).unwrap();

    // Ground truth: exact match vs. plaintext ANN (which is approximate+randomized).
    let exact = "apple banana cherry date";
    let texts = [
        exact,
        "apple banana cherry melon",
        "apple lemon mango pear",
        "olive grape fig plum",
    ];
    for t in texts {
        eng.remember("plain", AtomInput::new("fact", t)).unwrap();
        eng.remember("enc", AtomInput::new("fact", t)).unwrap();
    }
    let qvec = embedder().embed(&[exact]).unwrap()[0].clone();

    // The exact match is the top hit on both the sealed and plaintext paths.
    let enc = eng
        .recall("enc", RecallQuery::by_embedding(qvec.clone(), 4))
        .unwrap();
    let plain = eng
        .recall("plain", RecallQuery::by_embedding(qvec.clone(), 4))
        .unwrap();
    assert_eq!(
        enc[0].text, exact,
        "sealed recall top hit is the exact match"
    );
    assert_eq!(
        plain[0].text, exact,
        "plaintext recall top hit is the exact match"
    );
    assert_eq!(enc.len(), 4, "sealed recall returns the requested k");

    // Decrypt-then-rank is deterministic (exact, no randomized index).
    let enc2 = eng
        .recall("enc", RecallQuery::by_embedding(qvec, 4))
        .unwrap();
    let o1: Vec<&str> = enc.iter().map(|h| h.text.as_str()).collect();
    let o2: Vec<&str> = enc2.iter().map(|h| h.text.as_str()).collect();
    assert_eq!(o1, o2, "sealed recall is deterministic");
}

#[test]
fn sealed_read_paths_are_functional() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_enc_db(&dir.path().join("m.db"), b"pw", true).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();

    let a = eng
        .remember(
            "r",
            AtomInput::new("fact", "rust is memory safe").with_payload(json!({"topic": "rust"})),
        )
        .unwrap();
    let b = eng
        .remember(
            "r",
            AtomInput::new("note", "borrow checker enforces aliasing")
                .with_payload(json!({"topic": "rust"})),
        )
        .unwrap();
    let c = eng
        .remember("r", AtomInput::new("fact", "the sky is blue"))
        .unwrap();
    eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();

    // fetch by kind, fetch_one, fetch_last all decrypt correctly.
    let facts = eng.fetch("r", "fact", None, 10).unwrap();
    assert_eq!(facts.len(), 2);
    assert_eq!(
        eng.fetch_one("r", a).unwrap().unwrap().text,
        "rust is memory safe"
    );
    assert_eq!(
        eng.fetch_last("r", "fact").unwrap().unwrap().id,
        c,
        "fetch_last returns the highest-id fact"
    );

    // payload filter is applied in Rust after decrypt.
    let rust_atoms = eng
        .fetch("r", "fact", Some(&json!({"topic": "rust"})), 10)
        .unwrap();
    assert_eq!(rust_atoms.len(), 1);
    assert_eq!(rust_atoms[0].id, a);

    // graph expansion walks plaintext edges then decrypts the reached atom.
    let expanded = eng
        .recall(
            "r",
            RecallQuery::by_text("rust is memory safe", 1)
                .with_graph_expand(GraphExpand::new(1, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    assert!(
        expanded.iter().any(|h| h.id == b),
        "derived atom is reachable via graph expansion on an encrypted region"
    );

    // summarize reads plaintext metadata columns.
    let summary = eng.summarize("r", 0).unwrap();
    assert_eq!(summary.total, 3);

    // predicate eviction decrypts and matches in Rust.
    let removed = eng
        .evict(
            "r",
            EvictionPolicy::PredicateMatch {
                predicate: json!({"topic": "rust"}),
            },
        )
        .unwrap();
    assert_eq!(removed.removed, 2, "both rust-topic atoms evicted");
    assert_eq!(eng.summarize("r", 0).unwrap().total, 1);
}

#[test]
fn update_atom_payload_reseals() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_enc_db(&dir.path().join("m.db"), b"pw", true).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    let id = eng
        .remember(
            "r",
            AtomInput::new("fact", "status").with_payload(json!({"v": 1})),
        )
        .unwrap();

    eng.update_atom_payload("r", id, &json!({"v": 2})).unwrap();
    let hit = eng.fetch_one("r", id).unwrap().unwrap();
    assert_eq!(hit.payload["v"], 2, "payload re-sealed");
    assert_eq!(hit.text, "status", "text preserved through re-seal");
}

#[test]
fn recall_reflects_updated_payload_after_cache_is_built() {
    // The sealed recall path caches the decrypted payload at index build. Updating an
    // atom must invalidate that cache, or recall would serve the stale pre-update payload.
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_enc_db(&dir.path().join("m.db"), b"pw", true).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    let id = eng
        .remember(
            "r",
            AtomInput::new("fact", "the needle phrase").with_payload(json!({"v": 1})),
        )
        .unwrap();

    // First recall builds and caches the in-RAM index, capturing payload v1.
    let first = eng
        .recall("r", RecallQuery::by_text("the needle phrase", 1))
        .unwrap();
    assert_eq!(first[0].payload["v"], 1);

    eng.update_atom_payload("r", id, &json!({"v": 2})).unwrap();

    let second = eng
        .recall("r", RecallQuery::by_text("the needle phrase", 1))
        .unwrap();
    assert_eq!(
        second[0].payload["v"], 2,
        "recall served a stale cached payload after update_atom_payload"
    );
}

#[test]
fn in_memory_db_with_region_keys_is_rejected() {
    let err = DatabaseBuilder::new("ignored.db")
        .passphrase(b"pw")
        .enable_region_keys(true)
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap_err();
    assert!(
        err.to_string().contains("file-backed"),
        "in-memory + region keys must be rejected cleanly, got: {err}"
    );
}

#[test]
fn encrypted_region_survives_close_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let pass = b"pw";

    let (a, b) = {
        let db = Arc::new(open_enc_db(&path, pass, true).unwrap());
        let eng = MemoryEngine::open(db).unwrap();
        eng.create_encrypted_region("r", embedder()).unwrap();
        let a = eng
            .remember(
                "r",
                AtomInput::new("fact", "alpha quick fox").with_payload(json!({"k": "v"})),
            )
            .unwrap();
        let b = eng
            .remember("r", AtomInput::new("fact", "beta lazy dog"))
            .unwrap();
        eng.link(a, b, EdgeKind::DerivedFrom, 1.0).unwrap();
        (a, b)
    };

    // Cold reopen: re-attach unwraps the RCK from the sidecar slot (no in-process cache).
    let db = Arc::new(open_enc_db(&path, pass, false).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();

    let hit = eng.fetch_one("r", a).unwrap().unwrap();
    assert_eq!(hit.text, "alpha quick fox", "sealed text survives reopen");
    assert_eq!(hit.payload["k"], "v", "sealed payload survives reopen");

    let expanded = eng
        .recall(
            "r",
            RecallQuery::by_text("alpha quick fox", 1)
                .with_graph_expand(GraphExpand::new(1, vec![EdgeKind::DerivedFrom])),
        )
        .unwrap();
    assert!(
        expanded.iter().any(|h| h.id == b),
        "edge is traversable after reopen"
    );
}

#[test]
fn sealed_predicate_eviction_is_exhaustive_beyond_scan_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_enc_db(&dir.path().join("m.db"), b"pw", true).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();

    // >4096 atoms: exhaustive eviction must remove every id, not just first 4096.
    let n: usize = 4200;
    let atoms: Vec<AtomInput> = (0..n)
        .map(|i| AtomInput::new("fact", format!("atom {i}")).with_payload(json!({"drop": true})))
        .collect();
    eng.remember_batch("r", atoms).unwrap();

    let report = eng
        .evict(
            "r",
            EvictionPolicy::PredicateMatch {
                predicate: json!({"drop": true}),
            },
        )
        .unwrap();
    assert_eq!(
        report.removed, n as u64,
        "every matching atom evicted, not just the first 4096"
    );
    assert_eq!(eng.summarize("r", 0).unwrap().total, 0);
}

#[test]
fn encrypted_recall_covers_whole_region_via_prism() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_enc_db(&dir.path().join("m.db"), b"pw", true).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();

    // In-RAM PRISM index spans the whole region; needle after thousands still recalled.
    let fillers: Vec<AtomInput> = (0..4096)
        .map(|i| AtomInput::new("fact", format!("filler number {i}")))
        .collect();
    eng.remember_batch("r", fillers).unwrap();
    let needle = "zzz unique needle phrase";
    eng.remember("r", AtomInput::new("fact", needle)).unwrap();

    let hits = eng.recall("r", RecallQuery::by_text(needle, 10)).unwrap();
    assert!(
        hits.iter().any(|h| h.text == needle),
        "the needle is recalled from a 4097-atom sealed region via the PRISM index"
    );
}

#[test]
fn compact_then_recall_encrypted_region() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("compact.db");
    {
        let db = Arc::new(open_enc_db(&dir.path().join("m.db"), b"pw", true).unwrap());
        let eng = MemoryEngine::open(db.clone()).unwrap();
        eng.create_encrypted_region("r", embedder()).unwrap();
        eng.remember(
            "r",
            AtomInput::new("fact", "compacted secret").with_payload(json!({"n": 1})),
        )
        .unwrap();
        db.compact(&dest).unwrap();
    }

    let db = Arc::new(open_enc_db(&dest, b"pw", false).unwrap());
    assert!(
        db.region_store_path().exists(),
        "compact carried the sidecar"
    );
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    let hits = eng
        .recall("r", RecallQuery::by_text("compacted secret", 3))
        .unwrap();
    let hit = hits
        .iter()
        .find(|h| h.text == "compacted secret")
        .expect("compacted encrypted atom recovers");
    assert_eq!(hit.payload["n"], 1);
}

#[test]
fn backup_then_forget_live_vs_backup_with_sibling_survival() {
    let dir = tempfile::tempdir().unwrap();
    let backup = dir.path().join("backup.db");
    let db = Arc::new(open_enc_db(&dir.path().join("m.db"), b"pw", true).unwrap());
    let eng = MemoryEngine::open(db.clone()).unwrap();
    eng.create_encrypted_region("a", embedder()).unwrap();
    eng.create_encrypted_region("b", embedder()).unwrap();
    eng.remember("a", AtomInput::new("fact", "secret A"))
        .unwrap();
    eng.remember("b", AtomInput::new("fact", "secret B"))
        .unwrap();
    db.backup(&backup).unwrap();

    eng.drop_region("a").unwrap();

    // Live store: A erased, B intact (forget of A did not touch B's slot).
    eng.create_encrypted_region("a", embedder()).unwrap();
    assert!(eng
        .recall("a", RecallQuery::by_text("secret A", 5))
        .unwrap()
        .is_empty());
    assert!(eng
        .recall("b", RecallQuery::by_text("secret B", 5))
        .unwrap()
        .iter()
        .any(|h| h.text == "secret B"));

    // Pre-forget backup: BOTH recover (documented retention limit; sibling unaffected).
    let bdb = Arc::new(open_enc_db(&backup, b"pw", false).unwrap());
    let beng = MemoryEngine::open(bdb).unwrap();
    beng.create_encrypted_region("a", embedder()).unwrap();
    beng.create_encrypted_region("b", embedder()).unwrap();
    assert!(beng
        .recall("a", RecallQuery::by_text("secret A", 5))
        .unwrap()
        .iter()
        .any(|h| h.text == "secret A"));
    assert!(beng
        .recall("b", RecallQuery::by_text("secret B", 5))
        .unwrap()
        .iter()
        .any(|h| h.text == "secret B"));
}

#[test]
fn forget_encrypted_region_with_cross_region_edges() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_enc_db(&dir.path().join("m.db"), b"pw", true).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("a", embedder()).unwrap();
    eng.create_encrypted_region("b", embedder()).unwrap();
    let a1 = eng.remember("a", AtomInput::new("fact", "a-one")).unwrap();
    let a2 = eng.remember("a", AtomInput::new("fact", "a-two")).unwrap();
    let b1 = eng.remember("b", AtomInput::new("fact", "b-one")).unwrap();
    eng.link(a1, b1, EdgeKind::DerivedFrom, 1.0).unwrap(); // A -> B
    eng.link(b1, a2, EdgeKind::DerivedFrom, 1.0).unwrap(); // B -> A
    eng.link(a1, a2, EdgeKind::DerivedFrom, 1.0).unwrap(); // intra-A

    eng.drop_region("a").unwrap();

    // Every edge incident to a1/a2 (both directions, intra and cross) is gone.
    assert!(eng.fetch_edges(Some(a1), None, None).unwrap().is_empty());
    assert!(eng.fetch_edges(None, Some(a2), None).unwrap().is_empty());
    assert!(eng
        .fetch_edges(Some(a1), Some(a2), None)
        .unwrap()
        .is_empty());
    // B's atoms survive and recall; re-creating A by name is empty.
    assert!(eng
        .recall("b", RecallQuery::by_text("b-one", 5))
        .unwrap()
        .iter()
        .any(|h| h.text == "b-one"));
    eng.create_encrypted_region("a", embedder()).unwrap();
    assert!(eng
        .recall("a", RecallQuery::by_text("a-one", 5))
        .unwrap()
        .is_empty());
    let _ = b1;
}

#[test]
fn grow_under_load_reopen_and_recycle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let pass = b"pw";
    let forgotten = [5i32, 17, 40, 63];

    {
        let db = Arc::new(open_enc_db(&path, pass, true).unwrap());
        let eng = MemoryEngine::open(db).unwrap();
        // 70 regions forces growth past the 64-slot pre-allocated run.
        for i in 0..70 {
            let name = format!("r{i}");
            eng.create_encrypted_region(&name, embedder()).unwrap();
            eng.remember(&name, AtomInput::new("fact", format!("atom for {name}")))
                .unwrap();
        }
        // Forget a subset spanning the prealloc/grown boundary.
        for i in forgotten {
            eng.drop_region(&format!("r{i}")).unwrap();
        }
        // Create more: recycle freed tombstones first, then grow again.
        for i in 70..80 {
            let name = format!("r{i}");
            eng.create_encrypted_region(&name, embedder()).unwrap();
            eng.remember(&name, AtomInput::new("fact", format!("atom for {name}")))
                .unwrap();
        }
    }

    // Fresh engine: re-read slot_count from the header and re-attach every live region.
    let db = Arc::new(open_enc_db(&path, pass, false).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    for i in (0..80).filter(|i| !forgotten.contains(i)) {
        let name = format!("r{i}");
        eng.create_encrypted_region(&name, embedder()).unwrap();
        let want = format!("atom for {name}");
        let hits = eng.recall(&name, RecallQuery::by_text(&want, 1)).unwrap();
        assert!(
            hits.iter().any(|h| h.text == want),
            "{name} re-attaches the right key across grow + reopen"
        );
    }
    for i in forgotten {
        let name = format!("r{i}");
        eng.create_encrypted_region(&name, embedder()).unwrap();
        assert!(
            eng.recall(&name, RecallQuery::by_text("atom", 5))
                .unwrap()
                .is_empty(),
            "{name} was forgotten and re-creates empty"
        );
    }
}

#[test]
fn encrypted_recall_with_kinds_filter_and_reranker() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_enc_db(&dir.path().join("m.db"), b"pw", true).unwrap());
    let mut eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    eng.remember(
        "r",
        AtomInput::new("fact", "rust memory safety").with_payload(json!({"t": "rust"})),
    )
    .unwrap();
    eng.remember(
        "r",
        AtomInput::new("note", "borrow checker rules").with_payload(json!({"t": "rust"})),
    )
    .unwrap();
    eng.remember(
        "r",
        AtomInput::new("fact", "the sky is blue").with_payload(json!({"t": "sky"})),
    )
    .unwrap();

    // kinds filter is pushed to SQL on the sealed scan.
    let facts = eng
        .recall(
            "r",
            RecallQuery::by_text("rust", 10).with_kinds(vec!["fact".into()]),
        )
        .unwrap();
    assert!(facts.iter().all(|h| h.kind == "fact"));
    // payload filter applied in Rust after decrypt.
    let rust = eng
        .recall(
            "r",
            RecallQuery::by_text("rust", 10).with_payload_filter(json!({"t": "rust"})),
        )
        .unwrap();
    assert_eq!(rust.len(), 2);
    assert!(rust.iter().all(|h| h.payload["t"] == "rust"));
    // reranker drives the sealed fuse_rerank branch over the decrypted text.
    eng.set_reranker(Arc::new(MockReranker), RerankStrategy::Replace);
    let rr = eng
        .recall("r", RecallQuery::by_text("rust memory safety", 1))
        .unwrap();
    assert_eq!(
        rr[0].text, "rust memory safety",
        "reranker picks the best overlap on the sealed path"
    );
}

#[test]
fn encrypted_immutable_expires_and_nonpredicate_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let pass = b"pw";

    let imm_id = {
        let db = Arc::new(open_enc_db(&path, pass, true).unwrap());
        let eng = MemoryEngine::open(db).unwrap();
        eng.create_encrypted_region("r", embedder()).unwrap();
        let imm = eng
            .remember("r", AtomInput::new("fact", "permanent").immutable())
            .unwrap();
        eng.remember(
            "r",
            AtomInput::new("fact", "ephemeral").with_expires_at(123),
        )
        .unwrap();
        imm
    };

    // Immutable flag (plaintext metadata on the sealed row) persists across reopen.
    let db = Arc::new(open_enc_db(&path, pass, false).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    assert!(
        eng.fetch_one("r", imm_id).unwrap().unwrap().immutable,
        "immutable persists on a sealed atom"
    );

    // Stale eviction routes through the plaintext metadata columns of the sealed table
    // and spares the immutable atom.
    let report = eng
        .evict(
            "r",
            EvictionPolicy::Stale {
                older_than_micros: 0,
            },
        )
        .unwrap();
    assert!(
        report.removed >= 1,
        "stale eviction removed a non-immutable atom"
    );
    assert!(
        eng.fetch_one("r", imm_id).unwrap().is_some(),
        "immutable survives Stale eviction"
    );

    // delete_atoms force-removes even an immutable sealed atom.
    let r = eng.delete_atoms("r", &[imm_id]).unwrap();
    assert_eq!(r.removed, 1);
    assert!(eng.fetch_one("r", imm_id).unwrap().is_none());
}

#[test]
fn evolve_and_update_payload_on_encrypted_region_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let pass = b"pw";

    let (a, b) = {
        let db = Arc::new(open_enc_db(&path, pass, true).unwrap());
        let eng = MemoryEngine::open(db).unwrap();
        eng.create_encrypted_region("r", embedder()).unwrap();
        let a = eng
            .remember(
                "r",
                AtomInput::new("fact", "alpha quick fox").with_payload(json!({"v": 1})),
            )
            .unwrap();
        let b = eng
            .remember("r", AtomInput::new("fact", "alpha quick foxes"))
            .unwrap();
        // evolve decrypts a's sealed embedding and links it to near neighbours.
        let report = eng.evolve("r", a, 3, 2.0).unwrap();
        assert!(
            report.links_added >= 1,
            "evolve linked a near neighbour on the sealed path"
        );
        eng.update_atom_payload("r", a, &json!({"v": 2})).unwrap();
        (a, b)
    };

    let db = Arc::new(open_enc_db(&path, pass, false).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    assert_eq!(
        eng.fetch_one("r", a).unwrap().unwrap().payload["v"],
        2,
        "re-sealed payload survives reopen"
    );
    assert!(
        !eng.fetch_edges(Some(a), Some(b), None).unwrap().is_empty(),
        "evolve edge a->b survived reopen"
    );
}

#[test]
fn two_engines_over_one_database_share_the_key_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    {
        let db = Arc::new(open_enc_db(&path, b"pw", true).unwrap());
        let eng1 = MemoryEngine::open(db.clone()).unwrap();
        let eng2 = MemoryEngine::open(db.clone()).unwrap();
        eng1.create_encrypted_region("a", embedder()).unwrap();
        eng1.remember("a", AtomInput::new("fact", "alpha secret"))
            .unwrap();
        eng2.create_encrypted_region("b", embedder()).unwrap();
        eng2.remember("b", AtomInput::new("fact", "bravo secret"))
            .unwrap();

        assert!(eng1
            .recall("a", RecallQuery::by_text("alpha secret", 3))
            .unwrap()
            .iter()
            .any(|h| h.text == "alpha secret"));
        assert!(eng2
            .recall("b", RecallQuery::by_text("bravo secret", 3))
            .unwrap()
            .iter()
            .any(|h| h.text == "bravo secret"));

        eng1.create_encrypted_region("b", embedder()).unwrap();
        assert!(eng1
            .recall("b", RecallQuery::by_text("bravo secret", 3))
            .unwrap()
            .iter()
            .any(|h| h.text == "bravo secret"));
    }

    let db = Arc::new(open_enc_db(&path, b"pw", false).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("a", embedder()).unwrap();
    eng.create_encrypted_region("b", embedder()).unwrap();
    assert!(eng
        .recall("a", RecallQuery::by_text("alpha secret", 3))
        .unwrap()
        .iter()
        .any(|h| h.text == "alpha secret"));
    assert!(eng
        .recall("b", RecallQuery::by_text("bravo secret", 3))
        .unwrap()
        .iter()
        .any(|h| h.text == "bravo secret"));
}

#[test]
fn encrypted_region_is_fail_closed_without_region_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    {
        let db = Arc::new(open_enc_db(&path, b"pw", true).unwrap());
        let eng = MemoryEngine::open(db).unwrap();
        eng.create_encrypted_region("r", embedder()).unwrap();
        eng.remember("r", AtomInput::new("fact", "still here"))
            .unwrap();
    }
    {
        let db = Arc::new(
            DatabaseBuilder::new(&path)
                .passphrase(b"pw")
                .argon2_profile(Argon2Profile::Iot)
                .open()
                .unwrap(),
        );
        let eng = MemoryEngine::open(db).unwrap();
        assert!(
            eng.create_encrypted_region("r", embedder()).is_err(),
            "attaching an encrypted region without region keys must fail"
        );
        assert!(
            eng.drop_region("r").is_err(),
            "dropping an encrypted region without keys must fail closed (no row delete)"
        );
    }
    let db = Arc::new(open_enc_db(&path, b"pw", false).unwrap());
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    assert!(eng
        .recall("r", RecallQuery::by_text("still here", 3))
        .unwrap()
        .iter()
        .any(|h| h.text == "still here"));
}
