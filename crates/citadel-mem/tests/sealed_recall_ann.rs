//! Sealed-recall ANN coverage: the encrypted recall path builds an ephemeral in-RAM
//! PRISM index over decrypted vectors, so it is correct over large regions, fresh
//! after incremental writes, kind-filterable, and excludes deleted atoms - while
//! matching the plaintext PRISM top hit.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{AtomInput, Embedder, MemoryEngine, MockEmbedder, RecallQuery};
use std::sync::Arc;

const DIM: usize = 64;

fn open_enc_db(path: &std::path::Path) -> citadel::Result<Database> {
    DatabaseBuilder::new(path)
        .passphrase(b"pw")
        .enable_region_keys(true)
        .argon2_profile(Argon2Profile::Iot)
        .create()
}

fn embedder() -> Arc<MockEmbedder> {
    Arc::new(MockEmbedder::new(DIM))
}

fn engine(dir: &std::path::Path) -> MemoryEngine {
    let db = Arc::new(open_enc_db(&dir.join("m.db")).unwrap());
    MemoryEngine::open(db).unwrap()
}

#[test]
fn large_region_finds_exact_match() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("r", embedder()).unwrap();

    let needle = "the meeting is scheduled for friday at noon";
    let atoms: Vec<AtomInput> = (0..300)
        .map(|i| AtomInput::new("fact", format!("unrelated chatter line {i}")))
        .chain(std::iter::once(AtomInput::new("fact", needle)))
        .collect();
    eng.remember_batch("r", atoms).unwrap();

    let hits = eng.recall("r", RecallQuery::by_text(needle, 5)).unwrap();
    assert_eq!(
        hits[0].text, needle,
        "exact match is the top hit via the in-RAM PRISM index over a 301-atom region"
    );
}

#[test]
fn sees_atoms_added_after_first_recall() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("r", embedder()).unwrap();

    let seed: Vec<AtomInput> = (0..50)
        .map(|i| AtomInput::new("fact", format!("seed line {i}")))
        .collect();
    eng.remember_batch("r", seed).unwrap();
    // First recall builds + caches the index over the 50 seed atoms.
    let _ = eng
        .recall("r", RecallQuery::by_text("seed line 0", 3))
        .unwrap();

    // An atom inserted AFTER the snapshot must surface via the tail-delta exact scan.
    let fresh = "freshly added distinctive memory token qwxz";
    eng.remember("r", AtomInput::new("fact", fresh)).unwrap();
    let hits = eng.recall("r", RecallQuery::by_text(fresh, 3)).unwrap();
    assert_eq!(
        hits[0].text, fresh,
        "atom inserted after the cached snapshot is found via the tail-delta scan"
    );
}

#[test]
fn kind_filter_restricts_results() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("r", embedder()).unwrap();

    for i in 0..20 {
        eng.remember("r", AtomInput::new("fact", format!("fact item {i}")))
            .unwrap();
        eng.remember("r", AtomInput::new("note", format!("note item {i}")))
            .unwrap();
    }

    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("note item 3", 10).with_kinds(vec!["note".into()]),
        )
        .unwrap();
    assert!(!hits.is_empty(), "kind-filtered recall returns matches");
    assert!(
        hits.iter().all(|h| h.kind == "note"),
        "kind filter restricts sealed recall to the requested kind"
    );
}

#[test]
fn excludes_deleted_atom() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("r", embedder()).unwrap();

    for i in 0..30 {
        eng.remember("r", AtomInput::new("fact", format!("ordinary line {i}")))
            .unwrap();
    }
    let target = "secret to be forgotten zzqq";
    let tid = eng.remember("r", AtomInput::new("fact", target)).unwrap();

    // Build the index (caching the target's id), then delete the atom.
    let before = eng.recall("r", RecallQuery::by_text(target, 3)).unwrap();
    assert_eq!(before[0].text, target, "target is found before delete");
    eng.delete_atoms("r", &[tid]).unwrap();

    let after = eng.recall("r", RecallQuery::by_text(target, 5)).unwrap();
    assert!(
        !after.iter().any(|h| h.id == tid),
        "a deleted atom is not returned even though the cached index may still hold its id"
    );
}

#[test]
fn fetch_and_recall_cover_a_region_past_the_decrypt_page() {
    // Exceeds the internal decrypt page size (4096): fetch must page, recall must index the
    // whole region.
    const N: usize = 4096 + 50;
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("r", embedder()).unwrap();

    let needle = "distinctive tail needle past the scan window zzqq";
    let mut atoms: Vec<AtomInput> = (0..N - 1)
        .map(|i| AtomInput::new("turn", format!("filler chatter line {i}")))
        .collect();
    atoms.push(AtomInput::new("turn", needle)); // highest id, well past row 4096
    eng.remember_batch("r", atoms).unwrap();

    // limit past the page size: fetch pages and returns every atom.
    let all = eng.fetch("r", "turn", None, 100_000).unwrap();
    assert_eq!(
        all.len(),
        N,
        "fetch pages past the 4096 window and returns every atom"
    );
    assert!(
        all.iter().any(|h| h.text == needle),
        "the tail atom past row 4096 is returned by fetch"
    );

    // a smaller limit returns exactly that many.
    let few = eng.fetch("r", "turn", None, 10).unwrap();
    assert_eq!(few.len(), 10, "fetch honors a small limit exactly");

    // recall reaches an atom past the page boundary via the full-region index.
    let hits = eng.recall("r", RecallQuery::by_text(needle, 3)).unwrap();
    assert_eq!(
        hits[0].text, needle,
        "recall covers the whole encrypted region via the full in-RAM PRISM index"
    );
}

#[test]
fn encrypted_matches_plaintext_top_hit_over_larger_set() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region("plain", embedder()).unwrap();
    eng.create_encrypted_region("enc", embedder()).unwrap();

    let exact = "quarterly revenue grew by twelve percent";
    let texts: Vec<String> = (0..120)
        .map(|i| format!("filler sentence number {i}"))
        .chain(std::iter::once(exact.to_string()))
        .collect();
    for t in &texts {
        eng.remember("plain", AtomInput::new("fact", t.clone()))
            .unwrap();
        eng.remember("enc", AtomInput::new("fact", t.clone()))
            .unwrap();
    }
    let qvec = embedder().embed(&[exact]).unwrap()[0].clone();

    let enc = eng
        .recall("enc", RecallQuery::by_embedding(qvec.clone(), 5))
        .unwrap();
    let plain = eng
        .recall("plain", RecallQuery::by_embedding(qvec, 5))
        .unwrap();
    assert_eq!(
        enc[0].text, exact,
        "encrypted PRISM recall top hit is the exact match"
    );
    assert_eq!(
        plain[0].text, exact,
        "plaintext PRISM recall top hit is the exact match"
    );
}
