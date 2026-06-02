//! Language-agnostic BM25 keyword signal: it helps exact/rare-term recall, is driven by
//! IDF (no stopword list, any language), and behaves identically on the plaintext and
//! encrypted (sealed) recall paths - they share one primitive.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_mem::{AtomInput, FusionWeights, MemoryEngine, MockEmbedder, RecallQuery};
use std::sync::Arc;

const DIM: usize = 32;

fn embedder() -> Arc<MockEmbedder> {
    Arc::new(MockEmbedder::new(DIM))
}

/// File-backed DB with region keys enabled, so both `create_region` (plaintext) and
/// `create_encrypted_region` (sealed) work on the same database.
fn db(path: &std::path::Path) -> Arc<Database> {
    Arc::new(
        DatabaseBuilder::new(path)
            .passphrase(b"pw")
            .enable_region_keys(true)
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    )
}

/// Keyword-only fusion weights, so BM25 alone drives ranking - isolates the keyword
/// path and makes the assertions deterministic regardless of the mock embedder.
fn keyword_only() -> FusionWeights {
    FusionWeights {
        semantic: 0.0,
        keyword: 1.0,
        recency: 0.0,
        importance: 0.0,
    }
}

fn seed(eng: &MemoryEngine, region: &str) {
    for t in [
        "the quick brown fox jumps",
        "a lazy dog sleeps all day",
        "error code QX7Z42 occurred at startup",
        "the meeting is scheduled for friday",
        "brown rice and green tea",
    ] {
        eng.remember(region, AtomInput::new("fact", t)).unwrap();
    }
}

/// ENCRYPTED path: a rare exact term (an error code) is recalled via BM25 even though it
/// is semantically unremarkable - the classic hybrid-retrieval win a reranker can't add.
#[test]
fn encrypted_keyword_recalls_rare_exact_term() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(db(&dir.path().join("m.db"))).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    seed(&eng, "r");
    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("QX7Z42", 1).with_weights(keyword_only()),
        )
        .unwrap();
    assert_eq!(hits[0].text, "error code QX7Z42 occurred at startup");
}

/// PLAINTEXT path: same exact-term recall, proving the unified primitive (no SQL FTS).
#[test]
fn plaintext_keyword_recalls_rare_exact_term() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(db(&dir.path().join("m.db"))).unwrap();
    eng.create_region("r", embedder()).unwrap();
    seed(&eng, "r");
    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("QX7Z42", 1).with_weights(keyword_only()),
        )
        .unwrap();
    assert_eq!(hits[0].text, "error code QX7Z42 occurred at startup");
}

/// Plaintext and encrypted produce the SAME keyword-only ranking on identical data:
/// one BM25 primitive, two storage paths.
#[test]
fn plaintext_and_encrypted_keyword_ranking_match() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(db(&dir.path().join("m.db"))).unwrap();
    eng.create_region("plain", embedder()).unwrap();
    eng.create_encrypted_region("enc", embedder()).unwrap();
    seed(&eng, "plain");
    seed(&eng, "enc");
    let q = || RecallQuery::by_text("brown", 5).with_weights(keyword_only());
    let p: Vec<String> = eng
        .recall("plain", q())
        .unwrap()
        .into_iter()
        .map(|h| h.text)
        .collect();
    let e: Vec<String> = eng
        .recall("enc", q())
        .unwrap()
        .into_iter()
        .map(|h| h.text)
        .collect();
    assert_eq!(p, e, "plaintext and encrypted keyword rankings agree");
    assert!(p[0].contains("brown"), "a brown-bearing atom ranks first");
}

/// BM25 is language-agnostic: non-English (Cyrillic) content stores, and a keyword query
/// recalls the matching atom with no language configuration whatsoever.
#[test]
fn keyword_is_language_agnostic_cyrillic() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(db(&dir.path().join("m.db"))).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    for t in [
        "\u{43f}\u{440}\u{438}\u{432}\u{435}\u{442} \u{43c}\u{438}\u{440} \u{43a}\u{430}\u{43a} \u{434}\u{435}\u{43b}\u{430}",
        "\u{434}\u{43e}\u{431}\u{440}\u{44b}\u{439} \u{434}\u{435}\u{43d}\u{44c} \u{434}\u{440}\u{443}\u{437}\u{44c}\u{44f}",
        "guten tag welt freunde",
    ] {
        eng.remember("r", AtomInput::new("fact", t)).unwrap();
    }
    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("\u{43f}\u{440}\u{438}\u{432}\u{435}\u{442}", 1)
                .with_weights(keyword_only()),
        )
        .unwrap();
    assert!(
        hits[0]
            .text
            .starts_with("\u{43f}\u{440}\u{438}\u{432}\u{435}\u{442}"),
        "the Cyrillic-greeting atom is recalled by its keyword"
    );
}

/// IDF down-weights pool-common terms: a query mixing common and rare terms ranks the
/// doc carrying the rare term above docs that share only the common ones.
#[test]
fn idf_prefers_rare_over_common_terms() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(db(&dir.path().join("m.db"))).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    for t in [
        "report about quarterly revenue",
        "report about hiring plans",
        "report about the zephyr prototype",
    ] {
        eng.remember("r", AtomInput::new("fact", t)).unwrap();
    }
    // 'report'/'about' are in all three (low IDF); 'zephyr' is rare (high IDF).
    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("report about zephyr", 1).with_weights(keyword_only()),
        )
        .unwrap();
    assert_eq!(hits[0].text, "report about the zephyr prototype");
}

/// Mixed-case + punctuation in the query tokenizes via UAX#29 (case-folded, punctuation
/// split) and still matches.
#[test]
fn query_tokenization_is_case_and_punctuation_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(db(&dir.path().join("m.db"))).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    seed(&eng, "r");
    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("BROWN, fox!", 1).with_weights(keyword_only()),
        )
        .unwrap();
    assert_eq!(hits[0].text, "the quick brown fox jumps");
}

/// Keyword-only recall with zero lexical overlap is safe: all keyword scores are 0,
/// recall still returns k results (deterministic id tie-break), no panic.
#[test]
fn keyword_only_with_no_overlap_returns_k_safely() {
    let dir = tempfile::tempdir().unwrap();
    let eng = MemoryEngine::open(db(&dir.path().join("m.db"))).unwrap();
    eng.create_encrypted_region("r", embedder()).unwrap();
    seed(&eng, "r");
    let hits = eng
        .recall(
            "r",
            RecallQuery::by_text("zzzznomatchhere", 3).with_weights(keyword_only()),
        )
        .unwrap();
    assert_eq!(
        hits.len(),
        3,
        "still returns k results when nothing matches"
    );
}
