//! Token-free proof that semantic recall (citadel-vector ANN via a real bge-small
//! embedder) surfaces a MEANING-relevant memory the word-overlap MockEmbedder misses.
//!
//! Ignored: needs a local bge-small-en-v1.5 dir in CITADEL_BGE_SMALL_DIR. Run:
//!   CITADEL_BGE_SMALL_DIR=... cargo test -p citadeldb-ai --features candle-embed \
//!     --test recall_quality -- --ignored --nocapture
#![cfg(feature = "candle-embed")]

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_ai::BeliefGraph;
use citadel_mem::{AtomInput, CandleEmbedder, Embedder, MemoryEngine, MockEmbedder};

// Matches the query's MEANING (an off-by-one / boundary bug) but shares no
// content words with it.
const RELEVANT: &str = "the last element is dropped because the loop stops one index too soon";
// Shares the surface word "boundary" with the query but is otherwise unrelated.
const DECOY: &str = "the boundary wall in the back garden was repainted last spring";
const FILLER: &str = "lunch is served at noon in the building cafeteria";
const QUERY: &str = "how do I fix an off-by-one boundary bug";

/// Seed a fresh region with the three atoms (as `evidence`, which `recall_relevant`
/// admits) and return the ranked recall for QUERY: (text, fused score), best first.
fn ranked_recall(embedder: Arc<dyn Embedder>) -> Vec<(String, f32)> {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"recall-quality")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let eng = Arc::new(MemoryEngine::open(db).unwrap());
    eng.create_region("agent", embedder).unwrap();
    for t in [RELEVANT, DECOY, FILLER] {
        eng.remember("agent", AtomInput::new("evidence", t))
            .unwrap();
    }
    let graph = BeliefGraph::new(eng, "agent");
    graph
        .recall_relevant(QUERY, 3)
        .unwrap()
        .into_iter()
        .map(|h| (h.text, h.score))
        .collect()
}

#[test]
#[ignore = "needs CITADEL_BGE_SMALL_DIR (a local bge-small-en-v1.5 dir)"]
fn semantic_recall_surfaces_meaning_that_word_overlap_misses() {
    let dir = std::env::var("CITADEL_BGE_SMALL_DIR")
        .expect("set CITADEL_BGE_SMALL_DIR to a local bge-small-en-v1.5 dir");

    let bge = ranked_recall(Arc::new(
        CandleEmbedder::bge_small(&dir).expect("load bge-small"),
    ));
    let mock = ranked_recall(Arc::new(MockEmbedder::new(64)));

    eprintln!("[recall-quality] query: {QUERY:?}");
    eprintln!("[recall-quality] bge-small ranking:");
    for (t, s) in &bge {
        eprintln!("    {s:.4}  {t:?}");
    }
    eprintln!("[recall-quality] mock ranking:");
    for (t, s) in &mock {
        eprintln!("    {s:.4}  {t:?}");
    }

    assert_eq!(
        bge[0].0, RELEVANT,
        "bge-small must rank the MEANING-relevant memory first; got {:?}",
        bge[0].0
    );
    assert_ne!(
        mock[0].0, RELEVANT,
        "the word-overlap baseline must NOT surface the meaning-relevant memory \
         (it is fooled by the shared word 'boundary'); got {:?}",
        mock[0].0
    );
}
