//! recall_many: RRF merge, id dedup, one cross-encoder pass over the merged pool.

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{
    AtomInput, MemoryEngine, MockEmbedder, MockReranker, MultiRecallQuery, RecallQuery,
    RerankStrategy,
};

const DIM: usize = 64;

fn engine(dir: &std::path::Path) -> MemoryEngine {
    let db = Arc::new(
        DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .unwrap(),
    );
    MemoryEngine::open(db).unwrap()
}

fn seed(eng: &MemoryEngine, region: &str) {
    for t in [
        "Alice's cat is named Mochi",
        "Bob lives in Berlin",
        "the meeting is on Friday afternoon",
    ] {
        eng.remember(region, AtomInput::new("fact", t)).unwrap();
    }
}

fn plain_region(eng: &MemoryEngine, name: &str) {
    eng.create_region(name, Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    seed(eng, name);
}

#[test]
fn merges_sub_query_lists_and_dedups_ids() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let mq = MultiRecallQuery::new(
        vec![
            RecallQuery::by_text("cat named Mochi", 10),
            RecallQuery::by_text("Berlin Bob", 10),
            // Overlapping sub-query: its hits must not duplicate ids.
            RecallQuery::by_text("cat named Mochi", 10),
        ],
        10,
    );
    let hits = eng.recall_many("notes", mq).unwrap();

    let mut ids: Vec<_> = hits.iter().map(|h| h.id).collect();
    ids.sort_unstable();
    let before = ids.len();
    ids.dedup();
    assert_eq!(ids.len(), before, "no duplicate ids after merge");
    assert_eq!(hits.len(), 3, "union covers all seeded atoms");
}

#[test]
fn truncates_to_k_and_orders_by_merged_score() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let mq = MultiRecallQuery::new(
        vec![
            RecallQuery::by_text("cat named Mochi", 10),
            RecallQuery::by_text("Berlin Bob", 10),
        ],
        1,
    );
    let hits = eng.recall_many("notes", mq).unwrap();
    assert_eq!(hits.len(), 1);

    let mq_all = MultiRecallQuery::new(vec![RecallQuery::by_text("cat", 10)], 10);
    let hits = eng.recall_many("notes", mq_all).unwrap();
    for w in hits.windows(2) {
        assert!(w[0].score >= w[1].score, "merged scores descending");
    }
}

#[test]
fn identical_calls_are_deterministic() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let mq = || {
        MultiRecallQuery::new(
            vec![
                RecallQuery::by_text("cat named Mochi", 10),
                RecallQuery::by_text("meeting Friday", 10),
            ],
            10,
        )
    };
    let a: Vec<_> = eng
        .recall_many("notes", mq())
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    let b: Vec<_> = eng
        .recall_many("notes", mq())
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert_eq!(a, b);
}

#[test]
fn rerank_query_runs_one_pass_over_the_merged_pool() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");
    eng.set_reranker(Arc::new(MockReranker), RerankStrategy::Replace);

    let mq = MultiRecallQuery::new(
        vec![
            RecallQuery::by_text("household pets", 10),
            RecallQuery::by_text("city of residence", 10),
        ],
        3,
    )
    .with_rerank_query("what is the name of Alice's cat Mochi");
    let hits = eng.recall_many("notes", mq).unwrap();
    assert!(
        hits[0].text.contains("Mochi"),
        "cross-encoder pass puts the overlap winner first, got: {}",
        hits[0].text
    );

    // Reranker set but no rerank_query: merged RRF order, no error.
    let mq = MultiRecallQuery::new(vec![RecallQuery::by_text("cat", 10)], 3);
    assert!(!eng.recall_many("notes", mq).unwrap().is_empty());
}

#[test]
fn sealed_region_recall_many_works() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_encrypted_region("vault", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    seed(&eng, "vault");

    let mq = MultiRecallQuery::new(
        vec![
            RecallQuery::by_text("cat named Mochi", 10),
            RecallQuery::by_text("Berlin Bob", 10),
        ],
        10,
    );
    let hits = eng.recall_many("vault", mq).unwrap();
    assert_eq!(hits.len(), 3, "sealed path merges the union");
}

#[test]
fn empty_inputs_return_empty() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let none = MultiRecallQuery::new(Vec::new(), 5);
    assert!(eng.recall_many("notes", none).unwrap().is_empty());

    let zero_k = MultiRecallQuery::new(vec![RecallQuery::by_text("cat", 10)], 0);
    assert!(eng.recall_many("notes", zero_k).unwrap().is_empty());
}

#[test]
fn sub_query_without_text_or_embedding_errors() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    plain_region(&eng, "notes");

    let mut bad = RecallQuery::by_text("x", 5);
    bad.text = None;
    let mq = MultiRecallQuery::new(vec![bad], 5);
    assert!(eng.recall_many("notes", mq).is_err());
}
