use std::path::Path;
use std::sync::{Arc, Mutex};

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_core::CancelToken;
use citadel_mem::{
    AtomInput, EmbedError, FusionWeights, MemoryEngine, MockEmbedder, MockReranker, RecallQuery,
    RerankStrategy, Reranker,
};
use serde_json::json;

const REGIONS: [&str; 2] = ["plain", "sealed"];
const TARGET: &str = "needle";

fn open_engine(path: &Path, create: bool) -> MemoryEngine {
    let builder = DatabaseBuilder::new(path.join("memory.cdl"))
        .passphrase(b"candidate-test")
        .argon2_profile(Argon2Profile::Iot)
        .enable_region_keys(true);
    let db = if create {
        builder.create()
    } else {
        builder.open()
    }
    .unwrap();
    let engine = MemoryEngine::open(Arc::new(db)).unwrap();
    if create {
        engine
            .create_region(REGIONS[0], Arc::new(MockEmbedder::new(2)))
            .unwrap();
        engine
            .create_encrypted_region(REGIONS[1], Arc::new(MockEmbedder::new(2)))
            .unwrap();
    } else {
        for region in REGIONS {
            engine
                .attach_existing_region(region, Arc::new(MockEmbedder::new(2)))
                .unwrap();
        }
    }
    engine
}

fn atom(text: impl Into<String>, near: bool, eligible: bool) -> AtomInput {
    AtomInput::new("fact", text)
        .with_embedding(if near { vec![1.0, 0.0] } else { vec![0.0, 1.0] })
        .with_created_at(1_000_000)
        .with_importance(0.0)
        .with_payload(json!({"eligible": eligible}))
}

fn seed_small(engine: &MemoryEngine) {
    for region in REGIONS {
        let mut atoms = (0..64)
            .map(|index| atom(format!("distractor {index}"), true, true))
            .collect::<Vec<_>>();
        atoms.push(atom(TARGET, false, true));
        engine.remember_batch(region, atoms).unwrap();
    }
}

fn query(weights: FusionWeights, filtered: bool) -> RecallQuery {
    let query = RecallQuery::by_embedding(vec![1.0, 0.0], 1)
        .with_text(TARGET)
        .with_as_of(1_000_000)
        .with_weights(weights);
    if filtered {
        query.with_payload_filter(json!({"eligible": true}))
    } else {
        query
    }
}

fn keyword_only() -> FusionWeights {
    FusionWeights {
        semantic: 0.0,
        keyword: 1.0,
        recency: 0.0,
        importance: 0.0,
    }
}

fn assert_keyword_target(engine: &MemoryEngine, filtered: bool) {
    for region in REGIONS {
        let control = engine
            .recall(region, query(FusionWeights::semantic_only(), filtered))
            .unwrap();
        assert_eq!(control.len(), 1);
        assert!(control[0].text.starts_with("distractor "));

        let hits = engine
            .recall(region, query(keyword_only(), filtered))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].text, TARGET, "{region}");
        assert_eq!(hits[0].relevance, Some(1.0));
    }
}

#[derive(Default)]
struct RecordingReranker {
    calls: Mutex<Vec<Vec<String>>>,
}

impl Reranker for RecordingReranker {
    fn model_id(&self) -> &str {
        "recording-overlap"
    }

    fn rerank_with_cancel(
        &self,
        query: &str,
        passages: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<f32>, EmbedError> {
        self.calls
            .lock()
            .unwrap()
            .push(passages.iter().map(|text| (*text).to_string()).collect());
        MockReranker.rerank_with_cancel(query, passages, cancel)
    }
}

fn assert_reranker_target(engine: &MemoryEngine, filtered: bool, candidate_count: usize) {
    let reranker = Arc::new(RecordingReranker::default());
    engine.set_reranker(reranker.clone(), RerankStrategy::Replace);
    for region in REGIONS {
        let hits = engine
            .recall(region, query(FusionWeights::semantic_only(), filtered))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].text, TARGET, "{region}");
        assert_eq!(hits[0].relevance, Some(1.0));
    }
    let calls = reranker.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    let mut expected = calls[0].clone();
    expected.sort();
    for call in calls.iter() {
        assert_eq!(call.len(), candidate_count);
        assert!(call.iter().any(|text| text == TARGET));
        let mut actual = call.clone();
        actual.sort();
        assert_eq!(actual, expected);
    }
    engine.clear_reranker();
}

#[test]
fn keyword_recall_uses_the_same_candidate_pool_for_plain_and_sealed_regions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path(), true);
    seed_small(&engine);
    assert_keyword_target(&engine, false);
    assert_keyword_target(&engine, false);
    drop(engine);

    let reopened = open_engine(dir.path(), false);
    assert_keyword_target(&reopened, false);
}

#[test]
fn replace_reranker_receives_the_same_plain_and_sealed_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path(), true);
    seed_small(&engine);
    assert_reranker_target(&engine, false, 65);
    assert_reranker_target(&engine, false, 65);
    drop(engine);

    let reopened = open_engine(dir.path(), false);
    assert_reranker_target(&reopened, false, 65);
}

#[test]
fn sealed_filters_fill_the_candidate_pool_instead_of_only_the_final_result() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path(), true);
    for region in REGIONS {
        let mut atoms = (0..4096)
            .map(|index| atom(format!("distractor {index}"), true, index < 2))
            .collect::<Vec<_>>();
        atoms.push(atom(TARGET, false, true));
        engine.remember_batch(region, atoms).unwrap();
    }
    assert_keyword_target(&engine, true);
    assert_reranker_target(&engine, true, 3);
}
