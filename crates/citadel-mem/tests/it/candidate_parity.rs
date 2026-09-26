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

fn assert_payload_filter_parity(
    payload: serde_json::Value,
    filter: serde_json::Value,
    expected: bool,
) {
    fn check(engine: &MemoryEngine, filter: &serde_json::Value, expected: bool, phase: &str) {
        for region in REGIONS {
            let hits = engine
                .recall(
                    region,
                    RecallQuery::by_embedding(vec![1.0, 0.0], 1)
                        .with_weights(FusionWeights::semantic_only())
                        .with_payload_filter(filter.clone()),
                )
                .unwrap();
            assert_eq!(hits.len(), usize::from(expected), "{region}: {phase}");
            if expected {
                assert_eq!(hits[0].text, TARGET, "{region}: {phase}");
            }
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path(), true);
    for region in REGIONS {
        engine
            .remember(
                region,
                atom(TARGET, true, true).with_payload(payload.clone()),
            )
            .unwrap();
    }
    check(&engine, &filter, expected, "cold");
    check(&engine, &filter, expected, "warm");
    drop(engine);

    let reopened = open_engine(dir.path(), false);
    check(&reopened, &filter, expected, "reopened");
}

#[test]
fn payload_filter_does_not_match_an_object_inside_an_array_without_array_wrapper() {
    assert_payload_filter_parity(
        json!({"items": [{"a": 1, "b": 2}]}),
        json!({"items": {"a": 1}}),
        false,
    );
}

#[test]
fn payload_filter_does_not_match_a_scalar_inside_a_nested_array() {
    assert_payload_filter_parity(json!({"items": [[1]]}), json!({"items": 1}), false);
}

#[test]
fn payload_filter_matches_partial_objects_with_an_array_wrapper() {
    assert_payload_filter_parity(
        json!({"items": [{"a": 1, "b": 2}]}),
        json!({"items": [{"a": 1}]}),
        true,
    );
}

#[test]
fn payload_filter_matches_a_direct_scalar_array_member() {
    assert_payload_filter_parity(json!([1, 2]), json!(1), true);
}

#[test]
fn payload_filter_does_not_match_an_object_array_value_against_a_scalar() {
    assert_payload_filter_parity(json!({"items": [1, 2]}), json!({"items": 1}), false);
}

#[test]
fn payload_filter_matches_primitive_array_members_with_an_array_wrapper() {
    assert_payload_filter_parity(json!({"items": [1, 2]}), json!({"items": [1]}), true);
}

#[test]
fn payload_filter_preserves_integer_and_real_distinction() {
    assert_payload_filter_parity(json!({"items": 1}), json!({"items": 1.0}), false);
    assert_payload_filter_parity(json!({"items": 1.0}), json!({"items": 1}), false);
}

#[test]
fn payload_filter_preserves_array_nesting() {
    assert_payload_filter_parity(json!({"items": [[1, 2]]}), json!({"items": [1]}), false);
}

#[test]
fn payload_filter_does_not_match_an_exact_object_without_array_wrapper() {
    assert_payload_filter_parity(
        json!({"items": [{"a": 1}]}),
        json!({"items": {"a": 1}}),
        false,
    );
}

#[test]
fn payload_filter_matches_a_nested_array_with_matching_nesting() {
    assert_payload_filter_parity(json!({"items": [[1, 2]]}), json!({"items": [[1]]}), true);
}

fn assert_distance_order_parity(
    vectors: &[(&str, [f32; 2])],
    query: RecallQuery,
    expected: &[(&str, f32, Option<f32>)],
) {
    fn check(
        engine: &MemoryEngine,
        query: &RecallQuery,
        expected: &[(&str, f32, Option<f32>)],
        phase: &str,
    ) {
        for region in REGIONS {
            let hits = engine.recall(region, query.clone()).unwrap();
            assert_eq!(hits.len(), expected.len(), "{region}: {phase}");
            for (hit, &(text, score, distance)) in hits.iter().zip(expected) {
                assert_eq!(hit.text, text, "{region}: {phase}");
                assert_eq!(
                    hit.relevance.map(f32::to_bits),
                    Some(score.to_bits()),
                    "{region}: {phase}: {text}"
                );
                assert_eq!(
                    hit.distance.map(f32::to_bits),
                    distance.map(f32::to_bits),
                    "{region}: {phase}: {text}"
                );
            }
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path(), true);
    for region in REGIONS {
        for &(text, embedding) in vectors {
            engine
                .remember(
                    region,
                    atom(text, true, true).with_embedding(embedding.to_vec()),
                )
                .unwrap();
        }
    }
    check(&engine, &query, expected, "cold");
    check(&engine, &query, expected, "warm");
    drop(engine);

    let reopened = open_engine(dir.path(), false);
    check(&reopened, &query, expected, "reopened");
}

#[test]
fn semantic_recall_prefers_a_defined_exact_match_over_an_older_zero_vector() {
    assert_distance_order_parity(
        &[("zero", [0.0, 0.0]), ("exact", [1.0, 0.0])],
        RecallQuery::by_embedding(vec![1.0, 0.0], 1).with_weights(FusionWeights::semantic_only()),
        &[("exact", 0.0, Some(0.0))],
    );
}

#[test]
fn semantic_recall_prefers_the_worst_defined_distance_over_an_older_zero_vector() {
    assert_distance_order_parity(
        &[
            ("zero", [0.0, 0.0]),
            ("near", [1.0, 0.0]),
            ("far", [0.0, 1.0]),
        ],
        RecallQuery::by_embedding(vec![1.0, 0.0], 2).with_weights(FusionWeights::semantic_only()),
        &[("near", 1.0, Some(0.0)), ("far", 0.0, Some(1.0))],
    );
}

#[test]
fn semantic_recall_with_a_zero_query_preserves_id_ties_and_zero_scores() {
    assert_distance_order_parity(
        &[("zero", [0.0, 0.0]), ("nonzero", [1.0, 0.0])],
        RecallQuery::by_embedding(vec![0.0, 0.0], 2).with_weights(FusionWeights::semantic_only()),
        &[("zero", 0.0, None), ("nonzero", 0.0, None)],
    );
}

#[test]
fn zero_semantic_weight_does_not_prefer_defined_distances_on_score_ties() {
    assert_distance_order_parity(
        &[("zero", [0.0, 0.0]), ("nonzero", [1.0, 0.0])],
        RecallQuery::by_embedding(vec![1.0, 0.0], 2)
            .with_text("unmatched")
            .with_weights(keyword_only()),
        &[("zero", 0.0, None), ("nonzero", 0.0, Some(0.0))],
    );
}

#[test]
fn equal_defined_distances_preserve_id_ties_and_zero_scores() {
    assert_distance_order_parity(
        &[("first", [1.0, 0.0]), ("second", [1.0, 0.0])],
        RecallQuery::by_embedding(vec![1.0, 0.0], 2).with_weights(FusionWeights::semantic_only()),
        &[("first", 0.0, Some(0.0)), ("second", 0.0, Some(0.0))],
    );
}
