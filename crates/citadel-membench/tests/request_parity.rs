use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_llm::testing;
use citadel_mem::{MemoryEngine, MockEmbedder};
use citadel_membench::{run_sample, BenchConfig};
use serde_json::json;
use std::sync::Arc;

#[test]
fn ordinary_request_hashes_match_the_pre_accounting_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("test.cdl"))
        .passphrase(b"test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let engine = MemoryEngine::open(Arc::new(db)).unwrap();
    let samples = citadel_membench::parse_root(&json!([{
        "sample_id":"parity", "conversation": {
            "session_1_date_time":"2:00 pm on 1 January, 2024",
            "session_1":[{"speaker":"Alice", "dia_id":"D1:1", "text":"My dog is Rex."}]
        }, "qa":[{"question":"What is my dog's name?", "answer":"Rex", "category":4, "evidence":["D1:1"]}]
    }])).unwrap();
    let reader = testing::constant("Rex");
    let judge = testing::constant("CORRECT");
    let results = run_sample(
        &engine,
        &samples[0],
        Arc::new(MockEmbedder::new(8)),
        &*reader,
        &*judge,
        BenchConfig::default(),
    )
    .unwrap();
    let result = &results[0];
    // Frozen from the verified pre-accounting path with agentic disabled.
    assert_eq!(
        result.reader_calls[0].request_sha256,
        "7556220f6edb8f5604962c9455f8a2c943d6ac2bba8f2e62f49cf129241307a2"
    );
    assert_eq!(
        result.judge.as_ref().unwrap().call.request_sha256,
        "e2705487783f13f5a41e0f0afc8175f76e4f9c6532c60938b6ebebf9445bd8af"
    );
}
