use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_llm::{factory, CompletionRequest, CompletionResponse, LLMClient};
use citadel_mem::{MemoryEngine, MockEmbedder};
use citadel_membench::{run_sample, sha256_hex, BenchConfig};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

fn capturing_constant(
    answer: &'static str,
    requests: Arc<Mutex<Vec<CompletionRequest>>>,
) -> Arc<dyn LLMClient> {
    // Preserve the original fixture's model, client identity, and token counter.
    factory::from_fn_with("const", factory::TokenCount::PerMessage(1), move |req| {
        requests.lock().unwrap().push(req.clone());
        Ok(CompletionResponse::text(answer))
    })
}

fn audit_digest(request: &CompletionRequest, canonical: &str) -> String {
    sha256_hex(
        &serde_json::to_vec(&json!({
            "schema": "citadel-membench-call-v1",
            "model_id": "const",
            "client": citadel_llm::ClientRequestIdentity::in_process(),
            "canonical_request": canonical,
            "seed": request.seed,
        }))
        .unwrap(),
    )
}

#[test]
fn ordinary_requests_stay_unchanged_when_seed_enters_canonical_identity() {
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
    let reader_requests = Arc::new(Mutex::new(Vec::new()));
    let judge_requests = Arc::new(Mutex::new(Vec::new()));
    let reader = capturing_constant("Rex", Arc::clone(&reader_requests));
    let judge = capturing_constant("CORRECT", Arc::clone(&judge_requests));
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
    let reader_requests = reader_requests.lock().unwrap();
    let judge_requests = judge_requests.lock().unwrap();
    assert_eq!(reader_requests.len(), 1);
    assert_eq!(judge_requests.len(), 1);
    for (request, actual, previous_digest) in [
        (
            &reader_requests[0],
            &result.reader_calls[0].request_sha256,
            "7556220f6edb8f5604962c9455f8a2c943d6ac2bba8f2e62f49cf129241307a2",
        ),
        (
            &judge_requests[0],
            &result.judge.as_ref().unwrap().call.request_sha256,
            "e2705487783f13f5a41e0f0afc8175f76e4f9c6532c60938b6ebebf9445bd8af",
        ),
    ] {
        let canonical = citadel_llm::canonical_json(request);
        let mut previous: Value = serde_json::from_str(&canonical).unwrap();
        assert_eq!(previous.get("seed"), Some(&json!(request.seed)));
        previous.as_object_mut().unwrap().remove("seed");
        // The original fixtures still bind every request option, including the
        // seed in the outer envelope. Only its additional presence inside the
        // canonical representation changed; no old encoding is used at runtime.
        assert_eq!(
            audit_digest(request, &previous.to_string()),
            previous_digest
        );
        assert_eq!(*actual, audit_digest(request, &canonical));
        assert_ne!(actual, previous_digest);
    }
}
