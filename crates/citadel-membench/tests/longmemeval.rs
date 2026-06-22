//! Token-free LongMemEval runner test: inline fixture + MockEmbedder + constant reader.

use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_ai::testing;
use citadel_mem::{Embedder, MemoryEngine, MockEmbedder};
use citadel_membench::benchmarks::longmemeval::{dataset, run, LmevalConfig};
use citadel_membench::{BenchConfig, Pacer};
use serde_json::json;

const DIM: usize = 64;

fn engine(path: &std::path::Path) -> MemoryEngine {
    let db: Arc<Database> = Arc::new(
        DatabaseBuilder::new(path.join("m.db"))
            .passphrase(b"test")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    MemoryEngine::open(db).unwrap()
}

fn fixture() -> serde_json::Value {
    json!([
        {
            "question_id": "q_first",
            "question_type": "single-session-user",
            "question": "what pet did I mention?",
            "answer": "a golden retriever named Rex",
            "question_date": "2023/05/20 (Sat) 02:21",
            "haystack_session_ids": ["answer_aaa_1", "noans_bbb_2"],
            "haystack_dates": ["2023/05/01 (Mon) 09:00", "2023/05/03 (Wed) 18:00"],
            "haystack_sessions": [
                [{"role": "user", "content": "My dog Rex is a golden retriever.", "has_answer": true},
                 {"role": "assistant", "content": "Rex sounds lovely!", "has_answer": false}],
                [{"role": "user", "content": "The weather was nice today.", "has_answer": false}]
            ],
            "answer_session_ids": ["answer_aaa_1"]
        },
        {
            "question_id": "q_second_abs",
            "question_type": "temporal-reasoning",
            "question": "when did I buy a car?",
            "answer": "not answerable: no car purchase was ever mentioned",
            "question_date": "2023/06/01 (Thu) 10:00",
            "haystack_session_ids": ["noans_ccc_1"],
            "haystack_dates": ["2023/05/10 (Wed) 12:00"],
            "haystack_sessions": [
                [{"role": "user", "content": "I went for a walk.", "has_answer": false}]
            ],
            "answer_session_ids": []
        }
    ])
}

#[test]
fn run_emits_one_hypothesis_per_question_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let samples = dataset::parse_root(&fixture()).unwrap();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    let reader = testing::constant("an answer");
    let pacer = Pacer::unbounded();
    let cfg = LmevalConfig {
        bench: BenchConfig::default(),
        encrypted: false,
        reader_concurrency: 2,
    };

    let mut emitted: Vec<(String, String)> = Vec::new();
    let out = run(
        &eng,
        &samples,
        embedder,
        &*reader,
        &pacer,
        &cfg,
        &mut |_, qid, hyp| {
            emitted.push((qid.to_string(), hyp.to_string()));
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(out.len(), 2);
    assert_eq!(out[0].0, "q_first");
    assert_eq!(out[1].0, "q_second_abs");
    assert!(out.iter().all(|(_, hyp)| hyp == "an answer"));
    assert_eq!(emitted.len(), 2);
}
