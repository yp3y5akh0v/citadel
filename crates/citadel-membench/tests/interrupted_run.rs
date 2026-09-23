//! Completed and failed concurrent questions must survive an interrupted run.
use std::sync::{Arc, Barrier};

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_llm::{CompletionResponse, LlmError, Message, TokenUsage};
use citadel_mem::{MemoryEngine, MockEmbedder};
use citadel_membench::{BenchConfig, BenchError, Pacer, QuestionEvent};
use serde_json::{json, Value};

struct ConcurrencyGuard(Option<std::ffi::OsString>);
impl ConcurrencyGuard {
    fn two_workers() -> Self {
        let previous = std::env::var_os("CITADEL_LOCOMO_CONCURRENCY");
        std::env::set_var("CITADEL_LOCOMO_CONCURRENCY", "2");
        Self(previous)
    }
}
impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        match &self.0 {
            Some(value) => std::env::set_var("CITADEL_LOCOMO_CONCURRENCY", value),
            None => std::env::remove_var("CITADEL_LOCOMO_CONCURRENCY"),
        }
    }
}

fn engine() -> (tempfile::TempDir, MemoryEngine) {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("test.cdl"))
        .passphrase(b"test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let engine = MemoryEngine::open(Arc::new(db)).unwrap();
    (dir, engine)
}

fn reader() -> Arc<dyn citadel_llm::LLMClient> {
    let barrier = Arc::new(Barrier::new(2));
    citadel_llm::factory::from_fn("gpt-4o-mini", move |request| {
        barrier.wait();
        let fails = request.messages.iter().any(|message| match message {
            Message::User(text) => text.contains("FAIL_QUESTION"),
            _ => false,
        });
        if fails {
            return Err(LlmError::Backend("one failed sibling".into()));
        }
        let mut response = CompletionResponse::text("retained answer");
        response.usage = TokenUsage {
            input_tokens: 20,
            output_tokens: 4,
            cost_usd: None,
        };
        Ok(response)
    })
}

fn assert_complete_interruption(error: BenchError, journal: Vec<u8>, call_count: usize) {
    let BenchError::Questions(batch) = error else {
        panic!("missing batch receipts")
    };
    assert_eq!(batch.failures.len(), 1);
    assert_eq!(batch.completed.len(), 1);
    assert_eq!(batch.completed[0].output.answer, "retained answer");
    assert_eq!(batch.completed[0].calls.len(), call_count);
    assert_eq!(batch.failures[0].calls.len(), 1);
    assert_eq!(batch.accounting().observed_input_tokens, 20);
    assert_eq!(batch.accounting().unknown_usage_attempts, 1);
    assert_eq!(batch.accounting().estimated_cost_usd, None);
    let rows: Vec<Value> = String::from_utf8(journal)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .filter(|row| row["event"] == "completed")
            .count(),
        1
    );
    assert_eq!(
        rows.iter().filter(|row| row["event"] == "failed").count(),
        1
    );
    let serialized = serde_json::to_value(batch).unwrap();
    assert_eq!(serialized["accounting"]["observed_input_tokens"], 20);
}

#[test]
fn locomo_keeps_completed_sibling_and_unconditional_journal() {
    // This integration-test process owns this variable; the other test uses
    // the LME runner's explicit concurrency configuration.
    let _concurrency = ConcurrencyGuard::two_workers();
    let samples = citadel_membench::parse_root(&json!([{
        "sample_id":"mixed", "conversation": {
            "session_1_date_time":"2:00 pm on 1 January, 2024",
            "session_1":[{"speaker":"Alice", "dia_id":"D1:1", "text":"A retained fact."}]
        }, "qa":[
            {"question":"SUCCESS_QUESTION?", "answer":"retained answer", "category":4, "evidence":["D1:1"]},
            {"question":"FAIL_QUESTION?", "answer":"irrelevant", "category":4, "evidence":["D1:1"]}
        ]
    }])).unwrap();
    let (_dir, engine) = engine();
    let reader = reader();
    let judge = citadel_llm::testing::constant("CORRECT");
    let mut journal = Vec::new();
    let error = citadel_membench::run_sample_observed(
        &engine,
        &samples[0],
        Arc::new(MockEmbedder::new(8)),
        &*reader,
        &*judge,
        BenchConfig::default(),
        false,
        &Pacer::unbounded(),
        &mut |event| match event {
            QuestionEvent::Completed(completed) => {
                completed.completion_receipt().write_json_line(&mut journal)
            }
            QuestionEvent::Failed(failure) => failure.write_json_line(&mut journal),
        },
    )
    .unwrap_err();
    assert_complete_interruption(error, journal, 2);
}

#[test]
fn longmemeval_keeps_completed_sibling_and_unconditional_journal() {
    use citadel_membench::benchmarks::longmemeval::{dataset, run, LmevalConfig};
    let samples = dataset::parse_root(&json!([
        {"question_id":"success", "question_type":"single-session-user", "question":"SUCCESS_QUESTION?", "answer":"retained answer", "question_date":"2024/01/02 (Tue) 12:00", "haystack_session_ids":["s1"], "haystack_dates":["2024/01/01 (Mon) 12:00"], "haystack_sessions":[[{"role":"user", "content":"A retained fact.", "has_answer":true}]], "answer_session_ids":["s1"]},
        {"question_id":"failure", "question_type":"single-session-user", "question":"FAIL_QUESTION?", "answer":"irrelevant", "question_date":"2024/01/02 (Tue) 12:00", "haystack_session_ids":["s1"], "haystack_dates":["2024/01/01 (Mon) 12:00"], "haystack_sessions":[[{"role":"user", "content":"A retained fact.", "has_answer":true}]], "answer_session_ids":["s1"]}
    ])).unwrap();
    let (_dir, engine) = engine();
    let reader = reader();
    let cfg = LmevalConfig {
        bench: BenchConfig::default(),
        encrypted: false,
        reuse: false,
        reader_concurrency: 2,
    };
    let mut journal = Vec::new();
    let error = run(
        &engine,
        &samples,
        Arc::new(MockEmbedder::new(8)),
        &*reader,
        &Pacer::unbounded(),
        &cfg,
        &mut |event| match event {
            QuestionEvent::Completed(completed) => {
                completed.completion_receipt().write_json_line(&mut journal)
            }
            QuestionEvent::Failed(failure) => failure.write_json_line(&mut journal),
        },
    )
    .unwrap_err();
    assert_complete_interruption(error, journal, 1);
}
