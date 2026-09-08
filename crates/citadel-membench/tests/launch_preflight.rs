#![cfg(all(feature = "openai", feature = "candle-embed"))]

use std::path::Path;
use std::process::{Command, Output};

use serde_json::{json, Value};

fn fixture(benchmark: &str) -> Value {
    if benchmark == "locomo" {
        json!([{
            "sample_id": "preflight",
            "conversation": {
                "speaker_a": "Alice", "speaker_b": "Bob",
                "session_1": [{
                    "speaker": "Alice", "dia_id": "D1:1", "text": "My dog is Rex."
                }],
                "session_1_date_time": "2:00 pm on 1 January, 2024"
            },
            "qa": [{
                "question": "What is my dog's name?", "answer": "Rex",
                "category": 4, "evidence": ["D1:1"]
            }]
        }])
    } else {
        json!([{
            "question_id": "preflight",
            "question_type": "single-session-user",
            "question": "What is my dog's name?",
            "answer": "Rex",
            "question_date": "2023/05/20 (Sat) 02:21",
            "haystack_session_ids": ["first"],
            "haystack_dates": ["2023/05/01 (Mon) 09:00"],
            "haystack_sessions": [[{
                "role": "user", "content": "My dog is Rex.", "has_answer": true
            }]],
            "answer_session_ids": ["first"]
        }])
    }
}

fn launch(benchmark: &str, mode: &str, with_key: bool, scratch: &Path) -> Output {
    let input = scratch.join("input.json");
    std::fs::write(&input, serde_json::to_vec(&fixture(benchmark)).unwrap()).unwrap();
    let empty_model = scratch.join("empty-model");
    let db_dir = scratch.join("database");
    let temp_dir = scratch.join("tmp");
    for path in [&empty_model, &db_dir, &temp_dir] {
        std::fs::create_dir(path).unwrap();
    }
    let binary = match benchmark {
        "locomo" => env!("CARGO_BIN_EXE_locomo"),
        "longmemeval" => env!("CARGO_BIN_EXE_longmemeval"),
        _ => unreachable!(),
    };
    let mut command = Command::new(binary);
    command.env_clear();
    for key in ["SystemRoot", "PATH"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    let prefix = format!("CITADEL_{}", benchmark.to_ascii_uppercase());
    command
        .current_dir(scratch)
        .arg(input)
        .env(format!("{prefix}_MODE"), mode)
        .env("CITADEL_EMBEDDER_DIR", &empty_model)
        .env("CITADEL_RERANKER_DIR", &empty_model)
        .env("OPENAI_BASE_URL", "http://127.0.0.1:9/v1")
        .env("TMP", &temp_dir)
        .env("TEMP", &temp_dir)
        .env("TMPDIR", &temp_dir);
    if mode == "scored" {
        command.env(format!("{prefix}_DB_PATH"), db_dir.join("memory.cdl"));
    }
    if with_key {
        command.env("OPENAI_API_KEY", "preflight-test-not-a-credential");
    }
    let output = command.output().unwrap();
    assert_eq!(
        std::fs::read_dir(db_dir).unwrap().count(),
        0,
        "{benchmark}: preflight created database files"
    );
    assert_eq!(
        std::fs::read_dir(temp_dir).unwrap().count(),
        0,
        "{benchmark}: preflight created a temporary database"
    );
    output
}

#[test]
fn invalid_reranker_is_rejected_before_database_creation() {
    for benchmark in ["locomo", "longmemeval"] {
        for mode in ["scored", "retrieval-diag"] {
            let scratch = tempfile::tempdir().unwrap();
            let output = launch(benchmark, mode, mode == "scored", scratch.path());
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(!output.status.success(), "{benchmark}/{mode}: {error}");
            assert!(
                error.contains("reranker model:"),
                "{benchmark}/{mode} failed before checking the reranker: {error}"
            );
        }
    }
}

#[test]
fn missing_credentials_are_rejected_before_models_or_database() {
    for benchmark in ["locomo", "longmemeval"] {
        let scratch = tempfile::tempdir().unwrap();
        let output = launch(benchmark, "scored", false, scratch.path());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{benchmark}: {error}");
        assert!(
            error.contains("reader LLM:") && error.contains("requires OPENAI_API_KEY"),
            "{benchmark} did not reject the missing reader credential: {error}"
        );
    }
}

#[test]
fn dry_run_requires_neither_models_nor_credentials() {
    for benchmark in ["locomo", "longmemeval"] {
        let scratch = tempfile::tempdir().unwrap();
        let output = launch(benchmark, "dry-run", false, scratch.path());
        assert!(
            output.status.success(),
            "{benchmark}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
