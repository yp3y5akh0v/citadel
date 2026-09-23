//! Token-free LongMemEval runner test: inline fixture + MockEmbedder + constant
//! reader.

use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_llm::{testing, CompletionResponse, Message};
use citadel_mem::{AtomInput, Embedder, FetchQuery, MemoryEngine, MockEmbedder};
use citadel_membench::benchmarks::longmemeval::{
    dataset, ingest, prompts, retrieval, run, LmevalConfig,
};
use citadel_membench::{BenchConfig, BenchError, Pacer};
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
fn repeated_session_ids_preserve_occurrences_and_official_evidence_ids() {
    let samples = dataset::parse_root(&json!([{
        "question_id": "q_repeated",
        "question_type": "multi-session",
        "question": "what happened?",
        "answer": "three chats",
        "question_date": "2023/05/20 (Sat) 02:21",
        "haystack_session_ids": ["shared", "shared", "shared"],
        "haystack_dates": [
            "2023/05/03 (Wed) 09:00",
            "2023/05/01 (Mon) 09:00",
            "2023/05/03 (Wed) 09:00"
        ],
        "haystack_sessions": [
            [{"role": "user", "content": "first occurrence", "has_answer": true},
             {"role": "assistant", "content": "reply to first", "has_answer": false}],
            [{"role": "user", "content": "older occurrence", "has_answer": true}],
            [{"role": "user", "content": "same-date occurrence", "has_answer": true}]
        ],
        "answer_session_ids": ["shared"]
    }]))
    .unwrap();
    let sample = &samples[0];
    assert_eq!(
        sample
            .turns
            .iter()
            .map(|t| t.session_occurrence)
            .collect::<Vec<_>>(),
        [0, 0, 1, 2]
    );
    assert!(sample.turns.iter().all(|t| t.session_id == "shared"));
    assert_eq!(sample.evidence, ["shared"]);

    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    eng.create_region(&sample.question_id, Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    ingest::ingest_sample(&eng, &sample.question_id, sample).unwrap();
    let mut hits = eng
        .fetch_range(&sample.question_id, &FetchQuery::new(10))
        .unwrap();
    assert_eq!(
        hits.iter()
            .map(|h| h.payload["session_occurrence"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [0, 0, 1, 2]
    );
    assert_eq!(retrieval::distinct_session_ids(&hits), ["shared"]);
    let prompt =
        prompts::build_reader_prompt(&hits, &sample.question, &sample.question_date).unwrap();
    hits.reverse();
    let reversed =
        prompts::build_reader_prompt(&hits, &sample.question, &sample.question_date).unwrap();
    let (Message::User(text), Message::User(reversed_text)) = (&prompt[0], &reversed[0]) else {
        panic!("expected user messages");
    };
    assert_eq!(text, reversed_text);
    assert_eq!(text.matches("### Session ").count(), 3);
    let older = text.find("user: older occurrence").unwrap();
    let first = text.find("user: first occurrence").unwrap();
    let reply = text.find("assistant: reply to first").unwrap();
    let same_date = text.find("user: same-date occurrence").unwrap();
    assert!(older < first && first < reply && reply < same_date);
}

#[test]
fn source_dates_reject_invalid_values_and_preserve_unknown_dates() {
    let mut data = fixture();
    data[0]["haystack_dates"][0] = json!("2023/02/29 (Wed) 09:00");
    let error = dataset::parse_root(&data).unwrap_err();
    assert!(error
        .to_string()
        .contains("invalid haystack date at session occurrence 0"));
    data[0]["haystack_dates"][0] = json!("");
    let samples = dataset::parse_root(&data).unwrap();
    assert!(samples[0].turns[0].date.is_empty());
    assert_eq!(samples[0].turns[0].event_micros, None);
}

#[test]
fn invalid_or_inconsistent_dates_fail_before_ingestion_or_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let valid = dataset::parse_root(&fixture()).unwrap().remove(0);
    let mut malformed = valid.clone();
    malformed.turns[0].date = "bad date".into();
    let mut inconsistent = valid.clone();
    inconsistent.turns[0].event_micros = None;
    let mut question = valid;
    question.question_date = "2023/05/20 (Sun) 02:21".into();
    for sample in [malformed, inconsistent, question] {
        for result in [
            ingest::ingest_sample(&eng, "not-created", &sample).map(|_| ()),
            ingest::validate_reuse(&eng, "not-created", &sample),
        ] {
            assert!(matches!(result, Err(BenchError::Dataset(_))));
        }
    }
}

#[test]
fn reuse_accepts_exact_corpus_without_reingestion() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let samples = dataset::parse_root(&fixture()).unwrap();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    for sample in &samples {
        eng.create_region(&sample.question_id, Arc::clone(&embedder))
            .unwrap();
        ingest::ingest_sample(&eng, &sample.question_id, sample).unwrap();
    }
    let reader = testing::capturing(vec![
        CompletionResponse::text("first"),
        CompletionResponse::text("second"),
    ]);
    let output = run(
        &eng,
        &samples,
        embedder,
        &*reader.client(),
        &Pacer::unbounded(),
        &LmevalConfig {
            bench: BenchConfig::default(),
            encrypted: false,
            reuse: true,
            reader_concurrency: 1,
        },
        &mut |_, _, _| Ok(()),
    )
    .unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(reader.requests().len(), 2);
    for sample in &samples {
        assert_eq!(
            eng.count_region(&sample.question_id).unwrap(),
            sample.turns.len() as u64
        );
    }
}

#[test]
fn reuse_rejects_missing_region_empty_corpus_and_legacy_payload_before_reader_calls() {
    for cache_state in ["missing", "empty", "legacy"] {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let samples = dataset::parse_root(&fixture()).unwrap();
        let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
        let first = &samples[0];
        eng.create_region(&first.question_id, Arc::clone(&embedder))
            .unwrap();
        ingest::ingest_sample(&eng, &first.question_id, first).unwrap();
        let last = &samples[1];
        if cache_state != "missing" {
            eng.create_region(&last.question_id, Arc::clone(&embedder))
                .unwrap();
        }
        if cache_state == "legacy" {
            let atoms = last
                .turns
                .iter()
                .map(|t| {
                    AtomInput::new("turn", ingest::turn_content(t))
                        .with_payload(json!({
                            "session_id": t.session_id,
                            "role": t.role,
                            "has_answer": t.has_answer,
                        }))
                        .with_created_at(t.event_micros.unwrap())
                })
                .collect();
            eng.remember_batch(&last.question_id, atoms).unwrap();
        }
        let reader = testing::capturing(Vec::new());
        let error = run(
            &eng,
            &samples,
            Arc::clone(&embedder),
            &*reader.client(),
            &Pacer::unbounded(),
            &LmevalConfig {
                bench: BenchConfig::default(),
                encrypted: false,
                reuse: true,
                reader_concurrency: 1,
            },
            &mut |_, _, _| panic!("invalid cache must not emit"),
        )
        .unwrap_err();
        assert!(
            matches!(error, BenchError::Dataset(_)),
            "{cache_state}: {error}"
        );
        assert!(
            error.to_string().contains(&last.question_id),
            "{cache_state}: {error}"
        );
        assert!(reader.requests().is_empty(), "{cache_state}");
        if cache_state == "missing" {
            assert!(eng
                .attach_existing_region(&last.question_id, embedder)
                .is_err());
        }
    }
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
        reuse: false,
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
            assert!(!hyp.reader_calls.is_empty());
            emitted.push((qid.to_string(), hyp.answer.clone()));
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

#[test]
fn invalid_configuration_and_session_metadata_fail_before_ingestion() {
    for invalid in [
        "concurrency",
        "top_k",
        "duplicate_question",
        "session_id",
        "session_date",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let mut samples = dataset::parse_root(&fixture()).unwrap();
        let mut cfg = LmevalConfig {
            bench: BenchConfig::default(),
            encrypted: false,
            reuse: false,
            reader_concurrency: 1,
        };
        match invalid {
            "concurrency" => cfg.reader_concurrency = 0,
            "top_k" => cfg.bench.top_k = 0,
            "duplicate_question" => samples[1].question_id = samples[0].question_id.to_uppercase(),
            "session_id" => samples[0].turns[1].session_id = "other".into(),
            "session_date" => {
                let other = samples[0].turns[2].clone();
                samples[0].turns[1].date = other.date;
                samples[0].turns[1].event_micros = other.event_micros;
            }
            _ => unreachable!(),
        }
        let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
        let reader = testing::capturing(Vec::new());
        let error = run(
            &eng,
            &samples,
            embedder.clone(),
            &*reader.client(),
            &Pacer::unbounded(),
            &cfg,
            &mut |_, _, _| panic!("invalid input must not emit"),
        )
        .unwrap_err();
        assert!(
            matches!(error, BenchError::Dataset(_)),
            "{invalid}: {error}"
        );
        assert!(reader.requests().is_empty());
        assert!(eng
            .attach_existing_region(&samples[0].question_id, embedder)
            .is_err());
    }
}

/// Agentic path end-to-end: an aggregation question runs extract -> code
/// dedup/count -> answer (two reader calls); a non-aggregation question keeps
/// the single-prompt path. Scripted responses assert the call sequence.
#[test]
fn agentic_routes_aggregation_and_direct_questions_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    let eng = engine(dir.path());
    let samples = dataset::parse_root(&json!([
        {
            "question_id": "q_count",
            "question_type": "multi-session",
            "question": "How many pets did I mention?",
            "answer": "2",
            "question_date": "2023/05/20 (Sat) 02:21",
            "haystack_session_ids": ["answer_aaa_1"],
            "haystack_dates": ["2023/05/01 (Mon) 09:00"],
            "haystack_sessions": [
                [{"role": "user", "content": "My dog Rex and my cat Mia are pals.", "has_answer": true}]
            ],
            "answer_session_ids": ["answer_aaa_1"]
        },
        {
            "question_id": "q_plain",
            "question_type": "single-session-user",
            "question": "what pet did I mention?",
            "answer": "Rex",
            "question_date": "2023/05/20 (Sat) 02:21",
            "haystack_session_ids": ["answer_bbb_1"],
            "haystack_dates": ["2023/05/02 (Tue) 09:00"],
            "haystack_sessions": [
                [{"role": "user", "content": "My dog Rex is a golden retriever.", "has_answer": true}]
            ],
            "answer_session_ids": ["answer_bbb_1"]
        }
    ]))
    .unwrap();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    // Serial (concurrency 1): q_count consumes the first two scripted responses
    // (extraction JSON + final), q_plain the third (single-prompt).
    let reader = testing::scripted(vec![
        citadel_llm::CompletionResponse::text(
            r#"[{"item":"dog Rex","date":"2023/05/01","evidence":"My dog Rex and my cat Mia are pals.","amount":null},{"item":"cat Mia","date":"2023/05/01","evidence":"My dog Rex and my cat Mia are pals.","amount":null}]"#,
        ),
        citadel_llm::CompletionResponse::text("You mentioned 2 pets."),
        citadel_llm::CompletionResponse::text("Rex, a golden retriever."),
    ]);
    let cfg = LmevalConfig {
        bench: BenchConfig {
            agentic: true,
            ..BenchConfig::default()
        },
        encrypted: false,
        reuse: false,
        reader_concurrency: 1,
    };
    let out = run(
        &eng,
        &samples,
        embedder,
        &*reader,
        &Pacer::unbounded(),
        &cfg,
        &mut |_, _, _| Ok(()),
    )
    .unwrap();
    assert_eq!(out[0].1, "You mentioned 2 pets.", "agentic two-pass answer");
    assert_eq!(out[1].1, "Rex, a golden retriever.", "single-prompt path");
}
