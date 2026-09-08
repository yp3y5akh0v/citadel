//! Token-free harness tests: no network, no real model files. Everything runs
//! against an inline LoCoMo-shaped fixture, a `MockEmbedder`, and the
//! `citadel_llm::testing` client toolkit.

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_llm::{testing, CompletionResponse, FinishReason, LlmError, Message, TokenUsage};
use citadel_mem::{Embedder, MemoryEngine, MockEmbedder};
use citadel_membench::core::eval::CompletionFinish;
use citadel_membench::{
    aggregate, build_reader_prompt, ingest_sample, judge_abstained, judge_correct, parse_root,
    provenance, reader_view, run_sample, run_sample_observed, turn_content, BenchConfig,
    BenchError, Category, Pacer, QuestionResult, ReaderOrder, Turn,
};
use serde_json::{json, Value};

const DIM: usize = 64;

/// A 3-session conversation with QA in every category (incl. adversarial), one
/// non-string answer (scalar rendering), and sibling `_date_time`/`_summary`
/// keys the loader must not treat as sessions.
fn fixture() -> Value {
    json!([{
        "sample_id": "conv_alpha",
        "conversation": {
            "speaker_a": "Alice",
            "speaker_b": "Bob",
            "session_1": [
                {"speaker": "Alice", "dia_id": "D1:1", "text": "I adopted a dog named Rex."},
                {"speaker": "Bob", "dia_id": "D1:2", "text": "Nice! What breed is Rex?"}
            ],
            "session_1_date_time": "2:00 pm on 1 January, 2024",
            "session_1_summary": "Alice got a dog.",
            "session_2": [
                {"speaker": "Alice", "dia_id": "D2:1", "text": "Rex is a golden retriever."},
                {"speaker": "Alice", "dia_id": "D2:2", "text": "I paid 1200 dollars for him."}
            ],
            "session_2_date_time": "3:00 pm on 5 January, 2024",
            "session_2_observation": "ignore me",
            "session_10": [
                {"speaker": "Bob", "dia_id": "D10:1", "text": "We hiked Mount Tam last weekend."}
            ],
            "session_10_date_time": "12:00 pm on 20 March, 2024"
        },
        "qa": [
            {"question": "What breed is Rex?", "answer": "golden retriever",
             "category": 4, "evidence": ["D2:1"]},
            {"question": "How much did Alice pay for Rex?", "answer": 1200,
             "category": 1, "evidence": ["D2:2"]},
            {"question": "When did Alice get Rex relative to the hike?",
             "answer": "before", "category": 2, "evidence": ["D1:1", "D10:1"]},
            {"question": "What is the capital of France?", "answer": "Paris",
             "category": 3, "evidence": []},
            {"question": "What car does Alice drive?", "answer": "no information",
             "category": 5, "evidence": []}
        ]
    }])
}

fn open_engine() -> (tempfile::TempDir, MemoryEngine) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("t.cdl"))
            .passphrase(b"membench")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let eng = MemoryEngine::open(db).unwrap();
    (dir, eng)
}

#[test]
fn loader_roundtrip_with_dynamic_keys_and_nonstring_answer() {
    let samples = parse_root(&fixture()).unwrap();
    assert_eq!(samples.len(), 1);
    let s = &samples[0];
    assert_eq!(s.sample_id, "conv_alpha");

    // 2 + 2 + 1 turns; _summary/_observation/_date_time siblings are not
    // sessions.
    assert_eq!(s.turns.len(), 5);
    // Sorted by session number, so session_10 sorts after session_2
    // numerically.
    assert_eq!(s.turns.last().unwrap().session, 10);
    assert_eq!(
        s.turns.last().unwrap().text,
        "We hiked Mount Tam last weekend."
    );
    // date_time is paired from the matching `session_<n>_date_time`.
    assert_eq!(s.turns[0].date_time, "2:00 pm on 1 January, 2024");
    assert_eq!(s.turns[0].dia_id, "D1:1");

    assert_eq!(s.qa.len(), 5);
    // Non-string answer (number 1200) rendered to a plain string.
    let multi =
        s.qa.iter()
            .find(|q| q.category == Category::MultiHop)
            .unwrap();
    assert_eq!(multi.gold, "1200");
    // Categories mapped correctly, incl. the adversarial one.
    assert!(s.qa.iter().any(|q| q.category == Category::Adversarial));
    assert!(s.qa.iter().any(|q| q.category == Category::SingleHop));
}

#[test]
fn invalid_source_dates_fail_before_ingestion_or_reuse() {
    let (_dir, eng) = open_engine();
    let mut sample = parse_root(&fixture()).unwrap().remove(0);
    sample.turns[0].date_time = "2pm on 1 Jan 2024".into();
    for result in [
        ingest_sample(&eng, "not-created", &sample).map(|_| ()),
        citadel_membench::benchmarks::locomo::ingest::validate_reuse(&eng, "not-created", &sample),
    ] {
        assert!(matches!(result, Err(BenchError::Dataset(_))));
    }
}

#[test]
fn category_guard_rejects_out_of_range() {
    let mut bad = fixture();
    bad[0]["qa"][0]["category"] = json!(7);
    assert!(parse_root(&bad).is_err(), "category 7 must be rejected");

    // Truncation trap: 261u64 as u8 == 5; must be rejected, not read as
    // Adversarial.
    let mut wrap = fixture();
    wrap[0]["qa"][0]["category"] = json!(261);
    assert!(
        parse_root(&wrap).is_err(),
        "category 261 must be rejected, not truncated to 5 (Adversarial)"
    );
}

#[test]
fn category_mapping_matches_locomo_data() {
    // Guards the 2=temporal / 3=open-domain / 4=single-hop mapping against
    // re-swapping.
    assert_eq!(Category::from_int(1).unwrap(), Category::MultiHop);
    assert_eq!(Category::from_int(2).unwrap(), Category::Temporal);
    assert_eq!(Category::from_int(3).unwrap(), Category::OpenDomain);
    assert_eq!(Category::from_int(4).unwrap(), Category::SingleHop);
    assert_eq!(Category::from_int(5).unwrap(), Category::Adversarial);
}

#[test]
fn ingest_count_equals_turn_count() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    eng.create_region(&s.sample_id, embedder).unwrap();

    let ids = ingest_sample(&eng, &s.sample_id, s).unwrap();
    assert_eq!(ids.len(), s.turns.len(), "one atom per turn");
}

#[test]
fn turn_content_folds_date_speaker_caption_and_query() {
    let samples = parse_root(&fixture()).unwrap();
    assert_eq!(
        turn_content(&samples[0].turns[0]),
        "[2:00 pm on 1 January, 2024] Alice: I adopted a dog named Rex."
    );

    let full = Turn {
        session: 1,
        date_time: "1:56 pm on 8 May, 2023".to_string(),
        speaker: "Bob".to_string(),
        dia_id: "D1:9".to_string(),
        text: "Look at this.".to_string(),
        blip_caption: "a red barn".to_string(),
        query: "barn sunset".to_string(),
    };
    assert_eq!(
        turn_content(&full),
        "[1:56 pm on 8 May, 2023] Bob: Look at this. \
         [shared a photo: a red barn] [image search: barn sunset]"
    );

    let undated = Turn {
        date_time: String::new(),
        ..full
    };
    assert_eq!(
        turn_content(&undated),
        "Bob: Look at this. [shared a photo: a red barn] [image search: barn sunset]"
    );
}

#[test]
fn reader_view_expands_neighbors_dedups_and_orders() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    eng.create_region(&s.sample_id, embedder).unwrap();
    let ids = ingest_sample(&eng, &s.sample_id, s).unwrap();
    let hit = |i: usize| eng.fetch_one(&s.sample_id, ids[i]).unwrap().unwrap();
    let view_ids = |hits, config| {
        reader_view(&eng, &s.sample_id, hits, config)
            .unwrap()
            .iter()
            .map(|h| h.id)
            .collect::<Vec<_>>()
    };

    // A middle hit pulls in its previous and next turns, in conversation order.
    let chrono = BenchConfig {
        reader_order: ReaderOrder::Chrono,
        neighbor_radius: 1,
        ..BenchConfig::default()
    };
    assert_eq!(
        view_ids(vec![hit(2)], chrono),
        vec![ids[1], ids[2], ids[3]],
        "radius-1 chrono view around a middle turn"
    );

    // The first turn has no predecessor in the region: nothing fetched, no
    // error.
    assert_eq!(view_ids(vec![hit(0)], chrono), vec![ids[0], ids[1]]);

    // Adjacent hits share neighbors exactly once.
    assert_eq!(
        view_ids(vec![hit(2), hit(1)], chrono),
        vec![ids[0], ids[1], ids[2], ids[3]],
        "overlapping windows dedup"
    );

    // The default (relevance order, no expansion) passes the hits through
    // untouched.
    assert_eq!(
        view_ids(vec![hit(3), hit(1)], BenchConfig::default()),
        vec![ids[3], ids[1]]
    );
    let owned = hit(3);
    let text_allocation = owned.text.as_ptr();
    let unchanged = reader_view(&eng, &s.sample_id, vec![owned], BenchConfig::default()).unwrap();
    assert_eq!(unchanged[0].text.as_ptr(), text_allocation);
}

#[test]
fn session_prompt_rejects_invalid_or_conflicting_metadata() {
    let (_dir, eng) = open_engine();
    eng.create_region("metadata", Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    let sample = parse_root(&fixture()).unwrap().remove(0);
    let ids = ingest_sample(&eng, "metadata", &sample).unwrap();
    let first = eng.fetch_one("metadata", ids[0]).unwrap().unwrap();
    for (field, value) in [("session", json!("1")), ("date_time", json!(null))] {
        let mut invalid = first.clone();
        invalid.payload[field] = value;
        assert!(matches!(
            build_reader_prompt(&[invalid], "question", true),
            Err(BenchError::Dataset(_))
        ));
    }
    let mut second = eng.fetch_one("metadata", ids[1]).unwrap().unwrap();
    second.payload["date_time"] = json!("a conflicting date");
    assert!(build_reader_prompt(&[first, second], "question", true)
        .unwrap_err()
        .to_string()
        .contains("conflicting dates"));
}

#[test]
fn session_reader_order_keeps_best_session_first_and_turns_chronological() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    eng.create_region(&s.sample_id, embedder).unwrap();
    let ids = ingest_sample(&eng, &s.sample_id, s).unwrap();
    let hit = |i: usize| eng.fetch_one(&s.sample_id, ids[i]).unwrap().unwrap();
    let config = BenchConfig {
        reader_order: ReaderOrder::Sessions,
        ..BenchConfig::default()
    };

    // Session 2 arrives out of conversation order, so it must be restored.
    let grouped = reader_view(
        &eng,
        &s.sample_id,
        vec![hit(4), hit(0), hit(3), hit(2)],
        config,
    )
    .unwrap();
    assert_eq!(
        grouped.iter().map(|hit| hit.id).collect::<Vec<_>>(),
        vec![ids[4], ids[0], ids[2], ids[3]]
    );

    let rendered = render(&build_reader_prompt(&grouped, "What happened?", true).unwrap());
    let s10 = rendered
        .find("[Session 10 from 12:00 pm on 20 March, 2024]")
        .unwrap();
    let s1 = rendered
        .find("[Session 1 from 2:00 pm on 1 January, 2024]")
        .unwrap();
    let s2 = rendered
        .find("[Session 2 from 3:00 pm on 5 January, 2024]")
        .unwrap();
    assert!(s10 < s1 && s1 < s2, "session block relevance order");
    assert!(
        rendered.find("Rex is a golden retriever").unwrap()
            < rendered.find("I paid 1200 dollars").unwrap(),
        "conversation order inside session 2"
    );

    // LongMemEval uses string session_id metadata. The same ordering contract
    // applies, while an atom with neither schema fails instead of flattening.
    let mut later = hit(3);
    let mut earlier = hit(2);
    for item in [&mut later, &mut earlier] {
        let payload = item.payload.as_object_mut().unwrap();
        payload.remove("session");
        payload.insert("session_id".into(), serde_json::json!("session-2"));
    }
    let grouped = reader_view(&eng, &s.sample_id, vec![later, earlier], config).unwrap();
    assert_eq!(
        grouped.iter().map(|hit| hit.id).collect::<Vec<_>>(),
        vec![ids[2], ids[3]]
    );

    let mut missing = hit(0);
    missing.payload.as_object_mut().unwrap().remove("session");
    let error = reader_view(&eng, &s.sample_id, vec![missing], config).unwrap_err();
    assert!(error
        .to_string()
        .contains("requires numeric payload.session"));
}

#[test]
fn reader_prompt_contains_only_passed_hits_not_gold_or_evidence() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    eng.create_region(&s.sample_id, embedder).unwrap();
    ingest_sample(&eng, &s.sample_id, s).unwrap();

    // Retrieve a single hit, then build the prompt from only that hit.
    let hits = eng
        .recall(
            &s.sample_id,
            citadel_mem::RecallQuery::by_text("What breed is Rex?", 1),
        )
        .unwrap();
    assert_eq!(hits.len(), 1, "k=1 yields exactly one hit");
    let retrieved_text = hits[0].text.clone();

    let prompt = build_reader_prompt(&hits, "What breed is Rex?", false).unwrap();
    let blob = render(&prompt);

    // The single retrieved turn and the question are present.
    assert!(blob.contains(&retrieved_text));
    assert!(blob.contains("What breed is Rex?"));

    // No non-retrieved turn leaks in. Identify the retrieved turn by dia_id,
    // since its raw text is a substring of the speaker-prefixed hit.
    let retrieved_dia = hits[0]
        .payload
        .get("dia_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    for turn in &s.turns {
        if turn.dia_id != retrieved_dia {
            assert!(
                !blob.contains(&turn.text),
                "non-retrieved turn leaked: {}",
                turn.text
            );
        }
    }
    // No gold answer and no evidence id leak in.
    assert!(!blob.contains("golden retriever") || retrieved_text.contains("golden retriever"));
    for qa in &s.qa {
        for ev in &qa.evidence {
            assert!(!blob.contains(ev), "evidence id leaked: {ev}");
        }
    }
}

#[test]
fn aggregate_excludes_adversarial_from_overall_and_reports_abstention() {
    let results = vec![
        res(Category::SingleHop, true),
        res(Category::MultiHop, false),
        res(Category::Temporal, true),
        res(Category::OpenDomain, true),
        // Two adversarial: one abstained (correct), one fabricated (wrong).
        res(Category::Adversarial, true),
        res(Category::Adversarial, false),
    ];
    let report = aggregate(&results, prov());

    // Overall covers only the 4 scored questions (3 correct of 4).
    assert_eq!(report.overall_total, 4);
    assert_eq!(report.overall_correct, 3);
    assert!((report.overall_accuracy - 0.75).abs() < 1e-9);

    // Adversarial is excluded from per_category scored map and overall.
    assert!(!report.per_category.contains_key("adversarial"));
    assert_eq!(report.adversarial_total, 2);
    assert!((report.adversarial_abstention - 0.5).abs() < 1e-9);
}

#[test]
fn judge_parses_final_json_and_exact_plain_labels() {
    for (reply, expected) in [
        ("CORRECT", true),
        ("WRONG", false),
        (r#"{"label":"CORRECT"}"#, true),
        (r#"{"label":"WRONG"}"#, false),
        ("The dates match.\n{\"label\":\"CORRECT\"}\n\n", true),
        ("The dates differ.\n{\"label\":\"WRONG\"}", false),
        (
            "Example: {\"label\":\"CORRECT\"}\n{\"label\":\"WRONG\"}",
            false,
        ),
        (
            "Example: {\"label\":\"WRONG\"}\n{\"label\":\"CORRECT\"}",
            true,
        ),
    ] {
        let judge = testing::reply_once(reply);
        let (actual, _) = judge_correct(&*judge, &Pacer::unbounded(), "q", "gold", "pred").unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn judge_rejects_invalid_verdicts_and_preserves_response() {
    for reply in [
        "",
        "INCORRECT",
        "NOT CORRECT",
        "This is not correct, it is WRONG",
        "correct",
        r#""label":"CORRECT""#,
        r#"{"label":"CORRECT""#,
        r#"{"label":"INCORRECT"}"#,
        r#"{"label":"correct"}"#,
        r#"{"label":true}"#,
        r#"{"label":"CORRECT","extra":0}"#,
        r#"{"label":"WRONG","label":"CORRECT"}"#,
        "{\"label\":\"CORRECT\"}\nmore text",
    ] {
        let judge = testing::reply_once(reply);
        let error = judge_correct(&*judge, &Pacer::unbounded(), "q", "gold", "pred").unwrap_err();
        let BenchError::InvalidJudgeResponse {
            response,
            finish_reason,
            ..
        } = error
        else {
            panic!("expected an invalid judge response");
        };
        assert_eq!(response, reply);
        assert_eq!(finish_reason, FinishReason::Stop);
    }
}

#[test]
fn judge_rejects_abnormal_completions_even_with_valid_labels() {
    for finish_reason in [
        FinishReason::Length,
        FinishReason::ToolUse,
        FinishReason::Refusal,
        FinishReason::ContentFilter,
        FinishReason::Error,
    ] {
        let mut response = CompletionResponse::text("CORRECT");
        response.finish_reason = finish_reason;
        response.usage = TokenUsage {
            input_tokens: 12,
            output_tokens: 3,
            cost_usd: Some(0.01),
        };
        let expected_usage = response.usage;
        for abstention in [false, true] {
            let judge = testing::scripted(vec![response.clone()]);
            let result = if abstention {
                judge_abstained(&*judge, &Pacer::unbounded(), "q", "pred")
            } else {
                judge_correct(&*judge, &Pacer::unbounded(), "q", "gold", "pred")
            };
            let BenchError::InvalidJudgeResponse {
                response,
                finish_reason: actual_finish,
                usage,
                ..
            } = result.unwrap_err()
            else {
                panic!("expected an invalid judge response");
            };
            assert_eq!(response, "CORRECT");
            assert_eq!(actual_finish, finish_reason);
            assert_eq!(usage, expected_usage);
        }
    }
}

#[test]
fn abstention_judge_requires_an_exact_response() {
    for (reply, expected) in [(" CORRECT\n", true), ("WRONG", false)] {
        let judge = testing::reply_once(reply);
        assert_eq!(
            judge_abstained(&*judge, &Pacer::unbounded(), "q", "pred")
                .unwrap()
                .0,
            expected
        );
    }
    for reply in [
        "",
        "INCORRECT",
        "NOT CORRECT",
        "CORRECT but WRONG",
        "CORRECT\nWRONG",
        r#"{"label":"CORRECT"}"#,
    ] {
        let judge = testing::reply_once(reply);
        assert!(matches!(
            judge_abstained(&*judge, &Pacer::unbounded(), "q", "pred"),
            Err(BenchError::InvalidJudgeResponse { .. })
        ));
    }
}

#[test]
fn invalid_judge_response_aborts_run_without_emitting_a_score() {
    let mut sample = parse_root(&fixture()).unwrap().remove(0);
    sample.qa.truncate(1);
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    let reader = testing::constant("golden retriever");
    let judge = testing::constant("INCORRECT");
    let mut observed = 0;
    let result = run_sample_observed(
        &eng,
        &sample,
        embedder,
        &*reader,
        &*judge,
        BenchConfig::default(),
        false,
        &Pacer::unbounded(),
        &mut |_| {
            observed += 1;
            Ok(())
        },
    );
    assert!(matches!(
        result,
        Err(BenchError::InvalidJudgeResponse { .. })
    ));
    assert_eq!(observed, 0);
}

#[test]
fn run_sample_preserves_reader_and_judge_completion_audit() {
    let mut sample = parse_root(&fixture()).unwrap().remove(0);
    sample.qa.truncate(1);
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    let mut reader_response = CompletionResponse::text("partial answer");
    reader_response.finish_reason = FinishReason::Length;
    reader_response.usage = TokenUsage {
        input_tokens: 100,
        output_tokens: 20,
        cost_usd: None,
    };
    let reader = citadel_llm::factory::from_fn("gpt-4o", move |_| Ok(reader_response.clone()));
    let raw_judge = "The answer is incomplete.\n{\"label\":\"WRONG\"}";
    let mut judge_response = CompletionResponse::text(raw_judge);
    judge_response.usage = TokenUsage {
        input_tokens: 12,
        output_tokens: 7,
        cost_usd: Some(0.01),
    };
    let judge = citadel_llm::factory::from_fn("gpt-4o-mini", move |_| Ok(judge_response.clone()));

    let results = run_sample(
        &eng,
        &sample,
        embedder,
        &*reader,
        &*judge,
        BenchConfig::default(),
    )
    .unwrap();
    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert!(result.scorable);
    assert!(!result.correct);
    assert_eq!(result.predicted, "partial answer");
    assert_eq!(result.reader_finish_reasons, [CompletionFinish::Length]);
    assert_eq!(result.reader_calls.len(), 1);
    assert_eq!(result.reader_calls[0].request_sha256.len(), 64);
    assert_eq!(result.reader_calls[0].max_output_tokens, Some(512));
    assert_eq!(result.reader_calls[0].model_id, "gpt-4o");
    let judge_audit = &result.judge.as_ref().unwrap().call;
    assert_eq!(judge_audit.model_id, "gpt-4o-mini");
    assert_eq!(judge_audit.request_sha256.len(), 64);
    assert!(judge_audit.rendered_atom_ids.is_empty());
    assert_eq!(judge_audit.usage.input_tokens, 12);
    assert_eq!(judge_audit.usage.output_tokens, 7);
    assert!((result.cost_usd.unwrap() - 0.000456).abs() < 1e-12);
    let row = serde_json::to_value(result).unwrap();
    assert_eq!(row["reader_finish_reasons"], json!(["length"]));
    assert_eq!(row["judge"]["response"], raw_judge);
    assert_eq!(row["judge"]["finish_reason"], "stop");
    assert_eq!(row["judge"]["correct"], false);
    assert_eq!(
        row["judge"]["usage"],
        json!({"input_tokens":12,"output_tokens":7,"cost_usd":0.01})
    );
    assert_eq!(aggregate(&results, prov()).overall_total, 1);
}

#[test]
fn agentic_audit_preserves_both_completions_including_fallback() {
    for (extraction, extraction_finish, answer_finish) in [
        (
            r#"[{"item":"Rex"}]"#,
            FinishReason::Length,
            FinishReason::Stop,
        ),
        (
            "NOT_ENUMERATION",
            FinishReason::ContentFilter,
            FinishReason::Length,
        ),
    ] {
        let mut sample = parse_root(&fixture()).unwrap().remove(0);
        sample.qa.truncate(1);
        sample.qa[0].question = "How many dogs did Alice adopt?".into();
        for turn in &mut sample.turns {
            if turn.session == 1 {
                turn.date_time = "2:00 pm on 10 January, 2024".into();
            }
        }
        let (_dir, eng) = open_engine();
        let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
        let mut extracted = CompletionResponse::text(extraction);
        extracted.finish_reason = extraction_finish;
        let mut answer = CompletionResponse::text("One dog.");
        answer.finish_reason = answer_finish;
        let reader = testing::capturing(vec![extracted, answer]);
        let judge = testing::reply_once("CORRECT");
        let config = BenchConfig {
            agentic: true,
            reader_order: ReaderOrder::Chrono,
            ..BenchConfig::default()
        };
        let results =
            run_sample(&eng, &sample, embedder, &*reader.client(), &*judge, config).unwrap();
        assert_eq!(
            results[0].reader_finish_reasons,
            [extraction_finish.into(), answer_finish.into()]
        );
        assert_eq!(results[0].predicted, "One dog.");
        assert_eq!(results[0].reader_calls.len(), 2);
        assert_ne!(
            results[0].reader_calls[0].request_sha256,
            results[0].reader_calls[1].request_sha256
        );
        let requests = reader.requests();
        assert_eq!(requests.len(), 2);
        assert_ne!(
            results[0].reader_calls[0].rendered_atom_ids,
            results[0].reader_calls[1].rendered_atom_ids
        );
        for (request, audit) in requests.iter().zip(&results[0].reader_calls) {
            let text = render(&request.messages);
            let positions: Vec<_> = audit
                .rendered_atom_ids
                .iter()
                .map(|id| {
                    let hit = eng.fetch_one(&sample.sample_id, *id).unwrap().unwrap();
                    text.find(&hit.text).unwrap()
                })
                .collect();
            assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
        }
        assert!(results[0].judge.as_ref().unwrap().correct);
    }
}

#[test]
fn run_sample_is_token_free_end_to_end() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));

    // Separate reader/judge scripts: one answer and one verdict per question.
    let reader = testing::scripted(repeat_text("an answer", s.qa.len()));
    let judge = testing::scripted(repeat_text("CORRECT", s.qa.len()));

    let results = run_sample(&eng, s, embedder, &*reader, &*judge, BenchConfig::default()).unwrap();
    for (qa_index, result) in results.iter().enumerate() {
        assert_eq!(result.sample_id, s.sample_id);
        assert_eq!(result.qa_index, qa_index);
    }
    let audit_row = serde_json::to_value(&results[0]).unwrap();
    assert_eq!(audit_row["sample_id"], s.sample_id);
    assert_eq!(audit_row["qa_index"], 0);
    assert_eq!(results.len(), s.qa.len());

    let report = aggregate(&results, prov());
    // 4 scored questions all judged correct; 1 adversarial judged abstained.
    assert_eq!(report.overall_total, 4);
    assert_eq!(report.overall_correct, 4);
    assert_eq!(report.adversarial_total, 1);
    assert!((report.adversarial_abstention - 1.0).abs() < 1e-9);
}

#[test]
fn run_sample_reuses_an_unchanged_corpus() {
    let mut sample = parse_root(&fixture()).unwrap().remove(0);
    sample.qa.truncate(1);
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    let reader = testing::constant("golden retriever");
    let judge = testing::constant("CORRECT");
    let fresh = run_sample(
        &eng,
        &sample,
        embedder.clone(),
        &*reader,
        &*judge,
        BenchConfig::default(),
    )
    .unwrap();
    let mut observed = 0;
    let reused = run_sample_observed(
        &eng,
        &sample,
        embedder,
        &*reader,
        &*judge,
        BenchConfig::default(),
        true,
        &Pacer::unbounded(),
        &mut |_| {
            observed += 1;
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(observed, 1);
    assert_eq!(reused.len(), 1);
    assert_eq!(reused[0].retrieved, fresh[0].retrieved);
    assert_eq!(reused[0].predicted, fresh[0].predicted);
    assert_eq!(
        eng.count_region(&sample.sample_id).unwrap(),
        sample.turns.len() as u64
    );
}

#[test]
fn run_sample_rejects_changed_or_missing_corpus_before_model_calls() {
    let mut sample = parse_root(&fixture()).unwrap().remove(0);
    sample.qa.truncate(1);
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    eng.create_region(&sample.sample_id, embedder.clone())
        .unwrap();
    ingest_sample(&eng, &sample.sample_id, &sample).unwrap();
    let forbidden = testing::error(|| panic!("invalid corpus reached a model call"));
    for missing in [false, true] {
        let mut changed = sample.clone();
        if missing {
            changed.sample_id = "missing-corpus".into();
        } else {
            changed.turns[0].text.push_str(" changed");
        }
        let mut observed = 0;
        let result = run_sample_observed(
            &eng,
            &changed,
            embedder.clone(),
            &*forbidden,
            &*forbidden,
            BenchConfig::default(),
            true,
            &Pacer::unbounded(),
            &mut |_| {
                observed += 1;
                Ok(())
            },
        );
        assert!(matches!(result, Err(BenchError::Dataset(_))));
        assert_eq!(observed, 0);
    }
    assert!(eng
        .stored_region_identity("missing-corpus")
        .unwrap()
        .is_none());
    assert_eq!(
        eng.count_region(&sample.sample_id).unwrap(),
        sample.turns.len() as u64
    );
}

#[test]
fn run_sample_records_gold_turn_texts_and_in_view() {
    // k=50 over the 5-turn fixture retrieves every turn, so each gold id is in
    // view.
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    let reader = testing::scripted(repeat_text("an answer", s.qa.len()));
    let judge = testing::scripted(repeat_text("CORRECT", s.qa.len()));

    let results = run_sample(&eng, s, embedder, &*reader, &*judge, BenchConfig::default()).unwrap();

    // Single-hop: one gold id (D2:1) -> its rendered turn text, present in
    // view.
    let single = &results[0];
    assert_eq!(single.category, Category::SingleHop);
    assert_eq!(single.gold_evidence, vec!["D2:1"]);
    assert_eq!(
        single.gold_turn_texts,
        vec!["[3:00 pm on 5 January, 2024] Alice: Rex is a golden retriever."]
    );
    assert_eq!(single.gold_in_view, vec![true]);

    // Temporal: two gold ids -> two parallel texts + flags.
    let temporal = &results[2];
    assert_eq!(temporal.category, Category::Temporal);
    assert_eq!(temporal.gold_turn_texts.len(), 2);
    assert_eq!(temporal.gold_in_view, vec![true, true]);

    // Open-domain has empty evidence: both stay empty (no spurious rows).
    let open = &results[3];
    assert_eq!(open.category, Category::OpenDomain);
    assert!(open.gold_turn_texts.is_empty());
    assert!(open.gold_in_view.is_empty());
}

#[test]
fn aggregate_separates_unscorable_from_accuracy() {
    let results = vec![
        res(Category::SingleHop, true),
        unscorable(Category::MultiHop),
        res(Category::Temporal, false),
    ];
    let report = aggregate(&results, prov());
    // Unscorable is excluded from the scored denominator and per-category map.
    assert_eq!(report.overall_total, 2);
    assert_eq!(report.overall_correct, 1);
    assert_eq!(report.unscorable_total, 1);
    assert!(!report.per_category.contains_key("multi_hop"));
}

#[test]
fn run_sample_marks_empty_gold_scored_question_unscorable() {
    // One well-formed scored question + one scored question with an empty
    // answer key (malformed). The empty-gold one must skip the reader+judge
    // entirely.
    let mut f = fixture();
    f[0]["qa"] = json!([
        {"question": "What breed is Rex?", "answer": "golden retriever",
         "category": 4, "evidence": []},
        {"question": "Malformed key question", "answer": "", "category": 1,
         "evidence": []}
    ]);
    let samples = parse_root(&f).unwrap();
    let s = &samples[0];
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));

    // Exactly one scripted reader+judge response: the unscorable question must
    // consume neither (else the mock drains and errors).
    let reader = testing::scripted(repeat_text("golden retriever", 1));
    let judge = testing::scripted(repeat_text("CORRECT", 1));

    let results = run_sample(&eng, s, embedder, &*reader, &*judge, BenchConfig::default()).unwrap();
    assert_eq!(results.len(), 2);
    let report = aggregate(&results, prov());
    assert_eq!(
        report.overall_total, 1,
        "only the well-formed scored question counts"
    );
    assert_eq!(report.unscorable_total, 1);
}

fn repeat_text(text: &str, n: usize) -> Vec<CompletionResponse> {
    (0..n).map(|_| CompletionResponse::text(text)).collect()
}

/// `run_sample_observed` must fire the callback exactly once per question from
/// inside the parallel region, and a callback error must abort the run (not be
/// swallowed).
#[test]
fn observer_fires_once_per_question_and_error_aborts() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];
    let config = BenchConfig::default();
    let reader = testing::constant("golden retriever");
    let judge = testing::constant("CORRECT");

    // Happy path under concurrency: one callback per question, run completes.
    std::env::set_var("CITADEL_LOCOMO_CONCURRENCY", "8");
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    let seen = std::sync::atomic::AtomicUsize::new(0);
    let out = run_sample_observed(
        &eng,
        s,
        embedder,
        &*reader,
        &*judge,
        config,
        false,
        &Pacer::unbounded(),
        &mut |_| {
            seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        },
    )
    .unwrap();
    std::env::remove_var("CITADEL_LOCOMO_CONCURRENCY");
    assert_eq!(out.len(), s.qa.len(), "a result per question");
    assert_eq!(
        seen.load(std::sync::atomic::Ordering::Relaxed),
        s.qa.len(),
        "callback fires exactly once per question",
    );

    // Error path: a callback error aborts the run instead of being swallowed.
    let (_dir2, eng2) = open_engine();
    let embedder2: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    let aborted = run_sample_observed(
        &eng2,
        s,
        embedder2,
        &*reader,
        &*judge,
        config,
        false,
        &Pacer::unbounded(),
        &mut |_| Err(BenchError::Dataset("observer boom".into())),
    );
    assert!(aborted.is_err(), "observer error aborts the run");
}

/// A sustained 429 storm must be ridden out, not fatal: `paced_complete`
/// retries through 40 rate-limit errors and still returns a scored result.
#[test]
fn paced_complete_rides_out_a_429_storm() {
    // Tiny backoff so 40 retries finish fast; config is read fresh per call so
    // these overrides apply. MAX_ELAPSED is a hard ceiling against a hang.
    std::env::set_var("CITADEL_MEMBENCH_RETRY_BASE_MS", "1");
    std::env::set_var("CITADEL_MEMBENCH_RETRY_CAP_MS", "2");
    std::env::set_var("CITADEL_MEMBENCH_RETRY_MAX_ELAPSED_SECS", "30");
    std::env::set_var("CITADEL_MEMBENCH_RETRY_MAX_ATTEMPTS", "100");

    // 40 consecutive 429s then success; the body carries a "try again in"
    // phrase so the Retry-After body-parse path is exercised.
    let storm = testing::http_storm(
        40,
        429,
        "Rate limit reached. Please try again in 1ms.",
        CompletionResponse::text("CORRECT"),
    );
    let client = storm.client();
    let pacer = citadel_membench::Pacer::unbounded();
    let res = judge_correct(&*client, &pacer, "q", "gold", "pred");

    std::env::remove_var("CITADEL_MEMBENCH_RETRY_BASE_MS");
    std::env::remove_var("CITADEL_MEMBENCH_RETRY_CAP_MS");
    std::env::remove_var("CITADEL_MEMBENCH_RETRY_MAX_ELAPSED_SECS");
    std::env::remove_var("CITADEL_MEMBENCH_RETRY_MAX_ATTEMPTS");

    let (correct, _) = res.expect("a 40-deep 429 storm must be ridden out, not fatal");
    assert!(correct, "the eventual CORRECT response is returned");
}

/// A terminal (non-retryable) error fails fast - we do NOT retry 4xx/Backend.
#[test]
fn paced_complete_fails_fast_on_terminal_error() {
    let dead = testing::error(|| LlmError::Backend("malformed".into()));
    let pacer = citadel_membench::Pacer::unbounded();
    assert!(
        judge_correct(&*dead, &pacer, "q", "gold", "pred").is_err(),
        "a terminal Backend error must not be retried"
    );
}

/// Serial vs concurrent must produce a byte-identical result vector, proving
/// concurrency is a latency optimization, never a score change.
#[test]
fn concurrent_questions_match_serial_byte_for_byte() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];

    let run = |concurrency: &str| -> Vec<QuestionResult> {
        std::env::set_var("CITADEL_LOCOMO_CONCURRENCY", concurrency);
        let (_dir, eng) = open_engine();
        let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
        let reader = testing::constant("golden retriever");
        let judge = testing::constant("CORRECT");
        run_sample(&eng, s, embedder, &*reader, &*judge, BenchConfig::default()).unwrap()
    };

    let serial = run("1");
    let concurrent = run("8");
    std::env::remove_var("CITADEL_LOCOMO_CONCURRENCY");

    assert_eq!(serial.len(), concurrent.len(), "same question count");
    for (a, b) in serial.iter().zip(&concurrent) {
        // Field-by-field; recall_micros excluded (latency varies, not a score).
        assert_eq!(a.question, b.question, "question order preserved");
        assert_eq!(a.category, b.category);
        assert_eq!(a.scorable, b.scorable);
        assert_eq!(a.correct, b.correct, "verdict identical for {}", a.question);
        assert_eq!(a.predicted, b.predicted);
        assert_eq!(a.gold, b.gold);
        assert_eq!(
            a.retrieved, b.retrieved,
            "retrieval identical for {}",
            a.question
        );
        assert_eq!(a.gold_evidence, b.gold_evidence);
    }

    // The aggregate score must be identical too.
    let ra = aggregate(&serial, prov());
    let rb = aggregate(&concurrent, prov());
    assert_eq!(ra.overall_correct, rb.overall_correct);
    assert_eq!(ra.overall_total, rb.overall_total);
    assert_eq!(ra.adversarial_total, rb.adversarial_total);
    assert!((ra.overall_accuracy - rb.overall_accuracy).abs() < 1e-12);
}

fn res(category: Category, correct: bool) -> QuestionResult {
    QuestionResult {
        sample_id: "fixture".into(),
        qa_index: 0,
        category,
        scorable: true,
        correct,
        recall_micros: 10,
        input_tokens: 5,
        output_tokens: 3,
        cost_usd: Some(0.0),
        retrieved: Vec::new(),
        retrieved_atom_ids: Vec::new(),
        gold_evidence: Vec::new(),
        gold_turn_texts: Vec::new(),
        gold_in_view: Vec::new(),
        question: String::new(),
        gold: String::new(),
        predicted: String::new(),
        reader_finish_reasons: Vec::new(),
        reader_calls: Vec::new(),
        judge: None,
    }
}

/// An unscorable result: a scored question with an empty gold key.
fn unscorable(category: Category) -> QuestionResult {
    QuestionResult {
        sample_id: "fixture".into(),
        qa_index: 0,
        category,
        scorable: false,
        correct: false,
        recall_micros: 0,
        input_tokens: 0,
        output_tokens: 0,
        cost_usd: Some(0.0),
        retrieved: Vec::new(),
        retrieved_atom_ids: Vec::new(),
        gold_evidence: Vec::new(),
        gold_turn_texts: Vec::new(),
        gold_in_view: Vec::new(),
        question: String::new(),
        gold: String::new(),
        predicted: String::new(),
        reader_finish_reasons: Vec::new(),
        reader_calls: Vec::new(),
        judge: None,
    }
}

fn prov() -> citadel_membench::Provenance {
    provenance(
        "mock",
        "mock",
        "mock-fnv1a-bow-v1",
        BenchConfig::default(),
        "inline fixture",
        "0000000000000000000000000000000000000000000000000000000000000000",
    )
}

fn render(messages: &[Message]) -> String {
    messages
        .iter()
        .map(|m| match m {
            Message::System(s) | Message::User(s) => s.clone(),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn aggregate_sums_per_question_cost() {
    let mut results = vec![
        res(Category::SingleHop, true),
        res(Category::MultiHop, false),
        res(Category::Adversarial, true),
    ];
    results[0].cost_usd = Some(0.10);
    results[1].cost_usd = Some(0.25);
    results[2].cost_usd = Some(0.05);
    let report = aggregate(&results, prov());
    // Cost is the sum of per-question cost, independent of the token counts in
    // `res`.
    assert!((report.estimated_cost_usd.unwrap() - 0.40).abs() < 1e-9);
    results[1].cost_usd = None;
    assert_eq!(aggregate(&results, prov()).estimated_cost_usd, None);
}

#[test]
fn provenance_records_the_reader_models_rate_not_a_hardcoded_one() {
    let sha = "0".repeat(64);
    let mini = provenance(
        "gpt-4o-mini",
        "gpt-4o-mini",
        "e5-large",
        BenchConfig::default(),
        "n",
        sha.clone(),
    );
    assert!(!mini.agentic, "the default benchmark is single-reader-call");
    assert!((mini.cost_rate_input_usd_per_m.unwrap() - 0.15).abs() < 1e-9);
    assert!((mini.cost_rate_output_usd_per_m.unwrap() - 0.60).abs() < 1e-9);
    // A gpt-4o reader records gpt-4o's rate, proving it derives from the model.
    let big = provenance(
        "gpt-4o",
        "gpt-4o-mini",
        "e5-large",
        BenchConfig::default(),
        "n",
        sha,
    );
    assert!((big.cost_rate_input_usd_per_m.unwrap() - 2.50).abs() < 1e-9);
    assert!((big.cost_rate_output_usd_per_m.unwrap() - 10.00).abs() < 1e-9);
}
