//! Token-free harness tests: no network, no real model files. Everything runs
//! against an inline LoCoMo-shaped fixture, a `MockEmbedder`, and a `MockClient`.

use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_ai::{CompletionRequest, CompletionResponse, LLMClient, LlmError, Message, MockClient};
use citadel_mem::{Embedder, MemoryEngine, MockEmbedder};
use citadel_membench::{
    aggregate, build_reader_prompt, ingest_sample, judge_correct, parse_root, provenance,
    run_sample, run_sample_observed, BenchConfig, BenchError, Category, Pacer, QuestionResult,
};
use serde_json::{json, Value};

const DIM: usize = 64;

/// A 3-session conversation with QA in every category (incl. adversarial), one
/// non-string answer (scalar rendering), and sibling `_date_time`/`_summary` keys
/// the loader must not treat as sessions.
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
            "session_1_date_time": "2pm on 1 Jan 2024",
            "session_1_summary": "Alice got a dog.",
            "session_2": [
                {"speaker": "Alice", "dia_id": "D2:1", "text": "Rex is a golden retriever."},
                {"speaker": "Alice", "dia_id": "D2:2", "text": "I paid 1200 dollars for him."}
            ],
            "session_2_date_time": "3pm on 5 Jan 2024",
            "session_2_observation": "ignore me",
            "session_10": [
                {"speaker": "Bob", "dia_id": "D10:1", "text": "We hiked Mount Tam last weekend."}
            ],
            "session_10_date_time": "noon on 20 Mar 2024"
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

    // 2 + 2 + 1 turns; _summary/_observation/_date_time siblings are not sessions.
    assert_eq!(s.turns.len(), 5);
    // Sorted by session number, so session_10 sorts after session_2 numerically.
    assert_eq!(s.turns.last().unwrap().session, 10);
    assert_eq!(
        s.turns.last().unwrap().text,
        "We hiked Mount Tam last weekend."
    );
    // date_time is paired from the matching `session_<n>_date_time`.
    assert_eq!(s.turns[0].date_time, "2pm on 1 Jan 2024");
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
fn category_guard_rejects_out_of_range() {
    let mut bad = fixture();
    bad[0]["qa"][0]["category"] = json!(7);
    assert!(parse_root(&bad).is_err(), "category 7 must be rejected");

    // Truncation trap: 261u64 as u8 == 5; must be rejected, not read as Adversarial.
    let mut wrap = fixture();
    wrap[0]["qa"][0]["category"] = json!(261);
    assert!(
        parse_root(&wrap).is_err(),
        "category 261 must be rejected, not truncated to 5 (Adversarial)"
    );
}

#[test]
fn category_mapping_matches_locomo_data() {
    // Guards the 2=temporal / 3=open-domain / 4=single-hop mapping against re-swapping.
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
fn reader_prompt_contains_only_passed_hits_not_gold_or_evidence() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    eng.create_region(&s.sample_id, embedder).unwrap();
    ingest_sample(&eng, &s.sample_id, s).unwrap();

    // Retrieve a single hit, then build the prompt from ONLY that hit.
    let hits = eng
        .recall(
            &s.sample_id,
            citadel_mem::RecallQuery::by_text("What breed is Rex?", 1),
        )
        .unwrap();
    assert_eq!(hits.len(), 1, "k=1 yields exactly one hit");
    let retrieved_text = hits[0].text.clone();

    let prompt = build_reader_prompt(&hits, "What breed is Rex?");
    let blob = render(&prompt);

    // The single retrieved turn and the question are present.
    assert!(blob.contains(&retrieved_text));
    assert!(blob.contains("What breed is Rex?"));

    // No non-retrieved turn leaks in. Identify the retrieved turn by dia_id, since its
    // raw text is a substring of the speaker-prefixed hit.
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
fn judge_parses_correct_wrong_including_the_not_correct_trap() {
    let pacer = citadel_membench::Pacer::unbounded();
    let (ok, _) = judge_correct(
        &MockClient::replying("CORRECT"),
        &pacer,
        "q",
        "gold",
        "pred",
    )
    .unwrap();
    assert!(ok);

    let (bad, _) =
        judge_correct(&MockClient::replying("WRONG"), &pacer, "q", "gold", "pred").unwrap();
    assert!(!bad);

    // The trap: a reply that CONTAINS "correct" but is a rejection must be WRONG.
    let (trap, _) = judge_correct(
        &MockClient::replying("This is not correct, it is WRONG"),
        &pacer,
        "q",
        "gold",
        "pred",
    )
    .unwrap();
    assert!(!trap, "must parse by prefix, not contains(\"correct\")");
}

#[test]
fn run_sample_is_token_free_end_to_end() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));

    // Separate reader/judge scripts: one answer and one verdict per question.
    let reader = MockClient::scripted(repeat_text("an answer", s.qa.len()));
    let judge = MockClient::scripted(repeat_text("CORRECT", s.qa.len()));

    let results = run_sample(
        &eng,
        s,
        embedder,
        &reader,
        &judge,
        BenchConfig::default().top_k,
    )
    .unwrap();
    assert_eq!(results.len(), s.qa.len());

    let report = aggregate(&results, prov());
    // 4 scored questions all judged CORRECT; 1 adversarial judged abstained.
    assert_eq!(report.overall_total, 4);
    assert_eq!(report.overall_correct, 4);
    assert_eq!(report.adversarial_total, 1);
    assert!((report.adversarial_abstention - 1.0).abs() < 1e-9);
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
    // One well-formed scored question + one scored question with an empty answer
    // key (malformed). The empty-gold one must skip the reader+judge entirely.
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

    // Exactly ONE scripted reader+judge response: the unscorable question must
    // consume neither (else the mock drains and errors).
    let reader = MockClient::scripted(repeat_text("golden retriever", 1));
    let judge = MockClient::scripted(repeat_text("CORRECT", 1));

    let results = run_sample(
        &eng,
        s,
        embedder,
        &reader,
        &judge,
        BenchConfig::default().top_k,
    )
    .unwrap();
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

/// `run_sample_observed` must fire the callback exactly once per question from inside
/// the parallel region, and a callback error must abort the run (not be swallowed).
#[test]
fn observer_fires_once_per_question_and_error_aborts() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];
    let top_k = BenchConfig::default().top_k;
    let reader = ConstClient("golden retriever");
    let judge = ConstClient("CORRECT");

    // Happy path under concurrency: one callback per question, run completes.
    std::env::set_var("LOCOMO_CONCURRENCY", "8");
    let (_dir, eng) = open_engine();
    let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
    let seen = std::sync::atomic::AtomicUsize::new(0);
    let out = run_sample_observed(
        &eng,
        s,
        embedder,
        &reader,
        &judge,
        top_k,
        &Pacer::unbounded(),
        &mut |_| {
            seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        },
    )
    .unwrap();
    std::env::remove_var("LOCOMO_CONCURRENCY");
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
        &reader,
        &judge,
        top_k,
        &Pacer::unbounded(),
        &mut |_| Err(BenchError::Dataset("observer boom".into())),
    );
    assert!(aborted.is_err(), "observer error aborts the run");
}

struct ConstClient(&'static str);
impl LLMClient for ConstClient {
    fn complete(
        &self,
        _req: &CompletionRequest,
    ) -> std::result::Result<CompletionResponse, LlmError> {
        Ok(CompletionResponse::text(self.0))
    }
    fn model_id(&self) -> &str {
        "const"
    }
    fn count_tokens(&self, messages: &[Message]) -> usize {
        messages.len()
    }
}

/// Returns a 429 for its first `storm` calls then succeeds; the 429 carries a
/// "try again in" body so the Retry-After body-parse is exercised.
struct StormClient {
    remaining: std::sync::atomic::AtomicU32,
}
impl StormClient {
    fn new(storm: u32) -> Self {
        Self {
            remaining: std::sync::atomic::AtomicU32::new(storm),
        }
    }
}
impl LLMClient for StormClient {
    fn complete(
        &self,
        _req: &CompletionRequest,
    ) -> std::result::Result<CompletionResponse, LlmError> {
        if self
            .remaining
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            > 0
        {
            Err(LlmError::Http {
                status: 429,
                retry_after: None,
                message: "Rate limit reached. Please try again in 1ms.".into(),
            })
        } else {
            Ok(CompletionResponse::text("CORRECT"))
        }
    }
    fn model_id(&self) -> &str {
        "storm"
    }
    fn count_tokens(&self, _m: &[Message]) -> usize {
        10
    }
}

/// A sustained 429 storm must be ridden out, not fatal: `paced_complete` retries
/// through 40 rate-limit errors and still returns a scored result.
#[test]
fn paced_complete_rides_out_a_429_storm() {
    // Tiny backoff so 40 retries finish fast; config is read fresh per call so these
    // overrides apply. MAX_ELAPSED is a hard ceiling against a hang.
    std::env::set_var("LOCOMO_RETRY_BASE_MS", "1");
    std::env::set_var("LOCOMO_RETRY_CAP_MS", "2");
    std::env::set_var("LOCOMO_RETRY_MAX_ELAPSED_SECS", "30");
    std::env::set_var("LOCOMO_RETRY_MAX_ATTEMPTS", "100");

    let storm = StormClient::new(40); // 40 consecutive 429s, then success
    let pacer = citadel_membench::Pacer::unbounded();
    let res = judge_correct(&storm, &pacer, "q", "gold", "pred");

    std::env::remove_var("LOCOMO_RETRY_BASE_MS");
    std::env::remove_var("LOCOMO_RETRY_CAP_MS");
    std::env::remove_var("LOCOMO_RETRY_MAX_ELAPSED_SECS");
    std::env::remove_var("LOCOMO_RETRY_MAX_ATTEMPTS");

    let (correct, _) = res.expect("a 40-deep 429 storm must be ridden out, not fatal");
    assert!(correct, "the eventual CORRECT response is returned");
}

/// A terminal (non-retryable) error fails fast - we do NOT retry 4xx/Backend.
#[test]
fn paced_complete_fails_fast_on_terminal_error() {
    struct DeadClient;
    impl LLMClient for DeadClient {
        fn complete(
            &self,
            _req: &CompletionRequest,
        ) -> std::result::Result<CompletionResponse, LlmError> {
            Err(LlmError::Backend("malformed".into()))
        }
        fn model_id(&self) -> &str {
            "dead"
        }
        fn count_tokens(&self, _m: &[Message]) -> usize {
            1
        }
    }
    let pacer = citadel_membench::Pacer::unbounded();
    assert!(
        judge_correct(&DeadClient, &pacer, "q", "gold", "pred").is_err(),
        "a terminal Backend error must not be retried"
    );
}

/// Serial (LOCOMO_CONCURRENCY=1) vs concurrent (=8) must produce a byte-identical
/// result vector - proving concurrency is a latency optimization, never a score change.
#[test]
fn concurrent_questions_match_serial_byte_for_byte() {
    let samples = parse_root(&fixture()).unwrap();
    let s = &samples[0];

    let run = |concurrency: &str| -> Vec<QuestionResult> {
        std::env::set_var("LOCOMO_CONCURRENCY", concurrency);
        let (_dir, eng) = open_engine();
        let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(DIM));
        let reader = ConstClient("golden retriever");
        let judge = ConstClient("CORRECT");
        run_sample(
            &eng,
            s,
            embedder,
            &reader,
            &judge,
            BenchConfig::default().top_k,
        )
        .unwrap()
    };

    let serial = run("1");
    let concurrent = run("8");
    std::env::remove_var("LOCOMO_CONCURRENCY");

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
        category,
        scorable: true,
        correct,
        recall_micros: 10,
        input_tokens: 5,
        output_tokens: 3,
        cost_usd: 0.0,
        retrieved: Vec::new(),
        gold_evidence: Vec::new(),
        question: String::new(),
        gold: String::new(),
        predicted: String::new(),
    }
}

/// An unscorable result: a scored question with an empty gold key.
fn unscorable(category: Category) -> QuestionResult {
    QuestionResult {
        category,
        scorable: false,
        correct: false,
        recall_micros: 0,
        input_tokens: 0,
        output_tokens: 0,
        cost_usd: 0.0,
        retrieved: Vec::new(),
        gold_evidence: Vec::new(),
        question: String::new(),
        gold: String::new(),
        predicted: String::new(),
    }
}

fn prov() -> citadel_membench::Provenance {
    provenance(
        "mock",
        "mock",
        "mock",
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
    results[0].cost_usd = 0.10;
    results[1].cost_usd = 0.25;
    results[2].cost_usd = 0.05;
    let report = aggregate(&results, prov());
    // Cost is the sum of per-question cost, independent of the token counts in `res`.
    assert!((report.estimated_cost_usd - 0.40).abs() < 1e-9);
}

#[test]
fn provenance_records_the_reader_models_rate_not_a_hardcoded_one() {
    let sha = "0".repeat(64);
    let mini = provenance(
        "gpt-4o-mini",
        "gpt-4o-mini",
        "bge",
        BenchConfig::default(),
        "n",
        sha.clone(),
    );
    assert!((mini.cost_rate_input_usd_per_m - 0.15).abs() < 1e-9);
    assert!((mini.cost_rate_output_usd_per_m - 0.60).abs() < 1e-9);
    // A gpt-4o reader records gpt-4o's rate, proving the rate is derived from the model.
    let big = provenance(
        "gpt-4o",
        "gpt-4o-mini",
        "bge",
        BenchConfig::default(),
        "n",
        sha,
    );
    assert!((big.cost_rate_input_usd_per_m - 2.50).abs() < 1e-9);
    assert!((big.cost_rate_output_usd_per_m - 10.00).abs() < 1e-9);
}
