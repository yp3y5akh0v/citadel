//! LoCoMo long-term-conversational-memory benchmark for citadel-mem.
//!
//! Headline accuracy excludes the adversarial category (no answerable gold) and
//! reports it separately as abstention. The reader sees only the top-k retrieved
//! memories, never the transcript/gold/evidence (see [`eval::build_reader_prompt`]).

pub mod dataset;
pub mod error;
pub mod eval;
pub mod ingest;
pub mod ratelimit;

use std::sync::Arc;

use rustc_hash::FxHashMap;
use serde::Serialize;

use citadel_ai::LLMClient;
use citadel_mem::{Embedder, FusionWeights, MemoryEngine};

pub use dataset::{load, load_with_hash, parse_root, sha256_hex, Category, QaSample, Sample, Turn};
pub use error::{BenchError, Result};
pub use eval::{
    answer_question, build_reader_prompt, judge_abstained, judge_correct, AnswerOutcome,
};
pub use ingest::ingest_sample;
pub use ratelimit::{Gate, Pacer};

/// Published OpenAI USD per 1M tokens as `(input, output)`, keyed by model id. OpenAI
/// returns no `cost_usd`, so the bench estimates it from the recorded token counts at
/// these rates; the real bill is lower when prompt-prefix caching applies. Reader and
/// judge are each costed at their own model's rate. Unknown models fall back to
/// gpt-4o-mini rates.
fn model_rate(model: &str) -> (f64, f64) {
    if model.starts_with("gpt-4o-mini") {
        (0.15, 0.60)
    } else if model.starts_with("gpt-4o") {
        (2.50, 10.00)
    } else {
        (0.15, 0.60)
    }
}

/// Estimated USD for one model's token usage at its published rate.
fn token_cost(model: &str, input_tokens: u32, output_tokens: u32) -> f64 {
    let (rate_in, rate_out) = model_rate(model);
    (f64::from(input_tokens) / 1_000_000.0) * rate_in
        + (f64::from(output_tokens) / 1_000_000.0) * rate_out
}

/// LoCoMo's documented weaknesses, surfaced in every report.
const KNOWN_FLAWS: &str = "De facto LLM-judge protocol, not the paper's token-F1, \
     so comparable only to runs using the same judge model. Reader and judge are \
     separate, independently-selected models (reader and judge gpt-4o-mini, the \
     reference setup); both are pinned in Provenance. The reader uses \
     ONE category-blind answer prompt (the answerer never receives the gold \
     question category), matching the Mem0/Zep single-prompt protocol. A 40-case \
     adversarial probe of the judge measured 0% false-accept (judge-probe.ps1), so \
     judge lenience appears low; LoCoMo answer keys nonetheless have ~6.4% errors \
     (an independent audit found ~99 wrong gold answers), so the honest accuracy \
     ceiling is ~93.6%, not 100%. Cost is computed from the recorded reader+judge \
     tokens at each model's published rate (an upper bound: prompt caching lowers \
     the real bill). Ingestion is raw conversation turns plus \
     each shared photo's BLIP caption (LoCoMo substitutes the image with its \
     caption), not LLM-extracted facts, so head-to-head vendor comparison is \
     apples-to-oranges. Under this raw-turn ingest the recency and importance \
     fusion weights are inert (all turns share one ingest timestamp and carry no \
     importance), so ranking is effectively semantic+keyword only; the recorded \
     weights describe the engine default, not an active 4-signal blend. Headline \
     excludes the adversarial category; adversarial is a separate abstention metric.";

/// Knobs for a benchmark run.
#[derive(Debug, Clone, Copy)]
pub struct BenchConfig {
    /// Number of memories retrieved per question and shown to the reader.
    pub top_k: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        // 30, not 50: a wider window buys ~1-2 pts but invites the "top_k >= pool =
        // retrieve-everything" criticism. 30 is the common, defensible value.
        Self { top_k: 30 }
    }
}

/// The per-question outcome, before aggregation.
#[derive(Debug, Clone, Serialize)]
pub struct QuestionResult {
    pub category: Category,
    /// Unscorable (empty gold key) -> excluded from accuracy. Always true for adversarial.
    pub scorable: bool,
    /// For scored categories: judged correct. For adversarial: judged abstained.
    pub correct: bool,
    pub recall_micros: u128,
    pub input_tokens: u32,
    pub output_tokens: u32,
    /// Estimated USD for this question: reader + judge tokens, each at its model's rate.
    pub cost_usd: f64,
    /// `dia_id`s retrieved into the reader's top-k; vs `gold_evidence` this splits a
    /// miss into reader-failure (gold retrieved) vs retrieval-gap (gold absent).
    pub retrieved: Vec<String>,
    /// Gold evidence `dia_id`s (from the dataset); joined against `retrieved`.
    pub gold_evidence: Vec<String>,
    /// Audit trail: question, gold, and the reader's predicted answer.
    pub question: String,
    pub gold: String,
    pub predicted: String,
}

/// Per-category roll-up (scored categories only).
#[derive(Debug, Clone, Serialize)]
pub struct CategoryStats {
    pub total: usize,
    pub correct: usize,
    pub accuracy: f64,
}

/// How the run was configured, carried into the report for traceability.
#[derive(Debug, Clone, Serialize)]
pub struct Provenance {
    pub reader_model: String,
    pub judge_model: String,
    pub embedder_model: String,
    /// Cross-encoder reranker model id ("none" if recall used fusion only).
    pub reranker_model: String,
    pub top_k: usize,
    pub temperature: f32,
    /// Retrieval fusion weights (citadel-mem defaults); recorded for reproducibility.
    pub fusion_semantic: f32,
    pub fusion_keyword: f32,
    pub fusion_recency: f32,
    pub fusion_importance: f32,
    pub dataset_note: String,
    /// SHA-256 of the scored dataset file: pins the exact input.
    pub dataset_sha256: String,
    /// The reader model's published per-1M rates; the bench costs reader and judge
    /// each at its own model's rate (estimated, not billed).
    pub cost_rate_input_usd_per_m: f64,
    pub cost_rate_output_usd_per_m: f64,
    pub known_flaws: String,
}

/// The full, serializable benchmark report.
#[derive(Debug, Clone, Serialize)]
pub struct BenchReport {
    pub provenance: Provenance,
    pub per_category: FxHashMap<String, CategoryStats>,
    /// Headline accuracy over the four scored categories (adversarial excluded).
    pub overall_accuracy: f64,
    pub overall_total: usize,
    pub overall_correct: usize,
    /// Secondary metric: fraction of adversarial questions the reader abstained on.
    pub adversarial_abstention: f64,
    pub adversarial_total: usize,
    /// Scored questions skipped for an empty/malformed gold key (not in accuracy).
    pub unscorable_total: usize,
    pub recall_p95_micros: u128,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub estimated_cost_usd: f64,
}

/// Whether to use encrypted regions (per-atom sealed + crypto erasure). Env vars are
/// strings, so `LOCOMO_ENCRYPTED` is parsed as a bool ("true"/"false", case-insensitive);
/// unset = false.
pub fn encrypted_regions() -> bool {
    std::env::var("LOCOMO_ENCRYPTED")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Create the per-conversation region: encrypted (per-atom sealed + crypto erasure) when
/// [`encrypted_regions`] is on, else the plaintext path used by the historical baselines.
pub fn create_bench_region(
    eng: &MemoryEngine,
    name: &str,
    embedder: Arc<dyn Embedder>,
) -> Result<()> {
    if encrypted_regions() {
        eng.create_encrypted_region(name, embedder)?;
    } else {
        eng.create_region(name, embedder)?;
    }
    Ok(())
}

/// Run one conversation end-to-end: ingest into a fresh region, then retrieve, read,
/// and judge each question. Returns one result per question.
pub fn run_sample(
    eng: &MemoryEngine,
    sample: &Sample,
    embedder: Arc<dyn Embedder>,
    reader: &dyn LLMClient,
    judge: &dyn LLMClient,
    top_k: usize,
) -> Result<Vec<QuestionResult>> {
    run_sample_observed(
        eng,
        sample,
        embedder,
        reader,
        judge,
        top_k,
        &Pacer::unbounded(),
        &mut |_| Ok(()),
    )
}

/// Like [`run_sample`] but invokes `on_result` per question as it scores (for live
/// tracing). Scoring is identical; an error from the callback aborts the run.
#[allow(clippy::too_many_arguments)]
pub fn run_sample_observed(
    eng: &MemoryEngine,
    sample: &Sample,
    embedder: Arc<dyn Embedder>,
    reader: &dyn LLMClient,
    judge: &dyn LLMClient,
    top_k: usize,
    pacer: &Pacer,
    on_result: &mut (dyn FnMut(&QuestionResult) -> Result<()> + Send),
) -> Result<Vec<QuestionResult>> {
    // One region per conversation (no cross-conversation retrieval). Ingest is the
    // single-writer phase and must finish first; questions then fan out concurrently.
    create_bench_region(eng, &sample.sample_id, embedder)?;
    ingest_sample(eng, &sample.sample_id, sample)?;

    // Reader and judge keep independent in-flight caps (Gates); `pacer` enforces
    // per-model TPM. Questions run on a fixed pool of OS threads, NOT rayon: each task
    // blocks (HTTP, gate waits) and recall() uses rayon internally, so a rayon pool
    // would nest and deadlock once workers park. LOCOMO_CONCURRENCY=1 = serial.
    let legacy = std::env::var("LOCOMO_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    let (reader_n, judge_n) = match legacy {
        Some(1) => (1, 1),
        _ => (
            env_usize("LOCOMO_READER_CONCURRENCY", 3),
            env_usize("LOCOMO_JUDGE_CONCURRENCY", 12),
        ),
    };
    let workers = legacy.unwrap_or(8).max(reader_n).max(judge_n);
    let reader_gate = Gate::new(reader_n);
    let judge_gate = Gate::new(judge_n);

    // The callback fires per question in completion order (mutex-serialized) for live
    // tracing; results are returned in QUESTION order (by index), so the report stays
    // byte-identical to a serial run. A worker error aborts the run.
    let total = sample.qa.len();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let failed = std::sync::atomic::AtomicBool::new(false);
    let observed = std::sync::Mutex::new(on_result);
    let err_slot: std::sync::Mutex<Option<BenchError>> = std::sync::Mutex::new(None);
    let (tx, rx) = std::sync::mpsc::channel::<(usize, QuestionResult)>();
    let (rg, jg) = (&reader_gate, &judge_gate);
    let (next_r, failed_r, observed_r, err_r) = (&next, &failed, &observed, &err_slot);

    std::thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            scope.spawn(move || {
                use std::sync::atomic::Ordering::Relaxed;
                loop {
                    if failed_r.load(Relaxed) {
                        break;
                    }
                    let i = next_r.fetch_add(1, Relaxed);
                    if i >= total {
                        break;
                    }
                    match process_one_question(
                        eng,
                        &sample.sample_id,
                        reader,
                        judge,
                        top_k,
                        &sample.qa[i],
                        pacer,
                        rg,
                        jg,
                    ) {
                        Ok(r) => {
                            // Run the observer under its lock, then send: never hold two locks at once.
                            let observe = (*observed_r.lock().expect("observer poisoned"))(&r);
                            match observe {
                                Ok(()) => {
                                    let _ = tx.send((i, r));
                                }
                                Err(e) => {
                                    *err_r.lock().expect("err slot poisoned") = Some(e);
                                    failed_r.store(true, Relaxed);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            *err_r.lock().expect("err slot poisoned") = Some(e);
                            failed_r.store(true, Relaxed);
                            break;
                        }
                    }
                }
            });
        }
        drop(tx); // drop the original sender so `rx` closes once all workers finish
    });

    if let Some(e) = err_slot.into_inner().expect("err slot poisoned") {
        return Err(e);
    }
    let mut slots: Vec<Option<QuestionResult>> = (0..total).map(|_| None).collect();
    for (i, r) in rx {
        slots[i] = Some(r);
    }
    let out: Vec<QuestionResult> = slots
        .into_iter()
        .map(|o| o.expect("every question produced a result"))
        .collect();
    Ok(out)
}

/// Read an environment variable as a `usize >= 1`, else `default`.
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(default)
}

/// Score one question: recall -> read -> judge. Self-contained and order-independent
/// (runs concurrently); the reader and judge calls each hold their own permit.
#[allow(clippy::too_many_arguments)]
fn process_one_question(
    eng: &MemoryEngine,
    region: &str,
    reader: &dyn LLMClient,
    judge: &dyn LLMClient,
    top_k: usize,
    qa: &QaSample,
    pacer: &Pacer,
    reader_gate: &Gate,
    judge_gate: &Gate,
) -> Result<QuestionResult> {
    // Empty gold on a scored question = malformed key: record unscorable (no LLM call)
    // rather than grading it wrong. Returns before acquiring any gate/pacer.
    if qa.category.is_scored() && qa.gold.trim().is_empty() {
        return Ok(QuestionResult {
            category: qa.category,
            scorable: false,
            correct: false,
            recall_micros: 0,
            input_tokens: 0,
            output_tokens: 0,
            cost_usd: 0.0,
            retrieved: Vec::new(),
            gold_evidence: qa.evidence.clone(),
            question: qa.question.clone(),
            gold: qa.gold.clone(),
            predicted: String::new(),
        });
    }

    let outcome = {
        let _permit = reader_gate.acquire();
        answer_question(reader, pacer, eng, region, &qa.question, top_k)?
    };

    // Scored -> correctness judge. Adversarial -> abstention judge, except
    // false-premise adversarials with a real gold (graded for correctness).
    let (correct, judge_usage) = {
        let _permit = judge_gate.acquire();
        if qa.category.is_scored() {
            judge_correct(judge, pacer, &qa.question, &qa.gold, &outcome.answer)?
        } else if qa.gold.trim().is_empty() {
            judge_abstained(judge, pacer, &qa.question, &outcome.answer)?
        } else {
            judge_correct(judge, pacer, &qa.question, &qa.gold, &outcome.answer)?
        }
    };

    Ok(QuestionResult {
        category: qa.category,
        scorable: true,
        correct,
        recall_micros: outcome.recall_micros,
        input_tokens: outcome
            .usage
            .input_tokens
            .saturating_add(judge_usage.input_tokens),
        output_tokens: outcome
            .usage
            .output_tokens
            .saturating_add(judge_usage.output_tokens),
        cost_usd: token_cost(
            reader.model_id(),
            outcome.usage.input_tokens,
            outcome.usage.output_tokens,
        ) + token_cost(
            judge.model_id(),
            judge_usage.input_tokens,
            judge_usage.output_tokens,
        ),
        retrieved: outcome.retrieved,
        gold_evidence: qa.evidence.clone(),
        question: qa.question.clone(),
        gold: qa.gold.clone(),
        predicted: outcome.answer,
    })
}

/// Roll per-question results into a [`BenchReport`] (overall = scored categories only).
pub fn aggregate(results: &[QuestionResult], provenance: Provenance) -> BenchReport {
    let mut per_category: FxHashMap<String, CategoryStats> = FxHashMap::default();
    let mut overall_total = 0usize;
    let mut overall_correct = 0usize;
    let mut adversarial_total = 0usize;
    let mut adversarial_abstained = 0usize;
    let mut unscorable_total = 0usize;

    let mut total_input_tokens = 0u64;
    let mut total_output_tokens = 0u64;
    let mut total_cost_usd = 0.0f64;
    let mut latencies = Vec::with_capacity(results.len());

    for r in results {
        total_input_tokens += u64::from(r.input_tokens);
        total_output_tokens += u64::from(r.output_tokens);
        total_cost_usd += r.cost_usd;
        // Unscorable questions skip recall (latency 0); excluding keeps p95 honest.
        if r.scorable {
            latencies.push(r.recall_micros);
        }

        if r.category.is_scored() {
            if !r.scorable {
                unscorable_total += 1;
                continue;
            }
            let stats = per_category
                .entry(r.category.label().to_string())
                .or_insert(CategoryStats {
                    total: 0,
                    correct: 0,
                    accuracy: 0.0,
                });
            stats.total += 1;
            overall_total += 1;
            if r.correct {
                stats.correct += 1;
                overall_correct += 1;
            }
        } else {
            adversarial_total += 1;
            if r.correct {
                adversarial_abstained += 1;
            }
        }
    }

    for stats in per_category.values_mut() {
        stats.accuracy = ratio(stats.correct, stats.total);
    }

    let estimated_cost_usd = total_cost_usd;

    BenchReport {
        provenance,
        per_category,
        overall_accuracy: ratio(overall_correct, overall_total),
        overall_total,
        overall_correct,
        adversarial_abstention: ratio(adversarial_abstained, adversarial_total),
        adversarial_total,
        unscorable_total,
        recall_p95_micros: p95(&mut latencies),
        total_input_tokens,
        total_output_tokens,
        estimated_cost_usd,
    }
}

/// Build a [`Provenance`] block; fusion weights, cost rates, and known flaws pinned here.
pub fn provenance(
    reader_model: impl Into<String>,
    judge_model: impl Into<String>,
    embedder_model: impl Into<String>,
    config: BenchConfig,
    dataset_note: impl Into<String>,
    dataset_sha256: impl Into<String>,
) -> Provenance {
    let w = FusionWeights::default();
    let reader_model = reader_model.into();
    let (rate_in, rate_out) = model_rate(&reader_model);
    Provenance {
        reader_model,
        judge_model: judge_model.into(),
        embedder_model: embedder_model.into(),
        reranker_model: "none".to_string(),
        top_k: config.top_k,
        temperature: 0.0,
        fusion_semantic: w.semantic,
        fusion_keyword: w.keyword,
        fusion_recency: w.recency,
        fusion_importance: w.importance,
        dataset_note: dataset_note.into(),
        dataset_sha256: dataset_sha256.into(),
        cost_rate_input_usd_per_m: rate_in,
        cost_rate_output_usd_per_m: rate_out,
        known_flaws: KNOWN_FLAWS.to_string(),
    }
}

fn ratio(num: usize, den: usize) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

/// Nearest-rank p95 of `latencies` (sorted in place). Empty -> 0.
fn p95(latencies: &mut [u128]) -> u128 {
    if latencies.is_empty() {
        return 0;
    }
    latencies.sort_unstable();
    // Nearest-rank: ceil(0.95 * n) maps to a 1-based index, clamped to len.
    let rank = ((latencies.len() as f64) * 0.95).ceil() as usize;
    let idx = rank.clamp(1, latencies.len()) - 1;
    latencies[idx]
}

#[cfg(test)]
mod cost_tests {
    use super::*;

    #[test]
    fn model_rate_matches_known_models_and_versioned_aliases() {
        assert_eq!(model_rate("gpt-4o-mini"), (0.15, 0.60));
        assert_eq!(model_rate("gpt-4o-mini-2024-07-18"), (0.15, 0.60));
        assert_eq!(model_rate("gpt-4o"), (2.50, 10.00));
        assert_eq!(model_rate("gpt-4o-2024-08-06"), (2.50, 10.00));
    }

    #[test]
    fn model_rate_mini_wins_over_the_gpt4o_prefix() {
        // "gpt-4o-mini" also starts with "gpt-4o"; the mini branch must be checked first.
        assert_eq!(model_rate("gpt-4o-mini"), (0.15, 0.60));
        assert_ne!(model_rate("gpt-4o-mini"), model_rate("gpt-4o"));
    }

    #[test]
    fn model_rate_unknown_falls_back_to_mini() {
        assert_eq!(model_rate("gpt-5.4-mini"), (0.15, 0.60));
        assert_eq!(model_rate("claude-sonnet"), (0.15, 0.60));
        assert_eq!(model_rate(""), (0.15, 0.60));
    }

    #[test]
    fn token_cost_applies_the_models_published_rate() {
        // 1M input + 1M output at gpt-4o-mini = 0.15 + 0.60 = 0.75.
        assert!((token_cost("gpt-4o-mini", 1_000_000, 1_000_000) - 0.75).abs() < 1e-9);
        // 1M input + 1M output at gpt-4o = 2.50 + 10.00 = 12.50.
        assert!((token_cost("gpt-4o", 1_000_000, 1_000_000) - 12.50).abs() < 1e-9);
    }

    #[test]
    fn token_cost_scales_input_and_output_independently() {
        // 2M input + 0.5M output at gpt-4o-mini = 2*0.15 + 0.5*0.60 = 0.30 + 0.30 = 0.60.
        assert!((token_cost("gpt-4o-mini", 2_000_000, 500_000) - 0.60).abs() < 1e-9);
    }

    #[test]
    fn token_cost_is_zero_without_tokens() {
        assert_eq!(token_cost("gpt-4o-mini", 0, 0), 0.0);
        assert_eq!(token_cost("gpt-4o", 0, 0), 0.0);
    }

    #[test]
    fn per_question_cost_bills_reader_and_judge_at_their_own_models() {
        // As in process_one_question: a gpt-4o reader and gpt-4o-mini judge are each
        // costed at their own model's rate, then summed.
        let reader = token_cost("gpt-4o", 1_000_000, 200_000); // 2.50 + 0.2*10 = 4.50
        let judge = token_cost("gpt-4o-mini", 400_000, 100_000); // 0.4*0.15 + 0.1*0.60 = 0.12
        assert!((reader - 4.50).abs() < 1e-9);
        assert!((judge - 0.12).abs() < 1e-9);
        assert!(((reader + judge) - 4.62).abs() < 1e-9);
    }
}
