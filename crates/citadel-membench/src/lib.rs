//! Long-term-memory benchmark harness: a dataset-agnostic engine (`core`)
//! driving pluggable benchmarks (`benchmarks::{locomo, longmemeval}`).

pub mod benchmarks;
pub mod core;

use std::sync::Arc;

use rustc_hash::FxHashMap;
use serde::Serialize;

use citadel_llm::LLMClient;
use citadel_mem::{Embedder, FusionWeights, MemoryEngine};

use crate::benchmarks::locomo::Locomo;
use crate::core::benchmark::Benchmark;

pub use benchmarks::locomo::dataset::{
    load, load_with_hash, parse_root, Category, QaSample, Sample, Turn,
};
pub use benchmarks::locomo::ingest::{ingest_sample, turn_content};
pub use benchmarks::locomo::prompts::{build_reader_prompt, judge_abstained, judge_correct};
pub use core::db::{open_bench_db, BenchDb};
pub use core::error::{BenchError, Result};
pub use core::error::{
    CompletedOutput, QuestionBatchFailure, QuestionCompletion, QuestionEvent, QuestionFailure,
    QuestionIdentity, QuestionObserver, QuestionStage, UsageAccounting,
};
pub use core::eval::{answer_question, reader_view, AnswerOutcome, Question};
pub use core::hash::sha256_hex;
pub use core::ratelimit::{default_tpm_for_model, Gate, Pacer};

/// Estimated USD for one model's token usage at its published rate.
fn token_cost(model: &str, input_tokens: u32, output_tokens: u32) -> Option<f64> {
    if input_tokens == 0 && output_tokens == 0 {
        return Some(0.0);
    }
    let (rate_in, rate_out) = citadel_llm::known_token_rates_usd_per_million(model)?;
    Some(
        (f64::from(input_tokens) / 1_000_000.0) * rate_in
            + (f64::from(output_tokens) / 1_000_000.0) * rate_out,
    )
}

/// Order in which retrieved memories are rendered for the reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderOrder {
    /// Conversation order (ascending atom id = ingest = chronological).
    Chrono,
    /// Fusion/reranker relevance order, best hit first.
    Relevance,
    /// Sessions ordered by their best-ranked hit, chronological within each
    /// session. The retrieved set is unchanged.
    Sessions,
}

impl ReaderOrder {
    pub fn label(self) -> &'static str {
        match self {
            ReaderOrder::Chrono => "chrono",
            ReaderOrder::Relevance => "relevance",
            ReaderOrder::Sessions => "sessions",
        }
    }
}

/// Knobs for a benchmark run.
#[derive(Debug, Clone, Copy)]
pub struct BenchConfig {
    /// Number of memories retrieved per question and shown to the reader.
    pub top_k: usize,
    /// Presentation order of those memories in the reader prompt.
    pub reader_order: ReaderOrder,
    /// Adjacent turns rendered around each hit (0 disables expansion).
    pub neighbor_radius: usize,
    /// Add deterministic relative-date annotations when rendering reader evidence.
    /// Stored content and retrieval are unchanged.
    pub temporal_glosses: bool,
    /// Reader output-token cap; raise for a chain-of-thought reader (env
    /// override: `CITADEL_MEMBENCH_MAX_TOKENS`).
    pub reader_max_tokens: u32,
    /// Agentic reader: aggregation-shaped questions (detected from the question
    /// text only) run extract -> code dedup/count/sort -> answer-from-list. A
    /// separate labeled number; recall is untouched.
    pub agentic: bool,
}

impl Default for BenchConfig {
    fn default() -> Self {
        // Measured defaults; selection rationale is in RESULTS.md.
        Self {
            top_k: 50,
            reader_order: ReaderOrder::Relevance,
            neighbor_radius: 0,
            temporal_glosses: false,
            reader_max_tokens: 512,
            agentic: false,
        }
    }
}

impl BenchConfig {
    pub fn validate(self) -> Result<()> {
        if self.top_k == 0 || self.reader_max_tokens == 0 {
            return Err(BenchError::Dataset(
                "top_k and reader_max_tokens must be positive".into(),
            ));
        }
        i64::try_from(self.neighbor_radius).map_err(|_| {
            BenchError::Dataset("neighbor_radius exceeds the atom identifier range".into())
        })?;
        Ok(())
    }
}

/// The per-question outcome, before aggregation.
#[derive(Debug, Clone, Serialize)]
pub struct QuestionResult {
    /// Stable row identity. `sample_id + qa_index` is unique even when the
    /// dataset repeats the same question text or assigns it conflicting labels.
    pub sample_id: String,
    pub qa_index: usize,
    pub category: Category,
    /// Unscorable (empty gold key) is excluded; adversarial is always scorable.
    pub scorable: bool,
    /// For scored categories: judged correct. For adversarial: judged
    /// abstained.
    pub correct: bool,
    pub recall_micros: u128,
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Failed attempts lacking both measured usage and a no-dispatch guarantee.
    pub unknown_usage_attempts: u64,
    /// Estimated USD: reader + judge tokens, each at its model's rate.
    pub cost_usd: Option<f64>,
    /// Evidence IDs in reader-view order. Annotation coverage alone does not
    /// establish whether an incorrect answer was caused by retrieval or reading.
    pub retrieved: Vec<String>,
    pub retrieved_atom_ids: Vec<citadel_mem::AtomId>,
    /// Gold evidence `dia_id`s (from the dataset); joined against `retrieved`.
    pub gold_evidence: Vec<String>,
    /// Rendered text of each gold evidence turn, parallel to `gold_evidence`.
    /// Audit only: never fed into recall/read, so a miss stays classifiable
    /// from the log. An unknown gold id renders a `<no turn for ...>` marker.
    pub gold_turn_texts: Vec<String>,
    /// Whether each gold evidence turn reached the reader's view, parallel to
    /// `gold_evidence`. Splits a miss into retrieval-gap (any false) vs
    /// reader-miss.
    pub gold_in_view: Vec<bool>,
    /// Audit trail: question, gold, and the reader's predicted answer.
    pub question: String,
    pub gold: String,
    pub predicted: String,
    pub reader_finish_reasons: Vec<core::eval::CompletionFinish>,
    pub reader_calls: Vec<core::eval::CompletionCallAudit>,
    /// Absent only when an unscorable question made no judge call.
    pub judge: Option<core::eval::JudgeOutcome>,
}

impl QuestionResult {
    pub fn completion_receipt(&self) -> QuestionCompletion {
        QuestionCompletion {
            identity: QuestionIdentity::Locomo {
                sample_id: self.sample_id.clone(),
                qa_index: self.qa_index,
            },
            calls: self
                .reader_calls
                .iter()
                .cloned()
                .chain(self.judge.iter().map(|judge| judge.call.clone()))
                .collect(),
            output: CompletedOutput {
                answer: self.predicted.clone(),
                judge: self.judge.clone(),
            },
        }
    }
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
    /// Presentation order of retrieved memories in the reader prompt.
    pub reader_order: String,
    /// Adjacent turns rendered around each hit (0 = none).
    pub neighbor_radius: usize,
    /// Whether the optional multi-call aggregation reader was enabled.
    pub agentic: bool,
    /// Whether deterministic relative-date annotations were rendered.
    pub temporal_glosses: bool,
    /// Identifies the annotation rules for a run that enabled temporal glosses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temporal_gloss_policy: Option<&'static str>,
    pub temperature: f32,
    /// Sampling seed sent with every reader/judge request (best-effort on the
    /// provider side); pinned so paired runs are comparable.
    pub sampling_seed: u64,
    /// Retrieval fusion weights (citadel-mem defaults); recorded for
    /// reproducibility.
    pub fusion_semantic: f32,
    pub fusion_keyword: f32,
    pub fusion_recency: f32,
    pub fusion_importance: f32,
    pub dataset_note: String,
    /// SHA-256 of the scored dataset file: pins the exact input.
    pub dataset_sha256: String,
    /// The reader model's published per-1M rates; the bench costs reader and
    /// judge each at its own model's rate (estimated, not billed).
    pub cost_rate_input_usd_per_m: Option<f64>,
    pub cost_rate_output_usd_per_m: Option<f64>,
    pub known_flaws: String,
}

/// The full, serializable benchmark report.
#[derive(Debug, Clone, Serialize)]
pub struct BenchReport {
    pub provenance: Provenance,
    pub per_category: FxHashMap<String, CategoryStats>,
    /// Headline accuracy over the four scored categories (adversarial
    /// excluded).
    pub overall_accuracy: f64,
    pub overall_total: usize,
    pub overall_correct: usize,
    /// Secondary metric: fraction of adversarial questions the reader abstained
    /// on.
    pub adversarial_abstention: f64,
    pub adversarial_total: usize,
    /// Scored questions skipped for an empty/malformed gold key (not in
    /// accuracy).
    pub unscorable_total: usize,
    pub recall_p95_micros: u128,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub unknown_usage_attempts: u64,
    pub estimated_cost_usd: Option<f64>,
}

/// Whether to use encrypted regions (per-atom sealed + crypto erasure), from
/// `CITADEL_LOCOMO_ENCRYPTED` ("true"/"false", case-insensitive; unset=false).
pub fn encrypted_regions() -> bool {
    std::env::var("CITADEL_LOCOMO_ENCRYPTED")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Create the per-conversation region: encrypted when [`encrypted_regions`] is
/// on, else the plaintext path used by the historical baselines.
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

/// Run one conversation end-to-end: ingest into a fresh region, then retrieve,
/// read, and judge each question. Returns one result per question.
pub fn run_sample(
    eng: &MemoryEngine,
    sample: &Sample,
    embedder: Arc<dyn Embedder>,
    reader: &dyn LLMClient,
    judge: &dyn LLMClient,
    config: BenchConfig,
) -> Result<Vec<QuestionResult>> {
    run_sample_observed(
        eng,
        sample,
        embedder,
        reader,
        judge,
        config,
        false,
        &Pacer::unbounded(),
        &mut |_| Ok(()),
    )
}

/// Like [`run_sample`] but invokes `on_result` for each completed or failed question
/// (live tracing); scoring is identical. `reuse = true` validates the persisted
/// corpus before recalling without ingestion.
#[allow(clippy::too_many_arguments)]
pub fn run_sample_observed(
    eng: &MemoryEngine,
    sample: &Sample,
    embedder: Arc<dyn Embedder>,
    reader: &dyn LLMClient,
    judge: &dyn LLMClient,
    config: BenchConfig,
    reuse: bool,
    pacer: &Pacer,
    on_result: &mut QuestionObserver<'_, QuestionResult>,
) -> Result<Vec<QuestionResult>> {
    benchmarks::locomo::dataset::validate_samples(std::slice::from_ref(sample))?;
    config.validate()?;
    if reuse {
        core::db::attach_reused_region(eng, &sample.sample_id, embedder, encrypted_regions())?;
        benchmarks::locomo::ingest::validate_reuse(eng, &sample.sample_id, sample)?;
    } else {
        create_bench_region(eng, &sample.sample_id, embedder)?;
        ingest_sample(eng, &sample.sample_id, sample)?;
    }

    // dia_id -> rendered turn text, built once for the per-question gold audit.
    let gold_index: FxHashMap<&str, String> = sample
        .turns
        .iter()
        .map(|t| (t.dia_id.as_str(), turn_content(t)))
        .collect();

    // Reader and judge keep independent in-flight caps; `pacer` enforces
    // per-model TPM. Tasks run on OS threads, NOT rayon: each blocks (HTTP,
    // gate waits) and recall() uses rayon internally, so nesting would
    // deadlock. CITADEL_LOCOMO_CONCURRENCY=1 = serial.
    let legacy = std::env::var("CITADEL_LOCOMO_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    let (reader_n, judge_n) = match legacy {
        Some(1) => (1, 1),
        _ => (
            env_usize("CITADEL_LOCOMO_READER_CONCURRENCY", 3),
            env_usize("CITADEL_LOCOMO_JUDGE_CONCURRENCY", 12),
        ),
    };
    let workers = legacy.unwrap_or(8).max(reader_n).max(judge_n);
    let reader_gate = Gate::new(reader_n);
    let judge_gate = Gate::new(judge_n);

    // The callback fires in completion order (serialized) for live tracing;
    // results return in question order, so the report is byte-identical to a
    // serial run. A worker error aborts.
    let total = sample.qa.len();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let failed = std::sync::atomic::AtomicBool::new(false);
    let observed = std::sync::Mutex::new(on_result);
    let err_slot: std::sync::Mutex<Vec<QuestionFailure>> = std::sync::Mutex::new(Vec::new());
    let (tx, rx) = std::sync::mpsc::channel::<(usize, QuestionResult)>();
    let (rg, jg) = (&reader_gate, &judge_gate);
    let gi = &gold_index;
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
                        config,
                        &sample.qa[i],
                        i,
                        gi,
                        pacer,
                        rg,
                        jg,
                    ) {
                        Ok(r) => {
                            // Run the observer under its lock, then send: never
                            // hold two locks at once.
                            let observe = {
                                let mut observer = observed_r.lock().expect("observer poisoned");
                                (*observer)(QuestionEvent::Completed(&r))
                            };
                            match observe {
                                Ok(()) => {
                                    let _ = tx.send((i, r));
                                }
                                Err(e) => {
                                    failed_r.store(true, Relaxed);
                                    let failure = QuestionFailure::new(
                                        QuestionIdentity::Locomo {
                                            sample_id: sample.sample_id.clone(),
                                            qa_index: i,
                                        },
                                        QuestionStage::Observer,
                                        e,
                                    )
                                    .prepend_calls(
                                        r.reader_calls
                                            .iter()
                                            .cloned()
                                            .chain(r.judge.iter().map(|j| j.call.clone())),
                                    )
                                    .with_completed_output(r.predicted, r.judge);
                                    core::error::observe_failure(failure, observed_r, err_r);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            failed_r.store(true, Relaxed);
                            core::error::observe_failure(e, observed_r, err_r);
                            break;
                        }
                    }
                }
            });
        }
        drop(tx); // drop the original sender so `rx` closes once all workers finish
    });

    let failures = err_slot.into_inner().expect("err slot poisoned");
    if !failures.is_empty() {
        let mut completed: Vec<_> = rx.into_iter().collect();
        completed.sort_by_key(|(index, _)| *index);
        return Err(BenchError::Questions(Box::new(QuestionBatchFailure {
            failures,
            completed: completed
                .into_iter()
                .map(|(_, result)| result.completion_receipt())
                .collect(),
        })));
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

/// Score one question: recall -> read -> judge. Self-contained and
/// order-independent (runs concurrently); the reader and judge calls each hold
/// their own permit.
#[allow(clippy::too_many_arguments)]
fn process_one_question(
    eng: &MemoryEngine,
    region: &str,
    reader: &dyn LLMClient,
    judge: &dyn LLMClient,
    config: BenchConfig,
    qa: &QaSample,
    qa_index: usize,
    gold_index: &FxHashMap<&str, String>,
    pacer: &Pacer,
    reader_gate: &Gate,
    judge_gate: &Gate,
) -> std::result::Result<QuestionResult, QuestionFailure> {
    let identity = QuestionIdentity::Locomo {
        sample_id: region.to_owned(),
        qa_index,
    };
    // Empty gold on a scored question = malformed key: record unscorable (no
    // LLM call) rather than grading it wrong. Returns before acquiring any
    // gate/pacer.
    if !qa.is_scorable() {
        return Ok(QuestionResult {
            sample_id: region.to_owned(),
            qa_index,
            category: qa.category,
            scorable: false,
            correct: false,
            recall_micros: 0,
            input_tokens: 0,
            output_tokens: 0,
            unknown_usage_attempts: 0,
            cost_usd: Some(0.0),
            retrieved: Vec::new(),
            retrieved_atom_ids: Vec::new(),
            gold_evidence: qa.evidence.clone(),
            gold_turn_texts: resolve_gold_texts(&qa.evidence, gold_index),
            gold_in_view: gold_in_view_flags(&qa.evidence, &[]),
            question: qa.question.clone(),
            gold: qa.gold.clone(),
            predicted: String::new(),
            reader_finish_reasons: Vec::new(),
            reader_calls: Vec::new(),
            judge: None,
        });
    }

    let bench = Locomo::new(config.reader_order == ReaderOrder::Sessions)
        .with_temporal_glosses(config.temporal_glosses);
    let outcome = {
        let _permit = reader_gate.acquire();
        let q = Question {
            text: &qa.question,
            date: "",
        };
        answer_question(&bench, reader, pacer, eng, region, q, config)
            .map_err(|error| QuestionFailure::new(identity.clone(), QuestionStage::Reader, error))?
    };

    let judge_outcome = {
        let _permit = judge_gate.acquire();
        bench
            .judge(
                judge,
                pacer,
                qa.category.is_scored(),
                &qa.question,
                &qa.gold,
                &outcome.answer,
            )
            .map_err(|error| {
                QuestionFailure::new(identity.clone(), QuestionStage::Judge, error)
                    .prepend_calls(outcome.reader_calls.clone())
                    .with_completed_output(outcome.answer.clone(), None)
            })?
    };

    // Gold instrumentation computed before `outcome.retrieved` is moved into
    // the result.
    let gold_in_view = gold_in_view_flags(&qa.evidence, &outcome.retrieved);
    let gold_turn_texts = resolve_gold_texts(&qa.evidence, gold_index);

    let accounting = UsageAccounting::from_calls(
        outcome
            .reader_calls
            .iter()
            .chain(std::iter::once(&judge_outcome.call)),
    );
    Ok(QuestionResult {
        sample_id: region.to_owned(),
        qa_index,
        category: qa.category,
        scorable: true,
        correct: judge_outcome.correct,
        recall_micros: outcome.recall_micros,
        input_tokens: accounting.observed_input_tokens,
        output_tokens: accounting.observed_output_tokens,
        unknown_usage_attempts: accounting.unknown_usage_attempts,
        cost_usd: accounting.estimated_cost_usd,
        retrieved: outcome.retrieved,
        retrieved_atom_ids: outcome.retrieved_atom_ids,
        gold_evidence: qa.evidence.clone(),
        gold_turn_texts,
        gold_in_view,
        question: qa.question.clone(),
        gold: qa.gold.clone(),
        predicted: outcome.answer,
        reader_finish_reasons: outcome.reader_finish_reasons,
        reader_calls: outcome.reader_calls,
        judge: Some(judge_outcome),
    })
}

/// Roll per-question results into a [`BenchReport`] (overall = scored
/// categories only).
pub fn aggregate(results: &[QuestionResult], provenance: Provenance) -> BenchReport {
    let mut per_category: FxHashMap<String, CategoryStats> = FxHashMap::default();
    let mut overall_total = 0usize;
    let mut overall_correct = 0usize;
    let mut adversarial_total = 0usize;
    let mut adversarial_abstained = 0usize;
    let mut unscorable_total = 0usize;

    let mut total_input_tokens = 0u64;
    let mut total_output_tokens = 0u64;
    let mut unknown_usage_attempts = 0u64;
    let mut total_cost_usd = Some(0.0f64);
    let mut latencies = Vec::with_capacity(results.len());

    for r in results {
        total_input_tokens = total_input_tokens.saturating_add(r.input_tokens);
        total_output_tokens = total_output_tokens.saturating_add(r.output_tokens);
        unknown_usage_attempts = unknown_usage_attempts.saturating_add(r.unknown_usage_attempts);
        total_cost_usd = core::error::sum_costs(total_cost_usd, r.cost_usd);
        // Unscorable questions skip recall (latency 0); excluding keeps p95
        // honest.
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
        unknown_usage_attempts,
        estimated_cost_usd,
    }
}

/// Build a [`Provenance`] block; fusion weights, cost rates, and known flaws
/// pinned here.
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
    let rate = citadel_llm::known_token_rates_usd_per_million(&reader_model);
    Provenance {
        reader_model,
        judge_model: judge_model.into(),
        embedder_model: embedder_model.into(),
        reranker_model: "none".to_string(),
        top_k: config.top_k,
        reader_order: config.reader_order.label().to_string(),
        neighbor_radius: config.neighbor_radius,
        agentic: config.agentic,
        temporal_glosses: config.temporal_glosses,
        temporal_gloss_policy: config.temporal_glosses.then_some(core::temporal::POLICY),
        temperature: 0.0,
        sampling_seed: core::eval::SAMPLING_SEED,
        fusion_semantic: w.semantic,
        fusion_keyword: w.keyword,
        fusion_recency: w.recency,
        fusion_importance: w.importance,
        dataset_note: dataset_note.into(),
        dataset_sha256: dataset_sha256.into(),
        cost_rate_input_usd_per_m: rate.map(|(input, _)| input),
        cost_rate_output_usd_per_m: rate.map(|(_, output)| output),
        known_flaws: Locomo::new(false).known_flaws().to_string(),
    }
}

fn ratio(num: usize, den: usize) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

/// Resolve each gold `dia_id` to its rendered turn text via `index`, parallel
/// to `evidence`. An unknown id renders a `<no turn for ...>` marker rather
/// than dropping.
fn resolve_gold_texts(evidence: &[String], index: &FxHashMap<&str, String>) -> Vec<String> {
    evidence
        .iter()
        .map(|d| {
            index
                .get(d.as_str())
                .cloned()
                .unwrap_or_else(|| format!("<no turn for {d}>"))
        })
        .collect()
}

/// Per-gold-id presence in the reader's view: `true` iff the gold `dia_id` is
/// in `retrieved`. Parallel to `evidence`.
fn gold_in_view_flags(evidence: &[String], retrieved: &[String]) -> Vec<bool> {
    evidence
        .iter()
        .map(|d| retrieved.iter().any(|r| r == d))
        .collect()
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
    fn costs_use_shared_rates_including_the_launch_snapshot() {
        assert_eq!(token_cost("gpt-4o-mini", 1_000_000, 1_000_000), Some(0.75));
        assert_eq!(token_cost("gpt-4o", 1_000_000, 1_000_000), Some(12.50));
        assert_eq!(
            token_cost("gpt-4o-2024-05-13", 1_000_000, 1_000_000),
            Some(20.0)
        );
    }

    #[test]
    fn unknown_rates_do_not_fabricate_a_cost() {
        assert_eq!(token_cost("unlisted-model", 100, 50), None);
        assert_eq!(token_cost("gemini-unlisted", 100, 50), None);
        assert_eq!(token_cost("unlisted-model", 0, 0), Some(0.0));
    }

    #[test]
    fn token_cost_scales_input_and_output_independently() {
        assert!((token_cost("gpt-4o-mini", 2_000_000, 500_000).unwrap() - 0.60).abs() < 1e-9);
    }
}

#[cfg(test)]
mod provenance_tests {
    use super::*;

    #[test]
    fn temporal_policy_is_recorded_only_when_enabled() {
        assert!(!BenchConfig::default().temporal_glosses);
        for enabled in [false, true] {
            let config = BenchConfig {
                temporal_glosses: enabled,
                ..BenchConfig::default()
            };
            let report = serde_json::to_value(provenance(
                "reader", "judge", "embedder", config, "fixture", "hash",
            ))
            .unwrap();
            assert_eq!(report["temporal_glosses"], enabled);
            assert_eq!(
                report.get("temporal_gloss_policy"),
                enabled
                    .then(|| serde_json::Value::String(core::temporal::POLICY.into()))
                    .as_ref(),
            );
        }
    }
}

#[cfg(test)]
mod gold_instrumentation_tests {
    use super::*;

    fn index() -> FxHashMap<&'static str, String> {
        let mut m = FxHashMap::default();
        m.insert(
            "D2:1",
            "[3pm] Alice: Rex is a golden retriever.".to_string(),
        );
        m.insert("D2:2", "[3pm] Alice: I paid 1200.".to_string());
        m
    }

    #[test]
    fn resolve_gold_texts_maps_ids_and_marks_unknown() {
        let idx = index();
        let texts = resolve_gold_texts(&["D2:1".to_string(), "D9:9".to_string()], &idx);
        assert_eq!(texts[0], "[3pm] Alice: Rex is a golden retriever.");
        assert_eq!(texts[1], "<no turn for D9:9>");
        assert_eq!(texts.len(), 2, "parallel to evidence, one row per id");
    }

    #[test]
    fn gold_in_view_flags_are_per_id_membership() {
        let evidence = vec!["D2:1".to_string(), "D2:2".to_string()];
        let retrieved = vec!["D2:1".to_string(), "D1:1".to_string()];
        assert_eq!(gold_in_view_flags(&evidence, &retrieved), vec![true, false]);
        // No evidence -> no flags (no spurious row).
        assert!(gold_in_view_flags(&[], &retrieved).is_empty());
    }
}
