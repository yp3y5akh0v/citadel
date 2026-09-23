//! Reader and judge: turn retrieved memories into an answer, then score it.
//!
//! Isolation invariant: the reader sees only the top-k hits + the question,
//! never the transcript/gold/evidence. The judge sees the gold, the reader
//! never does.

use std::sync::OnceLock;
use std::thread::sleep;
use std::time::{Duration, Instant};

use citadel_llm::{
    CompletionRequest, CompletionResponse, FinishReason, LLMClient, LlmError, Message, TokenUsage,
};
use citadel_mem::{AtomHit, AtomId, MemoryEngine, RecallQuery};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::core::agentic;
use crate::core::benchmark::Benchmark;
use crate::core::error::{
    BenchError, CompletedReaderCall, CompletionFailure, ReaderFailure, ReaderStage, Result,
};
use crate::core::ratelimit::Pacer;
use crate::core::retrieval::baseline_recall;
use crate::{BenchConfig, ReaderOrder};

/// Default hard cap on reader/judge output so a runaway response cannot inflate
/// cost.
const DEFAULT_MAX_TOKENS: u32 = 512;

/// Pinned so two runs of the same config are comparable. Temperature 0 alone is not
/// reproducible: the reader rewrites a large share of its answers between runs.
pub(crate) const SAMPLING_SEED: u64 = 1;

/// Output-token cap: `CITADEL_MEMBENCH_MAX_TOKENS` overrides the caller's
/// `default` (raise it for a reasoning/CoT reader whose thinking tokens would
/// crowd out the answer).
fn max_output_tokens(default: u32) -> Result<u32> {
    match std::env::var("CITADEL_MEMBENCH_MAX_TOKENS") {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Ok(raw) => raw.parse::<u32>().ok().filter(|&n| n > 0).ok_or_else(|| {
            BenchError::Dataset("CITADEL_MEMBENCH_MAX_TOKENS must be a positive u32".into())
        }),
        Err(_) => Err(BenchError::Dataset(
            "CITADEL_MEMBENCH_MAX_TOKENS must be Unicode text".into(),
        )),
    }
}

/// Retry budget for transient (429/5xx/transport) LLM failures. The wall-clock
/// budget is the primary, per-question guard: a call holds its role permit for
/// the whole retry loop, so an unbounded budget would let one stuck question
/// hog a slot. Terminal errors are not retried.
#[derive(Debug, Clone, Copy)]
struct RetryConfig {
    max_elapsed: Duration,
    max_attempts: u32,
    base_ms: u64,
    cap_ms: u64,
}

impl RetryConfig {
    /// Read fresh each call (cheap). NOT cached: a process-global `OnceLock`
    /// froze the first test's config, silently ignoring per-run budget
    /// overrides.
    fn get() -> Self {
        Self::from_env()
    }

    fn from_env() -> Self {
        let g = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(d)
        };
        Self {
            max_elapsed: Duration::from_secs(g("CITADEL_MEMBENCH_RETRY_MAX_ELAPSED_SECS", 240)),
            max_attempts: g("CITADEL_MEMBENCH_RETRY_MAX_ATTEMPTS", 12) as u32,
            base_ms: g("CITADEL_MEMBENCH_RETRY_BASE_MS", 500),
            cap_ms: g("CITADEL_MEMBENCH_RETRY_CAP_MS", 60_000),
        }
    }

    /// Capped exponential backoff jittered into `[exp, 1.5*exp]`, then floored
    /// to the server's `Retry-After` (`server_ms`, the only value allowed past
    /// `cap_ms`).
    fn delay_ms(&self, attempt: u32, server_ms: Option<u64>, jitter01: f64) -> u64 {
        let shift = attempt.saturating_sub(1).min(16);
        let exp = self.base_ms.saturating_mul(1u64 << shift).min(self.cap_ms);
        let jittered = exp + ((exp as f64) * 0.5 * jitter01) as u64;
        jittered.max(server_ms.unwrap_or(0))
    }
}

/// Complete via the per-model [`Pacer`], retrying transient failures with a
/// wall-clock-bounded backoff; a residual 429 backs the whole pool off
/// together.
fn paced_complete(
    pacer: &Pacer,
    client: &dyn LLMClient,
    req: &CompletionRequest,
    attempts: &mut Vec<CompletionAttempt>,
) -> Result<CompletionResponse> {
    let cfg = RetryConfig::get();
    let model = client.model_id();
    let output_tokens = match req.max_tokens {
        Some(limit) => limit,
        None => max_output_tokens(DEFAULT_MAX_TOKENS)?,
    };
    let cost = client
        .count_tokens(&req.messages)
        .saturating_add(output_tokens as usize);
    let started = Instant::now();
    let mut attempt: u32 = 0;
    loop {
        pacer.acquire(model, cost); // pace submission BEFORE firing
        let result = client.complete(req);
        attempts.push(CompletionAttempt {
            ordinal: attempt + 1,
            outcome: match &result {
                Ok(response) => AttemptOutcome::Response {
                    finish_reason: response.finish_reason.into(),
                    usage: response.usage,
                    message: response.message.clone(),
                },
                Err(error) if error.is_pre_dispatch() => AttemptOutcome::NotDispatched {
                    error: error.to_string(),
                },
                Err(error) => AttemptOutcome::FailedUsageUnknown {
                    error: error.to_string(),
                },
            },
        });
        match result {
            Ok(resp) => return Ok(resp),
            Err(e) if e.is_retryable() => {
                pacer.penalize(model); // whole pool backs off in unison
                let spent = started.elapsed();
                // Wall-clock budget is the primary terminating guard.
                if spent >= cfg.max_elapsed || attempt + 1 >= cfg.max_attempts {
                    return Err(e.into());
                }
                let server_ms = server_retry_after_ms(&e);
                let remaining = cfg.max_elapsed - spent;
                let delay = cfg
                    .delay_ms(attempt + 1, server_ms, jitter01(attempt))
                    .min(remaining.as_millis() as u64)
                    .max(1);
                log_retry(attempt + 1, delay, &e, spent, cfg.max_elapsed);
                sleep(Duration::from_millis(delay));
                attempt += 1;
            }
            Err(e) => return Err(e.into()), // terminal: fail fast
        }
    }
}

/// Server's requested wait in milliseconds: the `Retry-After` header (x1000) or
/// the "try again in Xs/Xms" body hint (which keeps sub-second precision).
fn server_retry_after_ms(e: &LlmError) -> Option<u64> {
    match e {
        LlmError::Http { message, .. } => parse_retry_after_body_ms(message).or_else(|| match e {
            LlmError::Http {
                retry_after: Some(s),
                ..
            } => Some(s.saturating_mul(1_000)),
            _ => None,
        }),
        _ => None,
    }
}

/// Wait in ms from a "try again in 3.46s" / "334ms" body (sub-second precise).
fn parse_retry_after_body_ms(msg: &str) -> Option<u64> {
    let lower = msg.to_ascii_lowercase();
    let after = lower[lower.find("try again in")? + "try again in".len()..].trim_start();
    let num: String = after
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let val: f64 = num.parse().ok()?;
    let rest = after[num.len()..].trim_start();
    let ms = if rest.starts_with("ms") {
        val
    } else {
        val * 1_000.0
    };
    Some(ms.ceil().max(1.0) as u64)
}

/// Deterministic-per-thread jitter in `[0, 1)` (std-only splitmix64), seeded
/// with monotonic entropy so concurrent workers desynchronize their backoffs.
fn jitter01(attempt: u32) -> f64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    let mut h = DefaultHasher::new();
    std::thread::current().id().hash(&mut h);
    attempt.hash(&mut h);
    start.elapsed().as_nanos().hash(&mut h);
    let mut z = h.finish().wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    (z >> 11) as f64 / (1u64 << 53) as f64
}

/// Log the first retry then every 16th, so a sustained storm stays readable.
fn log_retry(attempt: u32, delay_ms: u64, e: &LlmError, spent: Duration, budget: Duration) {
    if attempt == 1 || attempt.is_multiple_of(16) {
        eprintln!(
            "  retry {attempt} after {delay_ms}ms (transient: {e}; elapsed {}s/{}s)",
            spent.as_secs(),
            budget.as_secs()
        );
    }
}

/// The memory list as the reader sees it: each hit expanded with +/-`radius`
/// adjacent turns, deduped, in the configured order. Ingest writes turns in
/// source order, so ascending atom id restores that order; under
/// `Relevance` each hit renders as a `[id-r ..= id+r]` snippet by hit rank.
/// Expansion preserves the first original hit's metadata for every seed ID.
pub fn reader_view(
    eng: &MemoryEngine,
    region: &str,
    hits: Vec<AtomHit>,
    config: BenchConfig,
) -> Result<Vec<AtomHit>> {
    config.validate()?;
    let radius = i64::try_from(config.neighbor_radius).map_err(|_| {
        BenchError::Dataset("neighbor_radius exceeds the atom identifier range".into())
    })?;
    let mut seen: FxHashSet<AtomId> = FxHashSet::default();
    let mut view: Vec<AtomHit> = Vec::with_capacity(hits.len());
    let mut seed_sessions = Vec::new();
    if radius == 0 {
        for hit in hits {
            if seen.insert(hit.id) {
                view.push(hit);
            }
        }
    } else {
        let mut seed_ids = Vec::with_capacity(hits.len());
        let mut seeds = FxHashMap::default();
        let mut seen_sessions = FxHashSet::default();
        for hit in hits {
            if let std::collections::hash_map::Entry::Vacant(entry) = seeds.entry(hit.id) {
                if config.reader_order == ReaderOrder::Sessions {
                    let session = session_key(&hit)?;
                    if seen_sessions.insert(session.clone()) {
                        seed_sessions.push(session);
                    }
                }
                seed_ids.push(hit.id);
                entry.insert(hit);
            }
        }
        for seed_id in seed_ids {
            let start = seed_id.checked_sub(radius).ok_or_else(|| {
                BenchError::Dataset("neighbor range underflows atom identifiers".into())
            })?;
            let end = seed_id.checked_add(radius).ok_or_else(|| {
                BenchError::Dataset("neighbor range overflows atom identifiers".into())
            })?;
            for id in start..=end {
                if !seen.insert(id) {
                    continue;
                }
                // A later-ranked seed can first appear in an earlier window.
                // Move its original hit rather than replacing its scores with
                // an unranked fetch; its own window is still expanded later.
                if let Some(seed) = seeds.remove(&id) {
                    view.push(seed);
                } else if let Some(neighbor) = eng.fetch_one(region, id)? {
                    view.push(neighbor);
                }
            }
        }
    }
    match config.reader_order {
        ReaderOrder::Chrono => view.sort_by_key(|h| h.id),
        ReaderOrder::Relevance => {}
        ReaderOrder::Sessions => view = session_grouped(view, seed_sessions)?,
    }
    Ok(view)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum SessionKey {
    Number(i64),
    Text(String),
}

fn session_key(hit: &AtomHit) -> Result<SessionKey> {
    if let Some(session) = hit.payload.get("session").and_then(|value| value.as_i64()) {
        Ok(SessionKey::Number(session))
    } else if let Some(session_id) = hit
        .payload
        .get("session_id")
        .and_then(|value| value.as_str())
    {
        Ok(SessionKey::Text(session_id.to_owned()))
    } else {
        Err(BenchError::Dataset(
            "session reader order requires numeric payload.session or string payload.session_id on every hit"
                .into(),
        ))
    }
}

/// Original-hit sessions follow their best hit's rank; neighbor-only sessions
/// follow in first-encounter order. Turns inside each block return to conversation
/// order. Session metadata is required: there is no flat fallback.
fn session_grouped(view: Vec<AtomHit>, mut order: Vec<SessionKey>) -> Result<Vec<AtomHit>> {
    let mut seen: FxHashSet<SessionKey> = order.iter().cloned().collect();
    let mut blocks: FxHashMap<SessionKey, Vec<AtomHit>> = FxHashMap::default();
    for hit in view {
        let session = session_key(&hit)?;
        if seen.insert(session.clone()) {
            order.push(session.clone());
        }
        blocks.entry(session).or_default().push(hit);
    }

    let mut grouped = Vec::new();
    for session in order {
        let mut block = blocks.remove(&session).expect("session block was inserted");
        block.sort_unstable_by_key(|hit| hit.id);
        grouped.extend(block);
    }
    Ok(grouped)
}

/// The reader's answer plus retrieval facts: latency, token usage, and the
/// `dia_id`s the reader actually saw (the retrieval-gap-vs-reader-miss
/// instrumentation).
pub struct AnswerOutcome {
    pub answer: String,
    /// Completion states in call order, including a discarded extraction.
    pub reader_finish_reasons: Vec<CompletionFinish>,
    pub reader_calls: Vec<CompletionCallAudit>,
    /// Recall plus neighbor-expansion latency: everything the memory system
    /// does to assemble the reader's context.
    pub recall_micros: u128,
    pub retrieved: Vec<String>,
    pub retrieved_atom_ids: Vec<AtomId>,
}

/// Configured client identity and measured usage for one completion call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReaderRoute {
    Direct,
    NotEnumeration,
    EmptyEnumeration,
    Enumeration,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(tag = "stage", content = "route", rename_all = "snake_case")]
pub enum CompletionPurpose {
    Extraction,
    Reader(ReaderRoute),
    Judge,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AttemptOutcome {
    Response {
        finish_reason: CompletionFinish,
        #[serde(serialize_with = "serialize_optional_usage")]
        usage: Option<TokenUsage>,
        /// Returned logical completion, not the raw provider HTTP response.
        #[serde(serialize_with = "serialize_assistant_message")]
        message: citadel_llm::AssistantMessage,
    },
    /// The client guarantees that no provider dispatch or spend occurred.
    NotDispatched { error: String },
    /// Transport, malformed response, and other errors do not prove zero spend.
    FailedUsageUnknown { error: String },
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CompletionAttempt {
    pub ordinal: u32,
    #[serde(flatten)]
    pub outcome: AttemptOutcome,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CompletionCallAudit {
    #[serde(flatten)]
    pub purpose: CompletionPurpose,
    pub request_sha256: String,
    /// Configured client model; the client interface does not expose returned snapshots.
    pub model_id: String,
    pub client: citadel_llm::ClientRequestIdentity,
    pub input_tokens_estimate: usize,
    pub max_output_tokens: Option<u32>,
    pub rendered_atom_ids: Vec<AtomId>,
    /// Validated client-reported usage; absent after an error or when the
    /// returned response did not include complete, valid token counters.
    #[serde(serialize_with = "serialize_optional_usage")]
    pub usage: Option<TokenUsage>,
    pub attempts: Vec<CompletionAttempt>,
}

impl CompletionCallAudit {
    pub fn unknown_usage_attempts(&self) -> u64 {
        self.attempts
            .iter()
            .filter(|attempt| {
                matches!(
                    attempt.outcome,
                    AttemptOutcome::FailedUsageUnknown { .. }
                        | AttemptOutcome::Response { usage: None, .. }
                )
            })
            .count() as u64
    }

    /// Estimate from client-reported usage when every dispatched attempt has known usage.
    pub fn estimated_cost_usd(&self) -> Option<f64> {
        if self.unknown_usage_attempts() != 0 {
            return None;
        }
        match self.usage {
            Some(usage) => {
                crate::token_cost(&self.model_id, usage.input_tokens, usage.output_tokens)
                    .or(usage.cost_usd)
                    .filter(|cost| cost.is_finite() && *cost >= 0.0)
            }
            None => Some(0.0), // No returned response and no unknown-dispatch attempt.
        }
    }
}

fn complete_with_audit(
    pacer: &Pacer,
    reader: &dyn LLMClient,
    request: &CompletionRequest,
    rendered_atom_ids: Vec<AtomId>,
    purpose: CompletionPurpose,
) -> Result<(CompletionResponse, CompletionCallAudit)> {
    let model_id = reader.model_id().to_owned();
    let client = reader.request_identity();
    let encoded = serde_json::to_vec(&serde_json::json!({
        "schema": "citadel-membench-call-v1",
        "model_id": model_id,
        "client": client,
        "canonical_request": citadel_llm::canonical_json(request),
        "seed": request.seed,
    }))?;
    let mut audit = CompletionCallAudit {
        purpose,
        request_sha256: crate::sha256_hex(&encoded),
        model_id,
        client,
        input_tokens_estimate: reader.count_tokens(&request.messages),
        max_output_tokens: request.max_tokens,
        rendered_atom_ids,
        usage: None,
        attempts: Vec::new(),
    };
    let response =
        paced_complete(pacer, reader, request, &mut audit.attempts).map_err(|source| {
            BenchError::Completion(Box::new(CompletionFailure {
                call: audit.clone(),
                source: Box::new(source),
            }))
        })?;
    audit.usage = response.usage;
    Ok((response, audit))
}

fn retrieved_ids(bench: &dyn Benchmark, view: &[AtomHit]) -> Result<Vec<String>> {
    view.iter()
        .map(|hit| {
            hit.payload
                .get(bench.gold_id_key())
                .and_then(|value| value.as_str())
                .filter(|id| !id.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| {
                    BenchError::Dataset(format!(
                        "retrieved atom {} lacks nonempty {} metadata",
                        hit.id,
                        bench.gold_id_key()
                    ))
                })
        })
        .collect()
}

fn validate_rendered_atoms(view: &[AtomHit], rendered: &[AtomId]) -> Result<()> {
    let mut remaining: FxHashSet<_> = view.iter().map(|hit| hit.id).collect();
    if rendered.len() != view.len()
        || rendered.iter().any(|id| !remaining.remove(id))
        || !remaining.is_empty()
    {
        return Err(BenchError::Dataset(
            "reader prompt must render every retrieved atom exactly once".into(),
        ));
    }
    Ok(())
}

/// Read the assembled memories: render the prompt and ask the paced, retried
/// reader. The question to answer and the date it was asked (the reader's
/// current-date anchor; `date` is empty for benchmarks without one).
#[derive(Debug, Clone, Copy)]
pub struct Question<'a> {
    pub text: &'a str,
    pub date: &'a str,
}

fn read_assembled(
    bench: &dyn Benchmark,
    reader: &dyn LLMClient,
    pacer: &Pacer,
    q: Question,
    reader_max_tokens: u32,
    view: Vec<AtomHit>,
    recall_micros: u128,
) -> Result<AnswerOutcome> {
    let retrieved = retrieved_ids(bench, &view)?;
    let retrieved_atom_ids = view.iter().map(|hit| hit.id).collect();
    let rendered = bench.reader_prompt(&view, q.text, q.date)?;
    validate_rendered_atoms(&view, &rendered.atom_ids)?;
    let mut req = CompletionRequest::new(rendered.messages);
    req.temperature = Some(0.0);
    req.seed = Some(SAMPLING_SEED);
    req.max_tokens = Some(max_output_tokens(reader_max_tokens)?);
    let (resp, audit) = complete_with_audit(
        pacer,
        reader,
        &req,
        rendered.atom_ids,
        CompletionPurpose::Reader(ReaderRoute::Direct),
    )?;
    Ok(AnswerOutcome {
        answer: resp.message.content,
        reader_finish_reasons: vec![resp.finish_reason.into()],
        reader_calls: vec![audit],
        recall_micros,
        retrieved,
        retrieved_atom_ids,
    })
}

/// Recall the top-`config.top_k` memories through the baseline engine path, expand to the
/// reader view, then ask the reader (paced + retried).
pub fn answer_question(
    bench: &dyn Benchmark,
    reader: &dyn LLMClient,
    pacer: &Pacer,
    eng: &MemoryEngine,
    region: &str,
    q: Question,
    config: BenchConfig,
) -> Result<AnswerOutcome> {
    config.validate()?;
    let started = Instant::now();
    let hits = baseline_recall(eng, region, RecallQuery::by_text(q.text, config.top_k))?;
    let view = reader_view(eng, region, hits, config)?;
    let recall_micros = started.elapsed().as_micros();
    if config.agentic && agentic::is_aggregation_question(q.text) {
        return answer_aggregation(bench, reader, pacer, q, config.reader_max_tokens, &view).map(
            |outcome| AnswerOutcome {
                recall_micros,
                ..outcome
            },
        );
    }
    read_assembled(
        bench,
        reader,
        pacer,
        q,
        config.reader_max_tokens,
        view,
        recall_micros,
    )
}

/// Two-pass agentic read: extract -> dedup/sort/count in code -> answer from
/// the list. Only an explicit NOT_ENUMERATION selects the ordinary prompt. Same
/// retrieval/view/isolation as [`read_assembled`]: reader sees only the
/// retrieved memories and the question, never gold.
fn answer_aggregation(
    bench: &dyn Benchmark,
    reader: &dyn LLMClient,
    pacer: &Pacer,
    q: Question,
    reader_max_tokens: u32,
    view: &[AtomHit],
) -> Result<AnswerOutcome> {
    let retrieved = retrieved_ids(bench, view)?;
    let retrieved_atom_ids: Vec<_> = view.iter().map(|hit| hit.id).collect();
    let rendered = bench.reader_prompt(view, q.text, q.date)?;
    validate_rendered_atoms(view, &rendered.atom_ids)?;
    let mut messages = rendered.messages;
    let extraction = agentic::extraction_prompt(bench, view, q.text, q.date)?;
    validate_rendered_atoms(view, &extraction.atom_ids)?;
    let output_cap = max_output_tokens(reader_max_tokens)?;
    let mut extract = CompletionRequest::new(extraction.messages);
    extract.temperature = Some(0.0);
    extract.seed = Some(SAMPLING_SEED);
    extract.max_tokens = Some(output_cap);
    let (extracted, extract_audit) = complete_with_audit(
        pacer,
        reader,
        &extract,
        extraction.atom_ids,
        CompletionPurpose::Extraction,
    )
    .map_err(|source| reader_failure(ReaderStage::Extraction, Vec::new(), source))?;
    let receipt = CompletedReaderCall {
        call: extract_audit.clone(),
        finish_reason: extracted.finish_reason.into(),
    };
    if extracted.finish_reason != FinishReason::Stop || !extracted.message.tool_calls.is_empty() {
        return Err(reader_failure(
            ReaderStage::ExtractionValidation,
            vec![receipt],
            BenchError::InvalidExtractionCompletion {
                reason: "extraction requires a normal stop and no tool calls",
                response: extracted.message.content,
                finish_reason: extracted.finish_reason,
            },
        ));
    }
    let decision = agentic::parse_extraction(&extracted.message.content).map_err(|source| {
        reader_failure(
            ReaderStage::ExtractionValidation,
            vec![receipt.clone()],
            BenchError::InvalidExtraction {
                response: extracted.message.content.clone(),
                source,
            },
        )
    })?;
    let route = match &decision {
        agentic::ExtractionDecision::NotEnumeration => ReaderRoute::NotEnumeration,
        agentic::ExtractionDecision::Items(items) if items.is_empty() => {
            ReaderRoute::EmptyEnumeration
        }
        agentic::ExtractionDecision::Items(_) => ReaderRoute::Enumeration,
    };
    if let agentic::ExtractionDecision::Items(items) = decision {
        let anchor =
            agentic::anchor_message(&agentic::dedup_and_sort(items)).map_err(|source| {
                reader_failure(
                    ReaderStage::ExtractionValidation,
                    vec![receipt.clone()],
                    BenchError::InvalidExtraction {
                        response: extracted.message.content.clone(),
                        source,
                    },
                )
            })?;
        messages.push(anchor);
    }
    let mut answer = CompletionRequest::new(messages);
    answer.temperature = Some(0.0);
    answer.seed = Some(SAMPLING_SEED);
    answer.max_tokens = Some(output_cap);
    let (resp, answer_audit) = complete_with_audit(
        pacer,
        reader,
        &answer,
        rendered.atom_ids,
        CompletionPurpose::Reader(route),
    )
    .map_err(|source| reader_failure(ReaderStage::FinalAnswer, vec![receipt], source))?;
    Ok(AnswerOutcome {
        answer: resp.message.content,
        reader_finish_reasons: vec![extracted.finish_reason.into(), resp.finish_reason.into()],
        reader_calls: vec![extract_audit, answer_audit],
        recall_micros: 0,
        retrieved,
        retrieved_atom_ids,
    })
}

fn reader_failure(
    stage: ReaderStage,
    completed_calls: Vec<CompletedReaderCall>,
    source: BenchError,
) -> BenchError {
    BenchError::Reader(Box::new(ReaderFailure {
        stage,
        completed_calls,
        source: Box::new(source),
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionFinish {
    Stop,
    Length,
    ToolUse,
    Refusal,
    ContentFilter,
    Error,
}

impl From<FinishReason> for CompletionFinish {
    fn from(reason: FinishReason) -> Self {
        match reason {
            FinishReason::Stop => Self::Stop,
            FinishReason::Length => Self::Length,
            FinishReason::ToolUse => Self::ToolUse,
            FinishReason::Refusal => Self::Refusal,
            FinishReason::ContentFilter => Self::ContentFilter,
            FinishReason::Error => Self::Error,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct JudgeOutcome {
    pub correct: bool,
    pub response: String,
    pub finish_reason: CompletionFinish,
    #[serde(serialize_with = "serialize_optional_usage")]
    pub usage: Option<TokenUsage>,
    pub call: CompletionCallAudit,
}

impl JudgeOutcome {
    pub(crate) fn from_response(
        correct: bool,
        response: CompletionResponse,
        call: CompletionCallAudit,
    ) -> Self {
        let mut usage = response.usage;
        if call.unknown_usage_attempts() != 0 {
            if let Some(usage) = &mut usage {
                usage.cost_usd = None;
            }
        }
        Self {
            correct,
            response: response.message.content,
            finish_reason: response.finish_reason.into(),
            usage,
            call,
        }
    }
}

fn serialize_usage<S: serde::Serializer>(
    usage: &TokenUsage,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::SerializeStruct;

    let mut fields = serializer.serialize_struct("TokenUsage", 3)?;
    fields.serialize_field("input_tokens", &usage.input_tokens)?;
    fields.serialize_field("output_tokens", &usage.output_tokens)?;
    fields.serialize_field("cost_usd", &usage.cost_usd)?;
    fields.end()
}

fn serialize_assistant_message<S: serde::Serializer>(
    message: &citadel_llm::AssistantMessage,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::Serialize;
    serde_json::json!({
        "content": message.content,
        "tool_calls": message.tool_calls.iter().map(|call| serde_json::json!({
            "id": call.id, "name": call.name, "arguments": call.arguments,
        })).collect::<Vec<_>>(),
    })
    .serialize(serializer)
}

fn serialize_optional_usage<S: serde::Serializer>(
    usage: &Option<TokenUsage>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match usage {
        Some(usage) => serialize_usage(usage, serializer),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn complete_judge(
    judge: &dyn LLMClient,
    pacer: &Pacer,
    system: &str,
    user: &str,
) -> Result<(CompletionResponse, CompletionCallAudit)> {
    let mut req = CompletionRequest::new(vec![
        Message::system(system),
        Message::user(user.to_string()),
    ]);
    req.temperature = Some(0.0);
    req.seed = Some(SAMPLING_SEED);
    req.max_tokens = Some(max_output_tokens(DEFAULT_MAX_TOKENS)?);
    let (response, audit) =
        complete_with_audit(pacer, judge, &req, Vec::new(), CompletionPurpose::Judge)?;
    if response.finish_reason != FinishReason::Stop {
        return Err(
            invalid_judge_response(&response, "completion did not finish normally")
                .with_completion_call(audit),
        );
    }
    Ok((response, audit))
}

#[derive(serde::Deserialize)]
enum JudgeLabel {
    #[serde(rename = "CORRECT")]
    Correct,
    #[serde(rename = "WRONG")]
    Wrong,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct JudgeVerdict {
    label: JudgeLabel,
}

/// Accept a final JSON verdict or an exact plain label used by earlier clients.
/// A single trailing JSON object may also follow prose or span multiple lines.
pub(crate) fn judge_label(response: &CompletionResponse) -> Result<bool> {
    let last = response
        .message
        .content
        .lines()
        .rev()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("");
    match last {
        "CORRECT" => Ok(true),
        "WRONG" => Ok(false),
        _ => {
            // Preserve the existing whole-line form before trying a suffix. The
            // fallback starts at the first opening brace, so it cannot skip an
            // invalid outer object or choose between multiple inline objects.
            let verdict = serde_json::from_str::<JudgeVerdict>(last).ok().or_else(|| {
                let content = response.message.content.trim();
                let start = content.find('{')?;
                let prefix = content[..start].trim_end();
                if prefix.contains('}') || prefix.ends_with(['[', '"']) {
                    return None;
                }
                serde_json::from_str::<JudgeVerdict>(&content[start..]).ok()
            });
            verdict
                .map(|verdict| matches!(verdict.label, JudgeLabel::Correct))
                .ok_or_else(|| {
                    invalid_judge_response(response, "expected a final CORRECT/WRONG verdict")
                })
        }
    }
}

pub(crate) fn abstention_label(response: &CompletionResponse) -> Result<bool> {
    match response.message.content.trim() {
        "CORRECT" => Ok(true),
        "WRONG" => Ok(false),
        _ => Err(invalid_judge_response(
            response,
            "expected exactly CORRECT or WRONG",
        )),
    }
}

fn invalid_judge_response(response: &CompletionResponse, reason: &'static str) -> BenchError {
    BenchError::InvalidJudgeResponse {
        reason,
        response: response.message.content.clone(),
        finish_reason: response.finish_reason,
        usage: response.usage,
        call: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn judge_label_accepts_inline_and_multiline_final_json() {
        for (text, expected) in [
            ("The dates differ. {\"label\": \"WRONG\"}", false),
            ("The answer matches. {\"label\":\"CORRECT\"}", true),
            ("{\n  \"label\": \"WRONG\"\n}\n", false),
            (
                "The answer matches.\n{\n  \"label\": \"CORRECT\"\n}\n",
                true,
            ),
        ] {
            assert_eq!(
                judge_label(&CompletionResponse::text(text)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn judge_label_preserves_existing_final_line_forms() {
        for (text, expected) in [
            ("CORRECT", true),
            ("Explanation.\n WRONG \n\n", false),
            (
                "Earlier prose contains {braces}.\n{\"label\":\"CORRECT\"}",
                true,
            ),
            ("{\"label\":\"CORRECT\"}\n{\"label\":\"WRONG\"}", false),
        ] {
            assert_eq!(
                judge_label(&CompletionResponse::text(text)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn judge_label_rejects_malformed_or_ambiguous_suffixes() {
        for text in [
            "The answer is CORRECT.",
            "Explanation. {}",
            "Explanation. {\"label\":\"correct\"}",
            "Explanation. {\"label\":\"WRONG\",\"extra\":true}",
            "Explanation. {\"label\":\"WRONG\",\"label\":\"CORRECT\"}",
            "Explanation. {\"label\":\"WRONG\"} trailing text",
            "Explanation. {\"label\":\"WRONG\"} {\"label\":\"CORRECT\"}",
            "Explanation. {\"label\":\"WRONG\"",
            "Explanation. {{\"label\":\"WRONG\"}",
            "Explanation. {\"nested\":{\"label\":\"WRONG\"}}",
            "[{\"label\":\"WRONG\"}",
            "\"{\"label\":\"WRONG\"}",
            "} {\"label\":\"WRONG\"}",
        ] {
            assert!(
                judge_label(&CompletionResponse::text(text)).is_err(),
                "{text}"
            );
        }
    }

    #[test]
    fn abstention_label_still_requires_only_a_plain_label() {
        assert!(abstention_label(&CompletionResponse::text(" CORRECT\n")).unwrap());
        assert!(!abstention_label(&CompletionResponse::text("WRONG")).unwrap());
        for text in [
            "Explanation.\nCORRECT",
            "{\"label\":\"CORRECT\"}",
            "Explanation. {\"label\":\"WRONG\"}",
            "{\n\"label\":\"WRONG\"\n}",
        ] {
            assert!(
                abstention_label(&CompletionResponse::text(text)).is_err(),
                "{text}"
            );
        }
    }

    #[test]
    fn reader_audit_matches_native_recall_for_plaintext_and_sealed_regions() {
        use std::sync::Arc;

        use citadel_mem::{Embedder, MockEmbedder, RecallQuery};

        use crate::benchmarks::locomo::{dataset, ingest, Locomo};
        use crate::core::retrieval::baseline_recall;

        for encrypted in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = citadel::DatabaseBuilder::new(dir.path().join("audit.cdl"))
                .passphrase(b"test")
                .argon2_profile(citadel::Argon2Profile::Iot)
                .enable_region_keys(encrypted)
                .create()
                .unwrap();
            let eng = MemoryEngine::open(Arc::new(db)).unwrap();
            let embedder: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(64));
            let region = "audit";
            if encrypted {
                eng.create_encrypted_region(region, embedder).unwrap();
            } else {
                eng.create_region(region, embedder).unwrap();
            }
            let sample = dataset::parse_root(&serde_json::json!([{
                "sample_id": region,
                "conversation": {
                    "session_1_date_time": "9:00 am on 1 May, 2023",
                    "session_1": [
                        {"speaker":"Alice","dia_id":"D1:1","text":"My dog Rex is a retriever."},
                        {"speaker":"Bob","dia_id":"D1:2","text":"I visited the coast."}
                    ],
                    "session_2_date_time": "9:00 am on 2 May, 2023",
                    "session_2": [
                        {"speaker":"Alice","dia_id":"D2:1","text":"Rex likes the park."}
                    ]
                }, "qa": []
            }]))
            .unwrap()
            .remove(0);
            ingest::ingest_sample(&eng, region, &sample).unwrap();
            let question = "Where does Rex like to go?";
            let config = BenchConfig {
                top_k: 2,
                reader_order: ReaderOrder::Sessions,
                ..BenchConfig::default()
            };
            let expected =
                baseline_recall(&eng, region, RecallQuery::by_text(question, 2)).unwrap();
            assert_eq!(expected.len(), 2);
            let expected = reader_view(&eng, region, expected, config).unwrap();
            let bench = Locomo::new(true);
            let prompt = bench.reader_prompt(&expected, question, "").unwrap();
            let reader = citadel_llm::testing::capturing(vec![CompletionResponse::text("answer")]);
            let outcome = answer_question(
                &bench,
                &*reader.client(),
                &Pacer::unbounded(),
                &eng,
                region,
                Question {
                    text: question,
                    date: "",
                },
                config,
            )
            .unwrap();
            assert_eq!(outcome.retrieved, retrieved_ids(&bench, &expected).unwrap());
            assert_eq!(outcome.retrieved_atom_ids, prompt.atom_ids);
            assert_eq!(outcome.reader_calls[0].rendered_atom_ids, prompt.atom_ids);
            let requests = reader.requests();
            assert_eq!(requests.len(), 1);
            let mut expected_request = requests[0].clone();
            expected_request.messages = prompt.messages;
            assert_eq!(
                citadel_llm::canonical_json(&requests[0]),
                citadel_llm::canonical_json(&expected_request)
            );

            for ids in [
                vec![prompt.atom_ids[0]],
                vec![prompt.atom_ids[0], prompt.atom_ids[0]],
                vec![prompt.atom_ids[0], i64::MAX],
            ] {
                assert!(validate_rendered_atoms(&expected, &ids).is_err());
            }
            let mut reversed = prompt.atom_ids;
            reversed.reverse();
            assert!(validate_rendered_atoms(&expected, &reversed).is_ok());
        }
    }

    #[test]
    fn reader_audit_binds_message_order_seed_and_output_cap() {
        let reader = citadel_llm::testing::constant("answer");
        let mut request =
            CompletionRequest::new(vec![Message::user("first"), Message::user("second")]);
        request.max_tokens = Some(17);
        request.seed = Some(1);
        let audit = |request: &CompletionRequest| {
            complete_with_audit(
                &Pacer::unbounded(),
                &*reader,
                request,
                vec![7, 9],
                CompletionPurpose::Reader(ReaderRoute::Direct),
            )
            .unwrap()
            .1
        };
        let baseline = audit(&request);
        assert_eq!(baseline.request_sha256, audit(&request).request_sha256);
        assert_eq!(baseline.client, reader.request_identity());
        assert_eq!(baseline.model_id, reader.model_id());
        let expected = serde_json::json!({
            "schema": "citadel-membench-call-v1",
            "model_id": reader.model_id(),
            "client": reader.request_identity(),
            "canonical_request": citadel_llm::canonical_json(&request),
            "seed": request.seed,
        });
        assert_eq!(
            baseline.request_sha256,
            crate::sha256_hex(&serde_json::to_vec(&expected).unwrap())
        );
        assert_eq!(baseline.max_output_tokens, Some(17));
        assert_eq!(baseline.rendered_atom_ids, [7, 9]);
        assert_eq!(
            baseline.input_tokens_estimate,
            reader.count_tokens(&request.messages)
        );
        request.messages.reverse();
        assert_ne!(baseline.request_sha256, audit(&request).request_sha256);
        request.messages.reverse();
        request.seed = None;
        let unset = audit(&request).request_sha256;
        request.seed = Some(0);
        assert_ne!(unset, audit(&request).request_sha256);
        request.seed = Some(2);
        assert_ne!(baseline.request_sha256, audit(&request).request_sha256);
        request.seed = Some(1);
        request.max_tokens = Some(18);
        assert_ne!(baseline.request_sha256, audit(&request).request_sha256);
    }

    #[test]
    fn call_identity_distinguishes_configured_models() {
        let request = CompletionRequest::new(vec![Message::user("same body")]);
        let hash = |model: &str| {
            let client =
                citadel_llm::factory::from_fn(model, |_| Ok(CompletionResponse::text("answer")));
            complete_with_audit(
                &Pacer::unbounded(),
                &*client,
                &request,
                Vec::new(),
                CompletionPurpose::Reader(ReaderRoute::Direct),
            )
            .unwrap()
            .1
            .request_sha256
        };
        assert_eq!(hash("reader-a"), hash("reader-a"));
        assert_ne!(hash("reader-a"), hash("reader-b"));
    }

    #[test]
    fn successful_responses_preserve_unknown_usage_and_distinguish_measured_zero() {
        let mut hashes = Vec::new();
        for known in [false, true] {
            let mut response = CompletionResponse::text("retained answer");
            response.finish_reason = FinishReason::Length;
            if known {
                response.usage = Some(TokenUsage {
                    input_tokens: 0,
                    output_tokens: 0,
                    cost_usd: Some(0.0),
                });
            }
            let client = citadel_llm::testing::capturing(vec![response]);
            let request = CompletionRequest::new(vec![Message::user("same request")]);
            let (response, audit) = complete_with_audit(
                &Pacer::unbounded(),
                &*client.client(),
                &request,
                vec![7],
                CompletionPurpose::Reader(ReaderRoute::Direct),
            )
            .unwrap();
            assert_eq!(response.message.content, "retained answer");
            assert_eq!(response.finish_reason, FinishReason::Length);
            assert_eq!(audit.usage.is_some(), known);
            assert_eq!(audit.unknown_usage_attempts(), u64::from(!known));
            let accounting = crate::core::error::UsageAccounting::from_calls([&audit]);
            assert_eq!(accounting.observed_input_tokens, 0);
            assert_eq!(accounting.observed_output_tokens, 0);
            assert_eq!(accounting.estimated_cost_usd, known.then_some(0.0));
            let value = serde_json::to_value(&audit).unwrap();
            assert_eq!(
                value["attempts"][0]["message"]["content"],
                "retained answer"
            );
            assert_eq!(value["attempts"][0]["finish_reason"], "length");
            assert_eq!(value["attempts"][0]["usage"].is_null(), !known);
            assert_eq!(value["usage"].is_null(), !known);
            hashes.push(audit.request_sha256);
        }
        assert_eq!(
            hashes[0], hashes[1],
            "response usage cannot alter the request"
        );
    }

    #[test]
    fn retries_preserve_each_attempt_and_do_not_price_unknown_spend() {
        for succeeds in [true, false] {
            let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let observed = seen.clone();
            let client = citadel_llm::factory::from_fn("gpt-4o-mini", move |request| {
                let mut seen = observed.lock().unwrap();
                seen.push(citadel_llm::canonical_json(request));
                if seen.len() == 1 {
                    return Err(LlmError::Http {
                        status: 429,
                        retry_after: None,
                        message: "try again in 1ms".into(),
                    });
                }
                if !succeeds {
                    return Err(LlmError::Backend("invalid body".into()));
                }
                let mut response = CompletionResponse::text("answer");
                response.usage = Some(TokenUsage {
                    input_tokens: 40,
                    output_tokens: 9,
                    cost_usd: Some(0.03),
                });
                Ok(response)
            });
            let request = CompletionRequest::new(vec![Message::user("same request")]);
            let result = complete_with_audit(
                &Pacer::unbounded(),
                &*client,
                &request,
                vec![7],
                CompletionPurpose::Judge,
            );
            let audit = match result {
                Ok((_, audit)) => {
                    assert!(succeeds);
                    audit
                }
                Err(BenchError::Completion(failure)) => {
                    assert!(!succeeds);
                    failure.call
                }
                _ => panic!("unexpected retry outcome"),
            };
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[0], seen[1], "retries preserve the exact request");
            assert_eq!(
                audit.attempts.iter().map(|a| a.ordinal).collect::<Vec<_>>(),
                [1, 2]
            );
            assert_eq!(audit.unknown_usage_attempts(), if succeeds { 1 } else { 2 });
            assert_eq!(audit.estimated_cost_usd(), None);
            assert_eq!(audit.usage.is_some(), succeeds);
            let accounting = crate::core::error::UsageAccounting::from_calls([&audit]);
            assert_eq!(
                accounting.observed_input_tokens,
                if succeeds { 40 } else { 0 }
            );
            assert_eq!(accounting.estimated_cost_usd, None);
        }
    }

    #[test]
    fn pre_dispatch_failure_has_a_request_receipt_but_no_unknown_spend() {
        let client = citadel_llm::factory::from_fn("gpt-4o-mini", |_| {
            Err(LlmError::UnsupportedRequest("schema unsupported".into()))
        });
        let request = CompletionRequest::new(vec![Message::user("question")]);
        let error = complete_with_audit(
            &Pacer::unbounded(),
            &*client,
            &request,
            vec![],
            CompletionPurpose::Reader(ReaderRoute::Direct),
        )
        .unwrap_err();
        let BenchError::Completion(failure) = error else {
            panic!("missing call receipt")
        };
        assert_eq!(failure.call.attempts.len(), 1);
        assert!(matches!(
            failure.call.attempts[0].outcome,
            AttemptOutcome::NotDispatched { .. }
        ));
        assert_eq!(failure.call.unknown_usage_attempts(), 0);
        assert_eq!(failure.call.estimated_cost_usd(), Some(0.0));
        assert_eq!(failure.call.request_sha256.len(), 64);
        assert!(failure.call.usage.is_none());
    }
}
