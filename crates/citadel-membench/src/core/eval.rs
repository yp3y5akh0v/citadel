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
use crate::core::error::{BenchError, Result};
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
        match client.complete(req) {
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
    for hit in hits {
        if radius == 0 {
            if seen.insert(hit.id) {
                view.push(hit);
            }
            continue;
        }
        let start = hit.id.checked_sub(radius).ok_or_else(|| {
            BenchError::Dataset("neighbor range underflows atom identifiers".into())
        })?;
        let end = hit.id.checked_add(radius).ok_or_else(|| {
            BenchError::Dataset("neighbor range overflows atom identifiers".into())
        })?;
        for id in start..=end {
            if !seen.insert(id) {
                continue;
            }
            if id == hit.id {
                view.push(hit.clone());
            } else if let Some(neighbor) = eng.fetch_one(region, id)? {
                view.push(neighbor);
            }
        }
    }
    match config.reader_order {
        ReaderOrder::Chrono => view.sort_by_key(|h| h.id),
        ReaderOrder::Relevance => {}
        ReaderOrder::Sessions => view = session_grouped(view)?,
    }
    Ok(view)
}

/// The best hit fixes each session's position; turns inside it return to
/// conversation order. Session metadata is required: there is no flat fallback.
fn session_grouped(view: Vec<AtomHit>) -> Result<Vec<AtomHit>> {
    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    enum SessionKey {
        Number(i64),
        Text(String),
    }

    let mut order = Vec::new();
    let mut blocks: FxHashMap<SessionKey, Vec<AtomHit>> = FxHashMap::default();
    for hit in view {
        let session = if let Some(session) =
            hit.payload.get("session").and_then(|value| value.as_i64())
        {
            SessionKey::Number(session)
        } else if let Some(session_id) = hit
            .payload
            .get("session_id")
            .and_then(|value| value.as_str())
        {
            SessionKey::Text(session_id.to_owned())
        } else {
            return Err(crate::BenchError::Dataset(
                "session reader order requires numeric payload.session or string payload.session_id on every hit"
                    .into(),
            ));
        };
        if !blocks.contains_key(&session) {
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
    pub usage: TokenUsage,
    pub retrieved: Vec<String>,
    pub retrieved_atom_ids: Vec<AtomId>,
}

/// Configured client identity and measured usage for one completion call.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CompletionCallAudit {
    pub request_sha256: String,
    /// Configured client model; the client interface does not expose returned snapshots.
    pub model_id: String,
    pub client: citadel_llm::ClientRequestIdentity,
    pub input_tokens_estimate: usize,
    pub max_output_tokens: Option<u32>,
    pub rendered_atom_ids: Vec<AtomId>,
    #[serde(serialize_with = "serialize_usage")]
    pub usage: TokenUsage,
}

fn complete_with_audit(
    pacer: &Pacer,
    reader: &dyn LLMClient,
    request: &CompletionRequest,
    rendered_atom_ids: Vec<AtomId>,
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
        request_sha256: crate::sha256_hex(&encoded),
        model_id,
        client,
        input_tokens_estimate: reader.count_tokens(&request.messages),
        max_output_tokens: request.max_tokens,
        rendered_atom_ids,
        usage: TokenUsage::default(),
    };
    let response = paced_complete(pacer, reader, request)?;
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
    let (resp, audit) = complete_with_audit(pacer, reader, &req, rendered.atom_ids)?;
    Ok(AnswerOutcome {
        answer: resp.message.content,
        reader_finish_reasons: vec![resp.finish_reason.into()],
        reader_calls: vec![audit],
        recall_micros,
        usage: resp.usage,
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
        match answer_aggregation(bench, reader, pacer, q, config.reader_max_tokens, &view)? {
            Aggregation::Answered(outcome) => {
                return Ok(AnswerOutcome {
                    recall_micros,
                    ..outcome
                })
            }
            // Unusable extraction: fall back, but keep its spend on the ledger.
            Aggregation::FellBack(spent, finish_reason, audit) => {
                let mut out = read_assembled(
                    bench,
                    reader,
                    pacer,
                    q,
                    config.reader_max_tokens,
                    view,
                    recall_micros,
                )?;
                add_usage(&mut out.usage, &spent);
                out.reader_finish_reasons.insert(0, finish_reason);
                out.reader_calls.insert(0, audit);
                return Ok(out);
            }
        }
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

/// Outcome of the agentic attempt: an answer, or a fallback carrying the tokens
/// the discarded extraction call already spent.
enum Aggregation {
    Answered(AnswerOutcome),
    FellBack(TokenUsage, CompletionFinish, CompletionCallAudit),
}

/// Accumulate `b` into `a` (tokens add; cost adds when both sides price it).
fn add_usage(a: &mut TokenUsage, b: &TokenUsage) {
    a.input_tokens = a.input_tokens.saturating_add(b.input_tokens);
    a.output_tokens = a.output_tokens.saturating_add(b.output_tokens);
    a.cost_usd = match (a.cost_usd, b.cost_usd) {
        (Some(x), Some(y)) => Some(x + y),
        _ => None,
    };
}

/// Two-pass agentic read: extract -> dedup/sort/count in code -> answer from
/// the list. `None` (unparseable) falls back to the single-prompt path. Same
/// retrieval/view/isolation as [`read_assembled`]: reader sees only the
/// retrieved memories and the question, never gold.
fn answer_aggregation(
    bench: &dyn Benchmark,
    reader: &dyn LLMClient,
    pacer: &Pacer,
    q: Question,
    reader_max_tokens: u32,
    view: &[AtomHit],
) -> Result<Aggregation> {
    let retrieved = retrieved_ids(bench, view)?;
    let retrieved_atom_ids: Vec<_> = view.iter().map(|hit| hit.id).collect();
    let rendered = bench.reader_prompt(view, q.text, q.date)?;
    validate_rendered_atoms(view, &rendered.atom_ids)?;
    let mut messages = rendered.messages;
    let extraction = agentic::extraction_prompt(view, q.text, q.date);
    let mut extract = CompletionRequest::new(extraction.messages);
    extract.temperature = Some(0.0);
    extract.seed = Some(SAMPLING_SEED);
    extract.max_tokens = Some(max_output_tokens(reader_max_tokens)?);
    let (extracted, extract_audit) =
        complete_with_audit(pacer, reader, &extract, extraction.atom_ids)?;
    let Some(items) = agentic::parse_items(&extracted.message.content) else {
        return Ok(Aggregation::FellBack(
            extracted.usage,
            extracted.finish_reason.into(),
            extract_audit,
        ));
    };
    let items = agentic::dedup_and_sort(items);

    messages.push(agentic::anchor_message(&items));
    let mut answer = CompletionRequest::new(messages);
    answer.temperature = Some(0.0);
    answer.seed = Some(SAMPLING_SEED);
    answer.max_tokens = Some(max_output_tokens(reader_max_tokens)?);
    let (resp, answer_audit) = complete_with_audit(pacer, reader, &answer, rendered.atom_ids)?;
    let mut usage = extracted.usage;
    add_usage(&mut usage, &resp.usage);
    Ok(Aggregation::Answered(AnswerOutcome {
        answer: resp.message.content,
        reader_finish_reasons: vec![extracted.finish_reason.into(), resp.finish_reason.into()],
        reader_calls: vec![extract_audit, answer_audit],
        recall_micros: 0,
        usage,
        retrieved,
        retrieved_atom_ids,
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
    #[serde(serialize_with = "serialize_usage")]
    pub usage: TokenUsage,
    pub call: CompletionCallAudit,
}

impl JudgeOutcome {
    pub(crate) fn from_response(
        correct: bool,
        response: CompletionResponse,
        call: CompletionCallAudit,
    ) -> Self {
        Self {
            correct,
            response: response.message.content,
            finish_reason: response.finish_reason.into(),
            usage: response.usage,
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
    let (response, audit) = complete_with_audit(pacer, judge, &req, Vec::new())?;
    if response.finish_reason != FinishReason::Stop {
        return Err(invalid_judge_response(
            &response,
            "completion did not finish normally",
        ));
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
            complete_with_audit(&Pacer::unbounded(), &*reader, request, vec![7, 9])
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
            complete_with_audit(&Pacer::unbounded(), &*client, &request, Vec::new())
                .unwrap()
                .1
                .request_sha256
        };
        assert_eq!(hash("reader-a"), hash("reader-a"));
        assert_ne!(hash("reader-a"), hash("reader-b"));
    }

    #[test]
    fn summing_usage_does_not_hide_an_unknown_cost() {
        let mut usage = TokenUsage {
            input_tokens: u32::MAX,
            output_tokens: 2,
            cost_usd: Some(0.1),
        };
        add_usage(
            &mut usage,
            &TokenUsage {
                input_tokens: 3,
                output_tokens: 4,
                cost_usd: None,
            },
        );
        assert_eq!(usage.input_tokens, u32::MAX);
        assert_eq!(usage.output_tokens, 6);
        assert_eq!(usage.cost_usd, None);
    }
}
