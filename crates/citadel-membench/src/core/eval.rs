//! Reader and judge: turn retrieved memories into an answer, then score it.
//!
//! Isolation invariant: the reader sees only the top-k hits + the question, never
//! the transcript/gold/evidence. The judge sees the gold; the reader never does.

use std::sync::OnceLock;
use std::thread::sleep;
use std::time::{Duration, Instant};

use citadel_ai::{CompletionRequest, CompletionResponse, LLMClient, LlmError, Message, TokenUsage};
use citadel_mem::{AtomHit, AtomId, MemoryEngine, RecallProfile, RecallQuery};
use rustc_hash::FxHashSet;

use crate::core::benchmark::Benchmark;
use crate::core::error::Result;
use crate::core::ratelimit::Pacer;
use crate::{BenchConfig, ReaderOrder};

/// Default hard cap on reader/judge output so a runaway response cannot inflate cost.
const DEFAULT_MAX_TOKENS: u32 = 512;

/// Output-token cap: `CITADEL_MEMBENCH_MAX_TOKENS` overrides the caller's `default`
/// (raise it for a reasoning/CoT reader whose thinking tokens would crowd out the answer).
fn max_output_tokens(default: u32) -> u32 {
    std::env::var("CITADEL_MEMBENCH_MAX_TOKENS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(default)
}

/// Retry budget for transient (429/5xx/transport) LLM failures. The wall-clock budget
/// is the primary guard and is per-question-bounded: a call holds its role permit for
/// the whole retry loop, so an unbounded budget would let one stuck question hog a
/// slot. Terminal errors (other 4xx, malformed body) are not retried.
#[derive(Debug, Clone, Copy)]
struct RetryConfig {
    max_elapsed: Duration,
    max_attempts: u32,
    base_ms: u64,
    cap_ms: u64,
}

impl RetryConfig {
    /// Read fresh each call (cheap). NOT cached: a process-global `OnceLock` froze the
    /// first test's config, silently ignoring per-run budget overrides.
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

    /// Capped exponential backoff jittered into `[exp, 1.5*exp]`, then floored to the
    /// server's `Retry-After` (`server_ms`, the only value allowed past `cap_ms`).
    fn delay_ms(&self, attempt: u32, server_ms: Option<u64>, jitter01: f64) -> u64 {
        let shift = attempt.saturating_sub(1).min(16);
        let exp = self.base_ms.saturating_mul(1u64 << shift).min(self.cap_ms);
        let jittered = exp + ((exp as f64) * 0.5 * jitter01) as u64;
        jittered.max(server_ms.unwrap_or(0))
    }
}

/// Complete via the per-model [`Pacer`], retrying transient failures with a
/// wall-clock-bounded backoff; a residual 429 backs the whole pool off together.
fn paced_complete(
    pacer: &Pacer,
    client: &dyn LLMClient,
    req: &CompletionRequest,
) -> Result<CompletionResponse> {
    let cfg = RetryConfig::get();
    let model = client.model_id();
    let cost = client.count_tokens(&req.messages)
        + req
            .max_tokens
            .unwrap_or(max_output_tokens(DEFAULT_MAX_TOKENS)) as usize;
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

/// Server's requested wait in MILLISECONDS: the `Retry-After` header (x1000) or the
/// "try again in Xs/Xms" body hint (which keeps sub-second precision).
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

/// Wait in MILLISECONDS from a "try again in 3.46s" / "334ms" body (sub-second precise).
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

/// Deterministic-per-thread jitter in `[0, 1)` (std-only splitmix64), seeded with
/// monotonic entropy so concurrent workers desynchronize their backoffs.
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
    if attempt == 1 || attempt % 16 == 0 {
        eprintln!(
            "  retry {attempt} after {delay_ms}ms (transient: {e}; elapsed {}s/{}s)",
            spent.as_secs(),
            budget.as_secs()
        );
    }
}

/// The memory list as the reader sees it: each hit expanded with its +/-`radius`
/// adjacent turns, deduped, in the configured order. Ingest writes turns in
/// conversation order, so ascending atom id IS chronological order; a neighbor id
/// outside the region simply fetches nothing. Under `Relevance` order each hit is
/// rendered as a chronological `[id-r ..= id+r]` snippet, snippets by hit rank.
pub fn reader_view(
    eng: &MemoryEngine,
    region: &str,
    hits: Vec<AtomHit>,
    config: BenchConfig,
) -> Result<Vec<AtomHit>> {
    let radius = config.neighbor_radius as i64;
    let mut seen: FxHashSet<AtomId> = FxHashSet::default();
    let mut view: Vec<AtomHit> = Vec::with_capacity(hits.len() * (2 * radius as usize + 1));
    for hit in hits {
        for id in hit.id - radius..=hit.id + radius {
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
    if config.reader_order == ReaderOrder::Chrono {
        view.sort_by_key(|h| h.id);
    }
    Ok(view)
}

/// The reader's answer plus retrieval facts: latency, token usage, and the `dia_id`s
/// the reader actually saw (the retrieval-gap-vs-reader-miss instrumentation).
pub struct AnswerOutcome {
    pub answer: String,
    /// Recall plus neighbor-expansion latency: everything the memory system does
    /// to assemble the reader's context.
    pub recall_micros: u128,
    pub usage: TokenUsage,
    pub retrieved: Vec<String>,
}

/// Read the assembled memories: render the prompt and ask the paced, retried reader.
/// The question to answer and the date it was asked (the reader's current-date anchor;
/// `date` is empty for benchmarks without one).
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
    let retrieved = view
        .iter()
        .filter_map(|h| {
            h.payload
                .get(bench.gold_id_key())
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .collect();
    let mut req = CompletionRequest::new(bench.reader_prompt(&view, q.text, q.date));
    req.temperature = Some(0.0);
    req.max_tokens = Some(max_output_tokens(reader_max_tokens));
    let resp = paced_complete(pacer, reader, &req)?;
    Ok(AnswerOutcome {
        answer: resp.message.content,
        recall_micros,
        usage: resp.usage,
        retrieved,
    })
}

/// Recall the top-`config.top_k` memories, expand to the reader view, then ask the
/// reader; the reader call is paced + retried. Recall uses the canonical
/// `RecallProfile::default` (the scored recipe). It keeps the default wall clock:
/// grading recency as of the conversation's end was measured to hurt evidence
/// recall (diag C-asof, -4.6 any@30), so no as-of is passed here.
pub fn answer_question(
    bench: &dyn Benchmark,
    reader: &dyn LLMClient,
    pacer: &Pacer,
    eng: &MemoryEngine,
    region: &str,
    q: Question,
    config: BenchConfig,
) -> Result<AnswerOutcome> {
    let started = Instant::now();
    let hits = eng.recall(
        region,
        RecallProfile::default().apply(RecallQuery::by_text(q.text, config.top_k)),
    )?;
    let view = reader_view(eng, region, hits, config)?;
    let recall_micros = started.elapsed().as_micros();
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

pub(crate) fn complete_judge(
    judge: &dyn LLMClient,
    pacer: &Pacer,
    system: &str,
    user: &str,
) -> Result<CompletionResponse> {
    let mut req = CompletionRequest::new(vec![
        Message::system(system),
        Message::user(user.to_string()),
    ]);
    req.temperature = Some(0.0);
    req.max_tokens = Some(max_output_tokens(DEFAULT_MAX_TOKENS));
    paced_complete(pacer, judge, &req)
}

/// Parse the judge reply to a bool: prefer JSON `{"label": ...}`, else the last
/// non-empty line. Anchored on the final signal so the reasoning can't flip it.
pub(crate) fn judge_label(reply: &str) -> bool {
    if let Some(v) = json_label(reply) {
        return v.eq_ignore_ascii_case("CORRECT");
    }
    let last = reply
        .lines()
        .rev()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("");
    let up = last.to_ascii_uppercase();
    // The label appearing LAST wins (the prompt forbids emitting both).
    match (up.rfind("WRONG"), up.rfind("CORRECT")) {
        (Some(w), Some(c)) => c > w,
        (Some(_), None) => false,
        (None, Some(_)) => true,
        (None, None) => false,
    }
}

/// Extract the string value of the first `"label"` key in a JSON-ish reply.
fn json_label(reply: &str) -> Option<&str> {
    let after = &reply[reply.find("\"label\"")? + "\"label\"".len()..];
    let rest = after[after.find(':')? + 1..].trim_start();
    let rest = rest.strip_prefix('"')?;
    Some(&rest[..rest.find('"')?])
}

/// True iff the trimmed upper-cased reply begins with `token` as a whole word.
pub(crate) fn starts_with_token(reply: &str, token: &str) -> bool {
    match reply.trim().to_ascii_uppercase().strip_prefix(token) {
        Some(rest) => rest
            .chars()
            .next()
            .map_or(true, |c| !c.is_ascii_alphanumeric()),
        None => false,
    }
}
