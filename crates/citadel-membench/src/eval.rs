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

use crate::error::Result;
use crate::ratelimit::Pacer;
use crate::{BenchConfig, ReaderOrder};

/// Default hard cap on reader/judge output so a runaway response cannot inflate cost.
const DEFAULT_MAX_TOKENS: u32 = 512;

/// Output-token cap, read fresh so CITADEL_LOCOMO_MAX_TOKENS can raise it for a
/// reasoning reader whose thinking tokens would otherwise crowd out the answer.
fn max_output_tokens() -> u32 {
    std::env::var("CITADEL_LOCOMO_MAX_TOKENS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_MAX_TOKENS)
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
            max_elapsed: Duration::from_secs(g("CITADEL_LOCOMO_RETRY_MAX_ELAPSED_SECS", 240)),
            max_attempts: g("CITADEL_LOCOMO_RETRY_MAX_ATTEMPTS", 12) as u32,
            base_ms: g("CITADEL_LOCOMO_RETRY_BASE_MS", 500),
            cap_ms: g("CITADEL_LOCOMO_RETRY_CAP_MS", 60_000),
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
    let cost =
        client.count_tokens(&req.messages) + req.max_tokens.unwrap_or(max_output_tokens()) as usize;
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

/// Build the reader prompt from only the hits + question, with one category-blind
/// system prompt (the gold category would be test-metadata leakage). Hits are
/// rendered verbatim in the order given (see [`reader_view`]); each turn's text
/// already carries its `[date] speaker:` prefix from ingest. The signature
/// isolates gold.
pub fn build_reader_prompt(hits: &[AtomHit], question: &str) -> Vec<Message> {
    let system = "You answer the question using ONLY the provided memories. Each \
         memory is a line from a past conversation, prefixed with the date it was \
         said and the speaker, and may end with a photo description in the form \
         '[shared a photo: ...]' or '[image search: ...]' - treat those photo \
         descriptions as valid evidence (e.g. for what a sign, poster, or painting \
         shows or says). Carefully analyze all the memories and combine them across \
         turns as needed.\n\
         For questions about time, read the dates on the memories and convert \
         relative references to specific dates: 'yesterday' means the day before \
         that memory's date, 'last year' means the prior calendar year, etc.\n\
         When the question asks whether something is likely or what someone would \
         probably do/think/have ('would X likely ...', 'is X likely ...'), give \
         your best-supported verdict (e.g. 'Yes' or 'Likely no') with a one-clause \
         reason grounded in the memories, rather than declining. Likewise, state a \
         fact that the memories clearly imply even if not worded identically (e.g. \
         a 'single parent' who mentions a breakup is single).\n\
         Answer the question whenever the memories support an answer - directly, by \
         a clear single-step inference, or by combining several turns. Do NOT \
         decline just because the answer is not stated word-for-word, because it \
         must be inferred, or because it is spread across turns: a 'next month' said \
         in May means June; 'discomfort with religious conservatives' supports 'not \
         very religious'.\n\
         Before answering, find the specific memory that states or clearly implies \
         the fact for the EXACT person, object, or subject the question names, and \
         answer with what that memory says. Attribute each fact to whoever the \
         memory says it belongs to: if a memory gives a fact about one person or \
         object, do not transfer it to a different person or object the question \
         asks about, and conversely do answer for the person who genuinely owns the \
         fact even if another person has a similar one. Do not add a \
         plausible-sounding detail that no memory states; if the only basis would be \
         a typical association rather than something a memory actually says for the \
         named subject, treat it as unsupported. When a question lists or asks \
         'what' things someone did or has, include every matching item the memories \
         provide, not just one. If the question assumes something the memories \
         contradict or never support (a fact, an action, or who did it), say so \
         plainly and stop there rather than substituting a different person's or \
         subject's fact.\n\
         Only reply that the memories contain no such information when, after \
         checking every memory, nothing states or implies an answer for the \
         specific person or value asked about - not by lookup, inference, or \
         combination. Answer concisely.";

    let mut user = String::from("Memories:\n");
    for (rank, hit) in hits.iter().enumerate() {
        user.push_str(&format!("{}. {}\n", rank + 1, hit.text));
    }
    user.push_str(&format!("\nQuestion: {question}"));

    vec![Message::system(system), Message::user(user)]
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
fn read_assembled(
    reader: &dyn LLMClient,
    pacer: &Pacer,
    question: &str,
    view: Vec<AtomHit>,
    recall_micros: u128,
) -> Result<AnswerOutcome> {
    let retrieved = view
        .iter()
        .filter_map(|h| {
            h.payload
                .get("dia_id")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .collect();
    let mut req = CompletionRequest::new(build_reader_prompt(&view, question));
    req.temperature = Some(0.0);
    req.max_tokens = Some(max_output_tokens());
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
    reader: &dyn LLMClient,
    pacer: &Pacer,
    eng: &MemoryEngine,
    region: &str,
    question: &str,
    config: BenchConfig,
) -> Result<AnswerOutcome> {
    let started = Instant::now();
    let hits = eng.recall(
        region,
        RecallProfile::default().apply(RecallQuery::by_text(question, config.top_k)),
    )?;
    let view = reader_view(eng, region, hits, config)?;
    let recall_micros = started.elapsed().as_micros();
    read_assembled(reader, pacer, question, view, recall_micros)
}

/// LLM-as-judge correctness with Mem0's generous LoCoMo rubric (same topic = CORRECT,
/// tolerant of length/phrasing/date-format), binary. Returns `(correct, judge_usage)`.
pub fn judge_correct(
    judge: &dyn LLMClient,
    pacer: &Pacer,
    question: &str,
    gold: &str,
    predicted: &str,
) -> Result<(bool, TokenUsage)> {
    let system = "Your task is to label an answer to a question as CORRECT or WRONG. \
         You are given (1) a question one user asked about another user, (2) a gold \
         (ground-truth) answer, and (3) a generated answer to score.\n\
         The gold answer is usually concise and names the referenced topic. The \
         generated answer may be much longer; be GENEROUS - as long as it touches on \
         the same topic as the gold answer, count it CORRECT.\n\
         For time-related questions the gold answer is a specific date/month/year. The \
         generated answer may be longer or use relative references; be generous - if \
         it refers to the same date or time period as the gold answer, count it \
         CORRECT. Even if the format differs (e.g. May 7th vs 7 May), it is CORRECT if \
         it is the same date.\n\
         First give a one-sentence explanation of your reasoning, then on a final line \
         output a JSON object with a single key \"label\" whose value is exactly \
         CORRECT or WRONG, e.g. {\"label\": \"CORRECT\"}. Do not include both CORRECT \
         and WRONG anywhere in your reply.";
    let user = format!("Question: {question}\nGold answer: {gold}\nGenerated answer: {predicted}");
    let resp = complete_judge(judge, pacer, system, &user)?;
    let correct = judge_label(&resp.message.content);
    Ok((correct, resp.usage))
}

/// Adversarial questions: did the reader abstain rather than fabricate? `(abstained, usage)`.
pub fn judge_abstained(
    judge: &dyn LLMClient,
    pacer: &Pacer,
    question: &str,
    predicted: &str,
) -> Result<(bool, TokenUsage)> {
    let system = "You check whether an answer correctly indicates that the \
         information is unknown or not available, rather than fabricating a \
         specific answer. Reply with exactly CORRECT if it abstains, or WRONG if \
         it fabricates a specific answer.";
    let user = format!("Question: {question}\nPredicted answer: {predicted}");
    let resp = complete_judge(judge, pacer, system, &user)?;
    let abstained = starts_with_token(&resp.message.content, "CORRECT");
    Ok((abstained, resp.usage))
}

fn complete_judge(
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
    req.max_tokens = Some(max_output_tokens());
    paced_complete(pacer, judge, &req)
}

/// Parse the judge reply to a bool: prefer JSON `{"label": ...}`, else the last
/// non-empty line. Anchored on the final signal so the reasoning can't flip it.
fn judge_label(reply: &str) -> bool {
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
fn starts_with_token(reply: &str, token: &str) -> bool {
    match reply.trim().to_ascii_uppercase().strip_prefix(token) {
        Some(rest) => rest
            .chars()
            .next()
            .map_or(true, |c| !c.is_ascii_alphanumeric()),
        None => false,
    }
}
