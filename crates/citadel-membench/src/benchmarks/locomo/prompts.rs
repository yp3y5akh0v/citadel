//! LoCoMo reader prompt, judge rubrics, and evaluation metadata.

use citadel_llm::{LLMClient, Message, TokenUsage};
use citadel_mem::AtomHit;
use rustc_hash::FxHashMap;
use std::borrow::Cow;

use super::dataset::parse_locomo_datetime;
use crate::core::error::{BenchError, Result};
use crate::core::eval::{abstention_label, complete_judge, judge_label, JudgeOutcome};
use crate::core::ratelimit::Pacer;
use crate::core::temporal;

/// Annotate only the dialogue body, preserving its attribution and image metadata.
pub(crate) fn source_text(hit: &AtomHit, enabled: bool) -> Result<Cow<'_, str>> {
    if !enabled {
        return Ok(Cow::Borrowed(&hit.text));
    }
    let metadata = |key: &str| -> Result<&str> {
        hit.payload
            .get(key)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                BenchError::Dataset(format!(
                    "LoCoMo atom {} lacks string {key} metadata",
                    hit.id
                ))
            })
    };
    let date = metadata("date_time")?;
    if date.is_empty() {
        return Ok(Cow::Borrowed(&hit.text));
    }
    let anchor = parse_locomo_datetime(date).ok_or_else(|| {
        BenchError::Dataset(format!(
            "LoCoMo atom {} has an invalid session date",
            hit.id
        ))
    })?;
    let prefix = format!("[{date}] {}: ", metadata("speaker")?);
    let mut suffix = String::new();
    for (key, label) in [
        ("blip_caption", "shared a photo"),
        ("query", "image search"),
    ] {
        let value = metadata(key)?;
        if !value.is_empty() {
            suffix.push_str(&format!(" [{label}: {value}]"));
        }
    }
    let body = hit
        .text
        .strip_prefix(&prefix)
        .and_then(|s| s.strip_suffix(&suffix))
        .ok_or_else(|| {
            BenchError::Dataset(format!(
                "LoCoMo atom {} text conflicts with its source metadata; rebuild its cached corpus",
                hit.id
            ))
        })?;
    match temporal::annotate(body, anchor) {
        Cow::Borrowed(_) => Ok(Cow::Borrowed(&hit.text)),
        Cow::Owned(body) => Ok(Cow::Owned(format!("{prefix}{body}{suffix}"))),
    }
}

/// Evaluation protocol and limitations included in each report.
pub(crate) const KNOWN_FLAWS: &str = "Accuracy uses an LLM correctness judge rather than \
     token-F1. Comparisons require matching reader and judge models, prompts, input \
     construction, and question sets. The reader receives retrieved turns and the \
     question, without gold answers or categories. Ingestion uses raw conversation \
     turns and supplied image descriptions, without LLM fact extraction. Recency \
     uses the wall clock; raw turns use default importance. Adversarial abstention \
     is reported separately from answer accuracy. Evidence coverage is measured \
     against dataset annotations, not answer accuracy. Hosted model outputs can \
     vary between runs. Token costs are estimates where model rates are known; \
     unknown rates remain unpriced.";

/// Build the reader prompt from only the hits + question, with one category-blind
/// system prompt (the gold category would be test-metadata leakage). Hits are
/// rendered verbatim in the order given (see [`reader_view`]); each turn's text
/// already carries its `[date] speaker:` prefix from ingest. The signature
/// isolates gold.
///
/// [`reader_view`]: crate::core::eval::reader_view
pub fn build_reader_prompt(
    hits: &[AtomHit],
    question: &str,
    session_headers: bool,
) -> Result<Vec<Message>> {
    build_reader_prompt_with_glosses(hits, question, session_headers, false)
}

pub(crate) fn build_reader_prompt_with_glosses(
    hits: &[AtomHit],
    question: &str,
    session_headers: bool,
    temporal_glosses: bool,
) -> Result<Vec<Message>> {
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
    let mut last_session = None;
    let mut session_dates = FxHashMap::default();
    for (rank, hit) in hits.iter().enumerate() {
        if session_headers {
            let session = hit
                .payload
                .get("session")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| {
                    BenchError::Dataset(format!(
                        "LoCoMo atom {} lacks numeric session metadata",
                        hit.id
                    ))
                })?;
            let date = hit
                .payload
                .get("date_time")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    BenchError::Dataset(format!(
                        "LoCoMo atom {} lacks string date_time metadata",
                        hit.id
                    ))
                })?;
            if let Some(previous) = session_dates.insert(session, date) {
                if previous != date {
                    return Err(BenchError::Dataset(format!(
                        "LoCoMo session {session} has conflicting dates"
                    )));
                }
            }
            if last_session != Some(session) {
                user.push_str(&format!("\n[Session {session} from {date}]\n"));
                last_session = Some(session);
            }
        }
        user.push_str(&format!(
            "{}. {}\n",
            rank + 1,
            source_text(hit, temporal_glosses)?
        ));
    }
    user.push_str(&format!("\nQuestion: {question}"));

    Ok(vec![Message::system(system), Message::user(user)])
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
    let outcome = judge_correct_observed(judge, pacer, question, gold, predicted)?;
    Ok((outcome.correct, outcome.usage))
}

pub(crate) fn judge_correct_observed(
    judge: &dyn LLMClient,
    pacer: &Pacer,
    question: &str,
    gold: &str,
    predicted: &str,
) -> Result<JudgeOutcome> {
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
    let (resp, audit) = complete_judge(judge, pacer, system, &user)?;
    let correct = judge_label(&resp).map_err(|error| error.with_completion_call(audit.clone()))?;
    Ok(JudgeOutcome::from_response(correct, resp, audit))
}

/// Adversarial questions: did the reader abstain rather than fabricate? `(abstained, usage)`.
pub fn judge_abstained(
    judge: &dyn LLMClient,
    pacer: &Pacer,
    question: &str,
    predicted: &str,
) -> Result<(bool, TokenUsage)> {
    let outcome = judge_abstained_observed(judge, pacer, question, predicted)?;
    Ok((outcome.correct, outcome.usage))
}

pub(crate) fn judge_abstained_observed(
    judge: &dyn LLMClient,
    pacer: &Pacer,
    question: &str,
    predicted: &str,
) -> Result<JudgeOutcome> {
    let system = "You check whether an answer correctly indicates that the \
         information is unknown or not available, rather than fabricating a \
         specific answer. Reply with exactly CORRECT if it abstains, or WRONG if \
         it fabricates a specific answer.";
    let user = format!("Question: {question}\nPredicted answer: {predicted}");
    let (resp, audit) = complete_judge(judge, pacer, system, &user)?;
    let abstained =
        abstention_label(&resp).map_err(|error| error.with_completion_call(audit.clone()))?;
    Ok(JudgeOutcome::from_response(abstained, resp, audit))
}

#[cfg(test)]
mod temporal_tests {
    use super::*;
    use crate::benchmarks::locomo::Locomo;
    use crate::core::{agentic, benchmark::Benchmark};
    use serde_json::json;

    fn hit() -> AtomHit {
        AtomHit {
            id: 7, kind: "turn".into(),
            text: "[1:00 pm on 1 March, 2024] Tomorrow: Yesterday I visited. [shared a photo: yesterday] [image search: tomorrow]".into(),
            payload: json!({"session": 1, "date_time": "1:00 pm on 1 March, 2024", "speaker": "Tomorrow", "blip_caption": "yesterday", "query": "tomorrow"}),
            importance: 0.0, confidence: 1.0, relevance: None, distance: None,
            graph_depth: None, created_at: 0, expires_at: None, immutable: false,
        }
    }

    #[test]
    fn glosses_preserve_attribution_media_and_original_atoms_in_both_reader_passes() {
        let hit = hit();
        let original = hit.text.clone();
        let expected = original.replace("Yesterday I", "Yesterday (29 February 2024) I");
        assert_eq!(source_text(&hit, true).unwrap(), expected);
        assert!(matches!(
            source_text(&hit, false).unwrap(),
            Cow::Borrowed(_)
        ));
        let hits = [hit];
        let bench = Locomo::new(true).with_temporal_glosses(true);
        let final_prompt = bench
            .reader_prompt(&hits, "What happened yesterday?", "")
            .unwrap();
        let extraction =
            agentic::extraction_prompt(&bench, &hits, "How many visits yesterday?", "").unwrap();
        for prompt in [final_prompt, extraction] {
            assert_eq!(prompt.atom_ids, [7]);
            assert!(prompt
                .messages
                .iter()
                .any(|m| matches!(m, Message::User(text) if text.contains(&expected))));
        }
        assert_eq!(hits[0].text, original);
        let raw = build_reader_prompt(&hits, "Q", true).unwrap();
        let disabled = Locomo::new(true).reader_prompt(&hits, "Q", "").unwrap();
        assert_eq!(format!("{raw:?}"), format!("{:?}", disabled.messages));
    }

    #[test]
    fn missing_dates_are_not_replaced_with_ingestion_time_and_bad_metadata_is_rejected() {
        let mut hit = hit();
        hit.payload["date_time"] = json!("");
        hit.text = "Tomorrow: Yesterday I visited.".into();
        assert_eq!(source_text(&hit, true).unwrap(), hit.text);
        hit.payload["date_time"] = json!("invalid");
        assert!(source_text(&hit, true).is_err());
        hit = self::hit();
        hit.payload["speaker"] = json!("Other");
        assert!(source_text(&hit, true).is_err());
        hit = self::hit();
        hit.payload["blip_caption"] = json!("mismatch");
        assert!(source_text(&hit, true).is_err());
        for key in ["blip_caption", "query"] {
            hit = self::hit();
            hit.payload.as_object_mut().unwrap().remove(key);
            assert!(source_text(&hit, true).is_err());
        }
    }
}
