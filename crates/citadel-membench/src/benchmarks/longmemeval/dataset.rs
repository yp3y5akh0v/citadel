//! LongMemEval loader (ICLR 2025): one question + its own session haystack per instance.

use std::fs;
use std::path::Path;

use serde_json::Value;

use crate::core::civil::datetime_micros;
use crate::core::error::{BenchError, Result};
use crate::core::hash::sha256_hex;

/// The six question types (abstention is a `_abs` id-suffix overlay, not a type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LmKind {
    SingleSessionUser,
    SingleSessionAssistant,
    SingleSessionPreference,
    MultiSession,
    TemporalReasoning,
    KnowledgeUpdate,
}

impl LmKind {
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "single-session-user" => LmKind::SingleSessionUser,
            "single-session-assistant" => LmKind::SingleSessionAssistant,
            "single-session-preference" => LmKind::SingleSessionPreference,
            "multi-session" => LmKind::MultiSession,
            "temporal-reasoning" => LmKind::TemporalReasoning,
            "knowledge-update" => LmKind::KnowledgeUpdate,
            other => {
                return Err(BenchError::Dataset(format!(
                    "unknown question_type '{other}'"
                )))
            }
        })
    }

    pub fn label(self) -> &'static str {
        match self {
            LmKind::SingleSessionUser => "single_session_user",
            LmKind::SingleSessionAssistant => "single_session_assistant",
            LmKind::SingleSessionPreference => "single_session_preference",
            LmKind::MultiSession => "multi_session",
            LmKind::TemporalReasoning => "temporal_reasoning",
            LmKind::KnowledgeUpdate => "knowledge_update",
        }
    }
}

/// One turn; `session_id` is the session-level gold join key, shared by the session's turns.
#[derive(Debug, Clone)]
pub struct LmTurn {
    pub session_id: String,
    /// Position in `haystack_sessions`, independent of the official session ID.
    pub session_occurrence: usize,
    pub role: String,
    pub content: String,
    pub date: String,
    pub has_answer: bool,
    pub event_micros: Option<i64>,
}

/// One question with its private haystack. `gold` is the answer, or the explanation of
/// unanswerability for an `_abs` question.
#[derive(Debug, Clone)]
pub struct LmSample {
    pub question_id: String,
    pub question: String,
    pub question_date: String,
    pub gold: String,
    pub kind: LmKind,
    pub abstention: bool,
    pub turns: Vec<LmTurn>,
    pub evidence: Vec<String>,
}

pub fn load_with_hash(path: impl AsRef<Path>) -> Result<(Vec<LmSample>, String)> {
    let bytes = fs::read(path.as_ref())
        .map_err(|e| BenchError::Dataset(format!("read {}: {e}", path.as_ref().display())))?;
    let sha = sha256_hex(&bytes);
    let root: Value = serde_json::from_slice(&bytes)?;
    Ok((parse_root(&root)?, sha))
}

/// Empty date strings are unknown; nonempty question and session dates must be valid.
pub fn parse_root(root: &Value) -> Result<Vec<LmSample>> {
    let arr = root
        .as_array()
        .ok_or_else(|| BenchError::Dataset("top level must be a JSON array".into()))?;
    arr.iter().map(parse_sample).collect()
}

pub fn validate_samples(samples: &[LmSample]) -> Result<()> {
    for sample in samples {
        validate_question_date(&sample.question_date, &sample.question_id)?;
        for turn in &sample.turns {
            let parsed = parse_lmeval_datetime(&turn.date);
            if (!turn.date.is_empty() && parsed.is_none()) || parsed != turn.event_micros {
                return Err(BenchError::Dataset(format!(
                    "invalid or inconsistent haystack date at session occurrence {} in {}",
                    turn.session_occurrence, sample.question_id
                )));
            }
        }
    }
    Ok(())
}

fn parse_sample(v: &Value) -> Result<LmSample> {
    let obj = v
        .as_object()
        .ok_or_else(|| BenchError::Dataset("sample must be an object".into()))?;
    let question_id = str_field(obj, "question_id")?;
    let kind = LmKind::from_str(&str_field(obj, "question_type")?)?;
    let question = str_field(obj, "question")?;
    let question_date = str_field(obj, "question_date")?;
    validate_question_date(&question_date, &question_id)?;
    let gold = render_answer(obj.get("answer"));
    let abstention = question_id.ends_with("_abs");

    let session_ids = str_array(obj, "haystack_session_ids")?;
    let dates = str_array(obj, "haystack_dates")?;
    let sessions = obj
        .get("haystack_sessions")
        .and_then(Value::as_array)
        .ok_or_else(|| BenchError::Dataset("missing haystack_sessions array".into()))?;
    if session_ids.len() != dates.len() || session_ids.len() != sessions.len() {
        return Err(BenchError::Dataset(format!(
            "haystack_session_ids/haystack_dates/haystack_sessions length mismatch in {question_id}"
        )));
    }

    let mut turns = Vec::new();
    for (idx, session) in sessions.iter().enumerate() {
        let session_id = &session_ids[idx];
        let date = &dates[idx];
        let event_micros = parse_lmeval_datetime(date);
        if !date.is_empty() && event_micros.is_none() {
            return Err(BenchError::Dataset(format!(
                "invalid haystack date at session occurrence {idx} in {question_id}"
            )));
        }
        let session_turns = session
            .as_array()
            .ok_or_else(|| BenchError::Dataset("haystack session must be an array".into()))?;
        for turn in session_turns {
            let t = turn
                .as_object()
                .ok_or_else(|| BenchError::Dataset("turn must be an object".into()))?;
            turns.push(LmTurn {
                session_id: session_id.clone(),
                session_occurrence: idx,
                role: t
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                content: t
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                date: date.clone(),
                has_answer: t
                    .get("has_answer")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                event_micros,
            });
        }
    }

    let evidence = str_array(obj, "answer_session_ids")?;

    Ok(LmSample {
        question_id,
        question,
        question_date,
        gold,
        kind,
        abstention,
        turns,
        evidence,
    })
}

fn str_field(obj: &serde_json::Map<String, Value>, key: &str) -> Result<String> {
    obj.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| BenchError::Dataset(format!("missing string field '{key}'")))
}

fn validate_question_date(date: &str, question_id: &str) -> Result<()> {
    if !date.is_empty() && parse_lmeval_datetime(date).is_none() {
        return Err(BenchError::Dataset(format!(
            "invalid question_date in {question_id}: {date:?}"
        )));
    }
    Ok(())
}

fn str_array(obj: &serde_json::Map<String, Value>, key: &str) -> Result<Vec<String>> {
    obj.get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| BenchError::Dataset(format!("missing array field '{key}'")))?
        .iter()
        .map(|e| {
            e.as_str()
                .map(str::to_string)
                .ok_or_else(|| BenchError::Dataset(format!("'{key}' must contain strings")))
        })
        .collect()
}

/// Render a possibly-non-string `answer` (the cleaned split has a few int answers).
fn render_answer(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

/// Parse LongMemEval's strict `%Y/%m/%d (%a) %H:%M` stamp to epoch micros (`None` if off-format).
pub(super) fn parse_lmeval_datetime(s: &str) -> Option<i64> {
    let mut parts = s.split_whitespace();
    let date = parts.next()?;
    let weekday = parts.next()?;
    let time = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let mut d = date.split('/');
    let year = d.next()?.parse::<i64>().ok()?;
    let month = d.next()?.parse::<i64>().ok()?;
    let day = d.next()?.parse::<i64>().ok()?;
    if d.next().is_some() {
        return None;
    }

    let (h, m) = time.split_once(':')?;
    let (hour, min) = (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?);
    let micros = datetime_micros(year, month, day, hour, min)?;
    const WEEKDAYS: [&str; 7] = [
        "(Mon)", "(Tue)", "(Wed)", "(Thu)", "(Fri)", "(Sat)", "(Sun)",
    ];
    let days = micros.div_euclid(86_400_000_000);
    let weekday_index = (days + 3).rem_euclid(7) as usize;
    (weekday == WEEKDAYS[weekday_index]).then_some(micros)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_lmeval_stamp() {
        // 2023-04-10T23:07:00Z = 1_681_168_020 epoch seconds.
        assert_eq!(
            parse_lmeval_datetime("2023/04/10 (Mon) 23:07"),
            Some(1_681_168_020 * 1_000_000)
        );
        assert_eq!(parse_lmeval_datetime("1970/01/01 (Thu) 00:00"), Some(0));
    }

    #[test]
    fn deviating_stamps_return_none() {
        assert_eq!(parse_lmeval_datetime(""), None);
        assert_eq!(parse_lmeval_datetime("2023-04-10 23:07"), None);
        assert_eq!(parse_lmeval_datetime("2023/13/01 (Mon) 10:00"), None);
        assert_eq!(parse_lmeval_datetime("2023/02/29 (Wed) 10:00"), None);
        assert_eq!(parse_lmeval_datetime("2023/04/10 (Mon) 24:00"), None);
        assert_eq!(parse_lmeval_datetime("2023/04/10 (Mon) 23:07 extra"), None);
        assert_eq!(parse_lmeval_datetime("2023/04/10 (Tue) 23:07"), None);
        assert_eq!(parse_lmeval_datetime("2023/04/10 (Foo) 23:07"), None);
        assert_eq!(parse_lmeval_datetime("2023/04/10 Mon 23:07"), None);
        assert_eq!(parse_lmeval_datetime("2023/04/10 (Mon) 23:60"), None);
        assert_eq!(parse_lmeval_datetime("2023/04/10 (Mon) 23:07:00"), None);
        assert_eq!(parse_lmeval_datetime("2023/04/10/01 (Mon) 23:07"), None);
    }

    #[test]
    fn datetime_range_is_checked_without_rejecting_valid_boundary_minutes() {
        for year in [i64::MIN, -1_000_000, 1_000_000, i64::MAX] {
            assert_eq!(
                parse_lmeval_datetime(&format!("{year}/01/01 (Mon) 00:00")),
                None
            );
        }
        assert_eq!(
            parse_lmeval_datetime("294247/01/10 (Sun) 04:00"),
            Some(9_223_372_036_800_000_000)
        );
        assert_eq!(parse_lmeval_datetime("294247/01/10 (Sun) 04:01"), None);
        assert_eq!(
            parse_lmeval_datetime("-290308/12/21 (Sun) 20:00"),
            Some(-9_223_372_036_800_000_000)
        );
        assert_eq!(parse_lmeval_datetime("-290308/12/21 (Sun) 19:59"), None);
        assert_eq!(
            parse_lmeval_datetime("-1/01/01 (Fri) 00:00"),
            Some(-62_198_755_200_000_000)
        );
    }

    #[test]
    fn question_dates_validate_even_without_turns_and_allow_explicit_unknown() {
        let mut root = serde_json::json!([{
            "question_id": "q", "question_type": "multi-session",
            "question": "q", "question_date": "", "answer": "a",
            "haystack_session_ids": [], "haystack_dates": [],
            "haystack_sessions": [], "answer_session_ids": []
        }]);
        assert!(parse_root(&root).is_ok());
        for date in [
            " ",
            "not a date",
            "2023/02/29 (Wed) 00:00",
            "2023/04/10 (Tue) 23:07",
            "1000000/01/01 (Sat) 00:00",
        ] {
            root[0]["question_date"] = serde_json::json!(date);
            assert!(parse_root(&root)
                .unwrap_err()
                .to_string()
                .contains("invalid question_date"));
        }
    }

    #[test]
    fn parses_a_minimal_record_with_abstention_and_gold() {
        let root = serde_json::json!([
            {
                "question_id": "q1_abs",
                "question_type": "temporal-reasoning",
                "question": "when did X happen?",
                "answer": "not answerable: never mentioned",
                "question_date": "2023/04/10 (Mon) 23:07",
                "haystack_session_ids": ["answer_a_1", "noans_b_2"],
                "haystack_dates": ["2023/04/01 (Sat) 09:00", "2023/04/05 (Wed) 18:00"],
                "haystack_sessions": [
                    [{"role": "user", "content": "hi", "has_answer": true},
                     {"role": "assistant", "content": "hello", "has_answer": false}],
                    [{"role": "user", "content": "filler"}]
                ],
                "answer_session_ids": ["answer_a_1"]
            }
        ]);
        let s = &parse_root(&root).unwrap()[0];
        assert_eq!(s.question_id, "q1_abs");
        assert!(s.abstention);
        assert_eq!(s.kind, LmKind::TemporalReasoning);
        assert_eq!(s.gold, "not answerable: never mentioned");
        assert_eq!(s.turns.len(), 3);
        assert_eq!(s.turns[0].session_id, "answer_a_1");
        assert_eq!(s.turns[0].session_occurrence, 0);
        assert_eq!(s.turns[1].session_occurrence, 0);
        assert!(s.turns[0].has_answer);
        assert!(!s.turns[1].has_answer);
        assert_eq!(s.turns[2].session_id, "noans_b_2");
        assert_eq!(s.turns[2].session_occurrence, 1);
        assert_eq!(s.evidence, vec!["answer_a_1".to_string()]);
        assert!(s.turns[0].event_micros.is_some());
    }
}
