//! LoCoMo dataset loader: parses `locomo10.json` into typed samples.
//!
//! Session keys are dynamic (`session_1`, ...) interleaved with `*_date_time`/
//! `*_summary` siblings, so the conversation is parsed as `Value` and walked by key.

use std::fs;
use std::path::Path;

use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use serde_json::Value;

use crate::core::civil::datetime_micros;
use crate::core::error::{BenchError, Result};
use crate::core::hash::sha256_hex;

/// LoCoMo question category: 1=multi-hop, 2=temporal, 3=open-domain, 4=single-hop,
/// 5=adversarial. Adversarial has no answerable gold and is excluded from the headline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Category {
    MultiHop,
    Temporal,
    OpenDomain,
    SingleHop,
    Adversarial,
}

impl Category {
    /// Map the integer label as stored in `locomo10.json` (1..=5).
    pub fn from_int(n: u8) -> Result<Self> {
        match n {
            1 => Ok(Category::MultiHop),
            2 => Ok(Category::Temporal),
            3 => Ok(Category::OpenDomain),
            4 => Ok(Category::SingleHop),
            5 => Ok(Category::Adversarial),
            other => Err(BenchError::Dataset(format!(
                "category {other} out of range 1..=5"
            ))),
        }
    }

    /// Counts toward the headline accuracy (false for adversarial, scored by abstention).
    pub fn is_scored(self) -> bool {
        !matches!(self, Category::Adversarial)
    }

    /// The stable label used as the per-category key in the report.
    pub fn label(self) -> &'static str {
        match self {
            Category::MultiHop => "multi_hop",
            Category::Temporal => "temporal",
            Category::OpenDomain => "open_domain",
            Category::SingleHop => "single_hop",
            Category::Adversarial => "adversarial",
        }
    }
}

/// One dialogue turn within a session.
#[derive(Debug, Clone)]
pub struct Turn {
    pub session: u32,
    pub date_time: String,
    pub speaker: String,
    pub dia_id: String,
    pub text: String,
    /// BLIP caption of a shared photo (empty otherwise). LoCoMo substitutes the image
    /// with this caption, so it must be indexed or caption-evidence answers are lost.
    pub blip_caption: String,
    /// Image-search `query` that sourced the photo (empty otherwise). Some golds rest
    /// only on this (the caption is generic), so it is indexed alongside the caption.
    pub query: String,
}

impl Turn {
    /// Event time in micros since the epoch (UTC-naive), parsed from the session's
    /// `date_time` ("1:56 pm on 8 May, 2023"). Loaded samples have valid or empty
    /// dates; invalid or unrepresentable stamps return `None`.
    pub fn event_micros(&self) -> Option<i64> {
        parse_locomo_datetime(&self.date_time)
    }
}

/// One question/answer probe over a conversation.
#[derive(Debug, Clone)]
pub struct QaSample {
    pub question: String,
    pub gold: String,
    pub category: Category,
    pub evidence: Vec<String>,
}

impl QaSample {
    pub fn is_scorable(&self) -> bool {
        !self.category.is_scored() || !self.gold.trim().is_empty()
    }

    pub fn has_scored_evidence(&self) -> bool {
        self.category.is_scored() && self.is_scorable() && !self.evidence.is_empty()
    }
}

/// A single LoCoMo conversation with its question set.
#[derive(Debug, Clone)]
pub struct Sample {
    pub sample_id: String,
    pub turns: Vec<Turn>,
    pub qa: Vec<QaSample>,
}

impl Sample {
    /// The recency reference clock for this conversation's questions: one day after
    /// the last session (LoCoMo probes a finished conversation, so "now" is just
    /// past its end, not the bench's wall clock). `None` if no session date parses
    /// or adding a day would exceed the timestamp range.
    pub fn as_of_micros(&self) -> Option<i64> {
        const DAY_MICROS: i64 = 86_400 * 1_000_000;
        self.turns
            .iter()
            .filter_map(Turn::event_micros)
            .max()
            .and_then(|t| t.checked_add(DAY_MICROS))
    }
}

/// Parse `locomo10.json` (a JSON array of samples) from `path`.
pub fn load(path: impl AsRef<Path>) -> Result<Vec<Sample>> {
    Ok(load_with_hash(path)?.0)
}

/// Like [`load`], also returning the SHA-256 of the file bytes so a report pins the
/// exact dataset (a substituted file is detectable on re-run).
pub fn load_with_hash(path: impl AsRef<Path>) -> Result<(Vec<Sample>, String)> {
    let bytes = fs::read(path.as_ref())
        .map_err(|e| BenchError::Dataset(format!("read {}: {e}", path.as_ref().display())))?;
    let sha = sha256_hex(&bytes);
    let root: Value = serde_json::from_slice(&bytes)?;
    Ok((parse_root(&root)?, sha))
}

/// Parse an already-decoded LoCoMo root array (shared by `load` and tests).
/// Missing or empty session dates are unknown; present invalid dates are errors.
pub fn parse_root(root: &Value) -> Result<Vec<Sample>> {
    let arr = root
        .as_array()
        .ok_or_else(|| BenchError::Dataset("top level must be a JSON array".into()))?;
    let samples = arr.iter().map(parse_sample).collect::<Result<Vec<_>>>()?;
    validate_samples(&samples)?;
    Ok(samples)
}

pub fn validate_samples(samples: &[Sample]) -> Result<()> {
    let mut regions = FxHashSet::default();
    for sample in samples {
        if sample.sample_id.trim().is_empty()
            || !regions.insert(sample.sample_id.to_ascii_lowercase())
        {
            return Err(BenchError::Dataset(format!(
                "sample_id must be nonempty and unique ignoring ASCII case: {:?}",
                sample.sample_id
            )));
        }
        let mut turns = FxHashSet::default();
        let mut sessions = FxHashMap::default();
        for turn in &sample.turns {
            validate_date_time(&turn.date_time, &sample.sample_id, turn.session)?;
            if let Some(previous) = sessions.insert(turn.session, turn.date_time.as_str()) {
                if previous != turn.date_time {
                    return Err(BenchError::Dataset(format!(
                        "{}: session {} has conflicting date metadata",
                        sample.sample_id, turn.session
                    )));
                }
            }
            if turn.dia_id.trim().is_empty() || !turns.insert(turn.dia_id.as_str()) {
                return Err(BenchError::Dataset(format!(
                    "{}: dia_id must be nonempty and unique within its conversation: {:?}",
                    sample.sample_id, turn.dia_id
                )));
            }
        }
    }
    Ok(())
}

fn parse_sample(v: &Value) -> Result<Sample> {
    let obj = v
        .as_object()
        .ok_or_else(|| BenchError::Dataset("sample must be an object".into()))?;
    let sample_id = obj
        .get("sample_id")
        .and_then(Value::as_str)
        .ok_or_else(|| BenchError::Dataset("sample missing string sample_id".into()))?
        .to_string();
    let conversation = obj
        .get("conversation")
        .and_then(Value::as_object)
        .ok_or_else(|| BenchError::Dataset("sample missing conversation object".into()))?;

    let mut turns = Vec::new();
    for (key, val) in conversation {
        // A session key is exactly `session_<u32>`; `session_1_date_time` etc. must not match.
        let Some(session) = session_number(key) else {
            continue;
        };
        let date_key = format!("session_{session}_date_time");
        let date_time = match conversation.get(&date_key) {
            None => "",
            Some(value) => value.as_str().ok_or_else(|| {
                BenchError::Dataset(format!("{sample_id}: {date_key} must be a string"))
            })?,
        };
        validate_date_time(date_time, &sample_id, session)?;
        let arr = val
            .as_array()
            .ok_or_else(|| BenchError::Dataset(format!("{key} must be an array of turns")))?;
        for turn in arr {
            turns.push(parse_turn(turn, session, date_time)?);
        }
    }
    turns.sort_by_key(|t| t.session);

    let qa = match obj.get("qa") {
        Some(qa) => qa
            .as_array()
            .ok_or_else(|| BenchError::Dataset("qa must be an array".into()))?
            .iter()
            .map(parse_qa)
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };

    Ok(Sample {
        sample_id,
        turns,
        qa,
    })
}

/// `Some(n)` iff `key` is `session_<n>` with `<n>` a bare u32 (no further suffix).
fn session_number(key: &str) -> Option<u32> {
    key.strip_prefix("session_")?.parse::<u32>().ok()
}

fn validate_date_time(date: &str, sample_id: &str, session: u32) -> Result<()> {
    if !date.is_empty() && parse_locomo_datetime(date).is_none() {
        return Err(BenchError::Dataset(format!(
            "{sample_id}: invalid date at session {session}: {date:?}"
        )));
    }
    Ok(())
}

fn parse_turn(v: &Value, session: u32, date_time: &str) -> Result<Turn> {
    let obj = v
        .as_object()
        .ok_or_else(|| BenchError::Dataset("turn must be an object".into()))?;
    let speaker = obj
        .get("speaker")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let dia_id = obj
        .get("dia_id")
        .and_then(Value::as_str)
        .ok_or_else(|| BenchError::Dataset("turn missing string dia_id".into()))?
        .to_string();
    let text = obj
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let blip_caption = obj
        .get("blip_caption")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let query = obj
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Ok(Turn {
        session,
        date_time: date_time.to_string(),
        speaker,
        dia_id,
        text,
        blip_caption,
        query,
    })
}

fn parse_qa(v: &Value) -> Result<QaSample> {
    let obj = v
        .as_object()
        .ok_or_else(|| BenchError::Dataset("qa entry must be an object".into()))?;
    let question = obj
        .get("question")
        .and_then(Value::as_str)
        .ok_or_else(|| BenchError::Dataset("qa missing string question".into()))?
        .to_string();
    // Range-check before narrowing: `261u64 as u8 == 5` would slip through as Adversarial.
    let raw_category = obj
        .get("category")
        .and_then(Value::as_u64)
        .ok_or_else(|| BenchError::Dataset("qa missing integer category".into()))?;
    let category = u8::try_from(raw_category)
        .ok()
        .ok_or_else(|| BenchError::Dataset(format!("category {raw_category} out of range 1..=5")))
        .and_then(Category::from_int)?;
    // `answer` may be a string, number, or bool; render any scalar to a string.
    let gold = render_answer(obj.get("answer"));
    let evidence = obj
        .get("evidence")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|e| {
                    e.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| e.to_string())
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(QaSample {
        question,
        gold,
        category,
        evidence,
    })
}

/// Render a possibly-non-string `answer` into a plain string for the judge.
fn render_answer(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

/// Parse LoCoMo's `H:MM am|pm on D Month, YYYY` session stamp to epoch micros.
/// Strict on purpose: a deviating string returns `None` rather than a guess.
fn parse_locomo_datetime(s: &str) -> Option<i64> {
    let (clock, date) = s.split_once(" on ")?;
    let (hm, ampm) = clock.trim().rsplit_once(' ')?;
    let (h, m) = hm.split_once(':')?;
    let (h, m) = (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?);
    if !(1..=12).contains(&h) || !(0..=59).contains(&m) {
        return None;
    }
    let hour = match ampm {
        "am" => h % 12,
        "pm" => h % 12 + 12,
        _ => return None,
    };

    let (day_month, year) = date.trim().split_once(", ")?;
    let (day, month) = day_month.split_once(' ')?;
    let day = day.parse::<i64>().ok()?;
    let month = month_number(month)?;
    let year = year.parse::<i64>().ok()?;
    datetime_micros(year, month, day, hour, m)
}

fn month_number(name: &str) -> Option<i64> {
    const MONTHS: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    MONTHS.iter().position(|&m| m == name).map(|i| i as i64 + 1)
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    use serde_json::json;

    fn fixture() -> Value {
        json!([{
            "sample_id": "conversation",
            "conversation": {"session_1": [
                {"speaker": "A", "text": "one", "dia_id": "D1:1"},
                {"speaker": "B", "text": "two", "dia_id": "D1:2"}
            ]},
            "qa": [{"question": "q", "category": 1,
                "evidence": ["D1:1", "D1:1", "D-missing"]}]
        }])
    }

    #[test]
    fn one_session_cannot_have_conflicting_dates() {
        let mut samples = parse_root(&fixture()).unwrap();
        samples[0].turns[0].date_time = "9:00 am on 1 May, 2023".into();
        assert!(validate_samples(&samples)
            .unwrap_err()
            .to_string()
            .contains("conflicting date metadata"));
        samples[0].turns[1].date_time = samples[0].turns[0].date_time.clone();
        assert!(validate_samples(&samples).is_ok());
    }

    #[test]
    fn evidence_diagnostics_exclude_unscorable_and_adversarial_questions() {
        let mut qa = QaSample {
            question: "q".into(),
            gold: "answer".into(),
            category: Category::MultiHop,
            evidence: vec!["D1:1".into()],
        };
        assert!(qa.is_scorable());
        assert!(qa.has_scored_evidence());
        qa.gold = " \t".into();
        assert!(!qa.is_scorable());
        assert!(!qa.has_scored_evidence());
        qa.category = Category::Adversarial;
        assert!(qa.is_scorable());
        assert!(!qa.has_scored_evidence());
        qa.category = Category::OpenDomain;
        qa.gold = "answer".into();
        qa.evidence.clear();
        assert!(qa.is_scorable());
        assert!(!qa.has_scored_evidence());
    }

    #[test]
    fn region_identity_is_nonempty_and_case_insensitively_unique() {
        for id in ["", " \t"] {
            let mut root = fixture();
            root[0]["sample_id"] = json!(id);
            assert!(parse_root(&root).is_err());
        }
        let mut root = fixture();
        let mut repeated = root[0].clone();
        repeated["sample_id"] = json!("CONVERSATION");
        root.as_array_mut().unwrap().push(repeated);
        assert!(parse_root(&root).is_err());
    }

    #[test]
    fn missing_empty_or_duplicate_turn_ids_are_rejected() {
        for id in [Value::Null, json!(42), json!(""), json!(" "), json!("D1:1")] {
            let mut root = fixture();
            root[0]["conversation"]["session_1"][1]["dia_id"] = id;
            assert!(parse_root(&root).is_err());
        }
    }

    #[test]
    fn gold_annotations_are_preserved_including_duplicates_and_unknown_ids() {
        let samples = parse_root(&fixture()).unwrap();
        assert_eq!(samples[0].qa[0].evidence, ["D1:1", "D1:1", "D-missing"]);
    }

    #[test]
    fn missing_and_empty_dates_are_unknown_but_invalid_dates_fail() {
        let mut root = fixture();
        assert_eq!(parse_root(&root).unwrap()[0].turns[0].event_micros(), None);
        root[0]["conversation"]["session_1_date_time"] = json!("");
        assert_eq!(parse_root(&root).unwrap()[0].turns[0].event_micros(), None);
        for date in [
            json!(null),
            json!(42),
            json!(" "),
            json!("2pm on 1 Jan 2024"),
            json!("12:00 am on 1 January, 1000000"),
        ] {
            root[0]["conversation"]["session_1_date_time"] = date;
            assert!(parse_root(&root).is_err());
            let mut empty = root.clone();
            empty[0]["conversation"]["session_1"] = json!([]);
            assert!(parse_root(&empty).is_err());
        }
    }

    #[test]
    fn manually_constructed_invalid_dates_fail_validation() {
        let mut samples = parse_root(&fixture()).unwrap();
        samples[0].turns[0].date_time = "invalid".into();
        assert!(validate_samples(&samples).is_err());
    }

    #[test]
    fn recency_reference_addition_is_checked() {
        let mut sample = parse_root(&fixture()).unwrap().remove(0);
        sample.turns[0].date_time = "12:00 am on 1 January, 1970".into();
        assert_eq!(sample.as_of_micros(), Some(86_400_000_000));
        sample.turns[0].date_time = "4:00 am on 10 January, 294247".into();
        assert_eq!(sample.as_of_micros(), None);
    }
}

#[cfg(test)]
mod datetime_tests {
    use super::parse_locomo_datetime;

    #[test]
    fn parses_the_locomo_session_stamp() {
        // 2023-05-08T13:56:00Z = 1_683_554_160 epoch seconds.
        assert_eq!(
            parse_locomo_datetime("1:56 pm on 8 May, 2023"),
            Some(1_683_554_160 * 1_000_000)
        );
        // 2023-06-27T10:37:00Z = 1_687_862_220.
        assert_eq!(
            parse_locomo_datetime("10:37 am on 27 June, 2023"),
            Some(1_687_862_220 * 1_000_000)
        );
    }

    #[test]
    fn twelve_oclock_wraps_correctly() {
        assert_eq!(
            parse_locomo_datetime("12:00 am on 1 January, 1970"),
            Some(0)
        );
        assert_eq!(
            parse_locomo_datetime("12:30 pm on 1 January, 1970"),
            Some((12 * 3600 + 30 * 60) * 1_000_000)
        );
    }

    #[test]
    fn deviating_stamps_return_none() {
        assert_eq!(parse_locomo_datetime(""), None);
        assert_eq!(parse_locomo_datetime("2pm on 1 Jan 2024"), None);
        assert_eq!(parse_locomo_datetime("13:56 pm on 8 May, 2023"), None);
        assert_eq!(parse_locomo_datetime("1:56 pm on 8 Floreal, 2023"), None);
        assert_eq!(parse_locomo_datetime("1:56 pm on 8 May, 2023 extra"), None);
        assert_eq!(parse_locomo_datetime("1:56 pm on 8 May 2023"), None);
        assert_eq!(parse_locomo_datetime("1:56 pm on 8,May,,2023"), None);
    }

    #[test]
    fn impossible_civil_dates_return_none_not_a_rollover() {
        assert_eq!(parse_locomo_datetime("12:00 am on 31 February, 2023"), None);
        assert_eq!(parse_locomo_datetime("12:00 am on 29 February, 2023"), None);
        assert_eq!(parse_locomo_datetime("12:00 am on 31 April, 2023"), None);
        // 2024 is a leap year: 29 February is real.
        assert!(parse_locomo_datetime("12:00 am on 29 February, 2024").is_some());
    }

    #[test]
    fn datetime_range_is_checked_without_rejecting_valid_boundary_minutes() {
        for year in [i64::MIN, -1_000_000, 1_000_000, i64::MAX] {
            assert_eq!(
                parse_locomo_datetime(&format!("12:00 am on 1 January, {year}")),
                None
            );
        }
        assert_eq!(
            parse_locomo_datetime("4:00 am on 10 January, 294247"),
            Some(9_223_372_036_800_000_000)
        );
        assert_eq!(parse_locomo_datetime("4:01 am on 10 January, 294247"), None);
        assert_eq!(
            parse_locomo_datetime("8:00 pm on 21 December, -290308"),
            Some(-9_223_372_036_800_000_000)
        );
        assert_eq!(
            parse_locomo_datetime("7:59 pm on 21 December, -290308"),
            None
        );
        assert_eq!(
            parse_locomo_datetime("12:00 am on 1 January, -1"),
            Some(-62_198_755_200_000_000)
        );
    }
}
