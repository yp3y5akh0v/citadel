//! LoCoMo dataset loader: parses `locomo10.json` into typed samples.
//!
//! Session keys are dynamic (`session_1`, ...) interleaved with `*_date_time`/
//! `*_summary` siblings, so the conversation is parsed as `Value` and walked by key.

use std::fs;
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

use crate::error::{BenchError, Result};

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

/// One question/answer probe over a conversation.
#[derive(Debug, Clone)]
pub struct QaSample {
    pub question: String,
    pub gold: String,
    pub category: Category,
    pub evidence: Vec<String>,
}

/// A single LoCoMo conversation with its question set.
#[derive(Debug, Clone)]
pub struct Sample {
    pub sample_id: String,
    pub turns: Vec<Turn>,
    pub qa: Vec<QaSample>,
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

/// Lowercase-hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Parse an already-decoded LoCoMo root array (shared by `load` and tests).
pub fn parse_root(root: &Value) -> Result<Vec<Sample>> {
    let arr = root
        .as_array()
        .ok_or_else(|| BenchError::Dataset("top level must be a JSON array".into()))?;
    arr.iter().map(parse_sample).collect()
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
        let date_time = conversation
            .get(&format!("session_{session}_date_time"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let arr = val
            .as_array()
            .ok_or_else(|| BenchError::Dataset(format!("{key} must be an array of turns")))?;
        for turn in arr {
            turns.push(parse_turn(turn, session, &date_time)?);
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
        .unwrap_or("")
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
