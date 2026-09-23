//! Agentic reader for aggregation questions: extract items as JSON,
//! dedup/sort/count in code, then answer from the verified list. Routing keys
//! on the question text only (no dataset type labels); memory recall is
//! untouched, so "zero-LLM memory" stays intact. Scores from this path are
//! reported as a separate labeled number.

use citadel_llm::Message;
use citadel_mem::AtomHit;
use serde::Deserialize;
use std::borrow::Cow;

use crate::core::benchmark::{Benchmark, ReaderPrompt};
use crate::core::error::Result;

/// Question shapes that need multi-item aggregation: counting, ordering,
/// totaling. Conservative on purpose; a false negative just keeps the standard
/// path.
pub fn is_aggregation_question(text: &str) -> bool {
    let t = text.to_ascii_lowercase();
    t.contains("how many")
        || t.contains("how much")
        || t.contains("total number")
        || t.contains("in what order")
        || t.contains("order of")
        || t.contains("earliest to latest")
        || t.contains("latest to earliest")
        || t.contains("chronological")
        || t.contains("list all")
}

/// One extracted item; `date` is `YYYY/MM/DD` when the chats state it, else the
/// session date. `amount` only for spend/total questions.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractedItem {
    pub item: String,
    pub date: String,
    pub evidence: String,
    #[serde(deserialize_with = "required_amount")]
    pub amount: Option<f64>,
}

fn required_amount<'de, D: serde::Deserializer<'de>>(
    de: D,
) -> std::result::Result<Option<f64>, D::Error> {
    Option::<f64>::deserialize(de)
}

#[derive(Debug)]
pub enum ExtractionDecision {
    NotEnumeration,
    Items(Vec<ExtractedItem>),
}

#[derive(Debug, thiserror::Error)]
pub enum ExtractionParseError {
    #[error("expected exactly NOT_ENUMERATION or a complete JSON item array: {0}")]
    Json(#[from] serde_json::Error),
    #[error("item {index}: {reason}")]
    InvalidItem { index: usize, reason: &'static str },
    #[error("the extracted amount total is not finite")]
    InvalidTotal,
}

/// Pass 1: ask the reader to mine every distinct relevant item from the
/// retrieved memories as a JSON array. Memories render date-sorted and flat
/// (extraction wants a scannable list, not conversation flow).
pub fn extraction_messages(hits: &[AtomHit], question: &str, current_date: &str) -> Vec<Message> {
    extraction_from_sources(
        hits.iter()
            .map(|hit| (hit, Cow::Borrowed(hit.text.as_str())))
            .collect(),
        question,
        current_date,
    )
    .messages
}

pub(crate) fn extraction_prompt(
    bench: &dyn Benchmark,
    hits: &[AtomHit],
    question: &str,
    current_date: &str,
) -> Result<ReaderPrompt> {
    let sources = hits
        .iter()
        .map(|hit| Ok((hit, bench.reader_source_text(hit)?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(extraction_from_sources(sources, question, current_date))
}

fn extraction_from_sources(
    mut sorted: Vec<(&AtomHit, Cow<'_, str>)>,
    question: &str,
    current_date: &str,
) -> ReaderPrompt {
    sorted.sort_by_key(|(hit, _)| hit.created_at);
    let mut memories = String::new();
    let mut atom_ids = Vec::with_capacity(sorted.len());
    for (h, source) in sorted {
        atom_ids.push(h.id);
        memories.push_str(&format!("- {source}\n"));
    }
    let prompt = format!(
        "I will give you memories from past chats between you and a user, plus a \
         question. If the question is answered by ENUMERATING multiple distinct \
         real-world items (events, activities, purchases, visits...), extract EVERY \
         such item as a JSON array. Each element: {{\"item\": \"short name\", \"date\": \
         \"YYYY/MM/DD\", \"evidence\": \"short quote\", \"amount\": number-or-null}}. \
         Include all four fields and no extra fields. Names and evidence must be \
         nonempty. Use an empty date string if no date is available, and null for \
         an inapplicable amount. If no supported items can be extracted, output []. \
         Use the stated date when the text gives one, else the memory's bracketed date. \
         Merge repeated mentions of the SAME real-world item into one element. Include \
         only items the memories explicitly support; `amount` only for money/quantity \
         questions. If the question is instead answered by a single stored fact, a \
         stated quantity, or arithmetic between two facts or dates, output exactly \
         NOT_ENUMERATION. Output ONLY the JSON array or NOT_ENUMERATION.\n\n\
         Memories:\n{memories}\nCurrent Date: {current_date}\nQuestion: \
         {question}\nJSON:"
    );
    ReaderPrompt {
        messages: vec![Message::user(prompt)],
        atom_ids,
    }
}

/// Parse the complete extraction protocol without repairing or discarding output.
pub fn parse_extraction(
    reply: &str,
) -> std::result::Result<ExtractionDecision, ExtractionParseError> {
    let body = reply.trim();
    if body == "NOT_ENUMERATION" {
        return Ok(ExtractionDecision::NotEnumeration);
    }
    let items: Vec<ExtractedItem> = serde_json::from_str(body)?;
    for (index, item) in items.iter().enumerate() {
        let reason = if item.item.trim().is_empty() {
            Some("item name must be nonempty")
        } else if item.evidence.trim().is_empty() {
            Some("evidence must be nonempty")
        } else if !item.date.is_empty() && calendar_date(&item.date).is_none() {
            Some("date must be empty or a valid YYYY/MM/DD calendar date")
        } else if item.amount.is_some_and(|amount| !amount.is_finite()) {
            Some("amount must be finite")
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(ExtractionParseError::InvalidItem { index, reason });
        }
    }
    Ok(ExtractionDecision::Items(items))
}

fn calendar_date(date: &str) -> Option<(i64, i64, i64)> {
    let bytes = date.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'/'
        || bytes[7] != b'/'
        || bytes
            .iter()
            .enumerate()
            .any(|(i, b)| i != 4 && i != 7 && !b.is_ascii_digit())
    {
        return None;
    }
    let year = date[..4].parse().ok()?;
    let month = date[5..7].parse().ok()?;
    let day = date[8..].parse().ok()?;
    if year == 0 {
        return None;
    }
    crate::core::civil::days_from_civil(year, month, day)?;
    Some((year, month, day))
}

/// Dedup on (folded name, date), then date-sort (dated first, undated last).
/// Date is part of the key, so a repeated event on three dates stays three
/// items; only same-name same-date mentions merge.
pub fn dedup_and_sort(items: Vec<ExtractedItem>) -> Vec<ExtractedItem> {
    let mut seen = rustc_hash::FxHashSet::default();
    let mut kept: Vec<ExtractedItem> = Vec::with_capacity(items.len());
    for it in items {
        let norm: String = it
            .item
            .to_ascii_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if seen.insert((norm, it.date.clone())) {
            kept.push(it);
        }
    }
    kept.sort_by_key(|i| date_key(&i.date));
    kept
}

/// Calendar-date sort key; missing or invalid dates sort last.
fn date_key(date: &str) -> (i64, i64, i64) {
    calendar_date(date).unwrap_or((i64::MAX, 0, 0))
}

/// Pass 2 anchor, appended to the ordinary reader prompt: the full history
/// stays in view (a list-only prompt loses evidence). The list anchors
/// counting/ordering; the history stays the evidence.
pub fn anchor_message(
    items: &[ExtractedItem],
) -> std::result::Result<Message, ExtractionParseError> {
    if items.is_empty() {
        return Ok(Message::user(
            "No supported candidates were extracted from the history. This does not \
             establish that the answer or count is zero. Recheck the history for evidence \
             answering the question; if it is insufficient, say that you cannot determine \
             the answer from the available history.",
        ));
    }
    let mut list = String::new();
    for (i, it) in items.iter().enumerate() {
        list.push_str(&format!("{}. [{}] {}", i + 1, it.date, it.item));
        if let Some(a) = it.amount {
            list.push_str(&format!(" (amount: {a})"));
        }
        if !it.evidence.is_empty() {
            list.push_str(&format!(" - \"{}\"", it.evidence));
        }
        list.push('\n');
    }
    let total: f64 = items.iter().filter_map(|i| i.amount).sum();
    if !total.is_finite() {
        return Err(ExtractionParseError::InvalidTotal);
    }
    let amount_line = if items.iter().any(|i| i.amount.is_some()) {
        format!("Sum of listed amounts: {total}.\n")
    } else {
        String::new()
    };
    Ok(Message::user(format!(
        "Aid for counting and ordering: the reader extracted these candidates from \
         the SAME history chats above, then code de-duplicated and date-sorted them. \
         The candidates still require verification against that history:\n\n{list}\n\
         Candidate count: {}.\n{amount_line}Cross-check the list against the history; \
         drop candidates the question's constraints exclude and add anything the list \
         missed. Anchor any final count, order, or total on that verification, then \
         answer the question.",
        items.len()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detection_hits_aggregation_shapes_only() {
        assert!(is_aggregation_question("How many concerts did I attend?"));
        assert!(is_aggregation_question(
            "What is the order of the six museums I visited from earliest to latest?"
        ));
        assert!(is_aggregation_question("How much did I spend on gifts?"));
        assert!(!is_aggregation_question("What pet did I mention?"));
        assert!(!is_aggregation_question("When did I visit Tokyo?"));
    }

    #[test]
    fn extraction_distinguishes_explicit_routing_from_empty_and_nonempty_items() {
        assert!(matches!(
            parse_extraction(" \nNOT_ENUMERATION ").unwrap(),
            ExtractionDecision::NotEnumeration
        ));
        let ExtractionDecision::Items(empty) = parse_extraction(" [] ").unwrap() else {
            panic!("items")
        };
        assert!(empty.is_empty());
        let ExtractionDecision::Items(items) = parse_extraction(
            r#"[{"item":"museum","date":"2024/02/29","evidence":"visited a museum","amount":null}]"#,
        ).unwrap() else { panic!("items") };
        assert_eq!(items.len(), 1);
        assert!(items[0].amount.is_none());
    }

    #[test]
    fn extraction_rejects_surrounding_garbage_and_invalid_fields_without_filtering() {
        for text in [
            "not_enumeration",
            "NOT_ENUMERATION because...",
            "\"NOT_ENUMERATION\"",
            "before []",
            "[] after",
            "[] []",
            "```json\n[]\n```",
            "[",
            "] stray then [",
            r#"[{"item":"museum"}]"#,
            r#"[{"item":"museum","date":"","evidence":"visited"}]"#,
            r#"[{"item":"museum","date":"","evidence":"visited","amount":null,"extra":1}]"#,
            r#"[{"item":"museum","item":"other","date":"","evidence":"visited","amount":null}]"#,
            r#"[{"item":" \t ","date":"","evidence":"visited","amount":null}]"#,
            r#"[{"item":"museum","date":"","evidence":" ","amount":null}]"#,
            r#"[{"item":"museum","date":"2023/02/29","evidence":"visited","amount":null}]"#,
            r#"[{"item":"museum","date":"0000/01/01","evidence":"visited","amount":null}]"#,
            r#"[{"item":"museum","date":"2024/1/1","evidence":"visited","amount":null}]"#,
            r#"[{"item":"museum","date":"","evidence":"visited","amount":1e309}]"#,
        ] {
            assert!(parse_extraction(text).is_err(), "must reject {text}");
        }
    }

    #[test]
    fn empty_candidate_aid_does_not_assert_a_zero_answer() {
        let Message::User(text) = anchor_message(&[]).unwrap() else {
            panic!("user message")
        };
        assert!(text.contains("does not establish"));
        assert!(text.contains("insufficient"));
        assert!(!text.contains("Candidate count: 0"));
    }

    #[test]
    fn dedup_keys_on_name_and_date_and_sorts() {
        let mk = |item: &str, date: &str| ExtractedItem {
            item: item.into(),
            date: date.into(),
            evidence: String::new(),
            amount: None,
        };
        let out = dedup_and_sort(vec![
            mk("Gym  Visit", "2023/02/20"),
            mk("gym visit", "2023/02/20"),
            mk("gym visit", "2023/02/21"),
            mk("Science Museum", "2023/01/15"),
            mk("Undated Fair", ""),
        ]);
        // Same name + same date merges; a new date is a distinct event
        // (repeated-event counts must not collapse).
        assert_eq!(out.len(), 4);
        assert_eq!(out[0].item, "Science Museum");
        assert_eq!(out[1].date, "2023/02/20");
        assert_eq!(out[2].date, "2023/02/21", "repeat event kept");
        assert_eq!(out[3].item, "Undated Fair", "undated sorts last");
    }

    #[test]
    fn anchor_carries_count_and_sum() {
        let items = vec![
            ExtractedItem {
                item: "gift A".into(),
                date: "2023/01/01".into(),
                evidence: "bought A".into(),
                amount: Some(20.0),
            },
            ExtractedItem {
                item: "gift B".into(),
                date: "2023/02/01".into(),
                evidence: String::new(),
                amount: Some(22.5),
            },
        ];
        let Message::User(text) = &anchor_message(&items).unwrap() else {
            panic!("expected user message");
        };
        assert!(text.contains("Candidate count: 2."));
        assert!(text.contains("Sum of listed amounts: 42.5."));
        assert!(text.contains("[2023/01/01] gift A (amount: 20)"));
    }

    #[test]
    fn amount_total_is_validated_after_the_actual_deduplication_and_sort() {
        let item = |name: &str, day: u8, amount: f64| ExtractedItem {
            item: name.into(),
            date: format!("2024/01/{day:02}"),
            evidence: name.into(),
            amount: Some(amount),
        };
        let dedup_overflow = vec![
            item("A", 1, 1e308),
            item("D", 1, -1e308),
            item("D", 1, -1e308),
            item("B", 1, 1e308),
            item("C", 1, 1e308),
        ];
        let sort_overflow = vec![
            item("A", 1, 1e308),
            item("B", 3, -1e308),
            item("C", 2, 1e308),
        ];
        for input in [dedup_overflow, sort_overflow] {
            assert!(input
                .iter()
                .filter_map(|i| i.amount)
                .sum::<f64>()
                .is_finite());
            assert!(matches!(
                anchor_message(&dedup_and_sort(input)),
                Err(ExtractionParseError::InvalidTotal)
            ));
        }
        let finite_normalized = vec![
            item("A", 1, 1e308),
            item("B", 3, 1e308),
            item("C", 2, -1e308),
        ];
        assert!(!finite_normalized
            .iter()
            .filter_map(|i| i.amount)
            .sum::<f64>()
            .is_finite());
        assert!(anchor_message(&dedup_and_sort(finite_normalized)).is_ok());
    }
}
