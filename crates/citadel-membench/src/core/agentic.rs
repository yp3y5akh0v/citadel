//! Agentic reader for aggregation questions: extract items as JSON,
//! dedup/sort/count in code, then answer from the verified list. Routing keys
//! on the question text only (no dataset type labels); memory recall is
//! untouched, so "zero-LLM memory" stays intact. Scores from this path are
//! reported as a separate labeled number.

use citadel_ai::Message;
use citadel_mem::AtomHit;
use serde::Deserialize;

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
pub struct ExtractedItem {
    pub item: String,
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub evidence: String,
    #[serde(default)]
    pub amount: Option<f64>,
}

/// Pass 1: ask the reader to mine every distinct relevant item from the
/// retrieved memories as a JSON array. Memories render date-sorted and flat
/// (extraction wants a scannable list, not conversation flow).
pub fn extraction_messages(hits: &[AtomHit], question: &str, current_date: &str) -> Vec<Message> {
    let mut sorted: Vec<&AtomHit> = hits.iter().collect();
    sorted.sort_by_key(|h| h.created_at);
    let mut memories = String::new();
    for h in sorted {
        memories.push_str(&format!("- {}\n", h.text));
    }
    let prompt = format!(
        "I will give you memories from past chats between you and a user, plus a \
         question. If the question is answered by ENUMERATING multiple distinct \
         real-world items (events, activities, purchases, visits...), extract EVERY \
         such item as a JSON array. Each element: {{\"item\": \"short name\", \"date\": \
         \"YYYY/MM/DD\", \"evidence\": \"short quote\", \"amount\": number-or-null}}. \
         Use the stated date when the text gives one, else the memory's bracketed date. \
         Merge repeated mentions of the SAME real-world item into one element. Include \
         only items the memories explicitly support; `amount` only for money/quantity \
         questions. If the question is instead answered by a single stored fact, a \
         stated quantity, or arithmetic between two facts or dates, output exactly \
         NOT_ENUMERATION. Output ONLY the JSON array or NOT_ENUMERATION.\n\n\
         Memories:\n{memories}\nCurrent Date: {current_date}\nQuestion: \
         {question}\nJSON:"
    );
    vec![Message::user(prompt)]
}

/// Parse the extraction reply (tolerates a ```json fence). `None` = unusable;
/// caller falls back to the single-prompt reader.
pub fn parse_items(reply: &str) -> Option<Vec<ExtractedItem>> {
    let body = reply.trim();
    let body = body
        .strip_prefix("```json")
        .or_else(|| body.strip_prefix("```"))
        .map(|s| s.trim_end_matches("```"))
        .unwrap_or(body)
        .trim();
    let start = body.find('[')?;
    let end = body.rfind(']').filter(|&e| e > start)?;
    let items: Vec<ExtractedItem> = serde_json::from_str(&body[start..=end]).ok()?;
    let items: Vec<ExtractedItem> = items.into_iter().filter(|i| !i.item.is_empty()).collect();
    if items.is_empty() {
        None
    } else {
        Some(items)
    }
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

/// Sort key for a `YYYY/MM/DD...` prefix; unparsable dates sort last.
fn date_key(date: &str) -> (i64, i64, i64) {
    let mut parts = date.split(&['/', ' '][..]);
    let parse = |s: Option<&str>| s.and_then(|v| v.parse::<i64>().ok());
    match (
        parse(parts.next()),
        parse(parts.next()),
        parse(parts.next()),
    ) {
        (Some(y), Some(m), Some(d)) => (y, m, d),
        _ => (i64::MAX, 0, 0),
    }
}

/// Pass 2 anchor, appended to the ordinary reader prompt: the full history
/// stays in view (a list-only prompt loses evidence). The list anchors
/// counting/ordering; the history stays the evidence.
pub fn anchor_message(items: &[ExtractedItem]) -> Message {
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
    let amount_line = if items.iter().any(|i| i.amount.is_some()) {
        format!("Sum of listed amounts: {total}.\n")
    } else {
        String::new()
    };
    Message::user(format!(
        "Aid for counting and ordering: this de-duplicated, date-ordered candidate list \
         was extracted programmatically from the SAME history chats above:\n\n{list}\n\
         Candidate count: {}.\n{amount_line}Cross-check the list against the history; \
         drop candidates the question's constraints exclude and add anything the list \
         missed. Anchor any final count, order, or total on that verification, then \
         answer the question.",
        items.len()
    ))
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
    fn parse_tolerates_fence_and_rejects_garbage() {
        let fenced = "```json\n[{\"item\":\"Science Museum\",\"date\":\"2023/01/15\"}]\n```";
        assert_eq!(parse_items(fenced).unwrap().len(), 1);
        let prose = "Here are the items: [{\"item\":\"a\"},{\"item\":\"b\"}] as requested";
        assert_eq!(parse_items(prose).unwrap().len(), 2);
        assert!(parse_items("no json here").is_none());
        assert!(parse_items("[]").is_none());
        assert!(
            parse_items("NOT_ENUMERATION").is_none(),
            "self-classified non-enumeration falls back to the standard prompt"
        );
        assert!(
            parse_items("] stray then [ later").is_none(),
            "']' before '[' must not slice out of order"
        );
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
        let Message::User(text) = &anchor_message(&items) else {
            panic!("expected user message");
        };
        assert!(text.contains("Candidate count: 2."));
        assert!(text.contains("Sum of listed amounts: 42.5."));
        assert!(text.contains("[2023/01/01] gift A (amount: 20)"));
    }
}
