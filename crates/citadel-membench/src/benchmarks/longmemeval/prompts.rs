//! LongMemEval reader prompt and documented flaws (no Rust judge; scored
//! externally).

use citadel_llm::Message;
use citadel_mem::{AtomHit, AtomId};
use rustc_hash::FxHashMap;

use super::dataset::parse_lmeval_datetime;
use crate::core::error::{BenchError, Result};

pub(crate) const KNOWN_FLAWS: &str = "Emit-only harness: citadel produces a JSONL \
     hypothesis file (question_id + hypothesis per line); the official score comes from \
     the repo's evaluate_qa.py (gpt-4o-2024-08-06 judge, per-question-type prompts) then \
     print_qa_metrics.py, NOT from citadel. The reader model is a chosen component and \
     MUST be named with any number; the headline uses a gpt-4o reader, while \
     gpt-4o-mini runs are lower-cost diagnostics and are not reader-matched. The reader \
     replicates the official run_generation.py CoT prompt (generic, category-blind) with \
     Current Date = question_date; recall uses the scored RecallProfile default (no \
     as-of). Gold is dual: session-level \
     (answer_session_ids) and turn-level (has_answer); abstention (_abs) questions are \
     scored by the official judge for correct refusal.";

struct SessionBlock {
    event_micros: Option<i64>,
    occurrence: u64,
    date: String,
    turns: Vec<(AtomId, String)>,
}

/// Split `"[date] role: content"` into `(date, "role: content")`.
fn split_turn(text: &str) -> (&str, &str) {
    if let Some(rest) = text.strip_prefix('[') {
        if let Some(end) = rest.find("] ") {
            return (&rest[..end], &rest[end + 2..]);
        }
    }
    ("", text)
}

/// Reader prompt matching the official `run_generation.py` (CoT + `nl`
/// history): session occurrences ordered by date and source position, with
/// undated occurrences first and turns in ascending atom-ID order.
pub fn build_reader_prompt(
    hits: &[AtomHit],
    question: &str,
    current_date: &str,
) -> Result<Vec<Message>> {
    let mut by_occurrence: FxHashMap<u64, usize> = FxHashMap::default();
    let mut sessions: Vec<SessionBlock> = Vec::new();
    for hit in hits {
        let occurrence = hit
            .payload
            .get("session_occurrence")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                BenchError::Dataset(format!(
                    "LongMemEval atom {} has missing or invalid session_occurrence; rebuild its cached corpus",
                    hit.id
                ))
            })?;
        let (date, body) = split_turn(&hit.text);
        let event_micros = parse_lmeval_datetime(date);
        if !date.is_empty() && event_micros.is_none() {
            return Err(BenchError::Dataset(format!(
                "LongMemEval atom {} has an invalid session date",
                hit.id
            )));
        }
        let gi = *by_occurrence.entry(occurrence).or_insert_with(|| {
            sessions.push(SessionBlock {
                event_micros,
                occurrence,
                date: date.to_string(),
                turns: Vec::new(),
            });
            sessions.len() - 1
        });
        sessions[gi].turns.push((hit.id, body.to_string()));
    }
    sessions.sort_unstable_by_key(|session| (session.event_micros, session.occurrence));

    let mut history = String::new();
    for (i, session) in sessions.iter_mut().enumerate() {
        session.turns.sort_unstable_by_key(|(id, _)| *id);
        history.push_str(&format!(
            "\n### Session {}:\nSession Date: {}\nSession Content:\n",
            i + 1,
            session.date
        ));
        for (_, body) in &session.turns {
            history.push_str(&format!("\n\n{body}"));
        }
        history.push('\n');
    }

    let prompt = format!(
        "I will give you several history chats between you and a user. Please answer the \
         question based on the relevant chat history. Answer the question step by step: \
         first extract all the relevant information, and then reason over the information \
         to get the answer.\n\n\nHistory Chats:\n\n{history}\n\nCurrent Date: {current_date}\n\
         Question: {question}\nAnswer (step by step):"
    );
    Ok(vec![Message::user(prompt)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hit(id: i64, text: &str, sid: &str, occurrence: u64, created_at: i64) -> AtomHit {
        AtomHit {
            id,
            kind: "turn".into(),
            text: text.into(),
            payload: json!({ "session_id": sid, "session_occurrence": occurrence, "role": "user" }),
            importance: 0.0,
            confidence: 1.0,
            relevance: Some(0.0),
            distance: Some(0.0),
            graph_depth: None,
            created_at,
            expires_at: None,
            immutable: false,
        }
    }

    #[test]
    fn groups_turns_into_sessions_ordered_by_date() {
        // Retrieval order interleaves two sessions and shuffles s1's dialogue:
        // gamma (id 7) outranks alpha (id 5), but conversation order must
        // render alpha first.
        let hits = vec![
            hit(7, "[2023/06/01 (Thu) 09:00] assistant: gamma", "s1", 0, 200),
            hit(3, "[2023/05/01 (Mon) 09:00] user: beta", "s2", 1, 100),
            hit(5, "[2023/06/01 (Thu) 09:00] user: alpha", "s1", 0, 200),
        ];
        let msg = build_reader_prompt(&hits, "who?", "2023/07/01").unwrap();
        let Message::User(text) = &msg[0] else {
            panic!("expected a user message");
        };
        // Older session (s2, 05/01) renders before s1 (06/01).
        let p2 = text
            .find("Session Date: 2023/05/01")
            .expect("s2 date header");
        let p1 = text
            .find("Session Date: 2023/06/01")
            .expect("s1 date header");
        assert!(p2 < p1, "sessions ordered by date");
        // Within s1, conversation order (ascending atom id), not retrieval
        // order.
        let pa = text.find("user: alpha").expect("alpha present");
        let pg = text.find("assistant: gamma").expect("gamma present");
        assert!(pa < pg, "turns in conversation order within the session");
        // Two session blocks, no flat per-turn chats, date stripped into the
        // header.
        assert!(text.contains("### Session 1:") && text.contains("### Session 2:"));
        assert!(!text.contains("### Chat"), "no flat per-turn fragments");
        assert!(
            !text.contains("[2023/06/01 (Thu) 09:00]"),
            "inline date moved to header"
        );
        assert!(text.contains("Question: who?") && text.contains("Current Date: 2023/07/01"));

        let permuted = vec![hits[2].clone(), hits[0].clone(), hits[1].clone()];
        let permuted_msg = build_reader_prompt(&permuted, "who?", "2023/07/01").unwrap();
        let Message::User(permuted_text) = &permuted_msg[0] else {
            panic!("expected a user message");
        };
        assert_eq!(
            text, permuted_text,
            "retrieval-list order must not change a distinct-date LongMemEval prompt"
        );
    }

    #[test]
    fn missing_or_invalid_occurrence_requires_reingestion() {
        for occurrence in [None, Some(json!("0")), Some(json!(-1)), Some(json!(0.5))] {
            let mut atom = hit(1, "[2023/05/01] user: text", "repeated", 0, 100);
            let payload = atom.payload.as_object_mut().unwrap();
            if let Some(value) = occurrence {
                payload.insert("session_occurrence".into(), value);
            } else {
                payload.remove("session_occurrence");
            }
            let error = build_reader_prompt(&[atom], "what happened?", "2023/07/01").unwrap_err();
            assert!(matches!(error, BenchError::Dataset(_)));
            assert!(error
                .to_string()
                .contains("missing or invalid session_occurrence"));
        }
    }

    #[test]
    fn undated_occurrences_ignore_ingestion_and_retrieval_order() {
        let mut hits = vec![
            hit(1, "user: first", "shared", 0, 999),
            hit(2, "assistant: reply", "shared", 0, 888),
            hit(3, "user: second", "shared", 1, 1),
        ];
        let prompt = build_reader_prompt(&hits, "what?", "").unwrap();
        hits.reverse();
        let reversed = build_reader_prompt(&hits, "what?", "").unwrap();
        let (Message::User(text), Message::User(reversed_text)) = (&prompt[0], &reversed[0]) else {
            panic!("expected user messages");
        };
        assert_eq!(text, reversed_text);
        assert_eq!(text.matches("### Session ").count(), 2);
        assert!(text.find("user: first").unwrap() < text.find("user: second").unwrap());
        assert!(text.find("assistant: reply").unwrap() < text.find("user: second").unwrap());
    }
}
