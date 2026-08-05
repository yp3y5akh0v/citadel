//! LongMemEval reader prompt and documented flaws (no Rust judge; scored
//! externally).

use citadel_llm::Message;
use citadel_mem::{AtomHit, AtomId};
use rustc_hash::FxHashMap;

pub(crate) const KNOWN_FLAWS: &str = "Emit-only harness: citadel produces a JSONL \
     hypothesis file (question_id + hypothesis per line); the official score comes from \
     the repo's evaluate_qa.py (gpt-4o-2024-08-06 judge, per-question-type prompts) then \
     print_qa_metrics.py, NOT from citadel. The reader model is a chosen component and \
     MUST be named with any number; the comparable like-for-like field uses a gpt-4o \
     reader (~82-86), while vendor 94-95 figures use stronger readers. The reader \
     replicates the official run_generation.py CoT prompt (generic, category-blind) with \
     Current Date = question_date; recall uses the scored RecallProfile default (no \
     as-of). Gold is dual: session-level \
     (answer_session_ids) and turn-level (has_answer); abstention (_abs) questions are \
     scored by the official judge for correct refusal.";

/// One session block: `(created_at sort key, session date, turns as (atom id,
/// body))`.
type SessionBlock = (i64, String, Vec<(AtomId, String)>);

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
/// history): turns regrouped into `### Session N` blocks ordered by date,
/// turns within a session in conversation order (ascending atom id).
pub fn build_reader_prompt(hits: &[AtomHit], question: &str, current_date: &str) -> Vec<Message> {
    let mut by_sid: FxHashMap<&str, usize> = FxHashMap::default();
    let mut sessions: Vec<SessionBlock> = Vec::new();
    for hit in hits {
        let sid = hit
            .payload
            .get("session_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let (date, body) = split_turn(&hit.text);
        let gi = *by_sid.entry(sid).or_insert_with(|| {
            sessions.push((hit.created_at, date.to_string(), Vec::new()));
            sessions.len() - 1
        });
        sessions[gi].2.push((hit.id, body.to_string()));
    }
    sessions.sort_by_key(|s| s.0);

    let mut history = String::new();
    for (i, (_, date, turns)) in sessions.iter_mut().enumerate() {
        // Relevance order shuffles dialogue; restore conversation flow so
        // anaphora ("I also bought one more") lands after its antecedent.
        turns.sort_unstable_by_key(|(id, _)| *id);
        history.push_str(&format!(
            "\n### Session {}:\nSession Date: {}\nSession Content:\n",
            i + 1,
            date
        ));
        for (_, body) in turns.iter() {
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
    vec![Message::user(prompt)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hit(id: i64, text: &str, sid: &str, created_at: i64) -> AtomHit {
        AtomHit {
            id,
            kind: "turn".into(),
            text: text.into(),
            payload: json!({ "session_id": sid, "role": "user" }),
            distance: 0.0,
            score: 0.0,
            created_at,
            immutable: false,
        }
    }

    #[test]
    fn groups_turns_into_sessions_ordered_by_date() {
        // Retrieval order interleaves two sessions and shuffles s1's dialogue:
        // gamma (id 7) outranks alpha (id 5), but conversation order must
        // render alpha first.
        let hits = vec![
            hit(7, "[2023/06/01] assistant: gamma", "s1", 200),
            hit(3, "[2023/05/01] user: beta", "s2", 100),
            hit(5, "[2023/06/01] user: alpha", "s1", 200),
        ];
        let msg = build_reader_prompt(&hits, "who?", "2023/07/01");
        let Message::User(text) = &msg[0] else {
            panic!("expected a user message");
        };
        println!("----- PROMPT -----\n{text}\n------------------");
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
            !text.contains("[2023/06/01]"),
            "inline date moved to header"
        );
        assert!(text.contains("Question: who?") && text.contains("Current Date: 2023/07/01"));
    }
}
