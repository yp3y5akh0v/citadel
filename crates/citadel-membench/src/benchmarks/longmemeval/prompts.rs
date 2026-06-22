//! LongMemEval reader prompt and documented flaws (no Rust judge; scored externally).

use citadel_ai::Message;
use citadel_mem::AtomHit;

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

/// Reader prompt replicating LongMemEval's official `run_generation.py` (the CoT variant):
/// a generic, category-blind chain-of-thought prompt over the retrieved chats (ordered by
/// date) plus the question's current date. No per-type tailoring - abstention is the
/// model's own call, exactly as in the reference generator.
pub fn build_reader_prompt(hits: &[AtomHit], question: &str, current_date: &str) -> Vec<Message> {
    // The official generator sorts the retrieved chats by date before prompting.
    let mut ordered: Vec<&AtomHit> = hits.iter().collect();
    ordered.sort_by_key(|h| h.created_at);

    let mut history = String::new();
    for (i, hit) in ordered.iter().enumerate() {
        history.push_str(&format!("\n### Chat {}:\n{}\n", i + 1, hit.text));
    }

    let prompt = format!(
        "I will give you several history chats between you and a user. Please answer the \
         question based on the relevant chat history. Answer the question step by step: \
         first extract all the relevant information, and then reason over the information \
         to get the answer.\n\n\nHistory Chats:\n{history}\n\nCurrent Date: {current_date}\n\
         Question: {question}\nAnswer (step by step):"
    );
    vec![Message::user(prompt)]
}
