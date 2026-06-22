//! LoCoMo: the long-term-conversational-memory benchmark (ACL 2024). Loader,
//! per-turn ingest, reader prompt, and judge live here.

pub mod dataset;
pub mod ingest;
pub mod prompts;

use citadel_ai::{LLMClient, Message, TokenUsage};
use citadel_mem::AtomHit;

use crate::core::benchmark::Benchmark;
use crate::core::error::Result;
use crate::core::ratelimit::Pacer;

/// The LoCoMo benchmark plugin.
pub struct Locomo;

impl Benchmark for Locomo {
    fn gold_id_key(&self) -> &str {
        "dia_id"
    }

    // LoCoMo dialogue lines carry their own dates; there is no separate current-date anchor.
    fn reader_prompt(&self, hits: &[AtomHit], question: &str, _current_date: &str) -> Vec<Message> {
        prompts::build_reader_prompt(hits, question)
    }

    fn known_flaws(&self) -> &str {
        prompts::KNOWN_FLAWS
    }
}

impl Locomo {
    /// Score one answer in-process: correctness for scored categories, abstention
    /// otherwise, except a false-premise adversarial carrying a real gold (correctness).
    pub fn judge(
        &self,
        judge: &dyn LLMClient,
        pacer: &Pacer,
        scored: bool,
        question: &str,
        gold: &str,
        predicted: &str,
    ) -> Result<(bool, TokenUsage)> {
        if scored || !gold.trim().is_empty() {
            prompts::judge_correct(judge, pacer, question, gold, predicted)
        } else {
            prompts::judge_abstained(judge, pacer, question, predicted)
        }
    }
}
