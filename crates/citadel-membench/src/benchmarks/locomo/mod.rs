//! LoCoMo: the long-term-conversational-memory benchmark (ACL 2024). Loader,
//! per-turn ingest, reader prompt, and judge live here.

pub mod config;
pub mod dataset;
pub mod ingest;
pub mod prompts;

use citadel_llm::LLMClient;
use citadel_mem::AtomHit;

use crate::core::benchmark::{Benchmark, ReaderPrompt};
use crate::core::error::Result;
use crate::core::eval::JudgeOutcome;
use crate::core::ratelimit::Pacer;

/// The LoCoMo benchmark plugin.
pub struct Locomo {
    session_headers: bool,
    temporal_glosses: bool,
}

impl Locomo {
    pub(crate) fn new(session_headers: bool) -> Self {
        Self {
            session_headers,
            temporal_glosses: false,
        }
    }

    pub(crate) fn with_temporal_glosses(mut self, enabled: bool) -> Self {
        self.temporal_glosses = enabled;
        self
    }
}

impl Benchmark for Locomo {
    fn reader_source_text<'a>(&self, hit: &'a AtomHit) -> Result<std::borrow::Cow<'a, str>> {
        prompts::source_text(hit, self.temporal_glosses)
    }
    fn gold_id_key(&self) -> &str {
        "dia_id"
    }

    // LoCoMo dialogue lines carry their own dates; there is no separate current-date anchor.
    fn reader_prompt(
        &self,
        hits: &[AtomHit],
        question: &str,
        _current_date: &str,
    ) -> Result<ReaderPrompt> {
        Ok(ReaderPrompt {
            messages: prompts::build_reader_prompt_with_glosses(
                hits,
                question,
                self.session_headers,
                self.temporal_glosses,
            )?,
            atom_ids: hits.iter().map(|hit| hit.id).collect(),
        })
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
    ) -> Result<JudgeOutcome> {
        if scored || !gold.trim().is_empty() {
            prompts::judge_correct_observed(judge, pacer, question, gold, predicted)
        } else {
            prompts::judge_abstained_observed(judge, pacer, question, predicted)
        }
    }
}
