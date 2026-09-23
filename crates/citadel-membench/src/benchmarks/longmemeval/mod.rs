//! LongMemEval (ICLR 2025), emit-only: citadel produces `{question_id, hypothesis}`
//! predictions; the official Python scorer judges them.

pub mod config;
pub mod dataset;
pub mod ingest;
pub mod prompts;
pub mod retrieval;
pub mod run;

use citadel_mem::AtomHit;

use crate::core::benchmark::{Benchmark, ReaderPrompt};
use crate::core::error::Result;

pub use run::{run, LmevalConfig};

#[derive(Default)]
pub struct LongMemEval {
    temporal_glosses: bool,
}

impl LongMemEval {
    pub fn new(temporal_glosses: bool) -> Self {
        Self { temporal_glosses }
    }
}

impl Benchmark for LongMemEval {
    fn reader_source_text<'a>(&self, hit: &'a AtomHit) -> Result<std::borrow::Cow<'a, str>> {
        prompts::source_text(hit, self.temporal_glosses)
    }
    fn gold_id_key(&self) -> &str {
        "session_id"
    }

    fn reader_prompt(
        &self,
        hits: &[AtomHit],
        question: &str,
        current_date: &str,
    ) -> Result<ReaderPrompt> {
        prompts::render_reader_prompt_with_glosses(
            hits,
            question,
            current_date,
            self.temporal_glosses,
        )
    }

    fn known_flaws(&self) -> &str {
        prompts::KNOWN_FLAWS
    }
}
