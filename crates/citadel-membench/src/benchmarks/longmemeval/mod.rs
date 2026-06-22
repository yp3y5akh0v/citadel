//! LongMemEval (ICLR 2025), emit-only: citadel produces `{question_id, hypothesis}`
//! predictions; the official Python scorer judges them.

pub mod dataset;
pub mod ingest;
pub mod prompts;
pub mod retrieval;
pub mod run;

use citadel_ai::Message;
use citadel_mem::AtomHit;

use crate::core::benchmark::Benchmark;

pub use run::{run, LmevalConfig};

pub struct LongMemEval;

impl Benchmark for LongMemEval {
    fn gold_id_key(&self) -> &str {
        "session_id"
    }

    fn reader_prompt(&self, hits: &[AtomHit], question: &str, current_date: &str) -> Vec<Message> {
        prompts::build_reader_prompt(hits, question, current_date)
    }

    fn known_flaws(&self) -> &str {
        prompts::KNOWN_FLAWS
    }
}
