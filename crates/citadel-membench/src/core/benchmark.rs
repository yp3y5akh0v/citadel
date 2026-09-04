//! The contract a benchmark plugin implements; the engine drives recall + reader
//! through it. Scoring varies per benchmark and is each plugin's own concern.

use citadel_llm::Message;
use citadel_mem::AtomHit;

use crate::core::error::Result;

pub trait Benchmark: Sync {
    /// Atom-payload key holding a turn's gold/evidence id, joined against retrieved hits.
    fn gold_id_key(&self) -> &str;
    /// Category-blind reader messages from the retrieved hits, question, and the date the
    /// question was asked (the "current date" anchor; empty when the benchmark has none).
    fn reader_prompt(
        &self,
        hits: &[AtomHit],
        question: &str,
        current_date: &str,
    ) -> Result<Vec<Message>>;
    /// Documented weaknesses, surfaced in every report.
    fn known_flaws(&self) -> &str;
}
