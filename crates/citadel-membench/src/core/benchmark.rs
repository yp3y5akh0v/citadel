//! The contract a benchmark plugin implements; the engine drives recall + reader
//! through it. Scoring varies per benchmark and is each plugin's own concern.

use citadel_llm::Message;
use citadel_mem::{AtomHit, AtomId};
use std::borrow::Cow;

use crate::core::error::Result;

#[derive(Debug)]
pub struct ReaderPrompt {
    pub messages: Vec<Message>,
    /// Every input atom exactly once, in the order rendered in `messages`.
    pub atom_ids: Vec<AtomId>,
}

pub trait Benchmark: Sync {
    /// Source text shared by the final reader and optional item extraction.
    fn reader_source_text<'a>(&self, hit: &'a AtomHit) -> Result<Cow<'a, str>> {
        Ok(Cow::Borrowed(&hit.text))
    }
    /// Atom-payload key holding a turn's gold/evidence id, joined against retrieved hits.
    fn gold_id_key(&self) -> &str;
    /// Category-blind reader messages from the retrieved hits, question, and the date the
    /// question was asked (the "current date" anchor; empty when the benchmark has none).
    fn reader_prompt(
        &self,
        hits: &[AtomHit],
        question: &str,
        current_date: &str,
    ) -> Result<ReaderPrompt>;
    /// Documented weaknesses, surfaced in every report.
    fn known_flaws(&self) -> &str;
}
