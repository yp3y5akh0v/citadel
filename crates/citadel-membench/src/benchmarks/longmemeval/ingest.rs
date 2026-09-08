//! Ingest one LongMemEval question's haystack, one atom per turn.

use citadel_mem::{AtomId, AtomInput, MemoryEngine};
use serde_json::json;

use super::dataset::{LmSample, LmTurn};
use crate::core::db::validate_ingested_atoms;
use crate::core::error::Result;

pub fn turn_content(t: &LmTurn) -> String {
    if t.date.is_empty() {
        format!("{}: {}", t.role, t.content)
    } else {
        format!("[{}] {}: {}", t.date, t.role, t.content)
    }
}

fn atom_input(t: &LmTurn) -> AtomInput {
    let mut input = AtomInput::new("turn", turn_content(t)).with_payload(json!({
        "session_id": t.session_id,
        "session_occurrence": t.session_occurrence,
        "role": t.role,
        "has_answer": t.has_answer,
    }));
    if let Some(ev) = t.event_micros {
        input = input.with_created_at(ev);
    }
    input
}

pub fn ingest_sample(eng: &MemoryEngine, region: &str, sample: &LmSample) -> Result<Vec<AtomId>> {
    super::dataset::validate_samples(std::slice::from_ref(sample))?;
    let atoms = sample.turns.iter().map(atom_input).collect();
    Ok(eng.remember_batch(region, atoms)?)
}

pub fn validate_reuse(eng: &MemoryEngine, region: &str, sample: &LmSample) -> Result<()> {
    super::dataset::validate_samples(std::slice::from_ref(sample))?;
    validate_ingested_atoms(eng, region, sample.turns.iter().map(atom_input))
}
