//! Ingest one LongMemEval question's haystack, one atom per turn.

use std::sync::Arc;
use std::time::Instant;

use citadel_mem::{AtomId, AtomInput, Embedder, MemoryEngine};
use serde_json::json;

use super::dataset::{LmSample, LmTurn};
use crate::core::db::{attach_reused_region, validate_ingested_atoms};
use crate::core::error::Result;
use crate::core::progress::IngestEmbedder;

/// Prepare the same complete corpus for scored runs and retrieval diagnostics.
/// Progress is per region/batch, never per turn. The non-embedding remainder
/// includes formatting, validation, region setup, encryption and persistence.
pub fn prepare_regions(
    eng: &MemoryEngine,
    samples: &[LmSample],
    embedder: Arc<dyn Embedder>,
    encrypted: bool,
    reuse: bool,
) -> Result<()> {
    super::dataset::validate_samples(samples)?;
    let observed = Arc::new(IngestEmbedder::new(embedder));
    let started = Instant::now();
    let operation = if reuse { "validate reuse" } else { "ingest" };
    let n = samples.len();
    eprintln!("phase 1: {operation} {n} regions: started");
    for (index, sample) in samples.iter().enumerate() {
        let region_started = Instant::now();
        let embedding_before = observed.elapsed();
        eprintln!(
            "  {operation} region {}/{n} ({} turns): started",
            index + 1,
            sample.turns.len()
        );
        let region_embedder: Arc<dyn Embedder> = observed.clone();
        if reuse {
            attach_reused_region(eng, &sample.question_id, region_embedder, encrypted)?;
            validate_reuse(eng, &sample.question_id, sample)?;
        } else {
            if encrypted {
                eng.create_encrypted_region(&sample.question_id, region_embedder)?;
            } else {
                eng.create_region(&sample.question_id, region_embedder)?;
            }
            ingest_sample(eng, &sample.question_id, sample)?;
        }
        let elapsed = region_started.elapsed();
        let embedding = observed.elapsed().saturating_sub(embedding_before);
        eprintln!(
            "  {operation} region {}/{n}: finished in {:.1}s (embedding {:.1}s, other {:.1}s); phase elapsed {:.1}s",
            index + 1,
            elapsed.as_secs_f64(),
            embedding.as_secs_f64(),
            elapsed.saturating_sub(embedding).as_secs_f64(),
            started.elapsed().as_secs_f64()
        );
    }
    eprintln!(
        "phase 1: {operation} {n} regions: finished in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

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
