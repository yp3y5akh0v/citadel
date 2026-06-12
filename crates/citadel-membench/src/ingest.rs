//! Ingest a LoCoMo conversation into a memory region, one atom per turn.

use citadel_mem::{AtomId, AtomInput, MemoryEngine};
use serde_json::json;

use crate::dataset::{Sample, Turn};
use crate::error::Result;

/// The exact text indexed for one turn, shared with the retrieval diagnostic so
/// measurements embed what the engine embeds. The session date and speaker are
/// folded in so they enter the vector + keyword index (dates ground temporal
/// questions), and the BLIP caption / image-search query (LoCoMo's image
/// stand-ins) stay retrievable.
pub fn turn_content(t: &Turn) -> String {
    let mut content = if t.date_time.is_empty() {
        format!("{}: {}", t.speaker, t.text)
    } else {
        format!("[{}] {}: {}", t.date_time, t.speaker, t.text)
    };
    if !t.blip_caption.is_empty() {
        content.push_str(&format!(" [shared a photo: {}]", t.blip_caption));
    }
    if !t.query.is_empty() {
        content.push_str(&format!(" [image search: {}]", t.query));
    }
    content
}

/// Store every turn of `sample` as a `kind="turn"` atom in `region`; the payload
/// carries session/date_time/speaker/dia_id so the harness can join hits against
/// gold evidence. Turns are written in conversation order (atom ids ascend
/// chronologically) with the session's parsed date as event-time `created_at`.
/// The caller must create `region` (bound to the embedder) first.
pub fn ingest_sample(eng: &MemoryEngine, region: &str, sample: &Sample) -> Result<Vec<AtomId>> {
    let atoms = sample
        .turns
        .iter()
        .map(|t| {
            let mut input = AtomInput::new("turn", turn_content(t)).with_payload(json!({
                "session": t.session,
                "date_time": t.date_time,
                "speaker": t.speaker,
                "dia_id": t.dia_id,
                "blip_caption": t.blip_caption,
                "query": t.query,
            }));
            if let Some(event) = t.event_micros() {
                input = input.with_created_at(event);
            }
            input
        })
        .collect();
    Ok(eng.remember_batch(region, atoms)?)
}
