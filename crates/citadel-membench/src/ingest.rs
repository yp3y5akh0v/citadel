//! Ingest a LoCoMo conversation into a memory region, one atom per turn.

use citadel_mem::{AtomId, AtomInput, MemoryEngine};
use serde_json::json;

use crate::dataset::Sample;
use crate::error::Result;

/// Store every turn of `sample` as a `kind="turn"` atom in `region`; the payload
/// carries session/date_time/speaker/dia_id so the reader can cite them. The caller
/// must create `region` (bound to the embedder) first.
pub fn ingest_sample(eng: &MemoryEngine, region: &str, sample: &Sample) -> Result<Vec<AtomId>> {
    let atoms = sample
        .turns
        .iter()
        .map(|t| {
            // Speaker-prefix the text so the name enters the vector + keyword index,
            // and fold in the BLIP caption (LoCoMo's image stand-in) so it's retrievable.
            let mut content = format!("{}: {}", t.speaker, t.text);
            if !t.blip_caption.is_empty() {
                content.push_str(&format!(" [shared a photo: {}]", t.blip_caption));
            }
            // The image-search query names concepts the generic caption misses; index it too.
            if !t.query.is_empty() {
                content.push_str(&format!(" [image search: {}]", t.query));
            }
            AtomInput::new("turn", content).with_payload(json!({
                "session": t.session,
                "date_time": t.date_time,
                "speaker": t.speaker,
                "dia_id": t.dia_id,
                "blip_caption": t.blip_caption,
                "query": t.query,
            }))
        })
        .collect();
    Ok(eng.remember_batch(region, atoms)?)
}
