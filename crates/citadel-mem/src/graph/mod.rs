//! Deterministic zero-LLM graph repair: audit, reweight, weave, activation.

mod activation;
mod audit;
mod reweight;
mod weave;

pub use activation::{
    activation_rerank_cached, activation_scores_cached, pin_into_view, DiffusionCache,
};
pub use audit::{audit_provenance, ProvenanceAudit, ProvenanceViolation, AUDIT_DEPTH_CAP};
pub use reweight::{reweight_turns_from_provenance, ReweightStats, WeightShape, REWEIGHT_REVISION};
pub use weave::{
    weave_similar_notes, WeaveStats, WEAVE_MAX_DISTANCE, WEAVE_NEIGHBORS, WEAVE_REVISION,
};
