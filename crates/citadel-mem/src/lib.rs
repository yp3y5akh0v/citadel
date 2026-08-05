//! Encrypted-first memory engine: regions, atoms, edges, traces.

#[cfg(feature = "candle-embed")]
pub mod candle;
pub mod embed;
pub mod engine;
pub mod error;
mod fusion;
pub mod graph;
pub mod profile;
pub mod types;

#[cfg(feature = "candle-embed")]
pub use candle::{CandleConfig, CandleEmbedder, CrossEncoder, Pooling};
pub use citadel_sql::executor::{AnnIndexSource, AnnSegmentInfo};
pub use embed::{EmbedError, Embedder, EmbeddingMetric, MockEmbedder, MockReranker, Reranker};
pub use engine::{MemoryEngine, RegionId};
pub use error::{MemError, Result};
pub use graph::{
    activation_rerank_cached, activation_scores_cached, audit_provenance, pin_into_view,
    reweight_turns_from_provenance, weave_similar_notes, DiffusionCache, ProvenanceAudit,
    ProvenanceViolation, ReweightStats, WeaveStats, WeightShape, AUDIT_DEPTH_CAP,
    REWEIGHT_REVISION, WEAVE_MAX_DISTANCE, WEAVE_NEIGHBORS, WEAVE_REVISION,
};
pub use profile::{RecallProfile, NARRATIVE_KINDS};
pub use types::{
    AtomAttestation, AtomHit, AtomId, AtomInput, AttestVerdict, Edge, EdgeKind, ErasureReceipt,
    EvictionPolicy, EvictionReport, EvolutionReport, FetchQuery, FusionWeights, GraphExpand,
    KindDigest, MultiRecallQuery, RecallQuery, RememberOutcome, RerankStrategy, SlotErasure,
    SourceSnapshot, StoredAtomRetrievalState, StoredEmbeddingsIdentity, StoredRegionIdentity,
    SummaryReport, STORED_EMBEDDINGS_SCHEMA,
};
