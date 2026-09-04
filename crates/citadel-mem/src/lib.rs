//! Encrypted-first memory engine: regions, atoms, edges, traces.

#[cfg(feature = "candle-embed")]
pub mod candle;
pub mod embed;
pub mod engine;
pub mod error;
mod fusion;
pub mod graph;
mod plaintext;
pub mod profile;
mod read_limits;
pub mod types;

#[cfg(feature = "candle-embed")]
pub use candle::{CandleConfig, CandleEmbedder, CrossEncoder, Pooling};
pub use citadel_sql::executor::{AnnIndexSource, AnnSegmentInfo};
pub use embed::{EmbedError, Embedder, EmbeddingMetric, MockEmbedder, MockReranker, Reranker};
pub use engine::{
    owns_table, MemoryEngine, MemoryMaintenance, RegionId, DEFAULT_SUMMARY_KIND_LIMIT,
    MAX_DEPENDENT_FORGET_ATOMS, MAX_GRAPH_EXPANSION_EDGES, MAX_SUMMARY_KIND_LIMIT,
};
pub use error::{MemError, Result};
pub use graph::{
    activation_rerank_cached, activation_scores_cached, audit_provenance, pin_into_view,
    reweight_turns_from_provenance, weave_similar_notes, weave_similar_notes_replacing,
    DiffusionCache, ProvenanceAudit, ProvenanceViolation, ReweightStats, WeaveStats, WeightShape,
    AUDIT_DEPTH_CAP, REWEIGHT_REVISION, WEAVE_MAX_DISTANCE, WEAVE_NEIGHBORS, WEAVE_REVISION,
};
pub use profile::{RecallProfile, NARRATIVE_KINDS};
pub use read_limits::MemoryReadLimits;
pub use types::{
    AtomAttestation, AtomHit, AtomId, AtomInput, AttestVerdict, Edge, EdgeCursor, EdgeKind,
    EdgePage, ErasureReceipt, EvictionPolicy, EvictionReport, EvolutionReport, FetchPage,
    FetchQuery, FusionWeights, GraphExpand, KindDigest, MemoryProfileReport, MemoryRegionInfo,
    MemoryRegionInventory, MultiRecallQuery, PayloadUpdateOutcome, RecallQuery, ReembedReport,
    RememberOutcome, RerankStrategy, SlotErasure, SourceSnapshot, StoredAtomRetrievalState,
    StoredEmbeddingsIdentity, StoredRegionIdentity, SummaryQuery, SummaryReport,
    STORED_EMBEDDINGS_SCHEMA,
};
