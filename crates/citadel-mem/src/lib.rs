//! Encrypted-first agent memory engine: regions, atoms, edges, traces.

#[cfg(feature = "candle-embed")]
pub mod candle;
pub mod embed;
pub mod engine;
pub mod error;
mod fusion;
pub mod types;

#[cfg(feature = "candle-embed")]
pub use candle::{CandleConfig, CandleEmbedder, CrossEncoder, Pooling};
pub use citadel_sql::executor::{AnnIndexSource, AnnSegmentInfo};
pub use embed::{EmbedError, Embedder, EmbeddingMetric, MockEmbedder, MockReranker, Reranker};
pub use engine::{MemoryEngine, RegionId};
pub use error::{MemError, Result};
pub use types::{
    AtomAttestation, AtomHit, AtomId, AtomInput, AttestVerdict, Edge, EdgeKind, ErasureReceipt,
    EvictionPolicy, EvictionReport, EvolutionReport, FusionWeights, GraphExpand, KindDigest,
    RecallQuery, RerankStrategy, SlotErasure, SummaryReport,
};
