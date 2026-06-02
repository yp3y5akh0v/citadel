//! Public data types for remember/recall.

use serde_json::Value as Json;

/// Stable identifier for a memory atom (globally unique across per-dim tables).
pub type AtomId = i64;

/// Input to [`remember`](crate::MemoryEngine::remember): `text` is embedded, `payload` stored as JSONB.
#[derive(Debug, Clone)]
pub struct AtomInput {
    pub kind: String,
    pub text: String,
    pub payload: Json,
    pub score: f32,
    pub confidence: f32,
    pub expires_at: Option<i64>,
    /// Protected from eviction (except `PurgeRegion`).
    pub immutable: bool,
}

impl AtomInput {
    pub fn new(kind: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            text: text.into(),
            payload: Json::Null,
            score: 0.0,
            confidence: 1.0,
            expires_at: None,
            immutable: false,
        }
    }

    pub fn with_payload(mut self, payload: Json) -> Self {
        self.payload = payload;
        self
    }

    pub fn with_score(mut self, score: f32) -> Self {
        self.score = score;
        self
    }

    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence;
        self
    }

    pub fn with_expires_at(mut self, micros: i64) -> Self {
        self.expires_at = Some(micros);
        self
    }

    pub fn immutable(mut self) -> Self {
        self.immutable = true;
        self
    }
}

/// Recall fusion weights (need not sum to 1); each signal is normalized to [0,1] first.
#[derive(Debug, Clone, Copy)]
pub struct FusionWeights {
    pub semantic: f32,
    pub keyword: f32,
    pub recency: f32,
    pub importance: f32,
}

impl Default for FusionWeights {
    fn default() -> Self {
        Self {
            semantic: 0.4,
            keyword: 0.25,
            recency: 0.2,
            importance: 0.15,
        }
    }
}

/// How a reranker combines with linear fusion.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RerankStrategy {
    /// Cross-encoder logit replaces the fusion score (discards keyword/recency).
    Replace,
    /// Reciprocal Rank Fusion of cross-encoder and fusion ranks; `k` is the damping
    /// constant (60 is standard).
    Rrf { k: f32 },
}

impl Default for RerankStrategy {
    fn default() -> Self {
        Self::Rrf { k: 60.0 }
    }
}

/// Relationship between two atoms; `DependsOn`/`Supersedes` are acyclic, the rest may cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    Causes,
    Contradicts,
    Refines,
    Precedes,
    Supersedes,
    DerivedFrom,
    DependsOn,
}

impl EdgeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EdgeKind::Causes => "causes",
            EdgeKind::Contradicts => "contradicts",
            EdgeKind::Refines => "refines",
            EdgeKind::Precedes => "precedes",
            EdgeKind::Supersedes => "supersedes",
            EdgeKind::DerivedFrom => "derived_from",
            EdgeKind::DependsOn => "depends_on",
        }
    }

    /// Whether `link` must reject cycles for this kind.
    pub(crate) fn is_acyclic(self) -> bool {
        matches!(self, EdgeKind::DependsOn | EdgeKind::Supersedes)
    }
}

/// A directed edge between two atoms, read back from `memory_edges`.
#[derive(Debug, Clone)]
pub struct Edge {
    pub src_id: AtomId,
    pub dst_id: AtomId,
    pub kind: EdgeKind,
    pub weight: f32,
    pub evidence_ref: Option<Json>,
}

/// Recall graph expansion: from each seed, walk `memory_edges` up to `depth` hops over `kinds`.
#[derive(Debug, Clone)]
pub struct GraphExpand {
    pub depth: usize,
    pub kinds: Vec<EdgeKind>,
}

impl GraphExpand {
    pub fn new(depth: usize, kinds: Vec<EdgeKind>) -> Self {
        Self { depth, kinds }
    }
}

/// A recall request: provide `text` (embedded + keyword-ranked) or an `embedding`.
#[derive(Debug, Clone)]
pub struct RecallQuery {
    pub text: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub kinds: Vec<String>,
    pub payload_filter: Option<Json>,
    pub k: usize,
    pub weights: FusionWeights,
    pub graph_expand: Option<GraphExpand>,
}

impl RecallQuery {
    pub fn by_text(text: impl Into<String>, k: usize) -> Self {
        Self {
            text: Some(text.into()),
            embedding: None,
            kinds: Vec::new(),
            payload_filter: None,
            k,
            weights: FusionWeights::default(),
            graph_expand: None,
        }
    }

    pub fn by_embedding(embedding: Vec<f32>, k: usize) -> Self {
        Self {
            text: None,
            embedding: Some(embedding),
            kinds: Vec::new(),
            payload_filter: None,
            k,
            weights: FusionWeights::default(),
            graph_expand: None,
        }
    }

    pub fn with_kinds(mut self, kinds: Vec<String>) -> Self {
        self.kinds = kinds;
        self
    }

    pub fn with_payload_filter(mut self, filter: Json) -> Self {
        self.payload_filter = Some(filter);
        self
    }

    pub fn with_weights(mut self, weights: FusionWeights) -> Self {
        self.weights = weights;
        self
    }

    pub fn with_graph_expand(mut self, expand: GraphExpand) -> Self {
        self.graph_expand = Some(expand);
        self
    }

    /// Attach the query text to a [`by_embedding`](Self::by_embedding) query so the
    /// keyword signal and cross-encoder reranker still run, without re-embedding.
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }
}

/// A recalled atom with its raw distance and fused ranking score.
#[derive(Debug, Clone)]
pub struct AtomHit {
    pub id: AtomId,
    pub kind: String,
    pub text: String,
    pub payload: Json,
    pub distance: f32,
    pub score: f32,
    /// Protected from eviction and in-place payload edits.
    pub immutable: bool,
}

/// Selective-forgetting policy; `immutable` atoms survive all but `PurgeRegion`.
#[derive(Debug, Clone)]
pub enum EvictionPolicy {
    /// Never-accessed atoms older than `older_than_micros`.
    Stale { older_than_micros: i64 },
    /// Drop least-recently-accessed atoms, keeping the top `keep_fraction` (0.0..=1.0).
    Lru { keep_fraction: f32 },
    /// Atoms below both score and confidence thresholds.
    LowScore {
        score_threshold: f32,
        confidence_threshold: f32,
    },
    /// Wipe the whole region (including immutable atoms; key-rotation prep).
    PurgeRegion,
    /// Atoms whose payload contains `predicate` (JSONB `@>`).
    PredicateMatch { predicate: Json },
}

#[derive(Debug, Clone, Copy)]
pub struct EvictionReport {
    pub removed: u64,
}

#[derive(Debug, Clone)]
pub struct EvolutionReport {
    pub links_added: usize,
    pub score: f32,
}

/// Per-kind structural digest of a region's atoms.
#[derive(Debug, Clone)]
pub struct KindDigest {
    pub kind: String,
    pub count: u64,
    pub earliest: i64,
    pub latest: i64,
    pub avg_score: f32,
    pub avg_confidence: f32,
}

#[derive(Debug, Clone)]
pub struct SummaryReport {
    pub total: u64,
    pub kinds: Vec<KindDigest>,
}
