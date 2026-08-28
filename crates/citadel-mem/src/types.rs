//! Public data types for remember/recall.

use serde_json::Value as Json;

use crate::embed::EmbeddingMetric;

/// Stable identifier for a memory atom (globally unique across per-dim tables).
pub type AtomId = i64;

/// Persisted identity fields; construction validates encrypted rows' live RSK.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRegionIdentity {
    name: String,
    encrypted: bool,
    dim: u16,
    metric: EmbeddingMetric,
    model_id: String,
}

impl StoredRegionIdentity {
    pub(crate) fn new(
        name: String,
        encrypted: bool,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: String,
    ) -> Self {
        Self {
            name,
            encrypted,
            dim,
            metric,
            model_id,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn encrypted(&self) -> bool {
        self.encrypted
    }

    pub fn dim(&self) -> u16 {
        self.dim
    }

    pub fn metric(&self) -> EmbeddingMetric {
        self.metric
    }

    pub fn model_id(&self) -> &str {
        &self.model_id
    }
}

/// Content-free retrieval metadata; score keeps exact f32 bits (+0.0 vs -0.0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAtomRetrievalState {
    atom_id: AtomId,
    kind: String,
    score_bits: u32,
    expires_at: Option<i64>,
}

impl StoredAtomRetrievalState {
    pub(crate) fn new(
        atom_id: AtomId,
        kind: String,
        score_bits: u32,
        expires_at: Option<i64>,
    ) -> Self {
        Self {
            atom_id,
            kind,
            score_bits,
            expires_at,
        }
    }

    pub fn atom_id(&self) -> AtomId {
        self.atom_id
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn score_bits(&self) -> u32 {
        self.score_bits
    }

    pub fn expires_at(&self) -> Option<i64> {
        self.expires_at
    }
}

/// Canonical encoding identity; any encoding change requires a new schema string.
pub const STORED_EMBEDDINGS_SCHEMA: &str = "citadel-mem-stored-embeddings-v1";

/// Opaque SHA-256 proof of stored embeddings; raw vectors are never exposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEmbeddingsIdentity {
    schema: &'static str,
    region: String,
    kind: String,
    count: u64,
    dim: u32,
    sha256: String,
}

impl StoredEmbeddingsIdentity {
    pub(crate) fn new(region: String, kind: String, count: u64, dim: u32, sha256: String) -> Self {
        Self {
            schema: STORED_EMBEDDINGS_SCHEMA,
            region,
            kind,
            count,
            dim,
            sha256,
        }
    }

    pub fn schema(&self) -> &'static str {
        self.schema
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn dim(&self) -> u32 {
        self.dim
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

/// Input to [`remember`](crate::MemoryEngine::remember): `text` is embedded,
/// `payload` stored as JSONB.
#[derive(Debug, Clone)]
pub struct AtomInput {
    pub kind: String,
    pub text: String,
    pub payload: Json,
    pub score: f32,
    pub confidence: f32,
    /// Event time (micros): when the remembered fact happened, vs the ingest
    /// wall clock used when `None`. Drives the recency fusion signal and
    /// `Stale` eviction.
    pub created_at: Option<i64>,
    pub expires_at: Option<i64>,
    /// Protected from eviction (except `PurgeRegion`).
    pub immutable: bool,
    /// Vector to store instead of embedding `text`; keeps one vector space.
    pub embedding: Option<Vec<f32>>,
}

impl AtomInput {
    pub fn new(kind: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            text: text.into(),
            payload: Json::Null,
            score: 0.0,
            confidence: 1.0,
            created_at: None,
            expires_at: None,
            immutable: false,
            embedding: None,
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

    pub fn with_created_at(mut self, micros: i64) -> Self {
        self.created_at = Some(micros);
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

    /// Store `vector` rather than embedding `text`.
    pub fn with_embedding(mut self, vector: Vec<f32>) -> Self {
        self.embedding = Some(vector);
        self
    }
}

/// Recall fusion weights (need not sum to 1); each signal is normalized to
/// [0,1] first.
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
            semantic: 0.45,
            keyword: 0.2,
            recency: 0.2,
            importance: 0.15,
        }
    }
}

impl FusionWeights {
    /// Pure vector similarity - for immutable reference corpora, where recency
    /// and access patterns carry no signal.
    pub fn semantic_only() -> Self {
        Self {
            semantic: 1.0,
            keyword: 0.0,
            recency: 0.0,
            importance: 0.0,
        }
    }
}

/// How a reranker combines with linear fusion.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RerankStrategy {
    /// Cross-encoder logit replaces the fusion score (discards
    /// keyword/recency).
    Replace,
    /// Reciprocal Rank Fusion of cross-encoder and fusion ranks; `k` is the
    /// damping constant (60 is the literature standard; lower trusts top ranks
    /// more).
    Rrf { k: f32 },
}

impl Default for RerankStrategy {
    fn default() -> Self {
        Self::Rrf { k: 20.0 }
    }
}

/// Relationship between two atoms; `DependsOn`/`Supersedes` acyclic, rest
/// cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    Causes,
    Contradicts,
    Refines,
    Precedes,
    Supersedes,
    /// True provenance: the src atom was derived from the dst atom.
    DerivedFrom,
    DependsOn,
    /// Vector-neighbor similarity; distinct from DerivedFrom to keep provenance clean.
    SimilarTo,
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
            EdgeKind::SimilarTo => "similar_to",
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

/// Recall graph expansion: walk `memory_edges` up to `depth` hops over `kinds`.
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

/// A recall request: provide `text` (embedded + keyword-ranked) or an
/// `embedding`.
#[derive(Debug, Clone)]
pub struct RecallQuery {
    pub text: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub kinds: Vec<String>,
    pub payload_filter: Option<Json>,
    pub k: usize,
    pub weights: FusionWeights,
    /// Reference clock (micros) for the recency signal; `None` = wall clock.
    /// Lets a caller rank event-time atoms as of a past moment. Does not filter
    /// expiry.
    pub as_of_micros: Option<i64>,
    pub graph_expand: Option<GraphExpand>,
    /// Rank superseded atoms too; off by default so recall answers with current facts.
    pub include_superseded: bool,
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
            as_of_micros: None,
            graph_expand: None,
            include_superseded: false,
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
            as_of_micros: None,
            graph_expand: None,
            include_superseded: false,
        }
    }

    pub fn with_superseded(mut self, include: bool) -> Self {
        self.include_superseded = include;
        self
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

    pub fn with_as_of(mut self, micros: i64) -> Self {
        self.as_of_micros = Some(micros);
        self
    }

    pub fn with_graph_expand(mut self, expand: GraphExpand) -> Self {
        self.graph_expand = Some(expand);
        self
    }

    /// Attach the query text to a [`by_embedding`](Self::by_embedding) query so
    /// the keyword signal and cross-encoder reranker still run, without
    /// re-embedding.
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }
}

/// Multi-query recall: independent sub-queries, RRF merge, one optional rerank pass.
#[derive(Debug, Clone)]
pub struct MultiRecallQuery {
    pub queries: Vec<RecallQuery>,
    pub k: usize,
    /// One cross-encoder pass over the merged pool; `None` keeps the pure RRF order.
    pub rerank_query: Option<String>,
    /// RRF damping constant (60 is the literature standard; lower trusts top ranks).
    pub rrf_k: f32,
}

impl MultiRecallQuery {
    pub fn new(queries: Vec<RecallQuery>, k: usize) -> Self {
        Self {
            queries,
            k,
            rerank_query: None,
            rrf_k: 60.0,
        }
    }

    pub fn with_rerank_query(mut self, text: impl Into<String>) -> Self {
        self.rerank_query = Some(text.into());
        self
    }

    pub fn with_rrf_k(mut self, k: f32) -> Self {
        self.rrf_k = k;
        self
    }
}

/// Deterministic id-order listing; after_id resumes, [from, before) bounds time.
#[derive(Debug, Clone)]
pub struct FetchQuery {
    /// `None` lists every kind.
    pub kind: Option<String>,
    pub payload_filter: Option<Json>,
    pub after_id: Option<AtomId>,
    pub created_from: Option<i64>,
    pub created_before: Option<i64>,
    pub limit: usize,
    /// Take the newest `limit` rows; results stay id-ascending either way.
    pub newest: bool,
}

impl FetchQuery {
    pub fn new(limit: usize) -> Self {
        Self {
            kind: None,
            payload_filter: None,
            after_id: None,
            created_from: None,
            created_before: None,
            limit,
            newest: false,
        }
    }

    pub fn with_kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }

    pub fn with_payload_filter(mut self, filter: Json) -> Self {
        self.payload_filter = Some(filter);
        self
    }

    pub fn with_after_id(mut self, id: AtomId) -> Self {
        self.after_id = Some(id);
        self
    }

    pub fn with_created_from(mut self, micros: i64) -> Self {
        self.created_from = Some(micros);
        self
    }

    pub fn with_created_before(mut self, micros: i64) -> Self {
        self.created_before = Some(micros);
        self
    }

    /// Select the newest `limit` rows instead of the oldest.
    pub fn newest(mut self) -> Self {
        self.newest = true;
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
    /// Stored creation clock (micros): the event time when remembered with
    /// [`AtomInput::with_created_at`], else the ingest wall clock.
    pub created_at: i64,
    /// Protected from eviction and in-place payload edits.
    pub immutable: bool,
}

/// Selective-forgetting policy; `immutable` survives all but `PurgeRegion`.
///
/// `Stale`/`Lru` recency combines the persisted insert-time floor with
/// in-process read tracking (per engine); reads stay write-free, so access
/// history is per-engine-lifetime, not durable across reopens.
#[derive(Debug, Clone)]
pub enum EvictionPolicy {
    /// Atoms older than `older_than_micros` that no read has touched.
    Stale { older_than_micros: i64 },
    /// Drop least-recently-accessed atoms, keeping the top `keep_fraction`
    /// (0.0..=1.0).
    Lru { keep_fraction: f32 },
    /// Atoms whose `expires_at` TTL has lapsed (physical delete; encrypted
    /// regions get per-atom cryptographic erasure like every eviction).
    Expired,
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

/// One key slot proven destroyed (Live -> Tombstone, `new_gen` = `old_gen`+1).
#[derive(Debug, Clone)]
pub struct SlotErasure {
    pub slot: u32,
    pub atom_id: AtomId,
    pub old_gen: u64,
    pub new_gen: u64,
}

/// Scope caveat carried by every [`ErasureReceipt`].
pub const ERASURE_SCOPE_CAVEAT: &str =
    "Cryptographic erasure destroys the per-atom AES-256 key (AES-KW / RFC 3394 wrapped); the \
     sealed ciphertext becomes computationally unrecoverable (NIST SP 800-88 cryptographic erase). \
     This is logical-copy destruction, not physical-media destruction: storage wear-leveling may \
     retain stale physical copies, and any external backup, replica, or escrowed key is out of \
     scope. Plaintext regions are logically deleted only, not cryptographically erased.";

/// Outcome of re-embedding a region.
#[derive(Debug, Clone, Default)]
pub struct ReembedReport {
    /// Atoms given a new vector.
    pub atoms_migrated: u64,
    /// Model the region records afterwards.
    pub model_id: String,
    /// Whether the region's ANN index was rebuilt for the new vectors.
    ///
    /// When false, no persisted plaintext ANN was rebuilt. Plaintext recall
    /// falls back to an exact scan until `persist_ann_index` is called;
    /// encrypted recall lazily rebuilds its in-memory sealed ANN.
    pub ann_rebuilt: bool,
    /// Managed `SimilarTo` edges rebuilt from their persisted neighbor policy.
    /// Authored edges are untouched. Score thresholds and fusion weights may
    /// need recalibration for the new model's score distribution.
    pub similarity_edges_rewoven: u64,
    /// Managed or explicitly adopted edges no longer selected by their policy.
    pub similarity_edges_cleared: u64,
}

/// Result of [`forget_atoms`](crate::MemoryEngine::forget_atoms). On a
/// plaintext region `cryptographic_erasure` is false (logical delete only).
#[derive(Debug, Clone)]
pub struct ErasureReceipt {
    /// True only on an encrypted region (else a logical row delete).
    pub cryptographic_erasure: bool,
    pub rows_deleted: u64,
    /// Keys destroyed (`== slots_erased.len()`); may be fewer than ids
    /// requested.
    pub erased_count: u64,
    pub slots_erased: Vec<SlotErasure>,
    /// Ids skipped as immutable (when `force` is false).
    pub immutable_skipped: Vec<AtomId>,
    /// Key-wrap algorithm, or "" on a plaintext region.
    pub algorithm: &'static str,
    /// Wrapped-key size in bytes (0 on a plaintext region).
    pub wrapped_key_size: u32,
    /// Key store fsynced the erasure (encrypted only).
    pub fsync: bool,
    /// Each tombstone was read back and confirmed (encrypted only).
    pub readback_confirmed: bool,
    pub scope_caveat: &'static str,
}

/// One atom's integrity verdict from
/// [`verify_atoms`](crate::MemoryEngine::verify_atoms).
#[derive(Debug, Clone)]
pub struct AtomAttestation {
    pub atom_id: AtomId,
    pub verdict: AttestVerdict,
    /// Verdict came from an HMAC bound to the atom id (proves origin, not just
    /// integrity); false for key-erased/missing/plaintext.
    pub aad_bound: bool,
    /// Key slot and generation (encrypted regions only).
    pub key_slot: Option<u32>,
    pub key_gen: Option<u64>,
}

/// `Authentic` proves byte-integrity and origin, NOT that content is benign.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttestVerdict {
    /// Sealed bytes re-authenticated and bound to this id.
    Authentic,
    /// HMAC did not verify: bytes altered or key slot corrupt.
    Tampered,
    /// Key was erased (forgotten); content unrecoverable.
    KeyErased,
    /// No atom with this id exists in the region.
    Missing,
    /// Plaintext region: no per-atom MAC to attest.
    PlaintextUnattested,
}

impl AttestVerdict {
    /// Stable snake_case wire name.
    pub fn as_str(&self) -> &'static str {
        match self {
            AttestVerdict::Authentic => "authentic",
            AttestVerdict::Tampered => "tampered",
            AttestVerdict::KeyErased => "key_erased",
            AttestVerdict::Missing => "missing",
            AttestVerdict::PlaintextUnattested => "plaintext_unattested",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EvolutionReport {
    pub links_added: usize,
    pub score: f32,
}

/// Result of [`crate::MemoryEngine::remember_if_absent`]: id + whether it inserted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RememberOutcome {
    pub id: AtomId,
    pub inserted: bool,
}

/// One member of the caller-declared source snapshot for
/// [`remember_derived_checked`](crate::MemoryEngine::remember_derived_checked):
/// an atom the derivation read and the SHA-256 of the text it read from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSnapshot {
    pub id: AtomId,
    pub text_sha256: [u8; 32],
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
