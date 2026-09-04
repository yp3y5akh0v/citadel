//! MemoryEngine: region lifecycle on top of citadel's encrypted SQL store.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use rustc_hash::{FxHashMap, FxHashSet};
use sha2::{Digest, Sha256};

use citadel::{
    Database, KeyLifecycleGuard, MemoryAtomCallbackGuard, MemoryEdgesGuard, MemoryRegionGuard,
    SlotRecord, SlotState,
};
use citadel_core::{PageId, TxnId, WRAPPED_KEY_SIZE};
use citadel_crypto::blob_seal;
use citadel_crypto::hkdf_utils::{
    derive_atom_wrap_key, derive_identity_mac_key, derive_seal_keys, AtomWrapKey, IdentityMacKey,
};
use citadel_sql::executor::{AnnIndexSource, AnnSegmentInfo};
use citadel_sql::{Connection, ExecutionResult, Value};
use citadel_vector::{AnnIndex, Filter, Metric};
use zeroize::{Zeroize, Zeroizing};

use crate::embed::{Embedder, EmbeddingMetric, Reranker};
use crate::error::{MemError, Result};
use crate::fusion::{
    fuse_rank, fuse_rerank, recency_score, rerank_hits, rrf_merge, Candidate, RerankContext,
    RERANK_POOL,
};
use crate::read_limits::{
    atom_content_bytes, charge_atom_content, charge_edge_evidence, charge_materialized_bytes,
    charge_returned_atom_content, charge_returned_bytes, charge_returned_edge_evidence,
    with_storage_read_cap,
};
use crate::types::{
    AtomAttestation, AtomHit, AtomId, AtomInput, AttestVerdict, Edge, EdgeCursor, EdgeKind,
    EdgePage, ErasureReceipt, EvictionPolicy, EvictionReport, EvolutionReport, FetchPage,
    FetchQuery, FusionWeights, GraphExpand, KindDigest, MemoryProfileReport, MemoryRegionInfo,
    MemoryRegionInventory, MultiRecallQuery, PayloadUpdateOutcome, RecallQuery, ReembedReport,
    RememberOutcome, RerankStrategy, SlotErasure, SourceSnapshot, StoredAtomRetrievalState,
    StoredEmbeddingsIdentity, StoredRegionIdentity, SummaryQuery, SummaryReport,
    ERASURE_SCOPE_CAVEAT, STORED_EMBEDDINGS_SCHEMA,
};

/// Batch size for encrypted decrypt scans; no ANN/FTS index over ciphertext.
const EXACT_SCAN_LIMIT: usize = 4096;
const MAX_MMR_CANDIDATES: usize = EXACT_SCAN_LIMIT;
const MAX_MMR_VECTOR_BYTES: usize = 16 * 1024 * 1024;
const MAX_MMR_SIMILARITY_COMPONENTS: usize = 64 * 1024 * 1024;
const MAX_MMR_SEALED_VALUE_BYTES: usize = 8 * 1024 * 1024;
const MAX_MMR_SEALED_TOTAL_BYTES: usize = 32 * 1024 * 1024;
/// Default and hard caps for the number of kind digests returned by one summary page.
pub const DEFAULT_SUMMARY_KIND_LIMIT: usize = 256;
pub const MAX_SUMMARY_KIND_LIMIT: usize = 4096;
/// Hard cap on one reverse-provenance closure erased by dependent forgetting.
pub const MAX_DEPENDENT_FORGET_ATOMS: usize = 10_000;

/// Over-fetch factor for ANN candidates before fusion re-ranking.
const CAND_OVERFETCH: usize = 4;
/// Floor on ANN candidates evaluated (small-k recall stability).
const MIN_CANDIDATES: usize = 4096;
const ANN_SEARCH_OVERFETCH: usize = 8;
const MIN_ANN_SEARCH_CANDIDATES: usize = 64;

fn recall_candidate_limit(k: usize) -> usize {
    k.saturating_mul(CAND_OVERFETCH).max(MIN_CANDIDATES)
}

fn recall_search_window(k: usize, candidate_limit: usize) -> usize {
    // Preserve ANN exploration even when a repair needs only a few survivors.
    k.saturating_mul(ANN_SEARCH_OVERFETCH)
        .max(MIN_ANN_SEARCH_CANDIDATES)
        .max(candidate_limit)
}

#[cfg(test)]
std::thread_local! {
    /// Test fault point: sealed segment key + chunk table must be reclaimed on failure.
    static FAIL_SEALED_SEGMENT_AFTER_CHUNKS: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    /// Thread-local so parallel unit tests cannot consume another test's fault.
    static FAIL_ENCRYPTED_REGION_AFTER_SLOT: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    /// Fault after tombstone + retire, before row deletes: epoch armed, reopen converges.
    static FAIL_ERASE_BEFORE_ROW_DELETE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    /// Fault after the cascade's segment retirement, before atom-key erasure.
    static FAIL_CASCADE_AFTER_SEGMENT_RETIRE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    /// Fault after reconcile's drift-key tombstones, before row deletes: keys die first.
    static FAIL_RECONCILE_AFTER_DRIFT_KEYS: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    /// Fault at the segment-probe entry: reconcile propagates the error, destroys nothing.
    static FAIL_SEGMENT_PROBE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static FAIL_REEMBED_AFTER_SEGMENT_RETIRE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static FAILED_ENCRYPTED_REGION_WRAPPED_KEY:
        std::cell::RefCell<Option<Zeroizing<[u8; WRAPPED_KEY_SIZE]>>> =
            const { std::cell::RefCell::new(None) };
    /// Deterministically trip and detach a database token after SQL has
    /// returned but while a public operation is still doing local work.
    static CANCEL_AFTER_LOCAL_WORK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    /// Trip cancellation after ACK tombstones but before SQL residue cleanup.
    static CANCEL_AFTER_KEY_ERASURE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    /// Trip cancellation immediately after a persisted segment key is destroyed.
    static CANCEL_AFTER_SEGMENT_KEY_ERASURE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    static CANCEL_AFTER_ATOM_KEY_ALLOCATION: std::cell::RefCell<Option<citadel_core::CancelToken>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn debug_fire_cancel_after_local_work() {
    let hook = CANCEL_AFTER_LOCAL_WORK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn debug_fire_cancel_after_key_erasure() {
    let hook = CANCEL_AFTER_KEY_ERASURE.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn debug_fire_cancel_after_segment_key_erasure() {
    let hook = CANCEL_AFTER_SEGMENT_KEY_ERASURE.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

/// Stable identifier for a memory region (row id in `memory_regions`).
pub type RegionId = i64;

/// Rolls back not-yet-durable key allocations through the retained span; no live orphans.
struct PendingAtomSlots<'a> {
    kl: Option<&'a KeyLifecycleGuard<'a>>,
    slots: Vec<(u32, u64, u64)>,
    armed: bool,
}

impl<'a> PendingAtomSlots<'a> {
    fn new(kl: Option<&'a KeyLifecycleGuard<'a>>, capacity: usize) -> Self {
        Self {
            kl,
            // Reserve first: tracking stays alloc-free so an OOM unwind reaches Drop.
            slots: Vec::with_capacity(capacity),
            armed: true,
        }
    }

    fn track(&mut self, slot: u32, owner: u64, generation: u64) {
        assert!(
            self.kl.is_some(),
            "atom key allocations happen only inside a sealed lifecycle span"
        );
        self.slots.push((slot, owner, generation));
    }

    fn finish<T>(mut self, result: Result<T>) -> Result<T> {
        match result {
            Ok(value) => {
                self.armed = false;
                Ok(value)
            }
            Err(source) => {
                if self.slots.is_empty() {
                    self.armed = false;
                    return Err(source);
                }
                let kl = self.kl.expect("track() asserted the capability");
                match kl.atom_store_tombstone_batch(&self.slots) {
                    Ok(_) => {
                        self.armed = false;
                        Err(source)
                    }
                    Err(rollback) => Err(MemError::Invalid(format!(
                        "{source}; additionally failed to tombstone pending atom keys: {rollback}"
                    ))),
                }
            }
        }
    }
}

impl Drop for PendingAtomSlots<'_> {
    fn drop(&mut self) {
        if self.armed && !self.slots.is_empty() {
            if let Some(kl) = self.kl {
                let _ = kl.atom_store_tombstone_batch(&self.slots);
            }
        }
    }
}

/// Region-key slot durable before its row commits; rolled back via the retained span.
struct PendingRegionSlot<'a> {
    kl: &'a KeyLifecycleGuard<'a>,
    binding: Option<(u32, u64, u64)>,
}

impl<'a> PendingRegionSlot<'a> {
    fn new(kl: &'a KeyLifecycleGuard<'a>, slot: u32, owner: u64, generation: u64) -> Self {
        Self {
            kl,
            binding: Some((slot, owner, generation)),
        }
    }

    fn finish<T>(mut self, result: Result<T>) -> Result<T> {
        match result {
            Ok(value) => {
                self.binding = None;
                Ok(value)
            }
            Err(source) => {
                let (slot, owner, generation) = self
                    .binding
                    .expect("pending region slot is armed until finish succeeds");
                match self.kl.region_store_tombstone(slot, owner, generation) {
                    Ok(()) => {
                        self.binding = None;
                        Err(source)
                    }
                    Err(rollback) => Err(MemError::Invalid(format!(
                        "{source}; additionally failed to tombstone pending region key: {rollback}"
                    ))),
                }
            }
        }
    }
}

impl Drop for PendingRegionSlot<'_> {
    fn drop(&mut self) {
        if let Some((slot, owner, generation)) = self.binding {
            let _ = self.kl.region_store_tombstone(slot, owner, generation);
        }
    }
}

/// Both per-region secrets derived from one RCK unwrap.
struct RegionKeys {
    atom_wrap: Arc<AtomWrapKey>,
    identity_mac: Arc<IdentityMacKey>,
}

/// A region attached to a live embedder in this process.
struct RegionState {
    id: RegionId,
    dim: u16,
    metric: EmbeddingMetric,
    embedder: Arc<dyn Embedder>,
    /// Snapshotted before any engine lock is taken; embedder callbacks are external code.
    model_id: Arc<str>,
    /// Encrypted regions: wraps/unwraps each atom's ACK (derived from the RCK).
    atom_wrap: Option<Arc<AtomWrapKey>>,
    /// Encrypted regions: keyed MAC for identity tags (own HKDF label).
    identity_mac: Option<Arc<IdentityMacKey>>,
    /// Lazy in-RAM ANN index over decrypted vectors for sealed recall.
    ann: Arc<RwLock<Option<SealedAnn>>>,
    /// Highest atom id; sealed recall reads it to detect post-snapshot inserts
    /// without a per-call `MAX(id)` scan.
    max_id: Arc<AtomicI64>,
}

/// Region fields needed off-lock by remember/recall.
struct RegionHandle {
    id: RegionId,
    table: String,
    embedder: Arc<dyn Embedder>,
    dim: u16,
    metric: EmbeddingMetric,
    model_id: Arc<str>,
    atom_wrap: Option<Arc<AtomWrapKey>>,
    identity_mac: Option<Arc<IdentityMacKey>>,
    ann: Arc<RwLock<Option<SealedAnn>>>,
    max_id: Arc<AtomicI64>,
}

/// Lazy per-region in-RAM PRISM index over decrypted vectors; zeroized on drop
/// so they never outlive the region key. May persist as a sealed segment under
/// its own erasable key, so destroying that slot crypto-erases the SQ8 codes.
struct SealedAnn {
    index: AnnIndex,
    /// Atom `kind` -> PRISM attribute code, for kind-filtered recall.
    kind_codes: FxHashMap<String, u32>,
    /// Rank inputs cached at build so the hot path skips re-fetch/decrypt;
    /// plaintext, zeroized on drop with the index vectors.
    cached: FxHashMap<AtomId, CachedAtom>,
    /// Cosine distance is undefined for these zero-norm vectors. PRISM reports
    /// the same numeric value as an orthogonal vector, so preserve the semantic
    /// distinction alongside the index.
    zero_norm_atoms: FxHashSet<AtomId>,
    /// Persisted sealed segment or a scan build.
    source: AnnIndexSource,
    /// [`Database::cache_epoch`] at build; once it moves, stale plaintext is refused.
    build_epoch: u64,
    /// Non-ABA stamp of the atom table snapshot that supplied every cached field.
    table_stamp: (PageId, TxnId),
}

type SealedCandidateSnapshot = (Vec<(AtomId, Option<f32>)>, Option<(PageId, TxnId)>);

/// Detached state whose decrypted ANN is zeroized with it, outside shared locks.
struct RetiredRegionState {
    id: RegionId,
    _ann: Option<SealedAnn>,
    _state: RegionState,
}

fn retire_region_state(state: RegionState) -> RetiredRegionState {
    let ann = state.ann.write().unwrap().take();
    RetiredRegionState {
        id: state.id,
        _ann: ann,
        _state: state,
    }
}

/// Per-atom fields a sealed recall needs, decrypted once at index build.
struct CachedAtom {
    kind: String,
    text: String,
    payload: serde_json::Value,
    owned_content_bytes: usize,
    importance: f32,
    confidence: f32,
    created_micros: i64,
    immutable: bool,
    /// TTL lapse instant; recall skips the atom once wall-clock passes it.
    expires_micros: Option<i64>,
}

impl Drop for CachedAtom {
    fn drop(&mut self) {
        self.text.zeroize();
        zeroize_json_strings(&mut self.payload);
    }
}

/// Nulls every owned string (keys included); returns count so tests prove recursion.
fn zeroize_json_strings(value: &mut serde_json::Value) -> usize {
    let mut scrubbed = 0;
    match value {
        serde_json::Value::String(text) => {
            text.zeroize();
            scrubbed += 1;
        }
        serde_json::Value::Array(values) => {
            for value in values.iter_mut() {
                scrubbed += zeroize_json_strings(value);
            }
            values.clear();
        }
        serde_json::Value::Object(fields) => {
            for (mut key, mut value) in std::mem::take(fields) {
                key.zeroize();
                scrubbed += 1;
                scrubbed += zeroize_json_strings(&mut value);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
    *value = serde_json::Value::Null;
    scrubbed
}

fn zeroize_atom_content(text: &mut String, payload: &mut serde_json::Value) {
    text.zeroize();
    zeroize_json_strings(payload);
}

struct ScrubbedHitSlots(Vec<Option<AtomHit>>);

impl ScrubbedHitSlots {
    fn from_hits(hits: Vec<AtomHit>) -> Self {
        Self(hits.into_iter().map(Some).collect())
    }

    fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    fn into_hits(mut self) -> Vec<AtomHit> {
        std::mem::take(&mut self.0).into_iter().flatten().collect()
    }
}

impl Drop for ScrubbedHitSlots {
    fn drop(&mut self) {
        for hit in self.0.iter_mut().filter_map(Option::as_mut) {
            zeroize_atom_content(&mut hit.text, &mut hit.payload);
        }
    }
}

fn charge_owned_atom_content(
    kind: &str,
    text: &mut String,
    payload: &mut serde_json::Value,
) -> Result<()> {
    if let Err(error) = charge_atom_content(kind, text, payload) {
        zeroize_atom_content(text, payload);
        return Err(error);
    }
    Ok(())
}

fn charge_owned_edge_evidence(evidence: &mut Option<serde_json::Value>) -> Result<()> {
    if let Err(error) = charge_edge_evidence(evidence.as_ref()) {
        if let Some(evidence) = evidence {
            zeroize_json_strings(evidence);
        }
        return Err(error);
    }
    Ok(())
}

fn charge_returned_hits(hits: &mut [AtomHit]) -> Result<()> {
    for hit in hits.iter() {
        if let Err(error) = charge_returned_atom_content(&hit.kind, &hit.text, &hit.payload) {
            for hit in hits {
                zeroize_atom_content(&mut hit.text, &mut hit.payload);
            }
            return Err(error);
        }
    }
    Ok(())
}

fn charge_returned_hit(hit: &mut Option<AtomHit>) -> Result<()> {
    let Some(hit) = hit else {
        return Ok(());
    };
    if let Err(error) = charge_returned_atom_content(&hit.kind, &hit.text, &hit.payload) {
        zeroize_atom_content(&mut hit.text, &mut hit.payload);
        return Err(error);
    }
    Ok(())
}

fn charge_returned_optional_hits(hits: &mut [Option<AtomHit>]) -> Result<()> {
    for index in 0..hits.len() {
        if let Err(error) = charge_returned_hit(&mut hits[index]) {
            for hit in hits.iter_mut().filter_map(Option::as_mut) {
                zeroize_atom_content(&mut hit.text, &mut hit.payload);
            }
            return Err(error);
        }
    }
    Ok(())
}

fn charge_returned_edges(edges: &mut [Edge]) -> Result<()> {
    for edge in edges.iter() {
        if let Err(error) = charge_returned_edge_evidence(edge.evidence_ref.as_ref()) {
            for edge in edges {
                if let Some(evidence) = &mut edge.evidence_ref {
                    zeroize_json_strings(evidence);
                }
            }
            return Err(error);
        }
    }
    Ok(())
}

fn charge_returned_text<'a>(values: impl IntoIterator<Item = &'a str>) -> Result<()> {
    for value in values {
        charge_returned_bytes(value.len())?;
    }
    Ok(())
}

fn charge_returned_embedding_identity(identity: &StoredEmbeddingsIdentity) -> Result<()> {
    charge_returned_text([
        identity.schema(),
        identity.region(),
        identity.kind(),
        identity.sha256(),
    ])
}

fn embedding_bytes(dimension: usize) -> Result<usize> {
    dimension
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| MemError::Invalid("stored embedding size overflow".into()))
}

fn validate_mmr_request(fetch_k: usize, lambda_mult: f32) -> Result<()> {
    if !lambda_mult.is_finite() || !(0.0..=1.0).contains(&lambda_mult) {
        return Err(MemError::Invalid(
            "MMR lambda must be finite and between 0 and 1".into(),
        ));
    }
    if fetch_k > MAX_MMR_CANDIDATES {
        return Err(MemError::WorkLimitExceeded {
            operation: "MMR candidate count",
            limit: MAX_MMR_CANDIDATES,
        });
    }
    Ok(())
}

fn validate_mmr_region_work(
    fetch_k: usize,
    k: usize,
    dimension: usize,
    metric: EmbeddingMetric,
) -> Result<()> {
    if metric != EmbeddingMetric::Cosine {
        return Err(MemError::Invalid(
            "MMR recall currently requires a cosine region".into(),
        ));
    }
    let vector_bytes = embedding_bytes(dimension)?
        .checked_mul(fetch_k)
        .ok_or_else(|| MemError::Invalid("MMR candidate-vector size overflow".into()))?;
    if vector_bytes > MAX_MMR_VECTOR_BYTES {
        return Err(MemError::WorkLimitExceeded {
            operation: "MMR candidate-vector bytes",
            limit: MAX_MMR_VECTOR_BYTES,
        });
    }
    let similarity_components = fetch_k
        .checked_mul(k.min(fetch_k))
        .and_then(|work| work.checked_mul(dimension))
        .ok_or_else(|| MemError::Invalid("MMR similarity work overflow".into()))?;
    if similarity_components > MAX_MMR_SIMILARITY_COMPONENTS {
        return Err(MemError::WorkLimitExceeded {
            operation: "MMR similarity components",
            limit: MAX_MMR_SIMILARITY_COMPONENTS,
        });
    }
    Ok(())
}

fn cosine_similarity(
    left: &[f32],
    right: &[f32],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<f64> {
    debug_assert_eq!(left.len(), right.len());
    let mut dot = 0.0f64;
    let mut left_norm = 0.0f64;
    let mut right_norm = 0.0f64;
    for (index, (&left, &right)) in left.iter().zip(right).enumerate() {
        if index % 1024 == 0 {
            check_cancel(cancel)?;
        }
        let left = f64::from(left);
        let right = f64::from(right);
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    let denominator = left_norm.sqrt() * right_norm.sqrt();
    Ok(if denominator == 0.0 {
        0.0
    } else {
        dot / denominator
    })
}

fn maximal_marginal_relevance(
    query: &[f32],
    candidates: &[&[f32]],
    k: usize,
    lambda_mult: f32,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<usize>> {
    let take = k.min(candidates.len());
    if take == 0 {
        return Ok(Vec::new());
    }
    let query_scores = candidates
        .iter()
        .map(|candidate| cosine_similarity(query, candidate, cancel))
        .collect::<Result<Vec<_>>>()?;
    let first = query_scores
        .iter()
        .enumerate()
        .max_by(|(left_index, left), (right_index, right)| {
            left.partial_cmp(right)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right_index.cmp(left_index))
        })
        .map(|(index, _)| index)
        .expect("non-empty candidate scores");
    let mut selected = Vec::with_capacity(take);
    selected.push(first);
    if take == 1 {
        return Ok(selected);
    }
    let mut is_selected = vec![false; candidates.len()];
    is_selected[first] = true;
    let mut max_redundancy = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        max_redundancy.push(cosine_similarity(candidate, candidates[first], cancel)?);
    }
    while selected.len() < take {
        check_cancel(cancel)?;
        let mut best: Option<(usize, f64)> = None;
        for (index, &query_score) in query_scores.iter().enumerate() {
            if is_selected[index] {
                continue;
            }
            let lambda = f64::from(lambda_mult);
            let score = lambda * query_score - (1.0 - lambda) * max_redundancy[index];
            if best.is_none_or(|(_, best_score)| score > best_score) {
                best = Some((index, score));
            }
        }
        let next = best.expect("unselected MMR candidate remains").0;
        selected.push(next);
        is_selected[next] = true;
        if selected.len() < take {
            for index in 0..candidates.len() {
                if is_selected[index] {
                    continue;
                }
                max_redundancy[index] = max_redundancy[index].max(cosine_similarity(
                    candidates[index],
                    candidates[next],
                    cancel,
                )?);
            }
        }
    }
    Ok(selected)
}

/// Map the memory metric to PRISM's distance metric.
fn ann_metric(m: EmbeddingMetric) -> Metric {
    match m {
        EmbeddingMetric::Cosine => Metric::Cosine,
        EmbeddingMetric::L2 => Metric::L2,
        EmbeddingMetric::InnerProduct => Metric::InnerProduct,
    }
}

/// Encrypted-first memory engine over a shared [`Database`].
pub struct MemoryEngine {
    db: Arc<Database>,
    regions: Arc<Mutex<FxHashMap<String, RegionState>>>,
    _region_invalidator: Arc<citadel::MemoryRegionInvalidator>,
    _atom_invalidator: Arc<citadel::MemoryAtomInvalidator>,
    /// Cross-encoder + strategy applied in `recall` before truncation (`None`
    /// = linear fusion). Snapshotted before the callback so the guard is not
    /// held across a re-entrant reranker.
    reranker: RwLock<Option<(Arc<dyn Reranker>, RerankStrategy)>>,
    /// Per-table `region_id -> MAX(id)` re-attach snapshot, tagged with the
    /// commit generation (any commit invalidates it): one grouped scan for R
    /// regions instead of R full scans.
    attach_max: Mutex<FxHashMap<String, AttachMaxSnapshot>>,
    /// In-process read tracking feeding the `Lru`/`Stale` eviction policies.
    /// Reads stay write-free, so this is engine-lifetime state layered over the
    /// persisted insert-time floor.
    access_stats: Mutex<FxHashMap<RegionId, RegionAccessStats>>,
}

/// Re-attach `MAX(id)` snapshot for one atoms table: `(commit generation at
/// scan, region_id -> MAX(id))`.
type AttachMaxSnapshot = (u64, FxHashMap<RegionId, i64>);

/// One region's in-process read hits: `atom -> (last access micros, count)`.
type RegionAccessStats = FxHashMap<AtomId, (i64, u32)>;

/// A recall request paired with its validated query vector.
#[derive(Clone, Copy)]
struct ResolvedRecall<'a> {
    query: &'a RecallQuery,
    vector: &'a [f32],
}

/// Atoms committed per re-embedding batch.
const REEMBED_BATCH: usize = 256;

/// Durable checkpoint written before the first batch and advanced with each commit.
#[derive(Clone, PartialEq, Eq)]
struct ReembedMark {
    to_model: String,
    to_dim: u16,
    to_metric: String,
    /// The highest atom id already converted. Resume starts after it.
    done_through: i64,
    phase: ReembedPhase,
}

/// Independently resumable stages of re-embedding.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReembedPhase {
    /// Vectors are being converted. The region holds two models at once, so it
    /// is closed to attach, read and write.
    Vectors,
    /// Vectors and provenance are correct, but access remains closed until the
    /// `SimilarTo` web has been repaired over the new vector space.
    Repair,
}

impl ReembedPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Vectors => "vectors",
            Self::Repair => "repair",
        }
    }
}

/// Record the in-flight migration while preserving unrelated metadata.
fn write_reembed_mark(conn: &Connection, region_id: i64, mark: &ReembedMark) -> Result<()> {
    let json = mark_json(read_metadata(conn, region_id)?.as_deref(), mark)?;
    conn.execute_params(
        "UPDATE memory_regions SET metadata = $1 WHERE id = $2",
        &[Value::Text(json.into()), Value::Integer(region_id)],
    )?;
    Ok(())
}

/// Keys owned by the re-embedding checkpoint.
const REEMBED_MARK_KEYS: [&str; 5] = [
    "reembed_to_model",
    "reembed_to_dim",
    "reembed_to_metric",
    "reembed_done_through",
    "reembed_phase",
];

fn metadata_object(existing: Option<&str>) -> Result<serde_json::Map<String, serde_json::Value>> {
    let Some(existing) = existing else {
        return Ok(serde_json::Map::new());
    };
    match serde_json::from_str(existing) {
        Ok(serde_json::Value::Object(map)) => Ok(map),
        Ok(_) => Err(MemError::Invalid(
            "memory region metadata must be a JSON object".into(),
        )),
        Err(error) => Err(MemError::Invalid(format!(
            "memory region metadata is malformed JSON: {error}"
        ))),
    }
}

fn mark_json(existing: Option<&str>, mark: &ReembedMark) -> Result<String> {
    let mut doc = metadata_object(existing)?;
    doc.insert("reembed_to_model".into(), mark.to_model.clone().into());
    doc.insert("reembed_to_dim".into(), mark.to_dim.into());
    doc.insert("reembed_to_metric".into(), mark.to_metric.clone().into());
    doc.insert("reembed_done_through".into(), mark.done_through.into());
    doc.insert("reembed_phase".into(), mark.phase.as_str().into());
    Ok(serde_json::Value::Object(doc).to_string())
}

/// Remove the checkpoint while preserving unrelated metadata.
fn without_mark(existing: Option<&str>) -> Result<Option<String>> {
    let mut doc = metadata_object(existing)?;
    for k in REEMBED_MARK_KEYS {
        doc.remove(k);
    }
    Ok((!doc.is_empty()).then(|| serde_json::Value::Object(doc).to_string()))
}

/// The row's `metadata` as JSON text. The column is JSONB, so it reads back as
/// encoded bytes and the cast is what decodes it.
fn read_metadata(conn: &Connection, region_id: i64) -> Result<Option<String>> {
    let qr = conn.query_params(
        "SELECT CAST(metadata AS TEXT) FROM memory_regions WHERE id = $1",
        &[Value::Integer(region_id)],
    )?;
    Ok(qr.rows.first().and_then(|r| match &r[0] {
        Value::Text(t) => Some(t.to_string()),
        _ => None,
    }))
}

fn parse_reembed_mark(metadata: Option<&str>, region_id: i64) -> Result<Option<ReembedMark>> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let doc = metadata_object(Some(metadata))?;
    if !REEMBED_MARK_KEYS.iter().any(|key| doc.contains_key(*key)) {
        return Ok(None);
    }
    let invalid = |field: &str| {
        MemError::Invalid(format!(
            "memory region {region_id} has an incomplete or invalid re-embed mark ({field})"
        ))
    };
    let to_model = doc
        .get("reembed_to_model")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("reembed_to_model"))?
        .to_owned();
    let to_dim = doc
        .get("reembed_to_dim")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid("reembed_to_dim"))?;
    let to_metric = doc
        .get("reembed_to_metric")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("reembed_to_metric"))?
        .to_owned();
    metric_from_str(&to_metric).map_err(|_| invalid("reembed_to_metric"))?;
    let done_through = doc
        .get("reembed_done_through")
        .and_then(serde_json::Value::as_i64)
        .filter(|value| *value >= 0)
        .ok_or_else(|| invalid("reembed_done_through"))?;
    let phase = match doc.get("reembed_phase").and_then(serde_json::Value::as_str) {
        Some("vectors") => ReembedPhase::Vectors,
        Some("repair") => ReembedPhase::Repair,
        _ => return Err(invalid("reembed_phase")),
    };
    Ok(Some(ReembedMark {
        to_model,
        to_dim,
        to_metric,
        done_through,
        phase,
    }))
}

fn validate_repair_mark(
    mark: &ReembedMark,
    model_id: &str,
    dim: u16,
    metric: EmbeddingMetric,
    region_id: i64,
) -> Result<()> {
    if mark.phase == ReembedPhase::Repair
        && (mark.to_model != model_id || mark.to_dim != dim || mark.to_metric != metric_tag(metric))
    {
        return Err(MemError::Invalid(format!(
            "memory region {region_id} has a repair mark that disagrees with its stored provenance"
        )));
    }
    Ok(())
}

/// Clear a zero-progress migration while preserving unrelated metadata.
fn clear_reembed_mark(conn: &Connection, region_id: i64) -> Result<()> {
    if let Some(rest) = without_mark(read_metadata(conn, region_id)?.as_deref())? {
        conn.execute_params(
            "UPDATE memory_regions SET metadata = $1 WHERE id = $2",
            &[Value::Text(rest.into()), Value::Integer(region_id)],
        )?;
        return Ok(());
    }
    conn.execute_params(
        "UPDATE memory_regions SET metadata = NULL WHERE id = $1",
        &[Value::Integer(region_id)],
    )?;
    Ok(())
}

/// Restore pre-migration metadata through an uncancelled recovery transaction.
fn restore_reembed_metadata(
    conn: &Connection,
    region_id: i64,
    metadata: Option<&str>,
) -> Result<()> {
    conn.execute_params_uncancelled_recovery(
        "UPDATE memory_regions SET metadata = $1 WHERE id = $2",
        &[
            metadata
                .map(|json| Value::Text(json.into()))
                .unwrap_or(Value::Null),
            Value::Integer(region_id),
        ],
    )?;
    Ok(())
}

/// Restore metadata only while this caller still owns the first checkpoint.
/// Recovery is uncancelled because the operation's token may already be tripped.
fn restore_reembed_metadata_if_unchanged(
    conn: &Connection,
    region_id: i64,
    expected: &ReembedMark,
    metadata: Option<&str>,
) -> Result<()> {
    conn.execute_params_uncancelled_recovery(
        "UPDATE memory_regions SET metadata = $1 WHERE id = $2 \
         AND metadata ->> 'reembed_to_model' = $3 \
         AND metadata ->> 'reembed_to_dim' = $4 \
         AND metadata ->> 'reembed_to_metric' = $5 \
         AND metadata ->> 'reembed_done_through' = $6 \
         AND metadata ->> 'reembed_phase' = $7",
        &[
            metadata
                .map(|json| Value::Text(json.into()))
                .unwrap_or(Value::Null),
            Value::Integer(region_id),
            Value::Text(expected.to_model.as_str().into()),
            Value::Text(expected.to_dim.to_string().into()),
            Value::Text(expected.to_metric.as_str().into()),
            Value::Text(expected.done_through.to_string().into()),
            Value::Text(expected.phase.as_str().into()),
        ],
    )?;
    Ok(())
}

/// Atomically publish provenance and advance the checkpoint to graph repair.
fn publish_reembed(conn: &Connection, region_id: i64, mark: &ReembedMark) -> Result<()> {
    let repair = ReembedMark {
        to_model: mark.to_model.clone(),
        to_dim: mark.to_dim,
        to_metric: mark.to_metric.clone(),
        done_through: mark.done_through,
        phase: ReembedPhase::Repair,
    };
    conn.execute_params(
        "UPDATE memory_regions SET model_id = $1, embedding_dim = $2, embedding_metric = $3, \
         metadata = $4 WHERE id = $5",
        &[
            Value::Text(mark.to_model.as_str().into()),
            Value::Integer(mark.to_dim as i64),
            Value::Text(mark.to_metric.as_str().into()),
            Value::Text(mark_json(read_metadata(conn, region_id)?.as_deref(), &repair)?.into()),
            Value::Integer(region_id),
        ],
    )?;
    Ok(())
}

/// What the vector stage leaves for the repair stage that follows it.
struct Migration {
    report: ReembedReport,
    region_id: RegionId,
    /// The exact repair-phase mark this call observed or published. `None`
    /// means the region already used this provenance and needed no work.
    repair: Option<ReembedMark>,
}

/// What one migration batch needs beyond the rows it is writing.
struct BatchContext<'a> {
    /// `Some` for a sealed region, whose text lives inside the blob.
    atom_wrap: Option<&'a AtomWrapKey>,
    region_id: RegionId,
    /// Advanced inside the batch's own transaction, so the rows and the record
    /// of how far the migration got can never disagree.
    checkpoint: &'a ReembedMark,
    cancel: Option<&'a citadel_core::CancelToken>,
}

/// One bounded page of atoms a re-embed has to recompute.
#[derive(Default)]
struct ReembedWork {
    atoms: Vec<ReembedAtom>,
    /// Atoms stored with a vector but no text.
    empty_text: u64,
    /// Highest row id the page read, including rows whose erased key made them
    /// unrecoverable. Pagination must advance even when every row is skipped.
    last_id: Option<AtomId>,
}

struct ReembedAtom {
    id: AtomId,
    text: String,
    /// Present only for a sealed atom; retained so resealing does not decrypt
    /// and decode the same row a third time.
    payload_json: Option<Zeroizing<String>>,
}

impl Drop for ReembedWork {
    fn drop(&mut self) {
        // Sealed-region text was plaintext only for this migration page.
        for atom in &mut self.atoms {
            atom.text.zeroize();
        }
    }
}

/// An atom's vector plus the two columns recall scoring derives from it.
struct AtomState {
    embedding: Vec<f32>,
    access_count: i64,
    created: i64,
}

/// The exact durable rule that produced one atom's managed `SimilarTo` edges.
struct WeaveShape {
    src: AtomId,
    neighbors: usize,
    max_distance: f32,
    kinds: Vec<String>,
}

type ManagedSimilarityEdge = (AtomId, f32, Option<serde_json::Value>);

/// How a recall run differs between one caller query and a multi-query member.
#[derive(Clone, Copy)]
struct RecallMode {
    rerank: bool,
    record_access: bool,
}

/// Optional final diversification over a bounded recall candidate pool.
#[derive(Clone, Copy)]
struct MmrSelection {
    k: usize,
    lambda_mult: f32,
}

impl RecallMode {
    /// A caller's recall.
    const USER: Self = Self {
        rerank: true,
        record_access: true,
    };
    /// A sub-query of `recall_many`, which reranks the merged pool instead.
    const SUBQUERY: Self = Self {
        rerank: false,
        record_access: true,
    };
    /// Deterministic derived-data maintenance.
    const INTERNAL: Self = Self {
        rerank: false,
        record_access: false,
    };
}

/// What a re-embed did to the region's `SimilarTo` web.
#[derive(Default)]
struct WeaveRebuild {
    /// Edges recomputed over the new vectors.
    rewoven: u64,
    /// Stale edges nothing could recompute.
    cleared: u64,
}

/// A bounded set of atom ids as a SQL `IN` list.
fn id_list(ids: &[AtomId]) -> String {
    ids.iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn live_region_atom_ids(
    conn: &Connection<'_>,
    tables: &[String],
    region_id: RegionId,
    ids: &[AtomId],
) -> Result<FxHashSet<AtomId>> {
    let mut live = FxHashSet::default();
    for page in ids.chunks(REEMBED_BATCH) {
        if page.is_empty() {
            continue;
        }
        let list = id_list(page);
        for table in tables {
            let rows = conn.query_params(
                &format!("SELECT id FROM {table} WHERE region_id = $1 AND id IN ({list})"),
                &[Value::Integer(region_id)],
            )?;
            for row in &rows.rows {
                live.insert(as_int(&row[0])?);
            }
        }
    }
    Ok(live)
}

fn check_cancel(cancel: Option<&citadel_core::CancelToken>) -> Result<()> {
    if let Some(token) = cancel {
        token.check().map_err(MemError::Core)?;
    }
    Ok(())
}

fn segment_operation_error(error: citadel_vector::segment::SegmentOperationError) -> MemError {
    match error {
        citadel_vector::segment::SegmentOperationError::Interrupted => {
            MemError::Core(citadel_core::Error::Interrupted)
        }
        citadel_vector::segment::SegmentOperationError::Allocation(where_) => {
            MemError::Invalid(format!("ANN segment allocation failed in {where_}"))
        }
        citadel_vector::segment::SegmentOperationError::Segment(source) => {
            MemError::Invalid(format!("ANN segment operation failed: {source}"))
        }
    }
}

/// Check and retain the database token that governs one external call.
fn check_db_cancel(db: &Database) -> Result<Option<citadel_core::CancelToken>> {
    let cancel = db.cancel_token();
    check_cancel(cancel.as_ref())?;
    Ok(cancel)
}

const BOOTSTRAP_SQL: &str = "\
CREATE TABLE IF NOT EXISTS memory_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS memory_regions (\
 id INTEGER PRIMARY KEY,\
 name TEXT UNIQUE NOT NULL,\
 embedding_dim INTEGER NOT NULL,\
 embedding_metric TEXT NOT NULL,\
 model_id TEXT NOT NULL,\
 encrypted INTEGER NOT NULL DEFAULT 0,\
 rsk_slot INTEGER,\
 rsk_gen INTEGER,\
 created_at TIMESTAMP NOT NULL,\
 metadata JSONB);
CREATE TABLE IF NOT EXISTS memory_edges (\
 src_id INTEGER NOT NULL,\
 dst_id INTEGER NOT NULL,\
 kind TEXT NOT NULL,\
 weight REAL DEFAULT 1.0,\
 evidence_ref JSONB,\
 PRIMARY KEY (src_id, dst_id, kind));
CREATE TABLE IF NOT EXISTS memory_similarity_policies (\
 region_id INTEGER NOT NULL,\
 src_id INTEGER PRIMARY KEY,\
 neighbors INTEGER NOT NULL,\
 max_distance REAL NOT NULL,\
 kinds JSONB NOT NULL);
CREATE TABLE IF NOT EXISTS memory_similarity_edges (\
 src_id INTEGER NOT NULL,\
 dst_id INTEGER NOT NULL,\
 PRIMARY KEY (src_id, dst_id));
CREATE INDEX IF NOT EXISTS memory_similarity_policies_region \
 ON memory_similarity_policies (region_id, src_id);
CREATE INDEX IF NOT EXISTS memory_similarity_edges_dst \
 ON memory_similarity_edges (dst_id);
CREATE TABLE IF NOT EXISTS memory_idempotency (\
 region_id INTEGER NOT NULL,\
 kind TEXT NOT NULL,\
 key_mac TEXT NOT NULL,\
 request_mac TEXT NOT NULL,\
 atom_id INTEGER NOT NULL,\
 PRIMARY KEY (region_id, kind, key_mac));
CREATE UNIQUE INDEX IF NOT EXISTS memory_idempotency_atom ON memory_idempotency (atom_id);
INSERT INTO memory_meta (key, value) VALUES ('next_region_id', 1) ON CONFLICT (key) DO NOTHING;
INSERT INTO memory_meta (key, value) VALUES ('next_atom_id', 1) ON CONFLICT (key) DO NOTHING;";

/// Model-free access for inventory, verification, and erasure tooling.
///
/// Unlike [`MemoryEngine`], opening this capability never creates or reconciles schema.
/// It has no embedding, recall, remember, edge, or vector-mutation surface. Each operation
/// reloads the stored region and holds the database key-lifecycle capability through the
/// read or erasure, so a key cannot be destroyed or rebound underneath plaintext already
/// returned by the operation.
pub struct MemoryMaintenance {
    db: Arc<Database>,
}

enum RegionKeyInventorySnapshot {
    NotRequired,
    Missing,
    Invalid(String),
    Available(FxHashMap<u32, std::result::Result<SlotRecord, String>>),
}

#[derive(Clone, Copy)]
enum MaintenanceRegionMode {
    ReadContent,
    InspectMetadata,
    IrreversibleErasure,
}

impl MaintenanceRegionMode {
    fn loads_content_key(self) -> bool {
        matches!(self, Self::ReadContent)
    }

    fn checks_completion_cancel(self) -> bool {
        !matches!(self, Self::IrreversibleErasure)
    }
}

impl MemoryMaintenance {
    /// Open an existing memory schema without changing the vault.
    pub fn open(db: Arc<Database>) -> Result<Self> {
        let cancel = check_db_cancel(&db)?;
        let conn = Connection::open(&db)?;
        let schema = conn
            .table_schema("memory_regions")
            .ok_or_else(|| MemError::Invalid("memory schema is not present".into()))?;
        for required in [
            "id",
            "name",
            "embedding_dim",
            "embedding_metric",
            "model_id",
            "encrypted",
            "rsk_slot",
            "rsk_gen",
            "metadata",
        ] {
            if !schema.columns.iter().any(|column| column.name == required) {
                return Err(MemError::Invalid(format!(
                    "incompatible memory schema: memory_regions lacks '{required}'"
                )));
            }
        }
        check_cancel(cancel.as_ref())?;
        drop(conn);
        Ok(Self { db })
    }

    /// Persisted descriptions, including a region whose content key was erased.
    ///
    /// A returned row is inventory only. Call [`count_region`](Self::count_region),
    /// [`fetch_range`](Self::fetch_range), or [`verify_atoms`](Self::verify_atoms) to
    /// establish that its content remains readable.
    pub fn regions(&self) -> Result<Vec<MemoryRegionInfo>> {
        let cancel = check_db_cancel(&self.db)?;
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            "SELECT name, id, embedding_dim, embedding_metric, model_id, encrypted, \
             rsk_slot, rsk_gen FROM memory_regions",
            &[],
        )?;
        let mut regions = Vec::with_capacity(qr.rows.len());
        for row in &qr.rows {
            check_cancel(cancel.as_ref())?;
            let name = as_text(&row[0])?;
            let stored = parse_region_row(&row[1..])?;
            regions.push(MemoryRegionInfo::new(
                name.to_owned(),
                stored.encrypted,
                stored.dim,
                stored.metric,
                stored.model_id,
            ));
        }
        regions.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        check_cancel(cancel.as_ref())?;
        charge_returned_text(
            regions
                .iter()
                .flat_map(|region| [region.name(), region.model_id()]),
        )?;
        Ok(regions)
    }

    /// Region descriptions and live counts from one key-lifecycle snapshot.
    ///
    /// Region-key slots are authenticated from one lifecycle snapshot; atom-key bindings are
    /// read in bounded pages for each encrypted region. A region that cannot be counted remains
    /// present with `live_atoms == None` and the exact engine error in `unavailable`.
    pub fn inventory(&self) -> Result<Vec<MemoryRegionInventory>> {
        let cancel = check_db_cancel(&self.db)?;
        let lifecycle = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            "SELECT name, id, embedding_dim, embedding_metric, model_id, encrypted, \
             rsk_slot, rsk_gen, CAST(metadata AS TEXT) FROM memory_regions",
            &[],
        )?;
        let mut stored_regions = Vec::with_capacity(qr.rows.len());
        for row in &qr.rows {
            check_cancel(cancel.as_ref())?;
            let metadata = match &row[8] {
                Value::Null => None,
                Value::Text(value) => Some(value.to_string()),
                other => {
                    return Err(MemError::Invalid(format!(
                        "expected region metadata text, got {other:?}"
                    )))
                }
            };
            let (stored, binding_error) = parse_inventory_region_row(&row[1..8])?;
            stored_regions.push((
                as_text(&row[0])?.to_owned(),
                stored,
                metadata,
                binding_error,
            ));
        }
        let region_key_records = if stored_regions
            .iter()
            .any(|(_, stored, _, _)| stored.encrypted)
        {
            let slots: Vec<u32> = stored_regions
                .iter()
                .filter_map(|(_, stored, _, binding_error)| {
                    (stored.encrypted && binding_error.is_none())
                        .then_some(stored.rsk_slot)
                        .flatten()
                })
                .collect();
            match self.db.region_store_slot_results(&slots) {
                Ok(records) => RegionKeyInventorySnapshot::Available(
                    slots
                        .into_iter()
                        .zip(
                            records
                                .into_iter()
                                .map(|record| record.map_err(|error| error.to_string())),
                        )
                        .collect::<FxHashMap<_, _>>(),
                ),
                Err(citadel_core::Error::Io(source))
                    if source.kind() == std::io::ErrorKind::NotFound =>
                {
                    RegionKeyInventorySnapshot::Missing
                }
                Err(error) => RegionKeyInventorySnapshot::Invalid(error.to_string()),
            }
        } else {
            RegionKeyInventorySnapshot::NotRequired
        };
        check_cancel(cancel.as_ref())?;
        let now = now_micros();
        let mut inventory = Vec::with_capacity(stored_regions.len());
        for (name, stored, metadata, binding_error) in stored_regions {
            check_cancel(cancel.as_ref())?;
            let info = MemoryRegionInfo::new(
                name.clone(),
                stored.encrypted,
                stored.dim,
                stored.metric,
                stored.model_id.clone(),
            );
            let counted = (|| {
                if let Some(error) = binding_error {
                    return Err(MemError::Invalid(error));
                }
                if let Some(mark) = parse_reembed_mark(metadata.as_deref(), stored.id)? {
                    return Err(MemError::Invalid(format!(
                        "region '{name}' stopped during re-embedding to '{}'; resume \
                         reembed_region before using it",
                        mark.to_model
                    )));
                }
                let table = atoms_table(stored.dim, stored.metric, stored.encrypted);
                if stored.encrypted {
                    if !self.db.region_keys_enabled() {
                        return Err(MemError::Core(citadel_core::Error::RegionKeysDisabled));
                    }
                    verify_maintenance_region_key_from_snapshot(
                        &name,
                        &stored,
                        &region_key_records,
                    )?;
                    return maintenance_count_sealed(
                        &self.db,
                        &conn,
                        stored.id,
                        &table,
                        now,
                        cancel.as_ref(),
                    );
                }
                let rows = conn.query_params(
                    &format!(
                        "SELECT COUNT(*) FROM {table} WHERE region_id = $1 \
                         AND (expires_at IS NULL OR expires_at > $2)"
                    ),
                    &[Value::Integer(stored.id), Value::Timestamp(now)],
                )?;
                match rows.rows.first().and_then(|row| row.first()) {
                    Some(Value::Integer(count)) if *count >= 0 => Ok(*count as u64),
                    other => Err(MemError::Invalid(format!(
                        "COUNT returned no non-negative integer: {other:?}"
                    ))),
                }
            })();
            check_cancel(cancel.as_ref())?;
            inventory.push(match counted {
                Ok(count) => MemoryRegionInventory::available(info, count),
                Err(error) => MemoryRegionInventory::from_error(info, error),
            });
        }
        drop(lifecycle);
        inventory.sort_unstable_by(|left, right| left.region().name().cmp(right.region().name()));
        check_cancel(cancel.as_ref())?;
        charge_returned_text(inventory.iter().flat_map(|item| {
            [
                Some(item.region().name()),
                Some(item.region().model_id()),
                item.unavailable(),
            ]
            .into_iter()
            .flatten()
        }))?;
        Ok(inventory)
    }

    /// Count every live, unexpired atom without materializing its content.
    pub fn count_region(&self, region: &str) -> Result<u64> {
        self.with_region_without_content_key(
            region,
            |conn, stored, table, _atom_wrap, cancel, _lifecycle| {
                let now = now_micros();
                let params = [Value::Integer(stored.id), Value::Timestamp(now)];
                if stored.encrypted {
                    return maintenance_count_sealed(&self.db, conn, stored.id, table, now, cancel);
                }
                let qr = conn.query_params(
                    &format!(
                        "SELECT COUNT(*) FROM {table} WHERE region_id = $1 \
                     AND (expires_at IS NULL OR expires_at > $2)"
                    ),
                    &params,
                )?;
                match qr.rows.first().and_then(|row| row.first()) {
                    Some(Value::Integer(count)) if *count >= 0 => Ok(*count as u64),
                    other => Err(MemError::Invalid(format!(
                        "COUNT returned no non-negative integer: {other:?}"
                    ))),
                }
            },
        )
    }

    /// Deterministic id-order listing without embedding or recall.
    pub fn fetch_range(&self, region: &str, query: &FetchQuery) -> Result<Vec<AtomHit>> {
        let mut hits = self.fetch_range_materialized(region, query)?;
        charge_returned_hits(&mut hits)?;
        Ok(hits)
    }

    /// Read exact live atoms in request order without attaching an embedder.
    ///
    /// Missing, expired, or key-erased ids produce `None`. Duplicate ids are
    /// rejected so one stored value cannot be multiplied into unbounded output.
    pub fn fetch_by_ids(&self, region: &str, ids: &[AtomId]) -> Result<Vec<Option<AtomHit>>> {
        validate_fetch_ids(ids)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut found = self.with_region(
            region,
            |conn, stored, table, atom_wrap, cancel, _lifecycle| match atom_wrap {
                Some(atom_wrap) => fetch_atoms_by_ids_sealed(
                    &self.db, conn, stored.id, table, atom_wrap, ids, cancel,
                ),
                None => fetch_atoms_by_ids_plain(conn, stored.id, table, ids, cancel),
            },
        )?;
        let mut ordered = order_fetched_ids(ids, &mut found);
        charge_returned_optional_hits(&mut ordered)?;
        Ok(ordered)
    }

    fn fetch_range_materialized(&self, region: &str, query: &FetchQuery) -> Result<Vec<AtomHit>> {
        if query.limit == 0 {
            return self.with_region_without_content_key(
                region,
                |_conn, _stored, _table, _atom_wrap, _cancel, _lifecycle| Ok(Vec::new()),
            );
        }
        self.with_region(
            region,
            |conn, stored, table, atom_wrap, cancel, _lifecycle| {
                if let Some(atom_wrap) = atom_wrap {
                    return maintenance_fetch_sealed(
                        &self.db, conn, stored.id, table, atom_wrap, query, cancel,
                    );
                }
                maintenance_fetch_plain(conn, stored.id, table, query, cancel)
            },
        )
    }

    /// Read one deterministic page without falsely advertising a terminal cursor.
    pub fn fetch_page(&self, region: &str, query: &FetchQuery) -> Result<FetchPage> {
        let mut page =
            fetch_page_result(query, |paged| self.fetch_range_materialized(region, paged))?;
        charge_returned_hits(&mut page.atoms)?;
        Ok(page)
    }

    /// Re-authenticate the requested ids from stored bytes, in request order.
    pub fn verify_atoms(&self, region: &str, ids: &[AtomId]) -> Result<Vec<AtomAttestation>> {
        if ids.is_empty() {
            return self.with_region_without_content_key(
                region,
                |_conn, _stored, _table, _atom_wrap, _cancel, _lifecycle| Ok(Vec::new()),
            );
        }
        self.with_region(
            region,
            |conn, stored, table, atom_wrap, cancel, _lifecycle| {
                if let Some(atom_wrap) = atom_wrap {
                    return maintenance_verify_sealed(
                        &self.db, conn, stored.id, table, atom_wrap, ids, cancel,
                    );
                }
                maintenance_verify_plain(conn, stored.id, table, ids, cancel)
            },
        )
    }

    /// Forget atoms without requiring the model that produced their vectors.
    ///
    /// Encrypted atoms are key-erased before their rows; plaintext atoms are logical
    /// deletes. After key destruction, residue cleanup ignores cancellation and
    /// completes the receipt. Immutable atoms are returned unless `force` is true.
    pub fn forget_atoms(
        &self,
        region: &str,
        ids: &[AtomId],
        force: bool,
    ) -> Result<ErasureReceipt> {
        self.with_region_irreversible(
            region,
            |conn, stored, table, _atom_wrap, cancel, lifecycle| {
                let encrypted = stored.encrypted;
                let mut targets = ids.to_vec();
                let mut immutable_skipped = Vec::new();
                if !force && !ids.is_empty() {
                    let in_list = id_list(ids);
                    let qr = conn.query_params(
                        &format!(
                            "SELECT id FROM {table} WHERE region_id = $1 \
                         AND id IN ({in_list}) AND immutable = 1"
                        ),
                        &[Value::Integer(stored.id)],
                    )?;
                    let skipped: FxHashSet<AtomId> = qr
                        .rows
                        .iter()
                        .map(|row| {
                            check_cancel(cancel)?;
                            as_int(&row[0])
                        })
                        .collect::<Result<_>>()?;
                    targets.retain(|id| !skipped.contains(id));
                    immutable_skipped = skipped.into_iter().collect();
                    immutable_skipped.sort_unstable();
                }

                check_cancel(cancel)?;
                if targets.is_empty() {
                    return Ok(build_erasure_receipt(
                        encrypted,
                        0,
                        Vec::new(),
                        immutable_skipped,
                    ));
                }

                let delete_layout = maintenance_delete_layout(conn, table, encrypted)?;
                let edges = self.db.memory_edges_lock();
                let in_list = id_list(&targets);
                let atom_slots = if encrypted {
                    atom_key_slots_for(conn, stored.id, table, &in_list)?
                } else {
                    Vec::new()
                };
                if encrypted {
                    let atom_ids = atom_slots
                        .iter()
                        .map(|&(_, atom_id, _)| atom_id)
                        .collect::<Vec<_>>();
                    lifecycle.ensure_memory_atoms_unreserved(&atom_ids)?;
                }
                // Last caller poll. Segment retirement may still cancel before its
                // key commit; after that, cleanup is uncancelled.
                check_cancel(cancel)?;
                let slots_erased = if encrypted {
                    self.retire_sealed_segment(conn, stored.id, table, lifecycle)?;
                    lifecycle
                        .atom_store_tombstone_batch(&atom_slots)?
                        .into_iter()
                        .map(|(slot, atom_id, old_gen, new_gen)| SlotErasure {
                            slot,
                            atom_id: atom_id as AtomId,
                            old_gen,
                            new_gen,
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let rows_deleted = if encrypted {
                    #[cfg(test)]
                    debug_fire_cancel_after_key_erasure();
                    delete_atoms_for_layout_uncancelled_recovery(
                        conn,
                        stored.id,
                        table,
                        &in_list,
                        &edges,
                        delete_layout,
                    )?
                } else {
                    with_write_txn(conn, |write| {
                        delete_atoms_for_layout(
                            write,
                            stored.id,
                            table,
                            &in_list,
                            &edges,
                            delete_layout,
                        )
                    })?
                };
                #[cfg(test)]
                debug_fire_cancel_after_local_work();
                Ok(build_erasure_receipt(
                    encrypted,
                    rows_deleted,
                    slots_erased,
                    immutable_skipped,
                ))
            },
        )
    }

    fn with_region<T>(
        &self,
        region: &str,
        operation: impl FnOnce(
            &Connection<'_>,
            &RegionRow,
            &str,
            Option<&AtomWrapKey>,
            Option<&citadel_core::CancelToken>,
            &KeyLifecycleGuard<'_>,
        ) -> Result<T>,
    ) -> Result<T> {
        self.with_region_impl(region, MaintenanceRegionMode::ReadContent, operation)
    }

    fn with_region_without_content_key<T>(
        &self,
        region: &str,
        operation: impl FnOnce(
            &Connection<'_>,
            &RegionRow,
            &str,
            Option<&AtomWrapKey>,
            Option<&citadel_core::CancelToken>,
            &KeyLifecycleGuard<'_>,
        ) -> Result<T>,
    ) -> Result<T> {
        self.with_region_impl(region, MaintenanceRegionMode::InspectMetadata, operation)
    }

    /// Run an operation whose successful result must survive cancellation after its
    /// irreversible boundary. The operation remains responsible for its final poll before
    /// crossing that boundary.
    fn with_region_irreversible<T>(
        &self,
        region: &str,
        operation: impl FnOnce(
            &Connection<'_>,
            &RegionRow,
            &str,
            Option<&AtomWrapKey>,
            Option<&citadel_core::CancelToken>,
            &KeyLifecycleGuard<'_>,
        ) -> Result<T>,
    ) -> Result<T> {
        self.with_region_impl(
            region,
            MaintenanceRegionMode::IrreversibleErasure,
            operation,
        )
    }

    fn with_region_impl<T>(
        &self,
        region: &str,
        mode: MaintenanceRegionMode,
        operation: impl FnOnce(
            &Connection<'_>,
            &RegionRow,
            &str,
            Option<&AtomWrapKey>,
            Option<&citadel_core::CancelToken>,
            &KeyLifecycleGuard<'_>,
        ) -> Result<T>,
    ) -> Result<T> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let lifecycle = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let stored =
            load_region_row(&conn, &key)?.ok_or_else(|| MemError::RegionNotFound(key.clone()))?;
        if let Some(mark) =
            parse_reembed_mark(read_metadata(&conn, stored.id)?.as_deref(), stored.id)?
        {
            return Err(MemError::Invalid(format!(
                "region '{key}' stopped during re-embedding to '{}'; resume reembed_region \
                 before using it",
                mark.to_model
            )));
        }
        let atom_wrap = if stored.encrypted && mode.loads_content_key() {
            if !self.db.region_keys_enabled() {
                return Err(MemError::Core(citadel_core::Error::RegionKeysDisabled));
            }
            Some(load_maintenance_atom_wrap(&self.db, &key, &stored)?)
        } else {
            if stored.encrypted {
                if !self.db.region_keys_enabled() {
                    return Err(MemError::Core(citadel_core::Error::RegionKeysDisabled));
                }
                verify_maintenance_region_key(&self.db, &key, &stored)?;
            }
            None
        };
        check_cancel(cancel.as_ref())?;
        let table = atoms_table(stored.dim, stored.metric, stored.encrypted);
        let result = operation(
            &conn,
            &stored,
            &table,
            atom_wrap.as_ref(),
            cancel.as_ref(),
            &lifecycle,
        )?;
        if mode.checks_completion_cancel() {
            check_cancel(cancel.as_ref())?;
        }
        Ok(result)
    }

    fn retire_sealed_segment(
        &self,
        conn: &Connection<'_>,
        region_id: RegionId,
        table: &str,
        lifecycle: &KeyLifecycleGuard<'_>,
    ) -> Result<()> {
        retire_sealed_segment_parts(&self.db, conn, region_id, table, lifecycle)
    }
}

impl MemoryEngine {
    /// Open the engine over a database, creating catalog tables if absent.
    pub fn open(db: Arc<Database>) -> Result<Self> {
        {
            let conn = Connection::open(&db)?;
            if let Some(e) = conn.execute_script(BOOTSTRAP_SQL).error {
                return Err(e.into());
            }
            // Reject a legacy pre-erasure schema (memory_regions lacks the
            // `encrypted` column).
            let has_encrypted_col = conn
                .table_schema("memory_regions")
                .is_some_and(|s| s.columns.iter().any(|c| c.name == "encrypted"));
            if !has_encrypted_col {
                return Err(MemError::Invalid(
                    "incompatible memory schema (pre-region-erasure): recreate the database \
                     or export and reimport its memories"
                        .into(),
                ));
            }
        } // drop the connection's borrow before moving `db`
        let regions = Arc::new(Mutex::new(FxHashMap::<String, RegionState>::default()));
        let weak_regions = Arc::downgrade(&regions);
        let region_invalidator: Arc<citadel::MemoryRegionInvalidator> =
            Arc::new(move |region_id| {
                let regions = weak_regions.upgrade()?;
                let detached = {
                    let mut regions = regions.lock().unwrap();
                    let key = regions.iter().find_map(|(key, state)| {
                        (state.id as u64 == region_id).then(|| key.clone())
                    });
                    key.and_then(|key| regions.remove(&key))
                };
                detached.map(|state| {
                    Box::new(retire_region_state(state)) as Box<dyn std::any::Any + Send>
                })
            });
        db.register_memory_region_invalidator(Arc::clone(&region_invalidator));
        let weak_regions = Arc::downgrade(&regions);
        let atom_invalidator: Arc<citadel::MemoryAtomInvalidator> = Arc::new(move |atom_ids| {
            let Some(regions) = weak_regions.upgrade() else {
                return Vec::new();
            };
            let atom_ids: FxHashSet<AtomId> = atom_ids
                .iter()
                .filter_map(|&id| AtomId::try_from(id).ok())
                .collect();
            if atom_ids.is_empty() {
                return Vec::new();
            }
            let caches: Vec<_> = regions
                .lock()
                .unwrap()
                .values()
                .map(|state| Arc::clone(&state.ann))
                .collect();
            caches
                .into_iter()
                .filter_map(|cache| {
                    let mut cache = cache.write().unwrap();
                    cache
                        .as_ref()
                        .is_some_and(|ann| atom_ids.iter().any(|id| ann.cached.contains_key(id)))
                        .then(|| cache.take())
                        .flatten()
                        .map(|ann| Box::new(ann) as Box<dyn std::any::Any + Send>)
                })
                .collect()
        });
        db.register_memory_atom_invalidator(Arc::clone(&atom_invalidator));
        let engine = Self {
            db,
            regions,
            _region_invalidator: region_invalidator,
            _atom_invalidator: atom_invalidator,
            reranker: RwLock::new(None),
            attach_max: Mutex::new(FxHashMap::default()),
            access_stats: Mutex::new(FxHashMap::default()),
        };
        if engine.db.region_keys_enabled() && engine.db.region_store_path().exists() {
            engine.reconcile_region_store()?;
        }
        if engine.db.region_keys_enabled() && engine.db.atom_store_path().exists() {
            engine.reconcile_atom_store()?;
        }
        engine.reconcile_identity_records()?;
        engine.reconcile_similarity_state()?;
        Ok(engine)
    }

    /// Reclaim slots left live by an interrupted create or bound at a drifted generation.
    fn reconcile_region_store(&self) -> Result<()> {
        // Serialize against in-flight allocate->commit spans on other handles.
        let _kl = self.db.key_lifecycle_lock();
        let live = self.db.region_store_live_bindings()?;
        if live.is_empty() {
            return Ok(());
        }
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            "SELECT id, rsk_slot, rsk_gen FROM memory_regions \
             WHERE encrypted = 1 AND rsk_slot IS NOT NULL",
            &[],
        )?;
        // (slot, owner) -> required gen; a gen-less row fails closed and is reclaimed.
        let mut valid: FxHashMap<(u32, u64), u64> = FxHashMap::default();
        for row in &qr.rows {
            // Checked domains: out-of-range values never truncate into another binding.
            let owner = as_int(&row[0]).ok().and_then(|v| u64::try_from(v).ok());
            let slot = as_int(&row[1]).ok().and_then(|v| u32::try_from(v).ok());
            if let ((Some(owner), Some(slot)), Some(gen)) = ((owner, slot), opt_u64(&row[2])?) {
                valid.insert((slot, owner), gen);
            }
        }
        for (slot, owner, gen) in live {
            if valid.get(&(slot, owner)) != Some(&gen) {
                _kl.region_store_tombstone(slot, owner, gen)?;
            }
        }
        Ok(())
    }

    /// Two-way reconcile: tombstone live key slots no committed row references
    /// (interrupted insert), and finish interrupted erasures by deleting rows
    /// whose key is already dead (keys die first, so a crash leaves
    /// undecryptable rows to clean up here).
    fn reconcile_atom_store(&self) -> Result<()> {
        // Serialize against in-flight allocate->commit spans on other handles.
        let _kl = self.db.key_lifecycle_lock();
        let _edges_guard = self.db.memory_edges_lock();
        // Phase 0 - normalize torn erases before any row or slot binding is forgotten.
        _kl.normalize_atom_store_torn_erases()?;
        let conn = Connection::open(&self.db)?;

        // Phase 1 - inventory only; a live slot attests its exact (owner, gen) alone.
        let live = self.db.atom_store_live_bindings()?;
        let live_gens: FxHashMap<(u32, u64), u64> =
            live.iter().map(|&(s, o, g)| ((s, o), g)).collect();

        // Valid parent = region row whose key binding is exactly live (attachment's bar).
        let region_live: FxHashMap<(u32, u64), u64> = if self.db.region_store_path().exists() {
            self.db
                .region_store_live_bindings()?
                .into_iter()
                .map(|(slot, owner, gen)| ((slot, owner), gen))
                .collect()
        } else {
            FxHashMap::default()
        };
        let regions = conn.query_params(
            "SELECT id, embedding_dim, embedding_metric, rsk_slot, rsk_gen, model_id, \
             CAST(metadata AS TEXT) \
             FROM memory_regions WHERE encrypted = 1",
            &[],
        )?;
        let mut region_tables: FxHashMap<RegionId, String> = FxHashMap::default();
        // A shape-changing re-embed moves converted rows to the destination
        // table BEFORE the row that describes the region is updated, so a
        // region interrupted part-way legitimately has rows in two tables. The
        // row's own shape names only the source; a reconcile that knew nothing
        // else would read every converted row as belonging to no region, and
        // orphan here means the atom's key is destroyed and its row deleted.
        let mut migrating_to: FxHashMap<RegionId, String> = FxHashMap::default();
        for row in &regions.rows {
            let dim = u16::try_from(as_int(&row[1])?)
                .map_err(|_| MemError::Invalid("stored embedding_dim out of range".into()))?;
            let metric = metric_from_str(as_text(&row[2])?)?;
            let id = as_int(&row[0])?;
            let row_generation = opt_u64(&row[4])?;
            let bound = as_int(&row[3])
                .ok()
                .and_then(|v| u32::try_from(v).ok())
                .zip(u64::try_from(id).ok())
                .and_then(|key| region_live.get(&key))
                .is_some_and(|&gen| row_generation == Some(gen));
            if bound {
                let home = atoms_table(dim, metric, true);
                let metadata = match &row[6] {
                    Value::Text(value) => Some(value.as_str()),
                    Value::Null => None,
                    _ => {
                        return Err(MemError::Invalid(format!(
                            "memory region {id} has unreadable metadata"
                        )))
                    }
                };
                if let Some(mark) = parse_reembed_mark(metadata, id)? {
                    validate_repair_mark(&mark, as_text(&row[5])?, dim, metric, id)?;
                    let destination =
                        atoms_table(mark.to_dim, metric_from_str(&mark.to_metric)?, true);
                    if destination != home {
                        migrating_to.insert(id, destination);
                    }
                }
                region_tables.insert(id, home);
            }
        }
        // Catalog-driven table list: rows of a removed parent region still scan.
        let enc_tables: FxHashSet<String> = conn
            .tables()
            .into_iter()
            .filter(|name| is_encrypted_atoms_table(name))
            .collect();

        // Valid row = exact live binding AND a parent homed in this table; else orphan.
        let mut row_claims: FxHashSet<(u32, u64)> = FxHashSet::default();
        let mut valid_ids: FxHashSet<AtomId> = FxHashSet::default();
        let mut table_orphans: Vec<(String, Vec<AtomId>)> = Vec::new();
        let mut orphan_rids: FxHashSet<RegionId> = FxHashSet::default();
        for table in &enc_tables {
            let qr = conn.query_params(
                &format!("SELECT id, key_slot, region_id, key_gen FROM {table}"),
                &[],
            )?;
            let mut orphans: Vec<AtomId> = Vec::new();
            for r in &qr.rows {
                let id = as_int(&r[0])?;
                let rid = as_int(&r[2])?;
                // Out-of-range slot/gen/id is an orphan, never a truncated alias.
                let slot = as_int(&r[1]).ok().and_then(|v| u32::try_from(v).ok());
                let owner = u64::try_from(id).ok();
                let gen = as_int(&r[3]).ok().and_then(|v| u64::try_from(v).ok());
                match slot.zip(owner).zip(gen) {
                    Some(((slot, owner), gen))
                        if live_gens.get(&(slot, owner)) == Some(&gen)
                            && (region_tables.get(&rid) == Some(table)
                                || migrating_to.get(&rid) == Some(table)) =>
                    {
                        row_claims.insert((slot, owner));
                        valid_ids.insert(id);
                    }
                    _ => {
                        orphans.push(id);
                        orphan_rids.insert(rid);
                    }
                }
            }
            if !orphans.is_empty() {
                table_orphans.push((table.clone(), orphans));
            }
        }

        // Segments from metadata AND the physical catalog, so parentless trees are found.
        let qr = conn.query_params("SELECT key FROM memory_meta WHERE key LIKE 'annseg_%'", &[])?;
        let mut seg_rids: FxHashSet<RegionId> = FxHashSet::default();
        for row in &qr.rows {
            if let Some((_, region)) = as_text(&row[0])?.split_once(':') {
                if let Ok(rid) = region.parse::<RegionId>() {
                    seg_rids.insert(rid);
                }
            }
        }
        let mut inv_trees: Vec<(RegionId, String)> = Vec::new();
        for name in self.db.table_names()? {
            let Ok(name) = String::from_utf8(name) else {
                continue;
            };
            if let Some((rid, _)) = parse_sealed_segment_table(&name) {
                seg_rids.insert(rid);
                inv_trees.push((rid, name));
            }
        }
        let mut seg_cleanup: Vec<(RegionId, Option<String>)> = Vec::new();
        let mut seg_claims: FxHashSet<(u32, u64)> = FxHashSet::default();
        let mut valid_seg_trees: FxHashMap<RegionId, String> = FxHashMap::default();
        for &rid in &seg_rids {
            let seg_table = region_tables
                .get(&rid)
                .filter(|table| enc_tables.contains(*table))
                .map(|table| sealed_segment_table(table, rid));
            // Malformed binding fields claim no key. Reconciliation treats them
            // as cleanup work; operational reads and retirement fail closed.
            let meta = match read_annseg_meta(&conn, rid) {
                Ok(meta) => meta,
                Err(MemError::Invalid(_)) => None,
                Err(error) => return Err(error),
            };
            let is_valid = match (&meta, &seg_table) {
                (Some((slot, gen, id)), Some(seg_table)) => {
                    let key = (*slot, *id);
                    // Single-owner: colliding metadata loses; the atom's record stays.
                    !row_claims.contains(&key)
                        && !seg_claims.contains(&key)
                        && !orphan_rids.contains(&rid)
                        && live_gens.get(&key) == Some(gen)
                        && self.sealed_segment_tree_state(seg_table)? == SegmentTreeState::Present
                }
                _ => false,
            };
            if is_valid {
                let (slot, _, id) = meta.expect("validity requires complete metadata");
                seg_claims.insert((slot, id));
                valid_seg_trees.insert(rid, seg_table.expect("validity requires a known table"));
            } else {
                seg_cleanup.push((rid, seg_table));
            }
        }

        // Phase 2 - unclaimed live keys die FIRST, before any derived data or row.
        let doomed: Vec<(u32, u64, u64)> = live
            .iter()
            .copied()
            .filter(|&(slot, owner, _)| {
                !row_claims.contains(&(slot, owner)) && !seg_claims.contains(&(slot, owner))
            })
            .collect();
        if !doomed.is_empty() {
            _kl.atom_store_tombstone_batch(&doomed)?;
        }
        #[cfg(test)]
        if FAIL_RECONCILE_AFTER_DRIFT_KEYS.with(std::cell::Cell::take) {
            return Err(MemError::Invalid(
                "injected reconcile failure after key destruction".into(),
            ));
        }

        // Phase 3 - trees first, meta only after, so an interrupted cleanup can retry.
        for (rid, seg_table) in &seg_cleanup {
            for (_, name) in inv_trees.iter().filter(|(tree_rid, _)| tree_rid == rid) {
                self.drop_segment_tree(name)?;
            }
            if let Some(name) = seg_table {
                self.drop_segment_tree(name)?;
            }
            clear_annseg_meta(&conn, *rid)?;
        }
        // A stale foreign tree under a valid segment's region id is residue.
        for (rid, name) in &inv_trees {
            if valid_seg_trees
                .get(rid)
                .is_some_and(|expected| expected != name)
            {
                self.drop_segment_tree(name)?;
            }
        }

        // Phase 4 - rows; a forged duplicate id loses only its row, the graph survives.
        for (table, orphan_ids) in &table_orphans {
            let dead: Vec<AtomId> = orphan_ids
                .iter()
                .copied()
                .filter(|id| !valid_ids.contains(id))
                .collect();
            let dead_list = dead
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let in_list = orphan_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            with_write_txn(&conn, |c| {
                if !dead.is_empty() {
                    c.execute_params(
                        &format!("DELETE FROM memory_idempotency WHERE atom_id IN ({dead_list})"),
                        &[],
                    )?;
                    delete_similarity_state_for_atom_list(c, &dead_list)?;
                    c.execute_params(
                        &format!(
                            "DELETE FROM memory_edges WHERE src_id IN ({dead_list}) \
                             OR dst_id IN ({dead_list})"
                        ),
                        &[],
                    )?;
                }
                c.execute_params(&format!("DELETE FROM {table} WHERE id IN ({in_list})"), &[])?;
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Open-time sweep: identity records whose region or atom row is gone
    /// (removed by an older binary or direct SQL) are deleted; the lazy
    /// self-heal only covers retried keys. Expired atoms keep their records
    /// (expiry is the lookup's TTL semantics, not an orphan state).
    fn reconcile_identity_records(&self) -> Result<()> {
        let conn = Connection::open(&self.db)?;
        let idents = conn.query_params("SELECT region_id, atom_id FROM memory_idempotency", &[])?;
        if idents.rows.is_empty() {
            return Ok(());
        }
        let mut by_region: FxHashMap<RegionId, Vec<AtomId>> = FxHashMap::default();
        for row in &idents.rows {
            by_region
                .entry(as_int(&row[0])?)
                .or_default()
                .push(as_int(&row[1])?);
        }
        let regions = conn.query_params(
            "SELECT id, embedding_dim, embedding_metric, encrypted, model_id, \
             CAST(metadata AS TEXT) FROM memory_regions",
            &[],
        )?;
        // Both tables, for the same reason the encrypted reconcile needs both:
        // a region part-way through a shape-changing re-embed has its converted
        // rows in the destination while the row still names the source. Looking
        // only at the source finds none of them and deletes their identity
        // records, which is what makes a repeated `remember` write a duplicate.
        let mut tables: FxHashMap<RegionId, Vec<String>> = FxHashMap::default();
        for row in &regions.rows {
            let dim = u16::try_from(as_int(&row[1])?)
                .map_err(|_| MemError::Invalid("stored embedding_dim out of range".into()))?;
            let metric = metric_from_str(as_text(&row[2])?)?;
            let encrypted = as_exact_bool(&row[3], "encrypted")?;
            let region_id = as_int(&row[0])?;
            let home = atoms_table(dim, metric, encrypted);
            let mut homes = vec![home.clone()];
            let metadata = match &row[5] {
                Value::Text(value) => Some(value.as_str()),
                Value::Null => None,
                _ => {
                    return Err(MemError::Invalid(format!(
                        "memory region {region_id} has unreadable metadata"
                    )))
                }
            };
            if let Some(mark) = parse_reembed_mark(metadata, region_id)? {
                validate_repair_mark(&mark, as_text(&row[4])?, dim, metric, region_id)?;
                let destination =
                    atoms_table(mark.to_dim, metric_from_str(&mark.to_metric)?, encrypted);
                if destination != home {
                    homes.push(destination);
                }
            }
            tables.insert(region_id, homes);
        }
        let mut orphans: Vec<(RegionId, Vec<AtomId>)> = Vec::new();
        for (region_id, atom_ids) in by_region {
            let in_list = atom_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            // The region row or its tables are gone: all records orphaned.
            let mut live: FxHashSet<AtomId> = FxHashSet::default();
            for table in tables.get(&region_id).into_iter().flatten() {
                if conn.table_schema(table).is_none() {
                    continue;
                }
                let qr = conn.query_params(
                    &format!("SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list})"),
                    &[Value::Integer(region_id)],
                )?;
                for r in &qr.rows {
                    live.insert(as_int(&r[0])?);
                }
            }
            let gone: Vec<AtomId> = atom_ids
                .into_iter()
                .filter(|id| !live.contains(id))
                .collect();
            if !gone.is_empty() {
                orphans.push((region_id, gone));
            }
        }
        if orphans.is_empty() {
            return Ok(());
        }
        // Only the proven (region, atom) pairs - a global atom_id match
        // could sweep a malformed duplicate in another region.
        with_write_txn(&conn, |c| {
            for (region_id, atom_ids) in &orphans {
                let in_list = atom_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                c.execute_params(
                    &format!(
                        "DELETE FROM memory_idempotency \
                         WHERE region_id = $1 AND atom_id IN ({in_list})"
                    ),
                    &[Value::Integer(*region_id)],
                )?;
            }
            Ok(())
        })
    }

    /// Remove managed-similarity sidecars whose atom or policy disappeared.
    /// Untracked `SimilarTo` rows remain authored data; only pairs that still
    /// carry explicit engine ownership may be deleted automatically.
    fn reconcile_similarity_state(&self) -> Result<()> {
        let _kl = self.db.key_lifecycle_lock();
        let _edges_guard = self.db.memory_edges_lock();
        let conn = Connection::open(&self.db)?;
        let regions = conn.query(
            "SELECT id, embedding_dim, embedding_metric, model_id, encrypted, rsk_slot, rsk_gen \
             FROM memory_regions",
        )?;
        let mut region_tables = FxHashMap::default();
        for values in &regions.rows {
            let row = parse_region_row(values)?;
            region_tables.insert(row.id, self.region_atom_tables(&conn, &row)?);
        }

        let mut invalid_policies = FxHashSet::default();
        let mut delete_owned = Vec::new();
        let mut after = i64::MIN;
        loop {
            let page = conn.query_params(
                &format!(
                    "SELECT region_id, src_id FROM memory_similarity_policies \
                     WHERE src_id > $1 ORDER BY src_id LIMIT {REEMBED_BATCH}"
                ),
                &[Value::Integer(after)],
            )?;
            let Some(last) = page.rows.last() else {
                break;
            };
            after = as_int(&last[1])?;

            let mut by_region: FxHashMap<RegionId, Vec<AtomId>> = FxHashMap::default();
            for row in &page.rows {
                by_region
                    .entry(as_int(&row[0])?)
                    .or_default()
                    .push(as_int(&row[1])?);
            }
            for (region_id, sources) in by_region {
                let Some(tables) = region_tables.get(&region_id) else {
                    invalid_policies.extend(sources);
                    continue;
                };
                let live_sources = live_region_atom_ids(&conn, tables, region_id, &sources)?;
                invalid_policies.extend(
                    sources
                        .iter()
                        .filter(|src| !live_sources.contains(src))
                        .copied(),
                );
                let valid_sources: Vec<_> = sources
                    .into_iter()
                    .filter(|src| live_sources.contains(src))
                    .collect();
                if valid_sources.is_empty() {
                    continue;
                }
                let tracked = conn.query(&format!(
                    "SELECT src_id, dst_id FROM memory_similarity_edges WHERE src_id IN ({})",
                    id_list(&valid_sources)
                ))?;
                let mut destinations = Vec::with_capacity(tracked.rows.len());
                let mut pairs = Vec::with_capacity(tracked.rows.len());
                for row in &tracked.rows {
                    let pair = (as_int(&row[0])?, as_int(&row[1])?);
                    destinations.push(pair.1);
                    pairs.push(pair);
                }
                destinations.sort_unstable();
                destinations.dedup();
                let live_destinations =
                    live_region_atom_ids(&conn, tables, region_id, &destinations)?;
                delete_owned.extend(
                    pairs
                        .into_iter()
                        .filter(|(_, dst)| !live_destinations.contains(dst)),
                );
            }
        }

        with_write_txn(&conn, |c| {
            for &(src, dst) in &delete_owned {
                c.execute_params(
                    "DELETE FROM memory_edges WHERE src_id = $1 AND dst_id = $2 \
                     AND kind = 'similar_to'",
                    &[Value::Integer(src), Value::Integer(dst)],
                )?;
            }
            for &(src, dst) in &delete_owned {
                c.execute_params(
                    "DELETE FROM memory_similarity_edges WHERE src_id = $1 AND dst_id = $2",
                    &[Value::Integer(src), Value::Integer(dst)],
                )?;
            }
            let mut invalid: Vec<_> = invalid_policies.iter().copied().collect();
            invalid.sort_unstable();
            for sources in invalid.chunks(REEMBED_BATCH) {
                let ids = id_list(sources);
                let owned = c.query(&format!(
                    "SELECT src_id, dst_id FROM memory_similarity_edges \
                     WHERE src_id IN ({ids})"
                ))?;
                for row in &owned.rows {
                    c.execute_params(
                        "DELETE FROM memory_edges WHERE src_id = $1 AND dst_id = $2 \
                         AND kind = 'similar_to'",
                        &[
                            Value::Integer(as_int(&row[0])?),
                            Value::Integer(as_int(&row[1])?),
                        ],
                    )?;
                }
                c.execute(&format!(
                    "DELETE FROM memory_similarity_edges WHERE src_id IN ({ids})"
                ))?;
                c.execute(&format!(
                    "DELETE FROM memory_similarity_policies WHERE src_id IN ({ids})"
                ))?;
            }
            c.execute(
                "DELETE FROM memory_similarity_edges WHERE src_id NOT IN \
                 (SELECT src_id FROM memory_similarity_policies)",
            )?;
            Ok(())
        })
    }

    /// Record recall hits for `Lru`/`Stale` eviction. In-process and
    /// write-free, so recall never serializes behind the writer.
    fn note_access(
        &self,
        region_id: RegionId,
        ids: impl IntoIterator<Item = AtomId>,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<()> {
        let now = now_micros();
        let mut stats = self.access_stats.lock().unwrap();
        let region = stats.entry(region_id).or_default();
        for id in ids {
            check_cancel(cancel)?;
            let entry = region.entry(id).or_insert((0, 0));
            entry.0 = now;
            entry.1 += 1;
        }
        check_cancel(cancel)?;
        Ok(())
    }

    /// Attach a cross-encoder reranker for later `recall`s, per `strategy`.
    pub fn set_reranker(&self, reranker: Arc<dyn Reranker>, strategy: RerankStrategy) {
        let previous = self.reranker.write().unwrap().replace((reranker, strategy));
        drop(previous);
    }

    /// Detach any reranker so subsequent `recall`s use linear fusion only.
    pub fn clear_reranker(&self) {
        let previous = self.reranker.write().unwrap().take();
        drop(previous);
    }

    /// Get-or-create a plaintext region bound to `embedder` (dim/metric/model
    /// must match).
    pub fn create_region(&self, name: &str, embedder: Arc<dyn Embedder>) -> Result<RegionId> {
        let cancel = check_db_cancel(&self.db)?;
        self.create_region_inner(name, embedder, false, cancel.as_ref())
    }

    /// Get-or-create an encrypted region: each atom sealed under its own random
    /// key (ACK) wrapped by a per-region key. `drop_region`/`forget_atom` erase
    /// the region/one atom. Requires `enable_region_keys(true)`.
    pub fn create_encrypted_region(
        &self,
        name: &str,
        embedder: Arc<dyn Embedder>,
    ) -> Result<RegionId> {
        let cancel = check_db_cancel(&self.db)?;
        if !self.db.region_keys_enabled() {
            return Err(MemError::Core(citadel_core::Error::RegionKeysDisabled));
        }
        self.create_region_inner(name, embedder, true, cancel.as_ref())
    }

    fn create_region_inner(
        &self,
        name: &str,
        embedder: Arc<dyn Embedder>,
        encrypted: bool,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<RegionId> {
        let key = name.to_ascii_lowercase();
        let dim = checked_embedder_dim(embedder.as_ref())?;
        let metric = embedder.metric();
        let model_id = normalize_model_id(embedder.model_id())?;
        check_cancel(cancel)?;

        // Region incarnation is persisted state, not a property of this
        // engine's local attachment map. Hold the lifecycle guard across the
        // lookup/create and state replacement so a cross-engine drop cannot
        // interleave, and always consult the row before accepting a cached
        // handle (drop + recreate binds the same name to a fresh id).
        let mut retired_states = Vec::new();
        let _kl = self.db.key_lifecycle_lock();
        check_cancel(cancel)?;
        let conn = Connection::open(&self.db)?;
        // A fresh region has no atoms; only a re-attach needs the MAX(id) scan.
        let (id, keys, init_max) = match self.load_region_row(&conn, &key)? {
            Some(existing) => {
                // Refused rather than served: a marked region holds vectors from
                // two models at once, so recall would rank across vector spaces
                // and report nothing wrong. Naming the call that finishes it is
                // what turns a silent half-migration into an instructed one.
                if let Some(mark) = self.read_reembed_mark(&conn, existing.id)? {
                    return Err(MemError::Invalid(format!(
                        "region '{key}' stopped during re-embedding to '{}'; resume \
                         reembed_region before using it",
                        mark.to_model
                    )));
                }
                let (attached, stale) = self.check_attached_incarnation(
                    &key, &existing, dim, metric, &model_id, encrypted,
                )?;
                if stale {
                    if let Some(state) = self.take_attached_region(&key, None) {
                        retired_states.push(state);
                    }
                }
                if attached.is_some() && encrypted {
                    if let Err(err) = self.verify_region_key_live(&key, &existing) {
                        if let Some(state) = self.take_attached_region(&key, Some(existing.id)) {
                            retired_states.push(state);
                        }
                        return Err(err);
                    }
                }
                if let Some(id) = attached {
                    check_cancel(cancel)?;
                    return Ok(id);
                }
                existing.verify_matches(&key, dim, metric, &model_id, encrypted)?;
                let keys = if encrypted {
                    Some(self.attach_region_key(&key, &existing)?)
                } else {
                    None
                };
                let table = atoms_table(dim, metric, keys.is_some());
                let max = self.reattach_max_id(&conn, &table, existing.id, cancel)?;
                check_cancel(cancel)?;
                (existing.id, keys, max)
            }
            None if encrypted => {
                check_cancel(cancel)?;
                let (id, keys) =
                    self.insert_encrypted_region(&conn, &key, dim, metric, &model_id, &_kl)?;
                (id, keys, 0)
            }
            None => {
                check_cancel(cancel)?;
                (
                    self.insert_region(&conn, &key, dim, metric, &model_id)?,
                    None,
                    0,
                )
            }
        };

        let (atom_wrap, identity_mac) = match keys {
            Some(k) => (Some(k.atom_wrap), Some(k.identity_mac)),
            None => (None, None),
        };
        let replaced = self.regions.lock().unwrap().insert(
            key,
            RegionState {
                id,
                dim,
                metric,
                embedder,
                model_id: Arc::from(model_id.as_str()),
                atom_wrap,
                identity_mac,
                ann: Arc::new(RwLock::new(None)),
                max_id: Arc::new(AtomicI64::new(init_max)),
            },
        );
        if let Some(state) = replaced {
            retired_states.push(retire_region_state(state));
        }
        Ok(id)
    }

    /// Fail-if-absent attach: reuse preflights must never create or probe via TOCTOU.
    pub fn attach_existing_region(
        &self,
        name: &str,
        embedder: Arc<dyn Embedder>,
    ) -> Result<RegionId> {
        let cancel = check_db_cancel(&self.db)?;
        self.attach_existing_region_inner(name, embedder, cancel.as_ref(), false)
    }

    fn attach_reembed_region(
        &self,
        name: &str,
        embedder: Arc<dyn Embedder>,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<RegionId> {
        self.attach_existing_region_inner(name, embedder, cancel, true)
    }

    fn attach_existing_region_inner(
        &self,
        name: &str,
        embedder: Arc<dyn Embedder>,
        cancel: Option<&citadel_core::CancelToken>,
        allow_repair: bool,
    ) -> Result<RegionId> {
        let key = name.to_ascii_lowercase();
        let dim = checked_embedder_dim(embedder.as_ref())?;
        let metric = embedder.metric();
        let model_id = normalize_model_id(embedder.model_id())?;
        check_cancel(cancel)?;

        // One lifecycle span so a concurrent drop_region orders around this attach.
        let mut retired_states = Vec::new();
        let _kl = self.db.key_lifecycle_lock();
        check_cancel(cancel)?;
        let conn = Connection::open(&self.db)?;
        let Some(existing) = self.load_region_row(&conn, &key)? else {
            // A failed attach must not leave a stale local map entry usable by reads.
            if let Some(state) = self.take_attached_region(&key, None) {
                retired_states.push(state);
            }
            return Err(MemError::RegionNotFound(key));
        };
        // Refused rather than served: a marked region holds vectors from two
        // models at once, so recall would rank across vector spaces and report
        // nothing wrong. Naming the call that finishes it turns a silent
        // half-migration into an instructed one.
        if let Some(mark) = self.read_reembed_mark(&conn, existing.id)? {
            if allow_repair {
                validate_repair_mark(
                    &mark,
                    &existing.model_id,
                    existing.dim,
                    existing.metric,
                    existing.id,
                )?;
            }
            if mark.phase != ReembedPhase::Repair || !allow_repair {
                return Err(MemError::Invalid(format!(
                    "region '{key}' stopped during re-embedding to '{}'; resume reembed_region \
                     before using it",
                    mark.to_model
                )));
            }
        }
        let encrypted = existing.encrypted;
        if encrypted && !self.db.region_keys_enabled() {
            return Err(MemError::Core(citadel_core::Error::RegionKeysDisabled));
        }
        // Already attached in this process (existence is proven above).
        let (attached, stale) =
            self.check_attached_incarnation(&key, &existing, dim, metric, &model_id, encrypted)?;
        if stale {
            if let Some(state) = self.take_attached_region(&key, None) {
                retired_states.push(state);
            }
        }
        if attached.is_some() && encrypted {
            if let Err(err) = self.verify_region_key_live(&key, &existing) {
                if let Some(state) = self.take_attached_region(&key, Some(existing.id)) {
                    retired_states.push(state);
                }
                return Err(err);
            }
        }
        if let Some(id) = attached {
            check_cancel(cancel)?;
            return Ok(id);
        }
        existing.verify_matches(&key, dim, metric, &model_id, encrypted)?;
        let (atom_wrap, identity_mac) = if encrypted {
            let k = self.attach_region_key(&key, &existing)?;
            (Some(k.atom_wrap), Some(k.identity_mac))
        } else {
            (None, None)
        };
        let table = atoms_table(dim, metric, atom_wrap.is_some());
        let init_max = self.reattach_max_id(&conn, &table, existing.id, cancel)?;
        check_cancel(cancel)?;
        let replaced = self.regions.lock().unwrap().insert(
            key,
            RegionState {
                id: existing.id,
                dim,
                metric,
                embedder,
                model_id: Arc::from(model_id.as_str()),
                atom_wrap,
                identity_mac,
                ann: Arc::new(RwLock::new(None)),
                max_id: Arc::new(AtomicI64::new(init_max)),
            },
        );
        if let Some(state) = replaced {
            retired_states.push(retire_region_state(state));
        }
        Ok(existing.id)
    }

    /// Correct which model a region says produced its vectors, touching none of
    /// them.
    ///
    /// The counterpart to `reembed_region`, for the opposite fault. There the
    /// vectors are wrong and must be recomputed; here they are right and only
    /// the label is false. Re-embedding would be both wasteful and, for a region
    /// whose atoms carry vectors but no text, impossible.
    ///
    /// The fault this exists to repair is one the shipped integrations wrote.
    /// An integration that embeds caller-side still had to hand
    /// `create_encrypted_region` an embedder, so it passed a `MockEmbedder` it
    /// never called - and the region recorded `model_id = "mock"` over genuine
    /// vectors from a real model. Nothing about those stores is damaged; their
    /// provenance simply lies.
    ///
    /// **Deliberately not automatic.** The historical `"mock"` label identifies
    /// both genuine old `MockEmbedder` vectors and integrations that mislabelled
    /// real vectors. Only the caller knows whether to name the current mock
    /// pipeline, name the real model, or re-embed. The engine refuses a
    /// mismatched attach with a message naming this call instead of guessing.
    ///
    /// An attachment the new label contradicts is detached. Re-attach with the
    /// model the region now names.
    pub fn reclassify_region(&self, name: &str, model_id: String) -> Result<()> {
        let cancel = check_db_cancel(&self.db)?;
        let key = name.to_ascii_lowercase();
        let recorded = normalize_model_id(&model_id)?;

        // The same span drop_region holds: this rewrites the region row, and a
        // concurrent drop must order around it rather than interleave.
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let Some(row) = self.load_region_row(&conn, &key)? else {
            return Err(MemError::RegionNotFound(key));
        };
        if _kl.memory_region_active(row.id as u64) {
            return Err(MemError::Core(citadel_core::Error::RegionInUse {
                region_id: row.id as u64,
            }));
        }
        // A relabel is not a way past a half-migration. Renaming the target
        // model would make the resume take the "already on this model" early
        // return, leaving the mark standing and the region unattachable with
        // nothing able to finish it.
        if let Some(mark) = self.read_reembed_mark(&conn, row.id)? {
            return Err(MemError::Invalid(format!(
                "region '{key}' is part-way through a re-embed to '{}'; finish or resume that \
                 with reembed_region before relabelling it",
                mark.to_model
            )));
        }
        check_cancel(cancel.as_ref())?;

        conn.execute_params(
            "UPDATE memory_regions SET model_id = $1 WHERE id = $2",
            &[Value::Text(recorded.clone().into()), Value::Integer(row.id)],
        )?;
        let retired = if row.model_id == recorded {
            Vec::new()
        } else {
            self.db.drain_memory_region_caches(row.id as u64)
        };
        drop(_kl);
        drop(retired);
        Ok(())
    }

    /// The `SimilarTo` web is rebuilt over the new vectors rather than left
    /// asserting a nearness the old space implied and this one does not.
    ///
    /// Recall quality is **not** preserved and cannot be: a new model is a new
    /// vector space with different neighbours, so `FusionWeights`, thresholds
    /// and any `RecallProfile` calibration tuned on the old model no longer
    /// describe this region. Only re-measurement fixes that.
    pub fn reembed_region(
        &self,
        name: &str,
        new_embedder: Arc<dyn Embedder>,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<ReembedReport> {
        // The explicit token governs memory-side phase checks. With `None`, a
        // snapshot of the handle token does. Database statements always retain
        // the handle-wide cancellation policy as an additional stop source.
        let database_cancel = self.db.cancel_token();
        let cancel = cancel.or(database_cancel.as_ref());
        check_cancel(cancel)?;
        let key = name.to_ascii_lowercase();
        let mut migration = self.migrate_region_vectors(&key, Arc::clone(&new_embedder), cancel)?;
        check_cancel(cancel)?;
        let rebuild = if let Some(expected_repair) = migration.repair.as_ref() {
            // Repair is the only code allowed to attach and read a region while
            // its repair mark stands. Always pass through the durable attach
            // check so another engine's cached pre-migration handle is evicted.
            self.attach_reembed_region(&key, new_embedder, cancel)?;
            let h = self.region_handle(&key)?;
            migration.report.ann_rebuilt =
                self.rebuild_region_ann(&h.table, h.atom_wrap.is_some(), cancel)?;
            check_cancel(cancel)?;

            // The repair and retirement are one edge-mutation span. Releasing
            // between them lets an edge writer win the single-writer slot, make
            // this call fail while its graph is already repaired, and publish a
            // new edge under a still-standing repair mark.
            let kl = self.db.key_lifecycle_lock();
            let edges_guard = self.db.memory_edges_lock();
            let conn = Connection::open(&self.db)?;
            let rebuild = match self.read_reembed_mark(&conn, migration.region_id)? {
                None => WeaveRebuild::default(),
                Some(current) if current == *expected_repair => {
                    let rebuild = self.reweave_similarity_edges(&key, cancel, &kl, &edges_guard)?;

                    // Retire the mark only after the graph is durable. A
                    // cancellation before this point leaves an idempotent
                    // repair for the next call.
                    check_cancel(cancel)?;
                    clear_reembed_mark(&conn, migration.region_id)?;
                    rebuild
                }
                Some(_) => {
                    return Err(MemError::Invalid(format!(
                        "region '{key}' changed its re-embed repair while this retry was waiting"
                    )))
                }
            };
            rebuild
        } else {
            WeaveRebuild::default()
        };

        migration.report.similarity_edges_rewoven = rebuild.rewoven;
        migration.report.similarity_edges_cleared = rebuild.cleared;
        Ok(migration.report)
    }

    /// Put the region's atoms on the new model's vectors.
    ///
    /// Holds the key lifecycle capability while inspecting or mutating durable
    /// region state, but releases it around the external embedder callback.
    /// The vectors-phase mark closes the target region during that gap, and the
    /// exact row and checkpoint are revalidated after every callback.
    fn migrate_region_vectors(
        &self,
        key: &str,
        new_embedder: Arc<dyn Embedder>,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Migration> {
        let new_dim = checked_embedder_dim(new_embedder.as_ref())?;
        let new_metric = new_embedder.metric();
        let new_model = normalize_model_id(new_embedder.model_id())?;

        // Durable inspection and mutation are serialized with key destruction.
        // The guard is released only for the external callback below, then
        // reacquired before its result can affect storage.
        let mut key_lifecycle = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let Some(row) = self.load_region_row(&conn, key)? else {
            return Err(MemError::RegionNotFound(key.into()));
        };
        if key_lifecycle.memory_region_active(row.id as u64) {
            return Err(MemError::Core(citadel_core::Error::RegionInUse {
                region_id: row.id as u64,
            }));
        }

        let standing = self.read_reembed_mark(&conn, row.id)?;
        if let Some(mark) = standing.as_ref() {
            validate_repair_mark(mark, &row.model_id, row.dim, row.metric, row.id)?;
        }

        // A matching repair checkpoint still has graph work to resume.
        if row.dim == new_dim && row.metric == new_metric && row.model_id == new_model {
            if matches!(&standing, Some(mark) if mark.phase == ReembedPhase::Vectors) {
                return Err(MemError::Invalid(format!(
                    "memory region {} has a vectors-phase re-embed mark but already records its \
                     target provenance",
                    row.id
                )));
            }
            return Ok(Migration {
                report: ReembedReport {
                    model_id: new_model,
                    ..Default::default()
                },
                region_id: row.id,
                repair: standing,
            });
        }
        let table = atoms_table(row.dim, row.metric, row.encrypted);
        // A different width or metric means a different physical table, because
        // its name is a function of (dim, metric, encrypted). Rows move; ids do
        // not change, which is what keeps edges and idempotency records valid.
        let destination = atoms_table(new_dim, new_metric, row.encrypted);
        let moving = destination != table;

        // Resume only matching vector work. Repair-phase provenance is complete,
        // so a new target may supersede it and rebuild the graph afterwards.
        let resuming_matching_vectors = matches!(
            standing.as_ref(),
            Some(mark)
                if mark.phase == ReembedPhase::Vectors
                    && mark.to_model == new_model
                    && mark.to_dim == new_dim
                    && mark.to_metric == metric_tag(new_metric)
        );
        let resume_from = match standing.as_ref() {
            Some(mark)
                if mark.phase == ReembedPhase::Vectors
                    && mark.to_model == new_model
                    && mark.to_dim == new_dim
                    && mark.to_metric == metric_tag(new_metric) =>
            {
                mark.done_through
            }
            Some(mark) if mark.phase == ReembedPhase::Vectors => {
                return Err(MemError::Invalid(format!(
                    "region '{key}' is part-way through a re-embed to model '{}' ({} dim, {}); \
                     finish that one before starting another",
                    mark.to_model, mark.to_dim, mark.to_metric
                )))
            }
            _ => 0,
        };

        // Before the mark, not after it: a token already tripped when the call
        // arrives has converted nothing, and marking a healthy region would
        // make it unattachable for a migration that never began.
        check_cancel(cancel)?;

        if moving {
            ensure_atoms_table(&conn, new_dim, new_metric, row.encrypted)?;
        }
        let atom_wrap = if row.encrypted {
            Some(self.attach_region_key(key, &row)?.atom_wrap)
        } else {
            None
        };

        // Capture the whole value, not only the re-embed fields. A zero-progress
        // attempt made while an older repair mark stands must put that mark (and
        // any unrelated metadata keys) back byte-for-byte.
        let metadata_before_mark = read_metadata(&conn, row.id)?;

        // Mark before enumerating atoms so concurrent writes cannot escape the
        // migration plan. `done_through` makes committed batches resumable.
        let first_checkpoint = ReembedMark {
            to_model: new_model.clone(),
            to_dim: new_dim,
            to_metric: metric_tag(new_metric).to_string(),
            done_through: resume_from,
            phase: ReembedPhase::Vectors,
        };
        write_reembed_mark(&conn, row.id, &first_checkpoint)?;
        // Unlike the database-wide capability, this narrow reservation may
        // cross external code. It prevents only destruction of this source
        // region; callbacks remain free to use every other encrypted region.
        let _region_reservation = key_lifecycle.reserve_memory_region(row.id as u64);

        // Validate the whole region before making the first vector durable. A
        // later empty-text row cannot be discovered after an earlier page was
        // committed: that would leave a migration which can never finish. The
        // preflight is paged, so this invariant does not require holding every
        // atom's plaintext in memory at once.
        let empty_text = match self.count_empty_reembed_text(
            &conn,
            &table,
            row.id,
            atom_wrap.as_deref(),
            cancel,
        ) {
            Ok(count) => count,
            Err(e) => {
                if !resuming_matching_vectors {
                    restore_reembed_metadata(&conn, row.id, metadata_before_mark.as_deref())?;
                }
                return Err(e);
            }
        };
        if empty_text > 0 {
            // Nothing was converted, so a region this call marked itself is put
            // back as it was. One that arrived marked keeps its mark: that
            // migration is genuinely in flight and this refusal is not its end.
            if !resuming_matching_vectors {
                restore_reembed_metadata(&conn, row.id, metadata_before_mark.as_deref())?;
            }
            return Err(MemError::Invalid(format!(
                "region '{key}' has {} atom(s) stored with a vector but no text, so no new \
                 vector can be computed for them; re-embedding would leave them on the old \
                 model's vectors while reporting the region converted",
                empty_text
            )));
        }

        let mut migrated = 0u64;
        let batches = (|| {
            let mut after = resume_from;
            let mut durable_checkpoint = first_checkpoint.clone();
            loop {
                check_cancel(cancel)?;
                let page = self.collect_reembed_work(
                    &conn,
                    &table,
                    row.id,
                    atom_wrap.as_deref(),
                    after,
                    cancel,
                )?;
                let Some(last_read) = page.last_id else {
                    break;
                };
                after = last_read;
                if page.atoms.is_empty() {
                    continue;
                }

                let chunk = page.atoms.as_slice();
                let texts: Vec<&str> = chunk.iter().map(|atom| atom.text.as_str()).collect();
                check_cancel(cancel)?;
                let atom_ids = chunk
                    .iter()
                    .map(|atom| {
                        u64::try_from(atom.id).map_err(|_| {
                            MemError::Invalid(format!(
                                "atom id {} cannot name an atom key",
                                atom.id
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let _atom_callback_reservation = row
                    .encrypted
                    .then(|| key_lifecycle.reserve_memory_atom_callbacks(&atom_ids));
                // Embeddings are plaintext too. Keep the whole returned batch
                // under an RAII scrubber so success, validation errors and
                // cancellation after the external call all clear it.
                //
                // No database/key capability may cross this external call. A
                // callback is allowed to use the same database (for example to
                // recall configuration or record accounting in another sealed
                // region), and those paths acquire the lifecycle guard too.
                drop(key_lifecycle);
                let embedded = new_embedder
                    .embed_with_cancel(&texts, cancel)
                    .map(Zeroizing::new);
                key_lifecycle = self.db.key_lifecycle_lock();

                // While the callback ran, a drop or another resume was free to
                // order ahead of us. Trust the computed vectors only if this is
                // still the same region incarnation at the exact checkpoint we
                // released. This check happens before cancellation/error
                // propagation so all exits once again own the lifecycle guard.
                self.verify_reembed_continuation(&conn, key, &row, &durable_checkpoint)?;
                let vectors = embedded?;
                check_cancel(cancel)?;
                if vectors.len() != chunk.len() {
                    return Err(MemError::Invalid(format!(
                        "embedder returned {} vectors for {} atoms",
                        vectors.len(),
                        chunk.len()
                    )));
                }
                // The same boundary every other write crosses. Counting the vectors
                // says nothing about their width or their values, and a NaN written
                // here would be a stored embedding no caller could have produced
                // through `remember`.
                for vector in vectors.iter() {
                    validate_embedding(key, new_dim, vector, "re-embedded")?;
                }
                // Advanced inside the batch's own transaction, not after it: this
                // is the only record of how much is already durable, and a crash
                // between the rows and their checkpoint would leave the two
                // disagreeing about where a resume should start.
                let last_id = chunk.last().expect("chunks never yields an empty slice").id;
                let next_checkpoint = ReembedMark {
                    to_model: new_model.clone(),
                    to_dim: new_dim,
                    to_metric: metric_tag(new_metric).to_string(),
                    done_through: last_id,
                    phase: ReembedPhase::Vectors,
                };
                let ctx = BatchContext {
                    atom_wrap: atom_wrap.as_deref(),
                    region_id: row.id,
                    checkpoint: &next_checkpoint,
                    cancel,
                };
                migrated += if moving {
                    self.move_reembedded_batch(
                        &conn,
                        &table,
                        &destination,
                        chunk,
                        vectors.as_slice(),
                        &ctx,
                    )? as u64
                } else {
                    self.write_reembedded_batch(&conn, &table, chunk, vectors.as_slice(), &ctx)?
                        as u64
                };
                durable_checkpoint = next_checkpoint;
                check_cancel(cancel)?;
            }

            // After the last page and before the provenance flip. A cancellation
            // that races a completed final batch leaves its checkpoint standing,
            // which is exactly what a resume needs.
            check_cancel(cancel)?;
            if row.encrypted {
                self.retire_sealed_segment_parts(&conn, row.id, &table, &key_lifecycle)?;
            }
            #[cfg(test)]
            if FAIL_REEMBED_AFTER_SEGMENT_RETIRE.with(std::cell::Cell::take) {
                return Err(MemError::Invalid(
                    "injected re-embed failure after segment retirement".into(),
                ));
            }
            Ok(key_lifecycle)
        })();

        let key_lifecycle = match batches {
            Ok(key_lifecycle) => key_lifecycle,
            Err(e) => {
                // A migration that converted nothing leaves nothing to resume,
                // so a mark this call wrote itself is taken back.
                if migrated == 0 && !resuming_matching_vectors {
                    restore_reembed_metadata_if_unchanged(
                        &conn,
                        row.id,
                        &first_checkpoint,
                        metadata_before_mark.as_deref(),
                    )?;
                }
                return Err(e);
            }
        };

        // Provenance last, and together with the mark: until this lands the
        // region still describes itself by the model its vectors were actually
        // written with, and by the shape whose table its rows are in.
        let repair = ReembedMark {
            to_model: new_model.clone(),
            to_dim: new_dim,
            to_metric: metric_tag(new_metric).to_string(),
            done_through: 0,
            phase: ReembedPhase::Repair,
        };
        publish_reembed(&conn, row.id, &repair)?;

        // Every engine over this Database may hold an ANN index and embedder for
        // the old provenance. Detach all of them before reopening this region
        // for repair; their user-owned embedders are dropped outside the lock.
        let retired_states = self.db.drain_memory_region_caches(row.id as u64);
        drop(key_lifecycle);
        drop(retired_states);

        Ok(Migration {
            report: ReembedReport {
                atoms_migrated: migrated,
                model_id: new_model,
                ..Default::default()
            },
            region_id: row.id,
            repair: Some(repair),
        })
    }

    /// Rebuild the region's `SimilarTo` web over the new vectors.
    ///
    /// The edges are derived data: they assert "these two atoms are near each
    /// other", measured in a vector space the migration just replaced. Leaving
    /// them would leave a false claim in the graph. Managed edges are rebuilt
    /// from the policy persisted by `evolve`; authored edges are never touched:
    /// `Causes`,
    /// `Contradicts`, `Refines`, `Precedes`, `Supersedes`, `DerivedFrom` and
    /// `DependsOn` record what someone asserted, not what a vector space
    /// implied.
    fn reweave_similarity_edges(
        &self,
        key: &str,
        cancel: Option<&citadel_core::CancelToken>,
        kl: &KeyLifecycleGuard<'_>,
        edges_guard: &MemoryEdgesGuard<'_>,
    ) -> Result<WeaveRebuild> {
        check_cancel(cancel)?;
        let h = self.region_handle(key)?;
        let mut rewoven = 0u64;
        let mut cleared = 0u64;
        let mut after = i64::MIN;

        // Page region atom ids first, bounding each IN list and the
        // source/rebuilt bookkeeping. `read_weave_shapes` independently
        // keyset-pages the matching edges and carries a source's aggregate
        // across page boundaries, so high fan-out is not materialized at once.
        loop {
            let sources = self.next_similarity_sources(&h, after, cancel)?;
            let Some(last_source) = sources.last().copied() else {
                break;
            };
            after = last_source;
            let shapes = self.read_weave_shapes(&h, &sources, cancel)?;

            for shape in &shapes {
                check_cancel(cancel)?;
                let neighbours = self.similar_neighbours_guarded(key, &h, shape, cancel, kl)?;
                check_cancel(cancel)?;
                self.db.debug_fire_memory_edges_reweave_hook();
                let replacement = match neighbours {
                    Some(neighbours) => self.replace_managed_similarity_edges_locked(
                        key,
                        &h,
                        shape.src,
                        &neighbours,
                        cancel,
                        (kl, edges_guard),
                    )?,
                    None => self.clear_managed_similarity_source_locked(
                        key,
                        &h,
                        shape.src,
                        cancel,
                        kl,
                        edges_guard,
                    )?,
                };
                rewoven += replacement.rewoven;
                cleared += replacement.cleared;
                check_cancel(cancel)?;
            }
        }
        Ok(WeaveRebuild { rewoven, cleared })
    }

    /// The next bounded page of region atoms whose outgoing web must be checked.
    fn next_similarity_sources(
        &self,
        h: &RegionHandle,
        after: AtomId,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<AtomId>> {
        check_cancel(cancel)?;
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            &format!(
                "SELECT src_id FROM memory_similarity_policies \
                 WHERE region_id = $1 AND src_id > $2 ORDER BY src_id LIMIT {REEMBED_BATCH}"
            ),
            &[Value::Integer(h.id), Value::Integer(after)],
        )?;
        check_cancel(cancel)?;
        let mut ids = Vec::with_capacity(qr.rows.len());
        for row in &qr.rows {
            check_cancel(cancel)?;
            ids.push(as_int(&row[0])?);
        }
        Ok(ids)
    }

    /// Read the exact rules that produced the managed similarity edges.
    fn read_weave_shapes(
        &self,
        h: &RegionHandle,
        scope: &[AtomId],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<WeaveShape>> {
        check_cancel(cancel)?;
        let conn = Connection::open(&self.db)?;
        let ids = id_list(scope);
        let qr = conn.query_params(
            &format!(
                "SELECT src_id, neighbors, max_distance, CAST(kinds AS TEXT) \
                 FROM memory_similarity_policies WHERE region_id = $1 AND src_id IN ({ids}) \
                 ORDER BY src_id"
            ),
            &[Value::Integer(h.id)],
        )?;
        let mut shapes = Vec::with_capacity(qr.rows.len());
        for row in &qr.rows {
            check_cancel(cancel)?;
            let neighbors = usize::try_from(as_int(&row[1])?).map_err(|_| {
                MemError::Invalid("stored similarity neighbor count is out of range".into())
            })?;
            let max_distance = as_f32(&row[2])?;
            if !max_distance.is_finite() {
                return Err(MemError::Invalid(
                    "stored similarity distance ceiling must be finite".into(),
                ));
            }
            let mut kinds: Vec<String> = serde_json::from_str(as_text(&row[3])?).map_err(|e| {
                MemError::Invalid(format!("stored similarity kind filter is invalid: {e}"))
            })?;
            kinds.sort();
            kinds.dedup();
            shapes.push(WeaveShape {
                src: as_int(&row[0])?,
                neighbors,
                max_distance,
                kinds,
            });
        }
        Ok(shapes)
    }

    /// The neighbours one atom's rule selects from the new vectors.
    fn similar_neighbours_guarded(
        &self,
        key: &str,
        h: &RegionHandle,
        shape: &WeaveShape,
        cancel: Option<&citadel_core::CancelToken>,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<Option<Vec<ManagedSimilarityEdge>>> {
        check_cancel(cancel)?;
        let conn = Connection::open(&self.db)?;
        self.verify_region_live_for_repair(&conn, h, key)?;
        let live = conn.query_params(
            &format!(
                "SELECT 1 FROM {} WHERE id = $1 AND region_id = $2 \
                 AND (expires_at IS NULL OR expires_at > $3) LIMIT 1",
                h.table
            ),
            &[
                Value::Integer(shape.src),
                Value::Integer(h.id),
                Value::Timestamp(now_micros()),
            ],
        )?;
        if live.rows.is_empty() {
            return Ok(None);
        }
        let state = self.read_atom_state_locked(&conn, key, h, shape.src)?;
        check_cancel(cancel)?;
        // Pure distance ordering, as the weave does: fused relevance would rank
        // by signals that have nothing to do with the neighbourhood.
        let kinds = shape.kinds.clone();
        let qvec = state.embedding;
        let query = RecallQuery::by_embedding(qvec.clone(), shape.neighbors.saturating_add(1))
            .with_kinds(kinds)
            .with_weights(FusionWeights::semantic_only());
        let candidates = if h.atom_wrap.is_some() {
            self.recall_sealed_candidates(
                h,
                ResolvedRecall {
                    query: &query,
                    vector: &qvec,
                },
                &conn,
                kl,
                query.k,
                cancel,
            )?
        } else {
            self.recall_plain_semantic_candidates(h, &query, &qvec, &conn, cancel)?
        };
        let recalled = fuse_rank(
            candidates,
            FusionWeights::semantic_only(),
            query.as_of_micros.unwrap_or_else(now_micros),
            query.k,
        );
        // Fusion itself is local work after the candidate pipeline; retain the
        // operation token across it without replacing the Database-global token.
        check_cancel(cancel)?;
        Ok(Some(
            recalled
                .into_iter()
                .filter_map(|n| {
                    let distance = n.distance?;
                    (n.id != shape.src && distance <= shape.max_distance).then_some((
                        n.id,
                        1.0 / (1.0 + distance.max(0.0)),
                        None,
                    ))
                })
                .take(shape.neighbors)
                .collect(),
        ))
    }

    fn clear_managed_similarity_source_locked(
        &self,
        key: &str,
        h: &RegionHandle,
        src: AtomId,
        cancel: Option<&citadel_core::CancelToken>,
        lifecycle: &KeyLifecycleGuard<'_>,
        _edges_guard: &MemoryEdgesGuard<'_>,
    ) -> Result<WeaveRebuild> {
        let conn = Connection::open(&self.db)?;
        with_write_txn(&conn, |c| {
            check_cancel(cancel)?;
            self.verify_region_live_for_repair(c, h, key)?;
            let old = c.query_params(
                "SELECT COUNT(*) FROM memory_similarity_edges WHERE src_id = $1",
                &[Value::Integer(src)],
            )?;
            let old = old
                .rows
                .first()
                .and_then(|row| row.first())
                .map(as_int)
                .transpose()?
                .unwrap_or(0)
                .max(0) as u64;
            c.execute_params(
                "DELETE FROM memory_edges WHERE src_id = $1 AND kind = 'similar_to' \
                 AND dst_id IN (SELECT dst_id FROM memory_similarity_edges WHERE src_id = $1)",
                &[Value::Integer(src)],
            )?;
            c.execute_params(
                "DELETE FROM memory_similarity_edges WHERE src_id = $1",
                &[Value::Integer(src)],
            )?;
            c.execute_params(
                "DELETE FROM memory_similarity_policies WHERE src_id = $1",
                &[Value::Integer(src)],
            )?;
            check_cancel(cancel)?;
            Ok(WeaveRebuild {
                rewoven: 0,
                cleared: old,
            })
        })
        .inspect_err(|e| self.defer_stale_region(key, h.id, e, lifecycle))
    }

    /// Exact semantic candidates for the repair path while its caller owns the
    /// lifecycle and edge guards. This is the semantic-only subset of
    /// `recall_impl`; keeping it here avoids re-entering the lifecycle wrapper.
    fn recall_plain_semantic_candidates(
        &self,
        h: &RegionHandle,
        q: &RecallQuery,
        qvec: &[f32],
        conn: &Connection<'_>,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<Candidate>> {
        let distop = match h.metric {
            EmbeddingMetric::Cosine => "<=>",
            EmbeddingMetric::L2 => "<->",
            EmbeddingMetric::InnerProduct => "<#>",
        };
        let mut params = vec![Value::Vector(qvec.to_vec().into()), Value::Integer(h.id)];
        let mut predicates = vec!["region_id = $2".to_string()];
        if !q.kinds.is_empty() {
            let mut placeholders = Vec::with_capacity(q.kinds.len());
            for kind in &q.kinds {
                check_cancel(cancel)?;
                params.push(Value::Text(kind.clone().into()));
                placeholders.push(format!("${}", params.len()));
            }
            predicates.push(format!("kind IN ({})", placeholders.join(", ")));
        }
        params.push(Value::Timestamp(now_micros()));
        predicates.push(format!(
            "(expires_at IS NULL OR expires_at > ${})",
            params.len()
        ));
        if !q.include_superseded {
            predicates.push(
                "id NOT IN (SELECT dst_id FROM memory_edges WHERE kind = 'supersedes')".into(),
            );
        }
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, CAST(payload AS TEXT), text_content, score, confidence, \
                 created_at, expires_at, embedding {distop} $1, 0.0, immutable FROM {} WHERE {} \
                 ORDER BY embedding {distop} $1, id LIMIT {}",
                h.table,
                predicates.join(" AND "),
                q.k
            ),
            &params,
        )?;
        let mut candidates = Vec::with_capacity(qr.rows.len());
        for row in &qr.rows {
            check_cancel(cancel)?;
            candidates.push(parse_candidate(row)?);
        }
        check_cancel(cancel)?;
        Ok(candidates)
    }

    /// Persist an ANN index over the region's new vectors.
    ///
    /// An encrypted region has no plaintext vector column to index - it recalls
    /// by decrypt-then-rank - so there is nothing to rebuild and the report says
    /// so rather than claiming a rebuild that never applied.
    fn rebuild_region_ann(
        &self,
        table: &str,
        encrypted: bool,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<bool> {
        check_cancel(cancel)?;
        if encrypted {
            return Ok(false);
        }
        // A persisted ANN segment is an optional accelerator: every read has an
        // exact-scan fallback, and `ann_rebuilt = false` is the established way
        // this best-effort step reports an open/build/I/O refusal. Cancellation
        // is different. Swallowing it would let the migration clear its repair
        // mark and report success after the Database token stopped the build.
        let rebuilt = match Connection::open(&self.db) {
            Ok(conn) => match conn.persist_ann_index(table, "embedding") {
                Ok(_) => true,
                Err(citadel_sql::SqlError::Storage(citadel_core::Error::Interrupted)) => {
                    return Err(MemError::Core(citadel_core::Error::Interrupted));
                }
                Err(_) => false,
            },
            Err(citadel_sql::SqlError::Storage(citadel_core::Error::Interrupted)) => {
                return Err(MemError::Core(citadel_core::Error::Interrupted));
            }
            Err(_) => false,
        };
        // Index construction/persistence is an uninterruptible lower-layer call.
        // If cancellation landed inside it, leave the repair mark standing and
        // let a retry rebuild/reweave idempotently.
        check_cancel(cancel)?;
        Ok(rebuilt)
    }

    /// Give one batch of atoms their new vectors, in a single transaction.
    ///
    /// An `UPDATE` keyed by id, never a delete-and-reinsert: the row keeps its
    /// id and therefore its edges, and every column this statement does not
    /// name is preserved because it is never touched. That is what makes the
    /// preservation guarantee structural rather than something the code has to
    /// remember to uphold column by column.
    fn write_reembedded_batch(
        &self,
        conn: &Connection<'_>,
        table: &str,
        batch: &[ReembedAtom],
        vectors: &[Vec<f32>],
        ctx: &BatchContext<'_>,
    ) -> Result<usize> {
        let atom_wrap = ctx.atom_wrap;
        conn.execute("BEGIN")?;
        let result = (|| -> Result<usize> {
            let batch_ids: Vec<AtomId> = batch.iter().map(|atom| atom.id).collect();
            let wrapped_by_id = match atom_wrap {
                Some(_) => exact_live_atom_wrapped_for_ids(
                    &self.db,
                    conn,
                    table,
                    ctx.region_id,
                    &batch_ids,
                )?,
                None => FxHashMap::default(),
            };
            for (atom, vector) in batch.iter().zip(vectors) {
                check_cancel(ctx.cancel)?;
                let id = atom.id;
                match atom_wrap {
                    None => {
                        conn.execute_params(
                            &format!("UPDATE {table} SET embedding = $1 WHERE id = $2"),
                            &[Value::Vector(vector.clone().into()), Value::Integer(id)],
                        )?;
                    }
                    Some(wrap) => {
                        let Some(wrapped) = wrapped_by_id.get(&id) else {
                            continue;
                        };
                        let payload = atom.payload_json.as_deref().ok_or_else(|| {
                            MemError::Invalid(format!(
                                "sealed atom {id} is missing its opened payload"
                            ))
                        })?;
                        // Reseal under the atom's existing key, so its slot and
                        // generation stay valid and nothing has to be retired.
                        let sealed = reseal_atom(wrap, wrapped, id, vector, &atom.text, payload)?;
                        conn.execute_params(
                            &format!("UPDATE {table} SET sealed = $1 WHERE id = $2"),
                            &[Value::Blob(sealed), Value::Integer(id)],
                        )?;
                    }
                }
            }
            check_cancel(ctx.cancel)?;
            write_reembed_mark(conn, ctx.region_id, ctx.checkpoint)?;
            check_cancel(ctx.cancel)?;
            Ok(batch.len())
        })();
        match result {
            Ok(n) => {
                if let Err(error) = check_cancel(ctx.cancel) {
                    let _ = conn.execute("ROLLBACK");
                    return Err(error);
                }
                conn.execute("COMMIT")?;
                Ok(n)
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Move one batch of atoms to a differently-shaped atoms table, giving them
    /// their new vectors on the way.
    ///
    /// The insert names **every** column and takes each value from the source
    /// row, so nothing is defaulted back to zero on the way across: not
    /// `access_count`, not `accessed_at`, not `confidence`, not `expires_at`.
    /// The id is carried too, which is what keeps `memory_edges` and
    /// `memory_idempotency` pointing at the same atoms - ids come from one
    /// global counter, so the same id is free in the destination table.
    ///
    /// The source table is never dropped, even when this empties it. Its name is
    /// a function of `(dim, metric, encrypted)`, so every region of that shape
    /// shares it, and an unconditional drop would take another region's rows
    /// with it. An empty table costs nothing; that mistake is unrecoverable.
    fn move_reembedded_batch(
        &self,
        conn: &Connection<'_>,
        source: &str,
        destination: &str,
        batch: &[ReembedAtom],
        vectors: &[Vec<f32>],
        ctx: &BatchContext<'_>,
    ) -> Result<usize> {
        let atom_wrap = ctx.atom_wrap;
        conn.execute("BEGIN")?;
        let result = (|| -> Result<usize> {
            let batch_ids: Vec<AtomId> = batch.iter().map(|atom| atom.id).collect();
            let wrapped_by_id = match atom_wrap {
                Some(_) => exact_live_atom_wrapped_for_ids(
                    &self.db,
                    conn,
                    source,
                    ctx.region_id,
                    &batch_ids,
                )?,
                None => FxHashMap::default(),
            };
            for (atom, vector) in batch.iter().zip(vectors) {
                check_cancel(ctx.cancel)?;
                let id = atom.id;
                match atom_wrap {
                    None => {
                        let qr = conn.query_params(
                            &format!(
                                "SELECT region_id, kind, payload, text_content, score, \
                                 confidence, access_count, immutable, created_at, accessed_at, \
                                 expires_at FROM {source} WHERE id = $1"
                            ),
                            &[Value::Integer(id)],
                        )?;
                        let Some(r) = qr.rows.first() else { continue };
                        conn.execute_params(
                            &format!(
                                "INSERT INTO {destination} (id, region_id, kind, embedding, \
                                 payload, text_content, score, confidence, access_count, \
                                 immutable, created_at, accessed_at, expires_at) \
                                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"
                            ),
                            &[
                                Value::Integer(id),
                                r[0].clone(),
                                r[1].clone(),
                                Value::Vector(vector.clone().into()),
                                r[2].clone(),
                                r[3].clone(),
                                r[4].clone(),
                                r[5].clone(),
                                r[6].clone(),
                                r[7].clone(),
                                r[8].clone(),
                                r[9].clone(),
                                r[10].clone(),
                            ],
                        )?;
                    }
                    Some(wrap) => {
                        let qr = conn.query_params(
                            &format!(
                                "SELECT region_id, kind, key_slot, key_gen, score, \
                                 confidence, access_count, immutable, created_at, accessed_at, \
                                 expires_at FROM {source} WHERE id = $1"
                            ),
                            &[Value::Integer(id)],
                        )?;
                        let Some(r) = qr.rows.first() else { continue };
                        let Some(wrapped) = wrapped_by_id.get(&id) else {
                            continue;
                        };
                        let payload = atom.payload_json.as_deref().ok_or_else(|| {
                            MemError::Invalid(format!(
                                "sealed atom {id} is missing its opened payload"
                            ))
                        })?;
                        // Resealed under the atom's own existing key, so its
                        // slot and generation move across unchanged and the key
                        // store is never touched.
                        let sealed = reseal_atom(wrap, wrapped, id, vector, &atom.text, payload)?;
                        conn.execute_params(
                            &format!(
                                "INSERT INTO {destination} (id, region_id, kind, sealed, \
                                 key_slot, key_gen, score, confidence, access_count, immutable, \
                                 created_at, accessed_at, expires_at) \
                                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"
                            ),
                            &[
                                Value::Integer(id),
                                r[0].clone(),
                                r[1].clone(),
                                Value::Blob(sealed),
                                r[2].clone(),
                                r[3].clone(),
                                r[4].clone(),
                                r[5].clone(),
                                r[6].clone(),
                                r[7].clone(),
                                r[8].clone(),
                                r[9].clone(),
                                r[10].clone(),
                            ],
                        )?;
                    }
                }
                conn.execute_params(
                    &format!("DELETE FROM {source} WHERE id = $1"),
                    &[Value::Integer(id)],
                )?;
            }
            check_cancel(ctx.cancel)?;
            write_reembed_mark(conn, ctx.region_id, ctx.checkpoint)?;
            check_cancel(ctx.cancel)?;
            Ok(batch.len())
        })();
        match result {
            Ok(n) => {
                if let Err(error) = check_cancel(ctx.cancel) {
                    let _ = conn.execute("ROLLBACK");
                    return Err(error);
                }
                conn.execute("COMMIT")?;
                Ok(n)
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Count atoms that cannot be re-embedded without retaining the region's
    /// plaintext. This preflight keeps the all-or-nothing empty-text rule while
    /// using the same bounded, cancellable pages as conversion.
    fn count_empty_reembed_text(
        &self,
        conn: &Connection<'_>,
        table: &str,
        region_id: RegionId,
        atom_wrap: Option<&AtomWrapKey>,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<u64> {
        let mut empty_text = 0u64;
        let mut after = i64::MIN;
        loop {
            let page =
                self.collect_reembed_work(conn, table, region_id, atom_wrap, after, cancel)?;
            empty_text += page.empty_text;
            let Some(last_id) = page.last_id else {
                break;
            };
            after = last_id;
        }
        Ok(empty_text)
    }

    /// Read one bounded atom/text page, counting rows that have no text.
    fn collect_reembed_work(
        &self,
        conn: &Connection<'_>,
        table: &str,
        region_id: RegionId,
        atom_wrap: Option<&AtomWrapKey>,
        after: AtomId,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<ReembedWork> {
        check_cancel(cancel)?;
        let mut atoms = Vec::with_capacity(REEMBED_BATCH);
        let mut empty_text = 0u64;
        let mut last_id = None;

        match atom_wrap {
            None => {
                let qr = conn.query_params(
                    &format!(
                        "SELECT id, text_content FROM {table} \
                         WHERE region_id = $1 AND id > $2 ORDER BY id LIMIT {REEMBED_BATCH}"
                    ),
                    &[Value::Integer(region_id), Value::Integer(after)],
                )?;
                check_cancel(cancel)?;
                for r in &qr.rows {
                    check_cancel(cancel)?;
                    let id = as_int(&r[0])?;
                    last_id = Some(id);
                    let text = match &r[1] {
                        Value::Text(t) => t.to_string(),
                        _ => String::new(),
                    };
                    if text.is_empty() {
                        empty_text += 1;
                    } else {
                        atoms.push(ReembedAtom {
                            id,
                            text,
                            payload_json: None,
                        });
                    }
                }
            }
            Some(wrap) => {
                // Sealed regions keep the text inside the blob, so it has to be
                // opened to be re-embedded.
                let qr = conn.query_params(
                    &format!(
                        "SELECT id, sealed, key_slot, key_gen FROM {table} \
                         WHERE region_id = $1 AND id > $2 ORDER BY id LIMIT {REEMBED_BATCH}"
                    ),
                    &[Value::Integer(region_id), Value::Integer(after)],
                )?;
                check_cancel(cancel)?;
                let wrapped = exact_live_atom_wrapped_rows(&self.db, &qr.rows, 0, 2, 3)?;
                for (r, wrapped) in qr.rows.iter().zip(wrapped) {
                    check_cancel(cancel)?;
                    let id = as_int(&r[0])?;
                    last_id = Some(id);
                    let Some(wrapped) = wrapped else {
                        // The atom's key is gone, so its content is already
                        // unrecoverable. Skipping is right; counting it as
                        // migrated would not be.
                        continue;
                    };
                    check_cancel(cancel)?;
                    let (text, mut payload) =
                        open_atom_content(wrap, &wrapped, id, as_blob(&r[1])?)?;
                    let mut text = Zeroizing::new(text);
                    check_cancel(cancel)?;
                    if text.is_empty() {
                        empty_text += 1;
                    } else {
                        let encoded = serde_json::to_string(&payload);
                        zeroize_json_strings(&mut payload);
                        let payload_json = Zeroizing::new(encoded.map_err(|error| {
                            MemError::Invalid(format!("payload not serializable: {error}"))
                        })?);
                        atoms.push(ReembedAtom {
                            id,
                            text: std::mem::take(&mut *text),
                            payload_json: Some(payload_json),
                        });
                    }
                    zeroize_json_strings(&mut payload);
                }
            }
        }
        Ok(ReembedWork {
            atoms,
            empty_text,
            last_id,
        })
    }

    /// For an encrypted region, key destruction is the irreversible boundary.
    /// Cancellation is observed immediately before it; afterward the key erasure
    /// and atomic SQL cleanup run to completion and return the successful result.
    /// A storage failure can still leave unrecoverable residue for retry/reconcile.
    pub fn drop_region(&self, name: &str) -> Result<()> {
        let cancel = check_db_cancel(&self.db)?;
        let key = name.to_ascii_lowercase();
        // Preflight + segment retirement -> RCK/ACK tombstones -> atomic row cleanup.
        let retired_state;
        let shared_retired;
        let _kl = self.db.key_lifecycle_lock();
        let _edges_guard = self.db.memory_edges_lock();
        let conn = Connection::open(&self.db)?;
        let Some(row) = self.load_region_row(&conn, &key)? else {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            retired_state = self.take_attached_region(&key, None);
            shared_retired = retired_state
                .as_ref()
                .map(|state| self.db.drain_memory_region_caches(state.id as u64))
                .unwrap_or_default();
            drop(_edges_guard);
            drop(_kl);
            drop(retired_state);
            drop(shared_retired);
            return Ok(());
        };
        if _kl.memory_region_active(row.id as u64) {
            return Err(MemError::Core(citadel_core::Error::RegionInUse {
                region_id: row.id as u64,
            }));
        }
        let atoms = atoms_table(row.dim, row.metric, row.encrypted);
        // A region stopped part-way through a shape-changing re-embed has rows
        // in the destination too. Deleting only the table the row names would
        // leave the converted ones behind - orphaned, and for an encrypted
        // region still holding a live key after the region that owned it is
        // gone. Erasure has to be complete on its own, not left to reconcile.
        let homes = self.region_atom_tables(&conn, &row)?;
        let managed = conn.query_params(
            "SELECT e.src_id, e.dst_id FROM memory_similarity_edges e \
             JOIN memory_similarity_policies p ON p.src_id = e.src_id \
             WHERE p.region_id = $1",
            &[Value::Integer(row.id)],
        )?;
        let managed_edges = managed
            .rows
            .iter()
            .map(|edge| Ok((as_int(&edge[0])?, as_int(&edge[1])?)))
            .collect::<Result<Vec<_>>>()?;
        let cleanup = drop_region_cleanup_statements(row.id, &homes, &managed_edges);

        // Read every row-side ACK binding before the RCK is destroyed. Nothing
        // after that boundary may execute cancellable SQL or discover new work.
        let mut atom_slots: Vec<(u32, u64, u64)> = Vec::new();
        let region_key_slot = if row.encrypted {
            for table in &homes {
                if conn.table_schema(table).is_none() {
                    continue;
                }
                let qr = conn.query_params(
                    &format!("SELECT id, key_slot, key_gen FROM {table} WHERE region_id = $1"),
                    &[Value::Integer(row.id)],
                )?;
                for atom in &qr.rows {
                    let binding = atom_key_binding(as_int(&atom[0])?, &atom[1], &atom[2])?;
                    atom_slots.push((binding.slot, binding.atom_id, binding.generation));
                }
            }
            let slot = row.rsk_slot.ok_or_else(|| {
                MemError::Invalid(format!(
                    "encrypted region '{key}' has no key slot; refusing to delete its \
                     rows without destroying a key"
                ))
            })?;
            // Only THIS row's slot; retries converge; TOMBSTONE still scrubs a torn sibling.
            let generation = row.rsk_gen.ok_or_else(|| {
                MemError::Invalid(format!(
                    "encrypted region '{key}' has no key generation; refusing to destroy it"
                ))
            })?;
            let record = self.db.region_store_slot(slot)?;
            match record.state {
                SlotState::Live
                    if record.region_id == row.id as u64 && record.gen == generation =>
                {
                    Some((slot, generation))
                }
                SlotState::Tombstone => Some((slot, generation)),
                SlotState::Live if record.region_id == row.id as u64 => {
                    return Err(MemError::Invalid(format!(
                        "encrypted region '{key}' key generation is stale; refusing to destroy rows"
                    )))
                }
                // A different live binding or an empty slot proves this row's
                // RCK is already gone. Continue cleanup without touching a successor.
                SlotState::Live | SlotState::Empty => None,
            }
        } else {
            None
        };
        if row.encrypted {
            ensure_atom_key_slots_unreserved(&atom_slots, &_kl)?;
        }

        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        // Last caller poll; segment retirement may still cancel before its key commit.
        check_cancel(cancel.as_ref())?;

        // Destroy keys and drop the atom-wrap cache before deleting rows.
        if row.encrypted {
            // Segment retirement may query SQL, but after its key is destroyed
            // every remaining step is explicitly uncancelled and convergent.
            self.retire_sealed_segment_parts(&conn, row.id, &atoms, &_kl)?;
            if let Some((slot, generation)) = region_key_slot {
                _kl.region_store_tombstone(slot, row.id as u64, generation)?;
            }
            #[cfg(test)]
            debug_fire_cancel_after_key_erasure();
        }
        retired_state = self.take_attached_region(&key, Some(row.id));
        shared_retired = self.db.drain_memory_region_caches(row.id as u64);

        // Reclaim the region's atom key slots (RCK gone, so these are dead).
        if row.encrypted {
            _kl.atom_store_tombstone_batch(&atom_slots)?;
        }

        if row.encrypted {
            execute_owned_uncancelled_recovery(&conn, &cleanup)?;
        } else {
            with_write_txn(&conn, |write| execute_owned_statements(write, &cleanup))?;
        }
        drop(_edges_guard);
        drop(_kl);
        drop(retired_state);
        drop(shared_retired);
        Ok(())
    }

    pub fn remember(&self, region: &str, atom: AtomInput) -> Result<AtomId> {
        self.remember_derived(region, atom, &[], None)
    }

    /// Atom + DerivedFrom edges in one txn so a crash cannot orphan a derived fact.
    pub fn remember_derived(
        &self,
        region: &str,
        atom: AtomInput,
        sources: &[AtomId],
        evidence_ref: Option<serde_json::Value>,
    ) -> Result<AtomId> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let cancel = check_db_cancel(&self.db)?;
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;
        let prep = prepare_atom_row(&h, &key, &atom, cancel.as_ref())?;
        let src_ids = dedup_sources(sources);

        let conn = Connection::open(&self.db)?;
        // Keys precede row commit; guard against a mid-span reconcile reclaim.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let edges_guard = self.db.memory_edges_lock();
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), 1);
        let result = with_write_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;
            self.verify_atoms_live(c, &h, &key, &src_ids)?;
            let id = next_id(c, "next_atom_id")?;
            self.insert_atom_row(c, &h, id, atom, prep, &mut pending)?;
            link_derived_sources(c, id, &src_ids, evidence_ref.as_ref(), &edges_guard)?;
            check_cancel(cancel.as_ref())?;
            Ok(id)
        });
        let id = match pending.finish(result) {
            Ok(id) => id,
            Err(error) => {
                drop(edges_guard);
                drop(_kl);
                self.evict_stale_region(&key, h.id, &error);
                return Err(error);
            }
        };
        h.max_id.fetch_max(id, Ordering::Relaxed);
        self.note_sealed_insert(&h);
        Ok(id)
    }

    /// Idempotent remember_derived: (kind, exact text) dedup inside the write txn.
    pub fn remember_if_absent(
        &self,
        region: &str,
        atom: AtomInput,
        sources: &[AtomId],
        evidence_ref: Option<serde_json::Value>,
    ) -> Result<RememberOutcome> {
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let cancel = check_db_cancel(&self.db)?;
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;
        let prep = prepare_atom_row(&h, &key, &atom, cancel.as_ref())?;
        let src_ids = dedup_sources(sources);

        let conn = Connection::open(&self.db)?;
        // Keys precede row commit; guard against a mid-span reconcile reclaim.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let edges_guard = self.db.memory_edges_lock();
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), 1);
        let result = with_write_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;
            self.verify_atoms_live(c, &h, &key, &src_ids)?;
            if let Some(id) =
                self.find_atom_by_text(c, &h, &atom.kind, &atom.text, cancel.as_ref())?
            {
                link_derived_sources(c, id, &src_ids, evidence_ref.as_ref(), &edges_guard)?;
                check_cancel(cancel.as_ref())?;
                return Ok(RememberOutcome {
                    id,
                    inserted: false,
                });
            }
            let id = next_id(c, "next_atom_id")?;
            self.insert_atom_row(c, &h, id, atom, prep, &mut pending)?;
            link_derived_sources(c, id, &src_ids, evidence_ref.as_ref(), &edges_guard)?;
            check_cancel(cancel.as_ref())?;
            Ok(RememberOutcome { id, inserted: true })
        });
        let out = match pending.finish(result) {
            Ok(out) => out,
            Err(error) => {
                drop(edges_guard);
                drop(_kl);
                self.evict_stale_region(&key, h.id, &error);
                return Err(error);
            }
        };
        h.max_id.fetch_max(out.id, Ordering::Relaxed);
        if out.inserted {
            self.note_sealed_insert(&h);
        }
        Ok(out)
    }

    /// [`remember_if_absent`](Self::remember_if_absent) keyed by a
    /// caller-supplied idempotency key instead of exact-text dedup: the same
    /// key converges on the original atom without touching it or its
    /// provenance; different keys store even identical texts.
    ///
    /// Keys are scoped to `(region, kind)`; the identity record also binds a
    /// canonical tag over every semantic input. While the bound atom is
    /// live, the same key with ANY changed input fails loudly (an edited
    /// retry needs a NEW key); a stale binding self-heals and frees the key.
    /// Encrypted regions store keyed BLAKE3 MACs, so no plaintext equality
    /// tag reaches disk - but the MAC key is REGION-lifetime, so freed-page
    /// residue of a purged record stays a guess-confirmation commitment for
    /// a later RCK holder; only [`drop_region`](Self::drop_region) closes
    /// that channel.
    pub fn remember_if_absent_keyed(
        &self,
        region: &str,
        atom: AtomInput,
        sources: &[AtomId],
        evidence_ref: Option<serde_json::Value>,
        idempotency_key: &str,
    ) -> Result<RememberOutcome> {
        validate_idempotency_key(idempotency_key)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let cancel = check_db_cancel(&self.db)?;
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;
        validate_atom_input(&atom)?;
        if let Some(vector) = &atom.embedding {
            validate_embedding(&key, h.dim, vector, "passage")?;
        }
        let src_ids = dedup_sources(sources);
        let mac = h.identity_mac.as_deref();
        let key_tag = identity_key_tag(mac, &atom.kind, idempotency_key);
        let payload_json = serde_json::to_string(&atom.payload)
            .map_err(|error| MemError::Invalid(format!("payload not serializable: {error}")))?;
        let request_tag = identity_request_tag(
            mac,
            &key_tag,
            &atom,
            &payload_json,
            &src_ids,
            evidence_ref.as_ref(),
        )?;

        let conn = Connection::open(&self.db)?;
        // Resolve an identical retry or conflicting key before model work. The
        // write transaction below rechecks authoritatively before inserting.
        let _peek_kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let replay = with_read_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;
            let id = self.peek_keyed_identity_hit(c, &h, &atom.kind, &key_tag, &request_tag)?;
            check_cancel(cancel.as_ref())?;
            Ok(id)
        })?;
        drop(_peek_kl);
        if let Some(id) = replay {
            return Ok(RememberOutcome {
                id,
                inserted: false,
            });
        }

        let prep = prepare_atom_row(&h, &key, &atom, cancel.as_ref())?;
        // Sealed inserts allocate keys before their rows commit; hold the guard
        // so a concurrent reconcile cannot reclaim them mid-span.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let edges_guard = self.db.memory_edges_lock();
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), 1);
        let result = with_write_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;
            // Replay resolves first: the identical retry writes nothing, so
            // it must converge even if a source has since been forgotten.
            if let Some(id) = self.keyed_identity_hit(c, &h, &atom.kind, &key_tag, &request_tag)? {
                check_cancel(cancel.as_ref())?;
                return Ok(RememberOutcome {
                    id,
                    inserted: false,
                });
            }
            self.verify_atoms_live(c, &h, &key, &src_ids)?;
            let id = next_id(c, "next_atom_id")?;
            c.execute_params(
                "INSERT INTO memory_idempotency \
                 (region_id, kind, key_mac, request_mac, atom_id) \
                 VALUES ($1, $2, $3, $4, $5)",
                &[
                    Value::Integer(h.id),
                    Value::Text(atom.kind.as_str().into()),
                    Value::Text(key_tag.as_str().into()),
                    Value::Text(request_tag.as_str().into()),
                    Value::Integer(id),
                ],
            )?;
            self.insert_atom_row(c, &h, id, atom, prep, &mut pending)?;
            link_derived_sources(c, id, &src_ids, evidence_ref.as_ref(), &edges_guard)?;
            check_cancel(cancel.as_ref())?;
            Ok(RememberOutcome { id, inserted: true })
        });
        let out = match pending.finish(result) {
            Ok(out) => out,
            Err(error) => {
                drop(edges_guard);
                drop(_kl);
                self.evict_stale_region(&key, h.id, &error);
                return Err(error);
            }
        };
        h.max_id.fetch_max(out.id, Ordering::Relaxed);
        if out.inserted {
            self.note_sealed_insert(&h);
        }
        Ok(out)
    }

    /// Store a keyed batch without replacing any live binding.
    ///
    /// Every entry requires a distinct, non-empty idempotency key. Outcomes
    /// preserve request order. An identical replay returns the original atom id
    /// with `inserted = false`; reuse of a live key for changed input returns
    /// [`MemError::IdempotencyConflict`] and commits none of the batch. All
    /// bindings are resolved before the first insert, and all fresh atoms plus
    /// their identity records commit in one transaction.
    pub fn remember_if_absent_keyed_batch(
        &self,
        region: &str,
        entries: Vec<(AtomInput, String)>,
    ) -> Result<Vec<RememberOutcome>> {
        let cancel = check_db_cancel(&self.db)?;
        if entries.is_empty() {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            return Ok(Vec::new());
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let (atoms, keys): (Vec<AtomInput>, Vec<String>) = entries.into_iter().unzip();
        validate_distinct_idempotency_keys(&keys)?;
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;
        for atom in &atoms {
            validate_atom_input(atom)?;
            if let Some(vector) = &atom.embedding {
                validate_embedding(&key, h.dim, vector, "passage")?;
            }
        }
        let KeyedBatchTags { kinds, tags } =
            keyed_batch_tags(h.identity_mac.as_deref(), &atoms, &keys)?;

        let conn = Connection::open(&self.db)?;
        // A lost-response retry should not run the model again. This short,
        // read-only transaction gives every binding one coherent liveness
        // snapshot. Mixed/fresh batches still recheck under the write txn after
        // inference, so this optimization cannot authorize a stale decision.
        let _peek_kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let replay = with_read_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;
            let mut outcomes = Vec::with_capacity(atoms.len());
            let mut all_replayed = true;
            for (kind, (key_tag, request_tag)) in kinds.iter().zip(&tags) {
                check_cancel(cancel.as_ref())?;
                match self.peek_keyed_identity_hit(c, &h, kind, key_tag, request_tag)? {
                    Some(id) => outcomes.push(RememberOutcome {
                        id,
                        inserted: false,
                    }),
                    None => all_replayed = false,
                }
            }
            check_cancel(cancel.as_ref())?;
            Ok(all_replayed.then_some(outcomes))
        })?;
        drop(_peek_kl);
        if let Some(outcomes) = replay {
            return Ok(outcomes);
        }

        let vecs = self.vectorise_atoms(&key, &h, &atoms, cancel.as_ref())?;
        // Sealed inserts allocate keys before their rows commit; hold the guard
        // so a concurrent reconcile cannot reclaim them mid-span.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), atoms.len());
        let result = with_write_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;

            // Preflight every binding before allocating an id or writing a row.
            // A later conflict therefore cannot leave an earlier entry stored.
            let mut replay_ids = Vec::with_capacity(atoms.len());
            for (kind, (key_tag, request_tag)) in kinds.iter().zip(&tags) {
                check_cancel(cancel.as_ref())?;
                replay_ids.push(self.keyed_identity_hit(c, &h, kind, key_tag, request_tag)?);
            }

            let mut atoms: Vec<Option<AtomInput>> = atoms.into_iter().map(Some).collect();
            let mut vecs: Vec<Option<Vec<f32>>> = vecs.into_iter().map(Some).collect();
            let writing: Vec<usize> = replay_ids
                .iter()
                .enumerate()
                .filter_map(|(index, replay)| replay.is_none().then_some(index))
                .collect();
            let fresh = writing
                .iter()
                .map(|&index| atoms[index].take().expect("each atom is taken once"))
                .collect();
            let fresh_vecs = writing
                .iter()
                .map(|&index| vecs[index].take().expect("each vector is taken once"))
                .collect();
            let ids =
                self.insert_atom_rows(c, &h, fresh, fresh_vecs, &mut pending, cancel.as_ref())?;

            let mut outcomes = replay_ids
                .into_iter()
                .map(|id| {
                    id.map(|id| RememberOutcome {
                        id,
                        inserted: false,
                    })
                })
                .collect::<Vec<_>>();
            for (slot, &index) in writing.iter().enumerate() {
                check_cancel(cancel.as_ref())?;
                let id = ids[slot];
                c.execute_params(
                    "INSERT INTO memory_idempotency \
                     (region_id, kind, key_mac, request_mac, atom_id) \
                     VALUES ($1, $2, $3, $4, $5)",
                    &[
                        Value::Integer(h.id),
                        Value::Text(kinds[index].as_str().into()),
                        Value::Text(tags[index].0.as_str().into()),
                        Value::Text(tags[index].1.as_str().into()),
                        Value::Integer(id),
                    ],
                )?;
                outcomes[index] = Some(RememberOutcome { id, inserted: true });
            }
            check_cancel(cancel.as_ref())?;
            Ok(outcomes
                .into_iter()
                .map(|outcome| outcome.expect("every keyed entry is replayed or inserted"))
                .collect::<Vec<_>>())
        });
        let outcomes = match pending.finish(result) {
            Ok(outcomes) => outcomes,
            Err(error) => {
                drop(_kl);
                self.evict_stale_region(&key, h.id, &error);
                return Err(error);
            }
        };
        if let Some(max) = outcomes
            .iter()
            .filter(|outcome| outcome.inserted)
            .map(|outcome| outcome.id)
            .max()
        {
            h.max_id.fetch_max(max, Ordering::Relaxed);
            self.note_sealed_insert(&h);
        }
        Ok(outcomes)
    }

    /// [`remember_if_absent_keyed`](Self::remember_if_absent_keyed) that
    /// supersedes instead of refusing: changed input replaces the atom the key
    /// named, an identical retry replays it. Insert, rebind and the old row's
    /// delete commit together, so a key names one atom and writers serialize.
    /// The old key dies after that commit; a crash between leaves an unnamed key
    /// for reconcile. [`forget_atoms`](Self::forget_atoms) erases for compliance.
    pub fn remember_replacing_keyed(
        &self,
        region: &str,
        atom: AtomInput,
        idempotency_key: &str,
    ) -> Result<RememberOutcome> {
        let mut out =
            self.remember_replacing_keyed_batch(region, vec![(atom, idempotency_key.to_string())])?;
        out.pop()
            .ok_or_else(|| MemError::Invalid("keyed replace returned no outcome".into()))
    }

    /// [`remember_replacing_keyed`](Self::remember_replacing_keyed) over a batch
    /// in one transaction, keeping the batched seal-and-allocate: one fsync for
    /// the batch where a per-atom loop pays one each. Outcomes come back per
    /// entry, in order. Keys must be distinct within a batch; two atoms for one
    /// key refuses rather than guessing.
    pub fn remember_replacing_keyed_batch(
        &self,
        region: &str,
        entries: Vec<(AtomInput, String)>,
    ) -> Result<Vec<RememberOutcome>> {
        let cancel = check_db_cancel(&self.db)?;
        if entries.is_empty() {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            return Ok(Vec::new());
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let (atoms, keys): (Vec<AtomInput>, Vec<String>) = entries.into_iter().unzip();
        validate_distinct_idempotency_keys(&keys)?;
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;
        let vecs = self.vectorise_atoms(&key, &h, &atoms, cancel.as_ref())?;
        let KeyedBatchTags { kinds, tags } =
            keyed_batch_tags(h.identity_mac.as_deref(), &atoms, &keys)?;

        let encrypted = h.atom_wrap.is_some();
        let conn = Connection::open(&self.db)?;
        // One span over both the inserts' key allocation and the supersedes'
        // destruction, so no reconcile reclaims either mid-replace.
        let _kl = encrypted.then(|| self.db.key_lifecycle_lock());
        let edges_guard = self.db.memory_edges_lock();
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        check_cancel(cancel.as_ref())?;
        if let Some(kl) = _kl.as_ref() {
            self.ensure_keyed_replacements_unreserved(&conn, &h, &kinds, &tags, kl)?;
            // Before the txn: a crash cannot leave erased codes under a live segment key.
            self.retire_sealed_segment(&h, &conn, kl)?;
        }
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), atoms.len());
        let result = with_write_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;
            // All resolved before any insert, so no entry supersedes itself.
            let mut bound: Vec<Option<(String, AtomId)>> = Vec::with_capacity(kinds.len());
            for (kind, (key_tag, _)) in kinds.iter().zip(&tags) {
                check_cancel(cancel.as_ref())?;
                bound.push(self.live_keyed_binding(c, &h, kind, key_tag)?);
            }
            // An identical retry writes nothing, so it never reaches the id range.
            let mut atoms: Vec<Option<AtomInput>> = atoms.into_iter().map(Some).collect();
            let mut vecs: Vec<Option<Vec<f32>>> = vecs.into_iter().map(Some).collect();
            let mut writing: Vec<usize> = Vec::with_capacity(kinds.len());
            let mut outcomes: Vec<Option<RememberOutcome>> = vec![None; kinds.len()];
            for i in 0..kinds.len() {
                check_cancel(cancel.as_ref())?;
                match &bound[i] {
                    Some((request, id)) if request == &tags[i].1 => {
                        outcomes[i] = Some(RememberOutcome {
                            id: *id,
                            inserted: false,
                        });
                    }
                    _ => writing.push(i),
                }
            }
            let fresh: Vec<AtomInput> = writing
                .iter()
                .map(|&i| atoms[i].take().expect("each index is taken once"))
                .collect();
            let fresh_vecs: Vec<Vec<f32>> = writing
                .iter()
                .map(|&i| vecs[i].take().expect("each index is taken once"))
                .collect();
            let ids =
                self.insert_atom_rows(c, &h, fresh, fresh_vecs, &mut pending, cancel.as_ref())?;

            let mut stale: Vec<AtomId> = Vec::new();
            for (slot, &i) in writing.iter().enumerate() {
                check_cancel(cancel.as_ref())?;
                let id = ids[slot];
                let params = [
                    Value::Integer(h.id),
                    Value::Text(kinds[i].as_str().into()),
                    Value::Text(tags[i].0.as_str().into()),
                    Value::Text(tags[i].1.as_str().into()),
                    Value::Integer(id),
                ];
                match bound[i].take() {
                    Some((_, superseded)) => {
                        c.execute_params(
                            "UPDATE memory_idempotency SET request_mac = $4, atom_id = $5 \
                             WHERE region_id = $1 AND kind = $2 AND key_mac = $3",
                            &params,
                        )?;
                        stale.push(superseded);
                    }
                    None => {
                        c.execute_params(
                            "INSERT INTO memory_idempotency \
                             (region_id, kind, key_mac, request_mac, atom_id) \
                             VALUES ($1, $2, $3, $4, $5)",
                            &params,
                        )?;
                    }
                }
                outcomes[i] = Some(RememberOutcome { id, inserted: true });
            }

            let doomed = if stale.is_empty() {
                Vec::new()
            } else {
                let in_list = stale
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                // Read while the rows still name their keys; the delete orphans them.
                let slots = if encrypted {
                    atom_key_slots(c, &h, &in_list)?
                } else {
                    Vec::new()
                };
                delete_atoms_in_txn(c, &h, &in_list, &edges_guard)?;
                slots
            };
            let outcomes: Vec<RememberOutcome> = outcomes
                .into_iter()
                .map(|o| o.expect("every entry is replayed or written"))
                .collect();
            check_cancel(cancel.as_ref())?;
            Ok((outcomes, doomed))
        });
        let (out, doomed) = match pending.finish(result) {
            Ok(done) => done,
            Err(error) => {
                drop(edges_guard);
                drop(_kl);
                self.evict_stale_region(&key, h.id, &error);
                return Err(error);
            }
        };
        if !doomed.is_empty() {
            // Non-empty only on the sealed path, which is where the guard is held.
            let kl = _kl.as_ref().expect("sealed slots imply a lifecycle span");
            kl.atom_store_tombstone_batch(&doomed)?;
        }
        if let Some(max) = out.iter().map(|o| o.id).max() {
            h.max_id.fetch_max(max, Ordering::Relaxed);
        }
        if doomed.is_empty() {
            self.note_sealed_insert(&h);
        } else {
            // A deleted row invalidates the cached index outright.
            *h.ann.write().unwrap() = None;
        }
        Ok(out)
    }

    fn ensure_keyed_replacements_unreserved(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        kinds: &[String],
        tags: &[(String, String)],
        lifecycle: &KeyLifecycleGuard<'_>,
    ) -> Result<()> {
        let reserved = lifecycle.memory_atom_callback_ids();
        if reserved.is_empty() {
            return Ok(());
        }
        let reserved_ids = reserved
            .iter()
            .copied()
            .map(|atom_id| {
                i64::try_from(atom_id).map_err(|_| {
                    MemError::Invalid(format!("reserved atom id {atom_id} is out of range"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let planned = kinds
            .iter()
            .zip(tags)
            .map(|(kind, (key_tag, request_tag))| {
                ((kind.as_str(), key_tag.as_str()), request_tag.as_str())
            })
            .collect::<FxHashMap<_, _>>();
        let qr = conn.query_params(
            &format!(
                "SELECT kind, key_mac, request_mac, atom_id FROM memory_idempotency \
                 WHERE region_id = $1 AND atom_id IN ({})",
                id_list(&reserved_ids)
            ),
            &[Value::Integer(h.id)],
        )?;
        for row in &qr.rows {
            let Some(planned_request) = planned.get(&(as_text(&row[0])?, as_text(&row[1])?)) else {
                continue;
            };
            if *planned_request != as_text(&row[2])? {
                let atom_id = u64::try_from(as_int(&row[3])?)
                    .map_err(|_| MemError::Invalid("reserved atom id is out of range".into()))?;
                lifecycle.ensure_memory_atoms_unreserved(&[atom_id])?;
            }
        }
        Ok(())
    }

    /// Resolve a keyed write against the identity table inside the caller's
    /// write transaction: `Some(id)` replays the original atom untouched,
    /// `None` means insert fresh (any stale record was self-healed away),
    /// and the same key bound to a different request tag fails loudly.
    fn keyed_identity_hit(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        kind: &str,
        key_tag: &str,
        request_tag: &str,
    ) -> Result<Option<AtomId>> {
        let Some((bound, atom_id)) = self.live_keyed_binding(conn, h, kind, key_tag)? else {
            return Ok(None);
        };
        if bound != request_tag {
            return Err(MemError::IdempotencyConflict { atom_id });
        }
        Ok(Some(atom_id))
    }

    /// Read-only counterpart used only as a model-work preflight. Stale
    /// bindings read as absent but are left for the authoritative write
    /// transaction to heal.
    fn peek_keyed_identity_hit(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        kind: &str,
        key_tag: &str,
        request_tag: &str,
    ) -> Result<Option<AtomId>> {
        let Some((bound, atom_id)) = self.keyed_binding(conn, h, kind, key_tag, false)? else {
            return Ok(None);
        };
        if bound != request_tag {
            return Err(MemError::IdempotencyConflict { atom_id });
        }
        Ok(Some(atom_id))
    }

    /// The live atom a key names and the request tag it was bound under, inside
    /// the caller's write transaction. A binding whose atom is gone, expired or
    /// key-erased is deleted here and reads as absent, freeing the key.
    fn live_keyed_binding(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        kind: &str,
        key_tag: &str,
    ) -> Result<Option<(String, AtomId)>> {
        self.keyed_binding(conn, h, kind, key_tag, true)
    }

    fn keyed_binding(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        kind: &str,
        key_tag: &str,
        heal_stale: bool,
    ) -> Result<Option<(String, AtomId)>> {
        let qr = conn.query_params(
            "SELECT request_mac, atom_id FROM memory_idempotency \
             WHERE region_id = $1 AND kind = $2 AND key_mac = $3",
            &[
                Value::Integer(h.id),
                Value::Text(kind.into()),
                Value::Text(key_tag.into()),
            ],
        )?;
        let Some(row) = qr.rows.first() else {
            return Ok(None);
        };
        let atom_id = as_int(&row[1])?;
        // Liveness first: expired or key-erased (sealed triple bind) targets
        // count as absent and self-heal, freeing the key for rebinding; only
        // a live binding may refuse a changed request.
        let alive = if h.atom_wrap.is_some() {
            let qr = conn.query_params(
                &format!(
                    "SELECT key_slot, key_gen FROM {} WHERE region_id = $1 AND id = $2 \
                     AND (expires_at IS NULL OR expires_at > $3)",
                    h.table
                ),
                &[
                    Value::Integer(h.id),
                    Value::Integer(atom_id),
                    Value::Timestamp(now_micros()),
                ],
            )?;
            match qr.rows.first() {
                None => false,
                Some(row) => {
                    exact_live_atom_wrapped(&self.db, atom_id, &row[0], &row[1])?.is_some()
                }
            }
        } else {
            let qr = conn.query_params(
                &format!(
                    "SELECT id FROM {} WHERE region_id = $1 AND id = $2 \
                     AND (expires_at IS NULL OR expires_at > $3)",
                    h.table
                ),
                &[
                    Value::Integer(h.id),
                    Value::Integer(atom_id),
                    Value::Timestamp(now_micros()),
                ],
            )?;
            !qr.rows.is_empty()
        };
        if !alive && heal_stale {
            conn.execute_params(
                "DELETE FROM memory_idempotency \
                 WHERE region_id = $1 AND kind = $2 AND key_mac = $3",
                &[
                    Value::Integer(h.id),
                    Value::Text(kind.into()),
                    Value::Text(key_tag.into()),
                ],
            )?;
        }
        if !alive {
            return Ok(None);
        }
        Ok(Some((as_text(&row[0])?.to_string(), atom_id)))
    }

    /// [`remember_derived`](Self::remember_derived) with caller-declared
    /// snapshot validation: every `snapshot` member must be present,
    /// unexpired, and still hash to its declared SHA-256, checked in the
    /// same write transaction as the insert and the `DerivedFrom` edges for
    /// `provenance` (a required subset of the snapshot). Validates only what
    /// the caller declares; serializing ingest against derivation is the
    /// controller's job.
    pub fn remember_derived_checked(
        &self,
        region: &str,
        atom: AtomInput,
        snapshot: &[SourceSnapshot],
        provenance: &[AtomId],
        evidence_ref: Option<serde_json::Value>,
    ) -> Result<AtomId> {
        let mut declared: FxHashSet<AtomId> = FxHashSet::default();
        for member in snapshot {
            if !declared.insert(member.id) {
                return Err(MemError::Invalid(format!(
                    "duplicate snapshot id {}",
                    member.id
                )));
            }
        }
        let src_ids = dedup_sources(provenance);
        if let Some(missing) = src_ids.iter().find(|id| !declared.contains(id)) {
            return Err(MemError::Invalid(format!(
                "provenance atom {missing} is not in the declared snapshot"
            )));
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let cancel = check_db_cancel(&self.db)?;
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;
        let prep = prepare_atom_row(&h, &key, &atom, cancel.as_ref())?;

        let conn = Connection::open(&self.db)?;
        // Sealed inserts allocate keys before their rows commit; hold the guard
        // so a concurrent reconcile cannot reclaim them mid-span.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let edges_guard = self.db.memory_edges_lock();
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), 1);
        let result = with_write_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;
            self.verify_source_snapshot(c, &h, &key, snapshot, cancel.as_ref())?;
            let id = next_id(c, "next_atom_id")?;
            self.insert_atom_row(c, &h, id, atom, prep, &mut pending)?;
            link_derived_sources(c, id, &src_ids, evidence_ref.as_ref(), &edges_guard)?;
            check_cancel(cancel.as_ref())?;
            Ok(id)
        });
        let id = match pending.finish(result) {
            Ok(id) => id,
            Err(error) => {
                drop(edges_guard);
                drop(_kl);
                self.evict_stale_region(&key, h.id, &error);
                return Err(error);
            }
        };
        h.max_id.fetch_max(id, Ordering::Relaxed);
        self.note_sealed_insert(&h);
        Ok(id)
    }

    /// Every snapshot member must be present, unexpired, and its stored text
    /// must still hash to the declared digest - all read inside the caller's
    /// write transaction.
    fn verify_source_snapshot(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        region_key: &str,
        snapshot: &[SourceSnapshot],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<()> {
        let now = now_micros();
        let sealed_wrapped = match &h.atom_wrap {
            Some(_) => {
                let ids: Vec<AtomId> = snapshot.iter().map(|member| member.id).collect();
                exact_live_atom_wrapped_for_ids(&self.db, conn, &h.table, h.id, &ids)?
            }
            None => FxHashMap::default(),
        };
        for member in snapshot {
            check_cancel(cancel)?;
            let id = member.id;
            let text = match &h.atom_wrap {
                None => {
                    let qr = conn.query_params(
                        &format!(
                            "SELECT text_content, expires_at FROM {} \
                             WHERE region_id = $1 AND id = $2",
                            h.table
                        ),
                        &[Value::Integer(h.id), Value::Integer(id)],
                    )?;
                    let Some(row) = qr.rows.first() else {
                        return Err(MemError::Invalid(format!(
                            "source atom {id} not in region '{region_key}'"
                        )));
                    };
                    verify_snapshot_unexpired(&row[1], id, now)?;
                    match &row[0] {
                        Value::Text(t) => t.to_string(),
                        other => {
                            return Err(MemError::Invalid(format!(
                                "atom text is not text: {other:?}"
                            )))
                        }
                    }
                }
                Some(atom_wrap) => {
                    let qr = conn.query_params(
                        &format!(
                            "SELECT sealed, key_slot, key_gen, expires_at FROM {} \
                             WHERE region_id = $1 AND id = $2",
                            h.table
                        ),
                        &[Value::Integer(h.id), Value::Integer(id)],
                    )?;
                    let Some(row) = qr.rows.first() else {
                        return Err(MemError::Invalid(format!(
                            "source atom {id} not in region '{region_key}'"
                        )));
                    };
                    verify_snapshot_unexpired(&row[3], id, now)?;
                    let Some(wrapped) = sealed_wrapped.get(&id) else {
                        return Err(MemError::Invalid(format!(
                            "source atom {id} not in region '{region_key}'"
                        )));
                    };
                    open_atom_text(atom_wrap, wrapped, id, as_blob(&row[0])?)?
                }
            };
            let text = Zeroizing::new(text);
            let got: [u8; 32] = Sha256::digest(text.as_bytes()).into();
            if got != member.text_sha256 {
                return Err(MemError::Invalid(format!(
                    "source atom {id} text changed since it was digested"
                )));
            }
        }
        check_cancel(cancel)?;
        Ok(())
    }

    /// Bump the epoch so other handles rebuild; the caller's current cache re-stamps.
    fn note_sealed_insert(&self, h: &RegionHandle) {
        if h.atom_wrap.is_none() {
            return;
        }
        let new_epoch = self.db.bump_cache_epoch();
        let table_stamp = atom_table_root_stamp(&self.db, &h.table).ok();
        let mut guard = h.ann.write().unwrap();
        if let Some(sa) = guard.as_mut() {
            if sa.build_epoch == new_epoch - 1 {
                if let Some(table_stamp) = table_stamp {
                    sa.build_epoch = new_epoch;
                    sa.table_stamp = table_stamp;
                } else {
                    *guard = None;
                }
            }
        }
    }

    /// Write one atom row inside the caller's transaction.
    fn insert_atom_row(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        id: AtomId,
        atom: AtomInput,
        prep: PreparedAtomRow,
        pending: &mut PendingAtomSlots<'_>,
    ) -> Result<()> {
        let table = &h.table;
        if let Some(atom_wrap) = &h.atom_wrap {
            let (sealed, wrapped) = seal_atom(atom_wrap, id, &prep.vec, &atom.text, &prep.payload);
            // Fsync the ACK first so a committed row references a durable key slot.
            let (slot, gen) = self.db.atom_store_allocate_write(id as u64, &wrapped)?;
            pending.track(slot, id as u64, gen);
            insert_sealed_atom(
                conn,
                table,
                id,
                h.id,
                &atom.kind,
                sealed,
                slot,
                gen,
                atom.importance,
                atom.confidence,
                prep.immutable,
                prep.created,
                prep.expires,
            )?;
        } else {
            conn.execute_params(
                &format!(
                    "INSERT INTO {table} \
                     (id, region_id, kind, embedding, payload, text_content, score, confidence, \
                      access_count, immutable, created_at, accessed_at, expires_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, \
                      $10, CURRENT_TIMESTAMP, $11)"
                ),
                &[
                    Value::Integer(id),
                    Value::Integer(h.id),
                    Value::Text(atom.kind.into()),
                    Value::Vector(prep.vec.into()),
                    Value::Text(prep.payload.into()),
                    Value::Text(atom.text.into()),
                    Value::Real(atom.importance as f64),
                    Value::Real(atom.confidence as f64),
                    Value::Integer(prep.immutable),
                    prep.created,
                    prep.expires,
                ],
            )?;
        }
        Ok(())
    }

    /// Oldest live atom in the region with this exact `kind` + `text`, if any.
    fn find_atom_by_text(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        kind: &str,
        text: &str,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Option<AtomId>> {
        let table = &h.table;
        let Some(atom_wrap) = &h.atom_wrap else {
            let qr = conn.query_params(
                &format!(
                    "SELECT id FROM {table} WHERE region_id = $1 AND kind = $2 \
                     AND text_content = $3 AND (expires_at IS NULL OR expires_at > $4) \
                     ORDER BY id LIMIT 1"
                ),
                &[
                    Value::Integer(h.id),
                    Value::Text(kind.into()),
                    Value::Text(text.into()),
                    Value::Timestamp(now_micros()),
                ],
            )?;
            check_cancel(cancel)?;
            return qr.rows.first().map(|r| as_int(&r[0])).transpose();
        };
        let qr = conn.query_params(
            &format!(
                "SELECT id, sealed, key_slot, key_gen FROM {table} WHERE region_id = $1 \
                 AND kind = $2 AND (expires_at IS NULL OR expires_at > $3) ORDER BY id"
            ),
            &[
                Value::Integer(h.id),
                Value::Text(kind.into()),
                Value::Timestamp(now_micros()),
            ],
        )?;
        let wrapped = exact_live_atom_wrapped_rows(&self.db, &qr.rows, 0, 2, 3)?;
        for (row, wrapped) in qr.rows.iter().zip(wrapped) {
            check_cancel(cancel)?;
            let id = as_int(&row[0])?;
            // Erased/recycled key: unrecoverable text cannot match - skip like recall.
            let Some(wrapped) = wrapped else {
                continue;
            };
            let row_text =
                Zeroizing::new(open_atom_text(atom_wrap, &wrapped, id, as_blob(&row[1])?)?);
            let hit = row_text.as_str() == text;
            if hit {
                check_cancel(cancel)?;
                return Ok(Some(id));
            }
        }
        check_cancel(cancel)?;
        Ok(None)
    }

    /// Embed + store atoms in one transaction; faster than looping `remember`.
    pub fn remember_batch(&self, region: &str, atoms: Vec<AtomInput>) -> Result<Vec<AtomId>> {
        let cancel = check_db_cancel(&self.db)?;
        if atoms.is_empty() {
            return Ok(Vec::new());
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;

        let vecs = self.vectorise_atoms(&key, &h, &atoms, cancel.as_ref())?;
        let conn = Connection::open(&self.db)?;
        // Sealed inserts allocate keys before their rows commit; hold the guard
        // so a concurrent reconcile cannot reclaim them mid-span.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let mut pending = PendingAtomSlots::new(_kl.as_ref(), atoms.len());
        let result = with_write_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;
            let ids = self.insert_atom_rows(c, &h, atoms, vecs, &mut pending, cancel.as_ref())?;
            check_cancel(cancel.as_ref())?;
            Ok(ids)
        });
        let ids = match pending.finish(result) {
            Ok(ids) => ids,
            Err(error) => {
                drop(_kl);
                self.evict_stale_region(&key, h.id, &error);
                return Err(error);
            }
        };
        if let Some(&last) = ids.last() {
            h.max_id.fetch_max(last, Ordering::Relaxed);
            self.note_sealed_insert(&h);
        }
        Ok(ids)
    }

    /// Validate and vectorise atoms before any transaction: one shared input
    /// boundary, and the embedder never runs with a write txn open.
    fn vectorise_atoms(
        &self,
        key: &str,
        h: &RegionHandle,
        atoms: &[AtomInput],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<Vec<f32>>> {
        for atom in atoms {
            check_cancel(cancel)?;
            validate_atom_input(atom)?;
        }

        // Only atoms without a supplied vector reach the embedder.
        let texts: Vec<&str> = atoms
            .iter()
            .filter(|a| a.embedding.is_none())
            .map(|a| a.text.as_str())
            .collect();
        let embedded = if texts.is_empty() {
            Vec::new()
        } else {
            check_cancel(cancel)?;
            let embedded = h.embedder.embed_with_cancel(&texts, cancel);
            check_cancel(cancel)?;
            embedded?
        };
        if embedded.len() != texts.len() {
            return Err(MemError::Invalid(format!(
                "embedder returned {} vectors for {} texts",
                embedded.len(),
                texts.len()
            )));
        }
        let mut taken = 0usize;
        let mut vecs: Vec<Vec<f32>> = Vec::with_capacity(atoms.len());
        for atom in atoms {
            check_cancel(cancel)?;
            let vector = match &atom.embedding {
                Some(supplied) => supplied.clone(),
                None => {
                    let v = embedded[taken].clone();
                    taken += 1;
                    v
                }
            };
            validate_embedding(key, h.dim, &vector, "passage")?;
            vecs.push(vector);
        }
        Ok(vecs)
    }

    /// Insert vectorised atoms inside the caller's transaction, sealing and
    /// allocating every key in one batch (one fsync, not one per atom).
    fn insert_atom_rows(
        &self,
        c: &Connection<'_>,
        h: &RegionHandle,
        atoms: Vec<AtomInput>,
        vecs: Vec<Vec<f32>>,
        pending: &mut PendingAtomSlots<'_>,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<AtomId>> {
        let table = &h.table;
        let n = atoms.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        let count = i64::try_from(n)
            .map_err(|_| MemError::Invalid(format!("atom batch size {n} is out of range")))?;
        let start = next_id_range(c, "next_atom_id", count)?;
        let ids: Vec<AtomId> = (0..count).map(|offset| start + offset).collect();
        {
            if let Some(atom_wrap) = &h.atom_wrap {
                // Seal all atoms, persist their wrapped ACKs with one fsync.
                let mut sealed_blobs: Vec<Vec<u8>> = Vec::with_capacity(n);
                let mut key_items: Vec<(u64, [u8; WRAPPED_KEY_SIZE])> = Vec::with_capacity(n);
                for ((atom, vec), &id) in atoms.iter().zip(&vecs).zip(&ids) {
                    check_cancel(cancel)?;
                    let payload = serde_json::to_string(&atom.payload)
                        .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;
                    let (sealed, wrapped) = seal_atom(atom_wrap, id, vec, &atom.text, &payload);
                    sealed_blobs.push(sealed);
                    key_items.push((id as u64, wrapped));
                }
                let slots = self.db.atom_store_allocate_batch(&key_items)?;
                #[cfg(test)]
                if let Some(token) =
                    CANCEL_AFTER_ATOM_KEY_ALLOCATION.with(|slot| slot.borrow_mut().take())
                {
                    token.cancel();
                }
                for (&id, &(slot, generation)) in ids.iter().zip(&slots) {
                    pending.track(slot, id as u64, generation);
                }
                for (((atom, &id), sealed), &(slot, gen)) in
                    atoms.iter().zip(&ids).zip(sealed_blobs).zip(&slots)
                {
                    check_cancel(cancel)?;
                    let expires = atom.expires_at.map(Value::Timestamp).unwrap_or(Value::Null);
                    let created = Value::Timestamp(atom.created_at.unwrap_or_else(now_micros));
                    insert_sealed_atom(
                        c,
                        table,
                        id,
                        h.id,
                        &atom.kind,
                        sealed,
                        slot,
                        gen,
                        atom.importance,
                        atom.confidence,
                        i64::from(atom.immutable),
                        created,
                        expires,
                    )?;
                }
            } else {
                let plaintext_sql = format!(
                    "INSERT INTO {table} \
                     (id, region_id, kind, embedding, payload, text_content, score, confidence, \
                      access_count, immutable, created_at, accessed_at, expires_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, \
                      $10, CURRENT_TIMESTAMP, $11)"
                );
                for ((atom, vec), &id) in atoms.into_iter().zip(vecs).zip(&ids) {
                    check_cancel(cancel)?;
                    let payload = serde_json::to_string(&atom.payload)
                        .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;
                    let expires = atom.expires_at.map(Value::Timestamp).unwrap_or(Value::Null);
                    let created = Value::Timestamp(atom.created_at.unwrap_or_else(now_micros));
                    c.execute_params(
                        &plaintext_sql,
                        &[
                            Value::Integer(id),
                            Value::Integer(h.id),
                            Value::Text(atom.kind.into()),
                            Value::Vector(vec.into()),
                            Value::Text(payload.into()),
                            Value::Text(atom.text.into()),
                            Value::Real(atom.importance as f64),
                            Value::Real(atom.confidence as f64),
                            Value::Integer(i64::from(atom.immutable)),
                            created,
                            expires,
                        ],
                    )?;
                }
            }
        }
        check_cancel(cancel)?;
        Ok(ids)
    }

    /// Fetch up to `limit` atoms of `kind` (optional JSONB `@>` filter).
    /// Encrypted regions decrypt in id order, paging to honor `limit`.
    pub fn fetch(
        &self,
        region: &str,
        kind: &str,
        payload_filter: Option<&serde_json::Value>,
        limit: usize,
    ) -> Result<Vec<AtomHit>> {
        let mut q = FetchQuery::new(limit).with_kind(kind);
        q.payload_filter = payload_filter.cloned();
        self.fetch_range(region, &q)
    }

    /// Deterministic id-order listing; resume by passing the last id as `after_id`.
    pub fn fetch_range(&self, region: &str, q: &FetchQuery) -> Result<Vec<AtomHit>> {
        let mut hits = self.fetch_range_materialized(region, q)?;
        charge_returned_hits(&mut hits)?;
        Ok(hits)
    }

    fn fetch_range_materialized(&self, region: &str, q: &FetchQuery) -> Result<Vec<AtomHit>> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if q.limit == 0 {
            return Ok(Vec::new());
        }
        let hits = if h.atom_wrap.is_some() {
            self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                self.fetch_sealed(&h, q, conn, atom_wrap, cancel.as_ref())
            })?
        } else {
            self.with_live_plain_access(&key, &h, |conn| {
                maintenance_fetch_plain(conn, h.id, &h.table, q, cancel.as_ref())
            })?
        };
        check_cancel(cancel.as_ref())?;
        Ok(hits)
    }

    /// Read one deterministic page without falsely advertising a terminal cursor.
    pub fn fetch_page(&self, region: &str, query: &FetchQuery) -> Result<FetchPage> {
        let mut page =
            fetch_page_result(query, |paged| self.fetch_range_materialized(region, paged))?;
        charge_returned_hits(&mut page.atoms)?;
        Ok(page)
    }

    /// Count atoms of `kind` without materializing them (`kind` is plaintext in
    /// both flavors). A sealed region counts only atoms whose key is live.
    pub fn count(&self, region: &str, kind: &str) -> Result<u64> {
        self.count_matching(region, Some(kind))
    }

    /// Count every live atom in the region, whatever its kind. Callers that must show a
    /// region's size have no kind to name and no way to enumerate the kinds present, so
    /// without this the only options are paging to exhaustion or reading the per-region
    /// atoms table directly.
    pub fn count_region(&self, region: &str) -> Result<u64> {
        self.count_matching(region, None)
    }

    /// `None` counts every kind. TTL-expired rows are excluded either way, and a sealed
    /// region still counts only atoms whose key is live.
    fn count_matching(&self, region: &str, kind: Option<&str>) -> Result<u64> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let mut params: Vec<Value> = vec![Value::Integer(h.id)];
        let kind_pred = match kind {
            Some(kind) => {
                params.push(Value::Text(kind.into()));
                " AND kind = $2"
            }
            None => "",
        };
        // Numbered from what precedes it, so dropping the kind predicate does not leave a
        // dangling parameter index.
        params.push(Value::Timestamp(now_micros()));
        let ttl_pred = format!(
            " AND (expires_at IS NULL OR expires_at > ${})",
            params.len()
        );
        if h.atom_wrap.is_some() {
            let live = self.with_live_sealed_read(&key, &h, |conn, _, _kl| {
                let qr = conn.query_params(
                    &format!(
                        "SELECT id, key_slot, key_gen FROM {table} \
                         WHERE region_id = $1{kind_pred}{ttl_pred}",
                        table = h.table
                    ),
                    &params,
                )?;
                if qr.rows.is_empty() {
                    return Ok(0);
                }
                let wrapped = exact_live_atom_wrapped_rows(&self.db, &qr.rows, 0, 1, 2)?;
                let mut live = 0;
                for entry in wrapped {
                    #[cfg(test)]
                    debug_fire_cancel_after_local_work();
                    check_cancel(cancel.as_ref())?;
                    if entry.is_some() {
                        live += 1;
                    }
                }
                Ok(live)
            })?;
            check_cancel(cancel.as_ref())?;
            return Ok(live);
        }
        let count = self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(
                &format!(
                    "SELECT COUNT(*) FROM {table} WHERE region_id = $1{kind_pred}{ttl_pred}",
                    table = h.table
                ),
                &params,
            )?;
            match qr.rows.first().and_then(|r| r.first()) {
                Some(Value::Integer(n)) => Ok(*n as u64),
                other => Err(MemError::Invalid(format!(
                    "COUNT returned no integer: {other:?}"
                ))),
            }
        })?;
        check_cancel(cancel.as_ref())?;
        Ok(count)
    }

    /// Exact (unnormalized) data path: a binding check for external cache-set locks.
    pub fn database_data_path(&self) -> &Path {
        self.db.data_path()
    }

    pub(crate) fn database_identity(&self) -> std::sync::Weak<Database> {
        Arc::downgrade(&self.db)
    }

    /// Run one operation with a request-scoped cancellation token.
    ///
    /// The token applies only to work started synchronously by `operation` on
    /// the current thread. Concurrent operations over the same database do not
    /// inherit it. Nested scopes restore the outer token on every exit, including
    /// panic unwinding. A clone may be cancelled from another thread.
    pub fn with_cancel_token<T>(
        &self,
        token: citadel_core::CancelToken,
        operation: impl FnOnce(&Self) -> T,
    ) -> T {
        self.db.with_cancel_token(token, || operation(self))
    }

    /// Persisted region identities; a half-erased encrypted region fails closed.
    pub fn stored_region_identities(&self) -> Result<Vec<StoredRegionIdentity>> {
        let identities = self.stored_region_identities_materialized()?;
        charge_returned_text(
            identities
                .iter()
                .flat_map(|identity| [identity.name(), identity.model_id()]),
        )?;
        Ok(identities)
    }

    fn stored_region_identities_materialized(&self) -> Result<Vec<StoredRegionIdentity>> {
        let cancel = check_db_cancel(&self.db)?;
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            "SELECT name, id, embedding_dim, embedding_metric, model_id, encrypted, \
             rsk_slot, rsk_gen FROM memory_regions",
            &[],
        )?;
        let mut identities = Vec::with_capacity(qr.rows.len());
        for row in &qr.rows {
            check_cancel(cancel.as_ref())?;
            let name = as_text(&row[0])?;
            let persisted = parse_region_row(&row[1..])?;
            if persisted.encrypted {
                self.verify_region_key_live(name, &persisted)?;
            }
            identities.push(StoredRegionIdentity::new(
                name.to_owned(),
                persisted.encrypted,
                persisted.dim,
                persisted.metric,
                persisted.model_id,
            ));
        }
        identities.sort_by(|left, right| left.name().cmp(right.name()));
        check_cancel(cancel.as_ref())?;
        Ok(identities)
    }

    /// One persisted region identity; an encrypted row must still have a live RSK.
    pub fn stored_region_identity(&self, name: &str) -> Result<Option<StoredRegionIdentity>> {
        let cancel = check_db_cancel(&self.db)?;
        let key = name.to_ascii_lowercase();
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let Some(persisted) = self.load_region_row(&conn, &key)? else {
            check_cancel(cancel.as_ref())?;
            return Ok(None);
        };
        if persisted.encrypted {
            self.verify_region_key_live(&key, &persisted)?;
        }
        check_cancel(cancel.as_ref())?;
        let identity = StoredRegionIdentity::new(
            key,
            persisted.encrypted,
            persisted.dim,
            persisted.metric,
            persisted.model_id,
        );
        charge_returned_text([identity.name(), identity.model_id()])?;
        Ok(Some(identity))
    }

    /// Exact persisted region names, sorted deterministically.
    pub fn stored_region_names(&self) -> Result<Vec<String>> {
        let cancel = check_db_cancel(&self.db)?;
        let identities = self.stored_region_identities_materialized()?;
        let mut names = Vec::with_capacity(identities.len());
        for identity in identities {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            names.push(identity.name().to_owned());
        }
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        check_cancel(cancel.as_ref())?;
        charge_returned_text(names.iter().map(String::as_str))?;
        Ok(names)
    }

    /// Stored kinds: storage inventory (expired rows stay visible), never decrypts.
    pub fn stored_atom_kinds(&self, region: &str) -> Result<Vec<String>> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let row = self.load_live_region_row(&conn, &key)?;
        let table = atoms_table(row.dim, row.metric, row.encrypted);
        let qr = conn.query_params(
            &format!("SELECT DISTINCT kind FROM {table} WHERE region_id = $1"),
            &[Value::Integer(row.id)],
        )?;
        let mut kinds = Vec::with_capacity(qr.rows.len());
        for stored in &qr.rows {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            kinds.push(as_text(&stored[0])?.to_owned());
        }
        check_cancel(cancel.as_ref())?;
        kinds.sort();
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        check_cancel(cancel.as_ref())?;
        charge_returned_text(kinds.iter().map(String::as_str))?;
        Ok(kinds)
    }

    /// Content-free per-row storage inventory, not recall eligibility; no decryption.
    pub fn stored_atom_retrieval_state(
        &self,
        region: &str,
    ) -> Result<Vec<StoredAtomRetrievalState>> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let _kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        let row = self.load_live_region_row(&conn, &key)?;
        let table = atoms_table(row.dim, row.metric, row.encrypted);
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, score, expires_at FROM {table} WHERE region_id = $1 ORDER BY id"
            ),
            &[Value::Integer(row.id)],
        )?;
        let mut states = Vec::with_capacity(qr.rows.len());
        for stored in &qr.rows {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            states.push(StoredAtomRetrievalState::new(
                as_int(&stored[0])?,
                as_text(&stored[1])?.to_owned(),
                exact_f32_bits(&stored[2])?,
                exact_opt_ts(&stored[3])?,
            ));
        }
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        check_cancel(cancel.as_ref())?;
        charge_returned_text(states.iter().map(StoredAtomRetrievalState::kind))?;
        Ok(states)
    }

    /// SHA-256 identity of live stored embeddings; row-at-a-time decrypt, zeroized.
    pub fn stored_embeddings_identity(
        &self,
        region: &str,
        kind: &str,
    ) -> Result<StoredEmbeddingsIdentity> {
        self.scan_stored_embeddings(region, kind, None)
    }

    /// Verify exact stored f32 bits; `expected` = every live atom in ascending id order.
    pub fn verify_stored_embeddings_exact(
        &self,
        region: &str,
        kind: &str,
        expected: &[(AtomId, Vec<f32>)],
    ) -> Result<StoredEmbeddingsIdentity> {
        self.scan_stored_embeddings(region, kind, Some(expected))
    }

    fn scan_stored_embeddings(
        &self,
        region: &str,
        kind: &str,
        expected: Option<&[(AtomId, Vec<f32>)]>,
    ) -> Result<StoredEmbeddingsIdentity> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if let Some(expected) = expected {
            validate_expected_embeddings(expected, usize::from(h.dim))?;
            check_cancel(cancel.as_ref())?;
        }

        let params = [
            Value::Integer(h.id),
            Value::Text(kind.into()),
            Value::Timestamp(now_micros()),
        ];
        let mut scan = StoredEmbeddingScan::new(&key, kind, h.dim, expected);
        if h.atom_wrap.is_some() {
            let result = self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                let qr = conn.query_params(
                    &format!(
                        "SELECT id, sealed, key_slot, key_gen FROM {table} \
                         WHERE region_id = $1 AND kind = $2 \
                         AND (expires_at IS NULL OR expires_at > $3) ORDER BY id",
                        table = h.table
                    ),
                    &params,
                )?;
                let wrapped = exact_live_atom_wrapped_rows(&self.db, &qr.rows, 0, 2, 3)?;
                for (row, wrapped_ack) in qr.rows.iter().zip(wrapped) {
                    check_cancel(cancel.as_ref())?;
                    let id = as_int(&row[0])?;
                    let Some(wrapped_ack) = wrapped_ack else {
                        continue;
                    };
                    let mut embedding =
                        open_atom_embedding(atom_wrap, &wrapped_ack, id, as_blob(&row[1])?)?;
                    let result = scan.consume(id, &embedding);
                    embedding.zeroize();
                    result?;
                }
                scan.finish()
            })?;
            check_cancel(cancel.as_ref())?;
            charge_returned_embedding_identity(&result)?;
            return Ok(result);
        }

        let result = self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(
                &format!(
                    "SELECT id, embedding FROM {table} WHERE region_id = $1 AND kind = $2 \
                     AND (expires_at IS NULL OR expires_at > $3) ORDER BY id",
                    table = h.table
                ),
                &params,
            )?;
            for row in &qr.rows {
                check_cancel(cancel.as_ref())?;
                let id = as_int(&row[0])?;
                let embedding = match &row[1] {
                    Value::Vector(vector) => vector.as_ref(),
                    other => {
                        return Err(MemError::Invalid(format!(
                            "stored embedding for atom {id} is not a vector: {other:?}"
                        )))
                    }
                };
                scan.consume(id, embedding)?;
            }
            scan.finish()
        })?;
        check_cancel(cancel.as_ref())?;
        charge_returned_embedding_identity(&result)?;
        Ok(result)
    }

    /// Freeze the region's ANN index into a persisted segment so a cold attach
    /// loads it instead of rebuilding. Sealed regions seal the segment under a
    /// random erasable-store key, so destroying that slot crypto-erases every
    /// on-disk embedding derivative.
    pub fn persist_ann_index(&self, region: &str) -> Result<AnnSegmentInfo> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, _, _kl| {
                self.persist_sealed_segment(&h, conn, _kl, cancel.as_ref())
            });
        }
        self.with_live_plain_access(&key, &h, |conn| {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            Ok(conn.persist_ann_index(&h.table, "embedding")?)
        })
    }

    /// [`ann_cache_status`](Self::ann_cache_status) plus whether the entry is current.
    pub fn ann_cache_status_current(&self, region: &str) -> Result<Option<(AnnIndexSource, bool)>> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let status = if h.atom_wrap.is_some() {
            self.with_live_sealed_read(&key, &h, |_, _, _| {
                let table_stamp = atom_table_root_stamp(&self.db, &h.table)?;
                Ok(h.ann.read().unwrap().as_ref().map(|sa| {
                    (
                        sa.source.clone(),
                        sa.build_epoch == self.db.cache_epoch() && sa.table_stamp == table_stamp,
                    )
                }))
            })?
        } else {
            self.with_live_plain_access(&key, &h, |conn| {
                Ok(conn
                    .ann_cache_status(&h.table, "embedding")?
                    .map(|(source, _)| (source, true)))
            })?
        };
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        check_cancel(cancel.as_ref())?;
        Ok(status)
    }

    /// Serving ANN index: `Loaded` or `Built`; `None` if unbuilt or epoch-stale.
    pub fn ann_cache_status(&self, region: &str) -> Result<Option<AnnIndexSource>> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let status = if h.atom_wrap.is_some() {
            self.with_live_sealed_read(&key, &h, |_, _, _| {
                let table_stamp = atom_table_root_stamp(&self.db, &h.table)?;
                Ok(h.ann
                    .read()
                    .unwrap()
                    .as_ref()
                    .filter(|sa| {
                        sa.build_epoch == self.db.cache_epoch() && sa.table_stamp == table_stamp
                    })
                    .map(|sa| sa.source.clone()))
            })?
        } else {
            self.with_live_plain_access(&key, &h, |conn| {
                Ok(conn
                    .ann_cache_status(&h.table, "embedding")?
                    .map(|(source, _)| source))
            })?
        };
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        check_cancel(cancel.as_ref())?;
        Ok(status)
    }

    /// Persist a sealed region's ANN graph: scan + decrypt (with the
    /// liveness-aware fingerprint), build the PRISM index, and seal it under a
    /// fresh segment key held only in the erasable store under a pseudo-atom
    /// id. Chunks go to a hidden tree scoped by region id and atom table.
    fn persist_sealed_segment(
        &self,
        h: &RegionHandle,
        conn: &Connection<'_>,
        kl: &KeyLifecycleGuard<'_>,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<AnnSegmentInfo> {
        let atom_wrap = h.atom_wrap.as_ref().expect("sealed persist");

        let mut kind_codes: FxHashMap<String, u32> = FxHashMap::default();
        let mut triples: Vec<(u64, Vec<f32>, Vec<u32>)> = Vec::new();
        let fingerprint = sealed_fp_scan(
            conn,
            &self.db,
            h,
            cancel,
            &mut |id, kind, sealed, wrapped, _, _, _, _, _| {
                let emb = open_atom_embedding(atom_wrap, wrapped, id, sealed)?;
                let next = kind_codes.len() as u32;
                let code = *kind_codes.entry(kind.to_string()).or_insert(next);
                triples.push((id as u64, emb, vec![code]));
                Ok(true)
            },
        )?
        .0;
        check_cancel(cancel)?;
        if triples.is_empty() {
            return Err(MemError::Invalid(
                "nothing to persist: the sealed region has no live atoms".into(),
            ));
        }
        let n = triples.len() as u64;
        let index = AnnIndex::build_with_attrs(triples, 1, ann_metric(h.metric), h.dim)
            .map_err(|e| MemError::Invalid(format!("sealed ANN build: {e}")))?;
        check_cancel(cancel)?;

        // Inner plaintext: [fp 32][config_hash 32][kind_codes][segment body];
        // zeroized after seal.
        let body = citadel_vector::segment::encode_with_cancel(&index, cancel)
            .map_err(segment_operation_error)?;
        let mut inner = Zeroizing::new(Vec::with_capacity(body.len() + 256));
        inner.extend_from_slice(&fingerprint);
        // Pin the PRISM config (incl. search-geometry version): a binary whose
        // active config differs must refuse the segment and rebuild from rows.
        inner.extend_from_slice(&citadel_vector::segment::prism_config_hash(
            &AnnIndex::active_config(ann_metric(h.metric)),
        ));
        inner.extend_from_slice(&(kind_codes.len() as u32).to_le_bytes());
        let mut kinds: Vec<(&String, &u32)> = kind_codes.iter().collect();
        check_cancel(cancel)?;
        kinds.sort_by_key(|&(_, code)| *code);
        check_cancel(cancel)?;
        for (kind, &code) in kinds {
            check_cancel(cancel)?;
            inner.extend_from_slice(&(kind.len() as u32).to_le_bytes());
            inner.extend_from_slice(kind.as_bytes());
            inner.extend_from_slice(&code.to_le_bytes());
        }
        inner.extend_from_slice(&body);

        // Allocating the pseudo-id is the first durable write. Everything
        // above is reversible local work, so observe this operation's token
        // once more before crossing that boundary.
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        check_cancel(cancel)?;

        // Seal under a fresh segment key; the pseudo-atom id binds the AAD and
        // owns the erasable slot. Drawn from the atom-id sequence, so it can
        // never collide with a real atom's slot.
        let pseudo_id = with_write_txn(conn, |c| next_id(c, "next_atom_id"))?;
        let pseudo_owner = u64::try_from(pseudo_id).map_err(|_| {
            MemError::Invalid(format!(
                "allocated sealed ANN id {pseudo_id} is out of range"
            ))
        })?;
        use rand::RngCore;
        let mut sk = Zeroizing::new([0u8; citadel_core::KEY_SIZE]);
        rand::thread_rng().fill_bytes(sk.as_mut());
        let seal_keys = derive_seal_keys(&sk);
        let sealed = blob_seal::seal(&seal_keys, pseudo_owner, &inner);
        let segment_b3 = citadel_vector::segment::digest_with_cancel(&sealed, cancel)
            .map_err(segment_operation_error)?;
        let wrapped_sk = atom_wrap.wrap_atom_key(&sk);

        // Retire any previous segment first (old key must not survive as
        // decryptable residue), then key-before-data like atoms.
        // Catch cancellation that arrives while sealing before destroying an
        // existing segment. The consumed sequence value is harmless.
        check_cancel(cancel)?;
        self.retire_sealed_segment(h, conn, kl)?;
        let mut pending = PendingAtomSlots::new(Some(kl), 1);
        let seg_table = sealed_segment_table(&h.table, h.id);
        let result = (|| {
            let (slot, gen) = self
                .db
                .atom_store_allocate_write(pseudo_owner, &wrapped_sk)?;
            pending.track(slot, pseudo_owner, gen);

            {
                let mut wtx = self.db.begin_write()?;
                match wtx.drop_table(seg_table.as_bytes()) {
                    Ok(()) | Err(citadel_core::Error::TableNotFound(_)) => {}
                    Err(e) => return Err(e.into()),
                }
                wtx.create_table(seg_table.as_bytes())?;
                let chunk_count = sealed.len().div_ceil(SEALED_SEG_CHUNK) as u32;
                wtx.table_insert(
                    seg_table.as_bytes(),
                    &0u32.to_be_bytes(),
                    &chunk_count.to_le_bytes(),
                )?;
                for (i, chunk) in sealed.chunks(SEALED_SEG_CHUNK).enumerate() {
                    wtx.table_insert(seg_table.as_bytes(), &((i + 1) as u32).to_be_bytes(), chunk)?;
                }
                wtx.commit()?;
            }
            #[cfg(test)]
            if FAIL_SEALED_SEGMENT_AFTER_CHUNKS.with(std::cell::Cell::take) {
                return Err(MemError::Invalid(
                    "injected sealed-segment failure after chunk commit".into(),
                ));
            }
            write_annseg_meta(conn, h.id, slot, gen, pseudo_id)?;

            Ok(AnnSegmentInfo {
                segment_b3,
                content_fingerprint: fingerprint,
                n,
                dim: h.dim,
                metric_tag: citadel_vector::segment::metric_tag(ann_metric(h.metric)),
                chunk_count: sealed.len().div_ceil(SEALED_SEG_CHUNK) as u32,
            })
        })();
        match pending.finish(result) {
            Ok(info) => Ok(info),
            Err(source) => {
                // Finish every data cleanup step before surfacing the primary failure.
                match self.cleanup_failed_sealed_segment(conn, h.id, &seg_table) {
                    Ok(()) => Err(source),
                    Err(cleanup) => Err(MemError::Invalid(format!(
                        "{source}; additionally failed to clean pending sealed segment: {cleanup}"
                    ))),
                }
            }
        }
    }

    /// Try to serve the persisted segment: unwrap its key, decrypt, decode, and
    /// rehydrate from live rows whose fingerprint must match the sealed one.
    /// Any persisted-state failure returns a refusal for the caller to retire
    /// after releasing the cache lock, then falls back to a scan build.
    #[allow(clippy::type_complexity)]
    fn try_load_sealed_segment(
        &self,
        h: &RegionHandle,
        conn: &Connection<'_>,
        epoch: u64,
        table_stamp: (PageId, TxnId),
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<std::result::Result<SealedAnn, Option<String>>> {
        use zeroize::Zeroize;
        let atom_wrap = h.atom_wrap.as_ref().expect("sealed load");
        let Some((slot, gen, pseudo_id)) = read_annseg_meta(conn, h.id)? else {
            return Ok(Err(None));
        };
        let refuse = |why: &str| -> Result<std::result::Result<SealedAnn, Option<String>>> {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel)?;
            Ok(Err(Some(why.to_string())))
        };
        let rec = match self.db.atom_store_slot(slot) {
            Ok(rec) => rec,
            Err(e) => return refuse(&format!("slot read: {e}")),
        };
        if rec.state != citadel::SlotState::Live || rec.region_id != pseudo_id || rec.gen != gen {
            return refuse(&format!(
                "slot mismatch: state={:?} owner={} (want {pseudo_id}) gen={} (want {gen})",
                rec.state, rec.region_id, rec.gen
            ));
        }

        let seg_table = sealed_segment_table(&h.table, h.id);
        let sealed = {
            let mut rtx = self.db.begin_read();
            let Ok(Some(count_bytes)) = rtx.table_get(seg_table.as_bytes(), &0u32.to_be_bytes())
            else {
                return refuse("chunk count row missing");
            };
            let count = u32::from_le_bytes(match count_bytes.as_slice().try_into() {
                Ok(b) => b,
                Err(_) => return refuse("chunk count malformed"),
            });
            if !plausible_chunk_count(count) {
                return refuse("chunk count implausible");
            }
            let mut sealed = Vec::new();
            for i in 1..=count {
                check_cancel(cancel)?;
                match rtx.table_get(seg_table.as_bytes(), &i.to_be_bytes()) {
                    Ok(Some(chunk)) => sealed.extend_from_slice(&chunk),
                    _ => return refuse("chunk missing"),
                }
            }
            check_cancel(cancel)?;
            sealed
        };

        let mut sk = match atom_wrap.unwrap_atom_key(&rec.wrapped) {
            Ok(sk) => sk,
            Err(_) => return refuse("segment key unwrap failed"),
        };
        let seal_keys = derive_seal_keys(&sk);
        sk.zeroize();
        let inner = match blob_seal::open(&seal_keys, pseudo_id, &sealed) {
            Ok(inner) => Zeroizing::new(inner),
            Err(_) => {
                eprintln!(
                    "citadel-mem: sealed ANN segment for region {} failed authenticated \
                     decryption (corrupt); rebuilding from scan",
                    h.id
                );
                return refuse("authenticated decryption failed");
            }
        };
        check_cancel(cancel)?;
        let parsed = match parse_sealed_segment(&inner, cancel) {
            Ok(parsed) => parsed,
            Err(citadel_vector::segment::SegmentOperationError::Interrupted) => {
                return Err(MemError::Core(citadel_core::Error::Interrupted));
            }
            Err(error @ citadel_vector::segment::SegmentOperationError::Allocation(_)) => {
                return Err(segment_operation_error(error));
            }
            Err(citadel_vector::segment::SegmentOperationError::Segment(source)) => {
                return refuse(&format!("inner segment decode failed: {source}"));
            }
        };
        let Some((stored_fp, stored_cfg, kind_codes, parts)) = parsed else {
            return refuse("inner parse/decode failed");
        };
        let active_cfg = citadel_vector::segment::prism_config_hash(&AnnIndex::active_config(
            ann_metric(h.metric),
        ));
        if stored_cfg != active_cfg {
            return refuse("prism config changed since the segment was built");
        }

        // Rehydrate by decrypting live rows, placed by the id_map permutation;
        // the recall cache comes from the same decrypt pass.
        let slot_of = parts.internal_of_row();
        let dim = h.dim as usize;
        let mut vectors = Zeroizing::new(vec![0.0f32; parts.n() * dim]);
        let mut filled = 0usize;
        let mut cached: FxHashMap<AtomId, CachedAtom> = FxHashMap::default();
        let mut zero_norm_atoms = FxHashSet::default();
        let mut unknown = false;
        let (live_fp, _) = sealed_fp_scan(
            conn,
            &self.db,
            h,
            cancel,
            &mut |id, kind, sealed_row, wrapped, score, confidence, created, immutable, expires| {
                let Some(&slot) = slot_of.get(&(id as u64)) else {
                    unknown = true;
                    return Ok(false);
                };
                let (emb, text, payload) = open_atom(atom_wrap, wrapped, id, sealed_row)?;
                let emb = Zeroizing::new(emb);
                if h.metric == EmbeddingMetric::Cosine && emb.iter().all(|value| *value == 0.0) {
                    zero_norm_atoms.insert(id);
                }
                vectors[slot as usize * dim..(slot as usize + 1) * dim].copy_from_slice(&emb);
                filled += 1;
                let kind = kind.to_string();
                let owned_content_bytes = atom_content_bytes(&kind, &text, &payload, usize::MAX);
                cached.insert(
                    id,
                    CachedAtom {
                        kind,
                        text,
                        payload,
                        owned_content_bytes,
                        importance: score,
                        confidence,
                        created_micros: created,
                        immutable,
                        expires_micros: expires,
                    },
                );
                Ok(true)
            },
        )?;
        if unknown || live_fp != stored_fp || filled != parts.n() {
            // Stale (liveness or content moved): expected after forgets that
            // bypassed explicit retirement.
            return refuse(&format!(
                "stale: unknown={unknown} fp_match={} filled={filled}/{}",
                live_fp == stored_fp,
                parts.n()
            ));
        }
        let segment_b3 = citadel_vector::segment::digest_with_cancel(&sealed, cancel)
            .map_err(segment_operation_error)?;
        // Pre-validate while the buffer is still zeroizing-owned; PRISM scrubs on drop.
        if filled != parts.n() || vectors.len() != parts.n() * dim {
            return refuse("sealed ANN vector rehydration shape changed");
        }
        let index = match parts.into_index(std::mem::take(vectors.as_mut()), filled) {
            Ok(i) => i,
            Err(e) => return refuse(&format!("into_index: {e}")),
        };
        check_cancel(cancel)?;
        if atom_table_root_stamp(&self.db, &h.table)? != table_stamp {
            return refuse("atom table changed while the sealed segment was loaded");
        }
        Ok(Ok(SealedAnn {
            index,
            kind_codes,
            cached,
            zero_norm_atoms,
            source: AnnIndexSource::Loaded { segment_b3 },
            // Entry epoch, never re-read: pre-bump data must not be stamped current.
            build_epoch: epoch,
            table_stamp,
        }))
    }

    /// Destroy the sealed segment's key slot (crypto-erasing all on-disk
    /// residue), delete its own chunk keys (other regions may share the tree),
    /// and clear the meta rows. Safe when nothing is persisted.
    fn retire_sealed_segment(
        &self,
        h: &RegionHandle,
        conn: &Connection<'_>,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<()> {
        self.retire_sealed_segment_parts(conn, h.id, &h.table, kl)
    }

    /// Classify the chunk tree; only well-formed is Present, storage errors propagate.
    fn sealed_segment_tree_state(&self, seg_table: &str) -> Result<SegmentTreeState> {
        #[cfg(test)]
        if FAIL_SEGMENT_PROBE.with(std::cell::Cell::take) {
            return Err(MemError::Invalid("injected segment probe failure".into()));
        }
        let mut rtx = self.db.begin_read();
        let count_bytes = match rtx.table_get(seg_table.as_bytes(), &0u32.to_be_bytes()) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(SegmentTreeState::Incomplete),
            Err(citadel_core::Error::TableNotFound(_)) => return Ok(SegmentTreeState::Absent),
            Err(e) => return Err(e.into()),
        };
        let Ok(count) = <[u8; 4]>::try_from(count_bytes.as_slice()).map(u32::from_le_bytes) else {
            return Ok(SegmentTreeState::Incomplete);
        };
        if !plausible_chunk_count(count) {
            return Ok(SegmentTreeState::Incomplete);
        }
        for i in 1..=count {
            // Presence only - never materialize the ciphertext chunks.
            match rtx.table_contains_key(seg_table.as_bytes(), &i.to_be_bytes()) {
                Ok(true) => {}
                // The span is held: a hole is an interrupted persist, not a vanished tree.
                Ok(false) | Err(citadel_core::Error::TableNotFound(_)) => {
                    return Ok(SegmentTreeState::Incomplete)
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(SegmentTreeState::Present)
    }

    /// Drop a segment chunk tree by physical name; already-absent is fine.
    fn drop_segment_tree(&self, seg_table: &str) -> Result<()> {
        drop_segment_tree(&self.db, seg_table)
    }

    /// Remove segment residue post-tombstone; tree before meta so a retry re-enters.
    fn cleanup_failed_sealed_segment(
        &self,
        conn: &Connection<'_>,
        region_id: RegionId,
        seg_table: &str,
    ) -> Result<()> {
        self.drop_segment_tree(seg_table)?;
        clear_annseg_meta(conn, region_id)
    }

    /// [`retire_sealed_segment`] by raw parts, for callers without a live
    /// handle. Key-first (fail-secure); a crash is finished by
    /// `reconcile_atom_store` at next open. Tolerates any resumed-retire slot
    /// state (tombstoned or recycled both mean the key is dead).
    fn retire_sealed_segment_parts(
        &self,
        conn: &Connection<'_>,
        region_id: RegionId,
        table: &str,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<()> {
        retire_sealed_segment_parts(&self.db, conn, region_id, table, kl)
    }

    pub fn fetch_one(&self, region: &str, atom_id: AtomId) -> Result<Option<AtomHit>> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            let mut hit = self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                self.fetch_one_sealed(&h, atom_id, conn, atom_wrap)
            })?;
            check_cancel(cancel.as_ref())?;
            charge_returned_hit(&mut hit)?;
            return Ok(hit);
        }
        let mut hit = self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(
                &format!(
                    "SELECT id, kind, CAST(payload AS TEXT), text_content, score, confidence, \
                     immutable, created_at, expires_at \
                     FROM {table} WHERE id = $1 AND region_id = $2 \
                     AND (expires_at IS NULL OR expires_at > $3)",
                    table = h.table
                ),
                &[
                    Value::Integer(atom_id),
                    Value::Integer(h.id),
                    Value::Timestamp(now_micros()),
                ],
            )?;
            qr.rows.first().map(|row| parse_fetched(row)).transpose()
        })?;
        check_cancel(cancel.as_ref())?;
        charge_returned_hit(&mut hit)?;
        Ok(hit)
    }

    /// Read exact live atoms in request order without embedding.
    ///
    /// Missing, expired, or key-erased ids produce `None`. Duplicate ids are
    /// rejected so one stored value cannot be multiplied into unbounded output.
    pub fn fetch_by_ids(&self, region: &str, ids: &[AtomId]) -> Result<Vec<Option<AtomHit>>> {
        validate_fetch_ids(ids)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let mut found = if h.atom_wrap.is_some() {
            self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                fetch_atoms_by_ids_sealed(
                    &self.db,
                    conn,
                    h.id,
                    &h.table,
                    atom_wrap,
                    ids,
                    cancel.as_ref(),
                )
            })?
        } else {
            self.with_live_plain_access(&key, &h, |conn| {
                fetch_atoms_by_ids_plain(conn, h.id, &h.table, ids, cancel.as_ref())
            })?
        };
        check_cancel(cancel.as_ref())?;

        let mut ordered = order_fetched_ids(ids, &mut found);
        charge_returned_optional_hits(&mut ordered)?;
        Ok(ordered)
    }

    /// Most recent atom of `kind` in `region` (highest id), or `None`.
    pub fn fetch_last(&self, region: &str, kind: &str) -> Result<Option<AtomHit>> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if h.atom_wrap.is_some() {
            let mut hit = self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                self.fetch_last_sealed(&h, kind, conn, atom_wrap, cancel.as_ref())
            })?;
            check_cancel(cancel.as_ref())?;
            charge_returned_hit(&mut hit)?;
            return Ok(hit);
        }
        let mut hit = self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(
                &format!(
                    "SELECT id, kind, CAST(payload AS TEXT), text_content, score, confidence, \
                     immutable, created_at, expires_at \
                     FROM {table} WHERE region_id = $1 AND kind = $2 \
                     AND (expires_at IS NULL OR expires_at > $3) ORDER BY id DESC LIMIT 1",
                    table = h.table
                ),
                &[
                    Value::Integer(h.id),
                    Value::Text(kind.into()),
                    Value::Timestamp(now_micros()),
                ],
            )?;
            qr.rows.first().map(|row| parse_fetched(row)).transpose()
        })?;
        check_cancel(cancel.as_ref())?;
        charge_returned_hit(&mut hit)?;
        Ok(hit)
    }

    /// Read raw global edge storage without checking endpoint ownership.
    ///
    /// This exists only for unit tests that construct legacy or corrupt graph
    /// states. Production callers must use the region-scoped readers below.
    #[cfg(test)]
    pub(crate) fn fetch_edges(
        &self,
        src: Option<AtomId>,
        dst: Option<AtomId>,
        kind: Option<EdgeKind>,
    ) -> Result<Vec<Edge>> {
        let cancel = check_db_cancel(&self.db)?;
        let mut params: Vec<Value> = Vec::new();
        let mut clauses: Vec<String> = Vec::new();
        if let Some(s) = src {
            params.push(Value::Integer(s));
            clauses.push(format!("src_id = ${}", params.len()));
        }
        if let Some(d) = dst {
            params.push(Value::Integer(d));
            clauses.push(format!("dst_id = ${}", params.len()));
        }
        if let Some(k) = kind {
            params.push(Value::Text(k.as_str().into()));
            clauses.push(format!("kind = ${}", params.len()));
        }
        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };

        let conn = Connection::open(&self.db)?;
        let qr = conn.query_params(
            &format!(
                "SELECT src_id, dst_id, kind, weight, CAST(evidence_ref AS TEXT) \
                 FROM memory_edges{where_clause} ORDER BY src_id, dst_id, kind"
            ),
            &params,
        )?;
        let edges = qr
            .rows
            .iter()
            .map(|row| {
                #[cfg(test)]
                debug_fire_cancel_after_local_work();
                check_cancel(cancel.as_ref())?;
                parse_edge(row)
            })
            .collect::<Result<Vec<_>>>()?;
        check_cancel(cancel.as_ref())?;
        Ok(edges)
    }

    /// Read only edges whose two endpoints are live atoms in `region`.
    ///
    /// Global edges to or from another region are excluded even when the caller
    /// filters by a local endpoint. Results use stable edge order and stop at
    /// `limit`; zero returns no edges.
    pub fn fetch_edges_in_region(
        &self,
        region: &str,
        src: Option<AtomId>,
        dst: Option<AtomId>,
        kind: Option<EdgeKind>,
        limit: usize,
    ) -> Result<Vec<Edge>> {
        let mut edges = self.fetch_region_edges(
            region,
            RegionEdgeQuery {
                src: src.map_or(EdgeEndpointFilter::Any, EdgeEndpointFilter::One),
                dst: dst.map_or(EdgeEndpointFilter::Any, EdgeEndpointFilter::One),
                kind,
                after: None,
                limit,
            },
        )?;
        charge_returned_edges(&mut edges)?;
        Ok(edges)
    }

    /// Read one stable page of live, region-local edges.
    pub fn fetch_edges_page_in_region(
        &self,
        region: &str,
        src: Option<AtomId>,
        dst: Option<AtomId>,
        kind: Option<EdgeKind>,
        after: Option<EdgeCursor>,
        limit: usize,
    ) -> Result<EdgePage> {
        if limit == 0 {
            return Ok(EdgePage {
                edges: Vec::new(),
                next_after: None,
            });
        }
        let fetch_limit = limit
            .checked_add(1)
            .ok_or_else(|| MemError::Invalid("edge fetch limit out of range".into()))?;
        let mut edges = self.fetch_region_edges(
            region,
            RegionEdgeQuery {
                src: src.map_or(EdgeEndpointFilter::Any, EdgeEndpointFilter::One),
                dst: dst.map_or(EdgeEndpointFilter::Any, EdgeEndpointFilter::One),
                kind,
                after,
                limit: fetch_limit,
            },
        )?;
        let has_more = edges.len() > limit;
        edges.truncate(limit);
        let next_after = has_more.then(|| {
            let last = edges.last().expect("a nonempty edge page has a cursor");
            EdgeCursor {
                src_id: last.src_id,
                dst_id: last.dst_id,
                kind: last.kind,
            }
        });
        charge_returned_edges(&mut edges)?;
        Ok(EdgePage { edges, next_after })
    }

    /// Read live region-local edges whose source belongs to `sources`.
    ///
    /// This batches provenance and graph views without one query per atom.
    pub fn fetch_edges_from_atoms_in_region(
        &self,
        region: &str,
        sources: &[AtomId],
        kind: Option<EdgeKind>,
        limit: usize,
    ) -> Result<Vec<Edge>> {
        let mut edges = self.fetch_region_edges(
            region,
            RegionEdgeQuery {
                src: EdgeEndpointFilter::AnyOf(sources),
                dst: EdgeEndpointFilter::Any,
                kind,
                after: None,
                limit,
            },
        )?;
        charge_returned_edges(&mut edges)?;
        Ok(edges)
    }

    /// Read only source/destination pairs for live region-local edges.
    ///
    /// This projection avoids materializing weight and evidence when a caller
    /// only needs provenance topology.
    pub fn fetch_edge_endpoints_from_atoms_in_region(
        &self,
        region: &str,
        sources: &[AtomId],
        kind: Option<EdgeKind>,
        limit: usize,
    ) -> Result<Vec<(AtomId, AtomId)>> {
        self.fetch_region_edge_endpoints(
            region,
            RegionEdgeQuery {
                src: EdgeEndpointFilter::AnyOf(sources),
                dst: EdgeEndpointFilter::Any,
                kind,
                after: None,
                limit,
            },
        )
    }

    /// Drain all live region-local edges for an internal graph algorithm.
    /// External surfaces use the bounded/pageable public readers instead.
    pub(crate) fn fetch_all_edges_in_region(
        &self,
        region: &str,
        src: Option<AtomId>,
        dst: Option<AtomId>,
        kind: Option<EdgeKind>,
    ) -> Result<Vec<Edge>> {
        const PAGE_SIZE: usize = 1_024;

        let mut edges = Vec::new();
        let mut after = None;
        loop {
            let page = self.fetch_region_edges(
                region,
                RegionEdgeQuery {
                    src: src.map_or(EdgeEndpointFilter::Any, EdgeEndpointFilter::One),
                    dst: dst.map_or(EdgeEndpointFilter::Any, EdgeEndpointFilter::One),
                    kind,
                    after,
                    limit: PAGE_SIZE,
                },
            )?;
            if page.is_empty() {
                return Ok(edges);
            }
            after = page.last().map(|edge| EdgeCursor {
                src_id: edge.src_id,
                dst_id: edge.dst_id,
                kind: edge.kind,
            });
            let drained = page.len() < PAGE_SIZE;
            edges.extend(page);
            if drained {
                return Ok(edges);
            }
        }
    }

    /// Read live region-local edges whose source and destination both belong
    /// to `atoms`, in stable edge order.
    pub fn fetch_edges_between_atoms_in_region(
        &self,
        region: &str,
        atoms: &[AtomId],
        kind: Option<EdgeKind>,
        limit: usize,
    ) -> Result<Vec<Edge>> {
        let mut edges = self.fetch_region_edges(
            region,
            RegionEdgeQuery {
                src: EdgeEndpointFilter::AnyOf(atoms),
                dst: EdgeEndpointFilter::AnyOf(atoms),
                kind,
                after: None,
                limit,
            },
        )?;
        charge_returned_edges(&mut edges)?;
        Ok(edges)
    }

    fn fetch_region_edges(&self, region: &str, query: RegionEdgeQuery<'_>) -> Result<Vec<Edge>> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let edges = if h.atom_wrap.is_some() {
            self.with_live_sealed_read(&key, &h, |conn, _atom_wrap, _kl| {
                query_region_edges(&self.db, conn, &h, query, cancel.as_ref())
            })?
        } else {
            self.with_live_plain_access(&key, &h, |conn| {
                query_region_edges(&self.db, conn, &h, query, cancel.as_ref())
            })?
        };
        check_cancel(cancel.as_ref())?;
        Ok(edges)
    }

    fn fetch_region_edge_endpoints(
        &self,
        region: &str,
        query: RegionEdgeQuery<'_>,
    ) -> Result<Vec<(AtomId, AtomId)>> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let endpoints = if h.atom_wrap.is_some() {
            self.with_live_sealed_read(&key, &h, |conn, _atom_wrap, _kl| {
                query_region_edge_endpoints(&self.db, conn, &h, query, cancel.as_ref())
            })?
        } else {
            self.with_live_plain_access(&key, &h, |conn| {
                query_region_edge_endpoints(&self.db, conn, &h, query, cancel.as_ref())
            })?
        };
        check_cancel(cancel.as_ref())?;
        Ok(endpoints)
    }

    /// Replace a live mutable atom's JSONB payload; same-value updates are no-ops.
    pub fn update_atom_payload(
        &self,
        region: &str,
        atom_id: AtomId,
        payload: &serde_json::Value,
    ) -> Result<PayloadUpdateOutcome> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let now = now_micros();
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, atom_wrap, kl| {
                #[cfg(test)]
                debug_fire_cancel_after_local_work();
                check_cancel(cancel.as_ref())?;
                let context = SealedPayloadUpdateContext {
                    region_key: &key,
                    handle: &h,
                    now,
                    conn,
                    atom_wrap,
                };
                let Some(prepared) =
                    self.prepare_atom_payload_update_sealed(&context, atom_id, payload)?
                else {
                    return Ok(PayloadUpdateOutcome { changed: false });
                };
                check_cancel(cancel.as_ref())?;
                // Retire before the rewrite so the old payload survives nowhere.
                self.db.bump_cache_epoch();
                self.retire_sealed_segment(&h, conn, kl)?;
                self.update_atom_payload_sealed(&key, &h, atom_id, prepared, now, conn)?;
                *h.ann.write().unwrap() = None;
                Ok(PayloadUpdateOutcome { changed: true })
            });
        }
        let js = serde_json::to_string(payload)
            .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;

        self.with_live_plain_access(&key, &h, |conn| {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            with_write_txn(conn, |c| {
                let qr = c.query_params(
                    &format!(
                        "SELECT CAST(payload AS TEXT) FROM {table} \
                         WHERE id = $1 AND region_id = $2 AND immutable = 0 \
                         AND (expires_at IS NULL OR expires_at > $3)",
                        table = h.table
                    ),
                    &[
                        Value::Integer(atom_id),
                        Value::Integer(h.id),
                        Value::Timestamp(now),
                    ],
                )?;
                let Some(row) = qr.rows.first() else {
                    return Err(MemError::AtomNotMutable {
                        atom_id,
                        region: key.clone(),
                    });
                };
                let stored: serde_json::Value =
                    serde_json::from_str(as_text(&row[0])?).map_err(|error| {
                        MemError::Invalid(format!(
                            "atom {atom_id} payload is invalid JSON: {error}"
                        ))
                    })?;
                if &stored == payload {
                    return Ok(PayloadUpdateOutcome { changed: false });
                }
                let res = c.execute_params(
                    &format!(
                        "UPDATE {table} SET payload = CAST($1 AS JSONB) \
                         WHERE id = $2 AND region_id = $3 AND immutable = 0 \
                         AND (expires_at IS NULL OR expires_at > $4)",
                        table = h.table
                    ),
                    &[
                        Value::Text(js.into()),
                        Value::Integer(atom_id),
                        Value::Integer(h.id),
                        Value::Timestamp(now),
                    ],
                )?;
                match res {
                    ExecutionResult::RowsAffected(0) => Err(MemError::AtomNotMutable {
                        atom_id,
                        region: key.clone(),
                    }),
                    _ => Ok(PayloadUpdateOutcome { changed: true }),
                }
            })
        })
    }

    /// Set fusion importance; same-value skips make a converged pass write nothing.
    pub fn set_importance(&self, region: &str, updates: &[(AtomId, f32)]) -> Result<usize> {
        let cancel = check_db_cancel(&self.db)?;
        if updates.is_empty() {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            return Ok(0);
        }
        if let Some((atom_id, _)) = updates
            .iter()
            .find(|(_, importance)| !importance.is_finite())
        {
            return Err(MemError::Invalid(format!(
                "importance for atom {atom_id} must be finite"
            )));
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let apply = |conn: &Connection<'_>| {
            with_write_txn(conn, |c| {
                let mut n = 0usize;
                for &(id, importance) in updates {
                    let res = c.execute_params(
                        &format!(
                            "UPDATE {table} SET score = $1 \
                             WHERE id = $2 AND region_id = $3 AND immutable = 0 \
                             AND score <> $1",
                            table = h.table
                        ),
                        &[
                            Value::Real(importance as f64),
                            Value::Integer(id),
                            Value::Integer(h.id),
                        ],
                    )?;
                    if !matches!(res, ExecutionResult::RowsAffected(0)) {
                        n += 1;
                    }
                }
                Ok(n)
            })
        };
        if h.atom_wrap.is_some() {
            return self.with_live_sealed_read(&key, &h, |conn, _, _kl| {
                #[cfg(test)]
                debug_fire_cancel_after_local_work();
                check_cancel(cancel.as_ref())?;
                let updated = apply(conn)?;
                if updated > 0 {
                    // The index bakes importance in; the bump drops all handles' caches.
                    self.db.bump_cache_epoch();
                    *h.ann.write().unwrap() = None;
                    self.retire_sealed_segment(&h, conn, _kl)?;
                }
                Ok(updated)
            });
        }
        self.with_live_plain_access(&key, &h, |conn| {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            apply(conn)
        })
    }

    /// Hybrid recall: ANN retrieval then fusion re-ranking; top `q.k` atoms.
    ///
    /// Encrypted regions decrypt once into an in-RAM PRISM index over the whole
    /// region (cached; post-snapshot tail exact-ranked); keyword is in-Rust
    /// BM25, not SQL `ts_rank`.
    pub fn recall(&self, region: &str, q: RecallQuery) -> Result<Vec<AtomHit>> {
        let cancel = check_db_cancel(&self.db)?;
        let mut hits = self.recall_impl(region, q, RecallMode::USER, None, cancel.as_ref())?;
        charge_returned_hits(&mut hits)?;
        Ok(hits)
    }

    /// Validate an MMR request against the currently attached region without
    /// embedding a query or reading atoms. Recall revalidates after this call.
    pub fn preflight_mmr(
        &self,
        region: &str,
        k: usize,
        fetch_k: usize,
        lambda_mult: f32,
    ) -> Result<()> {
        let cancel = check_db_cancel(&self.db)?;
        validate_mmr_request(fetch_k, lambda_mult)?;
        if fetch_k == 0 || k == 0 {
            return Ok(());
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        validate_mmr_region_work(fetch_k, k, usize::from(h.dim), h.metric)?;
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;
        check_cancel(cancel.as_ref())
    }

    /// Recall a bounded candidate pool, then diversify it with maximal marginal relevance.
    ///
    /// `q.k` is the candidate-pool size and `k` is the final result size. The
    /// operation uses exact stored vectors from a cosine region. Graph expansion
    /// is deliberately unsupported because it would append undiversified hits.
    pub fn recall_mmr(
        &self,
        region: &str,
        q: RecallQuery,
        k: usize,
        lambda_mult: f32,
    ) -> Result<Vec<AtomHit>> {
        let cancel = check_db_cancel(&self.db)?;
        validate_mmr_request(q.k, lambda_mult)?;
        if q.graph_expand.is_some() {
            return Err(MemError::Invalid(
                "MMR recall does not support graph expansion".into(),
            ));
        }
        if q.k == 0 || k == 0 {
            return Ok(Vec::new());
        }
        let mut hits = self.recall_impl(
            region,
            q,
            RecallMode::USER,
            Some(MmrSelection { k, lambda_mult }),
            cancel.as_ref(),
        )?;
        charge_returned_hits(&mut hits)?;
        Ok(hits)
    }

    fn select_mmr_hits(
        &self,
        key: &str,
        h: &RegionHandle,
        hits: Vec<AtomHit>,
        query_vector: &[f32],
        selection: MmrSelection,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<AtomHit>> {
        let mut hits = ScrubbedHitSlots::from_hits(hits);
        if hits.0.is_empty() || selection.k == 0 {
            return Ok(Vec::new());
        }
        let ids = hits
            .0
            .iter()
            .filter_map(Option::as_ref)
            .map(|hit| hit.id)
            .collect::<Vec<_>>();
        let _atom_reservation = self.reserve_plaintext_atoms(key, h, ids.iter().copied())?;
        check_cancel(cancel)?;
        let mut embeddings = if h.atom_wrap.is_some() {
            self.with_live_sealed_read(key, h, |conn, atom_wrap, _kl| {
                fetch_mmr_embeddings_sealed(
                    &self.db, conn, key, h.id, &h.table, h.dim, atom_wrap, &ids, cancel,
                )
            })?
        } else {
            self.with_live_plain_access(key, h, |conn| {
                fetch_mmr_embeddings_plain(conn, key, h.id, &h.table, h.dim, &ids, cancel)
            })?
        };
        check_cancel(cancel)?;

        let mut live_hits = ScrubbedHitSlots::with_capacity(hits.0.len());
        let mut live_embeddings = Vec::with_capacity(hits.0.len());
        for slot in &mut hits.0 {
            let hit = slot.take().expect("MMR input hit is present");
            match embeddings.remove(&hit.id) {
                Some(embedding) => {
                    live_hits.0.push(Some(hit));
                    live_embeddings.push(embedding);
                }
                None => {
                    let mut discarded = hit;
                    zeroize_atom_content(&mut discarded.text, &mut discarded.payload);
                }
            }
        }
        let vectors = live_embeddings
            .iter()
            .map(|embedding| embedding.as_slice())
            .collect::<Vec<_>>();
        let selected = maximal_marginal_relevance(
            query_vector,
            &vectors,
            selection.k,
            selection.lambda_mult,
            cancel,
        )?;
        let mut result = ScrubbedHitSlots::with_capacity(selected.len());
        for index in selected {
            result.0.push(Some(
                live_hits.0[index]
                    .take()
                    .expect("MMR returns each candidate at most once"),
            ));
        }
        check_cancel(cancel)?;
        Ok(result.into_hits())
    }

    fn finish_recall_hits(
        &self,
        region_id: RegionId,
        mut hits: Vec<AtomHit>,
        mode: RecallMode,
        scrub_on_error: bool,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<AtomHit>> {
        let finish = (|| {
            check_cancel(cancel)?;
            if mode.record_access {
                self.note_access(region_id, hits.iter().map(|hit| hit.id), cancel)?;
            }
            Ok(())
        })();
        if let Err(error) = finish {
            if scrub_on_error {
                for hit in &mut hits {
                    zeroize_atom_content(&mut hit.text, &mut hit.payload);
                }
            }
            return Err(error);
        }
        Ok(hits)
    }

    /// Recall a ranked/expanded atom view and return the live region-local edge
    /// subgraph induced by those atoms. Edge order is stable; `edge_limit`
    /// bounds returned edges and `edges_truncated` reports an omitted suffix.
    pub fn profile(
        &self,
        region: &str,
        query: RecallQuery,
        edge_limit: usize,
    ) -> Result<MemoryProfileReport> {
        let atoms = self.recall(region, query)?;
        let ids = atoms.iter().map(|hit| hit.id).collect::<Vec<_>>();
        let fetch_limit = edge_limit
            .checked_add(1)
            .ok_or_else(|| MemError::Invalid("profile edge limit out of range".into()))?;
        let mut edges = self.fetch_region_edges(
            region,
            RegionEdgeQuery {
                src: EdgeEndpointFilter::AnyOf(&ids),
                dst: EdgeEndpointFilter::AnyOf(&ids),
                kind: None,
                after: None,
                limit: fetch_limit,
            },
        )?;
        let edges_truncated = edges.len() > edge_limit;
        edges.truncate(edge_limit);
        charge_returned_edges(&mut edges)?;
        Ok(MemoryProfileReport {
            atoms,
            edges,
            edges_truncated,
        })
    }

    /// Multi-query recall: one batch embed, RRF merge with dedup, one rerank pass.
    pub fn recall_many(&self, region: &str, q: MultiRecallQuery) -> Result<Vec<AtomHit>> {
        let cancel = check_db_cancel(&self.db)?;
        if q.k == 0 || q.queries.is_empty() {
            return Ok(Vec::new());
        }
        validate_rrf_k(q.rrf_k, "multi-query RRF constant")?;
        for query in &q.queries {
            validate_fusion_weights(query.weights)?;
        }
        // Validate first: a bad RRF strategy must not partially execute sub-queries.
        let reranker = if q.rerank_query.is_some() {
            let snapshot = self.reranker.read().unwrap().clone();
            if let Some((_, strategy)) = &snapshot {
                validate_rerank_strategy(*strategy)?;
            }
            snapshot
        } else {
            None
        };
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;

        let mut queries = q.queries;
        let need: Vec<usize> = queries
            .iter()
            .enumerate()
            .filter(|(_, sq)| sq.embedding.is_none())
            .map(|(i, _)| i)
            .collect();
        if !need.is_empty() {
            let texts: Vec<&str> = need
                .iter()
                .map(|&i| {
                    queries[i].text.as_deref().ok_or_else(|| {
                        MemError::Invalid("recall requires either text or embedding".into())
                    })
                })
                .collect::<Result<_>>()?;
            check_cancel(cancel.as_ref())?;
            let embs = h
                .embedder
                .embed_queries_with_cancel(&texts, cancel.as_ref());
            check_cancel(cancel.as_ref())?;
            let embs = embs?;
            if embs.len() != need.len() {
                return Err(MemError::Invalid(format!(
                    "embedder returned {} vectors for {} queries",
                    embs.len(),
                    need.len()
                )));
            }
            for (&i, e) in need.iter().zip(embs) {
                queries[i].embedding = Some(e);
            }
        }
        // A bad later vector must not leave earlier sub-queries partially observed.
        for query in &queries {
            if let Some(vector) = &query.embedding {
                validate_embedding(&key, h.dim, vector, "query")?;
            }
        }
        let mut lists = Vec::with_capacity(queries.len());
        for sq in queries {
            lists.push(self.recall_impl(
                region,
                sq,
                RecallMode::SUBQUERY,
                None,
                cancel.as_ref(),
            )?);
            check_cancel(cancel.as_ref())?;
        }
        let mut merged = rrf_merge(lists, q.rrf_k);
        check_cancel(cancel.as_ref())?;

        let mut hits = match (reranker.as_ref(), &q.rerank_query) {
            (Some((r, strategy)), Some(text)) => {
                check_cancel(cancel.as_ref())?;
                let _callback = self.reserve_plaintext_atoms(
                    &key,
                    &h,
                    merged.iter().take(RERANK_POOL).map(|hit| hit.id),
                )?;
                check_cancel(cancel.as_ref())?;
                let reranked = rerank_hits(
                    r.as_ref(),
                    merged,
                    RerankContext {
                        query: text,
                        strategy: *strategy,
                        k: q.k,
                        cancel: cancel.as_ref(),
                    },
                );
                check_cancel(cancel.as_ref())?;
                reranked?
            }
            _ => {
                merged.truncate(q.k);
                check_cancel(cancel.as_ref())?;
                merged
            }
        };
        charge_returned_hits(&mut hits)?;
        Ok(hits)
    }

    fn recall_impl(
        &self,
        region: &str,
        q: RecallQuery,
        mode: RecallMode,
        mmr: Option<MmrSelection>,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<AtomHit>> {
        let cancel = cancel.cloned();
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        if q.k == 0 {
            return Ok(Vec::new());
        }
        if let Some(selection) = mmr {
            validate_mmr_region_work(q.k, selection.k, usize::from(h.dim), h.metric)?;
        }
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;
        validate_fusion_weights(q.weights)?;
        // Validate before storage access so bad RRF config leaves no access accounting.
        let reranker = if mode.rerank {
            let snapshot = self.reranker.read().unwrap().clone();
            if let Some((_, strategy)) = &snapshot {
                validate_rerank_strategy(*strategy)?;
            }
            snapshot
        } else {
            None
        };

        let mut qvec: Vec<f32> = match &q.embedding {
            Some(v) => v.clone(),
            None => {
                let text = q.text.as_deref().ok_or_else(|| {
                    MemError::Invalid("recall requires either text or embedding".into())
                })?;
                check_cancel(cancel.as_ref())?;
                let qvec = embed_query_one(&*h.embedder, text, cancel.as_ref());
                check_cancel(cancel.as_ref())?;
                qvec?
            }
        };
        validate_embedding(&key, h.dim, &qvec, "query")?;

        if h.atom_wrap.is_some() {
            let cands = self.with_live_sealed_read(&key, &h, |conn, _atom_wrap, _kl| {
                self.recall_sealed_candidates(
                    &h,
                    ResolvedRecall {
                        query: &q,
                        vector: &qvec,
                    },
                    conn,
                    _kl,
                    recall_candidate_limit(q.k),
                    cancel.as_ref(),
                )
            })?;
            check_cancel(cancel.as_ref())?;
            let as_of = q.as_of_micros.unwrap_or_else(now_micros);
            // Rerankers may re-enter the engine; the lifecycle lock is non-reentrant.
            let mut hits = match (reranker.as_ref(), &q.text) {
                (Some((r, strategy)), Some(text)) => {
                    check_cancel(cancel.as_ref())?;
                    let _callback = self.reserve_plaintext_atoms(
                        &key,
                        &h,
                        cands.iter().map(|candidate| candidate.id),
                    )?;
                    check_cancel(cancel.as_ref())?;
                    let reranked = fuse_rerank(
                        r.as_ref(),
                        cands,
                        q.weights,
                        as_of,
                        RerankContext {
                            query: text,
                            strategy: *strategy,
                            k: q.k,
                            cancel: cancel.as_ref(),
                        },
                    );
                    check_cancel(cancel.as_ref())?;
                    reranked?
                }
                _ => fuse_rank(cands, q.weights, as_of, q.k),
            };
            check_cancel(cancel.as_ref())?;
            if let Some(selection) = mmr {
                hits = self.select_mmr_hits(&key, &h, hits, &qvec, selection, cancel.as_ref())?;
            }
            if let Some(ge) = &q.graph_expand {
                let seeds: Vec<AtomId> = hits.iter().map(|hit| hit.id).collect();
                let present: FxHashSet<AtomId> = seeds.iter().copied().collect();
                let mut expanded =
                    self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                        let scope = GraphFetchScope {
                            table: &h.table,
                            region_id: h.id,
                            kind_allowlist: &q.kinds,
                            payload_filter: q.payload_filter.as_ref(),
                            sealed_db: Some(&self.db),
                        };
                        expand_graph_sealed(
                            &self.db,
                            conn,
                            atom_wrap,
                            scope,
                            &seeds,
                            ge,
                            cancel.as_ref(),
                        )
                    })?;
                check_cancel(cancel.as_ref())?;
                expanded.retain(|hit| !present.contains(&hit.id));
                hits.extend(expanded);
            }
            return self.finish_recall_hits(h.id, hits, mode, mmr.is_some(), cancel.as_ref());
        }

        let distop = match h.metric {
            EmbeddingMetric::Cosine => "<=>",
            EmbeddingMetric::L2 => "<->",
            EmbeddingMetric::InnerProduct => "<#>",
        };
        let table = h.table.clone();

        // $1 = query vector (reused in SELECT + ORDER BY), $2 = region_id.
        let sql_query_vector = if mmr.is_some() {
            qvec.clone()
        } else {
            std::mem::take(&mut qvec)
        };
        let mut params: Vec<Value> =
            vec![Value::Vector(sql_query_vector.into()), Value::Integer(h.id)];

        // Keyword rank uses the in-Rust BM25 primitive (assign_bm25_ranks)
        // shared with the sealed path; no SQL FTS, no language config.
        let mut where_parts = vec!["region_id = $2".to_string()];
        if !q.kinds.is_empty() {
            let mut ph = Vec::with_capacity(q.kinds.len());
            for kind in &q.kinds {
                params.push(Value::Text(kind.clone().into()));
                ph.push(format!("${}", params.len()));
            }
            where_parts.push(format!("kind IN ({})", ph.join(", ")));
        }
        if let Some(filter) = &q.payload_filter {
            let js = serde_json::to_string(filter)
                .map_err(|e| MemError::Invalid(format!("payload_filter not serializable: {e}")))?;
            params.push(Value::Text(js.into()));
            where_parts.push(format!("payload @> CAST(${} AS JSONB)", params.len()));
        }
        // TTL runs on the wall clock (a lapse is real-world time, unlike as_of
        // grading).
        params.push(Value::Timestamp(now_micros()));
        let ttl_param = params.len();
        where_parts.push(format!(
            "(expires_at IS NULL OR expires_at > ${})",
            ttl_param
        ));
        if !q.include_superseded {
            where_parts.push(format!(
                "id NOT IN (SELECT e.dst_id FROM memory_edges e \
                 JOIN {table} src ON src.id = e.src_id \
                 JOIN {table} dst ON dst.id = e.dst_id \
                 WHERE e.kind = 'supersedes' AND src.region_id = $2 AND dst.region_id = $2 \
                 AND (src.expires_at IS NULL OR src.expires_at > ${ttl_param}) \
                 AND (dst.expires_at IS NULL OR dst.expires_at > ${ttl_param}))"
            ));
        }

        // Over-fetch trades query latency for better ranking of keyword/recency
        // hits.
        let overfetch = recall_candidate_limit(q.k);
        let sql = format!(
            "SELECT id, kind, CAST(payload AS TEXT), text_content, score, confidence, \
             created_at, expires_at, embedding {distop} $1, 0.0, immutable \
             FROM {table} WHERE {} ORDER BY embedding {distop} $1 LIMIT {overfetch}",
            where_parts.join(" AND ")
        );

        let mut cands = self.with_live_plain_access(&key, &h, |conn| {
            let qr = conn.query_params(&sql, &params)?;
            let mut cands = Vec::with_capacity(qr.rows.len());
            for row in &qr.rows {
                #[cfg(test)]
                debug_fire_cancel_after_local_work();
                check_cancel(cancel.as_ref())?;
                cands.push(parse_candidate(row)?);
            }
            Ok(cands)
        })?;
        check_cancel(cancel.as_ref())?;
        let query_terms = query_keyword_terms(q.text.as_deref());
        assign_bm25_ranks(&mut cands, &query_terms, cancel.as_ref())?;
        check_cancel(cancel.as_ref())?;
        let as_of = q.as_of_micros.unwrap_or_else(now_micros);
        let mut hits = match (reranker.as_ref(), &q.text) {
            (Some((r, strategy)), Some(text)) => {
                check_cancel(cancel.as_ref())?;
                let _callback = self.reserve_plaintext_atoms(
                    &key,
                    &h,
                    cands.iter().map(|candidate| candidate.id),
                )?;
                check_cancel(cancel.as_ref())?;
                let reranked = fuse_rerank(
                    r.as_ref(),
                    cands,
                    q.weights,
                    as_of,
                    RerankContext {
                        query: text,
                        strategy: *strategy,
                        k: q.k,
                        cancel: cancel.as_ref(),
                    },
                );
                check_cancel(cancel.as_ref())?;
                reranked?
            }
            _ => fuse_rank(cands, q.weights, as_of, q.k),
        };
        check_cancel(cancel.as_ref())?;

        if let Some(selection) = mmr {
            hits = self.select_mmr_hits(&key, &h, hits, &qvec, selection, cancel.as_ref())?;
        }

        if let Some(ge) = &q.graph_expand {
            let seeds: Vec<AtomId> = hits.iter().map(|h| h.id).collect();
            let present: FxHashSet<AtomId> = seeds.iter().copied().collect();
            let mut expanded = self.with_live_plain_access(&key, &h, |conn| {
                let scope = GraphFetchScope {
                    table: &table,
                    region_id: h.id,
                    kind_allowlist: &q.kinds,
                    payload_filter: q.payload_filter.as_ref(),
                    sealed_db: None,
                };
                expand_graph(conn, scope, &seeds, ge, cancel.as_ref())
            })?;
            check_cancel(cancel.as_ref())?;
            expanded.retain(|e| !present.contains(&e.id));
            hits.extend(expanded);
        }
        self.finish_recall_hits(h.id, hits, mode, mmr.is_some(), cancel.as_ref())
    }

    /// Create a raw global edge for tests that exercise legacy/corrupt states.
    #[cfg(test)]
    pub(crate) fn link(&self, src: AtomId, dst: AtomId, kind: EdgeKind, weight: f32) -> Result<()> {
        self.link_with_evidence(src, dst, kind, weight, None)
    }

    /// Raw evidence-bearing counterpart to [`Self::link`].
    #[cfg(test)]
    pub(crate) fn link_with_evidence(
        &self,
        src: AtomId,
        dst: AtomId,
        kind: EdgeKind,
        weight: f32,
        evidence_ref: Option<serde_json::Value>,
    ) -> Result<()> {
        let cancel = check_db_cancel(&self.db)?;
        validate_edge_weight(weight)?;
        let edges_guard = self.db.memory_edges_lock();
        let conn = Connection::open(&self.db)?;
        with_write_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            link_edge(
                c,
                src,
                dst,
                kind,
                weight,
                evidence_ref.as_ref(),
                &edges_guard,
            )?;
            if kind == EdgeKind::SimilarTo {
                c.execute_params(
                    "DELETE FROM memory_similarity_edges WHERE src_id = $1 AND dst_id = $2",
                    &[Value::Integer(src), Value::Integer(dst)],
                )?;
            }
            check_cancel(cancel.as_ref())?;
            Ok(())
        })
    }

    /// Create or update an edge only when both endpoints are live in `region`.
    ///
    /// Membership validation and the edge upsert share one write transaction
    /// and the memory lifecycle/edge capabilities, so a concurrent forget or
    /// region drop cannot invalidate either endpoint between check and use.
    pub fn link_in_region(
        &self,
        region: &str,
        src: AtomId,
        dst: AtomId,
        kind: EdgeKind,
        weight: f32,
    ) -> Result<()> {
        self.link_with_evidence_in_region(region, src, dst, kind, weight, None)
    }

    /// [`link_in_region`](Self::link_in_region) plus a JSONB evidence payload.
    pub fn link_with_evidence_in_region(
        &self,
        region: &str,
        src: AtomId,
        dst: AtomId,
        kind: EdgeKind,
        weight: f32,
        evidence_ref: Option<serde_json::Value>,
    ) -> Result<()> {
        let cancel = check_db_cancel(&self.db)?;
        validate_edge_weight(weight)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;

        // Lock order is lifecycle -> edges everywhere both are needed. Holding
        // the lifecycle capability keeps the region and atom keys stable while
        // the write transaction proves both rows are live and publishes the edge.
        let lifecycle = self.db.key_lifecycle_lock();
        let edges_guard = self.db.memory_edges_lock();
        let conn = Connection::open(&self.db)?;
        let result = with_write_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;
            self.verify_atoms_live(c, &h, &key, &[src, dst])?;
            link_edge_in_region(
                &self.db,
                c,
                &h,
                EdgeMutation {
                    src,
                    dst,
                    kind,
                    weight,
                    evidence_ref: evidence_ref.as_ref(),
                },
                &edges_guard,
                cancel.as_ref(),
            )?;
            if kind == EdgeKind::SimilarTo {
                c.execute_params(
                    "DELETE FROM memory_similarity_edges WHERE src_id = $1 AND dst_id = $2",
                    &[Value::Integer(src), Value::Integer(dst)],
                )?;
            }
            check_cancel(cancel.as_ref())?;
            Ok(())
        });
        result.inspect_err(|error| self.defer_stale_region(&key, h.id, error, &lifecycle))
    }

    /// Remove one exact edge when both endpoints are live in `region`.
    ///
    /// Membership validation and deletion share the lifecycle/edge capabilities
    /// and one write transaction. Returns `false` when no such edge exists.
    pub fn unlink_in_region(
        &self,
        region: &str,
        src: AtomId,
        dst: AtomId,
        kind: EdgeKind,
    ) -> Result<bool> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let lifecycle = self.db.key_lifecycle_lock();
        let _edges_guard = self.db.memory_edges_lock();
        let conn = Connection::open(&self.db)?;
        let result = with_write_txn(&conn, |c| {
            check_cancel(cancel.as_ref())?;
            self.verify_region_live(c, &h, &key)?;
            self.verify_atoms_live(c, &h, &key, &[src, dst])?;
            let deleted = matches!(
                c.execute_params(
                    "DELETE FROM memory_edges WHERE src_id = $1 AND dst_id = $2 AND kind = $3",
                    &[
                        Value::Integer(src),
                        Value::Integer(dst),
                        Value::Text(kind.as_str().into()),
                    ],
                )?,
                ExecutionResult::RowsAffected(count) if count > 0
            );
            if deleted && kind == EdgeKind::SimilarTo {
                c.execute_params(
                    "DELETE FROM memory_similarity_edges WHERE src_id = $1 AND dst_id = $2",
                    &[Value::Integer(src), Value::Integer(dst)],
                )?;
            }
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            Ok(deleted)
        });
        result.inspect_err(|error| self.defer_stale_region(&key, h.id, error, &lifecycle))
    }

    /// Every id must be live in the region: present, unexpired, and (sealed)
    /// its key slot bound by (state, owner, generation) like every sealed
    /// read. Runs inside the caller's write transaction.
    fn verify_atoms_live(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        region_key: &str,
        ids: &[AtomId],
    ) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let in_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let not_live = |atom_id: AtomId| MemError::AtomNotLive {
            atom_id,
            region: region_key.to_owned(),
        };
        if h.atom_wrap.is_some() {
            let qr = conn.query_params(
                &format!(
                    "SELECT id, key_slot, key_gen FROM {} WHERE region_id = $1 \
                     AND id IN ({in_list}) AND (expires_at IS NULL OR expires_at > $2)",
                    h.table
                ),
                &[Value::Integer(h.id), Value::Timestamp(now_micros())],
            )?;
            let wrapped = exact_live_atom_wrapped_rows(&self.db, &qr.rows, 0, 1, 2)?;
            let live: FxHashSet<AtomId> = qr
                .rows
                .iter()
                .zip(wrapped)
                .filter_map(|(row, wrapped)| wrapped.map(|_| as_int(&row[0])))
                .collect::<Result<_>>()?;
            if let Some(&missing) = ids.iter().find(|id| !live.contains(id)) {
                return Err(not_live(missing));
            }
            return Ok(());
        }
        let qr = conn.query_params(
            &format!(
                "SELECT id FROM {} WHERE region_id = $1 AND id IN ({in_list}) \
                 AND (expires_at IS NULL OR expires_at > $2)",
                h.table
            ),
            &[Value::Integer(h.id), Value::Timestamp(now_micros())],
        )?;
        let present: FxHashSet<AtomId> = qr
            .rows
            .iter()
            .map(|r| as_int(&r[0]))
            .collect::<Result<_>>()?;
        if let Some(&missing) = ids.iter().find(|id| !present.contains(id)) {
            return Err(not_live(missing));
        }
        Ok(())
    }

    /// Replace only engine-managed similarity edges; authored `SimilarTo`
    /// assertions remain untouched even when they share the same source.
    fn replace_managed_similarity_edges_locked(
        &self,
        key: &str,
        h: &RegionHandle,
        src: AtomId,
        edges: &[ManagedSimilarityEdge],
        cancel: Option<&citadel_core::CancelToken>,
        guards: (&KeyLifecycleGuard<'_>, &MemoryEdgesGuard<'_>),
    ) -> Result<WeaveRebuild> {
        let conn = Connection::open(&self.db)?;
        with_write_txn(&conn, |c| {
            self.replace_managed_similarity_edges_in_txn(
                c, key, h, src, edges, cancel, true, false, guards.1,
            )
        })
        .inspect_err(|e| self.defer_stale_region(key, h.id, e, guards.0))
    }

    #[allow(clippy::too_many_arguments)]
    fn replace_managed_similarity_edges_in_txn(
        &self,
        c: &Connection<'_>,
        key: &str,
        h: &RegionHandle,
        src: AtomId,
        edges: &[ManagedSimilarityEdge],
        cancel: Option<&citadel_core::CancelToken>,
        allow_repair: bool,
        replace_all_similarity: bool,
        edges_guard: &MemoryEdgesGuard<'_>,
    ) -> Result<WeaveRebuild> {
        let mut canonical = BTreeMap::new();
        for e in edges {
            check_cancel(cancel)?;
            validate_edge_weight(e.1)?;
            if e.0 == src {
                return Err(MemError::Cycle { src, dst: e.0 });
            }
            if canonical.insert(e.0, e).is_some() {
                return Err(MemError::Invalid(format!(
                    "duplicate destination atom {}",
                    e.0
                )));
            }
        }
        let mut live_ids: Vec<AtomId> = canonical.keys().copied().collect();
        live_ids.push(src);
        let live_ids = dedup_sources(&live_ids);
        check_cancel(cancel)?;
        if allow_repair {
            self.verify_region_live_for_repair(c, h, key)?;
        } else {
            self.verify_region_live(c, h, key)?;
        }
        self.verify_atoms_live(c, h, key, &live_ids)?;
        let old = if replace_all_similarity {
            c.query_params(
                "SELECT dst_id FROM memory_edges WHERE src_id = $1 AND kind = 'similar_to'",
                &[Value::Integer(src)],
            )?
        } else {
            c.query_params(
                "SELECT dst_id FROM memory_similarity_edges WHERE src_id = $1",
                &[Value::Integer(src)],
            )?
        };
        let old: FxHashSet<AtomId> = old
            .rows
            .iter()
            .map(|row| {
                check_cancel(cancel)?;
                as_int(&row[0])
            })
            .collect::<Result<_>>()?;
        let cleared = old
            .iter()
            .filter(|dst| !canonical.contains_key(dst))
            .count() as u64;
        if replace_all_similarity {
            c.execute_params(
                "DELETE FROM memory_edges WHERE src_id = $1 AND kind = 'similar_to'",
                &[Value::Integer(src)],
            )?;
        } else {
            c.execute_params(
                "DELETE FROM memory_edges WHERE src_id = $1 AND kind = 'similar_to' \
                 AND dst_id IN (SELECT dst_id FROM memory_similarity_edges WHERE src_id = $1)",
                &[Value::Integer(src)],
            )?;
        }
        c.execute_params(
            "DELETE FROM memory_similarity_edges WHERE src_id = $1",
            &[Value::Integer(src)],
        )?;
        let mut written = 0u64;
        for e in canonical.values() {
            check_cancel(cancel)?;
            let existing = c.query_params(
                "SELECT 1 FROM memory_edges WHERE src_id = $1 AND dst_id = $2 \
                 AND kind = 'similar_to' LIMIT 1",
                &[Value::Integer(src), Value::Integer(e.0)],
            )?;
            if !existing.rows.is_empty() {
                continue;
            }
            link_edge(
                c,
                src,
                e.0,
                EdgeKind::SimilarTo,
                e.1,
                e.2.as_ref(),
                edges_guard,
            )?;
            c.execute_params(
                "INSERT INTO memory_similarity_edges (src_id, dst_id) VALUES ($1, $2)",
                &[Value::Integer(src), Value::Integer(e.0)],
            )?;
            written += 1;
        }
        check_cancel(cancel)?;
        Ok(WeaveRebuild {
            rewoven: written,
            cleared,
        })
    }

    /// Read one atom's vector and scoring columns, sealed or plaintext.
    ///
    /// Takes the key lifecycle lock through the live-access wrappers, so a
    /// caller already inside such a span must not call this.
    fn read_atom_state(&self, key: &str, h: &RegionHandle, atom_id: AtomId) -> Result<AtomState> {
        if h.atom_wrap.is_some() {
            self.with_live_sealed_read(key, h, |conn, _, _kl| {
                self.read_atom_state_locked(conn, key, h, atom_id)
            })
        } else {
            self.with_live_plain_access(key, h, |conn| {
                self.read_atom_state_locked(conn, key, h, atom_id)
            })
        }
    }

    /// Read state while the caller already owns the lifecycle span and has
    /// verified the region incarnation.
    fn read_atom_state_locked(
        &self,
        conn: &Connection<'_>,
        key: &str,
        h: &RegionHandle,
        atom_id: AtomId,
    ) -> Result<AtomState> {
        let missing = || MemError::AtomNotLive {
            atom_id,
            region: key.to_owned(),
        };
        let now = now_micros();
        if let Some(atom_wrap) = h.atom_wrap.as_deref() {
            let qr = conn.query_params(
                &format!(
                    "SELECT sealed, access_count, created_at, key_slot, key_gen FROM {} \
                     WHERE id = $1 AND region_id = $2 \
                     AND (expires_at IS NULL OR expires_at > $3)",
                    h.table
                ),
                &[
                    Value::Integer(atom_id),
                    Value::Integer(h.id),
                    Value::Timestamp(now),
                ],
            )?;
            let row = qr.rows.first().ok_or_else(missing)?;
            let Some(wrapped) = exact_live_atom_wrapped(&self.db, atom_id, &row[3], &row[4])?
            else {
                return Err(missing());
            };
            return Ok(AtomState {
                embedding: open_atom_embedding(atom_wrap, &wrapped, atom_id, as_blob(&row[0])?)?,
                access_count: as_int(&row[1])?.max(0),
                created: as_ts(&row[2])?,
            });
        }

        let qr = conn.query_params(
            &format!(
                "SELECT embedding, access_count, created_at FROM {} \
                 WHERE id = $1 AND region_id = $2 \
                 AND (expires_at IS NULL OR expires_at > $3)",
                h.table
            ),
            &[
                Value::Integer(atom_id),
                Value::Integer(h.id),
                Value::Timestamp(now),
            ],
        )?;
        let row = qr.rows.first().ok_or_else(missing)?;
        let Value::Vector(v) = &row[0] else {
            return Err(MemError::Invalid(format!(
                "atom embedding not a vector: {:?}",
                row[0]
            )));
        };
        Ok(AtomState {
            embedding: v.to_vec(),
            access_count: as_int(&row[1])?.max(0),
            created: as_ts(&row[2])?,
        })
    }

    /// Recompute `SimilarTo` neighbor edges and stored importance via recall; encrypted
    /// regions use the same full-region sealed ANN index.
    pub fn evolve(
        &self,
        region: &str,
        atom_id: AtomId,
        neighbors: usize,
        max_distance: f32,
    ) -> Result<EvolutionReport> {
        self.evolve_with_kinds_mode(region, atom_id, neighbors, max_distance, Vec::new(), false)
    }

    /// Recompute one atom's similarity web after adopting every existing
    /// outgoing `SimilarTo` edge as derived data.
    ///
    /// This is the explicit migration path for edges created before ownership
    /// tracking was available. It also removes authored `SimilarTo` edges from
    /// this source, so use [`evolve`](Self::evolve) unless the caller knows the
    /// whole outgoing similarity web is engine-generated.
    pub fn evolve_replacing_similarity(
        &self,
        region: &str,
        atom_id: AtomId,
        neighbors: usize,
        max_distance: f32,
    ) -> Result<EvolutionReport> {
        self.evolve_with_kinds_mode(region, atom_id, neighbors, max_distance, Vec::new(), true)
    }

    pub(crate) fn evolve_with_kinds(
        &self,
        region: &str,
        atom_id: AtomId,
        neighbors: usize,
        max_distance: f32,
        kinds: Vec<String>,
    ) -> Result<EvolutionReport> {
        self.evolve_with_kinds_mode(region, atom_id, neighbors, max_distance, kinds, false)
    }

    pub(crate) fn evolve_with_kinds_replacing_similarity(
        &self,
        region: &str,
        atom_id: AtomId,
        neighbors: usize,
        max_distance: f32,
        kinds: Vec<String>,
    ) -> Result<EvolutionReport> {
        self.evolve_with_kinds_mode(region, atom_id, neighbors, max_distance, kinds, true)
    }

    fn evolve_with_kinds_mode(
        &self,
        region: &str,
        atom_id: AtomId,
        neighbors: usize,
        max_distance: f32,
        mut kinds: Vec<String>,
        replace_all_similarity: bool,
    ) -> Result<EvolutionReport> {
        if !max_distance.is_finite() || max_distance < 0.0 {
            return Err(MemError::Invalid(
                "similarity distance ceiling must be finite and non-negative".into(),
            ));
        }
        kinds.sort();
        kinds.dedup();
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let _provenance = self.reserve_region_provenance(&key, &h, cancel.as_ref())?;
        let table = h.table.clone();

        let state = self.read_atom_state(&key, &h, atom_id)?;

        let query = RecallQuery::by_embedding(state.embedding.clone(), neighbors.saturating_add(1))
            .with_kinds(kinds.clone())
            .with_weights(FusionWeights::semantic_only());
        let mut found =
            self.recall_impl(&key, query, RecallMode::INTERNAL, None, cancel.as_ref())?;
        found.retain(|n| {
            n.id != atom_id && n.distance.is_some_and(|distance| distance <= max_distance)
        });
        found.truncate(neighbors);
        let edges = found
            .iter()
            .filter_map(|n| {
                n.distance
                    .map(|distance| (n.id, 1.0 / (1.0 + distance.max(0.0)), None))
            })
            .collect::<Vec<_>>();

        let recency = recency_score(now_micros(), state.created);
        let new_score = recency * (1.0 + (state.access_count as f32).ln_1p());

        // Serialize the RSK liveness check with drop_region's key-first erase span.
        let _kl = h.atom_wrap.is_some().then(|| self.db.key_lifecycle_lock());
        let edges_guard = self.db.memory_edges_lock();
        let conn = Connection::open(&self.db)?;
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        check_cancel(cancel.as_ref())?;
        if h.atom_wrap.is_some() {
            // Epoch + retire precede the commit: pre-evolve importance survives nowhere.
            self.db.bump_cache_epoch();
            self.retire_sealed_segment(
                &h,
                &conn,
                _kl.as_ref().expect("sealed span holds the guard"),
            )?;
        }
        let replacement = with_write_txn(&conn, |c| {
            let replacement = self.replace_managed_similarity_edges_in_txn(
                c,
                &key,
                &h,
                atom_id,
                &edges,
                cancel.as_ref(),
                false,
                replace_all_similarity,
                &edges_guard,
            )?;
            let kinds_json = serde_json::to_string(&kinds).map_err(|e| {
                MemError::Invalid(format!("similarity kind filter is not serializable: {e}"))
            })?;
            c.execute_params(
                "INSERT INTO memory_similarity_policies \
                 (region_id, src_id, neighbors, max_distance, kinds) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (src_id) DO UPDATE SET region_id = $1, neighbors = $3, \
                 max_distance = $4, kinds = $5",
                &[
                    Value::Integer(h.id),
                    Value::Integer(atom_id),
                    Value::Integer(i64::try_from(neighbors).map_err(|_| {
                        MemError::Invalid("similarity neighbor count is out of range".into())
                    })?),
                    Value::Real(max_distance as f64),
                    Value::Text(kinds_json.into()),
                ],
            )?;
            c.execute_params(
                &format!("UPDATE {table} SET score = $1 WHERE id = $2 AND region_id = $3"),
                &[
                    Value::Real(new_score as f64),
                    Value::Integer(atom_id),
                    Value::Integer(h.id),
                ],
            )?;
            check_cancel(cancel.as_ref())?;
            Ok(replacement)
        });
        let replacement = match replacement {
            Ok(replacement) => replacement,
            Err(error) => {
                drop(edges_guard);
                drop(_kl);
                self.evict_stale_region(&key, h.id, &error);
                return Err(error);
            }
        };
        // The cached recall index holds the pre-evolve importance; rebuild it on
        // next recall.
        *h.ann.write().unwrap() = None;

        Ok(EvolutionReport {
            links_added: replacement.rewoven as usize,
            importance: new_score,
        })
    }

    /// Remove atoms matching `policy` and their edges; spares `immutable`
    /// except `PurgeRegion`. Evicted atoms are crypto-erased (key destroyed
    /// before the row); `PredicateMatch` decrypts each atom to test the
    /// payload, other policies use plaintext metadata columns.
    ///
    /// On encrypted regions, an error after key destruction does not mean the
    /// content remains decryptable. Retry or reopen to finish residue cleanup.
    pub fn evict(&self, region: &str, policy: EvictionPolicy) -> Result<EvictionReport> {
        let cancel = check_db_cancel(&self.db)?;
        validate_eviction_policy(&policy)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let table = h.table.clone();

        // Bind the whole eviction to one incarnation; a stale handle must not succeed.
        let _kl = self.db.key_lifecycle_lock();
        let _edges_guard = self.db.memory_edges_lock();
        let conn = Connection::open(&self.db)?;
        if let Err(error) = self.verify_region_live(&conn, &h, &key) {
            self.defer_stale_region(&key, h.id, &error, &_kl);
            return Err(error);
        }
        // Snapshot this region's in-process access stats out of the lock;
        // `Lru`/`Stale` layer them over the persisted insert-time floor.
        let accessed = self
            .access_stats
            .lock()
            .unwrap()
            .get(&h.id)
            .cloned()
            .unwrap_or_default();
        let ids = match (&h.atom_wrap, &policy) {
            // Payload containment can't be pushed to SQL over sealed rows;
            // filter in Rust after decrypt.
            (Some(atom_wrap), EvictionPolicy::PredicateMatch { predicate }) => self
                .evict_predicate_sealed_ids(
                    &h,
                    predicate,
                    &conn,
                    atom_wrap.as_ref(),
                    cancel.as_ref(),
                )?,
            _ => evict_target_ids(
                &conn,
                &table,
                h.id,
                &policy,
                now_micros(),
                &accessed,
                cancel.as_ref(),
            )?,
        };
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        if ids.is_empty() {
            check_cancel(cancel.as_ref())?;
            return Ok(EvictionReport { removed: 0 });
        }

        let in_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let encrypted = h.atom_wrap.is_some();
        let atom_slots = if encrypted {
            let qr = conn.query_params(
                &format!("SELECT id, key_slot, key_gen FROM {table} WHERE id IN ({in_list})"),
                &[],
            )?;
            let slots: Vec<(u32, u64, u64)> = qr
                .rows
                .iter()
                .map(|row| {
                    let binding = atom_key_binding(as_int(&row[0])?, &row[1], &row[2])?;
                    Ok((binding.slot, binding.atom_id, binding.generation))
                })
                .collect::<Result<Vec<_>>>()?;
            slots
        } else {
            Vec::new()
        };
        if encrypted {
            ensure_atom_key_slots_unreserved(&atom_slots, &_kl)?;
        }
        // Last caller poll; segment retirement may still cancel before its key commit.
        check_cancel(cancel.as_ref())?;

        if encrypted {
            self.retire_sealed_segment(&h, &conn, &_kl)?;
            _kl.atom_store_tombstone_batch(&atom_slots)?;
        }

        if encrypted {
            #[cfg(test)]
            debug_fire_cancel_after_key_erasure();
            delete_atoms_for_layout_uncancelled_recovery(
                &conn,
                h.id,
                &h.table,
                &in_list,
                &_edges_guard,
                DeleteAtomsLayout::CURRENT,
            )?;
        } else {
            with_write_txn(&conn, |c| {
                delete_atoms_in_txn(c, &h, &in_list, &_edges_guard).map(|_| ())
            })?;
        }
        *h.ann.write().unwrap() = None;
        Ok(EvictionReport {
            removed: ids.len() as u64,
        })
    }

    /// Destroy the keys of the region-scoped `ids` (overwrite + fsync +
    /// read-back). The (slot, id, gen) binding lets a retry over recycled
    /// crash residue skip it and still converge on the row delete.
    fn erase_atom_key_bindings(
        &self,
        bindings: &[(u32, u64, u64)],
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<Vec<SlotErasure>> {
        Ok(kl
            .atom_store_tombstone_batch(bindings)?
            .into_iter()
            .map(|(slot, atom_id, old_gen, new_gen)| SlotErasure {
                slot,
                atom_id: atom_id as AtomId,
                old_gen,
                new_gen,
            })
            .collect())
    }

    /// Erase the keys of `ids` (encrypted only) then delete their rows and
    /// edges, returning `(rows_deleted, slots_erased)`.
    fn erase_and_delete(
        &self,
        region_key: &str,
        h: &RegionHandle,
        ids: &[AtomId],
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<(u64, Vec<SlotErasure>)> {
        // Segment retirement -> ACK tombstones -> atomic row cleanup is one lifecycle span.
        let _kl = self.db.key_lifecycle_lock();
        let edges_guard = self.db.memory_edges_lock();
        let in_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let conn = Connection::open(&self.db)?;
        self.verify_region_live(&conn, h, region_key)?;
        let encrypted = h.atom_wrap.is_some();
        let atom_slots = if encrypted {
            atom_key_slots(&conn, h, &in_list)?
        } else {
            Vec::new()
        };
        if encrypted {
            ensure_atom_key_slots_unreserved(&atom_slots, &_kl)?;
        }
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        // Last caller poll; segment retirement may still cancel before its key commit.
        check_cancel(cancel)?;

        let slots_erased = if encrypted {
            self.retire_sealed_segment(h, &conn, &_kl)?;
            self.erase_atom_key_bindings(&atom_slots, &_kl)?
        } else {
            Vec::new()
        };

        #[cfg(test)]
        if FAIL_ERASE_BEFORE_ROW_DELETE.with(std::cell::Cell::take) {
            return Err(MemError::Invalid(
                "injected erase failure after key tombstone, before row delete".into(),
            ));
        }

        let rows_deleted = if encrypted {
            #[cfg(test)]
            debug_fire_cancel_after_key_erasure();
            delete_atoms_for_layout_uncancelled_recovery(
                &conn,
                h.id,
                &h.table,
                &in_list,
                &edges_guard,
                DeleteAtomsLayout::CURRENT,
            )?
        } else {
            with_write_txn(&conn, |c| delete_atoms_in_txn(c, h, &in_list, &edges_guard))?
        };
        *h.ann.write().unwrap() = None;
        Ok((rows_deleted, slots_erased))
    }

    /// Delete atoms. Encrypted regions crypto-erase each atom's key
    /// (overwrite/fsync/read-back) before its row, leaving it undecryptable on
    /// a crash. Privileged: ignores `immutable`;
    /// [`forget_atoms`](Self::forget_atoms) is the model-safe variant.
    /// After key destruction, residue cleanup runs to an atomic receipt even if
    /// cancellation arrives; storage failure can still require retry/reconciliation.
    pub fn delete_atoms(&self, region: &str, ids: &[AtomId]) -> Result<EvictionReport> {
        let cancel = check_db_cancel(&self.db)?;
        if ids.is_empty() {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel.as_ref())?;
            return Ok(EvictionReport { removed: 0 });
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        // Honest count: nonexistent/deleted ids do not inflate `removed`.
        let (rows_deleted, _) = self
            .erase_and_delete(&key, &h, ids, cancel.as_ref())
            .inspect_err(|error| self.evict_stale_region(&key, h.id, error))?;
        Ok(EvictionReport {
            removed: rows_deleted,
        })
    }

    /// Forget atoms and return a verifiable [`ErasureReceipt`]. On an encrypted
    /// region each atom's key is cryptographically destroyed; on a plaintext
    /// region this is a logical delete (the receipt's `cryptographic_erasure`
    /// is false). Immutable atoms are skipped (reported in `immutable_skipped`)
    /// unless `force` is set.
    ///
    /// After encrypted key destruction, residue cleanup ignores cancellation and
    /// commits atomically so a completed erasure is returned with its receipt.
    pub fn forget_atoms(
        &self,
        region: &str,
        ids: &[AtomId],
        force: bool,
    ) -> Result<ErasureReceipt> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let encrypted = h.atom_wrap.is_some();

        let mut immutable_skipped = Vec::new();
        let mut targets: Vec<AtomId> = ids.to_vec();
        {
            // Even an empty request must certify the incarnation, not a stale handle.
            let kl = self.db.key_lifecycle_lock();
            let conn = Connection::open(&self.db)?;
            if let Err(error) = self.verify_region_live(&conn, &h, &key) {
                self.defer_stale_region(&key, h.id, &error, &kl);
                return Err(error);
            }
            if !force && !ids.is_empty() {
                let in_list = ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let qr = conn.query_params(
                    &format!(
                        "SELECT id FROM {} WHERE region_id = $1 AND id IN ({in_list}) AND immutable = 1",
                        h.table
                    ),
                    &[Value::Integer(h.id)],
                )?;
                let skip: FxHashSet<AtomId> = qr
                    .rows
                    .iter()
                    .map(|r| {
                        check_cancel(cancel.as_ref())?;
                        as_int(&r[0])
                    })
                    .collect::<Result<_>>()?;
                if !skip.is_empty() {
                    targets.retain(|id| !skip.contains(id));
                    immutable_skipped = skip.into_iter().collect();
                    immutable_skipped.sort_unstable();
                }
            }
        }

        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        // Final outer poll; erase_and_delete performs its own pre-erasure check.
        check_cancel(cancel.as_ref())?;

        let (rows_deleted, slots_erased) = if targets.is_empty() {
            (0, Vec::new())
        } else {
            self.erase_and_delete(&key, &h, &targets, cancel.as_ref())
                .inspect_err(|error| self.evict_stale_region(&key, h.id, error))?
        };

        Ok(build_erasure_receipt(
            encrypted,
            rows_deleted,
            slots_erased,
            immutable_skipped,
        ))
    }

    /// [`forget_atoms`](Self::forget_atoms) extended over the reverse
    /// `DerivedFrom` closure, so no derived copy of forgotten content
    /// survives. Classification, the closure walk, the immutable gate, key
    /// destruction, and deletion share the lifecycle and edge capabilities.
    /// Plaintext cleanup uses one write transaction; encrypted cleanup is one
    /// uncancelled transaction after its keys are destroyed.
    ///
    /// Absent roots are ignored (a retry converges on a zero receipt); a
    /// root owned by another region fails loudly. Cross-table roots read as
    /// absent and the closure is region-scoped (regions are ownership
    /// boundaries) - both best-effort by design. Without `force`, any
    /// immutable atom in the closure refuses the whole cascade. Closures over
    /// [`MAX_DEPENDENT_FORGET_ATOMS`] atoms fail before key erasure or deletion.
    ///
    /// Encrypted key destruction is fail-secure and cannot be rolled back. Row
    /// cleanup therefore runs in its own uncancelled transaction after that
    /// boundary; storage failures remain retryable residue.
    pub fn forget_atoms_with_dependents(
        &self,
        region: &str,
        ids: &[AtomId],
        force: bool,
    ) -> Result<ErasureReceipt> {
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let encrypted = h.atom_wrap.is_some();

        // One lifecycle span serializes discovery, key destruction, and cleanup.
        let _kl = self.db.key_lifecycle_lock();
        let edges_guard = self.db.memory_edges_lock();
        let conn = Connection::open(&self.db)?;
        let prepare = |c: &Connection<'_>| -> Result<Option<String>> {
            self.verify_region_live(c, &h, &key)?;
            let roots = classify_cascade_roots(c, &h, &key, ids)?;
            check_cancel(cancel.as_ref())?;
            if roots.is_empty() {
                return Ok(None);
            }
            let closure =
                dependent_closure(c, &h, &roots, MAX_DEPENDENT_FORGET_ATOMS, cancel.as_ref())?;
            if !force {
                let blockers = immutable_members(c, &h, &closure)?;
                if let Some(&atom_id) = blockers.first() {
                    return Err(MemError::AtomNotMutable {
                        atom_id,
                        region: key.clone(),
                    });
                }
            }
            check_cancel(cancel.as_ref())?;
            Ok(Some(id_list(&closure)))
        };

        let (rows_deleted, slots_erased) = if encrypted {
            let in_list = match prepare(&conn) {
                Ok(Some(in_list)) => in_list,
                Ok(None) => String::new(),
                Err(error) => {
                    self.defer_stale_region(&key, h.id, &error, &_kl);
                    return Err(error);
                }
            };
            if in_list.is_empty() {
                (0, Vec::new())
            } else {
                let atom_slots = atom_key_slots(&conn, &h, &in_list)?;
                ensure_atom_key_slots_unreserved(&atom_slots, &_kl)?;
                #[cfg(test)]
                debug_fire_cancel_after_local_work();
                // Last caller poll; segment retirement may still cancel before its key commit.
                check_cancel(cancel.as_ref())?;
                self.retire_sealed_segment(&h, &conn, &_kl)?;
                #[cfg(test)]
                if FAIL_CASCADE_AFTER_SEGMENT_RETIRE.with(std::cell::Cell::take) {
                    return Err(MemError::Invalid(
                        "injected cascade failure after segment retire, before cleanup".into(),
                    ));
                }
                let slots_erased = match self.erase_atom_key_bindings(&atom_slots, &_kl) {
                    Ok(slots) => slots,
                    Err(error) => {
                        self.defer_stale_region(&key, h.id, &error, &_kl);
                        return Err(error);
                    }
                };
                #[cfg(test)]
                debug_fire_cancel_after_key_erasure();
                let cleanup: Vec<OwnedStatement> =
                    delete_atoms_statements(&h.table, &in_list, DeleteAtomsLayout::CURRENT)
                        .into_iter()
                        .map(|sql| (sql, vec![Value::Integer(h.id)]))
                        .collect();
                let results = execute_owned_uncancelled_recovery(&conn, &cleanup)
                    .inspect_err(|error| self.defer_stale_region(&key, h.id, error, &_kl))?;
                let rows_deleted = match results.last() {
                    Some(ExecutionResult::RowsAffected(count)) => *count,
                    _ => 0,
                };
                (rows_deleted, slots_erased)
            }
        } else {
            with_write_txn(&conn, |c| {
                let Some(in_list) = prepare(c)? else {
                    return Ok((0, Vec::new()));
                };
                let rows_deleted = delete_atoms_in_txn(c, &h, &in_list, &edges_guard)?;
                Ok((rows_deleted, Vec::new()))
            })
            .inspect_err(|error| self.defer_stale_region(&key, h.id, error, &_kl))?
        };

        *h.ann.write().unwrap() = None;
        Ok(build_erasure_receipt(
            encrypted,
            rows_deleted,
            slots_erased,
            Vec::new(),
        ))
    }

    /// Crypto-erase a single atom: destroy its key (overwrite + fsync +
    /// read-back) and delete its row. Siblings and the region are untouched.
    pub fn forget_atom(&self, region: &str, id: AtomId) -> Result<()> {
        self.delete_atoms(region, &[id]).map(|_| ())
    }

    /// Re-authenticate atoms by id, one [`AtomAttestation`] per id in order.
    /// Reads sealed bytes fresh from disk (not the recall cache) and recomputes
    /// the id-bound HMAC, catching tampering or a blob replayed from another
    /// row. Apart from cancellation, every requested id gets a verdict.
    pub fn verify_atoms(&self, region: &str, ids: &[AtomId]) -> Result<Vec<AtomAttestation>> {
        let cancel = check_db_cancel(&self.db)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;

        if h.atom_wrap.is_some() {
            let attestations = self.with_live_sealed_read(&key, &h, |conn, atom_wrap, _kl| {
                maintenance_verify_sealed(
                    &self.db,
                    conn,
                    h.id,
                    &h.table,
                    atom_wrap,
                    ids,
                    cancel.as_ref(),
                )
            })?;
            check_cancel(cancel.as_ref())?;
            return Ok(attestations);
        }

        let attestations = self.with_live_plain_access(&key, &h, |conn| {
            maintenance_verify_plain(conn, h.id, &h.table, ids, cancel.as_ref())
        })?;
        check_cancel(cancel.as_ref())?;
        Ok(attestations)
    }

    /// First bounded page of per-kind counts since `since_micros` (no LLM).
    pub fn summarize(&self, region: &str, since_micros: i64) -> Result<SummaryReport> {
        self.summarize_page(
            region,
            &SummaryQuery::new(since_micros, DEFAULT_SUMMARY_KIND_LIMIT),
        )
    }

    /// Per-kind counts, time span, and average importance/confidence in stable kind order.
    pub fn summarize_page(&self, region: &str, query: &SummaryQuery) -> Result<SummaryReport> {
        if query.limit == 0 || query.limit > MAX_SUMMARY_KIND_LIMIT {
            return Err(MemError::Invalid(format!(
                "summary limit must be in 1..={MAX_SUMMARY_KIND_LIMIT}, got {}",
                query.limit
            )));
        }
        let cancel = check_db_cancel(&self.db)?;
        let key = region.to_ascii_lowercase();
        let h = self.region_handle(&key)?;
        let sealed = h.atom_wrap.is_some();
        let now = now_micros();
        let read = |conn: &Connection<'_>| -> Result<SummaryReport> {
            // Keep only the lexicographically first limit+1 live kinds while
            // scanning id pages. This bounds memory even when kind cardinality
            // is attacker-controlled; the extra kind proves another page exists.
            let capacity = query.limit + 1;
            let mut digests: BTreeMap<String, (u64, i64, i64, f64, f64)> = BTreeMap::new();
            let mut total = 0u64;
            let columns = if sealed {
                "id, kind, created_at, score, confidence, key_slot, key_gen"
            } else {
                "id, kind, created_at, score, confidence"
            };
            let sql = format!(
                "SELECT {columns} FROM {table} WHERE region_id = $1 AND created_at > $2 \
                 AND (expires_at IS NULL OR expires_at > $3) AND id > $4 \
                 ORDER BY id LIMIT {EXACT_SCAN_LIMIT}",
                table = h.table
            );
            let mut id_cursor = i64::MIN;
            loop {
                check_cancel(cancel.as_ref())?;
                let qr = conn.query_params(
                    &sql,
                    &[
                        Value::Integer(h.id),
                        Value::Timestamp(query.since_micros),
                        Value::Timestamp(now),
                        Value::Integer(id_cursor),
                    ],
                )?;
                if qr.rows.is_empty() {
                    break;
                }
                let page_len = qr.rows.len();
                id_cursor = as_int(&qr.rows[page_len - 1][0])?;
                let live = if sealed {
                    exact_live_atom_binding_rows(&self.db, &qr.rows, 0, 5, 6)?
                } else {
                    vec![true; page_len]
                };
                for (row, is_live) in qr.rows.iter().zip(live) {
                    #[cfg(test)]
                    debug_fire_cancel_after_local_work();
                    check_cancel(cancel.as_ref())?;
                    if !is_live {
                        continue;
                    }
                    total = total
                        .checked_add(1)
                        .ok_or_else(|| MemError::Invalid("summary count overflow".into()))?;
                    let kind = as_text(&row[1])?;
                    if query
                        .after_kind
                        .as_deref()
                        .is_some_and(|after| kind <= after)
                    {
                        continue;
                    }
                    if !digests.contains_key(kind) && digests.len() == capacity {
                        let largest = digests
                            .last_key_value()
                            .expect("a full summary page has a largest key")
                            .0;
                        if kind > largest.as_str() {
                            continue;
                        }
                        digests.pop_last();
                    }
                    let created = as_ts(&row[2])?;
                    let score = as_f32(&row[3])? as f64;
                    let confidence = as_f32(&row[4])? as f64;
                    let digest = digests
                        .entry(kind.to_owned())
                        .or_insert((0, created, created, 0.0, 0.0));
                    digest.0 += 1;
                    digest.1 = digest.1.min(created);
                    digest.2 = digest.2.max(created);
                    digest.3 += score;
                    digest.4 += confidence;
                }
                if page_len < EXACT_SCAN_LIMIT {
                    break;
                }
            }
            let has_more = digests.len() > query.limit;
            if has_more {
                digests.pop_last();
            }
            let kinds: Vec<KindDigest> = digests
                .into_iter()
                .map(
                    |(kind, (count, earliest, latest, score, confidence))| KindDigest {
                        kind,
                        count,
                        earliest,
                        latest,
                        avg_importance: (score / count as f64) as f32,
                        avg_confidence: (confidence / count as f64) as f32,
                    },
                )
                .collect();
            let next_after_kind = has_more.then(|| {
                kinds
                    .last()
                    .expect("a non-empty bounded page precedes another page")
                    .kind
                    .clone()
            });
            Ok(SummaryReport {
                total,
                kinds,
                next_after_kind,
            })
        };
        let mut report = if sealed {
            self.with_live_sealed_read(&key, &h, |conn, _, _kl| read(conn))?
        } else {
            self.with_live_plain_access(&key, &h, read)?
        };
        check_cancel(cancel.as_ref())?;
        if let Err(error) = charge_returned_text(
            report
                .kinds
                .iter()
                .map(|digest| digest.kind.as_str())
                .chain(report.next_after_kind.as_deref()),
        ) {
            for digest in &mut report.kinds {
                digest.kind.zeroize();
            }
            if let Some(cursor) = &mut report.next_after_kind {
                cursor.zeroize();
            }
            return Err(error);
        }
        Ok(report)
    }

    /// Evict the stale entry only if it still holds the failed write's incarnation.
    fn evict_stale_region(&self, key: &str, id: RegionId, err: &MemError) {
        drop(self.take_stale_region(key, id, err));
    }

    fn defer_stale_region(
        &self,
        key: &str,
        id: RegionId,
        err: &MemError,
        lifecycle: &KeyLifecycleGuard<'_>,
    ) {
        if let Some(state) = self.take_stale_region(key, id, err) {
            lifecycle.retire_memory(Box::new(state));
        }
    }

    fn take_stale_region(
        &self,
        key: &str,
        id: RegionId,
        err: &MemError,
    ) -> Option<RetiredRegionState> {
        matches!(err, MemError::RegionNotFound(_))
            .then(|| self.take_attached_region(key, Some(id)))
            .flatten()
    }

    fn take_attached_region(
        &self,
        key: &str,
        expected_id: Option<RegionId>,
    ) -> Option<RetiredRegionState> {
        let detached = {
            let mut guard = self.regions.lock().unwrap();
            let matches = expected_id.is_none_or(|id| guard.get(key).is_some_and(|st| st.id == id));
            matches.then(|| guard.remove(key)).flatten()
        };
        detached.map(retire_region_state)
    }

    /// Revalidate inside the caller's txn; a row left by a partial erase is NOT live.
    fn verify_region_live(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        region_key: &str,
    ) -> Result<()> {
        self.verify_region_live_inner(conn, h, region_key, false)
    }

    fn verify_region_live_for_repair(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        region_key: &str,
    ) -> Result<()> {
        self.verify_region_live_inner(conn, h, region_key, true)
    }

    fn verify_region_live_inner(
        &self,
        conn: &Connection<'_>,
        h: &RegionHandle,
        region_key: &str,
        allow_repair: bool,
    ) -> Result<()> {
        let not_found = || MemError::RegionNotFound(region_key.into());
        let qr = conn.query_params(
            "SELECT name, encrypted, rsk_slot, rsk_gen, embedding_dim, embedding_metric, \
             model_id, CAST(metadata AS TEXT) \
             FROM memory_regions WHERE id = $1",
            &[Value::Integer(h.id)],
        )?;
        let Some(row) = qr.rows.first() else {
            return Err(not_found());
        };
        let encrypted = as_exact_bool(&row[1], "encrypted")?;
        if as_text(&row[0])? != region_key || encrypted != h.atom_wrap.is_some() {
            return Err(not_found());
        }
        // A re-embed rewrites the shape and the model without changing the
        // region's id, so identity does not tell a current handle from one taken
        // before the migration. Every operation captures its handle - and, for a
        // write, computes its vector - BEFORE reaching this check, so a handle
        // that crossed a completed migration would otherwise commit an old
        // model's vector under the new model's provenance, or address the table
        // the rows have already left. Neither reports anything wrong.
        let row_dim = u16::try_from(as_int(&row[4])?)
            .map_err(|_| MemError::Invalid("stored embedding_dim out of range".into()))?;
        let row_metric = metric_from_str(as_text(&row[5])?)?;
        let row_model = as_text(&row[6])?;
        if row_dim != h.dim || row_metric != h.metric || row_model != &*h.model_id {
            return Err(MemError::Invalid(format!(
                "region '{region_key}' was re-embedded to '{row_model}' ({row_dim} dim) while \
                 this handle was in use; re-attach it before reading or writing again"
            )));
        }
        // Attaching is not the only way in. A handle taken before a re-embed
        // began keeps working from cached state, so a migration that stopped
        // part-way would go on serving recalls that rank across two vector
        // spaces and accepting writes the migration has already walked past.
        // The mark is checked per access, on the row itself, because that is
        // the one thing every read and every write already consults.
        //
        let metadata = match row.get(7) {
            Some(Value::Text(value)) => Some(value.as_str()),
            Some(Value::Null) | None => None,
            _ => {
                return Err(MemError::Invalid(format!(
                    "memory region {} has unreadable metadata",
                    h.id
                )))
            }
        };
        if let Some(mark) = parse_reembed_mark(metadata, h.id)? {
            validate_repair_mark(&mark, row_model, row_dim, row_metric, h.id)?;
            if mark.phase != ReembedPhase::Repair || !allow_repair {
                return Err(MemError::Invalid(format!(
                    "region '{region_key}' stopped during re-embedding to '{}'; resume \
                     reembed_region before using it",
                    mark.to_model
                )));
            }
        }
        if !encrypted {
            return Ok(());
        }

        let slot = opt_u32(&row[2])?.ok_or_else(&not_found)?;
        let generation = opt_u64(&row[3])?.ok_or_else(&not_found)?;
        let rec = match self.db.region_store_slot(slot) {
            Ok(record) => record,
            Err(citadel_core::Error::Io(source))
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                return Err(not_found())
            }
            Err(error) => return Err(error.into()),
        };
        if rec.state != SlotState::Live || rec.region_id != h.id as u64 || rec.gen != generation {
            return Err(not_found());
        }
        Ok(())
    }

    /// Spans the RSK check + callback so reads cannot race a key-first drop; no re-entry.
    fn with_live_sealed_read<T>(
        &self,
        region_key: &str,
        h: &RegionHandle,
        read: impl FnOnce(&Connection<'_>, &AtomWrapKey, &KeyLifecycleGuard<'_>) -> Result<T>,
    ) -> Result<T> {
        let atom_wrap = h
            .atom_wrap
            .as_deref()
            .expect("with_live_sealed_read on plaintext region");
        let kl = self.db.key_lifecycle_lock();
        let result = (|| {
            let conn = Connection::open(&self.db)?;
            self.verify_region_live(&conn, h, region_key)?;
            read(&conn, atom_wrap, &kl)
        })();
        result.inspect_err(|e| self.defer_stale_region(region_key, h.id, e, &kl))
    }

    /// Span stops a cross-engine drop/recreate interleave; callbacks must not re-enter.
    fn with_live_plain_access<T>(
        &self,
        region_key: &str,
        h: &RegionHandle,
        access: impl FnOnce(&Connection<'_>) -> Result<T>,
    ) -> Result<T> {
        debug_assert!(h.atom_wrap.is_none());
        let kl = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        if let Err(error) = self.verify_region_live(&conn, h, region_key) {
            self.defer_stale_region(region_key, h.id, &error, &kl);
            return Err(error);
        }
        let _reservation = kl.reserve_memory_region(h.id as u64);
        drop(kl);
        access(&conn).inspect_err(|error| self.evict_stale_region(region_key, h.id, error))
    }

    /// Every atoms table this region's rows may be in.
    ///
    /// One, normally: the shape the region row names. A region part-way
    /// through a shape-changing re-embed has rows in two, because converted
    /// rows move to the destination table before the row describing the region
    /// is updated.
    fn region_atom_tables(&self, conn: &Connection<'_>, row: &RegionRow) -> Result<Vec<String>> {
        let home = atoms_table(row.dim, row.metric, row.encrypted);
        let mut tables = vec![home.clone()];
        if let Some(mark) = self.read_reembed_mark(conn, row.id)? {
            let destination = atoms_table(
                mark.to_dim,
                metric_from_str(&mark.to_metric)?,
                row.encrypted,
            );
            if destination != home {
                tables.push(destination);
            }
        }
        Ok(tables)
    }

    fn region_handle(&self, key: &str) -> Result<RegionHandle> {
        if let Some(handle) = self.attached_region_handle(key) {
            return Ok(handle);
        }
        let conn = Connection::open(&self.db)?;
        let exists = load_region_row(&conn, key)?.is_some();
        // An attach can publish the local handle after the catalog lookup.
        if let Some(handle) = self.attached_region_handle(key) {
            return Ok(handle);
        }
        if exists {
            Err(MemError::RegionNotAttached(key.into()))
        } else {
            Err(MemError::RegionNotFound(key.into()))
        }
    }

    fn attached_region_handle(&self, key: &str) -> Option<RegionHandle> {
        let guard = self.regions.lock().unwrap();
        let st = guard.get(key)?;
        Some(RegionHandle {
            id: st.id,
            table: atoms_table(st.dim, st.metric, st.atom_wrap.is_some()),
            embedder: st.embedder.clone(),
            dim: st.dim,
            metric: st.metric,
            model_id: Arc::clone(&st.model_id),
            atom_wrap: st.atom_wrap.clone(),
            identity_mac: st.identity_mac.clone(),
            ann: Arc::clone(&st.ann),
            max_id: Arc::clone(&st.max_id),
        })
    }

    /// Keep a region's provenance stable from before vector computation through
    /// the write that stores the result. The reservation is registered while
    /// the lifecycle capability is held, so reclassify, re-embed and drop either
    /// order before it or observe it and refuse.
    fn reserve_region_provenance<'a>(
        &'a self,
        key: &str,
        h: &RegionHandle,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<MemoryRegionGuard<'a>> {
        check_cancel(cancel)?;
        let key_lifecycle = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        if let Err(error) = self.verify_region_live(&conn, h, key) {
            let retired = matches!(error, MemError::RegionNotFound(_))
                .then(|| self.take_attached_region(key, Some(h.id)))
                .flatten();
            drop(key_lifecycle);
            drop(retired);
            return Err(error);
        }
        let reservation = key_lifecycle.reserve_memory_region(h.id as u64);
        drop(key_lifecycle);
        check_cancel(cancel)?;
        Ok(reservation)
    }

    /// Keep sealed atom keys live while local ranking or a callback holds their plaintext.
    fn reserve_plaintext_atoms<'a>(
        &'a self,
        key: &str,
        h: &RegionHandle,
        atom_ids: impl IntoIterator<Item = AtomId>,
    ) -> Result<Option<MemoryAtomCallbackGuard<'a>>> {
        if h.atom_wrap.is_none() {
            return Ok(None);
        }
        let atom_ids = dedup_sources(&atom_ids.into_iter().collect::<Vec<_>>());
        if atom_ids.is_empty() {
            return Ok(None);
        }
        let callback_ids = atom_ids
            .iter()
            .map(|&atom_id| {
                u64::try_from(atom_id).map_err(|_| {
                    MemError::Invalid(format!("atom id {atom_id} cannot name an atom key"))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let key_lifecycle = self.db.key_lifecycle_lock();
        let conn = Connection::open(&self.db)?;
        self.verify_region_live(&conn, h, key)?;
        for chunk in atom_ids.chunks(REEMBED_BATCH) {
            self.verify_atoms_live(&conn, h, key, chunk)?;
        }
        let reservation = key_lifecycle.reserve_memory_atom_callbacks(&callback_ids);
        drop(key_lifecycle);
        Ok(Some(reservation))
    }

    /// Evict cached state that no longer describes the durable region before
    /// checking the requested embedder.
    fn check_attached_incarnation(
        &self,
        key: &str,
        persisted: &RegionRow,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
        encrypted: bool,
    ) -> Result<(Option<RegionId>, bool)> {
        let stale = self.regions.lock().unwrap().get(key).is_some_and(|state| {
            state.id != persisted.id
                || state.dim != persisted.dim
                || state.metric != persisted.metric
                || state.model_id.as_ref() != persisted.model_id
                || state.atom_wrap.is_some() != persisted.encrypted
        });
        if stale {
            return Ok((None, true));
        }
        Ok((
            self.check_attached(key, dim, metric, model_id, encrypted)?,
            false,
        ))
    }

    /// Return the id if `key` is attached and the embedder matches; error on
    /// mismatch.
    fn check_attached(
        &self,
        key: &str,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
        encrypted: bool,
    ) -> Result<Option<RegionId>> {
        let guard = self.regions.lock().unwrap();
        let Some(st) = guard.get(key) else {
            return Ok(None);
        };
        if st.dim != dim {
            return Err(MemError::DimMismatch {
                region: key.into(),
                expected: st.dim,
                got: dim as usize,
            });
        }
        if st.metric != metric {
            return Err(MemError::MetricMismatch {
                region: key.into(),
                expected: metric_tag(st.metric).into(),
                got: metric_tag(metric).into(),
            });
        }
        if &*st.model_id != model_id {
            return Err(MemError::ModelMismatch {
                region: key.into(),
                expected: st.model_id.to_string(),
                got: model_id.into(),
            });
        }
        if st.atom_wrap.is_some() != encrypted {
            return Err(MemError::Invalid(format!(
                "region '{key}' already attached with encrypted={}",
                st.atom_wrap.is_some()
            )));
        }
        Ok(Some(st.id))
    }

    /// The in-flight re-embed recorded against a region, if any.
    fn read_reembed_mark(
        &self,
        conn: &Connection<'_>,
        region_id: i64,
    ) -> Result<Option<ReembedMark>> {
        let metadata = read_metadata(conn, region_id)?;
        parse_reembed_mark(metadata.as_deref(), region_id)
    }

    /// Prove that no operation changed the migration while its external
    /// embedder callback ran without the key-lifecycle capability.
    fn verify_reembed_continuation(
        &self,
        conn: &Connection<'_>,
        key: &str,
        original: &RegionRow,
        expected_checkpoint: &ReembedMark,
    ) -> Result<()> {
        let Some(current) = self.load_region_row(conn, key)? else {
            return Err(MemError::RegionNotFound(key.to_owned()));
        };
        if current != *original {
            return Err(MemError::Invalid(format!(
                "region '{key}' changed while its re-embedder was running; retry reembed_region"
            )));
        }
        if current.encrypted {
            self.verify_region_key_live(key, &current)?;
        }
        if self.read_reembed_mark(conn, current.id)?.as_ref() != Some(expected_checkpoint) {
            return Err(MemError::Invalid(format!(
                "region '{key}' advanced while its re-embedder was running; retry reembed_region"
            )));
        }
        Ok(())
    }

    fn load_region_row(&self, conn: &Connection<'_>, key: &str) -> Result<Option<RegionRow>> {
        let qr = conn.query_params(
            "SELECT id, embedding_dim, embedding_metric, model_id, encrypted, rsk_slot, rsk_gen \
             FROM memory_regions WHERE name = $1",
            &[Value::Text(key.into())],
        )?;
        let Some(row) = qr.rows.first() else {
            return Ok(None);
        };
        Ok(Some(parse_region_row(row)?))
    }

    /// Encrypted rows must hold a live RSK binding; inventory callers hold the span.
    fn load_live_region_row(&self, conn: &Connection<'_>, key: &str) -> Result<RegionRow> {
        let Some(row) = self.load_region_row(conn, key)? else {
            return Err(MemError::RegionNotFound(key.to_owned()));
        };
        if row.encrypted {
            self.verify_region_key_live(key, &row)?;
        }
        Ok(row)
    }

    fn insert_region(
        &self,
        conn: &Connection<'_>,
        key: &str,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
    ) -> Result<RegionId> {
        with_write_txn(conn, |c| {
            let id = next_id(c, "next_region_id")?;
            c.execute_params(
                "INSERT INTO memory_regions \
                 (id, name, embedding_dim, embedding_metric, model_id, encrypted, created_at, metadata) \
                 VALUES ($1, $2, $3, $4, $5, 0, CURRENT_TIMESTAMP, NULL)",
                &[
                    Value::Integer(id),
                    Value::Text(key.into()),
                    Value::Integer(dim as i64),
                    Value::Text(metric_tag(metric).into()),
                    Value::Text(model_id.into()),
                ],
            )?;
            ensure_atoms_table(c, dim, metric, false)?;
            Ok(id)
        })
    }
}

/// Encrypted-region paths: sealed writes, decrypt-then-rank reads, and key
/// lifecycle.
impl MemoryEngine {
    /// `MAX(id)` per region from one `GROUP BY region_id` scan (a per-region
    /// full scan was O(R x table)); tagged with the commit generation, so a hit
    /// is never stale.
    fn reattach_max_id(
        &self,
        conn: &Connection<'_>,
        table: &str,
        region_id: RegionId,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<i64> {
        let generation = self.db.manager().commit_generation();
        if let Some((tagged, snap)) = self.attach_max.lock().unwrap().get(table) {
            if *tagged == generation {
                return Ok(snap.get(&region_id).copied().unwrap_or(0));
            }
        }
        let mut snap = FxHashMap::default();
        if conn.table_schema(table).is_some() {
            let qr = conn.query_params(
                &format!("SELECT region_id, MAX(id) FROM {table} GROUP BY region_id"),
                &[],
            )?;
            for row in &qr.rows {
                check_cancel(cancel)?;
                snap.insert(as_int(&row[0])?, as_int(&row[1])?);
            }
        }
        check_cancel(cancel)?;
        let max = snap.get(&region_id).copied().unwrap_or(0);
        self.attach_max
            .lock()
            .unwrap()
            .insert(table.to_string(), (generation, snap));
        Ok(max)
    }

    /// Attach an existing encrypted region: read its live slot, unwrap the RCK,
    /// derive the atom-wrap key. `RegionForgotten` if the slot was tombstoned
    /// or its generation moved.
    fn live_region_wrapped(&self, name: &str, row: &RegionRow) -> Result<[u8; WRAPPED_KEY_SIZE]> {
        let slot = row
            .rsk_slot
            .ok_or_else(|| MemError::RegionForgotten(name.into()))?;
        let expected_gen = row
            .rsk_gen
            .ok_or_else(|| MemError::RegionForgotten(name.into()))?;
        let rec = match self.db.region_store_slot(slot) {
            Ok(record) => record,
            Err(citadel_core::Error::Io(source))
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                return Err(MemError::RegionForgotten(name.into()))
            }
            Err(error) => return Err(error.into()),
        };
        if rec.state != SlotState::Live || rec.gen != expected_gen || rec.region_id != row.id as u64
        {
            return Err(MemError::RegionForgotten(name.into()));
        }
        Ok(rec.wrapped)
    }

    fn verify_region_key_live(&self, name: &str, row: &RegionRow) -> Result<()> {
        self.live_region_wrapped(name, row).map(|_| ())
    }

    fn attach_region_key(&self, name: &str, row: &RegionRow) -> Result<RegionKeys> {
        let wrapped = self.live_region_wrapped(name, row)?;
        let mut rck = self.db.unwrap_region_key(&wrapped)?;
        let atom_wrap = derive_atom_wrap_key(&rck);
        let identity_mac = derive_identity_mac_key(&rck);
        rck.zeroize();
        Ok(RegionKeys {
            atom_wrap: Arc::new(atom_wrap),
            identity_mac: Arc::new(identity_mac),
        })
    }

    /// Create a new encrypted region: generate a random RCK, wrap it, persist
    /// the live slot (fsync'd) before inserting the region row, and return the
    /// atom-wrap key.
    fn insert_encrypted_region(
        &self,
        conn: &Connection<'_>,
        key: &str,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
        kl: &KeyLifecycleGuard<'_>,
    ) -> Result<(RegionId, Option<RegionKeys>)> {
        use rand::RngCore;

        // Caller holds the key-lifecycle guard across allocate -> row commit
        // so a concurrent reconcile/drop cannot tombstone the key mid-span.
        // Reserve the region id first so the key slot binds to it.
        let id = with_write_txn(conn, |c| next_id(c, "next_region_id"))?;

        let mut rck = Zeroizing::new([0u8; citadel_core::KEY_SIZE]);
        rand::thread_rng().fill_bytes(rck.as_mut());
        let wrapped = Zeroizing::new(self.db.wrap_region_key(&rck)?);

        // Persist the wrapped key (fsync'd) before the row, so a committed
        // region row always references a durable key.
        let region_owner = u64::try_from(id)
            .map_err(|_| MemError::Invalid(format!("allocated region id {id} is out of range")))?;
        let (slot, gen) = self
            .db
            .region_store_allocate_write(region_owner, &wrapped)?;
        let pending = PendingRegionSlot::new(kl, slot, region_owner, gen);
        let gen_value = match sql_key_generation(gen, "region") {
            Ok(value) => value,
            Err(error) => return pending.finish(Err(error)),
        };

        #[cfg(test)]
        if FAIL_ENCRYPTED_REGION_AFTER_SLOT.with(std::cell::Cell::take) {
            FAILED_ENCRYPTED_REGION_WRAPPED_KEY.with(|captured| {
                *captured.borrow_mut() = Some(Zeroizing::new(*wrapped));
            });
            return pending.finish(Err(MemError::Invalid(
                "injected encrypted-region failure after key-slot allocation".into(),
            )));
        }

        let inserted = with_write_txn(conn, |c| {
            c.execute_params(
                "INSERT INTO memory_regions \
                 (id, name, embedding_dim, embedding_metric, model_id, encrypted, rsk_slot, rsk_gen, created_at, metadata) \
                 VALUES ($1, $2, $3, $4, $5, 1, $6, $7, CURRENT_TIMESTAMP, NULL)",
                &[
                    Value::Integer(id),
                    Value::Text(key.into()),
                    Value::Integer(dim as i64),
                    Value::Text(metric_tag(metric).into()),
                    Value::Text(model_id.into()),
                    Value::Integer(slot as i64),
                    Value::Integer(gen_value),
                ],
            )?;
            ensure_atoms_table(c, dim, metric, true)?;
            Ok(())
        });
        pending.finish(inserted)?;

        let keys = RegionKeys {
            atom_wrap: Arc::new(derive_atom_wrap_key(&rck)),
            identity_mac: Arc::new(derive_identity_mac_key(&rck)),
        };
        Ok((id, Some(keys)))
    }

    /// ANN recall over an encrypted region via an ephemeral in-RAM PRISM index
    /// from decrypted vectors (no ANN/FTS index runs over ciphertext); cached
    /// per region and zeroized on drop.
    ///
    /// Supersession, expiry and the payload filter read plaintext, so they can
    /// only discard after the window is cut. Widen until the candidate budget
    /// survives or the window spans the region.
    fn recall_sealed_candidates(
        &self,
        h: &RegionHandle,
        resolved: ResolvedRecall<'_>,
        conn: &Connection<'_>,
        kl: &KeyLifecycleGuard<'_>,
        candidate_limit: usize,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<Candidate>> {
        let q = resolved.query;
        let mut cand_k = recall_search_window(q.k, candidate_limit);
        loop {
            let (mut cands, spanned) =
                self.sealed_window_candidates(h, resolved, conn, kl, cand_k, cancel)?;
            // Short is ambiguous: survivors ran out, or the window did. The window
            // is the nearest `cand_k`, so widening only appends.
            if spanned || cands.len() >= candidate_limit {
                cands.sort_by(|a, b| {
                    match (a.dist, b.dist) {
                        (Some(left), Some(right)) => left
                            .partial_cmp(&right)
                            .unwrap_or(std::cmp::Ordering::Equal),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    }
                    .then(a.id.cmp(&b.id))
                });
                cands.truncate(candidate_limit);
                assign_bm25_ranks(&mut cands, &query_keyword_terms(q.text.as_deref()), cancel)?;
                check_cancel(cancel)?;
                return Ok(cands);
            }
            cand_k = cand_k.saturating_mul(2);
        }
    }

    /// Decrypted candidates for one candidate window, and whether that window
    /// already spanned every atom the scan can reach.
    fn sealed_window_candidates(
        &self,
        h: &RegionHandle,
        resolved: ResolvedRecall<'_>,
        conn: &Connection<'_>,
        kl: &KeyLifecycleGuard<'_>,
        cand_k: usize,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<(Vec<Candidate>, bool)> {
        let q = resolved.query;
        let atom_wrap = h
            .atom_wrap
            .as_deref()
            .expect("sealed_window_candidates on plaintext region");
        let table = &h.table;
        let (mut ranked, ranked_table_stamp) =
            self.sealed_ann_candidates(h, conn, resolved, cand_k, kl, cancel)?;
        // Fewer ids than asked for means every atom was reached. Read before the
        // retains below, which shrink it for reasons that are not exhaustion.
        let spanned = ranked.len() < cand_k;
        if !q.include_superseded {
            // Drop stale versions before any cache read or decrypt.
            let mut ids = Vec::with_capacity(ranked.len());
            for &(id, _) in &ranked {
                check_cancel(cancel)?;
                ids.push(id);
            }
            let stale = superseded_ids(&self.db, conn, h, &ids, cancel)?;
            ranked.retain(|(id, _)| !stale.contains(id));
            check_cancel(cancel)?;
        }
        if ranked.is_empty() {
            return Ok((Vec::new(), spanned));
        }

        // The ANN cache is a ranking snapshot, not a key-liveness authority. Raw
        // corruption or a missing sidecar need not move cache_epoch/max_id, so bind
        // every candidate back to its current row and exact key-slot generation.
        let ranked_ids: Vec<AtomId> = ranked.iter().map(|(id, _)| *id).collect();
        let qr = conn.query_params(
            &format!(
                "SELECT id, key_slot, key_gen FROM {table} WHERE region_id = $1 \
                 AND id IN ({ids})",
                table = h.table,
                ids = id_list(&ranked_ids),
            ),
            &[Value::Integer(h.id)],
        )?;
        let wrapped = exact_live_atom_wrapped_rows(&self.db, &qr.rows, 0, 1, 2)?;
        let live: FxHashSet<AtomId> = qr
            .rows
            .iter()
            .zip(wrapped)
            .filter_map(|(row, wrapped)| wrapped.map(|_| as_int(&row[0])))
            .collect::<Result<_>>()?;
        ranked.retain(|(id, _)| live.contains(id));
        if ranked.is_empty() {
            return Ok((Vec::new(), spanned));
        }

        // Build candidates from the index-build cache, so the hot path touches
        // no decryption. Only post-snapshot tail atoms miss and fall through to
        // the fetch + decrypt below.
        // TTL runs on the wall clock (unlike as_of grading).
        let ttl_now = now_micros();
        let mut cands: Vec<Candidate> = Vec::with_capacity(ranked.len());
        let mut misses: Vec<(AtomId, Option<f32>)> = Vec::new();
        {
            let guard = h.ann.read().unwrap();
            if guard.as_ref().map(|sa| sa.table_stamp) != ranked_table_stamp {
                return Err(MemError::Invalid(format!(
                    "atom table '{}' changed between sealed ranking and materialization; retry",
                    h.table
                )));
            }
            let cache = guard.as_ref().map(|sa| &sa.cached);
            for &(id, dist) in &ranked {
                check_cancel(cancel)?;
                match cache.and_then(|c| c.get(&id)) {
                    Some(ca) => {
                        if ca.expires_micros.is_some_and(|e| e <= ttl_now) {
                            continue;
                        }
                        if let Some(filter) = &q.payload_filter {
                            if !json_contains(&ca.payload, filter) {
                                continue;
                            }
                        }
                        charge_materialized_bytes(ca.owned_content_bytes)?;
                        cands.push(Candidate {
                            id,
                            kind: ca.kind.clone(),
                            text: ca.text.clone(),
                            payload: ca.payload.clone(),
                            dist,
                            text_rank: 0.0,
                            importance: ca.importance,
                            confidence: ca.confidence,
                            created_micros: ca.created_micros,
                            expires_micros: ca.expires_micros,
                            immutable: ca.immutable,
                        });
                    }
                    None => misses.push((id, dist)),
                }
            }
        }

        // Tail / cache-miss atoms: fetch and decrypt only these. The hot path
        // still authenticates selected key slots above, but avoids decryption.
        if !misses.is_empty() {
            let mut id_params = Vec::with_capacity(misses.len() + 1);
            let mut placeholders = Vec::with_capacity(misses.len());
            let mut dist_by_id: FxHashMap<AtomId, Option<f32>> = FxHashMap::default();
            for (index, &(id, distance)) in misses.iter().enumerate() {
                check_cancel(cancel)?;
                id_params.push(Value::Integer(id));
                placeholders.push(format!("${}", index + 1));
                dist_by_id.insert(id, distance);
            }
            let placeholders = placeholders.join(", ");
            let sql = format!(
                "SELECT id, kind, sealed, score, confidence, created_at, immutable, expires_at, \
                 key_slot, key_gen \
                 FROM {table} \
                 WHERE id IN ({placeholders}) \
                 AND (expires_at IS NULL OR expires_at > ${})",
                id_params.len() + 1
            );
            id_params.push(Value::Timestamp(ttl_now));
            let qr = conn.query_params(&sql, &id_params)?;
            let wrapped = exact_live_atom_wrapped_rows(&self.db, &qr.rows, 0, 8, 9)?;
            for (row, wrapped) in qr.rows.iter().zip(wrapped) {
                check_cancel(cancel)?;
                let id = as_int(&row[0])?;
                let Some(wrapped) = wrapped else {
                    continue;
                };
                let (mut text, mut payload) =
                    open_atom_content(atom_wrap, &wrapped, id, as_blob(&row[2])?)?;
                if let Some(filter) = &q.payload_filter {
                    if !json_contains(&payload, filter) {
                        zeroize_atom_content(&mut text, &mut payload);
                        continue;
                    }
                }
                let kind = as_text(&row[1])?.to_string();
                charge_owned_atom_content(&kind, &mut text, &mut payload)?;
                cands.push(Candidate {
                    id,
                    kind,
                    text,
                    payload,
                    dist: dist_by_id.get(&id).copied().flatten(),
                    text_rank: 0.0,
                    importance: as_f32(&row[3])?,
                    confidence: as_f32(&row[4])?,
                    created_micros: as_ts(&row[5])?,
                    expires_micros: opt_ts(&row[7])?,
                    immutable: as_bool(&row[6])?,
                });
            }
        }

        // Close the interval between index validation, row/key binding reads,
        // and cloning cached plaintext. A concurrent raw SQL mutation does not
        // bump the managed epoch, but it always moves this non-ABA root stamp.
        if let Some(stamp) = ranked_table_stamp {
            if atom_table_root_stamp(&self.db, &h.table)? != stamp {
                let mut cache = h.ann.write().unwrap();
                if cache.as_ref().map(|ann| ann.table_stamp) == Some(stamp) {
                    cache.take();
                }
                drop(cache);
                for candidate in &mut cands {
                    zeroize_atom_content(&mut candidate.text, &mut candidate.payload);
                }
                return Err(MemError::Invalid(format!(
                    "atom table '{}' changed during sealed recall; retry",
                    h.table
                )));
            }
        }

        check_cancel(cancel)?;
        Ok((cands, spanned))
    }

    /// Top `cand_k` `(atom_id, distance)` for a sealed region: search the
    /// cached PRISM index (rebuilt if stale) plus an exact scan of atoms
    /// inserted after the snapshot.
    fn sealed_ann_candidates(
        &self,
        h: &RegionHandle,
        conn: &Connection<'_>,
        resolved: ResolvedRecall<'_>,
        cand_k: usize,
        kl: &KeyLifecycleGuard<'_>,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<SealedCandidateSnapshot> {
        let q = resolved.query;
        let qvec = resolved.vector;
        let atom_wrap = h
            .atom_wrap
            .as_ref()
            .expect("sealed_ann_candidates on plaintext region");
        let max_id = h.max_id.load(Ordering::Relaxed);
        // Stable within this call: the held guard excludes key destruction.
        let epoch = self.db.cache_epoch();
        let table_stamp = atom_table_root_stamp(&self.db, &h.table)?;

        // Fast path: a fresh index searches under a shared read lock (recalls
        // don't serialize).
        {
            let guard = h.ann.read().unwrap();
            if let Some(sa) = guard.as_ref() {
                if !sealed_index_stale(sa, max_id, epoch, table_stamp) {
                    let result = search_sealed_index(
                        sa, qvec, q, cand_k, conn, atom_wrap, &self.db, h, max_id, cancel,
                    )?;
                    return Ok((result, Some(sa.table_stamp)));
                }
            }
        }

        // Slow path under the write lock (re-checked in case another writer won).
        // A refused persisted segment is retired only after releasing this lock,
        // because key retirement invalidates ANN caches in every open engine.
        let rebuild_refusal = {
            let mut guard = h.ann.write().unwrap();
            let table_stamp = atom_table_root_stamp(&self.db, &h.table)?;
            let need_full = guard
                .as_ref()
                .map(|sa| sealed_index_stale(sa, max_id, epoch, table_stamp))
                .unwrap_or(true);
            if !need_full {
                None
            } else {
                match self.try_load_sealed_segment(h, conn, epoch, table_stamp, cancel)? {
                    Ok(loaded) => {
                        *guard = Some(loaded);
                        None
                    }
                    Err(refusal) => {
                        drop(guard);
                        Some(refusal)
                    }
                }
            }
        };
        if let Some(refusal) = rebuild_refusal {
            if refusal.is_some() {
                self.retire_sealed_segment(h, conn, kl)?;
            }
            let mut guard = h.ann.write().unwrap();
            // Pre-scan stamp: retirement may have bumped the epoch.
            let build_epoch = self.db.cache_epoch();
            let table_stamp = atom_table_root_stamp(&self.db, &h.table)?;
            let mut rows = decrypt_scan(conn, &self.db, atom_wrap, &h.table, h.id, None, cancel)?;
            if rows.is_empty() {
                *guard = None;
                return Ok((Vec::new(), None));
            }
            let mut kind_codes: FxHashMap<String, u32> = FxHashMap::default();
            let mut cached: FxHashMap<AtomId, CachedAtom> = FxHashMap::default();
            let mut zero_norm_atoms = FxHashSet::default();
            let mut triples: Vec<(u64, Vec<f32>, Vec<u32>)> = Vec::with_capacity(rows.0.len());
            for (
                id,
                emb,
                kind,
                text,
                payload,
                importance,
                confidence,
                created_micros,
                immutable,
                expires_micros,
            ) in rows.drain()
            {
                check_cancel(cancel)?;
                let next = kind_codes.len() as u32;
                let code = *kind_codes.entry(kind.clone()).or_insert(next);
                if h.metric == EmbeddingMetric::Cosine && emb.iter().all(|value| *value == 0.0) {
                    zero_norm_atoms.insert(id);
                }
                let owned_content_bytes = atom_content_bytes(&kind, &text, &payload, usize::MAX);
                cached.insert(
                    id,
                    CachedAtom {
                        kind,
                        text,
                        payload,
                        owned_content_bytes,
                        importance,
                        confidence,
                        created_micros,
                        immutable,
                        expires_micros,
                    },
                );
                triples.push((id as u64, emb, vec![code]));
            }
            let index = AnnIndex::build_with_attrs(triples, 1, ann_metric(h.metric), h.dim)
                .map_err(|e| MemError::Invalid(format!("sealed ANN index build: {e}")))?;
            check_cancel(cancel)?;
            if atom_table_root_stamp(&self.db, &h.table)? != table_stamp {
                return Err(MemError::Invalid(format!(
                    "atom table '{}' changed while its sealed ANN cache was built; retry recall",
                    h.table
                )));
            }
            *guard = Some(SealedAnn {
                index,
                kind_codes,
                cached,
                zero_norm_atoms,
                source: AnnIndexSource::Built { refusal },
                build_epoch,
                table_stamp,
            });
        }

        let guard = h.ann.read().unwrap();
        let Some(sa) = guard.as_ref() else {
            return Ok((Vec::new(), None));
        };
        let table_stamp = sa.table_stamp;
        let result = search_sealed_index(
            sa, qvec, q, cand_k, conn, atom_wrap, &self.db, h, max_id, cancel,
        )?;
        drop(guard);
        Ok((result, Some(table_stamp)))
    }

    fn fetch_sealed(
        &self,
        h: &RegionHandle,
        q: &FetchQuery,
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<AtomHit>> {
        let mut params: Vec<Value> = vec![Value::Integer(h.id)];
        let mut preds = String::new();
        if let Some(kind) = &q.kind {
            params.push(Value::Text(kind.as_str().into()));
            preds += &format!(" AND kind = ${}", params.len());
        }
        if let Some(from) = q.created_from {
            params.push(Value::Timestamp(from));
            preds += &format!(" AND created_at >= ${}", params.len());
        }
        if let Some(before) = q.created_before {
            params.push(Value::Timestamp(before));
            preds += &format!(" AND created_at < ${}", params.len());
        }
        params.push(Value::Timestamp(now_micros()));
        preds += &format!(
            " AND (expires_at IS NULL OR expires_at > ${})",
            params.len()
        );
        // Walking down, `after_id` is a fixed bound, not the ascending watermark.
        if q.newest {
            if let Some(after) = q.after_id {
                params.push(Value::Integer(after));
                preds += &format!(" AND id > ${}", params.len());
            }
        }
        // The payload filter runs after decryption, so page by id until
        // `limit` is met or drained; the id cursor doubles as the watermark.
        let page_param = params.len() + 1;
        let (cmp, dir) = if q.newest {
            ("<", "DESC")
        } else {
            (">", "ASC")
        };
        let sql = format!(
            "SELECT id, kind, sealed, score, confidence, immutable, created_at, expires_at, \
             key_slot, key_gen \
             FROM {table} \
             WHERE region_id = $1{preds} AND id {cmp} ${page_param} \
             ORDER BY id {dir} LIMIT {EXACT_SCAN_LIMIT}",
            table = h.table
        );

        let mut out = Vec::new();
        let mut last_id: AtomId = if q.newest {
            i64::MAX
        } else {
            q.after_id.unwrap_or(i64::MIN)
        };
        'pages: loop {
            check_cancel(cancel)?;
            let mut page_params = params.clone();
            page_params.push(Value::Integer(last_id));
            let qr = conn.query_params(&sql, &page_params)?;
            if qr.rows.is_empty() {
                break;
            }
            let wrapped = exact_live_atom_wrapped_rows(&self.db, &qr.rows, 0, 8, 9)?;
            let batch = qr.rows.len();
            for (row, wrapped) in qr.rows.iter().zip(wrapped) {
                check_cancel(cancel)?;
                let id = as_int(&row[0])?;
                last_id = id;
                let Some(wrapped) = wrapped else {
                    continue;
                };
                let (mut text, mut payload) =
                    open_atom_content(atom_wrap, &wrapped, id, as_blob(&row[2])?)?;
                if let Some(filter) = &q.payload_filter {
                    if !json_contains(&payload, filter) {
                        zeroize_atom_content(&mut text, &mut payload);
                        continue;
                    }
                }
                let kind = as_text(&row[1])?.to_string();
                charge_owned_atom_content(&kind, &mut text, &mut payload)?;
                out.push(AtomHit {
                    id,
                    kind,
                    payload,
                    text,
                    importance: as_f32(&row[3])?,
                    confidence: as_f32(&row[4])?,
                    relevance: None,
                    distance: None,
                    graph_depth: None,
                    created_at: as_ts(&row[6])?,
                    expires_at: opt_ts(&row[7])?,
                    immutable: as_bool(&row[5])?,
                });
                if out.len() >= q.limit {
                    break 'pages;
                }
            }
            if batch < EXACT_SCAN_LIMIT {
                break;
            }
        }
        // The window was taken from the end; callers still read oldest first.
        if q.newest {
            out.reverse();
        }
        Ok(out)
    }

    fn fetch_one_sealed(
        &self,
        h: &RegionHandle,
        atom_id: AtomId,
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
    ) -> Result<Option<AtomHit>> {
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, sealed, score, confidence, immutable, created_at, expires_at, \
                 key_slot, key_gen \
                 FROM {table} WHERE id = $1 AND region_id = $2 \
                 AND (expires_at IS NULL OR expires_at > $3)",
                table = h.table
            ),
            &[
                Value::Integer(atom_id),
                Value::Integer(h.id),
                Value::Timestamp(now_micros()),
            ],
        )?;
        let Some(row) = qr.rows.first() else {
            return Ok(None);
        };
        let id = as_int(&row[0])?;
        // Erased/recycled key = absent atom: the same triple bind recall applies.
        let Some(wrapped) = exact_live_atom_wrapped(&self.db, id, &row[8], &row[9])? else {
            return Ok(None);
        };
        let (mut text, mut payload) =
            open_atom_content(atom_wrap, &wrapped, id, as_blob(&row[2])?)?;
        let kind = as_text(&row[1])?.to_string();
        charge_owned_atom_content(&kind, &mut text, &mut payload)?;
        Ok(Some(AtomHit {
            id,
            kind,
            payload,
            text,
            importance: as_f32(&row[3])?,
            confidence: as_f32(&row[4])?,
            relevance: None,
            distance: None,
            graph_depth: None,
            created_at: as_ts(&row[6])?,
            expires_at: opt_ts(&row[7])?,
            immutable: as_bool(&row[5])?,
        }))
    }

    fn fetch_last_sealed(
        &self,
        h: &RegionHandle,
        kind: &str,
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Option<AtomHit>> {
        // Skip erased/recycled residue so it cannot mask the genuine latest atom.
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, sealed, score, confidence, immutable, created_at, expires_at, \
                 key_slot, key_gen \
                 FROM {table} WHERE region_id = $1 AND kind = $2 \
                 AND (expires_at IS NULL OR expires_at > $3) ORDER BY id DESC",
                table = h.table
            ),
            &[
                Value::Integer(h.id),
                Value::Text(kind.into()),
                Value::Timestamp(now_micros()),
            ],
        )?;
        let wrapped = exact_live_atom_wrapped_rows(&self.db, &qr.rows, 0, 8, 9)?;
        for (row, wrapped) in qr.rows.iter().zip(wrapped) {
            check_cancel(cancel)?;
            let id = as_int(&row[0])?;
            let Some(wrapped) = wrapped else {
                continue;
            };
            let (mut text, mut payload) =
                open_atom_content(atom_wrap, &wrapped, id, as_blob(&row[2])?)?;
            let kind = as_text(&row[1])?.to_string();
            charge_owned_atom_content(&kind, &mut text, &mut payload)?;
            return Ok(Some(AtomHit {
                id,
                kind,
                payload,
                text,
                importance: as_f32(&row[3])?,
                confidence: as_f32(&row[4])?,
                relevance: None,
                distance: None,
                graph_depth: None,
                created_at: as_ts(&row[6])?,
                expires_at: opt_ts(&row[7])?,
                immutable: as_bool(&row[5])?,
            }));
        }
        Ok(None)
    }

    /// Validate and prepare a sealed payload rewrite before segment retirement.
    fn prepare_atom_payload_update_sealed(
        &self,
        context: &SealedPayloadUpdateContext<'_, '_>,
        atom_id: AtomId,
        payload: &serde_json::Value,
    ) -> Result<Option<PreparedSealedPayloadUpdate>> {
        let new_payload = Zeroizing::new(
            serde_json::to_string(payload)
                .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?,
        );
        let qr = context.conn.query_params(
            &format!(
                "SELECT sealed, key_slot, key_gen FROM {table} \
                 WHERE id = $1 AND region_id = $2 AND immutable = 0 \
                 AND (expires_at IS NULL OR expires_at > $3)",
                table = context.handle.table
            ),
            &[
                Value::Integer(atom_id),
                Value::Integer(context.handle.id),
                Value::Timestamp(context.now),
            ],
        )?;
        let Some(row) = qr.rows.first() else {
            return Err(MemError::AtomNotMutable {
                atom_id,
                region: context.region_key.to_owned(),
            });
        };
        let binding = atom_key_binding(atom_id, &row[1], &row[2])?;
        let Some(wrapped) = exact_live_atom_wrapped(&self.db, atom_id, &row[1], &row[2])? else {
            return Err(MemError::AtomNotMutable {
                atom_id,
                region: context.region_key.to_owned(),
            });
        };
        let ack = Zeroizing::new(context.atom_wrap.unwrap_atom_key(&wrapped)?);
        let seal_keys = derive_seal_keys(&ack);
        let old_blob = Zeroizing::new(blob_seal::open(
            &seal_keys,
            atom_id as u64,
            as_blob(&row[0])?,
        )?);
        let (emb, text, old_payload) = decode_atom_blob(&old_blob)?;
        let emb = Zeroizing::new(emb);
        let text = Zeroizing::new(text);
        let old_payload = Zeroizing::new(old_payload);
        let mut stored: serde_json::Value =
            serde_json::from_str(&old_payload).map_err(|error| {
                MemError::Invalid(format!("atom {atom_id} payload is invalid JSON: {error}"))
            })?;
        let unchanged = &stored == payload;
        zeroize_json_strings(&mut stored);
        if unchanged {
            return Ok(None);
        }
        let blob = Zeroizing::new(encode_atom_blob(&emb, &text, &new_payload));
        Ok(Some(PreparedSealedPayloadUpdate {
            sealed: blob_seal::seal(&seal_keys, atom_id as u64, &blob),
            binding,
        }))
    }

    /// Publish a prevalidated rewrite after the old persisted segment is retired.
    fn update_atom_payload_sealed(
        &self,
        key: &str,
        h: &RegionHandle,
        atom_id: AtomId,
        prepared: PreparedSealedPayloadUpdate,
        now: i64,
        conn: &Connection<'_>,
    ) -> Result<()> {
        let table = h.table.clone();
        with_write_txn(conn, |c| {
            let qr = c.query_params(
                &format!(
                    "SELECT key_slot, key_gen FROM {table} \
                     WHERE id = $1 AND region_id = $2 AND immutable = 0 \
                     AND (expires_at IS NULL OR expires_at > $3)"
                ),
                &[
                    Value::Integer(atom_id),
                    Value::Integer(h.id),
                    Value::Timestamp(now),
                ],
            )?;
            let Some(row) = qr.rows.first() else {
                return Err(MemError::AtomNotMutable {
                    atom_id,
                    region: key.to_owned(),
                });
            };
            let binding = atom_key_binding(atom_id, &row[0], &row[1])?;
            if binding != prepared.binding
                || !exact_live_atom_bindings_batch(&self.db, &[binding])?[0]
            {
                return Err(MemError::AtomNotMutable {
                    atom_id,
                    region: key.to_owned(),
                });
            }
            match c.execute_params(
                &format!(
                    "UPDATE {table} SET sealed = $1 WHERE id = $2 AND region_id = $3 \
                     AND immutable = 0 AND (expires_at IS NULL OR expires_at > $4)"
                ),
                &[
                    Value::Blob(prepared.sealed),
                    Value::Integer(atom_id),
                    Value::Integer(h.id),
                    Value::Timestamp(now),
                ],
            )? {
                ExecutionResult::RowsAffected(0) => Err(MemError::AtomNotMutable {
                    atom_id,
                    region: key.to_owned(),
                }),
                _ => Ok(()),
            }
        })
    }

    /// Ids of non-immutable sealed atoms whose payload `@>`-contains
    /// `predicate`. Exhaustive: forgetting must not silently under-delete, so
    /// this pages through every atom by id (unlike the bounded read paths)
    /// until the region is drained.
    fn evict_predicate_sealed_ids(
        &self,
        h: &RegionHandle,
        predicate: &serde_json::Value,
        conn: &Connection<'_>,
        atom_wrap: &AtomWrapKey,
        cancel: Option<&citadel_core::CancelToken>,
    ) -> Result<Vec<AtomId>> {
        check_cancel(cancel)?;
        let sql = format!(
            "SELECT id, sealed, key_slot, key_gen FROM {table} \
             WHERE region_id = $1 AND immutable = 0 \
             AND id > $2 ORDER BY id LIMIT {EXACT_SCAN_LIMIT}",
            table = h.table
        );
        let mut ids = Vec::new();
        let mut last_id: AtomId = i64::MIN;
        loop {
            check_cancel(cancel)?;
            let qr = conn.query_params(&sql, &[Value::Integer(h.id), Value::Integer(last_id)])?;
            if qr.rows.is_empty() {
                break;
            }
            let wrapped = exact_live_atom_wrapped_rows(&self.db, &qr.rows, 0, 2, 3)?;
            for (row, wrapped) in qr.rows.iter().zip(wrapped) {
                #[cfg(test)]
                debug_fire_cancel_after_local_work();
                check_cancel(cancel)?;
                let id = as_int(&row[0])?;
                last_id = id;
                let Some(wrapped) = wrapped else {
                    continue;
                };
                let (mut text, mut payload) =
                    open_atom_content(atom_wrap, &wrapped, id, as_blob(&row[1])?)?;
                let matched = json_contains(&payload, predicate);
                zeroize_atom_content(&mut text, &mut payload);
                if matched {
                    ids.push(id);
                }
            }
            if qr.rows.len() < EXACT_SCAN_LIMIT {
                break;
            }
        }
        check_cancel(cancel)?;
        Ok(ids)
    }
}

fn atom_table_root_stamp(db: &Database, table: &str) -> Result<(PageId, TxnId)> {
    let mut read = db.begin_read();
    read.table_root_stamp(table.as_bytes())?
        .ok_or_else(|| MemError::Invalid(format!("atom table '{table}' is missing")))
}

/// Cached index needs a rebuild when either the managed epoch or the underlying
/// table snapshot moved, or when its exact-ranked tail grew too large.
fn sealed_index_stale(
    sa: &SealedAnn,
    max_id: i64,
    epoch: u64,
    table_stamp: (PageId, TxnId),
) -> bool {
    sa.build_epoch != epoch
        || sa.table_stamp != table_stamp
        || sa.index.tail_is_stale(max_id.max(0) as u64)
}

/// Top `cand_k` `(atom_id, distance)` from the cached index, plus exact-ranked
/// atoms inserted after its snapshot.
#[allow(clippy::too_many_arguments)]
fn search_sealed_index(
    sa: &SealedAnn,
    qvec: &[f32],
    q: &RecallQuery,
    cand_k: usize,
    conn: &Connection<'_>,
    atom_wrap: &AtomWrapKey,
    db: &Database,
    h: &RegionHandle,
    max_id: i64,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<(AtomId, Option<f32>)>> {
    check_cancel(cancel)?;
    // A kind absent from the snapshot can still exist in the tail, so an empty
    // code set skips only the index search, not the tail scan below.
    let filter = if q.kinds.is_empty() {
        Some(Filter::none())
    } else {
        let mut codes = Vec::with_capacity(q.kinds.len());
        for kind in &q.kinds {
            check_cancel(cancel)?;
            if let Some(code) = sa.kind_codes.get(kind).copied() {
                codes.push(code);
            }
        }
        if codes.is_empty() {
            None
        } else {
            Some(Filter::new(vec![(0, codes)]))
        }
    };

    let found = filter
        .map(|f| sa.index.search_filtered_default_ef(qvec, cand_k, &f))
        .transpose()
        .map_err(|e| MemError::Invalid(format!("sealed ANN search failed: {e}")))?
        .unwrap_or_default();
    check_cancel(cancel)?;
    let mut ranked = Vec::with_capacity(found.len());
    let zero_norm_query =
        h.metric == EmbeddingMetric::Cosine && qvec.iter().all(|value| *value == 0.0);
    for (id, distance) in found {
        check_cancel(cancel)?;
        let id = id as AtomId;
        let distance = if zero_norm_query || sa.zero_norm_atoms.contains(&id) {
            None
        } else {
            Some(distance)
        };
        ranked.push((id, distance));
    }

    // Exact-rank atoms inserted after the snapshot. This is the only hot-path
    // decryption; selected ACK slots for cached candidates are authenticated
    // separately before their cached plaintext is materialized.
    let snap = sa.index.snapshot_max as i64;
    if max_id > snap {
        let ttl_now = now_micros();
        let mut tail = decrypt_scan(conn, db, atom_wrap, &h.table, h.id, Some(snap), cancel)?;
        for (id, mut emb, kind, mut text, mut payload, _, _, _, _, expires) in tail.drain() {
            check_cancel(cancel)?;
            let included = (q.kinds.is_empty() || q.kinds.iter().any(|k| k == &kind))
                && expires.is_none_or(|expires| expires > ttl_now);
            if included {
                ranked.push((id, vec_distance(h.metric, qvec, &emb)));
            }
            emb.zeroize();
            text.zeroize();
            zeroize_json_strings(&mut payload);
        }
    }

    check_cancel(cancel)?;
    ranked.sort_by(|a, b| match (a.1, b.1) {
        (Some(left), Some(right)) => left
            .partial_cmp(&right)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.0.cmp(&b.0),
    });
    check_cancel(cancel)?;
    ranked.truncate(cand_k);
    Ok(ranked)
}

/// INSERT one sealed atom into an `_enc` table.
#[allow(clippy::too_many_arguments)]
fn insert_sealed_atom(
    c: &Connection<'_>,
    table: &str,
    id: AtomId,
    region_id: RegionId,
    kind: &str,
    sealed: Vec<u8>,
    key_slot: u32,
    key_gen: u64,
    score: f32,
    confidence: f32,
    immutable: i64,
    created: Value,
    expires: Value,
) -> Result<()> {
    let key_gen = sql_key_generation(key_gen, "atom")?;
    c.execute_params(
        &format!(
            "INSERT INTO {table} \
             (id, region_id, kind, sealed, key_slot, key_gen, score, confidence, access_count, \
              immutable, created_at, accessed_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, $10, CURRENT_TIMESTAMP, $11)"
        ),
        &[
            Value::Integer(id),
            Value::Integer(region_id),
            Value::Text(kind.into()),
            Value::Blob(sealed),
            Value::Integer(key_slot as i64),
            Value::Integer(key_gen),
            Value::Real(score as f64),
            Value::Real(confidence as f64),
            Value::Integer(immutable),
            created,
            expires,
        ],
    )?;
    Ok(())
}

/// The plaintext payload sealed per encrypted atom: `dim | embedding(f32 LE) |
/// text | payload-json`, each variable field length-prefixed (u32 LE).
fn encode_atom_blob(embedding: &[f32], text: &str, payload_json: &str) -> Vec<u8> {
    let dim = embedding.len() as u16;
    let tb = text.as_bytes();
    let pb = payload_json.as_bytes();
    let mut out = Vec::with_capacity(2 + embedding.len() * 4 + 4 + tb.len() + 4 + pb.len());
    out.extend_from_slice(&dim.to_le_bytes());
    for &f in embedding {
        out.extend_from_slice(&f.to_le_bytes());
    }
    out.extend_from_slice(&(tb.len() as u32).to_le_bytes());
    out.extend_from_slice(tb);
    out.extend_from_slice(&(pb.len() as u32).to_le_bytes());
    out.extend_from_slice(pb);
    out
}

struct AtomBlobParts<'a> {
    dim: usize,
    embedding: &'a [u8],
    text: &'a [u8],
    payload: &'a [u8],
}

/// Validate framing once so narrow decoders borrow fields without redoing bounds.
fn parse_atom_blob(b: &[u8]) -> Result<AtomBlobParts<'_>> {
    let truncated = || MemError::Invalid("sealed atom blob is truncated".into());
    let mut o = 0usize;
    let take = |o: &mut usize, n: usize| -> Result<std::ops::Range<usize>> {
        let end = o
            .checked_add(n)
            .filter(|&end| end <= b.len())
            .ok_or_else(truncated)?;
        let range = *o..end;
        *o = end;
        Ok(range)
    };

    let dim = u16::from_le_bytes(b[take(&mut o, 2)?].try_into().map_err(|_| truncated())?) as usize;
    let embedding_len = dim.checked_mul(4).ok_or_else(truncated)?;
    let embedding = &b[take(&mut o, embedding_len)?];
    let tlen =
        u32::from_le_bytes(b[take(&mut o, 4)?].try_into().map_err(|_| truncated())?) as usize;
    let text = &b[take(&mut o, tlen)?];
    let plen =
        u32::from_le_bytes(b[take(&mut o, 4)?].try_into().map_err(|_| truncated())?) as usize;
    let payload = &b[take(&mut o, plen)?];
    if o != b.len() {
        return Err(MemError::Invalid(
            "sealed atom blob has trailing bytes".into(),
        ));
    }
    Ok(AtomBlobParts {
        dim,
        embedding,
        text,
        payload,
    })
}

fn decode_embedding(parts: &AtomBlobParts<'_>) -> Vec<f32> {
    let mut embedding = Vec::with_capacity(parts.dim);
    for bytes in parts.embedding.as_chunks::<4>().0 {
        embedding.push(f32::from_le_bytes(*bytes));
    }
    embedding
}

fn decode_atom_blob(b: &[u8]) -> Result<(Vec<f32>, String, String)> {
    let parts = parse_atom_blob(b)?;
    let mut embedding = decode_embedding(&parts);
    let mut text = match std::str::from_utf8(parts.text) {
        Ok(text) => text.to_owned(),
        Err(_) => {
            embedding.zeroize();
            return Err(MemError::Invalid("sealed text is not valid UTF-8".into()));
        }
    };
    let payload = match std::str::from_utf8(parts.payload) {
        Ok(payload) => payload.to_owned(),
        Err(_) => {
            embedding.zeroize();
            text.zeroize();
            return Err(MemError::Invalid(
                "sealed payload is not valid UTF-8".into(),
            ));
        }
    };
    Ok((embedding, text, payload))
}

/// Decode only the vector; text/payload are framing-checked, never allocated.
fn decode_atom_embedding(b: &[u8]) -> Result<Vec<f32>> {
    Ok(decode_embedding(&parse_atom_blob(b)?))
}

/// Decode only the text; payload bytes are framing-checked, never materialized.
fn decode_atom_text(b: &[u8]) -> Result<String> {
    let parts = parse_atom_blob(b)?;
    std::str::from_utf8(parts.text)
        .map(str::to_owned)
        .map_err(|_| MemError::Invalid("sealed text is not valid UTF-8".into()))
}

/// Decode only user-visible content; the embedding is never copied to a second alloc.
fn decode_atom_content(b: &[u8]) -> Result<(String, String)> {
    let parts = parse_atom_blob(b)?;
    let mut text = std::str::from_utf8(parts.text)
        .map(str::to_owned)
        .map_err(|_| MemError::Invalid("sealed text is not valid UTF-8".into()))?;
    let payload = match std::str::from_utf8(parts.payload) {
        Ok(payload) => payload.to_owned(),
        Err(_) => {
            text.zeroize();
            return Err(MemError::Invalid(
                "sealed payload is not valid UTF-8".into(),
            ));
        }
    };
    Ok((text, payload))
}

/// Streaming identity state shared by plaintext and encrypted stored-vector scans.
struct StoredEmbeddingScan<'a> {
    hasher: Sha256,
    expected: Option<&'a [(AtomId, Vec<f32>)]>,
    count: u64,
    dim: usize,
    region: String,
    kind: String,
}

impl<'a> StoredEmbeddingScan<'a> {
    fn new(region: &str, kind: &str, dim: u16, expected: Option<&'a [(AtomId, Vec<f32>)]>) -> Self {
        let mut hasher = Sha256::new();
        hash_len_prefixed(&mut hasher, STORED_EMBEDDINGS_SCHEMA.as_bytes());
        hash_len_prefixed(&mut hasher, region.as_bytes());
        hash_len_prefixed(&mut hasher, kind.as_bytes());
        hasher.update(u32::from(dim).to_le_bytes());
        Self {
            hasher,
            expected,
            count: 0,
            dim: usize::from(dim),
            region: region.to_owned(),
            kind: kind.to_owned(),
        }
    }

    fn consume(&mut self, id: AtomId, embedding: &[f32]) -> Result<()> {
        if embedding.len() != self.dim {
            return Err(MemError::Invalid(format!(
                "stored embedding for atom {id} has dimension {}, expected {}",
                embedding.len(),
                self.dim
            )));
        }
        if let Some((component, _)) = embedding
            .iter()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(MemError::Invalid(format!(
                "stored embedding for atom {id} has a non-finite component at index {component}"
            )));
        }

        if let Some(expected) = self.expected {
            let index = usize::try_from(self.count)
                .map_err(|_| MemError::Invalid("stored embedding count overflow".into()))?;
            let Some((expected_id, expected_embedding)) = expected.get(index) else {
                return Err(MemError::Invalid(format!(
                    "stored embedding count exceeds expected count {}",
                    expected.len()
                )));
            };
            if id != *expected_id {
                return Err(MemError::Invalid(format!(
                    "stored embedding id mismatch at index {index}: stored {id}, expected {expected_id}"
                )));
            }
            if let Some(component) = embedding
                .iter()
                .zip(expected_embedding)
                .position(|(stored, expected)| stored.to_bits() != expected.to_bits())
            {
                return Err(MemError::Invalid(format!(
                    "stored embedding bit mismatch for atom {id} at component {component}"
                )));
            }
        }

        self.hasher.update(id.to_le_bytes());
        for value in embedding {
            self.hasher.update(value.to_bits().to_le_bytes());
        }
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| MemError::Invalid("stored embedding count overflow".into()))?;
        Ok(())
    }

    fn finish(mut self) -> Result<StoredEmbeddingsIdentity> {
        if let Some(expected) = self.expected {
            let actual = usize::try_from(self.count)
                .map_err(|_| MemError::Invalid("stored embedding count overflow".into()))?;
            if actual != expected.len() {
                return Err(MemError::Invalid(format!(
                    "stored embedding count mismatch: stored {actual}, expected {}",
                    expected.len()
                )));
            }
        }
        self.hasher.update(self.count.to_le_bytes());
        let digest = self.hasher.finalize();
        Ok(StoredEmbeddingsIdentity::new(
            self.region,
            self.kind,
            self.count,
            self.dim as u32,
            hex_lower(&digest),
        ))
    }
}

fn validate_expected_embeddings(expected: &[(AtomId, Vec<f32>)], dim: usize) -> Result<()> {
    let mut previous = None;
    for (index, (id, embedding)) in expected.iter().enumerate() {
        if previous.is_some_and(|previous_id| *id <= previous_id) {
            return Err(MemError::Invalid(format!(
                "expected embedding ids must be strictly ascending; id {id} at index {index} follows {}",
                previous.expect("checked as some")
            )));
        }
        if embedding.len() != dim {
            return Err(MemError::Invalid(format!(
                "expected embedding for atom {id} has dimension {}, expected {dim}",
                embedding.len()
            )));
        }
        if let Some((component, _)) = embedding
            .iter()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(MemError::Invalid(format!(
                "expected embedding for atom {id} has a non-finite component at index {component}"
            )));
        }
        previous = Some(*id);
    }
    Ok(())
}

fn hash_len_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    out
}

/// One decrypted sealed atom with the fields recall caches: `(id, embedding,
/// kind, text, payload, importance, confidence, created_micros, immutable,
/// expires_micros)`.
type DecryptedAtom = (
    AtomId,
    Vec<f32>,
    String,
    String,
    serde_json::Value,
    f32,
    f32,
    i64,
    bool,
    Option<i64>,
);

/// Zeroizes decrypted rows on every early return before ANN/cache take ownership.
struct DecryptedAtoms(Vec<DecryptedAtom>);

impl DecryptedAtoms {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn drain(&mut self) -> std::vec::Drain<'_, DecryptedAtom> {
        self.0.drain(..)
    }
}

impl Drop for DecryptedAtoms {
    fn drop(&mut self) {
        for (_, embedding, _, text, payload, ..) in &mut self.0 {
            embedding.zeroize();
            text.zeroize();
            zeroize_json_strings(payload);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct AtomKeyBinding {
    slot: u32,
    atom_id: u64,
    generation: u64,
}

struct PreparedSealedPayloadUpdate {
    sealed: Vec<u8>,
    binding: AtomKeyBinding,
}

struct SealedPayloadUpdateContext<'a, 'db> {
    region_key: &'a str,
    handle: &'a RegionHandle,
    now: i64,
    conn: &'a Connection<'db>,
    atom_wrap: &'a AtomWrapKey,
}

/// Parse the row-side half of the exact ACK binding. An id-only lookup can accept a
/// detached or recycled key even though the row no longer names that slot generation.
fn atom_key_binding(atom_id: AtomId, slot: &Value, generation: &Value) -> Result<AtomKeyBinding> {
    Ok(AtomKeyBinding {
        slot: u32::try_from(as_int(slot)?)
            .map_err(|_| MemError::Invalid(format!("atom {atom_id} key_slot is out of range")))?,
        atom_id: u64::try_from(atom_id)
            .map_err(|_| MemError::Invalid(format!("atom id {atom_id} is out of range")))?,
        generation: u64::try_from(as_int(generation)?).map_err(|_| {
            MemError::Invalid(format!("atom {atom_id} key generation is out of range"))
        })?,
    })
}

/// Resolve the exact ACK binding: id alone could decrypt via a detached or recycled slot.
fn exact_live_atom_wrapped(
    db: &Database,
    atom_id: AtomId,
    slot: &Value,
    generation: &Value,
) -> Result<Option<[u8; WRAPPED_KEY_SIZE]>> {
    let binding = atom_key_binding(atom_id, slot, generation)?;
    Ok(exact_live_atom_wrapped_batch(db, &[binding])?
        .into_iter()
        .next()
        .expect("one binding returns one result"))
}

/// Resolve selected row bindings with one key-store lock and file open.
fn exact_live_atom_wrapped_batch(
    db: &Database,
    bindings: &[AtomKeyBinding],
) -> Result<Vec<Option<[u8; WRAPPED_KEY_SIZE]>>> {
    if bindings.is_empty() {
        return Ok(Vec::new());
    }
    let slots: Vec<u32> = bindings.iter().map(|binding| binding.slot).collect();
    let records = db.atom_store_slots(&slots).map_err(|error| match error {
        citadel_core::Error::Io(source) if source.kind() == std::io::ErrorKind::NotFound => {
            MemError::Invalid("atom key store is missing for a non-empty encrypted region".into())
        }
        other => MemError::Core(other),
    })?;
    Ok(records
        .into_iter()
        .zip(bindings)
        .map(|(record, binding)| {
            (record.state == SlotState::Live
                && record.region_id == binding.atom_id
                && record.gen == binding.generation)
                .then_some(record.wrapped)
        })
        .collect())
}

fn maintenance_live_atom_wrapped_rows(
    db: &Database,
    rows: &[Vec<Value>],
    id_column: usize,
    slot_column: usize,
    generation_column: usize,
) -> Result<Vec<Result<Option<[u8; WRAPPED_KEY_SIZE]>>>> {
    let bindings = rows
        .iter()
        .map(|row| {
            atom_key_binding(
                as_int(&row[id_column])?,
                &row[slot_column],
                &row[generation_column],
            )
        })
        .collect::<Vec<_>>();
    let slots = bindings
        .iter()
        .filter_map(|binding| binding.as_ref().ok().map(|binding| binding.slot))
        .collect::<Vec<_>>();
    let records = db
        .atom_store_slot_results(&slots)
        .map_err(|error| match error {
            citadel_core::Error::Io(source) if source.kind() == std::io::ErrorKind::NotFound => {
                MemError::Invalid(
                    "atom key store is missing for a non-empty encrypted region".into(),
                )
            }
            other => MemError::Core(other),
        })?;
    let mut records = records.into_iter();
    Ok(bindings
        .into_iter()
        .map(|binding| {
            let binding = binding?;
            let record = records
                .next()
                .expect("one selected key-store result per valid row binding")
                .map_err(MemError::Core)?;
            Ok((record.state == SlotState::Live
                && record.region_id == binding.atom_id
                && record.gen == binding.generation)
                .then_some(record.wrapped))
        })
        .collect())
}

/// Check exact row/key bindings without returning wrapped ACK bytes.
fn exact_live_atom_bindings_batch(db: &Database, bindings: &[AtomKeyBinding]) -> Result<Vec<bool>> {
    if bindings.is_empty() {
        return Ok(Vec::new());
    }
    let tuples: Vec<(u32, u64, u64)> = bindings
        .iter()
        .map(|binding| (binding.slot, binding.atom_id, binding.generation))
        .collect();
    db.atom_store_bindings_live(&tuples)
        .map_err(|error| match error {
            citadel_core::Error::Io(source) if source.kind() == std::io::ErrorKind::NotFound => {
                MemError::Invalid(
                    "atom key store is missing for a non-empty encrypted region".into(),
                )
            }
            other => MemError::Core(other),
        })
}

fn exact_live_atom_binding_rows(
    db: &Database,
    rows: &[Vec<Value>],
    id_column: usize,
    slot_column: usize,
    generation_column: usize,
) -> Result<Vec<bool>> {
    let bindings = rows
        .iter()
        .map(|row| {
            atom_key_binding(
                as_int(&row[id_column])?,
                &row[slot_column],
                &row[generation_column],
            )
        })
        .collect::<Result<Vec<_>>>()?;
    exact_live_atom_bindings_batch(db, &bindings)
}

fn exact_live_atom_wrapped_rows(
    db: &Database,
    rows: &[Vec<Value>],
    id_column: usize,
    slot_column: usize,
    generation_column: usize,
) -> Result<Vec<Option<[u8; WRAPPED_KEY_SIZE]>>> {
    let bindings = rows
        .iter()
        .map(|row| {
            atom_key_binding(
                as_int(&row[id_column])?,
                &row[slot_column],
                &row[generation_column],
            )
        })
        .collect::<Result<Vec<_>>>()?;
    exact_live_atom_wrapped_batch(db, &bindings)
}

fn exact_live_atom_wrapped_for_ids(
    db: &Database,
    conn: &Connection<'_>,
    table: &str,
    region_id: RegionId,
    ids: &[AtomId],
) -> Result<FxHashMap<AtomId, [u8; WRAPPED_KEY_SIZE]>> {
    if ids.is_empty() {
        return Ok(FxHashMap::default());
    }
    let rows = conn.query_params(
        &format!(
            "SELECT id, key_slot, key_gen FROM {table} WHERE region_id = $1 AND id IN ({})",
            id_list(ids)
        ),
        &[Value::Integer(region_id)],
    )?;
    let wrapped = exact_live_atom_wrapped_rows(db, &rows.rows, 0, 1, 2)?;
    rows.rows
        .iter()
        .zip(wrapped)
        .filter_map(|(row, wrapped)| wrapped.map(|wrapped| (&row[0], wrapped)))
        .map(|(id, wrapped)| Ok((as_int(id)?, wrapped)))
        .collect()
}

/// Decrypt a sealed region's atoms (all, or only `id > after`) into
/// [`DecryptedAtom`]s, to (re)build the ANN index and exact-rank the tail.
fn decrypt_scan(
    conn: &Connection<'_>,
    db: &Database,
    atom_wrap: &AtomWrapKey,
    table: &str,
    region_id: RegionId,
    after: Option<i64>,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<DecryptedAtoms> {
    if conn.table_schema(table).is_none() {
        return Ok(DecryptedAtoms(Vec::new()));
    }
    let cols =
        "id, kind, sealed, score, confidence, created_at, immutable, expires_at, key_slot, key_gen";
    let (sql, params) = match after {
        Some(a) => (
            format!("SELECT {cols} FROM {table} WHERE region_id = $1 AND id > $2 ORDER BY id"),
            vec![Value::Integer(region_id), Value::Integer(a)],
        ),
        None => (
            format!("SELECT {cols} FROM {table} WHERE region_id = $1 ORDER BY id"),
            vec![Value::Integer(region_id)],
        ),
    };
    let qr = conn.query_params(&sql, &params)?;
    let wrapped = exact_live_atom_wrapped_rows(db, &qr.rows, 0, 8, 9)?;
    let mut out = DecryptedAtoms(Vec::with_capacity(qr.rows.len()));
    for (row, wrapped) in qr.rows.iter().zip(wrapped) {
        check_cancel(cancel)?;
        let id = as_int(&row[0])?;
        let kind = as_text(&row[1])?.to_string();
        let Some(wrapped) = wrapped else {
            continue;
        };
        let (emb, text, payload) = open_atom(atom_wrap, &wrapped, id, as_blob(&row[2])?)?;
        out.0.push((
            id,
            emb,
            kind,
            text,
            payload,
            as_f32(&row[3])?,
            as_f32(&row[4])?,
            as_ts(&row[5])?,
            as_bool(&row[6])?,
            opt_ts(&row[7])?,
        ));
    }
    check_cancel(cancel)?;
    Ok(out)
}

/// Sealed-segment ciphertext chunk size; only bounds per-value buffers
/// (storage chains pages anyway).
const SEALED_SEG_CHUNK: usize = 1024 * 1024;

/// Hidden chunk tree for one region's sealed segment. Per-region (the region is
/// the sealed lifecycle unit, so shared-table regions can't destroy each
/// other's segments), separate from the SQL layer's `__annseg_{table}` name.
fn sealed_segment_table(table: &str, region_id: RegionId) -> String {
    format!("__annseg_r{region_id}__{table}")
}

/// Inverse of [`sealed_segment_table`]; a loose prefix match could drop a foreign tree.
fn parse_sealed_segment_table(name: &str) -> Option<(RegionId, &str)> {
    let rest = name.strip_prefix("__annseg_r")?;
    let (rid, table) = rest.split_once("__")?;
    let rid: RegionId = rid.parse().ok()?;
    parse_encrypted_atoms_table(table)?;
    (sealed_segment_table(table, rid) == name).then_some((rid, table))
}

/// Cap the trusted chunk count so malformed metadata cannot drive unbounded work.
fn plausible_chunk_count(count: u32) -> bool {
    (1..=65_536).contains(&count)
}

/// Pseudo ids are row-less: an atom row with `id` anywhere marks metadata forged/stale.
fn atom_row_exists_anywhere(conn: &Connection<'_>, id: i64) -> Result<bool> {
    for table in conn.tables() {
        if !is_encrypted_atoms_table(&table) {
            continue;
        }
        let qr = conn.query_params(
            &format!("SELECT id FROM {table} WHERE id = $1"),
            &[Value::Integer(id)],
        )?;
        if !qr.rows.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Chunk-tree classification; only a well-formed `Present` tree validates a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentTreeState {
    Absent,
    Incomplete,
    Present,
}

/// One exactly-bound live sealed row delivered to a scan consumer. Returning
/// `false` stops delivery (the fingerprint still covers the remaining rows).
type SealedRowFn<'a> = dyn FnMut(
        AtomId,
        &str,
        &[u8],
        &[u8; WRAPPED_KEY_SIZE],
        f32,
        f32,
        i64,
        bool,
        Option<i64>,
    ) -> Result<bool>
    + 'a;

/// Fingerprint of every live row field materialized by a persisted sealed segment.
/// Dead rows contribute only their identity, ciphertext, and liveness state.
fn sealed_fp_scan(
    conn: &Connection<'_>,
    db: &Database,
    h: &RegionHandle,
    cancel: Option<&citadel_core::CancelToken>,
    live: &mut SealedRowFn<'_>,
) -> Result<([u8; 32], bool)> {
    let mut fp = blake3::Hasher::new();
    fp.update(b"citadel-annseg-sealed-fp-v3");
    fp.update(&h.id.to_le_bytes());
    fp.update(&h.dim.to_le_bytes());
    fp.update(&[citadel_vector::segment::metric_tag(ann_metric(h.metric))]);

    let qr = conn.query_params(
        &format!(
            "SELECT id, kind, sealed, score, confidence, created_at, immutable, expires_at, \
             key_slot, key_gen FROM {table} \
             WHERE region_id = $1 ORDER BY id",
            table = h.table
        ),
        &[Value::Integer(h.id)],
    )?;
    let wrapped = exact_live_atom_wrapped_rows(db, &qr.rows, 0, 8, 9)?;
    let mut completed = true;
    for (row, wrapped) in qr.rows.iter().zip(wrapped) {
        check_cancel(cancel)?;
        let id = as_int(&row[0])?;
        let sealed = as_blob(&row[2])?;
        let is_live = wrapped.is_some();
        fp.update(&id.to_le_bytes());
        fp.update(&(sealed.len() as u64).to_le_bytes());
        fp.update(sealed);
        fp.update(&[u8::from(is_live)]);
        if let Some(wrapped) = wrapped.as_ref() {
            let kind = as_text(&row[1])?;
            let score = as_f32(&row[3])?;
            let confidence = as_f32(&row[4])?;
            let created = as_ts(&row[5])?;
            let immutable = as_bool(&row[6])?;
            let expires = opt_ts(&row[7])?;
            fp.update(&(kind.len() as u64).to_le_bytes());
            fp.update(kind.as_bytes());
            fp.update(&score.to_bits().to_le_bytes());
            fp.update(&confidence.to_bits().to_le_bytes());
            fp.update(&created.to_le_bytes());
            fp.update(&[u8::from(immutable)]);
            match expires {
                Some(expires) => {
                    fp.update(&[1]);
                    fp.update(&expires.to_le_bytes());
                }
                None => {
                    fp.update(&[0]);
                }
            }
            if completed
                && !live(
                    id, kind, sealed, wrapped, score, confidence, created, immutable, expires,
                )?
            {
                // Keep hashing the remaining rows (the fingerprint must cover
                // the whole table) but stop delivering them.
                completed = false;
            }
        }
    }
    check_cancel(cancel)?;
    Ok((*fp.finalize().as_bytes(), completed))
}

/// Parse the sealed segment's inner plaintext: `[fp 32][config_hash
/// 32][kind_count u32][(len u32, kind, code u32)*][segment body]`.
#[allow(clippy::type_complexity)]
fn parse_sealed_segment(
    inner: &[u8],
    cancel: Option<&citadel_core::CancelToken>,
) -> std::result::Result<
    Option<(
        [u8; 32],
        [u8; 32],
        FxHashMap<String, u32>,
        citadel_vector::segment::SegmentParts,
    )>,
    citadel_vector::segment::SegmentOperationError,
> {
    macro_rules! malformed {
        ($value:expr) => {
            match $value {
                Some(value) => value,
                None => return Ok(None),
            }
        };
    }

    let check_cancel = || {
        if cancel.is_some_and(citadel_core::CancelToken::is_cancelled) {
            Err(citadel_vector::segment::SegmentOperationError::Interrupted)
        } else {
            Ok(())
        }
    };
    check_cancel()?;
    let mut at = 0usize;
    let take = |at: &mut usize, n: usize| -> Option<&[u8]> {
        let end = at.checked_add(n).filter(|&e| e <= inner.len())?;
        let s = &inner[*at..end];
        *at = end;
        Some(s)
    };
    let fp: [u8; 32] = malformed!(take(&mut at, 32).and_then(|bytes| bytes.try_into().ok()));
    let cfg: [u8; 32] = malformed!(take(&mut at, 32).and_then(|bytes| bytes.try_into().ok()));
    let count = u32::from_le_bytes(malformed!(
        take(&mut at, 4).and_then(|bytes| bytes.try_into().ok())
    )) as usize;
    if count == 0 || count > inner.len().saturating_sub(at) / 8 {
        return Ok(None);
    }
    let mut kind_codes = FxHashMap::default();
    for expected_code in 0..count {
        check_cancel()?;
        let len = u32::from_le_bytes(malformed!(
            take(&mut at, 4).and_then(|bytes| bytes.try_into().ok())
        )) as usize;
        let kind =
            malformed!(take(&mut at, len).and_then(|bytes| String::from_utf8(bytes.to_vec()).ok()));
        let code = u32::from_le_bytes(malformed!(
            take(&mut at, 4).and_then(|bytes| bytes.try_into().ok())
        ));
        if code != expected_code as u32 || kind_codes.insert(kind, code).is_some() {
            return Ok(None);
        }
    }
    check_cancel()?;
    #[cfg(test)]
    debug_fire_cancel_after_local_work();
    let parts = citadel_vector::segment::decode_with_cancel(&inner[at..], cancel)?;
    if !parts.attributes_fit_domains_with_cancel(&[kind_codes.len()], cancel)? {
        return Ok(None);
    }
    Ok(Some((fp, cfg, kind_codes, parts)))
}

fn annseg_meta_key(region_id: RegionId, field: &str) -> String {
    format!("annseg_{field}:{region_id}")
}

fn read_annseg_field(
    conn: &Connection<'_>,
    region_id: RegionId,
    field: &str,
) -> Result<Option<i64>> {
    let qr = conn.query_params(
        "SELECT value FROM memory_meta WHERE key = $1",
        &[Value::Text(annseg_meta_key(region_id, field).into())],
    )?;
    Ok(match qr.rows.first().map(|r| &r[0]) {
        Some(Value::Integer(v)) => Some(*v),
        _ => None,
    })
}

fn read_annseg_meta(conn: &Connection<'_>, region_id: RegionId) -> Result<Option<(u32, u64, u64)>> {
    let (Some(slot), Some(gen), Some(id)) = (
        read_annseg_field(conn, region_id, "slot")?,
        read_annseg_field(conn, region_id, "gen")?,
        read_annseg_field(conn, region_id, "id")?,
    ) else {
        return Ok(None);
    };
    let slot = u32::try_from(slot)
        .map_err(|_| MemError::Invalid("sealed ANN slot is out of range".into()))?;
    let generation = u64::try_from(gen)
        .map_err(|_| MemError::Invalid("sealed ANN key generation is out of range".into()))?;
    let owner =
        u64::try_from(id).map_err(|_| MemError::Invalid("sealed ANN id is out of range".into()))?;
    Ok(Some((slot, generation, owner)))
}

fn write_annseg_meta(
    conn: &Connection<'_>,
    region_id: RegionId,
    slot: u32,
    gen: u64,
    pseudo_id: i64,
) -> Result<()> {
    let generation = sql_key_generation(gen, "sealed ANN")?;
    if pseudo_id < 0 {
        return Err(MemError::Invalid(format!(
            "sealed ANN id {pseudo_id} is out of range"
        )));
    }
    with_write_txn(conn, |c| {
        for (field, value) in [
            ("slot", slot as i64),
            ("gen", generation),
            ("id", pseudo_id),
        ] {
            let key = annseg_meta_key(region_id, field);
            c.execute_params(
                "DELETE FROM memory_meta WHERE key = $1",
                &[Value::Text(key.clone().into())],
            )?;
            c.execute_params(
                "INSERT INTO memory_meta (key, value) VALUES ($1, $2)",
                &[Value::Text(key.into()), Value::Integer(value)],
            )?;
        }
        Ok(())
    })
}

fn sql_key_generation(generation: u64, owner: &str) -> Result<i64> {
    i64::try_from(generation).map_err(|_| {
        MemError::Invalid(format!(
            "{owner} key generation {generation} cannot be represented in SQL metadata"
        ))
    })
}

fn clear_annseg_meta(conn: &Connection<'_>, region_id: RegionId) -> Result<()> {
    let keys: Vec<Value> = ["slot", "gen", "id"]
        .into_iter()
        .map(|field| Value::Text(annseg_meta_key(region_id, field).into()))
        .collect();
    let statements: Vec<(&str, &[Value])> = keys
        .iter()
        .map(|key| {
            (
                "DELETE FROM memory_meta WHERE key = $1",
                std::slice::from_ref(key),
            )
        })
        .collect();
    conn.execute_params_batch_uncancelled_recovery(&statements)?;
    Ok(())
}

fn drop_segment_tree(db: &Database, segment_table: &str) -> Result<()> {
    let mut write = db.begin_write()?;
    write.set_cancel(None);
    match write.drop_table(segment_table.as_bytes()) {
        Ok(()) | Err(citadel_core::Error::TableNotFound(_)) => {}
        Err(error) => return Err(error.into()),
    }
    write.commit()?;
    Ok(())
}

/// Key-first retirement shared by operational and model-free maintenance paths.
fn retire_sealed_segment_parts(
    db: &Database,
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    lifecycle: &KeyLifecycleGuard<'_>,
) -> Result<()> {
    let Some((slot, generation, pseudo_owner)) = read_annseg_meta(conn, region_id)? else {
        return Ok(());
    };
    let record = db.atom_store_slot(slot)?;
    let pseudo_id = i64::try_from(pseudo_owner)
        .map_err(|_| MemError::Invalid("sealed ANN id is out of SQL range".into()))?;
    // A tombstone may still need torn-sibling normalization. A live key is destroyed only
    // when both its persisted binding and the absence of a user atom prove it is a segment.
    if record.state == SlotState::Tombstone
        || (record.state == SlotState::Live
            && record.region_id == pseudo_owner
            && record.gen == generation
            && !atom_row_exists_anywhere(conn, pseudo_id)?)
    {
        lifecycle.atom_store_tombstone(slot, pseudo_owner, generation)?;
        #[cfg(test)]
        debug_fire_cancel_after_segment_key_erasure();
    }
    drop_segment_tree(db, &sealed_segment_table(table, region_id))?;
    clear_annseg_meta(conn, region_id)
}

/// Open one sealed atom: unwrap its ACK (wrapped under the region atom-wrap
/// key) and decrypt the blob. The ACK is zeroized before returning.
fn open_atom(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    sealed: &[u8],
) -> Result<(Vec<f32>, String, serde_json::Value)> {
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    ack.zeroize();
    let blob = Zeroizing::new(blob_seal::open(&seal_keys, id as u64, sealed)?);
    let (emb, text, payload_json) = decode_atom_blob(&blob)?;
    let payload_json = Zeroizing::new(payload_json);
    let payload = serde_json::from_str(&payload_json).unwrap_or(serde_json::Value::Null);
    Ok((emb, text, payload))
}

/// Open only text+payload; the embedding is never materialized as a `Vec<f32>`.
fn open_atom_content(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    sealed: &[u8],
) -> Result<(String, serde_json::Value)> {
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    ack.zeroize();
    let blob = Zeroizing::new(blob_seal::open(&seal_keys, id as u64, sealed)?);
    let (text, payload_json) = decode_atom_content(&blob)?;
    let payload_json = Zeroizing::new(payload_json);
    let payload = serde_json::from_str(&payload_json).unwrap_or(serde_json::Value::Null);
    Ok((text, payload))
}

/// Open only the vector; the RAII zeroizer scrubs the blob on every return path.
fn open_atom_embedding(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    sealed: &[u8],
) -> Result<Vec<f32>> {
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    ack.zeroize();
    let blob = Zeroizing::new(blob_seal::open(&seal_keys, id as u64, sealed)?);
    decode_atom_embedding(&blob)
}

/// Open one MMR candidate vector under the operation's mandatory work cap.
/// The authenticated framing dimension is checked before the second vector
/// allocation, and both decrypted-blob and vector storage are accounted.
fn open_mmr_embedding(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    sealed: &[u8],
    expected_dim: usize,
) -> Result<Zeroizing<Vec<f32>>> {
    charge_materialized_bytes(sealed.len())?;
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    ack.zeroize();
    let blob = Zeroizing::new(blob_seal::open(&seal_keys, id as u64, sealed)?);
    let parts = parse_atom_blob(&blob)?;
    if parts.dim != expected_dim {
        return Err(MemError::Invalid(format!(
            "stored embedding for atom {id} has dim {} != region dim {expected_dim}",
            parts.dim
        )));
    }
    charge_materialized_bytes(parts.embedding.len())?;
    Ok(Zeroizing::new(decode_embedding(&parts)))
}

/// Open only the text for sealed dedup; other fields stay in the zeroized blob.
fn open_atom_text(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    sealed: &[u8],
) -> Result<String> {
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    ack.zeroize();
    let blob = Zeroizing::new(blob_seal::open(&seal_keys, id as u64, sealed)?);
    decode_atom_text(&blob)
}

/// Seal one atom under a fresh random ACK, returning `(sealed_blob,
/// wrapped_ack)`. The wrapped ACK is the sole copy; store it (fsync'd) before
/// the atom row commits.
fn seal_atom(
    atom_wrap: &AtomWrapKey,
    id: AtomId,
    embedding: &[f32],
    text: &str,
    payload_json: &str,
) -> (Vec<u8>, [u8; WRAPPED_KEY_SIZE]) {
    use rand::RngCore;
    let mut ack = [0u8; citadel_core::KEY_SIZE];
    rand::thread_rng().fill_bytes(&mut ack);
    let seal_keys = derive_seal_keys(&ack);
    let blob = Zeroizing::new(encode_atom_blob(embedding, text, payload_json));
    let sealed = blob_seal::seal(&seal_keys, id as u64, &blob);
    let wrapped = atom_wrap.wrap_atom_key(&ack);
    ack.zeroize();
    (sealed, wrapped)
}

/// Reseal an atom's content under the key it already has.
///
/// [`seal_atom`] mints a fresh ACK, which would leave the atom's `key_slot` and
/// `key_gen` pointing at the old one and require retiring it. Re-embedding
/// changes only the vector inside the blob, so keeping the same key keeps the
/// key store untouched: nothing to allocate, nothing to tombstone, and no
/// window where an atom's row and its key disagree.
fn reseal_atom(
    atom_wrap: &AtomWrapKey,
    wrapped: &[u8; WRAPPED_KEY_SIZE],
    id: AtomId,
    embedding: &[f32],
    text: &str,
    payload_json: &str,
) -> Result<Vec<u8>> {
    let mut ack = atom_wrap.unwrap_atom_key(wrapped)?;
    let seal_keys = derive_seal_keys(&ack);
    let blob = Zeroizing::new(encode_atom_blob(embedding, text, payload_json));
    let sealed = blob_seal::seal(&seal_keys, id as u64, &blob);
    ack.zeroize();
    Ok(sealed)
}

/// Distance between two vectors, matching citadel-sql's `<->`/`<#>`/`<=>` so
/// sealed decrypt-then-rank recall matches the plaintext index path.
fn vec_distance(metric: EmbeddingMetric, a: &[f32], b: &[f32]) -> Option<f32> {
    match metric {
        EmbeddingMetric::L2 => {
            let mut sum = 0.0f64;
            for (x, y) in a.iter().zip(b.iter()) {
                let d = *x as f64 - *y as f64;
                sum += d * d;
            }
            Some(sum.sqrt() as f32)
        }
        EmbeddingMetric::InnerProduct => {
            let mut sum = 0.0f64;
            for (x, y) in a.iter().zip(b.iter()) {
                sum += *x as f64 * *y as f64;
            }
            Some((-sum) as f32)
        }
        EmbeddingMetric::Cosine => {
            let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
            for (x, y) in a.iter().zip(b.iter()) {
                let (xf, yf) = (*x as f64, *y as f64);
                dot += xf * yf;
                na += xf * xf;
                nb += yf * yf;
            }
            let denom = na.sqrt() * nb.sqrt();
            if denom == 0.0 {
                None
            } else {
                Some((1.0 - dot / denom) as f32)
            }
        }
    }
}

/// Language-agnostic word tokens (UAX#29 boundaries, lowercased); spaceless
/// scripts (CJK/Thai) fall back to per-character tokens.
fn tokenize(text: &str) -> Vec<String> {
    use unicode_segmentation::UnicodeSegmentation;
    text.unicode_words().map(str::to_lowercase).collect()
}

/// Distinct query tokens for the BM25 keyword signal (UAX#29, lowercased,
/// deduped).
fn query_keyword_terms(text: Option<&str>) -> Vec<String> {
    let Some(t) = text else {
        return Vec::new();
    };
    let mut v = tokenize(t);
    v.sort();
    v.dedup();
    v
}

/// Set each candidate's `text_rank` to its Okapi BM25 score for `query_terms`,
/// with the candidate pool as the corpus. IDF down-weights pool-common terms,
/// so no stoplist or stemmer is needed.
fn assign_bm25_ranks(
    cands: &mut [Candidate],
    query_terms: &[String],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<()> {
    if query_terms.is_empty() || cands.is_empty() {
        return Ok(());
    }
    const K1: f32 = 1.2;
    const B: f32 = 0.75;
    let n = cands.len() as f32;
    // Tokenize each candidate once: per-term frequency + document length.
    let mut docs: Vec<(FxHashMap<String, u32>, f32)> = Vec::with_capacity(cands.len());
    for candidate in cands.iter() {
        check_cancel(cancel)?;
        let mut tf: FxHashMap<String, u32> = FxHashMap::default();
        let mut len = 0u32;
        for tok in tokenize(&candidate.text) {
            *tf.entry(tok).or_insert(0) += 1;
            len += 1;
        }
        docs.push((tf, len as f32));
    }
    let avgdl = (docs.iter().map(|(_, l)| *l).sum::<f32>() / n).max(1.0);
    // IDF per query term over the pool (Lucene's +1 form, never negative).
    let mut idf = Vec::with_capacity(query_terms.len());
    for term in query_terms {
        check_cancel(cancel)?;
        let df = docs.iter().filter(|(tf, _)| tf.contains_key(term)).count() as f32;
        idf.push(((n - df + 0.5) / (df + 0.5) + 1.0).ln());
    }
    for (c, (tf, dl)) in cands.iter_mut().zip(&docs) {
        check_cancel(cancel)?;
        let mut score = 0.0;
        for (t, &w) in query_terms.iter().zip(&idf) {
            let f = tf.get(t).copied().unwrap_or(0) as f32;
            if f > 0.0 {
                score += w * (f * (K1 + 1.0)) / (f + K1 * (1.0 - B + B * dl / avgdl));
            }
        }
        c.text_rank = score;
    }
    check_cancel(cancel)?;
    Ok(())
}

/// JSONB `@>` containment: every member of `needle` is present in `haystack`.
fn json_contains(haystack: &serde_json::Value, needle: &serde_json::Value) -> bool {
    use serde_json::Value as J;
    match (haystack, needle) {
        (J::Object(h), J::Object(n)) => n
            .iter()
            .all(|(k, nv)| h.get(k).is_some_and(|hv| json_contains(hv, nv))),
        (J::Array(h), J::Array(n)) => n.iter().all(|ne| h.iter().any(|he| json_contains(he, ne))),
        (J::Array(h), ne) => h.iter().any(|he| json_contains(he, ne)),
        (a, b) => a == b,
    }
}

fn maintenance_count_sealed(
    db: &Database,
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    now: i64,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<u64> {
    let sql = format!(
        "SELECT id, key_slot, key_gen FROM {table} \
         WHERE region_id = $1 AND (expires_at IS NULL OR expires_at > $2) AND id > $3 \
         ORDER BY id LIMIT {EXACT_SCAN_LIMIT}"
    );
    let mut cursor = i64::MIN;
    let mut count = 0u64;
    loop {
        check_cancel(cancel)?;
        let qr = conn.query_params(
            &sql,
            &[
                Value::Integer(region_id),
                Value::Timestamp(now),
                Value::Integer(cursor),
            ],
        )?;
        if qr.rows.is_empty() {
            break;
        }
        let page_len = qr.rows.len();
        cursor = as_int(&qr.rows[page_len - 1][0])?;
        for is_live in exact_live_atom_binding_rows(db, &qr.rows, 0, 1, 2)? {
            check_cancel(cancel)?;
            count = count
                .checked_add(u64::from(is_live))
                .ok_or_else(|| MemError::Invalid("live atom count overflow".into()))?;
        }
        if page_len < EXACT_SCAN_LIMIT {
            break;
        }
    }
    Ok(count)
}

fn maintenance_fetch_plain(
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    query: &FetchQuery,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomHit>> {
    let mut params = vec![Value::Integer(region_id)];
    let mut predicates = String::new();
    if let Some(kind) = &query.kind {
        params.push(Value::Text(kind.as_str().into()));
        predicates += &format!(" AND kind = ${}", params.len());
    }
    if let Some(filter) = &query.payload_filter {
        let json = serde_json::to_string(filter).map_err(|error| {
            MemError::Invalid(format!("payload_filter not serializable: {error}"))
        })?;
        params.push(Value::Text(json.into()));
        predicates += &format!(" AND payload @> CAST(${} AS JSONB)", params.len());
    }
    if let Some(after) = query.after_id {
        params.push(Value::Integer(after));
        predicates += &format!(" AND id > ${}", params.len());
    }
    if let Some(from) = query.created_from {
        params.push(Value::Timestamp(from));
        predicates += &format!(" AND created_at >= ${}", params.len());
    }
    if let Some(before) = query.created_before {
        params.push(Value::Timestamp(before));
        predicates += &format!(" AND created_at < ${}", params.len());
    }
    params.push(Value::Timestamp(now_micros()));
    predicates += &format!(
        " AND (expires_at IS NULL OR expires_at > ${})",
        params.len()
    );
    let qr = conn.query_params(
        &format!(
            "SELECT id, kind, CAST(payload AS TEXT), text_content, score, confidence, immutable, \
             created_at, expires_at \
             FROM {table} WHERE region_id = $1{predicates} ORDER BY id {direction} LIMIT {limit}",
            direction = if query.newest { "DESC" } else { "ASC" },
            limit = query.limit,
        ),
        &params,
    )?;
    let mut hits = Vec::with_capacity(qr.rows.len());
    for row in &qr.rows {
        #[cfg(test)]
        debug_fire_cancel_after_local_work();
        check_cancel(cancel)?;
        hits.push(parse_fetched(row)?);
    }
    if query.newest {
        hits.reverse();
    }
    Ok(hits)
}

fn validate_fetch_ids(ids: &[AtomId]) -> Result<()> {
    let mut unique = FxHashSet::default();
    for &id in ids {
        if !unique.insert(id) {
            return Err(MemError::Invalid(format!(
                "atom ids must be unique; duplicate id {id}"
            )));
        }
    }
    Ok(())
}

fn order_fetched_ids(
    ids: &[AtomId],
    found: &mut FxHashMap<AtomId, AtomHit>,
) -> Vec<Option<AtomHit>> {
    ids.iter().map(|id| found.remove(id)).collect()
}

fn fetch_atoms_by_ids_plain(
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    ids: &[AtomId],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<FxHashMap<AtomId, AtomHit>> {
    let mut found = FxHashMap::default();
    let now = now_micros();
    for batch in ids.chunks(EXACT_SCAN_LIMIT) {
        check_cancel(cancel)?;
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, CAST(payload AS TEXT), text_content, score, confidence, immutable, \
                 created_at, expires_at \
                 FROM {table} WHERE region_id = $1 AND id IN ({ids}) \
                 AND (expires_at IS NULL OR expires_at > $2)",
                table = table,
                ids = id_list(batch),
            ),
            &[Value::Integer(region_id), Value::Timestamp(now)],
        )?;
        for row in &qr.rows {
            check_cancel(cancel)?;
            let hit = parse_fetched(row)?;
            found.insert(hit.id, hit);
        }
    }
    check_cancel(cancel)?;
    Ok(found)
}

fn fetch_atoms_by_ids_sealed(
    db: &Database,
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    atom_wrap: &AtomWrapKey,
    ids: &[AtomId],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<FxHashMap<AtomId, AtomHit>> {
    let mut found = FxHashMap::default();
    let now = now_micros();
    for batch in ids.chunks(EXACT_SCAN_LIMIT) {
        check_cancel(cancel)?;
        let qr = conn.query_params(
            &format!(
                "SELECT id, kind, sealed, score, confidence, immutable, created_at, expires_at, \
                 key_slot, key_gen \
                 FROM {table} WHERE region_id = $1 AND id IN ({ids}) \
                 AND (expires_at IS NULL OR expires_at > $2)",
                table = table,
                ids = id_list(batch),
            ),
            &[Value::Integer(region_id), Value::Timestamp(now)],
        )?;
        let wrapped = exact_live_atom_wrapped_rows(db, &qr.rows, 0, 8, 9)?;
        for (row, wrapped) in qr.rows.iter().zip(wrapped) {
            check_cancel(cancel)?;
            let id = as_int(&row[0])?;
            let Some(wrapped) = wrapped else {
                continue;
            };
            let kind = as_text(&row[1])?.to_owned();
            let (mut text, mut payload) =
                open_atom_content(atom_wrap, &wrapped, id, as_blob(&row[2])?)?;
            charge_owned_atom_content(&kind, &mut text, &mut payload)?;
            found.insert(
                id,
                AtomHit {
                    id,
                    kind,
                    payload,
                    text,
                    importance: as_f32(&row[3])?,
                    confidence: as_f32(&row[4])?,
                    relevance: None,
                    distance: None,
                    graph_depth: None,
                    created_at: as_ts(&row[6])?,
                    expires_at: opt_ts(&row[7])?,
                    immutable: as_bool(&row[5])?,
                },
            );
        }
    }
    check_cancel(cancel)?;
    Ok(found)
}

fn fetch_mmr_embeddings_plain(
    conn: &Connection<'_>,
    region: &str,
    region_id: RegionId,
    table: &str,
    expected_dim: u16,
    ids: &[AtomId],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<FxHashMap<AtomId, Zeroizing<Vec<f32>>>> {
    let mut found = FxHashMap::default();
    let now = now_micros();
    for batch in ids.chunks(EXACT_SCAN_LIMIT) {
        check_cancel(cancel)?;
        let qr = with_storage_read_cap(MAX_MMR_VECTOR_BYTES, MAX_MMR_VECTOR_BYTES, || {
            Ok(conn.query_params(
                &format!(
                    "SELECT id, embedding FROM {table} WHERE region_id = $1 AND id IN ({ids}) \
                     AND (expires_at IS NULL OR expires_at > $2)",
                    ids = id_list(batch),
                ),
                &[Value::Integer(region_id), Value::Timestamp(now)],
            )?)
        })?;
        for row in &qr.rows {
            check_cancel(cancel)?;
            let id = as_int(&row[0])?;
            let Value::Vector(vector) = &row[1] else {
                return Err(MemError::Invalid(format!(
                    "stored embedding for atom {id} is not a vector: {:?}",
                    row[1]
                )));
            };
            validate_embedding(region, expected_dim, vector, "stored")?;
            charge_materialized_bytes(embedding_bytes(vector.len())?)?;
            found.insert(id, Zeroizing::new(vector.to_vec()));
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel)?;
        }
    }
    check_cancel(cancel)?;
    Ok(found)
}

#[allow(clippy::too_many_arguments)]
fn fetch_mmr_embeddings_sealed(
    db: &Database,
    conn: &Connection<'_>,
    region: &str,
    region_id: RegionId,
    table: &str,
    expected_dim: u16,
    atom_wrap: &AtomWrapKey,
    ids: &[AtomId],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<FxHashMap<AtomId, Zeroizing<Vec<f32>>>> {
    let mut found = FxHashMap::default();
    let now = now_micros();
    for batch in ids.chunks(EXACT_SCAN_LIMIT) {
        check_cancel(cancel)?;
        let qr = with_storage_read_cap(
            MAX_MMR_SEALED_VALUE_BYTES,
            MAX_MMR_SEALED_TOTAL_BYTES,
            || {
                Ok(conn.query_params(
                    &format!(
                        "SELECT id, sealed, key_slot, key_gen FROM {table} \
                         WHERE region_id = $1 AND id IN ({ids}) \
                         AND (expires_at IS NULL OR expires_at > $2)",
                        ids = id_list(batch),
                    ),
                    &[Value::Integer(region_id), Value::Timestamp(now)],
                )?)
            },
        )?;
        let wrapped = exact_live_atom_wrapped_rows(db, &qr.rows, 0, 2, 3)?;
        for (row, wrapped) in qr.rows.iter().zip(wrapped) {
            check_cancel(cancel)?;
            let id = as_int(&row[0])?;
            let Some(wrapped) = wrapped else {
                continue;
            };
            let sealed = as_blob(&row[1])?;
            let embedding =
                open_mmr_embedding(atom_wrap, &wrapped, id, sealed, usize::from(expected_dim))?;
            validate_embedding(region, expected_dim, &embedding, "stored")?;
            found.insert(id, embedding);
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel)?;
        }
    }
    check_cancel(cancel)?;
    Ok(found)
}

#[allow(clippy::too_many_arguments)]
fn maintenance_fetch_sealed(
    db: &Database,
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    atom_wrap: &AtomWrapKey,
    query: &FetchQuery,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomHit>> {
    let mut params = vec![Value::Integer(region_id)];
    let mut predicates = String::new();
    if let Some(kind) = &query.kind {
        params.push(Value::Text(kind.as_str().into()));
        predicates += &format!(" AND kind = ${}", params.len());
    }
    if let Some(from) = query.created_from {
        params.push(Value::Timestamp(from));
        predicates += &format!(" AND created_at >= ${}", params.len());
    }
    if let Some(before) = query.created_before {
        params.push(Value::Timestamp(before));
        predicates += &format!(" AND created_at < ${}", params.len());
    }
    params.push(Value::Timestamp(now_micros()));
    predicates += &format!(
        " AND (expires_at IS NULL OR expires_at > ${})",
        params.len()
    );
    if query.newest {
        if let Some(after) = query.after_id {
            params.push(Value::Integer(after));
            predicates += &format!(" AND id > ${}", params.len());
        }
    }
    let cursor_parameter = params.len() + 1;
    let (comparison, direction) = if query.newest {
        ("<", "DESC")
    } else {
        (">", "ASC")
    };
    let mut hits = Vec::new();
    let mut cursor = if query.newest {
        i64::MAX
    } else {
        query.after_id.unwrap_or(i64::MIN)
    };
    'pages: loop {
        check_cancel(cancel)?;
        let remaining = query.limit - hits.len();
        let page_limit = if query.payload_filter.is_some() {
            EXACT_SCAN_LIMIT
        } else {
            remaining.min(EXACT_SCAN_LIMIT)
        };
        let sql = format!(
            "SELECT id, kind, sealed, score, confidence, immutable, created_at, expires_at, \
             key_slot, key_gen \
             FROM {table} \
             WHERE region_id = $1{predicates} AND id {comparison} ${cursor_parameter} \
             ORDER BY id {direction} LIMIT {page_limit}"
        );
        let mut page_params = params.clone();
        page_params.push(Value::Integer(cursor));
        let qr = conn.query_params(&sql, &page_params)?;
        if qr.rows.is_empty() {
            break;
        }
        let wrapped = maintenance_live_atom_wrapped_rows(db, &qr.rows, 0, 8, 9)?;
        let page_len = qr.rows.len();
        for (row, wrapped_key) in qr.rows.iter().zip(wrapped) {
            check_cancel(cancel)?;
            let id = as_int(&row[0])?;
            cursor = id;
            let Some(wrapped_key) = wrapped_key? else {
                continue;
            };
            let (mut text, mut payload) =
                open_atom_content(atom_wrap, &wrapped_key, id, as_blob(&row[2])?)?;
            if query
                .payload_filter
                .as_ref()
                .is_some_and(|filter| !json_contains(&payload, filter))
            {
                zeroize_atom_content(&mut text, &mut payload);
                continue;
            }
            let kind = as_text(&row[1])?.to_owned();
            charge_owned_atom_content(&kind, &mut text, &mut payload)?;
            hits.push(AtomHit {
                id,
                kind,
                payload,
                text,
                importance: as_f32(&row[3])?,
                confidence: as_f32(&row[4])?,
                relevance: None,
                distance: None,
                graph_depth: None,
                created_at: as_ts(&row[6])?,
                expires_at: opt_ts(&row[7])?,
                immutable: as_bool(&row[5])?,
            });
            if hits.len() >= query.limit {
                break 'pages;
            }
        }
        if page_len < page_limit {
            break;
        }
    }
    if query.newest {
        hits.reverse();
    }
    Ok(hits)
}

fn maintenance_verify_plain(
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    ids: &[AtomId],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomAttestation>> {
    let mut attestations = Vec::with_capacity(ids.len());
    for batch in ids.chunks(EXACT_SCAN_LIMIT) {
        check_cancel(cancel)?;
        let in_list = id_list(batch);
        let qr = conn.query_params(
            &format!("SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list})"),
            &[Value::Integer(region_id)],
        )?;
        let mut present = FxHashSet::default();
        for row in &qr.rows {
            check_cancel(cancel)?;
            present.insert(as_int(&row[0])?);
        }
        for &atom_id in batch {
            check_cancel(cancel)?;
            attestations.push(AtomAttestation {
                atom_id,
                verdict: if present.contains(&atom_id) {
                    AttestVerdict::PlaintextUnattested
                } else {
                    AttestVerdict::Missing
                },
                aad_bound: false,
                key_slot: None,
                key_gen: None,
            });
        }
    }
    Ok(attestations)
}

#[allow(clippy::too_many_arguments)]
fn maintenance_verify_sealed(
    db: &Database,
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    atom_wrap: &AtomWrapKey,
    ids: &[AtomId],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomAttestation>> {
    let mut attestations = Vec::with_capacity(ids.len());
    for batch in ids.chunks(EXACT_SCAN_LIMIT) {
        check_cancel(cancel)?;
        let in_list = id_list(batch);
        let qr = conn.query_params(
            &format!(
                "SELECT id, key_slot, sealed, key_gen FROM {table} \
                 WHERE region_id = $1 AND id IN ({in_list})"
            ),
            &[Value::Integer(region_id)],
        )?;
        let mut found = FxHashMap::default();
        for row in qr.rows {
            check_cancel(cancel)?;
            let atom_id = as_int(&row[0])?;
            let binding = atom_key_binding(atom_id, &row[1], &row[3]);
            let record = match (binding, row.into_iter().nth(2)) {
                (Ok(binding), Some(Value::Blob(sealed))) => Ok((binding, sealed)),
                _ => Err(()),
            };
            found.insert(atom_id, record);
        }
        let slots: Vec<u32> = found
            .values()
            .filter_map(|record| record.as_ref().ok().map(|(binding, _)| binding.slot))
            .collect();
        let live_records = match db.atom_store_slot_results(&slots) {
            Ok(records) => Some(
                slots
                    .into_iter()
                    .zip(records.into_iter().map(|record| record.map_err(|_| ())))
                    .collect::<FxHashMap<_, _>>(),
            ),
            Err(citadel_core::Error::Io(source))
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                None
            }
            Err(error) => return Err(error.into()),
        };
        for &atom_id in batch {
            check_cancel(cancel)?;
            let Some(record) = found.get(&atom_id) else {
                attestations.push(AtomAttestation {
                    atom_id,
                    verdict: AttestVerdict::Missing,
                    aad_bound: false,
                    key_slot: None,
                    key_gen: None,
                });
                continue;
            };
            let Ok((binding, sealed)) = record else {
                attestations.push(AtomAttestation {
                    atom_id,
                    verdict: AttestVerdict::Tampered,
                    aad_bound: false,
                    key_slot: None,
                    key_gen: None,
                });
                continue;
            };
            let Some(records) = live_records.as_ref() else {
                attestations.push(AtomAttestation {
                    atom_id,
                    verdict: AttestVerdict::KeyErased,
                    aad_bound: false,
                    key_slot: Some(binding.slot),
                    key_gen: None,
                });
                continue;
            };
            let record = records
                .get(&binding.slot)
                .expect("present key-store slots were read as one batch");
            let Ok(record) = record else {
                attestations.push(AtomAttestation {
                    atom_id,
                    verdict: AttestVerdict::Tampered,
                    aad_bound: false,
                    key_slot: Some(binding.slot),
                    key_gen: None,
                });
                continue;
            };
            if record.state != SlotState::Live
                || record.region_id != binding.atom_id
                || record.gen != binding.generation
            {
                attestations.push(AtomAttestation {
                    atom_id,
                    verdict: AttestVerdict::KeyErased,
                    aad_bound: false,
                    key_slot: Some(binding.slot),
                    key_gen: Some(record.gen),
                });
                continue;
            }
            let (verdict, aad_bound) = match atom_wrap.unwrap_atom_key(&record.wrapped) {
                Ok(mut atom_key) => {
                    let seal_keys = derive_seal_keys(&atom_key);
                    atom_key.zeroize();
                    match blob_seal::open(&seal_keys, binding.atom_id, sealed) {
                        Ok(mut plaintext) => {
                            plaintext.zeroize();
                            (AttestVerdict::Authentic, true)
                        }
                        Err(_) => (AttestVerdict::Tampered, true),
                    }
                }
                Err(_) => (AttestVerdict::Tampered, false),
            };
            attestations.push(AtomAttestation {
                atom_id,
                verdict,
                aad_bound,
                key_slot: Some(binding.slot),
                key_gen: Some(record.gen),
            });
        }
    }
    Ok(attestations)
}

fn as_blob(v: &Value) -> Result<&[u8]> {
    match v {
        Value::Blob(b) => Ok(b.as_slice()),
        other => Err(MemError::Invalid(format!("expected blob, got {other:?}"))),
    }
}

/// Parse an optional `INTEGER` slot index (`NULL` -> `None`).
fn opt_u32(v: &Value) -> Result<Option<u32>> {
    match v {
        Value::Null => Ok(None),
        Value::Integer(i) => u32::try_from(*i)
            .map(Some)
            .map_err(|_| MemError::Invalid("rsk_slot out of range".into())),
        other => Err(MemError::Invalid(format!(
            "expected integer rsk_slot, got {other:?}"
        ))),
    }
}

/// Parse an optional `INTEGER` generation (`NULL` -> `None`).
fn opt_u64(v: &Value) -> Result<Option<u64>> {
    match v {
        Value::Null => Ok(None),
        Value::Integer(i) => u64::try_from(*i)
            .map(Some)
            .map_err(|_| MemError::Invalid("key generation out of range".into())),
        other => Err(MemError::Invalid(format!(
            "expected integer key generation, got {other:?}"
        ))),
    }
}

#[derive(PartialEq, Eq)]
struct RegionRow {
    id: RegionId,
    dim: u16,
    metric: EmbeddingMetric,
    model_id: String,
    encrypted: bool,
    rsk_slot: Option<u32>,
    rsk_gen: Option<u64>,
}

fn parse_region_row(row: &[Value]) -> Result<RegionRow> {
    if row.len() < 7 {
        return Err(MemError::Invalid(format!(
            "region row has {} columns, expected 7",
            row.len()
        )));
    }
    let mut stored = parse_region_identity(row)?;
    stored.rsk_slot = opt_u32(&row[5])?;
    stored.rsk_gen = opt_u64(&row[6])?;
    Ok(stored)
}

fn parse_inventory_region_row(row: &[Value]) -> Result<(RegionRow, Option<String>)> {
    if row.len() < 7 {
        return Err(MemError::Invalid(format!(
            "region row has {} columns, expected 7",
            row.len()
        )));
    }
    let mut stored = parse_region_identity(row)?;
    if !stored.encrypted {
        return Ok((stored, None));
    }
    match (opt_u32(&row[5]), opt_u64(&row[6])) {
        (Ok(slot), Ok(generation)) => {
            stored.rsk_slot = slot;
            stored.rsk_gen = generation;
            Ok((stored, None))
        }
        (slot, generation) => {
            let detail = slot
                .err()
                .or_else(|| generation.err())
                .expect("one region-key binding field failed to parse");
            Ok((stored, Some(detail.to_string())))
        }
    }
}

fn parse_region_identity(row: &[Value]) -> Result<RegionRow> {
    let id = as_int(&row[0])?;
    u64::try_from(id)
        .map_err(|_| MemError::Invalid(format!("stored region id {id} is out of range")))?;
    let dim = u16::try_from(as_int(&row[1])?)
        .map_err(|_| MemError::Invalid("stored embedding_dim out of range".into()))?;
    Ok(RegionRow {
        id,
        dim,
        metric: metric_from_str(as_text(&row[2])?)?,
        model_id: as_text(&row[3])?.to_owned(),
        encrypted: as_exact_bool(&row[4], "encrypted")?,
        rsk_slot: None,
        rsk_gen: None,
    })
}

impl RegionRow {
    fn verify_matches(
        &self,
        region: &str,
        dim: u16,
        metric: EmbeddingMetric,
        model_id: &str,
        encrypted: bool,
    ) -> Result<()> {
        if self.dim != dim {
            return Err(MemError::DimMismatch {
                region: region.into(),
                expected: self.dim,
                got: dim as usize,
            });
        }
        if self.metric != metric {
            return Err(MemError::MetricMismatch {
                region: region.into(),
                expected: metric_tag(self.metric).into(),
                got: metric_tag(metric).into(),
            });
        }
        if self.model_id != model_id {
            return Err(MemError::ModelMismatch {
                region: region.into(),
                expected: self.model_id.clone(),
                got: model_id.into(),
            });
        }
        if self.encrypted != encrypted {
            return Err(MemError::Invalid(format!(
                "region '{region}' exists with encrypted={}, requested encrypted={encrypted}",
                self.encrypted
            )));
        }
        Ok(())
    }
}

fn load_region_row(conn: &Connection<'_>, key: &str) -> Result<Option<RegionRow>> {
    let qr = conn.query_params(
        "SELECT id, embedding_dim, embedding_metric, model_id, encrypted, rsk_slot, rsk_gen \
         FROM memory_regions WHERE name = $1",
        &[Value::Text(key.into())],
    )?;
    qr.rows.first().map(|row| parse_region_row(row)).transpose()
}

fn load_maintenance_atom_wrap(
    db: &Database,
    name: &str,
    stored: &RegionRow,
) -> Result<AtomWrapKey> {
    let wrapped = verify_maintenance_region_key(db, name, stored)?;
    let mut region_key = db.unwrap_region_key(&wrapped)?;
    let atom_wrap = derive_atom_wrap_key(&region_key);
    region_key.zeroize();
    Ok(atom_wrap)
}

fn verify_maintenance_region_key(
    db: &Database,
    name: &str,
    stored: &RegionRow,
) -> Result<[u8; WRAPPED_KEY_SIZE]> {
    let slot = stored
        .rsk_slot
        .ok_or_else(|| MemError::RegionForgotten(name.into()))?;
    let generation = stored
        .rsk_gen
        .ok_or_else(|| MemError::RegionForgotten(name.into()))?;
    let record = match db.region_store_slot(slot) {
        Ok(record) => record,
        Err(citadel_core::Error::Io(source)) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(MemError::RegionForgotten(name.into()))
        }
        Err(error) => return Err(error.into()),
    };
    if record.state != SlotState::Live
        || record.region_id != stored.id as u64
        || record.gen != generation
    {
        return Err(MemError::RegionForgotten(name.into()));
    }
    Ok(record.wrapped)
}

fn verify_maintenance_region_key_from_snapshot(
    name: &str,
    stored: &RegionRow,
    snapshot: &RegionKeyInventorySnapshot,
) -> Result<[u8; WRAPPED_KEY_SIZE]> {
    let slot = stored
        .rsk_slot
        .ok_or_else(|| MemError::RegionForgotten(name.into()))?;
    let generation = stored
        .rsk_gen
        .ok_or_else(|| MemError::RegionForgotten(name.into()))?;
    let record = match snapshot {
        RegionKeyInventorySnapshot::Available(records) => match records.get(&slot) {
            Some(Ok(record)) => record,
            Some(Err(error)) => return Err(MemError::Invalid(error.clone())),
            None => return Err(MemError::RegionForgotten(name.into())),
        },
        RegionKeyInventorySnapshot::Missing => return Err(MemError::RegionForgotten(name.into())),
        RegionKeyInventorySnapshot::Invalid(error) => return Err(MemError::Invalid(error.clone())),
        RegionKeyInventorySnapshot::NotRequired => {
            return Err(MemError::Invalid(
                "region key inventory is unavailable for an encrypted region".into(),
            ))
        }
    };
    if record.state != SlotState::Live
        || record.region_id != stored.id as u64
        || record.gen != generation
    {
        return Err(MemError::RegionForgotten(name.into()));
    }
    Ok(record.wrapped)
}

/// Atoms table for a (dim, metric) pair (`region_id` isolates regions).
/// Encrypted regions use a distinct `_enc` table of sealed content.
pub(crate) fn atoms_table(dim: u16, metric: EmbeddingMetric, encrypted: bool) -> String {
    let suffix = if encrypted { "_enc" } else { "" };
    format!("memory_atoms_d{}_{}{}", dim, metric_tag(metric), suffix)
}

/// Whether `name` is an encrypted atoms table produced by [`atoms_table`].
fn is_encrypted_atoms_table(name: &str) -> bool {
    parse_encrypted_atoms_table(name).is_some()
}

/// The fixed tables the memory schema creates, beside the per-shape atoms tables.
const OWNED_TABLES: [&str; 6] = [
    "memory_meta",
    "memory_regions",
    "memory_edges",
    "memory_similarity_policies",
    "memory_similarity_edges",
    "memory_idempotency",
];

/// Whether this crate creates and owns `name`.
///
/// For a caller listing a vault's tables to a person: these are storage behind the memory
/// API, not tables anyone wrote, and presenting them as the latter offers sealed blobs to
/// browse and an atoms table's vectors as if they were a column someone chose.
///
/// A user table that merely shares the prefix is never claimed - the atoms tables are
/// PARSED, as [`parse_atoms_table`] does, not prefix-matched.
pub fn owns_table(name: &str) -> bool {
    OWNED_TABLES.contains(&name) || parse_atoms_table(name).is_some()
}

/// Canonical inverse of [`atoms_table`], either encryption.
fn parse_atoms_table(name: &str) -> Option<(u16, EmbeddingMetric, bool)> {
    let rest = name.strip_prefix("memory_atoms_d")?;
    let (rest, encrypted) = match rest.strip_suffix("_enc") {
        Some(rest) => (rest, true),
        None => (rest, false),
    };
    let (dim, metric) = rest.split_once('_')?;
    let dim: u16 = dim.parse().ok()?;
    let metric = metric_from_str(metric).ok()?;
    (atoms_table(dim, metric, encrypted) == name).then_some((dim, metric, encrypted))
}

/// Canonical inverse of [`atoms_table`]; a prefix-sharing user table is never claimed.
fn parse_encrypted_atoms_table(name: &str) -> Option<(u16, EmbeddingMetric)> {
    let rest = name.strip_prefix("memory_atoms_d")?;
    let rest = rest.strip_suffix("_enc")?;
    let (dim, metric) = rest.split_once('_')?;
    let dim: u16 = dim.parse().ok()?;
    let metric = metric_from_str(metric).ok()?;
    (atoms_table(dim, metric, true) == name).then_some((dim, metric))
}

fn ensure_atoms_table(
    conn: &Connection<'_>,
    dim: u16,
    metric: EmbeddingMetric,
    encrypted: bool,
) -> Result<()> {
    let t = atoms_table(dim, metric, encrypted);
    if let Some(schema) = conn.table_schema(&t) {
        // Pre-per-atom-erasure tables lack the key columns; fail loudly here.
        if encrypted {
            let has = |col: &str| schema.columns.iter().any(|c| c.name == col);
            if !has("key_slot") || !has("key_gen") {
                return Err(MemError::Invalid(format!(
                    "encrypted atoms table '{t}' is missing key_slot/key_gen columns \
                     (pre-per-atom-erasure schema); recreate the database"
                )));
            }
        }
        return Ok(());
    }
    if encrypted {
        // Sealed-only: no plaintext column (a stale CoW page can't leak it).
        conn.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {t} (\
             id INTEGER PRIMARY KEY,\
             region_id INTEGER NOT NULL,\
             kind TEXT NOT NULL,\
             sealed BLOB NOT NULL,\
             key_slot INTEGER NOT NULL,\
             key_gen INTEGER NOT NULL,\
             score REAL DEFAULT 0,\
             confidence REAL DEFAULT 1,\
             access_count INTEGER DEFAULT 0,\
             immutable INTEGER DEFAULT 0,\
             created_at TIMESTAMP NOT NULL,\
             accessed_at TIMESTAMP NOT NULL,\
             expires_at TIMESTAMP)"
        ))?;
        conn.execute(&format!(
            "CREATE INDEX IF NOT EXISTS {t}_rk ON {t} (region_id, kind)"
        ))?;
        return Ok(());
    }
    let tag = metric_tag(metric);
    conn.execute(&format!(
        "CREATE TABLE IF NOT EXISTS {t} (\
         id INTEGER PRIMARY KEY,\
         region_id INTEGER NOT NULL,\
         kind TEXT NOT NULL,\
         embedding VECTOR({dim}) NOT NULL,\
         payload JSONB NOT NULL,\
         text_content TEXT,\
         score REAL DEFAULT 0,\
         confidence REAL DEFAULT 1,\
         access_count INTEGER DEFAULT 0,\
         immutable INTEGER DEFAULT 0,\
         created_at TIMESTAMP NOT NULL,\
         accessed_at TIMESTAMP NOT NULL,\
         expires_at TIMESTAMP)"
    ))?;
    conn.execute(&format!(
        "CREATE INDEX IF NOT EXISTS {t}_ann ON {t} USING ann (embedding) \
         WITH (metric = '{tag}', filters = 'region_id,kind')"
    ))?;
    conn.execute(&format!(
        "CREATE INDEX IF NOT EXISTS {t}_rk ON {t} (region_id, kind)"
    ))?;
    conn.execute(&format!(
        "CREATE INDEX IF NOT EXISTS {t}_jsonb ON {t} USING gin (payload) WITH (ops = 'jsonb_path_ops')"
    ))?;
    Ok(())
}

/// Next id for `key` from `memory_meta`. Must run inside a write txn.
fn next_id(conn: &Connection<'_>, key: &str) -> Result<i64> {
    next_id_range(conn, key, 1)
}

/// Reserve `n` contiguous ids for `key`, returning the first.
fn next_id_range(conn: &Connection<'_>, key: &str, n: i64) -> Result<i64> {
    if n <= 0 {
        return Err(MemError::Invalid(format!(
            "id reservation for '{key}' must be positive, got {n}"
        )));
    }
    let qr = conn.query_params(
        "SELECT value FROM memory_meta WHERE key = $1",
        &[Value::Text(key.into())],
    )?;
    let cur = qr
        .rows
        .first()
        .map(|r| as_int(&r[0]))
        .transpose()?
        .ok_or_else(|| MemError::Invalid(format!("memory_meta missing key '{key}'")))?;
    if cur < 0 {
        return Err(MemError::Invalid(format!(
            "memory_meta key '{key}' contains negative next id {cur}"
        )));
    }
    let next = cur.checked_add(n).ok_or_else(|| {
        MemError::Invalid(format!("memory_meta key '{key}' id sequence overflowed"))
    })?;
    conn.execute_params(
        "UPDATE memory_meta SET value = $1 WHERE key = $2",
        &[Value::Integer(next), Value::Text(key.into())],
    )?;
    Ok(cur)
}

/// Run `f` inside an enforced read-only transaction.
fn with_read_txn<T>(
    conn: &Connection<'_>,
    f: impl FnOnce(&Connection<'_>) -> Result<T>,
) -> Result<T> {
    with_txn(conn, "BEGIN READ ONLY", f)
}

/// Run `f` inside a read-write transaction.
fn with_write_txn<T>(
    conn: &Connection<'_>,
    f: impl FnOnce(&Connection<'_>) -> Result<T>,
) -> Result<T> {
    with_txn(conn, "BEGIN", f)
}

/// Run `f` inside the requested transaction mode, rolling back on error.
fn with_txn<T>(
    conn: &Connection<'_>,
    begin: &str,
    f: impl FnOnce(&Connection<'_>) -> Result<T>,
) -> Result<T> {
    conn.execute(begin)?;
    match f(conn) {
        Ok(v) => match conn.execute("COMMIT") {
            Ok(_) => Ok(v),
            Err(e) => {
                // A failed COMMIT may leave the txn active; clean up best-effort.
                let _ = conn.execute("ROLLBACK");
                Err(e.into())
            }
        },
        Err(e) => {
            let _ = conn.execute("ROLLBACK");
            Err(e)
        }
    }
}

pub(crate) fn metric_tag(m: EmbeddingMetric) -> &'static str {
    match m {
        EmbeddingMetric::Cosine => "cosine",
        EmbeddingMetric::L2 => "l2",
        EmbeddingMetric::InnerProduct => "inner",
    }
}

fn metric_from_str(s: &str) -> Result<EmbeddingMetric> {
    match s {
        "cosine" => Ok(EmbeddingMetric::Cosine),
        "l2" => Ok(EmbeddingMetric::L2),
        "inner" => Ok(EmbeddingMetric::InnerProduct),
        other => Err(MemError::Invalid(format!(
            "unknown stored metric '{other}'"
        ))),
    }
}

fn as_int(v: &Value) -> Result<i64> {
    match v {
        Value::Integer(i) => Ok(*i),
        other => Err(MemError::Invalid(format!(
            "expected integer, got {other:?}"
        ))),
    }
}

fn as_text(v: &Value) -> Result<&str> {
    match v {
        Value::Text(s) => Ok(s.as_str()),
        other => Err(MemError::Invalid(format!("expected text, got {other:?}"))),
    }
}

/// Boolean columns (e.g. `immutable`) are stored as INTEGER 0/1.
fn as_bool(v: &Value) -> Result<bool> {
    as_exact_bool(v, "boolean")
}

fn as_exact_bool(v: &Value, field: &str) -> Result<bool> {
    match v {
        Value::Integer(0) => Ok(false),
        Value::Integer(1) => Ok(true),
        other => Err(MemError::Invalid(format!(
            "stored {field} must be INTEGER 0 or 1, got {other:?}"
        ))),
    }
}

fn checked_embedder_dim(embedder: &dyn Embedder) -> Result<u16> {
    let dimension = embedder.dim();
    let dimension = u16::try_from(dimension).map_err(|_| {
        MemError::Invalid(format!(
            "embedding dimension must be between 1 and {}, got {dimension}",
            u16::MAX
        ))
    })?;
    if dimension == 0 {
        return Err(MemError::Invalid(
            "embedding dimension must be at least 1, got 0".into(),
        ));
    }
    Ok(dimension)
}

fn normalize_model_id(model_id: &str) -> Result<String> {
    crate::embed::normalize_model_id_label(model_id).map_err(MemError::Invalid)
}

/// Embedded, validated, serialized column values for one atom insert.
struct PreparedAtomRow {
    vec: Vec<f32>,
    payload: String,
    created: Value,
    expires: Value,
    immutable: i64,
}

fn prepare_atom_row(
    h: &RegionHandle,
    key: &str,
    atom: &AtomInput,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<PreparedAtomRow> {
    validate_atom_input(atom)?;
    let vec = match &atom.embedding {
        Some(supplied) => supplied.clone(),
        None => {
            check_cancel(cancel)?;
            let vec = embed_one(&*h.embedder, &atom.text, cancel);
            check_cancel(cancel)?;
            vec?
        }
    };
    validate_embedding(key, h.dim, &vec, "passage")?;
    let payload = serde_json::to_string(&atom.payload)
        .map_err(|e| MemError::Invalid(format!("payload not serializable: {e}")))?;
    Ok(PreparedAtomRow {
        vec,
        payload,
        created: Value::Timestamp(atom.created_at.unwrap_or_else(now_micros)),
        expires: atom.expires_at.map(Value::Timestamp).unwrap_or(Value::Null),
        immutable: i64::from(atom.immutable),
    })
}

/// Domain tags for keyed-idempotency identity material.
const IK_KEY_DOMAIN: &[u8] = b"citadel-mem-ik-key-v1";
const IK_REQUEST_DOMAIN: &[u8] = b"citadel-mem-ik-req-v2";

/// Hex identity tag over `material`: keyed BLAKE3 for encrypted regions (no
/// plaintext-equality oracle reaches disk), plain BLAKE3 for plaintext ones.
fn identity_tag(mac: Option<&IdentityMacKey>, material: &[u8]) -> String {
    match mac {
        Some(mac) => blake3::keyed_hash(&mac.key, material).to_hex().to_string(),
        None => blake3::hash(material).to_hex().to_string(),
    }
}

fn push_len_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn push_opt_micros(buf: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(v) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        None => buf.push(0),
    }
}

fn validate_idempotency_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(MemError::Invalid("empty idempotency key".into()));
    }
    Ok(())
}

fn validate_distinct_idempotency_keys(keys: &[String]) -> Result<()> {
    let mut distinct = FxHashSet::default();
    for key in keys {
        validate_idempotency_key(key)?;
        if !distinct.insert(key.as_str()) {
            return Err(MemError::Invalid(format!(
                "idempotency key {key:?} appears twice in one batch"
            )));
        }
    }
    Ok(())
}

struct KeyedBatchTags {
    kinds: Vec<String>,
    tags: Vec<(String, String)>,
}

fn keyed_batch_tags(
    mac: Option<&IdentityMacKey>,
    atoms: &[AtomInput],
    keys: &[String],
) -> Result<KeyedBatchTags> {
    let kinds = atoms.iter().map(|atom| atom.kind.clone()).collect();
    let mut tags = Vec::with_capacity(atoms.len());
    for (atom, key) in atoms.iter().zip(keys) {
        let payload_json = serde_json::to_string(&atom.payload)
            .map_err(|error| MemError::Invalid(format!("payload not serializable: {error}")))?;
        let key_tag = identity_key_tag(mac, &atom.kind, key);
        let request_tag = identity_request_tag(mac, &key_tag, atom, &payload_json, &[], None)?;
        tags.push((key_tag, request_tag));
    }
    Ok(KeyedBatchTags { kinds, tags })
}

/// Identity tag of one caller idempotency key, bound to its `kind` scope so
/// a key reused across kinds leaves no visible equality on disk.
fn identity_key_tag(mac: Option<&IdentityMacKey>, kind: &str, idempotency_key: &str) -> String {
    let mut material = Zeroizing::new(Vec::new());
    push_len_prefixed(&mut material, IK_KEY_DOMAIN);
    push_len_prefixed(&mut material, kind.as_bytes());
    push_len_prefixed(&mut material, idempotency_key.as_bytes());
    identity_tag(mac, &material)
}

/// Canonical tag binding every semantic input of one keyed remember (key
/// tag included, so identical requests under different keys stay distinct
/// on disk). `AtomInput` destructures exhaustively: a new field must extend
/// the material AND bump the domain tag (frozen vectors pin the encoding).
fn identity_request_tag(
    mac: Option<&IdentityMacKey>,
    key_tag: &str,
    atom: &AtomInput,
    payload_json: &str,
    src_ids: &[AtomId],
    evidence_ref: Option<&serde_json::Value>,
) -> Result<String> {
    let AtomInput {
        kind,
        text,
        // The canonical serialization is `payload_json`.
        payload: _,
        importance,
        confidence,
        created_at,
        expires_at,
        immutable,
        embedding,
    } = atom;
    let evidence = match evidence_ref {
        Some(v) => serde_json::to_string(v)
            .map_err(|e| MemError::Invalid(format!("evidence_ref not serializable: {e}")))?,
        None => String::new(),
    };
    let mut material = Zeroizing::new(Vec::new());
    push_len_prefixed(&mut material, IK_REQUEST_DOMAIN);
    push_len_prefixed(&mut material, key_tag.as_bytes());
    push_len_prefixed(&mut material, kind.as_bytes());
    push_len_prefixed(&mut material, text.as_bytes());
    push_len_prefixed(&mut material, payload_json.as_bytes());
    material.extend_from_slice(&importance.to_bits().to_le_bytes());
    material.extend_from_slice(&confidence.to_bits().to_le_bytes());
    push_opt_micros(&mut material, *created_at);
    push_opt_micros(&mut material, *expires_at);
    material.push(u8::from(*immutable));
    match embedding {
        None => material.push(0),
        Some(vector) => {
            material.push(1);
            material.extend_from_slice(&(vector.len() as u64).to_le_bytes());
            for value in vector {
                material.extend_from_slice(&value.to_bits().to_le_bytes());
            }
        }
    }
    material.extend_from_slice(&(src_ids.len() as u64).to_le_bytes());
    for &id in src_ids {
        material.extend_from_slice(&id.to_le_bytes());
    }
    push_len_prefixed(&mut material, evidence.as_bytes());
    Ok(identity_tag(mac, &material))
}

/// Nullable `expires_at` gate for snapshot members: present-but-lapsed is a
/// distinct, loud error (unlike recall, which silently hides expired atoms).
fn verify_snapshot_unexpired(v: &Value, id: AtomId, now: i64) -> Result<()> {
    match opt_ts(v)? {
        Some(t) if t <= now => Err(MemError::Invalid(format!("source atom {id} expired"))),
        _ => Ok(()),
    }
}

fn dedup_sources(sources: &[AtomId]) -> Vec<AtomId> {
    let mut ids = sources.to_vec();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn link_derived_sources(
    conn: &Connection<'_>,
    id: AtomId,
    src_ids: &[AtomId],
    evidence_ref: Option<&serde_json::Value>,
    edges_guard: &MemoryEdgesGuard<'_>,
) -> Result<()> {
    for &src in src_ids {
        link_edge(
            conn,
            id,
            src,
            EdgeKind::DerivedFrom,
            1.0,
            evidence_ref,
            edges_guard,
        )?;
    }
    Ok(())
}

/// Inside the caller's write transaction: delete identity records, incident
/// edges, then rows of `in_list`, returning the row DELETE's count. All
/// deletes subselect region-scoped rows, so unverified caller ids can never
/// touch another region's records.
/// `(slot, atom_id, generation)` per atom key, which only its live row names:
/// read this before deleting the rows that hold it.
fn atom_key_slots(
    conn: &Connection<'_>,
    h: &RegionHandle,
    in_list: &str,
) -> Result<Vec<(u32, u64, u64)>> {
    atom_key_slots_for(conn, h.id, &h.table, in_list)
}

fn atom_key_slots_for(
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    in_list: &str,
) -> Result<Vec<(u32, u64, u64)>> {
    let qr = conn.query_params(
        &format!(
            "SELECT id, key_slot, key_gen FROM {table} \
             WHERE region_id = $1 AND id IN ({in_list})",
        ),
        &[Value::Integer(region_id)],
    )?;
    qr.rows
        .iter()
        .map(|row| {
            let binding = atom_key_binding(as_int(&row[0])?, &row[1], &row[2])?;
            Ok((binding.slot, binding.atom_id, binding.generation))
        })
        .collect()
}

fn ensure_atom_key_slots_unreserved(
    bindings: &[(u32, u64, u64)],
    lifecycle: &KeyLifecycleGuard<'_>,
) -> Result<()> {
    let atom_ids = bindings
        .iter()
        .map(|&(_, atom_id, _)| atom_id)
        .collect::<Vec<_>>();
    lifecycle.ensure_memory_atoms_unreserved(&atom_ids)?;
    Ok(())
}

fn delete_atoms_in_txn(
    conn: &Connection<'_>,
    h: &RegionHandle,
    in_list: &str,
    edges_guard: &MemoryEdgesGuard<'_>,
) -> Result<u64> {
    delete_atoms_for(conn, h.id, &h.table, in_list, edges_guard)
}

fn delete_atoms_for(
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    in_list: &str,
    _edges_guard: &MemoryEdgesGuard<'_>,
) -> Result<u64> {
    delete_atoms_for_layout(
        conn,
        region_id,
        table,
        in_list,
        _edges_guard,
        DeleteAtomsLayout::CURRENT,
    )
}

#[derive(Clone, Copy)]
struct DeleteAtomsLayout {
    similarity_policies: bool,
    similarity_edges: bool,
}

impl DeleteAtomsLayout {
    const CURRENT: Self = Self {
        similarity_policies: true,
        similarity_edges: true,
    };
}

type OwnedStatement = (String, Vec<Value>);

fn execute_owned_statements(conn: &Connection<'_>, statements: &[OwnedStatement]) -> Result<()> {
    for (sql, params) in statements {
        conn.execute_params(sql, params)?;
    }
    Ok(())
}

fn execute_owned_uncancelled_recovery(
    conn: &Connection<'_>,
    statements: &[OwnedStatement],
) -> Result<Vec<ExecutionResult>> {
    let borrowed: Vec<(&str, &[Value])> = statements
        .iter()
        .map(|(sql, params)| (sql.as_str(), params.as_slice()))
        .collect();
    Ok(conn.execute_params_batch_uncancelled_recovery(&borrowed)?)
}

fn drop_region_cleanup_statements(
    region_id: RegionId,
    homes: &[String],
    managed_edges: &[(AtomId, AtomId)],
) -> Vec<OwnedStatement> {
    let region = || vec![Value::Integer(region_id)];
    let mut statements = Vec::new();
    for &(src, dst) in managed_edges {
        statements.push((
            "DELETE FROM memory_edges WHERE src_id = $1 AND dst_id = $2 \
             AND kind = 'similar_to'"
                .into(),
            vec![Value::Integer(src), Value::Integer(dst)],
        ));
    }
    statements.push((
        "DELETE FROM memory_similarity_edges WHERE src_id IN \
         (SELECT src_id FROM memory_similarity_policies WHERE region_id = $1)"
            .into(),
        region(),
    ));
    statements.push((
        "DELETE FROM memory_similarity_policies WHERE region_id = $1".into(),
        region(),
    ));
    for table in homes {
        statements.push((
            format!(
                "DELETE FROM memory_similarity_edges WHERE src_id IN \
                 (SELECT id FROM {table} WHERE region_id = $1) \
                 OR dst_id IN (SELECT id FROM {table} WHERE region_id = $1)"
            ),
            region(),
        ));
        statements.push((
            format!(
                "DELETE FROM memory_edges WHERE src_id IN \
                 (SELECT id FROM {table} WHERE region_id = $1) \
                 OR dst_id IN (SELECT id FROM {table} WHERE region_id = $1)"
            ),
            region(),
        ));
        statements.push((
            format!("DELETE FROM {table} WHERE region_id = $1"),
            region(),
        ));
    }
    statements.push((
        "DELETE FROM memory_idempotency WHERE region_id = $1".into(),
        region(),
    ));
    statements.push(("DELETE FROM memory_regions WHERE id = $1".into(), region()));
    statements
}

fn require_maintenance_columns(
    conn: &Connection<'_>,
    table: &str,
    columns: &[&str],
    optional: bool,
) -> Result<bool> {
    let Some(schema) = conn.table_schema(table) else {
        if optional {
            return Ok(false);
        }
        return Err(MemError::Invalid(format!(
            "memory maintenance cannot safely erase atoms: required table '{table}' is missing"
        )));
    };
    let missing: Vec<&str> = columns
        .iter()
        .copied()
        .filter(|required| !schema.columns.iter().any(|column| column.name == *required))
        .collect();
    if !missing.is_empty() {
        return Err(MemError::Invalid(format!(
            "memory maintenance cannot safely erase atoms: table '{table}' lacks {}",
            missing.join(", ")
        )));
    }
    Ok(true)
}

/// Validate every post-tombstone SQL dependency before maintenance destroys a key.
fn maintenance_delete_layout(
    conn: &Connection<'_>,
    table: &str,
    encrypted: bool,
) -> Result<DeleteAtomsLayout> {
    let atom_columns: &[&str] = if encrypted {
        &["id", "region_id", "key_slot", "key_gen"]
    } else {
        &["id", "region_id"]
    };
    require_maintenance_columns(conn, table, atom_columns, false)?;
    require_maintenance_columns(conn, "memory_meta", &["key", "value"], false)?;
    require_maintenance_columns(conn, "memory_edges", &["src_id", "dst_id"], false)?;
    require_maintenance_columns(conn, "memory_idempotency", &["atom_id"], false)?;
    let similarity_policies =
        require_maintenance_columns(conn, "memory_similarity_policies", &["src_id"], true)?;
    let similarity_edges =
        require_maintenance_columns(conn, "memory_similarity_edges", &["src_id", "dst_id"], true)?;
    Ok(DeleteAtomsLayout {
        similarity_policies,
        similarity_edges,
    })
}

fn delete_atoms_for_layout(
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    in_list: &str,
    _edges_guard: &MemoryEdgesGuard<'_>,
    layout: DeleteAtomsLayout,
) -> Result<u64> {
    let statements = delete_atoms_statements(table, in_list, layout);
    let params = [Value::Integer(region_id)];
    let mut last = ExecutionResult::RowsAffected(0);
    for statement in statements {
        last = conn.execute_params(&statement, &params)?;
    }
    Ok(match last {
        ExecutionResult::RowsAffected(n) => n,
        _ => 0,
    })
}

fn delete_atoms_for_layout_uncancelled_recovery(
    conn: &Connection<'_>,
    region_id: RegionId,
    table: &str,
    in_list: &str,
    _edges_guard: &MemoryEdgesGuard<'_>,
    layout: DeleteAtomsLayout,
) -> Result<u64> {
    let sql = delete_atoms_statements(table, in_list, layout);
    let params = [Value::Integer(region_id)];
    let statements: Vec<(&str, &[Value])> = sql
        .iter()
        .map(|statement| (statement.as_str(), params.as_slice()))
        .collect();
    let results = conn.execute_params_batch_uncancelled_recovery(&statements)?;
    Ok(match results.last() {
        Some(ExecutionResult::RowsAffected(n)) => *n,
        _ => 0,
    })
}

fn delete_atoms_statements(table: &str, in_list: &str, layout: DeleteAtomsLayout) -> Vec<String> {
    let mut statements = vec![format!(
        "DELETE FROM memory_idempotency WHERE atom_id IN \
             (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list}))"
    )];
    if layout.similarity_policies {
        statements.push(format!(
            "DELETE FROM memory_similarity_policies WHERE src_id IN \
                 (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list}))"
        ));
    }
    if layout.similarity_edges {
        statements.push(format!(
            "DELETE FROM memory_similarity_edges WHERE \
                 src_id IN (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list})) \
                 OR dst_id IN (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list}))"
        ));
    }
    statements.push(format!(
        "DELETE FROM memory_edges WHERE \
             src_id IN (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list})) \
             OR dst_id IN (SELECT id FROM {table} WHERE region_id = $1 AND id IN ({in_list}))"
    ));
    statements.push(format!(
        "DELETE FROM {table} WHERE region_id = $1 AND id IN ({in_list})"
    ));
    statements
}

fn delete_similarity_state_for_atom_list(conn: &Connection<'_>, in_list: &str) -> Result<()> {
    conn.execute(&format!(
        "DELETE FROM memory_similarity_policies WHERE src_id IN ({in_list})"
    ))?;
    conn.execute(&format!(
        "DELETE FROM memory_similarity_edges WHERE src_id IN ({in_list}) \
         OR dst_id IN ({in_list})"
    ))?;
    Ok(())
}

fn build_erasure_receipt(
    encrypted: bool,
    rows_deleted: u64,
    slots_erased: Vec<SlotErasure>,
    immutable_skipped: Vec<AtomId>,
) -> ErasureReceipt {
    let slots_erased_empty = slots_erased.is_empty();
    ErasureReceipt {
        cryptographic_erasure: encrypted,
        rows_deleted,
        erased_count: slots_erased.len() as u64,
        slots_erased,
        immutable_skipped,
        algorithm: if encrypted { "AES-256-KW(RFC3394)" } else { "" },
        wrapped_key_size: if encrypted {
            WRAPPED_KEY_SIZE as u32
        } else {
            0
        },
        // Claim durability only for erasures that passed the key store's
        // overwrite + fsync + read-back gate; an empty one attests nothing.
        fsync: encrypted && !slots_erased_empty,
        readback_confirmed: encrypted && !slots_erased_empty,
        scope_caveat: ERASURE_SCOPE_CAVEAT,
    }
}

/// Split cascade roots: in this region -> seeds, absent -> ignored (retries
/// converge), owned by another region -> loud error naming it. Runs inside
/// the caller's write transaction.
fn classify_cascade_roots(
    conn: &Connection<'_>,
    h: &RegionHandle,
    region_key: &str,
    ids: &[AtomId],
) -> Result<Vec<AtomId>> {
    let roots = dedup_sources(ids);
    if roots.is_empty() {
        return Ok(Vec::new());
    }
    let in_list = roots
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let qr = conn.query_params(
        &format!(
            "SELECT id, region_id FROM {} WHERE id IN ({in_list})",
            h.table
        ),
        &[],
    )?;
    let mut present = Vec::with_capacity(qr.rows.len());
    for row in &qr.rows {
        let id = as_int(&row[0])?;
        let owner = as_int(&row[1])?;
        if owner != h.id {
            let named = conn.query_params(
                "SELECT name FROM memory_regions WHERE id = $1",
                &[Value::Integer(owner)],
            )?;
            let owner_name = match named.rows.first() {
                Some(r) => format!("region '{}'", as_text(&r[0])?),
                None => format!("region id {owner}"),
            };
            return Err(MemError::Invalid(format!(
                "cascade root {id} belongs to {owner_name}, not '{region_key}'"
            )));
        }
        present.push(id);
    }
    present.sort_unstable();
    Ok(present)
}

/// The requested ids plus every region atom whose `DerivedFrom` closure
/// reaches them, to a fixpoint (cycle-safe via the visited set). Runs inside
/// the caller's write transaction.
fn dependent_closure(
    conn: &Connection<'_>,
    h: &RegionHandle,
    ids: &[AtomId],
    limit: usize,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomId>> {
    check_cancel(cancel)?;
    let mut visited: FxHashSet<AtomId> = ids.iter().copied().collect();
    if visited.len() > limit {
        return Err(MemError::WorkLimitExceeded {
            operation: "dependent forget",
            limit,
        });
    }
    let mut wave: Vec<AtomId> = ids.to_vec();
    while !wave.is_empty() {
        check_cancel(cancel)?;
        let in_list = wave
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let qr = conn.query_params(
            &format!(
                "SELECT e.src_id FROM memory_edges e JOIN {} a \
                 ON a.id = e.src_id AND a.region_id = $1 \
                 WHERE e.kind = $2 AND e.dst_id IN ({in_list})",
                h.table
            ),
            &[
                Value::Integer(h.id),
                Value::Text(EdgeKind::DerivedFrom.as_str().into()),
            ],
        )?;
        let mut next = Vec::new();
        for row in &qr.rows {
            check_cancel(cancel)?;
            let id = as_int(&row[0])?;
            if visited.contains(&id) {
                continue;
            }
            if visited.len() == limit {
                return Err(MemError::WorkLimitExceeded {
                    operation: "dependent forget",
                    limit,
                });
            }
            visited.insert(id);
            next.push(id);
        }
        wave = next;
    }
    check_cancel(cancel)?;
    let mut out: Vec<AtomId> = visited.into_iter().collect();
    out.sort_unstable();
    Ok(out)
}

/// Which of `ids` are immutable in the region, ascending. Runs inside the
/// caller's write transaction.
fn immutable_members(
    conn: &Connection<'_>,
    h: &RegionHandle,
    ids: &[AtomId],
) -> Result<Vec<AtomId>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let in_list = ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let qr = conn.query_params(
        &format!(
            "SELECT id FROM {} WHERE region_id = $1 AND id IN ({in_list}) \
             AND immutable = 1 ORDER BY id",
            h.table
        ),
        &[Value::Integer(h.id)],
    )?;
    qr.rows.iter().map(|row| as_int(&row[0])).collect()
}

/// Which of `ids` are superseded by another live atom in the same region.
fn superseded_ids(
    db: &Database,
    conn: &Connection<'_>,
    h: &RegionHandle,
    ids: &[AtomId],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<FxHashSet<AtomId>> {
    if ids.is_empty() {
        return Ok(FxHashSet::default());
    }
    let mut ph = Vec::with_capacity(ids.len());
    let mut params = Vec::with_capacity(ids.len());
    for (index, &id) in ids.iter().enumerate() {
        check_cancel(cancel)?;
        ph.push(format!("${}", index + 1));
        params.push(Value::Integer(id));
    }
    params.push(Value::Integer(h.id));
    let region_param = params.len();
    params.push(Value::Timestamp(now_micros()));
    let now_param = params.len();
    let key_columns = if h.atom_wrap.is_some() {
        ", src.key_slot, src.key_gen, dst.key_slot, dst.key_gen"
    } else {
        ""
    };
    let qr = conn.query_params(
        &format!(
            "SELECT e.src_id, e.dst_id{key_columns} FROM memory_edges e \
             JOIN {table} src ON src.id = e.src_id \
             JOIN {table} dst ON dst.id = e.dst_id \
             WHERE e.kind = 'supersedes' AND e.dst_id IN ({ids}) \
             AND src.region_id = ${region_param} AND dst.region_id = ${region_param} \
             AND (src.expires_at IS NULL OR src.expires_at > ${now_param}) \
             AND (dst.expires_at IS NULL OR dst.expires_at > ${now_param})",
            table = h.table,
            ids = ph.join(", "),
        ),
        &params,
    )?;
    let mut stale = FxHashSet::default();
    if h.atom_wrap.is_some() {
        let mut endpoints = Vec::with_capacity(qr.rows.len());
        let mut bindings = Vec::with_capacity(qr.rows.len().saturating_mul(2));
        let mut unique = FxHashSet::default();
        for row in &qr.rows {
            check_cancel(cancel)?;
            let src = atom_key_binding(as_int(&row[0])?, &row[2], &row[3])?;
            let dst = atom_key_binding(as_int(&row[1])?, &row[4], &row[5])?;
            endpoints.push((src, dst));
            for binding in [src, dst] {
                if unique.insert(binding) {
                    bindings.push(binding);
                }
            }
        }
        let live_flags = exact_live_atom_bindings_batch(db, &bindings)?;
        let mut live = FxHashSet::default();
        for (binding, is_live) in bindings.into_iter().zip(live_flags) {
            check_cancel(cancel)?;
            if is_live {
                live.insert(binding);
            }
        }
        for (row, (src, dst)) in qr.rows.iter().zip(endpoints) {
            check_cancel(cancel)?;
            if live.contains(&src) && live.contains(&dst) {
                stale.insert(as_int(&row[1])?);
            }
        }
    } else {
        for row in &qr.rows {
            check_cancel(cancel)?;
            stale.insert(as_int(&row[1])?);
        }
    }
    check_cancel(cancel)?;
    Ok(stale)
}

/// Upsert one edge (caller owns the txn); rejects self-loops (every kind) and
/// cycles (acyclic kinds).
fn link_edge(
    conn: &Connection<'_>,
    src: AtomId,
    dst: AtomId,
    kind: EdgeKind,
    weight: f32,
    evidence_ref: Option<&serde_json::Value>,
    _edges_guard: &MemoryEdgesGuard<'_>,
) -> Result<()> {
    validate_edge_weight(weight)?;
    if src == dst {
        return Err(MemError::Cycle { src, dst });
    }
    if kind.is_acyclic() && would_cycle(conn, src, dst, kind)? {
        return Err(MemError::Cycle { src, dst });
    }
    persist_edge(conn, src, dst, kind, weight, evidence_ref, _edges_guard)
}

#[derive(Clone, Copy)]
struct EdgeMutation<'a> {
    src: AtomId,
    dst: AtomId,
    kind: EdgeKind,
    weight: f32,
    evidence_ref: Option<&'a serde_json::Value>,
}

fn link_edge_in_region(
    db: &Database,
    conn: &Connection<'_>,
    h: &RegionHandle,
    edge: EdgeMutation<'_>,
    edges_guard: &MemoryEdgesGuard<'_>,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<()> {
    validate_edge_weight(edge.weight)?;
    if edge.src == edge.dst {
        return Err(MemError::Cycle {
            src: edge.src,
            dst: edge.dst,
        });
    }
    if edge.kind.is_acyclic()
        && would_cycle_in_region(db, conn, h, edge.src, edge.dst, edge.kind, cancel)?
    {
        return Err(MemError::Cycle {
            src: edge.src,
            dst: edge.dst,
        });
    }
    persist_edge(
        conn,
        edge.src,
        edge.dst,
        edge.kind,
        edge.weight,
        edge.evidence_ref,
        edges_guard,
    )
}

fn persist_edge(
    conn: &Connection<'_>,
    src: AtomId,
    dst: AtomId,
    kind: EdgeKind,
    weight: f32,
    evidence_ref: Option<&serde_json::Value>,
    _edges_guard: &MemoryEdgesGuard<'_>,
) -> Result<()> {
    let evidence = match evidence_ref {
        Some(v) => Value::Text(
            serde_json::to_string(v)
                .map_err(|e| MemError::Invalid(format!("evidence_ref not serializable: {e}")))?
                .into(),
        ),
        None => Value::Null,
    };
    conn.execute_params(
        "INSERT INTO memory_edges (src_id, dst_id, kind, weight, evidence_ref) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (src_id, dst_id, kind) DO UPDATE \
         SET weight = excluded.weight, evidence_ref = excluded.evidence_ref",
        &[
            Value::Integer(src),
            Value::Integer(dst),
            Value::Text(kind.as_str().into()),
            Value::Real(weight as f64),
            evidence,
        ],
    )?;
    Ok(())
}

fn validate_edge_weight(weight: f32) -> Result<()> {
    if weight.is_finite() {
        Ok(())
    } else {
        Err(MemError::Invalid("edge weight must be finite".into()))
    }
}

fn validate_fusion_weights(weights: FusionWeights) -> Result<()> {
    for (name, value) in [
        ("semantic", weights.semantic),
        ("keyword", weights.keyword),
        ("recency", weights.recency),
        ("importance", weights.importance),
    ] {
        if !value.is_finite() {
            return Err(MemError::Invalid(format!(
                "recall fusion weight '{name}' must be finite"
            )));
        }
    }
    Ok(())
}

fn validate_rrf_k(k: f32, label: &str) -> Result<()> {
    if !k.is_finite() || k <= 0.0 {
        return Err(MemError::Invalid(format!(
            "{label} must be finite and greater than zero"
        )));
    }
    Ok(())
}

fn validate_rerank_strategy(strategy: RerankStrategy) -> Result<()> {
    if let RerankStrategy::Rrf { k } = strategy {
        validate_rrf_k(k, "reranker RRF constant")?;
    }
    Ok(())
}

/// Reject NaN/infinity: codecs must never assign them ordering semantics.
fn validate_atom_input(atom: &AtomInput) -> Result<()> {
    if !atom.importance.is_finite() {
        return Err(MemError::Invalid("atom importance must be finite".into()));
    }
    if !atom.confidence.is_finite() {
        return Err(MemError::Invalid("atom confidence must be finite".into()));
    }
    if !(0.0..=1.0).contains(&atom.confidence) {
        return Err(MemError::Invalid(
            "atom confidence must be between 0 and 1".into(),
        ));
    }
    Ok(())
}

/// One boundary for both caller-supplied and embedder-produced vectors.
fn validate_embedding(region: &str, expected_dim: u16, vector: &[f32], role: &str) -> Result<()> {
    if vector.len() != expected_dim as usize {
        return Err(MemError::DimMismatch {
            region: region.into(),
            expected: expected_dim,
            got: vector.len(),
        });
    }
    if let Some((component, _)) = vector
        .iter()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(MemError::Invalid(format!(
            "{role} embedding for region '{region}' has a non-finite component at index {component}"
        )));
    }
    Ok(())
}

fn fetch_page_result(
    query: &FetchQuery,
    fetch: impl FnOnce(&FetchQuery) -> Result<Vec<AtomHit>>,
) -> Result<FetchPage> {
    if query.limit == 0 {
        return fetch(query).map(|atoms| FetchPage {
            atoms,
            next_after_id: None,
        });
    }
    if query.newest {
        return fetch(query).map(|atoms| FetchPage {
            atoms,
            next_after_id: None,
        });
    }
    let mut paged = query.clone();
    paged.limit = query
        .limit
        .checked_add(1)
        .ok_or_else(|| MemError::Invalid("atom fetch limit out of range".into()))?;
    let mut atoms = fetch(&paged)?;
    let has_more = atoms.len() > query.limit;
    atoms.truncate(query.limit);
    let next_after_id =
        has_more.then(|| atoms.last().expect("a nonempty atom page has a cursor").id);
    Ok(FetchPage {
        atoms,
        next_after_id,
    })
}

fn validate_eviction_policy(policy: &EvictionPolicy) -> Result<()> {
    match policy {
        EvictionPolicy::Stale { older_than_micros } if *older_than_micros <= 0 => Err(
            MemError::Invalid("stale eviction age must be greater than zero".into()),
        ),
        EvictionPolicy::Lru { keep_fraction }
            if !keep_fraction.is_finite() || !(0.0 < *keep_fraction && *keep_fraction <= 1.0) =>
        {
            Err(MemError::Invalid(
                "LRU keep fraction must be finite and in (0, 1]".into(),
            ))
        }
        EvictionPolicy::LowImportance {
            importance_threshold,
            confidence_threshold,
        } if !importance_threshold.is_finite()
            || !confidence_threshold.is_finite()
            || !(0.0..=1.0).contains(confidence_threshold) =>
        {
            Err(MemError::Invalid(
                "low-importance eviction thresholds must be finite and confidence must be between 0 and 1"
                    .into(),
            ))
        }
        _ => Ok(()),
    }
}

fn select_ids(
    conn: &Connection<'_>,
    sql: &str,
    params: &[Value],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomId>> {
    let qr = conn.query_params(sql, params)?;
    let mut ids = Vec::with_capacity(qr.rows.len());
    for row in &qr.rows {
        check_cancel(cancel)?;
        ids.push(as_int(&row[0])?);
    }
    check_cancel(cancel)?;
    Ok(ids)
}

fn evict_target_ids(
    conn: &Connection<'_>,
    table: &str,
    region_id: RegionId,
    policy: &EvictionPolicy,
    now: i64,
    accessed: &FxHashMap<AtomId, (i64, u32)>,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomId>> {
    match policy {
        EvictionPolicy::Stale { older_than_micros } => {
            // Persisted floor (access_count at insert) plus in-process read
            // hits: an atom recalled this lifetime is not "never accessed".
            let mut ids = select_ids(
                conn,
                &format!(
                    "SELECT id FROM {table} WHERE region_id = $1 AND immutable = 0 \
                     AND access_count = 0 AND created_at < $2"
                ),
                &[
                    Value::Integer(region_id),
                    Value::Timestamp(now.checked_sub(*older_than_micros).ok_or_else(|| {
                        MemError::Invalid("stale eviction cutoff is out of range".into())
                    })?),
                ],
                cancel,
            )?;
            check_cancel(cancel)?;
            ids.retain(|id| !accessed.contains_key(id));
            check_cancel(cancel)?;
            Ok(ids)
        }
        EvictionPolicy::LowImportance {
            importance_threshold,
            confidence_threshold,
        } => select_ids(
            conn,
            &format!(
                "SELECT id FROM {table} WHERE region_id = $1 AND immutable = 0 \
                 AND score < $2 AND confidence < $3"
            ),
            &[
                Value::Integer(region_id),
                Value::Real(*importance_threshold as f64),
                Value::Real(*confidence_threshold as f64),
            ],
            cancel,
        ),
        EvictionPolicy::Expired => select_ids(
            conn,
            &format!(
                "SELECT id FROM {table} WHERE region_id = $1 AND immutable = 0 \
                 AND expires_at IS NOT NULL AND expires_at <= $2"
            ),
            &[Value::Integer(region_id), Value::Timestamp(now)],
            cancel,
        ),
        EvictionPolicy::PurgeRegion => select_ids(
            conn,
            &format!("SELECT id FROM {table} WHERE region_id = $1"),
            &[Value::Integer(region_id)],
            cancel,
        ),
        EvictionPolicy::PredicateMatch { predicate } => {
            let js = serde_json::to_string(predicate)
                .map_err(|e| MemError::Invalid(format!("predicate not serializable: {e}")))?;
            select_ids(
                conn,
                &format!(
                    "SELECT id FROM {table} WHERE region_id = $1 AND immutable = 0 \
                     AND payload @> CAST($2 AS JSONB)"
                ),
                &[Value::Integer(region_id), Value::Text(js.into())],
                cancel,
            )
        }
        EvictionPolicy::Lru { keep_fraction } => {
            let count_qr = conn.query_params(
                &format!("SELECT COUNT(*) FROM {table} WHERE region_id = $1 AND immutable = 0"),
                &[Value::Integer(region_id)],
            )?;
            let total = count_qr
                .rows
                .first()
                .map(|r| as_int(&r[0]))
                .transpose()?
                .unwrap_or(0)
                .max(0);
            check_cancel(cancel)?;
            let delete_n = ((total as f32) * (1.0 - *keep_fraction)).floor() as i64;
            if delete_n <= 0 {
                return Ok(Vec::new());
            }
            // Least-recently-accessed first: persisted floor merged with
            // in-process hits, ranked in Rust (ties by ascending id).
            let qr = conn.query_params(
                &format!(
                    "SELECT id, accessed_at, access_count FROM {table} \
                     WHERE region_id = $1 AND immutable = 0"
                ),
                &[Value::Integer(region_id)],
            )?;
            let mut rows: Vec<(i64, u32, AtomId)> = Vec::with_capacity(qr.rows.len());
            for r in &qr.rows {
                check_cancel(cancel)?;
                let id = as_int(&r[0])?;
                let mut last = as_ts(&r[1])?;
                let mut count = as_int(&r[2])?.max(0) as u32;
                if let Some(&(mem_last, mem_count)) = accessed.get(&id) {
                    last = last.max(mem_last);
                    count += mem_count;
                }
                rows.push((last, count, id));
            }
            check_cancel(cancel)?;
            rows.sort_unstable();
            check_cancel(cancel)?;
            rows.truncate(delete_n as usize);
            let mut ids = Vec::with_capacity(rows.len());
            for (_, _, id) in rows {
                check_cancel(cancel)?;
                ids.push(id);
            }
            Ok(ids)
        }
    }
}

/// True if adding `src -> dst` would close a cycle over `kind` edges.
fn would_cycle(conn: &Connection<'_>, src: AtomId, dst: AtomId, kind: EdgeKind) -> Result<bool> {
    if src == dst {
        return Ok(true);
    }
    let qr = conn.query_params(
        "WITH RECURSIVE reach(node) AS (\
           SELECT $1 \
           UNION \
           SELECT e.dst_id FROM memory_edges e JOIN reach r ON e.src_id = r.node \
           WHERE e.kind = $3\
         ) SELECT 1 FROM reach WHERE node = $2 LIMIT 1",
        &[
            Value::Integer(dst),
            Value::Integer(src),
            Value::Text(kind.as_str().into()),
        ],
    )?;
    Ok(!qr.rows.is_empty())
}

/// Region-safe cycle check for `link_in_region`. The caller has already proved
/// `src` and `dst` live; every discovered sealed destination is exact-bound in
/// one batch before it can become the next wave.
fn would_cycle_in_region(
    db: &Database,
    conn: &Connection<'_>,
    h: &RegionHandle,
    src: AtomId,
    dst: AtomId,
    kind: EdgeKind,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<bool> {
    if src == dst {
        return Ok(true);
    }
    let scope = GraphFetchScope {
        table: &h.table,
        region_id: h.id,
        kind_allowlist: &[],
        payload_filter: None,
        sealed_db: h.atom_wrap.is_some().then_some(db),
    };
    let mut wave = vec![dst];
    let mut visited = FxHashSet::default();
    visited.insert(dst);
    let mut inspected = 0usize;
    while !wave.is_empty() {
        check_cancel(cancel)?;
        let remaining = HARD_GRAPH_EXPANSION_MAX_NODES.saturating_sub(inspected);
        let (candidates, examined, overflowed) = region_edge_wave(
            conn,
            scope,
            &wave,
            &visited,
            std::slice::from_ref(&kind),
            remaining,
            cancel,
        )?;
        if overflowed {
            return Err(MemError::Invalid(format!(
                "region cycle check exceeds the engine limit {HARD_GRAPH_EXPANSION_MAX_NODES}"
            )));
        }
        inspected += examined;
        let mut next = Vec::with_capacity(candidates.len());
        for id in candidates {
            check_cancel(cancel)?;
            if id == src {
                return Ok(true);
            }
            if visited.insert(id) {
                next.push(id);
            }
        }
        wave = next;
    }
    Ok(false)
}

const HARD_GRAPH_EXPANSION_MAX_NODES: usize = 100_000;

/// BFS depth of each non-seed atom reachable from `seeds` over `memory_edges`.
/// Each wave is bounded before it is materialized. Sealed rows become a source
/// for the next hop only after one batched exact key-binding check.
fn graph_walk_depths(
    conn: &Connection<'_>,
    scope: GraphFetchScope<'_>,
    seeds: &[AtomId],
    ge: &GraphExpand,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<FxHashMap<AtomId, usize>> {
    check_cancel(cancel)?;
    if ge.max_nodes > HARD_GRAPH_EXPANSION_MAX_NODES {
        return Err(MemError::WorkLimitExceeded {
            operation: "graph expansion",
            limit: HARD_GRAPH_EXPANSION_MAX_NODES,
        });
    }
    if seeds.is_empty() || ge.depth == 0 {
        return Ok(FxHashMap::default());
    }

    let selected_columns = if scope.sealed_db.is_some() {
        "id, key_slot, key_gen"
    } else {
        "id"
    };
    let seed_rows = conn.query_params(
        &format!(
            "SELECT {selected_columns} FROM {table} WHERE region_id = $1 \
             AND id IN ({seeds}) AND (expires_at IS NULL OR expires_at > $2) \
             ORDER BY id",
            table = scope.table,
            seeds = id_list(seeds),
        ),
        &[
            Value::Integer(scope.region_id),
            Value::Timestamp(now_micros()),
        ],
    )?;
    let mut wave = live_graph_row_ids(scope, &seed_rows.rows, cancel)?;
    let seed_set: FxHashSet<AtomId> = seeds.iter().copied().collect();
    let mut visited = seed_set.clone();
    let mut depth_of: FxHashMap<AtomId, usize> = FxHashMap::default();
    let mut inspected = 0usize;

    for depth in 1..=ge.depth {
        check_cancel(cancel)?;
        if wave.is_empty() {
            break;
        }

        let remaining = ge.max_nodes.saturating_sub(inspected);
        let (candidates, examined, overflowed) =
            region_edge_wave(conn, scope, &wave, &visited, &ge.kinds, remaining, cancel)?;
        if overflowed {
            return Err(MemError::WorkLimitExceeded {
                operation: "graph expansion",
                limit: ge.max_nodes,
            });
        }
        inspected += examined;

        let mut next = Vec::with_capacity(candidates.len());
        for id in candidates {
            check_cancel(cancel)?;
            if visited.insert(id) {
                depth_of.insert(id, depth);
                next.push(id);
            }
        }
        wave = next;
    }
    check_cancel(cancel)?;
    Ok(depth_of)
}

fn live_graph_row_ids(
    scope: GraphFetchScope<'_>,
    rows: &[Vec<Value>],
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomId>> {
    let live = match scope.sealed_db {
        Some(db) => exact_live_atom_binding_rows(db, rows, 0, 1, 2)?,
        None => vec![true; rows.len()],
    };
    check_cancel(cancel)?;
    rows.iter()
        .zip(live)
        .filter_map(|(row, live)| live.then_some(row))
        .map(|row| {
            check_cancel(cancel)?;
            as_int(&row[0])
        })
        .collect()
}

/// One bounded BFS wave over edges whose source and destination are live in the
/// selected region. `examined` includes stale sealed candidates so corrupted
/// key metadata cannot be used to evade the work budget.
fn region_edge_wave(
    conn: &Connection<'_>,
    scope: GraphFetchScope<'_>,
    wave: &[AtomId],
    visited: &FxHashSet<AtomId>,
    edge_kinds: &[EdgeKind],
    remaining: usize,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<(Vec<AtomId>, usize, bool)> {
    let mut params = vec![
        Value::Integer(scope.region_id),
        Value::Timestamp(now_micros()),
    ];
    let kind_clause = if edge_kinds.is_empty() {
        String::new()
    } else {
        let mut placeholders = Vec::with_capacity(edge_kinds.len());
        for kind in edge_kinds {
            check_cancel(cancel)?;
            params.push(Value::Text(kind.as_str().into()));
            placeholders.push(format!("${}", params.len()));
        }
        format!(" AND e.kind IN ({})", placeholders.join(", "))
    };
    let destination_columns = if scope.sealed_db.is_some() {
        "dst.id, dst.key_slot, dst.key_gen"
    } else {
        "dst.id"
    };
    let mut visited_ids: Vec<AtomId> = visited.iter().copied().collect();
    visited_ids.sort_unstable();
    let row_limit = remaining.saturating_add(1);
    let rows = conn.query_params(
        &format!(
            "SELECT DISTINCT {destination_columns} FROM memory_edges e \
             JOIN {table} src ON src.id = e.src_id \
             JOIN {table} dst ON dst.id = e.dst_id \
             WHERE e.src_id IN ({wave}) \
             AND src.region_id = $1 AND dst.region_id = $1 \
             AND (src.expires_at IS NULL OR src.expires_at > $2) \
             AND (dst.expires_at IS NULL OR dst.expires_at > $2) \
             AND dst.id NOT IN ({visited}){kind_clause} \
             ORDER BY dst.id LIMIT {row_limit}",
            table = scope.table,
            wave = id_list(wave),
            visited = id_list(&visited_ids),
        ),
        &params,
    )?;
    check_cancel(cancel)?;
    let examined = rows.rows.len();
    if examined > remaining {
        return Ok((Vec::new(), examined, true));
    }
    Ok((
        live_graph_row_ids(scope, &rows.rows, cancel)?,
        examined,
        false,
    ))
}

/// Order graph-reached `(depth, hit)` pairs nearest-first (ties by id),
/// dropping depth.
fn order_graph_hits(
    mut hits: Vec<(usize, AtomHit)>,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomHit>> {
    check_cancel(cancel)?;
    hits.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.id.cmp(&b.1.id)));
    check_cancel(cancel)?;
    let mut ordered = Vec::with_capacity(hits.len());
    for (_, hit) in hits {
        check_cancel(cancel)?;
        ordered.push(hit);
    }
    Ok(ordered)
}

#[derive(Clone, Copy)]
struct GraphFetchScope<'a> {
    table: &'a str,
    region_id: RegionId,
    kind_allowlist: &'a [String],
    /// The query's JSONB containment filter; expansion honours it like the
    /// seeds, so a filtered recall can't widen through edges.
    payload_filter: Option<&'a serde_json::Value>,
    /// The key store that makes exact row bindings authoritative for sealed hops.
    sealed_db: Option<&'a Database>,
}

/// In-clause placeholders `$3..` for `depth_of`'s ids plus the `[region_id,
/// now, ids..]` param vector. `kind_allowlist` applies to expanded atoms as
/// to the seeds.
fn graph_fetch_params(
    scope: GraphFetchScope<'_>,
    depth_of: &FxHashMap<AtomId, usize>,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<(Vec<Value>, String, String)> {
    let mut fparams: Vec<Value> = vec![
        Value::Integer(scope.region_id),
        Value::Timestamp(now_micros()),
    ];
    let mut fph = Vec::with_capacity(depth_of.len());
    for &id in depth_of.keys() {
        check_cancel(cancel)?;
        fparams.push(Value::Integer(id));
        fph.push(format!("${}", fparams.len()));
    }
    let kind_clause = if scope.kind_allowlist.is_empty() {
        String::new()
    } else {
        let mut ph = Vec::with_capacity(scope.kind_allowlist.len());
        for kind in scope.kind_allowlist {
            check_cancel(cancel)?;
            fparams.push(Value::Text(kind.clone().into()));
            ph.push(format!("${}", fparams.len()));
        }
        format!(" AND kind IN ({})", ph.join(", "))
    };
    check_cancel(cancel)?;
    Ok((fparams, fph.join(", "), kind_clause))
}

/// Walk `memory_edges` from `seeds` up to `ge.depth` hops; reachable atoms,
/// nearest first.
fn expand_graph(
    conn: &Connection<'_>,
    scope: GraphFetchScope<'_>,
    seeds: &[AtomId],
    ge: &GraphExpand,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomHit>> {
    if seeds.is_empty() || ge.depth == 0 {
        return Ok(Vec::new());
    }
    let depth_of = graph_walk_depths(conn, scope, seeds, ge, cancel)?;
    if depth_of.is_empty() {
        return Ok(Vec::new());
    }
    let (fparams, in_list, kind_clause) = graph_fetch_params(scope, &depth_of, cancel)?;
    let table = scope.table;
    let fetch_sql = format!(
        "SELECT id, kind, CAST(payload AS TEXT), text_content, immutable, created_at, score, \
         confidence, expires_at \
         FROM {table} WHERE region_id = $1 AND id IN ({in_list}) \
         AND (expires_at IS NULL OR expires_at > $2){kind_clause}"
    );
    let fetched = conn.query_params(&fetch_sql, &fparams)?;

    let mut hits: Vec<(usize, AtomHit)> = Vec::with_capacity(fetched.rows.len());
    for row in &fetched.rows {
        check_cancel(cancel)?;
        let id = as_int(&row[0])?;
        let depth = *depth_of.get(&id).unwrap_or(&1);
        let mut payload = parse_payload(&row[2])?;
        if let Some(filter) = scope.payload_filter {
            if !json_contains(&payload, filter) {
                continue;
            }
        }
        let kind = as_text(&row[1])?.to_string();
        let mut text = opt_text(&row[3])?;
        charge_owned_atom_content(&kind, &mut text, &mut payload)?;
        hits.push((
            depth,
            AtomHit {
                id,
                kind,
                payload,
                text,
                importance: as_f32(&row[6])?,
                confidence: as_f32(&row[7])?,
                relevance: None,
                distance: None,
                graph_depth: Some(depth),
                created_at: as_ts(&row[5])?,
                expires_at: opt_ts(&row[8])?,
                immutable: as_bool(&row[4])?,
            },
        ));
    }
    order_graph_hits(hits, cancel)
}

/// Graph expansion for an encrypted region: walk plaintext edges, then decrypt
/// the reachable atoms' sealed content.
fn expand_graph_sealed(
    db: &Database,
    conn: &Connection<'_>,
    atom_wrap: &AtomWrapKey,
    scope: GraphFetchScope<'_>,
    seeds: &[AtomId],
    ge: &GraphExpand,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<AtomHit>> {
    if seeds.is_empty() || ge.depth == 0 {
        return Ok(Vec::new());
    }
    let depth_of = graph_walk_depths(conn, scope, seeds, ge, cancel)?;
    if depth_of.is_empty() {
        return Ok(Vec::new());
    }
    let (fparams, in_list, kind_clause) = graph_fetch_params(scope, &depth_of, cancel)?;
    let table = scope.table;
    let fetch_sql = format!(
        "SELECT id, kind, sealed, immutable, created_at, confidence, expires_at, key_slot, \
         key_gen, score FROM {table} \
         WHERE region_id = $1 AND id IN ({in_list}) \
         AND (expires_at IS NULL OR expires_at > $2){kind_clause}"
    );
    let fetched = conn.query_params(&fetch_sql, &fparams)?;
    let wrapped = exact_live_atom_wrapped_rows(db, &fetched.rows, 0, 7, 8)?;

    let mut hits: Vec<(usize, AtomHit)> = Vec::with_capacity(fetched.rows.len());
    for (row, wrapped) in fetched.rows.iter().zip(wrapped) {
        check_cancel(cancel)?;
        let id = as_int(&row[0])?;
        let depth = *depth_of.get(&id).unwrap_or(&1);
        let Some(wrapped) = wrapped else {
            continue;
        };
        let (mut text, mut payload) =
            open_atom_content(atom_wrap, &wrapped, id, as_blob(&row[2])?)?;
        if let Some(filter) = scope.payload_filter {
            if !json_contains(&payload, filter) {
                zeroize_atom_content(&mut text, &mut payload);
                continue;
            }
        }
        let kind = as_text(&row[1])?.to_string();
        charge_owned_atom_content(&kind, &mut text, &mut payload)?;
        hits.push((
            depth,
            AtomHit {
                id,
                kind,
                payload,
                text,
                importance: as_f32(&row[9])?,
                confidence: as_f32(&row[5])?,
                relevance: None,
                distance: None,
                graph_depth: Some(depth),
                created_at: as_ts(&row[4])?,
                expires_at: opt_ts(&row[6])?,
                immutable: as_bool(&row[3])?,
            },
        ));
    }
    order_graph_hits(hits, cancel)
}

fn embed_one(
    embedder: &dyn Embedder,
    text: &str,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<f32>> {
    embedder
        .embed_with_cancel(&[text], cancel)?
        .into_iter()
        .next()
        .ok_or_else(|| MemError::Invalid("embedder returned no vector".into()))
}

/// Query-side embedding: asymmetric models (E5) encode queries differently.
fn embed_query_one(
    embedder: &dyn Embedder,
    text: &str,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<f32>> {
    embedder
        .embed_queries_with_cancel(&[text], cancel)?
        .into_iter()
        .next()
        .ok_or_else(|| MemError::Invalid("embedder returned no vector".into()))
}

/// Columns: id, kind, payload(text), text_content, score, confidence,
/// created_at, expires_at, distance, text_rank, immutable.
fn parse_candidate(row: &[Value]) -> Result<Candidate> {
    if row.len() < 11 {
        return Err(MemError::Invalid("unexpected recall row shape".into()));
    }
    let kind = as_text(&row[1])?.to_string();
    let mut payload = parse_payload(&row[2])?;
    let mut text = opt_text(&row[3])?;
    charge_owned_atom_content(&kind, &mut text, &mut payload)?;
    Ok(Candidate {
        id: as_int(&row[0])?,
        kind,
        payload,
        text,
        importance: as_f32(&row[4])?,
        confidence: as_f32(&row[5])?,
        created_micros: as_ts(&row[6])?,
        expires_micros: opt_ts(&row[7])?,
        dist: dist_value(&row[8])?,
        text_rank: as_f32(&row[9])?,
        immutable: as_bool(&row[10])?,
    })
}

/// Columns: id, kind, payload(text), text_content, score, confidence,
/// immutable, created_at, expires_at.
fn parse_fetched(row: &[Value]) -> Result<AtomHit> {
    if row.len() < 9 {
        return Err(MemError::Invalid("unexpected fetch row shape".into()));
    }
    let kind = as_text(&row[1])?.to_string();
    let mut payload = parse_payload(&row[2])?;
    let mut text = opt_text(&row[3])?;
    charge_owned_atom_content(&kind, &mut text, &mut payload)?;
    Ok(AtomHit {
        id: as_int(&row[0])?,
        kind,
        payload,
        text,
        importance: as_f32(&row[4])?,
        confidence: as_f32(&row[5])?,
        relevance: None,
        distance: None,
        graph_depth: None,
        created_at: as_ts(&row[7])?,
        expires_at: opt_ts(&row[8])?,
        immutable: as_bool(&row[6])?,
    })
}

/// Columns: src_id, dst_id, kind, weight, evidence_ref(text).
fn parse_edge(row: &[Value]) -> Result<Edge> {
    if row.len() < 5 {
        return Err(MemError::Invalid("unexpected edge row shape".into()));
    }
    let mut evidence_ref = match &row[4] {
        Value::Null => None,
        other => Some(parse_payload(other)?),
    };
    charge_owned_edge_evidence(&mut evidence_ref)?;
    Ok(Edge {
        src_id: as_int(&row[0])?,
        dst_id: as_int(&row[1])?,
        kind: edge_kind_from_str(as_text(&row[2])?)?,
        weight: as_f32(&row[3])?,
        evidence_ref,
    })
}

#[derive(Clone, Copy)]
enum EdgeEndpointFilter<'a> {
    Any,
    One(AtomId),
    AnyOf(&'a [AtomId]),
}

#[derive(Clone, Copy)]
struct RegionEdgeQuery<'a> {
    src: EdgeEndpointFilter<'a>,
    dst: EdgeEndpointFilter<'a>,
    kind: Option<EdgeKind>,
    after: Option<EdgeCursor>,
    limit: usize,
}

#[derive(Clone, Copy)]
struct RegionEdgeProjection {
    columns: &'static str,
    key_column_offset: usize,
}

fn append_edge_endpoint_filter(
    filter: EdgeEndpointFilter<'_>,
    column: &str,
    params: &mut Vec<Value>,
    clauses: &mut Vec<String>,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<bool> {
    match filter {
        EdgeEndpointFilter::Any => Ok(true),
        EdgeEndpointFilter::One(id) => {
            params.push(Value::Integer(id));
            clauses.push(format!("{column} = ${}", params.len()));
            Ok(true)
        }
        EdgeEndpointFilter::AnyOf([]) => Ok(false),
        EdgeEndpointFilter::AnyOf(ids) => {
            let mut placeholders = Vec::with_capacity(ids.len());
            for &id in ids {
                check_cancel(cancel)?;
                params.push(Value::Integer(id));
                placeholders.push(format!("${}", params.len()));
            }
            clauses.push(format!("{column} IN ({})", placeholders.join(", ")));
            Ok(true)
        }
    }
}

fn query_region_edges(
    db: &Database,
    conn: &Connection<'_>,
    h: &RegionHandle,
    query: RegionEdgeQuery<'_>,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<Edge>> {
    query_region_edge_projection(
        db,
        conn,
        h,
        query,
        cancel,
        RegionEdgeProjection {
            columns: "e.src_id, e.dst_id, e.kind, e.weight, CAST(e.evidence_ref AS TEXT)",
            key_column_offset: 5,
        },
        parse_edge,
    )
}

fn query_region_edge_endpoints(
    db: &Database,
    conn: &Connection<'_>,
    h: &RegionHandle,
    query: RegionEdgeQuery<'_>,
    cancel: Option<&citadel_core::CancelToken>,
) -> Result<Vec<(AtomId, AtomId)>> {
    query_region_edge_projection(
        db,
        conn,
        h,
        query,
        cancel,
        RegionEdgeProjection {
            columns: "e.src_id, e.dst_id, e.kind",
            key_column_offset: 3,
        },
        |row| Ok((as_int(&row[0])?, as_int(&row[1])?)),
    )
}

fn query_region_edge_projection<T>(
    db: &Database,
    conn: &Connection<'_>,
    h: &RegionHandle,
    query: RegionEdgeQuery<'_>,
    cancel: Option<&citadel_core::CancelToken>,
    projection: RegionEdgeProjection,
    parse: impl Fn(&[Value]) -> Result<T>,
) -> Result<Vec<T>> {
    if query.limit == 0 {
        check_cancel(cancel)?;
        return Ok(Vec::new());
    }
    let mut params = vec![Value::Integer(h.id), Value::Timestamp(now_micros())];
    let mut clauses = vec![
        "s.region_id = $1".to_owned(),
        "d.region_id = $1".to_owned(),
        "(s.expires_at IS NULL OR s.expires_at > $2)".to_owned(),
        "(d.expires_at IS NULL OR d.expires_at > $2)".to_owned(),
    ];
    if !append_edge_endpoint_filter(query.src, "e.src_id", &mut params, &mut clauses, cancel)?
        || !append_edge_endpoint_filter(query.dst, "e.dst_id", &mut params, &mut clauses, cancel)?
    {
        check_cancel(cancel)?;
        return Ok(Vec::new());
    }
    if let Some(kind) = query.kind {
        params.push(Value::Text(kind.as_str().into()));
        clauses.push(format!("e.kind = ${}", params.len()));
    }
    let page_size = if h.atom_wrap.is_some() {
        query.limit.max(64)
    } else {
        query.limit
    };
    let page_limit = i64::try_from(page_size)
        .map_err(|_| MemError::Invalid("edge fetch limit out of range".into()))?;

    let key_columns = if h.atom_wrap.is_some() {
        ", s.id, s.key_slot, s.key_gen, d.id, d.key_slot, d.key_gen"
    } else {
        ""
    };
    let mut projected = Vec::with_capacity(query.limit.min(1_024));
    let mut cursor = query
        .after
        .map(|after| (after.src_id, after.dst_id, after.kind.as_str().to_owned()));
    loop {
        check_cancel(cancel)?;
        let mut page_params = params.clone();
        let mut page_clauses = clauses.clone();
        if let Some((src, dst, kind)) = &cursor {
            page_params.push(Value::Integer(*src));
            let src_parameter = page_params.len();
            page_params.push(Value::Integer(*dst));
            let dst_parameter = page_params.len();
            page_params.push(Value::Text(kind.as_str().into()));
            let kind_parameter = page_params.len();
            page_clauses.push(format!(
                "(e.src_id > ${src_parameter} OR (e.src_id = ${src_parameter} AND \
                 e.dst_id > ${dst_parameter}) OR (e.src_id = ${src_parameter} AND \
                 e.dst_id = ${dst_parameter} AND e.kind > ${kind_parameter}))"
            ));
        }
        page_params.push(Value::Integer(page_limit));
        let limit_parameter = page_params.len();
        let rows = conn.query_params(
            &format!(
                "SELECT {columns}{key_columns} FROM memory_edges e \
                 JOIN {table} s ON s.id = e.src_id \
                 JOIN {table} d ON d.id = e.dst_id \
                 WHERE {where_clause} ORDER BY e.src_id, e.dst_id, e.kind \
                 LIMIT ${limit_parameter}",
                table = h.table,
                columns = projection.columns,
                where_clause = page_clauses.join(" AND "),
            ),
            &page_params,
        )?;
        if rows.rows.is_empty() {
            break;
        }
        let last = rows.rows.last().expect("nonempty edge page");
        cursor = Some((
            as_int(&last[0])?,
            as_int(&last[1])?,
            as_text(&last[2])?.to_owned(),
        ));

        let live_bindings = if h.atom_wrap.is_some() {
            let mut bindings = Vec::with_capacity(rows.rows.len() * 2);
            for row in &rows.rows {
                check_cancel(cancel)?;
                bindings.push(atom_key_binding(
                    as_int(&row[projection.key_column_offset])?,
                    &row[projection.key_column_offset + 1],
                    &row[projection.key_column_offset + 2],
                )?);
                bindings.push(atom_key_binding(
                    as_int(&row[projection.key_column_offset + 3])?,
                    &row[projection.key_column_offset + 4],
                    &row[projection.key_column_offset + 5],
                )?);
            }
            Some(exact_live_atom_bindings_batch(db, &bindings)?)
        } else {
            None
        };
        for (index, row) in rows.rows.iter().enumerate() {
            #[cfg(test)]
            debug_fire_cancel_after_local_work();
            check_cancel(cancel)?;
            if live_bindings
                .as_ref()
                .is_some_and(|live| !live[index * 2] || !live[index * 2 + 1])
            {
                continue;
            }
            projected.push(parse(row)?);
            if projected.len() == query.limit {
                break;
            }
        }
        if projected.len() == query.limit || rows.rows.len() < page_size {
            break;
        }
    }
    check_cancel(cancel)?;
    Ok(projected)
}

fn edge_kind_from_str(s: &str) -> Result<EdgeKind> {
    s.parse()
}

fn parse_payload(v: &Value) -> Result<serde_json::Value> {
    match v {
        Value::Text(s) => serde_json::from_str(s)
            .map_err(|error| MemError::Invalid(format!("stored payload is invalid JSON: {error}"))),
        Value::Null => Ok(serde_json::Value::Null),
        other => Err(MemError::Invalid(format!(
            "stored payload has unexpected type: {other:?}"
        ))),
    }
}

fn opt_text(v: &Value) -> Result<String> {
    match v {
        Value::Text(s) => Ok(s.to_string()),
        Value::Null => Ok(String::new()),
        other => Err(MemError::Invalid(format!(
            "stored text has unexpected type: {other:?}"
        ))),
    }
}

fn as_f32(v: &Value) -> Result<f32> {
    match v {
        Value::Real(r) => checked_stored_f32(*r),
        Value::Integer(i) => Ok(*i as f32),
        Value::Null => Ok(0.0),
        other => Err(MemError::Invalid(format!(
            "stored number has unexpected type: {other:?}"
        ))),
    }
}

fn checked_stored_f32(value: f64) -> Result<f32> {
    let narrowed = value as f32;
    if !value.is_finite() || !narrowed.is_finite() {
        return Err(MemError::Invalid(format!(
            "stored number is outside the finite f32 range: {value}"
        )));
    }
    Ok(narrowed)
}

fn exact_f32_bits(v: &Value) -> Result<u32> {
    match v {
        Value::Real(value) => Ok(checked_stored_f32(*value)?.to_bits()),
        Value::Integer(value) => Ok((*value as f32).to_bits()),
        other => Err(MemError::Invalid(format!(
            "expected stored f32 score, got {other:?}"
        ))),
    }
}

fn as_ts(v: &Value) -> Result<i64> {
    match v {
        Value::Timestamp(t) => Ok(*t),
        Value::Integer(i) => Ok(*i),
        Value::Null => Ok(0),
        other => Err(MemError::Invalid(format!(
            "stored timestamp has unexpected type: {other:?}"
        ))),
    }
}

/// Nullable TIMESTAMP column (`NULL` -> `None`).
fn opt_ts(v: &Value) -> Result<Option<i64>> {
    exact_opt_ts(v)
}

fn exact_opt_ts(v: &Value) -> Result<Option<i64>> {
    match v {
        Value::Timestamp(value) | Value::Integer(value) => Ok(Some(*value)),
        Value::Null => Ok(None),
        other => Err(MemError::Invalid(format!(
            "expected nullable stored timestamp, got {other:?}"
        ))),
    }
}

/// NULL distance (e.g. cosine of a zero-norm vector) sorts worst.
fn dist_value(v: &Value) -> Result<Option<f32>> {
    match v {
        Value::Null => Ok(None),
        value => as_f32(value).map(Some),
    }
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;
