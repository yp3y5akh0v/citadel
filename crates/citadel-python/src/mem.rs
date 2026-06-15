//! Memory engine: regions, recall/remember, forgetting, attestation.

use std::sync::Arc;

use citadel_mem::types::{
    AtomAttestation, AtomHit, AtomInput, EdgeKind, ErasureReceipt, EvictionPolicy, FusionWeights,
    GraphExpand, RecallQuery, RerankStrategy, SlotErasure,
};
#[cfg(feature = "candle-embed")]
use citadel_mem::{CandleConfig, CandleEmbedder, CrossEncoder};
use citadel_mem::{
    EmbedError, Embedder, EmbeddingMetric, MemoryEngine, MockEmbedder, MockReranker, Reranker,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;
use serde_json::Value as Json;

use crate::vector::require_finite;
use crate::{
    ann_index_source_dict, ann_segment_info_dict, dict_item, json_to_py, py_to_json, to_pyerr,
};

// ---- conversions / parsing -------------------------------------------------

fn parse_embedding_metric(s: &str) -> PyResult<EmbeddingMetric> {
    match s.to_ascii_lowercase().as_str() {
        "cosine" | "cos" => Ok(EmbeddingMetric::Cosine),
        "l2" | "euclidean" => Ok(EmbeddingMetric::L2),
        "ip" | "inner" | "inner_product" | "dot" => Ok(EmbeddingMetric::InnerProduct),
        other => Err(PyValueError::new_err(format!(
            "unknown embedding metric '{other}' (cosine|l2|inner)"
        ))),
    }
}

fn embedding_metric_name(m: EmbeddingMetric) -> &'static str {
    match m {
        EmbeddingMetric::Cosine => "cosine",
        EmbeddingMetric::L2 => "l2",
        EmbeddingMetric::InnerProduct => "inner",
    }
}

fn parse_edge_kind(s: &str) -> PyResult<EdgeKind> {
    match s.to_ascii_lowercase().as_str() {
        "causes" => Ok(EdgeKind::Causes),
        "contradicts" => Ok(EdgeKind::Contradicts),
        "refines" => Ok(EdgeKind::Refines),
        "precedes" => Ok(EdgeKind::Precedes),
        "supersedes" => Ok(EdgeKind::Supersedes),
        "derived_from" => Ok(EdgeKind::DerivedFrom),
        "depends_on" => Ok(EdgeKind::DependsOn),
        other => Err(PyValueError::new_err(format!(
            "unknown edge kind '{other}' (causes|contradicts|refines|precedes|supersedes|derived_from|depends_on)"
        ))),
    }
}

/// Build an `AtomInput` from a Python dict (`kind` + `text` required).
fn dict_to_atom_input(py: Python<'_>, d: &Bound<'_, PyDict>) -> PyResult<AtomInput> {
    let kind: String = d
        .get_item("kind")?
        .ok_or_else(|| PyValueError::new_err("atom dict missing 'kind'"))?
        .extract()?;
    let text: String = d
        .get_item("text")?
        .ok_or_else(|| PyValueError::new_err("atom dict missing 'text'"))?
        .extract()?;
    let payload = match d.get_item("payload")? {
        Some(p) if !p.is_none() => py_to_json(py, &p)?,
        _ => Json::Null,
    };
    Ok(AtomInput {
        kind,
        text,
        payload,
        score: dict_item(d, "score")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(0.0),
        confidence: dict_item(d, "confidence")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(1.0),
        created_at: dict_item(d, "created_at")?
            .map(|v| v.extract())
            .transpose()?,
        expires_at: dict_item(d, "expires_at")?
            .map(|v| v.extract())
            .transpose()?,
        immutable: dict_item(d, "immutable")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(false),
    })
}

// ---- embedder bridge -------------------------------------------------------

/// Adapts a Python embedder object to citadel-mem's `Embedder`. An
/// `embed_queries` method, if present, enables asymmetric (E5/granite) encoding.
struct PyEmbedder {
    callable: Py<PyAny>,
    dim: usize,
    metric: EmbeddingMetric,
    model_id: String,
    has_query_method: bool,
}

impl PyEmbedder {
    fn from_object(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let dim: usize = obj.getattr("dim")?.extract()?;
        let metric: String = obj.getattr("metric")?.extract()?;
        let metric = parse_embedding_metric(&metric)?;
        let model_id: String = obj.getattr("model_id")?.extract()?;
        let has_query_method = obj.hasattr("embed_queries")?;
        Ok(Self {
            callable: obj.clone().unbind(),
            dim,
            metric,
            model_id,
            has_query_method,
        })
    }

    /// Call `method` on the Python object and validate count + dim of the result.
    fn call(&self, method: &str, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        Python::attach(|py| {
            let arg: Vec<&str> = texts.to_vec();
            let out = self
                .callable
                .bind(py)
                .call_method1(method, (arg,))
                .map_err(|e| EmbedError::Backend(e.to_string()))?;
            let vecs = out
                .extract::<Vec<Vec<f32>>>()
                .map_err(|e| EmbedError::Backend(e.to_string()))?;
            if vecs.len() != texts.len() {
                return Err(EmbedError::Backend(format!(
                    "{method} returned {} vectors for {} texts",
                    vecs.len(),
                    texts.len()
                )));
            }
            if let Some(bad) = vecs.iter().find(|v| v.len() != self.dim) {
                return Err(EmbedError::Backend(format!(
                    "{method} returned dim {} != declared dim {}",
                    bad.len(),
                    self.dim
                )));
            }
            Ok(vecs)
        })
    }
}

impl Embedder for PyEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn metric(&self) -> EmbeddingMetric {
        self.metric
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.call("embed", texts)
    }

    fn embed_queries(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if self.has_query_method {
            self.call("embed_queries", texts)
        } else {
            self.call("embed", texts)
        }
    }
}

fn build_embedder(obj: &Bound<'_, PyAny>) -> PyResult<Arc<dyn Embedder>> {
    // A built-in CandleEmbedder is already a Rust `Embedder` - use it directly
    // (no Rust->Python->Rust round-trip per batch).
    #[cfg(feature = "candle-embed")]
    {
        if let Ok(ce) = obj.extract::<PyRef<'_, PyCandleEmbedder>>() {
            return Ok(ce.inner.clone());
        }
    }
    Ok(Arc::new(PyEmbedder::from_object(obj)?))
}

// ---- reranker bridge -------------------------------------------------------

/// Adapts a Python reranker object (`model_id` + `rerank(query, passages) ->
/// list[float]`) to citadel-mem's `Reranker`.
struct PyReranker {
    callable: Py<PyAny>,
    model_id: String,
}

impl PyReranker {
    fn from_object(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let model_id: String = obj.getattr("model_id")?.extract()?;
        Ok(Self {
            callable: obj.clone().unbind(),
            model_id,
        })
    }
}

impl Reranker for PyReranker {
    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn rerank(&self, query: &str, passages: &[&str]) -> Result<Vec<f32>, EmbedError> {
        Python::attach(|py| {
            let arg: Vec<&str> = passages.to_vec();
            let out = self
                .callable
                .bind(py)
                .call_method1("rerank", (query, arg))
                .map_err(|e| EmbedError::Backend(e.to_string()))?;
            let scores = out
                .extract::<Vec<f32>>()
                .map_err(|e| EmbedError::Backend(e.to_string()))?;
            if scores.len() != passages.len() {
                return Err(EmbedError::Backend(format!(
                    "rerank returned {} scores for {} passages",
                    scores.len(),
                    passages.len()
                )));
            }
            Ok(scores)
        })
    }
}

fn build_reranker(obj: &Bound<'_, PyAny>) -> PyResult<Arc<dyn Reranker>> {
    // Built-in rerankers are already Rust `Reranker`s - use them directly.
    if obj.extract::<PyRef<'_, PyMockReranker>>().is_ok() {
        return Ok(Arc::new(MockReranker));
    }
    #[cfg(feature = "candle-embed")]
    {
        if let Ok(ce) = obj.extract::<PyRef<'_, PyCrossEncoder>>() {
            return Ok(ce.inner.clone());
        }
    }
    Ok(Arc::new(PyReranker::from_object(obj)?))
}

fn parse_rerank_strategy(strategy: &str, rrf_k: f32) -> PyResult<RerankStrategy> {
    match strategy.to_ascii_lowercase().as_str() {
        "replace" => Ok(RerankStrategy::Replace),
        "rrf" => Ok(RerankStrategy::Rrf { k: rrf_k }),
        other => Err(PyValueError::new_err(format!(
            "unknown rerank strategy '{other}' (replace|rrf)"
        ))),
    }
}

/// Deterministic hashed cross-encoder (no models, no network); for tests and
/// quickstarts. Plugs into `Memory.set_reranker`.
#[pyclass(name = "MockReranker")]
pub(crate) struct PyMockReranker;

#[pymethods]
impl PyMockReranker {
    #[new]
    fn new() -> Self {
        Self
    }
}

/// Map a preset name to a [`CandleConfig`] (pooling / prefixes; dim comes from the model).
#[cfg(feature = "candle-embed")]
fn candle_config_for(preset: &str) -> PyResult<CandleConfig> {
    Ok(match preset.to_ascii_lowercase().as_str() {
        "bge-small" => CandleConfig::bge_small(),
        "bge-base" => CandleConfig::bge_base(),
        "bge-large" => CandleConfig::bge_large(),
        "minilm" => CandleConfig::minilm_l6(),
        "e5-large" => CandleConfig::e5_large(),
        "granite-r2" => CandleConfig::granite_r2(),
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown model preset '{other}' (bge-small|bge-base|bge-large|minilm|e5-large|granite-r2)"
            )))
        }
    })
}

/// In-process Candle sentence embedder loaded from a local model directory.
/// A `cuda-embed` build runs on GPU 0 (CPU fallback if init fails), else CPU.
#[cfg(feature = "candle-embed")]
#[pyclass(name = "CandleEmbedder")]
pub(crate) struct PyCandleEmbedder {
    inner: Arc<CandleEmbedder>,
}

#[cfg(feature = "candle-embed")]
#[pymethods]
impl PyCandleEmbedder {
    /// Load `config.json` + `tokenizer.json` + `model.safetensors` from `model_dir`.
    /// `preset` selects pooling/prefixes: bge-small|bge-base|bge-large|minilm|e5-large|granite-r2.
    #[new]
    #[pyo3(signature = (model_dir, preset="bge-small"))]
    fn new(model_dir: &str, preset: &str) -> PyResult<Self> {
        let cfg = candle_config_for(preset)?;
        let inner = CandleEmbedder::from_dir(model_dir, cfg).map_err(to_pyerr)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[getter]
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    #[getter]
    fn metric(&self) -> &'static str {
        embedding_metric_name(self.inner.metric())
    }

    #[getter]
    fn model_id(&self) -> String {
        self.inner.model_id().to_string()
    }

    /// Embed texts; releases the GIL during model inference (GPU or CPU).
    fn embed(&self, py: Python<'_>, texts: Vec<String>) -> PyResult<Vec<Vec<f32>>> {
        let inner = self.inner.clone();
        py.detach(move || {
            let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            inner.embed(&refs)
        })
        .map_err(to_pyerr)
    }

    /// Embed queries with the model's query prefix (E5/granite asymmetric
    /// retrieval); equals `embed` for symmetric presets.
    fn embed_queries(&self, py: Python<'_>, texts: Vec<String>) -> PyResult<Vec<Vec<f32>>> {
        let inner = self.inner.clone();
        py.detach(move || {
            let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            inner.embed_queries(&refs)
        })
        .map_err(to_pyerr)
    }
}

/// In-process Candle cross-encoder reranker (e.g. ms-marco-MiniLM) loaded from a
/// local model directory. Plugs into `Memory.set_reranker`.
#[cfg(feature = "candle-embed")]
#[pyclass(name = "CrossEncoder")]
pub(crate) struct PyCrossEncoder {
    inner: Arc<CrossEncoder>,
}

#[cfg(feature = "candle-embed")]
#[pymethods]
impl PyCrossEncoder {
    /// Load `config.json` + `tokenizer.json` + `model.safetensors` from `model_dir`
    /// as a ms-marco-MiniLM-L-6-v2-style cross-encoder (512-token pairs).
    #[new]
    fn new(model_dir: &str) -> PyResult<Self> {
        let inner = CrossEncoder::ms_marco_minilm_l6(model_dir).map_err(to_pyerr)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[getter]
    fn model_id(&self) -> String {
        self.inner.model_id().to_string()
    }
}

/// Deterministic hashed bag-of-words embedder (no models, no network); satisfies
/// the embedder protocol for tests and quickstarts.
#[pyclass(name = "MockEmbedder")]
pub(crate) struct PyMockEmbedder {
    inner: MockEmbedder,
}

#[pymethods]
impl PyMockEmbedder {
    #[new]
    #[pyo3(signature = (dim, metric="cosine"))]
    fn new(dim: usize, metric: &str) -> PyResult<Self> {
        Ok(Self {
            inner: MockEmbedder::with_metric(dim, parse_embedding_metric(metric)?),
        })
    }

    #[getter]
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    #[getter]
    fn metric(&self) -> &'static str {
        embedding_metric_name(self.inner.metric())
    }

    #[getter]
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }

    fn embed(&self, texts: Vec<String>) -> PyResult<Vec<Vec<f32>>> {
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        self.inner.embed(&refs).map_err(to_pyerr)
    }

    /// Query-side embedding (symmetric for the mock: equals `embed`).
    fn embed_queries(&self, texts: Vec<String>) -> PyResult<Vec<Vec<f32>>> {
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        self.inner.embed_queries(&refs).map_err(to_pyerr)
    }
}

// ---- result DTOs -----------------------------------------------------------

/// A recalled atom with its raw distance and fused score.
#[pyclass(name = "AtomHit")]
pub(crate) struct PyAtomHit {
    #[pyo3(get)]
    id: i64,
    #[pyo3(get)]
    kind: String,
    #[pyo3(get)]
    text: String,
    payload: Json,
    #[pyo3(get)]
    distance: f32,
    #[pyo3(get)]
    score: f32,
    #[pyo3(get)]
    created_at: i64,
    #[pyo3(get)]
    immutable: bool,
}

impl PyAtomHit {
    pub(crate) fn from_hit(h: AtomHit) -> Self {
        Self {
            id: h.id,
            kind: h.kind,
            text: h.text,
            payload: h.payload,
            distance: h.distance,
            score: h.score,
            created_at: h.created_at,
            immutable: h.immutable,
        }
    }
}

#[pymethods]
impl PyAtomHit {
    #[getter]
    fn payload(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &self.payload)
    }

    fn __repr__(&self) -> String {
        format!(
            "AtomHit(id={}, kind={:?}, score={:.4})",
            self.id, self.kind, self.score
        )
    }
}

/// A selective-forgetting policy for `Memory.evict`.
#[pyclass(name = "EvictionPolicy")]
pub(crate) struct PyEvictionPolicy {
    inner: EvictionPolicy,
}

impl PyEvictionPolicy {
    /// Share the underlying policy with the graph binding (`evict_guarded`).
    pub(crate) fn policy(&self) -> EvictionPolicy {
        self.inner.clone()
    }
}

#[pymethods]
impl PyEvictionPolicy {
    /// Never-accessed atoms older than `older_than_micros`.
    #[staticmethod]
    fn stale(older_than_micros: i64) -> Self {
        Self {
            inner: EvictionPolicy::Stale { older_than_micros },
        }
    }

    /// Keep the top `keep_fraction` (0.0..=1.0) by recency; drop the rest.
    #[staticmethod]
    fn lru(keep_fraction: f32) -> Self {
        Self {
            inner: EvictionPolicy::Lru { keep_fraction },
        }
    }

    /// Atoms below both thresholds.
    #[staticmethod]
    fn low_score(score_threshold: f32, confidence_threshold: f32) -> Self {
        Self {
            inner: EvictionPolicy::LowScore {
                score_threshold,
                confidence_threshold,
            },
        }
    }

    /// Wipe the whole region (including immutable atoms).
    #[staticmethod]
    fn purge_region() -> Self {
        Self {
            inner: EvictionPolicy::PurgeRegion,
        }
    }

    /// Atoms whose payload contains `predicate` (JSONB containment).
    #[staticmethod]
    fn predicate_match(py: Python<'_>, predicate: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: EvictionPolicy::PredicateMatch {
                predicate: py_to_json(py, predicate)?,
            },
        })
    }
}

/// One atom's key slot proven destroyed (Live -> Tombstone) by `Memory.forget`.
#[pyclass(name = "SlotErasure")]
pub(crate) struct PySlotErasure {
    inner: SlotErasure,
}

#[pymethods]
impl PySlotErasure {
    #[getter]
    fn slot(&self) -> u32 {
        self.inner.slot
    }

    #[getter]
    fn atom_id(&self) -> i64 {
        self.inner.atom_id
    }

    #[getter]
    fn old_gen(&self) -> u64 {
        self.inner.old_gen
    }

    #[getter]
    fn new_gen(&self) -> u64 {
        self.inner.new_gen
    }

    fn __repr__(&self) -> String {
        format!(
            "SlotErasure(slot={}, atom_id={}, old_gen={}, new_gen={})",
            self.inner.slot, self.inner.atom_id, self.inner.old_gen, self.inner.new_gen
        )
    }
}

/// Receipt from `Memory.forget`: what was (cryptographically) erased.
#[pyclass(name = "ErasureReceipt")]
pub(crate) struct PyErasureReceipt {
    inner: ErasureReceipt,
}

#[pymethods]
impl PyErasureReceipt {
    #[getter]
    fn cryptographic_erasure(&self) -> bool {
        self.inner.cryptographic_erasure
    }

    #[getter]
    fn rows_deleted(&self) -> u64 {
        self.inner.rows_deleted
    }

    #[getter]
    fn erased_count(&self) -> u64 {
        self.inner.erased_count
    }

    #[getter]
    fn immutable_skipped(&self) -> Vec<i64> {
        self.inner.immutable_skipped.clone()
    }

    #[getter]
    fn algorithm(&self) -> &'static str {
        self.inner.algorithm
    }

    #[getter]
    fn readback_confirmed(&self) -> bool {
        self.inner.readback_confirmed
    }

    #[getter]
    fn scope_caveat(&self) -> &'static str {
        self.inner.scope_caveat
    }

    /// Per-atom proof of key destruction (slot Live -> Tombstone at a new gen).
    #[getter]
    fn slots_erased(&self) -> Vec<PySlotErasure> {
        self.inner
            .slots_erased
            .iter()
            .map(|s| PySlotErasure { inner: s.clone() })
            .collect()
    }

    #[getter]
    fn wrapped_key_size(&self) -> u32 {
        self.inner.wrapped_key_size
    }

    #[getter]
    fn fsync(&self) -> bool {
        self.inner.fsync
    }

    fn __repr__(&self) -> String {
        format!(
            "ErasureReceipt(cryptographic_erasure={}, erased_count={}, rows_deleted={})",
            self.inner.cryptographic_erasure, self.inner.erased_count, self.inner.rows_deleted
        )
    }
}

/// One atom's integrity verdict from `Memory.verify`.
#[pyclass(name = "AtomAttestation")]
pub(crate) struct PyAtomAttestation {
    inner: AtomAttestation,
}

#[pymethods]
impl PyAtomAttestation {
    #[getter]
    fn atom_id(&self) -> i64 {
        self.inner.atom_id
    }

    #[getter]
    fn verdict(&self) -> &'static str {
        self.inner.verdict.as_str()
    }

    #[getter]
    fn aad_bound(&self) -> bool {
        self.inner.aad_bound
    }

    #[getter]
    fn key_slot(&self) -> Option<u32> {
        self.inner.key_slot
    }

    #[getter]
    fn key_gen(&self) -> Option<u64> {
        self.inner.key_gen
    }

    fn __repr__(&self) -> String {
        format!(
            "AtomAttestation(atom_id={}, verdict={})",
            self.inner.atom_id,
            self.inner.verdict.as_str()
        )
    }
}

// ---- recall options --------------------------------------------------------

/// Advanced `recall` modifiers (all optional): `payload_filter`, fusion
/// `weights`, `as_of_micros` recency anchor, and `graph_expand`.
#[pyclass(name = "RecallOptions")]
pub(crate) struct PyRecallOptions {
    payload_filter: Option<Json>,
    weights: Option<FusionWeights>,
    as_of_micros: Option<i64>,
    graph_expand: Option<GraphExpand>,
}

#[pymethods]
impl PyRecallOptions {
    #[new]
    #[pyo3(signature = (*, payload_filter=None, weights=None, as_of_micros=None, graph_expand=None))]
    fn new(
        py: Python<'_>,
        payload_filter: Option<Py<PyAny>>,
        weights: Option<(f32, f32, f32, f32)>,
        as_of_micros: Option<i64>,
        graph_expand: Option<(usize, Vec<String>)>,
    ) -> PyResult<Self> {
        let payload_filter = match &payload_filter {
            Some(p) => Some(py_to_json(py, p.bind(py))?),
            None => None,
        };
        let weights = weights.map(|(semantic, keyword, recency, importance)| FusionWeights {
            semantic,
            keyword,
            recency,
            importance,
        });
        let graph_expand = match graph_expand {
            Some((depth, kinds)) => {
                let parsed = kinds
                    .iter()
                    .map(|s| parse_edge_kind(s))
                    .collect::<PyResult<Vec<_>>>()?;
                Some(GraphExpand::new(depth, parsed))
            }
            None => None,
        };
        Ok(Self {
            payload_filter,
            weights,
            as_of_micros,
            graph_expand,
        })
    }
}

// ---- the engine ------------------------------------------------------------

/// The memory engine over a `Database`. Obtain via `db.memory()`.
#[pyclass(name = "Memory")]
pub(crate) struct PyMemory {
    inner: Arc<MemoryEngine>,
}

impl PyMemory {
    pub(crate) fn from_engine(inner: Arc<MemoryEngine>) -> Self {
        Self { inner }
    }

    /// Share the underlying engine with the agent/graph bindings.
    pub(crate) fn engine(&self) -> Arc<MemoryEngine> {
        Arc::clone(&self.inner)
    }
}

#[pymethods]
impl PyMemory {
    /// Create a plaintext region bound to `embedder`.
    fn create_region(&self, name: &str, embedder: &Bound<'_, PyAny>) -> PyResult<i64> {
        self.inner
            .create_region(name, build_embedder(embedder)?)
            .map_err(to_pyerr)
    }

    /// Create an encrypted region (per-atom sealing + crypto-erasure). Requires the
    /// database to have been opened with `region_keys=True`.
    fn create_encrypted_region(&self, name: &str, embedder: &Bound<'_, PyAny>) -> PyResult<i64> {
        self.inner
            .create_encrypted_region(name, build_embedder(embedder)?)
            .map_err(to_pyerr)
    }

    fn drop_region(&self, name: &str) -> PyResult<()> {
        self.inner.drop_region(name).map_err(to_pyerr)
    }

    /// Remember one atom (a dict with `kind` + `text`, optional `payload`, `score`,
    /// `confidence`, `created_at`, `expires_at`, `immutable`). Returns its id.
    fn remember(&self, py: Python<'_>, region: &str, atom: &Bound<'_, PyDict>) -> PyResult<i64> {
        let input = dict_to_atom_input(py, atom)?;
        self.inner.remember(region, input).map_err(to_pyerr)
    }

    /// Remember a list of atom dicts in one transaction. Returns their ids.
    fn remember_batch(
        &self,
        py: Python<'_>,
        region: &str,
        atoms: Vec<Py<PyDict>>,
    ) -> PyResult<Vec<i64>> {
        let inputs = atoms
            .iter()
            .map(|a| dict_to_atom_input(py, a.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;
        self.inner.remember_batch(region, inputs).map_err(to_pyerr)
    }

    /// Hybrid recall by `text` (embedded + keyword-ranked) and/or a precomputed
    /// `embedding`; returns the top `k` atoms by fused score. `options` carries the
    /// advanced `RecallQuery` modifiers (payload filter, weights, recency, graph).
    #[pyo3(signature = (region, *, text=None, embedding=None, k=10, kinds=None, options=None))]
    fn recall(
        &self,
        region: &str,
        text: Option<String>,
        embedding: Option<Vec<f32>>,
        k: usize,
        kinds: Option<Vec<String>>,
        options: Option<&PyRecallOptions>,
    ) -> PyResult<Vec<PyAtomHit>> {
        if let Some(e) = embedding.as_deref() {
            require_finite("embedding", e)?;
        }
        let mut q = match (text, embedding) {
            (Some(t), None) => RecallQuery::by_text(t, k),
            (None, Some(e)) => RecallQuery::by_embedding(e, k),
            (Some(t), Some(e)) => RecallQuery::by_embedding(e, k).with_text(t),
            (None, None) => {
                return Err(PyValueError::new_err("recall requires text= or embedding="))
            }
        };
        if let Some(ks) = kinds {
            q = q.with_kinds(ks);
        }
        if let Some(opts) = options {
            if let Some(pf) = &opts.payload_filter {
                q = q.with_payload_filter(pf.clone());
            }
            if let Some(w) = opts.weights {
                q = q.with_weights(w);
            }
            if let Some(m) = opts.as_of_micros {
                q = q.with_as_of(m);
            }
            if let Some(ge) = &opts.graph_expand {
                q = q.with_graph_expand(ge.clone());
            }
        }
        Ok(self
            .inner
            .recall(region, q)
            .map_err(to_pyerr)?
            .into_iter()
            .map(PyAtomHit::from_hit)
            .collect())
    }

    /// Non-semantic fetch of a `kind`, optionally narrowed by a JSONB `payload_filter`.
    #[pyo3(signature = (region, kind, *, payload_filter=None, limit=100))]
    fn fetch(
        &self,
        py: Python<'_>,
        region: &str,
        kind: &str,
        payload_filter: Option<Py<PyAny>>,
        limit: usize,
    ) -> PyResult<Vec<PyAtomHit>> {
        let pf = match &payload_filter {
            Some(p) => Some(py_to_json(py, p.bind(py))?),
            None => None,
        };
        Ok(self
            .inner
            .fetch(region, kind, pf.as_ref(), limit)
            .map_err(to_pyerr)?
            .into_iter()
            .map(PyAtomHit::from_hit)
            .collect())
    }

    fn fetch_one(&self, region: &str, atom_id: i64) -> PyResult<Option<PyAtomHit>> {
        Ok(self
            .inner
            .fetch_one(region, atom_id)
            .map_err(to_pyerr)?
            .map(PyAtomHit::from_hit))
    }

    fn count(&self, region: &str, kind: &str) -> PyResult<u64> {
        self.inner.count(region, kind).map_err(to_pyerr)
    }

    /// Link two atoms with a typed edge (causes/contradicts/refines/precedes/
    /// supersedes/derived_from/depends_on).
    #[pyo3(signature = (src, dst, kind, weight=1.0))]
    fn link(&self, src: i64, dst: i64, kind: &str, weight: f32) -> PyResult<()> {
        self.inner
            .link(src, dst, parse_edge_kind(kind)?, weight)
            .map_err(to_pyerr)
    }

    /// Evict atoms by policy; returns the number removed.
    fn evict(&self, region: &str, policy: &PyEvictionPolicy) -> PyResult<u64> {
        Ok(self
            .inner
            .evict(region, policy.inner.clone())
            .map_err(to_pyerr)?
            .removed)
    }

    /// Forget atoms (cryptographic erasure on encrypted regions); returns a receipt.
    #[pyo3(signature = (region, ids, force=false))]
    fn forget(&self, region: &str, ids: Vec<i64>, force: bool) -> PyResult<PyErasureReceipt> {
        Ok(PyErasureReceipt {
            inner: self
                .inner
                .forget_atoms(region, &ids, force)
                .map_err(to_pyerr)?,
        })
    }

    /// Attest the integrity/origin of atoms (encrypted regions).
    fn verify(&self, region: &str, ids: Vec<i64>) -> PyResult<Vec<PyAtomAttestation>> {
        Ok(self
            .inner
            .verify_atoms(region, &ids)
            .map_err(to_pyerr)?
            .into_iter()
            .map(|inner| PyAtomAttestation { inner })
            .collect())
    }

    /// Replace an atom's JSON payload.
    fn update_atom_payload(
        &self,
        py: Python<'_>,
        region: &str,
        atom_id: i64,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let json = py_to_json(py, payload)?;
        self.inner
            .update_atom_payload(region, atom_id, &json)
            .map_err(to_pyerr)
    }

    /// The most recently created atom of `kind`, if any.
    fn fetch_last(&self, region: &str, kind: &str) -> PyResult<Option<PyAtomHit>> {
        Ok(self
            .inner
            .fetch_last(region, kind)
            .map_err(to_pyerr)?
            .map(PyAtomHit::from_hit))
    }

    /// Read edges, optionally filtered by `src`/`dst`/`kind`; each is
    /// `{src, dst, kind, weight, evidence}`.
    #[pyo3(signature = (*, src=None, dst=None, kind=None))]
    fn fetch_edges(
        &self,
        py: Python<'_>,
        src: Option<i64>,
        dst: Option<i64>,
        kind: Option<String>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let kind = kind.map(|s| parse_edge_kind(&s)).transpose()?;
        self.inner
            .fetch_edges(src, dst, kind)
            .map_err(to_pyerr)?
            .iter()
            .map(|e| {
                let d = PyDict::new(py);
                d.set_item("src", e.src_id)?;
                d.set_item("dst", e.dst_id)?;
                d.set_item("kind", e.kind.as_str())?;
                d.set_item("weight", e.weight)?;
                let evidence = match &e.evidence_ref {
                    Some(j) => json_to_py(py, j)?,
                    None => py.None(),
                };
                d.set_item("evidence", evidence)?;
                d.into_py_any(py)
            })
            .collect()
    }

    /// Recompute an atom's neighbor links by ANN search; returns `{links_added, score}`.
    fn evolve(
        &self,
        py: Python<'_>,
        region: &str,
        atom_id: i64,
        neighbors: usize,
        max_distance: f32,
    ) -> PyResult<Py<PyAny>> {
        let r = self
            .inner
            .evolve(region, atom_id, neighbors, max_distance)
            .map_err(to_pyerr)?;
        let d = PyDict::new(py);
        d.set_item("links_added", r.links_added)?;
        d.set_item("score", r.score)?;
        d.into_py_any(py)
    }

    /// Structural digest of a region since `since_micros`: `{total, kinds: [...]}`.
    fn summarize(&self, py: Python<'_>, region: &str, since_micros: i64) -> PyResult<Py<PyAny>> {
        let r = self
            .inner
            .summarize(region, since_micros)
            .map_err(to_pyerr)?;
        let kinds = r
            .kinds
            .iter()
            .map(|kd| {
                let k = PyDict::new(py);
                k.set_item("kind", kd.kind.as_str())?;
                k.set_item("count", kd.count)?;
                k.set_item("earliest", kd.earliest)?;
                k.set_item("latest", kd.latest)?;
                k.set_item("avg_score", kd.avg_score)?;
                k.set_item("avg_confidence", kd.avg_confidence)?;
                k.into_py_any(py)
            })
            .collect::<PyResult<Vec<_>>>()?;
        let d = PyDict::new(py);
        d.set_item("total", r.total)?;
        d.set_item("kinds", kinds)?;
        d.into_py_any(py)
    }

    /// Attach a cross-encoder reranker applied in `recall` before truncation.
    /// `reranker` is a built-in `MockReranker`/`CrossEncoder` or any object with
    /// `model_id` + `rerank(query, passages) -> list[float]`. `strategy` is "rrf"
    /// (reciprocal-rank fusion, damping `rrf_k`) or "replace".
    #[pyo3(signature = (reranker, *, strategy="rrf", rrf_k=20.0))]
    fn set_reranker(
        &self,
        reranker: &Bound<'_, PyAny>,
        strategy: &str,
        rrf_k: f32,
    ) -> PyResult<()> {
        self.inner.set_reranker(
            build_reranker(reranker)?,
            parse_rerank_strategy(strategy, rrf_k)?,
        );
        Ok(())
    }

    /// Detach the reranker so later `recall`s use linear fusion only (the default).
    fn clear_reranker(&self) {
        self.inner.clear_reranker();
    }

    /// Freeze the region's ANN index into a persisted segment so a later cold
    /// reopen LOADs it instead of paying the full PRISM rebuild (encrypted regions
    /// seal it under an erasable key). Returns the segment manifest dict.
    fn persist_ann_index(&self, py: Python<'_>, region: &str) -> PyResult<Py<PyAny>> {
        let eng = Arc::clone(&self.inner);
        let region = region.to_string();
        let info = py
            .detach(move || eng.persist_ann_index(&region))
            .map_err(to_pyerr)?;
        ann_segment_info_dict(py, &info)?.into_py_any(py)
    }

    /// How this region's recall is served: `None` if nothing is cached yet, else
    /// `{"source": "loaded", "segment_b3": bytes}` or `{"source": "built",
    /// "refusal": str|None}`.
    fn ann_cache_status(&self, py: Python<'_>, region: &str) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.ann_cache_status(region).map_err(to_pyerr)? {
            None => Ok(None),
            Some(src) => Ok(Some(ann_index_source_dict(py, &src)?.into_py_any(py)?)),
        }
    }
}
