//! Memory engine: regions, recall/remember, forgetting, attestation.

use std::sync::Arc;

use citadel::CancelToken;
use citadel_mem::types::{
    AtomAttestation, AtomHit, AtomInput, Edge, EdgeKind, ErasureReceipt, EvictionPolicy,
    FetchQuery, FusionWeights, GraphExpand, MemoryRegionInfo, MemoryRegionInventory, RecallQuery,
    ReembedReport, RememberOutcome, RerankStrategy, SlotErasure, StoredRegionIdentity,
    SummaryQuery,
};
#[cfg(feature = "candle-embed")]
use citadel_mem::{CandleConfig, CandleEmbedder, CrossEncoder};
use citadel_mem::{
    EmbedError, Embedder, EmbeddingMetric, MemoryEngine, MemoryMaintenance, MockEmbedder,
    MockReranker, Reranker,
};
use pyo3::exceptions::{PyInterruptedError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict};
use pyo3::IntoPyObjectExt;
use serde_json::Value as Json;

use crate::sql::PyCancelToken;
use crate::vector::require_finite;
use crate::{
    ann_index_source_dict, ann_segment_info_dict, dict_item, json_to_py, py_to_json, to_pyerr,
};

fn callback_error(py: Python<'_>, error: PyErr) -> EmbedError {
    if error.is_instance_of::<PyInterruptedError>(py) {
        EmbedError::Interrupted
    } else {
        EmbedError::Backend(error.to_string())
    }
}

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
    let normalized = s.to_ascii_lowercase();
    normalized.parse().map_err(|_| {
        PyValueError::new_err(format!(
            "unknown edge kind '{normalized}' (causes|contradicts|refines|precedes|supersedes|derived_from|depends_on|similar_to)"
        ))
    })
}

fn edge_to_py(py: Python<'_>, edge: &Edge) -> PyResult<Py<PyAny>> {
    let row = PyDict::new(py);
    row.set_item("src", edge.src_id)?;
    row.set_item("dst", edge.dst_id)?;
    row.set_item("kind", edge.kind.as_str())?;
    row.set_item("weight", edge.weight)?;
    let evidence = match &edge.evidence_ref {
        Some(value) => json_to_py(py, value)?,
        None => py.None(),
    };
    row.set_item("evidence", evidence)?;
    row.into_py_any(py)
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
        importance: dict_item(d, "importance")?
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
        // Supplying a vector skips the region embedder; the dim is still validated.
        embedding: dict_item(d, "embedding")?
            .map(|v| v.extract::<Vec<f32>>())
            .transpose()?,
    })
}

// ---- embedder bridge -------------------------------------------------------

/// Adapts a token-aware Python embedder object to citadel-mem's `Embedder`.
/// An `embed_queries_with_cancel` method, if present, enables E5-style
/// asymmetric encoding.
struct PyEmbedder {
    callable: Py<PyAny>,
    dim: usize,
    metric: EmbeddingMetric,
    model_id: String,
    has_query_cancel_method: bool,
}

fn extract_embedder_dim(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    let raw: i64 = value.extract()?;
    if value.is_instance_of::<PyBool>() || !(1..=i64::from(u16::MAX)).contains(&raw) {
        return Err(PyValueError::new_err(
            "embedder dim must be a positive integer no greater than 65535",
        ));
    }
    Ok(raw as usize)
}

impl PyEmbedder {
    fn from_object(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let dim_value = obj.getattr("dim")?;
        let dim = extract_embedder_dim(&dim_value)?;
        let metric: String = obj.getattr("metric")?.extract()?;
        let metric = parse_embedding_metric(&metric)?;
        let model_id: String = obj.getattr("model_id")?.extract()?;
        let model_id = model_id.trim().to_owned();
        if model_id.is_empty()
            || matches!(
                model_id.to_ascii_lowercase().as_str(),
                "unknown" | "default"
            )
        {
            return Err(PyValueError::new_err(
                "embedder model_id must be a nonblank string other than 'unknown' or 'default'",
            ));
        }
        let has_query_cancel_method = obj.hasattr("embed_queries_with_cancel")?;
        if !obj.hasattr("embed_with_cancel")? || !obj.getattr("embed_with_cancel")?.is_callable() {
            return Err(PyTypeError::new_err(
                "embedder must provide a callable embed_with_cancel(texts, cancel_token) method",
            ));
        }
        if has_query_cancel_method && !obj.getattr("embed_queries_with_cancel")?.is_callable() {
            return Err(PyTypeError::new_err(
                "embedder embed_queries_with_cancel attribute must be callable",
            ));
        }
        Ok(Self {
            callable: obj.clone().unbind(),
            dim,
            metric,
            model_id,
            has_query_cancel_method,
        })
    }

    /// Call `method` on the Python object and validate count + dim of the result.
    fn call(
        &self,
        method: &str,
        texts: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        Python::attach(|py| {
            let arg: Vec<&str> = texts.to_vec();
            let cancel = cancel
                .map(|token| Py::new(py, PyCancelToken::from_inner(token.clone())))
                .transpose()
                .map_err(|error| EmbedError::Backend(error.to_string()))?;
            let out = self
                .callable
                .bind(py)
                .call_method1(method, (arg, cancel))
                .map_err(|error| callback_error(py, error))?;
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

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.call("embed_with_cancel", texts, cancel)
    }

    fn embed_queries_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        if self.has_query_cancel_method {
            self.call("embed_queries_with_cancel", texts, cancel)
        } else {
            self.call("embed_with_cancel", texts, cancel)
        }
    }
}

fn build_embedder(obj: &Bound<'_, PyAny>) -> PyResult<Arc<dyn Embedder>> {
    // Built-in embedders are already Rust `Embedder`s; avoid a
    // Rust->Python->Rust round-trip and Python list allocation per batch.
    if let Ok(mock) = obj.extract::<PyRef<'_, PyMockEmbedder>>() {
        return Ok(mock.inner.clone());
    }
    #[cfg(feature = "candle-embed")]
    {
        if let Ok(ce) = obj.extract::<PyRef<'_, PyCandleEmbedder>>() {
            return Ok(ce.inner.clone());
        }
    }
    Ok(Arc::new(PyEmbedder::from_object(obj)?))
}

// ---- reranker bridge -------------------------------------------------------

/// Adapts a token-aware Python reranker object to citadel-mem's `Reranker`.
struct PyReranker {
    callable: Py<PyAny>,
    model_id: String,
}

impl PyReranker {
    fn from_object(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let model_id: String = obj.getattr("model_id")?.extract()?;
        let model_id = model_id.trim().to_owned();
        if model_id.is_empty() {
            return Err(PyValueError::new_err(
                "reranker model_id must be a nonblank string",
            ));
        }
        if !obj.hasattr("rerank_with_cancel")? || !obj.getattr("rerank_with_cancel")?.is_callable()
        {
            return Err(PyTypeError::new_err(
                "reranker must provide a callable rerank_with_cancel(query, passages, cancel_token) method",
            ));
        }
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

    fn rerank_with_cancel(
        &self,
        query: &str,
        passages: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<f32>, EmbedError> {
        Python::attach(|py| {
            let arg: Vec<&str> = passages.to_vec();
            let cancel = cancel
                .map(|token| Py::new(py, PyCancelToken::from_inner(token.clone())))
                .transpose()
                .map_err(|error| EmbedError::Backend(error.to_string()))?;
            let out = self
                .callable
                .bind(py)
                .call_method1("rerank_with_cancel", (query, arg, cancel))
                .map_err(|error| callback_error(py, error))?;
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
            if let Some((index, score)) = scores
                .iter()
                .enumerate()
                .find(|(_, score)| !score.is_finite())
            {
                return Err(EmbedError::Backend(format!(
                    "rerank returned non-finite score {score} at index {index}"
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
        "rrf" if rrf_k.is_finite() && rrf_k > 0.0 => Ok(RerankStrategy::Rrf { k: rrf_k }),
        "rrf" => Err(PyValueError::new_err(
            "rrf_k must be finite and greater than zero",
        )),
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

    #[getter]
    fn model_id(&self) -> &str {
        MockReranker.model_id()
    }

    fn rerank(&self, query: &str, passages: Vec<String>) -> PyResult<Vec<f32>> {
        let refs: Vec<&str> = passages.iter().map(String::as_str).collect();
        MockReranker.rerank(query, &refs).map_err(to_pyerr)
    }

    #[pyo3(signature = (query, passages, cancel_token=None))]
    fn rerank_with_cancel(
        &self,
        query: &str,
        passages: Vec<String>,
        cancel_token: Option<PyRef<'_, PyCancelToken>>,
    ) -> PyResult<Vec<f32>> {
        let refs: Vec<&str> = passages.iter().map(String::as_str).collect();
        MockReranker
            .rerank_with_cancel(
                query,
                &refs,
                cancel_token.as_deref().map(PyCancelToken::as_inner),
            )
            .map_err(to_pyerr)
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
        "e5-large-v2" => CandleConfig::e5_large_v2(),
        "granite-r2" => CandleConfig::granite_r2(),
        "arctic" => CandleConfig::arctic(),
        "modernbert-embed" => CandleConfig::modernbert_embed(),
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown model preset '{other}' (bge-small|bge-base|bge-large|minilm|e5-large|e5-large-v2|granite-r2|arctic|modernbert-embed)"
            )))
        }
    })
}

#[cfg(all(test, feature = "candle-embed"))]
mod candle_config_tests {
    use super::candle_config_for;

    #[test]
    fn python_exposes_every_rust_candle_preset() {
        let expected = [
            ("bge-small", "bge-small-en-v1.5"),
            ("bge-base", "bge-base-en-v1.5"),
            ("bge-large", "bge-large-en-v1.5"),
            ("minilm", "all-MiniLM-L6-v2"),
            ("e5-large", "e5-large"),
            ("e5-large-v2", "e5-large-v2"),
            ("granite-r2", "granite-embedding-english-r2"),
            ("arctic", "snowflake-arctic-embed"),
            ("modernbert-embed", "modernbert-embed-base"),
        ];
        for (preset, model_id) in expected {
            assert_eq!(candle_config_for(preset).unwrap().model_id, model_id);
        }
    }
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
    /// `preset` selects pooling/prefixes: bge-small|bge-base|bge-large|minilm|
    /// e5-large|e5-large-v2|granite-r2|arctic|modernbert-embed.
    #[new]
    #[pyo3(signature = (model_dir, preset="e5-large"))]
    fn new(py: Python<'_>, model_dir: &str, preset: &str) -> PyResult<Self> {
        let cfg = candle_config_for(preset)?;
        let model_dir = model_dir.to_owned();
        let inner = py
            .detach(move || CandleEmbedder::from_dir(&model_dir, cfg))
            .map_err(to_pyerr)?;
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
    /// Friendly model label plus the artifact and pipeline fingerprint.
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

    #[pyo3(signature = (texts, cancel_token=None))]
    fn embed_with_cancel(
        &self,
        py: Python<'_>,
        texts: Vec<String>,
        cancel_token: Option<PyRef<'_, PyCancelToken>>,
    ) -> PyResult<Vec<Vec<f32>>> {
        let inner = self.inner.clone();
        let cancel = cancel_token.map(|token| token.as_inner().clone());
        py.detach(move || {
            let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            inner.embed_with_cancel(&refs, cancel.as_ref())
        })
        .map_err(to_pyerr)
    }

    /// Embed queries with the model's query prefix for asymmetric retrieval;
    /// equals `embed` for symmetric presets.
    fn embed_queries(&self, py: Python<'_>, texts: Vec<String>) -> PyResult<Vec<Vec<f32>>> {
        let inner = self.inner.clone();
        py.detach(move || {
            let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            inner.embed_queries(&refs)
        })
        .map_err(to_pyerr)
    }

    #[pyo3(signature = (texts, cancel_token=None))]
    fn embed_queries_with_cancel(
        &self,
        py: Python<'_>,
        texts: Vec<String>,
        cancel_token: Option<PyRef<'_, PyCancelToken>>,
    ) -> PyResult<Vec<Vec<f32>>> {
        let inner = self.inner.clone();
        let cancel = cancel_token.map(|token| token.as_inner().clone());
        py.detach(move || {
            let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            inner.embed_queries_with_cancel(&refs, cancel.as_ref())
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
    fn new(py: Python<'_>, model_dir: &str) -> PyResult<Self> {
        let model_dir = model_dir.to_owned();
        let inner = py
            .detach(move || CrossEncoder::ms_marco_minilm_l6(&model_dir))
            .map_err(to_pyerr)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[getter]
    fn model_id(&self) -> String {
        self.inner.model_id().to_string()
    }

    fn rerank(&self, py: Python<'_>, query: String, passages: Vec<String>) -> PyResult<Vec<f32>> {
        let inner = self.inner.clone();
        py.detach(move || {
            let refs: Vec<&str> = passages.iter().map(String::as_str).collect();
            inner.rerank(&query, &refs)
        })
        .map_err(to_pyerr)
    }

    #[pyo3(signature = (query, passages, cancel_token=None))]
    fn rerank_with_cancel(
        &self,
        py: Python<'_>,
        query: String,
        passages: Vec<String>,
        cancel_token: Option<PyRef<'_, PyCancelToken>>,
    ) -> PyResult<Vec<f32>> {
        let inner = self.inner.clone();
        let cancel = cancel_token.map(|token| token.as_inner().clone());
        py.detach(move || {
            let refs: Vec<&str> = passages.iter().map(String::as_str).collect();
            inner.rerank_with_cancel(&query, &refs, cancel.as_ref())
        })
        .map_err(to_pyerr)
    }
}

/// Deterministic hashed bag-of-words embedder (no models, no network); satisfies
/// the embedder protocol for tests and quickstarts.
#[pyclass(name = "MockEmbedder")]
pub(crate) struct PyMockEmbedder {
    inner: Arc<MockEmbedder>,
}

#[pymethods]
impl PyMockEmbedder {
    #[new]
    #[pyo3(signature = (dim, metric="cosine"))]
    fn new(dim: &Bound<'_, PyAny>, metric: &str) -> PyResult<Self> {
        let dim = extract_embedder_dim(dim)?;
        Ok(Self {
            inner: Arc::new(MockEmbedder::with_metric(
                dim,
                parse_embedding_metric(metric)?,
            )),
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

    #[pyo3(signature = (texts, cancel_token=None))]
    fn embed_with_cancel(
        &self,
        texts: Vec<String>,
        cancel_token: Option<PyRef<'_, PyCancelToken>>,
    ) -> PyResult<Vec<Vec<f32>>> {
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        self.inner
            .embed_with_cancel(&refs, cancel_token.as_deref().map(PyCancelToken::as_inner))
            .map_err(to_pyerr)
    }

    /// Query-side embedding (symmetric for the mock: equals `embed`).
    fn embed_queries(&self, texts: Vec<String>) -> PyResult<Vec<Vec<f32>>> {
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        self.inner.embed_queries(&refs).map_err(to_pyerr)
    }

    #[pyo3(signature = (texts, cancel_token=None))]
    fn embed_queries_with_cancel(
        &self,
        texts: Vec<String>,
        cancel_token: Option<PyRef<'_, PyCancelToken>>,
    ) -> PyResult<Vec<Vec<f32>>> {
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        self.inner
            .embed_queries_with_cancel(&refs, cancel_token.as_deref().map(PyCancelToken::as_inner))
            .map_err(to_pyerr)
    }
}

// ---- result DTOs -----------------------------------------------------------

/// A fetched or recalled atom with explicit stored and query-time metadata.
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
    importance: f32,
    #[pyo3(get)]
    confidence: f32,
    #[pyo3(get)]
    relevance: Option<f32>,
    #[pyo3(get)]
    distance: Option<f32>,
    #[pyo3(get)]
    graph_depth: Option<usize>,
    #[pyo3(get)]
    created_at: i64,
    #[pyo3(get)]
    expires_at: Option<i64>,
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
            importance: h.importance,
            confidence: h.confidence,
            relevance: h.relevance,
            distance: h.distance,
            graph_depth: h.graph_depth,
            created_at: h.created_at,
            expires_at: h.expires_at,
            immutable: h.immutable,
        }
    }
}

/// The stable result of an idempotent remember operation.
#[pyclass(name = "RememberOutcome")]
pub(crate) struct PyRememberOutcome {
    #[pyo3(get)]
    id: i64,
    #[pyo3(get)]
    inserted: bool,
}

impl PyRememberOutcome {
    fn from_outcome(outcome: RememberOutcome) -> Self {
        Self {
            id: outcome.id,
            inserted: outcome.inserted,
        }
    }
}

#[pymethods]
impl PyRememberOutcome {
    fn __repr__(&self) -> String {
        format!(
            "RememberOutcome(id={}, inserted={})",
            self.id, self.inserted
        )
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
            "AtomHit(id={}, kind={:?}, importance={:.4}, relevance={:?})",
            self.id, self.kind, self.importance, self.relevance
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

    /// Atoms whose `expires_at` TTL has lapsed.
    #[staticmethod]
    fn expired() -> Self {
        Self {
            inner: EvictionPolicy::Expired,
        }
    }

    /// Atoms below both thresholds.
    #[staticmethod]
    fn low_importance(importance_threshold: f32, confidence_threshold: f32) -> Self {
        Self {
            inner: EvictionPolicy::LowImportance {
                importance_threshold,
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

/// What a region records about itself, read from its row.
#[pyclass(name = "RegionIdentity")]
pub(crate) struct PyRegionIdentity {
    inner: StoredRegionIdentity,
}

#[pymethods]
impl PyRegionIdentity {
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    #[getter]
    fn dim(&self) -> u16 {
        self.inner.dim()
    }

    #[getter]
    fn metric(&self) -> &'static str {
        embedding_metric_name(self.inner.metric())
    }

    #[getter]
    fn encrypted(&self) -> bool {
        self.inner.encrypted()
    }

    /// The model the region records.
    #[getter]
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }

    fn __repr__(&self) -> String {
        format!(
            "RegionIdentity(name={:?}, dim={}, metric={:?}, encrypted={}, model_id={:?})",
            self.name(),
            self.dim(),
            self.metric(),
            self.encrypted(),
            self.model_id()
        )
    }
}

/// Persisted region inventory returned by `MemoryMaintenance.regions`.
#[pyclass(name = "MemoryRegionInfo")]
pub(crate) struct PyMemoryRegionInfo {
    inner: MemoryRegionInfo,
}

#[pymethods]
impl PyMemoryRegionInfo {
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    #[getter]
    fn dim(&self) -> u16 {
        self.inner.dim()
    }

    #[getter]
    fn metric(&self) -> &'static str {
        embedding_metric_name(self.inner.metric())
    }

    #[getter]
    fn encrypted(&self) -> bool {
        self.inner.encrypted()
    }

    #[getter]
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }

    fn __repr__(&self) -> String {
        format!(
            "MemoryRegionInfo(name={:?}, dim={}, metric={:?}, encrypted={}, model_id={:?})",
            self.name(),
            self.dim(),
            self.metric(),
            self.encrypted(),
            self.model_id()
        )
    }
}

/// One region and its live-atom count from an inventory pass under one
/// key-lifecycle guard.
#[pyclass(name = "MemoryRegionInventory")]
pub(crate) struct PyMemoryRegionInventory {
    inner: MemoryRegionInventory,
}

#[pymethods]
impl PyMemoryRegionInventory {
    #[getter]
    fn region(&self) -> PyMemoryRegionInfo {
        PyMemoryRegionInfo {
            inner: self.inner.region().clone(),
        }
    }

    #[getter]
    fn live_atoms(&self) -> Option<u64> {
        self.inner.live_atoms()
    }

    #[getter]
    fn unavailable(&self) -> Option<&str> {
        self.inner.unavailable()
    }

    fn __repr__(&self) -> String {
        format!(
            "MemoryRegionInventory(region={:?}, live_atoms={:?}, unavailable={:?})",
            self.inner.region().name(),
            self.inner.live_atoms(),
            self.inner.unavailable()
        )
    }
}

/// Report from `Memory.reembed_region`: what the migration did, and did not do.
#[pyclass(name = "ReembedReport")]
pub(crate) struct PyReembedReport {
    inner: ReembedReport,
}

#[pymethods]
impl PyReembedReport {
    #[getter]
    fn atoms_migrated(&self) -> u64 {
        self.inner.atoms_migrated
    }

    #[getter]
    fn model_id(&self) -> String {
        self.inner.model_id.clone()
    }

    #[getter]
    fn ann_rebuilt(&self) -> bool {
        self.inner.ann_rebuilt
    }

    #[getter]
    fn similarity_edges_rewoven(&self) -> u64 {
        self.inner.similarity_edges_rewoven
    }

    #[getter]
    fn similarity_edges_cleared(&self) -> u64 {
        self.inner.similarity_edges_cleared
    }

    fn __repr__(&self) -> String {
        format!(
            "ReembedReport(atoms_migrated={}, model_id={:?}, ann_rebuilt={}, \
             similarity_edges_rewoven={}, similarity_edges_cleared={})",
            self.inner.atoms_migrated,
            self.inner.model_id,
            self.inner.ann_rebuilt,
            self.inner.similarity_edges_rewoven,
            self.inner.similarity_edges_cleared
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
    include_superseded: bool,
}

#[pymethods]
impl PyRecallOptions {
    #[new]
    #[pyo3(signature = (*, payload_filter=None, weights=None, as_of_micros=None, graph_expand=None, include_superseded=false))]
    fn new(
        py: Python<'_>,
        payload_filter: Option<Py<PyAny>>,
        weights: Option<(f32, f32, f32, f32)>,
        as_of_micros: Option<i64>,
        graph_expand: Option<(usize, Vec<String>)>,
        include_superseded: bool,
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
            include_superseded,
        })
    }
}

fn build_recall_query(
    text: Option<String>,
    embedding: Option<Vec<f32>>,
    k: usize,
    kinds: Option<Vec<String>>,
    options: Option<&PyRecallOptions>,
) -> PyResult<RecallQuery> {
    if let Some(embedding) = embedding.as_deref() {
        require_finite("embedding", embedding)?;
    }
    let mut query = match (text, embedding) {
        (Some(text), None) => RecallQuery::by_text(text, k),
        (None, Some(embedding)) => RecallQuery::by_embedding(embedding, k),
        (Some(text), Some(embedding)) => RecallQuery::by_embedding(embedding, k).with_text(text),
        (None, None) => return Err(PyValueError::new_err("text= or embedding= is required")),
    };
    if let Some(kinds) = kinds {
        query = query.with_kinds(kinds);
    }
    if let Some(options) = options {
        if let Some(filter) = &options.payload_filter {
            query = query.with_payload_filter(filter.clone());
        }
        if let Some(weights) = options.weights {
            query = query.with_weights(weights);
        }
        if let Some(as_of_micros) = options.as_of_micros {
            query = query.with_as_of(as_of_micros);
        }
        if let Some(expand) = &options.graph_expand {
            query = query.with_graph_expand(expand.clone());
        }
        if options.include_superseded {
            query = query.with_superseded(true);
        }
    }
    Ok(query)
}

// ---- maintenance -----------------------------------------------------------

/// Model-free inventory, verification, and erasure over existing memory data.
#[pyclass(name = "MemoryMaintenance")]
pub(crate) struct PyMemoryMaintenance {
    inner: Arc<MemoryMaintenance>,
}

impl PyMemoryMaintenance {
    pub(crate) fn open(py: Python<'_>, db: Arc<citadel::Database>) -> PyResult<Self> {
        let inner = py
            .detach(move || MemoryMaintenance::open(db))
            .map_err(to_pyerr)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }
}

#[pymethods]
impl PyMemoryMaintenance {
    /// Persisted region inventory. A row can remain after its content key was erased.
    fn regions(&self, py: Python<'_>) -> PyResult<Vec<PyMemoryRegionInfo>> {
        let maintenance = Arc::clone(&self.inner);
        Ok(py
            .detach(move || maintenance.regions())
            .map_err(to_pyerr)?
            .into_iter()
            .map(|inner| PyMemoryRegionInfo { inner })
            .collect())
    }

    /// Region metadata and counts from one key-lifecycle-guarded inventory pass;
    /// unreadable rows carry an error.
    fn inventory(&self, py: Python<'_>) -> PyResult<Vec<PyMemoryRegionInventory>> {
        let maintenance = Arc::clone(&self.inner);
        Ok(py
            .detach(move || maintenance.inventory())
            .map_err(to_pyerr)?
            .into_iter()
            .map(|inner| PyMemoryRegionInventory { inner })
            .collect())
    }

    /// Count every live, unexpired atom in a region.
    fn count(&self, py: Python<'_>, region: &str) -> PyResult<u64> {
        let maintenance = Arc::clone(&self.inner);
        let region = region.to_owned();
        py.detach(move || maintenance.count_region(&region))
            .map_err(to_pyerr)
    }

    /// Deterministic, non-semantic fetch without attaching an embedder.
    #[pyo3(signature =(region, kind=None, *, payload_filter=None, limit=100, newest=false, after_id=None))]
    #[allow(clippy::too_many_arguments)]
    fn fetch(
        &self,
        py: Python<'_>,
        region: &str,
        kind: Option<&str>,
        payload_filter: Option<Py<PyAny>>,
        limit: usize,
        newest: bool,
        after_id: Option<i64>,
    ) -> PyResult<Vec<PyAtomHit>> {
        let mut query = FetchQuery::new(limit);
        if let Some(kind) = kind {
            query = query.with_kind(kind);
        }
        query.payload_filter = match &payload_filter {
            Some(filter) => Some(py_to_json(py, filter.bind(py))?),
            None => None,
        };
        query.newest = newest;
        query.after_id = after_id;
        let maintenance = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(py
            .detach(move || maintenance.fetch_range(&region, &query))
            .map_err(to_pyerr)?
            .into_iter()
            .map(PyAtomHit::from_hit)
            .collect())
    }

    /// Fetch requested atoms in input order without loading an embedder.
    fn fetch_by_ids(
        &self,
        py: Python<'_>,
        region: &str,
        ids: Vec<i64>,
    ) -> PyResult<Vec<Option<PyAtomHit>>> {
        let maintenance = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(py
            .detach(move || maintenance.fetch_by_ids(&region, &ids))
            .map_err(to_pyerr)?
            .into_iter()
            .map(|hit| hit.map(PyAtomHit::from_hit))
            .collect())
    }

    /// Re-authenticate requested atoms from stored bytes.
    fn verify(
        &self,
        py: Python<'_>,
        region: &str,
        ids: Vec<i64>,
    ) -> PyResult<Vec<PyAtomAttestation>> {
        let maintenance = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(py
            .detach(move || maintenance.verify_atoms(&region, &ids))
            .map_err(to_pyerr)?
            .into_iter()
            .map(|inner| PyAtomAttestation { inner })
            .collect())
    }

    /// Forget atoms without loading the model that produced their vectors.
    #[pyo3(signature = (region, ids, force=false))]
    fn forget(
        &self,
        py: Python<'_>,
        region: &str,
        ids: Vec<i64>,
        force: bool,
    ) -> PyResult<PyErasureReceipt> {
        let maintenance = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(PyErasureReceipt {
            inner: py
                .detach(move || maintenance.forget_atoms(&region, &ids, force))
                .map_err(to_pyerr)?,
        })
    }
}

// ---- the engine ------------------------------------------------------------

/// The memory engine over a `Database`. Obtain via `db.memory()`.
///
/// Shares the engine's database reference and remains usable after the originating
/// Python `Database` handle closes.
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
    fn create_region(
        &self,
        py: Python<'_>,
        name: &str,
        embedder: &Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let engine = Arc::clone(&self.inner);
        let name = name.to_owned();
        let embedder = build_embedder(embedder)?;
        py.detach(move || engine.create_region(&name, embedder))
            .map_err(to_pyerr)
    }

    /// Create an encrypted region (per-atom sealing + crypto-erasure). Requires the
    /// database to have been opened with `region_keys=True`.
    fn create_encrypted_region(
        &self,
        py: Python<'_>,
        name: &str,
        embedder: &Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let engine = Arc::clone(&self.inner);
        let name = name.to_owned();
        let embedder = build_embedder(embedder)?;
        py.detach(move || engine.create_encrypted_region(&name, embedder))
            .map_err(to_pyerr)
    }

    /// Identities for operational regions whose persisted key state is live.
    ///
    /// No embedder or attachment is needed, but encrypted rows still authenticate
    /// their region-key binding. Use `Database.memory_maintenance().regions()` to
    /// inventory rows whose content key may already be unavailable.
    fn regions(&self, py: Python<'_>) -> PyResult<Vec<PyRegionIdentity>> {
        let engine = Arc::clone(&self.inner);
        Ok(py
            .detach(move || engine.stored_region_identities())
            .map_err(to_pyerr)?
            .into_iter()
            .map(|inner| PyRegionIdentity { inner })
            .collect())
    }

    /// One operational region identity, including its live key-state check.
    fn region(&self, py: Python<'_>, name: &str) -> PyResult<Option<PyRegionIdentity>> {
        let engine = Arc::clone(&self.inner);
        let name = name.to_owned();
        py.detach(move || engine.stored_region_identity(&name))
            .map(|identity| identity.map(|inner| PyRegionIdentity { inner }))
            .map_err(to_pyerr)
    }

    /// Attach an existing region bound to `embedder`.
    fn attach_existing_region(
        &self,
        py: Python<'_>,
        name: &str,
        embedder: &Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let engine = Arc::clone(&self.inner);
        let name = name.to_owned();
        let embedder = build_embedder(embedder)?;
        py.detach(move || engine.attach_existing_region(&name, embedder))
            .map_err(to_pyerr)
    }

    fn drop_region(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let engine = Arc::clone(&self.inner);
        let name = name.to_owned();
        py.detach(move || engine.drop_region(&name))
            .map_err(to_pyerr)
    }

    /// Correct persisted model provenance without changing vectors. Use only when
    /// the vectors are already correct and `model_id` is wrong.
    fn reclassify_region(&self, py: Python<'_>, name: &str, model_id: String) -> PyResult<()> {
        let engine = Arc::clone(&self.inner);
        let name = name.to_owned();
        py.detach(move || engine.reclassify_region(&name, model_id))
            .map_err(to_pyerr)
    }

    /// Recompute a region's vectors with a different model, from the stored text.
    ///
    /// For a store whose vectors are wrong, which is the opposite fault to
    /// `reclassify_region`. Atom ids are preserved, so edges, idempotency
    /// records, TTLs, confidence, scores and access counts all survive; only the
    /// vectors change, and the `SimilarTo` web is rebuilt over them.
    ///
    /// Refuses rather than converting a region holding atoms that carry a vector
    /// but no text, since no new vector can be computed for those.
    fn reembed_region(
        &self,
        py: Python<'_>,
        name: &str,
        embedder: &Bound<'_, PyAny>,
    ) -> PyResult<PyReembedReport> {
        let engine = Arc::clone(&self.inner);
        let name = name.to_owned();
        let embedder = build_embedder(embedder)?;
        py.detach(move || engine.reembed_region(&name, embedder, None))
            .map(|inner| PyReembedReport { inner })
            .map_err(to_pyerr)
    }

    /// Remember one atom (a dict with `kind` + `text`, optional `embedding`,
    /// `payload`, `score`, `confidence`, `created_at`, `expires_at`, `immutable`).
    /// A supplied embedding skips passage embedding but is still validated against
    /// the region's dimension and finite-value rules. Returns the atom id.
    fn remember(&self, py: Python<'_>, region: &str, atom: &Bound<'_, PyDict>) -> PyResult<i64> {
        let input = dict_to_atom_input(py, atom)?;
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        py.detach(move || engine.remember(&region, input))
            .map_err(to_pyerr)
    }

    /// Remember one atom under a non-replacing idempotency key. An identical
    /// retry returns the original id with `inserted = false`; changed reuse of
    /// a live key fails without modifying the stored atom.
    fn remember_if_absent_keyed(
        &self,
        py: Python<'_>,
        region: &str,
        atom: &Bound<'_, PyDict>,
        key: &str,
    ) -> PyResult<PyRememberOutcome> {
        let input = dict_to_atom_input(py, atom)?;
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let key = key.to_owned();
        py.detach(move || engine.remember_if_absent_keyed(&region, input, &[], None, &key))
            .map(PyRememberOutcome::from_outcome)
            .map_err(to_pyerr)
    }

    /// Store `(atom, key)` pairs atomically without replacing any live key.
    /// Outcomes preserve input order and distinguish inserts from exact replays.
    fn remember_if_absent_keyed_batch(
        &self,
        py: Python<'_>,
        region: &str,
        entries: Vec<(Py<PyDict>, String)>,
    ) -> PyResult<Vec<PyRememberOutcome>> {
        let inputs = entries
            .iter()
            .map(|(atom, key)| Ok((dict_to_atom_input(py, atom.bind(py))?, key.clone())))
            .collect::<PyResult<Vec<_>>>()?;
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        py.detach(move || engine.remember_if_absent_keyed_batch(&region, inputs))
            .map(|outcomes| {
                outcomes
                    .into_iter()
                    .map(PyRememberOutcome::from_outcome)
                    .collect()
            })
            .map_err(to_pyerr)
    }

    /// Remember one atom as the sole occupant of `key`, superseding whatever atom
    /// that key named before. Insert, rebind and the old row's delete commit
    /// together, so concurrent writers to one key serialize instead of each
    /// inserting. Returns the atom's id; an identical retry returns the stored
    /// one untouched. Keys are scoped to `(region, kind)`.
    fn remember_replacing_keyed(
        &self,
        py: Python<'_>,
        region: &str,
        atom: &Bound<'_, PyDict>,
        key: &str,
    ) -> PyResult<i64> {
        let input = dict_to_atom_input(py, atom)?;
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let key = key.to_owned();
        py.detach(move || engine.remember_replacing_keyed(&region, input, &key))
            .map(|o| o.id)
            .map_err(to_pyerr)
    }

    /// `remember_replacing_keyed` over `(atom, key)` pairs in one transaction,
    /// which keeps bulk ingest at one fsync for the batch rather than one per
    /// atom. Returns their ids, in order. Keys must be distinct within a batch.
    fn remember_replacing_keyed_batch(
        &self,
        py: Python<'_>,
        region: &str,
        entries: Vec<(Py<PyDict>, String)>,
    ) -> PyResult<Vec<i64>> {
        let inputs = entries
            .iter()
            .map(|(atom, key)| Ok((dict_to_atom_input(py, atom.bind(py))?, key.clone())))
            .collect::<PyResult<Vec<_>>>()?;
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        py.detach(move || engine.remember_replacing_keyed_batch(&region, inputs))
            .map(|outs| outs.into_iter().map(|o| o.id).collect())
            .map_err(to_pyerr)
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
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        py.detach(move || engine.remember_batch(&region, inputs))
            .map_err(to_pyerr)
    }

    /// Hybrid recall by `text` (embedded + keyword-ranked) and/or a precomputed
    /// `embedding`; returns the top `k` atoms by fused relevance. `options` carries the
    /// advanced `RecallQuery` modifiers (payload filter, weights, recency, graph).
    #[pyo3(signature = (region, *, text=None, embedding=None, k=10, kinds=None, options=None))]
    // The parameter list is the Python keyword signature; a struct would break it.
    #[allow(clippy::too_many_arguments)]
    fn recall(
        &self,
        py: Python<'_>,
        region: &str,
        text: Option<String>,
        embedding: Option<Vec<f32>>,
        k: usize,
        kinds: Option<Vec<String>>,
        options: Option<&PyRecallOptions>,
    ) -> PyResult<Vec<PyAtomHit>> {
        let query = build_recall_query(text, embedding, k, kinds, options)?;
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(py
            .detach(move || engine.recall(&region, query))
            .map_err(to_pyerr)?
            .into_iter()
            .map(PyAtomHit::from_hit)
            .collect())
    }

    /// Recall atoms and return their induced region-local edge subgraph.
    #[pyo3(signature = (region, *, text=None, embedding=None, k=10, kinds=None, options=None, edge_limit=500))]
    #[allow(clippy::too_many_arguments)]
    fn profile(
        &self,
        py: Python<'_>,
        region: &str,
        text: Option<String>,
        embedding: Option<Vec<f32>>,
        k: usize,
        kinds: Option<Vec<String>>,
        options: Option<&PyRecallOptions>,
        edge_limit: usize,
    ) -> PyResult<Py<PyAny>> {
        let query = build_recall_query(text, embedding, k, kinds, options)?;
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let profile = py
            .detach(move || engine.profile(&region, query, edge_limit))
            .map_err(to_pyerr)?;
        let atoms = profile
            .atoms
            .into_iter()
            .map(PyAtomHit::from_hit)
            .collect::<Vec<_>>();
        let edges = profile
            .edges
            .iter()
            .map(|edge| edge_to_py(py, edge))
            .collect::<PyResult<Vec<_>>>()?;
        let result = PyDict::new(py);
        result.set_item("atoms", atoms)?;
        result.set_item("edges", edges)?;
        result.set_item("edges_truncated", profile.edges_truncated)?;
        result.into_py_any(py)
    }

    /// Non-semantic fetch of a `kind`, optionally narrowed by a JSONB `payload_filter`.
    ///
    /// Always id-ascending; `newest` takes the last `limit` rows, `after_id` pages.
    #[pyo3(signature =(region, kind, *, payload_filter=None, limit=100, newest=false, after_id=None))]
    // The parameter list is the Python keyword signature; a struct would break it.
    #[allow(clippy::too_many_arguments)]
    fn fetch(
        &self,
        py: Python<'_>,
        region: &str,
        kind: &str,
        payload_filter: Option<Py<PyAny>>,
        limit: usize,
        newest: bool,
        after_id: Option<i64>,
    ) -> PyResult<Vec<PyAtomHit>> {
        let mut q = FetchQuery::new(limit).with_kind(kind);
        q.payload_filter = match &payload_filter {
            Some(p) => Some(py_to_json(py, p.bind(py))?),
            None => None,
        };
        q.newest = newest;
        q.after_id = after_id;
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(py
            .detach(move || engine.fetch_range(&region, &q))
            .map_err(to_pyerr)?
            .into_iter()
            .map(PyAtomHit::from_hit)
            .collect())
    }

    fn fetch_one(&self, py: Python<'_>, region: &str, atom_id: i64) -> PyResult<Option<PyAtomHit>> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(py
            .detach(move || engine.fetch_one(&region, atom_id))
            .map_err(to_pyerr)?
            .map(PyAtomHit::from_hit))
    }

    /// Fetch requested atoms in input order; missing atoms are returned as `None`.
    fn fetch_by_ids(
        &self,
        py: Python<'_>,
        region: &str,
        ids: Vec<i64>,
    ) -> PyResult<Vec<Option<PyAtomHit>>> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(py
            .detach(move || engine.fetch_by_ids(&region, &ids))
            .map_err(to_pyerr)?
            .into_iter()
            .map(|hit| hit.map(PyAtomHit::from_hit))
            .collect())
    }

    fn count(&self, py: Python<'_>, region: &str, kind: &str) -> PyResult<u64> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let kind = kind.to_owned();
        py.detach(move || engine.count(&region, &kind))
            .map_err(to_pyerr)
    }

    /// Link two atoms with a typed edge (causes/contradicts/refines/precedes/
    /// supersedes/derived_from/depends_on/similar_to).
    #[pyo3(signature = (region, src, dst, kind, weight=1.0))]
    fn link(
        &self,
        py: Python<'_>,
        region: &str,
        src: i64,
        dst: i64,
        kind: &str,
        weight: f32,
    ) -> PyResult<()> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let kind = parse_edge_kind(kind)?;
        py.detach(move || engine.link_in_region(&region, src, dst, kind, weight))
            .map_err(to_pyerr)
    }

    /// Remove one exact typed edge. Returns false when it does not exist.
    fn unlink(
        &self,
        py: Python<'_>,
        region: &str,
        src: i64,
        dst: i64,
        kind: &str,
    ) -> PyResult<bool> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let kind = parse_edge_kind(kind)?;
        py.detach(move || engine.unlink_in_region(&region, src, dst, kind))
            .map_err(to_pyerr)
    }

    /// Evict atoms by policy; returns the number removed.
    fn evict(&self, py: Python<'_>, region: &str, policy: &PyEvictionPolicy) -> PyResult<u64> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let policy = policy.inner.clone();
        Ok(py
            .detach(move || engine.evict(&region, policy))
            .map_err(to_pyerr)?
            .removed)
    }

    /// Forget atoms, optionally including their transitive provenance dependents.
    ///
    /// Encrypted regions use cryptographic erasure and return its receipt.
    #[pyo3(signature = (region, ids, force=false, cascade_dependents=false))]
    fn forget(
        &self,
        py: Python<'_>,
        region: &str,
        ids: Vec<i64>,
        force: bool,
        cascade_dependents: bool,
    ) -> PyResult<PyErasureReceipt> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(PyErasureReceipt {
            inner: py
                .detach(move || {
                    if cascade_dependents {
                        engine.forget_atoms_with_dependents(&region, &ids, force)
                    } else {
                        engine.forget_atoms(&region, &ids, force)
                    }
                })
                .map_err(to_pyerr)?,
        })
    }

    /// Attest the integrity/origin of atoms (encrypted regions).
    fn verify(
        &self,
        py: Python<'_>,
        region: &str,
        ids: Vec<i64>,
    ) -> PyResult<Vec<PyAtomAttestation>> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(py
            .detach(move || engine.verify_atoms(&region, &ids))
            .map_err(to_pyerr)?
            .into_iter()
            .map(|inner| PyAtomAttestation { inner })
            .collect())
    }

    /// Replace an atom's JSON payload; returns false for an exact no-op.
    fn update_atom_payload(
        &self,
        py: Python<'_>,
        region: &str,
        atom_id: i64,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let json = py_to_json(py, payload)?;
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        Ok(py
            .detach(move || engine.update_atom_payload(&region, atom_id, &json))
            .map_err(to_pyerr)?
            .changed)
    }

    /// The most recently created atom of `kind`, if any.
    fn fetch_last(&self, py: Python<'_>, region: &str, kind: &str) -> PyResult<Option<PyAtomHit>> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let kind = kind.to_owned();
        Ok(py
            .detach(move || engine.fetch_last(&region, &kind))
            .map_err(to_pyerr)?
            .map(PyAtomHit::from_hit))
    }

    /// Read at most `limit` live region-local edges, optionally filtered by
    /// `src`/`dst`/`kind`; each is `{src, dst, kind, weight, evidence}`.
    #[pyo3(signature = (region, *, src=None, dst=None, kind=None, limit=1000))]
    fn fetch_edges(
        &self,
        py: Python<'_>,
        region: &str,
        src: Option<i64>,
        dst: Option<i64>,
        kind: Option<String>,
        limit: usize,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let kind = kind.map(|s| parse_edge_kind(&s)).transpose()?;
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        py.detach(move || engine.fetch_edges_in_region(&region, src, dst, kind, limit))
            .map_err(to_pyerr)?
            .iter()
            .map(|edge| edge_to_py(py, edge))
            .collect()
    }

    /// Recompute an atom's `similar_to` links by ANN search; returns
    /// `{links_added, importance}`.
    fn evolve(
        &self,
        py: Python<'_>,
        region: &str,
        atom_id: i64,
        neighbors: usize,
        max_distance: f32,
    ) -> PyResult<Py<PyAny>> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let r = py
            .detach(move || engine.evolve(&region, atom_id, neighbors, max_distance))
            .map_err(to_pyerr)?;
        let d = PyDict::new(py);
        d.set_item("links_added", r.links_added)?;
        d.set_item("importance", r.importance)?;
        d.into_py_any(py)
    }

    /// One bounded kind-summary page since `since_micros`.
    #[pyo3(signature = (region, since_micros, *, after_kind=None, limit=256))]
    fn summarize(
        &self,
        py: Python<'_>,
        region: &str,
        since_micros: i64,
        after_kind: Option<String>,
        limit: usize,
    ) -> PyResult<Py<PyAny>> {
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let mut query = SummaryQuery::new(since_micros, limit);
        if let Some(after_kind) = after_kind {
            query = query.with_after_kind(after_kind);
        }
        let r = py
            .detach(move || engine.summarize_page(&region, &query))
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
                k.set_item("avg_importance", kd.avg_importance)?;
                k.set_item("avg_confidence", kd.avg_confidence)?;
                k.into_py_any(py)
            })
            .collect::<PyResult<Vec<_>>>()?;
        let d = PyDict::new(py);
        d.set_item("total", r.total)?;
        d.set_item("kinds", kinds)?;
        d.set_item("next_after_kind", r.next_after_kind)?;
        d.into_py_any(py)
    }

    /// Attach a cross-encoder reranker applied in `recall` before truncation.
    /// `reranker` is a built-in `MockReranker`/`CrossEncoder` or any object with
    /// `model_id` + `rerank_with_cancel(query, passages, cancel_token)`. `strategy`
    /// is "rrf" (reciprocal-rank fusion, damping `rrf_k`) or "replace".
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
        let engine = Arc::clone(&self.inner);
        let region = region.to_owned();
        let status = py
            .detach(move || engine.ann_cache_status(&region))
            .map_err(to_pyerr)?;
        match status {
            None => Ok(None),
            Some(src) => Ok(Some(ann_index_source_dict(py, &src)?.into_py_any(py)?)),
        }
    }
}
