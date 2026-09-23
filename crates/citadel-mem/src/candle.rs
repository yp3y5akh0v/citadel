//! Sentence embeddings via Candle (feature `candle-embed`), over two encoder
//! backbones: classic BERT (BGE, MiniLM, E5) and ModernBERT (IBM granite-r2).
//! Pooling is model-specific: BGE/granite use CLS, MiniLM/E5 use masked mean.

use std::path::Path;

use candle_core::{Device, Tensor};
use candle_nn::{Linear, Module, VarBuilder};
use candle_transformers::models::bert::{BertModel, Config, DTYPE};
use candle_transformers::models::modernbert::{Config as ModernBertConfig, ModernBert};
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

use citadel_core::CancelToken;

use crate::embed::{
    check_cancel, normalize_model_id_label, EmbedError, Embedder, EmbeddingMetric, Reranker,
};

fn backend(e: impl std::fmt::Display) -> EmbedError {
    EmbedError::Backend(e.to_string())
}

/// Reject non-finite model output at the source; a NaN that reaches storage
/// otherwise surfaces as a distant index/quantizer panic.
fn ensure_finite(values: &[f32], model_id: &str) -> Result<(), EmbedError> {
    if values.iter().all(|v| v.is_finite()) {
        Ok(())
    } else {
        Err(EmbedError::Backend(format!(
            "{model_id}: model produced a non-finite output"
        )))
    }
}

/// Micro-batch size for length-bucketed encoding; inputs are length-sorted so
/// padding tracks each chunk (attention cost is ~length squared).
const MICRO_BATCH: usize = 16;

#[cfg(test)]
std::thread_local! {
    static CANCEL_AFTER_TOKENIZATION: std::cell::RefCell<Option<CancelToken>> = const {
        std::cell::RefCell::new(None)
    };
    static CANCEL_AFTER_MODEL_FORWARD: std::cell::RefCell<Option<CancelToken>> = const {
        std::cell::RefCell::new(None)
    };
}

#[cfg(test)]
fn cancel_after_tokenization() {
    CANCEL_AFTER_TOKENIZATION.with(|slot| {
        if let Some(token) = slot.borrow_mut().take() {
            token.cancel();
        }
    });
}

#[cfg(test)]
fn cancel_after_model_forward() {
    CANCEL_AFTER_MODEL_FORWARD.with(|slot| {
        if let Some(token) = slot.borrow_mut().take() {
            token.cancel();
        }
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pooling {
    /// First-token (`[CLS]`) hidden state. Used by BGE and granite models.
    Cls,
    /// Attention-masked mean over tokens. Used by MiniLM and E5.
    Mean,
}

/// Encoder architecture behind a model - selects the `config.json` shape and
/// forward signature (BERT takes token-type ids; ModernBERT uses rotary/local
/// attention instead).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arch {
    Bert,
    ModernBert,
}

/// Settings for [`CandleEmbedder::from_dir`] (`dim` comes from the model
/// config).
#[derive(Debug, Clone)]
pub struct CandleConfig {
    /// Human-readable label. The loaded embedder appends a content fingerprint
    /// to make its durable [`Embedder::model_id`] artifact-specific.
    pub model_id: String,
    pub arch: Arch,
    pub metric: EmbeddingMetric,
    pub pooling: Pooling,
    /// L2-normalize each output vector (required for cosine similarity).
    pub normalize: bool,
    /// Prepended to stored texts before encoding (e.g. E5's `"passage: "`).
    pub passage_prefix: Option<String>,
    /// Prepended to search queries before encoding (e.g. E5's `"query: "`).
    pub query_prefix: Option<String>,
    pub max_length: usize,
}

impl CandleConfig {
    /// `BAAI/bge-small-en-v1.5` / `bge-base-en-v1.5` (cosine, CLS pooling).
    pub fn bge_small() -> Self {
        Self {
            model_id: "bge-small-en-v1.5".into(),
            arch: Arch::Bert,
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Cls,
            normalize: true,
            passage_prefix: None,
            query_prefix: None,
            max_length: 512,
        }
    }

    /// `BAAI/bge-base-en-v1.5` (768d, cosine, CLS pooling).
    pub fn bge_base() -> Self {
        Self {
            model_id: "bge-base-en-v1.5".into(),
            ..Self::bge_small()
        }
    }

    /// `BAAI/bge-large-en-v1.5` (1024d, cosine, CLS pooling).
    pub fn bge_large() -> Self {
        Self {
            model_id: "bge-large-en-v1.5".into(),
            ..Self::bge_small()
        }
    }

    /// `sentence-transformers/all-MiniLM-L6-v2` (384d, cosine, mean pooling).
    pub fn minilm_l6() -> Self {
        Self {
            model_id: "all-MiniLM-L6-v2".into(),
            arch: Arch::Bert,
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Mean,
            normalize: true,
            passage_prefix: None,
            query_prefix: None,
            max_length: 256,
        }
    }

    /// `intfloat/e5-large` (v1) - recommended (1024d, cosine, mean). E5 is
    /// asymmetric: stored texts MUST carry `"passage: "` and queries
    /// `"query: "`, or retrieval collapses.
    pub fn e5_large() -> Self {
        Self {
            model_id: "e5-large".into(),
            arch: Arch::Bert,
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Mean,
            normalize: true,
            passage_prefix: Some("passage: ".into()),
            query_prefix: Some("query: ".into()),
            max_length: 512,
        }
    }

    /// `intfloat/e5-large-v2` - the later retrain, selectable but not the default.
    pub fn e5_large_v2() -> Self {
        Self {
            model_id: "e5-large-v2".into(),
            ..Self::e5_large()
        }
    }

    /// `ibm-granite/granite-embedding-english-r2` (768d, cosine, CLS, no
    /// prefixes - symmetric). ModernBERT + byte-BPE tokenizer: 0% UNK on
    /// symbol-dense text (Lean) where a BERT vocab maps math symbols to [UNK].
    pub fn granite_r2() -> Self {
        Self {
            model_id: "granite-embedding-english-r2".into(),
            arch: Arch::ModernBert,
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Cls,
            normalize: true,
            passage_prefix: None,
            query_prefix: None,
            max_length: 512,
        }
    }

    /// `Snowflake/snowflake-arctic-embed-l` / `-m-v1.5` (BERT, CLS). Retrieval
    /// needs the query instruction; documents carry no prefix.
    pub fn arctic() -> Self {
        Self {
            model_id: "snowflake-arctic-embed".into(),
            arch: Arch::Bert,
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Cls,
            normalize: true,
            passage_prefix: None,
            query_prefix: Some("Represent this sentence for searching relevant passages: ".into()),
            max_length: 512,
        }
    }

    /// `nomic-ai/modernbert-embed-base` (ModernBERT, mean pooling, asymmetric
    /// `search_query:` / `search_document:` prefixes).
    pub fn modernbert_embed() -> Self {
        Self {
            model_id: "modernbert-embed-base".into(),
            arch: Arch::ModernBert,
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Mean,
            normalize: true,
            passage_prefix: Some("search_document: ".into()),
            query_prefix: Some("search_query: ".into()),
            max_length: 512,
        }
    }
}

/// `v1` freezes the fingerprint encoding. The pipeline revision is separate:
/// bump it when inference or preprocessing semantics can change for identical
/// artifacts and settings.
const CANDLE_FINGERPRINT_CONTEXT: &str = "CitadelDB Candle embedding pipeline identity v1";
const CANDLE_PIPELINE_REVISION: u64 = 1;

fn fingerprint_field(hasher: &mut blake3::Hasher, tag: u8, bytes: &[u8]) {
    let len = u64::try_from(bytes.len()).expect("fingerprint field length fits u64");
    hasher.update(&[tag]);
    hasher.update(&len.to_le_bytes());
    hasher.update(bytes);
}

fn fingerprint_optional_field(hasher: &mut blake3::Hasher, tag: u8, value: Option<&str>) {
    let value_len = value.map_or(0, str::len);
    let len = u64::try_from(value_len)
        .expect("fingerprint field length fits u64")
        .checked_add(1)
        .expect("fingerprint option length fits u64");
    hasher.update(&[tag]);
    hasher.update(&len.to_le_bytes());
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(value.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn candle_pipeline_fingerprint_with_revision(
    config_json: &[u8],
    tokenizer_json: &[u8],
    weights: &[u8],
    cfg: &CandleConfig,
    pipeline_revision: u64,
) -> [u8; 32] {
    let CandleConfig {
        model_id: _,
        arch,
        metric,
        pooling,
        normalize,
        passage_prefix,
        query_prefix,
        max_length,
    } = cfg;
    let arch = match arch {
        Arch::Bert => 1,
        Arch::ModernBert => 2,
    };
    let metric = match metric {
        EmbeddingMetric::Cosine => 1,
        EmbeddingMetric::L2 => 2,
        EmbeddingMetric::InnerProduct => 3,
    };
    let pooling = match pooling {
        Pooling::Cls => 1,
        Pooling::Mean => 2,
    };
    let max_length = u64::try_from(*max_length).expect("maximum token length fits u64");
    let micro_batch = u64::try_from(MICRO_BATCH).expect("micro-batch size fits u64");

    let mut hasher = blake3::Hasher::new_derive_key(CANDLE_FINGERPRINT_CONTEXT);
    fingerprint_field(&mut hasher, 1, &pipeline_revision.to_le_bytes());
    fingerprint_field(&mut hasher, 2, config_json);
    fingerprint_field(&mut hasher, 3, tokenizer_json);
    fingerprint_field(&mut hasher, 4, weights);
    fingerprint_field(&mut hasher, 5, &[arch]);
    fingerprint_field(&mut hasher, 6, &[metric]);
    fingerprint_field(&mut hasher, 7, &[pooling]);
    fingerprint_field(&mut hasher, 8, &[u8::from(*normalize)]);
    fingerprint_optional_field(&mut hasher, 9, passage_prefix.as_deref());
    fingerprint_optional_field(&mut hasher, 10, query_prefix.as_deref());
    fingerprint_field(&mut hasher, 11, &max_length.to_le_bytes());
    fingerprint_field(&mut hasher, 12, &micro_batch.to_le_bytes());
    fingerprint_field(&mut hasher, 13, b"f32");
    *hasher.finalize().as_bytes()
}

fn candle_pipeline_fingerprint(
    config_json: &[u8],
    tokenizer_json: &[u8],
    weights: &[u8],
    cfg: &CandleConfig,
) -> [u8; 32] {
    candle_pipeline_fingerprint_with_revision(
        config_json,
        tokenizer_json,
        weights,
        cfg,
        CANDLE_PIPELINE_REVISION,
    )
}

fn content_bound_model_id(label: &str, fingerprint: [u8; 32], pipeline_revision: u64) -> String {
    let fingerprint = blake3::Hash::from_bytes(fingerprint);
    format!("{label}@citadel-candle-v1-p{pipeline_revision}:{fingerprint}")
}

enum ParsedEncoderConfig {
    Bert(Config),
    ModernBert(ModernBertConfig),
}

/// The loaded encoder. BERT consumes `(ids, type_ids, mask)`; ModernBERT has no
/// token-type embedding and consumes `(ids, mask)`. Boxed: the BERT struct is
/// several times the ModernBERT one (clippy::large_enum_variant).
enum Backbone {
    Bert(Box<BertModel>),
    ModernBert(Box<ModernBert>),
}

impl Backbone {
    fn forward(
        &self,
        ids: &Tensor,
        type_ids: &Tensor,
        mask: &Tensor,
    ) -> candle_core::Result<Tensor> {
        match self {
            Backbone::Bert(m) => m.forward(ids, type_ids, Some(mask)),
            Backbone::ModernBert(m) => m.forward(ids, mask),
        }
    }
}

/// A local sentence-embedding model (Candle backend).
pub struct CandleEmbedder {
    model: Backbone,
    tokenizer: Tokenizer,
    device: Device,
    dim: usize,
    metric: EmbeddingMetric,
    pooling: Pooling,
    normalize: bool,
    passage_prefix: Option<String>,
    query_prefix: Option<String>,
    model_id: String,
}

/// Use the explicitly compiled inference backend. CUDA initialization errors
/// propagate to the caller; CPU inference requires a build without `cuda-embed`.
#[cfg(feature = "cuda-embed")]
fn select_device() -> Result<Device, EmbedError> {
    let device = Device::new_cuda(0)
        .map_err(|error| EmbedError::Backend(format!("cuda-embed requires CUDA GPU 0: {error}")))?;
    eprintln!("[citadel-mem] cuda-embed: using CUDA GPU 0");
    // Candle's f32 GEMM mode is process-global. Both inference constructors
    // establish the same CUDA policy, independent of which model loads first.
    // Standalone reranking uses the embedder-plus-reranker TF32 policy too.
    candle_core::cuda::set_gemm_reduced_precision_f32(true);
    eprintln!("[citadel-mem] cuda-embed: TF32 f32 GEMM enabled (tensor cores)");
    Ok(device)
}

#[cfg(not(feature = "cuda-embed"))]
fn select_device() -> Result<Device, EmbedError> {
    Ok(Device::Cpu)
}

impl CandleEmbedder {
    /// Load from raw model bytes (no filesystem; the wasm/browser loader).
    pub fn from_bytes(
        config_json: &[u8],
        tokenizer_json: &[u8],
        weights: Vec<u8>,
        cfg: CandleConfig,
    ) -> Result<Self, EmbedError> {
        let model_label = normalize_model_id_label(&cfg.model_id).map_err(EmbedError::Backend)?;
        let mut tokenizer = Tokenizer::from_bytes(tokenizer_json).map_err(backend)?;
        let parsed_config = match cfg.arch {
            Arch::Bert => {
                ParsedEncoderConfig::Bert(serde_json::from_slice(config_json).map_err(backend)?)
            }
            Arch::ModernBert => ParsedEncoderConfig::ModernBert(
                serde_json::from_slice(config_json).map_err(backend)?,
            ),
        };
        let fingerprint = candle_pipeline_fingerprint(config_json, tokenizer_json, &weights, &cfg);
        let model_id = content_bound_model_id(&model_label, fingerprint, CANDLE_PIPELINE_REVISION);

        let device = select_device()?;

        let mut padding = PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..Default::default()
        };
        let vb = VarBuilder::from_buffered_safetensors(weights, DTYPE, &device).map_err(backend)?;
        let (model, dim) = match parsed_config {
            ParsedEncoderConfig::Bert(config) => {
                let dim = config.hidden_size;
                (
                    Backbone::Bert(Box::new(BertModel::load(vb, &config).map_err(backend)?)),
                    dim,
                )
            }
            ParsedEncoderConfig::ModernBert(config) => {
                // ModernBERT vocabs don't put [PAD] at id 0; take it from the
                // config.
                padding.pad_id = config.pad_token_id;
                if let Some(tok) = tokenizer.id_to_token(config.pad_token_id) {
                    padding.pad_token = tok;
                }
                let dim = config.hidden_size;
                // Candle's loader addresses the backbone under a `model.`
                // prefix (the ForMaskedLM layout); a bare ModernBertModel
                // checkpoint (granite-r2) stores tensors unprefixed - strip it
                // on lookup.
                let vb = if vb.contains_tensor("model.embeddings.tok_embeddings.weight") {
                    vb
                } else {
                    vb.rename_f(|name: &str| {
                        name.strip_prefix("model.").unwrap_or(name).to_string()
                    })
                };
                (
                    Backbone::ModernBert(Box::new(ModernBert::load(vb, &config).map_err(backend)?)),
                    dim,
                )
            }
        };
        tokenizer.with_padding(Some(padding));
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: cfg.max_length,
                ..Default::default()
            }))
            .map_err(backend)?;

        Ok(Self {
            model,
            tokenizer,
            device,
            dim,
            metric: cfg.metric,
            pooling: cfg.pooling,
            normalize: cfg.normalize,
            passage_prefix: cfg.passage_prefix,
            query_prefix: cfg.query_prefix,
            model_id,
        })
    }

    /// Load from a directory of `config.json` / `tokenizer.json` /
    /// `model.safetensors`.
    pub fn from_dir(dir: impl AsRef<Path>, cfg: CandleConfig) -> Result<Self, EmbedError> {
        let dir = dir.as_ref();
        let config = std::fs::read(dir.join("config.json")).map_err(backend)?;
        let tokenizer = std::fs::read(dir.join("tokenizer.json")).map_err(backend)?;
        let weights = std::fs::read(dir.join("model.safetensors")).map_err(backend)?;
        Self::from_bytes(&config, &tokenizer, weights, cfg)
    }

    /// `BAAI/bge-small-en-v1.5` from a directory.
    pub fn bge_small(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, CandleConfig::bge_small())
    }

    /// `BAAI/bge-base-en-v1.5` from a directory.
    pub fn bge_base(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, CandleConfig::bge_base())
    }

    /// `BAAI/bge-large-en-v1.5` from a directory.
    pub fn bge_large(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, CandleConfig::bge_large())
    }

    /// `sentence-transformers/all-MiniLM-L6-v2` from a directory.
    pub fn minilm_l6(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, CandleConfig::minilm_l6())
    }

    /// `intfloat/e5-large` (the original v1) from a directory.
    pub fn e5_large(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, CandleConfig::e5_large())
    }

    /// `intfloat/e5-large-v2` from a directory.
    pub fn e5_large_v2(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, CandleConfig::e5_large_v2())
    }

    /// `ibm-granite/granite-embedding-english-r2` from a directory.
    pub fn granite_r2(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, CandleConfig::granite_r2())
    }

    /// `Snowflake/snowflake-arctic-embed-l` / `-m-v1.5` from a directory.
    pub fn arctic(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, CandleConfig::arctic())
    }

    /// `nomic-ai/modernbert-embed-base` from a directory.
    pub fn modernbert_embed(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, CandleConfig::modernbert_embed())
    }

    /// Embed each text (rows in input order), length-bucketed so padding tracks
    /// each chunk; masked padding keeps outputs identical to a single batch.
    /// Encoding stays sequential to avoid rayon contention with concurrent
    /// recalls.
    fn run(
        &self,
        texts: &[&str],
        prefix: Option<&str>,
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        check_cancel(cancel)?;
        let mut encodings = Vec::with_capacity(texts.len());
        for t in texts {
            check_cancel(cancel)?;
            let input = match prefix {
                Some(p) => format!("{p}{t}"),
                None => (*t).to_string(),
            };
            encodings.push(self.tokenizer.encode(input, true).map_err(backend)?);
            #[cfg(test)]
            cancel_after_tokenization();
            check_cancel(cancel)?;
        }

        let mut order: Vec<usize> = (0..encodings.len()).collect();
        order.sort_by_key(|&i| encodings[i].get_ids().len());

        let mut rows: Vec<Vec<f32>> = vec![Vec::new(); encodings.len()];
        for chunk in order.chunks(MICRO_BATCH) {
            check_cancel(cancel)?;
            let seq = chunk
                .iter()
                .map(|&i| encodings[i].get_ids().len())
                .max()
                .unwrap_or(0);
            let bsz = chunk.len();
            let mut ids = Vec::with_capacity(bsz * seq);
            let mut type_ids = Vec::with_capacity(bsz * seq);
            let mut mask = Vec::with_capacity(bsz * seq);
            for &i in chunk {
                let enc = &encodings[i];
                let mut row_ids = enc.get_ids().to_vec();
                row_ids.resize(seq, 0);
                ids.extend_from_slice(&row_ids);
                let mut row_types = enc.get_type_ids().to_vec();
                row_types.resize(seq, 0);
                type_ids.extend_from_slice(&row_types);
                let mut row_mask: Vec<f32> =
                    enc.get_attention_mask().iter().map(|&m| m as f32).collect();
                row_mask.resize(seq, 0.0);
                mask.extend_from_slice(&row_mask);
            }

            let input_ids = Tensor::from_vec(ids, (bsz, seq), &self.device).map_err(backend)?;
            let type_ids = Tensor::from_vec(type_ids, (bsz, seq), &self.device).map_err(backend)?;
            let attn = Tensor::from_vec(mask, (bsz, seq), &self.device).map_err(backend)?;

            check_cancel(cancel)?;
            let hidden = self
                .model
                .forward(&input_ids, &type_ids, &attn)
                .map_err(backend)?;
            #[cfg(test)]
            cancel_after_model_forward();
            check_cancel(cancel)?;
            let pooled = match self.pooling {
                Pooling::Cls => cls_pool(&hidden).map_err(backend)?,
                Pooling::Mean => masked_mean_pool(&hidden, &attn).map_err(backend)?,
            };
            let out = if self.normalize {
                l2_normalize(&pooled).map_err(backend)?
            } else {
                pooled
            };
            let chunk_rows = out
                .contiguous()
                .and_then(|out| out.to_vec2::<f32>())
                .map_err(backend)?;
            for (pos, &i) in chunk.iter().enumerate() {
                rows[i].clone_from(&chunk_rows[pos]);
            }
            check_cancel(cancel)?;
        }
        check_cancel(cancel)?;
        Ok(rows)
    }
}

impl Embedder for CandleEmbedder {
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
        check_cancel(cancel)?;
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self.run(texts, self.passage_prefix.as_deref(), cancel)?;
        for row in &rows {
            check_cancel(cancel)?;
            ensure_finite(row, &self.model_id)?;
        }
        check_cancel(cancel)?;
        Ok(rows)
    }

    fn embed_queries_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        check_cancel(cancel)?;
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self.run(texts, self.query_prefix.as_deref(), cancel)?;
        for row in &rows {
            check_cancel(cancel)?;
            ensure_finite(row, &self.model_id)?;
        }
        check_cancel(cancel)?;
        Ok(rows)
    }
}

/// BERT cross-encoder (one-logit classifier) for reranking; the logit is the
/// `(query, passage)` relevance score (monotonic, so no sigmoid).
pub struct CrossEncoder {
    model: BertModel,
    pooler: Linear,
    classifier: Linear,
    tokenizer: Tokenizer,
    device: Device,
    model_id: String,
}

impl CrossEncoder {
    /// Load from raw model bytes; `max_length` caps the `(query, passage)`
    /// token length.
    pub fn from_bytes(
        config_json: &[u8],
        tokenizer_json: &[u8],
        weights: Vec<u8>,
        model_id: impl Into<String>,
        max_length: usize,
    ) -> Result<Self, EmbedError> {
        let device = select_device()?;
        let config: Config = serde_json::from_slice(config_json).map_err(backend)?;
        let hidden = config.hidden_size;

        let mut tokenizer = Tokenizer::from_bytes(tokenizer_json).map_err(backend)?;
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length,
                ..Default::default()
            }))
            .map_err(backend)?;

        let vb = VarBuilder::from_buffered_safetensors(weights, DTYPE, &device).map_err(backend)?;
        // HF BertForSequenceClassification nests the encoder under `bert`.
        let model = BertModel::load(vb.pp("bert"), &config).map_err(backend)?;
        let pooler = candle_nn::linear(hidden, hidden, vb.pp("bert").pp("pooler").pp("dense"))
            .map_err(backend)?;
        let classifier = candle_nn::linear(hidden, 1, vb.pp("classifier")).map_err(backend)?;

        Ok(Self {
            model,
            pooler,
            classifier,
            tokenizer,
            device,
            model_id: model_id.into(),
        })
    }

    /// Load from a directory of `config.json` / `tokenizer.json` /
    /// `model.safetensors`.
    pub fn from_dir(
        dir: impl AsRef<Path>,
        model_id: impl Into<String>,
        max_length: usize,
    ) -> Result<Self, EmbedError> {
        let dir = dir.as_ref();
        let config = std::fs::read(dir.join("config.json")).map_err(backend)?;
        let tokenizer = std::fs::read(dir.join("tokenizer.json")).map_err(backend)?;
        let weights = std::fs::read(dir.join("model.safetensors")).map_err(backend)?;
        Self::from_bytes(&config, &tokenizer, weights, model_id, max_length)
    }

    /// `cross-encoder/ms-marco-MiniLM-L-6-v2` from a directory (1 logit,
    /// 512-token pairs).
    pub fn ms_marco_minilm_l6(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, "ms-marco-MiniLM-L-6-v2", 512)
    }

    /// Score every `(query, passage)` pair: one relevance logit per passage.
    /// Length-bucketed micro-batches keep padding near each chunk's real
    /// length. Tokenization stays sequential: `encode_batch`'s rayon fan-out
    /// contends with concurrent recalls.
    fn run(
        &self,
        query: &str,
        passages: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<f32>, EmbedError> {
        check_cancel(cancel)?;
        let mut encodings = Vec::with_capacity(passages.len());
        for p in passages {
            check_cancel(cancel)?;
            encodings.push(self.tokenizer.encode((query, *p), true).map_err(backend)?);
            #[cfg(test)]
            cancel_after_tokenization();
            check_cancel(cancel)?;
        }

        // Process short pairs together and long pairs together: sorting by
        // token length keeps each micro-batch's padding near its real content
        // length.
        let mut order: Vec<usize> = (0..encodings.len()).collect();
        order.sort_by_key(|&i| encodings[i].get_ids().len());

        let mut scores = vec![0f32; encodings.len()];
        for chunk in order.chunks(MICRO_BATCH) {
            check_cancel(cancel)?;
            let seq = chunk
                .iter()
                .map(|&i| encodings[i].get_ids().len())
                .max()
                .unwrap_or(0);
            let bsz = chunk.len();
            let mut ids = Vec::with_capacity(bsz * seq);
            let mut type_ids = Vec::with_capacity(bsz * seq);
            let mut mask = Vec::with_capacity(bsz * seq);
            for &i in chunk {
                let enc = &encodings[i];
                let mut row_ids = enc.get_ids().to_vec();
                row_ids.resize(seq, 0);
                ids.extend_from_slice(&row_ids);
                let mut row_types = enc.get_type_ids().to_vec();
                row_types.resize(seq, 0);
                type_ids.extend_from_slice(&row_types);
                let mut row_mask: Vec<f32> =
                    enc.get_attention_mask().iter().map(|&m| m as f32).collect();
                row_mask.resize(seq, 0.0);
                mask.extend_from_slice(&row_mask);
            }

            let input_ids = Tensor::from_vec(ids, (bsz, seq), &self.device).map_err(backend)?;
            let type_ids = Tensor::from_vec(type_ids, (bsz, seq), &self.device).map_err(backend)?;
            let attn = Tensor::from_vec(mask, (bsz, seq), &self.device).map_err(backend)?;

            check_cancel(cancel)?;
            let hidden = self
                .model
                .forward(&input_ids, &type_ids, Some(&attn))
                .map_err(backend)?;
            #[cfg(test)]
            cancel_after_model_forward();
            check_cancel(cancel)?;
            // narrow+squeeze is non-contiguous; candle's CUDA matmul needs
            // contiguous.
            let cls = hidden
                .narrow(1, 0, 1)
                .and_then(|hidden| hidden.squeeze(1))
                .and_then(|hidden| hidden.contiguous())
                .map_err(backend)?;
            let pooled = self
                .pooler
                .forward(&cls)
                .and_then(|pooled| pooled.tanh())
                .map_err(backend)?;
            let logits = self.classifier.forward(&pooled).map_err(backend)?;
            let batch_scores = logits
                .squeeze(1)
                .and_then(|logits| logits.to_vec1::<f32>())
                .map_err(backend)?;
            for (pos, &i) in chunk.iter().enumerate() {
                scores[i] = batch_scores[pos];
            }
            check_cancel(cancel)?;
        }
        check_cancel(cancel)?;
        Ok(scores)
    }
}

impl Reranker for CrossEncoder {
    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn rerank_with_cancel(
        &self,
        query: &str,
        passages: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<f32>, EmbedError> {
        check_cancel(cancel)?;
        if passages.is_empty() {
            return Ok(Vec::new());
        }
        let scores = self.run(query, passages, cancel)?;
        check_cancel(cancel)?;
        ensure_finite(&scores, &self.model_id)?;
        check_cancel(cancel)?;
        Ok(scores)
    }
}

/// Masked mean over tokens; denominator floored so all-padding rows don't
/// divide by zero.
fn masked_mean_pool(hidden: &Tensor, mask: &Tensor) -> candle_core::Result<Tensor> {
    let mask3 = mask.unsqueeze(2)?; // [b, seq, 1]
    let summed = hidden.broadcast_mul(&mask3)?.sum(1)?; // [b, h]
    let counts = mask.sum_keepdim(1)?.clamp(1e-9f64, f64::INFINITY)?; // [b, 1]
    summed.broadcast_div(&counts)
}

fn cls_pool(hidden: &Tensor) -> candle_core::Result<Tensor> {
    hidden.narrow(1, 0, 1)?.squeeze(1)
}

/// Row-wise L2 normalization; norm floored to avoid dividing by zero.
fn l2_normalize(t: &Tensor) -> candle_core::Result<Tensor> {
    let norm = t
        .sqr()?
        .sum_keepdim(1)?
        .sqrt()?
        .clamp(1e-12f64, f64::INFINITY)?;
    t.broadcast_div(&norm)
}

#[cfg(test)]
mod tests {
    use super::*;
    use candle_nn::{VarBuilder, VarMap};
    use tokenizers::models::wordpiece::WordPieceBuilder;
    use tokenizers::pre_tokenizers::whitespace::Whitespace;

    fn approx(a: f32, b: f32, eps: f32) -> bool {
        (a - b).abs() < eps
    }

    fn fingerprint_fixture() -> CandleConfig {
        CandleConfig {
            model_id: "fixture-label".into(),
            arch: Arch::Bert,
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Mean,
            normalize: true,
            passage_prefix: Some("passage: ".into()),
            query_prefix: Some("query: ".into()),
            max_length: 384,
        }
    }

    #[test]
    fn pipeline_fingerprint_has_a_stable_known_answer() {
        let cfg = fingerprint_fixture();
        let fingerprint = candle_pipeline_fingerprint(
            b"{\"hidden_size\":32}",
            b"{\"tokenizer\":\"wordpiece\"}",
            b"fixed safetensors bytes\0\xff",
            &cfg,
        );
        assert_eq!(
            fingerprint,
            [
                9, 214, 58, 246, 145, 158, 91, 166, 93, 14, 8, 154, 70, 33, 81, 188, 134, 177, 214,
                181, 68, 226, 33, 13, 226, 38, 28, 101, 15, 204, 199, 16,
            ],
            "changing fingerprint framing or pipeline policy requires a new scheme or revision"
        );
        assert_eq!(
            content_bound_model_id("fixture-label", fingerprint, CANDLE_PIPELINE_REVISION),
            "fixture-label@citadel-candle-v1-p1:09d63af6919e5ba65d0e089a462151bc86b1d6b544e2210de2261c650fccc710"
        );
    }

    #[test]
    fn every_pipeline_input_perturbs_the_fingerprint() {
        let config = b"config";
        let tokenizer = b"tokenizer";
        let weights = b"weights";
        let cfg = fingerprint_fixture();
        let base = candle_pipeline_fingerprint(config, tokenizer, weights, &cfg);

        let mut variants = Vec::new();
        variants.push(candle_pipeline_fingerprint(
            b"config changed",
            tokenizer,
            weights,
            &cfg,
        ));
        variants.push(candle_pipeline_fingerprint(
            config,
            b"tokenizer changed",
            weights,
            &cfg,
        ));
        variants.push(candle_pipeline_fingerprint(
            config,
            tokenizer,
            b"weights changed",
            &cfg,
        ));

        let mut changed = cfg.clone();
        changed.arch = Arch::ModernBert;
        variants.push(candle_pipeline_fingerprint(
            config, tokenizer, weights, &changed,
        ));

        let metric_hashes = [
            EmbeddingMetric::Cosine,
            EmbeddingMetric::L2,
            EmbeddingMetric::InnerProduct,
        ]
        .map(|metric| {
            let mut changed = cfg.clone();
            changed.metric = metric;
            candle_pipeline_fingerprint(config, tokenizer, weights, &changed)
        });
        for (index, left) in metric_hashes.iter().enumerate() {
            for right in &metric_hashes[index + 1..] {
                assert_ne!(left, right, "every metric needs a unique tag");
            }
        }
        variants.extend(metric_hashes[1..].iter().copied());

        let mut changed = cfg.clone();
        changed.pooling = Pooling::Cls;
        variants.push(candle_pipeline_fingerprint(
            config, tokenizer, weights, &changed,
        ));
        let mut changed = cfg.clone();
        changed.normalize = false;
        variants.push(candle_pipeline_fingerprint(
            config, tokenizer, weights, &changed,
        ));
        let mut changed = cfg.clone();
        changed.passage_prefix = Some("document: ".into());
        variants.push(candle_pipeline_fingerprint(
            config, tokenizer, weights, &changed,
        ));
        let mut changed = cfg.clone();
        changed.query_prefix = Some("search: ".into());
        variants.push(candle_pipeline_fingerprint(
            config, tokenizer, weights, &changed,
        ));
        let mut changed = cfg.clone();
        changed.max_length += 1;
        variants.push(candle_pipeline_fingerprint(
            config, tokenizer, weights, &changed,
        ));
        variants.push(candle_pipeline_fingerprint_with_revision(
            config,
            tokenizer,
            weights,
            &cfg,
            CANDLE_PIPELINE_REVISION + 1,
        ));

        for (index, variant) in variants.iter().enumerate() {
            assert_ne!(
                *variant, base,
                "pipeline input {index} was not fingerprinted"
            );
        }
    }

    #[test]
    fn fingerprint_frames_fields_and_option_presence() {
        let cfg = fingerprint_fixture();
        assert_ne!(
            candle_pipeline_fingerprint(b"a", b"bc", b"weights", &cfg),
            candle_pipeline_fingerprint(b"ab", b"c", b"weights", &cfg),
            "artifact boundaries must not alias"
        );

        let mut left = cfg.clone();
        left.passage_prefix = Some("a".into());
        left.query_prefix = Some("bc".into());
        let mut right = cfg.clone();
        right.passage_prefix = Some("ab".into());
        right.query_prefix = Some("c".into());
        assert_ne!(
            candle_pipeline_fingerprint(b"c", b"t", b"w", &left),
            candle_pipeline_fingerprint(b"c", b"t", b"w", &right),
            "prefix boundaries must not alias"
        );

        let mut none = cfg.clone();
        none.passage_prefix = None;
        let mut empty = cfg;
        empty.passage_prefix = Some(String::new());
        assert_ne!(
            candle_pipeline_fingerprint(b"c", b"t", b"w", &none),
            candle_pipeline_fingerprint(b"c", b"t", b"w", &empty),
            "an absent prefix differs from an explicitly empty prefix"
        );
    }

    #[test]
    fn friendly_label_is_not_pipeline_fingerprint_material() {
        let first = fingerprint_fixture();
        let mut renamed = first.clone();
        renamed.model_id = "another-friendly-label".into();
        let first_fingerprint = candle_pipeline_fingerprint(b"c", b"t", b"w", &first);
        let renamed_fingerprint = candle_pipeline_fingerprint(b"c", b"t", b"w", &renamed);
        assert_eq!(first_fingerprint, renamed_fingerprint);
        assert_ne!(
            content_bound_model_id(&first.model_id, first_fingerprint, CANDLE_PIPELINE_REVISION),
            content_bound_model_id(
                &renamed.model_id,
                renamed_fingerprint,
                CANDLE_PIPELINE_REVISION
            ),
            "the human label remains part of the complete persisted identity"
        );
    }

    #[test]
    fn placeholder_labels_are_rejected_before_artifacts_are_loaded() {
        for label in ["", "   ", "unknown", " UNKNOWN ", "default", " Default "] {
            let mut cfg = fingerprint_fixture();
            cfg.model_id = label.into();
            let result =
                CandleEmbedder::from_bytes(b"bad config", b"bad tokenizer", Vec::new(), cfg);
            let Err(error) = result else {
                panic!("placeholder label {label:?} unexpectedly loaded")
            };
            assert!(
                error.to_string().contains("model id"),
                "label {label:?} reached artifact parsing: {error}"
            );
        }
    }

    #[test]
    fn masked_mean_pool_averages_unmasked_tokens() {
        let dev = Device::Cpu;
        let hidden = Tensor::from_vec(vec![1f32, 2., 3., 4.], (1, 2, 2), &dev).unwrap();
        let m_all = Tensor::from_vec(vec![1f32, 1.], (1, 2), &dev).unwrap();
        let out = masked_mean_pool(&hidden, &m_all)
            .unwrap()
            .to_vec2::<f32>()
            .unwrap();
        assert!(approx(out[0][0], 2.0, 1e-6) && approx(out[0][1], 3.0, 1e-6));
        let m_first = Tensor::from_vec(vec![1f32, 0.], (1, 2), &dev).unwrap();
        let out = masked_mean_pool(&hidden, &m_first)
            .unwrap()
            .to_vec2::<f32>()
            .unwrap();
        assert!(approx(out[0][0], 1.0, 1e-6) && approx(out[0][1], 2.0, 1e-6));
    }

    #[test]
    fn cls_pool_takes_first_token() {
        let dev = Device::Cpu;
        let hidden = Tensor::from_vec(vec![1f32, 2., 3., 4.], (1, 2, 2), &dev).unwrap();
        let out = cls_pool(&hidden).unwrap().to_vec2::<f32>().unwrap();
        assert!(approx(out[0][0], 1.0, 1e-6) && approx(out[0][1], 2.0, 1e-6));
    }

    #[test]
    fn l2_normalize_yields_unit_vectors() {
        let dev = Device::Cpu;
        let v = Tensor::from_vec(vec![3f32, 4.], (1, 2), &dev).unwrap();
        let out = l2_normalize(&v).unwrap().to_vec2::<f32>().unwrap();
        assert!(approx(out[0][0], 0.6, 1e-6) && approx(out[0][1], 0.8, 1e-6));
        let norm = (out[0][0] * out[0][0] + out[0][1] * out[0][1]).sqrt();
        assert!(approx(norm, 1.0, 1e-6));
    }

    fn synthetic_tokenizer() -> Tokenizer {
        let vocab = [
            ("[PAD]".to_string(), 0u32),
            ("[UNK]".to_string(), 1),
            ("[CLS]".to_string(), 2),
            ("[SEP]".to_string(), 3),
            ("hello".to_string(), 4),
            ("world".to_string(), 5),
            ("test".to_string(), 6),
            ("foo".to_string(), 7),
            ("query".to_string(), 8),
            ("passage".to_string(), 9),
        ];
        let wp = WordPieceBuilder::new()
            .vocab(vocab)
            .unk_token("[UNK]".into())
            .build()
            .unwrap();
        let mut tokenizer = Tokenizer::new(wp);
        tokenizer.with_pre_tokenizer(Some(Whitespace {}));
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..Default::default()
        }));
        tokenizer
    }

    fn synthetic_config() -> Config {
        Config {
            vocab_size: 10,
            hidden_size: 32,
            num_hidden_layers: 2,
            num_attention_heads: 2,
            intermediate_size: 64,
            max_position_embeddings: 64,
            type_vocab_size: 2,
            ..Config::default()
        }
    }

    fn make_varmap_deterministic(varmap: &VarMap, device: &Device) {
        let variables = varmap.data().lock().unwrap();
        for (name, variable) in variables.iter() {
            let values = (0..variable.elem_count())
                .map(|index| {
                    let mut hash = 0xcbf2_9ce4_8422_2325u64;
                    for byte in name
                        .bytes()
                        .chain(u64::try_from(index).unwrap().to_le_bytes())
                    {
                        hash ^= u64::from(byte);
                        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
                    }
                    let unit = ((hash >> 40) as u32) as f32 / 16_777_215.0;
                    (unit - 0.5) * 0.1
                })
                .collect::<Vec<_>>();
            let tensor = Tensor::from_vec(values, variable.shape().clone(), device).unwrap();
            variable.set(&tensor).unwrap();
        }
    }

    /// Tiny random-weight BERT + WordPiece tokenizer; runs the full path in CI
    /// offline.
    fn synthetic_embedder() -> CandleEmbedder {
        let device = Device::Cpu;
        let tokenizer = synthetic_tokenizer();
        let config = synthetic_config();
        let varmap = VarMap::new();
        let vb = VarBuilder::from_varmap(&varmap, DTYPE, &device);
        let model = BertModel::load(vb, &config).unwrap();

        CandleEmbedder {
            model: Backbone::Bert(Box::new(model)),
            tokenizer,
            device,
            dim: config.hidden_size,
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Mean,
            normalize: true,
            passage_prefix: None,
            query_prefix: None,
            model_id: "synthetic".into(),
        }
    }

    fn synthetic_artifacts() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let config_json = br#"{
            "vocab_size": 10,
            "hidden_size": 32,
            "num_hidden_layers": 2,
            "num_attention_heads": 2,
            "intermediate_size": 64,
            "hidden_act": "gelu",
            "hidden_dropout_prob": 0.1,
            "max_position_embeddings": 64,
            "type_vocab_size": 2,
            "initializer_range": 0.02,
            "layer_norm_eps": 1e-12,
            "pad_token_id": 0,
            "position_embedding_type": "absolute",
            "use_cache": true,
            "classifier_dropout": null,
            "model_type": "bert"
        }"#
        .to_vec();
        let tokenizer_json = synthetic_tokenizer().to_string(false).unwrap().into_bytes();

        let device = Device::Cpu;
        let varmap = VarMap::new();
        let vb = VarBuilder::from_varmap(&varmap, DTYPE, &device);
        let _model = BertModel::load(vb, &synthetic_config()).unwrap();
        make_varmap_deterministic(&varmap, &device);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("model.safetensors");
        varmap.save(&path).unwrap();
        let weights = std::fs::read(path).unwrap();
        (config_json, tokenizer_json, weights)
    }

    fn synthetic_artifact_config() -> CandleConfig {
        CandleConfig {
            model_id: "synthetic-artifact".into(),
            arch: Arch::Bert,
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Mean,
            normalize: true,
            passage_prefix: None,
            query_prefix: None,
            max_length: 64,
        }
    }

    #[test]
    fn from_bytes_and_from_dir_publish_the_content_bound_identity() {
        let (config_json, tokenizer_json, weights) = synthetic_artifacts();
        let cfg = synthetic_artifact_config();
        let fingerprint =
            candle_pipeline_fingerprint(&config_json, &tokenizer_json, &weights, &cfg);
        let expected = content_bound_model_id(&cfg.model_id, fingerprint, CANDLE_PIPELINE_REVISION);

        let from_bytes =
            CandleEmbedder::from_bytes(&config_json, &tokenizer_json, weights.clone(), cfg.clone())
                .unwrap();
        assert_eq!(from_bytes.model_id(), expected);

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config.json"), &config_json).unwrap();
        std::fs::write(dir.path().join("tokenizer.json"), &tokenizer_json).unwrap();
        std::fs::write(dir.path().join("model.safetensors"), &weights).unwrap();
        let from_dir = CandleEmbedder::from_dir(dir.path(), cfg).unwrap();
        assert_eq!(from_dir.model_id(), expected);
        assert_eq!(
            from_dir.model_id().rsplit_once(':').unwrap().1.len(),
            64,
            "the complete 256-bit fingerprint must be retained"
        );
    }

    #[test]
    fn pipeline_revision_pins_reference_inference_output() {
        let (config_json, tokenizer_json, weights) = synthetic_artifacts();
        let embedder = CandleEmbedder::from_bytes(
            &config_json,
            &tokenizer_json,
            weights,
            synthetic_artifact_config(),
        )
        .unwrap();
        let output = embedder.embed(&["hello world"]).unwrap().remove(0);
        let expected = [
            -0.203_813, -0.330_531, 0.014_233, 0.255_428, 0.099_244, 0.131_812, 0.357_670,
            -0.191_728, -0.219_566, -0.457_037, 0.090_915, 0.159_536, 0.108_990, 0.138_407,
            0.010_061, -0.169_396, -0.268_809, -0.019_827, 0.026_564, 0.083_869, 0.115_970,
            0.160_870, -0.051_976, -0.138_320, 0.172_926, -0.062_050, 0.069_390, 0.038_648,
            0.131_878, 0.154_857, 0.017_256, -0.133_886,
        ];
        assert_eq!(output.len(), expected.len());
        let max_abs_error = output
            .iter()
            .zip(expected)
            .map(|(actual, expected)| (actual - expected).abs())
            .fold(0.0f32, f32::max);
        assert!(
            max_abs_error <= 1e-4,
            "an inference change requires bumping CANDLE_PIPELINE_REVISION; max error {max_abs_error}"
        );
    }

    #[test]
    fn a_region_refuses_changed_artifacts_under_the_same_friendly_label() {
        let (config_json, tokenizer_json, weights) = synthetic_artifacts();
        let mut changed_weights = weights.clone();
        *changed_weights.last_mut().unwrap() ^= 1;
        let cfg = synthetic_artifact_config();
        let original =
            CandleEmbedder::from_bytes(&config_json, &tokenizer_json, weights, cfg.clone())
                .unwrap();
        let changed =
            CandleEmbedder::from_bytes(&config_json, &tokenizer_json, changed_weights, cfg)
                .unwrap();
        assert_ne!(original.model_id(), changed.model_id());

        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(
            citadel::DatabaseBuilder::new(dir.path().join("provenance.citadel"))
                .passphrase(b"test-passphrase")
                .argon2_profile(citadel::Argon2Profile::Iot)
                .create()
                .unwrap(),
        );
        let engine = crate::MemoryEngine::open(std::sync::Arc::clone(&db)).unwrap();
        engine
            .create_region("notes", std::sync::Arc::new(original))
            .unwrap();
        drop(engine);

        let reopened = crate::MemoryEngine::open(db).unwrap();
        let error = reopened
            .attach_existing_region("notes", std::sync::Arc::new(changed))
            .expect_err("changed model bytes reused vectors written by another artifact");
        assert!(
            matches!(error, crate::MemError::ModelMismatch { .. }),
            "expected a model mismatch, got {error:?}"
        );
    }

    #[test]
    fn synthetic_model_embeds_normalized_and_deterministic() {
        let e = synthetic_embedder();
        assert_eq!(e.dim(), 32);
        assert_eq!(e.metric(), EmbeddingMetric::Cosine);

        let out = e.embed(&["hello world", "test"]).unwrap();
        assert_eq!(out.len(), 2, "one vector per input");
        assert!(out.iter().all(|v| v.len() == 32), "dim matches config");
        for v in &out {
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!(approx(norm, 1.0, 1e-4), "L2-normalized, got {norm}");
        }

        let again = e.embed(&["hello world", "test"]).unwrap();
        assert_eq!(out, again, "deterministic for identical input");

        assert!(e.embed(&[]).unwrap().is_empty(), "empty batch -> empty");
        assert!(e.embed_queries(&[]).unwrap().is_empty());
    }

    #[test]
    fn asymmetric_prefixes_split_passage_and_query_encodings() {
        let mut e = synthetic_embedder();
        // Symmetric model: both sides encode identically.
        assert_eq!(
            e.embed(&["hello world"]).unwrap(),
            e.embed_queries(&["hello world"]).unwrap()
        );

        // E5-style asymmetric prefixes: each side must equal the manually
        // prefixed raw encoding, and the two sides must differ.
        e.passage_prefix = Some("passage ".into());
        e.query_prefix = Some("query ".into());
        let passage = e.embed(&["hello world"]).unwrap();
        let query = e.embed_queries(&["hello world"]).unwrap();
        assert_ne!(passage, query, "prefixes must separate the two sides");

        e.passage_prefix = None;
        e.query_prefix = None;
        assert_eq!(passage, e.embed(&["passage hello world"]).unwrap());
        assert_eq!(query, e.embed(&["query hello world"]).unwrap());
    }

    #[test]
    #[ignore = "needs CITADEL_BGE_SMALL_DIR pointing at a local bge-small-en-v1.5 dir"]
    fn bge_small_loads_real_model_and_embeds_semantically() {
        let dir = std::env::var("CITADEL_BGE_SMALL_DIR")
            .expect("set CITADEL_BGE_SMALL_DIR to a local bge-small-en-v1.5 directory");
        let e = CandleEmbedder::bge_small(&dir).expect("load bge-small from dir");
        assert_eq!(e.dim(), 384, "bge-small-en-v1.5 is 384-dim");

        let out = e
            .embed(&[
                "the database schema lives in src/schema.rs",
                "where is the db table layout defined",
                "a recipe for tomato soup",
            ])
            .unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|v| v.len() == 384));

        // bge vectors are L2-normalized, so a dot product is the cosine
        // similarity.
        let cos = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        let related = cos(&out[0], &out[1]);
        let unrelated = cos(&out[0], &out[2]);
        assert!(
            related > unrelated,
            "semantic ordering broke: related={related} unrelated={unrelated}"
        );
    }

    #[test]
    #[ignore = "needs CITADEL_GRANITE_R2_DIR pointing at a local granite-embedding-english-r2 dir"]
    fn granite_r2_loads_and_distinguishes_lean_negation() {
        let dir = std::env::var("CITADEL_GRANITE_R2_DIR")
            .expect("set CITADEL_GRANITE_R2_DIR to a local granite-embedding-english-r2 directory");
        let e = CandleEmbedder::granite_r2(&dir).expect("load granite-r2 from dir");
        assert_eq!(e.dim(), 768, "granite-embedding-english-r2 is 768-dim");

        // The property that disqualified the BERT-vocab tier: a negation-only
        // difference must be visible at the input layer. With NFD+strip-accents
        // WordPiece, `=` and not-equal tokenize identically and these two
        // statements collapse; granite's byte-BPE must keep them apart.
        let out = e
            .embed(&[
                "\u{2200} (a b : \u{2115}), a + b = b + a",
                "\u{2200} (a b : \u{2115}), a + b \u{2260} b + a",
                "\u{2200} (n m : \u{2115}), n + m = m + n",
                "theorem about continuous functions on compact spaces",
            ])
            .unwrap();
        assert_eq!(out.len(), 4);
        assert!(out.iter().all(|v| v.len() == 768));
        for v in &out {
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!(approx(norm, 1.0, 1e-3), "L2-normalized, got {norm}");
        }

        let cos = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        let negation = cos(&out[0], &out[1]);
        let renamed = cos(&out[0], &out[2]);
        let unrelated = cos(&out[0], &out[3]);
        eprintln!("GRANITE cos: negation={negation} alpha-renamed={renamed} unrelated={unrelated}");
        // The negation may legitimately sit closer than a rename (one character
        // of surface difference vs four) - the vector tier is a recall widener
        // and a recalled negation is refuted downstream by the kernel. What
        // must hold: the pair is not input-identical (the BERT-vocab failure
        // mode, cos ~1.0), and both restatements rank far above unrelated text.
        assert!(
            negation < 0.999,
            "negation-only pair must not collapse to identity: {negation}"
        );
        assert!(
            renamed > unrelated && negation > unrelated,
            "restatements must outrank unrelated text: renamed={renamed} negation={negation} unrelated={unrelated}"
        );
    }

    /// Tiny random-weight cross-encoder; runs the full rerank path in CI
    /// offline.
    fn synthetic_cross_encoder() -> CrossEncoder {
        let device = Device::Cpu;
        let vocab = [
            ("[PAD]".to_string(), 0u32),
            ("[UNK]".to_string(), 1),
            ("[CLS]".to_string(), 2),
            ("[SEP]".to_string(), 3),
            ("hello".to_string(), 4),
            ("world".to_string(), 5),
            ("test".to_string(), 6),
            ("foo".to_string(), 7),
        ];
        let vocab_size = vocab.len();
        let wp = WordPieceBuilder::new()
            .vocab(vocab)
            .unk_token("[UNK]".into())
            .build()
            .unwrap();
        let mut tokenizer = Tokenizer::new(wp);
        tokenizer.with_pre_tokenizer(Some(Whitespace {}));

        let config = Config {
            vocab_size,
            hidden_size: 32,
            num_hidden_layers: 2,
            num_attention_heads: 2,
            intermediate_size: 64,
            max_position_embeddings: 64,
            type_vocab_size: 2,
            ..Config::default()
        };
        let varmap = VarMap::new();
        let vb = VarBuilder::from_varmap(&varmap, DTYPE, &device);
        let model = BertModel::load(vb.pp("bert"), &config).unwrap();
        let pooler = candle_nn::linear(32, 32, vb.pp("bert").pp("pooler").pp("dense")).unwrap();
        let classifier = candle_nn::linear(32, 1, vb.pp("classifier")).unwrap();

        CrossEncoder {
            model,
            pooler,
            classifier,
            tokenizer,
            device,
            model_id: "synthetic-ce".into(),
        }
    }

    #[test]
    fn built_in_models_poll_after_tokenization_and_forward_work() {
        let embedder = synthetic_embedder();
        let token = CancelToken::new();
        CANCEL_AFTER_TOKENIZATION.with(|slot| *slot.borrow_mut() = Some(token.clone()));
        assert!(matches!(
            embedder.embed_with_cancel(&["hello world"], Some(&token)),
            Err(EmbedError::Interrupted)
        ));

        let token = CancelToken::new();
        CANCEL_AFTER_MODEL_FORWARD.with(|slot| *slot.borrow_mut() = Some(token.clone()));
        assert!(matches!(
            embedder.embed_with_cancel(&["hello world"], Some(&token)),
            Err(EmbedError::Interrupted)
        ));

        let reranker = synthetic_cross_encoder();
        let token = CancelToken::new();
        CANCEL_AFTER_TOKENIZATION.with(|slot| *slot.borrow_mut() = Some(token.clone()));
        assert!(matches!(
            reranker.rerank_with_cancel("hello", &["world"], Some(&token)),
            Err(EmbedError::Interrupted)
        ));

        let token = CancelToken::new();
        CANCEL_AFTER_MODEL_FORWARD.with(|slot| *slot.borrow_mut() = Some(token.clone()));
        assert!(matches!(
            reranker.rerank_with_cancel("hello", &["world"], Some(&token)),
            Err(EmbedError::Interrupted)
        ));
    }

    #[test]
    fn synthetic_cross_encoder_scores_each_passage_deterministically() {
        let ce = synthetic_cross_encoder();
        let scores = ce
            .rerank("hello world", &["hello world", "test foo", "world"])
            .unwrap();
        assert_eq!(scores.len(), 3, "one score per passage");
        let again = ce
            .rerank("hello world", &["hello world", "test foo", "world"])
            .unwrap();
        assert_eq!(scores, again, "deterministic for identical input");
        assert!(
            ce.rerank("hello", &[]).unwrap().is_empty(),
            "empty -> empty"
        );
        assert_eq!(ce.model_id(), "synthetic-ce");
    }

    #[test]
    fn ensure_finite_rejects_nan_and_infinity() {
        assert!(ensure_finite(&[0.0, 1.5, -2.0], "m").is_ok());
        assert!(ensure_finite(&[0.0, f32::NAN], "m").is_err());
        assert!(ensure_finite(&[f32::NEG_INFINITY], "m").is_err());
    }

    /// Mixed-length passages spanning many micro-batches: chunked-batch scores
    /// must equal per-pair scores, mapped back to input order.
    #[test]
    fn rerank_chunked_batch_matches_per_pair_scores() {
        let ce = synthetic_cross_encoder();
        let words = ["hello", "world", "test", "foo"];
        let mut passages = Vec::new();
        for i in 0..600 {
            let len = 1 + (i * 7) % 50;
            let mut p = String::new();
            for j in 0..len {
                if j > 0 {
                    p.push(' ');
                }
                p.push_str(words[(i + j) % words.len()]);
            }
            passages.push(p);
        }
        assert!(passages.len() > MICRO_BATCH, "must span multiple chunks");

        let refs: Vec<&str> = passages.iter().map(String::as_str).collect();
        let batched = ce.rerank("hello world", &refs).unwrap();
        assert_eq!(batched.len(), refs.len(), "one score per passage");
        for (i, &p) in refs.iter().enumerate() {
            let solo = ce.rerank("hello world", &[p]).unwrap()[0];
            assert!(
                approx(batched[i], solo, 1e-4),
                "passage {i}: batched={} solo={solo}",
                batched[i]
            );
        }
    }

    /// Real-model integration (local-only; not run in CI). With `cuda-embed`
    /// this exercises the GPU rerank path end to end:
    ///   cargo test -p citadeldb-mem --features cuda-embed -- --ignored
    #[test]
    #[ignore = "needs CITADEL_RERANKER_DIR -> a local ms-marco-MiniLM-L-6-v2 directory"]
    fn real_cross_encoder_is_finite_and_batch_consistent() {
        let dir = std::env::var("CITADEL_RERANKER_DIR")
            .expect("set CITADEL_RERANKER_DIR to a local ms-marco-MiniLM-L-6-v2 directory");
        let ce = CrossEncoder::ms_marco_minilm_l6(dir).unwrap();
        let passages: Vec<String> = (0..96)
            .map(|i| {
                let filler = " and we compared ticket prices for the jazz concert".repeat(i % 5);
                format!("Turn {i}: we planned the trip to Boston{filler}.")
            })
            .collect();
        let refs: Vec<&str> = passages.iter().map(String::as_str).collect();
        let batched = ce.rerank("what trip did they plan?", &refs).unwrap();
        assert_eq!(batched.len(), refs.len(), "one score per passage");
        for (i, &p) in refs.iter().enumerate() {
            let solo = ce.rerank("what trip did they plan?", &[p]).unwrap()[0];
            assert!(
                (batched[i] - solo).abs() < 0.05,
                "passage {i}: batched={} solo={solo}",
                batched[i]
            );
        }
    }
}
