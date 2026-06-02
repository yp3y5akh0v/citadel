//! BERT sentence embeddings via Candle (feature `candle-embed`).
//! Pooling is model-specific: BGE uses CLS, MiniLM/E5 use masked mean.

use std::path::Path;

use candle_core::{Device, Tensor};
use candle_nn::{Linear, Module, VarBuilder};
use candle_transformers::models::bert::{BertModel, Config, DTYPE};
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

use crate::embed::{EmbedError, Embedder, EmbeddingMetric, Reranker};

fn backend(e: impl std::fmt::Display) -> EmbedError {
    EmbedError::Backend(e.to_string())
}

/// Rerank micro-batch size; pairs are length-sorted so padding tracks each chunk.
const MICRO_BATCH: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pooling {
    /// First-token (`[CLS]`) hidden state. Used by BGE models.
    Cls,
    /// Attention-masked mean over tokens. Used by MiniLM and E5.
    Mean,
}

/// Settings for [`CandleEmbedder::from_dir`] (`dim` comes from the model config).
#[derive(Debug, Clone)]
pub struct CandleConfig {
    pub model_id: String,
    pub metric: EmbeddingMetric,
    pub pooling: Pooling,
    /// L2-normalize each output vector (required for cosine similarity).
    pub normalize: bool,
    /// Text prepended to every input before encoding (e.g. E5's `"query: "`).
    pub query_prefix: Option<String>,
    pub max_length: usize,
}

impl CandleConfig {
    /// `BAAI/bge-small-en-v1.5` / `bge-base-en-v1.5` (cosine, CLS pooling).
    pub fn bge_small() -> Self {
        Self {
            model_id: "bge-small-en-v1.5".into(),
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Cls,
            normalize: true,
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
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Mean,
            normalize: true,
            query_prefix: None,
            max_length: 256,
        }
    }

    /// `intfloat/e5-large-v2` (1024d, cosine, mean pooling; requires a `"query: "` prefix).
    pub fn e5_large() -> Self {
        Self {
            model_id: "e5-large-v2".into(),
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Mean,
            normalize: true,
            query_prefix: Some("query: ".into()),
            max_length: 512,
        }
    }
}

/// A local BERT sentence-embedding model (Candle backend).
pub struct CandleEmbedder {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
    dim: usize,
    metric: EmbeddingMetric,
    pooling: Pooling,
    normalize: bool,
    prefix: Option<String>,
    model_id: String,
}

/// Inference device: GPU 0 with `cuda-embed` (CPU fallback at runtime), else CPU.
#[cfg(feature = "cuda-embed")]
fn select_device() -> Device {
    // cuda-embed was requested, so warn rather than silently drop to CPU.
    match Device::new_cuda(0) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("[citadel-mem] cuda-embed enabled but GPU init failed; using CPU: {e}");
            Device::Cpu
        }
    }
}

#[cfg(not(feature = "cuda-embed"))]
fn select_device() -> Device {
    Device::Cpu
}

impl CandleEmbedder {
    /// Load from raw model bytes (no filesystem; the wasm/browser loader).
    pub fn from_bytes(
        config_json: &[u8],
        tokenizer_json: &[u8],
        weights: Vec<u8>,
        cfg: CandleConfig,
    ) -> Result<Self, EmbedError> {
        let device = select_device();

        let config: Config = serde_json::from_slice(config_json).map_err(backend)?;
        let dim = config.hidden_size;

        let mut tokenizer = Tokenizer::from_bytes(tokenizer_json).map_err(backend)?;
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..Default::default()
        }));
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: cfg.max_length,
                ..Default::default()
            }))
            .map_err(backend)?;

        let vb = VarBuilder::from_buffered_safetensors(weights, DTYPE, &device).map_err(backend)?;
        let model = BertModel::load(vb, &config).map_err(backend)?;

        Ok(Self {
            model,
            tokenizer,
            device,
            dim,
            metric: cfg.metric,
            pooling: cfg.pooling,
            normalize: cfg.normalize,
            prefix: cfg.query_prefix,
            model_id: cfg.model_id,
        })
    }

    /// Load from a directory of `config.json` / `tokenizer.json` / `model.safetensors`.
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

    /// `intfloat/e5-large-v2` from a directory.
    pub fn e5_large(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, CandleConfig::e5_large())
    }

    fn run(&self, texts: &[&str]) -> candle_core::Result<Tensor> {
        let inputs: Vec<String> = match &self.prefix {
            Some(p) => texts.iter().map(|t| format!("{p}{t}")).collect(),
            None => texts.iter().map(|t| t.to_string()).collect(),
        };
        let encodings = self
            .tokenizer
            .encode_batch(inputs, true)
            .map_err(candle_core::Error::wrap)?;

        let bsz = encodings.len();
        let seq = encodings[0].get_ids().len();
        let mut ids = Vec::with_capacity(bsz * seq);
        let mut type_ids = Vec::with_capacity(bsz * seq);
        let mut mask = Vec::with_capacity(bsz * seq);
        for enc in &encodings {
            ids.extend_from_slice(enc.get_ids());
            type_ids.extend_from_slice(enc.get_type_ids());
            mask.extend(enc.get_attention_mask().iter().map(|&m| m as f32));
        }

        let input_ids = Tensor::from_vec(ids, (bsz, seq), &self.device)?;
        let type_ids = Tensor::from_vec(type_ids, (bsz, seq), &self.device)?;
        let attn = Tensor::from_vec(mask, (bsz, seq), &self.device)?;

        let hidden = self.model.forward(&input_ids, &type_ids, Some(&attn))?;
        let pooled = match self.pooling {
            Pooling::Cls => cls_pool(&hidden)?,
            Pooling::Mean => masked_mean_pool(&hidden, &attn)?,
        };
        let out = if self.normalize {
            l2_normalize(&pooled)?
        } else {
            pooled
        };
        out.contiguous()
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

    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let out = self.run(texts).map_err(backend)?;
        out.to_vec2::<f32>().map_err(backend)
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
    /// Load from raw model bytes; `max_length` caps the `(query, passage)` token length.
    pub fn from_bytes(
        config_json: &[u8],
        tokenizer_json: &[u8],
        weights: Vec<u8>,
        model_id: impl Into<String>,
        max_length: usize,
    ) -> Result<Self, EmbedError> {
        let device = select_device();
        let config: Config = serde_json::from_slice(config_json).map_err(backend)?;
        let hidden = config.hidden_size;

        let mut tokenizer = Tokenizer::from_bytes(tokenizer_json).map_err(backend)?;
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..Default::default()
        }));
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

    /// Load from a directory of `config.json` / `tokenizer.json` / `model.safetensors`.
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

    /// `cross-encoder/ms-marco-MiniLM-L-6-v2` from a directory (1 logit, 512-token pairs).
    pub fn ms_marco_minilm_l6(dir: impl AsRef<Path>) -> Result<Self, EmbedError> {
        Self::from_dir(dir, "ms-marco-MiniLM-L-6-v2", 512)
    }

    /// Score every `(query, passage)` pair: one relevance logit per passage.
    /// Length-bucketed micro-batches keep padding near each chunk's real length.
    fn run(&self, query: &str, passages: &[&str]) -> candle_core::Result<Vec<f32>> {
        let mut encodings = Vec::with_capacity(passages.len());
        for p in passages {
            encodings.push(
                self.tokenizer
                    .encode((query, *p), true)
                    .map_err(candle_core::Error::wrap)?,
            );
        }

        // Process short pairs together and long pairs together: sorting by token
        // length keeps each micro-batch's padding near its real content length.
        let mut order: Vec<usize> = (0..encodings.len()).collect();
        order.sort_by_key(|&i| encodings[i].get_ids().len());

        let mut scores = vec![0f32; encodings.len()];
        for chunk in order.chunks(MICRO_BATCH) {
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

            let input_ids = Tensor::from_vec(ids, (bsz, seq), &self.device)?;
            let type_ids = Tensor::from_vec(type_ids, (bsz, seq), &self.device)?;
            let attn = Tensor::from_vec(mask, (bsz, seq), &self.device)?;

            // [CLS] hidden -> pooler (dense + tanh) -> classifier -> one logit per row.
            let hidden = self.model.forward(&input_ids, &type_ids, Some(&attn))?;
            // narrow+squeeze is non-contiguous; candle's CUDA matmul needs contiguous.
            let cls = hidden.narrow(1, 0, 1)?.squeeze(1)?.contiguous()?;
            let pooled = self.pooler.forward(&cls)?.tanh()?;
            let logits = self.classifier.forward(&pooled)?;
            let batch_scores = logits.squeeze(1)?.to_vec1::<f32>()?;
            for (pos, &i) in chunk.iter().enumerate() {
                scores[i] = batch_scores[pos];
            }
        }
        Ok(scores)
    }
}

impl Reranker for CrossEncoder {
    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn rerank(&self, query: &str, passages: &[&str]) -> Result<Vec<f32>, EmbedError> {
        if passages.is_empty() {
            return Ok(Vec::new());
        }
        self.run(query, passages).map_err(backend)
    }
}

/// Masked mean over tokens; denominator floored so all-padding rows don't divide by zero.
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

    #[test]
    fn masked_mean_pool_averages_unmasked_tokens() {
        let dev = Device::Cpu;
        // One row, 2 tokens, 2 dims: [[1,2],[3,4]].
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

    /// Tiny random-weight BERT + WordPiece tokenizer; runs the full path in CI offline.
    fn synthetic_embedder() -> CandleEmbedder {
        let device = Device::Cpu;
        // WordPieceBuilder::vocab wants Into<AHashMap>; the array form avoids ahash.
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
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..Default::default()
        }));

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
        let model = BertModel::load(vb, &config).unwrap();

        CandleEmbedder {
            model,
            tokenizer,
            device,
            dim: config.hidden_size,
            metric: EmbeddingMetric::Cosine,
            pooling: Pooling::Mean,
            normalize: true,
            prefix: None,
            model_id: "synthetic".into(),
        }
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
    }

    #[test]
    #[ignore = "needs CITADEL_AI_BGE_SMALL_DIR pointing at a local bge-small-en-v1.5 dir"]
    fn bge_small_loads_real_model_and_embeds_semantically() {
        let dir = std::env::var("CITADEL_AI_BGE_SMALL_DIR")
            .expect("set CITADEL_AI_BGE_SMALL_DIR to a local bge-small-en-v1.5 directory");
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

        // bge vectors are L2-normalized, so a dot product is the cosine similarity.
        let cos = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        let related = cos(&out[0], &out[1]);
        let unrelated = cos(&out[0], &out[2]);
        assert!(
            related > unrelated,
            "semantic ordering broke: related={related} unrelated={unrelated}"
        );
    }

    /// Tiny random-weight cross-encoder; runs the full rerank path in CI offline.
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
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..Default::default()
        }));

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
}
