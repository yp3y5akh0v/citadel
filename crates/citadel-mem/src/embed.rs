//! Pluggable text-to-vector embedding backends.

use citadel_core::CancelToken;

/// Distance metric for comparing an embedder's vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingMetric {
    Cosine,
    L2,
    InnerProduct,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    #[error("embedding operation interrupted")]
    Interrupted,
    #[error("embedding backend error: {0}")]
    Backend(String),
}

#[inline]
pub(crate) fn check_cancel(cancel: Option<&CancelToken>) -> Result<(), EmbedError> {
    if cancel.is_some_and(CancelToken::is_cancelled) {
        Err(EmbedError::Interrupted)
    } else {
        Ok(())
    }
}

pub(crate) fn normalize_model_id_label(model_id: &str) -> Result<String, String> {
    let model_id = model_id.trim();
    if model_id.is_empty() {
        return Err("model id must not be empty".into());
    }
    if model_id.eq_ignore_ascii_case("unknown") || model_id.eq_ignore_ascii_case("default") {
        return Err(format!(
            "model id '{model_id}' is a placeholder; supply the embedder's stable model id"
        ));
    }
    Ok(model_id.to_owned())
}

/// Sync, bring-your-own embedding backend: text -> fixed-dim vectors.
///
/// Implementations must expose the cancellation-aware passage path. The
/// non-cancellable convenience method is derived from it, so a backend cannot
/// satisfy this trait by implementing only a blocking legacy callback.
///
/// ```compile_fail
/// use citadel_mem::{EmbedError, Embedder, EmbeddingMetric};
///
/// struct BlockingLegacyBackend;
///
/// impl Embedder for BlockingLegacyBackend {
///     fn dim(&self) -> usize { 1 }
///     fn metric(&self) -> EmbeddingMetric { EmbeddingMetric::Cosine }
///     fn model_id(&self) -> &str { "legacy" }
///     fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
///         Ok(vec![vec![0.0]; texts.len()])
///     }
/// }
/// ```
pub trait Embedder: Send + Sync {
    fn dim(&self) -> usize;
    fn metric(&self) -> EmbeddingMetric;
    /// Durable identity of the vector-producing pipeline. Together with
    /// [`dim`](Self::dim) and [`metric`](Self::metric), the same value promises
    /// that stored texts produce compatible vectors.
    fn model_id(&self) -> &str;
    /// Cooperatively embed stored texts (the passage side), polling `cancel`
    /// during tokenization and inference. Returns one `dim()`-wide vector per
    /// input.
    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError>;
    /// Embed stored texts without a cancellation token.
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.embed_with_cancel(texts, None)
    }
    /// Embed a batch of search queries. Asymmetric retrieval models (E5's
    /// `query: `/`passage: ` format) encode the two sides differently; symmetric
    /// models keep the default, which uses their cancellable passage path.
    fn embed_queries(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.embed_queries_with_cancel(texts, None)
    }
    /// Cooperatively cancellable query embedding. Symmetric models delegate to
    /// their required passage implementation; asymmetric models override this.
    fn embed_queries_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.embed_with_cancel(texts, cancel)
    }
}

/// Sync, bring-your-own reranker: scores `(query, passage)` pairs jointly (higher = better).
///
/// ```compile_fail
/// use citadel_mem::{EmbedError, Reranker};
///
/// struct BlockingLegacyReranker;
///
/// impl Reranker for BlockingLegacyReranker {
///     fn model_id(&self) -> &str { "legacy" }
///     fn rerank(&self, _: &str, passages: &[&str]) -> Result<Vec<f32>, EmbedError> {
///         Ok(vec![0.0; passages.len()])
///     }
/// }
/// ```
pub trait Reranker: Send + Sync {
    fn model_id(&self) -> &str;
    /// Cooperatively rerank passages, polling `cancel` during tokenization and
    /// inference. Returns one finite relevance score per passage, in input order.
    /// Scores may be negative and need not lie in a fixed range.
    fn rerank_with_cancel(
        &self,
        query: &str,
        passages: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<f32>, EmbedError>;
    /// Rerank without a cancellation token.
    fn rerank(&self, query: &str, passages: &[&str]) -> Result<Vec<f32>, EmbedError> {
        self.rerank_with_cancel(query, passages, None)
    }
}

/// Deterministic test reranker: scores a passage by how many query words it repeats.
pub struct MockReranker;

impl Reranker for MockReranker {
    fn model_id(&self) -> &str {
        "mock-reranker"
    }

    fn rerank_with_cancel(
        &self,
        query: &str,
        passages: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<f32>, EmbedError> {
        let q: Vec<&str> = query.split_whitespace().collect();
        let mut scores = Vec::with_capacity(passages.len());
        for passage in passages {
            check_cancel(cancel)?;
            scores.push(
                passage
                    .split_whitespace()
                    .filter(|word| q.contains(word))
                    .count() as f32,
            );
        }
        check_cancel(cancel)?;
        Ok(scores)
    }
}

/// Deterministic test embedder: a hashed bag-of-words (shared tokens -> near under cosine).
pub struct MockEmbedder {
    dim: usize,
    metric: EmbeddingMetric,
}

impl MockEmbedder {
    pub fn new(dim: usize) -> Self {
        Self {
            dim,
            metric: EmbeddingMetric::Cosine,
        }
    }

    pub fn with_metric(dim: usize, metric: EmbeddingMetric) -> Self {
        Self { dim, metric }
    }
}

impl Embedder for MockEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn metric(&self) -> EmbeddingMetric {
        self.metric
    }

    fn model_id(&self) -> &str {
        "mock-fnv1a-bow-v1"
    }

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        let mut vectors = Vec::with_capacity(texts.len());
        for text in texts {
            check_cancel(cancel)?;
            vectors.push(hashed_bow(text, self.dim));
        }
        check_cancel(cancel)?;
        Ok(vectors)
    }
}

/// FNV-1a hashed bag-of-words.
fn hashed_bow(text: &str, dim: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; dim];
    if dim == 0 {
        return v;
    }
    for token in text.split_whitespace() {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in token.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        v[(h % dim as u64) as usize] += 1.0;
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TokenAwareEmbedder {
        calls: AtomicUsize,
    }

    impl Embedder for TokenAwareEmbedder {
        fn dim(&self) -> usize {
            1
        }

        fn metric(&self) -> EmbeddingMetric {
            EmbeddingMetric::Cosine
        }

        fn model_id(&self) -> &str {
            "token-aware"
        }

        fn embed_with_cancel(
            &self,
            texts: &[&str],
            cancel: Option<&CancelToken>,
        ) -> Result<Vec<Vec<f32>>, EmbedError> {
            check_cancel(cancel)?;
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(vec![vec![1.0]; texts.len()])
        }
    }

    struct TokenAwareReranker {
        calls: AtomicUsize,
    }

    impl Reranker for TokenAwareReranker {
        fn model_id(&self) -> &str {
            "token-aware"
        }

        fn rerank_with_cancel(
            &self,
            _query: &str,
            passages: &[&str],
            cancel: Option<&CancelToken>,
        ) -> Result<Vec<f32>, EmbedError> {
            check_cancel(cancel)?;
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(vec![1.0; passages.len()])
        }
    }

    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
        let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if na == 0.0 || nb == 0.0 {
            0.0
        } else {
            dot / (na * nb)
        }
    }

    #[test]
    fn required_cancellable_paths_power_the_convenience_methods() {
        let embedder = TokenAwareEmbedder {
            calls: AtomicUsize::new(0),
        };
        let reranker = TokenAwareReranker {
            calls: AtomicUsize::new(0),
        };
        let live = CancelToken::new();

        assert_eq!(embedder.embed(&["text"]).unwrap(), vec![vec![1.0]]);
        assert_eq!(embedder.embed_queries(&["query"]).unwrap(), vec![vec![1.0]]);
        assert_eq!(reranker.rerank("query", &["passage"]).unwrap(), vec![1.0]);
        assert_eq!(embedder.calls.load(Ordering::Relaxed), 2);
        assert_eq!(reranker.calls.load(Ordering::Relaxed), 1);

        live.cancel();
        assert!(matches!(
            embedder.embed_with_cancel(&["not called"], Some(&live)),
            Err(EmbedError::Interrupted)
        ));
        assert!(matches!(
            reranker.rerank_with_cancel("query", &["not called"], Some(&live)),
            Err(EmbedError::Interrupted)
        ));
        assert_eq!(embedder.calls.load(Ordering::Relaxed), 2);
        assert_eq!(reranker.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn deterministic_same_text() {
        let e = MockEmbedder::new(64);
        let a = e.embed(&["the quick brown fox"]).unwrap();
        let b = e.embed(&["the quick brown fox"]).unwrap();
        assert_eq!(a, b);
        assert_eq!(a[0].len(), 64);
    }

    #[test]
    fn shared_tokens_are_more_similar() {
        let e = MockEmbedder::new(256);
        let v = e
            .embed(&["red green blue", "red green yellow", "alpha beta gamma"])
            .unwrap();
        let near = cosine(&v[0], &v[1]);
        let far = cosine(&v[0], &v[2]);
        assert!(near > far, "near {near} should exceed far {far}");
        assert!(
            far.abs() < 0.001,
            "disjoint texts should be near-orthogonal"
        );
    }

    #[test]
    fn batch_arity_and_dim() {
        let e = MockEmbedder::with_metric(32, EmbeddingMetric::L2);
        let out = e.embed(&["a b", "c", ""]).unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|v| v.len() == 32));
        assert!(
            out[2].iter().all(|&x| x == 0.0),
            "empty text -> zero vector"
        );
        assert_eq!(e.metric(), EmbeddingMetric::L2);
        assert_eq!(e.model_id(), "mock-fnv1a-bow-v1");
    }

    #[test]
    fn mock_pipeline_has_a_stable_known_answer() {
        let e = MockEmbedder::new(32);
        assert_eq!(e.model_id(), "mock-fnv1a-bow-v1");
        assert_eq!(
            e.embed(&["citadel mock known answer alpha alpha"]).unwrap()[0],
            [
                0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 1.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0,
            ],
            "changing the mock identity or vector algorithm requires a new versioned model id"
        );
    }
}
