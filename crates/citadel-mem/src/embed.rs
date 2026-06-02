//! Pluggable text-to-vector embedding backends.

/// Distance metric for comparing an embedder's vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingMetric {
    Cosine,
    L2,
    InnerProduct,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    #[error("embedding backend error: {0}")]
    Backend(String),
}

/// Sync, bring-your-own embedding backend: text -> fixed-dim vectors.
pub trait Embedder: Send + Sync {
    fn dim(&self) -> usize;
    fn metric(&self) -> EmbeddingMetric;
    fn model_id(&self) -> &str;
    /// Embed a batch of texts, one vector per input, each of length `dim()`.
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError>;
}

/// Sync, bring-your-own reranker: scores `(query, passage)` pairs jointly (higher = better).
pub trait Reranker: Send + Sync {
    fn model_id(&self) -> &str;
    /// One relevance score per passage, in input order (`passages.len()` scores).
    fn rerank(&self, query: &str, passages: &[&str]) -> Result<Vec<f32>, EmbedError>;
}

/// Deterministic test reranker: scores a passage by how many query words it repeats.
pub struct MockReranker;

impl Reranker for MockReranker {
    fn model_id(&self) -> &str {
        "mock-reranker"
    }

    fn rerank(&self, query: &str, passages: &[&str]) -> Result<Vec<f32>, EmbedError> {
        let q: Vec<&str> = query.split_whitespace().collect();
        Ok(passages
            .iter()
            .map(|p| p.split_whitespace().filter(|w| q.contains(w)).count() as f32)
            .collect())
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
        "mock"
    }

    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        Ok(texts.iter().map(|t| hashed_bow(t, self.dim)).collect())
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
        assert_eq!(e.model_id(), "mock");
    }
}
