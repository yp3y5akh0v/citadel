//! Coarse ingestion observations without changing embedding inputs or outputs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use citadel::CancelToken;
use citadel_mem::{EmbedError, Embedder, EmbeddingMetric};

/// Measures synchronous passage calls, including the backend's normal transfers.
/// It does not synchronize CUDA separately or estimate pure storage time.
pub(crate) struct IngestEmbedder {
    inner: Arc<dyn Embedder>,
    elapsed_micros: AtomicU64,
}

impl IngestEmbedder {
    pub(crate) fn new(inner: Arc<dyn Embedder>) -> Self {
        Self {
            inner,
            elapsed_micros: AtomicU64::new(0),
        }
    }

    pub(crate) fn elapsed(&self) -> Duration {
        Duration::from_micros(self.elapsed_micros.load(Ordering::Relaxed))
    }
}

impl Embedder for IngestEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn metric(&self) -> EmbeddingMetric {
        self.inner.metric()
    }

    fn model_id(&self) -> &str {
        self.inner.model_id()
    }

    fn embed_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        eprintln!("    embedding {} passages: started", texts.len());
        let started = Instant::now();
        let result = self.inner.embed_with_cancel(texts, cancel);
        let elapsed = started.elapsed();
        self.elapsed_micros.fetch_add(
            elapsed.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        eprintln!(
            "    embedding {} passages: {} in {:.1}s",
            texts.len(),
            if result.is_ok() { "finished" } else { "failed" },
            elapsed.as_secs_f64()
        );
        result
    }

    fn embed_queries_with_cancel(
        &self,
        texts: &[&str],
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.inner.embed_queries_with_cancel(texts, cancel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Asymmetric;

    impl Embedder for Asymmetric {
        fn dim(&self) -> usize {
            2
        }
        fn metric(&self) -> EmbeddingMetric {
            EmbeddingMetric::InnerProduct
        }
        fn model_id(&self) -> &str {
            "progress-test-asymmetric"
        }
        fn embed_with_cancel(
            &self,
            texts: &[&str],
            cancel: Option<&CancelToken>,
        ) -> Result<Vec<Vec<f32>>, EmbedError> {
            assert_eq!(texts, ["unchanged passage"]);
            if cancel.is_some_and(CancelToken::is_cancelled) {
                Err(EmbedError::Interrupted)
            } else {
                assert!(cancel.is_none());
                Ok(vec![vec![0.75, 0.25]])
            }
        }
        fn embed_queries_with_cancel(
            &self,
            texts: &[&str],
            cancel: Option<&CancelToken>,
        ) -> Result<Vec<Vec<f32>>, EmbedError> {
            assert_eq!(texts, ["unchanged query"]);
            assert!(cancel.is_some());
            Ok(vec![vec![0.25, -0.75]])
        }
    }

    #[test]
    fn observing_ingestion_preserves_identity_asymmetry_cancellation_and_errors() {
        let observed = IngestEmbedder::new(Arc::new(Asymmetric));
        assert_eq!(observed.dim(), 2);
        assert_eq!(observed.metric(), EmbeddingMetric::InnerProduct);
        assert_eq!(observed.model_id(), "progress-test-asymmetric");
        assert_eq!(
            observed.embed(&["unchanged passage"]).unwrap(),
            vec![vec![0.75, 0.25]]
        );
        let cancel = CancelToken::new();
        cancel.cancel();
        assert!(matches!(
            observed.embed_with_cancel(&["unchanged passage"], Some(&cancel)),
            Err(EmbedError::Interrupted)
        ));
        let measured = observed.elapsed();
        assert_eq!(
            observed
                .embed_queries_with_cancel(&["unchanged query"], Some(&cancel))
                .unwrap(),
            vec![vec![0.25, -0.75]]
        );
        assert_eq!(
            observed.elapsed(),
            measured,
            "queries must not enter ingestion timing"
        );
    }
}
