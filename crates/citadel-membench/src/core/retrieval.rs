//! Shared baseline retrieval and embedding validation.

use citadel_mem::{AtomHit, MemoryEngine, RecallProfile, RecallQuery};

use crate::core::error::{BenchError, Result};

pub fn validate_embeddings(embeddings: &[Vec<f32>], expected: usize, dim: usize) -> Result<()> {
    if embeddings.len() != expected
        || embeddings
            .iter()
            .any(|vector| vector.len() != dim || vector.iter().any(|x| !x.is_finite()))
    {
        return Err(BenchError::Dataset(format!(
            "query embedder must return {expected} finite {dim}-dimensional vectors, got {} vectors",
            embeddings.len()
        )));
    }
    Ok(())
}

/// Apply the scored baseline recipe to a text or pre-embedded query.
pub fn baseline_recall(
    eng: &MemoryEngine,
    region: &str,
    query: RecallQuery,
) -> Result<Vec<AtomHit>> {
    Ok(eng.recall(region, RecallProfile::default().apply(query))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel::{Argon2Profile, DatabaseBuilder};
    use citadel_mem::{AtomInput, Embedder, RerankStrategy};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct CountingEmbedder {
        passages: AtomicUsize,
        queries: AtomicUsize,
    }

    impl CountingEmbedder {
        fn new() -> Self {
            Self {
                passages: AtomicUsize::new(0),
                queries: AtomicUsize::new(0),
            }
        }
    }

    impl Embedder for CountingEmbedder {
        fn dim(&self) -> usize {
            2
        }

        fn metric(&self) -> citadel_mem::EmbeddingMetric {
            citadel_mem::EmbeddingMetric::Cosine
        }

        fn model_id(&self) -> &str {
            "baseline-parity-test"
        }

        fn embed_with_cancel(
            &self,
            texts: &[&str],
            _: Option<&citadel::CancelToken>,
        ) -> std::result::Result<Vec<Vec<f32>>, citadel_mem::EmbedError> {
            self.passages.fetch_add(texts.len(), Ordering::Relaxed);
            Ok(texts
                .iter()
                .map(|text| match *text {
                    "candidate 0" => vec![1.0, 0.0],
                    "candidate 1" => vec![0.8, 0.6],
                    "candidate 2" => vec![0.0, 1.0],
                    "candidate 3" => vec![-1.0, 0.0],
                    _ => panic!("unexpected passage: {text}"),
                })
                .collect())
        }

        fn embed_queries_with_cancel(
            &self,
            texts: &[&str],
            _: Option<&citadel::CancelToken>,
        ) -> std::result::Result<Vec<Vec<f32>>, citadel_mem::EmbedError> {
            self.queries.fetch_add(texts.len(), Ordering::Relaxed);
            Ok(texts
                .iter()
                .map(|text| {
                    assert_eq!(*text, "query");
                    vec![1.0, 0.0]
                })
                .collect())
        }
    }

    #[derive(Default)]
    struct CountingReranker {
        passages: Mutex<Vec<Vec<String>>>,
    }

    impl citadel_mem::Reranker for CountingReranker {
        fn model_id(&self) -> &str {
            "baseline-parity-reranker-test"
        }

        fn rerank_with_cancel(
            &self,
            query: &str,
            passages: &[&str],
            _: Option<&citadel::CancelToken>,
        ) -> std::result::Result<Vec<f32>, citadel_mem::EmbedError> {
            assert_eq!(query, "query");
            self.passages
                .lock()
                .unwrap()
                .push(passages.iter().map(|text| (*text).to_owned()).collect());
            Ok(passages
                .iter()
                .map(|text| text.strip_prefix("candidate ").unwrap().parse().unwrap())
                .collect())
        }
    }

    #[test]
    fn baseline_text_and_preembedded_queries_use_the_same_engine_recipe() {
        for sealed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = Arc::new(
                DatabaseBuilder::new(dir.path().join("baseline.cdl"))
                    .passphrase(b"test")
                    .argon2_profile(Argon2Profile::Iot)
                    .enable_region_keys(sealed)
                    .create()
                    .unwrap(),
            );
            let engine = MemoryEngine::open(db).unwrap();
            let embedder = Arc::new(CountingEmbedder::new());
            if sealed {
                engine
                    .create_encrypted_region("turns", embedder.clone())
                    .unwrap();
            } else {
                engine.create_region("turns", embedder.clone()).unwrap();
            }
            let ids = engine
                .remember_batch(
                    "turns",
                    (0..4)
                        .map(|i| {
                            AtomInput::new("turn", format!("candidate {i}"))
                                .with_created_at(1_700_000_000_000_000)
                        })
                        .collect(),
                )
                .unwrap();
            assert_eq!(embedder.passages.load(Ordering::Relaxed), 4);

            for strategy in [RerankStrategy::Replace, RerankStrategy::default()] {
                let reranker = Arc::new(CountingReranker::default());
                engine.set_reranker(reranker.clone(), strategy);
                let before_queries = embedder.queries.load(Ordering::Relaxed);
                let text =
                    baseline_recall(&engine, "turns", RecallQuery::by_text("query", 3)).unwrap();
                assert_eq!(embedder.queries.load(Ordering::Relaxed), before_queries + 1);

                let mut embeddings = embedder.embed_queries(&["query"]).unwrap();
                validate_embeddings(&embeddings, 1, embedder.dim()).unwrap();
                let preembedded = baseline_recall(
                    &engine,
                    "turns",
                    RecallQuery::by_embedding(embeddings.remove(0), 3).with_text("query"),
                )
                .unwrap();
                assert_eq!(embedder.queries.load(Ordering::Relaxed), before_queries + 2);

                let signature = |hits: &[AtomHit]| {
                    hits.iter()
                        .map(|hit| (hit.id, hit.relevance.expect("ranked hit").to_bits()))
                        .collect::<Vec<_>>()
                };
                assert_eq!(signature(&text), signature(&preembedded));
                assert_eq!(text.len(), 3);
                if matches!(strategy, RerankStrategy::Replace) {
                    assert_eq!(
                        text.iter().map(|hit| hit.id).collect::<Vec<_>>(),
                        [ids[3], ids[2], ids[1]]
                    );
                }
                let calls = reranker.passages.lock().unwrap();
                assert_eq!(calls.len(), 2);
                assert_eq!(calls[0], calls[1]);
                assert_eq!(calls[0].len(), 4);
                assert_eq!(embedder.passages.load(Ordering::Relaxed), 4);
            }
        }
    }
}
