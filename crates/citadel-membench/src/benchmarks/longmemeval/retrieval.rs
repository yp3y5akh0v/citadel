//! Retrieval recall scoring: compare recalled atoms against the LongMemEval gold
//! (session ids and `has_answer` turns), mirroring the official recall any/all @k metric.

use citadel_mem::{AtomHit, Embedder, MemoryEngine, RecallProfile, RecallQuery};
use rustc_hash::FxHashSet;

use crate::core::error::Result;
use crate::core::retrieval::validate_embeddings;

/// Vector-only control; omitting query text also bypasses an attached reranker.
pub fn semantic_only_recall(
    eng: &MemoryEngine,
    region: &str,
    embedder: &dyn Embedder,
    question: &str,
    k: usize,
) -> Result<Vec<AtomHit>> {
    if k == 0 {
        return Ok(Vec::new());
    }
    let mut embeddings = embedder
        .embed_queries(&[question])
        .map_err(citadel_mem::MemError::from)?;
    validate_embeddings(&embeddings, 1, embedder.dim())?;
    let query = RecallQuery::by_embedding(embeddings.remove(0), k);
    Ok(eng.recall(region, RecallProfile::semantic_only().apply(query))?)
}

/// ANY/ALL gold-recall counts at three cutoffs, plus the question count.
#[derive(Default, Clone, Copy)]
pub struct Tally {
    pub any: [usize; 3],
    pub all: [usize; 3],
    pub n: usize,
}

impl Tally {
    /// Membership recall: is any / every gold id within the top-k ranked ids?
    pub fn record_membership(&mut self, ranked: &[&str], gold: &[&str], ks: [usize; 3]) {
        self.n += 1;
        for (ki, &k) in ks.iter().enumerate() {
            let top = &ranked[..k.min(ranked.len())];
            self.any[ki] += usize::from(gold.iter().any(|g| top.contains(g)));
            self.all[ki] += usize::from(gold.iter().all(|g| top.contains(g)));
        }
    }

    /// `has_answer` recall: how many top-k hits are evidence turns? `total` is the
    /// haystack's evidence-turn count; all = every evidence turn retrieved.
    pub fn record_has_answer(&mut self, hits: &[AtomHit], total: usize, ks: [usize; 3]) {
        self.n += 1;
        for (ki, &k) in ks.iter().enumerate() {
            let got = hits[..k.min(hits.len())]
                .iter()
                .filter(|h| {
                    h.payload
                        .get("has_answer")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                })
                .count();
            self.any[ki] += usize::from(got >= 1);
            self.all[ki] += usize::from(total > 0 && got == total);
        }
    }

    /// `@k any%/all%` per cutoff.
    pub fn cells(&self, ks: [usize; 3]) -> String {
        ks.iter()
            .enumerate()
            .map(|(ki, k)| {
                format!(
                    "@{k} {:.1}/{:.1}",
                    pct(self.any[ki], self.n),
                    pct(self.all[ki], self.n)
                )
            })
            .collect::<Vec<_>>()
            .join("  ")
    }
}

pub fn pct(a: usize, b: usize) -> f64 {
    if b == 0 {
        0.0
    } else {
        100.0 * a as f64 / b as f64
    }
}

/// Retrieved session ids in rank order, first occurrence only (a session spans turns).
pub fn distinct_session_ids(hits: &[AtomHit]) -> Vec<&str> {
    let mut seen = FxHashSet::default();
    let mut out = Vec::new();
    for h in hits {
        let sid = h.payload.get("session_id").and_then(|v| v.as_str());
        // Every ingested atom carries session_id (see `ingest_sample`); a missing one
        // would SILENTLY under-report session recall, so fail loudly in tests instead.
        debug_assert!(
            sid.is_some(),
            "retrieved atom {} missing session_id payload - ingest invariant violated",
            h.id
        );
        if let Some(sid) = sid {
            if seen.insert(sid) {
                out.push(sid);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel::{Argon2Profile, CancelToken, DatabaseBuilder};
    use citadel_mem::{AtomInput, EmbedError, EmbeddingMetric, RerankStrategy, Reranker};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const KS: [usize; 3] = [2, 3, 5];

    struct QueryEmbedder {
        queries: AtomicUsize,
        output: Vec<Vec<f32>>,
    }

    impl QueryEmbedder {
        fn new(output: Vec<Vec<f32>>) -> Self {
            Self {
                queries: AtomicUsize::new(0),
                output,
            }
        }
    }

    impl Embedder for QueryEmbedder {
        fn dim(&self) -> usize {
            2
        }
        fn metric(&self) -> EmbeddingMetric {
            EmbeddingMetric::Cosine
        }
        fn model_id(&self) -> &str {
            "semantic-control-test"
        }
        fn embed_with_cancel(
            &self,
            _: &[&str],
            _: Option<&CancelToken>,
        ) -> std::result::Result<Vec<Vec<f32>>, EmbedError> {
            panic!("semantic control must use query embedding")
        }
        fn embed_queries_with_cancel(
            &self,
            texts: &[&str],
            _: Option<&CancelToken>,
        ) -> std::result::Result<Vec<Vec<f32>>, EmbedError> {
            assert_eq!(texts, ["query"]);
            self.queries.fetch_add(1, Ordering::Relaxed);
            Ok(self.output.clone())
        }
    }

    struct ReversingReranker(AtomicUsize);

    impl Reranker for ReversingReranker {
        fn model_id(&self) -> &str {
            "reversing-test"
        }
        fn rerank_with_cancel(
            &self,
            _: &str,
            passages: &[&str],
            _: Option<&CancelToken>,
        ) -> std::result::Result<Vec<f32>, EmbedError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(passages
                .iter()
                .map(|passage| if *passage == "far" { 10.0 } else { 0.0 })
                .collect())
        }
    }

    fn engine(sealed: bool) -> (tempfile::TempDir, MemoryEngine, Arc<QueryEmbedder>) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            DatabaseBuilder::new(dir.path().join("semantic-control.cdl"))
                .passphrase(b"test")
                .argon2_profile(Argon2Profile::Iot)
                .enable_region_keys(sealed)
                .create()
                .unwrap(),
        );
        let eng = MemoryEngine::open(db).unwrap();
        let embedder = Arc::new(QueryEmbedder::new(vec![vec![1.0, 0.0]]));
        if sealed {
            eng.create_encrypted_region("turns", embedder.clone())
                .unwrap();
        } else {
            eng.create_region("turns", embedder.clone()).unwrap();
        }
        eng.remember_batch(
            "turns",
            vec![
                AtomInput::new("turn", "near").with_embedding(vec![1.0, 0.0]),
                AtomInput::new("turn", "far").with_embedding(vec![0.0, 1.0]),
            ],
        )
        .unwrap();
        (dir, eng, embedder)
    }

    #[test]
    fn semantic_control_bypasses_reranker_without_changing_default_recall() {
        for sealed in [false, true] {
            let (_dir, eng, embedder) = engine(sealed);
            let reranker = Arc::new(ReversingReranker(AtomicUsize::new(0)));
            eng.set_reranker(reranker.clone(), RerankStrategy::Replace);

            let control = semantic_only_recall(&eng, "turns", &*embedder, "query", 2).unwrap();
            assert_eq!(control[0].text, "near");
            assert_eq!(control[1].text, "far");
            assert_eq!(embedder.queries.load(Ordering::Relaxed), 1);
            assert_eq!(reranker.0.load(Ordering::Relaxed), 0);

            let default = eng
                .recall(
                    "turns",
                    RecallProfile::default().apply(RecallQuery::by_text("query", 2)),
                )
                .unwrap();
            assert_eq!(default[0].text, "far");
            assert_eq!(default[1].text, "near");
            assert_eq!(embedder.queries.load(Ordering::Relaxed), 2);
            assert_eq!(reranker.0.load(Ordering::Relaxed), 1);
        }
    }

    #[test]
    fn semantic_control_rejects_invalid_embeddings_before_recall() {
        let (_dir, eng, _) = engine(false);
        for output in [
            vec![],
            vec![vec![1.0, 0.0], vec![1.0, 0.0]],
            vec![vec![1.0]],
            vec![vec![f32::NAN, 0.0]],
            vec![vec![f32::INFINITY, 0.0]],
        ] {
            let embedder = QueryEmbedder::new(output);
            let result = semantic_only_recall(&eng, "missing", &embedder, "query", 1);
            assert!(matches!(result, Err(crate::BenchError::Dataset(_))));
            assert_eq!(embedder.queries.load(Ordering::Relaxed), 1);
        }
    }

    #[test]
    fn empty_semantic_control_does_not_embed_or_recall() {
        let (_dir, eng, embedder) = engine(false);
        assert!(
            semantic_only_recall(&eng, "missing", &*embedder, "query", 0)
                .unwrap()
                .is_empty()
        );
        assert_eq!(embedder.queries.load(Ordering::Relaxed), 0);
    }

    fn hit(id: i64, payload: serde_json::Value) -> AtomHit {
        AtomHit {
            id,
            kind: "turn".into(),
            text: String::new(),
            payload,
            importance: 0.0,
            confidence: 1.0,
            relevance: Some(0.0),
            distance: Some(0.0),
            graph_depth: None,
            created_at: 0,
            expires_at: None,
            immutable: false,
        }
    }

    #[test]
    fn membership_any_and_all_respect_cutoffs() {
        // ranked a,b,c,d,e; gold {b,d} -> b@2, d@4.
        let mut t = Tally::default();
        t.record_membership(&["a", "b", "c", "d", "e"], &["b", "d"], KS);
        // @2 [a,b]: b present (any), d absent (not all).
        assert_eq!(t.any[0], 1);
        assert_eq!(t.all[0], 0);
        // @3 [a,b,c]: still only b -> any yes, all no.
        assert_eq!(t.all[1], 0);
        // @5 [a..e]: both -> all yes.
        assert_eq!(t.any[2], 1);
        assert_eq!(t.all[2], 1);
        assert_eq!(t.n, 1);
    }

    #[test]
    fn membership_misses_when_gold_outside_topk() {
        let mut t = Tally::default();
        t.record_membership(&["a", "b", "c", "d", "e"], &["e"], KS);
        // e is rank 5: absent @2 and @3, present @5.
        assert_eq!(t.any[0], 0);
        assert_eq!(t.any[1], 0);
        assert_eq!(t.any[2], 1);
        assert_eq!(t.all, [0, 0, 1]);
    }

    #[test]
    fn has_answer_counts_evidence_turns_in_topk() {
        // hits (rank order): no, yes, no, yes, no ; haystack has 2 evidence turns.
        let hits = vec![
            hit(1, json!({"has_answer": false})),
            hit(2, json!({"has_answer": true})),
            hit(3, json!({"has_answer": false})),
            hit(4, json!({"has_answer": true})),
            hit(5, json!({"has_answer": false})),
        ];
        let mut t = Tally::default();
        t.record_has_answer(&hits, 2, KS);
        // @2 [no,yes]: got 1 -> any yes, all (1==2) no.
        assert_eq!(t.any[0], 1);
        assert_eq!(t.all[0], 0);
        // @3 [no,yes,no]: still 1.
        assert_eq!(t.all[1], 0);
        // @5: got 2 == total -> all yes.
        assert_eq!(t.all[2], 1);
    }

    #[test]
    fn has_answer_any_false_when_no_evidence_in_topk() {
        let hits = vec![
            hit(1, json!({"has_answer": false})),
            hit(2, json!({"has_answer": false})),
            hit(3, json!({"has_answer": true})),
        ];
        let mut t = Tally::default();
        t.record_has_answer(&hits, 1, KS);
        // @2: no evidence -> any 0; @3: evidence present -> any 1, all (1==1) 1.
        assert_eq!(t.any[0], 0);
        assert_eq!(t.any[1], 1);
        assert_eq!(t.all[1], 1);
    }

    #[test]
    fn distinct_sessions_dedupe_in_rank_order() {
        let hits = vec![
            hit(1, json!({"session_id": "s1"})),
            hit(2, json!({"session_id": "s1"})),
            hit(3, json!({"session_id": "s2"})),
            hit(4, json!({"session_id": "s1"})),
            hit(5, json!({"session_id": "s3"})),
        ];
        assert_eq!(distinct_session_ids(&hits), ["s1", "s2", "s3"]);
    }

    #[test]
    fn pct_handles_zero_denominator() {
        assert_eq!(pct(0, 0), 0.0);
        assert!((pct(3, 4) - 75.0).abs() < 1e-9);
    }
}
