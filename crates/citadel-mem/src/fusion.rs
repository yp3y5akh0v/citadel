//! Multi-signal recall fusion: re-rank ANN candidates by a weighted blend of
//! semantic distance, keyword rank, recency, and importance.

use rustc_hash::FxHashMap;
use serde_json::Value as Json;

use citadel_core::CancelToken;

use crate::embed::{EmbedError, Reranker};
use crate::types::{AtomHit, AtomId, FusionWeights, RerankStrategy};

const RECENCY_HALF_LIFE_DAYS: f32 = 30.0;

/// Cross-encoding every candidate is CPU-bound; rerank pre-trims to this pool.
pub(crate) const RERANK_POOL: usize = 256;

/// An ANN candidate with the raw signals recall projected for it.
pub(crate) struct Candidate {
    pub id: AtomId,
    pub kind: String,
    pub text: String,
    pub payload: Json,
    pub dist: Option<f32>,
    pub text_rank: f32,
    pub importance: f32,
    pub confidence: f32,
    pub created_micros: i64,
    pub expires_micros: Option<i64>,
    pub immutable: bool,
}

pub(crate) struct RerankContext<'a> {
    pub query: &'a str,
    pub strategy: RerankStrategy,
    pub k: usize,
    pub cancel: Option<&'a CancelToken>,
}

enum NormalizationRange {
    F32(f32),
    F64(f64),
}

impl NormalizationRange {
    fn new(min: f32, max: f32) -> Self {
        let range = (max - min).max(f32::EPSILON);
        if range.is_finite() {
            Self::F32(range)
        } else {
            Self::F64(f64::from(max) - f64::from(min))
        }
    }

    fn difference(&self, upper: f32, lower: f32) -> f32 {
        match *self {
            Self::F32(range) => (upper - lower) / range,
            Self::F64(range) => ((f64::from(upper) - f64::from(lower)) / range) as f32,
        }
    }
}

pub(crate) fn recency_score(now_micros: i64, created_micros: i64) -> f32 {
    let age_days = now_micros.saturating_sub(created_micros).max(0) as f32 / 1e6 / 86_400.0;
    (-std::f32::consts::LN_2 * age_days / RECENCY_HALF_LIFE_DAYS).exp()
}

/// Per-candidate fusion score: each signal min-max normalized, then blended by `w`.
fn fusion_scores(cands: &[Candidate], w: FusionWeights, now_micros: i64) -> Vec<f32> {
    let mut distance_bounds: Option<(f32, f32)> = None;
    let mut rmax = 0.0f32;
    let mut imin = f32::MAX;
    let mut imax = f32::MIN;
    for c in cands {
        if let Some(distance) = c.dist {
            distance_bounds = Some(match distance_bounds {
                Some((min, max)) => (min.min(distance), max.max(distance)),
                None => (distance, distance),
            });
        }
        rmax = rmax.max(c.text_rank);
        imin = imin.min(c.importance);
        imax = imax.max(c.importance);
    }
    let (dmin, dmax) = distance_bounds.unwrap_or((0.0, 0.0));
    let drange = NormalizationRange::new(dmin, dmax);
    let irange = NormalizationRange::new(imin, imax);

    cands
        .iter()
        .map(|c| {
            let semantic = c
                .dist
                .map_or(0.0, |distance| drange.difference(dmax, distance));
            let keyword = if rmax > 0.0 { c.text_rank / rmax } else { 0.0 };
            let recency = recency_score(now_micros, c.created_micros);
            let importance = irange.difference(c.importance, imin);
            w.semantic * semantic
                + w.keyword * keyword
                + w.recency * recency
                + w.importance * importance
        })
        .collect()
}

/// Fuse signals, sort by descending score, and keep the top `k`.
pub(crate) fn fuse_rank(
    cands: Vec<Candidate>,
    w: FusionWeights,
    now_micros: i64,
    k: usize,
) -> Vec<AtomHit> {
    if cands.is_empty() {
        return Vec::new();
    }
    let scores = fusion_scores(&cands, w, now_micros);
    let mut scored: Vec<AtomHit> = cands
        .into_iter()
        .zip(scores)
        .map(|(c, score)| AtomHit {
            id: c.id,
            kind: c.kind,
            text: c.text,
            payload: c.payload,
            importance: c.importance,
            confidence: c.confidence,
            relevance: Some(score),
            distance: c.dist,
            graph_depth: None,
            created_at: c.created_micros,
            expires_at: c.expires_micros,
            immutable: c.immutable,
        })
        .collect();

    scored.sort_by(|a, b| {
        b.relevance
            .partial_cmp(&a.relevance)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.id.cmp(&b.id))
    });
    scored.truncate(k);
    scored
}

/// `rank[i]` = position of item `i` sorted by descending key (ties by ascending index).
fn ranks_desc(keys: &[f32]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_by(|&a, &b| {
        keys[b]
            .partial_cmp(&keys[a])
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.cmp(&b))
    });
    let mut rank = vec![0usize; keys.len()];
    for (pos, &i) in order.iter().enumerate() {
        rank[i] = pos;
    }
    rank
}

fn validate_reranker_scores(scores: &[f32], passages: usize) -> Result<(), EmbedError> {
    if scores.len() != passages {
        return Err(EmbedError::Backend(format!(
            "reranker returned {} scores for {} passages",
            scores.len(),
            passages
        )));
    }
    if let Some(index) = scores.iter().position(|score| !score.is_finite()) {
        return Err(EmbedError::Backend(format!(
            "reranker returned a non-finite score for passage {index}"
        )));
    }
    Ok(())
}

/// Re-rank candidates with a cross-encoder, then keep the top `k`. `strategy` is
/// Replace (trust the logit) or Rrf (blend cross-encoder and fusion ranks).
pub(crate) fn fuse_rerank(
    reranker: &dyn Reranker,
    mut cands: Vec<Candidate>,
    w: FusionWeights,
    now_micros: i64,
    context: RerankContext<'_>,
) -> std::result::Result<Vec<AtomHit>, EmbedError> {
    if cands.is_empty() {
        return Ok(Vec::new());
    }
    // Pre-trim to the top RERANK_POOL by linear fusion; the dropped tail is
    // the low-fusion, likely-irrelevant remainder.
    if cands.len() > RERANK_POOL {
        let pre = fusion_scores(&cands, w, now_micros);
        let mut idx: Vec<usize> = (0..cands.len()).collect();
        idx.sort_by(|&a, &b| {
            pre[b]
                .partial_cmp(&pre[a])
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.cmp(&b))
        });
        idx.truncate(RERANK_POOL);
        idx.sort_unstable();
        let mut kept = Vec::with_capacity(RERANK_POOL);
        for (i, c) in cands.into_iter().enumerate() {
            if idx.binary_search(&i).is_ok() {
                kept.push(c);
            }
        }
        cands = kept;
    }
    let passages: Vec<&str> = cands.iter().map(|c| c.text.as_str()).collect();
    let ce_scores = reranker.rerank_with_cancel(context.query, &passages, context.cancel)?;
    validate_reranker_scores(&ce_scores, passages.len())?;

    let scores: Vec<f32> = match context.strategy {
        RerankStrategy::Replace => ce_scores,
        RerankStrategy::Rrf { k: rrf_k } => {
            let fusion_scores = fusion_scores(&cands, w, now_micros);
            let ce_rank = ranks_desc(&ce_scores);
            let fusion_rank = ranks_desc(&fusion_scores);
            (0..cands.len())
                .map(|i| 1.0 / (rrf_k + ce_rank[i] as f32) + 1.0 / (rrf_k + fusion_rank[i] as f32))
                .collect()
        }
    };

    let mut scored: Vec<AtomHit> = cands
        .into_iter()
        .zip(scores)
        .map(|(c, s)| AtomHit {
            id: c.id,
            kind: c.kind,
            text: c.text,
            payload: c.payload,
            importance: c.importance,
            confidence: c.confidence,
            relevance: Some(s),
            distance: c.dist,
            graph_depth: None,
            created_at: c.created_micros,
            expires_at: c.expires_micros,
            immutable: c.immutable,
        })
        .collect();
    scored.sort_by(|a, b| {
        b.relevance
            .partial_cmp(&a.relevance)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.id.cmp(&b.id))
    });
    scored.truncate(context.k);
    Ok(scored)
}

/// RRF-merge hit lists: sum of 1/(rrf_k + rank), first-seen fields, id tiebreak.
pub(crate) fn rrf_merge(lists: Vec<Vec<AtomHit>>, rrf_k: f32) -> Vec<AtomHit> {
    let mut merged: Vec<AtomHit> = Vec::new();
    let mut index: FxHashMap<AtomId, usize> = FxHashMap::default();
    for list in lists {
        for (rank, hit) in list.into_iter().enumerate() {
            let contrib = 1.0 / (rrf_k + rank as f32);
            match index.get(&hit.id) {
                Some(&i) => {
                    let relevance = merged[i].relevance.get_or_insert(0.0);
                    *relevance += contrib;
                }
                None => {
                    index.insert(hit.id, merged.len());
                    let mut h = hit;
                    h.relevance = Some(contrib);
                    merged.push(h);
                }
            }
        }
    }
    merged.sort_by(|a, b| {
        b.relevance
            .partial_cmp(&a.relevance)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.id.cmp(&b.id))
    });
    merged
}

/// One cross-encoder pass over the merged pool: pre-trim, Replace/Rrf-blend, top k.
pub(crate) fn rerank_hits(
    reranker: &dyn Reranker,
    mut hits: Vec<AtomHit>,
    context: RerankContext<'_>,
) -> std::result::Result<Vec<AtomHit>, EmbedError> {
    if hits.is_empty() {
        return Ok(Vec::new());
    }
    hits.truncate(RERANK_POOL);
    let passages: Vec<&str> = hits.iter().map(|h| h.text.as_str()).collect();
    let ce_scores = reranker.rerank_with_cancel(context.query, &passages, context.cancel)?;
    validate_reranker_scores(&ce_scores, passages.len())?;
    let scores: Vec<f32> = match context.strategy {
        RerankStrategy::Replace => ce_scores,
        RerankStrategy::Rrf { k: rrf_k } => {
            let pool = hits
                .iter()
                .map(|h| h.relevance)
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| EmbedError::Backend("reranker received an unranked hit".into()))?;
            let ce_rank = ranks_desc(&ce_scores);
            let pool_rank = ranks_desc(&pool);
            (0..hits.len())
                .map(|i| 1.0 / (rrf_k + ce_rank[i] as f32) + 1.0 / (rrf_k + pool_rank[i] as f32))
                .collect()
        }
    };
    for (h, s) in hits.iter_mut().zip(&scores) {
        h.relevance = Some(*s);
    }
    hits.sort_by(|a, b| {
        b.relevance
            .partial_cmp(&a.relevance)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.id.cmp(&b.id))
    });
    hits.truncate(context.k);
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embed::MockReranker;

    fn cand_text(id: AtomId, text: &str) -> Candidate {
        Candidate {
            id,
            kind: "fact".into(),
            text: text.into(),
            payload: Json::Null,
            dist: Some(0.0),
            text_rank: 0.0,
            importance: 0.0,
            confidence: 1.0,
            created_micros: 0,
            expires_micros: None,
            immutable: false,
        }
    }

    fn cand(id: AtomId, dist: f32, text_rank: f32, importance: f32) -> Candidate {
        Candidate {
            id,
            kind: "fact".into(),
            text: String::new(),
            payload: Json::Null,
            dist: Some(dist),
            text_rank,
            importance,
            confidence: 1.0,
            created_micros: 0,
            expires_micros: None,
            immutable: false,
        }
    }

    #[test]
    fn finite_ranges_preserve_f32_score_bits() {
        let now = 1_700_000_000_000_000i64;
        let day = 86_400_000_000i64;
        let distances = [
            Some(0.95),
            Some(0.17),
            None,
            Some(1.3),
            Some(0.17000002),
            Some(0.48),
            Some(0.0),
            Some(2.0),
            Some(0.7),
            Some(1.0),
        ];
        let importance = [-3.0, -1.0, 0.0, 0.1, 0.2, 0.5, 1.0, 8.0, 9.0, 3.0];
        let keywords = [0.0, 0.1, 0.8, 1.0, 2.3, 0.0, 0.00001, 4.0, 2.0, 1.4];
        let created = [
            -1_000_000_000_000_000,
            0,
            1_000_000,
            now + 1,
            now,
            now - day,
            now - 30 * day,
            now - 90 * day,
            now - 60 * day,
            now - 300 * day,
        ];
        let cands: Vec<_> = (0..distances.len())
            .map(|i| Candidate {
                dist: distances[i],
                created_micros: created[i],
                ..cand(i as AtomId, 0.0, keywords[i], importance[i])
            })
            .collect();
        for weights in [FusionWeights::default(), FusionWeights::semantic_only()] {
            let expected: Vec<_> = (0..distances.len())
                .map(|i| {
                    let semantic = distances[i].map_or(0.0, |distance| (2.0 - distance) / 2.0);
                    let keyword = keywords[i] / 4.0;
                    let age_days = (now - created[i]).max(0) as f32 / 1e6 / 86_400.0;
                    let recency = (-std::f32::consts::LN_2 * age_days / 30.0).exp();
                    let importance = (importance[i] - -3.0) / 12.0;
                    (weights.semantic * semantic
                        + weights.keyword * keyword
                        + weights.recency * recency
                        + weights.importance * importance)
                        .to_bits()
                })
                .collect();
            let bits: Vec<_> = fusion_scores(&cands, weights, now)
                .into_iter()
                .map(f32::to_bits)
                .collect();
            assert_eq!(bits, expected);
        }
    }

    #[test]
    fn small_and_equal_ranges_keep_epsilon_normalization() {
        let range = NormalizationRange::new(0.0, f32::EPSILON / 2.0);
        assert_eq!(range.difference(f32::EPSILON / 2.0, 0.0), 0.5);
        assert_eq!(range.difference(f32::EPSILON / 4.0, 0.0), 0.25);
        for value in [-f32::MAX, -1.0, 0.0, 1.0, f32::MAX] {
            assert_eq!(
                NormalizationRange::new(value, value).difference(value, value),
                0.0
            );
        }
    }

    #[test]
    fn finite_importance_extremes_stay_finite_with_zero_or_nonzero_weight() {
        let values = [-f32::MAX, -f32::MAX / 2.0, 0.0, f32::MAX / 2.0, f32::MAX];
        let cands: Vec<_> = values
            .into_iter()
            .enumerate()
            .map(|(i, importance)| cand(i as AtomId, 1.0 - i as f32 / 4.0, 0.0, importance))
            .collect();
        let importance_only = FusionWeights {
            semantic: 0.0,
            keyword: 0.0,
            recency: 0.0,
            importance: 1.0,
        };
        assert_eq!(
            fusion_scores(&cands, importance_only, 0),
            [0.0, 0.25, 0.5, 0.75, 1.0]
        );
        assert_eq!(
            fusion_scores(&cands, FusionWeights::semantic_only(), 0),
            [0.0, 0.25, 0.5, 0.75, 1.0]
        );
        assert!(fusion_scores(&cands, FusionWeights::default(), 0)
            .iter()
            .all(|score| score.is_finite()));
    }

    #[test]
    fn finite_distance_extremes_stay_finite_with_zero_or_nonzero_weight() {
        let values = [-f32::MAX, -f32::MAX / 2.0, 0.0, f32::MAX / 2.0, f32::MAX];
        let cands: Vec<_> = values
            .into_iter()
            .enumerate()
            .map(|(i, distance)| cand(i as AtomId, distance, 0.0, 0.0))
            .collect();
        assert_eq!(
            fusion_scores(&cands, FusionWeights::semantic_only(), 0),
            [1.0, 0.75, 0.5, 0.25, 0.0]
        );
        let recency_only = FusionWeights {
            semantic: 0.0,
            keyword: 0.0,
            recency: 1.0,
            importance: 0.0,
        };
        assert_eq!(fusion_scores(&cands, recency_only, 0), [1.0; 5]);
        assert!(fusion_scores(&cands, FusionWeights::default(), 0)
            .iter()
            .all(|score| score.is_finite()));
    }

    #[test]
    fn recency_handles_the_entire_timestamp_range() {
        let timestamps = [i64::MIN, i64::MIN + 1, -1, 0, 1, 2, i64::MAX - 1, i64::MAX];
        for now in timestamps {
            for created in timestamps {
                let age_days =
                    (i128::from(now) - i128::from(created)).max(0) as f64 / 1e6 / 86_400.0;
                let expected = (-std::f64::consts::LN_2 * age_days / 30.0).exp() as f32;
                let actual = recency_score(now, created);
                assert!(actual.is_finite());
                assert!(
                    (actual - expected).abs() <= f32::EPSILON,
                    "{now}, {created}"
                );
            }
        }
    }

    #[test]
    fn semantic_only_scores_ignore_extreme_timestamps() {
        let cands = vec![
            Candidate {
                created_micros: i64::MIN,
                ..cand(1, 0.0, 0.0, 0.0)
            },
            cand(2, 0.5, 0.0, 0.0),
            Candidate {
                created_micros: i64::MAX,
                ..cand(3, 1.0, 0.0, 0.0)
            },
        ];
        for now in [i64::MIN, 0, 2, i64::MAX] {
            assert_eq!(
                fusion_scores(&cands, FusionWeights::semantic_only(), now),
                [1.0, 0.5, 0.0]
            );
        }
    }

    #[test]
    fn rerank_pretrim_keeps_best_distance_with_extreme_finite_signals() {
        for extreme_importance in [false, true] {
            let mut cands: Vec<_> = (0..RERANK_POOL)
                .map(|i| {
                    if extreme_importance {
                        cand(i as AtomId, 1.0, 0.0, -f32::MAX)
                    } else {
                        cand(i as AtomId, f32::MAX, 0.0, 0.0)
                    }
                })
                .collect();
            cands.push(Candidate {
                dist: Some(if extreme_importance { 0.0 } else { -f32::MAX }),
                importance: if extreme_importance { f32::MAX } else { 0.0 },
                ..cand_text(RERANK_POOL as AtomId, "target")
            });
            let hits = fuse_rerank(
                &MockReranker,
                cands,
                FusionWeights::semantic_only(),
                0,
                RerankContext {
                    query: "target",
                    strategy: RerankStrategy::Replace,
                    k: 1,
                    cancel: None,
                },
            )
            .unwrap();
            assert_eq!(hits[0].id, RERANK_POOL as AtomId);
        }
    }

    #[test]
    fn nearest_with_keyword_ranks_first() {
        let now = 0;
        let w = FusionWeights::default();
        let cands = vec![
            cand(1, 0.1, 0.9, 0.0),
            cand(2, 0.9, 0.0, 0.0),
            cand(3, 0.5, 0.1, 0.0),
        ];
        let hits = fuse_rank(cands, w, now, 3);
        assert_eq!(hits[0].id, 1);
        assert_eq!(hits.last().unwrap().id, 2);
    }

    #[test]
    fn ties_break_by_id_for_deterministic_order() {
        // equal scores tie-break by ascending id, so recall order is reproducible.
        let w = FusionWeights::default();
        let hits = fuse_rank(
            vec![cand(2, 0.5, 0.5, 0.5), cand(1, 0.5, 0.5, 0.5)],
            w,
            0,
            2,
        );
        assert_eq!(hits[0].id, 1);
        assert_eq!(hits[1].id, 2);
    }

    #[test]
    fn truncates_to_k() {
        let hits = fuse_rank(
            vec![
                cand(1, 0.1, 0.0, 0.0),
                cand(2, 0.2, 0.0, 0.0),
                cand(3, 0.3, 0.0, 0.0),
            ],
            FusionWeights::default(),
            0,
            2,
        );
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn importance_breaks_ties_when_distance_equal() {
        let hits = fuse_rank(
            vec![cand(1, 0.5, 0.0, 0.1), cand(2, 0.5, 0.0, 0.9)],
            FusionWeights::default(),
            0,
            2,
        );
        assert_eq!(hits[0].id, 2, "higher importance should rank first");
    }

    #[test]
    fn empty_in_empty_out() {
        assert!(fuse_rank(Vec::new(), FusionWeights::default(), 0, 5).is_empty());
        assert!(fuse_rerank(
            &MockReranker,
            Vec::new(),
            FusionWeights::default(),
            0,
            RerankContext {
                query: "q",
                strategy: RerankStrategy::Replace,
                k: 5,
                cancel: None,
            },
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn rerank_replace_orders_by_cross_encoder_score() {
        // MockReranker scores by word overlap, so the passage echoing the query wins.
        let cands = vec![
            cand_text(1, "the sky is blue today"),
            cand_text(2, "quick brown fox jumps"),
            cand_text(3, "brown fox"),
        ];
        let hits = fuse_rerank(
            &MockReranker,
            cands,
            FusionWeights::default(),
            0,
            RerankContext {
                query: "quick brown fox",
                strategy: RerankStrategy::Replace,
                k: 2,
                cancel: None,
            },
        )
        .unwrap();
        assert_eq!(hits[0].id, 2, "most word overlap ranks first");
        assert_eq!(hits.len(), 2, "truncated to k");
        assert!(
            hits[0].relevance >= hits[1].relevance,
            "relevance descending"
        );
    }

    #[test]
    fn rerank_rrf_blends_fusion_and_cross_encoder() {
        // RRF blends both rankings; cand 2 wins on overlap and a small dist.
        let cands = vec![
            Candidate {
                dist: Some(0.1),
                ..cand_text(1, "the sky is blue today")
            },
            Candidate {
                dist: Some(0.2),
                ..cand_text(2, "quick brown fox jumps over")
            },
            Candidate {
                dist: Some(0.9),
                ..cand_text(3, "brown fox")
            },
        ];
        let hits = fuse_rerank(
            &MockReranker,
            cands,
            FusionWeights::default(),
            0,
            RerankContext {
                query: "quick brown fox",
                strategy: RerankStrategy::Rrf { k: 60.0 },
                k: 3,
                cancel: None,
            },
        )
        .unwrap();
        assert_eq!(hits[0].id, 2, "high on both rankings leads under RRF");
        assert_eq!(hits.len(), 3);
        assert!(
            hits[0].relevance >= hits[1].relevance && hits[1].relevance >= hits[2].relevance,
            "RRF scores descending"
        );
    }

    fn hit(id: AtomId, text: &str) -> AtomHit {
        AtomHit {
            id,
            kind: "fact".into(),
            text: text.into(),
            payload: Json::Null,
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

    struct FixedScoreReranker(f32);

    impl Reranker for FixedScoreReranker {
        fn model_id(&self) -> &str {
            "fixed-score"
        }

        fn rerank_with_cancel(
            &self,
            _: &str,
            passages: &[&str],
            cancel: Option<&CancelToken>,
        ) -> Result<Vec<f32>, EmbedError> {
            crate::embed::check_cancel(cancel)?;
            Ok(vec![self.0; passages.len()])
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum RerankPath {
        Candidates,
        Hits,
    }

    fn rerank_fixed_score(
        path: RerankPath,
        score: f32,
        strategy: RerankStrategy,
    ) -> Result<Vec<AtomHit>, EmbedError> {
        let reranker = FixedScoreReranker(score);
        let context = RerankContext {
            query: "query",
            strategy,
            k: 1,
            cancel: None,
        };
        match path {
            RerankPath::Candidates => fuse_rerank(
                &reranker,
                vec![cand_text(1, "passage")],
                FusionWeights::default(),
                0,
                context,
            ),
            RerankPath::Hits => rerank_hits(&reranker, vec![hit(1, "passage")], context),
        }
    }

    #[test]
    fn reranker_score_validation_preserves_cardinality_errors() {
        for scores in [&[0.0][..], &[f32::NAN][..]] {
            let error = validate_reranker_scores(scores, 2).unwrap_err();
            assert!(error
                .to_string()
                .contains("reranker returned 1 scores for 2 passages"));
        }
    }

    #[test]
    fn reranking_rejects_nonfinite_backend_scores() {
        let mut failures = Vec::new();
        for path in [RerankPath::Candidates, RerankPath::Hits] {
            for strategy in [RerankStrategy::Replace, RerankStrategy::Rrf { k: 20.0 }] {
                for score in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
                    match rerank_fixed_score(path, score, strategy) {
                        Err(EmbedError::Backend(message)) if message.contains("non-finite") => {}
                        result => {
                            failures.push(format!("{path:?}, {strategy:?}, {score:?}: {result:?}"))
                        }
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[test]
    fn reranking_accepts_finite_backend_scores_without_clamping() {
        for path in [RerankPath::Candidates, RerankPath::Hits] {
            for strategy in [RerankStrategy::Replace, RerankStrategy::Rrf { k: 20.0 }] {
                for score in [f32::MIN, -7.5, -0.0, 0.0, 11.25, f32::MAX] {
                    let hits = rerank_fixed_score(path, score, strategy).unwrap();
                    assert_eq!(hits.len(), 1);
                    assert_eq!(hits[0].id, 1);
                    let expected = match strategy {
                        RerankStrategy::Replace => score,
                        RerankStrategy::Rrf { k } => 1.0 / k + 1.0 / k,
                    };
                    assert_eq!(hits[0].relevance.unwrap().to_bits(), expected.to_bits());
                }
            }
        }
    }

    #[test]
    fn rrf_merge_scores_cross_list_atoms_highest() {
        // Atom 1 appears at rank 0 in both lists; single-list atoms trail it.
        let merged = rrf_merge(
            vec![
                vec![hit(1, "a"), hit(2, "b")],
                vec![hit(1, "a"), hit(3, "c")],
            ],
            60.0,
        );
        assert_eq!(merged[0].id, 1);
        assert_eq!(merged.len(), 3, "deduped by id");
        let expect = 2.0 / 60.0;
        assert!((merged[0].relevance.unwrap() - expect).abs() < 1e-6);
    }

    #[test]
    fn rrf_merge_ties_break_by_id() {
        // Atoms 2 and 3 both hold rank 1 in one list: equal score, id order.
        let merged = rrf_merge(
            vec![
                vec![hit(1, "a"), hit(3, "c")],
                vec![hit(1, "a"), hit(2, "b")],
            ],
            60.0,
        );
        assert_eq!(merged[1].id, 2);
        assert_eq!(merged[2].id, 3);
    }

    #[test]
    fn rrf_merge_empty_lists() {
        assert!(rrf_merge(Vec::new(), 60.0).is_empty());
        assert!(rrf_merge(vec![Vec::new(), Vec::new()], 60.0).is_empty());
    }

    #[test]
    fn rerank_hits_replace_orders_by_cross_encoder() {
        let pool = vec![
            hit(1, "the sky is blue today"),
            hit(2, "quick brown fox jumps"),
        ];
        let hits = rerank_hits(
            &MockReranker,
            pool,
            RerankContext {
                query: "quick brown fox",
                strategy: RerankStrategy::Replace,
                k: 2,
                cancel: None,
            },
        )
        .unwrap();
        assert_eq!(hits[0].id, 2, "most word overlap ranks first");
    }

    #[test]
    fn rerank_hits_truncates_to_k() {
        let pool = vec![hit(1, "a b"), hit(2, "c d"), hit(3, "e f")];
        let hits = rerank_hits(
            &MockReranker,
            pool,
            RerankContext {
                query: "a b",
                strategy: RerankStrategy::Replace,
                k: 1,
                cancel: None,
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn rerank_hits_rrf_blends_pool_order() {
        // Atom 1 leads the pool, atom 2 wins the cross-encoder; RRF blends both.
        let mut a = hit(1, "unrelated text");
        a.relevance = Some(0.9);
        let mut b = hit(2, "quick brown fox");
        b.relevance = Some(0.1);
        let hits = rerank_hits(
            &MockReranker,
            vec![a, b],
            RerankContext {
                query: "quick brown fox",
                strategy: RerankStrategy::Rrf { k: 60.0 },
                k: 2,
                cancel: None,
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits[0].relevance >= hits[1].relevance);
    }
}
