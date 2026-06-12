//! Multi-signal recall fusion: re-rank ANN candidates by a weighted blend of
//! semantic distance, keyword rank, recency, and importance.

use serde_json::Value as Json;

use crate::embed::{EmbedError, Reranker};
use crate::types::{AtomHit, AtomId, FusionWeights, RerankStrategy};

const RECENCY_HALF_LIFE_DAYS: f32 = 30.0;

/// An ANN candidate with the raw signals recall projected for it.
pub(crate) struct Candidate {
    pub id: AtomId,
    pub kind: String,
    pub text: String,
    pub payload: Json,
    pub dist: f32,
    pub text_rank: f32,
    pub importance: f32,
    pub created_micros: i64,
    pub immutable: bool,
}

/// Per-candidate fusion score: each signal min-max normalized, then blended by `w`.
fn fusion_scores(cands: &[Candidate], w: FusionWeights, now_micros: i64) -> Vec<f32> {
    let mut dmin = f32::MAX;
    let mut dmax = f32::MIN;
    let mut rmax = 0.0f32;
    let mut imin = f32::MAX;
    let mut imax = f32::MIN;
    for c in cands {
        dmin = dmin.min(c.dist);
        dmax = dmax.max(c.dist);
        rmax = rmax.max(c.text_rank);
        imin = imin.min(c.importance);
        imax = imax.max(c.importance);
    }
    let drange = (dmax - dmin).max(f32::EPSILON);
    let irange = (imax - imin).max(f32::EPSILON);
    let ln2 = std::f32::consts::LN_2;

    cands
        .iter()
        .map(|c| {
            let semantic = (dmax - c.dist) / drange; // nearest -> 1
            let keyword = if rmax > 0.0 { c.text_rank / rmax } else { 0.0 };
            let age_days = (now_micros - c.created_micros).max(0) as f32 / 1e6 / 86_400.0;
            let recency = (-ln2 * age_days / RECENCY_HALF_LIFE_DAYS).exp();
            let importance = (c.importance - imin) / irange;
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
            distance: c.dist,
            score,
            created_at: c.created_micros,
            immutable: c.immutable,
        })
        .collect();

    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
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

/// Re-rank candidates with a cross-encoder, then keep the top `k`. `strategy` is
/// Replace (trust the logit) or Rrf (blend cross-encoder and fusion ranks).
pub(crate) fn fuse_rerank(
    reranker: &dyn Reranker,
    query: &str,
    mut cands: Vec<Candidate>,
    w: FusionWeights,
    now_micros: i64,
    strategy: RerankStrategy,
    k: usize,
) -> std::result::Result<Vec<AtomHit>, EmbedError> {
    if cands.is_empty() {
        return Ok(Vec::new());
    }
    // Scoring every candidate is CPU-bound, so pre-trim to the top RERANK_POOL by
    // linear fusion; the dropped tail is the low-fusion, likely-irrelevant remainder.
    const RERANK_POOL: usize = 256;
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
    let ce_scores = reranker.rerank(query, &passages)?;
    if ce_scores.len() != passages.len() {
        return Err(EmbedError::Backend(format!(
            "reranker returned {} scores for {} passages",
            ce_scores.len(),
            passages.len()
        )));
    }

    let scores: Vec<f32> = match strategy {
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
            distance: c.dist,
            score: s,
            created_at: c.created_micros,
            immutable: c.immutable,
        })
        .collect();
    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.id.cmp(&b.id))
    });
    scored.truncate(k);
    Ok(scored)
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
            dist: 0.0,
            text_rank: 0.0,
            importance: 0.0,
            created_micros: 0,
            immutable: false,
        }
    }

    fn cand(id: AtomId, dist: f32, text_rank: f32, importance: f32) -> Candidate {
        Candidate {
            id,
            kind: "fact".into(),
            text: String::new(),
            payload: Json::Null,
            dist,
            text_rank,
            importance,
            created_micros: 0,
            immutable: false,
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
            "q",
            Vec::new(),
            FusionWeights::default(),
            0,
            RerankStrategy::Replace,
            5,
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
            "quick brown fox",
            cands,
            FusionWeights::default(),
            0,
            RerankStrategy::Replace,
            2,
        )
        .unwrap();
        assert_eq!(hits[0].id, 2, "most word overlap ranks first");
        assert_eq!(hits.len(), 2, "truncated to k");
        assert!(hits[0].score >= hits[1].score, "scores descending");
    }

    #[test]
    fn rerank_rrf_blends_fusion_and_cross_encoder() {
        // RRF blends both rankings; cand 2 wins on overlap and a small dist.
        let cands = vec![
            Candidate {
                dist: 0.1,
                ..cand_text(1, "the sky is blue today")
            },
            Candidate {
                dist: 0.2,
                ..cand_text(2, "quick brown fox jumps over")
            },
            Candidate {
                dist: 0.9,
                ..cand_text(3, "brown fox")
            },
        ];
        let hits = fuse_rerank(
            &MockReranker,
            "quick brown fox",
            cands,
            FusionWeights::default(),
            0,
            RerankStrategy::Rrf { k: 60.0 },
            3,
        )
        .unwrap();
        assert_eq!(hits[0].id, 2, "high on both rankings leads under RRF");
        assert_eq!(hits.len(), 3);
        assert!(
            hits[0].score >= hits[1].score && hits[1].score >= hits[2].score,
            "RRF scores descending"
        );
    }
}
