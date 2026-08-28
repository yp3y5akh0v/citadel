//! Deterministic managed `SimilarTo` weave.

use crate::{AtomId, FetchQuery, MemoryEngine};

/// Default neighbor count and the ceiling beyond which similarity is noise.
pub const WEAVE_NEIGHBORS: usize = 3;
pub const WEAVE_MAX_DISTANCE: f32 = 0.30;

/// Weave algorithm revision; bump on any behavior change.
pub const WEAVE_REVISION: u32 = 2;

const WEAVE_PAGE: usize = 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WeaveStats {
    /// Derived notes processed.
    pub notes: usize,
    /// SimilarTo edges written (re-links of the same pair count once here).
    pub edges: usize,
}

/// Link each derived note to its nearest same-kind atoms, weight 1/(1+distance).
pub fn weave_similar_notes(
    eng: &MemoryEngine,
    region: &str,
    derived_kind: &str,
    neighbors: usize,
    max_distance: f32,
) -> crate::Result<WeaveStats> {
    weave_similar_notes_mode(eng, region, derived_kind, neighbors, max_distance, false)
}

/// Rebuild a legacy weave, adopting every outgoing `SimilarTo` edge as managed.
///
/// This migration removes authored `SimilarTo` edges from the derived notes.
/// Use [`weave_similar_notes`] when ownership tracking is already present.
pub fn weave_similar_notes_replacing(
    eng: &MemoryEngine,
    region: &str,
    derived_kind: &str,
    neighbors: usize,
    max_distance: f32,
) -> crate::Result<WeaveStats> {
    weave_similar_notes_mode(eng, region, derived_kind, neighbors, max_distance, true)
}

fn weave_similar_notes_mode(
    eng: &MemoryEngine,
    region: &str,
    derived_kind: &str,
    neighbors: usize,
    max_distance: f32,
    replace_all_similarity: bool,
) -> crate::Result<WeaveStats> {
    let mut stats = WeaveStats::default();
    let mut after: Option<AtomId> = None;
    loop {
        let mut q = FetchQuery::new(WEAVE_PAGE).with_kind(derived_kind);
        if let Some(id) = after {
            q = q.with_after_id(id);
        }
        let page = eng.fetch_range(region, &q)?;
        let Some(last) = page.last() else {
            break;
        };
        after = Some(last.id);
        for note in &page {
            stats.notes += 1;
            let kinds = vec![derived_kind.to_string()];
            let report = if replace_all_similarity {
                eng.evolve_with_kinds_replacing_similarity(
                    region,
                    note.id,
                    neighbors,
                    max_distance,
                    kinds,
                )?
            } else {
                eng.evolve_with_kinds(region, note.id, neighbors, max_distance, kinds)?
            };
            stats.edges += report.links_added;
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AtomInput, EdgeKind, MockEmbedder};
    use std::sync::Arc;

    fn engine(dir: &std::path::Path) -> MemoryEngine {
        use citadel::{Argon2Profile, DatabaseBuilder};
        let db = DatabaseBuilder::new(dir.join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let eng = MemoryEngine::open(Arc::new(db)).unwrap();
        eng.create_region("r", Arc::new(MockEmbedder::new(64)))
            .unwrap();
        eng
    }

    #[test]
    fn weaves_derived_neighbors_only_and_reruns_converge() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let t = eng
            .remember("r", AtomInput::new("turn", "a raw turn"))
            .unwrap();
        for text in ["fact one", "fact two", "fact three"] {
            eng.remember_derived("r", AtomInput::new("derived", text), &[t], None)
                .unwrap();
        }

        // Accept-all distance: only the structural properties are under test.
        let stats = weave_similar_notes(&eng, "r", "derived", 2, f32::MAX).unwrap();
        assert_eq!(stats.notes, 3);
        assert!(stats.edges > 0);
        assert!(
            stats.edges <= stats.notes * 2,
            "the per-note degree bound holds even when a note does not \
             retrieve itself (asymmetric embedders)"
        );

        let edges = eng
            .fetch_edges(None, None, Some(EdgeKind::SimilarTo))
            .unwrap();
        assert_eq!(edges.len(), stats.edges, "one live edge per link written");
        assert!(
            edges.iter().all(|e| e.dst_id != t && e.src_id != t),
            "turns never enter the SimilarTo web"
        );

        // Idempotent: a second pass re-links the same pairs, no growth.
        let again = weave_similar_notes(&eng, "r", "derived", 2, f32::MAX).unwrap();
        assert_eq!(again, stats);
        let edges_again = eng
            .fetch_edges(None, None, Some(EdgeKind::SimilarTo))
            .unwrap();
        assert_eq!(edges_again.len(), edges.len());
    }

    #[test]
    fn degree_bound_holds_when_a_note_does_not_retrieve_itself() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let t = eng.remember("r", AtomInput::new("turn", "a turn")).unwrap();
        // Four identical notes force the "self not retrieved" shape; degree holds.
        for _ in 0..4 {
            eng.remember_derived("r", AtomInput::new("derived", "identical text"), &[t], None)
                .unwrap();
        }
        let stats = weave_similar_notes(&eng, "r", "derived", 1, f32::MAX).unwrap();
        assert_eq!(stats.notes, 4);
        assert_eq!(
            stats.edges, 4,
            "one link per note even for notes that never retrieve themselves"
        );
    }

    #[test]
    fn empty_region_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        assert_eq!(
            weave_similar_notes(&eng, "r", "derived", 3, 0.3).unwrap(),
            WeaveStats::default()
        );
    }

    #[test]
    fn replacing_weave_adopts_legacy_edges_without_widening_the_kind_filter() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let turn = eng
            .remember("r", AtomInput::new("turn", "raw turn"))
            .unwrap();
        let first = eng
            .remember("r", AtomInput::new("derived", "first note"))
            .unwrap();
        let second = eng
            .remember("r", AtomInput::new("derived", "second note"))
            .unwrap();
        eng.link(first, turn, EdgeKind::SimilarTo, 0.25).unwrap();

        let stats = weave_similar_notes_replacing(&eng, "r", "derived", 1, f32::MAX).unwrap();
        assert_eq!(stats.notes, 2);
        let first_edges = eng
            .fetch_edges(Some(first), None, Some(EdgeKind::SimilarTo))
            .unwrap();
        assert_eq!(first_edges.len(), 1);
        assert_eq!(first_edges[0].dst_id, second);
        assert_ne!(first_edges[0].dst_id, turn);
    }

    #[test]
    fn weave_ignores_an_attached_reranker() {
        use crate::{MockReranker, RerankStrategy};
        // A reranker-equipped engine must weave the same edges as a bare one.
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let plain = engine(d1.path());
        let reranked = engine(d2.path());
        for eng in [&plain, &reranked] {
            let t = eng
                .remember("r", AtomInput::new("turn", "a raw turn"))
                .unwrap();
            for text in [
                "alpha likes hiking",
                "alpha likes climbing",
                "beta plays chess",
                "beta plays checkers",
            ] {
                eng.remember_derived("r", AtomInput::new("derived", text), &[t], None)
                    .unwrap();
            }
        }
        reranked.set_reranker(Arc::new(MockReranker), RerankStrategy::Replace);

        let a = weave_similar_notes(&plain, "r", "derived", 2, f32::MAX).unwrap();
        let b = weave_similar_notes(&reranked, "r", "derived", 2, f32::MAX).unwrap();
        assert_eq!(a, b);
        let edge_pairs = |eng: &MemoryEngine| {
            eng.fetch_edges(None, None, Some(EdgeKind::SimilarTo))
                .unwrap()
                .into_iter()
                .map(|e| (e.src_id, e.dst_id))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            edge_pairs(&plain),
            edge_pairs(&reranked),
            "an attached reranker must not influence the weave"
        );
    }
}
