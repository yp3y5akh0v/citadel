//! Turn importance from provenance citations: zero-LLM, idempotent by construction.

use crate::{AtomId, EdgeKind, FetchQuery, MemoryEngine};
use rustc_hash::{FxHashMap, FxHashSet};

/// Page size for the region turn listing (same scale as the audit's).
const REWEIGHT_PAGE: usize = 1024;

/// Mirrors the audit's cap: deeper chains are rejected there, never walked here.
const REWEIGHT_DEPTH_CAP: usize = super::audit::AUDIT_DEPTH_CAP;

/// Reweight algorithm revision; bump on any behavior change (shape bound apart).
pub const REWEIGHT_REVISION: u32 = 1;

/// Count-to-importance curve; fusion min-max normalizes, so the CURVE decides.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WeightShape {
    /// Raw count: 5 citations pull 5x harder than 1.
    Linear,
    /// sqrt(count): moderate compression, the default.
    #[default]
    Sqrt,
    /// ln(1+count): strongest compression - near-binary cited-vs-not.
    Log,
}

impl WeightShape {
    pub fn apply(self, value: f32) -> f32 {
        match self {
            WeightShape::Linear => value,
            WeightShape::Sqrt => value.sqrt(),
            WeightShape::Log => (1.0 + value).ln(),
        }
    }

    /// Parse a probe knob value; unknown text falls back to the default.
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "linear" => WeightShape::Linear,
            "sqrt" => WeightShape::Sqrt,
            "log" => WeightShape::Log,
            _ => WeightShape::default(),
        }
    }

    /// Stable identity for generation hashing; round-trips parse, never Debug.
    pub fn canonical_name(self) -> &'static str {
        match self {
            WeightShape::Linear => "linear",
            WeightShape::Sqrt => "sqrt",
            WeightShape::Log => "log",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReweightStats {
    /// Turns actually CHANGED; a converged recompute reports 0 (no index churn).
    pub turns: usize,
    /// Turns reached by at least one derived atom's provenance closure.
    pub cited: usize,
}

/// Turn importance = derived atoms whose closure reaches it (deduped), shaped.
pub fn reweight_turns_from_provenance(
    eng: &MemoryEngine,
    region: &str,
    turn_kind: &str,
    shape: WeightShape,
) -> crate::Result<ReweightStats> {
    let mut turn_ids: Vec<AtomId> = Vec::new();
    let mut after: Option<AtomId> = None;
    loop {
        let mut q = FetchQuery::new(REWEIGHT_PAGE).with_kind(turn_kind);
        if let Some(id) = after {
            q = q.with_after_id(id);
        }
        let page = eng.fetch_range(region, &q)?;
        let Some(last) = page.last() else {
            break;
        };
        after = Some(last.id);
        turn_ids.extend(page.iter().map(|h| h.id));
    }
    if turn_ids.is_empty() {
        return Ok(ReweightStats::default());
    }
    let turn_set: FxHashSet<AtomId> = turn_ids.iter().copied().collect();

    // Global listing; region filters below. Outgoing DerivedFrom = derived atom.
    let mut adjacency: FxHashMap<AtomId, Vec<AtomId>> = FxHashMap::default();
    for edge in eng.fetch_edges(None, None, Some(EdgeKind::DerivedFrom))? {
        adjacency.entry(edge.src_id).or_default().push(edge.dst_id);
    }

    let mut citations: FxHashMap<AtomId, u32> = FxHashMap::default();
    for &src in adjacency.keys() {
        let mut visited: FxHashSet<AtomId> = [src].into_iter().collect();
        let mut frontier = vec![src];
        for _ in 0..REWEIGHT_DEPTH_CAP {
            let mut next = Vec::new();
            for &node in &frontier {
                let Some(dsts) = adjacency.get(&node) else {
                    continue;
                };
                for &dst in dsts {
                    if !visited.insert(dst) {
                        continue;
                    }
                    if turn_set.contains(&dst) {
                        *citations.entry(dst).or_insert(0) += 1;
                    } else {
                        next.push(dst);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
    }

    let updates: Vec<(AtomId, f32)> = turn_ids
        .iter()
        .map(|&id| {
            (
                id,
                shape.apply(citations.get(&id).copied().unwrap_or(0) as f32),
            )
        })
        .collect();
    let turns = eng.set_importance(region, &updates)?;
    Ok(ReweightStats {
        turns,
        cited: citations.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AtomInput, MockEmbedder, RecallProfile, RecallQuery};
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
    fn closure_citations_become_importance_and_rerank_recall() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let t = 1_700_000_000_000_000i64;
        // Twins in every signal; only citations will differ.
        let cited = eng
            .remember(
                "r",
                AtomInput::new("turn", "the day at the lake house").with_created_at(t),
            )
            .unwrap();
        let uncited = eng
            .remember(
                "r",
                AtomInput::new("turn", "the day at the lake house").with_created_at(t),
            )
            .unwrap();
        let fact = eng
            .remember_derived(
                "r",
                AtomInput::new("derived", "a fact about the lake house"),
                &[cited],
                None,
            )
            .unwrap();
        // The timeline reaches the turn THROUGH the fact: transitive credit.
        eng.remember_derived(
            "r",
            AtomInput::new("derived", "an entity timeline"),
            &[fact],
            None,
        )
        .unwrap();

        // turns counts CHANGES: 0 -> 2 for the cited twin; the stored zero skips.
        let stats = reweight_turns_from_provenance(&eng, "r", "turn", WeightShape::Linear).unwrap();
        assert_eq!(stats, ReweightStats { turns: 1, cited: 1 });
        // fact (direct) + timeline (through the fact) = closure count 2.
        let hit = eng.fetch_one("r", cited).unwrap().unwrap();
        assert_eq!(hit.score, 2.0, "transitive citation counted");

        let hits = eng
            .recall(
                "r",
                RecallProfile::default().apply(RecallQuery::by_text("lake house day", 2)),
            )
            .unwrap();
        assert_eq!(
            hits.iter().map(|h| h.id).collect::<Vec<_>>(),
            vec![cited, uncited],
            "the fact-cited twin outranks the uncited one"
        );

        // Idempotent: a converged recompute writes nothing at all.
        let again = reweight_turns_from_provenance(&eng, "r", "turn", WeightShape::Linear).unwrap();
        assert_eq!(again, ReweightStats { turns: 0, cited: 1 });
    }

    #[test]
    fn diamond_closures_count_once_per_atom_and_shapes_compress() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let t = eng
            .remember("r", AtomInput::new("turn", "shared turn"))
            .unwrap();
        let f1 = eng
            .remember_derived("r", AtomInput::new("derived", "f1"), &[t], None)
            .unwrap();
        let f2 = eng
            .remember_derived("r", AtomInput::new("derived", "f2"), &[t], None)
            .unwrap();
        // Timeline reaches the turn via BOTH facts: one credit, count = 3.
        eng.remember_derived("r", AtomInput::new("derived", "tl"), &[f1, f2], None)
            .unwrap();

        reweight_turns_from_provenance(&eng, "r", "turn", WeightShape::Linear).unwrap();
        assert_eq!(eng.fetch_one("r", t).unwrap().unwrap().score, 3.0);

        reweight_turns_from_provenance(&eng, "r", "turn", WeightShape::Sqrt).unwrap();
        let sqrt = eng.fetch_one("r", t).unwrap().unwrap().score;
        assert!((sqrt - 3f32.sqrt()).abs() < 1e-6);

        reweight_turns_from_provenance(&eng, "r", "turn", WeightShape::Log).unwrap();
        let log = eng.fetch_one("r", t).unwrap().unwrap().score;
        assert!((log - 4f32.ln()).abs() < 1e-6);
    }

    #[test]
    fn erased_citations_deterministically_reset_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let t = eng
            .remember("r", AtomInput::new("turn", "alpha turn"))
            .unwrap();
        let fact = eng
            .remember_derived("r", AtomInput::new("derived", "alpha fact"), &[t], None)
            .unwrap();
        let stats = reweight_turns_from_provenance(&eng, "r", "turn", WeightShape::Linear).unwrap();
        assert_eq!(stats.cited, 1);

        // Erased fact: the recompute must drop the orphaned weight, not accumulate.
        eng.delete_atoms("r", &[fact]).unwrap();
        let stats = reweight_turns_from_provenance(&eng, "r", "turn", WeightShape::Linear).unwrap();
        assert_eq!(stats, ReweightStats { turns: 1, cited: 0 });
        assert_eq!(eng.fetch_one("r", t).unwrap().unwrap().score, 0.0);
    }

    #[test]
    fn empty_region_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        assert_eq!(
            reweight_turns_from_provenance(&eng, "r", "turn", WeightShape::Linear).unwrap(),
            ReweightStats::default()
        );
    }

    #[test]
    fn shape_parse_falls_back_to_the_sqrt_default() {
        assert_eq!(WeightShape::parse("sqrt"), WeightShape::Sqrt);
        assert_eq!(WeightShape::parse("LOG"), WeightShape::Log);
        assert_eq!(WeightShape::parse("linear"), WeightShape::Linear);
        assert_eq!(WeightShape::parse("garbage"), WeightShape::Sqrt);
        assert_eq!(WeightShape::parse(""), WeightShape::Sqrt);
    }
}
