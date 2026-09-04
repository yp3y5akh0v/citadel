//! Deterministic spreading activation: co-evidence surfaces at zero lexical overlap.

use crate::{AtomId, EdgeKind, FetchQuery, MemoryEngine};
use rustc_hash::{FxHashMap, FxHashSet};

/// turn -> fact -> fact/timeline -> turn needs 3; more recirculates capped mass.
const ACTIVATION_HOPS: usize = 3;

/// Per-hop decay of transmitted activation.
const ACTIVATION_DECAY: f32 = 0.5;

/// Activation ceiling: a hub cited by everything saturates instead of dominating.
const ACTIVATION_CAP: f32 = 1.0;

const ACTIVATION_PAGE: usize = 1024;

/// The interned diffusion universe; edges are undirected and built once per cache slot.
struct Diffusion {
    ids: Vec<AtomId>,
    index: FxHashMap<AtomId, usize>,
    adjacency: Vec<Vec<(usize, f32)>>,
    fan: Vec<f32>,
    turn_ids: Vec<AtomId>,
}

/// Build the diffusion graph, `None` without turns; ordered edges = deterministic.
fn build_diffusion(
    eng: &MemoryEngine,
    region: &str,
    turn_kind: &str,
    derived_kind: &str,
) -> crate::Result<Option<Diffusion>> {
    let turn_ids = page_kind_ids(eng, region, turn_kind)?;
    if turn_ids.is_empty() {
        return Ok(None);
    }
    let derived_ids = page_kind_ids(eng, region, derived_kind)?;
    let region_ids: FxHashSet<AtomId> = turn_ids.iter().chain(&derived_ids).copied().collect();

    let mut index: FxHashMap<AtomId, usize> = FxHashMap::default();
    let mut ids: Vec<AtomId> = Vec::new();
    let intern = |id: AtomId, ids: &mut Vec<AtomId>, index: &mut FxHashMap<AtomId, usize>| {
        *index.entry(id).or_insert_with(|| {
            ids.push(id);
            ids.len() - 1
        })
    };
    for &t in &turn_ids {
        intern(t, &mut ids, &mut index);
    }
    let mut adjacency: Vec<Vec<(usize, f32)>> = vec![Vec::new(); ids.len()];
    for kind in [EdgeKind::DerivedFrom, EdgeKind::SimilarTo] {
        for edge in eng.fetch_all_edges_in_region(region, None, None, Some(kind))? {
            if !region_ids.contains(&edge.src_id) || !region_ids.contains(&edge.dst_id) {
                continue;
            }
            let s = intern(edge.src_id, &mut ids, &mut index);
            let d = intern(edge.dst_id, &mut ids, &mut index);
            while adjacency.len() < ids.len() {
                adjacency.push(Vec::new());
            }
            let w = if kind == EdgeKind::SimilarTo {
                edge.weight
            } else {
                1.0
            };
            // Mutual pairs double-transmit; kept as-is.
            adjacency[s].push((d, w));
            adjacency[d].push((s, w));
        }
    }
    while adjacency.len() < ids.len() {
        adjacency.push(Vec::new());
    }

    // Fan normalization: divide by sender's total weight so hubs spread thinner.
    let fan: Vec<f32> = adjacency
        .iter()
        .map(|n| n.iter().map(|(_, w)| w).sum::<f32>().max(1.0))
        .collect();

    Ok(Some(Diffusion {
        ids,
        index,
        adjacency,
        fan,
        turn_ids,
    }))
}

impl Diffusion {
    /// Synchronous id-ordered activation from `seeds`; `cap` bounds hub mass.
    fn run(&self, seeds: &[(AtomId, f32)], cap: f32) -> Vec<f32> {
        let mut activation = vec![0.0f32; self.ids.len()];
        for &(id, s) in seeds {
            if let Some(&i) = self.index.get(&id) {
                activation[i] = s.min(cap);
            }
        }
        for _ in 0..ACTIVATION_HOPS {
            let mut next = activation.clone();
            for (i, neighbors) in self.adjacency.iter().enumerate() {
                if activation[i] == 0.0 {
                    continue;
                }
                let send = activation[i] * ACTIVATION_DECAY / self.fan[i];
                for &(j, w) in neighbors {
                    next[j] = (next[j] + send * w).min(cap);
                }
            }
            activation = next;
        }
        activation
    }
}

/// A graph cache bound to one database and region until reset.
#[derive(Default)]
pub struct DiffusionCache {
    slot: std::sync::Mutex<Option<CachedDiffusion>>,
}

impl DiffusionCache {
    /// Drop the cached graph; the controller must serialize resets against reads.
    pub fn reset(&self) {
        *self.slot.lock().unwrap() = None;
    }
}

struct CachedDiffusion {
    database: std::sync::Weak<citadel::Database>,
    region: String,
    turn_kind: String,
    derived_kind: String,
    graph: std::sync::Arc<Diffusion>,
}

impl CachedDiffusion {
    fn checked_graph(
        &self,
        database: &std::sync::Weak<citadel::Database>,
        region: &str,
        turn_kind: &str,
        derived_kind: &str,
    ) -> crate::Result<std::sync::Arc<Diffusion>> {
        if !self.database.ptr_eq(database) {
            return Err(crate::MemError::Invalid(
                "diffusion cache bound to a different database; reset before reuse".into(),
            ));
        }
        if self.region != region || self.turn_kind != turn_kind || self.derived_kind != derived_kind
        {
            return Err(crate::MemError::Invalid(format!(
                "diffusion cache bound to region '{}' kinds '{}'/'{}', not '{region}' \
                 '{turn_kind}'/'{derived_kind}'",
                self.region, self.turn_kind, self.derived_kind
            )));
        }
        Ok(std::sync::Arc::clone(&self.graph))
    }
}

fn graph_for(
    cache: &DiffusionCache,
    eng: &MemoryEngine,
    region: &str,
    turn_kind: &str,
    derived_kind: &str,
) -> crate::Result<Option<std::sync::Arc<Diffusion>>> {
    let database = eng.database_identity();
    if let Some(c) = cache.slot.lock().unwrap().as_ref() {
        return c
            .checked_graph(&database, region, turn_kind, derived_kind)
            .map(Some);
    }
    let built = build_diffusion(eng, region, turn_kind, derived_kind)?;
    let mut slot = cache.slot.lock().unwrap();
    if let Some(c) = slot.as_ref() {
        return c
            .checked_graph(&database, region, turn_kind, derived_kind)
            .map(Some);
    }
    let Some(g) = built else {
        return Ok(None);
    };
    let g = std::sync::Arc::new(g);
    *slot = Some(CachedDiffusion {
        database,
        region: region.to_string(),
        turn_kind: turn_kind.to_string(),
        derived_kind: derived_kind.to_string(),
        graph: std::sync::Arc::clone(&g),
    });
    Ok(Some(g))
}

/// [`activation_scores_cached`] keeping only the ranked turn ids - the
/// read-path view order.
pub fn activation_rerank_cached(
    cache: &DiffusionCache,
    eng: &MemoryEngine,
    region: &str,
    turn_kind: &str,
    derived_kind: &str,
    seeds: &[(AtomId, f32)],
    k: usize,
) -> crate::Result<Vec<AtomId>> {
    Ok(
        activation_scores_cached(cache, eng, region, turn_kind, derived_kind, seeds, k)?
            .into_iter()
            .map(|(id, _)| id)
            .collect(),
    )
}

/// Return up to `k` cached turns as `(atom id, activation)`, ordered by
/// descending activation then ascending id. Seeds may be turns or derived notes.
///
/// The cached graph is a snapshot, not a live view. Callers must exclude
/// mutations while building it and reset before reuse after atom, edge, or
/// region mutations, including erasure, or TTL changes/expiry. Cache hits do
/// not revalidate atom liveness or automatically invalidate the snapshot.
pub fn activation_scores_cached(
    cache: &DiffusionCache,
    eng: &MemoryEngine,
    region: &str,
    turn_kind: &str,
    derived_kind: &str,
    seeds: &[(AtomId, f32)],
    k: usize,
) -> crate::Result<Vec<(AtomId, f32)>> {
    let Some(graph) = graph_for(cache, eng, region, turn_kind, derived_kind)? else {
        return Ok(Vec::new());
    };
    Ok(rank_turns(&graph, seeds, k))
}

/// Activation-ordered `(turn id, score)` prefix of a built graph.
fn rank_turns(graph: &Diffusion, seeds: &[(AtomId, f32)], k: usize) -> Vec<(AtomId, f32)> {
    let activation = graph.run(seeds, ACTIVATION_CAP);
    let mut ranked: Vec<(AtomId, f32)> = graph
        .turn_ids
        .iter()
        .map(|&id| (id, activation[graph.index[&id]]))
        .collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then(a.0.cmp(&b.0)));
    ranked.truncate(k);
    ranked
}

/// All live ids of `kind` in the region, paged in id order.
fn page_kind_ids(eng: &MemoryEngine, region: &str, kind: &str) -> crate::Result<Vec<AtomId>> {
    let mut out: Vec<AtomId> = Vec::new();
    let mut after: Option<AtomId> = None;
    loop {
        let mut q = FetchQuery::new(ACTIVATION_PAGE).with_kind(kind);
        if let Some(id) = after {
            q = q.with_after_id(id);
        }
        let page = eng.fetch_range(region, &q)?;
        let Some(last) = page.last() else {
            break;
        };
        after = Some(last.id);
        out.extend(page.iter().map(|h| h.id));
    }
    Ok(out)
}

/// Guarantee `pinned` ids a slot in `ranked`: each absent id evicts the
/// lowest-ranked entry that is not itself pinned, then joins at the tail
/// (its diffusion rank was below the cut, so the tail is its true
/// position). Everything else keeps its order. If fewer unpinned entries
/// exist than absent pinned ids, the view grows rather than dropping a
/// pinned id.
pub fn pin_into_view(ranked: &mut Vec<AtomId>, pinned: &[AtomId]) {
    let have: FxHashSet<AtomId> = ranked.iter().copied().collect();
    let missing: Vec<AtomId> = pinned
        .iter()
        .copied()
        .filter(|id| !have.contains(id))
        .collect();
    if missing.is_empty() {
        return;
    }
    let keep: FxHashSet<AtomId> = pinned.iter().copied().collect();
    let mut evict = missing.len();
    let mut i = ranked.len();
    while evict > 0 && i > 0 {
        i -= 1;
        if !keep.contains(&ranked[i]) {
            ranked.remove(i);
            evict -= 1;
        }
    }
    ranked.extend(missing);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AtomInput;

    fn engine(dir: &std::path::Path) -> MemoryEngine {
        engine_with_database(dir).1
    }

    fn engine_with_database(
        dir: &std::path::Path,
    ) -> (std::sync::Arc<citadel::Database>, MemoryEngine) {
        use crate::MockEmbedder;
        use citadel::{Argon2Profile, DatabaseBuilder};
        use std::sync::Arc;
        let db = Arc::new(
            DatabaseBuilder::new(dir.join("m.db"))
                .passphrase(b"test-passphrase")
                .argon2_profile(Argon2Profile::Iot)
                .create()
                .unwrap(),
        );
        let eng = MemoryEngine::open(Arc::clone(&db)).unwrap();
        eng.create_region("r", Arc::new(MockEmbedder::new(64)))
            .unwrap();
        (db, eng)
    }

    /// Single-shot rank through a fresh cache (the only production path).
    fn rank(eng: &MemoryEngine, seeds: &[(AtomId, f32)], k: usize) -> Vec<AtomId> {
        activation_rerank_cached(
            &DiffusionCache::default(),
            eng,
            "r",
            "turn",
            "derived",
            seeds,
            k,
        )
        .unwrap()
    }

    #[test]
    fn co_evidence_rises_above_unconnected_noise() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let strong = eng
            .remember("r", AtomInput::new("turn", "strong hit"))
            .unwrap();
        let hidden = eng
            .remember("r", AtomInput::new("turn", "hidden co-evidence"))
            .unwrap();
        let noise = eng.remember("r", AtomInput::new("turn", "noise")).unwrap();
        // The sweep bound strong+hidden through one fact; noise is isolated.
        eng.remember_derived(
            "r",
            AtomInput::new("derived", "a fact"),
            &[strong, hidden],
            None,
        )
        .unwrap();

        // Fusion found only `strong` (rank 1) and `noise` (rank 2); `hidden`
        // was invisible to it. Activation must pull `hidden` above `noise`.
        let ranked = rank(&eng, &[(strong, 1.0), (noise, 0.2)], 3);
        assert_eq!(ranked[0], strong);
        assert_eq!(
            ranked[1], hidden,
            "co-cited evidence outranks an unconnected mid-seed turn"
        );
        assert_eq!(ranked[2], noise);
    }

    #[test]
    fn similar_fact_bridge_carries_activation_across_wording() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let seed_turn = eng.remember("r", AtomInput::new("turn", "seed")).unwrap();
        let far_turn = eng
            .remember("r", AtomInput::new("turn", "far evidence"))
            .unwrap();
        let noise = eng.remember("r", AtomInput::new("turn", "noise")).unwrap();
        let f1 = eng
            .remember_derived("r", AtomInput::new("derived", "f1"), &[seed_turn], None)
            .unwrap();
        let f2 = eng
            .remember_derived("r", AtomInput::new("derived", "f2"), &[far_turn], None)
            .unwrap();
        // The weave linked the two facts; that is the only bridge.
        eng.link(f1, f2, EdgeKind::SimilarTo, 0.9).unwrap();

        // Noise seeded at deep-tail rank mass: a three-hop verified bridge
        // must beat tail noise, but deliberately NOT a mid-ranked hit
        // (similar-distractor precision - weak trickles shouldn't outrank
        // real fusion signal).
        let ranked = rank(&eng, &[(seed_turn, 1.0), (noise, 0.02)], 3);
        assert_eq!(
            ranked[1], far_turn,
            "three-hop chain seed->f1~f2->turn beats unconnected tail noise"
        );
    }

    #[test]
    fn cross_region_edges_never_enter_the_diffusion_universe() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        eng.create_region("other", Arc::new(crate::MockEmbedder::new(64)))
            .unwrap();
        let seed = eng.remember("r", AtomInput::new("turn", "seed")).unwrap();
        let hidden = eng.remember("r", AtomInput::new("turn", "hidden")).unwrap();
        let noise = eng.remember("r", AtomInput::new("turn", "noise")).unwrap();
        let foreign = eng
            .remember("other", AtomInput::new("derived", "foreign bridge"))
            .unwrap();
        eng.link(seed, foreign, EdgeKind::SimilarTo, 1.0).unwrap();
        eng.link(foreign, hidden, EdgeKind::SimilarTo, 1.0).unwrap();

        let ranked = rank(&eng, &[(seed, 1.0), (noise, 0.2)], 3);
        assert_eq!(ranked, vec![seed, noise, hidden]);
    }

    #[test]
    fn deterministic_and_capped() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let hub = eng
            .remember("r", AtomInput::new("turn", "hub turn"))
            .unwrap();
        let mut others = Vec::new();
        for i in 0..12 {
            let t = eng
                .remember("r", AtomInput::new("turn", format!("t{i}")))
                .unwrap();
            eng.remember_derived(
                "r",
                AtomInput::new("derived", format!("fact {i}")),
                &[hub, t],
                None,
            )
            .unwrap();
            others.push(t);
        }
        let seeds = vec![(others[0], 1.0)];
        let a = rank(&eng, &seeds, 13);
        let b = rank(&eng, &seeds, 13);
        assert_eq!(a, b, "bit-identical across runs");
        // The hub is cited by every fact but the cap + fan normalization
        // keep it below the actual seed.
        assert_eq!(a[0], others[0], "seed stays on top despite the hub");
    }

    #[test]
    fn cache_hit_matches_the_build_path_across_independent_caches() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let a = eng.remember("r", AtomInput::new("turn", "alpha")).unwrap();
        let b = eng.remember("r", AtomInput::new("turn", "beta")).unwrap();
        let c = eng.remember("r", AtomInput::new("turn", "gamma")).unwrap();
        eng.remember_derived("r", AtomInput::new("derived", "fact"), &[a, c], None)
            .unwrap();

        let seeds = vec![(a, 1.0), (b, 0.4)];
        let cache = DiffusionCache::default();
        let first =
            activation_rerank_cached(&cache, &eng, "r", "turn", "derived", &seeds, 3).unwrap();
        // Second call takes the cached-graph path; results must not differ.
        let second =
            activation_rerank_cached(&cache, &eng, "r", "turn", "derived", &seeds, 3).unwrap();
        assert_eq!(first, second, "cache hit matches the build path");
        // An independently built cache agrees: construction is deterministic.
        let other = activation_rerank_cached(
            &DiffusionCache::default(),
            &eng,
            "r",
            "turn",
            "derived",
            &seeds,
            3,
        )
        .unwrap();
        assert_eq!(first, other, "independent caches build the same graph");
        // The scores variant ranks identically (shared rank_turns).
        let scored = activation_scores_cached(
            &DiffusionCache::default(),
            &eng,
            "r",
            "turn",
            "derived",
            &seeds,
            3,
        )
        .unwrap();
        assert_eq!(
            first,
            scored.iter().map(|&(id, _)| id).collect::<Vec<_>>(),
            "rerank is exactly the id projection of the scores"
        );
    }

    #[test]
    fn cache_refuses_mismatched_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let a = eng.remember("r", AtomInput::new("turn", "alpha")).unwrap();
        let cache = DiffusionCache::default();
        activation_rerank_cached(&cache, &eng, "r", "turn", "derived", &[(a, 1.0)], 1).unwrap();

        let err = activation_rerank_cached(&cache, &eng, "r", "turn", "note", &[(a, 1.0)], 1)
            .unwrap_err();
        assert!(err.to_string().contains("diffusion cache bound"));
        let err =
            activation_rerank_cached(&cache, &eng, "other", "turn", "derived", &[(a, 1.0)], 1)
                .unwrap_err();
        assert!(err.to_string().contains("diffusion cache bound"));

        // A reset drops the binding: the refused inputs now rebuild cleanly.
        cache.reset();
        activation_rerank_cached(&cache, &eng, "r", "turn", "note", &[(a, 1.0)], 1).unwrap();
    }

    #[test]
    fn cache_refuses_a_different_database_until_reset() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let first = engine(first_dir.path());
        let second = engine(second_dir.path());
        let insert_turns = |eng: &MemoryEngine| {
            ["seed", "neighbor"]
                .map(|text| eng.remember("r", AtomInput::new("turn", text)).unwrap())
        };
        let first_ids = insert_turns(&first);
        let second_ids = insert_turns(&second);
        assert_eq!(first_ids, second_ids);
        first
            .remember_derived(
                "r",
                AtomInput::new("derived", "shared fact"),
                &first_ids,
                None,
            )
            .unwrap();
        let seeds = [(first_ids[0], 1.0)];
        let score = |cache: &DiffusionCache, eng: &MemoryEngine| {
            activation_scores_cached(cache, eng, "r", "turn", "derived", &seeds, 2)
        };
        let cache = DiffusionCache::default();
        let first_scores = score(&cache, &first).unwrap();
        let second_scores = score(&DiffusionCache::default(), &second).unwrap();
        assert_ne!(first_scores, second_scores);

        let error = score(&cache, &second).unwrap_err();
        assert!(error.to_string().contains("diffusion cache bound"));
        assert_eq!(score(&cache, &first).unwrap(), first_scores);

        cache.reset();
        assert_eq!(score(&cache, &second).unwrap(), second_scores);
    }

    #[test]
    fn cache_accepts_shared_database_handles_without_retaining_the_database() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let (db, first) = engine_with_database(dir.path());
        let id = first.remember("r", AtomInput::new("turn", "seed")).unwrap();
        let second = MemoryEngine::open(Arc::clone(&db)).unwrap();
        second
            .attach_existing_region("r", Arc::new(crate::MockEmbedder::new(64)))
            .unwrap();
        let weak = Arc::downgrade(&db);
        let cache = DiffusionCache::default();
        let first_scores =
            activation_scores_cached(&cache, &first, "r", "turn", "derived", &[(id, 1.0)], 1)
                .unwrap();
        let second_scores =
            activation_scores_cached(&cache, &second, "r", "turn", "derived", &[(id, 1.0)], 1)
                .unwrap();
        assert_eq!(first_scores, second_scores);
        drop(first);
        drop(second);
        drop(db);
        assert!(weak.upgrade().is_none());
        assert!(cache.slot.lock().unwrap().is_some());
    }

    #[test]
    fn concurrent_first_use_binds_exactly_one_database() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let first = engine(first_dir.path());
        let second = engine(second_dir.path());
        let first_id = first
            .remember("r", AtomInput::new("turn", "first database"))
            .unwrap();
        let second_id = second
            .remember("r", AtomInput::new("turn", "second database"))
            .unwrap();
        assert_eq!(first_id, second_id);
        let cache = DiffusionCache::default();
        let start = std::sync::Barrier::new(2);
        let score = |eng: &MemoryEngine| {
            activation_scores_cached(&cache, eng, "r", "turn", "derived", &[(first_id, 1.0)], 1)
        };
        let (first_result, second_result) = std::thread::scope(|scope| {
            let first_call = scope.spawn(|| {
                start.wait();
                score(&first)
            });
            let second_call = scope.spawn(|| {
                start.wait();
                score(&second)
            });
            (first_call.join().unwrap(), second_call.join().unwrap())
        });
        let (winner, loser, error) = match (first_result, second_result) {
            (Ok(_), Err(error)) => (&first, &second, error),
            (Err(error), Ok(_)) => (&second, &first, error),
            other => panic!("expected exactly one database binding, got {other:?}"),
        };
        assert!(error.to_string().contains("diffusion cache bound"));
        assert!(score(winner).is_ok());
        assert!(score(loser).is_err());
    }

    #[test]
    fn pin_evicts_lowest_unpinned_and_appends() {
        let mut ranked = vec![10, 20, 30, 40];
        pin_into_view(&mut ranked, &[99, 20]);
        // 20 already present; 99 evicts the lowest unpinned entry (40).
        assert_eq!(ranked, vec![10, 20, 30, 99]);
    }

    #[test]
    fn pin_present_is_a_no_op() {
        let mut ranked = vec![1, 2, 3];
        pin_into_view(&mut ranked, &[3, 1]);
        assert_eq!(ranked, vec![1, 2, 3]);
    }

    #[test]
    fn pin_eviction_skips_pinned_tail_and_grows_when_exhausted() {
        // Tail entry 3 is itself pinned: the eviction must skip it and take 2.
        let mut ranked = vec![1, 2, 3];
        pin_into_view(&mut ranked, &[3, 7]);
        assert_eq!(ranked, vec![1, 3, 7]);
        // Every entry pinned: nothing evictable, the view grows.
        let mut all_pinned = vec![1, 2];
        pin_into_view(&mut all_pinned, &[1, 2, 9]);
        assert_eq!(all_pinned, vec![1, 2, 9]);
    }

    #[test]
    fn empty_region_and_no_edges_degrade_to_seed_order() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        assert!(rank(&eng, &[], 5).is_empty());
        let a = eng.remember("r", AtomInput::new("turn", "a")).unwrap();
        let b = eng.remember("r", AtomInput::new("turn", "b")).unwrap();
        assert_eq!(rank(&eng, &[(b, 0.9), (a, 0.5)], 2), vec![b, a]);
    }
}
