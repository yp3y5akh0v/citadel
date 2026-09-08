//! Deterministic spreading activation: co-evidence surfaces at zero lexical overlap.

use crate::engine::graph_snapshot::{GraphLiveness, GraphRevision, GraphSnapshot};
use crate::{AtomId, EdgeKind, MemoryEngine};
use rustc_hash::{FxHashMap, FxHashSet};

/// turn -> fact -> fact/timeline -> turn needs 3; more recirculates capped mass.
const ACTIVATION_HOPS: usize = 3;

/// Per-hop decay of transmitted activation.
const ACTIVATION_DECAY: f32 = 0.5;

/// Activation ceiling: a hub cited by everything saturates instead of dominating.
const ACTIVATION_CAP: f32 = 1.0;

/// The interned diffusion universe; edges are undirected and built once per cache slot.
struct Diffusion {
    ids: Vec<AtomId>,
    index: FxHashMap<AtomId, usize>,
    adjacency: Vec<Vec<(usize, f32)>>,
    fan: Vec<Fan>,
    turn_ids: Vec<AtomId>,
    revision: GraphRevision,
    liveness: GraphLiveness,
}

enum Fan {
    Finite(f32),
    Wide(f64),
}

impl Fan {
    fn from_neighbors(neighbors: &[(usize, f32)]) -> Self {
        let total = neighbors.iter().map(|(_, weight)| weight).sum::<f32>();
        if total.is_finite() {
            Self::Finite(total.max(1.0))
        } else {
            Self::Wide(neighbors.iter().map(|(_, weight)| f64::from(*weight)).sum())
        }
    }
}

fn build_diffusion(
    snapshot: &GraphSnapshot<'_, '_>,
    turn_kind: &str,
    derived_kind: &str,
) -> crate::Result<Diffusion> {
    let turns = snapshot.atoms(turn_kind)?;
    if turns.ids.is_empty() {
        return Ok(Diffusion {
            ids: Vec::new(),
            index: FxHashMap::default(),
            adjacency: Vec::new(),
            fan: Vec::new(),
            turn_ids: Vec::new(),
            revision: snapshot.revision(),
            liveness: turns.liveness,
        });
    }
    let derived = snapshot.atoms(derived_kind)?;
    let turn_ids = turns.ids;
    let derived_ids = derived.ids;
    let mut liveness = turns.liveness;
    liveness.extend(derived.liveness);
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
    let sources: Vec<AtomId> = region_ids.iter().copied().collect();
    for (src, dst, kind, weight) in snapshot.edges(&sources)? {
        snapshot.check_cancel()?;
        if kind == EdgeKind::SimilarTo && (!weight.is_finite() || weight < 0.0) {
            return Err(crate::MemError::Invalid(format!(
                "diffusion requires a finite nonnegative SimilarTo weight for edge {src} -> {dst}"
            )));
        }
        let s = intern(src, &mut ids, &mut index);
        let d = intern(dst, &mut ids, &mut index);
        while adjacency.len() < ids.len() {
            adjacency.push(Vec::new());
        }
        let w = if kind == EdgeKind::SimilarTo {
            weight
        } else {
            1.0
        };
        // Mutual pairs double-transmit; kept as-is.
        adjacency[s].push((d, w));
        adjacency[d].push((s, w));
    }
    while adjacency.len() < ids.len() {
        adjacency.push(Vec::new());
    }

    // Fan normalization: divide by sender's total weight so hubs spread thinner.
    let fan = adjacency.iter().map(|n| Fan::from_neighbors(n)).collect();

    Ok(Diffusion {
        ids,
        index,
        adjacency,
        fan,
        turn_ids,
        revision: snapshot.revision(),
        liveness,
    })
}

impl Diffusion {
    /// Synchronous id-ordered activation from `seeds`; `cap` bounds hub mass.
    fn run(
        &self,
        seeds: &[(AtomId, f32)],
        cap: f32,
        check_cancel: impl Fn() -> crate::Result<()>,
    ) -> crate::Result<Vec<f32>> {
        let mut activation = vec![0.0f32; self.ids.len()];
        for &(id, s) in seeds {
            check_cancel()?;
            if let Some(&i) = self.index.get(&id) {
                activation[i] = s.min(cap);
            }
        }
        for _ in 0..ACTIVATION_HOPS {
            let mut next = activation.clone();
            for (i, neighbors) in self.adjacency.iter().enumerate() {
                check_cancel()?;
                if activation[i] == 0.0 {
                    continue;
                }
                match self.fan[i] {
                    Fan::Finite(fan) => {
                        let send = activation[i] * ACTIVATION_DECAY / fan;
                        for &(j, w) in neighbors {
                            check_cancel()?;
                            next[j] = (next[j] + send * w).min(cap);
                        }
                    }
                    Fan::Wide(fan) => {
                        let send = f64::from(activation[i]) * f64::from(ACTIVATION_DECAY) / fan;
                        for &(j, w) in neighbors {
                            check_cancel()?;
                            next[j] = (next[j] + (send * f64::from(w)) as f32).min(cap);
                        }
                    }
                }
            }
            activation = next;
        }
        check_cancel()?;
        Ok(activation)
    }
}

/// A graph cache bound to one database and region until reset.
#[derive(Default)]
pub struct DiffusionCache {
    slot: std::sync::Mutex<Option<CachedDiffusion>>,
}

impl DiffusionCache {
    /// Drop the binding. An in-flight read may populate it again after reset.
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

fn cached_graph(
    cache: &DiffusionCache,
    database: &std::sync::Weak<citadel::Database>,
    region: &str,
    turn_kind: &str,
    derived_kind: &str,
) -> crate::Result<Option<std::sync::Arc<Diffusion>>> {
    if let Some(c) = cache.slot.lock().unwrap().as_ref() {
        return c
            .checked_graph(database, region, turn_kind, derived_kind)
            .map(Some);
    }
    Ok(None)
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
/// Seed scores and participating `SimilarTo` weights must be finite and nonnegative.
///
/// Reads use one validated database snapshot. Mutations and TTL expiry rebuild
/// the graph; cached encrypted bindings are revalidated before use. A concurrent
/// raw database write can fail the read with an explicit retry error.
pub fn activation_scores_cached(
    cache: &DiffusionCache,
    eng: &MemoryEngine,
    region: &str,
    turn_kind: &str,
    derived_kind: &str,
    seeds: &[(AtomId, f32)],
    k: usize,
) -> crate::Result<Vec<(AtomId, f32)>> {
    for &(id, score) in seeds {
        if !score.is_finite() || score < 0.0 {
            return Err(crate::MemError::Invalid(format!(
                "diffusion requires a finite nonnegative seed score for atom {id}"
            )));
        }
    }
    if k == 0 {
        return Ok(Vec::new());
    }
    let database = eng.database_identity();
    let cached = cached_graph(cache, &database, region, turn_kind, derived_kind)?;
    let (graph, ranked) = eng.with_graph_snapshot(region, |snapshot| {
        let graph = match cached {
            Some(graph)
                if graph.revision == snapshot.revision()
                    && snapshot.is_live(&graph.liveness)? =>
            {
                graph
            }
            _ => std::sync::Arc::new(build_diffusion(snapshot, turn_kind, derived_kind)?),
        };
        let ranked = rank_turns(&graph, seeds, k, || snapshot.check_cancel())?;
        snapshot.ensure_live(&graph.liveness)?;
        Ok((graph, ranked))
    })?;
    let mut slot = cache.slot.lock().unwrap();
    if let Some(current) = slot.as_ref() {
        current.checked_graph(&database, region, turn_kind, derived_kind)?;
    }
    let retired = slot.replace(CachedDiffusion {
        database,
        region: region.to_owned(),
        turn_kind: turn_kind.to_owned(),
        derived_kind: derived_kind.to_owned(),
        graph,
    });
    drop(slot);
    drop(retired);
    Ok(ranked)
}

/// Activation-ordered `(turn id, score)` prefix of a built graph.
fn rank_turns(
    graph: &Diffusion,
    seeds: &[(AtomId, f32)],
    k: usize,
    check_cancel: impl Fn() -> crate::Result<()>,
) -> crate::Result<Vec<(AtomId, f32)>> {
    let activation = graph.run(seeds, ACTIVATION_CAP, &check_cancel)?;
    let mut ranked: Vec<(AtomId, f32)> = graph
        .turn_ids
        .iter()
        .map(|&id| (id, activation[graph.index[&id]]))
        .collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then(a.0.cmp(&b.0)));
    ranked.truncate(k);
    check_cancel()?;
    Ok(ranked)
}

/// Guarantee `pinned` ids a slot in `ranked`: each absent id evicts the
/// lowest-ranked entry that is not itself pinned, then joins at the tail
/// (its diffusion rank was below the cut, so the tail is its true
/// position). Everything else keeps its order. If fewer unpinned entries
/// exist than absent pinned ids, the view grows rather than dropping a
/// pinned id. Repeated pins are ignored; missing ids append in first-requested order.
pub fn pin_into_view(ranked: &mut Vec<AtomId>, pinned: &[AtomId]) {
    let mut seen: FxHashSet<AtomId> = ranked.iter().copied().collect();
    let missing: Vec<AtomId> = pinned
        .iter()
        .copied()
        .filter(|id| seen.insert(*id))
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
                .enable_region_keys(true)
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
    fn invalid_seed_scores_fail_on_cold_and_warm_caches() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let atom = eng.remember("r", AtomInput::new("turn", "seed")).unwrap();
        let cache = DiffusionCache::default();
        for warm in [false, true] {
            if warm {
                activation_scores_cached(&cache, &eng, "r", "turn", "derived", &[(atom, 1.0)], 1)
                    .unwrap();
            }
            for score in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -1.0] {
                for id in [atom, i64::MAX] {
                    let error = activation_scores_cached(
                        &cache,
                        &eng,
                        "r",
                        "turn",
                        "derived",
                        &[(id, score)],
                        1,
                    )
                    .unwrap_err();
                    assert!(error.to_string().contains("nonnegative seed score"));
                    assert!(error.to_string().contains(&id.to_string()));
                }
            }
        }
    }

    #[test]
    fn empty_graph_still_rejects_invalid_seed_scores() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let error = activation_scores_cached(
            &DiffusionCache::default(),
            &eng,
            "r",
            "turn",
            "derived",
            &[(1, f32::NAN)],
            0,
        )
        .unwrap_err();
        assert!(error.to_string().contains("nonnegative seed score"));
    }

    #[test]
    fn signed_edges_are_rejected_by_activation_not_by_storage() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let seed = eng.remember("r", AtomInput::new("turn", "seed")).unwrap();
        let neighbor = eng
            .remember("r", AtomInput::new("turn", "neighbor"))
            .unwrap();
        eng.link(seed, neighbor, EdgeKind::SimilarTo, -0.5).unwrap();

        let error = activation_scores_cached(
            &DiffusionCache::default(),
            &eng,
            "r",
            "turn",
            "derived",
            &[(seed, 1.0)],
            2,
        )
        .unwrap_err();
        assert!(error.to_string().contains("nonnegative SimilarTo weight"));
    }

    #[test]
    fn overflowing_positive_fan_preserves_activation() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let ids = ["seed", "first", "second"]
            .map(|text| eng.remember("r", AtomInput::new("turn", text)).unwrap());
        for neighbor in &ids[1..] {
            eng.link(ids[0], *neighbor, EdgeKind::SimilarTo, f32::MAX)
                .unwrap();
        }
        let graph = eng
            .with_graph_snapshot("r", |snapshot| build_diffusion(snapshot, "turn", "derived"))
            .unwrap();
        let activation = graph
            .run(&[(ids[0], 1.0)], ACTIVATION_CAP, || Ok(()))
            .unwrap();
        for neighbor in &ids[1..] {
            let score = activation[graph.index[neighbor]];
            assert_eq!(score, 0.75);
        }
        assert!(activation
            .iter()
            .all(|score| score.is_finite() && *score <= 1.0));
    }

    #[test]
    fn finite_fan_keeps_existing_score_bits() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let seed = eng.remember("r", AtomInput::new("turn", "seed")).unwrap();
        let neighbor = eng
            .remember("r", AtomInput::new("turn", "neighbor"))
            .unwrap();
        eng.link(seed, neighbor, EdgeKind::SimilarTo, 0.25).unwrap();
        let graph = eng
            .with_graph_snapshot("r", |snapshot| build_diffusion(snapshot, "turn", "derived"))
            .unwrap();
        assert!(graph.fan.iter().all(|fan| matches!(fan, Fan::Finite(_))));
        let activation = graph
            .run(
                &[(seed, f32::MAX), (neighbor, -0.0)],
                ACTIVATION_CAP,
                || Ok(()),
            )
            .unwrap();
        assert_eq!(activation[graph.index[&seed]].to_bits(), 1.0_f32.to_bits());
        assert_eq!(
            activation[graph.index[&neighbor]].to_bits(),
            0.375_f32.to_bits()
        );
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
    fn cache_rebuilds_after_edge_and_atom_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let seed = eng.remember("r", AtomInput::new("turn", "seed")).unwrap();
        let neighbor = eng
            .remember("r", AtomInput::new("turn", "neighbor"))
            .unwrap();
        let cache = DiffusionCache::default();
        let score = || {
            activation_scores_cached(&cache, &eng, "r", "turn", "derived", &[(seed, 1.0)], 8)
                .unwrap()
        };
        assert_eq!(score(), vec![(seed, 1.0), (neighbor, 0.0)]);
        eng.link(seed, neighbor, EdgeKind::SimilarTo, 0.5).unwrap();
        assert!(score().iter().find(|(id, _)| *id == neighbor).unwrap().1 > 0.0);
        eng.unlink_in_region("r", seed, neighbor, EdgeKind::SimilarTo)
            .unwrap();
        assert_eq!(score(), vec![(seed, 1.0), (neighbor, 0.0)]);
        let added = eng
            .remember("r", AtomInput::new("turn", "new turn"))
            .unwrap();
        assert!(score().iter().any(|(id, _)| *id == added));
        eng.forget_atom("r", neighbor).unwrap();
        assert!(!score().iter().any(|(id, _)| *id == neighbor));
    }

    #[test]
    fn cache_rebuilds_after_raw_sql_without_a_key_epoch_change() {
        use citadel_sql::Connection;

        let dir = tempfile::tempdir().unwrap();
        let (db, eng) = engine_with_database(dir.path());
        let seed = eng.remember("r", AtomInput::new("turn", "seed")).unwrap();
        let neighbor = eng
            .remember("r", AtomInput::new("turn", "neighbor"))
            .unwrap();
        eng.link(seed, neighbor, EdgeKind::SimilarTo, 0.5).unwrap();
        let cache = DiffusionCache::default();
        let score = || {
            activation_scores_cached(&cache, &eng, "r", "turn", "derived", &[(seed, 1.0)], 2)
                .unwrap()
        };
        assert!(score().iter().find(|(id, _)| *id == neighbor).unwrap().1 > 0.0);
        let epoch = db.cache_epoch();
        Connection::open(&db)
            .unwrap()
            .execute("DELETE FROM memory_edges")
            .unwrap();
        assert_eq!(db.cache_epoch(), epoch);
        assert_eq!(score(), vec![(seed, 1.0), (neighbor, 0.0)]);
    }

    #[test]
    fn cache_rebuilds_after_ttl_expiry_without_a_write() {
        use crate::engine::graph_snapshot::with_test_clock;

        let dir = tempfile::tempdir().unwrap();
        let (db, eng) = engine_with_database(dir.path());
        let expires_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
            + 60_000_000;
        let atom = eng
            .remember(
                "r",
                AtomInput::new("turn", "temporary").with_expires_at(expires_at),
            )
            .unwrap();
        let cache = DiffusionCache::default();
        let score = || {
            activation_scores_cached(&cache, &eng, "r", "turn", "derived", &[(atom, 1.0)], 1)
                .unwrap()
        };
        assert_eq!(with_test_clock(expires_at - 1, score), vec![(atom, 1.0)]);
        let revision = db.manager().commit_generation();
        assert!(with_test_clock(expires_at, score).is_empty());
        assert_eq!(db.manager().commit_generation(), revision);
    }

    #[test]
    fn cache_rejects_a_dropped_region() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let atom = eng.remember("r", AtomInput::new("turn", "seed")).unwrap();
        let cache = DiffusionCache::default();
        activation_scores_cached(&cache, &eng, "r", "turn", "derived", &[(atom, 1.0)], 1).unwrap();
        eng.drop_region("r").unwrap();
        assert!(matches!(
            activation_scores_cached(&cache, &eng, "r", "turn", "derived", &[(atom, 1.0)], 1),
            Err(crate::MemError::RegionNotAttached(_) | crate::MemError::RegionNotFound(_))
        ));
    }

    #[test]
    fn cache_revalidates_key_first_erasure_without_a_row_commit() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let (db, eng) = engine_with_database(dir.path());
        eng.create_encrypted_region("s", Arc::new(crate::MockEmbedder::new(64)))
            .unwrap();
        let atom = eng.remember("s", AtomInput::new("turn", "sealed")).unwrap();
        let cache = DiffusionCache::default();
        let score = || {
            activation_scores_cached(&cache, &eng, "s", "turn", "derived", &[(atom, 1.0)], 1)
                .unwrap()
        };
        assert_eq!(score(), vec![(atom, 1.0)]);
        let revision = db.manager().commit_generation();
        let (slot, owner, generation) = db
            .atom_store_live_bindings()
            .unwrap()
            .into_iter()
            .find(|(_, owner, _)| *owner == atom as u64)
            .unwrap();
        db.atom_store_tombstone(slot, owner, generation).unwrap();
        assert_eq!(db.manager().commit_generation(), revision);
        assert!(score().is_empty());
    }

    #[test]
    fn cache_revalidates_sidecar_presence_without_an_epoch_change() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let (db, eng) = engine_with_database(dir.path());
        eng.create_encrypted_region("s", Arc::new(crate::MockEmbedder::new(64)))
            .unwrap();
        let atom = eng.remember("s", AtomInput::new("turn", "sealed")).unwrap();
        let cache = DiffusionCache::default();
        activation_scores_cached(&cache, &eng, "s", "turn", "derived", &[(atom, 1.0)], 1).unwrap();
        let epoch = db.cache_epoch();
        let revision = db.manager().commit_generation();
        std::fs::remove_file(db.atom_store_path()).unwrap();
        assert_eq!(db.cache_epoch(), epoch);
        assert_eq!(db.manager().commit_generation(), revision);
        let error =
            activation_scores_cached(&cache, &eng, "s", "turn", "derived", &[(atom, 1.0)], 1)
                .unwrap_err();
        assert!(error.to_string().contains("atom key store is missing"));
    }

    #[test]
    fn warm_cache_observes_request_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let eng = engine(dir.path());
        let atom = eng.remember("r", AtomInput::new("turn", "seed")).unwrap();
        let cache = DiffusionCache::default();
        activation_scores_cached(&cache, &eng, "r", "turn", "derived", &[(atom, 1.0)], 1).unwrap();
        let cancel = citadel_core::CancelToken::new();
        cancel.cancel();
        let error = eng
            .with_cancel_token(cancel, |eng| {
                activation_scores_cached(&cache, eng, "r", "turn", "derived", &[(atom, 1.0)], 1)
            })
            .unwrap_err();
        assert!(matches!(
            error,
            crate::MemError::Core(citadel_core::Error::Interrupted)
        ));
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
        pin_into_view(&mut ranked, &[3, 1, 3, 1]);
        assert_eq!(ranked, vec![1, 2, 3]);
    }

    #[test]
    fn pin_duplicate_absent_ids_evict_once() {
        let mut ranked = vec![1, 2, 3, 4];
        pin_into_view(&mut ranked, &[9, 9]);
        assert_eq!(ranked, vec![1, 2, 3, 9]);
    }

    #[test]
    fn pin_mixed_duplicates_preserve_rank_and_request_order() {
        let mut ranked = vec![10, 20, 30, 40, 50];
        pin_into_view(&mut ranked, &[40, 99, 99, 20, 88, 40, 88]);
        assert_eq!(ranked, vec![10, 20, 40, 99, 88]);
    }

    #[test]
    fn pin_duplicate_requests_grow_only_for_distinct_ids() {
        let mut ranked = vec![1, 2];
        pin_into_view(&mut ranked, &[2, 9, 9, 8, 8, 7]);
        assert_eq!(ranked, vec![2, 9, 8, 7]);
    }

    #[test]
    fn pin_empty_inputs() {
        let mut ranked = Vec::new();
        pin_into_view(&mut ranked, &[]);
        assert!(ranked.is_empty());

        pin_into_view(&mut ranked, &[9, 8, 9, 8]);
        assert_eq!(ranked, vec![9, 8]);

        pin_into_view(&mut ranked, &[]);
        assert_eq!(ranked, vec![9, 8]);
    }

    #[test]
    fn pin_preserves_existing_ranked_duplicates() {
        let mut ranked = vec![1, 2, 1, 3];
        pin_into_view(&mut ranked, &[1, 4, 4]);
        assert_eq!(ranked, vec![1, 2, 1, 4]);
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
