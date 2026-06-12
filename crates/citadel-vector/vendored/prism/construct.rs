use super::binary::BinaryStore;
use super::distance::{self, Metric};
use super::graph::{AdjBuilder, Graph};
use super::partition::PartitionTree;
use super::point::PointStore;
use super::quantize::SQ8Store;

use rand::prelude::*;
use rayon::prelude::*;
use std::collections::HashSet;

/// Configuration for PRISM index construction.
#[derive(Clone, Debug)]
pub struct PrismConfig {
    /// Local degree (edges within each leaf cell).
    pub m_local: usize,
    /// Greedy cross-partition degree.
    pub m_greedy: usize,
    /// Random cross-partition degree (must be even).
    pub m_random: usize,
    /// Covering strength for attribute-diverse selection.
    pub t: usize,
    /// Proximity-diversity tradeoff for cross-neighbor selection (0 = pure diversity).
    pub alpha: f32,
    /// Vamana pruning parameter (standard DiskANN: 1.2).
    pub vamana_alpha: f32,
    /// Beam width for candidate search during construction (paper: 10 * M_g).
    pub beam_width: usize,
    /// Distance metric.
    pub metric: Metric,
    /// Selectivity threshold for HIGH regime.
    pub sigma_high: f32,
    /// Selectivity threshold for LOW regime.
    pub sigma_low: f32,
    /// Bridge budget multiplier for MID regime.
    pub beta: f32,
    /// Search pruning tolerance for filtered queries.
    pub epsilon: f32,
    /// Binary pre-filter rerank factor. Top `binary_rerank * ef` Hamming candidates
    /// are reranked with SQ8. 0 disables binary pre-filter.
    pub binary_rerank: usize,
}

impl Default for PrismConfig {
    fn default() -> Self {
        Self {
            m_local: 16,
            m_greedy: 12,
            m_random: 4,
            t: 2,
            alpha: 1.0,
            vamana_alpha: 1.0,
            beam_width: 120,
            metric: Metric::L2,
            sigma_high: 0.10,
            sigma_low: 0.001,
            beta: 3.0,
            epsilon: 0.2,
            binary_rerank: 4,
        }
    }
}

/// The complete PRISM index.
pub struct PrismIndex {
    pub store: PointStore,
    pub tree: PartitionTree,
    pub graph: Graph,
    /// Local-only graph (intra-cell edges) for per-cell graph search.
    pub local_graph: Graph,
    pub medoids: Vec<u32>,
    pub global_medoid: u32,
    /// Reverse mapping: point_id -> cell index.
    pub point_cell: Vec<u32>,
    /// Maps internal ID -> original ID.
    pub original_ids: Vec<u32>,
    /// Scalar-quantized vectors for distance computation.
    pub sq8: SQ8Store,
    /// Binary codes for Hamming pre-filter.
    pub binary: BinaryStore,
    pub config: PrismConfig,
}

impl PrismIndex {
    /// Build a PRISM index from a PointStore (Algorithm 2).
    pub fn build(mut store: PointStore, config: PrismConfig) -> Self {
        let n = store.len;
        assert!(n > 0, "cannot build index from empty point store");
        assert!(
            config.m_random >= 4 && config.m_random % 2 == 0,
            "m_random must be >= 4 and even (Friedman model requires d >= 4)"
        );

        // Cosine: normalize once at build so SQ8-L2 code distances are
        // rank-equivalent to cosine (L2^2 = 2 - 2cos on unit vectors). The
        // exact rerank is scale-invariant, so reported distances and segment
        // rehydration from raw table rows are unaffected.
        if config.metric == Metric::Cosine {
            let dim = store.dim;
            distance::normalize_rows(&mut store.vectors, dim);
        }

        let tree = PartitionTree::build(&store);
        let (store, tree, original_ids) = reorder_by_cell(store, tree);
        let sq8 = SQ8Store::build(&store);
        let binary = if config.binary_rerank > 0 {
            BinaryStore::build(&store)
        } else {
            BinaryStore::empty(store.dim)
        };

        let mut point_cell = vec![0u32; n];
        for (ci, cell) in tree.cells.iter().enumerate() {
            for &pid in &cell.point_ids {
                point_cell[pid as usize] = ci as u32;
            }
        }

        // Local Vamana graphs within each cell
        let mut adj = AdjBuilder::new(n);
        build_local_edges(&store, &tree, &sq8, &config, &mut adj);

        let medoids = compute_medoids(&store, &tree, config.metric);

        let local_graph = adj.snapshot();

        // The global graph (cross edges + random overlay) is traversed only by
        // REGIME_MID; when sigma_high <= sigma_low that regime is unreachable
        // and the two most expensive construction phases would build dead edges.
        if config.sigma_high > config.sigma_low {
            // Greedy cross-partition edges (attribute-diverse selection)
            build_greedy_cross_edges(
                &store,
                &tree,
                &medoids,
                &local_graph,
                &sq8,
                &point_cell,
                &config,
                &mut adj,
            );

            // Random regular overlay (Friedman permutation model)
            build_random_overlay(n, config.m_random, &mut adj);
        }

        let graph = adj.build();

        let global_medoid = compute_global_medoid(&store, config.metric);

        Self {
            store,
            tree,
            graph,
            local_graph,
            medoids,
            global_medoid,
            point_cell,
            original_ids,
            sq8,
            binary,
            config,
        }
    }
}

/// Reorder so points in the same cell are contiguous. Returns (store, tree, original_ids).
fn reorder_by_cell(
    store: PointStore,
    mut tree: PartitionTree,
) -> (PointStore, PartitionTree, Vec<u32>) {
    let n = store.len;
    let dim = store.dim;
    let k = store.k();

    let mut new_order: Vec<u32> = Vec::with_capacity(n);
    for cell in &tree.cells {
        new_order.extend_from_slice(&cell.point_ids);
    }

    let mut old_to_new = vec![0u32; n];
    for (new_id, &old_id) in new_order.iter().enumerate() {
        old_to_new[old_id as usize] = new_id as u32;
    }

    let mut new_vectors = vec![0.0f32; n * dim];
    for (new_id, &old_id) in new_order.iter().enumerate() {
        let src = &store.vectors[old_id as usize * dim..(old_id as usize + 1) * dim];
        new_vectors[new_id * dim..(new_id + 1) * dim].copy_from_slice(src);
    }

    let mut new_attrs = Vec::with_capacity(k);
    for j in 0..k {
        let mut attr_col = vec![0u32; n];
        for (new_id, &old_id) in new_order.iter().enumerate() {
            attr_col[new_id] = store.attrs[j][old_id as usize];
        }
        new_attrs.push(attr_col);
    }

    for cell in &mut tree.cells {
        for pid in &mut cell.point_ids {
            *pid = old_to_new[*pid as usize];
        }
    }

    let new_store = PointStore::from_parts(new_vectors, dim, new_attrs);
    (new_store, tree, new_order)
}

/// Build local Vamana graphs within each cell. Small cells get complete graphs,
/// larger cells use greedy Vamana construction with robust pruning.
fn build_local_edges(
    store: &PointStore,
    tree: &PartitionTree,
    sq8: &SQ8Store,
    config: &PrismConfig,
    adj: &mut AdjBuilder,
) {
    let cell_edges: Vec<Vec<(u32, u32)>> = tree
        .cells
        .par_iter()
        .map(|cell| {
            let pts = &cell.point_ids;
            let mut edges = Vec::new();
            if pts.len() <= 1 {
                return edges;
            }

            if pts.len() <= config.m_local + 1 {
                for i in 0..pts.len() {
                    for j in (i + 1)..pts.len() {
                        edges.push((pts[i], pts[j]));
                        edges.push((pts[j], pts[i]));
                    }
                }
            } else {
                let mut rng = rand::thread_rng();
                build_vamana_cell(store, sq8, pts, config, &mut edges, &mut rng);
            }
            edges
        })
        .collect();

    for edges in cell_edges {
        for (src, dst) in edges {
            adj.add_edge(src, dst);
        }
    }
}

/// Vamana construction within a single cell: code-space beam search + f32 pruning, two passes.
fn build_vamana_cell(
    store: &PointStore,
    sq8: &SQ8Store,
    pts: &[u32],
    config: &PrismConfig,
    edges: &mut Vec<(u32, u32)>,
    rng: &mut impl Rng,
) {
    let n = pts.len();
    let r = config.m_local;
    let beam = n.min(config.beam_width);
    let alpha = config.vamana_alpha;

    let actual_r = r.min(n - 1);
    let mut graph: Vec<Vec<usize>> = (0..n)
        .map(|i| {
            let mut neighbors = Vec::with_capacity(actual_r);
            while neighbors.len() < actual_r {
                let j = rng.gen_range(0..n);
                if j != i && !neighbors.contains(&j) {
                    neighbors.push(j);
                }
            }
            neighbors
        })
        .collect();

    let dim = store.dim;
    let mut centroid = vec![0.0f32; dim];
    for &p in pts {
        let v = store.vector(p);
        for (c, &x) in centroid.iter_mut().zip(v.iter()) {
            *c += x;
        }
    }
    let inv_n = 1.0 / n as f32;
    for c in &mut centroid {
        *c *= inv_n;
    }
    let entry = (0..n)
        .min_by(|&a, &b| {
            let da = distance::distance(&centroid, store.vector(pts[a]), config.metric);
            let db = distance::distance(&centroid, store.vector(pts[b]), config.metric);
            da.partial_cmp(&db).unwrap()
        })
        .unwrap();

    for _pass in 0..2 {
        let mut order: Vec<usize> = (0..n).collect();
        order.shuffle(rng);

        for &i in &order {
            let search_results =
                vamana_search_code(store, sq8, config.metric, pts, &graph, entry, pts[i], beam);

            let mut candidates = search_results;
            for &nb in &graph[i] {
                if !candidates.contains(&nb) {
                    candidates.push(nb);
                }
            }

            graph[i] = robust_prune(store, pts, i, &candidates, alpha, r, config.metric);

            let new_neighbors: Vec<usize> = graph[i].clone();
            for &j in &new_neighbors {
                if !graph[j].contains(&i) {
                    graph[j].push(i);
                    if graph[j].len() > r {
                        let cands: Vec<usize> = graph[j].clone();
                        graph[j] = robust_prune(store, pts, j, &cands, alpha, r, config.metric);
                    }
                }
            }
        }
    }

    for (i, neighbors) in graph.iter().enumerate() {
        for &j in neighbors {
            edges.push((pts[i], pts[j]));
        }
    }
}

/// Heap-ordered candidate distance between two stored points. L2 and
/// (build-normalized) Cosine rank by SQ8 codes; InnerProduct cannot be ranked
/// in code-space L2, so it uses the exact f32 metric via a total-order key.
#[inline]
fn build_cand_dist(store: &PointStore, sq8: &SQ8Store, metric: Metric, a: u32, b: u32) -> u32 {
    match metric {
        Metric::L2 | Metric::Cosine => distance::l2_sq8(sq8.code(a), sq8.code(b)),
        Metric::InnerProduct => distance::ord_key(distance::distance(
            store.vector(a),
            store.vector(b),
            Metric::InnerProduct,
        )),
    }
}

/// Code-space beam search within a cell's local graph. Returns visited local indices.
#[allow(clippy::too_many_arguments)]
fn vamana_search_code(
    store: &PointStore,
    sq8: &SQ8Store,
    metric: Metric,
    pts: &[u32],
    graph: &[Vec<usize>],
    entry: usize,
    query_id: u32,
    beam: usize,
) -> Vec<usize> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut visited = vec![false; pts.len()];
    let mut candidates: BinaryHeap<Reverse<(u32, usize)>> = BinaryHeap::new();
    let mut results: BinaryHeap<(u32, usize)> = BinaryHeap::new();

    let d = build_cand_dist(store, sq8, metric, query_id, pts[entry]);
    visited[entry] = true;
    candidates.push(Reverse((d, entry)));
    results.push((d, entry));

    while let Some(Reverse((d, c))) = candidates.pop() {
        if results.len() >= beam {
            if let Some(&(worst, _)) = results.peek() {
                if d > worst {
                    break;
                }
            }
        }

        for &w in &graph[c] {
            if visited[w] {
                continue;
            }
            visited[w] = true;
            let wd = build_cand_dist(store, sq8, metric, query_id, pts[w]);
            candidates.push(Reverse((wd, w)));
            results.push((wd, w));
            if results.len() > beam {
                results.pop();
            }
        }
    }

    results.into_iter().map(|(_, idx)| idx).collect()
}

/// Robust prune: rejects c if alpha * dist(c, selected) <= dist(p, c).
fn robust_prune(
    store: &PointStore,
    pts: &[u32],
    p: usize,
    candidates: &[usize],
    alpha: f32,
    r: usize,
    metric: Metric,
) -> Vec<usize> {
    let p_vec = store.vector(pts[p]);
    let mut sorted: Vec<(usize, f32)> = candidates
        .iter()
        .filter(|&&c| c != p)
        .map(|&c| (c, distance::distance(p_vec, store.vector(pts[c]), metric)))
        .collect();
    sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    sorted.dedup_by_key(|x| x.0);

    let mut selected: Vec<usize> = Vec::with_capacity(r);
    for &(c, d_pc) in &sorted {
        if selected.len() >= r {
            break;
        }
        let dominated = selected.iter().any(|&s| {
            let d_cs = distance::distance(store.vector(pts[c]), store.vector(pts[s]), metric);
            alpha * d_cs <= d_pc
        });
        if !dominated {
            selected.push(c);
        }
    }
    selected
}

/// Greedy attribute-diverse cross-partition edges. SQ8 beam search for
/// candidate discovery, f32 rerank, parallelized across points.
#[allow(clippy::too_many_arguments)]
fn build_greedy_cross_edges(
    store: &PointStore,
    tree: &PartitionTree,
    medoids: &[u32],
    local_graph: &Graph,
    sq8: &SQ8Store,
    point_cell: &[u32],
    config: &PrismConfig,
    adj: &mut AdjBuilder,
) {
    let n = store.len;
    let k = store.k();
    let t = config.t.min(k);
    let beam = config.beam_width;
    let subsets = t_subsets(k, t);
    // SQ8-L2 candidate ranking is rank-faithful for L2 and for Cosine (vectors
    // are build-normalized); InnerProduct falls back to exact full-cell scans.
    let use_sq8 = config.metric != Metric::InnerProduct;

    let point_edges: Vec<Vec<u32>> = (0..n as u32)
        .into_par_iter()
        .map(|p_id| {
            let p_cell_idx = point_cell[p_id as usize];
            let p_vec = store.vector(p_id);

            let p_code = sq8.code(p_id);
            let mut cell_dists: Vec<(usize, u32)> = tree
                .cells
                .iter()
                .enumerate()
                .filter(|&(ci, _)| ci as u32 != p_cell_idx)
                .map(|(ci, _)| {
                    let d = distance::l2_sq8(p_code, sq8.code(medoids[ci]));
                    (ci, d)
                })
                .collect();
            cell_dists.sort_unstable_by_key(|&(_, d)| d);

            let mut all_cand_ids: Vec<u32> = Vec::with_capacity(beam);
            for &(ci, _) in &cell_dists {
                let cell_size = tree.cells[ci].point_ids.len();

                if use_sq8 && cell_size > beam * 2 {
                    let found = beam_search_sq8(sq8, local_graph, p_code, medoids[ci], beam);
                    for (id, _) in found {
                        all_cand_ids.push(id);
                    }
                } else if use_sq8 {
                    let mut scored: Vec<(u32, u32)> = tree.cells[ci]
                        .point_ids
                        .iter()
                        .map(|&q| (q, distance::l2_sq8(p_code, sq8.code(q))))
                        .collect();
                    scored.sort_unstable_by_key(|&(_, d)| d);
                    for &(id, _) in scored.iter().take(beam) {
                        all_cand_ids.push(id);
                    }
                } else {
                    for &q_id in &tree.cells[ci].point_ids {
                        all_cand_ids.push(q_id);
                    }
                }

                if all_cand_ids.len() >= beam {
                    break;
                }
            }

            let mut candidates: Vec<(u32, f32)> = all_cand_ids
                .iter()
                .map(|&id| {
                    (
                        id,
                        distance::distance(p_vec, store.vector(id), config.metric),
                    )
                })
                .collect();
            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            candidates.truncate(beam);

            select_cross_neighbors(store, &candidates, config, &subsets)
        })
        .collect();

    for (p_id, neighbors) in point_edges.into_iter().enumerate() {
        for q_id in neighbors {
            adj.add_edge(p_id as u32, q_id);
        }
    }
}

/// SQ8 beam search through a cell's local graph. Returns (point_id, sq8_distance).
fn beam_search_sq8(
    sq8: &SQ8Store,
    graph: &Graph,
    query_code: &[u8],
    entry: u32,
    beam: usize,
) -> Vec<(u32, u32)> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut visited = HashSet::new();
    let mut candidates: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
    let mut results: BinaryHeap<(u32, u32)> = BinaryHeap::new();

    let d = distance::l2_sq8(query_code, sq8.code(entry));
    visited.insert(entry);
    candidates.push(Reverse((d, entry)));
    results.push((d, entry));

    while let Some(Reverse((d, c))) = candidates.pop() {
        if results.len() >= beam {
            if let Some(&(worst, _)) = results.peek() {
                if d > worst {
                    break;
                }
            }
        }

        for &w in graph.neighbors(c) {
            if !visited.insert(w) {
                continue;
            }
            let wd = distance::l2_sq8(query_code, sq8.code(w));
            candidates.push(Reverse((wd, w)));
            results.push((wd, w));
            if results.len() > beam {
                results.pop();
            }
        }
    }

    results.into_iter().map(|(d, id)| (id, d)).collect()
}

/// Attribute-diverse neighbor selection. Candidates sorted by distance.
pub(crate) fn select_cross_neighbors(
    store: &PointStore,
    candidates: &[(u32, f32)],
    config: &PrismConfig,
    subsets: &[Vec<usize>],
) -> Vec<u32> {
    let m_g = config.m_greedy;
    let alpha = config.alpha;

    if candidates.is_empty() || m_g == 0 {
        return Vec::new();
    }

    let mut covered: HashSet<u64> = HashSet::new();
    let mut selected = Vec::with_capacity(m_g);
    let mut available: Vec<bool> = vec![true; candidates.len()];

    for _ in 0..m_g {
        let mut best_idx = None;
        let mut best_score = f32::NEG_INFINITY;

        for (idx, &(q_id, dist)) in candidates.iter().enumerate() {
            if !available[idx] {
                continue;
            }

            let new_tuples = count_new_tuples(store, q_id, &covered, subsets);

            // score = gain / cost; fall back to proximity when fully covered
            let score = if alpha == 0.0 || dist == 0.0 {
                new_tuples as f32
            } else {
                (new_tuples as f32 + 0.001) / dist.powf(alpha)
            };

            if score > best_score {
                best_score = score;
                best_idx = Some(idx);
            }
        }

        let Some(idx) = best_idx else { break };
        selected.push(candidates[idx].0);
        available[idx] = false;

        add_tuples(store, candidates[idx].0, &mut covered, subsets);
    }

    selected
}

/// Encode (combo, values) as u64 key. Supports up to 8 dims, values < 256.
#[inline]
fn tuple_key(combo: &[usize], store: &PointStore, q: u32) -> u64 {
    let mut key: u64 = 0;
    for (i, &j) in combo.iter().enumerate() {
        let val = store.attr(q, j) as u64;
        key |= ((j as u64) << 8 | val) << (i * 16);
    }
    key
}

/// Count how many new t-tuples a candidate would contribute.
fn count_new_tuples(
    store: &PointStore,
    q: u32,
    covered: &HashSet<u64>,
    subsets: &[Vec<usize>],
) -> usize {
    let mut count = 0;
    for combo in subsets {
        let key = tuple_key(combo, store, q);
        if !covered.contains(&key) {
            count += 1;
        }
    }
    count
}

/// Add all t-tuples of a point to the covered set.
pub(crate) fn add_tuples(
    store: &PointStore,
    q: u32,
    covered: &mut HashSet<u64>,
    subsets: &[Vec<usize>],
) {
    for combo in subsets {
        let key = tuple_key(combo, store, q);
        covered.insert(key);
    }
}

/// Generate all t-element subsets of [0..k].
pub(crate) fn t_subsets(k: usize, t: usize) -> Vec<Vec<usize>> {
    let mut result = Vec::new();
    let mut combo = Vec::with_capacity(t);
    generate_subsets(k, t, 0, &mut combo, &mut result);
    result
}

fn generate_subsets(
    k: usize,
    t: usize,
    start: usize,
    combo: &mut Vec<usize>,
    result: &mut Vec<Vec<usize>>,
) {
    if combo.len() == t {
        result.push(combo.clone());
        return;
    }
    for i in start..k {
        combo.push(i);
        generate_subsets(k, t, i + 1, combo, result);
        combo.pop();
    }
}

/// Random cross-partition edges via Friedman permutation model.
pub(crate) fn build_random_overlay(n: usize, m_random: usize, adj: &mut AdjBuilder) {
    if m_random == 0 || n <= 1 {
        return;
    }
    let mut rng = rand::thread_rng();
    let half = m_random / 2;

    for _ in 0..half {
        let mut perm: Vec<u32> = (0..n as u32).collect();
        perm.shuffle(&mut rng);
        for (i, &j) in perm.iter().enumerate() {
            if i as u32 != j {
                adj.add_undirected(i as u32, j);
            }
        }
    }
}

/// Per-cell medoid via centroid-nearest approximation.
fn compute_medoids(store: &PointStore, tree: &PartitionTree, metric: Metric) -> Vec<u32> {
    let dim = store.dim;
    tree.cells
        .iter()
        .map(|cell| {
            let pts = &cell.point_ids;
            if pts.len() == 1 {
                return pts[0];
            }
            let mut centroid = vec![0.0f32; dim];
            for &p in pts {
                let v = store.vector(p);
                for (c, &x) in centroid.iter_mut().zip(v.iter()) {
                    *c += x;
                }
            }
            let inv_n = 1.0 / pts.len() as f32;
            for c in &mut centroid {
                *c *= inv_n;
            }
            *pts.iter()
                .min_by(|&&a, &&b| {
                    let da = distance::distance(&centroid, store.vector(a), metric);
                    let db = distance::distance(&centroid, store.vector(b), metric);
                    da.partial_cmp(&db).unwrap()
                })
                .unwrap()
        })
        .collect()
}

/// Compute global medoid: the point closest to the centroid of the entire dataset.
fn compute_global_medoid(store: &PointStore, metric: Metric) -> u32 {
    let n = store.len;
    let dim = store.dim;
    let mut centroid = vec![0.0f32; dim];
    for i in 0..n as u32 {
        let v = store.vector(i);
        for (c, &x) in centroid.iter_mut().zip(v.iter()) {
            *c += x;
        }
    }
    let inv_n = 1.0 / n as f32;
    for c in &mut centroid {
        *c *= inv_n;
    }
    (0..n as u32)
        .min_by(|&a, &b| {
            let da = distance::distance(&centroid, store.vector(a), metric);
            let db = distance::distance(&centroid, store.vector(b), metric);
            da.partial_cmp(&db).unwrap()
        })
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::super::point::PointStore;
    use super::*;

    #[test]
    fn test_build_small() {
        let mut store = PointStore::new(2, 2);
        // 4 points, 2 attributes with 2 values each = 4 cells.
        store.push(&[0.0, 0.0], &[0, 0]);
        store.push(&[1.0, 0.0], &[0, 1]);
        store.push(&[0.0, 1.0], &[1, 0]);
        store.push(&[1.0, 1.0], &[1, 1]);

        let config = PrismConfig {
            m_local: 2,
            m_greedy: 2,
            m_random: 4,
            t: 1,
            alpha: 0.0,
            beam_width: 10,
            ..Default::default()
        };

        let index = PrismIndex::build(store, config);
        assert_eq!(index.tree.cells.len(), 4);
        assert_eq!(index.medoids.len(), 4);
        // Each point should have some neighbors
        for i in 0..4u32 {
            assert!(index.graph.degree(i) > 0);
        }
    }

    #[test]
    fn test_t_subsets() {
        let subs = t_subsets(4, 2);
        assert_eq!(subs.len(), 6); // C(4,2) = 6
        let subs = t_subsets(3, 1);
        assert_eq!(subs.len(), 3);
    }
}
