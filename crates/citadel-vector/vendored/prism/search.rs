use super::construct::PrismIndex;
use super::distance;
use super::filter::Filter;

use rayon::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// A search result: (point_id, distance).
#[derive(Clone, Debug)]
pub struct SearchResult {
    pub id: u32,
    pub dist: f32,
}

/// Bitset for O(1) visited tracking, sized to the cell for L1 cache locality.
struct Bitset {
    bits: Vec<u64>,
}

impl Bitset {
    fn new(n: usize) -> Self {
        Self {
            bits: vec![0u64; n.div_ceil(64)],
        }
    }

    /// Returns true if the bit was newly set (not previously visited).
    #[inline]
    fn insert(&mut self, i: u32) -> bool {
        let word = i as usize >> 6;
        let bit = 1u64 << (i & 63);
        if self.bits[word] & bit != 0 {
            false
        } else {
            self.bits[word] |= bit;
            true
        }
    }

    /// Check if a bit is set without modifying the bitset.
    #[inline]
    fn contains(&self, i: u32) -> bool {
        let word = i as usize >> 6;
        let bit = 1u64 << (i & 63);
        self.bits[word] & bit != 0
    }
}

/// Prefetch into L1.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
#[inline]
unsafe fn prefetch_t0(ptr: *const u8) {
    std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
}

/// Software prefetch hint.
#[inline(always)]
fn prefetch_read(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        prefetch_t0(ptr);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

/// Prefetch `len` bytes starting at `ptr`.
#[inline(always)]
fn prefetch_range(ptr: *const u8, len: usize) {
    let mut offset = 0;
    while offset < len {
        prefetch_read(unsafe { ptr.add(offset) });
        offset += 64;
    }
}

/// Ordered f32 wrapper for use in BinaryHeap.
#[derive(Clone, Copy, PartialEq)]
struct OrdF32(f32);

impl Eq for OrdF32 {}
impl PartialOrd for OrdF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Insert into a bounded max-heap of (u32_dist, point_id), keeping only the `cap` smallest.
#[inline]
fn heap_insert_sq8(heap: &mut BinaryHeap<(u32, u32)>, dist: u32, id: u32, cap: usize) {
    if heap.len() < cap {
        heap.push((dist, id));
    } else if let Some(&(worst, _)) = heap.peek() {
        if dist < worst {
            heap.pop();
            heap.push((dist, id));
        }
    }
}

impl PrismIndex {
    /// Filtered k-NN search with automatic regime selection.
    pub fn search(&self, query: &[f32], filter: &Filter, k: usize, ef: usize) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.store.dim);

        // Match the build-time Cosine normalization so code-space distances
        // stay rank-faithful. Reported distances are unchanged (cosine is
        // scale-invariant in both arguments).
        let normalized;
        let query = if self.config.metric == distance::Metric::Cosine {
            normalized = distance::normalized(query);
            normalized.as_slice()
        } else {
            query
        };

        let cell_indices = self.tree.filter_cells(filter.constraints());
        let n_f = self.tree.count_points(&cell_indices);
        let sigma = n_f as f32 / self.store.len as f32;
        if sigma >= self.config.sigma_high {
            self.regime_high_filtered(query, &cell_indices, k, ef)
        } else if sigma > self.config.sigma_low {
            self.regime_mid(query, &cell_indices, k, ef)
        } else {
            self.regime_low(query, filter, &cell_indices, k)
        }
    }

    /// Heap-ordered candidate distance from the query to point `p`. L2 and
    /// (build-normalized) Cosine rank by SQ8 codes; InnerProduct cannot be
    /// ranked in code-space L2, so it ranks by the exact f32 metric mapped to
    /// a total-order key (mirrors the construct-side `use_sq8` gate).
    #[inline]
    fn cand_dist(&self, query: &[f32], q_code: &[u8], p: u32) -> u32 {
        match self.config.metric {
            distance::Metric::L2 | distance::Metric::Cosine => {
                distance::l2_sq8(q_code, self.sq8.code(p))
            }
            distance::Metric::InnerProduct => distance::ord_key(distance::distance(
                query,
                self.store.vector(p),
                distance::Metric::InnerProduct,
            )),
        }
    }

    /// Per-cell SQ8 search: brute-force scan for small cells,
    /// Vamana graph beam search for large cells.
    fn regime_high_filtered(
        &self,
        query: &[f32],
        cell_indices: &[usize],
        k: usize,
        ef: usize,
    ) -> Vec<SearchResult> {
        if cell_indices.is_empty() {
            return Vec::new();
        }

        let q_code = self.sq8.quantize_query(query);
        let q_binary = if self.config.binary_rerank > 0 {
            self.binary.encode_query(query)
        } else {
            Vec::new()
        };
        let mut merged: BinaryHeap<(u32, u32)> = BinaryHeap::new();

        if cell_indices.len() == self.tree.cells.len() {
            // All cells match: binary pre-filter -> code-space rerank over entire index.
            let n = self.store.len as u32;
            let rerank_budget = self.config.binary_rerank * ef;
            if self.config.binary_rerank > 0 && (n as usize) > rerank_budget {
                let mut binary_heap: BinaryHeap<(u32, u32)> = BinaryHeap::new();
                for p in 0..n {
                    let hd = distance::hamming(&q_binary, self.binary.code(p));
                    heap_insert_sq8(&mut binary_heap, hd, p, rerank_budget);
                }
                for (_, p) in binary_heap {
                    let dist = self.cand_dist(query, &q_code, p);
                    heap_insert_sq8(&mut merged, dist, p, ef);
                }
            } else {
                for p in 0..n {
                    let dist = self.cand_dist(query, &q_code, p);
                    heap_insert_sq8(&mut merged, dist, p, ef);
                }
            }
        } else {
            // Visit cells nearest-medoid-first so the ef heap tightens early.
            let mut ranked: Vec<(usize, u32)> = cell_indices
                .iter()
                .map(|&ci| {
                    let d = self.cand_dist(query, &q_code, self.medoids[ci]);
                    (ci, d)
                })
                .collect();
            ranked.sort_unstable_by_key(|&(_, d)| d);

            let scan_threshold = (ef * self.config.m_local).max(2000);

            for &(ci, _) in &ranked {
                let cands = self.search_cell(query, &q_code, &q_binary, ci, ef, scan_threshold);
                for (cand_dist, id) in cands {
                    heap_insert_sq8(&mut merged, cand_dist, id, ef);
                }
            }
        }

        let mut results: Vec<SearchResult> = merged
            .into_iter()
            .map(|(_, id)| SearchResult {
                id,
                dist: distance::distance(query, self.store.vector(id), self.config.metric),
            })
            .collect();
        results.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap());
        results.truncate(k);
        results
    }

    /// Bridge routing for medium selectivity. Traverses full graph, using
    /// non-matching nodes as bridges when bridge score > tau. SQ8 traversal, f32 rerank.
    fn regime_mid(
        &self,
        query: &[f32],
        compatible_cells: &[usize],
        k: usize,
        ef: usize,
    ) -> Vec<SearchResult> {
        if compatible_cells.is_empty() {
            return Vec::new();
        }

        let q_code = self.sq8.quantize_query(query);

        let n_cells = self.tree.cells.len();
        let mut cell_match = vec![false; n_cells];
        for &ci in compatible_cells {
            cell_match[ci] = true;
        }

        let (_, entry) = compatible_cells
            .iter()
            .map(|&ci| {
                let d = self.cand_dist(query, &q_code, self.medoids[ci]);
                (d, self.medoids[ci])
            })
            .min_by_key(|&(d, _)| d)
            .unwrap();

        let entry_dist = self.cand_dist(query, &q_code, entry);

        let mut visited = Bitset::new(self.store.len);
        visited.insert(entry);

        let mut candidates: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
        let mut results: BinaryHeap<(u32, u32)> = BinaryHeap::new();

        candidates.push(Reverse((entry_dist, entry)));
        results.push((entry_dist, entry));

        let bridge_budget = (self.config.beta * ef as f32) as usize;
        let mut bridges_used = 0usize;
        let epsilon_factor = ((1.0 + self.config.epsilon) * (1.0 + self.config.epsilon)) as f64;

        // Bridge threshold tau = sigma / (1 + sigma), sigma = selectivity.
        let n_f: usize = compatible_cells
            .iter()
            .map(|&ci| self.tree.cells[ci].point_ids.len())
            .sum();
        let sigma = n_f as f32 / self.store.len as f32;
        let tau = sigma / (1.0 + sigma);

        while let Some(Reverse((d, c))) = candidates.pop() {
            if results.len() >= ef {
                if let Some(&(worst, _)) = results.peek() {
                    if (d as f64) > (worst as f64) * epsilon_factor {
                        break;
                    }
                }
            }

            if bridges_used >= bridge_budget {
                break;
            }

            let neighbors = self.graph.neighbors(c);
            let sq8_dim = self.store.dim;

            let mut unvisited_buf: Vec<u32> = Vec::with_capacity(neighbors.len());
            for &w in neighbors {
                if visited.insert(w) {
                    unvisited_buf.push(w);
                    prefetch_range(self.sq8.code(w).as_ptr(), sq8_dim);
                }
            }

            for &w in &unvisited_buf {
                let wd = self.cand_dist(query, &q_code, w);
                let w_cell = self.point_cell[w as usize];

                if cell_match[w_cell as usize] {
                    heap_insert_sq8(&mut results, wd, w, ef);
                    candidates.push(Reverse((wd, w)));
                } else {
                    let w_neighbors = self.graph.neighbors(w);
                    if !w_neighbors.is_empty() {
                        let matching_unvisited = w_neighbors
                            .iter()
                            .filter(|&&u| {
                                cell_match[self.point_cell[u as usize] as usize]
                                    && !visited.contains(u)
                            })
                            .count();
                        let fraction = matching_unvisited as f32 / w_neighbors.len() as f32;

                        // Bridge score: matching fraction x proximity.
                        let r = results.peek().map_or(1.0f32, |&(worst, _)| worst as f32);
                        let bridge_score = fraction / (1.0 + wd as f32 / r.max(1.0));

                        if bridge_score > tau {
                            candidates.push(Reverse((wd, w)));
                            bridges_used += 1;
                        }
                    }
                }
            }
        }

        let mut final_results: Vec<SearchResult> = results
            .into_iter()
            .map(|(_, id)| SearchResult {
                id,
                dist: distance::distance(query, self.store.vector(id), self.config.metric),
            })
            .collect();
        final_results.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap());
        final_results.truncate(k);
        final_results
    }

    /// Code-space beam search within a cell's local graph. Returns (id, cand_dist) pairs.
    fn greedy_search_cell_sq8(
        &self,
        query: &[f32],
        q_code: &[u8],
        cell_idx: usize,
        ef: usize,
    ) -> Vec<(u32, u32)> {
        let pts = &self.tree.cells[cell_idx].point_ids;
        let base = pts[0];
        let sq8_dim = self.store.dim;

        let entry = self.medoids[cell_idx];
        let entry_dist = self.cand_dist(query, q_code, entry);

        let mut visited = Bitset::new(pts.len());
        visited.insert(entry - base);

        let mut candidates: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
        let mut results: BinaryHeap<(u32, u32)> = BinaryHeap::new();
        let mut unvisited: Vec<u32> = Vec::with_capacity(32);

        candidates.push(Reverse((entry_dist, entry)));
        results.push((entry_dist, entry));

        while let Some(Reverse((d, c))) = candidates.pop() {
            if results.len() >= ef {
                if let Some(&(worst, _)) = results.peek() {
                    if d > worst {
                        break;
                    }
                }
            }

            unvisited.clear();
            for &w in self.local_graph.neighbors(c) {
                if visited.insert(w - base) {
                    unvisited.push(w);
                    prefetch_range(self.sq8.code(w).as_ptr(), sq8_dim);
                }
            }

            for &w in &unvisited {
                let wd = self.cand_dist(query, q_code, w);
                if results.len() < ef {
                    candidates.push(Reverse((wd, w)));
                    results.push((wd, w));
                } else if let Some(&(worst, _)) = results.peek() {
                    if wd < worst {
                        results.pop();
                        results.push((wd, w));
                        candidates.push(Reverse((wd, w)));
                    }
                }
            }
        }

        results
            .into_vec()
            .into_iter()
            .map(|(d, id)| (id, d))
            .collect()
    }

    /// REGIME_LOW: brute-force within compatible cells for very selective filters.
    fn regime_low(
        &self,
        query: &[f32],
        filter: &Filter,
        cell_indices: &[usize],
        k: usize,
    ) -> Vec<SearchResult> {
        let mut heap: BinaryHeap<(OrdF32, u32)> = BinaryHeap::new();
        for &ci in cell_indices {
            for &p in &self.tree.cells[ci].point_ids {
                if filter.matches(&self.store, p) {
                    let dist = distance::distance(query, self.store.vector(p), self.config.metric);
                    if heap.len() < k {
                        heap.push((OrdF32(dist), p));
                    } else if let Some(&(OrdF32(worst), _)) = heap.peek() {
                        if dist < worst {
                            heap.pop();
                            heap.push((OrdF32(dist), p));
                        }
                    }
                }
            }
        }
        let mut results: Vec<SearchResult> = heap
            .into_iter()
            .map(|(OrdF32(d), id)| SearchResult { id, dist: d })
            .collect();
        results.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap());
        results
    }

    /// MQCB (Multi-Query Cell Batching): groups queries by target cell so
    /// cell data stays warm in L3 across queries. Cells processed in parallel,
    /// queries within each cell sequentially.
    pub fn batch_search(
        &self,
        queries: &[f32],
        filters: &[Filter],
        nq: usize,
        k: usize,
        ef: usize,
    ) -> Vec<Vec<SearchResult>> {
        let dim = self.store.dim;
        let n_cells = self.tree.cells.len();
        let scan_threshold = (ef * self.config.m_local).max(2000);

        // Match the build-time Cosine normalization (see `search`).
        let normalized;
        let queries = if self.config.metric == distance::Metric::Cosine {
            let mut buf = queries.to_vec();
            distance::normalize_rows(&mut buf, dim);
            normalized = buf;
            normalized.as_slice()
        } else {
            queries
        };

        let query_info: Vec<(Vec<u8>, Vec<u64>, Vec<usize>)> = (0..nq)
            .into_par_iter()
            .map(|qi| {
                let q = &queries[qi * dim..(qi + 1) * dim];
                let q_code = self.sq8.quantize_query(q);
                let q_binary = if self.config.binary_rerank > 0 {
                    self.binary.encode_query(q)
                } else {
                    Vec::new()
                };
                let cells = self.tree.filter_cells(filters[qi].constraints());
                (q_code, q_binary, cells)
            })
            .collect();

        let mut high_regime: Vec<usize> = Vec::with_capacity(nq);
        let mut mid_regime: Vec<usize> = Vec::new();
        let mut low_regime: Vec<usize> = Vec::new();
        let mut unfiltered: Vec<usize> = Vec::new();
        for (qi, info) in query_info.iter().enumerate() {
            let cells = &info.2;
            if cells.len() >= n_cells {
                unfiltered.push(qi);
            } else {
                let n_f: usize = cells
                    .iter()
                    .map(|&ci| self.tree.cells[ci].point_ids.len())
                    .sum();
                let sigma = n_f as f32 / self.store.len as f32;
                if sigma >= self.config.sigma_high {
                    high_regime.push(qi);
                } else if sigma > self.config.sigma_low {
                    mid_regime.push(qi);
                } else {
                    low_regime.push(qi);
                }
            }
        }

        let mut cell_queries: Vec<Vec<usize>> = vec![Vec::new(); n_cells];
        for &qi in &high_regime {
            for &ci in &query_info[qi].2 {
                cell_queries[ci].push(qi);
            }
        }

        // Cells in parallel, queries within each cell sequentially: cell data
        // stays warm in cache across queries.
        #[allow(clippy::type_complexity)]
        let cell_results: Vec<Vec<(usize, Vec<(u32, u32)>)>> = cell_queries
            .into_par_iter()
            .enumerate()
            .filter(|(_, qs)| !qs.is_empty())
            .map(|(ci, qs)| {
                qs.iter()
                    .map(|&qi| {
                        let q = &queries[qi * dim..(qi + 1) * dim];
                        let q_code = &query_info[qi].0;
                        let q_binary = &query_info[qi].1;
                        let cands = self.search_cell(q, q_code, q_binary, ci, ef, scan_threshold);
                        (qi, cands)
                    })
                    .collect()
            })
            .collect();

        let mut query_heaps: Vec<BinaryHeap<(u32, u32)>> =
            (0..nq).map(|_| BinaryHeap::new()).collect();
        for cell_batch in cell_results {
            for (qi, cands) in cell_batch {
                for (sq8_dist, id) in cands {
                    heap_insert_sq8(&mut query_heaps[qi], sq8_dist, id, ef);
                }
            }
        }

        let unfilt_heaps: Vec<(usize, BinaryHeap<(u32, u32)>)> = unfiltered
            .par_iter()
            .map(|&qi| {
                let q = &queries[qi * dim..(qi + 1) * dim];
                let q_code = &query_info[qi].0;
                let q_binary = &query_info[qi].1;
                let n = self.store.len as u32;
                let rerank_budget = self.config.binary_rerank * ef;
                let mut heap: BinaryHeap<(u32, u32)> = BinaryHeap::new();
                if self.config.binary_rerank > 0 && (n as usize) > rerank_budget {
                    let mut binary_heap: BinaryHeap<(u32, u32)> = BinaryHeap::new();
                    for p in 0..n {
                        let hd = distance::hamming(q_binary, self.binary.code(p));
                        heap_insert_sq8(&mut binary_heap, hd, p, rerank_budget);
                    }
                    for (_, p) in binary_heap {
                        let dist = self.cand_dist(q, q_code, p);
                        heap_insert_sq8(&mut heap, dist, p, ef);
                    }
                } else {
                    for p in 0..n {
                        let dist = self.cand_dist(q, q_code, p);
                        heap_insert_sq8(&mut heap, dist, p, ef);
                    }
                }
                (qi, heap)
            })
            .collect();
        for (qi, heap) in unfilt_heaps {
            query_heaps[qi] = heap;
        }

        let mut all_results: Vec<Vec<SearchResult>> = query_heaps
            .into_par_iter()
            .enumerate()
            .map(|(qi, heap)| {
                if heap.is_empty() {
                    return Vec::new();
                }
                let q = &queries[qi * dim..(qi + 1) * dim];
                let mut results: Vec<SearchResult> = heap
                    .into_iter()
                    .map(|(_, id)| SearchResult {
                        id,
                        dist: distance::distance(q, self.store.vector(id), self.config.metric),
                    })
                    .collect();
                results.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap());
                results.truncate(k);
                results
            })
            .collect();

        if !mid_regime.is_empty() {
            let mid_results: Vec<(usize, Vec<SearchResult>)> = mid_regime
                .par_iter()
                .map(|&qi| {
                    let q = &queries[qi * dim..(qi + 1) * dim];
                    let cells = &query_info[qi].2;
                    let results = self.regime_mid(q, cells, k, ef);
                    (qi, results)
                })
                .collect();
            for (qi, results) in mid_results {
                all_results[qi] = results;
            }
        }

        if !low_regime.is_empty() {
            let low_results: Vec<(usize, Vec<SearchResult>)> = low_regime
                .par_iter()
                .map(|&qi| {
                    let q = &queries[qi * dim..(qi + 1) * dim];
                    let results = self.search(q, &filters[qi], k, ef);
                    (qi, results)
                })
                .collect();
            for (qi, results) in low_results {
                all_results[qi] = results;
            }
        }

        all_results
    }

    /// Search a single cell. Small cells: code-space scan (with optional binary
    /// pre-filter). Large cells: graph search with adaptive ef. Returns
    /// (cand_dist, point_id) pairs.
    fn search_cell(
        &self,
        query: &[f32],
        q_code: &[u8],
        q_binary: &[u64],
        cell_idx: usize,
        ef: usize,
        scan_threshold: usize,
    ) -> Vec<(u32, u32)> {
        let pts = &self.tree.cells[cell_idx].point_ids;
        let mut heap: BinaryHeap<(u32, u32)> = BinaryHeap::new();

        if pts.len() <= scan_threshold {
            let base = pts[0];
            let rerank_budget = self.config.binary_rerank * ef;

            if self.config.binary_rerank > 0 && pts.len() > rerank_budget {
                let mut binary_heap: BinaryHeap<(u32, u32)> = BinaryHeap::new();
                for i in 0..pts.len() {
                    let p = base + i as u32;
                    let hd = distance::hamming(q_binary, self.binary.code(p));
                    heap_insert_sq8(&mut binary_heap, hd, p, rerank_budget);
                }
                for (_, p) in binary_heap {
                    let dist = self.cand_dist(query, q_code, p);
                    heap_insert_sq8(&mut heap, dist, p, ef);
                }
            } else {
                for i in 0..pts.len() {
                    let p = base + i as u32;
                    let dist = self.cand_dist(query, q_code, p);
                    heap_insert_sq8(&mut heap, dist, p, ef);
                }
            }
        } else {
            // Scale the graph-search budget with cell size, capped at 5x ef.
            let ef_cell = ef.max((pts.len() / 200).min(ef * 5));
            let local = self.greedy_search_cell_sq8(query, q_code, cell_idx, ef_cell);
            for (id, dist) in local {
                heap_insert_sq8(&mut heap, dist, id, ef);
            }
        }

        heap.into_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::super::construct::{PrismConfig, PrismIndex};
    use super::super::distance;
    use super::super::filter::Filter;
    use super::super::point::PointStore;

    fn build_test_index() -> PrismIndex {
        let mut store = PointStore::new(2, 1);
        for i in 0..10 {
            let x = (i as f32) * 0.1;
            let attr = if i < 5 { 0 } else { 1 };
            store.push(&[x, x], &[attr]);
        }
        let config = PrismConfig {
            m_local: 4,
            m_greedy: 2,
            m_random: 4,
            t: 1,
            alpha: 0.0,
            beam_width: 10,
            ..Default::default()
        };
        PrismIndex::build(store, config)
    }

    #[test]
    fn test_search_no_filter() {
        let index = build_test_index();
        let results = index.search(&[0.25, 0.25], &Filter::none(), 3, 10);
        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.dist >= 0.0);
        }
    }

    #[test]
    fn test_search_with_filter() {
        let index = build_test_index();
        let filter = Filter::eq(0, 1);
        let results = index.search(&[0.5, 0.5], &filter, 3, 10);
        assert!(!results.is_empty());
        for r in &results {
            assert!(filter.matches(&index.store, r.id));
        }
    }

    #[test]
    fn test_graph_search_mid_selectivity() {
        let dim = 16;
        let n = 2000;
        let n_vals = 20;
        let mut store = PointStore::new(dim, 1);
        for i in 0..n {
            let vec: Vec<f32> = (0..dim).map(|d| ((i * dim + d) as f32).sin()).collect();
            store.push(&vec, &[(i % n_vals) as u32]);
        }
        let config = PrismConfig {
            m_local: 4,
            m_greedy: 2,
            m_random: 4,
            t: 1,
            beam_width: 10,
            ..Default::default()
        };
        let index = PrismIndex::build(store, config);

        let query: Vec<f32> = (0..dim).map(|d| (d as f32 * 0.3).sin()).collect();
        let filter = Filter::eq(0, 0);
        let k = 5;
        let ef = 10;

        let results = index.search(&query, &filter, k, ef);
        assert!(!results.is_empty());
        assert!(results.len() <= k);
        for r in &results {
            assert!(filter.matches(&index.store, r.id));
        }
        for w in results.windows(2) {
            assert!(w[0].dist <= w[1].dist);
        }
    }

    #[test]
    fn test_search_empty_filter() {
        let index = build_test_index();
        let filter = Filter::eq(0, 99);
        let results = index.search(&[0.0, 0.0], &filter, 3, 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_regime_mid_bridge_routing() {
        // 20 attribute values x 100 points/value = 2000 points; sigma_high=0.10,
        // each value is 5% selectivity, so single-value filters route to MID.
        let dim = 16;
        let n = 2000;
        let n_vals = 20;
        let mut store = PointStore::new(dim, 1);
        for i in 0..n {
            let vec: Vec<f32> = (0..dim).map(|d| ((i * dim + d) as f32).sin()).collect();
            store.push(&vec, &[(i % n_vals) as u32]);
        }
        let config = PrismConfig {
            m_local: 4,
            m_greedy: 4,
            m_random: 4,
            t: 1,
            beam_width: 20,
            sigma_high: 0.10,
            sigma_low: 0.001,
            beta: 3.0,
            epsilon: 0.2,
            ..Default::default()
        };
        let index = PrismIndex::build(store, config);

        // Value 0 matches 100 of 2000 points = 5% selectivity = MID regime.
        let query: Vec<f32> = (0..dim).map(|d| (d as f32 * 0.3).sin()).collect();
        let filter = Filter::eq(0, 0);
        let k = 5;
        let ef = 50;

        let results = index.search(&query, &filter, k, ef);
        assert!(!results.is_empty());
        assert!(results.len() <= k);
        for r in &results {
            assert!(filter.matches(&index.store, r.id));
        }
        for w in results.windows(2) {
            assert!(w[0].dist <= w[1].dist);
        }
    }

    #[test]
    fn test_batch_search_mixed_regimes() {
        let dim = 8;
        let n = 1000;
        let n_vals = 10;
        let mut store = PointStore::new(dim, 1);
        for i in 0..n {
            let vec: Vec<f32> = (0..dim).map(|d| ((i * dim + d) as f32).sin()).collect();
            store.push(&vec, &[(i % n_vals) as u32]);
        }
        let config = PrismConfig {
            m_local: 4,
            m_greedy: 4,
            m_random: 4,
            t: 1,
            beam_width: 20,
            sigma_high: 0.10,
            sigma_low: 0.001,
            ..Default::default()
        };
        let index = PrismIndex::build(store, config);

        let k = 3;
        let ef = 20;
        let nq = 3;

        // Query 0: unfiltered (sigma=1.0, HIGH); queries 1 and 2: single-value
        // filters (10% selectivity, MID).
        let queries: Vec<f32> = (0..nq)
            .flat_map(|qi| (0..dim).map(move |d| ((qi * dim + d) as f32 * 0.5).sin()))
            .collect();
        let filters = vec![Filter::none(), Filter::eq(0, 0), Filter::eq(0, 5)];

        let results = index.batch_search(&queries, &filters, nq, k, ef);
        assert_eq!(results.len(), nq);
        for (qi, res) in results.iter().enumerate() {
            assert!(!res.is_empty(), "query {} returned no results", qi);
            assert!(res.len() <= k);
            for r in res {
                assert!(filters[qi].matches(&index.store, r.id));
            }
        }
    }

    #[test]
    fn inner_product_candidates_survive_l2_blind_spot() {
        // 59 decoys hug the query in L2 with tiny dot products; one high-norm
        // point is the true IP winner but the L2-farthest point in the set. An
        // SQ8-L2 candidate heap (ef < n) would evict it before the rerank.
        let mut store = PointStore::new(2, 1);
        for i in 0..59 {
            let j = (i as f32) * 0.001;
            store.push(&[0.5 + j, j], &[0]);
        }
        store.push(&[20.0, 0.0], &[0]);
        let config = PrismConfig {
            m_local: 4,
            m_greedy: 2,
            m_random: 4,
            t: 1,
            beam_width: 10,
            metric: distance::Metric::InnerProduct,
            binary_rerank: 0,
            ..Default::default()
        };
        let index = PrismIndex::build(store, config);

        let results = index.search(&[1.0, 0.0], &Filter::none(), 1, 8);
        assert_eq!(results[0].id, 59, "true IP winner must reach the rerank");
        assert!((results[0].dist - (-20.0)).abs() < 1e-3);
    }

    #[test]
    fn cosine_candidates_survive_unnormalized_inputs() {
        // Unnormalized data: the best-angle point has a huge norm (L2-farthest
        // from the raw query) and would be evicted from a raw SQ8-L2 candidate
        // heap; build-time normalization keeps code distances angle-faithful.
        let mut store = PointStore::new(2, 1);
        for i in 0..59 {
            let j = (i as f32) * 0.001;
            store.push(&[j, 1.0 + j], &[0]);
        }
        store.push(&[50.0, 1.0], &[0]);
        let config = PrismConfig {
            m_local: 4,
            m_greedy: 2,
            m_random: 4,
            t: 1,
            beam_width: 10,
            metric: distance::Metric::Cosine,
            binary_rerank: 0,
            ..Default::default()
        };
        let index = PrismIndex::build(store, config);

        let results = index.search(&[3.0, 0.0], &Filter::none(), 1, 8);
        assert_eq!(results[0].id, 59, "best-angle point must reach the rerank");
        assert!(
            results[0].dist < 0.01,
            "dist {} is not ~1-cos",
            results[0].dist
        );
    }

    #[test]
    fn test_binary_prefilter_recall() {
        // The binary pre-filter is an approximation; results stay valid and
        // ordered, just not necessarily identical to the pure SQ8 path.
        let dim = 64;
        let n = 2000;
        let n_vals = 10;
        let mut store = PointStore::new(dim, 1);
        for i in 0..n {
            let vec: Vec<f32> = (0..dim)
                .map(|d| ((i * dim + d) as f32 * 0.01).sin())
                .collect();
            store.push(&vec, &[(i % n_vals) as u32]);
        }

        let config_binary = PrismConfig {
            m_local: 4,
            m_greedy: 2,
            m_random: 4,
            t: 1,
            beam_width: 10,
            binary_rerank: 4,
            ..Default::default()
        };
        let index_binary = PrismIndex::build(store, config_binary);

        let query: Vec<f32> = (0..dim).map(|d| (d as f32 * 0.3).sin()).collect();
        let filter = Filter::eq(0, 0);
        let k = 10;
        let ef = 50;

        let results_binary = index_binary.search(&query, &filter, k, ef);
        assert!(!results_binary.is_empty());
        assert!(results_binary.len() <= k);
        for r in &results_binary {
            assert!(filter.matches(&index_binary.store, r.id));
        }
        for w in results_binary.windows(2) {
            assert!(w[0].dist <= w[1].dist);
        }
    }

    #[test]
    fn test_binary_prefilter_batch() {
        let dim = 32;
        let n = 500;
        let n_vals = 5;
        let mut store = PointStore::new(dim, 1);
        for i in 0..n {
            let vec: Vec<f32> = (0..dim)
                .map(|d| ((i * dim + d) as f32 * 0.02).sin())
                .collect();
            store.push(&vec, &[(i % n_vals) as u32]);
        }

        let config = PrismConfig {
            m_local: 4,
            m_greedy: 2,
            m_random: 4,
            t: 1,
            beam_width: 10,
            binary_rerank: 4,
            ..Default::default()
        };
        let index = PrismIndex::build(store, config);

        let nq = 5;
        let k = 5;
        let ef = 20;
        let queries: Vec<f32> = (0..nq)
            .flat_map(|qi| (0..dim).map(move |d| ((qi * dim + d) as f32 * 0.1).sin()))
            .collect();
        let filters: Vec<Filter> = (0..nq)
            .map(|qi| Filter::eq(0, (qi % n_vals) as u32))
            .collect();

        let results = index.batch_search(&queries, &filters, nq, k, ef);
        assert_eq!(results.len(), nq);
        for (qi, res) in results.iter().enumerate() {
            assert!(!res.is_empty(), "query {} returned no results", qi);
            assert!(res.len() <= k);
            for r in res {
                assert!(filters[qi].matches(&index.store, r.id));
            }
        }
    }
}
