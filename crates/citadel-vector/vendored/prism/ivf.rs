//! IVF2 (geometric clusters x tag posting lists) + MQCB batch search.
//!
//! Two-level inverted index: K-means clusters for geometric proximity,
//! per-cluster tag posting lists for attribute filtering. Vectors stored
//! once (no duplication). Intra-cluster tag-affinity sort for sequential
//! memory access on posting list scans.

use super::binary::BinaryStore;
use super::distance;

use rand::prelude::*;
use rayon::prelude::*;
use std::cell::UnsafeCell;
use std::collections::BinaryHeap;

/// CSR sparse matrix (same layout as scipy.sparse.csr_matrix).
pub struct SpMat {
    pub rows: usize,
    pub cols: usize,
    pub indptr: Vec<i64>,
    pub indices: Vec<i32>,
}

/// Type-erased flat vector storage (u8 or f32).
pub enum VecStore {
    U8(Vec<u8>),
    F32(Vec<f32>),
}

/// Borrowed query batch (flat, nq x dim).
pub enum QueryStore<'a> {
    U8(&'a [u8]),
    F32(&'a [f32]),
}

/// Single query vector slice.
enum QueryVec<'a> {
    U8(&'a [u8]),
    F32(&'a [f32]),
}

/// Distance suitable for heap ordering. For u8: raw u32 from l2_sq8.
/// For f32: f32::to_bits() (monotonic for non-negative IEEE 754 floats).
#[inline]
fn compute_dist(store: &VecStore, gid: usize, query: &QueryVec, dim: usize) -> u32 {
    match (store, query) {
        (VecStore::U8(v), QueryVec::U8(q)) => distance::l2_sq8(q, &v[gid * dim..(gid + 1) * dim]),
        (VecStore::F32(v), QueryVec::F32(q)) => {
            distance::l2_squared(q, &v[gid * dim..(gid + 1) * dim]).to_bits()
        }
        _ => unreachable!("mismatched vector/query types"),
    }
}

/// IVF2 index: geometric clusters x per-cluster tag posting lists.
pub struct IvfIndex {
    /// Reordered vectors (contiguous per cluster).
    pub vectors: VecStore,
    /// Mapping: reordered_id -> original_id.
    pub original_ids: Vec<u32>,
    /// Cluster boundaries: cluster c spans [cluster_starts[c]..cluster_starts[c+1]).
    pub cluster_starts: Vec<u32>,
    /// Per-cluster tag index offsets.
    tag_offsets: Vec<u32>,
    /// (tag_id, posting_start, posting_len) triples, sorted by tag_id within each cluster.
    tag_index: Vec<(u32, u32, u32)>,
    /// Flat array of local IDs for all (cluster, tag) posting lists.
    posting_ids: Vec<u32>,
    /// Per-tag list of clusters containing matching vectors.
    pub tag_clusters: Vec<Vec<u16>>,
    /// Vector dimensionality.
    pub dim: usize,
    /// Number of clusters.
    pub n_clusters: usize,
}

impl IvfIndex {
    /// Build the IVF2 index from clustered vectors and metadata.
    ///
    /// Reorders vectors by cluster, sorts within each cluster by most popular
    /// tag (tag-affinity sort), and builds per-cluster tag posting lists.
    pub fn build(
        base: &VecStore,
        base_meta: &SpMat,
        assignments: &[u16],
        n: usize,
        dim: usize,
        n_clusters: usize,
    ) -> Self {
        let mut cluster_sizes = vec![0u32; n_clusters];
        for &a in assignments {
            cluster_sizes[a as usize] += 1;
        }
        let mut cluster_starts = vec![0u32; n_clusters + 1];
        for i in 0..n_clusters {
            cluster_starts[i + 1] = cluster_starts[i] + cluster_sizes[i];
        }

        let mut position = cluster_starts[..n_clusters].to_vec();
        let mut new_order = vec![0u32; n];
        for (i, &ci_raw) in assignments.iter().enumerate().take(n) {
            let ci = ci_raw as usize;
            let new_id = position[ci] as usize;
            new_order[new_id] = i as u32;
            position[ci] += 1;
        }

        macro_rules! reorder_and_sort {
            ($base_data:expr, $zero:expr, $T:ty) => {{
                let mut vecs = vec![$zero; n * dim];
                for (new_id, &old_id) in new_order.iter().enumerate() {
                    let src = &$base_data[old_id as usize * dim..(old_id as usize + 1) * dim];
                    vecs[new_id * dim..(new_id + 1) * dim].copy_from_slice(src);
                }

                // Tag-affinity sort within each cluster
                let mut tag_freq = vec![0u32; base_meta.cols + 1];
                for &tag in &base_meta.indices {
                    tag_freq[tag as usize] += 1;
                }
                for ci in 0..n_clusters {
                    let cs = cluster_starts[ci] as usize;
                    let ce = cluster_starts[ci + 1] as usize;
                    if ce - cs <= 1 {
                        continue;
                    }

                    let mut sort_keys: Vec<(u32, usize)> = (0..ce - cs)
                        .map(|local| {
                            let old_id = new_order[cs + local] as usize;
                            let ms = base_meta.indptr[old_id] as usize;
                            let me = base_meta.indptr[old_id + 1] as usize;
                            let tag = base_meta.indices[ms..me]
                                .iter()
                                .max_by_key(|&&t| tag_freq[t as usize])
                                .map(|&t| t as u32)
                                .unwrap_or(u32::MAX);
                            (tag, local)
                        })
                        .collect();
                    sort_keys.sort_unstable_by_key(|&(tag, _)| tag);

                    let old_vecs: Vec<$T> = vecs[cs * dim..ce * dim].to_vec();
                    let old_ids: Vec<u32> = new_order[cs..ce].to_vec();
                    for (new_local, &(_, old_local)) in sort_keys.iter().enumerate() {
                        vecs[(cs + new_local) * dim..(cs + new_local + 1) * dim]
                            .copy_from_slice(&old_vecs[old_local * dim..(old_local + 1) * dim]);
                        new_order[cs + new_local] = old_ids[old_local];
                    }
                }
                vecs
            }};
        }

        let vectors = match base {
            VecStore::U8(data) => VecStore::U8(reorder_and_sort!(data, 0u8, u8)),
            VecStore::F32(data) => VecStore::F32(reorder_and_sort!(data, 0.0f32, f32)),
        };

        // old_to_new must come AFTER the intra-cluster tag-affinity sort.
        let mut old_to_new = vec![0u32; n];
        for (new_id, &old_id) in new_order.iter().enumerate() {
            old_to_new[old_id as usize] = new_id as u32;
        }

        let mut all_tag_entries: Vec<Vec<(u32, u32, u32)>> = Vec::with_capacity(n_clusters);
        let mut all_posting_ids: Vec<u32> = Vec::new();

        let mut cluster_maps: Vec<std::collections::HashMap<u32, Vec<u32>>> = (0..n_clusters)
            .map(|_| std::collections::HashMap::new())
            .collect();

        for old_id in 0..n {
            let new_id = old_to_new[old_id] as usize;
            let ci = assignments[old_id] as usize;
            let local_id = new_id - cluster_starts[ci] as usize;

            let start = base_meta.indptr[old_id] as usize;
            let end = base_meta.indptr[old_id + 1] as usize;
            for &tag in &base_meta.indices[start..end] {
                cluster_maps[ci]
                    .entry(tag as u32)
                    .or_default()
                    .push(local_id as u32);
            }
        }

        for cluster_map in cluster_maps.iter_mut().take(n_clusters) {
            let mut entries: Vec<(u32, Vec<u32>)> = cluster_map.drain().collect();
            entries.sort_unstable_by_key(|&(tag, _)| tag);

            let mut cluster_entries = Vec::with_capacity(entries.len());
            for (tag, mut ids) in entries {
                ids.sort_unstable();
                let posting_start = all_posting_ids.len() as u32;
                let posting_len = ids.len() as u32;
                all_posting_ids.extend_from_slice(&ids);
                cluster_entries.push((tag, posting_start, posting_len));
            }
            all_tag_entries.push(cluster_entries);
        }

        let mut tag_offsets = Vec::with_capacity(n_clusters + 1);
        let mut tag_index = Vec::new();
        let mut offset = 0u32;
        for entries in &all_tag_entries {
            tag_offsets.push(offset);
            tag_index.extend_from_slice(entries);
            offset += entries.len() as u32;
        }
        tag_offsets.push(offset);

        // Build per-tag cluster lists (for filtered cluster selection)
        let max_tag = tag_index.iter().map(|&(t, _, _)| t).max().unwrap_or(0) as usize;
        let mut tag_clusters: Vec<Vec<u16>> = vec![vec![]; max_tag + 1];
        for ci in 0..n_clusters {
            let start = tag_offsets[ci] as usize;
            let end = tag_offsets[ci + 1] as usize;
            for &(tag, _, _) in &tag_index[start..end] {
                tag_clusters[tag as usize].push(ci as u16);
            }
        }

        Self {
            vectors,
            original_ids: new_order,
            cluster_starts,
            tag_offsets,
            tag_index,
            posting_ids: all_posting_ids,
            tag_clusters,
            dim,
            n_clusters,
        }
    }

    /// Look up local IDs matching a tag within a cluster.
    #[inline]
    fn lookup_tag(&self, cluster: usize, tag: u32) -> &[u32] {
        let start = self.tag_offsets[cluster] as usize;
        let end = self.tag_offsets[cluster + 1] as usize;
        let entries = &self.tag_index[start..end];
        match entries.binary_search_by_key(&tag, |&(t, _, _)| t) {
            Ok(idx) => {
                let (_, ps, pl) = entries[idx];
                &self.posting_ids[ps as usize..(ps + pl) as usize]
            }
            Err(_) => &[],
        }
    }

    /// Scan local ids in a cluster against the query. The Hamming pre-filter
    /// applies when the candidate count exceeds the rerank budget.
    #[allow(clippy::too_many_arguments)]
    fn scan_cluster(
        &self,
        ci: usize,
        lids: impl ExactSizeIterator<Item = u32>,
        query: &QueryVec,
        q_binary: &[u64],
        binary: &BinaryStore,
        ef: usize,
        binary_rerank: usize,
        heap: &mut BinaryHeap<(u32, u32)>,
    ) {
        let dim = self.dim;
        let cluster_base = self.cluster_starts[ci] as usize;
        let rerank_budget = binary_rerank * ef;

        if binary_rerank > 0 && lids.len() > rerank_budget {
            let mut candidates: Vec<(u32, u32)> = lids
                .map(|lid| {
                    let gid = (cluster_base + lid as usize) as u32;
                    (distance::hamming(q_binary, binary.code(gid)), lid)
                })
                .collect();
            let budget = rerank_budget.min(candidates.len());
            candidates.select_nth_unstable_by_key(budget - 1, |&(d, _)| d);
            candidates.truncate(budget);
            for &(_, lid) in &candidates {
                let gid = (cluster_base + lid as usize) as u32;
                let dist = compute_dist(&self.vectors, gid as usize, query, dim);
                let orig_id = self.original_ids[gid as usize];
                heap_insert(heap, dist, orig_id, ef);
            }
        } else {
            for lid in lids {
                let gid = (cluster_base + lid as usize) as u32;
                let dist = compute_dist(&self.vectors, gid as usize, query, dim);
                let orig_id = self.original_ids[gid as usize];
                heap_insert(heap, dist, orig_id, ef);
            }
        }
    }

    /// MQCB: processes queries grouped by cluster for L3 cache reuse.
    #[allow(clippy::too_many_arguments)]
    pub fn batch_search_mqcb(
        &self,
        queries: &QueryStore,
        nq: usize,
        query_tags: &[Vec<usize>],
        query_binary: &[Vec<u64>],
        query_top_clusters: &[Vec<usize>],
        binary: &BinaryStore,
        k: usize,
        ef: usize,
        n_probe: usize,
        binary_rerank: usize,
    ) -> Vec<Vec<u32>> {
        let dim = self.dim;

        // Invert: cluster -> list of query indices.
        let mut cluster_queries: Vec<Vec<usize>> = vec![vec![]; self.n_clusters];
        for (qi, top_clusters) in query_top_clusters.iter().enumerate().take(nq) {
            let np = n_probe.min(top_clusters.len());
            for &ci in &top_clusters[..np] {
                cluster_queries[ci].push(qi);
            }
        }

        // Per-query heaps. Safety: each qi appears at most once per cluster
        // and clusters are processed sequentially, so no races.
        struct HeapArray(Vec<UnsafeCell<BinaryHeap<(u32, u32)>>>);
        unsafe impl Sync for HeapArray {}
        impl HeapArray {
            #[inline]
            #[allow(clippy::mut_from_ref)]
            unsafe fn get(&self, idx: usize) -> &mut BinaryHeap<(u32, u32)> {
                &mut *self.0[idx].get()
            }
        }
        let heaps = HeapArray(
            (0..nq)
                .map(|_| UnsafeCell::new(BinaryHeap::with_capacity(ef + 1)))
                .collect(),
        );

        // Sequential cluster iteration for prefetcher-friendly memory access
        for (ci, qi_list) in cluster_queries.iter().enumerate() {
            if qi_list.is_empty() {
                continue;
            }

            qi_list.par_iter().for_each(|&qi| {
                let query = match queries {
                    QueryStore::U8(data) => QueryVec::U8(&data[qi * dim..(qi + 1) * dim]),
                    QueryStore::F32(data) => QueryVec::F32(&data[qi * dim..(qi + 1) * dim]),
                };
                let tags = &query_tags[qi];
                let heap = unsafe { heaps.get(qi) };

                match tags.len() {
                    // Unfiltered: every point in the cluster is a candidate.
                    0 => {
                        let len = self.cluster_starts[ci + 1] - self.cluster_starts[ci];
                        self.scan_cluster(
                            ci,
                            0..len,
                            &query,
                            &query_binary[qi],
                            binary,
                            ef,
                            binary_rerank,
                            heap,
                        );
                    }
                    1 => {
                        let matching = self.lookup_tag(ci, tags[0] as u32);
                        self.scan_cluster(
                            ci,
                            matching.iter().copied(),
                            &query,
                            &query_binary[qi],
                            binary,
                            ef,
                            binary_rerank,
                            heap,
                        );
                    }
                    // Conjunctive filter: a candidate must match EVERY tag.
                    _ => {
                        let lists: Vec<&[u32]> = tags
                            .iter()
                            .map(|&t| self.lookup_tag(ci, t as u32))
                            .collect();
                        let matching = intersect_postings(lists);
                        self.scan_cluster(
                            ci,
                            matching.iter().copied(),
                            &query,
                            &query_binary[qi],
                            binary,
                            ef,
                            binary_rerank,
                            heap,
                        );
                    }
                }
            });
        }

        heaps
            .0
            .into_par_iter()
            .map(|cell| {
                let heap = cell.into_inner();
                let mut results: Vec<(u32, u32)> = heap.into_vec();
                results.sort_unstable_by_key(|&(d, _)| d);
                results.iter().take(k).map(|&(_, id)| id).collect()
            })
            .collect()
    }
}

/// Bounded max-heap insert via PeekMut (single sift-down).
#[inline]
fn heap_insert(heap: &mut BinaryHeap<(u32, u32)>, dist: u32, id: u32, cap: usize) {
    if heap.len() < cap {
        heap.push((dist, id));
    } else if let Some(mut top) = heap.peek_mut() {
        if dist < top.0 {
            *top = (dist, id);
        }
    }
}

/// Sorted k-way intersection of posting lists, smallest list first so the
/// accumulator only shrinks.
fn intersect_postings(mut lists: Vec<&[u32]>) -> Vec<u32> {
    lists.sort_unstable_by_key(|l| l.len());
    let mut acc: Vec<u32> = lists[0].to_vec();
    for list in &lists[1..] {
        if acc.is_empty() {
            break;
        }
        let mut out = Vec::with_capacity(acc.len().min(list.len()));
        let (mut i, mut j) = (0, 0);
        while i < acc.len() && j < list.len() {
            match acc[i].cmp(&list[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    out.push(acc[i]);
                    i += 1;
                    j += 1;
                }
            }
        }
        acc = out;
    }
    acc
}

/// Sorted intersection of two sorted u16 slices.
pub fn sorted_intersect_u16(a: &[u16], b: &[u16]) -> Vec<u16> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

/// K-means clustering. Returns (assignments, centroids as VecStore matching input type).
pub fn kmeans(
    base: &VecStore,
    n: usize,
    dim: usize,
    c: usize,
    iters: usize,
) -> (Vec<u16>, VecStore) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut centroid_ids: Vec<usize> = (0..n).collect();
    centroid_ids.shuffle(&mut rng);
    centroid_ids.truncate(c);

    let mut centroids_f32 = vec![0.0f32; c * dim];
    match base {
        VecStore::U8(data) => {
            for (ci, &vid) in centroid_ids.iter().enumerate() {
                for d in 0..dim {
                    centroids_f32[ci * dim + d] = data[vid * dim + d] as f32;
                }
            }
        }
        VecStore::F32(data) => {
            for (ci, &vid) in centroid_ids.iter().enumerate() {
                centroids_f32[ci * dim..(ci + 1) * dim]
                    .copy_from_slice(&data[vid * dim..(vid + 1) * dim]);
            }
        }
    }

    let mut assignments = vec![0u16; n];

    for _ in 0..iters {
        let new_assignments: Vec<u16> = match base {
            VecStore::U8(data) => {
                let centroids_u8: Vec<u8> = centroids_f32
                    .iter()
                    .map(|&x| x.round().clamp(0.0, 255.0) as u8)
                    .collect();
                (0..n)
                    .into_par_iter()
                    .map(|i| {
                        let v = &data[i * dim..(i + 1) * dim];
                        let mut best_c = 0u16;
                        let mut best_d = u32::MAX;
                        for ci in 0..c {
                            let cent = &centroids_u8[ci * dim..(ci + 1) * dim];
                            let d = distance::l2_sq8(v, cent);
                            if d < best_d {
                                best_d = d;
                                best_c = ci as u16;
                            }
                        }
                        best_c
                    })
                    .collect()
            }
            VecStore::F32(data) => (0..n)
                .into_par_iter()
                .map(|i| {
                    let v = &data[i * dim..(i + 1) * dim];
                    let mut best_c = 0u16;
                    let mut best_d = f32::INFINITY;
                    for ci in 0..c {
                        let cent = &centroids_f32[ci * dim..(ci + 1) * dim];
                        let d = distance::l2_squared(v, cent);
                        if d < best_d {
                            best_d = d;
                            best_c = ci as u16;
                        }
                    }
                    best_c
                })
                .collect(),
        };
        assignments = new_assignments;

        // Centroid update accumulates in f64 to avoid f32 cancellation.
        let mut sums = vec![0.0f64; c * dim];
        let mut counts = vec![0u32; c];
        match base {
            VecStore::U8(data) => {
                for i in 0..n {
                    let ci = assignments[i] as usize;
                    counts[ci] += 1;
                    for d in 0..dim {
                        sums[ci * dim + d] += data[i * dim + d] as f64;
                    }
                }
            }
            VecStore::F32(data) => {
                for i in 0..n {
                    let ci = assignments[i] as usize;
                    counts[ci] += 1;
                    for d in 0..dim {
                        sums[ci * dim + d] += data[i * dim + d] as f64;
                    }
                }
            }
        }
        for ci in 0..c {
            if counts[ci] > 0 {
                let inv = 1.0 / counts[ci] as f64;
                for d in 0..dim {
                    centroids_f32[ci * dim + d] = (sums[ci * dim + d] * inv) as f32;
                }
            }
        }

        // FAISS-style repair: reseed each empty cluster from a random point of
        // the most populated cluster (a frozen empty centroid rarely wins an
        // assignment again, so the effective cluster count would only shrink).
        for ci in 0..c {
            if counts[ci] > 0 {
                continue;
            }
            let donor = (0..c).max_by_key(|&d| counts[d]).unwrap();
            if counts[donor] <= 1 {
                break;
            }
            let members: Vec<usize> = (0..n)
                .filter(|&i| assignments[i] as usize == donor)
                .collect();
            let p = members[rng.gen_range(0..members.len())];
            match base {
                VecStore::U8(data) => {
                    for d in 0..dim {
                        centroids_f32[ci * dim + d] = data[p * dim + d] as f32;
                    }
                }
                VecStore::F32(data) => {
                    centroids_f32[ci * dim..(ci + 1) * dim]
                        .copy_from_slice(&data[p * dim..(p + 1) * dim]);
                }
            }
            assignments[p] = ci as u16;
            counts[donor] -= 1;
            counts[ci] = 1;
        }
    }

    let centroids = match base {
        VecStore::U8(_) => VecStore::U8(
            centroids_f32
                .iter()
                .map(|&x| x.round().clamp(0.0, 255.0) as u8)
                .collect(),
        ),
        VecStore::F32(_) => VecStore::F32(centroids_f32),
    };

    (assignments, centroids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prism::point::PointStore;

    /// 6 points, 4 tags, 2 hand-assigned clusters. Tag sets per point:
    /// 0:{0,1,2} 1:{0,1} 2:{0,2} 3:{1,2} 4:{0,1,2} 5:{3}.
    fn fixture() -> (IvfIndex, BinaryStore) {
        let points: Vec<Vec<f32>> = vec![
            vec![0.0, 0.0],
            vec![0.1, 0.0],
            vec![0.2, 0.0],
            vec![0.3, 0.0],
            vec![5.0, 5.0],
            vec![5.1, 5.0],
        ];
        let tag_sets: Vec<Vec<i32>> = vec![
            vec![0, 1, 2],
            vec![0, 1],
            vec![0, 2],
            vec![1, 2],
            vec![0, 1, 2],
            vec![3],
        ];
        let flat: Vec<f32> = points.iter().flatten().copied().collect();
        let mut indptr = vec![0i64];
        let mut indices = Vec::new();
        for tags in &tag_sets {
            indices.extend_from_slice(tags);
            indptr.push(indices.len() as i64);
        }
        let meta = SpMat {
            rows: points.len(),
            cols: 4,
            indptr,
            indices,
        };
        let assignments: Vec<u16> = vec![0, 0, 0, 0, 1, 1];
        let base = VecStore::F32(flat.clone());
        let index = IvfIndex::build(&base, &meta, &assignments, points.len(), 2, 2);
        let store = PointStore::from_parts(flat, 2, vec![vec![0; points.len()]]);
        let binary = BinaryStore::build(&store);
        (index, binary)
    }

    fn run_query(
        index: &IvfIndex,
        binary: &BinaryStore,
        query: &[f32],
        tags: Vec<usize>,
        k: usize,
    ) -> Vec<u32> {
        let qb = binary.encode_query(query);
        let mut results = index.batch_search_mqcb(
            &QueryStore::F32(query),
            1,
            &[tags],
            &[qb],
            &[vec![0, 1]],
            binary,
            k,
            10,
            2,
            0,
        );
        results.pop().unwrap()
    }

    #[test]
    fn batch_zero_tags_scans_whole_clusters() {
        let (index, binary) = fixture();
        let mut ids = run_query(&index, &binary, &[5.05, 5.0], Vec::new(), 2);
        ids.sort_unstable();
        assert_eq!(ids, vec![4, 5]);
    }

    #[test]
    fn batch_three_tags_enforces_full_conjunction() {
        let (index, binary) = fixture();
        let ids = run_query(&index, &binary, &[0.05, 0.0], vec![0, 1, 2], 4);
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        // Only points 0 and 4 carry all three tags; point 1 matches just {0,1}
        // and must not leak through a first-two-tags-only intersection.
        assert_eq!(sorted, vec![0, 4]);
    }

    #[test]
    fn kmeans_reseeds_empty_clusters() {
        // 60 identical points + 4 outliers, 8 clusters: without repair most
        // centroids never win an assignment and stay empty forever.
        let n = 64;
        let mut flat = vec![0.0f32; n * 2];
        for (i, off) in [(60, 50.0f32), (61, -50.0), (62, 100.0), (63, -100.0)] {
            flat[i * 2] = off;
            flat[i * 2 + 1] = off;
        }
        let (assignments, _) = kmeans(&VecStore::F32(flat), n, 2, 8, 3);
        let mut seen = [false; 8];
        for &a in &assignments {
            seen[a as usize] = true;
        }
        assert!(
            seen.iter().all(|&s| s),
            "every cluster must keep at least one member, got {assignments:?}"
        );
    }
}
