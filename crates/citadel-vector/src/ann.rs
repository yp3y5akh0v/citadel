//! In-memory ANN index wrapping the vendored PRISM engine.

use crate::prism::{Filter, Metric, PointStore, PrismConfig, PrismIndex};

/// Request `k * OVER_FETCH` candidates to offset PRISM recall below 1.0.
pub const OVER_FETCH: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum AnnError {
    #[error("ANN build requires at least one row")]
    EmptyInput,
    #[error("ANN build vector dim mismatch: expected {expected}, got {got} for row_id {row_id}")]
    DimMismatch {
        expected: u16,
        got: usize,
        row_id: u64,
    },
    #[error(
        "ANN build attribute arity mismatch: expected {expected}, got {got} for row_id {row_id}"
    )]
    AttrArityMismatch {
        expected: usize,
        got: usize,
        row_id: u64,
    },
}

// binary_rerank=0: the Hamming pre-filter kills recall on continuous vectors.
// sigma_high low: stay in the fast HIGH search regime.
fn prism_config(metric: Metric) -> PrismConfig {
    PrismConfig {
        metric,
        binary_rerank: 0,
        sigma_high: 0.001,
        ..PrismConfig::default()
    }
}

/// In-memory ANN index over a `(row_id, vector)` snapshot.
pub struct AnnIndex {
    prism: PrismIndex,
    /// PRISM internal id -> external row_id.
    id_map: Vec<u64>,
    /// Highest row_id in the snapshot.
    pub snapshot_max: u64,
    pub metric: Metric,
    pub dim: u16,
}

impl std::fmt::Debug for AnnIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnnIndex")
            .field("snapshot_max", &self.snapshot_max)
            .field("metric", &self.metric)
            .field("dim", &self.dim)
            .field("indexed_len", &self.id_map.len())
            .finish()
    }
}

impl AnnIndex {
    /// Build an unfiltered index from `(row_id, vector)` pairs.
    pub fn build(rows: Vec<(u64, Vec<f32>)>, metric: Metric, dim: u16) -> Result<Self, AnnError> {
        let with_attrs = rows
            .into_iter()
            .map(|(id, v)| (id, v, Vec::new()))
            .collect();
        Self::build_with_attrs(with_attrs, 0, metric, dim)
    }

    /// Build a filtered index from `(row_id, vector, attr_codes)` triples. Each
    /// attribute is a PRISM dimension; distinct tuples form the searchable cells.
    pub fn build_with_attrs(
        mut rows: Vec<(u64, Vec<f32>, Vec<u32>)>,
        num_attrs: usize,
        metric: Metric,
        dim: u16,
    ) -> Result<Self, AnnError> {
        if rows.is_empty() {
            return Err(AnnError::EmptyInput);
        }
        for (rid, v, a) in &rows {
            if v.len() != dim as usize {
                return Err(AnnError::DimMismatch {
                    expected: dim,
                    got: v.len(),
                    row_id: *rid,
                });
            }
            if a.len() != num_attrs {
                return Err(AnnError::AttrArityMismatch {
                    expected: num_attrs,
                    got: a.len(),
                    row_id: *rid,
                });
            }
        }

        rows.sort_unstable_by_key(|(id, _, _)| *id);
        let snapshot_max = rows.last().map(|(id, _, _)| *id).unwrap_or(0);

        let n = rows.len();
        let mut flat: Vec<f32> = Vec::with_capacity(n * dim as usize);
        let mut row_ids: Vec<u64> = Vec::with_capacity(n);
        // PRISM needs >=1 attribute dim; an all-zero column = one cell.
        let attr_dims = num_attrs.max(1);
        let mut attr_cols: Vec<Vec<u32>> = vec![Vec::with_capacity(n); attr_dims];
        for (rid, v, a) in &rows {
            flat.extend_from_slice(v);
            row_ids.push(*rid);
            if num_attrs == 0 {
                attr_cols[0].push(0);
            } else {
                for (j, &code) in a.iter().enumerate() {
                    attr_cols[j].push(code);
                }
            }
        }

        let store = PointStore::from_parts(flat, dim as usize, attr_cols);
        let prism = PrismIndex::build(store, prism_config(metric));

        // PRISM reorders points by cell; remap to external row_ids.
        let id_map: Vec<u64> = prism
            .original_ids
            .iter()
            .map(|&old| row_ids[old as usize])
            .collect();

        Ok(Self {
            prism,
            id_map,
            snapshot_max,
            metric,
            dim,
        })
    }

    /// Reassemble from persisted parts (the ANN segment decode path). The
    /// caller is responsible for `prism.store.vectors` being in PRISM-internal
    /// (cell-reordered) order - see `segment::SegmentParts::into_index`.
    pub fn from_parts(
        prism: PrismIndex,
        id_map: Vec<u64>,
        snapshot_max: u64,
        metric: Metric,
        dim: u16,
    ) -> Self {
        Self {
            prism,
            id_map,
            snapshot_max,
            metric,
            dim,
        }
    }

    pub fn prism(&self) -> &PrismIndex {
        &self.prism
    }

    /// PRISM internal id -> external row_id.
    pub fn id_map(&self) -> &[u64] {
        &self.id_map
    }

    /// The PRISM configuration this index family builds with - part of the
    /// persisted segment's binding (a config change invalidates segments).
    pub fn active_config(metric: Metric) -> PrismConfig {
        prism_config(metric)
    }

    /// Top-k search returning `(row_id, distance)` ascending, at the default ef.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        let ef = (k * OVER_FETCH).max(self.prism.config.beam_width);
        self.search_with_ef(query, k, ef)
    }

    /// Unfiltered search with an explicit beam width `ef`.
    pub fn search_with_ef(&self, query: &[f32], k: usize, ef: usize) -> Vec<(u64, f32)> {
        self.search_filtered(query, k, ef, &Filter::none())
    }

    /// Filtered search at the default ef.
    pub fn search_filtered_default_ef(
        &self,
        query: &[f32],
        k: usize,
        filter: &Filter,
    ) -> Vec<(u64, f32)> {
        let ef = (k * OVER_FETCH).max(self.prism.config.beam_width);
        self.search_filtered(query, k, ef, filter)
    }

    /// Filtered search; `filter` dims index the `build_with_attrs` attributes
    /// (`Filter::none()` matches all).
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: &Filter,
    ) -> Vec<(u64, f32)> {
        debug_assert_eq!(query.len(), self.dim as usize);
        self.prism
            .search(query, filter, k, ef)
            .into_iter()
            .map(|r| (self.id_map[r.id as usize], r.dist))
            .collect()
    }

    /// Number of indexed rows.
    pub fn indexed_len(&self) -> usize {
        self.id_map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synth_rows(n: usize, dim: u16) -> Vec<(u64, Vec<f32>)> {
        (0..n)
            .map(|i| {
                let row_id = (i as u64) + 1;
                let v: Vec<f32> = (0..dim).map(|d| (i as f32 + d as f32) * 0.01).collect();
                (row_id, v)
            })
            .collect()
    }

    #[test]
    fn build_empty_input_errors() {
        let err = AnnIndex::build(Vec::new(), Metric::L2, 4).unwrap_err();
        assert!(matches!(err, AnnError::EmptyInput));
    }

    #[test]
    fn build_dim_mismatch_errors() {
        let rows = vec![(1u64, vec![1.0, 2.0])];
        let err = AnnIndex::build(rows, Metric::L2, 4).unwrap_err();
        assert!(matches!(
            err,
            AnnError::DimMismatch {
                expected: 4,
                got: 2,
                row_id: 1
            }
        ));
    }

    #[test]
    fn build_single_row_succeeds() {
        let rows = vec![(7u64, vec![0.1, 0.2, 0.3, 0.4])];
        let idx = AnnIndex::build(rows, Metric::L2, 4).unwrap();
        assert_eq!(idx.indexed_len(), 1);
        assert_eq!(idx.snapshot_max, 7);
    }

    #[test]
    fn build_small_n_succeeds() {
        let rows = synth_rows(5, 8);
        let idx = AnnIndex::build(rows, Metric::L2, 8).unwrap();
        assert_eq!(idx.indexed_len(), 5);
    }

    #[test]
    fn build_large_n_succeeds() {
        let rows = synth_rows(500, 16);
        let idx = AnnIndex::build(rows, Metric::L2, 16).unwrap();
        assert_eq!(idx.indexed_len(), 500);
    }

    #[test]
    fn search_returns_row_ids_not_internal_ids() {
        let n = 200;
        let rows = synth_rows(n, 8);
        let idx = AnnIndex::build(rows, Metric::L2, 8).unwrap();
        let hits = idx.search(&[0.5; 8], 5);
        assert!(!hits.is_empty());
        for (rid, _d) in &hits {
            assert!(*rid >= 1 && *rid <= n as u64);
        }
    }

    #[test]
    fn snapshot_max_tracks_highest_row_id() {
        let rows = vec![
            (5u64, vec![1.0, 0.0]),
            (10u64, vec![0.0, 1.0]),
            (3u64, vec![1.0, 1.0]),
        ];
        let idx = AnnIndex::build(rows, Metric::L2, 2).unwrap();
        assert_eq!(idx.snapshot_max, 10);
    }

    #[test]
    fn cosine_metric_propagates_to_prism() {
        let rows = synth_rows(50, 16);
        let idx = AnnIndex::build(rows, Metric::Cosine, 16).unwrap();
        assert_eq!(idx.metric, Metric::Cosine);
        assert_eq!(idx.prism.config.metric, Metric::Cosine);
    }

    #[test]
    fn inner_metric_propagates_to_prism() {
        let rows = synth_rows(50, 16);
        let idx = AnnIndex::build(rows, Metric::InnerProduct, 16).unwrap();
        assert_eq!(idx.metric, Metric::InnerProduct);
        assert_eq!(idx.prism.config.metric, Metric::InnerProduct);
    }

    /// attr 0 = i % 2; row_id = i + 1.
    fn attr_rows(n: u64, dim: u16) -> Vec<(u64, Vec<f32>, Vec<u32>)> {
        (0..n)
            .map(|i| {
                let v: Vec<f32> = (0..dim).map(|d| (i as f32 + d as f32) * 0.01).collect();
                (i + 1, v, vec![(i % 2) as u32])
            })
            .collect()
    }

    #[test]
    fn build_with_attrs_filters_by_attribute() {
        let idx = AnnIndex::build_with_attrs(attr_rows(100, 8), 1, Metric::L2, 8).unwrap();
        let hits = idx.search_filtered(&[0.5; 8], 10, 200, &Filter::eq(0, 1));
        assert!(!hits.is_empty());
        assert!(hits.len() <= 10);
        for (rid, _) in &hits {
            // category 1 == odd i == even row_id.
            assert_eq!(rid % 2, 0, "row {rid} is not category 1");
        }
    }

    #[test]
    fn build_with_attrs_unfiltered_spans_all_cells() {
        let idx = AnnIndex::build_with_attrs(attr_rows(100, 8), 1, Metric::L2, 8).unwrap();
        let hits = idx.search_with_ef(&[0.5; 8], 10, 200);
        assert_eq!(hits.len(), 10);
        for (rid, _) in &hits {
            assert!(*rid >= 1 && *rid <= 100);
        }
    }

    #[test]
    fn build_with_attrs_two_dims_conjunctive_filter() {
        let n = 180u64;
        let dim = 8u16;
        let rows: Vec<(u64, Vec<f32>, Vec<u32>)> = (0..n)
            .map(|i| {
                let v: Vec<f32> = (0..dim).map(|d| (i as f32 + d as f32) * 0.01).collect();
                (i + 1, v, vec![(i % 2) as u32, (i % 3) as u32])
            })
            .collect();
        let idx = AnnIndex::build_with_attrs(rows, 2, Metric::L2, dim).unwrap();
        let filter = Filter::new(vec![(0, vec![1]), (1, vec![2])]);
        let hits = idx.search_filtered(&[0.5; 8], 10, 200, &filter);
        assert!(!hits.is_empty());
        for (rid, _) in &hits {
            let i = rid - 1;
            assert_eq!(i % 2, 1, "row {rid} fails attr0 = 1");
            assert_eq!(i % 3, 2, "row {rid} fails attr1 = 2");
        }
    }

    #[test]
    fn build_with_attrs_arity_mismatch_errors() {
        let rows = vec![(1u64, vec![0.0; 4], vec![0u32])];
        let err = AnnIndex::build_with_attrs(rows, 2, Metric::L2, 4).unwrap_err();
        assert!(matches!(
            err,
            AnnError::AttrArityMismatch {
                expected: 2,
                got: 1,
                row_id: 1
            }
        ));
    }

    #[test]
    fn build_delegates_to_attrs_path() {
        let idx = AnnIndex::build(synth_rows(50, 8), Metric::L2, 8).unwrap();
        assert_eq!(idx.indexed_len(), 50);
        let hits = idx.search(&[0.3; 8], 5);
        assert!(!hits.is_empty());
    }
}
