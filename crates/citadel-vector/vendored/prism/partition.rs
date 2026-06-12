use super::point::PointStore;

/// A single leaf cell in the Attribute Partition Tree.
#[derive(Clone, Debug)]
pub struct Cell {
    /// Attribute values that define this cell: `values[j]` = value for attribute j.
    pub values: Vec<u32>,
    /// Point ids belonging to this cell.
    pub point_ids: Vec<u32>,
}

/// Attribute Partition Tree (Algorithm 1 from the paper).
///
/// Recursive balanced partition on attributes. Each leaf is a cell of points
/// sharing the same attribute combination.
pub struct PartitionTree {
    /// All leaf cells.
    pub cells: Vec<Cell>,
    /// Attribute split order (permutation of [0..k]).
    pub split_order: Vec<usize>,
    /// Number of attribute dimensions.
    pub k: usize,
}

impl PartitionTree {
    /// Build the partition tree from a PointStore.
    /// Split order: most-distinct-values first (information gain heuristic).
    pub fn build(store: &PointStore) -> Self {
        let k = store.k();
        let n = store.len;

        // Determine split order: descending by cardinality
        let mut order: Vec<usize> = (0..k).collect();
        order.sort_by_key(|&b| std::cmp::Reverse(store.cardinality(b)));

        // Group points by their full attribute combination
        let mut groups: std::collections::HashMap<Vec<u32>, Vec<u32>> =
            std::collections::HashMap::new();
        for i in 0..n {
            let key: Vec<u32> = (0..k).map(|j| store.attr(i as u32, j)).collect();
            groups.entry(key).or_default().push(i as u32);
        }

        let mut cells: Vec<Cell> = groups
            .into_iter()
            .map(|(values, point_ids)| Cell { values, point_ids })
            .collect();
        // HashMap iteration order is random per process; sort so identical
        // data always builds identical (and byte-identical persisted) indexes.
        cells.sort_unstable_by(|a, b| a.values.cmp(&b.values));

        Self {
            cells,
            split_order: order,
            k,
        }
    }

    /// Find all cells compatible with a filter.
    /// A cell is compatible if for every constrained attribute j,
    /// the cell's value on j is in the allowed set.
    pub fn filter_cells(&self, constraints: &[(usize, Vec<u32>)]) -> Vec<usize> {
        self.cells
            .iter()
            .enumerate()
            .filter(|(_, cell)| {
                constraints
                    .iter()
                    .all(|(j, allowed)| allowed.contains(&cell.values[*j]))
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// Total number of points across given cell indices.
    pub fn count_points(&self, cell_indices: &[usize]) -> usize {
        cell_indices
            .iter()
            .map(|&i| self.cells[i].point_ids.len())
            .sum()
    }

    /// Get all point ids in the given cell indices.
    pub fn collect_points(&self, cell_indices: &[usize]) -> Vec<u32> {
        let mut pts = Vec::new();
        for &i in cell_indices {
            pts.extend_from_slice(&self.cells[i].point_ids);
        }
        pts
    }

    /// Find which cell a point belongs to. Returns cell index.
    pub fn cell_of(&self, store: &PointStore, point_id: u32) -> Option<usize> {
        let key: Vec<u32> = (0..self.k).map(|j| store.attr(point_id, j)).collect();
        self.cells.iter().position(|c| c.values == key)
    }
}

#[cfg(test)]
mod tests {
    use super::super::point::PointStore;
    use super::*;

    #[test]
    fn test_partition_tree() {
        // 6 points, 2 attributes: color(3 values), size(2 values)
        let vectors = vec![0.0f32; 6 * 2];
        let attrs = vec![
            vec![0, 0, 1, 1, 2, 2], // color
            vec![0, 1, 0, 1, 0, 1], // size
        ];
        let store = PointStore::from_parts(vectors, 2, attrs);
        let tree = PartitionTree::build(&store);
        assert_eq!(tree.cells.len(), 6); // 3*2 = 6 distinct combos

        // Filter: color=0
        let cells = tree.filter_cells(&[(0, vec![0])]);
        let pts = tree.collect_points(&cells);
        assert_eq!(pts.len(), 2);

        // Filter: color=1 AND size=0
        let cells = tree.filter_cells(&[(0, vec![1]), (1, vec![0])]);
        let pts = tree.collect_points(&cells);
        assert_eq!(pts.len(), 1);
    }
}
