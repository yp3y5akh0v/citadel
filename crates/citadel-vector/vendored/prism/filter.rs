use super::point::PointStore;

/// A conjunctive filter: `(attr_index, allowed_values)` pairs.
/// Point passes iff for every constrained attribute, its value is in the allowed set.
#[derive(Clone, Debug)]
pub struct Filter {
    /// Each entry: (attribute_index, set of allowed values)
    constraints: Vec<(usize, Vec<u32>)>,
}

impl Filter {
    /// Create from a list of (attr_index, allowed_values).
    pub fn new(constraints: Vec<(usize, Vec<u32>)>) -> Self {
        Self { constraints }
    }

    /// Unconstrained filter (matches everything).
    pub fn none() -> Self {
        Self {
            constraints: Vec::new(),
        }
    }

    /// Single equality filter: attribute `j` must equal `val`.
    pub fn eq(j: usize, val: u32) -> Self {
        Self {
            constraints: vec![(j, vec![val])],
        }
    }

    /// Number of constrained attributes (strength).
    pub fn strength(&self) -> usize {
        self.constraints.len()
    }

    /// Check if point `id` passes this filter.
    #[inline]
    pub fn matches(&self, store: &PointStore, id: u32) -> bool {
        for &(j, ref allowed) in &self.constraints {
            let val = store.attr(id, j);
            if !allowed.contains(&val) {
                return false;
            }
        }
        true
    }

    /// Get the constraints.
    pub fn constraints(&self) -> &[(usize, Vec<u32>)] {
        &self.constraints
    }
}

#[cfg(test)]
mod tests {
    use super::super::point::PointStore;
    use super::*;

    #[test]
    fn test_filter_matches() {
        let store = PointStore::from_parts(vec![0.0; 6], 2, vec![vec![0, 1, 2], vec![10, 20, 30]]);
        let f = Filter::new(vec![(0, vec![0, 1]), (1, vec![20])]);
        assert!(!f.matches(&store, 0)); // attr0=0 ok, attr1=10 fail
        assert!(f.matches(&store, 1)); // attr0=1 ok, attr1=20 ok
        assert!(!f.matches(&store, 2)); // attr0=2 fail
    }

    #[test]
    fn test_filter_none() {
        let store = PointStore::from_parts(vec![0.0; 3], 3, vec![vec![5]]);
        let f = Filter::none();
        assert!(f.matches(&store, 0));
    }

    #[test]
    fn test_filter_strength() {
        let f = Filter::new(vec![(0, vec![1]), (2, vec![3, 4])]);
        assert_eq!(f.strength(), 2);
    }
}
