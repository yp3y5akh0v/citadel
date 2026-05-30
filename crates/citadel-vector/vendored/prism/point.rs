/// Flat f32 storage for vectors + per-point attribute metadata.
///
/// Vectors are stored in a contiguous `Vec<f32>` with stride = `dim`.
/// Attributes are stored as `k` arrays of `u32`, one per attribute dimension.
pub struct PointStore {
    /// Contiguous vector data: point i is at `vectors[i*dim..(i+1)*dim]`
    pub vectors: Vec<f32>,
    /// Number of dimensions per vector
    pub dim: usize,
    /// Number of points
    pub len: usize,
    /// Attribute values: `attrs[j][i]` = value of attribute j for point i
    pub attrs: Vec<Vec<u32>>,
}

impl PointStore {
    pub fn new(dim: usize, k: usize) -> Self {
        Self {
            vectors: Vec::new(),
            dim,
            len: 0,
            attrs: vec![Vec::new(); k],
        }
    }

    /// Build from pre-allocated vectors and attributes.
    pub fn from_parts(vectors: Vec<f32>, dim: usize, attrs: Vec<Vec<u32>>) -> Self {
        let len = vectors.len() / dim;
        debug_assert_eq!(vectors.len(), len * dim);
        for a in &attrs {
            debug_assert_eq!(a.len(), len);
        }
        Self {
            vectors,
            dim,
            len,
            attrs,
        }
    }

    /// Number of attribute dimensions.
    pub fn k(&self) -> usize {
        self.attrs.len()
    }

    /// Get the vector slice for point `id`.
    #[inline]
    pub fn vector(&self, id: u32) -> &[f32] {
        let start = id as usize * self.dim;
        &self.vectors[start..start + self.dim]
    }

    /// Get attribute value for point `id` on dimension `j`.
    #[inline]
    pub fn attr(&self, id: u32, j: usize) -> u32 {
        self.attrs[j][id as usize]
    }

    /// Append a single point. Returns its id.
    pub fn push(&mut self, vector: &[f32], attr_values: &[u32]) -> u32 {
        debug_assert_eq!(vector.len(), self.dim);
        debug_assert_eq!(attr_values.len(), self.attrs.len());
        let id = self.len as u32;
        self.vectors.extend_from_slice(vector);
        for (j, &val) in attr_values.iter().enumerate() {
            self.attrs[j].push(val);
        }
        self.len += 1;
        id
    }

    /// Number of distinct values for attribute dimension `j`.
    pub fn cardinality(&self, j: usize) -> usize {
        let mut seen = std::collections::HashSet::new();
        for &v in &self.attrs[j] {
            seen.insert(v);
        }
        seen.len()
    }
}

impl Drop for PointStore {
    fn drop(&mut self) {
        // These vectors may be DECRYPTED plaintext (citadel-mem's sealed ANN cache);
        // zero them so they never outlive the region key after crypto-erasure.
        use zeroize::Zeroize;
        self.vectors.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_store_basic() {
        let mut store = PointStore::new(3, 2);
        let id0 = store.push(&[1.0, 2.0, 3.0], &[0, 1]);
        let id1 = store.push(&[4.0, 5.0, 6.0], &[1, 0]);
        assert_eq!(id0, 0);
        assert_eq!(id1, 1);
        assert_eq!(store.len, 2);
        assert_eq!(store.vector(0), &[1.0, 2.0, 3.0]);
        assert_eq!(store.vector(1), &[4.0, 5.0, 6.0]);
        assert_eq!(store.attr(0, 0), 0);
        assert_eq!(store.attr(0, 1), 1);
        assert_eq!(store.attr(1, 0), 1);
        assert_eq!(store.attr(1, 1), 0);
    }

    #[test]
    fn test_from_parts() {
        let vectors = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let attrs = vec![vec![0, 1], vec![1, 0]];
        let store = PointStore::from_parts(vectors, 3, attrs);
        assert_eq!(store.len, 2);
        assert_eq!(store.k(), 2);
        assert_eq!(store.cardinality(0), 2);
    }
}
