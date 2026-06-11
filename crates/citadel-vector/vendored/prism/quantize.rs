use super::point::PointStore;

/// Scalar-quantized (8-bit) vector store. 4x bandwidth reduction vs f32,
/// identity quantization for native u8 data (SIFT, YFCC).
pub struct SQ8Store {
    codes: Vec<u8>,
    mins: Vec<f32>,
    scales: Vec<f32>,
    dim: usize,
}

impl Drop for SQ8Store {
    fn drop(&mut self) {
        // Lossy 8-bit reconstructions of decrypted vectors; zero on drop alongside the
        // full-precision PointStore so no vector residue outlives the region key.
        use zeroize::Zeroize;
        self.codes.zeroize();
        self.mins.zeroize();
        self.scales.zeroize();
    }
}

impl SQ8Store {
    /// Reassemble from persisted parts (the ANN segment decode path). Additive
    /// to the vendored algorithm: construction semantics are untouched.
    pub fn from_parts(codes: Vec<u8>, mins: Vec<f32>, scales: Vec<f32>, dim: usize) -> Self {
        Self {
            codes,
            mins,
            scales,
            dim,
        }
    }

    pub fn codes(&self) -> &[u8] {
        &self.codes
    }

    pub fn mins(&self) -> &[f32] {
        &self.mins
    }

    pub fn scales(&self) -> &[f32] {
        &self.scales
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Build SQ8 codes. Uses identity quantization for integer [0,255] data.
    pub fn build(store: &PointStore) -> Self {
        let n = store.len;
        let dim = store.dim;

        let all_integer_byte = (0..n).all(|i| {
            store
                .vector(i as u32)
                .iter()
                .all(|&v| (0.0..=255.0).contains(&v) && v == v.round())
        });

        if all_integer_byte {
            let mut codes = vec![0u8; n * dim];
            for i in 0..n {
                let vec = store.vector(i as u32);
                let off = i * dim;
                for d in 0..dim {
                    codes[off + d] = vec[d] as u8;
                }
            }
            return Self {
                codes,
                mins: vec![0.0; dim],
                scales: vec![1.0; dim],
                dim,
            };
        }

        // Percentile-clipped quantization (p0.5..p99.5).
        let sample_n = n.min(10_000);
        let step = (n / sample_n).max(1);

        let (mins, maxs) = if sample_n >= 200 {
            let mut mins = vec![0.0f32; dim];
            let mut maxs = vec![0.0f32; dim];
            for d in 0..dim {
                let mut sample: Vec<f32> = (0..sample_n)
                    .map(|s| store.vector(((s * step).min(n - 1)) as u32)[d])
                    .collect();
                sample.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
                let lo = sample_n / 200;
                let hi = sample_n.saturating_sub(1 + sample_n / 200);
                mins[d] = sample[lo];
                maxs[d] = sample[hi.max(lo + 1).min(sample_n - 1)];
            }
            (mins, maxs)
        } else {
            let mut mins = vec![f32::MAX; dim];
            let mut maxs = vec![f32::MIN; dim];
            for i in 0..n {
                let vec = store.vector(i as u32);
                for d in 0..dim {
                    mins[d] = mins[d].min(vec[d]);
                    maxs[d] = maxs[d].max(vec[d]);
                }
            }
            (mins, maxs)
        };

        let scales: Vec<f32> = mins
            .iter()
            .zip(maxs.iter())
            .map(|(&mn, &mx)| {
                let range = mx - mn;
                if range > 0.0 {
                    range / 255.0
                } else {
                    1.0
                }
            })
            .collect();

        let mut codes = vec![0u8; n * dim];
        for i in 0..n {
            let vec = store.vector(i as u32);
            let off = i * dim;
            for d in 0..dim {
                let val = (vec[d] - mins[d]) / scales[d];
                codes[off + d] = val.round().clamp(0.0, 255.0) as u8;
            }
        }

        Self {
            codes,
            mins,
            scales,
            dim,
        }
    }

    /// Get the quantized code for point id.
    #[inline]
    pub fn code(&self, id: u32) -> &[u8] {
        let start = id as usize * self.dim;
        &self.codes[start..start + self.dim]
    }

    /// Quantize a f32 query vector to u8.
    pub fn quantize_query(&self, query: &[f32]) -> Vec<u8> {
        query
            .iter()
            .enumerate()
            .map(|(d, &v)| {
                let val = (v - self.mins[d]) / self.scales[d];
                val.round().clamp(0.0, 255.0) as u8
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sq8_roundtrip() {
        let store = PointStore::from_parts(
            vec![0.0, 0.0, 0.0, 255.0, 255.0, 255.0],
            3,
            vec![vec![0, 0]],
        );
        let sq8 = SQ8Store::build(&store);
        assert_eq!(sq8.code(0), &[0, 0, 0]);
        assert_eq!(sq8.code(1), &[255, 255, 255]);
    }

    #[test]
    fn test_sq8_midpoint() {
        let store = PointStore::from_parts(
            vec![0.0, 0.0, 255.0, 255.0, 128.0, 128.0],
            2,
            vec![vec![0, 0, 0]],
        );
        let sq8 = SQ8Store::build(&store);
        assert_eq!(sq8.code(2)[0], 128);
    }

    #[test]
    fn test_sq8_identity_quantization() {
        let store = PointStore::from_parts(
            vec![10.0, 200.0, 50.0, 150.0, 0.0, 255.0],
            2,
            vec![vec![0, 0, 0]],
        );
        let sq8 = SQ8Store::build(&store);
        assert_eq!(sq8.code(0), &[10, 200]);
        assert_eq!(sq8.code(1), &[50, 150]);
        assert_eq!(sq8.code(2), &[0, 255]);
        assert_eq!(sq8.mins, vec![0.0, 0.0]);
        assert_eq!(sq8.scales, vec![1.0, 1.0]);
    }

    #[test]
    fn test_sq8_non_identity_for_float_data() {
        let store = PointStore::from_parts(vec![0.0, 0.0, 1000.0, 500.5], 2, vec![vec![0, 0]]);
        let sq8 = SQ8Store::build(&store);
        assert_eq!(sq8.code(0), &[0, 0]);
        assert_eq!(sq8.code(1), &[255, 255]);
    }

    #[test]
    fn test_sq8_distance_ranking() {
        use super::super::distance;
        let store = PointStore::from_parts(
            vec![0.0, 0.0, 100.0, 100.0, 200.0, 200.0],
            2,
            vec![vec![0, 0, 0]],
        );
        let sq8 = SQ8Store::build(&store);
        let q = sq8.quantize_query(&[90.0, 90.0]);
        let d0 = distance::l2_sq8(&q, sq8.code(0));
        let d1 = distance::l2_sq8(&q, sq8.code(1));
        let d2 = distance::l2_sq8(&q, sq8.code(2));
        assert!(d1 < d0, "point 1 should be closer than point 0");
        assert!(d1 < d2, "point 1 should be closer than point 2");
    }
}
