use super::point::PointStore;
use rayon::prelude::*;

/// Binary code store for Hamming distance pre-filtering.
/// Encodes vectors as 1-bit-per-dimension codes via randomized Walsh-Hadamard
/// rotation + sign extraction (SimHash).
pub struct BinaryStore {
    codes: Vec<u64>,
    code_words: usize,
    signs: Vec<f32>,
    block_size: usize,
}

impl BinaryStore {
    /// Reassemble from persisted parts (the ANN segment decode path). The
    /// SIGNS are persisted rather than re-derived from the seed, so a future
    /// seed change can never silently desynchronize codes from queries.
    pub fn from_parts(
        codes: Vec<u64>,
        code_words: usize,
        signs: Vec<f32>,
        block_size: usize,
    ) -> Self {
        Self {
            codes,
            code_words,
            signs,
            block_size,
        }
    }

    pub fn codes(&self) -> &[u64] {
        &self.codes
    }

    pub fn signs(&self) -> &[f32] {
        &self.signs
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Build binary codes: random sign flips (D) + Walsh-Hadamard in blocks of
    /// `largest_pow2_factor(dim)`. Fixed seed for build/query consistency.
    pub fn build(store: &PointStore) -> Self {
        let n = store.len;
        let dim = store.dim;
        let code_words = dim.div_ceil(64);
        let block_size = largest_pow2_factor(dim);
        let signs = seeded_signs(dim);

        let mut codes = vec![0u64; n * code_words];
        codes
            .par_chunks_mut(code_words)
            .enumerate()
            .for_each(|(i, chunk)| {
                encode_vector(store.vector(i as u32), &signs, block_size, chunk);
            });

        Self {
            codes,
            code_words,
            signs,
            block_size,
        }
    }

    /// A store with signs but no codes, for configs that never consult the
    /// binary pre-filter (`binary_rerank == 0`). `encode_query` stays valid;
    /// `code()` must not be reached (every caller is gated on the rerank
    /// factor), so the per-point encoding pass and its memory are skipped.
    pub fn empty(dim: usize) -> Self {
        Self {
            codes: Vec::new(),
            code_words: dim.div_ceil(64),
            signs: seeded_signs(dim),
            block_size: largest_pow2_factor(dim),
        }
    }

    /// Get the binary code (packed u64 words) for point id.
    #[inline]
    pub fn code(&self, id: u32) -> &[u64] {
        let start = id as usize * self.code_words;
        &self.codes[start..start + self.code_words]
    }

    /// Number of u64 words per binary code.
    #[inline]
    pub fn code_words(&self) -> usize {
        self.code_words
    }

    /// Encode a query vector to binary code using the same HD rotation.
    pub fn encode_query(&self, query: &[f32]) -> Vec<u64> {
        let mut code = vec![0u64; self.code_words];
        encode_vector(query, &self.signs, self.block_size, &mut code);
        code
    }
}

/// Seed-fixed random sign flips shared by build and query encoding.
fn seeded_signs(dim: usize) -> Vec<f32> {
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x505249534D);
    (0..dim)
        .map(|_| if rng.gen_bool(0.5) { 1.0 } else { -1.0 })
        .collect()
}

/// Apply HD rotation (sign flip + WHT) and extract signs into packed u64 code.
fn encode_vector(vec: &[f32], signs: &[f32], block_size: usize, out: &mut [u64]) {
    let dim = vec.len();
    let mut buf: Vec<f32> = vec.iter().enumerate().map(|(d, &v)| v * signs[d]).collect();
    for start in (0..dim).step_by(block_size) {
        walsh_hadamard(&mut buf[start..start + block_size]);
    }
    for d in 0..dim {
        if buf[d] >= 0.0 {
            out[d / 64] |= 1u64 << (d % 64);
        }
    }
}

/// In-place Walsh-Hadamard transform on a slice of length 2^k.
/// Not normalized (irrelevant for sign extraction).
fn walsh_hadamard(data: &mut [f32]) {
    let n = data.len();
    debug_assert!(n.is_power_of_two());
    if n <= 1 {
        return;
    }
    let mut half = 1;
    while half < n {
        let step = half * 2;
        for i in (0..n).step_by(step) {
            for j in 0..half {
                let a = data[i + j];
                let b = data[i + j + half];
                data[i + j] = a + b;
                data[i + j + half] = a - b;
            }
        }
        half = step;
    }
}

/// Largest power-of-2 factor of n (i.e., 2^(trailing zeros of n)).
fn largest_pow2_factor(n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    1 << n.trailing_zeros()
}

#[cfg(test)]
mod tests {
    use super::super::point::PointStore;
    use super::*;

    #[test]
    fn test_walsh_hadamard_identity() {
        let mut data = vec![1.0, 0.0, 0.0, 0.0];
        walsh_hadamard(&mut data);
        assert_eq!(data, vec![1.0, 1.0, 1.0, 1.0]);
    }

    #[test]
    fn test_walsh_hadamard_butterfly() {
        let mut data = vec![1.0, 1.0];
        walsh_hadamard(&mut data);
        assert_eq!(data, vec![2.0, 0.0]);

        let mut data = vec![1.0, -1.0];
        walsh_hadamard(&mut data);
        assert_eq!(data, vec![0.0, 2.0]);
    }

    #[test]
    fn test_largest_pow2_factor() {
        assert_eq!(largest_pow2_factor(384), 128);
        assert_eq!(largest_pow2_factor(128), 128);
        assert_eq!(largest_pow2_factor(256), 256);
        assert_eq!(largest_pow2_factor(12), 4);
        assert_eq!(largest_pow2_factor(1), 1);
    }

    #[test]
    fn test_binary_query_encoding() {
        let dim = 128;
        let p0: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.1).sin()).collect();
        let mut vecs = Vec::with_capacity(dim);
        vecs.extend_from_slice(&p0);

        let store = PointStore::from_parts(vecs, dim, vec![vec![0]]);
        let binary = BinaryStore::build(&store);

        let q = binary.encode_query(&p0);
        let c0 = binary.code(0);
        assert_eq!(q, c0, "query encoding must match point encoding");
    }

    #[test]
    fn test_hamming_distance_ordering() {
        use super::super::distance;
        let dim = 128;
        let p0: Vec<f32> = (0..dim).map(|i| (i as f32 + 1.0) / dim as f32).collect();
        let p1: Vec<f32> = p0.iter().map(|&v| v + 0.001).collect();
        let p2: Vec<f32> = p0.iter().map(|&v| -v).collect();

        let mut vecs = Vec::with_capacity(3 * dim);
        vecs.extend_from_slice(&p0);
        vecs.extend_from_slice(&p1);
        vecs.extend_from_slice(&p2);

        let store = PointStore::from_parts(vecs, dim, vec![vec![0, 0, 0]]);
        let binary = BinaryStore::build(&store);
        let q = binary.encode_query(&p0);

        let d0 = distance::hamming(&q, binary.code(0));
        let d1 = distance::hamming(&q, binary.code(1));
        let d2 = distance::hamming(&q, binary.code(2));

        assert_eq!(d0, 0, "same vector must have 0 Hamming distance");
        assert!(
            d1 < d2,
            "close vector (d={d1}) must have smaller Hamming than opposite (d={d2})"
        );
    }

    #[test]
    fn test_binary_code_words() {
        let store = PointStore::from_parts(vec![0.0; 128], 128, vec![vec![0]]);
        let binary = BinaryStore::build(&store);
        assert_eq!(binary.code_words(), 2);

        let store = PointStore::from_parts(vec![0.0; 384], 384, vec![vec![0]]);
        let binary = BinaryStore::build(&store);
        assert_eq!(binary.code_words(), 6);
    }
}
