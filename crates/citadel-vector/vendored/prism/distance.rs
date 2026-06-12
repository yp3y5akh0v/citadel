/// Distance metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Metric {
    L2,
    InnerProduct,
    Cosine,
}

/// L2 squared distance between two vectors.
#[inline]
pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { l2_squared_avx2(a, b) };
        }
        if is_x86_feature_detected!("sse") {
            return unsafe { l2_squared_sse(a, b) };
        }
    }

    l2_squared_scalar(a, b)
}

/// Inner product (negative for distance: higher IP = closer).
#[inline]
pub fn inner_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { inner_product_avx2(a, b) };
        }
    }

    inner_product_scalar(a, b)
}

/// Cosine distance: `1 - (a . b) / (||a|| * ||b||)`. Returns 1.0 if either
/// vector has zero norm (degenerate case).
#[inline]
pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (&x, &y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom == 0.0 {
        1.0
    } else {
        1.0 - dot / denom
    }
}

/// Compute distance using the given metric.
#[inline]
pub fn distance(a: &[f32], b: &[f32], metric: Metric) -> f32 {
    match metric {
        Metric::L2 => l2_squared(a, b),
        Metric::InnerProduct => -inner_product(a, b),
        Metric::Cosine => cosine(a, b),
    }
}

/// Total-order u32 key for an f32: `a < b` iff `ord_key(a) < ord_key(b)`,
/// for any sign mix. Lets exact f32 distances flow through the u32
/// candidate heaps used for SQ8 ranking.
#[inline]
pub fn ord_key(x: f32) -> u32 {
    let b = x.to_bits();
    if b & 0x8000_0000 == 0 {
        b | 0x8000_0000
    } else {
        !b
    }
}

/// L2-normalize each `dim`-stride row in place; zero rows are left unchanged.
pub fn normalize_rows(data: &mut [f32], dim: usize) {
    for row in data.chunks_mut(dim) {
        let norm = row.iter().map(|&x| x as f64 * x as f64).sum::<f64>().sqrt();
        if norm > 0.0 {
            let inv = (1.0 / norm) as f32;
            for x in row {
                *x *= inv;
            }
        }
    }
}

/// L2-normalized copy of a single vector.
pub fn normalized(v: &[f32]) -> Vec<f32> {
    let mut out = v.to_vec();
    normalize_rows(&mut out, v.len());
    out
}

/// L2 squared distance between two SQ8 (u8) vectors.
#[inline]
pub fn l2_sq8(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { l2_sq8_avx2(a, b) };
        }
    }

    l2_sq8_scalar(a, b)
}

fn l2_sq8_scalar(a: &[u8], b: &[u8]) -> u32 {
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| {
            let d = x as i32 - y as i32;
            (d * d) as u32
        })
        .sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn l2_sq8_avx2(a: &[u8], b: &[u8]) -> u32 {
    use std::arch::x86_64::*;

    let n = a.len();
    let chunks = n / 32;
    let remainder = n % 32;

    let mut acc = _mm256_setzero_si256();
    let ap = a.as_ptr();
    let bp = b.as_ptr();

    for i in 0..chunks {
        let va = _mm256_loadu_si256(ap.add(i * 32) as *const __m256i);
        let vb = _mm256_loadu_si256(bp.add(i * 32) as *const __m256i);

        // Low 16 bytes -> 16 x i16, subtract, square-and-sum-adjacent -> 8 x i32.
        let a_lo = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(va));
        let b_lo = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(vb));
        let diff_lo = _mm256_sub_epi16(a_lo, b_lo);
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(diff_lo, diff_lo));

        // High 16 bytes: same.
        let a_hi = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(va, 1));
        let b_hi = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(vb, 1));
        let diff_hi = _mm256_sub_epi16(a_hi, b_hi);
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(diff_hi, diff_hi));
    }

    // Horizontal sum of 8 x i32.
    let hi = _mm256_extracti128_si256(acc, 1);
    let lo = _mm256_castsi256_si128(acc);
    let sum128 = _mm_add_epi32(lo, hi);
    let hi64 = _mm_unpackhi_epi64(sum128, sum128);
    let sum64 = _mm_add_epi32(sum128, hi64);
    let hi32 = _mm_shuffle_epi32(sum64, 1);
    let sum32 = _mm_add_epi32(sum64, hi32);
    let mut total = _mm_cvtsi128_si32(sum32) as u32;

    let offset = chunks * 32;
    for i in 0..remainder {
        let d = a[offset + i] as i32 - b[offset + i] as i32;
        total += (d * d) as u32;
    }

    total
}

/// Hamming distance between binary codes packed as u64 words (XOR + POPCNT).
#[inline]
pub fn hamming(a: &[u64], b: &[u64]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| (x ^ y).count_ones())
        .sum()
}

fn l2_squared_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| {
            let d = x - y;
            d * d
        })
        .sum()
}

fn inner_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn l2_squared_avx2(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let n = a.len();
    let chunks = n / 8;
    let remainder = n % 8;

    let mut sum = _mm256_setzero_ps();

    let ap = a.as_ptr();
    let bp = b.as_ptr();

    for i in 0..chunks {
        let va = _mm256_loadu_ps(ap.add(i * 8));
        let vb = _mm256_loadu_ps(bp.add(i * 8));
        let diff = _mm256_sub_ps(va, vb);
        sum = _mm256_fmadd_ps(diff, diff, sum);
    }

    // Horizontal sum of 8 floats
    let hi = _mm256_extractf128_ps(sum, 1);
    let lo = _mm256_castps256_ps128(sum);
    let sum128 = _mm_add_ps(lo, hi);
    let shuf = _mm_movehdup_ps(sum128);
    let sums = _mm_add_ps(sum128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let result = _mm_add_ss(sums, shuf2);
    let mut total = _mm_cvtss_f32(result);

    let offset = chunks * 8;
    for i in 0..remainder {
        let d = a[offset + i] - b[offset + i];
        total += d * d;
    }

    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn inner_product_avx2(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let n = a.len();
    let chunks = n / 8;
    let remainder = n % 8;

    let mut sum = _mm256_setzero_ps();
    let ap = a.as_ptr();
    let bp = b.as_ptr();

    for i in 0..chunks {
        let va = _mm256_loadu_ps(ap.add(i * 8));
        let vb = _mm256_loadu_ps(bp.add(i * 8));
        sum = _mm256_fmadd_ps(va, vb, sum);
    }

    let hi = _mm256_extractf128_ps(sum, 1);
    let lo = _mm256_castps256_ps128(sum);
    let sum128 = _mm_add_ps(lo, hi);
    let shuf = _mm_movehdup_ps(sum128);
    let sums = _mm_add_ps(sum128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let result = _mm_add_ss(sums, shuf2);
    let mut total = _mm_cvtss_f32(result);

    let offset = chunks * 8;
    for i in 0..remainder {
        total += a[offset + i] * b[offset + i];
    }

    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
unsafe fn l2_squared_sse(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let n = a.len();
    let chunks = n / 4;
    let remainder = n % 4;

    let mut sum = _mm_setzero_ps();
    let ap = a.as_ptr();
    let bp = b.as_ptr();

    for i in 0..chunks {
        let va = _mm_loadu_ps(ap.add(i * 4));
        let vb = _mm_loadu_ps(bp.add(i * 4));
        let diff = _mm_sub_ps(va, vb);
        let sq = _mm_mul_ps(diff, diff);
        sum = _mm_add_ps(sum, sq);
    }

    let shuf = _mm_movehdup_ps(sum);
    let sums = _mm_add_ps(sum, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let result = _mm_add_ss(sums, shuf2);
    let mut total = _mm_cvtss_f32(result);

    let offset = chunks * 4;
    for i in 0..remainder {
        let d = a[offset + i] - b[offset + i];
        total += d * d;
    }

    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_squared() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let d = l2_squared(&a, &b);
        assert!((d - 27.0).abs() < 1e-6);
    }

    #[test]
    fn test_l2_squared_large() {
        let dim = 128;
        let a: Vec<f32> = (0..dim).map(|i| i as f32).collect();
        let b: Vec<f32> = (0..dim).map(|i| (i as f32) + 1.0).collect();
        let d = l2_squared(&a, &b);
        assert!((d - dim as f32).abs() < 1e-3); // each diff=1, so sum = dim
    }

    #[test]
    fn test_inner_product() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let ip = inner_product(&a, &b);
        assert!((ip - 32.0).abs() < 1e-6);
    }

    #[test]
    fn test_distance_metric() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        assert!((distance(&a, &b, Metric::L2) - 25.0).abs() < 1e-6);
    }

    #[test]
    fn test_l2_sq8() {
        let a: Vec<u8> = vec![10, 20, 30, 40];
        let b: Vec<u8> = vec![11, 22, 27, 45];
        // (1)^2 + (2)^2 + (3)^2 + (5)^2 = 1 + 4 + 9 + 25 = 39
        assert_eq!(l2_sq8(&a, &b), 39);
    }

    #[test]
    fn test_hamming() {
        assert_eq!(hamming(&[0b1010], &[0b1001]), 2);
        assert_eq!(hamming(&[0, 0], &[0, 0]), 0);
        assert_eq!(hamming(&[u64::MAX], &[0]), 64);
        // Multi-word
        assert_eq!(hamming(&[u64::MAX, 0], &[0, u64::MAX]), 128);
    }

    #[test]
    fn ord_key_is_monotone_across_signs() {
        let vals = [-1e9f32, -100.0, -1.5, -0.0, 0.0, 1e-10, 3.0, 1e9];
        for w in vals.windows(2) {
            assert!(
                ord_key(w[0]) <= ord_key(w[1]),
                "ord_key({}) > ord_key({})",
                w[0],
                w[1]
            );
        }
        assert!(ord_key(-1.0) < ord_key(1.0));
    }

    #[test]
    fn normalize_rows_unit_norms_and_zero_rows() {
        let mut data = vec![3.0, 4.0, 0.0, 0.0, -2.0, 0.0];
        normalize_rows(&mut data, 2);
        assert!((data[0] - 0.6).abs() < 1e-6);
        assert!((data[1] - 0.8).abs() < 1e-6);
        assert_eq!(&data[2..4], &[0.0, 0.0]);
        assert!((data[4] + 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_l2_sq8_large() {
        let dim = 128;
        let a: Vec<u8> = (0..dim).map(|i| i as u8).collect();
        let b: Vec<u8> = (0..dim).map(|i| (i as u8).wrapping_add(1)).collect();
        // Every pair differs by exactly 1 (a[127]=127, b[127]=128), so the
        // squared sum is dim = 128.
        assert_eq!(l2_sq8(&a, &b), 128);
    }

    #[test]
    fn cosine_orthogonal_is_one() {
        assert!((cosine(&[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_parallel_is_zero() {
        assert!(cosine(&[1.0, 2.0, 3.0], &[2.0, 4.0, 6.0]).abs() < 1e-6);
    }

    #[test]
    fn cosine_antiparallel_is_two() {
        assert!((cosine(&[1.0, 0.0], &[-1.0, 0.0]) - 2.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_zero_vector_returns_one() {
        assert_eq!(cosine(&[0.0, 0.0, 0.0], &[1.0, 2.0, 3.0]), 1.0);
    }

    #[test]
    fn metric_dispatch_cosine() {
        let d = distance(&[1.0, 0.0], &[0.0, 1.0], Metric::Cosine);
        assert!((d - 1.0).abs() < 1e-6);
    }
}
