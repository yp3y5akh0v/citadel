//! Sliding-window maximum over a slice.

/// The maximum of each contiguous window of size `k` in `xs`, one value per
/// window. Returns an empty vec when `k` is 0 or larger than `xs`.
pub fn window_maxes(xs: &[i32], k: usize) -> Vec<i32> {
    if k == 0 || k > xs.len() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for i in 0..xs.len() - k {
        out.push(*xs[i..i + k].iter().max().unwrap());
    }
    out
}
