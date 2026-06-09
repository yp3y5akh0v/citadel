//! Lower-bound binary search over a sorted slice.

/// The index of the first element `>= key` in the sorted slice `xs` (the
/// insertion point if `key` is absent). With duplicates this is the index of
/// the first occurrence of `key`.
pub fn lower_bound(xs: &[i32], key: i32) -> usize {
    let (mut lo, mut hi) = (0, xs.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if xs[mid] <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}
