//! Find the first index whose element exceeds a threshold.

/// The index of the FIRST element of `xs` that is greater than `threshold`, or
/// `None` if no element exceeds it.
pub fn first_index_gt(xs: &[i32], threshold: i32) -> Option<usize> {
    let mut found = None;
    for (i, &x) in xs.iter().enumerate() {
        if x > threshold {
            found = Some(i);
        }
    }
    found
}
