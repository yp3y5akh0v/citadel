//! Arithmetic mean of a slice of values.

/// The arithmetic mean of `xs`, or `None` if `xs` is empty.
pub fn mean(xs: &[f64]) -> Option<f64> {
    if xs.is_empty() {
        return None;
    }
    Some(xs.iter().sum::<f64>() / xs.len() as f64)
}
