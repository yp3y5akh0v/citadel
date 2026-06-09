//! Combine a base value with a shifted value.

/// Returns `a + (b << k)`.
pub fn shift_add(a: u32, b: u32, k: u32) -> u32 {
    a + b << k
}
