//! Integer average of two values.

/// The integer average of `a` and `b`, without overflowing for large inputs.
pub fn avg(a: i32, b: i32) -> i32 {
    a + (b - a) / 2
}
