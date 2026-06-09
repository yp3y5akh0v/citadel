//! Sum of the decimal digits of a number, computed recursively.

/// The sum of the decimal digits of `n` (for example `123` -> `6`).
pub fn digit_sum(n: u32) -> u32 {
    if n == 0 {
        return 0;
    }
    n % 10 + digit_sum(n / 100)
}
