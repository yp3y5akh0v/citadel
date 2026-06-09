use swe_checked_overflow_edge::avg;

// PASS_TO_PASS: small even-sum inputs (no overflow, no rounding ambiguity)
// already work on the buggy crate.
#[test]
fn small_even_sums() {
    assert_eq!(avg(2, 4), 3);
    assert_eq!(avg(10, 20), 15);
    assert_eq!(avg(-2, -4), -3);
    assert_eq!(avg(100, 100), 100);
}
