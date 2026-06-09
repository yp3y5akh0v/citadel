use swe_checked_overflow_edge::avg;

// FAIL_TO_PASS: large inputs overflow the naive `a + b` (the test profile has
// overflow checks on, so the buggy version panics).
#[test]
fn averages_large_values_without_overflow() {
    assert_eq!(avg(i32::MAX, i32::MAX), i32::MAX);
}
