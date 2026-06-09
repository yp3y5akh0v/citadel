use swe_checked_overflow_edge::avg;

// Held-out: the extremes whose sum overflows i32.
#[test]
fn averages_extremes_without_overflow() {
    assert_eq!(avg(i32::MIN, i32::MIN), i32::MIN);
    assert_eq!(avg(2_000_000_000, 2_000_000_000), 2_000_000_000);
    assert_eq!(avg(2_000_000_000, 1_000_000_000), 1_500_000_000);
}
