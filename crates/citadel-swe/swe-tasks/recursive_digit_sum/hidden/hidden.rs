use swe_recursive_digit_sum::digit_sum;

// Held-out: numbers whose skipped digits the bug drops.
#[test]
fn multi_digit_sums() {
    assert_eq!(digit_sum(12), 3);
    assert_eq!(digit_sum(45), 9);
    assert_eq!(digit_sum(99), 18);
    assert_eq!(digit_sum(1000), 1);
}
