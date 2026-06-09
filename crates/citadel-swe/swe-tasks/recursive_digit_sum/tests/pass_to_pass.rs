use swe_recursive_digit_sum::digit_sum;

// PASS_TO_PASS: single-digit inputs already work on the buggy crate.
#[test]
fn single_digits_unchanged() {
    assert_eq!(digit_sum(0), 0);
    assert_eq!(digit_sum(7), 7);
    assert_eq!(digit_sum(9), 9);
}
