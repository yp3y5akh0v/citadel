use swe_recursive_digit_sum::digit_sum;

// FAIL_TO_PASS: a multi-digit number must sum all of its digits.
#[test]
fn three_digit_number() {
    assert_eq!(digit_sum(123), 6);
}
