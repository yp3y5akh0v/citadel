use swe_operator_precedence::shift_add;

// FAIL_TO_PASS: must compute a + (b << k), not (a + b) << k.
#[test]
fn base_plus_shifted() {
    assert_eq!(shift_add(1, 2, 3), 17);
}
