use swe_operator_precedence::shift_add;

// PASS_TO_PASS: when a is 0 or k is 0 the two groupings coincide.
#[test]
fn coinciding_cases() {
    assert_eq!(shift_add(0, 5, 2), 20);
    assert_eq!(shift_add(0, 1, 4), 16);
    assert_eq!(shift_add(3, 4, 0), 7);
    assert_eq!(shift_add(7, 2, 0), 9);
}
