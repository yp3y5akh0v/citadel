use swe_operator_precedence::shift_add;

// Held-out: a and k both nonzero, where the grouping matters.
#[test]
fn precedence_matters() {
    assert_eq!(shift_add(5, 3, 2), 17);
    assert_eq!(shift_add(1, 1, 1), 3);
    assert_eq!(shift_add(10, 1, 3), 18);
}
