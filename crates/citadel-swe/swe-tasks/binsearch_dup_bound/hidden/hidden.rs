use swe_binsearch_dup_bound::lower_bound;

// Held-out: present keys whose first occurrence the bug overshoots.
#[test]
fn all_equal_returns_zero() {
    assert_eq!(lower_bound(&[7, 7, 7, 7], 7), 0);
}

#[test]
fn first_of_a_run() {
    assert_eq!(lower_bound(&[5], 5), 0);
    assert_eq!(lower_bound(&[2, 2, 2, 8], 2), 0);
    assert_eq!(lower_bound(&[1, 3, 3, 7], 3), 1);
}
