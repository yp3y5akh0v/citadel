use swe_binsearch_dup_bound::lower_bound;

// PASS_TO_PASS: absent keys (where lower and upper bound coincide) already
// return the insertion point on the buggy crate, so a fix must keep them.
#[test]
fn absent_keys_return_insertion_point() {
    assert_eq!(lower_bound(&[2, 4, 6], 1), 0);
    assert_eq!(lower_bound(&[2, 4, 6], 5), 2);
    assert_eq!(lower_bound(&[2, 4, 6], 9), 3);
    assert_eq!(lower_bound(&[], 7), 0);
}
