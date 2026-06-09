use swe_binsearch_dup_bound::lower_bound;

// FAIL_TO_PASS: with duplicates, must return the FIRST occurrence.
#[test]
fn first_occurrence_with_duplicates() {
    assert_eq!(lower_bound(&[1, 5, 5, 5, 9], 5), 1);
}
