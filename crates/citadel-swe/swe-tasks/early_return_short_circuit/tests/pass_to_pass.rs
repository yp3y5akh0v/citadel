use swe_early_return_short_circuit::first_index_gt;

// PASS_TO_PASS: with zero or one match, first and last coincide.
#[test]
fn zero_or_one_match() {
    assert_eq!(first_index_gt(&[1, 2, 3], 5), None);
    assert_eq!(first_index_gt(&[1, 2, 9], 5), Some(2));
    assert_eq!(first_index_gt(&[9, 1, 2], 5), Some(0));
    assert_eq!(first_index_gt(&[], 0), None);
}
