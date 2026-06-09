use swe_early_return_short_circuit::first_index_gt;

// Held-out: several matches, where the first differs from the last.
#[test]
fn first_of_many() {
    assert_eq!(first_index_gt(&[5, 1, 6, 2, 7], 3), Some(0));
    assert_eq!(first_index_gt(&[1, 4, 4, 4], 3), Some(1));
    assert_eq!(first_index_gt(&[10, 20], 5), Some(0));
}
