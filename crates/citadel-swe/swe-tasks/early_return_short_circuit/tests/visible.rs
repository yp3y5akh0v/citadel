use swe_early_return_short_circuit::first_index_gt;

// FAIL_TO_PASS: must return the FIRST matching index, not the last.
#[test]
fn returns_first_match() {
    assert_eq!(first_index_gt(&[1, 4, 2, 5], 3), Some(1));
}
