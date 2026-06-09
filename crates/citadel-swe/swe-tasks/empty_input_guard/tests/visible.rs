use swe_empty_input_guard::mean;

// FAIL_TO_PASS: an empty slice must yield None, not Some(NaN).
#[test]
fn empty_is_none() {
    assert_eq!(mean(&[]), None);
}
