use swe_empty_input_guard::mean;

// Held-out: the empty case must be None and must not leak a NaN.
#[test]
fn empty_is_none_not_nan() {
    assert_eq!(mean(&[]), None);
    assert!(mean(&[]).is_none());
}
