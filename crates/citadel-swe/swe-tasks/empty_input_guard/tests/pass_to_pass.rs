use swe_empty_input_guard::mean;

// PASS_TO_PASS: non-empty inputs already compute the mean on the buggy crate.
#[test]
fn non_empty_means() {
    assert_eq!(mean(&[2.0, 4.0, 6.0]), Some(4.0));
    assert_eq!(mean(&[5.0]), Some(5.0));
    assert_eq!(mean(&[-2.0, 2.0]), Some(0.0));
}
