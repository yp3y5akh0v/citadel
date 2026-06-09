use swe_off_by_one_window::window_maxes;

// FAIL_TO_PASS: the maximum of the final window is dropped by the bug.
#[test]
fn last_window_is_included() {
    assert_eq!(window_maxes(&[1, 3, 2, 5], 2), vec![3, 3, 5]);
}
