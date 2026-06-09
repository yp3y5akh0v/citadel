use swe_off_by_one_window::window_maxes;

// Held-out grading tests (the agent never sees these). Stronger cases that a
// fix special-cased only to the visible input would still fail.
#[test]
fn max_lives_only_in_final_window() {
    assert_eq!(window_maxes(&[1, 1, 1, 9], 2), vec![1, 1, 9]);
}

#[test]
fn longer_array_all_windows() {
    assert_eq!(window_maxes(&[2, 1, 4, 1, 5], 3), vec![4, 4, 5]);
}

#[test]
fn window_equal_to_length_is_one_window() {
    assert_eq!(window_maxes(&[3, 7, 2], 3), vec![7]);
}
