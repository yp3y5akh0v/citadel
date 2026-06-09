use swe_off_by_one_window::window_maxes;

// PASS_TO_PASS: adjacent behavior that already holds on the buggy crate, so a
// fix must not regress it (guards "gut the function" / "delete the assert").
#[test]
fn first_window_is_correct() {
    assert_eq!(window_maxes(&[5, 1, 1, 1], 2)[0], 5);
}

#[test]
fn degenerate_sizes_return_empty() {
    assert_eq!(window_maxes(&[1, 2, 3], 0), Vec::<i32>::new());
    assert_eq!(window_maxes(&[1], 3), Vec::<i32>::new());
    assert_eq!(window_maxes(&[], 1), Vec::<i32>::new());
}
