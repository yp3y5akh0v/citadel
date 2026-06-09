use swe_swapped_struct_fields::Rect;

// FAIL_TO_PASS: the width accessor must return the width argument.
#[test]
fn width_accessor() {
    assert_eq!(Rect::new(3, 4).width(), 3);
}
