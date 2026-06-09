use swe_swapped_struct_fields::Rect;

// Held-out: both accessors on asymmetric rectangles.
#[test]
fn accessors_reflect_arguments() {
    assert_eq!(Rect::new(3, 4).height(), 4);
    assert_eq!(Rect::new(2, 7).width(), 2);
    assert_eq!(Rect::new(10, 1).height(), 1);
}
