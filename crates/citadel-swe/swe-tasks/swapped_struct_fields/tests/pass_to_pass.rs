use swe_swapped_struct_fields::Rect;

// PASS_TO_PASS: symmetric quantities are unaffected by the field swap.
#[test]
fn symmetric_methods() {
    assert_eq!(Rect::new(3, 4).area(), 12);
    assert_eq!(Rect::new(3, 4).perimeter(), 14);
    assert_eq!(Rect::new(5, 5).area(), 25);
}
