//! A rectangle with a width and a height.

pub struct Rect {
    width: u32,
    height: u32,
}

impl Rect {
    /// Create a rectangle with the given width and height.
    pub fn new(width: u32, height: u32) -> Self {
        Rect { width, height }
    }

    /// The rectangle's width.
    pub fn width(&self) -> u32 {
        self.width
    }

    /// The rectangle's height.
    pub fn height(&self) -> u32 {
        self.height
    }

    /// Area: width times height.
    pub fn area(&self) -> u32 {
        self.width * self.height
    }

    /// Perimeter: twice the sum of width and height.
    pub fn perimeter(&self) -> u32 {
        2 * (self.width + self.height)
    }
}
