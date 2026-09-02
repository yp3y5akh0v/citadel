//! Window icon decoded to raw RGBA by `build.rs`, avoiding runtime PNG decoding.

/// Raw RGBA8, `SIDE` by `SIDE`, written by `build.rs`.
const RGBA: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/icon-rgba.bin"));

/// Side of the decoded icon, matching `assets/icon-256.png`.
pub const SIDE: u32 = 256;

/// The icon eframe hands to the window manager.
pub fn icon_data() -> egui::IconData {
    egui::IconData {
        rgba: RGBA.to_vec(),
        width: SIDE,
        height: SIDE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_decoded_icon_is_the_declared_size() {
        assert_eq!(RGBA.len(), (SIDE * SIDE * 4) as usize);
    }

    /// The tile is opaque to its edges: the icon is square and unmasked, so the platform
    /// applies its own corner rounding.
    #[test]
    fn the_tile_is_opaque_to_the_corners() {
        let at = |x: u32, y: u32| {
            let i = ((y * SIDE + x) * 4) as usize;
            [RGBA[i], RGBA[i + 1], RGBA[i + 2], RGBA[i + 3]]
        };
        for (name, px) in [
            ("top-left", at(0, 0)),
            ("top-right", at(SIDE - 1, 0)),
            ("bottom-left", at(0, SIDE - 1)),
            ("bottom-right", at(SIDE - 1, SIDE - 1)),
        ] {
            assert_eq!(
                px[3], 255,
                "{name} is transparent, so the icon is pre-masked"
            );
            assert_eq!([px[0], px[1], px[2]], TILE, "{name} is not the tile colour");
        }
    }

    /// `#1F7AA6` measures 4.78:1 on white and 3.41:1 on the Windows dark taskbar, which
    /// is what clears Microsoft's 3.0:1 floor on both themes.
    const TILE: [u8; 3] = [0x1F, 0x7A, 0xA6];

    #[test]
    fn the_tile_clears_the_contrast_floor_on_both_themes() {
        fn luminance(c: [u8; 3]) -> f64 {
            let ch = |v: u8| {
                let v = v as f64 / 255.0;
                if v <= 0.03928 {
                    v / 12.92
                } else {
                    ((v + 0.055) / 1.055).powf(2.4)
                }
            };
            0.2126 * ch(c[0]) + 0.7152 * ch(c[1]) + 0.0722 * ch(c[2])
        }
        let contrast = |a: [u8; 3], b: [u8; 3]| {
            let (x, y) = (luminance(a), luminance(b));
            (x.max(y) + 0.05) / (x.min(y) + 0.05)
        };
        for (desktop, ground) in [("white", [0xff; 3]), ("dark", [0x20; 3])] {
            let ratio = contrast(TILE, ground);
            assert!(
                ratio >= 3.0,
                "the tile reads at {ratio:.2}:1 on a {desktop} desktop, under the 3.0:1 floor"
            );
        }
    }
}
