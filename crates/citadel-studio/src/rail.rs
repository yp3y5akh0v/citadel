//! A 3pt evidence rail. Pattern is the primary channel and colour reinforces it, keeping
//! states distinguishable when colour-vision deficiencies converge their hues.

use crate::theme::{metrics, Evidence, Palette};
use egui::{Color32, Painter, Rect};

/// Paint one rail into `rect`, whose width should be `metrics::RAIL_W`.
///
/// Snapped to physical pixels to avoid partially covered columns at fractional scaling.
pub fn paint(painter: &Painter, rect: Rect, ev: Evidence, p: &Palette, ppp: f32) {
    let Some(colour) = ev.colour(p) else {
        return; // NotAttestable draws nothing at all.
    };
    let snap = |v: f32| (v * ppp).round() / ppp;
    let x = snap(rect.left());
    let w = snap(metrics::RAIL_W);
    let top = snap(rect.top());
    let bot = snap(rect.bottom());
    let px = 1.0 / ppp;

    let bar = |y0: f32, y1: f32, c: Color32| {
        if y1 > y0 {
            painter.rect_filled(
                Rect::from_min_max(egui::pos2(x, y0), egui::pos2(x + w, y1)),
                0.0,
                c,
            );
        }
    };

    // Patterns vary along the vertical run; width-wise detail collapses at 3pt.
    let h = bot - top;
    match ev {
        Evidence::Verified => bar(top, bot, colour),
        // A broken seal, and the break is along the long axis so it reads at any width.
        Evidence::Tampered => {
            let gap = (h * 0.28).clamp(4.0 * px, 9.0 * px);
            let mid = top + h * 0.5;
            bar(top, mid - gap * 0.5, colour);
            bar(mid + gap * 0.5, bot, colour);
        }
        // 2 on, 2 off: an absence of any claim.
        Evidence::Unverified => {
            let mut y = top;
            while y < bot {
                bar(y, (y + 2.0 * px).min(bot), colour);
                y += 4.0 * px;
            }
        }
        // 1 on, 3 off: finer than Unverified so the two do not merge at a glance.
        Evidence::Erased => {
            let mut y = top;
            while y < bot {
                bar(y, (y + px).min(bot), colour);
                y += 4.0 * px;
            }
        }
        // A stub where a full rail should be, not a dash rhythm.
        Evidence::Missing => {
            let seg = (h * 0.3).clamp(6.0 * px, 10.0 * px);
            let mid = top + h * 0.5;
            bar(mid - seg * 0.5, mid + seg * 0.5, colour);
        }
        Evidence::NotAttestable => {}
    }
}

/// The glyph that carries the state when colour is unavailable, in a tooltip or a
/// screen-reader label.
pub fn glyph(ev: Evidence) -> &'static str {
    match ev {
        Evidence::Verified => "[+]",
        Evidence::Unverified => "[ ]",
        Evidence::Erased => "[/]",
        Evidence::Tampered => "[!]",
        Evidence::Missing => "[?]",
        Evidence::NotAttestable => "[-]",
    }
}

/// Verdict, optional verification time, and caller-supplied proof scope. `checked` applies
/// only to `Verified`; other states must not present it as their event time.
pub fn tooltip(ev: Evidence, checked: Option<&str>, scope: &str) -> String {
    let verdict = match (ev, checked) {
        (Evidence::Verified, Some(at)) => format!("{}, checked {at}", ev.label()),
        (Evidence::Verified, None) => format!("{} as stored, not checked this session", ev.label()),
        (Evidence::Unverified, _) => "No proof computed".to_owned(),
        (Evidence::Erased, _) => "Key destroyed; content is unrecoverable".to_owned(),
        (Evidence::Tampered, _) => "Authentication FAILED".to_owned(),
        (Evidence::Missing, _) => "Referenced object not found".to_owned(),
        (Evidence::NotAttestable, _) => ev.label().to_owned(),
    };
    // Glyph leads so the state survives greyscale.
    format!("{} {verdict}\n{scope}", glyph(ev))
}

/// Every evidence state, in the order they are presented to a reader.
pub const ALL: [Evidence; 6] = [
    Evidence::Verified,
    Evidence::Unverified,
    Evidence::Erased,
    Evidence::Tampered,
    Evidence::Missing,
    Evidence::NotAttestable,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unverified_is_the_default() {
        assert_eq!(Evidence::default(), Evidence::Unverified);
    }

    /// A plaintext region has no per-atom MAC, so any mark would be an unbacked claim.
    #[test]
    fn not_attestable_has_no_colour() {
        assert!(Evidence::NotAttestable.colour(&Palette::DARK).is_none());
    }
}
