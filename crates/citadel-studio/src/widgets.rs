//! Non-interactive painting primitives; callers own layout and hit testing.

use crate::fonts::role;
use crate::theme::{metrics, radius, Palette};
use egui::{Align2, Color32, CornerRadius, FontId, Painter, Pos2, Rect, Stroke, Vec2};

/// Joins related facts inside one field. A middle dot avoids colliding with the status
/// bar's pipe separators.
pub const SEP: &str = " \u{00b7} ";

/// Groups digits in threes for comparable counts.
pub fn thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

pub fn fill(painter: &Painter, rect: Rect, colour: Color32) {
    painter.rect_filled(rect, 0.0, colour);
}

/// A 1pt rule along one edge. Hairlines are the only separator this design uses.
pub fn hairline_bottom(painter: &Painter, rect: Rect, colour: Color32) {
    painter.rect_filled(
        Rect::from_min_max(
            Pos2::new(rect.left(), rect.bottom() - 1.0),
            rect.right_bottom(),
        ),
        0.0,
        colour,
    );
}

pub fn hairline_right(painter: &Painter, rect: Rect, colour: Color32) {
    painter.rect_filled(
        Rect::from_min_max(
            Pos2::new(rect.right() - 1.0, rect.top()),
            rect.right_bottom(),
        ),
        0.0,
        colour,
    );
}

pub fn hairline_top(painter: &Painter, rect: Rect, colour: Color32) {
    painter.rect_filled(
        Rect::from_min_max(rect.left_top(), Pos2::new(rect.right(), rect.top() + 1.0)),
        0.0,
        colour,
    );
}

/// Left-aligned text on a vertical centre line. Returns the advance width so callers can
/// lay a row out left to right without measuring twice.
pub fn text_left(painter: &Painter, pos: Pos2, s: &str, font: FontId, colour: Color32) -> f32 {
    let galley = painter.layout_no_wrap(s.to_owned(), font, colour);
    let w = galley.size().x;
    painter.galley(
        Pos2::new(pos.x, pos.y - galley.size().y * 0.5),
        galley,
        colour,
    );
    w
}

pub fn text_right(painter: &Painter, pos: Pos2, s: &str, font: FontId, colour: Color32) {
    painter.text(pos, Align2::RIGHT_CENTER, s, font, colour);
}

/// Lays out one elided line. Bisection over character boundaries bounds repeated text
/// layouts on grid hot paths.
pub fn elide_galley(
    painter: &Painter,
    text: &str,
    font: FontId,
    max_w: f32,
    colour: Color32,
) -> std::sync::Arc<egui::Galley> {
    let full = painter.layout_no_wrap(text.to_owned(), font.clone(), colour);
    if full.size().x <= max_w {
        return full;
    }

    // Byte index after each of the first k characters, so slicing never splits a char.
    let cuts: Vec<usize> = text
        .char_indices()
        .map(|(i, _)| i)
        .chain(std::iter::once(text.len()))
        .collect();
    let layout = |chars: usize| {
        painter.layout_no_wrap(format!("{}...", &text[..cuts[chars]]), font.clone(), colour)
    };
    if layout(0).size().x > max_w {
        return painter.layout_no_wrap(String::new(), font, colour);
    }

    // Largest character count whose elided form still fits. `lo` always fits.
    let (mut lo, mut hi) = (0usize, cuts.len() - 1);
    while lo < hi {
        let mid = (lo + hi).div_ceil(2);
        if layout(mid).size().x <= max_w {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    layout(lo)
}

/// Distance from a galley's top to its first baseline, used to align mixed faces/sizes.
pub fn baseline_offset(galley: &egui::Galley) -> f32 {
    galley
        .rows
        .first()
        .and_then(|placed| placed.row.glyphs.first().map(|g| placed.pos.y + g.pos.y))
        // Empty galleys draw nothing; return a finite conventional baseline.
        .unwrap_or(galley.size().y * 0.75)
}

/// Wraps to `max_w` and cuts to `max_h`. Bisection over character boundaries avoids
/// splitting UTF-8 and bounds layout attempts.
pub fn wrap_to_height(
    painter: &Painter,
    text: &str,
    font: FontId,
    max_w: f32,
    max_h: f32,
    colour: Color32,
) -> std::sync::Arc<egui::Galley> {
    let full = painter.layout(text.to_owned(), font.clone(), colour, max_w);
    if full.size().y <= max_h {
        return full;
    }

    let cuts: Vec<usize> = text
        .char_indices()
        .map(|(i, _)| i)
        .chain(std::iter::once(text.len()))
        .collect();
    let layout = |chars: usize| {
        painter.layout(
            format!("{}...", &text[..cuts[chars]]),
            font.clone(),
            colour,
            max_w,
        )
    };

    // Largest character count whose cut form still fits. `lo` always fits.
    let (mut lo, mut hi) = (0usize, cuts.len() - 1);
    while lo < hi {
        let mid = (lo + hi).div_ceil(2);
        if layout(mid).size().y <= max_h {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    layout(lo)
}

/// A rounded surface with an optional 1pt ring. Cards, popovers and inputs.
pub fn surface(painter: &Painter, rect: Rect, fill: Color32, ring: Option<Color32>, r: u8) {
    painter.rect_filled(rect, CornerRadius::same(r), fill);
    if let Some(c) = ring {
        painter.rect_stroke(
            rect,
            CornerRadius::same(r),
            Stroke::new(1.0, c),
            egui::StrokeKind::Inside,
        );
    }
}

/// A pill chip bounded by `max_w`, including padding. Fixed labels may pass
/// `f32::INFINITY`.
pub fn chip(
    painter: &Painter,
    origin: Pos2,
    label: &str,
    p: &Palette,
    text: Color32,
    bg: Option<Color32>,
    max_w: f32,
) -> f32 {
    let font = role::meta_mono();
    let galley = elide_galley(painter, label, font, (max_w - 12.0).max(0.0), text);
    let w = galley.size().x + 12.0;
    let rect = Rect::from_min_size(Pos2::new(origin.x, origin.y - 9.0), Vec2::new(w, 18.0));
    if let Some(bg) = bg {
        painter.rect_filled(rect, CornerRadius::same(radius::PILL), bg);
    } else {
        painter.rect_filled(rect, CornerRadius::same(radius::PILL), p.wash(15));
    }
    painter.galley(
        Pos2::new(rect.left() + 6.0, rect.center().y - galley.size().y * 0.5),
        galley,
        text,
    );
    w
}

/// Register painted status text with assistive technology.
pub fn semantic_status(
    ui: &mut egui::Ui,
    rect: Rect,
    id: impl std::hash::Hash + std::fmt::Debug,
    text: &str,
    alert: bool,
) {
    if text.is_empty() {
        return;
    }
    let response = ui.interact(
        rect,
        egui::Id::new(("semantic_status", id)),
        egui::Sense::hover(),
    );
    response
        .widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Label, true, text.to_owned()));
    response.ctx.accesskit_node_builder(response.id, |node| {
        node.set_role(if alert {
            egui::accesskit::Role::Alert
        } else {
            egui::accesskit::Role::Status
        });
        node.set_label(text);
    });
}

/// Status bar with the proof summary given layout priority.
pub fn status_bar(ui: &mut egui::Ui, rect: Rect, p: &Palette, left: &[&str], right: &[&str]) {
    /// Space between items, with the divider centred.
    const GAP: f32 = 20.0;
    const EDGE: f32 = 12.0;

    let status = left
        .iter()
        .chain(right)
        .copied()
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>()
        .join(", ");
    semantic_status(ui, rect, "application", &status, false);

    let painter = ui.painter();
    fill(painter, rect, p.ground3);
    let y = rect.center().y;
    let font = role::meta_mono();
    let measure = |s: &str| {
        painter
            .layout_no_wrap(s.to_owned(), font.clone(), p.text3)
            .size()
            .x
    };
    let run = |items: &[&str]| -> f32 {
        items.iter().copied().map(&measure).sum::<f32>()
            + GAP * items.len().saturating_sub(1) as f32
    };

    // Drop left-side details before allowing groups to overlap; proof stays visible.
    let room = rect.width() - EDGE * 2.0 - GAP;
    let mut lo = left.len();
    let mut hi = right.len();
    while lo + hi > 1 && run(&left[..lo]) + run(&right[..hi]) > room {
        if lo > 0 {
            lo -= 1;
        } else if hi > 1 {
            hi -= 1;
        }
    }
    let (left, right) = (&left[..lo], &right[..hi]);

    // At extreme widths even the highest-priority proof label may not fit. Elide it
    // inside the bar instead of letting it cross the window edge.
    if run(left) + run(right) > room {
        let item = right
            .first()
            .or_else(|| left.first())
            .copied()
            .unwrap_or("");
        let galley = elide_galley(painter, item, font, room.max(0.0), p.text3);
        painter.galley(
            Pos2::new(rect.left() + EDGE, y - galley.size().y * 0.5),
            galley,
            p.text3,
        );
        return;
    }

    let draw = |items: &[&str], mut x: f32| {
        for (i, s) in items.iter().enumerate() {
            if i > 0 {
                divider(painter, Pos2::new(x - GAP * 0.5, y), p);
            }
            x += text_left(painter, Pos2::new(x, y), s, font.clone(), p.text3) + GAP;
        }
    };

    draw(left, rect.left() + EDGE);
    // The right group is laid out from its own total so it ends flush with the edge.
    draw(right, rect.right() - EDGE - run(right));
}

fn divider(painter: &Painter, at: Pos2, p: &Palette) {
    painter.rect_filled(
        Rect::from_center_size(at, Vec2::new(1.0, 12.0)),
        0.0,
        p.hairline,
    );
}

/// A tracked uppercase sidebar heading, laid out as one galley.
pub fn section_header(painter: &Painter, rect: Rect, label: &str, p: &Palette) {
    let mut job = egui::text::LayoutJob::default();
    job.append(
        &label.to_uppercase(),
        0.0,
        egui::TextFormat {
            font_id: role::section_label(),
            color: p.text3,
            extra_letter_spacing: 0.6,
            ..Default::default()
        },
    );
    let galley = painter.layout_job(job);
    painter.galley(
        Pos2::new(
            rect.left() + metrics::PANE_PAD,
            rect.center().y - galley.size().y * 0.5,
        ),
        galley,
        p.text3,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elision_paints_nothing_when_even_the_ellipsis_cannot_fit() {
        let mut harness = egui_kittest::Harness::new_ui(|ui| {
            let font = egui::FontId::monospace(12.0);
            let dots = ui
                .painter()
                .layout_no_wrap("...".to_owned(), font.clone(), Color32::WHITE);
            let fitted = elide_galley(
                ui.painter(),
                "a value that needs elision",
                font,
                dots.size().x - 1.0,
                Color32::WHITE,
            );
            assert!(fitted.text().is_empty());
            assert_eq!(fitted.size().x, 0.0);
        });
        harness.run();
    }
}
