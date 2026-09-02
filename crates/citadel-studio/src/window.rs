//! Caption, drag and resize interactions for the undecorated window.

use crate::theme::Palette;
use crate::ui;
use egui::{
    Color32, CornerRadius, CursorIcon, Id, Pos2, Rect, ResizeDirection, Sense, Stroke, StrokeKind,
    Ui, Vec2, ViewportCommand, WidgetInfo, WidgetType,
};

const CAPTION_W: f32 = 46.0;
/// Resize perimeter width, kept usable at high display scaling.
const GRIP: f32 = 6.0;

/// Total width the band must leave clear on its right for the caption buttons.
pub const CAPTION_STRIP_W: f32 = 3.0 * CAPTION_W;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Caption {
    Close,
    Max,
    Min,
}

impl Caption {
    fn name(self, maximized: bool) -> &'static str {
        match self {
            Caption::Close => "Close window",
            Caption::Max if maximized => "Restore window",
            Caption::Max => "Maximize window",
            Caption::Min => "Minimize window",
        }
    }

    fn command(self, maximized: bool) -> ViewportCommand {
        match self {
            Caption::Close => ViewportCommand::Close,
            Caption::Max => ViewportCommand::Maximized(!maximized),
            Caption::Min => ViewportCommand::Minimized(true),
        }
    }
}

/// Whether the backend reports the window maximized.
fn maximized(ui: &Ui) -> bool {
    ui.input(|i| i.viewport().maximized).unwrap_or(false)
}

/// Register the drag band before its interactive controls so they win hit testing.
pub fn drag_band(ui: &mut Ui, band: Rect) {
    // Pointer-only window dragging must not create a keyboard focus stop.
    let response = ui.interact(band, Id::new("window_drag"), Sense::CLICK | Sense::DRAG);
    if response.drag_started() {
        ui.ctx().send_viewport_cmd(ViewportCommand::StartDrag);
    }
    if response.double_clicked() {
        let next = !maximized(ui);
        ui.ctx().send_viewport_cmd(ViewportCommand::Maximized(next));
    }
}

/// Register caption buttons and resize border above screen content.
pub fn chrome(ui: &mut Ui, rect: Rect, band: Rect, p: &Palette) {
    // Disable perimeter grips while maximized so they cannot cover caption buttons.
    if !maximized(ui) {
        resize_border(ui, rect);
    }
    caption_buttons(ui, band, p);
}

fn caption_buttons(ui: &mut Ui, band: Rect, p: &Palette) {
    let max = maximized(ui);
    let y = band.center().y;
    let stroke = Stroke::new(1.1, p.text2);

    // Registration order is keyboard order: minimize, maximize, close.
    for (kind, r) in caption_rects(band) {
        let response = ui.interact(r, Id::new(("caption", kind.name(false))), Sense::click());
        if response.hovered() {
            let fill = match kind {
                Caption::Close => p.tampered.gamma_multiply(0.5),
                _ => p.wash(20),
            };
            ui.painter().rect_filled(r, 0.0, fill);
        }
        glyph(ui, Pos2::new(r.center().x, y), kind, max, stroke);
        if response.has_focus() {
            ui::focus_ring(ui, r, p, 0);
        }
        response.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, kind.name(max)));
        if response.clicked() {
            ui.ctx().send_viewport_cmd(kind.command(max));
        }
    }
}

fn caption_rects(band: Rect) -> [(Caption, Rect); 3] {
    let left = band.right() - CAPTION_STRIP_W;
    [Caption::Min, Caption::Max, Caption::Close].map(|kind| {
        let offset = match kind {
            Caption::Min => 0.0,
            Caption::Max => CAPTION_W,
            Caption::Close => CAPTION_W * 2.0,
        };
        (
            kind,
            Rect::from_min_size(
                Pos2::new(left + offset, band.top()),
                Vec2::new(CAPTION_W, band.height()),
            ),
        )
    })
}

fn glyph(ui: &Ui, c: Pos2, kind: Caption, maximized: bool, stroke: Stroke) {
    let painter = ui.painter();
    match kind {
        Caption::Close => {
            painter.line_segment(
                [
                    Pos2::new(c.x - 5.0, c.y - 5.0),
                    Pos2::new(c.x + 5.0, c.y + 5.0),
                ],
                stroke,
            );
            painter.line_segment(
                [
                    Pos2::new(c.x + 5.0, c.y - 5.0),
                    Pos2::new(c.x - 5.0, c.y + 5.0),
                ],
                stroke,
            );
        }
        Caption::Max if maximized => {
            let front = Rect::from_min_size(Pos2::new(c.x - 5.0, c.y - 3.0), Vec2::splat(8.0));
            painter.rect_stroke(front, CornerRadius::ZERO, stroke, StrokeKind::Inside);
            painter.line_segment(
                [
                    Pos2::new(c.x - 2.0, c.y - 5.0),
                    Pos2::new(c.x + 5.0, c.y - 5.0),
                ],
                stroke,
            );
            painter.line_segment(
                [
                    Pos2::new(c.x + 5.0, c.y - 5.0),
                    Pos2::new(c.x + 5.0, c.y + 2.0),
                ],
                stroke,
            );
        }
        Caption::Max => {
            painter.rect_stroke(
                Rect::from_center_size(c, Vec2::splat(10.0)),
                CornerRadius::ZERO,
                stroke,
                StrokeKind::Inside,
            );
        }
        Caption::Min => {
            painter.line_segment(
                [Pos2::new(c.x - 5.0, c.y), Pos2::new(c.x + 5.0, c.y)],
                stroke,
            );
        }
    }
}

fn resize_border(ui: &mut Ui, rect: Rect) {
    // Corner grips overlap edge strips and must win hit testing.
    let corner = Vec2::splat(GRIP * 2.0);
    let corners = [
        (rect.left_top(), ResizeDirection::NorthWest),
        (rect.right_top(), ResizeDirection::NorthEast),
        (rect.left_bottom(), ResizeDirection::SouthWest),
        (rect.right_bottom(), ResizeDirection::SouthEast),
    ];
    let edges = [
        (
            Rect::from_min_max(rect.left_top(), Pos2::new(rect.right(), rect.top() + GRIP)),
            ResizeDirection::North,
        ),
        (
            Rect::from_min_max(
                Pos2::new(rect.left(), rect.bottom() - GRIP),
                rect.right_bottom(),
            ),
            ResizeDirection::South,
        ),
        (
            Rect::from_min_max(
                rect.left_top(),
                Pos2::new(rect.left() + GRIP, rect.bottom()),
            ),
            ResizeDirection::West,
        ),
        (
            Rect::from_min_max(
                Pos2::new(rect.right() - GRIP, rect.top()),
                rect.right_bottom(),
            ),
            ResizeDirection::East,
        ),
    ];

    for (r, dir) in edges {
        grip(ui, r, dir);
    }
    for (at, dir) in corners {
        grip(ui, Rect::from_center_size(at, corner), dir);
    }
}

fn grip(ui: &mut Ui, r: Rect, dir: ResizeDirection) {
    // `Sense::DRAG` avoids the focusable flag that would expose unnamed, pointer-only
    // resize grips in the Tab order.
    let response = ui.interact(r, Id::new(("resize", dir as u8)), Sense::DRAG);
    if response.hovered() || response.dragged() {
        ui.ctx().set_cursor_icon(cursor(dir));
    }
    if response.drag_started() {
        ui.ctx()
            .send_viewport_cmd(ViewportCommand::BeginResize(dir));
    }
}

fn cursor(dir: ResizeDirection) -> CursorIcon {
    match dir {
        ResizeDirection::North => CursorIcon::ResizeNorth,
        ResizeDirection::South => CursorIcon::ResizeSouth,
        ResizeDirection::East => CursorIcon::ResizeEast,
        ResizeDirection::West => CursorIcon::ResizeWest,
        ResizeDirection::NorthEast => CursorIcon::ResizeNorthEast,
        ResizeDirection::SouthEast => CursorIcon::ResizeSouthEast,
        ResizeDirection::NorthWest => CursorIcon::ResizeNorthWest,
        ResizeDirection::SouthWest => CursorIcon::ResizeSouthWest,
    }
}

/// One-point boundary for the undecorated window.
pub fn outline(ui: &Ui, rect: Rect, colour: Color32) {
    ui.painter().rect_stroke(
        rect,
        CornerRadius::ZERO,
        Stroke::new(1.0, colour),
        StrokeKind::Inside,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caption_registration_follows_visual_order() {
        let band = Rect::from_min_size(Pos2::ZERO, Vec2::new(600.0, 42.0));
        let buttons = caption_rects(band);

        assert_eq!(
            buttons.map(|(kind, _)| kind),
            [Caption::Min, Caption::Max, Caption::Close]
        );
        assert!(buttons
            .windows(2)
            .all(|pair| pair[0].1.right() == pair[1].1.left()));
        assert_eq!(buttons[2].1.right(), band.right());
    }
}
