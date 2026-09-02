//! Screens and shared chrome.

mod about;
mod create;
mod home;
mod import;
mod palette;
pub mod query;
pub mod security;
pub mod unlock;
pub mod vector;
mod workspace;

pub use about::about;
pub use create::create;
pub use home::home;
pub use import::import;
pub use palette::palette;
pub use unlock::unlock;
pub use workspace::workspace;

/// Studio build label sourced from this crate's package version.
pub(crate) const BUILD_LABEL: &str = concat!("Citadel Studio ", env!("CARGO_PKG_VERSION"));

use crate::fonts::role;
use crate::state::{self, Action, Doc, Route, State};
use crate::theme::{metrics, radius, Palette};
use crate::ui;
use crate::widgets as w;
use egui::{Color32, Painter, Pos2, Rect, Ui, Vec2};

/// The four horizontal bands every document screen has, resolved once.
pub struct Shell {
    pub band: Rect,
    pub tabs: Rect,
    pub body: Rect,
    pub status: Rect,
}

impl Shell {
    pub fn new(full: Rect) -> Self {
        let band = Rect::from_min_size(full.left_top(), Vec2::new(full.width(), metrics::BAND_H));
        let tabs = Rect::from_min_size(
            Pos2::new(full.left(), band.bottom()),
            Vec2::new(full.width(), metrics::TAB_H),
        );
        let status = Rect::from_min_size(
            Pos2::new(full.left(), full.bottom() - metrics::STATUS_H),
            Vec2::new(full.width(), metrics::STATUS_H),
        );
        let body = Rect::from_min_max(
            Pos2::new(full.left(), tabs.bottom()),
            Pos2::new(full.right(), status.top()),
        );
        Self {
            band,
            tabs,
            body,
            status,
        }
    }
}

/// The centred column an entry screen lays itself out in.
///
/// Clamped and centred within the body because platforms may violate the requested
/// minimum window size.
pub fn entry_column(body: Rect, want: f32) -> (f32, f32) {
    let w = want.min(body.width() - 48.0).max(200.0);
    (w, body.center().x - w * 0.5)
}

/// A circled plus drawn as strokes for size-independent alignment.
pub fn plus_glyph(painter: &Painter, c: Pos2, colour: Color32) {
    let stroke = egui::Stroke::new(1.6, colour);
    painter.circle_stroke(c, 8.5, stroke);
    painter.line_segment(
        [Pos2::new(c.x - 4.0, c.y), Pos2::new(c.x + 4.0, c.y)],
        stroke,
    );
    painter.line_segment(
        [Pos2::new(c.x, c.y - 4.0), Pos2::new(c.x, c.y + 4.0)],
        stroke,
    );
}

/// A disclosure chevron drawn as strokes for predictable small-size alignment.
pub fn caret_down(painter: &Painter, centre: Pos2, colour: Color32) {
    let s = 3.5;
    painter.add(egui::Shape::line(
        vec![
            Pos2::new(centre.x - s, centre.y - s * 0.5),
            Pos2::new(centre.x, centre.y + s * 0.5),
            Pos2::new(centre.x + s, centre.y - s * 0.5),
        ],
        egui::Stroke::new(1.3, colour),
    ));
}

/// The Citadel keep and keyhole, the same detailed mark the application icon carries.
///
/// The keyhole is painted in `ground` over the body rather than cut out of it, so the
/// mark must sit on a solid known colour.
pub fn app_mark(painter: &Painter, centre: Pos2, size: f32, ink: Color32, ground: Color32) {
    let u = size / 48.0;
    let o = centre - Vec2::splat(size * 0.5);
    let p = |x: f32, y: f32| Pos2::new(o.x + x * u, o.y + y * u);

    let mut crown = egui::Mesh::default();
    crown.add_colored_rect(Rect::from_min_max(p(7.5, 12.0), p(40.5, 18.5)), ink);
    crown.add_colored_rect(Rect::from_min_max(p(7.5, 8.0), p(14.0, 12.0)), ink);
    crown.add_colored_rect(Rect::from_min_max(p(20.0, 5.0), p(28.0, 12.0)), ink);
    crown.add_colored_rect(Rect::from_min_max(p(34.0, 8.0), p(40.5, 12.0)), ink);

    let base = crown.vertices.len() as u32;
    for point in [p(7.5, 18.5), p(10.5, 18.5), p(10.5, 21.0)] {
        crown.colored_vertex(point, ink);
    }
    crown.add_triangle(base, base + 1, base + 2);

    const CURVE_STEPS: u32 = 8;
    for step in 0..=CURVE_STEPS {
        let t = step as f32 / CURVE_STEPS as f32;
        let x = 10.5 + 27.0 * t;
        let y = (1.0 - t).powi(2) * 21.0 + 2.0 * (1.0 - t) * t * 16.6 + t.powi(2) * 21.0;
        crown.colored_vertex(p(x, 18.5), ink);
        crown.colored_vertex(p(x, y), ink);
    }
    let strip = base + 3;
    for step in 0..CURVE_STEPS {
        let top = strip + step * 2;
        crown.add_triangle(top, top + 1, top + 2);
        crown.add_triangle(top + 1, top + 3, top + 2);
    }

    let base = crown.vertices.len() as u32;
    for point in [p(37.5, 18.5), p(40.5, 18.5), p(37.5, 21.0)] {
        crown.colored_vertex(point, ink);
    }
    crown.add_triangle(base, base + 1, base + 2);
    painter.add(egui::Shape::mesh(crown));

    let mut shield = Vec::with_capacity(12);
    for step in 0..=CURVE_STEPS {
        let t = step as f32 / CURVE_STEPS as f32;
        let x = 12.0 + 24.0 * t;
        let y = (1.0 - t).powi(2) * 22.5 + 2.0 * (1.0 - t) * t * 18.5 + t.powi(2) * 22.5;
        shield.push(p(x, y));
    }
    shield.extend([p(33.8, 37.1), p(24.0, 43.0), p(14.2, 37.1)]);
    painter.add(egui::Shape::convex_polygon(shield, ink, egui::Stroke::NONE));

    painter.circle_filled(p(24.0, 26.95), 2.55 * u, ground);
    painter.add(egui::Shape::convex_polygon(
        vec![
            p(22.77, 29.25),
            p(25.23, 29.25),
            p(27.1, 34.8),
            p(20.9, 34.8),
        ],
        ground,
        egui::Stroke::NONE,
    ));
}

/// Space the band always keeps for the app mark and the vault picker, so the view
/// controls never squeeze out the one thing that says which vault is open.
const BAND_IDENTITY_W: f32 = 150.0;
const BAND_MENU_W: f32 = 138.0;
const BAND_MARK_SIZE: f32 = 20.0;
const BAND_MARK_GAP: f32 = 12.0;

/// Shared band for the frameless window.
///
/// With no vault open the picker, the sensitivity chip and the view controls have nothing
/// to act on, so they are absent rather than disabled.
pub fn band(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    w::fill(ui.painter(), rect, p.ground3);
    w::hairline_bottom(ui.painter(), rect, p.hairline);
    let y = rect.center().y;

    let mut x = rect.left() + 14.0;
    app_mark(
        ui.painter(),
        Pos2::new(x + BAND_MARK_SIZE * 0.5, y),
        BAND_MARK_SIZE,
        p.accent,
        p.ground3,
    );
    x += BAND_MARK_SIZE + BAND_MARK_GAP;

    let menu = band_menu_rect(rect, x);
    application_menu(ui, menu, state, out);
    x = menu.right() + 12.0;

    match &state.vault {
        Some(vault) => {
            // Elide within the space left by view and caption controls.
            let limit = view_controls(ui, rect, state, out, x) - 10.0;
            let room = (limit - x).max(0.0);

            let name = w::elide_galley(
                ui.painter(),
                &vault.name,
                role::chrome(),
                (room - 44.0).max(0.0),
                p.text1,
            );
            let picker = Rect::from_min_size(
                Pos2::new(x - 6.0, y - 14.0),
                Vec2::new((name.size().x + 30.0).min(room), 28.0),
            );
            let response = ui.interact(picker, egui::Id::new("vault_picker"), egui::Sense::click());
            if response.hovered() {
                ui.painter().rect_filled(
                    picker,
                    egui::CornerRadius::same(radius::CARD),
                    p.wash(20),
                );
            }
            ui.painter()
                .galley(Pos2::new(x, y - name.size().y * 0.5), name, p.text1);
            if response.has_focus() {
                ui::focus_ring(ui, picker, p, radius::CARD);
            }
            let accessible_name = format!("Close vault {}", vault.name);
            response.widget_info(|| {
                egui::WidgetInfo::labeled(egui::WidgetType::Button, true, &accessible_name)
            });
            let close_hint = format!("Close vault  {}", crate::state::shortcut("Shift W"));
            if response.on_hover_text(close_hint).clicked() {
                out.push(Action::CloseVault);
            }
        }
        None => {
            w::text_left(
                ui.painter(),
                Pos2::new(x, y),
                "Citadel Studio",
                role::chrome(),
                p.text3,
            );
        }
    }
}

fn band_menu_rect(band: Rect, left: f32) -> Rect {
    Rect::from_min_size(
        Pos2::new(left - 4.0, band.center().y - 14.0),
        Vec2::new(BAND_MENU_W, 28.0),
    )
}

fn application_menu(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    let can_enter = state.route == Route::Home && state.foreground_blocker().is_none();
    let can_import = matches!(state.route, Route::Home | Route::Workspace)
        && state.foreground_blocker().is_none();
    let can_use_vault = state.vault_navigation_available();
    let can_close = state.close_vault_blocker().is_none();
    let workspace_shortcuts = state.route == Route::Workspace;
    let mut menu_ui = ui.new_child(egui::UiBuilder::new().max_rect(rect));
    menu_ui.set_clip_rect(rect);
    egui::MenuBar::new().ui(&mut menu_ui, |ui| {
        ui.menu_button("File", |ui| {
            menu_item(ui, "Open vault…", can_enter, Action::ChooseVaultToOpen, out);
            menu_item(ui, "Create vault…", can_enter, Action::BeginCreate, out);
            let import_label = if can_enter {
                format!("Import SQLite schema…\t{}", state::shortcut("I"))
            } else {
                "Import SQLite schema…".to_owned()
            };
            menu_item(ui, &import_label, can_import, Action::BeginImport, out);
            let demo_label = if can_enter {
                format!("Open disposable demo\t{}", state::shortcut("D"))
            } else {
                "Open disposable demo".to_owned()
            };
            menu_item(ui, &demo_label, can_enter, Action::OpenDemoVault, out);
            ui.separator();
            menu_item(
                ui,
                &format!("Close vault\t{}", state::shortcut("Shift W")),
                can_close,
                Action::CloseVault,
                out,
            );
            ui.separator();
            menu_item(ui, "Exit Citadel Studio", true, Action::RequestExit, out);
        });
        ui.menu_button("View", |ui| {
            menu_item(
                ui,
                &format!("Command palette\t{}", state::shortcut("K")),
                true,
                Action::TogglePalette,
                out,
            );
            let query = if workspace_shortcuts {
                format!("Query editor\t{}", state::shortcut("N"))
            } else {
                "Query editor".to_owned()
            };
            menu_item(ui, &query, can_use_vault, Action::OpenDoc(Doc::Query), out);
            let security = if workspace_shortcuts {
                format!("Security\t{}", state::shortcut("I"))
            } else {
                "Security".to_owned()
            };
            menu_item(
                ui,
                &security,
                can_use_vault,
                Action::OpenDoc(Doc::Security),
                out,
            );
            ui.separator();
            let theme = if state.light {
                "Use dark theme"
            } else {
                "Use light theme"
            };
            let theme_label = if workspace_shortcuts {
                format!("{theme}\t{}", state::shortcut("L"))
            } else {
                theme.to_owned()
            };
            menu_item(ui, &theme_label, true, Action::ToggleTheme, out);
            let density = format!("Density: {}", state.density.label());
            let density_label = if workspace_shortcuts {
                format!("{density}\t{}", state::shortcut("D"))
            } else {
                density
            };
            menu_item(ui, &density_label, true, Action::CycleDensity, out);
        });
        ui.menu_button("Help", |ui| {
            menu_item(ui, "About Citadel Studio", true, Action::ShowAbout, out);
        });
    });
}

fn menu_item(ui: &mut Ui, label: &str, enabled: bool, action: Action, out: &mut Vec<Action>) {
    if ui.add_enabled(enabled, egui::Button::new(label)).clicked() {
        out.push(action);
        ui.close();
    }
}

/// Theme and density, right-aligned before the caption buttons the shell paints.
///
/// Returns the left edge they claimed, so what the band draws from the left knows where
/// it has to stop.
fn view_controls(
    ui: &mut Ui,
    rect: Rect,
    state: &State,
    out: &mut Vec<Action>,
    identity_start: f32,
) -> f32 {
    let p = &state.palette;
    let y = rect.center().y;
    let mut cx = rect.right() - crate::window::CAPTION_STRIP_W - 12.0;
    // Stable ids preserve keyboard focus when captions change.
    // Drop view controls before obscuring the vault identity.
    let floor = identity_start + BAND_IDENTITY_W;
    for (label, key, action) in [
        (
            if state.light { "Dark" } else { "Light" },
            "theme",
            Action::ToggleTheme,
        ),
        (state.density.label(), "density", Action::CycleDensity),
    ] {
        if cx - 78.0 < floor {
            break;
        }
        cx -= 78.0;
        let r = Rect::from_min_size(Pos2::new(cx, y - 13.0), Vec2::new(72.0, 26.0));
        let btn = ui::Btn::new(label, ui::ButtonKind::Ghost).keyed(key);
        if ui::button(ui, r, btn, p).clicked() {
            out.push(action);
        }
    }
    cx
}

/// One key/value row with an optional semantic value colour.
pub struct Field<'a> {
    pub label: &'a str,
    pub value: &'a str,
    /// Overrides the primary text ramp when the value carries a verdict of its own.
    pub colour: Option<Color32>,
}

impl<'a> Field<'a> {
    pub fn new(label: &'a str, value: &'a str) -> Self {
        Self {
            label,
            value,
            colour: None,
        }
    }

    pub fn tinted(label: &'a str, value: &'a str, colour: Color32) -> Self {
        Self {
            label,
            value,
            colour: Some(colour),
        }
    }
}

/// A fixed-order key/value list that elides values to its available width.
pub fn field_list(
    painter: &Painter,
    origin: Pos2,
    fields: &[Field<'_>],
    width: f32,
    p: &Palette,
) -> f32 {
    /// Minimum value-column offset.
    const VALUE_X: f32 = 110.0;
    const GAP: f32 = 12.0;

    // Measure labels so their values cannot overlap them.
    let value_x = fields
        .iter()
        .map(|f| {
            painter
                .layout_no_wrap(f.label.to_uppercase(), role::meta(), p.text3)
                .size()
                .x
                + GAP
        })
        .fold(VALUE_X, f32::max);

    let mut y = origin.y;
    for field in fields {
        let colour = match field.colour {
            Some(c) => c,
            None => p.text1,
        };
        let label = painter.layout_no_wrap(field.label.to_uppercase(), role::meta(), p.text3);
        let value = w::elide_galley(
            painter,
            field.value,
            role::cell_compact(),
            (width - value_x).max(40.0),
            colour,
        );

        // Different faces align by baseline rather than galley centre.
        let label_top = y - label.size().y * 0.5;
        let baseline = label_top + w::baseline_offset(&label);
        let value_top = baseline - w::baseline_offset(&value);
        painter.galley(Pos2::new(origin.x, label_top), label, p.text3);
        painter.galley(Pos2::new(origin.x + value_x, value_top), value, colour);
        y += 19.0;
    }
    y
}

/// A titled card. The only surface in the app that carries a ring.
pub fn card(painter: &Painter, rect: Rect, title: &str, p: &Palette) {
    w::surface(
        painter,
        rect,
        p.ground3,
        Some(p.hairline_strong),
        radius::CARD,
    );
    w::text_left(
        painter,
        Pos2::new(rect.left() + 16.0, rect.top() + 18.0),
        &title.to_uppercase(),
        role::section_label(),
        p.text3,
    );
}

/// Paint a toolbar strip. The caller already owns the rect, so nothing is returned.
pub fn paint_toolbar(painter: &Painter, rect: Rect, p: &Palette) {
    w::fill(painter, rect, p.ground3);
    w::hairline_bottom(painter, rect, p.hairline);
}

/// A form's cancel-and-confirm row, laid out so it stays inside its column.
///
/// Returns `(cancel, primary)`.
pub fn form_buttons(x: f32, y: f32, col_w: f32) -> (Rect, Rect) {
    const GAP: f32 = 9.0;
    const H: f32 = 32.0;
    // Preserve the primary action width before shrinking Cancel.
    let primary_w = 160.0_f32.min((col_w - GAP) * 0.6);
    let cancel_w = 120.0_f32.min((col_w - GAP - primary_w).max(0.0));
    let primary = Rect::from_min_size(Pos2::new(x + col_w - primary_w, y), Vec2::new(primary_w, H));
    let cancel = Rect::from_min_size(
        Pos2::new(primary.left() - GAP - cancel_w, y),
        Vec2::new(cancel_w, H),
    );
    (cancel, primary)
}

/// Explicit inspector empty state.
pub fn inspector_empty(painter: &Painter, rect: Rect, kind: &str, why: &str, p: &Palette) {
    let head = inspector_header(painter, rect, kind, "", p);
    let note = painter.layout(
        why.to_owned(),
        role::meta(),
        p.text4,
        rect.width() - metrics::PANE_PAD * 2.0,
    );
    painter.galley(
        Pos2::new(rect.left() + metrics::PANE_PAD, head.bottom() + 20.0),
        note,
        p.text4,
    );
}

pub fn inspector_header(painter: &Painter, rect: Rect, kind: &str, id: &str, p: &Palette) -> Rect {
    let head = Rect::from_min_size(rect.left_top(), Vec2::new(rect.width(), metrics::TOOLBAR_H));
    w::fill(painter, head, p.ground3);
    w::hairline_bottom(painter, head, p.hairline);
    let mut x = rect.left() + metrics::PANE_PAD;
    x += w::text_left(
        painter,
        Pos2::new(x, head.center().y),
        kind,
        role::section(),
        p.text1,
    );
    if !id.is_empty() {
        x += 6.0;
        w::text_left(
            painter,
            Pos2::new(x, head.center().y),
            id,
            role::cell_compact(),
            p.text1,
        );
    }
    head
}

/// A right-hand inspector pane with its left hairline.
pub fn inspector(painter: &Painter, body: Rect, p: &Palette) -> Rect {
    let rect = Rect::from_min_size(
        Pos2::new(body.right() - metrics::INSPECTOR_W, body.top()),
        Vec2::new(metrics::INSPECTOR_W, body.height()),
    );
    w::fill(painter, rect, p.ground2);
    painter.rect_filled(
        Rect::from_min_max(rect.left_top(), Pos2::new(rect.left() + 1.0, rect.bottom())),
        0.0,
        p.hairline,
    );
    rect
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimum_width_keeps_menu_identity_view_and_caption_zones_disjoint() {
        let band = Rect::from_min_size(Pos2::ZERO, Vec2::new(900.0, metrics::BAND_H));
        let menu = band_menu_rect(band, 14.0 + BAND_MARK_SIZE + BAND_MARK_GAP);
        let identity_left = menu.right() + 12.0;
        let two_view_controls_left =
            band.right() - crate::window::CAPTION_STRIP_W - 12.0 - 2.0 * 78.0;

        assert!(menu.right() < identity_left);
        assert!(identity_left + BAND_IDENTITY_W <= two_view_controls_left);
        assert!(two_view_controls_left < band.right() - crate::window::CAPTION_STRIP_W);
    }
}
