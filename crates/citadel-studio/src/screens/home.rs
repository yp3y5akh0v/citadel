//! Vault manager shown when no document is open.

use super::*;
use crate::fonts::role;
use crate::rail;
use crate::state::{Action, State};
use crate::theme::{radius, Evidence, Palette};
use crate::ui;
use crate::widgets as w;
use egui::{Color32, Painter, Pos2, Rect, Ui, Vec2};

pub fn home(ui: &mut Ui, full: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    let s = Shell::new(full);
    w::fill(ui.painter(), full, p.ground1);
    band(ui, s.band, state, out);

    // Shell chrome is painted outside the clipped content column.
    let open = state
        .vault
        .as_ref()
        .map(|v| format!("{} open, no document", v.name));
    let mut right: Vec<&str> = state.renderer.iter().map(String::as_str).collect();
    right.push(super::BUILD_LABEL);
    w::status_bar(
        ui,
        s.status,
        p,
        &[open.as_deref().unwrap_or("no vault open")],
        &right,
    );

    let (col_w, x) = entry_column(s.body, 520.0);
    // Clip the fixed-height block to the body at undersized window dimensions.
    let ui = &mut ui.new_child(egui::UiBuilder::new().max_rect(s.body));
    ui.set_clip_rect(s.body);
    // Preserve a usable recent-vault viewport at the minimum window height while
    // keeping the quieter vertical lead-in on taller windows.
    let intro = ((s.body.height() - 420.0) * 0.35).clamp(24.0, 88.0);
    let mut y = s.body.top() + intro;

    // Align mark and name on one baseline.
    const MARK: f32 = 30.0;
    y += MARK * 0.5;
    app_mark(
        ui.painter(),
        Pos2::new(x + MARK * 0.5, y),
        MARK,
        p.accent,
        p.ground1,
    );
    w::text_left(
        ui.painter(),
        Pos2::new(x + MARK + 12.0, y),
        "Citadel Studio",
        role::headline(),
        p.text1,
    );
    y += MARK * 0.5 + 26.0;
    let sub = ui.painter().layout(
        "An encrypted vault holds tables, vectors, and an agent's memory. Studio reads \
         what is in one and proves what has happened to it."
            .to_owned(),
        role::body(),
        p.text2,
        col_w,
    );
    let sub_h = sub.size().y;
    ui.painter().galley(Pos2::new(x, y - 8.0), sub, p.text2);
    y += sub_h + 22.0;

    let card_w = (col_w - 12.0) * 0.5;
    for (i, (title, detail)) in [
        ("Open vault", "Choose an existing .cdl file"),
        ("Create vault", "Choose a passphrase and a KDF"),
    ]
    .into_iter()
    .enumerate()
    {
        let r = Rect::from_min_size(
            Pos2::new(x + i as f32 * (card_w + 12.0), y),
            Vec2::new(card_w, 108.0),
        );
        let response = ui.interact(r, egui::Id::new(("home_card", title)), egui::Sense::click());
        let fill = if response.hovered() {
            p.ground4
        } else {
            p.ground3
        };
        w::surface(ui.painter(), r, fill, Some(p.hairline_strong), radius::CARD);
        let ink = p.accent;
        let tile = Rect::from_min_size(
            Pos2::new(r.left() + 20.0, r.top() + 20.0),
            Vec2::new(40.0, 40.0),
        );
        ui.painter().rect_filled(
            tile,
            egui::CornerRadius::same(radius::CARD),
            Palette::tint(ink, 26),
        );
        if i == 0 {
            folder_glyph(ui.painter(), tile.center(), ink);
        } else {
            plus_glyph(ui.painter(), tile.center(), ink);
        }
        w::text_left(
            ui.painter(),
            Pos2::new(r.left() + 20.0, r.top() + 74.0),
            title,
            role::chrome_strong(),
            p.text1,
        );
        w::text_left(
            ui.painter(),
            Pos2::new(r.left() + 20.0, r.top() + 92.0),
            detail,
            role::meta(),
            p.text4,
        );
        if response.has_focus() {
            ui::focus_ring(ui, r, p, radius::CARD);
        }
        let name = title.to_owned();
        response.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, &name));
        if response.clicked() {
            if i == 0 {
                // The effect layer owns the dialog; dismissal has no state transition.
                out.push(Action::ChooseVaultToOpen);
            } else {
                out.push(Action::BeginCreate);
            }
        }
    }
    y += 108.0 + 14.0;

    for (id, label, key, action) in [
        (
            "import_sqlite",
            "Import SQLite schema",
            crate::state::shortcut("I"),
            Action::BeginImport,
        ),
        (
            "demo",
            "Open disposable demo",
            crate::state::shortcut("D"),
            Action::OpenDemoVault,
        ),
    ] {
        let r = Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, 36.0));
        let response = ui.interact(r, egui::Id::new(("home_row", id)), egui::Sense::click());
        if response.hovered() {
            ui.painter()
                .rect_filled(r, egui::CornerRadius::same(radius::PILL), p.wash(12));
        }
        w::hairline_bottom(ui.painter(), r, p.hairline);
        w::text_left(
            ui.painter(),
            Pos2::new(r.left() + 6.0, r.center().y),
            label,
            role::chrome(),
            if response.hovered() { p.text1 } else { p.text2 },
        );
        keycap(
            ui.painter(),
            Pos2::new(r.right() - 6.0, r.center().y),
            &key,
            p,
        );
        if response.has_focus() {
            ui::focus_ring(ui, r, p, radius::PILL);
        }
        let name = label.to_owned();
        response.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, &name));
        if response.clicked() {
            out.push(action);
        }
        y += 36.0;
    }

    // Show the current opening attempt's engine failure in place.
    if let Some(error) = state.opening_error() {
        let text = error.to_string();
        let galley = ui
            .painter()
            .layout(text.clone(), role::meta(), p.tampered, col_w);
        let h = galley.size().y;
        let at = Rect::from_min_size(Pos2::new(x, y + 8.0), Vec2::new(col_w, h));
        // Register diagnostics in the accessibility tree as well as painting them.
        ui.interact(at, egui::Id::new("home_error"), egui::Sense::hover())
            .widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Label, true, &text));
        ui.painter().galley(at.left_top(), galley, p.tampered);
        y += h + 12.0;
    }
    y += 22.0;

    w::section_header(
        ui.painter(),
        Rect::from_min_size(Pos2::new(x - 12.0, y - 8.0), Vec2::new(col_w, 20.0)),
        "Recent vaults",
        p,
    );
    y += 20.0;

    // Keep all recent entries reachable at the minimum window height instead of clipping
    // a fixed-height list against the status bar.
    let recent = Rect::from_min_max(
        Pos2::new(x, y),
        Pos2::new(x + col_w, (s.body.bottom() - 8.0).max(y)),
    );
    recent_vaults(ui, recent, state, out);
}

fn recent_vaults(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    let ui = &mut ui.new_child(egui::UiBuilder::new().max_rect(rect));
    ui.set_clip_rect(rect);
    egui::ScrollArea::vertical()
        .id_salt("recent_vaults")
        .auto_shrink([false, false])
        .show(ui, |ui| {
            ui.set_width(rect.width());
            for pf in &state.recent {
                let (_, r) = ui.allocate_space(Vec2::new(ui.available_width(), 40.0));
                let name = pf.name();
                let facts = if pf.entries == 0 {
                    format!("{} pages", w::thousands(pf.pages as u64))
                } else {
                    format!(
                        "{} key-value entries{}{} pages",
                        w::thousands(pf.entries),
                        w::SEP,
                        w::thousands(pf.pages as u64)
                    )
                };
                let path = pf.path.display().to_string();
                // Full paths provide stable identities for same-named vaults.
                let response =
                    ui.interact(r, egui::Id::new(("recent", &pf.path)), egui::Sense::click());
                if response.hovered() {
                    ui.painter()
                        .rect_filled(r, egui::CornerRadius::same(radius::PILL), p.wash(12));
                }
                rail::paint(
                    ui.painter(),
                    Rect::from_min_size(Pos2::new(r.left(), r.top() + 9.0), Vec2::new(3.0, 22.0)),
                    Evidence::Unverified,
                    p,
                    state.ppp,
                );
                let facts_w = ui
                    .painter()
                    .layout_no_wrap(facts.clone(), role::meta_mono(), p.text3)
                    .size()
                    .x;
                let room = (r.width() - 16.0 - 24.0 - facts_w - 16.0).max(40.0);
                let title = w::elide_galley(ui.painter(), &name, role::chrome(), room, p.text1);
                ui.painter().galley(
                    Pos2::new(r.left() + 16.0, r.center().y - 8.0 - title.size().y * 0.5),
                    title,
                    p.text1,
                );
                let line = w::elide_galley(ui.painter(), &path, role::meta_mono(), room, p.text3);
                ui.painter().galley(
                    Pos2::new(r.left() + 16.0, r.center().y + 8.0 - line.size().y * 0.5),
                    line,
                    p.text3,
                );
                w::text_right(
                    ui.painter(),
                    Pos2::new(r.right() - 24.0, r.center().y),
                    &facts,
                    role::meta_mono(),
                    p.text3,
                );
                if response.has_focus() {
                    ui::focus_ring(ui, r, p, radius::PILL);
                    response.scroll_to_me(Some(egui::Align::Center));
                }
                let label = format!("{name}, {facts}, not attested");
                response.widget_info(|| {
                    egui::WidgetInfo::labeled(egui::WidgetType::Button, true, &label)
                });
                if response.clicked() {
                    out.push(Action::OpenRecent(pf.path.clone()));
                }
            }

            let note = if state.recent.is_empty() {
                "Vaults you open are listed here."
            } else {
                "Counts are read from each vault's header. Nothing is attested until a vault is \
                 unlocked."
            };
            let galley =
                ui.painter()
                    .layout(note.to_owned(), role::meta(), p.text4, ui.available_width());
            let (_, note_rect) =
                ui.allocate_space(Vec2::new(ui.available_width(), galley.size().y + 12.0));
            ui.painter().galley(
                Pos2::new(note_rect.left(), note_rect.top() + 6.0),
                galley,
                p.text4,
            );
            w::semantic_status(ui, note_rect, "recent_vault_note", note, false);
        });
}

fn folder_glyph(painter: &Painter, c: Pos2, colour: Color32) {
    let stroke = egui::Stroke::new(1.5, colour);
    painter.rect_stroke(
        Rect::from_center_size(Pos2::new(c.x, c.y + 1.0), Vec2::new(18.0, 13.0)),
        egui::CornerRadius::same(2),
        stroke,
        egui::StrokeKind::Inside,
    );
    painter.line_segment(
        [
            Pos2::new(c.x - 9.0, c.y - 5.5),
            Pos2::new(c.x - 2.0, c.y - 5.5),
        ],
        stroke,
    );
}

fn keycap(painter: &Painter, right_centre: Pos2, key: &str, p: &Palette) {
    let galley = painter.layout_no_wrap(key.to_owned(), role::meta_mono(), p.text3);
    let width = galley.size().x + 12.0;
    let r = Rect::from_min_size(
        Pos2::new(right_centre.x - width, right_centre.y - 9.0),
        Vec2::new(width, 18.0),
    );
    painter.rect_stroke(
        r,
        egui::CornerRadius::same(radius::PILL),
        egui::Stroke::new(1.0, p.hairline),
        egui::StrokeKind::Inside,
    );
    painter.galley(
        Pos2::new(r.left() + 6.0, r.center().y - galley.size().y * 0.5),
        galley,
        p.text3,
    );
}
