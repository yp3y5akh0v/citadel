//! Accessible in-canvas command palette.

use crate::fonts::role;
use crate::state::{self, Action, State};
use crate::theme::radius;
use crate::widgets as w;
use egui::{Align, Align2, Frame, Margin, Modal, Pos2, Rect, Stroke, TextEdit, Ui, Vec2};

const WIDTH: f32 = 620.0;
const ROW_H: f32 = 44.0;
const MAX_ROWS: usize = 8;

pub fn palette(ui: &mut Ui, full: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    let commands = state::commands(state);
    let shown = commands.len().min(MAX_ROWS);
    let overflow = commands.len() > shown;
    let height = 56.0
        + shown as f32 * ROW_H
        + if shown == 0 { 44.0 } else { 12.0 }
        + if overflow { 26.0 } else { 0.0 };
    let first = if shown > 0 {
        state
            .palette_index
            .saturating_sub(shown - 1)
            .min(commands.len() - shown)
    } else {
        0
    };
    let size = Vec2::new(WIDTH, height);
    let desired_center = Pos2::new(
        full.center().x,
        full.top() + full.height() * 0.16 + height * 0.5,
    );
    let area = Modal::default_area(egui::Id::new("command_palette"))
        .anchor(Align2::CENTER_CENTER, desired_center - full.center());

    let modal = Modal::new(egui::Id::new("command_palette"))
        .area(area)
        .frame(Frame::NONE)
        .backdrop_color(egui::Color32::from_rgba_unmultiplied(0, 0, 0, 120))
        .show(ui.ctx(), |ui| {
            ui.set_min_size(size);
            let rect = Rect::from_min_size(ui.min_rect().min, size);
            ui.ctx().accesskit_node_builder(ui.unique_id(), |node| {
                node.set_role(egui::accesskit::Role::Dialog);
                node.set_label("Command palette");
            });

            ui.painter().rect_filled(
                rect.translate(egui::vec2(0.0, 12.0)).expand(6.0),
                egui::CornerRadius::same(radius::PALETTE),
                egui::Color32::from_rgba_unmultiplied(6, 10, 16, 150),
            );
            w::surface(
                ui.painter(),
                rect,
                p.ground4,
                Some(p.stroke),
                radius::PALETTE,
            );

            let query = Rect::from_min_size(rect.left_top(), Vec2::new(rect.width(), 56.0));
            let search_id = egui::Id::new("palette_search");
            let placeholder = if state.vault.is_some() {
                "Type a command or an object name"
            } else {
                "Type a command"
            };
            let mut edited = state.palette_query.clone();
            let search = ui.put(
                query,
                TextEdit::singleline(&mut edited)
                    .id(search_id)
                    .font(role::dialog_title())
                    .text_color(p.text1)
                    .hint_text(
                        egui::RichText::new(placeholder)
                            .font(role::dialog_title())
                            .color(p.text4),
                    )
                    .frame(Frame::NONE)
                    .margin(Margin::symmetric(20, 0))
                    .min_size(query.size())
                    .vertical_align(Align::Center),
            );
            w::hairline_bottom(ui.painter(), query, p.hairline);
            if search.has_focus() {
                ui.painter().rect_stroke(
                    query.shrink(3.0),
                    egui::CornerRadius::same(radius::CONTROL),
                    Stroke::new(2.0, p.focus),
                    egui::StrokeKind::Inside,
                );
            }
            search.widget_info(|| {
                egui::WidgetInfo::text_edit(
                    true,
                    &state.palette_query,
                    &state.palette_query,
                    placeholder,
                )
            });
            search.ctx.accesskit_node_builder(search.id, |node| {
                node.set_role(egui::accesskit::Role::SearchInput);
                node.set_label("Search commands and objects");
            });
            if edited != state.palette_query {
                out.extend(std::iter::repeat_n(
                    Action::PaletteBackspace,
                    state.palette_query.chars().count(),
                ));
                out.extend(edited.chars().map(Action::PaletteChar));
            }

            let focused = ui.memory(|memory| memory.focused());
            let focus_is_inside = focused == Some(search_id)
                || (first..first + shown)
                    .any(|index| focused == Some(egui::Id::new(("palette_row", index))));
            // Do not reclaim focus while Tab hands it to the next row.
            let traversing = ui.input(|input| input.key_pressed(egui::Key::Tab));
            if !focus_is_inside && !traversing {
                search.request_focus();
            }

            if shown == 0 {
                w::text_left(
                    ui.painter(),
                    Pos2::new(rect.left() + 20.0, query.bottom() + 22.0),
                    "No command matches. Esc to close.",
                    role::body(),
                    p.text3,
                );
                return;
            }

            let mut last_group = None;
            for (i, command) in commands.iter().skip(first).take(shown).enumerate() {
                let index = first + i;
                let r = Rect::from_min_size(
                    Pos2::new(rect.left() + 6.0, query.bottom() + 4.0 + i as f32 * ROW_H),
                    Vec2::new(rect.width() - 12.0, ROW_H),
                );
                let response = ui.interact(
                    r,
                    egui::Id::new(("palette_row", index)),
                    egui::Sense::click(),
                );
                let on = index == state.palette_index;
                if on {
                    ui.painter().rect_filled(
                        r,
                        egui::CornerRadius::same(radius::CONTROL),
                        p.wash(26),
                    );
                } else if response.hovered() {
                    ui.painter().rect_filled(
                        r,
                        egui::CornerRadius::same(radius::CONTROL),
                        p.wash(12),
                    );
                }
                if response.has_focus() {
                    ui.painter().rect_stroke(
                        r,
                        egui::CornerRadius::same(radius::CONTROL),
                        Stroke::new(2.0, p.focus),
                        egui::StrokeKind::Inside,
                    );
                    if !on && !response.clicked() {
                        out.push(Action::PaletteMove(
                            index as i32 - state.palette_index as i32,
                        ));
                    }
                }

                if last_group != Some(command.group) {
                    w::text_left(
                        ui.painter(),
                        Pos2::new(r.left() + 14.0, r.center().y - 9.0),
                        &command.group.label().to_uppercase(),
                        role::meta(),
                        p.text4,
                    );
                    last_group = Some(command.group);
                }
                let ink = if on { p.text1 } else { p.text2 };
                let hint_w = ui
                    .painter()
                    .layout_no_wrap(command.hint.clone(), role::meta_mono(), p.text4)
                    .size()
                    .x;
                let room = (r.width() - 14.0 - 14.0 - hint_w - 12.0).max(40.0);
                let title =
                    w::elide_galley(ui.painter(), &command.title, role::chrome(), room, ink);
                ui.painter().galley(
                    Pos2::new(r.left() + 14.0, r.center().y + 7.0 - title.size().y * 0.5),
                    title,
                    ink,
                );
                w::text_right(
                    ui.painter(),
                    Pos2::new(r.right() - 14.0, r.center().y),
                    &command.hint,
                    role::meta_mono(),
                    p.text4,
                );

                let name = if command.hint.is_empty() {
                    command.title.clone()
                } else {
                    format!("{}, {}", command.title, command.hint)
                };
                response.widget_info(|| {
                    egui::WidgetInfo::labeled(egui::WidgetType::Button, true, &name)
                });
                if response.clicked() {
                    out.push(Action::PaletteMove(
                        index as i32 - state.palette_index as i32,
                    ));
                    out.push(Action::PaletteRun);
                }
            }

            if overflow {
                w::text_left(
                    ui.painter(),
                    Pos2::new(rect.left() + 20.0, rect.bottom() - 15.0),
                    &format!("{} more. Keep typing to narrow.", commands.len() - shown),
                    role::meta(),
                    p.text4,
                );
            }
        });

    if modal.should_close() {
        out.push(Action::TogglePalette);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use egui_kittest::kittest::{NodeT, Queryable};
    use egui_kittest::Harness;

    struct Fixture {
        state: State,
        installed: bool,
    }

    fn harness() -> Harness<'static, Fixture> {
        Harness::builder()
            .with_size(egui::vec2(900.0, 700.0))
            .build_ui_state(
                |ui, fixture| {
                    if !fixture.installed {
                        crate::theme::install(ui.ctx(), &fixture.state.palette);
                        fixture.installed = true;
                        ui.ctx().request_repaint();
                        return;
                    }

                    let mut actions = Vec::new();
                    palette(ui, ui.max_rect(), &fixture.state, &mut actions);
                    for action in actions {
                        state::apply(&mut fixture.state, action);
                    }
                },
                Fixture {
                    state: State::default(),
                    installed: false,
                },
            )
    }

    #[test]
    fn modal_exposes_a_real_search_input_and_traps_focus() {
        let mut harness = harness();
        harness.run();

        let dialog =
            harness.get_by_role_and_label(egui::accesskit::Role::Dialog, "Command palette");
        let search = dialog.get_by_role_and_label(
            egui::accesskit::Role::SearchInput,
            "Search commands and objects",
        );
        assert!(search.is_focused(), "opening the palette must focus search");

        for node in std::iter::once(dialog).chain(dialog.children_recursive()) {
            let accesskit = node.accesskit_node();
            assert!(
                !accesskit
                    .data()
                    .supports_action(egui::accesskit::Action::Focus)
                    || accesskit
                        .label()
                        .is_some_and(|label| !label.trim().is_empty()),
                "the modal exposes an unnamed focus stop: {node:?}"
            );
        }

        search.type_text("demo");
        harness.run();
        assert_eq!(harness.state().state.palette_query, "demo");

        harness.key_press(egui::Key::Tab);
        harness.run();
        assert!(
            harness
                .query_all_by_role(egui::accesskit::Role::Button)
                .any(|node| node.is_focused()),
            "Tab must move focus to a named command row inside the modal"
        );
    }
}
