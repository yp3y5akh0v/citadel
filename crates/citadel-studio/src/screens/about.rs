use super::app_mark;
use crate::fonts::role;
use crate::state::{Action, State};
use crate::theme::{radius, Palette};
use egui::{Align, Frame, Layout, Margin, Modal, Pos2, Rect, RichText, Stroke, Ui, Vec2};

const WEBSITE: &str = env!("CARGO_PKG_HOMEPAGE");
const SOURCE: &str = env!("CARGO_PKG_REPOSITORY");
const LICENSE: &str = env!("CARGO_PKG_LICENSE");
const VERSION_LABEL: &str = concat!("Version ", env!("CARGO_PKG_VERSION"));

pub fn about(ui: &mut Ui, full: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    let outer_width = (full.width() - 48.0).clamp(340.0, 500.0);
    let modal = Modal::new(egui::Id::new("about_citadel_studio"))
        .frame(
            Frame::new()
                .fill(p.ground3)
                .stroke(Stroke::new(1.0, p.hairline_strong))
                .corner_radius(radius::CARD)
                .inner_margin(Margin::same(24)),
        )
        .backdrop_color(Palette::tint(p.ground0, 232))
        .show(ui.ctx(), |ui| {
            ui.set_width(outer_width - 48.0);
            ui.ctx().accesskit_node_builder(ui.unique_id(), |node| {
                node.set_role(egui::accesskit::Role::Dialog);
                node.set_label("About Citadel Studio");
                node.set_description("Version, copyright, license, and project links");
            });

            let (_, header) = ui.allocate_space(Vec2::new(ui.available_width(), 48.0));
            let mark = Rect::from_min_size(header.left_top(), Vec2::splat(48.0));
            app_mark(ui.painter(), mark.center(), 44.0, p.accent, p.ground3);
            let text_x = mark.right() + 10.0;
            let mut title = ui.new_child(
                egui::UiBuilder::new()
                    .max_rect(Rect::from_min_max(
                        Pos2::new(text_x, header.top() + 4.0),
                        header.right_bottom(),
                    ))
                    .layout(Layout::top_down(Align::LEFT)),
            );
            title.spacing_mut().item_spacing.y = 1.0;
            title.label(
                RichText::new("Citadel Studio")
                    .font(role::dialog_title())
                    .color(p.text1),
            );
            title.label(
                RichText::new(VERSION_LABEL)
                    .font(role::meta_mono())
                    .color(p.text2),
            );
            ui.add_space(16.0);
            ui.add(
                egui::Label::new(
                    RichText::new("Inspect, query, verify, and maintain encrypted vaults locally.")
                        .font(role::body())
                        .color(p.text2),
                )
                .wrap(),
            );
            ui.add_space(16.0);
            ui.label(
                RichText::new("Copyright © 2026 Yuriy Peysakhov")
                    .font(role::body())
                    .color(p.text1),
            );
            ui.horizontal_wrapped(|ui| {
                ui.label(
                    RichText::new("Licensed under")
                        .font(role::body())
                        .color(p.text2),
                );
                ui.hyperlink_to(LICENSE, format!("{SOURCE}/blob/HEAD/LICENSE-APACHE"));
            });
            ui.horizontal_wrapped(|ui| {
                ui.hyperlink_to("Website", WEBSITE);
                ui.label(RichText::new("·").color(p.text3));
                ui.hyperlink_to("Source code", SOURCE);
            });

            ui.add_space(16.0);
            let (_, actions) = ui.allocate_space(Vec2::new(ui.available_width(), 30.0));
            let close = ui.put(
                Rect::from_min_size(
                    Pos2::new(actions.right() - 96.0, actions.top()),
                    Vec2::new(96.0, 30.0),
                ),
                egui::Button::new("Close"),
            );
            (close.clicked(), close.id)
        });

    let focus_is_inside = ui
        .memory(|memory| memory.focused())
        .and_then(|id| ui.ctx().read_response(id))
        .is_some_and(|response| modal.response.rect.contains_rect(response.rect));
    if !focus_is_inside {
        ui.memory_mut(|memory| memory.request_focus(modal.inner.1));
    }

    if modal.inner.0 || modal.should_close() {
        out.push(Action::CloseAbout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn about_metadata_names_the_product_and_owner() {
        assert!(VERSION_LABEL.starts_with("Version "));
        assert_eq!(WEBSITE, "https://citadeldb.dev");
        assert!(SOURCE.starts_with("https://github.com/"));
        assert_eq!(LICENSE, "Apache-2.0");
    }
}
