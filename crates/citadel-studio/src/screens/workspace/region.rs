//! Memory-region grid and selected-atom inspector.

use super::super::*;
use crate::engine::memory::AtomView;
use crate::fonts::role;
use crate::grid::{self, Align, Cell, Column, Row};
use crate::rail;
use crate::state::{Action, GridTab, Maintenance, State};
use crate::theme::{metrics, Evidence};
use crate::ui;
use crate::widgets as w;
use egui::{Frame, Id, Margin, Modal, Pos2, Rect, RichText, Sense, Stroke, StrokeKind, Ui, Vec2};

pub(super) fn region_pane(
    ui: &mut Ui,
    centre: Rect,
    state: &State,
    name: &str,
    show_forget_fallback: bool,
    out: &mut Vec<Action>,
) {
    let p = &state.palette;
    let vault = state.vault();
    let Some(region) = vault.region(name) else {
        return;
    };

    let bar = Rect::from_min_size(
        centre.left_top(),
        Vec2::new(centre.width(), metrics::TOOLBAR_H),
    );
    paint_toolbar(ui.painter(), bar, p);
    // Actions claim space first; drop the descriptive chip before overlapping them.
    let verify = Rect::from_min_size(
        Pos2::new(bar.right() - 118.0, bar.top() + 3.0),
        Vec2::new(106.0, 26.0),
    );
    let verify_label = if state.maintenance == Maintenance::Verifying {
        "Verifying…"
    } else {
        "Verify rows"
    };
    let btn = ui::Btn::new(verify_label, ui::ButtonKind::Ghost).keyed("Verify visible rows");
    let btn = if let Some(why) = state.verify_rows_blocker() {
        btn.unavailable(why)
    } else {
        btn.tip("Authenticates only the atom rows visible on screen")
    };
    if ui::button(ui, verify, btn, p).clicked() {
        out.push(Action::VerifyPage);
    }
    let action_left = if show_forget_fallback {
        let forget = Rect::from_min_size(
            Pos2::new(bar.right() - 118.0 - 108.0, bar.top() + 3.0),
            Vec2::new(102.0, 26.0),
        );
        let plaintext = region.facts.plaintext;
        let forget_label = if state.maintenance == Maintenance::Forgetting {
            "Forgetting…"
        } else {
            "Forget atom"
        };
        let btn = ui::Btn::new(forget_label, ui::ButtonKind::Danger).keyed("Forget atom");
        let btn = if let Some(why) = state.forget_row_blocker() {
            btn.unavailable(why)
        } else if plaintext {
            btn.tip("Deletes the selected atom. No key exists in this plaintext region.")
        } else {
            btn.tip("Destroys the key for the selected atom")
        };
        if ui::button(ui, forget, btn, p).clicked() {
            out.push(Action::BeginForgetSelected);
        }
        forget.left()
    } else {
        verify.left()
    };

    // Elide the region name within the space left by actions.
    let mut tx = bar.left() + 12.0;
    let title = w::elide_galley(
        ui.painter(),
        name,
        role::chrome(),
        (action_left - 12.0 - tx).max(0.0),
        p.text1,
    );
    let title_w = title.size().x;
    ui.painter().galley(
        Pos2::new(tx, bar.center().y - title.size().y * 0.5),
        title,
        p.text1,
    );
    tx += title_w + 10.0;
    let chip_room = action_left - 12.0 - tx;
    if chip_room > 130.0 {
        let chip = format!(
            "{}d {} {}",
            region.facts.dim,
            region.facts.metric_label(),
            region.facts.model
        );
        let fitted = w::elide_galley(
            ui.painter(),
            &chip,
            role::meta_mono(),
            chip_room - 14.0,
            p.text3,
        );
        w::chip(
            ui.painter(),
            Pos2::new(tx, bar.center().y),
            fitted.text(),
            p,
            p.text3,
            None,
            chip_room,
        );
    }

    let notice = maintenance_notice(state, name);
    let notice_h = if notice.is_some() { 34.0 } else { 0.0 };
    if let Some((message, tone)) = notice {
        paint_maintenance_notice(
            ui,
            Rect::from_min_size(
                Pos2::new(centre.left(), bar.bottom()),
                Vec2::new(centre.width(), notice_h),
            ),
            name,
            &message,
            tone,
            p,
        );
    }

    let action = Rect::from_min_size(
        Pos2::new(centre.left(), centre.bottom() - 30.0),
        Vec2::new(centre.width(), 30.0),
    );
    let body = Rect::from_min_max(
        Pos2::new(centre.left(), bar.bottom() + notice_h),
        Pos2::new(centre.right(), action.top()),
    );

    match state.grid_tab {
        GridTab::Data | GridTab::Attestation => {
            let columns = data_columns(state.grid_tab);
            let rows: Vec<Row> = region
                .atoms
                .iter()
                .map(|a| {
                    Row::new(a.evidence(), cells_for(a, state.grid_tab, p)).tooltip(rail::tooltip(
                        a.evidence(),
                        a.verified_at.as_deref(),
                        a.evidence().scope(),
                    ))
                })
                .collect();
            let event = grid::show(ui, body, &columns, &rows, state.scroll(), p);
            if let Some(i) = event.clicked.or(event.selected_to) {
                out.push(Action::SelectRow(i));
            }
            out.push(Action::ReportWindow {
                visible: event.visible,
                total: rows.len(),
            });
            if let Some(i) = event.scrolled_to {
                out.push(Action::ScrollTo(i));
            }
        }
        GridTab::Structure => structure_pane(ui.painter(), body, region, p),
    }

    w::fill(ui.painter(), action, p.ground3);
    w::hairline_top(ui.painter(), action, p.hairline);
    let active = match state.grid_tab {
        GridTab::Data => 0,
        GridTab::Structure => 1,
        GridTab::Attestation => 2,
    };
    const TABS: [&str; 3] = ["Data", "Structure", "Attestation"];
    if let Some(i) = ui::segmented(
        ui,
        "grid_tab",
        Pos2::new(action.left() + 10.0, action.center().y),
        &TABS,
        active,
        p,
    ) {
        out.push(Action::SetGridTab(match i {
            0 => GridTab::Data,
            1 => GridTab::Structure,
            _ => GridTab::Attestation,
        }));
    }
    // Distinguish the loaded page from the region's total atom count.
    let read = region.atoms.len() as u64;
    let total = region
        .facts
        .total
        .map(w::thousands)
        .unwrap_or_else(|| "unknown".to_owned());
    let counted = match state.grid_tab {
        GridTab::Structure => format!("{total} atoms in this region"),
        _ => format!("showing {} of {} atoms", w::thousands(read), total),
    };
    let tabs_w = ui::segmented_width(ui.painter(), &TABS, p);
    let count_w = ui
        .painter()
        .layout_no_wrap(counted.clone(), role::meta_mono(), p.text3)
        .size()
        .x;
    if action.width() >= 22.0 + tabs_w + count_w + 20.0 {
        w::text_right(
            ui.painter(),
            Pos2::new(action.right() - 12.0, action.center().y),
            &counted,
            role::meta_mono(),
            p.text3,
        );
    }
}

fn maintenance_notice(state: &State, region: &str) -> Option<(String, egui::Color32)> {
    let p = &state.palette;
    let owns_feedback = state.maintenance_region.as_deref() == Some(region);
    if owns_feedback {
        if let Some(error) = &state.maintenance_error {
            return Some((format!("Maintenance failed: {error}"), p.tampered));
        }
    }
    match (state.maintenance, owns_feedback) {
        (Maintenance::Verifying, true) => {
            return Some(("Authenticating visible rows…".to_owned(), p.accent));
        }
        (Maintenance::Forgetting, true) => {
            return Some(("Forgetting the selected atom…".to_owned(), p.missing));
        }
        _ => {}
    }
    if state
        .vault()
        .region(region)
        .is_some_and(|visible| visible.facts.plaintext)
    {
        return Some((
            "Plaintext region: per-row authentication is unavailable because no per-atom MAC exists. Forget atom deletes data instead of destroying a key."
                .to_owned(),
            p.text3,
        ));
    }
    if state.page_check_belongs_to_visible_window() && state.page_checked.scope > 0 {
        let tone = if state.page_checked.tampered + state.page_checked.missing > 0 {
            p.tampered
        } else if state.page_checked.erased > 0 {
            p.erased
        } else if state.page_checked.unattestable > 0 {
            p.missing
        } else {
            p.verified
        };
        return Some((state.page_check_label(), tone));
    }
    None
}

fn paint_maintenance_notice(
    ui: &mut Ui,
    rect: Rect,
    region: &str,
    message: &str,
    tone: egui::Color32,
    p: &crate::theme::Palette,
) {
    w::fill(ui.painter(), rect, p.ground2);
    w::hairline_bottom(ui.painter(), rect, p.hairline);
    ui.painter().rect_filled(
        Rect::from_min_size(rect.left_top(), Vec2::new(3.0, rect.height())),
        0.0,
        tone,
    );
    let galley = w::elide_galley(
        ui.painter(),
        message,
        role::meta(),
        (rect.width() - 28.0).max(0.0),
        p.text2,
    );
    let elided = galley.text() != message;
    ui.painter().galley(
        Pos2::new(rect.left() + 12.0, rect.center().y - galley.size().y * 0.5),
        galley,
        p.text2,
    );
    let response = ui.interact(
        rect,
        Id::new(("maintenance_notice", region)),
        Sense::hover(),
    );
    let accessible = message.to_owned();
    response.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Label, true, &accessible));
    if elided {
        response.on_hover_text(message);
    }
}

fn data_columns(tab: GridTab) -> Vec<Column> {
    match tab {
        GridTab::Attestation => vec![
            Column::fixed("atom id", 120.0, Align::Left),
            Column::fixed("verdict", 180.0, Align::Left),
            Column::fixed("key slot", 100.0, Align::Right),
            Column::flex("scope", Align::Left),
        ],
        _ => vec![
            Column::flex("content", Align::Left),
            Column::fixed("kind", 140.0, Align::Left),
            Column::fixed("created", 150.0, Align::Left),
        ],
    }
}

fn cells_for(a: &AtomView, tab: GridTab, p: &crate::theme::Palette) -> Vec<Cell> {
    let evidence = a.evidence();
    let tone = match evidence {
        Evidence::Erased | Evidence::Missing => Some(p.text4),
        Evidence::Tampered => Some(p.tampered),
        _ => None,
    };
    match tab {
        GridTab::Attestation => vec![
            Cell::new(a.id.to_string()),
            match evidence.colour(p) {
                Some(c) => Cell::tinted(a.verdict_label(), c),
                None => Cell::tinted(a.verdict_label(), p.text3),
            },
            Cell::tinted(a.key_label(), p.text2),
            Cell::tinted(evidence.scope(), p.text3),
        ],
        // Expose only columns returned by `AtomHit`.
        _ => vec![
            match tone {
                Some(c) => Cell::tinted(a.text.clone(), c),
                None => Cell::new(a.text.clone()),
            },
            Cell::tinted(a.kind.clone(), p.text3),
            Cell::new(a.created()),
        ],
    }
}

fn structure_pane(
    painter: &egui::Painter,
    rect: Rect,
    region: &crate::model::Region,
    p: &crate::theme::Palette,
) {
    w::fill(painter, rect, p.ground1);
    let c = region.counts();
    // Label region size separately from the amount fetched this session.
    let total = region
        .facts
        .total
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let read = c.total().to_string();
    field_list(
        painter,
        Pos2::new(rect.left() + 24.0, rect.top() + 30.0),
        &[
            Field::new("region", region.name()),
            Field::new("dimensions", &region.facts.dim.to_string()),
            Field::new("metric", region.facts.metric_label()),
            Field::new("model id", &region.facts.model),
            Field::new("atoms", &total),
            Field::new("read this session", &read),
            Field::new("encrypted", if region.plaintext() { "no" } else { "yes" }),
        ],
        rect.width() - 48.0,
        p,
    );
    if region.plaintext() {
        let note = painter.layout(
            "This region is plaintext. No per-atom MAC exists, so nothing in it can be \
             attested and erasure here is a row delete, not a key destruction."
                .to_owned(),
            role::meta(),
            p.text4,
            420.0,
        );
        painter.galley(
            Pos2::new(rect.left() + 24.0, rect.top() + 168.0),
            note,
            p.text4,
        );
    }
}

/// Selected atom with a matching evidence rail and explicitly scoped verdict.
pub(super) fn atom_inspector(ui: &mut Ui, insp: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    let Some(region) = state.visible_region() else {
        inspector_empty(ui.painter(), insp, "Atom", "No region is open.", p);
        return;
    };
    let plaintext = region.facts.plaintext;
    if let Some(reason) = region.facts.unreadable.as_deref() {
        inspector_empty(ui.painter(), insp, "Region unavailable", reason, p);
        return;
    }
    let Some(atom) = region.atoms.get(state.selected_row) else {
        let why = if region.facts.total == Some(0) {
            "This region holds no atoms."
        } else if region.atoms.is_empty() {
            "Loading rows."
        } else {
            "Select a row to see the atom behind it."
        };
        inspector_empty(ui.painter(), insp, "Atom", why, p);
        return;
    };

    let id = atom.id.to_string();
    let head = inspector_header(ui.painter(), insp, "Atom", &id, p);

    let quote_top = head.bottom() + 16.0;
    if let Some(colour) = atom.evidence().colour(p) {
        ui.painter().rect_filled(
            Rect::from_min_size(
                Pos2::new(insp.left() + metrics::PANE_PAD, quote_top),
                Vec2::new(2.0, 36.0),
            ),
            0.0,
            colour,
        );
    }
    // Bound the non-scrolling inspector excerpt; the grid retains the full content.
    const QUOTE_MAX_H: f32 = 96.0;
    let quote = w::wrap_to_height(
        ui.painter(),
        &atom.text,
        role::cell_compact(),
        insp.width() - 44.0,
        QUOTE_MAX_H,
        p.text1,
    );
    let quote_h = quote.size().y;
    ui.painter()
        .galley(Pos2::new(insp.left() + 28.0, quote_top), quote, p.text1);

    // Show only engine facts; key-slot state exists only after verification.
    let created = atom.created();
    let verdict = atom.verdict_label();
    let key = atom.key_label();
    let end = field_list(
        ui.painter(),
        Pos2::new(insp.left() + metrics::PANE_PAD, quote_top + quote_h + 26.0),
        &[
            Field::new("atom id", &id),
            Field::new("kind", &atom.kind),
            Field {
                label: "verdict",
                value: &verdict,
                colour: atom.evidence().colour(p),
            },
            Field::new("key slot", &key),
            Field::new("created", &created),
            Field::new("verified at", atom.checked()),
            Field::new("immutable", if atom.immutable { "yes" } else { "no" }),
        ],
        insp.width() - metrics::PANE_PAD * 2.0,
        p,
    );

    // Keep verification scope adjacent to its verdict.
    let scope = ui.painter().layout(
        format!("Scope: {}.", atom.evidence().scope()),
        role::meta(),
        p.text4,
        insp.width() - 24.0,
    );
    let scope_h = scope.size().y;
    ui.painter().galley(
        Pos2::new(insp.left() + metrics::PANE_PAD, end + 8.0),
        scope,
        p.text4,
    );

    let btn = Rect::from_min_size(
        Pos2::new(insp.left() + metrics::PANE_PAD, end + scope_h + 20.0),
        Vec2::new(150.0, 32.0),
    );
    // Studio uses non-forced erasure, so immutable atoms remain unavailable here.
    let can_forget = atom.evidence() != Evidence::Erased && !atom.immutable;
    let tip = if plaintext {
        "Deletes this atom. A plaintext region has no key to destroy. Irreversible."
    } else {
        "Destroys this atom's key. Irreversible."
    };
    let forget = ui::Btn::new("Forget atom", ui::ButtonKind::Danger);
    let forget = if !can_forget {
        forget.unavailable(if atom.immutable {
            "This atom is marked immutable"
        } else {
            "This atom's key is already destroyed"
        })
    } else if let Some(why) = state.foreground_blocker() {
        forget.unavailable(why)
    } else {
        forget.tip(tip)
    };
    if ui::button(ui, btn, forget, p).clicked() {
        out.push(Action::BeginForgetSelected);
    }

    let legend_y = btn.bottom() + 24.0;
    w::text_left(
        ui.painter(),
        Pos2::new(insp.left() + metrics::PANE_PAD, legend_y),
        "EVIDENCE STATES",
        role::meta(),
        p.text3,
    );
    for (i, ev) in rail::ALL.into_iter().enumerate() {
        let y = legend_y + 24.0 + i as f32 * 24.0;
        let on = ev == atom.evidence();
        let row = Rect::from_min_size(
            Pos2::new(insp.left() + metrics::PANE_PAD - 6.0, y - 11.0),
            Vec2::new(insp.width() - metrics::PANE_PAD * 2.0 + 12.0, 22.0),
        );
        if on {
            ui.painter()
                .rect_filled(row, egui::CornerRadius::same(3), p.wash(20));
        }
        let tone = ev.colour(p).unwrap_or(p.text3);
        let swatch = Rect::from_center_size(
            Pos2::new(insp.left() + metrics::PANE_PAD + 8.0, y),
            Vec2::new(16.0, 12.0),
        );
        if ev.colour(p).is_some() {
            ui.painter()
                .rect_filled(swatch, egui::CornerRadius::same(3), tone);
        } else {
            ui.painter().rect_stroke(
                swatch,
                egui::CornerRadius::same(3),
                Stroke::new(1.0, p.hairline_strong),
                StrokeKind::Inside,
            );
        }
        w::text_left(
            ui.painter(),
            Pos2::new(insp.left() + metrics::PANE_PAD + 26.0, y),
            rail::glyph(ev),
            role::meta_mono(),
            tone,
        );
        let context = match ev {
            Evidence::Verified => "after Verify rows",
            Evidence::Unverified => "before verification",
            Evidence::Erased => "content unrecoverable",
            Evidence::Tampered => "authentication failed",
            Evidence::Missing => "atom absent",
            Evidence::NotAttestable => "plaintext region",
        };
        let label = if on {
            format!("{} — {context} · current", ev.label())
        } else {
            format!("{} — {context}", ev.label())
        };
        w::text_left(
            ui.painter(),
            Pos2::new(insp.left() + metrics::PANE_PAD + 60.0, y),
            &label,
            role::meta(),
            if on { p.text1 } else { p.text2 },
        );
        let accessible = format!(
            "{} {} — {context}. {}",
            rail::glyph(ev),
            ev.label(),
            ev.scope()
        );
        let response = ui.interact(
            row,
            Id::new(("evidence_legend", ev.label())),
            Sense::hover(),
        );
        response
            .widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Label, true, &accessible));
        response.on_hover_text(accessible);
    }
}

pub(super) fn forget_confirmation(ui: &mut Ui, bounds: Rect, state: &State, out: &mut Vec<Action>) {
    let Some(prompt) = &state.forget_prompt else {
        return;
    };
    let p = &state.palette;
    let width = (bounds.width() - 32.0).clamp(260.0, 460.0);
    let warning = if prompt.plaintext {
        "This permanently deletes the row. Plaintext regions have no per-atom key to destroy."
    } else {
        "This permanently destroys the atom key. Its ciphertext remains, but the content cannot be recovered."
    };
    let modal = Modal::new(Id::new("forget_confirmation"))
        .frame(
            Frame::new()
                .fill(p.ground3)
                .stroke(Stroke::new(1.0, p.hairline_strong))
                .corner_radius(crate::theme::radius::CARD)
                .inner_margin(Margin::same(20)),
        )
        .backdrop_color(crate::theme::Palette::tint(p.ground0, 232))
        .show(ui.ctx(), |ui| {
            ui.set_width(width - 40.0);
            ui.ctx().accesskit_node_builder(ui.unique_id(), |node| {
                node.set_role(egui::accesskit::Role::Dialog);
                node.set_label(format!("Confirm forgetting atom {}", prompt.atom_id));
            });
            ui.label(
                RichText::new(format!("Forget atom {}?", prompt.atom_id))
                    .font(role::section())
                    .color(p.text1),
            );
            ui.add_space(8.0);
            ui.add(
                egui::Label::new(RichText::new(warning).font(role::cell()).color(p.text2)).wrap(),
            );
            ui.add_space(18.0);
            ui.label(
                RichText::new("This action cannot be undone.")
                    .font(role::meta())
                    .color(p.tampered),
            );
            ui.add_space(14.0);

            let (_, row) = ui.allocate_space(Vec2::new(ui.available_width(), 32.0));
            let confirm = Rect::from_min_size(
                Pos2::new(row.right() - 116.0, row.top()),
                Vec2::new(116.0, 32.0),
            );
            let cancel = Rect::from_min_size(
                Pos2::new(confirm.left() - 108.0, row.top()),
                Vec2::new(96.0, 32.0),
            );
            let cancel = ui::button(ui, cancel, ui::Btn::new("Cancel", ui::ButtonKind::Ghost), p);
            let confirm = ui::button(
                ui,
                confirm,
                ui::Btn::new("Forget", ui::ButtonKind::Danger).keyed("Confirm forget"),
                p,
            );

            let focus = ui.ctx().memory(|memory| memory.focused());
            if focus != Some(cancel.id) && focus != Some(confirm.id) {
                cancel.request_focus();
            }
            (cancel.clicked(), confirm.clicked())
        });

    if modal.inner.1 {
        out.push(Action::ConfirmForget);
    } else if modal.inner.0 || modal.should_close() {
        out.push(Action::CancelForget);
    }
}
