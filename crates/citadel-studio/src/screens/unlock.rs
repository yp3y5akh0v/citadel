//! Unlock preflight; unauthenticated header facts remain explicitly unverified.

use super::*;
use crate::fonts::role;
use crate::rail;
use crate::state::{Action, SecretField, State, UnlockStage};
use crate::theme::{radius, Evidence, Palette};
use crate::ui;
use crate::widgets as w;
use egui::{Painter, Pos2, Rect, Ui, Vec2};

pub fn unlock(ui: &mut Ui, full: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    let s = Shell::new(full);
    w::fill(ui.painter(), full, p.ground1);
    band(ui, s.band, state, out);

    // Shell chrome is painted outside the clipped content column.
    let (lock, proof) = match &state.vault {
        Some(vault) => (format!("{} open", vault.name), state.page_check_label()),
        None => ("locked".to_owned(), "nothing attested".to_owned()),
    };
    // Name the file being unlocked.
    let bar_path = state
        .target
        .path(state.preview.read().map(|i| i.data_path.as_path()));
    w::status_bar(
        ui,
        s.status,
        p,
        &[&bar_path, &lock],
        &[&proof, super::BUILD_LABEL],
    );

    let (col_w, x) = entry_column(s.body, 440.0);
    let ui = &mut ui.new_child(egui::UiBuilder::new().max_rect(s.body));
    ui.set_clip_rect(s.body);
    let mut y = s.body.top() + s.body.height() * 0.16;

    lock_glyph(ui.painter(), Pos2::new(x + 14.0, y + 4.0), p);
    w::text_left(
        ui.painter(),
        Pos2::new(x + 40.0, y + 8.0),
        "Unlock vault",
        role::headline(),
        p.text1,
    );
    y += 34.0;
    let info = state.preview.read();
    let path = state.target.path(info.map(|i| i.data_path.as_path()));
    let fitted = w::elide_galley(ui.painter(), &path, role::cell_compact(), col_w, p.text3);
    ui.painter()
        .galley(Pos2::new(x, y - fitted.size().y * 0.5), fitted, p.text3);
    y += 22.0;

    // These facts are read before authentication.
    let rows = crate::model::preview_rows(&state.preview);
    let pre = Rect::from_min_size(
        Pos2::new(x, y),
        Vec2::new(col_w, rows.len() as f32 * 20.0 + 20.0),
    );
    w::surface(ui.painter(), pre, p.ground0, None, radius::CONTROL);
    for (i, (label, value)) in rows.into_iter().enumerate() {
        let ry = pre.top() + 18.0 + i as f32 * 20.0;
        rail::paint(
            ui.painter(),
            Rect::from_min_size(Pos2::new(pre.left() + 12.0, ry - 7.0), Vec2::new(3.0, 14.0)),
            Evidence::Unverified,
            p,
            state.ppp,
        );
        w::text_left(
            ui.painter(),
            Pos2::new(pre.left() + 26.0, ry),
            &label.to_uppercase(),
            role::meta(),
            p.text3,
        );
        w::text_left(
            ui.painter(),
            Pos2::new(pre.left() + 150.0, ry),
            &value,
            role::meta_mono(),
            p.text1,
        );
    }
    y = pre.bottom() + 20.0;

    w::text_left(
        ui.painter(),
        Pos2::new(x, y),
        "PASSPHRASE",
        role::meta(),
        p.text3,
    );
    y += 16.0;
    let field = Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, 32.0));
    let out_field = ui::secret(
        ui,
        field,
        ui::Secret {
            id: egui::Id::new("passphrase"),
            label: "Vault passphrase",
            value: &state.passphrase,
            placeholder: "passphrase",
            revealed: state.revealed == Some(SecretField::Unlock),
            refused: state.unlock == UnlockStage::Rejected,
            // Freeze the field while the palette owns keyboard input.
            frozen: state.palette_open || state.unlock == UnlockStage::Deriving,
            autofocus: true,
        },
        p,
    );
    if let Some(value) = out_field.edited {
        out.push(Action::SetSecret(SecretField::Unlock, value));
    }
    if out_field.toggled {
        out.push(Action::ToggleReveal(SecretField::Unlock));
    }
    y = field.bottom() + 6.0;

    // Reserve the progress hairline so content below it remains stable.
    let track = Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, 4.0));
    ui.painter()
        .rect_filled(track, egui::CornerRadius::same(2), p.wash(15));
    if state.unlock == UnlockStage::Deriving {
        // Key derivation exposes no measurable progress.
        let sweep = col_w * 0.28;
        let travel = (col_w + sweep) * ui.input(|i| i.time as f32 % 1.4) / 1.4 - sweep;
        let lit = Rect::from_min_max(
            Pos2::new(track.left() + travel.max(0.0), track.top()),
            Pos2::new(
                (track.left() + travel + sweep).min(track.right()),
                track.bottom(),
            ),
        );
        if lit.width() > 0.0 {
            ui.painter()
                .rect_filled(lit, egui::CornerRadius::same(2), p.accent_solid);
        }
        // Repaint until the engine thread reports completion.
        ui.ctx().request_repaint();
    }
    y += 12.0;

    // Reserve a status slot so failures do not move the actions.
    let (msg, colour) = match state.unlock {
        // Completion time depends on the vault's KDF parameters.
        UnlockStage::Ready => (
            "Deriving the key cannot be cancelled once it starts.".to_owned(),
            p.text4,
        ),
        UnlockStage::Deriving => (
            "Deriving key from passphrase. This step cannot be cancelled.".to_owned(),
            p.text3,
        ),
        // Preserve the engine's distinction among authentication and format failures.
        UnlockStage::Rejected => match state.opening_error() {
            Some(error) => (error.to_string(), p.tampered),
            None => (
                format!(
                    "That passphrase did not unlock {}. Citadel cannot recover or reset it.",
                    state
                        .preview
                        .read()
                        .map_or_else(|| "this vault".to_owned(), crate::model::file_name)
                ),
                p.tampered,
            ),
        },
    };
    // Wrap diagnostics while preserving the minimum status height.
    let wrapped = ui
        .painter()
        .layout(msg.clone(), role::meta(), colour, col_w);
    let msg_h = wrapped.size().y.max(16.0);
    ui.painter().galley(Pos2::new(x, y), wrapped, colour);
    w::semantic_status(
        ui,
        Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, msg_h)),
        "unlock_vault",
        &msg,
        state.unlock == UnlockStage::Rejected,
    );
    y += msg_h + 12.0;

    // Shrink actions within undersized columns.
    let (cancel, primary) = form_buttons(x, y, col_w);
    match state.unlock {
        // Derivation has no cancellation token, so do not expose a false Cancel action.
        UnlockStage::Deriving => {
            ui::button(
                ui,
                primary,
                ui::Btn::new("Deriving", ui::ButtonKind::Primary)
                    .unavailable("Argon2 is running and has no cancel token"),
                p,
            );
        }
        _ => {
            if ui::button(
                ui,
                cancel,
                ui::Btn::new("Cancel", ui::ButtonKind::Outlined),
                p,
            )
            .clicked()
            {
                out.push(Action::CancelUnlock);
            }
            // Expose each refusal on the disabled control.
            let unlock = ui::Btn::new("Unlock", ui::ButtonKind::Primary);
            let unlock = if state.passphrase.is_empty() {
                unlock.unavailable("Enter the passphrase first")
            } else {
                unlock
            };
            if ui::button(ui, primary, unlock, p).clicked() {
                out.push(Action::SubmitPassphrase);
            }
        }
    }
}

fn lock_glyph(painter: &Painter, c: Pos2, p: &Palette) {
    let stroke = egui::Stroke::new(1.6, p.text2);
    painter.rect_stroke(
        Rect::from_center_size(Pos2::new(c.x, c.y + 6.0), Vec2::new(24.0, 18.0)),
        egui::CornerRadius::same(3),
        stroke,
        egui::StrokeKind::Inside,
    );
    painter.add(egui::Shape::line(
        vec![
            Pos2::new(c.x - 7.0, c.y - 3.0),
            Pos2::new(c.x - 7.0, c.y - 9.0),
            Pos2::new(c.x + 7.0, c.y - 9.0),
            Pos2::new(c.x + 7.0, c.y - 3.0),
        ],
        stroke,
    ));
    painter.circle_filled(Pos2::new(c.x, c.y + 5.0), 1.8, p.text2);
}
