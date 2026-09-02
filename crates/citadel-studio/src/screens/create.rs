//! Vault creation form.

use super::*;
use crate::fonts::role;
use crate::state::{self, Action, KdfAlgorithm, NewVault, SecretField, State, UnlockStage};
use crate::theme::{radius, Palette};
use crate::ui;
use crate::widgets as w;
use egui::{Pos2, Rect, Ui, Vec2};

/// Form height used to center it without losing actions below the body.
const FORM_H: f32 = 560.0;

/// Shared geometry for center-anchored labels/segments and top-anchored fields.
mod rhythm {
    /// Half a metadata line.
    pub const LINE_HALF: f32 = 7.0;
    /// Half the centered segmented-control track.
    pub const TRACK_HALF: f32 = 12.0;
    /// A text field's height.
    pub const FIELD_H: f32 = 32.0;
    /// Label edge to the top of the control it names.
    pub const LABEL_TO_CONTROL: f32 = 9.0;
    /// Control edge to the line explaining it.
    pub const CONTROL_TO_NOTE: f32 = 8.0;
    /// The bottom of one section to the top of the next section's label.
    pub const SECTION: f32 = 12.0;

    /// Label center to field top.
    pub const fn label_to_field_top() -> f32 {
        LINE_HALF + LABEL_TO_CONTROL
    }

    /// Label center to segmented-control center.
    pub const fn label_to_track() -> f32 {
        label_to_field_top() + TRACK_HALF
    }

    /// Segmented-control center to note center.
    pub const fn track_to_note() -> f32 {
        TRACK_HALF + CONTROL_TO_NOTE + LINE_HALF
    }

    /// Section bottom to next label center.
    pub const fn to_next_label() -> f32 {
        SECTION + LINE_HALF
    }
}

pub fn create(ui: &mut Ui, full: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    let s = Shell::new(full);
    w::fill(ui.painter(), full, p.ground1);
    band(ui, s.band, state, out);

    let form = &state.new_vault;
    let destination = form
        .path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "no destination chosen".to_owned());
    w::status_bar(
        ui,
        s.status,
        p,
        &[&destination, "nothing written"],
        &["nothing attested", super::BUILD_LABEL],
    );

    let (col_w, x) = entry_column(s.body, 440.0);
    let ui = &mut ui.new_child(egui::UiBuilder::new().max_rect(s.body));
    ui.set_clip_rect(s.body);
    // Center within the body while preserving a minimum top margin.
    let mut y = s.body.top() + ((s.body.height() - FORM_H) * 0.5).clamp(24.0, 120.0);

    plus_glyph(ui.painter(), Pos2::new(x + 14.0, y + 4.0), p.accent);
    w::text_left(
        ui.painter(),
        Pos2::new(x + 40.0, y + 8.0),
        "Create vault",
        role::headline(),
        p.text1,
    );
    y += 32.0;
    let note = ui.painter().layout(
        "Encrypted from the first byte. The passphrase is the only way back in, and \
         Citadel cannot recover or reset it."
            .to_owned(),
        role::body(),
        p.text2,
        col_w,
    );
    y += note.size().y;
    ui.painter()
        .galley(Pos2::new(x, y - note.size().y), note, p.text2);
    y += rhythm::to_next_label();

    y = destination_row(ui, x, y, col_w, form, p, out);
    y = passphrase_rows(ui, x, y, col_w, state, out);
    y = derivation_rows(ui, x, y, form, p, out);

    label(ui, x, y, "CIPHER", p);
    // Keep informational values on the control baseline.
    y += rhythm::label_to_field_top() + rhythm::LINE_HALF;
    // The engine's fixed construction is informational, not selectable.
    w::text_left(
        ui.painter(),
        Pos2::new(x, y),
        "AES-256-CTR, per-atom keys wrapped by the master key",
        role::meta_mono(),
        p.text2,
    );
    y += rhythm::LINE_HALF + rhythm::to_next_label();

    // Reserve a stable status slot; engine failures outrank local validation.
    let deriving = state.unlock == UnlockStage::Deriving;
    let (message, colour) = match (state.opening_error(), form.blocker()) {
        (Some(error), _) => (error.to_string(), p.tampered),
        (None, _) if deriving => (
            "Deriving the key and writing the vault.".to_owned(),
            p.text3,
        ),
        (None, Some(reason)) => (reason.to_owned(), p.text3),
        (None, None) => (String::new(), p.text3),
    };
    w::text_left(
        ui.painter(),
        Pos2::new(x, y),
        &message,
        role::meta(),
        colour,
    );
    w::semantic_status(
        ui,
        Rect::from_min_size(Pos2::new(x, y - 8.0), Vec2::new(col_w, 20.0)),
        "create_vault",
        &message,
        state.opening_error().is_some(),
    );
    y += rhythm::LINE_HALF + rhythm::SECTION;

    // Align actions to the field edge and shrink within narrow columns.
    let (cancel, primary) = form_buttons(x, y, col_w);
    let cancel_button = ui::Btn::new("Cancel", ui::ButtonKind::Outlined);
    let cancel_button = if deriving {
        cancel_button.unavailable("The vault write is already in progress")
    } else {
        cancel_button
    };
    if ui::button(ui, cancel, cancel_button, p).clicked() {
        out.push(Action::CancelCreate);
    }
    // Prevent duplicate creation while key derivation is in flight.
    let create = ui::Btn::new("Create vault", ui::ButtonKind::Primary);
    let create = match (deriving, form.blocker()) {
        (true, _) => create.unavailable("Writing the vault"),
        (false, Some(blocker)) => create.unavailable(blocker),
        (false, None) => create,
    };
    if ui::button(ui, primary, create, p).clicked() {
        out.push(Action::SubmitCreate);
    }
}

fn destination_row(
    ui: &mut Ui,
    x: f32,
    mut y: f32,
    col_w: f32,
    form: &NewVault,
    p: &Palette,
    out: &mut Vec<Action>,
) -> f32 {
    label(ui, x, y, "DESTINATION", p);
    y += rhythm::label_to_field_top();
    let button = Rect::from_min_size(
        Pos2::new(x + col_w - 100.0, y),
        Vec2::new(100.0, rhythm::FIELD_H),
    );
    let shown = Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w - 110.0, rhythm::FIELD_H));
    w::surface(
        ui.painter(),
        shown,
        p.ground0,
        Some(p.hairline_strong),
        radius::CONTROL,
    );
    let (text, ink) = match &form.path {
        Some(path) => (path.display().to_string(), p.text1),
        None => ("choose where to save".to_owned(), p.text4),
    };
    let fitted = w::elide_galley(ui.painter(), &text, role::cell(), shown.width() - 20.0, ink);
    ui.painter().galley(
        Pos2::new(
            shown.left() + 10.0,
            shown.center().y - fitted.size().y * 0.5,
        ),
        fitted,
        ink,
    );
    if ui::button(
        ui,
        button,
        ui::Btn::new("Choose", ui::ButtonKind::Outlined),
        p,
    )
    .clicked()
    {
        out.push(Action::ChooseVaultDestination);
    }
    y + rhythm::FIELD_H + rhythm::to_next_label()
}

fn passphrase_rows(
    ui: &mut Ui,
    x: f32,
    mut y: f32,
    col_w: f32,
    state: &State,
    out: &mut Vec<Action>,
) -> f32 {
    let p = &state.palette;
    let form = &state.new_vault;
    // Mark only confirmation when the pair differs.
    let mismatched = !form.confirm.is_empty() && form.confirm != form.passphrase;
    for (field, caption, accessible_label, value, refused) in [
        (
            SecretField::New,
            "PASSPHRASE",
            "New vault passphrase",
            &form.passphrase,
            false,
        ),
        (
            SecretField::Confirm,
            "CONFIRM PASSPHRASE",
            "Confirm new vault passphrase",
            &form.confirm,
            mismatched,
        ),
    ] {
        label(ui, x, y, caption, p);
        y += rhythm::label_to_field_top();
        let rect = Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, rhythm::FIELD_H));
        let out_field = ui::secret(
            ui,
            rect,
            ui::Secret {
                id: egui::Id::new(("create_secret", caption)),
                label: accessible_label,
                value,
                placeholder: "at least 12 characters",
                revealed: state.revealed == Some(field),
                refused,
                // Freeze the submitted passphrase while derivation runs.
                frozen: state.palette_open || state.unlock == UnlockStage::Deriving,
                autofocus: field == SecretField::New,
            },
            p,
        );
        if let Some(next) = out_field.edited {
            out.push(Action::SetSecret(field, next));
        }
        if out_field.toggled {
            out.push(Action::ToggleReveal(field));
        }
        y += rhythm::FIELD_H + rhythm::to_next_label();
    }
    y
}

fn derivation_rows(
    ui: &mut Ui,
    x: f32,
    mut y: f32,
    form: &NewVault,
    p: &Palette,
    out: &mut Vec<Action>,
) -> f32 {
    label(ui, x, y, "KEY DERIVATION", p);
    y += rhythm::label_to_track();
    let labels: Vec<&str> = state::KDFS.iter().map(|k| state::kdf_label(*k)).collect();
    let active = state::KDFS
        .iter()
        .position(|k| *k == form.kdf)
        .expect("in KDFS");
    if let Some(i) = ui::segmented(ui, "kdf", Pos2::new(x, y), &labels, active, p) {
        out.push(Action::SetKdf(state::KDFS[i]));
    }
    y += rhythm::track_to_note();
    w::text_left(
        ui.painter(),
        Pos2::new(x, y),
        state::kdf_detail(form.kdf),
        role::meta(),
        p.text3,
    );
    y += rhythm::LINE_HALF + rhythm::to_next_label();

    // Argon2id exposes a profile; PBKDF2 uses the engine's fixed iteration floor.
    label(ui, x, y, "COST", p);
    y += rhythm::label_to_track();
    match form.kdf {
        KdfAlgorithm::Argon2id => {
            let labels: Vec<&str> = state::PROFILES
                .iter()
                .map(|k| state::profile_label(*k))
                .collect();
            let active = state::PROFILES
                .iter()
                .position(|k| *k == form.profile)
                .expect("in PROFILES");
            if let Some(i) = ui::segmented(ui, "profile", Pos2::new(x, y), &labels, active, p) {
                out.push(Action::SetArgon2Profile(state::PROFILES[i]));
            }
            y += rhythm::track_to_note();
            w::text_left(
                ui.painter(),
                Pos2::new(x, y),
                &state::profile_detail(form.profile),
                role::meta_mono(),
                p.text3,
            );
        }
        // Match the Argon2 branch height so algorithm changes do not reflow the form.
        KdfAlgorithm::Pbkdf2HmacSha256 => {
            w::text_left(
                ui.painter(),
                Pos2::new(x, y),
                "600,000 iterations",
                role::meta_mono(),
                p.text2,
            );
            y += rhythm::track_to_note();
            w::text_left(
                ui.painter(),
                Pos2::new(x, y),
                "citadel-core's minimum, and the only count offered",
                role::meta(),
                p.text3,
            );
        }
    }
    y + rhythm::LINE_HALF + rhythm::to_next_label()
}

fn label(ui: &Ui, x: f32, y: f32, text: &str, p: &Palette) {
    w::text_left(ui.painter(), Pos2::new(x, y), text, role::meta(), p.text3);
}
