//! Vault integrity, audit, passphrase and erasure state.

use super::*;
use crate::fonts::role;
use crate::model::Readback;
use crate::rail;
use crate::state::{Action, IntegrityState, RotateState, SecretField, State};
use crate::theme::{metrics, radius, Evidence};
use crate::ui;
use crate::widgets as w;
use egui::{Color32, Painter, Pos2, Rect, Ui, Vec2};

pub fn pane(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    w::fill(ui.painter(), rect, state.palette.ground1);

    let content_w = column_width(rect) - metrics::CARD_PAD * 2.0;
    let plan = Plan::new(ui, state, content_w);
    let source = if state.palette_open {
        egui::scroll_area::ScrollSource::NONE
    } else {
        egui::scroll_area::ScrollSource::default()
    };
    let (output, bar_id) = ui
        .scope_builder(egui::UiBuilder::new().max_rect(rect), |ui| {
            ui.set_clip_rect(rect);
            let scroll_id = ui.make_persistent_id(egui::IdSalt::new("vault_security"));
            let bar_id = scroll_id.with("keyboard_scrollbar");
            ui.interact(rect, bar_id, egui::Sense::focusable_noninteractive())
                .widget_info(|| egui::WidgetInfo::new(egui::WidgetType::ScrollBar));
            let estimated_max = (plan.height() - rect.height()).max(0.0);
            let mut requested = state.pane_scroll.clamp(0.0, estimated_max);
            if !state.palette_open && ui.memory(|memory| memory.has_focus(bar_id)) {
                let page = (rect.height() - 40.0).max(40.0);
                ui.input_mut(|input| {
                    if input.consume_key(egui::Modifiers::NONE, egui::Key::ArrowUp) {
                        requested -= 40.0;
                    }
                    if input.consume_key(egui::Modifiers::NONE, egui::Key::ArrowDown) {
                        requested += 40.0;
                    }
                    if input.consume_key(egui::Modifiers::NONE, egui::Key::PageUp) {
                        requested -= page;
                    }
                    if input.consume_key(egui::Modifiers::NONE, egui::Key::PageDown) {
                        requested += page;
                    }
                    if input.consume_key(egui::Modifiers::NONE, egui::Key::Home) {
                        requested = 0.0;
                    }
                    if input.consume_key(egui::Modifiers::NONE, egui::Key::End) {
                        requested = estimated_max;
                    }
                });
                requested = requested.clamp(0.0, estimated_max);
            }
            let output = egui::ScrollArea::vertical()
                .id_salt("vault_security")
                .auto_shrink([false, false])
                .scroll_source(source)
                .vertical_scroll_offset(requested)
                .show_viewport(ui, |ui, _| {
                    let (_, content) =
                        ui.allocate_space(Vec2::new(ui.available_width(), plan.height()));
                    column(ui, content, state, &plan, out);
                });
            (output, bar_id)
        })
        .inner;

    let max = (output.content_size.y - output.inner_rect.height()).max(0.0);
    ui.ctx()
        .accesskit_node_builder(output.id.with(1), |node| node.set_hidden());
    if max > 0.0 {
        ui.ctx().accesskit_node_builder(bar_id, |node| {
            node.set_label("Vault security content");
            node.set_min_numeric_value(0.0);
            node.set_max_numeric_value(f64::from(max));
            node.set_numeric_value(f64::from(output.state.offset.y));
            node.set_numeric_value_step(40.0);
        });
    }

    let next = output.state.offset.y.clamp(0.0, max);
    if next != state.pane_scroll {
        out.push(Action::ScrollPane(next));
    }
}

/// Padding below the last card and width of the column gutters.
const PAD: f32 = 24.0;
const GUTTER: f32 = 96.0;
const COL_MAX: f32 = 780.0;

/// One vertical plan shared by painting and scroll clamping.
struct Plan {
    banner: bool,
    receipts: usize,
    note: Option<std::sync::Arc<egui::Galley>>,
    integrity_errors: usize,
    audit_breaks: usize,
    rotate: f32,
}

impl Plan {
    const TITLE: f32 = 46.0 + 22.0;
    const BANNER: f32 = 42.0 + 20.0;
    const CIPHER: f32 = 118.0 + 16.0;
    const ATTEST: f32 = 150.0 + 16.0;
    const ROW: f32 = 22.0;
    const GAP: f32 = 16.0;
    const INSET: f32 = 36.0;
    const INTEGRITY: f32 = 82.0;
    const AUDIT: f32 = 90.0;
    const ROTATE_FORM: f32 = 44.0 + 3.0 * 58.0 + 24.0 + 32.0 + 16.0;
    const ROTATE_LINE: f32 = 74.0;
    const LINE: f32 = 20.0;
    const HEAD: f32 = 74.0;
    const FOOT: f32 = 12.0;

    fn new(ui: &Ui, state: &State, content_w: f32) -> Self {
        let vault = state.vault();
        let note = vault
            .receipts
            .iter()
            .any(|r| r.readback() == Readback::NotApplicable)
            .then(|| {
                ui.painter().layout(
                    "A receipt over a plaintext region records a row delete. Cryptographic \
                     erasure does not apply there, which is what READBACK reports as \
                     not applicable."
                        .to_owned(),
                    role::meta(),
                    state.palette.text4,
                    content_w,
                )
            });
        Self {
            banner: vault.counts().unverified > 0,
            receipts: vault.receipts.len(),
            note,
            integrity_errors: match &state.integrity {
                IntegrityState::Checked(f) => f.errors.len(),
                _ => 0,
            },
            audit_breaks: vault.facts.audit.as_ref().map_or(0, |a| {
                a.segments.iter().filter(|s| !s.chain_valid).count()
                    + usize::from(a.count_shortfall() > 0)
            }),
            rotate: if vault.is_demo {
                0.0
            } else {
                match state.rotate {
                    RotateState::Closed => 0.0,
                    RotateState::Open(_) => Self::ROTATE_FORM + 16.0,
                    _ => Self::ROTATE_LINE + 16.0,
                }
            },
        }
    }

    fn note_h(&self) -> f32 {
        self.note.as_ref().map(|g| g.size().y + 8.0).unwrap_or(0.0)
    }

    fn rotate_h(&self) -> f32 {
        (self.rotate - 16.0).max(0.0)
    }

    fn integrity_h(&self) -> f32 {
        Self::INTEGRITY + self.integrity_errors as f32 * Self::LINE
    }

    fn audit_h(&self) -> f32 {
        Self::AUDIT + self.audit_breaks as f32 * Self::LINE
    }

    fn receipts_h(&self) -> f32 {
        Self::HEAD + self.receipts as f32 * Self::ROW + self.note_h() + Self::FOOT
    }

    fn height(&self) -> f32 {
        Self::INSET
            + Self::TITLE
            + if self.banner { Self::BANNER } else { 0.0 }
            + Self::CIPHER
            + self.rotate
            + self.integrity_h()
            + Self::GAP
            + self.audit_h()
            + Self::GAP
            + Self::ATTEST
            + self.receipts_h()
            + PAD
    }
}

fn column_width(rect: Rect) -> f32 {
    (rect.width() - GUTTER).min(COL_MAX)
}

fn tombstone_summary(count: u64) -> String {
    let slots = if count == 1 { "key slot" } else { "key slots" };
    format!("{count} {slots} currently tombstoned; lifetime total unavailable")
}

/// A card's label/value grid, two to a row.
fn pair_grid(
    ui: &mut Ui,
    painter: &Painter,
    id: impl std::hash::Hash + std::fmt::Debug,
    card: Rect,
    col_w: f32,
    p: &Palette,
    pairs: &[(&str, String)],
) {
    for (i, (k, v)) in pairs.iter().enumerate() {
        let cx = card.left() + metrics::CARD_PAD + (i % 2) as f32 * (col_w * 0.5);
        let cy = card.top() + 50.0 + (i / 2) as f32 * 22.0;
        w::text_left(
            painter,
            Pos2::new(cx, cy),
            &k.to_uppercase(),
            role::meta(),
            p.text3,
        );
        let g = w::elide_galley(
            painter,
            v,
            role::cell_compact(),
            col_w * 0.5 - 140.0,
            p.text1,
        );
        painter.galley(Pos2::new(cx + 130.0, cy - g.size().y * 0.5), g, p.text1);
    }
    let accessible = pairs
        .iter()
        .map(|(label, value)| format!("{label}: {value}"))
        .collect::<Vec<_>>()
        .join(", ");
    w::semantic_status(ui, card, ("security_facts", id), &accessible, false);
}

/// One detail line; `None` omits the evidence rail for non-verdict failures.
fn detail_line(
    ui: &mut Ui,
    id: impl std::hash::Hash + std::fmt::Debug,
    at: Pos2,
    w: f32,
    text: &str,
    ev: Option<Evidence>,
    state: &State,
) {
    let p = &state.palette;
    let painter = ui.painter().clone();
    if let Some(ev) = ev {
        rail::paint(
            &painter,
            Rect::from_min_size(Pos2::new(at.x, at.y - 7.0), Vec2::new(3.0, 14.0)),
            ev,
            p,
            state.ppp,
        );
    }
    let g = w::elide_galley(&painter, text, role::meta_mono(), w - 14.0, p.text2);
    painter.galley(Pos2::new(at.x + 14.0, at.y - g.size().y * 0.5), g, p.text2);
    w::semantic_status(
        ui,
        Rect::from_min_size(Pos2::new(at.x, at.y - 10.0), Vec2::new(w, 20.0)),
        ("security_detail", id),
        text,
        matches!(ev, Some(Evidence::Tampered)),
    );
}

/// Rewrap the same vault key only after proving the current passphrase.
fn rotate_card(
    ui: &mut Ui,
    painter: &Painter,
    card_rect: Rect,
    state: &State,
    out: &mut Vec<Action>,
) -> f32 {
    let p = &state.palette;
    let col_w = card_rect.width();
    match &state.rotate {
        RotateState::Closed => return card_rect.top(),
        RotateState::Working
        | RotateState::Done
        | RotateState::Warning(_)
        | RotateState::Failed(_) => {
            card(painter, card_rect, "Change passphrase", p);
            let (line, ev) = match &state.rotate {
                RotateState::Working => (
                    "Deriving new passphrase protection. This step cannot be cancelled.".to_owned(),
                    Evidence::Unverified,
                ),
                RotateState::Done => (
                    "Passphrase changed. The old one no longer opens this vault.".to_owned(),
                    Evidence::Verified,
                ),
                RotateState::Warning(e) => (e.to_string(), Evidence::Unverified),
                RotateState::Failed(e) => (e.to_string(), Evidence::Tampered),
                RotateState::Closed | RotateState::Open(_) => unreachable!("matched above"),
            };
            detail_line(
                ui,
                "rotation",
                Pos2::new(card_rect.left() + metrics::CARD_PAD, card_rect.top() + 50.0),
                col_w - metrics::CARD_PAD * 2.0,
                &line,
                Some(ev),
                state,
            );
            return card_rect.bottom() + 16.0;
        }
        RotateState::Open(_) => {}
    }

    card(painter, card_rect, "Change passphrase", p);
    let form = state.rotate.form().expect("the open form, matched above");
    let fx = card_rect.left() + metrics::CARD_PAD;
    let field_w = col_w - metrics::CARD_PAD * 2.0;
    let mut fy = card_rect.top() + 44.0;

    // A mismatch marks only the confirmation field.
    let mismatched = !form.confirm.is_empty() && form.confirm != form.next;
    for (caption, accessible_label, value, field, refused) in [
        (
            "CURRENT PASSPHRASE",
            "Current vault passphrase",
            &form.current,
            SecretField::RotateCurrent,
            false,
        ),
        (
            "NEW PASSPHRASE",
            "New vault passphrase",
            &form.next,
            SecretField::RotateNext,
            false,
        ),
        (
            "CONFIRM NEW PASSPHRASE",
            "Confirm new vault passphrase",
            &form.confirm,
            SecretField::RotateConfirm,
            mismatched,
        ),
    ] {
        w::text_left(painter, Pos2::new(fx, fy), caption, role::meta(), p.text3);
        fy += 16.0;
        let field_out = ui::secret(
            ui,
            Rect::from_min_size(Pos2::new(fx, fy), Vec2::new(field_w, 32.0)),
            ui::Secret {
                id: egui::Id::new(("rotate_secret", caption)),
                label: accessible_label,
                value,
                placeholder: "at least 12 characters",
                revealed: state.revealed == Some(field),
                refused,
                frozen: state.palette_open,
                autofocus: field == SecretField::RotateCurrent,
            },
            p,
        );
        if let Some(next) = field_out.edited {
            out.push(Action::SetSecret(field, next));
        }
        if field_out.toggled {
            out.push(Action::ToggleReveal(field));
        }
        fy += 32.0 + 10.0;
    }

    // Keep the blocker in the stable slot above the buttons.
    if let Some(blocker) = form.blocker() {
        w::text_left(
            painter,
            Pos2::new(fx, fy + 6.0),
            blocker,
            role::meta(),
            p.text3,
        );
        w::semantic_status(
            ui,
            Rect::from_min_size(Pos2::new(fx, fy), Vec2::new(field_w, 20.0)),
            "rotation_blocker",
            blocker,
            false,
        );
    }
    fy += 24.0;

    let rotate = ui::Btn::new("Change passphrase", ui::ButtonKind::Primary);
    let rotate = match form.blocker() {
        Some(blocker) => rotate.unavailable(blocker),
        None => rotate,
    };
    if ui::button(
        ui,
        Rect::from_min_size(Pos2::new(fx, fy), Vec2::new(180.0, 32.0)),
        rotate,
        p,
    )
    .clicked()
    {
        out.push(Action::SubmitRotate);
    }
    if ui::button(
        ui,
        Rect::from_min_size(Pos2::new(fx + 192.0, fy), Vec2::new(100.0, 32.0)),
        ui::Btn::new("Cancel", ui::ButtonKind::Ghost),
        p,
    )
    .clicked()
    {
        out.push(Action::CancelRotate);
    }
    card_rect.bottom() + 16.0
}

/// An explicit full-disk integrity walk and its findings.
fn integrity_card(
    ui: &mut Ui,
    painter: &Painter,
    card_rect: Rect,
    state: &State,
    out: &mut Vec<Action>,
) -> f32 {
    let p = &state.palette;
    let (y, col_w) = (card_rect.top(), card_rect.width());
    card(painter, card_rect, "Integrity", p);

    let checking = matches!(state.integrity, IntegrityState::Checking);
    let (label, action) = integrity_button(checking);
    let btn = ui::Btn::new(label, ui::ButtonKind::Outlined).tip(if checking {
        "Stops after the current storage page"
    } else {
        "Authenticates every storage page in the vault"
    });
    if ui::button(
        ui,
        Rect::from_min_size(
            Pos2::new(card_rect.right() - 130.0 - metrics::CARD_PAD, y + 12.0),
            Vec2::new(130.0, 30.0),
        ),
        btn,
        p,
    )
    .clicked()
    {
        out.push(action);
    }

    let (summary, ev) = match &state.integrity {
        IntegrityState::None => (
            "Not verified. Vault verification reads every storage page.".to_owned(),
            Some(Evidence::Unverified),
        ),
        IntegrityState::Checking => (
            "Authenticating every storage page. Cancel stops at the next page.".to_owned(),
            Some(Evidence::Unverified),
        ),
        // An incomplete walk is an operation error, not an evidence verdict.
        IntegrityState::Failed(e) => (e.to_string(), None),
        IntegrityState::Checked(f) if f.errors.is_empty() => (
            format!(
                "{} pages checked, no errors.",
                w::thousands(f.pages_checked)
            ),
            Some(Evidence::Verified),
        ),
        IntegrityState::Checked(f) => (
            format!(
                "{} problems in {} pages, {} of them altered bytes.",
                f.errors.len(),
                w::thousands(f.pages_checked),
                f.tampered()
            ),
            integrity_evidence(f.tampered() > 0),
        ),
    };
    detail_line(
        ui,
        "integrity_summary",
        Pos2::new(card_rect.left() + metrics::CARD_PAD, y + 62.0),
        col_w - metrics::CARD_PAD * 2.0,
        &summary,
        ev,
        state,
    );

    if let IntegrityState::Checked(f) = &state.integrity {
        for (i, err) in f.errors.iter().enumerate() {
            detail_line(
                ui,
                ("integrity_error", i),
                Pos2::new(
                    card_rect.left() + metrics::CARD_PAD,
                    y + 62.0 + (i + 1) as f32 * Plan::LINE,
                ),
                col_w - metrics::CARD_PAD * 2.0,
                &err.message,
                integrity_evidence(err.tampered),
                state,
            );
        }
    }
    card_rect.bottom() + 16.0
}

fn integrity_button(checking: bool) -> (&'static str, Action) {
    if checking {
        ("Cancel", Action::CancelQuery)
    } else {
        ("Verify vault", Action::CheckIntegrity)
    }
}

fn integrity_evidence(tampered: bool) -> Option<Evidence> {
    tampered.then_some(Evidence::Tampered)
}

/// The audit log, across every segment it has rotated through.
fn audit_card(ui: &mut Ui, painter: &Painter, card_rect: Rect, state: &State) -> f32 {
    let p = &state.palette;
    let (y, col_w) = (card_rect.top(), card_rect.width());
    card(painter, card_rect, "Audit log", p);

    let Some(audit) = &state.vault().facts.audit else {
        detail_line(
            ui,
            "audit_summary",
            Pos2::new(card_rect.left() + metrics::CARD_PAD, y + 50.0),
            col_w - metrics::CARD_PAD * 2.0,
            "This vault keeps no audit log.",
            Some(Evidence::NotAttestable),
            state,
        );
        return card_rect.bottom() + 16.0;
    };

    let pairs: [(&str, String); 3] = [
        ("entries", w::thousands(audit.entries)),
        ("segments", audit.segments.len().to_string()),
        (
            "chain",
            if audit.chain_links() {
                "links".to_owned()
            } else {
                "broken".to_owned()
            },
        ),
    ];
    pair_grid(ui, painter, "audit", card_rect, col_w, p, &pairs);

    let mut lines: Vec<(String, Evidence)> = audit
        .segments
        .iter()
        .filter(|s| !s.chain_valid)
        .map(|seg| match seg.break_at {
            Some(n) => (
                format!("{} breaks at entry {}", seg.name, n),
                Evidence::Tampered,
            ),
            None => (format!("{} does not verify", seg.name), Evidence::Tampered),
        })
        .collect();

    // Mutable header counts are consistency warnings, not authentication verdicts.
    let shortfall = audit.count_shortfall();
    if shortfall > 0 {
        lines.push((
            format!(
                "header count exceeds readable entries by {} (count is not authenticated)",
                w::thousands(shortfall)
            ),
            Evidence::Unverified,
        ));
    }

    for (i, (line, evidence)) in lines.iter().enumerate() {
        detail_line(
            ui,
            ("audit_detail", i),
            Pos2::new(
                card_rect.left() + metrics::CARD_PAD,
                y + 96.0 + i as f32 * Plan::LINE,
            ),
            col_w - metrics::CARD_PAD * 2.0,
            line,
            Some(*evidence),
            state,
        );
    }
    card_rect.bottom() + 16.0
}

/// Paint the column in the native scroll area's content coordinates.
fn column(ui: &mut Ui, rect: Rect, state: &State, plan: &Plan, out: &mut Vec<Action>) {
    let p = &state.palette;
    let vault = state.vault();
    let ppp = state.ppp;
    // Avoid holding a Ui borrow across controls that need `&mut Ui`.
    let painter = &ui.painter().clone();

    let col_w = column_width(rect);
    let x = rect.center().x - col_w * 0.5;
    let top = rect.top() + 36.0;
    let mut y = top;

    w::text_left(
        painter,
        Pos2::new(x, y + 10.0),
        "Vault security",
        role::headline(),
        p.text1,
    );
    if vault.is_demo {
        w::text_right(
            painter,
            Pos2::new(x + col_w, y + 10.0),
            "Disposable demo · passphrase is fixed",
            role::meta(),
            p.text3,
        );
    } else if !matches!(state.rotate, RotateState::Open(_)) {
        let rotating = matches!(state.rotate, RotateState::Working);
        let btn = ui::Btn::new("Change passphrase", ui::ButtonKind::Outlined);
        let btn = if rotating {
            btn.unavailable("Changing the passphrase")
        } else {
            btn
        };
        if ui::button(
            ui,
            Rect::from_min_size(Pos2::new(x + col_w - 160.0, y), Vec2::new(160.0, 32.0)),
            btn,
            p,
        )
        .clicked()
        {
            out.push(Action::BeginRotate);
        }
    }
    y += 46.0;
    painter.rect_filled(
        Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, 1.0)),
        0.0,
        p.hairline,
    );
    y += 22.0;

    let counts = vault.counts();
    if counts.unverified > 0 {
        let banner = Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, 42.0));
        w::surface(
            painter,
            banner,
            p.ground3,
            Some(p.hairline_strong),
            radius::CARD,
        );
        warn_glyph(
            painter,
            Pos2::new(banner.left() + 20.0, banner.center().y),
            p.missing,
        );
        w::text_left(
            painter,
            Pos2::new(banner.left() + 38.0, banner.center().y),
            &format!(
                "{} atoms read this session have not been verified.",
                counts.unverified
            ),
            role::chrome(),
            p.text1,
        );
        let warning = format!(
            "{} atoms read this session have not been verified.",
            counts.unverified
        );
        w::semantic_status(ui, banner, "unverified_atoms", &warning, true);
        y = banner.bottom() + 20.0;
    }

    let cipher = Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, 118.0));
    card(painter, cipher, "Cipher and key derivation", p);
    // Header facts were captured before the open handle took its exclusive lock.
    let facts = &vault.facts;
    let pairs: [(&str, String); 4] = [
        ("cipher", facts.key_file.cipher.as_str().to_owned()),
        ("kdf", facts.key_file.kdf.as_str().to_owned()),
        ("pages", w::thousands(facts.stats.total_pages as u64)),
        (
            "merkle root",
            crate::model::digest_label(&facts.stats.merkle_root),
        ),
    ];
    pair_grid(ui, painter, "cipher", cipher, col_w, p, &pairs);
    y = cipher.bottom() + 16.0;

    let at = |y: f32, h: f32| Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, h));
    if !vault.is_demo {
        y = rotate_card(ui, painter, at(y, plan.rotate_h()), state, out);
    }
    y = integrity_card(ui, painter, at(y, plan.integrity_h()), state, out);
    y = audit_card(ui, painter, at(y, plan.audit_h()), state);

    let att = Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, 150.0));
    card(painter, att, "Attestation", p);
    let series: [(Evidence, u32); 6] = [
        (Evidence::Verified, counts.verified),
        (Evidence::Unverified, counts.unverified),
        (Evidence::Erased, counts.erased),
        (Evidence::Tampered, counts.tampered),
        (Evidence::Missing, counts.missing),
        (Evidence::NotAttestable, counts.not_attestable),
    ];
    let total = counts.total().max(1);
    let bar = Rect::from_min_size(
        Pos2::new(att.left() + metrics::CARD_PAD, att.top() + 44.0),
        Vec2::new(col_w - 40.0, 8.0),
    );
    let mut bx = bar.left();
    for (ev, n) in series {
        let seg_w = bar.width() * n as f32 / total as f32;
        painter.rect_filled(
            Rect::from_min_size(Pos2::new(bx, bar.top()), Vec2::new(seg_w, bar.height())),
            0.0,
            ev.colour(p).unwrap_or(p.hairline),
        );
        bx += seg_w;
    }
    for (i, (ev, n)) in series.into_iter().enumerate() {
        let lx = att.left() + metrics::CARD_PAD + (i % 3) as f32 * ((col_w - 40.0) / 3.0);
        let ly = att.top() + 74.0 + (i / 3) as f32 * 26.0;
        rail::paint(
            painter,
            Rect::from_min_size(Pos2::new(lx, ly - 7.0), Vec2::new(3.0, 14.0)),
            ev,
            p,
            ppp,
        );
        w::text_left(
            painter,
            Pos2::new(lx + 14.0, ly),
            ev.label(),
            role::meta(),
            p.text2,
        );
        w::text_right(
            painter,
            Pos2::new(lx + (col_w - 40.0) / 3.0 - 24.0, ly),
            &w::thousands(n as u64),
            role::meta_mono(),
            p.text1,
        );
    }
    let attestation = format!(
        "Attestation: {} verified, {} unverified, {} erased, {} tampered, {} missing, {} not attestable",
        counts.verified,
        counts.unverified,
        counts.erased,
        counts.tampered,
        counts.missing,
        counts.not_attestable
    );
    w::semantic_status(
        ui,
        att,
        "attestation_counts",
        &attestation,
        counts.tampered > 0,
    );
    y = att.bottom() + 16.0;

    // Draw every session receipt; the pane scrolls instead of truncating them.
    let content_w = col_w - metrics::CARD_PAD * 2.0;
    let rows_bottom = Plan::HEAD + plan.receipts as f32 * Plan::ROW;
    let rec = Rect::from_min_size(Pos2::new(x, y), Vec2::new(col_w, plan.receipts_h()));
    card(painter, rec, "Erasure receipts · this session", p);

    let destroyed = match vault.facts.keys.region {
        Some(c) => tombstone_summary(u64::from(c.tombstoned)),
        None => "No key store: cryptographic erasure is unavailable".to_owned(),
    };
    w::text_right(
        painter,
        Pos2::new(rec.right() - metrics::CARD_PAD, rec.top() + 22.0),
        &destroyed,
        role::meta(),
        p.text3,
    );

    // Session receipts commonly start empty; omit table chrome until one exists.
    if vault.receipts.is_empty() {
        w::text_left(
            painter,
            Pos2::new(rec.left() + metrics::CARD_PAD, rec.top() + 50.0),
            "No erasures recorded in this Studio session. Receipts are not persisted.",
            role::meta(),
            p.text4,
        );
        w::semantic_status(
            ui,
            rec,
            "empty_erasure_receipts",
            "No erasures recorded in this Studio session. Receipts are not persisted.",
            false,
        );
        return;
    }

    // Proportional columns fit the card; receipts carry no durable sequence number.
    const COLS: [(&str, f32); 6] = [
        ("region", 0.18),
        ("atoms", 0.09),
        ("algorithm", 0.24),
        ("fsync", 0.09),
        ("readback", 0.18),
        ("issued", 0.22),
    ];
    let left = rec.left() + metrics::CARD_PAD;
    let mut hx = left;
    for (h, weight) in COLS {
        w::text_left(
            painter,
            Pos2::new(hx, rec.top() + 46.0),
            &h.to_uppercase(),
            role::column_header(),
            p.text3,
        );
        hx += content_w * weight;
    }
    painter.rect_filled(
        Rect::from_min_size(Pos2::new(left, rec.top() + 56.0), Vec2::new(content_w, 1.0)),
        0.0,
        p.hairline,
    );

    for (r, receipt) in vault.receipts.iter().enumerate() {
        let ry = rec.top() + Plan::HEAD + r as f32 * Plan::ROW;
        let readback = receipt.readback();
        let readback_ink = match readback {
            Readback::NotApplicable => p.text3,
            Readback::Unconfirmed => p.missing,
            Readback::Confirmed => p.text1,
        };
        let cells: [(String, Color32); 6] = [
            (receipt.region.clone(), p.text1),
            (receipt.atoms().to_string(), p.text1),
            (receipt.algorithm().to_owned(), p.text1),
            (
                if receipt.receipt.fsync { "yes" } else { "no" }.to_owned(),
                p.text1,
            ),
            (readback.label().to_owned(), readback_ink),
            (receipt.issued.clone(), p.text1),
        ];
        // Region names and microsecond timestamps are elided to their columns.
        let mut cx = left;
        for ((text, colour), (_, weight)) in cells.into_iter().zip(COLS) {
            let slot = content_w * weight;
            let fitted = w::elide_galley(painter, &text, role::cell_compact(), slot - 8.0, colour);
            painter.galley(Pos2::new(cx, ry - fitted.size().y * 0.5), fitted, colour);
            cx += slot;
        }
        painter.rect_filled(
            Rect::from_min_size(Pos2::new(left, ry + 11.0), Vec2::new(content_w, 1.0)),
            0.0,
            p.hairline_quiet,
        );
        let accessible = format!(
            "Erasure receipt {}: region {}, {} atoms, algorithm {}, fsync {}, readback {}, issued {}",
            r + 1,
            receipt.region,
            receipt.atoms(),
            receipt.algorithm(),
            if receipt.receipt.fsync { "yes" } else { "no" },
            readback.label(),
            receipt.issued
        );
        w::semantic_status(
            ui,
            Rect::from_min_size(Pos2::new(left, ry - 11.0), Vec2::new(content_w, 22.0)),
            ("erasure_receipt", r),
            &accessible,
            false,
        );
    }
    if let Some(note) = plan.note.clone() {
        painter.galley(
            Pos2::new(left, rec.top() + rows_bottom + 10.0),
            note,
            p.text4,
        );
    }
}

fn warn_glyph(painter: &Painter, c: Pos2, colour: Color32) {
    let stroke = egui::Stroke::new(1.4, colour);
    painter.circle_stroke(c, 8.0, stroke);
    painter.line_segment(
        [Pos2::new(c.x, c.y - 4.0), Pos2::new(c.x, c.y + 1.0)],
        stroke,
    );
    painter.circle_filled(Pos2::new(c.x, c.y + 4.0), 1.1, colour);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integrity_button_cancels_the_walk_only_while_it_is_running() {
        let (label, action) = integrity_button(false);
        assert_eq!(label, "Verify vault");
        assert!(matches!(action, Action::CheckIntegrity));

        let (label, action) = integrity_button(true);
        assert_eq!(label, "Cancel");
        assert!(matches!(action, Action::CancelQuery));
    }

    #[test]
    fn structural_integrity_problems_get_no_authentication_verdict() {
        assert_eq!(integrity_evidence(false), None);
        assert_eq!(integrity_evidence(true), Some(Evidence::Tampered));
    }

    #[test]
    fn tombstone_copy_does_not_claim_a_lifetime_erasure_count() {
        assert_eq!(
            tombstone_summary(0),
            "0 key slots currently tombstoned; lifetime total unavailable"
        );
        assert_eq!(
            tombstone_summary(1),
            "1 key slot currently tombstoned; lifetime total unavailable"
        );
    }
}
