//! Shared workspace chrome around the focused document.

mod objects;
mod region;
mod table;

use objects::{collapsed_objects, objects};
use region::{atom_inspector, forget_confirmation, region_pane};
use table::table_pane;

use super::*;
use crate::state::{Action, Doc, State};
use crate::theme::metrics;
use crate::ui;
use crate::widgets as w;
use egui::{Pos2, Rect, Ui, Vec2};

/// Minimum useful width of the data pane.
const CENTRE_MIN: f32 = 620.0;
/// Minimum body height that can show the complete atom inspector without clipping.
const INSPECTOR_MIN_H: f32 = 590.0;

/// How much horizontal room the side panes get at this window width.
struct Panes {
    sidebar: f32,
    inspector: f32,
}

fn panes(body_size: Vec2, wants_inspector: bool) -> Panes {
    let inspector_full = if wants_inspector && body_size.y >= INSPECTOR_MIN_H {
        metrics::INSPECTOR_W
    } else {
        0.0
    };
    // Preserve the data pane first; retain the inspector before expanded navigation
    // because it carries verdict scope.
    if body_size.x - metrics::SIDEBAR_W - inspector_full >= CENTRE_MIN {
        return Panes {
            sidebar: metrics::SIDEBAR_W,
            inspector: inspector_full,
        };
    }
    if body_size.x - metrics::SIDEBAR_RAIL_W - inspector_full >= CENTRE_MIN {
        return Panes {
            sidebar: metrics::SIDEBAR_RAIL_W,
            inspector: inspector_full,
        };
    }
    if body_size.x - metrics::SIDEBAR_W >= CENTRE_MIN {
        return Panes {
            sidebar: metrics::SIDEBAR_W,
            inspector: 0.0,
        };
    }
    Panes {
        sidebar: metrics::SIDEBAR_RAIL_W,
        inspector: 0.0,
    }
}

pub fn workspace(ui: &mut Ui, full: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    // The workspace is unreachable without an open vault.
    if state.vault.is_none() {
        return;
    }
    let s = Shell::new(full);
    w::fill(ui.painter(), full, p.ground1);

    band(ui, s.band, state, out);
    tabs(ui, s.tabs, state, out);

    let wants_inspector = matches!(
        state.active_doc(),
        Some(Doc::Region(_)) | Some(Doc::Vector(_))
    );
    let lanes = panes(s.body.size(), wants_inspector);

    let side = Rect::from_min_size(s.body.left_top(), Vec2::new(lanes.sidebar, s.body.height()));
    if lanes.sidebar > metrics::SIDEBAR_RAIL_W {
        objects(ui, side, state, out);
    } else {
        collapsed_objects(ui, side, state, out);
    }

    let centre_right = if lanes.inspector > 0.0 {
        let insp = inspector(ui.painter(), s.body, p);
        match state.active_doc() {
            Some(Doc::Region(_)) => atom_inspector(ui, insp, state, out),
            _ => vector_inspector_pane(ui.painter(), insp, state),
        }
        insp.left()
    } else {
        s.body.right()
    };

    let centre = Rect::from_min_max(
        Pos2::new(side.right(), s.body.top()),
        Pos2::new(centre_right, s.body.bottom()),
    );

    match state.active_doc() {
        Some(Doc::Region(name)) => {
            region_pane(ui, centre, state, name, lanes.inspector == 0.0, out)
        }
        Some(Doc::Security) => super::security::pane(ui, centre, state, out),
        Some(Doc::Query) => super::query::pane(ui, centre, state, out),
        Some(Doc::Vector(_)) => super::vector::pane(ui, centre, state, out),
        Some(Doc::Table(name)) => {
            let name = name.clone();
            table_pane(ui, centre, state, &name, out)
        }
        None => {}
    }

    status(ui, s.status, state);
    if state.forget_prompt.is_some() {
        forget_confirmation(ui, full, state, out);
    }
}

const TAB_W_MAX: f32 = 200.0;
/// Minimum tab width before the strip scrolls to preserve distinguishing title tails.
const TAB_W_MIN: f32 = 160.0;

fn tabs(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    w::fill(ui.painter(), rect, p.ground3);
    w::hairline_bottom(ui.painter(), rect, p.hairline);

    // Flowing tabs never overlap the pinned tab.
    let pinned: Vec<usize> = (0..state.docs.len())
        .filter(|i| state.docs[*i].pinned_right())
        .collect();
    let flowing: Vec<usize> = (0..state.docs.len())
        .filter(|i| !state.docs[*i].pinned_right())
        .collect();
    let right = rect.right() - pinned.len() as f32 * TAB_W_MAX;
    let room = (right - rect.left()).max(0.0);

    let width = if flowing.is_empty() {
        TAB_W_MAX
    } else {
        (room / flowing.len() as f32).clamp(TAB_W_MIN, TAB_W_MAX)
    };
    // Scroll overflowing tabs around the focused document; at extreme widths preserve
    // only the pinned tab rather than overlapping controls.
    let fits = (room / width).floor().max(0.0) as usize;
    let first = flowing
        .iter()
        .position(|i| *i == state.active)
        .map_or(0, |at| at.saturating_sub(fits.saturating_sub(1)))
        .min(flowing.len().saturating_sub(fits));

    let mut x = rect.left();
    for &i in flowing.iter().skip(first).take(fits) {
        let r = Rect::from_min_size(Pos2::new(x, rect.top()), Vec2::new(width, rect.height()));
        x += width;
        tab(ui, r, i, state, out);
    }
    let mut px = rect.right();
    for &i in &pinned {
        px -= TAB_W_MAX;
        let r = Rect::from_min_size(
            Pos2::new(px, rect.top()),
            Vec2::new(TAB_W_MAX, rect.height()),
        );
        tab(ui, r, i, state, out);
    }
}

fn tab(ui: &mut Ui, r: Rect, i: usize, state: &State, out: &mut Vec<Action>) {
    let (focus, close) = ui::tab(
        ui,
        r,
        &state.docs[i].title(),
        i == state.active,
        &state.palette,
    );
    if close.clicked() {
        out.push(Action::CloseDoc(i));
    } else if focus.clicked() {
        out.push(Action::FocusDoc(i));
    }
}

fn vector_inspector_pane(painter: &egui::Painter, insp: Rect, state: &State) {
    super::vector::inspector_body(painter, insp, state);
}

fn status(ui: &mut Ui, rect: Rect, state: &State) {
    let p = &state.palette;
    let vault = state.vault();
    let c = vault.counts();
    let path = if vault.is_demo {
        "Disposable demo · changes reset when closed".to_owned()
    } else {
        state
            .session
            .opened()
            .map_or_else(String::new, |o| o.path.display().to_string())
    };
    let cipher = crate::model::cipher_label(&vault.facts.key_file);
    // Use the shared build label.
    let left = [path.as_str(), super::BUILD_LABEL, cipher.as_str()];
    // Evidence counts cover only data read by this session.
    let vault_line = format!(
        "{} of {} read authentic",
        w::thousands(c.verified as u64),
        w::thousands(c.total() as u64)
    );
    // Preserve the audit log's distinction between broken and truncated histories.
    let audit = match &vault.facts.audit {
        Some(a) if !a.chain_links() => format!(
            "audit {} entries{}chain broken",
            w::thousands(a.entries),
            w::SEP
        ),
        Some(a) if a.count_shortfall() > 0 => format!(
            "audit {} entries{}count short by {}",
            w::thousands(a.entries),
            w::SEP,
            w::thousands(a.count_shortfall())
        ),
        Some(a) => format!("audit {} entries", w::thousands(a.entries)),
        None => "no audit log".to_owned(),
    };
    let page = state.page_check_label();
    let mut right = Vec::with_capacity(3);
    if matches!(state.active_doc(), Some(Doc::Region(_))) {
        right.push(page.as_str());
    }
    right.extend([vault_line.as_str(), audit.as_str()]);
    w::status_bar(ui, rect, p, &left, &right);
}
