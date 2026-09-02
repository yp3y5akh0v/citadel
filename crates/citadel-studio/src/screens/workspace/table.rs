//! Table browser; SQL rows carry no per-atom evidence rail.

use super::super::*;
use crate::fonts::role;
use crate::grid::{self, Align, Cell, Column, Row};
use crate::state::{Action, State};
use crate::theme::{metrics, Evidence};
use crate::widgets as w;
use egui::{Pos2, Rect, Ui, Vec2};

pub(super) fn table_pane(
    ui: &mut Ui,
    centre: Rect,
    state: &State,
    name: &str,
    out: &mut Vec<Action>,
) {
    let p = &state.palette;
    let vault = state.vault();
    let Some(table) = vault.tables.iter().find(|t| t.name == name) else {
        return;
    };

    let bar = Rect::from_min_size(
        centre.left_top(),
        Vec2::new(centre.width(), metrics::TOOLBAR_H),
    );
    paint_toolbar(ui.painter(), bar, p);
    let mut tx = bar.left() + 12.0;
    let title = w::elide_galley(
        ui.painter(),
        name,
        role::chrome(),
        (bar.width() - 150.0).max(0.0),
        p.text1,
    );
    let title_w = title.size().x;
    ui.painter().galley(
        Pos2::new(tx, bar.center().y - title.size().y * 0.5),
        title,
        p.text1,
    );
    tx += title_w + 10.0;
    w::chip(
        ui.painter(),
        Pos2::new(tx, bar.center().y),
        &format!("{} rows", w::thousands(table.rows)),
        p,
        p.text3,
        None,
        bar.right() - 12.0 - tx,
    );

    let action = Rect::from_min_size(
        Pos2::new(centre.left(), centre.bottom() - 30.0),
        Vec2::new(centre.width(), 30.0),
    );
    let body = Rect::from_min_max(
        Pos2::new(centre.left(), bar.bottom()),
        Pos2::new(centre.right(), action.top()),
    );

    let (columns, rows, browse) = browsed(state, name);
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

    w::fill(ui.painter(), action, p.ground3);
    w::hairline_top(ui.painter(), action, p.hairline);
    // Evidence guidance applies only to displayed data rows.
    // Count only data rows, not synthetic failure rows.
    let counted = match browse {
        Browse::Reading => "reading...".to_owned(),
        Browse::Failed => format!("{} rows, none read", w::thousands(table.rows)),
        // Distinguish rows read under the browse limit from the table total.
        Browse::Rows(n) => format!(
            "showing {} of {}",
            w::thousands(n as u64),
            w::thousands(table.rows)
        ),
    };
    let count_w = ui
        .painter()
        .layout_no_wrap(counted.clone(), role::meta_mono(), p.text3)
        .size()
        .x;
    w::text_right(
        ui.painter(),
        Pos2::new(action.right() - 12.0, action.center().y),
        &counted,
        role::meta_mono(),
        p.text3,
    );
    if matches!(browse, Browse::Rows(_)) {
        let guidance = "SQL rows have no per-row attestation.";
        let room = action.width() - count_w - 44.0;
        if room > 80.0 {
            let galley = w::elide_galley(ui.painter(), guidance, role::meta(), room, p.text4);
            ui.painter().galley(
                Pos2::new(
                    action.left() + 12.0,
                    action.center().y - galley.size().y * 0.5,
                ),
                galley,
                p.text4,
            );
        }
    }
}

/// Semantic grid state used by the footer.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Browse {
    /// A column-less placeholder means the read is in flight.
    Reading,
    /// The grid holds one synthetic row carrying the engine's message, not data.
    Failed,
    Rows(usize),
}

/// Browsed rows with statement-derived columns, or a pending placeholder.
fn browsed(state: &State, name: &str) -> (Vec<Column>, Vec<Row>, Browse) {
    let Some(browsed) = state.browse.as_ref().filter(|b| b.table == name) else {
        return (
            vec![Column::flex("reading", Align::Left)],
            Vec::new(),
            Browse::Reading,
        );
    };
    let result = match &browsed.rows {
        // Successful statements have columns; a column-less result is the pending marker.
        Ok(result) if result.columns.is_empty() => {
            return (
                vec![Column::flex("reading", Align::Left)],
                Vec::new(),
                Browse::Reading,
            )
        }
        Ok(result) => result,
        // Put failures in the row surface so they cannot resemble an empty table.
        Err(error) => {
            return (
                vec![Column::flex(error.kind.headline(), Align::Left)],
                vec![Row::new(
                    Evidence::NotAttestable,
                    vec![Cell::new(error.detail.clone())],
                )],
                Browse::Failed,
            )
        }
    };
    let columns = result
        .columns
        .iter()
        .map(|c| Column::flex(c, Align::Left))
        .collect();
    let rows = result
        .rows
        .iter()
        .map(|row| {
            let cells = row
                .iter()
                .map(|value| Cell::new(crate::engine::value::cell(value).display()))
                .collect();
            // SQL rows have no per-row proof.
            Row::new(Evidence::NotAttestable, cells)
        })
        .collect::<Vec<Row>>();
    let n = rows.len();
    (columns, rows, Browse::Rows(n))
}
