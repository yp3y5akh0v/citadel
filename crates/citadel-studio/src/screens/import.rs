//! Read-only SQLite schema inspection and import statement generation.

use super::*;
use crate::fonts::role;
use crate::grid::{self, Align, Cell, Column, Row};
use crate::sqlite::SourceTable;
use crate::state::{Action, ImportProgress, Source, State};
use crate::theme::{radius, Evidence};
use crate::ui;
use crate::widgets as w;
use egui::{Pos2, Rect, Ui, Vec2};

/// What the generated import can create from the inspected SQLite schema.
const SCHEMA_ONLY: &str = "Schema only. These statements create the tables, not their rows";

pub fn import(ui: &mut Ui, full: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    let s = Shell::new(full);
    w::fill(ui.painter(), full, p.ground1);
    band(ui, s.band, state, out);

    let (read, held) = match &state.source {
        Source::Read { tables, .. } => (
            "read".to_owned(),
            format!(
                "{} table{}",
                w::thousands(tables.len() as u64),
                if tables.len() == 1 { "" } else { "s" }
            ),
        ),
        Source::Reading { .. } => ("reading".to_owned(), "nothing read yet".to_owned()),
        Source::Failed { .. } => ("not read".to_owned(), "nothing read".to_owned()),
        Source::None => ("no source chosen".to_owned(), "nothing read".to_owned()),
    };
    let path = state.source.path().map(|p| p.display().to_string());
    let left: Vec<&str> = match &path {
        Some(path) => vec![path, &read],
        None => vec![&read],
    };
    w::status_bar(ui, s.status, p, &left, &[&held, super::BUILD_LABEL]);

    let ui = &mut ui.new_child(egui::UiBuilder::new().max_rect(s.body));
    ui.set_clip_rect(s.body);
    let head = header(ui, s.body, state, out);

    let action = Rect::from_min_size(
        Pos2::new(s.body.left(), s.body.bottom() - ACTION_H),
        Vec2::new(s.body.width(), ACTION_H),
    );
    let body = Rect::from_min_max(
        Pos2::new(s.body.left(), head),
        Pos2::new(s.body.right(), action.top()),
    );
    match &state.source {
        Source::Read { tables, .. } if !tables.is_empty() => {
            source_grid(ui, body, tables, state, out)
        }
        // Loading and failure states are not empty schemas.
        _ => notice(ui, body, state),
    }
    action_bar(ui, action, state, out);
}

/// Height of the bar carrying Cancel and Import.
const ACTION_H: f32 = 56.0;

/// Maximum source-row width.
const ROW_MAX: f32 = 720.0;

/// Title, the sentence under it, and the source picker. Returns the y the body starts at.
fn header(ui: &mut Ui, body: Rect, state: &State, out: &mut Vec<Action>) -> f32 {
    let p = &state.palette;
    let x = body.left() + 24.0;
    let mut y = body.top() + 26.0;

    stack_glyph(ui.painter(), Pos2::new(x + 11.0, y + 4.0), p.accent);
    w::text_left(
        ui.painter(),
        Pos2::new(x + 36.0, y + 8.0),
        "Import SQLite schema",
        role::headline(),
        p.text1,
    );
    y += 30.0;
    // Wrap explanatory text within the form column.
    let note = ui.painter().layout(
        "The file below is opened read-only. Studio reads its table definitions and row \
         counts; source rows are not copied."
            .to_owned(),
        role::body(),
        p.text2,
        (body.width() - 48.0).min(ROW_MAX),
    );
    let note_h = note.size().y;
    ui.painter().galley(Pos2::new(x, y), note, p.text2);
    y += note_h + 12.0;

    // Cap the path row on wide windows.
    let row_w = (body.width() - 48.0).min(ROW_MAX);
    let shown = Rect::from_min_size(Pos2::new(x, y), Vec2::new(row_w - 110.0, 32.0));
    let button = Rect::from_min_size(Pos2::new(x + row_w - 100.0, y), Vec2::new(100.0, 32.0));
    let choose = ui::Btn::new("Choose", ui::ButtonKind::Outlined);
    let choose = if state.import_progress.busy() {
        choose.unavailable("Wait for table creation to finish")
    } else if matches!(&state.source, Source::Reading { .. }) {
        choose.unavailable("Wait for the current source read to finish")
    } else {
        choose
    };
    if ui::button(ui, button, choose, p).clicked() {
        // The effect layer owns the dialog and asynchronous source read.
        out.push(Action::ChooseImportSource);
    }
    w::surface(
        ui.painter(),
        shown,
        p.ground0,
        Some(p.hairline_strong),
        radius::CONTROL,
    );
    let (text, ink) = match state.source.path() {
        Some(path) => (path.display().to_string(), p.text1),
        None => ("choose a .db, .sqlite or .sqlite3 file".to_owned(), p.text4),
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
    y + 32.0 + 20.0
}

fn source_grid(
    ui: &mut Ui,
    body: Rect,
    tables: &[SourceTable],
    state: &State,
    out: &mut Vec<Action>,
) {
    let p = &state.palette;
    let columns = [
        Column::fixed("table", 240.0, Align::Left),
        Column::fixed("rows", 110.0, Align::Right),
        Column::flex("columns", Align::Left),
    ];
    let rows: Vec<Row> = tables
        .iter()
        .map(|t| {
            let names = t
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            // Plaintext source metadata is not attestable.
            Row::new(
                Evidence::NotAttestable,
                vec![
                    Cell::new(&t.name),
                    Cell::new(w::thousands(t.rows)),
                    Cell::tinted(&names, p.text3),
                ],
            )
            // Keep declared types available without widening the grid.
            .tooltip(
                t.columns
                    .iter()
                    .map(|c| format!("{} {}", c.name, c.declared))
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
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

/// Paint loading, failure and no-source states.
fn notice(ui: &mut Ui, body: Rect, state: &State) {
    let p = &state.palette;
    let (line, detail, ink) = match &state.source {
        Source::None => (
            "No source chosen".to_owned(),
            "Choose a SQLite file to see what it holds.".to_owned(),
            p.text3,
        ),
        Source::Reading { path } => (
            "Reading".to_owned(),
            // Name the file whose read is pending.
            path.display().to_string(),
            p.text3,
        ),
        Source::Failed { error, .. } => (
            "That file could not be read".to_owned(),
            // Preserve SQLite's diagnostic.
            error.message().to_owned(),
            p.tampered,
        ),
        Source::Read { .. } => (
            "That database holds no tables".to_owned(),
            "It opened and was read; there is no table schema to create.".to_owned(),
            p.text3,
        ),
    };
    let c = body.center();
    ui.painter().text(
        Pos2::new(c.x, c.y - 10.0),
        egui::Align2::CENTER_CENTER,
        &line,
        role::chrome_strong(),
        ink,
    );
    let fitted = w::elide_galley(
        ui.painter(),
        &detail,
        role::meta(),
        (body.width() - 96.0).max(120.0),
        p.text4,
    );
    ui.painter().galley(
        Pos2::new(c.x - fitted.size().x * 0.5, c.y + 8.0),
        fitted,
        p.text4,
    );
    let accessible = format!("{line}. {detail}");
    w::semantic_status(
        ui,
        body,
        "sqlite_source",
        &accessible,
        matches!(state.source, Source::Failed { .. }),
    );
}

fn action_bar(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    w::fill(ui.painter(), rect, p.ground3);
    w::hairline_top(ui.painter(), rect, p.hairline);
    let primary = Rect::from_min_size(
        Pos2::new(rect.right() - 24.0 - 200.0, rect.center().y - 16.0),
        Vec2::new(200.0, 32.0),
    );
    let cancel_at = Rect::from_min_size(
        Pos2::new(primary.left() - 129.0, primary.top()),
        Vec2::new(120.0, 32.0),
    );
    // Status text elides against the leftmost action.
    let copy_at = Rect::from_min_size(
        Pos2::new(cancel_at.left() - 190.0, primary.top()),
        Vec2::new(180.0, primary.height()),
    );

    // State the current blocker and elide before the action controls.
    let approximate = crate::plan::approximate_columns(state.source.tables());
    let note = match (
        state.import_progress,
        state.import_error.as_ref(),
        state.source.blocker(),
        state.vault.is_some(),
        approximate,
    ) {
        (ImportProgress::Running, _, _, _, _) => "Creating tables…".to_owned(),
        (ImportProgress::Cancelling, _, _, _, _) => "Stopping table creation…".to_owned(),
        (ImportProgress::Idle, Some(error), _, _, _) => error.to_string(),
        (ImportProgress::Idle, None, Some(_), _, _) => String::new(),
        (ImportProgress::Idle, None, None, false, _) => {
            "Open a vault before creating these tables.".to_owned()
        }
        // Surface approximate conversions before executing generated statements.
        (ImportProgress::Idle, None, None, true, 1) => {
            "1 column has no exact CitadelDB type. The copied statements say which.".to_owned()
        }
        (ImportProgress::Idle, None, None, true, n) if n > 1 => {
            format!("{n} columns have no exact CitadelDB type. The copied statements say which.")
        }
        (ImportProgress::Idle, None, None, true, _) => SCHEMA_ONLY.to_owned(),
    };
    let completed = state
        .import_error
        .as_ref()
        .is_some_and(|error| error.kind == crate::engine::Kind::Completed);
    let ink = if completed {
        p.missing
    } else if state.import_error.is_some() {
        p.tampered
    } else if state.source.blocker().is_some() {
        p.text3
    } else if state.vault.is_none() || approximate > 0 {
        p.missing
    } else {
        p.text3
    };
    let room = copy_at.left() - rect.left() - 36.0;
    if room > 40.0 {
        let fitted = w::elide_galley(ui.painter(), &note, role::meta(), room, ink);
        ui.painter().galley(
            Pos2::new(rect.left() + 24.0, rect.center().y - fitted.size().y * 0.5),
            fitted,
            ink,
        );
        w::semantic_status(
            ui,
            Rect::from_min_size(
                Pos2::new(rect.left() + 24.0, primary.top()),
                Vec2::new(room, primary.height()),
            ),
            "import_action",
            &note,
            state.import_error.is_some() && !completed,
        );
    }
    let cancel = match state.import_progress {
        ImportProgress::Idle if completed => {
            ui::Btn::new("Back to vault", ui::ButtonKind::Outlined)
        }
        ImportProgress::Idle => ui::Btn::new("Cancel", ui::ButtonKind::Outlined),
        ImportProgress::Running => ui::Btn::new("Stop", ui::ButtonKind::Outlined),
        ImportProgress::Cancelling => ui::Btn::new("Stopping…", ui::ButtonKind::Outlined)
            .unavailable("Waiting for the current transaction to roll back"),
    };
    if ui::button(ui, cancel_at, cancel, p).clicked() {
        out.push(if state.import_progress == ImportProgress::Running {
            Action::CancelImportRun
        } else {
            Action::CancelImport
        });
    }

    let copy = ui::Btn::new("Copy CREATE TABLE", ui::ButtonKind::Ghost);
    let copy = match (state.import_progress.busy(), state.source.blocker()) {
        (true, _) => copy.unavailable("Wait for table creation to finish"),
        (false, Some(reason)) => copy.unavailable(reason),
        (false, None) => {
            copy.tip("CREATE TABLE for the tables listed, ready to run against a vault")
        }
    };
    if ui::button(ui, copy_at, copy, p).clicked() {
        ui.ctx()
            .copy_text(crate::plan::statements(state.source.tables()));
    }

    // Inspection provides schema and row counts, not source rows; generated statements
    // therefore create schema only.
    let run = ui::Btn::new("Create tables", ui::ButtonKind::Primary);
    let run = match (
        state.import_progress.busy(),
        state.source.blocker(),
        state.vault.is_some(),
        state
            .import_error
            .as_ref()
            .is_some_and(|error| error.kind == crate::engine::Kind::Completed),
    ) {
        (true, _, _, _) => run.unavailable("Table creation is already running"),
        (false, _, _, true) => {
            run.unavailable("The tables were created; return to the vault before continuing")
        }
        (false, Some(reason), _, false) => run.unavailable(reason),
        (false, None, false, false) => run.unavailable("Open a vault to create these tables in"),
        (false, None, true, false) => run.tip("Creates the tables listed above in the open vault"),
    };
    if ui::button(ui, primary, run, p).clicked() {
        out.push(Action::RunImport);
    }
}

/// A stack of plates, the conventional mark for a database.
fn stack_glyph(painter: &egui::Painter, c: Pos2, colour: egui::Color32) {
    let stroke = egui::Stroke::new(1.5, colour);
    for i in 0..3 {
        let y = c.y - 5.0 + i as f32 * 5.0;
        painter.rect_stroke(
            Rect::from_center_size(Pos2::new(c.x, y), Vec2::new(16.0, 4.0)),
            egui::CornerRadius::same(2),
            stroke,
            egui::StrokeKind::Inside,
        );
    }
}
