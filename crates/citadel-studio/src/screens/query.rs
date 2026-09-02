//! SQL editor and result evidence.

use super::*;
use crate::engine::Statement;
use crate::fonts::role;
use crate::grid::{self, Align, Cell, Column, Row};
use crate::state::{Action, EditorNotice, QueryState, State};
use crate::theme::{metrics, Evidence, Palette};
use crate::ui;
use crate::widgets as w;
use egui::{Align2, Frame, Margin, Modal, Pos2, Rect, RichText, Stroke, Ui, Vec2};

pub fn pane(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    // Avoid holding a shared painter borrow across mutable UI calls.
    let painter = &ui.painter().clone();
    w::fill(painter, rect, p.ground1);

    // Render editor input before toolbar intents so an IME/text commit and Run in the same
    // frame submit the buffer the user can see, never the previous model snapshot.
    let editor_h = (rect.height() * 0.36).clamp(200.0, 320.0);
    let editor = Rect::from_min_size(
        Pos2::new(rect.left(), rect.top() + metrics::TOOLBAR_H),
        Vec2::new(rect.width(), editor_h),
    );
    sql_editor(ui, editor, state, out);

    let bar = Rect::from_min_size(rect.left_top(), Vec2::new(rect.width(), metrics::TOOLBAR_H));
    paint_toolbar(painter, bar, p);
    // This is informational, not a vault or schema picker.
    let title = w::elide_galley(
        painter,
        &state.vault().name,
        role::chrome(),
        (bar.width() - 12.0 - 440.0).max(0.0),
        p.text2,
    );
    painter.galley(
        Pos2::new(bar.left() + 12.0, bar.center().y - title.size().y * 0.5),
        title,
        p.text2,
    );
    let running = state.result.busy();
    let blocker = state.query_blocker();
    // Analyze executes the statement; Explain does not.
    let explain = ui::button(
        ui,
        Rect::from_min_size(
            Pos2::new(bar.right() - 90.0 - 108.0 - 76.0, bar.top() + 3.0),
            Vec2::new(72.0, 26.0),
        ),
        {
            let btn = ui::Btn::new("Explain", ui::ButtonKind::Ghost)
                .tip("Estimates the plan without running the statement");
            if let Some(reason) = blocker {
                btn.unavailable(reason)
            } else {
                btn
            }
        },
        p,
    );
    if explain.clicked() {
        out.push(Action::ExplainQuery);
    }
    let analyze = ui::button(
        ui,
        Rect::from_min_size(
            Pos2::new(bar.right() - 90.0 - 108.0, bar.top() + 3.0),
            Vec2::new(104.0, 26.0),
        ),
        {
            let btn = ui::Btn::new("Analyze (runs)", ui::ButtonKind::Ghost)
                .keyed("Analyze")
                .tip("Runs the statement and reports the time and rows it really took");
            if let Some(reason) = blocker {
                btn.unavailable(reason)
            } else {
                btn
            }
        },
        p,
    );
    if analyze.clicked() {
        out.push(Action::AnalyzeQuery);
    }
    let format = ui::button(
        ui,
        Rect::from_min_size(
            Pos2::new(bar.right() - 90.0 - 108.0 - 76.0 - 76.0, bar.top() + 3.0),
            Vec2::new(72.0, 26.0),
        ),
        {
            let btn = ui::Btn::new("Format", ui::ButtonKind::Ghost)
                .tip("Reformats SQL while preserving leading comments and hints");
            if state.query_composing {
                btn.unavailable("Finish or cancel text composition before formatting")
            } else {
                btn
            }
        },
        p,
    );
    if format.clicked() {
        out.push(Action::FormatQuery);
    }
    let run = ui::button(
        ui,
        Rect::from_min_size(
            Pos2::new(bar.right() - 78.0, bar.top() + 3.0),
            Vec2::new(66.0, 26.0),
        ),
        // A single stable control represents the operation's current action.
        if running {
            ui::Btn::new("Cancel", ui::ButtonKind::Danger).keyed("Run")
        } else if let Some(reason) = blocker {
            ui::Btn::new("Run", ui::ButtonKind::Primary).unavailable(reason)
        } else {
            ui::Btn::new("Run", ui::ButtonKind::Primary)
        },
        p,
    );
    if run.clicked() {
        out.push(if running {
            Action::CancelQuery
        } else {
            Action::RunQuery
        });
    }

    let results = Rect::from_min_max(Pos2::new(rect.left(), editor.bottom()), rect.right_bottom());
    painter.rect_filled(
        Rect::from_min_size(results.left_top(), Vec2::new(results.width(), 1.0)),
        0.0,
        p.hairline,
    );
    let rbar = Rect::from_min_size(
        Pos2::new(results.left(), results.top() + 1.0),
        Vec2::new(results.width(), metrics::TOOLBAR_H),
    );
    paint_toolbar(painter, rbar, p);
    let mut rx = rbar.left() + 12.0;
    // Every fact on this bar comes from the outcome.
    let summary = summary(state);
    rx += w::text_left(
        painter,
        Pos2::new(rx, rbar.center().y),
        &summary.headline,
        role::chrome(),
        p.text1,
    ) + 10.0;
    if let Some(note) = &summary.note {
        // Engine diagnostics are unbounded, so elide to the remaining width.
        w::chip(
            painter,
            Pos2::new(rx, rbar.center().y),
            note,
            p,
            summary.colour(p),
            summary.wash(p),
            rbar.right() - 12.0 - rx,
        );
    }
    let semantic_summary = summary.note.as_ref().map_or_else(
        || summary.headline.clone(),
        |note| format!("{}{}{}", summary.headline, w::SEP, note),
    );
    w::semantic_status(
        ui,
        rbar,
        ("query_result", state.query_editor_revision),
        &semantic_summary,
        summary.failed,
    );

    let body = Rect::from_min_max(
        Pos2::new(results.left(), rbar.bottom()),
        results.right_bottom(),
    );
    // Result columns come from the statement.
    let (columns, rows) = result_grid(state);
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
    if let Some(destination) = state.query_discard.as_ref() {
        discard_dialog(ui, rect, state, destination, out);
    }
}

fn discard_dialog(
    ui: &mut Ui,
    full: Rect,
    state: &State,
    destination: &crate::state::QueryDiscard,
    out: &mut Vec<Action>,
) {
    let p = &state.palette;
    let width = (full.width() - 24.0).clamp(260.0, 430.0);
    let consequence = match destination {
        crate::state::QueryDiscard::CloseVault => {
            "Unsaved SQL will be lost.\nContinue closing the vault?"
        }
        crate::state::QueryDiscard::Exit => "Unsaved SQL will be lost.\nContinue exiting Studio?",
        crate::state::QueryDiscard::OpenDemoVault
        | crate::state::QueryDiscard::OpenRecent(_)
        | crate::state::QueryDiscard::ChooseVaultToOpen => {
            "Unsaved SQL will be lost.\nContinue opening another vault?"
        }
        crate::state::QueryDiscard::BeginCreate => {
            "Unsaved SQL will be lost.\nContinue creating another vault?"
        }
        crate::state::QueryDiscard::BeginImport => {
            "Unsaved SQL will be lost.\nContinue to the SQLite import?"
        }
    };
    let modal = Modal::new(egui::Id::new((
        "query_discard",
        state.query_editor_revision,
    )))
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
            node.set_label("Discard the edited query?");
            node.set_description(
                "Choose Keep editing to retain the query or Discard to continue the destructive action",
            );
        });
        ui.label(
            RichText::new("Discard the edited query?")
                .font(role::section())
                .color(p.text1),
        );
        ui.add_space(8.0);
        ui.add(
            egui::Label::new(RichText::new(consequence).font(role::body()).color(p.text2)).wrap(),
        );
        ui.add_space(18.0);

        let (_, row) = ui.allocate_space(Vec2::new(ui.available_width(), 32.0));
        let discard_rect = Rect::from_min_size(
            Pos2::new(row.right() - 104.0, row.top()),
            Vec2::new(104.0, 32.0),
        );
        let keep_rect = Rect::from_min_size(
            Pos2::new(discard_rect.left() - 124.0, row.top()),
            Vec2::new(112.0, 32.0),
        );
        let keep = ui::button(
            ui,
            keep_rect,
            ui::Btn::new("Keep editing", ui::ButtonKind::Ghost),
            p,
        );
        let discard = ui::button(
            ui,
            discard_rect,
            ui::Btn::new("Discard", ui::ButtonKind::Danger),
            p,
        );
        let focus = ui.ctx().memory(|memory| memory.focused());
        if focus != Some(keep.id) && focus != Some(discard.id) {
            keep.request_focus();
        }
        (keep.clicked(), discard.clicked())
    });

    if modal.inner.1 {
        out.push(Action::ConfirmDiscardQuery);
    } else if modal.inner.0 || modal.should_close() {
        out.push(Action::CancelDiscardQuery);
    }
}

/// SQL `TextEdit` with app-owned syntax colours.
///
/// `TextEdit` preserves native caret, selection, undo, clipboard, IME and accessibility
/// semantics; the layouter changes presentation only.
fn sql_editor(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    w::fill(ui.painter(), rect, p.ground1);
    let gutter = Rect::from_min_size(rect.left_top(), Vec2::new(GUTTER_W, rect.height()));
    w::fill(ui.painter(), gutter, p.ground0);

    // Screens read state and emit actions; egui retains edit state under this stable id.
    let mut buffer = state.query.clone();
    let text_top = rect.top() + EDITOR_PAD_Y;
    let row_h = ui.fonts_mut(|fonts| fonts.row_height(&role::cell()));
    let text_bottom = complete_editor_bottom(text_top, rect.bottom() - EDITOR_PAD_Y, row_h);
    let text = Rect::from_min_max(
        Pos2::new(gutter.right() + 8.0, text_top),
        Pos2::new(rect.right(), text_bottom),
    );
    let editor_id = state.query_editor_id();
    let desired_rows = ((text.height() / row_h).floor() as usize).max(1);

    let mut layouter = |ui: &Ui, buf: &dyn egui::TextBuffer, wrap: f32| {
        let mut job = highlight(buf.as_str(), p);
        job.wrap.max_width = wrap;
        ui.fonts_mut(|f| f.layout_job(job))
    };

    // The gutter follows the galley, so it shares the editor's scroll offset.
    let mut child = ui.new_child(egui::UiBuilder::new().max_rect(text));
    let output = egui::ScrollArea::vertical()
        .id_salt(state.query_scroll_salt())
        .auto_shrink([false; 2])
        .show(&mut child, |ui| {
            egui::TextEdit::multiline(&mut buffer)
                .id(editor_id)
                .font(role::cell())
                .frame(egui::Frame::NONE)
                .desired_width(text.width())
                // Fill the viewport when the source is short, so blank space below EOF is
                // still a real editor hit target and places the caret at the end.
                .desired_rows(desired_rows)
                // Tab indents; Escape releases focus so the editor cannot trap the keyboard.
                .lock_focus(true)
                .layouter(&mut layouter)
                .show(ui)
        })
        .inner;

    output
        .response
        .ctx
        .accesskit_node_builder(output.response.id, |node| {
            node.set_label("SQL query editor");
        });

    let ime = ui.input(|input| {
        input.events.iter().rev().find_map(|event| match event {
            egui::Event::Ime(event) => Some(event.clone()),
            _ => None,
        })
    });
    match ime {
        Some(egui::ImeEvent::Preedit { text, .. }) if !text.is_empty() => {
            if !state.query_composing {
                out.push(Action::BeginQueryComposition);
            }
            if buffer != state.query {
                out.push(Action::EditQuery(buffer));
            }
        }
        Some(egui::ImeEvent::Preedit { .. }) => {
            if buffer != state.query {
                out.push(Action::EditQuery(buffer));
            }
            if state.query_composing {
                out.push(Action::CancelQueryComposition);
            }
        }
        Some(egui::ImeEvent::Commit(_)) if state.query_composing => {
            if buffer != state.query {
                out.push(Action::EditQuery(buffer));
            }
            out.push(Action::CommitQueryComposition);
        }
        _ if state.query_composing && !output.response.has_focus() => {
            out.push(Action::CancelQueryComposition);
        }
        _ if buffer != state.query => out.push(Action::EditQuery(buffer)),
        _ => {}
    }

    // Number logical lines rather than wrapped galley rows; clip them to the gutter.
    let numbers = ui.painter().with_clip_rect(Rect::from_min_max(
        gutter.left_top(),
        Pos2::new(gutter.right(), text.bottom()),
    ));
    let mut line = 0;
    let mut new_line = true;
    for row in &output.galley.rows {
        if new_line {
            line += 1;
            numbers.text(
                Pos2::new(gutter.right() - 12.0, output.galley_pos.y + row.pos.y + 8.0),
                Align2::RIGHT_CENTER,
                line.to_string(),
                role::cell_compact(),
                p.text4,
            );
        }
        new_line = row.ends_with_newline;
    }
}

/// Width of the line-number gutter.
const GUTTER_W: f32 = 54.0;
const EDITOR_PAD_Y: f32 = 6.0;

/// End the viewport between text rows so the results divider never slices through one.
fn complete_editor_bottom(top: f32, bottom: f32, row_h: f32) -> f32 {
    let rows = ((bottom - top).max(0.0) / row_h).floor();
    (top + rows * row_h).clamp(top, bottom)
}

/// Colour a SQL source string with the palette's syntax ramp.
fn highlight(src: &str, p: &Palette) -> egui::text::LayoutJob {
    use crate::sql::Token;
    let mut job = egui::text::LayoutJob::default();
    for (range, token) in crate::sql::tokenize(src) {
        job.append(
            &src[range],
            0.0,
            egui::TextFormat {
                font_id: role::cell(),
                color: match token {
                    Token::Keyword => p.syntax_keyword,
                    Token::Function => p.syntax_function,
                    Token::Literal => p.syntax_string,
                    Token::Number => p.syntax_number,
                    Token::Comment => p.syntax_comment,
                    Token::Plain => p.text1,
                },
                ..Default::default()
            },
        );
    }
    job
}

/// Summarize vector values as three leading and trailing components when needed.
fn summarise(v: &[f32]) -> String {
    let f = |x: &f32| format!("{x:.4}");
    // Elide only when components are hidden.
    if v.len() <= 6 {
        let all: Vec<String> = v.iter().map(f).collect();
        return format!("[{}]", all.join(", "));
    }
    let head: Vec<String> = v.iter().take(3).map(f).collect();
    let tail: Vec<String> = v.iter().rev().take(3).rev().map(f).collect();
    format!("[{}, ..., {}]", head.join(", "), tail.join(", "))
}

/// Maximum rendered rows; this never changes or limits the executed statement.
const RENDER_CAP: usize = crate::engine::session::RETAINED_QUERY_ROWS;

/// What the results bar says, read from the outcome rather than declared.
struct Summary {
    headline: String,
    note: Option<String>,
    /// Engine diagnostic, when execution failed.
    failed: bool,
}

impl Summary {
    fn colour(&self, p: &Palette) -> egui::Color32 {
        if self.failed {
            p.tampered
        } else {
            p.text3
        }
    }

    fn wash(&self, p: &Palette) -> Option<egui::Color32> {
        self.failed.then(|| Palette::tint(p.tampered, 30))
    }
}

/// Format a row count with correct singular agreement.
fn rows(n: usize) -> &'static str {
    if n == 1 {
        "row"
    } else {
        "rows"
    }
}

fn summary(state: &State) -> Summary {
    if let Some(notice) = &state.query_notice {
        let summary = match notice {
            EditorNotice::Unchanged(note) => Summary {
                headline: "Query unchanged".to_owned(),
                note: Some(note.clone()),
                failed: false,
            },
            EditorNotice::Formatted { changed: true } => Summary {
                headline: "Query formatted".to_owned(),
                note: Some("Editor text changed; nothing was run".to_owned()),
                failed: false,
            },
            EditorNotice::Formatted { changed: false } => Summary {
                headline: "Already formatted".to_owned(),
                note: Some("Editor text was unchanged; nothing was run".to_owned()),
                failed: false,
            },
        };
        return mark_stale(state, summary);
    }
    // Explain describes a plan; Analyze also reports its execution.
    if let Some(plan) = &state.plan {
        let refresh_error = state
            .result
            .done()
            .and_then(|run| run.failed.as_ref())
            .map(|error| error.to_string());
        return mark_stale(
            state,
            Summary {
                headline: format!(
                    "{} · {} {}",
                    if plan.measured {
                        "Measured execution"
                    } else {
                        "Plan only"
                    },
                    w::thousands(plan.lines.len() as u64),
                    if plan.lines.len() == 1 {
                        "line"
                    } else {
                        "lines"
                    }
                ),
                note: Some(if let Some(error) = &refresh_error {
                    format!(
                        "Vault changed, but Studio could not refresh it{}{}",
                        w::SEP,
                        error
                    )
                } else if plan.measured {
                    "Statement executed; actual time, scanned rows, and emitted rows included"
                        .to_owned()
                } else {
                    "Statement was not executed".to_owned()
                }),
                failed: refresh_error.is_some(),
            },
        );
    }
    let summary = match &state.result {
        QueryState::None => Summary {
            headline: "No statement run yet".to_owned(),
            note: None,
            failed: false,
        },
        QueryState::Running(_) => Summary {
            headline: "Running".to_owned(),
            // Cancellation is cooperative; latency varies by execution phase.
            note: Some("Cancel stops it at the next cancellation check".to_owned()),
            failed: false,
        },
        QueryState::Done { run, .. } => {
            // Script statements committed before the first failure remain durable.
            let committed = run.statements.len();
            // Distinguish rendered rows from the engine's full result count.
            let headline = match (run.last_rows(), run.last_row_count()) {
                (Some(q), Some(total)) if q.rows.len() < total => format!(
                    "showing {} of {} rows",
                    w::thousands(q.rows.len() as u64),
                    w::thousands(total as u64)
                ),
                (Some(q), _) => format!(
                    "{} {}",
                    w::thousands(q.rows.len() as u64),
                    rows(q.rows.len())
                ),
                (None, _) => match run.statements.last() {
                    Some(Statement::Changed(n)) => {
                        format!("{} {} changed", w::thousands(*n), rows(*n as usize))
                    }
                    Some(Statement::Ok) => "Statement completed".to_owned(),
                    Some(Statement::Rows(_)) | None => "No result".to_owned(),
                },
            };
            match &run.failed {
                Some(error) if error.kind == crate::engine::Kind::Completed => Summary {
                    headline,
                    note: Some(error.to_string()),
                    failed: false,
                },
                Some(error) => Summary {
                    headline,
                    note: Some(match committed {
                        0 => error.to_string(),
                        1 => format!("stopped after 1 statement{}{}", w::SEP, error.detail),
                        n => {
                            format!("stopped after {n} statements{}{}", w::SEP, error.detail)
                        }
                    }),
                    failed: true,
                },
                None => Summary {
                    headline,
                    note: (committed > 1).then(|| format!("{committed} statements")),
                    failed: false,
                },
            }
        }
    };
    mark_stale(state, summary)
}

fn mark_stale(state: &State, mut summary: Summary) -> Summary {
    if state.query_outcome_stale() {
        summary.headline = format!("Stale · {}", summary.headline);
        let stale = "editor changed since this SQL was submitted";
        summary.note = Some(match summary.note {
            Some(note) => format!("{stale}{}{}", w::SEP, note),
            None => stale.to_owned(),
        });
    }
    summary
}

/// The columns and rows of the last result, named by the statement that produced them.
fn result_grid(state: &State) -> (Vec<Column>, Vec<Row>) {
    // Plans replace stale rows and preserve the engine's own formatting.
    if let Some(plan) = &state.plan {
        let rows = plan
            .lines
            .iter()
            .map(|line| Row::new(Evidence::NotAttestable, vec![Cell::new(line.clone())]))
            .collect();
        let header = if plan.measured {
            "measured operation"
        } else {
            "planned operation"
        };
        return (vec![Column::flex(header, Align::Left)], rows);
    }
    // The results bar already distinguishes the initial empty state.
    let Some(result) = state.result.done().and_then(|run| run.last_rows()) else {
        return (Vec::new(), Vec::new());
    };
    let columns = result
        .columns
        .iter()
        .map(|name| Column::flex(name, Align::Left))
        .collect();
    let rows = result
        .rows
        .iter()
        .take(RENDER_CAP)
        .map(|row| {
            let cells = row.iter().map(cell).collect();
            // SQL rows carry no per-atom evidence.
            Row::new(Evidence::NotAttestable, cells)
        })
        .collect();
    (columns, rows)
}

fn cell(value: &citadel_sql::Value) -> Cell {
    match value {
        // The magnitude strip makes vectors visually comparable.
        citadel_sql::Value::Vector(v) => Cell::vector(summarise(v), v.to_vec()),
        other => {
            let rendered = crate::engine::value::cell(other);
            match rendered.hover() {
                Some(_) => Cell::new(rendered.display()),
                None => Cell::new(rendered.text),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::QueryRequest;

    fn request(sql: &str, query_revision: u64) -> QueryRequest {
        QueryRequest {
            execution: 1,
            vault_revision: 0,
            query_revision,
            sql: sql.to_owned(),
        }
    }

    #[test]
    fn format_notice_does_not_hide_that_existing_rows_are_stale() {
        let state = State {
            query: "-- comment\nSELECT 2".into(),
            query_revision: 2,
            query_notice: Some(EditorNotice::Unchanged(
                "Format skipped: comments and hints must be preserved exactly".into(),
            )),
            result: QueryState::Done {
                request: request("SELECT 1", 1),
                run: Box::new(crate::engine::Run::default()),
            },
            ..State::default()
        };

        let summary = summary(&state);
        assert!(summary.headline.starts_with("Stale · Query unchanged"));
        let note = summary.note.expect("stale format notice");
        assert!(note.contains("editor changed"));
        assert!(note.contains("comments and hints"));
    }

    #[test]
    fn measured_plan_does_not_hide_a_failed_post_write_refresh() {
        let request = request("UPDATE documents SET title = 'changed'", 0);
        let state = State {
            query: request.sql.clone(),
            plan: Some(crate::engine::QueryPlan {
                lines: vec!["UPDATE documents rows=1 (actual time=0.1ms)".to_owned()],
                measured: true,
            }),
            plan_request: Some(request.clone()),
            result: QueryState::Done {
                request,
                run: Box::new(crate::engine::Run {
                    statements: Vec::new(),
                    failed: Some(crate::engine::StudioError::new(
                        crate::engine::Kind::Io,
                        "refresh failed",
                    )),
                    storage_changed: true,
                }),
            },
            ..State::default()
        };

        let summary = summary(&state);
        assert!(summary.failed);
        assert!(summary.headline.starts_with("Measured execution"));
        assert!(summary.note.is_some_and(
            |note| note.contains("could not refresh") && note.contains("refresh failed")
        ));
    }

    #[test]
    fn retained_rows_are_reported_against_the_exact_engine_total() {
        let state = State {
            query: "SELECT n".into(),
            result: QueryState::Done {
                request: request("SELECT n", 0),
                run: Box::new(crate::engine::Run {
                    statements: vec![crate::engine::Statement::Rows(
                        crate::engine::session::RowSet {
                            result: citadel_sql::QueryResult {
                                columns: vec!["n".into()],
                                rows: vec![
                                    vec![citadel_sql::Value::Integer(1)],
                                    vec![citadel_sql::Value::Integer(2)],
                                ],
                            },
                            total_rows: 2_505,
                        },
                    )],
                    failed: None,
                    storage_changed: false,
                }),
            },
            ..State::default()
        };

        assert_eq!(summary(&state).headline, "showing 2 of 2,505 rows");
    }
}
