//! Expanded and compact views of objects in the open vault.

use crate::fonts::role;
use crate::model::Vault;
use crate::state::{Action, Doc, State};
use crate::theme::{metrics, radius};
use crate::ui;
use crate::widgets as w;
use egui::{Align2, Pos2, Rect, Ui, Vec2};

/// One object in the sidebar, and the document opening it produces.
pub(super) struct Entry {
    pub label: String,
    /// Shape summary: dimensions or row count.
    pub meta: Option<String>,
    pub doc: Doc,
}

pub(super) struct Group {
    pub header: &'static str,
    /// Compact-rail prefix. The ordinal disambiguates siblings with the same initial.
    pub mark: &'static str,
    pub entries: Vec<Entry>,
}

/// Everything the open vault contains, in the order both surfaces show it.
pub(super) fn groups(vault: &Vault) -> Vec<Group> {
    vec![
        Group {
            header: "Regions",
            mark: "R",
            entries: vault
                .regions
                .iter()
                .map(|r| Entry {
                    label: r.name().to_owned(),
                    // Report the region's total size, not the loaded window.
                    meta: Some(format!(
                        "{}d{}{}",
                        r.facts.dim,
                        w::SEP,
                        r.facts
                            .total
                            .map(|total| total.to_string())
                            .unwrap_or_else(|| "unknown".to_owned())
                    )),
                    doc: Doc::Region(r.name().to_owned()),
                })
                .collect(),
        },
        Group {
            header: "Tables",
            mark: "T",
            entries: tables(vault, false),
        },
        Group {
            header: "Vector columns",
            mark: "V",
            entries: vault
                .vectors
                .iter()
                .map(|v| Entry {
                    label: v.qualified(),
                    meta: Some(format!("{}d", v.dim)),
                    doc: Doc::Vector(v.qualified()),
                })
                .collect(),
        },
        // Keep memory-owned storage separate from user tables.
        Group {
            header: "Memory storage",
            mark: "M",
            entries: tables(vault, true),
        },
        Group {
            header: "Vault",
            mark: "",
            entries: vec![
                Entry {
                    label: "Security".into(),
                    meta: None,
                    doc: Doc::Security,
                },
                // `Doc::Query` identifies the single query document.
                Entry {
                    label: "Query".into(),
                    meta: None,
                    doc: Doc::Query,
                },
            ],
        },
    ]
}

/// Partition vault tables by memory-engine ownership.
fn tables(vault: &Vault, engine_owned: bool) -> Vec<Entry> {
    vault
        .tables
        .iter()
        .filter(|t| t.engine_owned == engine_owned)
        .map(|t| Entry {
            label: t.name.clone(),
            meta: Some(w::thousands(t.rows)),
            doc: Doc::Table(t.name.clone()),
        })
        .collect()
}

/// True when this entry's document is the one on screen. Compared by identity, so two
/// regions never light up together.
fn is_open(entry: &Entry, state: &State) -> bool {
    state.active_doc() == Some(&entry.doc)
}

pub(super) fn objects(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    w::fill(ui.painter(), rect, p.ground2);
    w::hairline_right(ui.painter(), rect, p.hairline);

    // Scroll so every object remains reachable in tall collections.
    let mut child = ui.new_child(egui::UiBuilder::new().max_rect(rect));
    egui::ScrollArea::vertical()
        .auto_shrink([false; 2])
        .show(&mut child, |ui| {
            let top = ui.min_rect().top();
            let mut y = top;
            // Omit empty groups and their headers.
            for group in groups(state.vault())
                .iter()
                .filter(|g| !g.entries.is_empty())
            {
                if y > top {
                    y += 8.0;
                }
                w::section_header(
                    ui.painter(),
                    Rect::from_min_size(Pos2::new(rect.left(), y), Vec2::new(rect.width(), 32.0)),
                    group.header,
                    p,
                );
                y += 32.0;
                for entry in &group.entries {
                    let r = Rect::from_min_size(
                        Pos2::new(rect.left(), y),
                        Vec2::new(rect.width(), metrics::TREE_ROW_H),
                    );
                    let row = ui::tree_row(
                        ui,
                        r,
                        &entry.label,
                        entry.meta.as_deref(),
                        is_open(entry, state),
                        p,
                    );
                    if row.clicked() {
                        out.push(Action::OpenDoc(entry.doc.clone()));
                    }
                    y += metrics::TREE_ROW_H;
                }
            }
            ui.allocate_space(Vec2::new(rect.width(), y - top));
        });
}

/// Chip side, and the pitch between chips.
const CHIP: f32 = 26.0;
const PITCH: f32 = 32.0;

/// Compact rail with one semantically marked chip per object.
pub(super) fn collapsed_objects(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    w::fill(ui.painter(), rect, p.ground2);
    w::hairline_right(ui.painter(), rect, p.hairline);

    // Scroll rather than truncating objects beyond the viewport.
    let mut child = ui.new_child(egui::UiBuilder::new().max_rect(rect));
    egui::ScrollArea::vertical()
        .auto_shrink([false; 2])
        .show(&mut child, |ui| {
            let top = ui.min_rect().top();
            let mut y = top + 12.0;
            // Dividers appear only between non-empty groups.
            for group in groups(state.vault())
                .iter()
                .filter(|g| !g.entries.is_empty())
            {
                if y > top + 12.0 {
                    ui.painter().rect_filled(
                        Rect::from_min_size(Pos2::new(rect.left() + 12.0, y), Vec2::new(24.0, 1.0)),
                        0.0,
                        p.hairline,
                    );
                    y += 9.0;
                }
                for (ordinal, entry) in group.entries.iter().enumerate() {
                    chip(
                        ui,
                        Rect::from_min_size(
                            Pos2::new(rect.center().x - CHIP * 0.5, y),
                            Vec2::splat(CHIP),
                        ),
                        entry,
                        compact_mark(group, entry, ordinal),
                        state,
                        out,
                    );
                    y += PITCH;
                }
            }
            ui.allocate_space(Vec2::new(rect.width(), y - top));
        });
}

fn compact_mark(group: &Group, entry: &Entry, ordinal: usize) -> String {
    match &entry.doc {
        Doc::Security => "S".to_owned(),
        Doc::Query => "Q".to_owned(),
        _ => format!("{}{ordinal}", group.mark, ordinal = ordinal + 1),
    }
}

fn chip(ui: &mut Ui, r: Rect, entry: &Entry, mark: String, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    let on = is_open(entry, state);
    let response = ui.interact(
        r,
        egui::Id::new(("rail", &entry.label)),
        egui::Sense::click(),
    );
    if on || response.hovered() {
        ui.painter().rect_filled(
            r,
            egui::CornerRadius::same(radius::CONTROL),
            if on { p.wash(26) } else { p.wash(15) },
        );
    }
    ui.painter().rect_stroke(
        r,
        egui::CornerRadius::same(radius::CONTROL),
        egui::Stroke::new(1.0, if on { p.accent } else { p.hairline_strong }),
        egui::StrokeKind::Inside,
    );
    ui.painter().text(
        r.center(),
        Align2::CENTER_CENTER,
        mark,
        role::meta_mono(),
        if on { p.text1 } else { p.text2 },
    );
    if response.has_focus() {
        ui::focus_ring(ui, r, p, radius::CONTROL);
    }

    let name = match &entry.meta {
        Some(meta) => format!("{}  {meta}", entry.label),
        None => entry.label.clone(),
    };
    response.widget_info(|| {
        egui::WidgetInfo::selected(egui::WidgetType::SelectableLabel, true, on, &name)
    });
    if response.on_hover_text(&name).clicked() {
        out.push(Action::OpenDoc(entry.doc.clone()));
    }
}
