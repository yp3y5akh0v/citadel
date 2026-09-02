//! Stored vector components zero and one, normalized independently over a bounded sample.
//!
//! This is an inspection plot, not a nearest-neighbour or dimensionality-reduction view.

use super::*;
use crate::fonts::role;
use crate::rail;
use crate::state::{Action, State};
use crate::theme::{metrics, radius};
use crate::widgets as w;
use egui::{Painter, Pos2, Rect, Ui, Vec2};

pub fn pane(ui: &mut Ui, rect: Rect, state: &State, out: &mut Vec<Action>) {
    let p = &state.palette;
    w::fill(ui.painter(), rect, p.ground1);

    // Render the vector column identified by this document tab.
    let column = match state.active_doc() {
        Some(crate::state::Doc::Vector(name)) => state
            .vault()
            .vectors
            .iter()
            .find(|c| &c.qualified() == name),
        _ => None,
    };

    let bar = Rect::from_min_size(rect.left_top(), Vec2::new(rect.width(), metrics::TOOLBAR_H));
    paint_toolbar(ui.painter(), bar, p);
    const MAPPING_LABEL: &str = "raw components [0] and [1] · relative to this sample";
    let mapping = w::elide_galley(
        ui.painter(),
        MAPPING_LABEL,
        role::meta(),
        (bar.width() - 24.0).max(0.0),
        p.text3,
    );
    let mapping_x = (bar.right() - 12.0 - mapping.size().x).max(bar.left() + 12.0);
    ui.painter().galley(
        Pos2::new(mapping_x, bar.center().y - mapping.size().y * 0.5),
        mapping.clone(),
        p.text3,
    );
    let title = column.map_or_else(|| "no column".to_owned(), |c| c.qualified());
    let title = w::elide_galley(
        ui.painter(),
        &title,
        role::chrome(),
        (mapping_x - bar.left() - 24.0).max(0.0),
        p.text1,
    );
    ui.painter().galley(
        Pos2::new(bar.left() + 12.0, bar.center().y - title.size().y * 0.5),
        title,
        p.text1,
    );

    let canvas = Rect::from_min_max(
        Pos2::new(rect.left() + 16.0, bar.bottom() + 16.0),
        Pos2::new(rect.right() - 16.0, rect.bottom() - 16.0),
    );
    w::surface(
        ui.painter(),
        canvas,
        p.ground1,
        Some(p.hairline),
        radius::CARD,
    );
    if let Some(notice) = sample_notice(column) {
        paint_sample_notice(ui, canvas, &notice, state);
        return;
    }
    let column = column.expect("a ready vector sample has a source column");
    let points = column.points.as_slice();

    // Show only states represented by this cloud and reserve their legend before mapping
    // data, so no point can sit behind painted chrome.
    let present: Vec<crate::theme::Evidence> = rail::ALL
        .into_iter()
        .filter(|ev| points.iter().any(|pt| pt.evidence == *ev))
        .collect();
    let generic_sql_sample = present == [crate::theme::Evidence::NotAttestable];
    const LEGEND_PAD: f32 = 12.0;
    const SWATCH_COL: f32 = 14.0;
    let swatches = present.iter().any(|ev| ev.colour(p).is_some());
    let legend_inset = LEGEND_PAD + if swatches { SWATCH_COL } else { 0.0 };
    let legend_widest = if generic_sql_sample {
        ui.painter()
            .layout_no_wrap(
                "SQL sample · no per-row attestation".to_owned(),
                role::meta(),
                p.text2,
            )
            .size()
            .x
    } else {
        present
            .iter()
            .map(|ev| {
                ui.painter()
                    .layout_no_wrap(sample_evidence_label(*ev).to_owned(), role::meta(), p.text2)
                    .size()
                    .x
            })
            .fold(0.0_f32, f32::max)
    };

    let legend = Rect::from_min_size(
        Pos2::new(canvas.left() + 16.0, canvas.top() + 16.0),
        Vec2::new(
            legend_inset + legend_widest + LEGEND_PAD,
            if generic_sql_sample {
                34.0
            } else {
                22.0 * present.len() as f32 + 12.0
            },
        ),
    );
    let plot = projection_rect(canvas, (!present.is_empty()).then_some(legend.bottom()));

    crate::cloud::paint(
        ui.painter(),
        canvas,
        plot,
        state,
        &column.qualified(),
        &column.points,
    );
    paint_dimension_axes(ui.painter(), plot, p);
    if let Some(point) = state.selected_point.and_then(|i| points.get(i)) {
        marker(ui.painter(), plot, state, point);
    }
    camera_controls(ui, plot, state, points, out);

    if !present.is_empty() {
        w::surface(
            ui.painter(),
            legend,
            p.ground4,
            Some(p.hairline_strong),
            radius::CONTROL,
        );
    }
    if generic_sql_sample {
        let label = "SQL sample · no per-row attestation";
        w::text_left(
            ui.painter(),
            Pos2::new(legend.left() + LEGEND_PAD, legend.center().y),
            label,
            role::meta(),
            p.text2,
        );
        let response = ui.interact(
            legend,
            egui::Id::new("vector_sample_proof_notice"),
            egui::Sense::hover(),
        );
        response.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Label, true, label));
        response.on_hover_text(
            "Generic SQL vector rows do not carry the per-atom MAC needed for a row proof.",
        );
    } else {
        for (i, ev) in present.into_iter().enumerate() {
            let ly = legend.top() + 16.0 + i as f32 * 22.0;
            rail::paint(
                ui.painter(),
                Rect::from_min_size(
                    Pos2::new(legend.left() + LEGEND_PAD, ly - 7.0),
                    Vec2::new(3.0, 14.0),
                ),
                ev,
                p,
                state.ppp,
            );
            w::text_left(
                ui.painter(),
                Pos2::new(legend.left() + legend_inset, ly),
                sample_evidence_label(ev),
                role::meta(),
                p.text2,
            );
        }
    }

    // Visible normalized coordinates are `centre +/- 0.5 / zoom` on each axis.
    let half = Vec2::splat(0.5 / state.camera.zoom);
    let shown = column.index.count(
        &column.points,
        state.camera.centre - half,
        state.camera.centre + half,
    );
    // Distinguish visible points, sampled points and total column rows.
    let counts = if column.sampled() {
        format!(
            "{} of {} sampled from {}{}{:.1}x",
            w::thousands(u64::from(shown)),
            w::thousands(points.len() as u64),
            w::thousands(column.total),
            w::SEP,
            state.camera.zoom
        )
    } else {
        format!(
            "{} of {} shown{}{:.1}x",
            w::thousands(u64::from(shown)),
            w::thousands(points.len() as u64),
            w::SEP,
            state.camera.zoom
        )
    };
    // Size from the rendered count string and anchor to the right edge.
    const CHIP_PAD: f32 = 10.0;
    let counts_w = ui
        .painter()
        .layout_no_wrap(counts.clone(), role::meta_mono(), p.text1)
        .size()
        .x;
    let chip = Rect::from_min_size(
        Pos2::new(
            canvas.right() - 16.0 - counts_w - CHIP_PAD * 2.0,
            canvas.bottom() - 42.0,
        ),
        Vec2::new(counts_w + CHIP_PAD * 2.0, 26.0),
    );
    w::surface(
        ui.painter(),
        chip,
        p.ground4,
        Some(p.hairline_strong),
        radius::CONTROL,
    );
    w::text_left(
        ui.painter(),
        Pos2::new(chip.left() + CHIP_PAD, chip.center().y),
        &counts,
        role::meta_mono(),
        p.text1,
    );
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct SampleNotice {
    headline: &'static str,
    detail: String,
    alert: bool,
}

fn sample_notice(column: Option<&crate::model::VectorColumn>) -> Option<SampleNotice> {
    let Some(column) = column else {
        return Some(SampleNotice {
            headline: "Vector column unavailable",
            detail: "The selected column is no longer present in this vault.".to_owned(),
            alert: true,
        });
    };
    let qualified = column.qualified();
    match &column.sample {
        crate::model::VectorSample::Unloaded | crate::model::VectorSample::Loading => {
            Some(SampleNotice {
                headline: "Loading vector sample",
                detail: format!("Reading a bounded sample from {qualified}."),
                alert: false,
            })
        }
        crate::model::VectorSample::Failed(error) => Some(SampleNotice {
            headline: "Vector sample could not be loaded",
            detail: format!("{error}. Reopen the vault to retry."),
            alert: true,
        }),
        crate::model::VectorSample::Ready if column.points.is_empty() && column.total == 0 => {
            Some(SampleNotice {
                headline: "No vectors to plot",
                detail: format!("{qualified} has no rows."),
                alert: false,
            })
        }
        crate::model::VectorSample::Ready if column.points.is_empty() => Some(SampleNotice {
            headline: "No vectors to plot",
            detail: format!(
                "{qualified} has {} rows, but its bounded sample contains no stored vector values.",
                w::thousands(column.total)
            ),
            alert: false,
        }),
        crate::model::VectorSample::Ready => None,
    }
}

fn paint_sample_notice(ui: &mut Ui, canvas: Rect, notice: &SampleNotice, state: &State) {
    let p = &state.palette;
    let painter = ui.painter().with_clip_rect(canvas);
    let width = (canvas.width() - 64.0).clamp(0.0, 560.0);
    let headline = w::elide_galley(
        &painter,
        notice.headline,
        role::chrome_strong(),
        width,
        if notice.alert { p.tampered } else { p.text2 },
    );
    let detail = painter.layout(notice.detail.clone(), role::meta(), p.text4, width);
    let gap = 12.0;
    let block_height = headline.size().y + gap + detail.size().y;
    let top = (canvas.center().y - block_height * 0.5).max(canvas.top() + 24.0);
    let detail_top = top + headline.size().y + gap;
    painter.galley(
        Pos2::new(canvas.center().x - headline.size().x * 0.5, top),
        headline,
        if notice.alert { p.tampered } else { p.text2 },
    );
    painter.galley(
        Pos2::new(canvas.center().x - detail.size().x * 0.5, detail_top),
        detail,
        p.text4,
    );
    w::semantic_status(
        ui,
        canvas,
        "vector_sample",
        &format!("{}. {}", notice.headline, notice.detail),
        notice.alert,
    );
}

/// Resolve canvas navigation and return the resulting camera state.
fn camera_controls(
    ui: &mut Ui,
    plot: Rect,
    state: &State,
    points: &[crate::model::Projected],
    out: &mut Vec<Action>,
) {
    // Points at normalized extrema have a visible halo outside the center-mapping rect.
    // Receive pointer input over the full pick radius so those pixels are not dead.
    let response = ui.interact(
        plot.expand(PICK_RADIUS),
        egui::Id::new("vector_canvas"),
        canvas_sense(),
    );
    let camera = state.camera;

    if response.dragged() {
        let span = plot.size() * camera.zoom;
        let moved = crate::state::Camera {
            centre: camera.centre - response.drag_delta() / span,
            ..camera
        };
        out.push(Action::MoveCamera(moved.clamped()));
    }

    if let Some(at) = response.hover_pos() {
        let wheel = ui.input(|i| i.smooth_scroll_delta.y);
        if wheel != 0.0 && !state.palette_open {
            out.push(Action::MoveCamera(camera.zoomed_at(
                plot,
                at,
                (wheel * 0.004).exp(),
            )));
        }
        if let Some(i) = nearest(plot, camera, points, at) {
            let point = points[i];
            let screen = camera.to_screen(plot, egui::vec2(point.x, point.y));
            ui.painter()
                .circle_stroke(screen, 8.0, egui::Stroke::new(1.2, state.palette.text2));
            response.clone().on_hover_text(format!(
                "{}\nstored components [0] and [1], scaled relative to this sample\nscreen distance is not ANN distance",
                point.evidence.label()
            ));
        }
    }

    if response.clicked() {
        // Empty-space clicks clear the inspector selection.
        let hit = response
            .interact_pointer_pos()
            .and_then(|at| nearest(plot, camera, points, at));
        out.push(Action::SelectPoint(hit));
    }
    if response.double_clicked() {
        out.push(Action::ResetCamera);
    }

    response.widget_info(|| {
        egui::WidgetInfo::labeled(
            egui::WidgetType::Other,
            true,
            "Raw vector components zero and one, each normalized within this sample. Drag to pan, wheel to zoom, double click to reset.",
        )
    });
    response.ctx.accesskit_node_builder(response.id, |node| {
        node.set_role(egui::accesskit::Role::Canvas);
    });
}

fn paint_dimension_axes(painter: &Painter, plot: Rect, p: &crate::theme::Palette) {
    // The compute shader can extend a point by at most eight physical pixels. Keep the
    // axes outside that footprint so normalized extrema remain points, not line joints.
    const CLEARANCE: f32 = 10.0;
    let bottom_left = plot.left_bottom() + egui::vec2(-CLEARANCE, CLEARANCE);
    let bottom_right = plot.right_bottom() + egui::vec2(CLEARANCE, CLEARANCE);
    let top_left = plot.left_top() + egui::vec2(-CLEARANCE, -CLEARANCE);
    let stroke = egui::Stroke::new(1.0, p.hairline_strong);

    painter.line_segment([bottom_left, bottom_right], stroke);
    painter.line_segment([top_left, bottom_left], stroke);
    painter.line_segment([bottom_right, bottom_right - egui::vec2(5.0, 3.0)], stroke);
    painter.line_segment([bottom_right, bottom_right - egui::vec2(5.0, -3.0)], stroke);
    painter.line_segment([bottom_left, bottom_left + egui::vec2(-3.0, -5.0)], stroke);
    painter.line_segment([bottom_left, bottom_left + egui::vec2(3.0, -5.0)], stroke);
    painter.text(
        Pos2::new(plot.center().x, bottom_left.y + 5.0),
        egui::Align2::CENTER_TOP,
        "stored[0] (relative) →",
        role::meta(),
        p.text3,
    );
    painter.text(
        Pos2::new(plot.right(), top_left.y - 5.0),
        egui::Align2::RIGHT_BOTTOM,
        "stored[1] (relative) ↓",
        role::meta(),
        p.text3,
    );
}

const PLOT_INSET_X: f32 = 24.0;
const PLOT_INSET_TOP: f32 = 28.0;
const PLOT_INSET_BOTTOM: f32 = 52.0;

/// Reserve enough canvas around normalized extrema, chrome, axes, and the count chip.
fn projection_rect(canvas: Rect, reserved_top: Option<f32>) -> Rect {
    let horizontal = PLOT_INSET_X.min(canvas.width() * 0.2);
    let chrome = reserved_top.map_or(0.0, |bottom| bottom - canvas.top() + 12.0);
    let top = PLOT_INSET_TOP.max(chrome).min(canvas.height() * 0.35);
    let bottom = PLOT_INSET_BOTTOM.min(canvas.height() * 0.3);
    Rect::from_min_max(
        Pos2::new(canvas.left() + horizontal, canvas.top() + top),
        Pos2::new(canvas.right() - horizontal, canvas.bottom() - bottom),
    )
}

fn canvas_sense() -> egui::Sense {
    // Pointer-only canvas gestures are not exposed as an unusable keyboard focus stop.
    egui::Sense::CLICK | egui::Sense::DRAG
}

/// How close the pointer has to be, in points, to pick a point up.
const PICK_RADIUS: f32 = 7.0;

/// Paint selection as chrome, not as an evidence verdict.
fn marker(painter: &Painter, plot: Rect, state: &State, point: &crate::model::Projected) {
    let p = &state.palette;
    let at = state.camera.to_screen(plot, egui::vec2(point.x, point.y));
    if !plot.contains(at) {
        return;
    }
    painter.circle_stroke(at, 13.0, egui::Stroke::new(1.4, p.accent));
    painter.circle_stroke(at, 5.0, egui::Stroke::new(1.6, p.text1));
}

/// The point nearest the pointer, within `PICK_RADIUS`.
///
/// Linear nearest-point search over the bounded display sample.
fn nearest(
    plot: Rect,
    camera: crate::state::Camera,
    points: &[crate::model::Projected],
    at: Pos2,
) -> Option<usize> {
    let mut best: Option<(usize, f32)> = None;
    for (i, point) in points.iter().enumerate() {
        let d = camera
            .to_screen(plot, egui::vec2(point.x, point.y))
            .distance_sq(at);
        if d <= PICK_RADIUS * PICK_RADIUS && best.is_none_or(|(_, b)| d < b) {
            best = Some((i, d));
        }
    }
    best.map(|(i, _)| i)
}

/// Inspect only facts carried by a sampled point.
pub fn inspector_body(painter: &Painter, insp: Rect, state: &State) {
    let p = &state.palette;
    let column = match state.active_doc() {
        Some(crate::state::Doc::Vector(name)) => state
            .vault()
            .vectors
            .iter()
            .find(|c| &c.qualified() == name),
        _ => None,
    };
    let Some(column) = column else {
        inspector_empty(
            painter,
            insp,
            "Point details",
            "No vector column is open.",
            p,
        );
        return;
    };
    if let Some(notice) = sample_notice(Some(column)) {
        inspector_empty(painter, insp, notice.headline, &notice.detail, p);
        return;
    }
    let Some(i) = state.selected_point else {
        inspector_empty(
            painter,
            insp,
            "Point details",
            "Select a dot to inspect its source column, display coordinates, and available evidence.",
            p,
        );
        return;
    };
    let Some(point) = column.points.get(i) else {
        inspector_empty(
            painter,
            insp,
            "Point details",
            "That point is no longer in the sample.",
            p,
        );
        return;
    };

    // Sample position is not a source row id.
    let head = inspector_header(
        painter,
        insp,
        "Sample point",
        &format!("{} of {}", i + 1, w::thousands(column.points.len() as u64)),
        p,
    );

    let mut y = head.bottom() + 22.0;
    w::text_left(
        painter,
        Pos2::new(insp.left() + metrics::PANE_PAD, y),
        "COLUMN",
        role::meta(),
        p.text3,
    );
    y += 18.0;
    let dim = format!("{} dimensions", column.dim);
    let rows = w::thousands(column.total);
    y = field_list(
        painter,
        Pos2::new(insp.left() + metrics::PANE_PAD, y),
        &[
            Field::new("name", &column.qualified()),
            Field::new("width", &dim),
            Field::new("rows", &rows),
        ],
        insp.width() - metrics::PANE_PAD * 2.0,
        p,
    );

    y += 10.0;
    w::text_left(
        painter,
        Pos2::new(insp.left() + metrics::PANE_PAD, y),
        "COORDINATES",
        role::meta(),
        p.text3,
    );
    y += 18.0;
    let (dimension_1, dimension_2) = coordinate_values(point);
    y = field_list(
        painter,
        Pos2::new(insp.left() + metrics::PANE_PAD, y),
        &[
            Field::new("stored[0] (relative)", &dimension_1),
            Field::new("stored[1] (relative)", &dimension_2),
        ],
        insp.width() - metrics::PANE_PAD * 2.0,
        p,
    );

    y += 10.0;
    w::text_left(
        painter,
        Pos2::new(insp.left() + metrics::PANE_PAD, y),
        "ROW PROOF",
        role::meta(),
        p.text3,
    );
    y += 18.0;
    // `NotAttestable` uses neutral text rather than an evidence colour.
    let verdict = match point.evidence.colour(p) {
        Some(ink) => Field::tinted("status", sample_evidence_label(point.evidence), ink),
        None => Field::new("status", sample_evidence_label(point.evidence)),
    };
    y = field_list(
        painter,
        Pos2::new(insp.left() + metrics::PANE_PAD, y),
        &[verdict],
        insp.width() - metrics::PANE_PAD * 2.0,
        p,
    );
    y += 4.0;
    let scope = painter.layout(
        sample_evidence_scope(point.evidence).to_owned(),
        role::meta(),
        p.text4,
        insp.width() - 24.0,
    );
    let scope_h = scope.size().y;
    painter.galley(
        Pos2::new(insp.left() + metrics::PANE_PAD, y),
        scope,
        p.text4,
    );

    let note = painter.layout(
        "This raw component plot compares stored vector values [0] and [1]. Each axis is \
         independently scaled to 0-1 over the bounded sample. It is not PCA or UMAP, and \
         screen distance is not ANN distance. Stored vectors remain in the engine."
            .to_owned(),
        role::meta(),
        p.text4,
        insp.width() - 24.0,
    );
    painter.galley(
        Pos2::new(insp.left() + metrics::PANE_PAD, y + scope_h + 14.0),
        note,
        p.text4,
    );
}

fn coordinate_values(point: &crate::model::Projected) -> (String, String) {
    (format!("{:.4}", point.x), format!("{:.4}", point.y))
}

fn sample_evidence_label(evidence: crate::theme::Evidence) -> &'static str {
    match evidence {
        crate::theme::Evidence::NotAttestable => "No row proof",
        other => other.label(),
    }
}

fn sample_evidence_scope(evidence: crate::theme::Evidence) -> &'static str {
    match evidence {
        crate::theme::Evidence::NotAttestable => {
            "this sampled vector row has no per-row cryptographic attestation"
        }
        other => other.scope(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pointer_only_canvas_is_not_a_keyboard_focus_stop() {
        let sense = canvas_sense();
        assert!(sense.senses_click());
        assert!(sense.senses_drag());
        assert!(!sense.is_focusable());
    }

    #[test]
    fn inspector_labels_x_as_dimension_one_and_y_as_dimension_two() {
        let point = crate::model::Projected {
            x: 0.125,
            y: 0.875,
            evidence: crate::theme::Evidence::NotAttestable,
        };

        assert_eq!(
            coordinate_values(&point),
            ("0.1250".into(), "0.8750".into())
        );
    }

    #[test]
    fn a_generic_vector_sample_does_not_claim_plaintext_region_semantics() {
        let evidence = crate::theme::Evidence::NotAttestable;
        assert_eq!(sample_evidence_label(evidence), "No row proof");
        assert_eq!(
            sample_evidence_scope(evidence),
            "this sampled vector row has no per-row cryptographic attestation"
        );
        assert_ne!(sample_evidence_scope(evidence), evidence.scope());
    }

    #[test]
    fn loading_failure_and_empty_data_have_distinct_visible_notices() {
        let mut column = crate::model::VectorColumn::unloaded("documents", "embedding", 2, 4);
        let loading = sample_notice(Some(&column)).unwrap();
        assert_eq!(loading.headline, "Loading vector sample");
        assert!(!loading.alert);

        assert!(column.begin_loading());
        column.fail_sample(crate::engine::StudioError::new(
            crate::engine::Kind::Data,
            "invalid vector bytes",
        ));
        let failed = sample_notice(Some(&column)).unwrap();
        assert_eq!(failed.headline, "Vector sample could not be loaded");
        assert!(failed.detail.contains("invalid vector bytes"));
        assert!(failed.detail.contains("Reopen the vault to retry"));
        assert!(failed.alert);

        let empty = crate::model::VectorColumn::new("documents", "embedding", 2, 0, Vec::new());
        let empty = sample_notice(Some(&empty)).unwrap();
        assert_eq!(empty.headline, "No vectors to plot");
        assert_eq!(empty.detail, "documents.embedding has no rows.");
        assert!(!empty.alert);

        let values_missing =
            crate::model::VectorColumn::new("documents", "embedding", 2, 4, Vec::new());
        let values_missing = sample_notice(Some(&values_missing)).unwrap();
        assert!(values_missing.detail.contains("has 4 rows"));
        assert!(values_missing.detail.contains("no stored vector values"));
    }

    #[test]
    fn a_ready_nonempty_sample_reaches_the_interactive_canvas() {
        let column = crate::model::VectorColumn::new(
            "documents",
            "embedding",
            2,
            1,
            vec![crate::model::Projected {
                x: 0.5,
                y: 0.5,
                evidence: crate::theme::Evidence::NotAttestable,
            }],
        );

        assert_eq!(sample_notice(Some(&column)), None);
    }

    #[test]
    fn normalized_extrema_have_room_for_their_full_splats() {
        let canvas = Rect::from_min_size(Pos2::ZERO, Vec2::new(800.0, 600.0));
        let plot = projection_rect(canvas, None);
        let camera = crate::state::Camera::default();
        let minimum = camera.to_screen(plot, Vec2::ZERO);
        let maximum = camera.to_screen(plot, Vec2::splat(1.0));

        assert!(minimum.x - canvas.left() >= PLOT_INSET_X);
        assert!(minimum.y - canvas.top() >= PLOT_INSET_TOP);
        assert!(canvas.right() - maximum.x >= PLOT_INSET_X);
        assert!(canvas.bottom() - maximum.y >= PLOT_INSET_BOTTOM);
    }

    #[test]
    fn legend_space_is_removed_from_the_data_rect() {
        let canvas = Rect::from_min_size(Pos2::ZERO, Vec2::new(800.0, 600.0));
        let legend_bottom = canvas.top() + 50.0;
        let plot = projection_rect(canvas, Some(legend_bottom));

        assert!(plot.top() >= legend_bottom + 12.0);
    }
}
