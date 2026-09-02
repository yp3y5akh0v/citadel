//! Virtualized evidence-aware data grid.

use crate::fonts::role;
use crate::rail;
use crate::theme::{metrics, Evidence, Palette};
use crate::ui;
use crate::widgets;
use egui::{Align2, Color32, Painter, Pos2, Rect, Ui, Vec2};

#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum Density {
    Compact,
    Default,
    Comfortable,
}

impl Density {
    pub fn row_height(self) -> f32 {
        match self {
            Self::Compact => metrics::ROW_COMPACT,
            Self::Default => metrics::ROW_DEFAULT,
            Self::Comfortable => metrics::ROW_COMFORTABLE,
        }
    }
    /// Exhaustive on purpose: a new density must not silently inherit a font.
    pub fn cell_font(self) -> egui::FontId {
        match self {
            Self::Compact => role::cell_compact(),
            Self::Default | Self::Comfortable => role::cell(),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Compact => "Compact",
            Self::Default => "Default",
            Self::Comfortable => "Comfort",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Align {
    Left,
    Right,
}

pub struct Column {
    /// Owned because query column names are runtime data.
    pub label: String,
    /// Fixed width, or None to take the remaining space.
    pub width: Option<f32>,
    pub align: Align,
}

impl Column {
    /// A column that takes the space left over.
    pub fn flex(label: impl Into<String>, align: Align) -> Self {
        Self {
            label: label.into(),
            width: None,
            align,
        }
    }

    pub fn fixed(label: impl Into<String>, width: f32, align: Align) -> Self {
        Self {
            label: label.into(),
            width: Some(width),
            align,
        }
    }
}

pub struct Cell {
    pub text: String,
    pub colour: Option<Color32>,
    /// Downsampled vector components in -1..=1 for the magnitude strip.
    pub strip: Option<Vec<f32>>,
}

impl Cell {
    pub fn new(s: impl Into<String>) -> Self {
        Self {
            text: s.into(),
            colour: None,
            strip: None,
        }
    }
    pub fn tinted(s: impl Into<String>, c: Color32) -> Self {
        Self {
            text: s.into(),
            colour: Some(c),
            strip: None,
        }
    }
    pub fn vector(s: impl Into<String>, strip: Vec<f32>) -> Self {
        Self {
            text: s.into(),
            colour: None,
            strip: Some(strip),
        }
    }
}

/// One data row; selection and hover remain view state.
pub struct Row {
    pub evidence: Evidence,
    pub cells: Vec<Cell>,
    /// Shown on hover when the cells cannot hold the whole value. Empty for no tooltip.
    pub tooltip: String,
}

impl Row {
    pub fn new(evidence: Evidence, cells: Vec<Cell>) -> Self {
        Self {
            evidence,
            cells,
            tooltip: String::new(),
        }
    }

    pub fn tooltip(mut self, text: impl Into<String>) -> Self {
        self.tooltip = text.into();
        self
    }
}

/// Minimum readable width of a flex column.
const FLEX_MIN: f32 = 220.0;

/// Resolve shared header/body extents; omit rightmost fixed columns when space runs out.
fn lay_out(rect: Rect, columns: &[Column]) -> Vec<Option<(f32, f32)>> {
    let budget = rect.width() - metrics::GUTTER_W;
    let flex_count = columns.iter().filter(|c| c.width.is_none()).count();
    // All-fixed grids keep their declared widths and may leave slack.
    let Some(flex_count) = std::num::NonZeroUsize::new(flex_count) else {
        let mut x = rect.left() + metrics::GUTTER_W;
        return columns
            .iter()
            .map(|c| {
                let w = c.width.expect("no flex columns in this branch");
                let bound = (x, x + w);
                x += w;
                Some(bound)
            })
            .collect();
    };
    let flex_count = flex_count.get();

    // Drop from the right until flex columns reach their minimum.
    let mut keep: Vec<bool> = columns.iter().map(|_| true).collect();
    loop {
        let fixed: f32 = columns
            .iter()
            .zip(&keep)
            .filter(|(_, k)| **k)
            .filter_map(|(c, _)| c.width)
            .sum();
        if budget - fixed >= FLEX_MIN * flex_count as f32 {
            break;
        }
        let droppable = columns
            .iter()
            .zip(&keep)
            .enumerate()
            .filter(|(_, (c, k))| **k && c.width.is_some())
            .map(|(i, _)| i)
            .next_back();
        match droppable {
            Some(i) => keep[i] = false,
            None => break, // only flex columns remain; they take what is left
        }
    }

    let fixed: f32 = columns
        .iter()
        .zip(&keep)
        .filter(|(_, k)| **k)
        .filter_map(|(c, _)| c.width)
        .sum();
    let flex_w = ((budget - fixed) / flex_count as f32).max(80.0);

    let mut out = Vec::with_capacity(columns.len());
    let mut x = rect.left() + metrics::GUTTER_W;
    for (c, k) in columns.iter().zip(&keep) {
        if !k {
            out.push(None);
            continue;
        }
        let w = c.width.unwrap_or(flex_w);
        out.push(Some((x, x + w)));
        x += w;
    }
    out
}

/// Cell painting inputs.
struct Ink<'a> {
    align: Align,
    font: egui::FontId,
    colour: Color32,
    palette: &'a Palette,
}

fn draw_cell(painter: &Painter, bounds: (f32, f32), y: f32, cell: &Cell, ink: &Ink<'_>) {
    let Ink {
        align,
        font,
        colour,
        palette,
    } = ink;
    let (align, colour) = (*align, *colour);
    let font = font.clone();
    let (x0, x1) = bounds;
    let max_w = (x1 - x0 - metrics::CELL_PAD_X * 2.0).max(0.0);
    // Elide rather than wrap: a data row is one line tall by definition.
    let galley = widgets::elide_galley(painter, &cell.text, font, max_w, colour);
    // Vector text sits above its magnitude strip.
    let dy = if cell.strip.is_some() { -5.0 } else { 0.0 };
    let pos = match align {
        Align::Left => Pos2::new(x0 + metrics::CELL_PAD_X, y + dy - galley.size().y * 0.5),
        Align::Right => Pos2::new(
            x1 - metrics::CELL_PAD_X - galley.size().x,
            y + dy - galley.size().y * 0.5,
        ),
    };
    painter.galley(pos, galley, colour);

    if let Some(components) = &cell.strip {
        let strip = Rect::from_min_size(
            Pos2::new(x0 + metrics::CELL_PAD_X, y + 7.0),
            Vec2::new(max_w, 3.0),
        );
        paint_strip(painter, strip, components, palette);
    }
}

/// Magnitude by height and sign by baseline direction, without semantic colour.
pub fn paint_strip(painter: &Painter, rect: Rect, components: &[f32], p: &Palette) {
    if components.is_empty() {
        return;
    }
    let peak = components
        .iter()
        .fold(0.0_f32, |a, b| a.max(b.abs()))
        .max(f32::EPSILON);
    let bar_w = rect.width() / components.len() as f32;
    let ink = Palette::tint(p.text2, 110);
    for (i, v) in components.iter().enumerate() {
        let h = (v.abs() / peak * rect.height()).max(0.5);
        let x = rect.left() + i as f32 * bar_w;
        // Positive grows up from the baseline, negative grows down.
        let (y0, y1) = if *v >= 0.0 {
            (rect.bottom() - h, rect.bottom())
        } else {
            (rect.top(), rect.top() + h)
        };
        painter.rect_filled(
            Rect::from_min_max(Pos2::new(x, y0), Pos2::new(x + bar_w.max(1.0) - 0.4, y1)),
            0.0,
            ink,
        );
    }
}

/// Split a grid into the shared header and body geometry.
fn split(rect: Rect) -> (Rect, Rect) {
    let header = Rect::from_min_size(rect.left_top(), Vec2::new(rect.width(), metrics::HEADER_H));
    let body = Rect::from_min_max(Pos2::new(rect.left(), header.bottom()), rect.right_bottom());
    (header, body)
}

/// Grid view state; indices are relative to the painted window.
struct View<'a> {
    density: Density,
    palette: &'a Palette,
    ppp: f32,
    /// The true index of `rows[0]`, so the gutter numbers a scrolled window correctly.
    first_index: usize,
    selected: Option<usize>,
    hovered: Option<usize>,
}

/// Paint header plus rows.
fn paint(painter: &Painter, rect: Rect, columns: &[Column], rows: &[Row], view: &View<'_>) {
    let View {
        density,
        palette: p,
        ppp,
        first_index,
        selected,
        hovered,
    } = *view;
    let bounds = lay_out(rect, columns);

    let (header, _) = split(rect);
    widgets::fill(painter, header, p.ground3);
    widgets::hairline_bottom(painter, header, p.hairline);
    for (col, bound) in columns.iter().zip(&bounds) {
        let Some(bound) = bound else { continue };
        let cell = Cell::new(col.label.to_uppercase());
        draw_cell(
            painter,
            *bound,
            header.center().y,
            &cell,
            &Ink {
                align: col.align,
                font: role::column_header(),
                colour: p.text3,
                palette: p,
            },
        );
    }

    let row_h = density.row_height();
    let mut y = header.bottom();
    for (index, row) in rows.iter().enumerate() {
        let r = Rect::from_min_size(Pos2::new(rect.left(), y), Vec2::new(rect.width(), row_h));
        if r.bottom() > rect.bottom() {
            break;
        }

        if selected == Some(index) {
            // Chroma distinguishes focused selection without changing luminance.
            widgets::fill(painter, r, Palette::tint(p.accent, 41));
        } else if hovered == Some(index) {
            widgets::fill(painter, r, p.wash(10));
        } else if row.evidence == Evidence::Tampered {
            // Use the fill tier so semantic colour does not reduce text contrast.
            widgets::fill(painter, r, Palette::tint(p.tampered, 20));
        }
        widgets::hairline_bottom(painter, r, p.hairline_quiet);

        // Keep the evidence rail in the sticky gutter.
        rail::paint(painter, r, row.evidence, p, ppp);
        let n = (first_index + index + 1).to_string();
        painter.text(
            Pos2::new(rect.left() + metrics::GUTTER_W - 8.0, r.center().y),
            Align2::RIGHT_CENTER,
            n,
            role::gutter(),
            p.text4,
        );

        for ((col, bound), cell) in columns.iter().zip(&bounds).zip(&row.cells) {
            let Some(bound) = bound else { continue };
            draw_cell(
                painter,
                *bound,
                r.center().y,
                cell,
                &Ink {
                    align: col.align,
                    font: density.cell_font(),
                    colour: cell.colour.unwrap_or(p.text1),
                    palette: p,
                },
            );
        }
        y = r.bottom();
    }
}

/// What the user did to a grid this frame.
#[derive(Default)]
pub struct Event {
    pub clicked: Option<usize>,
    /// A wheel move that would otherwise leave the selected row off screen moves it to
    /// the nearest visible edge.
    pub selected_to: Option<usize>,
    /// How many rows fitted. Anything that reports a scope must use this, not a guess.
    pub visible: usize,
    /// New first-visible row, if the wheel or a selection move changed it.
    pub scrolled_to: Option<usize>,
}

/// Caller-owned scrolling, selection and modal state.
#[derive(Clone, Copy)]
pub struct Scroll {
    pub density: Density,
    pub selected: usize,
    pub first: usize,
    /// True while a modal owns the input, so the grid under it ignores the wheel.
    pub modal: bool,
}

/// Interactive virtualized grid; only visible rows are laid out.
pub fn show(
    ui: &mut Ui,
    rect: Rect,
    columns: &[Column],
    rows: &[Row],
    scroll: Scroll,
    p: &Palette,
) -> Event {
    let Scroll {
        density,
        selected,
        first,
        modal: _,
    } = scroll;
    let ppp = ui.ctx().pixels_per_point();
    let row_h = density.row_height();
    let mut event = Event::default();

    let (_, body) = split(rect);
    let visible = ((body.height() / row_h).floor() as usize).max(1);
    event.visible = visible;
    let max_first = rows.len().saturating_sub(visible);

    // Report clamping so verification scope matches the rows actually painted.
    let mut first = first.min(max_first);
    if first != scroll.first {
        event.scrolled_to = Some(first);
    }

    // Keep keyboard selection visible.
    if selected < first {
        first = selected;
        event.scrolled_to = Some(first);
    } else if selected >= first + visible {
        // Re-clamp stale selections inherited from longer documents.
        first = (selected + 1 - visible).min(max_first);
        event.scrolled_to = Some(first);
    }

    // Modal overlays own scrolling.
    let wheel = ui.input(|i| i.smooth_scroll_delta.y);
    if !scroll.modal && wheel != 0.0 && ui.rect_contains_pointer(body) {
        let by = (wheel / row_h).round() as i32;
        if by != 0 {
            let next = (first as i32 - by).clamp(0, max_first as i32) as usize;
            if next != first {
                first = next;
                event.scrolled_to = Some(first);
                let last = (first + visible).min(rows.len()).saturating_sub(1);
                let selected = selected.clamp(first, last);
                if selected != scroll.selected {
                    event.selected_to = Some(selected);
                }
            }
        }
    }

    let window = &rows[first.min(rows.len())..(first + visible).min(rows.len())];

    // Hit test first so hover fills paint underneath the text in one pass.
    let mut hovered = None;
    let mut focused = None;
    for (i, row) in window.iter().enumerate() {
        let r = Rect::from_min_size(
            Pos2::new(body.left(), body.top() + i as f32 * row_h),
            Vec2::new(body.width(), row_h),
        );
        if r.bottom() > body.bottom() {
            break;
        }
        let index = first + i;
        let response = ui.interact(r, egui::Id::new(("row", index)), egui::Sense::click());
        if response.hovered() {
            hovered = Some(i);
        }
        if response.clicked() {
            event.clicked = Some(index);
        }
        if response.has_focus() {
            focused = Some(r);
        }

        // AccessKit lacks a portable grid-row role, so the node name carries the row.
        let name = row_name(index, rows.len(), row, columns);
        let is_selected = index == selected;
        response.widget_info(|| {
            egui::WidgetInfo::selected(egui::WidgetType::SelectableLabel, true, is_selected, &name)
        });

        if response.hovered() {
            if row.tooltip.is_empty() {
                response.on_hover_text(row_values(row, columns));
            } else {
                response.on_hover_text(&row.tooltip);
            }
        }
    }

    let painter = ui.painter().clone();
    paint(
        &painter,
        rect,
        columns,
        window,
        &View {
            density,
            palette: p,
            ppp,
            first_index: first,
            selected: selected.checked_sub(first).filter(|i| *i < window.len()),
            hovered,
        },
    );

    if rows.len() > visible {
        scrollbar(&painter, body, first, visible, rows.len(), p);
    }
    if let Some(r) = focused {
        ui::focus_ring(ui, r, p, 0);
    }
    event
}

/// Compose one row into a sentence: position, then each column as "header, value", then
/// the evidence state last so it is the thing most recently heard.
fn row_name(index: usize, total: usize, row: &Row, columns: &[Column]) -> String {
    let mut parts = vec![format!("Row {} of {}", index + 1, total)];
    for (col, cell) in columns.iter().zip(&row.cells) {
        if !cell.text.is_empty() {
            parts.push(format!("{}, {}", col.label, cell.text));
        }
    }
    // Lowercase the verdict because it ends a sentence.
    parts.push(row.evidence.label().to_lowercase());
    parts.join(". ")
}

fn row_values(row: &Row, columns: &[Column]) -> String {
    columns
        .iter()
        .zip(&row.cells)
        .filter(|(_, cell)| !cell.text.is_empty())
        .map(|(column, cell)| format!("{}: {}", column.label, cell.text))
        .collect::<Vec<_>>()
        .join("\n")
}

/// A thin overlay scrollbar. Present only when there is something off screen, so its
/// absence is itself information: everything is visible.
fn scrollbar(
    painter: &Painter,
    body: Rect,
    first: usize,
    visible: usize,
    total: usize,
    p: &Palette,
) {
    let track = Rect::from_min_max(
        Pos2::new(body.right() - 8.0, body.top()),
        Pos2::new(body.right() - 4.0, body.bottom()),
    );
    painter.rect_filled(track, egui::CornerRadius::same(2), p.wash(10));
    let frac = visible as f32 / total as f32;
    let thumb_h = (track.height() * frac).max(24.0);
    let span = track.height() - thumb_h;
    let pos = first as f32 / (total - visible).max(1) as f32;
    let top = track.top() + span * pos;
    painter.rect_filled(
        Rect::from_min_size(
            Pos2::new(track.left(), top),
            Vec2::new(track.width(), thumb_h),
        ),
        egui::CornerRadius::same(2),
        p.wash(60),
    );
}
