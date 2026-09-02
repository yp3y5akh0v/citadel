//! Custom-painted interactive primitives over explicit screen-owned rectangles.

use crate::fonts::role;
use crate::theme::{radius, Palette};
use crate::widgets as w;
use egui::{Align2, Color32, CornerRadius, Id, Rect, Response, Sense, Stroke, Ui, Vec2};

/// A focus ring drawn flush against the outer edge. WCAG 2.2 accepts a flush 2pt
/// indicator; one inset away from the edge would need 3pt.
pub(crate) fn focus_ring(ui: &Ui, rect: Rect, p: &Palette, r: u8) {
    ui.painter().rect_stroke(
        rect,
        CornerRadius::same(r),
        Stroke::new(2.0, p.focus),
        egui::StrokeKind::Inside,
    );
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ButtonKind {
    Ghost,
    Primary,
    Outlined,
    /// Erase, revoke: destructive and irreversible.
    Danger,
}

/// Button appearance, identity, state and availability.
pub struct Btn<'a> {
    pub label: &'a str,
    pub kind: ButtonKind,
    /// Stable identity for controls whose label changes when pressed.
    pub key: &'a str,
    /// `None` for actions; `Some` only for controls with a toggled state.
    pub on: Option<bool>,
    /// Why a disabled control is unavailable.
    pub enabled: bool,
    pub tooltip: Option<&'a str>,
}

impl<'a> Btn<'a> {
    pub fn new(label: &'a str, kind: ButtonKind) -> Self {
        Self {
            label,
            kind,
            key: label,
            on: None,
            enabled: true,
            tooltip: None,
        }
    }

    /// Pin the identity for a button whose label changes when it is pressed.
    pub fn keyed(mut self, key: &'a str) -> Self {
        self.key = key;
        self
    }

    /// Only `Ghost` paints a toggled state.
    pub fn toggled(mut self, on: bool) -> Self {
        debug_assert_eq!(
            self.kind,
            ButtonKind::Ghost,
            "only a ghost button paints a toggled state"
        );
        self.on = Some(on);
        self
    }
    /// Mark unavailable and retain a discoverable reason.
    pub fn unavailable(mut self, why: &'a str) -> Self {
        self.enabled = false;
        self.tooltip = Some(why);
        self
    }
    pub fn tip(mut self, text: &'a str) -> Self {
        self.tooltip = Some(text);
        self
    }
}

/// Give custom buttons explicit action/toggle semantics and an accessible name.
fn button_info(on: Option<bool>, enabled: bool, name: &str) -> egui::WidgetInfo {
    match on {
        Some(on) => egui::WidgetInfo::selected(egui::WidgetType::Button, enabled, on, name),
        None => egui::WidgetInfo::labeled(egui::WidgetType::Button, enabled, name),
    }
}

fn describe_button(response: &Response, description: Option<&str>) {
    if let Some(description) = description {
        response
            .ctx
            .accesskit_node_builder(response.id, |node| node.set_description(description));
    }
}

pub fn button(ui: &mut Ui, rect: Rect, btn: Btn<'_>, p: &Palette) -> Response {
    let Btn {
        label,
        kind,
        key,
        on,
        enabled,
        tooltip,
    } = btn;

    // Geometry must not enter an id or resizing drops keyboard focus.
    let sense = if enabled {
        Sense::click()
    } else {
        Sense::hover()
    };
    let response = ui.interact(rect, Id::new(("btn", key)), sense);
    let painter = ui.painter();

    if !enabled {
        if matches!(kind, ButtonKind::Primary) {
            painter.rect_filled(rect, CornerRadius::same(radius::CONTROL), p.wash(15));
        }
        if let Some(border) = match kind {
            ButtonKind::Outlined | ButtonKind::Danger => Some(p.hairline),
            _ => None,
        } {
            painter.rect_stroke(
                rect,
                CornerRadius::same(radius::CONTROL),
                Stroke::new(1.0, border),
                egui::StrokeKind::Inside,
            );
        }
        painter.text(
            rect.center(),
            Align2::CENTER_CENTER,
            label,
            role::chrome(),
            p.text4,
        );
        let name = label.to_owned();
        response.widget_info(|| button_info(on, false, &name));
        describe_button(&response, tooltip);
        return match tooltip {
            Some(text) => response.on_hover_text(text),
            None => response,
        };
    }

    let (fill, text, border) = match kind {
        ButtonKind::Primary => {
            let base = if response.is_pointer_button_down_on() {
                p.focus
            } else if response.hovered() {
                Palette::lighten(p.accent_solid, 12)
            } else {
                p.accent_solid
            };
            (Some(base), Color32::WHITE, None)
        }
        ButtonKind::Outlined => {
            let fill = response.hovered().then(|| p.wash(15));
            (fill, p.text1, Some(p.hairline_strong))
        }
        ButtonKind::Danger => {
            let fill = response.hovered().then(|| Palette::tint(p.tampered, 26));
            (fill, p.tampered, Some(p.hairline_strong))
        }
        ButtonKind::Ghost => {
            if on == Some(true) {
                (Some(Palette::tint(p.accent, 36)), p.accent, None)
            } else if response.is_pointer_button_down_on() {
                (Some(p.wash(41)), p.text1, None)
            } else if response.hovered() {
                (Some(p.wash(20)), p.text1, None)
            } else {
                (None, p.text2, None)
            }
        }
    };

    if let Some(fill) = fill {
        painter.rect_filled(rect, CornerRadius::same(radius::CONTROL), fill);
    }
    if let Some(border) = border {
        painter.rect_stroke(
            rect,
            CornerRadius::same(radius::CONTROL),
            Stroke::new(1.0, border),
            egui::StrokeKind::Inside,
        );
    }
    painter.text(
        rect.center(),
        Align2::CENTER_CENTER,
        label,
        role::chrome(),
        text,
    );
    if response.has_focus() {
        focus_ring(ui, rect, p, radius::CONTROL);
    }
    // Custom paint needs explicit AccessKit metadata.
    let name = label.to_owned();
    response.widget_info(|| button_info(on, true, &name));
    describe_button(&response, tooltip);
    match tooltip {
        Some(text) => response.on_hover_text(text),
        None => response,
    }
}

/// One document tab. Returns (focus, close) so the caller can tell a click on the tab
/// from a click on its close button.
pub fn tab(
    ui: &mut Ui,
    rect: Rect,
    label: &str,
    active: bool,
    p: &Palette,
) -> (Response, Response) {
    let id = Id::new(("tab", label));
    let response = ui.interact(rect, id, Sense::click());
    let close_rect = Rect::from_center_size(
        egui::pos2(rect.right() - 16.0, rect.center().y),
        Vec2::splat(20.0),
    );
    let close = ui.interact(close_rect, id.with("close"), Sense::click());

    let tab_name = format!("{label}{}", if active { ", selected" } else { "" });
    response.widget_info(|| {
        egui::WidgetInfo::selected(egui::WidgetType::SelectableLabel, true, active, &tab_name)
    });
    let close_name = format!("Close {label}");
    close.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, &close_name));

    let painter = ui.painter();
    if active {
        painter.rect_filled(rect, 0.0, p.ground1);
        painter.rect_filled(
            Rect::from_min_max(rect.left_top(), egui::pos2(rect.right(), rect.top() + 2.0)),
            0.0,
            p.accent,
        );
    } else {
        let fill = if response.hovered() {
            p.wash(15)
        } else {
            p.ground3
        };
        painter.rect_filled(rect, 0.0, fill);
        w::hairline_bottom(painter, rect, p.hairline);
    }

    let (font, colour) = if active {
        (role::chrome_strong(), p.text1)
    } else if response.hovered() {
        (role::chrome(), p.text1)
    } else {
        (role::chrome(), p.text2)
    };
    // Reserve close-button room so hover never reflows the label.
    let galley = w::elide_galley(painter, label, font, rect.width() - 38.0, colour);
    let elided = galley.text() != label;
    painter.galley(
        egui::pos2(rect.left() + 10.0, rect.center().y - galley.size().y * 0.5),
        galley,
        colour,
    );

    if active || response.hovered() || close.hovered() {
        let c = close_rect.center();
        let stroke = Stroke::new(1.2, if close.hovered() { p.text1 } else { p.text3 });
        painter.line_segment(
            [
                egui::pos2(c.x - 4.0, c.y - 4.0),
                egui::pos2(c.x + 4.0, c.y + 4.0),
            ],
            stroke,
        );
        painter.line_segment(
            [
                egui::pos2(c.x + 4.0, c.y - 4.0),
                egui::pos2(c.x - 4.0, c.y + 4.0),
            ],
            stroke,
        );
    }
    if !active {
        painter.rect_filled(
            Rect::from_min_size(
                egui::pos2(rect.right() - 1.0, rect.top() + 7.0),
                Vec2::new(1.0, 20.0),
            ),
            0.0,
            p.hairline,
        );
    }
    // Both click targets need a visible keyboard-focus indicator.
    if response.has_focus() {
        focus_ring(ui, rect, p, 0);
    }
    if close.has_focus() {
        focus_ring(ui, close_rect, p, radius::CONTROL);
    }

    // Preserve elided tab titles on hover.
    let response = if elided {
        response.on_hover_text(label)
    } else {
        response
    };
    (response, close)
}

/// A sidebar row. The hover and selection band spans the full pane width, not the row's
/// content rect, so it reaches both pane edges.
pub fn tree_row(
    ui: &mut Ui,
    rect: Rect,
    label: &str,
    meta: Option<&str>,
    selected: bool,
    p: &Palette,
) -> Response {
    let response = ui.interact(rect, Id::new(("tree", label)), Sense::click());
    let painter = ui.painter();

    let fill = if selected {
        Some(p.wash(26))
    } else if response.hovered() {
        Some(p.wash(9))
    } else {
        None
    };
    if let Some(fill) = fill {
        painter.rect_filled(
            rect.shrink2(Vec2::new(6.0, 0.0)),
            CornerRadius::same(radius::PILL),
            fill,
        );
    }
    let colour = if selected || response.hovered() {
        p.text1
    } else {
        p.text2
    };
    let meta_w = meta.map_or(0.0, |meta| {
        painter
            .layout_no_wrap(meta.to_owned(), role::meta_mono(), p.text4)
            .size()
            .x
    });
    let label_room = (rect.width() - 20.0 - 14.0 - meta_w - 10.0).max(0.0);
    let galley = w::elide_galley(painter, label, role::chrome(), label_room, colour);
    let elided = galley.text() != label;
    painter.galley(
        egui::pos2(rect.left() + 20.0, rect.center().y - galley.size().y * 0.5),
        galley,
        colour,
    );
    if let Some(meta) = meta {
        w::text_right(
            painter,
            egui::pos2(rect.right() - 14.0, rect.center().y),
            meta,
            role::meta_mono(),
            p.text4,
        );
    }
    if response.has_focus() {
        focus_ring(ui, rect.shrink2(Vec2::new(6.0, 0.0)), p, radius::PILL);
    }
    // Include trailing metadata in the accessible name.
    let name = match meta {
        Some(meta) => format!("{label}, {meta}"),
        None => label.to_owned(),
    };
    response.widget_info(|| {
        egui::WidgetInfo::selected(egui::WidgetType::SelectableLabel, true, selected, &name)
    });
    if elided {
        response.on_hover_text(label)
    } else {
        response
    }
}

/// A passphrase field.
pub struct Secret<'a> {
    pub id: Id,
    /// Stable accessible name, independent of the placeholder.
    pub label: &'a str,
    pub value: &'a str,
    /// Instruction shown while the value is empty.
    pub placeholder: &'a str,
    /// Show the characters instead of dots.
    pub revealed: bool,
    /// The value was refused, which is a stronger statement than "empty".
    pub refused: bool,
    /// Neither claims focus nor accepts keys.
    pub frozen: bool,
    /// Take focus on arrival when nothing else holds it.
    pub autofocus: bool,
}

/// Changes produced by one frame of the field.
pub struct SecretOut {
    pub response: Response,
    /// The value after this frame's keys, when they changed it.
    pub edited: Option<crate::state::Passphrase>,
    /// The reveal control was clicked.
    pub toggled: bool,
}

#[derive(Clone, Copy, Default)]
struct SecretEditState {
    anchor: usize,
    cursor: usize,
    dragging: bool,
    ime_active: bool,
}

impl SecretEditState {
    fn clamp(&mut self, len: usize) {
        self.anchor = self.anchor.min(len);
        self.cursor = self.cursor.min(len);
    }

    fn range(self) -> std::ops::Range<usize> {
        self.anchor.min(self.cursor)..self.anchor.max(self.cursor)
    }

    fn move_to(&mut self, cursor: usize, extend: bool) {
        self.cursor = cursor;
        if !extend {
            self.anchor = cursor;
        }
    }
}

fn char_byte(text: &str, index: usize) -> usize {
    text.char_indices()
        .nth(index)
        .map_or(text.len(), |(byte, _)| byte)
}

fn replace_secret_range(
    value: &str,
    range: std::ops::Range<usize>,
    inserted: &str,
) -> crate::state::Passphrase {
    let start = char_byte(value, range.start);
    let end = char_byte(value, range.end);
    let mut next = crate::state::Passphrase::default();
    next.push_single_line(&value[..start]);
    next.push_single_line(inserted);
    next.push_single_line(&value[end..]);
    next
}

fn replace_secret_selection(
    value: &mut crate::state::Passphrase,
    edit: &mut SecretEditState,
    inserted: &str,
) {
    let range = edit.range();
    let cursor = range.start + inserted.chars().count();
    *value = replace_secret_range(value, range, inserted);
    edit.move_to(cursor, false);
}

fn delete_secret_range(
    value: &mut crate::state::Passphrase,
    edit: &mut SecretEditState,
    range: std::ops::Range<usize>,
) {
    let cursor = range.start;
    *value = replace_secret_range(value, range, "");
    edit.move_to(cursor, false);
}

fn move_secret_cursor(edit: &mut SecretEditState, to: usize, extend: bool) {
    edit.move_to(to, extend);
}

/// Paint a passphrase field without putting plaintext into egui's persisted text undoer.
/// Arrows stay inside it while Tab and Escape remain dialog keys.
pub fn secret(ui: &mut Ui, rect: Rect, s: Secret<'_>, p: &Palette) -> SecretOut {
    let sense = if s.frozen {
        Sense::hover()
    } else {
        Sense::click_and_drag()
    };
    let mut response = ui.interact(rect, s.id, sense);
    // `interact` does not focus custom fields automatically on click.
    if !s.frozen && response.clicked() {
        response.request_focus();
    }
    // Claim initial focus once; never steal it back after Tab.
    if s.autofocus && !s.frozen && ui.memory(|m| m.focused()).is_none() {
        response.request_focus();
    }
    if !s.frozen {
        ui.memory_mut(|m| {
            m.set_focus_lock_filter(
                response.id,
                egui::EventFilter {
                    tab: false,
                    horizontal_arrows: true,
                    vertical_arrows: true,
                    escape: false,
                },
            );
        });
    }

    let edit_id = s.id.with("secure_edit_state");
    let mut edit = ui
        .data_mut(|data| data.get_temp::<SecretEditState>(edit_id))
        .unwrap_or(SecretEditState {
            anchor: s.value.chars().count(),
            cursor: s.value.chars().count(),
            ..SecretEditState::default()
        });
    edit.clamp(s.value.chars().count());

    let border = match (s.refused, response.has_focus()) {
        (true, _) => p.tampered,
        (_, true) => p.focus,
        _ => p.hairline_strong,
    };
    w::surface(ui.painter(), rect, p.ground0, Some(border), radius::CONTROL);

    let eye = Rect::from_center_size(
        egui::pos2(rect.right() - 18.0, rect.center().y),
        Vec2::splat(24.0),
    );
    let text_room = (eye.left() - rect.left() - 16.0).max(0.0);
    let display_for = |value: &str| {
        if value.is_empty() {
            (s.placeholder.to_owned(), p.text4)
        } else if s.revealed {
            (value.to_owned(), p.text1)
        } else {
            (
                std::iter::repeat_n('.', value.chars().count()).collect(),
                p.text1,
            )
        }
    };
    let (shown_before, _) = display_for(s.value);
    let before_galley = ui
        .painter()
        .layout_no_wrap(shown_before, role::cell(), p.text1);
    let cursor_x = if s.value.is_empty() {
        0.0
    } else {
        before_galley
            .pos_from_cursor(egui::text::CCursor::new(edit.cursor))
            .min
            .x
    };
    let max_offset = (before_galley.size().x - text_room).max(0.0);
    let offset = if response.has_focus() {
        (cursor_x - text_room + 2.0).clamp(0.0, max_offset)
    } else {
        0.0
    };
    let text_origin_x = rect.left() + 10.0 - offset;
    let pointer_char = |position: egui::Pos2| {
        if s.value.is_empty() {
            0
        } else {
            before_galley
                .cursor_from_pos(egui::vec2(position.x - text_origin_x, 0.0))
                .index
                .0
                .min(s.value.chars().count())
        }
    };

    if !s.frozen {
        if response.clicked() {
            if let Some(position) = response.interact_pointer_pos() {
                let extend = ui.input(|input| input.modifiers.shift);
                edit.move_to(pointer_char(position), extend);
            }
        }
        if response.drag_started() {
            if let Some(position) = response.interact_pointer_pos() {
                edit.move_to(pointer_char(position), false);
                edit.dragging = true;
            }
        } else if response.dragged() && edit.dragging {
            if let Some(position) = response.interact_pointer_pos() {
                edit.cursor = pointer_char(position);
            }
        }
        if !ui.input(|input| input.pointer.primary_down()) {
            edit.dragging = false;
        }
    }

    let mut edited = None;
    if !s.frozen && response.has_focus() {
        let mut next = crate::state::Passphrase::from(s.value);
        let mut handled_keys = Vec::new();
        for event in ui.input(|input| input.events.clone()) {
            match event {
                egui::Event::Text(text) | egui::Event::Paste(text) if !text.is_empty() => {
                    replace_secret_selection(&mut next, &mut edit, &text);
                    edit.ime_active = false;
                }
                egui::Event::Cut if s.revealed && edit.range().start != edit.range().end => {
                    let range = edit.range();
                    let start = char_byte(&next, range.start);
                    let end = char_byte(&next, range.end);
                    ui.ctx().copy_text(next[start..end].to_owned());
                    delete_secret_range(&mut next, &mut edit, range);
                }
                egui::Event::Copy if s.revealed && edit.range().start != edit.range().end => {
                    let range = edit.range();
                    let start = char_byte(&next, range.start);
                    let end = char_byte(&next, range.end);
                    ui.ctx().copy_text(next[start..end].to_owned());
                }
                egui::Event::Key {
                    key,
                    pressed: true,
                    modifiers,
                    ..
                } => {
                    let len = next.chars().count();
                    let extend = modifiers.shift;
                    let mut handled = true;
                    match key {
                        egui::Key::A if modifiers.command => {
                            edit.anchor = 0;
                            edit.cursor = len;
                        }
                        egui::Key::ArrowLeft => {
                            let to = if !extend && edit.anchor != edit.cursor {
                                edit.range().start
                            } else if modifiers.command {
                                0
                            } else {
                                edit.cursor.saturating_sub(1)
                            };
                            move_secret_cursor(&mut edit, to, extend);
                        }
                        egui::Key::ArrowRight => {
                            let to = if !extend && edit.anchor != edit.cursor {
                                edit.range().end
                            } else if modifiers.command {
                                len
                            } else {
                                (edit.cursor + 1).min(len)
                            };
                            move_secret_cursor(&mut edit, to, extend);
                        }
                        egui::Key::ArrowUp | egui::Key::Home => {
                            move_secret_cursor(&mut edit, 0, extend);
                        }
                        egui::Key::ArrowDown | egui::Key::End => {
                            move_secret_cursor(&mut edit, len, extend);
                        }
                        egui::Key::Backspace => {
                            let range = edit.range();
                            if range.start != range.end {
                                delete_secret_range(&mut next, &mut edit, range);
                            } else if edit.cursor > 0 {
                                let cursor = edit.cursor;
                                delete_secret_range(&mut next, &mut edit, cursor - 1..cursor);
                            }
                        }
                        egui::Key::Delete => {
                            let range = edit.range();
                            if range.start != range.end {
                                delete_secret_range(&mut next, &mut edit, range);
                            } else if edit.cursor < len {
                                let cursor = edit.cursor;
                                delete_secret_range(&mut next, &mut edit, cursor..cursor + 1);
                            }
                        }
                        _ => handled = false,
                    }
                    if handled {
                        handled_keys.push((modifiers, key));
                    }
                }
                egui::Event::Ime(egui::ImeEvent::Preedit { text, .. }) => {
                    edit.ime_active = !text.is_empty();
                }
                egui::Event::Ime(egui::ImeEvent::Commit(text)) => {
                    if !text.is_empty() {
                        replace_secret_selection(&mut next, &mut edit, &text);
                    }
                    edit.ime_active = false;
                }
                egui::Event::Ime(egui::ImeEvent::DeleteSurrounding {
                    before_chars,
                    after_chars,
                }) => {
                    let len = next.chars().count();
                    let start = edit.cursor.saturating_sub(before_chars);
                    let end = edit.cursor.saturating_add(after_chars).min(len);
                    if start < end {
                        delete_secret_range(&mut next, &mut edit, start..end);
                    }
                }
                #[allow(deprecated)]
                egui::Event::Ime(egui::ImeEvent::Enabled | egui::ImeEvent::Disabled) => {}
                _ => {}
            }
        }
        ui.input_mut(|input| {
            for (modifiers, key) in handled_keys {
                input.consume_key(modifiers, key);
            }
        });
        if next.as_str() != s.value {
            edit.clamp(next.chars().count());
            edited = Some(next);
            response.mark_changed();
        }
    }

    let current = edited.as_ref().map_or(s.value, |value| value.as_str());
    edit.clamp(current.chars().count());
    let (shown, colour) = display_for(current);
    let galley = ui.painter().layout_no_wrap(shown, role::cell(), colour);
    let cursor_x = if current.is_empty() {
        0.0
    } else {
        galley
            .pos_from_cursor(egui::text::CCursor::new(edit.cursor))
            .min
            .x
    };
    let max_offset = (galley.size().x - text_room).max(0.0);
    let offset = if response.has_focus() {
        (cursor_x - text_room + 2.0).clamp(0.0, max_offset)
    } else {
        0.0
    };
    let origin = egui::pos2(
        rect.left() + 10.0 - offset,
        rect.center().y - galley.size().y * 0.5,
    );
    let clip = Rect::from_min_max(
        egui::pos2(rect.left() + 10.0, rect.top() + 2.0),
        egui::pos2(eye.left() - 6.0, rect.bottom() - 2.0),
    );
    let painter = ui.painter().with_clip_rect(clip);
    if response.has_focus() && !current.is_empty() {
        let range = edit.range();
        if range.start != range.end {
            let left = galley
                .pos_from_cursor(egui::text::CCursor::new(range.start))
                .min
                .x;
            let right = galley
                .pos_from_cursor(egui::text::CCursor::new(range.end))
                .min
                .x;
            painter.rect_filled(
                Rect::from_min_max(
                    egui::pos2(origin.x + left, clip.top() + 3.0),
                    egui::pos2(origin.x + right, clip.bottom() - 3.0),
                ),
                0.0,
                Palette::tint(p.accent, 70),
            );
        }
    }
    painter.galley(origin, galley.clone(), colour);
    if response.has_focus() && !s.frozen {
        let caret_x = origin.x
            + if current.is_empty() {
                0.0
            } else {
                galley
                    .pos_from_cursor(egui::text::CCursor::new(edit.cursor))
                    .min
                    .x
            };
        let caret = Rect::from_min_size(
            egui::pos2(caret_x, rect.center().y - 8.0),
            egui::vec2(if edit.ime_active { 2.0 } else { 1.0 }, 16.0),
        );
        painter.rect_filled(caret, 0.0, p.text1);
        let to_global = ui
            .ctx()
            .layer_transform_to_global(ui.layer_id())
            .unwrap_or_default();
        ui.output_mut(|output| {
            output.ime = Some(egui::output::IMEOutput {
                purpose: egui::IMEPurpose::Password,
                rect: to_global * rect,
                cursor_rect: to_global * caret,
                should_interrupt_composition: false,
            });
        });
    }
    ui.data_mut(|data| data.insert_temp(edit_id, edit));

    let toggle = ui.interact(eye, s.id.with("reveal"), Sense::click());
    let ink = if toggle.hovered() { p.text1 } else { p.text3 };
    eye_glyph(ui.painter(), eye.center(), ink, s.revealed);
    if toggle.has_focus() {
        focus_ring(ui, eye, p, radius::CONTROL);
    }
    let name = format!("{} {}", if s.revealed { "Hide" } else { "Reveal" }, s.label);
    toggle.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, &name));

    // Report masks only; revealing changes paint, not diagnostics or accessibility data.
    let previous_mask = std::iter::repeat_n('.', s.value.chars().count()).collect::<String>();
    let current_mask = std::iter::repeat_n('.', current.chars().count()).collect::<String>();
    response.widget_info(|| {
        let mut info =
            egui::WidgetInfo::text_edit(!s.frozen, &previous_mask, &current_mask, s.placeholder);
        info.label = Some(s.label.to_owned());
        info
    });
    response.ctx.accesskit_node_builder(response.id, |node| {
        node.set_role(egui::accesskit::Role::PasswordInput);
        node.set_label(s.label);
    });

    SecretOut {
        toggled: toggle.clicked(),
        edited,
        response,
    }
}

/// Reveal-state eye glyph.
fn eye_glyph(painter: &egui::Painter, c: egui::Pos2, ink: Color32, revealed: bool) {
    let stroke = Stroke::new(1.2, ink);
    painter.add(egui::Shape::line(
        vec![
            egui::pos2(c.x - 7.0, c.y),
            egui::pos2(c.x, c.y - 4.5),
            egui::pos2(c.x + 7.0, c.y),
            egui::pos2(c.x, c.y + 4.5),
            egui::pos2(c.x - 7.0, c.y),
        ],
        stroke,
    ));
    painter.circle_stroke(c, 1.8, stroke);
    if revealed {
        painter.line_segment(
            [
                egui::pos2(c.x - 7.0, c.y + 5.0),
                egui::pos2(c.x + 7.0, c.y - 5.0),
            ],
            stroke,
        );
    }
}

/// A segmented control; `salt` prevents ids colliding across instances.
pub fn segmented(
    ui: &mut Ui,
    salt: &str,
    origin: egui::Pos2,
    items: &[&str],
    active: usize,
    p: &Palette,
) -> Option<usize> {
    let widths = segmented_widths(ui.painter(), items, p);
    let total: f32 = widths.iter().sum();
    let track = Rect::from_min_size(
        egui::pos2(origin.x, origin.y - 12.0),
        Vec2::new(total + 4.0, 24.0),
    );
    ui.painter()
        .rect_filled(track, CornerRadius::same(radius::CONTROL), p.wash(15));

    let mut clicked = None;
    let mut x = origin.x + 2.0;
    for (i, (label, width)) in items.iter().zip(&widths).enumerate() {
        let r = Rect::from_min_size(egui::pos2(x, origin.y - 10.0), Vec2::new(*width, 20.0));
        let response = ui.interact(r, Id::new(("seg", salt, *label)), Sense::click());
        let on = i == active;
        if on {
            ui.painter()
                .rect_filled(r, CornerRadius::same(radius::PILL), p.wash(26));
        } else if response.hovered() {
            ui.painter()
                .rect_filled(r, CornerRadius::same(radius::PILL), p.wash(12));
        }
        ui.painter().text(
            r.center(),
            Align2::CENTER_CENTER,
            *label,
            role::meta(),
            if on || response.hovered() {
                p.text1
            } else {
                p.text3
            },
        );
        // Click-sensing controls need a visible focus indicator.
        if response.has_focus() {
            focus_ring(ui, r, p, radius::PILL);
        }
        let name = (*label).to_owned();
        response.widget_info(|| {
            egui::WidgetInfo::selected(egui::WidgetType::RadioButton, true, on, &name)
        });
        if response.clicked() {
            clicked = Some(i);
        }
        x += width;
    }
    clicked
}

/// Painted width of a segmented control, used by owners to avoid adjacent labels.
pub fn segmented_width(painter: &egui::Painter, items: &[&str], p: &Palette) -> f32 {
    segmented_widths(painter, items, p).iter().sum::<f32>() + 4.0
}

fn segmented_widths(painter: &egui::Painter, items: &[&str], p: &Palette) -> Vec<f32> {
    items
        .iter()
        .map(|label| {
            painter
                .layout_no_wrap((*label).to_owned(), role::meta(), p.text3)
                .size()
                .x
                + 20.0
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use egui_kittest::kittest::{NodeT, Queryable};
    use egui_kittest::Harness;

    use super::{button, replace_secret_range, secret, Btn, ButtonKind, Secret};
    use crate::state::Passphrase;
    use crate::theme::Palette;

    #[test]
    fn secret_has_a_stable_accessible_name_without_exposing_its_value() {
        const CANARY: &str = "the-passphrase-must-not-escape";

        struct SecretApp;

        impl eframe::App for SecretApp {
            fn ui(&mut self, ui: &mut egui::Ui, _: &mut eframe::Frame) {
                let rect = egui::Rect::from_min_size(
                    ui.min_rect().min + egui::vec2(8.0, 8.0),
                    egui::vec2(280.0, 32.0),
                );
                secret(
                    ui,
                    rect,
                    Secret {
                        id: egui::Id::new("tested_secret"),
                        label: "Vault passphrase",
                        value: CANARY,
                        placeholder: "passphrase",
                        revealed: true,
                        refused: false,
                        frozen: false,
                        autofocus: false,
                    },
                    &Palette::DARK,
                );
            }
        }

        let mut harness = Harness::builder()
            .with_size(egui::vec2(320.0, 80.0))
            .build_eframe(|cc| {
                cc.egui_ctx.set_fonts(crate::fonts::definitions());
                SecretApp
            });
        harness.run();

        let node = harness
            .query_by_label("Vault passphrase")
            .expect("the password input has no accessible name");
        let accesskit = node.accesskit_node();
        assert_eq!(accesskit.role(), egui::accesskit::Role::PasswordInput);
        assert_eq!(
            accesskit.value().as_deref(),
            Some("..............................")
        );
        assert!(
            !format!("{accesskit:?}").contains(CANARY),
            "the accessibility tree exposed the passphrase"
        );
    }

    #[test]
    fn secure_range_replacement_uses_character_indices() {
        let replaced = replace_secret_range("ab中de", 2..4, "終");
        assert_eq!(replaced.as_str(), "ab終e");
    }

    #[test]
    fn disabled_button_reason_is_available_without_a_mouse_hover() {
        struct DisabledButtonApp;

        impl eframe::App for DisabledButtonApp {
            fn ui(&mut self, ui: &mut egui::Ui, _: &mut eframe::Frame) {
                let rect = egui::Rect::from_min_size(ui.min_rect().min, egui::vec2(160.0, 32.0));
                button(
                    ui,
                    rect,
                    Btn::new("Create vault", ButtonKind::Primary)
                        .unavailable("Choose where to save the vault"),
                    &Palette::DARK,
                );
            }
        }

        let mut harness = Harness::builder().build_eframe(|cc| {
            cc.egui_ctx.set_fonts(crate::fonts::definitions());
            DisabledButtonApp
        });
        harness.run();

        let node = harness
            .query_by_label("Create vault")
            .expect("the disabled button has no accessible name");
        assert_eq!(
            node.accesskit_node().description().as_deref(),
            Some("Choose where to save the vault")
        );
    }

    #[test]
    fn secret_supports_middle_editing_selection_delete_paste_and_ime_without_an_undoer() {
        struct SecretApp {
            value: Passphrase,
        }

        impl eframe::App for SecretApp {
            fn ui(&mut self, ui: &mut egui::Ui, _: &mut eframe::Frame) {
                let rect = egui::Rect::from_min_size(
                    ui.min_rect().min + egui::vec2(8.0, 8.0),
                    egui::vec2(280.0, 32.0),
                );
                let out = secret(
                    ui,
                    rect,
                    Secret {
                        id: egui::Id::new("editing_secret"),
                        label: "Vault passphrase",
                        value: &self.value,
                        placeholder: "passphrase",
                        revealed: false,
                        refused: false,
                        frozen: false,
                        autofocus: true,
                    },
                    &Palette::DARK,
                );
                if let Some(value) = out.edited {
                    self.value = value;
                }
            }
        }

        let mut harness = Harness::builder()
            .with_size(egui::vec2(320.0, 80.0))
            .build_eframe(|cc| {
                cc.egui_ctx.set_fonts(crate::fonts::definitions());
                SecretApp {
                    value: "ab中de".into(),
                }
            });
        harness.run();
        assert_eq!(
            harness.ctx.memory(|memory| memory.focused()),
            Some(egui::Id::new("editing_secret")),
            "the secure editor did not receive its requested initial focus"
        );
        harness.key_press(egui::Key::Home);
        harness.key_press(egui::Key::ArrowRight);
        harness.key_press(egui::Key::ArrowRight);
        harness.event(egui::Event::Text("X".into()));
        harness.run();
        assert_eq!(harness.state().value.as_str(), "abX中de");

        harness.key_press(egui::Key::End);
        harness.key_press(egui::Key::Backspace);
        harness.key_press(egui::Key::Home);
        harness.key_press(egui::Key::Delete);
        harness.run();
        assert_eq!(harness.state().value.as_str(), "bX中d");

        harness.key_press(egui::Key::End);
        harness.key_press_modifiers(egui::Modifiers::SHIFT, egui::Key::ArrowLeft);
        harness.key_press_modifiers(egui::Modifiers::SHIFT, egui::Key::ArrowLeft);
        harness.event(egui::Event::Paste("終".into()));
        harness.run();
        assert_eq!(harness.state().value.as_str(), "bX終");

        harness.event(egui::Event::Ime(egui::ImeEvent::Commit("界".into())));
        harness.run();
        assert_eq!(harness.state().value.as_str(), "bX終界");
        harness.event(egui::Event::Ime(egui::ImeEvent::DeleteSurrounding {
            before_chars: 1,
            after_chars: 0,
        }));
        harness.run();
        assert_eq!(harness.state().value.as_str(), "bX終");

        assert!(
            egui::TextEdit::load_state(&harness.ctx, egui::Id::new("editing_secret")).is_none(),
            "the secure field stored plaintext in egui's ordinary String undoer"
        );
    }
}
