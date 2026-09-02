//! Application shell using one root viewport. eframe initializes AccessKit only there;
//! child viewports would be invisible to Windows UI Automation.

use crate::effects::{Effects, Picker};
use crate::engine::{Command, CreateSpec, Handle, Reply};
use crate::screens;
use crate::state::{self, Action, Busy, Doc, QueryDiscard, QueryRequest, Route, State, Target};
use crate::{theme, window};
use egui::{Key, Rect, Ui};

#[derive(Default)]
pub struct Studio {
    pub state: State,
    /// External effects are kept outside the serializable model.
    pub effects: Effects,
    /// Lazily started engine thread.
    engine: Option<Handle>,
    /// In-flight page request, used to suppress duplicate requests across frames.
    loading: Option<(String, Option<citadel_mem::AtomId>)>,
    inflight_query: Option<QueryRequest>,
    vector_requests: std::collections::VecDeque<(u64, String, String)>,
    close_after_discard: bool,
    installed: bool,
}

fn vault_replacement_discard(action: &Action) -> Option<QueryDiscard> {
    match action {
        Action::OpenDemoVault => Some(QueryDiscard::OpenDemoVault),
        Action::OpenRecent(path) => Some(QueryDiscard::OpenRecent(path.clone())),
        Action::ChooseVaultToOpen => Some(QueryDiscard::ChooseVaultToOpen),
        Action::BeginCreate => Some(QueryDiscard::BeginCreate),
        Action::BeginImport => Some(QueryDiscard::BeginImport),
        _ => None,
    }
}

impl Studio {
    pub fn with_picker(picker: Picker) -> Self {
        Self {
            effects: Effects::with_picker(picker),
            ..Self::default()
        }
    }

    pub fn wire(&mut self, cc: &eframe::CreationContext<'_>) {
        if let Some(storage) = cc.storage {
            let prefs: state::Prefs =
                eframe::get_value(storage, eframe::APP_KEY).unwrap_or_default();
            prefs.apply(&mut self.state);
        }
        // Report the adapter wgpu selected, including software fallbacks.
        let render = cc
            .wgpu_render_state
            .as_ref()
            .expect("Studio renders the vector canvas with wgpu and has no path without it");
        self.state.renderer = crate::gpu::status_items(&render.adapter.get_info());
        let vector_rendering = crate::cloud::install(render);
        self.state
            .renderer
            .push(vector_rendering.status().to_owned());
    }
}

impl eframe::App for Studio {
    /// Persist preferences only; session state and verification claims must be recomputed.
    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        eframe::set_value(storage, eframe::APP_KEY, &state::Prefs::of(&self.state));
    }

    fn clear_color(&self, _visuals: &egui::Visuals) -> [f32; 4] {
        let c = self.state.palette.ground1;
        [
            c.r() as f32 / 255.0,
            c.g() as f32 / 255.0,
            c.b() as f32 / 255.0,
            1.0,
        ]
    }

    fn ui(&mut self, ui: &mut Ui, _frame: &mut eframe::Frame) {
        let rect = ui.max_rect();
        // set_fonts binds at the next begin_frame, so the first frame installs and draws
        // nothing rather than naming families that do not exist yet.
        if !self.installed {
            theme::install(ui.ctx(), &self.state.palette);
            self.installed = true;
            ui.ctx().request_repaint();
            return;
        }
        let editor_revision = self.state.query_editor_revision;
        if ui.input(|input| input.viewport().close_requested()) && self.state.query_dirty {
            ui.ctx()
                .send_viewport_cmd(egui::ViewportCommand::CancelClose);
            if self.state.query_discard.is_none() {
                state::apply(
                    &mut self.state,
                    Action::PromptDiscardQuery(QueryDiscard::Exit),
                );
            }
        }
        self.state.ppp = ui.ctx().pixels_per_point();
        let mut out = Vec::new();

        let band = Rect::from_min_size(
            rect.left_top(),
            egui::Vec2::new(rect.width(), theme::metrics::BAND_H),
        );
        window::drag_band(ui, band);

        match self.state.route {
            Route::Home => screens::home(ui, rect, &self.state, &mut out),
            Route::Unlock => screens::unlock(ui, rect, &self.state, &mut out),
            Route::Create => screens::create(ui, rect, &self.state, &mut out),
            Route::Import => screens::import(ui, rect, &self.state, &mut out),
            Route::Workspace => screens::workspace(ui, rect, &self.state, &mut out),
        }

        if self.state.about_open {
            screens::about(ui, rect, &self.state, &mut out);
        }

        if !self.state.about_open {
            if self.state.palette_open {
                screens::palette(ui, rect, &self.state, &mut out);
                self.palette_keys(ui, &mut out);
            } else if self.state.query_discard.is_some() {
                self.query_discard_keys(ui, &mut out);
            } else if self.state.forget_prompt.is_none() {
                self.keyboard(ui, &mut out);
            }
        }

        // Window controls must be last in hit-test order.
        window::chrome(ui, rect, band, &self.state.palette);
        window::outline(ui, rect, self.state.palette.hairline);

        // Apply completed work before processing new frame actions.
        if let Some(done) = self.effects.poll() {
            state::apply(&mut self.state, done);
        }
        while let Some(reply) = self.engine.as_mut().and_then(Handle::poll) {
            let stopped = matches!(reply, Reply::Stopped(_));
            if stopped || matches!(reply, Reply::Atoms { .. }) {
                self.loading = None;
            }
            if let Some(action) = self.received(reply) {
                state::apply(&mut self.state, action);
            }
            if stopped {
                self.engine = None;
                break;
            }
        }
        self.fetch_pages();
        self.fetch_browse();
        self.fetch_vector();
        // Worker channels have no UI wake-up.
        if self.engine_busy() {
            ui.ctx().request_repaint();
        }

        let was_light = self.state.light;
        for action in out {
            self.perform(action);
        }
        if self.state.query_editor_revision != editor_revision {
            let old_id = State::query_editor_id_for(editor_revision);
            ui.ctx().memory_mut(|memory| memory.surrender_focus(old_id));
            ui.ctx()
                .data_mut(|data| data.remove::<egui::text_edit::TextEditState>(old_id));
        }
        if self.close_after_discard {
            self.close_after_discard = false;
            ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
        }
        if self.state.light != was_light {
            theme::install(ui.ctx(), &self.state.palette);
            ui.ctx().request_repaint();
        }
    }
}

/// Maximum rows materialized by the table browser.
const BROWSE_LIMIT: usize = 2_000;

impl Studio {
    fn received(&mut self, reply: Reply) -> Option<Action> {
        Some(match reply {
            Reply::Stopped(error) => {
                self.inflight_query = None;
                self.vector_requests.clear();
                Action::EngineStopped(error)
            }
            Reply::Opened(opened) => Action::VaultOpened(opened),
            Reply::Closed => Action::VaultClosed,
            Reply::Checked(facts) => Action::IntegrityChecked(facts),
            Reply::PassphraseChanged(result) => Action::PassphraseChanged(result),
            Reply::Ran { run, refreshed } => Action::QueryFinished {
                request: self.inflight_query.take()?,
                run,
                refreshed,
            },
            Reply::Imported(opened) => Action::ImportFinished(opened),
            Reply::Explained { plan, refreshed } => Action::ExplainFinished {
                request: self.inflight_query.take()?,
                plan,
                refreshed,
            },
            Reply::Browsed { table, rows } => {
                Action::TableBrowsed(Box::new(state::Browsed { table, rows: *rows }))
            }
            Reply::VectorSampled {
                table,
                column,
                sample,
            } => {
                let position =
                    self.vector_requests
                        .iter()
                        .position(|(_, asked_table, asked_column)| {
                            asked_table == &table && asked_column == &column
                        })?;
                let (vault_revision, _, _) = self.vector_requests.remove(position)?;
                Action::VectorSampled {
                    vault_revision,
                    table,
                    column,
                    sample,
                }
            }
            Reply::Atoms { region, page } => Action::AtomsLoaded { region, page },
            Reply::Verified {
                region,
                asked,
                verdicts,
            } => Action::PageVerified {
                region,
                asked,
                verdicts,
            },
            Reply::Forgotten {
                region,
                asked,
                mutation,
            } => Action::Forgotten {
                region,
                asked,
                mutation,
            },
        })
    }

    /// Dispatch engine intents, lazily starting the engine, and pass other actions through.
    fn dispatch(&mut self, action: Action) -> Option<Action> {
        let query_intent = matches!(
            action,
            Action::RunQuery | Action::ExplainQuery | Action::AnalyzeQuery
        );
        if query_intent {
            if let Some(reason) = self.state.query_blocker() {
                return Some(Action::QueryNotice(format!(
                    "SQL command not started: {reason}"
                )));
            }
        }
        let engine_intent = matches!(
            action,
            Action::OpenDemoVault
                | Action::SubmitPassphrase
                | Action::SubmitCreate
                | Action::CheckIntegrity
                | Action::RunImport
                | Action::SubmitRotate
                | Action::RunQuery
                | Action::ExplainQuery
                | Action::AnalyzeQuery
                | Action::VerifyPage
                | Action::ConfirmForget
                | Action::CloseVault
        );
        if engine_intent && self.engine.as_ref().is_some_and(Handle::foreground_busy) {
            return query_intent.then(|| {
                Action::QueryNotice(
                    "SQL command not started: another vault operation is still running".to_owned(),
                )
            });
        }
        let mut query_request = None;
        let command = match action {
            Action::OpenDemoVault => Command::OpenDemo,
            Action::SubmitPassphrase => {
                if self.state.passphrase.is_empty() {
                    return None;
                }
                let target = self.state.target.clone();
                let passphrase = std::mem::take(&mut self.state.passphrase);
                match target {
                    Target::None => return None,
                    Target::Picked(path) => Command::Open { path, passphrase },
                }
            }
            Action::SubmitCreate => {
                let form = &self.state.new_vault;
                if form.blocker().is_some() {
                    return None;
                }
                let form = std::mem::take(&mut self.state.new_vault);
                Command::Create(Box::new(CreateSpec {
                    path: form.path?,
                    passphrase: form.passphrase,
                    kdf: form.kdf,
                    profile: form.profile,
                }))
            }
            // The actor cannot read a command while executing the work being cancelled.
            Action::CancelQuery => {
                if let Some(engine) = self.engine.as_ref() {
                    engine.cancel();
                }
                return None;
            }
            Action::CancelImportRun => {
                if self.state.import_progress == state::ImportProgress::Running {
                    if let Some(engine) = self.engine.as_ref() {
                        engine.cancel();
                    }
                    return Some(Action::ImportCancelRequested);
                }
                return None;
            }
            Action::CheckIntegrity => Command::CheckIntegrity,
            Action::SubmitRotate => {
                let form = self.state.rotate.form()?;
                if form.blocker().is_some() {
                    return None;
                }
                let form = match std::mem::take(&mut self.state.rotate) {
                    state::RotateState::Open(form) => *form,
                    other => {
                        self.state.rotate = other;
                        return None;
                    }
                };
                Command::ChangePassphrase {
                    current: form.current,
                    next: form.next,
                }
            }
            Action::RunQuery => {
                let request = self.state.next_query_request();
                let command = Command::Run(request.sql.clone());
                query_request = Some(request);
                command
            }
            Action::RunImport => {
                let tables = self.state.source.tables();
                if tables.is_empty() {
                    return None;
                }
                Command::Import(crate::plan::statements(tables))
            }
            Action::ExplainQuery => {
                let request = self.state.next_query_request();
                let command = Command::Explain {
                    sql: request.sql.clone(),
                    analyze: false,
                };
                query_request = Some(request);
                command
            }
            Action::AnalyzeQuery => {
                let request = self.state.next_query_request();
                let command = Command::Explain {
                    sql: request.sql.clone(),
                    analyze: true,
                };
                query_request = Some(request);
                command
            }
            Action::CloseVault => Command::Close,
            Action::VerifyPage => {
                if self.state.verify_rows_blocker().is_some() {
                    return None;
                }
                let (region, ids) = self.state.visible_atom_ids()?;
                Command::Verify { region, ids }
            }
            Action::ConfirmForget => {
                let prompt = self.state.forget_prompt.as_ref()?;
                Command::Forget {
                    region: prompt.region.clone(),
                    ids: vec![prompt.atom_id],
                }
            }
            other => return Some(other),
        };
        let busy = match &command {
            Command::Run(_) | Command::Explain { .. } => {
                query_request.clone().map(Action::QueryStarted)
            }
            Command::Import(_) => Some(Action::EngineBusy(Busy::Importing)),
            Command::CheckIntegrity => Some(Action::EngineBusy(Busy::Checking)),
            Command::ChangePassphrase { .. } => Some(Action::EngineBusy(Busy::Rotating)),
            Command::Verify { region, .. } => {
                Some(Action::EngineBusy(Busy::Verifying(region.clone())))
            }
            Command::Forget { region, .. } => {
                Some(Action::EngineBusy(Busy::Forgetting(region.clone())))
            }
            Command::Close => None,
            _ => Some(Action::EngineBusy(Busy::Opening)),
        };
        let sent = self.engine.get_or_insert_with(Handle::spawn).send(command);
        match sent {
            Ok(true) => {
                self.inflight_query = query_request;
                busy
            }
            Ok(false) => None,
            Err(error) => {
                self.engine = None;
                Some(Action::EngineStopped(error))
            }
        }
    }

    pub fn engine_busy(&self) -> bool {
        self.effects.busy() || self.engine.as_ref().is_some_and(Handle::busy)
    }

    /// Process an action through effects, engine dispatch, and the state reducer.
    pub fn perform(&mut self, action: Action) {
        if matches!(action, Action::RequestExit) {
            if self.state.query_dirty {
                state::apply(
                    &mut self.state,
                    Action::PromptDiscardQuery(QueryDiscard::Exit),
                );
            } else {
                self.close_after_discard = true;
            }
            return;
        }
        let mut action = if matches!(action, Action::PaletteRun) {
            let chosen = state::commands(&self.state)
                .get(self.state.palette_index)
                .map(|command| command.action.clone());
            self.state.palette_open = false;
            self.state.palette_query.clear();
            self.state.palette_index = 0;
            let Some(chosen) = chosen else {
                return;
            };
            chosen
        } else {
            action
        };
        if matches!(action, Action::ConfirmDiscardQuery) {
            let destination = self.state.query_discard.take();
            self.state.discard_query_edits();
            match destination {
                Some(QueryDiscard::CloseVault) => action = Action::CloseVault,
                Some(QueryDiscard::Exit) => {
                    self.close_after_discard = true;
                    return;
                }
                Some(QueryDiscard::OpenDemoVault) => action = Action::OpenDemoVault,
                Some(QueryDiscard::OpenRecent(path)) => action = Action::OpenRecent(path),
                Some(QueryDiscard::ChooseVaultToOpen) => action = Action::ChooseVaultToOpen,
                Some(QueryDiscard::BeginCreate) => action = Action::BeginCreate,
                Some(QueryDiscard::BeginImport) => action = Action::BeginImport,
                None => return,
            }
        }
        if matches!(action, Action::CloseVault) && self.state.close_vault_blocker().is_some() {
            return;
        }
        if matches!(action, Action::CloseVault) && self.state.query_dirty {
            if self.engine.as_ref().is_some_and(Handle::foreground_busy) {
                return;
            }
            state::apply(
                &mut self.state,
                Action::PromptDiscardQuery(QueryDiscard::CloseVault),
            );
            return;
        }
        if self.state.query_dirty {
            if let Some(destination) = vault_replacement_discard(&action) {
                state::apply(&mut self.state, Action::PromptDiscardQuery(destination));
                return;
            }
        }
        let Some(action) = self.effects.run(action) else {
            return;
        };
        if let Some(action) = self.dispatch(action) {
            state::apply(&mut self.state, action);
        }
    }

    /// Read the focused table with Studio's bounded query.
    fn fetch_browse(&mut self) {
        let Some(Doc::Table(name)) = self.state.active_doc() else {
            return;
        };
        if self.state.browse.as_ref().is_some_and(|b| &b.table == name) {
            return;
        }
        let table = name.clone();
        self.state.browse = Some(state::Browsed {
            table: table.clone(),
            rows: Ok(citadel_sql::QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
            }),
        });
        let sent = self
            .engine
            .get_or_insert_with(Handle::spawn)
            .send_background(Command::Browse {
                table,
                limit: BROWSE_LIMIT,
            });
        if let Err(error) = sent {
            self.engine = None;
            state::apply(&mut self.state, Action::EngineStopped(error));
        }
    }

    /// Start one bounded vector sample only after its document is visible.
    fn fetch_vector(&mut self) {
        let Some(Doc::Vector(qualified)) = self.state.active_doc().cloned() else {
            return;
        };
        let Some((table, column)) = self.state.vault.as_ref().and_then(|vault| {
            vault
                .vectors
                .iter()
                .find(|vector| {
                    vector.qualified() == qualified
                        && vector.sample == crate::model::VectorSample::Unloaded
                })
                .map(|vector| (vector.table.clone(), vector.column.clone()))
        }) else {
            return;
        };
        let vault_revision = self.state.vault_revision;
        state::apply(
            &mut self.state,
            Action::BeginVectorSample {
                vault_revision,
                table: table.clone(),
                column: column.clone(),
            },
        );
        let sent = self
            .engine
            .get_or_insert_with(Handle::spawn)
            .send_background(Command::SampleVector {
                table: table.clone(),
                column: column.clone(),
            });
        match sent {
            Ok(()) => self
                .vector_requests
                .push_back((vault_revision, table, column)),
            Err(error) => {
                state::apply(
                    &mut self.state,
                    Action::VectorSampled {
                        vault_revision,
                        table,
                        column,
                        sample: Box::new(Err(error)),
                    },
                );
                self.engine = None;
            }
        }
    }

    /// Fetch the focused region incrementally as the selection approaches the held page.
    fn fetch_pages(&mut self) {
        if self.loading.is_some() {
            return;
        }
        let Some(region) = self.state.visible_region() else {
            return;
        };
        // Avoid retrying an unreadable region every frame.
        if region.facts.unreadable.is_some() {
            return;
        }
        if region.exhausted {
            return;
        }
        let held = region.atoms.len() as u64;
        const LOOKAHEAD: usize = 100;
        if held > 0 && self.state.selected_row + LOOKAHEAD < region.atoms.len() {
            return;
        }
        let after = region.next_after_id;
        let name = region.name().to_owned();
        self.loading = Some((name.clone(), after));
        let sent = self
            .engine
            .get_or_insert_with(Handle::spawn)
            .send_background(Command::Atoms {
                region: name,
                after,
            });
        if let Err(error) = sent {
            self.loading = None;
            self.engine = None;
            state::apply(&mut self.state, Action::EngineStopped(error));
        }
    }

    /// Route keyboard input exclusively to the open palette.
    fn palette_keys(&self, ui: &Ui, out: &mut Vec<Action>) {
        ui.input(|i| {
            for event in &i.events {
                if let egui::Event::Key {
                    key,
                    pressed: true,
                    modifiers,
                    ..
                } = event
                {
                    match key {
                        Key::ArrowDown => out.push(Action::PaletteMove(1)),
                        Key::ArrowUp => out.push(Action::PaletteMove(-1)),
                        Key::Enter => out.push(Action::PaletteRun),
                        Key::Escape => out.push(Action::TogglePalette),
                        // Modifiers arrive as their own event in egui 0.36, so read the
                        // state carried on this key press rather than end-of-frame state.
                        Key::K if modifiers.command => out.push(Action::TogglePalette),
                        _ => {}
                    }
                }
            }
        });
    }

    fn query_discard_keys(&self, ui: &Ui, out: &mut Vec<Action>) {
        if ui.input(|input| input.key_pressed(Key::Escape)) {
            out.push(Action::CancelDiscardQuery);
            return;
        }
        let tab = ui.input(|input| {
            input.events.iter().find_map(|event| match event {
                egui::Event::Key {
                    key: Key::Tab,
                    pressed: true,
                    modifiers,
                    ..
                } => Some(*modifiers),
                _ => None,
            })
        });
        if let Some(modifiers) = tab {
            let keep = egui::Id::new(("btn", "Keep editing"));
            let discard = egui::Id::new(("btn", "Discard"));
            let focused = ui.memory(|memory| memory.focused());
            let next = if modifiers.shift {
                if focused == Some(keep) {
                    discard
                } else {
                    keep
                }
            } else if focused == Some(discard) {
                keep
            } else {
                discard
            };
            ui.memory_mut(|memory| memory.request_focus(next));
            ui.input_mut(|input| {
                input.consume_key(modifiers, Key::Tab);
            });
        }
    }

    fn keyboard(&self, ui: &Ui, out: &mut Vec<Action>) {
        let editor_id = self.state.query_editor_id();
        let editor_owns_navigation = ui.memory(|memory| memory.has_focus(editor_id));
        let ime_event_this_frame = ui.input(|input| {
            input
                .events
                .iter()
                .any(|event| matches!(event, egui::Event::Ime(_)))
        });
        ui.input(|i| {
            // Read the modifier state carried ON the key event rather than the
            // end-of-frame state. egui 0.36 delivers modifiers as their own event, so a
            // press and release inside one frame leaves `i.modifiers` already cleared.
            let chord = |want: Key| {
                i.events.iter().any(|e| {
                    matches!(
                        e,
                        egui::Event::Key { key, pressed: true, modifiers, .. }
                            if *key == want && modifiers.command
                    )
                })
            };

            if chord(Key::K) {
                out.push(Action::TogglePalette);
                return;
            }
            if self.state.close_vault_blocker().is_none()
                && i.events.iter().any(|e| {
                    matches!(e, egui::Event::Key { key: Key::W, pressed: true, modifiers, .. }
                        if modifiers.command && modifiers.shift)
                })
            {
                out.push(Action::CloseVault);
                return;
            }
            if self.state.route == Route::Unlock {
                if self.state.unlock == state::UnlockStage::Deriving {
                    return;
                }
                if i.key_pressed(Key::Enter) {
                    out.push(Action::SubmitPassphrase);
                }
                if i.key_pressed(Key::Escape) {
                    out.push(Action::CancelUnlock);
                }
                return;
            }

            // Create has multiple required fields, so Enter does not submit it globally.
            if self.state.route == Route::Create {
                if self.state.unlock == state::UnlockStage::Deriving {
                    return;
                }
                if i.key_pressed(Key::Escape) {
                    out.push(Action::CancelCreate);
                }
                return;
            }

            if self.state.route == Route::Import {
                if self.state.import_progress == state::ImportProgress::Running {
                    if i.key_pressed(Key::Escape) {
                        out.push(Action::CancelImportRun);
                    }
                    return;
                }
                if self.state.import_progress == state::ImportProgress::Cancelling {
                    return;
                }
                if matches!(&self.state.source, state::Source::Reading { .. }) {
                    if i.key_pressed(Key::Escape) {
                        out.push(Action::CancelImport);
                    }
                    return;
                }
                if i.key_pressed(Key::Escape) {
                    out.push(Action::CancelImport);
                }
                if i.key_pressed(Key::ArrowDown) {
                    out.push(Action::MoveSelection(1));
                }
                if i.key_pressed(Key::ArrowUp) {
                    out.push(Action::MoveSelection(-1));
                }
                return;
            }

            if self.state.route == Route::Home {
                if chord(Key::D) {
                    out.push(Action::OpenDemoVault);
                }
                if chord(Key::I) {
                    out.push(Action::BeginImport);
                }
                return;
            }

            if !editor_owns_navigation {
                if i.key_pressed(Key::ArrowDown) {
                    out.push(Action::MoveSelection(1));
                }
                if i.key_pressed(Key::ArrowUp) {
                    out.push(Action::MoveSelection(-1));
                }
                if i.key_pressed(Key::PageDown) {
                    out.push(Action::MoveSelection(10));
                }
                if i.key_pressed(Key::PageUp) {
                    out.push(Action::MoveSelection(-10));
                }
                if i.key_pressed(Key::Home) {
                    out.push(Action::SelectFirst);
                }
                if i.key_pressed(Key::End) {
                    out.push(Action::SelectLast);
                }
            }

            if matches!(self.state.active_doc(), Some(Doc::Query))
                && self.state.query_blocker().is_none()
                && !ime_event_this_frame
                && i.key_pressed(Key::F5)
            {
                out.push(Action::RunQuery);
            }

            if chord(Key::I) {
                out.push(Action::OpenDoc(Doc::Security));
            }
            if chord(Key::N) {
                out.push(Action::OpenDoc(Doc::Query));
            }
            if chord(Key::W) {
                out.push(Action::CloseDoc(self.state.active));
            }
            if chord(Key::L) {
                out.push(Action::ToggleTheme);
            }
            if chord(Key::D) {
                out.push(Action::CycleDensity);
            }
        });
    }
}

/// Shared Wayland, desktop-entry, AppStream, and macOS bundle identity.
pub const APP_ID: &str = "dev.citadeldb.studio";

pub fn run() -> eframe::Result {
    let result = start();
    // Windows release builds have no console, so startup failures need a dialog.
    if let Err(error) = &result {
        rfd::MessageDialog::new()
            .set_level(rfd::MessageLevel::Error)
            .set_title("Citadel Studio cannot start")
            .set_description(error.to_string())
            .show();
    }
    result
}

fn start() -> eframe::Result {
    let mut wgpu_options = eframe::egui_wgpu::WgpuConfiguration::default();
    if let eframe::egui_wgpu::WgpuSetup::CreateNew(setup) = &mut wgpu_options.wgpu_setup {
        setup.instance_descriptor.backends = crate::gpu::backends();
        setup.native_adapter_selector = Some(std::sync::Arc::new(crate::gpu::select));
    }
    let options = eframe::NativeOptions {
        wgpu_options,
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1600.0, 1000.0])
            .with_min_inner_size([900.0, 540.0])
            // `window.rs` supplies title-bar move and resize for this undecorated window.
            .with_decorations(false)
            .with_resizable(true)
            .with_title("Citadel Studio")
            .with_app_id(APP_ID)
            .with_icon(crate::icon::icon_data()),
        ..Default::default()
    };
    eframe::run_native(
        "Citadel Studio",
        options,
        Box::new(|cc| {
            let mut studio = Studio::default();
            studio.wire(cc);
            Ok(Box::new(studio))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait_for_engine(engine: &mut Handle) -> Reply {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "the engine did not answer"
            );
            if let Some(reply) = engine.poll() {
                return reply;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }

    #[test]
    fn palette_engine_commands_cross_the_dispatch_boundary() {
        let mut studio = Studio::default();
        studio.state.palette_open = true;
        studio.state.palette_query = "open disposable demo".to_owned();

        studio.perform(Action::PaletteRun);

        assert!(!studio.state.palette_open);
        assert!(matches!(studio.state.session, state::SessionState::Opening));
        assert!(studio.engine.as_ref().is_some_and(Handle::foreground_busy));
    }

    #[test]
    fn stopping_an_import_cancels_the_foreground_token_without_leaving_the_workflow() {
        let mut studio = Studio::default();
        let engine = studio.engine.get_or_insert_with(Handle::spawn);
        engine
            .send(Command::OpenDemo)
            .expect("the engine is running");
        let Reply::Opened(opened) = wait_for_engine(engine) else {
            panic!("the demo did not answer with Opened");
        };
        opened.expect("the demo opens");

        engine
            .send(Command::Import(
                "CREATE TABLE app_cancel_prefix (id INTEGER PRIMARY KEY); \
                 INSERT INTO app_cancel_prefix \
                 SELECT a.id * 10000 + b.id FROM documents a CROSS JOIN documents b;"
                    .to_owned(),
            ))
            .expect("the engine is running");
        studio.state.route = Route::Import;
        studio.state.import_progress = state::ImportProgress::Running;

        studio.perform(Action::CancelImportRun);

        assert_eq!(studio.state.route, Route::Import);
        assert_eq!(
            studio.state.import_progress,
            state::ImportProgress::Cancelling
        );
        let Reply::Imported(imported) = wait_for_engine(studio.engine.as_mut().unwrap()) else {
            panic!("the import did not answer with Imported");
        };
        let error = imported
            .outcome
            .expect_err("the dispatcher did not trip the import token");
        assert_eq!(error.kind, crate::engine::Kind::Cancelled);
    }

    #[test]
    fn menu_exit_closes_immediately_when_the_query_is_clean() {
        let mut studio = Studio::default();

        studio.perform(Action::RequestExit);

        assert!(studio.close_after_discard);
        assert!(studio.state.query_discard.is_none());
    }

    #[test]
    fn menu_exit_uses_the_existing_dirty_query_confirmation() {
        let mut studio = Studio::default();
        studio.state.query_dirty = true;
        studio.state.about_open = true;

        studio.perform(Action::RequestExit);

        assert!(!studio.close_after_discard);
        assert_eq!(studio.state.query_discard, Some(QueryDiscard::Exit));
        assert!(!studio.state.about_open);
    }

    #[test]
    fn every_vault_replacement_has_a_dirty_query_destination() {
        let recent = std::path::PathBuf::from("recent.cdl");
        for (action, expected) in [
            (Action::OpenDemoVault, QueryDiscard::OpenDemoVault),
            (
                Action::OpenRecent(recent.clone()),
                QueryDiscard::OpenRecent(recent),
            ),
            (Action::ChooseVaultToOpen, QueryDiscard::ChooseVaultToOpen),
            (Action::BeginCreate, QueryDiscard::BeginCreate),
            (Action::BeginImport, QueryDiscard::BeginImport),
        ] {
            assert_eq!(vault_replacement_discard(&action), Some(expected));
        }
        assert_eq!(vault_replacement_discard(&Action::ToggleTheme), None);
    }
}
