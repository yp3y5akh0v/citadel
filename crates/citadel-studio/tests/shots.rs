//! Headless application tests and review renders in `target/studio-shots/`. The harness
//! uses `build_eframe` to match the real window geometry without `build_ui`'s margin.

use citadel_studio::app::Studio;
use citadel_studio::effects::Ask;
use citadel_studio::state::{
    Action, Busy, Doc, GridTab, Route, Source, State, Target, UnlockStage,
};
use citadel_studio::theme::{Evidence, Palette};
use egui_kittest::Harness;

const W: f32 = 1600.0;
const H: f32 = 1000.0;

fn shot_path(name: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/studio-shots");
    std::fs::create_dir_all(&dir).expect("create the shots directory");
    dir.join(format!("{name}.png"))
}

/// One live application driven across frames.
struct Driver {
    harness: Harness<'static, Studio>,
    size: egui::Vec2,
    ppp: f32,
}

impl Driver {
    fn new(setup: impl FnOnce(&mut State)) -> Self {
        Self::sized(W, H, setup)
    }

    fn sized(w: f32, h: f32, setup: impl FnOnce(&mut State)) -> Self {
        Self::built(w, h, 1.0, Studio::default, setup)
    }

    /// Demo rendering at twice the pixel density without changing logical geometry.
    fn hidpi(setup: impl FnOnce(&mut State)) -> Self {
        Self::built(W, H, DEMO_PPP, Studio::default, setup)
    }

    /// Application with an injected native-dialog answer; downstream work remains real.
    fn picking(answer: impl Fn(Ask) -> Option<std::path::PathBuf> + Send + 'static) -> Self {
        Self::built(W, H, 1.0, || Studio::with_picker(Box::new(answer)), |_| {})
    }

    fn built(
        w: f32,
        h: f32,
        ppp: f32,
        build: impl FnOnce() -> Studio,
        setup: impl FnOnce(&mut State),
    ) -> Self {
        let size = egui::vec2(w, h);
        let harness = Harness::builder()
            .with_size(size)
            .with_pixels_per_point(ppp)
            .with_theme(egui::Theme::Dark)
            .wgpu()
            .build_eframe(|cc| {
                let mut studio = build();
                // Match the window's device setup.
                studio.wire(cc);
                setup(&mut studio.state);
                studio
            });

        let mut driver = Self { harness, size, ppp };
        // First frame installs fonts, second paints.
        driver.harness.run();
        driver.harness.run();
        driver
    }

    /// Runs frames until asynchronous work satisfies `done` or times out.
    fn until(&mut self, what: &str, done: impl Fn(&State) -> bool) -> &mut Self {
        for _ in 0..600 {
            if done(self.state()) {
                return self;
            }
            self.harness.step();
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        panic!(
            "gave up waiting for {what}; source is {:?}",
            self.state().source
        );
    }

    /// Applies an action through `Studio::perform`, preserving the engine/effect dispatch
    /// boundary when the control itself is not under test.
    fn apply(&mut self, action: Action) -> &mut Self {
        self.harness.state_mut().perform(action);
        self.settle()
    }

    /// Advances until the engine is idle across two frames.
    fn settle(&mut self) -> &mut Self {
        // Events, effects, and segmented controls can require successive frames.
        for _ in 0..4 {
            self.harness.step();
        }
        // Parallel encrypted-database tests make a deadline more stable than a frame count.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while std::time::Instant::now() < deadline {
            if !self.harness.state().engine_busy() {
                // Drawing an idle frame can start another page request.
                self.harness.step();
                if !self.harness.state().engine_busy() {
                    return self;
                }
            }
            self.harness.step();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("the engine never went idle");
    }

    /// Opens the real demo vault and waits for its first atom page.
    fn open_demo(&mut self) -> &mut Self {
        self.apply(Action::OpenDemoVault);
        self.until("the demo vault to open", |s| s.vault.is_some());
        // Wait for the separately requested first page.
        self.until("its first page of atoms", |s| {
            s.visible_region()
                .is_none_or(|r| r.facts.total == Some(0) || !r.atoms.is_empty())
        });
        self.settle()
    }

    fn with_state(&mut self, edit: impl FnOnce(&mut State)) -> &mut Self {
        edit(&mut self.harness.state_mut().state);
        self.settle()
    }

    /// Queues a press and release without choosing the frame-advance strategy.
    fn press_at(&mut self, pos: egui::Pos2) {
        let events = &mut self.harness.input_mut().events;
        events.push(egui::Event::PointerMoved(pos));
        events.push(egui::Event::PointerButton {
            pos,
            button: egui::PointerButton::Primary,
            pressed: true,
            modifiers: egui::Modifiers::NONE,
        });
        events.push(egui::Event::PointerButton {
            pos,
            button: egui::PointerButton::Primary,
            pressed: false,
            modifiers: egui::Modifiers::NONE,
        });
    }

    fn click(&mut self, x: f32, y: f32) -> &mut Self {
        self.press_at(egui::pos2(x, y));
        // A click can open a vault or a region, so it can leave engine work in flight.
        self.settle()
    }

    /// Clicks a widget that starts continuous repaints without demanding immediate idle.
    fn click_id_working(&mut self, id: egui::Id) -> &mut Self {
        let pos = self.rect_of(id).center();
        self.press_at(pos);
        for _ in 0..4 {
            self.harness.step();
        }
        self
    }

    fn rect_of(&self, id: egui::Id) -> egui::Rect {
        self.harness
            .ctx
            .read_response(id)
            .unwrap_or_else(|| panic!("no widget registered for {id:?}"))
            .rect
    }

    /// Clicks the widget's real laid-out rect by id.
    fn click_id(&mut self, id: egui::Id) -> &mut Self {
        let c = self.rect_of(id).center();
        self.click(c.x, c.y)
    }

    /// Captures copied text on the frame that emits it; `FullOutput` retains only one frame.
    fn click_for_copy(&mut self, id: egui::Id) -> String {
        let pos = self.rect_of(id).center();
        self.press_at(pos);
        for _ in 0..4 {
            self.harness.step();
            let copied = self
                .harness
                .output()
                .platform_output
                .commands
                .iter()
                .find_map(|c| match c {
                    egui::OutputCommand::CopyText(text) => Some(text.clone()),
                    _ => None,
                });
            if let Some(text) = copied {
                return text;
            }
        }
        String::new()
    }

    fn key(&mut self, key: egui::Key) -> &mut Self {
        let events = &mut self.harness.input_mut().events;
        events.push(egui::Event::Key {
            key,
            physical_key: None,
            pressed: true,
            repeat: false,
            modifiers: egui::Modifiers::NONE,
        });
        self.settle()
    }

    fn key_mod(&mut self, key: egui::Key, modifiers: egui::Modifiers) -> &mut Self {
        let events = &mut self.harness.input_mut().events;
        // egui 0.36 removed Modifiers from RawInput; the held state now arrives as its
        // own event, so a key event's modifiers field alone leaves `input.modifiers` clear.
        events.push(egui::Event::ModifiersChanged(modifiers));
        events.push(egui::Event::Key {
            key,
            physical_key: None,
            pressed: true,
            repeat: false,
            modifiers,
        });
        events.push(egui::Event::ModifiersChanged(egui::Modifiers::NONE));
        self.settle()
    }

    /// Turns the wheel, with negative values scrolling down per egui convention.
    fn scroll(&mut self, by: f32) -> &mut Self {
        self.scroll_at((self.size * 0.5).to_pos2(), by)
    }

    fn scroll_at(&mut self, over: egui::Pos2, by: f32) -> &mut Self {
        self.harness
            .input_mut()
            .events
            .push(egui::Event::PointerMoved(over));
        self.harness.step();
        self.harness
            .input_mut()
            .events
            .push(egui::Event::MouseWheel {
                unit: egui::MouseWheelUnit::Point,
                delta: egui::vec2(0.0, by),
                phase: egui::TouchPhase::Move,
                modifiers: egui::Modifiers::NONE,
            });
        self.harness.step();
        self.harness.step();
        // Park the pointer off the content so the next `run` is not fighting a tooltip.
        self.harness
            .input_mut()
            .events
            .push(egui::Event::PointerGone);
        self.harness.step();
        self
    }

    /// Queues a stepped pointer drag.
    fn drag(&mut self, from: egui::Pos2, by: egui::Vec2) -> &mut Self {
        let events = &mut self.harness.input_mut().events;
        events.push(egui::Event::PointerMoved(from));
        events.push(egui::Event::PointerButton {
            pos: from,
            button: egui::PointerButton::Primary,
            pressed: true,
            modifiers: egui::Modifiers::NONE,
        });
        self.harness.step();
        self.harness
            .input_mut()
            .events
            .push(egui::Event::PointerMoved(from + by));
        self.harness.step();
        self.harness
            .input_mut()
            .events
            .push(egui::Event::PointerButton {
                pos: from + by,
                button: egui::PointerButton::Primary,
                pressed: false,
                modifiers: egui::Modifiers::NONE,
            });
        self.harness.step();
        self.harness
            .input_mut()
            .events
            .push(egui::Event::PointerGone);
        self.harness.step();
        self
    }

    fn type_text(&mut self, text: &str) -> &mut Self {
        self.harness
            .input_mut()
            .events
            .push(egui::Event::Text(text.to_owned()));
        self.settle()
    }

    fn paste_text(&mut self, text: &str) -> &mut Self {
        self.harness
            .input_mut()
            .events
            .push(egui::Event::Paste(text.to_owned()));
        self.harness.step();
        self
    }

    /// Renders, saves, and rejects an almost blank frame.
    fn shot(&mut self, name: &str) -> &mut Self {
        self.settle();
        let image = self.harness.render().expect("render failed");
        image.save(shot_path(name)).expect("save failed");

        // Compared against the frame's most common colour. Pixel (0,0) is the 1pt window
        // outline, which makes every ground pixel count as distinct.
        let mut histogram = std::collections::HashMap::new();
        for px in image.pixels() {
            *histogram.entry(px.0).or_insert(0usize) += 1;
        }
        let ground = histogram
            .iter()
            .max_by_key(|(_, n)| **n)
            .map(|(c, _)| *c)
            .expect("a rendered frame has pixels");
        let inked = image.pixels().filter(|px| px.0 != ground).count();
        let floor = (image.width() * image.height()) as usize / 20;
        assert!(
            inked > floor,
            "{name} rendered an almost blank frame: {inked} pixels differ from the ground, \
             which is under the {floor} a real screen draws"
        );
        self
    }

    fn state(&self) -> &State {
        &self.harness.state().state
    }
}

/// Coverage-rasterised arrow for the headless demo, with its hotspot at the first point.
const ARROW: [(f32, f32); 7] = [
    (0.0, 0.0),
    (0.0, 16.0),
    (3.9, 12.4),
    (6.4, 18.6),
    (8.6, 17.6),
    (6.1, 11.6),
    (11.0, 11.6),
];
/// Compensates for the reel's displayed downscaling.
const CURSOR_SCALE: f32 = 1.25;
const RING: usize = 7;
/// Demo-only pixel density.
const DEMO_PPP: f32 = 2.0;

fn seg_dist(p: (f32, f32), a: (f32, f32), b: (f32, f32)) -> f32 {
    let (vx, vy) = (b.0 - a.0, b.1 - a.1);
    let (wx, wy) = (p.0 - a.0, p.1 - a.1);
    let len2 = vx * vx + vy * vy;
    let t = if len2 <= f32::EPSILON {
        0.0
    } else {
        ((wx * vx + wy * vy) / len2).clamp(0.0, 1.0)
    };
    let (dx, dy) = (wx - t * vx, wy - t * vy);
    (dx * dx + dy * dy).sqrt()
}

fn edge_dist(p: (f32, f32), poly: &[(f32, f32)]) -> f32 {
    let mut best = f32::MAX;
    let mut j = poly.len() - 1;
    for i in 0..poly.len() {
        best = best.min(seg_dist(p, poly[j], poly[i]));
        j = i;
    }
    best
}

fn inside(p: (f32, f32), poly: &[(f32, f32)]) -> bool {
    let mut hit = false;
    let mut j = poly.len() - 1;
    for i in 0..poly.len() {
        let ((xi, yi), (xj, yj)) = (poly[i], poly[j]);
        if (yi > p.1) != (yj > p.1) && p.0 < (xj - xi) * (p.1 - yi) / (yj - yi) + xi {
            hit = !hit;
        }
        j = i;
    }
    hit
}

/// Numbered frames consumed by `scripts/studio-demo.sh`.
struct Reel {
    dir: std::path::PathBuf,
    frame: usize,
    at: egui::Pos2,
    ring: usize,
}

impl Reel {
    fn new() -> Self {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/studio-demo");
        // Stale frames would be picked up by the encoder's glob and appear in the reel.
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the demo directory");
        Self {
            dir,
            frame: 0,
            at: egui::pos2(W * 0.5, H * 0.7),
            ring: 0,
        }
    }

    /// Advances, draws the synthetic pointer, and writes one frame.
    fn capture(&mut self, d: &mut Driver) {
        let ppp = d.ppp;
        d.harness.step();
        // Remove kittest's debug pointer; the reel draws its own and restores it next frame.
        d.harness.input_mut().events.push(egui::Event::PointerGone);
        d.harness.step();
        let mut image = d.harness.render().expect("render failed");
        if self.ring > 0 {
            self.draw_ring(&mut image, ppp);
            self.ring -= 1;
        }
        self.draw_cursor(&mut image, ppp);
        image
            .save(self.dir.join(format!("{:04}.png", self.frame)))
            .expect("save failed");
        self.frame += 1;
    }

    /// Captures identical idle frames that compress efficiently.
    fn rest(&mut self, d: &mut Driver, frames: usize) -> &mut Self {
        for _ in 0..frames {
            self.capture(d);
        }
        self
    }

    /// Eases the pointer toward a widget through real move events.
    fn glide(&mut self, d: &mut Driver, id: egui::Id, frames: usize) -> &mut Self {
        let to = d.rect_of(id).center();
        let from = self.at;
        for i in 1..=frames {
            let t = i as f32 / frames as f32;
            let eased = if t < 0.5 {
                2.0 * t * t
            } else {
                1.0 - 2.0 * (1.0 - t) * (1.0 - t)
            };
            self.at = from + (to - from) * eased;
            d.harness
                .input_mut()
                .events
                .push(egui::Event::PointerMoved(self.at));
            self.capture(d);
        }
        self
    }

    /// Presses at the pointer with a ring begun before the resulting screen transition.
    fn tap(&mut self, d: &mut Driver) -> &mut Self {
        self.ring = RING;
        self.rest(d, 2);
        d.press_at(self.at);
        self.rest(d, RING - 2)
    }

    fn draw_cursor(&self, image: &mut image::RgbaImage, ppp: f32) {
        let scale = CURSOR_SCALE * ppp;
        let poly: Vec<(f32, f32)> = ARROW.iter().map(|(x, y)| (x * scale, y * scale)).collect();
        // A centred outline preserves acute corners that an inset would fill.
        let stroke = 0.6 * scale;
        let (ox, oy) = ((self.at.x * ppp).floor(), (self.at.y * ppp).floor());
        let span = 22.0 * scale;

        for py in 0..span as i64 {
            for px in 0..span as i64 {
                let p = (px as f32 + 0.5, py as f32 + 0.5);
                let (x, y) = (ox as i64 + px, oy as i64 + py);

                // Offset copy first, so the arrow sits over its own shadow.
                let shifted = (p.0 - 1.6, p.1 - 1.6);
                let shadow = if inside(shifted, &poly) {
                    1.0
                } else {
                    (1.0 - edge_dist(shifted, &poly) / 2.6).clamp(0.0, 1.0)
                };
                if shadow > 0.0 {
                    Self::put(image, x, y, image::Rgba([0, 0, 0, 255]), 0.30 * shadow);
                }

                let d = edge_dist(p, &poly);
                if inside(p, &poly) {
                    Self::put(image, x, y, image::Rgba([255, 255, 255, 255]), 1.0);
                }
                // Straddles the boundary, so the outline keeps one width all the way
                // round and the white edge underneath it is never left aliased.
                let edge = (stroke - d + 0.5).clamp(0.0, 1.0);
                if edge > 0.0 {
                    Self::put(image, x, y, image::Rgba([16, 18, 24, 255]), edge);
                }
            }
        }
    }

    /// A ring opening out from the click, fading as it goes.
    fn draw_ring(&self, image: &mut image::RgbaImage, ppp: f32) {
        let done = (RING - self.ring) as f32 / RING as f32;
        let radius = (7.0 + 20.0 * done) * ppp;
        let alpha = 0.5 * (1.0 - done);
        let reach = (radius + 3.0) as i64;
        let (cx, cy) = ((self.at.x * ppp) as i64, (self.at.y * ppp) as i64);
        for dy in -reach..=reach {
            for dx in -reach..=reach {
                let d2 = ((dx * dx + dy * dy) as f32).sqrt();
                if (d2 - radius).abs() < ppp {
                    Self::put(
                        image,
                        cx + dx,
                        cy + dy,
                        image::Rgba([255, 255, 255, 255]),
                        alpha,
                    );
                }
            }
        }
    }

    fn put(image: &mut image::RgbaImage, x: i64, y: i64, colour: image::Rgba<u8>, alpha: f32) {
        if x < 0 || y < 0 || x >= image.width() as i64 || y >= image.height() as i64 {
            return;
        }
        let (x, y) = (x as u32, y as u32);
        let under = *image.get_pixel(x, y);
        let mix = |a: u8, b: u8| (a as f32 * (1.0 - alpha) + b as f32 * alpha) as u8;
        image.put_pixel(
            x,
            y,
            image::Rgba([
                mix(under[0], colour[0]),
                mix(under[1], colour[1]),
                mix(under[2], colour[2]),
                255,
            ]),
        );
    }
}

/// Driver with the demo open and its first page read; setup runs after opening.
fn workspace(setup: impl FnOnce(&mut State)) -> Driver {
    sized_workspace(W, H, setup)
}

/// The same, at a chosen window size.
fn sized_workspace(w: f32, h: f32, setup: impl FnOnce(&mut State)) -> Driver {
    let mut d = Driver::sized(w, h, |_| {});
    d.open_demo();
    d.with_state(setup);
    d
}

/// Renders the README demo frames; ignored because it produces output rather than assertions.
///
///     cargo test -p citadeldb-studio --test shots -- --ignored demo_reel
#[test]
#[ignore]
fn demo_reel() {
    let tree = |label: &str| egui::Id::new(("tree", label));
    let btn = |label: &str| egui::Id::new(("btn", label));

    let mut d = Driver::hidpi(|_| {});
    let mut reel = Reel::new();
    reel.rest(&mut d, 20);

    reel.glide(&mut d, egui::Id::new(("home_row", "demo")), 18)
        .rest(&mut d, 5)
        .tap(&mut d)
        .rest(&mut d, 30);

    reel.glide(&mut d, egui::Id::new(("row", 6usize)), 14)
        .rest(&mut d, 4)
        .tap(&mut d)
        .rest(&mut d, 24);

    reel.glide(&mut d, btn("Verify visible rows"), 18)
        .rest(&mut d, 4)
        .tap(&mut d)
        .rest(&mut d, 34);

    reel.glide(&mut d, tree("Query"), 20)
        .rest(&mut d, 4)
        .tap(&mut d)
        .rest(&mut d, 24);

    reel.glide(&mut d, btn("Run"), 16)
        .rest(&mut d, 4)
        .tap(&mut d)
        .rest(&mut d, 34);

    reel.glide(&mut d, tree("documents.embedding"), 18)
        .rest(&mut d, 4)
        .tap(&mut d)
        .rest(&mut d, 30);

    reel.rest(&mut d, 30);

    assert!(
        reel.frame >= 300,
        "a {}-frame reel is too short to follow",
        reel.frame
    );
}

#[test]
fn home_screen() {
    Driver::new(|_| {}).shot("10-home");
}

#[test]
fn unlock_screen() {
    Driver::new(|s| s.route = Route::Unlock).shot("11-unlock");
}

#[test]
fn create_screen() {
    Driver::new(|s| citadel_studio::state::apply(s, Action::BeginCreate)).shot("42-create");
}

#[test]
fn memory_screen() {
    workspace(|s| s.selected_row = 1).shot("12-memory");
}

#[test]
fn security_screen() {
    workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Security));
    })
    .shot("13-security");
}

/// Integrity remains explicitly requested because it reads every page; "not checked" and
/// "checked clean" are distinct states.
#[test]
fn the_integrity_check_is_asked_for_and_reports_what_it_found() {
    use citadel_studio::state::IntegrityState;

    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Security));
    });
    assert!(
        matches!(d.state().integrity, IntegrityState::None),
        "a vault nobody has checked must not read as checked"
    );

    d.click_id(egui::Id::new(("btn", "Verify vault")));

    match &d.state().integrity {
        IntegrityState::Checked(facts) => {
            assert!(facts.pages_checked > 0, "the walk read no pages");
            assert!(
                facts.errors.is_empty(),
                "the demo vault is intact: {:?}",
                facts.errors
            );
        }
        other => panic!("the check did not finish: {other:?}"),
    }
    d.shot("38-security-checked");
}

/// Explain renders the returned plan.
#[test]
fn explain_puts_its_plan_on_screen() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
        s.query = "SELECT * FROM documents".to_owned();
    });
    d.click_id(egui::Id::new(("btn", "Explain")));

    let plan = d.state().plan.clone().expect("Explain produced no plan");
    assert!(!plan.lines.is_empty(), "an empty plan is not a plan");
    assert!(
        !plan.measured,
        "EXPLAIN must not report measurements; it does not run the statement"
    );
    assert!(
        plan.lines.iter().all(|l| !l.contains("actual time=")),
        "a time appeared without anything having been timed: {:?}",
        plan.lines
    );
    let count = plan.lines.len();
    d.harness.get_by_role_and_label(
        egui::accesskit::Role::Status,
        &format!(
            "Plan only · {count} {} · Statement was not executed",
            if count == 1 { "line" } else { "lines" }
        ),
    );
    d.shot("40-query-explain");
}

/// Analyze executes the statement and reports measured time, unlike Explain.
#[test]
fn analyze_runs_the_statement_and_reports_what_it_measured() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
        s.query = "SELECT * FROM documents".to_owned();
    });
    d.click_id(egui::Id::new(("btn", "Analyze")));

    let plan = d.state().plan.clone().expect("Analyze produced no plan");
    assert!(plan.measured, "Analyze must report that it measured");
    assert!(
        plan.lines.iter().any(|l| l.contains("actual time=")),
        "no measured time in a measured plan: {:?}",
        plan.lines
    );
    assert!(
        plan.lines.iter().any(|l| l.contains("emitted=")),
        "no row count in a measured plan: {:?}",
        plan.lines
    );
    let count = plan.lines.len();
    d.harness.get_by_role_and_label(
        egui::accesskit::Role::Status,
        &format!(
            "Measured execution · {count} {} · Statement executed; actual time, scanned rows, and emitted rows included",
            if count == 1 { "line" } else { "lines" }
        ),
    );
    d.shot("41-query-analyze");
}

#[test]
fn explain_and_analyze_switch_cleanly_in_one_editor_session() {
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
        s.query = "SELECT * FROM documents".to_owned();
    });

    d.click_id(egui::Id::new(("btn", "Explain")));
    let planned = d.state().plan.as_ref().expect("Explain returned a plan");
    assert!(!planned.measured);
    assert!(planned
        .lines
        .iter()
        .all(|line| !line.contains("actual time=")));

    d.click_id(egui::Id::new(("btn", "Analyze")));
    let measured = d.state().plan.as_ref().expect("Analyze returned a plan");
    assert!(measured.measured);
    assert!(measured
        .lines
        .iter()
        .any(|line| line.contains("actual time=")));

    d.click_id(egui::Id::new(("btn", "Explain")));
    let planned_again = d.state().plan.as_ref().expect("Explain returned a plan");
    assert!(!planned_again.measured);
    assert!(
        planned_again
            .lines
            .iter()
            .all(|line| !line.contains("actual time=")),
        "Explain retained Analyze metrics: {:?}",
        planned_again.lines
    );
}

#[test]
fn analyze_runs_mutations_instead_of_only_estimating_them() {
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
        citadel_studio::state::apply(
            s,
            Action::EditQuery(
                "UPDATE documents SET title = 'measured mutation' WHERE id = 1;".to_owned(),
            ),
        );
    });

    d.click_id(egui::Id::new(("btn", "Analyze")));
    assert!(
        d.state().plan.as_ref().is_some_and(|plan| plan.measured),
        "Analyze did not produce a measured plan"
    );

    d.apply(Action::EditQuery(
        "SELECT title FROM documents WHERE id = 1;".to_owned(),
    ));
    d.click_id(egui::Id::new(("btn", "Run")));
    let run = d.state().result.done().expect("the read completed");
    let citadel_studio::engine::Statement::Rows(rows) = &run.statements[0] else {
        panic!("the verification query did not return rows: {run:?}");
    };
    assert_eq!(
        rows.result.rows[0][0],
        citadel_sql::Value::Text("measured mutation".into()),
        "Analyze estimated the update without executing it"
    );
}

/// Import creates the inspected SQLite schema; inspection alone must not copy rows.
#[test]
fn import_creates_the_tables_in_the_open_vault() {
    let (_source_dir, path) = sqlite_fixture(
        "notes",
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, ts INTEGER);
         INSERT INTO notes (body, ts) VALUES ('a', 1), ('b', 2);",
    );
    let tables = citadel_studio::sqlite::read(&path).expect("the fixture is readable");

    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::BeginImport);
        citadel_studio::state::apply(
            s,
            Action::SetImportSource(citadel_studio::state::Source::Read {
                path: path.clone(),
                tables: tables.clone(),
            }),
        );
    });
    d.click_id(egui::Id::new(("btn", "Create tables")));

    let opened = d
        .harness
        .state()
        .state
        .session
        .opened()
        .expect("the demo vault is open");
    assert!(
        opened.tables.iter().any(|t| t.name == "notes"),
        "the imported table is not in the vault: {:?}",
        opened.tables.iter().map(|t| &t.name).collect::<Vec<_>>()
    );
}

/// Format rewrites SQL through the engine parser's AST display.
#[test]
fn format_rewrites_the_query_through_the_parser() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
        s.query = "select   a,b  from   documents where a=1".to_owned();
    });
    d.click_id(egui::Id::new(("btn", "Format")));

    let formatted = d.state().query.clone();
    assert_ne!(
        formatted, "select   a,b  from   documents where a=1",
        "Format left the text exactly as typed"
    );
    assert!(
        formatted.contains("SELECT") && formatted.contains("FROM"),
        "the parser's own rendering keywords: {formatted}"
    );
    assert!(
        formatted.ends_with(';'),
        "statements are terminated: {formatted}"
    );
    assert_eq!(
        d.state().query_notice,
        Some(citadel_studio::state::EditorNotice::Formatted { changed: true })
    );
    d.harness.get_by_role_and_label(
        egui::accesskit::Role::Status,
        "Query formatted · Editor text changed; nothing was run",
    );
}

#[test]
fn format_handles_the_seeded_demo_query_and_preserves_its_note() {
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
    });
    let note = d
        .state()
        .query
        .lines()
        .next()
        .expect("the demo starts with a note")
        .to_owned();

    d.click_id(egui::Id::new(("btn", "Format")));

    assert_eq!(d.state().query.lines().next(), Some(note.as_str()));
    assert_eq!(
        d.state().query_notice,
        Some(citadel_studio::state::EditorNotice::Formatted { changed: true })
    );
}

#[test]
fn format_reports_when_the_editor_is_already_formatted() {
    use egui_kittest::kittest::Queryable;

    let formatted = citadel_studio::model::format_sql("SELECT id FROM documents;")
        .expect("the fixture is valid");
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
        citadel_studio::state::apply(s, Action::EditQuery(formatted.clone()));
    });

    d.click_id(egui::Id::new(("btn", "Format")));

    assert_eq!(d.state().query, formatted);
    assert_eq!(
        d.state().query_notice,
        Some(citadel_studio::state::EditorNotice::Formatted { changed: false })
    );
    d.harness.get_by_role_and_label(
        egui::accesskit::Role::Status,
        "Already formatted · Editor text was unchanged; nothing was run",
    );
}

/// Invalid SQL remains unchanged because Format is not a syntax check.
#[test]
fn format_leaves_unparseable_text_exactly_as_typed() {
    let typed = "select from where )(";
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
        s.query = typed.to_owned();
    });
    d.click_id(egui::Id::new(("btn", "Format")));
    assert_eq!(
        d.state().query,
        typed,
        "Format destroyed work it could not parse"
    );
}

#[test]
fn format_refuses_to_delete_comments_or_hints() {
    use egui_kittest::kittest::Queryable;

    let typed = "-- keep this operator note\nSELECT /*+ keep this hint */ * FROM documents;";
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
        citadel_studio::state::apply(s, Action::EditQuery(typed.to_owned()));
    });

    d.click_id(egui::Id::new(("btn", "Format")));

    assert_eq!(
        d.state().query,
        typed,
        "Format silently deleted source text"
    );
    assert!(
        d.state()
            .query_notice
            .as_ref()
            .is_some_and(|notice| matches!(
                notice,
                citadel_studio::state::EditorNotice::Unchanged(message)
                    if message.contains("comments and hints")
            )),
        "the refusal was not explained: {:?}",
        d.state().query_notice
    );
    d.harness.get_by_role_and_label(
        egui::accesskit::Role::Status,
        "Query unchanged · Format skipped: comments and hints must be preserved exactly",
    );
}

/// Cancels a bounded 1,500 x 1,500 join through the out-of-band token while the actor is
/// occupied and cannot read its command channel.
#[test]
fn a_running_statement_can_be_cancelled() {
    use citadel_studio::engine::{Command, Handle, Kind, Reply};

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut engine = Handle::spawn();
    engine
        .send(Command::OpenDemo)
        .expect("the engine is running");
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "the demo never opened"
        );
        if let Some(Reply::Opened(opened)) = engine.poll() {
            opened.expect("the demo vault opens");
            break;
        }
    }

    engine
        .send(Command::Run(
            "SELECT COUNT(*) FROM documents a, documents b".to_owned(),
        ))
        .expect("the engine is running");
    // Repeat the idempotent cancel to cover pickup races.
    let reply = loop {
        assert!(
            std::time::Instant::now() < deadline,
            "the statement neither finished nor cancelled"
        );
        engine.cancel();
        if let Some(reply) = engine.poll() {
            break reply;
        }
    };

    let Reply::Ran { run, .. } = reply else {
        panic!("expected the statement's answer, got something else");
    };
    let failure = run
        .failed
        .expect("a cancelled statement must not report success");
    assert_eq!(
        failure.kind,
        Kind::Cancelled,
        "a cancel must be reported as one, not as a damaged database: {failure}"
    );
}

/// Cancellation never queues a command behind the work it must stop.
#[test]
fn cancelling_does_not_travel_the_command_channel() {
    let mut d = workspace(|_| {});
    d.apply(Action::CancelQuery);
    assert!(
        !d.harness.state().engine_busy(),
        "cancelling queued a command instead of tripping the flag"
    );
}

/// Changes the passphrase through the UI and verifies the resulting file with both secrets.
#[test]
fn changing_the_passphrase_changes_which_secret_opens_the_vault() {
    use citadel_studio::engine::session::{CreateSpec, Session};
    use citadel_studio::state::{RotateState, SecretField};

    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("rotate.cdl");
    let spec = CreateSpec {
        path: path.clone(),
        passphrase: "the-first-passphrase".to_owned().into(),
        kdf: citadel_studio::state::KdfAlgorithm::Argon2id,
        profile: citadel_studio::state::Argon2Profile::Iot,
    };
    drop(Session::create(&spec).expect("the vault is created"));

    let mut d = Driver::new({
        let path = path.clone();
        move |s: &mut State| {
            s.route = Route::Unlock;
            s.target = Target::Picked(path.clone());
        }
    });
    d.type_text(&spec.passphrase).key(egui::Key::Enter);
    assert!(d.state().vault.is_some(), "the vault did not open");
    d.apply(Action::OpenDoc(Doc::Security));

    d.click_id(egui::Id::new(("btn", "Change passphrase")));
    assert!(
        matches!(d.state().rotate, RotateState::Open(_)),
        "the button did not open the form"
    );

    for (field, value) in [
        (SecretField::RotateCurrent, spec.passphrase.as_str()),
        (SecretField::RotateNext, "the-second-passphrase"),
        (SecretField::RotateConfirm, "the-second-passphrase"),
    ] {
        d.apply(Action::SetSecret(field, value.to_owned().into()));
    }
    d.click_id(egui::Id::new(("btn", "Change passphrase")));
    assert!(
        matches!(d.state().rotate, RotateState::Done),
        "the rotation did not finish: {:?}",
        d.state().rotate
    );
    d.shot("39-security-rotated");

    // Release the exclusive lock before reopening the file.
    d.apply(Action::CloseVault);
    drop(d);
    assert!(
        Session::open(&path, &spec.passphrase).is_err(),
        "the old passphrase still opens the vault"
    );
    drop(Session::open(&path, "the-second-passphrase").expect("the new passphrase opens it"));
}

/// A wrong current passphrase is refused, and the vault keeps the one it had.
#[test]
fn changing_with_the_wrong_current_passphrase_is_refused() {
    use citadel_studio::engine::session::{CreateSpec, Session};
    use citadel_studio::state::{RotateState, SecretField};

    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("refused.cdl");
    let spec = CreateSpec {
        path: path.clone(),
        passphrase: "the-first-passphrase".to_owned().into(),
        kdf: citadel_studio::state::KdfAlgorithm::Argon2id,
        profile: citadel_studio::state::Argon2Profile::Iot,
    };
    drop(Session::create(&spec).expect("the vault is created"));

    let mut d = Driver::new({
        let path = path.clone();
        move |s: &mut State| {
            s.route = Route::Unlock;
            s.target = Target::Picked(path.clone());
        }
    });
    d.type_text(&spec.passphrase).key(egui::Key::Enter);
    d.apply(Action::OpenDoc(Doc::Security));
    d.apply(Action::BeginRotate);
    for (field, value) in [
        (SecretField::RotateCurrent, "not-the-passphrase"),
        (SecretField::RotateNext, "the-second-passphrase"),
        (SecretField::RotateConfirm, "the-second-passphrase"),
    ] {
        d.apply(Action::SetSecret(field, value.to_owned().into()));
    }
    d.click_id(egui::Id::new(("btn", "Change passphrase")));
    assert!(
        matches!(d.state().rotate, RotateState::Failed(_)),
        "a wrong current passphrase must be refused: {:?}",
        d.state().rotate
    );

    d.apply(Action::CloseVault);
    drop(d);
    drop(Session::open(&path, &spec.passphrase).expect("the original passphrase still opens it"));
}

/// A disposable demo has no durable credential to rotate, in the UI or reducer.
#[test]
fn a_disposable_demo_cannot_offer_passphrase_rotation() {
    use citadel_studio::state::RotateState;
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|_| {});
    assert!(d.state().vault().is_demo);
    d.apply(Action::OpenDoc(Doc::Security));

    assert!(
        d.harness.query_by_label("Change passphrase").is_none(),
        "the disposable demo exposed a passphrase-rotation control"
    );
    d.apply(Action::BeginRotate);
    assert!(matches!(d.state().rotate, RotateState::Closed));
}

/// Audit verifies every segment and reports key erasures as a floor.
#[test]
fn the_audit_chain_and_key_count_are_reported_as_the_engine_states_them() {
    let d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Security));
    });
    let facts = &d.state().vault().facts;

    let audit = facts
        .audit
        .as_ref()
        .expect("the demo vault keeps an audit log");
    assert!(audit.entries > 0, "opening and seeding wrote audit entries");
    assert!(
        !audit.segments.is_empty(),
        "every log has at least one segment"
    );
    assert!(
        audit.has_no_detected_inconsistency(),
        "a freshly written chain must verify"
    );

    // Reused tombstone slots make this a floor rather than a total.
    let region = facts
        .keys
        .region
        .expect("an encrypted vault has a region store");
    assert!(region.total_slots >= region.tombstoned);
}

/// A truncated tail leaves the retained chain valid but produces an unauthenticated
/// mutable-count shortfall, not a chain-break claim.
#[test]
fn audit_header_count_shortfall_is_reported() {
    use citadel_studio::engine::session::{CreateSpec, Session};

    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("truncated.cdl");
    let spec = CreateSpec {
        path: path.clone(),
        passphrase: "the-real-passphrase".to_owned().into(),
        kdf: citadel_studio::state::KdfAlgorithm::Argon2id,
        profile: citadel_studio::state::Argon2Profile::Iot,
    };
    drop(Session::create(&spec).expect("the vault is created"));

    // Drop the last entry without updating the mutable header count.
    let log = path.with_extension("cdl.citadel-audit");
    let entries = citadel::read_audit_log(&log).expect("the log reads without a key");
    assert!(entries.len() >= 2, "the vault wrote entries to cut");
    let bytes = std::fs::read(&log).expect("read the log");
    let mut end = 64usize;
    for _ in 0..entries.len() - 1 {
        end += 4;
        let len = u32::from_le_bytes(bytes[end..end + 4].try_into().unwrap()) as usize;
        end += len;
    }
    std::fs::write(&log, &bytes[..end]).expect("truncate the log");

    let mut d = Driver::new(move |s: &mut State| {
        s.route = Route::Unlock;
        s.target = Target::Picked(path.clone());
    });
    d.type_text(&spec.passphrase);
    d.click_id(egui::Id::new(("btn", "Unlock")));
    d.click_id(egui::Id::new(("tree", "Security")));

    let audit = d
        .state()
        .vault()
        .facts
        .audit
        .as_ref()
        .expect("the vault keeps an audit log")
        .clone();
    assert!(
        audit.chain_links(),
        "the surviving entries still link; the stale count is a separate warning"
    );
    assert_eq!(
        audit.count_shortfall(),
        1,
        "readable records are one below the mutable header count"
    );
    assert!(
        !audit.has_no_detected_inconsistency(),
        "the mutable count mismatch should remain visible"
    );
}

#[test]
fn query_screen() {
    workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
    })
    .shot("14-query");
}

/// Renders the light syntax ramp; contrast is checked separately.
#[test]
fn query_screen_light() {
    workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
        citadel_studio::state::apply(s, Action::ToggleTheme);
    })
    .shot("41-query-light");
}

#[test]
fn vector_screen() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|s| {
        citadel_studio::state::apply(
            s,
            Action::OpenDoc(Doc::Vector("documents.embedding".into())),
        );
    });
    assert!(
        d.harness
            .query_by_label("SQL sample · no per-row attestation")
            .is_some(),
        "the generic vector sample implied a row proof it cannot carry"
    );
    assert!(
        d.harness
            .query_by_label_contains("Raw vector components zero and one")
            .is_some(),
        "the canvas still describes the plot as unnamed dimensions"
    );
    d.shot("15-vector");
}

fn stress_vector_points(count: usize) -> Vec<citadel_studio::model::Projected> {
    let unit = |i: usize, salt: u64| {
        let mut value = (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ salt;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        (value >> 40) as f32 / ((1u32 << 24) - 1) as f32
    };
    (0..count)
        .map(|i| citadel_studio::model::Projected {
            x: unit(i, 0x36d7_2f85_12a4_c9eb),
            y: unit(i, 0xa8f4_09b1_7e63_d52c),
            evidence: Evidence::NotAttestable,
        })
        .collect()
}

#[test]
fn vector_points_have_a_readable_raster_footprint() {
    let mut d = workspace(|s| {
        citadel_studio::state::apply(
            s,
            Action::OpenDoc(Doc::Vector("documents.embedding".into())),
        );
    });
    let points = d.state().vault().vectors[0].points.len();
    let canvas = d.rect_of(egui::Id::new("vector_canvas")).shrink(64.0);
    let image = d.harness.render().expect("render failed");
    let ground = Palette::DARK.ground1;
    let differs = |pixel: &image::Rgba<u8>| {
        pixel[0].abs_diff(ground.r()) as u16
            + pixel[1].abs_diff(ground.g()) as u16
            + pixel[2].abs_diff(ground.b()) as u16
            > 30
    };
    let visible = (canvas.top().ceil() as u32..canvas.bottom().floor() as u32)
        .flat_map(|y| {
            let image = &image;
            (canvas.left().ceil() as u32..canvas.right().floor() as u32)
                .map(move |x| image.get_pixel(x, y))
        })
        .filter(|pixel| differs(pixel))
        .count();

    assert!(
        visible >= points * 3,
        "{points} sampled vectors cover only {visible} readable pixels; a one-pixel cloud \
         disappears against the canvas"
    );
}

#[test]
fn normalized_extrema_are_complete_inside_the_pickable_canvas() {
    use citadel_studio::model::{Projected, VectorColumn};

    let mut d = workspace(|s| {
        citadel_studio::state::apply(
            s,
            Action::OpenDoc(Doc::Vector("documents.embedding".into())),
        );
    });
    d.with_state(|state| {
        state.vault.as_mut().unwrap().vectors[0] = VectorColumn::new(
            "documents",
            "embedding",
            2,
            4,
            vec![
                Projected {
                    x: 0.0,
                    y: 0.5,
                    evidence: Evidence::Tampered,
                },
                Projected {
                    x: 1.0,
                    y: 0.5,
                    evidence: Evidence::Tampered,
                },
                Projected {
                    x: 0.5,
                    y: 0.0,
                    evidence: Evidence::Tampered,
                },
                Projected {
                    x: 0.5,
                    y: 1.0,
                    evidence: Evidence::Tampered,
                },
            ],
        );
    });
    let interactive = d.rect_of(egui::Id::new("vector_canvas"));
    // The interaction rect expands the center-mapping rect by the seven-point pick radius.
    let plot = interactive.shrink(7.0);
    let image = d.harness.render().expect("render failed");
    let red = |x: u32, y: u32| {
        let pixel = image.get_pixel(x, y);
        pixel[0] > pixel[1].saturating_add(20) && pixel[0] > pixel[2].saturating_add(20)
    };
    let band_has_point = |x0: f32, x1: f32, y0: f32, y1: f32| {
        (y0.floor() as u32..y1.ceil() as u32)
            .flat_map(|y| (x0.floor() as u32..x1.ceil() as u32).map(move |x| (x, y)))
            .any(|(x, y)| red(x, y))
    };
    const BAND: f32 = 8.0;

    assert!(band_has_point(
        plot.left() - BAND,
        plot.left(),
        plot.center().y - BAND,
        plot.center().y + BAND
    ));
    assert!(band_has_point(
        plot.right(),
        plot.right() + BAND,
        plot.center().y - BAND,
        plot.center().y + BAND
    ));
    assert!(band_has_point(
        plot.center().x - BAND,
        plot.center().x + BAND,
        plot.top() - BAND,
        plot.top()
    ));
    assert!(band_has_point(
        plot.center().x - BAND,
        plot.center().x + BAND,
        plot.bottom(),
        plot.bottom() + BAND
    ));
}

/// Pins the canonical icon masters consumed by the app and packaging.
#[test]
fn the_packaged_icons_are_present_and_agree() {
    let open = |p: &str| {
        image::open(format!("assets/{p}"))
            .unwrap_or_else(|e| panic!("assets/{p}: {e}"))
            .to_rgba8()
    };
    // The vector and 1024 raster are the canonical source; the window icon is its
    // detailed 256 px rendering.
    let svg = std::fs::read_to_string("assets/icon.svg").expect("assets/icon.svg");
    assert!(svg.contains("L 14 8"));
    assert!(svg.contains("L 14 12"));
    assert!(svg.contains("L 20 12"));
    assert!(svg.contains("Q 24 16.6 10.5 21"));
    assert_eq!(open("icon.png").dimensions(), (1024, 1024));
    let window_png = open("icon-256.png");
    assert_eq!(window_png.dimensions(), (256, 256));

    let window = citadel_studio::icon::icon_data();
    assert_eq!(window.width, 256);
    assert_eq!(
        window.rgba,
        window_png.as_raw().as_slice(),
        "the window icon is not what icon-256.png holds"
    );

    window_png.save(shot_path("00-icon")).expect("save failed");
}

/// Pins the Windows icon's shell sizes. Every frame uses the canonical detailed mark.
#[test]
fn the_windows_icon_has_every_size() {
    use sha2::{Digest, Sha256};

    let d = std::fs::read("assets/icon.ico").expect("assets/icon.ico");
    let count = u16::from_le_bytes([d[4], d[5]]) as usize;
    let frames: Vec<(u32, String)> = (0..count)
        .map(|i| {
            let entry = 6 + i * 16;
            let width = if d[entry] == 0 { 256 } else { d[entry] as u32 };
            let len = u32::from_le_bytes(d[entry + 8..entry + 12].try_into().unwrap()) as usize;
            let offset = u32::from_le_bytes(d[entry + 12..entry + 16].try_into().unwrap()) as usize;
            let rgba = image::load_from_memory(&d[offset..offset + len])
                .unwrap_or_else(|e| panic!("decode {width} px icon frame: {e}"))
                .to_rgba8();
            (width, format!("{:x}", Sha256::digest(rgba.as_raw())))
        })
        .collect();
    assert_eq!(
        frames,
        [
            (
                16,
                "7efd9db7b947df71fa066099bfb273e5da7bd46cbe23eb4fa1bda12494f909e1".into()
            ),
            (
                24,
                "8061890c54eb97c0a3242cd9fc6ba2da6989a3e71837dc737b36b1fe46b28edc".into()
            ),
            (
                32,
                "b97c22bdf758ebe584396f21887c751d27a94d68cee5d65703c151adea6b6ee6".into()
            ),
            (
                48,
                "8f875fcab226b4c5b26928e3b9956a71a290f6f7feeba35546d775b08cef61d9".into()
            ),
            (
                256,
                "fbe2cfbb8532c0641164cbdc2e3b71653d30ec4dc2c779c8935411b8495ba75d".into()
            ),
        ],
        "the shell icon sizes or canonical detailed artwork changed"
    );
}

/// Renders the light variant through the application.
#[test]
fn memory_screen_light() {
    workspace(|s| {
        s.selected_row = 1;
        citadel_studio::state::apply(s, Action::ToggleTheme);
    })
    .shot("16-memory-light");
}

#[test]
fn attestation_tab() {
    workspace(|s| {
        citadel_studio::state::apply(s, Action::SetGridTab(GridTab::Attestation));
    })
    .shot("17-attestation");
}

/// Structure has no grid window and keeps visible-row verification unavailable.
#[test]
fn structure_tab() {
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::SetGridTab(GridTab::Structure));
    });
    d.shot("34-structure");

    let verify = d
        .harness
        .ctx
        .read_response(egui::Id::new(("btn", "Verify visible rows")))
        .expect("the button is still drawn");
    assert!(
        !verify.sense.senses_click(),
        "Verify rows is still clickable on a tab that reports no rows"
    );
}

/// Plaintext regions expose no attestable atoms.
#[test]
fn plaintext_region() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Region("scratch".into())));
    });
    assert!(
        d.harness
            .query_by_label_contains("Not attestable — plaintext region")
            .is_some(),
        "the plaintext region is still presented as waiting for a proof it cannot have"
    );
    assert!(
        d.state()
            .visible_atoms()
            .unwrap()
            .iter()
            .all(|atom| atom.evidence() == Evidence::NotAttestable
                && atom.verified_at.is_none()
                && atom.key_slot.is_none()),
        "plaintext policy was presented as a verification event"
    );
    d.shot("18-plaintext-region");
}

/// Every pane fits the declared minimum window.
#[test]
fn minimum_window_size() {
    sized_workspace(900.0, 540.0, |s| {
        s.selected_row = 1;
    })
    .shot("22-min-size");
}

#[test]
fn density_compact() {
    workspace(|s| {
        citadel_studio::state::apply(s, Action::CycleDensity);
        citadel_studio::state::apply(s, Action::CycleDensity);
    })
    .shot("23-density-compact");
}

/// SQL rows carry no evidence rail because attestation is per memory atom.
#[test]
fn table_browser() {
    workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Table("documents".into())));
    })
    .shot("25-table");
}

// ---------------------------------------------------------------- behaviour

/// Clicking the demo-vault row on Home opens the vault.
#[test]
fn clicking_demo_vault_opens_it() {
    let mut d = Driver::new(|_| {});
    assert!(d.state().vault.is_none());
    d.click_id(egui::Id::new(("home_row", "demo")));
    let s = d.state();
    assert!(s.vault.is_some(), "demo vault did not open");
    assert_eq!(s.route, Route::Workspace);
    assert_eq!(s.docs.len(), 1);
}

/// Selecting a sidebar region opens it as a document and focuses it.
#[test]
fn clicking_a_region_opens_a_document() {
    let mut d = workspace(|_| {});
    d.click_id(egui::Id::new(("tree", "semantic")));
    let s = d.state();
    let opened = Doc::Region("semantic".into());
    assert!(
        s.docs.contains(&opened),
        "semantic did not open; docs are {:?}",
        s.docs.iter().map(|d| d.title()).collect::<Vec<_>>()
    );
    // Opening also focuses the new document.
    assert_eq!(
        s.active_doc(),
        Some(&opened),
        "semantic opened but the focus stayed on {:?}",
        s.active_doc().map(Doc::title)
    );
    assert_eq!(s.scroll_row, 0, "the new document kept the old scroll");
}

/// Arrow keys move the grid selection and the inspector follows.
#[test]
fn arrows_move_the_selection() {
    let mut d = workspace(|s| s.selected_row = 0);
    d.key(egui::Key::ArrowDown).key(egui::Key::ArrowDown);
    assert_eq!(d.state().selected_row, 2);
    d.key(egui::Key::ArrowUp);
    assert_eq!(d.state().selected_row, 1);
    // Home clamps at the first row and does not underflow.
    d.key(egui::Key::Home);
    assert_eq!(d.state().selected_row, 0);
}

/// Verification records the engine verdict for every visible row in an encrypted region;
/// plaintext behavior is covered separately because attestability is region-wide.
#[test]
fn verification_only_claims_visible_rows() {
    let mut d = workspace(|_| {});
    assert!(
        count(&d, Evidence::Unverified) > 0,
        "nothing has been checked yet, so every row starts unverified"
    );

    d.apply(Action::VerifyPage);
    d.until("the verdicts to arrive", |s| s.page_checked.scope > 0);
    d.shot("19-after-verify");

    let s = d.state();
    let atoms = s.visible_atoms().expect("a region is focused");
    let checked = s.page_checked.scope as usize;
    assert_eq!(
        atoms
            .iter()
            .take(checked)
            .filter(|a| a.verified_at.is_none())
            .count(),
        0,
        "a row inside the checked window carries no proof time"
    );
    assert!(
        atoms
            .iter()
            .take(checked)
            .all(|a| a.evidence() == Evidence::Verified),
        "an untouched atom in a sealed region must come back authentic"
    );
    // The engine verdict remains bound to the atom id.
    assert!(atoms.iter().take(checked).all(|a| a.aad_bound));
    assert_eq!(s.page_checked.authentic as usize, checked);
}

/// On a short viewport, verification touches exactly the visible rows. This distinguishes
/// viewport scope from a fixed row count or the full region length.
#[test]
fn verify_covers_the_window_and_no_more() {
    let mut d = sized_workspace(1100.0, 700.0, |_| {});
    let visible = d.state().visible_rows;
    let total = d.state().visible_atoms().unwrap().len();
    assert!(
        visible > 0 && visible < total,
        "this test needs a window smaller than the region: {visible} of {total}"
    );

    d.apply(Action::VerifyPage);
    d.until("the verdicts to arrive", |s| s.page_checked.scope > 0);

    let s = d.state();
    assert_eq!(
        s.page_checked.scope as usize, visible,
        "reported scope does not match the window"
    );
    // `all` ensures verification does not overshoot the visible window.
    assert!(
        s.visible_atoms().unwrap()[visible..]
            .iter()
            .all(|a| a.verified_at.is_none()),
        "verify reached past the window it claimed"
    );
}

/// Visible-row verification state is scoped to the region where it was computed.
#[test]
fn a_page_check_is_not_claimed_over_another_region() {
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Region("episodic".into())));
    });
    d.apply(Action::VerifyPage);
    d.until("the verdicts to arrive", |s| s.page_checked.scope > 0);

    let checked = {
        let s = d.state();
        let label = s.page_check_label();
        assert!(
            label.ends_with("visible rows authentic"),
            "the region it ran on should report it, got {label:?}"
        );
        s.page_checked.clone()
    };

    d.harness.state_mut().state.scroll_row = 1;
    assert_eq!(
        d.state().page_check_label(),
        "no visible rows verified",
        "a result for the previous atom ids was claimed over a different visible window"
    );
    d.harness.state_mut().state.scroll_row = 0;
    assert_eq!(
        d.state().page_checked,
        checked,
        "scrolling should not discard the exact verification record"
    );

    // Switch to another region with no verification state.
    d.apply(Action::OpenDoc(Doc::Region("scratch".into())));
    let s = d.state();
    assert_eq!(
        s.page_checked, checked,
        "the verdict itself is still held; only the claim moves"
    );
    assert_eq!(
        s.page_check_label(),
        "no visible rows verified",
        "a count from another region was reported over this one"
    );

    d.apply(Action::OpenDoc(Doc::Security));
    let s = d.state();
    assert_eq!(
        s.page_check_label(),
        "no visible rows verified",
        "a region's count was reported over the Security document"
    );
}

/// Plaintext rows expose the missing proof without dispatching meaningless work.
#[test]
fn plaintext_rows_cannot_start_verification() {
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Region("scratch".into())));
    });
    // `scratch` is unsealed and has no per-atom MAC.
    assert!(d
        .state()
        .visible_region()
        .expect("scratch is focused")
        .plaintext());

    let verify = d
        .harness
        .ctx
        .read_response(egui::Id::new(("btn", "Verify visible rows")))
        .expect("the disabled verification control is still discoverable");
    assert!(
        !verify.sense.senses_click(),
        "plaintext rows have no proof, so verification must not be dispatched"
    );
    d.shot("30-verify-plaintext");

    // The same precondition guards direct actions and the command palette, not just the
    // painted button.
    d.apply(Action::VerifyPage);
    let commands = citadel_studio::state::commands(d.state());
    assert!(
        commands
            .iter()
            .all(|command| command.title != "Verify visible rows"),
        "the command palette bypassed the plaintext verification guard"
    );

    let s = d.state();
    assert_eq!(s.page_checked.scope, 0);
    assert!(s
        .visible_atoms()
        .unwrap()
        .iter()
        .all(|a| a.evidence() == Evidence::NotAttestable));
}

#[test]
fn command_palette_cannot_bypass_hidden_region_action_guards() {
    let mut d = workspace(|_| {});
    d.with_state(|state| {
        citadel_studio::state::apply(state, Action::SetGridTab(GridTab::Structure));
    });

    let commands = citadel_studio::state::commands(d.state());
    assert!(
        commands
            .iter()
            .all(|command| command.title != "Verify visible rows"),
        "the palette offered row verification while Structure hid every row"
    );
    assert!(
        commands
            .iter()
            .all(|command| command.title != "Forget the selected atom"),
        "the palette offered erasure for a row hidden by Structure"
    );
    d.apply(Action::BeginForgetSelected);
    assert!(
        d.state().forget_prompt.is_none(),
        "a direct action bypassed the hidden-row erasure guard"
    );

    d.with_state(|state| {
        citadel_studio::state::apply(state, Action::OpenDoc(Doc::Table("documents".to_owned())));
    });
    let commands = citadel_studio::state::commands(d.state());
    assert!(
        commands
            .iter()
            .all(|command| command.title != "Verify visible rows")
            && commands
                .iter()
                .all(|command| command.title != "Forget the selected atom"),
        "region-only actions leaked onto a table document"
    );
}

/// Forgetting a sealed atom destroys its key and surfaces the engine receipt.
#[test]
fn forget_issues_a_receipt() {
    let mut d = workspace(|s| s.selected_row = 0);
    // Receipts are session-issued; the engine keeps no receipt log.
    assert!(d.state().vault().receipts.is_empty());
    let doomed = d.state().visible_atoms().unwrap()[0].id;
    let before = d.state().visible_region().unwrap().facts.total.unwrap();

    d.click_id(egui::Id::new(("btn", "Forget atom")));
    assert!(d.state().forget_prompt.is_some());
    assert!(
        d.state().vault().receipts.is_empty(),
        "opening confirmation performed the irreversible action"
    );
    d.shot("20-forget-confirmation");
    d.click_id(egui::Id::new(("btn", "Confirm forget")));
    d.until("the receipt to arrive", |s| !s.vault().receipts.is_empty());
    d.shot("20-after-forget");

    let s = d.state();
    let receipt = &s.vault().receipts[0];
    assert!(
        receipt.receipt.cryptographic_erasure,
        "a sealed region destroys a key, and the receipt has to claim exactly that"
    );
    assert_eq!(receipt.atoms(), 1);
    assert!(receipt.receipt.readback_confirmed);
    assert_eq!(
        receipt.readback(),
        citadel_studio::model::Readback::Confirmed
    );
    // The deleted row leaves the visible window.
    assert!(s.visible_atoms().unwrap().iter().all(|a| a.id != doomed));
    assert_eq!(s.visible_region().unwrap().facts.total, Some(before - 1));
}

#[test]
fn escape_cancels_forget_confirmation_without_touching_the_row() {
    let mut d = workspace(|s| s.selected_row = 0);
    let before = d.state().visible_atoms().unwrap()[0].id;
    d.click_id(egui::Id::new(("btn", "Forget atom")));
    assert!(d.state().forget_prompt.is_some());

    d.key(egui::Key::Escape);
    assert!(d.state().forget_prompt.is_none());
    assert!(d.state().vault().receipts.is_empty());
    assert_eq!(d.state().visible_atoms().unwrap()[0].id, before);
}

#[test]
fn forget_confirmation_traps_keyboard_focus_inside_the_modal() {
    let mut d = workspace(|s| s.selected_row = 0);
    d.click_id(egui::Id::new(("btn", "Forget atom")));

    let cancel = egui::Id::new(("btn", "Cancel"));
    let confirm = egui::Id::new(("btn", "Confirm forget"));
    let focused = d.harness.ctx.memory(|memory| memory.focused());
    assert_eq!(
        focused,
        Some(cancel),
        "the modal did not claim initial focus"
    );

    for _ in 0..6 {
        d.key(egui::Key::Tab);
        let focused = d.harness.ctx.memory(|memory| memory.focused());
        assert!(
            focused == Some(cancel) || focused == Some(confirm),
            "Tab escaped the irreversible-action modal to {focused:?}"
        );
    }
}

#[test]
fn verification_progress_and_failures_are_visible_in_the_region() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|_| {});
    d.apply(Action::EngineBusy(Busy::Verifying("episodic".to_owned())));
    assert!(
        d.harness
            .query_by_label_contains("Authenticating visible rows")
            .is_some(),
        "verification entered the worker without a visible working state"
    );

    d.with_state(|state| {
        citadel_studio::state::apply(state, Action::OpenDoc(Doc::Region("scratch".to_owned())));
    });
    assert!(
        d.harness
            .query_by_label_contains("Authenticating visible rows")
            .is_none(),
        "region A's progress banner leaked onto region B"
    );

    d.apply(Action::PageVerified {
        region: "episodic".to_owned(),
        asked: vec![1],
        verdicts: Box::new(Err(citadel_studio::engine::StudioError::new(
            citadel_studio::engine::Kind::Io,
            "verification probe failed",
        ))),
    });
    assert!(
        d.harness
            .query_by_label_contains("Maintenance failed: The file could not be read or written")
            .is_none(),
        "region A's error banner leaked onto region B"
    );

    d.with_state(|state| {
        citadel_studio::state::apply(state, Action::OpenDoc(Doc::Region("episodic".to_owned())));
    });
    assert!(
        d.harness
            .query_by_label_contains("Maintenance failed: The file could not be read or written")
            .is_some(),
        "the engine error was not shown again on its owning region"
    );
}

/// A plaintext region has no per-atom key, and its receipt refuses to claim one.
#[test]
fn forgetting_plaintext_does_not_claim_crypto_erasure() {
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Region("scratch".into())));
        s.selected_row = 0;
    });
    d.apply(Action::BeginForgetSelected);
    d.apply(Action::ConfirmForget);
    d.until("the receipt to arrive", |s| !s.vault().receipts.is_empty());

    let s = d.state();
    let receipt = &s.vault().receipts[0];
    assert!(
        !receipt.receipt.cryptographic_erasure,
        "there was no key here to destroy"
    );
    assert_eq!(receipt.receipt.rows_deleted, 1);
    assert!(
        receipt.receipt.algorithm.is_empty(),
        "naming a key-wrap algorithm would claim a key that never existed"
    );
    assert_eq!(receipt.algorithm(), "row delete, no key");
    assert_eq!(
        receipt.readback(),
        citadel_studio::model::Readback::NotApplicable
    );
}

/// Closing the last document returns Home.
#[test]
fn closing_the_last_document_returns_home() {
    let mut d = workspace(|_| {});
    assert_eq!(d.state().docs.len(), 1);
    d.apply(Action::CloseDoc(0));
    let s = d.state();
    assert!(s.docs.is_empty());
    assert_eq!(s.route, Route::Home);
}

/// The wrong passphrase is reported as a wrong passphrase, never as a corrupt database.
#[test]
fn wrong_passphrase_is_rejected_not_blamed_on_the_file() {
    use citadel_studio::engine::session::{CreateSpec, Session};
    use citadel_studio::engine::Kind;

    // Use a real encrypted vault; the demo has no passphrase rejection path.
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.cdl");
    let spec = CreateSpec {
        path: path.clone(),
        passphrase: "the-real-passphrase".to_owned().into(),
        kdf: citadel_studio::state::KdfAlgorithm::Argon2id,
        profile: citadel_studio::state::Argon2Profile::Iot,
    };
    // Release the exclusive database lock before testing the passphrase.
    drop(Session::create(&spec).expect("the vault is created"));

    let unlocking = |path: std::path::PathBuf| {
        move |s: &mut State| {
            s.route = Route::Unlock;
            s.target = Target::Picked(path);
        }
    };

    let mut d = Driver::new(unlocking(path.clone()));
    d.type_text("nope").key(egui::Key::Enter);
    assert_eq!(d.state().unlock, UnlockStage::Rejected);
    assert!(d.state().vault.is_none());
    // Pin the specific passphrase category rather than any generic open failure.
    assert_eq!(
        d.state().session.error().map(|e| e.kind),
        Some(Kind::Passphrase),
        "a wrong passphrase must never be reported as a damaged file"
    );
    d.shot("21-unlock-rejected");

    // The correct passphrase still opens it.
    let mut d = Driver::new(unlocking(path));
    d.type_text(&spec.passphrase).key(egui::Key::Enter);
    assert!(d.state().vault.is_some());
    assert_eq!(d.state().route, Route::Workspace);
}

/// Home reports a vault-open failure without changing routes.
#[test]
fn a_failed_open_is_reported_on_home() {
    use egui_kittest::kittest::Queryable;

    let mut d = Driver::new(|s: &mut State| {
        s.route = Route::Home;
    });
    d.apply(Action::VaultOpened(Box::new(Err(
        citadel_studio::engine::StudioError::new(
            citadel_studio::engine::Kind::Io,
            "the test vault could not be opened",
        ),
    ))));
    d.settle();

    let reported = d
        .state()
        .session
        .error()
        .expect("the engine refused")
        .to_string();
    assert!(
        d.harness.query_by_label(&reported).is_some(),
        "Home did not report the failure it was told about: {reported:?}"
    );
    assert!(d.state().vault.is_none(), "nothing should have opened");
}

/// The Unlock button opens a chosen vault independently of the Enter-key binding.
#[test]
fn the_unlock_button_opens_a_chosen_vault() {
    use citadel_studio::engine::session::{CreateSpec, Session};

    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("clicked.cdl");
    let spec = CreateSpec {
        path: path.clone(),
        passphrase: "the-real-passphrase".to_owned().into(),
        kdf: citadel_studio::state::KdfAlgorithm::Argon2id,
        profile: citadel_studio::state::Argon2Profile::Iot,
    };
    drop(Session::create(&spec).expect("the vault is created"));

    let mut d = Driver::new(move |s: &mut State| {
        s.route = Route::Unlock;
        s.target = Target::Picked(path.clone());
    });
    d.type_text(&spec.passphrase);
    d.click_id(egui::Id::new(("btn", "Unlock")));
    assert!(
        d.state().vault.is_some(),
        "the Unlock button did not open it"
    );
    assert_eq!(d.state().route, Route::Workspace);
}

/// The Create button writes and opens a vault.
#[test]
fn the_create_button_writes_a_vault() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("made.cdl");

    let mut d = Driver::new(move |s: &mut State| {
        s.route = Route::Create;
        s.new_vault.path = Some(path.clone());
        s.new_vault.passphrase = "a-long-enough-passphrase".to_owned().into();
        s.new_vault.confirm = "a-long-enough-passphrase".to_owned().into();
        // Desktop is deliberately expensive and this is a test, not a vault anyone keeps.
        s.new_vault.profile = citadel_studio::state::Argon2Profile::Iot;
    });
    d.click_id(egui::Id::new(("btn", "Create vault")));

    let s = d.state();
    assert!(s.vault.is_some(), "the Create button did not write a vault");
    assert_eq!(s.route, Route::Workspace);
}

/// An incomplete form keeps Create unavailable.
#[test]
fn the_create_button_stays_shut_until_the_form_is_complete() {
    let mut d = Driver::new(|s: &mut State| {
        s.route = Route::Create;
        s.new_vault.passphrase = "short".to_owned().into();
    });
    d.click_id(egui::Id::new(("btn", "Create vault")));
    assert!(d.state().vault.is_none());
    assert_eq!(
        d.state().route,
        Route::Create,
        "the click must not navigate"
    );
}

/// An uninspected header claims nothing, while an inspected invalid file preserves the
/// refusal reason instead of reverting to "not read".
#[test]
fn a_refused_file_says_so_instead_of_claiming_nobody_looked() {
    use citadel_studio::effects::Ask;
    use citadel_studio::state::Preview;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let impostor = dir.path().join("not-a-vault.cdl");
    std::fs::write(
        &impostor,
        b"plain text, and certainly not an encrypted vault",
    )
    .expect("write");

    // Drive the real picker and header reader.
    let mut d = Driver::picking(move |ask| match ask {
        Ask::ExistingVault => Some(impostor.clone()),
        _ => None,
    });
    d.apply(Action::ChooseVaultToOpen);

    let why = match &d.state().preview {
        Preview::Refused(why) => why.clone(),
        other => panic!("a file that is not a vault should be refused, got {other:?}"),
    };
    assert!(
        !why.is_empty(),
        "the refusal must carry the engine's reason"
    );

    // One row must preserve the refusal reason.
    let info = citadel_studio::model::preview_rows(&d.state().preview);
    assert!(
        info.iter().any(|(_, v)| v == &why),
        "the engine's reason is not on any row: {info:?}"
    );
    assert!(
        !info.iter().any(|(_, v)| v == "not read"),
        "a file that was read reported itself unread: {info:?}"
    );
}

#[test]
fn an_unopened_header_claims_nothing() {
    let d = Driver::new(|_| {});
    assert_eq!(
        d.state().preview,
        citadel_studio::state::Preview::Unread,
        "nothing has been read, and that is now the absence of facts rather than ten \
         fields all saying so"
    );
    assert!(
        d.state().recent.is_empty(),
        "a default state has opened nothing, so it has no recent vaults"
    );
}

#[test]
fn application_menu_opens_versioned_about_information() {
    use egui_kittest::kittest::Queryable;

    let mut d = Driver::sized(900.0, 540.0, |_| {});
    for label in ["File", "View", "Help"] {
        assert!(
            d.harness.query_by_label(label).is_some(),
            "the application menu is missing {label:?}"
        );
    }

    let file = d
        .harness
        .query_by_label("File")
        .expect("the File menu is missing")
        .rect()
        .center();
    d.click(file.x, file.y);
    let import_label = format!(
        "Import SQLite schema…\t{}",
        citadel_studio::state::shortcut("I")
    );
    assert!(
        d.harness.query_by_label(&import_label).is_some(),
        "the File menu does not disclose the working import shortcut"
    );
    d.click(file.x, file.y);

    let help = d
        .harness
        .query_by_label("Help")
        .expect("the Help menu is missing")
        .rect()
        .center();
    d.click(help.x, help.y);
    let about = d
        .harness
        .query_by_label("About Citadel Studio")
        .expect("the Help menu has no About command")
        .rect()
        .center();
    d.click(about.x, about.y);

    d.harness
        .get_by_role_and_label(egui::accesskit::Role::Dialog, "About Citadel Studio");
    assert!(d.state().about_open);
    assert!(
        d.harness
            .get_by_role_and_label(egui::accesskit::Role::Button, "Close")
            .is_focused(),
        "About left keyboard focus behind the modal"
    );
    for text in [
        concat!("Version ", env!("CARGO_PKG_VERSION")),
        "Inspect, query, verify, and maintain encrypted vaults locally.",
        "Copyright © 2026 Yuriy Peysakhov",
        env!("CARGO_PKG_LICENSE"),
    ] {
        assert!(
            d.harness.query_by_label(text).is_some(),
            "the About dialog is missing {text:?}"
        );
    }
    assert!(
        d.harness.query_by_label_contains("CitadelDB").is_none(),
        "About uses the engine name as competing product branding"
    );
    assert!(
        d.harness.query_by_label("Third-party fonts").is_none(),
        "font license inventory belongs in bundled notices, not the main About dialog"
    );
    let close = d
        .harness
        .get_by_role_and_label(egui::accesskit::Role::Button, "Close");
    assert!(
        close.rect().center().x > d.size.x * 0.5,
        "the dialog action is not aligned to the trailing edge"
    );
    d.shot("60-about");
}

#[test]
fn an_open_vault_can_reach_import_and_cancel_back_to_the_same_workspace() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|_| {});
    let file = d
        .harness
        .query_by_label("File")
        .expect("the File menu is missing")
        .rect()
        .center();
    d.click(file.x, file.y);
    let import = d
        .harness
        .query_by_label("Import SQLite schema…")
        .expect("an open vault cannot reach destination import")
        .rect()
        .center();
    d.click(import.x, import.y);

    assert_eq!(d.state().route, Route::Import);
    assert!(
        d.state().vault.is_some(),
        "entering import closed the vault"
    );

    d.key(egui::Key::Escape);
    assert_eq!(d.state().route, Route::Workspace);
    assert!(
        d.state().vault.is_some(),
        "cancelling import closed the vault"
    );
}

#[test]
fn application_menu_cannot_abandon_a_transient_workflow() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|state| {
        citadel_studio::state::apply(state, Action::BeginImport);
        state.source = Source::Reading {
            path: "source.sqlite".into(),
        };
    });

    let view = d
        .harness
        .query_by_label("View")
        .expect("the View menu is missing")
        .rect()
        .center();
    d.click(view.x, view.y);
    let query = d
        .harness
        .query_by_label_contains("Query editor")
        .expect("the Query editor command is missing")
        .rect()
        .center();
    d.click(query.x, query.y);
    assert_eq!(
        d.state().route,
        Route::Import,
        "a disabled View command abandoned the import workflow"
    );
    assert!(
        matches!(d.state().source, Source::Reading { .. }),
        "the in-flight import source was discarded"
    );

    let file = d
        .harness
        .query_by_label("File")
        .expect("the File menu is missing")
        .rect()
        .center();
    d.click(file.x, file.y);
    let close_label = format!(
        "Close vault\t{}",
        citadel_studio::state::shortcut("Shift W")
    );
    let close = d
        .harness
        .query_by_label(&close_label)
        .expect("the Close vault command is missing")
        .rect()
        .center();
    d.click(close.x, close.y);
    assert!(
        d.state().vault.is_some(),
        "a disabled File command closed the vault during an import"
    );
    assert_eq!(d.state().route, Route::Import);
}

#[test]
fn every_recent_vault_is_keyboard_reachable_at_the_minimum_window_size() {
    use citadel_studio::model::RecentVault;
    use egui_kittest::kittest::Queryable;

    let mut d = Driver::sized(900.0, 540.0, |state| {
        state.route = Route::Home;
        state.recent = (0..8)
            .map(|i| RecentVault {
                path: std::path::PathBuf::from(format!("vault-{i}.cdl")),
                entries: 0,
                pages: i + 1,
            })
            .collect();
    });
    let last = d
        .harness
        .query_by_label("vault-7.cdl, 8 pages, not attested")
        .expect("the last recent vault is missing from the accessibility tree");
    last.focus();
    d.harness.run();
    d.harness.run();

    let focused = d
        .harness
        .query_by_label("vault-7.cdl, 8 pages, not attested")
        .expect("the focused recent vault disappeared");
    assert!(
        focused.is_focused(),
        "the last recent vault did not take focus"
    );
    assert!(
        focused.rect().bottom() <= d.size.y - 26.0,
        "the scroll area did not bring the focused last recent vault above the status bar: {:?}",
        focused.rect()
    );
}

/// The grid window follows the selection, so a keyboard move never scrolls the selected
/// row off screen.
#[test]
fn the_window_follows_the_selection() {
    let mut d = sized_workspace(1100.0, 700.0, |s| {
        s.selected_row = 0;
    });
    assert_eq!(d.state().scroll_row, 0);
    d.key(egui::Key::End);
    let s = d.state();
    let last = s.visible_atoms().unwrap().len() - 1;
    assert_eq!(s.selected_row, last);
    assert!(
        s.scroll_row > 0,
        "selection moved to row {last} but the window never scrolled"
    );
    assert!(
        s.selected_row >= s.scroll_row,
        "selected row is above the window"
    );
    d.shot("24-scrolled-to-end");
}

/// Representative custom controls expose names in the real AccessKit tree.
#[test]
fn controls_are_named_in_the_accessibility_tree() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|s| s.selected_row = 1);
    d.harness.run();

    for label in [
        "Verify rows",
        "Forget atom",
        "Close region: episodic",
        "Data",
        "Attestation",
    ] {
        assert!(
            d.harness.query_by_label(label).is_some(),
            "no accessible node named {label:?}"
        );
    }
    for duplicate in ["Forget row", "Forget this atom"] {
        assert!(
            d.harness.query_by_label(duplicate).is_none(),
            "obsolete duplicate action {duplicate:?} is still exposed"
        );
    }
    for evidence in [
        "[+] Authentic",
        "[ ] Not checked",
        "[/] Key erased",
        "[!] Tampered",
        "[?] Missing",
        "[-] Not attestable",
    ] {
        assert!(
            d.harness.query_by_label_contains(evidence).is_some(),
            "the evidence legend does not explain {evidence:?}"
        );
    }

    // Build the accessible metadata name from engine facts and the shared separator.
    let region = d
        .state()
        .vault()
        .region("episodic")
        .expect("the demo seeds episodic")
        .facts
        .clone();
    let expected = format!(
        "episodic, {}d{}{}",
        region.dim,
        citadel_studio::widgets::SEP,
        region.total.expect("the demo region is readable")
    );
    assert!(
        d.harness.query_by_label(&expected).is_some(),
        "sidebar row lost its metadata in the accessible name, wanted {expected:?}"
    );
}

#[test]
fn a_narrow_region_keeps_one_forget_action_when_the_inspector_is_hidden() {
    use egui_kittest::kittest::Queryable;

    let d = sized_workspace(900.0, 540.0, |state| state.selected_row = 0);
    assert!(
        d.harness.query_by_label("Forget atom").is_some(),
        "the toolbar fallback disappeared with the responsive inspector"
    );
    assert!(
        d.harness.query_by_label("Forget row").is_none()
            && d.harness.query_by_label("Forget this atom").is_none(),
        "the narrow layout exposed inconsistent forget labels"
    );
}

#[test]
fn a_short_region_hides_the_inspector_before_it_can_overlap_the_status_bar() {
    use egui_kittest::kittest::Queryable;

    let d = sized_workspace(1100.0, 540.0, |state| state.selected_row = 0);
    assert!(
        d.harness.query_by_label("Forget atom").is_some(),
        "the toolbar fallback disappeared with the short inspector"
    );
    assert!(
        d.harness.query_by_label_contains("Authentic").is_none(),
        "the short layout kept an evidence legend that cannot fit above the status bar"
    );
}

fn assert_accessibility_tree_has_no_anonymous_controls(d: &Driver, screen: &str) {
    use egui_kittest::kittest::NodeT;

    let mut unknown = Vec::new();
    let mut unnamed_focusable = Vec::new();
    let root = d.harness.root();
    for node in std::iter::once(root).chain(root.children_recursive()) {
        let accesskit = node.accesskit_node();
        if accesskit.is_hidden() {
            continue;
        }
        if accesskit.role() == egui::accesskit::Role::Unknown {
            unknown.push(format!("{node:?}"));
        }
        if accesskit
            .data()
            .supports_action(egui::accesskit::Action::Focus)
            && accesskit
                .label()
                .is_none_or(|label| label.trim().is_empty())
        {
            unnamed_focusable.push(format!("{node:?}"));
        }
    }

    assert!(
        unknown.is_empty(),
        "{screen} exposes visible Role::Unknown nodes:\n{}",
        unknown.join("\n")
    );
    assert!(
        unnamed_focusable.is_empty(),
        "{screen} exposes unnamed focusable controls:\n{}",
        unnamed_focusable.join("\n")
    );
}

/// Rejects unnamed focusable controls and unknown roles across representative trees.
#[test]
fn representative_screens_have_no_anonymous_accessible_controls() {
    use egui_kittest::kittest::NodeT;

    let home = Driver::new(|s| s.route = Route::Home);
    assert_accessibility_tree_has_no_anonymous_controls(&home, "Home");

    let mut workspace = workspace(|s| s.selected_row = 0);
    assert_accessibility_tree_has_no_anonymous_controls(&workspace, "workspace data view");

    workspace.apply(Action::OpenDoc(Doc::Vector("documents.embedding".into())));
    assert_accessibility_tree_has_no_anonymous_controls(&workspace, "vector view");

    use egui_kittest::kittest::Queryable;
    let canvas = workspace
        .harness
        .query_by_role(egui::accesskit::Role::Canvas)
        .expect("the projection must be exposed as an accessible canvas");
    assert!(
        !canvas
            .accesskit_node()
            .data()
            .supports_action(egui::accesskit::Action::Focus),
        "the pointer-only projection must not create an unusable Tab stop"
    );
}

/// A grid row's accessible name carries its position, cells, and evidence.
#[test]
fn grid_rows_read_as_whole_sentences() {
    use egui_kittest::kittest::{NodeT, Queryable};

    let mut d = workspace(|s| s.selected_row = 0);
    d.harness.run();

    let (row, total, evidence) = {
        let s = d.state();
        let atoms = s.visible_atoms().expect("a region is focused");
        let i = atoms
            .iter()
            .position(|a| a.text.contains("staging credentials were rotated"))
            .expect("the demo seeds that turn");
        (i + 1, atoms.len(), atoms[i].evidence())
    };

    let node = d
        .harness
        .query_by_label_contains("staging credentials were rotated")
        .expect("the row is not in the accessibility tree");
    let accesskit = node.accesskit_node();
    let name = accesskit.label().unwrap_or_default();

    assert!(
        name.starts_with(&format!("Row {row} of {total}")),
        "row name lost its position: {name:?}"
    );
    assert!(
        name.contains("content,"),
        "row name lost its column headers: {name:?}"
    );
    // The sentence ends with a lower-case rendering of the engine's actual verdict.
    assert!(
        name.trim_end().ends_with(&evidence.label().to_lowercase()),
        "row name must end with the evidence state, got {name:?}"
    );
}

/// Tab reaches the controls and Space presses the focused one.
#[test]
fn tab_reaches_controls_and_space_presses_them() {
    let mut d = workspace(|s| s.selected_row = 0);
    let verify = egui::Id::new(("btn", "Verify visible rows"));

    // Stop after one focus cycle rather than relying on a fixed control count.
    let mut seen = std::collections::HashSet::new();
    let mut reached = false;
    loop {
        d.key(egui::Key::Tab);
        let focused = d.harness.ctx.memory(|m| m.focused());
        if focused == Some(verify) {
            reached = true;
            break;
        }
        match focused {
            Some(id) if seen.insert(id) => continue,
            // A full cycle with no verification button, or nothing focusable at all.
            _ => break,
        }
    }
    assert!(
        reached,
        "Tab cycled through {} controls without reaching Verify rows",
        seen.len()
    );

    d.shot("26-focus-ring");
    assert_eq!(d.state().page_checked.scope, 0, "nothing checked yet");

    // Space activates the focused control.
    d.key(egui::Key::Space);
    let s = d.state();
    let covered = s.visible_atoms().unwrap().len().min(s.visible_rows);
    assert_eq!(
        s.page_checked.scope as usize, covered,
        "Space did not activate the focused button"
    );
    // A sealed region makes every covered atom attestable.
    assert!(s.visible_region().is_some_and(|r| !r.plaintext()));
    assert_eq!(
        s.page_checked.authentic as usize, covered,
        "verify proved a different number of atoms than it covered"
    );
}

/// The palette exposes commands absent from toolbars.
#[test]
fn command_palette() {
    let mut d = workspace(|_| {});
    d.key_mod(egui::Key::K, egui::Modifiers::COMMAND);
    assert!(d.state().palette_open, "Ctrl+K did not open the palette");
    d.shot("27-palette");

    // Subsequence match, so a few letters reach a buried command.
    d.type_text("scr");
    let list = citadel_studio::state::commands(d.state());
    assert!(
        list.iter().any(|c| c.title.contains("scratch")),
        "filter dropped the scratch region"
    );
    d.shot("28-palette-filtered");

    d.key(egui::Key::Enter);
    let s = d.state();
    assert!(!s.palette_open, "Enter left the palette open");
    assert!(
        s.docs.contains(&Doc::Region("scratch".into())),
        "running the command did not open the region"
    );
}

/// Escape closes it without running anything.
#[test]
fn palette_escape_runs_nothing() {
    let mut d = workspace(|_| {});
    let before = d.state().docs.len();
    d.key_mod(egui::Key::K, egui::Modifiers::COMMAND);
    d.key(egui::Key::Escape);
    let s = d.state();
    assert!(!s.palette_open);
    assert_eq!(s.docs.len(), before);
}

/// Security is a per-vault singleton pinned to the right of the strip.
#[test]
fn security_is_a_singleton_pinned_right() {
    let mut d = workspace(|_| {});
    for _ in 0..3 {
        d.apply(Action::OpenDoc(Doc::Security));
    }
    let s = d.state();
    assert_eq!(s.docs.iter().filter(|d| **d == Doc::Security).count(), 1);
    assert!(s.docs.last().unwrap().pinned_right());
}

/// Even below the requested minimum, screens must avoid negative geometry and drawing
/// outside the window.
#[test]
fn every_screen_survives_below_the_declared_minimum() {
    for (name, open) in [
        ("home", None),
        ("unlock", Some(Route::Unlock)),
        ("create", Some(Route::Create)),
        ("import", Some(Route::Import)),
        ("workspace", None),
    ] {
        let workspace = name == "workspace";
        // Import carries a read source, so the case laid out is the grid and not the
        // one-line notice. A grid given a rect shorter than its header builds a
        // negative-height widget.
        let source = (name == "import").then(|| citadel_studio::state::Source::Read {
            path: std::path::PathBuf::from("/tmp/tiny.sqlite"),
            tables: vec![citadel_studio::sqlite::SourceTable {
                name: "notes".into(),
                rows: 3,
                columns: vec![citadel_studio::sqlite::Column {
                    name: "id".into(),
                    declared: "INTEGER".into(),
                    primary_key: true,
                }],
            }],
        });
        let mut d = Driver::sized(340.0, 270.0, |_| {});
        if workspace {
            d.open_demo();
        }
        d.with_state(|s| {
            if let Some(source) = source {
                s.source = source;
            }
            if let Some(route) = open {
                s.route = route;
            }
        });
        d.shot(&format!("35-tiny-{name}"));

        let window = egui::Rect::from_min_size(egui::Pos2::ZERO, d.size);
        let strays: Vec<_> = d
            .harness
            .ctx
            .interactive_rects_last_pass()
            .into_iter()
            .filter(|r| {
                r.width() < 0.0 || r.height() < 0.0 || !window.expand(1.0).contains_rect(*r)
            })
            .collect();
        assert!(
            strays.is_empty(),
            "{name} at 340x270 laid out {} widget(s) outside the window or at a negative \
             size: {:?}",
            strays.len(),
            &strays[..strays.len().min(3)]
        );
    }
}

/// Closing a tab clamps selection to the newly focused document.
#[test]
fn closing_a_tab_does_not_strand_the_selection() {
    let mut d = sized_workspace(1100.0, 700.0, |s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Region("scratch".into())));
    });
    // Select the last row in the long document.
    d.apply(Action::FocusDoc(0));
    d.apply(Action::SelectLast);
    assert!(
        d.state().selected_row > 5,
        "episodic should be longer than scratch"
    );

    // Focus lands on the shorter scratch region.
    d.apply(Action::CloseDoc(0));
    d.shot("33-after-close");

    let s = d.state();
    let len = s.visible_atoms().unwrap().len();
    assert!(
        s.selected_row < len,
        "selection {} is past the end of the {len}-row document now on screen",
        s.selected_row
    );
    assert_eq!(s.scroll_row, 0, "the new document opened already scrolled");
}

/// Opening another vault clears the previous vault's session evidence.
#[test]
fn a_fresh_vault_starts_with_no_proof_claimed() {
    let mut d = workspace(|_| {});
    d.apply(Action::VerifyPage);
    assert!(
        d.state().page_checked.scope > 0,
        "the fixture should have verified something"
    );

    d.apply(Action::CloseVault);
    d.apply(Action::OpenDemoVault);

    let s = d.state();
    assert_eq!(
        s.page_checked,
        citadel_studio::state::PageCheck::default(),
        "the new vault inherited a check that never ran on it"
    );
    assert_eq!(s.page_check_label(), "no visible rows verified");
}

/// A modal owns wheel input as well as keyboard input.
#[test]
fn the_palette_stops_the_grid_scrolling_behind_it() {
    let mut d = sized_workspace(1100.0, 700.0, |state| {
        citadel_studio::state::apply(state, Action::OpenDoc(Doc::Table("documents".to_owned())));
    });
    d.until("the table browser to load a scrollable result", |state| {
        state
            .browse
            .as_ref()
            .and_then(|browse| browse.rows.as_ref().ok())
            .is_some_and(|result| result.rows.len() > state.visible_rows)
    });
    let over_grid = d.rect_of(egui::Id::new(("row", 0))).center();
    d.scroll_at(over_grid, -200.0);
    let moved = d.state().scroll_row;
    assert!(moved > 0, "the grid did not scroll to begin with");

    d.apply(Action::TogglePalette);
    d.scroll_at(over_grid, -200.0);
    assert_eq!(
        d.state().scroll_row,
        moved,
        "the grid scrolled behind the open palette"
    );
}

/// Security scrolls only when its content exceeds the available height.
#[test]
fn the_security_column_scrolls_when_it_does_not_fit() {
    use egui_kittest::kittest::Queryable;

    // Create receipts because fresh vaults intentionally have none.
    let mut small = sized_workspace(900.0, 540.0, |_| {});
    for _ in 0..3 {
        small.apply(Action::BeginForgetSelected);
        small.apply(Action::ConfirmForget);
    }
    assert_eq!(
        small.state().vault().receipts.len(),
        3,
        "three erasures should have issued three receipts"
    );
    small.apply(Action::OpenDoc(Doc::Security));
    small.shot("31-security-min");
    let before = small.harness.render().expect("render failed");

    small.scroll(-600.0);
    assert!(
        small.state().pane_scroll > 0.0,
        "the column did not scroll at the minimum window size, so its lower cards are \
         unreachable"
    );
    small.shot("32-security-scrolled");
    let after = small.harness.render().expect("render failed");
    assert!(
        before.pixels().zip(after.pixels()).any(|(a, b)| a != b),
        "pane_scroll moved but nothing on screen did"
    );

    let scrollbar = small
        .harness
        .get_by_role_and_label(egui::accesskit::Role::ScrollBar, "Vault security content");
    scrollbar.focus();
    small.harness.run();
    assert!(
        small
            .harness
            .get_by_role_and_label(egui::accesskit::Role::ScrollBar, "Vault security content")
            .is_focused(),
        "the security scrollbar did not accept accessibility focus"
    );
    small.key(egui::Key::End);
    assert!(
        small.state().pane_scroll > 0.0,
        "End on the focused security scrollbar did not reach the lower cards"
    );
    assert!(
        small
            .harness
            .get_by_role_and_label(egui::accesskit::Role::ScrollBar, "Vault security content")
            .is_focused(),
        "scrolling the security pane dropped scrollbar focus"
    );
    small.key(egui::Key::Home);
    assert_eq!(
        small.state().pane_scroll,
        0.0,
        "Home on the focused security scrollbar did not return to the first card"
    );

    // A taller viewport needs no scroll range.
    let mut large = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Security));
    });
    large.scroll(-600.0);
    assert_eq!(
        large.state().pane_scroll,
        0.0,
        "the column scrolled at 1600x1000 where it already fits"
    );
}

/// Flowing tabs never overlap the pinned Security tab.
#[test]
fn tabs_never_run_under_the_pinned_tab() {
    let mut d = sized_workspace(1100.0, 700.0, |_| {});
    for n in 0..8 {
        d.apply(Action::OpenDoc(Doc::Region(format!("region-{n}"))));
    }
    d.apply(Action::OpenDoc(Doc::Security));
    d.shot("29-many-tabs");

    let titles: Vec<String> = d.state().docs.iter().map(|doc| doc.title()).collect();
    let rects: Vec<(String, egui::Rect)> = titles
        .iter()
        .filter_map(|t| {
            d.harness
                .ctx
                .read_response(egui::Id::new(("tab", t.as_str())))
                .map(|r| (t.clone(), r.rect))
        })
        .collect();

    let pinned = rects
        .iter()
        .find(|(t, _)| t == "Security")
        .expect("the pinned tab must always be laid out");
    assert!(
        (pinned.1.right() - 1100.0).abs() < 1.0,
        "Security is not pinned to the right edge: {:?}",
        pinned.1
    );
    // Shared edges are valid; only positive-area intersection is a defect.
    for (a, ra) in &rects {
        for (b, rb) in &rects {
            if a != b {
                let over = ra.right().min(rb.right()) - ra.left().max(rb.left());
                assert!(over <= 0.0, "tabs {a} and {b} overlap by {over}pt");
            }
        }
    }
    // A focused flowing tab beyond the visible run must scroll into view.
    let last = d
        .state()
        .docs
        .iter()
        .rposition(|doc| !doc.pinned_right())
        .expect("the fixture opened flowing documents");
    assert!(
        last >= rects.len(),
        "every flowing tab already fits, so this test is not exercising the scroll"
    );
    d.apply(Action::FocusDoc(last));
    let focused = d.state().docs[last].title();
    let on_screen = d
        .harness
        .ctx
        .read_response(egui::Id::new(("tab", focused.as_str())))
        .is_some();
    assert!(
        on_screen,
        "focusing {focused} left it scrolled off the strip"
    );
}

/// Builds a real SQLite source for import tests.
fn sqlite_fixture(name: &str, build: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("create fixture directory");
    let path = dir.path().join(format!("{name}.sqlite"));
    let db = rusqlite::Connection::open(&path).expect("create fixture");
    db.execute_batch(build).expect("build fixture");
    (dir, path)
}

/// Exercises the injected dialog, reader thread, channel, and resulting import view.
#[test]
fn choosing_a_source_reads_it_on_a_worker_and_shows_the_result() {
    let (_source_dir, path) = sqlite_fixture(
        "worker",
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
         INSERT INTO notes (body) VALUES ('a'), ('b');",
    );

    let answer = path.clone();
    let mut d = Driver::picking(move |ask| {
        assert_eq!(ask, Ask::SqliteSource, "the wrong dialog was raised");
        Some(answer.clone())
    });
    d.click_id(egui::Id::new(("home_row", "import_sqlite")));
    assert_eq!(d.state().route, Route::Import);

    d.click_id_working(egui::Id::new(("btn", "Choose")));
    // The screen remains drawable while the reader runs off-thread.
    d.until("the reader to finish", |s| {
        matches!(s.source, citadel_studio::state::Source::Read { .. })
    });

    let tables = d.state().source.tables().to_vec();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "notes");
    assert_eq!(tables[0].rows, 2);
    assert_eq!(d.state().source.path(), Some(path.as_path()));
}

/// A dismissed dialog changes nothing: no route change, no half-entered form.
#[test]
fn a_dismissed_dialog_leaves_the_screen_alone() {
    let mut d = Driver::picking(|_| None);

    d.click_id(egui::Id::new(("home_card", "Open vault")));
    assert_eq!(
        d.state().route,
        Route::Home,
        "dismissing the open dialog still left Home"
    );

    d.click_id(egui::Id::new(("home_card", "Create vault")));
    assert_eq!(d.state().route, Route::Create);
    d.click_id(egui::Id::new(("btn", "Choose")));
    assert_eq!(
        d.state().new_vault.path,
        None,
        "dismissing the save dialog set a destination anyway"
    );

    d.key(egui::Key::Escape);
    d.click_id(egui::Id::new(("home_row", "import_sqlite")));
    d.click_id(egui::Id::new(("btn", "Choose")));
    assert_eq!(
        d.state().source,
        citadel_studio::state::Source::None,
        "dismissing the import dialog started a read anyway"
    );
}

/// The destination dialog reaches the create form through the same seam.
#[test]
fn choosing_a_destination_fills_the_create_form() {
    let want = std::env::temp_dir().join("citadel-studio-chosen.cdl");
    let answer = want.clone();
    let mut d = Driver::picking(move |ask| {
        assert_eq!(ask, Ask::VaultDestination);
        Some(answer.clone())
    });
    d.click_id(egui::Id::new(("home_card", "Create vault")));
    d.click_id(egui::Id::new(("btn", "Choose")));
    assert_eq!(d.state().new_vault.path.as_deref(), Some(want.as_path()));
    assert_eq!(
        d.state().new_vault.blocker(),
        Some("Passphrase must be at least 12 characters"),
        "with a destination in, the form should be asking for the passphrase"
    );
}

/// The vault dialog reaches the unlock screen through the same seam, carrying the chosen
/// path and not the demo's.
#[test]
fn choosing_a_vault_carries_the_path_to_unlock() {
    let want = std::env::temp_dir().join("citadel-studio-picked.cdl");
    let answer = want.clone();
    let mut d = Driver::picking(move |ask| {
        assert_eq!(ask, Ask::ExistingVault);
        Some(answer.clone())
    });
    d.click_id(egui::Id::new(("home_card", "Open vault")));
    assert_eq!(d.state().route, Route::Unlock);
    assert_eq!(
        d.state().target,
        citadel_studio::state::Target::Picked(want),
        "unlock is working on the demo rather than the file that was chosen"
    );
}

/// The Copy control produces statements for the file that was read.
#[test]
fn the_plan_is_generated_from_the_file_and_copied() {
    let (_source_dir, path) = sqlite_fixture(
        "plan",
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body VARCHAR(80), price DECIMAL(9,2));",
    );
    let answer = path.clone();
    let mut d = Driver::picking(move |_| Some(answer.clone()));
    d.click_id(egui::Id::new(("home_row", "import_sqlite")));
    d.click_id_working(egui::Id::new(("btn", "Choose")));
    d.until("the reader to finish", |s| {
        matches!(s.source, citadel_studio::state::Source::Read { .. })
    });
    d.shot("49-import-plan");

    let copied = d.click_for_copy(egui::Id::new(("btn", "Copy CREATE TABLE")));
    assert!(
        copied.contains("CREATE TABLE \"notes\""),
        "nothing usable reached the clipboard: {copied:?}"
    );
    assert!(copied.contains("\"id\" INTEGER"), "{copied}");
    assert!(
        copied.contains("\"body\" TEXT"),
        "VARCHAR did not take TEXT affinity: {copied}"
    );
    assert!(
        copied.contains("-- price declared DECIMAL(9,2)"),
        "the one inexact column was translated without saying so: {copied}"
    );
}

/// The import screen shows what the chosen file actually holds. Driven with a real
/// database on disk, read through the real reader.
#[test]
fn the_import_screen_reports_the_file_it_read() {
    let (_source_dir, path) = sqlite_fixture(
        "notes",
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, ts INTEGER);
         INSERT INTO notes (body, ts) VALUES ('a', 1), ('b', 2), ('c', 3);
         CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT, colour TEXT);
         INSERT INTO tags (name, colour) VALUES ('x', 'red');",
    );
    let tables = citadel_studio::sqlite::read(&path).expect("the fixture is readable");

    let mut d = Driver::new(|s| {
        citadel_studio::state::apply(s, Action::BeginImport);
    });
    assert_eq!(d.state().route, Route::Import);
    // Nothing chosen: the screen says so. An empty grid would read as "no tables".
    assert_eq!(
        d.state().source.blocker(),
        Some("Choose a SQLite file"),
        "an unchosen source did not say it was unchosen"
    );
    d.shot("46-import-empty");

    d.apply(Action::SetImportSource(
        citadel_studio::state::Source::Read {
            path: path.clone(),
            tables: tables.clone(),
        },
    ));
    assert_eq!(
        d.state().source.blocker(),
        None,
        "a file that was read and holds tables still reports a reason to fix"
    );
    d.shot("47-import-read");

    // The counts on screen are the file's, not a fixture's.
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[0].name, "notes");
    assert_eq!(tables[0].rows, 3);
    assert_eq!(tables[1].columns.len(), 3);
}

/// A read in progress is its own state on screen, and it names the file.
#[test]
fn a_read_in_progress_says_what_it_is_reading() {
    let mut d = Driver::new(|s| citadel_studio::state::apply(s, Action::BeginImport));
    d.apply(Action::SetImportSource(
        citadel_studio::state::Source::Reading {
            path: std::path::PathBuf::from("/data/very-large.sqlite"),
        },
    ));
    assert_eq!(
        d.state().source.blocker(),
        Some("Still reading that file"),
        "a read in progress was not distinguished from a finished one"
    );
    assert!(
        d.state().source.tables().is_empty(),
        "a read in progress reported tables it does not have yet"
    );
    d.shot("50-import-reading");
}

/// A file that could not be read says why, in SQLite's own words, and never appears as a
/// database with no tables.
#[test]
fn an_unreadable_source_is_reported_as_unreadable() {
    let source_dir = tempfile::tempdir().expect("create fixture directory");
    let path = source_dir.path().join("not-a-db.sqlite");
    std::fs::write(&path, b"not a database").expect("write");
    let error = citadel_studio::sqlite::read(&path).expect_err("a text file is not a database");

    let mut d = Driver::new(|s| citadel_studio::state::apply(s, Action::BeginImport));
    d.apply(Action::SetImportSource(
        citadel_studio::state::Source::Failed {
            path: path.clone(),
            error,
        },
    ));
    assert_eq!(
        d.state().source.blocker(),
        Some("That file could not be read"),
        "an unreadable file was not distinguished from an empty one"
    );
    d.shot("48-import-failed");

    // An empty database is a different answer, and has its own sentence.
    let (_empty_dir, empty) = sqlite_fixture("empty", "");
    d.apply(Action::SetImportSource(
        citadel_studio::state::Source::Read {
            path: empty.clone(),
            tables: Vec::new(),
        },
    ));
    assert_eq!(
        d.state().source.blocker(),
        Some("That database holds no tables")
    );
}

/// Leaving the import screen drops the source, so re-entering starts empty.
#[test]
fn cancelling_import_drops_the_source() {
    let (_source_dir, path) = sqlite_fixture("dropped", "CREATE TABLE t (x INTEGER);");
    let tables = citadel_studio::sqlite::read(&path).expect("readable");

    let mut d = Driver::new(|s| citadel_studio::state::apply(s, Action::BeginImport));
    d.apply(Action::SetImportSource(
        citadel_studio::state::Source::Read {
            path: path.clone(),
            tables,
        },
    ));
    assert!(d.state().source.path().is_some());

    d.key(egui::Key::Escape);
    assert_eq!(d.state().route, Route::Home);
    assert_eq!(d.state().source, citadel_studio::state::Source::None);
}

/// The `Import SQLite schema` row opens the screen, and its keycap names a working
/// shortcut.
#[test]
fn the_import_row_and_its_shortcut_both_work() {
    let mut d = Driver::new(|_| {});
    d.click_id(egui::Id::new(("home_row", "import_sqlite")));
    assert_eq!(
        d.state().route,
        Route::Import,
        "the row did not open import"
    );

    d.key(egui::Key::Escape);
    assert_eq!(d.state().route, Route::Home);
    d.key_mod(egui::Key::I, egui::Modifiers::COMMAND);
    assert_eq!(
        d.state().route,
        Route::Import,
        "the Ctrl I keycap on Home names a shortcut that does nothing"
    );
}

/// The `Create vault` card opens the form.
#[test]
fn the_create_card_opens_the_form() {
    let mut d = Driver::new(|_| {});
    assert_eq!(d.state().route, Route::Home);
    d.click_id(egui::Id::new(("home_card", "Create vault")));
    assert_eq!(
        d.state().route,
        Route::Create,
        "the Create vault card did not open the form"
    );
}

#[test]
fn the_create_button_states_the_first_missing_answer() {
    use citadel_studio::state::NewVault;

    let blocker = |form: &NewVault| form.blocker();
    let mut form = NewVault::default();
    assert_eq!(blocker(&form), Some("Choose where to save the vault"));

    form.path = Some(std::path::PathBuf::from("/tmp/new.cdl"));
    assert_eq!(
        blocker(&form),
        Some("Passphrase must be at least 12 characters")
    );

    // One short of the minimum is still short, which catches an off-by-one.
    form.passphrase = "x".repeat(citadel_studio::state::MIN_PASSPHRASE - 1).into();
    assert_eq!(
        blocker(&form),
        Some("Passphrase must be at least 12 characters")
    );

    form.passphrase = "x".repeat(citadel_studio::state::MIN_PASSPHRASE).into();
    assert_eq!(blocker(&form), Some("The two passphrases do not match"));

    form.confirm = form.passphrase.clone();
    assert_eq!(
        blocker(&form),
        None,
        "a complete form still reports a reason the reader could act on"
    );

    // Counted in characters, not bytes. A byte count accepts 4 multi-byte glyphs.
    let short = NewVault {
        passphrase: "AAAA".to_owned().into(),
        confirm: "AAAA".to_owned().into(),
        path: Some(std::path::PathBuf::from("/tmp/new.cdl")),
        ..NewVault::default()
    };
    assert!(blocker(&short).is_some(), "a 4-character passphrase passed");
}

/// Two secrets on one screen: a keystroke lands in the focused field, and revealing one
/// hides the other.
#[test]
fn the_create_form_keeps_its_two_secrets_apart() {
    use citadel_studio::state::SecretField;

    let mut d = Driver::new(|s| citadel_studio::state::apply(s, Action::BeginCreate));
    let field = |name: &str| egui::Id::new(("create_secret", name));

    d.click_id(field("PASSPHRASE"));
    d.type_text("correct horse");
    assert_eq!(d.state().new_vault.passphrase.as_str(), "correct horse");
    assert!(
        d.state().new_vault.confirm.is_empty(),
        "typing into the passphrase field also filled the confirm field"
    );

    // Clicking the second field moves the caret into it. An `interact` rect does not take
    // keyboard focus on click; unlock's single field auto-claimed it and hid that.
    d.click_id(field("CONFIRM PASSPHRASE"));
    assert_eq!(
        d.harness.ctx.memory(|m| m.focused()),
        Some(field("CONFIRM PASSPHRASE")),
        "clicking the confirm field left the caret in the passphrase field"
    );
    d.type_text("correct horse");
    assert_eq!(d.state().new_vault.confirm.as_str(), "correct horse");
    assert_eq!(
        d.state().new_vault.passphrase.as_str(),
        "correct horse",
        "typing into the confirm field changed the passphrase"
    );
    // With both secrets in and matching, the form waits on the destination, which comes
    // from a native dialog and cannot be typed.
    assert_eq!(
        d.state().new_vault.blocker(),
        Some("Choose where to save the vault")
    );
    d.apply(Action::SetCreatePath(std::path::PathBuf::from(
        "/tmp/new.cdl",
    )));
    assert_eq!(d.state().new_vault.blocker(), None);

    // Reveal is one at a time.
    d.click_id(field("PASSPHRASE").with("reveal"));
    assert_eq!(d.state().revealed, Some(SecretField::New));
    d.click_id(field("CONFIRM PASSPHRASE").with("reveal"));
    assert_eq!(
        d.state().revealed,
        Some(SecretField::Confirm),
        "revealing the second field left the first one showing"
    );
    d.shot("43-create-filled");

    // Clicking it again hides.
    d.click_id(field("CONFIRM PASSPHRASE").with("reveal"));
    assert_eq!(d.state().revealed, None);
}

#[test]
fn a_pasted_passphrase_is_accepted_but_never_reported_as_widget_text() {
    const CANARY: &str = "pasted-secret-canary";
    let mut d = Driver::new(|s| citadel_studio::state::apply(s, Action::BeginCreate));
    let field = egui::Id::new(("create_secret", "PASSPHRASE"));

    d.click_id(field).paste_text(CANARY);
    assert_eq!(d.state().new_vault.passphrase.as_str(), CANARY);

    let info = d
        .harness
        .output()
        .platform_output
        .events
        .iter()
        .find_map(|event| match event {
            egui::output::OutputEvent::ValueChanged(info)
                if info.typ == egui::WidgetType::TextEdit =>
            {
                Some(info)
            }
            _ => None,
        })
        .expect("pasting did not report a text-edit value change");
    assert_eq!(
        info.current_text_value.as_deref(),
        Some("...................."),
        "password widget metadata did not mask every pasted character"
    );
    assert!(
        !format!("{info:?}").contains(CANARY),
        "password widget metadata exposed the pasted value"
    );
}

/// Leaving the form takes the passphrase with it, so no abandoned secret stays in memory.
#[test]
fn cancelling_create_clears_the_passphrase() {
    let mut d = Driver::new(|s| citadel_studio::state::apply(s, Action::BeginCreate));
    d.click_id(egui::Id::new(("create_secret", "PASSPHRASE")));
    d.type_text("correct horse battery");
    assert!(!d.state().new_vault.passphrase.is_empty());

    d.key(egui::Key::Escape);
    assert_eq!(
        d.state().route,
        Route::Home,
        "Escape did not leave the form"
    );
    assert_eq!(
        d.state().new_vault,
        citadel_studio::state::NewVault::default(),
        "the form kept a secret after it was abandoned"
    );

    // Re-entering starts clean.
    d.apply(Action::BeginCreate);
    assert!(d.state().new_vault.passphrase.is_empty());
}

/// The two KDFs take their cost differently, so the cost row follows the chosen one. An
/// Argon2 profile control under PBKDF2 would change nothing.
#[test]
fn choosing_pbkdf2_replaces_the_argon2_profile_control() {
    use citadel_studio::state::{Argon2Profile, KdfAlgorithm};

    let mut d = Driver::new(|s| citadel_studio::state::apply(s, Action::BeginCreate));
    assert_eq!(
        d.state().new_vault.kdf,
        KdfAlgorithm::Argon2id,
        "Argon2id is default"
    );

    let profile = |label: &str| egui::Id::new(("seg", "profile", label));
    assert!(
        d.harness.ctx.read_response(profile("Server")).is_some(),
        "the Argon2 profile control is missing while Argon2id is chosen"
    );
    d.click_id(profile("Server"));
    assert_eq!(d.state().new_vault.profile, Argon2Profile::Server);

    d.click_id(egui::Id::new(("seg", "kdf", "PBKDF2-HMAC-SHA256")));
    assert_eq!(d.state().new_vault.kdf, KdfAlgorithm::Pbkdf2HmacSha256);
    assert!(
        d.harness.ctx.read_response(profile("Server")).is_none(),
        "the Argon2 profile control is still on screen under PBKDF2, where it changes nothing"
    );
    d.shot("44-create-pbkdf2");
}

/// Every control on the form is on screen at the declared minimum size. The form is the
/// tallest entry screen and is centred in what the body has left, so a short window is
/// where its bottom goes missing.
#[test]
fn the_whole_create_form_fits_the_minimum_window() {
    let mut d = Driver::sized(1100.0, 700.0, |s| {
        citadel_studio::state::apply(s, Action::BeginCreate);
    });
    d.shot("45-create-min");

    let window = egui::Rect::from_min_size(egui::Pos2::ZERO, d.size);
    for id in [
        egui::Id::new(("create_secret", "PASSPHRASE")),
        egui::Id::new(("create_secret", "CONFIRM PASSPHRASE")),
        egui::Id::new(("seg", "kdf", "Argon2id")),
        egui::Id::new(("seg", "profile", "Desktop")),
        egui::Id::new(("btn", "Choose")),
        egui::Id::new(("btn", "Cancel")),
        egui::Id::new(("btn", "Create vault")),
    ] {
        let rect = d
            .harness
            .ctx
            .read_response(id)
            .unwrap_or_else(|| panic!("{id:?} was never laid out at 1100x700"))
            .rect;
        assert!(
            window.contains_rect(rect),
            "{id:?} laid out at {rect:?}, outside the 1100x700 window"
        );
    }
}

/// The costs on screen are the engine's. Drift here misreports what the vault is built
/// with.
#[test]
fn the_argon2_profiles_carry_the_engines_costs() {
    use citadel_studio::state::{profile_costs, profile_detail, Argon2Profile};

    // These are the engine's own type now, and the numbers are read from it rather than
    // restated here, so this asserts the screen shows what the vault is built with.
    assert_eq!(profile_costs(Argon2Profile::Iot), (19, 2, 1));
    assert_eq!(profile_costs(Argon2Profile::Desktop), (64, 3, 4));
    assert_eq!(profile_costs(Argon2Profile::Server), (128, 4, 4));
    assert_eq!(
        profile_detail(Argon2Profile::Desktop),
        "64 MiB, 3 passes, 4 lanes"
    );
}

/// The SQL editor edits, and the text lives in the model. Typed through real keyboard
/// input into the real `TextEdit`, exercising the caret, the layouter and the
/// copy-and-diff that holds the one-way flow.
#[test]
fn the_sql_editor_edits_the_query_in_the_model() {
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
    });
    let before = d.state().query.clone();
    assert!(
        before.contains("SELECT"),
        "the fixture opens with a statement"
    );

    // Put the caret in the editor the way a user would.
    let editor_id = d.state().query_editor_id();
    let editor = d
        .harness
        .ctx
        .read_response(editor_id)
        .expect("the editor is registered");
    let at = editor.rect.left_top() + egui::vec2(20.0, 10.0);
    d.click(at.x, at.y);
    assert_eq!(
        d.harness.ctx.memory(|m| m.focused()),
        Some(editor_id),
        "clicking the editor did not focus it"
    );

    d.type_text("-- edited\n");
    let after = d.state().query.clone();
    assert_ne!(after, before, "typing did not reach the model");
    assert!(
        after.contains("-- edited"),
        "the typed text is missing from {after:?}"
    );
    d.shot("39-query-edited");

    // Undo is `TextEdit`'s, and it has to reach the model the same way an edit does.
    d.key_mod(egui::Key::Z, egui::Modifiers::COMMAND);
    assert_eq!(
        d.state().query,
        before,
        "undo did not restore the statement through the same path an edit takes"
    );

    // Tab indents rather than leaving the field: a code editor that loses focus on Tab
    // cannot be used to write indented SQL.
    let indented = d.state().query.clone();
    d.key(egui::Key::Tab);
    assert_ne!(
        d.state().query,
        indented,
        "Tab moved focus out of the editor instead of indenting"
    );

    // But the keyboard must not be trapped there. Escape hands focus back.
    d.key(egui::Key::Escape);
    assert_ne!(
        d.harness.ctx.memory(|m| m.focused()),
        Some(editor_id),
        "Escape did not release the editor, so the keyboard is trapped in it"
    );
}

#[test]
fn dirty_query_confirmation_and_vault_scoped_undo_are_enforced() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
    });
    let first_editor = d.state().query_editor_id();
    let rect = d.rect_of(first_editor);
    d.click(rect.left() + 20.0, rect.top() + 10.0)
        .type_text("-- vault-a-canary\n");
    assert!(d.state().query_dirty);

    d.apply(Action::CloseVault);
    assert_eq!(
        d.state().query_discard,
        Some(citadel_studio::state::QueryDiscard::CloseVault)
    );
    d.harness
        .get_by_role_and_label(egui::accesskit::Role::Dialog, "Discard the edited query?");
    assert!(
        d.harness
            .get_by_role_and_label(egui::accesskit::Role::Button, "Keep editing")
            .is_focused(),
        "the safe action did not receive modal focus"
    );
    d.key(egui::Key::Escape);
    assert!(d.state().query_discard.is_none());
    assert!(d.state().query.contains("vault-a-canary"));

    d.apply(Action::CloseVault);
    d.click_id(egui::Id::new(("btn", "Discard")));
    assert!(d.state().vault.is_none());

    d.open_demo();
    d.apply(Action::OpenDoc(Doc::Query));
    let second_editor = d.state().query_editor_id();
    assert_ne!(first_editor, second_editor);
    let rect = d.rect_of(second_editor);
    d.click(rect.left() + 20.0, rect.top() + 10.0);
    d.key_mod(egui::Key::Z, egui::Modifiers::COMMAND);
    assert!(
        !d.state().query.contains("vault-a-canary"),
        "undo resurrected SQL from the previous vault session"
    );
}

#[test]
fn replacing_a_vault_cannot_silently_erase_a_hidden_query() {
    use egui_kittest::kittest::Queryable;

    let mut d = workspace(|state| {
        state.docs = vec![Doc::Query];
        state.active = 0;
        state.route = Route::Workspace;
    });
    let editor = d.state().query_editor_id();
    let rect = d.rect_of(editor);
    d.click(rect.left() + 20.0, rect.top() + 10.0)
        .type_text("-- keep-this-query\n");
    assert!(d.state().query_dirty);

    d.apply(Action::CloseDoc(0));
    assert_eq!(d.state().route, Route::Home);
    assert!(d.state().query_dirty, "closing the tab discarded its draft");

    d.apply(Action::BeginImport);
    assert_eq!(
        d.state().query_discard,
        Some(citadel_studio::state::QueryDiscard::BeginImport)
    );
    d.harness
        .get_by_role_and_label(egui::accesskit::Role::Dialog, "Discard the edited query?");
    assert!(d.state().query.contains("keep-this-query"));

    d.apply(Action::CancelDiscardQuery);
    assert!(d.state().query.contains("keep-this-query"));
    d.apply(Action::CloseDoc(0));
    d.apply(Action::BeginImport);
    d.click_id(egui::Id::new(("btn", "Discard")));
    assert_eq!(d.state().route, Route::Import);
    assert!(!d.state().query_dirty);
    assert!(!d.state().query.contains("keep-this-query"));
}

#[test]
fn editor_navigation_does_not_move_results_and_f5_is_query_only() {
    let mut d = workspace(|_| {});
    let untouched = d.state().result.done().is_none();
    assert!(untouched);
    d.key(egui::Key::F5);
    assert!(
        d.state().result.done().is_none(),
        "F5 ran the hidden query from a non-query document"
    );

    d.apply(Action::OpenDoc(Doc::Query));
    d.click_id(egui::Id::new(("btn", "Run")));
    assert!(d.state().result.done().is_some());
    d.with_state(|state| {
        state.selected_row = 1;
        state.scroll_row = 0;
    });
    let editor = d.state().query_editor_id();
    let rect = d.rect_of(editor);
    d.click(rect.left() + 20.0, rect.top() + 10.0);
    let before = (d.state().selected_row, d.state().scroll_row);
    d.key(egui::Key::ArrowDown);
    assert_eq!(
        (d.state().selected_row, d.state().scroll_row),
        before,
        "editor navigation leaked into the result grid"
    );
}

/// A statement longer than the editor stays reachable by scrolling.
#[test]
fn a_long_statement_scrolls_inside_the_editor() {
    let long: String = (1..=120)
        .map(|i| format!("SELECT {i} AS n FROM documents;\n"))
        .collect();
    let mut d = workspace(|s| {
        citadel_studio::state::apply(s, Action::OpenDoc(Doc::Query));
        citadel_studio::state::apply(s, Action::EditQuery(long.clone()));
    });
    d.shot("40-query-long");

    // The editor lays out every line; the scroll area reaches the rest.
    let editor_id = d.state().query_editor_id();
    let editor = d
        .harness
        .ctx
        .read_response(editor_id)
        .expect("the editor is registered");
    assert!(
        editor.rect.height() > 1000.0,
        "a 120-line statement laid out {}pt tall, so it was clipped rather than scrolled",
        editor.rect.height()
    );
    assert_eq!(d.state().query, long, "the editor rewrote the statement");
}

/// What the canvas costs per frame, measured.
///
/// The bound is loose: it catches a catastrophic regression. A wall-clock assertion tight
/// enough to be interesting fails on a busy machine. Run with `--nocapture` for figures.
#[test]
fn the_canvas_frame_cost_is_bounded() {
    let mut d = workspace(|s| {
        citadel_studio::state::apply(
            s,
            Action::OpenDoc(Doc::Vector("documents.embedding".into())),
        );
    });
    d.with_state(|state| {
        state.vault.as_mut().unwrap().vectors[0] = citadel_studio::model::VectorColumn::new(
            "documents",
            "embedding",
            32,
            20_000,
            stress_vector_points(20_000),
        );
    });
    let points = d.state().vault().vectors[0].points.len();

    // Warm the caches so the figure is steady-state, not first-frame.
    for _ in 0..10 {
        d.harness.step();
    }
    let start = std::time::Instant::now();
    const FRAMES: u32 = 120;
    for _ in 0..FRAMES {
        d.harness.step();
    }
    let per_frame = start.elapsed() / FRAMES;

    println!(
        "vector canvas: {points} points, {:.2} ms/frame ({:.0} fps equivalent)",
        per_frame.as_secs_f64() * 1000.0,
        1.0 / per_frame.as_secs_f64()
    );
    assert!(
        per_frame < std::time::Duration::from_millis(100),
        "a {points}-point canvas took {per_frame:?} per frame, which is a regression \
         rather than a slow machine"
    );
}

#[test]
fn twenty_thousand_points_render_at_hidpi() {
    let mut d = Driver::hidpi(|_| {});
    d.open_demo();
    d.with_state(|state| {
        citadel_studio::state::apply(
            state,
            Action::OpenDoc(Doc::Vector("documents.embedding".into())),
        );
        state.vault.as_mut().unwrap().vectors[0] = citadel_studio::model::VectorColumn::new(
            "documents",
            "embedding",
            32,
            20_000,
            stress_vector_points(20_000),
        );
    });

    let image = d.harness.render().expect("high-DPI cloud render failed");
    assert_eq!(image.dimensions(), (3_200, 2_000));
}

/// The canvas is explorable: the wheel zooms, a drag pans, a click picks the nearest
/// point, a double click resets. Driven through real pointer input, so the hit test and
/// the camera are exercised together.
#[test]
fn the_vector_canvas_pans_zooms_and_picks() {
    use citadel_studio::state::Camera;

    let mut d = workspace(|s| {
        citadel_studio::state::apply(
            s,
            Action::OpenDoc(Doc::Vector("documents.embedding".into())),
        );
    });
    assert_eq!(d.state().camera, Camera::default(), "opens framed to fit");

    // Zoom in over a point inside the canvas.
    d.scroll(240.0);
    let zoomed = d.state().camera;
    assert!(
        zoomed.zoom > Camera::MIN_ZOOM,
        "the wheel did not zoom the canvas"
    );
    d.shot("38-vector-zoomed");

    // Culling leaves fewer points on screen once zoomed. The cloud is a real column read
    // back from the engine, so its size is the table's, not a number chosen here.
    let column = &d.state().vault().vectors[0];
    let all = column.points.len();
    assert_eq!(
        all as u64, column.total,
        "the whole column fits under the sample cap, so nothing should have been dropped"
    );
    assert!(all > 1_000, "too few points to exercise culling: {all}");

    // Drag pans, and the camera stays inside its clamp.
    let centre = (d.size * 0.5).to_pos2();
    d.drag(centre, egui::vec2(-160.0, -120.0));
    let panned = d.state().camera;
    assert_ne!(panned.centre, zoomed.centre, "the drag did not pan");
    assert_eq!(panned, panned.clamped(), "panning escaped the clamp");

    // Double click resets both the framing and the selection.
    d.apply(Action::SelectPoint(Some(7)));
    d.apply(Action::ResetCamera);
    assert_eq!(d.state().camera, Camera::default());
    assert_eq!(
        d.state().selected_point,
        None,
        "reset left a marker on a point the view no longer frames"
    );
}

/// A picked file exposes its unauthenticated header facts before unlock.
#[test]
fn a_picked_file_states_the_header_it_read() {
    use citadel_studio::engine::session::{CreateSpec, Session};
    use citadel_studio::state::Target;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let picked = dir.path().join("customer-export.cdl");
    let spec = CreateSpec {
        path: picked.clone(),
        passphrase: "the-real-passphrase".to_owned().into(),
        kdf: citadel_studio::state::KdfAlgorithm::Argon2id,
        profile: citadel_studio::state::Argon2Profile::Iot,
    };
    drop(Session::create(&spec).expect("the vault is created"));

    let facts = citadel::inspect_vault(&picked).expect("the header reads without a key");
    let mut d = Driver::new({
        let picked = picked.clone();
        move |s| {
            citadel_studio::state::apply(
                s,
                Action::BeginUnlock(
                    Target::Picked(picked.clone()),
                    citadel_studio::state::Preview::Read(Box::new(facts.clone())),
                ),
            );
        }
    });
    d.shot("37-unlock-picked");

    let s = d.state();
    assert_eq!(s.target, Target::Picked(picked.clone()));
    assert_eq!(
        s.target
            .path(s.preview.read().map(|i| i.data_path.as_path())),
        picked.display().to_string(),
        "the screen would show the demo's path over a file the user chose"
    );

    let info = s.preview.read().expect("the header was read at pick time");
    let rows = citadel_studio::model::header_rows(info);
    let cipher = &rows[0].1;
    assert!(
        cipher.contains("AES") || cipher.contains("ChaCha"),
        "the cipher row must name the cipher, not repeat 'not read': {cipher}"
    );
    assert_eq!(
        rows[3].1, "epoch 1",
        "a key file that belongs to this database reports its epoch"
    );

    // And Unlock is live: the engine can open a chosen file.
    let unlock = d
        .harness
        .ctx
        .read_response(egui::Id::new(("btn", "Unlock")))
        .expect("the button is drawn");
    assert!(
        !unlock.sense.senses_click(),
        "an empty passphrase still refuses, for its own stated reason"
    );
    d.type_text(&spec.passphrase);
    let unlock = d
        .harness
        .ctx
        .read_response(egui::Id::new(("btn", "Unlock")))
        .expect("drawn");
    assert!(
        unlock.sense.senses_click(),
        "with a passphrase typed, a chosen file must be openable"
    );
}

/// A key file belonging to another database is named before anyone waits on Argon2.
#[test]
fn a_foreign_key_file_is_named_in_the_header() {
    use citadel_studio::engine::session::{CreateSpec, Session};

    let dir = tempfile::tempdir().expect("a temporary directory");
    let mine = dir.path().join("mine.cdl");
    let other = dir.path().join("other.cdl");
    for path in [&mine, &other] {
        let spec = CreateSpec {
            path: path.clone(),
            passphrase: "the-real-passphrase".to_owned().into(),
            kdf: citadel_studio::state::KdfAlgorithm::Argon2id,
            profile: citadel_studio::state::Argon2Profile::Iot,
        };
        drop(Session::create(&spec).expect("the vault is created"));
    }

    // The other vault's key file, over this one's data file.
    let facts = citadel::inspect_vault_with_key(&mine, &citadel::default_key_path(&other))
        .expect("the header still reads");
    let rows = citadel_studio::model::header_rows(&facts);
    assert_eq!(
        rows[3].1, "belongs to another database",
        "the header must say which failure this is, before the passphrase is blamed"
    );
}

/// Preferences survive a restart; claims do not.
///
/// Driven through the real `eframe::App::save` and the real `get_value`/`set_value` pair
/// against an in-memory `Storage`, so this exercises the encoding the app actually uses
/// rather than a hand-rolled copy of it.
#[test]
fn preferences_persist_and_proof_claims_do_not() {
    use citadel_studio::state::Prefs;
    use eframe::App as _;

    #[derive(Default)]
    struct Mem(std::collections::HashMap<String, String>);
    impl eframe::Storage for Mem {
        fn get_string(&self, key: &str) -> Option<String> {
            self.0.get(key).cloned()
        }
        fn set_string(&mut self, key: &str, value: String) {
            self.0.insert(key.to_owned(), value);
        }
        fn remove_string(&mut self, key: &str) {
            self.0.remove(key);
        }
        fn flush(&mut self) {}
    }

    // A session that changed both preferences and verified a page.
    let mut d = workspace(|_| {});
    d.apply(Action::ToggleTheme);
    d.apply(Action::CycleDensity);
    d.apply(Action::VerifyPage);
    let (light, density) = {
        let s = d.state();
        assert!(
            s.light,
            "the fixture should have switched to the light theme"
        );
        assert!(s.page_checked.scope > 0, "and verified a page");
        (s.light, s.density)
    };

    let mut storage = Mem::default();
    d.harness.state_mut().save(&mut storage);

    // A fresh application, restored from that storage.
    let mut fresh = citadel_studio::app::Studio::default();
    let prefs: Prefs = eframe::get_value(&storage, eframe::APP_KEY).expect("saved prefs");
    prefs.apply(&mut fresh.state);

    assert_eq!(
        fresh.state.light, light,
        "theme did not survive the restart"
    );
    assert_eq!(fresh.state.density, density, "density did not survive");
    assert_eq!(
        fresh.state.palette.is_light(),
        light,
        "the theme was restored but the palette was not, so the next frame paints the \
         wrong one"
    );
    assert_eq!(
        fresh.state.page_checked,
        citadel_studio::state::PageCheck::default(),
        "a restart restored a proof claim that nothing has re-checked"
    );
    assert!(fresh.state.vault.is_none(), "a restart reopened a vault");
    assert!(fresh.state.docs.is_empty(), "a restart reopened documents");
}

/// Recent vaults survive restart and omit paths that no longer exist.
#[test]
fn recent_vaults_survive_a_restart_and_drop_files_that_vanished() {
    use citadel_studio::model::RecentVault;
    use citadel_studio::state::Prefs;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let kept = dir.path().join("kept.cdl");
    std::fs::write(&kept, b"not a real vault, only a file that exists").expect("write");
    let gone = dir.path().join("deleted.cdl");

    let saved = Prefs {
        recent: vec![
            RecentVault {
                path: kept.clone(),
                entries: 0,
                pages: 103,
            },
            RecentVault {
                path: gone,
                entries: 12,
                pages: 7,
            },
        ],
        ..Prefs::default()
    };

    let mut fresh = citadel_studio::app::Studio::default();
    saved.apply(&mut fresh.state);

    let paths: Vec<_> = fresh.state.recent.iter().map(|r| r.path.clone()).collect();
    assert_eq!(
        paths,
        vec![kept],
        "a restart should offer the vault that is still there and drop the one that is not"
    );
}

/// Renders multilingual memory text to exercise the fallback chain as pixels.
#[test]
fn a_region_of_non_latin_text() {
    let mut d = workspace(|s| {
        let vault = s.vault.as_mut().expect("the demo vault is open");
        let region = &mut vault.regions[0];
        for (atom, text) in region.atoms.iter_mut().zip([
            "部署窗口是每周四 02:00 UTC",
            "Пользователь предпочитает тёмные темы",
            "Η ομάδα χρησιμοποιεί μετρικές μονάδες",
            "デプロイは木曜日の02:00 UTCです",
            "배포 창은 매주 목요일 02:00 UTC입니다",
        ]) {
            atom.text = text.to_owned();
        }
        s.selected_row = 0;
    });
    d.shot("36-non-latin");
}

/// Text outside the vendored faces must reach a fallback rather than a tofu box.
///
/// Checked with `has_glyphs`, which reports real coverage. A layout-shape check does not
/// work: epaint substitutes an unknown-glyph box with an ordinary advance width, so an
/// uncovered codepoint produces a galley indistinguishable from a covered one.
#[test]
fn text_outside_the_vendored_faces_still_renders() {
    use citadel_studio::fonts::role;

    let d = Driver::new(|_| {});
    let ctx = &d.harness.ctx;
    // Cyrillic, Greek, maths, and currency symbols reach Ubuntu Light.
    for sample in ["Привет", "Ελλάδα", "√±≤≥", "€£¥"] {
        for (family, font) in [("mono", role::cell()), ("ui", role::chrome())] {
            assert!(
                ctx.fonts_mut(|f| f.has_glyphs(&font, sample)),
                "the {family} family has no glyphs for {sample}, so it renders as tofu"
            );
        }
    }

    // The check must be able to fail, or the assertions above prove nothing. A private-use
    // codepoint is the contrast case: no real face claims that block, so `has_glyphs` has
    // to say no even with a full system CJK face loaded behind everything else.
    assert!(
        !ctx.fonts_mut(|f| f.has_glyphs(&role::cell(), "\u{E000}")),
        "a private-use codepoint reported coverage, so this test cannot tell covered from not"
    );

    // System CJK coverage varies; assert only that accepted glyphs remain reachable.
    for (probe, script) in [
        ('\u{4E2D}', "Han"),
        ('\u{3042}', "Hiragana"),
        ('\u{D55C}', "Hangul"),
    ] {
        let promised = citadel_studio::fonts::covers(probe);
        let loaded = ctx.fonts_mut(|f| f.has_glyphs(&role::cell(), &probe.to_string()));
        assert_eq!(
            promised, loaded,
            "the font chain says {promised} for {script} but the loaded stack reports \
             {loaded}: one of them is lying"
        );
    }
}

/// Every evidence hue replaced by one grey, leaving the fill pattern as the only channel.
fn colour_stripped(mut p: Palette) -> Palette {
    let g = egui::Color32::from_rgb(0x8a, 0x8f, 0x96);
    p.verified = g;
    p.unverified = g;
    p.erased = g;
    p.tampered = g;
    p.missing = g;
    p
}

fn count(d: &Driver, ev: Evidence) -> usize {
    d.state()
        .visible_atoms()
        .unwrap()
        .iter()
        .filter(|a| a.evidence() == ev)
        .count()
}

/// The signature element, in colour and with every evidence hue replaced by one grey.
#[test]
fn rail_specimen() {
    use citadel_studio::fonts::role;
    use citadel_studio::rail;
    use citadel_studio::theme::{self, metrics};

    let p = Palette::DARK;
    let grey = colour_stripped(p);

    let mut ready = false;
    let mut harness = Harness::builder()
        .with_size(egui::vec2(760.0, 300.0))
        .with_pixels_per_point(1.0)
        .wgpu()
        .build_ui(move |ui| {
            if !ready {
                theme::install(ui.ctx(), &p);
                ready = true;
                ui.ctx().request_repaint();
                return;
            }
            let full = ui.max_rect();
            ui.painter().rect_filled(full, 0.0, p.ground1);
            let painter = ui.painter().clone();
            for (col, (pal, title)) in [(&p, "IN COLOUR"), (&grey, "COLOUR STRIPPED")]
                .into_iter()
                .enumerate()
            {
                let x0 = full.left() + 32.0 + col as f32 * 360.0;
                painter.text(
                    egui::pos2(x0, full.top() + 20.0),
                    egui::Align2::LEFT_CENTER,
                    title,
                    role::column_header(),
                    p.text3,
                );
                for (i, ev) in rail::ALL.into_iter().enumerate() {
                    let y = full.top() + 44.0 + i as f32 * (metrics::ROW_DEFAULT + 8.0);
                    let row = egui::Rect::from_min_size(
                        egui::pos2(x0, y),
                        egui::vec2(300.0, metrics::ROW_DEFAULT),
                    );
                    rail::paint(&painter, row, ev, pal, 1.0);
                    painter.text(
                        egui::pos2(row.left() + 16.0, row.center().y),
                        egui::Align2::LEFT_CENTER,
                        rail::glyph(ev),
                        role::cell_compact(),
                        p.text3,
                    );
                    painter.text(
                        egui::pos2(row.left() + 52.0, row.center().y),
                        egui::Align2::LEFT_CENTER,
                        ev.label(),
                        role::cell(),
                        p.text1,
                    );
                }
            }
        });
    harness.run();
    harness.run();
    let image = harness.render().expect("render failed");
    image.save(shot_path("01-rail")).unwrap();

    // Require both specimen columns so a blank rail cannot pass.
    let ground = image.get_pixel(1, 1);
    let inked = |x0: u32, x1: u32| {
        (x0..x1)
            .flat_map(|x| (0..image.height()).map(move |y| (x, y)))
            .filter(|(x, y)| image.get_pixel(*x, *y) != ground)
            .count()
    };
    assert!(inked(0, 360) > 500, "the colour column drew almost nothing");
    assert!(
        inked(360, 720) > 500,
        "the colour-stripped column drew almost nothing"
    );
}

/// Every colour the SQL editor paints code in has to be readable on the editor's own
/// ground, in both themes, at the WCAG AA floor for body text.
///
/// SQL comments are user-authored prose and must meet the body-text contrast floor.
#[test]
fn code_text_is_legible_in_both_themes() {
    /// WCAG 2.1 relative luminance.
    fn luminance(c: egui::Color32) -> f64 {
        let channel = |v: u8| {
            let v = v as f64 / 255.0;
            if v <= 0.03928 {
                v / 12.92
            } else {
                ((v + 0.055) / 1.055).powf(2.4)
            }
        };
        0.2126 * channel(c.r()) + 0.7152 * channel(c.g()) + 0.0722 * channel(c.b())
    }
    fn contrast(a: egui::Color32, b: egui::Color32) -> f64 {
        let (x, y) = (luminance(a), luminance(b));
        (x.max(y) + 0.05) / (x.min(y) + 0.05)
    }
    /// Largest single-channel difference, which separates two hues sharing a luminance.
    fn apart(a: egui::Color32, b: egui::Color32) -> i32 {
        [(a.r(), b.r()), (a.g(), b.g()), (a.b(), b.b())]
            .into_iter()
            .map(|(x, y)| (x as i32 - y as i32).abs())
            .max()
            .expect("three channels")
    }

    for (theme, p) in [("dark", Palette::DARK), ("light", Palette::LIGHT)] {
        let ink = [
            ("keyword", p.syntax_keyword),
            ("function", p.syntax_function),
            ("string", p.syntax_string),
            ("number", p.syntax_number),
            ("comment", p.syntax_comment),
            ("plain", p.text1),
        ];

        for (what, colour) in ink {
            let ratio = contrast(colour, p.ground1);
            assert!(
                ratio >= 4.5,
                "{theme} {what} reads at {ratio:.2}:1 on the editor ground, under the 4.5:1 floor"
            );
        }

        // Six colours that a reader cannot tell apart are one colour with extra steps.
        for (i, (a_name, a)) in ink.iter().enumerate() {
            for (b_name, b) in &ink[i + 1..] {
                let gap = apart(*a, *b);
                assert!(
                    gap >= 24,
                    "{theme}: {a_name} and {b_name} differ by {gap}/255 at most, too close to read as different"
                );
            }
        }

        // Comments stay the quietest thing in the editor. That subordination is what says
        // "not part of the statement" before a single word is read.
        let comment = contrast(p.syntax_comment, p.ground1);
        for (what, colour) in ink.iter().filter(|(what, _)| *what != "comment") {
            let ratio = contrast(*colour, p.ground1);
            assert!(
                comment < ratio,
                "{theme}: comments at {comment:.2}:1 are not quieter than {what} at {ratio:.2}:1"
            );
        }
    }
}

/// All six rails must remain distinguishable without colour; measure rendered pixels.
#[test]
fn rails_survive_greyscale() {
    use citadel_studio::rail;
    use citadel_studio::theme;

    let p = Palette::DARK;
    let grey = colour_stripped(p);

    let sigs: Vec<Vec<bool>> = rail::ALL
        .into_iter()
        .map(|ev| {
            let mut ready = false;
            let mut harness = Harness::builder()
                .with_size(egui::vec2(40.0, 28.0))
                .with_pixels_per_point(1.0)
                .wgpu()
                .build_ui(move |ui| {
                    if !ready {
                        theme::install(ui.ctx(), &p);
                        ready = true;
                        ui.ctx().request_repaint();
                        return;
                    }
                    let full = ui.max_rect();
                    ui.painter().rect_filled(full, 0.0, p.ground1);
                    rail::paint(ui.painter(), full, ev, &grey, 1.0);
                });
            harness.run();
            harness.run();
            let img = harness.render().expect("render failed");
            (0..img.height())
                .map(|y| {
                    (0..img.width()).any(|x| {
                        let px = img.get_pixel(x, y);
                        px[0] as u32 + px[1] as u32 + px[2] as u32 > 120
                    })
                })
                .collect()
        })
        .collect();

    for (ev, sig) in rail::ALL.into_iter().zip(&sigs) {
        let lit = sig.iter().filter(|b| **b).count();
        if ev == Evidence::NotAttestable {
            assert_eq!(lit, 0, "{ev:?} must draw nothing at all");
        } else {
            assert!(lit > 0, "{ev:?} drew nothing");
        }
    }
    for i in 0..rail::ALL.len() {
        for j in i + 1..rail::ALL.len() {
            assert_ne!(
                sigs[i],
                sigs[j],
                "{:?} and {:?} are indistinguishable with colour stripped",
                rail::ALL[i],
                rail::ALL[j]
            );
        }
    }
}
