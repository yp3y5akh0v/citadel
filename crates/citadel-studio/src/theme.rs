//! Design tokens and the egui style they produce.
//!
//! `ground1` is the data plane; higher ground tiers express chrome depth without shadow.

use egui::{Color32, CornerRadius, Stroke, Style, Visuals};

const fn hex(rgb: u32) -> Color32 {
    Color32::from_rgb(
        (rgb >> 16) as u8,
        ((rgb >> 8) & 0xff) as u8,
        (rgb & 0xff) as u8,
    )
}

/// Every colour in the application. Nothing outside this struct may name a literal.
#[derive(Clone, Copy, Debug)]
pub struct Palette {
    pub ground0: Color32,
    pub ground1: Color32,
    pub ground2: Color32,
    pub ground3: Color32,
    pub ground4: Color32,
    pub ground5: Color32,

    pub text1: Color32,
    pub text2: Color32,
    pub text3: Color32,
    pub text4: Color32,

    pub hairline_quiet: Color32,
    pub hairline: Color32,
    pub hairline_strong: Color32,
    pub stroke: Color32,

    /// Chrome only. Never means "good", never fills a grid cell.
    pub accent: Color32,
    pub accent_solid: Color32,
    pub focus: Color32,
    pub select: Color32,

    pub verified: Color32,
    pub unverified: Color32,
    pub erased: Color32,
    pub tampered: Color32,
    pub missing: Color32,

    /// SQL syntax colours remain distinct from evidence hues.
    pub syntax_keyword: Color32,
    pub syntax_function: Color32,
    pub syntax_string: Color32,
    pub syntax_number: Color32,
    /// Comments meet the same legibility floor as other code text.
    pub syntax_comment: Color32,
}

impl Palette {
    pub const DARK: Self = Self {
        ground0: hex(0x060a10),
        ground1: hex(0x0a0e14),
        ground2: hex(0x10151b),
        ground3: hex(0x161a21),
        ground4: hex(0x1d2128),
        ground5: hex(0x24292f),

        text1: hex(0xe6edf3),
        text2: hex(0xa7b3c0),
        text3: hex(0x7d8a99),
        text4: hex(0x5c6773),

        hairline_quiet: hex(0x23282d),
        hairline: hex(0x2d3136),
        hairline_strong: hex(0x363a3f),
        stroke: hex(0x45484d),

        accent: hex(0x5fb9ea),
        accent_solid: hex(0x248abb),
        focus: hex(0x56baef),
        select: hex(0x11374b),

        verified: hex(0x249d6f),
        unverified: hex(0x8b8676),
        erased: hex(0xa76ab9),
        tampered: hex(0xcf5d5e),
        missing: hex(0xb47729),

        syntax_keyword: hex(0x7fb3d5),
        syntax_function: hex(0x9db8c9),
        syntax_string: hex(0xc8a973),
        syntax_number: hex(0xa8bfa0),
        syntax_comment: hex(0x7b8794),
    };

    /// Light variant with semantic hues tuned for light grounds.
    pub const LIGHT: Self = Self {
        ground0: hex(0xffffff),
        ground1: hex(0xfbfcfd),
        ground2: hex(0xf3f5f8),
        ground3: hex(0xeaeef3),
        ground4: hex(0xffffff),
        ground5: hex(0xffffff),

        text1: hex(0x11161c),
        text2: hex(0x3d4854),
        text3: hex(0x5f6b78),
        text4: hex(0x8a95a1),

        hairline_quiet: hex(0xe3e8ee),
        hairline: hex(0xd4dae2),
        hairline_strong: hex(0xc0c8d2),
        stroke: hex(0xa4aeba),

        accent: hex(0x1f7aa6),
        accent_solid: hex(0x1b6d95),
        focus: hex(0x156189),
        select: hex(0xd7e9f5),

        verified: hex(0x1a7a53),
        unverified: hex(0x8a8264),
        erased: hex(0x7d3f8e),
        tampered: hex(0xa8322f),
        missing: hex(0x8a5710),

        // Light-theme semantic ink needs greater contrast against near-white.
        syntax_keyword: hex(0x1f5c85),
        syntax_function: hex(0x40606f),
        syntax_string: hex(0x8a5c1a),
        syntax_number: hex(0x3f6b39),
        syntax_comment: hex(0x676f79),
    };

    /// Overlay that moves a surface toward its text colour.
    pub fn wash(&self, alpha: u8) -> Color32 {
        if self.is_light() {
            Color32::from_rgba_unmultiplied(0x1a, 0x2a, 0x3a, alpha / 2)
        } else {
            Color32::from_rgba_unmultiplied(0xd9, 0xe8, 0xf8, alpha)
        }
    }

    /// Low-opacity semantic tint suitable for a surface.
    pub fn tint(colour: Color32, alpha: u8) -> Color32 {
        Color32::from_rgba_unmultiplied(colour.r(), colour.g(), colour.b(), alpha)
    }

    /// Lifts a solid colour for pressed and hovered filled controls.
    pub fn lighten(colour: Color32, by: u8) -> Color32 {
        Color32::from_rgb(
            colour.r().saturating_add(by),
            colour.g().saturating_add(by),
            colour.b().saturating_add(by),
        )
    }

    /// True when the data plane is lighter than the text it carries.
    pub fn is_light(&self) -> bool {
        self.ground1.r() as u32 + self.ground1.g() as u32 + self.ground1.b() as u32 > 384
    }
}

/// What can be proven about one object, right now.
///
/// Five engine verdicts plus Studio's default `Unverified` state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Evidence {
    Verified,
    /// Nothing has been proven about this object yet. Everything starts here.
    #[default]
    Unverified,
    Erased,
    Tampered,
    Missing,
    NotAttestable,
}

impl Evidence {
    pub fn colour(self, p: &Palette) -> Option<Color32> {
        match self {
            Self::Verified => Some(p.verified),
            Self::Unverified => Some(p.unverified),
            Self::Erased => Some(p.erased),
            Self::Tampered => Some(p.tampered),
            Self::Missing => Some(p.missing),
            Self::NotAttestable => None,
        }
    }

    /// Human-readable state label; [`Evidence::scope`] carries its precision.
    pub fn label(self) -> &'static str {
        match self {
            Self::Verified => "Authentic",
            Self::Unverified => "Not checked",
            Self::Erased => "Key erased",
            Self::Tampered => "Tampered",
            Self::Missing => "Missing",
            Self::NotAttestable => "Not attestable",
        }
    }

    /// The scope line shown with every verdict. A bare verdict states more than was
    /// checked, so no surface prints one without this.
    pub fn scope(self) -> &'static str {
        match self {
            Self::Verified => "bytes and origin of this atom, checked against its key slot",
            Self::Tampered => "authentication failed against the stored MAC",
            Self::Erased => "key destroyed; ciphertext retained but unrecoverable",
            Self::Missing => "referenced by the index, absent from the store",
            Self::NotAttestable => "plaintext region: no per-atom MAC exists",
            Self::Unverified => "no proof computed in this session",
        }
    }
}

/// Complete mapping from engine verdicts; `None` becomes Studio's `Unverified` state.
impl From<Option<citadel_mem::AttestVerdict>> for Evidence {
    fn from(verdict: Option<citadel_mem::AttestVerdict>) -> Self {
        use citadel_mem::AttestVerdict as V;
        match verdict {
            None => Self::Unverified,
            Some(V::Authentic) => Self::Verified,
            Some(V::Tampered) => Self::Tampered,
            Some(V::KeyErased) => Self::Erased,
            Some(V::Missing) => Self::Missing,
            Some(V::PlaintextUnattested) => Self::NotAttestable,
        }
    }
}

/// Geometry, in logical points. Every value is a multiple of 4 except the 24/28/32 row
/// ladder and the 3pt rail.
pub mod metrics {
    pub const BAND_H: f32 = 40.0;
    pub const TAB_H: f32 = 34.0;
    pub const STATUS_H: f32 = 26.0;
    pub const TOOLBAR_H: f32 = 32.0;
    pub const SIDEBAR_W: f32 = 280.0;
    pub const SIDEBAR_RAIL_W: f32 = 48.0;
    pub const INSPECTOR_W: f32 = 420.0;

    pub const ROW_COMPACT: f32 = 24.0;
    pub const ROW_DEFAULT: f32 = 28.0;
    pub const ROW_COMFORTABLE: f32 = 32.0;
    pub const HEADER_H: f32 = 32.0;
    pub const TREE_ROW_H: f32 = 26.0;

    /// rail + gap + row number + checkbox, sticky, never scrolls horizontally.
    pub const GUTTER_W: f32 = 40.0;
    pub const RAIL_W: f32 = 3.0;

    pub const CELL_PAD_X: f32 = 8.0;
    /// Inset from a pane edge to its content.
    pub const PANE_PAD: f32 = 12.0;
    /// Inset from a card edge to its content. Larger than a pane so a card reads as a
    /// distinct object rather than a region of the pane behind it.
    pub const CARD_PAD: f32 = 20.0;
}

pub mod radius {
    pub const ROW: u8 = 0;
    pub const PILL: u8 = 4;
    pub const CONTROL: u8 = 6;
    pub const CARD: u8 = 8;
    pub const PALETTE: u8 = 12;
}

/// Install the fonts and the style for the palette in use.
///
/// Both egui themes get the same style because the palette, not egui's theme setting, is
/// what decides light or dark here: the theme is a user choice inside the application and
/// must not change again underneath it when the OS preference differs.
pub fn install(ctx: &egui::Context, p: &Palette) {
    ctx.set_fonts(crate::fonts::definitions());
    let s = style(p);
    ctx.set_style_of(egui::Theme::Dark, s.clone());
    ctx.set_style_of(egui::Theme::Light, s);
}

/// Builds the application-wide egui style.
pub fn style(p: &Palette) -> Style {
    // Preserve matching base-theme defaults for properties not overridden here.
    let mut visuals = if p.is_light() {
        Visuals::light()
    } else {
        Visuals::dark()
    };
    visuals.dark_mode = !p.is_light();

    visuals.panel_fill = p.ground1;
    visuals.window_fill = p.ground4;
    visuals.extreme_bg_color = p.ground0;
    visuals.faint_bg_color = p.ground2;
    visuals.window_stroke = Stroke::new(1.0, p.hairline_strong);
    visuals.window_corner_radius = CornerRadius::same(radius::CARD);
    visuals.selection.bg_fill = p.select;
    visuals.selection.stroke = Stroke::new(1.0, p.accent);
    visuals.hyperlink_color = p.accent;

    // Keep control geometry stable across interaction states.
    for w in [
        &mut visuals.widgets.noninteractive,
        &mut visuals.widgets.inactive,
        &mut visuals.widgets.hovered,
        &mut visuals.widgets.active,
        &mut visuals.widgets.open,
    ] {
        w.corner_radius = CornerRadius::same(radius::CONTROL);
        w.bg_stroke = Stroke::NONE;
        w.expansion = 0.0;
    }

    // Plain labels inherit the primary text tier.
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0, p.text1);
    visuals.widgets.noninteractive.bg_fill = Color32::TRANSPARENT;
    visuals.widgets.noninteractive.weak_bg_fill = Color32::TRANSPARENT;

    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0, p.text2);
    visuals.widgets.inactive.bg_fill = Color32::TRANSPARENT;
    visuals.widgets.inactive.weak_bg_fill = Color32::TRANSPARENT;

    visuals.widgets.hovered.fg_stroke = Stroke::new(1.0, p.text1);
    visuals.widgets.hovered.bg_fill = p.wash(20);
    visuals.widgets.hovered.weak_bg_fill = p.wash(20);

    visuals.widgets.active.fg_stroke = Stroke::new(1.0, p.text1);
    visuals.widgets.active.bg_fill = p.wash(41);
    visuals.widgets.active.weak_bg_fill = p.wash(41);

    let mut style = Style {
        visuals,
        ..Default::default()
    };

    // Grid rows own their vertical spacing.
    style.spacing.item_spacing = egui::vec2(8.0, 0.0);
    style.spacing.button_padding = egui::vec2(8.0, 0.0);
    style.spacing.indent = 14.0;
    style.spacing.interact_size = egui::vec2(0.0, 26.0);
    style.spacing.window_margin = egui::Margin::same(0);

    style
}
