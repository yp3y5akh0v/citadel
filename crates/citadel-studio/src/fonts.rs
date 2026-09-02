//! Vendored Inter and JetBrains Mono faces plus system-script fallbacks. Mono numerals use
//! a fixed 0.6-em advance because epaint exposes no OpenType-feature API for `tnum`.

use egui::epaint::text::VariationCoords;
use egui::{FontData, FontDefinitions, FontFamily, FontTweak};
use std::sync::{Arc, OnceLock};

const INTER: &[u8] = include_bytes!("../assets/Inter.ttf");

/// Upstream no-ligature JetBrains Mono NL; epaint cannot disable ligatures at layout time.
const JETBRAINS_REGULAR: &[u8] = include_bytes!("../assets/JetBrainsMonoNL-Regular.ttf");
const JETBRAINS_MEDIUM: &[u8] = include_bytes!("../assets/JetBrainsMonoNL-Medium.ttf");

/// Chrome text at Medium.
pub const UI: &str = "ui";
/// Chrome text at Regular, for flowing prose and empty states only.
pub const UI_REGULAR: &str = "ui-regular";
/// Chrome text at Semibold, for headings and the active tab.
pub const UI_STRONG: &str = "ui-strong";
/// All data, all numerals, all identifiers.
pub const MONO: &str = "mono";
/// Grid column headers and other uppercase mono labels.
pub const MONO_MEDIUM: &str = "mono-medium";

/// One probe per CJK script because a single family may not cover all three.
const PROBES: [(char, &str); 3] = [
    ('\u{4E2D}', "Han"),
    ('\u{3042}', "Hiragana"),
    ('\u{D55C}', "Hangul"),
];

/// CJK families in preference order. Names only: a name is a claim, checked against the
/// actual glyph table before the face is accepted.
const CJK_FAMILIES: &[&str] = &[
    // Linux, and the best-covering of the set.
    "Noto Sans CJK SC",
    "Noto Sans CJK JP",
    "Noto Sans CJK KR",
    "Source Han Sans SC",
    // Windows.
    "Microsoft YaHei UI",
    "Microsoft YaHei",
    "Yu Gothic UI",
    "Meiryo UI",
    "Malgun Gothic",
    "SimSun",
    // macOS.
    "PingFang SC",
    "Hiragino Sans",
    "Apple SD Gothic Neo",
];

/// System faces covering as much of `PROBES` as this machine provides. CJK fonts remain
/// system-supplied to avoid adding 10-20 MB to the binary; an empty result is valid.
pub fn system_scripts() -> Vec<(String, FontData)> {
    let mut db = fontdb::Database::new();
    db.load_system_fonts();

    let mut chosen: Vec<(String, FontData)> = Vec::new();
    let mut wanted: Vec<char> = PROBES.iter().map(|(c, _)| *c).collect();

    for family in CJK_FAMILIES {
        if wanted.is_empty() {
            break;
        }
        let Some(id) = db.query(&fontdb::Query {
            families: &[fontdb::Family::Name(family)],
            ..Default::default()
        }) else {
            continue;
        };
        let taken = db.with_face_data(id, |bytes, index| {
            let face = ttf_parser::Face::parse(bytes, index).ok()?;
            // Register only faces that extend script coverage.
            let adds: Vec<char> = wanted
                .iter()
                .copied()
                .filter(|c| face.glyph_index(*c).is_some())
                .collect();
            if adds.is_empty() {
                return None;
            }
            Some((adds, bytes.to_vec(), index))
        });
        if let Some(Some((adds, bytes, index))) = taken {
            wanted.retain(|c| !adds.contains(c));
            chosen.push((
                format!("system-{}", chosen.len()),
                FontData {
                    font: std::borrow::Cow::Owned(bytes),
                    index,
                    tweak: FontTweak::default(),
                },
            ));
        }
    }
    chosen
}

/// Whether the registered fallback chain covers `c`.
pub fn covers(c: char) -> bool {
    system_scripts().iter().any(|(_, face)| {
        ttf_parser::Face::parse(&face.font, face.index)
            .ok()
            .and_then(|f| f.glyph_index(c))
            .is_some()
    })
}

fn variable_face(bytes: &'static [u8], wght: f32, opsz: Option<f32>) -> Arc<FontData> {
    let mut coords = VariationCoords::new([(b"wght", wght)]);
    if let Some(opsz) = opsz {
        coords.push(b"opsz", opsz);
    }
    Arc::new(FontData::from_static(bytes).tweak(FontTweak {
        coords,
        ..Default::default()
    }))
}

/// egui built-ins appended behind the selected application and system faces.
const FALLBACKS: [&str; 3] = ["Ubuntu-Light", "NotoEmoji-Regular", "emoji-icon-font"];

fn build_definitions() -> FontDefinitions {
    // Preserve built-in fallbacks for glyphs outside the vendored faces.
    let mut fonts = FontDefinitions::default();

    // opsz 14 selects Inter's Text design for small chrome.
    let entries: [(&str, Arc<FontData>); 5] = [
        (UI, variable_face(INTER, 500.0, Some(14.0))),
        (UI_REGULAR, variable_face(INTER, 400.0, Some(14.0))),
        (UI_STRONG, variable_face(INTER, 600.0, Some(14.0))),
        (MONO, Arc::new(FontData::from_static(JETBRAINS_REGULAR))),
        (
            MONO_MEDIUM,
            Arc::new(FontData::from_static(JETBRAINS_MEDIUM)),
        ),
    ];

    // Once, not per family: `load_system_fonts` scans directories.
    let system: Vec<String> = system_scripts()
        .into_iter()
        .map(|(name, face)| {
            fonts.font_data.insert(name.clone(), Arc::new(face));
            name
        })
        .collect();
    let with_fallbacks = |name: &str| {
        std::iter::once(name.to_owned())
            .chain(FALLBACKS.iter().map(|f| (*f).to_owned()))
            // CJK fallbacks stay behind the Latin primary face.
            .chain(system.iter().cloned())
            .collect::<Vec<_>>()
    };

    for (name, face) in entries {
        fonts.font_data.insert(name.to_owned(), face);
        fonts
            .families
            .insert(FontFamily::Name(name.into()), with_fallbacks(name));
    }

    // Still resolved for anything that does not name a family.
    fonts
        .families
        .insert(FontFamily::Proportional, with_fallbacks(UI));
    fonts
        .families
        .insert(FontFamily::Monospace, with_fallbacks(MONO));

    fonts
}

/// The immutable font catalog. Theme changes clone only maps of shared font bytes;
/// they do not rescan the operating system's font directories.
pub fn definitions() -> FontDefinitions {
    static DEFINITIONS: OnceLock<FontDefinitions> = OnceLock::new();
    DEFINITIONS.get_or_init(build_definitions).clone()
}

/// The type ramp. Nothing in the shell exceeds 16 except the two headline roles.
pub mod role {
    use super::*;
    use egui::FontId;

    fn f(size: f32, family: &str) -> FontId {
        FontId::new(size, FontFamily::Name(family.into()))
    }

    /// 20/600. Unlock and Home only.
    pub fn headline() -> FontId {
        f(20.0, UI_STRONG)
    }
    /// 16/600.
    pub fn dialog_title() -> FontId {
        f(16.0, UI_STRONG)
    }
    /// 14/600. Inspector and card headings.
    pub fn section() -> FontId {
        f(14.0, UI_STRONG)
    }
    /// 13/400. Prose and empty states.
    pub fn body() -> FontId {
        f(13.0, UI_REGULAR)
    }
    /// 13/500. Buttons, tabs, tree rows, toolbar labels.
    pub fn chrome() -> FontId {
        f(13.0, UI)
    }
    /// 13/600. The active tab.
    pub fn chrome_strong() -> FontId {
        f(13.0, UI_STRONG)
    }
    /// 11/500. Status bar, tooltips, metadata.
    pub fn meta() -> FontId {
        f(11.0, UI)
    }
    /// 11/600 uppercase. Sidebar section headers.
    pub fn section_label() -> FontId {
        f(11.0, UI_STRONG)
    }
    /// 13/400 mono. Grid cells at default and comfortable density.
    pub fn cell() -> FontId {
        f(13.0, MONO)
    }
    /// 12/400 mono. Grid cells at compact density; inspector values.
    pub fn cell_compact() -> FontId {
        f(12.0, MONO)
    }
    /// 11/500 mono uppercase. Grid column headers.
    pub fn column_header() -> FontId {
        f(11.0, MONO_MEDIUM)
    }
    /// 10/400 mono. Row-number gutter.
    pub fn gutter() -> FontId {
        f(10.0, MONO)
    }
    /// 11/400 mono. Status-bar values and any numeral in chrome.
    pub fn meta_mono() -> FontId {
        f(11.0, MONO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn sha256(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    #[test]
    fn bundled_fonts_match_the_recorded_upstream_artifacts() {
        assert_eq!(
            sha256(INTER),
            "4989b125924991b90d05b2d16e0e388c48f7d5bb8b30539bbf9c755278d0ccaf"
        );
        assert_eq!(
            sha256(JETBRAINS_REGULAR),
            "fb3b2575d7b0657359707993288f12a7360344d39387bb26050e276d61f6bd2a"
        );
        assert_eq!(
            sha256(JETBRAINS_MEDIUM),
            "44099e1efefba55637e0abbbf8dd3f526e59523345888a257bb01d39df4af74c"
        );
    }

    #[test]
    fn bundled_mono_faces_have_the_weights_the_roles_claim() {
        let regular = ttf_parser::Face::parse(JETBRAINS_REGULAR, 0).expect("regular mono face");
        let medium = ttf_parser::Face::parse(JETBRAINS_MEDIUM, 0).expect("medium mono face");

        assert_eq!(regular.weight().to_number(), 400);
        assert_eq!(medium.weight().to_number(), 500);
        assert!(!regular.is_variable());
        assert!(!medium.is_variable());

        for face in [regular, medium] {
            let version = face
                .names()
                .into_iter()
                .find(|name| name.name_id == ttf_parser::name_id::VERSION && name.is_unicode())
                .and_then(|name| name.to_string())
                .expect("mono face version");
            assert!(version.starts_with("Version 2.304;"), "{version}");
        }
    }
}
