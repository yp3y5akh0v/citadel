//! Engine answers and session evidence in the shape consumed by the UI.

use crate::engine::memory::{AtomView, RegionFacts};
use crate::theme::Evidence;

/// One region and the paged window read during this session. `facts.total` remains the
/// engine's full count.
#[derive(Clone)]
pub struct Region {
    pub facts: RegionFacts,
    pub atoms: Vec<AtomView>,
    /// Cursor supplied by the last page. `None` together with `exhausted` means no retry.
    pub next_after_id: Option<citadel_mem::AtomId>,
    pub exhausted: bool,
}

impl Region {
    pub fn new(facts: RegionFacts) -> Self {
        Self {
            facts,
            atoms: Vec::new(),
            next_after_id: None,
            exhausted: false,
        }
    }

    pub fn name(&self) -> &str {
        &self.facts.name
    }

    pub fn plaintext(&self) -> bool {
        self.facts.plaintext
    }

    /// Evidence counts over fetched rows, not the whole region.
    pub fn counts(&self) -> Counts {
        let mut c = Counts::default();
        for a in &self.atoms {
            match a.evidence() {
                Evidence::Verified => c.verified += 1,
                Evidence::Unverified => c.unverified += 1,
                Evidence::Erased => c.erased += 1,
                Evidence::Tampered => c.tampered += 1,
                Evidence::Missing => c.missing += 1,
                Evidence::NotAttestable => c.not_attestable += 1,
            }
        }
        c
    }
}

#[derive(Default, Clone, Copy)]
pub struct Counts {
    pub verified: u32,
    pub unverified: u32,
    pub erased: u32,
    pub tampered: u32,
    pub missing: u32,
    pub not_attestable: u32,
}

impl Counts {
    pub fn total(&self) -> u32 {
        self.verified
            + self.unverified
            + self.erased
            + self.tampered
            + self.missing
            + self.not_attestable
    }
}

#[derive(Clone)]
pub struct Table {
    pub name: String,
    pub rows: u64,
    /// Storage owned by the memory engine.
    pub engine_owned: bool,
}

/// The first two vector dimensions in normalized 0..1 display space.
///
/// Screen proximity is not index distance.
#[derive(Clone, Copy)]
pub struct Projected {
    pub x: f32,
    pub y: f32,
    pub evidence: Evidence,
}

/// A vector column, displayed with its qualified `table.column` name.
#[derive(Clone)]
pub struct VectorColumn {
    pub table: String,
    pub column: String,
    pub dim: u32,
    /// How many rows the column holds, against however many were sampled into `points`.
    pub total: u64,
    /// Stable normalized sample shared with the GPU callback, which may outlive a frame.
    pub points: std::sync::Arc<Vec<Projected>>,
    /// Answers "how many are in view" without walking `points` every frame.
    pub index: std::sync::Arc<crate::visible::Index>,
    pub sample: VectorSample,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum VectorSample {
    Unloaded,
    Loading,
    Ready,
    Failed(crate::engine::StudioError),
}

impl VectorColumn {
    /// The index is derived from the points, so the two cannot be set independently and
    /// fall out of step.
    pub fn new(table: &str, column: &str, dim: u32, total: u64, points: Vec<Projected>) -> Self {
        Self {
            table: table.to_owned(),
            column: column.to_owned(),
            dim,
            total,
            index: std::sync::Arc::new(crate::visible::Index::build(&points)),
            points: std::sync::Arc::new(points),
            sample: VectorSample::Ready,
        }
    }

    /// A catalog descriptor whose values have not been read from storage.
    pub fn unloaded(table: &str, column: &str, dim: u32, total: u64) -> Self {
        let points = Vec::new();
        Self {
            table: table.to_owned(),
            column: column.to_owned(),
            dim,
            total,
            index: std::sync::Arc::new(crate::visible::Index::build(&points)),
            points: std::sync::Arc::new(points),
            sample: VectorSample::Unloaded,
        }
    }

    pub fn begin_loading(&mut self) -> bool {
        if self.sample != VectorSample::Unloaded {
            return false;
        }
        self.sample = VectorSample::Loading;
        true
    }

    pub fn install_sample(&mut self, sample: &crate::engine::VectorFacts) -> bool {
        if self.table != sample.table || self.column != sample.column {
            return false;
        }
        let points: Vec<_> = sample
            .points
            .iter()
            .map(|&(x, y)| Projected {
                x,
                y,
                // SQL samples carry no per-atom proof.
                evidence: Evidence::NotAttestable,
            })
            .collect();
        self.dim = sample.dim;
        self.total = sample.total;
        self.index = std::sync::Arc::new(crate::visible::Index::build(&points));
        self.points = std::sync::Arc::new(points);
        self.sample = VectorSample::Ready;
        true
    }

    pub fn fail_sample(&mut self, error: crate::engine::StudioError) {
        self.sample = VectorSample::Failed(error);
    }

    pub fn qualified(&self) -> String {
        format!("{}.{}", self.table, self.column)
    }

    pub fn sampled(&self) -> bool {
        self.sample == VectorSample::Ready && self.points.len() as u64 != self.total
    }
}

/// The platform-native file name used in vault lists.
pub fn file_name(info: &citadel::VaultInfo) -> String {
    info.data_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| info.data_path.display().to_string())
}

/// The cipher and KDF, which live in the key file rather than the database.
pub fn cipher_label(key_file: &citadel::KeyFileInfo) -> String {
    format!(
        "{}{}{}",
        key_file.cipher.as_str(),
        crate::widgets::SEP,
        key_file.kdf.as_str()
    )
}

pub fn format_label(info: &citadel::VaultInfo) -> String {
    format!(
        "v{}{}{} B pages",
        info.format_version,
        crate::widgets::SEP,
        info.page_size
    )
}

/// Persisted summary of a recently opened vault.
#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct RecentVault {
    pub path: std::path::PathBuf,
    pub entries: u64,
    pub pages: u32,
}

impl RecentVault {
    pub fn name(&self) -> String {
        self.path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| self.path.display().to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormatSqlError {
    Empty,
    Invalid,
    Comments,
}

impl FormatSqlError {
    pub fn message(self) -> &'static str {
        match self {
            Self::Empty => "Format skipped: the query is empty",
            Self::Invalid => "Format skipped: the query could not be parsed",
            Self::Comments => "Format skipped: comments and hints must be preserved exactly",
        }
    }
}

/// Reformat SQL through the engine's own parser without silently deleting source text.
pub fn format_sql(sql: &str) -> Result<String, FormatSqlError> {
    // Leading notes and hints can be retained byte-for-byte. Once SQL has started, the AST
    // renderer cannot restore a comment to its exact source position, so refuse instead of
    // silently moving or deleting it.
    let mut prefix_end = 0;
    let mut saw_leading_comment = false;
    let mut sql_started = false;
    for (range, token) in crate::sql::tokenize(sql) {
        let text = &sql[range.clone()];
        if !sql_started {
            match token {
                crate::sql::Token::Comment => {
                    saw_leading_comment = true;
                    prefix_end = range.end;
                }
                crate::sql::Token::Plain if text.trim().is_empty() => {
                    prefix_end = range.end;
                }
                _ => sql_started = true,
            }
        } else if token == crate::sql::Token::Comment {
            return Err(FormatSqlError::Comments);
        }
    }
    let prefix_end = if saw_leading_comment { prefix_end } else { 0 };
    let (prefix, body) = sql.split_at(prefix_end);
    let statements =
        citadel_sql::dialect::parse_statements(body).map_err(|_| FormatSqlError::Invalid)?;
    if statements.is_empty() {
        return Err(FormatSqlError::Empty);
    }
    let formatted = statements
        .iter()
        .map(|s| format!("{s:#};"))
        .collect::<Vec<_>>()
        .join("\n\n");
    Ok(format!("{prefix}{formatted}"))
}

#[cfg(test)]
mod format_tests {
    use super::*;

    #[test]
    fn leading_comments_are_preserved_while_the_statement_is_formatted() {
        let source = "-- operator note\r\n\tselect   id,title  from documents where id=1";
        let formatted = format_sql(source).expect("the leading note is preservable");

        assert!(formatted.starts_with("-- operator note\r\n\t"));
        assert!(formatted.contains("SELECT"));
        assert!(formatted.contains("FROM"));
        assert!(formatted.contains("documents"));
        assert!(formatted.ends_with(';'));
        assert_ne!(formatted, source);
    }

    #[test]
    fn leading_hints_are_preserved_byte_for_byte() {
        let prefix = "/*+ keep this hint */\n\n";
        let formatted = format_sql(&format!("{prefix}select 1"))
            .expect("a leading hint has an exact source position");
        assert_eq!(&formatted[..prefix.len()], prefix);
        assert!(formatted[prefix.len()..].starts_with("SELECT"));
        assert!(formatted.ends_with("1;"));
    }

    #[test]
    fn comments_inside_or_after_a_statement_are_refused() {
        for source in [
            "SELECT /* keep */ * FROM documents;",
            "SELECT * FROM documents; -- keep",
        ] {
            assert_eq!(format_sql(source), Err(FormatSqlError::Comments));
        }
    }

    #[test]
    fn comment_markers_inside_dollar_quoted_literals_are_not_comments() {
        for source in [
            "SELECT $$-- not a comment$$ AS s;",
            "SELECT $tag$/* not a comment */$tag$ AS s;",
        ] {
            let formatted = format_sql(source).expect("the dollar-quoted value is SQL text");
            assert!(formatted.contains("not a comment"));
        }
    }

    #[test]
    fn a_comment_without_a_statement_is_empty() {
        assert_eq!(format_sql("-- only a note"), Err(FormatSqlError::Empty));
    }
}

/// Current wall-clock timestamp for session evidence.
pub fn stamp_now() -> String {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_micros() as i64)
        .unwrap_or_default();
    citadel_sql::datetime::format_timestamp(micros)
}

/// A digest as `head...tail`, which is how a reader compares two of them.
pub fn digest_label(bytes: &[u8]) -> String {
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    match bytes.len() {
        0 => "-".to_owned(),
        n if n <= 8 => hex(bytes),
        n => format!("{}...{}", hex(&bytes[..4]), hex(&bytes[n - 4..])),
    }
}

/// Header-only vault size. `entry_count` describes only the default key-value tree, so
/// pages lead and that count appears only when nonzero.
pub fn contents_label(stats: &citadel::DbStats) -> String {
    let pages = format!(
        "{} pages",
        crate::widgets::thousands(u64::from(stats.total_pages))
    );
    if stats.entry_count == 0 {
        return pages;
    }
    format!(
        "{} key-value entries{}{}",
        crate::widgets::thousands(stats.entry_count),
        crate::widgets::SEP,
        pages
    )
}

/// Five pre-open rows preserving unread, readable, and refused outcomes.
pub fn preview_rows(preview: &crate::state::Preview) -> [(&'static str, String); 5] {
    use crate::state::Preview;
    const UNREAD: &str = "not read";
    const REFUSED: &str = "could not be read";
    match preview {
        Preview::Read(info) => header_rows(info),
        Preview::Unread => [
            ("cipher", UNREAD.to_owned()),
            ("format", UNREAD.to_owned()),
            ("contents", UNREAD.to_owned()),
            ("key file", UNREAD.to_owned()),
            ("state", UNREAD.to_owned()),
        ],
        // Refusal and recovery both describe the file's current state.
        Preview::Refused(why) => [
            ("cipher", REFUSED.to_owned()),
            ("format", REFUSED.to_owned()),
            ("contents", REFUSED.to_owned()),
            ("key file", REFUSED.to_owned()),
            ("state", why.clone()),
        ],
    }
}

/// Passphrase-free header facts for the Unlock screen.
pub fn header_rows(info: &citadel::VaultInfo) -> [(&'static str, String); 5] {
    let key_file = match &info.key_file {
        citadel::KeyFileStatus::Missing => "missing".to_owned(),
        citadel::KeyFileStatus::Unreadable(error) => format!("unreadable: {error}"),
        citadel::KeyFileStatus::Invalid(error) => format!("invalid: {error}"),
        citadel::KeyFileStatus::Present {
            file_id_matches: false,
            ..
        } => "belongs to another database".to_owned(),
        citadel::KeyFileStatus::Present { info, .. } if info.rotation_active => {
            "rotation interrupted".to_owned()
        }
        citadel::KeyFileStatus::Present { info, .. } => format!("epoch {}", info.epoch),
        _ => "unknown key-file state".to_owned(),
    };
    [
        (
            "cipher",
            info.key_file
                .info()
                .map_or_else(|| "no key file".to_owned(), cipher_label),
        ),
        ("format", format_label(info)),
        ("contents", contents_label(&info.stats)),
        ("key file", key_file),
        (
            "state",
            if info.recovery_required {
                "recovers on open".to_owned()
            } else {
                format!("clean . slot {}", info.active_slot)
            },
        ),
    ]
}

#[derive(Clone)]
pub struct Vault {
    pub name: String,
    pub is_demo: bool,
    pub facts: crate::engine::VaultFacts,
    pub regions: Vec<Region>,
    pub tables: Vec<Table>,
    pub vectors: Vec<VectorColumn>,
    /// Receipts issued during this session; the engine keeps no receipt log.
    pub receipts: Vec<Receipt>,
}

/// One engine erasure receipt plus its region and issue time.
#[derive(Clone)]
pub struct Receipt {
    pub region: String,
    pub issued: String,
    pub receipt: citadel_mem::ErasureReceipt,
}

impl Receipt {
    /// Key-wrap algorithm, or an explicit plaintext row-delete description.
    pub fn algorithm(&self) -> &str {
        if self.receipt.algorithm.is_empty() {
            "row delete, no key"
        } else {
            self.receipt.algorithm
        }
    }

    /// How many atoms this receipt actually accounts for.
    pub fn atoms(&self) -> u64 {
        if self.receipt.cryptographic_erasure {
            self.receipt.erased_count
        } else {
            self.receipt.rows_deleted
        }
    }

    pub fn readback(&self) -> Readback {
        // Readback is meaningful only when a key was destroyed.
        match (
            self.receipt.cryptographic_erasure,
            self.receipt.readback_confirmed,
        ) {
            (false, _) => Readback::NotApplicable,
            (true, true) => Readback::Confirmed,
            (true, false) => Readback::Unconfirmed,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Readback {
    Confirmed,
    /// The key was destroyed but the tombstone was not read back, so the erasure is
    /// claimed and not proved.
    Unconfirmed,
    /// Plaintext region: cryptographic erasure does not apply, and saying "confirmed"
    /// would claim something the engine cannot back.
    NotApplicable,
}

impl Readback {
    pub fn label(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Unconfirmed => "not confirmed",
            Self::NotApplicable => "not applicable",
        }
    }
}

impl Vault {
    /// Evidence counts across rows fetched during this session.
    pub fn counts(&self) -> Counts {
        let mut c = Counts::default();
        for r in &self.regions {
            let rc = r.counts();
            c.verified += rc.verified;
            c.unverified += rc.unverified;
            c.erased += rc.erased;
            c.tampered += rc.tampered;
            c.missing += rc.missing;
            c.not_attestable += rc.not_attestable;
        }
        c
    }

    /// Builds a vault view; region rows arrive through subsequent page reads.
    pub fn from_engine(opened: &crate::engine::Opened) -> Self {
        Self {
            name: opened.label.clone(),
            is_demo: opened.is_demo,
            facts: opened.facts.clone(),
            regions: opened.regions.iter().cloned().map(Region::new).collect(),
            tables: opened
                .tables
                .iter()
                .map(|t| Table {
                    name: t.name.clone(),
                    rows: t.rows,
                    engine_owned: t.engine_owned,
                })
                .collect(),
            vectors: opened
                .vectors
                .iter()
                .map(|v| VectorColumn::unloaded(&v.table, &v.column, v.dim, v.total))
                .collect(),
            receipts: Vec::new(),
        }
    }

    pub fn region(&self, name: &str) -> Option<&Region> {
        self.regions.iter().find(|r| r.name() == name)
    }

    pub fn region_mut(&mut self, name: &str) -> Option<&mut Region> {
        self.regions.iter_mut().find(|r| r.name() == name)
    }
}

#[cfg(test)]
mod vector_tests {
    use super::*;

    #[test]
    fn a_vector_descriptor_retains_no_points_until_its_sample_arrives() {
        let mut column = VectorColumn::unloaded("docs", "embedding", 1_536, 10_000);
        assert_eq!(column.sample, VectorSample::Unloaded);
        assert!(column.points.is_empty());
        assert!(column.begin_loading());
        assert!(
            !column.begin_loading(),
            "one document open queues one sample"
        );

        let sample = crate::engine::VectorFacts {
            table: "docs".into(),
            column: "embedding".into(),
            dim: 1_536,
            total: 10_000,
            points: vec![(0.25, 0.75)],
        };
        assert!(column.install_sample(&sample));
        assert_eq!(column.sample, VectorSample::Ready);
        assert_eq!(column.points.len(), 1);
    }

    #[test]
    fn a_sample_for_another_column_cannot_replace_this_one() {
        let mut column = VectorColumn::unloaded("docs", "embedding", 8, 1);
        let wrong = crate::engine::VectorFacts {
            table: "docs".into(),
            column: "other".into(),
            dim: 8,
            total: 1,
            points: vec![(0.0, 1.0)],
        };

        assert!(!column.install_sample(&wrong));
        assert_eq!(column.sample, VectorSample::Unloaded);
        assert!(column.points.is_empty());
    }
}
