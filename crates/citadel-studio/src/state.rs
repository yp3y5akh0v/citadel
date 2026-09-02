//! Application state and the actions that change it. Screens READ it and push `Action`s;
//! `apply` is the only place it changes.

use crate::grid::{self, Density};
use crate::model::{Receipt, Vault};
use crate::theme::Palette;

/// Opening contents of the query document.
const DEMO_QUERY: &str = "\
-- documents nearest a chosen one, by cosine distance over the stored vectors
SELECT d.id,
       d.title,
       d.embedding <=> (SELECT embedding FROM documents WHERE id = 1) AS distance
FROM documents d
WHERE d.collection = 'research'
ORDER BY distance
LIMIT 20;
";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Route {
    Home,
    Unlock,
    Create,
    Import,
    Workspace,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UnlockStage {
    Ready,
    Deriving,
    Rejected,
}

/// A passphrase held while a form is open.
///
/// The private `Zeroizing` value clears on drop. Keeping it behind this type also makes
/// the redacted `Debug` implementation apply when an action or engine command derives
/// `Debug`; `Zeroizing<String>`'s own implementation prints the string.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Passphrase(zeroize::Zeroizing<String>);

impl From<String> for Passphrase {
    fn from(value: String) -> Self {
        Self(zeroize::Zeroizing::new(value))
    }
}

impl From<&str> for Passphrase {
    fn from(value: &str) -> Self {
        value.to_owned().into()
    }
}

impl std::ops::Deref for Passphrase {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Passphrase {
    pub fn clear(&mut self) {
        zeroize::Zeroize::zeroize(&mut *self.0);
    }

    pub(crate) fn push_single_line(&mut self, value: &str) {
        if value.is_empty() {
            return;
        }
        self.reserve_secure(value.len());
        self.0.extend(
            value
                .chars()
                .map(|ch| if matches!(ch, '\r' | '\n') { ' ' } else { ch }),
        );
    }

    #[cfg(test)]
    fn pop(&mut self) -> Option<char> {
        let removed = self.chars().next_back()?;
        let next_len = self.len() - removed.len_utf8();
        const ZEROES: &str = "\0\0\0\0";
        self.0
            .replace_range(next_len.., &ZEROES[..removed.len_utf8()]);
        self.0.truncate(next_len);
        Some(removed)
    }

    fn reserve_secure(&mut self, additional: usize) {
        if self.0.capacity() - self.len() >= additional {
            return;
        }
        let required = self
            .len()
            .checked_add(additional)
            .expect("a passphrase length fits in usize");
        let capacity = required
            .checked_next_power_of_two()
            .unwrap_or(required)
            .max(64);
        let mut next = zeroize::Zeroizing::new(String::with_capacity(capacity));
        next.push_str(self);
        zeroize::Zeroize::zeroize(&mut *self.0);
        self.0 = next;
    }
}

impl std::fmt::Debug for Passphrase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Passphrase([REDACTED])")
    }
}

#[cfg(test)]
mod passphrase_tests {
    use super::{
        Action, Argon2Profile, KdfAlgorithm, NewVault, Passphrase, RotateKey, SecretField,
    };
    use crate::engine::{Command, CreateSpec};

    #[test]
    fn debug_output_never_contains_passphrase_text() {
        const CANARY: &str = "studio-debug-secret-canary";
        let passphrase = Passphrase::from(CANARY);
        let renderings = [
            format!("{passphrase:?}"),
            format!(
                "{:?}",
                Action::SetSecret(SecretField::Unlock, passphrase.clone())
            ),
            format!(
                "{:?}",
                NewVault {
                    passphrase: passphrase.clone(),
                    confirm: passphrase.clone(),
                    ..NewVault::default()
                }
            ),
            format!(
                "{:?}",
                RotateKey {
                    current: passphrase.clone(),
                    next: passphrase.clone(),
                    confirm: passphrase.clone(),
                }
            ),
            format!(
                "{:?}",
                Command::Open {
                    path: "debug.cdl".into(),
                    passphrase: passphrase.clone(),
                }
            ),
            format!(
                "{:?}",
                CreateSpec {
                    path: "debug.cdl".into(),
                    passphrase,
                    kdf: KdfAlgorithm::Argon2id,
                    profile: Argon2Profile::Iot,
                }
            ),
        ];

        for rendered in renderings {
            assert!(
                rendered.contains("REDACTED"),
                "the debug output did not exercise passphrase redaction: {rendered}"
            );
            assert!(
                !rendered.contains(CANARY),
                "a passphrase escaped through derived Debug: {rendered}"
            );
        }
    }

    #[test]
    fn clearing_and_backspacing_wipe_retired_bytes() {
        let mut cleared = Passphrase::from("clear-this-passphrase");
        let clear_ptr = cleared.0.as_ptr();
        let clear_capacity = cleared.0.capacity();
        cleared.clear();
        assert!(cleared.is_empty());
        assert_eq!(cleared.0.as_ptr(), clear_ptr);
        assert!(
            unsafe { std::slice::from_raw_parts(clear_ptr, clear_capacity) }
                .iter()
                .all(|byte| *byte == 0),
            "clear left passphrase bytes in its allocation"
        );

        let mut shortened = Passphrase::from("ending-\u{00e9}");
        let removed_at = shortened.len() - '\u{00e9}'.len_utf8();
        let shortened_ptr = shortened.0.as_ptr();
        assert_eq!(shortened.pop(), Some('\u{00e9}'));
        assert_eq!(&*shortened, "ending-");
        assert_eq!(
            unsafe {
                std::slice::from_raw_parts(shortened_ptr.add(removed_at), '\u{00e9}'.len_utf8())
            },
            &[0, 0],
            "backspace left the removed UTF-8 bytes in capacity"
        );
    }

    #[test]
    fn secret_growth_is_reserved_before_text_is_appended() {
        let mut passphrase = Passphrase::default();
        passphrase.push_single_line("first\nsecond");
        assert_eq!(&*passphrase, "first second");
        let allocation = passphrase.0.as_ptr();
        for _ in 0..16 {
            passphrase.push_single_line("x");
        }
        assert_eq!(passphrase.0.as_ptr(), allocation);
    }
}

/// Which passphrase a keystroke belongs to. The create form holds two at once.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SecretField {
    Unlock,
    New,
    Confirm,
    /// Rotation fields remain distinct so reveal state cannot cross forms.
    RotateCurrent,
    RotateNext,
    RotateConfirm,
}

/// Engine-owned KDF and cost types.
pub use citadel::{Argon2Profile, KdfAlgorithm};

/// The KDFs a new vault can be built with, in the engine's order.
pub const KDFS: [KdfAlgorithm; 2] = [KdfAlgorithm::Argon2id, KdfAlgorithm::Pbkdf2HmacSha256];

pub const PROFILES: [Argon2Profile; 3] = [
    Argon2Profile::Iot,
    Argon2Profile::Desktop,
    Argon2Profile::Server,
];

pub fn kdf_label(kdf: KdfAlgorithm) -> &'static str {
    match kdf {
        KdfAlgorithm::Argon2id => "Argon2id",
        KdfAlgorithm::Pbkdf2HmacSha256 => "PBKDF2-HMAC-SHA256",
    }
}

/// One line describing the algorithm. Numbers live in the cost row.
pub fn kdf_detail(kdf: KdfAlgorithm) -> &'static str {
    match kdf {
        KdfAlgorithm::Argon2id => "Memory-hard. The default, and what a new vault should use.",
        KdfAlgorithm::Pbkdf2HmacSha256 => {
            "No memory hardness. For FIPS-constrained deployments only."
        }
    }
}

pub fn profile_label(profile: Argon2Profile) -> &'static str {
    match profile {
        Argon2Profile::Iot => "IoT",
        Argon2Profile::Desktop => "Desktop",
        Argon2Profile::Server => "Server",
    }
}

/// Memory cost in MiB, time cost, and parallelism.
pub fn profile_costs(profile: Argon2Profile) -> (u32, u32, u32) {
    (profile.m_cost() / 1024, profile.t_cost(), profile.p_cost())
}

pub fn profile_detail(profile: Argon2Profile) -> String {
    let (m, t, p) = profile_costs(profile);
    format!("{m} MiB, {t} passes, {p} lanes")
}

/// Studio's minimum. citadel-crypto imposes none, and the screen says whose rule it is.
pub const MIN_PASSPHRASE: usize = 12;

/// The form behind `Change passphrase`.
///
/// The current passphrase is asked for because the engine needs it: the operation unwraps
/// the REK with the old key and rewraps it under the new one. There is no path that changes a
/// passphrase without proving the old one.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct RotateKey {
    pub current: Passphrase,
    pub next: Passphrase,
    pub confirm: Passphrase,
}

impl RotateKey {
    /// The first unanswered question, in the order the form asks them.
    pub fn blocker(&self) -> Option<&'static str> {
        if self.current.is_empty() {
            Some("Enter the current passphrase")
        } else if self.next.chars().count() < MIN_PASSPHRASE {
            Some("New passphrase must be at least 12 characters")
        } else if self.confirm != self.next {
            Some("The two new passphrases do not match")
        } else if self.next == self.current {
            Some("The new passphrase is the current one")
        } else {
            None
        }
    }
}

/// Rotate-key form and operation state.
#[derive(Debug, Default)]
pub enum RotateState {
    #[default]
    Closed,
    Open(Box<RotateKey>),
    Working,
    Done,
    /// The key changed, but a follow-up durability or audit step failed.
    Warning(Box<crate::engine::StudioError>),
    Failed(Box<crate::engine::StudioError>),
}

impl RotateState {
    pub fn form(&self) -> Option<&RotateKey> {
        match self {
            Self::Open(form) => Some(form),
            _ => None,
        }
    }
}

/// The form behind `Create vault`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NewVault {
    pub path: Option<std::path::PathBuf>,
    pub passphrase: Passphrase,
    pub confirm: Passphrase,
    pub kdf: KdfAlgorithm,
    pub profile: Argon2Profile,
}

impl Default for NewVault {
    fn default() -> Self {
        Self {
            path: None,
            passphrase: Passphrase::default(),
            confirm: Passphrase::default(),
            kdf: KdfAlgorithm::Argon2id,
            profile: Argon2Profile::Desktop,
        }
    }
}

impl NewVault {
    /// Return the first unmet form requirement.
    pub fn blocker(&self) -> Option<&'static str> {
        if self.path.is_none() {
            Some("Choose where to save the vault")
        } else if self.passphrase.chars().count() < MIN_PASSPHRASE {
            Some("Passphrase must be at least 12 characters")
        } else if self.confirm != self.passphrase {
            Some("The two passphrases do not match")
        } else {
            None
        }
    }
}

/// Import-source read state.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub enum Source {
    #[default]
    None,
    Reading {
        path: std::path::PathBuf,
    },
    Read {
        path: std::path::PathBuf,
        tables: Vec<crate::sqlite::SourceTable>,
    },
    Failed {
        path: std::path::PathBuf,
        error: crate::sqlite::ReadError,
    },
}

impl Source {
    pub fn path(&self) -> Option<&std::path::Path> {
        match self {
            Self::None => None,
            Self::Reading { path } | Self::Read { path, .. } | Self::Failed { path, .. } => {
                Some(path)
            }
        }
    }

    /// Return the first reason the source cannot be imported.
    pub fn blocker(&self) -> Option<&'static str> {
        match self {
            Self::None => Some("Choose a SQLite file"),
            Self::Reading { .. } => Some("Still reading that file"),
            Self::Failed { .. } => Some("That file could not be read"),
            Self::Read { tables, .. } if tables.is_empty() => Some("That database holds no tables"),
            Self::Read { tables, .. }
                if tables
                    .iter()
                    .any(|table| !table.columns.iter().any(|column| column.primary_key)) =>
            {
                Some("Every source table needs a primary key")
            }
            Self::Read { .. } => None,
        }
    }

    pub fn tables(&self) -> &[crate::sqlite::SourceTable] {
        match self {
            Self::Read { tables, .. } => tables,
            _ => &[],
        }
    }
}

/// Destination-side schema creation state. Source inspection has its own `Source::Reading`.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub enum ImportProgress {
    #[default]
    Idle,
    Running,
    Cancelling,
}

impl ImportProgress {
    pub fn busy(self) -> bool {
        !matches!(self, Self::Idle)
    }
}

/// Engine operation in flight.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Busy {
    Opening,
    Importing,
    Checking,
    Rotating,
    Verifying(String),
    Forgetting(String),
}

/// Foreground maintenance work shown in the region toolbar.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub enum Maintenance {
    #[default]
    Idle,
    Verifying,
    Forgetting,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ForgetPrompt {
    pub region: String,
    pub atom_id: citadel_mem::AtomId,
    pub plaintext: bool,
}

/// Open-vault session state.
#[derive(Debug, Default)]
pub enum SessionState {
    #[default]
    None,
    Opening,
    Open(Box<crate::engine::Opened>),
    Failed(crate::engine::StudioError),
}

/// Integrity-check state.
#[derive(Debug, Default)]
pub enum IntegrityState {
    #[default]
    None,
    Checking,
    Checked(Box<crate::engine::IntegrityFacts>),
    Failed(Box<crate::engine::StudioError>),
}

impl SessionState {
    pub fn opened(&self) -> Option<&crate::engine::Opened> {
        match self {
            Self::Open(opened) => Some(opened),
            _ => None,
        }
    }

    pub fn error(&self) -> Option<&crate::engine::StudioError> {
        match self {
            Self::Failed(error) => Some(error),
            _ => None,
        }
    }

    pub fn busy(&self) -> bool {
        matches!(self, Self::Opening)
    }
}

/// Bounded table-browser result, kept separate from user query results.
#[derive(Clone, Debug)]
pub struct Browsed {
    pub table: String,
    pub rows: Result<citadel_sql::QueryResult, crate::engine::StudioError>,
}

/// Immutable identity of one SQL submission.
///
/// Replies carry this back through the application shell, so a completion can never be
/// attached to another vault or a later editor execution merely because it arrived late.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryRequest {
    pub execution: u64,
    pub vault_revision: u64,
    pub query_revision: u64,
    pub sql: String,
}

/// Transient feedback owned by the query editor rather than an engine result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EditorNotice {
    /// An intent was refused without changing editor text.
    Unchanged(String),
    /// Formatting completed; `changed` distinguishes a rewrite from an idempotent pass.
    Formatted { changed: bool },
}

/// Last query-editor run. `Run` can contain both committed work and a later failure.
#[derive(Debug, Default)]
pub enum QueryState {
    #[default]
    None,
    Running(QueryRequest),
    Done {
        request: QueryRequest,
        run: Box<crate::engine::Run>,
    },
}

impl QueryState {
    pub fn done(&self) -> Option<&crate::engine::Run> {
        match self {
            Self::Done { run, .. } => Some(run),
            _ => None,
        }
    }

    pub fn request(&self) -> Option<&QueryRequest> {
        match self {
            Self::Running(request) | Self::Done { request, .. } => Some(request),
            Self::None => None,
        }
    }

    pub fn busy(&self) -> bool {
        matches!(self, Self::Running(_))
    }
}

/// Destructive destination waiting for explicit permission to discard an edited query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryDiscard {
    CloseVault,
    Exit,
    OpenDemoVault,
    OpenRecent(std::path::PathBuf),
    ChooseVaultToOpen,
    BeginCreate,
    BeginImport,
}

/// What a document tab shows.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Doc {
    Region(String),
    Table(String),
    Query,
    Vector(String),
    Security,
}

impl Doc {
    pub fn title(&self) -> String {
        match self {
            Self::Region(name) => format!("region: {name}"),
            Self::Table(name) => name.clone(),
            Self::Query => "Untitled query".into(),
            Self::Vector(col) => col.clone(),
            Self::Security => "Security".into(),
        }
    }

    pub fn pinned_right(&self) -> bool {
        matches!(self, Self::Security)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GridTab {
    Data,
    Structure,
    Attestation,
}

#[derive(Clone, Debug)]
pub enum Action {
    OpenDemoVault,
    /// Header facts read before passphrase entry.
    BeginUnlock(Target, Preview),
    /// A recent vault whose header still needs inspection.
    OpenRecent(std::path::PathBuf),
    SetSecret(SecretField, Passphrase),
    ToggleReveal(SecretField),
    SubmitPassphrase,
    CancelUnlock,
    BeginCreate,
    SubmitCreate,
    FormatQuery,
    BeginQueryComposition,
    CommitQueryComposition,
    CancelQueryComposition,
    QueryNotice(String),
    RunImport,
    /// Plan and execute the editor contents to collect measured statistics.
    AnalyzeQuery,
    /// Trip the cancel flag for cancellable engine work in flight.
    ///
    /// Consumed by the dispatcher like an engine intent, but it sends no command: the
    /// actor is inside the query or integrity walk, so anything queued behind it arrives
    /// too late. The dispatcher trips the shared token instead, and the engine answers
    /// through the ordinary reply channel with `Kind::Cancelled`.
    CancelQuery,
    CheckIntegrity,
    IntegrityChecked(Box<Result<crate::engine::IntegrityFacts, crate::engine::StudioError>>),
    BeginRotate,
    CancelRotate,
    SubmitRotate,
    PassphraseChanged(Box<crate::engine::Mutation<()>>),
    CancelCreate,
    SetCreatePath(std::path::PathBuf),
    SetKdf(KdfAlgorithm),
    SetArgon2Profile(Argon2Profile),
    BeginImport,
    CancelImport,
    /// Cancel destination schema creation without leaving the import screen.
    CancelImportRun,
    ImportCancelRequested,
    SetImportSource(Source),
    /// File-dialog intents consumed by `effects`.
    ChooseVaultToOpen,
    ChooseVaultDestination,
    ChooseImportSource,
    /// Ask the application shell to close, preserving the edited-query prompt.
    RequestExit,
    CloseVault,
    OpenDoc(Doc),
    FocusDoc(usize),
    CloseDoc(usize),
    SelectRow(usize),
    MoveSelection(i32),
    SelectFirst,
    SelectLast,
    ScrollTo(usize),
    ReportWindow {
        visible: usize,
        total: usize,
    },
    VerifyPage,
    BeginForgetSelected,
    CancelForget,
    ConfirmForget,
    SetGridTab(GridTab),
    CycleDensity,
    ToggleTheme,
    ShowAbout,
    CloseAbout,
    EditQuery(String),
    MoveCamera(Camera),
    ResetCamera,
    SelectPoint(Option<usize>),
    RunQuery,
    ExplainQuery,
    QueryStarted(QueryRequest),
    /// The engine actor exited. Any displayed vault belonged to that actor and is no
    /// longer a usable session.
    EngineStopped(crate::engine::StudioError),
    EngineBusy(Busy),
    QueryFinished {
        request: QueryRequest,
        run: Box<crate::engine::Run>,
        refreshed: Option<Box<Result<crate::engine::Opened, crate::engine::StudioError>>>,
    },
    ImportFinished(Box<crate::engine::Mutation<()>>),
    ExplainFinished {
        request: QueryRequest,
        plan: Box<Result<crate::engine::QueryPlan, crate::engine::StudioError>>,
        refreshed: Option<Box<Result<crate::engine::Opened, crate::engine::StudioError>>>,
    },
    PromptDiscardQuery(QueryDiscard),
    CancelDiscardQuery,
    ConfirmDiscardQuery,
    VaultOpened(Box<Result<crate::engine::Opened, crate::engine::StudioError>>),
    VaultClosed,
    TableBrowsed(Box<Browsed>),
    BeginVectorSample {
        vault_revision: u64,
        table: String,
        column: String,
    },
    VectorSampled {
        vault_revision: u64,
        table: String,
        column: String,
        sample: Box<Result<crate::engine::VectorFacts, crate::engine::StudioError>>,
    },
    AtomsLoaded {
        region: String,
        page: Box<Result<crate::engine::memory::AtomPage, crate::engine::StudioError>>,
    },
    PageVerified {
        region: String,
        /// Exact ids sent to the engine, retained even if a malformed reply omits one.
        asked: Vec<citadel_mem::AtomId>,
        verdicts: Box<Result<Vec<citadel_mem::AtomAttestation>, crate::engine::StudioError>>,
    },
    Forgotten {
        region: String,
        /// What was asked for. A plaintext region destroys no keys, so `slots_erased` is
        /// empty there and the receipt alone cannot say which rows went.
        asked: Vec<citadel_mem::AtomId>,
        mutation: Box<crate::engine::Mutation<citadel_mem::ErasureReceipt>>,
    },
    TogglePalette,
    PaletteChar(char),
    PaletteBackspace,
    PaletteMove(i32),
    PaletteRun,
    /// Absolute scroll offset, clamped by the pane that knows its content height.
    ScrollPane(f32),
}

/// One command-palette entry.
pub struct Command {
    pub title: String,
    pub group: Group,
    pub hint: String,
    pub action: Action,
}

/// Palette sections, in the order they are offered.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Group {
    GoTo,
    Proof,
    Vault,
    View,
}

impl Group {
    pub fn label(self) -> &'static str {
        match self {
            Self::GoTo => "Go to",
            Self::Proof => "Proof",
            Self::Vault => "Vault",
            Self::View => "View",
        }
    }
}

/// Label a shortcut using the platform modifier represented by `egui::Modifiers::command`.
pub fn shortcut(keys: &str) -> String {
    if cfg!(target_os = "macos") {
        keys.strip_prefix("Shift ")
            .map_or_else(|| format!("⌘{keys}"), |key| format!("⇧⌘{key}"))
    } else {
        format!("Ctrl {keys}")
    }
}

/// Everything reachable from the palette, given what is open right now.
pub fn commands(state: &State) -> Vec<Command> {
    let mut out = Vec::new();
    let mut push = |title: String, group: Group, hint: String, action: Action| {
        out.push(Command {
            title,
            group,
            hint,
            action,
        });
    };

    if state.vault_navigation_available() {
        let vault = state.vault.as_ref().expect("availability requires a vault");
        let workspace_shortcuts = state.route == Route::Workspace;
        for region in &vault.regions {
            push(
                format!("Open region: {}", region.name()),
                Group::GoTo,
                format!("{}d {}", region.facts.dim, region.facts.metric_label()),
                Action::OpenDoc(Doc::Region(region.name().to_owned())),
            );
        }
        for table in &vault.tables {
            push(
                format!("Open table: {}", table.name),
                Group::GoTo,
                format!("{} rows", crate::widgets::thousands(table.rows)),
                Action::OpenDoc(Doc::Table(table.name.clone())),
            );
        }
        push(
            "Open Security".into(),
            Group::GoTo,
            if workspace_shortcuts {
                shortcut("I")
            } else {
                "vault document".into()
            },
            Action::OpenDoc(Doc::Security),
        );
        push(
            "Open query editor".into(),
            Group::GoTo,
            if workspace_shortcuts {
                shortcut("N")
            } else {
                "vault document".into()
            },
            Action::OpenDoc(Doc::Query),
        );
        if state.verify_rows_blocker().is_none() {
            push(
                "Verify visible rows".into(),
                Group::Proof,
                "authenticates only what is on screen".into(),
                Action::VerifyPage,
            );
        }
        if state.forget_row_blocker().is_none() {
            let hint = if state
                .visible_region()
                .is_some_and(|region| region.plaintext())
            {
                "deletes the selected plaintext row"
            } else {
                "destroys its key, issues a receipt"
            };
            push(
                "Forget the selected atom".into(),
                Group::Proof,
                hint.into(),
                Action::BeginForgetSelected,
            );
        }
        if state.close_vault_blocker().is_none() {
            push(
                "Close vault".into(),
                Group::Vault,
                shortcut("Shift W"),
                Action::CloseVault,
            );
        }
    } else if state.vault.is_none() && state.route == Route::Home {
        push(
            "Open disposable demo".into(),
            Group::Vault,
            shortcut("D"),
            Action::OpenDemoVault,
        );
        // Only with nothing open: leaving the form returns Home, which from a workspace
        // would close a vault nobody asked to close.
        push(
            "Create vault".into(),
            Group::Vault,
            "choose a passphrase and a KDF".into(),
            Action::BeginCreate,
        );
    }
    push(
        "Toggle theme".into(),
        Group::View,
        if state.route == Route::Workspace {
            shortcut("L")
        } else {
            "light / dark".into()
        },
        Action::ToggleTheme,
    );
    push(
        "Cycle density".into(),
        Group::View,
        "compact, default, comfortable".into(),
        Action::CycleDensity,
    );

    let q = state.palette_query.trim().to_lowercase();
    if q.is_empty() {
        return out;
    }
    // Subsequence match, so "orsc" finds "Open region: scratch".
    out.retain(|c| subsequence(&q, &c.title.to_lowercase()));
    out
}

fn subsequence(needle: &str, haystack: &str) -> bool {
    let mut chars = haystack.chars();
    needle.chars().all(|want| chars.any(|have| have == want))
}

pub struct State {
    pub palette: Palette,
    pub light: bool,
    pub density: Density,
    pub route: Route,
    pub vault: Option<Vault>,
    /// Session-local integrity result.
    pub integrity: IntegrityState,
    pub rotate: RotateState,
    pub preview: Preview,
    /// Unattested summaries offered on the home screen.
    pub recent: Vec<crate::model::RecentVault>,
    pub target: Target,
    /// SQL owned by the model rather than a widget.
    pub query: String,
    /// Last fully committed editor text. IME pre-edit text may be visible in `query`, but
    /// engine commands are blocked until it is committed or cancelled.
    pub query_committed: String,
    /// Text installed with the vault, used to detect an unsaved ephemeral draft.
    pub query_baseline: String,
    pub query_dirty: bool,
    pub query_composing: bool,
    pub query_notice: Option<EditorNotice>,
    pub query_discard: Option<QueryDiscard>,
    /// Transient application-information dialog.
    pub about_open: bool,
    /// Changes only when the visible editor document changes.
    pub query_revision: u64,
    /// Changes only when a vault session is replaced, isolating egui widget state.
    pub query_editor_revision: u64,
    pub next_query_execution: u64,
    pub camera: Camera,
    pub selected_point: Option<usize>,
    pub renderer: Vec<String>,
    pub passphrase: Passphrase,
    pub unlock: UnlockStage,
    pub new_vault: NewVault,
    pub source: Source,
    pub import_progress: ImportProgress,
    /// A destination-side import failure. Kept apart from a source read failure because
    /// one describes the SQLite file and the other describes the open vault.
    pub import_error: Option<crate::engine::StudioError>,
    /// An open or create failure that left the previous engine session intact.
    pub open_error: Option<crate::engine::StudioError>,
    /// A verification, page-read, or erasure failure against the still-open vault.
    pub maintenance_error: Option<crate::engine::StudioError>,
    pub maintenance: Maintenance,
    /// Region that owns the foreground maintenance status or its last failure.
    pub maintenance_region: Option<String>,
    pub forget_prompt: Option<ForgetPrompt>,
    /// At most one secret is revealed at a time.
    pub revealed: Option<SecretField>,
    pub docs: Vec<Doc>,
    pub active: usize,
    pub selected_row: usize,
    pub scroll_row: usize,
    pub grid_tab: GridTab,
    pub page_checked: PageCheck,
    pub next_receipt: u32,
    pub palette_open: bool,
    pub palette_query: String,
    pub palette_index: usize,
    pub fixture_row_count: usize,
    /// Security-pane scroll offset in points.
    pub pane_scroll: f32,
    /// Number of rows visible during the last grid frame.
    pub visible_rows: usize,
    /// Physical pixels per point, used to snap the evidence rail.
    pub ppp: f32,
    pub session: SessionState,
    pub result: QueryState,
    /// The last plan `EXPLAIN` returned, and whether producing it ran the statement.
    pub plan: Option<crate::engine::QueryPlan>,
    pub plan_request: Option<QueryRequest>,
    pub browse: Option<Browsed>,
    /// Changes whenever the vault model or its stored data is replaced. GPU and browse
    /// caches include it so two vaults with the same table names cannot share bytes.
    pub vault_revision: u64,
}

/// Vector-canvas camera in normalized display space.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Camera {
    pub centre: egui::Vec2,
    pub zoom: f32,
}

impl Default for Camera {
    fn default() -> Self {
        Self {
            centre: egui::vec2(0.5, 0.5),
            zoom: 1.0,
        }
    }
}

impl Camera {
    pub const MIN_ZOOM: f32 = 1.0;
    pub const MAX_ZOOM: f32 = 64.0;

    pub fn to_screen(&self, rect: egui::Rect, p: egui::Vec2) -> egui::Pos2 {
        let span = rect.size() * self.zoom;
        rect.center() + (p - self.centre) * span
    }

    pub fn to_projection(&self, rect: egui::Rect, at: egui::Pos2) -> egui::Vec2 {
        let span = rect.size() * self.zoom;
        self.centre + (at - rect.center()) / span
    }

    /// Zoom while keeping the pointer's projection-space anchor fixed.
    pub fn zoomed_at(mut self, rect: egui::Rect, at: egui::Pos2, factor: f32) -> Self {
        let anchor = self.to_projection(rect, at);
        self.zoom = (self.zoom * factor).clamp(Self::MIN_ZOOM, Self::MAX_ZOOM);
        let after = self.to_projection(rect, at);
        self.centre += anchor - after;
        self.clamped()
    }

    /// Clamp the camera so the projection continues to cover the view.
    pub fn clamped(mut self) -> Self {
        let half = 0.5 / self.zoom;
        let (lo, hi) = (half, 1.0 - half);
        // At zoom 1 the projection exactly fills the view: lo > hi, and the only valid
        // centre is the middle. This clamp order pins that case.
        self.centre.x = self.centre.x.clamp(lo.min(hi), hi.max(lo));
        self.centre.y = self.centre.y.clamp(lo.min(hi), hi.max(lo));
        self
    }
}

/// Pre-open inspection result, distinguishing unread input from an explicit refusal.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub enum Preview {
    #[default]
    Unread,
    Read(Box<citadel::VaultInfo>),
    Refused(String),
}

impl Preview {
    pub fn read(&self) -> Option<&citadel::VaultInfo> {
        match self {
            Self::Read(info) => Some(info),
            _ => None,
        }
    }
}

/// Vault target used by the unlock screen.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub enum Target {
    #[default]
    None,
    Picked(std::path::PathBuf),
}

impl Target {
    /// Resolve the selected target, falling back to the path of an opened session.
    pub fn path(&self, opened: Option<&std::path::Path>) -> String {
        match self {
            Self::None => opened
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "no vault selected".to_owned()),
            Self::Picked(p) => p.display().to_string(),
        }
    }
}

/// Preferences safe to restore without reviving session evidence.
#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct Prefs {
    pub light: bool,
    pub density: Density,
    /// `default` preserves preference files written before recent vaults were stored.
    #[serde(default)]
    pub recent: Vec<crate::model::RecentVault>,
}

/// Maximum recent vaults in Home's fixed-height list.
pub const RECENT_LIMIT: usize = 8;

impl Default for Prefs {
    fn default() -> Self {
        Self {
            light: false,
            density: Density::Default,
            recent: Vec::new(),
        }
    }
}

impl Prefs {
    pub fn of(state: &State) -> Self {
        Self {
            light: state.light,
            density: state.density,
            recent: state.recent.clone(),
        }
    }

    pub fn apply(self, state: &mut State) {
        state.light = self.light;
        state.palette = if self.light {
            Palette::LIGHT
        } else {
            Palette::DARK
        };
        state.density = self.density;
        state.recent = self
            .recent
            .into_iter()
            .filter(|r| r.path.is_file())
            .take(RECENT_LIMIT)
            .collect();
    }
}

/// Snapshot of the visible page covered by the last verification.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct PageCheck {
    pub authentic: u32,
    pub tampered: u32,
    pub missing: u32,
    pub erased: u32,
    pub unattestable: u32,
    pub scope: u32,
    pub region: Option<String>,
    pub atom_ids: Vec<citadel_mem::AtomId>,
}

impl PageCheck {
    pub fn belongs_to(&self, showing: Option<&Doc>, visible_ids: &[citadel_mem::AtomId]) -> bool {
        matches!(
            (showing, self.region.as_deref()),
            (Some(Doc::Region(open)), Some(checked)) if open == checked
        ) && self.atom_ids == visible_ids
    }

    /// Compact session result for the status bar.
    pub fn label(&self, showing: Option<&Doc>, visible_ids: &[citadel_mem::AtomId]) -> String {
        if !self.belongs_to(showing, visible_ids) {
            return "no visible rows verified".to_owned();
        }
        if self.authentic == self.scope {
            return format!("{} visible rows authentic", self.authentic);
        }

        let mut parts = Vec::new();
        let mut push = |count: u32, label: &str| {
            if count > 0 {
                parts.push(format!("{count} {label}"));
            }
        };
        push(self.authentic, "authentic");
        push(self.tampered, "tampered");
        push(self.missing, "missing");
        push(self.erased, "key erased");
        push(self.unattestable, "not attestable");
        let reported =
            self.authentic + self.tampered + self.missing + self.erased + self.unattestable;
        push(self.scope.saturating_sub(reported), "without a result");
        parts.join(" · ")
    }
}

impl Default for State {
    fn default() -> Self {
        Self {
            palette: Palette::DARK,
            light: false,
            density: Density::Default,
            route: Route::Home,
            vault: None,
            integrity: IntegrityState::None,
            rotate: RotateState::Closed,
            preview: Preview::Unread,
            recent: Vec::new(),
            target: Target::None,
            query: DEMO_QUERY.to_owned(),
            query_committed: DEMO_QUERY.to_owned(),
            query_baseline: DEMO_QUERY.to_owned(),
            query_dirty: false,
            query_composing: false,
            query_notice: None,
            query_discard: None,
            about_open: false,
            query_revision: 0,
            query_editor_revision: 0,
            next_query_execution: 0,
            camera: Camera::default(),
            selected_point: None,
            renderer: vec!["renderer not reported".to_owned()],
            passphrase: Passphrase::default(),
            unlock: UnlockStage::Ready,
            new_vault: NewVault::default(),
            source: Source::default(),
            import_progress: ImportProgress::Idle,
            import_error: None,
            open_error: None,
            maintenance_error: None,
            maintenance: Maintenance::Idle,
            maintenance_region: None,
            forget_prompt: None,
            revealed: None,
            docs: Vec::new(),
            active: 0,
            selected_row: 1,
            scroll_row: 0,
            grid_tab: GridTab::Data,
            page_checked: PageCheck::default(),
            next_receipt: 42,
            palette_open: false,
            palette_query: String::new(),
            palette_index: 0,
            fixture_row_count: 0,
            pane_scroll: 0.0,
            visible_rows: 0,
            ppp: 1.0,
            session: SessionState::None,
            result: QueryState::None,
            plan: None,
            plan_request: None,
            browse: None,
            vault_revision: 0,
        }
    }
}

impl State {
    pub fn active_doc(&self) -> Option<&Doc> {
        self.docs.get(self.active)
    }

    pub fn query_editor_id_for(revision: u64) -> egui::Id {
        egui::Id::new(("sql_editor", revision))
    }

    pub fn query_editor_id(&self) -> egui::Id {
        Self::query_editor_id_for(self.query_editor_revision)
    }

    pub fn query_scroll_salt(&self) -> (&'static str, u64) {
        ("sql_scroll", self.query_editor_revision)
    }

    pub fn next_query_request(&self) -> QueryRequest {
        QueryRequest {
            execution: self.next_query_execution.wrapping_add(1),
            vault_revision: self.vault_revision,
            query_revision: self.query_revision,
            sql: self.query.clone(),
        }
    }

    pub fn query_outcome_stale(&self) -> bool {
        self.result
            .request()
            .or(self.plan_request.as_ref())
            .is_some_and(|request| {
                request.query_revision != self.query_revision || request.sql != self.query
            })
    }

    pub fn query_blocker(&self) -> Option<&'static str> {
        if self.query_composing {
            Some("Finish or cancel text composition before running SQL")
        } else {
            self.foreground_blocker()
        }
    }

    fn commit_query_text(&mut self) {
        if self.query_committed != self.query {
            self.query_committed.clone_from(&self.query);
            self.query_revision = self.query_revision.wrapping_add(1);
        }
        self.query_dirty = self.query_committed != self.query_baseline;
    }

    fn mark_query_clean(&mut self) {
        self.query_committed.clone_from(&self.query);
        self.query_baseline.clone_from(&self.query);
        self.query_dirty = false;
        self.query_composing = false;
        self.query_notice = None;
    }

    pub(crate) fn discard_query_edits(&mut self) {
        if self.query != self.query_baseline {
            self.query.clone_from(&self.query_baseline);
            self.query_revision = self.query_revision.wrapping_add(1);
        }
        self.query_committed.clone_from(&self.query);
        self.query_dirty = false;
        self.query_composing = false;
        self.query_notice = None;
        self.query_discard = None;
    }

    fn accepts_query_reply(&self, request: &QueryRequest) -> bool {
        request.vault_revision == self.vault_revision
            && self
                .result
                .request()
                .is_some_and(|running| running == request && self.result.busy())
    }

    pub fn opening_error(&self) -> Option<&crate::engine::StudioError> {
        self.open_error.as_ref().or_else(|| self.session.error())
    }

    /// Install a vault after clearing all state scoped to the previous vault.
    fn open_vault(&mut self, vault: Vault) {
        let first = Self::first_doc(&vault);
        self.clear_vault_scope();
        self.vault = Some(vault);
        self.route = Route::Workspace;
        self.docs = vec![first];
        self.active = 0;
    }

    /// Keep the persisted recent list authoritative and most-recent-first.
    fn remember_opened(&mut self, opened: &crate::engine::Opened) {
        if opened.is_demo {
            return;
        }
        self.recent.retain(|recent| recent.path != opened.path);
        self.recent.insert(
            0,
            crate::model::RecentVault {
                path: opened.path.clone(),
                entries: opened.facts.stats.entry_count,
                pages: opened.facts.stats.total_pages,
            },
        );
        self.recent.truncate(RECENT_LIMIT);
    }

    fn clear_vault_scope(&mut self) {
        self.vault = None;
        self.vault_revision = self.vault_revision.wrapping_add(1);
        self.query_editor_revision = self.query_editor_revision.wrapping_add(1);
        self.session = SessionState::None;
        self.integrity = IntegrityState::None;
        self.rotate = RotateState::Closed;
        self.result = QueryState::None;
        self.plan = None;
        self.plan_request = None;
        self.browse = None;
        self.docs.clear();
        self.active = 0;
        self.query.clear();
        self.query_committed.clear();
        self.query_baseline.clear();
        self.query_dirty = false;
        self.query_composing = false;
        self.query_notice = None;
        self.query_discard = None;
        self.query_revision = self.query_revision.wrapping_add(1);
        self.selected_row = 0;
        self.scroll_row = 0;
        self.pane_scroll = 0.0;
        self.page_checked = PageCheck::default();
        self.camera = Camera::default();
        self.selected_point = None;
        self.grid_tab = GridTab::Data;
        self.fixture_row_count = 0;
        self.visible_rows = 0;
        self.revealed = None;
        self.passphrase.clear();
        self.unlock = UnlockStage::Ready;
        self.target = Target::None;
        self.preview = Preview::Unread;
        self.new_vault = NewVault::default();
        self.source = Source::default();
        self.import_progress = ImportProgress::Idle;
        self.import_error = None;
        self.open_error = None;
        self.maintenance_error = None;
        self.maintenance = Maintenance::Idle;
        self.maintenance_region = None;
        self.forget_prompt = None;
        self.palette_open = false;
        self.palette_query.clear();
        self.palette_index = 0;
    }

    /// Refresh facts after a committed mutation without discarding the query result that
    /// caused it. Cached rows, projections and proofs describe the old root and are reset.
    fn refresh_vault(&mut self, opened: &crate::engine::Opened) {
        let active_doc = self.active_doc().cloned();
        let receipts = self
            .vault
            .as_mut()
            .map(|vault| std::mem::take(&mut vault.receipts))
            .unwrap_or_default();
        let mut vault = Vault::from_engine(opened);
        vault.receipts = receipts;
        self.docs.retain(|doc| Self::doc_exists(&vault, doc));
        if self.docs.is_empty() {
            self.docs.push(Self::first_doc(&vault));
        }
        self.active = active_doc
            .and_then(|active| self.docs.iter().position(|doc| doc == &active))
            .unwrap_or_else(|| self.active.min(self.docs.len().saturating_sub(1)));
        self.vault = Some(vault);
        self.vault_revision = self.vault_revision.wrapping_add(1);
        self.browse = None;
        self.integrity = IntegrityState::None;
        self.page_checked = PageCheck::default();
        self.selected_row = 0;
        self.scroll_row = 0;
        self.pane_scroll = 0.0;
        self.fixture_row_count = 0;
        self.visible_rows = 0;
        self.camera = Camera::default();
        self.selected_point = None;
        self.maintenance_error = None;
        self.maintenance = Maintenance::Idle;
        self.maintenance_region = None;
        self.forget_prompt = None;
        self.session = SessionState::Open(Box::new(opened.clone()));
        self.remember_opened(opened);
    }

    /// Passphrase rotation changes authenticated vault facts but not table or row data.
    fn refresh_security_facts(&mut self, opened: &crate::engine::Opened) {
        if let Some(vault) = &mut self.vault {
            vault.facts = opened.facts.clone();
        }
        self.session = SessionState::Open(Box::new(opened.clone()));
    }

    fn first_doc(vault: &Vault) -> Doc {
        vault
            .regions
            .first()
            .map(|region| Doc::Region(region.name().to_owned()))
            .or_else(|| {
                vault
                    .tables
                    .first()
                    .map(|table| Doc::Table(table.name.clone()))
            })
            .unwrap_or(Doc::Query)
    }

    fn doc_exists(vault: &Vault, doc: &Doc) -> bool {
        match doc {
            Doc::Region(name) => vault.region(name).is_some(),
            Doc::Table(name) => vault.tables.iter().any(|table| table.name == *name),
            Doc::Vector(column) => vault
                .vectors
                .iter()
                .any(|vector| vector.qualified() == *column),
            Doc::Query | Doc::Security => true,
        }
    }

    pub fn scroll(&self) -> grid::Scroll {
        grid::Scroll {
            density: self.density,
            selected: self.selected_row,
            first: self.scroll_row,
            modal: self.palette_open
                || self.about_open
                || self.forget_prompt.is_some()
                || self.query_discard.is_some(),
        }
    }

    pub fn foreground_blocker(&self) -> Option<&'static str> {
        if self.result.busy() {
            Some("Wait for the SQL operation to finish")
        } else if self.unlock == UnlockStage::Deriving {
            Some("Wait for the vault operation to finish")
        } else if matches!(self.integrity, IntegrityState::Checking) {
            Some("Wait for the vault integrity check to finish")
        } else if matches!(self.rotate, RotateState::Working) {
            Some("Wait for the passphrase change to finish")
        } else {
            match self.maintenance {
                Maintenance::Verifying => Some("Wait for visible-row verification to finish"),
                Maintenance::Forgetting => Some("Wait for atom erasure to finish"),
                Maintenance::Idle => None,
            }
        }
    }

    /// Whether opening a vault document can leave no transient workflow behind.
    pub fn vault_navigation_available(&self) -> bool {
        self.vault.is_some() && matches!(self.route, Route::Home | Route::Workspace)
    }

    /// Shared close policy for the menu, shortcut, command palette, and dispatcher.
    pub fn close_vault_blocker(&self) -> Option<&'static str> {
        if self.vault.is_none() {
            return Some("No vault is open");
        }
        if !matches!(self.route, Route::Home | Route::Workspace) {
            return Some("Finish or cancel the current workflow first");
        }
        if matches!(self.source, Source::Reading { .. }) {
            return Some("Wait for the SQLite source read to finish");
        }
        self.foreground_blocker()
    }

    pub fn vault(&self) -> &Vault {
        self.vault
            .as_ref()
            .expect("a workspace surface rendered with no vault open")
    }

    /// The loaded window of atoms for the focused region, not the full region.
    pub fn visible_atoms(&self) -> Option<&[crate::engine::memory::AtomView]> {
        let (Some(vault), Some(Doc::Region(name))) = (&self.vault, self.active_doc()) else {
            return None;
        };
        vault.region(name).map(|r| r.atoms.as_slice())
    }

    pub fn visible_region(&self) -> Option<&crate::model::Region> {
        let (Some(vault), Some(Doc::Region(name))) = (&self.vault, self.active_doc()) else {
            return None;
        };
        vault.region(name)
    }

    /// Exact atom window currently painted for the focused region.
    pub fn visible_atom_ids(&self) -> Option<(String, Vec<citadel_mem::AtomId>)> {
        let region = self.visible_region()?;
        let ids = region
            .atoms
            .iter()
            .skip(self.scroll_row)
            .take(self.visible_rows)
            .map(|atom| atom.id)
            .collect();
        Some((region.name().to_owned(), ids))
    }

    /// Shared verification precondition used by toolbar, palette and dispatcher.
    pub fn verify_rows_blocker(&self) -> Option<&'static str> {
        if let Some(reason) = self.foreground_blocker() {
            return Some(reason);
        }
        if self.grid_tab == GridTab::Structure {
            return Some("Structure shows no rows to verify");
        }
        let Some(region) = self.visible_region() else {
            return Some("Open a memory region to verify rows");
        };
        if region.facts.plaintext {
            return Some("Plaintext rows have no per-atom proof to verify");
        }
        if self
            .visible_atom_ids()
            .is_none_or(|(_, ids)| ids.is_empty())
        {
            return Some("No rows are visible to verify");
        }
        None
    }

    /// Shared erasure precondition used by toolbar, palette and reducer.
    pub fn forget_row_blocker(&self) -> Option<&'static str> {
        if let Some(reason) = self.foreground_blocker() {
            return Some(reason);
        }
        if self.grid_tab == GridTab::Structure {
            return Some("Structure shows no row to forget");
        }
        let Some(region) = self.visible_region() else {
            return Some("Open a memory region to forget an atom");
        };
        let Some(atom) = region.atoms.get(self.selected_row) else {
            return Some("No row is selected");
        };
        let visible_end = self
            .scroll_row
            .saturating_add(self.visible_rows)
            .min(region.atoms.len());
        if self.selected_row < self.scroll_row || self.selected_row >= visible_end {
            return Some("Select a visible row to forget");
        }
        if atom.immutable {
            return Some("This atom is marked immutable");
        }
        if atom.evidence() == crate::theme::Evidence::Erased {
            return Some("This atom's key is already destroyed");
        }
        None
    }

    pub fn page_check_belongs_to_visible_window(&self) -> bool {
        let ids = self
            .visible_atom_ids()
            .map_or_else(Vec::new, |(_, ids)| ids);
        self.page_checked.belongs_to(self.active_doc(), &ids)
    }

    pub fn page_check_label(&self) -> String {
        let ids = self
            .visible_atom_ids()
            .map_or_else(Vec::new, |(_, ids)| ids);
        self.page_checked.label(self.active_doc(), &ids)
    }

    pub fn visible_row_count(&self) -> usize {
        match self.active_doc() {
            Some(Doc::Region(_)) => self.visible_atoms().expect("region has atoms").len(),
            Some(Doc::Table(_) | Doc::Query) => self.fixture_row_count,
            _ => 0,
        }
    }

    fn open_or_focus(&mut self, doc: Doc) {
        if let Some(i) = self.docs.iter().position(|d| *d == doc) {
            self.active = i;
        } else {
            let insert_at = if doc.pinned_right() {
                self.docs.len()
            } else {
                self.docs
                    .iter()
                    .position(|d| d.pinned_right())
                    .unwrap_or(self.docs.len())
            };
            self.docs.insert(insert_at, doc);
            self.active = insert_at;
        }
        self.route = Route::Workspace;
        self.selected_row = 0;
        self.scroll_row = 0;
    }
}

pub fn apply(state: &mut State, action: Action) {
    match action {
        // Effects must resolve these intents before the pure reducer.
        Action::ChooseVaultToOpen
        | Action::ChooseVaultDestination
        | Action::ChooseImportSource
        | Action::OpenRecent(_)
        | Action::RequestExit => {
            debug_assert!(
                false,
                "a dialog intent reached apply; effects must consume it"
            );
        }
        // Engine dispatch must consume these intents before the pure reducer.
        Action::OpenDemoVault
        | Action::SubmitPassphrase
        | Action::SubmitCreate
        | Action::CheckIntegrity
        | Action::RunImport
        | Action::CancelImportRun
        | Action::CancelQuery
        | Action::SubmitRotate
        | Action::RunQuery
        | Action::ExplainQuery
        | Action::AnalyzeQuery
        | Action::VerifyPage
        | Action::ConfirmForget
        | Action::CloseVault
        | Action::ConfirmDiscardQuery => {
            debug_assert!(
                false,
                "an engine intent reached apply; the dispatcher must consume it"
            );
        }
        Action::EngineBusy(what) => match what {
            Busy::Opening => {
                state.open_error = None;
                if state.vault.is_none() {
                    state.session = SessionState::Opening;
                }
                state.unlock = UnlockStage::Deriving;
            }
            Busy::Importing => {
                state.import_error = None;
                state.import_progress = ImportProgress::Running;
            }
            Busy::Checking => state.integrity = IntegrityState::Checking,
            Busy::Rotating => state.rotate = RotateState::Working,
            Busy::Verifying(region) => {
                state.maintenance_error = None;
                state.maintenance = Maintenance::Verifying;
                state.maintenance_region = Some(region);
            }
            Busy::Forgetting(region) => {
                state.maintenance_error = None;
                state.maintenance = Maintenance::Forgetting;
                state.maintenance_region = Some(region);
                state.forget_prompt = None;
                // Key destruction can become irreversible before the operation reports.
                state.integrity = IntegrityState::None;
            }
        },
        Action::EngineStopped(error) => {
            state.clear_vault_scope();
            state.route = Route::Home;
            state.session = SessionState::Failed(error);
        }
        Action::QueryStarted(request) => {
            state.next_query_execution = state.next_query_execution.max(request.execution);
            state.result = QueryState::Running(request);
            state.plan = None;
            state.plan_request = None;
            state.query_notice = None;
        }
        Action::VaultOpened(opened) => match *opened {
            Ok(opened) => {
                let opening_demo = opened.is_demo;
                state.passphrase.clear();
                state.unlock = UnlockStage::Ready;
                state.remember_opened(&opened);
                state.open_vault(Vault::from_engine(&opened));
                if opening_demo {
                    state.query = DEMO_QUERY.to_owned();
                }
                state.mark_query_clean();
                state.session = SessionState::Open(Box::new(opened));
            }
            Err(error) => {
                // The passphrase is cleared on every outcome, not only the ones that
                // leave the screen. A refused secret has no reason to stay in memory.
                state.passphrase.clear();
                state.unlock = if error.kind == crate::engine::Kind::Passphrase {
                    UnlockStage::Rejected
                } else {
                    UnlockStage::Ready
                };
                if state.vault.is_some() {
                    state.open_error = Some(error);
                } else {
                    state.session = SessionState::Failed(error);
                }
            }
        },
        Action::QueryFinished {
            request,
            mut run,
            refreshed,
        } => {
            if !state.accepts_query_reply(&request) {
                return;
            }
            if run.storage_changed {
                state.integrity = IntegrityState::None;
            }
            if let Some(refreshed) = refreshed {
                match *refreshed {
                    Ok(opened) => state.refresh_vault(&opened),
                    Err(error) if run.failed.is_none() => run.failed = Some(error),
                    Err(error) => {
                        state.maintenance_error = Some(error);
                        state.maintenance_region = None;
                    }
                }
            }
            state.result = QueryState::Done { request, run };
            state.selected_row = 0;
            state.scroll_row = 0;
        }
        Action::ImportFinished(mutation) => {
            state.result = QueryState::None;
            state.import_progress = ImportProgress::Idle;
            let crate::engine::Mutation { outcome, refreshed } = *mutation;
            let changed = outcome.is_ok()
                || matches!(&outcome, Err(error) if error.kind == crate::engine::Kind::Completed);
            if changed {
                state.integrity = IntegrityState::None;
            }
            let refresh_error = match refreshed {
                Ok(opened) => {
                    state.refresh_vault(&opened);
                    None
                }
                Err(error) => {
                    if changed {
                        state.integrity = IntegrityState::None;
                    }
                    Some(error)
                }
            };
            state.import_error = match (outcome, refresh_error) {
                (Ok(()), None) => {
                    state.route = Route::Workspace;
                    None
                }
                (Ok(()), Some(error)) => Some(crate::engine::StudioError::new(
                    crate::engine::Kind::Completed,
                    format!(
                        "the tables were created, but Studio could not refresh vault facts: {error}"
                    ),
                )),
                (Err(mut error), Some(refresh_error)) => {
                    error.detail.push_str(&format!(
                        "; Studio also could not refresh vault facts: {refresh_error}"
                    ));
                    Some(error)
                }
                (Err(error), None) => Some(error),
            };
            if state
                .import_error
                .as_ref()
                .is_some_and(|error| error.kind == crate::engine::Kind::Completed)
            {
                // Keep the completed warning visible. The primary action is disabled,
                // and Cancel returns to the still-open vault.
                state.route = Route::Import;
            }
        }
        Action::ExplainFinished {
            request,
            plan,
            refreshed,
        } => {
            if !state.accepts_query_reply(&request) {
                return;
            }
            if refreshed.is_some() {
                state.integrity = IntegrityState::None;
            }
            let refresh_error = refreshed.and_then(|refreshed| match *refreshed {
                Ok(opened) => {
                    state.refresh_vault(&opened);
                    None
                }
                Err(error) => Some(error),
            });
            match *plan {
                Ok(lines) => {
                    state.plan = Some(lines);
                    state.plan_request = Some(request.clone());
                    state.result =
                        refresh_error.map_or(QueryState::None, |error| QueryState::Done {
                            request: request.clone(),
                            run: Box::new(crate::engine::Run {
                                statements: Vec::new(),
                                failed: Some(error),
                                storage_changed: true,
                            }),
                        });
                }
                Err(error) => {
                    state.plan = None;
                    state.plan_request = None;
                    state.result = QueryState::Done {
                        request,
                        run: Box::new(crate::engine::Run {
                            statements: Vec::new(),
                            failed: Some(error),
                            storage_changed: false,
                        }),
                    };
                    if let Some(refresh_error) = refresh_error {
                        state.maintenance_error = Some(refresh_error);
                        state.maintenance_region = None;
                    }
                }
            }
        }
        Action::BeginRotate => {
            if state.vault.as_ref().is_some_and(|vault| !vault.is_demo) {
                state.rotate = RotateState::Open(Box::default());
            }
        }
        Action::CancelRotate => state.rotate = RotateState::Closed,
        Action::PassphraseChanged(mutation) => {
            let crate::engine::Mutation { outcome, refreshed } = *mutation;
            let changed = outcome.is_ok()
                || matches!(&outcome, Err(error) if error.kind == crate::engine::Kind::Completed);
            let refresh_error = match refreshed {
                Ok(opened) => {
                    state.refresh_security_facts(&opened);
                    None
                }
                Err(error) => {
                    state.integrity = IntegrityState::None;
                    Some(error)
                }
            };
            if changed {
                state.integrity = IntegrityState::None;
            }
            state.rotate = match (outcome, refresh_error) {
                (Ok(()), None) => RotateState::Done,
                (Ok(()), Some(error)) => RotateState::Warning(Box::new(
                    crate::engine::StudioError::new(
                        crate::engine::Kind::Completed,
                        format!("the passphrase changed, but Studio could not refresh vault facts: {error}"),
                    ),
                )),
                (Err(mut warning), Some(refresh_error))
                    if warning.kind == crate::engine::Kind::Completed =>
                {
                    warning.detail.push_str(&format!(
                        "; Studio also could not refresh vault facts: {refresh_error}"
                    ));
                    RotateState::Warning(Box::new(warning))
                }
                (Err(error), _) if error.kind == crate::engine::Kind::Completed => {
                    RotateState::Warning(Box::new(error))
                }
                (Err(error), _) => RotateState::Failed(Box::new(error)),
            };
        }
        Action::IntegrityChecked(facts) => {
            state.integrity = match *facts {
                Ok(facts) => IntegrityState::Checked(Box::new(facts)),
                Err(error) => IntegrityState::Failed(Box::new(error)),
            }
        }
        Action::VaultClosed => {
            state.clear_vault_scope();
            state.route = Route::Home;
        }
        Action::PromptDiscardQuery(destination) => {
            state.about_open = false;
            state.query_discard = Some(destination);
            state.open_or_focus(Doc::Query);
        }
        Action::CancelDiscardQuery => state.query_discard = None,
        Action::BeginUnlock(target, facts) => {
            state.route = Route::Unlock;
            state.open_error = None;
            state.unlock = UnlockStage::Ready;
            state.passphrase.clear();
            state.revealed = None;
            state.target = target;
            state.preview = facts;
        }
        Action::SetSecret(field, value) => match field {
            SecretField::Unlock => {
                if state.unlock != UnlockStage::Deriving {
                    state.passphrase = value;
                    state.unlock = UnlockStage::Ready;
                }
            }
            SecretField::New => state.new_vault.passphrase = value,
            SecretField::Confirm => state.new_vault.confirm = value,
            SecretField::RotateCurrent | SecretField::RotateNext | SecretField::RotateConfirm => {
                let mut form = match std::mem::take(&mut state.rotate) {
                    RotateState::Open(form) => *form,
                    _ => RotateKey::default(),
                };
                match field {
                    SecretField::RotateCurrent => form.current = value,
                    SecretField::RotateNext => form.next = value,
                    _ => form.confirm = value,
                }
                state.rotate = RotateState::Open(Box::new(form));
            }
        },
        Action::ToggleReveal(field) => {
            state.revealed = if state.revealed == Some(field) {
                None
            } else {
                Some(field)
            };
        }
        Action::CancelUnlock => {
            state.route = Route::Home;
            state.open_error = None;
            state.passphrase.clear();
            state.revealed = None;
            state.unlock = UnlockStage::Ready;
        }
        // Replacing the form zeroizes any abandoned passphrases.
        Action::BeginCreate => {
            state.route = Route::Create;
            state.open_error = None;
            state.new_vault = NewVault::default();
            state.revealed = None;
        }
        Action::CancelCreate => {
            state.route = Route::Home;
            state.open_error = None;
            state.new_vault = NewVault::default();
            state.revealed = None;
        }
        Action::SetCreatePath(path) => state.new_vault.path = Some(path),
        Action::BeginImport => {
            state.route = Route::Import;
            state.source = Source::default();
            state.import_progress = ImportProgress::Idle;
            state.import_error = None;
            state.selected_row = 0;
            state.scroll_row = 0;
        }
        Action::CancelImport => {
            if state.import_progress.busy() {
                return;
            }
            state.route = if state.vault.is_some() {
                Route::Workspace
            } else {
                Route::Home
            };
            state.source = Source::default();
            state.import_error = None;
        }
        Action::ImportCancelRequested => {
            if state.import_progress == ImportProgress::Running {
                state.import_progress = ImportProgress::Cancelling;
            }
        }
        Action::SetImportSource(source) => {
            state.source = source;
            state.selected_row = 0;
            state.scroll_row = 0;
        }
        Action::SetKdf(kdf) => state.new_vault.kdf = kdf,
        Action::SetArgon2Profile(profile) => state.new_vault.profile = profile,
        Action::OpenDoc(doc) => state.open_or_focus(doc),
        Action::FocusDoc(i) => {
            if i < state.docs.len() {
                state.active = i;
                state.selected_row = 0;
                state.scroll_row = 0;
            }
        }
        Action::CloseDoc(i) => {
            if i < state.docs.len() {
                state.docs.remove(i);
                // Preserve focus when removing a tab before the active one.
                if i < state.active {
                    state.active -= 1;
                } else {
                    state.active = state.active.min(state.docs.len().saturating_sub(1));
                }
                state.selected_row = 0;
                state.scroll_row = 0;
                if state.docs.is_empty() {
                    state.route = Route::Home;
                }
            }
        }
        Action::SelectRow(i) => state.selected_row = i,
        Action::ScrollTo(i) => state.scroll_row = i,
        Action::ReportWindow { visible, total } => {
            state.visible_rows = visible;
            if matches!(state.active_doc(), Some(Doc::Table(_) | Doc::Query)) {
                state.fixture_row_count = total;
            }
        }
        Action::MoveSelection(delta) => {
            let len = state.visible_row_count();
            if len > 0 {
                let next = state.selected_row as i32 + delta;
                state.selected_row = next.clamp(0, len as i32 - 1) as usize;
            }
        }
        Action::SelectFirst => state.selected_row = 0,
        Action::SelectLast => {
            state.selected_row = state.visible_row_count().saturating_sub(1);
        }
        Action::BeginForgetSelected => {
            if state.forget_row_blocker().is_some() {
                return;
            }
            let Some(region) = state.visible_region() else {
                return;
            };
            let Some(atom) = region.atoms.get(state.selected_row) else {
                return;
            };
            state.forget_prompt = Some(ForgetPrompt {
                region: region.name().to_owned(),
                atom_id: atom.id,
                plaintext: region.facts.plaintext,
            });
        }
        Action::CancelForget => state.forget_prompt = None,
        Action::TableBrowsed(browsed) => {
            state.browse = Some(*browsed);
            state.selected_row = 0;
            state.scroll_row = 0;
        }
        Action::BeginVectorSample {
            vault_revision,
            table,
            column,
        } => {
            if vault_revision == state.vault_revision {
                if let Some(vector) = state.vault.as_mut().and_then(|vault| {
                    vault
                        .vectors
                        .iter_mut()
                        .find(|vector| vector.table == table && vector.column == column)
                }) {
                    vector.begin_loading();
                }
            }
        }
        Action::VectorSampled {
            vault_revision,
            table,
            column,
            sample,
        } => {
            if vault_revision != state.vault_revision {
                return;
            }
            let Some(vector) = state.vault.as_mut().and_then(|vault| {
                vault
                    .vectors
                    .iter_mut()
                    .find(|vector| vector.table == table && vector.column == column)
            }) else {
                return;
            };
            if vector.sample != crate::model::VectorSample::Loading {
                return;
            }
            match *sample {
                Ok(sample) => {
                    vector.install_sample(&sample);
                }
                Err(error) => vector.fail_sample(error),
            }
        }
        Action::AtomsLoaded { region, page } => {
            let Some(vault) = &mut state.vault else {
                return;
            };
            let Some(target) = vault.region_mut(&region) else {
                return;
            };
            match *page {
                // `after_id` pages extend the currently held window.
                Ok(mut page) => {
                    if target.facts.plaintext {
                        for atom in &mut page.atoms {
                            atom.mark_plaintext_unattested();
                        }
                    }
                    target.next_after_id = page.next_after_id;
                    target.exhausted = page.next_after_id.is_none();
                    target.atoms.extend(page.atoms);
                    target.facts.unreadable = None;
                }
                Err(error) => {
                    target.next_after_id = None;
                    target.exhausted = true;
                    target.facts.unreadable = Some(error.to_string());
                    state.maintenance_error = Some(error);
                    state.maintenance_region = Some(region);
                }
            }
        }
        Action::PageVerified {
            region,
            asked,
            verdicts,
        } => {
            state.maintenance = Maintenance::Idle;
            let stamp = crate::model::stamp_now();
            let verdicts = match *verdicts {
                Ok(verdicts) => verdicts,
                Err(error) => {
                    state.maintenance_error = Some(error);
                    state.maintenance_region = Some(region);
                    return;
                }
            };
            let Some(vault) = &mut state.vault else {
                return;
            };
            let Some(target) = vault.region_mut(&region) else {
                return;
            };
            state.maintenance_error = None;
            state.maintenance_region = None;
            let mut authentic = 0;
            let mut tampered = 0;
            let mut missing = 0;
            let mut erased = 0;
            let mut unattestable = 0;
            for atom_id in &asked {
                let Some(verdict) = verdicts.iter().find(|verdict| verdict.atom_id == *atom_id)
                else {
                    continue;
                };
                if let Some(atom) = target.atoms.iter_mut().find(|atom| atom.id == *atom_id) {
                    atom.attest(verdict, &stamp);
                }
                match verdict.verdict {
                    citadel_mem::AttestVerdict::Authentic => authentic += 1,
                    citadel_mem::AttestVerdict::Tampered => tampered += 1,
                    citadel_mem::AttestVerdict::Missing => missing += 1,
                    citadel_mem::AttestVerdict::KeyErased => erased += 1,
                    citadel_mem::AttestVerdict::PlaintextUnattested => unattestable += 1,
                }
            }
            state.page_checked = PageCheck {
                authentic,
                tampered,
                missing,
                erased,
                unattestable,
                scope: asked.len() as u32,
                region: Some(region),
                atom_ids: asked,
            };
            state.grid_tab = GridTab::Attestation;
        }
        Action::Forgotten {
            region,
            asked,
            mutation,
        } => {
            state.maintenance = Maintenance::Idle;
            let crate::engine::Mutation { outcome, refreshed } = *mutation;
            let refresh_error = match refreshed {
                Ok(opened) => {
                    state.refresh_vault(&opened);
                    None
                }
                Err(error) => Some(error),
            };
            let stamp = crate::model::stamp_now();
            let receipt = match outcome {
                Ok(receipt) => receipt,
                Err(mut error) => {
                    if let Some(refresh_error) = refresh_error {
                        error.detail.push_str(&format!(
                            "; Studio also could not refresh vault facts: {refresh_error}"
                        ));
                    }
                    state.maintenance_error = Some(error);
                    state.maintenance_region = Some(region);
                    return;
                }
            };
            state.maintenance_error = refresh_error.map(|error| {
                crate::engine::StudioError::new(
                    crate::engine::Kind::Completed,
                    format!(
                        "the atom was forgotten, but Studio could not refresh vault facts: {error}"
                    ),
                )
            });
            let Some(vault) = &mut state.vault else {
                return;
            };
            state.maintenance_region = state.maintenance_error.as_ref().map(|_| region.clone());
            if let Some(target) = vault.region_mut(&region) {
                // Keep immutable atoms the engine refused to erase.
                let gone: Vec<_> = asked
                    .iter()
                    .filter(|id| !receipt.immutable_skipped.contains(id))
                    .collect();
                let removed = target.atoms.len();
                target.atoms.retain(|a| !gone.contains(&&a.id));
                let removed = (removed - target.atoms.len()) as u64;
                if let Some(total) = &mut target.facts.total {
                    *total = total.saturating_sub(removed);
                }
                state.selected_row = state.selected_row.min(target.atoms.len().saturating_sub(1));
            }
            state.page_checked = PageCheck::default();
            vault.receipts.insert(
                0,
                Receipt {
                    region,
                    issued: stamp,
                    receipt,
                },
            );
        }
        Action::SetGridTab(tab) => state.grid_tab = tab,
        Action::CycleDensity => {
            state.density = match state.density {
                Density::Compact => Density::Default,
                Density::Default => Density::Comfortable,
                Density::Comfortable => Density::Compact,
            }
        }
        Action::ToggleTheme => {
            state.light = !state.light;
            state.palette = if state.light {
                Palette::LIGHT
            } else {
                Palette::DARK
            };
        }
        Action::ShowAbout => {
            state.palette_open = false;
            state.about_open = true;
        }
        Action::CloseAbout => state.about_open = false,
        Action::BeginQueryComposition => {
            if !state.query_composing {
                state.query_committed.clone_from(&state.query);
                state.query_composing = true;
            }
        }
        Action::CommitQueryComposition => {
            state.query_composing = false;
            state.commit_query_text();
        }
        Action::CancelQueryComposition => {
            state.query_composing = false;
            state.query.clone_from(&state.query_committed);
        }
        Action::EditQuery(text) => {
            state.query = text;
            state.query_notice = None;
            if !state.query_composing {
                state.commit_query_text();
            }
        }
        Action::QueryNotice(message) => {
            state.query_notice = Some(EditorNotice::Unchanged(message));
        }
        // Preserve the original text when formatting cannot preserve all source content.
        Action::FormatQuery => {
            if state.query_composing {
                state.query_notice = Some(EditorNotice::Unchanged(
                    "Format skipped: finish or cancel text composition first".to_owned(),
                ));
            } else {
                match crate::model::format_sql(&state.query) {
                    Ok(formatted) => {
                        let changed = formatted != state.query;
                        state.query = formatted;
                        state.commit_query_text();
                        state.query_notice = Some(EditorNotice::Formatted { changed });
                    }
                    Err(error) => {
                        state.query_notice =
                            Some(EditorNotice::Unchanged(error.message().to_owned()));
                    }
                }
            }
        }
        Action::TogglePalette => {
            state.palette_open = !state.palette_open;
            state.palette_query.clear();
            state.palette_index = 0;
        }
        Action::PaletteChar(c) => {
            state.palette_query.push(c);
            state.palette_index = 0;
        }
        Action::PaletteBackspace => {
            state.palette_query.pop();
            state.palette_index = 0;
        }
        Action::PaletteMove(delta) => {
            let len = commands(state).len();
            if len > 0 {
                let next = state.palette_index as i32 + delta;
                state.palette_index = next.rem_euclid(len as i32) as usize;
            }
        }
        Action::ScrollPane(to) => state.pane_scroll = to,
        Action::MoveCamera(camera) => state.camera = camera,
        Action::ResetCamera => {
            state.camera = Camera::default();
            state.selected_point = None;
        }
        Action::SelectPoint(i) => state.selected_point = i,
        Action::PaletteRun => {
            debug_assert!(false, "the dispatcher must resolve a palette intent");
        }
    }
}

#[cfg(test)]
mod reducer_tests {
    use super::*;
    use crate::engine::memory::{AtomPage, AtomView, RegionFacts};
    use crate::engine::{AuditFacts, IntegrityFacts, Kind, Opened, QueryPlan, Run, StudioError};
    use citadel_mem::types::ERASURE_SCOPE_CAVEAT;
    use citadel_mem::{EmbeddingMetric, ErasureReceipt, SlotErasure};
    use std::sync::OnceLock;

    fn vault_facts() -> crate::engine::VaultFacts {
        static FACTS: OnceLock<crate::engine::VaultFacts> = OnceLock::new();
        FACTS
            .get_or_init(|| {
                let session = crate::engine::session::DemoVault::new();
                session.facts().expect("the demo reports its facts")
            })
            .clone()
    }

    fn opened(label: &str, region: bool) -> Opened {
        Opened {
            label: label.to_owned(),
            path: std::path::PathBuf::from(format!("{label}.cdl")),
            is_demo: false,
            facts: vault_facts(),
            tables: Vec::new(),
            regions: region
                .then(|| RegionFacts {
                    name: "notes".to_owned(),
                    dim: 3,
                    metric: EmbeddingMetric::Cosine,
                    model: "test-model".to_owned(),
                    plaintext: false,
                    total: Some(2),
                    unreadable: None,
                })
                .into_iter()
                .collect(),
            vectors: Vec::new(),
        }
    }

    fn marked_opened(label: &str, region: bool, marker: u8) -> Opened {
        let mut opened = opened(label, region);
        opened.facts.stats.merkle_root[0] = marker;
        opened.facts.key_file.epoch = u32::from(marker);
        opened.facts.audit = Some(AuditFacts {
            entries: u64::from(marker),
            segments: Vec::new(),
            live_count_shortfall_at_open: 0,
        });
        opened
            .facts
            .keys
            .atom
            .as_mut()
            .expect("the seeded demo has an atom key store")
            .tombstoned = u32::from(marker);
        opened
    }

    fn checked_integrity(state: &mut State) {
        state.integrity = IntegrityState::Checked(Box::new(IntegrityFacts {
            pages_checked: 9,
            errors: Vec::new(),
        }));
    }

    fn erasure_receipt(atom_id: i64) -> ErasureReceipt {
        ErasureReceipt {
            cryptographic_erasure: true,
            rows_deleted: 1,
            erased_count: 1,
            slots_erased: vec![SlotErasure {
                slot: 3,
                atom_id,
                old_gen: 1,
                new_gen: 2,
            }],
            immutable_skipped: Vec::new(),
            algorithm: "AES-KW",
            wrapped_key_size: 40,
            fsync: true,
            readback_confirmed: true,
            scope_caveat: ERASURE_SCOPE_CAVEAT,
        }
    }

    fn error(detail: &str) -> StudioError {
        StudioError::new(Kind::Io, detail)
    }

    fn atom(id: i64) -> AtomView {
        AtomView {
            id,
            kind: "note".to_owned(),
            text: format!("note {id}"),
            created_at: 0,
            immutable: false,
            verdict: None,
            aad_bound: false,
            key_slot: None,
            key_gen: None,
            verified_at: None,
        }
    }

    fn open(state: &mut State, label: &str, region: bool) {
        apply(
            state,
            Action::VaultOpened(Box::new(Ok(opened(label, region)))),
        );
    }

    #[test]
    fn opening_another_vault_clears_every_session_scoped_view() {
        let mut state = State::default();
        open(&mut state, "first", true);
        state.query = "SELECT a secret from first".to_owned();
        state.docs.push(Doc::Security);
        state.browse = Some(Browsed {
            table: "first_table".to_owned(),
            rows: Ok(citadel_sql::QueryResult {
                columns: vec!["value".to_owned()],
                rows: Vec::new(),
            }),
        });
        state.rotate = RotateState::Open(Box::new(RotateKey {
            current: "first-current".into(),
            next: "first-next-value".into(),
            confirm: "first-next-value".into(),
        }));
        state.integrity = IntegrityState::Failed(Box::new(error("first integrity")));
        state.maintenance_error = Some(error("first maintenance"));
        state.import_error = Some(error("first import"));
        state.camera.zoom = 8.0;
        state.selected_point = Some(4);
        state.grid_tab = GridTab::Attestation;
        state.target = Target::Picked("first.cdl".into());
        state.preview = Preview::Refused("first header".to_owned());
        state.palette_open = true;
        state.palette_query = "first command".to_owned();
        state.palette_index = 2;
        state.page_checked = PageCheck {
            authentic: 2,
            tampered: 0,
            missing: 0,
            erased: 0,
            unattestable: 0,
            scope: 2,
            region: Some("notes".to_owned()),
            atom_ids: vec![1, 2],
        };
        let previous_revision = state.vault_revision;

        open(&mut state, "second", false);

        assert_eq!(state.vault().name, "second");
        assert!(state.vault_revision > previous_revision);
        assert!(state.query.is_empty());
        assert!(state.browse.is_none());
        assert!(matches!(state.integrity, IntegrityState::None));
        assert!(matches!(state.rotate, RotateState::Closed));
        assert!(state.maintenance_error.is_none());
        assert!(state.import_error.is_none());
        assert_eq!(state.target, Target::None);
        assert_eq!(state.preview, Preview::Unread);
        assert!(!state.palette_open);
        assert!(state.palette_query.is_empty());
        assert_eq!(state.palette_index, 0);
        assert_eq!(state.camera, Camera::default());
        assert_eq!(state.selected_point, None);
        assert_eq!(state.grid_tab, GridTab::Data);
        assert_eq!(state.page_checked, PageCheck::default());
        assert_eq!(state.docs, vec![Doc::Query]);
        assert!(matches!(state.session, SessionState::Open(_)));

        apply(&mut state, Action::VaultClosed);
        assert!(state.vault.is_none());
        assert!(state.docs.is_empty());
        assert!(matches!(state.session, SessionState::None));
        assert_eq!(state.route, Route::Home);
    }

    #[test]
    fn transient_workflows_hide_vault_navigation_and_close_commands() {
        let mut state = State::default();
        open(&mut state, "first", true);
        apply(&mut state, Action::BeginImport);
        state.source = Source::Reading {
            path: "source.sqlite".into(),
        };

        assert!(!state.vault_navigation_available());
        assert_eq!(
            state.close_vault_blocker(),
            Some("Finish or cancel the current workflow first")
        );
        let titles: Vec<_> = commands(&state)
            .into_iter()
            .map(|command| command.title)
            .collect();
        for unavailable in ["Open Security", "Open query editor", "Close vault"] {
            assert!(
                !titles.iter().any(|title| title == unavailable),
                "{unavailable:?} escaped a transient workflow through the command palette"
            );
        }
    }

    #[test]
    fn a_disposable_demo_is_not_added_to_recent_vaults() {
        let mut state = State::default();
        let mut demo = opened("demo", true);
        demo.is_demo = true;

        apply(&mut state, Action::VaultOpened(Box::new(Ok(demo))));

        assert!(state.recent.is_empty());
        assert_eq!(state.vault().name, "demo");
    }

    #[test]
    fn reopening_and_mutating_a_vault_refreshes_and_reorders_its_recent_summary() {
        let mut state = State::default();
        let mut stale = opened("same", false);
        stale.facts.stats.entry_count = 1;
        stale.facts.stats.total_pages = 2;
        apply(&mut state, Action::VaultOpened(Box::new(Ok(stale))));
        state.recent.insert(
            0,
            crate::model::RecentVault {
                path: "other.cdl".into(),
                entries: 8,
                pages: 9,
            },
        );

        let mut reopened = opened("same", false);
        reopened.facts.stats.entry_count = 10;
        reopened.facts.stats.total_pages = 11;
        apply(
            &mut state,
            Action::VaultOpened(Box::new(Ok(reopened.clone()))),
        );
        assert_eq!(state.recent[0].path, reopened.path);
        assert_eq!(state.recent[0].entries, 10);
        assert_eq!(state.recent[0].pages, 11);
        assert_eq!(state.recent.len(), 2, "the old summary was duplicated");

        reopened.facts.stats.entry_count = 20;
        reopened.facts.stats.total_pages = 21;
        state.refresh_vault(&reopened);
        assert_eq!(state.recent[0].entries, 20);
        assert_eq!(state.recent[0].pages, 21);
        assert_eq!(state.recent.len(), 2, "a mutation duplicated the summary");
    }

    #[test]
    fn restored_preferences_keep_existing_vaults() {
        let dir = tempfile::tempdir().unwrap();
        let current = dir.path().join("demo-v2").join("demo.cdl");
        let previous = dir.path().join("demo-v1").join("demo.cdl");
        std::fs::create_dir_all(current.parent().unwrap()).unwrap();
        std::fs::create_dir_all(previous.parent().unwrap()).unwrap();
        std::fs::write(&current, b"current").unwrap();
        std::fs::write(&previous, b"previous").unwrap();

        let mut state = State::default();
        Prefs {
            light: false,
            density: Density::Default,
            recent: vec![
                crate::model::RecentVault {
                    path: current.clone(),
                    entries: 1,
                    pages: 1,
                },
                crate::model::RecentVault {
                    path: previous.clone(),
                    entries: 1,
                    pages: 1,
                },
            ],
        }
        .apply(&mut state);

        assert_eq!(state.recent.len(), 2);
        assert_eq!(state.recent[0].path, current);
        assert_eq!(state.recent[1].path, previous);
    }

    #[test]
    fn a_failed_replacement_keeps_the_open_session_usable() {
        let mut state = State::default();
        open(&mut state, "first", true);
        let previous_revision = state.vault_revision;

        apply(&mut state, Action::EngineBusy(Busy::Opening));
        assert!(matches!(state.session, SessionState::Open(_)));

        apply(
            &mut state,
            Action::VaultOpened(Box::new(Err(error("second vault refused")))),
        );

        assert_eq!(state.vault().name, "first");
        assert_eq!(state.vault_revision, previous_revision);
        assert!(matches!(state.session, SessionState::Open(_)));
        assert_eq!(
            state.opening_error().map(|error| error.detail.as_str()),
            Some("second vault refused")
        );

        apply(&mut state, Action::CancelUnlock);
        assert!(state.opening_error().is_none());
        assert!(matches!(state.session, SessionState::Open(_)));
    }

    #[test]
    fn a_stopped_engine_invalidates_the_displayed_session() {
        let mut state = State::default();
        open(&mut state, "first", true);
        state.query = "SELECT 1".to_owned();

        apply(
            &mut state,
            Action::EngineStopped(error("the engine stopped before it answered")),
        );

        assert_eq!(state.route, Route::Home);
        assert!(state.vault.is_none());
        assert!(state.docs.is_empty());
        assert!(state.query.is_empty());
        assert_eq!(
            state.opening_error().map(|error| error.detail.as_str()),
            Some("the engine stopped before it answered")
        );
        assert!(matches!(state.session, SessionState::Failed(_)));
    }

    #[test]
    fn query_refresh_replaces_catalog_without_discarding_the_run() {
        let mut state = State::default();
        open(&mut state, "before", false);
        state.query = "SELECT 1".to_owned();
        state.docs = vec![Doc::Table("removed_by_query".to_owned())];
        state.active = 0;
        let revision = state.vault_revision;
        let request = state.next_query_request();
        apply(&mut state, Action::QueryStarted(request.clone()));
        let run = Run {
            statements: Vec::new(),
            failed: None,
            storage_changed: true,
        };

        apply(
            &mut state,
            Action::QueryFinished {
                request,
                run: Box::new(run),
                refreshed: Some(Box::new(Ok(opened("after", false)))),
            },
        );

        assert_eq!(state.vault().name, "after");
        assert!(state.vault_revision > revision);
        assert_eq!(state.query, "SELECT 1");
        assert_eq!(state.docs, vec![Doc::Query]);
        assert!(matches!(state.session, SessionState::Open(_)));
        assert!(state
            .result
            .done()
            .is_some_and(|run| run.storage_changed && run.failed.is_none()));
    }

    #[test]
    fn a_committed_query_with_a_failed_refresh_cannot_keep_green_integrity() {
        let mut state = State::default();
        open(&mut state, "before", false);
        checked_integrity(&mut state);
        state.query = "CREATE TABLE changed (id INTEGER PRIMARY KEY)".to_owned();
        let request = state.next_query_request();
        apply(&mut state, Action::QueryStarted(request.clone()));

        apply(
            &mut state,
            Action::QueryFinished {
                request,
                run: Box::new(Run {
                    statements: Vec::new(),
                    failed: None,
                    storage_changed: true,
                }),
                refreshed: Some(Box::new(Err(error("refresh failed")))),
            },
        );

        assert!(matches!(state.integrity, IntegrityState::None));
        assert!(state.result.done().is_some_and(|run| {
            run.failed
                .as_ref()
                .is_some_and(|error| error.detail == "refresh failed")
        }));
    }

    #[test]
    fn import_failure_preserves_the_open_session_and_success_refreshes_it() {
        let mut state = State::default();
        open(&mut state, "before", false);
        apply(&mut state, Action::BeginImport);
        apply(&mut state, Action::EngineBusy(Busy::Importing));
        assert_eq!(state.import_progress, ImportProgress::Running);
        assert_eq!(state.route, Route::Import);

        apply(&mut state, Action::ImportCancelRequested);
        assert_eq!(state.import_progress, ImportProgress::Cancelling);
        assert_eq!(state.route, Route::Import);

        apply(
            &mut state,
            Action::ImportFinished(Box::new(crate::engine::Mutation {
                outcome: Err(error("import failed")),
                refreshed: Ok(opened("before", false)),
            })),
        );

        assert!(matches!(state.session, SessionState::Open(_)));
        assert_eq!(state.vault().name, "before");
        assert_eq!(state.route, Route::Import);
        assert_eq!(state.import_progress, ImportProgress::Idle);
        assert!(state.import_error.is_some());
        assert!(!state.result.busy());

        apply(
            &mut state,
            Action::ImportFinished(Box::new(crate::engine::Mutation {
                outcome: Ok(()),
                refreshed: Ok(opened("after", false)),
            })),
        );
        assert_eq!(state.vault().name, "after");
        assert_eq!(state.route, Route::Workspace);
        assert!(state.import_error.is_none());
        assert!(matches!(state.session, SessionState::Open(_)));
    }

    #[test]
    fn explain_refresh_error_is_visible_and_never_leaves_the_toolbar_busy() {
        let mut state = State::default();
        open(&mut state, "before", false);
        checked_integrity(&mut state);
        let request = state.next_query_request();
        apply(&mut state, Action::QueryStarted(request.clone()));
        apply(
            &mut state,
            Action::ExplainFinished {
                request,
                plan: Box::new(Ok(QueryPlan {
                    lines: vec!["scan".to_owned()],
                    measured: true,
                })),
                refreshed: Some(Box::new(Err(error("refresh failed")))),
            },
        );

        assert!(state.plan.as_ref().is_some_and(|plan| plan.measured));
        assert!(state.result.done().is_some_and(|run| {
            run.storage_changed
                && run
                    .failed
                    .as_ref()
                    .is_some_and(|error| error.detail == "refresh failed")
        }));
        assert!(matches!(state.integrity, IntegrityState::None));
    }

    #[test]
    fn a_completed_import_with_a_failed_refresh_cannot_be_retried_as_if_nothing_happened() {
        let mut state = State::default();
        open(&mut state, "before", false);
        checked_integrity(&mut state);
        apply(&mut state, Action::BeginImport);
        apply(&mut state, Action::EngineBusy(Busy::Importing));

        apply(
            &mut state,
            Action::ImportFinished(Box::new(crate::engine::Mutation {
                outcome: Ok(()),
                refreshed: Err(error("refresh failed")),
            })),
        );

        let warning = state
            .import_error
            .as_ref()
            .expect("the completed import must retain its refresh warning");
        assert_eq!(warning.kind, Kind::Completed);
        assert!(warning.detail.contains("tables were created"));
        assert!(warning.detail.contains("refresh failed"));
        assert_eq!(state.route, Route::Import);
        assert!(matches!(state.integrity, IntegrityState::None));

        apply(&mut state, Action::CancelImport);
        assert_eq!(state.route, Route::Workspace);
        assert!(state.vault.is_some());
    }

    #[test]
    fn paging_uses_the_engine_cursor_and_stops_after_terminal_or_failed_pages() {
        let mut state = State::default();
        open(&mut state, "paged", true);
        apply(
            &mut state,
            Action::AtomsLoaded {
                region: "notes".to_owned(),
                page: Box::new(Ok(AtomPage {
                    atoms: vec![atom(1)],
                    next_after_id: Some(1),
                })),
            },
        );
        let region = state.vault().region("notes").unwrap();
        assert_eq!(region.next_after_id, Some(1));
        assert!(!region.exhausted);

        apply(
            &mut state,
            Action::AtomsLoaded {
                region: "notes".to_owned(),
                page: Box::new(Ok(AtomPage {
                    atoms: Vec::new(),
                    next_after_id: None,
                })),
            },
        );
        assert!(state.vault().region("notes").unwrap().exhausted);

        open(&mut state, "failed-page", true);
        apply(
            &mut state,
            Action::AtomsLoaded {
                region: "notes".to_owned(),
                page: Box::new(Err(error("page failed"))),
            },
        );
        let region = state.vault().region("notes").unwrap();
        assert!(region.exhausted);
        assert_eq!(region.next_after_id, None);
        assert_eq!(
            region.facts.unreadable.as_deref(),
            Some("The file could not be read or written: page failed")
        );
        assert!(matches!(state.session, SessionState::Open(_)));
        assert!(state.maintenance_error.is_some());
    }

    #[test]
    fn refresh_vault_preserves_existing_erasure_receipts() {
        let mut state = State::default();
        apply(
            &mut state,
            Action::VaultOpened(Box::new(Ok(marked_opened("before", true, 1)))),
        );
        state.vault.as_mut().unwrap().receipts.push(Receipt {
            region: "notes".to_owned(),
            issued: "before refresh".to_owned(),
            receipt: erasure_receipt(1),
        });

        state.refresh_vault(&marked_opened("after", true, 2));

        assert_eq!(state.vault().facts.stats.merkle_root[0], 2);
        assert_eq!(state.vault().receipts.len(), 1);
        assert_eq!(state.vault().receipts[0].issued, "before refresh");
        assert_eq!(state.vault().receipts[0].receipt.slots_erased[0].atom_id, 1);
    }

    #[test]
    fn beginning_an_irreversible_forget_invalidates_checked_integrity() {
        let mut state = State::default();
        open(&mut state, "forget", true);
        checked_integrity(&mut state);

        apply(
            &mut state,
            Action::EngineBusy(Busy::Forgetting("notes".to_owned())),
        );

        assert!(matches!(state.integrity, IntegrityState::None));
        assert_eq!(state.maintenance, Maintenance::Forgetting);
    }

    #[test]
    fn failed_forget_still_installs_the_authoritative_post_attempt_snapshot() {
        let mut state = State::default();
        apply(
            &mut state,
            Action::VaultOpened(Box::new(Ok(marked_opened("before", true, 1)))),
        );
        apply(
            &mut state,
            Action::EngineBusy(Busy::Forgetting("notes".to_owned())),
        );

        apply(
            &mut state,
            Action::Forgotten {
                region: "notes".to_owned(),
                asked: vec![1],
                mutation: Box::new(crate::engine::Mutation {
                    outcome: Err(error("cleanup failed after erasure")),
                    refreshed: Ok(marked_opened("after", true, 7)),
                }),
            },
        );

        assert_eq!(state.vault().facts.stats.merkle_root[0], 7);
        assert_eq!(state.vault().facts.keys.atom.unwrap().tombstoned, 7);
        let opened = state
            .session
            .opened()
            .expect("the refreshed session stays open");
        assert_eq!(opened.facts.stats.merkle_root[0], 7);
        assert_eq!(
            state
                .maintenance_error
                .as_ref()
                .map(|error| error.detail.as_str()),
            Some("cleanup failed after erasure")
        );
        assert!(state.vault().receipts.is_empty());
        assert!(matches!(state.integrity, IntegrityState::None));
    }

    #[test]
    fn successful_forget_refreshes_facts_and_keeps_every_session_receipt() {
        let mut state = State::default();
        apply(
            &mut state,
            Action::VaultOpened(Box::new(Ok(marked_opened("before", true, 1)))),
        );
        state.vault.as_mut().unwrap().receipts.push(Receipt {
            region: "notes".to_owned(),
            issued: "older receipt".to_owned(),
            receipt: erasure_receipt(1),
        });
        checked_integrity(&mut state);
        apply(
            &mut state,
            Action::EngineBusy(Busy::Forgetting("notes".to_owned())),
        );

        apply(
            &mut state,
            Action::Forgotten {
                region: "notes".to_owned(),
                asked: vec![2],
                mutation: Box::new(crate::engine::Mutation {
                    outcome: Ok(erasure_receipt(2)),
                    refreshed: Ok(marked_opened("after", true, 8)),
                }),
            },
        );

        assert_eq!(state.vault().facts.stats.merkle_root[0], 8);
        assert_eq!(state.vault().facts.keys.atom.unwrap().tombstoned, 8);
        assert_eq!(state.vault().receipts.len(), 2);
        assert_eq!(state.vault().receipts[0].receipt.slots_erased[0].atom_id, 2);
        assert_eq!(state.vault().receipts[1].issued, "older receipt");
        assert!(state.maintenance_error.is_none());
        assert!(matches!(state.integrity, IntegrityState::None));
    }

    #[test]
    fn maintenance_failure_never_reclassifies_an_open_session_as_opening() {
        let mut state = State::default();
        open(&mut state, "maintained", true);
        apply(
            &mut state,
            Action::EngineBusy(Busy::Verifying("notes".to_owned())),
        );
        assert_eq!(state.maintenance, Maintenance::Verifying);
        assert!(matches!(state.session, SessionState::Open(_)));

        apply(
            &mut state,
            Action::PageVerified {
                region: "notes".to_owned(),
                asked: vec![1],
                verdicts: Box::new(Err(error("verification failed"))),
            },
        );
        assert!(matches!(state.session, SessionState::Open(_)));
        assert_eq!(state.maintenance, Maintenance::Idle);
        assert_eq!(
            state
                .maintenance_error
                .as_ref()
                .map(|error| error.detail.as_str()),
            Some("verification failed")
        );

        apply(
            &mut state,
            Action::EngineBusy(Busy::Forgetting("notes".to_owned())),
        );
        assert_eq!(state.maintenance, Maintenance::Forgetting);
        assert!(state.maintenance_error.is_none());
        assert!(matches!(state.session, SessionState::Open(_)));

        apply(
            &mut state,
            Action::Forgotten {
                region: "notes".to_owned(),
                asked: vec![1],
                mutation: Box::new(crate::engine::Mutation {
                    outcome: Err(error("erasure failed")),
                    refreshed: Ok(opened("before", true)),
                }),
            },
        );
        assert!(matches!(state.session, SessionState::Open(_)));
        assert_eq!(
            state
                .maintenance_error
                .as_ref()
                .map(|error| error.detail.as_str()),
            Some("erasure failed")
        );
        assert_eq!(state.maintenance, Maintenance::Idle);
    }

    #[test]
    fn verification_summary_keeps_each_engine_verdict_distinct() {
        use citadel_mem::{AtomAttestation, AttestVerdict};

        let mut state = State::default();
        open(&mut state, "verdicts", true);
        apply(
            &mut state,
            Action::AtomsLoaded {
                region: "notes".to_owned(),
                page: Box::new(Ok(AtomPage {
                    atoms: (1..=5).map(atom).collect(),
                    next_after_id: None,
                })),
            },
        );
        state.visible_rows = 5;
        let verdicts = [
            AttestVerdict::Authentic,
            AttestVerdict::Tampered,
            AttestVerdict::Missing,
            AttestVerdict::KeyErased,
            AttestVerdict::PlaintextUnattested,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, verdict)| AtomAttestation {
            atom_id: index as i64 + 1,
            verdict,
            aad_bound: verdict == AttestVerdict::Authentic,
            key_slot: None,
            key_gen: None,
        })
        .collect();
        apply(
            &mut state,
            Action::PageVerified {
                region: "notes".to_owned(),
                asked: (1..=5).collect(),
                verdicts: Box::new(Ok(verdicts)),
            },
        );

        assert_eq!(state.page_checked.authentic, 1);
        assert_eq!(state.page_checked.tampered, 1);
        assert_eq!(state.page_checked.missing, 1);
        assert_eq!(state.page_checked.erased, 1);
        assert_eq!(state.page_checked.unattestable, 1);
        assert_eq!(state.page_checked.scope, 5);
        assert_eq!(
            state.page_check_label(),
            "1 authentic · 1 tampered · 1 missing · 1 key erased · 1 not attestable"
        );
        assert_eq!(state.grid_tab, GridTab::Attestation);
    }

    #[test]
    fn forgetting_requires_an_explicit_confirmation() {
        let mut state = State::default();
        open(&mut state, "confirm", true);
        apply(
            &mut state,
            Action::AtomsLoaded {
                region: "notes".to_owned(),
                page: Box::new(Ok(AtomPage {
                    atoms: vec![atom(7)],
                    next_after_id: None,
                })),
            },
        );
        state.visible_rows = 1;

        apply(&mut state, Action::BeginForgetSelected);
        let prompt = state.forget_prompt.as_ref().expect("confirmation opened");
        assert_eq!(prompt.region, "notes");
        assert_eq!(prompt.atom_id, 7);
        assert!(state.vault().receipts.is_empty());
        assert!(state.scroll().modal);

        apply(&mut state, Action::CancelForget);
        assert!(state.forget_prompt.is_none());
        assert!(!state.scroll().modal);
    }

    #[test]
    fn about_is_transient_and_blocks_the_workspace_underneath() {
        let mut state = State {
            palette_open: true,
            ..State::default()
        };

        apply(&mut state, Action::ShowAbout);

        assert!(state.about_open);
        assert!(!state.palette_open);
        assert!(state.scroll().modal);

        apply(&mut state, Action::CloseAbout);
        assert!(!state.about_open);
        assert!(!state.scroll().modal);
    }

    #[test]
    fn successful_passphrase_change_refreshes_security_facts_and_integrity_state() {
        let mut state = State::default();
        apply(
            &mut state,
            Action::VaultOpened(Box::new(Ok(marked_opened("before", false, 1)))),
        );
        checked_integrity(&mut state);

        apply(
            &mut state,
            Action::PassphraseChanged(Box::new(crate::engine::Mutation {
                outcome: Ok(()),
                refreshed: Ok(marked_opened("after", false, 9)),
            })),
        );

        assert!(matches!(state.rotate, RotateState::Done));
        assert!(matches!(state.integrity, IntegrityState::None));
        assert_eq!(state.vault().facts.key_file.epoch, 9);
        assert_eq!(state.vault().facts.audit.as_ref().unwrap().entries, 9);
        let opened = state
            .session
            .opened()
            .expect("the refreshed session stays open");
        assert_eq!(opened.facts.key_file.epoch, 9);
        assert_eq!(opened.facts.audit.as_ref().unwrap().entries, 9);
    }

    #[test]
    fn key_rotation_follow_up_warning_is_not_reported_as_a_failed_rotation() {
        let mut state = State::default();
        apply(
            &mut state,
            Action::VaultOpened(Box::new(Ok(marked_opened("before", false, 1)))),
        );
        checked_integrity(&mut state);
        let warning = StudioError::new(Kind::Completed, "audit append failed after rotation");
        apply(
            &mut state,
            Action::PassphraseChanged(Box::new(crate::engine::Mutation {
                outcome: Err(warning),
                refreshed: Ok(marked_opened("after", false, 10)),
            })),
        );
        assert!(matches!(state.rotate, RotateState::Warning(_)));
        assert!(matches!(state.integrity, IntegrityState::None));
        assert_eq!(state.vault().facts.key_file.epoch, 10);
        assert_eq!(state.vault().facts.audit.as_ref().unwrap().entries, 10);
    }

    #[test]
    fn passphrase_refresh_failure_becomes_a_completed_warning_without_green_integrity() {
        let mut state = State::default();
        open(&mut state, "before", false);
        checked_integrity(&mut state);

        apply(
            &mut state,
            Action::PassphraseChanged(Box::new(crate::engine::Mutation {
                outcome: Ok(()),
                refreshed: Err(error("refresh failed")),
            })),
        );

        let RotateState::Warning(warning) = &state.rotate else {
            panic!("a completed passphrase change with a failed refresh must warn");
        };
        assert_eq!(warning.kind, Kind::Completed);
        assert!(warning.detail.contains("passphrase changed"));
        assert!(warning.detail.contains("refresh failed"));
        assert!(matches!(state.integrity, IntegrityState::None));
    }

    #[test]
    fn forget_refresh_failure_keeps_the_receipt_and_reports_completion() {
        let mut state = State::default();
        apply(
            &mut state,
            Action::VaultOpened(Box::new(Ok(marked_opened("before", true, 1)))),
        );
        checked_integrity(&mut state);
        apply(
            &mut state,
            Action::EngineBusy(Busy::Forgetting("notes".to_owned())),
        );

        apply(
            &mut state,
            Action::Forgotten {
                region: "notes".to_owned(),
                asked: vec![1],
                mutation: Box::new(crate::engine::Mutation {
                    outcome: Ok(erasure_receipt(1)),
                    refreshed: Err(error("refresh failed")),
                }),
            },
        );

        let warning = state
            .maintenance_error
            .as_ref()
            .expect("the completed erasure must retain its refresh warning");
        assert_eq!(warning.kind, Kind::Completed);
        assert!(warning.detail.contains("atom was forgotten"));
        assert!(warning.detail.contains("refresh failed"));
        assert_eq!(state.vault().receipts.len(), 1);
        assert_eq!(state.vault().receipts[0].receipt.slots_erased[0].atom_id, 1);
        assert!(matches!(state.integrity, IntegrityState::None));
    }

    #[test]
    fn shortcut_labels_follow_the_command_modifier() {
        if cfg!(target_os = "macos") {
            assert_eq!(shortcut("I"), "⌘I");
            assert_eq!(shortcut("Shift W"), "⇧⌘W");
        } else {
            assert_eq!(shortcut("I"), "Ctrl I");
            assert_eq!(shortcut("Shift W"), "Ctrl Shift W");
        }
    }

    #[test]
    fn query_editor_identity_changes_with_the_vault_session() {
        let mut state = State::default();
        open(&mut state, "first", false);
        let first = state.query_editor_id();
        apply(&mut state, Action::VaultClosed);
        open(&mut state, "second", false);

        assert_ne!(first, state.query_editor_id());
        assert!(!state.query_dirty);
        assert!(state.query.is_empty());
    }

    #[test]
    fn late_query_replies_cannot_replace_a_newer_execution() {
        let mut state = State::default();
        open(&mut state, "vault", false);
        apply(&mut state, Action::EditQuery("SELECT 1".into()));
        let first = state.next_query_request();
        apply(&mut state, Action::QueryStarted(first.clone()));

        apply(&mut state, Action::EditQuery("SELECT 2".into()));
        let second = state.next_query_request();
        apply(&mut state, Action::QueryStarted(second.clone()));
        apply(
            &mut state,
            Action::QueryFinished {
                request: first,
                run: Box::new(Run::default()),
                refreshed: None,
            },
        );
        assert_eq!(state.result.request(), Some(&second));
        assert!(state.result.busy());

        apply(
            &mut state,
            Action::QueryFinished {
                request: second,
                run: Box::new(Run::default()),
                refreshed: None,
            },
        );
        assert!(!state.query_outcome_stale());
        apply(&mut state, Action::EditQuery("SELECT 3".into()));
        assert!(state.query_outcome_stale());
    }

    #[test]
    fn ime_preedit_is_never_a_submittable_query() {
        let mut state = State::default();
        open(&mut state, "vault", false);
        apply(&mut state, Action::EditQuery("SELECT 'before'".into()));
        apply(&mut state, Action::BeginQueryComposition);
        apply(
            &mut state,
            Action::EditQuery("SELECT 'before日本語'".into()),
        );

        assert!(state.query_blocker().is_some());
        assert_eq!(state.query_committed, "SELECT 'before'");
        apply(&mut state, Action::CancelQueryComposition);
        assert_eq!(state.query, "SELECT 'before'");
    }
}

#[cfg(test)]
mod camera_tests {
    use super::Camera;
    use egui::{pos2, vec2, Rect};

    fn canvas() -> Rect {
        Rect::from_min_size(pos2(100.0, 50.0), vec2(800.0, 600.0))
    }

    /// Screen and projection space must be exact inverses, or a click lands on a
    /// different point than the one under the cursor.
    #[test]
    fn round_trips_between_screen_and_projection() {
        let rect = canvas();
        for zoom in [1.0, 2.5, 17.0, 64.0] {
            let camera = Camera {
                centre: vec2(0.5, 0.5),
                zoom,
            };
            for at in [rect.center(), rect.left_top(), pos2(345.0, 210.0)] {
                let back = camera.to_screen(rect, camera.to_projection(rect, at));
                assert!(
                    (back - at).length() < 0.01,
                    "at zoom {zoom} the point {at:?} came back as {back:?}"
                );
            }
        }
    }

    /// Why zoom takes an anchor: the target stays under the pointer.
    #[test]
    fn zoom_keeps_the_anchor_under_the_pointer() {
        let rect = canvas();
        let at = pos2(300.0, 500.0);
        let mut camera = Camera::default();
        let before = camera.to_projection(rect, at);

        for _ in 0..6 {
            camera = camera.zoomed_at(rect, at, 1.4);
        }
        assert!(camera.zoom > 4.0, "six steps should have zoomed in");

        let after = camera.to_projection(rect, at);
        assert!(
            (after - before).length() < 0.002,
            "the projection under the pointer moved from {before:?} to {after:?}"
        );
    }

    #[test]
    fn zoom_is_bounded_at_both_ends() {
        let rect = canvas();
        let mut out = Camera::default();
        for _ in 0..40 {
            out = out.zoomed_at(rect, rect.center(), 2.0);
        }
        assert_eq!(out.zoom, Camera::MAX_ZOOM);
        for _ in 0..40 {
            out = out.zoomed_at(rect, rect.center(), 0.5);
        }
        assert_eq!(out.zoom, Camera::MIN_ZOOM);
    }

    /// A drag must not fling the projection off screen.
    #[test]
    fn panning_cannot_lose_the_projection() {
        for zoom in [1.0, 3.0, 64.0] {
            let flung = Camera {
                centre: vec2(-40.0, 90.0),
                zoom,
            }
            .clamped();
            let half = 0.5 / zoom;
            assert!(
                flung.centre.x >= half.min(1.0 - half) - 0.001
                    && flung.centre.x <= half.max(1.0 - half) + 0.001,
                "at zoom {zoom} the centre escaped to {:?}",
                flung.centre
            );
            // The view still covers part of the projection, which is the point.
            let visible =
                Rect::from_center_size(flung.centre.to_pos2(), vec2(1.0 / zoom, 1.0 / zoom));
            assert!(
                visible.intersects(Rect::from_min_size(pos2(0.0, 0.0), vec2(1.0, 1.0))),
                "at zoom {zoom} the view sees nothing"
            );
        }
    }

    /// At zoom 1 the projection exactly fills the view, so there is nowhere to pan to.
    #[test]
    fn zoom_one_is_pinned_to_the_middle() {
        let out = Camera {
            centre: vec2(0.9, 0.1),
            zoom: 1.0,
        }
        .clamped();
        assert!((out.centre - vec2(0.5, 0.5)).length() < 0.001);
    }
}
