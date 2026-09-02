//! Typed channel boundary to the thread owning the non-`Send` database connection.
//!
//! Blocking KDF and database work stays off the UI thread.

mod actor;
mod demo;
pub mod error;
pub mod memory;
pub mod session;
pub mod value;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

use citadel_sql::QueryResult;

pub use error::{Kind, StudioError};
pub use session::{CreateSpec, Run, Statement};

/// Work the interface sends to the engine actor.
#[derive(Debug)]
pub enum Command {
    Open {
        path: PathBuf,
        passphrase: crate::state::Passphrase,
    },
    Create(Box<CreateSpec>),
    /// Create and seed a disposable demo vault.
    OpenDemo,
    Close,
    /// Walk every page on demand.
    CheckIntegrity,
    /// Rewrap the key under a new passphrase.
    ChangePassphrase {
        current: crate::state::Passphrase,
        next: crate::state::Passphrase,
    },
    Run(String),
    Import(String),
    /// `analyze` executes the statement.
    Explain {
        sql: String,
        analyze: bool,
    },
    Browse {
        table: String,
        limit: usize,
    },
    /// Load one bounded vector-column sample after its document is opened.
    SampleVector {
        table: String,
        column: String,
    },
    Atoms {
        region: String,
        after: Option<citadel_mem::AtomId>,
    },
    /// Verify exactly the visible ids.
    Verify {
        region: String,
        ids: Vec<citadel_mem::AtomId>,
    },
    Forget {
        region: String,
        ids: Vec<citadel_mem::AtomId>,
    },
}

/// Engine reply; fallible operations carry their own `Result`.
#[derive(Debug)]
pub enum Reply {
    /// The actor exited before it could answer. Unlike an ordinary operation error,
    /// this invalidates the session represented by the UI.
    Stopped(StudioError),
    Opened(Box<Result<Opened, StudioError>>),
    Closed,
    Checked(Box<Result<IntegrityFacts, StudioError>>),
    PassphraseChanged(Box<Mutation<()>>),
    Ran {
        run: Box<Run>,
        /// Present only when the authenticated root changed.
        refreshed: Option<Box<Result<Opened, StudioError>>>,
    },
    Imported(Box<Mutation<()>>),
    Explained {
        plan: Box<Result<QueryPlan, StudioError>>,
        /// `EXPLAIN ANALYZE` may execute a mutation.
        refreshed: Option<Box<Result<Opened, StudioError>>>,
    },
    Browsed {
        table: String,
        rows: Box<Result<QueryResult, StudioError>>,
    },
    VectorSampled {
        table: String,
        column: String,
        sample: Box<Result<VectorFacts, StudioError>>,
    },
    Atoms {
        region: String,
        page: Box<Result<memory::AtomPage, StudioError>>,
    },
    Verified {
        region: String,
        asked: Vec<citadel_mem::AtomId>,
        verdicts: Box<Result<Vec<citadel_mem::AtomAttestation>, StudioError>>,
    },
    Forgotten {
        region: String,
        /// Echoed back because a plaintext region destroys no keys, so `slots_erased` is
        /// empty there and the receipt alone cannot name which rows went.
        asked: Vec<citadel_mem::AtomId>,
        mutation: Box<Mutation<citadel_mem::ErasureReceipt>>,
    },
}

/// A mutation's primary result and the authoritative vault state observed afterwards.
///
/// Erasure and passphrase rotation can cross an irreversible boundary before a later
/// step reports an error. Returning both facts prevents the interface from preserving
/// a stale root, key inventory, audit summary, or successful integrity badge.
#[derive(Clone, Debug)]
pub struct Mutation<T> {
    pub outcome: Result<T, StudioError>,
    pub refreshed: Result<Opened, StudioError>,
}

/// Statement plan and whether producing it executed the statement.
#[derive(Clone, Debug)]
pub struct QueryPlan {
    pub lines: Vec<String>,
    pub measured: bool,
}

/// Facts read through the open handle, which holds the exclusive file lock.
#[derive(Clone, Debug)]
pub struct VaultFacts {
    pub stats: citadel::DbStats,
    pub key_file: citadel::KeyFileInfo,
    pub keys: citadel::KeyStoreFacts,
    /// `None` when this vault keeps no audit log.
    pub audit: Option<AuditFacts>,
}

/// Audit-log state read at open.
#[derive(Clone, Debug)]
pub struct AuditFacts {
    pub entries: u64,
    /// Retained segments with local chains and cross-segment links verified.
    pub segments: Vec<AuditSegment>,
    /// Initial unauthenticated count shortfall retained before open rewrites the header.
    pub live_count_shortfall_at_open: u64,
}

impl AuditFacts {
    pub fn chain_links(&self) -> bool {
        self.segments.iter().all(|s| s.chain_valid)
    }

    /// Mutable count mismatch, not authenticated rollback evidence.
    pub fn count_shortfall(&self) -> u64 {
        self.segments
            .iter()
            .fold(self.live_count_shortfall_at_open, |total, segment| {
                total.saturating_add(segment.count_shortfall)
            })
    }

    /// No local inconsistency; does not prove external freshness.
    pub fn has_no_detected_inconsistency(&self) -> bool {
        self.chain_links() && self.count_shortfall() == 0
    }
}

#[derive(Clone, Debug)]
pub struct AuditSegment {
    pub name: String,
    pub entries: u64,
    pub chain_valid: bool,
    /// Which entry the chain stops verifying at.
    pub break_at: Option<u64>,
    /// Mutable count shortfall; the live segment's initial value is stored separately.
    pub count_shortfall: u64,
}

/// One integrity-walk problem with its engine classification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntegrityProblem {
    /// Original engine wording.
    pub message: String,
    pub tampered: bool,
}

impl IntegrityProblem {
    fn from_error(error: &citadel::IntegrityError) -> Self {
        Self {
            message: error.to_string(),
            tampered: error.is_tamper(),
        }
    }
}

/// On-demand full-disk integrity result.
#[derive(Clone, Debug)]
pub struct IntegrityFacts {
    pub pages_checked: u64,
    pub errors: Vec<IntegrityProblem>,
}

impl IntegrityFacts {
    /// Problems proving on-disk bytes changed.
    pub fn tampered(&self) -> usize {
        self.errors.iter().filter(|error| error.tampered).count()
    }
}

/// Description of the currently open vault.
#[derive(Clone, Debug)]
pub struct Opened {
    pub label: String,
    pub path: PathBuf,
    /// Explicit session identity; a temporary path is not a durable demo marker.
    pub is_demo: bool,
    pub facts: VaultFacts,
    pub tables: Vec<TableFacts>,
    /// Empty without a memory schema; discovery must not bootstrap one.
    pub regions: Vec<memory::RegionFacts>,
    /// Vector-column descriptors. Their samples are loaded only when requested.
    pub vectors: Vec<VectorFacts>,
}

#[derive(Clone, Debug)]
pub struct TableFacts {
    pub name: String,
    pub rows: u64,
    pub columns: Vec<ColumnFacts>,
    /// Storage owned by the memory API, classified by `citadel_mem::owns_table`.
    pub engine_owned: bool,
}

/// Column facts preserving original constraint expressions.
#[derive(Clone, Debug)]
pub struct ColumnFacts {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub default_sql: Option<String>,
    pub check_sql: Option<String>,
    pub generated_sql: Option<String>,
    /// Set directly from the schema type for `VECTOR(n)`.
    pub vector_dim: Option<u16>,
}

/// Vector-column sample mapped into normalized canvas coordinates.
#[derive(Clone, Debug)]
pub struct VectorFacts {
    pub table: String,
    pub column: String,
    pub dim: u32,
    pub total: u64,
    /// Axis-aligned dimensions 0 and 1, normalized across an on-demand sample.
    /// Empty on the catalog-opening path.
    pub points: Vec<(f32, f32)>,
}

/// Actor command paired with the cancellation-token id registered before send.
struct Work {
    id: u64,
    command: Command,
}

struct Answer {
    id: u64,
    reply: Reply,
}

/// Out-of-band cancellation state shared with the actor thread.
///
/// The actor cannot receive a command while executing a query. Each command gets a fresh
/// token registered by id before send; tripped tokens are never reused.
#[derive(Default)]
struct CancellationState {
    active: Option<(u64, citadel::CancelToken)>,
    queued: VecDeque<(u64, citadel::CancelToken)>,
}

impl CancellationState {
    fn queue(&mut self, id: u64, token: citadel::CancelToken) {
        self.queued.push_back((id, token));
    }

    fn undo_queue(&mut self, id: u64) {
        let (queued_id, _) = self
            .queued
            .pop_back()
            .expect("a failed send has a queued cancel token");
        assert_eq!(queued_id, id, "cancel tokens follow channel order");
    }

    fn activate(&mut self, id: u64) -> citadel::CancelToken {
        assert!(
            self.active.is_none(),
            "the actor runs one command at a time"
        );
        let (queued_id, token) = self
            .queued
            .pop_front()
            .expect("a received command has a queued cancel token");
        assert_eq!(queued_id, id, "cancel tokens follow channel order");
        self.active = Some((id, token.clone()));
        token
    }

    fn finish(&mut self, id: u64) {
        let (active_id, _) = self.active.take().expect("a finished command was active");
        assert_eq!(active_id, id, "only the active command can finish");
    }

    fn cancel(&self, id: u64) {
        if let Some((_, token)) = self.active.as_ref().filter(|(active, _)| *active == id) {
            token.cancel();
            return;
        }
        if let Some((_, token)) = self.queued.iter().find(|(queued, _)| *queued == id) {
            token.cancel();
        }
    }

    fn cancel_all(&self) {
        if let Some((_, token)) = &self.active {
            token.cancel();
        }
        for (_, token) in &self.queued {
            token.cancel();
        }
    }
}

type CancelState = std::sync::Arc<std::sync::Mutex<CancellationState>>;

pub struct Handle {
    tx: Sender<Work>,
    rx: Receiver<Answer>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Commands sent and not yet answered.
    pending: usize,
    next_id: u64,
    /// User-requested operation targeted by the visible Cancel action.
    foreground: Option<u64>,
    cancel: CancelState,
}

impl Handle {
    pub fn spawn() -> Self {
        let (to_engine, commands) = std::sync::mpsc::channel();
        let (replies, from_engine) = std::sync::mpsc::channel();
        let cancel: CancelState = Default::default();
        let worker = std::sync::Arc::clone(&cancel);
        let thread = std::thread::Builder::new()
            .name("citadel-engine".to_owned())
            .spawn(move || actor::run(&commands, &replies, &worker))
            .expect("spawn the engine thread");
        Self {
            tx: to_engine,
            rx: from_engine,
            thread: Some(thread),
            pending: 0,
            next_id: 0,
            foreground: None,
            cancel,
        }
    }

    /// Queue foreground work. `Ok(false)` means another foreground command owns the
    /// slot; `Err` means the actor has exited and the current session is no longer valid.
    pub fn send(&mut self, command: Command) -> Result<bool, StudioError> {
        if self.foreground.is_some() {
            return Ok(false);
        }
        let id = self.queue(command)?;
        self.foreground = Some(id);
        Ok(true)
    }

    /// Queue background work outside the visible cancellation target.
    pub fn send_background(&mut self, command: Command) -> Result<(), StudioError> {
        self.queue(command).map(|_| ())
    }

    fn queue(&mut self, command: Command) -> Result<u64, StudioError> {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("the command sequence does not exhaust u64");

        // Register before send so cancel-before-start sticks to this command.
        let mut cancel = self.cancel.lock().expect("cancel state");
        cancel.queue(id, citadel::CancelToken::new());
        if self.tx.send(Work { id, command }).is_ok() {
            self.pending += 1;
            Ok(id)
        } else {
            cancel.undo_queue(id);
            Err(stopped_error())
        }
    }

    /// Trip the foreground token without waiting for the ordinary reply.
    pub fn cancel(&self) {
        if let Some(id) = self.foreground {
            self.cancel.lock().expect("cancel state").cancel(id);
        }
    }

    /// Poll the next reply without blocking.
    pub fn poll(&mut self) -> Option<Reply> {
        match self.rx.try_recv() {
            Ok(answer) => {
                self.pending = self.pending.saturating_sub(1);
                if self.foreground == Some(answer.id) {
                    self.foreground = None;
                }
                Some(answer.reply)
            }
            Err(TryRecvError::Empty) => None,
            // Convert an actor panic into a terminal reply instead of repainting forever.
            Err(TryRecvError::Disconnected) => {
                if self.pending == 0 {
                    return None;
                }
                self.pending = 0;
                self.foreground = None;
                Some(Reply::Stopped(stopped_error()))
            }
        }
    }

    pub fn busy(&self) -> bool {
        self.pending > 0
    }

    pub fn foreground_busy(&self) -> bool {
        self.foreground.is_some()
    }
}

fn stopped_error() -> StudioError {
    StudioError::new(Kind::Io, "the engine stopped before it answered")
}

impl Drop for Handle {
    /// Wait for the actor to release its database lock.
    fn drop(&mut self) {
        self.cancel.lock().expect("cancel state").cancel_all();
        let (tx, _) = std::sync::mpsc::channel();
        drop(std::mem::replace(&mut self.tx, tx));
        // Dropping replies makes the actor exit instead of draining queued work.
        let (_answers, rx) = std::sync::mpsc::channel();
        drop(std::mem::replace(&mut self.rx, rx));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("pending", &self.pending)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integrity_problems_keep_each_errors_tamper_classification() {
        let changed =
            IntegrityProblem::from_error(&citadel::IntegrityError::CommitSlotChecksumMismatch {
                slot: 1,
            });
        let structural =
            IntegrityProblem::from_error(&citadel::IntegrityError::EntryCountMismatch {
                expected: 7,
                actual: 6,
            });
        let facts = IntegrityFacts {
            pages_checked: 3,
            errors: vec![changed.clone(), structural.clone()],
        };

        assert!(changed.tampered);
        assert!(!structural.tampered);
        assert_eq!(
            changed.message,
            "commit slot 1 failed its checksum; its write was torn or altered"
        );
        assert_eq!(facts.tampered(), 1);
    }

    #[test]
    fn queuing_another_command_does_not_retarget_active_cancellation() {
        let first = citadel::CancelToken::new();
        let second = citadel::CancelToken::new();
        let mut state = CancellationState::default();

        state.queue(7, first.clone());
        let installed = state.activate(7);
        state.queue(8, second.clone());

        state.cancel(7);
        assert!(
            first.is_cancelled(),
            "the selected active command is cancelled"
        );
        assert!(
            installed.is_cancelled(),
            "the database sees the active command's token"
        );
        assert!(
            !second.is_cancelled(),
            "queued work keeps its independent token"
        );
    }

    #[test]
    fn foreground_cancellation_does_not_trip_an_active_background_read() {
        let background = citadel::CancelToken::new();
        let foreground = citadel::CancelToken::new();
        let mut state = CancellationState::default();

        state.queue(20, background.clone());
        state.activate(20);
        state.queue(21, foreground.clone());
        state.cancel(21);

        assert!(
            !background.is_cancelled(),
            "the visible Cancel action must not retarget an older background read"
        );
        assert!(foreground.is_cancelled());
        state.finish(20);
        assert!(state.activate(21).is_cancelled());
    }

    #[test]
    fn cancellation_between_commands_reaches_the_named_queued_command() {
        let first = citadel::CancelToken::new();
        let second = citadel::CancelToken::new();
        let mut state = CancellationState::default();

        state.queue(12, first);
        state.activate(12);
        state.queue(13, second.clone());
        state.finish(12);

        // Cancellation between commands must already be visible to the next command.
        state.cancel(13);
        let installed = state.activate(13);
        assert!(second.is_cancelled());
        assert!(installed.is_cancelled());

        // Completion removes the token; cancellation remains idempotent.
        state.cancel(13);
        state.finish(13);
        state.cancel(13);
    }

    fn wait(handle: &mut Handle) -> Reply {
        for _ in 0..600 {
            if let Some(reply) = handle.poll() {
                return reply;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("the engine never answered");
    }

    fn disconnected_handle(pending: usize, foreground: Option<u64>) -> Handle {
        let (tx, commands) = std::sync::mpsc::channel::<Work>();
        drop(commands);
        let (answers, rx) = std::sync::mpsc::channel::<Answer>();
        drop(answers);
        Handle {
            tx,
            rx,
            thread: None,
            pending,
            next_id: 0,
            foreground,
            cancel: Default::default(),
        }
    }

    #[test]
    fn a_stopped_actor_is_not_reported_as_an_open_failure() {
        let mut idle = disconnected_handle(0, None);
        let refused = idle
            .send(Command::Run("SELECT 1;".into()))
            .expect_err("a closed actor channel must be reported");
        assert_eq!(refused.kind, Kind::Io);
        assert!(!idle.busy());
        assert!(!idle.foreground_busy());

        let mut waiting = disconnected_handle(1, Some(7));
        assert!(matches!(waiting.poll(), Some(Reply::Stopped(_))));
        assert!(!waiting.busy());
        assert!(!waiting.foreground_busy());
    }

    #[test]
    fn only_one_foreground_command_can_be_queued() {
        let mut handle = Handle::spawn();
        assert!(handle
            .send(Command::Run("SELECT 1;".into()))
            .expect("the engine is running"));
        assert!(handle.foreground_busy());
        assert!(
            !handle
                .send(Command::Run("SELECT 2;".into()))
                .expect("the engine is running"),
            "a repeated shortcut must not queue a second foreground operation"
        );

        let _ = wait(&mut handle);
        assert!(!handle.foreground_busy());
        assert!(handle
            .send(Command::Run("SELECT 3;".into()))
            .expect("the engine is running"));
        let _ = wait(&mut handle);
    }

    #[test]
    fn cancelling_an_atomic_import_leaves_no_committed_prefix() {
        let mut handle = Handle::spawn();
        handle
            .send(Command::OpenDemo)
            .expect("the engine is running");
        let Reply::Opened(opened) = wait(&mut handle) else {
            panic!("the demo did not answer with Opened");
        };
        opened.expect("the demo opens");

        handle
            .send(Command::Import(
                "CREATE TABLE cancelled_import_prefix (id INTEGER PRIMARY KEY); \
                 INSERT INTO cancelled_import_prefix \
                 SELECT a.id * 10000 + b.id FROM documents a CROSS JOIN documents b;"
                    .to_owned(),
            ))
            .expect("the engine is running");
        // Give the batch a chance to execute its first statement before cancellation
        // reaches the deliberately expensive second statement.
        std::thread::sleep(std::time::Duration::from_millis(10));
        let imported = loop {
            handle.cancel();
            if let Some(reply) = handle.poll() {
                break reply;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        };
        let Reply::Imported(imported) = imported else {
            panic!("the import did not answer with Imported");
        };
        let error = imported
            .outcome
            .expect_err("the import completed after cancellation");
        assert_eq!(error.kind, Kind::Cancelled);
        imported
            .refreshed
            .expect("the cancelled import reconciles the open vault");

        handle
            .send(Command::Run(
                "CREATE TABLE cancelled_import_prefix (id INTEGER PRIMARY KEY);".to_owned(),
            ))
            .expect("the engine is running");
        let Reply::Ran { run, .. } = wait(&mut handle) else {
            panic!("the follow-up statement did not answer with Ran");
        };
        assert!(
            run.failed.is_none(),
            "the cancelled batch committed its first table: {:?}",
            run.failed
        );
    }

    #[test]
    fn dropping_a_handle_cancels_work_and_joins_the_worker() {
        let mut handle = Handle::spawn();
        assert!(handle
            .send(Command::OpenDemo)
            .expect("the engine is running"));
        let _ = wait(&mut handle);
        assert!(handle
            .send(Command::Run(
                "SELECT SUM(a.id + b.id + c.id) FROM documents a \
             CROSS JOIN documents b CROSS JOIN documents c;"
                    .into(),
            ))
            .expect("the engine is running"));

        let (finished, done) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            drop(handle);
            let _ = finished.send(());
        });
        done.recv_timeout(std::time::Duration::from_secs(10))
            .expect("dropping Studio must cancel work before joining its engine thread");
    }

    #[test]
    fn the_demo_opens_and_reports_its_tables() {
        let mut handle = Handle::spawn();
        handle
            .send(Command::OpenDemo)
            .expect("the engine is running");
        assert!(handle.busy(), "a sent command is owed an answer");

        let Reply::Opened(opened) = wait(&mut handle) else {
            panic!("OpenDemo should answer with Opened");
        };
        let opened = opened.expect("the demo vault opens");
        let documents = opened
            .tables
            .iter()
            .find(|t| t.name == "documents")
            .expect("the demo seeds documents");
        assert_eq!(documents.rows, 1_500);
        assert!(documents.columns.iter().any(|c| c.name == "embedding"));
        assert!(
            documents.columns.iter().any(|c| c.primary_key),
            "the schema records which column is the key"
        );
        assert!(!handle.busy());
    }

    #[test]
    fn vector_points_are_loaded_only_by_the_vector_sample_command() {
        let mut handle = Handle::spawn();
        handle
            .send(Command::OpenDemo)
            .expect("the engine is running");
        let Reply::Opened(opened) = wait(&mut handle) else {
            panic!("OpenDemo should answer with Opened");
        };
        let opened = opened.expect("the demo vault opens");
        let descriptor = opened
            .vectors
            .iter()
            .find(|vector| vector.table == "documents" && vector.column == "embedding")
            .expect("the demo exposes its vector column");
        assert!(
            descriptor.points.is_empty(),
            "vault open reads metadata only"
        );

        handle
            .send_background(Command::SampleVector {
                table: "documents".into(),
                column: "embedding".into(),
            })
            .expect("the engine is running");
        let Reply::VectorSampled { sample, .. } = wait(&mut handle) else {
            panic!("SampleVector should answer with VectorSampled");
        };
        let sample = sample.expect("the vector document loads its sample");
        assert_eq!(sample.points.len(), 1_500);
    }

    #[test]
    fn a_statement_runs_and_its_rows_come_back() {
        let mut handle = Handle::spawn();
        handle
            .send(Command::OpenDemo)
            .expect("the engine is running");
        wait(&mut handle);

        handle
            .send(Command::Run(
                "SELECT name FROM customers ORDER BY id;".into(),
            ))
            .expect("the engine is running");
        let Reply::Ran { run, refreshed } = wait(&mut handle) else {
            panic!("Run should answer with Ran");
        };
        assert!(refreshed.is_none(), "a read does not refresh the catalog");
        assert!(run.failed.is_none(), "{:?}", run.failed);
        let rows = run.last_rows().expect("a SELECT returns rows");
        assert_eq!(rows.columns, vec!["name".to_owned()]);
        assert_eq!(rows.rows.len(), 8);
    }

    #[test]
    fn mutating_sql_returns_a_refreshed_catalog() {
        let mut handle = Handle::spawn();
        assert!(handle
            .send(Command::OpenDemo)
            .expect("the engine is running"));
        let _ = wait(&mut handle);

        assert!(handle
            .send(Command::Run(
                "CREATE TABLE refreshed (id INTEGER PRIMARY KEY); \
             INSERT INTO refreshed VALUES (1) RETURNING id;"
                    .into(),
            ))
            .expect("the engine is running"));
        let Reply::Ran { run, refreshed } = wait(&mut handle) else {
            panic!("Run should answer with Ran");
        };
        assert!(run.failed.is_none(), "{:?}", run.failed);
        assert!(run.storage_changed);
        let opened = refreshed
            .expect("a changed authenticated root refreshes the catalog")
            .expect("the refreshed catalog is readable");
        let table = opened
            .tables
            .iter()
            .find(|table| table.name == "refreshed")
            .expect("the refreshed catalog includes the new table");
        assert_eq!(table.rows, 1, "DML returning rows still refreshes counts");
    }

    #[test]
    fn running_without_a_vault_is_refused_not_ignored() {
        let mut handle = Handle::spawn();
        handle
            .send(Command::Run("SELECT 1;".into()))
            .expect("the engine is running");
        let Reply::Ran { run, refreshed } = wait(&mut handle) else {
            panic!("Run should answer with Ran even when it cannot run");
        };
        assert!(refreshed.is_none());
        let failed = run.failed.expect("there is no vault to run against");
        assert_eq!(failed.kind, Kind::Usage);
    }

    #[test]
    fn closing_removes_the_disposable_demo() {
        let mut handle = Handle::spawn();
        handle
            .send(Command::OpenDemo)
            .expect("the engine is running");
        let Reply::Opened(opened) = wait(&mut handle) else {
            panic!("expected Opened");
        };
        let first = opened.expect("opens").path;
        assert!(first.exists());

        handle.send(Command::Close).expect("the engine is running");
        assert!(matches!(wait(&mut handle), Reply::Closed));
        assert!(!first.exists(), "closing must delete the disposable vault");

        handle
            .send(Command::OpenDemo)
            .expect("the engine is running");
        let Reply::Opened(reopened) = wait(&mut handle) else {
            panic!("expected Opened");
        };
        let second = reopened.expect("the next demo opens").path;
        assert_ne!(first, second);
    }
}
