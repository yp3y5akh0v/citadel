//! One open vault: database, borrowed connection, and lazy memory engine.
//!
//! `self_cell` holds the self-referential database/connection pair on its actor thread.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use citadel::{Argon2Profile, Database, DatabaseBuilder, KdfAlgorithm};
use citadel_mem::{MemoryEngine, MemoryMaintenance};
use citadel_sql::{Connection, ExecutionResult, QueryResult, TableSchema};
use self_cell::self_cell;

use super::error::{IntoStudioError, Kind, StudioError};

/// Rows retained from each statement for the query grid.
pub const RETAINED_QUERY_ROWS: usize = 2_000;
/// Refuse one materialized storage value before a desktop query allocates without bound.
const QUERY_MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
/// Shared storage-materialization allowance across one editor script.
const QUERY_TOTAL_READ_BYTES: usize = 64 * 1024 * 1024;

self_cell!(
    struct DbCell {
        owner: Arc<Database>,
        #[not_covariant]
        dependent: Connection,
    }
);

/// Statement result preserving DDL, row-count, and row-set distinctions.
#[derive(Clone, Debug)]
pub enum Statement {
    Rows(RowSet),
    Changed(u64),
    Ok,
}

/// A bounded retained prefix plus the exact number of rows the engine returned.
#[derive(Clone, Debug)]
pub struct RowSet {
    pub result: QueryResult,
    pub total_rows: usize,
}

impl std::ops::Deref for RowSet {
    type Target = QueryResult;

    fn deref(&self) -> &Self::Target {
        &self.result
    }
}

impl From<ExecutionResult> for Statement {
    fn from(result: ExecutionResult) -> Self {
        match result {
            ExecutionResult::Query(mut result) => {
                let total_rows = result.rows.len();
                result.rows.truncate(RETAINED_QUERY_ROWS);
                Self::Rows(RowSet { result, total_rows })
            }
            ExecutionResult::RowsAffected(n) => Self::Changed(n),
            ExecutionResult::Ok => Self::Ok,
        }
    }
}

/// Script outcome retaining committed prefixes and the first failure.
#[derive(Clone, Debug, Default)]
pub struct Run {
    pub statements: Vec<Statement>,
    pub failed: Option<StudioError>,
    /// Whether the script published a commit, independent of result shape.
    pub storage_changed: bool,
}

impl Run {
    /// Last row-producing statement.
    pub fn last_rows(&self) -> Option<&QueryResult> {
        self.statements.iter().rev().find_map(|s| match s {
            Statement::Rows(rows) => Some(&rows.result),
            _ => None,
        })
    }

    pub fn last_row_count(&self) -> Option<usize> {
        self.statements
            .iter()
            .rev()
            .find_map(|statement| match statement {
                Statement::Rows(rows) => Some(rows.total_rows),
                _ => None,
            })
    }
}

/// Validated vault-creation request.
#[derive(Clone, Debug)]
pub struct CreateSpec {
    pub path: PathBuf,
    pub passphrase: crate::state::Passphrase,
    pub kdf: KdfAlgorithm,
    pub profile: Argon2Profile,
}

const DEMO_PASSPHRASE: &str = "citadel-studio-demo";

pub struct Session {
    cell: DbCell,
    /// Opened lazily because memory bootstrap and recovery can write.
    mem: Option<MemoryEngine>,
    /// Model-free inventory, attestation, and erasure.
    maintenance: Option<MemoryMaintenance>,
    path: PathBuf,
    is_demo: bool,
    /// Declared last so every database handle and file lock drops before cleanup runs.
    demo_dir: Option<tempfile::TempDir>,
}

impl Session {
    /// Open an existing vault under its lifetime exclusive lock.
    pub fn open(path: &Path, passphrase: &str) -> Result<Self, StudioError> {
        let db = DatabaseBuilder::new(path)
            .passphrase(passphrase.as_bytes())
            // Region wrap keys are required for attestation and cryptographic erasure.
            .enable_region_keys(true)
            .open()
            .map_err(IntoStudioError::into_studio)?;
        Self::wrap(db, path.to_owned())
    }

    /// Create a vault and open it. The engine refuses a path whose sidecars outlive their
    /// database, so this cannot destroy key material for a backup of an older vault.
    pub fn create(spec: &CreateSpec) -> Result<Self, StudioError> {
        let db = DatabaseBuilder::new(&spec.path)
            .passphrase(spec.passphrase.as_bytes())
            .kdf_algorithm(spec.kdf)
            .argon2_profile(spec.profile)
            // Without region keys there are no encrypted regions, so no per-atom
            // attestation and no cryptographic erasure. Every vault Studio makes can
            // prove what it stores.
            .enable_region_keys(true)
            .create()
            .map_err(IntoStudioError::into_studio)?;
        Self::wrap(db, spec.path.clone())
    }

    /// Create a disposable, file-backed demo and seed it through the real engine.
    ///
    /// Region keys keep attestation and cryptographic erasure real. The `Iot` KDF profile
    /// keeps this demonstration responsive. The temporary directory owns the data file
    /// and every sidecar, so user mutations last for this session and never alter the seed
    /// presented by the next demo.
    pub fn demo() -> Result<Self, StudioError> {
        let demo_dir = tempfile::Builder::new()
            .prefix("citadel-studio-demo-")
            .tempdir()
            .map_err(IntoStudioError::into_studio)?;
        let path = demo_dir.path().join("demo.cdl");
        let db = DatabaseBuilder::new(&path)
            .passphrase(DEMO_PASSPHRASE.as_bytes())
            .argon2_profile(Argon2Profile::Iot)
            .enable_region_keys(true)
            .create()
            .map_err(IntoStudioError::into_studio)?;
        let mut session = Self::wrap(db, path.to_owned())?;
        session.is_demo = true;
        session.demo_dir = Some(demo_dir);
        super::demo::seed(&mut session)?;
        Ok(session)
    }

    fn wrap(db: Database, path: PathBuf) -> Result<Self, StudioError> {
        let cell = DbCell::try_new(Arc::new(db), |db| {
            Connection::open(db).map_err(IntoStudioError::into_studio)
        })?;
        Ok(Self {
            cell,
            mem: None,
            maintenance: None,
            path,
            is_demo: false,
            demo_dir: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_demo(&self) -> bool {
        self.is_demo
    }

    /// What the workspace calls this vault. The file stem, so a demo in a temporary
    /// directory does not show the temporary directory.
    pub fn label(&self) -> String {
        self.path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| self.path.display().to_string())
    }

    /// What this vault reports about itself, from the handle that holds it open.
    pub fn facts(&self) -> Result<super::VaultFacts, StudioError> {
        let db = self.database();
        Ok(super::VaultFacts {
            stats: db.stats(),
            key_file: db.key_file().clone(),
            keys: db.key_store_facts().map_err(IntoStudioError::into_studio)?,
            audit: self.audit_facts()?,
        })
    }

    /// The audit log's own account of itself. Every retained segment is verified, not just
    /// the live one, including v2 seed handoff and sequence/generation continuity between
    /// adjacent files. The oldest retained segment has no external anti-rollback anchor.
    fn audit_facts(&self) -> Result<Option<super::AuditFacts>, StudioError> {
        let db = self.database();
        if db.live_audit_entry_count().is_none() {
            return Ok(None);
        }
        let segments: Vec<super::AuditSegment> = db
            .verify_audit_chain()
            .map_err(IntoStudioError::into_studio)?
            .into_iter()
            .map(|(path, result)| super::AuditSegment {
                name: path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| path.display().to_string()),
                entries: result.entries_verified,
                chain_valid: result.chain_valid,
                break_at: result.chain_break_at,
                count_shortfall: result.entries_missing(),
            })
            .collect();
        let entries = segments.iter().map(|segment| segment.entries).sum();
        Ok(Some(super::AuditFacts {
            entries,
            segments,
            live_count_shortfall_at_open: db.audit_entries_missing().unwrap_or(0),
        }))
    }

    /// Rewrap the key under a new passphrase, proving the current one first.
    ///
    /// The vault stays open: only the key file changes, and the REK this handle already
    /// unwrapped is the same one afterwards.
    pub fn change_passphrase(&self, current: &str, next: &str) -> Result<(), StudioError> {
        if self.is_demo {
            return Err(refuse(
                Kind::Usage,
                "the disposable demo passphrase cannot be changed",
            ));
        }
        self.database()
            .change_passphrase(current.as_bytes(), next.as_bytes())
            .map_err(IntoStudioError::into_studio)
    }

    /// Walk every page and report what the engine found.
    ///
    /// `integrity_check_quiet`, so asking does not itself write an audit entry: a reader
    /// checking a vault should not change the thing being checked.
    pub fn integrity(&self) -> Result<super::IntegrityFacts, StudioError> {
        let report = self
            .database()
            .integrity_check_quiet()
            .map_err(IntoStudioError::into_studio)?;
        Ok(super::IntegrityFacts {
            pages_checked: report.pages_checked,
            errors: report
                .errors
                .iter()
                .map(super::IntegrityProblem::from_error)
                .collect(),
        })
    }

    pub fn database(&self) -> &Arc<Database> {
        self.cell.borrow_owner()
    }

    /// Run whatever is in the editor, exactly as written.
    ///
    /// `execute_script` rather than `execute_batch`: the batch form wraps everything in
    /// one transaction and aborts the lot, which hides which statement was wrong.
    /// Splitting the text here instead would duplicate a parser the engine already has.
    pub fn run(&self, sql: &str) -> Run {
        let before = self.database().manager().commit_generation();
        self.cell.with_dependent(|_, conn| {
            let budget =
                citadel_sql::ReadBudget::new(QUERY_MAX_VALUE_BYTES, QUERY_TOTAL_READ_BYTES);
            let script = conn.execute_script_with_read_budget(sql, &budget);
            Run {
                statements: script.completed.into_iter().map(Statement::from).collect(),
                failed: script.error.map(IntoStudioError::into_studio),
                storage_changed: self.database().manager().commit_generation() != before,
            }
        })
    }

    /// Execute Studio's generated import as one transaction. Unlike editor scripts, an
    /// import has no useful partial-success state: either every source table is created
    /// or the destination is unchanged.
    pub fn run_atomic(&self, sql: &str) -> Result<(), StudioError> {
        self.cell.with_dependent(|_, conn| {
            conn.execute_batch(sql)
                .map(|_| ())
                .map_err(IntoStudioError::into_studio)
        })
    }

    /// One statement Studio composed itself. Never used for text a reader typed: a
    /// single-statement path would silently drop everything after the first semicolon.
    pub fn query(&self, sql: &str) -> Result<QueryResult, StudioError> {
        self.cell
            .with_dependent(|_, conn| conn.query(sql).map_err(IntoStudioError::into_studio))
    }

    /// Consume a Studio-composed vector query row by row, retaining only the two
    /// coordinates used by the inspection plot.
    pub(super) fn sample_vector_xy(&self, sql: &str) -> Result<Vec<(f32, f32)>, StudioError> {
        self.cell.with_dependent(|_, conn| {
            let statement = conn.prepare(sql).map_err(IntoStudioError::into_studio)?;
            let mut rows = statement.query(&[]).map_err(IntoStudioError::into_studio)?;
            let mut sample = Vec::new();
            while let Some(row) = rows.next().map_err(IntoStudioError::into_studio)? {
                let Some(citadel_sql::Value::Vector(vector)) = row.get(0) else {
                    continue;
                };
                if let Some((&x, &y)) = vector.first().zip(vector.get(1)) {
                    sample.push((x, y));
                }
            }
            Ok(sample)
        })
    }

    /// The structural or measured plan for a statement, one line per row.
    pub fn explain(&self, sql: &str, analyze: bool) -> Result<super::QueryPlan, StudioError> {
        let keyword = if analyze {
            "EXPLAIN ANALYZE"
        } else {
            "EXPLAIN"
        };
        let plan = self.query(&format!("{keyword} {}", sql.trim().trim_end_matches(';')))?;
        Ok(super::QueryPlan {
            lines: plan
                .rows
                .iter()
                .filter_map(|row| row.first())
                .map(|value| value.to_string())
                .collect(),
            measured: analyze,
        })
    }

    pub fn tables(&self) -> Vec<String> {
        let mut names = self.cell.with_dependent(|_, conn| conn.tables());
        names.sort();
        names
    }

    pub fn table_schema(&self, name: &str) -> Option<TableSchema> {
        self.cell.with_dependent(|_, conn| conn.table_schema(name))
    }

    /// Whether the existing catalog contains the memory schema, without creating it.
    pub fn has_memory(&self) -> bool {
        self.table_schema("memory_regions").is_some()
    }

    /// The demo's memory engine, opened only when seeding needs its mutation surface.
    pub(super) fn demo_memory(&mut self) -> Result<&MemoryEngine, StudioError> {
        if self.mem.is_none() {
            let db = Arc::clone(self.cell.borrow_owner());
            let engine = MemoryEngine::open(db).map_err(IntoStudioError::into_studio)?;
            // The bootstrap runs on the engine's own connection, so ours still holds the
            // schema from before it. Without this the object tree keeps reporting a
            // schema that is one DDL behind for the rest of the session.
            self.cell.with_dependent(|_, conn| {
                conn.refresh_schema().map_err(IntoStudioError::into_studio)
            })?;
            self.mem = Some(engine);
        }
        Ok(self.mem.as_ref().expect("just opened"))
    }

    /// Restricted model-free access for Studio's memory inspection and erasure screens.
    pub fn memory_maintenance(&mut self) -> Result<&MemoryMaintenance, StudioError> {
        if self.maintenance.is_none() {
            let db = Arc::clone(self.cell.borrow_owner());
            self.maintenance =
                Some(MemoryMaintenance::open(db).map_err(IntoStudioError::into_studio)?);
        }
        Ok(self.maintenance.as_ref().expect("just opened"))
    }
}

impl std::fmt::Debug for Session {
    /// Hand-written because neither `DbCell` nor `MemoryEngine` is `Debug`, and because a
    /// derived one would be an invitation to log a vault's contents.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("path", &self.path)
            .field("is_demo", &self.is_demo)
            .field("memory_open", &self.mem.is_some())
            .field("memory_maintenance_open", &self.maintenance.is_some())
            .finish()
    }
}

/// A failure Studio itself detects before the engine is asked.
pub fn refuse(kind: Kind, detail: impl Into<String>) -> StudioError {
    StudioError::new(kind, detail)
}

/// Isolated demo vault whose temporary directory shares its lifetime.
#[cfg(test)]
pub(crate) struct DemoVault(Session);

#[cfg(test)]
impl DemoVault {
    pub(crate) fn new() -> Self {
        Self(Session::demo().expect("the demo vault creates"))
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.0.path().to_owned()
    }
}

#[cfg(test)]
impl std::ops::Deref for DemoVault {
    type Target = Session;

    fn deref(&self) -> &Session {
        &self.0
    }
}

#[cfg(test)]
impl std::ops::DerefMut for DemoVault {
    fn deref_mut(&mut self) -> &mut Session {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_demo_is_a_fresh_working_copy() {
        let first = Session::demo().unwrap();
        let first_path = first.path().to_owned();
        let changed = first.run(
            "DELETE FROM documents WHERE id = 1;\
             CREATE TABLE user_marker (id INTEGER PRIMARY KEY, note TEXT NOT NULL);\
             INSERT INTO user_marker VALUES (1, 'session only');",
        );
        assert!(changed.failed.is_none(), "{:?}", changed.failed);
        drop(first);

        let second = Session::demo().unwrap();
        assert_ne!(first_path, second.path());
        assert_eq!(
            second
                .query("SELECT id FROM documents WHERE id = 1")
                .unwrap()
                .rows
                .len(),
            1,
            "a new demo must restore the deterministic seed"
        );
        let marker = second.run("SELECT note FROM user_marker;");
        assert_eq!(
            marker.failed.as_ref().map(|error| error.kind),
            Some(Kind::Missing),
            "a new demo retained a table created in the previous working copy"
        );
    }

    #[test]
    fn forgetting_from_one_demo_does_not_change_the_next() {
        let mut first = Session::demo().unwrap();
        let page = crate::engine::memory::atoms(&mut first, "episodic", None).unwrap();
        let atom_id = page.atoms.first().expect("the seed has an atom").id;
        crate::engine::memory::forget(&mut first, "episodic", &[atom_id]).unwrap();
        let remaining = crate::engine::memory::atoms(&mut first, "episodic", None).unwrap();
        assert!(remaining.atoms.iter().all(|atom| atom.id != atom_id));
        drop(first);

        let mut second = Session::demo().unwrap();
        let restored = crate::engine::memory::atoms(&mut second, "episodic", None).unwrap();
        assert!(
            restored.atoms.iter().any(|atom| atom.id == atom_id),
            "the next demo inherited an atom erasure"
        );
    }

    #[test]
    fn dropping_a_demo_removes_its_data_and_sidecars() {
        let session = Session::demo().unwrap();
        let root = session.path().parent().unwrap().to_owned();
        let data = session.path().to_owned();
        let key = session.database().key_path().to_owned();
        let region_keys = session.database().region_store_path();
        let atom_keys = session.database().atom_store_path();
        let audit = session.database().audit_log_path();
        for path in [&data, &key, &region_keys, &atom_keys] {
            assert!(
                path.exists(),
                "demo sidecar was not created: {}",
                path.display()
            );
        }
        if let Some(audit) = &audit {
            assert!(audit.exists(), "demo audit sidecar was not created");
        }

        drop(session);

        assert!(
            !root.exists(),
            "the disposable demo directory survived its database handles"
        );
    }

    #[test]
    fn a_demo_passphrase_cannot_be_changed() {
        let session = Session::demo().unwrap();
        assert!(session.is_demo());
        let error = session
            .change_passphrase(DEMO_PASSPHRASE, "a-new-demo-passphrase")
            .expect_err("a disposable demo has no persistent passphrase to change");
        assert_eq!(error.kind, Kind::Usage);
        assert_eq!(
            session
                .query("SELECT COUNT(*) FROM documents")
                .unwrap()
                .rows
                .len(),
            1,
            "a refused passphrase change damaged the open demo"
        );
    }

    #[test]
    fn a_demo_vault_is_a_real_database_that_answers_sql() {
        let session = DemoVault::new();
        assert!(session.path().exists(), "the demo is a file, not a fiction");

        let tables = session.tables();
        assert!(
            tables.iter().any(|t| t == "documents"),
            "seeded tables should be listed by the engine: {tables:?}"
        );

        let rows = session.query("SELECT COUNT(*) FROM documents").unwrap();
        assert_eq!(rows.rows.len(), 1);
    }

    #[test]
    fn ddl_and_an_empty_result_are_different_outcomes() {
        let session = DemoVault::new();
        let run = session.run("CREATE TABLE probe (id INTEGER PRIMARY KEY);");
        assert!(run.failed.is_none(), "{:?}", run.failed);
        assert!(matches!(run.statements.as_slice(), [Statement::Ok]));

        let run = session.run("SELECT id FROM probe WHERE id = -1;");
        match run.statements.as_slice() {
            [Statement::Rows(q)] => assert!(q.rows.is_empty()),
            other => panic!("a SELECT should return rows, got {other:?}"),
        }
    }

    #[test]
    fn editor_results_keep_an_exact_count_but_only_the_display_prefix() {
        let session = DemoVault::new();
        let run = session.run(
            "WITH RECURSIVE numbers(n) AS (\
                 SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 2505\
             ) SELECT n FROM numbers;",
        );
        assert!(run.failed.is_none(), "{:?}", run.failed);
        assert_eq!(run.last_row_count(), Some(2_505));
        assert_eq!(
            run.last_rows().expect("the SELECT returns rows").rows.len(),
            RETAINED_QUERY_ROWS
        );
    }

    #[test]
    fn a_failed_script_reports_what_committed_before_it_stopped() {
        let session = DemoVault::new();
        let run = session.run(
            "CREATE TABLE first (id INTEGER PRIMARY KEY);\
             CREATE TABLE second (id INTEGER PRIMARY KEY);\
             SELECT * FROM no_such_table;",
        );
        assert_eq!(run.statements.len(), 2, "both DDL statements committed");
        let failed = run.failed.expect("the third statement fails");
        assert_eq!(failed.kind, Kind::Missing);
        assert!(
            session.tables().iter().any(|t| t == "second"),
            "the committed statements really did commit"
        );
    }

    #[test]
    fn an_atomic_import_rolls_back_earlier_statements_on_failure() {
        let session = DemoVault::new();
        let error = session
            .run_atomic(
                "CREATE TABLE rolled_back (id INTEGER PRIMARY KEY);\
                 INSERT INTO rolled_back VALUES (1);\
                 INSERT INTO rolled_back VALUES (1);",
            )
            .expect_err("the duplicate primary key must abort the import");
        assert!(!error.detail.is_empty());
        assert!(
            !session.tables().iter().any(|name| name == "rolled_back"),
            "a failed import must not leave its successful prefix committed"
        );
    }

    #[test]
    fn run_reports_storage_changes_without_parsing_sql() {
        let session = DemoVault::new();
        let read = session.run("SELECT 1;");
        assert!(!read.storage_changed);

        let write = session.run("CREATE TABLE changed (id INTEGER PRIMARY KEY);");
        assert!(write.failed.is_none(), "{:?}", write.failed);
        assert!(write.storage_changed);

        let returning = session.run("INSERT INTO changed VALUES (1) RETURNING id;");
        assert!(returning.failed.is_none(), "{:?}", returning.failed);
        assert!(returning.last_rows().is_some());
        assert!(returning.storage_changed);
    }

    /// The engine's own message survives to the screen rather than being paraphrased.
    #[test]
    fn a_syntax_error_carries_the_engines_wording() {
        let session = DemoVault::new();
        let run = session.run("SELCT 1;");
        let failed = run.failed.expect("that is not a statement");
        assert_eq!(failed.kind, Kind::Syntax);
        assert!(!failed.detail.is_empty());
    }

    #[test]
    fn explain_returns_a_plan_without_running_the_statement() {
        let session = DemoVault::new();
        let plan = session.explain("SELECT * FROM documents", false).unwrap();
        assert!(
            !plan.lines.is_empty(),
            "a plan should have at least one line"
        );
        assert!(!plan.measured, "EXPLAIN alone measures nothing");
    }

    /// The distinction the two buttons carry: one of them executes.
    #[test]
    fn analyze_runs_the_statement_and_says_it_did() {
        let session = DemoVault::new();
        let plan = session.explain("SELECT * FROM documents", true).unwrap();
        assert!(plan.measured);
        assert!(
            plan.lines.iter().any(|l| l.contains("actual time=")),
            "no measurement in an analyzed plan: {:?}",
            plan.lines
        );
    }

    fn scratch_vault(dir: &tempfile::TempDir) -> CreateSpec {
        CreateSpec {
            path: dir.path().join("scratch.cdl"),
            passphrase: "a-long-enough-passphrase".to_owned().into(),
            kdf: KdfAlgorithm::Argon2id,
            profile: Argon2Profile::Iot,
        }
    }

    #[test]
    fn ordinary_vault_changes_survive_close_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let spec = scratch_vault(&dir);
        {
            let session = Session::create(&spec).unwrap();
            assert!(!session.is_demo());
            let changed = session.run(
                "CREATE TABLE durable (id INTEGER PRIMARY KEY, note TEXT NOT NULL);\
                 INSERT INTO durable VALUES (1, 'kept');",
            );
            assert!(changed.failed.is_none(), "{:?}", changed.failed);
            session
                .change_passphrase(&spec.passphrase, "the-next-long-passphrase")
                .unwrap();
        }

        assert!(
            Session::open(&spec.path, &spec.passphrase).is_err(),
            "the old passphrase still opens the vault"
        );
        let reopened = Session::open(&spec.path, "the-next-long-passphrase").unwrap();
        assert!(!reopened.is_demo());
        let rows = reopened
            .query("SELECT note FROM durable WHERE id = 1")
            .unwrap();
        assert_eq!(rows.rows.len(), 1, "the committed row did not persist");
    }

    /// Opening the memory engine runs its bootstrap schema and reconciles crash residue,
    /// so it writes. A vault opened to read SQL must not be modified by having been
    /// opened, which means the engine cannot be opened eagerly.
    #[test]
    fn the_memory_engine_is_not_opened_until_it_is_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        let mut session = Session::create(&scratch_vault(&dir)).unwrap();
        assert!(
            !session.tables().iter().any(|t| t == "memory_regions"),
            "creating a vault must not bootstrap the memory schema"
        );

        session.demo_memory().expect("the engine opens on demand");
        assert!(
            session.tables().iter().any(|t| t == "memory_regions"),
            "after opening the engine our own connection must see its schema"
        );
    }

    /// The lock is exclusive for the life of the `Database`, and the reader is told which
    /// of the two situations they are in rather than getting one generic failure.
    #[test]
    fn a_wrong_passphrase_and_a_held_lock_are_different_answers() {
        let dir = tempfile::tempdir().unwrap();
        let spec = scratch_vault(&dir);
        let held = Session::create(&spec).unwrap();

        let locked = Session::open(&spec.path, &spec.passphrase)
            .expect_err("the first session still holds the lock");
        assert_eq!(locked.kind, Kind::Locked);
        drop(held);

        let wrong = Session::open(&spec.path, "not the passphrase")
            .expect_err("a wrong passphrase cannot open it");
        assert_eq!(
            wrong.kind,
            Kind::Passphrase,
            "a typo must never be reported as a damaged file"
        );
        Session::open(&spec.path, &spec.passphrase).expect("the right passphrase opens it");
    }
}
