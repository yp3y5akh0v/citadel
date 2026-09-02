//! Public SQL connection API.

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

fn generate_temp_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = (crate::datetime::now_micros() as u64) & 0xFFFF_FFFF;
    (nanos << 32) | (counter & 0xFFFF_FFFF)
}

fn temp_storage_name(temp_id: u64, user_name: &str) -> String {
    format!("__temp_{temp_id}_{}", user_name.to_ascii_lowercase())
}

use lru::LruCache;

use citadel::Database;
use citadel_txn::write_txn::{WriteTxn, WriteTxnSnapshot};

use crate::error::{Result, SqlError};
use crate::executor;
use crate::parser;
use crate::parser::{BeginAccessMode, Statement, TimezoneValue};
use crate::prepared::PreparedStatement;
use crate::schema::{SchemaManager, SchemaSnapshot};
use crate::types::{ExecutionResult, QueryResult, TableSchema, Value};
use crate::ReadBudget;

const DEFAULT_CACHE_CAPACITY: usize = 64;
const DEFERRED_TEMP_DROPS_CACHE_KEY: &str = "citadel-sql:internal:deferred-temp-drops:v1";
static PENDING_TEMP_DROP_QUEUES: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static READ_BUDGET_STACK: RefCell<Vec<ReadBudget>> = const { RefCell::new(Vec::new()) };
}

/// Run `f` with a shared read-materialization budget for SQL queries on this
/// thread.
///
/// While the scope is active, [`Connection::query`] and
/// [`Connection::query_params`] accept SELECT statements only and execute them
/// through a budgeted storage transaction. Scopes nest, restore the previous
/// budget on unwind, and share the supplied budget with its clones. Worker
/// threads must enter their own scope explicitly. `f` must perform its work
/// synchronously; returning a future ends the scope before that future runs.
pub fn with_read_budget<R>(budget: &ReadBudget, f: impl FnOnce() -> R) -> R {
    struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            READ_BUDGET_STACK.with(|stack| {
                stack
                    .borrow_mut()
                    .pop()
                    .expect("read-budget scope stack remains balanced");
            });
        }
    }

    READ_BUDGET_STACK.with(|stack| stack.borrow_mut().push(budget.clone()));
    let _guard = Guard;
    f()
}

fn scoped_read_budget() -> Option<ReadBudget> {
    READ_BUDGET_STACK.with(|stack| stack.borrow().last().cloned())
}

#[cfg(test)]
thread_local! {
    static LATE_CANCEL_HOOK: RefCell<Option<citadel::CancelToken>> = const { RefCell::new(None) };
}

#[cfg(test)]
struct LateCancelGuard;

#[cfg(test)]
impl Drop for LateCancelGuard {
    fn drop(&mut self) {
        LATE_CANCEL_HOOK.with(|hook| *hook.borrow_mut() = None);
    }
}

#[cfg(test)]
fn cancel_after_statement(token: citadel::CancelToken) -> LateCancelGuard {
    LATE_CANCEL_HOOK.with(|hook| *hook.borrow_mut() = Some(token));
    LateCancelGuard
}

#[cfg(test)]
fn trip_late_cancel_hook() {
    if let Some(token) = LATE_CANCEL_HOOK.with(|hook| hook.borrow_mut().take()) {
        token.cancel();
    }
}

#[derive(Default)]
struct DeferredTempDrops {
    names: parking_lot::Mutex<Vec<String>>,
    pending: AtomicBool,
}

impl Drop for DeferredTempDrops {
    fn drop(&mut self) {
        if self.pending.load(Ordering::Acquire) {
            PENDING_TEMP_DROP_QUEUES.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Return the per-database TEMP cleanup queue. The shared SQL cache is already
/// the database-scoped rendezvous point used by independent connections, so it
/// lets a connection hand cleanup to whichever connection next releases the
/// single-writer slot.
fn deferred_temp_drops(db: &Database, create_if_missing: bool) -> Option<Arc<DeferredTempDrops>> {
    let handle = db.sql_cache_handle();
    let mut cache = handle.lock();
    if let Some(entry) = cache.get(DEFERRED_TEMP_DROPS_CACHE_KEY) {
        if let Ok(pending) = Arc::clone(entry).downcast::<DeferredTempDrops>() {
            return Some(pending);
        }
    }
    if !create_if_missing {
        return None;
    }
    let pending = Arc::new(DeferredTempDrops::default());
    cache.insert(
        DEFERRED_TEMP_DROPS_CACHE_KEY.to_owned(),
        Arc::clone(&pending) as Arc<dyn std::any::Any + Send + Sync>,
    );
    Some(pending)
}

/// Remove a drained queue from the diagnostic cache surface when nobody else
/// has already cloned it to enqueue or drain work. A missed removal is benign;
/// the global pending counter still keeps empty queues off the statement path.
fn remove_drained_temp_queue(db: &Database, pending: &Arc<DeferredTempDrops>) {
    if pending.pending.load(Ordering::Acquire) {
        return;
    }
    let handle = db.sql_cache_handle();
    let Some(mut cache) = handle.try_lock() else {
        return;
    };
    let Some(current) = cache
        .get(DEFERRED_TEMP_DROPS_CACHE_KEY)
        .map(Arc::clone)
        .and_then(|entry| entry.downcast::<DeferredTempDrops>().ok())
    else {
        return;
    };
    // `cache`, `pending`, and `current` are the three references owned here.
    // Any fourth reference can be an enqueuer which must keep this queue
    // reachable until its own drain attempt.
    if Arc::ptr_eq(&current, pending) && Arc::strong_count(pending) == 3 {
        cache.remove(DEFERRED_TEMP_DROPS_CACHE_KEY);
    }
}

/// Make one non-blocking attempt to remove every queued TEMP backing table.
///
/// The queue lock is held through the cleanup transaction, and `begin_write`
/// refuses when a writer is active, so this never waits on what it cleans up.
fn try_drain_deferred_temp_drops(db: &Database) {
    // TEMP tables are uncommon. Keep the normal statement path to one atomic
    // load rather than taking the shared cache mutex twice per statement.
    if PENDING_TEMP_DROP_QUEUES.load(Ordering::Acquire) == 0 {
        return;
    }
    let Some(pending) = deferred_temp_drops(db, false) else {
        return;
    };
    let mut names = pending.names.lock();
    if names.is_empty() {
        remove_drained_temp_queue(db, &pending);
        return;
    }
    let Ok(mut wtx) = db.begin_write() else {
        return;
    };
    // Cleanup restores an internal invariant and must not inherit the token
    // that interrupted the user operation which scheduled it.
    wtx.set_cancel(None);
    for name in names.iter() {
        match wtx.drop_table(name.as_bytes()) {
            Ok(()) | Err(citadel_core::Error::TableNotFound(_)) => {}
            Err(_) => {
                wtx.abort();
                return;
            }
        }
    }
    if wtx.commit().is_ok() {
        names.clear();
        if pending.pending.swap(false, Ordering::AcqRel) {
            PENDING_TEMP_DROP_QUEUES.fetch_sub(1, Ordering::AcqRel);
        }
        remove_drained_temp_queue(db, &pending);
    }
}

fn defer_temp_drops(db: &Database, temp_names: Vec<String>) {
    let pending = if temp_names.is_empty() {
        None
    } else {
        let pending = deferred_temp_drops(db, true).expect("TEMP cleanup queue was just created");
        let mut names = pending.names.lock();
        let was_empty = names.is_empty();
        names.extend(temp_names);
        if was_empty && !pending.pending.swap(true, Ordering::AcqRel) {
            PENDING_TEMP_DROP_QUEUES.fetch_add(1, Ordering::Release);
        }
        drop(names);
        Some(pending)
    };
    // Dropping any connection can release the writer which blocked cleanup
    // queued by a different connection, even when this connection owns no TEMP
    // tables itself.
    try_drain_deferred_temp_drops(db);
    if let Some(pending) = pending {
        remove_drained_temp_queue(db, &pending);
    }
}

#[derive(Debug)]
pub struct ScriptExecution {
    pub completed: Vec<ExecutionResult>,
    pub error: Option<SqlError>,
}

fn rewrite_show_triggers(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    let trimmed = trimmed.trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("show triggers") {
        return None;
    }
    let after = lower["show triggers".len()..].trim_start();
    let base = "SELECT trigger_name, event_object_table AS table_name, action_timing, \
                event_manipulation, action_orientation, action_statement \
                FROM information_schema.triggers";
    if after.is_empty() {
        return Some(format!("{base} ORDER BY trigger_name"));
    }
    if let Some(rest) = after.strip_prefix("on ") {
        let table = rest.trim().trim_end_matches(';').trim();
        if table.is_empty() {
            return None;
        }
        let escaped = table.replace('\'', "''");
        return Some(format!(
            "{base} WHERE LOWER(event_object_table) = LOWER('{escaped}') ORDER BY trigger_name"
        ));
    }
    None
}

fn rewrite_show_matviews(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    let trimmed = trimmed.trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower != "show materialized views" {
        return None;
    }
    Some(
        "SELECT matviewname, ispopulated, hasindexes, definition \
         FROM pg_matviews ORDER BY matviewname"
            .to_string(),
    )
}

/// Whether a statement may be refused before it starts.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AtTheDoor {
    /// An ordinary statement: a tripped token stops it before it touches
    /// anything, which is the only check a statement that never enters a scan
    /// loop would otherwise get.
    Refuse,
    /// Transaction control. It still needs the current token installed, because
    /// COMMIT is refused by the transaction itself, but refusing it here would
    /// leave a cancelled connection with no way to close its transaction:
    /// ROLLBACK is the caller doing exactly what the cancel asked for.
    Admit,
}

fn is_active_txn_control(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Commit
            | Statement::Rollback
            | Statement::Savepoint(_)
            | Statement::ReleaseSavepoint(_)
            | Statement::RollbackTo(_)
    )
}

fn is_txn_control(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Begin { .. }) || is_active_txn_control(stmt)
}

fn try_normalize_insert(sql: &str) -> Option<(String, Vec<Value>)> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i + 6 > len || !bytes[i..i + 6].eq_ignore_ascii_case(b"INSERT") {
        return None;
    }
    i += 6;
    if i >= len || !bytes[i].is_ascii_whitespace() {
        return None;
    }
    while i < len && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    if i + 4 > len || !bytes[i..i + 4].eq_ignore_ascii_case(b"INTO") {
        return None;
    }
    i += 4;
    if i >= len || !bytes[i].is_ascii_whitespace() {
        return None;
    }

    let prefix_start = 0;
    let mut values_pos = None;
    let mut j = i;
    while j + 6 <= len {
        if bytes[j..j + 6].eq_ignore_ascii_case(b"VALUES")
            && (j == 0 || !bytes[j - 1].is_ascii_alphanumeric() && bytes[j - 1] != b'_')
            && (j + 6 >= len || !bytes[j + 6].is_ascii_alphanumeric() && bytes[j + 6] != b'_')
        {
            values_pos = Some(j);
            break;
        }
        j += 1;
    }
    let values_pos = values_pos?;

    let prefix = &sql[prefix_start..values_pos + 6];
    let mut pos = values_pos + 6;

    while pos < len && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if pos >= len || bytes[pos] != b'(' {
        return None;
    }
    pos += 1;

    let mut values = Vec::new();
    let mut normalized = String::with_capacity(sql.len());
    normalized.push_str(prefix);
    normalized.push_str(" (");

    loop {
        while pos < len && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len {
            return None;
        }

        let param_idx = values.len() + 1;
        if param_idx > 1 {
            normalized.push_str(", ");
        }

        if bytes[pos] == b'\'' {
            pos += 1;
            let mut seg_start = pos;
            let mut s = String::new();
            loop {
                if pos >= len {
                    return None;
                }
                if bytes[pos] == b'\'' {
                    s.push_str(std::str::from_utf8(&bytes[seg_start..pos]).ok()?);
                    pos += 1;
                    if pos < len && bytes[pos] == b'\'' {
                        s.push('\'');
                        pos += 1;
                        seg_start = pos;
                    } else {
                        break;
                    }
                } else {
                    pos += 1;
                }
            }
            values.push(Value::Text(s.into()));
        } else if bytes[pos] == b'-' || bytes[pos].is_ascii_digit() {
            let start = pos;
            if bytes[pos] == b'-' {
                pos += 1;
            }
            while pos < len && bytes[pos].is_ascii_digit() {
                pos += 1;
            }
            if pos < len && bytes[pos] == b'.' {
                pos += 1;
                while pos < len && bytes[pos].is_ascii_digit() {
                    pos += 1;
                }
                let num: f64 = std::str::from_utf8(&bytes[start..pos]).ok()?.parse().ok()?;
                values.push(Value::Real(num));
            } else {
                let num: i64 = std::str::from_utf8(&bytes[start..pos]).ok()?.parse().ok()?;
                values.push(Value::Integer(num));
            }
        } else if pos + 4 <= len && bytes[pos..pos + 4].eq_ignore_ascii_case(b"NULL") {
            let after = if pos + 4 < len { bytes[pos + 4] } else { b')' };
            if !after.is_ascii_alphanumeric() && after != b'_' {
                pos += 4;
                values.push(Value::Null);
            } else {
                return None;
            }
        } else if pos + 4 <= len && bytes[pos..pos + 4].eq_ignore_ascii_case(b"TRUE") {
            let after = if pos + 4 < len { bytes[pos + 4] } else { b')' };
            if !after.is_ascii_alphanumeric() && after != b'_' {
                pos += 4;
                values.push(Value::Boolean(true));
            } else {
                return None;
            }
        } else if pos + 5 <= len && bytes[pos..pos + 5].eq_ignore_ascii_case(b"FALSE") {
            let after = if pos + 5 < len { bytes[pos + 5] } else { b')' };
            if !after.is_ascii_alphanumeric() && after != b'_' {
                pos += 5;
                values.push(Value::Boolean(false));
            } else {
                return None;
            }
        } else {
            return None;
        }

        normalized.push('$');
        normalized.push_str(&param_idx.to_string());

        while pos < len && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len {
            return None;
        }

        if bytes[pos] == b',' {
            pos += 1;
        } else if bytes[pos] == b')' {
            pos += 1;
            break;
        } else {
            return None;
        }
    }

    normalized.push(')');

    while pos < len && (bytes[pos].is_ascii_whitespace() || bytes[pos] == b';') {
        pos += 1;
    }
    if pos != len {
        return None;
    }

    if values.is_empty() {
        return None;
    }

    Some((normalized, values))
}

pub(crate) struct CacheEntry {
    pub(crate) stmt: Arc<Statement>,
    pub(crate) schema_gen: u64,
    pub(crate) param_count: usize,
    pub(crate) compiled: Option<Arc<dyn executor::CompiledPlan>>,
}

struct SavepointEntry {
    name: String,
    snapshot: Option<SavepointSnapshot>,
    timezone: SessionTimezone,
    timezone_after_commit: SessionTimezone,
}

struct SavepointSnapshot {
    wtx_snap: WriteTxnSnapshot,
    schema_snap: SchemaSnapshot,
    temp_table_names_len: usize,
}

#[derive(Clone)]
struct SessionTimezone {
    name: String,
    zone: jiff::tz::TimeZone,
}

impl SessionTimezone {
    fn utc() -> Self {
        Self {
            name: "UTC".to_owned(),
            zone: jiff::tz::TimeZone::UTC,
        }
    }
}

#[derive(Clone)]
struct TransactionTimezone {
    before: SessionTimezone,
    after_commit: SessionTimezone,
}

/// Active transaction held by a Connection. `None` outside BEGIN/COMMIT;
/// `Write` for normal BEGIN (or BEGIN READ WRITE); `Read` for BEGIN READ ONLY.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ActiveTxn<'a> {
    None,
    Write(WriteTxn<'a>),
    Read(citadel_txn::read_txn::ReadTxn<'a>),
}

impl<'a> ActiveTxn<'a> {
    fn is_none(&self) -> bool {
        matches!(self, ActiveTxn::None)
    }
    fn is_active(&self) -> bool {
        !self.is_none()
    }
    fn is_read_only(&self) -> bool {
        matches!(self, ActiveTxn::Read(_))
    }
    fn as_write_mut(&mut self) -> Option<&mut WriteTxn<'a>> {
        match self {
            ActiveTxn::Write(w) => Some(w),
            _ => None,
        }
    }
    fn replace_read_budget(&mut self, budget: Option<ReadBudget>) -> Option<ReadBudget> {
        match self {
            ActiveTxn::Write(txn) => {
                let previous = txn.read_budget().cloned();
                txn.set_read_budget(budget);
                previous
            }
            ActiveTxn::Read(txn) => {
                let previous = txn.read_budget().cloned();
                txn.set_read_budget(budget);
                previous
            }
            ActiveTxn::None => None,
        }
    }
    fn take(&mut self) -> ActiveTxn<'a> {
        std::mem::replace(self, ActiveTxn::None)
    }
}

pub(crate) struct ConnectionInner<'a> {
    pub(crate) schema: SchemaManager,
    active_txn: ActiveTxn<'a>,
    savepoint_stack: Vec<SavepointEntry>,
    pub(crate) stmt_cache: LruCache<String, CacheEntry>,
    txn_start_ts: Option<i64>,
    session_timezone: SessionTimezone,
    transaction_timezone: Option<TransactionTimezone>,
    /// Namespaces TEMP tables as `__temp_<id>_<name>`. Cleaned up on Connection
    /// drop.
    temp_id: u64,
    temp_table_names: Vec<String>,
}

pub struct Connection<'a> {
    pub(crate) db: &'a Database,
    pub(crate) inner: RefCell<ConnectionInner<'a>>,
}

impl<'a> Connection<'a> {
    /// Open a SQL session and load its initial schema. Loading uses a read
    /// transaction, so an already-tripped token returns `Interrupted`; later
    /// statements re-read the token at each boundary.
    pub fn open(db: &'a Database) -> Result<Self> {
        // A previous connection may have closed while another connection held
        // the single-writer slot. Opening a connection is a deterministic
        // writer-free retry point, and cleanup ignores the user token.
        try_drain_deferred_temp_drops(db);
        let schema = SchemaManager::load(db)?;
        let stmt_cache = LruCache::new(NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap());
        let temp_id = generate_temp_id();
        Ok(Self {
            db,
            inner: RefCell::new(ConnectionInner {
                schema,
                active_txn: ActiveTxn::None,
                savepoint_stack: Vec::new(),
                stmt_cache,
                txn_start_ts: None,
                session_timezone: SessionTimezone::utc(),
                transaction_timezone: None,
                temp_id,
                temp_table_names: Vec::new(),
            }),
        })
    }

    /// Txn-start UTC micros inside BEGIN/COMMIT, else `None`.
    pub fn txn_start_ts(&self) -> Option<i64> {
        self.inner.borrow().txn_start_ts
    }

    /// Returns the session time-zone (IANA name or fixed offset). Default
    /// `"UTC"`.
    pub fn session_timezone(&self) -> String {
        self.inner.borrow().session_timezone.name.clone()
    }

    /// Set the session time-zone. Accepts IANA names, ISO-8601 offsets,
    /// `"UTC"`, `"Z"`.
    pub fn set_session_timezone(&self, tz: &str) -> Result<()> {
        let timezone = TimezoneValue::Named(tz.to_owned());
        self.inner
            .borrow_mut()
            .set_session_timezone_impl(&timezone, false)
    }

    /// A miss that another connection's DDL would explain, so it is worth reloading for.
    fn is_schema_miss(&self, e: &SqlError) -> bool {
        matches!(
            e,
            SqlError::TableNotFound(_) | SqlError::ColumnNotFound(_) | SqlError::ViewNotFound(_)
        ) && !self.in_transaction()
    }

    /// Run `f`, reloading the schema and retrying once if it missed on a name.
    ///
    /// The schema is read at open, so reloading only on a miss keeps the happy path
    /// free of any staleness check.
    fn with_schema_retry<T>(
        &self,
        mut f: impl FnMut(&mut ConnectionInner<'a>) -> Result<T>,
    ) -> Result<T> {
        // Bound, not matched on directly: the guard borrows, and the scrutinee's own
        // borrow would still be live.
        let first = f(&mut self.inner.borrow_mut());
        match first {
            Err(ref e) if self.is_schema_miss(e) => {}
            other => return other,
        }
        // `generation` counts local edits, so the reload is the only check.
        let mut fresh = SchemaManager::load(self.db)?;
        {
            let mut inner = self.inner.borrow_mut();
            fresh.bump_generation_past(inner.schema.generation());
            fresh.adopt_temp_aliases(&inner.schema);
            inner.schema = fresh;
        }
        // The retry's own error: it names what is missing from the schema now in force,
        // where the first names only what was missing from the stale one.
        f(&mut self.inner.borrow_mut())
    }

    pub fn execute(&self, sql: &str) -> Result<ExecutionResult> {
        self.with_schema_retry(|inner| inner.execute_impl(self.db, sql))
    }

    pub fn execute_params(&self, sql: &str, params: &[Value]) -> Result<ExecutionResult> {
        self.with_schema_retry(|inner| inner.execute_params_impl(self.db, sql, params))
    }

    /// Execute one internal recovery write without inheriting the database's
    /// cancellation token. Owns and finishes its own write transaction and
    /// refuses transaction-control or read-only SQL.
    #[doc(hidden)]
    pub fn execute_params_uncancelled_recovery(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        self.inner
            .borrow_mut()
            .execute_params_uncancelled_recovery(self.db, sql, params)
    }

    /// Execute internal recovery writes atomically without inheriting the database's
    /// cancellation token. Every entry must be INSERT, UPDATE, or DELETE.
    #[doc(hidden)]
    pub fn execute_params_batch_uncancelled_recovery(
        &self,
        statements: &[(&str, &[Value])],
    ) -> Result<Vec<ExecutionResult>> {
        self.inner
            .borrow_mut()
            .execute_params_batch_uncancelled_recovery(self.db, statements)
    }

    /// Execute `;`-separated SQL statements. Stops at the first failure.
    pub fn execute_script(&self, sql: &str) -> ScriptExecution {
        self.execute_script_impl(sql, None)
    }

    /// Execute a script while sharing one storage-materialization budget across its
    /// read-only SELECT statements.
    ///
    /// Mutating and transaction-control statements retain their normal semantics. This
    /// does not rewrite SQL or impose a row limit; it refuses a read before its admitted
    /// storage materialization exceeds `budget` and returns the already-completed prefix.
    pub fn execute_script_with_read_budget(
        &self,
        sql: &str,
        budget: &ReadBudget,
    ) -> ScriptExecution {
        self.execute_script_impl(sql, Some(budget))
    }

    fn execute_script_impl(&self, sql: &str, budget: Option<&ReadBudget>) -> ScriptExecution {
        let stmts = match parser::parse_sql_multi(sql) {
            Ok(s) => s,
            Err(e) => {
                return ScriptExecution {
                    completed: vec![],
                    error: Some(e),
                }
            }
        };
        let mut completed = Vec::with_capacity(stmts.len());
        for stmt in stmts {
            let result = self.with_schema_retry(|inner| {
                if let Some(budget) = budget.filter(|_| {
                    matches!(&stmt, Statement::Select(_)) && !executor::stmt_mutates(&stmt)
                }) {
                    inner.execute_read_statement_bounded_impl(self.db, &stmt, &[], budget.clone())
                } else {
                    inner.dispatch(self.db, &stmt, &[])
                }
            });
            match result {
                Ok(r) => completed.push(r),
                Err(e) => {
                    return ScriptExecution {
                        completed,
                        error: Some(e),
                    }
                }
            }
        }
        ScriptExecution {
            completed,
            error: None,
        }
    }

    pub fn execute_batch(&self, sql: &str) -> Result<Vec<ExecutionResult>> {
        self.with_schema_retry(|inner| inner.execute_batch_impl(self.db, sql))
    }

    pub fn query(&self, sql: &str) -> Result<QueryResult> {
        self.query_params(sql, &[])
    }

    pub fn query_params(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        if let Some(budget) = scoped_read_budget() {
            return self.query_params_bounded(sql, params, &budget);
        }
        match self.execute_params(sql, params)? {
            ExecutionResult::Query(qr) => Ok(qr),
            ExecutionResult::RowsAffected(n) => Ok(QueryResult {
                columns: vec!["rows_affected".into()],
                rows: vec![vec![Value::Integer(n as i64)]],
            }),
            ExecutionResult::Ok => Ok(QueryResult {
                columns: vec![],
                rows: vec![],
            }),
        }
    }

    /// Execute a SELECT with a storage-materialization budget.
    ///
    /// `budget` is shared, so callers may reuse it across several queries that
    /// make up one logical read. An existing explicit read or write transaction
    /// is used when present; otherwise the SELECT owns one read transaction.
    /// Storage rejects an oversized row before its overflow buffer is allocated.
    pub fn query_params_bounded(
        &self,
        sql: &str,
        params: &[Value],
        budget: &ReadBudget,
    ) -> Result<QueryResult> {
        self.with_schema_retry(|inner| {
            inner.query_params_bounded_impl(self.db, sql, params, budget.clone())
        })
    }

    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement<'_, 'a>> {
        if let Some(rewritten) = rewrite_show_triggers(sql) {
            return PreparedStatement::new(self, &rewritten);
        }
        if let Some(rewritten) = rewrite_show_matviews(sql) {
            return PreparedStatement::new(self, &rewritten);
        }
        PreparedStatement::new(self, sql)
    }

    pub fn tables(&self) -> Vec<String> {
        self.inner
            .borrow()
            .schema
            .table_names()
            .into_iter()
            .map(String::from)
            .collect()
    }

    /// Returns true if an explicit transaction is active (BEGIN was issued).
    pub fn in_transaction(&self) -> bool {
        self.inner.borrow().active_txn.is_active()
    }

    pub fn table_schema(&self, name: &str) -> Option<TableSchema> {
        self.inner.borrow().schema.get(name).cloned()
    }

    pub fn refresh_schema(&self) -> Result<()> {
        let mut new_schema = SchemaManager::load(self.db)?;
        let mut inner = self.inner.borrow_mut();
        new_schema.bump_generation_past(inner.schema.generation());
        new_schema.adopt_temp_aliases(&inner.schema);
        inner.schema = new_schema;
        Ok(())
    }

    /// Freeze the ANN index for `table.column` into a persisted segment: build
    /// off a read snapshot without the writer lock, then verify the non-ABA
    /// table stamp in a short write transaction before committing.
    /// Refused inside an explicit transaction (it owns its own txn), and for
    /// TEMP tables (their storage bypasses the DDL paths that purge segments).
    pub fn persist_ann_index(
        &self,
        table: &str,
        column: &str,
    ) -> Result<crate::executor::AnnSegmentInfo> {
        if let Some(token) = self.db.cancel_token() {
            token.check().map_err(SqlError::Storage)?;
        }
        if self.in_transaction() {
            return Err(SqlError::InvalidValue(
                "persist_ann_index: not allowed inside an explicit transaction".into(),
            ));
        }
        let inner = self.inner.borrow();
        executor::reject_legacy_volatile_schema(&inner.schema)?;
        let lower = table.to_ascii_lowercase();
        if inner.schema.resolve_temp(&lower) != lower {
            return Err(SqlError::InvalidValue(
                "persist_ann_index: TEMP tables are not persistable".into(),
            ));
        }
        let table_schema = inner
            .schema
            .get(&lower)
            .ok_or_else(|| SqlError::TableNotFound(table.to_string()))?;
        crate::executor::persist_ann_index(self.db, &inner.schema, table_schema, column)
    }

    /// The identity of the index currently cached for `table.column`:
    /// `(source, snapshot generation)` - `Loaded{segment_b3}` means queries are
    /// served by the persisted segment; `Built{refusal}` carries why a segment
    /// was rejected, if one was.
    pub fn ann_cache_status(
        &self,
        table: &str,
        column: &str,
    ) -> Result<Option<(crate::executor::AnnIndexSource, u64)>> {
        let inner = self.inner.borrow();
        let table_schema = inner
            .schema
            .get(&table.to_ascii_lowercase())
            .ok_or_else(|| SqlError::TableNotFound(table.to_string()))?;
        crate::executor::ann_cache_status(&inner.schema, table_schema, column)
    }
}

impl<'a> ConnectionInner<'a> {
    pub(crate) fn active_txn_is_some(&self) -> bool {
        self.active_txn.is_active()
    }

    fn jsonpath_session_context(&self, timestamp: i64) -> crate::json::JsonPathSessionContext {
        let date = jiff::Timestamp::from_microsecond(timestamp)
            .expect("SQL statement clock must be a valid timestamp")
            .to_zoned(self.session_timezone.zone.clone())
            .date();
        crate::json::JsonPathSessionContext {
            timezone: self.session_timezone.zone.clone(),
            date,
        }
    }

    fn set_session_timezone_impl(&mut self, value: &TimezoneValue, local: bool) -> Result<()> {
        if local && self.active_txn.is_none() {
            return Err(SqlError::NoActiveTransaction);
        }
        let timezone = match value {
            TimezoneValue::Default | TimezoneValue::Local => SessionTimezone::utc(),
            TimezoneValue::Named(tz) => SessionTimezone {
                name: tz.trim().to_owned(),
                zone: crate::datetime::resolve_timezone(tz)?,
            },
            TimezoneValue::OffsetSeconds(seconds) => SessionTimezone {
                name: crate::datetime::format_timezone_offset(*seconds),
                zone: crate::datetime::fixed_timezone(*seconds)?,
            },
        };
        let evaluation_noop = timezone.zone == self.session_timezone.zone;
        if self.schema.legacy_volatile_definition().is_some() {
            if evaluation_noop {
                return Ok(());
            }
            executor::reject_legacy_volatile_schema(&self.schema)?;
        }
        if let Some(definition) = self
            .schema
            .all_schemas()
            .find_map(TableSchema::session_dependent_persisted_expression)
        {
            if evaluation_noop {
                return Ok(());
            }
            return Err(SqlError::Unsupported(format!(
                "cannot change the session time zone while {definition} uses session-dependent JSON path evaluation; rewrite or drop/recreate that definition first"
            )));
        }
        self.session_timezone = timezone.clone();
        if !local {
            if let Some(transaction) = &mut self.transaction_timezone {
                transaction.after_commit = timezone;
            }
        }
        Ok(())
    }

    fn begin_timezone_transaction(&mut self) {
        debug_assert!(self.transaction_timezone.is_none());
        self.transaction_timezone = Some(TransactionTimezone {
            before: self.session_timezone.clone(),
            after_commit: self.session_timezone.clone(),
        });
    }

    fn finish_timezone_transaction(&mut self, committed: bool) {
        if let Some(transaction) = self.transaction_timezone.take() {
            self.session_timezone = if committed {
                transaction.after_commit
            } else {
                transaction.before
            };
        }
    }

    fn execute_impl(&mut self, db: &'a Database, sql: &str) -> Result<ExecutionResult> {
        if let Some(rewritten) = rewrite_show_triggers(sql) {
            return self.execute_params_impl(db, &rewritten, &[]);
        }
        if let Some(rewritten) = rewrite_show_matviews(sql) {
            return self.execute_params_impl(db, &rewritten, &[]);
        }
        if matches!(sql.as_bytes().first(), Some(b'I' | b'i')) {
            if let Some((normalized_key, extracted)) = try_normalize_insert(sql) {
                let gen = self.schema.generation();
                let stmt = if let Some(entry) = self.stmt_cache.get(&normalized_key) {
                    if entry.schema_gen == gen {
                        Arc::clone(&entry.stmt)
                    } else {
                        self.parse_and_cache(normalized_key, gen)?
                    }
                } else {
                    self.parse_and_cache(normalized_key, gen)?
                };
                return self.dispatch(db, &stmt, &extracted);
            }
        }
        self.execute_params_impl(db, sql, &[])
    }

    fn execute_batch_impl(&mut self, db: &'a Database, sql: &str) -> Result<Vec<ExecutionResult>> {
        if self.active_txn.is_active() {
            return Err(SqlError::TransactionAlreadyActive);
        }
        if let Some(token) = db.cancel_token() {
            token.check().map_err(SqlError::Storage)?;
        }
        let stmts = parser::parse_sql_multi(sql)?;
        if stmts.iter().any(is_txn_control) {
            return Err(SqlError::Unsupported(
                "transaction-control statements are not allowed in execute_batch".into(),
            ));
        }

        let wtx = db.begin_write().map_err(SqlError::Storage)?;
        let ts = crate::datetime::now_micros();
        self.active_txn = ActiveTxn::Write(wtx);
        self.begin_timezone_transaction();
        self.txn_start_ts = Some(ts);

        let execution = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut results = Vec::with_capacity(stmts.len());
            for stmt in &stmts {
                match self.dispatch(db, stmt, &[]) {
                    Ok(r) => results.push(r),
                    Err(e) => {
                        self.abort_active_txn(db);
                        return Err(e);
                    }
                }
            }

            let commit = match self.active_txn.take() {
                ActiveTxn::Write(mut wtx) => {
                    match crate::executor::helpers::drain_deferred_fk_checks(&mut wtx) {
                        Ok(()) => {
                            executor::commit_with_ann_publication(wtx, &self.schema).map(|_| ())
                        }
                        Err(e) => {
                            wtx.abort();
                            Err(e)
                        }
                    }
                }
                _ => Err(SqlError::NoActiveTransaction),
            };
            self.finish_timezone_transaction(commit.is_ok());
            self.reset_txn_state();
            try_drain_deferred_temp_drops(db);
            match commit {
                Ok(()) => Ok(results),
                Err(e) => {
                    // Past the current generation, not merely reloaded: the batch's
                    // schema edits were just rolled back, and plans compiled against
                    // them are still cached under their original generation.
                    let mut fresh = SchemaManager::load_ignoring_cancel(db)?;
                    fresh.bump_generation_past(self.schema.generation());
                    fresh.adopt_temp_aliases(&self.schema);
                    self.schema = fresh;
                    Err(e)
                }
            }
        }));
        match execution {
            Ok(result) => result,
            Err(payload) => {
                // The batch owns the writer across every statement. If an executor bug
                // unwinds, release that writer and discard its prefix before preserving the
                // original panic for the caller.
                self.abort_active_txn(db);
                std::panic::resume_unwind(payload)
            }
        }
    }

    fn reset_txn_state(&mut self) {
        self.clear_savepoint_state();
        self.txn_start_ts = None;
    }

    fn abort_active_txn(&mut self, db: &'a Database) {
        if let ActiveTxn::Write(wtx) = self.active_txn.take() {
            wtx.abort();
        }
        if let Ok(mut fresh) = SchemaManager::load_ignoring_cancel(db) {
            fresh.bump_generation_past(self.schema.generation());
            fresh.adopt_temp_aliases(&self.schema);
            self.schema = fresh;
        }
        self.finish_timezone_transaction(false);
        self.reset_txn_state();
        try_drain_deferred_temp_drops(db);
    }

    fn execute_params_impl(
        &mut self,
        db: &'a Database,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        let gen = self.schema.generation();
        if self.active_txn.is_none() {
            if let Some(entry) = self.stmt_cache.get(sql) {
                if entry.schema_gen == gen && entry.param_count == params.len() {
                    if let Some(plan) = entry.compiled.as_ref().map(Arc::clone) {
                        let stmt = Arc::clone(&entry.stmt);
                        return self.run_compiled(db, &plan, &stmt, params);
                    }
                }
            }
        }

        let (stmt, param_count) = self.get_or_parse(sql)?;

        if param_count != params.len() {
            return Err(SqlError::ParameterCountMismatch {
                expected: param_count,
                got: params.len(),
            });
        }

        if self.active_txn.is_none() {
            if let Some(plan) = executor::compile(&self.schema, &stmt) {
                if let Some(e) = self.stmt_cache.get_mut(sql) {
                    e.compiled = Some(Arc::clone(&plan));
                }
                let stmt_owned = Arc::clone(&stmt);
                return self.run_compiled(db, &plan, &stmt_owned, params);
            }
        }

        self.dispatch(db, &stmt, params)
    }

    fn query_params_bounded_impl(
        &mut self,
        db: &'a Database,
        sql: &str,
        params: &[Value],
        budget: ReadBudget,
    ) -> Result<QueryResult> {
        let (stmt, param_count) = self.get_or_parse(sql)?;
        if param_count != params.len() {
            return Err(SqlError::ParameterCountMismatch {
                expected: param_count,
                got: params.len(),
            });
        }
        if !matches!(&*stmt, Statement::Select(_)) || executor::stmt_mutates(&stmt) {
            return Err(SqlError::Unsupported(
                "bounded queries accept read-only SELECT statements only".into(),
            ));
        }

        let result = self.execute_read_statement_bounded_impl(db, &stmt, params, budget)?;
        Self::bounded_query_result(result)
    }

    fn execute_read_statement_bounded_impl(
        &mut self,
        db: &'a Database,
        stmt: &Statement,
        params: &[Value],
        budget: ReadBudget,
    ) -> Result<ExecutionResult> {
        if !matches!(stmt, Statement::Select(_)) || executor::stmt_mutates(stmt) {
            return Err(SqlError::Unsupported(
                "bounded queries accept read-only SELECT statements only".into(),
            ));
        }

        if self.active_txn.is_active() {
            let previous = self.active_txn.replace_read_budget(Some(budget));
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.dispatch(db, stmt, params)
            }));
            self.active_txn.replace_read_budget(previous);
            return match outcome {
                Ok(result) => result,
                Err(payload) => std::panic::resume_unwind(payload),
            };
        }

        let statement_timestamp = crate::datetime::now_micros();
        let timezone = self.session_timezone.zone.clone();
        let jsonpath_context = self.jsonpath_session_context(statement_timestamp);
        let mut rtx = db.begin_read();
        rtx.set_read_budget(Some(budget));
        let execute = || {
            if params.is_empty() {
                executor::execute_with_read(&mut rtx, &self.schema, stmt, params)
            } else {
                crate::eval::with_scoped_params(params, || {
                    executor::execute_with_read(&mut rtx, &self.schema, stmt, params)
                })
            }
        };
        crate::datetime::with_session_timezone(timezone, || {
            crate::datetime::with_statement_clock(Some(statement_timestamp), || {
                crate::datetime::with_txn_clock(Some(statement_timestamp), || {
                    crate::json::with_jsonpath_session_context(jsonpath_context, execute)
                })
            })
        })
    }

    fn bounded_query_result(result: ExecutionResult) -> Result<QueryResult> {
        match result {
            ExecutionResult::Query(query) => Ok(query),
            ExecutionResult::RowsAffected(_) | ExecutionResult::Ok => Err(SqlError::Unsupported(
                "bounded SELECT did not return a query result".into(),
            )),
        }
    }

    fn execute_params_uncancelled_recovery(
        &mut self,
        db: &'a Database,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        let statements = [(sql, params)];
        let mut results = self.execute_params_batch_uncancelled_recovery(db, &statements)?;
        Ok(results
            .pop()
            .expect("one recovery statement returns one result"))
    }

    fn execute_params_batch_uncancelled_recovery(
        &mut self,
        db: &'a Database,
        statements: &[(&str, &[Value])],
    ) -> Result<Vec<ExecutionResult>> {
        if self.active_txn.is_active() {
            return Err(SqlError::TransactionAlreadyActive);
        }
        try_drain_deferred_temp_drops(db);
        let mut parsed = Vec::with_capacity(statements.len());
        for &(sql, params) in statements {
            let stmt = parser::parse_sql(sql)?;
            let expected = parser::count_params(&stmt);
            if expected != params.len() {
                return Err(SqlError::ParameterCountMismatch {
                    expected,
                    got: params.len(),
                });
            }
            if !matches!(
                stmt,
                Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
            ) {
                return Err(SqlError::Unsupported(
                    "uncancelled recovery execution accepts only INSERT, UPDATE, or DELETE".into(),
                ));
            }
            parsed.push((stmt, params));
        }
        if parsed.is_empty() {
            return Ok(Vec::new());
        }
        executor::reject_legacy_volatile_schema(&self.schema)?;

        let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
        // `begin_write` inherits the handle token. Recovery is the exceptional
        // case: clear it before the first storage operation.
        wtx.set_cancel(None);
        let ts = crate::datetime::now_micros();
        self.active_txn = ActiveTxn::Write(wtx);
        self.begin_timezone_transaction();
        self.txn_start_ts = Some(ts);

        let execution = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut values = Vec::with_capacity(parsed.len());
            for (stmt, params) in &parsed {
                match self.dispatch_clocked(db, stmt, params) {
                    Ok(value) => values.push(value),
                    Err(error) => {
                        if let ActiveTxn::Write(wtx) = self.active_txn.take() {
                            wtx.abort();
                        }
                        self.finish_timezone_transaction(false);
                        self.reset_txn_state();
                        try_drain_deferred_temp_drops(db);
                        return Err(error);
                    }
                }
            }
            let result = match self.active_txn.take() {
                ActiveTxn::Write(mut wtx) => {
                    match crate::executor::helpers::drain_deferred_fk_checks(&mut wtx) {
                        Ok(()) => {
                            executor::commit_with_ann_publication(wtx, &self.schema).map(|_| values)
                        }
                        Err(error) => {
                            wtx.abort();
                            Err(error)
                        }
                    }
                }
                _ => Err(SqlError::NoActiveTransaction),
            };
            self.finish_timezone_transaction(result.is_ok());
            self.reset_txn_state();
            try_drain_deferred_temp_drops(db);
            result
        }));
        match execution {
            Ok(result) => result,
            Err(payload) => {
                // Recovery ignores cancellation, not genuine
                // executor bugs. Never leave its private transaction or session
                // state reachable if a caller catches the original unwind.
                self.abort_active_txn(db);
                std::panic::resume_unwind(payload)
            }
        }
    }

    fn run_compiled(
        &mut self,
        db: &'a Database,
        plan: &Arc<dyn executor::CompiledPlan>,
        stmt: &Statement,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        use executor::compile::ActiveTxnRef;
        self.guarded(db, AtTheDoor::Refuse, stmt, |conn| {
            let statement_timestamp = crate::datetime::now_micros();
            let transaction_timestamp = conn.txn_start_ts.unwrap_or(statement_timestamp);
            let timezone = conn.session_timezone.zone.clone();
            let jsonpath_context = conn.jsonpath_session_context(transaction_timestamp);
            let schema = &conn.schema;
            let exec = || {
                if params.is_empty() {
                    plan.execute(db, schema, stmt, params, ActiveTxnRef::None)
                } else {
                    crate::eval::with_scoped_params(params, || {
                        plan.execute(db, schema, stmt, params, ActiveTxnRef::None)
                    })
                }
            };
            crate::datetime::with_session_timezone(timezone, || {
                crate::datetime::with_statement_clock(Some(statement_timestamp), || {
                    if plan.needs_txn_clock() {
                        crate::datetime::with_txn_clock(Some(transaction_timestamp), || {
                            crate::json::with_jsonpath_session_context(jsonpath_context, exec)
                        })
                    } else {
                        crate::json::with_jsonpath_session_context(jsonpath_context, exec)
                    }
                })
            })
        })
    }

    pub(crate) fn parse_and_cache(
        &mut self,
        normalized_key: String,
        gen: u64,
    ) -> Result<Arc<Statement>> {
        let stmt = Arc::new(parser::parse_sql(&normalized_key)?);
        let param_count = parser::count_params(&stmt);
        self.stmt_cache.put(
            normalized_key,
            CacheEntry {
                stmt: Arc::clone(&stmt),
                schema_gen: gen,
                param_count,
                compiled: None,
            },
        );
        Ok(stmt)
    }

    pub(crate) fn get_or_parse(&mut self, sql: &str) -> Result<(Arc<Statement>, usize)> {
        let gen = self.schema.generation();

        if let Some(entry) = self.stmt_cache.get(sql) {
            if entry.schema_gen == gen {
                return Ok((Arc::clone(&entry.stmt), entry.param_count));
            }
        }

        let stmt = Arc::new(parser::parse_sql(sql)?);
        let param_count = parser::count_params(&stmt);

        let cacheable = !matches!(
            *stmt,
            Statement::CreateTable(_)
                | Statement::DropTable(_)
                | Statement::CreateIndex(_)
                | Statement::DropIndex(_)
                | Statement::CreateView(_)
                | Statement::DropView(_)
                | Statement::AlterTable(_)
        );

        if cacheable {
            self.stmt_cache.put(
                sql.to_string(),
                CacheEntry {
                    stmt: Arc::clone(&stmt),
                    schema_gen: gen,
                    param_count,
                    compiled: None,
                },
            );
        }

        Ok((stmt, param_count))
    }

    pub(crate) fn execute_prepared(
        &mut self,
        db: &'a Database,
        stmt: &Statement,
        compiled: Option<&Arc<dyn executor::CompiledPlan>>,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        if let Some(plan) = compiled {
            if self.active_txn.is_none() {
                return self.run_compiled(db, plan, stmt, params);
            }
            return self.run_compiled_in_txn(db, plan, stmt, params);
        }
        self.dispatch(db, stmt, params)
    }

    fn run_compiled_in_txn(
        &mut self,
        db: &'a Database,
        plan: &Arc<dyn executor::CompiledPlan>,
        stmt: &Statement,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        use executor::compile::ActiveTxnRef;
        self.guarded(db, AtTheDoor::Refuse, stmt, |conn| {
            // The guard installs and checks the current token before this
            // closure runs. A refused prepared mutation must not advance
            // the write transaction just because a savepoint is pending.
            if !conn.savepoint_stack.is_empty() && executor::stmt_mutates(stmt) {
                conn.capture_pending_snapshots();
            }
            let statement_timestamp = crate::datetime::now_micros();
            let transaction_timestamp = conn.txn_start_ts.unwrap_or(statement_timestamp);
            let timezone = conn.session_timezone.zone.clone();
            let jsonpath_context = conn.jsonpath_session_context(transaction_timestamp);
            let schema = &conn.schema;
            let txn = match &mut conn.active_txn {
                ActiveTxn::Write(wtx) => ActiveTxnRef::Write(wtx),
                ActiveTxn::Read(rtx) => ActiveTxnRef::Read(rtx),
                ActiveTxn::None => ActiveTxnRef::None,
            };
            let execute = || {
                if params.is_empty() || !plan.uses_scoped_params() {
                    plan.execute(db, schema, stmt, params, txn)
                } else {
                    crate::eval::with_scoped_params(params, || {
                        plan.execute(db, schema, stmt, params, txn)
                    })
                }
            };
            crate::datetime::with_session_timezone(timezone, || {
                crate::datetime::with_statement_clock(Some(statement_timestamp), || {
                    if plan.needs_txn_clock() {
                        crate::datetime::with_txn_clock(Some(transaction_timestamp), || {
                            crate::json::with_jsonpath_session_context(jsonpath_context, execute)
                        })
                    } else {
                        crate::json::with_jsonpath_session_context(jsonpath_context, execute)
                    }
                })
            })
        })
    }

    /// Install and check cancellation around one statement, and refuse an
    /// explicit transaction when any mutating statement leaves a prefix. All
    /// three lanes call this; guarding one leaves the others unguarded.
    fn guarded<F>(
        &mut self,
        db: &'a Database,
        door: AtTheDoor,
        stmt: &Statement,
        run: F,
    ) -> Result<ExecutionResult>
    where
        F: FnOnce(&mut Self) -> Result<ExecutionResult>,
    {
        executor::guard_legacy_volatile_schema(&self.schema, stmt)?;
        // Retry before the statement so a tripped user token cannot strand
        // internal cleanup. If this connection owns the writer, the bounded
        // attempt simply defers until the post-statement retry below.
        try_drain_deferred_temp_drops(db);
        // Re-read per statement rather than captured at BEGIN. An explicit
        // transaction outlives many statements, so a token installed after it
        // opened would never reach it, and the token it opened with would go on
        // cancelling statements the caller has since moved past.
        let token = db.cancel_token();
        let mutates = executor::stmt_mutates(stmt);
        let explicit = door == AtTheDoor::Refuse && self.active_txn.is_active();
        if door == AtTheDoor::Refuse {
            if let Some(wtx) = self.active_txn.as_write_mut() {
                wtx.check_usable().map_err(SqlError::Storage)?;
            }
        }
        match &mut self.active_txn {
            ActiveTxn::Write(wtx) => wtx.set_cancel(token.clone()),
            ActiveTxn::Read(rtx) => rtx.set_cancel(token.clone()),
            ActiveTxn::None => {}
        }
        // At the door, because not every statement reaches a scan loop.
        if door == AtTheDoor::Refuse {
            if let Some(t) = &token {
                t.check().map_err(SqlError::Storage)?;
            }
        }
        let mutation_marker = if explicit && mutates {
            self.active_txn
                .as_write_mut()
                .map(|wtx| wtx.mutation_marker())
        } else {
            None
        };
        let timezone_before = if explicit && matches!(stmt, Statement::SetTimezone { .. }) {
            Some((
                self.session_timezone.clone(),
                self.transaction_timezone.clone(),
            ))
        } else {
            None
        };
        let mut outcome = if mutation_marker.is_some() {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(self))) {
                Ok(outcome) => outcome,
                Err(payload) => {
                    // A callback can panic between an in-place write and the
                    // transaction's mutation counter advancing. Any panic from
                    // an explicit mutator therefore poisons conservatively.
                    if let Some(wtx) = self.active_txn.as_write_mut() {
                        wtx.mark_failed();
                    }
                    std::panic::resume_unwind(payload)
                }
            }
        } else {
            run(self)
        };
        #[cfg(test)]
        trip_late_cancel_hook();
        // An explicit transaction has not committed, so a late cancel can still
        // be reported and the transaction refused. Autocommit is excluded:
        // checking after its commit would flag a write that is already durable.
        if explicit && outcome.is_ok() {
            if let Some(t) = &token {
                if let Err(err) = t.check() {
                    if let Some((timezone, transaction_timezone)) = timezone_before {
                        self.session_timezone = timezone;
                        self.transaction_timezone = transaction_timezone;
                    }
                    outcome = Err(SqlError::Storage(err));
                }
            }
        }
        // A failed statement with a changed marker left a prefix in its
        // explicit transaction. Errors before the first write remain
        // recoverable; autocommit has already committed or aborted internally.
        if let Some(marker) = mutation_marker {
            if let (Err(error), Some(wtx)) = (&outcome, self.active_txn.as_write_mut()) {
                if wtx.mutated_since(marker) {
                    executor::mark_write_statement_failed(wtx, error);
                }
            }
        }
        // COMMIT/ROLLBACK and autocommit statements may have released the
        // writer which made an earlier connection's destructor defer cleanup.
        try_drain_deferred_temp_drops(db);
        outcome
    }

    pub(crate) fn dispatch(
        &mut self,
        db: &'a Database,
        stmt: &Statement,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        let door = if is_active_txn_control(stmt) {
            AtTheDoor::Admit
        } else {
            AtTheDoor::Refuse
        };
        self.guarded(db, door, stmt, |conn| {
            conn.dispatch_clocked(db, stmt, params)
        })
    }

    fn dispatch_clocked(
        &mut self,
        db: &'a Database,
        stmt: &Statement,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        let statement_timestamp = crate::datetime::now_micros();
        let transaction_timestamp = self.txn_start_ts.unwrap_or(statement_timestamp);
        let timezone = self.session_timezone.zone.clone();
        let jsonpath_context = self.jsonpath_session_context(transaction_timestamp);
        crate::datetime::with_session_timezone(timezone, || {
            crate::datetime::with_statement_clock(Some(statement_timestamp), || {
                crate::datetime::with_txn_clock(Some(transaction_timestamp), || {
                    crate::json::with_jsonpath_session_context(jsonpath_context, || {
                        if params.is_empty() {
                            self.dispatch_inner(db, stmt, params)
                        } else {
                            crate::eval::with_scoped_params(params, || {
                                self.dispatch_inner(db, stmt, params)
                            })
                        }
                    })
                })
            })
        })
    }

    fn dispatch_inner(
        &mut self,
        db: &'a Database,
        stmt: &Statement,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        match stmt {
            Statement::Begin { access_mode } => {
                if self.active_txn.is_active() {
                    return Err(SqlError::TransactionAlreadyActive);
                }
                let ts = crate::datetime::now_micros();
                match access_mode {
                    BeginAccessMode::ReadOnly => {
                        let rtx = db.begin_read();
                        self.active_txn = ActiveTxn::Read(rtx);
                    }
                    BeginAccessMode::ReadWrite | BeginAccessMode::Default => {
                        let wtx = db.begin_write().map_err(SqlError::Storage)?;
                        self.active_txn = ActiveTxn::Write(wtx);
                    }
                }
                self.begin_timezone_transaction();
                self.txn_start_ts = Some(ts);
                Ok(ExecutionResult::Ok)
            }
            Statement::Commit => {
                let outcome = match self.active_txn.take() {
                    ActiveTxn::None => return Err(SqlError::NoActiveTransaction),
                    ActiveTxn::Write(mut wtx) => {
                        match crate::executor::helpers::drain_deferred_fk_checks(&mut wtx) {
                            Ok(()) => {
                                executor::commit_with_ann_publication(wtx, &self.schema).map(|_| ())
                            }
                            Err(e) => {
                                wtx.abort();
                                Err(e)
                            }
                        }
                    }
                    ActiveTxn::Read(_rtx) => Ok(()),
                };
                // A refused COMMIT ends the transaction as surely as a
                // successful one, so returning early would strand the frozen
                // clock, savepoint stack and rolled-back schema edits.
                self.finish_timezone_transaction(outcome.is_ok());
                self.reset_txn_state();
                match outcome {
                    Ok(()) => Ok(ExecutionResult::Ok),
                    Err(e) => {
                        // Past the current generation: transactional schema
                        // edits were just rolled back, and plans compiled
                        // against them are still cached under the generation
                        // they were written at.
                        let mut fresh = SchemaManager::load_ignoring_cancel(db)?;
                        fresh.bump_generation_past(self.schema.generation());
                        fresh.adopt_temp_aliases(&self.schema);
                        self.schema = fresh;
                        Err(e)
                    }
                }
            }
            Statement::Rollback => {
                let reload = match self.active_txn.take() {
                    ActiveTxn::None => return Err(SqlError::NoActiveTransaction),
                    ActiveTxn::Write(wtx) => {
                        wtx.abort();
                        Some(SchemaManager::load_ignoring_cancel(db))
                    }
                    ActiveTxn::Read(_rtx) => None,
                };
                self.finish_timezone_transaction(false);
                self.reset_txn_state();
                if let Some(reload) = reload {
                    let mut fresh = reload?;
                    fresh.bump_generation_past(self.schema.generation());
                    fresh.adopt_temp_aliases(&self.schema);
                    self.schema = fresh;
                }
                Ok(ExecutionResult::Ok)
            }
            Statement::Savepoint(name) => self.do_savepoint(name),
            Statement::ReleaseSavepoint(name) => self.do_release(name),
            Statement::RollbackTo(name) => self.do_rollback_to(name),
            Statement::SetTimezone { zone, local } => {
                self.set_session_timezone_impl(zone, *local)?;
                Ok(ExecutionResult::Ok)
            }
            Statement::CreateTable(ct) if ct.temporary => {
                if self.active_txn.is_read_only() {
                    return Err(SqlError::Unsupported(
                        "cannot execute mutating statement inside a read-only transaction".into(),
                    ));
                }
                let user_name = ct.name.clone();
                let prefixed = temp_storage_name(self.temp_id, &user_name);
                if self.schema.contains(&user_name) {
                    if ct.if_not_exists {
                        return Ok(ExecutionResult::Ok);
                    }
                    return Err(SqlError::TableAlreadyExists(user_name));
                }
                if self.active_txn.as_write_mut().is_some() {
                    self.capture_pending_snapshots();
                }
                let mut clone = ct.clone();
                clone.name = prefixed.clone();
                clone.temporary = false;
                let stmt_concrete = Statement::CreateTable(clone);
                let outcome = if let Some(wtx) = self.active_txn.as_write_mut() {
                    executor::execute_in_txn(wtx, &mut self.schema, &stmt_concrete, params)?
                } else {
                    executor::execute(db, &mut self.schema, &stmt_concrete, params)?
                };
                self.schema
                    .register_temp_alias(&user_name, prefixed.clone());
                self.temp_table_names.push(prefixed);
                Ok(outcome)
            }
            Statement::Insert(ins) if self.active_txn.as_write_mut().is_some() => {
                self.capture_pending_snapshots();
                let wtx = self.active_txn.as_write_mut().unwrap();
                executor::exec_insert_in_txn(wtx, &self.schema, ins, params)
            }
            _ => {
                if self.active_txn.is_read_only() && executor::stmt_mutates(stmt) {
                    return Err(SqlError::Unsupported(
                        "cannot execute mutating statement inside a read-only transaction".into(),
                    ));
                }
                if self.active_txn.as_write_mut().is_some() && executor::stmt_mutates(stmt) {
                    self.capture_pending_snapshots();
                }
                let outcome = match &mut self.active_txn {
                    ActiveTxn::Write(wtx) => {
                        executor::execute_in_txn(wtx, &mut self.schema, stmt, params)?
                    }
                    ActiveTxn::Read(rtx) => {
                        executor::execute_with_read(rtx, &self.schema, stmt, params)?
                    }
                    ActiveTxn::None => executor::execute(db, &mut self.schema, stmt, params)?,
                };
                if let Statement::DropTable(dt) = stmt {
                    self.schema.unregister_temp_alias(&dt.name);
                }
                Ok(outcome)
            }
        }
    }

    fn clear_savepoint_state(&mut self) {
        self.savepoint_stack.clear();
    }

    fn do_savepoint(&mut self, name: &str) -> Result<ExecutionResult> {
        if self.active_txn.as_write_mut().is_none() {
            return Err(SqlError::NoActiveTransaction);
        }

        let timezone_after_commit = self
            .transaction_timezone
            .as_ref()
            .map(|state| state.after_commit.clone())
            .unwrap_or_else(|| self.session_timezone.clone());

        self.savepoint_stack.push(SavepointEntry {
            name: name.to_string(),
            snapshot: None,
            timezone: self.session_timezone.clone(),
            timezone_after_commit,
        });

        Ok(ExecutionResult::Ok)
    }

    fn capture_pending_snapshots(&mut self) {
        let last_pending = match self
            .savepoint_stack
            .iter()
            .rposition(|e| e.snapshot.is_none())
        {
            Some(i) => i,
            None => return,
        };
        let wtx = match self.active_txn.as_write_mut() {
            Some(w) => w,
            None => return,
        };
        let wtx_snap = wtx.begin_savepoint();
        let schema_snap = self.schema.save_snapshot();
        let temp_table_names_len = self.temp_table_names.len();

        for i in 0..last_pending {
            if self.savepoint_stack[i].snapshot.is_none() {
                self.savepoint_stack[i].snapshot = Some(SavepointSnapshot {
                    wtx_snap: wtx_snap.clone(),
                    schema_snap: schema_snap.clone(),
                    temp_table_names_len,
                });
            }
        }
        self.savepoint_stack[last_pending].snapshot = Some(SavepointSnapshot {
            wtx_snap,
            schema_snap,
            temp_table_names_len,
        });
    }

    fn do_release(&mut self, name: &str) -> Result<ExecutionResult> {
        if !self.active_txn.is_active() {
            return Err(SqlError::NoActiveTransaction);
        }

        let idx = self
            .savepoint_stack
            .iter()
            .rposition(|e| e.name == name)
            .ok_or_else(|| SqlError::SavepointNotFound(name.to_string()))?;
        self.savepoint_stack.truncate(idx);

        Ok(ExecutionResult::Ok)
    }

    fn do_rollback_to(&mut self, name: &str) -> Result<ExecutionResult> {
        if !self.active_txn.is_active() {
            return Err(SqlError::NoActiveTransaction);
        }

        let idx = self
            .savepoint_stack
            .iter()
            .rposition(|e| e.name == name)
            .ok_or_else(|| SqlError::SavepointNotFound(name.to_string()))?;

        self.savepoint_stack.truncate(idx + 1);
        let entry = self.savepoint_stack.last_mut().unwrap();
        let snapshot = entry.snapshot.take();
        self.session_timezone = entry.timezone.clone();
        if let Some(transaction) = &mut self.transaction_timezone {
            transaction.after_commit = entry.timezone_after_commit.clone();
        }
        let Some(snapshot) = snapshot else {
            return Ok(ExecutionResult::Ok);
        };

        let wtx = match self.active_txn.as_write_mut() {
            Some(w) => w,
            None => return Err(SqlError::NoActiveTransaction),
        };
        wtx.restore_snapshot(snapshot.wtx_snap);
        self.schema.restore_snapshot(snapshot.schema_snap);
        self.temp_table_names
            .truncate(snapshot.temp_table_names_len);

        Ok(ExecutionResult::Ok)
    }
}

impl<'a> Drop for Connection<'a> {
    fn drop(&mut self) {
        let (temp_names, active_txn) = {
            let mut inner = self.inner.borrow_mut();
            (
                std::mem::take(&mut inner.temp_table_names),
                inner.active_txn.take(),
            )
        };
        // An explicit transaction owns the single-writer slot; drop it first or
        // begin_write fails and leaves TEMP tables behind. Dropping also aborts
        // uncommitted work, which is the expected close behavior.
        drop(active_txn);
        defer_temp_drops(self.db, temp_names);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel::{Argon2Profile, DatabaseBuilder};

    fn fresh_db(dir: &std::path::Path) -> citadel::Database {
        DatabaseBuilder::new(dir.join("t.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap()
    }

    fn install_sorting_insert_trigger(conn: &Connection<'_>) {
        conn.execute("CREATE TABLE sort_input (id INTEGER PRIMARY KEY, label TEXT)")
            .unwrap();
        conn.execute("INSERT INTO sort_input VALUES (1, 'charlie'), (2, 'alpha'), (3, 'bravo')")
            .unwrap();
        conn.execute("CREATE TABLE sort_sink (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER sort_after_insert AFTER INSERT ON sort_sink FOR EACH ROW \
             BEGIN SELECT label FROM sort_input ORDER BY label; END",
        )
        .unwrap();
    }

    #[test]
    fn bounded_select_rejects_overflow_before_length_or_projection_can_materialize_it() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        let body = "x".repeat(citadel_core::MAX_INLINE_VALUE_SIZE + 1);
        conn.execute_params(
            "INSERT INTO docs VALUES (1, $1)",
            &[Value::Text(body.clone().into())],
        )
        .unwrap();
        let budget = ReadBudget::new(body.len(), body.len() * 4);

        for sql in [
            "SELECT LENGTH(body) FROM docs WHERE id = 1",
            "SELECT id FROM docs WHERE id = 1",
        ] {
            let err = conn.query_params_bounded(sql, &[], &budget).unwrap_err();
            assert!(matches!(
                err,
                SqlError::Storage(citadel_core::Error::ReadBudgetExceeded { size, .. })
                    if size > body.len()
            ));
        }
        assert_eq!(
            budget.remaining(),
            body.len() * 4,
            "a refused row consumed the shared total"
        );
    }

    #[test]
    fn bounded_select_is_read_only_and_shares_its_total_across_calls() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        conn.execute("INSERT INTO docs VALUES (1, 'abc')").unwrap();
        let probe = ReadBudget::new(128, 128);
        let before = probe.remaining();
        conn.query_params_bounded(
            "SELECT body FROM docs WHERE id = $1",
            &[Value::Integer(1)],
            &probe,
        )
        .unwrap();
        let charged = before - probe.remaining();
        assert!(charged > 0);

        let budget = ReadBudget::new(charged, charged * 2 - 1);
        conn.query_params_bounded("SELECT body FROM docs WHERE id = 1", &[], &budget)
            .unwrap();
        let err = conn
            .query_params_bounded("SELECT body FROM docs WHERE id = 1", &[], &budget)
            .unwrap_err();
        assert!(matches!(
            err,
            SqlError::Storage(citadel_core::Error::ReadBudgetExceeded { .. })
        ));

        let err = conn
            .query_params_bounded(
                "DELETE FROM docs WHERE id = 1",
                &[],
                &ReadBudget::new(128, 128),
            )
            .unwrap_err();
        assert!(matches!(err, SqlError::Unsupported(_)));
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM docs").unwrap().rows[0][0],
            Value::Integer(1)
        );
    }

    #[test]
    fn bounded_script_preserves_mutations_and_stops_at_the_read_budget() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        conn.execute("INSERT INTO docs VALUES (1, 'abc')").unwrap();
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY)")
            .unwrap();

        let probe = ReadBudget::new(128, 128);
        let before = probe.remaining();
        conn.query_params_bounded("SELECT body FROM docs", &[], &probe)
            .unwrap();
        let charged = before - probe.remaining();
        assert!(charged > 0);

        let budget = ReadBudget::new(charged, charged * 2 - 1);
        let run = conn.execute_script_with_read_budget(
            "INSERT INTO events VALUES (1); \
             SELECT body FROM docs; \
             SELECT body FROM docs;",
            &budget,
        );

        assert_eq!(run.completed.len(), 2, "the committed prefix is retained");
        assert!(matches!(
            run.error,
            Some(SqlError::Storage(
                citadel_core::Error::ReadBudgetExceeded { .. }
            ))
        ));
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM events").unwrap().rows[0][0],
            Value::Integer(1),
            "a bounded read must not roll back an earlier independent write"
        );
    }

    #[test]
    fn scoped_read_budgets_nest_and_restore_the_outer_budget() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        conn.execute("INSERT INTO docs VALUES (1, 'abc')").unwrap();

        let probe = ReadBudget::new(128, 128);
        let before = probe.remaining();
        conn.query_params_bounded("SELECT body FROM docs", &[], &probe)
            .unwrap();
        let charged = before - probe.remaining();
        assert!(charged > 0);
        let outer = ReadBudget::new(charged, charged * 2);
        let inner = ReadBudget::new(charged, 0);

        with_read_budget(&outer, || {
            conn.query("SELECT body FROM docs").unwrap();
            assert_eq!(outer.remaining(), charged);

            with_read_budget(&inner, || {
                let err = conn.query("SELECT body FROM docs").unwrap_err();
                assert!(matches!(
                    err,
                    SqlError::Storage(citadel_core::Error::ReadBudgetExceeded { .. })
                ));
            });
            assert_eq!(outer.remaining(), charged);

            conn.query("SELECT body FROM docs").unwrap();
        });
        assert_eq!(outer.remaining(), 0);

        conn.query("SELECT body FROM docs").unwrap();
    }

    #[test]
    fn scoped_read_budget_rejects_mutation_and_restores_after_unwind() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        conn.execute("INSERT INTO docs VALUES (1, 'abc')").unwrap();
        let budget = ReadBudget::new(128, 128);

        with_read_budget(&budget, || {
            let err = conn
                .query_params("DELETE FROM docs WHERE id = 1", &[])
                .unwrap_err();
            assert!(matches!(err, SqlError::Unsupported(_)));
        });
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM docs").unwrap().rows[0][0],
            Value::Integer(1)
        );

        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_read_budget(&budget, || panic!("scope probe"));
        }));
        assert!(panicked.is_err());

        let result = conn
            .query_params("DELETE FROM docs WHERE id = 1", &[])
            .unwrap();
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
    }

    #[test]
    fn scoped_read_budget_uses_and_restores_active_transactions() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        conn.execute("INSERT INTO docs VALUES (1, 'abc')").unwrap();

        let probe = ReadBudget::new(128, 128);
        let before = probe.remaining();
        conn.query_params_bounded("SELECT body FROM docs", &[], &probe)
            .unwrap();
        let charged = before - probe.remaining();
        assert!(charged > 0);

        conn.execute("BEGIN READ ONLY").unwrap();
        let too_small = ReadBudget::new(charged - 1, charged * 2);
        with_read_budget(&too_small, || {
            let err = conn.query("SELECT body FROM docs").unwrap_err();
            assert!(matches!(
                err,
                SqlError::Storage(citadel_core::Error::ReadBudgetExceeded { .. })
            ));
        });
        conn.query("SELECT body FROM docs").unwrap();
        conn.execute("COMMIT").unwrap();

        conn.execute("BEGIN").unwrap();
        let too_small = ReadBudget::new(charged - 1, charged * 2);
        with_read_budget(&too_small, || {
            let err = conn.query("SELECT body FROM docs").unwrap_err();
            assert!(matches!(
                err,
                SqlError::Storage(citadel_core::Error::ReadBudgetExceeded { .. })
            ));
        });
        let budget = ReadBudget::new(charged, charged);
        with_read_budget(&budget, || {
            conn.query("SELECT body FROM docs").unwrap();
            assert_eq!(budget.remaining(), 0);
            let err = conn
                .query(
                    "WITH removed AS (DELETE FROM docs WHERE id = 1 RETURNING *) \
                     SELECT COUNT(*) FROM removed",
                )
                .unwrap_err();
            assert!(matches!(err, SqlError::Unsupported(_)));
        });
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM docs").unwrap().rows[0][0],
            Value::Integer(1)
        );
        conn.execute("INSERT INTO docs VALUES (2, 'after-read')")
            .unwrap();
        conn.execute("COMMIT").unwrap();
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM docs").unwrap().rows[0][0],
            Value::Integer(2)
        );
    }

    #[test]
    fn a_refused_prepared_mutator_does_not_capture_a_pending_savepoint() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'before')").unwrap();
        let update = conn
            .prepare("UPDATE t SET value = $1 WHERE id = $2")
            .unwrap();

        conn.execute("BEGIN").unwrap();
        conn.execute("SAVEPOINT s").unwrap();
        let token = citadel::CancelToken::new();
        token.cancel();
        db.set_cancel(Some(token));
        let error = update
            .execute(&[Value::Text("after".into()), Value::Integer(1)])
            .expect_err("the prepared update ignored cancellation");
        assert!(matches!(
            error,
            SqlError::Storage(citadel_core::Error::Interrupted)
        ));
        assert!(conn.inner.borrow().savepoint_stack[0].snapshot.is_none());

        db.set_cancel(None);
        conn.execute("ROLLBACK").unwrap();
    }

    #[cfg(panic = "unwind")]
    fn assert_injected_sort_panic(payload: Box<dyn std::any::Any + Send>) {
        assert!(
            payload.is::<crate::executor::helpers::InjectedSortComparatorPanic>(),
            "the executor replaced the comparator's panic payload"
        );
    }

    /// A row is durable in the in-memory write set before its AFTER trigger
    /// runs. If that trigger's sort has a genuine comparator bug, catching the
    /// unwind must not turn the row prefix into a committable transaction.
    #[test]
    #[cfg(panic = "unwind")]
    fn an_unwinding_sort_poisons_its_explicit_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        install_sorting_insert_trigger(&conn);
        conn.execute("BEGIN").unwrap();

        let injection = crate::executor::helpers::inject_sort_comparator_panic();
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = conn.execute("INSERT INTO sort_sink VALUES (1)");
        }))
        .expect_err("the injected comparator panic did not surface");
        drop(injection);
        assert_injected_sort_panic(payload);

        let commit = conn
            .execute("COMMIT")
            .expect_err("the unwound row prefix remained committable");
        assert!(matches!(
            commit,
            SqlError::Storage(citadel_core::Error::TransactionFailed)
        ));

        drop(conn);
        drop(db);
        let reopened = DatabaseBuilder::new(dir.path().join("t.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .open()
            .unwrap();
        let observer = Connection::open(&reopened).unwrap();
        let count = observer.query("SELECT COUNT(*) FROM sort_sink").unwrap();
        assert_eq!(count.rows[0][0], Value::Integer(0));
    }

    #[test]
    fn recovery_batch_ignores_handle_cancellation_and_finishes_its_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE recovery_sink (id INTEGER PRIMARY KEY, value INTEGER)")
            .unwrap();
        let token = citadel::CancelToken::new();
        token.cancel();
        db.set_cancel(Some(token));
        let first = [Value::Integer(1), Value::Integer(10)];
        let second = [Value::Integer(20), Value::Integer(1)];

        let result = conn.execute_params_batch_uncancelled_recovery(&[
            ("INSERT INTO recovery_sink VALUES ($1, $2)", &first),
            ("UPDATE recovery_sink SET value = $1 WHERE id = $2", &second),
        ]);
        db.set_cancel(None);
        let results = result.unwrap();

        assert_eq!(results.len(), 2);
        assert!(!conn.in_transaction());
        assert_eq!(
            conn.query("SELECT value FROM recovery_sink WHERE id = 1")
                .unwrap()
                .rows[0][0],
            Value::Integer(20)
        );
    }

    #[test]
    fn recovery_batch_rejects_ddl_before_it_can_stale_the_schema_cache() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();

        let error = conn
            .execute_params_batch_uncancelled_recovery(&[
                ("CREATE TABLE ghost (id INTEGER PRIMARY KEY)", &[]),
                ("INSERT INTO missing VALUES (1)", &[]),
            ])
            .unwrap_err();

        assert!(matches!(error, SqlError::Unsupported(_)));
        assert!(conn.table_schema("ghost").is_none());
        conn.execute("CREATE TABLE ghost (id INTEGER PRIMARY KEY)")
            .unwrap();
    }

    /// The recovery escape hatch owns a private transaction. A genuine
    /// executor panic must abort that transaction and restore the connection
    /// before the same panic payload is resumed to its caller.
    #[test]
    #[cfg(panic = "unwind")]
    fn an_unwinding_uncancelled_recovery_write_does_not_strand_its_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        install_sorting_insert_trigger(&conn);

        let injection = crate::executor::helpers::inject_sort_comparator_panic();
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ =
                conn.execute_params_uncancelled_recovery("INSERT INTO sort_sink VALUES (1)", &[]);
        }))
        .expect_err("the injected comparator panic did not surface");
        drop(injection);
        assert_injected_sort_panic(payload);

        assert!(!conn.in_transaction(), "recovery left its writer installed");
        assert!(matches!(
            conn.execute("COMMIT"),
            Err(SqlError::NoActiveTransaction)
        ));
        let count = conn.query("SELECT COUNT(*) FROM sort_sink").unwrap();
        assert_eq!(count.rows[0][0], Value::Integer(0));
    }

    #[test]
    #[cfg(panic = "unwind")]
    fn an_unwinding_batch_aborts_its_private_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        install_sorting_insert_trigger(&conn);

        let injection = crate::executor::helpers::inject_sort_comparator_panic();
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = conn.execute_batch("INSERT INTO sort_sink VALUES (1)");
        }))
        .expect_err("the injected comparator panic did not surface");
        drop(injection);
        assert_injected_sort_panic(payload);

        assert!(!conn.in_transaction(), "batch left its writer installed");
        assert!(matches!(
            conn.execute("COMMIT"),
            Err(SqlError::NoActiveTransaction)
        ));
        conn.execute("INSERT INTO sort_sink VALUES (2)").unwrap();
        let rows = conn.query("SELECT id FROM sort_sink").unwrap().rows;
        assert_eq!(rows, vec![vec![Value::Integer(2)]]);
    }

    #[test]
    fn a_pre_cancelled_batch_stops_before_parsing_or_taking_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        let token = citadel::CancelToken::new();
        token.cancel();
        db.set_cancel(Some(token));

        let error = conn
            .execute_batch("this is deliberately not sql")
            .expect_err("the pre-cancelled batch reached the parser");
        assert!(matches!(
            error,
            SqlError::Storage(citadel_core::Error::Interrupted)
        ));
        assert!(!conn.in_transaction());

        db.set_cancel(None);
        conn.execute_batch("CREATE TABLE after_cancel (id INTEGER PRIMARY KEY)")
            .unwrap();
    }

    /// The two public executor entry points can be called without a Connection,
    /// so each must apply the same unwind poisoning on its own.
    #[test]
    #[cfg(panic = "unwind")]
    fn direct_mutating_executor_entries_poison_on_unwind() {
        for direct_insert in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = fresh_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            install_sorting_insert_trigger(&conn);
            drop(conn);

            let mut schema = SchemaManager::load(&db).unwrap();
            let mut wtx = db.begin_write().unwrap();
            let stmt = parser::parse_sql("INSERT INTO sort_sink VALUES (1)").unwrap();
            let injection = crate::executor::helpers::inject_sort_comparator_panic();
            let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if direct_insert {
                    let Statement::Insert(insert) = &stmt else {
                        unreachable!("the test parsed an INSERT")
                    };
                    let _ = crate::executor::exec_insert_in_txn(&mut wtx, &schema, insert, &[]);
                } else {
                    let _ = crate::executor::execute_in_txn(&mut wtx, &mut schema, &stmt, &[]);
                }
            }))
            .expect_err("the injected comparator panic did not surface");
            drop(injection);
            assert_injected_sort_panic(payload);
            assert!(matches!(
                wtx.commit(),
                Err(citadel_core::Error::TransactionFailed)
            ));
        }
    }

    #[test]
    fn a_tripped_token_does_not_strand_temp_storage_on_connection_drop() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
            .unwrap();
        let physical_name = conn.inner.borrow().temp_table_names[0].clone();
        conn.execute("BEGIN").unwrap();

        let token = citadel::CancelToken::new();
        token.cancel();
        db.set_cancel(Some(token));
        drop(conn);
        db.set_cancel(None);

        let observer = Connection::open(&db).unwrap();
        let err = observer
            .query(&format!("SELECT * FROM {physical_name}"))
            .expect_err("cancelled connection drop left its TEMP backing table behind");
        assert!(
            matches!(
                err,
                crate::SqlError::TableNotFound(_)
                    | crate::SqlError::Storage(citadel_core::Error::TableNotFound(_))
            ),
            "unexpected lookup error for cleaned TEMP backing table: {err:?}"
        );
    }

    #[test]
    fn deferred_temp_cleanup_drains_after_the_competing_writer_commits() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let temp_owner = Connection::open(&db).unwrap();
        temp_owner
            .execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
            .unwrap();
        let physical_name = temp_owner.inner.borrow().temp_table_names[0].clone();

        let writer = Connection::open(&db).unwrap();
        writer.execute("BEGIN").unwrap();
        drop(temp_owner);

        writer.execute("COMMIT").unwrap();

        let mut read = db.begin_read();
        assert!(
            matches!(
                read.table_entry_count(physical_name.as_bytes()),
                Err(citadel_core::Error::TableNotFound(_))
            ),
            "COMMIT must drain the cleanup queued behind its writer slot"
        );
        drop(read);

        let observer = Connection::open(&db).unwrap();
        let err = observer
            .query(&format!("SELECT * FROM {physical_name}"))
            .expect_err("the next writer-release boundary must drain deferred TEMP cleanup");
        assert!(
            matches!(
                err,
                crate::SqlError::TableNotFound(_)
                    | crate::SqlError::Storage(citadel_core::Error::TableNotFound(_))
            ),
            "unexpected lookup error for cleaned TEMP backing table: {err:?}"
        );
    }

    #[test]
    fn a_failed_write_transaction_is_not_misreported_as_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("BEGIN").unwrap();

        let injected = {
            let mut inner = conn.inner.borrow_mut();
            let wtx = inner.active_txn.as_write_mut().unwrap();
            wtx.table_update_range(b"t", b"", |_key, _value| {
                Err::<Option<bool>, _>(citadel_core::Error::DatabaseCorrupted)
            })
            .unwrap_err()
        };
        assert!(matches!(injected, citadel_core::Error::DatabaseCorrupted));

        let next = conn.query("SELECT 1").unwrap_err();
        assert!(matches!(
            next,
            SqlError::Storage(citadel_core::Error::TransactionFailed)
        ));
        let commit = conn.execute("COMMIT").unwrap_err();
        assert!(matches!(
            commit,
            SqlError::Storage(citadel_core::Error::TransactionFailed)
        ));
        assert!(!conn.in_transaction());
    }

    /// The streaming dedup key is the whole encoded row key, so a composite primary key made
    /// every row unique and the fast path answered differently from the general one.
    #[test]
    fn distinct_agrees_across_paths_for_a_composite_pk() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE d (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
            .unwrap();
        for (a, b) in [(1, 1), (1, 2), (1, 3)] {
            conn.execute(&format!("INSERT INTO d VALUES ({a}, {b})"))
                .unwrap();
        }
        // The WHERE clause is what forces the general path, so the two must agree.
        assert_eq!(row_count(&conn, "SELECT DISTINCT a FROM d"), 1);
        assert_eq!(row_count(&conn, "SELECT DISTINCT a FROM d WHERE b > 0"), 1);
    }

    /// Joined columns were rebuilt with a hardcoded Binary collation, so any query containing
    /// a JOIN silently compared case-sensitively.
    #[test]
    fn join_preserves_column_collation() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE ca (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)")
            .unwrap();
        conn.execute("CREATE TABLE cb (id INTEGER PRIMARY KEY, aid INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO ca VALUES (1, 'Alice')").unwrap();
        conn.execute("INSERT INTO cb VALUES (1, 1)").unwrap();

        assert_eq!(
            row_count(&conn, "SELECT name FROM ca WHERE name = 'alice'"),
            1
        );
        assert_eq!(
            row_count(
                &conn,
                "SELECT ca.name FROM ca JOIN cb ON cb.aid = ca.id WHERE ca.name = 'alice'"
            ),
            1,
            "NOCASE collation was dropped when building the joined columns"
        );
    }

    /// Value had no Vector arm in PartialEq, so a vector did not equal itself while Ord and
    /// Hash both treated the pair as identical.
    #[test]
    fn vectors_compare_equal_to_themselves() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE vt (id INTEGER PRIMARY KEY, v VECTOR(3))")
            .unwrap();
        conn.execute("INSERT INTO vt VALUES (1, '[1,2,3]'::VECTOR(3))")
            .unwrap();
        conn.execute("INSERT INTO vt VALUES (2, '[1,2,3]'::VECTOR(3))")
            .unwrap();

        let eq = conn.query("SELECT count(*) FROM vt WHERE v = v").unwrap();
        assert_eq!(
            eq.rows[0][0],
            Value::Integer(2),
            "a vector must equal itself"
        );
        // Both DISTINCT paths must agree that the two rows hold the same vector.
        assert_eq!(
            row_count(&conn, "SELECT DISTINCT v FROM vt WHERE id > 0"),
            1
        );
        assert_eq!(row_count(&conn, "SELECT DISTINCT v FROM vt"), 1);
    }

    /// A branch-rooted table with wide key gaps, so a later insert lands in a middle leaf.
    fn seeded_gapped_table(conn: &Connection) {
        // NOT NULL + fixed width, so a range UPDATE takes the in-place patch lane.
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        for i in 0..4000i64 {
            conn.execute(&format!("INSERT INTO t VALUES ({}, {})", i * 10, i))
                .unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }

    fn row_count(conn: &Connection, sql: &str) -> usize {
        conn.query(sql).unwrap().rows.len()
    }

    /// SAVEPOINT advances the write txn id, so the next insert CoWs the spine to fresh page
    /// ids. An insert into a non-rightmost leaf left the last-insert cache naming the
    /// superseded ancestors, and the following cached insert re-rooted the tree from them,
    /// discarding the intervening write on an otherwise successful COMMIT.
    #[test]
    fn insert_after_savepoint_survives_a_cached_append() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        seeded_gapped_table(&conn);

        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (1000000, 1)").unwrap();
        conn.execute("SAVEPOINT sp1").unwrap();
        conn.execute("INSERT INTO t VALUES (15005, 2)").unwrap();
        conn.execute("INSERT INTO t VALUES (1000010, 3)").unwrap();
        conn.execute("COMMIT").unwrap();

        assert_eq!(row_count(&conn, "SELECT v FROM t WHERE id = 15005"), 1);
        assert_eq!(row_count(&conn, "SELECT v FROM t WHERE id = 1000000"), 1);
        assert_eq!(row_count(&conn, "SELECT v FROM t WHERE id = 1000010"), 1);
    }

    /// A middle-leaf insert rewrites the cached delete path through the newly copied page
    /// ids. If that remap were wrong, the next cached delete would propagate over superseded
    /// ancestors and re-root the tree from them.
    #[test]
    fn delete_through_a_remapped_cache_keeps_the_tree_intact() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        seeded_gapped_table(&conn);

        conn.execute("BEGIN").unwrap();
        conn.execute("DELETE FROM t WHERE id = 1000").unwrap(); // arms the delete cache
        conn.execute("SAVEPOINT sp1").unwrap(); // bumps txn_id, so the spine gets copied
        conn.execute("INSERT INTO t VALUES (25005, 999)").unwrap(); // middle leaf -> remap
        conn.execute("DELETE FROM t WHERE id = 1010").unwrap(); // same leaf -> cache hit
        conn.execute("COMMIT").unwrap();

        assert_eq!(row_count(&conn, "SELECT v FROM t WHERE id = 1000"), 0);
        assert_eq!(row_count(&conn, "SELECT v FROM t WHERE id = 1010"), 0);
        assert_eq!(row_count(&conn, "SELECT v FROM t WHERE id = 25005"), 1);
        let total = conn.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(total.rows[0][0], Value::Integer(3999));
    }

    /// The range update re-roots the tree outside BTree's own methods; the same cached
    /// append would otherwise revert every updated row.
    #[test]
    fn range_update_after_savepoint_survives_a_cached_append() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        seeded_gapped_table(&conn);

        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (1000000, 1)").unwrap();
        conn.execute("SAVEPOINT sp1").unwrap();
        // Sentinel no seeded row can already hold.
        conn.execute("UPDATE t SET v = -1 WHERE id BETWEEN 10000 AND 20000")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1000010, 3)").unwrap();
        conn.execute("COMMIT").unwrap();

        let updated = row_count(&conn, "SELECT id FROM t WHERE v = -1");
        assert_eq!(
            updated, 1001,
            "range update reverted after the cached append"
        );
    }

    /// The path functions take 2..=4 arguments, but the NULL short-circuit indexed [0] and [1]
    /// before the callee could reject a short list.
    #[test]
    fn jsonb_path_functions_reject_short_arg_lists() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        for f in [
            "JSONB_PATH_EXISTS",
            "JSONB_PATH_MATCH",
            "JSONB_PATH_QUERY_FIRST",
            "JSONB_PATH_QUERY_ARRAY",
            "JSONB_PATH_EXISTS_TZ",
            "JSONB_PATH_MATCH_TZ",
            "JSONB_PATH_QUERY_TZ",
            "JSONB_PATH_QUERY_FIRST_TZ",
            "JSONB_PATH_QUERY_ARRAY_TZ",
        ] {
            assert!(
                conn.query(&format!("SELECT {f}()")).is_err(),
                "{f} with 0 args"
            );
            assert!(
                conn.query(&format!("SELECT {f}('{{}}')")).is_err(),
                "{f} with 1 arg"
            );
        }
        // A well-formed call still works.
        conn.query("SELECT JSONB_PATH_EXISTS('{\"a\":1}'::JSONB, '$.a')")
            .unwrap();
    }

    #[test]
    fn strftime_rejects_bad_formats_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        for fmt in ["%", "%E", "%O", "%:", "%-", "%_", "%1", "%#"] {
            let sql = format!("SELECT STRFTIME('{fmt}', CURRENT_TIMESTAMP)");
            assert!(conn.query(&sql).is_err(), "STRFTIME('{fmt}') should error");
        }
        conn.query("SELECT STRFTIME('%Y-%m-%d', CURRENT_TIMESTAMP)")
            .unwrap();
    }

    #[test]
    fn multibyte_timezone_offset_is_rejected_not_panicked() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        // Four BYTES but three chars, so the +HHMM split lands mid-character.
        assert!(conn.execute("SET TIME ZONE '+\u{20AC}a'").is_err());
        assert!(conn.set_session_timezone("-\u{20AC}a").is_err());
        assert_eq!(conn.session_timezone(), "UTC");
    }

    #[test]
    fn legacy_session_dependent_schema_opens_at_utc_until_remediated() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE legacy_path (id INTEGER PRIMARY KEY, j JSONB, g BOOLEAN)")
            .unwrap();

        // Simulate a catalog written before persistent-expression validation
        // existed. Current DDL deliberately cannot create this definition.
        let sql = "JSONB_PATH_EXISTS_TZ(j, '$.timestamp_tz()')";
        let mut legacy = conn.table_schema("legacy_path").unwrap();
        legacy.columns[2].generated_sql = Some(sql.into());
        legacy.columns[2].generated_expr = Some(parser::parse_sql_expr(sql).unwrap());
        legacy.columns[2].generated_kind = Some(parser::GeneratedKind::Stored);
        drop(conn);
        let mut wtx = db.begin_write().unwrap();
        SchemaManager::save_schema(&mut wtx, &legacy).unwrap();
        wtx.commit().unwrap();

        let conn = Connection::open(&db).expect("legacy vault must remain openable at UTC");
        assert_eq!(conn.session_timezone(), "UTC");
        let error = conn
            .execute("SET TIME ZONE 'America/New_York'")
            .expect_err("legacy generated expression allowed a session-zone change");
        let message = error.to_string();
        assert!(message.contains("generated column \"legacy_path.g\""));
        assert!(message.contains("rewrite or drop/recreate"));
        assert_eq!(conn.session_timezone(), "UTC");

        conn.execute("BEGIN").unwrap();
        assert!(conn.execute("SET LOCAL TIME ZONE '+10:00'").is_err());
        conn.execute("ROLLBACK").unwrap();

        conn.execute("DROP TABLE legacy_path").unwrap();
        conn.execute("SET TIME ZONE 'America/New_York'").unwrap();
        assert_eq!(conn.session_timezone(), "America/New_York");
    }

    #[test]
    fn legacy_volatile_expression_index_opens_in_drop_only_recovery_mode() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE legacy_random (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE INDEX legacy_random_idx ON legacy_random (id)")
            .unwrap();

        let mut legacy = conn.table_schema("legacy_random").unwrap();
        let index = legacy
            .indices
            .iter_mut()
            .find(|index| index.name == "legacy_random_idx")
            .unwrap();
        index.keys = vec![crate::types::IndexKey::Expr {
            expr: parser::parse_sql_expr("RANDOM()").unwrap(),
            original_sql: "RANDOM()".into(),
        }];
        drop(conn);
        let mut wtx = db.begin_write().unwrap();
        SchemaManager::save_schema(&mut wtx, &legacy).unwrap();
        wtx.commit().unwrap();

        let conn = Connection::open(&db).expect("volatile legacy catalog must remain openable");
        conn.execute("SET TIME ZONE 'UTC'")
            .expect("evaluation-equivalent connection initialization must remain safe");
        let timezone_error = conn
            .execute("SET TIME ZONE 'America/New_York'")
            .expect_err("legacy volatile catalog allowed a session-zone change");
        assert!(timezone_error
            .to_string()
            .contains("legacy catalog recovery required"));
        let error = conn
            .query("SELECT 1")
            .expect_err("unsafe legacy index was exposed to normal execution");
        let message = error.to_string();
        assert!(message.contains("legacy catalog recovery required"));
        assert!(message.contains("legacy_random_idx"));
        assert!(message.contains("volatile function RANDOM()"));
        assert!(message.contains("only DROP INDEX, DROP TABLE, or ALTER TABLE DROP COLUMN"));

        conn.execute("DROP INDEX legacy_random_idx").unwrap();
        assert_eq!(
            conn.query("SELECT 1").unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
    }

    #[test]
    fn a_connection_does_not_inherit_an_unrelated_clock_scope() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        let unrelated = crate::datetime::parse_timestamp("2000-01-01 00:00:00").unwrap();

        let actual = crate::datetime::with_txn_clock(Some(unrelated), || {
            conn.query("SELECT CURRENT_TIMESTAMP").unwrap().rows[0][0].clone()
        });

        assert_ne!(actual, Value::Timestamp(unrelated));
    }

    #[test]
    fn transaction_lifecycle_does_not_replace_an_ambient_clock_scope() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        let unrelated = crate::datetime::parse_timestamp("2000-01-01 00:00:00").unwrap();
        let expression = crate::parser::Expr::Function {
            name: "CURRENT_TIMESTAMP".into(),
            args: Vec::new(),
            distinct: false,
        };
        let columns = crate::eval::ColumnMap::new(&[]);
        let direct_eval = || {
            crate::eval::eval_expr(&expression, &crate::eval::EvalCtx::new(&columns, &[])).unwrap()
        };

        crate::datetime::with_txn_clock(Some(unrelated), || {
            conn.execute("BEGIN").unwrap();
            assert_eq!(direct_eval(), Value::Timestamp(unrelated));
            assert_ne!(
                conn.query("SELECT CURRENT_TIMESTAMP").unwrap().rows[0][0],
                Value::Timestamp(unrelated)
            );
            assert_eq!(direct_eval(), Value::Timestamp(unrelated));
            conn.execute("COMMIT").unwrap();
            assert_eq!(direct_eval(), Value::Timestamp(unrelated));
        });
    }

    #[test]
    fn late_cancellation_restores_a_timezone_change_before_commit() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("BEGIN").unwrap();

        let token = citadel::CancelToken::new();
        db.set_cancel(Some(token.clone()));
        let _late_cancel = cancel_after_statement(token);
        let error = conn
            .execute("SET TIME ZONE '+10:00'")
            .expect_err("the late cancellation was not observed");
        assert!(matches!(
            error,
            SqlError::Storage(citadel_core::Error::Interrupted)
        ));
        assert_eq!(conn.session_timezone(), "UTC");

        db.set_cancel(None);
        conn.execute("COMMIT").unwrap();
        assert_eq!(conn.session_timezone(), "UTC");
    }

    #[test]
    fn execute_batch_commits_all_statements() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
            .unwrap();

        let results = conn
            .execute_batch(
                "INSERT INTO t VALUES (1, 10); \
                 INSERT INTO t VALUES (2, 20); \
                 UPDATE t SET n = n + 1 WHERE id = 1;",
            )
            .unwrap();
        assert_eq!(results.len(), 3);

        let qr = conn.query("SELECT id, n FROM t ORDER BY id").unwrap();
        assert_eq!(qr.rows.len(), 2);
        assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(11)]);
        assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Integer(20)]);
    }

    #[test]
    fn execute_batch_rolls_back_whole_batch_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

        let res =
            conn.execute_batch("INSERT INTO t VALUES (2, 20); INSERT INTO t VALUES (1, 999);");
        assert!(res.is_err());

        let qr = conn.query("SELECT id, n FROM t ORDER BY id").unwrap();
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(100)]);

        conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();
        assert!(!conn.in_transaction());
    }

    #[test]
    fn execute_batch_rejects_txn_control() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        assert!(conn
            .execute_batch("INSERT INTO t VALUES (1); COMMIT;")
            .is_err());
        assert!(!conn.in_transaction());
    }

    #[test]
    fn execute_batch_rejected_inside_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        assert!(conn.execute_batch("INSERT INTO t VALUES (1);").is_err());
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn streamed_expression_projection_correct() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
            .unwrap();
        conn.execute_batch(
            "INSERT INTO t VALUES (1, 10); INSERT INTO t VALUES (2, 20); INSERT INTO t VALUES (3, 30);",
        )
        .unwrap();
        let stmt = conn.prepare("SELECT id + 1, n * 2 FROM t").unwrap();
        let qr = stmt.query_collect(&[]).unwrap();
        assert_eq!(qr.rows.len(), 3);
        assert_eq!(qr.rows[0], vec![Value::Integer(2), Value::Integer(20)]);
        assert_eq!(qr.rows[1], vec![Value::Integer(3), Value::Integer(40)]);
        assert_eq!(qr.rows[2], vec![Value::Integer(4), Value::Integer(60)]);
    }

    #[test]
    fn jsonb_contains_raw_predicate_correct() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, data JSONB)")
            .unwrap();
        conn.execute_batch(
            "INSERT INTO u VALUES (1, '{\"role\":\"admin\",\"x\":1}'); \
             INSERT INTO u VALUES (2, '{\"role\":\"user\"}'); \
             INSERT INTO u VALUES (3, NULL); \
             INSERT INTO u VALUES (4, '{\"role\":\"admin\"}');",
        )
        .unwrap();
        let stmt = conn
            .prepare("SELECT id FROM u WHERE data @> '{\"role\":\"admin\"}'::jsonb")
            .unwrap();
        let qr = stmt.query_collect(&[]).unwrap();
        let mut ids: Vec<i64> = qr
            .rows
            .iter()
            .map(|r| match r[0] {
                Value::Integer(i) => i,
                _ => -1,
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 4]);
    }
}
