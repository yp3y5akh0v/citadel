//! Public SQL connection API.

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
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
use crate::parser::{BeginAccessMode, QueryBody, SelectQuery, Statement};
use crate::prepared::PreparedStatement;
use crate::schema::{SchemaManager, SchemaSnapshot};
use crate::types::{ExecutionResult, QueryResult, TableSchema, Value};

const DEFAULT_CACHE_CAPACITY: usize = 64;

#[derive(Debug)]
pub struct ScriptExecution {
    pub completed: Vec<ExecutionResult>,
    pub error: Option<SqlError>,
}

fn parse_fixed_offset(s: &str) -> Option<()> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("z") || s.eq_ignore_ascii_case("utc") {
        return Some(());
    }
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    if !matches!(bytes[0], b'+' | b'-') {
        return None;
    }
    let rest = &s[1..];
    let (hh, mm) = if let Some((h, m)) = rest.split_once(':') {
        (h, m)
    } else if rest.len() == 4 {
        (&rest[..2], &rest[2..])
    } else if rest.len() == 2 {
        (rest, "00")
    } else {
        return None;
    };
    let h: u32 = hh.parse().ok()?;
    let m: u32 = mm.parse().ok()?;
    if h > 23 || m > 59 {
        return None;
    }
    Some(())
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

fn stmt_mutates(stmt: &Statement) -> bool {
    if matches!(
        stmt,
        Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Truncate(_)
            | Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::AlterTable(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger(_)
            | Statement::CreateMaterializedView(_)
            | Statement::RefreshMaterializedView(_)
            | Statement::DropMaterializedView(_)
    ) {
        return true;
    }
    if let Statement::Select(sq) = stmt {
        if select_query_has_dml(sq) {
            return true;
        }
    }
    false
}

fn select_query_has_dml(sq: &SelectQuery) -> bool {
    sq.ctes.iter().any(|cte| query_body_has_dml(&cte.body)) || query_body_has_dml(&sq.body)
}

fn query_body_has_dml(body: &QueryBody) -> bool {
    match body {
        QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => true,
        QueryBody::Compound(c) => query_body_has_dml(&c.left) || query_body_has_dml(&c.right),
        QueryBody::Select(_) => false,
    }
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
}

struct SavepointSnapshot {
    wtx_snap: WriteTxnSnapshot,
    schema_snap: SchemaSnapshot,
}

/// Active transaction held by a Connection. `None` outside BEGIN/COMMIT; `Write` for normal
/// BEGIN (or BEGIN READ WRITE); `Read` for BEGIN READ ONLY.
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
    fn take(&mut self) -> ActiveTxn<'a> {
        std::mem::replace(self, ActiveTxn::None)
    }
}

pub(crate) struct ConnectionInner<'a> {
    pub(crate) schema: SchemaManager,
    active_txn: ActiveTxn<'a>,
    savepoint_stack: Vec<SavepointEntry>,
    in_place_saved: Option<bool>,
    pub(crate) stmt_cache: LruCache<String, CacheEntry>,
    txn_start_ts: Option<i64>,
    session_timezone: String,
    /// Namespaces TEMP tables as `__temp_<id>_<name>`. Cleaned up on Connection drop.
    temp_id: u64,
    temp_table_names: Vec<String>,
}

pub struct Connection<'a> {
    pub(crate) db: &'a Database,
    pub(crate) inner: RefCell<ConnectionInner<'a>>,
}

impl<'a> Connection<'a> {
    pub fn open(db: &'a Database) -> Result<Self> {
        let schema = SchemaManager::load(db)?;
        let stmt_cache = LruCache::new(NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap());
        let temp_id = generate_temp_id();
        Ok(Self {
            db,
            inner: RefCell::new(ConnectionInner {
                schema,
                active_txn: ActiveTxn::None,
                savepoint_stack: Vec::new(),
                in_place_saved: None,
                stmt_cache,
                txn_start_ts: None,
                session_timezone: "UTC".to_string(),
                temp_id,
                temp_table_names: Vec::new(),
            }),
        })
    }

    /// Txn-start UTC µs inside BEGIN/COMMIT, else `None`.
    pub fn txn_start_ts(&self) -> Option<i64> {
        self.inner.borrow().txn_start_ts
    }

    /// Returns the session time-zone (IANA name or fixed offset). Default `"UTC"`.
    pub fn session_timezone(&self) -> String {
        self.inner.borrow().session_timezone.clone()
    }

    /// Set the session time-zone. Accepts IANA names, ISO-8601 offsets, `"UTC"`, `"Z"`.
    pub fn set_session_timezone(&self, tz: &str) -> Result<()> {
        self.inner.borrow_mut().set_session_timezone_impl(tz)
    }

    pub fn execute(&self, sql: &str) -> Result<ExecutionResult> {
        self.inner.borrow_mut().execute_impl(self.db, sql)
    }

    pub fn execute_params(&self, sql: &str, params: &[Value]) -> Result<ExecutionResult> {
        self.inner
            .borrow_mut()
            .execute_params_impl(self.db, sql, params)
    }

    /// Execute `;`-separated SQL statements. Stops at the first failure.
    pub fn execute_script(&self, sql: &str) -> ScriptExecution {
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
            match self.inner.borrow_mut().dispatch(self.db, &stmt, &[]) {
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

    pub fn query(&self, sql: &str) -> Result<QueryResult> {
        self.query_params(sql, &[])
    }

    pub fn query_params(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
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
        let new_schema = SchemaManager::load(self.db)?;
        self.inner.borrow_mut().schema = new_schema;
        Ok(())
    }
}

impl<'a> ConnectionInner<'a> {
    pub(crate) fn active_txn_is_some(&self) -> bool {
        self.active_txn.is_active()
    }

    fn set_session_timezone_impl(&mut self, tz: &str) -> Result<()> {
        let upper = tz.to_ascii_uppercase();
        if (upper.starts_with("UTC+") || upper.starts_with("UTC-")) && tz.len() > 3 {
            return Err(SqlError::InvalidTimezone(format!(
                "'{tz}' is ambiguous; use ISO-8601 offset (e.g. '+05:00') or named zone (e.g. 'Etc/GMT-5')"
            )));
        }
        if jiff::tz::TimeZone::get(tz).is_err() && parse_fixed_offset(tz).is_none() {
            return Err(SqlError::InvalidTimezone(format!(
                "{tz}: not a known IANA zone or ISO-8601 offset (e.g. '+05:00', 'UTC', 'America/New_York')"
            )));
        }
        self.session_timezone = tz.to_string();
        Ok(())
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

    fn run_compiled(
        &mut self,
        db: &'a Database,
        plan: &Arc<dyn executor::CompiledPlan>,
        stmt: &Statement,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        use executor::compile::ActiveTxnRef;
        let schema = &self.schema;
        let exec = || {
            if params.is_empty() {
                plan.execute(db, schema, stmt, params, ActiveTxnRef::None)
            } else {
                crate::eval::with_scoped_params(params, || {
                    plan.execute(db, schema, stmt, params, ActiveTxnRef::None)
                })
            }
        };
        if plan.needs_txn_clock() {
            let cached_ts = self
                .txn_start_ts
                .or_else(|| Some(crate::datetime::now_micros()));
            crate::datetime::with_txn_clock(cached_ts, exec)
        } else {
            exec()
        }
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
            if !self.savepoint_stack.is_empty() && stmt_mutates(stmt) {
                self.capture_pending_snapshots();
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
        let schema = &self.schema;
        let txn = match &mut self.active_txn {
            ActiveTxn::Write(wtx) => ActiveTxnRef::Write(wtx),
            ActiveTxn::Read(rtx) => ActiveTxnRef::Read(rtx),
            ActiveTxn::None => ActiveTxnRef::None,
        };
        if params.is_empty() || !plan.uses_scoped_params() {
            plan.execute(db, schema, stmt, params, txn)
        } else {
            crate::eval::with_scoped_params(params, || plan.execute(db, schema, stmt, params, txn))
        }
    }

    pub(crate) fn dispatch(
        &mut self,
        db: &'a Database,
        stmt: &Statement,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        let cached_ts = self
            .txn_start_ts
            .or_else(|| Some(crate::datetime::now_micros()));
        crate::datetime::with_txn_clock(cached_ts, || {
            if params.is_empty() {
                self.dispatch_inner(db, stmt, params)
            } else {
                crate::eval::with_scoped_params(params, || self.dispatch_inner(db, stmt, params))
            }
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
                let ts = crate::datetime::txn_or_clock_micros();
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
                self.txn_start_ts = Some(ts);
                crate::datetime::set_txn_clock(Some(ts));
                Ok(ExecutionResult::Ok)
            }
            Statement::Commit => {
                match self.active_txn.take() {
                    ActiveTxn::None => return Err(SqlError::NoActiveTransaction),
                    ActiveTxn::Write(mut wtx) => {
                        crate::executor::helpers::drain_deferred_fk_checks(&mut wtx)?;
                        wtx.commit().map_err(SqlError::Storage)?;
                    }
                    ActiveTxn::Read(_rtx) => {}
                }
                self.clear_savepoint_state();
                self.txn_start_ts = None;
                crate::datetime::set_txn_clock(None);
                Ok(ExecutionResult::Ok)
            }
            Statement::Rollback => {
                match self.active_txn.take() {
                    ActiveTxn::None => return Err(SqlError::NoActiveTransaction),
                    ActiveTxn::Write(wtx) => {
                        wtx.abort();
                        self.schema = SchemaManager::load(db)?;
                    }
                    ActiveTxn::Read(_rtx) => {}
                }
                self.clear_savepoint_state();
                self.txn_start_ts = None;
                crate::datetime::set_txn_clock(None);
                Ok(ExecutionResult::Ok)
            }
            Statement::Savepoint(name) => self.do_savepoint(name),
            Statement::ReleaseSavepoint(name) => self.do_release(name),
            Statement::RollbackTo(name) => self.do_rollback_to(name),
            Statement::SetTimezone(zone) => {
                self.set_session_timezone_impl(zone)?;
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
                if self.active_txn.is_read_only() && stmt_mutates(stmt) {
                    return Err(SqlError::Unsupported(
                        "cannot execute mutating statement inside a read-only transaction".into(),
                    ));
                }
                if self.active_txn.as_write_mut().is_some() && stmt_mutates(stmt) {
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
        self.in_place_saved = None;
    }

    fn do_savepoint(&mut self, name: &str) -> Result<ExecutionResult> {
        let wtx = self
            .active_txn
            .as_write_mut()
            .ok_or(SqlError::NoActiveTransaction)?;

        if self.savepoint_stack.is_empty() {
            self.in_place_saved = Some(wtx.in_place());
            wtx.set_in_place(false);
        }

        self.savepoint_stack.push(SavepointEntry {
            name: name.to_string(),
            snapshot: None,
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

        for i in 0..last_pending {
            if self.savepoint_stack[i].snapshot.is_none() {
                self.savepoint_stack[i].snapshot = Some(SavepointSnapshot {
                    wtx_snap: wtx_snap.clone(),
                    schema_snap: schema_snap.clone(),
                });
            }
        }
        self.savepoint_stack[last_pending].snapshot = Some(SavepointSnapshot {
            wtx_snap,
            schema_snap,
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

        if self.savepoint_stack.is_empty() {
            if let (Some(wtx), Some(original)) =
                (self.active_txn.as_write_mut(), self.in_place_saved.take())
            {
                wtx.set_in_place(original);
            }
        }

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
        let snapshot = match entry.snapshot.take() {
            Some(s) => s,
            None => return Ok(ExecutionResult::Ok),
        };

        let wtx = match self.active_txn.as_write_mut() {
            Some(w) => w,
            None => return Err(SqlError::NoActiveTransaction),
        };
        wtx.restore_snapshot(snapshot.wtx_snap);
        self.schema.restore_snapshot(snapshot.schema_snap);

        Ok(ExecutionResult::Ok)
    }
}

impl<'a> Drop for Connection<'a> {
    fn drop(&mut self) {
        let temp_names = std::mem::take(&mut self.inner.borrow_mut().temp_table_names);
        if temp_names.is_empty() {
            return;
        }
        if let Ok(mut wtx) = self.db.begin_write() {
            for prefixed in &temp_names {
                let _ = wtx.drop_table(prefixed.as_bytes());
            }
            let _ = wtx.commit();
        }
    }
}
