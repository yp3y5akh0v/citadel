//! Public SQL connection API.

use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;

use citadel::Database;
use citadel_txn::write_txn::WriteTxn;

use crate::error::{Result, SqlError};
use crate::executor;
use crate::parser;
use crate::parser::Statement;
use crate::schema::SchemaManager;
use crate::types::{ExecutionResult, QueryResult, TableSchema, Value};

const DEFAULT_CACHE_CAPACITY: usize = 64;

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

struct CacheEntry {
    stmt: Arc<Statement>,
    schema_gen: u64,
    param_count: usize,
}

/// A SQL connection wrapping a Citadel database.
///
/// Supports explicit transactions via BEGIN / COMMIT / ROLLBACK.
/// Without BEGIN, each statement runs in auto-commit mode.
///
/// Caches parsed SQL statements in an LRU cache keyed by SQL string.
/// Cache entries are invalidated when the schema changes (DDL operations).
pub struct Connection<'a> {
    db: &'a Database,
    schema: SchemaManager,
    active_txn: Option<WriteTxn<'a>>,
    stmt_cache: LruCache<String, CacheEntry>,
    insert_bufs: executor::InsertBufs,
}

impl<'a> Connection<'a> {
    /// Open a SQL connection to a database.
    pub fn open(db: &'a Database) -> Result<Self> {
        let schema = SchemaManager::load(db)?;
        let stmt_cache = LruCache::new(NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap());
        Ok(Self {
            db,
            schema,
            active_txn: None,
            stmt_cache,
            insert_bufs: executor::InsertBufs::new(),
        })
    }

    /// Execute a SQL statement. Returns the result.
    pub fn execute(&mut self, sql: &str) -> Result<ExecutionResult> {
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
            return self.dispatch(&stmt, &extracted);
        }

        self.execute_params(sql, &[])
    }

    /// Execute a SQL statement with positional parameters ($1, $2, ...).
    pub fn execute_params(&mut self, sql: &str, params: &[Value]) -> Result<ExecutionResult> {
        let (stmt, param_count) = self.get_or_parse(sql)?;

        if param_count != params.len() {
            return Err(SqlError::ParameterCountMismatch {
                expected: param_count,
                got: params.len(),
            });
        }

        if param_count > 0 && matches!(*stmt, Statement::Insert(_)) {
            self.dispatch(&stmt, params)
        } else if param_count > 0 {
            let bound = parser::bind_params(&stmt, params)?;
            self.dispatch(&bound, &[])
        } else {
            self.dispatch(&stmt, &[])
        }
    }

    /// Execute a SQL query and return the result set.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.query_params(sql, &[])
    }

    /// Execute a SQL query with positional parameters ($1, $2, ...).
    pub fn query_params(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult> {
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

    /// List all table names.
    pub fn tables(&self) -> Vec<&str> {
        self.schema.table_names()
    }

    /// Returns true if an explicit transaction is active (BEGIN was issued).
    pub fn in_transaction(&self) -> bool {
        self.active_txn.is_some()
    }

    /// Get the schema for a named table.
    pub fn table_schema(&self, name: &str) -> Option<&TableSchema> {
        self.schema.get(name)
    }

    /// Reload schemas from the database.
    pub fn refresh_schema(&mut self) -> Result<()> {
        self.schema = SchemaManager::load(self.db)?;
        Ok(())
    }

    fn parse_and_cache(&mut self, normalized_key: String, gen: u64) -> Result<Arc<Statement>> {
        let stmt = Arc::new(parser::parse_sql(&normalized_key)?);
        let param_count = parser::count_params(&stmt);
        self.stmt_cache.put(
            normalized_key,
            CacheEntry {
                stmt: Arc::clone(&stmt),
                schema_gen: gen,
                param_count,
            },
        );
        Ok(stmt)
    }

    fn get_or_parse(&mut self, sql: &str) -> Result<(Arc<Statement>, usize)> {
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
                | Statement::Begin
                | Statement::Commit
                | Statement::Rollback
        );

        if cacheable {
            self.stmt_cache.put(
                sql.to_string(),
                CacheEntry {
                    stmt: Arc::clone(&stmt),
                    schema_gen: gen,
                    param_count,
                },
            );
        }

        Ok((stmt, param_count))
    }

    fn dispatch(&mut self, stmt: &Statement, params: &[Value]) -> Result<ExecutionResult> {
        match stmt {
            Statement::Begin => {
                if self.active_txn.is_some() {
                    return Err(SqlError::TransactionAlreadyActive);
                }
                let wtx = self.db.begin_write().map_err(SqlError::Storage)?;
                self.active_txn = Some(wtx);
                Ok(ExecutionResult::Ok)
            }
            Statement::Commit => {
                let wtx = self
                    .active_txn
                    .take()
                    .ok_or(SqlError::NoActiveTransaction)?;
                wtx.commit().map_err(SqlError::Storage)?;
                Ok(ExecutionResult::Ok)
            }
            Statement::Rollback => {
                let wtx = self
                    .active_txn
                    .take()
                    .ok_or(SqlError::NoActiveTransaction)?;
                wtx.abort();
                self.schema = SchemaManager::load(self.db)?;
                Ok(ExecutionResult::Ok)
            }
            Statement::Insert(ins) if self.active_txn.is_some() => {
                let wtx = self.active_txn.as_mut().unwrap();
                executor::exec_insert_in_txn(wtx, &self.schema, ins, params, &mut self.insert_bufs)
            }
            _ => {
                if let Some(ref mut wtx) = self.active_txn {
                    executor::execute_in_txn(wtx, &mut self.schema, stmt, params)
                } else {
                    executor::execute(self.db, &mut self.schema, stmt, params)
                }
            }
        }
    }
}
