//! Public SQL connection API.

use std::num::NonZeroUsize;

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

struct CacheEntry {
    stmt: Statement,
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
}

impl<'a> Connection<'a> {
    /// Open a SQL connection to a database.
    pub fn open(db: &'a Database) -> Result<Self> {
        let schema = SchemaManager::load(db)?;
        let stmt_cache = LruCache::new(NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap());
        Ok(Self { db, schema, active_txn: None, stmt_cache })
    }

    /// Execute a SQL statement. Returns the result.
    pub fn execute(&mut self, sql: &str) -> Result<ExecutionResult> {
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

        let bound = if param_count > 0 {
            parser::bind_params(&stmt, params)?
        } else {
            stmt
        };

        self.dispatch(bound)
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

    fn get_or_parse(&mut self, sql: &str) -> Result<(Statement, usize)> {
        let gen = self.schema.generation();

        if let Some(entry) = self.stmt_cache.get(sql) {
            if entry.schema_gen == gen {
                return Ok((entry.stmt.clone(), entry.param_count));
            }
        }

        let stmt = parser::parse_sql(sql)?;
        let param_count = parser::count_params(&stmt);

        let cacheable = !matches!(
            stmt,
            Statement::CreateTable(_) | Statement::DropTable(_)
            | Statement::CreateIndex(_) | Statement::DropIndex(_)
            | Statement::Begin | Statement::Commit | Statement::Rollback
        );

        if cacheable {
            self.stmt_cache.put(sql.to_string(), CacheEntry {
                stmt: stmt.clone(),
                schema_gen: gen,
                param_count,
            });
        }

        Ok((stmt, param_count))
    }

    fn dispatch(&mut self, stmt: Statement) -> Result<ExecutionResult> {
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
                let wtx = self.active_txn.take()
                    .ok_or(SqlError::NoActiveTransaction)?;
                wtx.commit().map_err(SqlError::Storage)?;
                Ok(ExecutionResult::Ok)
            }
            Statement::Rollback => {
                let wtx = self.active_txn.take()
                    .ok_or(SqlError::NoActiveTransaction)?;
                wtx.abort();
                self.schema = SchemaManager::load(self.db)?;
                Ok(ExecutionResult::Ok)
            }
            _ => {
                if let Some(ref mut wtx) = self.active_txn {
                    executor::execute_in_txn(wtx, &mut self.schema, &stmt)
                } else {
                    executor::execute(self.db, &mut self.schema, &stmt)
                }
            }
        }
    }
}
