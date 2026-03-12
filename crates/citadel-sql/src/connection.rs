//! Public SQL connection API.

use citadel::Database;
use citadel_txn::write_txn::WriteTxn;

use crate::error::{Result, SqlError};
use crate::executor;
use crate::parser;
use crate::parser::Statement;
use crate::schema::SchemaManager;
use crate::types::{ExecutionResult, QueryResult};

/// A SQL connection wrapping a Citadel database.
///
/// Supports explicit transactions via BEGIN / COMMIT / ROLLBACK.
/// Without BEGIN, each statement runs in auto-commit mode.
pub struct Connection<'a> {
    db: &'a Database,
    schema: SchemaManager,
    active_txn: Option<WriteTxn<'a>>,
}

impl<'a> Connection<'a> {
    /// Open a SQL connection to a database.
    pub fn open(db: &'a Database) -> Result<Self> {
        let schema = SchemaManager::load(db)?;
        Ok(Self { db, schema, active_txn: None })
    }

    /// Execute a SQL statement. Returns the result.
    pub fn execute(&mut self, sql: &str) -> Result<ExecutionResult> {
        let stmt = parser::parse_sql(sql)?;

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
                // Reload schema to discard any uncommitted DDL changes
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

    /// Execute a SQL query and return the result set.
    /// Convenience method that extracts QueryResult from ExecutionResult.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        match self.execute(sql)? {
            ExecutionResult::Query(qr) => Ok(qr),
            ExecutionResult::RowsAffected(n) => Ok(QueryResult {
                columns: vec!["rows_affected".into()],
                rows: vec![vec![crate::types::Value::Integer(n as i64)]],
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
}
