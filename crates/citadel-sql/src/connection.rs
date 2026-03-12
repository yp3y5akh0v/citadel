//! Public SQL connection API.

use citadel::Database;

use crate::error::Result;
use crate::executor;
use crate::parser;
use crate::schema::SchemaManager;
use crate::types::{ExecutionResult, QueryResult};

/// A SQL connection wrapping a Citadel database.
pub struct Connection<'a> {
    db: &'a Database,
    schema: SchemaManager,
}

impl<'a> Connection<'a> {
    /// Open a SQL connection to a database.
    pub fn open(db: &'a Database) -> Result<Self> {
        let schema = SchemaManager::load(db)?;
        Ok(Self { db, schema })
    }

    /// Execute a SQL statement. Returns the result.
    pub fn execute(&mut self, sql: &str) -> Result<ExecutionResult> {
        let stmt = parser::parse_sql(sql)?;
        executor::execute(self.db, &mut self.schema, &stmt)
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
}
