use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::types::{ExecutionResult, Value};
use citadel_sql::Connection;
use self_cell::self_cell;

self_cell!(
    struct DbCell {
        owner: Database,
        #[not_covariant]
        dependent: Connection,
    }
);

/// In-memory encrypted Citadel database. Data is lost on drop.
///
/// Holds a single long-lived `Connection` so BEGIN/SAVEPOINT/COMMIT
/// state persists across `execute`/`query` calls.
pub struct CitadelDb {
    cell: DbCell,
}

impl CitadelDb {
    /// Create a new in-memory encrypted database.
    pub fn create(passphrase: &str) -> Result<Self, String> {
        #[cfg(target_arch = "wasm32")]
        {
            static HOOK: std::sync::Once = std::sync::Once::new();
            HOOK.call_once(|| console_error_panic_hook::set_once());
        }
        let db = DatabaseBuilder::new("")
            .passphrase(passphrase.as_bytes())
            .argon2_profile(Argon2Profile::Iot)
            .create_in_memory()
            .map_err(|e| format!("{e}"))?;
        let cell = DbCell::try_new(db, |db| Connection::open(db).map_err(|e| format!("{e}")))?;
        Ok(Self { cell })
    }

    fn db(&self) -> &Database {
        self.cell.borrow_owner()
    }

    /// Execute a DDL/DML statement. Returns rows affected (0 for DDL).
    pub fn execute(&self, sql: &str) -> Result<u64, String> {
        self.cell.with_dependent(
            |_, conn| match conn.execute(sql).map_err(|e| format!("{e}"))? {
                ExecutionResult::RowsAffected(n) => Ok(n),
                ExecutionResult::Query(_) => Ok(0),
                ExecutionResult::Ok => Ok(0),
            },
        )
    }

    /// Execute a SQL query and return results as structured data.
    pub fn query(&self, sql: &str) -> Result<QueryResultData, String> {
        self.cell.with_dependent(|_, conn| {
            let qr = conn.query(sql).map_err(|e| format!("{e}"))?;
            let rows: Vec<Vec<CellValue>> = qr
                .rows
                .iter()
                .map(|row| row.iter().map(CellValue::from_value).collect())
                .collect();
            Ok(QueryResultData {
                columns: qr.columns,
                rows,
            })
        })
    }

    /// Execute multiple SQL statements separated by semicolons.
    pub fn execute_batch(&self, sql: &str) -> Result<(), String> {
        self.cell.with_dependent(|_, conn| {
            for stmt in sql.split(';') {
                let trimmed = stmt.trim();
                if trimmed.is_empty() {
                    continue;
                }
                conn.execute(trimmed).map_err(|e| format!("{e}"))?;
            }
            Ok(())
        })
    }

    /// Execute `;`-separated SQL statements. Stops at the first failure.
    pub fn execute_script(&self, sql: &str) -> Vec<ScriptOutcome> {
        self.cell.with_dependent(|_, conn| {
            let exec = conn.execute_script(sql);
            let mut outcomes: Vec<ScriptOutcome> = exec
                .completed
                .into_iter()
                .map(|r| match r {
                    ExecutionResult::RowsAffected(n) => ScriptOutcome::Rows(n),
                    ExecutionResult::Query(qr) => ScriptOutcome::Query(QueryResultData {
                        columns: qr.columns,
                        rows: qr
                            .rows
                            .iter()
                            .map(|row| row.iter().map(CellValue::from_value).collect())
                            .collect(),
                    }),
                    ExecutionResult::Ok => ScriptOutcome::Ok,
                })
                .collect();
            if let Some(e) = exec.error {
                outcomes.push(ScriptOutcome::Error(format!("{e}")));
            }
            outcomes
        })
    }

    /// Put a key-value pair into the default table.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        let mut wtx = self.db().begin_write().map_err(|e| format!("{e}"))?;
        wtx.insert(key, value).map_err(|e| format!("{e}"))?;
        wtx.commit().map_err(|e| format!("{e}"))?;
        Ok(())
    }

    /// Get a value by key from the default table.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let mut rtx = self.db().begin_read();
        rtx.get(key).map_err(|e| format!("{e}"))
    }

    /// Delete a key from the default table. Returns true if it existed.
    pub fn delete(&self, key: &[u8]) -> Result<bool, String> {
        let mut wtx = self.db().begin_write().map_err(|e| format!("{e}"))?;
        let existed = wtx.delete(key).map_err(|e| format!("{e}"))?;
        wtx.commit().map_err(|e| format!("{e}"))?;
        Ok(existed)
    }

    /// Put a key-value pair into a named table.
    pub fn table_put(&self, table: &str, key: &[u8], value: &[u8]) -> Result<(), String> {
        let mut wtx = self.db().begin_write().map_err(|e| format!("{e}"))?;
        wtx.table_insert(table.as_bytes(), key, value)
            .map_err(|e| format!("{e}"))?;
        wtx.commit().map_err(|e| format!("{e}"))?;
        Ok(())
    }

    /// Get a value by key from a named table.
    pub fn table_get(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let mut rtx = self.db().begin_read();
        rtx.table_get(table.as_bytes(), key)
            .map_err(|e| format!("{e}"))
    }

    /// Delete a key from a named table.
    pub fn table_delete(&self, table: &str, key: &[u8]) -> Result<bool, String> {
        let mut wtx = self.db().begin_write().map_err(|e| format!("{e}"))?;
        let existed = wtx
            .table_delete(table.as_bytes(), key)
            .map_err(|e| format!("{e}"))?;
        wtx.commit().map_err(|e| format!("{e}"))?;
        Ok(existed)
    }

    /// Get database statistics.
    pub fn stats(&self) -> StatsData {
        let s = self.db().stats();
        StatsData {
            entry_count: s.entry_count,
            total_pages: s.total_pages,
            tree_depth: s.tree_depth,
        }
    }
}

/// A single cell value in a query result.
#[derive(Debug, Clone)]
pub enum CellValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
    Date {
        days: i32,
        iso: String,
    },
    Time {
        micros: i64,
        iso: String,
    },
    Timestamp {
        micros: i64,
        iso: String,
    },
    Interval {
        months: i32,
        days: i32,
        micros: i64,
        iso: String,
    },
}

impl CellValue {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Null => CellValue::Null,
            Value::Integer(i) => CellValue::Integer(*i),
            Value::Real(r) => CellValue::Real(*r),
            Value::Text(s) => CellValue::Text(s.to_string()),
            Value::Blob(b) => CellValue::Blob(b.clone()),
            Value::Boolean(b) => CellValue::Boolean(*b),
            Value::Date(d) => CellValue::Date {
                days: *d,
                iso: citadel_sql::datetime::format_date(*d),
            },
            Value::Time(t) => CellValue::Time {
                micros: *t,
                iso: citadel_sql::datetime::format_time(*t),
            },
            Value::Timestamp(t) => CellValue::Timestamp {
                micros: *t,
                iso: citadel_sql::datetime::format_timestamp(*t),
            },
            Value::Interval {
                months,
                days,
                micros,
            } => CellValue::Interval {
                months: *months,
                days: *days,
                micros: *micros,
                iso: citadel_sql::datetime::format_interval(*months, *days, *micros),
            },
        }
    }
}

/// Query result containing columns and rows.
#[derive(Debug)]
pub struct QueryResultData {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<CellValue>>,
}

#[derive(Debug)]
pub enum ScriptOutcome {
    Rows(u64),
    Query(QueryResultData),
    Ok,
    Error(String),
}

/// Database statistics.
#[derive(Debug, Clone)]
pub struct StatsData {
    pub entry_count: u64,
    pub total_pages: u32,
    pub tree_depth: u16,
}

#[cfg(target_arch = "wasm32")]
mod wasm_api;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
