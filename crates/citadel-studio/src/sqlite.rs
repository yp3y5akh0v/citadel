//! Reads untrusted, plaintext SQLite import sources. Nothing returned is attested.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SourceTable {
    pub name: String,
    pub rows: u64,
    pub columns: Vec<Column>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Column {
    pub name: String,
    /// Declared schema type, which SQLite does not enforce on stored values.
    pub declared: String,
    /// Whether the column belongs to the primary key required by CitadelDB.
    pub primary_key: bool,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReadError {
    NotReadable(String),
    Schema(String),
}

impl ReadError {
    pub fn message(&self) -> &str {
        match self {
            Self::NotReadable(m) | Self::Schema(m) => m,
        }
    }
}

/// Read every user table in `path`. Blocking: each `COUNT(*)` may scan a table.
pub fn read(path: &Path) -> Result<Vec<SourceTable>, ReadError> {
    read_cancellable(path, Arc::new(AtomicBool::new(false)))
}

/// Read a source with cancellation through SQLite's progress handler.
pub fn read_cancellable(
    path: &Path,
    cancelled: Arc<AtomicBool>,
) -> Result<Vec<SourceTable>, ReadError> {
    // Read-only, and never created if missing: this is someone else's database.
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI;
    let db = rusqlite::Connection::open_with_flags(path, flags)
        .map_err(|e| ReadError::NotReadable(e.to_string()))?;
    db.progress_handler(1_000, Some(move || cancelled.load(Ordering::Acquire)))
        .map_err(|e| ReadError::Schema(e.to_string()))?;

    // `sqlite_%` is SQLite's bookkeeping; a view has no rows of its own to import.
    let mut stmt = db
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )
        .map_err(|e| ReadError::Schema(e.to_string()))?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .map_err(|e| ReadError::Schema(e.to_string()))?
        .collect::<Result<_, _>>()
        .map_err(|e| ReadError::Schema(e.to_string()))?;

    names
        .into_iter()
        .map(|name| {
            let columns = columns_of(&db, &name)?;
            let rows = count_of(&db, &name)?;
            Ok(SourceTable {
                name,
                rows,
                columns,
            })
        })
        .collect()
}

fn columns_of(db: &rusqlite::Connection, table: &str) -> Result<Vec<Column>, ReadError> {
    let mut stmt = db
        .prepare("SELECT name, type, pk FROM pragma_table_info(?1)")
        .map_err(|e| ReadError::Schema(e.to_string()))?;
    let columns = stmt
        .query_map([table], |row| {
            Ok(Column {
                name: row.get(0)?,
                declared: row.get(1)?,
                primary_key: row.get::<_, i64>(2)? > 0,
            })
        })
        .map_err(|e| ReadError::Schema(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ReadError::Schema(e.to_string()))?;
    Ok(columns)
}

/// Quoted by doubling, since a table name cannot be bound as a parameter and
/// `"; DROP TABLE x; --` is a legal one.
fn count_of(db: &rusqlite::Connection, table: &str) -> Result<u64, ReadError> {
    let quoted = table.replace('"', "\"\"");
    db.query_row(&format!("SELECT COUNT(*) FROM \"{quoted}\""), [], |row| {
        row.get::<_, i64>(0)
    })
    .map(|n| n.max(0) as u64)
    .map_err(|e| ReadError::Schema(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str, build: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("create fixture directory");
        let path = dir.path().join(format!("{name}.sqlite"));
        let db = rusqlite::Connection::open(&path).expect("create fixture");
        db.execute_batch(build).expect("build fixture");
        (dir, path)
    }

    #[test]
    fn reads_tables_rows_and_columns() {
        let (_dir, path) = fixture(
            "basic",
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, ts INTEGER);
             INSERT INTO notes (body, ts) VALUES ('a', 1), ('b', 2), ('c', 3);
             CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO tags (name) VALUES ('x');
             CREATE VIEW recent AS SELECT * FROM notes;
             CREATE INDEX notes_ts ON notes (ts);",
        );

        let tables = read(&path).expect("readable");
        assert_eq!(
            tables.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            ["notes", "tags"]
        );
        assert_eq!(tables[0].rows, 3);
        assert_eq!(tables[1].rows, 1);
        assert_eq!(
            tables[0]
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.declared.as_str()))
                .collect::<Vec<_>>(),
            [("id", "INTEGER"), ("body", "TEXT"), ("ts", "INTEGER")]
        );
    }

    #[test]
    fn skips_sqlites_own_tables() {
        let (_dir, path) = fixture(
            "internal",
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT);
             INSERT INTO t DEFAULT VALUES;",
        );
        let tables = read(&path).expect("readable");
        assert_eq!(
            tables.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            ["t"],
            "sqlite_sequence was reported as a table to import"
        );
    }

    #[test]
    fn a_hostile_table_name_is_quoted_not_executed() {
        let (_dir, path) = fixture(
            "hostile",
            "CREATE TABLE \"a\"\"; DROP TABLE keep; --\" (x INTEGER);
             INSERT INTO \"a\"\"; DROP TABLE keep; --\" VALUES (1), (2);
             CREATE TABLE keep (y INTEGER);",
        );
        let tables = read(&path).expect("readable");
        assert!(
            tables.iter().any(|t| t.name == "keep"),
            "the hostile name dropped a table while being counted"
        );
        let hostile = tables
            .iter()
            .find(|t| t.name.contains("DROP"))
            .expect("the hostile table is reported like any other");
        assert_eq!(hostile.rows, 2);
    }

    #[test]
    fn an_empty_database_reads_as_empty_not_as_an_error() {
        let (_dir, path) = fixture("empty", "");
        assert_eq!(read(&path), Ok(Vec::new()));
    }

    #[test]
    fn a_file_that_is_not_a_database_reports_why() {
        let dir = tempfile::tempdir().expect("create fixture directory");
        let path = dir.path().join("not-a-db.sqlite");
        std::fs::write(&path, b"this is not a database").expect("write");
        let err = read(&path).expect_err("a text file is not a database");
        assert!(
            !err.message().is_empty(),
            "the failure carried no message to show"
        );
    }

    #[test]
    fn a_missing_file_is_not_created() {
        let dir = tempfile::tempdir().expect("create fixture directory");
        let path = dir.path().join("absent.sqlite");
        assert!(read(&path).is_err());
        assert!(
            !path.exists(),
            "reading a missing file created it, which would write to someone else's disk"
        );
    }

    #[test]
    fn a_cancelled_source_read_is_interrupted_by_sqlite() {
        let schema = (0..256)
            .map(|i| format!("CREATE TABLE t{i} (id INTEGER PRIMARY KEY);"))
            .collect::<String>();
        let (_dir, path) = fixture("cancelled", &schema);
        let cancelled = Arc::new(AtomicBool::new(true));
        let error = read_cancellable(&path, cancelled).expect_err("the read is cancelled");
        assert!(
            error.message().to_ascii_lowercase().contains("interrupt"),
            "SQLite did not report its interrupted operation: {error:?}"
        );
    }
}
