//! Converts SQLite schemas into CitadelDB statements without executing them.

use crate::sqlite::SourceTable;

/// How a SQLite declared type maps to CitadelDB, including approximate mappings.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mapping {
    Exact(&'static str),
    Approximate {
        citadel: &'static str,
        why: &'static str,
    },
}

impl Mapping {
    pub fn citadel(self) -> &'static str {
        match self {
            Self::Exact(t) => t,
            Self::Approximate { citadel, .. } => citadel,
        }
    }

    pub fn is_exact(self) -> bool {
        matches!(self, Self::Exact(_))
    }
}

/// SQLite's "Determination Of Column Affinity", substring-matched in SQLite's own order.
/// Order matters: `INTCHAR` matches rule 1 before rule 2 and is INTEGER.
pub fn affinity(declared: &str) -> Mapping {
    let d = declared.to_ascii_uppercase();
    if d.contains("INT") {
        Mapping::Exact("INTEGER")
    } else if d.contains("CHAR") || d.contains("CLOB") || d.contains("TEXT") {
        Mapping::Exact("TEXT")
    } else if d.contains("BLOB") || d.is_empty() {
        Mapping::Exact("BLOB")
    } else if d.contains("REAL") || d.contains("FLOA") || d.contains("DOUB") {
        Mapping::Exact("REAL")
    } else {
        // NUMERIC has no exact CitadelDB equivalent: it preserves integers and floats.
        Mapping::Approximate {
            citadel: "REAL",
            why: "SQLite NUMERIC stores integers exactly and other values as floats",
        }
    }
}

/// Quote an identifier without permitting SQL injection.
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Render source metadata inside a SQL line comment. SQLite permits CR and LF in quoted
/// identifiers, so copying either byte into a `--` comment would end the comment and turn
/// the rest of a hostile name into executable SQL.
fn comment_text(value: &str) -> String {
    value.escape_debug().to_string()
}

/// Generate `CREATE TABLE` statements in source order without inventing constraints.
pub fn statements(tables: &[SourceTable]) -> String {
    let mut out = String::new();
    out.push_str("-- Generated from the source schema. Column types and the primary key:\n");
    out.push_str("-- defaults, other constraints and indexes are not translated.\n");
    for table in tables {
        out.push('\n');
        out.push_str(&format!("CREATE TABLE {} (\n", quote(&table.name)));
        let key: Vec<&str> = table
            .columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.as_str())
            .collect();
        for (i, column) in table.columns.iter().enumerate() {
            let mapping = affinity(&column.declared);
            let comma = if i + 1 == table.columns.len() && key.is_empty() {
                ""
            } else {
                ","
            };
            out.push_str(&format!(
                "    {} {}{}",
                quote(&column.name),
                mapping.citadel(),
                comma
            ));
            if let Mapping::Approximate { why, .. } = mapping {
                out.push_str(&format!(
                    "  -- {} declared {}: {why}",
                    comment_text(&column.name),
                    comment_text(&column.declared)
                ));
            }
            out.push('\n');
        }
        // Do not invent a key for a source table that lacks one.
        if !key.is_empty() {
            let named: Vec<String> = key.iter().map(|c| quote(c)).collect();
            out.push_str(&format!("    PRIMARY KEY ({})\n", named.join(", ")));
        }
        out.push_str(");\n");
    }
    out
}

/// How many columns did not map exactly, so the screen can say so before anyone runs it.
pub fn approximate_columns(tables: &[SourceTable]) -> usize {
    tables
        .iter()
        .flat_map(|t| &t.columns)
        .filter(|c| !affinity(&c.declared).is_exact())
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::Column;

    fn table(name: &str, columns: &[(&str, &str)]) -> SourceTable {
        SourceTable {
            name: name.to_owned(),
            rows: 0,
            columns: columns
                .iter()
                .map(|(n, d)| Column {
                    name: (*n).to_owned(),
                    declared: (*d).to_owned(),
                    primary_key: false,
                })
                .collect(),
        }
    }

    #[test]
    fn affinity_follows_sqlites_own_rules() {
        for (declared, want) in [
            ("INTEGER", "INTEGER"),
            ("INT", "INTEGER"),
            ("BIGINT", "INTEGER"),
            ("UNSIGNED BIG INT", "INTEGER"),
            ("VARCHAR(255)", "TEXT"),
            ("NVARCHAR(10)", "TEXT"),
            ("CLOB", "TEXT"),
            ("TEXT", "TEXT"),
            ("BLOB", "BLOB"),
            ("", "BLOB"),
            ("REAL", "REAL"),
            ("DOUBLE PRECISION", "REAL"),
            ("FLOAT", "REAL"),
        ] {
            assert_eq!(
                affinity(declared).citadel(),
                want,
                "{declared:?} took the wrong affinity"
            );
            assert!(affinity(declared).is_exact(), "{declared:?} is not exact");
        }

        assert_eq!(affinity("INTCHAR").citadel(), "INTEGER");

        for declared in ["NUMERIC", "DECIMAL(10,5)", "BOOLEAN", "DATETIME"] {
            assert!(
                !affinity(declared).is_exact(),
                "{declared:?} was reported as an exact mapping"
            );
        }
    }

    #[test]
    fn generates_statements_for_every_table() {
        let sql = statements(&[
            table("notes", &[("id", "INTEGER"), ("body", "TEXT")]),
            table("tags", &[("name", "VARCHAR(40)")]),
        ]);
        assert!(sql.contains("CREATE TABLE \"notes\" ("));
        assert!(sql.contains("\"id\" INTEGER,"));
        assert!(sql.contains("\"body\" TEXT\n"));
        assert!(sql.contains("CREATE TABLE \"tags\" ("));
        assert!(sql.contains("\"name\" TEXT\n"));
        assert!(!sql.contains("TEXT,\n)"));
    }

    #[test]
    fn identifiers_are_quoted() {
        let sql = statements(&[table("a\"; DROP TABLE keep; --", &[("x\"y", "INT")])]);
        assert!(
            sql.contains("CREATE TABLE \"a\"\"; DROP TABLE keep; --\""),
            "the table name was not quoted: {sql}"
        );
        assert!(
            sql.contains("\"x\"\"y\" INTEGER"),
            "the column name was not quoted: {sql}"
        );
    }

    #[test]
    fn line_breaks_in_source_metadata_cannot_escape_the_warning_comment() {
        let hostile = "amount\n); DROP TABLE keep; --";
        let declared = "NUMERIC\r\n); DROP TABLE keep; --";
        let mut source = table("incoming", &[("id", "INTEGER"), (hostile, declared)]);
        source.columns[0].primary_key = true;

        let sql = statements(&[source]);
        assert!(
            sql.contains(
                "-- amount\\n); DROP TABLE keep; -- declared \
                          NUMERIC\\r\\n); DROP TABLE keep; --"
            ),
            "source line endings were not escaped in the comment: {sql}"
        );

        let vault = crate::engine::session::DemoVault::new();
        assert!(vault
            .run("CREATE TABLE keep (id INTEGER PRIMARY KEY);")
            .failed
            .is_none());
        let run = vault.run_atomic(&sql);
        assert!(run.is_ok(), "the escaped schema should execute: {run:?}");
        assert!(
            vault.tables().iter().any(|name| name == "keep"),
            "source metadata escaped its comment and executed DROP TABLE"
        );
    }

    #[test]
    fn an_approximate_column_says_so_in_the_sql() {
        let tables = [table("t", &[("price", "DECIMAL(10,2)"), ("id", "INTEGER")])];
        assert_eq!(approximate_columns(&tables), 1);
        let sql = statements(&tables);
        assert!(
            sql.contains("-- price declared DECIMAL(10,2)"),
            "the approximate column was translated silently: {sql}"
        );
        assert!(!sql.contains("-- id declared"));
    }

    #[test]
    fn no_tables_still_produces_a_readable_header() {
        let sql = statements(&[]);
        assert!(sql.starts_with("-- Generated"));
        assert!(!sql.contains("CREATE TABLE"));
    }
}
