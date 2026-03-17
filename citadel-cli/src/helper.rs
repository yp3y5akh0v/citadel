use rustyline::completion::{Completer, Pair};
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper};

use citadel_sql::Connection;

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
    "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "PRIMARY", "KEY",
    "NOT", "NULL", "INTEGER", "TEXT", "REAL", "BLOB", "BOOLEAN",
    "AND", "OR", "IN", "EXISTS", "BETWEEN", "LIKE", "IS",
    "ORDER", "BY", "ASC", "DESC", "LIMIT", "OFFSET",
    "GROUP", "HAVING", "DISTINCT", "AS", "ON", "JOIN",
    "INNER", "LEFT", "RIGHT", "CROSS", "OUTER",
    "BEGIN", "COMMIT", "ROLLBACK",
    "COUNT", "SUM", "AVG", "MIN", "MAX",
    "CASE", "WHEN", "THEN", "ELSE", "END",
    "COALESCE", "NULLIF", "CAST", "EXPLAIN",
    "TRUE", "FALSE", "IF", "UNIQUE",
];

const DOT_COMMANDS: &[&str] = &[
    ".help", ".quit", ".exit", ".tables", ".schema", ".indexes",
    ".mode", ".headers", ".nullvalue", ".timer", ".changes",
    ".stats", ".backup", ".compact", ".verify", ".audit",
    ".rekey", ".dump", ".read", ".open", ".output", ".width",
    ".sync", ".listen", ".nodeid",
];

pub struct CitadelHelper {
    table_names: Vec<String>,
    column_names: Vec<String>,
    hinter: HistoryHinter,
}

impl CitadelHelper {
    pub fn new(conn: &Connection<'_>) -> Self {
        let mut helper = Self {
            table_names: Vec::new(),
            column_names: Vec::new(),
            hinter: HistoryHinter {},
        };
        helper.update_schema(conn);
        helper
    }

    pub fn update_schema(&mut self, conn: &Connection<'_>) {
        self.table_names = conn.tables().into_iter().map(|s| s.to_string()).collect();
        self.table_names.sort();

        self.column_names.clear();
        for table_name in &self.table_names {
            if let Some(schema) = conn.table_schema(table_name) {
                for col in &schema.columns {
                    if !self.column_names.contains(&col.name) {
                        self.column_names.push(col.name.clone());
                    }
                }
            }
        }
        self.column_names.sort();
    }
}

impl Completer for CitadelHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let input = &line[..pos];

        if input.starts_with('.') {
            let lower = input.to_ascii_lowercase();
            let matches: Vec<Pair> = DOT_COMMANDS
                .iter()
                .filter(|cmd| cmd.starts_with(&lower))
                .map(|cmd| Pair {
                    display: cmd.to_string(),
                    replacement: cmd.to_string(),
                })
                .collect();
            return Ok((0, matches));
        }

        let word_start = input
            .rfind(|c: char| !c.is_alphanumeric() && c != '_')
            .map(|i| i + 1)
            .unwrap_or(0);
        let word = &input[word_start..];
        if word.is_empty() {
            return Ok((pos, Vec::new()));
        }

        let upper = word.to_ascii_uppercase();
        let lower = word.to_ascii_lowercase();

        let mut matches: Vec<Pair> = Vec::new();

        for kw in SQL_KEYWORDS {
            if kw.starts_with(&upper) {
                let replacement = if word.chars().next().map_or(false, |c| c.is_lowercase()) {
                    kw.to_ascii_lowercase()
                } else {
                    kw.to_string()
                };
                matches.push(Pair {
                    display: kw.to_string(),
                    replacement,
                });
            }
        }

        for t in &self.table_names {
            if t.to_ascii_lowercase().starts_with(&lower) {
                matches.push(Pair {
                    display: t.clone(),
                    replacement: t.clone(),
                });
            }
        }

        for c in &self.column_names {
            if c.to_ascii_lowercase().starts_with(&lower) {
                if !matches.iter().any(|m| m.replacement == *c) {
                    matches.push(Pair {
                        display: c.clone(),
                        replacement: c.clone(),
                    });
                }
            }
        }

        Ok((word_start, matches))
    }
}

impl Hinter for CitadelHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx)
    }
}

impl Highlighter for CitadelHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> std::borrow::Cow<'l, str> {
        use owo_colors::OwoColorize;

        if line.starts_with('.') {
            return std::borrow::Cow::Owned(format!("{}", line.yellow().bold()));
        }

        let mut result = String::with_capacity(line.len() + 64);
        let mut chars = line.char_indices().peekable();
        let mut last_end = 0;

        while let Some(&(i, ch)) = chars.peek() {
            if ch == '\'' {
                let start = i;
                chars.next();
                while let Some(&(_, c)) = chars.peek() {
                    chars.next();
                    if c == '\'' {
                        break;
                    }
                }
                let end = chars.peek().map(|&(idx, _)| idx).unwrap_or(line.len());
                result.push_str(&line[last_end..start]);
                let slice: &str = &line[start..end];
                result.push_str(&format!("{}", slice.green()));
                last_end = end;
            } else if ch.is_alphabetic() || ch == '_' {
                let start = i;
                chars.next();
                while let Some(&(_, c)) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' {
                        chars.next();
                    } else {
                        break;
                    }
                }
                let end = chars.peek().map(|&(idx, _)| idx).unwrap_or(line.len());
                let word = &line[start..end];
                let is_keyword = SQL_KEYWORDS
                    .iter()
                    .any(|kw| kw.eq_ignore_ascii_case(word));

                result.push_str(&line[last_end..start]);
                if is_keyword {
                    result.push_str(&format!("{}", word.blue().bold()));
                } else {
                    result.push_str(word);
                }
                last_end = end;
            } else if ch.is_ascii_digit() {
                let start = i;
                chars.next();
                while let Some(&(_, c)) = chars.peek() {
                    if c.is_ascii_digit() || c == '.' {
                        chars.next();
                    } else {
                        break;
                    }
                }
                let end = chars.peek().map(|&(idx, _)| idx).unwrap_or(line.len());
                result.push_str(&line[last_end..start]);
                let slice: &str = &line[start..end];
                result.push_str(&format!("{}", slice.cyan()));
                last_end = end;
            } else {
                chars.next();
            }
        }

        result.push_str(&line[last_end..]);
        std::borrow::Cow::Owned(result)
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: CmdKind) -> bool {
        true
    }
}

impl Validator for CitadelHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> rustyline::Result<ValidationResult> {
        let input = ctx.input();
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return Ok(ValidationResult::Valid(None));
        }

        if trimmed.starts_with('.') {
            return Ok(ValidationResult::Valid(None));
        }

        let mut in_single = false;
        let mut in_double = false;
        for ch in trimmed.chars() {
            match ch {
                '\'' if !in_double => in_single = !in_single,
                '"' if !in_single => in_double = !in_double,
                _ => {}
            }
        }

        if in_single || in_double || !trimmed.ends_with(';') {
            Ok(ValidationResult::Incomplete)
        } else {
            Ok(ValidationResult::Valid(None))
        }
    }
}

impl Helper for CitadelHelper {}
