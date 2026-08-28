use std::fs;
use std::io::Write;
use std::net::TcpListener;
use std::path::Path;
use std::time::Instant;

use citadel::Database;
use citadel_sql::Connection;

use crate::formatter::{self, OutputMode};
use crate::repl::Settings;

struct DotCommand {
    name: &'static str,
    args: &'static str,
    description: &'static str,
}

const DOT_COMMANDS: &[DotCommand] = &[
    DotCommand {
        name: ".help",
        args: "[CMD]",
        description: "Show help for dot-commands",
    },
    DotCommand {
        name: ".quit",
        args: "",
        description: "Exit the shell",
    },
    DotCommand {
        name: ".exit",
        args: "",
        description: "Exit the shell",
    },
    DotCommand {
        name: ".tables",
        args: "",
        description: "List all tables",
    },
    DotCommand {
        name: ".schema",
        args: "[TABLE]",
        description: "Show CREATE TABLE statement",
    },
    DotCommand {
        name: ".indexes",
        args: "[TABLE]",
        description: "Show indexes",
    },
    DotCommand {
        name: ".mode",
        args: "MODE",
        description: "Set output mode (box/table/csv/json/line)",
    },
    DotCommand {
        name: ".headers",
        args: "on|off",
        description: "Toggle column headers",
    },
    DotCommand {
        name: ".nullvalue",
        args: "STRING",
        description: "Set NULL display string",
    },
    DotCommand {
        name: ".timer",
        args: "on|off",
        description: "Toggle query timing",
    },
    DotCommand {
        name: ".changes",
        args: "on|off",
        description: "Toggle 'N row(s) affected' display",
    },
    DotCommand {
        name: ".stats",
        args: "",
        description: "Show database statistics",
    },
    DotCommand {
        name: ".backup",
        args: "PATH",
        description: "Create a hot backup",
    },
    DotCommand {
        name: ".compact",
        args: "PATH",
        description: "Compact database to a new file",
    },
    DotCommand {
        name: ".verify",
        args: "",
        description: "Run integrity check",
    },
    DotCommand {
        name: ".upgrade",
        args: "",
        description: "Upgrade the file to the authenticated slot format (one-way)",
    },
    DotCommand {
        name: ".audit",
        args: "[verify]",
        description: "Show or verify audit log",
    },
    DotCommand {
        name: ".rekey",
        args: "",
        description: "Change database passphrase",
    },
    DotCommand {
        name: ".dump",
        args: "[TABLE]",
        description: "Dump CREATE + INSERT statements",
    },
    DotCommand {
        name: ".read",
        args: "FILE",
        description: "Execute SQL from a file",
    },
    DotCommand {
        name: ".open",
        args: "PATH",
        description: "Open a different database",
    },
    DotCommand {
        name: ".output",
        args: "[FILE]",
        description: "Redirect output to file (no arg = stdout)",
    },
    DotCommand {
        name: ".width",
        args: "N...",
        description: "Set column widths for box/table mode",
    },
    DotCommand {
        name: ".sync",
        args: "HOST:PORT KEY",
        description: "Push tables to a remote peer",
    },
    DotCommand {
        name: ".listen",
        args: "[PORT] KEY",
        description: "Listen for one incoming sync",
    },
    DotCommand {
        name: ".keygen",
        args: "",
        description: "Generate a sync key",
    },
    DotCommand {
        name: ".nodeid",
        args: "",
        description: "Show this database's node ID",
    },
];

pub enum Action {
    Continue,
    Failed,
    Quit,
    QuitFailed,
    Reopen(String),
}

fn command_action(success: bool) -> Action {
    if success {
        Action::Continue
    } else {
        Action::Failed
    }
}

pub fn execute_dot_command(
    input: &str,
    db: &Database,
    conn: &Connection<'_>,
    settings: &mut Settings,
    out: &mut dyn Write,
) -> Action {
    let parts: Vec<&str> = input.split_whitespace().collect();
    let cmd = parts
        .first()
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    let args: Vec<&str> = parts[1..].to_vec();

    match cmd.as_str() {
        ".help" => command_action(cmd_help(&args, out)),
        ".quit" | ".exit" => Action::Quit,
        ".tables" => {
            cmd_tables(conn, out);
            Action::Continue
        }
        ".schema" => command_action(cmd_schema(&args, conn, out)),
        ".indexes" => command_action(cmd_indexes(&args, conn, out)),
        ".mode" => command_action(cmd_mode(&args, settings, out)),
        ".headers" => command_action(cmd_headers(&args, settings, out)),
        ".nullvalue" => {
            cmd_nullvalue(&args, settings, out);
            Action::Continue
        }
        ".timer" => command_action(cmd_timer(&args, settings, out)),
        ".changes" => command_action(cmd_changes(&args, settings, out)),
        ".stats" => {
            cmd_stats(db, out);
            Action::Continue
        }
        ".backup" => command_action(cmd_backup(&args, db, out)),
        ".compact" => command_action(cmd_compact(&args, db, out)),
        ".verify" => {
            if cmd_verify(db, out) {
                Action::Continue
            } else {
                Action::Failed
            }
        }
        ".upgrade" => command_action(cmd_upgrade(db, out)),
        ".audit" => {
            if cmd_audit(&args, db, out) {
                Action::Continue
            } else {
                Action::Failed
            }
        }
        ".rekey" => command_action(cmd_rekey(db, out)),
        ".dump" => command_action(cmd_dump(&args, conn, settings, out)),
        ".read" => cmd_read(&args, db, conn, settings, out),
        ".open" => {
            if args.is_empty() {
                let _ = writeln!(out, "Usage: .open PATH");
                return Action::Failed;
            }
            if conn.in_transaction() {
                let _ = writeln!(out, "Error: COMMIT or ROLLBACK first");
                return Action::Failed;
            }
            Action::Reopen(args[0].to_string())
        }
        ".output" => command_action(cmd_output(&args, settings, out)),
        ".width" => command_action(cmd_width(&args, settings, out)),
        ".sync" => command_action(cmd_sync(&args, db, conn, out)),
        ".listen" => command_action(cmd_listen(&args, db, conn, out)),
        ".keygen" => {
            cmd_keygen(out);
            Action::Continue
        }
        ".nodeid" => command_action(cmd_nodeid(db, out)),
        _ => {
            let _ = writeln!(
                out,
                "Unknown command: {cmd}. Use .help for available commands."
            );
            Action::Failed
        }
    }
}

fn cmd_help(args: &[&str], out: &mut dyn Write) -> bool {
    if let Some(name) = args.first() {
        let search = if name.starts_with('.') {
            name.to_string()
        } else {
            format!(".{name}")
        };
        if let Some(cmd) = DOT_COMMANDS.iter().find(|c| c.name == search) {
            let _ = writeln!(out, "{} {}  -- {}", cmd.name, cmd.args, cmd.description);
            true
        } else {
            let _ = writeln!(out, "Unknown command: {search}");
            false
        }
    } else {
        for cmd in DOT_COMMANDS {
            let _ = writeln!(out, "{:<16} {:<12} {}", cmd.name, cmd.args, cmd.description);
        }
        true
    }
}

fn cmd_tables(conn: &Connection<'_>, out: &mut dyn Write) {
    let mut tables = conn.tables();
    tables.sort();
    for t in tables {
        let _ = writeln!(out, "{t}");
    }
}

fn cmd_schema(args: &[&str], conn: &Connection<'_>, out: &mut dyn Write) -> bool {
    let tables = if let Some(name) = args.first() {
        vec![name.to_string()]
    } else {
        let mut t: Vec<String> = conn.tables().into_iter().map(|s| s.to_string()).collect();
        t.sort();
        t
    };

    let mut success = true;
    for name in &tables {
        if let Some(schema) = conn.table_schema(name) {
            let mut ddl = format!("CREATE TABLE {} (\n", name);
            for (i, col) in schema.columns.iter().enumerate() {
                if i > 0 {
                    ddl.push_str(",\n");
                }
                ddl.push_str(&format!("  {} {}", col.name, col.data_type));
                if !col.nullable {
                    ddl.push_str(" NOT NULL");
                }
            }
            if !schema.primary_key_columns.is_empty() {
                let pk_cols: Vec<&str> = schema
                    .primary_key_columns
                    .iter()
                    .filter_map(|&idx| schema.columns.get(idx as usize).map(|c| c.name.as_str()))
                    .collect();
                ddl.push_str(&format!(",\n  PRIMARY KEY ({})", pk_cols.join(", ")));
            }
            ddl.push_str("\n);");
            let _ = writeln!(out, "{ddl}");
        } else {
            let _ = writeln!(out, "Error: table '{name}' not found");
            success = false;
        }
    }
    success
}

fn cmd_indexes(args: &[&str], conn: &Connection<'_>, out: &mut dyn Write) -> bool {
    let tables = if let Some(name) = args.first() {
        vec![name.to_string()]
    } else {
        let mut t: Vec<String> = conn.tables().into_iter().map(|s| s.to_string()).collect();
        t.sort();
        t
    };

    let mut success = true;
    for name in &tables {
        if let Some(schema) = conn.table_schema(name) {
            for idx in &schema.indices {
                let unique = if idx.unique { " UNIQUE" } else { "" };
                let col_names: Vec<String> = idx
                    .column_positions_iter()
                    .filter_map(|ci| schema.columns.get(ci as usize).map(|c| c.name.clone()))
                    .collect();
                let _ = writeln!(
                    out,
                    "{}{} ON {} ({})",
                    idx.name,
                    unique,
                    name,
                    col_names.join(", ")
                );
            }
        } else {
            let _ = writeln!(out, "Error: table '{name}' not found");
            success = false;
        }
    }
    success
}

fn cmd_mode(args: &[&str], settings: &mut Settings, out: &mut dyn Write) -> bool {
    if let Some(mode_str) = args.first() {
        if let Some(mode) = OutputMode::from_str_opt(mode_str) {
            settings.mode = mode;
            true
        } else {
            let _ = writeln!(
                out,
                "Unknown mode: {mode_str}. Use: box, table, csv, json, line"
            );
            false
        }
    } else {
        let _ = writeln!(out, "Current mode: {}", settings.mode);
        true
    }
}

fn cmd_headers(args: &[&str], settings: &mut Settings, out: &mut dyn Write) -> bool {
    match args.first().copied() {
        Some("on") => {
            settings.show_headers = true;
            true
        }
        Some("off") => {
            settings.show_headers = false;
            true
        }
        _ => {
            let _ = writeln!(out, "Usage: .headers on|off");
            false
        }
    }
}

fn cmd_nullvalue(args: &[&str], settings: &mut Settings, out: &mut dyn Write) {
    if let Some(val) = args.first() {
        settings.null_display = val.to_string();
    } else {
        let _ = writeln!(out, "Current null display: \"{}\"", settings.null_display);
    }
}

fn cmd_timer(args: &[&str], settings: &mut Settings, out: &mut dyn Write) -> bool {
    match args.first().copied() {
        Some("on") => {
            settings.timer = true;
            true
        }
        Some("off") => {
            settings.timer = false;
            true
        }
        _ => {
            let _ = writeln!(out, "Usage: .timer on|off");
            false
        }
    }
}

fn cmd_changes(args: &[&str], settings: &mut Settings, out: &mut dyn Write) -> bool {
    match args.first().copied() {
        Some("on") => {
            settings.show_changes = true;
            true
        }
        Some("off") => {
            settings.show_changes = false;
            true
        }
        _ => {
            let _ = writeln!(out, "Usage: .changes on|off");
            false
        }
    }
}

fn cmd_stats(db: &Database, out: &mut dyn Write) {
    let stats = db.stats();
    let _ = writeln!(out, "Tree depth:       {}", stats.tree_depth);
    let _ = writeln!(out, "Entry count:      {}", stats.entry_count);
    let _ = writeln!(out, "Total pages:      {}", stats.total_pages);
    let _ = writeln!(out, "High water mark:  {}", stats.high_water_mark);
    let mut merkle_hex = String::with_capacity(stats.merkle_root.len() * 2);
    for byte in &stats.merkle_root {
        merkle_hex.push_str(&format!("{byte:02x}"));
    }
    let _ = writeln!(out, "Merkle root:      {merkle_hex}");
}

fn cmd_backup(args: &[&str], db: &Database, out: &mut dyn Write) -> bool {
    if let Some(path) = args.first() {
        match db.backup(Path::new(path)) {
            Ok(()) => {
                let _ = writeln!(out, "Backup created: {path}");
                true
            }
            Err(e) => {
                let _ = writeln!(out, "Error: {e}");
                false
            }
        }
    } else {
        let _ = writeln!(out, "Usage: .backup PATH");
        false
    }
}

fn cmd_compact(args: &[&str], db: &Database, out: &mut dyn Write) -> bool {
    if let Some(path) = args.first() {
        match db.compact(Path::new(path)) {
            Ok(()) => {
                let _ = writeln!(out, "Compacted to: {path}");
                true
            }
            Err(e) => {
                let _ = writeln!(out, "Error: {e}");
                false
            }
        }
    } else {
        let _ = writeln!(out, "Usage: .compact PATH");
        false
    }
}

fn cmd_upgrade(db: &Database, out: &mut dyn Write) -> bool {
    match db.upgrade_format() {
        Ok(report) => {
            let _ = writeln!(out, "Tables refreshed: {}", report.tables_refreshed);
            let _ = writeln!(
                out,
                "Commit slots: {}",
                if report.slots_flagged {
                    "sealed V1, header flag set"
                } else {
                    "resealed, flag pending"
                }
            );
            let _ = writeln!(
                out,
                "Audit log: {}",
                if report.audit_upgraded {
                    "upgraded to v2"
                } else {
                    "already current (or disabled)"
                }
            );
            true
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            false
        }
    }
}

fn cmd_verify(db: &Database, out: &mut dyn Write) -> bool {
    match db.integrity_check_quiet() {
        Ok(report) => {
            let _ = writeln!(out, "Pages checked: {}", report.pages_checked);
            if report.errors.is_empty() {
                let _ = writeln!(out, "No errors found.");
                true
            } else {
                let _ = writeln!(out, "Errors found: {}", report.errors.len());
                for err in &report.errors {
                    let _ = writeln!(out, "  {err}");
                }
                let tampered = report.tampered().count();
                if tampered > 0 {
                    let _ = writeln!(
                        out,
                        "{tampered} of those mean the bytes on disk were altered, not merely \
                         unreadable."
                    );
                }
                false
            }
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            false
        }
    }
}

fn cmd_audit(args: &[&str], db: &Database, out: &mut dyn Write) -> bool {
    macro_rules! emit {
        ($($arg:tt)*) => {
            if writeln!(out, $($arg)*).is_err() {
                return false;
            }
        };
    }

    let verify = args.len() == 1 && args[0].eq_ignore_ascii_case("verify");
    if !args.is_empty() && !verify {
        emit!("Usage: .audit [verify]");
        return false;
    }

    if verify {
        match db.verify_audit_chain() {
            Ok(segments) => {
                let mut total = 0u64;
                let mut broken = false;
                let mut count_shortfall = db.audit_entries_missing().unwrap_or(0);
                for (path, result) in &segments {
                    total += result.entries_verified;
                    let name = escaped_file_name(path);
                    if result.chain_valid {
                        count_shortfall = count_shortfall.saturating_add(result.entries_missing());
                        emit!("{name}: valid ({} entries)", result.entries_verified);
                    } else {
                        broken = true;
                        emit!("{name}: BROKEN");
                        if let Some(seq) = result.chain_break_at {
                            emit!("  chain break at sequence: {seq}");
                        }
                        if result.entries_declared != result.entries_verified {
                            emit!(
                                "  header declared {}, verified {} before the break",
                                result.entries_declared,
                                result.entries_verified
                            );
                        }
                    }
                }
                emit!("Entries verified: {total}");
                emit!("HMAC chain: {}", if broken { "BROKEN" } else { "valid" });
                if count_shortfall > 0 {
                    emit!(
                        "Header-count consistency: short by {count_shortfall} entries (count is not authenticated)"
                    );
                } else {
                    emit!(
                        "Header-count consistency: no mismatch observed (not an anti-rollback guarantee)"
                    );
                }
                !broken && count_shortfall == 0
            }
            Err(e) => {
                emit!("Error: {e}");
                false
            }
        }
    } else {
        let mut current_path = None;
        let visited = db.visit_verified_audit_history(|path, entry| {
            if current_path.as_deref() != Some(path) {
                let name = escaped_file_name(path);
                writeln!(out, "-- {name} --").map_err(citadel::Error::from)?;
                current_path = Some(path.to_path_buf());
            }
            let detail = citadel::AuditDetail::decode(entry.event_type, &entry.detail);
            let shown = detail.to_string();
            let separator = if shown.is_empty() { "" } else { "  " };
            writeln!(
                out,
                "[seq={:>4}] {:>20} {}{separator}{shown}",
                entry.sequence_no,
                entry.timestamp,
                entry.event_type.as_str(),
            )
            .map_err(citadel::Error::from)?;
            Ok(())
        });
        match visited {
            Ok(None) => {
                emit!("Audit logging is not enabled.");
                true
            }
            Ok(Some(0)) => {
                emit!("No audit entries.");
                true
            }
            Ok(Some(total)) => {
                emit!("Total: {total} entries");
                true
            }
            Err(error) => {
                emit!("Error verifying audit history: {error}");
                false
            }
        }
    }
}

fn escaped_file_name(path: &Path) -> String {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string());
    format!("{name:?}")
}

fn cmd_rekey(db: &Database, out: &mut dyn Write) -> bool {
    let old_pass = match rpassword::prompt_password("Current passphrase: ") {
        Ok(p) => p,
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            return false;
        }
    };

    let new_pass = match rpassword::prompt_password("New passphrase: ") {
        Ok(p) => p,
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            return false;
        }
    };

    let confirm = match rpassword::prompt_password("Confirm new passphrase: ") {
        Ok(p) => p,
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            return false;
        }
    };

    if new_pass != confirm {
        let _ = writeln!(out, "Error: passphrases do not match");
        return false;
    }

    match db.change_passphrase(old_pass.as_bytes(), new_pass.as_bytes()) {
        Ok(()) => {
            let _ = writeln!(out, "Passphrase changed successfully.");
            true
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            false
        }
    }
}

fn cmd_dump(
    args: &[&str],
    conn: &Connection<'_>,
    settings: &mut Settings,
    out: &mut dyn Write,
) -> bool {
    let result = match settings.output_file.as_mut() {
        Some(file) => dump_data(conn, args.first().copied(), file),
        None => dump_data(conn, args.first().copied(), out),
    };
    match result {
        Ok(success) => success,
        Err(error) => {
            let _ = writeln!(out, "Error writing dump: {error}");
            false
        }
    }
}

fn dump_data(
    conn: &Connection<'_>,
    table_name: Option<&str>,
    out: &mut dyn Write,
) -> std::io::Result<bool> {
    let tables = if let Some(name) = table_name {
        vec![name.to_string()]
    } else {
        let mut t: Vec<String> = conn.tables().into_iter().map(|s| s.to_string()).collect();
        t.sort();
        t
    };

    writeln!(out, "BEGIN TRANSACTION;")?;

    let mut success = true;
    for name in &tables {
        if let Some(schema) = conn.table_schema(name) {
            let mut ddl = format!("CREATE TABLE {} (\n", name);
            for (i, col) in schema.columns.iter().enumerate() {
                if i > 0 {
                    ddl.push_str(",\n");
                }
                ddl.push_str(&format!("  {} {}", col.name, col.data_type));
                if !col.nullable {
                    ddl.push_str(" NOT NULL");
                }
            }
            if !schema.primary_key_columns.is_empty() {
                let pk_cols: Vec<&str> = schema
                    .primary_key_columns
                    .iter()
                    .filter_map(|&idx| schema.columns.get(idx as usize).map(|c| c.name.as_str()))
                    .collect();
                ddl.push_str(&format!(",\n  PRIMARY KEY ({})", pk_cols.join(", ")));
            }
            ddl.push_str("\n);");
            writeln!(out, "{ddl}")?;

            for idx in &schema.indices {
                let unique = if idx.unique { "UNIQUE " } else { "" };
                let col_names: Vec<String> = idx
                    .column_positions_iter()
                    .filter_map(|ci| schema.columns.get(ci as usize).map(|c| c.name.clone()))
                    .collect();
                writeln!(
                    out,
                    "CREATE {unique}INDEX {} ON {} ({});",
                    idx.name,
                    name,
                    col_names.join(", ")
                )?;
            }

            let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
            let col_list = col_names.join(", ");

            let sql = format!("SELECT * FROM {name};");
            match conn.query(&sql) {
                Ok(qr) => {
                    for row in &qr.rows {
                        let values: Vec<String> = row.iter().map(sql_literal).collect();
                        writeln!(
                            out,
                            "INSERT INTO {} ({}) VALUES ({});",
                            name,
                            col_list,
                            values.join(", ")
                        )?;
                    }
                }
                Err(e) => {
                    writeln!(out, "-- Error dumping {name}: {e}")?;
                    success = false;
                }
            }
        } else {
            writeln!(out, "-- Error: table '{name}' not found")?;
            success = false;
        }
    }

    writeln!(out, "COMMIT;")?;
    Ok(success)
}

fn sql_literal(v: &citadel_sql::Value) -> String {
    match v {
        citadel_sql::Value::Null => "NULL".to_string(),
        citadel_sql::Value::Integer(n) => n.to_string(),
        citadel_sql::Value::Real(r) => r.to_string(),
        citadel_sql::Value::Text(s) => {
            let escaped = s.replace('\'', "''");
            format!("'{escaped}'")
        }
        citadel_sql::Value::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        citadel_sql::Value::Blob(b) => {
            let mut hex = String::with_capacity(2 + b.len() * 2);
            hex.push_str("X'");
            for byte in b {
                hex.push_str(&format!("{byte:02X}"));
            }
            hex.push('\'');
            hex
        }
        citadel_sql::Value::Date(d) => {
            format!("DATE '{}'", citadel_sql::datetime::format_date(*d))
        }
        citadel_sql::Value::Time(t) => {
            format!("TIME '{}'", citadel_sql::datetime::format_time(*t))
        }
        citadel_sql::Value::Timestamp(t) => {
            format!(
                "TIMESTAMP '{}'",
                citadel_sql::datetime::format_timestamp(*t)
            )
        }
        citadel_sql::Value::Interval {
            months,
            days,
            micros,
        } => format!(
            "INTERVAL '{}'",
            citadel_sql::datetime::format_interval(*months, *days, *micros)
        ),
        citadel_sql::Value::Json(s) => {
            let escaped = s.replace('\'', "''");
            format!("'{escaped}'::json")
        }
        citadel_sql::Value::Jsonb(b) => {
            let text = citadel_sql::json::decode_to_text(b).unwrap_or_default();
            let escaped = text.replace('\'', "''");
            format!("'{escaped}'::jsonb")
        }
        citadel_sql::Value::TsVector(b) => {
            let text = citadel_sql::fts::tsvector_display(b).replace('\'', "''");
            format!("'{text}'::tsvector")
        }
        citadel_sql::Value::TsQuery(b) => {
            let text = citadel_sql::fts::tsquery_display(b).replace('\'', "''");
            format!("'{text}'::tsquery")
        }
        citadel_sql::Value::Array(elems) => {
            let inner: Vec<String> = elems.iter().map(sql_literal).collect();
            format!("ARRAY[{}]", inner.join(", "))
        }
        citadel_sql::Value::Vector(v) => {
            let inner: Vec<String> = v.iter().map(|x| x.to_string()).collect();
            format!("'[{}]'::VECTOR({})", inner.join(","), v.len())
        }
    }
}

fn cmd_read(
    args: &[&str],
    db: &Database,
    conn: &Connection<'_>,
    settings: &mut Settings,
    out: &mut dyn Write,
) -> Action {
    let path = match args.first() {
        Some(p) => *p,
        None => {
            let _ = writeln!(out, "Usage: .read FILE");
            return Action::Failed;
        }
    };

    let canonical = match fs::canonicalize(path) {
        Ok(path) => path,
        Err(e) => {
            let _ = writeln!(out, "Error resolving file: {e}");
            return Action::Failed;
        }
    };
    if settings.read_stack.contains(&canonical) {
        let _ = writeln!(
            out,
            "Error: recursive .read detected: {}",
            canonical.display()
        );
        return Action::Failed;
    }
    if settings.read_stack.len() >= 32 {
        let _ = writeln!(out, "Error: .read nesting exceeds 32 files");
        return Action::Failed;
    }

    let content = match fs::read_to_string(&canonical) {
        Ok(c) => c,
        Err(e) => {
            let _ = writeln!(out, "Error reading file: {e}");
            return Action::Failed;
        }
    };

    settings.read_stack.push(canonical);
    let outcome = run_read_content(&content, db, conn, settings, out);
    settings.read_stack.pop();
    outcome
}

fn run_read_content(
    content: &str,
    db: &Database,
    conn: &Connection<'_>,
    settings: &mut Settings,
    out: &mut dyn Write,
) -> Action {
    let mut buf = String::new();
    let mut success = true;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }

        if trimmed.starts_with('.') {
            match execute_dot_command(trimmed, db, conn, settings, out) {
                Action::Continue => {}
                Action::Failed => success = false,
                Action::Quit => {
                    return if success {
                        Action::Quit
                    } else {
                        Action::QuitFailed
                    };
                }
                Action::QuitFailed => return Action::QuitFailed,
                Action::Reopen(path) => {
                    let _ = writeln!(out, "Error: .open is unavailable inside .read ({path})");
                    success = false;
                }
            }
            continue;
        }

        buf.push_str(line);
        buf.push(' ');

        if has_complete_sql(&buf) {
            let sql = buf.trim();
            if !sql.is_empty() {
                let start = Instant::now();
                match conn.execute(sql) {
                    Ok(result) => {
                        let output = formatter::format_result(&result, settings);
                        if !output.is_empty() && !write_result_output(settings, out, &output) {
                            success = false;
                        }
                        if settings.timer {
                            let elapsed = start.elapsed();
                            if !write_result_output(
                                settings,
                                out,
                                &format!("Run Time: {:.3}s", elapsed.as_secs_f64()),
                            ) {
                                success = false;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = writeln!(out, "Error: {e}");
                        success = false;
                    }
                }
            }
            buf.clear();
        }
    }
    if !buf.trim().is_empty() {
        let _ = writeln!(out, "Error: incomplete SQL at end of .read input");
        success = false;
    }
    command_action(success)
}

fn write_result_output(settings: &mut Settings, out: &mut dyn Write, text: &str) -> bool {
    let result = match settings.output_file.as_mut() {
        Some(file) => writeln!(file, "{text}"),
        None => writeln!(out, "{text}"),
    };
    if let Err(error) = result {
        let _ = writeln!(out, "Error writing output: {error}");
        false
    } else {
        true
    }
}

fn has_complete_sql(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return false;
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
    !in_single && !in_double && trimmed.ends_with(';')
}

fn cmd_output(args: &[&str], settings: &mut Settings, out: &mut dyn Write) -> bool {
    if args.is_empty() {
        settings.output_file = None;
        let _ = writeln!(out, "Output: stdout");
        true
    } else {
        match fs::File::create(args[0]) {
            Ok(f) => {
                settings.output_file = Some(f);
                let _ = writeln!(out, "Output: {}", args[0]);
                true
            }
            Err(e) => {
                let _ = writeln!(out, "Error opening output file: {e}");
                false
            }
        }
    }
}

fn cmd_width(args: &[&str], settings: &mut Settings, out: &mut dyn Write) -> bool {
    if args.is_empty() {
        settings.column_widths.clear();
        let _ = writeln!(out, "Column widths reset.");
        return true;
    }

    let mut widths = Vec::new();
    for arg in args {
        match arg.parse::<usize>() {
            Ok(w) => widths.push(w),
            Err(_) => {
                let _ = writeln!(out, "Error: '{arg}' is not a valid width");
                return false;
            }
        }
    }
    settings.column_widths = widths;
    true
}

fn cmd_sync(args: &[&str], db: &Database, conn: &Connection<'_>, out: &mut dyn Write) -> bool {
    if args.len() < 2 {
        let _ = writeln!(out, "Usage: .sync HOST:PORT KEY");
        return false;
    }
    let addr = args[0];
    let sync_key = match citadel::SyncKey::from_base64(args[1]) {
        Ok(k) => k,
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            return false;
        }
    };

    let _ = writeln!(out, "Syncing to {addr}...");

    match db.sync_to(addr, &sync_key) {
        Ok(outcome) => {
            print_sync_outcome(&outcome, out);
            if let Err(e) = conn.refresh_schema() {
                let _ = writeln!(out, "Warning: failed to refresh schema: {e}");
                return false;
            }
            true
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            false
        }
    }
}

fn cmd_listen(args: &[&str], db: &Database, conn: &Connection<'_>, out: &mut dyn Write) -> bool {
    if args.is_empty() {
        let _ = writeln!(out, "Usage: .listen [PORT] KEY");
        return false;
    }

    let (port, key_str) = if args.len() >= 2 {
        match args[0].parse::<u16>() {
            Ok(p) => (p, args[1]),
            Err(_) => {
                let _ = writeln!(out, "Error: invalid port '{}'", args[0]);
                return false;
            }
        }
    } else {
        (4248, args[0])
    };

    let sync_key = match citadel::SyncKey::from_base64(key_str) {
        Ok(k) => k,
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            return false;
        }
    };

    let listener = match TcpListener::bind(("0.0.0.0", port)) {
        Ok(l) => l,
        Err(e) => {
            let _ = writeln!(out, "Error binding port {port}: {e}");
            return false;
        }
    };

    let addr = listener.local_addr().unwrap();
    let _ = writeln!(out, "Listening on {addr}...");

    let (stream, peer) = match listener.accept() {
        Ok(pair) => pair,
        Err(e) => {
            let _ = writeln!(out, "Error accepting connection: {e}");
            return false;
        }
    };

    let _ = writeln!(out, "Connection from {peer}");

    match db.handle_sync(stream, &sync_key) {
        Ok(outcome) => {
            print_sync_outcome(&outcome, out);
            if let Err(e) = conn.refresh_schema() {
                let _ = writeln!(out, "Warning: failed to refresh schema: {e}");
                return false;
            }
            true
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            false
        }
    }
}

fn cmd_keygen(out: &mut dyn Write) {
    let key = citadel::SyncKey::generate();
    let _ = writeln!(out, "{}", key.to_base64());
}

fn cmd_nodeid(db: &Database, out: &mut dyn Write) -> bool {
    match db.node_id() {
        Ok(id) => {
            let _ = writeln!(out, "{id}");
            true
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            false
        }
    }
}

fn print_sync_outcome(outcome: &citadel::SyncOutcome, out: &mut dyn Write) {
    if outcome.tables_synced.is_empty() {
        let _ = writeln!(out, "No tables synced.");
        return;
    }

    let mut total: u64 = 0;
    for (name_bytes, entries) in &outcome.tables_synced {
        let name = String::from_utf8_lossy(name_bytes);
        let _ = writeln!(out, "  {name}: {entries} entries");
        total += entries;
    }
    let _ = writeln!(
        out,
        "Synced {} table(s), {} total entries.",
        outcome.tables_synced.len(),
        total,
    );
}

#[cfg(test)]
mod tests {
    use super::{escaped_file_name, execute_dot_command, Action};
    use crate::formatter::OutputMode;
    use crate::repl::Settings;
    use std::io::{Error, ErrorKind, Write};
    use std::path::Path;

    struct BrokenWriter;

    impl Write for BrokenWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(Error::new(ErrorKind::BrokenPipe, "injected write failure"))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn audit_file_names_escape_terminal_controls() {
        let shown = escaped_file_name(Path::new("vault\n\u{1b}[31m.citadel-audit"));

        assert!(shown.contains("\\n"), "{shown:?}");
        assert!(!shown.contains('\n'), "{shown:?}");
        assert!(!shown.contains('\u{1b}'), "{shown:?}");
    }

    #[test]
    fn audit_and_dump_propagate_output_failures() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output-errors.cdl");
        let builder = citadel::DatabaseBuilder::new(&path).passphrase(b"test-passphrase");
        #[cfg(not(feature = "fips"))]
        let builder = builder.argon2_profile(citadel::Argon2Profile::Iot);
        #[cfg(feature = "fips")]
        let builder = builder
            .kdf_algorithm(citadel::KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000);
        let db = builder.create().unwrap();
        let conn = citadel_sql::Connection::open(&db).unwrap();
        let mut settings = Settings {
            mode: OutputMode::Box,
            show_headers: true,
            null_display: "NULL".to_owned(),
            timer: false,
            show_changes: false,
            use_color: false,
            column_widths: Vec::new(),
            output_file: None,
            read_stack: Vec::new(),
        };

        for command in [".audit", ".dump"] {
            assert!(matches!(
                execute_dot_command(command, &db, &conn, &mut settings, &mut BrokenWriter,),
                Action::Failed
            ));
        }
    }
}
