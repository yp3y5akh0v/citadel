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
    DotCommand { name: ".help", args: "[CMD]", description: "Show help for dot-commands" },
    DotCommand { name: ".quit", args: "", description: "Exit the shell" },
    DotCommand { name: ".exit", args: "", description: "Exit the shell" },
    DotCommand { name: ".tables", args: "", description: "List all tables" },
    DotCommand { name: ".schema", args: "[TABLE]", description: "Show CREATE TABLE statement" },
    DotCommand { name: ".indexes", args: "[TABLE]", description: "Show indexes" },
    DotCommand { name: ".mode", args: "MODE", description: "Set output mode (box/table/csv/json/line)" },
    DotCommand { name: ".headers", args: "on|off", description: "Toggle column headers" },
    DotCommand { name: ".nullvalue", args: "STRING", description: "Set NULL display string" },
    DotCommand { name: ".timer", args: "on|off", description: "Toggle query timing" },
    DotCommand { name: ".changes", args: "on|off", description: "Toggle 'N row(s) affected' display" },
    DotCommand { name: ".stats", args: "", description: "Show database statistics" },
    DotCommand { name: ".backup", args: "PATH", description: "Create a hot backup" },
    DotCommand { name: ".compact", args: "PATH", description: "Compact database to a new file" },
    DotCommand { name: ".verify", args: "", description: "Run integrity check" },
    DotCommand { name: ".audit", args: "[verify]", description: "Show or verify audit log" },
    DotCommand { name: ".rekey", args: "", description: "Change database passphrase" },
    DotCommand { name: ".dump", args: "[TABLE]", description: "Dump CREATE + INSERT statements" },
    DotCommand { name: ".read", args: "FILE", description: "Execute SQL from a file" },
    DotCommand { name: ".open", args: "PATH", description: "Open a different database" },
    DotCommand { name: ".output", args: "[FILE]", description: "Redirect output to file (no arg = stdout)" },
    DotCommand { name: ".width", args: "N...", description: "Set column widths for box/table mode" },
    DotCommand { name: ".sync", args: "HOST:PORT KEY", description: "Push tables to a remote peer" },
    DotCommand { name: ".listen", args: "[PORT] KEY", description: "Listen for one incoming sync" },
    DotCommand { name: ".keygen", args: "", description: "Generate a sync key" },
    DotCommand { name: ".nodeid", args: "", description: "Show this database's node ID" },
];

pub enum Action {
    Continue,
    Quit,
    Reopen(String),
}

pub fn execute_dot_command(
    input: &str,
    db: &Database,
    conn: &mut Connection<'_>,
    settings: &mut Settings,
    out: &mut dyn Write,
) -> Action {
    let parts: Vec<&str> = input.split_whitespace().collect();
    let cmd = parts.first().map(|s| s.to_ascii_lowercase()).unwrap_or_default();
    let args: Vec<&str> = parts[1..].to_vec();

    match cmd.as_str() {
        ".help" => {
            cmd_help(&args, out);
            Action::Continue
        }
        ".quit" | ".exit" => Action::Quit,
        ".tables" => {
            cmd_tables(conn, out);
            Action::Continue
        }
        ".schema" => {
            cmd_schema(&args, conn, out);
            Action::Continue
        }
        ".indexes" => {
            cmd_indexes(&args, conn, out);
            Action::Continue
        }
        ".mode" => {
            cmd_mode(&args, settings, out);
            Action::Continue
        }
        ".headers" => {
            cmd_headers(&args, settings, out);
            Action::Continue
        }
        ".nullvalue" => {
            cmd_nullvalue(&args, settings, out);
            Action::Continue
        }
        ".timer" => {
            cmd_timer(&args, settings, out);
            Action::Continue
        }
        ".changes" => {
            cmd_changes(&args, settings, out);
            Action::Continue
        }
        ".stats" => {
            cmd_stats(db, out);
            Action::Continue
        }
        ".backup" => {
            cmd_backup(&args, db, out);
            Action::Continue
        }
        ".compact" => {
            cmd_compact(&args, db, out);
            Action::Continue
        }
        ".verify" => {
            cmd_verify(db, out);
            Action::Continue
        }
        ".audit" => {
            cmd_audit(&args, db, out);
            Action::Continue
        }
        ".rekey" => {
            cmd_rekey(db, out);
            Action::Continue
        }
        ".dump" => {
            cmd_dump(&args, conn, out);
            Action::Continue
        }
        ".read" => {
            cmd_read(&args, db, conn, settings, out);
            Action::Continue
        }
        ".open" => {
            if args.is_empty() {
                let _ = writeln!(out, "Usage: .open PATH");
                return Action::Continue;
            }
            if conn.in_transaction() {
                let _ = writeln!(out, "Error: COMMIT or ROLLBACK first");
                return Action::Continue;
            }
            Action::Reopen(args[0].to_string())
        }
        ".output" => {
            cmd_output(&args, settings, out);
            Action::Continue
        }
        ".width" => {
            cmd_width(&args, settings, out);
            Action::Continue
        }
        ".sync" => {
            cmd_sync(&args, db, conn, out);
            Action::Continue
        }
        ".listen" => {
            cmd_listen(&args, db, conn, out);
            Action::Continue
        }
        ".keygen" => {
            cmd_keygen(out);
            Action::Continue
        }
        ".nodeid" => {
            cmd_nodeid(db, out);
            Action::Continue
        }
        _ => {
            let _ = writeln!(out, "Unknown command: {cmd}. Use .help for available commands.");
            Action::Continue
        }
    }
}

fn cmd_help(args: &[&str], out: &mut dyn Write) {
    if let Some(name) = args.first() {
        let search = if name.starts_with('.') {
            name.to_string()
        } else {
            format!(".{name}")
        };
        if let Some(cmd) = DOT_COMMANDS.iter().find(|c| c.name == search) {
            let _ = writeln!(out, "{} {}  -- {}", cmd.name, cmd.args, cmd.description);
        } else {
            let _ = writeln!(out, "Unknown command: {search}");
        }
        return;
    }

    for cmd in DOT_COMMANDS {
        let _ = writeln!(out, "{:<16} {:<12} {}", cmd.name, cmd.args, cmd.description);
    }
}

fn cmd_tables(conn: &Connection<'_>, out: &mut dyn Write) {
    let mut tables = conn.tables();
    tables.sort();
    for t in tables {
        let _ = writeln!(out, "{t}");
    }
}

fn cmd_schema(args: &[&str], conn: &Connection<'_>, out: &mut dyn Write) {
    let tables = if let Some(name) = args.first() {
        vec![name.to_string()]
    } else {
        let mut t: Vec<String> = conn.tables().into_iter().map(|s| s.to_string()).collect();
        t.sort();
        t
    };

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
        }
    }
}

fn cmd_indexes(args: &[&str], conn: &Connection<'_>, out: &mut dyn Write) {
    let tables = if let Some(name) = args.first() {
        vec![name.to_string()]
    } else {
        let mut t: Vec<String> = conn.tables().into_iter().map(|s| s.to_string()).collect();
        t.sort();
        t
    };

    for name in &tables {
        if let Some(schema) = conn.table_schema(name) {
            for idx in &schema.indices {
                let unique = if idx.unique { " UNIQUE" } else { "" };
                let col_names: Vec<&str> = idx
                    .columns
                    .iter()
                    .filter_map(|&ci| schema.columns.get(ci as usize).map(|c| c.name.as_str()))
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
        }
    }
}

fn cmd_mode(args: &[&str], settings: &mut Settings, out: &mut dyn Write) {
    if let Some(mode_str) = args.first() {
        if let Some(mode) = OutputMode::from_str_opt(mode_str) {
            settings.mode = mode;
        } else {
            let _ = writeln!(out, "Unknown mode: {mode_str}. Use: box, table, csv, json, line");
        }
    } else {
        let _ = writeln!(out, "Current mode: {}", settings.mode);
    }
}

fn cmd_headers(args: &[&str], settings: &mut Settings, out: &mut dyn Write) {
    match args.first().copied() {
        Some("on") => settings.show_headers = true,
        Some("off") => settings.show_headers = false,
        _ => {
            let _ = writeln!(out, "Usage: .headers on|off");
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

fn cmd_timer(args: &[&str], settings: &mut Settings, out: &mut dyn Write) {
    match args.first().copied() {
        Some("on") => settings.timer = true,
        Some("off") => settings.timer = false,
        _ => {
            let _ = writeln!(out, "Usage: .timer on|off");
        }
    }
}

fn cmd_changes(args: &[&str], settings: &mut Settings, out: &mut dyn Write) {
    match args.first().copied() {
        Some("on") => settings.show_changes = true,
        Some("off") => settings.show_changes = false,
        _ => {
            let _ = writeln!(out, "Usage: .changes on|off");
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

fn cmd_backup(args: &[&str], db: &Database, out: &mut dyn Write) {
    if let Some(path) = args.first() {
        match db.backup(Path::new(path)) {
            Ok(()) => {
                let _ = writeln!(out, "Backup created: {path}");
            }
            Err(e) => {
                let _ = writeln!(out, "Error: {e}");
            }
        }
    } else {
        let _ = writeln!(out, "Usage: .backup PATH");
    }
}

fn cmd_compact(args: &[&str], db: &Database, out: &mut dyn Write) {
    if let Some(path) = args.first() {
        match db.compact(Path::new(path)) {
            Ok(()) => {
                let _ = writeln!(out, "Compacted to: {path}");
            }
            Err(e) => {
                let _ = writeln!(out, "Error: {e}");
            }
        }
    } else {
        let _ = writeln!(out, "Usage: .compact PATH");
    }
}

fn cmd_verify(db: &Database, out: &mut dyn Write) {
    match db.integrity_check() {
        Ok(report) => {
            let _ = writeln!(out, "Pages checked: {}", report.pages_checked);
            if report.errors.is_empty() {
                let _ = writeln!(out, "No errors found.");
            } else {
                let _ = writeln!(out, "Errors found: {}", report.errors.len());
                for err in &report.errors {
                    let _ = writeln!(out, "  {err:?}");
                }
            }
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
        }
    }
}

fn cmd_audit(args: &[&str], db: &Database, out: &mut dyn Write) {
    if args.first().copied() == Some("verify") {
        match db.verify_audit_log() {
            Ok(result) => {
                let _ = writeln!(out, "Entries verified: {}", result.entries_verified);
                if result.chain_valid {
                    let _ = writeln!(out, "HMAC chain: valid");
                } else {
                    let _ = writeln!(out, "HMAC chain: BROKEN");
                    if let Some(seq) = result.chain_break_at {
                        let _ = writeln!(out, "Chain break at sequence: {seq}");
                    }
                }
            }
            Err(e) => {
                let _ = writeln!(out, "Error: {e}");
            }
        }
        return;
    }

    if let Some(path) = db.audit_log_path() {
        match citadel::read_audit_log(&path) {
            Ok(entries) => {
                if entries.is_empty() {
                    let _ = writeln!(out, "No audit entries.");
                    return;
                }
                for entry in &entries {
                    let _ = writeln!(
                        out,
                        "[seq={:>4}] {:>20} {:?}  detail={}B",
                        entry.sequence_no,
                        entry.timestamp,
                        entry.event_type,
                        entry.detail.len(),
                    );
                }
                let _ = writeln!(out, "Total: {} entries", entries.len());
            }
            Err(e) => {
                let _ = writeln!(out, "Error reading audit log: {e}");
            }
        }
    } else {
        let _ = writeln!(out, "Audit logging is not enabled.");
    }
}

fn cmd_rekey(db: &Database, out: &mut dyn Write) {
    let old_pass = match rpassword::prompt_password("Current passphrase: ") {
        Ok(p) => p,
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            return;
        }
    };

    let new_pass = match rpassword::prompt_password("New passphrase: ") {
        Ok(p) => p,
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            return;
        }
    };

    let confirm = match rpassword::prompt_password("Confirm new passphrase: ") {
        Ok(p) => p,
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            return;
        }
    };

    if new_pass != confirm {
        let _ = writeln!(out, "Error: passphrases do not match");
        return;
    }

    match db.change_passphrase(old_pass.as_bytes(), new_pass.as_bytes()) {
        Ok(()) => {
            let _ = writeln!(out, "Passphrase changed successfully.");
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
        }
    }
}

fn cmd_dump(args: &[&str], conn: &Connection<'_>, out: &mut dyn Write) {
    let tables = if let Some(name) = args.first() {
        vec![name.to_string()]
    } else {
        let mut t: Vec<String> = conn.tables().into_iter().map(|s| s.to_string()).collect();
        t.sort();
        t
    };

    let _ = writeln!(out, "BEGIN TRANSACTION;");

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

            for idx in &schema.indices {
                let unique = if idx.unique { "UNIQUE " } else { "" };
                let col_names: Vec<&str> = idx
                    .columns
                    .iter()
                    .filter_map(|&ci| schema.columns.get(ci as usize).map(|c| c.name.as_str()))
                    .collect();
                let _ = writeln!(
                    out,
                    "CREATE {unique}INDEX {} ON {} ({});",
                    idx.name,
                    name,
                    col_names.join(", ")
                );
            }
        }
    }

    let _ = writeln!(out, "COMMIT;");
}

pub fn dump_data(
    conn: &mut Connection<'_>,
    table_name: Option<&str>,
    _settings: &Settings,
    out: &mut dyn Write,
) {
    let tables = if let Some(name) = table_name {
        vec![name.to_string()]
    } else {
        let mut t: Vec<String> = conn.tables().into_iter().map(|s| s.to_string()).collect();
        t.sort();
        t
    };

    let _ = writeln!(out, "BEGIN TRANSACTION;");

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

            for idx in &schema.indices {
                let unique = if idx.unique { "UNIQUE " } else { "" };
                let col_names: Vec<&str> = idx
                    .columns
                    .iter()
                    .filter_map(|&ci| schema.columns.get(ci as usize).map(|c| c.name.as_str()))
                    .collect();
                let _ = writeln!(
                    out,
                    "CREATE {unique}INDEX {} ON {} ({});",
                    idx.name,
                    name,
                    col_names.join(", ")
                );
            }

            let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
            let col_list = col_names.join(", ");

            let sql = format!("SELECT * FROM {name};");
            match conn.query(&sql) {
                Ok(qr) => {
                    for row in &qr.rows {
                        let values: Vec<String> = row.iter().map(|v| sql_literal(v)).collect();
                        let _ = writeln!(
                            out,
                            "INSERT INTO {} ({}) VALUES ({});",
                            name,
                            col_list,
                            values.join(", ")
                        );
                    }
                }
                Err(e) => {
                    let _ = writeln!(out, "-- Error dumping {name}: {e}");
                }
            }
        }
    }

    let _ = writeln!(out, "COMMIT;");
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
    }
}

fn cmd_read(
    args: &[&str],
    db: &Database,
    conn: &mut Connection<'_>,
    settings: &mut Settings,
    out: &mut dyn Write,
) {
    let path = match args.first() {
        Some(p) => *p,
        None => {
            let _ = writeln!(out, "Usage: .read FILE");
            return;
        }
    };

    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            let _ = writeln!(out, "Error reading file: {e}");
            return;
        }
    };

    let mut buf = String::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }

        if trimmed.starts_with('.') {
            execute_dot_command(trimmed, db, conn, settings, out);
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
                        if !output.is_empty() {
                            let _ = writeln!(out, "{output}");
                        }
                        if settings.timer {
                            let elapsed = start.elapsed();
                            let _ = writeln!(out, "Run Time: {:.3}s", elapsed.as_secs_f64());
                        }
                    }
                    Err(e) => {
                        let _ = writeln!(out, "Error: {e}");
                    }
                }
            }
            buf.clear();
        }
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

fn cmd_output(args: &[&str], settings: &mut Settings, out: &mut dyn Write) {
    if args.is_empty() {
        settings.output_file = None;
        let _ = writeln!(out, "Output: stdout");
    } else {
        match fs::File::create(args[0]) {
            Ok(f) => {
                settings.output_file = Some(f);
                let _ = writeln!(out, "Output: {}", args[0]);
            }
            Err(e) => {
                let _ = writeln!(out, "Error opening output file: {e}");
            }
        }
    }
}

fn cmd_width(args: &[&str], settings: &mut Settings, out: &mut dyn Write) {
    if args.is_empty() {
        settings.column_widths.clear();
        let _ = writeln!(out, "Column widths reset.");
        return;
    }

    let mut widths = Vec::new();
    for arg in args {
        match arg.parse::<usize>() {
            Ok(w) => widths.push(w),
            Err(_) => {
                let _ = writeln!(out, "Error: '{arg}' is not a valid width");
                return;
            }
        }
    }
    settings.column_widths = widths;
}

fn cmd_sync(args: &[&str], db: &Database, conn: &mut Connection<'_>, out: &mut dyn Write) {
    if args.len() < 2 {
        let _ = writeln!(out, "Usage: .sync HOST:PORT KEY");
        return;
    }
    let addr = args[0];
    let sync_key = match citadel::SyncKey::from_base64(args[1]) {
        Ok(k) => k,
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            return;
        }
    };

    let _ = writeln!(out, "Syncing to {addr}...");

    match db.sync_to(addr, &sync_key) {
        Ok(outcome) => {
            print_sync_outcome(&outcome, out);
            if let Err(e) = conn.refresh_schema() {
                let _ = writeln!(out, "Warning: failed to refresh schema: {e}");
            }
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
        }
    }
}

fn cmd_listen(args: &[&str], db: &Database, conn: &mut Connection<'_>, out: &mut dyn Write) {
    if args.is_empty() {
        let _ = writeln!(out, "Usage: .listen [PORT] KEY");
        return;
    }

    let (port, key_str) = if args.len() >= 2 {
        match args[0].parse::<u16>() {
            Ok(p) => (p, args[1]),
            Err(_) => {
                let _ = writeln!(out, "Error: invalid port '{}'", args[0]);
                return;
            }
        }
    } else {
        (4248, args[0])
    };

    let sync_key = match citadel::SyncKey::from_base64(key_str) {
        Ok(k) => k,
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
            return;
        }
    };

    let listener = match TcpListener::bind(("0.0.0.0", port)) {
        Ok(l) => l,
        Err(e) => {
            let _ = writeln!(out, "Error binding port {port}: {e}");
            return;
        }
    };

    let addr = listener.local_addr().unwrap();
    let _ = writeln!(out, "Listening on {addr}...");

    let (stream, peer) = match listener.accept() {
        Ok(pair) => pair,
        Err(e) => {
            let _ = writeln!(out, "Error accepting connection: {e}");
            return;
        }
    };

    let _ = writeln!(out, "Connection from {peer}");

    match db.handle_sync(stream, &sync_key) {
        Ok(outcome) => {
            print_sync_outcome(&outcome, out);
            if let Err(e) = conn.refresh_schema() {
                let _ = writeln!(out, "Warning: failed to refresh schema: {e}");
            }
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
        }
    }
}

fn cmd_keygen(out: &mut dyn Write) {
    let key = citadel::SyncKey::generate();
    let _ = writeln!(out, "{}", key.to_base64());
}

fn cmd_nodeid(db: &Database, out: &mut dyn Write) {
    match db.node_id() {
        Ok(id) => {
            let _ = writeln!(out, "{id}");
        }
        Err(e) => {
            let _ = writeln!(out, "Error: {e}");
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

pub fn execute_dot_command_mut(
    input: &str,
    db: &Database,
    conn: &mut Connection<'_>,
    settings: &mut Settings,
    out: &mut dyn Write,
) -> Action {
    let parts: Vec<&str> = input.split_whitespace().collect();
    let cmd = parts.first().map(|s| s.to_ascii_lowercase()).unwrap_or_default();

    match cmd.as_str() {
        ".dump" => {
            let args: Vec<&str> = parts[1..].to_vec();
            dump_data(conn, args.first().copied(), settings, out);
            Action::Continue
        }
        ".read" => {
            let args: Vec<&str> = parts[1..].to_vec();
            cmd_read(&args, db, conn, settings, out);
            Action::Continue
        }
        _ => execute_dot_command(input, db, conn, settings, out),
    }
}
