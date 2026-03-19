mod commands;
mod formatter;
mod helper;
mod repl;

use std::io::IsTerminal;
use std::path::PathBuf;
use std::process;

use clap::Parser;

use crate::formatter::OutputMode;

#[derive(Parser)]
#[command(
    name = "citadel",
    about = "Interactive SQL shell for Citadel encrypted database"
)]
#[command(version)]
struct Cli {
    /// Path to database file
    database: Option<PathBuf>,

    /// SQL to execute (non-interactive mode)
    sql: Option<String>,

    /// Create a new database
    #[arg(long)]
    create: bool,

    /// Passphrase (prompted if omitted)
    #[arg(long)]
    passphrase: Option<String>,

    /// Output mode: box, table, csv, json, line
    #[arg(long, default_value = "box")]
    mode: String,

    /// Show column headers
    #[arg(long, default_value = "on")]
    header: String,

    /// NULL display string
    #[arg(long, default_value = "NULL")]
    nullvalue: String,

    /// Disable colors
    #[arg(long)]
    no_color: bool,

    /// Read/execute commands from FILE on startup
    #[arg(long)]
    init: Option<PathBuf>,

    /// Execute TEXT before interactive input
    #[arg(long)]
    cmd: Option<String>,
}

fn main() {
    let cli = Cli::parse();

    let db_path = match &cli.database {
        Some(p) => p.clone(),
        None => {
            eprintln!("Error: database path is required");
            eprintln!("Usage: citadel [OPTIONS] <DATABASE> [SQL]");
            process::exit(1);
        }
    };

    let passphrase = match &cli.passphrase {
        Some(p) => p.clone(),
        None => {
            if !std::io::stdin().is_terminal() {
                eprintln!("Error: passphrase required (use --passphrase in non-interactive mode)");
                process::exit(1);
            }
            match rpassword::prompt_password("Enter passphrase: ") {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Error reading passphrase: {e}");
                    process::exit(1);
                }
            }
        }
    };

    let db = if cli.create {
        match citadel::DatabaseBuilder::new(&db_path)
            .passphrase(passphrase.as_bytes())
            .create()
        {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Error creating database: {e}");
                process::exit(1);
            }
        }
    } else {
        match citadel::DatabaseBuilder::new(&db_path)
            .passphrase(passphrase.as_bytes())
            .open()
        {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Error opening database: {e}");
                process::exit(1);
            }
        }
    };

    let output_mode = match cli.mode.as_str() {
        "box" => OutputMode::Box,
        "table" => OutputMode::Table,
        "csv" => OutputMode::Csv,
        "json" => OutputMode::Json,
        "line" => OutputMode::Line,
        other => {
            eprintln!("Error: unknown output mode '{other}'. Use: box, table, csv, json, line");
            process::exit(1);
        }
    };

    let is_interactive = cli.sql.is_none() && std::io::stdin().is_terminal();
    let use_color = is_interactive && !cli.no_color;

    let mut settings = repl::Settings {
        mode: output_mode,
        show_headers: cli.header != "off",
        null_display: cli.nullvalue.clone(),
        timer: false,
        show_changes: false,
        use_color,
        column_widths: Vec::new(),
        output_file: None,
    };

    if let Some(ref sql) = cli.sql {
        run_batch(&db, sql, &mut settings);
        return;
    }

    if !is_interactive {
        run_piped(&db, &mut settings);
        return;
    }

    repl::run_interactive(db, db_path, passphrase, settings, cli.init, cli.cmd);
}

fn run_batch(db: &citadel::Database, sql: &str, settings: &mut repl::Settings) {
    use std::time::Instant;

    let mut conn = match citadel_sql::Connection::open(db) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {e}");
            process::exit(1);
        }
    };

    let start = Instant::now();
    match conn.execute(sql) {
        Ok(result) => {
            let output = formatter::format_result(&result, settings);
            if !output.is_empty() {
                settings.write_output(&output);
            }
            if settings.timer {
                settings.write_output(&format!("Run Time: {:.3}s", start.elapsed().as_secs_f64()));
            }
        }
        Err(e) => {
            eprintln!("Error: {e}");
            process::exit(1);
        }
    }
}

fn run_piped(db: &citadel::Database, settings: &mut repl::Settings) {
    use std::io::{self, BufRead};

    let mut conn = match citadel_sql::Connection::open(db) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {e}");
            process::exit(1);
        }
    };

    let mut buf = String::new();
    let stdin = io::stdin();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading stdin: {e}");
                process::exit(1);
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.starts_with('.') {
            commands::execute_dot_command_mut(trimmed, db, &mut conn, settings, &mut io::stdout());
            continue;
        }

        buf.push_str(&line);
        buf.push(' ');

        if has_complete_statement(&buf) {
            let sql = buf.trim();
            if !sql.is_empty() {
                execute_and_display(&mut conn, sql, &mut *settings);
            }
            buf.clear();
        }
    }

    if !buf.trim().is_empty() {
        execute_and_display(&mut conn, buf.trim(), settings);
    }
}

fn execute_and_display(
    conn: &mut citadel_sql::Connection<'_>,
    sql: &str,
    settings: &mut repl::Settings,
) {
    use std::time::Instant;

    let start = Instant::now();
    match conn.execute(sql) {
        Ok(result) => {
            let output = formatter::format_result(&result, settings);
            if !output.is_empty() {
                settings.write_output(&output);
            }
            if settings.timer {
                settings.write_output(&format!("Run Time: {:.3}s", start.elapsed().as_secs_f64()));
            }
        }
        Err(e) => {
            eprintln!("Error: {e}");
        }
    }
}

fn has_complete_statement(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return false;
    }

    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut last_char = '\0';

    for ch in trimmed.chars() {
        match ch {
            '\'' if !in_double_quote && last_char != '\\' => in_single_quote = !in_single_quote,
            '"' if !in_single_quote && last_char != '\\' => in_double_quote = !in_double_quote,
            _ => {}
        }
        last_char = ch;
    }

    !in_single_quote && !in_double_quote && trimmed.ends_with(';')
}
