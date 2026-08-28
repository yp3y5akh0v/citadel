mod commands;
mod formatter;
mod helper;
mod repl;

use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Parser;
use zeroize::{Zeroize, Zeroizing};

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
    null_value: String,

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

/// Run the CLI with the given argv (argv[0] is the program name); returns exit code.
pub fn run(args: Vec<String>) -> i32 {
    let mut cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(e) => {
            let _ = e.print();
            // clap sends help/version to stdout (exit 0), errors to stderr (exit 2).
            return if e.use_stderr() { 2 } else { 0 };
        }
    };

    let db_path = match &cli.database {
        Some(p) => p.clone(),
        None => {
            eprintln!("Error: database path is required");
            eprintln!("Usage: citadel [OPTIONS] <DATABASE> [SQL]");
            return 1;
        }
    };

    // Wiped on drop: the interactive prompt is the only copy of this one.
    let passphrase: Zeroizing<String> = match &cli.passphrase {
        Some(p) => Zeroizing::new(p.clone()),
        None => {
            if !std::io::stdin().is_terminal() {
                eprintln!("Error: passphrase required (use --passphrase in non-interactive mode)");
                return 1;
            }
            match rpassword::prompt_password("Enter passphrase: ") {
                Ok(p) => Zeroizing::new(p),
                Err(e) => {
                    eprintln!("Error reading passphrase: {e}");
                    return 1;
                }
            }
        }
    };
    // The parsed argv copy is redundant once it is held above.
    cli.passphrase.zeroize();

    let db = if cli.create {
        let builder = citadel::DatabaseBuilder::new(&db_path).passphrase(passphrase.as_bytes());
        #[cfg(feature = "fips")]
        let builder = builder
            .kdf_algorithm(citadel::KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000);
        match builder.create() {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Error creating database: {e}");
                return 1;
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
                return 1;
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
            return 1;
        }
    };

    let is_interactive = cli.sql.is_none() && std::io::stdin().is_terminal();
    let use_color = is_interactive && !cli.no_color;

    let mut settings = repl::Settings {
        mode: output_mode,
        show_headers: cli.header != "off",
        null_display: cli.null_value.clone(),
        timer: false,
        show_changes: false,
        use_color,
        column_widths: Vec::new(),
        output_file: None,
        read_stack: Vec::new(),
    };

    if let Some(ref sql) = cli.sql {
        return run_batch(&db, sql, &mut settings);
    }

    if !is_interactive {
        return run_piped(&db, &mut settings);
    }

    repl::run_interactive(db, db_path, passphrase, settings, cli.init, cli.cmd);
    0
}

fn run_batch(db: &citadel::Database, sql: &str, settings: &mut repl::Settings) -> i32 {
    use std::time::Instant;

    let conn = match citadel_sql::Connection::open(db) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {e}");
            return 1;
        }
    };

    let start = Instant::now();
    match conn.execute(sql) {
        Ok(result) => {
            let output = formatter::format_result(&result, settings);
            if !output.is_empty() {
                if let Err(error) = settings.write_output(&output) {
                    eprintln!("Error writing output: {error}");
                    return 1;
                }
            }
            if settings.timer {
                if let Err(error) = settings
                    .write_output(&format!("Run Time: {:.3}s", start.elapsed().as_secs_f64()))
                {
                    eprintln!("Error writing output: {error}");
                    return 1;
                }
            }
            0
        }
        Err(e) => {
            eprintln!("Error: {e}");
            1
        }
    }
}

fn run_piped(db: &citadel::Database, settings: &mut repl::Settings) -> i32 {
    use std::io::{self, BufRead};

    let conn = match citadel_sql::Connection::open(db) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {e}");
            return 1;
        }
    };

    let mut buf = String::new();
    let stdin = io::stdin();
    let mut failed = false;

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading stdin: {e}");
                return 1;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.starts_with('.') {
            match commands::execute_dot_command(trimmed, db, &conn, settings, &mut io::stdout()) {
                commands::Action::Continue => {}
                commands::Action::Failed => failed = true,
                commands::Action::Quit => break,
                commands::Action::QuitFailed => {
                    failed = true;
                    break;
                }
                commands::Action::Reopen(path) => {
                    eprintln!("Error: .open is unavailable in piped input ({path})");
                    failed = true;
                }
            }
            continue;
        }

        buf.push_str(&line);
        buf.push(' ');

        if has_complete_statement(&buf) {
            let sql = buf.trim();
            if !sql.is_empty() {
                failed |= !execute_and_display(&conn, sql, &mut *settings);
            }
            buf.clear();
        }
    }

    if !buf.trim().is_empty() {
        failed |= !execute_and_display(&conn, buf.trim(), settings);
    }

    i32::from(failed)
}

fn execute_and_display(
    conn: &citadel_sql::Connection<'_>,
    sql: &str,
    settings: &mut repl::Settings,
) -> bool {
    use std::time::Instant;

    let start = Instant::now();
    match conn.execute(sql) {
        Ok(result) => {
            let output = formatter::format_result(&result, settings);
            if !output.is_empty() {
                if let Err(error) = settings.write_output(&output) {
                    eprintln!("Error writing output: {error}");
                    return false;
                }
            }
            if settings.timer {
                if let Err(error) = settings
                    .write_output(&format!("Run Time: {:.3}s", start.elapsed().as_secs_f64()))
                {
                    eprintln!("Error writing output: {error}");
                    return false;
                }
            }
            true
        }
        Err(e) => {
            eprintln!("Error: {e}");
            false
        }
    }
}

/// True when quotes are balanced (honoring backslash escapes) and it ends with `;`.
pub(crate) fn has_complete_statement(s: &str) -> bool {
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
