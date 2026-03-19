use std::path::PathBuf;
use std::time::Instant;

use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{Config, Editor};

use citadel::Database;
use citadel_sql::Connection;

use crate::commands::{self, Action};
use crate::formatter::{self, OutputMode};
use crate::helper::CitadelHelper;

pub struct Settings {
    pub mode: OutputMode,
    pub show_headers: bool,
    pub null_display: String,
    pub timer: bool,
    pub show_changes: bool,
    pub use_color: bool,
    pub column_widths: Vec<usize>,
    pub output_file: Option<std::fs::File>,
}

impl Settings {
    pub fn write_output(&mut self, text: &str) {
        use std::io::Write;
        if let Some(ref mut f) = self.output_file {
            let _ = writeln!(f, "{text}");
        } else {
            println!("{text}");
        }
    }
}

pub fn run_interactive(
    mut db: Database,
    mut db_path: PathBuf,
    mut passphrase: String,
    mut settings: Settings,
    init_file: Option<PathBuf>,
    init_cmd: Option<String>,
) {
    let config = Config::builder().auto_add_history(true).build();

    let history_path = history_file_path();
    let mut rl: Editor<CitadelHelper, DefaultHistory> =
        Editor::with_config(config).expect("failed to create editor");

    if let Some(ref path) = history_path {
        let _ = rl.load_history(path);
    }

    'outer: loop {
        let mut conn = match Connection::open(&db) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error opening connection: {e}");
                break;
            }
        };

        let helper = CitadelHelper::new(&conn);
        rl.set_helper(Some(helper));

        if let Some(ref init_path) = init_file {
            if let Ok(content) = std::fs::read_to_string(init_path) {
                execute_batch_sql(&mut conn, &db, &content, &mut settings);
            }
        }

        if let Some(ref cmd) = init_cmd {
            execute_single(&mut conn, &db, cmd, &mut settings);
        }

        let mut buf = String::new();

        loop {
            let prompt = build_prompt(&buf, conn.in_transaction());

            match rl.readline(&prompt) {
                Ok(line) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    if buf.is_empty() && trimmed.starts_with('.') {
                        match commands::execute_dot_command_mut(
                            trimmed,
                            &db,
                            &mut conn,
                            &mut settings,
                            &mut std::io::stdout(),
                        ) {
                            Action::Quit => break 'outer,
                            Action::Reopen(new_path) => {
                                let new_pass =
                                    match rpassword::prompt_password("Enter passphrase: ") {
                                        Ok(p) => p,
                                        Err(e) => {
                                            eprintln!("Error: {e}");
                                            continue;
                                        }
                                    };

                                drop(conn);

                                match citadel::DatabaseBuilder::new(&new_path)
                                    .passphrase(new_pass.as_bytes())
                                    .open()
                                {
                                    Ok(new_db) => {
                                        db = new_db;
                                        db_path = PathBuf::from(&new_path);
                                        passphrase = new_pass;
                                        continue 'outer;
                                    }
                                    Err(e) => {
                                        eprintln!("Error opening {new_path}: {e}");
                                        match citadel::DatabaseBuilder::new(&db_path)
                                            .passphrase(passphrase.as_bytes())
                                            .open()
                                        {
                                            Ok(old_db) => {
                                                db = old_db;
                                                continue 'outer;
                                            }
                                            Err(e2) => {
                                                eprintln!(
                                                    "Fatal: cannot reopen original database: {e2}"
                                                );
                                                break 'outer;
                                            }
                                        }
                                    }
                                }
                            }
                            Action::Continue => {
                                update_helper_schema(&mut rl, &conn);
                            }
                        }
                        continue;
                    }

                    buf.push_str(&line);
                    buf.push(' ');

                    if has_complete_statement(&buf) {
                        let sql = buf.trim().to_string();
                        execute_sql(&mut conn, &db, &sql, &mut settings);
                        buf.clear();
                        update_helper_schema(&mut rl, &conn);
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    buf.clear();
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    break 'outer;
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    break 'outer;
                }
            }
        }
    }

    if let Some(ref path) = history_path {
        let _ = rl.save_history(path);
    }
}

fn build_prompt(buf: &str, in_txn: bool) -> String {
    if !buf.is_empty() {
        "   ...> ".to_string()
    } else if in_txn {
        "citadel*> ".to_string()
    } else {
        "citadel> ".to_string()
    }
}

fn execute_sql(conn: &mut Connection<'_>, _db: &Database, sql: &str, settings: &mut Settings) {
    let start = Instant::now();
    match conn.execute(sql) {
        Ok(result) => {
            let output = formatter::format_result(&result, settings);
            if !output.is_empty() {
                settings.write_output(&output);
            }
            if settings.timer {
                let elapsed = start.elapsed();
                settings.write_output(&format!("Run Time: {:.3}s", elapsed.as_secs_f64()));
            }
        }
        Err(e) => {
            if settings.use_color {
                use owo_colors::OwoColorize;
                eprintln!("{} {e}", "Error:".red().bold());
            } else {
                eprintln!("Error: {e}");
            }
        }
    }
}

fn execute_single(conn: &mut Connection<'_>, db: &Database, input: &str, settings: &mut Settings) {
    let trimmed = input.trim();
    if trimmed.starts_with('.') {
        commands::execute_dot_command_mut(trimmed, db, conn, settings, &mut std::io::stdout());
    } else {
        execute_sql(conn, db, trimmed, settings);
    }
}

fn execute_batch_sql(
    conn: &mut Connection<'_>,
    db: &Database,
    content: &str,
    settings: &mut Settings,
) {
    let mut buf = String::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }

        if trimmed.starts_with('.') {
            let mut out = Vec::new();
            commands::execute_dot_command(trimmed, db, conn, settings, &mut out);
            if !out.is_empty() {
                settings.write_output(&String::from_utf8_lossy(&out));
            }
            continue;
        }

        buf.push_str(line);
        buf.push(' ');
        if has_complete_statement(&buf) {
            let sql = buf.trim();
            if !sql.is_empty() {
                match conn.execute(sql) {
                    Ok(result) => {
                        let output = formatter::format_result(&result, settings);
                        if !output.is_empty() {
                            settings.write_output(&output);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {e}");
                    }
                }
            }
            buf.clear();
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

    for ch in trimmed.chars() {
        match ch {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            _ => {}
        }
    }

    !in_single_quote && !in_double_quote && trimmed.ends_with(';')
}

fn history_file_path() -> Option<PathBuf> {
    dirs::config_dir().map(|mut p| {
        p.push("citadel");
        let _ = std::fs::create_dir_all(&p);
        p.push("history");
        p
    })
}

fn update_helper_schema(rl: &mut Editor<CitadelHelper, DefaultHistory>, conn: &Connection<'_>) {
    if let Some(helper) = rl.helper_mut() {
        helper.update_schema(conn);
    }
}
