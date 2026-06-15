//! End-to-end tests driving the built `citadel` binary.

use std::io::Write;
use std::process::{Command, Output, Stdio};

use tempfile::TempDir;

const PASS: &str = "smoketest";

/// Run the binary with `args` and optional piped stdin; capture status + output.
fn run(args: &[&str], stdin: Option<&str>) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_citadel"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn citadel");
    if let Some(input) = stdin {
        child
            .stdin
            .as_mut()
            .expect("stdin")
            .write_all(input.as_bytes())
            .expect("write stdin");
    }
    child.wait_with_output().expect("wait citadel")
}

/// A fresh encrypted database in a self-cleaning temp dir.
fn create_db() -> (TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("test.cdl").to_string_lossy().into_owned();
    let out = run(&["--create", "--passphrase", PASS, db.as_str()], Some(""));
    assert_eq!(out.status.code(), Some(0), "db create failed");
    (dir, db)
}

#[test]
fn roundtrip_piped_create_then_read_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("test.cdl").to_string_lossy().into_owned();

    // create + populate over piped stdin (run_piped + has_complete_statement).
    let setup = "CREATE TABLE t(x INTEGER PRIMARY KEY, name TEXT);\n\
                 INSERT INTO t VALUES (42, 'hello');\n";
    let out = run(
        &["--create", "--passphrase", PASS, db.as_str()],
        Some(setup),
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "create: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // reopen + positional SQL (run_batch) -> 42.
    let out = run(
        &["--passphrase", PASS, db.as_str(), "SELECT x FROM t;"],
        None,
    );
    assert_eq!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stdout).contains("42"));

    // reopen over piped stdin -> hello.
    let out = run(
        &["--passphrase", PASS, db.as_str()],
        Some("SELECT name FROM t;\n"),
    );
    assert_eq!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stdout).contains("hello"));
}

#[test]
fn missing_db_path_exits_1() {
    assert_eq!(run(&["--passphrase", PASS], None).status.code(), Some(1));
}

#[test]
fn unknown_mode_exits_1() {
    let (_dir, db) = create_db();
    let out = run(
        &[
            "--passphrase",
            PASS,
            "--mode",
            "bogus",
            db.as_str(),
            "SELECT 1;",
        ],
        None,
    );
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn wrong_passphrase_exits_1() {
    let (_dir, db) = create_db();
    let out = run(
        &["--passphrase", "wrongpass", db.as_str(), "SELECT 1;"],
        None,
    );
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn bad_sql_exits_1() {
    let (_dir, db) = create_db();
    let out = run(
        &[
            "--passphrase",
            PASS,
            db.as_str(),
            "SELECT * FROM no_such_table;",
        ],
        None,
    );
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn help_and_version_exit_0() {
    assert_eq!(run(&["--help"], None).status.code(), Some(0));
    assert_eq!(run(&["--version"], None).status.code(), Some(0));
}

#[test]
fn unknown_flag_exits_2() {
    assert_eq!(run(&["--not-a-flag"], None).status.code(), Some(2));
}
