//! End-to-end tests driving the built `citadel` binary.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
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

fn xor_file_byte(path: &Path, offset: u64, mask: u8) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= mask;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
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
fn bad_piped_sql_exits_1() {
    let (_dir, db) = create_db();
    let out = run(
        &["--passphrase", PASS, db.as_str()],
        Some("SELECT * FROM no_such_table;\n"),
    );
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn piped_dot_command_errors_exit_1() {
    let (dir, db) = create_db();
    let missing = dir
        .path()
        .join("missing.sql")
        .to_string_lossy()
        .into_owned();
    for input in [
        ".backup\n".to_string(),
        format!(".read {missing}\n"),
        ".sync\n".to_string(),
    ] {
        let out = run(&["--passphrase", PASS, db.as_str()], Some(&input));
        assert_eq!(
            out.status.code(),
            Some(1),
            "command unexpectedly succeeded: {input:?}\n{}",
            String::from_utf8_lossy(&out.stdout)
        );
    }
}

#[test]
fn read_propagates_nested_sql_failure() {
    let (dir, db) = create_db();
    let script = dir.path().join("broken.sql");
    std::fs::write(&script, "SELECT * FROM no_such_table;\n").unwrap();
    let input = format!(".read {}\n", script.display());

    let out = run(&["--passphrase", PASS, db.as_str()], Some(&input));

    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stdout).contains("no_such_table"));
}

#[test]
fn read_rejects_cycles_and_nested_command_failures() {
    let (dir, db) = create_db();
    let recursive = dir.path().join("recursive.sql");
    std::fs::write(&recursive, format!(".read {}\n", recursive.display())).unwrap();
    let bad_command = dir.path().join("bad-command.sql");
    std::fs::write(&bad_command, ".backup\n").unwrap();
    let incomplete = dir.path().join("incomplete.sql");
    std::fs::write(&incomplete, "SELECT 1").unwrap();
    let reopen = dir.path().join("reopen.sql");
    std::fs::write(&reopen, ".open another.cdl\n").unwrap();

    for (script, expected) in [
        (&recursive, "recursive .read detected"),
        (&bad_command, "Usage: .backup PATH"),
        (&incomplete, "incomplete SQL"),
        (&reopen, ".open is unavailable inside .read"),
    ] {
        let input = format!(".read {}\n", script.display());
        let out = run(&["--passphrase", PASS, db.as_str()], Some(&input));
        assert_eq!(
            out.status.code(),
            Some(1),
            "script unexpectedly succeeded: {}",
            script.display()
        );
        assert!(
            String::from_utf8_lossy(&out.stdout).contains(expected),
            "{}",
            String::from_utf8_lossy(&out.stdout)
        );
    }
}

#[test]
fn quit_inside_read_exits_the_outer_shell() {
    let (dir, db) = create_db();
    let script = dir.path().join("quit.sql");
    std::fs::write(&script, ".quit\n").unwrap();
    let input = format!(
        ".read {}\nSELECT * FROM must_not_execute;\n",
        script.display()
    );

    let out = run(&["--passphrase", PASS, db.as_str()], Some(&input));

    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!stdout.contains("must_not_execute"), "{stdout}");

    std::fs::write(&script, "SELECT * FROM inner_failure;\n.quit\n").unwrap();
    let failed = run(&["--passphrase", PASS, db.as_str()], Some(&input));
    assert_eq!(failed.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&failed.stdout);
    assert!(stdout.contains("inner_failure"), "{stdout}");
    assert!(!stdout.contains("must_not_execute"), "{stdout}");
}

#[test]
fn dump_is_consistent_inside_read_and_honors_output_redirection() {
    let (dir, db) = create_db();
    let setup = run(
        &["--passphrase", PASS, db.as_str()],
        Some("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);\nINSERT INTO t VALUES (1, 'saved');\n"),
    );
    assert_eq!(setup.status.code(), Some(0));

    let script = dir.path().join("dump.sql");
    std::fs::write(&script, ".dump t\n").unwrap();
    let read_input = format!(".read {}\n", script.display());
    let through_read = run(&["--passphrase", PASS, db.as_str()], Some(&read_input));
    assert_eq!(through_read.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&through_read.stdout).contains("INSERT INTO t (id, value)"));

    let redirected = dir.path().join("redirected.sql");
    let redirect_input = format!(".output {}\n.dump t\n", redirected.display());
    let direct = run(&["--passphrase", PASS, db.as_str()], Some(&redirect_input));
    assert_eq!(direct.status.code(), Some(0));
    let dump = std::fs::read_to_string(redirected).unwrap();
    assert!(dump.contains("CREATE TABLE t"), "{dump}");
    assert!(dump.contains("INSERT INTO t (id, value)"), "{dump}");
}

#[test]
fn audit_rejects_unknown_subcommands_in_piped_mode() {
    let (_dir, db) = create_db();
    let out = run(
        &["--passphrase", PASS, db.as_str()],
        Some(".audit something-else\n"),
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stdout).contains("Usage: .audit [verify]"));
}

#[test]
fn audit_display_includes_retained_rotated_history() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("rotated.cdl");
    let builder = citadel::DatabaseBuilder::new(&path)
        .passphrase(PASS.as_bytes())
        .audit_config(citadel::AuditConfig {
            enabled: true,
            max_file_size: 200,
            max_rotated_files: 32,
        });
    #[cfg(not(feature = "fips"))]
    let builder = builder.argon2_profile(citadel::Argon2Profile::Iot);
    #[cfg(feature = "fips")]
    let builder = builder
        .kdf_algorithm(citadel::KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000);
    let db = builder.create().unwrap();
    for _ in 0..14 {
        db.integrity_check().unwrap();
    }
    drop(db);
    let db_path = path.to_string_lossy().into_owned();

    let out = run(&["--passphrase", PASS, db_path.as_str()], Some(".audit\n"));

    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("Database created"), "{stdout}");
    assert!(
        stdout.contains("-- \"rotated.cdl.citadel-audit."),
        "{stdout}"
    );
}

#[test]
fn audit_verification_failure_exits_1() {
    let (dir, db) = create_db();
    let audit_path = dir.path().join("test.cdl.citadel-audit");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&audit_path)
        .unwrap();
    file.seek(SeekFrom::Start(24)).unwrap();
    let mut count = [0u8; 8];
    file.read_exact(&mut count).unwrap();
    let inflated = u64::from_le_bytes(count) + 5;
    file.seek(SeekFrom::Start(24)).unwrap();
    file.write_all(&inflated.to_le_bytes()).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let out = run(
        &["--passphrase", PASS, db.as_str()],
        Some(".audit VERIFY\n"),
    );

    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stdout).contains("short by 5 entries"));
}

#[test]
fn audit_display_rejects_forged_entries_before_printing_any() {
    let (dir, db) = create_db();
    let audit_path = dir.path().join("test.cdl.citadel-audit");
    let last_hmac_byte = std::fs::metadata(&audit_path).unwrap().len() - 1;
    xor_file_byte(&audit_path, last_hmac_byte, 0x80);

    let out = run(&["--passphrase", PASS, db.as_str()], Some(".audit\n"));

    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("Error verifying audit history"), "{stdout}");
    assert!(!stdout.contains("[seq="), "{stdout}");
}

#[test]
fn verify_reports_a_corrupt_inactive_slot_and_exits_1() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("verify.cdl").to_string_lossy().into_owned();
    let setup = "CREATE TABLE t(x INTEGER PRIMARY KEY);\n\
                 INSERT INTO t VALUES (1);\n\
                 INSERT INTO t VALUES (2);\n";
    let created = run(
        &["--create", "--passphrase", PASS, db.as_str()],
        Some(setup),
    );
    assert_eq!(created.status.code(), Some(0));

    let info = citadel::inspect_vault(Path::new(&db)).unwrap();
    let inactive = 1 - info.active_slot;
    let offset = citadel::core::COMMIT_SLOT_OFFSET
        + inactive * citadel::core::COMMIT_SLOT_SIZE
        + citadel::core::SLOT_CHECKSUM;
    xor_file_byte(Path::new(&db), offset as u64, 0x80);

    let out = run(&["--passphrase", PASS, db.as_str()], Some(".verify\n"));

    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains(&format!("commit slot {inactive} failed its checksum")),
        "{stdout}"
    );
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
