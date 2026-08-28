//! `create()` refuses sidecars left behind after their database was removed.

#[cfg(feature = "audit-log")]
use citadel::AuditConfig;
use citadel::{Argon2Profile, DatabaseBuilder};
use std::path::{Path, PathBuf};

const PASSPHRASE: &[u8] = b"create-guard-passphrase";

fn sidecar(data: &Path, suffix: &str) -> PathBuf {
    let mut name = data.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn create_at(path: &Path) -> citadel::Result<citadel::Database> {
    DatabaseBuilder::new(path)
        .passphrase(PASSPHRASE)
        .argon2_profile(Argon2Profile::Iot)
        .create()
}

#[test]
fn a_stale_key_file_is_never_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("vault.cdl");
    let keys = sidecar(&data, ".citadel-keys");

    drop(create_at(&data).unwrap());
    let original = std::fs::read(&keys).unwrap();
    std::fs::remove_file(&data).unwrap();

    let err = create_at(&data).expect_err("a stale key file must refuse the create");
    assert!(
        err.to_string().contains("key file"),
        "the message must name what is in the way, got: {err}"
    );
    assert_eq!(
        std::fs::read(&keys).unwrap(),
        original,
        "the refused create still rewrote the key material"
    );
    assert!(
        !data.exists(),
        "a refused create left a data file behind for the next attempt to trip over"
    );
}

#[test]
#[cfg(feature = "audit-log")]
fn a_stale_audit_log_is_refused_before_anything_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("vault.cdl");
    let keys = sidecar(&data, ".citadel-keys");
    let audit = sidecar(&data, ".citadel-audit");

    drop(create_at(&data).unwrap());
    assert!(audit.exists(), "the audit log is on by default");
    std::fs::remove_file(&data).unwrap();
    std::fs::remove_file(&keys).unwrap();

    let err = create_at(&data).expect_err("a stale audit log must refuse the create");
    assert!(
        err.to_string().contains("audit log"),
        "the message must name what is in the way, got: {err}"
    );
    assert!(
        !data.exists(),
        "the data file was written before the refusal"
    );
    assert!(
        !keys.exists(),
        "the key file was written before the refusal"
    );
}

#[test]
#[cfg(feature = "audit-log")]
fn an_orphaned_rotated_audit_generation_is_never_adopted() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("vault.cdl");
    let keys = sidecar(&data, ".citadel-keys");
    let rotated = sidecar(&data, ".citadel-audit.1");
    std::fs::write(&rotated, b"foreign audit history").unwrap();

    let err = create_at(&data).expect_err("retained audit history must refuse a new vault");
    assert!(err.to_string().contains("rotated generations"));
    assert_eq!(std::fs::read(&rotated).unwrap(), b"foreign audit history");
    assert!(!data.exists());
    assert!(!keys.exists());
}

#[test]
#[cfg(feature = "audit-log")]
fn disabled_audit_still_refuses_stale_audit_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("vault.cdl");
    let keys = sidecar(&data, ".citadel-keys");
    let rotated = sidecar(&data, ".citadel-audit.1");
    std::fs::write(&rotated, b"foreign audit history").unwrap();

    let config = AuditConfig {
        enabled: false,
        ..AuditConfig::default()
    };
    let err = DatabaseBuilder::new(&data)
        .passphrase(PASSPHRASE)
        .argon2_profile(Argon2Profile::Iot)
        .audit_config(config)
        .create()
        .expect_err("disabling audit must not adopt stale audit history");

    assert!(err.to_string().contains("rotated generations"));
    assert_eq!(std::fs::read(&rotated).unwrap(), b"foreign audit history");
    assert!(!data.exists());
    assert!(!keys.exists());
}

#[cfg(unix)]
#[test]
fn dangling_symlinks_still_occupy_every_reserved_sidecar_name() {
    use std::os::unix::fs::symlink;

    #[cfg(feature = "audit-log")]
    let cases = [
        (".citadel-keys", "key file"),
        (".citadel-audit", "audit log"),
        (".citadel-audit.upgrade", "audit upgrade image"),
    ];
    #[cfg(not(feature = "audit-log"))]
    let cases = [(".citadel-keys", "key file")];

    for (suffix, expected_name) in cases {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("vault.cdl");
        let occupied = sidecar(&data, suffix);
        symlink(dir.path().join("missing-target"), &occupied).unwrap();

        let error = create_at(&data).expect_err("a dangling link must reserve its sidecar name");
        assert!(error.to_string().contains(expected_name), "{error}");
        assert!(!data.exists());
        assert!(std::fs::symlink_metadata(&occupied)
            .unwrap()
            .file_type()
            .is_symlink());
    }
}

#[test]
fn a_clean_path_still_creates_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("vault.cdl");

    drop(create_at(&data).unwrap());
    let reopened = DatabaseBuilder::new(&data)
        .passphrase(PASSPHRASE)
        .open()
        .expect("a vault created here reopens with its own passphrase");
    drop(reopened);
}

#[test]
fn an_existing_database_still_refuses_a_create() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("vault.cdl");
    let keys = sidecar(&data, ".citadel-keys");

    drop(create_at(&data).unwrap());
    let original = std::fs::read(&keys).unwrap();

    create_at(&data).expect_err("creating over a live database must fail");
    assert_eq!(std::fs::read(&keys).unwrap(), original);
}
