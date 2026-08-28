#[cfg(not(feature = "fips"))]
use citadel::Argon2Profile;
use citadel::{Database, DatabaseBuilder, KdfAlgorithm};
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;

#[cfg(unix)]
fn sidecar(data: &Path, suffix: &str) -> PathBuf {
    let mut name = data.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn create_test_db(dir: &Path, passphrase: &[u8]) -> Database {
    let builder = DatabaseBuilder::new(dir.join("test.citadel"))
        .passphrase(passphrase)
        .cache_size(64);
    #[cfg(not(feature = "fips"))]
    let builder = builder.argon2_profile(Argon2Profile::Iot);
    #[cfg(feature = "fips")]
    let builder = builder
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000);
    builder.create().unwrap()
}

#[cfg(not(feature = "fips"))]
fn create_test_db_argon2(dir: &Path, passphrase: &[u8]) -> Database {
    DatabaseBuilder::new(dir.join("test.citadel"))
        .passphrase(passphrase)
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(64)
        .create()
        .unwrap()
}

#[test]
fn export_and_restore_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db_pass = b"database-password";
    let backup_pass = b"backup-password";
    let backup_path = dir.path().join("backup.citadel-key-backup");

    let db = create_test_db(dir.path(), db_pass);

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key1", b"value1").unwrap();
    wtx.insert(b"key2", b"value2").unwrap();
    wtx.commit().unwrap();

    db.export_key_backup(db_pass, backup_pass, &backup_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");
    std::fs::remove_file(&key_path).unwrap();

    let new_pass = b"new-database-password";
    Database::restore_key_from_backup(
        &backup_path,
        backup_pass,
        new_pass,
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(new_pass)
        .open()
        .unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(rtx.get(b"key2").unwrap(), Some(b"value2".to_vec()));
}

#[test]
fn backup_wrong_db_passphrase() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"correct");
    let backup_path = dir.path().join("backup.bin");

    let result = db.export_key_backup(b"wrong", b"backup", &backup_path);
    assert!(result.is_err());
}

#[test]
fn restore_wrong_backup_passphrase() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"db-pass");
    let backup_path = dir.path().join("backup.bin");

    db.export_key_backup(b"db-pass", b"correct-backup", &backup_path)
        .unwrap();
    drop(db);

    let result = Database::restore_key_from_backup(
        &backup_path,
        b"wrong-backup",
        b"new-pass",
        &dir.path().join("test.citadel"),
    );
    assert!(result.is_err());
}

#[test]
fn backup_file_tamper_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"db-pass");
    let backup_path = dir.path().join("backup.bin");

    db.export_key_backup(b"db-pass", b"backup-pass", &backup_path)
        .unwrap();
    drop(db);

    let mut data = std::fs::read(&backup_path).unwrap();
    data[60] ^= 0x01;
    std::fs::write(&backup_path, &data).unwrap();

    let result = Database::restore_key_from_backup(
        &backup_path,
        b"backup-pass",
        b"new-pass",
        &dir.path().join("test.citadel"),
    );
    assert!(result.is_err());
}

#[test]
fn backup_different_passphrase_from_db() {
    let dir = tempfile::tempdir().unwrap();
    let db_pass = b"database-pass";
    let backup_pass = b"totally-different-backup-pass";
    let backup_path = dir.path().join("backup.bin");

    let db = create_test_db(dir.path(), db_pass);

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"secret", b"data").unwrap();
    wtx.commit().unwrap();

    db.export_key_backup(db_pass, backup_pass, &backup_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");
    std::fs::remove_file(&key_path).unwrap();

    Database::restore_key_from_backup(
        &backup_path,
        backup_pass,
        b"yet-another-pass",
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"yet-another-pass")
        .open()
        .unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"secret").unwrap(), Some(b"data".to_vec()));
}

#[test]
fn restore_with_new_passphrase_old_fails() {
    let dir = tempfile::tempdir().unwrap();
    let db_pass = b"old-pass";
    let backup_path = dir.path().join("backup.bin");

    let db = create_test_db(dir.path(), db_pass);
    db.export_key_backup(db_pass, b"backup", &backup_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");
    std::fs::remove_file(&key_path).unwrap();

    let new_pass = b"new-pass";
    Database::restore_key_from_backup(
        &backup_path,
        b"backup",
        new_pass,
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let result = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(db_pass)
        .open();
    assert!(result.is_err());

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(new_pass)
        .open()
        .unwrap();
    drop(db);
}

#[test]
fn backup_preserves_file_id() {
    let dir = tempfile::tempdir().unwrap();
    let db_pass = b"pass";
    let backup_path = dir.path().join("backup.bin");

    let db = create_test_db(dir.path(), db_pass);
    db.export_key_backup(db_pass, b"backup", &backup_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");
    std::fs::remove_file(&key_path).unwrap();

    Database::restore_key_from_backup(
        &backup_path,
        b"backup",
        b"new-pass",
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"new-pass")
        .open()
        .unwrap();
    drop(db);
}

#[test]
fn backup_pbkdf2_database() {
    let dir = tempfile::tempdir().unwrap();
    let db_pass = b"pbkdf2-pass";
    let backup_path = dir.path().join("backup.bin");

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(db_pass)
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000)
        .cache_size(64)
        .create()
        .unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"val").unwrap();
    wtx.commit().unwrap();

    db.export_key_backup(db_pass, b"backup", &backup_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");
    std::fs::remove_file(&key_path).unwrap();

    Database::restore_key_from_backup(
        &backup_path,
        b"backup",
        b"new-pass",
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"new-pass")
        .open()
        .unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"val".to_vec()));
}

#[cfg(not(feature = "fips"))]
#[test]
fn backup_argon2_database() {
    let dir = tempfile::tempdir().unwrap();
    let db_pass = b"argon2-pass";
    let backup_path = dir.path().join("backup.bin");

    let db = create_test_db_argon2(dir.path(), db_pass);

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"val").unwrap();
    wtx.commit().unwrap();

    db.export_key_backup(db_pass, b"backup", &backup_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");
    std::fs::remove_file(&key_path).unwrap();

    Database::restore_key_from_backup(
        &backup_path,
        b"backup",
        b"new-pass",
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"new-pass")
        .open()
        .unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"val".to_vec()));
}

#[test]
fn multiple_backups_same_db() {
    let dir = tempfile::tempdir().unwrap();
    let db_pass = b"db-pass";
    let backup1_path = dir.path().join("backup1.bin");
    let backup2_path = dir.path().join("backup2.bin");

    let db = create_test_db(dir.path(), db_pass);

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"value").unwrap();
    wtx.commit().unwrap();

    db.export_key_backup(db_pass, b"backup-pass-1", &backup1_path)
        .unwrap();
    db.export_key_backup(db_pass, b"backup-pass-2", &backup2_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");

    std::fs::remove_file(&key_path).unwrap();
    Database::restore_key_from_backup(
        &backup1_path,
        b"backup-pass-1",
        b"restored-pass-1",
        &dir.path().join("test.citadel"),
    )
    .unwrap();
    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"restored-pass-1")
        .open()
        .unwrap();
    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"value".to_vec()));
    drop(rtx);
    drop(db);

    std::fs::remove_file(&key_path).unwrap();
    Database::restore_key_from_backup(
        &backup2_path,
        b"backup-pass-2",
        b"restored-pass-2",
        &dir.path().join("test.citadel"),
    )
    .unwrap();
    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"restored-pass-2")
        .open()
        .unwrap();
    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn backup_after_passphrase_change() {
    let dir = tempfile::tempdir().unwrap();
    let old_pass = b"old-pass";
    let new_pass = b"new-pass";
    let backup_path = dir.path().join("backup.bin");

    let db = create_test_db(dir.path(), old_pass);

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"value").unwrap();
    wtx.commit().unwrap();

    db.change_passphrase(old_pass, new_pass).unwrap();

    db.export_key_backup(new_pass, b"backup-pass", &backup_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");
    std::fs::remove_file(&key_path).unwrap();

    Database::restore_key_from_backup(
        &backup_path,
        b"backup-pass",
        b"restored-pass",
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"restored-pass")
        .open()
        .unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn backup_file_size_exact() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let backup_path = dir.path().join("backup.bin");

    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();

    let data = std::fs::read(&backup_path).unwrap();
    assert_eq!(data.len(), 124);
}

#[test]
fn backup_binary_format_magic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let backup_path = dir.path().join("backup.bin");

    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();

    let data = std::fs::read(&backup_path).unwrap();
    let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
    assert_eq!(magic, 0x4B45_5942);
}

#[test]
fn backup_empty_db() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let backup_path = dir.path().join("backup.bin");

    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");
    std::fs::remove_file(&key_path).unwrap();

    Database::restore_key_from_backup(
        &backup_path,
        b"backup",
        b"new-pass",
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"new-pass")
        .open()
        .unwrap();

    assert_eq!(db.stats().entry_count, 0);
}

#[test]
fn backup_large_db() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let backup_path = dir.path().join("backup.bin");

    let mut wtx = db.begin_write().unwrap();
    for i in 0..1000u32 {
        wtx.insert(&i.to_be_bytes(), &format!("val-{i}").into_bytes())
            .unwrap();
    }
    wtx.commit().unwrap();

    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");
    std::fs::remove_file(&key_path).unwrap();

    Database::restore_key_from_backup(
        &backup_path,
        b"backup",
        b"new-pass",
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"new-pass")
        .open()
        .unwrap();

    let mut rtx = db.begin_read();
    for i in 0..1000u32 {
        let val = rtx.get(&i.to_be_bytes()).unwrap().unwrap();
        assert_eq!(val, format!("val-{i}").into_bytes());
    }
}

#[test]
fn restore_overwrite_existing_key_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let backup_path = dir.path().join("backup.bin");

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"val").unwrap();
    wtx.commit().unwrap();

    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();
    drop(db);

    Database::restore_key_from_backup(
        &backup_path,
        b"backup",
        b"new-pass",
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let result = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"pass")
        .open();
    assert!(result.is_err());

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"new-pass")
        .open()
        .unwrap();
    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"val".to_vec()));
}

#[test]
fn restore_refuses_to_race_an_open_database() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.citadel");
    let backup_path = dir.path().join("backup.bin");
    let db = create_test_db(dir.path(), b"pass");
    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();

    let result = Database::restore_key_from_backup(&backup_path, b"backup", b"new-pass", &db_path);
    assert!(matches!(result, Err(citadel::Error::DatabaseLocked)));

    drop(db);
    drop(
        DatabaseBuilder::new(db_path)
            .passphrase(b"pass")
            .open()
            .unwrap(),
    );
}

#[test]
fn restore_refuses_a_missing_live_audit_log_with_retained_history() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.citadel");
    let key_path = dir.path().join("test.citadel.citadel-keys");
    let audit_path = dir.path().join("test.citadel.citadel-audit");
    let rotated_path = dir.path().join("test.citadel.citadel-audit.1");
    let backup_path = dir.path().join("backup.bin");
    let db = create_test_db(dir.path(), b"pass");
    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();
    drop(db);

    std::fs::remove_file(&key_path).unwrap();
    std::fs::rename(&audit_path, &rotated_path).unwrap();
    let error = Database::restore_key_from_backup(&backup_path, b"backup", b"new-pass", &db_path)
        .unwrap_err();

    assert!(error.to_string().contains("rotated generations"));
    assert!(!key_path.exists());
    assert!(!audit_path.exists());
    assert!(rotated_path.exists());
}

#[test]
fn restore_refuses_a_pending_audit_upgrade_before_publishing_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.citadel");
    let key_path = dir.path().join("test.citadel.citadel-keys");
    let audit_path = dir.path().join("test.citadel.citadel-audit");
    let upgrade_path = dir.path().join("test.citadel.citadel-audit.upgrade");
    let backup_path = dir.path().join("backup.bin");
    let db = create_test_db(dir.path(), b"pass");
    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();
    drop(db);

    std::fs::remove_file(&key_path).unwrap();
    std::fs::remove_file(&audit_path).unwrap();
    std::fs::write(&upgrade_path, b"pending upgrade").unwrap();
    let error = Database::restore_key_from_backup(&backup_path, b"backup", b"new-pass", &db_path)
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("upgrade image exists without a live audit log"));
    assert!(!key_path.exists());
    assert_eq!(std::fs::read(upgrade_path).unwrap(), b"pending upgrade");
}

#[test]
fn restore_recovers_an_abandoned_upgrade_beside_the_live_audit() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.citadel");
    let key_path = dir.path().join("test.citadel.citadel-keys");
    let upgrade_path = dir.path().join("test.citadel.citadel-audit.upgrade");
    let backup_path = dir.path().join("backup.bin");
    let db = create_test_db(dir.path(), b"pass");
    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();
    drop(db);

    std::fs::remove_file(&key_path).unwrap();
    std::fs::write(&upgrade_path, b"abandoned upgrade").unwrap();
    Database::restore_key_from_backup(&backup_path, b"backup", b"new-pass", &db_path).unwrap();

    assert!(!upgrade_path.exists());
    drop(
        DatabaseBuilder::new(db_path)
            .passphrase(b"new-pass")
            .open()
            .unwrap(),
    );
}

#[cfg(unix)]
#[test]
fn backup_refuses_a_dangling_destination_audit_sidecar() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let destination = dir.path().join("backup.citadel");
    let destination_key = sidecar(&destination, ".citadel-keys");
    let destination_audit = sidecar(&destination, ".citadel-audit");
    symlink(
        dir.path().join("missing-destination-audit"),
        &destination_audit,
    )
    .unwrap();

    let error = db.backup(&destination).unwrap_err();
    assert!(error.to_string().contains("audit log"));
    assert!(!destination.exists());
    assert!(!destination_key.exists());
    assert!(std::fs::symlink_metadata(&destination_audit)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[cfg(unix)]
#[test]
fn compact_refuses_a_dangling_destination_audit_upgrade() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let destination = dir.path().join("compact.citadel");
    let destination_key = sidecar(&destination, ".citadel-keys");
    let destination_upgrade = sidecar(&destination, ".citadel-audit.upgrade");
    symlink(
        dir.path().join("missing-destination-upgrade"),
        &destination_upgrade,
    )
    .unwrap();

    let error = db.compact(&destination).unwrap_err();
    assert!(error.to_string().contains("audit upgrade image"));
    assert!(!destination.exists());
    assert!(!destination_key.exists());
    assert!(std::fs::symlink_metadata(&destination_upgrade)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[cfg(unix)]
#[test]
fn restore_refuses_a_dangling_live_audit_before_publishing_the_key() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.citadel");
    let key_path = sidecar(&db_path, ".citadel-keys");
    let audit_path = sidecar(&db_path, ".citadel-audit");
    let backup_path = dir.path().join("backup.bin");
    let db = create_test_db(dir.path(), b"pass");
    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();
    drop(db);
    std::fs::remove_file(&key_path).unwrap();
    std::fs::remove_file(&audit_path).unwrap();
    symlink(dir.path().join("missing-live-audit"), &audit_path).unwrap();

    Database::restore_key_from_backup(&backup_path, b"backup", b"new-pass", &db_path).unwrap_err();
    assert!(!key_path.exists());
    assert!(std::fs::symlink_metadata(&audit_path)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[cfg(unix)]
#[test]
fn restore_refuses_a_symlinked_backup_before_publishing_the_key() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.citadel");
    let key_path = sidecar(&db_path, ".citadel-keys");
    let backup_path = dir.path().join("backup.bin");
    let backup_link = dir.path().join("backup-link.bin");
    let db = create_test_db(dir.path(), b"pass");
    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();
    drop(db);
    std::fs::remove_file(&key_path).unwrap();
    symlink(&backup_path, &backup_link).unwrap();

    Database::restore_key_from_backup(&backup_link, b"backup", b"new-pass", &db_path).unwrap_err();

    assert!(!key_path.exists());
    assert!(backup_path.exists());
    assert!(std::fs::symlink_metadata(&backup_link)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[cfg(unix)]
#[test]
fn restore_refuses_a_dangling_key_sidecar_instead_of_replacing_it() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.citadel");
    let key_path = sidecar(&db_path, ".citadel-keys");
    let missing_target = dir.path().join("missing-key-target");
    let backup_path = dir.path().join("backup.bin");
    let db = create_test_db(dir.path(), b"pass");
    db.export_key_backup(b"pass", b"backup", &backup_path)
        .unwrap();
    drop(db);
    std::fs::remove_file(&key_path).unwrap();
    symlink(&missing_target, &key_path).unwrap();

    Database::restore_key_from_backup(&backup_path, b"backup", b"new-pass", &db_path).unwrap_err();
    assert!(!missing_target.exists());
    assert!(std::fs::symlink_metadata(&key_path)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[test]
fn backup_path_does_not_exist() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");

    let result = db.export_key_backup(
        b"pass",
        b"backup",
        &dir.path().join("nonexistent/subdir/backup.bin"),
    );
    assert!(result.is_err());
}

#[test]
fn restore_backup_file_wrong_size() {
    let dir = tempfile::tempdir().unwrap();
    let backup_path = dir.path().join("bad_backup.bin");

    std::fs::write(&backup_path, b"too short").unwrap();

    let result = Database::restore_key_from_backup(
        &backup_path,
        b"backup",
        b"new-pass",
        &dir.path().join("test.citadel"),
    );
    assert!(result.is_err());
}

#[test]
fn backup_named_tables_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db_pass = b"pass";
    let backup_path = dir.path().join("backup.bin");

    let db = create_test_db(dir.path(), db_pass);

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"users").unwrap();
    wtx.table_insert(b"users", b"alice", b"admin").unwrap();
    wtx.create_table(b"logs").unwrap();
    wtx.table_insert(b"logs", b"entry1", b"logged in").unwrap();
    wtx.commit().unwrap();

    db.export_key_backup(db_pass, b"backup", &backup_path)
        .unwrap();
    drop(db);

    let key_path = dir.path().join("test.citadel.citadel-keys");
    std::fs::remove_file(&key_path).unwrap();

    Database::restore_key_from_backup(
        &backup_path,
        b"backup",
        b"new-pass",
        &dir.path().join("test.citadel"),
    )
    .unwrap();

    let db = DatabaseBuilder::new(dir.path().join("test.citadel"))
        .passphrase(b"new-pass")
        .open()
        .unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(
        rtx.table_get(b"users", b"alice").unwrap(),
        Some(b"admin".to_vec())
    );
    assert_eq!(
        rtx.table_get(b"logs", b"entry1").unwrap(),
        Some(b"logged in".to_vec())
    );
}
