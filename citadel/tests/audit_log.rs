#![cfg(feature = "audit-log")]

use std::path::Path;

#[cfg(not(feature = "fips"))]
use citadel::Argon2Profile;
#[cfg(feature = "fips")]
use citadel::KdfAlgorithm;
use citadel::{
    read_audit_log, scan_corrupted_audit_log, verify_audit_log, AuditConfig, AuditEventType,
    Database, DatabaseBuilder,
};

fn test_builder(path: impl Into<std::path::PathBuf>) -> DatabaseBuilder {
    let builder = DatabaseBuilder::new(path).cache_size(64);
    #[cfg(not(feature = "fips"))]
    {
        builder.argon2_profile(Argon2Profile::Iot)
    }
    #[cfg(feature = "fips")]
    {
        builder
            .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000)
    }
}

fn create_test_db(dir: &Path, passphrase: &[u8]) -> Database {
    test_builder(dir.join("test.citadel"))
        .passphrase(passphrase)
        .create()
        .unwrap()
}

fn open_test_db(dir: &Path, passphrase: &[u8]) -> Database {
    test_builder(dir.join("test.citadel"))
        .passphrase(passphrase)
        .open()
        .unwrap()
}

fn audit_path(dir: &Path) -> std::path::PathBuf {
    dir.join("test.citadel.citadel-audit")
}

fn get_audit_key(dir: &Path, passphrase: &[u8]) -> [u8; 32] {
    use citadel::core::KEY_FILE_SIZE;
    use citadel::crypto::key_manager::open_key_file;

    let key_path = dir.join("test.citadel.citadel-keys");
    let key_data = std::fs::read(&key_path).unwrap();
    let key_buf: [u8; KEY_FILE_SIZE] = key_data.try_into().unwrap();
    let (_kf, keys) = open_key_file(&key_buf, passphrase, 0).unwrap_or_else(|_| {
        let data_path = dir.join("test.citadel");
        let mut file = std::fs::File::open(&data_path).unwrap();
        use std::io::{Read, Seek, SeekFrom};
        let mut header_buf = [0u8; citadel::core::FILE_HEADER_SIZE];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut header_buf).unwrap();
        let header = citadel::io::file_manager::FileHeader::deserialize(&header_buf).unwrap();
        open_key_file(&key_buf, passphrase, header.file_id).unwrap()
    });
    keys.audit_key
}

fn create_rotated_history(dir: &Path, passphrase: &[u8]) {
    let config = AuditConfig {
        enabled: true,
        max_file_size: 200,
        max_rotated_files: 3,
    };
    let db = test_builder(dir.join("test.citadel"))
        .passphrase(passphrase)
        .audit_config(config)
        .create()
        .unwrap();
    for _ in 0..14 {
        db.integrity_check().unwrap();
    }
    drop(db);

    assert!(audit_path(dir).with_extension("citadel-audit.1").exists());
    assert!(audit_path(dir).with_extension("citadel-audit.2").exists());
}

#[test]
fn audit_log_created_on_db_create() {
    let dir = tempfile::tempdir().unwrap();
    let _db = create_test_db(dir.path(), b"pass");
    assert!(audit_path(dir.path()).exists());
}

#[test]
fn audit_log_records_create_event() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    assert!(entries.len() >= 2);
    assert_eq!(entries[0].event_type, AuditEventType::DatabaseCreated);
    assert_eq!(entries[0].detail.len(), 2);
}

#[test]
fn audit_log_records_open_event() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    drop(db);

    let db = open_test_db(dir.path(), b"pass");
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let open_events: Vec<_> = entries
        .iter()
        .filter(|e| e.event_type == AuditEventType::DatabaseOpened)
        .collect();
    assert_eq!(open_events.len(), 1);
}

#[test]
fn audit_log_records_close_event() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let close_events: Vec<_> = entries
        .iter()
        .filter(|e| e.event_type == AuditEventType::DatabaseClosed)
        .collect();
    assert_eq!(close_events.len(), 1);
}

#[test]
fn audit_log_passphrase_change() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"old");
    db.change_passphrase(b"old", b"new").unwrap();
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let change_events: Vec<_> = entries
        .iter()
        .filter(|e| e.event_type == AuditEventType::PassphraseChanged)
        .collect();
    assert_eq!(change_events.len(), 1);
}

#[test]
fn audit_failure_reports_that_the_passphrase_change_already_completed() {
    let dir = tempfile::tempdir().unwrap();
    let config = AuditConfig {
        enabled: true,
        max_file_size: 65,
        max_rotated_files: 3,
    };
    let db = test_builder(dir.path().join("test.citadel"))
        .passphrase(b"old")
        .audit_config(config)
        .create()
        .unwrap();

    let mut work_name = audit_path(dir.path()).as_os_str().to_os_string();
    work_name.push(".rotation-work");
    let work = std::path::PathBuf::from(work_name);
    std::fs::create_dir(&work).unwrap();
    std::fs::write(work.join("unknown"), b"blocks recovery").unwrap();

    let error = db.change_passphrase(b"old", b"new").unwrap_err();
    match error {
        citadel::Error::AuditFailureAfterOperation { operation, .. } => {
            assert_eq!(operation, "passphrase change");
        }
        other => panic!("unexpected error: {other}"),
    }
    assert!(db.verify_passphrase(b"new").unwrap());
    assert!(!db.verify_passphrase(b"old").unwrap());

    drop(db);
    std::fs::remove_file(work.join("unknown")).unwrap();
    std::fs::remove_dir(work).unwrap();
    drop(open_test_db(dir.path(), b"new"));
}

#[test]
fn audit_log_key_backup_export() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let backup_path = dir.path().join("backup.citadel-key-backup");
    db.export_key_backup(b"pass", b"backup-pass", &backup_path)
        .unwrap();
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let export_events: Vec<_> = entries
        .iter()
        .filter(|e| e.event_type == AuditEventType::KeyBackupExported)
        .collect();
    assert_eq!(export_events.len(), 1);
    assert!(!export_events[0].detail.is_empty());
}

#[test]
fn audit_log_backup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let backup_dest = dir.path().join("backup.citadel");
    db.backup(&backup_dest).unwrap();
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let backup_events: Vec<_> = entries
        .iter()
        .filter(|e| e.event_type == AuditEventType::BackupCreated)
        .collect();
    assert_eq!(backup_events.len(), 1);
    assert!(!backup_events[0].detail.is_empty());
}

#[test]
fn audit_log_compact() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key1", b"value1").unwrap();
    wtx.commit().unwrap();

    let compact_dest = dir.path().join("compact.citadel");
    db.compact(&compact_dest).unwrap();
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let compact_events: Vec<_> = entries
        .iter()
        .filter(|e| e.event_type == AuditEventType::CompactionPerformed)
        .collect();
    assert_eq!(compact_events.len(), 1);
}

#[test]
fn audit_log_integrity_check() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let report = db.integrity_check().unwrap();
    assert!(report.errors.is_empty());
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let check_events: Vec<_> = entries
        .iter()
        .filter(|e| e.event_type == AuditEventType::IntegrityCheckPerformed)
        .collect();
    assert_eq!(check_events.len(), 1);
    let error_count = u32::from_le_bytes(check_events[0].detail[..4].try_into().unwrap());
    assert_eq!(error_count, 0);
}

#[test]
fn audit_chain_verification_valid() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let db = create_test_db(dir.path(), pass);

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"k", b"v").unwrap();
    wtx.commit().unwrap();

    db.integrity_check().unwrap();
    drop(db);

    let audit_key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&audit_path(dir.path()), &audit_key).unwrap();
    assert!(result.chain_valid);
    assert!(result.entries_verified >= 3);
    assert!(result.chain_break_at.is_none());
}

#[test]
fn audit_chain_tamper_detected() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let db = create_test_db(dir.path(), pass);
    drop(db);

    let ap = audit_path(dir.path());
    let mut data = std::fs::read(&ap).unwrap();
    if data.len() > 74 {
        data[73] ^= 0x01;
        std::fs::write(&ap, &data).unwrap();
    }

    let audit_key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&ap, &audit_key).unwrap();
    assert!(!result.chain_valid);
    assert!(result.chain_break_at.is_some());
}

#[test]
fn audit_wrong_key_fails_verification() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    drop(db);

    let wrong_key = [0xFFu8; 32];
    let result = verify_audit_log(&audit_path(dir.path()), &wrong_key).unwrap();
    assert!(!result.chain_valid);
}

#[test]
fn audit_disabled_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let config = AuditConfig {
        enabled: false,
        max_file_size: 10 * 1024 * 1024,
        max_rotated_files: 3,
    };
    let db = test_builder(dir.path().join("test.citadel"))
        .passphrase(b"pass")
        .audit_config(config)
        .create()
        .unwrap();
    drop(db);
    assert!(!audit_path(dir.path()).exists());
}

#[test]
fn audit_in_memory_no_log() {
    let dir = tempfile::tempdir().unwrap();
    let db = test_builder(dir.path().join("test.citadel"))
        .passphrase(b"pass")
        .create_in_memory()
        .unwrap();
    drop(db);
    assert!(!audit_path(dir.path()).exists());
}

#[test]
fn audit_sequence_numbers_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    db.integrity_check().unwrap();
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    assert!(entries.len() >= 2);
    for i in 1..entries.len() {
        assert!(
            entries[i].sequence_no > entries[i - 1].sequence_no,
            "sequence numbers must be strictly increasing"
        );
    }
}

#[test]
fn audit_timestamps_nondecreasing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    db.integrity_check().unwrap();
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    for i in 1..entries.len() {
        assert!(
            entries[i].timestamp >= entries[i - 1].timestamp,
            "timestamps must be non-decreasing"
        );
    }
}

#[test]
fn read_audit_log_without_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    assert!(entries.len() >= 2);
    for entry in &entries {
        assert!(entry.timestamp > 0);
        assert!(entry.sequence_no > 0);
    }
}

#[test]
fn audit_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    drop(db);

    let db = open_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let event_types: Vec<_> = entries.iter().map(|e| e.event_type).collect();
    assert!(event_types.contains(&AuditEventType::DatabaseCreated));
    assert!(event_types.contains(&AuditEventType::DatabaseOpened));
    assert!(event_types.contains(&AuditEventType::IntegrityCheckPerformed));

    let close_count = event_types
        .iter()
        .filter(|&&et| et == AuditEventType::DatabaseClosed)
        .count();
    assert_eq!(close_count, 2);

    let audit_key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&audit_path(dir.path()), &audit_key).unwrap();
    assert!(result.chain_valid);
}

#[test]
fn audit_multiple_operations() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key1", b"value1").unwrap();
    wtx.commit().unwrap();

    db.change_passphrase(pass, b"newpass").unwrap();

    let backup_path = dir.path().join("backup.citadel");
    db.backup(&backup_path).unwrap();

    db.integrity_check().unwrap();

    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    assert!(entries.len() >= 5);

    let event_types: Vec<_> = entries.iter().map(|e| e.event_type).collect();
    assert!(event_types.contains(&AuditEventType::DatabaseCreated));
    assert!(event_types.contains(&AuditEventType::PassphraseChanged));
    assert!(event_types.contains(&AuditEventType::BackupCreated));
    assert!(event_types.contains(&AuditEventType::IntegrityCheckPerformed));
    assert!(event_types.contains(&AuditEventType::DatabaseClosed));
}

#[test]
fn audit_file_format_magic() {
    let dir = tempfile::tempdir().unwrap();
    let _db = create_test_db(dir.path(), b"pass");

    let data = std::fs::read(audit_path(dir.path())).unwrap();
    let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
    assert_eq!(magic, 0x4155_4454);
}

#[test]
fn current_database_rejects_a_v1_audit_header_downgrade() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    drop(create_test_db(dir.path(), pass));

    let path = audit_path(dir.path());
    let mut bytes = std::fs::read(&path).unwrap();
    assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 2);
    assert_eq!(&bytes[32..64], &[0u8; 32]);
    bytes[4..8].copy_from_slice(&1u32.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();

    // A first-generation v2 file seeds from zero, as v1 verification does, so
    // the records still verify if the mutable version word is considered alone.
    let audit_key = get_audit_key(dir.path(), pass);
    assert!(verify_audit_log(&path, &audit_key).unwrap().chain_valid);

    let error = match test_builder(dir.path().join("test.citadel"))
        .passphrase(pass)
        .open()
    {
        Ok(db) => {
            drop(db);
            panic!("a current database accepted a downgraded audit header")
        }
        Err(error) => error,
    };
    assert!(error.to_string().contains("audit-log downgrade detected"));
}

#[test]
fn audit_entry_detail_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let create_entry = entries
        .iter()
        .find(|e| e.event_type == AuditEventType::DatabaseCreated)
        .unwrap();

    assert_eq!(create_entry.detail.len(), 2);
    assert_eq!(create_entry.detail[0], 0); // cipher_id
    let expected_kdf = if cfg!(feature = "fips") { 1 } else { 0 };
    assert_eq!(create_entry.detail[1], expected_kdf);
}

#[cfg(unix)]
#[test]
fn open_refuses_key_and_audit_symlinks_even_when_the_targets_are_valid() {
    use std::os::unix::fs::symlink;

    for suffix in [".citadel-keys", ".citadel-audit"] {
        let dir = tempfile::tempdir().unwrap();
        drop(create_test_db(dir.path(), b"pass"));
        let data = dir.path().join("test.citadel");
        let sidecar = dir.path().join(format!("test.citadel{suffix}"));
        let moved = dir.path().join("moved-sidecar");
        std::fs::rename(&sidecar, &moved).unwrap();
        symlink(&moved, &sidecar).unwrap();
        let before = std::fs::read(&moved).unwrap();

        test_builder(data)
            .passphrase(b"pass")
            .open()
            .expect_err("storage sidecars must not be followed through symlinks");
        assert_eq!(std::fs::read(&moved).unwrap(), before);
        assert!(std::fs::symlink_metadata(&sidecar)
            .unwrap()
            .file_type()
            .is_symlink());
    }
}

#[test]
fn audit_rotation_triggers_on_size() {
    let dir = tempfile::tempdir().unwrap();
    let config = AuditConfig {
        enabled: true,
        max_file_size: 200,
        max_rotated_files: 2,
    };

    let db = test_builder(dir.path().join("test.citadel"))
        .passphrase(b"pass")
        .audit_config(config)
        .create()
        .unwrap();

    for _ in 0..10 {
        db.integrity_check().unwrap();
    }
    drop(db);

    let rotated_1 = dir.path().join("test.citadel.citadel-audit.1");
    assert!(rotated_1.exists());
    assert!(audit_path(dir.path()).exists());
}

#[test]
fn audit_rotation_deletes_old_files() {
    let dir = tempfile::tempdir().unwrap();
    let config = AuditConfig {
        enabled: true,
        max_file_size: 200,
        max_rotated_files: 1,
    };

    let db = test_builder(dir.path().join("test.citadel"))
        .passphrase(b"pass")
        .audit_config(config)
        .create()
        .unwrap();

    for _ in 0..20 {
        db.integrity_check().unwrap();
    }
    drop(db);

    let rotated_1 = dir.path().join("test.citadel.citadel-audit.1");
    let rotated_2 = dir.path().join("test.citadel.citadel-audit.2");
    assert!(rotated_1.exists());
    assert!(!rotated_2.exists());
}

#[test]
fn audit_verify_chain_valid_after_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    let config = AuditConfig {
        enabled: true,
        max_file_size: 200,
        max_rotated_files: 2,
    };

    let db = test_builder(dir.path().join("test.citadel"))
        .passphrase(pass)
        .audit_config(config)
        .create()
        .unwrap();

    for _ in 0..10 {
        db.integrity_check().unwrap();
    }
    drop(db);

    let rotated_1 = dir.path().join("test.citadel.citadel-audit.1");
    assert!(rotated_1.exists());

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    assert!(!entries.is_empty());

    let audit_key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&ap, &audit_key).unwrap();
    assert!(result.chain_valid);
    assert_eq!(result.entries_verified, entries.len() as u64);
    assert!(result.chain_break_at.is_none());

    let rotated_result = verify_audit_log(&rotated_1, &audit_key).unwrap();
    assert!(rotated_result.chain_valid);
}

#[test]
fn audit_chain_detects_a_missing_rotation_generation() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    create_rotated_history(dir.path(), pass);

    let rotated_1 = dir.path().join("test.citadel.citadel-audit.1");
    let rotated_2 = dir.path().join("test.citadel.citadel-audit.2");
    std::fs::remove_file(&rotated_1).unwrap();

    let db = open_test_db(dir.path(), pass);
    let paths = db.audit_log_paths().unwrap();
    assert!(
        paths.contains(&rotated_2),
        "discovery must not stop at the missing .1 and hide .2"
    );
    let verified = db.verify_audit_chain().unwrap();
    assert!(
        verified.iter().any(|(_, result)| !result.chain_valid),
        "a generation gap must break whole-history verification"
    );
}

#[test]
fn open_refuses_to_replace_a_missing_live_log_beside_retained_history() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    create_rotated_history(dir.path(), pass);

    let live = audit_path(dir.path());
    let retained = live.with_extension("citadel-audit.1");
    assert!(retained.exists());
    std::fs::remove_file(&live).unwrap();

    let error = match test_builder(dir.path().join("test.citadel"))
        .passphrase(pass)
        .open()
    {
        Ok(db) => {
            drop(db);
            panic!("missing live audit history was silently replaced")
        }
        Err(error) => error,
    };
    assert!(error.to_string().contains("rotated generations"));
    assert!(
        !live.exists(),
        "the refused open created a detached live log"
    );
    assert!(retained.exists());
}

#[test]
fn audit_chain_detects_reordered_individually_valid_segments() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    create_rotated_history(dir.path(), pass);

    let rotated_1 = dir.path().join("test.citadel.citadel-audit.1");
    let rotated_2 = dir.path().join("test.citadel.citadel-audit.2");
    let swap = dir.path().join("audit-swap.tmp");
    std::fs::rename(&rotated_1, &swap).unwrap();
    std::fs::rename(&rotated_2, &rotated_1).unwrap();
    std::fs::rename(&swap, &rotated_2).unwrap();

    let audit_key = get_audit_key(dir.path(), pass);
    assert!(
        verify_audit_log(&rotated_1, &audit_key)
            .unwrap()
            .chain_valid
    );
    assert!(
        verify_audit_log(&rotated_2, &audit_key)
            .unwrap()
            .chain_valid
    );

    let db = open_test_db(dir.path(), pass);
    let verified = db.verify_audit_chain().unwrap();
    assert!(
        verified.iter().any(|(_, result)| !result.chain_valid),
        "per-file verification must not substitute for seed handoff and sequence order"
    );
}

#[test]
fn audit_chain_checks_every_segment_file_identity() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    create_rotated_history(dir.path(), pass);

    let rotated_1 = dir.path().join("test.citadel.citadel-audit.1");
    let mut bytes = std::fs::read(&rotated_1).unwrap();
    let file_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    bytes[8..16].copy_from_slice(&(file_id ^ 1).to_le_bytes());
    std::fs::write(&rotated_1, bytes).unwrap();

    let audit_key = get_audit_key(dir.path(), pass);
    assert!(
        verify_audit_log(&rotated_1, &audit_key)
            .unwrap()
            .chain_valid,
        "the legacy single-file API does not know the expected database identity"
    );

    let db = open_test_db(dir.path(), pass);
    let verified = db.verify_audit_chain().unwrap();
    let (_, result) = verified
        .iter()
        .find(|(path, _)| path == &rotated_1)
        .unwrap();
    assert!(!result.chain_valid);
}

#[test]
fn audit_chain_detects_count_patched_tail_cut_in_a_rotated_segment() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";
    create_rotated_history(dir.path(), pass);

    let rotated_1 = dir.path().join("test.citadel.citadel-audit.1");
    let entries = read_audit_log(&rotated_1).unwrap();
    assert!(entries.len() >= 2);
    let bytes = std::fs::read(&rotated_1).unwrap();
    let mut end = 64usize;
    for _ in 0..entries.len() - 1 {
        end += 4;
        let len = u32::from_le_bytes(bytes[end..end + 4].try_into().unwrap()) as usize;
        end += len;
    }
    let mut truncated = bytes[..end].to_vec();
    let declared = u64::from_le_bytes(truncated[24..32].try_into().unwrap());
    truncated[24..32].copy_from_slice(&(declared - 1).to_le_bytes());
    std::fs::write(&rotated_1, truncated).unwrap();

    let audit_key = get_audit_key(dir.path(), pass);
    let local = verify_audit_log(&rotated_1, &audit_key).unwrap();
    assert!(local.chain_valid);
    assert_eq!(local.entries_missing(), 0);

    let db = open_test_db(dir.path(), pass);
    let verified = db.verify_audit_chain().unwrap();
    assert!(
        verified.iter().any(|(_, result)| !result.chain_valid),
        "the successor seed must expose a patched-count tail cut in its predecessor"
    );
}

#[test]
fn audit_torn_trailing_record_truncated_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    drop(db);

    let ap = audit_path(dir.path());
    let entries_before = read_audit_log(&ap).unwrap();

    // Simulate a torn write: crash after log() wrote only the entry magic.
    let mut data = std::fs::read(&ap).unwrap();
    data.extend_from_slice(&0x454E_5452u32.to_le_bytes());
    std::fs::write(&ap, &data).unwrap();

    let db = open_test_db(dir.path(), pass);
    drop(db);

    // The DatabaseOpened/DatabaseClosed entries appended after the torn
    // record must be visible and the chain must verify end to end.
    let entries = read_audit_log(&ap).unwrap();
    assert_eq!(entries.len(), entries_before.len() + 2);
    for i in 1..entries.len() {
        assert_eq!(entries[i].sequence_no, entries[i - 1].sequence_no + 1);
    }
    assert_eq!(
        entries[entries_before.len()].event_type,
        AuditEventType::DatabaseOpened
    );

    let audit_key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&ap, &audit_key).unwrap();
    assert!(result.chain_valid);
    assert_eq!(result.entries_verified, entries.len() as u64);
}

#[test]
fn audit_log_path_method() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    let path = db.audit_log_path();
    assert!(path.is_some());
    assert_eq!(path.unwrap(), audit_path(dir.path()));
}

#[test]
fn scenario_full_lifecycle_audit_trail() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"initial-pass";

    let db = create_test_db(dir.path(), pass);
    let mut wtx = db.begin_write().unwrap();
    for i in 0..100u32 {
        wtx.insert(&i.to_be_bytes(), &[i as u8; 64]).unwrap();
    }
    wtx.commit().unwrap();

    db.change_passphrase(pass, b"new-pass").unwrap();
    let pass = b"new-pass";

    let report = db.integrity_check().unwrap();
    assert!(report.errors.is_empty());

    let backup_path = dir.path().join("backup.citadel");
    db.backup(&backup_path).unwrap();

    let key_backup_path = dir.path().join("key.backup");
    db.export_key_backup(pass, b"backup-pw", &key_backup_path)
        .unwrap();

    let compact_path = dir.path().join("compact.citadel");
    db.compact(&compact_path).unwrap();

    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let event_types: Vec<AuditEventType> = entries.iter().map(|e| e.event_type).collect();

    assert!(event_types.contains(&AuditEventType::DatabaseCreated));
    assert!(event_types.contains(&AuditEventType::PassphraseChanged));
    assert!(event_types.contains(&AuditEventType::IntegrityCheckPerformed));
    assert!(event_types.contains(&AuditEventType::BackupCreated));
    assert!(event_types.contains(&AuditEventType::KeyBackupExported));
    assert!(event_types.contains(&AuditEventType::CompactionPerformed));
    assert!(event_types.contains(&AuditEventType::DatabaseClosed));

    for i in 1..entries.len() {
        assert!(entries[i].sequence_no == entries[i - 1].sequence_no + 1);
    }

    assert_eq!(event_types[0], AuditEventType::DatabaseCreated);
    assert_eq!(*event_types.last().unwrap(), AuditEventType::DatabaseClosed);

    let audit_key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&audit_path(dir.path()), &audit_key).unwrap();
    assert!(result.chain_valid);
    assert_eq!(result.entries_verified, entries.len() as u64);

    let db = open_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    drop(db);

    let entries_after = read_audit_log(&audit_path(dir.path())).unwrap();
    assert!(entries_after.len() > entries.len());

    let result = verify_audit_log(&audit_path(dir.path()), &audit_key).unwrap();
    assert!(result.chain_valid);

    let last_old_seq = entries.last().unwrap().sequence_no;
    let first_new_seq = entries_after[entries.len()].sequence_no;
    assert_eq!(first_new_seq, last_old_seq + 1);
}

#[test]
fn scenario_tamper_middle_of_chain_detected() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    db.integrity_check().unwrap();
    db.integrity_check().unwrap();
    drop(db);

    let ap = audit_path(dir.path());
    let entries_before = read_audit_log(&ap).unwrap();
    assert!(entries_before.len() >= 5);

    // Tamper with the 3rd entry (index 2), not the first or last
    let data = std::fs::read(&ap).unwrap();
    let audit_key = get_audit_key(dir.path(), pass);

    let pre_result = verify_audit_log(&ap, &audit_key).unwrap();
    assert!(pre_result.chain_valid);

    let mut offset = 64usize;
    for _ in 0..2 {
        offset += 4; // skip magic
        let entry_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += entry_len;
    }

    let mut tampered = data.clone();
    tampered[offset + 4 + 5] ^= 0xFF;
    std::fs::write(&ap, &tampered).unwrap();

    let result = verify_audit_log(&ap, &audit_key).unwrap();
    assert!(!result.chain_valid);
    assert_eq!(result.entries_verified, 2);
    assert_eq!(result.chain_break_at, Some(3));
}

#[test]
fn scenario_truncation_detected_by_count() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    drop(db);

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    let original_count = entries.len();
    assert!(original_count >= 3);

    let data = std::fs::read(&ap).unwrap();
    let last_entry = &entries[original_count - 1];
    assert_eq!(last_entry.event_type, AuditEventType::DatabaseClosed);

    let mut offset = 64usize;
    for _ in 0..original_count - 1 {
        offset += 4;
        let entry_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += entry_len;
    }
    std::fs::write(&ap, &data[..offset]).unwrap();

    let entries_after = read_audit_log(&ap).unwrap();
    assert_eq!(entries_after.len(), original_count - 1);

    // Cutting the tail takes the linking MAC with it, so the remainder is a
    // shorter chain that verifies. A count left behind exposes this particular
    // uncoordinated cut, but the count itself is not authenticated.
    let audit_key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&ap, &audit_key).unwrap();
    assert!(result.chain_valid);
    assert_eq!(result.entries_verified, (original_count - 1) as u64);
    assert_eq!(result.entries_declared, original_count as u64);
    assert_eq!(result.entries_missing(), 1);
}

#[test]
fn truncated_log_reports_its_shortfall_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    drop(db);

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    let original_count = entries.len();
    assert!(original_count >= 3);

    let data = std::fs::read(&ap).unwrap();
    let mut offset = 64usize;
    for _ in 0..original_count - 1 {
        offset += 4;
        let entry_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += entry_len;
    }
    std::fs::write(&ap, &data[..offset]).unwrap();

    // Opening recounts and the open event rewrites the header, so a shortfall is
    // legible for one moment. This surface keeps that best-effort signal.
    let db = open_test_db(dir.path(), pass);
    assert_eq!(db.audit_entries_missing(), Some(1));

    // The chain itself still verifies: this mismatch is a separate consistency
    // warning, not authenticated proof of what happened.
    let verified = db.verify_audit_chain().unwrap();
    assert!(verified.iter().all(|(_, r)| r.chain_valid));
}

#[test]
fn an_untouched_log_reports_no_shortfall() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    drop(db);

    let db = open_test_db(dir.path(), pass);
    assert_eq!(db.audit_entries_missing(), Some(0));
    for (_, result) in db.verify_audit_chain().unwrap() {
        assert_eq!(result.entries_missing(), 0);
    }
}

#[test]
fn a_torn_trailing_write_is_not_a_shortfall() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    drop(db);

    // An entry is synced before the header count that covers it, so a crash
    // between the two leaves more records than the header declares. Benign, and
    // the opposite direction from a removal.
    let ap = audit_path(dir.path());
    let data = std::fs::read(&ap).unwrap();
    let count = u64::from_le_bytes(data[24..32].try_into().unwrap());
    let mut patched = data.clone();
    patched[24..32].copy_from_slice(&(count - 1).to_le_bytes());
    std::fs::write(&ap, &patched).unwrap();

    let db = open_test_db(dir.path(), pass);
    assert_eq!(db.audit_entries_missing(), Some(0));
}

#[test]
fn scenario_insertion_attack_detected() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    drop(db);

    let ap = audit_path(dir.path());
    let data = std::fs::read(&ap).unwrap();
    let audit_key = get_audit_key(dir.path(), pass);

    let result_before = verify_audit_log(&ap, &audit_key).unwrap();
    assert!(result_before.chain_valid);
    let count_before = result_before.entries_verified;

    let fake_entry_len: u32 = 56;
    let mut fake = Vec::new();
    fake.extend_from_slice(&0x454E_5452u32.to_le_bytes());
    fake.extend_from_slice(&fake_entry_len.to_le_bytes());
    fake.extend_from_slice(&999u64.to_le_bytes());
    fake.extend_from_slice(&(count_before + 1).to_le_bytes());
    fake.extend_from_slice(&2u16.to_le_bytes());
    fake.extend_from_slice(&0u16.to_le_bytes());
    fake.extend_from_slice(&[0xAA; 32]);

    let mut tampered = data.clone();
    tampered.extend_from_slice(&fake);
    std::fs::write(&ap, &tampered).unwrap();

    let result = verify_audit_log(&ap, &audit_key).unwrap();
    assert!(!result.chain_valid);
    assert_eq!(result.entries_verified, count_before);
}

#[test]
fn scenario_multiple_sessions_chain_continuity() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key1", b"val1").unwrap();
    wtx.commit().unwrap();
    drop(db);

    let db = open_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    drop(db);

    let db = open_test_db(dir.path(), pass);
    db.change_passphrase(pass, b"pass2").unwrap();
    drop(db);
    let pass = b"pass2";

    let db = open_test_db(dir.path(), pass);
    let backup_path = dir.path().join("backup.citadel");
    db.backup(&backup_path).unwrap();
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();

    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence_no, (i + 1) as u64);
    }

    let created = entries
        .iter()
        .filter(|e| e.event_type == AuditEventType::DatabaseCreated)
        .count();
    let opened = entries
        .iter()
        .filter(|e| e.event_type == AuditEventType::DatabaseOpened)
        .count();
    let closed = entries
        .iter()
        .filter(|e| e.event_type == AuditEventType::DatabaseClosed)
        .count();

    assert_eq!(created, 1);
    assert_eq!(opened, 3);
    assert_eq!(closed, 4);

    let audit_key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&audit_path(dir.path()), &audit_key).unwrap();
    assert!(result.chain_valid);
    assert_eq!(result.entries_verified, entries.len() as u64);
}

#[test]
fn scenario_corrupted_entry_recovery_via_sentinel() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    db.integrity_check().unwrap();
    db.integrity_check().unwrap();
    db.integrity_check().unwrap();
    db.integrity_check().unwrap();
    drop(db);

    let ap = audit_path(dir.path());
    let entries_before = read_audit_log(&ap).unwrap();
    let total = entries_before.len();
    assert!(total >= 7);

    let mut data = std::fs::read(&ap).unwrap();

    let mut offset = 64usize;
    for _ in 0..2 {
        offset += 4;
        let entry_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += entry_len;
    }
    data[offset + 4] = 0xFF;
    data[offset + 5] = 0xFF;
    data[offset + 6] = 0xFF;
    data[offset + 7] = 0x7F;
    std::fs::write(&ap, &data).unwrap();

    // read_audit_log resyncs past the damaged record like the writer does,
    // so every intact entry stays visible; only the corrupted one is lost.
    let entries_after = read_audit_log(&ap).unwrap();
    assert_eq!(entries_after.len(), total - 1);

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), entries_after.len());
    assert!(!scan.corruption_offsets.is_empty());

    let recovered_seq_nos: Vec<u64> = entries_after.iter().map(|e| e.sequence_no).collect();
    assert!(recovered_seq_nos.contains(&1));
    assert!(recovered_seq_nos.contains(&2));
    let recovered_past_gap = recovered_seq_nos.iter().filter(|&&s| s >= 4).count();
    assert!(recovered_past_gap > 0);
}

#[test]
fn scenario_torn_write_partial_last_entry() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    drop(db);

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    let original_count = entries.len();

    let data = std::fs::read(&ap).unwrap();
    let truncated_len = data.len() - 10;
    std::fs::write(&ap, &data[..truncated_len]).unwrap();

    let entries_after = read_audit_log(&ap).unwrap();
    assert_eq!(entries_after.len(), original_count - 1);

    let audit_key = get_audit_key(dir.path(), pass);
    let result = verify_audit_log(&ap, &audit_key).unwrap();
    assert!(result.chain_valid);
    assert_eq!(result.entries_verified, (original_count - 1) as u64);
}

#[test]
fn scenario_zeroed_magic_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let pass = b"pass";

    let db = create_test_db(dir.path(), pass);
    db.integrity_check().unwrap();
    db.integrity_check().unwrap();
    db.integrity_check().unwrap();
    drop(db);

    let ap = audit_path(dir.path());
    let entries = read_audit_log(&ap).unwrap();
    let total = entries.len();
    assert!(total >= 5);

    let mut data = std::fs::read(&ap).unwrap();
    let mut offset = 64usize;
    offset += 4;
    let entry_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += entry_len;
    data[offset] = 0;
    data[offset + 1] = 0;
    data[offset + 2] = 0;
    data[offset + 3] = 0;
    std::fs::write(&ap, &data).unwrap();

    // The zeroed-magic record is lost, but read_audit_log resyncs and keeps
    // every other entry visible, matching the scanner's recovery.
    let entries_after = read_audit_log(&ap).unwrap();
    assert_eq!(entries_after.len(), total - 1);

    let scan = scan_corrupted_audit_log(&ap).unwrap();
    assert_eq!(scan.entries.len(), entries_after.len());
    assert!(!scan.corruption_offsets.is_empty());

    let recovered_seq: Vec<u64> = entries_after.iter().map(|e| e.sequence_no).collect();
    assert!(recovered_seq.contains(&1));
    for seq in 3..=total as u64 {
        assert!(
            recovered_seq.contains(&seq),
            "entry {} should be recovered",
            seq
        );
    }
}

#[test]
fn scenario_entry_sentinel_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");
    drop(db);

    let ap = audit_path(dir.path());
    let data = std::fs::read(&ap).unwrap();

    let magic = u32::from_le_bytes(data[64..68].try_into().unwrap());
    assert_eq!(magic, 0x454E_5452);

    let entry_len = u32::from_le_bytes(data[68..72].try_into().unwrap()) as usize;
    let second_offset = 64 + 4 + entry_len;
    let magic2 = u32::from_le_bytes(data[second_offset..second_offset + 4].try_into().unwrap());
    assert_eq!(magic2, 0x454E_5452);
}

#[test]
fn scenario_path_detail_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_test_db(dir.path(), b"pass");

    let backup_path = dir.path().join("my-backup.citadel");
    db.backup(&backup_path).unwrap();
    drop(db);

    let entries = read_audit_log(&audit_path(dir.path())).unwrap();
    let backup_entry = entries
        .iter()
        .find(|e| e.event_type == AuditEventType::BackupCreated)
        .unwrap();

    assert!(backup_entry.detail.len() >= 2);
    let path_len = u16::from_le_bytes(backup_entry.detail[0..2].try_into().unwrap()) as usize;
    assert_eq!(path_len, backup_entry.detail.len() - 2);
    let path_str = std::str::from_utf8(&backup_entry.detail[2..]).unwrap();
    assert!(path_str.contains("my-backup.citadel"));
}
