#![cfg(feature = "audit-log")]

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use citadel::core::{
    AUDIT_LOG_VERSION, AUDIT_LOG_VERSION_LEGACY, FILE_HEADER_SIZE, KEY_FILE_SIZE, SLOT_MAC_SIZE,
};
use citadel::crypto::kdf::derive_mk;
use citadel::crypto::key_manager::{
    KeyFile, KEY_FILE_FLAG_AUDIT_V2_REQUIRED, KEY_FILE_FLAG_SLOTS_V1_REQUIRED,
};
use citadel::io::file_manager::{FileHeader, SlotFormat};
use citadel::{Argon2Profile, AuditConfig, Database, DatabaseBuilder, Error};

const PASS: &[u8] = b"correct horse";

fn builder(path: &Path, passphrase: &[u8]) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(passphrase)
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(64)
}

fn key_path(data_path: &Path) -> std::path::PathBuf {
    let mut name = data_path.as_os_str().to_os_string();
    name.push(".citadel-keys");
    name.into()
}

fn read_key_file(path: &Path) -> KeyFile {
    let bytes = std::fs::read(path).unwrap();
    let image: [u8; KEY_FILE_SIZE] = bytes.try_into().unwrap();
    KeyFile::deserialize(&image).unwrap()
}

fn rewrite_key_file_as_released_legacy(path: &Path, passphrase: &[u8]) {
    let mut key_file = read_key_file(path);
    let mk = derive_mk(
        key_file.kdf_algorithm,
        passphrase,
        &key_file.argon2_salt,
        key_file.argon2_m_cost,
        key_file.argon2_t_cost,
        key_file.argon2_p_cost,
    )
    .unwrap();
    key_file.flags = 0;
    key_file.update_mac(&mk).unwrap();
    std::fs::write(path, key_file.serialize()).unwrap();
}

fn rewrite_slots_as_legacy_and_clear_header(path: &Path) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut image = [0u8; FILE_HEADER_SIZE];
    file.read_exact(&mut image).unwrap();
    let mut header = FileHeader::deserialize(&image).unwrap();
    header.flags = 0;
    for slot in &mut header.slots {
        slot.slot_format = SlotFormat::Legacy;
        slot.slot_mac = [0u8; SLOT_MAC_SIZE];
    }
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&header.serialize()).unwrap();
    file.sync_all().unwrap();
}

fn rewrite_audit_header_as_legacy(path: &Path) {
    let mut audit_name = path.as_os_str().to_os_string();
    audit_name.push(".citadel-audit");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(Path::new(&audit_name))
        .unwrap();
    file.seek(SeekFrom::Start(4)).unwrap();
    file.write_all(&AUDIT_LOG_VERSION_LEGACY.to_le_bytes())
        .unwrap();
    file.sync_all().unwrap();
}

fn read_audit_version(path: &Path) -> u32 {
    let mut audit_name = path.as_os_str().to_os_string();
    audit_name.push(".citadel-audit");
    let mut file = OpenOptions::new()
        .read(true)
        .open(Path::new(&audit_name))
        .unwrap();
    file.seek(SeekFrom::Start(4)).unwrap();
    let mut version = [0u8; 4];
    file.read_exact(&mut version).unwrap();
    u32::from_le_bytes(version)
}

fn assert_downgrade_rejected(path: &Path, passphrase: &[u8]) {
    let result = builder(path, passphrase).open();
    assert!(matches!(result, Err(Error::SlotDowngradeDetected)));
}

#[test]
fn a_new_vault_authenticates_the_v1_requirement() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("new.citadel");
    let key = key_path(&data);
    drop(builder(&data, PASS).create().unwrap());

    let key_file = read_key_file(&key);
    assert_eq!(
        key_file.flags & KEY_FILE_FLAG_SLOTS_V1_REQUIRED,
        KEY_FILE_FLAG_SLOTS_V1_REQUIRED
    );
    assert!(key_file.audit_v2_required());

    rewrite_slots_as_legacy_and_clear_header(&data);
    assert_downgrade_rejected(&data, PASS);
}

#[test]
fn reopening_heals_the_old_header_only_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("reopen.citadel");
    let key = key_path(&data);
    drop(builder(&data, PASS).create().unwrap());

    // Reproduce a key file written before the authenticated marker existed;
    // the data header remains V1-required.
    rewrite_key_file_as_released_legacy(&key, PASS);
    assert!(!read_key_file(&key).slots_v1_required());
    drop(builder(&data, PASS).open().unwrap());
    let healed = read_key_file(&key);
    assert!(healed.slots_v1_required());
    assert!(healed.audit_v2_required());

    rewrite_slots_as_legacy_and_clear_header(&data);
    assert_downgrade_rejected(&data, PASS);
}

#[test]
fn runtime_upgrade_persists_the_authenticated_requirement() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("upgrade.citadel");
    let key = key_path(&data);
    drop(builder(&data, PASS).create().unwrap());

    rewrite_key_file_as_released_legacy(&key, PASS);
    rewrite_slots_as_legacy_and_clear_header(&data);
    let db = builder(&data, PASS).open().unwrap();
    assert!(!read_key_file(&key).slots_v1_required());
    let report = db.upgrade_format().unwrap();
    assert!(report.slots_flagged);
    drop(db);
    let upgraded = read_key_file(&key);
    assert!(upgraded.slots_v1_required());
    assert!(upgraded.audit_v2_required());

    rewrite_slots_as_legacy_and_clear_header(&data);
    assert_downgrade_rejected(&data, PASS);
}

#[test]
fn ordinary_slot_resealing_finishes_the_audit_upgrade_before_authenticating_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("ordinary-reseal.citadel");
    let key = key_path(&data);
    drop(builder(&data, PASS).create().unwrap());

    // Reproduce a released vault: both data slots and the audit header are
    // legacy, and the key file carries no authenticated V1 requirement.
    rewrite_key_file_as_released_legacy(&key, PASS);
    rewrite_slots_as_legacy_and_clear_header(&data);
    rewrite_audit_header_as_legacy(&data);

    // Two ordinary commits reseal the two alternating physical slots. They do
    // not explicitly call upgrade_format, so the audit file remains v1 until
    // the next open observes that both slots are now V1.
    let db = builder(&data, PASS).open().unwrap();
    for n in 0..2u8 {
        let mut txn = db.begin_write().unwrap();
        txn.insert(&[n], &[n]).unwrap();
        txn.commit().unwrap();
    }
    drop(db);
    assert!(!read_key_file(&key).slots_v1_required());
    assert_eq!(read_audit_version(&data), AUDIT_LOG_VERSION_LEGACY);

    // The mutable data flag may already be stamped while audit remains legacy.
    // Opening must durably upgrade audit v2 before authenticating the
    // requirement, or the vault becomes permanently stranded.
    drop(builder(&data, PASS).open().unwrap());
    assert_eq!(read_audit_version(&data), AUDIT_LOG_VERSION);
    let healed = read_key_file(&key);
    assert!(healed.slots_v1_required());
    assert!(healed.audit_v2_required());
}

#[test]
fn enabling_audit_later_finishes_its_upgrade_independently() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("audit-disabled-reseal.citadel");
    let key = key_path(&data);
    drop(builder(&data, PASS).create().unwrap());

    rewrite_key_file_as_released_legacy(&key, PASS);
    rewrite_slots_as_legacy_and_clear_header(&data);
    rewrite_audit_header_as_legacy(&data);

    let db = builder(&data, PASS).open().unwrap();
    for n in 0..2u8 {
        let mut txn = db.begin_write().unwrap();
        txn.insert(&[n], &[n]).unwrap();
        txn.commit().unwrap();
    }
    drop(db);

    let disabled = AuditConfig {
        enabled: false,
        ..AuditConfig::default()
    };
    drop(builder(&data, PASS).audit_config(disabled).open().unwrap());

    let slot_only = read_key_file(&key);
    assert!(slot_only.slots_v1_required());
    assert!(!slot_only.audit_v2_required());
    assert_eq!(
        slot_only.flags & KEY_FILE_FLAG_AUDIT_V2_REQUIRED,
        0,
        "opening with audit disabled must not claim the legacy sidecar is v2"
    );
    assert_eq!(read_audit_version(&data), AUDIT_LOG_VERSION_LEGACY);

    drop(builder(&data, PASS).open().unwrap());
    let fully_healed = read_key_file(&key);
    assert!(fully_healed.slots_v1_required());
    assert!(fully_healed.audit_v2_required());
    assert_eq!(read_audit_version(&data), AUDIT_LOG_VERSION);
}

#[test]
fn passphrase_change_preserves_the_authenticated_requirement() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("passphrase.citadel");
    let key = key_path(&data);
    let db = builder(&data, PASS).create().unwrap();
    db.change_passphrase(PASS, b"new passphrase").unwrap();
    drop(db);
    let changed = read_key_file(&key);
    assert!(changed.slots_v1_required());
    assert!(changed.audit_v2_required());

    rewrite_slots_as_legacy_and_clear_header(&data);
    assert_downgrade_rejected(&data, b"new passphrase");
}

#[test]
fn hot_backup_and_key_backup_restore_preserve_the_requirement() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("source.citadel");
    let hot = dir.path().join("hot.citadel");
    let escrow = dir.path().join("keys.backup");
    let db = builder(&data, PASS).create().unwrap();
    db.backup(&hot).unwrap();
    db.export_key_backup(PASS, b"backup passphrase", &escrow)
        .unwrap();
    drop(db);

    let hot_key = read_key_file(&key_path(&hot));
    assert!(hot_key.slots_v1_required());
    assert!(hot_key.audit_v2_required());
    rewrite_slots_as_legacy_and_clear_header(&hot);
    assert_downgrade_rejected(&hot, PASS);

    let key = key_path(&data);
    std::fs::remove_file(&key).unwrap();
    Database::restore_key_from_backup(&escrow, b"backup passphrase", b"restored passphrase", &data)
        .unwrap();
    let restored = read_key_file(&key);
    assert!(restored.slots_v1_required());
    assert!(restored.audit_v2_required());
    rewrite_slots_as_legacy_and_clear_header(&data);
    assert_downgrade_rejected(&data, b"restored passphrase");
}

#[test]
fn key_restore_refuses_data_downgraded_below_the_backup_policy() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("downgraded-restore.citadel");
    let key = key_path(&data);
    let escrow = dir.path().join("keys.backup");
    let db = builder(&data, PASS).create().unwrap();
    db.export_key_backup(PASS, b"backup passphrase", &escrow)
        .unwrap();
    drop(db);

    rewrite_slots_as_legacy_and_clear_header(&data);
    std::fs::remove_file(&key).unwrap();
    let error = Database::restore_key_from_backup(
        &escrow,
        b"backup passphrase",
        b"restored passphrase",
        &data,
    )
    .unwrap_err();

    assert!(matches!(error, Error::SlotDowngradeDetected));
    assert!(
        !key.exists(),
        "a rejected restore must not publish a key file"
    );
}

#[test]
fn restoring_an_old_backup_preserves_policy_earned_by_the_current_data_file() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("old-backup.citadel");
    let key = key_path(&data);
    let escrow = dir.path().join("legacy-keys.backup");
    drop(builder(&data, PASS).create().unwrap());

    rewrite_key_file_as_released_legacy(&key, PASS);
    rewrite_slots_as_legacy_and_clear_header(&data);

    let disabled = AuditConfig {
        enabled: false,
        ..AuditConfig::default()
    };
    let db = builder(&data, PASS).audit_config(disabled).open().unwrap();
    db.export_key_backup(PASS, b"backup passphrase", &escrow)
        .unwrap();
    db.upgrade_format().unwrap();
    drop(db);

    let before_restore = read_key_file(&key);
    assert!(before_restore.slots_v1_required());
    assert!(!before_restore.audit_v2_required());

    std::fs::remove_file(&key).unwrap();
    Database::restore_key_from_backup(&escrow, b"backup passphrase", b"restored passphrase", &data)
        .unwrap();

    let restored = read_key_file(&key);
    assert!(restored.slots_v1_required());
    assert!(restored.audit_v2_required());

    rewrite_slots_as_legacy_and_clear_header(&data);
    assert_downgrade_rejected(&data, b"restored passphrase");
}

#[test]
fn an_open_handle_never_blesses_or_copies_a_replaced_key_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("replaced-key.citadel");
    let key = key_path(&data);
    let export = dir.path().join("keys.backup");
    let backup = dir.path().join("hot.citadel");
    let compact = dir.path().join("compact.citadel");
    let db = builder(&data, PASS).create().unwrap();

    let mut replacement = std::fs::read(&key).unwrap();
    *replacement.last_mut().unwrap() ^= 0x80;
    std::fs::write(&key, &replacement).unwrap();
    let slot_before = db.manager().current_slot();

    assert!(matches!(
        db.change_passphrase(PASS, b"new passphrase"),
        Err(Error::KeyFileIntegrity)
    ));
    assert!(matches!(
        db.export_key_backup(PASS, b"backup passphrase", &export),
        Err(Error::KeyFileIntegrity)
    ));
    assert!(matches!(db.backup(&backup), Err(Error::KeyFileIntegrity)));
    assert!(matches!(db.compact(&compact), Err(Error::KeyFileIntegrity)));
    assert!(matches!(db.upgrade_format(), Err(Error::KeyFileIntegrity)));

    assert_eq!(std::fs::read(&key).unwrap(), replacement);
    assert_eq!(
        db.manager().current_slot(),
        slot_before,
        "format upgrade touched commit slots before rejecting the key replacement"
    );
    assert!(!export.exists());
    assert!(!backup.exists());
    assert!(!compact.exists());
}

#[test]
fn backup_and_compaction_refuse_orphaned_destination_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("source.citadel");
    let db = builder(&data, PASS).create().unwrap();

    let backup = dir.path().join("backup.citadel");
    let backup_key = key_path(&backup);
    std::fs::write(&backup_key, b"keep this key file").unwrap();
    db.backup(&backup).unwrap_err();
    assert_eq!(std::fs::read(&backup_key).unwrap(), b"keep this key file");
    assert!(!backup.exists());

    let compact = dir.path().join("compact.citadel");
    let mut rotated_name = compact.as_os_str().to_os_string();
    rotated_name.push(".citadel-audit.1");
    let rotated: std::path::PathBuf = rotated_name.into();
    std::fs::write(&rotated, b"keep this audit history").unwrap();
    db.compact(&compact).unwrap_err();
    assert_eq!(std::fs::read(&rotated).unwrap(), b"keep this audit history");
    assert!(!compact.exists());

    let upgrade_dest = dir.path().join("upgrade.citadel");
    let mut upgrade_name = upgrade_dest.as_os_str().to_os_string();
    upgrade_name.push(".citadel-audit.upgrade");
    let upgrade: std::path::PathBuf = upgrade_name.into();
    std::fs::write(&upgrade, b"keep this upgrade image").unwrap();
    db.backup(&upgrade_dest).unwrap_err();
    assert_eq!(std::fs::read(&upgrade).unwrap(), b"keep this upgrade image");
    assert!(!upgrade_dest.exists());
}
