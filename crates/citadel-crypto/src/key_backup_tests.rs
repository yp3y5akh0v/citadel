use super::*;
use crate::key_manager::create_key_file;

#[test]
fn serialize_deserialize_roundtrip() {
    let rek = [0x42u8; KEY_SIZE];
    let backup_data = create_key_backup(
        &rek,
        b"backup-pass",
        0xDEAD_BEEF,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
    )
    .unwrap();

    assert_eq!(backup_data.len(), KEY_BACKUP_SIZE);

    let backup = KeyBackup::deserialize(&backup_data).unwrap();
    assert_eq!(backup.magic, KEY_BACKUP_MAGIC);
    assert_eq!(backup.version, KEY_BACKUP_VERSION);
    assert_eq!(backup.file_id, 0xDEAD_BEEF);
    assert_eq!(backup.cipher_id, CipherId::Aes256Ctr);
    assert_eq!(backup.kdf_algorithm, KdfAlgorithm::Argon2id);
    assert_eq!(backup.epoch, 1);
}

#[test]
fn serialize_deserialize_pbkdf2() {
    let rek = [0x42u8; KEY_SIZE];
    let backup_data = create_key_backup(
        &rek,
        b"backup-pass",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Pbkdf2HmacSha256,
        600_000,
        0,
        0,
        1,
    )
    .unwrap();

    let backup = KeyBackup::deserialize(&backup_data).unwrap();
    assert_eq!(backup.kdf_algorithm, KdfAlgorithm::Pbkdf2HmacSha256);
    assert_eq!(backup.kdf_param1, 600_000);
}

#[test]
fn invalid_magic_rejected() {
    let mut buf = [0u8; KEY_BACKUP_SIZE];
    buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
    let result = KeyBackup::deserialize(&buf);
    assert!(matches!(
        result,
        Err(citadel_core::Error::InvalidMagic { .. })
    ));
}

#[test]
fn invalid_version_rejected() {
    let mut buf = [0u8; KEY_BACKUP_SIZE];
    buf[0..4].copy_from_slice(&KEY_BACKUP_MAGIC.to_le_bytes());
    buf[4..8].copy_from_slice(&99u32.to_le_bytes());
    let result = KeyBackup::deserialize(&buf);
    assert!(matches!(
        result,
        Err(citadel_core::Error::UnsupportedVersion(99))
    ));
}

#[test]
fn hmac_verification() {
    let rek = [0x42u8; KEY_SIZE];
    let backup_data = create_key_backup(
        &rek,
        b"backup-pass",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
    )
    .unwrap();

    let backup = KeyBackup::deserialize(&backup_data).unwrap();
    let bek = derive_mk(
        KdfAlgorithm::Argon2id,
        b"backup-pass",
        &backup.backup_salt,
        64,
        1,
        1,
    )
    .unwrap();
    assert!(backup.verify_hmac(&bek).is_ok());

    let wrong_bek = [0xFF; KEY_SIZE];
    assert!(backup.verify_hmac(&wrong_bek).is_err());
}

#[test]
fn tamper_detected() {
    let rek = [0x42u8; KEY_SIZE];
    let mut backup_data = create_key_backup(
        &rek,
        b"backup-pass",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
    )
    .unwrap();

    backup_data[60] ^= 0x01;

    let result = restore_rek_from_backup(&backup_data, b"backup-pass");
    assert!(result.is_err());
}

#[test]
fn restore_roundtrip() {
    let (kf, original_keys) = create_key_file(
        b"db-pass",
        0xCAFE,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let mk = crate::kdf::derive_mk(
        KdfAlgorithm::Argon2id,
        b"db-pass",
        &kf.argon2_salt,
        64,
        1,
        1,
    )
    .unwrap();
    let rek = unwrap_rek(&mk, &kf.wrapped_rek).unwrap();

    let backup_data = create_key_backup(
        &rek,
        b"backup-pass",
        kf.file_id,
        kf.cipher_id,
        kf.kdf_algorithm,
        kf.argon2_m_cost,
        kf.argon2_t_cost,
        kf.argon2_p_cost,
        kf.current_epoch,
    )
    .unwrap();

    let result = restore_rek_from_backup(&backup_data, b"backup-pass").unwrap();
    assert_eq!(result.file_id, 0xCAFE);
    assert_eq!(result.cipher_id, CipherId::Aes256Ctr);
    assert_eq!(result.epoch, 1);
    assert_eq!(result.keys.dek, original_keys.dek);
    assert_eq!(result.keys.mac_key, original_keys.mac_key);
}

#[test]
fn wrong_backup_passphrase_fails() {
    let rek = [0x42u8; KEY_SIZE];
    let backup_data = create_key_backup(
        &rek,
        b"correct-pass",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
    )
    .unwrap();

    let result = restore_rek_from_backup(&backup_data, b"wrong-pass");
    assert!(result.is_err());
}

#[test]
fn backup_preserves_file_id() {
    let rek = [0x42u8; KEY_SIZE];
    let file_id = 0x1234_5678_9ABC_DEF0u64;
    let backup_data = create_key_backup(
        &rek,
        b"pass",
        file_id,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        5,
    )
    .unwrap();

    let result = restore_rek_from_backup(&backup_data, b"pass").unwrap();
    assert_eq!(result.file_id, file_id);
    assert_eq!(result.epoch, 5);
}

#[test]
fn backup_size_exact() {
    let rek = [0x42u8; KEY_SIZE];
    let backup_data = create_key_backup(
        &rek,
        b"pass",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
    )
    .unwrap();
    assert_eq!(backup_data.len(), 124);
}

#[test]
fn backup_binary_format_magic() {
    let rek = [0x42u8; KEY_SIZE];
    let backup_data = create_key_backup(
        &rek,
        b"pass",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
    )
    .unwrap();

    let magic = u32::from_le_bytes(backup_data[0..4].try_into().unwrap());
    assert_eq!(magic, 0x4B45_5942);
}
