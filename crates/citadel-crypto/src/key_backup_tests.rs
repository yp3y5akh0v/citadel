use super::*;
use crate::key_manager::{
    create_key_file, KEY_FILE_FLAG_AUDIT_V2_REQUIRED, KEY_FILE_FLAG_SLOTS_V1_REQUIRED,
};

#[test]
fn backup_mac_key_derivation_is_frozen() {
    let key = derive_backup_mac_key(&[0x42u8; KEY_SIZE]);
    let hex: String = key.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(
        hex,
        "5e956eca50e2dffc33f10e015c5f7d1c088af24a1b1550bc06771c4583dd0873"
    );
}

#[test]
fn unknown_cipher_id_is_rejected() {
    let mut buf = [0u8; KEY_BACKUP_SIZE];
    buf[0..4].copy_from_slice(&KEY_BACKUP_MAGIC.to_le_bytes());
    buf[4..8].copy_from_slice(&KEY_BACKUP_VERSION.to_le_bytes());
    buf[16] = 2;

    assert!(matches!(
        KeyBackup::deserialize(&buf),
        Err(citadel_core::Error::UnsupportedCipher(2))
    ));
}

#[test]
fn changing_only_the_cipher_marker_fails_authentication() {
    let passphrase = b"authenticated-marker";
    let backup = create_key_backup(
        &[0x42; KEY_SIZE],
        passphrase,
        42,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
        0,
    )
    .unwrap();
    let mut image = backup;
    image[16] = LEGACY_AES_CIPHER_ID;

    assert!(matches!(
        restore_rek_from_backup(&image, passphrase),
        Err(citadel_core::Error::KeyFileIntegrity)
    ));
}

#[test]
fn legacy_cipher_marker_and_canonical_backup_have_stable_known_answers() {
    let mut backup = KeyBackup {
        magic: KEY_BACKUP_MAGIC,
        version: KEY_BACKUP_VERSION,
        file_id: 0x0102_0304_0506_0708,
        encoded_cipher_id: LEGACY_AES_CIPHER_ID,
        kdf_algorithm: KdfAlgorithm::Pbkdf2HmacSha256,
        key_file_flags: KEY_FILE_FLAG_SLOTS_V1_REQUIRED | KEY_FILE_FLAG_AUDIT_V2_REQUIRED,
        kdf_param1: 0x1112_1314,
        kdf_param2: 0x2122_2324,
        kdf_param3: 0x3132_3334,
        backup_salt: [0x10; ARGON2_SALT_SIZE],
        wrapped_rek: [0x22; WRAPPED_KEY_SIZE],
        epoch: 0x4142_4344,
        hmac: [0; MAC_SIZE],
    };
    let bek = [0x42; KEY_SIZE];
    let mac_key = derive_backup_mac_key(&bek);
    let legacy_mac = compute_backup_mac(&mac_key, &backup.serialize()[..92]);

    assert_eq!(
        legacy_mac,
        [
            0xc1, 0xdb, 0x53, 0xad, 0xdf, 0x4e, 0x1a, 0xec, 0x6d, 0xe2, 0xc2, 0x6d, 0x1a, 0x6a,
            0x64, 0xf4, 0xd8, 0xb0, 0x24, 0x86, 0x3a, 0xbc, 0x06, 0x91, 0x83, 0xbf, 0x38, 0x41,
            0xa7, 0x08, 0xdf, 0x97,
        ]
    );
    backup.hmac = legacy_mac;
    backup.verify_hmac(&bek).unwrap();

    backup.update_hmac(&bek);
    assert!(!backup.has_legacy_cipher_encoding());
    assert_eq!(backup.serialize()[16], CipherId::Aes256Ctr as u8);
    assert_eq!(
        backup.hmac,
        [
            0xf6, 0x25, 0x19, 0x13, 0x01, 0x7b, 0xf9, 0x6b, 0xcc, 0x8c, 0x2e, 0xf8, 0x55, 0x05,
            0x43, 0xf8, 0x6a, 0x35, 0x9f, 0xcb, 0xd6, 0xee, 0xd4, 0x4e, 0xb8, 0xf6, 0x72, 0x05,
            0xa7, 0xda, 0x72, 0x22,
        ]
    );
}

#[test]
fn serialize_deserialize_roundtrip() {
    let rek = [0x42u8; KEY_SIZE];
    let backup_data = create_key_backup(
        &rek,
        b"backup-pass",
        0xDEAD_BEEF,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
        0,
    )
    .unwrap();

    assert_eq!(backup_data.len(), KEY_BACKUP_SIZE);

    let backup = KeyBackup::deserialize(&backup_data).unwrap();
    assert_eq!(backup.magic, KEY_BACKUP_MAGIC);
    assert_eq!(backup.version, KEY_BACKUP_VERSION);
    assert_eq!(backup.file_id, 0xDEAD_BEEF);
    assert_eq!(backup.cipher_id(), CipherId::Aes256Ctr);
    assert_eq!(backup_data[16], CipherId::Aes256Ctr as u8);
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
        KdfAlgorithm::Pbkdf2HmacSha256,
        600_000,
        0,
        0,
        1,
        0,
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
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
        0,
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
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
        0,
    )
    .unwrap();

    backup_data[60] ^= 0x01;

    let result = restore_rek_from_backup(&backup_data, b"backup-pass");
    assert!(result.is_err());
}

#[test]
fn restore_roundtrip() {
    let (kf, original_keys) =
        create_key_file(b"db-pass", 0xCAFE, KdfAlgorithm::Argon2id, 64, 1, 1).unwrap();

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
        kf.kdf_algorithm,
        kf.argon2_m_cost,
        kf.argon2_t_cost,
        kf.argon2_p_cost,
        kf.current_epoch,
        0,
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
fn released_id_one_backup_restores_as_aes() {
    let rek = [0x5Au8; KEY_SIZE];
    let passphrase = b"released-id-one-backup";
    let canonical = create_key_backup(
        &rek,
        passphrase,
        0xC1A0,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        7,
        0,
    )
    .unwrap();
    let mut backup = KeyBackup::deserialize(&canonical).unwrap();
    backup.encoded_cipher_id = LEGACY_AES_CIPHER_ID;
    let bek = derive_mk(
        backup.kdf_algorithm,
        passphrase,
        &backup.backup_salt,
        backup.kdf_param1,
        backup.kdf_param2,
        backup.kdf_param3,
    )
    .unwrap();
    let mac_key = derive_backup_mac_key(&bek);
    backup.hmac = compute_backup_mac(&mac_key, &backup.serialize()[..92]);
    let released_image = backup.serialize();

    let restored = restore_rek_from_backup(&released_image, passphrase).unwrap();

    assert_eq!(released_image[16], LEGACY_AES_CIPHER_ID);
    assert_eq!(restored.rek, rek);
    assert_eq!(restored.cipher_id, CipherId::Aes256Ctr);
    assert_eq!(restored.key_file_flags, 0);
    assert_eq!(restored.epoch, 7);
}

#[test]
fn wrong_backup_passphrase_fails() {
    let rek = [0x42u8; KEY_SIZE];
    let backup_data = create_key_backup(
        &rek,
        b"correct-pass",
        42,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
        0,
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
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        5,
        0,
    )
    .unwrap();

    let result = restore_rek_from_backup(&backup_data, b"pass").unwrap();
    assert_eq!(result.file_id, file_id);
    assert_eq!(result.epoch, 5);
}

#[test]
fn backup_preserves_authenticated_key_file_flags() {
    let rek = [0x42u8; KEY_SIZE];
    let flags = crate::key_manager::KEY_FILE_FLAG_SLOTS_V1_REQUIRED
        | crate::key_manager::KEY_FILE_FLAG_AUDIT_V2_REQUIRED;
    let backup_data = create_key_backup(
        &rek,
        b"pass",
        42,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        5,
        flags,
    )
    .unwrap();

    let result = restore_rek_from_backup(&backup_data, b"pass").unwrap();
    assert_eq!(result.key_file_flags, flags);
}

#[test]
fn backup_policy_layout_has_a_stable_known_answer() {
    let backup = KeyBackup {
        magic: KEY_BACKUP_MAGIC,
        version: KEY_BACKUP_VERSION,
        file_id: 0x0102_0304_0506_0708,
        encoded_cipher_id: LEGACY_AES_CIPHER_ID,
        kdf_algorithm: KdfAlgorithm::Pbkdf2HmacSha256,
        key_file_flags: crate::key_manager::KEY_FILE_FLAG_SLOTS_V1_REQUIRED
            | crate::key_manager::KEY_FILE_FLAG_AUDIT_V2_REQUIRED,
        kdf_param1: 0x1112_1314,
        kdf_param2: 0x2122_2324,
        kdf_param3: 0x3132_3334,
        backup_salt: [0x10; ARGON2_SALT_SIZE],
        wrapped_rek: [0x22; WRAPPED_KEY_SIZE],
        epoch: 0x4142_4344,
        hmac: [0x44; MAC_SIZE],
    };
    let hex: String = backup
        .serialize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    assert_eq!(
        hex,
        "4259454b010000000807060504030201010103001413121124232221343332311010101010101010101010101010101022222222222222222222222222222222222222222222222222222222222222222222222222222222444342414444444444444444444444444444444444444444444444444444444444444444"
    );
}

#[test]
fn backup_rejects_unknown_must_understand_flags() {
    let rek = [0x42u8; KEY_SIZE];
    assert!(matches!(
        create_key_backup(
            &rek,
            b"pass",
            42,
            KdfAlgorithm::Argon2id,
            64,
            1,
            1,
            5,
            0x0040,
        ),
        Err(citadel_core::Error::KeyFileIntegrity)
    ));
}

#[test]
fn backup_rejects_audit_v2_policy_without_protected_slots() {
    let rek = [0x42u8; KEY_SIZE];
    assert!(matches!(
        create_key_backup(
            &rek,
            b"pass",
            42,
            KdfAlgorithm::Argon2id,
            64,
            1,
            1,
            5,
            crate::key_manager::KEY_FILE_FLAG_AUDIT_V2_REQUIRED,
        ),
        Err(citadel_core::Error::KeyFileIntegrity)
    ));
}

#[test]
fn backup_key_file_flags_are_hmac_covered() {
    let rek = [0x42u8; KEY_SIZE];
    let mut backup_data = create_key_backup(
        &rek,
        b"pass",
        42,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
        1,
        crate::key_manager::KEY_FILE_FLAG_SLOTS_V1_REQUIRED,
    )
    .unwrap();
    backup_data[18] ^= 1;

    assert!(matches!(
        restore_rek_from_backup(&backup_data, b"pass"),
        Err(citadel_core::Error::KeyFileIntegrity)
    ));
}

#[test]
fn backup_size_exact() {
    let rek = [0x42u8; KEY_SIZE];
    let backup_data =
        create_key_backup(&rek, b"pass", 42, KdfAlgorithm::Argon2id, 64, 1, 1, 1, 0).unwrap();
    assert_eq!(backup_data.len(), 124);
}

#[test]
fn backup_binary_format_magic() {
    let rek = [0x42u8; KEY_SIZE];
    let backup_data =
        create_key_backup(&rek, b"pass", 42, KdfAlgorithm::Argon2id, 64, 1, 1, 1, 0).unwrap();

    let magic = u32::from_le_bytes(backup_data[0..4].try_into().unwrap());
    assert_eq!(magic, 0x4B45_5942);
}
