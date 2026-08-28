use super::*;

#[test]
fn key_file_data_mac_info_has_stable_known_answer() {
    let auth_key = KeyFileAuthKey::from_database_mac_key(&[0x42; KEY_SIZE]);

    assert_eq!(
        auth_key.0,
        [
            0xa1, 0xe4, 0xef, 0xf2, 0x37, 0x64, 0x4e, 0x7c, 0x34, 0x14, 0xde, 0xe1, 0xf1, 0x9d,
            0x81, 0x5c, 0x89, 0x4b, 0x67, 0xa4, 0x1b, 0xd8, 0xa5, 0xb9, 0x51, 0x55, 0x26, 0xd4,
            0x8b, 0x11, 0xaf, 0xde,
        ]
    );
}

#[test]
fn key_file_policy_layout_has_a_stable_known_answer() {
    let mut key_file = KeyFile {
        magic: KEY_FILE_MAGIC,
        version: KEY_FILE_VERSION,
        file_id: 0x0102_0304_0506_0708,
        argon2_salt: [0x10; ARGON2_SALT_SIZE],
        argon2_m_cost: 0x1112_1314,
        argon2_t_cost: 0x2122_2324,
        argon2_p_cost: 0x3132_3334,
        cipher_id: CipherId::ChaCha20,
        kdf_algorithm: KdfAlgorithm::Pbkdf2HmacSha256,
        flags: KEY_FILE_FLAG_SLOTS_V1_REQUIRED | KEY_FILE_FLAG_AUDIT_V2_REQUIRED,
        wrapped_rek: [0x22; WRAPPED_KEY_SIZE],
        current_epoch: 0x4142_4344,
        prev_wrapped_rek: [0x33; WRAPPED_KEY_SIZE],
        prev_epoch: 0x5152_5354,
        rotation_active: true,
        file_mac: [0x44; MAC_SIZE],
    };
    let hex: String = key_file
        .serialize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    assert_eq!(
        hex,
        "5359454b010000000807060504030201101010101010101010101010101010101413121124232221343332310101030022222222222222222222222222222222222222222222222222222222222222222222222222222222444342413333333333333333333333333333333333333333333333333333333333333333333333333333333354535251010000004444444444444444444444444444444444444444444444444444444444444444"
    );

    let auth_key = KeyFileAuthKey::from_database_mac_key(&[0x42; KEY_SIZE]);
    key_file.update_mac_with_auth_key(&auth_key);
    assert_eq!(
        key_file.file_mac,
        [
            0x7c, 0x97, 0x66, 0x65, 0x66, 0x51, 0x25, 0xcf, 0xa7, 0xfd, 0x50, 0xe7, 0xfb, 0xdf,
            0x5f, 0x56, 0xc7, 0xfe, 0x68, 0x1a, 0xb0, 0x6c, 0x4f, 0x97, 0x7c, 0x04, 0x96, 0xb0,
            0x5c, 0xf3, 0xe3, 0x5d,
        ]
    );
}

#[test]
fn noncanonical_rotation_flag_is_rejected_before_mac_verification() {
    let (key_file, _) = create_key_file(
        b"password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();
    let mut serialized = key_file.serialize();
    serialized[136] = 2;

    assert!(matches!(
        KeyFile::deserialize(&serialized),
        Err(citadel_core::Error::KeyFileIntegrity)
    ));
}

#[test]
fn nonzero_key_file_padding_is_rejected_before_mac_verification() {
    let (key_file, _) = create_key_file(
        b"password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();
    let mut serialized = key_file.serialize();
    serialized[137] = 1;

    assert!(matches!(
        KeyFile::deserialize(&serialized),
        Err(citadel_core::Error::KeyFileIntegrity)
    ));
}

#[test]
fn key_file_serialize_deserialize_roundtrip() {
    let (kf, _keys) = create_key_file(
        b"test-password",
        0x1234567890ABCDEF,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let serialized = kf.serialize();
    assert_eq!(serialized.len(), KEY_FILE_SIZE);

    let deserialized = KeyFile::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.magic, KEY_FILE_MAGIC);
    assert_eq!(deserialized.version, KEY_FILE_VERSION);
    assert_eq!(deserialized.file_id, 0x1234567890ABCDEF);
    assert_eq!(deserialized.cipher_id, CipherId::Aes256Ctr);
    assert_eq!(deserialized.kdf_algorithm, KdfAlgorithm::Argon2id);
    assert!(deserialized.slots_v1_required());
    assert!(deserialized.audit_v2_required());
    assert_eq!(deserialized.current_epoch, 1);
    assert!(!deserialized.rotation_active);
}

#[test]
fn key_file_serialize_deserialize_pbkdf2() {
    let (kf, _keys) = create_key_file(
        b"test-password",
        0xDEAD,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Pbkdf2HmacSha256,
        600_000,
        0,
        0,
    )
    .unwrap();

    let serialized = kf.serialize();
    let deserialized = KeyFile::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.kdf_algorithm, KdfAlgorithm::Pbkdf2HmacSha256);
    assert_eq!(deserialized.argon2_m_cost, 600_000);
    assert_eq!(deserialized.argon2_t_cost, 0);
    assert_eq!(deserialized.argon2_p_cost, 0);
}

#[test]
fn backward_compat_zero_byte_is_argon2id() {
    let (kf, _) = create_key_file(
        b"test",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let serialized = kf.serialize();
    assert_eq!(serialized[45], 0x00); // Argon2id = 0
    let deserialized = KeyFile::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.kdf_algorithm, KdfAlgorithm::Argon2id);
}

#[test]
fn key_file_mac_verification() {
    let (kf, _keys) = create_key_file(
        b"test-password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let mk = crate::kdf::derive_mk_argon2id(
        b"test-password",
        &kf.argon2_salt,
        kf.argon2_m_cost,
        kf.argon2_t_cost,
        kf.argon2_p_cost,
    )
    .unwrap();

    assert!(kf.verify_mac(&mk).is_ok());

    let wrong_mk = [0xFFu8; KEY_SIZE];
    assert!(kf.verify_mac(&wrong_mk).is_err());
}

#[test]
fn wrap_unwrap_roundtrip() {
    let mk = [0xAA; KEY_SIZE];
    let rek = [0xBB; KEY_SIZE];
    let wrapped = wrap_rek(&mk, &rek);
    assert_eq!(wrapped.len(), WRAPPED_KEY_SIZE);

    let unwrapped = unwrap_rek(&mk, &wrapped).unwrap();
    assert_eq!(*unwrapped, rek);
}

#[test]
fn unwrap_rek_returns_zeroizing_wrapper() {
    // Regression: unwrap_rek must hand back the REK inside Zeroizing so the
    // secret is wiped on drop on every caller exit path, not left on the stack.
    let mk = [0xAA; KEY_SIZE];
    let rek = [0xBB; KEY_SIZE];
    let wrapped = wrap_rek(&mk, &rek);
    let unwrapped: Zeroizing<[u8; KEY_SIZE]> = unwrap_rek(&mk, &wrapped).unwrap();
    assert_eq!(*unwrapped, rek);
}

#[test]
fn wrong_key_unwrap_fails() {
    let mk = [0xAA; KEY_SIZE];
    let rek = [0xBB; KEY_SIZE];
    let wrapped = wrap_rek(&mk, &rek);

    let wrong_mk = [0xCC; KEY_SIZE];
    assert!(unwrap_rek(&wrong_mk, &wrapped).is_err());
}

#[test]
fn open_key_file_correct_password() {
    let passphrase = b"correct-horse-battery-staple";
    let file_id = 0xDEAD_BEEF;

    let (kf, keys1) = create_key_file(
        passphrase,
        file_id,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let serialized = kf.serialize();
    let (_kf2, keys2) = open_key_file(&serialized, passphrase, file_id).unwrap();

    assert_eq!(keys1.dek, keys2.dek);
    assert_eq!(keys1.mac_key, keys2.mac_key);
}

#[test]
fn open_key_file_pbkdf2() {
    let passphrase = b"pbkdf2-password";
    let file_id = 0xBEEF;

    let (kf, keys1) = create_key_file(
        passphrase,
        file_id,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Pbkdf2HmacSha256,
        600_000,
        0,
        0,
    )
    .unwrap();

    let serialized = kf.serialize();
    let (_kf2, keys2) = open_key_file(&serialized, passphrase, file_id).unwrap();

    assert_eq!(keys1.dek, keys2.dek);
    assert_eq!(keys1.mac_key, keys2.mac_key);
}

#[test]
fn open_key_file_wrong_password() {
    let (kf, _) = create_key_file(
        b"correct-password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let serialized = kf.serialize();
    let result = open_key_file(&serialized, b"wrong-password", 42);
    assert!(matches!(result, Err(citadel_core::Error::BadPassphrase)));
}

#[test]
fn released_zero_flag_key_file_still_opens() {
    let passphrase = b"legacy-password";
    let (mut kf, expected) = create_key_file(
        passphrase,
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();
    let mk = crate::kdf::derive_mk_argon2id(
        passphrase,
        &kf.argon2_salt,
        kf.argon2_m_cost,
        kf.argon2_t_cost,
        kf.argon2_p_cost,
    )
    .unwrap();

    // Reproduce the released image: reserved flags are zero and the MAC is
    // derived from the passphrase master key.
    kf.flags = 0;
    kf.update_mac(&mk).unwrap();
    let (opened, actual) = open_key_file(&kf.serialize(), passphrase, 42).unwrap();
    assert!(!opened.slots_v1_required());
    assert!(!opened.audit_v2_required());
    assert_eq!(actual.dek, expected.dek);
    assert_eq!(actual.mac_key, expected.mac_key);
}

#[test]
fn clearing_authenticated_v1_requirement_is_detected() {
    let (kf, _) = create_key_file(
        b"password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();
    let mut serialized = kf.serialize();
    serialized[46..48].copy_from_slice(&0u16.to_le_bytes());

    let result = open_key_file(&serialized, b"password", 42);
    assert!(matches!(result, Err(citadel_core::Error::KeyFileIntegrity)));
}

#[test]
fn flagged_key_file_mac_update_rejects_a_wrong_master_key_without_mutation() {
    let (mut key_file, _) = create_key_file(
        b"password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();
    let before = key_file.file_mac;

    assert!(matches!(
        key_file.update_mac(&[0x42; KEY_SIZE]),
        Err(citadel_core::Error::KeyFileIntegrity)
    ));
    assert_eq!(key_file.file_mac, before);
}

#[test]
fn flagged_key_file_verification_reports_a_wrong_passphrase() {
    let (key_file, _) = create_key_file(
        b"password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    assert!(matches!(
        key_file.verify_mac(&[0x42; KEY_SIZE]),
        Err(citadel_core::Error::BadPassphrase)
    ));
}

#[test]
fn setting_authenticated_v1_requirement_on_a_legacy_mac_is_detected() {
    let passphrase = b"password";
    let (mut kf, _) = create_key_file(
        passphrase,
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();
    let mk = crate::kdf::derive_mk_argon2id(
        passphrase,
        &kf.argon2_salt,
        kf.argon2_m_cost,
        kf.argon2_t_cost,
        kf.argon2_p_cost,
    )
    .unwrap();
    kf.flags = 0;
    kf.update_mac(&mk).unwrap();
    let mut serialized = kf.serialize();
    serialized[46..48].copy_from_slice(&KEY_FILE_FLAG_SLOTS_V1_REQUIRED.to_le_bytes());

    let result = open_key_file(&serialized, passphrase, 42);
    assert!(matches!(result, Err(citadel_core::Error::KeyFileIntegrity)));
}

#[test]
fn unknown_key_file_policy_flags_are_rejected() {
    let (kf, _) = create_key_file(
        b"password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();
    let mut serialized = kf.serialize();
    serialized[46..48].copy_from_slice(&0x0040u16.to_le_bytes());

    assert!(matches!(
        KeyFile::deserialize(&serialized),
        Err(citadel_core::Error::KeyFileIntegrity)
    ));
}

#[test]
fn audit_v2_requirement_without_protected_slots_is_rejected() {
    let (kf, _) = create_key_file(
        b"password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();
    let mut serialized = kf.serialize();
    serialized[46..48].copy_from_slice(&KEY_FILE_FLAG_AUDIT_V2_REQUIRED.to_le_bytes());

    assert!(matches!(
        KeyFile::deserialize(&serialized),
        Err(citadel_core::Error::KeyFileIntegrity)
    ));
}

#[test]
fn open_key_file_wrong_file_id() {
    let (kf, _) = create_key_file(
        b"password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let serialized = kf.serialize();
    let result = open_key_file(&serialized, b"password", 99);
    assert!(matches!(result, Err(citadel_core::Error::KeyFileMismatch)));
}

#[test]
fn invalid_magic_rejected() {
    let mut buf = [0u8; KEY_FILE_SIZE];
    buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
    let result = KeyFile::deserialize(&buf);
    assert!(matches!(
        result,
        Err(citadel_core::Error::InvalidKeyFileMagic)
    ));
}

#[test]
fn tampered_key_file_detected() {
    let (kf, _) = create_key_file(
        b"password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let mut serialized = kf.serialize();
    serialized[50] ^= 0x01;

    let result = open_key_file(&serialized, b"password", 42);
    assert!(result.is_err());
}

#[test]
fn invalid_kdf_algorithm_rejected() {
    let (kf, _) = create_key_file(
        b"password",
        42,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let mut serialized = kf.serialize();
    serialized[45] = 0xFF; // Invalid KDF algorithm
    let result = KeyFile::deserialize(&serialized);
    assert!(matches!(
        result,
        Err(citadel_core::Error::UnsupportedKdf(0xFF))
    ));
}

#[test]
fn pbkdf2_different_keys_from_argon2id() {
    let passphrase = b"same-password";
    let file_id = 42;

    let (_, keys_argon2) = create_key_file(
        passphrase,
        file_id,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Argon2id,
        64,
        1,
        1,
    )
    .unwrap();

    let (_, keys_pbkdf2) = create_key_file(
        passphrase,
        file_id,
        CipherId::Aes256Ctr,
        KdfAlgorithm::Pbkdf2HmacSha256,
        600_000,
        0,
        0,
    )
    .unwrap();

    assert_ne!(keys_argon2.dek, keys_pbkdf2.dek);
}
