use super::*;

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
    assert_eq!(unwrapped, rek);
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
    assert!(result.is_err());
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
