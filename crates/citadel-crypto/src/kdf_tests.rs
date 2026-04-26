use super::*;

#[test]
fn argon2id_deterministic() {
    let passphrase = b"test-passphrase";
    let salt = [0x42u8; ARGON2_SALT_SIZE];
    let mk1 = derive_mk_argon2id(passphrase, &salt, 64, 1, 1).unwrap();
    let mk2 = derive_mk_argon2id(passphrase, &salt, 64, 1, 1).unwrap();
    assert_eq!(mk1, mk2);
}

#[test]
fn argon2id_different_passphrase() {
    let salt = [0x42u8; ARGON2_SALT_SIZE];
    let mk1 = derive_mk_argon2id(b"password1", &salt, 64, 1, 1).unwrap();
    let mk2 = derive_mk_argon2id(b"password2", &salt, 64, 1, 1).unwrap();
    assert_ne!(mk1, mk2);
}

#[test]
fn argon2id_different_salt() {
    let passphrase = b"test-passphrase";
    let salt1 = [0x01u8; ARGON2_SALT_SIZE];
    let salt2 = [0x02u8; ARGON2_SALT_SIZE];
    let mk1 = derive_mk_argon2id(passphrase, &salt1, 64, 1, 1).unwrap();
    let mk2 = derive_mk_argon2id(passphrase, &salt2, 64, 1, 1).unwrap();
    assert_ne!(mk1, mk2);
}

#[test]
fn argon2id_profile_desktop() {
    let salt = [0x42u8; ARGON2_SALT_SIZE];
    let mk = derive_mk_argon2id(b"test", &salt, 256, 1, 1).unwrap();
    assert_eq!(mk.len(), KEY_SIZE);
}

#[test]
fn pbkdf2_deterministic() {
    let passphrase = b"test-passphrase";
    let salt = [0x42u8; ARGON2_SALT_SIZE];
    let mk1 = derive_mk_pbkdf2(passphrase, &salt, PBKDF2_MIN_ITERATIONS).unwrap();
    let mk2 = derive_mk_pbkdf2(passphrase, &salt, PBKDF2_MIN_ITERATIONS).unwrap();
    assert_eq!(mk1, mk2);
}

#[test]
fn pbkdf2_different_passphrase() {
    let salt = [0x42u8; ARGON2_SALT_SIZE];
    let mk1 = derive_mk_pbkdf2(b"password1", &salt, PBKDF2_MIN_ITERATIONS).unwrap();
    let mk2 = derive_mk_pbkdf2(b"password2", &salt, PBKDF2_MIN_ITERATIONS).unwrap();
    assert_ne!(mk1, mk2);
}

#[test]
fn pbkdf2_different_salt() {
    let passphrase = b"test-passphrase";
    let salt1 = [0x01u8; ARGON2_SALT_SIZE];
    let salt2 = [0x02u8; ARGON2_SALT_SIZE];
    let mk1 = derive_mk_pbkdf2(passphrase, &salt1, PBKDF2_MIN_ITERATIONS).unwrap();
    let mk2 = derive_mk_pbkdf2(passphrase, &salt2, PBKDF2_MIN_ITERATIONS).unwrap();
    assert_ne!(mk1, mk2);
}

#[test]
fn pbkdf2_too_few_iterations() {
    let salt = [0x42u8; ARGON2_SALT_SIZE];
    let result = derive_mk_pbkdf2(b"test", &salt, 1000);
    assert!(result.is_err());
}

#[test]
fn pbkdf2_differs_from_argon2id() {
    let passphrase = b"same-passphrase";
    let salt = [0x42u8; ARGON2_SALT_SIZE];
    let mk_argon2 = derive_mk_argon2id(passphrase, &salt, 64, 1, 1).unwrap();
    let mk_pbkdf2 = derive_mk_pbkdf2(passphrase, &salt, PBKDF2_MIN_ITERATIONS).unwrap();
    assert_ne!(mk_argon2, mk_pbkdf2);
}

#[test]
fn derive_mk_dispatches_correctly() {
    let passphrase = b"test";
    let salt = [0x42u8; ARGON2_SALT_SIZE];

    let mk_direct = derive_mk_argon2id(passphrase, &salt, 64, 1, 1).unwrap();
    let mk_via_dispatch = derive_mk(KdfAlgorithm::Argon2id, passphrase, &salt, 64, 1, 1).unwrap();
    assert_eq!(mk_direct, mk_via_dispatch);

    let mk_pbkdf2_direct = derive_mk_pbkdf2(passphrase, &salt, PBKDF2_MIN_ITERATIONS).unwrap();
    let mk_pbkdf2_dispatch = derive_mk(
        KdfAlgorithm::Pbkdf2HmacSha256,
        passphrase,
        &salt,
        PBKDF2_MIN_ITERATIONS,
        0,
        0,
    )
    .unwrap();
    assert_eq!(mk_pbkdf2_direct, mk_pbkdf2_dispatch);
}

#[test]
fn salt_generation() {
    let s1 = generate_salt();
    let s2 = generate_salt();
    assert_ne!(s1, s2, "Two random salts should differ");
}

#[test]
fn master_key_zeroize_on_drop() {
    let key = [0xFFu8; KEY_SIZE];
    let mk = MasterKey::new(key);
    assert_eq!(mk.as_bytes(), &[0xFFu8; KEY_SIZE]);
}
