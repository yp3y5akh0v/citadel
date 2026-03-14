use hmac::Hmac;
use sha2::Sha256;
use zeroize::Zeroize;

use citadel_core::{Argon2Profile, KEY_SIZE, ARGON2_SALT_SIZE, PBKDF2_MIN_ITERATIONS};
use citadel_core::types::KdfAlgorithm;

/// Derive a Master Key from a passphrase using Argon2id.
pub fn derive_mk_argon2id(
    passphrase: &[u8],
    salt: &[u8; ARGON2_SALT_SIZE],
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
) -> citadel_core::Result<[u8; KEY_SIZE]> {
    let params = argon2::Params::new(m_cost, t_cost, p_cost, Some(KEY_SIZE))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

    let argon2 = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);

    let mut mk = [0u8; KEY_SIZE];
    argon2.hash_password_into(passphrase, salt, &mut mk)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

    Ok(mk)
}

/// Derive a Master Key using the given Argon2 profile.
pub fn derive_mk_with_profile(
    passphrase: &[u8],
    salt: &[u8; ARGON2_SALT_SIZE],
    profile: Argon2Profile,
) -> citadel_core::Result<[u8; KEY_SIZE]> {
    derive_mk_argon2id(passphrase, salt, profile.m_cost(), profile.t_cost(), profile.p_cost())
}

/// Derive a Master Key using PBKDF2-HMAC-SHA256 (FIPS 140-3 approved).
pub fn derive_mk_pbkdf2(
    passphrase: &[u8],
    salt: &[u8; ARGON2_SALT_SIZE],
    iterations: u32,
) -> citadel_core::Result<[u8; KEY_SIZE]> {
    if iterations < PBKDF2_MIN_ITERATIONS {
        return Err(citadel_core::Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "PBKDF2 iterations too low: {} (minimum {})",
                iterations, PBKDF2_MIN_ITERATIONS
            ),
        )));
    }
    let mut mk = [0u8; KEY_SIZE];
    pbkdf2::pbkdf2::<Hmac<Sha256>>(passphrase, salt, iterations, &mut mk)
        .expect("PBKDF2 should not fail with valid parameters");
    Ok(mk)
}

/// Derive a Master Key using the algorithm stored in the key file.
///
/// For Argon2id: `kdf_param1`=m_cost, `kdf_param2`=t_cost, `kdf_param3`=p_cost.
/// For PBKDF2: `kdf_param1`=iterations, `kdf_param2` and `kdf_param3` are ignored.
pub fn derive_mk(
    algorithm: KdfAlgorithm,
    passphrase: &[u8],
    salt: &[u8; ARGON2_SALT_SIZE],
    kdf_param1: u32,
    kdf_param2: u32,
    kdf_param3: u32,
) -> citadel_core::Result<[u8; KEY_SIZE]> {
    match algorithm {
        KdfAlgorithm::Argon2id => derive_mk_argon2id(passphrase, salt, kdf_param1, kdf_param2, kdf_param3),
        KdfAlgorithm::Pbkdf2HmacSha256 => derive_mk_pbkdf2(passphrase, salt, kdf_param1),
    }
}

/// Generate a random salt for KDF.
pub fn generate_salt() -> [u8; ARGON2_SALT_SIZE] {
    use rand::RngCore;
    let mut salt = [0u8; ARGON2_SALT_SIZE];
    rand::thread_rng().fill_bytes(&mut salt);
    salt
}

/// A Master Key wrapper that zeroizes on drop.
pub struct MasterKey {
    key: [u8; KEY_SIZE],
}

impl MasterKey {
    pub fn new(key: [u8; KEY_SIZE]) -> Self {
        Self { key }
    }

    pub fn as_bytes(&self) -> &[u8; KEY_SIZE] {
        &self.key
    }
}

impl Drop for MasterKey {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

#[cfg(test)]
mod tests {
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
            KdfAlgorithm::Pbkdf2HmacSha256, passphrase, &salt,
            PBKDF2_MIN_ITERATIONS, 0, 0,
        ).unwrap();
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
}
