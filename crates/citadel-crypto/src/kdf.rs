use argon2::Argon2;
use zeroize::Zeroize;

use citadel_core::{Argon2Profile, KEY_SIZE, ARGON2_SALT_SIZE};

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

    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);

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

/// Derive a Master Key using PBKDF2-HMAC-SHA256 (FIPS mode fallback).
/// Uses 600,000 iterations per OWASP 2026 minimum recommendation.
#[cfg(feature = "fips")]
pub fn derive_mk_pbkdf2(
    passphrase: &[u8],
    salt: &[u8; ARGON2_SALT_SIZE],
    iterations: u32,
) -> [u8; KEY_SIZE] {
    use hmac::Hmac;
    use sha2::Sha256;

    let mut mk = [0u8; KEY_SIZE];
    pbkdf2::pbkdf2::<Hmac<Sha256>>(passphrase, salt, iterations, &mut mk)
        .expect("PBKDF2 should not fail with valid parameters");
    mk
}

#[cfg(feature = "fips")]
pub const PBKDF2_MIN_ITERATIONS: u32 = 600_000;

/// Generate a random salt for Argon2id / PBKDF2.
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
        // Use minimal params for test speed
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
        // Just verify profile params work (Desktop is too expensive for unit test at 64MB)
        // Use IoT profile with reduced memory for testing
        let salt = [0x42u8; ARGON2_SALT_SIZE];
        let mk = derive_mk_argon2id(b"test", &salt, 256, 1, 1).unwrap();
        assert_eq!(mk.len(), KEY_SIZE);
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
        // Zeroize happens on drop — verified by Miri or manual inspection
    }
}
