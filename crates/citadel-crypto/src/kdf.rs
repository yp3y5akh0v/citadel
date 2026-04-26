use hmac::Hmac;
use sha2::Sha256;
use zeroize::Zeroize;

use citadel_core::types::KdfAlgorithm;
use citadel_core::{Argon2Profile, ARGON2_SALT_SIZE, KEY_SIZE, PBKDF2_MIN_ITERATIONS};

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
    argon2
        .hash_password_into(passphrase, salt, &mut mk)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

    Ok(mk)
}

/// Derive a Master Key using the given Argon2 profile.
pub fn derive_mk_with_profile(
    passphrase: &[u8],
    salt: &[u8; ARGON2_SALT_SIZE],
    profile: Argon2Profile,
) -> citadel_core::Result<[u8; KEY_SIZE]> {
    derive_mk_argon2id(
        passphrase,
        salt,
        profile.m_cost(),
        profile.t_cost(),
        profile.p_cost(),
    )
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
        KdfAlgorithm::Argon2id => {
            derive_mk_argon2id(passphrase, salt, kdf_param1, kdf_param2, kdf_param3)
        }
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
#[path = "kdf_tests.rs"]
mod tests;
