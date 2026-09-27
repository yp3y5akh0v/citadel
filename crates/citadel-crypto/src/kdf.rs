use std::io;

use hmac::Hmac;
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use citadel_core::types::KdfAlgorithm;
use citadel_core::{
    Argon2Profile, ARGON2_MAX_T_COST, ARGON2_SALT_SIZE, KEY_SIZE, PBKDF2_MAX_ITERATIONS,
    PBKDF2_MIN_ITERATIONS,
};

use crate::physical_memory;

/// Derive a Master Key from a passphrase using Argon2id.
///
/// Returned in a [`Zeroizing`] wrapper so the MK is wiped on drop on every
/// exit path of the caller.
pub fn derive_mk_argon2id(
    passphrase: &[u8],
    salt: &[u8; ARGON2_SALT_SIZE],
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
) -> citadel_core::Result<Zeroizing<[u8; KEY_SIZE]>> {
    if t_cost > ARGON2_MAX_T_COST {
        return Err(invalid_input(format!(
            "Argon2 time cost too high: {t_cost} (maximum {ARGON2_MAX_T_COST})"
        )));
    }
    // argon2 0.5 multiplies p_cost by 8 before bounding it, which overflows.
    if p_cost > argon2::Params::MAX_P_COST {
        return Err(invalid_input(format!(
            "Argon2 parallelism too high: {p_cost} (maximum {})",
            argon2::Params::MAX_P_COST
        )));
    }
    let params = argon2::Params::new(m_cost, t_cost, p_cost, Some(KEY_SIZE))
        .map_err(|e| invalid_input(e.to_string()))?;
    let mut memory = argon2_memory(params.block_count())?;

    let argon2 = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);

    let mut mk = Zeroizing::new([0u8; KEY_SIZE]);
    argon2
        .hash_password_into_with_memory(passphrase, salt, &mut *mk, &mut memory)
        .map_err(|e| invalid_input(e.to_string()))?;

    Ok(mk)
}

/// Argon2's working memory, sized by a memory cost that may come from an unauthenticated
/// file: more than the machine has, or than it can allocate now, is an error rather than an
/// aborted process.
fn argon2_memory(blocks: usize) -> citadel_core::Result<Vec<argon2::Block>> {
    const MIB: u64 = 1024 * 1024;
    let bytes = (blocks as u64).saturating_mul(argon2::Block::SIZE as u64);
    if let Some(total) = physical_memory::total().filter(|&total| bytes > total) {
        return Err(out_of_memory(format!(
            "Argon2 memory cost of {} MiB exceeds this machine's {} MiB of physical memory",
            bytes / MIB,
            total / MIB
        )));
    }
    let mut memory = Vec::new();
    memory.try_reserve_exact(blocks).map_err(|_| {
        out_of_memory(format!(
            "Argon2 memory cost of {} MiB cannot be allocated",
            bytes / MIB
        ))
    })?;
    memory.resize(blocks, argon2::Block::default());
    Ok(memory)
}

fn check_pbkdf2_iterations(iterations: u32) -> citadel_core::Result<()> {
    if iterations < PBKDF2_MIN_ITERATIONS {
        return Err(invalid_input(format!(
            "PBKDF2 iterations too low: {iterations} (minimum {PBKDF2_MIN_ITERATIONS})"
        )));
    }
    if iterations > PBKDF2_MAX_ITERATIONS {
        return Err(invalid_input(format!(
            "PBKDF2 iterations too high: {iterations} (maximum {PBKDF2_MAX_ITERATIONS})"
        )));
    }
    Ok(())
}

fn invalid_input(message: String) -> citadel_core::Error {
    citadel_core::Error::Io(io::Error::new(io::ErrorKind::InvalidInput, message))
}

fn out_of_memory(message: String) -> citadel_core::Error {
    citadel_core::Error::Io(io::Error::new(io::ErrorKind::OutOfMemory, message))
}

/// Derive a Master Key using the given Argon2 profile.
pub fn derive_mk_with_profile(
    passphrase: &[u8],
    salt: &[u8; ARGON2_SALT_SIZE],
    profile: Argon2Profile,
) -> citadel_core::Result<Zeroizing<[u8; KEY_SIZE]>> {
    derive_mk_argon2id(
        passphrase,
        salt,
        profile.m_cost(),
        profile.t_cost(),
        profile.p_cost(),
    )
}

/// Derive a Master Key using PBKDF2-HMAC-SHA256.
pub fn derive_mk_pbkdf2(
    passphrase: &[u8],
    salt: &[u8; ARGON2_SALT_SIZE],
    iterations: u32,
) -> citadel_core::Result<Zeroizing<[u8; KEY_SIZE]>> {
    check_pbkdf2_iterations(iterations)?;
    let mut mk = Zeroizing::new([0u8; KEY_SIZE]);
    pbkdf2::pbkdf2::<Hmac<Sha256>>(passphrase, salt, iterations, &mut *mk)
        .expect("PBKDF2 should not fail with valid parameters");
    Ok(mk)
}

/// Derive a Master Key using the algorithm stored in the key file.
///
/// For Argon2id: `kdf_param1`=m_cost, `kdf_param2`=t_cost, `kdf_param3`=p_cost.
/// For PBKDF2: `kdf_param1`=iterations; `kdf_param2`/`kdf_param3` ignored.
pub fn derive_mk(
    algorithm: KdfAlgorithm,
    passphrase: &[u8],
    salt: &[u8; ARGON2_SALT_SIZE],
    kdf_param1: u32,
    kdf_param2: u32,
    kdf_param3: u32,
) -> citadel_core::Result<Zeroizing<[u8; KEY_SIZE]>> {
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
