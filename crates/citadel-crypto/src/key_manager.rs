use aes_kw::Kek;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use zeroize::Zeroize;

use citadel_core::types::{CipherId, KdfAlgorithm};
use citadel_core::{
    ARGON2_SALT_SIZE, KEY_FILE_MAGIC, KEY_FILE_SIZE, KEY_FILE_VERSION, KEY_SIZE, MAC_SIZE,
    WRAPPED_KEY_SIZE,
};

use crate::hkdf_utils::{
    derive_keyfile_mac_key, derive_keys_from_rek, derive_region_wrap_keys, DerivedKeys,
    RegionWrapKeys,
};
use crate::kdf::derive_mk;

type HmacSha256 = Hmac<Sha256>;

/// On-disk key file representation (172 bytes fixed).
#[derive(Clone)]
pub struct KeyFile {
    pub magic: u32,
    pub version: u32,
    pub file_id: u64,
    pub argon2_salt: [u8; ARGON2_SALT_SIZE],
    pub argon2_m_cost: u32,
    pub argon2_t_cost: u32,
    pub argon2_p_cost: u32,
    pub cipher_id: CipherId,
    pub kdf_algorithm: KdfAlgorithm,
    pub wrapped_rek: [u8; WRAPPED_KEY_SIZE],
    pub current_epoch: u32,
    pub prev_wrapped_rek: [u8; WRAPPED_KEY_SIZE],
    pub prev_epoch: u32,
    pub rotation_active: bool,
    pub file_mac: [u8; MAC_SIZE],
}

impl KeyFile {
    /// Serialize key file to 172 bytes.
    pub fn serialize(&self) -> [u8; KEY_FILE_SIZE] {
        let mut buf = [0u8; KEY_FILE_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.file_id.to_le_bytes());
        buf[16..32].copy_from_slice(&self.argon2_salt);
        buf[32..36].copy_from_slice(&self.argon2_m_cost.to_le_bytes());
        buf[36..40].copy_from_slice(&self.argon2_t_cost.to_le_bytes());
        buf[40..44].copy_from_slice(&self.argon2_p_cost.to_le_bytes());
        buf[44] = self.cipher_id as u8;
        buf[45] = self.kdf_algorithm as u8;
        // [46..48] reserved
        buf[48..88].copy_from_slice(&self.wrapped_rek);
        buf[88..92].copy_from_slice(&self.current_epoch.to_le_bytes());
        buf[92..132].copy_from_slice(&self.prev_wrapped_rek);
        buf[132..136].copy_from_slice(&self.prev_epoch.to_le_bytes());
        buf[136] = if self.rotation_active { 1 } else { 0 };
        // [137..140] pad
        buf[140..172].copy_from_slice(&self.file_mac);
        buf
    }

    /// Deserialize key file from 172 bytes.
    pub fn deserialize(buf: &[u8; KEY_FILE_SIZE]) -> citadel_core::Result<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != KEY_FILE_MAGIC {
            return Err(citadel_core::Error::InvalidKeyFileMagic);
        }

        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != KEY_FILE_VERSION {
            return Err(citadel_core::Error::UnsupportedVersion(version));
        }

        let cipher_id =
            CipherId::from_u8(buf[44]).ok_or(citadel_core::Error::UnsupportedCipher(buf[44]))?;

        // Byte 45: KDF algorithm. 0x00 = Argon2id (backward compatible with
        // existing key files where this byte was reserved/zero).
        let kdf_algorithm =
            KdfAlgorithm::from_u8(buf[45]).ok_or(citadel_core::Error::UnsupportedKdf(buf[45]))?;

        Ok(Self {
            magic,
            version,
            file_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            argon2_salt: buf[16..32].try_into().unwrap(),
            argon2_m_cost: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
            argon2_t_cost: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
            argon2_p_cost: u32::from_le_bytes(buf[40..44].try_into().unwrap()),
            cipher_id,
            kdf_algorithm,
            wrapped_rek: buf[48..88].try_into().unwrap(),
            current_epoch: u32::from_le_bytes(buf[88..92].try_into().unwrap()),
            prev_wrapped_rek: buf[92..132].try_into().unwrap(),
            prev_epoch: u32::from_le_bytes(buf[132..136].try_into().unwrap()),
            rotation_active: buf[136] != 0,
            file_mac: buf[140..172].try_into().unwrap(),
        })
    }

    /// Verify the key file HMAC using the Master Key.
    pub fn verify_mac(&self, mk: &[u8; KEY_SIZE]) -> citadel_core::Result<()> {
        let mac_key = derive_keyfile_mac_key(mk);
        let computed = compute_file_mac(&mac_key, &self.serialize()[..140]);
        if self.file_mac.ct_eq(&computed).into() {
            Ok(())
        } else {
            Err(citadel_core::Error::KeyFileIntegrity)
        }
    }

    /// Recompute and set the file MAC.
    pub fn update_mac(&mut self, mk: &[u8; KEY_SIZE]) {
        let mac_key = derive_keyfile_mac_key(mk);
        let data = self.serialize();
        self.file_mac = compute_file_mac(&mac_key, &data[..140]);
    }
}

/// Compute HMAC-SHA256(mac_key, data) for key file integrity.
fn compute_file_mac(mac_key: &[u8; KEY_SIZE], data: &[u8]) -> [u8; MAC_SIZE] {
    let mut mac = HmacSha256::new_from_slice(mac_key).expect("HMAC key size is always valid");
    mac.update(data);
    let result = mac.finalize().into_bytes();
    let mut out = [0u8; MAC_SIZE];
    out.copy_from_slice(&result);
    out
}

/// Wrap a 32-byte REK using AES-256-KW (RFC 3394). Produces 40 bytes.
pub fn wrap_rek(mk: &[u8; KEY_SIZE], rek: &[u8; KEY_SIZE]) -> [u8; WRAPPED_KEY_SIZE] {
    let kek = Kek::from(*mk);
    let mut out = [0u8; WRAPPED_KEY_SIZE];
    kek.wrap(rek, &mut out)
        .expect("AES-KW wrap should not fail for valid key sizes");
    out
}

/// Unwrap a 40-byte wrapped REK using AES-256-KW. Produces 32 bytes.
pub fn unwrap_rek(
    mk: &[u8; KEY_SIZE],
    wrapped: &[u8; WRAPPED_KEY_SIZE],
) -> citadel_core::Result<[u8; KEY_SIZE]> {
    let kek = Kek::from(*mk);
    let mut rek = [0u8; KEY_SIZE];
    kek.unwrap(wrapped, &mut rek)
        .map_err(|_| citadel_core::Error::KeyUnwrapFailed)?;
    Ok(rek)
}

/// Create a new key file for a fresh database.
///
/// For Argon2id: m_cost, t_cost, p_cost are the standard Argon2 parameters.
/// For PBKDF2: m_cost is the iteration count, t_cost and p_cost must be 0.
pub fn create_key_file(
    passphrase: &[u8],
    file_id: u64,
    cipher_id: CipherId,
    kdf_algorithm: KdfAlgorithm,
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
) -> citadel_core::Result<(KeyFile, DerivedKeys)> {
    let (kf, keys, _region) = create_key_file_with_region_keys(
        passphrase,
        file_id,
        cipher_id,
        kdf_algorithm,
        m_cost,
        t_cost,
        p_cost,
    )?;
    Ok((kf, keys))
}

/// Like [`create_key_file`] but also returns the region wrap keys used by citadel-mem
/// for per-region cryptographic erasure. Derived from the REK before it is zeroized;
/// callers that do not enable region keys use [`create_key_file`] so no region key
/// material is held.
pub fn create_key_file_with_region_keys(
    passphrase: &[u8],
    file_id: u64,
    cipher_id: CipherId,
    kdf_algorithm: KdfAlgorithm,
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
) -> citadel_core::Result<(KeyFile, DerivedKeys, RegionWrapKeys)> {
    use rand::RngCore;

    let salt = crate::kdf::generate_salt();
    let mut rek = [0u8; KEY_SIZE];
    rand::thread_rng().fill_bytes(&mut rek);

    let mk = derive_mk(kdf_algorithm, passphrase, &salt, m_cost, t_cost, p_cost)?;

    let wrapped = wrap_rek(&mk, &rek);
    let keys = derive_keys_from_rek(&rek);
    let region = derive_region_wrap_keys(&rek);

    let mut kf = KeyFile {
        magic: KEY_FILE_MAGIC,
        version: KEY_FILE_VERSION,
        file_id,
        argon2_salt: salt,
        argon2_m_cost: m_cost,
        argon2_t_cost: t_cost,
        argon2_p_cost: p_cost,
        cipher_id,
        kdf_algorithm,
        wrapped_rek: wrapped,
        current_epoch: 1,
        prev_wrapped_rek: [0u8; WRAPPED_KEY_SIZE],
        prev_epoch: 0,
        rotation_active: false,
        file_mac: [0u8; MAC_SIZE],
    };
    kf.update_mac(&mk);

    rek.zeroize();

    Ok((kf, keys, region))
}

/// Open an existing key file with a passphrase.
pub fn open_key_file(
    buf: &[u8; KEY_FILE_SIZE],
    passphrase: &[u8],
    expected_file_id: u64,
) -> citadel_core::Result<(KeyFile, DerivedKeys)> {
    let (kf, keys, _region) = open_key_file_with_region_keys(buf, passphrase, expected_file_id)?;
    Ok((kf, keys))
}

/// Like [`open_key_file`] but also returns the region wrap keys (see
/// [`create_key_file_with_region_keys`]).
pub fn open_key_file_with_region_keys(
    buf: &[u8; KEY_FILE_SIZE],
    passphrase: &[u8],
    expected_file_id: u64,
) -> citadel_core::Result<(KeyFile, DerivedKeys, RegionWrapKeys)> {
    let kf = KeyFile::deserialize(buf)?;

    if kf.file_id != expected_file_id {
        return Err(citadel_core::Error::KeyFileMismatch);
    }

    // Derive MK using the KDF algorithm stored in the key file
    let mk = derive_mk(
        kf.kdf_algorithm,
        passphrase,
        &kf.argon2_salt,
        kf.argon2_m_cost,
        kf.argon2_t_cost,
        kf.argon2_p_cost,
    )?;

    kf.verify_mac(&mk)?;

    let mut rek =
        unwrap_rek(&mk, &kf.wrapped_rek).map_err(|_| citadel_core::Error::BadPassphrase)?;

    let keys = derive_keys_from_rek(&rek);
    let region = derive_region_wrap_keys(&rek);
    rek.zeroize();

    Ok((kf, keys, region))
}

#[cfg(test)]
#[path = "key_manager_tests.rs"]
mod tests;
