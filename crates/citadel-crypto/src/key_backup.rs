use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use zeroize::Zeroize;

use citadel_core::types::{CipherId, KdfAlgorithm};
use citadel_core::{
    ARGON2_SALT_SIZE, HKDF_INFO_BACKUP_MAC, KEY_BACKUP_MAGIC, KEY_BACKUP_SIZE, KEY_BACKUP_VERSION,
    KEY_SIZE, MAC_SIZE, WRAPPED_KEY_SIZE,
};

use crate::hkdf_utils::derive_keys_from_rek;
use crate::kdf::derive_mk;
use crate::key_manager::{unwrap_rek, wrap_rek};

type HmacSha256 = Hmac<Sha256>;

/// Encrypted key backup file (124 bytes fixed).
#[derive(Clone)]
pub struct KeyBackup {
    pub magic: u32,
    pub version: u32,
    pub file_id: u64,
    pub cipher_id: CipherId,
    pub kdf_algorithm: KdfAlgorithm,
    pub kdf_param1: u32,
    pub kdf_param2: u32,
    pub kdf_param3: u32,
    pub backup_salt: [u8; ARGON2_SALT_SIZE],
    pub wrapped_rek: [u8; WRAPPED_KEY_SIZE],
    pub epoch: u32,
    pub hmac: [u8; MAC_SIZE],
}

impl KeyBackup {
    /// Serialize to 124 bytes.
    pub fn serialize(&self) -> [u8; KEY_BACKUP_SIZE] {
        let mut buf = [0u8; KEY_BACKUP_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.file_id.to_le_bytes());
        buf[16] = self.cipher_id as u8;
        buf[17] = self.kdf_algorithm as u8;
        // [18..20] reserved
        buf[20..24].copy_from_slice(&self.kdf_param1.to_le_bytes());
        buf[24..28].copy_from_slice(&self.kdf_param2.to_le_bytes());
        buf[28..32].copy_from_slice(&self.kdf_param3.to_le_bytes());
        buf[32..48].copy_from_slice(&self.backup_salt);
        buf[48..88].copy_from_slice(&self.wrapped_rek);
        buf[88..92].copy_from_slice(&self.epoch.to_le_bytes());
        buf[92..124].copy_from_slice(&self.hmac);
        buf
    }

    /// Deserialize from 124 bytes.
    pub fn deserialize(buf: &[u8; KEY_BACKUP_SIZE]) -> citadel_core::Result<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != KEY_BACKUP_MAGIC {
            return Err(citadel_core::Error::InvalidMagic {
                expected: KEY_BACKUP_MAGIC,
                found: magic,
            });
        }

        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != KEY_BACKUP_VERSION {
            return Err(citadel_core::Error::UnsupportedVersion(version));
        }

        let cipher_id =
            CipherId::from_u8(buf[16]).ok_or(citadel_core::Error::UnsupportedCipher(buf[16]))?;

        let kdf_algorithm =
            KdfAlgorithm::from_u8(buf[17]).ok_or(citadel_core::Error::UnsupportedKdf(buf[17]))?;

        Ok(Self {
            magic,
            version,
            file_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            cipher_id,
            kdf_algorithm,
            kdf_param1: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
            kdf_param2: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            kdf_param3: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
            backup_salt: buf[32..48].try_into().unwrap(),
            wrapped_rek: buf[48..88].try_into().unwrap(),
            epoch: u32::from_le_bytes(buf[88..92].try_into().unwrap()),
            hmac: buf[92..124].try_into().unwrap(),
        })
    }

    /// Verify the backup HMAC using the Backup Encryption Key.
    pub fn verify_hmac(&self, bek: &[u8; KEY_SIZE]) -> citadel_core::Result<()> {
        let mac_key = derive_backup_mac_key(bek);
        let computed = compute_backup_mac(&mac_key, &self.serialize()[..92]);
        if self.hmac.ct_eq(&computed).into() {
            Ok(())
        } else {
            Err(citadel_core::Error::KeyFileIntegrity)
        }
    }

    /// Recompute and set the HMAC field.
    pub fn update_hmac(&mut self, bek: &[u8; KEY_SIZE]) {
        let mac_key = derive_backup_mac_key(bek);
        let data = self.serialize();
        self.hmac = compute_backup_mac(&mac_key, &data[..92]);
    }
}

fn derive_backup_mac_key(bek: &[u8; KEY_SIZE]) -> [u8; KEY_SIZE] {
    use hkdf::Hkdf;
    let salt = [0u8; 32];
    let hk = Hkdf::<Sha256>::new(Some(&salt), bek);
    let mut key = [0u8; KEY_SIZE];
    hk.expand(HKDF_INFO_BACKUP_MAC, &mut key)
        .expect("HKDF expand should not fail for 32-byte output");
    key
}

fn compute_backup_mac(mac_key: &[u8; KEY_SIZE], data: &[u8]) -> [u8; MAC_SIZE] {
    let mut mac = HmacSha256::new_from_slice(mac_key).expect("HMAC key size is always valid");
    mac.update(data);
    let result = mac.finalize().into_bytes();
    let mut out = [0u8; MAC_SIZE];
    out.copy_from_slice(&result);
    out
}

/// Create a key backup, wrapping the REK under a BEK derived from `backup_passphrase`.
#[allow(clippy::too_many_arguments)]
pub fn create_key_backup(
    rek: &[u8; KEY_SIZE],
    backup_passphrase: &[u8],
    file_id: u64,
    cipher_id: CipherId,
    kdf_algorithm: KdfAlgorithm,
    kdf_param1: u32,
    kdf_param2: u32,
    kdf_param3: u32,
    epoch: u32,
) -> citadel_core::Result<[u8; KEY_BACKUP_SIZE]> {
    let backup_salt = crate::kdf::generate_salt();

    let mut bek = derive_mk(
        kdf_algorithm,
        backup_passphrase,
        &backup_salt,
        kdf_param1,
        kdf_param2,
        kdf_param3,
    )?;

    let wrapped = wrap_rek(&bek, rek);

    let mut backup = KeyBackup {
        magic: KEY_BACKUP_MAGIC,
        version: KEY_BACKUP_VERSION,
        file_id,
        cipher_id,
        kdf_algorithm,
        kdf_param1,
        kdf_param2,
        kdf_param3,
        backup_salt,
        wrapped_rek: wrapped,
        epoch,
        hmac: [0u8; MAC_SIZE],
    };
    backup.update_hmac(&bek);
    bek.zeroize();

    Ok(backup.serialize())
}

/// Restore a REK from a backup file.
pub fn restore_rek_from_backup(
    backup_data: &[u8; KEY_BACKUP_SIZE],
    backup_passphrase: &[u8],
) -> citadel_core::Result<RestoreResult> {
    let backup = KeyBackup::deserialize(backup_data)?;

    let mut bek = derive_mk(
        backup.kdf_algorithm,
        backup_passphrase,
        &backup.backup_salt,
        backup.kdf_param1,
        backup.kdf_param2,
        backup.kdf_param3,
    )?;

    backup.verify_hmac(&bek)?;

    let mut rek =
        unwrap_rek(&bek, &backup.wrapped_rek).map_err(|_| citadel_core::Error::BadPassphrase)?;
    bek.zeroize();

    let keys = derive_keys_from_rek(&rek);

    let result = RestoreResult {
        rek,
        keys,
        file_id: backup.file_id,
        cipher_id: backup.cipher_id,
        kdf_algorithm: backup.kdf_algorithm,
        kdf_param1: backup.kdf_param1,
        kdf_param2: backup.kdf_param2,
        kdf_param3: backup.kdf_param3,
        epoch: backup.epoch,
    };
    rek.zeroize();

    Ok(result)
}

pub struct RestoreResult {
    pub rek: [u8; KEY_SIZE],
    pub keys: crate::hkdf_utils::DerivedKeys,
    pub file_id: u64,
    pub cipher_id: CipherId,
    pub kdf_algorithm: KdfAlgorithm,
    pub kdf_param1: u32,
    pub kdf_param2: u32,
    pub kdf_param3: u32,
    pub epoch: u32,
}

impl Drop for RestoreResult {
    fn drop(&mut self) {
        self.rek.zeroize();
    }
}

#[cfg(test)]
mod tests {
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
}
