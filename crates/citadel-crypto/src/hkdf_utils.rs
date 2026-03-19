use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroize;

use citadel_core::{DEK_SIZE, KEY_SIZE, MAC_KEY_SIZE};
use citadel_core::{
    HKDF_INFO_AUDIT_KEY, HKDF_INFO_DEK, HKDF_INFO_KEYFILE_MAC, HKDF_INFO_KMS_MASTER,
    HKDF_INFO_MAC_KEY, HKDF_KMS_SALT,
};

pub struct DerivedKeys {
    pub dek: [u8; DEK_SIZE],
    pub mac_key: [u8; MAC_KEY_SIZE],
    pub audit_key: [u8; KEY_SIZE],
}

impl Drop for DerivedKeys {
    fn drop(&mut self) {
        self.dek.zeroize();
        self.mac_key.zeroize();
        self.audit_key.zeroize();
    }
}

/// Derive DEK and MAC_KEY from REK via HKDF-SHA256.
///
/// DEK = HKDF-SHA256(ikm=REK, salt=zeros(32), info="citadel-dek-v1", len=32)
/// MAC_KEY = HKDF-SHA256(ikm=REK, salt=zeros(32), info="citadel-mac-key-v1", len=32)
pub fn derive_keys_from_rek(rek: &[u8; KEY_SIZE]) -> DerivedKeys {
    let salt = [0u8; 32];

    let hk = Hkdf::<Sha256>::new(Some(&salt), rek);
    let mut dek = [0u8; DEK_SIZE];
    hk.expand(HKDF_INFO_DEK, &mut dek)
        .expect("HKDF expand should not fail for 32-byte output");

    let hk = Hkdf::<Sha256>::new(Some(&salt), rek);
    let mut mac_key = [0u8; MAC_KEY_SIZE];
    hk.expand(HKDF_INFO_MAC_KEY, &mut mac_key)
        .expect("HKDF expand should not fail for 32-byte output");

    let hk = Hkdf::<Sha256>::new(Some(&salt), rek);
    let mut audit_key = [0u8; KEY_SIZE];
    hk.expand(HKDF_INFO_AUDIT_KEY, &mut audit_key)
        .expect("HKDF expand should not fail for 32-byte output");

    DerivedKeys {
        dek,
        mac_key,
        audit_key,
    }
}

/// Derive the key file MAC key from the Master Key.
///
/// mac_from_mk = HKDF-SHA256(ikm=MK, salt=zeros(32), info="citadel-keyfile-mac", len=32)
pub fn derive_keyfile_mac_key(mk: &[u8; KEY_SIZE]) -> [u8; KEY_SIZE] {
    let salt = [0u8; 32];
    let hk = Hkdf::<Sha256>::new(Some(&salt), mk);
    let mut key = [0u8; KEY_SIZE];
    hk.expand(HKDF_INFO_KEYFILE_MAC, &mut key)
        .expect("HKDF expand should not fail for 32-byte output");
    key
}

/// Derive Master Key from KMS-provided raw bytes via HKDF.
///
/// MK = HKDF-SHA256(ikm=kms_bytes, salt="citadel-v1", info="citadel-master-key", len=32)
pub fn derive_mk_from_kms(kms_bytes: &[u8]) -> [u8; KEY_SIZE] {
    let hk = Hkdf::<Sha256>::new(Some(HKDF_KMS_SALT), kms_bytes);
    let mut mk = [0u8; KEY_SIZE];
    hk.expand(HKDF_INFO_KMS_MASTER, &mut mk)
        .expect("HKDF expand should not fail for 32-byte output");
    mk
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_keys_deterministic() {
        let rek = [0x42u8; 32];
        let keys1 = derive_keys_from_rek(&rek);
        let keys2 = derive_keys_from_rek(&rek);
        assert_eq!(keys1.dek, keys2.dek);
        assert_eq!(keys1.mac_key, keys2.mac_key);
    }

    #[test]
    fn dek_and_mac_key_differ() {
        let rek = [0x42u8; 32];
        let keys = derive_keys_from_rek(&rek);
        assert_ne!(keys.dek, keys.mac_key, "DEK and MAC_KEY must be different");
    }

    #[test]
    fn different_rek_different_keys() {
        let rek1 = [0x01u8; 32];
        let rek2 = [0x02u8; 32];
        let keys1 = derive_keys_from_rek(&rek1);
        let keys2 = derive_keys_from_rek(&rek2);
        assert_ne!(keys1.dek, keys2.dek);
        assert_ne!(keys1.mac_key, keys2.mac_key);
    }

    #[test]
    fn keyfile_mac_key_deterministic() {
        let mk = [0xABu8; 32];
        let k1 = derive_keyfile_mac_key(&mk);
        let k2 = derive_keyfile_mac_key(&mk);
        assert_eq!(k1, k2);
    }

    #[test]
    fn keyfile_mac_key_differs_from_dek() {
        let key = [0xABu8; 32];
        let mac_key = derive_keyfile_mac_key(&key);
        let keys = derive_keys_from_rek(&key);
        assert_ne!(mac_key, keys.dek);
        assert_ne!(mac_key, keys.mac_key);
    }

    #[test]
    fn kms_derivation() {
        let kms_bytes = b"some-kms-provided-material";
        let mk1 = derive_mk_from_kms(kms_bytes);
        let mk2 = derive_mk_from_kms(kms_bytes);
        assert_eq!(mk1, mk2);

        let mk3 = derive_mk_from_kms(b"different-material");
        assert_ne!(mk1, mk3);
    }
}
