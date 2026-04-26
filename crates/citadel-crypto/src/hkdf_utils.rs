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
#[path = "hkdf_utils_tests.rs"]
mod tests;
