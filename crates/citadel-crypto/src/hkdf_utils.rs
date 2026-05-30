use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroize;

use citadel_core::{DEK_SIZE, KEY_SIZE, MAC_KEY_SIZE, WRAPPED_KEY_SIZE};
use citadel_core::{
    HKDF_INFO_ATOM_WRAP, HKDF_INFO_AUDIT_KEY, HKDF_INFO_DEK, HKDF_INFO_KEYFILE_MAC,
    HKDF_INFO_KMS_MASTER, HKDF_INFO_MAC_KEY, HKDF_INFO_RCK_DEK, HKDF_INFO_RCK_MAC,
    HKDF_INFO_REGION_STORE_MAC, HKDF_INFO_REGION_WRAP, HKDF_KMS_SALT,
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

/// HMAC key for region key-store integrity / torn-write detection, from the REK.
/// This authenticates slot bytes; it does NOT protect RCK secrecy (AES-KW does).
pub fn derive_region_store_mac_key(rek: &[u8; KEY_SIZE]) -> [u8; KEY_SIZE] {
    let salt = [0u8; 32];
    let hk = Hkdf::<Sha256>::new(Some(&salt), rek);
    let mut key = [0u8; KEY_SIZE];
    hk.expand(HKDF_INFO_REGION_STORE_MAC, &mut key)
        .expect("HKDF expand should not fail for 32-byte output");
    key
}

/// Region-erasure secrets: `kek` wraps RCK (AES-256-KW),
/// `store_mac_key` authenticates key-store slots. Zeroized on drop.
pub struct RegionWrapKeys {
    pub kek: [u8; KEY_SIZE],
    pub store_mac_key: [u8; KEY_SIZE],
}

impl Drop for RegionWrapKeys {
    fn drop(&mut self) {
        self.kek.zeroize();
        self.store_mac_key.zeroize();
    }
}

impl RegionWrapKeys {
    /// Wrap a region's random content key (RCK) under the region KEK (AES-256-KW).
    /// The 40-byte result is the SOLE copy of the RCK; destroying it erases the region.
    pub fn wrap_region_key(&self, rck: &[u8; KEY_SIZE]) -> [u8; WRAPPED_KEY_SIZE] {
        crate::key_manager::wrap_rek(&self.kek, rck)
    }

    /// Unwrap a region RCK. Fails (AES-KW integrity) if the slot was erased (zeroed).
    pub fn unwrap_region_key(
        &self,
        wrapped: &[u8; WRAPPED_KEY_SIZE],
    ) -> citadel_core::Result<[u8; KEY_SIZE]> {
        crate::key_manager::unwrap_rek(&self.kek, wrapped)
    }
}

/// Derive the region wrap KEK and store MAC key from the REK.
pub fn derive_region_wrap_keys(rek: &[u8; KEY_SIZE]) -> RegionWrapKeys {
    let salt = [0u8; 32];
    let hk = Hkdf::<Sha256>::new(Some(&salt), rek);
    let mut kek = [0u8; KEY_SIZE];
    hk.expand(HKDF_INFO_REGION_WRAP, &mut kek)
        .expect("HKDF expand should not fail for 32-byte output");
    RegionWrapKeys {
        kek,
        store_mac_key: derive_region_store_mac_key(rek),
    }
}

/// Content-sealing keys derived from a per-atom RANDOM content key (ACK). Because the
/// ACK is random, destroying its sole wrapped copy makes these non-recomputable.
pub struct SealKeys {
    pub dek: [u8; KEY_SIZE],
    pub mac_key: [u8; MAC_KEY_SIZE],
}

impl Drop for SealKeys {
    fn drop(&mut self) {
        self.dek.zeroize();
        self.mac_key.zeroize();
    }
}

/// dek = HKDF(ack, "citadel-rck-dek-v1"); mac_key = HKDF(ack, "citadel-rck-mac-v1"). The
/// `-rck-` info strings are a historical wire-format constant (unchanged so existing
/// sealed data stays readable); the input is the per-atom ACK.
pub fn derive_seal_keys(ack: &[u8; KEY_SIZE]) -> SealKeys {
    let salt = [0u8; 32];

    let hk = Hkdf::<Sha256>::new(Some(&salt), ack);
    let mut dek = [0u8; KEY_SIZE];
    hk.expand(HKDF_INFO_RCK_DEK, &mut dek)
        .expect("HKDF expand should not fail for 32-byte output");

    let hk = Hkdf::<Sha256>::new(Some(&salt), ack);
    let mut mac_key = [0u8; MAC_KEY_SIZE];
    hk.expand(HKDF_INFO_RCK_MAC, &mut mac_key)
        .expect("HKDF expand should not fail for 32-byte output");

    SealKeys { dek, mac_key }
}

/// Per-region atom wrap KEK (AES-256-KW). Destroying RCK makes all wrapped
/// ACKs permanently unwrappable. Zeroized on drop.
pub struct AtomWrapKey {
    pub kek: [u8; KEY_SIZE],
}

impl Drop for AtomWrapKey {
    fn drop(&mut self) {
        self.kek.zeroize();
    }
}

impl AtomWrapKey {
    /// Wrap an atom's random content key (ACK) under the region atom KEK (AES-256-KW).
    /// The 40-byte result is the SOLE copy of the ACK; destroying it erases that atom.
    pub fn wrap_atom_key(&self, ack: &[u8; KEY_SIZE]) -> [u8; WRAPPED_KEY_SIZE] {
        crate::key_manager::wrap_rek(&self.kek, ack)
    }

    /// Unwrap an atom ACK. Fails (AES-KW integrity) if the slot was erased (zeroed).
    pub fn unwrap_atom_key(
        &self,
        wrapped: &[u8; WRAPPED_KEY_SIZE],
    ) -> citadel_core::Result<[u8; KEY_SIZE]> {
        crate::key_manager::unwrap_rek(&self.kek, wrapped)
    }
}

/// Derive the per-region atom-wrap KEK from the region's random content key (RCK).
pub fn derive_atom_wrap_key(rck: &[u8; KEY_SIZE]) -> AtomWrapKey {
    let salt = [0u8; 32];
    let hk = Hkdf::<Sha256>::new(Some(&salt), rck);
    let mut kek = [0u8; KEY_SIZE];
    hk.expand(HKDF_INFO_ATOM_WRAP, &mut kek)
        .expect("HKDF expand should not fail for 32-byte output");
    AtomWrapKey { kek }
}

#[cfg(test)]
#[path = "hkdf_utils_tests.rs"]
mod tests;
