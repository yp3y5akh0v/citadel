//! Variable-length envelope sealing for application content (citadel-mem atoms).
//!
//! Sibling of [`crate::page_cipher`] using the same primitives (AES-256-CTR +
//! HMAC-SHA256, encrypt-then-MAC, MAC verified before decrypt). `page_cipher` is
//! hardwired to fixed page sizes and a `page_id`-bound MAC, so it cannot be reused
//! verbatim. Keys come from a per-atom random content key (ACK; see
//! [`crate::hkdf_utils::derive_seal_keys`]); `aad` binds the seal to its atom id so a
//! blob cannot be replayed into another row.

use aes::Aes256;
use cipher::{KeyIvInit, StreamCipher};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use citadel_core::{IV_SIZE, MAC_SIZE};

use crate::hkdf_utils::SealKeys;

type Aes256Ctr = ctr::Ctr128BE<Aes256>;
type HmacSha256 = Hmac<Sha256>;

/// Seal `plaintext` to `[IV(16) | ciphertext(len) | MAC(32)]`.
///
/// `aad` (the atom id) is authenticated but not encrypted, binding the blob to its row.
pub fn seal(keys: &SealKeys, aad: u64, plaintext: &[u8]) -> Vec<u8> {
    let mut iv = [0u8; IV_SIZE];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut iv);

    let mut ct = plaintext.to_vec();
    let mut cipher = Aes256Ctr::new((&keys.dek).into(), (&iv).into());
    cipher.apply_keystream(&mut ct);

    let mut mac = HmacSha256::new_from_slice(&keys.mac_key).expect("HMAC key size is always valid");
    mac.update(&aad.to_le_bytes());
    mac.update(&iv);
    mac.update(&ct);
    let tag = mac.finalize().into_bytes();

    let mut out = Vec::with_capacity(IV_SIZE + ct.len() + MAC_SIZE);
    out.extend_from_slice(&iv);
    out.extend_from_slice(&ct);
    out.extend_from_slice(&tag);
    out
}

/// Open a [`seal`] blob. The MAC is verified in constant time before any decryption;
/// a wrong key (e.g. after the region was forgotten) yields `RegionSealTampered`.
pub fn open(keys: &SealKeys, aad: u64, blob: &[u8]) -> citadel_core::Result<Vec<u8>> {
    if blob.len() < IV_SIZE + MAC_SIZE {
        return Err(citadel_core::Error::RegionSealTampered);
    }
    let iv = &blob[..IV_SIZE];
    let ct = &blob[IV_SIZE..blob.len() - MAC_SIZE];
    let stored_mac = &blob[blob.len() - MAC_SIZE..];

    let mut mac = HmacSha256::new_from_slice(&keys.mac_key).expect("HMAC key size is always valid");
    mac.update(&aad.to_le_bytes());
    mac.update(iv);
    mac.update(ct);
    let computed = mac.finalize().into_bytes();

    if computed.ct_eq(stored_mac).into() {
        let mut pt = ct.to_vec();
        let mut cipher = Aes256Ctr::new((&keys.dek).into(), iv.into());
        cipher.apply_keystream(&mut pt);
        Ok(pt)
    } else {
        Err(citadel_core::Error::RegionSealTampered)
    }
}

#[cfg(test)]
#[path = "blob_seal_tests.rs"]
mod tests;
