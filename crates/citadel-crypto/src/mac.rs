//! Standalone HMAC-SHA256 for slot/header integrity in citadel-mem's region key
//! store (torn-write detection). The secrecy of a region's wrapped content key is
//! provided by AES-256-KW, NOT by this MAC; this only authenticates the on-disk
//! slot bytes so a partial/torn 512-byte write is detected rather than read as a key.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use citadel_core::MAC_SIZE;

type HmacSha256 = Hmac<Sha256>;

/// HMAC-SHA256(`key`, `data`) -> 32 bytes.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; MAC_SIZE] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    let tag = mac.finalize().into_bytes();
    let mut out = [0u8; MAC_SIZE];
    out.copy_from_slice(&tag);
    out
}

/// Constant-time check that `tag` is the HMAC-SHA256 of `data` under `key`.
pub fn verify_hmac_sha256(key: &[u8], data: &[u8], tag: &[u8; MAC_SIZE]) -> bool {
    hmac_sha256(key, data).ct_eq(tag).into()
}

#[cfg(test)]
#[path = "mac_tests.rs"]
mod tests;
