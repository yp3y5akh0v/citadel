use aes::Aes256;
use cipher::{KeyIvInit, StreamCipher};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use citadel_core::types::PageId;
use citadel_core::{BODY_SIZE, DEK_SIZE, IV_SIZE, MAC_KEY_SIZE, MAC_SIZE, PAGE_SIZE};

type Aes256Ctr = ctr::Ctr128BE<Aes256>;
type HmacSha256 = Hmac<Sha256>;

/// Encrypt: body(8160) -> [IV(16) | ciphertext(8160) | MAC(32)]
pub fn encrypt_page(
    dek: &[u8; DEK_SIZE],
    mac_key: &[u8; MAC_KEY_SIZE],
    page_id: PageId,
    encryption_epoch: u32,
    body: &[u8; BODY_SIZE],
    out: &mut [u8; PAGE_SIZE],
) {
    let mut iv = [0u8; IV_SIZE];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut iv);

    encrypt_page_with_iv(dek, mac_key, page_id, encryption_epoch, body, &iv, out);
}

pub fn encrypt_page_with_iv(
    dek: &[u8; DEK_SIZE],
    mac_key: &[u8; MAC_KEY_SIZE],
    page_id: PageId,
    encryption_epoch: u32,
    body: &[u8; BODY_SIZE],
    iv: &[u8; IV_SIZE],
    out: &mut [u8; PAGE_SIZE],
) {
    out[..IV_SIZE].copy_from_slice(iv);
    out[IV_SIZE..IV_SIZE + BODY_SIZE].copy_from_slice(body);
    let mut cipher = Aes256Ctr::new(dek.into(), iv.into());
    cipher.apply_keystream(&mut out[IV_SIZE..IV_SIZE + BODY_SIZE]);

    let mac = compute_mac(
        mac_key,
        encryption_epoch,
        page_id,
        iv,
        &out[IV_SIZE..IV_SIZE + BODY_SIZE],
    );
    out[IV_SIZE + BODY_SIZE..].copy_from_slice(&mac);
}

/// Decrypt: [IV(16) | ciphertext(8160) | MAC(32)] -> body(8160).
/// HMAC verified before decryption (AES-CTR is malleable).
pub fn decrypt_page(
    dek: &[u8; DEK_SIZE],
    mac_key: &[u8; MAC_KEY_SIZE],
    page_id: PageId,
    encryption_epoch: u32,
    data: &[u8; PAGE_SIZE],
    body: &mut [u8; BODY_SIZE],
) -> citadel_core::Result<()> {
    let iv = &data[..IV_SIZE];
    let ciphertext = &data[IV_SIZE..IV_SIZE + BODY_SIZE];
    let stored_mac = &data[IV_SIZE + BODY_SIZE..];

    let computed_mac = compute_mac(
        mac_key,
        encryption_epoch,
        page_id,
        iv.try_into().unwrap(),
        ciphertext,
    );

    if stored_mac.ct_eq(&computed_mac).into() {
        body.copy_from_slice(ciphertext);
        let mut cipher = Aes256Ctr::new(dek.into(), iv.into());
        cipher.apply_keystream(body);
        Ok(())
    } else {
        Err(citadel_core::Error::PageTampered(page_id))
    }
}

#[derive(Clone)]
pub struct HmacState {
    base: HmacSha256,
}

impl HmacState {
    pub fn new(mac_key: &[u8; MAC_KEY_SIZE], epoch: u32) -> Self {
        let mut base = HmacSha256::new_from_slice(mac_key).expect("HMAC key size is always valid");
        base.update(&epoch.to_le_bytes());
        Self { base }
    }

    fn compute_mac(
        &self,
        page_id: PageId,
        iv: &[u8; IV_SIZE],
        ciphertext: &[u8],
    ) -> [u8; MAC_SIZE] {
        let mut mac = self.base.clone();
        mac.update(&page_id.as_u32().to_le_bytes());
        mac.update(iv);
        mac.update(ciphertext);
        let result = mac.finalize().into_bytes();
        let mut out = [0u8; MAC_SIZE];
        out.copy_from_slice(&result);
        out
    }
}

pub fn encrypt_page_with_hmac(
    dek: &[u8; DEK_SIZE],
    hmac_state: &HmacState,
    page_id: PageId,
    body: &[u8; BODY_SIZE],
    out: &mut [u8; PAGE_SIZE],
) {
    let mut iv = [0u8; IV_SIZE];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut iv);

    out[..IV_SIZE].copy_from_slice(&iv);
    let mut cipher = Aes256Ctr::new(dek.into(), (&iv).into());
    cipher
        .apply_keystream_b2b(body, &mut out[IV_SIZE..IV_SIZE + BODY_SIZE])
        .expect("body/out size match");

    let mac = hmac_state.compute_mac(page_id, &iv, &out[IV_SIZE..IV_SIZE + BODY_SIZE]);
    out[IV_SIZE + BODY_SIZE..].copy_from_slice(&mac);
}

fn compute_mac(
    mac_key: &[u8; MAC_KEY_SIZE],
    epoch: u32,
    page_id: PageId,
    iv: &[u8; IV_SIZE],
    ciphertext: &[u8],
) -> [u8; MAC_SIZE] {
    let mut mac = HmacSha256::new_from_slice(mac_key).expect("HMAC key size is always valid");
    mac.update(&epoch.to_le_bytes());
    mac.update(&page_id.as_u32().to_le_bytes());
    mac.update(iv);
    mac.update(ciphertext);
    let result = mac.finalize().into_bytes();
    let mut out = [0u8; MAC_SIZE];
    out.copy_from_slice(&result);
    out
}

pub fn compute_dek_id(mac_key: &[u8; MAC_KEY_SIZE], dek: &[u8; DEK_SIZE]) -> [u8; MAC_SIZE] {
    let mut mac = HmacSha256::new_from_slice(mac_key).expect("HMAC key size is always valid");
    mac.update(dek);
    let result = mac.finalize().into_bytes();
    let mut out = [0u8; MAC_SIZE];
    out.copy_from_slice(&result);
    out
}

#[cfg(test)]
#[path = "page_cipher_tests.rs"]
mod tests;
