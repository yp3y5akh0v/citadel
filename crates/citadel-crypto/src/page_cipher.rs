use aes::Aes256;
use cipher::{KeyIvInit, StreamCipher};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use citadel_core::types::PageId;
use citadel_core::{BODY_SIZE, DEK_SIZE, IV_SIZE, MAC_KEY_SIZE, MAC_SIZE, PAGE_SIZE};

type Aes256Ctr = ctr::Ctr128BE<Aes256>;
type HmacSha256 = Hmac<Sha256>;

/// Encrypt a page body in-place and produce the on-disk page format.
///
/// Input: `body` = 8160 bytes of plaintext page body
/// Output: `out` = 8208 bytes = [IV(16) | ciphertext(8160) | MAC(32)]
///
/// MAC = HMAC-SHA256(mac_key, epoch_le(4) || page_id_le(4) || iv(16) || ciphertext(8160))
pub fn encrypt_page(
    dek: &[u8; DEK_SIZE],
    mac_key: &[u8; MAC_KEY_SIZE],
    page_id: PageId,
    encryption_epoch: u32,
    body: &[u8; BODY_SIZE],
    out: &mut [u8; PAGE_SIZE],
) {
    // Generate random IV
    let mut iv = [0u8; IV_SIZE];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut iv);

    encrypt_page_with_iv(dek, mac_key, page_id, encryption_epoch, body, &iv, out);
}

/// Encrypt with a specific IV (used for testing determinism).
pub fn encrypt_page_with_iv(
    dek: &[u8; DEK_SIZE],
    mac_key: &[u8; MAC_KEY_SIZE],
    page_id: PageId,
    encryption_epoch: u32,
    body: &[u8; BODY_SIZE],
    iv: &[u8; IV_SIZE],
    out: &mut [u8; PAGE_SIZE],
) {
    // Write IV
    out[..IV_SIZE].copy_from_slice(iv);

    // Encrypt body with AES-256-CTR
    out[IV_SIZE..IV_SIZE + BODY_SIZE].copy_from_slice(body);
    let mut cipher = Aes256Ctr::new(dek.into(), iv.into());
    cipher.apply_keystream(&mut out[IV_SIZE..IV_SIZE + BODY_SIZE]);

    // Compute MAC = HMAC-SHA256(mac_key, epoch_le || page_id_le || iv || ciphertext)
    let mac = compute_mac(
        mac_key,
        encryption_epoch,
        page_id,
        iv,
        &out[IV_SIZE..IV_SIZE + BODY_SIZE],
    );
    out[IV_SIZE + BODY_SIZE..].copy_from_slice(&mac);
}

/// Verify MAC and decrypt a page. Returns the decrypted body.
///
/// INVARIANT: MAC is verified BEFORE decryption. AES-CTR is malleable —
/// decrypting tampered ciphertext produces attacker-controlled plaintext.
///
/// Input: `data` = 8208 bytes = [IV(16) | ciphertext(8160) | MAC(32)]
/// Output: `body` = 8160 bytes of decrypted page body
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

    // INVARIANT: Verify HMAC BEFORE decryption
    let computed_mac = compute_mac(
        mac_key,
        encryption_epoch,
        page_id,
        iv.try_into().unwrap(),
        ciphertext,
    );

    // Constant-time comparison to prevent timing side-channels
    if stored_mac.ct_eq(&computed_mac).into() {
        // MAC valid — safe to decrypt
        body.copy_from_slice(ciphertext);
        let mut cipher = Aes256Ctr::new(dek.into(), iv.into());
        cipher.apply_keystream(body);
        Ok(())
    } else {
        Err(citadel_core::Error::PageTampered(page_id))
    }
}

/// Compute HMAC-SHA256(mac_key, epoch_le(4) || page_id_le(4) || iv(16) || ciphertext(8160))
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

/// Compute dek_id = HMAC-SHA256(MAC_KEY, DEK) for commit slot key commitment.
pub fn compute_dek_id(mac_key: &[u8; MAC_KEY_SIZE], dek: &[u8; DEK_SIZE]) -> [u8; MAC_SIZE] {
    let mut mac = HmacSha256::new_from_slice(mac_key).expect("HMAC key size is always valid");
    mac.update(dek);
    let result = mac.finalize().into_bytes();
    let mut out = [0u8; MAC_SIZE];
    out.copy_from_slice(&result);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_keys() -> ([u8; DEK_SIZE], [u8; MAC_KEY_SIZE]) {
        let dek = [0xAA; DEK_SIZE];
        let mac_key = [0xBB; MAC_KEY_SIZE];
        (dek, mac_key)
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let (dek, mac_key) = test_keys();
        let page_id = PageId(42);
        let epoch = 1u32;

        let mut body = [0u8; BODY_SIZE];
        body[0..8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        body[8000] = 0xFF;

        let mut encrypted = [0u8; PAGE_SIZE];
        encrypt_page(&dek, &mac_key, page_id, epoch, &body, &mut encrypted);

        // Ciphertext should differ from plaintext
        assert_ne!(&encrypted[IV_SIZE..IV_SIZE + BODY_SIZE], &body[..]);

        let mut decrypted = [0u8; BODY_SIZE];
        decrypt_page(&dek, &mac_key, page_id, epoch, &encrypted, &mut decrypted).unwrap();

        assert_eq!(decrypted, body);
    }

    #[test]
    fn tamper_detection_ciphertext() {
        let (dek, mac_key) = test_keys();
        let page_id = PageId(1);
        let epoch = 1u32;
        let body = [0x42u8; BODY_SIZE];

        let mut encrypted = [0u8; PAGE_SIZE];
        encrypt_page(&dek, &mac_key, page_id, epoch, &body, &mut encrypted);

        // Flip a bit in the ciphertext
        encrypted[IV_SIZE + 100] ^= 0x01;

        let mut decrypted = [0u8; BODY_SIZE];
        let result = decrypt_page(&dek, &mac_key, page_id, epoch, &encrypted, &mut decrypted);
        assert!(matches!(result, Err(citadel_core::Error::PageTampered(_))));
    }

    #[test]
    fn tamper_detection_iv() {
        let (dek, mac_key) = test_keys();
        let page_id = PageId(1);
        let epoch = 1u32;
        let body = [0x42u8; BODY_SIZE];

        let mut encrypted = [0u8; PAGE_SIZE];
        encrypt_page(&dek, &mac_key, page_id, epoch, &body, &mut encrypted);

        // Flip a bit in the IV
        encrypted[0] ^= 0x01;

        let mut decrypted = [0u8; BODY_SIZE];
        let result = decrypt_page(&dek, &mac_key, page_id, epoch, &encrypted, &mut decrypted);
        assert!(matches!(result, Err(citadel_core::Error::PageTampered(_))));
    }

    #[test]
    fn tamper_detection_mac() {
        let (dek, mac_key) = test_keys();
        let page_id = PageId(1);
        let epoch = 1u32;
        let body = [0x42u8; BODY_SIZE];

        let mut encrypted = [0u8; PAGE_SIZE];
        encrypt_page(&dek, &mac_key, page_id, epoch, &body, &mut encrypted);

        // Flip a bit in the MAC
        encrypted[PAGE_SIZE - 1] ^= 0x01;

        let mut decrypted = [0u8; BODY_SIZE];
        let result = decrypt_page(&dek, &mac_key, page_id, epoch, &encrypted, &mut decrypted);
        assert!(matches!(result, Err(citadel_core::Error::PageTampered(_))));
    }

    #[test]
    fn wrong_page_id_detected() {
        let (dek, mac_key) = test_keys();
        let epoch = 1u32;
        let body = [0x42u8; BODY_SIZE];

        let mut encrypted = [0u8; PAGE_SIZE];
        encrypt_page(&dek, &mac_key, PageId(1), epoch, &body, &mut encrypted);

        // Try to decrypt with wrong page_id
        let mut decrypted = [0u8; BODY_SIZE];
        let result = decrypt_page(&dek, &mac_key, PageId(2), epoch, &encrypted, &mut decrypted);
        assert!(matches!(result, Err(citadel_core::Error::PageTampered(_))));
    }

    #[test]
    fn wrong_epoch_detected() {
        let (dek, mac_key) = test_keys();
        let page_id = PageId(1);
        let body = [0x42u8; BODY_SIZE];

        let mut encrypted = [0u8; PAGE_SIZE];
        encrypt_page(&dek, &mac_key, page_id, 1, &body, &mut encrypted);

        // Try to decrypt with wrong epoch (cross-epoch swap attack)
        let mut decrypted = [0u8; BODY_SIZE];
        let result = decrypt_page(&dek, &mac_key, page_id, 2, &encrypted, &mut decrypted);
        assert!(matches!(result, Err(citadel_core::Error::PageTampered(_))));
    }

    #[test]
    fn wrong_key_detected() {
        let (_dek, mac_key) = test_keys();
        let wrong_dek = [0xCC; DEK_SIZE];
        let wrong_mac_key = [0xDD; MAC_KEY_SIZE];
        let page_id = PageId(1);
        let epoch = 1u32;
        let body = [0x42u8; BODY_SIZE];

        let mut encrypted = [0u8; PAGE_SIZE];
        encrypt_page(&_dek, &mac_key, page_id, epoch, &body, &mut encrypted);

        let mut decrypted = [0u8; BODY_SIZE];
        let result = decrypt_page(
            &wrong_dek,
            &wrong_mac_key,
            page_id,
            epoch,
            &encrypted,
            &mut decrypted,
        );
        assert!(matches!(result, Err(citadel_core::Error::PageTampered(_))));
    }

    #[test]
    fn deterministic_with_fixed_iv() {
        let (dek, mac_key) = test_keys();
        let page_id = PageId(1);
        let epoch = 1u32;
        let body = [0x42u8; BODY_SIZE];
        let iv = [0x01u8; IV_SIZE];

        let mut enc1 = [0u8; PAGE_SIZE];
        let mut enc2 = [0u8; PAGE_SIZE];
        encrypt_page_with_iv(&dek, &mac_key, page_id, epoch, &body, &iv, &mut enc1);
        encrypt_page_with_iv(&dek, &mac_key, page_id, epoch, &body, &iv, &mut enc2);

        assert_eq!(enc1, enc2);
    }

    #[test]
    fn random_iv_produces_different_ciphertext() {
        let (dek, mac_key) = test_keys();
        let page_id = PageId(1);
        let epoch = 1u32;
        let body = [0x42u8; BODY_SIZE];

        let mut enc1 = [0u8; PAGE_SIZE];
        let mut enc2 = [0u8; PAGE_SIZE];
        encrypt_page(&dek, &mac_key, page_id, epoch, &body, &mut enc1);
        encrypt_page(&dek, &mac_key, page_id, epoch, &body, &mut enc2);

        // IVs should differ (random)
        assert_ne!(&enc1[..IV_SIZE], &enc2[..IV_SIZE]);
        // Ciphertext should differ (different IVs)
        assert_ne!(
            &enc1[IV_SIZE..IV_SIZE + BODY_SIZE],
            &enc2[IV_SIZE..IV_SIZE + BODY_SIZE]
        );
    }

    #[test]
    fn dek_id_deterministic() {
        let mac_key = [0xBB; MAC_KEY_SIZE];
        let dek = [0xAA; DEK_SIZE];
        let id1 = compute_dek_id(&mac_key, &dek);
        let id2 = compute_dek_id(&mac_key, &dek);
        assert_eq!(id1, id2);
    }

    #[test]
    fn dek_id_different_keys() {
        let mac_key = [0xBB; MAC_KEY_SIZE];
        let dek1 = [0xAA; DEK_SIZE];
        let dek2 = [0xCC; DEK_SIZE];
        let id1 = compute_dek_id(&mac_key, &dek1);
        let id2 = compute_dek_id(&mac_key, &dek2);
        assert_ne!(id1, id2);
    }
}
