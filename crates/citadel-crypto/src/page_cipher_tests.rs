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

    assert_ne!(&enc1[..IV_SIZE], &enc2[..IV_SIZE]);

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
