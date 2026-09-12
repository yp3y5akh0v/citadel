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

#[test]
fn cached_hmac_decrypt_matches_raw_keys_across_contexts_and_pages() {
    for (dek, mac_key, epoch) in [
        ([0x12; DEK_SIZE], [0x34; MAC_KEY_SIZE], 0),
        ([0x56; DEK_SIZE], [0x78; MAC_KEY_SIZE], u32::MAX),
    ] {
        let state = HmacState::new(&mac_key, epoch);
        for id in [0, 42, u32::MAX] {
            let page_id = PageId(id);
            let mut body = [0u8; BODY_SIZE];
            for (i, byte) in body.iter_mut().enumerate() {
                *byte = (i as u8).wrapping_mul(17).wrapping_add(id as u8);
            }
            let iv = [id as u8; IV_SIZE];
            let mut encrypted = [0; PAGE_SIZE];
            encrypt_page_with_iv(&dek, &mac_key, page_id, epoch, &body, &iv, &mut encrypted);
            let mut raw = [0xA5; BODY_SIZE];
            let mut cached = [0x5A; BODY_SIZE];
            decrypt_page(&dek, &mac_key, page_id, epoch, &encrypted, &mut raw).unwrap();
            decrypt_page_with_hmac(&dek, &state, page_id, &encrypted, &mut cached).unwrap();
            assert_eq!(raw, body);
            assert_eq!(cached, raw);
        }
    }
}

#[test]
fn cached_hmac_decrypt_rejects_tampering_before_touching_output() {
    fn rejected(
        dek: &[u8; DEK_SIZE],
        mac_key: &[u8; MAC_KEY_SIZE],
        epoch: u32,
        page_id: PageId,
        encrypted: &[u8; PAGE_SIZE],
        state: &HmacState,
    ) {
        let mut raw = [0xA5; BODY_SIZE];
        let mut cached = [0x5A; BODY_SIZE];
        assert!(
            matches!(decrypt_page(dek, mac_key, page_id, epoch, encrypted, &mut raw),
            Err(citadel_core::Error::PageTampered(id)) if id == page_id)
        );
        assert!(
            matches!(decrypt_page_with_hmac(dek, state, page_id, encrypted, &mut cached),
            Err(citadel_core::Error::PageTampered(id)) if id == page_id)
        );
        assert_eq!(raw, [0xA5; BODY_SIZE]);
        assert_eq!(cached, [0x5A; BODY_SIZE]);
    }

    let (dek, mac_key) = test_keys();
    let epoch = 17;
    let page_id = PageId(9);
    let body = [0x42; BODY_SIZE];
    let mut encrypted = [0; PAGE_SIZE];
    encrypt_page_with_iv(
        &dek,
        &mac_key,
        page_id,
        epoch,
        &body,
        &[0x83; IV_SIZE],
        &mut encrypted,
    );
    let state = HmacState::new(&mac_key, epoch);
    let mut offsets = vec![
        0,
        IV_SIZE - 1,
        IV_SIZE,
        IV_SIZE + BODY_SIZE / 2,
        IV_SIZE + BODY_SIZE - 1,
    ];
    offsets.extend(IV_SIZE + BODY_SIZE..PAGE_SIZE);
    for offset in offsets {
        let mut tampered = encrypted;
        tampered[offset] ^= 0x80;
        rejected(&dek, &mac_key, epoch, page_id, &tampered, &state);
    }
    for (key, check_epoch, check_id) in [
        ([0x19; MAC_KEY_SIZE], epoch, page_id),
        (mac_key, epoch + 1, page_id),
        (mac_key, epoch, PageId(10)),
    ] {
        let wrong_state = HmacState::new(&key, check_epoch);
        rejected(&dek, &key, check_epoch, check_id, &encrypted, &wrong_state);
    }
    // A rejected page must not advance the reusable state.
    let mut recovered = [0; BODY_SIZE];
    decrypt_page_with_hmac(&dek, &state, page_id, &encrypted, &mut recovered).unwrap();
    assert_eq!(recovered, body);
}

#[test]
fn cached_hmac_decrypt_keeps_dek_separate_from_authentication() {
    let (dek, mac_key) = test_keys();
    let epoch = 4;
    let page_id = PageId(3);
    let body = [0x42; BODY_SIZE];
    let mut encrypted = [0; PAGE_SIZE];
    encrypt_page_with_iv(
        &dek,
        &mac_key,
        page_id,
        epoch,
        &body,
        &[0x31; IV_SIZE],
        &mut encrypted,
    );
    let wrong_dek = [0x21; DEK_SIZE];
    let mut raw = [0; BODY_SIZE];
    let mut cached = [0; BODY_SIZE];
    decrypt_page(&wrong_dek, &mac_key, page_id, epoch, &encrypted, &mut raw).unwrap();
    decrypt_page_with_hmac(
        &wrong_dek,
        &HmacState::new(&mac_key, epoch),
        page_id,
        &encrypted,
        &mut cached,
    )
    .unwrap();
    assert_eq!(raw, cached);
    assert_ne!(cached, body);
}
