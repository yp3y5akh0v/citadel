use super::{open, seal};
use crate::hkdf_utils::{derive_seal_keys, SealKeys};

fn keys(seed: u8) -> SealKeys {
    derive_seal_keys(&[seed; 32])
}

#[test]
fn round_trip() {
    let k = keys(7);
    let pt = b"the quick brown fox";
    let blob = seal(&k, 42, pt);
    assert_eq!(open(&k, 42, &blob).unwrap(), pt);
}

#[test]
fn empty_plaintext_round_trips() {
    let k = keys(1);
    let blob = seal(&k, 0, b"");
    assert_eq!(open(&k, 0, &blob).unwrap(), b"");
}

#[test]
fn tamper_rejected_before_decrypt() {
    let k = keys(3);
    let mut blob = seal(&k, 5, b"secret payload");
    let i = blob.len() / 2;
    blob[i] ^= 0x01;
    assert!(open(&k, 5, &blob).is_err());
}

#[test]
fn wrong_aad_rejected() {
    let k = keys(9);
    let blob = seal(&k, 100, b"row content");
    assert!(open(&k, 101, &blob).is_err());
}

#[test]
fn wrong_key_rejected() {
    let blob = seal(&keys(2), 1, b"data");
    assert!(open(&keys(4), 1, &blob).is_err());
}

#[test]
fn truncated_blob_rejected() {
    let k = keys(6);
    let blob = seal(&k, 1, b"data");
    assert!(open(&k, 1, &blob[..8]).is_err());
}

#[test]
fn cross_region_key_cannot_open_and_ciphertext_differs() {
    // Two regions = two RCKs = two seal-key sets. The same plaintext+aad sealed under
    // each produces different ciphertext, and neither key opens the other's blob.
    let (k1, k2) = (keys(1), keys(2));
    let pt = b"region-private content";
    let b1 = seal(&k1, 7, pt);
    let b2 = seal(&k2, 7, pt);
    // Compare past the random IV (first 16 bytes): the ciphertext bodies differ.
    assert_ne!(
        &b1[16..],
        &b2[16..],
        "different region keys -> different ciphertext"
    );
    assert!(
        open(&k2, 7, &b1).is_err(),
        "region 2 key cannot open region 1 blob"
    );
    assert!(
        open(&k1, 7, &b2).is_err(),
        "region 1 key cannot open region 2 blob"
    );
}

#[test]
fn cross_atom_replay_swap_rejected() {
    // A full two-blob swap (not a one-bit aad flip): each opens under its own atom id,
    // but neither opens under the other's id, so a blob cannot be replayed into a
    // different row of the same region.
    let k = keys(5);
    let blob10 = seal(&k, 10, b"atom ten content");
    let blob20 = seal(&k, 20, b"atom twenty content");
    assert_eq!(open(&k, 10, &blob10).unwrap(), b"atom ten content");
    assert_eq!(open(&k, 20, &blob20).unwrap(), b"atom twenty content");
    assert!(
        open(&k, 20, &blob10).is_err(),
        "blob10 must not open as atom 20"
    );
    assert!(
        open(&k, 10, &blob20).is_err(),
        "blob20 must not open as atom 10"
    );
}

// Deliberately independent of the production alias and its sealing helper.
type PriorFullAes256Ctr = ctr::Ctr128BE<aes::Aes256>;

fn prior_full_aes_blob(
    keys: &SealKeys,
    aad: u64,
    plaintext: &[u8],
    iv: &[u8; citadel_core::IV_SIZE],
) -> Vec<u8> {
    use cipher::{KeyIvInit, StreamCipher};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let mut ciphertext = plaintext.to_vec();
    PriorFullAes256Ctr::new((&keys.dek).into(), iv.into()).apply_keystream(&mut ciphertext);
    let mut mac = Hmac::<Sha256>::new_from_slice(&keys.mac_key).unwrap();
    mac.update(&aad.to_le_bytes());
    mac.update(iv);
    mac.update(&ciphertext);
    let mut blob = Vec::new();
    blob.extend_from_slice(iv);
    blob.extend_from_slice(&ciphertext);
    blob.extend_from_slice(&mac.finalize().into_bytes());
    blob
}

#[test]
fn blob_ctr_bytes_and_cross_open_match_prior_full_aes() {
    use cipher::{KeyIvInit, StreamCipher};
    use citadel_core::{IV_SIZE, MAC_SIZE};

    let mut carry_iv = [0x63; IV_SIZE];
    carry_iv[IV_SIZE - 2..].copy_from_slice(&[0xff, 0xfe]);
    for (keys, aad) in [
        (
            SealKeys {
                dek: [0; 32],
                mac_key: [0; 32],
            },
            0,
        ),
        (
            SealKeys {
                dek: std::array::from_fn(|i| (i as u8).wrapping_add(0x80)),
                mac_key: std::array::from_fn(|i| (i as u8).wrapping_mul(11)),
            },
            u64::MAX,
        ),
    ] {
        for len in [0, 1, 15, 16, 17, 31, 32, 33, 255, 4097] {
            let plaintext: Vec<u8> = (0..len)
                .map(|i| (i as u8).wrapping_mul(29).wrapping_add(3))
                .collect();
            for iv in [[0; IV_SIZE], carry_iv] {
                let prior = prior_full_aes_blob(&keys, aad, &plaintext, &iv);
                assert_eq!(open(&keys, aad, &prior).unwrap(), plaintext);
            }

            let current = seal(&keys, aad, &plaintext);
            let iv: &[u8; IV_SIZE] = current[..IV_SIZE].try_into().unwrap();
            assert_eq!(
                current,
                prior_full_aes_blob(&keys, aad, &plaintext, iv),
                "complete blob envelope changed at length {len}"
            );
            let mut old_reader = current[IV_SIZE..current.len() - MAC_SIZE].to_vec();
            PriorFullAes256Ctr::new((&keys.dek).into(), iv.into()).apply_keystream(&mut old_reader);
            assert_eq!(old_reader, plaintext);
            assert_eq!(open(&keys, aad, &current).unwrap(), plaintext);
        }
    }
}

#[test]
fn prior_full_aes_blobs_reject_tampering_and_wrong_context() {
    use citadel_core::{Error, IV_SIZE, MAC_KEY_SIZE, MAC_SIZE};

    let keys = keys(0x91);
    let aad = 0x1020_3040_5060_7080;
    for plaintext in [b"".as_slice(), b"seventeen bytes!!".as_slice()] {
        let blob = prior_full_aes_blob(&keys, aad, plaintext, &[0x42; IV_SIZE]);
        let mut offsets = vec![0, IV_SIZE - 1, blob.len() - MAC_SIZE, blob.len() - 1];
        if !plaintext.is_empty() {
            offsets.extend([IV_SIZE, blob.len() - MAC_SIZE - 1]);
        }
        for offset in offsets {
            let mut tampered = blob.clone();
            tampered[offset] ^= 0x80;
            assert!(matches!(
                open(&keys, aad, &tampered),
                Err(Error::RegionSealTampered)
            ));
        }
        let wrong_mac = SealKeys {
            dek: keys.dek,
            mac_key: [0x19; MAC_KEY_SIZE],
        };
        assert!(matches!(
            open(&wrong_mac, aad, &blob),
            Err(Error::RegionSealTampered)
        ));
        assert!(matches!(
            open(&keys, aad + 1, &blob),
            Err(Error::RegionSealTampered)
        ));
        for len in [
            0,
            IV_SIZE - 1,
            IV_SIZE,
            IV_SIZE + MAC_SIZE - 1,
            blob.len() - 1,
        ] {
            assert!(matches!(
                open(&keys, aad, &blob[..len]),
                Err(Error::RegionSealTampered)
            ));
        }
        assert_eq!(open(&keys, aad, &blob).unwrap(), plaintext);
    }
}
