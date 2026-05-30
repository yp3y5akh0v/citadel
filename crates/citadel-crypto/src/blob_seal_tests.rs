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
