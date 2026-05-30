use super::*;

#[test]
fn known_length_and_determinism() {
    let key = [7u8; 32];
    let a = hmac_sha256(&key, b"hello");
    let b = hmac_sha256(&key, b"hello");
    assert_eq!(a, b);
    assert_eq!(a.len(), MAC_SIZE);
}

#[test]
fn verify_accepts_matching_tag() {
    let key = [3u8; 32];
    let tag = hmac_sha256(&key, b"payload");
    assert!(verify_hmac_sha256(&key, b"payload", &tag));
}

#[test]
fn verify_rejects_wrong_data() {
    let key = [3u8; 32];
    let tag = hmac_sha256(&key, b"payload");
    assert!(!verify_hmac_sha256(&key, b"payloaX", &tag));
}

#[test]
fn verify_rejects_wrong_key() {
    let tag = hmac_sha256(&[1u8; 32], b"payload");
    assert!(!verify_hmac_sha256(&[2u8; 32], b"payload", &tag));
}
