use super::*;

#[test]
fn generate_unique() {
    let a = SyncKey::generate();
    let b = SyncKey::generate();
    assert_ne!(a.0, b.0);
}

#[test]
fn base64_roundtrip() {
    let key = SyncKey::generate();
    let encoded = key.to_base64();
    let decoded = SyncKey::from_base64(&encoded).unwrap();
    assert_eq!(key.0, decoded.0);
}

#[test]
fn from_bytes_roundtrip() {
    let raw = [0xABu8; KEY_SIZE];
    let key = SyncKey::from_bytes(raw);
    assert_eq!(*key.as_bytes(), raw);
}

#[test]
fn invalid_base64_rejected() {
    assert!(SyncKey::from_base64("not-valid-base64!!!").is_err());
}

#[test]
fn wrong_length_rejected() {
    let short = base64::engine::general_purpose::STANDARD.encode([0u8; 16]);
    assert!(SyncKey::from_base64(&short).is_err());
}

#[test]
fn debug_redacts() {
    let key = SyncKey::generate();
    let debug = format!("{:?}", key);
    assert_eq!(debug, "SyncKey([REDACTED])");
    assert!(!debug.contains(&key.to_base64()));
}

#[test]
fn display_is_base64() {
    let key = SyncKey::generate();
    assert_eq!(format!("{}", key), key.to_base64());
}

#[test]
fn base64_length_is_44() {
    let key = SyncKey::generate();
    assert_eq!(key.to_base64().len(), 44);
}
