use super::*;

#[test]
fn page_id_display() {
    assert_eq!(format!("{}", PageId(42)), "page:42");
}

#[test]
fn page_id_invalid() {
    assert!(!PageId::INVALID.is_valid());
    assert!(PageId(0).is_valid());
}

#[test]
fn txn_id_next() {
    assert_eq!(TxnId(5).next(), TxnId(6));
}

#[test]
fn page_type_roundtrip() {
    assert_eq!(PageType::from_u16(1), Some(PageType::Branch));
    assert_eq!(PageType::from_u16(2), Some(PageType::Leaf));
    assert_eq!(PageType::from_u16(3), Some(PageType::Overflow));
    assert_eq!(PageType::from_u16(4), Some(PageType::PendingFree));
    assert_eq!(PageType::from_u16(0), None);
    assert_eq!(PageType::from_u16(5), None);
}

#[test]
fn page_flags() {
    let mut f = PageFlags::NONE;
    assert!(!f.contains(PageFlags::IS_ROOT));
    f.set(PageFlags::IS_ROOT);
    assert!(f.contains(PageFlags::IS_ROOT));
    f.clear(PageFlags::IS_ROOT);
    assert!(!f.contains(PageFlags::IS_ROOT));
}

#[test]
fn cipher_id_roundtrip() {
    assert_eq!(CipherId::from_u8(0), Some(CipherId::Aes256Ctr));
    assert_eq!(CipherId::from_u8(1), None);
}

#[test]
fn kdf_algorithm_roundtrip() {
    assert_eq!(KdfAlgorithm::from_u8(0), Some(KdfAlgorithm::Argon2id));
    assert_eq!(
        KdfAlgorithm::from_u8(1),
        Some(KdfAlgorithm::Pbkdf2HmacSha256)
    );
    assert_eq!(KdfAlgorithm::from_u8(2), None);
}

#[test]
fn cipher_id_labels_every_variant() {
    for v in 0..=u8::MAX {
        let Some(c) = CipherId::from_u8(v) else {
            continue;
        };
        assert!(!c.as_str().is_empty(), "{c:?} has no label");
    }
    assert_eq!(CipherId::Aes256Ctr.as_str(), "AES-256-CTR");
}

#[test]
fn kdf_algorithm_labels_every_variant() {
    for v in 0..=u8::MAX {
        let Some(k) = KdfAlgorithm::from_u8(v) else {
            continue;
        };
        assert!(!k.as_str().is_empty(), "{k:?} has no label");
    }
    assert_eq!(KdfAlgorithm::Argon2id.as_str(), "Argon2id");
    assert_eq!(
        KdfAlgorithm::Pbkdf2HmacSha256.as_str(),
        "PBKDF2-HMAC-SHA256"
    );
}

#[test]
fn argon2_profiles() {
    assert_eq!(Argon2Profile::Iot.m_cost(), 19 * 1024);
    assert_eq!(Argon2Profile::Desktop.m_cost(), 64 * 1024);
    assert_eq!(Argon2Profile::Server.m_cost(), 128 * 1024);
    assert_eq!(Argon2Profile::Iot.p_cost(), 1);
    assert_eq!(Argon2Profile::Desktop.p_cost(), 4);
}
