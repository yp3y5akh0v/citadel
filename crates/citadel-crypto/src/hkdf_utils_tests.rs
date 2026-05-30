use super::*;

#[test]
fn derive_keys_deterministic() {
    let rek = [0x42u8; 32];
    let keys1 = derive_keys_from_rek(&rek);
    let keys2 = derive_keys_from_rek(&rek);
    assert_eq!(keys1.dek, keys2.dek);
    assert_eq!(keys1.mac_key, keys2.mac_key);
}

#[test]
fn dek_and_mac_key_differ() {
    let rek = [0x42u8; 32];
    let keys = derive_keys_from_rek(&rek);
    assert_ne!(keys.dek, keys.mac_key, "DEK and MAC_KEY must be different");
}

#[test]
fn different_rek_different_keys() {
    let rek1 = [0x01u8; 32];
    let rek2 = [0x02u8; 32];
    let keys1 = derive_keys_from_rek(&rek1);
    let keys2 = derive_keys_from_rek(&rek2);
    assert_ne!(keys1.dek, keys2.dek);
    assert_ne!(keys1.mac_key, keys2.mac_key);
}

#[test]
fn keyfile_mac_key_deterministic() {
    let mk = [0xABu8; 32];
    let k1 = derive_keyfile_mac_key(&mk);
    let k2 = derive_keyfile_mac_key(&mk);
    assert_eq!(k1, k2);
}

#[test]
fn keyfile_mac_key_differs_from_dek() {
    let key = [0xABu8; 32];
    let mac_key = derive_keyfile_mac_key(&key);
    let keys = derive_keys_from_rek(&key);
    assert_ne!(mac_key, keys.dek);
    assert_ne!(mac_key, keys.mac_key);
}

#[test]
fn kms_derivation() {
    let kms_bytes = b"some-kms-provided-material";
    let mk1 = derive_mk_from_kms(kms_bytes);
    let mk2 = derive_mk_from_kms(kms_bytes);
    assert_eq!(mk1, mk2);

    let mk3 = derive_mk_from_kms(b"different-material");
    assert_ne!(mk1, mk3);
}

// --- Per-region cryptographic erasure derivations ---

#[test]
fn region_wrap_keys_deterministic() {
    let rek = [0x42u8; 32];
    let a = derive_region_wrap_keys(&rek);
    let b = derive_region_wrap_keys(&rek);
    assert_eq!(a.kek, b.kek);
    assert_eq!(a.store_mac_key, b.store_mac_key);
}

#[test]
fn region_derivations_are_domain_separated() {
    let rek = [0x42u8; 32];
    let region = derive_region_wrap_keys(&rek);
    let data = derive_keys_from_rek(&rek);
    // The region KEK and store-MAC key are distinct from each other and from the
    // data DEK/MAC, so no key does double duty.
    assert_ne!(region.kek, region.store_mac_key);
    assert_ne!(region.kek, data.dek);
    assert_ne!(region.kek, data.mac_key);
    assert_ne!(region.store_mac_key, data.mac_key);
    // The standalone store-MAC derivation matches the bundle's.
    assert_eq!(region.store_mac_key, derive_region_store_mac_key(&rek));
}

#[test]
fn seal_keys_deterministic_domain_separated_and_random_ikm_diverges() {
    let rck = [0x11u8; 32];
    let a = derive_seal_keys(&rck);
    let b = derive_seal_keys(&rck);
    assert_eq!(a.dek, b.dek);
    assert_eq!(a.mac_key, b.mac_key);
    assert_ne!(a.dek, a.mac_key, "seal dek and mac_key are separated");
    // A different (random) RCK yields non-recomputable seal keys - the linchpin of
    // erasure: destroying the wrapped RCK makes these keys unrecoverable.
    let other = derive_seal_keys(&[0x22u8; 32]);
    assert_ne!(a.dek, other.dek);
    assert_ne!(a.mac_key, other.mac_key);
}

#[test]
fn region_key_wrap_unwrap_roundtrip_and_wrong_kek_rejected() {
    let rek = [0x42u8; 32];
    let region = derive_region_wrap_keys(&rek);
    let rck = [0x99u8; 32];

    let wrapped = region.wrap_region_key(&rck);
    assert_eq!(region.unwrap_region_key(&wrapped).unwrap(), rck);

    // A different REK's region keys cannot unwrap it (AES-KW integrity).
    let other = derive_region_wrap_keys(&[0x43u8; 32]);
    assert!(other.unwrap_region_key(&wrapped).is_err());
}
