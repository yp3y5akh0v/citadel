use super::*;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

const KAT_KEY: [u8; 32] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
];

#[test]
fn rek_derived_keys_are_frozen() {
    // These keys protect durable database content. Changing an HKDF label,
    // salt, or construction must not silently make existing data unreadable.
    let keys = derive_keys_from_rek(&KAT_KEY);
    assert_eq!(
        hex(&keys.dek),
        "ca468e6ab8e6b67998e0d2594211b5fbd6f722de7b47de3af62d776eaa70440d"
    );
    assert_eq!(
        hex(&keys.mac_key),
        "4cae84724241dc35d4cf8314236047a67fed395c9c9a54888cb5fac087b47351"
    );
    assert_eq!(
        hex(&keys.audit_key),
        "188ae3e3bdc8cc92fa45be3ce9c3b9e0892c1227e2c952ce6788c316f73caa1c"
    );
}

#[test]
fn keyfile_mac_key_derivation_is_frozen() {
    let key = derive_keyfile_mac_key(&KAT_KEY);
    assert_eq!(
        hex(&key),
        "82a78970f8ec6eae10a88c18138bd41be3b2fe8536276622fa32bdeed7f72878"
    );
}

#[test]
fn kms_master_key_derivation_is_frozen() {
    // Pins both the non-zero KMS salt and the master-key info string.
    let key = derive_mk_from_kms(b"some-kms-provided-material");
    assert_eq!(
        hex(&key),
        "983864e2047e4432617f41fc8dec72cef4115af726559c50a1b0c4ffd21140e8"
    );
}

#[test]
fn region_key_derivations_are_frozen() {
    let keys = derive_region_wrap_keys(&KAT_KEY);
    assert_eq!(
        hex(&keys.kek),
        "06aaa9f6f9b429a8c20d91690b0a7a5aec88966437d1bb116c227118a4ceb8d8"
    );
    assert_eq!(
        hex(&keys.store_mac_key),
        "2dfa5e67956e4da77a01def01b7e2378bac547e5b36ff52c7082d2c586bab627"
    );
}

#[test]
fn seal_key_derivations_are_frozen() {
    let keys = derive_seal_keys(&KAT_KEY);
    assert_eq!(
        hex(&keys.dek),
        "72f7a9f1d1ebe93bf570ed4a8667723253f4394c624e5a9182849781154c38f7"
    );
    assert_eq!(
        hex(&keys.mac_key),
        "dd1ec58bdea35d0541880ab798d8fc327068ec4e5d8e3e0f46da4b7220bcca2f"
    );
}

#[test]
fn atom_wrap_key_derivation_is_frozen() {
    let key = derive_atom_wrap_key(&KAT_KEY);
    assert_eq!(
        hex(&key.kek),
        "5af4b1e5bf1db7ae2fb054f822350d3f137ea963b7cdb5f8b2ab58a5e4e391a4"
    );
}

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
fn identity_mac_key_derivation_is_frozen() {
    // Re-pinning orphans existing records; version the label instead.
    let key = derive_identity_mac_key(&KAT_KEY);
    assert_eq!(
        hex(&key.key),
        "cefb46aaf81b4346d28fb6be43d66b9fd3190f8c19f661d2102a00183f0b7591"
    );
}

#[test]
fn identity_mac_key_deterministic_and_domain_separated() {
    let rck = [0x5Au8; 32];
    let a = derive_identity_mac_key(&rck);
    let b = derive_identity_mac_key(&rck);
    assert_eq!(a.key, b.key);
    // Identity tags must never be computable from (or leak) the atom-wrap KEK.
    let wrap = derive_atom_wrap_key(&rck);
    assert_ne!(a.key, wrap.kek, "identity MAC key reuses the atom-wrap KEK");
    let other = derive_identity_mac_key(&[0x5Bu8; 32]);
    assert_ne!(a.key, other.key);
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
