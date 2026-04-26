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
