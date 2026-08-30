use std::path::Path;

use citadel::core::KEY_FILE_SIZE;
use citadel::crypto::hkdf_utils::derive_keyfile_mac_key;
use citadel::crypto::kdf::derive_mk;
use citadel::crypto::key_manager::KeyFile;
use citadel::{default_key_path, inspect_vault, DatabaseBuilder};
use hmac::{Hmac, Mac};
use sha2::Sha256;

const PASSPHRASE: &[u8] = b"original passphrase";
const NEW_PASSPHRASE: &[u8] = b"replacement passphrase";
const RELEASED_CHACHA20_MARKER: u8 = 1;

fn builder(path: &Path, passphrase: &[u8]) -> DatabaseBuilder {
    let builder = DatabaseBuilder::new(path)
        .passphrase(passphrase)
        .cache_size(64);
    #[cfg(not(feature = "fips"))]
    {
        builder.argon2_profile(citadel::Argon2Profile::Iot)
    }
    #[cfg(feature = "fips")]
    {
        builder
            .kdf_algorithm(citadel::KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000)
    }
}

fn rewrite_as_released_id_one(key_path: &Path, passphrase: &[u8]) {
    let bytes = std::fs::read(key_path).unwrap();
    let mut image: [u8; KEY_FILE_SIZE] = bytes.try_into().unwrap();
    let key_file = KeyFile::deserialize(&image).unwrap();
    let master_key = derive_mk(
        key_file.kdf_algorithm,
        passphrase,
        &key_file.argon2_salt,
        key_file.argon2_m_cost,
        key_file.argon2_t_cost,
        key_file.argon2_p_cost,
    )
    .unwrap();
    let key_file_mac_key = derive_keyfile_mac_key(&master_key);
    image[44] = RELEASED_CHACHA20_MARKER;
    image[46..48].copy_from_slice(&0u16.to_le_bytes());
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_file_mac_key).unwrap();
    mac.update(&image[..140]);
    image[140..172].copy_from_slice(&mac.finalize().into_bytes());
    std::fs::write(key_path, image).unwrap();
}

#[test]
fn a_released_format_id_one_vault_opens_as_aes_and_canonicalizes_on_authenticated_repair() {
    let dir = tempfile::tempdir().unwrap();
    let data_path = dir.path().join("released-id-one.citadel");
    let key_path = default_key_path(&data_path);

    let db = builder(&data_path, PASSPHRASE).create().unwrap();
    let mut write = db.begin_write().unwrap();
    write.insert(b"before", b"value-1").unwrap();
    write.commit().unwrap();
    drop(db);

    rewrite_as_released_id_one(&key_path, PASSPHRASE);
    let inspected = inspect_vault(&data_path).unwrap();
    let key_info = inspected.key_file.info().unwrap();
    assert!(key_info.legacy_cipher_encoding);
    assert_eq!(key_info.cipher.as_str(), "AES-256-CTR");

    let db = builder(&data_path, PASSPHRASE).open().unwrap();
    assert!(!db.key_file().legacy_cipher_encoding);
    assert_eq!(std::fs::read(&key_path).unwrap()[44], 0);
    assert_eq!(
        db.begin_read().get(b"before").unwrap(),
        Some(b"value-1".to_vec())
    );
    let mut write = db.begin_write().unwrap();
    write.insert(b"after", b"value-2").unwrap();
    write.commit().unwrap();
    db.change_passphrase(PASSPHRASE, NEW_PASSPHRASE).unwrap();
    drop(db);

    let reopened = builder(&data_path, NEW_PASSPHRASE).open().unwrap();
    assert!(!reopened.key_file().legacy_cipher_encoding);
    let mut read = reopened.begin_read();
    assert_eq!(read.get(b"before").unwrap(), Some(b"value-1".to_vec()));
    assert_eq!(read.get(b"after").unwrap(), Some(b"value-2".to_vec()));
}
