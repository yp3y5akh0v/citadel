//! Tests for FIPS 140-3 mode and PBKDF2 KDF algorithm.

#[cfg(not(feature = "fips"))]
use citadel::Argon2Profile;
use citadel::{DatabaseBuilder, KdfAlgorithm};

/// PBKDF2 databases should create/open/reopen correctly.
#[test]
fn pbkdf2_create_open_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("pbkdf2.citadel");
    let passphrase = b"pbkdf2-test-passphrase";

    // Create with PBKDF2
    {
        let db = DatabaseBuilder::new(&db_path)
            .passphrase(passphrase)
            .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000)
            .cache_size(64)
            .create()
            .unwrap();

        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"key1", b"value1").unwrap();
        wtx.commit().unwrap();

        let stats = db.stats();
        assert_eq!(stats.entry_count, 1);
    }

    // Reopen with PBKDF2
    {
        let db = DatabaseBuilder::new(&db_path)
            .passphrase(passphrase)
            .open()
            .unwrap();

        let mut rtx = db.begin_read();
        let val = rtx.get(b"key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    // Wrong passphrase should fail
    {
        let result = DatabaseBuilder::new(&db_path).passphrase(b"wrong").open();
        assert!(result.is_err());
    }
}

/// PBKDF2 databases should work with named tables.
#[test]
fn pbkdf2_named_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("pbkdf2_tables.citadel");

    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"test")
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000)
        .cache_size(64)
        .create()
        .unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"users").unwrap();
    wtx.table_insert(b"users", b"alice", b"admin").unwrap();
    wtx.create_table(b"logs").unwrap();
    wtx.table_insert(b"logs", b"entry1", b"logged in").unwrap();
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(
        rtx.table_get(b"users", b"alice").unwrap(),
        Some(b"admin".to_vec())
    );
    assert_eq!(
        rtx.table_get(b"logs", b"entry1").unwrap(),
        Some(b"logged in".to_vec())
    );
}

/// PBKDF2 in-memory databases should work.
#[test]
fn pbkdf2_in_memory() {
    let db = DatabaseBuilder::new("unused_path")
        .passphrase(b"inmem-test")
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000)
        .cache_size(64)
        .create_in_memory()
        .unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..100u32 {
        wtx.insert(&i.to_be_bytes(), &(i * 2).to_be_bytes())
            .unwrap();
    }
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    for i in 0..100u32 {
        let val = rtx.get(&i.to_be_bytes()).unwrap().unwrap();
        assert_eq!(val, (i * 2).to_be_bytes());
    }
    assert_eq!(db.stats().entry_count, 100);
}

/// Change passphrase should work on a PBKDF2 database.
#[test]
fn pbkdf2_change_passphrase() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("pbkdf2_rekey.citadel");
    let old_pass = b"old-password";
    let new_pass = b"new-password";

    {
        let db = DatabaseBuilder::new(&db_path)
            .passphrase(old_pass)
            .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000)
            .cache_size(64)
            .create()
            .unwrap();

        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"secret", b"data").unwrap();
        wtx.commit().unwrap();

        db.change_passphrase(old_pass, new_pass).unwrap();
    }

    // Old passphrase should fail
    {
        let result = DatabaseBuilder::new(&db_path).passphrase(old_pass).open();
        assert!(result.is_err());
    }

    // New passphrase should work and data should be intact
    {
        let db = DatabaseBuilder::new(&db_path)
            .passphrase(new_pass)
            .open()
            .unwrap();

        let mut rtx = db.begin_read();
        assert_eq!(rtx.get(b"secret").unwrap(), Some(b"data".to_vec()));
    }
}

/// Argon2id is the default KDF (skipped in FIPS mode which rejects Argon2id).
#[test]
#[cfg(not(feature = "fips"))]
fn default_kdf_is_argon2id() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("default_kdf.citadel");

    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"test")
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(64)
        .create()
        .unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"val").unwrap();
    wtx.commit().unwrap();
    drop(db);

    // Reopen without specifying KDF (defaults to Argon2id)
    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"test")
        .open()
        .unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"val".to_vec()));
}

/// PBKDF2 and Argon2id produce different keys from the same passphrase.
/// (Skipped in FIPS mode which rejects Argon2id.)
#[test]
#[cfg(not(feature = "fips"))]
fn pbkdf2_and_argon2_produce_different_databases() {
    let dir = tempfile::tempdir().unwrap();
    let passphrase = b"same-passphrase";

    let db_argon2 = DatabaseBuilder::new(dir.path().join("argon2.citadel"))
        .passphrase(passphrase)
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(64)
        .create()
        .unwrap();

    let db_pbkdf2 = DatabaseBuilder::new(dir.path().join("pbkdf2.citadel"))
        .passphrase(passphrase)
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000)
        .cache_size(64)
        .create()
        .unwrap();

    // Write same data to both
    {
        let mut wtx = db_argon2.begin_write().unwrap();
        wtx.insert(b"key", b"value").unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = db_pbkdf2.begin_write().unwrap();
        wtx.insert(b"key", b"value").unwrap();
        wtx.commit().unwrap();
    }

    // Drop databases to release file locks before reading raw files
    drop(db_argon2);
    drop(db_pbkdf2);

    // Read the raw files - encrypted content should differ
    let raw_argon2 = std::fs::read(dir.path().join("argon2.citadel")).unwrap();
    let raw_pbkdf2 = std::fs::read(dir.path().join("pbkdf2.citadel")).unwrap();

    // Skip the 512-byte header, compare encrypted page data
    if raw_argon2.len() > 512 && raw_pbkdf2.len() > 512 {
        assert_ne!(
            &raw_argon2[512..],
            &raw_pbkdf2[512..],
            "different KDFs should produce different encryption keys and ciphertext"
        );
    }
}

/// Backup and integrity check should work with PBKDF2 databases.
#[test]
fn pbkdf2_backup_and_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("pbkdf2_backup.citadel");
    let backup_path = dir.path().join("pbkdf2_backup_copy.citadel");

    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"test")
        .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
        .pbkdf2_iterations(600_000)
        .cache_size(64)
        .create()
        .unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..50u32 {
        wtx.insert(&i.to_be_bytes(), &format!("val-{i}").into_bytes())
            .unwrap();
    }
    wtx.commit().unwrap();

    // Integrity check
    let report = db.integrity_check().unwrap();
    assert!(report.errors.is_empty());

    // Backup
    db.backup(&backup_path).unwrap();

    // Open backup
    let backup_db = DatabaseBuilder::new(&backup_path)
        .passphrase(b"test")
        .open()
        .unwrap();

    let mut rtx = backup_db.begin_read();
    for i in 0..50u32 {
        let val = rtx.get(&i.to_be_bytes()).unwrap().unwrap();
        assert_eq!(val, format!("val-{i}").into_bytes());
    }
}

// === FIPS mode tests (only compiled when fips feature is enabled) ===

#[cfg(feature = "fips")]
mod fips_tests {
    use citadel::core::types::CipherId;
    use citadel::{DatabaseBuilder, KdfAlgorithm};

    #[test]
    fn fips_rejects_argon2id() {
        let dir = tempfile::tempdir().unwrap();
        let result = DatabaseBuilder::new(dir.path().join("fips.citadel"))
            .passphrase(b"test")
            .kdf_algorithm(KdfAlgorithm::Argon2id)
            .cache_size(64)
            .create();
        let err = result.unwrap_err().to_string();
        assert!(err.contains("FIPS"), "error should mention FIPS: {err}");
    }

    #[test]
    fn fips_rejects_default_kdf() {
        let dir = tempfile::tempdir().unwrap();
        // Default is Argon2id, which FIPS rejects
        let result = DatabaseBuilder::new(dir.path().join("fips_default.citadel"))
            .passphrase(b"test")
            .cache_size(64)
            .create();
        assert!(result.is_err());
    }

    #[test]
    fn fips_rejects_chacha20() {
        let dir = tempfile::tempdir().unwrap();
        let result = DatabaseBuilder::new(dir.path().join("fips_chacha.citadel"))
            .passphrase(b"test")
            .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000)
            .cipher(CipherId::ChaCha20)
            .cache_size(64)
            .create();
        let err = result.unwrap_err().to_string();
        assert!(err.contains("FIPS"), "error should mention FIPS: {err}");
    }

    #[test]
    fn fips_accepts_pbkdf2_aes() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("fips_ok.citadel"))
            .passphrase(b"test")
            .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000)
            .cache_size(64)
            .create()
            .unwrap();

        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"key", b"value").unwrap();
        wtx.commit().unwrap();

        let mut rtx = db.begin_read();
        assert_eq!(rtx.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn fips_in_memory_accepts_pbkdf2() {
        let db = DatabaseBuilder::new("unused")
            .passphrase(b"test")
            .kdf_algorithm(KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000)
            .cache_size(64)
            .create_in_memory()
            .unwrap();

        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"k", b"v").unwrap();
        wtx.commit().unwrap();
    }

    #[test]
    fn fips_in_memory_rejects_argon2id() {
        let result = DatabaseBuilder::new("unused")
            .passphrase(b"test")
            .cache_size(64)
            .create_in_memory();
        assert!(result.is_err());
    }
}
