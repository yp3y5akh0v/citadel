use citadel::{Argon2Profile, DatabaseBuilder};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"original-passphrase")
        .argon2_profile(Argon2Profile::Iot)
}

#[test]
fn change_passphrase_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    // Create and populate
    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"key", b"value").unwrap();
        wtx.commit().unwrap();

        // Change passphrase while DB is open
        db.change_passphrase(b"original-passphrase", b"new-passphrase")
            .unwrap();
    }

    // Old passphrase should fail
    let result = DatabaseBuilder::new(&db_path)
        .passphrase(b"original-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open();
    assert!(result.is_err());

    // New passphrase should work and data is intact
    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"new-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn change_passphrase_wrong_old() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();
    let result = db.change_passphrase(b"wrong-old", b"new-passphrase");
    assert!(result.is_err());
}

#[test]
fn change_passphrase_preserves_named_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"default_key", b"default_val").unwrap();
        wtx.create_table(b"users").unwrap();
        wtx.table_insert(b"users", b"alice", b"admin").unwrap();
        wtx.commit().unwrap();

        db.change_passphrase(b"original-passphrase", b"rotated")
            .unwrap();
    }

    // Open with new passphrase, verify all data
    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"rotated")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let mut rtx = db.begin_read();
    assert_eq!(
        rtx.get(b"default_key").unwrap(),
        Some(b"default_val".to_vec())
    );
    assert_eq!(
        rtx.table_get(b"users", b"alice").unwrap(),
        Some(b"admin".to_vec())
    );
}

#[test]
fn change_passphrase_multiple_rotations() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"persistent", b"data").unwrap();
        wtx.commit().unwrap();

        // Rotate multiple times
        db.change_passphrase(b"original-passphrase", b"pass2")
            .unwrap();
        db.change_passphrase(b"pass2", b"pass3").unwrap();
        db.change_passphrase(b"pass3", b"final-pass").unwrap();
    }

    // Only the final passphrase should work
    assert!(DatabaseBuilder::new(&db_path)
        .passphrase(b"original-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .is_err());
    assert!(DatabaseBuilder::new(&db_path)
        .passphrase(b"pass2")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .is_err());

    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"final-pass")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let mut rtx = db.begin_read();
    assert_eq!(
        rtx.get(b"persistent").unwrap(),
        Some(b"data".to_vec())
    );
}

#[test]
fn change_passphrase_large_dataset() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let count = 2000u32;

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        for i in 0..count {
            let key = format!("k{i:06}");
            let val = format!("v{i:06}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();

        db.change_passphrase(b"original-passphrase", b"new-pw")
            .unwrap();
    }

    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"new-pw")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    assert_eq!(db.stats().entry_count, count as u64);

    let mut rtx = db.begin_read();
    for i in 0..count {
        let key = format!("k{i:06}");
        let val = format!("v{i:06}");
        assert_eq!(
            rtx.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "mismatch at {key}"
        );
    }
}
