use citadel::{Argon2Profile, DatabaseBuilder};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
}

#[test]
fn backup_empty_db() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("backup.db");

    let db = fast_builder(&db_path).create().unwrap();
    db.backup(&backup_path).unwrap();

    // Open the backup independently
    let backup = DatabaseBuilder::new(&backup_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    assert_eq!(backup.stats().entry_count, 0);
}

#[test]
fn backup_with_data() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("backup.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..1000u32 {
            let key = format!("k{i:05}");
            let val = format!("v{i:05}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    db.backup(&backup_path).unwrap();

    // Verify backup has all data
    let backup = DatabaseBuilder::new(&backup_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    assert_eq!(backup.stats().entry_count, 1000);

    let mut rtx = backup.begin_read();
    for i in 0..1000u32 {
        let key = format!("k{i:05}");
        let val = format!("v{i:05}");
        assert_eq!(
            rtx.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "mismatch at key {key}"
        );
    }
}

#[test]
fn backup_with_named_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("backup.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"default", b"value").unwrap();
        wtx.create_table(b"users").unwrap();
        wtx.table_insert(b"users", b"alice", b"admin").unwrap();
        wtx.table_insert(b"users", b"bob", b"user").unwrap();
        wtx.commit().unwrap();
    }

    db.backup(&backup_path).unwrap();

    let backup = DatabaseBuilder::new(&backup_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();

    let mut rtx = backup.begin_read();
    assert_eq!(rtx.get(b"default").unwrap(), Some(b"value".to_vec()));
    assert_eq!(
        rtx.table_get(b"users", b"alice").unwrap(),
        Some(b"admin".to_vec())
    );
    assert_eq!(
        rtx.table_get(b"users", b"bob").unwrap(),
        Some(b"user".to_vec())
    );
}

#[test]
fn backup_snapshot_consistency() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("backup.db");

    let db = fast_builder(&db_path).create().unwrap();

    // Write initial data
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("k{i:03}");
            wtx.insert(key.as_bytes(), b"v1").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Take backup
    db.backup(&backup_path).unwrap();

    // Write more data AFTER backup
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 100..200u32 {
            let key = format!("k{i:03}");
            wtx.insert(key.as_bytes(), b"v2").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Backup should only have the first 100 keys
    let backup = DatabaseBuilder::new(&backup_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    assert_eq!(backup.stats().entry_count, 100);

    // Source should have all 200
    assert_eq!(db.stats().entry_count, 200);
}

#[test]
fn backup_integrity_check() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("backup.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            let key = format!("k{i:04}");
            wtx.insert(key.as_bytes(), b"value").unwrap();
        }
        wtx.commit().unwrap();
    }

    db.backup(&backup_path).unwrap();

    let backup = DatabaseBuilder::new(&backup_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let report = backup.integrity_check().unwrap();
    assert!(report.is_ok(), "backup integrity errors: {:?}", report.errors);
}

#[test]
fn backup_fails_if_dest_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("backup.db");

    let db = fast_builder(&db_path).create().unwrap();

    // Create the destination file first
    std::fs::write(&backup_path, b"existing").unwrap();

    let result = db.backup(&backup_path);
    assert!(result.is_err());
}
