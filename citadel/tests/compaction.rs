use citadel::{Argon2Profile, DatabaseBuilder};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
}

#[test]
fn compact_empty_db() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let compact_path = dir.path().join("compact.db");

    let db = fast_builder(&db_path).create().unwrap();
    db.compact(&compact_path).unwrap();

    let compacted = DatabaseBuilder::new(&compact_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    assert_eq!(compacted.stats().entry_count, 0);
}

#[test]
fn compact_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let compact_path = dir.path().join("compact.db");

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

    db.compact(&compact_path).unwrap();

    let compacted = DatabaseBuilder::new(&compact_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    assert_eq!(compacted.stats().entry_count, 1000);

    let mut rtx = compacted.begin_read();
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
fn compact_reduces_file_size() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let compact_path = dir.path().join("compact.db");

    let db = fast_builder(&db_path).create().unwrap();

    // Insert many keys
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            let key = format!("k{i:04}");
            wtx.insert(key.as_bytes(), b"value-data-here").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Delete most of them (creating free pages)
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..400u32 {
            let key = format!("k{i:04}");
            wtx.delete(key.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let original_size = std::fs::metadata(&db_path).unwrap().len();
    db.compact(&compact_path).unwrap();
    let compact_size = std::fs::metadata(&compact_path).unwrap().len();

    // Compacted file should be smaller (fewer live pages)
    assert!(
        compact_size < original_size,
        "compact ({compact_size}) should be smaller than original ({original_size})"
    );
}

#[test]
fn compact_preserves_named_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let compact_path = dir.path().join("compact.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"default", b"value").unwrap();
        wtx.create_table(b"users").unwrap();
        wtx.table_insert(b"users", b"alice", b"admin").unwrap();
        wtx.create_table(b"settings").unwrap();
        wtx.table_insert(b"settings", b"theme", b"dark").unwrap();
        wtx.commit().unwrap();
    }

    db.compact(&compact_path).unwrap();

    let compacted = DatabaseBuilder::new(&compact_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();

    let mut rtx = compacted.begin_read();
    assert_eq!(rtx.get(b"default").unwrap(), Some(b"value".to_vec()));
    assert_eq!(
        rtx.table_get(b"users", b"alice").unwrap(),
        Some(b"admin".to_vec())
    );
    assert_eq!(
        rtx.table_get(b"settings", b"theme").unwrap(),
        Some(b"dark".to_vec())
    );
}

#[test]
fn compact_integrity_check() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let compact_path = dir.path().join("compact.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            let key = format!("k{i:04}");
            wtx.insert(key.as_bytes(), b"value").unwrap();
        }
        wtx.commit().unwrap();
    }

    db.compact(&compact_path).unwrap();

    let compacted = DatabaseBuilder::new(&compact_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let report = compacted.integrity_check().unwrap();
    assert!(
        report.is_ok(),
        "compact integrity errors: {:?}",
        report.errors
    );
}

#[test]
fn compact_no_pending_free() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let compact_path = dir.path().join("compact.db");

    let db = fast_builder(&db_path).create().unwrap();

    // Create some churn to generate pending-free pages
    for round in 0..5 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("r{round}_k{i:03}");
            wtx.insert(key.as_bytes(), b"data").unwrap();
        }
        wtx.commit().unwrap();
    }

    db.compact(&compact_path).unwrap();

    let compacted = DatabaseBuilder::new(&compact_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();

    // Compacted DB should have no pending-free pages
    let stats = compacted.stats();
    // HWM should equal total pages (no gaps)
    assert_eq!(stats.total_pages, stats.high_water_mark);
}

#[test]
fn compact_fails_if_dest_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let compact_path = dir.path().join("compact.db");

    let db = fast_builder(&db_path).create().unwrap();

    std::fs::write(&compact_path, b"existing").unwrap();
    let result = db.compact(&compact_path);
    assert!(result.is_err());
}

#[test]
fn compact_then_writable() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let compact_path = dir.path().join("compact.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            let key = format!("k{i:04}");
            wtx.insert(key.as_bytes(), b"original").unwrap();
        }
        wtx.commit().unwrap();
    }

    db.compact(&compact_path).unwrap();

    // Open the compacted DB and continue writing
    let compacted = DatabaseBuilder::new(&compact_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();

    {
        let mut wtx = compacted.begin_write().unwrap();
        wtx.insert(b"new_key", b"new_value").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = compacted.begin_read();
    assert_eq!(rtx.get(b"new_key").unwrap(), Some(b"new_value".to_vec()));
    assert_eq!(rtx.get(b"k0000").unwrap(), Some(b"original".to_vec()));
    assert_eq!(compacted.stats().entry_count, 201);
}
