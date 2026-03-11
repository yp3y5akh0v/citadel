use citadel::{Argon2Profile, DatabaseBuilder};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
}

#[test]
fn integrity_check_empty_db() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();
    let report = db.integrity_check().unwrap();
    assert!(report.is_ok(), "errors: {:?}", report.errors);
    assert!(report.pages_checked >= 1);
}

#[test]
fn integrity_check_with_data() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

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

    let report = db.integrity_check().unwrap();
    assert!(report.is_ok(), "errors: {:?}", report.errors);
    assert!(report.pages_checked > 1);
}

#[test]
fn integrity_check_with_named_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"default_key", b"default_val").unwrap();
        wtx.create_table(b"users").unwrap();
        wtx.create_table(b"config").unwrap();
        for i in 0..100u32 {
            let key = format!("u{i:03}");
            wtx.table_insert(b"users", key.as_bytes(), b"user").unwrap();
        }
        wtx.table_insert(b"config", b"setting", b"value").unwrap();
        wtx.commit().unwrap();
    }

    let report = db.integrity_check().unwrap();
    assert!(report.is_ok(), "errors: {:?}", report.errors);
}

#[test]
fn integrity_check_after_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            let key = format!("k{i:04}");
            wtx.insert(key.as_bytes(), b"val").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Delete half the keys
    {
        let mut wtx = db.begin_write().unwrap();
        for i in (0..500u32).step_by(2) {
            let key = format!("k{i:04}");
            wtx.delete(key.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let report = db.integrity_check().unwrap();
    assert!(report.is_ok(), "errors: {:?}", report.errors);
}

#[test]
fn integrity_check_detects_tampered_page() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("k{i:03}");
            wtx.insert(key.as_bytes(), b"value").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Read the file header to find the active tree root page
    let root_page_id: u32;
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::File::open(&db_path).unwrap();

        // Read god byte at offset 20 to determine active slot
        file.seek(SeekFrom::Start(20)).unwrap();
        let mut god = [0u8; 1];
        file.read_exact(&mut god).unwrap();
        let active_slot = (god[0] & 0x01) as u64;

        // Commit slot starts at offset 32, each slot is 240 bytes
        // tree_root is at offset 8 within a slot
        let slot_offset = 32 + active_slot * 240 + 8;
        file.seek(SeekFrom::Start(slot_offset)).unwrap();
        let mut root_bytes = [0u8; 4];
        file.read_exact(&mut root_bytes).unwrap();
        root_page_id = u32::from_le_bytes(root_bytes);
    }

    // Tamper with the root page's ciphertext area
    {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();

        // Page N starts at FILE_HEADER_SIZE (512) + N * PAGE_SIZE (8208)
        // Ciphertext starts after IV (16 bytes)
        let page_offset = 512 + root_page_id as u64 * 8208;
        let tamper_offset = page_offset + 16 + 100;
        file.seek(SeekFrom::Start(tamper_offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(tamper_offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.flush().unwrap();
    }

    // Integrity check should detect the tampering
    let db = fast_builder(&db_path).open().unwrap();
    let report = db.integrity_check().unwrap();
    assert!(
        !report.is_ok(),
        "expected integrity errors after page tampering"
    );
    assert!(!report.errors.is_empty());
}

#[test]
fn integrity_check_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            let key = format!("k{i:04}");
            wtx.insert(key.as_bytes(), b"value").unwrap();
        }
        wtx.commit().unwrap();
    }

    let db = fast_builder(&db_path).open().unwrap();
    let report = db.integrity_check().unwrap();
    assert!(report.is_ok(), "errors: {:?}", report.errors);
}

#[test]
fn integrity_check_after_multiple_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    for round in 0..10u32 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            let key = format!("r{round}_k{i:03}");
            wtx.insert(key.as_bytes(), b"val").unwrap();
        }
        wtx.commit().unwrap();
    }

    let report = db.integrity_check().unwrap();
    assert!(report.is_ok(), "errors: {:?}", report.errors);
    assert_eq!(db.stats().entry_count, 500);
}
