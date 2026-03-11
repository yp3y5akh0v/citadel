use citadel::{Argon2Profile, DatabaseBuilder, Error};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
}

#[test]
fn create_table_and_crud() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"users").unwrap();
        wtx.table_insert(b"users", b"alice", b"admin").unwrap();
        wtx.table_insert(b"users", b"bob", b"user").unwrap();
        wtx.table_insert(b"users", b"charlie", b"user").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(
        rtx.table_get(b"users", b"alice").unwrap(),
        Some(b"admin".to_vec())
    );
    assert_eq!(
        rtx.table_get(b"users", b"bob").unwrap(),
        Some(b"user".to_vec())
    );
    assert_eq!(rtx.table_get(b"users", b"missing").unwrap(), None);
}

#[test]
fn multiple_tables_independent() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"users").unwrap();
        wtx.create_table(b"config").unwrap();

        wtx.table_insert(b"users", b"alice", b"admin").unwrap();
        wtx.table_insert(b"config", b"theme", b"dark").unwrap();

        // Default table is separate
        wtx.insert(b"global", b"value").unwrap();

        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(
        rtx.table_get(b"users", b"alice").unwrap(),
        Some(b"admin".to_vec())
    );
    assert_eq!(
        rtx.table_get(b"config", b"theme").unwrap(),
        Some(b"dark".to_vec())
    );
    // Cross-table isolation
    assert_eq!(rtx.table_get(b"users", b"theme").unwrap(), None);
    assert_eq!(rtx.table_get(b"config", b"alice").unwrap(), None);
    // Default table unaffected
    assert_eq!(rtx.get(b"global").unwrap(), Some(b"value".to_vec()));
    assert_eq!(rtx.entry_count(), 1);
}

#[test]
fn table_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"data").unwrap();
        for i in 0..200u32 {
            let key = format!("k{i:04}");
            let val = format!("v{i:04}");
            wtx.table_insert(b"data", key.as_bytes(), val.as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }

    // Reopen and verify
    {
        let db = fast_builder(&db_path).open().unwrap();
        let mut rtx = db.begin_read();
        for i in 0..200u32 {
            let key = format!("k{i:04}");
            let val = format!("v{i:04}");
            assert_eq!(
                rtx.table_get(b"data", key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "mismatch at key {key}"
            );
        }
    }
}

#[test]
fn table_update_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    // Create and populate
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"items").unwrap();
        wtx.table_insert(b"items", b"a", b"1").unwrap();
        wtx.table_insert(b"items", b"b", b"2").unwrap();
        wtx.table_insert(b"items", b"c", b"3").unwrap();
        wtx.commit().unwrap();
    }

    // Update and delete
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.table_insert(b"items", b"a", b"updated").unwrap();
        wtx.table_delete(b"items", b"b").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(
        rtx.table_get(b"items", b"a").unwrap(),
        Some(b"updated".to_vec())
    );
    assert_eq!(rtx.table_get(b"items", b"b").unwrap(), None);
    assert_eq!(
        rtx.table_get(b"items", b"c").unwrap(),
        Some(b"3".to_vec())
    );
}

#[test]
fn drop_table() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"temp").unwrap();
        wtx.table_insert(b"temp", b"key", b"val").unwrap();
        wtx.commit().unwrap();
    }

    // Drop the table
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.drop_table(b"temp").unwrap();
        wtx.commit().unwrap();
    }

    // Table should no longer exist
    let mut rtx = db.begin_read();
    assert!(matches!(
        rtx.table_get(b"temp", b"key"),
        Err(Error::TableNotFound(_))
    ));
}

#[test]
fn table_not_found_error() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();
    let mut wtx = db.begin_write().unwrap();
    assert!(matches!(
        wtx.table_insert(b"nonexistent", b"k", b"v"),
        Err(Error::TableNotFound(_))
    ));
}

#[test]
fn table_already_exists_error() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();
    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"dup").unwrap();
    assert!(matches!(
        wtx.create_table(b"dup"),
        Err(Error::TableAlreadyExists(_))
    ));
}

#[test]
fn read_table_snapshot_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    // Create table with initial data
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"data").unwrap();
        wtx.table_insert(b"data", b"key1", b"v1").unwrap();
        wtx.commit().unwrap();
    }

    // Start a reader
    let mut rtx = db.begin_read();
    assert_eq!(
        rtx.table_get(b"data", b"key1").unwrap(),
        Some(b"v1".to_vec())
    );

    // Write more data after reader started
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.table_insert(b"data", b"key2", b"v2").unwrap();
        wtx.commit().unwrap();
    }

    // Old reader should NOT see key2
    assert_eq!(rtx.table_get(b"data", b"key2").unwrap(), None);

    // New reader should see both
    let mut rtx2 = db.begin_read();
    assert_eq!(
        rtx2.table_get(b"data", b"key1").unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        rtx2.table_get(b"data", b"key2").unwrap(),
        Some(b"v2".to_vec())
    );
}

#[test]
fn many_tables_stress() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    // Create 20 tables with 50 entries each
    {
        let mut wtx = db.begin_write().unwrap();
        for t in 0..20u32 {
            let table_name = format!("table_{t:02}");
            wtx.create_table(table_name.as_bytes()).unwrap();
            for i in 0..50u32 {
                let key = format!("k{i:03}");
                let val = format!("t{t}_v{i}");
                wtx.table_insert(table_name.as_bytes(), key.as_bytes(), val.as_bytes())
                    .unwrap();
            }
        }
        wtx.commit().unwrap();
    }

    // Verify all data
    let mut rtx = db.begin_read();
    for t in 0..20u32 {
        let table_name = format!("table_{t:02}");
        for i in 0..50u32 {
            let key = format!("k{i:03}");
            let val = format!("t{t}_v{i}");
            assert_eq!(
                rtx.table_get(table_name.as_bytes(), key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "mismatch at {table_name}/{key}"
            );
        }
    }
}

#[test]
fn table_contains_key() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"test").unwrap();
        wtx.table_insert(b"test", b"exists", b"yes").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert!(rtx.table_contains_key(b"test", b"exists").unwrap());
    assert!(!rtx.table_contains_key(b"test", b"nope").unwrap());
}
