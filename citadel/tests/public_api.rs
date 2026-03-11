//! Integration tests for the public Database API.

use citadel::{Argon2Profile, DatabaseBuilder, Error};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot) // fast for tests
}

#[test]
fn create_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    // Create
    {
        let db = fast_builder(&db_path).create().unwrap();
        let stats = db.stats();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.tree_depth, 1);
    }

    // Reopen
    {
        let db = fast_builder(&db_path).open().unwrap();
        assert_eq!(db.stats().entry_count, 0);
    }
}

#[test]
fn insert_and_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"hello", b"world").unwrap();
        wtx.insert(b"foo", b"bar").unwrap();
        wtx.commit().unwrap();

        let mut rtx = db.begin_read();
        assert_eq!(rtx.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(rtx.get(b"foo").unwrap(), Some(b"bar".to_vec()));
        assert_eq!(rtx.get(b"missing").unwrap(), None);
    }

    // Reopen and verify persistence
    {
        let db = fast_builder(&db_path).open().unwrap();
        let mut rtx = db.begin_read();
        assert_eq!(rtx.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(rtx.get(b"foo").unwrap(), Some(b"bar".to_vec()));
        assert_eq!(db.stats().entry_count, 2);
    }
}

#[test]
fn wrong_passphrase() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    fast_builder(&db_path).create().unwrap();

    let result = DatabaseBuilder::new(&db_path)
        .passphrase(b"wrong-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open();

    assert!(result.is_err());
}

#[test]
fn passphrase_required() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let result = DatabaseBuilder::new(&db_path).create();
    assert!(matches!(result, Err(Error::PassphraseRequired)));
}

#[test]
fn create_fails_if_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    fast_builder(&db_path).create().unwrap();

    // Second create should fail (file already exists)
    let result = fast_builder(&db_path).create();
    assert!(result.is_err());
}

#[test]
fn open_fails_if_not_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("nonexistent.db");

    let result = fast_builder(&db_path).open();
    assert!(result.is_err());
}

#[test]
fn multiple_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    // Txn 1: insert
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("key-{i:04}");
            let val = format!("val-{i:04}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(db.stats().entry_count, 100);

    // Txn 2: update some, delete some
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"key-0000", b"updated").unwrap();
        wtx.delete(b"key-0050").unwrap();
        wtx.commit().unwrap();
    }

    assert_eq!(db.stats().entry_count, 99);

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key-0000").unwrap(), Some(b"updated".to_vec()));
    assert_eq!(rtx.get(b"key-0050").unwrap(), None);
    assert_eq!(
        rtx.get(b"key-0001").unwrap(),
        Some(b"val-0001".to_vec())
    );
}

#[test]
fn abort_discards_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"key", b"value").unwrap();
        wtx.abort();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), None);
}

#[test]
fn snapshot_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    // Write initial data
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"key1", b"v1").unwrap();
        wtx.commit().unwrap();
    }

    // Start a read
    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.reader_count(), 1);

    // Write more data after the read started
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"key2", b"v2").unwrap();
        wtx.commit().unwrap();
    }

    // Read should NOT see key2
    assert_eq!(rtx.get(b"key2").unwrap(), None);

    // New read should see both
    let mut rtx2 = db.begin_read();
    assert_eq!(rtx2.get(b"key1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(rtx2.get(b"key2").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn custom_key_path() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let key_path = dir.path().join("custom.keys");

    {
        let db = DatabaseBuilder::new(&db_path)
            .passphrase(b"test")
            .argon2_profile(Argon2Profile::Iot)
            .key_path(&key_path)
            .create()
            .unwrap();

        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"k", b"v").unwrap();
        wtx.commit().unwrap();

        assert_eq!(db.key_path(), key_path);
    }

    // Reopen with same custom key path
    {
        let db = DatabaseBuilder::new(&db_path)
            .passphrase(b"test")
            .argon2_profile(Argon2Profile::Iot)
            .key_path(&key_path)
            .open()
            .unwrap();

        let mut rtx = db.begin_read();
        assert_eq!(rtx.get(b"k").unwrap(), Some(b"v".to_vec()));
    }
}

#[test]
fn stats_update_after_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let db = fast_builder(&db_path).create().unwrap();

    let s0 = db.stats();
    assert_eq!(s0.entry_count, 0);
    assert_eq!(s0.tree_depth, 1);

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            let key = format!("k{i:04}");
            wtx.insert(key.as_bytes(), b"value").unwrap();
        }
        wtx.commit().unwrap();
    }

    let s1 = db.stats();
    assert_eq!(s1.entry_count, 500);
    assert!(s1.tree_depth >= 2);
    assert!(s1.total_pages > 1);
    assert!(s1.high_water_mark > 1);
}

#[test]
fn large_dataset_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let count = 5000u32;

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        for i in 0..count {
            let key = format!("key-{i:06}");
            let val = format!("val-{i:06}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Reopen and verify all data
    {
        let db = fast_builder(&db_path).open().unwrap();
        assert_eq!(db.stats().entry_count, count as u64);

        let mut rtx = db.begin_read();
        for i in 0..count {
            let key = format!("key-{i:06}");
            let val = format!("val-{i:06}");
            assert_eq!(
                rtx.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "mismatch at key {key}"
            );
        }
    }
}
