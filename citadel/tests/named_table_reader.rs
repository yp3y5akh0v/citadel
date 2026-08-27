use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_core::MERKLE_HASH_SIZE;
use citadel_sync::diff::TreeReader;
use citadel_sync::LocalTreeReader;

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"reader-test")
        .argon2_profile(Argon2Profile::Iot)
}

const ZERO_HASH: [u8; MERKLE_HASH_SIZE] = [0u8; MERKLE_HASH_SIZE];

#[test]
fn default_tree_reader() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"k1", b"v1").unwrap();
    wtx.commit().unwrap();

    let reader = LocalTreeReader::new(db.manager());
    let (root, hash) = reader.root_info().unwrap();
    assert!(root.is_valid());
    assert_ne!(hash, ZERO_HASH);

    let slot = db.manager().current_slot();
    assert_eq!(root, slot.tree_root);
    assert_eq!(hash, slot.merkle_root);
}

#[test]
fn named_table_reader() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"users").unwrap();
    wtx.table_insert(b"users", b"k1", b"v1").unwrap();
    wtx.commit().unwrap();

    let table_root = db.manager().table_root(b"users").unwrap().unwrap();
    let reader = LocalTreeReader::for_table(db.manager(), b"users").unwrap();
    let (root, hash) = reader.root_info().unwrap();

    assert_eq!(root, table_root);
    assert_ne!(hash, ZERO_HASH);

    let default_reader = LocalTreeReader::new(db.manager());
    let (def_root, def_hash) = default_reader.root_info().unwrap();
    assert_ne!(root, def_root);
    assert_ne!(hash, def_hash);
}

#[test]
fn table_reader_can_read_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"data").unwrap();
    wtx.table_insert(b"data", b"alpha", b"100").unwrap();
    wtx.table_insert(b"data", b"beta", b"200").unwrap();
    wtx.commit().unwrap();

    let table_root = db.manager().table_root(b"data").unwrap().unwrap();
    let reader = LocalTreeReader::for_table(db.manager(), b"data").unwrap();

    // Read leaf entries from root (single leaf page for small table)
    reader.page_digest(table_root).unwrap();
    let entries = reader.leaf_entries(table_root).unwrap();
    assert_eq!(entries.len(), 2);

    let keys: Vec<&[u8]> = entries.iter().map(|e| e.key.as_slice()).collect();
    assert!(keys.contains(&b"alpha".as_slice()));
    assert!(keys.contains(&b"beta".as_slice()));
}

#[test]
fn two_table_readers_differ() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"t1").unwrap();
    wtx.create_table(b"t2").unwrap();
    wtx.table_insert(b"t1", b"k", b"val_t1").unwrap();
    wtx.table_insert(b"t2", b"k", b"val_t2").unwrap();
    wtx.commit().unwrap();

    let reader1 = LocalTreeReader::for_table(db.manager(), b"t1").unwrap();
    let reader2 = LocalTreeReader::for_table(db.manager(), b"t2").unwrap();

    let (_, hash1) = reader1.root_info().unwrap();
    let (_, hash2) = reader2.root_info().unwrap();

    assert_ne!(hash1, hash2);
}

#[test]
fn readers_reject_pages_outside_their_advertised_tree() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"default", b"value").unwrap();
    for table in [
        b"first".as_slice(),
        b"second".as_slice(),
        b"doomed".as_slice(),
    ] {
        wtx.create_table(table).unwrap();
        wtx.table_insert(table, b"key", b"value").unwrap();
    }
    wtx.commit().unwrap();

    let first_root = db.manager().table_root(b"first").unwrap().unwrap();
    let second_root = db.manager().table_root(b"second").unwrap().unwrap();
    let doomed_root = db.manager().table_root(b"doomed").unwrap().unwrap();
    assert_ne!(first_root, second_root);

    let default_reader = LocalTreeReader::new(db.manager());
    assert!(matches!(
        default_reader.page_digest(first_root),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));
    assert!(matches!(
        default_reader.leaf_entries(first_root),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));

    let first_reader = LocalTreeReader::for_table(db.manager(), b"first").unwrap();
    first_reader.page_digest(first_root).unwrap();
    assert!(matches!(
        first_reader.page_digest(second_root),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));
    assert!(matches!(
        first_reader.leaf_entries(second_root),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));

    let _stale_horizon = db.begin_read();
    let mut wtx = db.begin_write().unwrap();
    wtx.drop_table(b"doomed").unwrap();
    wtx.create_table(b"live").unwrap();
    wtx.table_insert(b"live", b"key", b"new").unwrap();
    wtx.commit().unwrap();
    let live_reader = LocalTreeReader::for_table(db.manager(), b"live").unwrap();
    let (live_root, _) = live_reader.root_info().unwrap();
    assert_ne!(live_root, doomed_root);
    assert!(matches!(
        live_reader.page_digest(doomed_root),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));
}
