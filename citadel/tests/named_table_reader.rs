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
    let reader = LocalTreeReader::for_table(db.manager(), table_root).unwrap();
    let (root, hash) = reader.root_info().unwrap();

    assert_eq!(root, table_root);
    assert_ne!(hash, ZERO_HASH);

    // Should be different from default tree
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
    let reader = LocalTreeReader::for_table(db.manager(), table_root).unwrap();

    // Read leaf entries from root (single leaf page for small table)
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

    let root1 = db.manager().table_root(b"t1").unwrap().unwrap();
    let root2 = db.manager().table_root(b"t2").unwrap().unwrap();

    let reader1 = LocalTreeReader::for_table(db.manager(), root1).unwrap();
    let reader2 = LocalTreeReader::for_table(db.manager(), root2).unwrap();

    let (_, hash1) = reader1.root_info().unwrap();
    let (_, hash2) = reader2.root_info().unwrap();

    assert_ne!(hash1, hash2);
}
