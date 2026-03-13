//! Integration tests for inline Merkle hashing.
//!
//! Tests that the BLAKE3 Merkle root in DbStats reflects actual
//! database content, changes on writes, persists across reopens,
//! and survives backup/compact operations.

use citadel::{Argon2Profile, DatabaseBuilder};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"merkle-test")
        .argon2_profile(Argon2Profile::Iot)
}

const ZERO_HASH: [u8; 28] = [0u8; 28];

// ============================================================
// Basic properties
// ============================================================

#[test]
fn empty_db_has_nonzero_merkle_root() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();
    let stats = db.stats();
    assert_ne!(stats.merkle_root, ZERO_HASH, "empty DB must have a hash (BLAKE3 of empty leaf)");
}

#[test]
fn merkle_root_changes_on_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let before = db.stats().merkle_root;

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"hello", b"world").unwrap();
    wtx.commit().unwrap();

    let after = db.stats().merkle_root;
    assert_ne!(before, after, "insert must change merkle root");
}

#[test]
fn merkle_root_changes_on_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"val").unwrap();
    wtx.commit().unwrap();
    let after_insert = db.stats().merkle_root;

    let mut wtx = db.begin_write().unwrap();
    wtx.delete(b"key").unwrap();
    wtx.commit().unwrap();
    let after_delete = db.stats().merkle_root;

    assert_ne!(after_insert, after_delete, "delete must change merkle root");
}

#[test]
fn merkle_root_changes_on_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"val1").unwrap();
    wtx.commit().unwrap();
    let h1 = db.stats().merkle_root;

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"val2").unwrap();
    wtx.commit().unwrap();
    let h2 = db.stats().merkle_root;

    assert_ne!(h1, h2, "update must change merkle root");
}

#[test]
fn abort_does_not_change_merkle_root() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let before = db.stats().merkle_root;

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"val").unwrap();
    wtx.abort();

    let after = db.stats().merkle_root;
    assert_eq!(before, after, "abort must not change merkle root");
}

// ============================================================
// Determinism
// ============================================================

#[test]
fn same_data_produces_same_merkle_root() {
    let dir = tempfile::tempdir().unwrap();

    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"alpha", b"one").unwrap();
        wtx.insert(b"beta", b"two").unwrap();
        wtx.insert(b"gamma", b"three").unwrap();
        wtx.commit().unwrap();
    }

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "identical data must produce identical merkle root"
    );
}

#[test]
fn different_data_produces_different_merkle_root() {
    let dir = tempfile::tempdir().unwrap();

    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"key", b"value-A").unwrap();
    wtx.commit().unwrap();

    let mut wtx = db2.begin_write().unwrap();
    wtx.insert(b"key", b"value-B").unwrap();
    wtx.commit().unwrap();

    assert_ne!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "different data must produce different merkle root"
    );
}

#[test]
fn different_keys_produce_different_merkle_root() {
    let dir = tempfile::tempdir().unwrap();

    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"keyA", b"value").unwrap();
    wtx.commit().unwrap();

    let mut wtx = db2.begin_write().unwrap();
    wtx.insert(b"keyB", b"value").unwrap();
    wtx.commit().unwrap();

    assert_ne!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
    );
}

#[test]
fn insert_order_does_not_affect_merkle_root() {
    let dir = tempfile::tempdir().unwrap();

    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // DB1: insert in order A, B, C
    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"aaa", b"1").unwrap();
    wtx.insert(b"bbb", b"2").unwrap();
    wtx.insert(b"ccc", b"3").unwrap();
    wtx.commit().unwrap();

    // DB2: insert in order C, A, B
    let mut wtx = db2.begin_write().unwrap();
    wtx.insert(b"ccc", b"3").unwrap();
    wtx.insert(b"aaa", b"1").unwrap();
    wtx.insert(b"bbb", b"2").unwrap();
    wtx.commit().unwrap();

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "insertion order within same txn must not affect merkle root"
    );
}

// ============================================================
// Persistence
// ============================================================

#[test]
fn merkle_root_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let h1;
    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
        h1 = db.stats().merkle_root;
    }

    {
        let db = fast_builder(&db_path).open().unwrap();
        let h2 = db.stats().merkle_root;
        assert_eq!(h1, h2, "merkle root must persist across reopen");
    }
}

#[test]
fn merkle_root_persists_across_multiple_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let mut hashes = Vec::new();

    // Session 1: create + insert
    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"key1", b"val1").unwrap();
        wtx.commit().unwrap();
        hashes.push(db.stats().merkle_root);
    }

    // Session 2: more inserts
    {
        let db = fast_builder(&db_path).open().unwrap();
        assert_eq!(db.stats().merkle_root, hashes[0]);
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"key2", b"val2").unwrap();
        wtx.commit().unwrap();
        hashes.push(db.stats().merkle_root);
    }

    // Session 3: verify accumulated
    {
        let db = fast_builder(&db_path).open().unwrap();
        assert_eq!(db.stats().merkle_root, hashes[1]);
        assert_ne!(hashes[0], hashes[1]);
    }
}

// ============================================================
// Backup & Compact
// ============================================================

#[test]
fn backup_preserves_merkle_root() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..100u32 {
        wtx.insert(&i.to_be_bytes(), b"data").unwrap();
    }
    wtx.commit().unwrap();

    let original_root = db.stats().merkle_root;

    let backup_path = dir.path().join("backup.db");
    db.backup(&backup_path).unwrap();
    drop(db);

    let backup_db = fast_builder(&backup_path).open().unwrap();
    assert_eq!(
        backup_db.stats().merkle_root,
        original_root,
        "backup must preserve merkle root"
    );
}

#[test]
fn compact_preserves_merkle_root() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    // Insert, then delete some to create free pages
    let mut wtx = db.begin_write().unwrap();
    for i in 0..100u32 {
        wtx.insert(&i.to_be_bytes(), b"data").unwrap();
    }
    wtx.commit().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 50..100u32 {
        wtx.delete(&i.to_be_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    let root_before = db.stats().merkle_root;

    let compact_path = dir.path().join("compact.db");
    db.compact(&compact_path).unwrap();
    drop(db);

    let compacted = fast_builder(&compact_path).open().unwrap();
    assert_eq!(
        compacted.stats().merkle_root,
        root_before,
        "compact must preserve merkle root"
    );
}

// ============================================================
// Multi-transaction tracking
// ============================================================

#[test]
fn each_commit_produces_unique_merkle_root() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut seen_roots = std::collections::HashSet::new();
    seen_roots.insert(db.stats().merkle_root);

    for i in 0..20u32 {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        wtx.commit().unwrap();

        let root = db.stats().merkle_root;
        assert!(
            seen_roots.insert(root),
            "transaction {i}: merkle root collision with a previous commit"
        );
    }
}

#[test]
fn delete_then_reinsert_restores_merkle_root() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"only-key", b"only-value").unwrap();
    wtx.commit().unwrap();
    let h_with = db.stats().merkle_root;

    let mut wtx = db.begin_write().unwrap();
    wtx.delete(b"only-key").unwrap();
    wtx.commit().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"only-key", b"only-value").unwrap();
    wtx.commit().unwrap();
    let h_restored = db.stats().merkle_root;

    assert_eq!(h_with, h_restored,
        "deleting and reinserting same key-value must restore the merkle root");
}

// ============================================================
// Named tables
// ============================================================

#[test]
fn named_table_operations_do_not_affect_default_merkle_root() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"default-key", b"val").unwrap();
    wtx.commit().unwrap();
    let h_before = db.stats().merkle_root;

    // Named table ops modify the catalog, not the default tree
    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"my_table").unwrap();
    wtx.table_insert(b"my_table", b"tkey", b"tval").unwrap();
    wtx.commit().unwrap();
    let h_after = db.stats().merkle_root;

    // The merkle_root in CommitSlot tracks the default tree only
    assert_eq!(h_before, h_after,
        "named table changes must not affect default tree merkle root");
}

// ============================================================
// Edge cases
// ============================================================

#[test]
fn empty_value_has_distinct_hash() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"key", b"").unwrap();
    wtx.commit().unwrap();

    let mut wtx = db2.begin_write().unwrap();
    wtx.insert(b"key", b"x").unwrap();
    wtx.commit().unwrap();

    assert_ne!(db1.stats().merkle_root, db2.stats().merkle_root);
}

#[test]
fn single_byte_keys_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut roots = Vec::new();
    for b in 0..5u8 {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(&[b], &[b]).unwrap();
        wtx.commit().unwrap();
        roots.push(db.stats().merkle_root);
    }

    // Each insert changes the root
    for i in 1..roots.len() {
        assert_ne!(roots[i - 1], roots[i], "insert {i} must change root");
    }
}

#[test]
fn integrity_check_passes_with_merkle_hashes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..200u32 {
        wtx.insert(&i.to_be_bytes(), &format!("value-{i}").into_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    let report = db.integrity_check().unwrap();
    assert!(report.is_ok(), "integrity check must pass: {:?}", report.errors);
}
