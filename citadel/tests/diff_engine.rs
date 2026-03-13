use std::collections::BTreeMap;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sync::{LocalTreeReader, TreeReader, merkle_diff};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"diff-engine-test")
        .argon2_profile(Argon2Profile::Iot)
}

fn diff(db1: &Database, db2: &Database) -> citadel_sync::DiffResult {
    let r1 = LocalTreeReader::new(db1.manager());
    let r2 = LocalTreeReader::new(db2.manager());
    merkle_diff(&r1, &r2).unwrap()
}

fn collect_all(db: &Database) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut data = BTreeMap::new();
    let mut rtx = db.begin_read();
    rtx.for_each(|k, v| {
        data.insert(k.to_vec(), v.to_vec());
        Ok(())
    })
    .unwrap();
    data
}

fn apply(db: &Database, result: &citadel_sync::DiffResult) {
    let mut wtx = db.begin_write().unwrap();
    for e in &result.entries {
        wtx.insert(&e.key, &e.value).unwrap();
    }
    wtx.commit().unwrap();
}

// ============================================================
// Basic diff detection
// ============================================================

#[test]
fn identical_dbs_empty_diff() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), &(i * 3).to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let result = diff(&db1, &db2);
    assert!(result.is_empty());
    assert_eq!(result.subtrees_skipped, 0);
}

#[test]
fn both_empty_dbs() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let result = diff(&db1, &db2);
    assert!(result.is_empty());
}

#[test]
fn single_insert_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..20u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"new-key", b"new-value").unwrap();
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(!result.is_empty());

    let has_new = result.entries.iter().any(|e| e.key == b"new-key" && e.value == b"new-value");
    assert!(has_new);

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn value_update_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..30u32 {
            wtx.insert(&i.to_be_bytes(), b"original").unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(&15u32.to_be_bytes(), b"modified").unwrap();
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(!result.is_empty());

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn multiple_changes_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut wtx = db1.begin_write().unwrap();
    for i in (0..100u32).step_by(10) {
        wtx.insert(&i.to_be_bytes(), b"UPDATED").unwrap();
    }
    for i in 100..110u32 {
        wtx.insert(&i.to_be_bytes(), b"new").unwrap();
    }
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(!result.is_empty());

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

// ============================================================
// Efficiency — subtree skipping
// ============================================================

#[test]
fn large_dataset_skips_matching_subtrees() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let val = [0xCC_u8; 128];
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            wtx.insert(&i.to_be_bytes(), &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(&42u32.to_be_bytes(), b"CHANGED").unwrap();
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(
        result.len() < 250,
        "should skip matching subtrees: got {} entries out of 500",
        result.len()
    );
    assert!(result.subtrees_skipped > 0, "must skip at least one subtree");
    assert!(result.entries.iter().any(|e| e.key == 42u32.to_be_bytes()));
}

#[test]
fn diff_metrics_pages_compared_and_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let val = [0xDD_u8; 128];
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            wtx.insert(&i.to_be_bytes(), &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(&100u32.to_be_bytes(), b"changed").unwrap();
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(result.pages_compared > 0);
    assert!(result.subtrees_skipped > 0);
    assert!(result.pages_compared > result.subtrees_skipped);
}

// ============================================================
// Edge cases
// ============================================================

#[test]
fn populated_source_vs_empty_target() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let mut wtx = db1.begin_write().unwrap();
    for i in 0..50u32 {
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(!result.is_empty());

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn empty_source_vs_populated_target() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let mut wtx = db2.begin_write().unwrap();
    for i in 0..50u32 {
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    // Source has no data, diff finds nothing (one-directional)
    let result = diff(&db1, &db2);
    // Source leaf is empty — diff collects source's entries which is just the empty leaf
    // The diff may or may not be empty depending on leaf structure
    // But applying it shouldn't break anything
    apply(&db2, &result);
}

#[test]
fn diff_after_sync_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"extra", b"data").unwrap();
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    apply(&db2, &result);

    let result2 = diff(&db1, &db2);
    assert!(result2.is_empty(), "second diff after sync must be empty");
}

#[test]
fn incremental_diff_3_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    for round in 0..3u32 {
        let mut wtx = db1.begin_write().unwrap();
        let key = (200 + round).to_be_bytes();
        wtx.insert(&key, &format!("round-{round}").into_bytes()).unwrap();
        wtx.commit().unwrap();

        let result = diff(&db1, &db2);
        assert!(!result.is_empty(), "round {round}: diff must find changes");
        apply(&db2, &result);

        assert_eq!(
            collect_all(&db1),
            collect_all(&db2),
            "round {round}: data must match after sync"
        );
    }
}

#[test]
fn diff_detects_all_changed_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), b"same").unwrap();
        }
        wtx.commit().unwrap();
    }

    let changed_keys: Vec<u32> = vec![5, 15, 25, 35, 45];
    let mut wtx = db1.begin_write().unwrap();
    for &k in &changed_keys {
        wtx.insert(&k.to_be_bytes(), b"CHANGED").unwrap();
    }
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    for &k in &changed_keys {
        let found = result.entries.iter().any(|e| e.key == k.to_be_bytes());
        assert!(found, "diff must contain changed key {k}");
    }
}

#[test]
fn large_values_diff() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let big_val = vec![0xAA_u8; 512];
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..20u32 {
            wtx.insert(&i.to_be_bytes(), &big_val).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(&10u32.to_be_bytes(), &vec![0xBB_u8; 512]).unwrap();
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(!result.is_empty());
    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn tree_reader_root_info() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"val").unwrap();
    wtx.commit().unwrap();

    let reader = LocalTreeReader::new(db.manager());
    let (root_pid, root_hash) = reader.root_info().unwrap();

    assert!(root_pid.is_valid());
    assert_ne!(root_hash, [0u8; 28]);
    assert_eq!(root_hash, db.stats().merkle_root);
}

#[test]
fn tree_reader_leaf_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"aaa", b"111").unwrap();
    wtx.insert(b"bbb", b"222").unwrap();
    wtx.insert(b"ccc", b"333").unwrap();
    wtx.commit().unwrap();

    let reader = LocalTreeReader::new(db.manager());
    let (root_pid, _) = reader.root_info().unwrap();

    let entries = reader.leaf_entries(root_pid).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].key, b"aaa");
    assert_eq!(entries[1].key, b"bbb");
    assert_eq!(entries[2].key, b"ccc");
}
