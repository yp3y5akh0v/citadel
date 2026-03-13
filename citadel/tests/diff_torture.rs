use std::collections::BTreeMap;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sync::{LocalTreeReader, merkle_diff};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"diff-torture")
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
// Correctness
// ============================================================

#[test]
fn random_500_entries_diff_correctness() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let mut wtx1 = db1.begin_write().unwrap();
    let mut wtx2 = db2.begin_write().unwrap();
    for i in 0..500u32 {
        let val = i.to_le_bytes();
        wtx1.insert(&i.to_be_bytes(), &val).unwrap();
        wtx2.insert(&i.to_be_bytes(), &val).unwrap();
    }
    wtx1.commit().unwrap();
    wtx2.commit().unwrap();

    // Mutate 50 random entries in db1
    let changed: Vec<u32> = (0..50).map(|i| i * 10).collect();
    let mut wtx = db1.begin_write().unwrap();
    for &k in &changed {
        wtx.insert(&k.to_be_bytes(), b"MUTATED").unwrap();
    }
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(!result.is_empty());

    // Every changed key must appear in diff
    for &k in &changed {
        let found = result.entries.iter().any(|e| e.key == k.to_be_bytes());
        assert!(found, "changed key {k} missing from diff");
    }

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn random_mutations_100_rounds_incremental() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Seed both with same data
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // 100 rounds of single-key mutation + sync
    for round in 0..100u32 {
        let key = (round % 200).to_be_bytes();
        let val = format!("round-{round}");
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(&key, val.as_bytes()).unwrap();
        wtx.commit().unwrap();

        let result = diff(&db1, &db2);
        assert!(!result.is_empty(), "round {round}: must find change");

        // The changed key must be in the diff
        let found = result.entries.iter().any(|e| e.key == key);
        assert!(found, "round {round}: changed key must appear in diff");

        apply(&db2, &result);

        // After apply, trees must be fully in sync
        let post_diff = diff(&db1, &db2);
        assert!(post_diff.is_empty(), "round {round}: must be in sync after apply");
    }

    assert_eq!(collect_all(&db1), collect_all(&db2));
}

// ============================================================
// Efficiency tests
// ============================================================

#[test]
fn many_small_changes_efficiency() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let val = [0xFF_u8; 64];
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..1000u32 {
            wtx.insert(&i.to_be_bytes(), &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Change only 3 entries out of 1000
    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(&100u32.to_be_bytes(), b"changed-a").unwrap();
    wtx.insert(&500u32.to_be_bytes(), b"changed-b").unwrap();
    wtx.insert(&900u32.to_be_bytes(), b"changed-c").unwrap();
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(result.subtrees_skipped > 0, "must skip matching subtrees");
    // Diff should return far fewer entries than the full dataset
    assert!(
        result.len() < 500,
        "expected efficient diff, got {} entries out of 1000",
        result.len()
    );

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn single_bit_value_change_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let val = [0x00_u8; 256];
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(&i.to_be_bytes(), &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Change a single bit in one value
    let mut modified_val = val;
    modified_val[128] = 0x01;
    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(&50u32.to_be_bytes(), &modified_val).unwrap();
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(!result.is_empty(), "single-bit change must be detected");
    assert!(
        result.entries.iter().any(|e| e.key == 50u32.to_be_bytes()),
        "changed key must be in diff"
    );

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

// ============================================================
// Convergence and stress
// ============================================================

#[test]
fn delete_and_reinsert_convergence() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Seed both
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), b"original").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Delete entries 20..30 from db1, then reinsert 22..28 with new values
    let mut wtx = db1.begin_write().unwrap();
    for i in 20..30u32 {
        wtx.delete(&i.to_be_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    let mut wtx = db1.begin_write().unwrap();
    for i in 22..28u32 {
        wtx.insert(&i.to_be_bytes(), b"reinserted").unwrap();
    }
    wtx.commit().unwrap();

    // Diff is one-directional: it collects entries from SOURCE (db1) in changed pages.
    // It does NOT propagate deletions (keys 20,21,28,29 still in db2 after apply).
    // Delete propagation requires CRDT tombstones.
    let result = diff(&db1, &db2);
    assert!(!result.is_empty());

    // The reinserted keys with new values must appear in the diff
    for i in 22..28u32 {
        let found = result.entries.iter().any(|e| e.key == i.to_be_bytes());
        assert!(found, "reinserted key {i} must be in diff");
    }

    apply(&db2, &result);

    // Verify reinserted keys have correct values in db2
    let db2_data = collect_all(&db2);
    for i in 22..28u32 {
        assert_eq!(db2_data[&i.to_be_bytes().to_vec()], b"reinserted");
    }

    // db2 still has keys 20,21,28,29 with "original" (deletions not propagated)
    // This is expected — delete propagation requires CRDT tombstones
    for i in [20u32, 21, 28, 29] {
        assert_eq!(db2_data[&i.to_be_bytes().to_vec()], b"original");
    }
}

#[test]
fn concurrent_reader_during_diff() {
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
    wtx.insert(b"extra-key", b"extra-value").unwrap();
    wtx.commit().unwrap();

    // Hold a reader on db2 while diffing
    let mut rtx = db2.begin_read();
    let result = diff(&db1, &db2);
    assert!(!result.is_empty());

    // Reader still works
    assert!(rtx.get(&0u32.to_be_bytes()).unwrap().is_some());
    drop(rtx);

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn all_entries_changed_full_diff() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(&i.to_be_bytes(), b"original").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Change every entry
    let mut wtx = db1.begin_write().unwrap();
    for i in 0..100u32 {
        wtx.insert(&i.to_be_bytes(), b"ALL-CHANGED").unwrap();
    }
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(!result.is_empty());
    // All entries should be captured (every leaf has changed)
    assert!(result.len() >= 100, "all entries must be in diff");

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn alternating_insert_delete_stress() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Build up data with alternating inserts and deletes
    let mut expected = BTreeMap::new();
    for round in 0..20u32 {
        let mut wtx = db1.begin_write().unwrap();
        // Insert 10 new keys
        for j in 0..10u32 {
            let key = (round * 100 + j).to_be_bytes();
            let val = format!("r{round}j{j}");
            wtx.insert(&key, val.as_bytes()).unwrap();
            expected.insert(key.to_vec(), val.into_bytes());
        }
        // Delete 3 from previous rounds if they exist
        if round > 0 {
            for j in 0..3u32 {
                let key = ((round - 1) * 100 + j).to_be_bytes();
                wtx.delete(&key).unwrap();
                expected.remove(&key.to_vec());
            }
        }
        wtx.commit().unwrap();
    }

    // db2 is empty — diff should capture all of db1
    let result = diff(&db1, &db2);
    assert!(!result.is_empty());

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
    assert_eq!(collect_all(&db1), expected);
}

#[test]
fn diff_symmetry_both_directions() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Seed both the same
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(&i.to_be_bytes(), b"same").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Modify different keys in each
    let mut wtx1 = db1.begin_write().unwrap();
    wtx1.insert(&10u32.to_be_bytes(), b"from-db1").unwrap();
    wtx1.commit().unwrap();

    let mut wtx2 = db2.begin_write().unwrap();
    wtx2.insert(&20u32.to_be_bytes(), b"from-db2").unwrap();
    wtx2.commit().unwrap();

    // Forward diff: db1 -> db2 (source=db1, target=db2)
    let forward = diff(&db1, &db2);
    assert!(!forward.is_empty());
    assert!(forward.entries.iter().any(|e| e.key == 10u32.to_be_bytes()));

    // Reverse diff: db2 -> db1 (source=db2, target=db1)
    let reverse = diff(&db2, &db1);
    assert!(!reverse.is_empty());
    assert!(reverse.entries.iter().any(|e| e.key == 20u32.to_be_bytes()));

    // Apply forward (db1→db2): db2 gets db1's version of the changed page
    apply(&db2, &forward);
    let db2_data = collect_all(&db2);
    assert_eq!(db2_data[&10u32.to_be_bytes().to_vec()], b"from-db1");

    // After forward apply, diff db1→db2 should be empty (db2 matches db1's source pages)
    let post = diff(&db1, &db2);
    assert!(post.is_empty(), "after forward apply, db1->db2 diff must be empty");
}

#[test]
fn large_values_multi_page_diff() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let big = vec![0xAA_u8; 1024];
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), &big).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Change a few large values
    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(&5u32.to_be_bytes(), &vec![0xBB_u8; 1024]).unwrap();
    wtx.insert(&25u32.to_be_bytes(), &vec![0xCC_u8; 1024]).unwrap();
    wtx.insert(&45u32.to_be_bytes(), &vec![0xDD_u8; 1024]).unwrap();
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(!result.is_empty());
    assert!(result.entries.iter().any(|e| e.key == 5u32.to_be_bytes()));
    assert!(result.entries.iter().any(|e| e.key == 25u32.to_be_bytes()));
    assert!(result.entries.iter().any(|e| e.key == 45u32.to_be_bytes()));

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn rapid_sync_cycles_50_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for round in 0..50u32 {
        let mut wtx = db1.begin_write().unwrap();
        let key = format!("key-{round}");
        let val = format!("val-{round}");
        wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        wtx.commit().unwrap();

        let result = diff(&db1, &db2);
        assert!(!result.is_empty(), "round {round}: should have changes");
        apply(&db2, &result);

        let verify = diff(&db1, &db2);
        assert!(verify.is_empty(), "round {round}: should be in sync");
    }

    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn disjoint_keys_no_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // db1 has only even keys, db2 has only odd keys
    let mut wtx1 = db1.begin_write().unwrap();
    for i in (0..100u32).step_by(2) {
        wtx1.insert(&i.to_be_bytes(), b"even").unwrap();
    }
    wtx1.commit().unwrap();

    let mut wtx2 = db2.begin_write().unwrap();
    for i in (1..100u32).step_by(2) {
        wtx2.insert(&i.to_be_bytes(), b"odd").unwrap();
    }
    wtx2.commit().unwrap();

    // Diff db1 -> db2: should find all even keys
    let result = diff(&db1, &db2);
    assert!(!result.is_empty());

    // Apply db1's entries to db2
    apply(&db2, &result);

    // Now db2 has all 100 keys (evens from db1 + odds from db2)
    let all = collect_all(&db2);
    assert_eq!(all.len(), 100);
    for i in (0..100u32).step_by(2) {
        assert_eq!(all[&i.to_be_bytes().to_vec()], b"even");
    }
    for i in (1..100u32).step_by(2) {
        assert_eq!(all[&i.to_be_bytes().to_vec()], b"odd");
    }
}

#[test]
fn diff_empty_after_full_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Different initial data
    let mut wtx1 = db1.begin_write().unwrap();
    for i in 0..100u32 {
        wtx1.insert(&i.to_be_bytes(), b"db1-original").unwrap();
    }
    wtx1.commit().unwrap();

    let mut wtx2 = db2.begin_write().unwrap();
    for i in 0..100u32 {
        wtx2.insert(&i.to_be_bytes(), b"db2-original").unwrap();
    }
    wtx2.commit().unwrap();

    // Overwrite db2 with db1's data
    let result = diff(&db1, &db2);
    apply(&db2, &result);

    // Now they should match
    assert_eq!(collect_all(&db1), collect_all(&db2));

    // Second diff should be empty
    let result2 = diff(&db1, &db2);
    assert!(result2.is_empty(), "second diff must be empty after full sync");
}

#[test]
fn mixed_key_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Both start identical with varied key sizes
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"a", b"short-key").unwrap();
        wtx.insert(b"medium-length-key", b"medium").unwrap();
        let long_key = vec![0x42_u8; 200];
        wtx.insert(&long_key, b"long-key").unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Change one of each size in db1
    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"a", b"SHORT-CHANGED").unwrap();
    wtx.insert(b"medium-length-key", b"MEDIUM-CHANGED").unwrap();
    let long_key = vec![0x42_u8; 200];
    wtx.insert(&long_key, b"LONG-CHANGED").unwrap();
    wtx.commit().unwrap();

    let result = diff(&db1, &db2);
    assert!(!result.is_empty());

    apply(&db2, &result);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}
