//! Torture tests for inline Merkle hashing.
//!
//! These tests go beyond basic property verification to deeply prove
//! that the Merkle tree implementation is correct:
//! - Independent hash recomputation from raw KV data
//! - Transaction boundary independence
//! - Split/merge propagation through tree levels
//! - Random operation stress with seeded RNG
//! - Cross-passphrase determinism
//! - Large-scale consistency

use std::collections::BTreeMap;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel::core::MERKLE_HASH_SIZE;

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"merkle-torture")
        .argon2_profile(Argon2Profile::Iot)
}

// ============================================================
// Independently compute expected Merkle root
// ============================================================

/// Compute the expected Merkle leaf hash from a sorted set of key-value pairs.
/// This mirrors the algorithm in merkle.rs but uses only public data.
fn expected_leaf_hash(entries: &BTreeMap<Vec<u8>, Vec<u8>>) -> [u8; MERKLE_HASH_SIZE] {
    let mut hasher = blake3::Hasher::new();
    for (key, value) in entries {
        hasher.update(&(key.len() as u16).to_le_bytes());
        hasher.update(key);
        // val_type for Inline = 0
        hasher.update(&[0u8]);
        hasher.update(&(value.len() as u32).to_le_bytes());
        hasher.update(value);
    }
    let hash = hasher.finalize();
    let mut out = [0u8; MERKLE_HASH_SIZE];
    out.copy_from_slice(&hash.as_bytes()[..MERKLE_HASH_SIZE]);
    out
}

// ============================================================
// Single-leaf tree verification
// ============================================================

#[test]
fn single_leaf_hash_matches() {
    // When all entries fit in one leaf page (no splits), the root hash
    // equals the leaf hash of all entries.
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut expected = BTreeMap::new();
    let mut wtx = db.begin_write().unwrap();
    for i in 0..5u32 {
        let key = format!("k{i:04}").into_bytes();
        let val = format!("v{i:04}").into_bytes();
        wtx.insert(&key, &val).unwrap();
        expected.insert(key, val);
    }
    wtx.commit().unwrap();

    let expected_hash = expected_leaf_hash(&expected);
    assert_eq!(
        db.stats().merkle_root, expected_hash,
        "single-leaf hash must match db merkle_root"
    );
}

#[test]
fn empty_db_hash_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let empty = BTreeMap::new();
    let expected_hash = expected_leaf_hash(&empty);
    assert_eq!(
        db.stats().merkle_root, expected_hash,
        "empty db hash must match"
    );
}

#[test]
fn delete_all_hash_matches_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let empty_hash = db.stats().merkle_root;

    let mut wtx = db.begin_write().unwrap();
    for i in 0..10u32 {
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..10u32 {
        wtx.delete(&i.to_be_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    assert_eq!(
        db.stats().merkle_root, empty_hash,
        "deleting all entries must restore empty db hash"
    );
}

#[test]
fn update_value_hash_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"only", b"first").unwrap();
    wtx.commit().unwrap();

    let mut expected = BTreeMap::new();
    expected.insert(b"only".to_vec(), b"first".to_vec());
    let h1 = expected_leaf_hash(&expected);
    assert_eq!(db.stats().merkle_root, h1);

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"only", b"second").unwrap();
    wtx.commit().unwrap();

    expected.insert(b"only".to_vec(), b"second".to_vec());
    let h2 = expected_leaf_hash(&expected);
    assert_eq!(db.stats().merkle_root, h2);
    assert_ne!(h1, h2);
}

// ============================================================
// Transaction boundary independence
// ============================================================

#[test]
fn single_txn_vs_many_txns_same_hash() {
    let dir = tempfile::tempdir().unwrap();

    // DB1: all entries in one transaction
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    {
        let mut wtx = db1.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), &(i * 7).to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // DB2: one entry per transaction
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();
    for i in 0..50u32 {
        let mut wtx = db2.begin_write().unwrap();
        wtx.insert(&i.to_be_bytes(), &(i * 7).to_le_bytes()).unwrap();
        wtx.commit().unwrap();
    }

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "transaction boundaries must not affect merkle root"
    );
}

#[test]
fn batch_sizes_dont_affect_hash() {
    let dir = tempfile::tempdir().unwrap();

    // DB1: batches of 10
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    for batch in 0..5 {
        let mut wtx = db1.begin_write().unwrap();
        for i in 0..10u32 {
            let key = (batch * 10 + i).to_be_bytes();
            wtx.insert(&key, b"data").unwrap();
        }
        wtx.commit().unwrap();
    }

    // DB2: batches of 25
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();
    for batch in 0..2 {
        let mut wtx = db2.begin_write().unwrap();
        for i in 0..25u32 {
            let key = (batch * 25 + i).to_be_bytes();
            wtx.insert(&key, b"data").unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "batch sizes must not affect merkle root"
    );
}

// ============================================================
// Split propagation — force tree structure changes
// ============================================================

#[test]
fn many_inserts_force_splits_hash_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut prev_hash = db.stats().merkle_root;
    let mut all_hashes = std::collections::HashSet::new();
    all_hashes.insert(prev_hash);

    // Insert enough to force multiple leaf splits and at least one branch split
    for i in 0..500u32 {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(&i.to_be_bytes(), &[0xAA; 64]).unwrap();
        wtx.commit().unwrap();

        let h = db.stats().merkle_root;
        assert_ne!(h, prev_hash, "insert {i} must change hash");
        assert!(all_hashes.insert(h), "insert {i} must produce unique hash");
        prev_hash = h;
    }

    // Verify depth increased (splits happened)
    assert!(
        db.stats().tree_depth >= 2,
        "500 inserts should create a multi-level tree, got depth {}",
        db.stats().tree_depth
    );

    // Integrity check must still pass
    let report = db.integrity_check().unwrap();
    assert!(report.is_ok(), "integrity check after splits: {:?}", report.errors);
}

#[test]
fn split_same_order_different_txn_granularity() {
    // Same insertion order with different transaction boundaries must produce
    // identical tree structure and thus identical Merkle hash — even when
    // the dataset forces multiple leaf splits.
    // Note: different insertion ORDERS can produce different split points,
    // which is correct — the Merkle hash reflects physical page structure.
    let dir = tempfile::tempdir().unwrap();

    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // DB1: all 500 in one txn
    let mut wtx = db1.begin_write().unwrap();
    for i in 0..500u32 {
        wtx.insert(&i.to_be_bytes(), &(i * 3).to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    // DB2: 500 in batches of 25
    for batch in 0..20u32 {
        let mut wtx = db2.begin_write().unwrap();
        for i in 0..25u32 {
            let key = (batch * 25 + i).to_be_bytes();
            wtx.insert(&key, &((batch * 25 + i) * 3).to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "same order, different txn boundaries must produce same hash after splits"
    );

    // Verify both have multi-level trees (splits happened)
    assert!(db1.stats().tree_depth >= 2, "db1 depth must be >= 2");
    assert!(db2.stats().tree_depth >= 2, "db2 depth must be >= 2");
}

#[test]
fn delete_half_then_reinsert_restores_hash() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    // Insert 200 entries
    let mut wtx = db.begin_write().unwrap();
    for i in 0..200u32 {
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();
    let full_hash = db.stats().merkle_root;

    // Delete even entries
    let mut wtx = db.begin_write().unwrap();
    for i in (0..200u32).step_by(2) {
        wtx.delete(&i.to_be_bytes()).unwrap();
    }
    wtx.commit().unwrap();
    let half_hash = db.stats().merkle_root;
    assert_ne!(full_hash, half_hash);

    // Reinsert even entries
    let mut wtx = db.begin_write().unwrap();
    for i in (0..200u32).step_by(2) {
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();
    let restored_hash = db.stats().merkle_root;

    assert_eq!(full_hash, restored_hash, "reinserting deleted entries must restore hash");
}

// ============================================================
// Random operation stress with seeded RNG
// ============================================================

/// Simple deterministic PRNG (xorshift32)
struct Rng(u32);

impl Rng {
    fn new(seed: u32) -> Self {
        Self(seed)
    }
    fn next(&mut self) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 17;
        self.0 ^= self.0 << 5;
        self.0
    }
    fn next_range(&mut self, max: u32) -> u32 {
        self.next() % max
    }
}

#[test]
fn random_ops_maintain_consistency() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut rng = Rng::new(12345);
    let mut expected = BTreeMap::new();
    let mut prev_hash = db.stats().merkle_root;

    for _ in 0..300 {
        let mut wtx = db.begin_write().unwrap();
        let ops = rng.next_range(5) + 1;
        let mut changed = false;

        for _ in 0..ops {
            let key = rng.next_range(100).to_be_bytes().to_vec();
            match rng.next_range(3) {
                0 | 1 => {
                    // Insert/update
                    let val = rng.next().to_le_bytes().to_vec();
                    let old = expected.insert(key.clone(), val.clone());
                    if old.as_ref() != Some(&val) {
                        changed = true;
                    }
                    wtx.insert(&key, &val).unwrap();
                }
                _ => {
                    // Delete
                    if expected.remove(&key).is_some() {
                        changed = true;
                    }
                    let _ = wtx.delete(&key);
                }
            }
        }
        wtx.commit().unwrap();

        let h = db.stats().merkle_root;
        if changed {
            assert_ne!(h, prev_hash, "data changed but hash stayed same");
        }
        prev_hash = h;
    }

    // Final consistency: verify all data matches expected
    let mut rtx = db.begin_read();
    let mut db_entries = BTreeMap::new();
    rtx.for_each(|k, v| {
        db_entries.insert(k.to_vec(), v.to_vec());
        Ok(())
    }).unwrap();

    assert_eq!(expected, db_entries, "expected and db must match after random ops");
}

#[test]
fn random_ops_two_dbs_converge() {
    // Two DBs with different random op sequences that reach same final state
    let dir = tempfile::tempdir().unwrap();

    // DB1: insert 0..50, then random updates
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    {
        let mut wtx = db1.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }
    // Update some values
    {
        let mut wtx = db1.begin_write().unwrap();
        for i in (0..50u32).step_by(3) {
            wtx.insert(&i.to_be_bytes(), &(i * 100).to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // DB2: insert in reverse, same updates
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();
    {
        let mut wtx = db2.begin_write().unwrap();
        for i in (0..50u32).rev() {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = db2.begin_write().unwrap();
        for i in (0..50u32).step_by(3) {
            wtx.insert(&i.to_be_bytes(), &(i * 100).to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "two DBs reaching same state must have same hash"
    );
}

// ============================================================
// Cross-passphrase determinism
// ============================================================

#[test]
fn different_passphrase_same_merkle_root() {
    let dir = tempfile::tempdir().unwrap();

    let db1 = DatabaseBuilder::new(dir.path().join("a.db"))
        .passphrase(b"password-alpha")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();

    let db2 = DatabaseBuilder::new(dir.path().join("b.db"))
        .passphrase(b"password-beta-totally-different")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(&i.to_be_bytes(), &(i * 7).to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "merkle hash must be passphrase-independent (computed on plaintext)"
    );
}

// ============================================================
// Persistence and reopen consistency
// ============================================================

#[test]
fn merkle_root_survives_many_reopen_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let mut hashes = Vec::new();

    for round in 0..5 {
        {
            let db = if round == 0 {
                fast_builder(&db_path).create().unwrap()
            } else {
                fast_builder(&db_path).open().unwrap()
            };

            // Verify previous hash persisted
            if !hashes.is_empty() {
                assert_eq!(db.stats().merkle_root, *hashes.last().unwrap());
            }

            let mut wtx = db.begin_write().unwrap();
            for i in 0..10u32 {
                let key = ((round * 10 + i) as u32).to_be_bytes();
                wtx.insert(&key, b"round-data").unwrap();
            }
            wtx.commit().unwrap();
            hashes.push(db.stats().merkle_root);
        }
    }

    // All hashes should be unique
    let unique: std::collections::HashSet<_> = hashes.iter().collect();
    assert_eq!(unique.len(), hashes.len(), "each round must produce unique hash");
}

// ============================================================
// Backup and compact under stress
// ============================================================

#[test]
fn backup_preserves_hash_after_heavy_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    // Heavy writes with deletes to create fragmentation
    let mut wtx = db.begin_write().unwrap();
    for i in 0..300u32 {
        wtx.insert(&i.to_be_bytes(), &[0xBB; 128]).unwrap();
    }
    wtx.commit().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in (0..300u32).step_by(3) {
        wtx.delete(&i.to_be_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    let original_hash = db.stats().merkle_root;

    let backup_path = dir.path().join("backup.db");
    db.backup(&backup_path).unwrap();
    drop(db);

    let backup_db = fast_builder(&backup_path).open().unwrap();
    assert_eq!(
        backup_db.stats().merkle_root,
        original_hash,
        "backup must preserve hash after fragmentation"
    );
}

#[test]
fn compact_preserves_hash_after_heavy_churn() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    // Create and delete a lot to fragment the file
    for round in 0..5 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            let key = (round * 100 + i).to_be_bytes();
            wtx.insert(&key, &[round as u8; 64]).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut wtx = db.begin_write().unwrap();
    for i in 0..250u32 {
        wtx.delete(&i.to_be_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    let pre_compact = db.stats().merkle_root;

    let compact_path = dir.path().join("compact.db");
    db.compact(&compact_path).unwrap();
    drop(db);

    let compact_db = fast_builder(&compact_path).open().unwrap();
    assert_eq!(
        compact_db.stats().merkle_root,
        pre_compact,
        "compact must preserve hash after heavy churn"
    );
}

#[test]
fn compact_then_more_writes_still_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..100u32 {
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    // Compact
    let compact_path = dir.path().join("compact.db");
    db.compact(&compact_path).unwrap();
    drop(db);

    // Open compacted DB, add more data
    let db = fast_builder(&compact_path).open().unwrap();
    let mut wtx = db.begin_write().unwrap();
    for i in 100..150u32 {
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    // Build a fresh DB with same final state
    let fresh_path = dir.path().join("fresh.db");
    let fresh_db = fast_builder(&fresh_path).create().unwrap();
    let mut wtx = fresh_db.begin_write().unwrap();
    for i in 0..150u32 {
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    assert_eq!(
        db.stats().merkle_root,
        fresh_db.stats().merkle_root,
        "compacted + extended DB must match fresh DB with same data"
    );
}

// ============================================================
// Edge cases
// ============================================================

#[test]
fn many_single_byte_keys_unique_hashes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut seen = std::collections::HashSet::new();
    seen.insert(db.stats().merkle_root);

    for b in 0u8..=255 {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(&[b], &[b]).unwrap();
        wtx.commit().unwrap();
        assert!(seen.insert(db.stats().merkle_root), "byte {b} must produce unique hash");
    }
}

#[test]
fn key_value_length_boundary() {
    let dir = tempfile::tempdir().unwrap();

    // Keys of different lengths with same prefix must produce different hashes
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"key", b"value").unwrap();
    wtx.commit().unwrap();

    let mut wtx = db2.begin_write().unwrap();
    wtx.insert(b"ke", b"yvalue").unwrap();
    wtx.commit().unwrap();

    assert_ne!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "different key/value split must produce different hash"
    );
}

#[test]
fn large_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    // Values near max inline size
    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"big", &[0xCC; 1900]).unwrap();
    wtx.commit().unwrap();
    let h1 = db.stats().merkle_root;

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"big", &[0xDD; 1900]).unwrap();
    wtx.commit().unwrap();
    let h2 = db.stats().merkle_root;

    assert_ne!(h1, h2, "different large values must produce different hashes");
}

#[test]
fn null_bytes_in_keys_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"k\x00ey", b"v\x00al").unwrap();
    wtx.commit().unwrap();

    let mut wtx = db2.begin_write().unwrap();
    wtx.insert(b"k\x00ey", b"v\x00al").unwrap();
    wtx.commit().unwrap();

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "null bytes in keys/values must hash correctly"
    );
}

#[test]
fn empty_key_and_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"", b"").unwrap();
    wtx.commit().unwrap();

    let h = db.stats().merkle_root;
    let empty_db_hash = {
        let db2 = fast_builder(&dir.path().join("empty.db")).create().unwrap();
        db2.stats().merkle_root
    };

    assert_ne!(h, empty_db_hash, "empty-key entry must differ from no entries");
}

// ============================================================
// Interleaved insert/delete cycles
// ============================================================

#[test]
fn insert_delete_interleave_convergence() {
    let dir = tempfile::tempdir().unwrap();

    // DB1: insert 0..100, delete 50..100
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    {
        let mut wtx = db1.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = db1.begin_write().unwrap();
        for i in 50..100u32 {
            wtx.delete(&i.to_be_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // DB2: insert only 0..50 (same final state)
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();
    {
        let mut wtx = db2.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "insert+delete must converge to same hash as direct insert"
    );
}

#[test]
fn overwrite_all_values_then_compare() {
    let dir = tempfile::tempdir().unwrap();

    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // DB1: insert with old values, then overwrite
    {
        let mut wtx = db1.begin_write().unwrap();
        for i in 0..80u32 {
            wtx.insert(&i.to_be_bytes(), b"old").unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = db1.begin_write().unwrap();
        for i in 0..80u32 {
            wtx.insert(&i.to_be_bytes(), b"new").unwrap();
        }
        wtx.commit().unwrap();
    }

    // DB2: insert with final values directly
    {
        let mut wtx = db2.begin_write().unwrap();
        for i in 0..80u32 {
            wtx.insert(&i.to_be_bytes(), b"new").unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "overwritten values must produce same hash as direct insert"
    );
}

// ============================================================
// Scale: large dataset consistency
// ============================================================

#[test]
fn large_dataset_integrity_and_determinism() {
    let dir = tempfile::tempdir().unwrap();

    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Insert 1000 entries in different batch sizes
    for batch_start in (0..1000u32).step_by(50) {
        let mut wtx = db1.begin_write().unwrap();
        for i in batch_start..batch_start + 50 {
            let key = format!("key-{i:06}").into_bytes();
            let val = format!("value-{i:06}-padded-data").into_bytes();
            wtx.insert(&key, &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Same data in single transaction
    {
        let mut wtx = db2.begin_write().unwrap();
        for i in 0..1000u32 {
            let key = format!("key-{i:06}").into_bytes();
            let val = format!("value-{i:06}-padded-data").into_bytes();
            wtx.insert(&key, &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "1000 entries: batch vs single txn must produce same hash"
    );

    // Integrity check must pass
    let r1 = db1.integrity_check().unwrap();
    let r2 = db2.integrity_check().unwrap();
    assert!(r1.is_ok(), "db1 integrity: {:?}", r1.errors);
    assert!(r2.is_ok(), "db2 integrity: {:?}", r2.errors);

    // Compact both and compare
    let c1_path = dir.path().join("c1.db");
    let c2_path = dir.path().join("c2.db");
    db1.compact(&c1_path).unwrap();
    db2.compact(&c2_path).unwrap();

    let c1 = fast_builder(&c1_path).open().unwrap();
    let c2 = fast_builder(&c2_path).open().unwrap();
    assert_eq!(
        c1.stats().merkle_root,
        c2.stats().merkle_root,
        "compacted DBs must have same hash"
    );
}

// ============================================================
// Multiple aborts followed by commit
// ============================================================

#[test]
fn multiple_aborts_then_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let initial = db.stats().merkle_root;

    // Several aborted transactions
    for _ in 0..5 {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"aborted", b"data").unwrap();
        wtx.abort();
    }

    assert_eq!(db.stats().merkle_root, initial, "aborts must not change hash");

    // Now a real commit
    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"committed", b"data").unwrap();
    wtx.commit().unwrap();

    assert_ne!(db.stats().merkle_root, initial, "commit after aborts must change hash");
}

// ============================================================
// Concurrent readers see consistent hash
// ============================================================

#[test]
fn reader_sees_consistent_hash_during_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..50u32 {
        wtx.insert(&i.to_be_bytes(), b"first").unwrap();
    }
    wtx.commit().unwrap();
    let snapshot_hash = db.stats().merkle_root;

    // Reader takes snapshot before write
    let mut rtx = db.begin_read();

    // Writer modifies data
    let mut wtx = db.begin_write().unwrap();
    for i in 0..50u32 {
        wtx.insert(&i.to_be_bytes(), b"second").unwrap();
    }
    wtx.commit().unwrap();

    // Reader should still see old data
    let mut count = 0u32;
    rtx.for_each(|_, v| {
        assert_eq!(v, b"first", "reader must see snapshot data");
        count += 1;
        Ok(())
    }).unwrap();
    assert_eq!(count, 50);

    // DB stats reflect the new write
    assert_ne!(db.stats().merkle_root, snapshot_hash, "new write must change hash");
    drop(rtx);
}

// ============================================================
// Named tables isolation
// ============================================================

#[test]
fn named_table_churn_does_not_affect_default_hash() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"default-key", b"val").unwrap();
    wtx.commit().unwrap();
    let h_before = db.stats().merkle_root;

    // Heavy named table operations
    for round in 0..5u32 {
        let name = format!("table_{round}");
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(name.as_bytes()).unwrap();
        for i in 0..50u32 {
            wtx.table_insert(name.as_bytes(), &i.to_be_bytes(), &[round as u8; 32]).unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(
        db.stats().merkle_root, h_before,
        "named table operations must not affect default tree merkle root"
    );

    // Delete named tables
    for round in 0..5u32 {
        let name = format!("table_{round}");
        let mut wtx = db.begin_write().unwrap();
        wtx.drop_table(name.as_bytes()).unwrap();
        wtx.commit().unwrap();
    }

    assert_eq!(
        db.stats().merkle_root, h_before,
        "dropping named tables must not affect default tree merkle root"
    );
}

// ============================================================
// Stress: rapid insert-delete cycles
// ============================================================

#[test]
fn rapid_insert_delete_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let empty_hash = db.stats().merkle_root;

    // 50 cycles: insert 20 keys, delete all 20 keys
    for cycle in 0..50 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..20u32 {
            let key = format!("cycle{cycle:03}-key{i:03}").into_bytes();
            wtx.insert(&key, b"ephemeral").unwrap();
        }
        wtx.commit().unwrap();

        assert_ne!(db.stats().merkle_root, empty_hash, "cycle {cycle}: non-empty must differ");

        let mut wtx = db.begin_write().unwrap();
        for i in 0..20u32 {
            let key = format!("cycle{cycle:03}-key{i:03}").into_bytes();
            wtx.delete(&key).unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(
        db.stats().merkle_root, empty_hash,
        "after all insert-delete cycles, must return to empty hash"
    );
}

// ============================================================
// Integrity check across all operations
// ============================================================

#[test]
fn integrity_check_after_complex_workload() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    // Phase 1: bulk insert
    let mut wtx = db.begin_write().unwrap();
    for i in 0..500u32 {
        wtx.insert(&i.to_be_bytes(), &format!("value-{i}").into_bytes()).unwrap();
    }
    wtx.commit().unwrap();
    let r = db.integrity_check().unwrap();
    assert!(r.is_ok(), "after bulk insert: {:?}", r.errors);

    // Phase 2: delete half
    let mut wtx = db.begin_write().unwrap();
    for i in (0..500u32).step_by(2) {
        wtx.delete(&i.to_be_bytes()).unwrap();
    }
    wtx.commit().unwrap();
    let r = db.integrity_check().unwrap();
    assert!(r.is_ok(), "after bulk delete: {:?}", r.errors);

    // Phase 3: update remaining
    let mut wtx = db.begin_write().unwrap();
    for i in (1..500u32).step_by(2) {
        wtx.insert(&i.to_be_bytes(), b"updated").unwrap();
    }
    wtx.commit().unwrap();
    let r = db.integrity_check().unwrap();
    assert!(r.is_ok(), "after bulk update: {:?}", r.errors);

    // Phase 4: reinsert deleted
    let mut wtx = db.begin_write().unwrap();
    for i in (0..500u32).step_by(2) {
        wtx.insert(&i.to_be_bytes(), b"reinserted").unwrap();
    }
    wtx.commit().unwrap();
    let r = db.integrity_check().unwrap();
    assert!(r.is_ok(), "after reinsert: {:?}", r.errors);
}

// ============================================================
// Hash sensitivity: single bit changes
// ============================================================

#[test]
fn single_bit_value_difference() {
    let dir = tempfile::tempdir().unwrap();

    let mut hashes = Vec::new();
    for bit in 0..8u8 {
        let db = fast_builder(&dir.path().join(format!("bit{bit}.db"))).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        let val = [1u8 << bit];
        wtx.insert(b"key", &val).unwrap();
        wtx.commit().unwrap();
        hashes.push(db.stats().merkle_root);
    }

    // All hashes must be unique
    let unique: std::collections::HashSet<_> = hashes.iter().collect();
    assert_eq!(unique.len(), 8, "each single-bit value must produce unique hash");
}

#[test]
fn single_bit_key_difference() {
    let dir = tempfile::tempdir().unwrap();

    let mut hashes = Vec::new();
    for bit in 0..8u8 {
        let db = fast_builder(&dir.path().join(format!("kbit{bit}.db"))).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        let key = [1u8 << bit];
        wtx.insert(&key, b"same-value").unwrap();
        wtx.commit().unwrap();
        hashes.push(db.stats().merkle_root);
    }

    let unique: std::collections::HashSet<_> = hashes.iter().collect();
    assert_eq!(unique.len(), 8, "each single-bit key must produce unique hash");
}
