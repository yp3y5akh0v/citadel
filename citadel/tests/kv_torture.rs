//! KV store torture tests (Phases 1-4): edge cases, stress, and correctness
//! verification for the public Database API covering encryption, buffer pool,
//! B+ tree, transactions, named tables, backup, compact, and integrity.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;

use citadel::{Argon2Profile, DatabaseBuilder, Error};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"torture-test")
        .argon2_profile(Argon2Profile::Iot)
}

fn fast_builder_cache(path: &std::path::Path, cache: usize) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"torture-test")
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(cache)
}

// ============================================================
// Key / Value boundary tests
// ============================================================

#[test]
fn empty_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"").unwrap();
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"".to_vec()));
}

#[test]
fn single_byte_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for b in 0..=255u8 {
        wtx.insert(&[b], &[b]).unwrap();
    }
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 256);
    for b in 0..=255u8 {
        assert_eq!(rtx.get(&[b]).unwrap(), Some(vec![b]), "byte {b:#04x}");
    }
}

#[test]
fn binary_key_with_null_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let keys: Vec<Vec<u8>> = vec![
        vec![0x00],
        vec![0x00, 0x00],
        vec![0x00, 0x01],
        vec![0x01, 0x00],
        vec![0x00, 0x00, 0x00],
        vec![0xFF, 0x00, 0xFF],
    ];

    let mut wtx = db.begin_write().unwrap();
    for (i, key) in keys.iter().enumerate() {
        wtx.insert(key, format!("v{i}").as_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), keys.len() as u64);
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            rtx.get(key).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "key {key:?}"
        );
    }
}

#[test]
fn large_key_near_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let key_2048 = vec![0xAA; 2048];
    let key_2047 = vec![0xBB; 2047];

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(&key_2048, b"max-key").unwrap();
    wtx.insert(&key_2047, b"near-max").unwrap();
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(&key_2048).unwrap(), Some(b"max-key".to_vec()));
    assert_eq!(rtx.get(&key_2047).unwrap(), Some(b"near-max".to_vec()));
}

#[test]
fn large_key_over_limit_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let key_2049 = vec![0xCC; 2049];
    let mut wtx = db.begin_write().unwrap();
    let result = wtx.insert(&key_2049, b"too-big");
    assert!(result.is_err(), "key > 2048 bytes should be rejected");
}

#[test]
fn large_value_near_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let val_1920 = vec![0xDD; 1920];
    let val_1919 = vec![0xEE; 1919];

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"max-val", &val_1920).unwrap();
    wtx.insert(b"near-max-val", &val_1919).unwrap();
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"max-val").unwrap(), Some(val_1920));
    assert_eq!(rtx.get(b"near-max-val").unwrap(), Some(val_1919));
}

#[test]
fn prefix_keys_distinguished() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"abc", b"1").unwrap();
    wtx.insert(b"abcd", b"2").unwrap();
    wtx.insert(b"abcde", b"3").unwrap();
    wtx.insert(b"ab", b"4").unwrap();
    wtx.insert(b"a", b"5").unwrap();
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 5);
    assert_eq!(rtx.get(b"a").unwrap(), Some(b"5".to_vec()));
    assert_eq!(rtx.get(b"ab").unwrap(), Some(b"4".to_vec()));
    assert_eq!(rtx.get(b"abc").unwrap(), Some(b"1".to_vec()));
    assert_eq!(rtx.get(b"abcd").unwrap(), Some(b"2".to_vec()));
    assert_eq!(rtx.get(b"abcde").unwrap(), Some(b"3".to_vec()));
}

// ============================================================
// Insert / Delete / Reinsert cycles
// ============================================================

#[test]
fn insert_delete_reinsert_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    for round in 0..50u32 {
        let mut wtx = db.begin_write().unwrap();
        let val = format!("round-{round}");
        wtx.insert(b"cycle-key", val.as_bytes()).unwrap();
        wtx.commit().unwrap();

        let mut wtx = db.begin_write().unwrap();
        wtx.delete(b"cycle-key").unwrap();
        wtx.commit().unwrap();
    }

    // Final insert
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"cycle-key", b"final").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"cycle-key").unwrap(), Some(b"final".to_vec()));
    assert_eq!(rtx.entry_count(), 1);
}

#[test]
fn insert_delete_reinsert_same_key_single_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"key", b"v1").unwrap();
    assert_eq!(wtx.get(b"key").unwrap(), Some(b"v1".to_vec()));

    wtx.delete(b"key").unwrap();
    assert_eq!(wtx.get(b"key").unwrap(), None);

    wtx.insert(b"key", b"v2").unwrap();
    assert_eq!(wtx.get(b"key").unwrap(), Some(b"v2".to_vec()));

    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(rtx.entry_count(), 1);
}

#[test]
fn delete_nonexistent_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    let existed = wtx.delete(b"does-not-exist").unwrap();
    assert!(!existed);
    wtx.commit().unwrap();
}

#[test]
fn double_delete_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"key", b"val").unwrap();
        wtx.commit().unwrap();
    }

    {
        let mut wtx = db.begin_write().unwrap();
        let existed = wtx.delete(b"key").unwrap();
        assert!(existed);
        let existed2 = wtx.delete(b"key").unwrap();
        assert!(!existed2);
        wtx.commit().unwrap();
    }

    let rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 0);
}

#[test]
fn insert_delete_half_verify_remaining() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();
    let count = 500u32;

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..count {
            wtx.insert(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    {
        let mut wtx = db.begin_write().unwrap();
        for i in (0..count).step_by(2) {
            wtx.delete(format!("k{i:04}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), count as u64 / 2);
    for i in (1..count).step_by(2) {
        let key = format!("k{i:04}");
        let val = format!("v{i:04}");
        assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(val.into_bytes()), "key {key}");
    }
    for i in (0..count).step_by(2) {
        let key = format!("k{i:04}");
        assert_eq!(rtx.get(key.as_bytes()).unwrap(), None, "deleted key {key}");
    }
}

// ============================================================
// Multiple reopen cycles
// ============================================================

#[test]
fn five_reopen_cycles_accumulate() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    for session in 0..5u32 {
        let db = if session == 0 {
            fast_builder(&db_path).create().unwrap()
        } else {
            fast_builder(&db_path).open().unwrap()
        };

        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("s{session}-k{i:03}");
            wtx.insert(key.as_bytes(), b"v").unwrap();
        }
        wtx.commit().unwrap();
    }

    let db = fast_builder(&db_path).open().unwrap();
    assert_eq!(db.stats().entry_count, 500);

    let mut rtx = db.begin_read();
    for session in 0..5u32 {
        for i in 0..100u32 {
            let key = format!("s{session}-k{i:03}");
            assert!(rtx.get(key.as_bytes()).unwrap().is_some(), "key {key}");
        }
    }
}

#[test]
fn reopen_with_delete_and_reinsert() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    // Session 1: insert
    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            wtx.insert(format!("k{i:04}").as_bytes(), b"original").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Session 2: delete half, reinsert with different values
    {
        let db = fast_builder(&db_path).open().unwrap();
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.delete(format!("k{i:04}").as_bytes()).unwrap();
        }
        for i in 0..100u32 {
            wtx.insert(format!("k{i:04}").as_bytes(), b"reinserted").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Session 3: verify
    {
        let db = fast_builder(&db_path).open().unwrap();
        assert_eq!(db.stats().entry_count, 200);
        let mut rtx = db.begin_read();
        for i in 0..100u32 {
            let key = format!("k{i:04}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(b"reinserted".to_vec()), "key {key}");
        }
        for i in 100..200u32 {
            let key = format!("k{i:04}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(b"original".to_vec()), "key {key}");
        }
    }
}

// ============================================================
// Named table edge cases
// ============================================================

#[test]
fn table_and_default_same_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"t").unwrap();
    wtx.insert(b"key", b"default-val").unwrap();
    wtx.table_insert(b"t", b"key", b"table-val").unwrap();
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"key").unwrap(), Some(b"default-val".to_vec()));
    assert_eq!(rtx.table_get(b"t", b"key").unwrap(), Some(b"table-val".to_vec()));
}

#[test]
fn abort_after_create_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"aborted_table").unwrap();
        wtx.table_insert(b"aborted_table", b"k", b"v").unwrap();
        wtx.abort();
    }

    let mut rtx = db.begin_read();
    assert!(matches!(
        rtx.table_get(b"aborted_table", b"k"),
        Err(Error::TableNotFound(_))
    ));
}

#[test]
fn create_drop_recreate_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    for round in 0..20u32 {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"ephemeral").unwrap();
        let val = format!("round-{round}");
        wtx.table_insert(b"ephemeral", b"key", val.as_bytes()).unwrap();
        wtx.commit().unwrap();

        let mut rtx = db.begin_read();
        assert_eq!(
            rtx.table_get(b"ephemeral", b"key").unwrap(),
            Some(val.into_bytes())
        );
        drop(rtx);

        let mut wtx = db.begin_write().unwrap();
        wtx.drop_table(b"ephemeral").unwrap();
        wtx.commit().unwrap();
    }
}

#[test]
fn many_tables_create_drop_subset() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    // Create 30 tables
    {
        let mut wtx = db.begin_write().unwrap();
        for t in 0..30u32 {
            let name = format!("t{t:03}");
            wtx.create_table(name.as_bytes()).unwrap();
            wtx.table_insert(name.as_bytes(), b"k", format!("v{t}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Drop even-numbered tables
    {
        let mut wtx = db.begin_write().unwrap();
        for t in (0..30u32).step_by(2) {
            let name = format!("t{t:03}");
            wtx.drop_table(name.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Verify odd tables survive, even tables gone
    let mut rtx = db.begin_read();
    for t in (1..30u32).step_by(2) {
        let name = format!("t{t:03}");
        assert_eq!(
            rtx.table_get(name.as_bytes(), b"k").unwrap(),
            Some(format!("v{t}").into_bytes()),
            "table {name}"
        );
    }
    for t in (0..30u32).step_by(2) {
        let name = format!("t{t:03}");
        assert!(matches!(
            rtx.table_get(name.as_bytes(), b"k"),
            Err(Error::TableNotFound(_))
        ), "dropped table {name}");
    }
}

#[test]
fn table_for_each_correctness() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"scan").unwrap();
        for i in 0..100u32 {
            let key = format!("k{i:04}");
            let val = format!("v{i:04}");
            wtx.table_insert(b"scan", key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    let mut entries = Vec::new();
    rtx.table_for_each(b"scan", |k, v| {
        entries.push((k.to_vec(), v.to_vec()));
        Ok(())
    }).unwrap();

    assert_eq!(entries.len(), 100);
    // Verify sorted order
    for i in 1..entries.len() {
        assert!(entries[i].0 > entries[i - 1].0);
    }
}

#[test]
fn named_table_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"persist").unwrap();
        for i in 0..500u32 {
            wtx.table_insert(b"persist", format!("k{i:04}").as_bytes(), b"v").unwrap();
        }
        wtx.insert(b"default-key", b"default-val").unwrap();
        wtx.commit().unwrap();
    }

    {
        let db = fast_builder(&db_path).open().unwrap();
        let mut rtx = db.begin_read();
        assert_eq!(rtx.get(b"default-key").unwrap(), Some(b"default-val".to_vec()));
        for i in 0..500u32 {
            let key = format!("k{i:04}");
            assert!(
                rtx.table_get(b"persist", key.as_bytes()).unwrap().is_some(),
                "key {key}"
            );
        }
    }
}

// ============================================================
// Concurrent readers (thread safety)
// ============================================================

#[test]
fn concurrent_readers_threaded() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let count = 1000u32;

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        for i in 0..count {
            wtx.insert(format!("k{i:05}").as_bytes(), format!("v{i:05}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let db = Arc::new(fast_builder(&db_path).open().unwrap());
    let mut handles = Vec::new();

    for thread_id in 0..4u32 {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            let mut rtx = db_clone.begin_read();
            assert_eq!(rtx.entry_count(), count as u64);
            let start = thread_id * 250;
            let end = start + 250;
            for i in start..end {
                let key = format!("k{i:05}");
                let val = format!("v{i:05}");
                assert_eq!(
                    rtx.get(key.as_bytes()).unwrap(),
                    Some(val.into_bytes()),
                    "thread {thread_id}, key {key}"
                );
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn reader_while_writer_threaded() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(format!("k{i:03}").as_bytes(), b"initial").unwrap();
        }
        wtx.commit().unwrap();
    }

    let db = Arc::new(fast_builder(&db_path).open().unwrap());

    let db_reader = db.clone();
    let reader = thread::spawn(move || {
        let mut rtx = db_reader.begin_read();
        assert_eq!(rtx.entry_count(), 100);
        for i in 0..100u32 {
            let key = format!("k{i:03}");
            assert!(rtx.get(key.as_bytes()).unwrap().is_some());
        }
    });

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 100..200u32 {
            wtx.insert(format!("k{i:03}").as_bytes(), b"new").unwrap();
        }
        wtx.commit().unwrap();
    }

    reader.join().unwrap();
}

// ============================================================
// Small cache (force eviction under real workload)
// ============================================================

#[test]
fn small_cache_forces_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder_cache(&dir.path().join("test.db"), 16).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            wtx.insert(format!("k{i:04}").as_bytes(), b"value-data").unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    for i in 0..500u32 {
        let key = format!("k{i:04}");
        assert!(rtx.get(key.as_bytes()).unwrap().is_some(), "key {key}");
    }
}

#[test]
fn small_cache_multi_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder_cache(&dir.path().join("test.db"), 8).create().unwrap();

    for batch in 0..20u32 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            let key = format!("b{batch:02}-k{i:02}");
            wtx.insert(key.as_bytes(), b"v").unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 1000);
    assert_eq!(rtx.get(b"b00-k00").unwrap(), Some(b"v".to_vec()));
    assert_eq!(rtx.get(b"b19-k49").unwrap(), Some(b"v".to_vec()));
}

// ============================================================
// for_each edge cases through public API
// ============================================================

#[test]
fn for_each_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut rtx = db.begin_read();
    let mut count = 0u32;
    rtx.for_each(|_, _| { count += 1; Ok(()) }).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn for_each_sorted_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    // Insert in reverse order
    for i in (0..200u32).rev() {
        let key = format!("k{i:04}");
        wtx.insert(key.as_bytes(), b"v").unwrap();
    }
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    let mut prev: Option<Vec<u8>> = None;
    let mut count = 0u32;
    rtx.for_each(|k, _| {
        if let Some(p) = &prev {
            assert!(k > p.as_slice(), "keys not sorted: {:?} > {:?}", p, k);
        }
        prev = Some(k.to_vec());
        count += 1;
        Ok(())
    }).unwrap();
    assert_eq!(count, 200);
}

#[test]
fn for_each_after_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(format!("k{i:03}").as_bytes(), b"v").unwrap();
        }
        wtx.commit().unwrap();
    }

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.delete(format!("k{i:03}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    let mut entries = Vec::new();
    rtx.for_each(|k, _| { entries.push(k.to_vec()); Ok(()) }).unwrap();
    assert_eq!(entries.len(), 50);
}

// ============================================================
// Snapshot isolation edge cases
// ============================================================

#[test]
fn snapshot_sees_consistent_state_during_heavy_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            wtx.insert(format!("k{i:04}").as_bytes(), b"v1").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Take snapshot
    let mut rtx = db.begin_read();
    let snapshot_count = rtx.entry_count();

    // Heavy modifications
    for round in 0..10u32 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            let key = format!("k{i:04}");
            let val = format!("round{round}");
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Old reader still sees original count
    assert_eq!(rtx.entry_count(), snapshot_count);
    for i in 0..200u32 {
        let key = format!("k{i:04}");
        assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(b"v1".to_vec()), "key {key}");
    }
}

#[test]
fn multiple_snapshots_at_different_versions() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    // Version 1: 10 keys
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..10u32 {
            wtx.insert(format!("k{i}").as_bytes(), b"v1").unwrap();
        }
        wtx.commit().unwrap();
    }
    let mut r1 = db.begin_read();

    // Version 2: add 10 more
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 10..20u32 {
            wtx.insert(format!("k{i}").as_bytes(), b"v2").unwrap();
        }
        wtx.commit().unwrap();
    }
    let mut r2 = db.begin_read();

    // Version 3: delete first 5
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..5u32 {
            wtx.delete(format!("k{i}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }
    let mut r3 = db.begin_read();

    // Version 4: update remaining
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 5..20u32 {
            wtx.insert(format!("k{i}").as_bytes(), b"v4").unwrap();
        }
        wtx.commit().unwrap();
    }
    let mut r4 = db.begin_read();

    assert_eq!(r1.entry_count(), 10);
    assert_eq!(r2.entry_count(), 20);
    assert_eq!(r3.entry_count(), 15);
    assert_eq!(r4.entry_count(), 15);

    assert_eq!(r1.get(b"k0").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(r2.get(b"k15").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(r3.get(b"k0").unwrap(), None);
    assert_eq!(r4.get(b"k5").unwrap(), Some(b"v4".to_vec()));
    assert_eq!(db.reader_count(), 4);
}

// ============================================================
// Backup / Compact after heavy churn
// ============================================================

#[test]
fn backup_after_heavy_churn() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("backup.db");

    let db = fast_builder(&db_path).create().unwrap();

    for round in 0..10u32 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            let key = format!("r{round}-k{i:03}");
            wtx.insert(key.as_bytes(), b"data").unwrap();
        }
        wtx.commit().unwrap();

        if round > 0 {
            let mut wtx = db.begin_write().unwrap();
            let prev = round - 1;
            for i in 0..50u32 {
                let key = format!("r{prev}-k{i:03}");
                wtx.delete(key.as_bytes()).unwrap();
            }
            wtx.commit().unwrap();
        }
    }

    db.backup(&backup_path).unwrap();

    let backup = DatabaseBuilder::new(&backup_path)
        .passphrase(b"torture-test")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();

    assert_eq!(backup.stats().entry_count, db.stats().entry_count);
    let report = backup.integrity_check().unwrap();
    assert!(report.is_ok(), "backup integrity errors: {:?}", report.errors);
}

#[test]
fn compact_then_continue_writing() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let compact_path = dir.path().join("compact.db");

    let db = fast_builder(&db_path).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            wtx.insert(format!("k{i:04}").as_bytes(), b"original").unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..400u32 {
            wtx.delete(format!("k{i:04}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    db.compact(&compact_path).unwrap();

    let compact_db = DatabaseBuilder::new(&compact_path)
        .passphrase(b"torture-test")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();

    assert_eq!(compact_db.stats().entry_count, 100);

    {
        let mut wtx = compact_db.begin_write().unwrap();
        for i in 0..200u32 {
            wtx.insert(format!("new{i:04}").as_bytes(), b"added").unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(compact_db.stats().entry_count, 300);
    let report = compact_db.integrity_check().unwrap();
    assert!(report.is_ok(), "errors: {:?}", report.errors);
}

// ============================================================
// Oracle: deterministic random ops vs BTreeMap
// ============================================================

struct SimpleRng(u32);
impl SimpleRng {
    fn next(&mut self) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 17;
        self.0 ^= self.0 << 5;
        self.0
    }
}

#[test]
fn kv_oracle_500_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = SimpleRng(12345);

    for txn in 0..500u32 {
        let mut wtx = db.begin_write().unwrap();
        let num_ops = 1 + rng.next() % 8;

        for _ in 0..num_ops {
            let key_id = rng.next() % 200;
            let key = format!("k{key_id:04}").into_bytes();

            if rng.next() % 10 < 7 {
                let val = format!("t{txn}-v{}", rng.next() % 100).into_bytes();
                wtx.insert(&key, &val).unwrap();
                oracle.insert(key, val);
            } else if oracle.contains_key(&key) {
                wtx.delete(&key).unwrap();
                oracle.remove(&key);
            }
        }

        wtx.commit().unwrap();

            if txn % 100 == 99 {
            let mut rtx = db.begin_read();
            assert_eq!(rtx.entry_count(), oracle.len() as u64,
                "count mismatch at txn {txn}");
            for (k, v) in &oracle {
                assert_eq!(rtx.get(k).unwrap(), Some(v.clone()),
                    "value mismatch for key {:?} at txn {txn}", String::from_utf8_lossy(k));
            }
        }
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), oracle.len() as u64);
    let mut scan_entries = Vec::new();
    rtx.for_each(|k, v| {
        scan_entries.push((k.to_vec(), v.to_vec()));
        Ok(())
    }).unwrap();

    let oracle_entries: Vec<_> = oracle.into_iter().collect();
    assert_eq!(scan_entries.len(), oracle_entries.len());
    for (s, o) in scan_entries.iter().zip(oracle_entries.iter()) {
        assert_eq!(s, o, "scan mismatch");
    }
}

// ============================================================
// Integrity after complex operations
// ============================================================

#[test]
fn integrity_after_insert_delete_compact_more_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let compact_path = dir.path().join("compact.db");

    let db = fast_builder(&db_path).create().unwrap();

    for round in 0..5u32 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            let key = format!("r{round}-{i:04}");
            wtx.insert(key.as_bytes(), b"data").unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            wtx.delete(format!("r0-{i:04}").as_bytes()).unwrap();
            wtx.delete(format!("r1-{i:04}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let report = db.integrity_check().unwrap();
    assert!(report.is_ok(), "pre-compact errors: {:?}", report.errors);

    db.compact(&compact_path).unwrap();
    let cdb = DatabaseBuilder::new(&compact_path)
        .passphrase(b"torture-test")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();

    {
        let mut wtx = cdb.begin_write().unwrap();
        for i in 0..300u32 {
            wtx.insert(format!("post-{i:04}").as_bytes(), b"new").unwrap();
        }
        wtx.commit().unwrap();
    }

    let report = cdb.integrity_check().unwrap();
    assert!(report.is_ok(), "post-compact errors: {:?}", report.errors);
    assert_eq!(cdb.stats().entry_count, 600 + 300);
}

// ============================================================
// Rapid overwrite stress (buffer pool cache invalidation)
// ============================================================

#[test]
fn rapid_overwrite_100_txns() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(format!("k{i:02}").as_bytes(), b"original").unwrap();
        }
        wtx.commit().unwrap();
    }

    for round in 0..100u32 {
        let mut wtx = db.begin_write().unwrap();
        let val = format!("round{round:03}");
        for i in 0..50u32 {
            wtx.insert(format!("k{i:02}").as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 50);
    for i in 0..50u32 {
        let key = format!("k{i:02}");
        assert_eq!(
            rtx.get(key.as_bytes()).unwrap(),
            Some(b"round099".to_vec()),
            "key {key}"
        );
    }
}

#[test]
fn rapid_overwrite_reopen_verify() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let db = fast_builder(&db_path).create().unwrap();
        for round in 0..50u32 {
            let mut wtx = db.begin_write().unwrap();
            let val = format!("v{round:03}");
            for i in 0..20u32 {
                wtx.insert(format!("k{i:02}").as_bytes(), val.as_bytes()).unwrap();
            }
            wtx.commit().unwrap();
        }
    }

    let db = fast_builder(&db_path).open().unwrap();
    let mut rtx = db.begin_read();
    for i in 0..20u32 {
        let key = format!("k{i:02}");
        assert_eq!(
            rtx.get(key.as_bytes()).unwrap(),
            Some(b"v049".to_vec()),
            "key {key}"
        );
    }
}

// ============================================================
// Contains-key edge cases
// ============================================================

#[test]
fn contains_key_after_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"exists", b"v").unwrap();
        wtx.insert(b"will-delete", b"v").unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.delete(b"will-delete").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert!(rtx.contains_key(b"exists").unwrap());
    assert!(!rtx.contains_key(b"will-delete").unwrap());
    assert!(!rtx.contains_key(b"never-existed").unwrap());
}

// ============================================================
// Abort stress
// ============================================================

#[test]
fn many_aborts_then_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    for _ in 0..50 {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"aborted", b"should-not-exist").unwrap();
        wtx.abort();
    }

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"real", b"data").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 1);
    assert_eq!(rtx.get(b"aborted").unwrap(), None);
    assert_eq!(rtx.get(b"real").unwrap(), Some(b"data".to_vec()));
}

#[test]
fn abort_with_named_table_modifications() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"committed").unwrap();
        wtx.table_insert(b"committed", b"k", b"v").unwrap();
        wtx.commit().unwrap();
    }

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.table_insert(b"committed", b"k", b"modified").unwrap();
        wtx.table_insert(b"committed", b"new-k", b"new-v").unwrap();
        wtx.create_table(b"aborted_table").unwrap();
        wtx.table_insert(b"aborted_table", b"x", b"y").unwrap();
        wtx.abort();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.table_get(b"committed", b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(rtx.table_get(b"committed", b"new-k").unwrap(), None);
    assert!(matches!(rtx.table_get(b"aborted_table", b"x"), Err(Error::TableNotFound(_))));
}

// ============================================================
// Drop WriteTxn without commit (implicit abort)
// ============================================================

#[test]
fn drop_write_txn_releases_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    for _ in 0..100 {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"temp", b"v").unwrap();
        // Dropped without commit
    }

    // Should still be writable
    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"final", b"v").unwrap();
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 1);
    assert_eq!(rtx.get(b"final").unwrap(), Some(b"v".to_vec()));
}

// ============================================================
// Large value stress (near overflow threshold)
// ============================================================

#[test]
fn many_large_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let big_val = vec![0x42u8; 1800];

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(format!("k{i:04}").as_bytes(), &big_val).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 100);
    for i in 0..100u32 {
        let key = format!("k{i:04}");
        assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(big_val.clone()), "key {key}");
    }
}

#[test]
fn large_values_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let big_val = vec![0xAB; 1900];

    {
        let db = fast_builder(&db_path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(format!("k{i:03}").as_bytes(), &big_val).unwrap();
        }
        wtx.commit().unwrap();
    }

    {
        let db = fast_builder(&db_path).open().unwrap();
        let mut rtx = db.begin_read();
        for i in 0..50u32 {
            let key = format!("k{i:03}");
            assert_eq!(rtx.get(key.as_bytes()).unwrap(), Some(big_val.clone()), "key {key}");
        }
    }
}
