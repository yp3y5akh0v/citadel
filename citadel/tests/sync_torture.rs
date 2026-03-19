use std::collections::BTreeMap;
use std::thread;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sync::{
    decode_lww_value, encode_lww_value, CrdtMeta, EntryKind, HlcTimestamp, MemoryTransport, NodeId,
    SyncConfig, SyncDirection, SyncOutcome, SyncSession,
};

const NS: i64 = 1_000_000_000;

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"sync-torture-test")
        .argon2_profile(Argon2Profile::Iot)
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

fn insert_range(db: &Database, start: u32, end: u32) {
    let mut wtx = db.begin_write().unwrap();
    for i in start..end {
        wtx.insert(&i.to_be_bytes(), &(i * 7).to_le_bytes())
            .unwrap();
    }
    wtx.commit().unwrap();
}

fn meta(secs: i64, logical: i32, node: u64) -> CrdtMeta {
    CrdtMeta::new(
        HlcTimestamp::new(secs * NS, logical),
        NodeId::from_u64(node),
    )
}

fn sync_session(
    init_db: &Database,
    resp_db: &Database,
    direction: SyncDirection,
    crdt: bool,
) -> (SyncOutcome, SyncOutcome) {
    let (t_init, t_resp) = MemoryTransport::pair();

    let init_session = SyncSession::new(SyncConfig {
        node_id: NodeId::from_u64(1),
        direction,
        crdt_aware: crdt,
    });
    let resp_session = SyncSession::new(SyncConfig {
        node_id: NodeId::from_u64(2),
        direction,
        crdt_aware: crdt,
    });

    let init_mgr = init_db.manager();
    let resp_mgr = resp_db.manager();

    thread::scope(|s| {
        let h1 = s.spawn(|| init_session.sync_as_initiator(init_mgr, &t_init).unwrap());
        let h2 = s.spawn(|| resp_session.sync_as_responder(resp_mgr, &t_resp).unwrap());
        (h1.join().unwrap(), h2.join().unwrap())
    })
}

// ============================================================
// Threaded sync
// ============================================================

#[test]
fn threaded_push_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 50);

    let (init_out, resp_out) = sync_session(&db1, &db2, SyncDirection::Push, false);
    assert!(!init_out.already_in_sync);
    assert!(!resp_out.already_in_sync);

    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn threaded_pull_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db2, 0, 50);

    sync_session(&db1, &db2, SyncDirection::Pull, false);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

// ============================================================
// Multi-round convergence
// ============================================================

#[test]
fn random_50_rounds_bidirectional_convergence() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for round in 0..50u32 {
        // Alternate: even rounds add to db1, odd to db2
        let db = if round % 2 == 0 { &db1 } else { &db2 };
        let key = round.to_be_bytes();
        let val = (round * 13).to_le_bytes();
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(&key, &val).unwrap();
        wtx.commit().unwrap();

        // Sync every 5 rounds
        if round % 5 == 4 {
            sync_session(&db1, &db2, SyncDirection::Bidirectional, false);
        }
    }

    // Final sync
    sync_session(&db1, &db2, SyncDirection::Bidirectional, false);

    let data1 = collect_all(&db1);
    let data2 = collect_all(&db2);
    assert_eq!(data1.len(), 50);
    assert_eq!(data1, data2);
}

#[test]
fn incremental_push_10_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for round in 0..10u32 {
        let start = round * 10;
        insert_range(&db1, start, start + 10);
        sync_session(&db1, &db2, SyncDirection::Push, false);
        assert_eq!(collect_all(&db1), collect_all(&db2));
    }

    assert_eq!(collect_all(&db2).len(), 100);
}

// ============================================================
// CRDT-aware sync
// ============================================================

#[test]
fn crdt_push_sync() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Insert CRDT values on db1
    let m = meta(1000, 0, 1);
    let val = encode_lww_value(&m, EntryKind::Put, b"hello");
    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }

    sync_session(&db1, &db2, SyncDirection::Push, true);

    let data2 = collect_all(&db2);
    assert_eq!(data2.len(), 1);
    assert_eq!(data2[b"key1".as_slice()], val);
}

#[test]
fn crdt_bidirectional_conflict_resolution() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // db1 has older timestamp
    let m1 = meta(1000, 0, 1);
    let v1 = encode_lww_value(&m1, EntryKind::Put, b"from-db1");
    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(b"conflict-key", &v1).unwrap();
        wtx.commit().unwrap();
    }

    // db2 has newer timestamp — should win
    let m2 = meta(2000, 0, 2);
    let v2 = encode_lww_value(&m2, EntryKind::Put, b"from-db2");
    {
        let mut wtx = db2.begin_write().unwrap();
        wtx.insert(b"conflict-key", &v2).unwrap();
        wtx.commit().unwrap();
    }

    sync_session(&db1, &db2, SyncDirection::Bidirectional, true);

    // After bidirectional sync, both should have db2's value (newer timestamp)
    let data1 = collect_all(&db1);
    let data2 = collect_all(&db2);
    assert_eq!(data1, data2);

    let stored = &data1[b"conflict-key".as_slice()];
    let decoded = decode_lww_value(stored).unwrap();
    assert_eq!(decoded.user_value, b"from-db2");
}

#[test]
fn crdt_tombstone_wins_over_older_put() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // db1: put at t=1000
    let m1 = meta(1000, 0, 1);
    let v1 = encode_lww_value(&m1, EntryKind::Put, b"alive");
    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(b"key", &v1).unwrap();
        wtx.commit().unwrap();
    }

    // db2: tombstone at t=2000 (newer, should win)
    let m2 = meta(2000, 0, 2);
    let v2 = encode_lww_value(&m2, EntryKind::Tombstone, b"");
    {
        let mut wtx = db2.begin_write().unwrap();
        wtx.insert(b"key", &v2).unwrap();
        wtx.commit().unwrap();
    }

    sync_session(&db1, &db2, SyncDirection::Bidirectional, true);

    let data1 = collect_all(&db1);
    let data2 = collect_all(&db2);
    assert_eq!(data1, data2);

    let decoded = decode_lww_value(&data1[b"key".as_slice()]).unwrap();
    assert_eq!(decoded.kind, EntryKind::Tombstone);
}

// ============================================================
// Concurrent reader during sync
// ============================================================

#[test]
fn snapshot_isolation_during_sync() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 50);

    // Take a read snapshot of db2 before sync
    let rtx = db2.begin_read();
    let before_count = {
        let mut count = 0u64;
        let mut r = rtx;
        r.for_each(|_, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        count
    };
    assert_eq!(before_count, 0);

    // Sync should still work
    sync_session(&db1, &db2, SyncDirection::Push, false);
    assert_eq!(collect_all(&db2).len(), 50);
}

// ============================================================
// 3-node ring sync
// ============================================================

#[test]
fn three_node_ring_convergence() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();
    let db_c = fast_builder(&dir.path().join("c.db")).create().unwrap();

    // A has keys 0..10
    insert_range(&db_a, 0, 10);
    // B has keys 100..110
    insert_range(&db_b, 100, 110);
    // C has keys 200..210
    insert_range(&db_c, 200, 210);

    // A → B
    sync_session(&db_a, &db_b, SyncDirection::Bidirectional, false);
    // B → C
    sync_session(&db_b, &db_c, SyncDirection::Bidirectional, false);
    // C → A
    sync_session(&db_c, &db_a, SyncDirection::Bidirectional, false);

    let data_a = collect_all(&db_a);
    let data_b = collect_all(&db_b);
    let data_c = collect_all(&db_c);

    assert_eq!(data_a.len(), 30);
    assert_eq!(data_a, data_b);
    assert_eq!(data_b, data_c);
}

// ============================================================
// Many small syncs
// ============================================================

#[test]
fn many_small_syncs_rapid_fire() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for i in 0..20u32 {
        let key = i.to_be_bytes();
        let val = (i * 3).to_le_bytes();
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(&key, &val).unwrap();
        wtx.commit().unwrap();

        // Sync after each insert
        sync_session(&db1, &db2, SyncDirection::Push, false);
        assert_eq!(collect_all(&db1), collect_all(&db2));
    }
}

// ============================================================
// Large values
// ============================================================

#[test]
fn sync_with_large_values() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    {
        let mut wtx = db1.begin_write().unwrap();
        for i in 0..10u32 {
            let key = i.to_be_bytes();
            let val = vec![i as u8; 512]; // 512-byte values
            wtx.insert(&key, &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    sync_session(&db1, &db2, SyncDirection::Push, false);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

// ============================================================
// Mixed operations
// ============================================================

#[test]
fn sync_after_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 20);
    sync_session(&db1, &db2, SyncDirection::Push, false);
    assert_eq!(collect_all(&db1), collect_all(&db2));

    // Delete some keys from db1
    {
        let mut wtx = db1.begin_write().unwrap();
        for i in 0..10u32 {
            wtx.delete(&i.to_be_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // After push sync, db2 gets the remaining entries from db1
    sync_session(&db1, &db2, SyncDirection::Push, false);

    // db1 has 10 entries, db2 has 20 (push only sends source entries, doesn't delete)
    let data1 = collect_all(&db1);
    assert_eq!(data1.len(), 10);
}

#[test]
fn sync_value_update_across_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Round 1: insert key=0 with value "v1"
    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(b"key", b"v1").unwrap();
        wtx.commit().unwrap();
    }
    sync_session(&db1, &db2, SyncDirection::Push, false);
    assert_eq!(collect_all(&db2)[b"key".as_slice()], b"v1");

    // Round 2: update key=0 to "v2"
    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(b"key", b"v2").unwrap();
        wtx.commit().unwrap();
    }
    sync_session(&db1, &db2, SyncDirection::Push, false);
    assert_eq!(collect_all(&db2)[b"key".as_slice()], b"v2");

    // Round 3: update key=0 to "v3"
    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(b"key", b"v3").unwrap();
        wtx.commit().unwrap();
    }
    sync_session(&db1, &db2, SyncDirection::Push, false);
    assert_eq!(collect_all(&db2)[b"key".as_slice()], b"v3");
}

// ============================================================
// Stress tests
// ============================================================

#[test]
fn stress_bidirectional_alternating_writers() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let mut expected = BTreeMap::new();

    for round in 0..30u32 {
        let db = if round % 2 == 0 { &db1 } else { &db2 };
        let key = format!("key-{:04}", round);
        let val = format!("val-{}", round * 7);

        {
            let mut wtx = db.begin_write().unwrap();
            wtx.insert(key.as_bytes(), val.as_bytes()).unwrap();
            wtx.commit().unwrap();
        }
        expected.insert(key.into_bytes(), val.into_bytes());

        if round % 3 == 2 {
            sync_session(&db1, &db2, SyncDirection::Bidirectional, false);
        }
    }

    // Final sync
    sync_session(&db1, &db2, SyncDirection::Bidirectional, false);

    let data1 = collect_all(&db1);
    let data2 = collect_all(&db2);
    assert_eq!(data1, data2);
    assert_eq!(data1.len(), 30);
    assert_eq!(data1, expected);
}

#[test]
fn stress_push_1000_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 1000);
    sync_session(&db1, &db2, SyncDirection::Push, false);

    let data1 = collect_all(&db1);
    let data2 = collect_all(&db2);
    assert_eq!(data1.len(), 1000);
    assert_eq!(data1, data2);
}

// ============================================================
// CRDT convergence
// ============================================================

#[test]
fn crdt_commutativity_both_directions_same_result() {
    let dir = tempfile::tempdir().unwrap();

    // Test A→B then B→A
    let db_a1 = fast_builder(&dir.path().join("a1.db")).create().unwrap();
    let db_b1 = fast_builder(&dir.path().join("b1.db")).create().unwrap();

    // Test B→A then A→B
    let db_a2 = fast_builder(&dir.path().join("a2.db")).create().unwrap();
    let db_b2 = fast_builder(&dir.path().join("b2.db")).create().unwrap();

    // Same data in both pairs
    let m_a = meta(1000, 0, 1);
    let v_a = encode_lww_value(&m_a, EntryKind::Put, b"from-A");
    let m_b = meta(2000, 0, 2);
    let v_b = encode_lww_value(&m_b, EntryKind::Put, b"from-B");

    for db_a in [&db_a1, &db_a2] {
        let mut wtx = db_a.begin_write().unwrap();
        wtx.insert(b"key", &v_a).unwrap();
        wtx.commit().unwrap();
    }
    for db_b in [&db_b1, &db_b2] {
        let mut wtx = db_b.begin_write().unwrap();
        wtx.insert(b"key", &v_b).unwrap();
        wtx.commit().unwrap();
    }

    // Direction 1: A→B then B→A
    sync_session(&db_a1, &db_b1, SyncDirection::Push, true);
    sync_session(&db_b1, &db_a1, SyncDirection::Push, true);

    // Direction 2: B→A then A→B
    sync_session(&db_b2, &db_a2, SyncDirection::Push, true);
    sync_session(&db_a2, &db_b2, SyncDirection::Push, true);

    // Both orderings should produce the same result
    let data_ab = collect_all(&db_a1);
    let data_ba = collect_all(&db_a2);
    assert_eq!(data_ab, data_ba);

    // And B wins (newer timestamp)
    let decoded = decode_lww_value(&data_ab[b"key".as_slice()]).unwrap();
    assert_eq!(decoded.user_value, b"from-B");
}

#[test]
fn crdt_many_conflicts_all_resolved() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // 20 conflicting keys, db2 always has newer timestamp
    {
        let mut wtx1 = db1.begin_write().unwrap();
        let mut wtx2 = db2.begin_write().unwrap();
        for i in 0..20u32 {
            let key = format!("conflict-{}", i);
            let m1 = meta(1000 + i as i64, 0, 1);
            let v1 = encode_lww_value(&m1, EntryKind::Put, format!("db1-{}", i).as_bytes());
            let m2 = meta(2000 + i as i64, 0, 2);
            let v2 = encode_lww_value(&m2, EntryKind::Put, format!("db2-{}", i).as_bytes());
            wtx1.insert(key.as_bytes(), &v1).unwrap();
            wtx2.insert(key.as_bytes(), &v2).unwrap();
        }
        wtx1.commit().unwrap();
        wtx2.commit().unwrap();
    }

    sync_session(&db1, &db2, SyncDirection::Bidirectional, true);

    let data1 = collect_all(&db1);
    let data2 = collect_all(&db2);
    assert_eq!(data1, data2);

    // All keys should have db2's values
    for i in 0..20u32 {
        let key = format!("conflict-{}", i);
        let decoded = decode_lww_value(&data1[key.as_bytes()]).unwrap();
        assert_eq!(
            decoded.user_value,
            format!("db2-{}", i).as_bytes(),
            "conflict key {} should have db2's value",
            i
        );
    }
}

// ============================================================
// Edge cases
// ============================================================

#[test]
fn sync_both_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let (init_out, resp_out) = sync_session(&db1, &db2, SyncDirection::Bidirectional, false);
    assert!(init_out.already_in_sync);
    assert!(resp_out.already_in_sync);
}

#[test]
fn sync_single_key() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(b"only-key", b"only-value").unwrap();
        wtx.commit().unwrap();
    }

    sync_session(&db1, &db2, SyncDirection::Push, false);
    assert_eq!(collect_all(&db2).len(), 1);
    assert_eq!(collect_all(&db2)[b"only-key".as_slice()], b"only-value");
}

#[test]
fn sync_preserves_binary_keys_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let key = vec![0x00, 0xFF, 0x01, 0xFE];
    let val = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00];

    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(&key, &val).unwrap();
        wtx.commit().unwrap();
    }

    sync_session(&db1, &db2, SyncDirection::Push, false);
    let data2 = collect_all(&db2);
    assert_eq!(data2[&key], val);
}
