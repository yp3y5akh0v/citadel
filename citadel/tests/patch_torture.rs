use std::collections::BTreeMap;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sync::{
    apply_patch, decode_lww_value, encode_lww_value, merkle_diff, ApplyResult, CrdtMeta, EntryKind,
    HlcTimestamp, LocalTreeReader, NodeId, SyncPatch,
};
use rand::Rng;

const NS: i64 = 1_000_000_000;

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"patch-torture")
        .argon2_profile(Argon2Profile::Iot)
}

fn meta(secs: i64, logical: i32, node: u64) -> CrdtMeta {
    CrdtMeta::new(
        HlcTimestamp::new(secs * NS, logical),
        NodeId::from_u64(node),
    )
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

fn sync_push(source: &Database, target: &Database, source_node: u64, crdt: bool) -> ApplyResult {
    let r1 = LocalTreeReader::new(source.manager());
    let r2 = LocalTreeReader::new(target.manager());
    let d = merkle_diff(&r1, &r2).unwrap();
    let patch = SyncPatch::from_diff(NodeId::from_u64(source_node), &d, crdt);
    apply_patch(target.manager(), &patch).unwrap()
}

fn assert_crdt_converged(db1: &Database, db2: &Database) {
    let data1 = collect_all(db1);
    let data2 = collect_all(db2);
    assert_eq!(data1.len(), data2.len(), "entry count mismatch");
    for (k, v1) in &data1 {
        let v2 = data2
            .get(k)
            .unwrap_or_else(|| panic!("key missing from db2"));
        let d1 = decode_lww_value(v1).unwrap();
        let d2 = decode_lww_value(v2).unwrap();
        assert_eq!(d1.user_value, d2.user_value, "value mismatch");
        assert_eq!(d1.meta, d2.meta, "meta mismatch");
    }
}

// ============================================================
// Torture tests
// ============================================================

#[test]
fn random_bidirectional_50_rounds_convergence() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("db1.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("db2.db")).create().unwrap();

    let mut rng = rand::thread_rng();
    let mut ts = 0i64;

    for _ in 0..50 {
        // Node 1: 1-3 random changes
        {
            let mut wtx = db1.begin_write().unwrap();
            for _ in 0..rng.gen_range(1..=3) {
                let key_num: u32 = rng.gen_range(0..100);
                ts += 1;
                let m = meta(ts, 0, 1);
                let val = encode_lww_value(&m, EntryKind::Put, format!("n1-{ts}").as_bytes());
                wtx.insert(&key_num.to_be_bytes(), &val).unwrap();
            }
            wtx.commit().unwrap();
        }

        // Node 2: 1-3 random changes
        {
            let mut wtx = db2.begin_write().unwrap();
            for _ in 0..rng.gen_range(1..=3) {
                let key_num: u32 = rng.gen_range(0..100);
                ts += 1;
                let m = meta(ts, 0, 2);
                let val = encode_lww_value(&m, EntryKind::Put, format!("n2-{ts}").as_bytes());
                wtx.insert(&key_num.to_be_bytes(), &val).unwrap();
            }
            wtx.commit().unwrap();
        }

        sync_push(&db1, &db2, 1, true);
        sync_push(&db2, &db1, 2, true);
    }

    assert_crdt_converged(&db1, &db2);
}

#[test]
fn many_conflicts_lww_alternating_winner() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("db1.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("db2.db")).create().unwrap();

    // Even keys: node 1 wins (higher ts). Odd keys: node 2 wins.
    {
        let mut wtx = db1.begin_write().unwrap();
        for i in 0u32..50 {
            let ts = if i % 2 == 0 { 200 } else { 100 };
            let m = meta(ts, 0, 1);
            let val = encode_lww_value(&m, EntryKind::Put, format!("n1-{i}").as_bytes());
            wtx.insert(&i.to_be_bytes(), &val).unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = db2.begin_write().unwrap();
        for i in 0u32..50 {
            let ts = if i % 2 == 0 { 100 } else { 200 };
            let m = meta(ts, 0, 2);
            let val = encode_lww_value(&m, EntryKind::Put, format!("n2-{i}").as_bytes());
            wtx.insert(&i.to_be_bytes(), &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    sync_push(&db1, &db2, 1, true);
    sync_push(&db2, &db1, 2, true);

    assert_crdt_converged(&db1, &db2);

    let data = collect_all(&db1);
    for i in 0u32..50 {
        let decoded = decode_lww_value(&data[&i.to_be_bytes().to_vec()]).unwrap();
        if i % 2 == 0 {
            assert_eq!(
                decoded.user_value,
                format!("n1-{i}").as_bytes(),
                "even key {i}"
            );
        } else {
            assert_eq!(
                decoded.user_value,
                format!("n2-{i}").as_bytes(),
                "odd key {i}"
            );
        }
    }
}

#[test]
fn patch_deserialize_random_bytes() {
    let mut rng = rand::thread_rng();
    for _ in 0..1000 {
        let len = rng.gen_range(0..256);
        let data: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
        // Must not panic, only return Err
        let _ = SyncPatch::deserialize(&data);
    }
}

#[test]
fn large_keys_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let big_key = vec![0xAA; 128];
    let big_val = vec![0xBB; 1800];

    {
        let mut wtx = source.begin_write().unwrap();
        for i in 0u8..20 {
            let mut key = big_key.clone();
            key[0] = i;
            let mut val = big_val.clone();
            val[0] = i;
            wtx.insert(&key, &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    sync_push(&source, &target, 1, false);
    assert_eq!(collect_all(&source), collect_all(&target));
}

#[test]
fn idempotent_apply_twice() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let m = meta(10, 0, 1);
    {
        let mut wtx = source.begin_write().unwrap();
        for i in 0u32..20 {
            let val = encode_lww_value(&m, EntryKind::Put, format!("v{i}").as_bytes());
            wtx.insert(&i.to_be_bytes(), &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    // First apply
    let r1 = sync_push(&source, &target, 1, true);
    assert!(r1.entries_applied > 0);

    let snapshot = collect_all(&target);

    // Second apply of the same diff: nothing should change
    let r2 = sync_push(&source, &target, 1, true);
    assert_eq!(r2.entries_applied, 0);
    assert!(r2.entries_equal > 0 || r2.entries_skipped == 0);

    assert_eq!(snapshot, collect_all(&target));
}

#[test]
fn three_node_ring_convergence() {
    let dir = tempfile::tempdir().unwrap();
    let a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let b = fast_builder(&dir.path().join("b.db")).create().unwrap();
    let c = fast_builder(&dir.path().join("c.db")).create().unwrap();

    // Each node writes unique keys
    {
        let mut wtx = a.begin_write().unwrap();
        for i in 0u32..10 {
            let val = encode_lww_value(&meta(1, 0, 1), EntryKind::Put, format!("a-{i}").as_bytes());
            wtx.insert(&[0, i as u8], &val).unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = b.begin_write().unwrap();
        for i in 0u32..10 {
            let val = encode_lww_value(&meta(1, 0, 2), EntryKind::Put, format!("b-{i}").as_bytes());
            wtx.insert(&[1, i as u8], &val).unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = c.begin_write().unwrap();
        for i in 0u32..10 {
            let val = encode_lww_value(&meta(1, 0, 3), EntryKind::Put, format!("c-{i}").as_bytes());
            wtx.insert(&[2, i as u8], &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Ring sync: A->B, B->C, C->A (forward)
    sync_push(&a, &b, 1, true);
    sync_push(&b, &c, 2, true);
    sync_push(&c, &a, 3, true);

    // Reverse ring: A->C, C->B, B->A
    sync_push(&a, &c, 1, true);
    sync_push(&c, &b, 3, true);
    sync_push(&b, &a, 2, true);

    // All three should have all 30 entries
    assert_crdt_converged(&a, &b);
    assert_crdt_converged(&b, &c);
    assert_eq!(collect_all(&a).len(), 30);
}

#[test]
fn incremental_100_inserts_crdt() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    for i in 0u32..100 {
        let m = meta(i as i64 + 1, 0, 1);
        let val = encode_lww_value(&m, EntryKind::Put, format!("v{i}").as_bytes());
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(&i.to_be_bytes(), &val).unwrap();
        wtx.commit().unwrap();

        sync_push(&source, &target, 1, true);
    }

    let src_data = collect_all(&source);
    let tgt_data = collect_all(&target);
    assert_eq!(src_data.len(), 100);
    assert_eq!(tgt_data.len(), 100);
    for (k, v) in &src_data {
        let td = decode_lww_value(tgt_data.get(k).unwrap()).unwrap();
        let sd = decode_lww_value(v).unwrap();
        assert_eq!(sd.user_value, td.user_value);
    }
}

#[test]
fn serialize_roundtrip_500_entries() {
    let mut rng = rand::thread_rng();
    let entries: Vec<_> = (0..500u32)
        .map(|i| {
            let key_len = rng.gen_range(4..64);
            let val_len = rng.gen_range(0..256);
            let key: Vec<u8> = (0..key_len).map(|_| rng.gen()).collect();
            let value: Vec<u8> = (0..val_len).map(|_| rng.gen()).collect();
            let kind = if i % 10 == 0 {
                EntryKind::Tombstone
            } else {
                EntryKind::Put
            };
            citadel_sync::PatchEntry {
                key,
                value,
                kind,
                crdt_meta: Some(meta(
                    rng.gen_range(1..10000),
                    rng.gen_range(0..100),
                    rng.gen(),
                )),
            }
        })
        .collect();

    let patch = SyncPatch {
        source_node: NodeId::from_u64(42),
        entries,
        crdt_aware: true,
    };

    let bytes = patch.serialize();
    let restored = SyncPatch::deserialize(&bytes).unwrap();

    assert_eq!(restored.len(), 500);
    assert!(restored.crdt_aware);
    for (orig, rest) in patch.entries.iter().zip(restored.entries.iter()) {
        assert_eq!(orig.key, rest.key);
        assert_eq!(orig.value, rest.value);
        assert_eq!(orig.kind, rest.kind);
        assert_eq!(orig.crdt_meta, rest.crdt_meta);
    }
}

#[test]
fn disjoint_patches_from_different_sources() {
    let dir = tempfile::tempdir().unwrap();
    let src_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let src_b = fast_builder(&dir.path().join("b.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    {
        let mut wtx = src_a.begin_write().unwrap();
        for i in 0u32..10 {
            wtx.insert(&[0, i as u8], format!("a-{i}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = src_b.begin_write().unwrap();
        for i in 0u32..10 {
            wtx.insert(&[1, i as u8], format!("b-{i}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }

    sync_push(&src_a, &target, 1, false);
    sync_push(&src_b, &target, 2, false);

    let data = collect_all(&target);
    assert_eq!(data.len(), 20);
}

#[test]
fn bidirectional_with_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("db1.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("db2.db")).create().unwrap();

    // db1: put key-a at t=1, tombstone key-b at t=3
    {
        let mut wtx = db1.begin_write().unwrap();
        let va = encode_lww_value(&meta(1, 0, 1), EntryKind::Put, b"alive-a");
        let vb = encode_lww_value(&meta(3, 0, 1), EntryKind::Tombstone, b"");
        wtx.insert(b"key-a", &va).unwrap();
        wtx.insert(b"key-b", &vb).unwrap();
        wtx.commit().unwrap();
    }

    // db2: tombstone key-a at t=2, put key-b at t=2
    {
        let mut wtx = db2.begin_write().unwrap();
        let va = encode_lww_value(&meta(2, 0, 2), EntryKind::Tombstone, b"");
        let vb = encode_lww_value(&meta(2, 0, 2), EntryKind::Put, b"alive-b");
        wtx.insert(b"key-a", &va).unwrap();
        wtx.insert(b"key-b", &vb).unwrap();
        wtx.commit().unwrap();
    }

    sync_push(&db1, &db2, 1, true);
    sync_push(&db2, &db1, 2, true);

    assert_crdt_converged(&db1, &db2);

    let data = collect_all(&db1);
    // key-a: db2's tombstone at t=2 > db1's put at t=1
    let da = decode_lww_value(&data[&b"key-a".to_vec()]).unwrap();
    assert_eq!(da.kind, EntryKind::Tombstone);
    // key-b: db1's tombstone at t=3 > db2's put at t=2
    let db = decode_lww_value(&data[&b"key-b".to_vec()]).unwrap();
    assert_eq!(db.kind, EntryKind::Tombstone);
}

#[test]
fn node_id_tiebreaker() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("db1.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("db2.db")).create().unwrap();

    // Same timestamp, node 2 > node 1 -> node 2 wins
    let m1 = CrdtMeta::new(HlcTimestamp::new(5 * NS, 0), NodeId::from_u64(1));
    let m2 = CrdtMeta::new(HlcTimestamp::new(5 * NS, 0), NodeId::from_u64(100));

    {
        let val = encode_lww_value(&m1, EntryKind::Put, b"node1");
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(b"tie", &val).unwrap();
        wtx.commit().unwrap();
    }
    {
        let val = encode_lww_value(&m2, EntryKind::Put, b"node100");
        let mut wtx = db2.begin_write().unwrap();
        wtx.insert(b"tie", &val).unwrap();
        wtx.commit().unwrap();
    }

    // Sync db2 -> db1: node 100 > node 1, remote wins
    sync_push(&db2, &db1, 100, true);
    let data1 = collect_all(&db1);
    let d1 = decode_lww_value(&data1[&b"tie".to_vec()]).unwrap();
    assert_eq!(d1.user_value, b"node100");

    // Sync db1 -> db2: db1 now has node100's value, equal -> skip
    sync_push(&db1, &db2, 1, true);
    assert_crdt_converged(&db1, &db2);
}

#[test]
fn overwrite_same_key_100_times() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    // Source overwrites key "hot" 100 times with increasing timestamps
    for i in 1..=100i64 {
        let m = meta(i, 0, 1);
        let val = encode_lww_value(&m, EntryKind::Put, format!("v{i}").as_bytes());
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"hot", &val).unwrap();
        wtx.commit().unwrap();
    }

    sync_push(&source, &target, 1, true);

    let data = collect_all(&target);
    let decoded = decode_lww_value(&data[&b"hot".to_vec()]).unwrap();
    assert_eq!(decoded.user_value, b"v100");
    assert_eq!(decoded.meta.timestamp, HlcTimestamp::new(100 * NS, 0));
}

#[test]
fn mixed_entry_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    {
        let mut wtx = source.begin_write().unwrap();
        // Tiny entries
        for i in 0u8..20 {
            wtx.insert(&[i], &[i]).unwrap();
        }
        // Medium entries
        for i in 20u8..40 {
            let val = vec![i; 200];
            wtx.insert(&[i], &val).unwrap();
        }
        // Larger entries
        for i in 40u8..50 {
            let val = vec![i; 1500];
            wtx.insert(&[i], &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    sync_push(&source, &target, 1, false);
    assert_eq!(collect_all(&source), collect_all(&target));
}

#[test]
fn apply_empty_then_full_then_incremental() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let r1 = sync_push(&source, &target, 1, false);
    assert_eq!(r1, ApplyResult::empty());

    {
        let mut wtx = source.begin_write().unwrap();
        for i in 0u32..200 {
            wtx.insert(&i.to_be_bytes(), format!("initial-{i}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }
    let r2 = sync_push(&source, &target, 1, false);
    assert!(r2.entries_applied >= 200);
    assert_eq!(collect_all(&source), collect_all(&target));

    {
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(&999u32.to_be_bytes(), b"new-entry").unwrap();
        wtx.commit().unwrap();
    }
    let r3 = sync_push(&source, &target, 1, false);
    assert!(r3.entries_applied >= 1);
    assert_eq!(collect_all(&source), collect_all(&target));
}
