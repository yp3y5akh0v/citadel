use std::collections::BTreeMap;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_core::constants::MAX_INLINE_VALUE_SIZE;
use citadel_core::types::ValueType;
use citadel_sync::{
    apply_patch, apply_patch_to_table, decode_lww_value, encode_lww_value, merkle_diff,
    ApplyResult, CrdtMeta, EntryKind, HlcTimestamp, LocalTreeReader, NodeId, SyncPatch,
};

const NS: i64 = 1_000_000_000;

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"sync-patch-test")
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

#[test]
fn empty_databases_noop() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let result = sync_push(&source, &target, 1, false);
    assert_eq!(result, ApplyResult::empty());
    assert!(collect_all(&target).is_empty());
}

#[test]
fn diff_serialize_apply_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    {
        let mut wtx = source.begin_write().unwrap();
        for i in 0u32..50 {
            wtx.insert(&i.to_be_bytes(), format!("val-{i}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }

    let r1 = LocalTreeReader::new(source.manager());
    let r2 = LocalTreeReader::new(target.manager());
    let d = merkle_diff(&r1, &r2).unwrap();

    let patch = SyncPatch::from_diff(NodeId::from_u64(1), &d, false);
    let serialized = patch.serialize().unwrap();
    let deserialized = SyncPatch::deserialize(&serialized).unwrap();

    let result = apply_patch(target.manager(), &deserialized).unwrap();
    assert_eq!(result.entries_applied as usize, collect_all(&source).len());
    assert_eq!(collect_all(&source), collect_all(&target));
}

#[test]
fn overflow_values_survive_default_and_named_patch_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();
    let default_value = vec![0xA1; MAX_INLINE_VALUE_SIZE + 4096];
    let named_value = vec![0xB2; MAX_INLINE_VALUE_SIZE + 8192];

    for db in [&source, &target] {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"large_values").unwrap();
        wtx.commit().unwrap();
    }
    let mut wtx = source.begin_write().unwrap();
    wtx.insert(b"default", &default_value).unwrap();
    wtx.table_insert(b"large_values", b"named", &named_value)
        .unwrap();
    wtx.commit().unwrap();

    let source_default = LocalTreeReader::new(source.manager());
    let target_default = LocalTreeReader::new(target.manager());
    let default_diff = merkle_diff(&source_default, &target_default).unwrap();
    assert_eq!(
        default_diff
            .entries
            .iter()
            .find(|entry| entry.key == b"default")
            .unwrap()
            .value,
        default_value
    );
    let default_patch = SyncPatch::deserialize(
        &SyncPatch::from_diff(NodeId::from_u64(1), &default_diff, false)
            .serialize()
            .unwrap(),
    )
    .unwrap();
    apply_patch(target.manager(), &default_patch).unwrap();

    let source_named = LocalTreeReader::for_table(source.manager(), b"large_values").unwrap();
    let target_named = LocalTreeReader::for_table(target.manager(), b"large_values").unwrap();
    let named_diff = merkle_diff(&source_named, &target_named).unwrap();
    assert_eq!(
        named_diff
            .entries
            .iter()
            .find(|entry| entry.key == b"named")
            .unwrap()
            .value,
        named_value
    );
    let named_patch = SyncPatch::deserialize(
        &SyncPatch::from_diff(NodeId::from_u64(1), &named_diff, false)
            .serialize()
            .unwrap(),
    )
    .unwrap();
    apply_patch_to_table(target.manager(), b"large_values", &named_patch).unwrap();

    let mut read = target.begin_read();
    assert_eq!(read.get(b"default").unwrap().unwrap(), default_value);
    assert_eq!(
        read.table_get(b"large_values", b"named").unwrap().unwrap(),
        named_value
    );
}

#[test]
fn same_length_divergent_overflow_value_is_emitted_by_merkle_diff() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();
    let len = MAX_INLINE_VALUE_SIZE + 4096;
    let source_value = vec![0x51; len];
    let target_value = vec![0xA7; len];

    let mut source_write = source.begin_write().unwrap();
    source_write.insert(b"large", &source_value).unwrap();
    source_write.commit().unwrap();
    let mut target_write = target.begin_write().unwrap();
    target_write.insert(b"large", &target_value).unwrap();
    target_write.commit().unwrap();

    let source_reader = LocalTreeReader::new(source.manager());
    let target_reader = LocalTreeReader::new(target.manager());
    let diff = merkle_diff(&source_reader, &target_reader).unwrap();

    assert_eq!(diff.entries.len(), 1);
    assert_eq!(diff.entries[0].key, b"large");
    assert_eq!(diff.entries[0].value, source_value);
    assert_eq!(diff.entries[0].val_type, ValueType::Overflow as u8);
}

#[test]
fn non_crdt_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    {
        let mut wtx = target.begin_write().unwrap();
        wtx.insert(b"key1", b"old-value").unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"key1", b"new-value").unwrap();
        wtx.commit().unwrap();
    }

    let result = sync_push(&source, &target, 1, false);
    assert!(result.entries_applied > 0);

    let data = collect_all(&target);
    assert_eq!(data[&b"key1".to_vec()], b"new-value");
}

#[test]
fn crdt_remote_wins() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let local_meta = meta(1, 0, 1);
    let remote_meta = meta(2, 0, 2);

    {
        let val = encode_lww_value(&local_meta, EntryKind::Put, b"local");
        let mut wtx = target.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }
    {
        let val = encode_lww_value(&remote_meta, EntryKind::Put, b"remote");
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }

    let result = sync_push(&source, &target, 2, true);
    assert!(result.entries_applied > 0);

    let data = collect_all(&target);
    let decoded = decode_lww_value(&data[&b"key1".to_vec()]).unwrap();
    assert_eq!(decoded.user_value, b"remote");
    assert_eq!(decoded.meta.timestamp, remote_meta.timestamp);
}

#[test]
fn crdt_local_wins() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let local_meta = meta(2, 0, 1);
    let remote_meta = meta(1, 0, 2);

    {
        let val = encode_lww_value(&local_meta, EntryKind::Put, b"local-wins");
        let mut wtx = target.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }
    {
        let val = encode_lww_value(&remote_meta, EntryKind::Put, b"should-lose");
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }

    sync_push(&source, &target, 2, true);

    let data = collect_all(&target);
    let decoded = decode_lww_value(&data[&b"key1".to_vec()]).unwrap();
    assert_eq!(decoded.user_value, b"local-wins");
}

#[test]
fn crdt_equal_entries_detected() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let m = meta(5, 3, 42);
    let shared_val = encode_lww_value(&m, EntryKind::Put, b"same");
    let extra_val = encode_lww_value(&meta(6, 0, 42), EntryKind::Put, b"only-source");

    // Source: key1 (shared) + key2 (extra)
    {
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"key1", &shared_val).unwrap();
        wtx.insert(b"key2", &extra_val).unwrap();
        wtx.commit().unwrap();
    }
    // Target: key1 only (same value as source)
    {
        let mut wtx = target.begin_write().unwrap();
        wtx.insert(b"key1", &shared_val).unwrap();
        wtx.commit().unwrap();
    }

    let result = sync_push(&source, &target, 42, true);
    // key2 is new -> applied. key1 is equal -> entries_equal.
    assert!(result.entries_applied >= 1);
    assert_eq!(result.entries_skipped, 0);
    assert!(result.entries_equal >= 1);

    assert_eq!(collect_all(&target).len(), 2);
}

#[test]
fn crdt_new_key() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let m = meta(1, 0, 1);
    let val = encode_lww_value(&m, EntryKind::Put, b"new-data");

    {
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"new-key", &val).unwrap();
        wtx.commit().unwrap();
    }

    let result = sync_push(&source, &target, 1, true);
    assert!(result.entries_applied > 0);

    let data = collect_all(&target);
    let decoded = decode_lww_value(&data[&b"new-key".to_vec()]).unwrap();
    assert_eq!(decoded.user_value, b"new-data");
}

#[test]
fn patch_with_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let live_meta = meta(1, 0, 1);
    let tomb_meta = meta(2, 0, 2);

    {
        let val = encode_lww_value(&live_meta, EntryKind::Put, b"alive");
        let mut wtx = target.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }
    {
        let val = encode_lww_value(&tomb_meta, EntryKind::Tombstone, b"");
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }

    let result = sync_push(&source, &target, 2, true);
    assert!(result.entries_applied > 0);

    let data = collect_all(&target);
    let decoded = decode_lww_value(&data[&b"key1".to_vec()]).unwrap();
    assert_eq!(decoded.kind, EntryKind::Tombstone);
}

#[test]
fn bidirectional_disjoint_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("db1.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("db2.db")).create().unwrap();

    let m1 = meta(1, 0, 1);
    let m2 = meta(1, 0, 2);

    {
        let val = encode_lww_value(&m1, EntryKind::Put, b"from-db1");
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(b"key-a", &val).unwrap();
        wtx.commit().unwrap();
    }
    {
        let val = encode_lww_value(&m2, EntryKind::Put, b"from-db2");
        let mut wtx = db2.begin_write().unwrap();
        wtx.insert(b"key-b", &val).unwrap();
        wtx.commit().unwrap();
    }

    sync_push(&db1, &db2, 1, true);
    sync_push(&db2, &db1, 2, true);

    let data1 = collect_all(&db1);
    let data2 = collect_all(&db2);

    assert!(data1.contains_key(b"key-a".as_slice()));
    assert!(data1.contains_key(b"key-b".as_slice()));
    assert!(data2.contains_key(b"key-a".as_slice()));
    assert!(data2.contains_key(b"key-b".as_slice()));

    let d1_a = decode_lww_value(&data1[&b"key-a".to_vec()]).unwrap();
    let d2_a = decode_lww_value(&data2[&b"key-a".to_vec()]).unwrap();
    assert_eq!(d1_a.user_value, b"from-db1");
    assert_eq!(d1_a.user_value, d2_a.user_value);

    let d1_b = decode_lww_value(&data1[&b"key-b".to_vec()]).unwrap();
    let d2_b = decode_lww_value(&data2[&b"key-b".to_vec()]).unwrap();
    assert_eq!(d1_b.user_value, b"from-db2");
    assert_eq!(d1_b.user_value, d2_b.user_value);
}

#[test]
fn bidirectional_crdt_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("db1.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("db2.db")).create().unwrap();

    let m1 = meta(2, 0, 1); // later -> wins
    let m2 = meta(1, 0, 2);

    {
        let val = encode_lww_value(&m1, EntryKind::Put, b"winner");
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }
    {
        let val = encode_lww_value(&m2, EntryKind::Put, b"loser");
        let mut wtx = db2.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }

    sync_push(&db1, &db2, 1, true);
    sync_push(&db2, &db1, 2, true);

    let data1 = collect_all(&db1);
    let data2 = collect_all(&db2);
    let d1 = decode_lww_value(&data1[&b"key1".to_vec()]).unwrap();
    let d2 = decode_lww_value(&data2[&b"key1".to_vec()]).unwrap();
    assert_eq!(d1.user_value, b"winner");
    assert_eq!(d2.user_value, b"winner");
    assert_eq!(d1.meta, d2.meta);
}

#[test]
fn incremental_patches_3_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    // Round 1: 10 keys
    {
        let mut wtx = source.begin_write().unwrap();
        for i in 0u32..10 {
            wtx.insert(&i.to_be_bytes(), format!("r1-{i}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }
    sync_push(&source, &target, 1, false);
    assert_eq!(collect_all(&target).len(), 10);

    // Round 2: 10 more keys
    {
        let mut wtx = source.begin_write().unwrap();
        for i in 10u32..20 {
            wtx.insert(&i.to_be_bytes(), format!("r2-{i}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }
    sync_push(&source, &target, 1, false);
    assert_eq!(collect_all(&target).len(), 20);

    // Round 3: modify some existing keys
    {
        let mut wtx = source.begin_write().unwrap();
        for i in 0u32..5 {
            wtx.insert(&i.to_be_bytes(), format!("r3-{i}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }
    sync_push(&source, &target, 1, false);
    assert_eq!(collect_all(&source), collect_all(&target));
}

#[test]
fn large_patch_1000_entries() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    {
        let mut wtx = source.begin_write().unwrap();
        for i in 0u32..1000 {
            wtx.insert(&i.to_be_bytes(), format!("data-{i}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }

    let r1 = LocalTreeReader::new(source.manager());
    let r2 = LocalTreeReader::new(target.manager());
    let d = merkle_diff(&r1, &r2).unwrap();
    assert_eq!(d.subtrees_skipped, 0);

    let patch = SyncPatch::from_diff(NodeId::from_u64(1), &d, false);
    let bytes = patch.serialize().unwrap();
    let restored = SyncPatch::deserialize(&bytes).unwrap();
    apply_patch(target.manager(), &restored).unwrap();

    assert_eq!(collect_all(&source), collect_all(&target));
}

#[test]
fn persistence_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let s_path = dir.path().join("s.db");
    let t_path = dir.path().join("t.db");

    {
        let source = fast_builder(&s_path).create().unwrap();
        let target = fast_builder(&t_path).create().unwrap();

        let mut wtx = source.begin_write().unwrap();
        for i in 0u32..100 {
            wtx.insert(&i.to_be_bytes(), format!("persistent-{i}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();

        sync_push(&source, &target, 1, false);
    }

    let target = fast_builder(&t_path).open().unwrap();
    let data = collect_all(&target);
    assert_eq!(data.len(), 100);
    for i in 0u32..100 {
        assert_eq!(
            data[&i.to_be_bytes().to_vec()],
            format!("persistent-{i}").as_bytes()
        );
    }
}

#[test]
fn crdt_metadata_preserved_in_patch() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let m = meta(42, 7, 99);
    let val = encode_lww_value(&m, EntryKind::Put, b"crdt-data");

    {
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"crdt-key", &val).unwrap();
        wtx.commit().unwrap();
    }

    let r1 = LocalTreeReader::new(source.manager());
    let r2 = LocalTreeReader::new(target.manager());
    let d = merkle_diff(&r1, &r2).unwrap();

    let patch = SyncPatch::from_diff(NodeId::from_u64(99), &d, true);
    assert!(patch.crdt_aware);

    let entry = patch.entries.iter().find(|e| e.key == b"crdt-key").unwrap();
    assert_eq!(entry.kind, EntryKind::Put);
    let entry_meta = entry.crdt_meta.unwrap();
    assert_eq!(entry_meta.timestamp, m.timestamp);
    assert_eq!(entry_meta.node_id, m.node_id);
}

#[test]
fn tombstone_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let put_meta = meta(1, 0, 1);
    let tomb_meta = meta(2, 0, 2);
    let resurrect_meta = meta(3, 0, 1);

    // Target: put at t=1
    {
        let val = encode_lww_value(&put_meta, EntryKind::Put, b"alive");
        let mut wtx = target.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }

    // Round 1: tombstone at t=2 from source
    {
        let val = encode_lww_value(&tomb_meta, EntryKind::Tombstone, b"");
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }
    sync_push(&source, &target, 2, true);

    {
        let data = collect_all(&target);
        let decoded = decode_lww_value(&data[&b"key1".to_vec()]).unwrap();
        assert_eq!(decoded.kind, EntryKind::Tombstone);
    }

    // Round 2: resurrect at t=3
    {
        let val = encode_lww_value(&resurrect_meta, EntryKind::Put, b"resurrected");
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }
    sync_push(&source, &target, 1, true);

    let data = collect_all(&target);
    let decoded = decode_lww_value(&data[&b"key1".to_vec()]).unwrap();
    assert_eq!(decoded.kind, EntryKind::Put);
    assert_eq!(decoded.user_value, b"resurrected");
}

#[test]
fn mixed_crdt_conflicts_and_new_keys() {
    let dir = tempfile::tempdir().unwrap();
    let source = fast_builder(&dir.path().join("s.db")).create().unwrap();
    let target = fast_builder(&dir.path().join("t.db")).create().unwrap();

    let old_meta = meta(1, 0, 1);
    let new_meta = meta(3, 0, 2);

    // Target: key1 at t=1
    {
        let val = encode_lww_value(&old_meta, EntryKind::Put, b"old");
        let mut wtx = target.begin_write().unwrap();
        wtx.insert(b"key1", &val).unwrap();
        wtx.commit().unwrap();
    }

    // Source: key1 at t=3 (wins) + key2 (new)
    {
        let val1 = encode_lww_value(&new_meta, EntryKind::Put, b"updated");
        let val2 = encode_lww_value(&new_meta, EntryKind::Put, b"brand-new");
        let mut wtx = source.begin_write().unwrap();
        wtx.insert(b"key1", &val1).unwrap();
        wtx.insert(b"key2", &val2).unwrap();
        wtx.commit().unwrap();
    }

    let result = sync_push(&source, &target, 2, true);
    assert!(result.entries_applied >= 2);

    let data = collect_all(&target);
    assert_eq!(
        decode_lww_value(&data[&b"key1".to_vec()])
            .unwrap()
            .user_value,
        b"updated"
    );
    assert_eq!(
        decode_lww_value(&data[&b"key2".to_vec()])
            .unwrap()
            .user_value,
        b"brand-new"
    );
}
