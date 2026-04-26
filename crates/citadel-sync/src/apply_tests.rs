use super::*;
use crate::crdt::{encode_lww_value, CrdtMeta, EntryKind};
use crate::hlc::HlcTimestamp;
use crate::node_id::NodeId;
use crate::patch::PatchEntry;

use citadel_core::constants::{DEK_SIZE, MAC_KEY_SIZE, MAC_SIZE};
use citadel_io::mmap_io::MmapPageIO;

const SECOND: i64 = 1_000_000_000;

fn meta(wall_ns: i64, logical: i32, node: u64) -> CrdtMeta {
    CrdtMeta::new(HlcTimestamp::new(wall_ns, logical), NodeId::from_u64(node))
}

fn test_manager(path: &std::path::Path) -> TxnManager {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap();
    let io = Box::new(MmapPageIO::try_new(file).unwrap());
    let dek = [0x42u8; DEK_SIZE];
    let mac_key = [0x43u8; MAC_KEY_SIZE];
    let dek_id = [0x44u8; MAC_SIZE];
    TxnManager::create(io, dek, mac_key, 1, 0x1234, dek_id, 256).unwrap()
}

#[test]
fn apply_empty_patch() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));
    let patch = SyncPatch::empty(NodeId::from_u64(1));
    let result = apply_patch(&mgr, &patch).unwrap();
    assert_eq!(result, ApplyResult::empty());
}

#[test]
fn apply_non_crdt_unconditional() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));

    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"key1", b"old-value").unwrap();
    wtx.commit().unwrap();

    let patch = SyncPatch {
        source_node: NodeId::from_u64(1),
        entries: vec![
            PatchEntry {
                key: b"key1".to_vec(),
                value: b"new-value".to_vec(),
                kind: EntryKind::Put,
                crdt_meta: None,
            },
            PatchEntry {
                key: b"key2".to_vec(),
                value: b"brand-new".to_vec(),
                kind: EntryKind::Put,
                crdt_meta: None,
            },
        ],
        crdt_aware: false,
    };

    let result = apply_patch(&mgr, &patch).unwrap();
    assert_eq!(result.entries_applied, 2);

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"key1").unwrap().unwrap(), b"new-value");
    assert_eq!(rtx.get(b"key2").unwrap().unwrap(), b"brand-new");
}

#[test]
fn apply_crdt_remote_wins() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));

    let local_meta = meta(1000 * SECOND, 0, 1);
    let remote_meta = meta(2000 * SECOND, 0, 2);

    let local_val = encode_lww_value(&local_meta, EntryKind::Put, b"local");
    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"key1", &local_val).unwrap();
    wtx.commit().unwrap();

    let remote_val = encode_lww_value(&remote_meta, EntryKind::Put, b"remote");
    let patch = SyncPatch {
        source_node: NodeId::from_u64(2),
        entries: vec![PatchEntry {
            key: b"key1".to_vec(),
            value: remote_val.clone(),
            kind: EntryKind::Put,
            crdt_meta: Some(remote_meta),
        }],
        crdt_aware: true,
    };

    let result = apply_patch(&mgr, &patch).unwrap();
    assert_eq!(result.entries_applied, 1);
    assert_eq!(result.entries_skipped, 0);

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"key1").unwrap().unwrap(), remote_val);
}

#[test]
fn apply_crdt_local_wins() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));

    let local_meta = meta(2000 * SECOND, 0, 1);
    let remote_meta = meta(1000 * SECOND, 0, 2);

    let local_val = encode_lww_value(&local_meta, EntryKind::Put, b"local");
    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"key1", &local_val).unwrap();
    wtx.commit().unwrap();

    let remote_val = encode_lww_value(&remote_meta, EntryKind::Put, b"remote");
    let patch = SyncPatch {
        source_node: NodeId::from_u64(2),
        entries: vec![PatchEntry {
            key: b"key1".to_vec(),
            value: remote_val,
            kind: EntryKind::Put,
            crdt_meta: Some(remote_meta),
        }],
        crdt_aware: true,
    };

    let result = apply_patch(&mgr, &patch).unwrap();
    assert_eq!(result.entries_applied, 0);
    assert_eq!(result.entries_skipped, 1);

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"key1").unwrap().unwrap(), local_val);
}

#[test]
fn apply_crdt_equal() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));

    let m = meta(1000 * SECOND, 5, 42);
    let val = encode_lww_value(&m, EntryKind::Put, b"same");

    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"key1", &val).unwrap();
    wtx.commit().unwrap();

    let patch = SyncPatch {
        source_node: NodeId::from_u64(42),
        entries: vec![PatchEntry {
            key: b"key1".to_vec(),
            value: val.clone(),
            kind: EntryKind::Put,
            crdt_meta: Some(m),
        }],
        crdt_aware: true,
    };

    let result = apply_patch(&mgr, &patch).unwrap();
    assert_eq!(result.entries_equal, 1);
    assert_eq!(result.entries_applied, 0);
}

#[test]
fn apply_crdt_new_key() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));

    let m = meta(1000 * SECOND, 0, 1);
    let val = encode_lww_value(&m, EntryKind::Put, b"new");

    let patch = SyncPatch {
        source_node: NodeId::from_u64(1),
        entries: vec![PatchEntry {
            key: b"new-key".to_vec(),
            value: val.clone(),
            kind: EntryKind::Put,
            crdt_meta: Some(m),
        }],
        crdt_aware: true,
    };

    let result = apply_patch(&mgr, &patch).unwrap();
    assert_eq!(result.entries_applied, 1);

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"new-key").unwrap().unwrap(), val);
}

#[test]
fn apply_crdt_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));

    let local_meta = meta(1000 * SECOND, 0, 1);
    let local_val = encode_lww_value(&local_meta, EntryKind::Put, b"alive");
    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"key1", &local_val).unwrap();
    wtx.commit().unwrap();

    let remote_meta = meta(2000 * SECOND, 0, 2);
    let tombstone_val = encode_lww_value(&remote_meta, EntryKind::Tombstone, b"");

    let patch = SyncPatch {
        source_node: NodeId::from_u64(2),
        entries: vec![PatchEntry {
            key: b"key1".to_vec(),
            value: tombstone_val.clone(),
            kind: EntryKind::Tombstone,
            crdt_meta: Some(remote_meta),
        }],
        crdt_aware: true,
    };

    let result = apply_patch(&mgr, &patch).unwrap();
    assert_eq!(result.entries_applied, 1);

    let mut rtx = mgr.begin_read();
    let stored = rtx.get(b"key1").unwrap().unwrap();
    let decoded = decode_lww_value(&stored).unwrap();
    assert_eq!(decoded.kind, EntryKind::Tombstone);
}

#[test]
fn apply_to_txn() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));

    let patch = SyncPatch {
        source_node: NodeId::from_u64(1),
        entries: vec![PatchEntry {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            kind: EntryKind::Put,
            crdt_meta: None,
        }],
        crdt_aware: false,
    };

    let mut wtx = mgr.begin_write().unwrap();
    let result = apply_patch_to_txn(&mut wtx, &patch).unwrap();
    assert_eq!(result.entries_applied, 1);
    wtx.commit().unwrap();

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"k").unwrap().unwrap(), b"v");
}
