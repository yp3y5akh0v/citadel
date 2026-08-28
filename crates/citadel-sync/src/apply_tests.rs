use super::*;
use crate::crdt::{encode_lww_value, CrdtMeta, EntryKind};
use crate::hlc::HlcTimestamp;
use crate::node_id::NodeId;
use crate::patch::PatchEntry;

use citadel_core::constants::{
    DEK_SIZE, MAC_KEY_SIZE, MAC_SIZE, MAX_INLINE_VALUE_SIZE, MAX_KEY_SIZE,
};
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

#[test]
fn non_crdt_tombstones_are_physical_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));

    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"default-key", b"value").unwrap();
    wtx.create_table(b"named").unwrap();
    wtx.table_insert(b"named", b"named-key", b"value").unwrap();
    wtx.commit().unwrap();

    let delete = |key: &[u8]| SyncPatch {
        source_node: NodeId::from_u64(1),
        entries: vec![PatchEntry {
            key: key.to_vec(),
            value: Vec::new(),
            kind: EntryKind::Tombstone,
            crdt_meta: None,
        }],
        crdt_aware: false,
    };

    apply_patch(&mgr, &delete(b"default-key")).unwrap();
    apply_patch_to_table(&mgr, b"named", &delete(b"named-key")).unwrap();

    let mut rtx = mgr.begin_read();
    assert!(rtx.get(b"default-key").unwrap().is_none());
    assert!(rtx.table_get(b"named", b"named-key").unwrap().is_none());
}

fn crdt_patch(key: &[u8], value: Vec<u8>, kind: EntryKind, meta: CrdtMeta) -> SyncPatch {
    SyncPatch {
        source_node: NodeId::from_u64(9),
        entries: vec![PatchEntry {
            key: key.to_vec(),
            value,
            kind,
            crdt_meta: Some(meta),
        }],
        crdt_aware: true,
    }
}

#[test]
fn contradictory_crdt_tombstone_cannot_resurrect_default_or_named_data() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));
    let old_meta = meta(100 * SECOND, 0, 1);
    let claimed_meta = meta(200 * SECOND, 0, 2);
    let original = encode_lww_value(&old_meta, EntryKind::Put, b"keep");

    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"k", &original).unwrap();
    wtx.create_table(b"named").unwrap();
    wtx.table_insert(b"named", b"k", &original).unwrap();
    wtx.commit().unwrap();

    // The outer entry claims a newer tombstone while the stored bytes encode an
    // older put; applying those bytes resurrects the key.
    let contradictory = crdt_patch(
        b"k",
        encode_lww_value(&old_meta, EntryKind::Put, b"resurrect"),
        EntryKind::Tombstone,
        claimed_meta,
    );
    assert!(matches!(
        apply_patch(&mgr, &contradictory),
        Err(Error::Sync(_))
    ));
    assert!(matches!(
        apply_patch_to_table(&mgr, b"named", &contradictory),
        Err(Error::Sync(_))
    ));

    let mut rtx = mgr.begin_read();
    assert_eq!(rtx.get(b"k").unwrap().as_deref(), Some(original.as_slice()));
    assert_eq!(
        rtx.table_get(b"named", b"k").unwrap().as_deref(),
        Some(original.as_slice())
    );
}

#[test]
fn equal_metadata_uses_canonical_bytes_as_a_total_order() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));
    let m = meta(500 * SECOND, 3, 7);
    let low = encode_lww_value(&m, EntryKind::Put, b"a");
    let high = encode_lww_value(&m, EntryKind::Put, b"z");
    let tombstone = encode_lww_value(&m, EntryKind::Tombstone, b"");

    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"remote-high", &low).unwrap();
    wtx.insert(b"local-high", &high).unwrap();
    wtx.create_table(b"named").unwrap();
    wtx.table_insert(b"named", b"remote-delete", &low).unwrap();
    wtx.table_insert(b"named", b"local-delete", &tombstone)
        .unwrap();
    wtx.commit().unwrap();

    let remote_high = crdt_patch(b"remote-high", high.clone(), EntryKind::Put, m);
    let local_high = crdt_patch(b"local-high", low, EntryKind::Put, m);
    assert_eq!(apply_patch(&mgr, &remote_high).unwrap().entries_applied, 1);
    assert_eq!(apply_patch(&mgr, &local_high).unwrap().entries_skipped, 1);

    let remote_delete = crdt_patch(b"remote-delete", tombstone.clone(), EntryKind::Tombstone, m);
    let local_delete = crdt_patch(
        b"local-delete",
        encode_lww_value(&m, EntryKind::Put, b"z"),
        EntryKind::Put,
        m,
    );
    assert_eq!(
        apply_patch_to_table(&mgr, b"named", &remote_delete)
            .unwrap()
            .entries_applied,
        1
    );
    assert_eq!(
        apply_patch_to_table(&mgr, b"named", &local_delete)
            .unwrap()
            .entries_skipped,
        1
    );

    let mut rtx = mgr.begin_read();
    assert_eq!(
        rtx.get(b"remote-high").unwrap().as_deref(),
        Some(high.as_slice())
    );
    assert_eq!(
        rtx.get(b"local-high").unwrap().as_deref(),
        Some(high.as_slice())
    );
    assert_eq!(
        rtx.table_get(b"named", b"remote-delete")
            .unwrap()
            .as_deref(),
        Some(tombstone.as_slice())
    );
    assert_eq!(
        rtx.table_get(b"named", b"local-delete").unwrap().as_deref(),
        Some(tombstone.as_slice())
    );
}

#[test]
fn equal_metadata_repairs_noncanonical_local_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));
    let m = meta(700 * SECOND, 0, 4);
    let canonical = encode_lww_value(&m, EntryKind::Put, b"same");
    let mut noncanonical = canonical.clone();
    noncanonical[1] = 1;

    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"k", &noncanonical).unwrap();
    wtx.commit().unwrap();

    let result = apply_patch(
        &mgr,
        &crdt_patch(b"k", canonical.clone(), EntryKind::Put, m),
    )
    .unwrap();
    assert_eq!(result.entries_equal, 1);
    assert_eq!(mgr.begin_read().get(b"k").unwrap().unwrap(), canonical);
}

#[test]
fn direct_apply_prevalidates_the_whole_patch_before_mutating() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));
    let patch = SyncPatch {
        source_node: NodeId::from_u64(1),
        entries: vec![
            PatchEntry {
                key: b"would-have-landed".to_vec(),
                value: b"value".to_vec(),
                kind: EntryKind::Put,
                crdt_meta: None,
            },
            PatchEntry {
                key: vec![b'x'; MAX_KEY_SIZE + 1],
                value: b"invalid".to_vec(),
                kind: EntryKind::Put,
                crdt_meta: None,
            },
        ],
        crdt_aware: false,
    };

    let mut wtx = mgr.begin_write().unwrap();
    assert!(matches!(
        apply_patch_to_txn(&mut wtx, &patch),
        Err(Error::Sync(_))
    ));
    // Validation failed before the first entry, so the caller's transaction
    // remains clean and committable.
    wtx.commit().unwrap();
    assert!(mgr
        .begin_read()
        .get(b"would-have-landed")
        .unwrap()
        .is_none());
}

#[test]
fn generation_checked_apply_returns_exact_noop_generation_and_rejects_stale() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));
    let m = meta(900 * SECOND, 0, 1);
    let value = encode_lww_value(&m, EntryKind::Put, b"same");
    let mut wtx = mgr.begin_write().unwrap();
    wtx.insert(b"k", &value).unwrap();
    wtx.commit().unwrap();

    let expected = mgr.commit_generation();
    let patch = crdt_patch(b"k", value, EntryKind::Put, m);
    let Some((result, actual)) = apply_patch_if_generation(&mgr, &patch, expected).unwrap() else {
        panic!("unchanged generation must admit the writer");
    };
    assert_eq!(result.entries_equal, 1);
    assert_eq!(actual, expected, "a physical no-op must not invent +1");

    let mut concurrent = mgr.begin_write().unwrap();
    concurrent.insert(b"newer", b"write").unwrap();
    concurrent.commit().unwrap();
    assert!(apply_patch_if_generation(&mgr, &patch, expected)
        .unwrap()
        .is_none());
}

#[test]
fn read_only_write_transaction_does_not_advance_generation() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));
    let overflow = vec![0xA5; MAX_INLINE_VALUE_SIZE + 1];
    let mut setup = mgr.begin_write().unwrap();
    setup.insert(b"inline", b"value").unwrap();
    setup.insert(b"overflow", &overflow).unwrap();
    setup.create_table(b"named").unwrap();
    setup
        .table_insert(b"named", b"inline", b"named-value")
        .unwrap();
    setup.commit().unwrap();

    let expected = mgr.commit_generation();
    let mut read_only = mgr.begin_write().unwrap();
    assert_eq!(
        read_only.get(b"inline").unwrap().as_deref(),
        Some(b"value".as_slice())
    );
    assert_eq!(
        read_only.get(b"overflow").unwrap().as_deref(),
        Some(overflow.as_slice())
    );
    assert_eq!(
        read_only.table_get(b"named", b"inline").unwrap().as_deref(),
        Some(b"named-value".as_slice())
    );
    assert_eq!(read_only.commit_with_generation().unwrap(), expected);
    assert_eq!(mgr.commit_generation(), expected);

    let mut replacement = mgr.begin_write().unwrap();
    replacement.insert(b"inline", b"changed").unwrap();
    assert_eq!(replacement.commit_with_generation().unwrap(), expected + 1);

    let mut deletion = mgr.begin_write().unwrap();
    deletion.delete(b"inline").unwrap();
    assert_eq!(deletion.commit_with_generation().unwrap(), expected + 2);

    let mut catalog = mgr.begin_write().unwrap();
    catalog.create_table(b"another").unwrap();
    assert_eq!(catalog.commit_with_generation().unwrap(), expected + 3);
}

#[test]
fn value_beyond_the_frame_limit_fails_before_any_prefix_write() {
    let dir = tempfile::tempdir().unwrap();
    let mgr = test_manager(&dir.path().join("test.db"));
    let patch = SyncPatch {
        source_node: NodeId::from_u64(1),
        entries: vec![
            PatchEntry {
                key: b"prefix".to_vec(),
                value: b"must-not-land".to_vec(),
                kind: EntryKind::Put,
                crdt_meta: None,
            },
            PatchEntry {
                key: b"too-large".to_vec(),
                value: vec![0; crate::protocol::MAX_SYNC_VALUE_SIZE + 1],
                kind: EntryKind::Put,
                crdt_meta: None,
            },
        ],
        crdt_aware: false,
    };

    let mut txn = mgr.begin_write().unwrap();
    assert!(matches!(
        apply_patch_to_txn(&mut txn, &patch),
        Err(Error::Sync(_))
    ));
    txn.commit().unwrap();
    assert!(mgr.begin_read().get(b"prefix").unwrap().is_none());
}
