use super::*;
use crate::hlc::HlcTimestamp;

fn meta(wall_ns: i64, logical: i32, node: u64) -> CrdtMeta {
    CrdtMeta::new(HlcTimestamp::new(wall_ns, logical), NodeId::from_u64(node))
}

#[test]
fn empty_patch_roundtrip() {
    let patch = SyncPatch::empty(NodeId::from_u64(42));
    let data = patch.serialize().unwrap();
    let decoded = SyncPatch::deserialize(&data).unwrap();
    assert!(decoded.is_empty());
    assert_eq!(decoded.source_node, NodeId::from_u64(42));
    assert!(!decoded.crdt_aware);
}

#[test]
fn patch_magic_and_version_wire_bytes_are_frozen() {
    let data = SyncPatch::empty(NodeId::from_u64(42)).serialize().unwrap();

    assert_eq!(&data[..5], b"CNYS\x02");
}

#[test]
fn patch_with_entries_roundtrip() {
    let patch = SyncPatch {
        source_node: NodeId::from_u64(1),
        entries: vec![
            PatchEntry {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                kind: EntryKind::Put,
                crdt_meta: None,
            },
            PatchEntry {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
                kind: EntryKind::Put,
                crdt_meta: None,
            },
        ],
        crdt_aware: false,
    };

    let data = patch.serialize().unwrap();
    let decoded = SyncPatch::deserialize(&data).unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded.entries[0].key, b"key1");
    assert_eq!(decoded.entries[0].value, b"value1");
    assert_eq!(decoded.entries[1].key, b"key2");
    assert_eq!(decoded.entries[1].value, b"value2");
}

#[test]
fn crdt_patch_roundtrip() {
    let m = meta(1_000_000_000, 5, 42);
    let patch = SyncPatch {
        source_node: NodeId::from_u64(1),
        entries: vec![
            PatchEntry {
                key: b"key1".to_vec(),
                value: crate::crdt::encode_lww_value(&m, EntryKind::Put, b"value1"),
                kind: EntryKind::Put,
                crdt_meta: Some(m),
            },
            PatchEntry {
                key: b"key2".to_vec(),
                value: crate::crdt::encode_lww_value(&m, EntryKind::Tombstone, b""),
                kind: EntryKind::Tombstone,
                crdt_meta: Some(m),
            },
        ],
        crdt_aware: true,
    };

    let data = patch.serialize().unwrap();
    let decoded = SyncPatch::deserialize(&data).unwrap();
    assert_eq!(decoded.len(), 2);
    assert!(decoded.crdt_aware);
    assert_eq!(decoded.entries[0].crdt_meta, Some(m));
    assert_eq!(decoded.entries[0].kind, EntryKind::Put);
    assert_eq!(decoded.entries[1].crdt_meta, Some(m));
    assert_eq!(decoded.entries[1].kind, EntryKind::Tombstone);
}

#[test]
fn large_values_roundtrip() {
    let big_key = vec![0xAA; 2048];
    let big_val = vec![0xBB; 8192];
    let patch = SyncPatch {
        source_node: NodeId::from_u64(99),
        entries: vec![PatchEntry {
            key: big_key.clone(),
            value: big_val.clone(),
            kind: EntryKind::Put,
            crdt_meta: None,
        }],
        crdt_aware: false,
    };

    let data = patch.serialize().unwrap();
    let decoded = SyncPatch::deserialize(&data).unwrap();
    assert_eq!(decoded.entries[0].key, big_key);
    assert_eq!(decoded.entries[0].value, big_val);
}

#[test]
fn invalid_magic_error() {
    let mut data = SyncPatch::empty(NodeId::from_u64(1)).serialize().unwrap();
    data[0] = 0xFF; // corrupt magic
    let err = SyncPatch::deserialize(&data).unwrap_err();
    assert!(matches!(err, PatchError::InvalidMagic { .. }));
}

#[test]
fn unsupported_version_error() {
    let mut data = SyncPatch::empty(NodeId::from_u64(1)).serialize().unwrap();
    data[4] = 99; // bad version
    let err = SyncPatch::deserialize(&data).unwrap_err();
    assert!(matches!(err, PatchError::UnsupportedVersion(99)));
}

#[test]
fn version_one_physical_overflow_patches_are_rejected() {
    let mut data = SyncPatch::empty(NodeId::from_u64(1)).serialize().unwrap();
    assert_eq!(data[4], 2);
    data[4] = 1;

    assert!(matches!(
        SyncPatch::deserialize(&data),
        Err(PatchError::UnsupportedVersion(1))
    ));
}

#[test]
fn truncated_header_error() {
    let err = SyncPatch::deserialize(&[0u8; 5]).unwrap_err();
    assert!(matches!(err, PatchError::Truncated { .. }));
}

#[test]
fn truncated_entry_error() {
    let patch = SyncPatch {
        source_node: NodeId::from_u64(1),
        entries: vec![PatchEntry {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            kind: EntryKind::Put,
            crdt_meta: None,
        }],
        crdt_aware: false,
    };
    let mut data = patch.serialize().unwrap();
    data.truncate(data.len() - 3); // cut off end of value
    let err = SyncPatch::deserialize(&data).unwrap_err();
    assert!(matches!(err, PatchError::Truncated { .. }));
}

#[test]
fn invalid_entry_kind_error() {
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
    let mut data = patch.serialize().unwrap();
    data[18 + 6] = 255;
    let err = SyncPatch::deserialize(&data).unwrap_err();
    assert!(matches!(err, PatchError::InvalidEntryKind(255)));
}

#[test]
fn many_entries_roundtrip() {
    let entries: Vec<PatchEntry> = (0..1000u32)
        .map(|i| PatchEntry {
            key: i.to_be_bytes().to_vec(),
            value: format!("val-{i}").into_bytes(),
            kind: EntryKind::Put,
            crdt_meta: None,
        })
        .collect();

    let patch = SyncPatch {
        source_node: NodeId::from_u64(7),
        entries,
        crdt_aware: false,
    };

    let data = patch.serialize().unwrap();
    let decoded = SyncPatch::deserialize(&data).unwrap();
    assert_eq!(decoded.len(), 1000);
    for (i, entry) in decoded.entries.iter().enumerate() {
        assert_eq!(entry.key, (i as u32).to_be_bytes());
    }
}

#[test]
fn from_diff_non_crdt() {
    let diff = DiffResult {
        entries: vec![
            crate::diff::DiffEntry {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                val_type: 0,
            },
            crate::diff::DiffEntry {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                val_type: 0,
            },
        ],
        pages_compared: 5,
        subtrees_skipped: 2,
    };

    let patch = SyncPatch::from_diff(NodeId::from_u64(1), &diff, false);
    assert_eq!(patch.len(), 2);
    assert!(!patch.crdt_aware);
    assert_eq!(patch.entries[0].key, b"k1");
    assert!(patch.entries[0].crdt_meta.is_none());
}

#[test]
fn impossible_entry_count_is_rejected_before_allocation() {
    let mut data = SyncPatch::empty(NodeId::from_u64(1)).serialize().unwrap();
    data[14..18].copy_from_slice(&u32::MAX.to_le_bytes());

    assert!(matches!(
        SyncPatch::deserialize(&data),
        Err(PatchError::InvalidEntryCount { .. })
    ));
}

#[test]
fn crdt_wire_metadata_and_kind_must_match_the_encoded_value() {
    let encoded_meta = meta(1, 0, 1);
    let claimed_meta = meta(2, 0, 2);
    let patch = SyncPatch {
        source_node: NodeId::from_u64(2),
        entries: vec![PatchEntry {
            key: b"victim".to_vec(),
            value: crate::crdt::encode_lww_value(&encoded_meta, EntryKind::Put, b"resurrect"),
            kind: EntryKind::Tombstone,
            crdt_meta: Some(claimed_meta),
        }],
        crdt_aware: true,
    };

    assert!(matches!(
        SyncPatch::deserialize(&patch.serialize_unchecked()),
        Err(PatchError::InvalidCrdtEntry { .. })
    ));
}

#[test]
fn from_diff_preserves_a_physical_tombstone() {
    let diff = DiffResult {
        entries: vec![crate::diff::DiffEntry {
            key: b"removed".to_vec(),
            value: b"not-a-logical-value".to_vec(),
            val_type: citadel_core::types::ValueType::Tombstone as u8,
        }],
        pages_compared: 1,
        subtrees_skipped: 0,
    };

    let patch = SyncPatch::from_diff(NodeId::from_u64(1), &diff, false);
    assert_eq!(patch.entries[0].kind, EntryKind::Tombstone);
    assert!(patch.entries[0].value.is_empty());
    assert!(patch.entries[0].crdt_meta.is_none());
}

#[test]
fn from_diff_crdt_extracts_meta() {
    let m = meta(1_000_000_000, 5, 42);
    let crdt_value = crate::crdt::encode_lww_value(&m, EntryKind::Put, b"user-data");

    let diff = DiffResult {
        entries: vec![crate::diff::DiffEntry {
            key: b"k1".to_vec(),
            value: crdt_value,
            val_type: 0,
        }],
        pages_compared: 1,
        subtrees_skipped: 0,
    };

    let patch = SyncPatch::from_diff(NodeId::from_u64(1), &diff, true);
    assert_eq!(patch.len(), 1);
    assert!(patch.crdt_aware);
    assert_eq!(patch.entries[0].crdt_meta, Some(m));
    assert_eq!(patch.entries[0].kind, EntryKind::Put);
}

#[test]
fn hostile_declared_value_length_is_rejected_before_copying() {
    let mut data = SyncPatch::empty(NodeId::from_u64(1)).serialize().unwrap();
    data[14..18].copy_from_slice(&1u32.to_le_bytes());
    data.extend_from_slice(&1u16.to_le_bytes());
    data.extend_from_slice(&u32::MAX.to_le_bytes());
    data.push(EntryKind::Put as u8);
    data.push(b'k');

    assert!(matches!(
        SyncPatch::deserialize(&data),
        Err(PatchError::InvalidEntry { .. })
    ));
}

#[test]
fn trailing_patch_bytes_are_rejected() {
    let mut data = SyncPatch::empty(NodeId::from_u64(1)).serialize().unwrap();
    data.push(0xAA);
    assert!(matches!(
        SyncPatch::deserialize(&data),
        Err(PatchError::TrailingData { .. })
    ));
}

#[test]
fn public_serializer_rejects_a_non_roundtrippable_key() {
    let patch = SyncPatch {
        source_node: NodeId::from_u64(1),
        entries: vec![PatchEntry {
            key: vec![b'k'; citadel_core::MAX_KEY_SIZE + 1],
            value: Vec::new(),
            kind: EntryKind::Put,
            crdt_meta: None,
        }],
        crdt_aware: false,
    };
    assert!(matches!(
        patch.serialize(),
        Err(PatchError::InvalidEntry { .. })
    ));
}
