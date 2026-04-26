use super::*;
use citadel_core::types::PageType;

fn sample_hash() -> MerkleHash {
    let mut h = [0u8; MERKLE_HASH_SIZE];
    for (i, byte) in h.iter_mut().enumerate() {
        *byte = i as u8;
    }
    h
}

#[test]
fn hello_roundtrip() {
    let msg = SyncMessage::Hello {
        node_id: NodeId::from_u64(42),
        root_page: PageId(7),
        root_hash: sample_hash(),
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::Hello {
            node_id,
            root_page,
            root_hash,
        } => {
            assert_eq!(node_id, NodeId::from_u64(42));
            assert_eq!(root_page, PageId(7));
            assert_eq!(root_hash, sample_hash());
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn hello_ack_roundtrip() {
    let msg = SyncMessage::HelloAck {
        node_id: NodeId::from_u64(99),
        root_page: PageId(3),
        root_hash: sample_hash(),
        in_sync: true,
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::HelloAck {
            node_id,
            root_page,
            root_hash,
            in_sync,
        } => {
            assert_eq!(node_id, NodeId::from_u64(99));
            assert_eq!(root_page, PageId(3));
            assert_eq!(root_hash, sample_hash());
            assert!(in_sync);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn digest_request_roundtrip() {
    let msg = SyncMessage::DigestRequest {
        page_ids: vec![PageId(1), PageId(5), PageId(100)],
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::DigestRequest { page_ids } => {
            assert_eq!(page_ids, vec![PageId(1), PageId(5), PageId(100)]);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn digest_response_roundtrip() {
    let msg = SyncMessage::DigestResponse {
        digests: vec![
            PageDigest {
                page_id: PageId(1),
                page_type: PageType::Leaf,
                merkle_hash: sample_hash(),
                children: vec![],
            },
            PageDigest {
                page_id: PageId(2),
                page_type: PageType::Branch,
                merkle_hash: [0xAA; MERKLE_HASH_SIZE],
                children: vec![PageId(3), PageId(4)],
            },
        ],
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::DigestResponse { digests } => {
            assert_eq!(digests.len(), 2);
            assert_eq!(digests[0].page_id, PageId(1));
            assert!(digests[0].children.is_empty());
            assert_eq!(digests[1].children, vec![PageId(3), PageId(4)]);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn entries_request_roundtrip() {
    let msg = SyncMessage::EntriesRequest {
        page_ids: vec![PageId(10)],
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::EntriesRequest { page_ids } => {
            assert_eq!(page_ids, vec![PageId(10)]);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn entries_response_roundtrip() {
    let msg = SyncMessage::EntriesResponse {
        entries: vec![
            DiffEntry {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                val_type: 0,
            },
            DiffEntry {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                val_type: 1,
            },
        ],
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::EntriesResponse { entries } => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].key, b"k1");
            assert_eq!(entries[1].val_type, 1);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn patch_data_roundtrip() {
    let msg = SyncMessage::PatchData {
        data: vec![1, 2, 3, 4, 5],
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::PatchData { data: d } => {
            assert_eq!(d, vec![1, 2, 3, 4, 5]);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn patch_ack_roundtrip() {
    let msg = SyncMessage::PatchAck {
        result: ApplyResult {
            entries_applied: 10,
            entries_skipped: 3,
            entries_equal: 2,
        },
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::PatchAck { result } => {
            assert_eq!(result.entries_applied, 10);
            assert_eq!(result.entries_skipped, 3);
            assert_eq!(result.entries_equal, 2);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn done_roundtrip() {
    let data = SyncMessage::Done.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    assert!(matches!(decoded, SyncMessage::Done));
}

#[test]
fn error_roundtrip() {
    let msg = SyncMessage::Error {
        message: "something broke".into(),
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::Error { message } => {
            assert_eq!(message, "something broke");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn pull_request_roundtrip() {
    let data = SyncMessage::PullRequest.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    assert!(matches!(decoded, SyncMessage::PullRequest));
}

#[test]
fn pull_response_roundtrip() {
    let msg = SyncMessage::PullResponse {
        root_page: PageId(15),
        root_hash: sample_hash(),
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::PullResponse {
            root_page,
            root_hash,
        } => {
            assert_eq!(root_page, PageId(15));
            assert_eq!(root_hash, sample_hash());
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn truncated_data() {
    let err = SyncMessage::deserialize(&[0, 1]).unwrap_err();
    assert!(matches!(err, ProtocolError::Truncated { .. }));
}

#[test]
fn unknown_message_type() {
    let data = [255, 0, 0, 0, 0];
    let err = SyncMessage::deserialize(&data).unwrap_err();
    assert!(matches!(err, ProtocolError::UnknownMessageType(255)));
}

#[test]
fn empty_digest_request() {
    let msg = SyncMessage::DigestRequest { page_ids: vec![] };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::DigestRequest { page_ids } => assert!(page_ids.is_empty()),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn table_list_request_roundtrip() {
    let data = SyncMessage::TableListRequest.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    assert!(matches!(decoded, SyncMessage::TableListRequest));
}

#[test]
fn table_list_response_roundtrip() {
    let msg = SyncMessage::TableListResponse {
        tables: vec![
            TableInfo {
                name: b"users".to_vec(),
                root_page: PageId(10),
                root_hash: sample_hash(),
            },
            TableInfo {
                name: b"orders".to_vec(),
                root_page: PageId(20),
                root_hash: [0xBB; MERKLE_HASH_SIZE],
            },
        ],
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::TableListResponse { tables } => {
            assert_eq!(tables.len(), 2);
            assert_eq!(tables[0].name, b"users");
            assert_eq!(tables[0].root_page, PageId(10));
            assert_eq!(tables[0].root_hash, sample_hash());
            assert_eq!(tables[1].name, b"orders");
            assert_eq!(tables[1].root_page, PageId(20));
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn table_list_response_empty() {
    let msg = SyncMessage::TableListResponse { tables: vec![] };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::TableListResponse { tables } => assert!(tables.is_empty()),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn table_sync_begin_roundtrip() {
    let msg = SyncMessage::TableSyncBegin {
        table_name: b"products".to_vec(),
        root_page: PageId(77),
        root_hash: sample_hash(),
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::TableSyncBegin {
            table_name,
            root_page,
            root_hash,
        } => {
            assert_eq!(table_name, b"products");
            assert_eq!(root_page, PageId(77));
            assert_eq!(root_hash, sample_hash());
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn table_sync_end_roundtrip() {
    let msg = SyncMessage::TableSyncEnd {
        table_name: b"products".to_vec(),
    };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::TableSyncEnd { table_name } => {
            assert_eq!(table_name, b"products");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn empty_entries_response() {
    let msg = SyncMessage::EntriesResponse { entries: vec![] };
    let data = msg.serialize();
    let decoded = SyncMessage::deserialize(&data).unwrap();
    match decoded {
        SyncMessage::EntriesResponse { entries } => assert!(entries.is_empty()),
        _ => panic!("wrong variant"),
    }
}
