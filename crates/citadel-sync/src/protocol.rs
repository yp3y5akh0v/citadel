use citadel_core::types::PageId;
use citadel_core::MERKLE_HASH_SIZE;

use crate::apply::ApplyResult;
use crate::diff::{DiffEntry, MerkleHash, PageDigest};
use crate::node_id::NodeId;

/// Message type tags for wire format.
const MSG_HELLO: u8 = 0;
const MSG_HELLO_ACK: u8 = 1;
const MSG_DIGEST_REQUEST: u8 = 2;
const MSG_DIGEST_RESPONSE: u8 = 3;
const MSG_ENTRIES_REQUEST: u8 = 4;
const MSG_ENTRIES_RESPONSE: u8 = 5;
const MSG_PATCH_DATA: u8 = 6;
const MSG_PATCH_ACK: u8 = 7;
const MSG_DONE: u8 = 8;
const MSG_ERROR: u8 = 9;
const MSG_PULL_REQUEST: u8 = 10;
const MSG_PULL_RESPONSE: u8 = 11;
const MSG_TABLE_LIST_REQUEST: u8 = 12;
const MSG_TABLE_LIST_RESPONSE: u8 = 13;
const MSG_TABLE_SYNC_BEGIN: u8 = 14;
const MSG_TABLE_SYNC_END: u8 = 15;

/// Metadata about a named table for multi-table sync negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub name: Vec<u8>,
    pub root_page: PageId,
    pub root_hash: MerkleHash,
}

/// Sync protocol messages exchanged between initiator and responder.
#[derive(Debug, Clone)]
pub enum SyncMessage {
    /// Initiator greeting with identity and tree root state.
    Hello {
        node_id: NodeId,
        root_page: PageId,
        root_hash: MerkleHash,
    },
    /// Responder acknowledgment with its own tree root state.
    HelloAck {
        node_id: NodeId,
        root_page: PageId,
        root_hash: MerkleHash,
        in_sync: bool,
    },
    /// Request page digests from the remote tree.
    DigestRequest { page_ids: Vec<PageId> },
    /// Response with page digests.
    DigestResponse { digests: Vec<PageDigest> },
    /// Request leaf entries from remote pages.
    EntriesRequest { page_ids: Vec<PageId> },
    /// Response with leaf entries.
    EntriesResponse { entries: Vec<DiffEntry> },
    /// Serialized SyncPatch data.
    PatchData { data: Vec<u8> },
    /// Acknowledgment after applying a patch.
    PatchAck { result: ApplyResult },
    /// Session complete.
    Done,
    /// Error during sync.
    Error { message: String },
    /// Request updated root info for pull phase after push.
    PullRequest,
    /// Response with updated root info for pull phase.
    PullResponse {
        root_page: PageId,
        root_hash: MerkleHash,
    },
    /// Request list of named tables from the remote peer.
    TableListRequest,
    /// Response with the list of named tables.
    TableListResponse { tables: Vec<TableInfo> },
    /// Begin syncing a specific named table.
    TableSyncBegin {
        table_name: Vec<u8>,
        root_page: PageId,
        root_hash: MerkleHash,
    },
    /// End syncing a specific named table.
    TableSyncEnd { table_name: Vec<u8> },
}

/// Errors from sync message serialization/deserialization.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("{context}: expected at least {expected} bytes, got {actual}")]
    Truncated {
        context: String,
        expected: usize,
        actual: usize,
    },

    #[error("unknown message type: {0}")]
    UnknownMessageType(u8),
}

impl SyncMessage {
    /// Serialize to wire format: `[msg_type: u8][payload_len: u32 LE][payload]`.
    pub fn serialize(&self) -> Vec<u8> {
        let (msg_type, payload) = match self {
            SyncMessage::Hello {
                node_id,
                root_page,
                root_hash,
            } => {
                let mut p = Vec::with_capacity(40);
                p.extend_from_slice(&node_id.to_bytes());
                p.extend_from_slice(&root_page.0.to_le_bytes());
                p.extend_from_slice(root_hash);
                (MSG_HELLO, p)
            }
            SyncMessage::HelloAck {
                node_id,
                root_page,
                root_hash,
                in_sync,
            } => {
                let mut p = Vec::with_capacity(41);
                p.extend_from_slice(&node_id.to_bytes());
                p.extend_from_slice(&root_page.0.to_le_bytes());
                p.extend_from_slice(root_hash);
                p.push(if *in_sync { 1 } else { 0 });
                (MSG_HELLO_ACK, p)
            }
            SyncMessage::DigestRequest { page_ids } => {
                let mut p = Vec::with_capacity(4 + page_ids.len() * 4);
                p.extend_from_slice(&(page_ids.len() as u32).to_le_bytes());
                for pid in page_ids {
                    p.extend_from_slice(&pid.0.to_le_bytes());
                }
                (MSG_DIGEST_REQUEST, p)
            }
            SyncMessage::DigestResponse { digests } => {
                let mut p = Vec::new();
                p.extend_from_slice(&(digests.len() as u32).to_le_bytes());
                for d in digests {
                    serialize_page_digest(&mut p, d);
                }
                (MSG_DIGEST_RESPONSE, p)
            }
            SyncMessage::EntriesRequest { page_ids } => {
                let mut p = Vec::with_capacity(4 + page_ids.len() * 4);
                p.extend_from_slice(&(page_ids.len() as u32).to_le_bytes());
                for pid in page_ids {
                    p.extend_from_slice(&pid.0.to_le_bytes());
                }
                (MSG_ENTRIES_REQUEST, p)
            }
            SyncMessage::EntriesResponse { entries } => {
                let mut p = Vec::new();
                p.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for e in entries {
                    serialize_diff_entry(&mut p, e);
                }
                (MSG_ENTRIES_RESPONSE, p)
            }
            SyncMessage::PatchData { data } => (MSG_PATCH_DATA, data.clone()),
            SyncMessage::PatchAck { result } => {
                let mut p = Vec::with_capacity(24);
                p.extend_from_slice(&result.entries_applied.to_le_bytes());
                p.extend_from_slice(&result.entries_skipped.to_le_bytes());
                p.extend_from_slice(&result.entries_equal.to_le_bytes());
                (MSG_PATCH_ACK, p)
            }
            SyncMessage::Done => (MSG_DONE, Vec::new()),
            SyncMessage::Error { message } => {
                let bytes = message.as_bytes();
                let mut p = Vec::with_capacity(4 + bytes.len());
                p.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                p.extend_from_slice(bytes);
                (MSG_ERROR, p)
            }
            SyncMessage::PullRequest => (MSG_PULL_REQUEST, Vec::new()),
            SyncMessage::PullResponse {
                root_page,
                root_hash,
            } => {
                let mut p = Vec::with_capacity(32);
                p.extend_from_slice(&root_page.0.to_le_bytes());
                p.extend_from_slice(root_hash);
                (MSG_PULL_RESPONSE, p)
            }
            SyncMessage::TableListRequest => (MSG_TABLE_LIST_REQUEST, Vec::new()),
            SyncMessage::TableListResponse { tables } => {
                let mut p = Vec::new();
                p.extend_from_slice(&(tables.len() as u32).to_le_bytes());
                for t in tables {
                    p.extend_from_slice(&(t.name.len() as u16).to_le_bytes());
                    p.extend_from_slice(&t.name);
                    p.extend_from_slice(&t.root_page.0.to_le_bytes());
                    p.extend_from_slice(&t.root_hash);
                }
                (MSG_TABLE_LIST_RESPONSE, p)
            }
            SyncMessage::TableSyncBegin {
                table_name,
                root_page,
                root_hash,
            } => {
                let mut p = Vec::with_capacity(2 + table_name.len() + 4 + MERKLE_HASH_SIZE);
                p.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
                p.extend_from_slice(table_name);
                p.extend_from_slice(&root_page.0.to_le_bytes());
                p.extend_from_slice(root_hash);
                (MSG_TABLE_SYNC_BEGIN, p)
            }
            SyncMessage::TableSyncEnd { table_name } => {
                let mut p = Vec::with_capacity(2 + table_name.len());
                p.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
                p.extend_from_slice(table_name);
                (MSG_TABLE_SYNC_END, p)
            }
        };

        let mut buf = Vec::with_capacity(5 + payload.len());
        buf.push(msg_type);
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload);
        buf
    }

    /// Deserialize from wire format.
    pub fn deserialize(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.len() < 5 {
            return Err(ProtocolError::Truncated {
                context: "message header".to_string(),
                expected: 5,
                actual: data.len(),
            });
        }

        let msg_type = data[0];
        let payload_len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;

        if data.len() < 5 + payload_len {
            return Err(ProtocolError::Truncated {
                context: "message payload".to_string(),
                expected: 5 + payload_len,
                actual: data.len(),
            });
        }

        let payload = &data[5..5 + payload_len];

        match msg_type {
            MSG_HELLO => {
                ensure_len(payload, 40, "Hello")?;
                let node_id = NodeId::from_bytes(payload[0..8].try_into().unwrap());
                let root_page = PageId(u32::from_le_bytes(payload[8..12].try_into().unwrap()));
                let mut root_hash = [0u8; MERKLE_HASH_SIZE];
                root_hash.copy_from_slice(&payload[12..40]);
                Ok(SyncMessage::Hello {
                    node_id,
                    root_page,
                    root_hash,
                })
            }
            MSG_HELLO_ACK => {
                ensure_len(payload, 41, "HelloAck")?;
                let node_id = NodeId::from_bytes(payload[0..8].try_into().unwrap());
                let root_page = PageId(u32::from_le_bytes(payload[8..12].try_into().unwrap()));
                let mut root_hash = [0u8; MERKLE_HASH_SIZE];
                root_hash.copy_from_slice(&payload[12..40]);
                let in_sync = payload[40] != 0;
                Ok(SyncMessage::HelloAck {
                    node_id,
                    root_page,
                    root_hash,
                    in_sync,
                })
            }
            MSG_DIGEST_REQUEST => {
                ensure_len(payload, 4, "DigestRequest")?;
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                ensure_len(payload, 4 + count * 4, "DigestRequest")?;
                let page_ids = (0..count)
                    .map(|i| {
                        let off = 4 + i * 4;
                        PageId(u32::from_le_bytes(
                            payload[off..off + 4].try_into().unwrap(),
                        ))
                    })
                    .collect();
                Ok(SyncMessage::DigestRequest { page_ids })
            }
            MSG_DIGEST_RESPONSE => {
                ensure_len(payload, 4, "DigestResponse")?;
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                let mut pos = 4;
                let mut digests = Vec::with_capacity(count);
                for _ in 0..count {
                    let (digest, consumed) = deserialize_page_digest(payload, pos)?;
                    digests.push(digest);
                    pos += consumed;
                }
                Ok(SyncMessage::DigestResponse { digests })
            }
            MSG_ENTRIES_REQUEST => {
                ensure_len(payload, 4, "EntriesRequest")?;
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                ensure_len(payload, 4 + count * 4, "EntriesRequest")?;
                let page_ids = (0..count)
                    .map(|i| {
                        let off = 4 + i * 4;
                        PageId(u32::from_le_bytes(
                            payload[off..off + 4].try_into().unwrap(),
                        ))
                    })
                    .collect();
                Ok(SyncMessage::EntriesRequest { page_ids })
            }
            MSG_ENTRIES_RESPONSE => {
                ensure_len(payload, 4, "EntriesResponse")?;
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                let mut pos = 4;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let (entry, consumed) = deserialize_diff_entry(payload, pos)?;
                    entries.push(entry);
                    pos += consumed;
                }
                Ok(SyncMessage::EntriesResponse { entries })
            }
            MSG_PATCH_DATA => Ok(SyncMessage::PatchData {
                data: payload.to_vec(),
            }),
            MSG_PATCH_ACK => {
                ensure_len(payload, 24, "PatchAck")?;
                let entries_applied = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                let entries_skipped = u64::from_le_bytes(payload[8..16].try_into().unwrap());
                let entries_equal = u64::from_le_bytes(payload[16..24].try_into().unwrap());
                Ok(SyncMessage::PatchAck {
                    result: ApplyResult {
                        entries_applied,
                        entries_skipped,
                        entries_equal,
                    },
                })
            }
            MSG_DONE => Ok(SyncMessage::Done),
            MSG_ERROR => {
                ensure_len(payload, 4, "Error")?;
                let msg_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                ensure_len(payload, 4 + msg_len, "Error")?;
                let message = String::from_utf8_lossy(&payload[4..4 + msg_len]).into_owned();
                Ok(SyncMessage::Error { message })
            }
            MSG_PULL_REQUEST => Ok(SyncMessage::PullRequest),
            MSG_PULL_RESPONSE => {
                ensure_len(payload, 32, "PullResponse")?;
                let root_page = PageId(u32::from_le_bytes(payload[0..4].try_into().unwrap()));
                let mut root_hash = [0u8; MERKLE_HASH_SIZE];
                root_hash.copy_from_slice(&payload[4..32]);
                Ok(SyncMessage::PullResponse {
                    root_page,
                    root_hash,
                })
            }
            MSG_TABLE_LIST_REQUEST => Ok(SyncMessage::TableListRequest),
            MSG_TABLE_LIST_RESPONSE => {
                ensure_len(payload, 4, "TableListResponse")?;
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                let mut pos = 4;
                let mut tables = Vec::with_capacity(count);
                for _ in 0..count {
                    ensure_len(payload, pos + 2, "TableInfo name_len")?;
                    let name_len =
                        u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    ensure_len(payload, pos + name_len + 4 + MERKLE_HASH_SIZE, "TableInfo")?;
                    let name = payload[pos..pos + name_len].to_vec();
                    pos += name_len;
                    let root_page = PageId(u32::from_le_bytes(
                        payload[pos..pos + 4].try_into().unwrap(),
                    ));
                    pos += 4;
                    let mut root_hash = [0u8; MERKLE_HASH_SIZE];
                    root_hash.copy_from_slice(&payload[pos..pos + MERKLE_HASH_SIZE]);
                    pos += MERKLE_HASH_SIZE;
                    tables.push(TableInfo {
                        name,
                        root_page,
                        root_hash,
                    });
                }
                Ok(SyncMessage::TableListResponse { tables })
            }
            MSG_TABLE_SYNC_BEGIN => {
                ensure_len(payload, 2, "TableSyncBegin")?;
                let name_len = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
                ensure_len(
                    payload,
                    2 + name_len + 4 + MERKLE_HASH_SIZE,
                    "TableSyncBegin",
                )?;
                let table_name = payload[2..2 + name_len].to_vec();
                let off = 2 + name_len;
                let root_page = PageId(u32::from_le_bytes(
                    payload[off..off + 4].try_into().unwrap(),
                ));
                let mut root_hash = [0u8; MERKLE_HASH_SIZE];
                root_hash.copy_from_slice(&payload[off + 4..off + 4 + MERKLE_HASH_SIZE]);
                Ok(SyncMessage::TableSyncBegin {
                    table_name,
                    root_page,
                    root_hash,
                })
            }
            MSG_TABLE_SYNC_END => {
                ensure_len(payload, 2, "TableSyncEnd")?;
                let name_len = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
                ensure_len(payload, 2 + name_len, "TableSyncEnd")?;
                let table_name = payload[2..2 + name_len].to_vec();
                Ok(SyncMessage::TableSyncEnd { table_name })
            }
            _ => Err(ProtocolError::UnknownMessageType(msg_type)),
        }
    }
}

fn ensure_len(data: &[u8], needed: usize, ctx: &str) -> Result<(), ProtocolError> {
    if data.len() < needed {
        Err(ProtocolError::Truncated {
            context: ctx.to_string(),
            expected: needed,
            actual: data.len(),
        })
    } else {
        Ok(())
    }
}

fn serialize_page_digest(buf: &mut Vec<u8>, d: &PageDigest) {
    buf.extend_from_slice(&d.page_id.0.to_le_bytes());
    buf.extend_from_slice(&(d.page_type as u16).to_le_bytes());
    buf.extend_from_slice(&d.merkle_hash);
    buf.extend_from_slice(&(d.children.len() as u32).to_le_bytes());
    for c in &d.children {
        buf.extend_from_slice(&c.0.to_le_bytes());
    }
}

fn deserialize_page_digest(
    data: &[u8],
    offset: usize,
) -> Result<(PageDigest, usize), ProtocolError> {
    // page_id(4) + page_type(2) + merkle_hash(28) + child_count(4) = 38
    let min = 38;
    if data.len() < offset + min {
        return Err(ProtocolError::Truncated {
            context: "PageDigest header".to_string(),
            expected: offset + min,
            actual: data.len(),
        });
    }

    let page_id = PageId(u32::from_le_bytes(
        data[offset..offset + 4].try_into().unwrap(),
    ));
    let page_type_raw = u16::from_le_bytes(data[offset + 4..offset + 6].try_into().unwrap());
    let page_type = citadel_core::types::PageType::from_u16(page_type_raw)
        .unwrap_or(citadel_core::types::PageType::Leaf);
    let mut merkle_hash = [0u8; MERKLE_HASH_SIZE];
    merkle_hash.copy_from_slice(&data[offset + 6..offset + 34]);
    let child_count =
        u32::from_le_bytes(data[offset + 34..offset + 38].try_into().unwrap()) as usize;

    if data.len() < offset + min + child_count * 4 {
        return Err(ProtocolError::Truncated {
            context: "PageDigest children".to_string(),
            expected: offset + min + child_count * 4,
            actual: data.len(),
        });
    }

    let children = (0..child_count)
        .map(|i| {
            let off = offset + 38 + i * 4;
            PageId(u32::from_le_bytes(data[off..off + 4].try_into().unwrap()))
        })
        .collect();

    Ok((
        PageDigest {
            page_id,
            page_type,
            merkle_hash,
            children,
        },
        min + child_count * 4,
    ))
}

fn serialize_diff_entry(buf: &mut Vec<u8>, e: &DiffEntry) {
    buf.extend_from_slice(&(e.key.len() as u16).to_le_bytes());
    buf.extend_from_slice(&(e.value.len() as u32).to_le_bytes());
    buf.push(e.val_type);
    buf.extend_from_slice(&e.key);
    buf.extend_from_slice(&e.value);
}

fn deserialize_diff_entry(data: &[u8], offset: usize) -> Result<(DiffEntry, usize), ProtocolError> {
    // key_len(2) + val_len(4) + val_type(1) = 7
    let header = 7;
    if data.len() < offset + header {
        return Err(ProtocolError::Truncated {
            context: "DiffEntry header".to_string(),
            expected: offset + header,
            actual: data.len(),
        });
    }

    let key_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
    let val_len = u32::from_le_bytes(data[offset + 2..offset + 6].try_into().unwrap()) as usize;
    let val_type = data[offset + 6];

    let total = header + key_len + val_len;
    if data.len() < offset + total {
        return Err(ProtocolError::Truncated {
            context: "DiffEntry data".to_string(),
            expected: offset + total,
            actual: data.len(),
        });
    }

    let key = data[offset + 7..offset + 7 + key_len].to_vec();
    let value = data[offset + 7 + key_len..offset + 7 + key_len + val_len].to_vec();

    Ok((
        DiffEntry {
            key,
            value,
            val_type,
        },
        total,
    ))
}

#[cfg(test)]
mod tests {
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
}
