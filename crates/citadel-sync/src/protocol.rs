use citadel_core::constants::MAX_KEY_SIZE;
use citadel_core::types::PageId;
use citadel_core::MERKLE_HASH_SIZE;

use crate::apply::ApplyResult;
use crate::diff::{DiffEntry, MerkleHash, PageDigest, MAX_BRANCH_CHILDREN};
use crate::node_id::NodeId;

/// Message type tags for wire format. The v2 default-tree and named-table
/// session entry tags use new values: released peers transmit physical overflow
/// references while this protocol transmits logical payload bytes, so a peer
/// must reject the other version rather than store an 8-byte ref as a value.
const MSG_HELLO: u8 = 16;
const MSG_HELLO_ACK: u8 = 17;
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
const MSG_TABLE_LIST_REQUEST: u8 = 18;
const MSG_TABLE_LIST_RESPONSE: u8 = 19;
const MSG_TABLE_SYNC_BEGIN: u8 = 20;
const MSG_TABLE_SYNC_END: u8 = 21;
const MAX_WIRE_ITEMS: usize = 1_000_000;
pub(crate) const MAX_SYNC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
pub(crate) const MAX_SYNC_PAYLOAD_SIZE: usize = MAX_SYNC_MESSAGE_SIZE - 5;
// One value must fit both a one-entry CRDT patch and an EntriesResponse with a
// maximum-size key. Larger values need streaming, not a truncated frame.
pub(crate) const MAX_SYNC_VALUE_SIZE: usize =
    MAX_SYNC_PAYLOAD_SIZE - 18 - (7 + crate::crdt::CRDT_META_SIZE) - MAX_KEY_SIZE;

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
        crdt_aware: bool,
    },
    /// Responder acknowledgment with its own tree root state.
    HelloAck {
        node_id: NodeId,
        root_page: PageId,
        root_hash: MerkleHash,
        in_sync: bool,
        crdt_aware: bool,
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
    TableListRequest { crdt_aware: bool },
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

    #[error("{context}: count {count} exceeds the {max} entries present")]
    InvalidCount {
        context: String,
        count: usize,
        max: usize,
    },

    #[error("{context}: unable to reserve capacity for {count} items")]
    AllocationFailed { context: String, count: usize },

    #[error("{context}: length arithmetic overflow")]
    LengthOverflow { context: String },

    #[error("{context}: expected exactly {expected} bytes, got {actual}")]
    UnexpectedLength {
        context: String,
        expected: usize,
        actual: usize,
    },

    #[error("invalid page type in page digest: {0}")]
    InvalidPageType(u16),

    #[error("invalid value type in diff entry: {0}")]
    InvalidValueType(u8),

    #[error("sync message is {actual} bytes, maximum is {max}")]
    MessageTooLarge { actual: usize, max: usize },

    #[error("sync value is {actual} bytes, maximum is {max}; streaming sync is required")]
    ValueTooLarge { actual: usize, max: usize },

    #[error("{context} length {actual} is invalid; maximum is {max}")]
    InvalidFieldLength {
        context: String,
        actual: usize,
        max: usize,
    },

    #[error("{context}: invalid boolean byte {value}")]
    InvalidBoolean { context: String, value: u8 },
}

impl SyncMessage {
    /// Compute and validate the complete framed wire size without allocating
    /// the serialized message.
    pub(crate) fn validated_wire_len(&self) -> Result<usize, ProtocolError> {
        let payload_len = match self {
            SyncMessage::Hello { .. } => 41,
            SyncMessage::HelloAck { .. } => 42,
            SyncMessage::DigestRequest { page_ids } | SyncMessage::EntriesRequest { page_ids } => {
                validate_outgoing_count(page_ids.len(), "page request")?;
                checked_collection_size(4, page_ids.len(), 4, "page request")?
            }
            SyncMessage::DigestResponse { digests } => {
                validate_outgoing_count(digests.len(), "DigestResponse")?;
                let mut len = 4usize;
                for digest in digests {
                    if digest.children.len() > MAX_BRANCH_CHILDREN {
                        return Err(ProtocolError::InvalidCount {
                            context: "PageDigest children".into(),
                            count: digest.children.len(),
                            max: MAX_BRANCH_CHILDREN,
                        });
                    }
                    let children = checked_collection_size(
                        0,
                        digest.children.len(),
                        4,
                        "PageDigest children",
                    )?;
                    len = checked_add(len, 38, "DigestResponse")?;
                    len = checked_add(len, children, "DigestResponse")?;
                }
                len
            }
            SyncMessage::EntriesResponse { entries } => {
                validate_outgoing_count(entries.len(), "EntriesResponse")?;
                let mut len = 4usize;
                for entry in entries {
                    if citadel_core::types::ValueType::from_u8(entry.val_type).is_none() {
                        return Err(ProtocolError::InvalidValueType(entry.val_type));
                    }
                    if entry.key.len() > MAX_KEY_SIZE {
                        return Err(ProtocolError::InvalidFieldLength {
                            context: "DiffEntry key".into(),
                            actual: entry.key.len(),
                            max: MAX_KEY_SIZE,
                        });
                    }
                    if entry.value.len() > MAX_SYNC_VALUE_SIZE {
                        return Err(ProtocolError::ValueTooLarge {
                            actual: entry.value.len(),
                            max: MAX_SYNC_VALUE_SIZE,
                        });
                    }
                    len = checked_add(len, 7, "EntriesResponse")?;
                    len = checked_add(len, entry.key.len(), "EntriesResponse")?;
                    len = checked_add(len, entry.value.len(), "EntriesResponse")?;
                }
                len
            }
            SyncMessage::PatchData { data } => data.len(),
            SyncMessage::PatchAck { .. } => 24,
            SyncMessage::Done | SyncMessage::PullRequest => 0,
            SyncMessage::TableListRequest { .. } => 1,
            SyncMessage::Error { message } => checked_add(4, message.len(), "Error")?,
            SyncMessage::PullResponse { .. } => 32,
            SyncMessage::TableListResponse { tables } => {
                validate_outgoing_count(tables.len(), "TableListResponse")?;
                let mut len = 4usize;
                for table in tables {
                    validate_table_name_len(&table.name, "TableInfo name")?;
                    len = checked_add(len, 34, "TableListResponse")?;
                    len = checked_add(len, table.name.len(), "TableListResponse")?;
                }
                len
            }
            SyncMessage::TableSyncBegin { table_name, .. } => {
                validate_table_name_len(table_name, "TableSyncBegin name")?;
                checked_add(34, table_name.len(), "TableSyncBegin")?
            }
            SyncMessage::TableSyncEnd { table_name } => {
                validate_table_name_len(table_name, "TableSyncEnd name")?;
                checked_add(2, table_name.len(), "TableSyncEnd")?
            }
        };
        let wire_len = checked_add(5, payload_len, "message")?;
        if wire_len > MAX_SYNC_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge {
                actual: wire_len,
                max: MAX_SYNC_MESSAGE_SIZE,
            });
        }
        Ok(wire_len)
    }

    /// Validate and serialize to wire format:
    /// `[msg_type: u8][payload_len: u32 LE][payload]`.
    pub fn serialize(&self) -> Result<Vec<u8>, ProtocolError> {
        let wire_len = self.validated_wire_len()?;
        let mut buf = Vec::new();
        buf.try_reserve_exact(wire_len)
            .map_err(|_| ProtocolError::AllocationFailed {
                context: "serialized message".into(),
                count: wire_len,
            })?;
        buf.resize(5, 0);

        let msg_type = match self {
            SyncMessage::Hello {
                node_id,
                root_page,
                root_hash,
                crdt_aware,
            } => {
                buf.extend_from_slice(&node_id.to_bytes());
                buf.extend_from_slice(&root_page.0.to_le_bytes());
                buf.extend_from_slice(root_hash);
                buf.push(u8::from(*crdt_aware));
                MSG_HELLO
            }
            SyncMessage::HelloAck {
                node_id,
                root_page,
                root_hash,
                in_sync,
                crdt_aware,
            } => {
                buf.extend_from_slice(&node_id.to_bytes());
                buf.extend_from_slice(&root_page.0.to_le_bytes());
                buf.extend_from_slice(root_hash);
                buf.push(u8::from(*in_sync));
                buf.push(u8::from(*crdt_aware));
                MSG_HELLO_ACK
            }
            SyncMessage::DigestRequest { page_ids } => {
                buf.extend_from_slice(&(page_ids.len() as u32).to_le_bytes());
                for pid in page_ids {
                    buf.extend_from_slice(&pid.0.to_le_bytes());
                }
                MSG_DIGEST_REQUEST
            }
            SyncMessage::DigestResponse { digests } => {
                buf.extend_from_slice(&(digests.len() as u32).to_le_bytes());
                for d in digests {
                    serialize_page_digest(&mut buf, d);
                }
                MSG_DIGEST_RESPONSE
            }
            SyncMessage::EntriesRequest { page_ids } => {
                buf.extend_from_slice(&(page_ids.len() as u32).to_le_bytes());
                for pid in page_ids {
                    buf.extend_from_slice(&pid.0.to_le_bytes());
                }
                MSG_ENTRIES_REQUEST
            }
            SyncMessage::EntriesResponse { entries } => {
                buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for e in entries {
                    serialize_diff_entry(&mut buf, e);
                }
                MSG_ENTRIES_RESPONSE
            }
            SyncMessage::PatchData { data } => {
                buf.extend_from_slice(data);
                MSG_PATCH_DATA
            }
            SyncMessage::PatchAck { result } => {
                buf.extend_from_slice(&result.entries_applied.to_le_bytes());
                buf.extend_from_slice(&result.entries_skipped.to_le_bytes());
                buf.extend_from_slice(&result.entries_equal.to_le_bytes());
                MSG_PATCH_ACK
            }
            SyncMessage::Done => MSG_DONE,
            SyncMessage::Error { message } => {
                let bytes = message.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
                MSG_ERROR
            }
            SyncMessage::PullRequest => MSG_PULL_REQUEST,
            SyncMessage::PullResponse {
                root_page,
                root_hash,
            } => {
                buf.extend_from_slice(&root_page.0.to_le_bytes());
                buf.extend_from_slice(root_hash);
                MSG_PULL_RESPONSE
            }
            SyncMessage::TableListRequest { crdt_aware } => {
                buf.push(u8::from(*crdt_aware));
                MSG_TABLE_LIST_REQUEST
            }
            SyncMessage::TableListResponse { tables } => {
                buf.extend_from_slice(&(tables.len() as u32).to_le_bytes());
                for t in tables {
                    buf.extend_from_slice(&(t.name.len() as u16).to_le_bytes());
                    buf.extend_from_slice(&t.name);
                    buf.extend_from_slice(&t.root_page.0.to_le_bytes());
                    buf.extend_from_slice(&t.root_hash);
                }
                MSG_TABLE_LIST_RESPONSE
            }
            SyncMessage::TableSyncBegin {
                table_name,
                root_page,
                root_hash,
            } => {
                buf.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
                buf.extend_from_slice(table_name);
                buf.extend_from_slice(&root_page.0.to_le_bytes());
                buf.extend_from_slice(root_hash);
                MSG_TABLE_SYNC_BEGIN
            }
            SyncMessage::TableSyncEnd { table_name } => {
                buf.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
                buf.extend_from_slice(table_name);
                MSG_TABLE_SYNC_END
            }
        };

        if buf.len() != wire_len {
            return Err(ProtocolError::UnexpectedLength {
                context: "serialized message".into(),
                expected: wire_len,
                actual: buf.len(),
            });
        }
        buf[0] = msg_type;
        buf[1..5].copy_from_slice(&((wire_len - 5) as u32).to_le_bytes());
        Ok(buf)
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

        let payload_end = checked_add(5, payload_len, "message payload")?;
        if payload_end > MAX_SYNC_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge {
                actual: payload_end,
                max: MAX_SYNC_MESSAGE_SIZE,
            });
        }
        if data.len() < payload_end {
            return Err(ProtocolError::Truncated {
                context: "message payload".to_string(),
                expected: payload_end,
                actual: data.len(),
            });
        }
        if data.len() != payload_end {
            return Err(ProtocolError::UnexpectedLength {
                context: "message frame".into(),
                expected: payload_end,
                actual: data.len(),
            });
        }

        let payload = &data[5..payload_end];

        match msg_type {
            MSG_HELLO => {
                ensure_exact_len(payload, 41, "Hello")?;
                let node_id = NodeId::from_bytes(payload[0..8].try_into().unwrap());
                let root_page = PageId(u32::from_le_bytes(payload[8..12].try_into().unwrap()));
                let mut root_hash = [0u8; MERKLE_HASH_SIZE];
                root_hash.copy_from_slice(&payload[12..40]);
                Ok(SyncMessage::Hello {
                    node_id,
                    root_page,
                    root_hash,
                    crdt_aware: parse_bool(payload[40], "Hello crdt_aware")?,
                })
            }
            MSG_HELLO_ACK => {
                ensure_exact_len(payload, 42, "HelloAck")?;
                let node_id = NodeId::from_bytes(payload[0..8].try_into().unwrap());
                let root_page = PageId(u32::from_le_bytes(payload[8..12].try_into().unwrap()));
                let mut root_hash = [0u8; MERKLE_HASH_SIZE];
                root_hash.copy_from_slice(&payload[12..40]);
                let in_sync = parse_bool(payload[40], "HelloAck in_sync")?;
                Ok(SyncMessage::HelloAck {
                    node_id,
                    root_page,
                    root_hash,
                    in_sync,
                    crdt_aware: parse_bool(payload[41], "HelloAck crdt_aware")?,
                })
            }
            MSG_DIGEST_REQUEST => {
                ensure_len(payload, 4, "DigestRequest")?;
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                ensure_count_fits(payload, 4, count, 4, "DigestRequest")?;
                let mut page_ids = reserved_vec(count, "DigestRequest")?;
                for i in 0..count {
                    let off = 4 + i * 4;
                    page_ids.push(PageId(u32::from_le_bytes(
                        payload[off..off + 4].try_into().unwrap(),
                    )));
                }
                ensure_exact_len(payload, 4 + count * 4, "DigestRequest")?;
                Ok(SyncMessage::DigestRequest { page_ids })
            }
            MSG_DIGEST_RESPONSE => {
                ensure_len(payload, 4, "DigestResponse")?;
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                ensure_count_fits(payload, 4, count, 38, "DigestResponse")?;
                let mut pos = 4;
                let mut digests = reserved_vec(count, "DigestResponse")?;
                for _ in 0..count {
                    let (digest, consumed) = deserialize_page_digest(payload, pos)?;
                    digests.push(digest);
                    pos += consumed;
                }
                ensure_exact_len(payload, pos, "DigestResponse")?;
                Ok(SyncMessage::DigestResponse { digests })
            }
            MSG_ENTRIES_REQUEST => {
                ensure_len(payload, 4, "EntriesRequest")?;
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                ensure_count_fits(payload, 4, count, 4, "EntriesRequest")?;
                let mut page_ids = reserved_vec(count, "EntriesRequest")?;
                for i in 0..count {
                    let off = 4 + i * 4;
                    page_ids.push(PageId(u32::from_le_bytes(
                        payload[off..off + 4].try_into().unwrap(),
                    )));
                }
                ensure_exact_len(payload, 4 + count * 4, "EntriesRequest")?;
                Ok(SyncMessage::EntriesRequest { page_ids })
            }
            MSG_ENTRIES_RESPONSE => {
                ensure_len(payload, 4, "EntriesResponse")?;
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                ensure_count_fits(payload, 4, count, 7, "EntriesResponse")?;
                let mut pos = 4;
                let mut entries = reserved_vec(count, "EntriesResponse")?;
                for _ in 0..count {
                    let (entry, consumed) = deserialize_diff_entry(payload, pos)?;
                    entries.push(entry);
                    pos += consumed;
                }
                ensure_exact_len(payload, pos, "EntriesResponse")?;
                Ok(SyncMessage::EntriesResponse { entries })
            }
            MSG_PATCH_DATA => Ok(SyncMessage::PatchData {
                data: payload.to_vec(),
            }),
            MSG_PATCH_ACK => {
                ensure_exact_len(payload, 24, "PatchAck")?;
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
            MSG_DONE => {
                ensure_exact_len(payload, 0, "Done")?;
                Ok(SyncMessage::Done)
            }
            MSG_ERROR => {
                ensure_len(payload, 4, "Error")?;
                let msg_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                let message_end = checked_add(4, msg_len, "Error")?;
                ensure_exact_len(payload, message_end, "Error")?;
                let message = String::from_utf8_lossy(&payload[4..message_end]).into_owned();
                Ok(SyncMessage::Error { message })
            }
            MSG_PULL_REQUEST => {
                ensure_exact_len(payload, 0, "PullRequest")?;
                Ok(SyncMessage::PullRequest)
            }
            MSG_PULL_RESPONSE => {
                ensure_exact_len(payload, 32, "PullResponse")?;
                let root_page = PageId(u32::from_le_bytes(payload[0..4].try_into().unwrap()));
                let mut root_hash = [0u8; MERKLE_HASH_SIZE];
                root_hash.copy_from_slice(&payload[4..32]);
                Ok(SyncMessage::PullResponse {
                    root_page,
                    root_hash,
                })
            }
            MSG_TABLE_LIST_REQUEST => {
                ensure_exact_len(payload, 1, "TableListRequest")?;
                Ok(SyncMessage::TableListRequest {
                    crdt_aware: parse_bool(payload[0], "TableListRequest crdt_aware")?,
                })
            }
            MSG_TABLE_LIST_RESPONSE => {
                ensure_len(payload, 4, "TableListResponse")?;
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                ensure_count_fits(payload, 4, count, 34, "TableListResponse")?;
                let mut pos = 4;
                let mut tables = reserved_vec(count, "TableListResponse")?;
                for _ in 0..count {
                    let name_header_end = checked_add(pos, 2, "TableInfo name_len")?;
                    ensure_len(payload, name_header_end, "TableInfo name_len")?;
                    let name_len =
                        u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
                    validate_table_name_len_value(name_len, "TableInfo name")?;
                    pos += 2;
                    let name_end = checked_add(pos, name_len, "TableInfo")?;
                    let root_end = checked_add(name_end, 4, "TableInfo")?;
                    let table_end = checked_add(root_end, MERKLE_HASH_SIZE, "TableInfo")?;
                    ensure_len(payload, table_end, "TableInfo")?;
                    let name = payload[pos..name_end].to_vec();
                    pos = name_end;
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
                ensure_exact_len(payload, pos, "TableListResponse")?;
                Ok(SyncMessage::TableListResponse { tables })
            }
            MSG_TABLE_SYNC_BEGIN => {
                ensure_len(payload, 2, "TableSyncBegin")?;
                let name_len = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
                validate_table_name_len_value(name_len, "TableSyncBegin name")?;
                let name_end = checked_add(2, name_len, "TableSyncBegin")?;
                let root_end = checked_add(name_end, 4, "TableSyncBegin")?;
                let message_end = checked_add(root_end, MERKLE_HASH_SIZE, "TableSyncBegin")?;
                ensure_exact_len(payload, message_end, "TableSyncBegin")?;
                let table_name = payload[2..name_end].to_vec();
                let off = name_end;
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
                validate_table_name_len_value(name_len, "TableSyncEnd name")?;
                let name_end = checked_add(2, name_len, "TableSyncEnd")?;
                ensure_exact_len(payload, name_end, "TableSyncEnd")?;
                let table_name = payload[2..name_end].to_vec();
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

fn ensure_exact_len(data: &[u8], expected: usize, context: &str) -> Result<(), ProtocolError> {
    ensure_len(data, expected, context)?;
    if data.len() != expected {
        return Err(ProtocolError::UnexpectedLength {
            context: context.to_string(),
            expected,
            actual: data.len(),
        });
    }
    Ok(())
}

fn validate_table_name_len(name: &[u8], context: &str) -> Result<(), ProtocolError> {
    validate_table_name_len_value(name.len(), context)
}

fn validate_table_name_len_value(len: usize, context: &str) -> Result<(), ProtocolError> {
    if len == 0 || len > MAX_KEY_SIZE {
        return Err(ProtocolError::InvalidFieldLength {
            context: context.to_string(),
            actual: len,
            max: MAX_KEY_SIZE,
        });
    }
    Ok(())
}

fn parse_bool(value: u8, context: &str) -> Result<bool, ProtocolError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ProtocolError::InvalidBoolean {
            context: context.to_string(),
            value,
        }),
    }
}

fn checked_add(left: usize, right: usize, context: &str) -> Result<usize, ProtocolError> {
    left.checked_add(right)
        .ok_or_else(|| ProtocolError::LengthOverflow {
            context: context.to_string(),
        })
}

fn checked_collection_size(
    base: usize,
    count: usize,
    item_size: usize,
    context: &str,
) -> Result<usize, ProtocolError> {
    let items = count
        .checked_mul(item_size)
        .ok_or_else(|| ProtocolError::LengthOverflow {
            context: context.to_string(),
        })?;
    checked_add(base, items, context)
}

fn validate_outgoing_count(count: usize, context: &str) -> Result<(), ProtocolError> {
    if count > MAX_WIRE_ITEMS {
        return Err(ProtocolError::InvalidCount {
            context: context.to_string(),
            count,
            max: MAX_WIRE_ITEMS,
        });
    }
    Ok(())
}

fn ensure_count_fits(
    data: &[u8],
    offset: usize,
    count: usize,
    minimum_item_size: usize,
    context: &str,
) -> Result<(), ProtocolError> {
    let remaining = data.len().saturating_sub(offset);
    let maximum = (remaining / minimum_item_size).min(MAX_WIRE_ITEMS);
    if count > maximum {
        Err(ProtocolError::InvalidCount {
            context: context.to_string(),
            count,
            max: maximum,
        })
    } else {
        Ok(())
    }
}

fn reserved_vec<T>(count: usize, context: &str) -> Result<Vec<T>, ProtocolError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| ProtocolError::AllocationFailed {
            context: context.to_string(),
            count,
        })?;
    Ok(values)
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
    let header_end = checked_add(offset, min, "PageDigest header")?;
    if data.len() < header_end {
        return Err(ProtocolError::Truncated {
            context: "PageDigest header".to_string(),
            expected: header_end,
            actual: data.len(),
        });
    }

    let page_id = PageId(u32::from_le_bytes(
        data[offset..offset + 4].try_into().unwrap(),
    ));
    let page_type_raw = u16::from_le_bytes(data[offset + 4..offset + 6].try_into().unwrap());
    let page_type = citadel_core::types::PageType::from_u16(page_type_raw)
        .ok_or(ProtocolError::InvalidPageType(page_type_raw))?;
    let mut merkle_hash = [0u8; MERKLE_HASH_SIZE];
    merkle_hash.copy_from_slice(&data[offset + 6..offset + 34]);
    let child_count =
        u32::from_le_bytes(data[offset + 34..offset + 38].try_into().unwrap()) as usize;
    if child_count > MAX_BRANCH_CHILDREN {
        return Err(ProtocolError::InvalidCount {
            context: "PageDigest children".into(),
            count: child_count,
            max: MAX_BRANCH_CHILDREN,
        });
    }
    ensure_count_fits(data, header_end, child_count, 4, "PageDigest children")?;
    let child_bytes = child_count
        .checked_mul(4)
        .ok_or_else(|| ProtocolError::LengthOverflow {
            context: "PageDigest children".into(),
        })?;
    let children_end = checked_add(header_end, child_bytes, "PageDigest children")?;
    if data.len() < children_end {
        return Err(ProtocolError::Truncated {
            context: "PageDigest children".to_string(),
            expected: children_end,
            actual: data.len(),
        });
    }

    let mut children = reserved_vec(child_count, "PageDigest children")?;
    for i in 0..child_count {
        let off = header_end + i * 4;
        children.push(PageId(u32::from_le_bytes(
            data[off..off + 4].try_into().unwrap(),
        )));
    }

    Ok((
        PageDigest {
            page_id,
            page_type,
            merkle_hash,
            children,
        },
        min + child_bytes,
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
    let header_end = checked_add(offset, header, "DiffEntry header")?;
    if data.len() < header_end {
        return Err(ProtocolError::Truncated {
            context: "DiffEntry header".to_string(),
            expected: header_end,
            actual: data.len(),
        });
    }

    let key_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
    let val_len = u32::from_le_bytes(data[offset + 2..offset + 6].try_into().unwrap()) as usize;
    let val_type = data[offset + 6];
    if key_len > MAX_KEY_SIZE {
        return Err(ProtocolError::InvalidFieldLength {
            context: "DiffEntry key".into(),
            actual: key_len,
            max: MAX_KEY_SIZE,
        });
    }
    if citadel_core::types::ValueType::from_u8(val_type).is_none() {
        return Err(ProtocolError::InvalidValueType(val_type));
    }

    let total = checked_add(
        checked_add(header, key_len, "DiffEntry data")?,
        val_len,
        "DiffEntry data",
    )?;
    let entry_end = checked_add(offset, total, "DiffEntry data")?;
    if data.len() < entry_end {
        return Err(ProtocolError::Truncated {
            context: "DiffEntry data".to_string(),
            expected: entry_end,
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
#[path = "protocol_tests.rs"]
mod tests;
