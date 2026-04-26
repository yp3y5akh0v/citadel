use crate::crdt::{CrdtMeta, EntryKind, CRDT_HEADER_SIZE, CRDT_META_SIZE};
use crate::diff::DiffResult;
use crate::node_id::NodeId;

const PATCH_MAGIC: u32 = 0x53594E43; // "SYNC"
const PATCH_VERSION: u8 = 1;

const FLAG_HAS_CRDT: u8 = 0x01;

/// A single entry in a sync patch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub kind: EntryKind,
    pub crdt_meta: Option<CrdtMeta>,
}

/// A serializable sync patch containing entries to apply to a target database.
#[derive(Debug, Clone)]
pub struct SyncPatch {
    pub source_node: NodeId,
    pub entries: Vec<PatchEntry>,
    pub crdt_aware: bool,
}

/// Errors from patch serialization/deserialization.
#[derive(Debug, thiserror::Error)]
pub enum PatchError {
    #[error("invalid patch magic: expected {expected:#010x}, got {actual:#010x}")]
    InvalidMagic { expected: u32, actual: u32 },

    #[error("unsupported patch version: {0}")]
    UnsupportedVersion(u8),

    #[error("patch data truncated: expected at least {expected} bytes, got {actual}")]
    Truncated { expected: usize, actual: usize },

    #[error("invalid entry kind: {0}")]
    InvalidEntryKind(u8),
}

impl SyncPatch {
    /// Build a SyncPatch from a DiffResult.
    ///
    /// If `crdt_aware` is true, values are expected to contain CRDT headers
    /// and entries will carry CrdtMeta extracted from the value prefix.
    pub fn from_diff(source_node: NodeId, diff: &DiffResult, crdt_aware: bool) -> Self {
        let entries = diff
            .entries
            .iter()
            .map(|e| {
                if crdt_aware && e.value.len() >= CRDT_HEADER_SIZE {
                    if let Ok(decoded) = crate::crdt::decode_lww_value(&e.value) {
                        return PatchEntry {
                            key: e.key.clone(),
                            value: e.value.clone(),
                            kind: decoded.kind,
                            crdt_meta: Some(decoded.meta),
                        };
                    }
                }
                PatchEntry {
                    key: e.key.clone(),
                    value: e.value.clone(),
                    kind: EntryKind::Put,
                    crdt_meta: None,
                }
            })
            .collect();

        SyncPatch {
            source_node,
            entries,
            crdt_aware,
        }
    }

    /// Create an empty patch.
    pub fn empty(source_node: NodeId) -> Self {
        SyncPatch {
            source_node,
            entries: Vec::new(),
            crdt_aware: false,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Serialize to binary wire format.
    ///
    /// Format:
    /// ```text
    /// [magic: u32 LE][version: u8][flags: u8][source_node: 8B][entry_count: u32 LE]
    /// Per entry:
    ///   [key_len: u16 LE][value_len: u32 LE][kind: u8]
    ///   [crdt_meta: 20B]  (if FLAG_HAS_CRDT)
    ///   [key: key_len bytes][value: value_len bytes]
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        let flags = if self.crdt_aware { FLAG_HAS_CRDT } else { 0 };

        let header_size = 4 + 1 + 1 + 8 + 4; // 18
        let per_entry_overhead = 2 + 4 + 1 + if self.crdt_aware { CRDT_META_SIZE } else { 0 };
        let data_size: usize = self
            .entries
            .iter()
            .map(|e| per_entry_overhead + e.key.len() + e.value.len())
            .sum();

        let mut buf = Vec::with_capacity(header_size + data_size);

        buf.extend_from_slice(&PATCH_MAGIC.to_le_bytes());
        buf.push(PATCH_VERSION);
        buf.push(flags);
        buf.extend_from_slice(&self.source_node.to_bytes());
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());

        for entry in &self.entries {
            buf.extend_from_slice(&(entry.key.len() as u16).to_le_bytes());
            buf.extend_from_slice(&(entry.value.len() as u32).to_le_bytes());
            buf.push(entry.kind as u8);
            if self.crdt_aware {
                if let Some(ref meta) = entry.crdt_meta {
                    buf.extend_from_slice(&meta.to_bytes());
                } else {
                    buf.extend_from_slice(&[0u8; CRDT_META_SIZE]);
                }
            }
            buf.extend_from_slice(&entry.key);
            buf.extend_from_slice(&entry.value);
        }

        buf
    }

    /// Deserialize from binary wire format.
    pub fn deserialize(data: &[u8]) -> Result<Self, PatchError> {
        let header_size = 4 + 1 + 1 + 8 + 4; // 18 bytes
        if data.len() < header_size {
            return Err(PatchError::Truncated {
                expected: header_size,
                actual: data.len(),
            });
        }

        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != PATCH_MAGIC {
            return Err(PatchError::InvalidMagic {
                expected: PATCH_MAGIC,
                actual: magic,
            });
        }

        let version = data[4];
        if version != PATCH_VERSION {
            return Err(PatchError::UnsupportedVersion(version));
        }

        let flags = data[5];
        let crdt_aware = (flags & FLAG_HAS_CRDT) != 0;
        let source_node = NodeId::from_bytes(data[6..14].try_into().unwrap());
        let entry_count = u32::from_le_bytes(data[14..18].try_into().unwrap()) as usize;

        let mut entries = Vec::with_capacity(entry_count);
        let mut pos = header_size;

        for _ in 0..entry_count {
            // key_len (2) + value_len (4) + kind (1) = 7
            let entry_header = 7 + if crdt_aware { CRDT_META_SIZE } else { 0 };
            if pos + entry_header > data.len() {
                return Err(PatchError::Truncated {
                    expected: pos + entry_header,
                    actual: data.len(),
                });
            }

            let key_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            let value_len = u32::from_le_bytes(data[pos + 2..pos + 6].try_into().unwrap()) as usize;
            let kind_byte = data[pos + 6];
            let kind =
                EntryKind::from_u8(kind_byte).ok_or(PatchError::InvalidEntryKind(kind_byte))?;
            pos += 7;

            let crdt_meta = if crdt_aware {
                let meta_bytes: &[u8; CRDT_META_SIZE] =
                    data[pos..pos + CRDT_META_SIZE].try_into().unwrap();
                pos += CRDT_META_SIZE;
                Some(CrdtMeta::from_bytes(meta_bytes))
            } else {
                None
            };

            if pos + key_len + value_len > data.len() {
                return Err(PatchError::Truncated {
                    expected: pos + key_len + value_len,
                    actual: data.len(),
                });
            }

            let key = data[pos..pos + key_len].to_vec();
            pos += key_len;
            let value = data[pos..pos + value_len].to_vec();
            pos += value_len;

            entries.push(PatchEntry {
                key,
                value,
                kind,
                crdt_meta,
            });
        }

        Ok(SyncPatch {
            source_node,
            entries,
            crdt_aware,
        })
    }
}

#[cfg(test)]
#[path = "patch_tests.rs"]
mod tests;
