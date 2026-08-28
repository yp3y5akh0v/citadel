use crate::crdt::{CrdtMeta, EntryKind, CRDT_HEADER_SIZE, CRDT_META_SIZE};
use crate::diff::{DiffEntry, DiffResult};
use crate::node_id::NodeId;
use crate::protocol::{MAX_SYNC_PAYLOAD_SIZE, MAX_SYNC_VALUE_SIZE};
use citadel_core::constants::{MAX_KEY_SIZE, MAX_VALUE_SIZE};
use citadel_core::types::ValueType;

// Numeric mnemonic "SYNC"; little-endian wire bytes are the frozen "CNYS".
const PATCH_MAGIC: u32 = 0x53594E43;
// Version 2 entries always carry logical values; version 1 could carry a
// database-local OverflowRef in place of an overflow value.
const PATCH_VERSION: u8 = 2;

const FLAG_HAS_CRDT: u8 = 0x01;
const MAX_PATCH_ENTRIES: usize = 1_000_000;

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

    #[error("unsupported patch flags: {0:#04x}")]
    UnsupportedFlags(u8),

    #[error("invalid patch entry count {count}: maximum is {max}")]
    InvalidEntryCount { count: usize, max: usize },

    #[error("unable to reserve storage for {count} patch entries")]
    AllocationFailed { count: usize },

    #[error("invalid patch entry {index}: {reason}")]
    InvalidEntry { index: usize, reason: String },

    #[error("invalid CRDT patch entry {index}: {reason}")]
    InvalidCrdtEntry { index: usize, reason: String },

    #[error("serialized patch is {actual} bytes, sync payload limit is {max}")]
    PatchTooLarge { actual: usize, max: usize },

    #[error("patch length arithmetic overflow while reading {context}")]
    LengthOverflow { context: String },

    #[error("patch has {actual} bytes, but entries end at {expected}")]
    TrailingData { expected: usize, actual: usize },
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
                if e.val_type == ValueType::Tombstone as u8 {
                    return PatchEntry {
                        key: e.key.clone(),
                        value: Vec::new(),
                        kind: EntryKind::Tombstone,
                        crdt_meta: None,
                    };
                }
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

    /// Build a patch by consuming a diff, avoiding a second copy of keys and
    /// logical overflow values at the session boundary.
    pub fn from_diff_owned(source_node: NodeId, diff: DiffResult, crdt_aware: bool) -> Self {
        let entries = diff
            .entries
            .into_iter()
            .map(|entry| patch_entry_from_owned_diff(entry, crdt_aware))
            .collect();
        SyncPatch {
            source_node,
            entries,
            crdt_aware,
        }
    }

    /// Validate every entry before a patch is applied to a transaction.
    pub fn validate(&self) -> Result<(), PatchError> {
        if self.entries.len() > MAX_PATCH_ENTRIES {
            return Err(PatchError::InvalidEntryCount {
                count: self.entries.len(),
                max: MAX_PATCH_ENTRIES,
            });
        }

        for (index, entry) in self.entries.iter().enumerate() {
            if entry.key.len() > MAX_KEY_SIZE {
                return Err(PatchError::InvalidEntry {
                    index,
                    reason: format!(
                        "key is {} bytes, maximum is {MAX_KEY_SIZE}",
                        entry.key.len()
                    ),
                });
            }
            if entry.value.len() > MAX_VALUE_SIZE {
                return Err(PatchError::InvalidEntry {
                    index,
                    reason: format!(
                        "value is {} bytes, maximum is {MAX_VALUE_SIZE}",
                        entry.value.len()
                    ),
                });
            }
            if entry.value.len() > MAX_SYNC_VALUE_SIZE {
                return Err(PatchError::InvalidEntry {
                    index,
                    reason: format!(
                        "value is {} bytes, sync limit is {MAX_SYNC_VALUE_SIZE}; streaming sync is required",
                        entry.value.len()
                    ),
                });
            }

            if !self.crdt_aware {
                if entry.crdt_meta.is_some() {
                    return Err(PatchError::InvalidEntry {
                        index,
                        reason: "non-CRDT patch carries CRDT metadata".into(),
                    });
                }
                if entry.kind == EntryKind::Tombstone && !entry.value.is_empty() {
                    return Err(PatchError::InvalidEntry {
                        index,
                        reason: "physical tombstone carries a value".into(),
                    });
                }
                continue;
            }

            let Some(meta) = entry.crdt_meta else {
                return Err(PatchError::InvalidCrdtEntry {
                    index,
                    reason: "metadata is missing".into(),
                });
            };
            let decoded = crate::crdt::decode_lww_value(&entry.value).map_err(|error| {
                PatchError::InvalidCrdtEntry {
                    index,
                    reason: error.to_string(),
                }
            })?;
            if decoded.kind != entry.kind {
                return Err(PatchError::InvalidCrdtEntry {
                    index,
                    reason: format!(
                        "wire kind {:?} does not match encoded kind {:?}",
                        entry.kind, decoded.kind
                    ),
                });
            }
            if decoded.meta != meta {
                return Err(PatchError::InvalidCrdtEntry {
                    index,
                    reason: "wire metadata does not match encoded metadata".into(),
                });
            }
            if entry.value[1..4] != [0u8; 3] {
                return Err(PatchError::InvalidCrdtEntry {
                    index,
                    reason: "encoded padding is not canonical".into(),
                });
            }
            if entry.kind == EntryKind::Tombstone && entry.value.len() != CRDT_HEADER_SIZE {
                return Err(PatchError::InvalidCrdtEntry {
                    index,
                    reason: "encoded tombstone carries trailing data".into(),
                });
            }
        }
        Ok(())
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
    pub fn serialize(&self) -> Result<Vec<u8>, PatchError> {
        self.try_serialize()
    }

    fn serialize_with_len(&self, len: usize) -> Result<Vec<u8>, PatchError> {
        let flags = if self.crdt_aware { FLAG_HAS_CRDT } else { 0 };
        let mut buf = Vec::new();
        buf.try_reserve_exact(len)
            .map_err(|_| PatchError::AllocationFailed {
                count: self.entries.len(),
            })?;

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

        debug_assert_eq!(buf.len(), len);
        Ok(buf)
    }

    #[cfg(test)]
    fn serialize_unchecked(&self) -> Vec<u8> {
        let overhead = 7 + if self.crdt_aware { CRDT_META_SIZE } else { 0 };
        let len = self.entries.iter().fold(18usize, |size, entry| {
            size + overhead + entry.key.len() + entry.value.len()
        });
        self.serialize_with_len(len)
            .expect("test patch serialization should allocate")
    }

    /// Validate and serialize a patch that is representable in one protocol
    /// message. Values beyond this version's limit require streaming sync.
    pub fn try_serialize(&self) -> Result<Vec<u8>, PatchError> {
        self.validate()?;
        let overhead = 7 + if self.crdt_aware { CRDT_META_SIZE } else { 0 };
        let mut len = 18usize;
        for entry in &self.entries {
            len = len
                .checked_add(overhead)
                .and_then(|size| size.checked_add(entry.key.len()))
                .and_then(|size| size.checked_add(entry.value.len()))
                .ok_or(PatchError::PatchTooLarge {
                    actual: usize::MAX,
                    max: MAX_SYNC_PAYLOAD_SIZE,
                })?;
            if len > MAX_SYNC_PAYLOAD_SIZE {
                return Err(PatchError::PatchTooLarge {
                    actual: len,
                    max: MAX_SYNC_PAYLOAD_SIZE,
                });
            }
        }
        self.serialize_with_len(len)
    }

    /// Deserialize from binary wire format.
    pub fn deserialize(data: &[u8]) -> Result<Self, PatchError> {
        if data.len() > MAX_SYNC_PAYLOAD_SIZE {
            return Err(PatchError::PatchTooLarge {
                actual: data.len(),
                max: MAX_SYNC_PAYLOAD_SIZE,
            });
        }
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
        if flags & !FLAG_HAS_CRDT != 0 {
            return Err(PatchError::UnsupportedFlags(flags));
        }
        let crdt_aware = (flags & FLAG_HAS_CRDT) != 0;
        let source_node = NodeId::from_bytes(data[6..14].try_into().unwrap());
        let entry_count = u32::from_le_bytes(data[14..18].try_into().unwrap()) as usize;

        let minimum_entry_size = 7 + if crdt_aware { CRDT_META_SIZE } else { 0 };
        let maximum_from_bytes = (data.len() - header_size) / minimum_entry_size;
        let maximum = maximum_from_bytes.min(MAX_PATCH_ENTRIES);
        if entry_count > maximum {
            return Err(PatchError::InvalidEntryCount {
                count: entry_count,
                max: maximum,
            });
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(entry_count)
            .map_err(|_| PatchError::AllocationFailed { count: entry_count })?;
        let mut pos = header_size;

        for _ in 0..entry_count {
            // key_len (2) + value_len (4) + kind (1) = 7
            let entry_header = 7 + if crdt_aware { CRDT_META_SIZE } else { 0 };
            let entry_header_end = patch_checked_add(pos, entry_header, "entry header")?;
            if entry_header_end > data.len() {
                return Err(PatchError::Truncated {
                    expected: entry_header_end,
                    actual: data.len(),
                });
            }

            let key_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            let value_len = u32::from_le_bytes(data[pos + 2..pos + 6].try_into().unwrap()) as usize;
            let kind_byte = data[pos + 6];
            let kind =
                EntryKind::from_u8(kind_byte).ok_or(PatchError::InvalidEntryKind(kind_byte))?;
            pos += 7;

            let index = entries.len();
            if key_len > MAX_KEY_SIZE {
                return Err(PatchError::InvalidEntry {
                    index,
                    reason: format!("key is {key_len} bytes, maximum is {MAX_KEY_SIZE}"),
                });
            }
            if value_len > MAX_VALUE_SIZE || value_len > MAX_SYNC_VALUE_SIZE {
                return Err(PatchError::InvalidEntry {
                    index,
                    reason: format!(
                        "value is {value_len} bytes, sync limit is {MAX_SYNC_VALUE_SIZE}"
                    ),
                });
            }

            let crdt_meta = if crdt_aware {
                let meta_bytes: &[u8; CRDT_META_SIZE] =
                    data[pos..pos + CRDT_META_SIZE].try_into().unwrap();
                pos += CRDT_META_SIZE;
                Some(CrdtMeta::from_bytes(meta_bytes))
            } else {
                None
            };

            let key_end = patch_checked_add(pos, key_len, "entry key")?;
            let value_end = patch_checked_add(key_end, value_len, "entry value")?;
            if value_end > data.len() {
                return Err(PatchError::Truncated {
                    expected: value_end,
                    actual: data.len(),
                });
            }

            let key = data[pos..key_end].to_vec();
            let value = data[key_end..value_end].to_vec();
            pos = value_end;

            entries.push(PatchEntry {
                key,
                value,
                kind,
                crdt_meta,
            });
        }

        if pos != data.len() {
            return Err(PatchError::TrailingData {
                expected: pos,
                actual: data.len(),
            });
        }

        let patch = SyncPatch {
            source_node,
            entries,
            crdt_aware,
        };
        patch.validate()?;
        Ok(patch)
    }
}

fn patch_entry_from_owned_diff(entry: DiffEntry, crdt_aware: bool) -> PatchEntry {
    if entry.val_type == ValueType::Tombstone as u8 {
        return PatchEntry {
            key: entry.key,
            value: Vec::new(),
            kind: EntryKind::Tombstone,
            crdt_meta: None,
        };
    }
    if crdt_aware && entry.value.len() >= CRDT_HEADER_SIZE {
        if let Ok(decoded) = crate::crdt::decode_lww_value(&entry.value) {
            let kind = decoded.kind;
            let meta = decoded.meta;
            return PatchEntry {
                key: entry.key,
                value: entry.value,
                kind,
                crdt_meta: Some(meta),
            };
        }
    }
    PatchEntry {
        key: entry.key,
        value: entry.value,
        kind: EntryKind::Put,
        crdt_meta: None,
    }
}

fn patch_checked_add(left: usize, right: usize, context: &str) -> Result<usize, PatchError> {
    left.checked_add(right)
        .ok_or_else(|| PatchError::LengthOverflow {
            context: context.to_string(),
        })
}

#[cfg(test)]
#[path = "patch_tests.rs"]
mod tests;
