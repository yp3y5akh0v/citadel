use crate::hlc::HlcTimestamp;
use crate::node_id::NodeId;

/// Per-entry CRDT metadata for LWW (Last-Writer-Wins) conflict resolution.
///
/// 20 bytes on wire: entry_kind (1B) + padding (3B) + HLC timestamp (12B) + NodeId (8B).
///
/// Conflict resolution: higher timestamp wins, NodeId tiebreaker.
/// This forms a join-semilattice with a total order, guaranteeing:
/// - Commutativity: merge(a, b) == merge(b, a)
/// - Associativity: merge(merge(a, b), c) == merge(a, merge(b, c))
/// - Idempotency: merge(a, a) == a
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CrdtMeta {
    pub timestamp: HlcTimestamp,
    pub node_id: NodeId,
}

/// Wire size of CrdtMeta: HLC (12B) + NodeId (8B) = 20 bytes.
pub const CRDT_META_SIZE: usize = 20;

/// Wire size of a full CRDT-encoded value header: kind (1B) + padding (3B) + meta (20B) = 24 bytes.
pub const CRDT_HEADER_SIZE: usize = 24;

/// Type of CRDT entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EntryKind {
    /// Key-value write (user value follows the header).
    Put = 0,
    /// Logical delete (tombstone). No user value follows.
    Tombstone = 1,
}

impl EntryKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Put),
            1 => Some(Self::Tombstone),
            _ => None,
        }
    }
}

impl CrdtMeta {
    #[inline]
    pub fn new(timestamp: HlcTimestamp, node_id: NodeId) -> Self {
        Self { timestamp, node_id }
    }

    pub fn to_bytes(&self) -> [u8; CRDT_META_SIZE] {
        let mut buf = [0u8; CRDT_META_SIZE];
        let ts_bytes = self.timestamp.to_bytes();
        let nid_bytes = self.node_id.to_bytes();
        buf[0..12].copy_from_slice(&ts_bytes);
        buf[12..20].copy_from_slice(&nid_bytes);
        buf
    }

    pub fn from_bytes(b: &[u8; CRDT_META_SIZE]) -> Self {
        let ts = HlcTimestamp::from_bytes(b[0..12].try_into().unwrap());
        let nid = NodeId::from_bytes(b[12..20].try_into().unwrap());
        Self {
            timestamp: ts,
            node_id: nid,
        }
    }

    /// Higher timestamp wins, NodeId tiebreaker.
    #[inline]
    pub fn lww_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then(self.node_id.cmp(&other.node_id))
    }

    #[inline]
    pub fn wins_over(&self, other: &Self) -> bool {
        self.lww_cmp(other) == std::cmp::Ordering::Greater
    }
}

impl std::fmt::Debug for CrdtMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CrdtMeta({:?}, {:?})", self.timestamp, self.node_id)
    }
}

/// Encode a user value with CRDT header.
///
/// Format: `[entry_kind: u8][_pad: 3B][HLC: 12B][NodeId: 8B][user_value...]`
///
/// Total header: 24 bytes. User value follows immediately after.
pub fn encode_lww_value(meta: &CrdtMeta, kind: EntryKind, user_value: &[u8]) -> Vec<u8> {
    let user_len = if kind == EntryKind::Tombstone {
        0
    } else {
        user_value.len()
    };
    let mut buf = Vec::with_capacity(CRDT_HEADER_SIZE + user_len);
    buf.push(kind as u8);
    buf.extend_from_slice(&[0u8; 3]); // padding
    buf.extend_from_slice(&meta.to_bytes());
    if kind == EntryKind::Put {
        buf.extend_from_slice(user_value);
    }
    buf
}

#[derive(Debug)]
pub struct DecodedValue<'a> {
    pub meta: CrdtMeta,
    pub kind: EntryKind,
    pub user_value: &'a [u8],
}

pub fn decode_lww_value(data: &[u8]) -> Result<DecodedValue<'_>, DecodeError> {
    if data.len() < CRDT_HEADER_SIZE {
        return Err(DecodeError::TooShort {
            expected: CRDT_HEADER_SIZE,
            actual: data.len(),
        });
    }

    let kind = EntryKind::from_u8(data[0]).ok_or(DecodeError::InvalidEntryKind(data[0]))?;
    // bytes 1..4 are padding (ignored on read)
    let meta_bytes: &[u8; CRDT_META_SIZE] = data[4..24].try_into().unwrap();
    let meta = CrdtMeta::from_bytes(meta_bytes);

    let user_value = if kind == EntryKind::Tombstone {
        &data[CRDT_HEADER_SIZE..CRDT_HEADER_SIZE] // empty slice
    } else {
        &data[CRDT_HEADER_SIZE..]
    };

    Ok(DecodedValue {
        meta,
        kind,
        user_value,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("CRDT value too short: expected at least {expected} bytes, got {actual}")]
    TooShort { expected: usize, actual: usize },

    #[error("invalid CRDT entry kind: {0}")]
    InvalidEntryKind(u8),
}

/// Merge two CRDT entries for the same key.
///
/// Returns which side wins using LWW resolution:
/// higher timestamp wins, NodeId tiebreaker.
/// The entry kind (Put vs Tombstone) does NOT affect the merge -
/// a tombstone with a higher timestamp defeats a put with a lower one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeResult {
    /// Keep the local entry.
    Local,
    /// Take the remote entry.
    Remote,
    /// Both entries are identical.
    Equal,
}

pub fn lww_merge(local: &CrdtMeta, remote: &CrdtMeta) -> MergeResult {
    match local.lww_cmp(remote) {
        std::cmp::Ordering::Greater => MergeResult::Local,
        std::cmp::Ordering::Less => MergeResult::Remote,
        std::cmp::Ordering::Equal => MergeResult::Equal,
    }
}

#[cfg(test)]
#[path = "crdt_tests.rs"]
mod tests;
