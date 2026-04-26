/// Unique node identifier for replication.
///
/// Each database instance generates a random `NodeId` on creation.
/// Used as a deterministic tiebreaker in LWW conflict resolution
/// when two entries have identical HLC timestamps.
///
/// Serialized as 8 bytes big-endian for consistent byte ordering.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u64);

impl NodeId {
    pub fn random() -> Self {
        use rand::Rng;
        Self(rand::thread_rng().gen())
    }

    #[inline]
    pub fn from_u64(v: u64) -> Self {
        Self(v)
    }

    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    #[inline]
    pub fn from_bytes(b: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(b))
    }
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeId({:016x})", self.0)
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

#[cfg(test)]
#[path = "node_id_tests.rs"]
mod tests;
