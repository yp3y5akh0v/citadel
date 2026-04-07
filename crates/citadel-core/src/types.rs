use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PageId(pub u32);

impl PageId {
    pub const INVALID: Self = Self(u32::MAX);

    #[inline]
    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    #[inline]
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "page:{}", self.0)
    }
}

impl From<u32> for PageId {
    #[inline]
    fn from(v: u32) -> Self {
        Self(v)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TxnId(pub u64);

impl TxnId {
    pub const ZERO: Self = Self(0);

    #[inline]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn:{}", self.0)
    }
}

impl From<u64> for TxnId {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PageType {
    Branch = 1,
    Leaf = 2,
    Overflow = 3,
    PendingFree = 4,
}

impl PageType {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            1 => Some(Self::Branch),
            2 => Some(Self::Leaf),
            3 => Some(Self::Overflow),
            4 => Some(Self::PendingFree),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageFlags(pub u16);

impl PageFlags {
    pub const NONE: Self = Self(0);
    pub const IS_ROOT: Self = Self(0x01);

    #[inline]
    pub fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }

    #[inline]
    pub fn set(&mut self, flag: Self) {
        self.0 |= flag.0;
    }

    #[inline]
    pub fn clear(&mut self, flag: Self) {
        self.0 &= !flag.0;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CipherId {
    Aes256Ctr = 0,
    ChaCha20 = 1,
}

impl CipherId {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Aes256Ctr),
            1 => Some(Self::ChaCha20),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    Inline = 0,
    Overflow = 1,
    Tombstone = 2,
}

impl ValueType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Inline),
            1 => Some(Self::Overflow),
            2 => Some(Self::Tombstone),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KdfAlgorithm {
    Argon2id = 0,
    Pbkdf2HmacSha256 = 1,
}

impl KdfAlgorithm {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Argon2id),
            1 => Some(Self::Pbkdf2HmacSha256),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Two fsyncs per commit. Crash-safe against power loss.
    #[default]
    Full,
    /// One fsync per commit. Previous commit always recoverable.
    Normal,
    /// No fsyncs. Process-crash safe but not power-loss safe.
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Argon2Profile {
    Iot,
    Desktop,
    Server,
}

impl Argon2Profile {
    pub fn m_cost(self) -> u32 {
        match self {
            Self::Iot => 19 * 1024,     // 19 MiB
            Self::Desktop => 64 * 1024, // 64 MiB
            Self::Server => 128 * 1024, // 128 MiB
        }
    }

    pub fn t_cost(self) -> u32 {
        match self {
            Self::Iot => 2,
            Self::Desktop => 3,
            Self::Server => 4,
        }
    }

    pub fn p_cost(self) -> u32 {
        match self {
            Self::Iot => 1,
            Self::Desktop => 4,
            Self::Server => 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_id_display() {
        assert_eq!(format!("{}", PageId(42)), "page:42");
    }

    #[test]
    fn page_id_invalid() {
        assert!(!PageId::INVALID.is_valid());
        assert!(PageId(0).is_valid());
    }

    #[test]
    fn txn_id_next() {
        assert_eq!(TxnId(5).next(), TxnId(6));
    }

    #[test]
    fn page_type_roundtrip() {
        assert_eq!(PageType::from_u16(1), Some(PageType::Branch));
        assert_eq!(PageType::from_u16(2), Some(PageType::Leaf));
        assert_eq!(PageType::from_u16(3), Some(PageType::Overflow));
        assert_eq!(PageType::from_u16(4), Some(PageType::PendingFree));
        assert_eq!(PageType::from_u16(0), None);
        assert_eq!(PageType::from_u16(5), None);
    }

    #[test]
    fn page_flags() {
        let mut f = PageFlags::NONE;
        assert!(!f.contains(PageFlags::IS_ROOT));
        f.set(PageFlags::IS_ROOT);
        assert!(f.contains(PageFlags::IS_ROOT));
        f.clear(PageFlags::IS_ROOT);
        assert!(!f.contains(PageFlags::IS_ROOT));
    }

    #[test]
    fn cipher_id_roundtrip() {
        assert_eq!(CipherId::from_u8(0), Some(CipherId::Aes256Ctr));
        assert_eq!(CipherId::from_u8(1), Some(CipherId::ChaCha20));
        assert_eq!(CipherId::from_u8(2), None);
    }

    #[test]
    fn kdf_algorithm_roundtrip() {
        assert_eq!(KdfAlgorithm::from_u8(0), Some(KdfAlgorithm::Argon2id));
        assert_eq!(
            KdfAlgorithm::from_u8(1),
            Some(KdfAlgorithm::Pbkdf2HmacSha256)
        );
        assert_eq!(KdfAlgorithm::from_u8(2), None);
    }

    #[test]
    fn argon2_profiles() {
        assert_eq!(Argon2Profile::Iot.m_cost(), 19 * 1024);
        assert_eq!(Argon2Profile::Desktop.m_cost(), 64 * 1024);
        assert_eq!(Argon2Profile::Server.m_cost(), 128 * 1024);
        assert_eq!(Argon2Profile::Iot.p_cost(), 1);
        assert_eq!(Argon2Profile::Desktop.p_cost(), 4);
    }
}
