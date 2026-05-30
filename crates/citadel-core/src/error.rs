use crate::types::PageId;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("page {0} failed MAC verification: data has been tampered with")]
    PageTampered(PageId),

    #[error("incorrect passphrase or wrong key file")]
    BadPassphrase,

    #[error("database file is locked by another process")]
    DatabaseLocked,

    #[error("key file does not match data file (file_id mismatch)")]
    KeyFileMismatch,

    #[error("transaction requires more pages than buffer pool capacity ({capacity})")]
    TransactionTooLarge { capacity: usize },

    #[error("database file is corrupted: both commit slots are invalid")]
    DatabaseCorrupted,

    #[error("page checksum mismatch on page {0} (post-decrypt integrity failure)")]
    ChecksumMismatch(PageId),

    #[error("invalid page type {0} on page {1}")]
    InvalidPageType(u16, PageId),

    #[error("key too large: {size} bytes (max {max})")]
    KeyTooLarge { size: usize, max: usize },

    #[error("value too large: {size} bytes (max {max})")]
    ValueTooLarge { size: usize, max: usize },

    #[error("invalid magic number: expected 0x{expected:08X}, found 0x{found:08X}")]
    InvalidMagic { expected: u32, found: u32 },

    #[error("unsupported format version: {0}")]
    UnsupportedVersion(u32),

    #[error("key file integrity check failed (HMAC mismatch)")]
    KeyFileIntegrity,

    #[error("invalid key file magic")]
    InvalidKeyFileMagic,

    #[error("key unwrap failed (AES-KW integrity check)")]
    KeyUnwrapFailed,

    #[error("no write transaction active")]
    NoWriteTransaction,

    #[error("a write transaction is already active")]
    WriteTransactionActive,

    #[error("page {0} is out of bounds (beyond high water mark)")]
    PageOutOfBounds(PageId),

    #[error("buffer pool is full and all pages are pinned")]
    BufferPoolFull,

    #[error("unsupported cipher: {0}")]
    UnsupportedCipher(u8),

    #[error("unsupported KDF algorithm: {0}")]
    UnsupportedKdf(u8),

    #[error("FIPS mode violation: {0}")]
    FipsViolation(String),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("passphrase is required")]
    PassphraseRequired,

    #[error("sync error: {0}")]
    Sync(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corrupted overflow chain: {0}")]
    CorruptOverflowChain(String),

    #[error("region content failed authentication (wrong key or erased region)")]
    RegionSealTampered,

    #[error("region key store is corrupt: {0}")]
    RegionStoreCorrupt(String),

    #[error("per-region cryptographic erasure is not enabled for this database")]
    RegionKeysDisabled,

    #[error("per-region cryptographic erasure requires a file-backed database (not in-memory)")]
    RegionKeysRequireFile,
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
