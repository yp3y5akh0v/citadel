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

    #[error("database file is corrupted")]
    DatabaseCorrupted,

    #[error("commit-slot downgrade detected: this file requires authenticated (V1) slots but holds a valid legacy slot; a pre-v1 binary or a rollback wrote to it - reopen with the binary that wrote it or restore from a trusted backup")]
    SlotDowngradeDetected,

    #[error("refusing to write an unauthenticated (legacy) commit slot into a V1-flagged file: stale named-table entries exceed the V1 slot capacity")]
    LegacySlotWriteOnV1File,

    #[error("page checksum mismatch on page {0} (post-decrypt integrity failure)")]
    ChecksumMismatch(PageId),

    #[error("invalid page type {0} on page {1}")]
    InvalidPageType(u16, PageId),

    #[error("key too large: {size} bytes (max {max})")]
    KeyTooLarge { size: usize, max: usize },

    #[error("value too large: {size} bytes (max {max})")]
    ValueTooLarge { size: usize, max: usize },

    #[error(
        "read materialization budget exceeded: value is {size} bytes, per-value limit is {max_value} bytes, {remaining} bytes remain"
    )]
    ReadBudgetExceeded {
        size: usize,
        max_value: usize,
        remaining: usize,
    },

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

    #[error(
        "table name {requested:?} collides with existing table {existing:?} in commit-slot hash {hash:#010x}"
    )]
    NamedTableHashCollision {
        requested: String,
        existing: String,
        hash: u32,
    },

    #[error("passphrase is required")]
    PassphraseRequired,

    #[error("sync error: {0}")]
    Sync(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{operation} completed, but audit logging failed: {source}")]
    AuditFailureAfterOperation {
        operation: &'static str,
        #[source]
        source: Box<Error>,
    },

    #[error(
        "{operation} completed, but its directory entry could not be confirmed durable: {source}"
    )]
    DurabilityFailureAfterOperation {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "{operation} completed, but its directory entry could not be confirmed durable: {durability}; audit logging also failed: {audit}"
    )]
    DurabilityAndAuditFailureAfterOperation {
        operation: &'static str,
        #[source]
        durability: std::io::Error,
        audit: Box<Error>,
    },

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

    #[error("the operation was cancelled")]
    Interrupted,

    #[error(
        "write transaction cannot be committed because an earlier mutation failed; roll it back"
    )]
    TransactionFailed,

    #[error("memory region {region_id} is in use by another operation")]
    RegionInUse { region_id: u64 },

    #[error("memory atom {atom_id} is in use by an external callback")]
    AtomInUse { atom_id: u64 },
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
