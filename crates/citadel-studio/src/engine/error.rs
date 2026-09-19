//! Engine failures classified for distinct user recovery actions.
//!
//! Matches remain exhaustive so new engine variants cannot be silently misclassified.

use citadel::Error as CoreError;
use citadel_mem::{EmbedError, MemError};
use citadel_sql::SqlError;

/// Failure category at the granularity the interface branches on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    /// The passphrase did not unlock this file. Nothing is said about the file itself.
    Passphrase,
    /// No passphrase was supplied.
    PassphraseRequired,
    /// The key file is absent, unreadable, or belongs to a different database.
    KeyFile,
    /// The header is not a Citadel vault's.
    NotAVault,
    /// Written by a build that disagrees with this one about the format.
    Unsupported,
    /// SQL this build does not implement, distinct from unsupported on-disk formats.
    UnsupportedStatement,
    /// Authentication or structure failed against bytes on disk.
    Damaged,
    /// Another process holds the file. The only recovery is to close it there.
    Locked,
    /// A file, table, column, index, view or region that is not there.
    Missing,
    /// A file, table, index or view that is already there.
    Exists,
    /// The statement did not parse or could not be planned.
    Syntax,
    /// A constraint the schema declares refused the write.
    Constraint,
    /// A value the statement supplied is the wrong type or out of range.
    Data,
    /// The call was made out of order, or asks for something that makes no sense here.
    Usage,
    /// A limit was reached: buffer pool, transaction size, row size.
    Capacity,
    /// Per-region cryptographic erasure is off, or needs a file-backed database.
    RegionKeys,
    /// The supplied operational embedder does not match the region's stored provenance.
    Embedder,
    /// The region's key was destroyed. Its content is gone for good.
    Forgotten,
    /// FIPS mode refused the configuration.
    Fips,
    /// The reader asked for the work to stop.
    Cancelled,
    /// The requested state change was published, but a follow-up durability or audit
    /// step failed. Retrying as though nothing happened would be unsafe.
    Completed,
    /// Everything the operating system reported.
    Io,
}

impl Kind {
    /// User-facing category summary; the original engine message remains the detail.
    pub fn headline(self) -> &'static str {
        match self {
            Self::Passphrase => "That passphrase did not unlock this file",
            Self::PassphraseRequired => "This vault needs a passphrase",
            Self::KeyFile => "The key file is missing, unreadable, or belongs to another vault",
            Self::NotAVault => "That file is not a CitadelDB vault",
            Self::Unsupported => "This build cannot read that format",
            Self::UnsupportedStatement => "This build does not support that statement",
            Self::Damaged => "The file failed its integrity check",
            Self::Locked => "Another process has this vault open",
            Self::Missing => "That does not exist",
            Self::Exists => "That already exists",
            Self::Syntax => "The statement could not be parsed",
            Self::Constraint => "A constraint refused the write",
            Self::Data => "A value was the wrong type or out of range",
            Self::Usage => "That call cannot run here",
            Self::Capacity => "A limit was reached",
            Self::RegionKeys => "Cryptographic erasure is not enabled for this vault",
            Self::Embedder => "That embedder does not match the stored region",
            Self::Forgotten => "That region was forgotten; its content is unrecoverable",
            Self::Fips => "FIPS mode refused that configuration",
            Self::Cancelled => "Cancelled",
            Self::Completed => "The operation completed with a follow-up warning",
            Self::Io => "The file could not be read or written",
        }
    }
}

/// A classified failure retaining the engine's original detail.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StudioError {
    pub kind: Kind,
    pub detail: String,
}

impl StudioError {
    /// A failure Studio itself detected, before or instead of an engine call.
    pub fn new(kind: Kind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for StudioError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind.headline(), self.detail)
    }
}

/// Local classification for foreign engine error types.
pub trait IntoStudioError {
    fn into_studio(self) -> StudioError;
}

impl IntoStudioError for CoreError {
    fn into_studio(self) -> StudioError {
        StudioError {
            kind: core_kind(&self),
            detail: self.to_string(),
        }
    }
}

impl IntoStudioError for SqlError {
    fn into_studio(self) -> StudioError {
        StudioError {
            kind: sql_kind(&self),
            detail: self.to_string(),
        }
    }
}

impl IntoStudioError for MemError {
    fn into_studio(self) -> StudioError {
        StudioError {
            kind: mem_kind(&self),
            detail: self.to_string(),
        }
    }
}

impl IntoStudioError for std::io::Error {
    fn into_studio(self) -> StudioError {
        StudioError {
            kind: io_kind(&self),
            detail: self.to_string(),
        }
    }
}

fn io_kind(e: &std::io::Error) -> Kind {
    match e.kind() {
        std::io::ErrorKind::NotFound => Kind::Missing,
        std::io::ErrorKind::AlreadyExists => Kind::Exists,
        _ => Kind::Io,
    }
}

fn core_kind(e: &CoreError) -> Kind {
    match e {
        // The key-file MAC can fail before passphrase unwrap, so keep these causes distinct.
        CoreError::BadPassphrase | CoreError::KeyFileIntegrity | CoreError::KeyUnwrapFailed => {
            Kind::Passphrase
        }
        CoreError::PassphraseRequired => Kind::PassphraseRequired,
        CoreError::KeyFileMismatch | CoreError::InvalidKeyFileMagic => Kind::KeyFile,
        CoreError::InvalidMagic { .. } => Kind::NotAVault,
        CoreError::UnsupportedVersion(_)
        | CoreError::UnsupportedCipher(_)
        | CoreError::UnsupportedKdf(_) => Kind::Unsupported,
        CoreError::PageTampered(_)
        | CoreError::ChecksumMismatch(_)
        | CoreError::DatabaseCorrupted
        | CoreError::SlotDowngradeDetected
        | CoreError::LegacySlotWriteOnV1File
        | CoreError::CorruptOverflowChain(_)
        | CoreError::InvalidPageType(_, _)
        | CoreError::PageOutOfBounds(_)
        | CoreError::RegionSealTampered
        | CoreError::RegionStoreCorrupt(_) => Kind::Damaged,
        CoreError::DatabaseLocked => Kind::Locked,
        CoreError::TableNotFound(_) => Kind::Missing,
        CoreError::TableAlreadyExists(_) => Kind::Exists,
        CoreError::NamedTableHashCollision { .. } => Kind::Usage,
        CoreError::TransactionTooLarge { .. }
        | CoreError::BufferPoolFull
        | CoreError::PageIdExhausted
        | CoreError::KeyTooLarge { .. }
        | CoreError::ValueTooLarge { .. }
        | CoreError::ReadBudgetExceeded { .. } => Kind::Capacity,
        CoreError::NoWriteTransaction
        | CoreError::WriteTransactionActive
        | CoreError::TransactionFailed
        | CoreError::RegionInUse { .. }
        | CoreError::AtomInUse { .. } => Kind::Usage,
        CoreError::RegionKeysDisabled | CoreError::RegionKeysRequireFile => Kind::RegionKeys,
        CoreError::FipsViolation(_) => Kind::Fips,
        CoreError::Interrupted => Kind::Cancelled,
        CoreError::Io(e) => io_kind(e),
        CoreError::AuditFailureAfterOperation { .. }
        | CoreError::DurabilityFailureAfterOperation { .. }
        | CoreError::DurabilityAndAuditFailureAfterOperation { .. } => Kind::Completed,
        CoreError::Sync(_) => Kind::Io,
    }
}

fn sql_kind(e: &SqlError) -> Kind {
    match e {
        SqlError::Storage(c) => core_kind(c),
        SqlError::Parse(_) | SqlError::Plan(_) => Kind::Syntax,
        SqlError::DuplicateKey
        | SqlError::NotNullViolation(_)
        | SqlError::UniqueViolation(_)
        | SqlError::CheckViolation(_)
        | SqlError::ForeignKeyViolation(_) => Kind::Constraint,
        SqlError::TableNotFound(_)
        | SqlError::ColumnNotFound(_)
        | SqlError::IndexNotFound(_)
        | SqlError::ViewNotFound(_)
        | SqlError::SavepointNotFound(_)
        | SqlError::QueryReturnedNoRows => Kind::Missing,
        SqlError::TableAlreadyExists(_)
        | SqlError::IndexAlreadyExists(_)
        | SqlError::ViewAlreadyExists(_)
        | SqlError::DuplicateColumn(_)
        | SqlError::DuplicateCteName(_) => Kind::Exists,
        SqlError::TypeMismatch { .. }
        | SqlError::InvalidValue(_)
        | SqlError::DivisionByZero
        | SqlError::IntegerOverflow
        | SqlError::InvalidDateLiteral(_)
        | SqlError::InvalidTimeLiteral(_)
        | SqlError::InvalidTimestampLiteral(_)
        | SqlError::InvalidIntervalLiteral(_)
        | SqlError::InvalidExtractField(_)
        | SqlError::InvalidDateTruncUnit(_)
        | SqlError::InvalidTimezone(_) => Kind::Data,
        SqlError::RowTooLarge { .. } | SqlError::KeyTooLarge { .. } => Kind::Capacity,
        SqlError::RecursiveCteMaxIterations(_, _) => Kind::Capacity,
        SqlError::Unsupported(_) | SqlError::TimeZoneUnsupported(_) => Kind::UnsupportedStatement,
        SqlError::AmbiguousColumn(_)
        | SqlError::PrimaryKeyRequired
        | SqlError::TransactionAlreadyActive
        | SqlError::NoActiveTransaction
        | SqlError::SubqueryMultipleColumns
        | SqlError::SubqueryMultipleRows
        | SqlError::ParameterCountMismatch { .. }
        | SqlError::CompoundColumnCountMismatch { .. }
        | SqlError::CteColumnAliasMismatch { .. }
        | SqlError::RecursiveCteNoUnion(_)
        | SqlError::WindowFunctionRequiresOrderBy(_)
        | SqlError::CannotModifyView(_)
        | SqlError::CircularViewReference(_)
        | SqlError::CannotInsertIntoGeneratedColumn(_)
        | SqlError::CannotUpdateGeneratedColumn(_)
        | SqlError::GeneratedColumnReference(_) => Kind::Usage,
    }
}

fn mem_kind(e: &MemError) -> Kind {
    match e {
        // Core errors can arrive directly or wrapped by SQL.
        MemError::Sql(e) => sql_kind(e),
        MemError::Core(e) => core_kind(e),
        MemError::Embed(e) => embed_kind(e),
        MemError::Io(e) => io_kind(e),
        MemError::RegionForgotten(_) => Kind::Forgotten,
        MemError::RegionNotFound(_) => Kind::Missing,
        MemError::RegionNotAttached(_) => Kind::Embedder,
        // Model-free inspection remains usable when vector provenance mismatches.
        MemError::DimMismatch { .. }
        | MemError::MetricMismatch { .. }
        | MemError::ModelMismatch { .. } => Kind::Embedder,
        MemError::Cycle { .. } => Kind::Usage,
        MemError::AtomNotLive { .. }
        | MemError::AtomNotMutable { .. }
        | MemError::IdempotencyConflict { .. }
        | MemError::ReadLimitExceeded { .. }
        | MemError::WorkLimitExceeded { .. } => Kind::Usage,
        MemError::Invalid(_) => Kind::Data,
    }
}

fn embed_kind(_: &EmbedError) -> Kind {
    Kind::Embedder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_unlock_failures_stay_three_different_answers() {
        assert_eq!(core_kind(&CoreError::KeyFileIntegrity), Kind::Passphrase);
        assert_eq!(core_kind(&CoreError::BadPassphrase), Kind::Passphrase);
        assert_eq!(core_kind(&CoreError::KeyUnwrapFailed), Kind::Passphrase);
        assert_eq!(core_kind(&CoreError::KeyFileMismatch), Kind::KeyFile);
        assert_eq!(core_kind(&CoreError::DatabaseCorrupted), Kind::Damaged);
        assert_eq!(
            core_kind(&CoreError::InvalidMagic {
                expected: 1,
                found: 2
            }),
            Kind::NotAVault
        );
    }

    #[test]
    fn a_locked_database_is_recognised_through_both_wrappers() {
        let direct = MemError::Core(CoreError::DatabaseLocked);
        let wrapped = MemError::Sql(SqlError::Storage(CoreError::DatabaseLocked));
        assert_eq!(mem_kind(&direct), Kind::Locked);
        assert_eq!(mem_kind(&wrapped), Kind::Locked);
    }

    #[test]
    fn a_named_table_hash_collision_is_actionable_usage() {
        let error = CoreError::NamedTableHashCollision {
            requested: "collision_table_134778".into(),
            existing: "collision_table_51661".into(),
            hash: 0xab88_afb6,
        };
        assert_eq!(core_kind(&error), Kind::Usage);
    }

    #[test]
    fn a_post_operation_failure_reports_that_the_change_completed() {
        let error = CoreError::DurabilityFailureAfterOperation {
            operation: "change passphrase",
            source: std::io::Error::other("directory sync failed"),
        };
        let studio = error.into_studio();
        assert_eq!(studio.kind, Kind::Completed);
        assert!(studio.to_string().contains("operation completed"));
        assert!(studio.detail.contains("change passphrase completed"));
    }

    #[test]
    fn a_persisted_unattached_region_asks_for_an_embedder() {
        assert_eq!(
            mem_kind(&MemError::RegionNotAttached("notes".into())),
            Kind::Embedder
        );
    }

    #[test]
    fn the_engines_wording_is_carried_not_replaced() {
        let err = CoreError::DatabaseLocked.into_studio();
        assert_eq!(err.kind, Kind::Locked);
        assert_eq!(err.detail, CoreError::DatabaseLocked.to_string());
        assert!(err.to_string().contains("Another process"));
        assert!(err.to_string().contains("locked by another process"));
    }
}
