mod builder;
mod database;

#[cfg(feature = "audit-log")]
pub mod audit;

// Public API
pub use builder::DatabaseBuilder;
pub use database::{Database, DbStats};

#[cfg(feature = "audit-log")]
pub use audit::{
    AuditConfig, AuditEntry, AuditEventType, AuditVerifyResult, ScanResult,
    read_audit_log, verify_audit_log, scan_corrupted_audit_log,
};
pub use citadel_core::error::{Error, Result};
pub use citadel_core::types::{Argon2Profile, CipherId, KdfAlgorithm};
pub use citadel_txn::integrity::{IntegrityReport, IntegrityError};

// Internal crate re-exports (used by integration tests and advanced usage)
pub use citadel_core as core;
pub use citadel_crypto as crypto;
pub use citadel_io as io;
pub use citadel_page as page;
pub use citadel_buffer as buffer;
pub use citadel_txn as txn;
