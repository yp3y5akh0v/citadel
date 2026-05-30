mod atom_store;
mod builder;
mod database;
mod key_codec;
mod region_store;

#[cfg(feature = "audit-log")]
pub mod audit;

// Public API
pub use builder::DatabaseBuilder;
pub use citadel_sync::SyncKey;
pub use database::{Database, DbStats, SharedCache, SqlCacheHandle, SyncOutcome};
pub use key_codec::{SlotRecord, SlotState};

#[cfg(feature = "audit-log")]
pub use audit::{
    read_audit_log, scan_corrupted_audit_log, verify_audit_log, AuditConfig, AuditEntry,
    AuditEventType, AuditVerifyResult, ScanResult,
};
pub use citadel_core::error::{Error, Result};
pub use citadel_core::types::{Argon2Profile, CipherId, KdfAlgorithm, SyncMode};
pub use citadel_txn::integrity::{IntegrityError, IntegrityReport};

// Internal crate re-exports (used by integration tests and advanced usage)
pub use citadel_buffer as buffer;
pub use citadel_core as core;
pub use citadel_crypto as crypto;
pub use citadel_io as io;
pub use citadel_page as page;
pub use citadel_txn as txn;
