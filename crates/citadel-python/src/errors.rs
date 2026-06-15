//! Typed exception hierarchy + engine-error mapping (PEP-249 category names).

use citadel::Error as CoreError;
use citadel_ai::agent::AgentError as EngineAgentError;
use citadel_ai::graph::GraphError;
use citadel_ai::llm::LlmError as EngineLlmError;
use citadel_ai::propose::ProposeError;
use citadel_ai::tools::ToolError;
use citadel_mem::{EmbedError, MemError};
use citadel_sql::SqlError;
use citadel_vector::ann::AnnError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    citadeldb,
    CitadelError,
    PyException,
    "Base class for every error raised by Citadel."
);
create_exception!(
    citadeldb,
    EncryptionError,
    CitadelError,
    "Wrong passphrase/key, failed key unwrap, or access to a forgotten region."
);
create_exception!(
    citadeldb,
    IntegrityError,
    CitadelError,
    "Tamper/corruption detected, or a violated SQL constraint (unique/FK/check/not-null)."
);
create_exception!(
    citadeldb,
    OperationalError,
    CitadelError,
    "Operational failure outside the caller's control (lock, I/O, buffer/resource limit)."
);
create_exception!(
    citadeldb,
    ProgrammingError,
    CitadelError,
    "API misuse: missing object, malformed query, or out-of-order transaction call."
);
create_exception!(
    citadeldb,
    DataError,
    CitadelError,
    "Invalid value, type mismatch, overflow, or out-of-range data."
);
create_exception!(
    citadeldb,
    NotSupportedError,
    CitadelError,
    "Unsupported feature, format version, cipher, or KDF."
);
create_exception!(
    citadeldb,
    LlmError,
    CitadelError,
    "LLM client backend, HTTP, or transport failure."
);
create_exception!(
    citadeldb,
    AgentError,
    CitadelError,
    "Agent loop, proposal, or verification failure."
);

/// Which Python exception class an engine error maps to.
#[derive(Clone, Copy)]
enum Category {
    Encryption,
    Integrity,
    Operational,
    Programming,
    Data,
    NotSupported,
    Llm,
    Agent,
}

impl Category {
    fn raise(self, msg: String) -> PyErr {
        match self {
            Category::Encryption => EncryptionError::new_err(msg),
            Category::Integrity => IntegrityError::new_err(msg),
            Category::Operational => OperationalError::new_err(msg),
            Category::Programming => ProgrammingError::new_err(msg),
            Category::Data => DataError::new_err(msg),
            Category::NotSupported => NotSupportedError::new_err(msg),
            Category::Llm => LlmError::new_err(msg),
            Category::Agent => AgentError::new_err(msg),
        }
    }
}

use Category::*;

fn core_category(e: &CoreError) -> Category {
    match e {
        CoreError::BadPassphrase
        | CoreError::KeyFileMismatch
        | CoreError::KeyUnwrapFailed
        | CoreError::PassphraseRequired
        | CoreError::KeyFileIntegrity
        | CoreError::InvalidKeyFileMagic => Encryption,
        CoreError::PageTampered(_)
        | CoreError::ChecksumMismatch(_)
        | CoreError::DatabaseCorrupted
        | CoreError::CorruptOverflowChain(_)
        | CoreError::InvalidMagic { .. }
        | CoreError::RegionSealTampered
        | CoreError::RegionStoreCorrupt(_)
        | CoreError::InvalidPageType(_, _) => Integrity,
        CoreError::DatabaseLocked
        | CoreError::TransactionTooLarge { .. }
        | CoreError::PageOutOfBounds(_)
        | CoreError::BufferPoolFull
        | CoreError::Io(_)
        | CoreError::Sync(_)
        | CoreError::FipsViolation(_) => Operational,
        CoreError::NoWriteTransaction
        | CoreError::WriteTransactionActive
        | CoreError::TableNotFound(_)
        | CoreError::TableAlreadyExists(_)
        | CoreError::RegionKeysDisabled
        | CoreError::RegionKeysRequireFile => Programming,
        CoreError::KeyTooLarge { .. } | CoreError::ValueTooLarge { .. } => Data,
        CoreError::UnsupportedVersion(_)
        | CoreError::UnsupportedCipher(_)
        | CoreError::UnsupportedKdf(_) => NotSupported,
    }
}

fn sql_category(e: &SqlError) -> Category {
    match e {
        SqlError::Storage(c) => core_category(c),
        SqlError::DuplicateKey
        | SqlError::NotNullViolation(_)
        | SqlError::UniqueViolation(_)
        | SqlError::CheckViolation(_)
        | SqlError::ForeignKeyViolation(_) => Integrity,
        SqlError::TypeMismatch { .. }
        | SqlError::RowTooLarge { .. }
        | SqlError::KeyTooLarge { .. }
        | SqlError::InvalidValue(_)
        | SqlError::DivisionByZero
        | SqlError::IntegerOverflow
        | SqlError::InvalidDateLiteral(_)
        | SqlError::InvalidTimeLiteral(_)
        | SqlError::InvalidTimestampLiteral(_)
        | SqlError::InvalidIntervalLiteral(_)
        | SqlError::InvalidExtractField(_)
        | SqlError::InvalidDateTruncUnit(_)
        | SqlError::InvalidTimezone(_) => Data,
        SqlError::Unsupported(_) | SqlError::TimeZoneUnsupported(_) => NotSupported,
        SqlError::RecursiveCteMaxIterations(_, _) => Operational,
        SqlError::Parse(_)
        | SqlError::Plan(_)
        | SqlError::TableNotFound(_)
        | SqlError::TableAlreadyExists(_)
        | SqlError::ColumnNotFound(_)
        | SqlError::PrimaryKeyRequired
        | SqlError::DuplicateColumn(_)
        | SqlError::AmbiguousColumn(_)
        | SqlError::IndexNotFound(_)
        | SqlError::IndexAlreadyExists(_)
        | SqlError::TransactionAlreadyActive
        | SqlError::NoActiveTransaction
        | SqlError::SavepointNotFound(_)
        | SqlError::SubqueryMultipleColumns
        | SqlError::SubqueryMultipleRows
        | SqlError::QueryReturnedNoRows
        | SqlError::ParameterCountMismatch { .. }
        | SqlError::CompoundColumnCountMismatch { .. }
        | SqlError::CteColumnAliasMismatch { .. }
        | SqlError::DuplicateCteName(_)
        | SqlError::RecursiveCteNoUnion(_)
        | SqlError::WindowFunctionRequiresOrderBy(_)
        | SqlError::ViewNotFound(_)
        | SqlError::ViewAlreadyExists(_)
        | SqlError::CannotModifyView(_)
        | SqlError::CircularViewReference(_)
        | SqlError::CannotInsertIntoGeneratedColumn(_)
        | SqlError::CannotUpdateGeneratedColumn(_)
        | SqlError::GeneratedColumnReference(_) => Programming,
    }
}

fn embed_category(_: &EmbedError) -> Category {
    Operational
}

fn mem_category(e: &MemError) -> Category {
    match e {
        MemError::Sql(e) => sql_category(e),
        MemError::Embed(e) => embed_category(e),
        MemError::Core(e) => core_category(e),
        MemError::Io(_) => Operational,
        MemError::RegionForgotten(_) => Encryption,
        MemError::RegionNotFound(_) => Programming,
        MemError::Cycle { .. } => Integrity,
        MemError::DimMismatch { .. }
        | MemError::MetricMismatch { .. }
        | MemError::ModelMismatch { .. }
        | MemError::Invalid(_) => Data,
    }
}

fn llm_category(_: &EngineLlmError) -> Category {
    Llm
}

fn graph_category(e: &GraphError) -> Category {
    match e {
        GraphError::Mem(e) => mem_category(e),
        GraphError::TooLarge { .. } => Operational,
        GraphError::SelfModelBranch => Integrity,
        GraphError::Payload { .. }
        | GraphError::TaskNotFound(_)
        | GraphError::SelfModelExists
        | GraphError::NoSelfModel
        | GraphError::SelfModelNotFound(_)
        | GraphError::GoalNotFound(_)
        | GraphError::GoalMutable(_)
        | GraphError::SelfModelMutable(_)
        | GraphError::SupersededBranch(_)
        | GraphError::EvictionRefused
        | GraphError::NoTraces
        | GraphError::CandidateNotFound(_) => Programming,
    }
}

fn agent_category(e: &EngineAgentError) -> Category {
    match e {
        EngineAgentError::Graph(e) => graph_category(e),
        EngineAgentError::Llm(e) => llm_category(e),
        EngineAgentError::Other(_) => Agent,
    }
}

fn propose_category(e: &ProposeError) -> Category {
    match e {
        ProposeError::Llm(e) => llm_category(e),
        ProposeError::Failed(_) => Agent,
    }
}

fn tool_category(e: &ToolError) -> Category {
    match e {
        ToolError::Unknown(_) | ToolError::BadArgs { .. } => Programming,
        ToolError::Failed { .. } => Operational,
    }
}

/// Maps a foreign engine error to a typed exception (local trait = orphan-rule).
pub(crate) trait IntoPyErr {
    fn into_pyerr(self) -> PyErr;
}

impl IntoPyErr for CoreError {
    fn into_pyerr(self) -> PyErr {
        core_category(&self).raise(self.to_string())
    }
}

impl IntoPyErr for SqlError {
    fn into_pyerr(self) -> PyErr {
        sql_category(&self).raise(self.to_string())
    }
}

impl IntoPyErr for MemError {
    fn into_pyerr(self) -> PyErr {
        mem_category(&self).raise(self.to_string())
    }
}

impl IntoPyErr for EmbedError {
    fn into_pyerr(self) -> PyErr {
        embed_category(&self).raise(self.to_string())
    }
}

impl IntoPyErr for AnnError {
    fn into_pyerr(self) -> PyErr {
        // Build-input errors; pre-validated to ValueError upstream.
        DataError::new_err(self.to_string())
    }
}

impl IntoPyErr for EngineLlmError {
    fn into_pyerr(self) -> PyErr {
        llm_category(&self).raise(self.to_string())
    }
}

impl IntoPyErr for GraphError {
    fn into_pyerr(self) -> PyErr {
        graph_category(&self).raise(self.to_string())
    }
}

impl IntoPyErr for EngineAgentError {
    fn into_pyerr(self) -> PyErr {
        agent_category(&self).raise(self.to_string())
    }
}

impl IntoPyErr for ProposeError {
    fn into_pyerr(self) -> PyErr {
        propose_category(&self).raise(self.to_string())
    }
}

impl IntoPyErr for ToolError {
    fn into_pyerr(self) -> PyErr {
        tool_category(&self).raise(self.to_string())
    }
}

impl IntoPyErr for serde_json::Error {
    fn into_pyerr(self) -> PyErr {
        DataError::new_err(self.to_string())
    }
}

/// Map an engine error to its typed Python exception.
pub(crate) fn to_pyerr<E: IntoPyErr>(e: E) -> PyErr {
    e.into_pyerr()
}

/// Factory construction failures (message-only) surface as `LlmError`.
pub(crate) fn llm_build_err(msg: String) -> PyErr {
    LlmError::new_err(msg)
}

/// Raise a `ProgrammingError` for binding-side API misuse (not an engine error).
pub(crate) fn programming_err(msg: impl Into<String>) -> PyErr {
    ProgrammingError::new_err(msg.into())
}

/// Register the exception classes on the `_core` module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("CitadelError", py.get_type::<CitadelError>())?;
    m.add("EncryptionError", py.get_type::<EncryptionError>())?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;
    m.add("OperationalError", py.get_type::<OperationalError>())?;
    m.add("ProgrammingError", py.get_type::<ProgrammingError>())?;
    m.add("DataError", py.get_type::<DataError>())?;
    m.add("NotSupportedError", py.get_type::<NotSupportedError>())?;
    m.add("LlmError", py.get_type::<LlmError>())?;
    m.add("AgentError", py.get_type::<AgentError>())?;
    Ok(())
}
