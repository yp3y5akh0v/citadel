//! Typed exception hierarchy + engine-error mapping (PEP-249 category names).

use citadel::Error as CoreError;
use citadel_ai::agent::AgentError as EngineAgentError;
use citadel_ai::graph::GraphError;
use citadel_ai::propose::ProposeError;
use citadel_ai::tools::ToolError;
use citadel_ai::TracePersistenceFailure;
use citadel_llm::LlmError as EngineLlmError;
use citadel_mem::{EmbedError, MemError};
use citadel_sql::SqlError;
use citadel_vector::ann::AnnError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;

use crate::json_to_py;
use crate::llm::{request_to_py, response_to_py};

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
    "Operation interrupted or failed at runtime (cancellation, lock, I/O, resource limit)."
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
        | CoreError::SlotDowngradeDetected
        | CoreError::LegacySlotWriteOnV1File
        | CoreError::CorruptOverflowChain(_)
        | CoreError::InvalidMagic { .. }
        | CoreError::RegionSealTampered
        | CoreError::RegionStoreCorrupt(_)
        | CoreError::InvalidPageType(_, _) => Integrity,
        // DB-API has no cancellation or failed-transaction exception class.
        CoreError::Interrupted
        | CoreError::TransactionFailed
        | CoreError::RegionInUse { .. }
        | CoreError::AtomInUse { .. }
        | CoreError::DatabaseLocked
        | CoreError::TransactionTooLarge { .. }
        | CoreError::PageOutOfBounds(_)
        | CoreError::BufferPoolFull
        | CoreError::PageIdExhausted
        | CoreError::Io(_)
        | CoreError::AuditFailureAfterOperation { .. }
        | CoreError::DurabilityFailureAfterOperation { .. }
        | CoreError::DurabilityAndAuditFailureAfterOperation { .. }
        | CoreError::Sync(_)
        | CoreError::FipsViolation(_) => Operational,
        CoreError::NoWriteTransaction
        | CoreError::WriteTransactionActive
        | CoreError::TableNotFound(_)
        | CoreError::TableAlreadyExists(_)
        | CoreError::NamedTableHashCollision { .. }
        | CoreError::RegionKeysDisabled
        | CoreError::RegionKeysRequireFile => Programming,
        CoreError::KeyTooLarge { .. }
        | CoreError::ValueTooLarge { .. }
        | CoreError::ReadBudgetExceeded { .. } => Data,
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
        MemError::RegionNotFound(_) | MemError::RegionNotAttached(_) => Programming,
        MemError::Cycle { .. } => Integrity,
        MemError::AtomNotLive { .. }
        | MemError::AtomNotMutable { .. }
        | MemError::IdempotencyConflict { .. }
        | MemError::DimMismatch { .. }
        | MemError::MetricMismatch { .. }
        | MemError::ModelMismatch { .. }
        | MemError::ReadLimitExceeded { .. }
        | MemError::WorkLimitExceeded { .. }
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
        EngineAgentError::Budget(_)
        | EngineAgentError::TracePersistence(_)
        | EngineAgentError::Other(_) => Agent,
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

/// Maps a foreign engine error to a typed exception (local trait =
/// orphan-rule).
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
        match self {
            EngineAgentError::TracePersistence(failure) => trace_persistence_err(*failure),
            other => agent_category(&other).raise(other.to_string()),
        }
    }
}

fn provider_error_to_py(py: Python<'_>, error: &EngineLlmError) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    let (kind, message) = match error {
        EngineLlmError::UnsupportedRequest(message) => ("unsupported_request", message),
        EngineLlmError::Backend(message) => ("backend", message),
        EngineLlmError::Transport(message) => ("transport", message),
        EngineLlmError::Http {
            status,
            retry_after,
            message,
        } => {
            value.set_item("status", status)?;
            value.set_item("retry_after", retry_after)?;
            ("http", message)
        }
    };
    value.set_item("kind", kind)?;
    value.set_item("message", message)?;
    value.set_item("pre_dispatch", error.is_pre_dispatch())?;
    value.set_item("retryable", error.is_retryable())?;
    value.into_py_any(py)
}

fn trace_persistence_err(failure: TracePersistenceFailure) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let error = AgentError::new_err(failure.to_string());
        let recovery = PyDict::new(py);
        let usage = PyDict::new(py);
        usage.set_item("steps", failure.usage.steps)?;
        usage.set_item("tokens", failure.usage.tokens)?;
        usage.set_item("wall_secs", failure.usage.wall_secs)?;
        usage.set_item("cost_usd", failure.usage.cost_usd)?;
        usage.set_item("proposals", failure.usage.proposals)?;
        usage.set_item("checker_calls", failure.usage.checker_calls)?;
        recovery.set_item("usage", usage)?;
        recovery.set_item("confirmed_persisted", failure.confirmed_persisted)?;
        let mut calls = Vec::with_capacity(failure.calls.len());
        for call in &failure.calls {
            let value = PyDict::new(py);
            value.set_item("request", request_to_py(py, &call.request, &call.model_id)?)?;
            value.set_item("request_hash", &call.request_hash)?;
            value.set_item("model_id", &call.model_id)?;
            value.set_item(
                "client",
                json_to_py(py, &serde_json::to_value(&call.client).map_err(to_pyerr)?)?,
            )?;
            value.set_item(
                "prompt",
                json_to_py(
                    py,
                    &serde_json::json!({
                        "id": call.prompt.id.as_str(),
                        "version": call.prompt.version,
                        "text": call.prompt.text,
                        "hash": call.prompt.hash,
                        "source": call.prompt.source.as_str(),
                    }),
                )?,
            )?;
            value.set_item("attempt", call.attempt)?;
            let outcome = PyDict::new(py);
            match &call.outcome {
                Ok(response) => {
                    outcome.set_item("kind", "response")?;
                    outcome.set_item("response", response_to_py(py, response)?)?;
                }
                Err(error) => {
                    outcome.set_item("kind", "error")?;
                    outcome.set_item("error", provider_error_to_py(py, error)?)?;
                }
            }
            value.set_item("outcome", outcome)?;
            calls.push(value.into_py_any(py)?);
        }
        recovery.set_item("calls", calls)?;
        error.value(py).setattr("recovery", recovery)?;
        // Explicit access preserves the original typed cause without including
        // arbitrary backend contents in ordinary exception traceback formatting.
        error
            .value(py)
            .setattr("storage_error", failure.source.into_pyerr().value(py))?;
        Ok(error)
    })
    .unwrap_or_else(|error| error)
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

/// Raise a `ProgrammingError` for binding-side API misuse (not an engine
/// error).
pub(crate) fn programming_err(msg: impl Into<String>) -> PyErr {
    ProgrammingError::new_err(msg.into())
}

/// Raise an `EncryptionError` for a passphrase the binding rejected before the
/// engine saw it, so a caller cannot tell the two apart.
pub(crate) fn encryption_err(msg: impl Into<String>) -> PyErr {
    EncryptionError::new_err(msg.into())
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
    let agent_error = py.get_type::<AgentError>();
    agent_error.setattr("recovery", py.None())?;
    agent_error.setattr("storage_error", py.None())?;
    m.add("AgentError", agent_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recovered_provider_failures_preserve_fields_and_dispatch_classification() {
        Python::initialize();
        Python::attach(|py| {
            for (error, expected) in [
                (
                    EngineLlmError::UnsupportedRequest("private local refusal".into()),
                    json!({
                        "kind": "unsupported_request", "message": "private local refusal", "pre_dispatch": true, "retryable": false,
                    }),
                ),
                (
                    EngineLlmError::Backend("private backend error".into()),
                    json!({
                        "kind": "backend", "message": "private backend error", "pre_dispatch": false, "retryable": false,
                    }),
                ),
                (
                    EngineLlmError::Transport("private transport error".into()),
                    json!({
                        "kind": "transport", "message": "private transport error", "pre_dispatch": false, "retryable": true,
                    }),
                ),
                (
                    EngineLlmError::Http {
                        status: 429,
                        retry_after: Some(7),
                        message: "private HTTP error".into(),
                    },
                    json!({
                        "kind": "http", "message": "private HTTP error", "status": 429, "retry_after": 7, "pre_dispatch": false, "retryable": true,
                    }),
                ),
                (
                    EngineLlmError::Http {
                        status: 400,
                        retry_after: None,
                        message: "private HTTP error".into(),
                    },
                    json!({
                        "kind": "http", "message": "private HTTP error", "status": 400, "retry_after": null, "pre_dispatch": false, "retryable": false,
                    }),
                ),
            ] {
                let rendered = provider_error_to_py(py, &error).unwrap();
                assert_eq!(crate::py_to_json(py, rendered.bind(py)).unwrap(), expected);
            }
        });
    }
}
