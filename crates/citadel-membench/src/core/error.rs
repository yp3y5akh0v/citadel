//! Error type for the LoCoMo benchmark harness.

use citadel_llm::{FinishReason, LlmError, TokenUsage};
use citadel_mem::MemError;

use crate::core::agentic::ExtractionParseError;
use crate::core::eval::{CompletionCallAudit, CompletionFinish};

pub type Result<T> = std::result::Result<T, BenchError>;

#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReaderStage {
    Extraction,
    ExtractionValidation,
    FinalAnswer,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CompletedReaderCall {
    pub call: CompletionCallAudit,
    pub finish_reason: CompletionFinish,
}

#[derive(Debug, thiserror::Error)]
#[error("reader {stage:?} failed: {source}")]
pub struct ReaderFailure {
    pub stage: ReaderStage,
    pub completed_calls: Vec<CompletedReaderCall>,
    #[source]
    pub source: Box<BenchError>,
}

#[derive(Debug, thiserror::Error)]
#[error("completion failed: {source}")]
pub struct CompletionFailure {
    /// The request was handed to the completion path; usage is absent on failure.
    pub call: CompletionCallAudit,
    #[source]
    pub source: Box<BenchError>,
}

#[derive(Debug, thiserror::Error)]
pub enum BenchError {
    #[error("dataset error: {0}")]
    Dataset(String),
    #[error(transparent)]
    Reader(#[from] Box<ReaderFailure>),
    #[error(transparent)]
    Completion(#[from] Box<CompletionFailure>),
    #[error("invalid extraction completion ({finish_reason:?}): {reason}")]
    InvalidExtractionCompletion {
        reason: &'static str,
        response: String,
        finish_reason: FinishReason,
    },
    #[error("invalid extraction: {source}")]
    InvalidExtraction {
        response: String,
        #[source]
        source: ExtractionParseError,
    },
    #[error("invalid judge response ({finish_reason:?}): {reason}")]
    InvalidJudgeResponse {
        reason: &'static str,
        response: String,
        finish_reason: FinishReason,
        usage: TokenUsage,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Mem(#[from] MemError),
    #[error(transparent)]
    Db(#[from] citadel::Error),
    #[error(transparent)]
    Llm(#[from] LlmError),
}
