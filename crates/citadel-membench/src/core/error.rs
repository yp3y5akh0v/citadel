//! Error type for the LoCoMo benchmark harness.

use citadel_llm::{FinishReason, LlmError, TokenUsage};
use citadel_mem::MemError;

pub type Result<T> = std::result::Result<T, BenchError>;

#[derive(Debug, thiserror::Error)]
pub enum BenchError {
    #[error("dataset error: {0}")]
    Dataset(String),
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
