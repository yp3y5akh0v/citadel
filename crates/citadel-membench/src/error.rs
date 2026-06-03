//! Error type for the LoCoMo benchmark harness.

use citadel_ai::LlmError;
use citadel_mem::MemError;

pub type Result<T> = std::result::Result<T, BenchError>;

#[derive(Debug, thiserror::Error)]
pub enum BenchError {
    #[error("dataset error: {0}")]
    Dataset(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Mem(#[from] MemError),
    #[error(transparent)]
    Llm(#[from] LlmError),
}
