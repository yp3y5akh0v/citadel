use crate::embed::EmbedError;

pub type Result<T> = std::result::Result<T, MemError>;

#[derive(Debug, thiserror::Error)]
pub enum MemError {
    #[error(transparent)]
    Sql(#[from] citadel_sql::SqlError),
    #[error(transparent)]
    Embed(#[from] EmbedError),
    #[error(transparent)]
    Core(#[from] citadel_core::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("region '{0}' has been forgotten: its content key was cryptographically erased")]
    RegionForgotten(String),
    #[error("region '{0}' not found (call create_region first)")]
    RegionNotFound(String),
    #[error(
        "region '{0}' exists but is not attached to an embedder in this engine (call \
         attach_existing_region first)"
    )]
    RegionNotAttached(String),
    #[error("link {src}->{dst} would create a cycle")]
    Cycle { src: i64, dst: i64 },
    #[error("region '{region}' exists with dim {expected}, embedder has dim {got}")]
    DimMismatch {
        region: String,
        expected: u16,
        got: usize,
    },
    #[error("region '{region}' exists with metric {expected}, embedder has {got}")]
    MetricMismatch {
        region: String,
        expected: String,
        got: String,
    },
    /// Names both repairs, because the caller knows which one applies and the
    /// engine cannot: only they know whether the stored vectors came from the
    /// model they are asking for, or from the one on record.
    #[error(
        "region '{region}' exists for model '{expected}', requested '{got}'; if its vectors \
         really came from '{got}' and only the label is wrong, call reclassify_region, and if \
         they came from '{expected}' and you want '{got}', call reembed_region"
    )]
    ModelMismatch {
        region: String,
        expected: String,
        got: String,
    },
    #[error("{0}")]
    Invalid(String),
}
