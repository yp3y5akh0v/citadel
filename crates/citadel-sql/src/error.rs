use thiserror::Error;

pub type Result<T> = std::result::Result<T, SqlError>;

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("table '{0}' not found")]
    TableNotFound(String),

    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("column '{0}' not found")]
    ColumnNotFound(String),

    #[error("duplicate primary key")]
    DuplicateKey,

    #[error("NOT NULL constraint failed: {0}")]
    NotNullViolation(String),

    #[error("type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("primary key required")]
    PrimaryKeyRequired,

    #[error("duplicate column: {0}")]
    DuplicateColumn(String),

    #[error("row too large: encoded size {size} exceeds limit {max}")]
    RowTooLarge { size: usize, max: usize },

    #[error("key too large: encoded size {size} exceeds limit {max}")]
    KeyTooLarge { size: usize, max: usize },

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("division by zero")]
    DivisionByZero,

    #[error("integer overflow")]
    IntegerOverflow,

    #[error("column '{0}' is ambiguous in aggregate query")]
    AmbiguousColumn(String),

    #[error("transaction already active")]
    TransactionAlreadyActive,

    #[error("no active transaction")]
    NoActiveTransaction,

    #[error("storage error: {0}")]
    Storage(#[from] citadel_core::Error),
}
