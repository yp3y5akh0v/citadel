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

    #[error("column '{0}' is ambiguous")]
    AmbiguousColumn(String),

    #[error("index '{0}' not found")]
    IndexNotFound(String),

    #[error("index '{0}' already exists")]
    IndexAlreadyExists(String),

    #[error("unique constraint violated on index '{0}'")]
    UniqueViolation(String),

    #[error("CHECK constraint failed: {0}")]
    CheckViolation(String),

    #[error("FOREIGN KEY constraint violated: {0}")]
    ForeignKeyViolation(String),

    #[error("transaction already active")]
    TransactionAlreadyActive,

    #[error("no active transaction")]
    NoActiveTransaction,

    #[error("subquery must return exactly one column")]
    SubqueryMultipleColumns,

    #[error("scalar subquery returned more than one row")]
    SubqueryMultipleRows,

    #[error("parameter count mismatch: expected {expected}, got {got}")]
    ParameterCountMismatch { expected: usize, got: usize },

    #[error("compound column count mismatch: left has {left}, right has {right}")]
    CompoundColumnCountMismatch { left: usize, right: usize },

    #[error("CTE '{name}' column alias count mismatch: expected {expected}, got {got}")]
    CteColumnAliasMismatch {
        name: String,
        expected: usize,
        got: usize,
    },

    #[error("duplicate CTE name: '{0}'")]
    DuplicateCteName(String),

    #[error("recursive CTE '{0}' requires UNION or UNION ALL")]
    RecursiveCteNoUnion(String),

    #[error("recursive CTE '{0}' exceeded maximum iterations ({1})")]
    RecursiveCteMaxIterations(String, usize),

    #[error("storage error: {0}")]
    Storage(#[from] citadel_core::Error),
}
