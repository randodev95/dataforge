use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataForgeError {
    #[error("Cycle detected in DAG at model: {0}")]
    CycleDetected(String),

    #[error("Model not found: {0}")]
    ModelNotFound(String),

    #[error("Environment not found: {0}")]
    EnvNotFound(String),

    #[error("Contract violation: Model '{model}' requires column '{column}' from upstream '{upstream}'")]
    ContractViolation {
        model: String,
        column: String,
        upstream: String,
    },

    #[error("SQL Parse Error: {0}")]
    SqlParseError(String),

    #[error("Warehouse Execution Error: {0}")]
    WarehouseError(String),

    #[error("Starlark Execution Error: {0}")]
    StarlarkError(String),

    #[error("State Storage Error: {0}")]
    StorageError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, DataForgeError>;
