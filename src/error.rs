use thiserror::Error;
use crate::fingerprint::LogicHash;

#[derive(Error, Debug)]
pub enum TitanError {
    #[error("Dependency '{0}' not found in environment '{1}'")]
    DependencyNotFound(String, String),

    #[error("Circular dependency detected in DAG: {0}")]
    CircularDependency(String),

    #[error("SQL parsing failed: {0}")]
    SqlParseError(String),

    #[error("Template rendering failed: {0}")]
    TemplateError(String),

    #[error("Execution failed: {0}")]
    ExecutionError(String),

    #[error("State store error: {0}")]
    StateError(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, TitanError>;
