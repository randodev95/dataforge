//! # Titan Error Handling
//! 
//! This module defines the central `TitanError` type, providing 
//! structured, library-grade error propagation across the engine.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TitanError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Project load error: {0}")]
    ProjectLoadError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Template rendering error: {0}")]
    TemplateError(String),

    #[error("SQL parsing/planning error: {0}")]
    SqlParseError(String),

    #[error("Database/Execution error: {0}")]
    DatabaseError(String),

    #[error("State store error: {0}")]
    StateError(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Dependency not found: {0} in environment {1}")]
    DependencyNotFound(String, String),

    #[error("Circular dependency detected: {0}")]
    CircularDependency(String),
}

pub type Result<T> = std::result::Result<T, TitanError>;
