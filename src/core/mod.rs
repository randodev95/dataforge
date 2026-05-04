//! # Titan Core Types
//! 
//! This module defines the fundamental types used throughout the Titan Engine, 
//! such as `TitanSQL`.

pub mod sql;

pub use sql::TitanSQL;

/// Quotes an identifier for the PostgreSQL dialect (double quotes).
/// This ensures safety against reserved words and special characters.
pub fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace("\"", "\"\""))
}
