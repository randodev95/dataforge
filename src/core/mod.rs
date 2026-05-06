//! # Titan Core Types
//!
//! This module defines the fundamental types used throughout the Titan Engine,
//! such as `TitanSQL`.

pub mod audit;
pub mod ci_diff;
pub mod column_map;
pub mod dialect;
pub mod drift;
pub mod lineage;
pub mod lineage_diff;
pub mod masking;
pub mod shadow;
pub mod sql;

pub use dialect::{SqlDialect, get_dialect};
pub use sql::TitanSQL;

/// Quotes an identifier for the PostgreSQL dialect (double quotes).
/// This ensures safety against reserved words and special characters.
pub fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
