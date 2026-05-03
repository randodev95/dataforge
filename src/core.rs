pub mod sql;

pub use sql::TitanSQL;

/// Quotes an identifier for the PostgreSQL dialect (double quotes).
/// This ensures safety against reserved words and special characters.
pub fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace("\"", "\"\""))
}
