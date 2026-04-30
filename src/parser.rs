use crate::error::{DataForgeError, Result};
use std::cell::Cell;

thread_local! {
    pub static PARSE_COUNT: Cell<u32> = Cell::new(0);
}

pub struct ParsedModel {
    pub header: String,
    pub body: String,
}

pub fn parse_sql_file(content: &str) -> Result<ParsedModel> {
    PARSE_COUNT.with(|c| c.set(c.get() + 1));
    let parts: Vec<&str> = content.split("---").collect();
    if parts.len() < 3 {
        return Err(DataForgeError::SqlParseError("SQL file must have --- header block".to_string()));
    }
    Ok(ParsedModel {
        header: parts[1].trim().to_string(),
        body: parts[2..].join("---").trim().to_string(),
    })
}
