use crate::error::{DataForgeError, Result};

pub struct ParsedModel {
    pub header: String,
    pub body: String,
}

pub fn parse_sql_file(content: &str) -> Result<ParsedModel> {
    let parts: Vec<&str> = content.split("---").collect();
    if parts.len() < 3 {
        return Err(DataForgeError::SqlParseError("SQL file must have --- header block".to_string()));
    }
    Ok(ParsedModel {
        header: parts[1].trim().to_string(),
        body: parts[2..].join("---").trim().to_string(),
    })
}
