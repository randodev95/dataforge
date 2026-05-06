use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum MaskStrategy {
    Hash,    // SHA256
    Redact,  // '***'
    Partial, // 'ab***yz'
    None,
}

impl MaskStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Hash => "hash",
            Self::Redact => "redact",
            Self::Partial => "partial",
            Self::None => "none",
        }
    }

    pub fn apply(&self, column: &str) -> String {
        match self {
            Self::Hash => format!("sha256(CAST({column} AS VARCHAR))"),
            Self::Redact => "'***'".to_string(),
            Self::Partial => format!(
                "CASE WHEN length(CAST({column} AS VARCHAR)) < 5 THEN '***' ELSE concat(substring(CAST({column} AS VARCHAR), 1, 2), '***', substring(CAST({column} AS VARCHAR), length(CAST({column} AS VARCHAR)) - 1, 2)) END"
            ),
            Self::None => column.to_string(),
        }
    }
}
