use serde::{Deserialize, Serialize};
use std::fmt;

/// TitanSQL is the "Serialized Pipe" boundary.
/// It wraps a SQL string that is guaranteed to be in the PostgreSQL dialect.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TitanSQL(String);

impl TitanSQL {
    pub fn new(sql: String) -> Self {
        Self(sql)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for TitanSQL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TitanSQL {
    fn from(sql: String) -> Self {
        Self(sql)
    }
}

impl From<&str> for TitanSQL {
    fn from(sql: &str) -> Self {
        Self(sql.to_string())
    }
}
