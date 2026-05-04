use blake3::Hasher;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicHash(String);

impl LogicHash {
    pub fn new(hash: String) -> Self {
        Self(hash)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LogicHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct TitanHasher {
    inner: Hasher,
}

impl Default for TitanHasher {
    fn default() -> Self {
        Self {
            inner: Hasher::new(),
        }
    }
}

impl TitanHasher {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, data: &str) {
        self.inner.update(data.as_bytes());
    }

    pub fn finalize(self) -> LogicHash {
        LogicHash(self.inner.finalize().to_hex().to_string())
    }

    pub fn calculate(
        normalized_sql: &str,
        config_json: &str,
        parent_hashes: &[LogicHash],
        is_inc: bool,
    ) -> LogicHash {
        let mut hasher = Self::new();
        
        // 1. Process Normalized SQL
        hasher.update(normalized_sql);
        
        // 2. Process Serialized Config
        hasher.update(config_json);

        // 3. Process is_inc flag
        hasher.update(if is_inc { "incremental" } else { "initial" });
        
        // 4. Process Parent Hashes in deterministic order
        let mut sorted_parents: Vec<&str> = parent_hashes
            .iter()
            .map(|h| h.as_str())
            .collect();
        sorted_parents.sort_unstable();
        
        for parent in sorted_parents {
            hasher.update(parent);
        }

        hasher.finalize()
    }
}
