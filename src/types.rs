use serde::{Serialize, Deserialize};
use std::fmt;

// rust-skills: Newtype Pattern for Domain Safety
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelName(pub String);

impl fmt::Display for ModelName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EnvName(pub String);

impl fmt::Display for EnvName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DagHash(pub String);

impl fmt::Display for DagHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnLineage {
    pub column: String,
    pub source_models: Vec<ModelName>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Model {
    pub name: ModelName,
    pub query: String,
    pub deps: Vec<ModelName>,
    pub contracts: Vec<String>,
    pub watermark: Option<String>,
    pub inferred_columns: Vec<String>,
    pub column_lineage: Vec<ColumnLineage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetadata {
    pub name: ModelName,
    pub hash: DagHash,
    pub deps: Vec<ModelName>,
    pub columns: Vec<String>,
    pub lineage: Vec<ColumnLineage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheItem {
    pub file_hash: String,
    pub metadata: ModelMetadata,
}
